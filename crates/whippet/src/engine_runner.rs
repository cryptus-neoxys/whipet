// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::time::sleep;
use tracing::{error, info, warn};
use whippet_core::headers::{MessageMeta, SCHEMA_EXECUTION, SCHEMA_MD_EVENT, SCHEMA_MD_SNAPSHOT};
use whippet_core::protocol::{Command, ExecKind, ExecutionReport, MdEvent, MdKind, MdSnapshot};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{JournalMessage, Topic};

use crate::context::NodeContext;

/// Owns one partition's books. The single writer is this task; the engine is a
/// pure state machine and never awaits.
pub async fn run_engine_shard(ctx: Arc<NodeContext>, shard_id: u32) -> Result<()> {
    let exchange = ctx.exchange();
    let loaded = ctx
        .snapshots
        .load(shard_id)
        .with_context(|| format!("loading snapshot for shard {shard_id}"))?;
    let snapshot_last_offset = loaded.as_ref().and_then(|snapshot| {
        snapshot
            .symbols
            .iter()
            .map(|symbol| symbol.last_offset)
            .max()
    });
    let mut engine = match loaded {
        Some(snapshot) => {
            let mut engine = ShardEngine::from_snapshot(exchange, snapshot)
                .with_context(|| format!("restoring shard {shard_id}"))?;
            engine.set_snapshot_interval(ctx.config.snapshot_every);
            info!(
                shard = shard_id,
                hash = %hex(&engine.state_hash()),
                "restored shard from snapshot"
            );
            engine
        }
        None => {
            ShardEngine::new_with_snapshot_interval(shard_id, exchange, ctx.config.snapshot_every)
        }
    };

    let consumer = format!("whippet-engine-p{shard_id}");
    let mut next_offset = match snapshot_last_offset {
        Some(offset) => offset.saturating_add(1),
        None => match ctx
            .journal
            .committed(Topic::Orders, shard_id, &consumer)
            .await
        {
            Ok(Some(offset)) => offset.saturating_add(1),
            Ok(None) => 0,
            Err(error) => {
                anyhow::bail!(
                    "could not read committed offset for shard {shard_id}: {error}; refusing to replay from zero"
                );
            }
        },
    };

    // Publish a snapshot immediately so a fresh dashboard has a state even
    // before the first order arrives.
    let mut initial_symbols: Vec<_> = engine.symbols().collect();
    initial_symbols.sort_unstable();
    let initial: Vec<MdSnapshot> = initial_symbols
        .iter()
        .filter_map(|symbol| engine.engine(*symbol).map(|book| book.snapshot()))
        .collect();
    if !initial.is_empty() {
        let messages: Vec<JournalMessage> = initial.iter().map(snapshot_message).collect();
        if let Err(error) = append_with_retry(&ctx, Topic::Md, shard_id, &messages).await {
            warn!(shard = shard_id, error = %error, "initial MD snapshot was not published");
        }
    }

    info!(
        shard = shard_id,
        start_offset = next_offset,
        consumer,
        "engine shard running"
    );
    let mut out = EngineOutput::with_capacity(64, 64);
    let mut shutdown = ctx.shutdown.subscribe();

    loop {
        let records = tokio::select! {
            _ = shutdown.changed() => {
                let snapshot = engine.snapshot_state();
                let store = ctx.snapshots.clone();
                match tokio::task::spawn_blocking(move || store.save(&snapshot)).await {
                    Ok(Ok(_)) => info!(shard = shard_id, hash = %hex(&engine.state_hash()), "engine shard stopped"),
                    Ok(Err(error)) => warn!(shard = shard_id, error = %error, "final snapshot write failed"),
                    Err(error) => warn!(shard = shard_id, error = %error, "final snapshot task failed"),
                }
                return Ok(());
            }
            result = ctx.journal.read(Topic::Orders, shard_id, &consumer, next_offset, 256, 50) => result,
        };
        let records = match records {
            Ok(records) => records,
            Err(error) => {
                warn!(shard = shard_id, error = %error, "order poll failed; retrying");
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        if records.is_empty() {
            if !ctx.journal.is_iggy() {
                sleep(Duration::from_millis(1)).await;
            }
            continue;
        }
        if let Some(first) = records.first()
            && first.offset > next_offset
        {
            anyhow::bail!(
                "order log gap on shard {shard_id}: requested offset {next_offset}, first available {}; a snapshot or reset is required",
                first.offset
            );
        }

        let mut executions = Vec::new();
        let mut md_messages = Vec::new();
        let mut batch_last_offset = next_offset.saturating_sub(1);
        let mut snapshot_due = false;

        for record in records {
            match Command::decode(&record.payload) {
                Ok(command) => {
                    let started = Instant::now();
                    if let Err(error) = engine.process(&command, record.offset, &mut out) {
                        warn!(
                            shard = shard_id,
                            offset = record.offset,
                            error = %error,
                            "engine rejected an input for this shard"
                        );
                    } else {
                        ctx.metrics
                            .record_match_latency_us(started.elapsed().as_micros() as u64);
                        ctx.metrics.record_engine_command();
                        for execution in &out.executions {
                            ctx.metrics.record_execution();
                            if matches!(execution.kind, ExecKind::Accepted | ExecKind::Rejected) {
                                ctx.metrics
                                    .record_order_outcome(execution.kind == ExecKind::Rejected);
                            }
                            executions.push(execution_message(execution));
                        }
                        for event in &out.md {
                            md_messages.push(md_message(event));
                        }
                        for snapshot in &out.snapshots {
                            snapshot_due = true;
                            md_messages.push(snapshot_message(snapshot));
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        shard = shard_id,
                        offset = record.offset,
                        error = %error,
                        "skipping undecodable order"
                    );
                }
            }
            batch_last_offset = record.offset;
        }

        if let Err(error) = append_with_retry(&ctx, Topic::Executions, shard_id, &executions).await
        {
            error!(shard = shard_id, error = %error, "execution publish failed");
            sleep(Duration::from_millis(250)).await;
            continue;
        }
        if let Err(error) = append_with_retry(&ctx, Topic::Md, shard_id, &md_messages).await {
            error!(shard = shard_id, error = %error, "MD publish failed");
            sleep(Duration::from_millis(250)).await;
            continue;
        }
        // The outputs are already appended. Retry only the offset commit; do
        // not re-read and re-apply the batch, which would duplicate outputs and
        // diverge the execution sequence from a clean replay.
        commit_with_retry(&ctx, Topic::Orders, shard_id, &consumer, batch_last_offset).await?;
        next_offset = batch_last_offset.saturating_add(1);

        if snapshot_due {
            let snapshot = engine.snapshot_state();
            let store = ctx.snapshots.clone();
            let hash = hex(&engine.state_hash());
            match tokio::task::spawn_blocking(move || store.save(&snapshot)).await {
                Ok(Ok(path)) => info!(
                    shard = shard_id,
                    path = %path.display(),
                    hash,
                    "snapshot written"
                ),
                Ok(Err(error)) => {
                    warn!(shard = shard_id, error = %error, "snapshot write failed");
                    ctx.mark_shard_failed();
                }
                Err(error) => {
                    warn!(shard = shard_id, error = %error, "snapshot task failed");
                    ctx.mark_shard_failed();
                }
            }
        }
    }
}

/// Consume the canonical market-data log and apply it to the dashboard cache.
pub async fn run_md_shard(ctx: Arc<NodeContext>, shard_id: u32) -> Result<()> {
    let consumer = format!("whippet-md-p{shard_id}");
    let mut next_offset = match ctx.journal.committed(Topic::Md, shard_id, &consumer).await {
        Ok(Some(offset)) => offset.saturating_add(1),
        Ok(None) => 0,
        Err(error) => {
            anyhow::bail!(
                "could not read MD committed offset for shard {shard_id}: {error}; refusing to replay from zero"
            );
        }
    };
    info!(
        shard = shard_id,
        start_offset = next_offset,
        "MD consumer running"
    );
    let mut shutdown = ctx.shutdown.subscribe();

    loop {
        let records = tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            result = ctx.journal.read(Topic::Md, shard_id, &consumer, next_offset, 1_024, 100) => result,
        };
        let records = match records {
            Ok(records) => records,
            Err(error) => {
                warn!(shard = shard_id, error = %error, "MD poll failed; retrying");
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        if records.is_empty() {
            if !ctx.journal.is_iggy() {
                sleep(Duration::from_millis(1)).await;
            }
            continue;
        }
        if let Some(first) = records.first()
            && first.offset > next_offset
        {
            anyhow::bail!(
                "MD log gap on shard {shard_id}: requested offset {next_offset}, first available {}; reset or resnapshot is required",
                first.offset
            );
        }
        let mut last_offset = next_offset.saturating_sub(1);
        for record in records {
            let kind = record.payload.get(1).copied();
            match kind {
                Some(code) if code == MdKind::Snapshot.as_code() => {
                    match MdSnapshot::decode(&record.payload) {
                        Ok(snapshot) => ctx.hub.apply_snapshot(&snapshot),
                        Err(error) => warn!(
                            shard = shard_id,
                            offset = record.offset,
                            error = %error,
                            "skipping undecodable MD snapshot"
                        ),
                    }
                }
                _ => match MdEvent::decode(&record.payload) {
                    Ok(event) => {
                        ctx.hub.apply(&event);
                    }
                    Err(error) => warn!(
                        shard = shard_id,
                        offset = record.offset,
                        error = %error,
                        "skipping undecodable MD event"
                    ),
                },
            }
            last_offset = record.offset;
        }
        commit_with_retry(&ctx, Topic::Md, shard_id, &consumer, last_offset).await?;
        next_offset = last_offset.saturating_add(1);
    }
}

async fn commit_with_retry(
    ctx: &NodeContext,
    topic: Topic,
    partition: u32,
    consumer: &str,
    offset: u64,
) -> Result<()> {
    let mut delay = Duration::from_millis(100);
    loop {
        match ctx.journal.commit(topic, partition, consumer, offset).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                warn!(topic = %topic, partition, offset, error = %error, "offset commit failed; retrying the commit only");
                sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
    }
}

async fn append_with_retry(
    ctx: &NodeContext,
    topic: Topic,
    partition: u32,
    messages: &[JournalMessage],
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let mut delay = Duration::from_millis(100);
    loop {
        match ctx.journal.append(topic, partition, messages).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                warn!(topic = %topic, partition, error = %error, "append failed; retrying");
                sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
    }
}

fn execution_message(execution: &ExecutionReport) -> JournalMessage {
    let meta = MessageMeta::new(
        execution.symbol,
        execution.seq,
        "execution",
        SCHEMA_EXECUTION,
        u128::from(execution.cause_offset),
    );
    JournalMessage::new(execution.encode(), meta.pairs())
}

fn md_message(event: &MdEvent) -> JournalMessage {
    let meta = MessageMeta::new(
        event.symbol,
        event.seq,
        "md",
        SCHEMA_MD_EVENT,
        u128::from(event.cause_offset),
    );
    JournalMessage::new(event.encode(), meta.pairs())
}

fn snapshot_message(snapshot: &MdSnapshot) -> JournalMessage {
    let meta = MessageMeta::new(
        snapshot.symbol,
        snapshot.seq,
        "md_snapshot",
        SCHEMA_MD_SNAPSHOT,
        u128::from(snapshot.cause_offset),
    );
    JournalMessage::new(snapshot.encode(), meta.pairs())
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
