// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{error, info, warn};
use whippet_core::headers::{MessageMeta, SCHEMA_EXECUTION, SCHEMA_MD_EVENT, SCHEMA_MD_SNAPSHOT};
use whippet_core::protocol::{Command, ExecKind, ExecutionReport, MdEvent, MdKind, MdSnapshot};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{JournalMessage, Topic};

use crate::context::NodeContext;
use crate::snapshot::{SequencedSnapshot, SnapshotWriter};

/// How many records a shard applies between consumer-offset commits.
const COMMIT_INTERVAL_RECORDS: u64 = 256;

/// Upper bound on how long the consumer offset may lag the applied offset.
const COMMIT_MAX_DELAY: Duration = Duration::from_millis(100);

/// A single commit attempt is bounded so a wedged broker cannot stall the loop.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Backoff for a commit that failed; the loop keeps applying records meanwhile.
const COMMIT_RETRY_INITIAL: Duration = Duration::from_millis(100);
const COMMIT_RETRY_MAX: Duration = Duration::from_secs(2);

/// Commit failures are logged at most this often, never once per record.
const COMMIT_WARN_INTERVAL: Duration = Duration::from_secs(1);

/// Batch sizes: one poll's worth of records, and the capacity the per-batch
/// output vectors are allocated with once.
const BATCH_CAPACITY: usize = 1_024;

/// Snapshot requests in flight between the shard loop and the writer.
const SNAPSHOT_QUEUE_CAPACITY: usize = 2;

/// Minimum spacing between engine-state snapshots.
const SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_secs(5);

/// The node gives each shard task five seconds to stop; the final snapshot flush
/// has to fit inside that budget.
const FINAL_SNAPSHOT_SEND_TIMEOUT: Duration = Duration::from_millis(1_000);
const FINAL_SNAPSHOT_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

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
            engine.set_md_snapshot_interval(1_000_000);
            info!(
                shard = shard_id,
                hash = %hex(&engine.state_hash()),
                "restored shard from snapshot"
            );
            engine
        }
        None => {
            let mut engine = ShardEngine::new_with_snapshot_interval(
                shard_id,
                exchange,
                ctx.config.snapshot_every,
            );
            engine.set_md_snapshot_interval(1_000_000);
            engine
        }
    };

    let consumer = format!("whippet-engine-p{shard_id}");
    // The snapshot is authoritative for the resume point, so its offset is
    // treated as already acknowledged: the watermark must not fire a redundant
    // commit on the first batch after a restore.
    let committed_offset = match snapshot_last_offset {
        Some(offset) => Some(offset),
        None => match ctx
            .journal
            .committed(Topic::Orders, shard_id, &consumer)
            .await
        {
            Ok(offset) => offset,
            Err(error) => {
                anyhow::bail!(
                    "could not read committed offset for shard {shard_id}: {error}; refusing to replay from zero"
                );
            }
        },
    };
    let mut next_offset = match committed_offset {
        Some(offset) => offset.saturating_add(1),
        None => 0,
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
    let mut out = EngineOutput::with_capacity(BATCH_CAPACITY, BATCH_CAPACITY);
    // Hoisted out of the batch loop and reused: a shard polls continuously, so
    // allocating these per batch is pure overhead. They are cleared at the top
    // of every batch.
    let mut executions: Vec<JournalMessage> = Vec::with_capacity(BATCH_CAPACITY);
    let mut md_messages: Vec<JournalMessage> = Vec::with_capacity(BATCH_CAPACITY);

    let (snapshot_tx, snapshot_rx) = mpsc::channel(SNAPSHOT_QUEUE_CAPACITY);
    let snapshot_writer = spawn_snapshot_writer(Arc::clone(&ctx), shard_id, snapshot_rx);
    let mut snapshot_sequence: u64 = 0;

    let mut shutdown = ctx.shutdown.subscribe();
    let mut last_snapshot_at = Instant::now();
    let mut watermark = CommitWatermark::new(committed_offset);
    let mut commit_retry = CommitRetry::new();

    loop {
        let records = tokio::select! {
            _ = shutdown.changed() => {
                // Persist progress before the final snapshot. The snapshot is the
                // authoritative resume point, but the committed offset is what the
                // lag metric and any other reader sees.
                if let Some(offset) = watermark.pending() {
                    match commit_once(&ctx, Topic::Orders, shard_id, &consumer, offset).await {
                        Ok(()) => {
                            watermark.mark_committed(offset, Instant::now());
                            info!(shard = shard_id, offset, "committed final applied offset");
                        }
                        Err(error) => warn!(
                            shard = shard_id,
                            offset,
                            error = %error,
                            "final offset commit failed; the snapshot still records the applied state"
                        ),
                    }
                }
                snapshot_sequence += 1;
                let final_snapshot = SequencedSnapshot {
                    sequence: snapshot_sequence,
                    snapshot: engine.snapshot_state(),
                };
                match tokio::time::timeout(
                    FINAL_SNAPSHOT_SEND_TIMEOUT,
                    snapshot_tx.send(final_snapshot),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => warn!(
                        shard = shard_id,
                        "snapshot writer stopped before the final snapshot was queued"
                    ),
                    Err(_) => warn!(shard = shard_id, "timed out queueing the final snapshot"),
                }
                // Close the queue so the writer drains and exits.
                drop(snapshot_tx);
                match tokio::time::timeout(FINAL_SNAPSHOT_JOIN_TIMEOUT, snapshot_writer).await {
                    Ok(Ok(())) => info!(
                        shard = shard_id,
                        sequence = snapshot_sequence,
                        "engine shard stopped"
                    ),
                    Ok(Err(error)) => warn!(shard = shard_id, error = %error, "final snapshot writer failed"),
                    Err(_) => warn!(
                        shard = shard_id,
                        "final snapshot did not finish within the shutdown budget"
                    ),
                }
                return Ok(());
            }
            result = ctx.journal.read_payloads(Topic::Orders, shard_id, &consumer, next_offset, BATCH_CAPACITY as u32, 50) => result,
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
            // Nothing new arrived. A due commit is still worth flushing so the
            // watermark does not sit uncommitted through an idle period.
            maybe_commit(
                &ctx,
                Topic::Orders,
                shard_id,
                &consumer,
                &mut watermark,
                &mut commit_retry,
            )
            .await;
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

        executions.clear();
        md_messages.clear();
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
                        snapshot_due |= out.state_snapshot_due;
                        for snapshot in &out.snapshots {
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

        // Executions and MD are independent topics. Awaiting them in sequence
        // added a full broker round trip to every batch; join them so the shard
        // pays one RTT instead of two. Both must succeed before the applied
        // offset advances.
        let (execution_result, md_result) = tokio::join!(
            append_with_retry(&ctx, Topic::Executions, shard_id, &executions),
            append_with_retry(&ctx, Topic::Md, shard_id, &md_messages),
        );
        if let Err(error) = execution_result {
            error!(shard = shard_id, error = %error, "execution publish failed");
            sleep(Duration::from_millis(250)).await;
            continue;
        }
        if let Err(error) = md_result {
            error!(shard = shard_id, error = %error, "MD publish failed");
            sleep(Duration::from_millis(250)).await;
            continue;
        }
        // The outputs are already appended, so this offset is fully applied.
        // The commit itself is decoupled: it is retried with backoff on later
        // iterations and never blocks the loop or re-reads the batch.
        watermark.note_applied(batch_last_offset);
        next_offset = batch_last_offset.saturating_add(1);
        maybe_commit(
            &ctx,
            Topic::Orders,
            shard_id,
            &consumer,
            &mut watermark,
            &mut commit_retry,
        )
        .await;

        if snapshot_due && last_snapshot_at.elapsed() >= SNAPSHOT_MIN_INTERVAL {
            last_snapshot_at = Instant::now();
            // Hand the state to the writer; the save (serialise + fsync +
            // rename) happens off this task.
            request_snapshot(
                &ctx,
                shard_id,
                &engine,
                &mut snapshot_sequence,
                &snapshot_tx,
            );
        }
    }
}

/// Consume the canonical market-data log and apply it to the dashboard cache.
pub async fn run_md_shard(ctx: Arc<NodeContext>, shard_id: u32) -> Result<()> {
    let consumer = format!("whippet-md-p{shard_id}");
    let committed_offset = match ctx.journal.committed(Topic::Md, shard_id, &consumer).await {
        Ok(offset) => offset,
        Err(error) => {
            anyhow::bail!(
                "could not read MD committed offset for shard {shard_id}: {error}; refusing to replay from zero"
            );
        }
    };
    let mut next_offset = match committed_offset {
        Some(offset) => offset.saturating_add(1),
        None => 0,
    };
    info!(
        shard = shard_id,
        start_offset = next_offset,
        "MD consumer running"
    );
    let mut shutdown = ctx.shutdown.subscribe();
    let mut watermark = CommitWatermark::new(committed_offset);
    let mut commit_retry = CommitRetry::new();
    // Reused across polls: a poll's worth of decoded deltas is applied in one
    // hub-lock batch.
    let mut events: Vec<MdEvent> = Vec::with_capacity(BATCH_CAPACITY);

    loop {
        let records = tokio::select! {
            _ = shutdown.changed() => {
                if let Some(offset) = watermark.pending() {
                    match commit_once(&ctx, Topic::Md, shard_id, &consumer, offset).await {
                        Ok(()) => watermark.mark_committed(offset, Instant::now()),
                        Err(error) => warn!(shard = shard_id, offset, error = %error, "final MD offset commit failed"),
                    }
                }
                return Ok(());
            }
            result = ctx.journal.read_payloads(Topic::Md, shard_id, &consumer, next_offset, BATCH_CAPACITY as u32, 100) => result,
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
            maybe_commit(
                &ctx,
                Topic::Md,
                shard_id,
                &consumer,
                &mut watermark,
                &mut commit_retry,
            )
            .await;
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
        events.clear();
        for record in records {
            let kind = record.payload.get(1).copied();
            match kind {
                Some(code) if code == MdKind::Snapshot.as_code() => {
                    // Order matters: deltas that precede a recovery point are
                    // applied before the snapshot, exactly as they were read.
                    flush_md_events(&ctx, &mut events);
                    match MdSnapshot::decode(&record.payload) {
                        Ok(snapshot) => ctx.hub.apply_snapshot(&snapshot),
                        Err(error) => {
                            ctx.mark_shard_failed();
                            anyhow::bail!(
                                "undecodable MD snapshot on shard {shard_id} at offset {}: {error}; refusing to advance past a lost recovery point",
                                record.offset
                            );
                        }
                    }
                }
                _ => match MdEvent::decode(&record.payload) {
                    Ok(event) => events.push(event),
                    Err(error) => {
                        ctx.mark_shard_failed();
                        anyhow::bail!(
                            "undecodable MD event on shard {shard_id} at offset {}: {error}; refusing to desynchronise the book",
                            record.offset
                        );
                    }
                },
            }
            last_offset = record.offset;
        }
        // The whole batch is applied under one hub lock, before the offset
        // commit, so the committed offset never runs ahead of the cached book.
        flush_md_events(&ctx, &mut events);
        watermark.note_applied(last_offset);
        next_offset = last_offset.saturating_add(1);
        maybe_commit(
            &ctx,
            Topic::Md,
            shard_id,
            &consumer,
            &mut watermark,
            &mut commit_retry,
        )
        .await;
    }
}

/// Apply the decoded deltas under one hub-lock acquisition, then reset the
/// buffer. Ordering is preserved: the batch is flushed before an `MdSnapshot`
/// and before the offset commit.
fn flush_md_events(ctx: &NodeContext, events: &mut Vec<MdEvent>) {
    if events.is_empty() {
        return;
    }
    ctx.hub.apply_batch(events);
    events.clear();
}

/// Where a consumer stands relative to its last acknowledged offset.
///
/// `applied` is the last record whose outputs were appended to the journal;
/// `committed` is the last offset the broker acknowledged. The two are allowed
/// to drift apart by [`COMMIT_INTERVAL_RECORDS`] records or
/// [`COMMIT_MAX_DELAY`] — whichever comes first — so the loop is not paying a
/// broker round trip per batch.
///
/// The replay window that buys: a crash before the next commit re-applies up to
/// that many already-applied records. That is safe for this shard because the
/// engine dedup table rejects a replayed order id instead of matching it twice
/// (books and offsets do not diverge), and because `run_md_shard` relies on the
/// per-symbol market-data sequence, which ignores an event at or below the last
/// applied sequence. The only visible artifact is a duplicate rejection
/// execution, which a crash between append and commit could already produce.
#[derive(Debug)]
struct CommitWatermark {
    applied: Option<u64>,
    committed: Option<u64>,
    last_commit_at: Instant,
}

impl CommitWatermark {
    fn new(committed: Option<u64>) -> Self {
        Self {
            applied: None,
            committed,
            last_commit_at: Instant::now(),
        }
    }

    /// Record that everything up to `offset` has been fully applied.
    fn note_applied(&mut self, offset: u64) {
        self.applied = Some(offset);
    }

    /// Record that the broker acknowledged `offset`. The watermark never moves
    /// backwards, even if an older offset is committed by mistake.
    fn mark_committed(&mut self, offset: u64, now: Instant) {
        self.committed = Some(match self.committed {
            Some(committed) => committed.max(offset),
            None => offset,
        });
        self.last_commit_at = now;
    }

    /// The highest applied offset that the broker has not acknowledged yet.
    fn pending(&self) -> Option<u64> {
        match (self.applied, self.committed) {
            (Some(applied), Some(committed)) if committed >= applied => None,
            (Some(applied), _) => Some(applied),
            (None, _) => None,
        }
    }

    fn due(&self, now: Instant) -> bool {
        commit_due(
            self.applied,
            self.committed,
            now.saturating_duration_since(self.last_commit_at),
            COMMIT_INTERVAL_RECORDS,
            COMMIT_MAX_DELAY,
        )
    }
}

/// Decide whether the applied offset is worth a broker round trip.
///
/// Pure so the watermark can be tested without a journal: `applied` is the last
/// fully applied record, `committed` the last acknowledged offset, and `elapsed`
/// the time since the last successful commit.
fn commit_due(
    applied: Option<u64>,
    committed: Option<u64>,
    elapsed: Duration,
    interval: u64,
    max_delay: Duration,
) -> bool {
    let Some(applied) = applied else {
        return false;
    };
    let uncommitted = match committed {
        Some(committed) if committed >= applied => return false,
        Some(committed) => applied - committed,
        // Nothing was ever committed: every applied record is uncommitted.
        None => applied.saturating_add(1),
    };
    uncommitted >= interval || elapsed >= max_delay
}

/// Backoff state for a commit that failed.
///
/// The shard keeps applying records while this is backing off; the warning is
/// throttled so a broker outage logs once per [`COMMIT_WARN_INTERVAL`] instead
/// of once per batch.
#[derive(Debug)]
struct CommitRetry {
    next_at: Instant,
    delay: Duration,
    last_warn: Option<Instant>,
}

impl CommitRetry {
    fn new() -> Self {
        Self {
            next_at: Instant::now(),
            delay: COMMIT_RETRY_INITIAL,
            last_warn: None,
        }
    }

    fn ready(&self, now: Instant) -> bool {
        now >= self.next_at
    }

    fn succeeded(&mut self) {
        self.delay = COMMIT_RETRY_INITIAL;
        self.next_at = Instant::now();
    }

    /// Note a failure and report whether this one should be logged.
    fn failed(&mut self, now: Instant) -> bool {
        let warn = self
            .last_warn
            .is_none_or(|at| now.saturating_duration_since(at) >= COMMIT_WARN_INTERVAL);
        if warn {
            self.last_warn = Some(now);
        }
        self.next_at = now + self.delay;
        self.delay = (self.delay * 2).min(COMMIT_RETRY_MAX);
        warn
    }
}

/// Commit the watermark when it is due and the retry backoff has elapsed.
///
/// A failure never propagates: the shard keeps applying records and retries the
/// commit later. Only the commit lags, never the applied state.
async fn maybe_commit(
    ctx: &NodeContext,
    topic: Topic,
    shard_id: u32,
    consumer: &str,
    watermark: &mut CommitWatermark,
    retry: &mut CommitRetry,
) {
    let now = Instant::now();
    if !retry.ready(now) || !watermark.due(now) {
        return;
    }
    let Some(offset) = watermark.pending() else {
        return;
    };
    match commit_once(ctx, topic, shard_id, consumer, offset).await {
        Ok(()) => {
            watermark.mark_committed(offset, Instant::now());
            retry.succeeded();
        }
        Err(error) => {
            if retry.failed(Instant::now()) {
                warn!(
                    topic = %topic,
                    partition = shard_id,
                    offset,
                    error = %error,
                    "offset commit failed; records stay applied and the commit is retried"
                );
            }
        }
    }
}

/// One bounded commit attempt.
async fn commit_once(
    ctx: &NodeContext,
    topic: Topic,
    partition: u32,
    consumer: &str,
    offset: u64,
) -> Result<()> {
    match tokio::time::timeout(
        COMMIT_TIMEOUT,
        ctx.journal.commit(topic, partition, consumer, offset),
    )
    .await
    {
        Ok(result) => result.map_err(anyhow::Error::from),
        Err(_elapsed) => anyhow::bail!("offset commit timed out after {COMMIT_TIMEOUT:?}"),
    }
}

/// Hand a snapshot to the writer task. Never awaits: if the writer is still busy
/// with an older snapshot, this one is dropped and the next one (5s later, with
/// a newer sequence) supersedes it.
fn request_snapshot(
    ctx: &NodeContext,
    shard_id: u32,
    engine: &ShardEngine,
    sequence: &mut u64,
    sender: &mpsc::Sender<SequencedSnapshot>,
) {
    *sequence += 1;
    let request = SequencedSnapshot {
        sequence: *sequence,
        snapshot: engine.snapshot_state(),
    };
    match sender.try_send(request) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            warn!(
                shard = shard_id,
                sequence = *sequence,
                "snapshot writer is behind; skipped this snapshot"
            );
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            warn!(
                shard = shard_id,
                "snapshot writer stopped; engine state is no longer being persisted"
            );
            ctx.mark_shard_failed();
        }
    }
}

/// Owns the snapshot file. `save` is blocking, so the whole writer runs on a
/// blocking thread and the shard loop only ever hands over an owned
/// `ShardSnapshot`.
fn spawn_snapshot_writer(
    ctx: Arc<NodeContext>,
    shard_id: u32,
    mut pending: mpsc::Receiver<SequencedSnapshot>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        let mut writer = SnapshotWriter::new(ctx.snapshots.clone());
        while let Some(request) = pending.blocking_recv() {
            let sequence = request.sequence;
            // Diagnostics are built here, off the shard loop.
            let hashes: Vec<String> = request
                .snapshot
                .symbols
                .iter()
                .map(|symbol| hex(&symbol.state_hash))
                .collect();
            let started = Instant::now();
            let result = writer.write(request);
            // `Metrics` has no snapshot-duration recorder and `context.rs` is
            // owned elsewhere, so the duration is logged rather than recorded.
            // TODO(lead): add a `snapshot_duration_us` histogram to `Metrics`.
            let duration_us = started.elapsed().as_micros() as u64;
            match result {
                Ok(Some(path)) => info!(
                    shard = shard_id,
                    sequence,
                    path = %path.display(),
                    hashes = ?hashes,
                    duration_us,
                    "snapshot written"
                ),
                Ok(None) => warn!(
                    shard = shard_id,
                    sequence, "snapshot is older than the last one written; skipped"
                ),
                Err(error) => {
                    warn!(
                        shard = shard_id,
                        sequence,
                        error = %error,
                        duration_us,
                        "snapshot write failed"
                    );
                    ctx.mark_shard_failed();
                }
            }
        }
    })
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

// Executions and market-data events carry routing headers only: consumers decode
// the fixed-layout payload, which is canonical, and never need `venue`,
// `trace_id` or `codec`. Orders and MD snapshots keep the full header set.
fn execution_message(execution: &ExecutionReport) -> JournalMessage {
    let meta = MessageMeta::new(
        execution.symbol,
        execution.seq,
        "execution",
        SCHEMA_EXECUTION,
        u128::from(execution.cause_offset),
    );
    JournalMessage::new(execution.encode(), meta.routing_pairs())
}

fn md_message(event: &MdEvent) -> JournalMessage {
    let meta = MessageMeta::new(
        event.symbol,
        event.seq,
        "md",
        SCHEMA_MD_EVENT,
        u128::from(event.cause_offset),
    );
    JournalMessage::new(event.encode(), meta.routing_pairs())
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

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: u64 = COMMIT_INTERVAL_RECORDS;

    #[test]
    fn commit_waits_for_the_record_interval() {
        // Below the interval and inside the delay window: not worth a round trip.
        assert!(!commit_due(
            Some(0),
            Some(0),
            Duration::ZERO,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
        assert!(!commit_due(
            Some(INTERVAL - 1),
            Some(0),
            Duration::ZERO,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
        assert!(commit_due(
            Some(INTERVAL),
            Some(0),
            Duration::ZERO,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
    }

    #[test]
    fn commit_waits_for_the_max_delay() {
        assert!(!commit_due(
            Some(1),
            Some(0),
            COMMIT_MAX_DELAY - Duration::from_millis(1),
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
        assert!(commit_due(
            Some(1),
            Some(0),
            COMMIT_MAX_DELAY,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
    }

    #[test]
    fn commit_is_skipped_when_nothing_is_pending() {
        let long = Duration::from_secs(60);
        assert!(!commit_due(None, None, long, INTERVAL, COMMIT_MAX_DELAY));
        assert!(!commit_due(
            Some(7),
            Some(7),
            long,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
        // A committed offset ahead of the applied one never triggers a commit.
        assert!(!commit_due(
            Some(7),
            Some(9),
            long,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
    }

    #[test]
    fn commit_without_a_prior_offset_counts_every_applied_record() {
        assert!(!commit_due(
            Some(INTERVAL - 2),
            None,
            Duration::ZERO,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
        assert!(commit_due(
            Some(INTERVAL - 1),
            None,
            Duration::ZERO,
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
    }

    #[test]
    fn watermark_tracks_applied_and_committed() {
        let mut watermark = CommitWatermark::new(Some(10));
        assert!(watermark.pending().is_none());

        watermark.note_applied(11);
        assert_eq!(watermark.pending(), Some(11));

        let now = Instant::now();
        watermark.mark_committed(11, now);
        assert!(watermark.pending().is_none());
        assert!(!watermark.due(now));

        // A stale commit must not move the watermark backwards.
        watermark.mark_committed(5, now);
        assert_eq!(watermark.committed, Some(11));

        watermark.note_applied(12);
        assert_eq!(watermark.pending(), Some(12));
    }

    #[test]
    fn watermark_is_due_after_the_delay() {
        let mut watermark = CommitWatermark::new(None);
        let now = Instant::now();
        watermark.note_applied(1);
        assert!(!watermark.due(now));
        assert!(watermark.due(now + COMMIT_MAX_DELAY));
    }

    #[test]
    fn retry_backoff_is_bounded_and_throttles_warnings() {
        let mut retry = CommitRetry::new();
        let start = Instant::now();
        assert!(retry.ready(start));

        assert!(retry.failed(start), "the first failure is logged");
        assert!(!retry.ready(start), "the retry waits for the backoff");

        // Within the warning window: silent, but the backoff still doubles.
        let early = start + Duration::from_millis(10);
        assert!(!retry.failed(early));
        assert!(!retry.ready(early + COMMIT_RETRY_INITIAL));

        // Once the window passes, one more warning is allowed.
        let later = start + COMMIT_WARN_INTERVAL;
        assert!(retry.failed(later));

        // The backoff never exceeds the cap.
        let mut delay = retry.delay;
        let mut now = later;
        for _ in 0..16 {
            retry.failed(now);
            delay = retry.delay;
            now += Duration::from_secs(10);
        }
        assert!(delay <= COMMIT_RETRY_MAX);

        retry.succeeded();
        assert_eq!(retry.delay, COMMIT_RETRY_INITIAL);
        assert!(retry.ready(Instant::now()));
    }
}
