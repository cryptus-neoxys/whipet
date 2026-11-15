// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{error, info, warn};
use whippet_core::headers::{
    HeaderMode, MessageMeta, SCHEMA_EXECUTION, SCHEMA_MD_EVENT, SCHEMA_MD_SNAPSHOT,
};
use whippet_core::protocol::{
    Command, ExecKind, ExecutionReport, MdEvent, MdKind, MdSnapshot, RejectReason,
};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{AppendResult, Journal, JournalMessage, PayloadRecord, Topic};
use whippet_marketdata::{ShardMetrics, Stage};

use crate::context::NodeContext;
use crate::snapshot::{SequencedSnapshot, SnapshotWriter};
use crate::turbo::{TurboAck, TurboAdmission, TurboEntry};

/// How many records a shard applies between consumer-offset commits.
const COMMIT_INTERVAL_RECORDS: u64 = 256;

/// Upper bound on how long the consumer offset may lag the applied offset.
const COMMIT_MAX_DELAY: Duration = Duration::from_millis(100);

/// A single commit attempt is bounded so a wedged broker cannot stall the loop.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(2);

/// An output append is retried this many times before the shard fails. Without
/// a bound, a wedged output topic stalls matching while the gateway keeps
/// acknowledging new orders.
#[doc(hidden)]
pub const APPEND_MAX_ATTEMPTS: u32 = 30;

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

/// Turbo write-behind: how many order records one append call carries. A
/// single request stays bounded even when the window hands over a full slice,
/// and the shard has exactly one writer, so chunk order is append order.
const ORDER_APPEND_CHUNK: usize = 256;

/// Consumer name used only by the startup probe that asks whether the orders
/// partition already holds records.
const TURBO_ANCHOR_PROBE_CONSUMER: &str = "whippet-turbo-anchor";

/// Batches buffered between the reader and the matcher, and between the matcher
/// and the writer. Small on purpose: this is the backpressure boundary.
#[doc(hidden)]
pub const SHARD_RING_CAPACITY: usize = 2;

/// Consecutive poll failures before the shard fails closed. At 250 ms per
/// retry this is ~10 s, long enough to ride a broker restart, short enough
/// that ingress stops acknowledging orders that cannot be matched.
const POLL_FAILURE_BUDGET: u32 = 40;

/// Minimum spacing between engine-state snapshots. The snapshot is bounded by
/// the dedup window, but building it still clones the book and the dedup vec on
/// the match thread, so the interval trades tail spikes against the replay
/// window. Defaults to 30 s and is configurable through
/// [`SNAPSHOT_MIN_INTERVAL_ENV`], clamped to
/// [`SNAPSHOT_MIN_INTERVAL_FLOOR_SECS`] .. [`SNAPSHOT_MIN_INTERVAL_CEILING_SECS`].
const SNAPSHOT_MIN_INTERVAL_DEFAULT_SECS: u64 = 30;
const SNAPSHOT_MIN_INTERVAL_FLOOR_SECS: u64 = 5;
const SNAPSHOT_MIN_INTERVAL_CEILING_SECS: u64 = 300;

/// Environment override for [`SNAPSHOT_MIN_INTERVAL_DEFAULT_SECS`].
#[doc(hidden)]
pub const SNAPSHOT_MIN_INTERVAL_ENV: &str = "WHIPPET_SNAPSHOT_MIN_INTERVAL_SECS";

/// The configured period between periodic engine-state snapshots.
///
/// Read once per shard at startup. A value outside the floor/ceiling is
/// clamped rather than rejected: the interval changes how often the tail is
/// paid, not whether the node is correct, and a deployment should not fail to
/// boot over it.
fn snapshot_min_interval() -> Duration {
    snapshot_min_interval_from(
        std::env::var(SNAPSHOT_MIN_INTERVAL_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok()),
    )
}

/// Pure form of [`snapshot_min_interval`], testable without the environment.
fn snapshot_min_interval_from(secs: Option<u64>) -> Duration {
    Duration::from_secs(secs.unwrap_or(SNAPSHOT_MIN_INTERVAL_DEFAULT_SECS).clamp(
        SNAPSHOT_MIN_INTERVAL_FLOOR_SECS,
        SNAPSHOT_MIN_INTERVAL_CEILING_SECS,
    ))
}

/// The node gives each shard task five seconds to stop; the final snapshot flush
/// has to fit inside that budget.
const FINAL_SNAPSHOT_SEND_TIMEOUT: Duration = Duration::from_millis(1_000);
const FINAL_SNAPSHOT_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

/// How many matcher output batches the writer merges into one set of appends.
///
/// The matcher hands over a batch every time the input ring wakes, which at
/// batch-10 ingress is a ten-order group. Appending once per group makes the
/// write-behind the bottleneck and fills the turbo window on a burst. Draining
/// what is already queued amortizes the round trip the same way durable group
/// commit does; the cap bounds the writer's memory and the commit delay.
const WRITER_COALESCE_BATCHES: usize = 8;

/// The journal operations a shard performs.
///
/// The shard loop is written against this trait so the failure paths — an
/// append that fails once, an append that never succeeds, a reader that returns
/// a non-contiguous batch — can be exercised without a broker. [`Journal`]
/// implements it by forwarding to its inherent methods, so production behaviour
/// is unchanged.
///
/// The methods return `impl Future + Send` rather than being `async fn` so the
/// trait's futures are known to be `Send` and the shard can be spawned.
#[doc(hidden)]
pub trait JournalOps {
    fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> impl Future<Output = whippet_journal::Result<AppendResult>> + Send;

    fn commit(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
    ) -> impl Future<Output = whippet_journal::Result<()>> + Send;

    fn committed(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
    ) -> impl Future<Output = whippet_journal::Result<Option<u64>>> + Send;

    #[allow(clippy::too_many_arguments)]
    fn read_payloads_into<'a>(
        &'a self,
        topic: Topic,
        partition: u32,
        consumer: &'a str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
        out: &'a mut Vec<PayloadRecord>,
    ) -> impl Future<Output = whippet_journal::Result<usize>> + Send + 'a;

    fn is_iggy(&self) -> bool;

    fn ping(&self) -> impl Future<Output = bool> + Send;
}

#[doc(hidden)]
impl JournalOps for Journal {
    fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> impl Future<Output = whippet_journal::Result<AppendResult>> + Send {
        Journal::append(self, topic, partition, messages)
    }

    fn commit(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
        offset: u64,
    ) -> impl Future<Output = whippet_journal::Result<()>> + Send {
        Journal::commit(self, topic, partition, consumer, offset)
    }

    fn committed(
        &self,
        topic: Topic,
        partition: u32,
        consumer: &str,
    ) -> impl Future<Output = whippet_journal::Result<Option<u64>>> + Send {
        Journal::committed(self, topic, partition, consumer)
    }

    #[allow(clippy::too_many_arguments)]
    fn read_payloads_into<'a>(
        &'a self,
        topic: Topic,
        partition: u32,
        consumer: &'a str,
        offset: u64,
        count: u32,
        max_wait_ms: u64,
        out: &'a mut Vec<PayloadRecord>,
    ) -> impl Future<Output = whippet_journal::Result<usize>> + Send + 'a {
        Journal::read_payloads_into(
            self,
            topic,
            partition,
            consumer,
            offset,
            count,
            max_wait_ms,
            out,
        )
    }

    fn is_iggy(&self) -> bool {
        Journal::is_iggy(self)
    }

    fn ping(&self) -> impl Future<Output = bool> + Send {
        Journal::ping(self)
    }
}

/// Owns one partition's books. The single writer is this task; the engine is a
/// pure state machine and never awaits.
///
/// `turbo` is the shard's ingress lane in turbo mode and `None` in durable
/// mode. The durable path is unchanged: `read_orders` polls the orders log and
/// the matcher applies decoded records. In turbo the matcher is fed from the
/// lane and is the only sequencer; the writer appends the commands to the
/// orders log behind the match.
pub async fn run_engine_shard(
    ctx: Arc<NodeContext>,
    shard_id: u32,
    turbo: Option<mpsc::Receiver<TurboEntry>>,
) -> Result<()> {
    // Mode is fixed at startup and never falls back: a turbo node whose shard
    // lost its lane must stop, not quietly start reading the log as durable
    // after the gateway already acknowledged matches on process-local state.
    if ctx.turbo.is_some() && turbo.is_none() {
        anyhow::bail!(
            "turbo mode is configured but shard {shard_id} has no ingress lane; refusing to fall back to the durable reader"
        );
    }
    let exchange = ctx.exchange();
    let loaded = ctx
        .snapshots
        .load(shard_id)
        .with_context(|| format!("loading snapshot for shard {shard_id}"))?;
    let snapshot_last_offset = loaded.as_ref().and_then(resume_offset);
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
    // A shard's journal handle is cloned once and threaded through the reader
    // and writer explicitly, so the journal edge can be substituted in tests.
    let journal = Arc::clone(ctx.engine_journal_for(shard_id));
    // Read the broker's committed offset even when a snapshot exists. The
    // snapshot is authoritative for the resume point, but the watermark must
    // start at the highest acknowledged offset so a crash during replay cannot
    // move the committed offset backwards.
    let journal_committed = match journal.committed(Topic::Orders, shard_id, &consumer).await {
        Ok(offset) => offset,
        Err(error) => {
            anyhow::bail!(
                "could not read committed offset for shard {shard_id}: {error}; refusing to replay from zero"
            );
        }
    };
    // No snapshot means the engine state is empty, so the only correct resume
    // point is the start of the log. Starting at the committed offset would
    // skip every order before it and diverge from a clean replay.
    // A snapshot ahead of the log head cannot be a resume point: the records
    // it claims are not in this partition. Fail before the reader waits on a
    // tail that will never arrive.
    if let Some(snapshot_offset) = snapshot_last_offset {
        let head = journal.lag(Topic::Orders, shard_id, 0).await.unwrap_or(0);
        if snapshot_offset > head {
            anyhow::bail!(
                "snapshot at offset {snapshot_offset} is ahead of the order log head {head} for shard {shard_id}; reset or resnapshot"
            );
        }
    }
    // Turbo anchors the local seq domain on the orders log: the write-behind
    // appends exactly one order record per seq from an empty log, so the seq
    // the matcher assigns equals the broker offset of the same command. That
    // anchor exists only when the partition started empty, or when a snapshot
    // already carries the seq domain forward (the check above). A non-empty
    // log without either would let this boot assign seqs over records it is
    // not replaying, duplicating cause_offsets; that is a startup error, never
    // a silent seq reset.
    if turbo.is_some() && snapshot_last_offset.is_none() {
        let head = journal.lag(Topic::Orders, shard_id, 0).await.unwrap_or(0);
        // `head` alone cannot tell an empty partition from one record at
        // offset 0 on either backend, so ask for offset 0 explicitly. The head
        // still matters: it catches a log whose first records retention has
        // already removed.
        let mut probe = Vec::new();
        let first = journal
            .read_payloads_into(
                Topic::Orders,
                shard_id,
                TURBO_ANCHOR_PROBE_CONSUMER,
                0,
                1,
                0,
                &mut probe,
            )
            .await
            .with_context(|| format!("probing the orders log for shard {shard_id}"))?;
        if first > 0 || head > 0 {
            anyhow::bail!(
                "turbo cannot anchor the local seq domain on a non-empty orders log (shard {shard_id}, head {head}); reset or restore a snapshot"
            );
        }
    }
    // A turbo restart with a snapshot behind the log head must replay the
    // tail before it accepts ingress. Otherwise the engine would start at the
    // snapshot while records it has not seen already sit above it, and the
    // write-behind would append duplicate seqs over them. The replay uses the
    // broker offsets directly: the turbo seq domain is anchored to the log, so
    // seq == offset.
    let mut resume_offset = snapshot_last_offset;
    if turbo.is_some()
        && let Some(snapshot_offset) = snapshot_last_offset
    {
        let head = journal.lag(Topic::Orders, shard_id, 0).await.unwrap_or(0);
        if head > snapshot_offset {
            replay_turbo_tail(
                journal.as_ref(),
                shard_id,
                &mut engine,
                snapshot_offset.saturating_add(1),
                head,
            )
            .await?;
            info!(
                shard = shard_id,
                from = snapshot_offset.saturating_add(1),
                to = head,
                "replayed the journaled turbo tail before accepting ingress"
            );
            resume_offset = Some(head);
        }
    }

    let next_offset = match resume_offset {
        Some(offset) => offset.saturating_add(1),
        None => 0,
    };
    let committed_offset = match (resume_offset, journal_committed) {
        (Some(snapshot), Some(journal)) => Some(snapshot.max(journal)),
        (Some(snapshot), None) => Some(snapshot),
        (None, journal) => journal,
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
        let mut startup_shutdown = ctx.shutdown.subscribe();
        if let Err(error) = append_with_retry(
            journal.as_ref(),
            Topic::Md,
            shard_id,
            &messages,
            &mut startup_shutdown,
        )
        .await
        {
            warn!(shard = shard_id, error = %error, "initial MD snapshot was not published");
        }
    }

    info!(
        shard = shard_id,
        start_offset = next_offset,
        consumer,
        "engine shard running"
    );
    // --- Dedicated shard thread and bounded rings ---------------------------
    //
    // The matcher owns the engine on a plain OS thread. It never awaits and
    // never calls the journal: `read_orders` fills the input ring and
    // `run_writer` drains the output ring. Ring capacity is the backpressure
    // boundary; a full ring blocks the producer instead of growing memory.
    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    let (output_tx, output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    // The matcher hands the polled buffer back so the reader can refill it
    // instead of allocating a Vec per batch.
    let (recycle_tx, recycle_rx) =
        mpsc::channel::<Vec<whippet_journal::PayloadRecord>>(SHARD_RING_CAPACITY);
    let (snapshot_tx, snapshot_rx) = mpsc::channel::<SnapshotJob>(SNAPSHOT_QUEUE_CAPACITY);
    let snapshot_writer = spawn_snapshot_writer(Arc::clone(&ctx), shard_id, snapshot_rx);
    // Highest offset whose executions/MD are actually in the journal. A
    // periodic snapshot is only taken once this catches up with the matcher,
    // otherwise the snapshot could claim an offset whose outputs were lost.
    // Highest offset whose outputs are durable; `u64::MAX` means "nothing
    // durable yet", so the snapshot gate cannot open at offset 0 before the
    // first append.
    let appended_offset = Arc::new(AtomicU64::new(committed_offset.unwrap_or(u64::MAX)));

    // Queued batch counts for the ring-occupancy metric. The producer
    // increments after a send succeeds and the consumer decrements after it
    // receives, so the shared `Relaxed` atomic is the live ring depth; the
    // matcher reads both once a second, never on the send/recv path.
    let input_queued = Arc::new(AtomicU32::new(0));
    let output_queued = Arc::new(AtomicU32::new(0));

    // The input edge is the only mode-specific part of the shard. Durable polls
    // the orders log; turbo batches the admitted lane entries. Both hand the
    // matcher an `InputBatch` through the same bounded ring, so the ring stays
    // the single backpressure boundary and the shutdown handshake (producer
    // drops its sender -> matcher builds the final snapshot) is unchanged.
    let reader = match turbo {
        Some(turbo_rx) => {
            // The write-behind owns the orders log; the reader's buffer
            // recycler has no user in this mode.
            drop(recycle_rx);
            tokio::spawn(feed_turbo(
                turbo_rx,
                input_tx,
                Arc::clone(&input_queued),
                ctx.shutdown.subscribe(),
            ))
        }
        None => tokio::spawn(read_orders(
            Arc::clone(&ctx),
            Arc::clone(&journal),
            shard_id,
            consumer.clone(),
            next_offset,
            input_tx,
            recycle_rx,
            ctx.shutdown.subscribe(),
            Arc::clone(&input_queued),
        )),
    };

    let matcher_ctx = Arc::clone(&ctx);
    let matcher_appended = Arc::clone(&appended_offset);
    let matcher_output_queued = Arc::clone(&output_queued);
    let matcher = std::thread::Builder::new()
        .name(format!("whippet-shard-{shard_id}"))
        .spawn(move || {
            run_matcher(
                matcher_ctx,
                shard_id,
                engine,
                input_rx,
                output_tx,
                recycle_tx,
                matcher_appended,
                next_offset.saturating_sub(1),
                next_offset,
                input_queued,
                matcher_output_queued,
            )
        })
        .with_context(|| format!("spawning matcher thread for shard {shard_id}"))?;
    let result = run_writer(
        Arc::clone(&ctx),
        journal.as_ref(),
        shard_id,
        consumer,
        committed_offset,
        output_rx,
        snapshot_tx,
        snapshot_writer,
        Arc::clone(&appended_offset),
        output_queued,
    )
    .await;

    // On shutdown the reader exits on the watch and drops its sender, which
    // closes the matcher input; the matcher sends its final snapshot and exits.
    // Abort is a safety net for a writer that stopped first.
    reader.abort();
    join_matcher(&ctx, shard_id, matcher)?;
    result
}

/// Join the matcher thread and fail the shard if it panicked.
///
/// Split out of [`run_engine_shard`] so the orchestration's panic path can be
/// tested without a broker: the caller gets an error instead of a silent exit.
#[doc(hidden)]
pub fn join_matcher(
    ctx: &NodeContext,
    shard_id: u32,
    matcher: std::thread::JoinHandle<()>,
) -> Result<()> {
    if matcher.join().is_err() {
        ctx.mark_shard_failed();
        anyhow::bail!("matcher thread for shard {shard_id} panicked");
    }
    Ok(())
}

/// Resume offset for a snapshot, if it is a real resume point.
///
/// A snapshot from a shard that never applied a command has none. Treating its
/// `last_offset: 0` as one would make the next boot start at offset 1 and
/// silently skip the first order.
pub(crate) fn resume_offset(snapshot: &whippet_engine::ShardSnapshot) -> Option<u64> {
    snapshot
        .symbols
        .iter()
        .filter(|symbol| symbol.commands > 0)
        .map(|symbol| symbol.last_offset)
        .max()
}

/// One poll's worth of shard input, handed to the matcher thread.
///
/// Replay the journaled commands a turbo snapshot does not cover.
///
/// Runs before the matcher thread starts and before the turbo lane is read, so
/// every command the log already contains is applied exactly once, in offset
/// order, with the same `engine.process` call the live path uses. Outputs are
/// discarded: they were appended before the crash and replay must not append
/// them again. A gap (fewer records than the head promises) is fatal.
async fn replay_turbo_tail<J: JournalOps + ?Sized>(
    journal: &J,
    shard_id: u32,
    engine: &mut ShardEngine,
    mut offset: u64,
    head: u64,
) -> Result<()> {
    let consumer = format!("whippet-turbo-replay-p{shard_id}");
    let mut records: Vec<whippet_journal::PayloadRecord> = Vec::with_capacity(BATCH_CAPACITY);
    let mut out = EngineOutput::with_capacity(BATCH_CAPACITY, BATCH_CAPACITY);
    while offset <= head {
        records.clear();
        let read = journal
            .read_payloads_into(
                Topic::Orders,
                shard_id,
                &consumer,
                offset,
                BATCH_CAPACITY as u32,
                0,
                &mut records,
            )
            .await
            .with_context(|| {
                format!("replaying turbo tail at offset {offset} for shard {shard_id}")
            })?;
        if read == 0 {
            break;
        }
        for record in &records {
            if record.offset > head {
                break;
            }
            match Command::decode(&record.payload) {
                Ok(command) => {
                    if let Err(error) = engine.process(&command, record.offset, &mut out) {
                        anyhow::bail!(
                            "turbo tail replay rejected offset {} for shard {shard_id}: {error}",
                            record.offset
                        );
                    }
                    out.clear();
                }
                Err(error) => {
                    warn!(
                        shard = shard_id,
                        offset = record.offset,
                        error = %error,
                        "skipping undecodable order during turbo tail replay"
                    );
                }
            }
            offset = record.offset.saturating_add(1);
        }
    }
    if offset <= head {
        anyhow::bail!(
            "turbo tail replay ended at offset {} before the log head {head} for shard {shard_id}; the log has a gap",
            offset.saturating_sub(1)
        );
    }
    Ok(())
}

/// A shard runs one mode for its lifetime, so `records` and `turbo` never mix:
/// durable batches carry decoded order-log records, turbo batches carry admitted
/// lane entries. The two constructors keep that invariant visible at every call
/// site.
#[doc(hidden)]
pub struct InputBatch {
    pub records: Vec<PayloadRecord>,
    pub turbo: Vec<TurboEntry>,
}

impl InputBatch {
    /// A durable-path batch from the orders log.
    pub fn durable(records: Vec<PayloadRecord>) -> Self {
        Self {
            records,
            turbo: Vec::new(),
        }
    }

    /// A turbo batch from the shard's ingress lane.
    pub fn turbo(entries: Vec<TurboEntry>) -> Self {
        Self {
            records: Vec::new(),
            turbo: entries,
        }
    }
}

/// A snapshot handed to the writer, with whether to pay for the full rebuild.
#[doc(hidden)]
pub struct SnapshotJob {
    pub request: SequencedSnapshot,
    pub verify: bool,
    /// When the matcher built and queued this snapshot. The writer reports the
    /// handoff delay so a queue backlog is not mistaken for write cost.
    pub queued_at: Instant,
}

/// One turbo command to journal, plus the window claim it holds.
///
/// The admission stays attached until the writer has appended the order record
/// and both output topics, so a command counts against the uncommitted bound
/// from admission until it is durable. Dropping it early would let ingress
/// admit work for a window that has not drained.
#[doc(hidden)]
pub struct TurboJournalRecord {
    pub message: JournalMessage,
    pub admission: TurboAdmission,
}

/// One batch of matcher output, handed to the async writer.
#[doc(hidden)]
pub struct OutputBatch {
    pub executions: Vec<JournalMessage>,
    pub md: Vec<JournalMessage>,
    /// Turbo only: commands to append to `Topic::Orders`, in seq order, each
    /// holding its window permit until that append lands.
    pub orders: Vec<TurboJournalRecord>,
    pub last_offset: u64,
    pub snapshot: Option<SequencedSnapshot>,
    /// The matcher is exiting: flush the final snapshot and commit.
    pub final_batch: bool,
}

/// Poll the orders log and feed the matcher. This is the only task that reads
/// the orders topic, so the consumer offset advances in one place.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn read_orders<J: JournalOps + 'static>(
    ctx: Arc<NodeContext>,
    journal: Arc<J>,
    shard_id: u32,
    consumer: String,
    start_offset: u64,
    sender: mpsc::Sender<InputBatch>,
    mut recycle: mpsc::Receiver<Vec<whippet_journal::PayloadRecord>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    input_queued: Arc<AtomicU32>,
) {
    let mut next_offset = start_offset;
    let mut poll_failures: u32 = 0;
    loop {
        // Reuse the buffer the matcher returned; `read_payloads_into` clears it.
        let mut records = recycle.try_recv().unwrap_or_default();
        let result = tokio::select! {
            _ = shutdown.changed() => break,
            result = journal.read_payloads_into(
                Topic::Orders,
                shard_id,
                &consumer,
                next_offset,
                BATCH_CAPACITY as u32,
                50,
                &mut records,
            ) => result,
        };
        let count = match result {
            Ok(count) => count,
            Err(error) => {
                poll_failures += 1;
                warn!(
                    shard = shard_id,
                    attempt = poll_failures,
                    error = %error,
                    "order poll failed; retrying"
                );
                if poll_failures >= POLL_FAILURE_BUDGET {
                    ctx.mark_shard_failed();
                    error!(
                        shard = shard_id,
                        attempts = poll_failures,
                        "order poll failed persistently; shard stopped so ingress can fail closed"
                    );
                    break;
                }
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        poll_failures = 0;
        if count == 0 {
            if !journal.is_iggy() {
                sleep(Duration::from_millis(1)).await;
            }
            continue;
        }
        // Validate the whole batch, not just its first record: an interior
        // gap would otherwise be skipped and the engine would silently miss an
        // order.
        if let (Some(first), Some(last)) = (records.first(), records.last()) {
            let span = last.offset.saturating_sub(first.offset).saturating_add(1);
            if first.offset != next_offset || span != records.len() as u64 {
                ctx.mark_shard_failed();
                error!(
                    shard = shard_id,
                    requested = next_offset,
                    first = first.offset,
                    last = last.offset,
                    count = records.len(),
                    "order log gap or overlap; a snapshot or reset is required"
                );
                break;
            }
        }
        next_offset = records
            .last()
            .map(|record| record.offset.saturating_add(1))
            .unwrap_or(next_offset);
        if sender.send(InputBatch::durable(records)).await.is_err() {
            break;
        }
        // Ring occupancy: one queued batch more for the matcher to receive.
        input_queued.fetch_add(1, Ordering::Relaxed);
    }
}

/// Collect admitted turbo entries from one shard's lane and hand them to the
/// matcher through the same input ring `read_orders` uses.
///
/// This is the reader's mirror, not a second matcher: it batches whatever has
/// already been admitted (each entry holds a window permit before it is queued)
/// and awaits the ring, so the ring is the backpressure boundary in both modes.
/// On shutdown it stops taking new work and drops the sender, which closes the
/// matcher's input exactly as the durable reader does.
#[doc(hidden)]
pub async fn feed_turbo(
    mut turbo: mpsc::Receiver<TurboEntry>,
    sender: mpsc::Sender<InputBatch>,
    input_queued: Arc<AtomicU32>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let mut entries: Vec<TurboEntry> = Vec::with_capacity(BATCH_CAPACITY);
        tokio::select! {
            _ = shutdown.changed() => break,
            received = turbo.recv() => match received {
                Some(entry) => entries.push(entry),
                // Every ingress sender is gone; the node is stopping.
                None => break,
            },
        }
        while entries.len() < BATCH_CAPACITY {
            match turbo.try_recv() {
                Ok(entry) => entries.push(entry),
                Err(_) => break,
            }
        }
        if sender.send(InputBatch::turbo(entries)).await.is_err() {
            break;
        }
        // Ring occupancy: one queued batch more for the matcher to receive.
        input_queued.fetch_add(1, Ordering::Relaxed);
    }
}

/// The matcher: a pure, blocking loop that owns the engine. No await, no I/O,
/// no lock. It records match latency and builds output messages, then hands
/// the batch to the writer.
///
/// In durable mode the record's log offset is the engine's per-record offset.
/// In turbo the matcher is the only sequencer: `start_turbo_seq` is the first
/// local seq (0 on an empty log, `snapshot_last_offset + 1` after a restore)
/// and every turbo command consumes exactly one seq, in arrival order.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub fn run_matcher(
    ctx: Arc<NodeContext>,
    shard_id: u32,
    mut engine: ShardEngine,
    mut input: mpsc::Receiver<InputBatch>,
    output: mpsc::Sender<OutputBatch>,
    recycle: mpsc::Sender<Vec<whippet_journal::PayloadRecord>>,
    appended_offset: Arc<AtomicU64>,
    start_last_offset: u64,
    start_turbo_seq: u64,
    input_queued: Arc<AtomicU32>,
    output_queued: Arc<AtomicU32>,
) {
    let mut out = EngineOutput::with_capacity(BATCH_CAPACITY, BATCH_CAPACITY);
    // Per-shard recorder: no global lock on the match path. It is merged into
    // the global metrics once a second, and once on exit.
    let shard_metrics = ShardMetrics::new();
    let mut last_merge = Instant::now();
    let mut snapshot_sequence: u64 = 0;
    let mut last_snapshot_at = Instant::now();
    let mut last_offset = start_last_offset;
    let mut next_turbo_seq = start_turbo_seq;
    let snapshot_interval = snapshot_min_interval();
    info!(
        shard = shard_id,
        interval_secs = snapshot_interval.as_secs(),
        "engine-state snapshot cadence"
    );
    // A snapshot that came due while the writer was behind. It is taken at the
    // next batch boundary where every output up to `last_offset` is appended.
    let mut snapshot_pending = false;

    while let Some(batch) = input.blocking_recv() {
        // The reader counted this batch when its send succeeded; the ring
        // occupancy drops by one as soon as the matcher holds it.
        input_queued.fetch_sub(1, Ordering::Relaxed);
        let mut executions: Vec<JournalMessage> = Vec::with_capacity(BATCH_CAPACITY);
        let mut md_messages: Vec<JournalMessage> = Vec::with_capacity(BATCH_CAPACITY);
        let mut orders: Vec<TurboJournalRecord> = Vec::with_capacity(BATCH_CAPACITY);
        let mut snapshot_due = false;
        let mut process_us: u64 = 0;
        // If a snapshot was deferred, this boundary is a candidate: the state
        // is still at `last_offset`, which the writer may now have caught up to.
        let durable = appended_offset.load(Ordering::Acquire);
        let mut snapshot = if snapshot_pending
            && durable != u64::MAX
            && durable >= last_offset
            && last_snapshot_at.elapsed() >= snapshot_interval
        {
            snapshot_pending = false;
            last_snapshot_at = Instant::now();
            snapshot_sequence += 1;
            Some(build_snapshot(
                shard_id,
                &mut engine,
                snapshot_sequence,
                "deferred",
            ))
        } else {
            None
        };

        for record in &batch.records {
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
                        let elapsed = started.elapsed().as_micros() as u64;
                        process_us = process_us.saturating_add(elapsed);
                        shard_metrics.record_match_latency_us(elapsed);
                        shard_metrics.record_engine_command();
                        for execution in &out.executions {
                            shard_metrics.record_execution();
                            if matches!(execution.kind, ExecKind::Accepted | ExecKind::Rejected) {
                                shard_metrics
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
            last_offset = record.offset;
        }

        // Turbo input: the command is already typed and its journal message is
        // already built, so the matcher assigns the seq and processes it exactly
        // as it would a decoded durable record. The ack goes out before the
        // writer sees anything: turbo acknowledges the match, not the journal.
        for entry in batch.turbo {
            let seq = next_turbo_seq;
            next_turbo_seq = next_turbo_seq.saturating_add(1);
            let started = Instant::now();
            let ack = match engine.process(&entry.command, seq, &mut out) {
                Ok(()) => {
                    let elapsed = started.elapsed().as_micros() as u64;
                    process_us = process_us.saturating_add(elapsed);
                    shard_metrics.record_match_latency_us(elapsed);
                    shard_metrics.record_engine_command();
                    for execution in &out.executions {
                        shard_metrics.record_execution();
                        if matches!(execution.kind, ExecKind::Accepted | ExecKind::Rejected) {
                            shard_metrics
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
                    turbo_ack(seq, &entry.command, &out)
                }
                Err(error) => {
                    // A command routed to the wrong shard: no state changed and
                    // no execution was emitted. The command is still journaled
                    // at its assigned seq so the log keeps one record per seq;
                    // the client is told the match did not happen.
                    warn!(
                        shard = shard_id,
                        seq,
                        symbol = %entry.command.symbol,
                        error = %error,
                        "engine rejected a turbo command for this shard"
                    );
                    TurboAck {
                        seq,
                        accepted: false,
                        reject: RejectReason::Internal,
                    }
                }
            };
            // A client that gave up closed the receiver; the command was still
            // matched and is still journaled, so the shard continues.
            let _ = entry.ack.send(ack);
            orders.push(TurboJournalRecord {
                message: entry.message,
                admission: entry.admission,
            });
            last_offset = seq;
        }

        if snapshot.is_none() && snapshot_due {
            let durable = appended_offset.load(Ordering::Acquire);
            if durable != u64::MAX
                && durable >= last_offset
                && last_snapshot_at.elapsed() >= snapshot_interval
            {
                last_snapshot_at = Instant::now();
                snapshot_sequence += 1;
                snapshot = Some(build_snapshot(
                    shard_id,
                    &mut engine,
                    snapshot_sequence,
                    "cadence",
                ));
            } else {
                snapshot_pending = true;
            }
        }

        // One stage sample per batch, not per command: the global stage
        // histogram is not on the per-command path.
        ctx.metrics
            .record_stage_us(Stage::EngineProcess, process_us);
        // Hand the polled buffer back for reuse.
        let _ = recycle.try_send(batch.records);
        // One global merge per second, not one lock per command. The ring
        // gauges ride the same cadence: the reader/matcher/writer maintain
        // the queued counts, the matcher just reads and publishes them.
        if last_merge.elapsed() >= Duration::from_secs(1) {
            shard_metrics.merge_into(&ctx.metrics);
            ctx.metrics.set_ring_depths(
                shard_id,
                input_queued.load(Ordering::Relaxed),
                output_queued.load(Ordering::Relaxed),
                SHARD_RING_CAPACITY as u32,
            );
            last_merge = Instant::now();
        }

        if output
            .blocking_send(OutputBatch {
                executions,
                md: md_messages,
                orders,
                last_offset,
                snapshot,
                final_batch: false,
            })
            .is_err()
        {
            shard_metrics.merge_into(&ctx.metrics);
            ctx.metrics.set_ring_depths(
                shard_id,
                input_queued.load(Ordering::Relaxed),
                output_queued.load(Ordering::Relaxed),
                SHARD_RING_CAPACITY as u32,
            );
            return;
        }
        // Ring occupancy: one queued batch more for the writer to receive.
        output_queued.fetch_add(1, Ordering::Relaxed);
    }
    shard_metrics.merge_into(&ctx.metrics);

    // Input closed: shutdown. Build the final snapshot here, where the engine
    // lives, and let the writer persist it inside the shutdown budget. The
    // writer fills the hashes and rebuilds the shard to verify them before the
    // rename, so a bad off-loop hash can never become the resume point.
    snapshot_sequence += 1;
    let started = Instant::now();
    let final_state = engine.snapshot_state_without_hash_compacted();
    let build_us = started.elapsed().as_micros() as u64;
    // A shard that never applied a command has no resume point; persisting an
    // empty snapshot would make the next boot start after offset 0.
    let has_commands = final_state.symbols.iter().any(|symbol| symbol.commands > 0);
    if has_commands {
        info!(
            shard = shard_id,
            sequence = snapshot_sequence,
            build_us,
            book_orders = final_state
                .symbols
                .iter()
                .map(|s| s.book.orders.len())
                .sum::<usize>(),
            dedup_entries = final_state
                .symbols
                .iter()
                .map(|s| s.dedup.len())
                .sum::<usize>(),
            "final snapshot state cloned for the writer"
        );
    }
    let final_snapshot = has_commands.then_some(SequencedSnapshot {
        sequence: snapshot_sequence,
        snapshot: final_state,
    });
    if output
        .blocking_send(OutputBatch {
            executions: Vec::new(),
            md: Vec::new(),
            orders: Vec::new(),
            last_offset,
            snapshot: final_snapshot,
            final_batch: true,
        })
        .is_ok()
    {
        // Ring occupancy: the writer drains the final batch like any other.
        output_queued.fetch_add(1, Ordering::Relaxed);
    }
    // Final ring-occupancy report: the queued counters are what the gauges
    // will read until a successor shard updates them.
    ctx.metrics.set_ring_depths(
        shard_id,
        input_queued.load(Ordering::Relaxed),
        output_queued.load(Ordering::Relaxed),
        SHARD_RING_CAPACITY as u32,
    );
}

/// Turn one processed turbo command into its acknowledgement.
///
/// The engine reports a refusal as a `Rejected` execution carrying the incoming
/// order id and the reason; an acceptance is an `Accepted` execution for the
/// same id. A cancel that succeeded emits only a `Cancelled` execution, so the
/// default is "accepted with no reject reason". Maker-side events (fills,
/// cancelled resting orders) carry other ids or kinds and are ignored.
fn turbo_ack(seq: u64, command: &Command, out: &EngineOutput) -> TurboAck {
    for execution in &out.executions {
        if execution.order_id != command.order_id {
            continue;
        }
        match execution.kind {
            ExecKind::Accepted => {
                return TurboAck {
                    seq,
                    accepted: true,
                    reject: RejectReason::None,
                };
            }
            ExecKind::Rejected => {
                return TurboAck {
                    seq,
                    accepted: false,
                    reject: execution.reason,
                };
            }
            _ => {}
        }
    }
    TurboAck {
        seq,
        accepted: true,
        reject: RejectReason::None,
    }
}

/// Merge a later matcher batch into the writer's current group.
///
/// Batches arrive in seq order, so extending preserves order and the last one
/// carries the highest offset. A later snapshot supersedes an earlier one; a
/// batch without a snapshot leaves the earlier one queued, and `last_written`
/// still rejects an out-of-order write.
fn merge_output_batch(into: &mut OutputBatch, next: OutputBatch) {
    into.executions.extend(next.executions);
    into.md.extend(next.md);
    into.orders.extend(next.orders);
    if next.snapshot.is_some() {
        into.snapshot = next.snapshot;
    }
    into.last_offset = next.last_offset;
    into.final_batch |= next.final_batch;
}

/// Clone the engine state for the snapshot writer and log what it cost the
/// match thread.
///
/// The hashes are deliberately **not** computed here: the writer fills them
/// with `SnapshotWriter::fill_hashes`, so the shard thread pays only the clone
/// (book walk + dedup vec) and the writer pays hash + serialize + fsync. The
/// final snapshot is rebuilt and verified by the writer before it is persisted.
fn build_snapshot(
    shard_id: u32,
    engine: &mut ShardEngine,
    sequence: u64,
    trigger: &'static str,
) -> SequencedSnapshot {
    let started = Instant::now();
    // Compact first: the saved walk must match the live one exactly, or a
    // restored table diverges once an id that was dead at snapshot time is
    // re-admitted after the tail. Compaction does not change the state hash.
    let snapshot = engine.snapshot_state_without_hash_compacted();
    let build_us = started.elapsed().as_micros() as u64;
    let book_orders: usize = snapshot
        .symbols
        .iter()
        .map(|symbol| symbol.book.orders.len())
        .sum();
    let dedup_entries: usize = snapshot
        .symbols
        .iter()
        .map(|symbol| symbol.dedup.len())
        .sum();
    info!(
        shard = shard_id,
        sequence,
        trigger,
        build_us,
        book_orders,
        dedup_entries,
        "snapshot state cloned for the writer"
    );
    SequencedSnapshot { sequence, snapshot }
}

/// Drain matcher output, publish executions and MD, commit the watermark, and
/// forward snapshots. This is the async journal edge for the shard.
#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn run_writer<J: JournalOps + ?Sized>(
    ctx: Arc<NodeContext>,
    journal: &J,
    shard_id: u32,
    consumer: String,
    committed_offset: Option<u64>,
    mut output: mpsc::Receiver<OutputBatch>,
    snapshot_tx: mpsc::Sender<SnapshotJob>,
    snapshot_writer: tokio::task::JoinHandle<()>,
    appended_offset: Arc<AtomicU64>,
    output_queued: Arc<AtomicU32>,
) -> Result<()> {
    let mut watermark = CommitWatermark::new(committed_offset);
    let mut commit_retry = CommitRetry::new();
    let mut final_snapshot: Option<SequencedSnapshot> = None;
    // The writer owns its own stage recorder; merging it once a second keeps
    // the global stage histograms off the per-batch path.
    let writer_metrics = ShardMetrics::new();
    let mut last_writer_merge = Instant::now();

    let mut shutdown_exec = ctx.shutdown.subscribe();
    let mut shutdown_md = ctx.shutdown.subscribe();
    let mut shutdown_orders = ctx.shutdown.subscribe();
    // Flush the commit watermark while idle. Without this tick the committed
    // offset sits behind the applied offset until the next order arrives,
    // because the reader discards empty polls and never wakes the writer.
    let mut commit_tick = tokio::time::interval(COMMIT_MAX_DELAY);
    commit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    commit_tick.tick().await;

    loop {
        let mut batch = tokio::select! {
            batch = output.recv() => match batch {
                Some(batch) => {
                    // The matcher counted this batch when its send succeeded;
                    // the ring occupancy drops as soon as the writer holds it.
                    output_queued.fetch_sub(1, Ordering::Relaxed);
                    batch
                }
                None => break,
            },
            _ = commit_tick.tick() => {
                maybe_commit(
                    journal,
                    Topic::Orders,
                    shard_id,
                    &consumer,
                    &mut watermark,
                    &mut commit_retry,
                )
                .await;
                continue;
            }
        };
        // Coalesce whatever the matcher has already produced. Appending once
        // per 10-order batch makes the write-behind the bottleneck; merging
        // up to eight batches restores the durable path's amortization while
        // keeping the window's latency bound meaningful.
        let mut coalesced = 1usize;
        while coalesced < WRITER_COALESCE_BATCHES && !batch.final_batch {
            match output.try_recv() {
                Ok(next) => {
                    merge_output_batch(&mut batch, next);
                    coalesced += 1;
                    output_queued.fetch_sub(1, Ordering::Relaxed);
                }
                Err(_) => break,
            }
        }
        // Executions and MD are independent topics: join them so the shard
        // pays one overlapped round trip per batch, not two sequential ones.
        //
        // Turbo adds the write-behind's orders append to the same join. The
        // commands were acked at match time, so their only durable record is
        // this append; a batch with no order records (the durable path) keeps
        // the exact two-way join above.
        let append_started = Instant::now();
        let (order_messages, admissions): (Vec<JournalMessage>, Vec<TurboAdmission>) = batch
            .orders
            .into_iter()
            .map(|record| (record.message, record.admission))
            .unzip();
        // The orders append is the window contract: release the permits as
        // soon as that append is durable, without waiting for the derived
        // execution/MD appends. The outputs still have to land before the
        // applied offset and commit advance; they run concurrently so the
        // writer pays one overlapped round trip per group.
        let orders_future = append_orders(journal, shard_id, &order_messages, &mut shutdown_orders);
        let outputs_future = async {
            tokio::join!(
                append_with_retry(
                    journal,
                    Topic::Executions,
                    shard_id,
                    &batch.executions,
                    &mut shutdown_exec,
                ),
                append_with_retry(journal, Topic::Md, shard_id, &batch.md, &mut shutdown_md),
            )
        };
        tokio::pin!(orders_future);
        tokio::pin!(outputs_future);
        // `admissions` is taken when the orders append settles. The empty
        // durable batch has nothing to release.
        let mut admissions = (!order_messages.is_empty()).then_some(admissions);
        let mut orders_result = order_messages.is_empty().then_some(Ok(()));
        let mut outputs_result = None;
        while orders_result.is_none() || outputs_result.is_none() {
            tokio::select! {
                result = &mut orders_future, if orders_result.is_none() => {
                    // Release on failure too: the shard is about to stop, and
                    // a leaked permit would shrink the window while it drains.
                    drop(admissions.take());
                    orders_result = Some(result);
                }
                result = &mut outputs_future, if outputs_result.is_none() => {
                    outputs_result = Some(result);
                }
            }
        }
        let (execution_result, md_result) = outputs_result.expect("outputs future completed");
        let orders_result = orders_result.expect("orders future completed");
        drop(admissions.take());
        // Do not advance the applied offset or commit unless every output is
        // durable. Advancing here would let a restart skip orders whose
        // executions/MD never reached the log. A failed orders append is worse:
        // the matched commands are not in the log at all, so the shard stops
        // rather than keep acking work whose write-behind can never drain.
        if orders_result.is_err() || execution_result.is_err() || md_result.is_err() {
            ctx.mark_shard_failed();
            if let Err(error) = orders_result {
                error!(
                    shard = shard_id,
                    error = %error,
                    "order write-behind failed; shard stopped"
                );
            }
            if let Err(error) = execution_result {
                error!(shard = shard_id, error = %error, "execution publish failed; shard stopped");
            }
            if let Err(error) = md_result {
                error!(shard = shard_id, error = %error, "MD publish failed; shard stopped");
            }
            anyhow::bail!(
                "shard {shard_id} stopped after an output append failed; orders are not being matched"
            );
        }
        writer_metrics.record_stage_us(
            Stage::EngineAppend,
            append_started.elapsed().as_micros() as u64,
        );
        appended_offset.store(batch.last_offset, Ordering::Release);
        watermark.note_applied(batch.last_offset);
        let commit_started = Instant::now();
        maybe_commit(
            journal,
            Topic::Orders,
            shard_id,
            &consumer,
            &mut watermark,
            &mut commit_retry,
        )
        .await;
        writer_metrics.record_stage_us(
            Stage::EngineCommit,
            commit_started.elapsed().as_micros() as u64,
        );
        if last_writer_merge.elapsed() >= Duration::from_secs(1) {
            writer_metrics.merge_into(&ctx.metrics);
            last_writer_merge = Instant::now();
        }

        if batch.final_batch {
            final_snapshot = batch.snapshot;
            break;
        }

        if let Some(snapshot) = batch.snapshot {
            let sequence = snapshot.sequence;
            match snapshot_tx.try_send(SnapshotJob {
                request: snapshot,
                verify: false,
                queued_at: Instant::now(),
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => warn!(
                    shard = shard_id,
                    sequence, "snapshot writer is behind; skipped this snapshot"
                ),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    warn!(
                        shard = shard_id,
                        "snapshot writer stopped; engine state is no longer being persisted"
                    );
                    ctx.mark_shard_failed();
                }
            }
        }
    }

    if let Some(snapshot) = final_snapshot {
        match tokio::time::timeout(
            FINAL_SNAPSHOT_SEND_TIMEOUT,
            snapshot_tx.send(SnapshotJob {
                request: snapshot,
                verify: true,
                queued_at: Instant::now(),
            }),
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
    }

    if let Some(offset) = watermark.pending() {
        match commit_once(journal, Topic::Orders, shard_id, &consumer, offset).await {
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

    writer_metrics.merge_into(&ctx.metrics);
    // Close the queue so the writer drains and exits, then wait for the flush.
    drop(snapshot_tx);
    match tokio::time::timeout(FINAL_SNAPSHOT_JOIN_TIMEOUT, snapshot_writer).await {
        Ok(Ok(())) => info!(shard = shard_id, "engine shard stopped"),
        Ok(Err(error)) => warn!(shard = shard_id, error = %error, "final snapshot writer failed"),
        Err(_) => warn!(
            shard = shard_id,
            "final snapshot did not finish within the shutdown budget"
        ),
    }
    Ok(())
}

/// Consume the canonical market-data log and apply it to the dashboard cache.
pub async fn run_md_shard(ctx: Arc<NodeContext>, shard_id: u32) -> Result<()> {
    let journal = ctx.engine_journal_for(shard_id);
    let consumer = format!("whippet-md-p{shard_id}");
    let committed_offset = match journal.committed(Topic::Md, shard_id, &consumer).await {
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
    let mut poll_failures: u32 = 0;

    loop {
        let records = tokio::select! {
            _ = shutdown.changed() => {
                if let Some(offset) = watermark.pending() {
                    match commit_once(journal.as_ref(), Topic::Md, shard_id, &consumer, offset).await {
                        Ok(()) => watermark.mark_committed(offset, Instant::now()),
                        Err(error) => warn!(shard = shard_id, offset, error = %error, "final MD offset commit failed"),
                    }
                }
                return Ok(());
            }
            result = journal.read_payloads(Topic::Md, shard_id, &consumer, next_offset, BATCH_CAPACITY as u32, 100) => result,
        };
        let records = match records {
            Ok(records) => records,
            Err(error) => {
                poll_failures += 1;
                warn!(shard = shard_id, attempt = poll_failures, error = %error, "MD poll failed; retrying");
                if poll_failures >= POLL_FAILURE_BUDGET {
                    ctx.mark_shard_failed();
                    anyhow::bail!(
                        "MD poll failed {poll_failures} times on shard {shard_id}; the dashboard feed is stopped"
                    );
                }
                sleep(Duration::from_millis(250)).await;
                continue;
            }
        };
        poll_failures = 0;
        if records.is_empty() {
            maybe_commit(
                journal.as_ref(),
                Topic::Md,
                shard_id,
                &consumer,
                &mut watermark,
                &mut commit_retry,
            )
            .await;
            if !journal.is_iggy() {
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
            journal.as_ref(),
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
#[allow(clippy::too_many_arguments)]
async fn maybe_commit<J: JournalOps + ?Sized>(
    journal: &J,
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
    match commit_once(journal, topic, shard_id, consumer, offset).await {
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
async fn commit_once<J: JournalOps + ?Sized>(
    journal: &J,
    topic: Topic,
    partition: u32,
    consumer: &str,
    offset: u64,
) -> Result<()> {
    match tokio::time::timeout(
        COMMIT_TIMEOUT,
        journal.commit(topic, partition, consumer, offset),
    )
    .await
    {
        Ok(result) => result.map_err(anyhow::Error::from),
        Err(_elapsed) => anyhow::bail!("offset commit timed out after {COMMIT_TIMEOUT:?}"),
    }
}

/// Owns the snapshot file. `save` is blocking, so the whole writer runs on a
/// blocking thread and the shard loop only ever hands over an owned
/// `ShardSnapshot`.
///
/// The stage split is logged per snapshot (`queue_us`, `hash_us`, `write_us`,
/// `total_us`) because the periodic p99 was previously attributed to the wrong
/// stage: the shard thread's clone, the off-loop hash, and the serialize+fsync
/// are three different costs on three different threads.
fn spawn_snapshot_writer(
    ctx: Arc<NodeContext>,
    shard_id: u32,
    mut pending: mpsc::Receiver<SnapshotJob>,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        // The verified writer rebuilds the shard from the snapshot before the
        // atomic rename, so a bad off-loop hash is rejected, not persisted.
        let mut writer =
            SnapshotWriter::with_exchange(ctx.snapshots.clone(), ctx.exchange().clone());
        while let Some(job) = pending.blocking_recv() {
            let SnapshotJob {
                mut request,
                verify,
                queued_at,
            } = job;
            let sequence = request.sequence;
            let received = Instant::now();
            let queue_us = received.saturating_duration_since(queued_at).as_micros() as u64;

            // The shard only cloned the state; the hash is filled here, off the
            // match loop, and a supplied hash that disagrees is rejected rather
            // than silently repaired.
            let hash_started = Instant::now();
            let filled = writer.fill_hashes(&mut request.snapshot);
            let hash_us = hash_started.elapsed().as_micros() as u64;
            if let Err(error) = filled {
                warn!(
                    shard = shard_id,
                    sequence,
                    error = %error,
                    queue_us, hash_us,
                    "snapshot hash fill failed; matching continues, the log is still canonical"
                );
                continue;
            }
            // Diagnostics are built here, off the shard loop, and only after the
            // fill so the log shows the hashes that will be persisted.
            let hashes: Vec<String> = request
                .snapshot
                .symbols
                .iter()
                .map(|symbol| hex(&symbol.state_hash))
                .collect();
            let write_started = Instant::now();
            // One snapshot write at a time: three shards fsyncing at once
            // contend with Iggy's WAL on the same volume and produced a
            // multi-hundred-millisecond gateway tail.
            let _guard = ctx
                .snapshot_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let result = if verify {
                writer.write(request)
            } else {
                writer.write_unverified(request)
            };
            let write_us = write_started.elapsed().as_micros() as u64;
            let total_us = received.elapsed().as_micros() as u64;
            // `Metrics` has no snapshot-duration recorder and `context.rs` is
            // owned elsewhere, so the split is logged rather than recorded.
            // TODO(lead): add a `snapshot_duration_us` histogram to `Metrics`.
            match result {
                Ok(Some(path)) => info!(
                    shard = shard_id,
                    sequence,
                    path = %path.display(),
                    hashes = ?hashes,
                    queue_us,
                    hash_us,
                    write_us,
                    total_us,
                    "snapshot written"
                ),
                Ok(None) => warn!(
                    shard = shard_id,
                    sequence,
                    queue_us,
                    hash_us,
                    write_us,
                    total_us,
                    "snapshot is older than the last one written; skipped"
                ),
                Err(error) => {
                    // A failed snapshot costs a resume point, not matching:
                    // the log is canonical. Do not fail ingress for a
                    // transient ENOSPC or read-only mount; the next snapshot
                    // retries. A dead writer is handled at the send site.
                    warn!(
                        shard = shard_id,
                        sequence,
                        error = %error,
                        queue_us,
                        hash_us,
                        write_us,
                        total_us,
                        "snapshot write failed; matching continues, the log is still canonical"
                    );
                }
            }
        }
    })
}

/// Append one turbo slice of order commands in seq order.
///
/// Each call carries at most [`ORDER_APPEND_CHUNK`] messages so a single request
/// is bounded. The shard has exactly one writer and appends slices in the order
/// the matcher produced them, so log order equals seq order and `seq == offset`
/// holds. Any failure propagates: retrying past the budget, or skipping a
/// chunk, would leave a gap in the seq domain the log can no longer derive.
async fn append_orders<J: JournalOps + ?Sized>(
    journal: &J,
    shard_id: u32,
    messages: &[JournalMessage],
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    for chunk in messages.chunks(ORDER_APPEND_CHUNK) {
        append_with_retry(journal, Topic::Orders, shard_id, chunk, shutdown).await?;
    }
    Ok(())
}

#[doc(hidden)]
pub async fn append_with_retry<J: JournalOps + ?Sized>(
    journal: &J,
    topic: Topic,
    partition: u32,
    messages: &[JournalMessage],
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    if messages.is_empty() {
        return Ok(());
    }
    let mut delay = Duration::from_millis(100);
    let mut attempts: u32 = 0;
    let mut last_warn: Option<Instant> = None;
    loop {
        match journal.append(topic, partition, messages).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                attempts += 1;
                let warn_now = last_warn.is_none_or(|at| at.elapsed() >= Duration::from_secs(1));
                if warn_now {
                    last_warn = Some(Instant::now());
                    warn!(
                        topic = %topic,
                        partition,
                        attempt = attempts,
                        error = %error,
                        "append failed; retrying"
                    );
                } else {
                    tracing::debug!(
                        topic = %topic,
                        partition,
                        attempt = attempts,
                        error = %error,
                        "append failed; retrying"
                    );
                }
                if attempts >= APPEND_MAX_ATTEMPTS {
                    anyhow::bail!(
                        "{topic} append failed {attempts} times on partition {partition}: {error}"
                    );
                }
                tokio::select! {
                    _ = shutdown.changed() => {
                        anyhow::bail!("shutdown while retrying a {topic} append");
                    }
                    _ = sleep(delay) => {}
                }
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
    JournalMessage::new_named(execution.encode(), meta.named_pairs(HeaderMode::Routing))
}

fn md_message(event: &MdEvent) -> JournalMessage {
    let meta = MessageMeta::new(
        event.symbol,
        event.seq,
        "md",
        SCHEMA_MD_EVENT,
        u128::from(event.cause_offset),
    );
    JournalMessage::new_named(event.encode(), meta.named_pairs(HeaderMode::Routing))
}

fn snapshot_message(snapshot: &MdSnapshot) -> JournalMessage {
    let meta = MessageMeta::new(
        snapshot.symbol,
        snapshot.seq,
        "md_snapshot",
        SCHEMA_MD_SNAPSHOT,
        u128::from(snapshot.cause_offset),
    );
    JournalMessage::new_named(snapshot.encode(), meta.named_pairs(HeaderMode::Full))
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

    #[test]
    fn an_empty_snapshot_has_no_resume_offset() {
        let exchange = whippet_core::config::ExchangeConfig::default();
        let engine = ShardEngine::new(1, &exchange);
        let snapshot = engine.snapshot_state_without_hash();
        assert!(snapshot.symbols.iter().all(|symbol| symbol.commands == 0));
        assert_eq!(resume_offset(&snapshot), None);
    }

    #[test]
    fn a_snapshot_with_commands_resumes_after_its_offset() {
        let exchange = whippet_core::config::ExchangeConfig::default();
        let engine = ShardEngine::new(1, &exchange);
        let mut snapshot = engine.snapshot_state_without_hash();
        if let Some(symbol) = snapshot.symbols.first_mut() {
            symbol.commands = 1;
            symbol.last_offset = 42;
        }
        assert_eq!(resume_offset(&snapshot), Some(42));
    }

    #[test]
    fn snapshot_cadence_defaults_to_thirty_seconds() {
        assert_eq!(snapshot_min_interval_from(None), Duration::from_secs(30));
    }

    #[test]
    fn snapshot_cadence_is_clamped_to_the_supported_range() {
        // Below the floor and above the ceiling clamp; inside the range is
        // honoured exactly, including the old 15 s.
        assert_eq!(snapshot_min_interval_from(Some(0)), Duration::from_secs(5));
        assert_eq!(snapshot_min_interval_from(Some(1)), Duration::from_secs(5));
        assert_eq!(
            snapshot_min_interval_from(Some(15)),
            Duration::from_secs(15)
        );
        assert_eq!(
            snapshot_min_interval_from(Some(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            snapshot_min_interval_from(Some(300)),
            Duration::from_secs(300)
        );
        assert_eq!(
            snapshot_min_interval_from(Some(10_000)),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn the_matcher_hands_the_writer_an_unhashed_snapshot() {
        // The shard thread pays the clone only; `SnapshotWriter::fill_hashes`
        // computes the hash off the match loop and the final snapshot is
        // rebuilt and verified before the rename.
        let exchange = whippet_core::config::ExchangeConfig::default();
        let mut engine = ShardEngine::new(1, &exchange);
        let job = build_snapshot(1, &mut engine, 1, "test");
        assert_eq!(job.sequence, 1);
        assert!(
            job.snapshot
                .symbols
                .iter()
                .all(|symbol| symbol.state_hash == [0u8; 32]),
            "the matcher must not hash on the shard thread"
        );
    }

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
