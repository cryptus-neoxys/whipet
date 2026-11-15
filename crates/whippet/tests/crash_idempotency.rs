// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Crash idempotency at the shard's output boundary.
//!
//! The contract under test is **at-least-once**, never exactly-once. The writer
//! appends executions and market data *before* it commits the input offset. A
//! process death in that window means the next life replays the same input and
//! can append the same execution/MD records a second time. What must hold is
//! that a duplicate carries the *same* stable identity as the first copy, so a
//! downstream consumer can deduplicate it and recover exactly one business
//! effect:
//!
//! - executions: `(symbol, cause_offset, seq, kind)`
//! - market data: `(symbol, cause_offset, seq, kind)`
//!
//! `seq` is the engine's monotonic per-symbol sequence, `cause_offset` is the
//! input offset that produced the event, and `kind` is the wire discriminant.
//! Replay is deterministic, so both copies of a record decode to identical
//! payloads; the identity is not a new field bolted onto the envelope, it is
//! the combination of fields the engine already stamps.
//!
//! The harness drives the production matcher/writer pair (`run_matcher` +
//! `run_writer`) against a recording `JournalOps` fake. Nothing here touches a
//! broker, a snapshot file, or the network, and no production code is changed.
//! The crash is modelled at the journal boundary: a writer life whose
//! executions/MD appends succeed while its commits always fail. A real `kill
//! -9` also skips the shutdown-time snapshot, so the only resume point used by
//! the tests is the *prefix* snapshot at the last committed offset. The
//! crashed life's shutdown snapshot is deliberately ignored: a crash does not
//! get to write one.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::TestNode;
use tokio::sync::mpsc;
use whippet::context::NodeContext;
use whippet::engine_runner::{
    InputBatch, JournalOps, OutputBatch, SHARD_RING_CAPACITY, SnapshotJob, run_matcher, run_writer,
};
use whippet_core::config::ExchangeConfig;
use whippet_core::headers::{SCHEMA_EXECUTION, SCHEMA_MD_EVENT};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, ExecKind, ExecutionReport, MdEvent, OrderType, Side};
use whippet_engine::{ShardEngine, ShardSnapshot, fill_symbol_hashes};
use whippet_journal::{
    AppendResult, JournalError, JournalMessage, PayloadRecord, Result as JournalResult, Topic,
};

// ---------------------------------------------------------------------------
// Recording journal fake
// ---------------------------------------------------------------------------

/// Every journal action a shard life performed, in order. The order is an
/// assertion: the crashed life must append both output topics before it reaches
/// the commit boundary it is refused at.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LifeEvent {
    Append(Topic),
    CommitAttempt(u64),
}

/// The broker state the fakes share across shard "lives".
#[derive(Default)]
struct JournalLog {
    executions: Vec<JournalMessage>,
    md: Vec<JournalMessage>,
    /// Append order, so a test can assert that a crashed life appended nothing.
    appends: Vec<Topic>,
    /// Append and commit-attempt order.
    events: Vec<LifeEvent>,
    /// Last acknowledged consumer offset. Survives a shard restart, like the
    /// broker's stored consumer offset.
    committed: Option<u64>,
    /// Every commit attempt, including the ones that fail. Used to prove the
    /// crashed life did reach the commit boundary and was refused there.
    commit_attempts: Vec<u64>,
}

/// Which outcome `commit` returns for one shard life. `Fail` is the crash: the
/// appends already landed, the offset acknowledgement never does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitPolicy {
    Accept,
    Fail,
}

/// A `JournalOps` fake that records execution/MD appends into a log shared by
/// every life of the shard, so a replay appends into the same canonical stream.
struct RecordingJournal {
    log: Arc<Mutex<JournalLog>>,
    policy: CommitPolicy,
}

impl RecordingJournal {
    fn accepting() -> Self {
        Self {
            log: Arc::new(Mutex::new(JournalLog::default())),
            policy: CommitPolicy::Accept,
        }
    }

    fn failing_commits(&self) -> Self {
        Self {
            log: Arc::clone(&self.log),
            policy: CommitPolicy::Fail,
        }
    }

    fn accepting_handle(&self) -> Self {
        Self {
            log: Arc::clone(&self.log),
            policy: CommitPolicy::Accept,
        }
    }

    fn executions(&self) -> Vec<JournalMessage> {
        lock(&self.log).executions.clone()
    }

    fn md(&self) -> Vec<JournalMessage> {
        lock(&self.log).md.clone()
    }

    fn committed_offset(&self) -> Option<u64> {
        lock(&self.log).committed
    }

    fn appends(&self) -> Vec<Topic> {
        lock(&self.log).appends.clone()
    }

    fn commit_attempts(&self) -> Vec<u64> {
        lock(&self.log).commit_attempts.clone()
    }

    fn events(&self) -> Vec<LifeEvent> {
        lock(&self.log).events.clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl JournalOps for RecordingJournal {
    fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> impl Future<Output = JournalResult<AppendResult>> + Send {
        let log = Arc::clone(&self.log);
        async move {
            let mut log = lock(&log);
            let base_offset = match topic {
                Topic::Executions => {
                    let base = log.executions.len() as u64;
                    log.executions.extend(messages.iter().cloned());
                    base
                }
                Topic::Md => {
                    let base = log.md.len() as u64;
                    log.md.extend(messages.iter().cloned());
                    base
                }
                // The durable shard path never writes orders from the writer.
                _ => return Err(JournalError::NotConnected),
            };
            log.appends.push(topic);
            log.events.push(LifeEvent::Append(topic));
            Ok(AppendResult {
                partition,
                base_offset,
                count: messages.len() as u32,
            })
        }
    }

    fn commit(
        &self,
        _topic: Topic,
        _partition: u32,
        _consumer: &str,
        offset: u64,
    ) -> impl Future<Output = JournalResult<()>> + Send {
        let log = Arc::clone(&self.log);
        let policy = self.policy;
        async move {
            let mut log = lock(&log);
            log.commit_attempts.push(offset);
            log.events.push(LifeEvent::CommitAttempt(offset));
            if policy == CommitPolicy::Fail {
                return Err(JournalError::NotConnected);
            }
            log.committed = Some(match log.committed {
                Some(committed) => committed.max(offset),
                None => offset,
            });
            Ok(())
        }
    }

    fn committed(
        &self,
        _topic: Topic,
        _partition: u32,
        _consumer: &str,
    ) -> impl Future<Output = JournalResult<Option<u64>>> + Send {
        let log = Arc::clone(&self.log);
        async move { Ok(lock(&log).committed) }
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_payloads_into<'a>(
        &'a self,
        _topic: Topic,
        _partition: u32,
        _consumer: &'a str,
        _offset: u64,
        _count: u32,
        _max_wait_ms: u64,
        out: &'a mut Vec<PayloadRecord>,
    ) -> JournalResult<usize> {
        out.clear();
        Ok(0)
    }

    fn is_iggy(&self) -> bool {
        false
    }

    async fn ping(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Shard harness
// ---------------------------------------------------------------------------

/// What one shard life produced. `snapshot` is the shutdown snapshot (hashes
/// still zeroed) and `applied_offset` is the last input offset it applied.
struct ShardRun {
    snapshot: Option<ShardSnapshot>,
    applied_offset: u64,
}

fn snapshot_collector() -> (
    mpsc::Sender<SnapshotJob>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<ShardSnapshot>>>,
) {
    let (sender, mut receiver) = mpsc::channel::<SnapshotJob>(SHARD_RING_CAPACITY);
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let task = tokio::spawn(async move {
        while let Some(job) = receiver.recv().await {
            lock(&sink).push(job.request.snapshot);
        }
    });
    (sender, task, captured)
}

/// Run one matcher/writer life over exactly one input batch, then shut down.
///
/// This mirrors how the node wires the shard: the matcher owns the engine on a
/// plain OS thread, the writer owns the journal edge, and the shared applied
/// offset gates snapshots. `committed` is the offset the life starts from
/// (production passes the snapshot/journal watermark) and `applied_offset` is
/// the last offset already fully applied (`resume - 1`).
async fn drive_shard(
    ctx: &Arc<NodeContext>,
    journal: &RecordingJournal,
    shard_id: u32,
    engine: ShardEngine,
    applied_offset: u64,
    committed: Option<u64>,
    batch: InputBatch,
) -> ShardRun {
    assert!(!batch.records.is_empty(), "the harness feeds one batch");
    let last_offset = batch
        .records
        .last()
        .map(|record| record.offset)
        .unwrap_or(applied_offset);
    let appended = Arc::new(AtomicU64::new(committed.unwrap_or(u64::MAX)));
    // Ring-occupancy counters for one life: the matcher and writer must share
    // them, otherwise the writer's decrement underflows a private counter. The
    // input counter starts at 1 because the harness has one batch queued, which
    // is what the production reader's increment represents.
    let input_queued = Arc::new(AtomicU32::new(1));
    let output_queued = Arc::new(AtomicU32::new(0));
    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    let (output_tx, output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    let (recycle_tx, _recycle_rx) = mpsc::channel::<Vec<PayloadRecord>>(SHARD_RING_CAPACITY);
    let (snapshot_tx, snapshot_task, snapshots) = snapshot_collector();

    let matcher_ctx = Arc::clone(ctx);
    let matcher_appended = Arc::clone(&appended);
    let matcher_input_queued = Arc::clone(&input_queued);
    let matcher_output_queued = Arc::clone(&output_queued);
    let matcher = std::thread::Builder::new()
        .name(format!("crash-idempotency-{shard_id}"))
        .spawn(move || {
            run_matcher(
                matcher_ctx,
                shard_id,
                engine,
                input_rx,
                output_tx,
                recycle_tx,
                matcher_appended,
                applied_offset,
                applied_offset,
                matcher_input_queued,
                matcher_output_queued,
            )
        })
        .expect("spawn the matcher thread");

    let writer = run_writer(
        Arc::clone(ctx),
        journal,
        shard_id,
        format!("crash-idempotency-p{shard_id}"),
        committed,
        output_rx,
        snapshot_tx,
        snapshot_task,
        Arc::clone(&appended),
        Arc::clone(&output_queued),
    );
    let ((), writer_result) = tokio::join!(
        async {
            input_tx
                .send(batch)
                .await
                .expect("the matcher input ring stays open");
            drop(input_tx);
        },
        writer,
    );
    writer_result.expect("the writer drains, appends, and commits");

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || matcher.join()),
    )
    .await
    .expect("the matcher must stop promptly")
    .expect("the join task runs")
    .expect("the matcher exits cleanly");

    ShardRun {
        snapshot: lock(&snapshots).pop(),
        applied_offset: last_offset,
    }
}

/// A writer life that is never handed another batch: it exits without applying
/// or committing anything. This is Test C's "commit succeeded, writer died
/// before the next batch" state; a `kill -9` and an orderly exit with no next
/// batch leave the same durable boundary.
async fn run_idle_writer(
    ctx: &Arc<NodeContext>,
    journal: &RecordingJournal,
    shard_id: u32,
    committed: Option<u64>,
) {
    let (output_tx, output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    drop(output_tx);
    let (snapshot_tx, snapshot_task, _snapshots) = snapshot_collector();
    run_writer(
        Arc::clone(ctx),
        journal,
        shard_id,
        format!("crash-idempotency-idle-p{shard_id}"),
        committed,
        output_rx,
        snapshot_tx,
        snapshot_task,
        Arc::new(AtomicU64::new(committed.unwrap_or(u64::MAX))),
        // No matcher exists for this life, so there is nothing to share the
        // ring counter with and nothing will decrement it.
        Arc::new(AtomicU32::new(0)),
    )
    .await
    .expect("an idle writer exits cleanly");
}

/// A matcher life that reads and matches the batch but has no writer: the
/// output ring's receiver is dropped before the matcher starts, so the first
/// `blocking_send` fails and the matcher exits without any durable output.
async fn crash_matcher_before_output(
    ctx: &Arc<NodeContext>,
    shard_id: u32,
    engine: ShardEngine,
    applied_offset: u64,
    batch: InputBatch,
) {
    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    let (output_tx, output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    let (recycle_tx, _recycle_rx) = mpsc::channel::<Vec<PayloadRecord>>(SHARD_RING_CAPACITY);
    // The writer is gone before the matcher produces anything.
    drop(output_rx);

    // No writer life exists to share these with; the matcher never increments
    // `output_queued` because its send fails. The input counter starts at 1 for
    // the one batch the harness queues.
    let input_queued = Arc::new(AtomicU32::new(1));
    let output_queued = Arc::new(AtomicU32::new(0));
    let matcher_ctx = Arc::clone(ctx);
    let matcher = std::thread::Builder::new()
        .name(format!("crash-idempotency-dying-{shard_id}"))
        .spawn(move || {
            run_matcher(
                matcher_ctx,
                shard_id,
                engine,
                input_rx,
                output_tx,
                recycle_tx,
                Arc::new(AtomicU64::new(applied_offset)),
                applied_offset,
                applied_offset,
                input_queued,
                output_queued,
            )
        })
        .expect("spawn the matcher thread");

    tokio::time::timeout(Duration::from_secs(5), input_tx.send(batch))
        .await
        .expect("the matcher reads its input")
        .expect("the matcher input ring stays open");
    drop(input_tx);

    tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || matcher.join()),
    )
    .await
    .expect("the dying matcher must stop promptly")
    .expect("the join task runs")
    .expect("the matcher exits cleanly");
}

// ---------------------------------------------------------------------------
// Deterministic workload
// ---------------------------------------------------------------------------

const PREFIX_OFFSET: u64 = 0;

fn btc() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol is valid")
}

/// A small deterministic log that exercises the three interesting outcomes:
/// a maker fill, a cancel, and rejects. Offset 0 rests the maker order; the
/// rest is the tail that a crash can duplicate.
fn command_log() -> Vec<Command> {
    let symbol = btc();
    vec![
        // Offset 0: rests on the sell side.
        Command::new_order(
            symbol,
            Side::Sell,
            OrderType::Limit,
            Price(65_000),
            Qty(2),
            OrderId(1),
            ParticipantId(1),
            1,
        ),
        // Offset 1: different participant, so it fills the resting maker.
        Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(65_000),
            Qty(1),
            OrderId(2),
            ParticipantId(2),
            2,
        ),
        // Offset 2: cancel the maker remainder.
        Command::cancel(symbol, OrderId(1), ParticipantId(1), 3),
        // Offset 3: cancel an order that is not in the book.
        Command::cancel(symbol, OrderId(99), ParticipantId(1), 4),
        // Offset 4: a price outside the configured ladder.
        Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(1),
            Qty(1),
            OrderId(3),
            ParticipantId(1),
            5,
        ),
    ]
}

fn records(commands: &[Command]) -> Vec<PayloadRecord> {
    commands
        .iter()
        .enumerate()
        .map(|(offset, command)| PayloadRecord {
            offset: offset as u64,
            timestamp_ns: 0,
            payload: Bytes::from(command.encode()),
        })
        .collect()
}

fn prefix_records(commands: &[Command]) -> Vec<PayloadRecord> {
    records(commands)
        .into_iter()
        .filter(|record| record.offset == PREFIX_OFFSET)
        .collect()
}

fn tail_records(commands: &[Command]) -> Vec<PayloadRecord> {
    records(commands)
        .into_iter()
        .filter(|record| record.offset > PREFIX_OFFSET)
        .collect()
}

// ---------------------------------------------------------------------------
// Recovery helpers
// ---------------------------------------------------------------------------

/// Mirror of the node's `engine_runner::resume_offset` (which is `pub(crate)`):
/// only symbols that actually applied a command count, and the resume point is
/// the highest such `last_offset`. An empty snapshot has no resume point.
fn snapshot_applied_offset(snapshot: &ShardSnapshot) -> Option<u64> {
    snapshot
        .symbols
        .iter()
        .filter(|symbol| symbol.commands > 0)
        .map(|symbol| symbol.last_offset)
        .max()
}

fn restore_engine(exchange: &ExchangeConfig, snapshot: &ShardSnapshot) -> ShardEngine {
    let mut snapshot = snapshot.clone();
    fill_symbol_hashes(&mut snapshot);
    ShardEngine::from_snapshot(exchange, snapshot)
        .expect("the prefix snapshot's hashes verify on restore")
}

/// The per-symbol hashes are the exact input to `ShardEngine::state_hash` (plus
/// the shard id, which both snapshots share), so comparing them compares the
/// shard state hash without re-implementing blake3 in the test.
fn symbol_hashes(snapshot: &ShardSnapshot) -> Vec<(Symbol, [u8; 32])> {
    let mut snapshot = snapshot.clone();
    fill_symbol_hashes(&mut snapshot);
    snapshot
        .symbols
        .into_iter()
        .map(|symbol| (symbol.symbol, symbol.state_hash))
        .collect()
}

fn next_offset_after(committed: Option<u64>) -> u64 {
    match committed {
        Some(offset) => offset.saturating_add(1),
        None => 0,
    }
}

// ---------------------------------------------------------------------------
// Identity and decoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ExecutionId {
    symbol: Symbol,
    cause_offset: u64,
    seq: u64,
    kind: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MarketDataId {
    symbol: Symbol,
    cause_offset: u64,
    seq: u64,
    kind: u8,
}

fn execution_id(report: &ExecutionReport) -> ExecutionId {
    ExecutionId {
        symbol: report.symbol,
        cause_offset: report.cause_offset,
        seq: report.seq.0,
        kind: report.kind.as_code(),
    }
}

fn market_data_id(event: &MdEvent) -> MarketDataId {
    MarketDataId {
        symbol: event.symbol,
        cause_offset: event.cause_offset,
        seq: event.seq.0,
        kind: event.kind.as_code(),
    }
}

/// Wire-visible contents of an append, for exact equality assertions without
/// requiring `JournalMessage: PartialEq`.
type WireMessage = (Vec<u8>, Vec<(String, String)>);

fn wire(messages: &[JournalMessage]) -> Vec<WireMessage> {
    messages
        .iter()
        .map(|message| (message.payload.to_vec(), message.headers.to_pairs()))
        .collect()
}

fn header_value(message: &JournalMessage, key: &str) -> Option<String> {
    message
        .headers
        .to_pairs()
        .into_iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
}

/// The envelope carries the routing subset only. `cause_offset` and the kind
/// code live in the canonical fixed-layout payload, which is why consumers
/// decode the payload to deduplicate.
fn assert_execution_envelope(message: &JournalMessage, report: &ExecutionReport) {
    assert_eq!(
        header_value(message, "symbol"),
        Some(report.symbol.to_string())
    );
    assert_eq!(header_value(message, "seq"), Some(report.seq.0.to_string()));
    assert_eq!(header_value(message, "type").as_deref(), Some("execution"));
    assert_eq!(
        header_value(message, "schema"),
        Some(SCHEMA_EXECUTION.to_string())
    );
    assert_eq!(
        header_value(message, "trace_id"),
        None,
        "executions use HeaderMode::Routing; cause_offset is only in the payload"
    );
}

fn assert_market_data_envelope(message: &JournalMessage, event: &MdEvent) {
    assert_eq!(
        header_value(message, "symbol"),
        Some(event.symbol.to_string())
    );
    assert_eq!(header_value(message, "seq"), Some(event.seq.0.to_string()));
    assert_eq!(header_value(message, "type").as_deref(), Some("md"));
    assert_eq!(
        header_value(message, "schema"),
        Some(SCHEMA_MD_EVENT.to_string())
    );
    assert_eq!(
        header_value(message, "trace_id"),
        None,
        "MD deltas use HeaderMode::Routing; cause_offset is only in the payload"
    );
}

fn decode_executions(messages: &[JournalMessage]) -> Vec<ExecutionReport> {
    messages
        .iter()
        .map(|message| {
            let report = ExecutionReport::decode(&message.payload)
                .expect("a writer execution payload decodes with the production type");
            assert_execution_envelope(message, &report);
            report
        })
        .collect()
}

fn decode_market_data(messages: &[JournalMessage]) -> Vec<MdEvent> {
    messages
        .iter()
        .map(|message| {
            let event = MdEvent::decode(&message.payload)
                .expect("a writer MD payload decodes with the production type");
            assert_market_data_envelope(message, &event);
            event
        })
        .collect()
}

fn execution_counts(reports: &[ExecutionReport]) -> BTreeMap<ExecutionId, usize> {
    let mut counts = BTreeMap::new();
    for report in reports {
        *counts.entry(execution_id(report)).or_insert(0) += 1;
    }
    counts
}

fn market_data_counts(events: &[MdEvent]) -> BTreeMap<MarketDataId, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(market_data_id(event)).or_insert(0) += 1;
    }
    counts
}

fn dedup_executions(reports: &[ExecutionReport]) -> Vec<ExecutionReport> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::with_capacity(reports.len());
    for report in reports {
        if seen.insert(execution_id(report)) {
            deduped.push(report.clone());
        }
    }
    deduped
}

fn dedup_market_data(events: &[MdEvent]) -> Vec<MdEvent> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::with_capacity(events.len());
    for event in events {
        if seen.insert(market_data_id(event)) {
            deduped.push(event.clone());
        }
    }
    deduped
}

/// Assert that `messages` is exactly two consecutive copies of `once` (same
/// payload bytes, same envelope), which is the append-before-commit duplicate.
fn assert_two_identical_copies(messages: &[JournalMessage], once: usize) {
    assert_eq!(
        messages.len(),
        2 * once,
        "the crashed attempt and the replay must each append the tail once"
    );
    for (first, second) in messages[..once].iter().zip(&messages[once..]) {
        assert_eq!(
            first.payload, second.payload,
            "replay must re-encode the identical execution/MD payload"
        );
        assert_eq!(
            first.headers.to_pairs(),
            second.headers.to_pairs(),
            "replay must produce the identical envelope"
        );
    }
}

// ---------------------------------------------------------------------------
// Test A: append succeeded, commit did not, replay duplicates by identity
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_outputs_after_append_before_commit_dedup_to_one_run() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let exchange = ExchangeConfig::default();
    let symbol = btc();
    let shard_id = symbol.partition(exchange.partitions);
    let commands = command_log();
    let prefix = prefix_records(&commands);
    let tail = tail_records(&commands);

    // Baseline: one clean life over the whole log, committing normally.
    let clean_journal = RecordingJournal::accepting();
    let clean = drive_shard(
        &ctx,
        &clean_journal,
        shard_id,
        ShardEngine::new(shard_id, &exchange),
        0,
        None,
        InputBatch::durable(records(&commands)),
    )
    .await;
    let clean_executions = decode_executions(&clean_journal.executions());
    let clean_md = decode_market_data(&clean_journal.md());
    let clean_execution_wire = wire(&clean_journal.executions());
    let clean_md_wire = wire(&clean_journal.md());
    assert!(
        !clean_executions.is_empty() && !clean_md.is_empty(),
        "the workload must produce both output streams"
    );
    for kind in [ExecKind::Filled, ExecKind::Cancelled, ExecKind::Rejected] {
        assert!(
            clean_executions.iter().any(|report| report.kind == kind),
            "the workload must exercise a {kind:?} execution"
        );
    }

    // Life 1: the committed prefix. Its shutdown snapshot is the resume point.
    let journal = RecordingJournal::accepting();
    let prefix_run = drive_shard(
        &ctx,
        &journal,
        shard_id,
        ShardEngine::new(shard_id, &exchange),
        0,
        None,
        InputBatch::durable(prefix.clone()),
    )
    .await;
    assert_eq!(
        journal.committed_offset(),
        Some(PREFIX_OFFSET),
        "the prefix offset is committed"
    );
    let prefix_snapshot = prefix_run
        .snapshot
        .as_ref()
        .expect("the prefix life has state to snapshot");
    assert_eq!(
        snapshot_applied_offset(prefix_snapshot),
        Some(PREFIX_OFFSET),
        "the resume point is the last committed input offset"
    );
    let prefix_exec_count = journal.executions().len();
    let prefix_md_count = journal.md().len();

    // Life 2: the crash. Appends succeed, every commit is refused. Its shutdown
    // snapshot is ignored on purpose: a killed process would never reach it.
    let events_before_crash = journal.events().len();
    let crash_run = drive_shard(
        &ctx,
        &journal.failing_commits(),
        shard_id,
        restore_engine(&exchange, prefix_snapshot),
        PREFIX_OFFSET,
        Some(PREFIX_OFFSET),
        InputBatch::durable(tail.clone()),
    )
    .await;
    assert_eq!(
        journal.committed_offset(),
        Some(PREFIX_OFFSET),
        "the crashed life appended outputs but never advanced the committed offset"
    );
    assert!(
        journal
            .commit_attempts()
            .contains(&crash_run.applied_offset),
        "the crashed life did reach the commit boundary for its applied batch"
    );
    let crash_events = &journal.events()[events_before_crash..];
    let executions_appended = crash_events
        .iter()
        .position(|event| *event == LifeEvent::Append(Topic::Executions))
        .expect("the crashed life appended executions");
    let md_appended = crash_events
        .iter()
        .position(|event| *event == LifeEvent::Append(Topic::Md))
        .expect("the crashed life appended MD");
    let commit_refused = crash_events
        .iter()
        .position(|event| *event == LifeEvent::CommitAttempt(crash_run.applied_offset))
        .expect("the crashed life attempted the commit");
    assert!(
        executions_appended < commit_refused && md_appended < commit_refused,
        "both output appends must precede the refused commit: {crash_events:?}"
    );
    // A real crash cannot persist a shutdown snapshot, so the tail snapshot is
    // not a legal resume point even though this test's writer built one.
    let _ignored_crash_snapshot = crash_run.snapshot;

    // Life 3: restart from the committed prefix snapshot and replay the tail.
    let restart_from = next_offset_after(journal.committed_offset());
    assert_eq!(
        restart_from,
        PREFIX_OFFSET + 1,
        "replay starts after the last committed input offset"
    );
    let restart_run = drive_shard(
        &ctx,
        &journal.accepting_handle(),
        shard_id,
        restore_engine(&exchange, prefix_snapshot),
        PREFIX_OFFSET,
        Some(PREFIX_OFFSET),
        InputBatch::durable(tail.clone()),
    )
    .await;

    let last_offset = tail.last().expect("tail is not empty").offset;
    assert_eq!(
        journal.committed_offset(),
        Some(last_offset),
        "the replay commits the tail once its outputs are durable"
    );

    // --- executions -------------------------------------------------------
    let execution_messages = journal.executions();
    assert_eq!(
        &wire(&execution_messages)[..prefix_exec_count],
        &clean_execution_wire[..prefix_exec_count],
        "the committed prefix was appended exactly once"
    );
    assert_two_identical_copies(
        &execution_messages[prefix_exec_count..],
        clean_executions.len() - prefix_exec_count,
    );
    let combined_executions = decode_executions(&execution_messages);
    assert_eq!(
        &combined_executions[..prefix_exec_count],
        &clean_executions[..prefix_exec_count],
        "the prefix decodes to the clean sequence"
    );
    let exec_counts = execution_counts(&combined_executions);
    assert_eq!(
        exec_counts.len(),
        clean_executions.len(),
        "the identity is unique inside one clean run"
    );
    for report in &clean_executions[..prefix_exec_count] {
        assert_eq!(
            exec_counts[&execution_id(report)],
            1,
            "a committed execution must not be duplicated"
        );
    }
    for report in &clean_executions[prefix_exec_count..] {
        assert_eq!(
            exec_counts[&execution_id(report)],
            2,
            "every execution appended before the failed commit must appear exactly twice"
        );
    }
    assert_eq!(
        dedup_executions(&combined_executions),
        clean_executions,
        "deduplicating by (symbol, cause_offset, seq, kind) restores the single clean run"
    );

    // --- market data ------------------------------------------------------
    let md_messages = journal.md();
    assert_eq!(
        &wire(&md_messages)[..prefix_md_count],
        &clean_md_wire[..prefix_md_count],
        "the committed prefix MD is appended exactly once"
    );
    assert_two_identical_copies(
        &md_messages[prefix_md_count..],
        clean_md.len() - prefix_md_count,
    );
    let combined_md = decode_market_data(&md_messages);
    assert_eq!(
        &combined_md[..prefix_md_count],
        &clean_md[..prefix_md_count],
        "the prefix decodes to the clean MD sequence"
    );
    let md_counts = market_data_counts(&combined_md);
    assert_eq!(
        md_counts.len(),
        clean_md.len(),
        "the MD identity is unique inside one clean run"
    );
    for event in &clean_md[..prefix_md_count] {
        assert_eq!(
            md_counts[&market_data_id(event)],
            1,
            "a committed MD delta must not be duplicated"
        );
    }
    for event in &clean_md[prefix_md_count..] {
        assert_eq!(
            md_counts[&market_data_id(event)],
            2,
            "every MD delta appended before the failed commit must appear exactly twice"
        );
    }
    assert_eq!(
        dedup_market_data(&combined_md),
        clean_md,
        "deduplicating by (symbol, cause_offset, seq, kind) restores the single clean MD run"
    );

    // --- state ------------------------------------------------------------
    let recovered = restart_run
        .snapshot
        .as_ref()
        .expect("the replay life has state to snapshot");
    assert_eq!(
        symbol_hashes(recovered),
        symbol_hashes(clean.snapshot.as_ref().expect("clean snapshot")),
        "the replayed state hash equals the clean single-run state hash"
    );
    assert_eq!(
        snapshot_applied_offset(recovered),
        Some(last_offset),
        "the recovered snapshot records the last applied offset"
    );

    // Stated plainly: this is at-least-once. The duplicate records are real
    // journal records; only a consumer that deduplicates by the stable identity
    // above sees exactly one business effect. Nothing here claims exactly-once.
}

// ---------------------------------------------------------------------------
// Test B: matcher died after reading input, before any output append
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matcher_death_before_output_append_replays_without_duplicates() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let exchange = ExchangeConfig::default();
    let symbol = btc();
    let shard_id = symbol.partition(exchange.partitions);
    let commands = command_log();
    let prefix = prefix_records(&commands);
    let tail = tail_records(&commands);

    let clean_journal = RecordingJournal::accepting();
    let clean = drive_shard(
        &ctx,
        &clean_journal,
        shard_id,
        ShardEngine::new(shard_id, &exchange),
        0,
        None,
        InputBatch::durable(records(&commands)),
    )
    .await;
    let clean_executions = decode_executions(&clean_journal.executions());
    let clean_md = decode_market_data(&clean_journal.md());

    // Committed prefix. The prefix snapshot is the resume point.
    let journal = RecordingJournal::accepting();
    let prefix_run = drive_shard(
        &ctx,
        &journal,
        shard_id,
        ShardEngine::new(shard_id, &exchange),
        0,
        None,
        InputBatch::durable(prefix.clone()),
    )
    .await;
    let prefix_snapshot = prefix_run
        .snapshot
        .as_ref()
        .expect("the prefix life has state to snapshot");
    assert_eq!(journal.committed_offset(), Some(PREFIX_OFFSET));

    let appends_before = journal.appends().len();
    let commit_attempts_before = journal.commit_attempts().len();
    let executions_before = wire(&journal.executions());
    let md_before = wire(&journal.md());

    // The matcher reads and matches the tail, then finds its output ring gone.
    crash_matcher_before_output(
        &ctx,
        shard_id,
        restore_engine(&exchange, prefix_snapshot),
        PREFIX_OFFSET,
        InputBatch::durable(tail.clone()),
    )
    .await;

    assert_eq!(
        journal.appends().len(),
        appends_before,
        "a matcher that dies before the output batch reaches the writer appends nothing"
    );
    assert_eq!(
        journal.commit_attempts().len(),
        commit_attempts_before,
        "no commit is attempted for an output that was never appended"
    );
    assert_eq!(
        journal.committed_offset(),
        Some(PREFIX_OFFSET),
        "no offset advances beyond persisted output"
    );
    assert_eq!(
        wire(&journal.executions()),
        executions_before,
        "the execution log is unchanged by the dead matcher"
    );
    assert_eq!(wire(&journal.md()), md_before, "the MD log is unchanged");

    // Restart at the committed prefix and replay. Nothing was appended, so the
    // replay is the *first* durable copy of the tail, not a duplicate.
    let restart_from = next_offset_after(journal.committed_offset());
    assert_eq!(restart_from, PREFIX_OFFSET + 1);
    let restart_run = drive_shard(
        &ctx,
        &journal.accepting_handle(),
        shard_id,
        restore_engine(&exchange, prefix_snapshot),
        PREFIX_OFFSET,
        Some(PREFIX_OFFSET),
        InputBatch::durable(tail.clone()),
    )
    .await;

    let last_offset = tail.last().expect("tail is not empty").offset;
    assert_eq!(journal.committed_offset(), Some(last_offset));

    let combined_executions = decode_executions(&journal.executions());
    let combined_md = decode_market_data(&journal.md());
    assert_eq!(
        combined_executions, clean_executions,
        "a pre-append matcher death leaves exactly one copy of every execution"
    );
    assert_eq!(
        combined_md, clean_md,
        "a pre-append matcher death leaves exactly one copy of every MD record"
    );
    assert!(
        execution_counts(&combined_executions)
            .values()
            .all(|count| *count == 1),
        "no execution identity appears twice"
    );
    assert!(
        market_data_counts(&combined_md)
            .values()
            .all(|count| *count == 1),
        "no MD identity appears twice"
    );

    let recovered = restart_run
        .snapshot
        .as_ref()
        .expect("the replay life has state to snapshot");
    assert_eq!(
        symbol_hashes(recovered),
        symbol_hashes(clean.snapshot.as_ref().expect("clean snapshot")),
        "the recovered state matches the clean single run"
    );
}

// ---------------------------------------------------------------------------
// Test C: the commit landed, then the writer died before the next batch
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_offset_is_the_last_applied_batch_and_replay_resumes_after_it() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let exchange = ExchangeConfig::default();
    let symbol = btc();
    let shard_id = symbol.partition(exchange.partitions);
    let commands = command_log();
    let prefix = prefix_records(&commands);
    let tail = tail_records(&commands);

    let clean_journal = RecordingJournal::accepting();
    let clean = drive_shard(
        &ctx,
        &clean_journal,
        shard_id,
        ShardEngine::new(shard_id, &exchange),
        0,
        None,
        InputBatch::durable(records(&commands)),
    )
    .await;
    let clean_executions = decode_executions(&clean_journal.executions());
    let clean_md = decode_market_data(&clean_journal.md());

    // Life 1: apply and commit the prefix.
    let journal = RecordingJournal::accepting();
    let prefix_run = drive_shard(
        &ctx,
        &journal,
        shard_id,
        ShardEngine::new(shard_id, &exchange),
        0,
        None,
        InputBatch::durable(prefix.clone()),
    )
    .await;
    let prefix_snapshot = prefix_run
        .snapshot
        .as_ref()
        .expect("the prefix life has state to snapshot");
    assert_eq!(
        prefix_run.applied_offset, PREFIX_OFFSET,
        "the prefix life applied exactly the committed batch"
    );
    assert_eq!(
        journal.committed_offset(),
        Some(prefix_run.applied_offset),
        "the committed offset is the last applied batch"
    );
    let executions_after_commit = wire(&journal.executions());
    let md_after_commit = wire(&journal.md());
    let appends_after_commit = journal.appends().len();
    let attempts_after_commit = journal.commit_attempts().len();

    // The writer dies before another batch exists: no append, no commit.
    run_idle_writer(
        &ctx,
        &journal.accepting_handle(),
        shard_id,
        journal.committed_offset(),
    )
    .await;
    assert_eq!(
        journal.committed_offset(),
        Some(PREFIX_OFFSET),
        "a writer that applied nothing cannot move the committed offset"
    );
    assert_eq!(
        wire(&journal.executions()),
        executions_after_commit,
        "the committed prefix is not rewritten by an idle writer life"
    );
    assert_eq!(wire(&journal.md()), md_after_commit);
    assert_eq!(journal.appends().len(), appends_after_commit);
    assert_eq!(
        journal.commit_attempts().len(),
        attempts_after_commit,
        "an idle writer has no applied offset to commit"
    );

    // Replay starts strictly after the committed prefix, so there is nothing
    // from the committed prefix left to duplicate.
    let restart_from = next_offset_after(journal.committed_offset());
    assert_eq!(restart_from, PREFIX_OFFSET + 1);
    assert_eq!(
        tail.first().expect("tail is not empty").offset,
        restart_from,
        "the tail begins exactly at the first uncommitted offset"
    );

    let restart_run = drive_shard(
        &ctx,
        &journal.accepting_handle(),
        shard_id,
        restore_engine(&exchange, prefix_snapshot),
        PREFIX_OFFSET,
        Some(PREFIX_OFFSET),
        InputBatch::durable(tail.clone()),
    )
    .await;

    let last_offset = tail.last().expect("tail is not empty").offset;
    assert_eq!(journal.committed_offset(), Some(last_offset));

    let combined_executions = decode_executions(&journal.executions());
    let combined_md = decode_market_data(&journal.md());
    assert_eq!(
        combined_executions, clean_executions,
        "resuming after the committed prefix appends no duplicate execution"
    );
    assert_eq!(
        combined_md, clean_md,
        "resuming after the committed prefix appends no duplicate MD record"
    );
    assert!(
        execution_counts(&combined_executions)
            .values()
            .all(|count| *count == 1),
        "every execution identity appears exactly once"
    );
    assert!(
        market_data_counts(&combined_md)
            .values()
            .all(|count| *count == 1),
        "every MD identity appears exactly once"
    );

    let recovered = restart_run
        .snapshot
        .as_ref()
        .expect("the replay life has state to snapshot");
    assert_eq!(
        symbol_hashes(recovered),
        symbol_hashes(clean.snapshot.as_ref().expect("clean snapshot")),
        "the recovered state matches the clean single run"
    );
}
