// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Turbo pipeline tests: the matcher/sequencer agreement with the durable path,
//! the ack-before-journal guarantee, and the startup anchor check.
//!
//! The differential test is the load-bearing one. Turbo is only a fair second
//! mode if the same commands produce the same executions, the same market data,
//! and the same state hash as the durable path. That comparison is only valid
//! when the local seq space and the broker offset space coincide, so the test
//! states and asserts that precondition instead of assuming it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::sync::{mpsc, oneshot, watch};
use whippet::config::{JournalKind, NodeConfig};
use whippet::context::NodeContext;
use whippet::engine_runner::{
    InputBatch, OutputBatch, SHARD_RING_CAPACITY, run_engine_shard, run_matcher,
};
use whippet::snapshot::{SequencedSnapshot, SnapshotStore, SnapshotWriter};
use whippet::turbo::{
    AdmitError, EnqueueError, TurboConfig, TurboEntry, TurboIngress, TurboWindow, now_ns,
};
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::headers::{HeaderMode, MessageMeta, SCHEMA_ORDER};
use whippet_core::ids::Seq;
use whippet_core::protocol::{Command, RejectReason};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{Journal, JournalMessage, Topic};
use whippet_marketdata::MdHub;
use whippet_testkit::{GoldenLog, scenario_config, standard_shard};

/// The exchange every turbo test runs against: one instrument, the default
/// partition count.
fn test_exchange() -> ExchangeConfig {
    ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Turbo,
        instruments: vec![scenario_config()],
    }
}

/// A node wired in memory with turbo as the configured mode. The shard
/// receivers are created per test, so `ctx.turbo` stays `None`: the shard task
/// only ever sees what `run_engine_shard` is handed.
fn test_node() -> (Arc<NodeContext>, TempDir) {
    test_node_with(test_exchange(), None)
}

fn test_node_with(
    exchange: ExchangeConfig,
    turbo: Option<Arc<whippet::turbo::TurboIngress>>,
) -> (Arc<NodeContext>, TempDir) {
    let tempdir = tempfile::tempdir().expect("temp dir");
    let config = NodeConfig {
        http_addr: "127.0.0.1:0".parse().expect("valid test address"),
        tcp_addr: None,
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        journal: JournalKind::Memory,
        mode: Mode::Turbo,
        exchange,
        snapshot_dir: tempdir.path().join("snapshots"),
        snapshot_every: 10_000,
        dashboard_dir: tempdir.path().join("dashboard"),
        build: "test".into(),
        api_token: None,
        api_participant: "api".into(),
        api_tokens: Vec::new(),
        operator_tokens: Vec::new(),
        metrics_token: None,
        rate_limit_per_sec: 0,
        ws_per_ip: 0,
        risk: whippet::config::RiskConfig::default(),
        turbo: TurboConfig::default(),
        storage: whippet::storage::StorageConfig {
            whippet_dir: tempdir.path().join("data"),
            iggy_dir: None,
            ..whippet::storage::StorageConfig::default()
        },
    };
    let journal = Arc::new(Journal::memory());
    let engine_journals = vec![Arc::clone(&journal)];
    let group_commit = whippet_journal::group::GroupCommit::new(
        Arc::clone(&journal),
        whippet_journal::group::GroupCommitConfig {
            max_batch: 256,
            linger: Duration::ZERO,
        },
    );
    let hub = MdHub::new(
        config.exchange.clone(),
        config.iggy_addr.clone(),
        "memory".into(),
        Some("test".into()),
    );
    let metrics = hub.metrics();
    let (shutdown, _) = watch::channel(false);
    let rate_limiter = Arc::new(whippet::ratelimit::RateLimiter::new(0));
    let ws_connections = Arc::new(tokio::sync::Semaphore::new(512));
    let storage = whippet::storage::StorageMonitor::new(config.storage.clone());
    let ctx = Arc::new(NodeContext {
        config,
        journal: Arc::clone(&journal),
        engine_journals,
        engine_journal: Arc::clone(&journal),
        group_commit,
        hub,
        metrics,
        storage,
        snapshots: SnapshotStore::new(tempdir.path().join("snapshots")),
        started: Instant::now(),
        shutdown,
        shards_ok: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        // The test drives `run_engine_shard` directly; readiness is not gated.
        health: whippet::health::ShardHealth::new(),
        active_shards: Vec::new(),
        rate_limiter,
        ws_connections,
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo,
    });
    (ctx, tempdir)
}

/// The engine a shard builds at startup, with the snapshot cadences the shard
/// sets. Both sides of the differential test use this so the only difference is
/// the input path.
fn shard_engine(shard: u32, exchange: &ExchangeConfig) -> ShardEngine {
    let mut engine = ShardEngine::new_with_snapshot_interval(shard, exchange, 10_000);
    // No market-data snapshots inside the golden log: the comparison is about
    // executions and MD events, and a snapshot interval that lands mid-run
    // would only add noise to both sides.
    engine.set_md_snapshot_interval(1_000_000);
    engine
}

/// The exact order message the gateway builds for a command, so the turbo
/// `orders` payloads can be compared to the ingress encoding.
fn order_message(command: &Command) -> JournalMessage {
    let meta = MessageMeta::new(
        command.symbol,
        Seq(0),
        "order",
        SCHEMA_ORDER,
        command.order_id.0,
    );
    JournalMessage::new_named(command.encode(), meta.named_pairs(HeaderMode::Full))
}

fn admit(
    window: &Arc<TurboWindow>,
    command: &Command,
) -> (TurboEntry, oneshot::Receiver<whippet::turbo::TurboAck>) {
    let (ack, receiver) = oneshot::channel();
    let entry = TurboEntry {
        command: command.clone(),
        message: order_message(command),
        admission: window.try_admit(now_ns()).expect("the test window admits"),
        ack,
        received_ns: now_ns(),
    };
    (entry, receiver)
}

fn spawn_matcher(
    ctx: &Arc<NodeContext>,
    shard: u32,
    exchange: &ExchangeConfig,
    input: mpsc::Receiver<InputBatch>,
    output: mpsc::Sender<OutputBatch>,
) -> std::thread::JoinHandle<()> {
    let (recycle_tx, _recycle_rx) =
        mpsc::channel::<Vec<whippet_journal::PayloadRecord>>(SHARD_RING_CAPACITY);
    let matcher_ctx = Arc::clone(ctx);
    let engine = shard_engine(shard, exchange);
    // `u64::MAX` is the writer's "nothing durable yet" sentinel, so no periodic
    // snapshot can open during the test; the final batch is what we inspect.
    let appended = Arc::new(AtomicU64::new(u64::MAX));
    // Ring-occupancy counters this test does not observe.
    let input_queued = Arc::new(AtomicU32::new(0));
    let output_queued = Arc::new(AtomicU32::new(0));
    std::thread::Builder::new()
        .name("turbo-test-matcher".into())
        .spawn(move || {
            run_matcher(
                matcher_ctx,
                shard,
                engine,
                input,
                output,
                recycle_tx,
                appended,
                // Fresh log: the durable domain would start at offset 0.
                0,
                // Turbo local seq starts at 0 too, which is the precondition the
                // differential test asserts before comparing hashes.
                0,
                input_queued,
                output_queued,
            )
        })
        .expect("spawn the matcher")
}

/// Turbo matcher output drained in order.
struct Matched {
    executions: Vec<Vec<u8>>,
    md: Vec<Vec<u8>>,
    orders: Vec<Vec<u8>>,
    acks: Vec<whippet::turbo::TurboAck>,
    final_snapshot: Option<SequencedSnapshot>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_matches_the_durable_path_on_the_golden_log() {
    let (ctx, _tempdir) = test_node();
    let exchange = ctx.exchange().clone();
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();
    assert!(!commands.is_empty(), "the golden log must not be empty");

    // (a) Durable reference. The log offset is the command's index, exactly as
    // a fresh journal partition numbers the same records.
    let mut reference = shard_engine(shard, &exchange);
    let mut out = EngineOutput::with_capacity(64, 64);
    let mut reference_executions = Vec::with_capacity(64);
    let mut reference_md = Vec::with_capacity(64);
    for (offset, command) in commands.iter().enumerate() {
        reference
            .process(command, offset as u64, &mut out)
            .expect("the standard scenario stays on one shard");
        assert!(
            out.snapshots.is_empty(),
            "the golden log must not emit MD snapshots during the comparison"
        );
        reference_executions.extend(out.executions.iter().map(|execution| execution.encode()));
        reference_md.extend(out.md.iter().map(|event| event.encode()));
        out.clear();
    }
    let reference_hash = reference.state_hash();

    // (b) Turbo: typed commands from the ingress lane, matched by the matcher.
    let window = TurboWindow::new(TurboConfig::new(commands.len(), 60_000).expect("test window"));
    let mut entries = Vec::with_capacity(commands.len());
    let mut ack_receivers = Vec::with_capacity(commands.len());
    for command in &commands {
        let (entry, receiver) = admit(&window, command);
        entries.push(entry);
        ack_receivers.push(receiver);
    }

    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    let (output_tx, mut output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    let matcher = spawn_matcher(&ctx, shard, &exchange, input_rx, output_tx);

    let split = entries.len() / 2;
    let mut entries = entries.into_iter();
    input_tx
        .send(InputBatch::turbo(entries.by_ref().take(split).collect()))
        .await
        .expect("the input ring accepts the first half");
    input_tx
        .send(InputBatch::turbo(entries.collect()))
        .await
        .expect("the input ring accepts the second half");
    drop(input_tx);

    let mut matched = Matched {
        executions: Vec::with_capacity(reference_executions.len()),
        md: Vec::with_capacity(reference_md.len()),
        orders: Vec::with_capacity(commands.len()),
        acks: Vec::with_capacity(commands.len()),
        final_snapshot: None,
    };
    while let Some(batch) = output_rx.recv().await {
        matched.executions.extend(
            batch
                .executions
                .into_iter()
                .map(|message| message.payload.to_vec()),
        );
        matched
            .md
            .extend(batch.md.into_iter().map(|message| message.payload.to_vec()));
        // Dropping each record's admission releases its window slot; the test
        // owns the writer's role here.
        matched.orders.extend(
            batch
                .orders
                .into_iter()
                .map(|record| record.message.payload.to_vec()),
        );
        if batch.final_batch {
            matched.final_snapshot = batch.snapshot;
            break;
        }
    }
    matcher
        .join()
        .expect("the matcher exits after input closes");

    for receiver in ack_receivers {
        matched.acks.push(
            receiver
                .await
                .expect("the matcher acknowledges every turbo command"),
        );
    }
    // The precondition, asserted rather than assumed: both runs number the
    // commands 0..n, so the turbo seq domain equals the durable offset domain
    // and the two state hashes are directly comparable.
    let expected_seqs: Vec<u64> = (0..commands.len() as u64).collect();
    assert_eq!(
        matched.acks.iter().map(|ack| ack.seq).collect::<Vec<_>>(),
        expected_seqs,
        "turbo must assign the same seq the durable path would use as an offset"
    );

    assert_eq!(
        matched.executions, reference_executions,
        "turbo executions must be byte-identical and in the same order"
    );
    assert_eq!(
        matched.md, reference_md,
        "turbo market data must be byte-identical and in the same order"
    );
    let expected_orders: Vec<Vec<u8>> = commands.iter().map(Command::encode).collect();
    assert_eq!(
        matched.orders, expected_orders,
        "the write-behind must append the ingress command encoding, one record per seq"
    );
    assert_eq!(
        window.in_flight(),
        0,
        "every matched command's window slot must be released once its record is handled"
    );

    let mut final_snapshot = matched
        .final_snapshot
        .expect("a run with commands persists a final snapshot");
    assert_eq!(
        final_snapshot.sequence, 1,
        "the final snapshot is the first"
    );
    let writer = SnapshotWriter::with_exchange(ctx.snapshots.clone(), exchange.clone());
    writer
        .fill_hashes(&mut final_snapshot.snapshot)
        .expect("the writer fills the hashes off the match thread");
    let restored = ShardEngine::from_snapshot(&exchange, final_snapshot.snapshot)
        .expect("the final snapshot verifies against its hashes");
    assert_eq!(
        restored.state_hash(),
        reference_hash,
        "the turbo final state hash diverged from the durable reference"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ack_arrives_before_the_writer_drains_anything() {
    let (ctx, _tempdir) = test_node();
    let exchange = ctx.exchange().clone();
    let shard = standard_shard(&exchange);
    let command = GoldenLog::standard().commands()[0].clone();

    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    // One slot, pre-filled: the matcher's batch cannot fit, so it parks in
    // `blocking_send` right after the match. That is a writer that never runs.
    let (output_tx, mut output_rx) = mpsc::channel::<OutputBatch>(1);
    output_tx
        .send(OutputBatch {
            executions: Vec::new(),
            md: Vec::new(),
            orders: Vec::new(),
            last_offset: 0,
            applied: false,
            snapshot: None,
            final_batch: false,
        })
        .await
        .expect("the ring holds the blocker");

    let matcher = spawn_matcher(&ctx, shard, &exchange, input_rx, output_tx);

    let window = TurboWindow::new(TurboConfig::new(1, 50).expect("test window"));
    let (entry, receiver) = admit(&window, &command);
    input_tx
        .send(InputBatch::turbo(vec![entry]))
        .await
        .expect("the input ring accepts the command");

    // The writer never drains, so this ack can only come from the matcher.
    let ack = tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .expect("the ack must not wait for the journal writer")
        .expect("the matcher acknowledges the command");
    assert_eq!(ack.seq, 0);
    assert!(ack.accepted, "the first golden command is accepted");
    assert_eq!(ack.reject, RejectReason::None);

    // Teardown: closing the ring unblocks the parked matcher.
    drop(input_tx);
    drop(output_rx.try_recv().expect("the blocker is still queued"));
    output_rx.close();
    while output_rx.recv().await.is_some() {}
    matcher.join().expect("the matcher exits cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_ack_receiver_does_not_stop_matching() {
    let (ctx, _tempdir) = test_node();
    let exchange = ctx.exchange().clone();
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();

    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    let (output_tx, mut output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    let matcher = spawn_matcher(&ctx, shard, &exchange, input_rx, output_tx);

    let window = TurboWindow::new(TurboConfig::new(2, 50).expect("test window"));
    let (first, first_receiver) = admit(&window, &commands[0]);
    let (second, second_receiver) = admit(&window, &commands[1]);
    // The client gave up on the first order before the matcher ran.
    drop(first_receiver);

    input_tx
        .send(InputBatch::turbo(vec![first, second]))
        .await
        .expect("the input ring accepts the batch");
    drop(input_tx);

    let mut orders = Vec::new();
    while let Some(batch) = output_rx.recv().await {
        orders.extend(
            batch
                .orders
                .into_iter()
                .map(|record| record.message.payload.to_vec()),
        );
        if batch.final_batch {
            break;
        }
    }
    matcher
        .join()
        .expect("the matcher must not panic on a closed ack");

    let ack = second_receiver
        .await
        .expect("the second command is still acknowledged");
    assert_eq!(ack.seq, 1, "the seq is assigned even when no one listens");
    assert_eq!(
        orders,
        vec![commands[0].encode(), commands[1].encode()],
        "both commands are still journaled in seq order"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_refuses_a_non_empty_orders_log_without_a_snapshot() {
    let (ctx, _tempdir) = test_node();
    let shard = standard_shard(ctx.exchange());
    // One order record is already at offset 0: the local seq domain has no
    // anchor, and starting would assign duplicate cause_offsets.
    ctx.journal
        .append(
            Topic::Orders,
            shard,
            &[JournalMessage::new(vec![0], Vec::new())],
        )
        .await
        .expect("seed the orders log");
    let (_tx, rx) = mpsc::channel(1);

    let error = run_engine_shard(Arc::clone(&ctx), shard, Some(rx))
        .await
        .expect_err("a non-empty log without a snapshot must not start");
    assert!(
        error
            .to_string()
            .contains("turbo cannot anchor the local seq domain"),
        "unexpected error: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_starts_on_an_empty_orders_log() {
    let (ctx, _tempdir) = test_node();
    let shard = standard_shard(ctx.exchange());
    let (_tx, rx) = mpsc::channel(1);

    let task = tokio::spawn(run_engine_shard(Arc::clone(&ctx), shard, Some(rx)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.shutdown.send_replace(true);

    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the shard stops inside the shutdown budget")
        .expect("the shard task runs")
        .expect("an empty orders log is a valid turbo anchor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_replays_the_journaled_tail_before_accepting_ingress() {
    let exchange = test_exchange();
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();

    // A lane for the shard, then a context whose journal and snapshot
    // directory the test owns.
    let (ingress, mut receivers) = TurboIngress::new(TurboConfig::default(), [shard]);
    let (ctx, _tempdir) = test_node_with(exchange.clone(), Some(Arc::clone(&ingress)));

    // Snapshot the state after command 0 (offset 0). The log will carry six
    // commands, so the snapshot is five records behind the head.
    let mut reference = shard_engine(shard, &exchange);
    let mut out = EngineOutput::with_capacity(8, 8);
    reference
        .process(&commands[0], 0, &mut out)
        .expect("the scenario stays on one shard");
    let mut writer = SnapshotWriter::with_exchange(ctx.snapshots.clone(), exchange.clone());
    writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: reference.snapshot_state(),
        })
        .expect("the snapshot verifies")
        .expect("the snapshot writes");

    let records: Vec<JournalMessage> = commands[..6]
        .iter()
        .map(|command| JournalMessage::new(command.encode(), Vec::new()))
        .collect();
    ctx.journal
        .append(Topic::Orders, shard, &records)
        .await
        .expect("seed the orders log");

    let receiver = receivers.remove(&shard).expect("a lane per shard");
    let task = tokio::spawn(run_engine_shard(Arc::clone(&ctx), shard, Some(receiver)));

    // The ack can only arrive after the tail replay: the lane is not read
    // until the engine has applied offsets 1..=5.
    let ack = ingress
        .enqueue(shard, commands[6].clone(), order_message(&commands[6]))
        .expect("the window admits the probe");
    let ack = tokio::time::timeout(Duration::from_secs(10), ack)
        .await
        .expect("the shard acknowledges after replay")
        .expect("the shard keeps the ack sender");
    assert_eq!(
        ack.seq, 6,
        "the first live command follows the replayed tail"
    );

    for (offset, command) in commands.iter().enumerate().take(7).skip(1) {
        reference
            .process(command, offset as u64, &mut out)
            .expect("the reference applies the same tail");
        out.clear();
    }

    ctx.shutdown.send_replace(true);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the shard stops inside the shutdown budget")
        .expect("the shard task runs")
        .expect("the shard stops cleanly");

    let snapshot = ctx
        .snapshots
        .load(shard)
        .expect("the final snapshot loads")
        .expect("the final snapshot exists");
    assert_eq!(snapshot.symbols[0].last_offset, 6);
    let restored = ShardEngine::from_snapshot(&exchange, snapshot).expect("restores");
    assert_eq!(
        restored.state_hash(),
        reference.state_hash(),
        "the replayed tail must produce the same state as a continuous run"
    );
    // No duplicate appends: six seeded records plus the one live command.
    assert_eq!(
        ctx.journal
            .lag(Topic::Orders, shard, 0)
            .await
            .expect("lag reads"),
        6,
        "the writer must not append the replayed tail again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_starts_from_a_snapshot_anchored_at_zero() {
    let (ctx, _tempdir) = test_node();
    let exchange = ctx.exchange().clone();
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();

    // One command applied at offset 0, persisted as a verified snapshot: the
    // seq domain is anchored even though the log holds records behind it.
    let mut engine = shard_engine(shard, &exchange);
    let mut out = EngineOutput::with_capacity(8, 8);
    engine
        .process(&commands[0], 0, &mut out)
        .expect("the scenario stays on one shard");
    let mut writer = SnapshotWriter::with_exchange(ctx.snapshots.clone(), exchange.clone());
    writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: engine.snapshot_state(),
        })
        .expect("the snapshot verifies")
        .expect("the snapshot writes");

    // Six records, so the partition head is 5.
    let records: Vec<JournalMessage> = (0..6)
        .map(|_| JournalMessage::new(vec![0], Vec::new()))
        .collect();
    ctx.journal
        .append(Topic::Orders, shard, &records)
        .await
        .expect("seed the orders log");

    let (_tx, rx) = mpsc::channel(1);
    let task = tokio::spawn(run_engine_shard(Arc::clone(&ctx), shard, Some(rx)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.shutdown.send_replace(true);

    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("the shard stops inside the shutdown budget")
        .expect("the shard task runs")
        .expect("a snapshot at 0 with a log tail is a valid turbo anchor");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_turbo_node_never_falls_back_to_the_durable_reader() {
    let exchange = test_exchange();
    let shard = standard_shard(&exchange);
    let (ingress, _receivers) = whippet::turbo::TurboIngress::new(TurboConfig::default(), [shard]);
    let (ctx, _tempdir) = test_node_with(exchange, Some(ingress));

    // No lane for the shard: mode is fixed at startup, so this is a wiring bug
    // and must fail rather than start matching from the orders log.
    let error = run_engine_shard(Arc::clone(&ctx), shard, None)
        .await
        .expect_err("a turbo node must not silently become durable");
    assert!(
        error.to_string().contains("refusing to fall back"),
        "unexpected error: {error}"
    );
}

// --- Admission lifecycle -----------------------------------------------------
//
// The window is the crash-loss bound, so its admission lifecycle is checked,
// not just its limits: which bound rejected, that a freed slot admits again,
// that an abandoned entry leaks nothing, and that the bookkeeping invariants
// survive a randomized admit/complete/abandon sequence.

/// A dead lane: the shard receiver is gone, so `try_send` fails closed.
fn dead_lane_ingress(config: TurboConfig) -> (Arc<TurboIngress>, u32) {
    let shard = 0;
    let (ingress, mut receivers) = TurboIngress::new(config, [shard]);
    drop(receivers.remove(&shard));
    (ingress, shard)
}

#[test]
fn enqueue_reports_the_binding_bound_and_frees_slots_on_failure() {
    let command = GoldenLog::standard().commands()[0].clone();
    let message = JournalMessage::new(vec![0], Vec::new());

    // The count bound is asserted at the window level below, where the clock
    // is fillable deterministically; here the lane is dead, so every send
    // fails closed after admission and the abandoned entry must release its
    // permit either way.
    let config = TurboConfig::new(2, 60_000).expect("valid config");
    let (ingress, shard) = dead_lane_ingress(config);
    assert!(
        matches!(
            ingress.enqueue(shard, command.clone(), message.clone()),
            Err(EnqueueError::ShardUnavailable)
        ),
        "a dead lane fails closed"
    );
    let window = ingress.window(shard).expect("a lane exists");
    assert_eq!(window.in_flight(), 0, "the failed send frees the slot");
    window.check_invariants().expect("invariants hold");

    // Age bound: a tiny window rejects by age, and the reason is distinct
    // from the count reason.
    let window = TurboWindow::new(TurboConfig::new(2, 1).expect("valid config"));
    let held = window.try_admit(now_ns()).expect("first admits");
    std::thread::sleep(Duration::from_millis(2));
    assert!(
        matches!(window.try_admit(now_ns()), Err(AdmitError::AgeFull)),
        "a window older than 1 ms rejects by age, not count"
    );
    assert_eq!(window.drop_reasons(), (0, 1));
    assert_eq!(window.drops(), 1);
    window.check_invariants().expect("invariants hold");
    drop(held);

    // Count bound at the window level: the third admit of a two-slot window
    // is rejected by count, the counters split, and completing one admission
    // frees a slot that admits again.
    let window = TurboWindow::new(TurboConfig::new(2, 60_000).expect("valid config"));
    let mut first = window.try_admit(1_000).expect("first");
    let second = window.try_admit(2_000).expect("second");
    assert!(
        matches!(window.try_admit(3_000), Err(AdmitError::CountFull)),
        "a full window rejects by count, not age"
    );
    assert_eq!(window.drop_reasons(), (1, 0));
    first.complete();
    assert!(
        window.try_admit(4_000).is_ok(),
        "a freed slot must admit again"
    );
    drop(second);
    window.check_invariants().expect("invariants hold");
}

#[test]
fn completing_admissions_out_of_order_releases_every_slot() {
    // The writer completes admissions in seq order, so the window front is
    // the id being completed and the release is an O(1) pop. A raced send
    // failure or a disconnect can abandon an entry ahead of its predecessors;
    // the release fallback search then removes it from the middle of the
    // clock, because a leaked slot would be a permanent capacity loss. Either
    // way the slot count must stay exact.
    let window = TurboWindow::new(TurboConfig::new(3, 60_000).expect("valid config"));
    let mut first = window.try_admit(1_000).expect("first");
    let mut second = window.try_admit(2_000).expect("second");
    let mut third = window.try_admit(3_000).expect("third");
    window.check_invariants().expect("invariants hold");

    second.complete();
    window.check_invariants().expect("invariants hold");
    third.complete();
    window.check_invariants().expect("invariants hold");
    assert_eq!(window.in_flight(), 1, "the out-of-order entries released");
    // The front entry is the last to complete: the fast pop.
    first.complete();
    window.check_invariants().expect("invariants hold");
    assert_eq!(window.in_flight(), 0);
    assert_eq!(window.oldest_queued_ns(), None);
}

#[test]
fn randomized_admission_sequences_hold_invariants() {
    // A deterministic LCG (no dependency on a rand crate) drives admit,
    // complete, and abandon. The invariant check runs after every operation,
    // so any step that loses a permit or a clock entry fails here.
    let window = TurboWindow::new(TurboConfig::new(16, 1).expect("valid config"));
    let mut held: Vec<whippet::turbo::TurboAdmission> = Vec::new();
    // (count, age) rejections this driver has observed, to compare against
    // the window's counters at the end.
    let mut seen = (0u64, 0u64);
    let mut ts: u64 = 0;
    let mut state = 0x2F1E3D4C5B6A7988u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 33
    };

    for _ in 0..500 {
        ts += 200_000; // 0.2 ms per attempt, so the 1 ms age bound bites.
        match next() % 8 {
            0..=4 => match window.try_admit(ts) {
                Ok(admission) => held.push(admission),
                Err(AdmitError::CountFull) => seen.0 += 1,
                Err(AdmitError::AgeFull) => seen.1 += 1,
            },
            5..=7 => {
                if let Some(index) = (!held.is_empty()).then(|| next() as usize % held.len()) {
                    let mut admission = held.swap_remove(index);
                    if next() & 1 == 0 {
                        admission.complete();
                    } else {
                        drop(admission); // abandoned
                    }
                }
            }
            _ => unreachable!("modulo 8"),
        }
        window
            .check_invariants()
            .expect("invariants hold after every operation");
    }

    while let Some(mut admission) = held.pop() {
        if next() & 1 == 0 {
            admission.complete();
        } else {
            drop(admission);
        }
        window
            .check_invariants()
            .expect("invariants hold while draining");
    }
    assert_eq!(window.in_flight(), 0);
    assert_eq!(window.drop_reasons(), seen, "counters split by the bound");
    assert_eq!(window.drops(), seen.0 + seen.1);
    window.check_invariants().expect("invariants hold");
}

#[test]
fn an_admission_failed_send_abandons_its_entry_not_its_slot() {
    let config = TurboConfig::new(4, 60_000).expect("valid config");
    let (ingress, shard) = dead_lane_ingress(config);
    let command = GoldenLog::standard().commands()[0].clone();
    let message = JournalMessage::new(vec![0], Vec::new());
    for _ in 0..4 {
        let result = ingress.enqueue(shard, command.clone(), message.clone());
        assert!(
            matches!(result, Err(EnqueueError::ShardUnavailable)),
            "a dead lane fails closed"
        );
    }
    let window = ingress.window(shard).expect("a lane exists");
    assert_eq!(
        window.in_flight(),
        0,
        "no failed send may leak a window slot"
    );
    assert_eq!(ingress.total_in_flight(), 0);
    window.check_invariants().expect("invariants hold");
}
