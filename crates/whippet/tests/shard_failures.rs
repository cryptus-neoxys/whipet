// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Fault injection for the shard loop.
//!
//! The shard's journal edge is the `JournalOps` trait. The fakes below
//! implement it so the failure paths — an output append that fails once, an
//! append that never succeeds, a panicking matcher, full rings, and a
//! non-contiguous reader batch — can be driven without a broker.

mod common;

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use whippet::engine_runner::{
    APPEND_MAX_ATTEMPTS, InputBatch, JournalOps, OutputBatch, SHARD_RING_CAPACITY,
    TurboJournalRecord, join_matcher, read_orders, run_matcher, run_writer,
};
use whippet::turbo::{TurboConfig, TurboWindow, now_ns};
use whippet_core::config::ExchangeConfig;
use whippet_engine::ShardEngine;
use whippet_journal::{
    AppendResult, JournalError, JournalMessage, PayloadRecord, Result as JournalResult, Topic,
};

use common::TestNode;

/// What the fake journal saw, in order. Ordering is the assertion: no commit
/// may appear before the execution append that made the batch durable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    AppendFailed(Topic),
    AppendOk(Topic),
    Commit(u64),
}

struct FakeJournal {
    /// Executions appends that fail before one succeeds. `usize::MAX` keeps
    /// failing forever (the "wedged output topic" case).
    executions_failures: AtomicUsize,
    /// Orders appends that fail before one succeeds; `usize::MAX` keeps
    /// failing (the turbo write-behind's "journal down" case).
    orders_failures: AtomicUsize,
    attempts: AtomicUsize,
    events: Mutex<Vec<Event>>,
    committed: Mutex<Option<u64>>,
    executions_log: Mutex<Vec<Vec<u8>>>,
    orders_log: Mutex<Vec<Vec<u8>>>,
    /// Shared with the writer under test so a failure can record how far the
    /// applied offset had advanced at that moment.
    appended: Arc<AtomicU64>,
    appended_at_failure: Mutex<Vec<u64>>,
    /// When set, every poll returns this fixed batch; `None` returns empty.
    reader_offsets: Option<Vec<u64>>,
    polls: AtomicUsize,
    /// When set, an orders append parks here until the test frees a permit, so
    /// the window slot can be observed while the append is still in flight.
    orders_gate: Option<Arc<tokio::sync::Semaphore>>,
    orders_started: AtomicBool,
}

impl FakeJournal {
    fn failing_executions_once(appended: Arc<AtomicU64>) -> Self {
        Self::new(appended, 1, 0, None, None)
    }

    fn failing_executions_forever(appended: Arc<AtomicU64>) -> Self {
        Self::new(appended, usize::MAX, 0, None, None)
    }

    fn failing_orders_forever(appended: Arc<AtomicU64>) -> Self {
        Self::new(appended, 0, usize::MAX, None, None)
    }

    fn gated_on_orders(appended: Arc<AtomicU64>, gate: Arc<tokio::sync::Semaphore>) -> Self {
        Self::new(appended, 0, 0, None, Some(gate))
    }

    fn with_reader(offsets: Vec<u64>) -> Self {
        Self::new(Arc::new(AtomicU64::new(0)), 0, 0, Some(offsets), None)
    }

    fn new(
        appended: Arc<AtomicU64>,
        executions_failures: usize,
        orders_failures: usize,
        reader_offsets: Option<Vec<u64>>,
        orders_gate: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Self {
        Self {
            executions_failures: AtomicUsize::new(executions_failures),
            orders_failures: AtomicUsize::new(orders_failures),
            attempts: AtomicUsize::new(0),
            events: Mutex::new(Vec::new()),
            committed: Mutex::new(None),
            executions_log: Mutex::new(Vec::new()),
            orders_log: Mutex::new(Vec::new()),
            appended,
            appended_at_failure: Mutex::new(Vec::new()),
            reader_offsets,
            polls: AtomicUsize::new(0),
            orders_gate,
            orders_started: AtomicBool::new(false),
        }
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }

    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }

    fn committed(&self) -> Option<u64> {
        *self.committed.lock().unwrap()
    }

    fn executions(&self) -> Vec<Vec<u8>> {
        self.executions_log.lock().unwrap().clone()
    }

    fn orders(&self) -> Vec<Vec<u8>> {
        self.orders_log.lock().unwrap().clone()
    }

    fn appended_at_failure(&self) -> Vec<u64> {
        self.appended_at_failure.lock().unwrap().clone()
    }

    fn polls(&self) -> usize {
        self.polls.load(Ordering::SeqCst)
    }

    fn orders_started(&self) -> bool {
        self.orders_started.load(Ordering::SeqCst)
    }
}

impl FakeJournal {
    /// Shared failure bookkeeping for an append that must fail.
    fn fail(&self, topic: Topic) -> JournalResult<AppendResult> {
        self.appended_at_failure
            .lock()
            .unwrap()
            .push(self.appended.load(Ordering::SeqCst));
        self.events.lock().unwrap().push(Event::AppendFailed(topic));
        Err(JournalError::NotConnected)
    }
}

impl JournalOps for FakeJournal {
    async fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: &[JournalMessage],
    ) -> JournalResult<AppendResult> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if topic == Topic::Executions {
            let left = self.executions_failures.load(Ordering::SeqCst);
            if left > 0 {
                if left != usize::MAX {
                    self.executions_failures.fetch_sub(1, Ordering::SeqCst);
                }
                return self.fail(topic);
            }
        }
        if topic == Topic::Orders {
            if let Some(gate) = &self.orders_gate {
                self.orders_started.store(true, Ordering::SeqCst);
                // Held to the end of the call: the test releases it to let the
                // append land.
                let _permit = gate.acquire().await;
            }
            let left = self.orders_failures.load(Ordering::SeqCst);
            if left > 0 {
                if left != usize::MAX {
                    self.orders_failures.fetch_sub(1, Ordering::SeqCst);
                }
                return self.fail(topic);
            }
        }

        let mut log = self.executions_log.lock().unwrap();
        let base_offset = log.len() as u64;
        if topic == Topic::Executions {
            for message in messages {
                log.push(message.payload.to_vec());
            }
        }
        drop(log);
        if topic == Topic::Orders {
            let mut orders = self.orders_log.lock().unwrap();
            for message in messages {
                orders.push(message.payload.to_vec());
            }
        }
        self.events.lock().unwrap().push(Event::AppendOk(topic));
        Ok(AppendResult {
            partition,
            base_offset,
            count: messages.len() as u32,
        })
    }

    async fn commit(
        &self,
        _topic: Topic,
        _partition: u32,
        _consumer: &str,
        offset: u64,
    ) -> JournalResult<()> {
        *self.committed.lock().unwrap() = Some(offset);
        self.events.lock().unwrap().push(Event::Commit(offset));
        Ok(())
    }

    async fn committed(
        &self,
        _topic: Topic,
        _partition: u32,
        _consumer: &str,
    ) -> JournalResult<Option<u64>> {
        Ok(*self.committed.lock().unwrap())
    }

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
        self.polls.fetch_add(1, Ordering::SeqCst);
        out.clear();
        if let Some(offsets) = &self.reader_offsets {
            for offset in offsets {
                out.push(PayloadRecord {
                    offset: *offset,
                    timestamp_ns: 0,
                    payload: Bytes::from_static(b"order"),
                });
            }
            Ok(out.len())
        } else {
            Ok(0)
        }
    }

    fn is_iggy(&self) -> bool {
        false
    }

    async fn ping(&self) -> bool {
        true
    }
}

/// A drained snapshot queue, so `run_writer`'s shutdown join completes.
fn snapshot_sink() -> (
    mpsc::Sender<whippet::engine_runner::SnapshotJob>,
    tokio::task::JoinHandle<()>,
) {
    let (sender, mut receiver) = mpsc::channel(2);
    let handle = tokio::spawn(async move { while receiver.recv().await.is_some() {} });
    (sender, handle)
}

fn output_batch(last_offset: u64, final_batch: bool) -> OutputBatch {
    OutputBatch {
        executions: vec![JournalMessage::new(vec![1, 2, 3], Vec::new())],
        md: Vec::new(),
        orders: Vec::new(),
        last_offset,
        snapshot: None,
        final_batch,
    }
}

fn input_batch(offset: u64) -> InputBatch {
    InputBatch::durable(vec![PayloadRecord {
        offset,
        timestamp_ns: 0,
        payload: Bytes::from_static(b"not-a-command"),
    }])
}

#[tokio::test(start_paused = true)]
async fn output_append_retries_then_commits_without_a_gap() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let appended = Arc::new(AtomicU64::new(0));
    let journal = FakeJournal::failing_executions_once(Arc::clone(&appended));
    let (snapshot_tx, snapshot_writer) = snapshot_sink();
    let (output_tx, output_rx) = mpsc::channel(SHARD_RING_CAPACITY);

    output_tx.send(output_batch(7, true)).await.unwrap();
    drop(output_tx);

    run_writer(
        Arc::clone(&ctx),
        &journal,
        0,
        "test-consumer".to_string(),
        None,
        output_rx,
        snapshot_tx,
        snapshot_writer,
        Arc::clone(&appended),
        Arc::new(AtomicU32::new(0)),
    )
    .await
    .expect("the second append attempt succeeds");

    assert!(
        ctx.shards_ok(),
        "a recovered append must not mark the shard failed"
    );
    assert_eq!(
        appended.load(Ordering::SeqCst),
        7,
        "the applied offset catches up only after the append is durable"
    );
    assert_eq!(
        journal.committed(),
        Some(7),
        "the commit catches up once the execution append succeeds"
    );
    assert_eq!(
        journal.executions(),
        vec![vec![1, 2, 3]],
        "the output log has exactly one copy of the execution"
    );
    assert_eq!(
        journal.appended_at_failure(),
        vec![0],
        "the applied offset was still at its initial value while the append was failing"
    );
    assert_eq!(
        journal.events(),
        vec![
            Event::AppendFailed(Topic::Executions),
            Event::AppendOk(Topic::Executions),
            Event::Commit(7),
        ],
        "nothing was committed before the execution append became durable"
    );
}

#[tokio::test(start_paused = true)]
async fn output_append_failure_forever_stops_without_committing() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let appended = Arc::new(AtomicU64::new(0));
    let journal = FakeJournal::failing_executions_forever(Arc::clone(&appended));
    let (snapshot_tx, snapshot_writer) = snapshot_sink();
    let (output_tx, output_rx) = mpsc::channel(SHARD_RING_CAPACITY);

    output_tx.send(output_batch(7, false)).await.unwrap();
    drop(output_tx);

    // Paused time: the 30-attempt exponential backoff is virtual, so this
    // returns as soon as the budget is exhausted instead of waiting ~50s.
    let error = run_writer(
        Arc::clone(&ctx),
        &journal,
        0,
        "test-consumer".to_string(),
        None,
        output_rx,
        snapshot_tx,
        snapshot_writer,
        Arc::clone(&appended),
        Arc::new(AtomicU32::new(0)),
    )
    .await
    .expect_err("a permanently failing append must stop the shard");

    assert!(
        error.to_string().contains("output append failed"),
        "unexpected error: {error}"
    );
    assert!(!ctx.shards_ok(), "the shard is marked failed");
    assert_eq!(
        journal.attempts(),
        APPEND_MAX_ATTEMPTS as usize,
        "the retry budget is exactly the production bound"
    );
    assert_eq!(
        journal.committed(),
        None,
        "the last applied offset is never committed"
    );
    assert_eq!(
        appended.load(Ordering::SeqCst),
        0,
        "the applied offset never advances"
    );
    assert!(
        journal
            .events()
            .iter()
            .all(|event| !matches!(event, Event::Commit(_))),
        "no commit is issued on the failure path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matcher_panic_is_joined_and_fails_the_shard() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let matcher = std::thread::Builder::new()
        .name("panicking-matcher".to_string())
        .spawn(|| panic!("matcher body exploded"))
        .expect("spawn the panicking matcher");

    let join_ctx = Arc::clone(&ctx);
    let joined = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || join_matcher(&join_ctx, 0, matcher)),
    )
    .await
    .expect("joining a panicked matcher must not hang")
    .expect("the blocking join task runs");

    let error = joined.expect_err("a panicked matcher must fail the shard");
    assert!(
        error.to_string().contains("panicked"),
        "unexpected error: {error}"
    );
    assert!(!ctx.shards_ok(), "the shard is marked failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_rings_block_producers_instead_of_growing() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let exchange = ExchangeConfig::default();
    let (input_tx, input_rx) = mpsc::channel::<InputBatch>(SHARD_RING_CAPACITY);
    let (output_tx, mut output_rx) = mpsc::channel::<OutputBatch>(SHARD_RING_CAPACITY);
    let (recycle_tx, _recycle_rx) = mpsc::channel(SHARD_RING_CAPACITY);
    let appended = Arc::new(AtomicU64::new(0));
    let engine = ShardEngine::new(0, &exchange);

    let matcher = std::thread::Builder::new()
        .name("ring-test-matcher".to_string())
        .spawn(move || {
            run_matcher(
                ctx,
                0,
                engine,
                input_rx,
                output_tx,
                recycle_tx,
                appended,
                0,
                0,
                Arc::new(AtomicU32::new(0)),
                Arc::new(AtomicU32::new(0)),
            )
        })
        .expect("spawn the matcher");

    // Two batches fill the output ring; the third parks the matcher in
    // `blocking_send`. That is the backpressure boundary.
    for offset in 0..3u64 {
        input_tx.send(input_batch(offset)).await.unwrap();
    }
    let filled = tokio::time::timeout(Duration::from_secs(2), async {
        // Wait for the matcher to have consumed the batch it is blocked on as
        // well as filled the output ring.
        while output_rx.len() < SHARD_RING_CAPACITY || input_tx.capacity() < SHARD_RING_CAPACITY {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        filled.is_ok(),
        "the matcher must fill the output ring and then stop"
    );
    assert_eq!(
        output_rx.len(),
        SHARD_RING_CAPACITY,
        "the matcher stops at the output ring instead of buffering"
    );
    // The matcher consumes the third batch before it parks in `blocking_send`,
    // so the input ring drains. Wait for that state instead of racing it.
    let drained = tokio::time::timeout(Duration::from_secs(2), async {
        while input_tx.capacity() < SHARD_RING_CAPACITY {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        drained.is_ok(),
        "the matcher must consume the batch it is blocked on"
    );

    // With the matcher parked, the input ring fills. The next reader send must
    // block, and the queue must stay bounded at its configured capacity.
    input_tx.send(input_batch(3)).await.unwrap();
    input_tx.send(input_batch(4)).await.unwrap();
    assert_eq!(input_tx.capacity(), 0, "the input ring is full");
    assert_eq!(
        input_tx.max_capacity(),
        SHARD_RING_CAPACITY,
        "memory is bounded by the ring capacity"
    );
    let blocked =
        tokio::time::timeout(Duration::from_millis(100), input_tx.send(input_batch(5))).await;
    assert!(
        blocked.is_err(),
        "the reader's send must block once the input ring is full"
    );

    // Teardown: closing the output receiver unblocks the parked matcher.
    drop(input_tx);
    output_rx.close();
    while output_rx.recv().await.is_some() {}
    matcher.join().expect("the matcher exits cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reader_gap_or_overlap_fails_the_shard_and_stops_polling() {
    // An interior gap (0, 2) and an overlap (0, 1, 1) must both be rejected:
    // the reader validates the whole batch, not just its first record.
    for offsets in [vec![0u64, 2], vec![0u64, 1, 1]] {
        let node = TestNode::new();
        let ctx = Arc::clone(&node.ctx);
        let journal = Arc::new(FakeJournal::with_reader(offsets.clone()));
        let (sender, mut receiver) = mpsc::channel(SHARD_RING_CAPACITY);
        let (_recycle_tx, recycle_rx) = mpsc::channel(SHARD_RING_CAPACITY);
        // Keep the sender alive so `shutdown.changed()` stays pending and the
        // reader actually polls.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        tokio::time::timeout(
            Duration::from_secs(2),
            read_orders(
                Arc::clone(&ctx),
                Arc::clone(&journal),
                0,
                "test-consumer".to_string(),
                0,
                sender,
                recycle_rx,
                shutdown_rx,
                Arc::new(AtomicU32::new(0)),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("the reader must stop on a non-contiguous batch {offsets:?}"));

        assert!(!ctx.shards_ok(), "gap/overlap {offsets:?} fails the shard");
        assert_eq!(
            journal.polls(),
            1,
            "a bad batch stops polling instead of retrying it"
        );
        assert!(
            receiver.try_recv().is_err(),
            "a bad batch is never forwarded to the matcher"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_order_record_releases_the_turbo_window_slot() {
    // The window slot is the crash-loss bound, so it must stay held until the
    // write-behind's orders append lands. The gated journal parks that append
    // while the test observes the slot.
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let appended = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let journal = Arc::new(FakeJournal::gated_on_orders(
        Arc::clone(&appended),
        Arc::clone(&gate),
    ));
    let (snapshot_tx, snapshot_writer) = snapshot_sink();
    let (output_tx, output_rx) = mpsc::channel(SHARD_RING_CAPACITY);

    let window = TurboWindow::new(TurboConfig::new(1, 50).unwrap());
    let admission = window.try_admit(now_ns()).expect("one window permit");
    assert_eq!(window.in_flight(), 1);

    output_tx
        .send(OutputBatch {
            executions: vec![JournalMessage::new(vec![9], Vec::new())],
            md: Vec::new(),
            orders: vec![TurboJournalRecord {
                message: JournalMessage::new(vec![7], Vec::new()),
                admission,
            }],
            last_offset: 4,
            snapshot: None,
            final_batch: true,
        })
        .await
        .unwrap();
    drop(output_tx);

    let writer = {
        let ctx = Arc::clone(&ctx);
        let journal = Arc::clone(&journal);
        tokio::spawn(async move {
            run_writer(
                ctx,
                journal.as_ref(),
                0,
                "test-consumer".to_string(),
                None,
                output_rx,
                snapshot_tx,
                snapshot_writer,
                appended,
                Arc::new(AtomicU32::new(0)),
            )
            .await
        })
    };

    tokio::time::timeout(Duration::from_secs(2), async {
        while !journal.orders_started() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the writer must attempt the orders append");
    assert_eq!(
        window.in_flight(),
        1,
        "the window slot stays held while the order record is not durable"
    );

    gate.add_permits(1);
    writer
        .await
        .expect("the writer task runs")
        .expect("the write-behind drains");

    assert_eq!(
        window.in_flight(),
        0,
        "a durable order record releases the window slot"
    );
    assert_eq!(journal.orders(), vec![vec![7]], "the command is journaled");
    assert_eq!(journal.committed(), Some(4));
    let events = journal.events();
    let orders_at = events
        .iter()
        .position(|event| *event == Event::AppendOk(Topic::Orders))
        .expect("the orders append is recorded");
    let commit_at = events
        .iter()
        .position(|event| matches!(event, Event::Commit(4)))
        .expect("the offset is committed");
    assert!(
        orders_at < commit_at,
        "the commit must follow the orders append: {events:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_failed_order_append_stops_the_shard_without_committing() {
    let node = TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    let appended = Arc::new(AtomicU64::new(0));
    let journal = FakeJournal::failing_orders_forever(Arc::clone(&appended));
    let (snapshot_tx, snapshot_writer) = snapshot_sink();
    let (output_tx, output_rx) = mpsc::channel(SHARD_RING_CAPACITY);

    let window = TurboWindow::new(TurboConfig::new(1, 50).unwrap());
    let admission = window.try_admit(now_ns()).expect("one window permit");
    output_tx
        .send(OutputBatch {
            executions: vec![JournalMessage::new(vec![9], Vec::new())],
            md: Vec::new(),
            orders: vec![TurboJournalRecord {
                message: JournalMessage::new(vec![7], Vec::new()),
                admission,
            }],
            last_offset: 4,
            snapshot: None,
            final_batch: true,
        })
        .await
        .unwrap();
    drop(output_tx);

    let error = run_writer(
        Arc::clone(&ctx),
        &journal,
        0,
        "test-consumer".to_string(),
        None,
        output_rx,
        snapshot_tx,
        snapshot_writer,
        Arc::clone(&appended),
        Arc::new(AtomicU32::new(0)),
    )
    .await
    .expect_err("a permanently failing write-behind must stop the shard");

    assert!(
        error.to_string().contains("output append failed"),
        "unexpected error: {error}"
    );
    assert!(!ctx.shards_ok(), "the shard is marked failed");
    assert_eq!(
        appended.load(Ordering::SeqCst),
        0,
        "the applied offset never advances past a lost order record"
    );
    assert_eq!(journal.committed(), None, "no offset is committed");
    assert!(
        journal
            .events()
            .iter()
            .all(|event| !matches!(event, Event::Commit(_))),
        "no commit is issued on the failure path"
    );
}
