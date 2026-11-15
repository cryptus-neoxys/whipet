// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Per-partition group commit.
//!
//! One writer task per `(topic, partition)` coalesces concurrent submissions
//! into a single journal append. A single-order client therefore does not pay
//! one Iggy append round trip per order when other orders are arriving at the
//! same time: their submissions share one `send_messages` call, and each
//! caller still gets back the offset of its own messages.
//!
//! The writer is intentionally boring: it awaits the first job, drains whatever
//! is already queued, optionally waits a short linger window for one more job,
//! then performs exactly one `append_owned`. Ordering is FIFO per partition
//! because submissions reach the writer over one channel.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{AppendResult, Journal, JournalError, JournalMessage, Result, Topic};

/// Largest number of submissions coalesced into one journal append.
///
/// Iggy's `message_bus.max_batch` is 256 (documented as such in Iggy 0.10 and
/// unchanged in the pinned 0.12), and [`Journal::append_owned`] chunks anything
/// larger into 256-message `send_messages` calls. Offsets are contiguous only
/// within one chunk, so `base_offset + i` is a per-message guarantee only while
/// a batch stays inside one chunk. Clamping the coalesced job count to 256 keeps
/// a batch on one chunk as long as each individual submission is at most 256
/// messages; a larger single submission is still appended, but its callers must
/// not assume contiguous offsets past the first chunk.
const MAX_GROUP_BATCH: usize = 256;

/// Depth of a partition's submission queue.
///
/// A full queue is backpressure: `append` awaits a slot instead of buffering
/// without bound. The writer keeps the queue shallow by draining it into one
/// append, so this only matters when the journal is slower than the producers.
const CHANNEL_CAPACITY: usize = 1024;

/// Raw values for [`Inner::state`]; see [`GroupCommitState`].
const STATE_OPEN: u8 = 0;
const STATE_SHUTTING_DOWN: u8 = 1;
const STATE_CLOSED: u8 = 2;

/// Lifecycle of one [`GroupCommit`].
///
/// A committer is built `Open`, and `Open -> ShuttingDown -> Closed` is the
/// only forward path. The transition table is the contract [`GroupCommit::append`]
/// and [`GroupCommit::shutdown`] implement:
///
/// | state | `append` | `shutdown` |
/// |---|---|---|
/// | `Open` | accepted; the partition's writer is spawned on first use | drains every writer and awaits it, then moves to `Closed` |
/// | `ShuttingDown` | refused with [`JournalError::WriterStopped`]; **no** writer is spawned | returns immediately: the caller that moved the state owns the drain |
/// | `Closed` | refused with [`JournalError::InvalidConfig`]; no writer exists | returns immediately |
///
/// `ShuttingDown` and `Closed` are terminal for submissions: a job is either
/// accepted while the committer is `Open` (and is then flushed, because the
/// drain awaits the writer that owns it) or it is refused before any bytes are
/// appended. The two errors differ on purpose: a submission that raced a
/// shutdown was stopped, while a submission to a closed committer is a
/// lifetime bug in the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupCommitState {
    Open,
    ShuttingDown,
    Closed,
}

impl GroupCommitState {
    fn from_raw(raw: u8) -> Self {
        match raw {
            STATE_OPEN => Self::Open,
            STATE_SHUTTING_DOWN => Self::ShuttingDown,
            _ => Self::Closed,
        }
    }
}

/// Group-commit tuning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitConfig {
    /// Maximum submissions coalesced into one append.
    ///
    /// Clamped to `1..=256` by [`GroupCommit::new`]. `1` is latency mode: every
    /// submission is flushed immediately and the writer never lingers.
    pub max_batch: usize,
    /// How long a writer waits for one more submission after it has a job.
    ///
    /// `Duration::ZERO` disables the wait. Only batches that are still below
    /// `max_batch` linger, so a saturated writer flushes without waiting.
    pub linger: Duration,
}

impl Default for GroupCommitConfig {
    fn default() -> Self {
        Self {
            max_batch: MAX_GROUP_BATCH,
            linger: Duration::from_micros(200),
        }
    }
}

/// A snapshot of what the writers have done.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    /// Journal appends that completed successfully; one per batch.
    pub batches: u64,
    /// Messages handed to the journal across those appends.
    pub messages: u64,
    /// Most messages seen in a single append.
    pub max_batch_seen: u64,
    /// Batches whose writer waited on the linger timer before appending.
    ///
    /// Counted with [`GroupCommitStats::batches`], so a failed append adds to
    /// neither: `linger_waits <= batches` always holds.
    pub linger_waits: u64,
}

/// Per-partition group commit over a [`Journal`].
///
/// Cheap to clone: every clone shares the same writer tasks and stats.
#[derive(Clone)]
pub struct GroupCommit {
    inner: Arc<Inner>,
}

struct Inner {
    journal: Arc<Journal>,
    config: GroupCommitConfig,
    writers: Mutex<HashMap<(Topic, u32), Writer>>,
    /// Lifecycle state, one of the `STATE_*` values; see [`GroupCommitState`].
    ///
    /// Moved to `ShuttingDown` by a single compare-and-swap before the writer
    /// map is drained, so a concurrent [`GroupCommit::append`] is refused
    /// instead of spawning a fresh writer behind the drain and leaking a task
    /// that shutdown already waited on.
    state: AtomicU8,
    /// Monotonic id handed to each spawned writer.
    ///
    /// A failed send names the writer it addressed by generation, so
    /// [`GroupCommit::remove_writer`] can remove that entry only: a concurrent
    /// `append` may already have replaced it with a live writer.
    next_generation: AtomicU64,
    stats: Arc<Stats>,
}

/// A live writer task and the sender that feeds it.
struct Writer {
    sender: mpsc::Sender<Job>,
    handle: JoinHandle<()>,
    /// Matches the [`Inner::next_generation`] value taken when the writer was
    /// inserted; used to remove exactly the writer a send failed on.
    generation: u64,
}

/// One caller's submission.
struct Job {
    messages: Vec<JournalMessage>,
    reply: oneshot::Sender<Result<AppendResult>>,
}

#[derive(Default)]
struct Stats {
    batches: AtomicU64,
    messages: AtomicU64,
    max_batch_seen: AtomicU64,
    linger_waits: AtomicU64,
}

impl GroupCommit {
    /// Build a group committer over `journal`.
    ///
    /// `config.max_batch` is clamped to `1..=256`; see [`MAX_GROUP_BATCH`] for
    /// why. No writer tasks are spawned here: each partition gets one on its
    /// first submission.
    pub fn new(journal: Arc<Journal>, config: GroupCommitConfig) -> Self {
        let config = GroupCommitConfig {
            max_batch: config.max_batch.clamp(1, MAX_GROUP_BATCH),
            linger: config.linger,
        };
        Self {
            inner: Arc::new(Inner {
                journal,
                config,
                writers: Mutex::new(HashMap::new()),
                state: AtomicU8::new(STATE_OPEN),
                next_generation: AtomicU64::new(0),
                stats: Arc::new(Stats::default()),
            }),
        }
    }

    /// Submit one or more messages for one partition.
    ///
    /// Resolves with the append result for this submission: message `i` of the
    /// submission is at `base_offset + i`, and `count` is the submission's
    /// length. That offset guarantee holds within one Iggy chunk (256
    /// messages); submissions larger than that should derive per-message
    /// offsets themselves.
    ///
    /// An empty submission is rejected with `InvalidConfig`: the backends
    /// disagree on what one means (Iggy reports base offset 0, the memory
    /// journal reports the partition head), and gateway and TCP ingress
    /// already reject empty batches before reaching group commit.
    ///
    /// Submissions on one partition are ordered by arrival at the writer, and
    /// a submission's messages are never interleaved with another submission's.
    pub async fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: Vec<JournalMessage>,
    ) -> Result<AppendResult> {
        // Refuse before doing any work. Once shutdown has started there is no
        // writer that may take the job, and no writer may be spawned behind the
        // drain: see [`GroupCommitState`] for the transition table.
        self.ensure_open()?;
        // One flush is one `send_messages`, so a submission larger than the
        // Iggy batch cap could span chunks and break the per-caller
        // `base_offset + i` guarantee. Reject it instead of mis-acking.
        if messages.len() > MAX_GROUP_BATCH {
            return Err(JournalError::InvalidConfig(format!(
                "a single group-commit submission is {} messages; the cap is {MAX_GROUP_BATCH}",
                messages.len()
            )));
        }
        // Same for the empty case, where the two backends would answer
        // differently: see the method docs.
        if messages.is_empty() {
            return Err(JournalError::InvalidConfig(
                "a group-commit submission must contain at least one message".to_string(),
            ));
        }
        let (reply, response) = oneshot::channel();
        let mut pending = Some((messages, reply));

        // Two attempts: a writer that died (a panic in the journal, or a
        // shutting-down runtime) leaves a stale sender in the map. Dropping the
        // stale entry and retrying keeps one dead task from bricking a
        // partition forever.
        for attempt in 0..2 {
            let (sender, generation) = match self.sender(topic, partition) {
                Ok(sender) => sender,
                // Shutdown started, so no fresh writer may be spawned and the
                // job was never appended.
                Err(error) => return Err(error),
            };
            let (messages, reply) = pending.take().expect("job is restored on a failed send");
            match sender.send(Job { messages, reply }).await {
                Ok(()) => {
                    return response.await.map_err(|_| JournalError::WriterStopped)?;
                }
                Err(error) => {
                    let job = error.0;
                    pending = Some((job.messages, job.reply));
                    self.remove_writer(topic, partition, generation);
                    if attempt == 1 {
                        break;
                    }
                }
            }
        }
        Err(JournalError::WriterStopped)
    }

    /// The committer's lifecycle state.
    ///
    /// Reads are `SeqCst`, matching the transitions, so a caller that sees
    /// [`GroupCommitState::Open`] either also sees every writer that shutdown
    /// drained before it moved the state, or loses the race to `append`.
    pub fn state(&self) -> GroupCommitState {
        GroupCommitState::from_raw(self.inner.state.load(Ordering::SeqCst))
    }

    /// Why a submission cannot be accepted now, if it cannot.
    ///
    /// `ShuttingDown` is a race with shutdown and reports
    /// [`JournalError::WriterStopped`] (the job was never appended).
    /// `Closed` is permanent and reports [`JournalError::InvalidConfig`]: the
    /// committer will never accept another submission.
    fn ensure_open(&self) -> Result<()> {
        match self.state() {
            GroupCommitState::Open => Ok(()),
            GroupCommitState::ShuttingDown => Err(JournalError::WriterStopped),
            GroupCommitState::Closed => Err(JournalError::InvalidConfig(
                "the group committer is closed and accepts no submissions".to_string(),
            )),
        }
    }

    /// Current counters. Reads are relaxed; this is telemetry, not a barrier.
    pub fn stats(&self) -> GroupCommitStats {
        let stats = &self.inner.stats;
        GroupCommitStats {
            batches: stats.batches.load(Ordering::Relaxed),
            messages: stats.messages.load(Ordering::Relaxed),
            max_batch_seen: stats.max_batch_seen.load(Ordering::Relaxed),
            linger_waits: stats.linger_waits.load(Ordering::Relaxed),
        }
    }

    /// Number of live writer tasks.
    pub fn writer_count(&self) -> usize {
        self.lock_writers().len()
    }

    /// Verify the group-commit contract the writers maintain.
    ///
    /// Diagnostic and test-only: `O(writers + stats)`, no I/O, and it is a
    /// point-in-time snapshot, not a barrier. Call it while quiescent (after a
    /// phase, not while racing appends) if the writer-map check must be exact;
    /// the stat counters are ordered so that every stat invariant below holds
    /// under any interleaving of the writers (see [`flush`]).
    ///
    /// - config: `1 <= max_batch <= 256`. The linger window has no bound: it is
    ///   whatever the caller configured.
    /// - stats: `messages >= batches` (every batch carries at least one
    ///   message), `max_batch_seen <= 256`, `max_batch_seen >= 1` once any
    ///   message was appended, `linger_waits <= batches`.
    /// - writers: at most one entry per `(topic, partition)`, no entry once a
    ///   drained shutdown has closed the committer, and the map's size agrees
    ///   with [`GroupCommit::writer_count`].
    pub fn check_invariants(&self) -> std::result::Result<(), String> {
        let config = self.inner.config;
        if config.max_batch == 0 || config.max_batch > MAX_GROUP_BATCH {
            return Err(format!(
                "config.max_batch is {}; it must be in 1..={MAX_GROUP_BATCH}",
                config.max_batch
            ));
        }

        let stats = self.stats();
        if stats.messages < stats.batches {
            return Err(format!(
                "stats report {} messages across {} batches; every batch carries at least one \
                 message",
                stats.messages, stats.batches
            ));
        }
        if stats.max_batch_seen > MAX_GROUP_BATCH as u64 {
            return Err(format!(
                "stats report a {}-message batch; the cap is {MAX_GROUP_BATCH}",
                stats.max_batch_seen
            ));
        }
        if stats.messages > 0 && stats.max_batch_seen == 0 {
            return Err(format!(
                "stats report {} appended messages but no batch size",
                stats.messages
            ));
        }
        if stats.linger_waits > stats.batches {
            return Err(format!(
                "stats report {} linger waits across {} batches; a wait belongs to one batch",
                stats.linger_waits, stats.batches
            ));
        }

        let state = self.state();
        // Scoped so the writers lock is released before `writer_count` takes
        // it again: `Mutex` is not reentrant, and holding the guard across the
        // second lock would deadlock the caller.
        let keys: Vec<(Topic, u32)> = {
            let writers = self.lock_writers();
            writers.keys().copied().collect()
        };
        let mut unique = HashSet::with_capacity(keys.len());
        for key in &keys {
            // A HashMap already deduplicates keys; the check makes the
            // one-writer-per-partition rule explicit for a future change that
            // stores writers some other way.
            if !unique.insert(*key) {
                return Err(format!(
                    "{} partition {} has more than one writer",
                    key.0, key.1
                ));
            }
        }
        if keys.len() != self.writer_count() {
            return Err(format!(
                "writer_count() is {} but the writer map holds {} entries",
                self.writer_count(),
                keys.len()
            ));
        }
        if state == GroupCommitState::Closed && !keys.is_empty() {
            return Err(format!(
                "the committer is closed but {} writer entries survived: {keys:?}",
                keys.len()
            ));
        }
        Ok(())
    }

    /// Close every partition queue and wait for the writers to finish.
    ///
    /// Idempotent. The first caller moves the state to
    /// [`GroupCommitState::ShuttingDown`], drains the writer map, drops each
    /// sender (so each writer flushes the jobs it already accepted and then
    /// exits), awaits every task, and finally moves the state to
    /// [`GroupCommitState::Closed`]. A later `shutdown` returns immediately:
    /// `ShuttingDown` means the caller that set it owns the drain, and `Closed`
    /// has nothing left to drain.
    ///
    /// Dropping the last [`GroupCommit`] does the same thing without waiting:
    /// the sender map drops, each writer's channel closes, and each task exits
    /// after flushing the jobs it already accepted. This is the awaitable form,
    /// for tests and orderly shutdown.
    pub async fn shutdown(&self) {
        // One compare-and-swap owns the drain. Doing this before the map lock
        // refuses fresh writers first: draining alone leaves a window where a
        // concurrent `append` re-inserts a writer that shutdown has already
        // waited on, leaking the task.
        if self
            .inner
            .state
            .compare_exchange(
                STATE_OPEN,
                STATE_SHUTTING_DOWN,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return;
        }
        let writers: Vec<Writer> = {
            let mut map = self.lock_writers();
            map.drain().map(|(_, writer)| writer).collect()
        };
        let mut handles = Vec::with_capacity(writers.len());
        for writer in writers {
            drop(writer.sender);
            handles.push(writer.handle);
        }
        for handle in handles {
            let _ = handle.await;
        }
        self.inner.state.store(STATE_CLOSED, Ordering::SeqCst);
    }

    /// Sender for `(topic, partition)` and the writer's generation, spawning
    /// the writer on first use.
    ///
    /// The map lock is held only for the lookup/insert and never across an
    /// await. Writer tasks never touch the map, so they cannot deadlock here.
    ///
    /// The state is re-checked under the map lock because it can flip between
    /// [`GroupCommit::append`]'s cheap check and this spawn: spawning after the
    /// drain would re-insert a task behind it and leak it.
    fn sender(&self, topic: Topic, partition: u32) -> Result<(mpsc::Sender<Job>, u64)> {
        let mut writers = self.lock_writers();
        self.ensure_open()?;
        if let Some(writer) = writers.get(&(topic, partition)) {
            return Ok((writer.sender.clone(), writer.generation));
        }
        let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let handle = tokio::spawn(run_writer(
            Arc::clone(&self.inner.journal),
            topic,
            partition,
            self.inner.config,
            Arc::clone(&self.inner.stats),
            receiver,
        ));
        writers.insert(
            (topic, partition),
            Writer {
                sender: sender.clone(),
                handle,
                generation,
            },
        );
        Ok((sender, generation))
    }

    /// Drop the writer entry a failed send addressed, if it is still the same
    /// writer.
    ///
    /// Two appends can lose a send to the same dead writer. The first removes
    /// the entry and spawns a fresh writer; removing by key alone would then
    /// discard that live writer, leaving the partition with two writers (one
    /// unmapped, so shutdown never awaits it) and breaking per-partition FIFO.
    /// The generation pins the entry the failed send was made on.
    fn remove_writer(&self, topic: Topic, partition: u32, generation: u64) {
        let mut writers = self.lock_writers();
        let stale = writers
            .get(&(topic, partition))
            .is_some_and(|writer| writer.generation == generation);
        if stale {
            writers.remove(&(topic, partition));
        }
    }

    fn lock_writers(&self) -> std::sync::MutexGuard<'_, HashMap<(Topic, u32), Writer>> {
        self.inner
            .writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The per-partition writer loop.
///
/// One flush is one `send_messages` call, so the drain is bounded by **message
/// count**, not job count: Iggy 0.10's `message_bus.max_batch` is 256, and a
/// second `send_messages` could interleave with another writer and break the
/// `base_offset + i` reply arithmetic. A job that would overflow the current
/// batch is held for the next flush.
async fn run_writer(
    journal: Arc<Journal>,
    topic: Topic,
    partition: u32,
    config: GroupCommitConfig,
    stats: Arc<Stats>,
    mut receiver: mpsc::Receiver<Job>,
) {
    let mut pending: Option<Job> = None;
    loop {
        let first = match pending.take() {
            Some(job) => job,
            None => match receiver.recv().await {
                Some(job) => job,
                None => break,
            },
        };
        let mut jobs = Vec::with_capacity(config.max_batch);
        let mut budget = config.max_batch.saturating_sub(first.messages.len());
        jobs.push(first);

        // Take every job that is already queued and fits the message budget.
        while budget > 0 {
            match receiver.try_recv() {
                Ok(job) => {
                    if job.messages.len() > budget {
                        pending = Some(job);
                        break;
                    }
                    budget -= job.messages.len();
                    jobs.push(job);
                }
                Err(_) => break,
            }
        }

        // Linger only while there is room and only when asked. `max_batch == 1`
        // therefore never waits: the batch is already full. If a job was held
        // because it did not fit, do not linger: accepting a later job now
        // would append it before the held one, and a second oversized job
        // would overwrite the held one and drop its waiter.
        let held = pending.is_some();
        let mut closed = false;
        let mut lingered = false;
        if budget > 0 && !held && !config.linger.is_zero() {
            lingered = true;
            let deadline = tokio::time::Instant::now() + config.linger;
            while let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now())
            {
                if remaining.is_zero() || budget == 0 {
                    break;
                }
                match tokio::time::timeout(remaining, receiver.recv()).await {
                    Ok(Some(job)) => {
                        if job.messages.len() > budget {
                            pending = Some(job);
                            break;
                        }
                        budget -= job.messages.len();
                        jobs.push(job);
                    }
                    // The senders are gone, but the jobs already accepted still
                    // have to be flushed and answered before the task exits.
                    Ok(None) => {
                        closed = true;
                        break;
                    }
                    Err(_elapsed) => break,
                }
            }
        }

        flush(&journal, topic, partition, jobs, &stats, lingered).await;
        if closed {
            // A held job was already received, so it must still be answered.
            if let Some(job) = pending.take() {
                flush(&journal, topic, partition, vec![job], &stats, false).await;
            }
            break;
        }
    }
}

/// Append one batch and answer every waiter.
///
/// `lingered` records whether the writer waited on the linger timer for this
/// batch. It is counted only when the batch is appended, so the
/// `linger_waits <= batches` invariant holds even when an append fails.
async fn flush(
    journal: &Journal,
    topic: Topic,
    partition: u32,
    jobs: Vec<Job>,
    stats: &Stats,
    lingered: bool,
) {
    let total: usize = jobs.iter().map(|job| job.messages.len()).sum();
    let mut replies = Vec::with_capacity(jobs.len());
    let mut messages = Vec::with_capacity(total);
    for job in jobs {
        replies.push((job.messages.len() as u32, job.reply));
        messages.extend(job.messages);
    }

    match journal.append_owned(topic, partition, messages).await {
        Ok(appended) => {
            // Increment in an order that keeps every stats invariant true for a
            // concurrent `check_invariants`: a visible batch implies its
            // messages and its batch size are visible, and a visible linger
            // wait implies the batch it belongs to is visible.
            stats
                .max_batch_seen
                .fetch_max(total as u64, Ordering::Relaxed);
            stats.messages.fetch_add(total as u64, Ordering::Relaxed);
            stats.batches.fetch_add(1, Ordering::Relaxed);
            if lingered {
                stats.linger_waits.fetch_add(1, Ordering::Relaxed);
            }

            let mut base_offset = appended.base_offset;
            for (count, reply) in replies {
                let _ = reply.send(Ok(AppendResult {
                    partition: appended.partition,
                    base_offset,
                    count,
                }));
                base_offset += u64::from(count);
            }
        }
        Err(error) => {
            // One failed append is one failure for every waiter it covered.
            // No waiter is dropped, and every waiter sees the same error.
            for (_, reply) in replies {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalRecord, MemoryJournal};

    fn message(value: u64) -> JournalMessage {
        JournalMessage::new(
            value.to_le_bytes().to_vec(),
            vec![("n".to_string(), value.to_string())],
        )
    }

    async fn records(
        journal: &Journal,
        topic: Topic,
        partition: u32,
        count: u32,
    ) -> Vec<JournalRecord> {
        journal
            .read(topic, partition, "group-commit-unit-test", 0, count, 0)
            .await
            .expect("read back appended records")
    }

    #[test]
    fn default_config_is_256_and_200_microseconds() {
        let config = GroupCommitConfig::default();
        assert_eq!(config.max_batch, 256);
        assert_eq!(config.linger, Duration::from_micros(200));
    }

    #[test]
    fn max_batch_is_clamped_to_the_iggy_chunk() {
        let journal = Arc::new(Journal::memory());
        let too_small = GroupCommit::new(
            Arc::clone(&journal),
            GroupCommitConfig {
                max_batch: 0,
                linger: Duration::ZERO,
            },
        );
        assert_eq!(too_small.inner.config.max_batch, 1);
        let too_large = GroupCommit::new(
            journal,
            GroupCommitConfig {
                max_batch: 10_000,
                linger: Duration::ZERO,
            },
        );
        assert_eq!(too_large.inner.config.max_batch, MAX_GROUP_BATCH);
    }

    #[tokio::test]
    async fn an_empty_submission_is_rejected() {
        let journal = Arc::new(Journal::memory());
        let group = GroupCommit::new(
            Arc::clone(&journal),
            GroupCommitConfig {
                max_batch: 4,
                linger: Duration::ZERO,
            },
        );
        journal
            .append_owned(Topic::Orders, 0, vec![JournalMessage::new(vec![1], vec![])])
            .await
            .expect("seed append");
        let error = group
            .append(Topic::Orders, 0, Vec::new())
            .await
            .expect_err("an empty submission must be rejected");
        assert!(
            matches!(error, JournalError::InvalidConfig(_)),
            "expected InvalidConfig, got {error:?}"
        );
        // The rejection happens before any writer is spawned, so the rejected
        // submission leaves nothing behind.
        assert_eq!(group.writer_count(), 0);
        group.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_racing_with_appends_leaves_no_writer_behind() {
        let journal = Arc::new(Journal::memory());
        let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());

        let mut handles = Vec::new();
        for value in 0..64u64 {
            let group = group.clone();
            handles.push(tokio::spawn(async move {
                (
                    value,
                    group.append(Topic::Orders, 0, vec![message(value)]).await,
                )
            }));
        }
        let shutdown = {
            let group = group.clone();
            tokio::spawn(async move { group.shutdown().await })
        };

        let mut successes = 0usize;
        for handle in handles {
            let (value, result) = handle.await.expect("submission task");
            match result {
                Ok(_) => successes += 1,
                // Refused after shutdown started: the submission was never
                // appended, which is the documented `WriterStopped` contract.
                Err(JournalError::WriterStopped) => {}
                Err(error) => panic!("submission {value} failed unexpectedly: {error}"),
            }
        }
        shutdown.await.expect("shutdown task");
        assert_eq!(
            group.writer_count(),
            0,
            "no writer may survive or be re-inserted after shutdown"
        );

        // No job was silently dropped: exactly the successful submissions were
        // appended, nothing more, and their acks cover distinct offsets.
        assert_eq!(group.stats().messages, successes as u64);
        let stored = records(&journal, Topic::Orders, 0, 64).await;
        assert_eq!(stored.len(), successes);
    }

    #[test]
    fn check_invariants_hold_for_a_fresh_committer() {
        let group = GroupCommit::new(Arc::new(Journal::memory()), GroupCommitConfig::default());
        group
            .check_invariants()
            .expect("a fresh committer has no writers and default stats");
        assert_eq!(group.state(), GroupCommitState::Open);
        assert_eq!(group.writer_count(), 0);
    }

    #[test]
    fn check_invariants_reject_an_impossible_stats_snapshot() {
        let group = GroupCommit::new(Arc::new(Journal::memory()), GroupCommitConfig::default());
        let stats = &group.inner.stats;

        stats.batches.store(3, Ordering::Relaxed);
        stats.messages.store(1, Ordering::Relaxed);
        let error = group.check_invariants().expect_err("messages < batches");
        assert!(error.contains("messages"), "{error}");

        stats.messages.store(300, Ordering::Relaxed);
        stats.max_batch_seen.store(300, Ordering::Relaxed);
        let error = group
            .check_invariants()
            .expect_err("a 300-message batch exceeds the cap");
        assert!(error.contains("256"), "{error}");

        stats.max_batch_seen.store(0, Ordering::Relaxed);
        let error = group
            .check_invariants()
            .expect_err("messages without a recorded batch");
        assert!(error.contains("no batch"), "{error}");

        stats.max_batch_seen.store(1, Ordering::Relaxed);
        stats.linger_waits.store(4, Ordering::Relaxed);
        let error = group
            .check_invariants()
            .expect_err("more linger waits than batches");
        assert!(error.contains("linger"), "{error}");

        stats.linger_waits.store(0, Ordering::Relaxed);
        stats.messages.store(0, Ordering::Relaxed);
        stats.batches.store(0, Ordering::Relaxed);
        group
            .check_invariants()
            .expect("the restored snapshot is consistent");
    }

    #[test]
    fn check_invariants_reject_an_out_of_range_max_batch() {
        // `new` clamps, so this snapshot can only come from a future bug in
        // construction; the diagnostic still has to catch it.
        let group = GroupCommit {
            inner: Arc::new(Inner {
                journal: Arc::new(Journal::memory()),
                config: GroupCommitConfig {
                    max_batch: 0,
                    linger: Duration::ZERO,
                },
                writers: Mutex::new(HashMap::new()),
                state: AtomicU8::new(STATE_OPEN),
                next_generation: AtomicU64::new(0),
                stats: Arc::new(Stats::default()),
            }),
        };
        let error = group
            .check_invariants()
            .expect_err("max_batch 0 is outside 1..=256");
        assert!(error.contains("max_batch"), "{error}");
    }

    #[tokio::test]
    async fn shutdown_is_idempotent_and_closed_refuses_submissions() {
        let journal = Arc::new(Journal::memory());
        let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());
        let accepted = group
            .append(Topic::Orders, 0, vec![message(1)])
            .await
            .expect("append while open");
        assert_eq!(accepted.base_offset, 0);
        assert_eq!(group.state(), GroupCommitState::Open);
        assert_eq!(group.writer_count(), 1);

        group.shutdown().await;
        assert_eq!(group.state(), GroupCommitState::Closed);
        assert_eq!(group.writer_count(), 0, "no writer survives Closed");
        group.check_invariants().expect("closed and drained");

        // A second shutdown has nothing to drain and must not hang.
        tokio::time::timeout(Duration::from_millis(250), group.shutdown())
            .await
            .expect("shutdown is idempotent");
        assert_eq!(group.state(), GroupCommitState::Closed);

        let error = group
            .append(Topic::Orders, 0, vec![message(2)])
            .await
            .expect_err("a closed committer accepts nothing");
        assert!(
            matches!(error, JournalError::InvalidConfig(_)),
            "expected InvalidConfig, got {error:?}"
        );
        assert_eq!(group.writer_count(), 0, "a refused append spawns nothing");
        assert_eq!(
            group.stats().messages,
            1,
            "a refused append appends nothing"
        );
        assert_eq!(
            records(&journal, Topic::Orders, 0, 10).await.len(),
            1,
            "the post-shutdown append left no bytes"
        );
        group
            .check_invariants()
            .expect("a refused append changes nothing");
    }

    #[tokio::test]
    async fn shutting_down_refuses_appends_without_spawning() {
        let journal = Arc::new(Journal::memory());
        let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());
        group
            .append(Topic::Orders, 0, vec![message(1)])
            .await
            .expect("append while open");

        // Pin the middle state without racing a real drain: this is the state
        // `shutdown` installs before it drains, and an append must see it.
        group
            .inner
            .state
            .store(STATE_SHUTTING_DOWN, Ordering::SeqCst);
        assert_eq!(group.state(), GroupCommitState::ShuttingDown);

        let error = group
            .append(Topic::Orders, 1, vec![message(2)])
            .await
            .expect_err("shutting down refuses submissions");
        assert!(
            matches!(error, JournalError::WriterStopped),
            "expected WriterStopped, got {error:?}"
        );
        assert_eq!(
            group.writer_count(),
            1,
            "the refused partition must not spawn a writer"
        );
        assert_eq!(group.stats().messages, 1);

        // The caller that installed `ShuttingDown` owns the drain; a second
        // `shutdown` returns immediately instead of draining again.
        tokio::time::timeout(Duration::from_millis(250), group.shutdown())
            .await
            .expect("shutdown from ShuttingDown returns immediately");
        assert_eq!(group.state(), GroupCommitState::ShuttingDown);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stale_writer_entry_is_replaced_by_a_fresh_writer() {
        let journal = Arc::new(Journal::memory());
        let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());

        // A writer entry whose task is gone: the receiver is dropped, so the
        // first send fails exactly like a writer that died under the job.
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        drop(receiver);
        let stale_handle = tokio::spawn(async {});
        group.lock_writers().insert(
            (Topic::Orders, 0),
            Writer {
                sender,
                handle: stale_handle,
                generation: 0,
            },
        );
        assert_eq!(group.writer_count(), 1);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            group.append(Topic::Orders, 0, vec![message(7)]),
        )
        .await
        .expect("the stale-writer retry path must not hang")
        .expect("the retry spawns a fresh writer");
        assert_eq!(result.base_offset, 0);
        assert_eq!(result.count, 1);
        assert_eq!(group.stats().messages, 1);
        assert_eq!(
            group.writer_count(),
            1,
            "the stale entry is replaced, never duplicated"
        );
        assert_eq!(
            records(&journal, Topic::Orders, 0, 1).await[0].payload,
            7u64.to_le_bytes().to_vec()
        );
        group
            .check_invariants()
            .expect("one live writer, one batch, one message");

        group.shutdown().await;
        assert_eq!(group.writer_count(), 0);
        group.check_invariants().expect("drained and closed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_writer_that_panics_returns_an_error_instead_of_hanging() {
        let journal = Arc::new(Journal::memory());
        let group = GroupCommit::new(Arc::clone(&journal), GroupCommitConfig::default());

        // Poison the memory journal so the writer panics inside its append.
        // The task unwinds, the reply sender drops with it, and the caller sees
        // `WriterStopped`: the exact "writer died" case the retry path exists
        // for. A test-only injection, since the journal is a closed enum.
        match journal.as_ref() {
            Journal::Memory(memory) => {
                let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = memory.state.lock().expect("fresh mutex");
                    panic!("poison the memory journal on purpose");
                }));
                assert!(poisoned.is_err(), "the mutex must be poisoned");
            }
            Journal::Iggy(_) => unreachable!("the test journal is the memory backend"),
        }

        for attempt in 0..2u64 {
            let result = tokio::time::timeout(
                Duration::from_secs(1),
                group.append(Topic::Orders, 0, vec![message(attempt)]),
            )
            .await
            .unwrap_or_else(|_| panic!("attempt {attempt} hung on a dead writer"));
            match result {
                Err(JournalError::WriterStopped) => {}
                other => panic!("attempt {attempt} must fail fast with WriterStopped: {other:?}"),
            }
        }
        assert_eq!(
            group.stats().messages,
            0,
            "an append that killed the writer records nothing"
        );
        assert!(group.state() == GroupCommitState::Open);

        // The committer is still usable: shutdown drains the dead entry and
        // leaves nothing behind.
        group.shutdown().await;
        assert_eq!(group.writer_count(), 0);
        group
            .check_invariants()
            .expect("a drained committer is consistent even after a writer panic");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_full_queue_applies_backpressure_and_still_makes_progress() {
        // Stall the first append with an awaitable gate. Blocking the writer
        // on a `std::sync::Mutex` would park a runtime worker (and with it the
        // timer driver), deadlocking the test instead of exercising the queue.
        let (memory, release) = MemoryJournal::with_append_gate();
        let journal = Arc::new(Journal::Memory(memory));
        let group = GroupCommit::new(
            Arc::clone(&journal),
            GroupCommitConfig {
                max_batch: 1,
                linger: Duration::ZERO,
            },
        );

        let total = CHANNEL_CAPACITY + 32;
        let mut handles = Vec::with_capacity(total);
        for value in 0..total {
            let group = group.clone();
            handles.push(tokio::spawn(async move {
                group
                    .append(Topic::Orders, 0, vec![message(value as u64)])
                    .await
            }));
        }

        let sender = {
            let mut waited = 0u32;
            loop {
                if let Some(writer) = group.lock_writers().get(&(Topic::Orders, 0)) {
                    break writer.sender.clone();
                }
                assert!(
                    waited < 5_000,
                    "the first submission never spawned a writer"
                );
                waited += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        assert_eq!(
            sender.max_capacity(),
            CHANNEL_CAPACITY,
            "the queue is bounded, so buffering cannot grow without limit"
        );
        let mut waited = 0u32;
        while sender.capacity() > 0 {
            assert!(
                waited < 5_000,
                "the writer never filled the bounded queue behind the stalled journal"
            );
            waited += 1;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // The queue is full and the writer is still stalled: one more
        // submission cannot be buffered, so it stays pending.
        let overflow = tokio::time::timeout(
            Duration::from_millis(100),
            group.append(Topic::Orders, 0, vec![message(u64::MAX)]),
        )
        .await;
        assert!(
            overflow.is_err(),
            "a full queue must block the sender instead of buffering without bound"
        );

        // The cloned sender is only for reading `capacity`; it must not keep
        // the writer's channel open past this point or shutdown would await a
        // writer that is still parked in `recv`.
        drop(sender);

        // Unstall the journal: a slow journal with many producers must still
        // make progress and lose nothing.
        release.send(()).expect("the stalled append still waits");
        for handle in handles {
            handle
                .await
                .expect("submission task")
                .expect("every accepted submission is appended");
        }
        assert_eq!(
            group.stats().messages,
            total as u64,
            "the cancelled overflow submission is the only one not appended"
        );
        group
            .check_invariants()
            .expect("bounded queue, one writer, consistent stats");
        group.shutdown().await;
        assert_eq!(group.writer_count(), 0);
    }
}
