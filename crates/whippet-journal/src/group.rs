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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Batches whose writer waited on the linger timer for another submission.
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
    stats: Arc<Stats>,
}

/// A live writer task and the sender that feeds it.
struct Writer {
    sender: mpsc::Sender<Job>,
    handle: JoinHandle<()>,
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
    /// Submissions on one partition are ordered by arrival at the writer, and
    /// a submission's messages are never interleaved with another submission's.
    pub async fn append(
        &self,
        topic: Topic,
        partition: u32,
        messages: Vec<JournalMessage>,
    ) -> Result<AppendResult> {
        let (reply, response) = oneshot::channel();
        let mut pending = Some((messages, reply));

        // Two attempts: a writer that died (a panic in the journal, or a
        // shutting-down runtime) leaves a stale sender in the map. Dropping the
        // stale entry and retrying keeps one dead task from bricking a
        // partition forever.
        for attempt in 0..2 {
            let sender = self.sender(topic, partition);
            let (messages, reply) = pending.take().expect("job is restored on a failed send");
            match sender.send(Job { messages, reply }).await {
                Ok(()) => {
                    return response.await.map_err(|_| JournalError::WriterStopped)?;
                }
                Err(error) => {
                    let job = error.0;
                    pending = Some((job.messages, job.reply));
                    self.remove_writer(topic, partition);
                    if attempt == 1 {
                        break;
                    }
                }
            }
        }
        Err(JournalError::WriterStopped)
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

    /// Close every partition queue and wait for the writers to finish.
    ///
    /// Dropping the last [`GroupCommit`] does the same thing without waiting:
    /// the sender map drops, each writer's channel closes, and each task exits
    /// after flushing the jobs it already accepted. This is the awaitable form,
    /// for tests and orderly shutdown.
    pub async fn shutdown(&self) {
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
    }

    /// Sender for `(topic, partition)`, spawning its writer on first use.
    ///
    /// The map lock is held only for the lookup/insert and never across an
    /// await. Writer tasks never touch the map, so they cannot deadlock here.
    fn sender(&self, topic: Topic, partition: u32) -> mpsc::Sender<Job> {
        let mut writers = self.lock_writers();
        if let Some(writer) = writers.get(&(topic, partition)) {
            return writer.sender.clone();
        }
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
            },
        );
        sender
    }

    fn remove_writer(&self, topic: Topic, partition: u32) {
        self.lock_writers().remove(&(topic, partition));
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
        // therefore never waits: the batch is already full.
        let mut closed = false;
        if budget > 0 && !config.linger.is_zero() {
            stats.linger_waits.fetch_add(1, Ordering::Relaxed);
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

        flush(&journal, topic, partition, jobs, &stats).await;
        if closed {
            // A held job was already received, so it must still be answered.
            if let Some(job) = pending.take() {
                flush(&journal, topic, partition, vec![job], &stats).await;
            }
            break;
        }
    }
}

/// Append one batch and answer every waiter.
async fn flush(journal: &Journal, topic: Topic, partition: u32, jobs: Vec<Job>, stats: &Stats) {
    let total: usize = jobs.iter().map(|job| job.messages.len()).sum();
    let mut replies = Vec::with_capacity(jobs.len());
    let mut messages = Vec::with_capacity(total);
    for job in jobs {
        replies.push((job.messages.len() as u32, job.reply));
        messages.extend(job.messages);
    }

    match journal.append_owned(topic, partition, messages).await {
        Ok(appended) => {
            stats.batches.fetch_add(1, Ordering::Relaxed);
            stats.messages.fetch_add(total as u64, Ordering::Relaxed);
            stats
                .max_batch_seen
                .fetch_max(total as u64, Ordering::Relaxed);

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
    async fn an_empty_submission_reports_the_partition_head() {
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
        let empty = group
            .append(Topic::Orders, 0, Vec::new())
            .await
            .expect("empty submission");
        assert_eq!(empty.partition, 0);
        assert_eq!(empty.base_offset, 1);
        assert_eq!(empty.count, 0);
        group.shutdown().await;
    }
}
