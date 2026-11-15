// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Turbo-mode ingress: a bounded uncommitted window and the per-shard
//! sequencer hand-off.
//!
//! Durable ingress acknowledges after the Iggy append; turbo acknowledges
//! after [`ShardEngine::process`](whippet_engine::ShardEngine) returns, on the
//! strength of process-local state, and a write-behind task journals the
//! command afterwards. The window is the difference: commands that are
//! matched and acknowledged but not yet appended to the orders log. The
//! window is the crash-loss bound, so it is enforced at admission, not by
//! dropping work later.
//!
//! [`TurboWindow::try_admit`] takes one permit from a bounded semaphore (the
//! count bound) and records the oldest outstanding ingestion timestamp under
//! one small mutex (the age bound). The permit travels with the command
//! through the matcher and is released only when the write-behind has
//! appended the command record and its two output topics. If the journal
//! cannot keep up, ingress is rejected with `window_full`; it never silently
//! falls back to durable.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use whippet_core::protocol::{Command, RejectReason};
use whippet_journal::JournalMessage;

/// Default number of commands allowed in one shard's uncommitted window.
pub const DEFAULT_WINDOW_COMMANDS: usize = 10_000;

/// Default maximum age of the oldest unjournaled command, in milliseconds.
pub const DEFAULT_WINDOW_MS: u64 = 50;

/// Largest accepted count bound. A larger window defeats the point of a
/// bounded loss window; the value is rejected, not clamped, so a typo cannot
/// silently weaken the guarantee.
pub const MAX_WINDOW_COMMANDS: usize = 1_000_000;

/// Largest accepted age bound, in milliseconds.
pub const MAX_WINDOW_MS: u64 = 60_000;

/// Turbo window limits from `WHIPPET_TURBO_WINDOW_COMMANDS` and
/// `WHIPPET_TURBO_WINDOW_MS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurboConfig {
    pub window_commands: usize,
    pub window_ms: u64,
}

impl Default for TurboConfig {
    fn default() -> Self {
        Self {
            window_commands: DEFAULT_WINDOW_COMMANDS,
            window_ms: DEFAULT_WINDOW_MS,
        }
    }
}

impl TurboConfig {
    /// Validate the two knobs. Both must be positive and within the documented
    /// maximum; a zero would make every order fail with `window_full`.
    pub fn new(window_commands: usize, window_ms: u64) -> Result<Self, String> {
        if window_commands == 0 {
            return Err("turbo window commands must be at least 1".into());
        }
        if window_commands > MAX_WINDOW_COMMANDS {
            return Err(format!(
                "turbo window commands must be at most {MAX_WINDOW_COMMANDS}"
            ));
        }
        if window_ms == 0 {
            return Err("turbo window age must be at least 1 ms".into());
        }
        if window_ms > MAX_WINDOW_MS {
            return Err(format!(
                "turbo window age must be at most {MAX_WINDOW_MS} ms"
            ));
        }
        Ok(Self {
            window_commands,
            window_ms,
        })
    }
}

/// One admitted command's claim on the window.
///
/// The permit is released by [`TurboWindow::complete`] once the write-behind
/// has appended the command. Dropping it without completing (shard failure,
/// panic) also releases the semaphore permit; the node is stopping in that
/// case, so the stale age entry is irrelevant.
pub struct TurboAdmission {
    window: Arc<TurboWindow>,
    id: u64,
    queued_ns: u64,
    permit: Option<OwnedSemaphorePermit>,
}

impl TurboAdmission {
    /// When the command entered the window (monotonic nanos).
    pub fn queued_ns(&self) -> u64 {
        self.queued_ns
    }
}

impl Drop for TurboAdmission {
    fn drop(&mut self) {
        // Every exit path releases the slot: a durable append, a rejected
        // send, a client disconnect before the shard sees the entry. A leaked
        // permit would shrink the window for the life of the process.
        if self.permit.take().is_some() {
            self.window.release(self.id);
        }
    }
}

struct Clock {
    /// `(admission id, ingestion nanos)`, in admission (and seq) order.
    queued: VecDeque<(u64, u64)>,
}

/// The admission gate shared by ingress, the matcher, and the write-behind.
pub struct TurboWindow {
    permits: Arc<Semaphore>,
    window_ms: u64,
    clock: Mutex<Clock>,
    next_id: AtomicU64,
    in_flight: AtomicU64,
    drops: AtomicU64,
    count_drops: AtomicU64,
    age_drops: AtomicU64,
}

impl TurboWindow {
    pub fn new(config: TurboConfig) -> Arc<Self> {
        Arc::new(Self {
            permits: Arc::new(Semaphore::new(config.window_commands)),
            window_ms: config.window_ms,
            clock: Mutex::new(Clock {
                queued: VecDeque::with_capacity(config.window_commands.min(16_384)),
            }),
            next_id: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            drops: AtomicU64::new(0),
            count_drops: AtomicU64::new(0),
            age_drops: AtomicU64::new(0),
        })
    }

    /// Admit one command, or reject it with `window_full`.
    ///
    /// The count bound is strict across ingress tasks because the semaphore is
    /// acquired before the age check. The age check rejects when the oldest
    /// command that is still unjournaled is older than the configured bound.
    pub fn try_admit(self: &Arc<Self>, now_ns: u64) -> Option<TurboAdmission> {
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.drops.fetch_add(1, Ordering::Relaxed);
                self.count_drops.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        let max_age_ns = self.window_ms.saturating_mul(1_000_000);
        let mut clock = self
            .clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(&(_, oldest)) = clock.queued.front()
            && now_ns.saturating_sub(oldest) > max_age_ns
        {
            drop(clock);
            // Releasing the permit here lets a later admission that finds the
            // window drained proceed immediately.
            drop(permit);
            self.drops.fetch_add(1, Ordering::Relaxed);
            self.age_drops.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        clock.queued.push_back((id, now_ns));
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Some(TurboAdmission {
            window: Arc::clone(self),
            id,
            queued_ns: now_ns,
            permit: Some(permit),
        })
    }

    /// Release one command's claim after its orders append is durable.
    ///
    /// The write-behind completes in seq order, so the queue front is the
    /// command being released. A mismatch is tolerated by searching for the
    /// id: a raced `try_send` failure or a disconnect can abandon an entry out
    /// of order, and a leaked slot would be a permanent capacity loss.
    fn release(&self, id: u64) {
        {
            let mut clock = self
                .clock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if clock.queued.front().is_some_and(|(front, _)| *front == id) {
                clock.queued.pop_front();
            } else if let Some(position) = clock.queued.iter().position(|(queued, _)| *queued == id)
            {
                clock.queued.remove(position);
            }
        }
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Commands currently admitted and not yet journaled.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Ingestion timestamp of the oldest unjournaled command, if any.
    pub fn oldest_queued_ns(&self) -> Option<u64> {
        let clock = self
            .clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        clock.queued.front().map(|(_, queued)| *queued)
    }

    /// Age of the oldest unjournaled command, for the metrics gauge.
    pub fn oldest_age(&self, now_ns: u64) -> Duration {
        self.oldest_queued_ns()
            .map(|queued| Duration::from_nanos(now_ns.saturating_sub(queued)))
            .unwrap_or_default()
    }

    /// Commands rejected by the count or age bound.
    pub fn drops(&self) -> u64 {
        self.drops.load(Ordering::Relaxed)
    }

    /// Rejections split by the binding bound.
    pub fn drop_reasons(&self) -> (u64, u64) {
        (
            self.count_drops.load(Ordering::Relaxed),
            self.age_drops.load(Ordering::Relaxed),
        )
    }
}

/// A command handed from ingress to the shard thread.
///
/// The gateway parses and validates once, builds the exact journal message the
/// write-behind will append, and keeps the typed command so the matcher never
/// re-decodes its own input.
pub struct TurboEntry {
    pub command: Command,
    pub message: JournalMessage,
    pub admission: TurboAdmission,
    /// Match result sent back to the waiting gateway task.
    pub ack: oneshot::Sender<TurboAck>,
    pub received_ns: u64,
}

/// The matcher's answer to a turbo ingress request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurboAck {
    /// The local sequence assigned by the matcher; the turbo offset domain.
    pub seq: u64,
    /// False when the engine rejected the command (see `reject`).
    pub accepted: bool,
    pub reject: RejectReason,
}

/// Why a turbo submit could not be queued for matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueError {
    /// The count or age bound rejected the command; retry shortly.
    WindowFull,
    /// The partition has no lane or its shard thread has stopped.
    ShardUnavailable,
}

/// One shard's turbo ingress lane.
pub struct TurboShard {
    pub sender: mpsc::Sender<TurboEntry>,
    pub window: Arc<TurboWindow>,
}

/// Every active shard's turbo ingress lane plus its window.
///
/// Built once at startup. The gateway and TCP listener submit through
/// [`TurboIngress::shard`]; the shard tasks own the matching receivers, which
/// are returned separately so the context never has to hand out a receiver.
pub struct TurboIngress {
    shards: HashMap<u32, TurboShard>,
}

impl TurboIngress {
    /// Build one bounded lane per active shard.
    ///
    /// The channel capacity equals the count bound: a command holds one window
    /// permit from admission until its orders append is durable, so at most
    /// `window_commands` entries can be queued on a lane.
    pub fn new(
        config: TurboConfig,
        shards: impl IntoIterator<Item = u32>,
    ) -> (Arc<Self>, HashMap<u32, mpsc::Receiver<TurboEntry>>) {
        let mut lanes = HashMap::new();
        let mut receivers = HashMap::new();
        for shard in shards {
            let window = TurboWindow::new(config);
            let (sender, receiver) = mpsc::channel(config.window_commands.max(1));
            lanes.insert(shard, TurboShard { sender, window });
            receivers.insert(shard, receiver);
        }
        (Arc::new(Self { shards: lanes }), receivers)
    }

    /// The lane for a partition, or `None` when the shard is not active.
    pub fn shard(&self, partition: u32) -> Option<&TurboShard> {
        self.shards.get(&partition)
    }

    pub fn shards(&self) -> impl Iterator<Item = (u32, &TurboShard)> {
        self.shards.iter().map(|(shard, lane)| (*shard, lane))
    }

    pub fn total_in_flight(&self) -> u64 {
        self.shards
            .values()
            .map(|lane| lane.window.in_flight())
            .sum()
    }

    pub fn total_drops(&self) -> u64 {
        self.shards.values().map(|lane| lane.window.drops()).sum()
    }

    /// Rejections across the pool, split into `(count, age)`.
    pub fn drop_reasons(&self) -> (u64, u64) {
        self.shards.values().fold((0, 0), |(count, age), lane| {
            let (lane_count, lane_age) = lane.window.drop_reasons();
            (count + lane_count, age + lane_age)
        })
    }

    pub fn window(&self, partition: u32) -> Option<&Arc<TurboWindow>> {
        self.shards.get(&partition).map(|lane| &lane.window)
    }

    /// Admit one command and enqueue it for the shard, without awaiting the
    /// match. The caller owns the returned receiver and decides the timeout.
    ///
    /// The window permit is taken before the send, so a full window is
    /// rejected before a queue slot is spent. A rejected send drops the entry
    /// and releases the permit through `TurboAdmission`'s drop.
    pub fn enqueue(
        &self,
        partition: u32,
        command: Command,
        message: JournalMessage,
    ) -> Result<oneshot::Receiver<TurboAck>, EnqueueError> {
        let lane = self
            .shards
            .get(&partition)
            .ok_or(EnqueueError::ShardUnavailable)?;
        let observed_ns = now_ns();
        let admission = lane
            .window
            .try_admit(observed_ns)
            .ok_or(EnqueueError::WindowFull)?;
        let (ack, receiver) = oneshot::channel();
        let entry = TurboEntry {
            command,
            message,
            admission,
            ack,
            received_ns: observed_ns,
        };
        match lane.sender.try_send(entry) {
            Ok(()) => Ok(receiver),
            Err(mpsc::error::TrySendError::Full(_)) => Err(EnqueueError::WindowFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(EnqueueError::ShardUnavailable),
        }
    }
}

/// Monotonic nanosecond clock used for window ages.
pub fn now_ns() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(commands: usize, ms: u64) -> Arc<TurboWindow> {
        TurboWindow::new(TurboConfig::new(commands, ms).unwrap())
    }

    #[test]
    fn a_zero_or_oversized_knob_is_rejected() {
        assert!(TurboConfig::new(0, 50).is_err());
        assert!(TurboConfig::new(1, 0).is_err());
        assert!(TurboConfig::new(MAX_WINDOW_COMMANDS + 1, 50).is_err());
        assert!(TurboConfig::new(1_000, MAX_WINDOW_MS + 1).is_err());
        assert_eq!(
            TurboConfig::new(2, 7).unwrap(),
            TurboConfig {
                window_commands: 2,
                window_ms: 7
            }
        );
    }

    #[test]
    fn the_count_bound_is_strict_and_completion_frees_a_slot() {
        let window = window(2, 50);
        let first = window.try_admit(1).expect("first");
        let second = window.try_admit(2).expect("second");
        assert!(window.try_admit(3).is_none());
        assert_eq!(window.drops(), 1);
        assert_eq!(window.in_flight(), 2);

        drop(first);
        assert_eq!(window.in_flight(), 1);
        let third = window.try_admit(4).expect("a freed slot is reusable");
        drop(second);
        drop(third);
        assert_eq!(window.in_flight(), 0);
        assert_eq!(window.oldest_queued_ns(), None);
    }

    #[test]
    fn the_age_bound_rejects_and_releases_the_permit() {
        let window = window(10, 5);
        let first = window.try_admit(0).expect("first");
        // 5 ms exactly is allowed; strictly older is not.
        let second = window.try_admit(5_000_000).expect("age equals the bound");
        assert!(window.try_admit(5_000_001).is_none());
        drop(first);
        // The oldest is now `second`, so the same timestamp is no longer old.
        assert!(window.try_admit(5_000_001).is_some());
        assert_eq!(window.drops(), 1);
        drop(second);
    }

    #[test]
    fn oldest_age_tracks_the_front_of_the_window() {
        let window = window(4, 50);
        let first = window.try_admit(1_000);
        let _second = window.try_admit(2_000);
        assert_eq!(window.oldest_age(3_000), Duration::from_nanos(2_000));
        drop(first);
        assert_eq!(window.oldest_age(3_000), Duration::from_nanos(1_000));
    }

    #[test]
    fn now_ns_is_monotonic() {
        let first = now_ns();
        let second = now_ns();
        assert!(second >= first);
    }
}
