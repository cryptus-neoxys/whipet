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
//!
//! The admission lifecycle is explicit: [`TurboWindow::try_admit`] returns a
//! [`TurboAdmission`] or an [`AdmitError`] naming the binding bound, and the
//! admission moves `Queued -> Completed` when its append is durable or
//! `Queued -> Abandoned` when its entry is dropped (a failed send, a client
//! disconnect, a stopping shard). Either way the slot is released exactly
//! once, and the hot path allocates nothing beyond the semaphore permit the
//! design already pays.

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

/// Which window bound rejected an admission.
///
/// The reason feeds counters and tests, not the wire: HTTP still answers 503
/// `window_full` and TCP still answers its `WindowFull` N-ack whichever bound
/// bound, so no new reject code is introduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitError {
    /// The count bound (`window_commands`) is binding: every slot is an
    /// unjournaled command.
    CountFull,
    /// The age bound (`window_ms`) is binding: the oldest unjournaled command
    /// is older than the configured bound.
    AgeFull,
}

/// The debug-build lifecycle marker for one admission.
///
/// `Queued -> Completed` is the writer's path (`TurboAdmission::complete`
/// after the orders append is durable). `Queued -> Abandoned` is every other
/// exit: a failed `try_send`, a client disconnect, a stopping shard. The state
/// compiles out of release builds; the permit is what enforces exactly-once
/// release everywhere.
#[cfg(debug_assertions)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionState {
    Queued,
    Completed,
    Abandoned,
}

/// One admitted command's claim on the window.
///
/// The admission owns one window id, its `queued_ns` timestamp, and one
/// semaphore permit. The permit is released by [`TurboAdmission::complete`]
/// once the write-behind has appended the command, or by `Drop` if the entry
/// never reaches that point. Either way it is released exactly once: the
/// release path takes the permit, so a second release has nothing to release.
/// Dropping it without completing (shard failure, panic) also releases the
/// semaphore permit; the node is stopping in that case, so the stale age entry
/// is irrelevant.
pub struct TurboAdmission {
    window: Arc<TurboWindow>,
    id: u64,
    queued_ns: u64,
    permit: Option<OwnedSemaphorePermit>,
    /// Lifecycle marker; debug builds only, so release stays allocation-free
    /// and branch-free in release.
    #[cfg(debug_assertions)]
    state: AdmissionState,
}

impl TurboAdmission {
    /// When the command entered the window (monotonic nanos).
    pub fn queued_ns(&self) -> u64 {
        self.queued_ns
    }

    /// The admission's id: strictly increasing across admissions to its
    /// window, unique per window.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Release this admission's slot exactly once: its orders append is
    /// durable.
    ///
    /// Invariant the writer must hold: admissions are completed in seq order,
    /// so the window front is the id being completed and the release is an
    /// O(1) pop. A second `complete` is a bug and panics in debug builds; in
    /// release it is a no-op because the permit is already gone.
    pub fn complete(&mut self) {
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(
                self.state,
                AdmissionState::Queued,
                "an admission's slot must be released exactly once"
            );
            self.state = AdmissionState::Completed;
        }
        self.release();
    }

    /// The release path: take the permit, return it to the semaphore, and let
    /// [`TurboWindow::release`] drop the clock entry. Allocation-free.
    fn release(&mut self) {
        if self.permit.take().is_some() {
            self.window.release(self.id);
        }
    }
}

impl Drop for TurboAdmission {
    fn drop(&mut self) {
        // Every exit path releases the slot exactly once: a durable append,
        // a rejected send, a client disconnect before the shard sees the
        // entry, a shard stopping. A leaked permit would shrink the window
        // for the life of the process.
        #[cfg(debug_assertions)]
        if self.permit.is_some() {
            debug_assert_eq!(
                self.state,
                AdmissionState::Queued,
                "an admission's slot must be released exactly once"
            );
            self.state = AdmissionState::Abandoned;
        }
        self.release();
    }
}

struct Clock {
    /// `(admission id, ingestion nanos)`, in admission (and seq) order.
    queued: VecDeque<(u64, u64)>,
}

/// The admission gate shared by ingress, the matcher, and the write-behind.
pub struct TurboWindow {
    permits: Arc<Semaphore>,
    /// The count bound, kept for the invariant checks.
    capacity: usize,
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
            capacity: config.window_commands,
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

    /// Admit one command, or reject it with the binding bound.
    ///
    /// The count bound is strict across ingress tasks because the semaphore is
    /// acquired before the age check. The age check rejects when the oldest
    /// command that is still unjournaled is older than the configured bound.
    /// Either rejection is counted under its reason and never logged per
    /// rejection; the caller maps both to the same wire answer.
    pub fn try_admit(self: &Arc<Self>, now_ns: u64) -> Result<TurboAdmission, AdmitError> {
        let permit = match self.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.drops.fetch_add(1, Ordering::Relaxed);
                self.count_drops.fetch_add(1, Ordering::Relaxed);
                return Err(AdmitError::CountFull);
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
            return Err(AdmitError::AgeFull);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        clock.queued.push_back((id, now_ns));
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Ok(TurboAdmission {
            window: Arc::clone(self),
            id,
            queued_ns: now_ns,
            permit: Some(permit),
            #[cfg(debug_assertions)]
            state: AdmissionState::Queued,
        })
    }

    /// Release one command's claim after its orders append is durable.
    ///
    /// Invariant the writer must hold: it completes admissions in seq order,
    /// so the queue front is the id being released and the pop is O(1). The
    /// fallback search exists for the out-of-order case: a raced `try_send`
    /// failure or a disconnect can abandon an entry before its predecessors,
    /// and a leaked slot would be a permanent capacity loss, so a mismatch is
    /// tolerated by searching for the id. Search cost is O(window) and paid
    /// only on the abnormal path.
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

    /// Check the window's bookkeeping invariants. Test-only: the walk is
    /// O(window) and takes the clock mutex, so no production path may call it.
    ///
    /// Checked:
    /// - `in_flight` equals the number of queued clock entries and never
    ///   exceeds the count bound,
    /// - `available_permits() + in_flight == capacity`, so every admitted
    ///   command holds exactly one permit and none are lost,
    /// - admission ids strictly increase (hence are unique) front to back and
    ///   queued timestamps are non-decreasing,
    /// - `oldest_queued_ns()` equals the front entry's timestamp,
    /// - total drops equal `count_drops + age_drops`.
    pub fn check_invariants(&self) -> Result<(), String> {
        let in_flight = self.in_flight.load(Ordering::Relaxed);
        let capacity = self.capacity as u64;
        let oldest = self.oldest_queued_ns();

        let clock = self
            .clock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if in_flight != clock.queued.len() as u64 {
            return Err(format!(
                "in_flight ({in_flight}) does not equal the queued entries ({})",
                clock.queued.len()
            ));
        }
        if in_flight > capacity {
            return Err(format!(
                "in_flight ({in_flight}) exceeds capacity ({capacity})"
            ));
        }
        for ((id, queued), (next_id, next_queued)) in
            clock.queued.iter().zip(clock.queued.iter().skip(1))
        {
            if next_id <= id {
                return Err(format!(
                    "admission ids must strictly increase (found {id} then {next_id})"
                ));
            }
            if next_queued < queued {
                return Err(format!(
                    "queued timestamps must not decrease (found {queued} then {next_queued})"
                ));
            }
        }
        let front = clock.queued.front().map(|(_, queued)| *queued);
        if front != oldest {
            return Err(format!(
                "oldest_queued_ns ({oldest:?}) does not equal the front ({front:?})"
            ));
        }
        drop(clock);

        let available = self.permits.available_permits() as u64;
        if available + in_flight != capacity {
            return Err(format!(
                "available permits ({available}) + in_flight ({in_flight}) \
                 does not equal capacity ({capacity})"
            ));
        }

        let drops = self.drops.load(Ordering::Relaxed);
        let (count, age) = self.drop_reasons();
        if drops != count + age {
            return Err(format!(
                "drops ({drops}) does not equal count_drops + age_drops ({count} + {age})"
            ));
        }
        Ok(())
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
    /// A window bound rejected the command; retry shortly. The payload names
    /// the binding bound for counters and tests; the wire answer is the same
    /// `window_full` (HTTP 503, TCP N-ack) whichever bound bound.
    WindowFull(AdmitError),
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
            .map_err(EnqueueError::WindowFull)?;
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
            // The lane's capacity equals the count bound and every queued
            // entry holds one permit, so a full lane means the count bound is
            // binding. The dropped entry releases its permit through
            // `TurboAdmission`'s drop.
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err(EnqueueError::WindowFull(AdmitError::CountFull))
            }
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
        let mut first = window.try_admit(1).expect("first");
        let second = window.try_admit(2).expect("second");
        assert!(matches!(window.try_admit(3), Err(AdmitError::CountFull)));
        assert_eq!(window.drops(), 1);
        assert_eq!(window.drop_reasons(), (1, 0));
        assert_eq!(window.in_flight(), 2);
        window.check_invariants().expect("invariants hold");

        // Explicit completion releases the slot exactly once.
        first.complete();
        assert_eq!(window.in_flight(), 1);
        window.check_invariants().expect("invariants hold");
        let third = window.try_admit(4).expect("a freed slot is reusable");
        window.check_invariants().expect("invariants hold");
        // Dropping after `complete` must not release again.
        drop(first);
        drop(second);
        assert_eq!(window.in_flight(), 1);
        window.check_invariants().expect("invariants hold");
        drop(third);
        assert_eq!(window.in_flight(), 0);
        assert_eq!(window.oldest_queued_ns(), None);
        window.check_invariants().expect("invariants hold");
    }

    #[test]
    fn the_age_bound_rejects_and_releases_the_permit() {
        let window = window(10, 5);
        let first = window.try_admit(0).expect("first");
        // 5 ms exactly is allowed; strictly older is not.
        let second = window.try_admit(5_000_000).expect("age equals the bound");
        assert!(matches!(
            window.try_admit(5_000_001),
            Err(AdmitError::AgeFull)
        ));
        assert_eq!(window.drop_reasons(), (0, 1));
        assert_eq!(window.drops(), 1);
        window.check_invariants().expect("invariants hold");
        drop(first);
        // The oldest is now `second`, so the same timestamp is no longer old.
        assert!(window.try_admit(5_000_001).is_ok());
        window.check_invariants().expect("invariants hold");
        assert_eq!(window.drops(), 1);
        drop(second);
        window.check_invariants().expect("invariants hold");
    }

    #[test]
    fn oldest_age_tracks_the_front_of_the_window() {
        let window = window(4, 50);
        let first = window.try_admit(1_000);
        let _second = window.try_admit(2_000);
        assert_eq!(window.oldest_age(3_000), Duration::from_nanos(2_000));
        window.check_invariants().expect("invariants hold");
        drop(first);
        assert_eq!(window.oldest_age(3_000), Duration::from_nanos(1_000));
        window.check_invariants().expect("invariants hold");
    }

    #[test]
    fn an_abandoned_admission_frees_the_slot() {
        let window = window(1, 50);
        let admission = window.try_admit(1).expect("first");
        assert_eq!(window.in_flight(), 1);
        // Never completed: a failed send or a disconnect drops the admission,
        // and the drop is the release.
        drop(admission);
        assert_eq!(window.in_flight(), 0);
        window.check_invariants().expect("invariants hold");
        // The slot is reusable.
        assert!(window.try_admit(2).is_ok());
        window.check_invariants().expect("invariants hold");
    }

    #[test]
    fn completion_out_of_order_still_releases_and_holds_invariants() {
        // The writer's invariant is seq-order completion, so the front pop is
        // the normal path. A raced send failure can abandon an entry ahead of
        // its predecessors; the release fallback search then removes the
        // abandoned entry from the middle of the clock so no slot leaks.
        let window = window(3, 50);
        let mut first = window.try_admit(1).expect("first");
        let mut second = window.try_admit(2).expect("second");
        let mut third = window.try_admit(3).expect("third");
        // Complete out of order: middle, then tail, then front.
        second.complete();
        window.check_invariants().expect("invariants hold");
        assert_eq!(window.in_flight(), 2);
        third.complete();
        window.check_invariants().expect("invariants hold");
        // The front is still `first`, so this completion is the fast pop.
        first.complete();
        window.check_invariants().expect("invariants hold");
        assert_eq!(window.in_flight(), 0);
        assert_eq!(window.oldest_queued_ns(), None);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "released exactly once")]
    fn completing_an_admission_twice_is_a_bug() {
        let window = window(1, 50);
        let mut admission = window.try_admit(1).expect("first");
        admission.complete();
        admission.complete();
    }

    #[test]
    fn now_ns_is_monotonic() {
        let first = now_ns();
        let second = now_ns();
        assert!(second >= first);
    }
}
