// Licensed under the Apache License, Version 2.0. See LICENSE.

//! The shard's consumer-offset commit and commit-retry state machines.
//!
//! A shard applies records before it acknowledges them: the applied offset is
//! the last record whose orders, executions, and MD are durable, and the
//! committed offset is the last one the broker acknowledged. Committing per
//! batch would pay a broker round trip per batch, so the two offsets are
//! allowed to drift; [`CommitWatermark`] owns that drift and decides when it is
//! worth a round trip. [`CommitRetry`] owns what happens when that round trip
//! fails, so a broken broker costs commit lag instead of availability.
//!
//! The invariants (`docs/INVARIANTS.md` §5) these machines must never break:
//!
//! - `committed <= applied` whenever both are known: a commit may never claim
//!   work that is not durable.
//! - The committed offset never moves backwards, and neither does the applied
//!   offset; a late or duplicated acknowledgement cannot rewind either.
//! - A pending commit has an onset time, and clearing the watermark clears that
//!   onset: the apply-to-commit lag stays bounded by
//!   [`COMMIT_MAX_DELAY`].
//! - A retry budget never changes the commit decision, only the pace and the
//!   log volume: records stay applied and the commit keeps being retried.
//!
//! Everything here is pure state; the journal lives in
//! [`crate::engine_runner`], which is what keeps these machines testable
//! without a broker.

use std::time::{Duration, Instant};

/// How many records a shard applies between consumer-offset commits.
pub(crate) const COMMIT_INTERVAL_RECORDS: u64 = 256;

/// Upper bound on how long the consumer offset may lag the applied offset.
pub(crate) const COMMIT_MAX_DELAY: Duration = Duration::from_millis(100);

/// A single commit attempt is bounded so a wedged broker cannot stall the loop.
pub(crate) const COMMIT_TIMEOUT: Duration = Duration::from_secs(2);

/// Backoff for a commit that failed; the loop keeps applying records meanwhile.
pub(crate) const COMMIT_RETRY_INITIAL: Duration = Duration::from_millis(100);

/// Ceiling for that backoff. Reached after five doublings.
pub(crate) const COMMIT_RETRY_MAX: Duration = Duration::from_secs(2);

/// Consecutive commit failures before the retry reports its budget spent.
///
/// This bounds the *diagnostic* budget and the growing part of the backoff, not
/// the retries: a commit is the only durable progress marker a shard has, so a
/// spent budget keeps retrying at [`COMMIT_RETRY_MAX`] and the next success
/// resets it. At the default backoff this is roughly a minute of failures
/// before the first message says the commit is stuck rather than late.
pub(crate) const COMMIT_RETRY_MAX_ATTEMPTS: u32 = 30;

/// Commit failures are logged at most this often, never once per record.
pub(crate) const COMMIT_WARN_INTERVAL: Duration = Duration::from_secs(1);

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
///
/// Both offsets are monotonic by construction, and `pending_since` is the onset
/// of the current uncommitted range: the delay bound is measured from the first
/// record that made the watermark dirty, so it bounds the applied-offset lag
/// itself rather than the time since the last commit.
#[derive(Debug)]
pub(crate) struct CommitWatermark {
    applied: Option<u64>,
    committed: Option<u64>,
    pending_since: Option<Instant>,
}

impl CommitWatermark {
    /// A watermark that starts at the broker's committed offset, if any.
    ///
    /// Nothing is applied yet, so nothing is pending: the first `note_applied`
    /// that moves past `committed` starts the delay clock.
    pub(crate) fn new(committed: Option<u64>) -> Self {
        Self {
            applied: None,
            committed,
            pending_since: None,
        }
    }

    /// Record that everything up to `offset` has been fully applied.
    ///
    /// Offsets are non-decreasing; a backwards one is a bug in the caller, so
    /// the debug build catches it and the release build ignores it rather than
    /// reopening a replay window a crash already closed.
    pub(crate) fn note_applied(&mut self, offset: u64) {
        self.note_applied_at(offset, Instant::now());
    }

    /// [`note_applied`](Self::note_applied) against an explicit clock, so the
    /// delay bound can be tested without sleeping.
    fn note_applied_at(&mut self, offset: u64, now: Instant) {
        if let Some(applied) = self.applied {
            debug_assert!(
                offset >= applied,
                "applied offset moved backwards: {applied} -> {offset}"
            );
            if offset < applied {
                return;
            }
        }
        self.applied = Some(offset);
        if self.pending().is_some() {
            // Only the onset is recorded: later records extend the pending
            // range, they do not restart the delay clock.
            self.pending_since.get_or_insert(now);
        }
    }

    /// Record that the broker acknowledged `offset`.
    ///
    /// The watermark never moves backwards, even if an older offset is
    /// acknowledged by mistake, and it never claims more than was applied. A
    /// commit that does not cover the whole applied range leaves the range
    /// pending, so the onset and the delay budget survive it.
    pub(crate) fn note_committed(&mut self, offset: u64) {
        let offset = match self.applied {
            Some(applied) => {
                debug_assert!(
                    offset <= applied,
                    "commit offset {offset} is ahead of the applied offset {applied}"
                );
                // Release behaviour: clamp instead of moving `committed` past
                // `applied`, which would claim records that are not durable.
                offset.min(applied)
            }
            None => offset,
        };
        self.committed = Some(match self.committed {
            Some(committed) => committed.max(offset),
            None => offset,
        });
        if self.pending().is_none() {
            self.pending_since = None;
        }
    }

    /// The highest applied offset that the broker has not acknowledged yet.
    pub(crate) fn pending(&self) -> Option<u64> {
        match (self.applied, self.committed) {
            (Some(applied), Some(committed)) if committed >= applied => None,
            (Some(applied), _) => Some(applied),
            (None, _) => None,
        }
    }

    /// Whether the pending range is worth a broker round trip.
    ///
    /// True when something is pending and either the record bound or the delay
    /// bound is reached; never true when the applied offset equals the
    /// committed one. A range that was committed long ago does not make an
    /// idle shard commit: there is nothing to acknowledge.
    pub(crate) fn should_commit(
        &self,
        now: Instant,
        max_records: u64,
        max_delay: Duration,
    ) -> bool {
        let Some(applied) = self.applied else {
            return false;
        };
        let uncommitted = match self.committed {
            Some(committed) if committed >= applied => return false,
            Some(committed) => applied - committed,
            // Nothing was ever committed: every applied record is uncommitted.
            None => applied.saturating_add(1),
        };
        if uncommitted >= max_records {
            return true;
        }
        self.pending_since
            .is_some_and(|since| now.saturating_duration_since(since) >= max_delay)
    }

    /// The state invariants. A shard that violates one has lost the link
    /// between what is durable and what the broker believes.
    ///
    /// Monotonicity is not checkable from a snapshot of the state; it is
    /// enforced by the `max` in [`note_committed`](Self::note_committed) and the
    /// early return in [`note_applied`](Self::note_applied).
    pub(crate) fn check_invariants(&self) -> Result<(), String> {
        if let (Some(applied), Some(committed)) = (self.applied, self.committed)
            && committed > applied
        {
            return Err(format!(
                "committed offset {committed} is ahead of the applied offset {applied}"
            ));
        }
        if self.applied.is_none() && self.pending_since.is_some() {
            return Err("a pending commit has no applied offset".to_string());
        }
        match (self.pending().is_some(), self.pending_since) {
            (true, None) => Err("a pending commit has no onset time".to_string()),
            (false, Some(_)) => Err("a cleared watermark still has an onset time".to_string()),
            _ => Ok(()),
        }
    }

    /// Fail loudly in a debug build if the state is inconsistent. The release
    /// build pays nothing: the check is cheap enough to run on the commit path,
    /// which is where a violation would first matter.
    pub(crate) fn debug_check(&self) {
        debug_assert!(
            self.check_invariants().is_ok(),
            "commit watermark invariant broken: {self:?}"
        );
    }
}

/// Backoff state for a commit that failed.
///
/// The shard keeps applying records while this is backing off; the warning is
/// throttled so a broker outage logs once per [`COMMIT_WARN_INTERVAL`] instead
/// of once per batch.
#[derive(Debug)]
pub(crate) struct CommitRetry {
    /// Consecutive failures, saturated at [`COMMIT_RETRY_MAX_ATTEMPTS`].
    attempts: u32,
    /// Earliest instant of the next attempt. Only ever moved forwards.
    next_at: Instant,
    delay: Duration,
    last_warn: Option<Instant>,
}

impl CommitRetry {
    pub(crate) fn new() -> Self {
        Self {
            attempts: 0,
            next_at: Instant::now(),
            delay: COMMIT_RETRY_INITIAL,
            last_warn: None,
        }
    }

    /// Whether the backoff has elapsed; the first attempt is always ready.
    pub(crate) fn ready(&self, now: Instant) -> bool {
        now >= self.next_at
    }

    /// Consecutive failures in the current episode, saturated at the budget.
    pub(crate) fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Record a failed attempt and report whether it was inside the budget.
    ///
    /// The schedule is monotonic: a later `now` never moves the next attempt
    /// earlier than an already-scheduled one, and the delay doubles up to
    /// [`COMMIT_RETRY_MAX`]. `false` means the budget is spent — the retry still
    /// runs at the capped delay, the caller just reports it differently.
    pub(crate) fn record_failure(&mut self, now: Instant) -> bool {
        self.attempts = self
            .attempts
            .saturating_add(1)
            .min(COMMIT_RETRY_MAX_ATTEMPTS);
        self.next_at = self.next_at.max(now + self.delay);
        self.delay = (self.delay * 2).min(COMMIT_RETRY_MAX);
        self.attempts < COMMIT_RETRY_MAX_ATTEMPTS
    }

    /// Whether this failure should be logged, throttled to
    /// [`COMMIT_WARN_INTERVAL`].
    pub(crate) fn should_warn(&mut self, now: Instant) -> bool {
        let warn = self
            .last_warn
            .is_none_or(|at| now.saturating_duration_since(at) >= COMMIT_WARN_INTERVAL);
        if warn {
            self.last_warn = Some(now);
        }
        warn
    }

    /// A commit succeeded: the next attempt is immediate and the backoff is
    /// back at its initial value. The warning throttle is deliberately *not*
    /// cleared — it bounds log volume across episodes, not the retry schedule.
    pub(crate) fn reset(&mut self) {
        self.attempts = 0;
        self.delay = COMMIT_RETRY_INITIAL;
        self.next_at = Instant::now();
    }

    /// The retry invariants: the attempt count never exceeds its budget and the
    /// backoff stays between the initial delay and the cap. Schedule
    /// monotonicity is enforced by the `max` in
    /// [`record_failure`](Self::record_failure), not checkable from a snapshot.
    pub(crate) fn check_invariants(&self) -> Result<(), String> {
        if self.attempts > COMMIT_RETRY_MAX_ATTEMPTS {
            return Err(format!(
                "retry attempts {} exceed the budget {COMMIT_RETRY_MAX_ATTEMPTS}",
                self.attempts
            ));
        }
        if self.delay < COMMIT_RETRY_INITIAL {
            return Err(format!(
                "backoff {:?} is below the initial {:?}",
                self.delay, COMMIT_RETRY_INITIAL
            ));
        }
        if self.delay > COMMIT_RETRY_MAX {
            return Err(format!(
                "backoff {:?} exceeds the cap {:?}",
                self.delay, COMMIT_RETRY_MAX
            ));
        }
        Ok(())
    }

    /// Fail loudly in a debug build if the retry state is inconsistent.
    pub(crate) fn debug_check(&self) {
        debug_assert!(
            self.check_invariants().is_ok(),
            "commit retry invariant broken: {self:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: u64 = COMMIT_INTERVAL_RECORDS;

    fn assert_invariants(watermark: &CommitWatermark) {
        watermark
            .check_invariants()
            .unwrap_or_else(|error| panic!("watermark invariant broken: {error}"));
    }

    fn assert_retry_invariants(retry: &CommitRetry) {
        retry
            .check_invariants()
            .unwrap_or_else(|error| panic!("retry invariant broken: {error}"));
    }

    #[test]
    fn commit_waits_for_the_record_interval() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(0));
        watermark.note_applied_at(INTERVAL - 1, start);
        assert_invariants(&watermark);
        assert!(
            !watermark.should_commit(start, INTERVAL, COMMIT_MAX_DELAY),
            "below the interval and inside the delay window: not worth a round trip"
        );

        watermark.note_applied_at(INTERVAL, start);
        assert_invariants(&watermark);
        assert!(watermark.should_commit(start, INTERVAL, COMMIT_MAX_DELAY));
    }

    #[test]
    fn commit_waits_for_the_max_delay() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(0));
        watermark.note_applied_at(1, start);
        assert_invariants(&watermark);
        assert!(!watermark.should_commit(
            start + COMMIT_MAX_DELAY - Duration::from_millis(1),
            INTERVAL,
            COMMIT_MAX_DELAY
        ));
        assert!(watermark.should_commit(start + COMMIT_MAX_DELAY, INTERVAL, COMMIT_MAX_DELAY));
    }

    #[test]
    fn the_delay_bound_is_measured_from_the_first_pending_record() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(0));
        // A long idle before the first record must not make the first commit
        // immediate: the bound is the applied-offset lag, not the age of the
        // last commit.
        watermark.note_applied_at(1, start);
        // A second record extends the pending range without restarting the
        // delay clock.
        watermark.note_applied_at(2, start + Duration::from_millis(90));
        assert_invariants(&watermark);
        assert!(
            watermark.should_commit(start + COMMIT_MAX_DELAY, INTERVAL, COMMIT_MAX_DELAY),
            "the onset of the pending range, not the latest record, arms the delay"
        );
    }

    #[test]
    fn commit_is_skipped_when_nothing_is_pending() {
        let start = Instant::now();
        let long = Duration::from_secs(60);

        let idle = CommitWatermark::new(None);
        assert_invariants(&idle);
        assert!(!idle.should_commit(start, INTERVAL, COMMIT_MAX_DELAY));
        assert!(!idle.should_commit(start + long, INTERVAL, COMMIT_MAX_DELAY));
        assert_eq!(idle.pending(), None);

        // applied == committed: acknowledged, nothing to do.
        let mut settled = CommitWatermark::new(Some(7));
        settled.note_applied_at(7, start);
        assert_invariants(&settled);
        assert_eq!(settled.pending(), None);
        assert!(!settled.should_commit(start + long, INTERVAL, COMMIT_MAX_DELAY));

        // A committed offset ahead of the applied one is not a state a shard
        // can reach (the resume point is at most the committed offset), so the
        // invariant rejects it; the commit decision must still stay safe.
        let mut ahead = CommitWatermark::new(Some(9));
        ahead.note_applied_at(7, start);
        assert_eq!(ahead.pending(), None);
        assert!(
            ahead.check_invariants().is_err(),
            "committed ahead of applied is a bug, not a pending range"
        );
        assert!(!ahead.should_commit(start + long, INTERVAL, COMMIT_MAX_DELAY));
    }

    #[test]
    fn commit_without_a_prior_offset_counts_every_applied_record() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(None);
        watermark.note_applied_at(INTERVAL - 2, start);
        assert_invariants(&watermark);
        assert!(!watermark.should_commit(start, INTERVAL, COMMIT_MAX_DELAY));

        watermark.note_applied_at(INTERVAL - 1, start);
        assert_invariants(&watermark);
        assert!(
            watermark.should_commit(start, INTERVAL, COMMIT_MAX_DELAY),
            "offset 0 is applied too, so N applied records are N uncommitted"
        );
    }

    #[test]
    fn watermark_tracks_applied_and_committed() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(10));
        assert!(watermark.pending().is_none());

        watermark.note_applied_at(11, start);
        assert_eq!(watermark.pending(), Some(11));
        assert_invariants(&watermark);

        watermark.note_committed(11);
        assert!(watermark.pending().is_none());
        assert!(!watermark.should_commit(start + COMMIT_MAX_DELAY, INTERVAL, COMMIT_MAX_DELAY));
        assert_invariants(&watermark);

        // A stale acknowledgement must not move the watermark backwards.
        watermark.note_committed(5);
        assert_eq!(watermark.committed, Some(11));
        assert_invariants(&watermark);

        watermark.note_applied_at(12, start);
        assert_eq!(watermark.pending(), Some(12));
        assert_invariants(&watermark);
    }

    #[test]
    fn watermark_is_due_after_the_delay() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(None);
        watermark.note_applied_at(1, start);
        assert!(!watermark.should_commit(start, INTERVAL, COMMIT_MAX_DELAY));
        assert!(watermark.should_commit(start + COMMIT_MAX_DELAY, INTERVAL, COMMIT_MAX_DELAY));
        assert_invariants(&watermark);
    }

    #[test]
    fn a_partial_commit_keeps_the_range_and_its_onset_pending() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(4));
        watermark.note_applied_at(10, start);
        assert_invariants(&watermark);

        watermark.note_committed(7);
        assert_eq!(watermark.committed, Some(7));
        assert_eq!(watermark.pending(), Some(10));
        assert_eq!(
            watermark.pending_since,
            Some(start),
            "a partial commit must not restart the delay clock"
        );
        assert_invariants(&watermark);

        watermark.note_committed(10);
        assert_eq!(watermark.pending(), None);
        assert_eq!(watermark.pending_since, None);
        assert_invariants(&watermark);
    }

    #[test]
    fn a_commit_without_an_applied_offset_is_legal() {
        // The watermark starts from the broker's committed offset before the
        // first record is applied; there is nothing pending, so there is no
        // onset to record.
        let mut watermark = CommitWatermark::new(None);
        watermark.note_committed(5);
        assert_eq!(watermark.committed, Some(5));
        assert_eq!(watermark.applied, None);
        assert_eq!(watermark.pending(), None);
        assert_eq!(watermark.pending_since, None);
        assert_invariants(&watermark);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "applied offset moved backwards")]
    fn a_backwards_applied_offset_is_caught_in_debug_builds() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(0));
        watermark.note_applied_at(10, start);
        watermark.note_applied_at(9, start);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn a_backwards_applied_offset_is_ignored_in_release_builds() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(Some(0));
        watermark.note_applied_at(10, start);
        watermark.note_applied_at(9, start);
        assert_eq!(watermark.applied, Some(10));
        assert_invariants(&watermark);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "ahead of the applied offset")]
    fn a_commit_ahead_of_the_applied_offset_is_caught_in_debug_builds() {
        let mut watermark = CommitWatermark::new(None);
        watermark.note_applied_at(7, Instant::now());
        watermark.note_committed(8);
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn a_commit_ahead_of_the_applied_offset_is_clamped_in_release_builds() {
        let mut watermark = CommitWatermark::new(None);
        watermark.note_applied_at(7, Instant::now());
        watermark.note_committed(8);
        assert_eq!(
            watermark.committed,
            Some(7),
            "the release build clamps instead of claiming unapplied work"
        );
        assert_invariants(&watermark);
    }

    #[test]
    fn retry_backoff_is_bounded_and_throttles_warnings() {
        let mut retry = CommitRetry::new();
        let start = Instant::now();
        assert!(retry.ready(start));
        assert_retry_invariants(&retry);

        assert!(
            retry.record_failure(start),
            "the first failure is inside the budget"
        );
        assert!(retry.should_warn(start), "the first failure is logged");
        assert!(!retry.ready(start), "the retry waits for the backoff");
        assert_retry_invariants(&retry);

        // Within the warning window: silent, but the backoff still doubles.
        let early = start + Duration::from_millis(10);
        assert!(retry.record_failure(early));
        assert!(!retry.should_warn(early));
        assert!(!retry.ready(early + COMMIT_RETRY_INITIAL));
        assert_retry_invariants(&retry);

        // Once the window passes, one more warning is allowed.
        let later = start + COMMIT_WARN_INTERVAL;
        assert!(retry.record_failure(later));
        assert!(retry.should_warn(later));

        // The backoff never exceeds the cap.
        let mut now = later;
        for _ in 0..16 {
            retry.record_failure(now);
            now += Duration::from_secs(10);
            assert!(retry.delay <= COMMIT_RETRY_MAX);
        }
        assert_retry_invariants(&retry);

        retry.reset();
        assert_retry_invariants(&retry);
        assert_eq!(retry.delay, COMMIT_RETRY_INITIAL);
        assert_eq!(retry.attempts(), 0);
        assert!(retry.ready(Instant::now()));
    }

    #[test]
    fn the_next_attempt_never_moves_earlier() {
        let mut retry = CommitRetry::new();
        let start = Instant::now();
        // A failure an hour into an outage schedules the next attempt from
        // that instant; a later failure reported with an earlier clock cannot
        // pull the schedule in.
        retry.record_failure(start + Duration::from_secs(3_600));
        let scheduled = retry.next_at;
        assert!(scheduled > start);

        retry.record_failure(start);
        assert!(
            retry.next_at >= scheduled,
            "next_attempt_at must stay monotonic"
        );
        assert_retry_invariants(&retry);
    }

    #[test]
    fn the_retry_budget_bounds_the_growth_but_not_the_retries() {
        let mut retry = CommitRetry::new();
        let mut now = Instant::now();

        for attempt in 1..COMMIT_RETRY_MAX_ATTEMPTS {
            assert!(
                retry.record_failure(now),
                "attempt {attempt} is inside the budget"
            );
            assert!(
                retry.ready(now + COMMIT_RETRY_MAX),
                "the retry is scheduled"
            );
            assert_retry_invariants(&retry);
            now += COMMIT_RETRY_MAX;
        }

        assert!(
            !retry.record_failure(now),
            "the failure that spends the budget reports it"
        );
        assert_eq!(retry.attempts(), COMMIT_RETRY_MAX_ATTEMPTS);
        assert!(
            !retry.record_failure(now),
            "a failure after the budget keeps reporting it spent"
        );
        assert_eq!(
            retry.attempts(),
            COMMIT_RETRY_MAX_ATTEMPTS,
            "the attempt count saturates rather than growing"
        );
        assert_eq!(retry.delay, COMMIT_RETRY_MAX, "the backoff is at the cap");
        assert!(
            retry.ready(now + COMMIT_RETRY_MAX),
            "a spent budget keeps retrying at the capped delay"
        );
        assert_retry_invariants(&retry);

        // A success clears the episode: the budget and the delay start over.
        retry.reset();
        assert_eq!(retry.attempts(), 0);
        assert_eq!(retry.delay, COMMIT_RETRY_INITIAL);
        assert!(retry.record_failure(now));
        assert_retry_invariants(&retry);
    }

    #[test]
    fn a_success_clears_the_pending_watermark() {
        let start = Instant::now();
        let mut watermark = CommitWatermark::new(None);
        let mut retry = CommitRetry::new();

        watermark.note_applied_at(9, start);
        assert!(
            retry.record_failure(start),
            "the first failure is inside the budget"
        );
        assert_eq!(watermark.pending(), Some(9));
        assert!(!retry.ready(start));

        // The retry succeeds and acknowledges the whole pending range.
        retry.reset();
        watermark.note_committed(9);
        assert_eq!(watermark.pending(), None);
        assert_eq!(watermark.pending_since, None);
        assert_eq!(retry.attempts(), 0);
        assert!(
            retry.ready(Instant::now()),
            "a success makes the next attempt immediate"
        );
        assert_invariants(&watermark);
        assert_retry_invariants(&retry);
    }

    /// A deterministic xorshift, so the property tests are reproducible and
    /// need no extra dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
    }

    #[test]
    fn random_sequences_keep_the_watermark_invariants() {
        let mut rng = Rng(0x5eed_0000_0000_0001);
        for episode in 0..64u64 {
            let start = Instant::now();
            let mut now = start;
            let committed = (episode % 3 == 0).then_some(rng.below(64));
            let mut watermark = CommitWatermark::new(committed);
            // The applied offset starts at the committed one: a shard resumes
            // at or after the offset the broker already acknowledged.
            let mut applied = committed;
            assert_invariants(&watermark);

            for _ in 0..64 {
                now += Duration::from_millis(rng.below(40));
                match rng.below(4) {
                    // Apply forward: the only legal direction.
                    0 | 1 => {
                        let base = applied.max(watermark.committed).unwrap_or(0);
                        let offset = base + rng.below(300);
                        watermark.note_applied_at(offset, now);
                        applied = Some(offset);
                    }
                    // Acknowledge at most what was applied; with nothing
                    // applied yet the broker's offset is all there is.
                    2 => match watermark.applied {
                        Some(applied) => watermark.note_committed(rng.below(applied + 1)),
                        None => watermark.note_committed(rng.below(64)),
                    },
                    // A full acknowledgement, or a stale one that must be inert.
                    _ => {
                        if let Some(applied) = watermark.applied {
                            watermark.note_committed(applied);
                        }
                    }
                }

                assert_invariants(&watermark);
                if let (Some(applied), Some(committed)) = (watermark.applied, watermark.committed) {
                    assert!(
                        committed <= applied,
                        "committed {committed} ran ahead of applied {applied}"
                    );
                }
                assert_eq!(
                    watermark.pending().is_some(),
                    watermark.pending_since.is_some(),
                    "the onset and the pending range must agree"
                );
                if watermark.pending().is_none() {
                    assert!(
                        !watermark.should_commit(now, INTERVAL, COMMIT_MAX_DELAY),
                        "an idle watermark never commits"
                    );
                }
            }
        }
    }

    #[test]
    fn random_failure_sequences_keep_the_retry_invariants() {
        let mut rng = Rng(0x5eed_0000_0000_0002);
        for _ in 0..64 {
            let mut retry = CommitRetry::new();
            let mut now = Instant::now();
            for _ in 0..96 {
                now += Duration::from_millis(rng.below(250));
                if rng.below(5) == 0 {
                    retry.reset();
                    assert_eq!(retry.attempts(), 0, "a success clears the episode");
                } else {
                    retry.record_failure(now);
                    let _ = retry.should_warn(now);
                }
                assert_retry_invariants(&retry);
                assert!(retry.attempts() <= COMMIT_RETRY_MAX_ATTEMPTS);
                assert!(retry.delay >= COMMIT_RETRY_INITIAL);
                assert!(retry.delay <= COMMIT_RETRY_MAX);
            }
        }
    }
}
