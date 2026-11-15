// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Replay-safe retention planning (ADR 0014, Part 2).
//!
//! This module is the policy half of retention: given the order-log head, the
//! committed offsets of the live consumers of that log, and the newest verified
//! snapshot, it computes the oldest log offset that may be deleted for one
//! partition. It never deletes, trims, commits, or appends; the command that
//! calls it (`whippet retention-plan`) is read-only by construction.
//!
//! The rule is fail-closed. Anything the input cannot prove returns
//! [`RetentionDecision::Blocked`]: a missing consumer offset, a missing
//! snapshot, a boundary at the start of the log, or a safety margin that covers
//! the whole window all retain the log instead of trimming it.

use std::cmp::Ordering;

/// Everything the retention rule observes for one partition.
///
/// Offsets are inclusive: a consumer committed at offset `c` has applied every
/// record `<= c`, `head` is the last offset in the partition, and
/// `snapshot_offset` is the newest verified snapshot's maximum `last_offset`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionInput {
    pub head: u64,
    /// `(consumer name, committed offset)`. Offsets must be positions in the
    /// same log being trimmed: a consumer of a different topic (the MD consumer
    /// commits on `md`) has an unrelated offset space and must not be listed
    /// here. An unknown committed offset must not be represented either: leave
    /// the consumer out and the rule blocks.
    pub consumers: Vec<(String, u64)>,
    /// `None` when no verified snapshot exists for the partition.
    pub snapshot_offset: Option<u64>,
    /// Records retained behind the boundary as a recovery cushion.
    pub safety_margin_records: u64,
}

/// The rule's verdict for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionDecision {
    /// Every consumer is past `safe_through` and the verified snapshot covers
    /// it, so records up to and including it may be deleted.
    Eligible { safe_through: u64 },
    /// The rule cannot prove any range is safe. `safe_through` is reserved for
    /// a partial range — one proven but not usable yet — and is `None` for
    /// every block the planner raises today, because none of them prove
    /// anything.
    Blocked {
        reason: String,
        safe_through: Option<u64>,
    },
}

impl RetentionDecision {
    /// Whether a deletion window was proven.
    pub fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible { .. })
    }

    /// The inclusive offset up to which records may be deleted, if any.
    pub fn safe_through(&self) -> Option<u64> {
        match self {
            Self::Eligible { safe_through } => Some(*safe_through),
            Self::Blocked { safe_through, .. } => *safe_through,
        }
    }
}

/// The oldest offset every consumer has committed and the verified snapshot
/// covers, with a human-readable source for it.
///
/// The minimum is the only provable boundary: records above it may still be
/// unread by the lagging consumer, records at or below it are consumed by all.
fn boundary(input: &RetentionInput, snapshot_offset: u64) -> (u64, String) {
    let consumer_min = input
        .consumers
        .iter()
        .map(|(_, offset)| *offset)
        .min()
        .unwrap_or(0);
    let blockers: Vec<&str> = input
        .consumers
        .iter()
        .filter(|(_, offset)| *offset == consumer_min)
        .map(|(name, _)| name.as_str())
        .collect();
    match consumer_min.cmp(&snapshot_offset) {
        Ordering::Less => (
            consumer_min,
            format!("consumer {} at offset {consumer_min}", blockers.join(", ")),
        ),
        Ordering::Greater => (
            snapshot_offset,
            format!("verified snapshot at offset {snapshot_offset}"),
        ),
        Ordering::Equal => (
            consumer_min,
            format!(
                "consumer {} and the verified snapshot both at offset {consumer_min}",
                blockers.join(", ")
            ),
        ),
    }
}

/// Evaluate the retention rule for one partition.
///
/// A consumer at offset `c` has consumed records `<= c`, so the boundary is
/// `min(oldest consumer committed, snapshot_offset)`; records up to it are
/// both consumed and covered by the recovery baseline. The safety margin is
/// then subtracted with saturating arithmetic, and the result may never exceed
/// `head`.
///
/// Returns [`RetentionDecision::Blocked`] when there is no consumer, no
/// snapshot, a boundary at offset 0 (nothing past the start is proven), a head
/// behind the boundary (the observations disagree), or a margin that consumes
/// the whole window.
pub fn evaluate(input: &RetentionInput) -> RetentionDecision {
    if input.consumers.is_empty() {
        return RetentionDecision::Blocked {
            reason: "no committed consumer: every consumer offset is unknown, so no record is \
                     proven consumed"
                .to_string(),
            safe_through: None,
        };
    }
    let Some(snapshot_offset) = input.snapshot_offset else {
        return RetentionDecision::Blocked {
            reason: "no verified snapshot: the partition has no recovery baseline for the log"
                .to_string(),
            safe_through: None,
        };
    };
    let (boundary, source) = boundary(input, snapshot_offset);
    if boundary == 0 {
        return RetentionDecision::Blocked {
            reason: format!(
                "boundary is offset 0 ({source}): no record past the start of the log is both \
                 consumed and covered by a verified snapshot"
            ),
            safe_through: None,
        };
    }
    // A consumer or snapshot ahead of the head means the reads disagree (for
    // example the log was read before the commits): re-read instead of
    // trusting arithmetic on inconsistent observations.
    if input.head < boundary {
        return RetentionDecision::Blocked {
            reason: format!(
                "log head {} is behind the boundary {boundary} ({source}): the observations \
                 cannot be proven against this log, re-read them",
                input.head
            ),
            safe_through: None,
        };
    }
    let safe_through = boundary.saturating_sub(input.safety_margin_records);
    if safe_through == 0 {
        return RetentionDecision::Blocked {
            reason: format!(
                "safety margin consumes the window: boundary {boundary} ({source}) minus {} \
                 record(s) of safety margin leaves no deletable range",
                input.safety_margin_records
            ),
            safe_through: None,
        };
    }
    // `safe_through <= boundary <= head`, so the clamp is defensive only.
    RetentionDecision::Eligible {
        safe_through: safe_through.min(input.head),
    }
}

/// One-line explanation of the rule's verdict, including the boundary and the
/// consumer or snapshot that set it.
///
/// [`RetentionDecision::Eligible`] has no reason field, so callers print this
/// instead of reconstructing the arithmetic themselves.
pub fn explain(input: &RetentionInput) -> String {
    let decision = evaluate(input);
    match decision {
        RetentionDecision::Blocked { reason, .. } => reason,
        RetentionDecision::Eligible { safe_through } => {
            let snapshot_offset = input.snapshot_offset.unwrap_or(0);
            let (boundary, source) = boundary(input, snapshot_offset);
            format!(
                "eligible: delete records through offset {safe_through} (inclusive); boundary \
                 {boundary} from {source}, head {}, safety margin {} record(s)",
                input.head, input.safety_margin_records
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consumer(name: &str, offset: u64) -> (String, u64) {
        (name.to_string(), offset)
    }

    fn input(
        head: u64,
        consumers: Vec<(String, u64)>,
        snapshot_offset: Option<u64>,
        safety_margin_records: u64,
    ) -> RetentionInput {
        RetentionInput {
            head,
            consumers,
            snapshot_offset,
            safety_margin_records,
        }
    }

    fn blocked_reason(decision: RetentionDecision) -> String {
        match decision {
            RetentionDecision::Blocked { reason, .. } => reason,
            other => panic!("expected Blocked, got {other:?}"),
        }
    }

    #[test]
    fn no_verified_snapshot_blocks() {
        let input = input(100, vec![consumer("whippet-engine-p1", 100)], None, 0);
        let reason = blocked_reason(evaluate(&input));
        assert!(reason.contains("no verified snapshot"), "{reason}");
        assert_eq!(evaluate(&input).safe_through(), None);
    }

    #[test]
    fn no_committed_consumer_blocks() {
        let input = input(100, vec![], Some(50), 0);
        let reason = blocked_reason(evaluate(&input));
        assert!(reason.contains("no committed consumer"), "{reason}");
    }

    #[test]
    fn the_oldest_consumer_sets_the_boundary() {
        // The engine is the lagging consumer; the other consumer and the
        // snapshot are both further ahead, so only the engine's commit is
        // provable.
        let input = input(
            120,
            vec![
                consumer("whippet-engine-p1", 80),
                consumer("whippet-replay-p1", 110),
            ],
            Some(95),
            0,
        );
        assert_eq!(
            evaluate(&input),
            RetentionDecision::Eligible { safe_through: 80 }
        );
        let explanation = explain(&input);
        assert!(explanation.contains("boundary 80"), "{explanation}");
        assert!(
            explanation.contains("whippet-engine-p1"),
            "the explanation must name the blocking consumer: {explanation}"
        );
    }

    #[test]
    fn a_snapshot_behind_the_consumers_sets_the_boundary() {
        let input = input(
            120,
            vec![
                consumer("whippet-engine-p1", 100),
                consumer("whippet-replay-p1", 110),
            ],
            Some(90),
            0,
        );
        assert_eq!(
            evaluate(&input),
            RetentionDecision::Eligible { safe_through: 90 }
        );
        let explanation = explain(&input);
        assert!(
            explanation.contains("verified snapshot"),
            "the snapshot must be named as the boundary source: {explanation}"
        );
    }

    #[test]
    fn safety_margin_retains_the_newest_records() {
        let input = input(200, vec![consumer("whippet-engine-p1", 100)], Some(120), 25);
        assert_eq!(
            evaluate(&input),
            RetentionDecision::Eligible { safe_through: 75 }
        );
        // The margin subtracts from the boundary, never from the head.
        assert!(evaluate(&input).safe_through().unwrap() <= 100);
    }

    #[test]
    fn a_safety_margin_covering_the_window_blocks() {
        for margin in [100, 101, u64::MAX] {
            let input = input(
                200,
                vec![consumer("whippet-engine-p1", 100)],
                Some(100),
                margin,
            );
            let reason = blocked_reason(evaluate(&input));
            assert!(
                reason.contains("safety margin consumes the window"),
                "margin {margin}: {reason}"
            );
            assert!(reason.contains("boundary 100"), "margin {margin}: {reason}");
        }
    }

    #[test]
    fn a_boundary_equal_to_the_head_is_eligible() {
        let input = input(
            100,
            vec![
                consumer("whippet-engine-p1", 100),
                consumer("whippet-replay-p1", 100),
            ],
            Some(100),
            0,
        );
        assert_eq!(
            evaluate(&input),
            RetentionDecision::Eligible { safe_through: 100 }
        );
    }

    #[test]
    fn an_empty_log_blocks() {
        // head 0 with committed offsets at 0: nothing past the start of the log
        // is proven, so the planner retains rather than deleting offset 0.
        let at_start = input(0, vec![consumer("whippet-engine-p1", 0)], Some(0), 0);
        let reason = blocked_reason(evaluate(&at_start));
        assert!(reason.contains("boundary is offset 0"), "{reason}");

        // Nothing has ever committed and no snapshot exists on a fresh stack.
        let empty = input(0, vec![], None, 10_000);
        assert!(!evaluate(&empty).is_eligible());
    }

    #[test]
    fn a_boundary_ahead_of_the_head_blocks() {
        // Offsets read at different times disagree. Trusting the min would
        // delete records this log does not even hold; retain instead.
        let input = input(50, vec![consumer("whippet-engine-p1", 100)], Some(100), 0);
        let reason = blocked_reason(evaluate(&input));
        assert!(
            reason.contains("behind the boundary"),
            "unexpected reason: {reason}"
        );
        assert!(reason.contains("head 50"), "{reason}");
    }

    #[test]
    fn the_decision_never_exceeds_the_head() {
        for head in [1, 10, 99, 100] {
            let input = input(
                head,
                vec![
                    consumer("whippet-engine-p1", 100),
                    consumer("whippet-replay-p1", 120),
                ],
                Some(100),
                0,
            );
            if let RetentionDecision::Eligible { safe_through } = evaluate(&input) {
                assert!(
                    safe_through <= head,
                    "safe_through {safe_through} exceeded head {head}"
                );
            }
        }
    }

    #[test]
    fn the_reason_names_the_snapshot_when_it_sets_the_boundary() {
        let input = input(100, vec![consumer("whippet-engine-p1", 90)], Some(40), 100);
        let reason = blocked_reason(evaluate(&input));
        assert!(reason.contains("boundary 40"), "{reason}");
        assert!(reason.contains("verified snapshot"), "{reason}");
        assert!(reason.contains("safety margin"), "{reason}");
    }

    #[test]
    fn every_consumer_at_the_boundary_keeps_the_snapshot_out_of_the_reason_choice() {
        // Both consumers tie the boundary, so the explanation names both.
        let input = input(
            100,
            vec![
                consumer("whippet-engine-p1", 60),
                consumer("whippet-replay-p1", 60),
            ],
            Some(80),
            0,
        );
        let explanation = explain(&input);
        assert!(explanation.contains("whippet-engine-p1"), "{explanation}");
        assert!(explanation.contains("whippet-replay-p1"), "{explanation}");
    }
}
