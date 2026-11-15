// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Educational surveillance indicators over the orders and executions logs.
//!
//! `whippet surveillance` scans one partition's `orders` and `executions`
//! topics and prints per-participant flow counters plus a small set of pattern
//! indicators. The indicators are **educational, not regulatory findings**:
//! nothing here is validated, calibrated, or reviewed, and a flag means "look
//! at this log with `whippet order-trace`", never "this participant did
//! something wrong".
//!
//! This module is the pure half: it reads only its arguments — no wall clock,
//! no network, no journal, no logging — so the same decoded logs always
//! produce the same report. The journal reads live in [`crate::cli`] behind
//! the [`Journal`] seam, exactly as the audit and order-trace tools do.
//!
//! [`Journal`]: whippet_journal::Journal

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty};
use whippet_core::protocol::{Command, CommandKind, ExecKind, ExecutionReport, RejectReason};

/// Nanoseconds in one second. The window is configured in seconds and applied
/// in nanoseconds, matching the logs' `timestamp_ns` fields.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// Detection thresholds for [`analyze`].
///
/// The defaults are conservative teaching values, not calibrated limits: one
/// hour of log, at least twenty orders before a cancel ratio is judged, a
/// 70% cancel ratio, and a 100,000.00-unit (in cents) notional for the
/// large-cancelled-order check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurveillanceConfig {
    /// Length of the analysis window in seconds, applied backwards from the
    /// newest event in the scanned logs.
    pub window_secs: u64,
    /// Minimum windowed `NewOrder` commands before [`Indicator::HighCancelRatio`]
    /// may fire for a participant.
    pub min_orders: u64,
    /// Cancel ratio in basis points (cancels per order) at and above which a
    /// participant with at least `min_orders` orders is flagged.
    pub cancel_ratio_bps: u32,
    /// Order notional (`price * qty`, in cents) at and above which a cancelled
    /// order counts as large.
    pub large_notional_cents: u64,
}

impl Default for SurveillanceConfig {
    fn default() -> Self {
        Self {
            window_secs: 3_600,
            min_orders: 20,
            cancel_ratio_bps: 7_000,
            large_notional_cents: 10_000_000,
        }
    }
}

impl SurveillanceConfig {
    /// Reject configurations that would make the indicators meaningless: a
    /// zero-length window, a zero order floor, a cancel ratio above its own
    /// 10_000 bps scale, or a zero large-order threshold (which would flag
    /// every cancelled order as large).
    pub fn validate(&self) -> Result<(), String> {
        if self.window_secs == 0 {
            return Err("window_secs must be at least 1; a zero window has no events".to_string());
        }
        if self.min_orders == 0 {
            return Err(
                "min_orders must be at least 1; a zero floor would judge participants with no orders"
                    .to_string(),
            );
        }
        if self.cancel_ratio_bps > 10_000 {
            return Err(format!(
                "cancel_ratio_bps must be at most 10_000 (100%); got {}",
                self.cancel_ratio_bps
            ));
        }
        if self.large_notional_cents == 0 {
            return Err(
                "large_notional_cents must be at least 1; a zero threshold flags every cancel"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// One pattern the analysis knows how to flag.
///
/// Every variant is a heuristic prompt for a human, never a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Indicator {
    /// Cancel ratio at or above `cancel_ratio_bps` with at least `min_orders`
    /// orders in the window.
    HighCancelRatio,
    /// At least one cancelled order at or above `large_notional_cents` with no
    /// fill on that order id.
    LargeCancelledWithoutFill,
    /// The participant had at least one self-trade-prevention event: the
    /// engine cancelled one of their resting orders (the maker side) because
    /// their own aggressor order would have crossed it.
    SelfTradePrevention,
    /// The participant had at least one `DuplicateOrderId` rejection: a
    /// command re-used an order id the engine still considered live.
    DuplicateOrderIdRetries,
}

impl Indicator {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HighCancelRatio => "high_cancel_ratio",
            Self::LargeCancelledWithoutFill => "large_cancelled_without_fill",
            Self::SelfTradePrevention => "self_trade_prevention",
            Self::DuplicateOrderIdRetries => "duplicate_order_id_retries",
        }
    }
}

/// One participant's windowed flow counters and indicators.
///
/// `cancels` counts cancel commands from the orders log; the engine-initiated
/// self-trade-prevention cancel of a resting order is reported separately in
/// `stp_cancels` and is never counted as a participant cancel.
/// `cancelled_notional_cents` is the original notional of every cancelled
/// order (the cancel command carries no quantity, so a partially filled
/// order contributes its full original notional).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParticipantReport {
    pub participant: ParticipantId,
    pub orders: u64,
    pub cancels: u64,
    pub fills: u64,
    pub rejected: u64,
    pub stp_cancels: u64,
    pub duplicate_rejects: u64,
    pub cancelled_notional_cents: u64,
    pub filled_notional_cents: u64,
    pub cancel_ratio_bps: u32,
    pub indicators: Vec<Indicator>,
}

/// The analysis result for one partition: the window bounds and every
/// participant that had at least one windowed order or attributable
/// execution, sorted by participant id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SurveillanceReport {
    pub partition: u32,
    /// Inclusive lower bound of the analysis window in nanoseconds:
    /// `newest_event_ts - window_secs * 1e9`.
    pub window_start_ns: u64,
    /// The window's reference "now": the newest order or execution timestamp
    /// in the scanned logs. An offline scan has no wall clock, so the log's
    /// own newest event is the anchor; the same logs always replay to the
    /// same window.
    pub window_end_ns: u64,
    /// Sorted by participant id.
    pub participants: Vec<ParticipantReport>,
}

/// What the analysis needs to know about one order id, taken from the orders
/// log. Cancel commands carry no price or quantity, so the cancel-side
/// counters look the original order up here.
struct OrderInfo {
    participant: ParticipantId,
    is_new_order: bool,
    /// `max(price, 0) * qty` in cents; negative prices contribute zero.
    notional: u128,
}

/// Per-participant accumulation, keyed by participant so iteration is sorted.
#[derive(Default)]
struct Accum {
    orders: u64,
    cancels: u64,
    fills: u64,
    rejected: u64,
    stp_cancels: u64,
    duplicate_rejects: u64,
    cancelled_notional: u128,
    filled_notional: u128,
    large_cancelled_without_fill: bool,
}

/// Order notional in cents, saturating at u64 when a corrupt log carries
/// absurd values.
fn notional(price: Price, qty: Qty) -> u128 {
    (price.0.max(0) as u128) * (u128::from(qty.0))
}

fn saturating_cents(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Build one partition's surveillance report from already-decoded logs.
///
/// `orders` is every decoded command on the partition with its orders-topic
/// offset; `executions` is every decoded execution report. Both are filtered
/// to the analysis window first: the window's reference "now" is the newest
/// order or execution timestamp in these logs, and an event is included when
/// `timestamp_ns >= newest - window_secs * 1e9`. There is deliberately no
/// wall-clock read: an offline replay has no "now" beyond the log itself, so
/// the same logs always produce the same report.
///
/// Attribution is order-id based. An execution report carries no participant,
/// so its participant is the participant of the order with the report's
/// `order_id` in the orders log; the engine's self-trade-prevention report
/// names the cancelled resting order, so the maker-side participant is
/// flagged, and a duplicate rejection names the rejected command's own order.
/// An execution whose order id is not in the orders log is unattributable and
/// is counted nowhere.
pub fn analyze(
    partition: u32,
    orders: &[(u64, Command)],
    executions: &[ExecutionReport],
    config: &SurveillanceConfig,
) -> SurveillanceReport {
    let newest = orders
        .iter()
        .map(|(_, command)| command.timestamp_ns)
        .chain(executions.iter().map(|report| report.timestamp_ns))
        .max()
        .unwrap_or(0);
    let window_start = newest.saturating_sub(config.window_secs.saturating_mul(NANOS_PER_SEC));

    // Order-id lookup over the full decoded log, not only the window: a
    // resting order accepted before the window can still be cancelled (and
    // therefore judged) inside it. First occurrence wins, so a malformed log
    // that re-uses an id keeps a deterministic answer.
    let mut order_by_id: BTreeMap<OrderId, OrderInfo> = BTreeMap::new();
    for (_, command) in orders {
        order_by_id.entry(command.order_id).or_insert(OrderInfo {
            participant: command.participant,
            is_new_order: command.kind == CommandKind::NewOrder,
            notional: notional(command.price, command.qty),
        });
    }

    let mut accounts: BTreeMap<ParticipantId, Accum> = BTreeMap::new();
    let mut filled_order_ids: BTreeSet<OrderId> = BTreeSet::new();

    // Windowed orders: the flow counters for commands the participant sent.
    for (_, command) in orders {
        if command.timestamp_ns < window_start {
            continue;
        }
        match command.kind {
            CommandKind::NewOrder => {
                accounts.entry(command.participant).or_default().orders += 1;
            }
            CommandKind::Cancel => {
                let acc = accounts.entry(command.participant).or_default();
                acc.cancels += 1;
                if let Some(info) = order_by_id.get(&command.order_id) {
                    acc.cancelled_notional += info.notional;
                }
            }
            CommandKind::Halt | CommandKind::Resume | CommandKind::SetSession => {}
        }
    }

    // Windowed executions: fills and rejections, attributed by order id.
    for report in executions {
        if report.timestamp_ns < window_start {
            continue;
        }
        if matches!(report.kind, ExecKind::Filled | ExecKind::PartiallyFilled) {
            filled_order_ids.insert(report.order_id);
            if report.maker_order_id != OrderId(0) {
                filled_order_ids.insert(report.maker_order_id);
            }
        }
        let Some(info) = order_by_id.get(&report.order_id) else {
            continue;
        };
        let acc = accounts.entry(info.participant).or_default();
        if report.reason == RejectReason::SelfTradePrevention {
            acc.stp_cancels += 1;
        }
        if report.reason == RejectReason::DuplicateOrderId {
            acc.duplicate_rejects += 1;
        }
        match report.kind {
            ExecKind::Filled | ExecKind::PartiallyFilled => {
                acc.fills += 1;
                acc.filled_notional += notional(report.price, report.qty);
            }
            ExecKind::Rejected => acc.rejected += 1,
            ExecKind::Accepted | ExecKind::Cancelled => {}
        }
    }

    // Large cancelled without fill: a cancelled NewOrder at or above the
    // notional threshold whose order id never received a fill report inside
    // the window.
    for (_, command) in orders {
        if command.kind != CommandKind::Cancel || command.timestamp_ns < window_start {
            continue;
        }
        let Some(info) = order_by_id.get(&command.order_id) else {
            continue;
        };
        if !info.is_new_order
            || info.notional < u128::from(config.large_notional_cents)
            || filled_order_ids.contains(&command.order_id)
        {
            continue;
        }
        accounts
            .entry(info.participant)
            .or_default()
            .large_cancelled_without_fill = true;
    }

    let participants = accounts
        .into_iter()
        .map(|(participant, acc)| {
            let cancel_ratio_bps =
                ((u128::from(acc.cancels) * 10_000) / u128::from(acc.orders.max(1))) as u32;
            let mut indicators = Vec::new();
            if acc.orders >= config.min_orders && cancel_ratio_bps >= config.cancel_ratio_bps {
                indicators.push(Indicator::HighCancelRatio);
            }
            if acc.large_cancelled_without_fill {
                indicators.push(Indicator::LargeCancelledWithoutFill);
            }
            if acc.stp_cancels >= 1 {
                indicators.push(Indicator::SelfTradePrevention);
            }
            if acc.duplicate_rejects >= 1 {
                indicators.push(Indicator::DuplicateOrderIdRetries);
            }
            ParticipantReport {
                participant,
                orders: acc.orders,
                cancels: acc.cancels,
                fills: acc.fills,
                rejected: acc.rejected,
                stp_cancels: acc.stp_cancels,
                duplicate_rejects: acc.duplicate_rejects,
                cancelled_notional_cents: saturating_cents(acc.cancelled_notional),
                filled_notional_cents: saturating_cents(acc.filled_notional),
                cancel_ratio_bps,
                indicators,
            }
        })
        .collect();

    SurveillanceReport {
        partition,
        window_start_ns: window_start,
        window_end_ns: newest,
        participants,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::ids::{Seq, Symbol};
    use whippet_core::protocol::{OrderType, Side};

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD").expect("static symbol")
    }

    fn new_order(id: u64, participant: u64, ts: u64, price: i64, qty: u64) -> (u64, Command) {
        (
            id,
            Command::new_order(
                symbol(),
                Side::Buy,
                OrderType::Limit,
                Price(price),
                Qty(qty),
                OrderId(u128::from(id)),
                ParticipantId(participant),
                ts,
            ),
        )
    }

    fn cancel_cmd(id: u64, participant: u64, ts: u64) -> (u64, Command) {
        (
            id,
            Command::cancel(
                symbol(),
                OrderId(u128::from(id)),
                ParticipantId(participant),
                ts,
            ),
        )
    }

    fn exec(
        order_id: u64,
        kind: ExecKind,
        reason: RejectReason,
        ts: u64,
        price: i64,
        qty: u64,
    ) -> ExecutionReport {
        ExecutionReport {
            kind,
            symbol: symbol(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Price(price),
            qty: Qty(qty),
            remaining: Qty(0),
            order_id: OrderId(u128::from(order_id)),
            maker_order_id: OrderId(0),
            seq: Seq(ts),
            cause_offset: order_id,
            timestamp_ns: ts,
            reason,
            flags: 0,
        }
    }

    fn cfg(
        window_secs: u64,
        min_orders: u64,
        cancel_ratio_bps: u32,
        large_notional_cents: u64,
    ) -> SurveillanceConfig {
        SurveillanceConfig {
            window_secs,
            min_orders,
            cancel_ratio_bps,
            large_notional_cents,
        }
    }

    #[test]
    fn defaults_match_the_documented_thresholds_and_validate() {
        assert_eq!(
            SurveillanceConfig::default(),
            cfg(3_600, 20, 7_000, 10_000_000)
        );
        assert!(SurveillanceConfig::default().validate().is_ok());

        for (name, config) in [
            ("zero window", cfg(0, 20, 7_000, 10_000_000)),
            ("zero min orders", cfg(3_600, 0, 7_000, 10_000_000)),
            ("ratio over scale", cfg(3_600, 20, 10_001, 10_000_000)),
            ("zero notional", cfg(3_600, 20, 7_000, 0)),
        ] {
            let error = config
                .validate()
                .expect_err(&format!("{name} must be rejected"));
            assert!(!error.is_empty(), "{name}: empty error");
        }
        // The 10_000 bps boundary (a 100% cancel ratio) is valid.
        assert!(cfg(3_600, 20, 10_000, 10_000_000).validate().is_ok());
    }

    #[test]
    fn indicator_names_are_stable() {
        assert_eq!(Indicator::HighCancelRatio.as_str(), "high_cancel_ratio");
        assert_eq!(
            Indicator::LargeCancelledWithoutFill.as_str(),
            "large_cancelled_without_fill"
        );
        assert_eq!(
            Indicator::SelfTradePrevention.as_str(),
            "self_trade_prevention"
        );
        assert_eq!(
            Indicator::DuplicateOrderIdRetries.as_str(),
            "duplicate_order_id_retries"
        );
    }

    #[test]
    fn high_cancel_ratio_respects_both_thresholds() {
        // 10 orders, 7 cancels = 7_000 bps exactly.
        let mut orders = Vec::new();
        for id in 1..=10u64 {
            orders.push(new_order(id, 7, id, 100, 1));
        }
        for id in 1..=7u64 {
            orders.push(cancel_cmd(id, 7, 100 + id));
        }

        // At the default floor (20 orders) the ratio is never judged.
        let report = analyze(1, &orders, &[], &SurveillanceConfig::default());
        let p = &report.participants[0];
        assert_eq!(p.orders, 10);
        assert_eq!(p.cancels, 7);
        assert_eq!(p.cancel_ratio_bps, 7_000);
        assert!(p.indicators.is_empty(), "10 orders is under the floor");

        // Lower the floor to exactly 10: the boundary ratio flags.
        let report = analyze(1, &orders, &[], &cfg(3_600, 10, 7_000, 10_000_000));
        assert_eq!(
            report.participants[0].indicators,
            vec![Indicator::HighCancelRatio]
        );

        // One basis point below the threshold does not flag.
        let report = analyze(1, &orders, &[], &cfg(3_600, 10, 7_001, 10_000_000));
        assert!(report.participants[0].indicators.is_empty());
    }

    #[test]
    fn large_cancelled_without_fill_needs_large_unfilled_cancelled_new_orders() {
        // Order 1: 100_000 * 100 = 10_000_000 cents, cancelled, never filled.
        // Order 2: same notional but filled first.
        // Order 3: below the threshold, cancelled, never filled.
        // Order 4: an extra resting order keeps the cancel ratio under the
        // 10_000 bps threshold so only the large-cancel indicator can fire.
        let orders = vec![
            new_order(1, 7, 1, 100_000, 100),
            new_order(2, 7, 2, 100_000, 100),
            new_order(3, 7, 3, 999, 1),
            new_order(4, 7, 4, 100, 1),
            cancel_cmd(1, 7, 5),
            cancel_cmd(2, 7, 6),
            cancel_cmd(3, 7, 7),
        ];
        let executions = vec![exec(
            2,
            ExecKind::Filled,
            RejectReason::None,
            5,
            100_000,
            100,
        )];

        let report = analyze(1, &orders, &executions, &cfg(3_600, 1, 10_000, 10_000_000));
        let p = &report.participants[0];
        assert_eq!(p.orders, 4);
        assert_eq!(p.cancels, 3);
        assert_eq!(p.cancelled_notional_cents, 20_000_000 + 999);
        assert_eq!(p.indicators, vec![Indicator::LargeCancelledWithoutFill]);

        // One cent above the large order's notional flags nothing.
        let report = analyze(1, &orders, &executions, &cfg(3_600, 1, 10_000, 10_000_001));
        assert_eq!(report.participants[0].indicators, vec![]);

        // A threshold at exactly the large order's notional still flags it.
        let report = analyze(1, &orders, &executions, &cfg(3_600, 1, 10_000, 10_000_000));
        assert_eq!(
            report.participants[0].indicators,
            vec![Indicator::LargeCancelledWithoutFill]
        );
    }

    #[test]
    fn stp_and_duplicate_reports_attribute_to_the_order_participant() {
        // Participant 9 rests order 100; participant 4's aggressor order 200
        // would have crossed it, so the engine cancels order 100 with the
        // self-trade-prevention reason (the report names the resting order).
        // Participant 4 also retries order 300, which the engine rejects as a
        // duplicate order id.
        let orders = vec![
            new_order(100, 9, 1, 100, 1),
            new_order(200, 4, 2, 100, 1),
            new_order(300, 4, 3, 100, 1),
        ];
        let executions = vec![
            exec(
                100,
                ExecKind::Cancelled,
                RejectReason::SelfTradePrevention,
                4,
                100,
                1,
            ),
            exec(
                300,
                ExecKind::Rejected,
                RejectReason::DuplicateOrderId,
                5,
                100,
                0,
            ),
        ];

        let report = analyze(1, &orders, &executions, &cfg(3_600, 1, 10_000, 10_000_000));
        let mut by_participant = report.participants.iter().cloned();
        // Sorted by participant id: 4 then 9.
        let p4 = by_participant.next().expect("participant 4");
        assert_eq!(p4.participant, ParticipantId(4));
        assert_eq!(p4.orders, 2);
        assert_eq!(p4.rejected, 1);
        assert_eq!(p4.duplicate_rejects, 1);
        assert_eq!(
            p4.indicators,
            vec![Indicator::DuplicateOrderIdRetries],
            "the STP cancel belongs to the maker, not the aggressor"
        );
        let p9 = by_participant.next().expect("participant 9");
        assert_eq!(p9.participant, ParticipantId(9));
        assert_eq!(p9.orders, 1);
        assert_eq!(
            p9.cancels, 0,
            "engine-initiated STP cancels are not command cancels"
        );
        assert_eq!(p9.stp_cancels, 1);
        assert_eq!(
            p9.indicators,
            vec![Indicator::SelfTradePrevention],
            "the maker side of the self-trade is the flagged participant"
        );
    }

    #[test]
    fn window_excludes_events_older_than_the_cutoff_but_keeps_attribution() {
        // Newest event at ts = 350 * 1e9 (the cancel); a 100 s window starts
        // at 250 * 1e9. Order 1 sits at ts = 1 (excluded from the counts) but
        // is still the target of an in-window cancel, so its notional is
        // reported. The old fill on order 2 is excluded from the fill counts.
        let ns = |secs: u64| secs * NANOS_PER_SEC;
        let orders = vec![
            new_order(1, 7, 1, 100_000, 100),
            new_order(2, 7, ns(310), 100, 1),
            cancel_cmd(1, 7, ns(350)),
        ];
        let executions = vec![
            exec(2, ExecKind::Filled, RejectReason::None, ns(200), 100, 1),
            exec(2, ExecKind::Filled, RejectReason::None, ns(320), 100, 1),
        ];

        let report = analyze(1, &orders, &executions, &cfg(100, 2, 10_000, 10_000_000));
        assert_eq!(report.window_end_ns, ns(350));
        assert_eq!(report.window_start_ns, ns(250));
        let p = &report.participants[0];
        assert_eq!(p.orders, 1, "the old order at ts=1 is outside the window");
        assert_eq!(p.cancels, 1);
        assert_eq!(
            p.cancelled_notional_cents, 10_000_000,
            "the cancelled order's original notional survives the order's exclusion"
        );
        assert_eq!(p.fills, 1, "the fill at ts=200 is outside the window");
        assert_eq!(p.filled_notional_cents, 100);

        // The cancel of order 1 is large and order 1 has no fill, so the
        // large-cancelled indicator still fires even though the order itself
        // predates the window.
        assert_eq!(p.indicators, vec![Indicator::LargeCancelledWithoutFill]);
    }

    #[test]
    fn participants_are_sorted_by_id() {
        let orders = vec![
            new_order(1, 30, 1, 1, 1),
            new_order(2, 5, 2, 1, 1),
            new_order(3, 30, 3, 1, 1),
        ];
        let report = analyze(0, &orders, &[], &cfg(3_600, 1, 10_000, 10_000_000));
        let ids: Vec<u64> = report
            .participants
            .iter()
            .map(|p| p.participant.0)
            .collect();
        assert_eq!(ids, vec![5, 30]);
    }

    #[test]
    fn empty_logs_produce_an_empty_windowed_report() {
        let report = analyze(3, &[], &[], &SurveillanceConfig::default());
        assert_eq!(report.partition, 3);
        assert_eq!(report.window_start_ns, 0);
        assert_eq!(report.window_end_ns, 0);
        assert!(report.participants.is_empty());
    }

    #[test]
    fn json_shape_carries_the_report_contract() {
        let orders = vec![new_order(1, 7, 1, 100_000, 100), cancel_cmd(1, 7, 2)];
        let report = analyze(1, &orders, &[], &cfg(3_600, 1, 10_000, 10_000_000));
        let json = serde_json::to_value(&report).expect("report serializes");
        assert_eq!(json["partition"], 1);
        assert_eq!(json["window_start_ns"], 0);
        assert_eq!(json["window_end_ns"], 2);
        assert_eq!(json["participants"][0]["participant"], 7);
        assert_eq!(json["participants"][0]["cancel_ratio_bps"], 10_000);
        // Indicators serialize in a stable order.
        assert_eq!(
            json["participants"][0]["indicators"]
                .as_array()
                .expect("indicators array"),
            &["high_cancel_ratio", "large_cancelled_without_fill"]
        );
    }
}
