// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic matching engine.
//!
//! The engine is a pure state machine over the input log. It reads no clock,
//! performs no I/O, and never allocates inside the matching loop (output
//! vectors are reused by the caller). Every output carries the input offset
//! that caused it, so replay can be verified against a state hash.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use whippet_core::config::{ExchangeConfig, InstrumentConfig};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{
    Command, CommandKind, ExecKind, ExecutionReport, FLAG_MAKER, FLAG_RESTED, FLAG_TAKER, MdEvent,
    MdKind, MdOrder, MdSnapshot, OrderType, RejectReason, SessionState, Side, TradingStatus,
};

use crate::book::{Book, BookError, BookSnapshot, RestingOrder, hash_book_snapshot};
use crate::dedup::{DedupEntry, DedupStatus, DedupTable, hash_dedup_entries};
use crate::risk::{RiskLimits, RiskTable};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("book error: {0}")]
    Book(#[from] BookError),
    #[error("unknown symbol {0} in shard {1}")]
    UnknownSymbol(Symbol, u32),
    #[error("snapshot error: {0}")]
    Snapshot(String),
    #[error("state hash mismatch after restore: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

/// Hard upper bound for a single order quantity. The gateway enforces the same
/// bound; the engine also enforces it because the journal is a trust boundary.
pub const MAX_ORDER_QTY: u64 = 1_000_000_000;

#[derive(Clone, Default)]
pub struct EngineOutput {
    pub executions: Vec<ExecutionReport>,
    pub md: Vec<MdEvent>,
    pub snapshots: Vec<MdSnapshot>,
    /// True when this command landed on the engine-state persistence interval.
    /// The node uses it to save `snapshot_state`; the engine does not emit a
    /// full market-data snapshot for this cadence. Market-data snapshots are
    /// published separately on `snapshots`.
    pub state_snapshot_due: bool,
}

impl EngineOutput {
    pub fn with_capacity(executions: usize, md: usize) -> Self {
        Self {
            executions: Vec::with_capacity(executions),
            md: Vec::with_capacity(md),
            snapshots: Vec::new(),
            state_snapshot_due: false,
        }
    }

    pub fn clear(&mut self) {
        self.executions.clear();
        self.md.clear();
        self.snapshots.clear();
        self.state_snapshot_due = false;
    }
}

/// Per-order scratch state used by [`EngineOutput::check_invariants`].
#[derive(Default)]
struct OutputOrderState {
    accepted_qty: Option<Qty>,
    /// The admission role carried by the `Accepted` report: exactly one of
    /// [`FLAG_TAKER`] or [`FLAG_RESTED`].
    accepted_role: Option<u16>,
    accepted_side: Option<Side>,
    filled_qty: u64,
    last_fill: Option<ExecKind>,
    add_qty: Option<Qty>,
    cancel_qty: Option<Qty>,
    terminal: Option<ExecKind>,
}

/// Per-order scratch state for the market-data walk.
#[derive(Default)]
struct MdOrderState {
    last_qty: Option<Qty>,
    added: bool,
    reduced: bool,
    removed: bool,
}

/// Key that pairs a fill report with the `Trade` event it must emit.
///
/// The engine emits one `Trade` per fill, named by the taker's order id and
/// side. Including both in the key makes the pairing exact instead of merely
/// numeric: a trade whose id or side drifted cannot hide behind an equal
/// price/quantity.
type FillTradeKey = (u64, Price, Qty, Side, OrderId);

/// Check that `seq` values are nonzero and strictly increasing.
fn check_monotonic_seq<I>(what: &str, seqs: I) -> Result<(), String>
where
    I: Iterator<Item = Seq>,
{
    let mut previous: Option<Seq> = None;
    for (index, seq) in seqs.enumerate() {
        if seq.0 == 0 {
            return Err(format!(
                "{what} seq 0 at index {index}: sequence numbers start at 1"
            ));
        }
        if let Some(previous) = previous
            && seq.0 <= previous.0
        {
            return Err(format!(
                "{what} seq {seq} at index {index} does not strictly increase past {previous}"
            ));
        }
        previous = Some(seq);
    }
    Ok(())
}

impl EngineOutput {
    /// Check the cross-checks between one command's execution reports, its
    /// market-data events, and the book mutation they encode.
    ///
    /// This is a diagnostic for tests and replay verification: it allocates,
    /// walks the whole output once, and is **never** called from `process`.
    /// Its cost is O(`executions` + `md` + `snapshots`). The companion
    /// [`SymbolEngine::check_output_invariants`] adds the provenance checks that
    /// need the command and the engine clock.
    ///
    /// An output is the complete result of one `process` call. The invariants
    /// are:
    ///
    /// - `executions` and `md` each carry their own sequence space; within each
    ///   vector `seq` is nonzero and strictly increasing.
    /// - Every execution report, market-data event, and snapshot names the same
    ///   `cause_offset` (one output is one command). Every execution report
    ///   carries the same `timestamp_ns`. Every market-data event and snapshot
    ///   carries the same timestamp.
    /// - An order id has at most one `Accepted`, it precedes every fill and
    ///   cancel for that id, and a terminal kind (`Filled`, `Cancelled`,
    ///   `Rejected`) is the last report for that id in the output.
    /// - A fill (`PartiallyFilled`, `Filled`) has `qty > 0`, `price > 0`, its
    ///   `side` is the taker's side, it carries [`FLAG_TAKER`], and it names a
    ///   nonzero `maker_order_id`. Its `remaining` is positive exactly for
    ///   `PartiallyFilled`, zero for `Filled`, and `remaining + qty` never
    ///   exceeds the `Accepted` quantity. Fills belong to an order accepted in
    ///   this same output: the maker leg is represented by `maker_order_id`,
    ///   never by a second fill report (see the exception notes below).
    /// - Every fill has exactly one `MdKind::Trade` with the same cause offset,
    ///   price, quantity, side, and taker order id, the counts are equal, and
    ///   no `Trade` exists without a fill.
    /// - Every fill has exactly one `MdKind::Remove` or `MdKind::Reduce` for
    ///   its `maker_order_id`.
    /// - `MdKind::Add` has `qty > 0`, a nonzero order id, and follows an
    ///   `Accepted` for that id. It is not preceded by a `Remove`/`Reduce` for
    ///   that id, and an id is added at most once.
    /// - `MdKind::Reduce` carries the order's new remaining quantity (`qty >
    ///   0`), never grows, and never follows a `Remove` for that id.
    ///   `MdKind::Remove` carries `qty == 0` and a nonzero order id, and an id
    ///   is removed at most once. A `Remove`/`Reduce` for an order accepted in
    ///   an earlier command has no preceding `Add` in this output; its trigger
    ///   (a fill naming it as maker, or a `Cancelled` report) and its
    ///   side/price are still checked. A self-trade-prevention maker cancel and
    ///   a client-cancel `Cancelled` report each have exactly one `Remove` event
    ///   for their order id.
    /// - `MdKind::Status` carries no order id, price, or quantity.
    /// - A `Rejected` report is the only execution report in its output and the
    ///   output has no market-data deltas (`md`): creation-time rejections happen
    ///   before any book mutation. A cadence snapshot on `snapshots` may still be
    ///   attached to the same command.
    /// - At most one role flag (`FLAG_TAKER`, `FLAG_RESTED`, `FLAG_MAKER`) is
    ///   set on an execution report. `Accepted` carries exactly one of
    ///   `FLAG_TAKER`/`FLAG_RESTED`; market/IOC/FOK carry `FLAG_TAKER` and
    ///   post-only carries `FLAG_RESTED`. A fill carries `FLAG_TAKER`. A
    ///   self-trade-prevention maker cancel carries `FLAG_MAKER` and reason
    ///   `SelfTradePrevention`. A successful client cancel carries no role flag.
    /// - The accepted quantity is conserved: fills + the resting `Add` (or the
    ///   terminal `Cancelled` remainder) equal the accepted quantity.
    ///
    /// Legitimate exceptions, documented rather than papered over:
    ///
    /// - **The maker fill is not a separate report.** The engine reports one
    ///   fill per trade, stamped with the taker's id and side, and names the
    ///   resting order in `maker_order_id`. The maker's book-side mutation is
    ///   the paired `Remove`/`Reduce` event. A checker therefore requires every
    ///   fill's `order_id` to have an `Accepted` in this output, while the maker
    ///   id has no fill report of its own.
    /// - **`FLAG_RESTED` is an admission-time role, not a promise to rest.** A
    ///   limit order that crosses is accepted as `FLAG_TAKER` even if self-trade
    ///   prevention clears the opposite side and the order later rests, and a
    ///   partially filled order keeps `FLAG_TAKER` on its resting remainder.
    ///   The checks therefore require "fills imply the taker role", not the
    ///   converse.
    /// - **A maker `Remove` carries quantity 0.** The quantity removed was
    ///   already carried by the preceding `Trade` (and, for a partial maker
    ///   fill, the `Reduce` carries the new remaining quantity). Consumers must
    ///   not expect a quantity on `Remove`.
    /// - **The two timestamp sources differ by design.** Execution reports carry
    ///   the command's own `timestamp_ns`; market data carries the venue clock
    ///   (`SymbolEngine::last_ts_ns`, a running maximum). On a non-monotone
    ///   input log the market-data timestamp can legitimately exceed the
    ///   command's. [`SymbolEngine::check_output_invariants`] checks each
    ///   against its own source.
    /// - **A `Cancelled` from a client cancel has no role flag.** It describes
    ///   neither a taker nor a maker; the removed order's side and price are
    ///   still reported so it agrees with the `Remove` event.
    pub fn check_invariants(&self) -> Result<(), String> {
        check_monotonic_seq("execution", self.executions.iter().map(|exec| exec.seq))?;
        check_monotonic_seq("market-data", self.md.iter().map(|event| event.seq))?;

        // One output is one command: every event names the same input offset.
        let expected_offset = self
            .executions
            .first()
            .map(|exec| exec.cause_offset)
            .or_else(|| self.md.first().map(|event| event.cause_offset))
            .or_else(|| self.snapshots.first().map(|snapshot| snapshot.cause_offset));
        if let Some(expected) = expected_offset {
            for exec in &self.executions {
                if exec.cause_offset != expected {
                    return Err(format!(
                        "execution seq {} ({} {}) has cause_offset {} but the output is for offset {expected}: one output is one command",
                        exec.seq,
                        exec.kind.as_str(),
                        exec.order_id,
                        exec.cause_offset
                    ));
                }
            }
            for event in &self.md {
                if event.cause_offset != expected {
                    return Err(format!(
                        "market-data seq {} ({:?}) has cause_offset {} but the output is for offset {expected}",
                        event.seq, event.kind, event.cause_offset
                    ));
                }
            }
            for snapshot in &self.snapshots {
                if snapshot.cause_offset != expected {
                    return Err(format!(
                        "snapshot has cause_offset {} but the output is for offset {expected}",
                        snapshot.cause_offset
                    ));
                }
            }
        }

        // Execution reports carry the command's timestamp.
        if let Some(expected) = self.executions.first().map(|exec| exec.timestamp_ns) {
            for exec in &self.executions {
                if exec.timestamp_ns != expected {
                    return Err(format!(
                        "execution seq {} has timestamp {} but the output's reports carry {expected}: every report carries the command's timestamp",
                        exec.seq, exec.timestamp_ns
                    ));
                }
            }
        }

        // Market data and snapshots carry the venue clock.
        if let Some(expected) = self
            .md
            .first()
            .map(|event| event.timestamp_ns)
            .or_else(|| self.snapshots.first().map(|snapshot| snapshot.timestamp_ns))
        {
            for event in &self.md {
                if event.timestamp_ns != expected {
                    return Err(format!(
                        "market-data seq {} has timestamp {} but the output's events carry {expected}: market data carries the venue clock",
                        event.seq, event.timestamp_ns
                    ));
                }
            }
            for snapshot in &self.snapshots {
                if snapshot.timestamp_ns != expected {
                    return Err(format!(
                        "snapshot has timestamp {} but the output's market data carries {expected}",
                        snapshot.timestamp_ns
                    ));
                }
            }
        }

        let mut summaries: FxHashMap<OrderId, OutputOrderState> = FxHashMap::default();
        let mut fill_reports: Vec<&ExecutionReport> = Vec::new();
        let mut fills_by_maker: FxHashMap<OrderId, &ExecutionReport> = FxHashMap::default();
        let mut cancels_by_order: FxHashMap<OrderId, &ExecutionReport> = FxHashMap::default();

        for (index, exec) in self.executions.iter().enumerate() {
            let role = exec.flags & (FLAG_TAKER | FLAG_RESTED | FLAG_MAKER);
            if role.count_ones() > 1 {
                return Err(format!(
                    "execution seq {} ({}) sets more than one role flag (flags {:#06x}); FLAG_TAKER, FLAG_RESTED and FLAG_MAKER are exclusive",
                    exec.seq,
                    exec.kind.as_str(),
                    exec.flags
                ));
            }

            let summary = summaries.entry(exec.order_id).or_default();
            match exec.kind {
                ExecKind::Accepted => {
                    if summary.accepted_qty.is_some() {
                        return Err(format!(
                            "order {} is Accepted twice in one output; an order id is admitted at most once",
                            exec.order_id
                        ));
                    }
                    if summary.terminal.is_some() {
                        return Err(format!(
                            "order {} has an Accepted report after a terminal report; Accepted must come first",
                            exec.order_id
                        ));
                    }
                    if exec.flags != FLAG_TAKER && exec.flags != FLAG_RESTED {
                        return Err(format!(
                            "Accepted order {} carries flags {:#06x}; an acceptance must carry exactly one of FLAG_TAKER or FLAG_RESTED",
                            exec.order_id, exec.flags
                        ));
                    }
                    match exec.order_type {
                        OrderType::Market | OrderType::Ioc | OrderType::Fok => {
                            if exec.flags != FLAG_TAKER {
                                return Err(format!(
                                    "Accepted {} order {} carries flags {:#06x}; a {:?} order is a taker",
                                    exec.order_type.as_str(),
                                    exec.order_id,
                                    exec.flags,
                                    exec.order_type
                                ));
                            }
                        }
                        OrderType::PostOnly => {
                            if exec.flags != FLAG_RESTED {
                                return Err(format!(
                                    "Accepted post-only order {} carries flags {:#06x}; a post-only order that was admitted is resting",
                                    exec.order_id, exec.flags
                                ));
                            }
                        }
                        OrderType::Limit => {}
                    }
                    if exec.qty.is_zero() {
                        return Err(format!(
                            "Accepted order {} has zero quantity",
                            exec.order_id
                        ));
                    }
                    if exec.remaining != exec.qty {
                        return Err(format!(
                            "Accepted order {} reports remaining {} but quantity {}; nothing has traded yet",
                            exec.order_id, exec.remaining, exec.qty
                        ));
                    }
                    if exec.maker_order_id != OrderId(0) {
                        return Err(format!(
                            "Accepted order {} names maker {}; an acceptance has no maker leg",
                            exec.order_id, exec.maker_order_id
                        ));
                    }
                    if exec.reason != RejectReason::None {
                        return Err(format!(
                            "Accepted order {} carries reject reason {:?}",
                            exec.order_id, exec.reason
                        ));
                    }
                    summary.accepted_qty = Some(exec.qty);
                    summary.accepted_role = Some(exec.flags);
                    summary.accepted_side = Some(exec.side);
                }
                ExecKind::PartiallyFilled | ExecKind::Filled => {
                    let Some(accepted_qty) = summary.accepted_qty else {
                        return Err(format!(
                            "execution {index} ({} {}) is a fill but the output has no Accepted report for that order; a fill is always the taker's and the maker leg is carried by maker_order_id",
                            exec.kind.as_str(),
                            exec.order_id
                        ));
                    };
                    if summary.terminal.is_some() {
                        return Err(format!(
                            "order {} has a fill after a terminal report; terminal kinds must come last for an id",
                            exec.order_id
                        ));
                    }
                    if exec.flags != FLAG_TAKER {
                        return Err(format!(
                            "fill for order {} carries flags {:#06x}; a fill must carry FLAG_TAKER",
                            exec.order_id, exec.flags
                        ));
                    }
                    if summary.accepted_role != Some(FLAG_TAKER) {
                        return Err(format!(
                            "order {} was accepted with role flags {:#06x} but filled; a resting order cannot take liquidity",
                            exec.order_id,
                            summary.accepted_role.unwrap_or(0)
                        ));
                    }
                    if summary.accepted_side != Some(exec.side) {
                        return Err(format!(
                            "fill for order {} reports side {} but the order was accepted as {}; a fill is on the taker's side",
                            exec.order_id,
                            exec.side,
                            summary
                                .accepted_side
                                .map_or("unknown".to_string(), |side| side.to_string())
                        ));
                    }
                    if exec.qty.is_zero() {
                        return Err(format!(
                            "fill for order {} has zero quantity",
                            exec.order_id
                        ));
                    }
                    if exec.price.0 <= 0 {
                        return Err(format!(
                            "fill for order {} has non-positive price {}",
                            exec.order_id, exec.price
                        ));
                    }
                    match exec.kind {
                        ExecKind::PartiallyFilled if exec.remaining.is_zero() => {
                            return Err(format!(
                                "order {} is PartiallyFilled with zero remaining; a zero remainder is Filled",
                                exec.order_id
                            ));
                        }
                        ExecKind::Filled if !exec.remaining.is_zero() => {
                            return Err(format!(
                                "order {} is Filled with remaining {}; a Filled report must leave nothing",
                                exec.order_id, exec.remaining
                            ));
                        }
                        _ => {}
                    }
                    if exec.maker_order_id == OrderId(0) {
                        return Err(format!(
                            "fill for order {} names maker_order_id 0; every fill consumes a nonzero resting order",
                            exec.order_id
                        ));
                    }
                    if exec.remaining.0.saturating_add(exec.qty.0) > accepted_qty.0 {
                        return Err(format!(
                            "fill for order {} reports remaining {} + qty {} above the accepted quantity {}",
                            exec.order_id, exec.remaining, exec.qty, accepted_qty
                        ));
                    }
                    summary.filled_qty = summary.filled_qty.saturating_add(exec.qty.0);
                    summary.last_fill = Some(exec.kind);
                    if exec.kind == ExecKind::Filled {
                        summary.terminal = Some(ExecKind::Filled);
                    }
                    fill_reports.push(exec);
                    fills_by_maker.insert(exec.maker_order_id, exec);
                }
                ExecKind::Cancelled => {
                    if summary.terminal.is_some() {
                        return Err(format!(
                            "order {} has a second terminal report; the first terminal kind must come last",
                            exec.order_id
                        ));
                    }
                    if !exec.remaining.is_zero() {
                        return Err(format!(
                            "Cancelled order {} reports remaining {}; a cancellation leaves nothing",
                            exec.order_id, exec.remaining
                        ));
                    }
                    if exec.qty.is_zero() {
                        return Err(format!(
                            "Cancelled order {} has zero quantity; a cancellation reports the cancelled remainder",
                            exec.order_id
                        ));
                    }
                    if exec.maker_order_id != OrderId(0) {
                        return Err(format!(
                            "Cancelled order {} names maker {}; a cancellation has no maker leg",
                            exec.order_id, exec.maker_order_id
                        ));
                    }
                    let is_maker = exec.flags & FLAG_MAKER != 0;
                    if is_maker {
                        if exec.reason != RejectReason::SelfTradePrevention {
                            return Err(format!(
                                "maker cancel for order {} carries reason {:?}; a maker-only cancel is self-trade prevention",
                                exec.order_id, exec.reason
                            ));
                        }
                        if exec.flags != FLAG_MAKER {
                            return Err(format!(
                                "maker cancel for order {} carries flags {:#06x}; a maker-only cancel carries exactly FLAG_MAKER",
                                exec.order_id, exec.flags
                            ));
                        }
                        if exec.price.0 <= 0 {
                            return Err(format!(
                                "maker cancel for order {} has non-positive price {}",
                                exec.order_id, exec.price
                            ));
                        }
                    } else if exec.reason == RejectReason::SelfTradePrevention {
                        return Err(format!(
                            "order {} carries reason SelfTradePrevention without FLAG_MAKER",
                            exec.order_id
                        ));
                    }
                    if let Some(role) = summary.accepted_role {
                        let cancelled_role = exec.flags & (FLAG_TAKER | FLAG_RESTED);
                        if cancelled_role != role {
                            return Err(format!(
                                "order {} was accepted with role flags {role:#06x} but cancelled with {cancelled_role:#06x}; the terminal report must agree with its acceptance",
                                exec.order_id
                            ));
                        }
                    }
                    summary.terminal = Some(ExecKind::Cancelled);
                    summary.cancel_qty = Some(exec.qty);
                    cancels_by_order.insert(exec.order_id, exec);
                }
                ExecKind::Rejected => {
                    if index != 0 || self.executions.len() != 1 || !self.md.is_empty() {
                        return Err(format!(
                            "output mixes a Rejected report for order {} with other reports or market data; a rejection is the only result of its command",
                            exec.order_id
                        ));
                    }
                    if !exec.qty.is_zero() {
                        return Err(format!(
                            "Rejected order {} has quantity {}; a rejection carries no quantity",
                            exec.order_id, exec.qty
                        ));
                    }
                    if !exec.remaining.is_zero() {
                        return Err(format!(
                            "Rejected order {} has remaining {}; a rejection leaves nothing",
                            exec.order_id, exec.remaining
                        ));
                    }
                    if exec.maker_order_id != OrderId(0) {
                        return Err(format!(
                            "Rejected order {} names maker {}; a rejection has no maker leg",
                            exec.order_id, exec.maker_order_id
                        ));
                    }
                    if exec.flags != 0 {
                        return Err(format!(
                            "Rejected order {} carries flags {:#06x}; a rejection has no role",
                            exec.order_id, exec.flags
                        ));
                    }
                    if exec.reason == RejectReason::None {
                        return Err(format!(
                            "Rejected order {} carries no reason",
                            exec.order_id
                        ));
                    }
                    summary.terminal = Some(ExecKind::Rejected);
                }
            }
        }

        let mut md_states: FxHashMap<OrderId, MdOrderState> = FxHashMap::default();
        let mut trade_events: FxHashMap<FillTradeKey, usize> = FxHashMap::default();
        let mut removes_by_order: FxHashMap<OrderId, &MdEvent> = FxHashMap::default();

        for (index, event) in self.md.iter().enumerate() {
            match event.kind {
                MdKind::Add => {
                    if event.order_id == OrderId(0) {
                        return Err(format!("market-data Add at index {index} has order id 0"));
                    }
                    if event.qty.is_zero() {
                        return Err(format!(
                            "market-data Add for order {} has zero quantity",
                            event.order_id
                        ));
                    }
                    if event.price.0 <= 0 {
                        return Err(format!(
                            "market-data Add for order {} has non-positive price {}",
                            event.order_id, event.price
                        ));
                    }
                    let state = md_states.entry(event.order_id).or_default();
                    if state.added {
                        return Err(format!(
                            "order {} is added more than once in one output",
                            event.order_id
                        ));
                    }
                    if state.reduced || state.removed {
                        return Err(format!(
                            "market-data Add for order {} follows a Remove/Reduce for the same id",
                            event.order_id
                        ));
                    }
                    state.added = true;
                    state.last_qty = Some(event.qty);
                    match summaries.get_mut(&event.order_id) {
                        Some(summary) if summary.accepted_qty.is_some() => {
                            if summary.terminal.is_some() {
                                return Err(format!(
                                    "market-data Add for order {} follows its terminal report",
                                    event.order_id
                                ));
                            }
                            if summary.add_qty.is_some() {
                                return Err(format!(
                                    "order {} has two Add events in one output",
                                    event.order_id
                                ));
                            }
                            summary.add_qty = Some(event.qty);
                        }
                        _ => {
                            return Err(format!(
                                "market-data Add for order {} has no Accepted report in this output",
                                event.order_id
                            ));
                        }
                    }
                }
                MdKind::Reduce => {
                    if event.order_id == OrderId(0) {
                        return Err(format!(
                            "market-data Reduce at index {index} has order id 0"
                        ));
                    }
                    if event.qty.is_zero() {
                        return Err(format!(
                            "market-data Reduce for order {} has zero remaining quantity; a zero remainder is a Remove",
                            event.order_id
                        ));
                    }
                    if event.price.0 <= 0 {
                        return Err(format!(
                            "market-data Reduce for order {} has non-positive price {}",
                            event.order_id, event.price
                        ));
                    }
                    let state = md_states.entry(event.order_id).or_default();
                    if state.removed {
                        return Err(format!(
                            "market-data Reduce for order {} follows a Remove for the same id",
                            event.order_id
                        ));
                    }
                    if let Some(previous) = state.last_qty
                        && event.qty.0 > previous.0
                    {
                        return Err(format!(
                            "market-data Reduce for order {} grows the remaining quantity from {previous} to {}",
                            event.order_id, event.qty
                        ));
                    }
                    state.reduced = true;
                    state.last_qty = Some(event.qty);
                }
                MdKind::Remove => {
                    if event.order_id == OrderId(0) {
                        return Err(format!(
                            "market-data Remove at index {index} has order id 0"
                        ));
                    }
                    if !event.qty.is_zero() {
                        return Err(format!(
                            "market-data Remove for order {} carries quantity {}; Remove carries no quantity",
                            event.order_id, event.qty
                        ));
                    }
                    let state = md_states.entry(event.order_id).or_default();
                    if state.removed {
                        return Err(format!(
                            "order {} is removed more than once in one output",
                            event.order_id
                        ));
                    }
                    state.removed = true;
                    state.last_qty = Some(Qty::ZERO);
                    removes_by_order.insert(event.order_id, event);
                }
                MdKind::Trade => {
                    if event.order_id == OrderId(0) {
                        return Err(format!("market-data Trade at index {index} has order id 0"));
                    }
                    if event.qty.is_zero() {
                        return Err(format!(
                            "market-data Trade for order {} has zero quantity",
                            event.order_id
                        ));
                    }
                    if event.price.0 <= 0 {
                        return Err(format!(
                            "market-data Trade for order {} has non-positive price {}",
                            event.order_id, event.price
                        ));
                    }
                    *trade_events
                        .entry((
                            event.cause_offset,
                            event.price,
                            event.qty,
                            event.side,
                            event.order_id,
                        ))
                        .or_default() += 1;
                }
                MdKind::Status => {
                    if event.order_id != OrderId(0) || !event.qty.is_zero() || event.price.0 != 0 {
                        return Err(format!(
                            "market-data Status seq {} carries order {} price {} qty {}; a status event carries none of them",
                            event.seq, event.order_id, event.price, event.qty
                        ));
                    }
                }
                // The engine never emits these on `EngineOutput::md`; snapshots
                // travel separately and BBO is a derived book view.
                MdKind::Bbo | MdKind::Snapshot => {}
            }
        }

        // Every fill has exactly one Trade with the same identity, and every
        // Trade has a fill.
        let mut fill_trades: FxHashMap<FillTradeKey, usize> = FxHashMap::default();
        for fill in &fill_reports {
            *fill_trades
                .entry((
                    fill.cause_offset,
                    fill.price,
                    fill.qty,
                    fill.side,
                    fill.order_id,
                ))
                .or_default() += 1;
        }
        for (key, fills) in &fill_trades {
            let events = trade_events.get(key).copied().unwrap_or(0);
            if events != *fills {
                return Err(format!(
                    "{fills} fill report(s) for order {} side {} price {} qty {} at offset {} have {events} matching Trade event(s)",
                    key.4, key.3, key.1, key.2, key.0
                ));
            }
        }
        for (key, events) in &trade_events {
            if !fill_trades.contains_key(key) {
                return Err(format!(
                    "{events} Trade event(s) for order {} side {} price {} qty {} at offset {} have no matching fill report",
                    key.4, key.3, key.1, key.2, key.0
                ));
            }
        }

        // Every fill has exactly one maker-side Remove/Reduce.
        let mut maker_md_counts: FxHashMap<OrderId, usize> = FxHashMap::default();
        for event in &self.md {
            if matches!(event.kind, MdKind::Reduce | MdKind::Remove) {
                *maker_md_counts.entry(event.order_id).or_default() += 1;
            }
        }
        for fill in &fill_reports {
            let count = maker_md_counts
                .get(&fill.maker_order_id)
                .copied()
                .unwrap_or(0);
            if count != 1 {
                return Err(format!(
                    "fill for order {} names maker {} but the output has {count} Remove/Reduce events for it; every fill emits exactly one maker-side event",
                    fill.order_id, fill.maker_order_id
                ));
            }
        }

        // A self-trade-prevention cancel removes a resting order outright, and a
        // client cancel of an order accepted in an earlier command does too, so
        // both must carry exactly one Remove event. A taker remainder cancel has
        // its Accepted in this output and never rested, so it emits no Remove.
        for exec in &self.executions {
            if exec.reason == RejectReason::SelfTradePrevention
                && !removes_by_order.contains_key(&exec.order_id)
            {
                return Err(format!(
                    "self-trade-prevention cancel for order {} has no Remove market-data event",
                    exec.order_id
                ));
            }
            if exec.kind == ExecKind::Cancelled
                && exec.flags & FLAG_MAKER == 0
                && summaries
                    .get(&exec.order_id)
                    .is_none_or(|state| state.accepted_qty.is_none())
                && !removes_by_order.contains_key(&exec.order_id)
            {
                return Err(format!(
                    "Cancelled order {} has no Remove market-data event in this output",
                    exec.order_id
                ));
            }
        }

        // Every Remove/Reduce is triggered by a fill naming it, or by a
        // Cancelled report for it, and agrees with that trigger on side/price.
        for event in &self.md {
            if !matches!(event.kind, MdKind::Reduce | MdKind::Remove) {
                continue;
            }
            let trigger = fills_by_maker
                .get(&event.order_id)
                .copied()
                .or_else(|| cancels_by_order.get(&event.order_id).copied());
            let Some(trigger) = trigger else {
                return Err(format!(
                    "market-data {:?} for order {} has no fill or Cancelled report in this output",
                    event.kind, event.order_id
                ));
            };
            let (expected_side, expected_price) = if trigger.kind == ExecKind::Cancelled {
                (trigger.side, trigger.price)
            } else {
                (trigger.side.opposite(), trigger.price)
            };
            if event.side != expected_side || event.price != expected_price {
                return Err(format!(
                    "market-data {:?} for order {} has side {} price {} but its trigger ({} seq {}) says side {} price {}",
                    event.kind,
                    event.order_id,
                    event.side,
                    event.price,
                    trigger.kind.as_str(),
                    trigger.seq,
                    expected_side,
                    expected_price
                ));
            }
        }

        // The accepted quantity is fully accounted for.
        for (order_id, state) in &summaries {
            let Some(accepted_qty) = state.accepted_qty else {
                continue;
            };
            if state.add_qty.is_some() && state.cancel_qty.is_some() {
                return Err(format!(
                    "order {order_id} both rests (Add) and terminates (Cancelled) in one output"
                ));
            }
            let added = state.add_qty.map_or(0, |qty| qty.0);
            let cancelled = state.cancel_qty.map_or(0, |qty| qty.0);
            if state.filled_qty + added + cancelled != accepted_qty.0 {
                return Err(format!(
                    "order {order_id} was accepted for {accepted_qty} but the output accounts for {} filled + {added} added + {cancelled} cancelled",
                    state.filled_qty
                ));
            }
            match state.last_fill {
                Some(ExecKind::PartiallyFilled)
                    if state.add_qty.is_none() && state.cancel_qty.is_none() =>
                {
                    return Err(format!(
                        "order {order_id} was partially filled and left a remainder with no Add or Cancelled report"
                    ));
                }
                Some(ExecKind::Filled) if state.filled_qty != accepted_qty.0 => {
                    return Err(format!(
                        "order {order_id} is Filled but only {} of {accepted_qty} traded",
                        state.filled_qty
                    ));
                }
                None if state.add_qty.is_none() && state.cancel_qty.is_none() => {
                    return Err(format!(
                        "order {order_id} was accepted but neither filled, added, nor cancelled"
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }
}

/// The instrument configuration fields that enter a symbol's state hash.
///
/// [`SymbolSnapshot`] stores these so [`hash_symbol_snapshot`] can reproduce
/// [`SymbolEngine::state_hash`] without consulting the live [`InstrumentConfig`]
/// (and therefore off the shard thread). `symbol` is omitted: it is already
/// carried on [`SymbolSnapshot::symbol`] and [`BookSnapshot::symbol`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolConfigSnapshot {
    pub tick_size_cents: i64,
    pub min_price_ticks: i64,
    pub max_price_ticks: i64,
    pub max_orders: usize,
    pub snapshot_depth: usize,
    pub market_collar_bps: u32,
}

impl SymbolConfigSnapshot {
    pub fn from_config(config: &InstrumentConfig) -> Self {
        Self {
            tick_size_cents: config.tick_size_cents,
            min_price_ticks: config.min_price_ticks,
            max_price_ticks: config.max_price_ticks,
            max_orders: config.max_orders,
            snapshot_depth: config.snapshot_depth,
            market_collar_bps: config.market_collar_bps,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSnapshot {
    pub symbol: Symbol,
    /// The config fields that [`SymbolEngine::state_hash`] hashes. Present so
    /// the snapshot can be re-hashed without the live config.
    ///
    /// Schema note: this field (and `BookSnapshot::min_price_ticks`) extend the
    /// serialized snapshot. The on-disk format is versioned by the `whippet`
    /// crate's snapshot store; an upgrade that changes the layout must bump
    /// that version there.
    pub config: SymbolConfigSnapshot,
    pub book: BookSnapshot,
    pub dedup: Vec<(OrderId, DedupEntry)>,
    /// Indices into [`SymbolSnapshot::dedup`] for the terminal ids in eviction
    /// order, snapshotted so a restored table evicts at the same point as a
    /// continuous replay. A `u32` is 4 bytes per terminal id, where serde
    /// writes an [`OrderId`] as a 32-character hex string (40 bytes). Not part
    /// of the state hash.
    pub dedup_terminal_idx: Vec<u32>,
    pub exec_seq: Seq,
    pub md_seq: Seq,
    pub last_offset: u64,
    pub last_ts_ns: u64,
    pub commands: u64,
    pub halted: bool,
    /// The instrument session at snapshot time. Log-ordered through
    /// [`CommandKind::SetSession`], so replay and restore agree on whether new
    /// orders are admitted. Added by snapshot-store version 5.
    pub session: SessionState,
    /// Per-participant risk limits and exposure, log-ordered through
    /// [`CommandKind::SetRisk`]. Added by snapshot-store version 6.
    pub risk: RiskTable,
    pub state_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    pub shard_id: u32,
    pub symbols: Vec<SymbolSnapshot>,
}

pub struct SymbolEngine {
    config: InstrumentConfig,
    book: Book,
    dedup: DedupTable,
    exec_seq: Seq,
    md_seq: Seq,
    last_offset: u64,
    last_ts_ns: u64,
    commands: u64,
    halted: bool,
    /// Instrument session. `Open` is the pre-session default, so an engine
    /// that never sees a [`CommandKind::SetSession`] behaves as before.
    session: SessionState,
    /// Engine-enforced per-participant limits and their live exposure. Limits
    /// arrive through [`CommandKind::SetRisk`]; exposure moves with the book
    /// and the fills, never from an out-of-band mutation.
    risk: RiskTable,
    /// How often the node should persist engine state (book + dedup). This
    /// does not emit a market-data snapshot.
    snapshot_every: u64,
    /// How often a full market-data snapshot is emitted on `EngineOutput`.
    md_snapshot_every: u64,
    /// Commands left before the next state persistence, counted down instead
    /// of dividing `commands` on every command. Re-derived whenever
    /// `commands` jumps (restore) or an interval changes.
    until_state_snapshot: u64,
    /// Commands left before the next market-data snapshot. See
    /// [`SymbolEngine::until_state_snapshot`].
    until_md_snapshot: u64,
}

/// Commands left before `commands` next reaches a multiple of `every`.
fn commands_until(every: u64, commands: u64) -> u64 {
    let every = every.max(1);
    let remainder = commands % every;
    if remainder == 0 {
        every
    } else {
        every - remainder
    }
}

/// Soft cap on the dedup table per symbol. The table is the idempotency
/// window, not an audit log: at 5,000 commands/s this is roughly 20 seconds of
/// history. Keeping it bounded keeps the snapshot proportional to the window
/// instead of to the whole run (a 500k-entry snapshot took ~570 ms to write).
const DEDUP_WINDOW: usize = 100_000;

fn dedup_capacity(config: &InstrumentConfig) -> usize {
    config.max_orders.saturating_mul(2).min(DEDUP_WINDOW)
}

impl SymbolEngine {
    pub fn new(config: InstrumentConfig) -> Self {
        let dedup_capacity = dedup_capacity(&config);
        let book = Book::new(&config);
        let mut engine = Self {
            config,
            book,
            dedup: DedupTable::new(dedup_capacity),
            exec_seq: Seq(1),
            md_seq: Seq(1),
            last_offset: 0,
            last_ts_ns: 0,
            commands: 0,
            halted: false,
            session: SessionState::Open,
            risk: RiskTable::default(),
            snapshot_every: 10_000,
            md_snapshot_every: 100_000,
            until_state_snapshot: 0,
            until_md_snapshot: 0,
        };
        engine.rederive_snapshot_countdowns();
        engine
    }

    /// Recompute the countdowns from `commands`. The cadence is defined as
    /// "every command count that is a multiple of the interval", so a restore
    /// or a mid-stream interval change resumes at exactly the same command.
    fn rederive_snapshot_countdowns(&mut self) {
        self.until_state_snapshot = commands_until(self.snapshot_every, self.commands);
        self.until_md_snapshot = commands_until(self.md_snapshot_every, self.commands);
    }

    pub fn with_snapshot_interval(mut self, every: u64) -> Self {
        self.set_snapshot_interval(every);
        self
    }

    pub fn set_snapshot_interval(&mut self, every: u64) {
        self.snapshot_every = every.max(1);
        self.until_state_snapshot = commands_until(self.snapshot_every, self.commands);
    }

    pub fn with_md_snapshot_interval(mut self, every: u64) -> Self {
        self.set_md_snapshot_interval(every);
        self
    }

    pub fn set_md_snapshot_interval(&mut self, every: u64) {
        self.md_snapshot_every = every.max(1);
        self.until_md_snapshot = commands_until(self.md_snapshot_every, self.commands);
    }

    pub fn config(&self) -> &InstrumentConfig {
        &self.config
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn dedup(&self) -> &DedupTable {
        &self.dedup
    }

    pub fn last_offset(&self) -> u64 {
        self.last_offset
    }

    pub fn last_timestamp_ns(&self) -> u64 {
        self.last_ts_ns
    }

    pub fn commands(&self) -> u64 {
        self.commands
    }

    pub fn is_halted(&self) -> bool {
        self.halted
    }

    /// The instrument session. New orders are admitted only while this is
    /// [`SessionState::Open`]; cancels and operator commands are always
    /// allowed.
    pub fn session(&self) -> SessionState {
        self.session
    }

    /// The live risk table. Diagnostic and operator read path; mutations go
    /// through [`CommandKind::SetRisk`] and the book's fills, never this
    /// accessor.
    pub fn risk_table(&self) -> &RiskTable {
        &self.risk
    }

    /// The halt and session fields as one value. This is the single admission
    /// predicate and MD flag encoding; the engine keeps the fields separate
    /// only because the snapshot layout stores them separately.
    pub fn trading_status(&self) -> TradingStatus {
        TradingStatus::new(self.session, self.halted)
    }

    /// Set the halt flag without an input command. This bypasses the log, so
    /// replay and snapshots only stay consistent when it is used before any
    /// command is logged (bootstrap/admin paths should prefer
    /// [`Command::halt`]/[`Command::resume`], which are ordered).
    pub fn set_halted(&mut self, halted: bool) {
        self.halted = halted;
    }

    /// Apply one input command and append its outputs.
    pub fn process(&mut self, command: &Command, input_offset: u64, out: &mut EngineOutput) {
        self.last_offset = input_offset;
        self.last_ts_ns = self.last_ts_ns.max(command.timestamp_ns);
        self.commands = self.commands.saturating_add(1);

        match command.kind {
            // Session control is dispatched before the order path: it changes
            // admission for every later command in the log, exactly like halt.
            CommandKind::SetSession => self.process_set_session(command, out),
            CommandKind::SetRisk => self.process_set_risk(command, out),
            CommandKind::Halt => self.process_halt(command, out),
            CommandKind::Resume => self.process_resume(command, out),
            CommandKind::Cancel => self.process_cancel(command, out),
            CommandKind::NewOrder => self.process_new_order(command, out),
        }

        out.state_snapshot_due = false;
        if self.until_state_snapshot > 0 {
            self.until_state_snapshot -= 1;
            if self.until_state_snapshot == 0 {
                out.state_snapshot_due = true;
                self.until_state_snapshot = self.snapshot_every;
            }
        }
        if self.until_md_snapshot > 0 {
            self.until_md_snapshot -= 1;
            if self.until_md_snapshot == 0 {
                out.snapshots.push(self.snapshot());
                self.until_md_snapshot = self.md_snapshot_every;
            }
        }
    }

    /// Halt the instrument.
    ///
    /// Halt and resume are log-ordered operator commands, so they take part in
    /// the same offset/sequence bookkeeping as client traffic: `process` has
    /// already set `last_offset` and advanced the command clock before this
    /// runs, which makes the emitted status event point at the halt command's
    /// own offset. The audit trail is the gateway's record for the command
    /// itself; the engine emits market data only, no execution report.
    ///
    /// Repeating a halt is idempotent on the book and re-emits the status
    /// event: an operator may need confirmation of the current state, and the
    /// event is harmless to consumers that already know.
    fn process_halt(&mut self, command: &Command, out: &mut EngineOutput) {
        self.halted = true;
        self.push_status(out, command);
    }

    /// Resume a halted instrument. A resume on a running instrument is a no-op
    /// on the book (it still re-emits the clear status).
    fn process_resume(&mut self, command: &Command, out: &mut EngineOutput) {
        self.halted = false;
        self.push_status(out, command);
    }

    /// Move the instrument to the session carried by the command.
    ///
    /// Like halt/resume, this is an ordered operator command: `process` has
    /// already advanced `last_offset`, `last_ts_ns` and `commands`, so the
    /// status event points at the command's own offset and the change survives
    /// replay. The book is untouched: a session change stops new risk, it does
    /// not cancel resting orders.
    ///
    /// A command whose `flags` do not decode (bits outside the session byte,
    /// or an unknown code) leaves the session unchanged and still re-emits the
    /// current status. The journal is a trust boundary, so an undecodable
    /// session is treated as "no instruction" rather than as a value to guess
    /// at; the gateway validates before append, so this is only reachable from
    /// a corrupt or crafted log, and the deterministic outcome is that the
    /// state hash does not change.
    fn process_set_session(&mut self, command: &Command, out: &mut EngineOutput) {
        if let Ok(session) = command.session_state() {
            self.session = session;
        }
        self.push_status(out, command);
    }

    /// Set or clear one participant's limits.
    ///
    /// `SetRisk` is an ordered operator command: the engine applies it at the
    /// command's log position, so a replay sees every order accepted under the
    /// old limits and every later order under the new ones. The command is a
    /// no-op when its `price` (the notional limit) is negative or its symbol is
    /// not this engine's instrument; both are unreachable through the gateway,
    /// which validates before append, and a corrupt log must not make the
    /// deterministic outcome a panic.
    ///
    /// Limits apply to the participant carried by the command; existing
    /// exposure is preserved, so tightening a limit immediately gates new risk
    /// without inventing or erasing the position already taken.
    fn process_set_risk(&mut self, command: &Command, _out: &mut EngineOutput) {
        if command.symbol != self.config.symbol || command.price.0 < 0 {
            return;
        }
        let limits = RiskLimits {
            max_net_qty: command.qty.0,
            max_open_buy_notional_cents: command.price.0 as u64,
        };
        self.risk.set_limits(command.participant, limits);
    }

    /// The MD status bits for the current venue state: the halt flag plus the
    /// session code. Both live in one byte so a Status event (and the trailing
    /// snapshot byte) is a complete venue-status view.
    fn status_flags(&self) -> u8 {
        self.trading_status().to_md_flags()
    }

    fn push_status(&mut self, out: &mut EngineOutput, command: &Command) {
        let flags = self.status_flags();
        self.push_md(
            out,
            MdKind::Status,
            Side::Buy,
            Price::ZERO,
            Qty::ZERO,
            OrderId(0),
            command.participant,
            flags,
        );
    }

    fn process_cancel(&mut self, command: &Command, out: &mut EngineOutput) {
        let removed = self
            .book
            .cancel(command.order_id, Some(command.participant));
        let Some(removed) = removed else {
            self.push_exec(
                out,
                ExecKind::Rejected,
                command,
                command.side,
                Price::ZERO,
                Qty::ZERO,
                Qty::ZERO,
                OrderId(0),
                RejectReason::UnknownOrder,
                0,
            );
            return;
        };

        self.risk.on_cancel(
            removed.participant,
            removed.side,
            self.quote_cents(removed.price),
            removed.remaining.0,
        );
        self.dedup.update(removed.order_id, DedupStatus::Cancelled);
        self.push_exec(
            out,
            ExecKind::Cancelled,
            command,
            // The report describes the cancelled order, not the cancel
            // request: a fixed-layout cancel command always carries side Buy.
            // Reporting the request's side would disagree with the `Remove`
            // market-data event for the same order id.
            removed.side,
            removed.price,
            removed.remaining,
            Qty::ZERO,
            OrderId(0),
            RejectReason::None,
            0,
        );
        self.push_md(
            out,
            MdKind::Remove,
            removed.side,
            removed.price,
            Qty::ZERO,
            removed.order_id,
            removed.participant,
            0,
        );
    }

    fn process_new_order(&mut self, command: &Command, out: &mut EngineOutput) {
        if command.symbol != self.config.symbol {
            self.reject(out, command, RejectReason::UnknownSymbol);
            return;
        }
        // A halt blocks new risk but not risk withdrawal: cancels are
        // dispatched above this check so participants can pull resting orders
        // while the instrument is halted, and resume itself is never blocked.
        //
        // The halt check runs before the session check on purpose: a halted
        // instrument is an explicit operator stop, so a client that also
        // arrives during a pre-open or closed session should be told "halted"
        // rather than given a session-specific reason that would read as a
        // scheduling problem. The rejection is the same either way; only the
        // reason differs.
        if self.halted {
            self.reject(out, command, RejectReason::Halted);
            return;
        }
        // Sessions only gate new risk. Cancels stay open in every session so a
        // closed instrument can still have its resting orders withdrawn.
        if self.session != SessionState::Open {
            self.reject(out, command, RejectReason::SessionNotOpen);
            return;
        }
        if command.qty.is_zero() || command.qty.0 > MAX_ORDER_QTY {
            self.reject(out, command, RejectReason::InvalidQty);
            return;
        }
        if command.order_id.0 == 0 {
            self.reject(out, command, RejectReason::InvalidQty);
            return;
        }
        if command.order_type.requires_price() && command.price.0 <= 0 {
            self.reject(out, command, RejectReason::InvalidPrice);
            return;
        }
        if command.order_type.requires_price() && !self.config.contains_ticks(command.price) {
            self.reject(out, command, RejectReason::PriceOutOfBand);
            return;
        }
        if self.dedup.contains(command.order_id) {
            self.reject(out, command, RejectReason::DuplicateOrderId);
            return;
        }

        let opposite = command.side.opposite();
        let best_opposite = self.book.best_index(opposite);
        let crosses = match command.order_type {
            OrderType::Market => best_opposite.is_some(),
            _ => match (command.side, best_opposite) {
                (Side::Buy, Some(index)) => self.book.price_at(index).0 <= command.price.0,
                (Side::Sell, Some(index)) => self.book.price_at(index).0 >= command.price.0,
                _ => false,
            },
        };

        if command.order_type == OrderType::PostOnly && crosses {
            self.reject(out, command, RejectReason::PostOnlyWouldCross);
            return;
        }

        if command.order_type == OrderType::Fok
            && !self.book.has_executable_qty_excluding(
                opposite,
                command.price,
                command.participant,
                command.qty,
            )
        {
            self.reject(out, command, RejectReason::FokUnfillable);
            return;
        }

        // Pre-trade risk runs after the order's own validity and fillability
        // checks and before the id is accepted: a refused order must not leave
        // a live dedup entry or any book/MD mutation. The candidate is charged
        // its full quantity, and a limit order that can rest is charged its
        // full buy notional even if it would cross, so admission never relies
        // on an execution that has not happened yet.
        let may_rest = matches!(command.order_type, OrderType::Limit | OrderType::PostOnly);
        if self
            .risk
            .check_new_order(
                command.participant,
                command.side,
                command.qty.0,
                self.quote_cents(command.price),
                may_rest,
            )
            .is_err()
        {
            // The rejection reason is the only client-visible signal; the
            // exact bound that tripped is operator telemetry, not wire data.
            self.reject(out, command, RejectReason::RiskLimit);
            return;
        }

        let entry = DedupEntry {
            status: DedupStatus::Accepted,
            input_offset: self.last_offset,
            timestamp_ns: command.timestamp_ns,
            seq: self.exec_seq,
        };
        self.dedup.insert(command.order_id, entry);

        let accepted_flags = if crosses
            || matches!(
                command.order_type,
                OrderType::Market | OrderType::Ioc | OrderType::Fok
            ) {
            FLAG_TAKER
        } else {
            FLAG_RESTED
        };
        self.push_exec(
            out,
            ExecKind::Accepted,
            command,
            command.side,
            command.price,
            command.qty,
            command.qty,
            OrderId(0),
            RejectReason::None,
            accepted_flags,
        );

        let mut remaining = command.qty;
        while !remaining.is_zero() {
            let Some(best_index) = self.book.best_index(opposite) else {
                break;
            };
            let maker_price = self.book.price_at(best_index);
            let price_crosses = match command.order_type {
                OrderType::Market => self.within_collar(command.side, maker_price),
                _ => match command.side {
                    Side::Buy => maker_price.0 <= command.price.0,
                    Side::Sell => maker_price.0 >= command.price.0,
                },
            };
            if !price_crosses {
                break;
            }
            let Some(head) = self.book.head(opposite, best_index) else {
                break;
            };
            let maker = *self.book.slot(head);

            // Cancel-resting self-trade prevention. The resting order is
            // removed, never the incoming one, so the aggressor keeps its
            // price-time priority at its own level if it later rests.
            if maker.participant == command.participant {
                if let Some(removed) = self.book.cancel(maker.order_id, None) {
                    self.risk.on_cancel(
                        removed.participant,
                        removed.side,
                        self.quote_cents(removed.price),
                        removed.remaining.0,
                    );
                    self.dedup.update(maker.order_id, DedupStatus::Cancelled);
                    self.push_maker_exec(
                        out,
                        ExecKind::Cancelled,
                        command,
                        &removed,
                        RejectReason::SelfTradePrevention,
                    );
                    self.push_md(
                        out,
                        MdKind::Remove,
                        removed.side,
                        removed.price,
                        Qty::ZERO,
                        removed.order_id,
                        removed.participant,
                        0,
                    );
                }
                continue;
            }

            let fill = remaining.min(maker.remaining);
            let maker_remaining_after = maker.remaining.saturating_sub(fill);
            let maker_fully_filled = self.book.reduce(head, fill);
            remaining = remaining.saturating_sub(fill);
            self.book.record_trade(maker_price, fill, command.side);
            // Both legs move risk: the taker takes a position, and the maker's
            // resting quantity leaves the book (leaving its net position in its
            // place). A partial maker fill therefore reduces open exposure by
            // exactly the filled quantity.
            self.risk.on_take(command.participant, command.side, fill.0);
            self.risk.on_maker_fill(
                maker.participant,
                maker.side,
                self.quote_cents(maker_price),
                fill.0,
            );

            let kind = if remaining.is_zero() {
                ExecKind::Filled
            } else {
                ExecKind::PartiallyFilled
            };
            self.push_exec(
                out,
                kind,
                command,
                command.side,
                maker_price,
                fill,
                remaining,
                maker.order_id,
                RejectReason::None,
                FLAG_TAKER,
            );

            self.push_md(
                out,
                MdKind::Trade,
                command.side,
                maker_price,
                fill,
                command.order_id,
                command.participant,
                0,
            );

            if maker_fully_filled {
                self.dedup.update(maker.order_id, DedupStatus::Filled);
                self.push_md(
                    out,
                    MdKind::Remove,
                    maker.side,
                    maker_price,
                    Qty::ZERO,
                    maker.order_id,
                    maker.participant,
                    0,
                );
            } else {
                self.push_md(
                    out,
                    MdKind::Reduce,
                    maker.side,
                    maker_price,
                    maker_remaining_after,
                    maker.order_id,
                    maker.participant,
                    0,
                );
            }
        }

        if remaining.is_zero() {
            self.dedup.update(command.order_id, DedupStatus::Filled);
            return;
        }

        if matches!(command.order_type, OrderType::Limit | OrderType::PostOnly) {
            match self.book.insert(
                command.order_id,
                command.participant,
                command.side,
                command.price,
                remaining,
                self.exec_seq.0,
            ) {
                Ok(_) => {
                    self.risk.on_rest(
                        command.participant,
                        command.side,
                        self.quote_cents(command.price),
                        remaining.0,
                    );
                    self.dedup.update(command.order_id, DedupStatus::Resting);
                    self.push_md(
                        out,
                        MdKind::Add,
                        command.side,
                        command.price,
                        remaining,
                        command.order_id,
                        command.participant,
                        0,
                    );
                }
                Err(_) => {
                    // A full book is an operational failure, not a silent drop.
                    // The order was already accepted and reported, so its
                    // terminal status matches the report the client sees
                    // (`Cancelled`/`Internal`). `Rejected` is only written by
                    // the creation path, where no accepted report existed.
                    //
                    // The terminal report carries the order's admission role
                    // (`accepted_flags`), not an unconditional taker bit: a
                    // passive limit order that could not rest never took
                    // liquidity, and its cancel must agree with its acceptance.
                    self.dedup.update(command.order_id, DedupStatus::Cancelled);
                    self.push_exec(
                        out,
                        ExecKind::Cancelled,
                        command,
                        command.side,
                        command.price,
                        remaining,
                        Qty::ZERO,
                        OrderId(0),
                        RejectReason::Internal,
                        accepted_flags,
                    );
                }
            }
        } else {
            // Market/IOC/FOK remainder is cancelled. FOK normally filled above;
            // STP or a collar can still leave a remainder.
            self.dedup.update(command.order_id, DedupStatus::Cancelled);
            self.push_exec(
                out,
                ExecKind::Cancelled,
                command,
                command.side,
                command.price,
                remaining,
                Qty::ZERO,
                OrderId(0),
                RejectReason::None,
                accepted_flags,
            );
        }
    }

    /// Convert a tick price to quote cents for the risk table.
    ///
    /// The engine stores tick indices; risk limits are quoted in cents because
    /// operators reason about money. Prices are validated before this runs, so
    /// the conversion is a saturating multiply rather than a fallible one; a
    /// non-positive tick size is rejected when the instrument is configured.
    fn quote_cents(&self, price: Price) -> u128 {
        u128::try_from(price.0)
            .unwrap_or_default()
            .saturating_mul(u128::try_from(self.config.tick_size_cents).unwrap_or_default())
    }

    fn within_collar(&self, side: Side, price: Price) -> bool {
        let (last_price, _, _) = self.book.last_trade();
        let reference = if last_price.0 > 0 {
            last_price
        } else if let (Some(bid), Some(ask)) = (self.book.best_bid(), self.book.best_ask()) {
            Price((bid.0 + ask.0) / 2)
        } else {
            self.config.reference_price()
        };
        let collar = reference.0 * i64::from(self.config.market_collar_bps) / 10_000;
        match side {
            Side::Buy => price.0 <= reference.0 + collar,
            Side::Sell => price.0 >= reference.0 - collar,
        }
    }

    fn reject(&mut self, out: &mut EngineOutput, command: &Command, reason: RejectReason) {
        self.dedup.insert(
            command.order_id,
            DedupEntry {
                status: DedupStatus::Rejected,
                input_offset: self.last_offset,
                timestamp_ns: command.timestamp_ns,
                seq: self.exec_seq,
            },
        );
        self.push_exec(
            out,
            ExecKind::Rejected,
            command,
            command.side,
            command.price,
            Qty::ZERO,
            Qty::ZERO,
            OrderId(0),
            reason,
            0,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn push_exec(
        &mut self,
        out: &mut EngineOutput,
        kind: ExecKind,
        command: &Command,
        side: Side,
        price: Price,
        qty: Qty,
        remaining: Qty,
        maker_order_id: OrderId,
        reason: RejectReason,
        flags: u16,
    ) {
        let seq = self.exec_seq.next_seq();
        out.executions.push(ExecutionReport {
            kind,
            symbol: command.symbol,
            side,
            order_type: command.order_type,
            price,
            qty,
            remaining,
            order_id: command.order_id,
            maker_order_id,
            seq,
            cause_offset: self.last_offset,
            timestamp_ns: command.timestamp_ns,
            reason,
            flags,
        });
    }

    fn push_maker_exec(
        &mut self,
        out: &mut EngineOutput,
        kind: ExecKind,
        command: &Command,
        removed: &RestingOrder,
        reason: RejectReason,
    ) {
        let seq = self.exec_seq.next_seq();
        out.executions.push(ExecutionReport {
            kind,
            symbol: command.symbol,
            side: removed.side,
            order_type: OrderType::Limit,
            price: removed.price,
            qty: removed.remaining,
            remaining: Qty::ZERO,
            order_id: removed.order_id,
            maker_order_id: OrderId(0),
            seq,
            cause_offset: self.last_offset,
            timestamp_ns: command.timestamp_ns,
            reason,
            flags: FLAG_MAKER,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn push_md(
        &mut self,
        out: &mut EngineOutput,
        kind: MdKind,
        side: Side,
        price: Price,
        qty: Qty,
        order_id: OrderId,
        participant: ParticipantId,
        flags: u8,
    ) {
        let seq = self.md_seq.next_seq();
        out.md.push(MdEvent {
            kind,
            symbol: self.config.symbol,
            seq,
            cause_offset: self.last_offset,
            timestamp_ns: self.last_ts_ns,
            side,
            flags,
            price,
            qty,
            order_id,
            participant,
        });
    }

    pub fn snapshot(&self) -> MdSnapshot {
        let (bids, asks) = self.book.snapshot_levels(self.config.snapshot_depth);
        let (last_trade_price, last_trade_qty, _) = self.book.last_trade();
        let book_snapshot = self.book.snapshot();
        let orders = book_snapshot
            .orders
            .iter()
            .map(|order| MdOrder {
                order_id: order.order_id,
                side: order.side,
                price: order.price,
                qty: order.remaining,
                participant: order.participant,
            })
            .collect();
        MdSnapshot {
            symbol: self.config.symbol,
            // The snapshot describes the state through the last emitted MD
            // event, not the next sequence to use. At startup this is 0, so the
            // first event is 1 and the sequence never repeats.
            seq: Seq(self.md_seq.0.saturating_sub(1)),
            cause_offset: self.last_offset,
            timestamp_ns: self.last_ts_ns,
            last_trade_price,
            last_trade_qty,
            bids,
            asks,
            orders,
            flags: self.status_flags(),
        }
    }

    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"whippet-engine-v2");
        hasher.update(&self.config.tick_size_cents.to_le_bytes());
        hasher.update(&self.config.min_price_ticks.to_le_bytes());
        hasher.update(&self.config.max_price_ticks.to_le_bytes());
        hasher.update(&(self.config.max_orders as u64).to_le_bytes());
        hasher.update(&(self.config.snapshot_depth as u64).to_le_bytes());
        hasher.update(&u64::from(self.config.market_collar_bps).to_le_bytes());
        hasher.update(&self.commands.to_le_bytes());
        self.book.hash_into(&mut hasher);
        self.dedup.hash_into(&mut hasher);
        hasher.update(&self.exec_seq.0.to_le_bytes());
        hasher.update(&self.md_seq.0.to_le_bytes());
        hasher.update(&self.last_offset.to_le_bytes());
        hasher.update(&self.last_ts_ns.to_le_bytes());
        // Venue status is two bytes: halt and session. The session byte is a
        // deliberate state-format change (snapshot store v5); it makes a
        // restored pre-open/closed instrument hash differently from an open
        // one, so replay cannot lose a session change.
        hasher.update(&[u8::from(self.halted), self.session.as_code()]);
        // Risk limits and exposure are engine state: a restored engine with a
        // dropped `SetRisk` must hash differently. Version 2 of the domain
        // covers this tail.
        self.risk.hash_into(&mut hasher);
        *hasher.finalize().as_bytes()
    }

    /// Check every book, dedup, and cross-structure invariant that must hold
    /// between commands.
    ///
    /// This is a diagnostic for tests; it allocates O(book) for the snapshot and
    /// is O(book + dedup window). It is never called from `process`.
    ///
    /// In addition to [`Book::check_invariants`] and
    /// [`DedupTable::check_invariants`] it requires the two structures to agree:
    ///
    /// - no id is left `Accepted`: that state only exists inside a single
    ///   `process` call, between admission and the transition that resolves it;
    /// - every `Resting` id has a book order and every book order has a
    ///   `Resting` id, with equal counts;
    /// - every terminal (`Filled`/`Cancelled`/`Rejected`) id is absent from the
    ///   book;
    /// - every live id is exactly one of `Resting` or terminal, so
    ///   `dedup.len() == resting + terminal`.
    pub fn check_invariants(&self) -> Result<(), String> {
        self.book
            .check_invariants()
            .map_err(|err| format!("book: {err}"))?;
        self.dedup
            .check_invariants()
            .map_err(|err| format!("dedup: {err}"))?;

        self.risk
            .check_invariants()
            .map_err(|err| format!("risk: {err}"))?;

        let snapshot = self.book.snapshot();
        let mut resting_in_book = 0usize;
        for order in &snapshot.orders {
            match self.dedup.status(order.order_id) {
                Some(DedupStatus::Resting) => resting_in_book += 1,
                other => {
                    return Err(format!(
                        "book order {} has dedup status {other:?}, expected Resting",
                        order.order_id
                    ));
                }
            }
        }

        let mut resting_in_dedup = 0usize;
        let mut terminal = 0usize;
        for (order_id, entry) in self.dedup.iter() {
            match entry.status {
                DedupStatus::Accepted => {
                    return Err(format!(
                        "id {order_id} is still Accepted after the command completed"
                    ));
                }
                DedupStatus::Resting => {
                    if self.book.order_index(order_id).is_none() {
                        return Err(format!(
                            "dedup id {order_id} is Resting but the book does not hold it"
                        ));
                    }
                    resting_in_dedup += 1;
                }
                status if status.is_terminal() => {
                    if self.book.order_index(order_id).is_some() {
                        return Err(format!(
                            "terminal id {order_id} ({status:?}) is still in the book"
                        ));
                    }
                    terminal += 1;
                }
                other => return Err(format!("id {order_id} has unknown status {other:?}")),
            }
        }

        if resting_in_dedup != resting_in_book {
            return Err(format!(
                "dedup has {resting_in_dedup} Resting ids but the book holds {resting_in_book} orders"
            ));
        }
        if resting_in_book != self.book.order_count() {
            return Err(format!(
                "book order_count is {} but its snapshot lists {resting_in_book}",
                self.book.order_count()
            ));
        }
        if resting_in_dedup + terminal != self.dedup.len() {
            return Err(format!(
                "dedup holds {} ids but {resting_in_dedup} Resting + {terminal} terminal",
                self.dedup.len()
            ));
        }
        self.risk
            .check_against_orders(&snapshot.orders, self.config.tick_size_cents)
            .map_err(|err| format!("risk: {err}"))?;
        Ok(())
    }

    /// Check one command's [`EngineOutput`] against the command that produced it.
    ///
    /// This is the provenance half of the output diagnostic: the output-only
    /// checks live in [`EngineOutput::check_invariants`], and this method adds
    /// the facts that need the input command and the engine clocks:
    ///
    /// - every execution report and market-data event carries
    ///   `cause_offset == input_offset`;
    /// - every execution report carries the command's `timestamp_ns`;
    /// - every market-data event (and snapshot) carries
    ///   [`SymbolEngine::last_timestamp_ns`], the monotone venue clock;
    /// - every execution report names the command's symbol, and every
    ///   market-data event names this engine's instrument;
    /// - the last execution and market-data `seq` in the output are one below
    ///   the engine's next sequence, so the per-symbol sequence has no gap
    ///   between consecutive calls.
    ///
    /// The sequence-continuity check reads the engine's live counters, so
    /// `out` must be the output of the `process` call that immediately
    /// preceded this check (the same rule as calling it after every command).
    /// The method allocates O(output), is a diagnostic for tests and replay
    /// verification, and is never called from `process`.
    ///
    /// The market-data timestamp is deliberately *not* compared to the
    /// command's: a non-monotone input log leaves the venue clock ahead of the
    /// command, and market data carries the venue clock while execution reports
    /// carry the command's own stamp.
    pub fn check_output_invariants(
        &self,
        command: &Command,
        input_offset: u64,
        out: &EngineOutput,
    ) -> Result<(), String> {
        out.check_invariants()?;

        for exec in &out.executions {
            if exec.cause_offset != input_offset {
                return Err(format!(
                    "execution seq {} ({} {}) has cause_offset {} but the command is at offset {input_offset}",
                    exec.seq,
                    exec.kind.as_str(),
                    exec.order_id,
                    exec.cause_offset
                ));
            }
            if exec.timestamp_ns != command.timestamp_ns {
                return Err(format!(
                    "execution seq {} ({} {}) has timestamp {} but the command's timestamp is {}",
                    exec.seq,
                    exec.kind.as_str(),
                    exec.order_id,
                    exec.timestamp_ns,
                    command.timestamp_ns
                ));
            }
            if exec.symbol != command.symbol {
                return Err(format!(
                    "execution seq {} names symbol {} but the command is for {}",
                    exec.seq, exec.symbol, command.symbol
                ));
            }
        }

        let venue_timestamp = self.last_ts_ns;
        for event in &out.md {
            if event.cause_offset != input_offset {
                return Err(format!(
                    "market-data seq {} ({:?}) has cause_offset {} but the command is at offset {input_offset}",
                    event.seq, event.kind, event.cause_offset
                ));
            }
            if event.timestamp_ns != venue_timestamp {
                return Err(format!(
                    "market-data seq {} ({:?}) has timestamp {} but the venue clock is {venue_timestamp}",
                    event.seq, event.kind, event.timestamp_ns
                ));
            }
            if event.symbol != self.config.symbol {
                return Err(format!(
                    "market-data seq {} names symbol {} but this engine owns {}",
                    event.seq, event.symbol, self.config.symbol
                ));
            }
        }

        for snapshot in &out.snapshots {
            if snapshot.cause_offset != input_offset {
                return Err(format!(
                    "snapshot has cause_offset {} but the command is at offset {input_offset}",
                    snapshot.cause_offset
                ));
            }
            if snapshot.timestamp_ns != venue_timestamp {
                return Err(format!(
                    "snapshot has timestamp {} but the venue clock is {venue_timestamp}",
                    snapshot.timestamp_ns
                ));
            }
        }

        if let Some(last) = out.executions.last()
            && last.seq.0.checked_add(1) != Some(self.exec_seq.0)
        {
            return Err(format!(
                "last execution seq {} does not precede the engine's next seq {}; the sequence must be continuous between outputs",
                last.seq, self.exec_seq
            ));
        }
        if let Some(last) = out.md.last()
            && last.seq.0.checked_add(1) != Some(self.md_seq.0)
        {
            return Err(format!(
                "last market-data seq {} does not precede the engine's next seq {}; the sequence must be continuous between outputs",
                last.seq, self.md_seq
            ));
        }
        Ok(())
    }

    /// Snapshot after compacting the dedup queues.
    ///
    /// The node uses this path so the saved insertion walk is exactly the live
    /// walk; the plain `snapshot_state` stays for tests that do not need it.
    pub fn snapshot_state_compacted(&mut self) -> SymbolSnapshot {
        self.dedup.compact();
        self.snapshot_state()
    }

    pub fn snapshot_state_without_hash_compacted(&mut self) -> SymbolSnapshot {
        self.dedup.compact();
        self.snapshot_state_without_hash()
    }

    pub fn snapshot_state(&self) -> SymbolSnapshot {
        let mut snapshot = self.snapshot_state_without_hash();
        snapshot.state_hash = self.state_hash();
        snapshot
    }

    /// Build the same snapshot as [`SymbolEngine::snapshot_state`] but leave
    /// `state_hash` zeroed. The caller fills it later, off the shard thread,
    /// with [`hash_symbol_snapshot`].
    pub fn snapshot_state_without_hash(&self) -> SymbolSnapshot {
        // One walk, reused for both the serialized entries and the compact
        // terminal queue: walking the dedup table twice was pure cost.
        let dedup = self.dedup.entries_in_order();
        let dedup_terminal_idx = self.dedup.terminal_indices(&dedup);
        SymbolSnapshot {
            symbol: self.config.symbol,
            config: SymbolConfigSnapshot::from_config(&self.config),
            book: self.book.snapshot(),
            dedup,
            dedup_terminal_idx,
            exec_seq: self.exec_seq,
            md_seq: self.md_seq,
            last_offset: self.last_offset,
            last_ts_ns: self.last_ts_ns,
            commands: self.commands,
            halted: self.halted,
            session: self.session,
            risk: self.risk.clone(),
            state_hash: [0; 32],
        }
    }

    pub fn from_snapshot(
        config: InstrumentConfig,
        snapshot: SymbolSnapshot,
    ) -> Result<Self, EngineError> {
        let dedup_capacity = dedup_capacity(&config);
        let book = Book::from_snapshot(&config, snapshot.book)?;
        let mut engine = Self {
            config,
            book,
            dedup: DedupTable::from_entries(
                dedup_capacity,
                snapshot.dedup,
                snapshot.dedup_terminal_idx,
            )
            .map_err(EngineError::Snapshot)?,
            exec_seq: snapshot.exec_seq,
            md_seq: snapshot.md_seq,
            last_offset: snapshot.last_offset,
            last_ts_ns: snapshot.last_ts_ns,
            commands: snapshot.commands,
            halted: snapshot.halted,
            session: snapshot.session,
            risk: snapshot.risk,
            snapshot_every: 10_000,
            md_snapshot_every: 100_000,
            until_state_snapshot: 0,
            until_md_snapshot: 0,
        };
        engine.rederive_snapshot_countdowns();
        let actual = engine.state_hash();
        if actual != snapshot.state_hash {
            return Err(EngineError::HashMismatch {
                expected: hex(&snapshot.state_hash),
                actual: hex(&actual),
            });
        }
        // Re-derive the book from the snapshot once more to ensure the arena
        // order is canonical, then verify the hash again.
        let canonical = engine.snapshot_state();
        if canonical.state_hash != snapshot.state_hash {
            return Err(EngineError::HashMismatch {
                expected: hex(&snapshot.state_hash),
                actual: hex(&canonical.state_hash),
            });
        }
        // The hash proves the bytes agree with the writer; the structural
        // checks prove the bytes describe an engine that can keep trading.
        // Restore is cold, so the O(book + dedup) walk is affordable.
        engine.check_invariants().map_err(EngineError::Snapshot)?;
        engine.last_offset = snapshot.last_offset;
        Ok(engine)
    }
}

/// Compute a symbol's state hash from a [`SymbolSnapshot`], byte-identically to
/// [`SymbolEngine::state_hash`].
///
/// This is the off-shard half of snapshot persistence: the shard thread calls
/// [`SymbolEngine::snapshot_state_without_hash`] and hands the snapshot to the
/// writer, which calls this (or [`fill_symbol_hashes`]) instead of walking the
/// live ladder. The field order and the `b"whippet-engine-v1"` domain prefix
/// match [`SymbolEngine::state_hash`] exactly.
/// Hash the dedup state carried by a snapshot.
///
/// The on-disk terminal queue is stored as indices into `snapshot.dedup`; the
/// hash is over the resolved ids so it matches the live table's
/// [`DedupTable::hash_into`] byte for byte. An out-of-range index is hashed as
/// an impossible order id rather than panicking: the restore path rejects it,
/// and a tampered snapshot's recomputed hash will not match the stored one.
fn hash_dedup_snapshot(snapshot: &SymbolSnapshot, hasher: &mut blake3::Hasher) {
    let terminal: Vec<OrderId> = snapshot
        .dedup_terminal_idx
        .iter()
        .map(|index| {
            snapshot
                .dedup
                .get(*index as usize)
                .map(|(order_id, _)| *order_id)
                .unwrap_or(OrderId(u128::MAX))
        })
        .collect();
    hash_dedup_entries(&snapshot.dedup, &terminal, hasher);
}

pub fn hash_symbol_snapshot(snapshot: &SymbolSnapshot) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"whippet-engine-v2");
    let config = &snapshot.config;
    hasher.update(&config.tick_size_cents.to_le_bytes());
    hasher.update(&config.min_price_ticks.to_le_bytes());
    hasher.update(&config.max_price_ticks.to_le_bytes());
    hasher.update(&(config.max_orders as u64).to_le_bytes());
    hasher.update(&(config.snapshot_depth as u64).to_le_bytes());
    hasher.update(&u64::from(config.market_collar_bps).to_le_bytes());
    hasher.update(&snapshot.commands.to_le_bytes());
    hash_book_snapshot(&snapshot.book, &mut hasher);
    hash_dedup_snapshot(snapshot, &mut hasher);
    hasher.update(&snapshot.exec_seq.0.to_le_bytes());
    hasher.update(&snapshot.md_seq.0.to_le_bytes());
    hasher.update(&snapshot.last_offset.to_le_bytes());
    hasher.update(&snapshot.last_ts_ns.to_le_bytes());
    hasher.update(&[u8::from(snapshot.halted), snapshot.session.as_code()]);
    snapshot.risk.hash_into(&mut hasher);
    *hasher.finalize().as_bytes()
}

pub struct ShardEngine {
    shard_id: u32,
    engines: FxHashMap<Symbol, SymbolEngine>,
}

impl ShardEngine {
    pub fn new(shard_id: u32, exchange: &ExchangeConfig) -> Self {
        let mut engines = FxHashMap::default();
        for instrument in &exchange.instruments {
            if instrument.symbol.partition(exchange.partitions) == shard_id {
                engines.insert(instrument.symbol, SymbolEngine::new(instrument.clone()));
            }
        }
        Self { shard_id, engines }
    }

    pub fn new_with_snapshot_interval(
        shard_id: u32,
        exchange: &ExchangeConfig,
        every: u64,
    ) -> Self {
        let mut shard = Self::new(shard_id, exchange);
        for engine in shard.engines.values_mut() {
            engine.set_snapshot_interval(every);
        }
        shard
    }

    pub fn set_snapshot_interval(&mut self, every: u64) {
        for engine in self.engines.values_mut() {
            engine.set_snapshot_interval(every);
        }
    }

    pub fn set_md_snapshot_interval(&mut self, every: u64) {
        for engine in self.engines.values_mut() {
            engine.set_md_snapshot_interval(every);
        }
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn symbols(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.engines.keys().copied()
    }

    pub fn engine(&self, symbol: Symbol) -> Option<&SymbolEngine> {
        self.engines.get(&symbol)
    }

    pub fn engine_mut(&mut self, symbol: Symbol) -> Option<&mut SymbolEngine> {
        self.engines.get_mut(&symbol)
    }

    pub fn process(
        &mut self,
        command: &Command,
        input_offset: u64,
        out: &mut EngineOutput,
    ) -> Result<(), EngineError> {
        out.clear();
        let engine = self
            .engines
            .get_mut(&command.symbol)
            .ok_or(EngineError::UnknownSymbol(command.symbol, self.shard_id))?;
        engine.process(command, input_offset, out);
        Ok(())
    }

    pub fn snapshot_state(&self) -> ShardSnapshot {
        let mut symbols: Vec<_> = self
            .engines
            .values()
            .map(SymbolEngine::snapshot_state)
            .collect();
        symbols.sort_unstable_by_key(|snapshot| snapshot.symbol);
        ShardSnapshot {
            shard_id: self.shard_id,
            symbols,
        }
    }

    /// Build the shard snapshot with every symbol's `state_hash` zeroed. The
    /// hashes are filled later, off the shard thread, with
    /// [`fill_symbol_hashes`].
    pub fn snapshot_state_without_hash(&self) -> ShardSnapshot {
        let mut symbols: Vec<_> = self
            .engines
            .values()
            .map(SymbolEngine::snapshot_state_without_hash)
            .collect();
        symbols.sort_unstable_by_key(|snapshot| snapshot.symbol);
        ShardSnapshot {
            shard_id: self.shard_id,
            symbols,
        }
    }

    /// Snapshot after compacting every symbol's dedup queues.
    ///
    /// The node snapshots through this path: the saved insertion walk matches
    /// the live walk, so a restored shard hashes identically even after the
    /// tail re-admits an id that was dead at snapshot time.
    pub fn snapshot_state_without_hash_compacted(&mut self) -> ShardSnapshot {
        for engine in self.engines.values_mut() {
            engine.dedup.compact();
        }
        self.snapshot_state_without_hash()
    }

    pub fn from_snapshot(
        exchange: &ExchangeConfig,
        snapshot: ShardSnapshot,
    ) -> Result<Self, EngineError> {
        let expected: Vec<Symbol> = exchange
            .instruments
            .iter()
            .filter(|instrument| {
                instrument.symbol.partition(exchange.partitions) == snapshot.shard_id
            })
            .map(|instrument| instrument.symbol)
            .collect();
        let found: Vec<Symbol> = snapshot
            .symbols
            .iter()
            .map(|symbol| symbol.symbol)
            .collect();
        if expected != found {
            return Err(EngineError::Snapshot(format!(
                "shard {} snapshot symbols {:?} do not match configured symbols {:?}",
                snapshot.shard_id, found, expected
            )));
        }
        let mut engines = FxHashMap::default();
        for symbol_snapshot in snapshot.symbols {
            let expected_shard = symbol_snapshot.symbol.partition(exchange.partitions);
            if expected_shard != snapshot.shard_id {
                return Err(EngineError::Snapshot(format!(
                    "symbol {} belongs to shard {} under {} partitions, but the snapshot is for shard {}",
                    symbol_snapshot.symbol, expected_shard, exchange.partitions, snapshot.shard_id
                )));
            }
            let config = exchange
                .instrument(symbol_snapshot.symbol)
                .ok_or(EngineError::UnknownSymbol(
                    symbol_snapshot.symbol,
                    snapshot.shard_id,
                ))?
                .clone();
            let engine = SymbolEngine::from_snapshot(config, symbol_snapshot)?;
            engines.insert(engine.config.symbol, engine);
        }
        Ok(Self {
            shard_id: snapshot.shard_id,
            engines,
        })
    }

    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"whippet-shard-v1");
        hasher.update(&self.shard_id.to_le_bytes());
        // Hash each symbol engine directly instead of materializing full
        // snapshots. Symbol order is canonical (sorted), so the bytes match the
        // previous snapshot-based computation exactly.
        let mut engines: Vec<_> = self.engines.iter().collect();
        engines.sort_unstable_by_key(|(symbol, _)| **symbol);
        for (_, engine) in engines {
            hasher.update(&engine.state_hash());
        }
        *hasher.finalize().as_bytes()
    }

    /// Replay a sequence of commands from a known offset. Used by tests and by
    /// the `verify` CLI to prove determinism.
    pub fn replay<'a, I>(&mut self, commands: I) -> Result<EngineOutput, EngineError>
    where
        I: IntoIterator<Item = (u64, &'a Command)>,
    {
        let mut out = EngineOutput::with_capacity(16, 16);
        let mut last = EngineOutput::default();
        for (offset, command) in commands {
            self.process(command, offset, &mut out)?;
            std::mem::swap(&mut last, &mut out);
        }
        Ok(last)
    }
}

/// Fill each symbol's `state_hash` in a [`ShardSnapshot`] with
/// [`hash_symbol_snapshot`]. This is the off-shard counterpart to
/// [`ShardEngine::snapshot_state_without_hash`]: the shard thread produces the
/// unhashed snapshot, and the snapshot writer calls this before persisting.
pub fn fill_symbol_hashes(snapshot: &mut ShardSnapshot) {
    for symbol in &mut snapshot.symbols {
        symbol.state_hash = hash_symbol_snapshot(symbol);
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::config::InstrumentConfig;
    use whippet_core::protocol::MD_FLAG_HALTED;

    fn config() -> InstrumentConfig {
        let mut config = InstrumentConfig::new("BTC-USD", 100, 100, 200).unwrap();
        config.max_orders = 1024;
        config.snapshot_depth = 5;
        config
    }

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD").unwrap()
    }

    fn limit(side: Side, price: i64, qty: u64, id: u128, participant: u64) -> Command {
        Command::new_order(
            symbol(),
            side,
            OrderType::Limit,
            Price(price),
            Qty(qty),
            OrderId(id),
            ParticipantId(participant),
            1,
        )
    }

    fn process(engine: &mut SymbolEngine, command: &Command, offset: u64) -> EngineOutput {
        let mut out = EngineOutput::with_capacity(8, 8);
        engine.process(command, offset, &mut out);
        engine
            .check_output_invariants(command, offset, &out)
            .unwrap_or_else(|error| {
                panic!(
                    "output invariants after offset {offset} ({:?}): {error}",
                    command.kind
                )
            });
        out
    }

    /// Assert an output the checker must reject, named by the substring that
    /// makes the violation identifiable.
    #[track_caller]
    fn assert_output_violation(out: &EngineOutput, needle: &str) {
        match out.check_invariants() {
            Ok(()) => panic!("expected an output invariant violation mentioning {needle:?}"),
            Err(error) => assert!(
                error.contains(needle),
                "error {error:?} does not mention {needle:?}"
            ),
        }
    }

    /// Two resting asks, then a buy that takes 5 from the first and 5 from the
    /// second: the output has two fills, two trades, one remove, one reduce.
    fn fill_output() -> EngineOutput {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 100, 7, 2, 2), 1);
        process(&mut engine, &limit(Side::Buy, 100, 10, 3, 3), 2)
    }

    /// A buy IOC that takes 4 of an available 4 and has a 6-lot remainder: the
    /// output has a partial fill, a trade, a remove, and a terminal cancel.
    fn ioc_remainder_output() -> EngineOutput {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut ioc = limit(Side::Buy, 100, 10, 2, 2);
        ioc.order_type = OrderType::Ioc;
        process(&mut engine, &ioc, 1)
    }

    #[test]
    fn resting_order_emits_accepted_and_add() {
        let mut engine = SymbolEngine::new(config());
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        assert_eq!(out.executions[0].flags & FLAG_RESTED, FLAG_RESTED);
        assert_eq!(out.md.len(), 1);
        assert_eq!(out.md[0].kind, MdKind::Add);
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
    }

    #[test]
    fn aggressive_order_fills_price_time_priority() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 100, 7, 2, 2), 1);
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 3, 3), 2);

        let fills: Vec<_> = out
            .executions
            .iter()
            .filter(|exec| {
                matches!(exec.kind, ExecKind::PartiallyFilled | ExecKind::Filled)
                    && exec.maker_order_id != OrderId(0)
            })
            .collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].maker_order_id, OrderId(1));
        assert_eq!(fills[0].qty, Qty(5));
        assert_eq!(fills[1].maker_order_id, OrderId(2));
        assert_eq!(fills[1].qty, Qty(5));
        assert_eq!(fills[1].kind, ExecKind::Filled);
        assert_eq!(engine.book().best_ask(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Sell, 0), Qty(2));
    }

    #[test]
    fn partial_fill_rests_remainder() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 2, 2), 1);
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::PartiallyFilled)
        );
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Buy, 0), Qty(6));
    }

    #[test]
    fn ioc_cancels_remainder() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut ioc = limit(Side::Buy, 100, 10, 2, 2);
        ioc.order_type = OrderType::Ioc;
        let out = process(&mut engine, &ioc, 1);
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::Cancelled)
        );
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn fok_rejects_when_not_fillable() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut fok = limit(Side::Buy, 100, 10, 2, 2);
        fok.order_type = OrderType::Fok;
        let out = process(&mut engine, &fok, 1);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::FokUnfillable);
    }

    #[test]
    fn post_only_rejects_if_crossing() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut post = limit(Side::Buy, 100, 10, 2, 2);
        post.order_type = OrderType::PostOnly;
        let out = process(&mut engine, &post, 1);
        assert_eq!(out.executions[0].reason, RejectReason::PostOnlyWouldCross);
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn duplicate_order_id_is_rejected() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 1);
        assert_eq!(out.executions[0].reason, RejectReason::DuplicateOrderId);
        assert_eq!(engine.book().level_qty(Side::Buy, 0), Qty(10));
    }

    #[test]
    fn self_trade_prevention_cancels_resting() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        let out = process(&mut engine, &limit(Side::Buy, 100, 5, 2, 1), 1);
        assert!(out.executions.iter().any(|exec| {
            exec.kind == ExecKind::Cancelled && exec.reason == RejectReason::SelfTradePrevention
        }));
        assert_eq!(engine.book().best_ask(), None);
    }

    #[test]
    fn market_order_fills_and_cancels_remainder() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        let mut market = Command::new_order(
            symbol(),
            Side::Buy,
            OrderType::Market,
            Price::ZERO,
            Qty(10),
            OrderId(2),
            ParticipantId(2),
            2,
        );
        market.timestamp_ns = 2;
        let out = process(&mut engine, &market, 1);
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::PartiallyFilled)
        );
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::Cancelled)
        );
        assert_eq!(engine.book().best_ask(), None);
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn fok_rejects_self_trade_liquidity_without_touching_the_book() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        let mut fok = limit(Side::Buy, 100, 10, 2, 1);
        fok.order_type = OrderType::Fok;
        let out = process(&mut engine, &fok, 1);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::FokUnfillable);
        assert_eq!(engine.book().best_ask(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Sell, 0), Qty(10));
    }

    #[test]
    fn oversized_quantity_is_rejected() {
        let mut engine = SymbolEngine::new(config());
        let mut command = limit(Side::Buy, 100, 1, 1, 1);
        command.qty = Qty(MAX_ORDER_QTY + 1);
        let out = process(&mut engine, &command, 0);
        assert_eq!(out.executions[0].reason, RejectReason::InvalidQty);
        assert!(engine.book().is_empty());
    }

    #[test]
    fn zero_order_id_is_rejected() {
        let mut engine = SymbolEngine::new(config());
        let command = limit(Side::Buy, 100, 1, 0, 1);
        let out = process(&mut engine, &command, 0);
        assert_eq!(out.executions[0].reason, RejectReason::InvalidQty);
    }

    #[test]
    fn cancel_then_replace_loses_time_priority() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 100, 5, 2, 2), 1);
        process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2),
            2,
        );
        process(&mut engine, &limit(Side::Sell, 100, 5, 3, 1), 3);
        let out = process(&mut engine, &limit(Side::Buy, 100, 5, 4, 3), 4);
        let fill = out
            .executions
            .iter()
            .find(|exec| exec.kind == ExecKind::Filled)
            .expect("fill");
        assert_eq!(fill.maker_order_id, OrderId(2));
    }

    #[test]
    fn cancel_removes_order() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let cancel = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2);
        let out = process(&mut engine, &cancel, 1);
        assert_eq!(out.executions[0].kind, ExecKind::Cancelled);
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn trading_status_tracks_both_fields_and_md_flags() {
        let mut engine = SymbolEngine::new(config());
        assert_eq!(
            engine.trading_status(),
            TradingStatus::new(SessionState::Open, false)
        );
        process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(1), 1),
            0,
        );
        assert_eq!(
            engine.trading_status(),
            TradingStatus::new(SessionState::Open, true)
        );
        process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::PreOpen, ParticipantId(1), 2),
            1,
        );
        let status = engine.trading_status();
        assert!(!status.can_accept_new_orders());
        assert_eq!(status.session, SessionState::PreOpen);
        assert!(status.halted);
        let md_snapshot = engine.snapshot();
        assert_eq!(
            TradingStatus::from_md_flags(md_snapshot.flags),
            Ok(status),
            "the MD snapshot carries the same combination"
        );
    }

    #[test]
    fn halt_rejects_new_orders_and_keeps_the_book() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);

        let halt = Command::halt(symbol(), ParticipantId(9), 5);
        let out = process(&mut engine, &halt, 1);

        assert!(engine.is_halted());
        assert!(
            out.executions.is_empty(),
            "operator commands emit no execution report"
        );
        assert_eq!(out.md.len(), 1);
        let status = &out.md[0];
        assert_eq!(status.kind, MdKind::Status);
        assert_eq!(status.symbol, symbol());
        assert_eq!(
            status.flags,
            MD_FLAG_HALTED | SessionState::Open.to_md_flags(),
            "status carries the halt bit and the open-session bits"
        );
        assert_eq!(status.participant, ParticipantId(9));
        assert_eq!(status.order_id, OrderId(0));
        assert_eq!(status.side, Side::Buy);
        assert_eq!(status.price, Price::ZERO);
        assert_eq!(status.qty, Qty::ZERO);
        assert_eq!(status.cause_offset, 1, "status points at the halt command");
        assert_eq!(status.timestamp_ns, 5);

        // The book is untouched by the halt itself.
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Buy, 0), Qty(10));

        let out = process(&mut engine, &limit(Side::Buy, 99, 5, 2, 2), 2);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::Halted);
        assert!(out.md.is_empty(), "a rejection emits no market data");
        assert_eq!(engine.book().level_qty(Side::Buy, 0), Qty(10));
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
    }

    #[test]
    fn repeated_halt_is_idempotent_and_reemits_status() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(9), 2),
            1,
        );
        let bids_before = engine.book().best_bid();
        let asks_before = engine.book().best_ask();
        let resting_before = engine.book().level_qty(Side::Sell, 0);

        let out = process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(9), 3),
            2,
        );
        assert!(engine.is_halted());
        assert!(out.executions.is_empty());
        assert_eq!(
            out.md.len(),
            1,
            "an operator may need the status confirmed again"
        );
        assert_eq!(out.md[0].kind, MdKind::Status);
        assert_eq!(
            out.md[0].flags,
            MD_FLAG_HALTED | SessionState::Open.to_md_flags()
        );
        assert_eq!(out.md[0].cause_offset, 2);

        assert_eq!(engine.book().best_bid(), bids_before);
        assert_eq!(engine.book().best_ask(), asks_before);
        assert_eq!(
            engine.book().level_qty(Side::Sell, 0),
            resting_before,
            "a repeated halt must not change the book"
        );
    }

    #[test]
    fn cancel_is_allowed_while_halted_and_resume_accepts_orders() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(9), 2),
            1,
        );

        // Risk withdrawal stays open: participants can pull resting orders.
        let cancel = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 3);
        let out = process(&mut engine, &cancel, 2);
        assert_eq!(out.executions[0].kind, ExecKind::Cancelled);
        assert_eq!(out.md[0].kind, MdKind::Remove);
        assert_eq!(engine.book().best_bid(), None);
        assert!(
            engine.is_halted(),
            "a cancel does not resume the instrument"
        );

        let out = process(
            &mut engine,
            &Command::resume(symbol(), ParticipantId(9), 4),
            3,
        );
        assert!(!engine.is_halted());
        assert!(out.executions.is_empty());
        assert_eq!(out.md.len(), 1);
        assert_eq!(out.md[0].kind, MdKind::Status);
        assert_eq!(
            out.md[0].flags,
            SessionState::Open.to_md_flags(),
            "a resume clears only the halt bit"
        );
        assert_eq!(out.md[0].participant, ParticipantId(9));
        assert_eq!(out.md[0].cause_offset, 3);

        let out = process(&mut engine, &limit(Side::Buy, 100, 4, 2, 2), 4);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
    }

    #[test]
    fn halt_survives_snapshot_and_restore() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(9), 2),
            1,
        );
        let hash = engine.state_hash();

        let restored =
            SymbolEngine::from_snapshot(config(), engine.snapshot_state()).expect("restore");
        assert!(restored.is_halted(), "the halt flag is part of the state");
        assert_eq!(restored.state_hash(), hash);

        let mut restored_engine = restored;
        let out = process(&mut restored_engine, &limit(Side::Buy, 105, 1, 2, 2), 2);
        assert_eq!(out.executions[0].reason, RejectReason::Halted);

        let out = process(
            &mut restored_engine,
            &Command::resume(symbol(), ParticipantId(9), 3),
            3,
        );
        assert!(!restored_engine.is_halted());
        assert_eq!(out.md.len(), 1);
        assert_eq!(out.md[0].flags, SessionState::Open.to_md_flags());

        // A rejected id stays in the dedup window, so use a fresh one.
        let out = process(&mut restored_engine, &limit(Side::Buy, 100, 1, 3, 2), 4);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
    }

    #[test]
    fn halt_and_resume_replay_deterministically() {
        let commands = [
            (0, limit(Side::Sell, 105, 10, 1, 1)),
            (1, Command::halt(symbol(), ParticipantId(9), 2)),
            (2, limit(Side::Buy, 100, 4, 2, 2)),
            (3, Command::resume(symbol(), ParticipantId(9), 4)),
            (4, limit(Side::Buy, 104, 8, 3, 3)),
        ];
        let mut a = SymbolEngine::new(config());
        let mut b = SymbolEngine::new(config());
        for (offset, command) in &commands {
            let out_a = process(&mut a, command, *offset);
            let out_b = process(&mut b, command, *offset);
            assert_eq!(
                out_a.md.iter().map(|e| e.encode()).collect::<Vec<_>>(),
                out_b.md.iter().map(|e| e.encode()).collect::<Vec<_>>()
            );
        }
        assert_eq!(a.state_hash(), b.state_hash());
        assert!(!a.is_halted());
        assert_eq!(a.book().best_bid(), Some(Price(104)));
    }

    #[test]
    fn set_session_emits_status_and_gates_new_orders() {
        let mut engine = SymbolEngine::new(config());
        assert_eq!(
            engine.session(),
            SessionState::Open,
            "the pre-session default stays open"
        );
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);

        // Pre-open: a status event, no execution report, book untouched.
        let out = process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::PreOpen, ParticipantId(9), 2),
            1,
        );
        assert_eq!(engine.session(), SessionState::PreOpen);
        assert!(out.executions.is_empty());
        assert_eq!(out.md.len(), 1);
        assert_eq!(out.md[0].kind, MdKind::Status);
        assert_eq!(out.md[0].flags, SessionState::PreOpen.to_md_flags());
        assert_eq!(out.md[0].participant, ParticipantId(9));
        assert_eq!(out.md[0].cause_offset, 1);
        assert_eq!(engine.book().best_bid(), Some(Price(100)));

        let out = process(&mut engine, &limit(Side::Buy, 99, 1, 2, 2), 2);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::SessionNotOpen);
        assert!(out.md.is_empty(), "a rejection emits no market data");

        // Risk withdrawal is still allowed while the session is not open.
        let cancel = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 3);
        let out = process(&mut engine, &cancel, 3);
        assert_eq!(out.executions[0].kind, ExecKind::Cancelled);
        assert_eq!(out.md[0].kind, MdKind::Remove);
        assert_eq!(engine.book().best_bid(), None);
        assert_eq!(engine.session(), SessionState::PreOpen);

        // Closed rejects too, and reopening admits orders again.
        let out = process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::Closed, ParticipantId(9), 4),
            4,
        );
        assert_eq!(out.md[0].flags, SessionState::Closed.to_md_flags());
        assert_eq!(engine.session(), SessionState::Closed);
        let out = process(&mut engine, &limit(Side::Buy, 98, 1, 3, 2), 5);
        assert_eq!(out.executions[0].reason, RejectReason::SessionNotOpen);

        process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::Open, ParticipantId(9), 6),
            6,
        );
        let out = process(&mut engine, &limit(Side::Buy, 101, 1, 4, 2), 7);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        assert_eq!(engine.book().best_bid(), Some(Price(101)));
    }

    #[test]
    fn halt_is_checked_before_the_session() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::PreOpen, ParticipantId(9), 2),
            1,
        );
        let out = process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(9), 3),
            2,
        );
        assert_eq!(
            out.md[0].flags,
            MD_FLAG_HALTED | SessionState::PreOpen.to_md_flags(),
            "one status byte carries both bits"
        );

        // Halted and pre-open: the explicit operator stop wins the reason.
        let out = process(&mut engine, &limit(Side::Buy, 100, 1, 2, 2), 3);
        assert_eq!(out.executions[0].reason, RejectReason::Halted);

        // Resume clears only the halt; orders are still gated by the session.
        let out = process(
            &mut engine,
            &Command::resume(symbol(), ParticipantId(9), 4),
            4,
        );
        assert!(!engine.is_halted());
        assert_eq!(out.md[0].flags, SessionState::PreOpen.to_md_flags());
        let out = process(&mut engine, &limit(Side::Buy, 100, 1, 3, 2), 5);
        assert_eq!(out.executions[0].reason, RejectReason::SessionNotOpen);

        // Cancelling is allowed in every session, halted or not.
        let cancel = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 6);
        let out = process(&mut engine, &cancel, 6);
        assert_eq!(out.executions[0].kind, ExecKind::Cancelled);
    }

    #[test]
    fn session_is_part_of_the_state_hash() {
        let mut open = SymbolEngine::new(config());
        let mut closed = SymbolEngine::new(config());
        for engine in [&mut open, &mut closed] {
            process(engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        }
        process(
            &mut closed,
            &Command::set_session(symbol(), SessionState::Closed, ParticipantId(9), 2),
            1,
        );
        assert_eq!(open.book().best_bid(), closed.book().best_bid());
        assert_ne!(
            open.state_hash(),
            closed.state_hash(),
            "the session byte must enter the state hash"
        );
    }

    #[test]
    fn session_survives_snapshot_and_restore() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::Closed, ParticipantId(9), 2),
            1,
        );
        let hash = engine.state_hash();
        let snapshot = engine.snapshot_state();
        assert_eq!(snapshot.session, SessionState::Closed);
        assert_eq!(
            snapshot.book.orders.len(),
            1,
            "a session change does not cancel resting orders"
        );

        let mut restored = SymbolEngine::from_snapshot(config(), snapshot).expect("restore");
        assert_eq!(restored.session(), SessionState::Closed);
        assert_eq!(restored.state_hash(), hash);

        let out = process(&mut restored, &limit(Side::Buy, 105, 1, 2, 2), 2);
        assert_eq!(out.executions[0].reason, RejectReason::SessionNotOpen);

        process(
            &mut restored,
            &Command::set_session(symbol(), SessionState::Open, ParticipantId(9), 3),
            3,
        );
        let out = process(&mut restored, &limit(Side::Buy, 100, 1, 3, 2), 4);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        assert_eq!(restored.book().best_bid(), Some(Price(100)));
    }

    #[test]
    fn md_snapshot_flags_carry_halt_and_session_together() {
        let mut engine = SymbolEngine::new(config());
        process(
            &mut engine,
            &Command::set_session(symbol(), SessionState::Closed, ParticipantId(9), 1),
            0,
        );
        assert_eq!(
            engine.snapshot().flags,
            SessionState::Closed.to_md_flags(),
            "a snapshot alone tells a subscriber the session"
        );
        process(
            &mut engine,
            &Command::halt(symbol(), ParticipantId(9), 2),
            1,
        );
        assert_eq!(
            engine.snapshot().flags,
            MD_FLAG_HALTED | SessionState::Closed.to_md_flags()
        );
        assert_eq!(
            SessionState::from_md_flags(engine.snapshot().flags).unwrap(),
            SessionState::Closed
        );
    }

    #[test]
    fn session_changes_replay_deterministically() {
        let commands = [
            (0, limit(Side::Sell, 105, 10, 1, 1)),
            (
                1,
                Command::set_session(symbol(), SessionState::PreOpen, ParticipantId(9), 2),
            ),
            (2, limit(Side::Buy, 100, 4, 2, 2)),
            (
                3,
                Command::cancel(symbol(), OrderId(1), ParticipantId(1), 4),
            ),
            (
                4,
                Command::set_session(symbol(), SessionState::Open, ParticipantId(9), 4),
            ),
            (5, limit(Side::Buy, 104, 8, 3, 3)),
        ];
        let mut a = SymbolEngine::new(config());
        let mut b = SymbolEngine::new(config());
        for (offset, command) in &commands {
            let out_a = process(&mut a, command, *offset);
            let out_b = process(&mut b, command, *offset);
            assert_eq!(
                out_a
                    .executions
                    .iter()
                    .map(|e| e.encode())
                    .collect::<Vec<_>>(),
                out_b
                    .executions
                    .iter()
                    .map(|e| e.encode())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                out_a.md.iter().map(|e| e.encode()).collect::<Vec<_>>(),
                out_b.md.iter().map(|e| e.encode()).collect::<Vec<_>>()
            );
        }
        assert_eq!(a.state_hash(), b.state_hash());
        assert_eq!(a.session(), SessionState::Open);
        assert_eq!(a.book().best_bid(), Some(Price(104)));
    }

    #[test]
    fn undecodable_session_command_changes_nothing() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let mut command = Command::set_session(symbol(), SessionState::Closed, ParticipantId(9), 2);
        command.flags = 0x0102;
        let out = process(&mut engine, &command, 1);
        assert_eq!(
            engine.session(),
            SessionState::Open,
            "an undecodable session must not silently close or reopen a market"
        );
        assert_eq!(out.md[0].kind, MdKind::Status);
        assert_eq!(out.md[0].flags, SessionState::Open.to_md_flags());

        // The book is untouched and new orders still work.
        let out = process(&mut engine, &limit(Side::Buy, 101, 1, 2, 2), 2);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
    }

    #[test]
    fn state_hash_is_deterministic_and_replayable() {
        let commands: Vec<(u64, Command)> = vec![
            (0, limit(Side::Sell, 105, 10, 1, 1)),
            (1, limit(Side::Sell, 104, 5, 2, 2)),
            (2, limit(Side::Buy, 100, 7, 3, 3)),
            (3, limit(Side::Buy, 104, 8, 4, 4)),
            (4, limit(Side::Buy, 103, 3, 5, 5)),
        ];
        let mut a = SymbolEngine::new(config());
        let mut b = SymbolEngine::new(config());
        for (offset, command) in &commands {
            process(&mut a, command, *offset);
            process(&mut b, command, *offset);
        }
        assert_eq!(a.state_hash(), b.state_hash());
    }

    #[test]
    fn snapshot_restores_exact_state_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 100, 7, 2, 2), 1);
        let snapshot = engine.snapshot_state();
        let restored = SymbolEngine::from_snapshot(config(), snapshot).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
        assert_eq!(restored.book().best_ask(), engine.book().best_ask());
    }

    #[test]
    fn a_reordered_terminal_queue_changes_the_snapshot_hash() {
        // The terminal queue is not derivable from the map: it decides which
        // id is evicted next. Two cancelled orders give a queue with a real
        // order to reorder. If the queue were not hashed, a snapshot whose
        // indices were shuffled at rest would verify and restore a table that
        // evicts differently from a continuous replay.
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 90, 1, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 91, 1, 2, 2), 1);
        for (offset, id) in [(2u64, 1u128), (3, 2)] {
            let cancel = Command::cancel(symbol(), OrderId(id), ParticipantId(id as u64), offset);
            process(&mut engine, &cancel, offset);
        }
        let snapshot = engine.snapshot_state_without_hash_compacted();
        assert_eq!(snapshot.dedup_terminal_idx.len(), 2);
        assert_eq!(hash_symbol_snapshot(&snapshot), engine.state_hash());

        let mut reordered = snapshot.clone();
        reordered.dedup_terminal_idx.reverse();
        assert_ne!(
            hash_symbol_snapshot(&reordered),
            hash_symbol_snapshot(&snapshot),
            "a reordered terminal queue must change the snapshot hash"
        );
        reordered.state_hash = hash_symbol_snapshot(&reordered);
        let restored = SymbolEngine::from_snapshot(config(), reordered)
            .expect("both orderings are internally valid");
        assert_ne!(
            restored.state_hash(),
            engine.state_hash(),
            "verify must be able to see the reordered queue"
        );
    }

    #[test]
    fn malformed_terminal_indices_are_rejected() {
        // Terminal indices are not hashed, so a corrupt list must fail closed
        // here; otherwise a restored table could evict differently from a
        // continuous replay while every hash still matches.
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 105, 4, 2, 2), 1);
        let mut snapshot = engine.snapshot_state_without_hash();
        // Out of range, and an index that points at a non-terminal entry.
        snapshot.dedup_terminal_idx = vec![u32::MAX, 0];
        snapshot.state_hash = hash_symbol_snapshot(&snapshot);
        let err = match SymbolEngine::from_snapshot(config(), snapshot) {
            Ok(_) => panic!("a corrupt terminal queue must be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, EngineError::Snapshot(_)), "{err:?}");
    }

    #[test]
    fn state_and_md_snapshot_intervals_are_independent() {
        let mut engine = SymbolEngine::new(config())
            .with_snapshot_interval(10)
            .with_md_snapshot_interval(100);
        let mut out = EngineOutput::with_capacity(8, 8);
        let mut state_due_at = Vec::new();
        let mut md_snapshot_at = Vec::new();

        for offset in 0..100u64 {
            out.clear();
            engine.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 1, 1),
                offset,
                &mut out,
            );
            if out.state_snapshot_due {
                state_due_at.push(engine.commands());
            }
            if !out.snapshots.is_empty() {
                assert_eq!(out.snapshots.len(), 1, "one MD snapshot per due command");
                md_snapshot_at.push(engine.commands());
            }
        }

        assert_eq!(
            state_due_at,
            (1..=10).map(|n| n * 10).collect::<Vec<u64>>(),
            "state persistence is due every ten commands"
        );
        assert_eq!(
            md_snapshot_at,
            vec![100],
            "market-data snapshots follow the separate, slower interval"
        );
    }

    #[test]
    fn md_snapshot_interval_setters_do_not_touch_state_interval() {
        let mut engine = SymbolEngine::new(config());
        engine.set_snapshot_interval(1_000);
        engine.set_md_snapshot_interval(3);
        let mut out = EngineOutput::with_capacity(8, 8);
        let mut state_due = 0usize;
        let mut md_snapshots = 0usize;

        for offset in 0..6u64 {
            out.clear();
            engine.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 1, 1),
                offset,
                &mut out,
            );
            state_due += usize::from(out.state_snapshot_due);
            md_snapshots += out.snapshots.len();
        }

        assert_eq!(state_due, 0, "state interval of 1000 was not reached");
        assert_eq!(md_snapshots, 2, "MD snapshots at commands 3 and 6");
    }

    #[test]
    fn snapshot_intervals_do_not_affect_state_hash() {
        let commands = [
            limit(Side::Sell, 105, 10, 1, 1),
            limit(Side::Buy, 100, 7, 2, 2),
            limit(Side::Buy, 105, 3, 3, 3),
            limit(Side::Sell, 104, 4, 4, 4),
        ];
        let mut eager = SymbolEngine::new(config())
            .with_snapshot_interval(1)
            .with_md_snapshot_interval(1);
        let mut lazy = SymbolEngine::new(config())
            .with_snapshot_interval(u64::MAX)
            .with_md_snapshot_interval(u64::MAX);
        let mut out = EngineOutput::with_capacity(8, 8);
        for (offset, command) in commands.iter().enumerate() {
            eager.process(command, offset as u64, &mut out);
            out.clear();
            lazy.process(command, offset as u64, &mut out);
            out.clear();
        }
        assert_eq!(eager.state_hash(), lazy.state_hash());
    }

    #[test]
    fn snapshot_cadence_resumes_from_the_restored_command_count() {
        let mut engine = SymbolEngine::new(config())
            .with_snapshot_interval(10)
            .with_md_snapshot_interval(10);
        let mut out = EngineOutput::with_capacity(8, 8);
        for offset in 0..7u64 {
            engine.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 1, 1),
                offset,
                &mut out,
            );
            out.clear();
        }
        assert_eq!(engine.commands(), 7);

        let mut restored =
            SymbolEngine::from_snapshot(config(), engine.snapshot_state()).expect("restore");
        restored.set_snapshot_interval(10);
        restored.set_md_snapshot_interval(10);

        // `commands` is 7, so command 10 is the next due one: three commands
        // away, exactly as `commands % interval` would have said before the
        // countdown replaced the division.
        for offset in 7..9u64 {
            restored.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 100, 1),
                offset,
                &mut out,
            );
            assert!(!out.state_snapshot_due);
            assert!(out.snapshots.is_empty());
            out.clear();
        }
        restored.process(&limit(Side::Buy, 100, 1, 200, 1), 9, &mut out);
        assert!(out.state_snapshot_due);
        assert_eq!(out.snapshots.len(), 1);
        assert_eq!(restored.commands(), 10);
    }

    #[test]
    fn shard_state_hash_matches_sorted_symbol_hashes() {
        let mut eth_config = config();
        eth_config.symbol = Symbol::new("ETH-USD").unwrap();
        let exchange = ExchangeConfig {
            venue: "test".into(),
            partitions: 2,
            mode: whippet_core::config::Mode::Durable,
            instruments: vec![config(), eth_config],
        };
        let btc = symbol();
        let eth = Symbol::new("ETH-USD").unwrap();
        let shard = btc.partition(exchange.partitions);
        assert_eq!(
            shard,
            eth.partition(exchange.partitions),
            "this test needs both symbols on the same shard"
        );

        let mut engine = ShardEngine::new(shard, &exchange);
        let mut out = EngineOutput::with_capacity(8, 8);
        let commands = [
            (btc, Side::Buy, 100i64),
            (eth, Side::Sell, 101),
            (btc, Side::Sell, 102),
            (eth, Side::Buy, 99),
        ];
        for (offset, (sym, side, price)) in commands.into_iter().enumerate() {
            let command = Command::new_order(
                sym,
                side,
                OrderType::Limit,
                Price(price),
                Qty(5),
                OrderId(offset as u128 + 1),
                ParticipantId(1),
                offset as u64,
            );
            engine.process(&command, offset as u64, &mut out).unwrap();
            out.clear();
        }

        // The shard hash must be exactly the sorted per-symbol state hashes.
        let mut symbols: Vec<Symbol> = engine.symbols().collect();
        symbols.sort_unstable();
        let mut expected = blake3::Hasher::new();
        expected.update(b"whippet-shard-v1");
        expected.update(&shard.to_le_bytes());
        for sym in symbols {
            let state_hash = engine
                .engine(sym)
                .expect("symbol engine")
                .snapshot_state()
                .state_hash;
            expected.update(&state_hash);
        }
        assert_eq!(engine.state_hash(), *expected.finalize().as_bytes());
    }

    #[test]
    fn book_never_crosses() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 106, 4, 2, 2), 1);
        let best_bid = engine.book().best_bid();
        let best_ask = engine.book().best_ask();
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            assert!(bid.0 < ask.0, "crossed book: {bid:?} >= {ask:?}");
        }
    }

    #[test]
    fn snapshot_hash_reproduces_live_state_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 105, 5, 2, 2), 1);
        process(&mut engine, &limit(Side::Buy, 105, 12, 3, 3), 2);
        process(&mut engine, &limit(Side::Buy, 100, 6, 4, 4), 3);
        let snapshot = engine.snapshot_state_without_hash();
        assert_eq!(snapshot.state_hash, [0; 32], "sentinel is left zeroed");
        assert_eq!(hash_symbol_snapshot(&snapshot), engine.state_hash());
    }

    #[test]
    fn snapshot_hash_matches_full_snapshot_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 104, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 104, 7, 2, 2), 1);
        let full = engine.snapshot_state();
        assert_eq!(hash_symbol_snapshot(&full), full.state_hash);
    }

    #[test]
    fn restored_engine_verifies_off_thread_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 100, 7, 2, 2), 1);
        let mut snapshot = engine.snapshot_state_without_hash();
        snapshot.state_hash = hash_symbol_snapshot(&snapshot);
        let restored = SymbolEngine::from_snapshot(config(), snapshot).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
    }

    #[test]
    fn fill_symbol_hashes_matches_per_symbol_snapshots() {
        let mut eth_config = config();
        eth_config.symbol = Symbol::new("ETH-USD").unwrap();
        let exchange = ExchangeConfig {
            venue: "test".into(),
            partitions: 2,
            mode: whippet_core::config::Mode::Durable,
            instruments: vec![config(), eth_config],
        };
        let btc = symbol();
        let eth = Symbol::new("ETH-USD").unwrap();
        let shard = btc.partition(exchange.partitions);
        assert_eq!(shard, eth.partition(exchange.partitions));

        let mut engine = ShardEngine::new(shard, &exchange);
        let mut out = EngineOutput::with_capacity(8, 8);
        let commands = [
            (btc, Side::Buy, 100i64),
            (eth, Side::Sell, 101),
            (btc, Side::Sell, 102),
            (eth, Side::Buy, 99),
        ];
        for (offset, (sym, side, price)) in commands.into_iter().enumerate() {
            let command = Command::new_order(
                sym,
                side,
                OrderType::Limit,
                Price(price),
                Qty(5),
                OrderId(offset as u128 + 1),
                ParticipantId(1),
                offset as u64,
            );
            engine.process(&command, offset as u64, &mut out).unwrap();
            out.clear();
        }

        let mut unhashed = engine.snapshot_state_without_hash();
        fill_symbol_hashes(&mut unhashed);
        assert_eq!(unhashed.shard_id, shard);
        for symbol_snapshot in &unhashed.symbols {
            let expected = engine
                .engine(symbol_snapshot.symbol)
                .expect("symbol engine")
                .snapshot_state()
                .state_hash;
            assert_eq!(symbol_snapshot.state_hash, expected);
        }
    }

    #[test]
    fn check_invariants_accepts_a_mixed_flow() {
        let mut engine = SymbolEngine::new(config());
        engine
            .check_invariants()
            .expect("a fresh engine is consistent");

        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        engine.check_invariants().expect("a resting maker");

        process(&mut engine, &limit(Side::Buy, 100, 8, 2, 2), 1);
        engine
            .check_invariants()
            .expect("a fill plus a resting remainder");

        process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(2), ParticipantId(2), 2),
            2,
        );
        engine.check_invariants().expect("a cancelled remainder");

        // Out of band: a create-time rejection, which is terminal and absent
        // from the book.
        process(&mut engine, &limit(Side::Buy, 99, 1, 3, 3), 3);
        engine.check_invariants().expect("a rejected order");

        // Self-trade prevention cancels the resting order.
        process(&mut engine, &limit(Side::Sell, 110, 4, 4, 4), 4);
        process(&mut engine, &limit(Side::Buy, 110, 4, 5, 4), 5);
        engine
            .check_invariants()
            .expect("after self-trade prevention");
    }

    #[test]
    fn full_book_after_acceptance_is_recorded_as_cancelled() {
        // A BookFull failure happens after the order was accepted and reported,
        // so the dedup record must be terminal `Cancelled`, matching the
        // `Cancelled`/`Internal` report the client sees. `Rejected` is reserved
        // for the creation path, where no accepted report was emitted.
        let mut config = config();
        config.max_orders = 1;
        let mut engine = SymbolEngine::new(config);
        process(&mut engine, &limit(Side::Buy, 100, 5, 1, 1), 0);
        assert_eq!(engine.book().order_count(), 1);

        let out = process(&mut engine, &limit(Side::Buy, 100, 5, 2, 2), 1);
        let last = out.executions.last().expect("a report");
        assert_eq!(last.kind, ExecKind::Cancelled);
        assert_eq!(last.reason, RejectReason::Internal);
        assert_eq!(last.order_id, OrderId(2));
        assert_eq!(
            last.flags, FLAG_RESTED,
            "a passive order that could not rest keeps its admission role"
        );
        assert_eq!(
            engine.dedup().status(OrderId(2)),
            Some(DedupStatus::Cancelled)
        );
        assert_eq!(
            engine.dedup().status(OrderId(1)),
            Some(DedupStatus::Resting)
        );
        engine
            .check_invariants()
            .expect("an internal failure still leaves a consistent engine");
    }

    #[test]
    fn check_invariants_detects_a_resting_id_whose_order_is_gone() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        // Remove the book order behind the table's back: only the cross-check
        // can see the split, the individual structures stay valid.
        engine
            .book
            .cancel(OrderId(1), None)
            .expect("the order rests");
        let error = engine
            .check_invariants()
            .expect_err("dedup still says Resting");
        assert!(error.contains("Resting"), "{error}");
    }

    #[test]
    fn check_invariants_detects_a_terminal_id_still_in_the_book() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        // A legal lifecycle transition, applied while the order still rests:
        // exactly the split the cross-check exists to catch.
        engine.dedup.update(OrderId(1), DedupStatus::Filled);
        let error = engine
            .check_invariants()
            .expect_err("a terminal id is still resting");
        assert!(
            error.contains("Filled") || error.contains("still in the book"),
            "{error}"
        );
    }

    #[test]
    fn check_invariants_detects_a_dangling_accepted_entry() {
        let mut engine = SymbolEngine::new(config());
        // `Accepted` is only legal inside a single `process` call. The dedup
        // table alone accepts this creation, so only the engine cross-check
        // rejects it.
        engine.dedup.insert(
            OrderId(7),
            DedupEntry {
                status: DedupStatus::Accepted,
                input_offset: 0,
                timestamp_ns: 0,
                seq: Seq(1),
            },
        );
        let error = engine
            .check_invariants()
            .expect_err("Accepted is transient");
        assert!(error.contains("Accepted"), "{error}");
    }

    fn resting_output() -> EngineOutput {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0)
    }

    fn rejection_output() -> EngineOutput {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        // Same id: the second order is a duplicate rejection.
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 1)
    }

    fn stp_output() -> EngineOutput {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 100, 5, 2, 1), 1)
    }

    #[test]
    fn output_check_accepts_a_rejection_with_a_cadence_snapshot() {
        let mut engine = SymbolEngine::new(config()).with_md_snapshot_interval(1);
        let mut out = EngineOutput::with_capacity(4, 4);
        let command = limit(Side::Buy, 100, 10, 1, 1);
        engine.process(&command, 0, &mut out);
        engine
            .check_output_invariants(&command, 0, &out)
            .expect("first command");
        out.clear();

        // A duplicate id is rejected, and the cadence still emits a snapshot:
        // snapshots are not market-data deltas and are allowed with a rejection.
        engine.process(&command, 1, &mut out);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert!(out.md.is_empty());
        assert_eq!(out.snapshots.len(), 1);
        engine
            .check_output_invariants(&command, 1, &out)
            .expect("a rejection may carry a cadence snapshot");
    }

    #[test]
    fn output_check_accepts_real_flows() {
        for out in [
            resting_output(),
            fill_output(),
            ioc_remainder_output(),
            rejection_output(),
            stp_output(),
        ] {
            out.check_invariants()
                .unwrap_or_else(|error| panic!("a real output was rejected: {error}"));
        }
    }

    #[test]
    fn output_check_rejects_a_non_increasing_execution_seq() {
        let mut out = fill_output();
        out.executions[1].seq = out.executions[0].seq;
        assert_output_violation(&out, "execution seq");
    }

    #[test]
    fn output_check_rejects_a_zero_execution_seq() {
        let mut out = fill_output();
        out.executions[0].seq = Seq(0);
        assert_output_violation(&out, "seq 0");
    }

    #[test]
    fn output_check_rejects_a_non_increasing_md_seq() {
        let mut out = fill_output();
        out.md[1].seq = out.md[0].seq;
        assert_output_violation(&out, "market-data seq");
    }

    #[test]
    fn output_check_rejects_a_foreign_cause_offset() {
        let mut out = resting_output();
        out.md[0].cause_offset += 1;
        assert_output_violation(&out, "cause_offset");
    }

    #[test]
    fn output_check_rejects_a_foreign_execution_timestamp() {
        let mut out = fill_output();
        out.executions[1].timestamp_ns += 1;
        assert_output_violation(&out, "timestamp");
    }

    #[test]
    fn output_check_rejects_a_fill_before_its_acceptance() {
        let mut out = fill_output();
        out.executions.swap(0, 1);
        // Keep the reports' sequence increasing so only the ordering rule fires.
        out.executions[0].seq = Seq(1);
        out.executions[1].seq = Seq(2);
        assert_output_violation(&out, "no Accepted");
    }

    #[test]
    fn output_check_rejects_a_second_acceptance() {
        let mut out = resting_output();
        let mut duplicate = out.executions[0].clone();
        duplicate.seq = Seq(out.executions[0].seq.0 + 1);
        out.executions.push(duplicate);
        assert_output_violation(&out, "Accepted twice");
    }

    #[test]
    fn output_check_rejects_a_rejection_mixed_with_a_fill() {
        let mut out = rejection_output();
        let mut fill = fill_output().executions[1].clone();
        fill.seq = Seq(out.executions[0].seq.0 + 1);
        // Match the rejection's provenance so the mixing rule, not the
        // one-command rule, is what fires.
        fill.cause_offset = out.executions[0].cause_offset;
        fill.timestamp_ns = out.executions[0].timestamp_ns;
        out.executions.push(fill);
        assert_output_violation(&out, "Rejected");
    }

    #[test]
    fn output_check_rejects_a_fill_after_a_terminal_report() {
        let mut out = ioc_remainder_output();
        let mut late = out.executions[1].clone();
        late.seq = Seq(out.executions.last().expect("a cancel").seq.0 + 1);
        out.executions.push(late);
        assert_output_violation(&out, "terminal");
    }

    #[test]
    fn output_check_rejects_a_zero_quantity_fill() {
        let mut out = fill_output();
        out.executions[1].qty = Qty::ZERO;
        assert_output_violation(&out, "zero quantity");
    }

    #[test]
    fn output_check_rejects_a_zero_price_fill() {
        let mut out = fill_output();
        out.executions[1].price = Price::ZERO;
        assert_output_violation(&out, "non-positive price");
    }

    #[test]
    fn output_check_rejects_a_fill_on_the_wrong_side() {
        let mut out = fill_output();
        out.executions[1].side = Side::Sell;
        assert_output_violation(&out, "taker's side");
    }

    #[test]
    fn output_check_rejects_a_fill_without_the_taker_flag() {
        let mut out = fill_output();
        out.executions[1].flags = 0;
        assert_output_violation(&out, "FLAG_TAKER");
    }

    #[test]
    fn output_check_rejects_a_fill_without_a_maker_id() {
        let mut out = fill_output();
        out.executions[1].maker_order_id = OrderId(0);
        assert_output_violation(&out, "maker_order_id 0");
    }

    #[test]
    fn output_check_rejects_a_fill_above_the_accepted_quantity() {
        let mut out = fill_output();
        out.executions[1].qty = Qty(100);
        assert_output_violation(&out, "above the accepted quantity");
    }

    #[test]
    fn output_check_rejects_a_fill_with_no_trade_event() {
        let mut out = fill_output();
        out.md.remove(0);
        assert_output_violation(&out, "matching Trade");
    }

    #[test]
    fn output_check_rejects_an_extra_trade_for_a_fill() {
        let mut out = fill_output();
        let mut extra = out.md[0].clone();
        extra.seq = Seq(out.md.last().expect("market data").seq.0 + 1);
        out.md.push(extra);
        assert_output_violation(&out, "matching Trade");
    }

    #[test]
    fn output_check_rejects_a_trade_without_a_fill() {
        let mut out = fill_output();
        let mut extra = out.md[0].clone();
        extra.seq = Seq(out.md.last().expect("market data").seq.0 + 1);
        extra.qty = Qty(999);
        out.md.push(extra);
        assert_output_violation(&out, "no matching fill");
    }

    #[test]
    fn output_check_rejects_a_trade_with_the_wrong_price() {
        let mut out = fill_output();
        out.md[0].price = Price(101);
        assert_output_violation(&out, "matching Trade");
    }

    #[test]
    fn output_check_rejects_an_add_after_a_remove() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        let mut out = process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2),
            1,
        );
        let mut add = out.md[0].clone();
        add.kind = MdKind::Add;
        add.qty = Qty(10);
        add.seq = Seq(out.md[0].seq.0 + 1);
        out.md.push(add);
        assert_output_violation(&out, "Remove/Reduce");
    }

    #[test]
    fn output_check_rejects_a_zero_quantity_add() {
        let mut out = resting_output();
        out.md[0].qty = Qty::ZERO;
        assert_output_violation(&out, "zero quantity");
    }

    #[test]
    fn output_check_rejects_a_zero_id_add() {
        let mut out = resting_output();
        out.md[0].order_id = OrderId(0);
        assert_output_violation(&out, "order id 0");
    }

    #[test]
    fn output_check_rejects_a_zero_id_remove() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let mut out = process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2),
            1,
        );
        out.md[0].order_id = OrderId(0);
        assert_output_violation(&out, "order id 0");
    }

    #[test]
    fn output_check_rejects_a_growing_reduce() {
        let mut out = resting_output();
        let mut reduce = out.md[0].clone();
        reduce.kind = MdKind::Reduce;
        reduce.qty = Qty(11);
        reduce.seq = Seq(out.md[0].seq.0 + 1);
        out.md.push(reduce);
        assert_output_violation(&out, "grows the remaining quantity");
    }

    #[test]
    fn output_check_rejects_a_rejection_with_quantity_state() {
        let mut out = rejection_output();
        out.executions[0].qty = Qty(1);
        assert_output_violation(&out, "no quantity");

        let mut out = rejection_output();
        out.executions[0].remaining = Qty(1);
        assert_output_violation(&out, "leaves nothing");

        let mut out = rejection_output();
        out.executions[0].maker_order_id = OrderId(1);
        assert_output_violation(&out, "no maker leg");
    }

    #[test]
    fn output_check_rejects_a_rejection_with_market_data() {
        let mut out = rejection_output();
        let mut event = resting_output().md[0].clone();
        event.seq = Seq(1);
        event.cause_offset = out.executions[0].cause_offset;
        event.timestamp_ns = out.executions[0].timestamp_ns;
        out.md.push(event);
        assert_output_violation(&out, "market data");
    }

    #[test]
    fn output_check_rejects_combined_maker_and_taker_flags() {
        let mut out = stp_output();
        let maker = out
            .executions
            .iter_mut()
            .find(|exec| exec.reason == RejectReason::SelfTradePrevention)
            .expect("an STP cancel");
        maker.flags = FLAG_MAKER | FLAG_TAKER;
        assert_output_violation(&out, "more than one role flag");
    }

    #[test]
    fn output_check_rejects_an_stp_cancel_without_the_maker_flag() {
        let mut out = stp_output();
        let maker = out
            .executions
            .iter_mut()
            .find(|exec| exec.reason == RejectReason::SelfTradePrevention)
            .expect("an STP cancel");
        maker.flags = 0;
        assert_output_violation(&out, "without FLAG_MAKER");
    }

    #[test]
    fn output_check_rejects_an_acceptance_without_a_role() {
        let mut out = resting_output();
        out.executions[0].flags = 0;
        assert_output_violation(&out, "exactly one of FLAG_TAKER or FLAG_RESTED");
    }

    #[test]
    fn output_check_rejects_a_cancel_role_that_disagrees_with_the_acceptance() {
        // A book-full failure happens after acceptance: the terminal report must
        // keep the passive role the acceptance reported, not claim a taker leg.
        let mut config = config();
        config.max_orders = 1;
        let mut engine = SymbolEngine::new(config);
        process(&mut engine, &limit(Side::Buy, 100, 5, 1, 1), 0);
        let mut out = process(&mut engine, &limit(Side::Buy, 100, 5, 2, 2), 1);
        let last = out.executions.last_mut().expect("a terminal cancel");
        assert_eq!(last.kind, ExecKind::Cancelled);
        last.flags = FLAG_TAKER;
        assert_output_violation(&out, "must agree with its acceptance");
    }

    #[test]
    fn cancel_report_side_matches_the_removed_order_and_the_remove_event() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        let out = process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2),
            1,
        );
        assert_eq!(out.executions[0].kind, ExecKind::Cancelled);
        assert_eq!(out.executions[0].side, Side::Sell);
        assert_eq!(out.md[0].kind, MdKind::Remove);
        assert_eq!(out.md[0].side, Side::Sell);

        let mut corrupt = out.clone();
        corrupt.executions[0].side = Side::Buy;
        assert_output_violation(&corrupt, "trigger");
    }

    #[test]
    fn output_check_rejects_a_fill_without_a_maker_side_event() {
        let mut out = fill_output();
        // Drop the Remove for the first maker; the second fill keeps its Reduce.
        let first_maker = out
            .executions
            .iter()
            .find(|exec| exec.kind == ExecKind::PartiallyFilled)
            .expect("a partial fill")
            .maker_order_id;
        out.md
            .retain(|event| event.kind != MdKind::Remove || event.order_id != first_maker);
        assert_output_violation(&out, "exactly one maker-side event");
    }

    #[test]
    fn output_check_rejects_a_remainder_without_a_terminal_report() {
        let mut out = ioc_remainder_output();
        out.executions.pop();
        assert_output_violation(&out, "accounts for");
    }

    #[test]
    fn output_check_rejects_a_second_terminal_report() {
        let mut out = ioc_remainder_output();
        let mut duplicate = out.executions.last().expect("a cancel").clone();
        duplicate.seq = Seq(duplicate.seq.0 + 1);
        out.executions.push(duplicate);
        assert_output_violation(&out, "second terminal");
    }

    #[test]
    fn output_check_rejects_an_stp_cancel_without_a_remove_event() {
        let mut out = stp_output();
        out.md.retain(|event| event.kind != MdKind::Remove);
        assert_output_violation(&out, "no Remove market-data event");
    }

    #[test]
    fn output_check_rejects_a_client_cancel_without_a_remove_event() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let mut out = process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2),
            1,
        );
        out.md.clear();
        assert_output_violation(&out, "has no Remove market-data event");
    }

    #[test]
    fn two_runs_of_the_same_flow_emit_identical_executions_and_md() {
        let mut commands = vec![
            limit(Side::Sell, 100, 5, 1, 1),
            limit(Side::Sell, 100, 7, 2, 2),
            limit(Side::Buy, 100, 10, 3, 3),
            Command::cancel(symbol(), OrderId(1), ParticipantId(1), 4),
            limit(Side::Sell, 105, 4, 4, 4),
            // Self-trade prevention: the taker removes the resting ask.
            limit(Side::Buy, 105, 4, 5, 4),
            Command::halt(symbol(), ParticipantId(9), 6),
            Command::resume(symbol(), ParticipantId(9), 7),
        ];
        let mut ioc = limit(Side::Buy, 106, 9, 6, 5);
        ioc.order_type = OrderType::Ioc;
        commands.push(ioc);

        // The flow mixes monotone timestamps (operator commands) with the test
        // helper's fixed stamp of 1, so the venue clock can lead the command.
        let mut first = SymbolEngine::new(config());
        let mut second = SymbolEngine::new(config());
        for (offset, command) in commands.iter().enumerate() {
            let out_first = process(&mut first, command, offset as u64);
            let out_second = process(&mut second, command, offset as u64);
            assert_eq!(
                out_first.executions, out_second.executions,
                "offset {offset}: execution reports (including seq) diverged"
            );
            assert_eq!(
                out_first.md, out_second.md,
                "offset {offset}: market data (including seq) diverged"
            );
        }
        assert_eq!(first.state_hash(), second.state_hash());
    }

    fn set_risk(participant: u64, max_net_qty: u64, max_notional_cents: i64) -> Command {
        Command::set_risk(
            symbol(),
            ParticipantId(participant),
            max_net_qty,
            max_notional_cents,
            1,
        )
    }

    #[test]
    fn risk_limit_rejects_before_acceptance_and_leaves_no_trace() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(1, 5, 0), 0);

        let out = process(&mut engine, &limit(Side::Buy, 100, 6, 1, 1), 1);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);
        assert!(
            out.md.is_empty(),
            "a creation rejection mutates no market data"
        );
        assert_eq!(engine.book().order_count(), 0);
        assert_eq!(
            engine.dedup().status(OrderId(1)),
            Some(DedupStatus::Rejected)
        );
        engine
            .check_invariants()
            .expect("a rejected order leaves no exposure");
    }

    #[test]
    fn risk_exposure_tracks_resting_fills_and_cancels() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(2, 10, 0), 0);
        // Participant 1 provides liquidity and is unconstrained.
        process(&mut engine, &limit(Side::Sell, 150, 10, 10, 1), 1);

        // A taker buy of 4 leaves a 4-lot net long; a resting buy of 7 would
        // make the worst case 11, one over the bound.
        let out = process(&mut engine, &limit(Side::Buy, 150, 4, 11, 2), 2);
        assert_eq!(out.executions.last().unwrap().kind, ExecKind::Filled);
        let out = process(&mut engine, &limit(Side::Buy, 120, 7, 12, 2), 3);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);

        // A resting buy of 6 reaches the bound exactly and is allowed.
        let out = process(&mut engine, &limit(Side::Buy, 120, 6, 13, 2), 4);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        assert_eq!(out.executions[0].remaining, Qty(6));
        engine
            .check_invariants()
            .expect("open buy is mirrored by the book");

        // Cancelling it frees the room again: the same 6-lot buy is admitted.
        process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(13), ParticipantId(2), 5),
            5,
        );
        let out = process(&mut engine, &limit(Side::Buy, 120, 6, 14, 2), 6);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        engine
            .check_invariants()
            .expect("the cancel freed the exposure");
    }

    #[test]
    fn risk_net_limit_charges_every_order_type() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(2, 1, 0), 0);
        process(&mut engine, &limit(Side::Sell, 150, 10, 1, 1), 1);

        // Net exposure is charged before matching, so an order that would
        // fill immediately is still refused when the candidate quantity
        // breaches the bound.
        for (offset, order_type) in [
            (2u64, OrderType::Ioc),
            (3, OrderType::Fok),
            (4, OrderType::Market),
        ] {
            let mut order = limit(
                Side::Buy,
                if order_type == OrderType::Market {
                    0
                } else {
                    150
                },
                2,
                10 + u128::from(offset),
                2,
            );
            order.order_type = order_type;
            let out = process(&mut engine, &order, offset);
            assert_eq!(
                out.executions[0].reason,
                RejectReason::RiskLimit,
                "{order_type:?} must be charged net quantity"
            );
        }

        // A post-only buy cannot rest above the bound either. Tick 120 is in
        // band and below the resting ask, so risk is the only refusal.
        let mut post_only = limit(Side::Buy, 120, 2, 20, 2);
        post_only.order_type = OrderType::PostOnly;
        let out = process(&mut engine, &post_only, 5);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);
        assert_eq!(
            engine.book().order_count(),
            1,
            "only the unconstrained liquidity order may rest"
        );
        engine
            .check_invariants()
            .expect("refused orders leave no exposure");
    }

    #[test]
    fn risk_buy_notional_is_charged_only_when_the_order_can_rest() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(2, 0, 20_000), 0);
        process(&mut engine, &limit(Side::Sell, 150, 4, 1, 1), 1);

        // A crossing limit is charged its full quantity before matching: two
        // lots at 150 ticks (15,000 cents each) are 30,000 quote cents even
        // though both would fill.
        let out = process(&mut engine, &limit(Side::Buy, 150, 2, 2, 2), 2);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);

        // IOC and FOK cannot rest, so they add no buy notional and may fill.
        for (offset, order_type, id) in [(3u64, OrderType::Ioc, 3u128), (4, OrderType::Fok, 4)] {
            let mut order = limit(Side::Buy, 150, 1, id, 2);
            order.order_type = order_type;
            let out = process(&mut engine, &order, offset);
            assert_eq!(
                out.executions.last().unwrap().kind,
                ExecKind::Filled,
                "{order_type:?} fills without resting"
            );
        }

        // PostOnly can rest, so it is charged: one lot at 120 ticks (12,000
        // cents) fits in the 20,000-cent bound, a second does not.
        let mut post_only = limit(Side::Buy, 120, 1, 5, 2);
        post_only.order_type = OrderType::PostOnly;
        let out = process(&mut engine, &post_only, 5);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        let mut second = limit(Side::Buy, 120, 1, 6, 2);
        second.order_type = OrderType::PostOnly;
        let out = process(&mut engine, &second, 6);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);
        engine
            .check_invariants()
            .expect("only the post-only order holds notional");
    }

    #[test]
    fn replacing_limits_preserves_existing_exposure() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(2, 10, 0), 0);
        process(&mut engine, &limit(Side::Sell, 150, 4, 10, 1), 1);
        process(&mut engine, &limit(Side::Buy, 150, 4, 11, 2), 2);

        // Tighten to 6 while the participant is already net long 4: a 2-lot
        // resting buy reaches the bound, a third lot would breach it.
        process(&mut engine, &set_risk(2, 6, 0), 3);
        let out = process(&mut engine, &limit(Side::Buy, 120, 2, 20, 2), 4);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        let out = process(&mut engine, &limit(Side::Buy, 120, 1, 21, 2), 5);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);

        // Loosening admits the same order.
        process(&mut engine, &set_risk(2, 8, 0), 6);
        let out = process(&mut engine, &limit(Side::Buy, 120, 1, 22, 2), 7);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);

        // Clearing the limits preserves the position and open interest; the
        // entry is pruned only once exposure is flat.
        process(&mut engine, &set_risk(2, 0, 0), 8);
        let entry = engine.risk_table().entry(ParticipantId(2));
        assert_eq!(entry.limits, RiskLimits::default());
        assert_eq!(entry.exposure.net_qty, 4);
        assert_eq!(entry.exposure.open_buy_qty, 3);

        for (offset, order_id) in [(9u64, OrderId(20)), (10, OrderId(21)), (11, OrderId(22))] {
            process(
                &mut engine,
                &Command::cancel(symbol(), order_id, ParticipantId(2), offset + 1),
                offset,
            );
        }
        let entry = engine.risk_table().entry(ParticipantId(2));
        assert_eq!(entry.exposure.open_buy_qty, 0);
        assert_eq!(entry.exposure.net_qty, 4);
        // Exposure is tracked for every participant, not only constrained
        // ones: participant 1 sold the opening 4 lots and is net short 4.
        assert_eq!(engine.risk_table().len(), 2);
        assert_eq!(
            engine.risk_table().entry(ParticipantId(1)).exposure.net_qty,
            -4
        );

        // Flatten the position against another participant's bid.
        process(&mut engine, &limit(Side::Buy, 120, 4, 30, 1), 12);
        process(&mut engine, &limit(Side::Sell, 120, 4, 31, 2), 13);
        assert!(engine.risk_table().is_empty());
        engine
            .check_invariants()
            .expect("flat exposure prunes the entry");
    }

    #[test]
    fn buy_notional_limit_uses_quote_cents() {
        // The instrument's tick is 100 cents, so one tick is 10,000 cents of
        // notional for a one-unit order.
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(1, 0, 10_000), 0);

        process(&mut engine, &limit(Side::Buy, 100, 1, 1, 1), 1);
        let out = process(&mut engine, &limit(Side::Buy, 100, 1, 2, 1), 2);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);

        // A sell adds no buy notional, and a market buy cannot rest.
        let out = process(&mut engine, &limit(Side::Sell, 110, 1, 3, 1), 3);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        let mut market = limit(Side::Buy, 0, 1, 4, 1);
        market.order_type = OrderType::Market;
        let out = process(&mut engine, &market, 4);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        engine
            .check_invariants()
            .expect("resting and market legs stay consistent");
    }

    #[test]
    fn risk_is_log_ordered_and_survives_a_snapshot() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(1, 5, 0), 0);
        process(&mut engine, &limit(Side::Buy, 120, 5, 1, 1), 1);

        let snapshot = engine.snapshot_state();
        let mut restored = SymbolEngine::from_snapshot(config(), snapshot).expect("restore");
        assert_eq!(restored.state_hash(), engine.state_hash());
        assert_eq!(
            restored
                .risk_table()
                .entry(ParticipantId(1))
                .limits
                .max_net_qty,
            5
        );
        let out = process(&mut restored, &limit(Side::Buy, 120, 1, 2, 1), 2);
        assert_eq!(out.executions[0].reason, RejectReason::RiskLimit);

        // Clearing the limit is the same ordered command: subsequent orders
        // are admitted and the risk entry is pruned once exposure is flat.
        process(&mut restored, &set_risk(1, 0, 0), 3);
        process(
            &mut restored,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 4),
            4,
        );
        assert!(restored.risk_table().is_empty());
        process(&mut restored, &limit(Side::Buy, 120, 1, 3, 1), 5);
        restored
            .check_invariants()
            .expect("cleared risk stays consistent");
    }

    #[test]
    fn a_negative_notional_limit_is_ignored_on_replay() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &set_risk(1, 10, -1), 0);
        assert!(engine.risk_table().is_empty());
        // The command still advances the log clock, but it configures nothing:
        // a crafted log cannot set a negative notional by wrapping it.
        assert_eq!(
            engine.risk_table().entry(ParticipantId(1)).limits,
            RiskLimits::default()
        );
    }
}
