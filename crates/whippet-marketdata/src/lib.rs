// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Market-data state machine and conflated fanout.
//!
//! The hub consumes the canonical MBO deltas the engine publishes to Iggy and
//! maintains a per-symbol book. The browser never sees raw deltas in Phase 1:
//! it receives a conflated snapshot at 20 Hz, which is the correct rendering
//! discipline for a ladder. Trades are retained losslessly in the visible tape
//! window; audit consumes the log, not the UI.
//!
//! A second, deliberately separate contract is the sequenced feed
//! ([`MdHub::subscribe_feed`]): every accepted delta is encoded once as an
//! [`whippet_core::api::MdFeedMessage::Delta`] frame and broadcast as an
//! `Arc`, paired with [`MdHub::feed_snapshot`] for a full depth-unlimited
//! image at a known sequence. Conflation is the dashboard's policy; it is not
//! the external feed's contract.
//!
//! ## Book lifecycle
//!
//! Each symbol's book has a baseline phase. The engine publishes a full image
//! before it publishes any delta, so a hub starts in
//! [`BookPhase::AwaitingSnapshot`], and an accepted snapshot moves the symbol
//! to [`BookPhase::Seeded`] for the rest of the process:
//!
//! ```text
//! AwaitingSnapshot --snapshot seq >= book.seq--> Seeded   (baseline image)
//! Seeded           --snapshot seq >  book.seq--> Seeded   (image replaces)
//! any              --snapshot seq <  book.seq--> any      (stale, refused)
//! AwaitingSnapshot --Status / Snapshot delta---> AwaitingSnapshot (seq advances)
//! AwaitingSnapshot --Add/Reduce/Remove/Trade/Bbo-> AwaitingSnapshot (refused, counted)
//! ```
//!
//! A delta advances the per-symbol sequence only when the book applied it, so
//! `seq` means "applied through" on both the book and the hub. A book-state
//! delta that arrives without a baseline is refused and counted
//! ([`MdHub::events_without_baseline`]) instead of inventing depth: the image
//! that follows covers it, because sequences only increase within a symbol.

#![forbid(unsafe_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::sync::{broadcast, watch};
use whippet_core::api::{
    BookView, LevelView, MdFeedDelta, MdFeedMessage, MdFeedSnapshot, MetricsView, StateView,
    TradeView,
};
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{OrderId, Price, Seq, Symbol};
use whippet_core::protocol::{
    MD_FLAG_HALTED, MdEvent, MdKind, MdSnapshot, SessionState, Side, TradingStatus,
};

const TRADE_TAPE_LEN: usize = 250;
const BOOK_DEPTH: usize = 20;

/// Bounded buffer for the sequenced feed, in frames.
///
/// The hub keeps no per-subscriber queue: a subscriber that falls further
/// behind than this is told it lagged and must take a fresh
/// [`MdFeedSnapshot`]. Sized for a short network hiccup on a demo feed, not
/// for replaying minutes of history — Iggy is the replay path.
pub const MD_FEED_CAPACITY: usize = 1024;

/// Publisher cadence. `spawn_publisher` ticks at this interval.
const PUBLISH_INTERVAL: Duration = Duration::from_millis(50);

/// Coalescing window for redundant `publish()` calls. It sits just below the
/// publisher cadence so scheduler jitter or the time spent building a frame
/// never suppresses a scheduled 20 Hz tick. A dirty hub always publishes
/// immediately; only back-to-back publishes with no new events are skipped.
const PUBLISH_COALESCE_WINDOW: Duration = Duration::from_millis(45);

/// Rolling window, in seconds, behind [`Metrics::window_summary`]. Long
/// enough to smooth a scrape's worth of samples, short enough that a soak
/// sees a degradation minutes after it starts instead of a process-lifetime
/// average that never recovers.
pub const LATENCY_WINDOW_SECS: u64 = 10;

/// Per-shard metric slots for consumer lag and ring occupancy. A shard
/// number at or above this folds into the last slot, so an unexpected shard
/// count still surfaces lag instead of dropping it silently.
const METRIC_SHARD_SLOTS: usize = 256;

#[derive(Debug, Clone, Copy)]
struct OrderRef {
    side: Side,
    price_cents: i64,
    qty: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct LevelState {
    qty: u64,
    orders: u32,
}

/// Lifecycle phase of one symbol's book.
///
/// The producer publishes a full image before it publishes any delta, because
/// a delta names one price level and cannot reconstruct the resting orders
/// behind the rest of the book. A book that has never accepted an image is
/// therefore [`BookPhase::AwaitingSnapshot`], and book-state deltas are refused
/// (and counted) rather than inventing depth.
///
/// The phase is monotonic within a process: an accepted snapshot seeds the
/// book and nothing unsets it. A snapshot cannot be "unseen", and a stale one
/// is rejected before it can touch the phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookPhase {
    /// No full image has been accepted yet. Only events that carry no book
    /// state (`Status`, `Snapshot`) apply.
    AwaitingSnapshot,
    /// A full image has been accepted; deltas apply from here on.
    Seeded,
}

impl BookPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingSnapshot => "awaiting_snapshot",
            Self::Seeded => "seeded",
        }
    }
}

/// Result of offering one delta to a book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyOutcome {
    /// The event mutated the book (or was an accepted no-op) and advanced the
    /// per-symbol sequence.
    Applied,
    /// Duplicate or stale: already covered by the book's sequence.
    Stale,
    /// A book-state delta arrived before any full image. Ignored and counted;
    /// the sequence did not advance.
    NoBaseline,
    /// The event cannot be represented against the current book (an
    /// over-reduce or an aggregate overflow). Ignored; the sequence did not
    /// advance, so the book never claims state it did not take.
    Refused,
}

impl ApplyOutcome {
    const fn is_applied(self) -> bool {
        matches!(self, Self::Applied)
    }
}

/// Whether a delta kind needs a baseline image to be meaningful.
///
/// `Status` mutates only the halt/session flags and an `MdKind::Snapshot`
/// delta carries no depth at all, so both are safe before a baseline. Every
/// other kind describes book state for a symbol: a resting order, a trade, or
/// a best bid/offer. Applying one of those without an image is what invents
/// depth, so the gate covers them all.
const fn carries_book_state(kind: MdKind) -> bool {
    matches!(
        kind,
        MdKind::Add | MdKind::Reduce | MdKind::Remove | MdKind::Trade | MdKind::Bbo
    )
}

#[derive(Debug)]
struct BookState {
    symbol: Symbol,
    tick_size_cents: i64,
    bids: BTreeMap<i64, LevelState>,
    asks: BTreeMap<i64, LevelState>,
    orders: HashMap<OrderId, OrderRef>,
    last_trade_price_cents: Option<i64>,
    last_trade_qty: u64,
    last_trade_side: Option<Side>,
    /// Baseline lifecycle. Monotonic; see [`BookPhase`].
    phase: BookPhase,
    /// `true` while `orders` is the complete resting-order set, so every level
    /// aggregate equals the refs at that level. A level-only snapshot
    /// (aggregates, no refs) clears this until a full image arrives.
    refs_complete: bool,
    /// Book-state deltas refused because the book had no baseline image.
    /// Nonzero means the producer's startup order was violated.
    events_without_baseline: u64,
    seq: Seq,
    /// Instrument halt state from the latest `MdKind::Status` event. A halt
    /// carries no book change; this flag is the only thing a status event
    /// mutates besides `seq`.
    halted: bool,
    /// Instrument session from the latest `MdKind::Status` event or snapshot
    /// flags. Defaults to [`SessionState::Open`], so a hub that never sees a
    /// session bit reports a trading instrument, exactly like the engine.
    session: SessionState,
    /// Input offset of the event that produced `seq`, or `0` before the first
    /// applied event. It is the snapshot header's link back to the input log:
    /// a consumer can say "my image is at seq S, caused by offset O".
    last_cause_offset: u64,
    /// Running sum of every bid level, including levels below the visible
    /// depth. Kept in step with the level maps so `view()` never folds them.
    depth_bid_qty: u64,
    /// Running sum of every ask level. See `depth_bid_qty`.
    depth_ask_qty: u64,
}

impl BookState {
    fn new(symbol: Symbol, tick_size_cents: i64) -> Self {
        Self {
            symbol,
            tick_size_cents,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            last_trade_price_cents: None,
            last_trade_qty: 0,
            last_trade_side: None,
            phase: BookPhase::AwaitingSnapshot,
            refs_complete: true,
            events_without_baseline: 0,
            seq: Seq(0),
            halted: false,
            session: SessionState::default(),
            last_cause_offset: 0,
            depth_bid_qty: 0,
            depth_ask_qty: 0,
        }
    }

    fn price_cents(&self, price: Price) -> i64 {
        price.0 * self.tick_size_cents
    }

    fn side_levels(&self, side: Side) -> &BTreeMap<i64, LevelState> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn side_levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, LevelState> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn depth(&self, side: Side) -> u64 {
        match side {
            Side::Buy => self.depth_bid_qty,
            Side::Sell => self.depth_ask_qty,
        }
    }

    fn depth_mut(&mut self, side: Side) -> &mut u64 {
        match side {
            Side::Buy => &mut self.depth_bid_qty,
            Side::Sell => &mut self.depth_ask_qty,
        }
    }

    /// Recompute the cached totals from the level maps. Only snapshots need
    /// this; deltas adjust the totals by the exact level change.
    fn recompute_depth(&mut self) {
        self.depth_bid_qty = total_depth(&self.bids);
        self.depth_ask_qty = total_depth(&self.asks);
    }

    /// Apply one delta.
    ///
    /// The sequence advances if and only if the event was applied. A stale
    /// duplicate, a baseline-less delta, and an event the book cannot represent
    /// all leave `seq` where it was; that is what keeps the hub sequence equal
    /// to the maximum book sequence and stops an ignored event from making
    /// every later one look stale.
    fn apply(&mut self, event: &MdEvent) -> ApplyOutcome {
        // At-least-once delivery: a crash between apply and commit replays the
        // batch. The per-symbol sequence makes deltas idempotent.
        if self.seq.0 != 0 && event.seq.0 <= self.seq.0 {
            return ApplyOutcome::Stale;
        }
        // Fail closed on book state without a baseline image. `Status` and
        // `Snapshot` deltas carry no depth, so they still apply and advance the
        // sequence exactly as before.
        if self.phase == BookPhase::AwaitingSnapshot && carries_book_state(event.kind) {
            self.events_without_baseline = self.events_without_baseline.saturating_add(1);
            return ApplyOutcome::NoBaseline;
        }
        let outcome = match event.kind {
            MdKind::Add => self.apply_add(event),
            MdKind::Reduce => self.apply_reduce(event),
            MdKind::Remove => self.apply_remove(event),
            MdKind::Trade => {
                self.last_trade_price_cents = Some(self.price_cents(event.price));
                self.last_trade_qty = event.qty.0;
                self.last_trade_side = Some(event.side);
                ApplyOutcome::Applied
            }
            MdKind::Status => {
                // One decoder for the halt + session combination. An
                // out-of-range session code leaves the current session
                // standing but still applies the halt bit; the accepted
                // combination lives in `TradingStatus`.
                if let Ok(status) = TradingStatus::from_md_flags(event.flags) {
                    self.halted = status.halted;
                    self.session = status.session;
                } else {
                    self.halted = event.flags & MD_FLAG_HALTED != 0;
                }
                ApplyOutcome::Applied
            }
            MdKind::Bbo | MdKind::Snapshot => ApplyOutcome::Applied,
        };
        if outcome.is_applied() {
            self.seq = event.seq;
            self.last_cause_offset = event.cause_offset;
        }
        outcome
    }

    /// Apply an add. Overflow in a level or in the cached depth refuses the
    /// event instead of saturating: a saturated aggregate can never equal the
    /// sum of its order refs again, and saturating silently is exactly the
    /// corruption this book is trying to avoid.
    fn apply_add(&mut self, event: &MdEvent) -> ApplyOutcome {
        let price_cents = self.price_cents(event.price);
        let before = self
            .side_levels(event.side)
            .get(&price_cents)
            .copied()
            .unwrap_or_default();
        let Some(qty) = before.qty.checked_add(event.qty.0) else {
            return ApplyOutcome::Refused;
        };
        let Some(orders) = before.orders.checked_add(1) else {
            return ApplyOutcome::Refused;
        };
        let Some(depth) = self.depth(event.side).checked_add(event.qty.0) else {
            return ApplyOutcome::Refused;
        };
        self.side_levels_mut(event.side)
            .insert(price_cents, LevelState { qty, orders });
        *self.depth_mut(event.side) = depth;
        self.orders.insert(
            event.order_id,
            OrderRef {
                side: event.side,
                price_cents,
                qty: event.qty.0,
            },
        );
        ApplyOutcome::Applied
    }

    /// Apply a reduce, which names the order's new remaining quantity.
    ///
    /// An unknown order ref is accepted as a no-op: a level-only snapshot has
    /// aggregates but no refs, so an order-level reduce cannot be matched
    /// against it. A reduce to zero is a cancel and removes the ref, the level
    /// quantity and the level's order count, so no zero-quantity level survives.
    fn apply_reduce(&mut self, event: &MdEvent) -> ApplyOutcome {
        let Some(order) = self.orders.get(&event.order_id).copied() else {
            return ApplyOutcome::Applied;
        };
        let new_qty = event.qty.0;
        if new_qty > order.qty {
            return ApplyOutcome::Refused;
        }
        if new_qty == 0 {
            self.orders.remove(&event.order_id);
            self.drop_order_from_levels(order);
            return ApplyOutcome::Applied;
        }
        let removed = {
            let levels = self.side_levels_mut(order.side);
            match levels.get_mut(&order.price_cents) {
                Some(level) => {
                    let before = level.qty;
                    level.qty = level.qty.saturating_sub(order.qty - new_qty);
                    before - level.qty
                }
                None => 0,
            }
        };
        if removed > 0 {
            let depth = self.depth_mut(order.side);
            *depth = depth.saturating_sub(removed);
        }
        if let Some(order) = self.orders.get_mut(&event.order_id) {
            order.qty = new_qty;
        }
        ApplyOutcome::Applied
    }

    /// Apply a remove. An unknown order (already removed, or a level-only
    /// snapshot with no refs) is an accepted no-op that still advances `seq`.
    fn apply_remove(&mut self, event: &MdEvent) -> ApplyOutcome {
        if let Some(order) = self.orders.remove(&event.order_id) {
            self.drop_order_from_levels(order);
        }
        ApplyOutcome::Applied
    }

    /// Subtract a removed order's remaining quantity from its level, drop the
    /// level when its last order leaves, and keep the cached depth in step.
    fn drop_order_from_levels(&mut self, order: OrderRef) {
        let (removed, empty, leftover) = {
            let levels = self.side_levels_mut(order.side);
            let Some(level) = levels.get_mut(&order.price_cents) else {
                return;
            };
            let before = level.qty;
            level.qty = level.qty.saturating_sub(order.qty);
            level.orders = level.orders.saturating_sub(1);
            (before - level.qty, level.orders == 0, level.qty)
        };
        if empty {
            self.side_levels_mut(order.side).remove(&order.price_cents);
        }
        // Removing the level also drops any quantity left behind by an
        // inconsistent order set, so subtract that too.
        let removed = if empty {
            removed.saturating_add(leftover)
        } else {
            removed
        };
        if removed > 0 {
            let depth = self.depth_mut(order.side);
            *depth = depth.saturating_sub(removed);
        }
    }

    fn apply_snapshot(&mut self, snapshot: &MdSnapshot) -> bool {
        // A retried append can deliver an older snapshot after newer deltas:
        // never regress a symbol that already has real state. While the book is
        // still awaiting a baseline it holds no depth, so an image at the
        // book's current sequence is exactly right (a replayed status advanced
        // `seq` without carrying book state) and a strictly older image is
        // still refused.
        let stale = if self.phase == BookPhase::Seeded {
            self.seq.0 != 0 && snapshot.seq.0 <= self.seq.0
        } else {
            snapshot.seq.0 < self.seq.0
        };
        if stale {
            return false;
        }
        self.seq = snapshot.seq;
        self.last_cause_offset = snapshot.cause_offset;
        // Same decoder as a status event. A legacy snapshot (pre-session, all
        // status bits zero) decodes as pre-open here, matching the wire: the
        // session code is carried explicitly, not inferred.
        if let Ok(status) = TradingStatus::from_md_flags(snapshot.flags) {
            self.halted = status.halted;
            self.session = status.session;
        } else {
            self.halted = snapshot.flags & MD_FLAG_HALTED != 0;
        }
        self.bids.clear();
        self.asks.clear();
        self.orders.clear();
        // A level-only image carries aggregates but no order refs, so the
        // per-level ref equality is unprovable until a full image arrives. An
        // empty image is complete: it has no levels and no refs.
        self.refs_complete =
            !snapshot.orders.is_empty() || (snapshot.bids.is_empty() && snapshot.asks.is_empty());
        self.phase = BookPhase::Seeded;
        if snapshot.orders.is_empty() {
            // Compatibility with a level-only snapshot: keep the aggregates
            // but accept that later order-level deltas cannot be applied.
            for level in &snapshot.bids {
                self.bids.insert(
                    self.price_cents(level.price),
                    LevelState {
                        qty: level.qty.0,
                        orders: level.orders,
                    },
                );
            }
            for level in &snapshot.asks {
                self.asks.insert(
                    self.price_cents(level.price),
                    LevelState {
                        qty: level.qty.0,
                        orders: level.orders,
                    },
                );
            }
        } else {
            // Rebuild the complete book from the resting-order set. The top-N
            // levels in the snapshot are for display only; applying deltas to
            // them would leave stale quantities for orders outside the top N.
            for order in &snapshot.orders {
                let price_cents = self.price_cents(order.price);
                let level = self
                    .side_levels_mut(order.side)
                    .entry(price_cents)
                    .or_default();
                level.qty = level.qty.saturating_add(order.qty.0);
                level.orders = level.orders.saturating_add(1);
                self.orders.insert(
                    order.order_id,
                    OrderRef {
                        side: order.side,
                        price_cents,
                        qty: order.qty.0,
                    },
                );
            }
        }
        self.recompute_depth();
        self.last_trade_side = None;
        if snapshot.last_trade_price.0 > 0 {
            self.last_trade_price_cents = Some(self.price_cents(snapshot.last_trade_price));
            self.last_trade_qty = snapshot.last_trade_qty.0;
        } else {
            self.last_trade_price_cents = None;
            self.last_trade_qty = 0;
        }
        true
    }

    /// Executable book invariants. Test-only: it walks every level and order,
    /// so it never belongs on a production path.
    ///
    /// ```text
    /// sequence      seq > 0 as soon as the book holds any state; a book at
    ///               seq 0 has cause offset 0 and no levels/orders
    /// phase         a book without a baseline image holds no depth
    /// levels        every level has qty > 0 and orders > 0; prices are
    ///               positive tick multiples; best bid < best ask
    /// depth         depth_bid_qty == sum(bids.qty), likewise asks, with
    ///               checked addition (an overflow is reported, not wrapped)
    /// order refs    every ref has positive qty, a level on its own side and
    ///               a positive tick-multiple price; while the ref set is
    ///               complete, each level's qty/orders equals the sum/count of
    ///               the refs at that level, and every level has refs
    /// last trade    last_trade_qty == 0 iff last_trade_price_cents is None;
    ///               a present price is positive and a tick multiple. The side
    ///               is unknown (None) after a snapshot: qty and price are not
    /// ```
    #[cfg(test)]
    fn check_invariants(&self) -> Result<(), String> {
        if self.tick_size_cents <= 0 {
            return Err(format!(
                "tick size {} is not positive",
                self.tick_size_cents
            ));
        }
        if self.seq.0 == 0 && self.last_cause_offset != 0 {
            return Err(format!(
                "sequence 0 carries cause offset {}",
                self.last_cause_offset
            ));
        }
        let holds_state = !self.bids.is_empty() || !self.asks.is_empty() || !self.orders.is_empty();
        if self.seq.0 == 0 && holds_state {
            return Err("book holds state at sequence 0".into());
        }
        if self.phase == BookPhase::AwaitingSnapshot && holds_state {
            return Err("a book without a baseline image holds depth".into());
        }
        let bid_sum = check_levels(&self.bids, self.tick_size_cents, "bid")?;
        let ask_sum = check_levels(&self.asks, self.tick_size_cents, "ask")?;
        if self.depth_bid_qty != bid_sum {
            return Err(format!(
                "cached bid depth {} != level sum {bid_sum}",
                self.depth_bid_qty
            ));
        }
        if self.depth_ask_qty != ask_sum {
            return Err(format!(
                "cached ask depth {} != level sum {ask_sum}",
                self.depth_ask_qty
            ));
        }
        if let (Some((best_bid, _)), Some((best_ask, _))) =
            (self.bids.last_key_value(), self.asks.first_key_value())
            && best_bid >= best_ask
        {
            return Err(format!(
                "crossed book: best bid {best_bid} >= best ask {best_ask}"
            ));
        }
        match (self.last_trade_price_cents, self.last_trade_qty) {
            (None, 0) => {}
            (Some(price_cents), qty) => {
                if qty == 0 {
                    return Err(format!("last trade price {price_cents} without a quantity"));
                }
                if price_cents <= 0 || price_cents % self.tick_size_cents != 0 {
                    return Err(format!(
                        "last trade price {price_cents} is not a positive tick multiple"
                    ));
                }
            }
            (None, qty) => return Err(format!("last trade quantity {qty} without a price")),
        }
        if self.orders.is_empty() {
            // A level-only image has aggregates but no refs: the per-level
            // equality cannot be proven, everything above still is.
            return Ok(());
        }
        if !self.refs_complete {
            return Ok(());
        }
        let mut aggregates: HashMap<(Side, i64), (u64, u32)> = HashMap::new();
        for (order_id, order) in &self.orders {
            if order.qty == 0 {
                return Err(format!("order {order_id} has zero quantity"));
            }
            if order.price_cents <= 0 || order.price_cents % self.tick_size_cents != 0 {
                return Err(format!(
                    "order {order_id} prices at {} which is not a positive tick multiple",
                    order.price_cents
                ));
            }
            let level = self
                .side_levels(order.side)
                .get(&order.price_cents)
                .ok_or_else(|| {
                    format!(
                        "order {order_id} rests at {} with no {:?} level",
                        order.price_cents, order.side
                    )
                })?;
            if level.orders == 0 {
                return Err(format!("order {order_id} rests in a level with no orders"));
            }
            let entry = aggregates
                .entry((order.side, order.price_cents))
                .or_insert((0, 0));
            entry.0 = entry
                .0
                .checked_add(order.qty)
                .ok_or_else(|| format!("order refs overflow at {}", order.price_cents))?;
            entry.1 += 1;
        }
        for (side, levels) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (price_cents, level) in levels {
                let (qty, count) = aggregates
                    .get(&(side, *price_cents))
                    .copied()
                    .unwrap_or_default();
                if level.qty != qty {
                    return Err(format!(
                        "{side:?} level {price_cents}: level qty {} != order refs {qty}",
                        level.qty
                    ));
                }
                if level.orders != count {
                    return Err(format!(
                        "{side:?} level {price_cents}: level orders {} != order refs {count}",
                        level.orders
                    ));
                }
            }
        }
        Ok(())
    }

    fn view(&self) -> BookView {
        let bids: Vec<LevelView> = self
            .bids
            .iter()
            .rev()
            .take(BOOK_DEPTH)
            .map(|(price_cents, level)| LevelView {
                price_cents: *price_cents,
                qty: level.qty,
                orders: level.orders,
            })
            .collect();
        let asks: Vec<LevelView> = self
            .asks
            .iter()
            .take(BOOK_DEPTH)
            .map(|(price_cents, level)| LevelView {
                price_cents: *price_cents,
                qty: level.qty,
                orders: level.orders,
            })
            .collect();
        let best_bid_cents = bids.first().map(|level| level.price_cents);
        let best_ask_cents = asks.first().map(|level| level.price_cents);
        let spread_cents = match (best_bid_cents, best_ask_cents) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        };
        let mid_cents = match (best_bid_cents, best_ask_cents) {
            (Some(bid), Some(ask)) => Some((bid as f64 + ask as f64) / 2.0),
            _ => None,
        };
        BookView {
            symbol: self.symbol,
            seq: self.seq,
            bids,
            asks,
            best_bid_cents,
            best_ask_cents,
            spread_cents,
            mid_cents,
            last_trade_price_cents: self.last_trade_price_cents,
            last_trade_qty: self.last_trade_qty,
            last_trade_side: self.last_trade_side,
            depth_bid_qty: self.depth_bid_qty,
            depth_ask_qty: self.depth_ask_qty,
            halted: self.halted,
            session: self.session,
        }
    }

    /// Full price-level image for the sequenced feed.
    ///
    /// Same `LevelView` shape and ordering as [`Self::view`], but every level
    /// is included instead of the best [`BOOK_DEPTH`]. A level-2 consumer that
    /// starts from a depth-capped image can never recover the levels it did
    /// not receive, because a delta only names the single level it changes.
    fn feed_snapshot(&self) -> MdFeedSnapshot {
        let bids: Vec<LevelView> = self
            .bids
            .iter()
            .rev()
            .map(|(price_cents, level)| level_view(*price_cents, level))
            .collect();
        let asks: Vec<LevelView> = self
            .asks
            .iter()
            .map(|(price_cents, level)| level_view(*price_cents, level))
            .collect();
        MdFeedSnapshot {
            symbol: self.symbol,
            seq: self.seq.0,
            cause_offset: self.last_cause_offset,
            bids,
            asks,
            halted: self.halted,
        }
    }
}

fn level_view(price_cents: i64, level: &LevelState) -> LevelView {
    LevelView {
        price_cents,
        qty: level.qty,
        orders: level.orders,
    }
}

fn total_depth(levels: &BTreeMap<i64, LevelState>) -> u64 {
    levels
        .values()
        .fold(0u64, |total, level| total.saturating_add(level.qty))
}

/// Validate one side's level map and return its checked quantity total.
///
/// A zero-quantity level is a real defect, not a rounding artefact: it renders
/// an empty rung in the ladder, it can only come from an inconsistent order
/// set, and it makes "best bid < best ask" meaningless.
#[cfg(test)]
fn check_levels(
    levels: &BTreeMap<i64, LevelState>,
    tick_size_cents: i64,
    side: &str,
) -> Result<u64, String> {
    let mut total = 0u64;
    for (price_cents, level) in levels {
        if *price_cents <= 0 {
            return Err(format!(
                "{side} level {price_cents} is not a positive price"
            ));
        }
        if price_cents % tick_size_cents != 0 {
            return Err(format!(
                "{side} level {price_cents} is not a multiple of the tick {tick_size_cents}"
            ));
        }
        if level.qty == 0 {
            return Err(format!("{side} level {price_cents} has zero quantity"));
        }
        if level.orders == 0 {
            return Err(format!("{side} level {price_cents} has no orders"));
        }
        total = total
            .checked_add(level.qty)
            .ok_or_else(|| format!("{side} quantity overflows u64 at level {price_cents}"))?;
    }
    Ok(total)
}

struct HubState {
    books: BTreeMap<Symbol, BookState>,
    trades: VecDeque<TradeView>,
    last_update_ns: u64,
    last_publish: Option<Instant>,
    sequence: u64,
    md_events: u64,
    trades_count: u64,
    dirty: bool,
}

/// Fields gathered while the hub lock is held. `metrics.snapshot()` runs after
/// the lock is released, so a histogram or rate lock never blocks the MD
/// consumer.
struct StateParts {
    symbols: Vec<BookView>,
    trades: Vec<TradeView>,
    freshness_ms: u64,
    sequence: u64,
}

/// Number of distinct [`Stage`] values. Kept in sync with the enum by
/// `Stage::ALL`; `Stage::index` relies on the declaration order.
const STAGE_COUNT: usize = 9;

/// A slice of the request-to-visible-state time budget.
///
/// The harness measures client-side send and wire time; these stages explain
/// where a request spends its time inside the node. Each stage is recorded by
/// the component that owns it, from one clock per request or batch: a stage is
/// the elapsed slice since the previous boundary, never a per-order stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Request intake: body read and `serde_json` parse (auth precedes the
    /// stage clock).
    GatewayParse,
    /// Field and participant validation, before any encoding.
    GatewayValidate,
    /// Command and journal-header encoding.
    GatewayEncode,
    /// The group-commit append round trip.
    GatewayAppend,
    /// Applying a batch of MD deltas to the hub's books.
    MdApply,
    /// Serializing a state frame for a WebSocket subscriber (cache miss only).
    WsFrame,
    /// Matching one command on the shard thread.
    EngineProcess,
    /// Publishing the executions and MD outputs of one batch.
    EngineAppend,
    /// The offset commit (only when the watermark is due).
    EngineCommit,
}

impl Stage {
    /// Every stage, in declaration and exposition order.
    pub const ALL: [Stage; STAGE_COUNT] = [
        Stage::GatewayParse,
        Stage::GatewayValidate,
        Stage::GatewayEncode,
        Stage::GatewayAppend,
        Stage::MdApply,
        Stage::WsFrame,
        Stage::EngineProcess,
        Stage::EngineAppend,
        Stage::EngineCommit,
    ];

    /// Stable snake_case name, used as the Prometheus `stage` label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Stage::GatewayParse => "gateway_parse",
            Stage::GatewayValidate => "gateway_validate",
            Stage::GatewayEncode => "gateway_encode",
            Stage::GatewayAppend => "gateway_append",
            Stage::MdApply => "md_apply",
            Stage::WsFrame => "ws_frame",
            Stage::EngineProcess => "engine_process",
            Stage::EngineAppend => "engine_append",
            Stage::EngineCommit => "engine_commit",
        }
    }

    /// Index into [`Metrics`]' per-stage histogram array.
    const fn index(self) -> usize {
        self as usize
    }
}

/// Why the ingress pre-trade risk guard refused an order.
///
/// The guard sits at the HTTP/TCP admission boundary, before any journal
/// append. It is an ingress guard, not engine-enforced risk; see the
/// `RiskConfig` documentation in the `whippet` crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskRejection {
    /// Quantity above `max_order_qty`.
    OrderQty,
    /// Notional above `max_notional_cents`.
    Notional,
}

impl RiskRejection {
    /// Stable snake_case name, used as the Prometheus `reason` label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OrderQty => "order_qty",
            Self::Notional => "notional",
        }
    }
}

/// Latency percentiles for one [`Stage`], in microseconds.
#[derive(Debug, Clone, Copy, Default)]
pub struct LatencySummary {
    pub p50_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    /// Samples behind the percentiles. Zero means the stage has not recorded
    /// yet, which is different from a stage whose samples were all sub-microsecond.
    pub samples: u64,
}

/// Latency percentiles over the rolling window, in microseconds.
///
/// Companion to the process-lifetime percentiles in [`MetricsView`]: a soak
/// scrapes this to see the current tail instead of a lifetime average that
/// never recovers. `secs` is the nominal window length.
#[derive(Debug, Clone, Copy, Default)]
pub struct WindowSummary {
    pub ingress_p50_us: f64,
    pub ingress_p99_us: f64,
    pub match_p50_us: f64,
    pub match_p99_us: f64,
    /// Nominal window length in seconds, i.e. [`LATENCY_WINDOW_SECS`] unless
    /// the [`Metrics`] was built with [`Metrics::with_window_secs`].
    pub secs: u64,
}

/// Lifetime and rolling-window latency histograms behind one mutex.
///
/// One lock holds all four histograms plus the window clock, so recording a
/// sample costs exactly one lock acquisition for both the lifetime and the
/// windowed percentile. Shard merges take the same lock and stay off the
/// per-command path.
struct LatencyState {
    ingress: Histogram<u64>,
    match_latency: Histogram<u64>,
    /// Nominal window length in seconds. [`Metrics::with_window_secs`]
    /// shortens it for tests; `0` rotates on every sample.
    window_secs: u64,
    window_started: Instant,
    window_ingress: Histogram<u64>,
    window_match: Histogram<u64>,
}

impl LatencyState {
    /// Open a fresh window once the current one has run its nominal length.
    ///
    /// Called on every record and merge path: the first sample after the
    /// window expires opens the next window, so a window spans one interval
    /// plus at most the gap to its first sample. Read paths do not rotate;
    /// see [`Metrics::window_summary`] for why.
    fn rotate_if_due(&mut self, now: Instant) {
        if now.duration_since(self.window_started).as_secs() >= self.window_secs {
            self.window_ingress.clear();
            self.window_match.clear();
            self.window_started = now;
        }
    }

    /// Window percentiles, or zeroes once the window has expired.
    ///
    /// An expired window is reported as empty and left for the next record to
    /// rotate: a scrape that lands just past the boundary must not discard a
    /// populated window it is about to read, and an idle node honestly has no
    /// current latency to report.
    fn window_percentiles(&self, now: Instant) -> (f64, f64, f64, f64) {
        if now.duration_since(self.window_started).as_secs() >= self.window_secs {
            return (0.0, 0.0, 0.0, 0.0);
        }
        (
            percentile_of(&self.window_ingress, 50.0),
            percentile_of(&self.window_ingress, 99.0),
            percentile_of(&self.window_match, 50.0),
            percentile_of(&self.window_match, 99.0),
        )
    }
}

/// Shared metrics. Counters are atomics; percentiles come from HDR histograms
/// at the edge, never from the match loop.
pub struct Metrics {
    orders_ingressed: AtomicU64,
    orders_matched: AtomicU64,
    orders_rejected: AtomicU64,
    /// Ingress pre-trade risk rejections, split by reason so the exposition
    /// can label `whippet_risk_rejected_total{reason=...}`.
    risk_rejected_qty: AtomicU64,
    risk_rejected_notional: AtomicU64,
    executions: AtomicU64,
    trades: AtomicU64,
    md_events: AtomicU64,
    engine_commands: AtomicU64,
    consumer_lag: AtomicU64,
    /// Consumer lag per shard slot (see [`METRIC_SHARD_SLOTS`]). Zero entries
    /// are omitted from [`Metrics::consumer_lag_by_shard`] to keep the metric
    /// cardinality bounded.
    consumer_lag_by_shard: [AtomicU64; METRIC_SHARD_SLOTS],
    /// Broker-reported bytes per log topic; index order is [`LOG_TOPICS`].
    log_bytes: [AtomicU64; 3],
    log_messages: [AtomicU64; 3],
    iggy_connected: AtomicBool,
    partition_count: AtomicU64,
    /// Lifetime and rolling-window latency histograms behind one mutex, so a
    /// sample costs one lock acquisition and the lifetime and windowed
    /// percentiles can never disagree about ordering.
    latency: Mutex<LatencyState>,
    /// Input/output ring occupancy per shard slot, with the capacity the
    /// reporter last saw. Zero entries are omitted from
    /// [`Metrics::ring_depths`].
    ring_input_depth: [AtomicU32; METRIC_SHARD_SLOTS],
    ring_output_depth: [AtomicU32; METRIC_SHARD_SLOTS],
    ring_capacity: [AtomicU32; METRIC_SHARD_SLOTS],
    /// Gateway receive -> turbo match ack. Empty in durable mode.
    turbo_ack: Mutex<Histogram<u64>>,
    /// One histogram per [`Stage`]. Indexed by `Stage::index`.
    stages: [Mutex<Histogram<u64>>; STAGE_COUNT],
    rate: Mutex<RateState>,
    started: Instant,
    mode: Mode,
}

struct RateState {
    at: Instant,
    orders: u64,
    rate: f64,
}

impl Metrics {
    pub fn new(mode: Mode, partitions: u32) -> Self {
        Self::with_window_secs(mode, partitions, LATENCY_WINDOW_SECS)
    }

    /// Like [`Metrics::new`], with a shortened latency window so tests can
    /// exercise rotation without sleeping. `0` rotates on every sample, which
    /// leaves each window holding only its most recent sample.
    pub fn with_window_secs(mode: Mode, partitions: u32, window_secs: u64) -> Self {
        Self {
            orders_ingressed: AtomicU64::new(0),
            orders_matched: AtomicU64::new(0),
            orders_rejected: AtomicU64::new(0),
            risk_rejected_qty: AtomicU64::new(0),
            risk_rejected_notional: AtomicU64::new(0),
            executions: AtomicU64::new(0),
            trades: AtomicU64::new(0),
            md_events: AtomicU64::new(0),
            engine_commands: AtomicU64::new(0),
            consumer_lag: AtomicU64::new(0),
            consumer_lag_by_shard: std::array::from_fn(|_| AtomicU64::new(0)),
            log_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            log_messages: std::array::from_fn(|_| AtomicU64::new(0)),
            ring_input_depth: std::array::from_fn(|_| AtomicU32::new(0)),
            ring_output_depth: std::array::from_fn(|_| AtomicU32::new(0)),
            ring_capacity: std::array::from_fn(|_| AtomicU32::new(0)),
            iggy_connected: AtomicBool::new(false),
            partition_count: AtomicU64::new(u64::from(partitions)),
            latency: Mutex::new(LatencyState {
                ingress: new_histogram(),
                match_latency: new_histogram(),
                window_secs,
                window_started: Instant::now(),
                window_ingress: new_histogram(),
                window_match: new_histogram(),
            }),
            turbo_ack: Mutex::new(new_histogram()),
            stages: std::array::from_fn(|_| Mutex::new(new_histogram())),
            rate: Mutex::new(RateState {
                at: Instant::now(),
                orders: 0,
                rate: 0.0,
            }),
            started: Instant::now(),
            mode,
        }
    }

    pub fn set_iggy_connected(&self, connected: bool) {
        self.iggy_connected.store(connected, Ordering::Relaxed);
    }

    /// The last health-cycle result for every journal client.
    ///
    /// The node's health monitor pings the ingress client and every engine-pool
    /// entry once per cycle and caches the aggregate here, so a readiness probe
    /// does no I/O and a dead non-zero pool client is visible.
    pub fn iggy_connected(&self) -> bool {
        self.iggy_connected.load(Ordering::Relaxed)
    }

    pub fn set_consumer_lag(&self, lag: u64) {
        self.consumer_lag.store(lag, Ordering::Relaxed);
    }

    /// Record the broker-reported size of one log topic.
    pub fn set_topic_stats(&self, index: usize, bytes: u64, messages: u64) {
        if let (Some(bytes_slot), Some(messages_slot)) =
            (self.log_bytes.get(index), self.log_messages.get(index))
        {
            bytes_slot.store(bytes, Ordering::Relaxed);
            messages_slot.store(messages, Ordering::Relaxed);
        }
    }

    /// Bytes and message counts for the three trading topics.
    pub fn topic_stats(&self) -> Vec<(&'static str, u64, u64)> {
        LOG_TOPICS
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    *name,
                    self.log_bytes[index].load(Ordering::Relaxed),
                    self.log_messages[index].load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// Consumer lag of one shard, in records.
    ///
    /// The node's lag sweep sets both this and the aggregate
    /// [`Metrics::set_consumer_lag`]: the aggregate is what an alert sums,
    /// the per-shard list is what says which partition determines recovery
    /// time. A shard number at or above [`METRIC_SHARD_SLOTS`] folds into the
    /// last slot so its lag stays visible.
    pub fn set_consumer_lag_for_shard(&self, shard: u32, lag: u64) {
        self.consumer_lag_by_shard[shard_slot(shard)].store(lag, Ordering::Relaxed);
    }

    /// Nonzero per-shard consumer lag as `(shard, lag)`, sorted by shard.
    ///
    /// Zero entries are omitted, so an idle cluster exports nothing and the
    /// metric cardinality stays bounded by the configured shard count.
    pub fn consumer_lag_by_shard(&self) -> Vec<(u32, u64)> {
        self.consumer_lag_by_shard
            .iter()
            .enumerate()
            .map(|(slot, lag)| (slot as u32, lag.load(Ordering::Relaxed)))
            .filter(|&(_, lag)| lag != 0)
            .collect()
    }

    /// Input/output ring occupancy of one shard, in batches, plus the ring
    /// capacity the caller reports.
    ///
    /// Called by the matcher thread about once per second, never from the
    /// send/recv path: the queued counts are `Relaxed` atomics the producers
    /// and consumers already maintain.
    pub fn set_ring_depths(&self, shard: u32, input: u32, output: u32, capacity: u32) {
        let slot = shard_slot(shard);
        self.ring_input_depth[slot].store(input, Ordering::Relaxed);
        self.ring_output_depth[slot].store(output, Ordering::Relaxed);
        self.ring_capacity[slot].store(capacity, Ordering::Relaxed);
    }

    /// Nonzero ring occupancy as `(shard, input, output, capacity)`, sorted by
    /// shard. A shard is listed when any of the three values is nonzero, so a
    /// ring with a set capacity but no queued batches still exports its
    /// occupancy as zero.
    pub fn ring_depths(&self) -> Vec<(u32, u32, u32, u32)> {
        self.ring_input_depth
            .iter()
            .zip(&self.ring_output_depth)
            .zip(&self.ring_capacity)
            .enumerate()
            .map(|(slot, ((input, output), capacity))| {
                (
                    slot as u32,
                    input.load(Ordering::Relaxed),
                    output.load(Ordering::Relaxed),
                    capacity.load(Ordering::Relaxed),
                )
            })
            .filter(|&(_, input, output, capacity)| input != 0 || output != 0 || capacity != 0)
            .collect()
    }

    pub fn record_ingress(&self) {
        self.orders_ingressed.fetch_add(1, Ordering::Relaxed);
    }

    /// Batched form for a request that admits several orders at once.
    pub fn record_ingress_n(&self, count: u64) {
        if count != 0 {
            self.orders_ingressed.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// One lock acquisition for the lifetime and windowed ingress percentiles.
    pub fn record_ingress_latency_us(&self, micros: u64) {
        let value = micros.min(STAGE_US_MAX);
        if let Ok(mut state) = self.latency.lock() {
            state.rotate_if_due(Instant::now());
            let _ = state.ingress.record(value);
            let _ = state.window_ingress.record(value);
        }
    }

    /// One lock acquisition for the lifetime and windowed match percentiles.
    pub fn record_match_latency_us(&self, micros: u64) {
        let value = micros.min(STAGE_US_MAX);
        if let Ok(mut state) = self.latency.lock() {
            state.rotate_if_due(Instant::now());
            let _ = state.match_latency.record(value);
            let _ = state.window_match.record(value);
        }
    }

    /// Turbo acknowledgement latency: gateway receive to match-complete ack.
    pub fn record_turbo_ack_latency_us(&self, micros: u64) {
        record(&self.turbo_ack, micros);
    }

    /// One lock acquisition for the turbo ack percentiles and sample count.
    pub fn turbo_ack_summary(&self) -> LatencySummary {
        latency_summary(&self.turbo_ack)
    }

    /// Record one stage sample in microseconds.
    ///
    /// Callers split a single request/batch clock, so a batch records one
    /// `GatewayAppend` sample, not one per order.
    pub fn record_stage_us(&self, stage: Stage, micros: u64) {
        record(&self.stages[stage.index()], micros);
    }

    /// p50/p99/p999 per stage, in microseconds, in [`Stage::ALL`] order.
    ///
    /// A stage with no samples reports zeroes, so the Prometheus exposition
    /// always has exactly one series per stage instead of a gap that a graph
    /// would interpolate.
    pub fn stage_snapshot(&self) -> Vec<(Stage, LatencySummary)> {
        Stage::ALL
            .into_iter()
            .map(|stage| (stage, latency_summary(&self.stages[stage.index()])))
            .collect()
    }

    /// Record one ingress pre-trade risk rejection.
    ///
    /// Callers are the gateway and TCP admission paths, which run before any
    /// journal append, so the counter never races with the matcher. Plain
    /// `Relaxed` atomics: no lock on the ingress hot path.
    pub fn record_risk_rejection(&self, reason: RiskRejection) {
        let counter = match reason {
            RiskRejection::OrderQty => &self.risk_rejected_qty,
            RiskRejection::Notional => &self.risk_rejected_notional,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// `(order_qty, notional)` risk rejection totals for the exposition.
    pub fn risk_rejections(&self) -> (u64, u64) {
        (
            self.risk_rejected_qty.load(Ordering::Relaxed),
            self.risk_rejected_notional.load(Ordering::Relaxed),
        )
    }

    pub fn record_execution(&self) {
        self.executions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_order_outcome(&self, rejected: bool) {
        if rejected {
            self.orders_rejected.fetch_add(1, Ordering::Relaxed);
        } else {
            self.orders_matched.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_md_event(&self) {
        self.record_md_events(1);
    }

    pub fn record_trade(&self) {
        self.record_trades(1);
    }

    /// Batch counterpart to repeated `record_md_event` calls. One atomic
    /// read-modify-write per batch instead of one per event.
    fn record_md_events(&self, count: u64) {
        if count != 0 {
            self.md_events.fetch_add(count, Ordering::Relaxed);
        }
    }

    fn record_trades(&self, count: u64) {
        if count != 0 {
            self.trades.fetch_add(count, Ordering::Relaxed);
        }
    }

    pub fn record_engine_command(&self) {
        self.engine_commands.fetch_add(1, Ordering::Relaxed);
    }

    /// Add one shard's counters to the global atomics. Called by
    /// [`ShardMetrics::merge_into`], which swaps its own counters to zero
    /// first, so each recorded event reaches the globals exactly once.
    pub(crate) fn add_match_counters(
        &self,
        commands: u64,
        executions: u64,
        matched: u64,
        rejected: u64,
    ) {
        if commands != 0 {
            self.engine_commands.fetch_add(commands, Ordering::Relaxed);
        }
        if executions != 0 {
            self.executions.fetch_add(executions, Ordering::Relaxed);
        }
        if matched != 0 {
            self.orders_matched.fetch_add(matched, Ordering::Relaxed);
        }
        if rejected != 0 {
            self.orders_rejected.fetch_add(rejected, Ordering::Relaxed);
        }
    }

    /// Drain a shard's match-latency histogram into the global lifetime and
    /// windowed match histograms and clear the source. Both targets are
    /// built by [`new_histogram`], so [`Histogram::add`] cannot fail; the
    /// value-by-value replay is a lossless fallback for a config mismatch
    /// that cannot happen today. The window takes the merged samples too:
    /// the hot path records match latency in `ShardMetrics`, so a merge is
    /// the only way windowed match percentiles ever get samples.
    pub(crate) fn merge_match_latency(&self, source: &mut Histogram<u64>) {
        if source.is_empty() {
            return;
        }
        // A poisoned lock leaves `source` unmerged and uncleaned, so the next
        // merge retries instead of dropping the samples.
        let Ok(mut state) = self.latency.lock() else {
            return;
        };
        state.rotate_if_due(Instant::now());
        add_or_replay(&mut state.match_latency, source);
        add_or_replay(&mut state.window_match, source);
        source.clear();
    }

    /// Drain a shard's per-stage histogram into the global one for `stage`
    /// and clear the source. Same drain-and-clear contract as
    /// [`Metrics::merge_match_latency`]: a merge without new samples is a
    /// no-op, and a poisoned lock leaves the source intact for a retry.
    pub(crate) fn merge_stage(&self, stage: Stage, source: &mut Histogram<u64>) {
        merge_histogram(&self.stages[stage.index()], source);
    }

    #[cfg(test)]
    fn match_latency_sample_count(&self) -> u64 {
        self.latency
            .lock()
            .map(|state| state.match_latency.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn stage_sample_count(&self, stage: Stage) -> u64 {
        self.stages[stage.index()]
            .lock()
            .map(|histogram| histogram.len())
            .unwrap_or(0)
    }

    /// Test hook: rotate the latency window immediately instead of waiting
    /// for it to elapse, so rotation tests do not sleep.
    #[cfg(test)]
    fn rotate_window_now(&self) {
        let Ok(mut state) = self.latency.lock() else {
            return;
        };
        state.window_ingress.clear();
        state.window_match.clear();
        state.window_started = Instant::now();
    }

    /// Percentiles over the rolling latency window, plus the nominal window
    /// length in seconds.
    ///
    /// A window that has expired without new samples reports zeroes: an idle
    /// node has no current latency, and replaying its last window as current
    /// would not be honest. The read path does not rotate; the first record
    /// after expiry opens the next window, so a scrape landing just past the
    /// boundary never discards a populated window it is about to read.
    pub fn window_summary(&self) -> WindowSummary {
        let Ok(state) = self.latency.lock() else {
            return WindowSummary::default();
        };
        let (ingress_p50_us, ingress_p99_us, match_p50_us, match_p99_us) =
            state.window_percentiles(Instant::now());
        WindowSummary {
            ingress_p50_us,
            ingress_p99_us,
            match_p50_us,
            match_p99_us,
            secs: state.window_secs,
        }
    }

    pub fn snapshot(&self) -> MetricsView {
        let uptime = self.started.elapsed().as_secs().max(1);
        let orders = self.orders_ingressed.load(Ordering::Relaxed);
        let rate = {
            let mut state = self.rate.lock().expect("rate mutex poisoned");
            let elapsed = state.at.elapsed().as_secs_f64();
            if elapsed >= 0.5 {
                let delta = orders.saturating_sub(state.orders) as f64;
                state.rate = delta / elapsed;
                state.at = Instant::now();
                state.orders = orders;
            }
            state.rate
        };
        // One lock for the lifetime and rolling-window percentiles. A poisoned
        // lock reports zeroes, exactly as the previous per-histogram reads did.
        // The window expiry rule matches `window_summary`: a read after the
        // window has run reports zeroes rather than a stale tail.
        let (lifetime, window) = match self.latency.lock() {
            Ok(state) => {
                let now = Instant::now();
                let (w50, w99, mw50, mw99) = state.window_percentiles(now);
                (
                    (
                        percentile_of(&state.ingress, 50.0),
                        percentile_of(&state.ingress, 99.0),
                        percentile_of(&state.match_latency, 50.0),
                        percentile_of(&state.match_latency, 99.0),
                    ),
                    WindowSummary {
                        ingress_p50_us: w50,
                        ingress_p99_us: w99,
                        match_p50_us: mw50,
                        match_p99_us: mw99,
                        secs: state.window_secs,
                    },
                )
            }
            Err(_) => ((0.0, 0.0, 0.0, 0.0), WindowSummary::default()),
        };
        let (ingress_p50_us, ingress_p99_us, match_p50_us, match_p99_us) = lifetime;
        MetricsView {
            mode: self.mode,
            orders_ingressed: orders,
            orders_matched: self.orders_matched.load(Ordering::Relaxed),
            orders_rejected: self.orders_rejected.load(Ordering::Relaxed),
            executions: self.executions.load(Ordering::Relaxed),
            trades: self.trades.load(Ordering::Relaxed),
            md_events: self.md_events.load(Ordering::Relaxed),
            engine_commands: self.engine_commands.load(Ordering::Relaxed),
            consumer_lag: self.consumer_lag.load(Ordering::Relaxed),
            ingress_p50_us,
            ingress_p99_us,
            match_p50_us,
            match_p99_us,
            ingress_window_p50_us: window.ingress_p50_us,
            ingress_window_p99_us: window.ingress_p99_us,
            match_window_p50_us: window.match_p50_us,
            match_window_p99_us: window.match_p99_us,
            window_secs: window.secs,
            throughput_orders_s: rate,
            uptime_s: uptime,
            iggy_connected: self.iggy_connected.load(Ordering::Relaxed),
            partition_count: self.partition_count.load(Ordering::Relaxed) as u32,
        }
    }
}

/// Topic order shared by `set_topic_stats` and the Prometheus renderer.
pub const LOG_TOPICS: [&str; 3] = ["orders", "executions", "md"];

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_max(60_000_000, 3).expect("valid histogram bounds")
}

/// Clamp shared by every stage recorder so a locally recorded sample and a
/// directly recorded sample land in identical HDR buckets and merge cleanly.
const STAGE_US_MAX: u64 = 59_999_999;

fn record(histogram: &Mutex<Histogram<u64>>, value: u64) {
    if let Ok(mut histogram) = histogram.lock() {
        let _ = histogram.record(value.min(STAGE_US_MAX));
    }
}

/// Percentile of an unlocked histogram, shared by the lifetime and window
/// read paths. An empty histogram reports zero, never NaN.
fn percentile_of(histogram: &Histogram<u64>, quantile: f64) -> f64 {
    if histogram.is_empty() {
        0.0
    } else {
        histogram.value_at_quantile(quantile / 100.0) as f64
    }
}

/// Drain `source` into `target` under `target`'s mutex and clear the source.
///
/// An empty source is a no-op, so a merge without new records adds nothing.
/// Both histograms are built by [`new_histogram`], so [`Histogram::add`]
/// cannot fail; the value-by-value replay is a lossless fallback for a config
/// mismatch that cannot happen today.
fn merge_histogram(target: &Mutex<Histogram<u64>>, source: &mut Histogram<u64>) {
    if source.is_empty() {
        return;
    }
    // A poisoned lock leaves `source` unmerged and uncleaned, so the next
    // merge retries instead of dropping the samples.
    let Ok(mut histogram) = target.lock() else {
        return;
    };
    add_or_replay(&mut histogram, source);
    source.clear();
}

/// Add `source` into `target` without clearing it, falling back to a
/// value-by-value replay if the HDR configs disagree. Callers clear the
/// source once, after it has reached every target.
fn add_or_replay(target: &mut Histogram<u64>, source: &Histogram<u64>) {
    if target.add(source).is_err() {
        for sample in source.iter_recorded() {
            let value = sample.value_iterated_to();
            for _ in 0..sample.count_at_value() {
                let _ = target.record(value);
            }
        }
    }
}

/// One lock acquisition for all three stage percentiles and the sample count.
fn latency_summary(histogram: &Mutex<Histogram<u64>>) -> LatencySummary {
    histogram
        .lock()
        .map(|histogram| {
            if histogram.is_empty() {
                LatencySummary::default()
            } else {
                LatencySummary {
                    p50_us: histogram.value_at_quantile(0.50) as f64,
                    p99_us: histogram.value_at_quantile(0.99) as f64,
                    p999_us: histogram.value_at_quantile(0.999) as f64,
                    samples: histogram.len(),
                }
            }
        })
        .unwrap_or_default()
}

/// Slot for a shard number: identity below [`METRIC_SHARD_SLOTS`], folded
/// into the last slot at or above it.
fn shard_slot(shard: u32) -> usize {
    (shard as usize).min(METRIC_SHARD_SLOTS - 1)
}

fn micros_since(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

/// Per-shard metrics recorder, owned by one match-loop task.
///
/// The shard thread owns this value, so recording never takes a lock: the
/// counters are plain [`Cell`]s and latency samples go into shard-local HDR
/// histograms behind a [`RefCell`], both uncontended single-thread writes on
/// the same stack the engine already touches. Stage samples are kept in one
/// local histogram per [`Stage`], allocated lazily on the stage's first
/// sample, so a shard that only records `EngineProcess` pays for one
/// histogram instead of nine.
///
/// `merge_into` is the only cross-thread step, and the shard loop calls it
/// periodically (e.g. once per second or on exit): counters are swapped to
/// zero and fetched onto the global atomics, and each non-empty local
/// histogram is drained into the global one under the existing histogram
/// mutex. Because counters swap to zero and a drained histogram clears, a
/// second merge without new records adds nothing, and no recorded sample is
/// ever added twice or lost.
///
/// `ShardMetrics` is `Send` but deliberately not `Sync`: it must move to, and
/// stay on, its shard thread.
pub struct ShardMetrics {
    engine_commands: Cell<u64>,
    executions: Cell<u64>,
    orders_matched: Cell<u64>,
    orders_rejected: Cell<u64>,
    match_latency: RefCell<Histogram<u64>>,
    /// One local histogram per [`Stage`], indexed by `Stage::index`. `None`
    /// until the stage records its first sample.
    stages: [RefCell<Option<Histogram<u64>>>; STAGE_COUNT],
}

impl Default for ShardMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardMetrics {
    pub fn new() -> Self {
        Self {
            engine_commands: Cell::new(0),
            executions: Cell::new(0),
            orders_matched: Cell::new(0),
            orders_rejected: Cell::new(0),
            match_latency: RefCell::new(new_histogram()),
            stages: std::array::from_fn(|_| RefCell::new(None)),
        }
    }

    /// Same clamping as [`Metrics::record_match_latency_us`] so a merged
    /// sample and a directly recorded sample land in identical buckets.
    pub fn record_match_latency_us(&self, micros: u64) {
        let mut histogram = self.match_latency.borrow_mut();
        let _ = histogram.record(micros.min(STAGE_US_MAX));
    }

    /// Record one stage sample in microseconds without any global lock.
    ///
    /// Single-thread counterpart to [`Metrics::record_stage_us`]: the sample
    /// lands in this shard's local histogram for `stage` and reaches the
    /// global one at the next `merge_into`. The clamping matches
    /// [`Metrics::record_stage_us`] exactly, so a merged sample and a
    /// directly recorded sample are bucket-identical and their percentiles
    /// agree within HDR resolution.
    pub fn record_stage_us(&self, stage: Stage, micros: u64) {
        let mut slot = self.stages[stage.index()].borrow_mut();
        let histogram = slot.get_or_insert_with(new_histogram);
        let _ = histogram.record(micros.min(STAGE_US_MAX));
    }

    pub fn record_engine_command(&self) {
        self.engine_commands.set(self.engine_commands.get() + 1);
    }

    pub fn record_execution(&self) {
        self.executions.set(self.executions.get() + 1);
    }

    pub fn record_order_outcome(&self, rejected: bool) {
        if rejected {
            self.orders_rejected.set(self.orders_rejected.get() + 1);
        } else {
            self.orders_matched.set(self.orders_matched.get() + 1);
        }
    }

    /// Hand this shard's records to the global [`Metrics`].
    ///
    /// Counters are read with a swap-to-zero, so they are added exactly once;
    /// each non-empty local histogram (match latency and per-stage) is
    /// drained into its global counterpart and cleared, so a merge without
    /// new records is a no-op. Call this only from the thread that records:
    /// the counters are not atomic.
    pub fn merge_into(&self, global: &Metrics) {
        global.add_match_counters(
            self.engine_commands.replace(0),
            self.executions.replace(0),
            self.orders_matched.replace(0),
            self.orders_rejected.replace(0),
        );
        global.merge_match_latency(&mut self.match_latency.borrow_mut());
        for stage in Stage::ALL {
            let mut slot = self.stages[stage.index()].borrow_mut();
            if let Some(histogram) = slot.as_mut() {
                global.merge_stage(stage, histogram);
            }
        }
    }
}

/// One sequenced feed frame, encoded once and shared by every subscriber.
///
/// `frame` is the serialized [`MdFeedMessage::Delta`] JSON with no trailing
/// newline. The MD consumer thread builds it and the hub broadcasts an
/// `Arc<str>`, so each subscriber pays a refcount bump rather than a second
/// serialization.
#[derive(Debug, Clone)]
pub struct MdFeedEvent {
    pub symbol: Symbol,
    pub seq: u64,
    /// Pre-serialized JSON frame. `Bytes` lets every WebSocket subscriber
    /// clone the buffer (refcount bump) instead of copying the UTF-8 payload
    /// into a fresh `String` per subscriber.
    pub frame: bytes::Bytes,
}

/// Market-data hub. Cloneable handle shared by the consumer, HTTP API, and
/// WebSocket tasks.
pub struct MdHub {
    config: ExchangeConfig,
    state: Mutex<HubState>,
    watch: watch::Sender<Arc<StateView>>,
    /// Sequenced delta feed. Bounded on purpose: the hub keeps one buffer,
    /// never one queue per subscriber.
    feed: broadcast::Sender<Arc<MdFeedEvent>>,
    /// Frames handed to the feed buffer since process start. Stays `0` while
    /// no subscriber is attached: the no-subscriber path encodes nothing.
    feed_sent: AtomicU64,
    /// Set by the first [`MdHub::subscribe_feed`] call. Distinguishes "no
    /// subscriber ever" from "the last subscriber left", which `feed_sent`
    /// alone cannot.
    feed_subscribed: AtomicBool,
    /// `Lagged` results reported by subscribers through
    /// [`MdHub::record_feed_lagged`].
    feed_lagged: AtomicU64,
    /// Resnapshots triggered through [`MdHub::record_feed_resync`].
    feed_resyncs: AtomicU64,
    /// Book-state deltas refused because the symbol had no baseline image.
    /// Nonzero means the producer published a delta before its snapshot, or
    /// the startup snapshot never landed.
    events_without_baseline: AtomicU64,
    metrics: Arc<Metrics>,
    iggy_addr: String,
    journal: String,
    dashboard_build: Option<String>,
}

impl MdHub {
    pub fn new(
        config: ExchangeConfig,
        iggy_addr: String,
        journal: String,
        dashboard_build: Option<String>,
    ) -> Arc<Self> {
        Self::with_feed_capacity(
            config,
            iggy_addr,
            journal,
            dashboard_build,
            MD_FEED_CAPACITY,
        )
    }

    /// Like [`Self::new`] with an explicit feed buffer size, in frames.
    ///
    /// `0` is clamped to `1` because `broadcast::channel` needs a non-zero
    /// capacity. Tests use a small value to reach the lag path without
    /// applying a thousand events.
    pub fn with_feed_capacity(
        config: ExchangeConfig,
        iggy_addr: String,
        journal: String,
        dashboard_build: Option<String>,
        feed_capacity: usize,
    ) -> Arc<Self> {
        let mut books = BTreeMap::new();
        for instrument in &config.instruments {
            books.insert(
                instrument.symbol,
                BookState::new(instrument.symbol, instrument.tick_size_cents),
            );
        }
        let metrics = Arc::new(Metrics::new(config.mode, config.partitions));
        // Seed the watch channel with a fully shaped frame (empty books, no
        // trades). Read-only consumers can call `latest()` before the first
        // 20 Hz publish and still see the configured symbols.
        let symbols: Vec<BookView> = config
            .instruments
            .iter()
            .filter_map(|instrument| books.get(&instrument.symbol))
            .map(BookState::view)
            .collect();
        let initial = Arc::new(StateView {
            mode: config.mode,
            venue: config.venue.clone(),
            symbols,
            trades: Vec::new(),
            metrics: metrics.snapshot(),
            freshness_ms: 0,
            sequence: 0,
            iggy_addr: iggy_addr.clone(),
            journal: journal.clone(),
            guarantee: config.mode.guarantee().to_string(),
            dashboard_build: dashboard_build.clone(),
        });
        let (watch, _) = watch::channel(initial);
        let (feed, _) = broadcast::channel(feed_capacity.max(1));
        Arc::new(Self {
            config,
            state: Mutex::new(HubState {
                books,
                trades: VecDeque::with_capacity(TRADE_TAPE_LEN),
                last_update_ns: 0,
                last_publish: None,
                sequence: 0,
                md_events: 0,
                trades_count: 0,
                dirty: true,
            }),
            watch,
            feed,
            feed_sent: AtomicU64::new(0),
            feed_subscribed: AtomicBool::new(false),
            feed_lagged: AtomicU64::new(0),
            feed_resyncs: AtomicU64::new(0),
            events_without_baseline: AtomicU64::new(0),
            metrics,
            iggy_addr,
            journal,
            dashboard_build,
        })
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<StateView>> {
        self.watch.subscribe()
    }

    /// Subscribe to the sequenced delta feed.
    ///
    /// Frames arrive in apply order as long as one task drives
    /// [`Self::apply_batch`]; concurrent appliers, each keeping per-symbol
    /// order internally, may interleave at batch boundaries. Pair the first
    /// received frames with [`Self::feed_snapshot`]: the snapshot carries the
    /// sequence, and the consumer drops deltas at or below it.
    pub fn subscribe_feed(&self) -> broadcast::Receiver<Arc<MdFeedEvent>> {
        self.feed_subscribed.store(true, Ordering::Relaxed);
        self.feed.subscribe()
    }

    /// Attached feed subscribers. Zero means [`Self::apply_batch`] does no
    /// feed work at all: it neither builds nor serializes a delta.
    pub fn feed_receiver_count(&self) -> usize {
        self.feed.receiver_count()
    }

    /// Book-state deltas refused because the symbol had no baseline image.
    ///
    /// A well-behaved producer publishes its initial snapshot before any
    /// delta, so this stays `0`. A nonzero value means the hub received a
    /// delta first and refused it rather than building depth from nothing;
    /// the book stays empty until an image arrives.
    pub fn events_without_baseline(&self) -> u64 {
        self.events_without_baseline.load(Ordering::Relaxed)
    }

    /// Baseline phase of one symbol's book. `None` for an untracked symbol.
    pub fn book_phase(&self, symbol: Symbol) -> Option<BookPhase> {
        let state = self.state.lock().expect("md state poisoned");
        state.books.get(&symbol).map(|book| book.phase)
    }

    /// Full price-level snapshot of `symbol` at its current sequence.
    ///
    /// Built under the hub lock, so the image cannot straddle a batch, and on
    /// demand rather than at 20 Hz. Unlike [`Self::latest`]'s `BookView`, this
    /// is **not** capped at `BOOK_DEPTH` levels. `None` for a symbol the hub
    /// does not track.
    pub fn feed_snapshot(&self, symbol: Symbol) -> Option<MdFeedSnapshot> {
        let state = self.state.lock().expect("md state poisoned");
        state.books.get(&symbol).map(BookState::feed_snapshot)
    }

    /// `(frames sent, lag reports, resyncs)` for the sequenced feed.
    pub fn feed_stats(&self) -> (u64, u64, u64) {
        (
            self.feed_sent.load(Ordering::Relaxed),
            self.feed_lagged.load(Ordering::Relaxed),
            self.feed_resyncs.load(Ordering::Relaxed),
        )
    }

    /// Record one `Lagged` result observed on a feed receiver. Only the
    /// receiver can see it, so the WS task reports it to the hub.
    pub fn record_feed_lagged(&self) {
        self.feed_lagged.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one resnapshot the WS task asked a subscriber to perform.
    pub fn record_feed_resync(&self) {
        self.feed_resyncs.fetch_add(1, Ordering::Relaxed);
    }

    /// Apply one delta. Thin wrapper so the batch and single-event paths can
    /// never drift.
    pub fn apply(&self, event: &MdEvent) {
        self.apply_batch(std::slice::from_ref(event));
    }

    /// Apply a batch of deltas under one hub-lock acquisition and one clock
    /// read. Events are applied sequentially and in order, so per-symbol
    /// sequence semantics are identical to repeated [`Self::apply`] calls: a
    /// duplicate or stale event is still ignored, and only applied events
    /// advance `sequence`, metrics, and the trade tape.
    pub fn apply_batch(&self, events: &[MdEvent]) {
        if events.is_empty() {
            return;
        }
        // Feed frames are only worth building when someone is listening.
        // `Vec::new()` does not allocate, and the push below is skipped, so a
        // hub with no feed subscriber allocates nothing on this path.
        let broadcasting = self.feed.receiver_count() > 0;
        let mut accepted: Vec<(&MdEvent, i64)> = Vec::new();
        // One clock for the whole batch. The stage records the slice the hub
        // spends under its own lock, including duplicate/stale rejection.
        let started = Instant::now();
        let mut state = self.state.lock().expect("md state poisoned");
        let mut applied = 0u64;
        let mut trades_applied = 0u64;
        let mut no_baseline = 0u64;
        let mut max_seq = 0u64;
        for event in events {
            // Unknown symbols are skipped exactly as `apply` skips them. The
            // instrument lookup exists only to check the price scale in debug
            // builds; in release the book lookup below is the whole check and
            // the linear scan is dead work inside the hub lock.
            #[cfg(debug_assertions)]
            let Some(instrument) = self.config.instrument(event.symbol) else {
                continue;
            };
            let Some(book) = state.books.get_mut(&event.symbol) else {
                continue;
            };
            #[cfg(debug_assertions)]
            debug_assert_eq!(book.tick_size_cents, instrument.tick_size_cents);
            match book.apply(event) {
                ApplyOutcome::Applied => {}
                ApplyOutcome::NoBaseline => {
                    no_baseline += 1;
                    continue;
                }
                ApplyOutcome::Stale | ApplyOutcome::Refused => continue,
            }
            // Read the price scale before touching `state.trades` so the book
            // borrow ends here (the two fields are disjoint, but NLL needs the
            // book borrow to be finished).
            let price_cents = book.price_cents(event.price);
            applied += 1;
            max_seq = max_seq.max(event.seq.0);
            if broadcasting {
                // A duplicate, stale, or refused record never reaches this
                // point, so the feed cannot publish a frame the book rejected.
                accepted.push((event, price_cents));
            }
            if event.kind == MdKind::Trade {
                trades_applied += 1;
                if state.trades.len() == TRADE_TAPE_LEN {
                    state.trades.pop_front();
                }
                state.trades.push_back(TradeView {
                    symbol: event.symbol,
                    price_cents,
                    qty: event.qty.0,
                    side: event.side,
                    order_id: event.order_id,
                    ts_ns: event.timestamp_ns,
                    seq: event.seq,
                });
            }
        }
        // Counted while the hub lock is held so the hub counter and the
        // per-book counters can be compared by `check_invariants`.
        if no_baseline > 0 {
            self.events_without_baseline
                .fetch_add(no_baseline, Ordering::Relaxed);
        }
        if applied == 0 {
            // A batch of duplicates still did work; record it before returning.
            drop(state);
            self.metrics
                .record_stage_us(Stage::MdApply, micros_since(started));
            return;
        }
        self.metrics.record_md_events(applied);
        self.metrics.record_trades(trades_applied);
        state.last_update_ns = now_ns();
        state.sequence = state.sequence.max(max_seq);
        state.md_events = state.md_events.saturating_add(applied);
        state.trades_count = state.trades_count.saturating_add(trades_applied);
        state.dirty = true;
        // Drop the hub lock before touching the histogram mutex or encoding a
        // single feed frame.
        drop(state);
        self.metrics
            .record_stage_us(Stage::MdApply, micros_since(started));
        if broadcasting {
            self.publish_feed(&accepted);
        }
    }

    /// Encode each accepted event once and hand the shared frame to the feed.
    ///
    /// Called after the hub lock is released, so JSON encoding and channel
    /// bookkeeping never block a state reader. Every accepted event is
    /// broadcast, including `MdKind::Status` and `MdKind::Snapshot`: both
    /// advance the symbol's sequence, and skipping them would show the
    /// consumer a hole it cannot distinguish from a lost frame.
    ///
    /// A refused event (an over-reduce, an aggregate overflow, or a delta that
    /// arrived before the symbol's baseline image) advances nothing and is not
    /// broadcast, so the frame stream is exactly the applied-event stream: the
    /// consumer's own sequence check covers the gap, and a baseline-less delta
    /// is dropped on both sides rather than published as book state.
    fn publish_feed(&self, accepted: &[(&MdEvent, i64)]) {
        let mut sent = 0u64;
        for (event, price_cents) in accepted {
            let message = MdFeedMessage::Delta(MdFeedDelta {
                symbol: event.symbol,
                seq: event.seq.0,
                cause_offset: event.cause_offset,
                timestamp_ns: event.timestamp_ns,
                kind: event.kind,
                side: event.side,
                flags: event.flags,
                price_cents: *price_cents,
                qty: event.qty.0,
                order_id: event.order_id,
                participant: event.participant,
            });
            // Integers, enum codes, and a fixed ASCII symbol: infallible while
            // the DTOs stay that shape. Skip rather than panic if that ever
            // changes; the sequence gap makes the consumer resnapshot.
            let Ok(frame) = serde_json::to_vec(&message) else {
                debug_assert!(false, "MdFeedMessage must serialize");
                continue;
            };
            let frame = bytes::Bytes::from(frame);
            let feed_event = Arc::new(MdFeedEvent {
                symbol: event.symbol,
                seq: event.seq.0,
                frame,
            });
            if self.feed.send(feed_event).is_ok() {
                sent += 1;
            }
        }
        if sent > 0 {
            self.feed_sent.fetch_add(sent, Ordering::Relaxed);
        }
    }

    pub fn apply_snapshot(&self, snapshot: &MdSnapshot) {
        let mut state = self.state.lock().expect("md state poisoned");
        let Some(book) = state.books.get_mut(&snapshot.symbol) else {
            return;
        };
        if !book.apply_snapshot(snapshot) {
            return;
        }
        self.metrics.record_md_event();
        state.last_update_ns = now_ns();
        state.sequence = state.sequence.max(snapshot.seq.0);
        state.dirty = true;
    }

    /// Executable hub invariants, covering the books, the tape, the sequence
    /// watermark, and the feed counters. Test-only: it walks every book.
    ///
    /// ```text
    /// books       keys == the configured instruments, each with its own tick
    ///             size, and every book passes `BookState::check_invariants`
    /// sequence    state.sequence == max(book.seq), 0 when no book has state
    /// counters    trades_count <= md_events (a trade is an applied event) and
    ///             the tape holds at most TRADE_TAPE_LEN entries, never more
    ///             than the trades counted
    /// tape        per symbol, seq strictly increases in tape order (the seq
    ///             domain is per symbol, so two symbols may interleave); every
    ///             entry has positive qty and price; the newest entry for a
    ///             symbol is that symbol's highest trade seq and never runs
    ///             past the book's sequence
    /// feed        no subscriber ever => feed_sent == 0; the lag and resync
    ///             counters are monotonic fetch_adds, so only the no-subscriber
    ///             rule needs state here
    /// baseline    the hub counter equals the sum of the per-book counters
    /// ```
    #[cfg(test)]
    pub fn check_invariants(&self) -> Result<(), String> {
        let state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Err("md state mutex is poisoned".into()),
        };
        if state.books.len() != self.config.instruments.len() {
            return Err(format!(
                "{} books for {} configured instruments",
                state.books.len(),
                self.config.instruments.len()
            ));
        }
        let mut no_baseline = 0u64;
        for instrument in &self.config.instruments {
            let book = state
                .books
                .get(&instrument.symbol)
                .ok_or_else(|| format!("configured symbol {} has no book", instrument.symbol))?;
            if book.tick_size_cents != instrument.tick_size_cents {
                return Err(format!(
                    "{}: book tick {} != instrument tick {}",
                    instrument.symbol, book.tick_size_cents, instrument.tick_size_cents
                ));
            }
            book.check_invariants()
                .map_err(|error| format!("{}: {error}", instrument.symbol))?;
            no_baseline = no_baseline.saturating_add(book.events_without_baseline);
        }
        for symbol in state.books.keys() {
            if self.config.instrument(*symbol).is_none() {
                return Err(format!("book {symbol} is not a configured instrument"));
            }
        }
        if self.events_without_baseline.load(Ordering::Relaxed) != no_baseline {
            return Err(format!(
                "hub counted {} baseline-less events, books counted {no_baseline}",
                self.events_without_baseline.load(Ordering::Relaxed)
            ));
        }
        let max_seq = state
            .books
            .values()
            .map(|book| book.seq.0)
            .max()
            .unwrap_or(0);
        if state.sequence != max_seq {
            return Err(format!(
                "hub sequence {} != max book sequence {max_seq}",
                state.sequence
            ));
        }
        if state.trades.len() > TRADE_TAPE_LEN {
            return Err(format!(
                "trade tape holds {} entries, over the {TRADE_TAPE_LEN} bound",
                state.trades.len()
            ));
        }
        if state.trades.len() as u64 > state.trades_count {
            return Err(format!(
                "trade tape holds {} entries but only {} trades were applied",
                state.trades.len(),
                state.trades_count
            ));
        }
        if state.trades_count > state.md_events {
            return Err(format!(
                "{} trades recorded from {} applied events",
                state.trades_count, state.md_events
            ));
        }
        let mut newest: HashMap<Symbol, u64> = HashMap::new();
        for trade in &state.trades {
            if trade.qty == 0 {
                return Err(format!(
                    "tape entry for {} has zero quantity at seq {}",
                    trade.symbol, trade.seq.0
                ));
            }
            if trade.price_cents <= 0 {
                return Err(format!(
                    "tape entry for {} has non-positive price {} at seq {}",
                    trade.symbol, trade.price_cents, trade.seq.0
                ));
            }
            if let Some(previous) = newest.insert(trade.symbol, trade.seq.0)
                && trade.seq.0 <= previous
            {
                return Err(format!(
                    "{}: tape seq {} does not increase past {previous}",
                    trade.symbol, trade.seq.0
                ));
            }
        }
        for (symbol, seq) in newest {
            let book = state
                .books
                .get(&symbol)
                .ok_or_else(|| format!("tape holds a trade for untracked {symbol}"))?;
            if seq > book.seq.0 {
                return Err(format!(
                    "{}: newest tape seq {seq} is ahead of the book sequence {}",
                    symbol, book.seq.0
                ));
            }
        }
        if !self.feed_subscribed.load(Ordering::Relaxed)
            && self.feed_sent.load(Ordering::Relaxed) != 0
        {
            return Err(format!(
                "{} frames were sent with no feed subscriber ever attached",
                self.feed_sent.load(Ordering::Relaxed)
            ));
        }
        Ok(())
    }

    /// Most recently published frame. This is an `Arc` clone, not a rebuild:
    /// use it for read-only consumers such as `GET /api/state`, `GET
    /// /api/trades`, and the initial WebSocket frame.
    pub fn latest(&self) -> Arc<StateView> {
        self.watch.borrow().clone()
    }

    /// A per-subscription slice of the most recently published frame.
    ///
    /// Only the requested `symbols` are cloned, in the configured display
    /// order (not the request order). An empty `symbols` slice means every
    /// configured symbol, which is the full frame. `trade_limit` keeps the
    /// first N trades of the published tape, which is newest-first; `0` keeps
    /// the whole tape. The tape itself is global, not symbol-filtered.
    ///
    /// The returned frame is independent of the watch channel, so the caller
    /// can serialize it on its own task without holding the hub lock.
    pub fn latest_for(&self, symbols: &[Symbol], trade_limit: usize) -> Arc<StateView> {
        let frame = self.latest();
        if symbols.is_empty() && trade_limit == 0 {
            // Nothing to filter: hand back the published Arc, no allocation.
            return frame;
        }
        let symbols: Vec<BookView> = if symbols.is_empty() {
            frame.symbols.clone()
        } else {
            self.config
                .instruments
                .iter()
                .filter(|instrument| symbols.contains(&instrument.symbol))
                .filter_map(|instrument| {
                    frame
                        .symbols
                        .iter()
                        .find(|book| book.symbol == instrument.symbol)
                        .cloned()
                })
                .collect()
        };
        let trades: Vec<TradeView> = if trade_limit == 0 || trade_limit >= frame.trades.len() {
            frame.trades.clone()
        } else {
            frame.trades[..trade_limit].to_vec()
        };
        Arc::new(StateView {
            mode: frame.mode,
            venue: frame.venue.clone(),
            symbols,
            trades,
            metrics: frame.metrics.clone(),
            freshness_ms: frame.freshness_ms,
            sequence: frame.sequence,
            iggy_addr: frame.iggy_addr.clone(),
            journal: frame.journal.clone(),
            guarantee: frame.guarantee.clone(),
            dashboard_build: frame.dashboard_build.clone(),
        })
    }

    /// Counters and latency percentiles without building a state frame.
    ///
    /// Intended call sites:
    /// - `GET /metrics`: `ctx.hub.metrics_snapshot()` for the counters, plus
    ///   `ctx.hub.latest()` if the exposition also needs `freshness_ms` and
    ///   `sequence`. Do not call `state()` there.
    /// - health checks: `ctx.metrics.snapshot()` already avoids the hub state
    ///   lock entirely.
    pub fn metrics_snapshot(&self) -> MetricsView {
        self.metrics.snapshot()
    }

    /// Build a fresh frame and publish it. Coalesces back-to-back calls when
    /// nothing changed, but never suppresses a scheduled 20 Hz tick.
    pub fn publish(&self) {
        if let Some(state) = self.build_publish_state() {
            // `send` refuses to update the stored value when there are no
            // receivers; `latest()` must still advance, so use `send_replace`.
            self.watch.send_replace(Arc::new(state));
        }
    }

    /// Build a fresh frame without touching the publish cadence or the dirty
    /// flag. Tests and tools use this; HTTP/WS read paths should use
    /// `latest()` instead.
    pub fn state(&self) -> StateView {
        let state = self.state.lock().expect("md state poisoned");
        let parts = self.state_parts(&state);
        drop(state);
        self.finish(parts)
    }

    fn build_publish_state(&self) -> Option<StateView> {
        let mut state = self.state.lock().expect("md state poisoned");
        let now = Instant::now();
        // Freshness still updates on the next 20 Hz tick. This window is below
        // the publisher cadence, so it only collapses redundant manual calls
        // (for example a caller that publishes after every applied event).
        if !state.dirty
            && state
                .last_publish
                .is_some_and(|last| now.duration_since(last) < PUBLISH_COALESCE_WINDOW)
        {
            return None;
        }
        state.dirty = false;
        state.last_publish = Some(now);
        let parts = self.state_parts(&state);
        drop(state);
        Some(self.finish(parts))
    }

    /// Build the symbol views and trade tape while the hub lock is held. The
    /// caller must drop the guard before `finish`, which snapshots metrics
    /// (and therefore locks the histograms and the rate estimator).
    fn state_parts(&self, state: &HubState) -> StateParts {
        let now = now_ns();
        let freshness_ms = if state.last_update_ns == 0 {
            0
        } else {
            now.saturating_sub(state.last_update_ns) / 1_000_000
        };
        let symbols: Vec<BookView> = self
            .config
            .instruments
            .iter()
            .filter_map(|instrument| state.books.get(&instrument.symbol))
            .map(BookState::view)
            .collect();
        // One clone per trade, with the capacity reserved up front. The tape is
        // small and bounded by TRADE_TAPE_LEN.
        let mut trades = Vec::with_capacity(state.trades.len());
        trades.extend(state.trades.iter().rev().cloned());
        StateParts {
            symbols,
            trades,
            freshness_ms,
            sequence: state.sequence,
        }
    }

    fn finish(&self, parts: StateParts) -> StateView {
        StateView {
            mode: self.config.mode,
            venue: self.config.venue.clone(),
            symbols: parts.symbols,
            trades: parts.trades,
            metrics: self.metrics.snapshot(),
            freshness_ms: parts.freshness_ms,
            sequence: parts.sequence,
            iggy_addr: self.iggy_addr.clone(),
            journal: self.journal.clone(),
            guarantee: self.config.mode.guarantee().to_string(),
            dashboard_build: self.dashboard_build.clone(),
        }
    }

    /// Spawn the 20 Hz conflated publisher. One task, no per-client work.
    pub fn spawn_publisher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(PUBLISH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                hub.publish();
            }
        })
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
    use whippet_core::protocol::{MdEvent, MdKind, MdOrder, MdSnapshot, Side};

    fn config() -> ExchangeConfig {
        ExchangeConfig::default()
    }

    /// The producer's startup image: an empty full snapshot for one symbol at
    /// sequence 0, carrying the open, running status a fresh engine reports.
    ///
    /// A hub refuses book-state deltas until a symbol has a baseline, exactly
    /// as the engine's initial MD snapshot provides one before any delta. A
    /// test that applies deltas has to seed first, or it is modelling a
    /// producer order that cannot happen.
    fn seed_symbol(hub: &MdHub, symbol: Symbol) {
        hub.apply_snapshot(&MdSnapshot {
            symbol,
            seq: Seq(0),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: Vec::new(),
            asks: Vec::new(),
            orders: Vec::new(),
            flags: SessionState::Open.to_md_flags(),
        });
    }

    /// Seed every configured symbol with an empty image at sequence 0.
    fn seed(hub: &MdHub) {
        for instrument in &hub.config.instruments {
            seed_symbol(hub, instrument.symbol);
        }
    }

    /// Full snapshot of one symbol's book, built from `(order_id, side,
    /// price_ticks, qty)` tuples. The seq is the image's own sequence.
    fn full_snapshot(symbol: Symbol, seq: u64, orders: &[(u128, Side, i64, u64)]) -> MdSnapshot {
        MdSnapshot {
            symbol,
            seq: Seq(seq),
            cause_offset: seq,
            timestamp_ns: seq,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: Vec::new(),
            asks: Vec::new(),
            orders: orders
                .iter()
                .map(|&(order_id, side, price, qty)| MdOrder {
                    order_id: OrderId(order_id),
                    side,
                    price: Price(price),
                    qty: Qty(qty),
                    participant: ParticipantId(1),
                })
                .collect(),
            flags: SessionState::Open.to_md_flags(),
        }
    }

    /// Deltas applied since the hub started, i.e. `HubState::md_events`
    /// (snapshots are counted in `Metrics`, not here).
    fn applied_deltas(hub: &MdHub) -> u64 {
        hub.state.lock().expect("md state").md_events
    }

    fn hub_sequence(hub: &MdHub) -> u64 {
        hub.state.lock().expect("md state").sequence
    }

    fn btc() -> Symbol {
        Symbol::new("BTC-USD").unwrap()
    }

    fn eth() -> Symbol {
        Symbol::new("ETH-USD").unwrap()
    }

    fn event(kind: MdKind, side: Side, price: i64, qty: u64, order_id: u128) -> MdEvent {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(1);
        MdEvent {
            kind,
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(SEQ.fetch_add(1, Ordering::Relaxed)),
            cause_offset: 0,
            timestamp_ns: 1,
            side,
            flags: 0,
            price: Price(price),
            qty: Qty(qty),
            order_id: OrderId(order_id),
            participant: ParticipantId(1),
        }
    }

    #[test]
    fn add_reduce_remove_updates_levels() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        hub.apply(&event(MdKind::Add, Side::Buy, 65_000, 10, 1));
        hub.apply(&event(MdKind::Add, Side::Buy, 65_000, 5, 2));
        let state = hub.state();
        let btc = state
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 15);
        assert_eq!(btc.bids[0].orders, 2);

        hub.apply(&event(MdKind::Reduce, Side::Buy, 65_000, 3, 1));
        let btc = hub
            .state()
            .symbols
            .into_iter()
            .find(|b| b.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 8);

        hub.apply(&event(MdKind::Remove, Side::Buy, 65_000, 0, 1));
        let btc = hub
            .state()
            .symbols
            .into_iter()
            .find(|b| b.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 5);
        assert_eq!(btc.bids[0].orders, 1);
    }

    #[test]
    fn duplicate_delta_is_ignored() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        let add = event(MdKind::Add, Side::Buy, 65_000, 10, 1);
        hub.apply(&add);
        hub.apply(&add);
        let state = hub.state();
        let btc = state
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 10);
        assert_eq!(btc.bids[0].orders, 1);
    }

    #[test]
    fn trade_updates_last_trade_and_tape() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        hub.apply(&event(MdKind::Add, Side::Sell, 65_100, 10, 1));
        hub.apply(&event(MdKind::Trade, Side::Buy, 65_100, 4, 2));
        let state = hub.state();
        let btc = state
            .symbols
            .iter()
            .find(|b| b.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.last_trade_price_cents, Some(6_510_000));
        assert_eq!(btc.last_trade_qty, 4);
        assert_eq!(state.trades.len(), 1);
    }

    #[test]
    fn snapshot_orders_support_following_deltas() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let snapshot = MdSnapshot {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(0),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: vec![whippet_core::protocol::MdLevel {
                price: Price(65_001),
                qty: Qty(7),
                orders: 1,
            }],
            asks: vec![],
            orders: vec![whippet_core::protocol::MdOrder {
                order_id: OrderId(77),
                side: Side::Buy,
                price: Price(65_001),
                qty: Qty(7),
                participant: ParticipantId(1),
            }],
            flags: 0,
        };
        hub.apply_snapshot(&snapshot);
        let mut reduce = event(MdKind::Reduce, Side::Buy, 65_001, 3, 77);
        reduce.seq = Seq(1);
        hub.apply(&reduce);
        let btc = hub
            .state()
            .symbols
            .into_iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 3);
        let mut remove = event(MdKind::Remove, Side::Buy, 65_001, 0, 77);
        remove.seq = Seq(2);
        hub.apply(&remove);
        let btc = hub
            .state()
            .symbols
            .into_iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert!(btc.bids.is_empty());
    }

    fn status_event(flags: u8, seq: u64) -> MdEvent {
        MdEvent {
            kind: MdKind::Status,
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(seq),
            cause_offset: 0,
            timestamp_ns: 1,
            side: Side::Buy,
            flags,
            price: Price::ZERO,
            qty: Qty::ZERO,
            order_id: OrderId(0),
            participant: ParticipantId(1),
        }
    }

    fn btc_book(state: &StateView) -> BookView {
        state
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .cloned()
            .expect("BTC-USD book")
    }

    #[test]
    fn status_sets_and_clears_the_halt_flag() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        assert!(!btc_book(&hub.state()).halted);

        hub.apply(&status_event(MD_FLAG_HALTED, 1));
        assert!(btc_book(&hub.state()).halted);

        hub.apply(&status_event(0, 2));
        assert!(!btc_book(&hub.state()).halted);
    }

    /// Session bits ride above the halt bit in the status flags and land in
    /// `BookView.session`; a stale (same- or older-seq) status event cannot
    /// regress either.
    #[test]
    fn status_session_bits_set_the_book_session() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        assert_eq!(btc_book(&hub.state()).session, SessionState::Open);

        hub.apply(&status_event(SessionState::PreOpen.to_md_flags(), 1));
        assert_eq!(btc_book(&hub.state()).session, SessionState::PreOpen);

        // Halt and session share one status event without colliding.
        hub.apply(&status_event(
            MD_FLAG_HALTED | SessionState::Closed.to_md_flags(),
            2,
        ));
        let book = btc_book(&hub.state());
        assert_eq!(book.session, SessionState::Closed);
        assert!(book.halted);

        // A stale redelivery with different bits must not regress anything.
        hub.apply(&status_event(SessionState::Open.to_md_flags(), 2));
        hub.apply(&status_event(SessionState::Open.to_md_flags(), 1));
        let book = btc_book(&hub.state());
        assert_eq!(book.session, SessionState::Closed);
        assert!(book.halted);

        hub.apply(&status_event(SessionState::Open.to_md_flags(), 3));
        let book = btc_book(&hub.state());
        assert_eq!(book.session, SessionState::Open);
        assert!(!book.halted);
    }

    /// An out-of-range session code is not a session the hub can name, so the
    /// current session stays standing while the event still applies.
    #[test]
    fn status_with_an_invalid_session_code_keeps_the_current_session() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        hub.apply(&status_event(SessionState::PreOpen.to_md_flags(), 1));
        hub.apply(&status_event(0b11 << 1, 2));
        assert_eq!(btc_book(&hub.state()).session, SessionState::PreOpen);
    }

    /// A snapshot's flags carry the session the same way a status event does,
    /// and a stale snapshot cannot regress it.
    #[test]
    fn snapshot_flags_set_the_session() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let make = |flags: u8, seq: u64| MdSnapshot {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(seq),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: vec![],
            asks: vec![],
            orders: vec![],
            flags,
        };
        assert_eq!(btc_book(&hub.state()).session, SessionState::Open);

        hub.apply_snapshot(&make(SessionState::Closed.to_md_flags(), 1));
        assert_eq!(btc_book(&hub.state()).session, SessionState::Closed);

        hub.apply_snapshot(&make(SessionState::PreOpen.to_md_flags(), 1));
        assert_eq!(btc_book(&hub.state()).session, SessionState::Closed);
    }

    #[test]
    fn stale_status_is_ignored() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        hub.apply(&status_event(MD_FLAG_HALTED, 2));
        assert!(btc_book(&hub.state()).halted);

        // Same seq (a redelivery) and an older seq must not clear the halt.
        hub.apply(&status_event(0, 2));
        assert!(btc_book(&hub.state()).halted);
        hub.apply(&status_event(0, 1));
        assert!(btc_book(&hub.state()).halted);
    }

    #[test]
    fn status_leaves_the_book_and_tape_untouched() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        let mut add = event(MdKind::Add, Side::Sell, 65_100, 10, 1);
        add.seq = Seq(1);
        hub.apply(&add);
        let mut trade = event(MdKind::Trade, Side::Buy, 65_100, 4, 2);
        trade.seq = Seq(2);
        hub.apply(&trade);
        let before = btc_book(&hub.state());

        hub.apply(&status_event(MD_FLAG_HALTED, 3));
        let after = btc_book(&hub.state());

        assert!(after.halted);
        let levels = |levels: &[LevelView]| {
            levels
                .iter()
                .map(|level| (level.price_cents, level.qty, level.orders))
                .collect::<Vec<_>>()
        };
        assert_eq!(levels(&after.bids), levels(&before.bids));
        assert_eq!(levels(&after.asks), levels(&before.asks));
        assert_eq!(after.best_bid_cents, before.best_bid_cents);
        assert_eq!(after.best_ask_cents, before.best_ask_cents);
        assert_eq!(after.last_trade_price_cents, before.last_trade_price_cents);
        assert_eq!(after.last_trade_qty, before.last_trade_qty);
        assert_eq!(after.last_trade_side, before.last_trade_side);
        assert_eq!(after.depth_bid_qty, before.depth_bid_qty);
        assert_eq!(after.depth_ask_qty, before.depth_ask_qty);
        // The status is a real applied event: it advances the per-symbol seq
        // and the hub sequence, but it is not a trade.
        assert_eq!(after.seq, Seq(3));
        let state = hub.state();
        assert_eq!(state.sequence, 3);
        assert_eq!(state.trades.len(), 1);
        assert_eq!(
            state.metrics.md_events, 4,
            "one seed image plus three deltas"
        );
        assert_eq!(state.metrics.trades, 1);
    }

    #[test]
    fn duplicate_trade_is_not_added_to_the_tape_or_metrics_twice() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        let mut add = event(MdKind::Add, Side::Sell, 65_100, 10, 1);
        add.seq = Seq(1);
        hub.apply(&add);
        let mut trade = event(MdKind::Trade, Side::Buy, 65_100, 4, 2);
        trade.seq = Seq(2);
        hub.apply(&trade);
        hub.apply(&trade);
        let state = hub.state();
        assert_eq!(state.trades.len(), 1);
        assert_eq!(state.metrics.trades, 1);
        assert_eq!(
            state.metrics.md_events, 3,
            "one seed image, the add, and the first trade"
        );
    }

    #[test]
    fn stale_snapshot_does_not_regress_the_book() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let make = |seq: u64, price: i64| MdSnapshot {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(seq),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: vec![whippet_core::protocol::MdLevel {
                price: Price(price),
                qty: Qty(1),
                orders: 1,
            }],
            asks: vec![],
            orders: vec![whippet_core::protocol::MdOrder {
                order_id: OrderId(1),
                side: Side::Buy,
                price: Price(price),
                qty: Qty(1),
                participant: ParticipantId(1),
            }],
            flags: 0,
        };
        hub.apply_snapshot(&make(5, 65_005));
        hub.apply_snapshot(&make(3, 65_003));
        let state = hub.state();
        let btc = state
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.best_bid_cents, Some(6_500_500));
        assert_eq!(btc.seq, Seq(5));
    }

    #[test]
    fn snapshot_replaces_levels() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, Symbol::new("BTC-USD").unwrap());
        hub.apply(&event(MdKind::Add, Side::Buy, 65_000, 10, 1));
        let snapshot = MdSnapshot {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(1000),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price(65_050),
            last_trade_qty: Qty(2),
            bids: vec![whippet_core::protocol::MdLevel {
                price: Price(65_001),
                qty: Qty(7),
                orders: 1,
            }],
            asks: vec![],
            orders: vec![whippet_core::protocol::MdOrder {
                order_id: OrderId(77),
                side: Side::Buy,
                price: Price(65_001),
                qty: Qty(7),
                participant: ParticipantId(1),
            }],
            flags: 0,
        };
        hub.apply_snapshot(&snapshot);
        let state = hub.state();
        let btc = state
            .symbols
            .iter()
            .find(|b| b.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.best_bid_cents, Some(6_500_100));
        assert_eq!(btc.bids[0].qty, 7);
        assert_eq!(state.sequence, 1000);
    }

    #[test]
    fn latest_returns_the_last_published_frame() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&hub);

        // The watch channel is seeded with the configured symbols, so a read
        // before the first publish is still a valid frame.
        let initial = hub.latest();
        assert_eq!(initial.symbols.len(), 3);
        assert_eq!(initial.sequence, 0);

        let add = event(MdKind::Add, Side::Buy, 65_000, 10, 1);
        let first_seq = add.seq;
        hub.apply(&add);
        hub.publish();

        let first = hub.latest();
        assert_eq!(first.sequence, first_seq.0);
        let btc = first
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 10);

        // Applying without publishing must not move the published frame.
        hub.apply(&event(MdKind::Add, Side::Buy, 65_000, 5, 2));
        assert!(Arc::ptr_eq(&first, &hub.latest()));

        hub.publish();
        let second = hub.latest();
        assert!(!Arc::ptr_eq(&first, &second));
        let btc = second
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 15);
        assert_eq!(btc.bids[0].orders, 2);
    }

    #[test]
    fn metrics_snapshot_does_not_rebuild_the_published_frame() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        let before = hub.latest();

        hub.apply(&event(MdKind::Trade, Side::Buy, 65_000, 4, 1));
        let metrics = hub.metrics_snapshot();
        assert_eq!(metrics.md_events, 2, "the seed snapshot plus one delta");
        assert_eq!(metrics.trades, 1);
        // The read path did not build or publish a new state frame.
        assert!(Arc::ptr_eq(&before, &hub.latest()));
    }

    #[test]
    fn stage_indexes_and_names_are_stable() {
        for (index, stage) in Stage::ALL.iter().enumerate() {
            assert_eq!(stage.index(), index, "ALL must follow declaration order");
            assert!(!stage.as_str().is_empty());
        }
        // The exposition labels are unique and snake_case.
        let mut names: Vec<&str> = Stage::ALL.iter().map(|stage| stage.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Stage::ALL.len());
    }

    #[test]
    fn stage_snapshot_reports_percentiles_per_stage() {
        let metrics = Metrics::new(config().mode, config().partitions);
        for sample in 1..=1_000u64 {
            metrics.record_stage_us(Stage::GatewayParse, sample);
        }
        metrics.record_stage_us(Stage::GatewayAppend, 42);

        let snapshot = metrics.stage_snapshot();
        assert_eq!(snapshot.len(), Stage::ALL.len());
        let summary = |stage: Stage| {
            snapshot
                .iter()
                .find(|(recorded, _)| *recorded == stage)
                .map(|(_, summary)| *summary)
                .expect("every stage is in the snapshot")
        };

        let parse = summary(Stage::GatewayParse);
        assert_eq!(parse.samples, 1_000);
        // HDR buckets at 3 significant digits: assert a tight band, not the
        // exact sample.
        assert!(
            (495.0..=505.0).contains(&parse.p50_us),
            "p50 = {}",
            parse.p50_us
        );
        assert!(
            (985.0..=995.0).contains(&parse.p99_us),
            "p99 = {}",
            parse.p99_us
        );
        assert!(
            (995.0..=1_000.0).contains(&parse.p999_us),
            "p999 = {}",
            parse.p999_us
        );
        assert!(parse.p50_us <= parse.p99_us && parse.p99_us <= parse.p999_us);

        // Stages are independent histograms, not one shared window.
        let append = summary(Stage::GatewayAppend);
        assert_eq!(append.samples, 1);
        assert_eq!(append.p50_us, 42.0);
        assert_eq!(append.p99_us, 42.0);

        // A stage with no samples reports zeroes and says so.
        let ws = summary(Stage::WsFrame);
        assert_eq!(ws.samples, 0);
        assert_eq!(ws.p50_us, 0.0);
        assert_eq!(ws.p99_us, 0.0);
        assert_eq!(ws.p999_us, 0.0);
    }

    #[test]
    fn apply_batch_records_one_md_apply_sample_per_batch() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&hub);
        let metrics = hub.metrics();
        let md_apply = |metrics: &Metrics| {
            metrics
                .stage_snapshot()
                .into_iter()
                .find(|(stage, _)| *stage == Stage::MdApply)
                .map(|(_, summary)| summary)
                .expect("md_apply is always in the snapshot")
        };

        hub.apply_batch(&mixed_stream());
        assert_eq!(md_apply(&metrics).samples, 1);

        // A replay of the same stream is all duplicates: the batch still did
        // work, so it still records exactly one sample.
        hub.apply_batch(&mixed_stream());
        assert_eq!(md_apply(&metrics).samples, 2);

        // An empty batch is not a request: nothing is recorded.
        hub.apply_batch(&[]);
        assert_eq!(md_apply(&metrics).samples, 2);
    }

    #[test]
    fn duplicate_and_stale_events_leave_the_published_frame_correct() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed_symbol(&hub, btc());
        let add = event(MdKind::Add, Side::Buy, 65_000, 10, 1);
        hub.apply(&add);
        let trade = event(MdKind::Trade, Side::Buy, 65_000, 4, 2);
        hub.apply(&trade);
        hub.publish();
        let baseline = hub.latest();

        // At-least-once replay: the add and trade are applied again.
        hub.apply(&add);
        hub.apply(&trade);
        // A stale reduce below the last applied sequence is ignored.
        let mut stale = event(MdKind::Reduce, Side::Buy, 65_000, 1, 1);
        stale.seq = Seq(add.seq.0.saturating_sub(1));
        hub.apply(&stale);

        hub.publish();
        let after = hub.latest();
        let btc = after
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids[0].qty, 10);
        assert_eq!(btc.bids[0].orders, 1);
        assert_eq!(after.trades.len(), 1);
        assert_eq!(
            after.metrics.md_events, 3,
            "one seed image, the add, and the trade"
        );
        assert_eq!(after.metrics.trades, 1);
        assert_eq!(after.sequence, baseline.sequence);
    }

    #[test]
    fn snapshot_plus_deltas_publishes_the_full_book() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let symbol = Symbol::new("BTC-USD").unwrap();
        // 45 orders across three price levels. `view()` must aggregate every
        // level for depth, not just the 20 levels shown in the ladder.
        let orders: Vec<MdOrder> = (0..45u128)
            .map(|index| MdOrder {
                order_id: OrderId(index + 1),
                side: Side::Buy,
                price: Price(65_000 + (index % 3) as i64),
                qty: Qty(1),
                participant: ParticipantId(1),
            })
            .collect();
        let snapshot = MdSnapshot {
            symbol,
            seq: Seq(1),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: vec![],
            asks: vec![],
            orders,
            flags: 0,
        };
        hub.apply_snapshot(&snapshot);
        hub.publish();

        let state = hub.latest();
        let btc = state
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids.len(), 3);
        assert_eq!(btc.depth_bid_qty, 45);
        assert_eq!(btc.best_bid_cents, Some(6_500_200));

        let mut remove = event(MdKind::Remove, Side::Buy, 65_000, 0, 1);
        remove.seq = Seq(2);
        hub.apply(&remove);
        hub.publish();

        let state = hub.latest();
        let btc = state
            .symbols
            .iter()
            .find(|book| book.symbol.as_str() == "BTC-USD")
            .unwrap();
        assert_eq!(btc.bids.len(), 3);
        assert_eq!(btc.depth_bid_qty, 44);
        assert_eq!(btc.best_bid_cents, Some(6_500_200));
    }

    fn event_at(
        kind: MdKind,
        symbol: Symbol,
        seq: u64,
        side: Side,
        price_ticks: i64,
        qty: u64,
        order_id: u128,
    ) -> MdEvent {
        MdEvent {
            kind,
            symbol,
            seq: Seq(seq),
            cause_offset: seq,
            timestamp_ns: seq,
            side,
            flags: 0,
            price: Price(price_ticks),
            qty: Qty(qty),
            order_id: OrderId(order_id),
            participant: ParticipantId(1),
        }
    }

    fn level_signature(levels: &[LevelView]) -> Vec<(i64, u64, u32)> {
        levels
            .iter()
            .map(|level| (level.price_cents, level.qty, level.orders))
            .collect()
    }

    fn assert_book_eq(left: &BookView, right: &BookView) {
        assert_eq!(left.symbol, right.symbol);
        assert_eq!(left.seq, right.seq);
        assert_eq!(level_signature(&left.bids), level_signature(&right.bids));
        assert_eq!(level_signature(&left.asks), level_signature(&right.asks));
        assert_eq!(left.best_bid_cents, right.best_bid_cents);
        assert_eq!(left.best_ask_cents, right.best_ask_cents);
        assert_eq!(left.spread_cents, right.spread_cents);
        assert_eq!(left.mid_cents, right.mid_cents);
        assert_eq!(left.last_trade_price_cents, right.last_trade_price_cents);
        assert_eq!(left.last_trade_qty, right.last_trade_qty);
        assert_eq!(left.last_trade_side, right.last_trade_side);
        assert_eq!(left.depth_bid_qty, right.depth_bid_qty);
        assert_eq!(left.depth_ask_qty, right.depth_ask_qty);
    }

    fn assert_state_eq(left: &StateView, right: &StateView) {
        assert_eq!(left.sequence, right.sequence);
        assert_eq!(left.metrics.md_events, right.metrics.md_events);
        assert_eq!(left.metrics.trades, right.metrics.trades);
        assert_eq!(left.symbols.len(), right.symbols.len());
        for (left, right) in left.symbols.iter().zip(&right.symbols) {
            assert_book_eq(left, right);
        }
        assert_eq!(left.trades.len(), right.trades.len());
        for (left, right) in left.trades.iter().zip(&right.trades) {
            assert_eq!(left.symbol, right.symbol);
            assert_eq!(left.price_cents, right.price_cents);
            assert_eq!(left.qty, right.qty);
            assert_eq!(left.side, right.side);
            assert_eq!(left.order_id, right.order_id);
            assert_eq!(left.ts_ns, right.ts_ns);
            assert_eq!(left.seq, right.seq);
        }
    }

    /// A mixed stream with duplicates, stale records, an unknown symbol, an
    /// over-reduce, and a trade per symbol.
    fn mixed_stream() -> Vec<MdEvent> {
        let btc = Symbol::new("BTC-USD").unwrap();
        let eth = Symbol::new("ETH-USD").unwrap();
        let sol = Symbol::new("SOL-USD").unwrap();
        let unknown = Symbol::new("XRP-USD").unwrap();
        vec![
            event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1),
            event_at(MdKind::Add, btc, 2, Side::Buy, 65_000, 5, 2),
            event_at(MdKind::Add, eth, 1, Side::Sell, 15_000, 7, 3),
            event_at(MdKind::Add, sol, 1, Side::Buy, 5_000, 9, 4),
            event_at(MdKind::Reduce, btc, 3, Side::Buy, 65_000, 3, 1),
            // Duplicate of the second add: ignored.
            event_at(MdKind::Add, btc, 2, Side::Buy, 65_000, 5, 2),
            // Stale reduce: ignored.
            event_at(MdKind::Reduce, btc, 2, Side::Buy, 65_000, 1, 2),
            event_at(MdKind::Trade, btc, 4, Side::Buy, 65_000, 4, 1),
            event_at(MdKind::Trade, eth, 2, Side::Sell, 15_000, 2, 3),
            event_at(MdKind::Remove, btc, 5, Side::Buy, 65_000, 0, 2),
            // Unknown symbol: skipped, like `apply`.
            event_at(MdKind::Add, unknown, 1, Side::Buy, 100, 3, 5),
            event_at(MdKind::Add, btc, 6, Side::Buy, 65_001, 8, 6),
            // No-op reduce (same quantity) still counts as applied.
            event_at(MdKind::Reduce, eth, 3, Side::Sell, 15_000, 7, 3),
            // Over-reduce: refused whole, so the sequence does not advance.
            event_at(MdKind::Reduce, btc, 7, Side::Buy, 65_000, 99, 1),
            event_at(MdKind::Add, btc, 8, Side::Buy, 65_002, 2, 7),
            event_at(MdKind::Trade, btc, 9, Side::Sell, 65_002, 1, 7),
            // Duplicate of the last trade: ignored.
            event_at(MdKind::Trade, btc, 9, Side::Sell, 65_002, 1, 7),
        ]
    }

    #[test]
    fn apply_batch_matches_repeated_apply() {
        let stream = mixed_stream();

        let single = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&single);
        for event in &stream {
            single.apply(event);
        }

        // Batch boundaries must not change the result, including when a chunk
        // splits a duplicate from the event it replays.
        let chunked = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&chunked);
        for chunk in stream.chunks(3) {
            chunked.apply_batch(chunk);
        }

        let once = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&once);
        once.apply_batch(&stream);

        for hub in [&single, &chunked, &once] {
            hub.check_invariants()
                .expect("batch shaping is invariant-free");
        }
        let single = single.state();
        let chunked = chunked.state();
        let once = once.state();
        assert_state_eq(&single, &chunked);
        assert_state_eq(&single, &once);
        assert_eq!(single.metrics.trades, 3);
        assert_eq!(single.sequence, 9);
    }

    #[test]
    fn incremental_depth_matches_full_fold() {
        let btc = Symbol::new("BTC-USD").unwrap();
        let mut book = BookState::new(btc, 100);
        // A baseline image first: without one, the book refuses deltas by
        // design and this test would be measuring nothing.
        assert!(book.apply_snapshot(&full_snapshot(btc, 0, &[])));
        assert_eq!(book.phase, BookPhase::Seeded);
        // Deterministic xorshift so a failure is reproducible.
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut seq = 0u64;
        let mut next_id = 1u128;
        // order id -> (side, price ticks, remaining qty)
        let mut live: HashMap<u128, (Side, i64, u64)> = HashMap::new();
        let mut history: Vec<MdEvent> = Vec::new();

        for step in 0..4_000 {
            let action = next() % 100;
            let event = if action < 35 || live.is_empty() {
                let side = if next() % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = 65_000 + (next() % 8) as i64;
                let qty = 1 + next() % 50;
                let id = next_id;
                next_id += 1;
                seq += 1;
                live.insert(id, (side, price, qty));
                event_at(MdKind::Add, btc, seq, side, price, qty, id)
            } else if action < 60 {
                let index = (next() as usize) % live.len();
                let (&id, &(side, price, qty)) = live.iter().nth(index).unwrap();
                let new_qty = next() % (qty + 1);
                seq += 1;
                if new_qty == 0 {
                    live.remove(&id);
                } else {
                    live.insert(id, (side, price, new_qty));
                }
                event_at(MdKind::Reduce, btc, seq, side, price, new_qty, id)
            } else if action < 80 {
                let index = (next() as usize) % live.len();
                let (&id, &(side, price, _)) = live.iter().nth(index).unwrap();
                live.remove(&id);
                seq += 1;
                event_at(MdKind::Remove, btc, seq, side, price, 0, id)
            } else if action < 90 && !history.is_empty() {
                // Replay a past record: a duplicate or stale event.
                let index = (next() as usize) % history.len();
                history[index].clone()
            } else {
                // A record below the current sequence.
                seq += 1;
                let stale_seq = seq.saturating_sub(2);
                event_at(
                    MdKind::Reduce,
                    btc,
                    stale_seq,
                    Side::Buy,
                    65_000,
                    1,
                    u128::MAX,
                )
            };
            book.apply(&event);
            history.push(event);
            assert_eq!(
                book.depth_bid_qty,
                total_depth(&book.bids),
                "bid depth diverged at step {step}"
            );
            assert_eq!(
                book.depth_ask_qty,
                total_depth(&book.asks),
                "ask depth diverged at step {step}"
            );
        }

        // A snapshot rebuild must recompute the totals from scratch.
        let mut orders: Vec<MdOrder> = live
            .iter()
            .map(|(&id, &(side, price, qty))| MdOrder {
                order_id: OrderId(id),
                side,
                price: Price(price),
                qty: Qty(qty),
                participant: ParticipantId(1),
            })
            .collect();
        orders.sort_by_key(|order| order.order_id.0);
        book.apply_snapshot(&MdSnapshot {
            symbol: btc,
            seq: Seq(seq + 1),
            cause_offset: 0,
            timestamp_ns: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            bids: vec![],
            asks: vec![],
            orders,
            flags: 0,
        });
        assert_eq!(book.depth_bid_qty, total_depth(&book.bids));
        assert_eq!(book.depth_ask_qty, total_depth(&book.asks));

        let view = book.view();
        assert_eq!(view.depth_bid_qty, total_depth(&book.bids));
        assert_eq!(view.depth_ask_qty, total_depth(&book.asks));
    }

    #[test]
    fn latest_for_clones_only_requested_symbols_and_trades() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&hub);
        let btc = Symbol::new("BTC-USD").unwrap();
        let eth = Symbol::new("ETH-USD").unwrap();
        let sol = Symbol::new("SOL-USD").unwrap();

        let mut seq = 0u64;
        for symbol in [btc, eth, sol] {
            for level in 0..5i64 {
                seq += 1;
                hub.apply(&event_at(
                    MdKind::Add,
                    symbol,
                    seq,
                    Side::Buy,
                    100 + level,
                    10,
                    u128::from(seq),
                ));
            }
        }
        for index in 0..70u64 {
            seq += 1;
            let symbol = if index % 2 == 0 { btc } else { eth };
            hub.apply(&event_at(
                MdKind::Trade,
                symbol,
                seq,
                Side::Buy,
                100,
                1,
                1_000_000 + u128::from(index),
            ));
        }
        hub.publish();

        let filtered = hub.latest_for(&[eth], 60);
        assert_eq!(filtered.symbols.len(), 1);
        assert_eq!(filtered.symbols[0].symbol, eth);
        assert_eq!(filtered.trades.len(), 60);
        // The published tape is newest-first, so the limit keeps the newest.
        assert_eq!(filtered.trades[0].seq, Seq(seq));
        assert_eq!(filtered.trades[59].seq, Seq(seq - 59));

        // Empty symbol slice means every configured symbol.
        let all = hub.latest_for(&[], 5);
        assert_eq!(all.symbols.len(), 3);
        assert_eq!(all.trades.len(), 5);

        // Zero means the whole tape.
        let full_tape = hub.latest_for(&[btc], 0);
        assert_eq!(full_tape.symbols.len(), 1);
        assert_eq!(full_tape.trades.len(), 70);

        // Request order does not matter; configured order wins.
        let ordered = hub.latest_for(&[sol, btc], 0);
        let order: Vec<Symbol> = ordered.symbols.iter().map(|book| book.symbol).collect();
        assert_eq!(order, vec![btc, sol]);

        // The shared full frame is untouched.
        let latest = hub.latest();
        assert_eq!(latest.symbols.len(), 3);
        assert_eq!(latest.trades.len(), 70);
        hub.check_invariants()
            .expect("a filtered read cannot hurt the hub");
    }

    fn decode_feed(frame: &[u8]) -> MdFeedMessage {
        serde_json::from_slice(frame).expect("feed frame is JSON")
    }

    #[test]
    fn feed_snapshot_reflects_levels_seq_and_halt_state() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = Symbol::new("BTC-USD").unwrap();
        seed_symbol(&hub, btc);
        let bid = event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1);
        hub.apply(&bid);
        hub.apply(&event_at(MdKind::Add, btc, 2, Side::Buy, 65_000, 5, 2));
        hub.apply(&event_at(MdKind::Add, btc, 3, Side::Sell, 65_100, 7, 3));
        let mut halt = event_at(MdKind::Status, btc, 4, Side::Buy, 0, 0, 0);
        halt.flags = MD_FLAG_HALTED;
        hub.apply(&halt);

        let snapshot = hub.feed_snapshot(btc).expect("BTC-USD snapshot");
        assert_eq!(snapshot.symbol, btc);
        assert_eq!(snapshot.seq, 4);
        // The snapshot names the input offset that produced its sequence.
        assert_eq!(snapshot.cause_offset, 4);
        assert!(snapshot.halted);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.bids[0].price_cents, 6_500_000);
        assert_eq!(snapshot.bids[0].qty, 15);
        assert_eq!(snapshot.bids[0].orders, 2);
        assert_eq!(snapshot.asks.len(), 1);
        assert_eq!(snapshot.asks[0].qty, 7);

        // A symbol the hub does not track has no image.
        let unknown = Symbol::new("XRP-USD").unwrap();
        assert!(hub.feed_snapshot(unknown).is_none());

        // An untouched configured symbol is a valid, empty snapshot.
        let eth = Symbol::new("ETH-USD").unwrap();
        let empty = hub.feed_snapshot(eth).expect("configured symbol");
        assert_eq!(empty.seq, 0);
        assert_eq!(empty.cause_offset, 0);
        assert!(empty.bids.is_empty() && empty.asks.is_empty());
        assert!(!empty.halted);
        assert_eq!(
            hub.book_phase(eth),
            Some(BookPhase::AwaitingSnapshot),
            "a symbol with no image is still awaiting its baseline"
        );
        assert_eq!(hub.book_phase(btc), Some(BookPhase::Seeded));
        hub.check_invariants().expect("invariants hold");
    }

    #[test]
    fn feed_snapshot_is_not_capped_at_the_dashboard_depth() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = Symbol::new("BTC-USD").unwrap();
        seed_symbol(&hub, btc);
        // 25 distinct bid levels and 25 ask levels: more than BOOK_DEPTH.
        for level in 0..25i64 {
            hub.apply(&event_at(
                MdKind::Add,
                btc,
                2 * level as u64 + 1,
                Side::Buy,
                65_000 - level,
                1,
                level as u128 + 1,
            ));
            hub.apply(&event_at(
                MdKind::Add,
                btc,
                2 * level as u64 + 2,
                Side::Sell,
                65_001 + level,
                1,
                100 + level as u128,
            ));
        }

        let snapshot = hub.feed_snapshot(btc).expect("BTC-USD snapshot");
        assert_eq!(snapshot.bids.len(), 25);
        assert_eq!(snapshot.asks.len(), 25);
        // Best-first on both sides: highest bid, lowest ask.
        assert_eq!(snapshot.bids[0].price_cents, 6_500_000);
        assert_eq!(snapshot.bids[24].price_cents, 6_497_600);
        assert_eq!(snapshot.asks[0].price_cents, 6_500_100);
        assert_eq!(snapshot.asks[24].price_cents, 6_502_500);
        // Bids descend strictly, asks ascend strictly.
        assert!(
            snapshot
                .bids
                .windows(2)
                .all(|pair| pair[0].price_cents > pair[1].price_cents)
        );
        assert!(
            snapshot
                .asks
                .windows(2)
                .all(|pair| pair[0].price_cents < pair[1].price_cents)
        );

        // The dashboard view keeps its own 20-level window.
        let book = btc_book(&hub.state());
        assert_eq!(book.bids.len(), BOOK_DEPTH);
        assert_eq!(book.asks.len(), BOOK_DEPTH);
    }

    #[test]
    fn feed_is_silent_without_subscribers() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        seed(&hub);
        assert_eq!(hub.feed_receiver_count(), 0);

        hub.apply_batch(&mixed_stream());
        hub.apply(&event(MdKind::Add, Side::Buy, 65_003, 1, 99));

        assert_eq!(hub.feed_receiver_count(), 0);
        // No frame was built, so nothing was counted and nothing panicked.
        assert_eq!(hub.feed_stats(), (0, 0, 0));
        // The state still applies: the feed is an output, not the apply path.
        assert!(applied_deltas(&hub) > 0);
        assert!(btc_book(&hub.state()).bids.len() >= 2);
        hub.check_invariants().expect("invariants hold");
    }

    #[test]
    fn subscribed_feed_delivers_every_applied_event_in_sequence() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = Symbol::new("BTC-USD").unwrap();
        seed_symbol(&hub, btc);
        let mut receiver = hub.subscribe_feed();
        assert_eq!(hub.feed_receiver_count(), 1);

        hub.apply(&event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1));
        hub.apply(&event_at(MdKind::Reduce, btc, 2, Side::Buy, 65_000, 4, 1));
        let mut halt = event_at(MdKind::Status, btc, 3, Side::Buy, 0, 0, 0);
        halt.flags = MD_FLAG_HALTED;
        hub.apply(&halt);

        let mut frames = Vec::new();
        for _ in 0..3 {
            let frame = receiver.try_recv().expect("queued frame");
            assert_eq!(frame.symbol, btc);
            frames.push(frame);
        }
        assert!(receiver.try_recv().is_err(), "exactly three frames");

        let seqs: Vec<u64> = frames.iter().map(|frame| frame.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
        // Encoding happens once; all subscribers share the same buffer.
        let expected = [
            (MdKind::Add, Side::Buy, 6_500_000, 10),
            (MdKind::Reduce, Side::Buy, 6_500_000, 4),
            (MdKind::Status, Side::Buy, 0, 0),
        ];
        for (frame, (kind, side, price_cents, qty)) in frames.iter().zip(expected) {
            match decode_feed(&frame.frame) {
                MdFeedMessage::Delta(delta) => {
                    assert_eq!(delta.symbol, btc);
                    assert_eq!(delta.seq, frame.seq);
                    assert_eq!(delta.cause_offset, frame.seq);
                    assert_eq!(delta.timestamp_ns, frame.seq);
                    assert_eq!(delta.kind, kind);
                    assert_eq!(delta.side, side);
                    assert_eq!(delta.price_cents, price_cents);
                    assert_eq!(delta.qty, qty);
                }
                other => panic!("expected a delta frame, got {other:?}"),
            }
        }
        match decode_feed(&frames[2].frame) {
            MdFeedMessage::Delta(delta) => assert_eq!(delta.flags, MD_FLAG_HALTED),
            other => panic!("expected a delta frame, got {other:?}"),
        }
        assert_eq!(hub.feed_stats(), (3, 0, 0));
        hub.check_invariants().expect("invariants hold");
    }

    /// Every accepted event gets exactly one frame, in apply order, per symbol
    /// strictly increasing; rejected events get none. The tape and the feed
    /// are outputs of the same apply decision, so a frame must never exist for
    /// an event the book ignored.
    #[test]
    fn feed_frames_track_accepted_events_across_symbols() {
        let hub = MdHub::with_feed_capacity(config(), "test".into(), "memory".into(), None, 16);
        let btc = Symbol::new("BTC-USD").unwrap();
        let eth = Symbol::new("ETH-USD").unwrap();
        seed(&hub);
        let mut receiver = hub.subscribe_feed();

        let stream = vec![
            event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1),
            event_at(MdKind::Add, eth, 1, Side::Sell, 15_000, 7, 2),
            // A duplicate of the first add: accepted once, no second frame.
            event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1),
            event_at(MdKind::Add, btc, 2, Side::Buy, 65_001, 5, 3),
            event_at(MdKind::Reduce, eth, 2, Side::Sell, 15_000, 3, 2),
            // A stale event below the applied sequence: silent.
            event_at(MdKind::Add, eth, 1, Side::Sell, 15_000, 7, 9),
            event_at(MdKind::Trade, btc, 3, Side::Buy, 65_001, 2, 3),
        ];
        hub.apply_batch(&stream);

        let mut frames = Vec::new();
        while let Ok(frame) = receiver.try_recv() {
            frames.push(frame);
        }
        let applied: Vec<(Symbol, u64)> = vec![(btc, 1), (eth, 1), (btc, 2), (eth, 2), (btc, 3)];
        let got: Vec<(Symbol, u64)> = frames
            .iter()
            .map(|frame| (frame.symbol, frame.seq))
            .collect();
        assert_eq!(got, applied, "one frame per accepted event, in apply order");
        assert_eq!(
            hub.feed_stats(),
            (applied.len() as u64, 0, 0),
            "feed_sent counts exactly the frames a drained receiver got"
        );
        assert_eq!(hub.feed_receiver_count(), 1);
        hub.check_invariants().expect("invariants hold");
    }

    /// The lag path at the smallest buffer: a `Lagged` notice, the hub's two
    /// counters, and a fresh image at or past the last applied sequence.
    #[test]
    fn feed_lag_at_capacity_one_resyncs_from_a_current_image() {
        let hub = MdHub::with_feed_capacity(config(), "test".into(), "memory".into(), None, 1);
        let btc = Symbol::new("BTC-USD").unwrap();
        seed_symbol(&hub, btc);
        let mut receiver = hub.subscribe_feed();

        for seq in 1..=3u64 {
            hub.apply(&event_at(
                MdKind::Add,
                btc,
                seq,
                Side::Buy,
                65_000 + seq as i64,
                1,
                seq as u128,
            ));
        }
        match receiver.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(dropped)) => {
                assert_eq!(dropped, 2, "capacity 1 retains only the newest frame");
            }
            other => panic!("expected a lag report, got {other:?}"),
        }
        hub.record_feed_lagged();
        hub.record_feed_resync();
        assert_eq!(hub.feed_stats(), (3, 1, 1));

        let snapshot = hub.feed_snapshot(btc).expect("BTC-USD snapshot");
        let state = btc_book(&hub.state());
        assert_eq!(snapshot.seq, state.seq.0);
        assert_eq!(snapshot.seq, 3, "the image is at the last applied seq");
        // Everything the lagging receiver missed is in the image it adopts.
        assert_eq!(snapshot.bids.len(), 3);
        assert_eq!(snapshot.bids.iter().map(|level| level.qty).sum::<u64>(), 3);

        // The oldest retained frame is still queued; drain it before the next
        // apply, or the receiver lags again on the way.
        assert_eq!(receiver.try_recv().expect("retained frame").seq, 3);
        hub.apply(&event_at(MdKind::Add, btc, 4, Side::Buy, 65_004, 1, 4));
        assert_eq!(receiver.try_recv().expect("post-lag frame").seq, 4);
    }

    #[test]
    fn feed_skips_duplicate_and_stale_events() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = Symbol::new("BTC-USD").unwrap();
        seed_symbol(&hub, btc);
        let mut receiver = hub.subscribe_feed();
        let add = event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1);
        hub.apply(&add);
        hub.apply(&add);
        // Stale: below the applied sequence.
        hub.apply(&event_at(MdKind::Reduce, btc, 1, Side::Buy, 65_000, 4, 1));

        let first = receiver.try_recv().expect("one frame");
        assert_eq!(first.seq, 1);
        assert!(receiver.try_recv().is_err(), "rejected events are silent");
        assert_eq!(hub.feed_stats().0, 1);
    }

    #[test]
    fn feed_reports_lag_and_counts_sent_frames() {
        // Capacity 2: five published frames overwrite the receiver's view.
        let hub = MdHub::with_feed_capacity(config(), "test".into(), "memory".into(), None, 2);
        let btc = Symbol::new("BTC-USD").unwrap();
        seed_symbol(&hub, btc);
        let mut receiver = hub.subscribe_feed();
        for seq in 1..=5u64 {
            hub.apply(&event_at(
                MdKind::Add,
                btc,
                seq,
                Side::Buy,
                65_000 + seq as i64,
                1,
                seq as u128,
            ));
        }

        // Every frame reached the bounded buffer; the receiver lost the old
        // ones instead of the hub growing a queue.
        assert_eq!(hub.feed_stats(), (5, 0, 0));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(dropped)) if dropped > 0
        ));
        // The receiver reports the lag; the hub only keeps the counter.
        hub.record_feed_lagged();
        assert_eq!(hub.feed_stats(), (5, 1, 0));
        hub.record_feed_resync();
        assert_eq!(hub.feed_stats(), (5, 1, 1));

        // After a `Lagged` the receiver is positioned at the oldest retained
        // frame, so it resumes with the tail of the buffer. The lost frames
        // are what the resync is for.
        assert_eq!(receiver.try_recv().expect("retained frame").seq, 4);
        assert_eq!(receiver.try_recv().expect("retained frame").seq, 5);

        // It resynchronises from a fresh snapshot and continues.
        let snapshot = hub.feed_snapshot(btc).expect("BTC-USD snapshot");
        assert_eq!(snapshot.seq, 5);
        assert_eq!(snapshot.bids.len(), 5);
        hub.apply(&event_at(MdKind::Add, btc, 6, Side::Buy, 65_100, 1, 6));
        let frame = receiver.try_recv().expect("post-lag frame");
        assert_eq!(frame.seq, 6);
    }

    /// The book's own invariants across a hand-written lifecycle, including
    /// the shapes the randomized run produces: partial reduces, a reduce to
    /// zero, a remove, a trade, and a snapshot that clears the last trade side.
    #[test]
    fn book_invariants_hold_across_the_delta_lifecycle() {
        let btc = btc();
        let mut book = BookState::new(btc, 100);
        assert_eq!(book.phase, BookPhase::AwaitingSnapshot);
        book.check_invariants()
            .expect("an empty book is consistent");

        assert!(book.apply_snapshot(&full_snapshot(
            btc,
            1,
            &[(1, Side::Buy, 65_000, 10), (2, Side::Sell, 65_100, 7)]
        )));
        assert_eq!(book.phase, BookPhase::Seeded);
        book.check_invariants()
            .expect("a seeded book is consistent");

        assert_eq!(
            book.apply(&event_at(MdKind::Add, btc, 2, Side::Buy, 65_000, 5, 3)),
            ApplyOutcome::Applied
        );
        assert_eq!(
            book.apply(&event_at(MdKind::Reduce, btc, 3, Side::Buy, 65_000, 4, 3)),
            ApplyOutcome::Applied
        );
        assert_eq!(book.depth_bid_qty, 14);
        book.check_invariants()
            .expect("adds and partial reduces hold");

        // A reduce to zero is a cancel: the ref, the level quantity and the
        // level's order count all leave, so no zero-quantity level survives.
        assert_eq!(
            book.apply(&event_at(MdKind::Reduce, btc, 4, Side::Buy, 65_000, 0, 3)),
            ApplyOutcome::Applied
        );
        assert_eq!(book.bids.get(&6_500_000).map(|level| level.qty), Some(10));
        assert_eq!(book.depth_bid_qty, 10);
        book.check_invariants()
            .expect("a reduce to zero is a cancel");

        assert_eq!(
            book.apply(&event_at(MdKind::Remove, btc, 5, Side::Buy, 65_000, 0, 1)),
            ApplyOutcome::Applied
        );
        assert!(
            book.bids.is_empty(),
            "the last bid level leaves with its order"
        );
        assert_eq!(book.depth_bid_qty, 0);
        book.check_invariants().expect("a remove holds");

        assert_eq!(
            book.apply(&event_at(MdKind::Trade, btc, 6, Side::Sell, 65_100, 3, 2)),
            ApplyOutcome::Applied
        );
        assert_eq!(book.last_trade_price_cents, Some(6_510_000));
        assert_eq!(book.last_trade_side, Some(Side::Sell));
        book.check_invariants().expect("a trade holds");

        // A snapshot keeps qty and price but drops the side: the image says
        // what traded, not who aggressed.
        assert!(book.apply_snapshot(&full_snapshot(btc, 7, &[(4, Side::Sell, 65_100, 7)])));
        assert_eq!(book.last_trade_side, None);
        assert_eq!(book.last_trade_qty, 0);
        assert_eq!(book.last_trade_price_cents, None);
        book.check_invariants().expect("a snapshot image holds");
    }

    #[test]
    fn check_invariants_detects_a_depth_drift() {
        let btc = btc();
        let mut book = BookState::new(btc, 100);
        assert!(book.apply_snapshot(&full_snapshot(btc, 1, &[(1, Side::Buy, 65_000, 10)])));
        book.depth_bid_qty += 1;
        let error = book
            .check_invariants()
            .expect_err("depth drift is detected");
        assert!(error.contains("cached bid depth"), "{error}");
    }

    #[test]
    fn check_invariants_detects_a_zero_quantity_level() {
        let btc = btc();
        let mut book = BookState::new(btc, 100);
        assert!(book.apply_snapshot(&full_snapshot(btc, 1, &[(1, Side::Buy, 65_000, 10)])));
        book.bids
            .insert(6_500_000, LevelState { qty: 0, orders: 1 });
        let error = book
            .check_invariants()
            .expect_err("a zero-qty level is detected");
        assert!(error.contains("zero quantity"), "{error}");
    }

    #[test]
    fn check_invariants_detects_a_crossed_book() {
        let btc = btc();
        let mut book = BookState::new(btc, 100);
        assert!(book.apply_snapshot(&full_snapshot(
            btc,
            1,
            &[(1, Side::Buy, 65_100, 10), (2, Side::Sell, 65_000, 7)]
        )));
        let error = book.check_invariants().expect_err("a cross is detected");
        assert!(error.contains("crossed book"), "{error}");
    }

    #[test]
    fn check_invariants_detects_a_zero_quantity_order_ref() {
        let btc = btc();
        let mut book = BookState::new(btc, 100);
        assert!(book.apply_snapshot(&full_snapshot(btc, 1, &[(1, Side::Buy, 65_000, 10)])));
        book.orders.get_mut(&OrderId(1)).expect("ref").qty = 0;
        let error = book
            .check_invariants()
            .expect_err("a zero-quantity ref is detected");
        assert!(error.contains("zero quantity"), "{error}");
    }

    #[test]
    fn check_invariants_detects_a_level_aggregate_mismatch() {
        let btc = btc();
        let mut book = BookState::new(btc, 100);
        assert!(book.apply_snapshot(&full_snapshot(btc, 1, &[(1, Side::Buy, 65_000, 10)])));
        book.bids.get_mut(&6_500_000).expect("level").qty += 1;
        book.depth_bid_qty += 1;
        let error = book
            .check_invariants()
            .expect_err("a level that disagrees with its refs is detected");
        assert!(error.contains("order refs"), "{error}");
    }

    /// A delta before any image is refused, counted, and does not advance the
    /// sequence; the image that follows seeds the book and deltas resume.
    #[test]
    fn delta_before_a_snapshot_is_refused_and_the_image_seeds_the_book() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = btc();
        hub.apply(&event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1));
        hub.apply(&event_at(MdKind::Trade, btc, 2, Side::Buy, 65_000, 4, 1));

        assert_eq!(hub.events_without_baseline(), 2);
        assert_eq!(hub.book_phase(btc), Some(BookPhase::AwaitingSnapshot));
        let book = btc_book(&hub.state());
        assert!(book.bids.is_empty(), "no depth was invented from a delta");
        assert_eq!(book.seq, Seq(0), "a refused delta does not advance seq");
        assert_eq!(book.last_trade_price_cents, None);
        assert_eq!(hub_sequence(&hub), 0);

        // The image that follows covers the refused deltas: it is the state
        // through a later sequence, so applying it loses nothing.
        hub.apply_snapshot(&full_snapshot(btc, 5, &[(1, Side::Buy, 65_000, 10)]));
        assert_eq!(hub.book_phase(btc), Some(BookPhase::Seeded));
        let book = btc_book(&hub.state());
        assert_eq!(book.seq, Seq(5));
        assert_eq!(book.bids[0].qty, 10);

        // Deltas apply on top of the image from here on.
        hub.apply(&event_at(MdKind::Add, btc, 6, Side::Buy, 65_001, 3, 2));
        let book = btc_book(&hub.state());
        assert_eq!(book.seq, Seq(6));
        assert_eq!(book.depth_bid_qty, 13);
        assert_eq!(hub.events_without_baseline(), 2, "the seed reset nothing");
        hub.check_invariants().expect("invariants hold");
    }

    /// A status event carries no depth, so it applies before a baseline and
    /// advances the sequence exactly as it does today. An image at that same
    /// sequence is still accepted: the replayed status left no book state to
    /// be stale against.
    #[test]
    fn status_applies_before_a_snapshot_and_seeds_at_the_same_sequence() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = btc();
        let mut halted = event_at(MdKind::Status, btc, 4, Side::Buy, 0, 0, 0);
        halted.flags = MD_FLAG_HALTED;
        hub.apply(&halted);

        assert!(btc_book(&hub.state()).halted);
        assert_eq!(btc_book(&hub.state()).seq, Seq(4));
        assert_eq!(hub.book_phase(btc), Some(BookPhase::AwaitingSnapshot));
        assert_eq!(hub.events_without_baseline(), 0);

        hub.apply_snapshot(&full_snapshot(btc, 4, &[(1, Side::Buy, 65_000, 10)]));
        assert_eq!(hub.book_phase(btc), Some(BookPhase::Seeded));
        let book = btc_book(&hub.state());
        // The image is authoritative for the status too.
        assert!(!book.halted);
        assert_eq!(book.bids[0].qty, 10);
        hub.check_invariants().expect("invariants hold");
    }

    /// Once seeded, the phase never goes back: a newer image replaces the
    /// book, and a stale image is refused without touching depth or phase.
    #[test]
    fn a_newer_snapshot_replaces_a_seeded_book_and_a_stale_one_does_not() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = btc();
        seed_symbol(&hub, btc);
        hub.apply(&event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1));
        hub.apply(&event_at(MdKind::Add, btc, 2, Side::Buy, 65_002, 4, 2));
        assert_eq!(btc_book(&hub.state()).bids.len(), 2);

        // The image is authoritative: levels it omits are gone even though no
        // delta removed them.
        hub.apply_snapshot(&full_snapshot(btc, 3, &[(3, Side::Sell, 65_100, 6)]));
        let book = btc_book(&hub.state());
        assert!(book.bids.is_empty());
        assert_eq!(book.asks[0].qty, 6);
        assert_eq!(book.depth_bid_qty, 0);
        assert_eq!(book.seq, Seq(3));

        hub.apply_snapshot(&full_snapshot(btc, 2, &[(1, Side::Buy, 65_000, 10)]));
        let book = btc_book(&hub.state());
        assert!(book.bids.is_empty(), "the stale image was refused whole");
        assert_eq!(book.seq, Seq(3));
        assert_eq!(hub.book_phase(btc), Some(BookPhase::Seeded));
        hub.check_invariants().expect("invariants hold");
    }

    #[test]
    fn hub_invariants_detect_a_sequence_and_tick_drift() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = btc();
        seed_symbol(&hub, btc);
        hub.apply(&event_at(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1));
        hub.check_invariants().expect("invariants hold");

        {
            let mut state = hub.state.lock().expect("md state");
            state.sequence += 1;
        }
        let error = hub
            .check_invariants()
            .expect_err("a sequence drift is detected");
        assert!(error.contains("hub sequence"), "{error}");

        {
            let mut state = hub.state.lock().expect("md state");
            state.sequence -= 1;
            state.books.get_mut(&btc).expect("book").tick_size_cents = 1;
        }
        let error = hub
            .check_invariants()
            .expect_err("a tick drift is detected");
        assert!(error.contains("book tick"), "{error}");
    }

    /// Deterministic SplitMix64. A fixed stream is what makes a failure
    /// reproducible from the seed alone.
    struct SplitMix64(u64);

    impl SplitMix64 {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
    }

    type MdModel = BTreeMap<(Symbol, u128), (Side, i64, u64)>;

    fn live_order_ids(model: &MdModel, symbol: Symbol) -> Vec<u128> {
        model
            .keys()
            .filter(|(modeled, _)| *modeled == symbol)
            .map(|(_, order_id)| *order_id)
            .collect()
    }

    /// A free id from the 32-id pool, or `None` when every id is resting.
    fn free_order_id(model: &MdModel, symbol: Symbol, rng: &mut SplitMix64) -> Option<u128> {
        for _ in 0..32 {
            let candidate = u128::from(1 + rng.below(32));
            if !model.contains_key(&(symbol, candidate)) {
                return Some(candidate);
            }
        }
        (1..=32u128).find(|order_id| !model.contains_key(&(symbol, *order_id)))
    }

    /// The full image of `symbol` at `seq`, built from the model. `level_only`
    /// publishes the same state as aggregates without refs, the shape a
    /// producer with no order-level history leaves behind.
    fn model_snapshot(model: &MdModel, symbol: Symbol, seq: u64, level_only: bool) -> MdSnapshot {
        let orders: Vec<(u128, Side, i64, u64)> = model
            .iter()
            .filter(|((modeled, _), _)| *modeled == symbol)
            .map(|((_, order_id), (side, price, qty))| (*order_id, *side, *price, *qty))
            .collect();
        let mut snapshot = full_snapshot(symbol, seq, &orders);
        if !level_only {
            return snapshot;
        }
        let mut bids: BTreeMap<i64, (u64, u32)> = BTreeMap::new();
        let mut asks: BTreeMap<i64, (u64, u32)> = BTreeMap::new();
        for &(_, side, price, qty) in &orders {
            let levels = if side == Side::Buy {
                &mut bids
            } else {
                &mut asks
            };
            let entry = levels.entry(price).or_default();
            entry.0 += qty;
            entry.1 += 1;
        }
        let levels = |levels: BTreeMap<i64, (u64, u32)>| -> Vec<whippet_core::protocol::MdLevel> {
            levels
                .into_iter()
                .map(|(price, (qty, orders))| whippet_core::protocol::MdLevel {
                    price: Price(price),
                    qty: Qty(qty),
                    orders,
                })
                .collect()
        };
        snapshot.bids = levels(bids);
        snapshot.asks = levels(asks);
        snapshot.orders = Vec::new();
        snapshot
    }

    /// 2,000 deterministic mixed events over two symbols, checked after every
    /// apply. The price bands are disjoint per side, so nothing here can
    /// produce a crossed book: a cross is what the invariant is for, not
    /// something the generator is allowed to create.
    #[test]
    fn randomized_invariants_hold_over_a_mixed_stream() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
        let btc = btc();
        let eth = eth();
        let symbols = [btc, eth];
        let mids = [(btc, 65_000i64), (eth, 30_000i64)];
        let mid_of = |symbol: Symbol| {
            mids.iter()
                .find(|(modeled, _)| *modeled == symbol)
                .map(|(_, mid)| *mid)
                .expect("configured symbol")
        };

        // Coverage pin: deltas before any baseline image are refused without
        // building depth, while a status event still applies.
        hub.apply(&event_at(MdKind::Add, btc, 1, Side::Buy, 64_990, 10, 100));
        hub.apply(&event_at(MdKind::Reduce, btc, 2, Side::Buy, 64_990, 5, 100));
        let mut halted = event_at(MdKind::Status, btc, 3, Side::Buy, 0, 0, 0);
        halted.flags = MD_FLAG_HALTED;
        hub.apply(&halted);
        assert_eq!(hub.events_without_baseline(), 2);
        assert!(btc_book(&hub.state()).bids.is_empty());

        // The producer's startup image for each symbol, at the sequence the
        // prelude left the book on.
        hub.apply_snapshot(&full_snapshot(btc, 4, &[]));
        hub.apply_snapshot(&full_snapshot(eth, 1, &[]));
        assert_eq!(hub.book_phase(btc), Some(BookPhase::Seeded));
        assert_eq!(hub.book_phase(eth), Some(BookPhase::Seeded));

        let mut rng = SplitMix64(0x5eed_1234_9abc_def0);
        let mut model: MdModel = BTreeMap::new();
        let mut seqs: HashMap<Symbol, u64> = HashMap::from([(btc, 4), (eth, 1)]);
        let mut history: Vec<Vec<MdEvent>> = Vec::new();
        let mut both_books_non_empty = false;
        let mut trades = 0u64;
        let mut statuses = 0u64;
        let mut accepted_snapshots = 0u64;
        let mut level_only_snapshots = 0u64;
        let mut stale_replays = 0u64;

        for step in 0..2_000u32 {
            let mut group: Vec<MdEvent> = Vec::new();
            let len = 1 + rng.below(4);
            for _ in 0..len {
                let symbol = symbols[rng.below(2) as usize];
                let mid = mid_of(symbol);
                let seq = {
                    let next = seqs.get_mut(&symbol).expect("configured symbol");
                    *next += 1;
                    *next
                };
                let live = live_order_ids(&model, symbol);
                let action = rng.below(100);
                let event = if action < 45 || live.is_empty() {
                    if live.len() >= 24 {
                        continue;
                    }
                    let Some(order_id) = free_order_id(&model, symbol, &mut rng) else {
                        continue;
                    };
                    let side = if rng.below(2) == 0 {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let offset = 1 + rng.below(10) as i64;
                    let price = if side == Side::Buy {
                        mid - offset
                    } else {
                        mid + offset
                    };
                    let qty = 1 + rng.below(20);
                    model.insert((symbol, order_id), (side, price, qty));
                    event_at(MdKind::Add, symbol, seq, side, price, qty, order_id)
                } else if action < 65 {
                    let order_id = live[rng.below(live.len() as u64) as usize];
                    let &(side, price, qty) = model.get(&(symbol, order_id)).expect("live order");
                    let new_qty = rng.below(qty + 1);
                    if new_qty == 0 {
                        model.remove(&(symbol, order_id));
                    } else {
                        model.insert((symbol, order_id), (side, price, new_qty));
                    }
                    event_at(MdKind::Reduce, symbol, seq, side, price, new_qty, order_id)
                } else if action < 78 {
                    let order_id = live[rng.below(live.len() as u64) as usize];
                    let &(side, price, _) = model.get(&(symbol, order_id)).expect("live order");
                    model.remove(&(symbol, order_id));
                    event_at(MdKind::Remove, symbol, seq, side, price, 0, order_id)
                } else if action < 88 {
                    // A trade prices inside the instrument's band and never
                    // touches depth, so it cannot cross the book.
                    let side = if rng.below(2) == 0 {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let offset = rng.below(10) as i64;
                    let price = if side == Side::Buy {
                        mid - offset
                    } else {
                        mid + offset
                    };
                    trades += 1;
                    event_at(
                        MdKind::Trade,
                        symbol,
                        seq,
                        side,
                        price,
                        1 + rng.below(20),
                        u128::from(1_000_000 + seq),
                    )
                } else {
                    statuses += 1;
                    let mut status = event_at(MdKind::Status, symbol, seq, Side::Buy, 0, 0, 0);
                    status.flags = match rng.below(4) {
                        0 => MD_FLAG_HALTED,
                        1 => SessionState::PreOpen.to_md_flags(),
                        2 => SessionState::Closed.to_md_flags() | MD_FLAG_HALTED,
                        _ => SessionState::Open.to_md_flags(),
                    };
                    status
                };
                group.push(event);
            }

            for event in &group {
                hub.apply(event);
                // After every apply: the book must be self-consistent.
                let state = hub.state.lock().expect("md state");
                for book in state.books.values() {
                    book.check_invariants()
                        .unwrap_or_else(|error| panic!("step {step}: {error}"));
                }
            }
            if !group.is_empty() {
                history.push(group.clone());
            }

            // Occasionally publish a fresh image, half of them level-only.
            if rng.below(100) < 7 {
                let symbol = symbols[rng.below(2) as usize];
                let seq = {
                    let next = seqs.get_mut(&symbol).expect("configured symbol");
                    *next += 1;
                    *next
                };
                let level_only = rng.below(100) < 30;
                hub.apply_snapshot(&model_snapshot(&model, symbol, seq, level_only));
                accepted_snapshots += 1;
                if level_only {
                    level_only_snapshots += 1;
                }
            }

            // Occasionally replay an earlier batch verbatim: every event in it
            // is already covered, so nothing may be applied.
            if step % 97 == 96 && !history.is_empty() {
                let index = rng.below(history.len() as u64) as usize;
                let before = applied_deltas(&hub);
                hub.apply_batch(&history[index]);
                assert_eq!(
                    applied_deltas(&hub),
                    before,
                    "step {step}: a replayed batch must be entirely stale"
                );
                stale_replays += 1;
            }

            {
                let state = hub.state.lock().expect("md state");
                let non_empty = state
                    .books
                    .values()
                    .filter(|book| !book.bids.is_empty() || !book.asks.is_empty())
                    .count();
                both_books_non_empty |= non_empty == 2;
                assert!(
                    state.trades.len() <= TRADE_TAPE_LEN,
                    "step {step}: the tape outgrew its bound"
                );
            }
            if step % 100 == 99 {
                hub.check_invariants()
                    .unwrap_or_else(|error| panic!("step {step}: {error}"));
            }
        }

        hub.check_invariants()
            .expect("hub invariants hold at the end");
        // Coverage pins, so a generator change that stops exercising a path is
        // a test failure instead of a silent hole.
        assert!(both_books_non_empty, "both books held depth at some point");
        assert!(trades > 0, "the stream generated trades");
        assert!(statuses > 0, "the stream generated status events");
        assert!(
            accepted_snapshots >= 1,
            "the stream generated snapshots: {accepted_snapshots}"
        );
        assert!(
            level_only_snapshots >= 1,
            "the stream generated level-only snapshots"
        );
        assert!(stale_replays >= 1, "the stream replayed a previous batch");
        assert!(
            hub.events_without_baseline() >= 2,
            "the prelude exercised the no-baseline refusal"
        );
        // No subscriber was ever attached, so the feed encoded nothing.
        assert_eq!(hub.feed_stats(), (0, 0, 0));
    }
}

#[cfg(test)]
mod shard_metrics_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn global() -> Metrics {
        Metrics::new(
            ExchangeConfig::default().mode,
            ExchangeConfig::default().partitions,
        )
    }

    fn record_batch(shard: &ShardMetrics, count: u64) {
        for sample in 0..count {
            shard.record_match_latency_us(50 + sample * 7);
            shard.record_engine_command();
            shard.record_execution();
            shard.record_order_outcome(sample % 4 == 0);
        }
    }

    #[test]
    fn merge_adds_counters_exactly_once() {
        let global = global();
        let shard = ShardMetrics::new();
        record_batch(&shard, 100);

        shard.merge_into(&global);
        shard.merge_into(&global);

        let view = global.snapshot();
        assert_eq!(view.engine_commands, 100);
        assert_eq!(view.executions, 100);
        assert_eq!(view.orders_matched, 75);
        assert_eq!(view.orders_rejected, 25);
        // The second merge was empty: the shard was drained by the first.
        assert_eq!(global.match_latency_sample_count(), 100);
    }

    #[test]
    fn merged_percentiles_match_direct_recording() {
        // A spread wide enough to cross several HDR buckets at 3 significant
        // digits, plus heavy tail weight so p99 is not the max.
        let samples: Vec<u64> = (0..2_000)
            .map(|index| match index % 10 {
                0..=4 => 100 + index as u64,
                5..=8 => 1_000 + index as u64,
                _ => 25_000 + index as u64,
            })
            .collect();

        let direct = global();
        for sample in &samples {
            direct.record_match_latency_us(*sample);
        }

        let merged = global();
        let shard = ShardMetrics::new();
        for sample in &samples {
            shard.record_match_latency_us(*sample);
        }
        shard.merge_into(&merged);

        let direct_view = direct.snapshot();
        let merged_view = merged.snapshot();
        assert_eq!(merged_view.match_p50_us, direct_view.match_p50_us);
        assert_eq!(merged_view.match_p99_us, direct_view.match_p99_us);
        assert!(merged_view.match_p50_us > 0.0);
        assert!(merged_view.match_p99_us >= merged_view.match_p50_us);
        // p99 sits in the heavy tail of the sample set (HDR may report the
        // bucket value, not the exact sample, so only bound it).
        assert!(merged_view.match_p99_us >= 25_000.0);
        assert!(merged_view.match_p99_us <= 27_000.0);
    }

    #[test]
    fn concurrent_merges_do_not_lose_counts() {
        const SHARDS: u64 = 4;
        const RECORDS_PER_SHARD: u64 = 5_000;
        const MERGE_EVERY: u64 = 10;

        let global = Arc::new(global());
        thread::scope(|scope| {
            for _ in 0..SHARDS {
                let global = Arc::clone(&global);
                scope.spawn(move || {
                    let shard = ShardMetrics::new();
                    for sample in 0..RECORDS_PER_SHARD {
                        shard.record_match_latency_us(20 + sample * 3);
                        shard.record_engine_command();
                        shard.record_execution();
                        shard.record_order_outcome(sample % 3 == 0);
                        if sample % MERGE_EVERY == MERGE_EVERY - 1 {
                            shard.merge_into(&global);
                        }
                    }
                    shard.merge_into(&global);
                });
            }
        });

        let view = global.snapshot();
        assert_eq!(view.engine_commands, SHARDS * RECORDS_PER_SHARD);
        assert_eq!(view.executions, SHARDS * RECORDS_PER_SHARD);
        // samples 0, 3, ..., 4998 are rejected per shard: ceil(count / 3).
        let rejected = SHARDS * RECORDS_PER_SHARD.div_ceil(3);
        assert_eq!(view.orders_rejected, rejected);
        assert_eq!(view.orders_matched, SHARDS * RECORDS_PER_SHARD - rejected);
        // Every latency sample survived its shard's merge loop.
        assert_eq!(
            global.match_latency_sample_count(),
            SHARDS * RECORDS_PER_SHARD
        );
    }

    #[test]
    fn empty_merge_is_a_no_op() {
        let global = global();
        let before = global.snapshot();

        let shard = ShardMetrics::new();
        shard.merge_into(&global);
        shard.merge_into(&global);

        let after = global.snapshot();
        assert_eq!(after.engine_commands, before.engine_commands);
        assert_eq!(after.executions, before.executions);
        assert_eq!(after.orders_matched, before.orders_matched);
        assert_eq!(after.orders_rejected, before.orders_rejected);
        assert_eq!(after.match_p50_us, 0.0);
        assert_eq!(after.match_p99_us, 0.0);
    }

    #[test]
    fn records_between_merges_are_not_lost() {
        let global = global();
        let shard = ShardMetrics::new();

        record_batch(&shard, 10);
        shard.merge_into(&global);
        // Records that land after one merge and before the next.
        record_batch(&shard, 5);
        shard.merge_into(&global);

        let view = global.snapshot();
        assert_eq!(view.engine_commands, 15);
        assert_eq!(global.match_latency_sample_count(), 15);
    }

    fn stage_summary(metrics: &Metrics, stage: Stage) -> LatencySummary {
        metrics
            .stage_snapshot()
            .into_iter()
            .find(|(recorded, _)| *recorded == stage)
            .map(|(_, summary)| summary)
            .expect("every stage is in the snapshot")
    }

    #[test]
    fn merged_stage_percentiles_match_direct_recording() {
        // A spread wide enough to cross several HDR buckets at 3 significant
        // digits, with a heavy tail so p99 is not the max.
        let samples: Vec<u64> = (0..2_000)
            .map(|index| match index % 10 {
                0..=4 => 150 + index as u64,
                5..=8 => 1_500 + index as u64,
                _ => 40_000 + index as u64,
            })
            .collect();

        let direct = global();
        for sample in &samples {
            direct.record_stage_us(Stage::EngineProcess, *sample);
        }

        let merged = global();
        let shard = ShardMetrics::new();
        for sample in &samples {
            shard.record_stage_us(Stage::EngineProcess, *sample);
        }
        shard.merge_into(&merged);

        let direct_summary = stage_summary(&direct, Stage::EngineProcess);
        let merged_summary = stage_summary(&merged, Stage::EngineProcess);
        assert_eq!(merged_summary.samples, direct_summary.samples);
        assert_eq!(merged_summary.p50_us, direct_summary.p50_us);
        assert_eq!(merged_summary.p99_us, direct_summary.p99_us);
        assert_eq!(merged_summary.p999_us, direct_summary.p999_us);
        assert!(merged_summary.p50_us > 0.0);
        assert!(merged_summary.p50_us <= merged_summary.p99_us);
        assert!(merged_summary.p99_us >= 40_000.0);
        assert!(merged_summary.p99_us <= 42_000.0);
    }

    #[test]
    fn stage_merge_adds_exactly_once() {
        let global = global();
        let shard = ShardMetrics::new();

        for sample in 1..=50u64 {
            shard.record_stage_us(Stage::EngineProcess, 100 + sample);
        }
        shard.merge_into(&global);
        let after_first = stage_summary(&global, Stage::EngineProcess);
        assert_eq!(after_first.samples, 50);

        // A second merge without new samples must add nothing: the first
        // merge drained the shard-local histogram.
        shard.merge_into(&global);
        let after_second = stage_summary(&global, Stage::EngineProcess);
        assert_eq!(after_second.samples, 50);
        assert_eq!(after_second.p50_us, after_first.p50_us);
        assert_eq!(after_second.p99_us, after_first.p99_us);

        // Samples that land after a merge still reach the globals on the
        // next one.
        for sample in 1..=10u64 {
            shard.record_stage_us(Stage::EngineProcess, 200 + sample);
        }
        shard.merge_into(&global);
        assert_eq!(stage_summary(&global, Stage::EngineProcess).samples, 60);
        assert_eq!(global.stage_sample_count(Stage::EngineProcess), 60);
    }

    #[test]
    fn stage_samples_are_isolated_per_stage() {
        let global = global();
        let shard = ShardMetrics::new();

        shard.record_stage_us(Stage::EngineProcess, 333);
        shard.merge_into(&global);

        let process = stage_summary(&global, Stage::EngineProcess);
        assert_eq!(process.samples, 1);
        assert_eq!(process.p50_us, 333.0);
        assert_eq!(process.p99_us, 333.0);

        // The sample went only to the stage it named. Every other stage,
        // including the other engine stages the writer records, stays empty.
        for stage in Stage::ALL {
            if stage == Stage::EngineProcess {
                continue;
            }
            let summary = stage_summary(&global, stage);
            assert_eq!(summary.samples, 0, "{:?} received foreign samples", stage);
            assert_eq!(summary.p50_us, 0.0, "{:?}", stage);
        }
    }

    #[test]
    fn empty_shard_stage_merge_is_a_no_op() {
        let global = global();
        let before = global.stage_snapshot();

        let shard = ShardMetrics::new();
        shard.merge_into(&global);
        shard.merge_into(&global);

        let after = global.stage_snapshot();
        for ((before_stage, before_summary), (after_stage, after_summary)) in
            before.iter().zip(&after)
        {
            assert_eq!(before_stage, after_stage);
            assert_eq!(before_summary.samples, after_summary.samples);
            assert_eq!(before_summary.p50_us, after_summary.p50_us);
            assert_eq!(before_summary.p99_us, after_summary.p99_us);
        }
    }

    #[test]
    fn every_stage_survives_the_round_trip() {
        let global = global();
        let shard = ShardMetrics::new();

        for stage in Stage::ALL {
            let base = 1_000 + stage.index() as u64;
            let tail = 2_000 + stage.index() as u64;
            shard.record_stage_us(stage, base);
            shard.record_stage_us(stage, tail);
        }
        shard.merge_into(&global);

        for stage in Stage::ALL {
            let base = 1_000 + stage.index() as u64;
            let tail = 2_000 + stage.index() as u64;
            let summary = stage_summary(&global, stage);
            assert_eq!(summary.samples, 2, "{:?} lost samples on merge", stage);
            assert_eq!(summary.p50_us, base as f64, "{:?}", stage);
            assert_eq!(summary.p99_us, tail as f64, "{:?}", stage);
        }
    }

    #[test]
    fn stage_records_clamp_like_the_global_path() {
        let global = global();
        let shard = ShardMetrics::new();

        shard.record_stage_us(Stage::EngineProcess, u64::MAX);
        shard.record_stage_us(Stage::EngineProcess, 0);
        shard.merge_into(&global);

        let direct_metrics = Metrics::new(
            ExchangeConfig::default().mode,
            ExchangeConfig::default().partitions,
        );
        direct_metrics.record_stage_us(Stage::EngineProcess, u64::MAX);
        direct_metrics.record_stage_us(Stage::EngineProcess, 0);

        let merged = stage_summary(&global, Stage::EngineProcess);
        let direct_summary = stage_summary(&direct_metrics, Stage::EngineProcess);
        assert_eq!(merged.samples, 2);
        assert_eq!(merged.p50_us, direct_summary.p50_us);
        assert_eq!(merged.p99_us, direct_summary.p99_us);
    }
}

#[cfg(test)]
mod latency_window_tests {
    use super::*;

    fn metrics() -> Metrics {
        Metrics::new(
            ExchangeConfig::default().mode,
            ExchangeConfig::default().partitions,
        )
    }

    #[test]
    fn window_tracks_samples_and_rotation_keeps_the_lifetime() {
        let metrics = metrics();
        for _ in 0..50 {
            metrics.record_ingress_latency_us(100);
        }
        for _ in 0..50 {
            metrics.record_match_latency_us(200);
        }

        // The lifetime view and the window agree while the window is fresh.
        let view = metrics.snapshot();
        assert!(view.ingress_p50_us > 0.0);
        assert!(view.match_p99_us > 0.0);
        let window = metrics.window_summary();
        assert_eq!(window.secs, LATENCY_WINDOW_SECS);
        assert!(window.ingress_p50_us > 0.0);
        assert!(window.match_p99_us > 0.0);

        // Forced rotation empties the window but never the lifetime data.
        metrics.rotate_window_now();
        let rotated = metrics.window_summary();
        assert_eq!(rotated.secs, LATENCY_WINDOW_SECS);
        assert_eq!(rotated.ingress_p50_us, 0.0);
        assert_eq!(rotated.ingress_p99_us, 0.0);
        assert_eq!(rotated.match_p50_us, 0.0);
        assert_eq!(rotated.match_p99_us, 0.0);
        let lifetime = metrics.snapshot();
        assert_eq!(lifetime.ingress_p99_us, view.ingress_p99_us);
        assert_eq!(lifetime.match_p99_us, view.match_p99_us);

        // The next sample opens a fresh window without touching the lifetime.
        metrics.record_ingress_latency_us(300);
        let reopened = metrics.window_summary();
        assert!(reopened.ingress_p99_us > 0.0);
        assert_eq!(reopened.match_p99_us, 0.0);
    }

    #[test]
    fn window_p99_reflects_a_recent_slow_sample_before_the_lifetime_moves() {
        let metrics = metrics();
        for _ in 0..20 {
            metrics.record_ingress_latency_us(100);
        }
        let before_window = metrics.window_summary();
        assert_eq!(before_window.ingress_p99_us, 100.0);

        // One slow sample: the window tail jumps immediately.
        metrics.record_ingress_latency_us(50_000);
        let after_window = metrics.window_summary();
        assert!(
            after_window.ingress_p99_us >= 49_000.0,
            "window p99 = {}",
            after_window.ingress_p99_us
        );
        assert_ne!(
            before_window.ingress_p99_us, after_window.ingress_p99_us,
            "the window must react to the recent slow sample"
        );

        // 21 samples with one outlier: the lifetime median is untouched.
        let lifetime = metrics.snapshot();
        assert_eq!(lifetime.ingress_p50_us, 100.0);
    }

    #[test]
    fn zero_length_window_reports_empty_until_the_next_sample_opens_one() {
        // A window of 0 rotates on every sample, so the read path reports it
        // as expired: an honest "no current latency" without sleeping.
        let metrics = Metrics::with_window_secs(
            ExchangeConfig::default().mode,
            ExchangeConfig::default().partitions,
            0,
        );
        metrics.record_ingress_latency_us(100);
        metrics.record_match_latency_us(200);
        let window = metrics.window_summary();
        assert_eq!(window.secs, 0);
        assert_eq!(window.ingress_p99_us, 0.0);
        assert_eq!(window.match_p99_us, 0.0);
        // The lifetime percentiles still see every sample.
        let view = metrics.snapshot();
        assert!(view.ingress_p50_us > 0.0);
        assert!(view.match_p50_us > 0.0);
    }

    #[test]
    fn shard_merges_feed_the_windowed_match_percentiles() {
        // The hot path records match latency in ShardMetrics, so a merge must
        // be the path that fills the window, not only the lifetime histogram.
        let global = metrics();
        let shard = ShardMetrics::new();
        for sample in 0..100u64 {
            shard.record_match_latency_us(150 + sample);
        }
        shard.merge_into(&global);

        let window = global.window_summary();
        assert!(
            window.match_p50_us > 0.0,
            "window match p50 = 0 after merge"
        );
        assert!(window.match_p99_us > 0.0);
        // The merged samples did not touch the windowed ingress histogram.
        assert_eq!(window.ingress_p99_us, 0.0);
    }
}

#[cfg(test)]
mod shard_lag_and_ring_tests {
    use super::*;

    fn metrics() -> Metrics {
        Metrics::new(
            ExchangeConfig::default().mode,
            ExchangeConfig::default().partitions,
        )
    }

    #[test]
    fn per_shard_lag_lists_only_nonzero_shards() {
        let metrics = metrics();
        assert!(metrics.consumer_lag_by_shard().is_empty());

        metrics.set_consumer_lag_for_shard(3, 42);
        metrics.set_consumer_lag_for_shard(7, 9);
        assert_eq!(metrics.consumer_lag_by_shard(), vec![(3, 42), (7, 9)]);

        // The aggregate is independent: the node sets both, one at a time.
        assert_eq!(metrics.snapshot().consumer_lag, 0);
        metrics.set_consumer_lag(51);
        assert_eq!(metrics.snapshot().consumer_lag, 51);
        assert_eq!(metrics.consumer_lag_by_shard(), vec![(3, 42), (7, 9)]);

        // Resetting a shard to zero removes it from the list.
        metrics.set_consumer_lag_for_shard(3, 0);
        assert_eq!(metrics.consumer_lag_by_shard(), vec![(7, 9)]);
    }

    #[test]
    fn lag_beyond_the_slot_count_folds_into_the_last_slot() {
        let metrics = metrics();
        metrics.set_consumer_lag_for_shard(u32::try_from(METRIC_SHARD_SLOTS).unwrap() + 44, 5);
        assert_eq!(
            metrics.consumer_lag_by_shard(),
            vec![(METRIC_SHARD_SLOTS as u32 - 1, 5)]
        );
    }

    #[test]
    fn ring_depths_round_trip_and_omit_zero_shards() {
        let metrics = metrics();
        assert!(metrics.ring_depths().is_empty());

        metrics.set_ring_depths(2, 1, 0, 2);
        metrics.set_ring_depths(5, 2, 1, 2);
        assert_eq!(metrics.ring_depths(), vec![(2, 1, 0, 2), (5, 2, 1, 2)]);

        // A report with empty rings keeps the shard listed by its capacity.
        metrics.set_ring_depths(2, 0, 0, 2);
        assert_eq!(metrics.ring_depths(), vec![(2, 0, 0, 2), (5, 2, 1, 2)]);
        // And a capacity of zero (unreported shard) drops off the list.
        metrics.set_ring_depths(5, 0, 0, 0);
        assert_eq!(metrics.ring_depths(), vec![(2, 0, 0, 2)]);
    }
}
