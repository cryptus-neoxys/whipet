// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Market-data state machine and conflated fanout.
//!
//! The hub consumes the canonical MBO deltas the engine publishes to Iggy and
//! maintains a per-symbol book. The browser never sees raw deltas in Phase 1:
//! it receives a conflated snapshot at 20 Hz, which is the correct rendering
//! discipline for a ladder. Trades are retained losslessly in the visible tape
//! window; audit consumes the log, not the UI.

#![forbid(unsafe_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::sync::watch;
use whippet_core::api::{BookView, LevelView, MetricsView, StateView, TradeView};
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{OrderId, Price, Seq, Symbol};
use whippet_core::protocol::{MdEvent, MdKind, MdSnapshot, Side};

const TRADE_TAPE_LEN: usize = 250;
const BOOK_DEPTH: usize = 20;

/// Publisher cadence. `spawn_publisher` ticks at this interval.
const PUBLISH_INTERVAL: Duration = Duration::from_millis(50);

/// Coalescing window for redundant `publish()` calls. It sits just below the
/// publisher cadence so scheduler jitter or the time spent building a frame
/// never suppresses a scheduled 20 Hz tick. A dirty hub always publishes
/// immediately; only back-to-back publishes with no new events are skipped.
const PUBLISH_COALESCE_WINDOW: Duration = Duration::from_millis(45);

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
    seq: Seq,
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
            seq: Seq(0),
            depth_bid_qty: 0,
            depth_ask_qty: 0,
        }
    }

    fn price_cents(&self, price: Price) -> i64 {
        price.0 * self.tick_size_cents
    }

    fn side_levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, LevelState> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
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

    fn apply(&mut self, event: &MdEvent) -> bool {
        // At-least-once delivery: a crash between apply and commit replays the
        // batch. The per-symbol sequence makes deltas idempotent.
        if self.seq.0 != 0 && event.seq.0 <= self.seq.0 {
            return false;
        }
        self.seq = event.seq;
        match event.kind {
            MdKind::Add => {
                let price_cents = self.price_cents(event.price);
                let added = {
                    let level = self
                        .side_levels_mut(event.side)
                        .entry(price_cents)
                        .or_default();
                    let before = level.qty;
                    level.qty = level.qty.saturating_add(event.qty.0);
                    level.orders = level.orders.saturating_add(1);
                    // Saturating arithmetic can absorb part of the add; track
                    // the change the map actually took, not the requested qty.
                    level.qty - before
                };
                let depth = self.depth_mut(event.side);
                *depth = depth.saturating_add(added);
                self.orders.insert(
                    event.order_id,
                    OrderRef {
                        side: event.side,
                        price_cents,
                        qty: event.qty.0,
                    },
                );
            }
            MdKind::Reduce => {
                if let Some(order) = self.orders.get(&event.order_id).copied() {
                    let new_qty = event.qty.0;
                    if new_qty > order.qty {
                        return false;
                    }
                    let delta = order.qty.saturating_sub(new_qty);
                    if delta > 0 {
                        let removed =
                            match self.side_levels_mut(order.side).get_mut(&order.price_cents) {
                                Some(level) => {
                                    let before = level.qty;
                                    level.qty = level.qty.saturating_sub(delta);
                                    before - level.qty
                                }
                                None => 0,
                            };
                        if removed > 0 {
                            let depth = self.depth_mut(order.side);
                            *depth = depth.saturating_sub(removed);
                        }
                    }
                    if let Some(order) = self.orders.get_mut(&event.order_id) {
                        order.qty = new_qty;
                    }
                }
            }
            MdKind::Remove => {
                if let Some(order) = self.orders.remove(&event.order_id) {
                    let (removed, remove_level, leftover) = {
                        let levels = self.side_levels_mut(order.side);
                        if let Some(level) = levels.get_mut(&order.price_cents) {
                            let before = level.qty;
                            level.qty = level.qty.saturating_sub(order.qty);
                            level.orders = level.orders.saturating_sub(1);
                            (before - level.qty, level.orders == 0, level.qty)
                        } else {
                            (0, false, 0)
                        }
                    };
                    if remove_level {
                        self.side_levels_mut(order.side).remove(&order.price_cents);
                    }
                    // Removing the level also drops any quantity left behind by
                    // an inconsistent order set, so subtract that too.
                    let removed = if remove_level {
                        removed.saturating_add(leftover)
                    } else {
                        removed
                    };
                    if removed > 0 {
                        let depth = self.depth_mut(order.side);
                        *depth = depth.saturating_sub(removed);
                    }
                }
            }
            MdKind::Trade => {
                self.last_trade_price_cents = Some(self.price_cents(event.price));
                self.last_trade_qty = event.qty.0;
                self.last_trade_side = Some(event.side);
            }
            MdKind::Bbo | MdKind::Status | MdKind::Snapshot => {}
        }
        true
    }

    fn apply_snapshot(&mut self, snapshot: &MdSnapshot) -> bool {
        // A retried append can deliver an older snapshot after newer deltas.
        // Never regress a symbol that already has state.
        if self.seq.0 != 0 && snapshot.seq.0 <= self.seq.0 {
            return false;
        }
        self.seq = snapshot.seq;
        self.bids.clear();
        self.asks.clear();
        self.orders.clear();
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
        }
    }
}

fn total_depth(levels: &BTreeMap<i64, LevelState>) -> u64 {
    levels
        .values()
        .fold(0u64, |total, level| total.saturating_add(level.qty))
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

/// Shared metrics. Counters are atomics; percentiles come from HDR histograms
/// at the edge, never from the match loop.
pub struct Metrics {
    orders_ingressed: AtomicU64,
    orders_matched: AtomicU64,
    orders_rejected: AtomicU64,
    executions: AtomicU64,
    trades: AtomicU64,
    md_events: AtomicU64,
    engine_commands: AtomicU64,
    consumer_lag: AtomicU64,
    iggy_connected: AtomicBool,
    partition_count: AtomicU64,
    ingress: Mutex<Histogram<u64>>,
    match_latency: Mutex<Histogram<u64>>,
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
        Self {
            orders_ingressed: AtomicU64::new(0),
            orders_matched: AtomicU64::new(0),
            orders_rejected: AtomicU64::new(0),
            executions: AtomicU64::new(0),
            trades: AtomicU64::new(0),
            md_events: AtomicU64::new(0),
            engine_commands: AtomicU64::new(0),
            consumer_lag: AtomicU64::new(0),
            iggy_connected: AtomicBool::new(false),
            partition_count: AtomicU64::new(u64::from(partitions)),
            ingress: Mutex::new(new_histogram()),
            match_latency: Mutex::new(new_histogram()),
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

    pub fn set_consumer_lag(&self, lag: u64) {
        self.consumer_lag.store(lag, Ordering::Relaxed);
    }

    pub fn record_ingress(&self) {
        self.orders_ingressed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_ingress_latency_us(&self, micros: u64) {
        record(&self.ingress, micros);
    }

    pub fn record_match_latency_us(&self, micros: u64) {
        record(&self.match_latency, micros);
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

    /// Drain a shard's match-latency histogram into the global one and clear
    /// the source. Both histograms are built by [`new_histogram`], so
    /// [`Histogram::add`] cannot fail; the value-by-value replay is a
    /// lossless fallback for a config mismatch that cannot happen today.
    pub(crate) fn merge_match_latency(&self, source: &mut Histogram<u64>) {
        if source.is_empty() {
            return;
        }
        // A poisoned lock leaves `source` unmerged and uncleaned, so the next
        // merge retries instead of dropping the samples.
        let Ok(mut histogram) = self.match_latency.lock() else {
            return;
        };
        if histogram.add(&*source).is_err() {
            for sample in source.iter_recorded() {
                let value = sample.value_iterated_to();
                for _ in 0..sample.count_at_value() {
                    let _ = histogram.record(value);
                }
            }
        }
        source.clear();
    }

    #[cfg(test)]
    fn match_latency_sample_count(&self) -> u64 {
        self.match_latency
            .lock()
            .map(|histogram| histogram.len())
            .unwrap_or(0)
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
            ingress_p50_us: percentile(&self.ingress, 50.0),
            ingress_p99_us: percentile(&self.ingress, 99.0),
            match_p50_us: percentile(&self.match_latency, 50.0),
            match_p99_us: percentile(&self.match_latency, 99.0),
            throughput_orders_s: rate,
            uptime_s: uptime,
            iggy_connected: self.iggy_connected.load(Ordering::Relaxed),
            partition_count: self.partition_count.load(Ordering::Relaxed) as u32,
        }
    }
}

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_max(60_000_000, 3).expect("valid histogram bounds")
}

fn record(histogram: &Mutex<Histogram<u64>>, value: u64) {
    if let Ok(mut histogram) = histogram.lock() {
        let _ = histogram.record(value.min(59_999_999));
    }
}

fn percentile(histogram: &Mutex<Histogram<u64>>, quantile: f64) -> f64 {
    histogram
        .lock()
        .map(|histogram| {
            if histogram.is_empty() {
                0.0
            } else {
                histogram.value_at_quantile(quantile / 100.0) as f64
            }
        })
        .unwrap_or(0.0)
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

fn micros_since(start: Instant) -> u64 {
    start.elapsed().as_micros() as u64
}

/// Per-shard metrics recorder, owned by one match-loop task.
///
/// The shard thread owns this value, so recording never takes a lock: the
/// counters are plain [`Cell`]s and latency samples go into a shard-local HDR
/// histogram behind a [`RefCell`], both uncontended single-thread writes on
/// the same stack the engine already touches.
///
/// `merge_into` is the only cross-thread step, and the shard loop calls it
/// periodically (e.g. once per batch or on the 20 Hz cadence): counters are
/// swapped to zero and fetched onto the global atomics, and the local
/// histogram is drained into the global one under the existing histogram
/// mutex. Because counters swap to zero and the histogram clears on drain, a
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
        }
    }

    /// Same clamping as [`Metrics::record_match_latency_us`] so a merged
    /// sample and a directly recorded sample land in identical buckets.
    pub fn record_match_latency_us(&self, micros: u64) {
        let mut histogram = self.match_latency.borrow_mut();
        let _ = histogram.record(micros.min(59_999_999));
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
    /// the latency histogram is drained and cleared, so a merge without new
    /// records is a no-op. Call this only from the thread that records: the
    /// counters are not atomic.
    pub fn merge_into(&self, global: &Metrics) {
        global.add_match_counters(
            self.engine_commands.replace(0),
            self.executions.replace(0),
            self.orders_matched.replace(0),
            self.orders_rejected.replace(0),
        );
        global.merge_match_latency(&mut self.match_latency.borrow_mut());
    }
}

/// Market-data hub. Cloneable handle shared by the consumer, HTTP API, and
/// WebSocket tasks.
pub struct MdHub {
    config: ExchangeConfig,
    state: Mutex<HubState>,
    watch: watch::Sender<Arc<StateView>>,
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
        // One clock for the whole batch. The stage records the slice the hub
        // spends under its own lock, including duplicate/stale rejection.
        let started = Instant::now();
        let mut state = self.state.lock().expect("md state poisoned");
        let mut applied = 0u64;
        let mut trades_applied = 0u64;
        let mut max_seq = 0u64;
        for event in events {
            // Unknown symbols are skipped exactly as `apply` skips them.
            let Some(instrument) = self.config.instrument(event.symbol) else {
                continue;
            };
            let Some(book) = state.books.get_mut(&event.symbol) else {
                continue;
            };
            debug_assert_eq!(book.tick_size_cents, instrument.tick_size_cents);
            if !book.apply(event) {
                continue;
            }
            // Read the price scale before touching `state.trades` so the book
            // borrow ends here (the two fields are disjoint, but NLL needs the
            // book borrow to be finished).
            let price_cents = book.price_cents(event.price);
            applied += 1;
            max_seq = max_seq.max(event.seq.0);
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
        // Drop the hub lock before touching the histogram mutex.
        drop(state);
        self.metrics
            .record_stage_us(Stage::MdApply, micros_since(started));
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

    #[test]
    fn duplicate_trade_is_not_added_to_the_tape_or_metrics_twice() {
        let hub = MdHub::new(config(), "test".into(), "memory".into(), None);
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
        assert_eq!(state.metrics.md_events, 2);
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
        let before = hub.latest();

        hub.apply(&event(MdKind::Trade, Side::Buy, 65_000, 4, 1));
        let metrics = hub.metrics_snapshot();
        assert_eq!(metrics.md_events, 1);
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
        assert_eq!(after.metrics.md_events, 2);
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
            // Over-reduce: rejected after the book advances its sequence.
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
        for event in &stream {
            single.apply(event);
        }

        // Batch boundaries must not change the result, including when a chunk
        // splits a duplicate from the event it replays.
        let chunked = MdHub::new(config(), "test".into(), "memory".into(), None);
        for chunk in stream.chunks(3) {
            chunked.apply_batch(chunk);
        }

        let once = MdHub::new(config(), "test".into(), "memory".into(), None);
        once.apply_batch(&stream);

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
}
