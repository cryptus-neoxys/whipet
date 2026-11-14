// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Market-data state machine and conflated fanout.
//!
//! The hub consumes the canonical MBO deltas the engine publishes to Iggy and
//! maintains a per-symbol book. The browser never sees raw deltas in Phase 1:
//! it receives a conflated snapshot at 20 Hz, which is the correct rendering
//! discipline for a ladder. Trades are retained losslessly in the visible tape
//! window; audit consumes the log, not the UI.

#![forbid(unsafe_code)]

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
                let level = self
                    .side_levels_mut(event.side)
                    .entry(price_cents)
                    .or_default();
                level.qty = level.qty.saturating_add(event.qty.0);
                level.orders = level.orders.saturating_add(1);
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
                        let level = self.side_levels_mut(order.side).get_mut(&order.price_cents);
                        if let Some(level) = level {
                            level.qty = level.qty.saturating_sub(delta);
                        }
                    }
                    if let Some(order) = self.orders.get_mut(&event.order_id) {
                        order.qty = new_qty;
                    }
                }
            }
            MdKind::Remove => {
                if let Some(order) = self.orders.remove(&event.order_id) {
                    let remove_level = {
                        let levels = self.side_levels_mut(order.side);
                        if let Some(level) = levels.get_mut(&order.price_cents) {
                            level.qty = level.qty.saturating_sub(order.qty);
                            level.orders = level.orders.saturating_sub(1);
                            level.orders == 0
                        } else {
                            false
                        }
                    };
                    if remove_level {
                        self.side_levels_mut(order.side).remove(&order.price_cents);
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
            depth_bid_qty: self
                .bids
                .values()
                .fold(0u64, |total, level| total.saturating_add(level.qty)),
            depth_ask_qty: self
                .asks
                .values()
                .fold(0u64, |total, level| total.saturating_add(level.qty)),
        }
    }
}

struct HubState {
    books: BTreeMap<Symbol, BookState>,
    trades: VecDeque<TradeView>,
    last_update_ns: u64,
    sequence: u64,
    md_events: u64,
    trades_count: u64,
    dirty: bool,
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
        self.md_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_trade(&self) {
        self.trades.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_engine_command(&self) {
        self.engine_commands.fetch_add(1, Ordering::Relaxed);
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
        let initial = Arc::new(StateView {
            mode: config.mode,
            venue: config.venue.clone(),
            symbols: Vec::new(),
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

    pub fn apply(&self, event: &MdEvent) {
        let Some(instrument) = self.config.instrument(event.symbol) else {
            return;
        };
        let mut state = self.state.lock().expect("md state poisoned");
        let (applied, price_cents) = {
            let Some(book) = state.books.get_mut(&event.symbol) else {
                return;
            };
            debug_assert_eq!(book.tick_size_cents, instrument.tick_size_cents);
            let applied = book.apply(event);
            (applied, book.price_cents(event.price))
        };
        if !applied {
            return;
        }
        self.metrics.record_md_event();
        if event.kind == MdKind::Trade {
            self.metrics.record_trade();
        }
        state.last_update_ns = now_ns();
        state.sequence = state.sequence.max(event.seq.0);
        state.md_events = state.md_events.saturating_add(1);
        if event.kind == MdKind::Trade {
            state.trades_count = state.trades_count.saturating_add(1);
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
        state.dirty = true;
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

    /// Build a fresh state view and publish it to WebSocket subscribers.
    pub fn publish(&self) {
        let state = self.build_state();
        let _ = self.watch.send(Arc::new(state));
    }

    pub fn state(&self) -> StateView {
        self.build_state()
    }

    fn build_state(&self) -> StateView {
        let mut state = self.state.lock().expect("md state poisoned");
        state.dirty = false;
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
        StateView {
            mode: self.config.mode,
            venue: self.config.venue.clone(),
            symbols,
            trades: state.trades.iter().rev().cloned().collect(),
            metrics: self.metrics.snapshot(),
            freshness_ms,
            sequence: state.sequence,
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
            let mut interval = tokio::time::interval(Duration::from_millis(50));
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
    use whippet_core::protocol::{MdEvent, MdKind, MdSnapshot, Side};

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
}
