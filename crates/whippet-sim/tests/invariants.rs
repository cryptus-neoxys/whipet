// Structural invariants for the simulator: open-book modelling, price bands,
// counters, and `check_invariants` itself.

mod common;

use std::collections::{HashMap, HashSet, VecDeque};

use common::{SCENARIOS, config, simulator};
use whippet_core::api::{CancelRequest, OrderRequest};
use whippet_core::config::{ExchangeConfig, InstrumentConfig, Mode};
use whippet_core::ids::{OrderId, Symbol};
use whippet_core::protocol::OrderType;
use whippet_sim::{Scenario, SimAction, SimConfig, Simulator};

const OPEN_CAPACITY: usize = 4_096;

/// Test-side model of `Simulator`'s open book. It is deliberately strict: a
/// cancel must name an id this model still believes is open, and a cancel must
/// retire that id forever.
#[derive(Default)]
struct OpenBook {
    fifo: VecDeque<OrderId>,
    meta: HashMap<OrderId, (Symbol, String)>,
    retired: HashSet<OrderId>,
    evicted: u32,
}

impl OpenBook {
    fn on_new(&mut self, order: &OrderRequest) {
        match order.order_type {
            OrderType::Limit | OrderType::PostOnly => {}
            OrderType::Market | OrderType::Ioc | OrderType::Fok => return,
        }
        let id = OrderId::derive(
            order.symbol,
            order
                .client_order_id
                .as_deref()
                .expect("sim ids every order"),
        );
        let participant = order.participant.clone().unwrap_or_default();
        if self.fifo.len() >= OPEN_CAPACITY {
            // Documented behaviour: the oldest open order is silently dropped so
            // the book stays bounded. No cancel is emitted for it.
            let evicted = self.fifo.pop_front().expect("checked non-empty");
            self.meta.remove(&evicted);
            assert!(
                self.retired.insert(evicted),
                "an evicted order id was evicted twice"
            );
            self.evicted += 1;
        }
        assert!(
            !self.meta.contains_key(&id),
            "open order id {} was already open",
            id.to_hex()
        );
        assert!(
            !self.retired.contains(&id),
            "open order id {} was already retired",
            id.to_hex()
        );
        self.meta.insert(id, (order.symbol, participant));
        self.fifo.push_back(id);
        assert!(self.fifo.len() <= OPEN_CAPACITY);
    }

    fn on_cancel(&mut self, cancel: &CancelRequest) {
        let (symbol, participant) = self
            .meta
            .get(&cancel.order_id)
            .unwrap_or_else(|| panic!("cancel for unknown id {}", cancel.order_id.to_hex()))
            .clone();
        assert_eq!(cancel.symbol, symbol, "cancel names the wrong symbol");
        assert_eq!(
            cancel.participant.as_deref(),
            Some(participant.as_str()),
            "cancel names the wrong participant"
        );
        let position = self
            .fifo
            .iter()
            .position(|id| *id == cancel.order_id)
            .expect("cancel target is open");
        self.fifo.remove(position);
        self.meta.remove(&cancel.order_id);
        assert!(
            !self.fifo.contains(&cancel.order_id),
            "a cancelled id stayed in the open book"
        );
        assert!(
            !self.meta.contains_key(&cancel.order_id),
            "a cancelled id stayed in the open book"
        );
        assert!(
            self.retired.insert(cancel.order_id),
            "an id was cancelled twice"
        );
    }
}

#[test]
fn invariants_hold_over_twenty_thousand_actions() {
    for scenario in SCENARIOS {
        let mut sim = simulator(config(scenario));
        let mut book = OpenBook::default();
        let mut orders = 0u64;
        let mut cancels = 0u64;
        let mut max_open = 0usize;

        for index in 1..=20_000u64 {
            let action = sim
                .next_action()
                .unwrap_or_else(|| panic!("{} ran dry at {index}", scenario.as_str()));
            match &action {
                SimAction::New(order) => {
                    orders += 1;
                    book.on_new(order);
                }
                SimAction::Cancel(cancel) => {
                    cancels += 1;
                    book.on_cancel(cancel);
                }
            }
            assert_eq!(book.fifo.len(), sim.open_orders());
            max_open = max_open.max(sim.open_orders());
            if index.is_multiple_of(100) {
                sim.check_invariants()
                    .unwrap_or_else(|error| panic!("{} at {index}: {error}", scenario.as_str()));
            }
        }

        sim.check_invariants().expect("final check");
        assert_eq!(sim.emitted(), 20_000);
        assert_eq!(sim.sequence(), orders);
        assert_eq!(
            20_000 - orders,
            cancels,
            "every non-order action is a cancel"
        );
        assert!(cancels > 0, "{} emitted no cancels", scenario.as_str());
        assert!(max_open <= OPEN_CAPACITY);
        assert!(sim.open_orders() <= OPEN_CAPACITY);
        if scenario == Scenario::CalmOpen {
            // Calm open grows the book into the cap; a liquidity drought
            // cancels as fast as it posts and legitimately never fills it.
            assert_eq!(max_open, OPEN_CAPACITY, "the open book must reach its cap");
            assert!(book.evicted > 0, "the cap must actually evict");
        }
    }
}

#[test]
fn prices_and_sizes_stay_inside_the_ladder() {
    let exchange = ExchangeConfig::default();
    for scenario in SCENARIOS {
        let mut sim = Simulator::new(
            SimConfig {
                seed: 7,
                run_nonce: 3,
                participants: 5,
                scenario,
                ..SimConfig::default()
            },
            &exchange,
        );
        let mut seen_participants = HashSet::new();
        for index in 0..5_000 {
            let Some(action) = sim.next_action() else {
                panic!("{} ran dry at {index}", scenario.as_str());
            };
            let SimAction::New(order) = action else {
                continue;
            };
            let instrument = exchange
                .instrument(order.symbol)
                .unwrap_or_else(|| panic!("{} left the instrument set", order.symbol));
            match order.order_type {
                OrderType::Market => assert_eq!(
                    order.price_cents, None,
                    "market orders must not carry a price"
                ),
                _ => {
                    let price = order.price_cents.expect("priced order");
                    assert!(
                        price >= instrument.min_price_cents(),
                        "{} priced {price} below the ladder",
                        order.symbol
                    );
                    assert!(
                        price <= instrument.max_price_cents(),
                        "{} priced {price} above the ladder",
                        order.symbol
                    );
                    assert_eq!(
                        price % instrument.tick_size_cents,
                        0,
                        "{} priced off the tick grid",
                        order.symbol
                    );
                }
            }
            assert!(
                (1..=25).contains(&order.qty),
                "qty {} out of range",
                order.qty
            );
            assert!(matches!(
                order.side,
                whippet_core::protocol::Side::Buy | whippet_core::protocol::Side::Sell
            ));
            let participant = order.participant.as_deref().expect("sim names everyone");
            let index: u64 = participant
                .strip_prefix("sim-")
                .and_then(|rest| rest.parse().ok())
                .unwrap_or_else(|| panic!("participant {participant} is not sim-NN"));
            assert!(
                (1..=5).contains(&index),
                "participant {participant} out of range"
            );
            seen_participants.insert(participant.to_string());
        }
        assert!(
            seen_participants.len() > 1,
            "{} used only one participant",
            scenario.as_str()
        );
    }
}

#[test]
fn empty_tracked_set_means_none_and_advances_nothing() {
    let config = SimConfig {
        symbols: vec![Symbol::new("NOPE-USD").unwrap()],
        ..SimConfig::default()
    };
    let mut sim = simulator(config);
    assert_eq!(sim.check_invariants(), Ok(()));
    for _ in 0..10 {
        assert!(sim.next_action().is_none());
    }
    // `next_action` must not consume RNG or counters on the None path: a second
    // simulator with a valid universe must still match a pristine one.
    assert_eq!(sim.emitted(), 0);
    assert_eq!(sim.sequence(), 0);
    assert_eq!(sim.open_orders(), 0);
    assert_eq!(sim.check_invariants(), Ok(()));

    let mut exchange = ExchangeConfig::default();
    exchange.instruments.clear();
    let mut empty = Simulator::new(SimConfig::default(), &exchange);
    assert!(empty.next_action().is_none());
    assert_eq!(empty.check_invariants(), Ok(()));
}

#[test]
fn duplicate_and_unknown_symbols_collapse_to_the_same_stream() {
    let exchange = ExchangeConfig::default();
    let btc = Symbol::new("BTC-USD").unwrap();
    let base = SimConfig {
        seed: 5,
        run_nonce: 9,
        symbols: vec![btc],
        ..SimConfig::default()
    };
    let noisy = SimConfig {
        symbols: vec![btc, btc, Symbol::new("NOPE-USD").unwrap(), btc],
        ..base.clone()
    };
    let mut clean = Simulator::new(base.clone(), &exchange);
    let mut deduped = Simulator::new(noisy, &exchange);
    assert_eq!(clean.check_invariants(), Ok(()));
    assert_eq!(deduped.check_invariants(), Ok(()));

    for _ in 0..200 {
        let left = clean.next_action().expect("one symbol");
        let right = deduped.next_action().expect("one symbol");
        assert_eq!(common::encode(&left), common::encode(&right));
        match left {
            SimAction::New(order) => assert_eq!(order.symbol, btc),
            SimAction::Cancel(cancel) => assert_eq!(cancel.symbol, btc),
        }
    }
}

#[test]
fn a_two_tick_ladder_constructs_and_never_panics() {
    let exchange = ExchangeConfig {
        venue: "test".into(),
        partitions: 1,
        mode: Mode::Durable,
        instruments: vec![InstrumentConfig {
            symbol: Symbol::new("TINY-USD").unwrap(),
            tick_size_cents: 1,
            min_price_ticks: 0,
            max_price_ticks: 1,
            max_orders: 100,
            snapshot_depth: 5,
            market_collar_bps: 500,
        }],
    };
    let mut sim = Simulator::new(SimConfig::default(), &exchange);
    assert_eq!(sim.check_invariants(), Ok(()));
    for index in 0..500 {
        let action = sim.next_action().expect("one instrument");
        if let SimAction::New(order) = action
            && let Some(price) = order.price_cents
        {
            assert!(
                (0..=1).contains(&price),
                "price {price} left the two-tick ladder"
            );
        }
        if index % 10 == 0 {
            sim.check_invariants()
                .expect("tiny ladder stays consistent");
        }
    }
}

#[test]
fn check_invariants_reports_corrupt_instruments_instead_of_panicking() {
    let symbol = Symbol::new("BTC-USD").unwrap();
    let broken_tick = ExchangeConfig {
        venue: "test".into(),
        partitions: 1,
        mode: Mode::Durable,
        instruments: vec![InstrumentConfig {
            symbol,
            tick_size_cents: 0,
            min_price_ticks: 40_000,
            max_price_ticks: 90_000,
            max_orders: 100,
            snapshot_depth: 5,
            market_collar_bps: 500,
        }],
    };
    let mut sim = Simulator::new(SimConfig::default(), &broken_tick);
    let error = sim.check_invariants().expect_err("tick size 0 is invalid");
    assert!(error.contains("tick size"), "unexpected error: {error}");
    // Generation must stay panic-free; the caller is expected to check first.
    let _ = sim.next_action();

    let inverted = ExchangeConfig {
        venue: "test".into(),
        partitions: 1,
        mode: Mode::Durable,
        instruments: vec![InstrumentConfig {
            symbol,
            tick_size_cents: 100,
            min_price_ticks: 90_000,
            max_price_ticks: 40_000,
            max_orders: 100,
            snapshot_depth: 5,
            market_collar_bps: 500,
        }],
    };
    let mut sim = Simulator::new(SimConfig::default(), &inverted);
    let error = sim
        .check_invariants()
        .expect_err("inverted ladder is invalid");
    assert!(error.contains("inverted"), "unexpected error: {error}");
    for _ in 0..20 {
        let _ = sim.next_action();
    }
}
