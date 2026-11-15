// Licensed under the Apache License, Version 2.0. See LICENSE.

use proptest::prelude::*;
use whippet_core::config::InstrumentConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_engine::{EngineOutput, SymbolEngine, hash_symbol_snapshot};

fn config() -> InstrumentConfig {
    let mut config = InstrumentConfig::new("BTC-USD", 100, 90, 120).unwrap();
    config.max_orders = 512;
    config
}

#[derive(Debug, Clone)]
enum Op {
    Limit {
        side: Side,
        price: i64,
        qty: u64,
        participant: u64,
    },
    Market {
        side: Side,
        qty: u64,
        participant: u64,
    },
    Cancel {
        id: u128,
    },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (any::<bool>(), 90i64..=120, 1u64..=50, 1u64..=4).prop_map(
            |(buy, price, qty, participant)| Op::Limit {
                side: if buy { Side::Buy } else { Side::Sell },
                price,
                qty,
                participant,
            }
        ),
        1 => (any::<bool>(), 1u64..=30, 1u64..=4).prop_map(|(buy, qty, participant)| {
            Op::Market {
                side: if buy { Side::Buy } else { Side::Sell },
                qty,
                participant,
            }
        }),
        2 => (0u128..40).prop_map(|id| Op::Cancel { id }),
    ]
}

fn check_invariants(engine: &SymbolEngine) {
    engine
        .check_invariants()
        .unwrap_or_else(|error| panic!("engine invariants: {error}"));

    let book = engine.book();
    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
        assert!(bid.0 < ask.0, "crossed book: {bid:?} >= {ask:?}");
    }

    let snapshot = book.snapshot();
    assert_eq!(
        snapshot.orders.len(),
        book.order_count(),
        "snapshot and arena disagree on order count"
    );

    let mut ids = std::collections::HashSet::new();
    let mut bid_qty = 0u64;
    let mut ask_qty = 0u64;
    for order in &snapshot.orders {
        assert!(ids.insert(order.order_id), "duplicate order id in book");
        assert!(order.remaining.0 > 0, "zero-remaining order in book");
        match order.side {
            Side::Buy => bid_qty += order.remaining.0,
            Side::Sell => ask_qty += order.remaining.0,
        }
    }
    assert_eq!(bid_qty, book.depth_qty(Side::Buy));
    assert_eq!(ask_qty, book.depth_qty(Side::Sell));
}

proptest! {
    #[test]
    fn book_invariants_hold_under_random_flow(ops in proptest::collection::vec(arb_op(), 1..200)) {
        let symbol = Symbol::new("BTC-USD").unwrap();
        let mut engine = SymbolEngine::new(config());
        let mut out = EngineOutput::with_capacity(16, 16);
        for (offset, op) in ops.into_iter().enumerate() {
            let command = match op {
                Op::Limit { side, price, qty, participant } => Command::new_order(
                    symbol,
                    side,
                    OrderType::Limit,
                    Price(price),
                    Qty(qty),
                    OrderId(offset as u128 + 1),
                    ParticipantId(participant),
                    offset as u64,
                ),
                Op::Market { side, qty, participant } => Command::new_order(
                    symbol,
                    side,
                    OrderType::Market,
                    Price(0),
                    Qty(qty),
                    OrderId(offset as u128 + 1),
                    ParticipantId(participant),
                    offset as u64,
                ),
                Op::Cancel { id } => Command::cancel(
                    symbol,
                    OrderId(id),
                    ParticipantId(1),
                    offset as u64,
                ),
            };
            engine.process(&command, offset as u64, &mut out);
            engine
                .check_output_invariants(&command, offset as u64, &out)
                .unwrap_or_else(|error| {
                    panic!("output invariants after command {offset}: {error}")
                });
            check_invariants(&engine);
            out.clear();
        }
    }

    #[test]
    fn replay_is_deterministic(ops in proptest::collection::vec(arb_op(), 1..100)) {
        let symbol = Symbol::new("BTC-USD").unwrap();
        let commands: Vec<Command> = ops
            .into_iter()
            .enumerate()
            .map(|(offset, op)| match op {
                Op::Limit { side, price, qty, participant } => Command::new_order(
                    symbol,
                    side,
                    OrderType::Limit,
                    Price(price),
                    Qty(qty),
                    OrderId(offset as u128 + 1),
                    ParticipantId(participant),
                    offset as u64,
                ),
                Op::Market { side, qty, participant } => Command::new_order(
                    symbol,
                    side,
                    OrderType::Market,
                    Price(0),
                    Qty(qty),
                    OrderId(offset as u128 + 1),
                    ParticipantId(participant),
                    offset as u64,
                ),
                Op::Cancel { id } => Command::cancel(
                    symbol,
                    OrderId(id),
                    ParticipantId(1),
                    offset as u64,
                ),
            })
            .collect();

        let mut first = SymbolEngine::new(config());
        let mut second = SymbolEngine::new(config());
        let mut out = EngineOutput::with_capacity(16, 16);
        for (offset, command) in commands.iter().enumerate() {
            first.process(command, offset as u64, &mut out);
            first
                .check_output_invariants(command, offset as u64, &out)
                .unwrap_or_else(|error| {
                    panic!("output invariants after command {offset}: {error}")
                });
            out.clear();
            second.process(command, offset as u64, &mut out);
            second
                .check_output_invariants(command, offset as u64, &out)
                .unwrap_or_else(|error| {
                    panic!("output invariants after command {offset}: {error}")
                });
            out.clear();
        }
        prop_assert_eq!(first.state_hash(), second.state_hash());
        prop_assert_eq!(first.book().best_bid(), second.book().best_bid());
        prop_assert_eq!(first.book().best_ask(), second.book().best_ask());
        if let Err(error) = first.check_invariants() {
            prop_assert!(false, "engine invariants: {error}");
        }
    }

    #[test]
    fn snapshot_hash_matches_live_state_hash(
        ops in proptest::collection::vec(arb_op(), 1..200)
    ) {
        let symbol = Symbol::new("BTC-USD").unwrap();
        let mut engine = SymbolEngine::new(config());
        let mut out = EngineOutput::with_capacity(16, 16);
        for (offset, op) in ops.into_iter().enumerate() {
            let command = match op {
                Op::Limit { side, price, qty, participant } => Command::new_order(
                    symbol,
                    side,
                    OrderType::Limit,
                    Price(price),
                    Qty(qty),
                    OrderId(offset as u128 + 1),
                    ParticipantId(participant),
                    offset as u64,
                ),
                Op::Market { side, qty, participant } => Command::new_order(
                    symbol,
                    side,
                    OrderType::Market,
                    Price(0),
                    Qty(qty),
                    OrderId(offset as u128 + 1),
                    ParticipantId(participant),
                    offset as u64,
                ),
                Op::Cancel { id } => Command::cancel(
                    symbol,
                    OrderId(id),
                    ParticipantId(1),
                    offset as u64,
                ),
            };
            engine.process(&command, offset as u64, &mut out);
            engine
                .check_output_invariants(&command, offset as u64, &out)
                .unwrap_or_else(|error| {
                    panic!("output invariants after command {offset}: {error}")
                });
            out.clear();
        }
        let snapshot = engine.snapshot_state_without_hash();
        prop_assert_eq!(snapshot.state_hash, [0; 32]);
        prop_assert_eq!(hash_symbol_snapshot(&snapshot), engine.state_hash());
        if let Err(error) = engine.check_invariants() {
            prop_assert!(false, "engine invariants: {error}");
        }
    }
}
