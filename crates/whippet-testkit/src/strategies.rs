// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Reusable proptest strategies for valid Whippet commands.
//!
//! A strategy never produces an out-of-ladder price, a zero quantity, or a
//! malformed symbol. It may produce a command the engine rejects for business
//! reasons (an unfillable FOK, a crossing post-only, a cancel for an unknown
//! id); those are valid inputs and exercising the reject paths is the point.

use proptest::prelude::*;
use whippet_core::config::InstrumentConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};

/// Symbols from the built-in demo universe.
pub fn arb_symbol() -> impl Strategy<Value = Symbol> {
    prop::sample::select(vec![
        Symbol::new("BTC-USD").expect("static symbol"),
        Symbol::new("ETH-USD").expect("static symbol"),
        Symbol::new("SOL-USD").expect("static symbol"),
    ])
}

/// Both sides, evenly weighted.
pub fn arb_side() -> impl Strategy<Value = Side> {
    prop::sample::select(vec![Side::Buy, Side::Sell])
}

/// Order types, weighted toward resting limit orders.
pub fn arb_order_type() -> impl Strategy<Value = OrderType> {
    prop_oneof![
        8 => Just(OrderType::Limit),
        1 => Just(OrderType::Market),
        1 => Just(OrderType::Ioc),
        1 => Just(OrderType::Fok),
        1 => Just(OrderType::PostOnly),
    ]
}

/// A positive quantity small enough to keep books readable in failures.
pub fn arb_qty() -> impl Strategy<Value = u64> {
    1u64..=100
}

/// A price inside the instrument's ladder, inclusive.
pub fn arb_price(config: &InstrumentConfig) -> impl Strategy<Value = Price> + use<> {
    (config.min_price_ticks..=config.max_price_ticks).prop_map(Price)
}

/// A single valid new-order command. Market orders get `Price::ZERO`, matching
/// the wire contract.
pub fn arb_new_order(
    config: &InstrumentConfig,
    order_id: u128,
    participant: u64,
    timestamp_ns: u64,
) -> impl Strategy<Value = Command> + use<> {
    let symbol = config.symbol;
    let price = arb_price(config);
    (arb_side(), arb_order_type(), price, arb_qty()).prop_map(
        move |(side, order_type, price, qty)| {
            let price = if order_type.requires_price() {
                price
            } else {
                Price::ZERO
            };
            Command::new_order(
                symbol,
                side,
                order_type,
                price,
                Qty(qty),
                OrderId(order_id),
                ParticipantId(participant),
                timestamp_ns,
            )
        },
    )
}

/// A valid cancel command for an id in `0..=max_id`.
pub fn arb_cancel(
    symbol: Symbol,
    max_id: u128,
    participant: u64,
    timestamp_ns: u64,
) -> impl Strategy<Value = Command> {
    (0u128..=max_id).prop_map(move |order_id| {
        Command::cancel(
            symbol,
            OrderId(order_id),
            ParticipantId(participant),
            timestamp_ns,
        )
    })
}

/// A random, valid command flow against one instrument.
///
/// New orders receive sequential ids starting at 1. Cancels target an id in
/// `1..=64`, so they may name an order that has not been submitted yet; the
/// engine treats that as `UnknownOrder`, which is still a valid command.
pub fn arb_flow(config: InstrumentConfig, max_len: usize) -> impl Strategy<Value = Vec<Command>> {
    let symbol = config.symbol;
    let op = prop_oneof![
        8 => (arb_side(), arb_order_type(), arb_price(&config), arb_qty(), 1u64..=4)
            .prop_map(|(side, order_type, price, qty, participant)| FlowOp::New {
                side,
                order_type,
                price,
                qty,
                participant,
            }),
        2 => (1u128..=64).prop_map(|id| FlowOp::Cancel { id }),
    ];
    proptest::collection::vec(op, 1..max_len).prop_map(move |ops| {
        let mut next_id = 1u128;
        ops.into_iter()
            .enumerate()
            .map(|(offset, op)| match op {
                FlowOp::New {
                    side,
                    order_type,
                    price,
                    qty,
                    participant,
                } => {
                    let order_id = next_id;
                    next_id += 1;
                    let price = if order_type.requires_price() {
                        price
                    } else {
                        Price::ZERO
                    };
                    Command::new_order(
                        symbol,
                        side,
                        order_type,
                        price,
                        Qty(qty),
                        OrderId(order_id),
                        ParticipantId(participant),
                        offset as u64,
                    )
                }
                FlowOp::Cancel { id } => {
                    Command::cancel(symbol, OrderId(id), ParticipantId(1), offset as u64)
                }
            })
            .collect()
    })
}

#[derive(Debug, Clone, Copy)]
enum FlowOp {
    New {
        side: Side,
        order_type: OrderType,
        price: Price,
        qty: u64,
        participant: u64,
    },
    Cancel {
        id: u128,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    fn config() -> InstrumentConfig {
        InstrumentConfig::new("BTC-USD", 100, 90, 120).expect("static config")
    }

    #[test]
    fn generated_commands_stay_inside_the_ladder() {
        let config = config();
        let mut runner = TestRunner::deterministic();
        let strategy = arb_flow(config.clone(), 40);
        for _ in 0..20 {
            let commands = strategy.new_tree(&mut runner).unwrap().current();
            for command in commands {
                if command.kind == whippet_core::protocol::CommandKind::Cancel {
                    continue;
                }
                if command.order_type.requires_price() {
                    assert!(
                        config.contains_ticks(command.price),
                        "price {} outside ladder",
                        command.price
                    );
                } else {
                    assert_eq!(command.price, Price::ZERO);
                }
                assert!(!command.qty.is_zero());
            }
        }
    }
}
