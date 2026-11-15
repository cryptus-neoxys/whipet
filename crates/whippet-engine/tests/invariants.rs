// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Invariant coverage for random valid order flow.
//!
//! `properties.rs` already checks the coarse book invariants through
//! `SymbolEngine::check_invariants`. This file adds per-level accounting (every
//! resting order contributes to exactly one level and the level total equals
//! the sum of those orders) on a flow that includes every order type and
//! cancels, plus a long randomized workload that validates every engine,
//! book, and dedup invariant after each command.

use std::collections::BTreeMap;

use proptest::prelude::*;
use whippet_core::config::InstrumentConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty};
use whippet_core::protocol::{Command, ExecKind, OrderType, RejectReason, SessionState, Side};
use whippet_engine::{EngineOutput, SymbolEngine};
use whippet_testkit::strategies::arb_flow;

fn config() -> InstrumentConfig {
    let mut config = InstrumentConfig::new("BTC-USD", 100, 90, 120).expect("static config");
    config.max_orders = 512;
    config
}

fn check_book(engine: &SymbolEngine, depth: usize) {
    engine
        .check_invariants()
        .unwrap_or_else(|error| panic!("engine invariants: {error}"));

    let book = engine.book();
    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
        assert!(bid.0 < ask.0, "crossed book: {bid:?} >= {ask:?}");
    }

    let snapshot = book.snapshot();
    let mut active: BTreeMap<(u8, i64), (u64, u32)> = BTreeMap::new();
    let mut bid_total = 0u64;
    let mut ask_total = 0u64;
    for order in &snapshot.orders {
        assert!(order.remaining.0 > 0, "zero-remaining order in book");
        let entry = active
            .entry((order.side.as_code(), order.price.0))
            .or_insert((0, 0));
        entry.0 += order.remaining.0;
        entry.1 += 1;
        match order.side {
            Side::Buy => bid_total += order.remaining.0,
            Side::Sell => ask_total += order.remaining.0,
        }
    }

    assert_eq!(bid_total, book.depth_qty(Side::Buy));
    assert_eq!(ask_total, book.depth_qty(Side::Sell));

    let (bids, asks) = book.snapshot_levels(depth);
    for level in bids {
        let expected = active
            .get(&(Side::Buy.as_code(), level.price.0))
            .copied()
            .unwrap_or((0, 0));
        assert_eq!(
            (level.qty.0, level.orders),
            expected,
            "bid level {} disagrees with resting orders",
            level.price
        );
    }
    for level in asks {
        let expected = active
            .get(&(Side::Sell.as_code(), level.price.0))
            .copied()
            .unwrap_or((0, 0));
        assert_eq!(
            (level.qty.0, level.orders),
            expected,
            "ask level {} disagrees with resting orders",
            level.price
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn random_valid_flow_keeps_levels_consistent(commands in arb_flow(config(), 120)) {
        let config = config();
        let depth = config.ladder_len();
        let mut engine = SymbolEngine::new(config);
        let mut out = EngineOutput::with_capacity(16, 16);
        for (offset, command) in commands.iter().enumerate() {
            engine.process(command, offset as u64, &mut out);
            out.clear();
            check_book(&engine, depth);
        }
    }
}

/// A tiny deterministic PRNG (SplitMix64). The workload needs a reproducible
/// random flow, not statistical quality, and this keeps the dependency set as
/// is (the same shape `snapshot_hash.rs` uses).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

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

/// A small book: 256 arena slots, so the workload can genuinely fill the arena
/// and drive the internal-failure path, over a 31-level ladder.
fn workload_config() -> InstrumentConfig {
    let mut config = InstrumentConfig::new("BTC-USD", 100, 90, 120).expect("static config");
    config.max_orders = 256;
    config
}

/// A mixed command flow: limits, market, IOC, FOK, post-only, cancels, retried
/// ids (duplicate rejections), two participants (self-trade prevention), and a
/// halt/resume and pre-open/reopen pair so the create-time rejection path is
/// exercised too.
fn workload(seed: u64, len: usize) -> Vec<Command> {
    let symbol = workload_config().symbol;
    let mut rng = Rng::new(seed);
    let mut commands = Vec::with_capacity(len);
    let mut next_id = 1u128;

    for offset in 0..len {
        let timestamp = offset as u64;
        // Most ids are fresh; a slice retries a recent one, which is either a
        // duplicate rejection or an order already gone from the dedup window.
        let order_id = if rng.below(100) < 12 && next_id > 8 {
            let back = u128::from(rng.below(40) + 1);
            OrderId(next_id.saturating_sub(back).max(1))
        } else {
            let order_id = OrderId(next_id);
            next_id += 1;
            order_id
        };
        let participant = ParticipantId(rng.below(2) + 1);
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        // Mix aggressive quotes (which cross and fill) with passive quotes
        // parked away from the spread. Without the passive side the book keeps
        // clearing itself and a 256-slot arena is never exercised.
        let price = if rng.below(100) < 60 {
            match side {
                Side::Buy => Price(90 + rng.below(6) as i64),
                Side::Sell => Price(115 + rng.below(6) as i64),
            }
        } else {
            Price(90 + rng.below(31) as i64)
        };
        let qty = Qty(rng.below(20) + 1);
        let roll = rng.below(100);

        let command = match offset {
            // Operator commands: one halt/resume and one session gate in the
            // middle of the run, so new orders are rejected (and recorded
            // terminal at creation) at least once.
            1_250 => Command::halt(symbol, ParticipantId(9), timestamp),
            1_260 => Command::resume(symbol, ParticipantId(9), timestamp),
            2_500 => {
                Command::set_session(symbol, SessionState::PreOpen, ParticipantId(9), timestamp)
            }
            2_510 => Command::set_session(symbol, SessionState::Open, ParticipantId(9), timestamp),
            _ => match roll {
                0..=54 => Command::new_order(
                    symbol,
                    side,
                    if rng.below(12) == 0 {
                        OrderType::PostOnly
                    } else {
                        OrderType::Limit
                    },
                    price,
                    qty,
                    order_id,
                    participant,
                    timestamp,
                ),
                55..=64 => Command::new_order(
                    symbol,
                    side,
                    OrderType::Market,
                    Price::ZERO,
                    qty,
                    order_id,
                    participant,
                    timestamp,
                ),
                65..=74 => Command::new_order(
                    symbol,
                    side,
                    OrderType::Ioc,
                    price,
                    qty,
                    order_id,
                    participant,
                    timestamp,
                ),
                75..=82 => Command::new_order(
                    symbol,
                    side,
                    OrderType::Fok,
                    price,
                    qty,
                    order_id,
                    participant,
                    timestamp,
                ),
                _ => Command::cancel(symbol, order_id, participant, timestamp),
            },
        };
        commands.push(command);
    }
    commands
}

#[test]
fn randomized_workload_keeps_every_engine_invariant() {
    const LEN: usize = 5_000;
    let config = workload_config();
    let commands = workload(0x5eed_f00d, LEN);
    let mut engine = SymbolEngine::new(config);
    let mut out = EngineOutput::with_capacity(16, 16);

    let mut accepted = 0usize;
    let mut duplicate_rejections = 0usize;
    let mut internal_cancels = 0usize;
    let mut stp_cancels = 0usize;
    let mut max_book = 0usize;

    for (offset, command) in commands.iter().enumerate() {
        engine.process(command, offset as u64, &mut out);
        for exec in &out.executions {
            match (exec.kind, exec.reason) {
                (ExecKind::Accepted, _) => accepted += 1,
                (ExecKind::Rejected, RejectReason::DuplicateOrderId) => duplicate_rejections += 1,
                (ExecKind::Cancelled, RejectReason::Internal) => internal_cancels += 1,
                (ExecKind::Cancelled, RejectReason::SelfTradePrevention) => stp_cancels += 1,
                _ => {}
            }
        }
        if let Err(error) = engine.check_invariants() {
            panic!(
                "invariants violated after command {offset} ({:?}): {error}",
                command.kind
            );
        }
        max_book = max_book.max(engine.book().order_count());
        out.clear();
    }

    // Pin that the flow actually reaches the states the invariants are meant to
    // guard, instead of degenerating into a stream of rejections.
    assert!(accepted > 0, "the workload never accepted an order");
    assert!(duplicate_rejections > 0, "the workload never retried an id");
    assert!(
        internal_cancels > 0,
        "the 256-slot arena was never filled (max book depth {max_book}), so the internal-failure path was not exercised"
    );
    assert!(
        stp_cancels > 0,
        "the workload never triggered self-trade prevention"
    );
    assert!(
        engine.dedup().len() < LEN,
        "the dedup window never turned over ({} live ids)",
        engine.dedup().len()
    );
    assert!(
        engine.book().order_count() > 0,
        "the workload ended with an empty book, so nothing was resting"
    );
}
