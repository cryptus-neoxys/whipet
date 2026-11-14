// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Property coverage for random valid order flow.
//!
//! `properties.rs` already checks the coarse book invariants. This file adds
//! per-level accounting (every resting order contributes to exactly one level
//! and the level total equals the sum of those orders) on a flow that includes
//! every order type and cancels.

use std::collections::BTreeMap;

use proptest::prelude::*;
use whippet_core::config::InstrumentConfig;
use whippet_core::protocol::Side;
use whippet_engine::{EngineOutput, SymbolEngine};
use whippet_testkit::strategies::arb_flow;

fn config() -> InstrumentConfig {
    let mut config = InstrumentConfig::new("BTC-USD", 100, 90, 120).expect("static config");
    config.max_orders = 512;
    config
}

fn check_book(engine: &SymbolEngine, depth: usize) {
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
