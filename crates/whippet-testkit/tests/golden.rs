// Licensed under the Apache License, Version 2.0. See LICENSE.

use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::Price;
use whippet_core::protocol::RejectReason;
use whippet_testkit::{GOLDEN_HASH, GoldenLog, scenario_config, standard_shard};

#[test]
fn standard_scenario_has_a_pinned_hash() {
    let result = GoldenLog::standard().run_symbol(scenario_config());
    println!("golden hash = {}", result.hash_hex());
    assert_eq!(
        result.hash_hex(),
        GOLDEN_HASH,
        "matching semantics changed; see whippet_testkit::golden module docs"
    );

    let book = result.books.values().next().expect("one book");
    assert_eq!(book.order_count, 4);
    assert_eq!(book.best_bid, Some(Price(104)));
    assert_eq!(book.best_ask, Some(Price(107)));
    assert_eq!(book.bid_depth_qty, 6);
    assert_eq!(book.ask_depth_qty, 12);

    let exec = result.execution_summary();
    assert!(exec.accepted > 0);
    assert!(exec.filled > 0);
    assert!(exec.partially_filled > 0);
    assert!(exec.cancelled > 0);
    assert_eq!(exec.rejects(RejectReason::DuplicateOrderId), 1);
    assert_eq!(exec.rejects(RejectReason::UnknownOrder), 1);
    assert_eq!(exec.rejects(RejectReason::FokUnfillable), 1);
    assert_eq!(exec.rejects(RejectReason::PostOnlyWouldCross), 1);

    let md = result.md_summary();
    assert!(md.add > 0);
    assert!(md.reduce > 0);
    assert!(md.remove > 0);
    assert!(md.trade > 0);
    assert!(md.traded_qty > 0);
}

#[test]
fn standard_scenario_is_deterministic_across_engine_kinds() {
    let log = GoldenLog::standard();
    let first = log.run_symbol(scenario_config());
    let second = log.run_symbol(scenario_config());
    assert_eq!(first, second);

    let exchange = ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Durable,
        instruments: vec![scenario_config()],
    };
    let shard = standard_shard(&exchange);
    let shard_first = log
        .run_shard(&exchange, shard)
        .expect("scenario stays on one shard");
    let shard_second = log
        .run_shard(&exchange, shard)
        .expect("scenario stays on one shard");
    assert_eq!(shard_first, shard_second);
    assert_eq!(shard_first.books, first.books);
    assert_eq!(shard_first.executions, first.executions);
    assert_eq!(shard_first.md, first.md);
}
