// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Golden, replay, and recovery tests for the matching engine.
//!
//! Everything here drives `SymbolEngine` or `ShardEngine` directly. No Tokio,
//! no journal, no snapshot files: the engine is a pure state machine and the
//! snapshot is a plain value.

use std::collections::BTreeMap;

use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{
    Command, CommandKind, ExecKind, ExecutionReport, OrderType, RejectReason, Side,
};
use whippet_engine::{
    Book, EngineOutput, ShardEngine, SymbolEngine, fill_symbol_hashes, hash_symbol_snapshot,
};
use whippet_testkit::builders;
use whippet_testkit::golden::{GOLDEN_HASH, GoldenLog, scenario_config, standard_shard};

fn symbol() -> Symbol {
    builders::default_symbol()
}

fn process(engine: &mut SymbolEngine, command: &Command, offset: u64) -> EngineOutput {
    let mut out = EngineOutput::with_capacity(16, 16);
    engine.process(command, offset, &mut out);
    out
}

fn process_all(engine: &mut SymbolEngine, commands: &[Command]) -> Vec<ExecutionReport> {
    let mut out = EngineOutput::with_capacity(16, 16);
    let mut executions = Vec::new();
    for (offset, command) in commands.iter().enumerate() {
        engine.process(command, offset as u64, &mut out);
        executions.extend(out.executions.iter().cloned());
        out.clear();
    }
    executions
}

fn assert_no_cross(book: &Book) {
    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
        assert!(bid.0 < ask.0, "crossed book: {bid:?} >= {ask:?}");
    }
}

#[test]
fn golden_hash_is_pinned() {
    let result = GoldenLog::standard().run_symbol(scenario_config());
    assert_eq!(
        result.hash_hex(),
        GOLDEN_HASH,
        "matching semantics changed; update GOLDEN_HASH deliberately (see whippet_testkit::golden)"
    );
    let final_state = result
        .books
        .get(&symbol())
        .expect("golden scenario owns BTC-USD");
    assert_eq!(final_state.order_count, 4);
    assert_eq!(final_state.best_bid, Some(Price(104)));
    assert_eq!(final_state.best_ask, Some(Price(107)));
    assert_eq!(final_state.bid_depth_qty, 6);
    assert_eq!(final_state.ask_depth_qty, 12);
}

#[test]
fn replay_is_deterministic_across_two_engines() {
    let log = GoldenLog::standard();
    let first = log.run_symbol(scenario_config());
    let second = log.run_symbol(scenario_config());
    assert_eq!(first.state_hash, second.state_hash);
    assert_eq!(first.executions, second.executions);
    assert_eq!(first.md, second.md);
    assert_eq!(first.books, second.books);
}

#[test]
fn snapshot_seeded_tail_replay_equals_full_replay() {
    let config = scenario_config();
    let commands = GoldenLog::standard().commands().to_vec();

    let mut full = SymbolEngine::new(config.clone());
    process_all(&mut full, &commands);
    let full_hash = full.state_hash();

    // Recover from several points, including the empty book and one command
    // before the end. Each tail must land on exactly the full-replay hash.
    for split in [0usize, 1, 5, 17, commands.len() - 1] {
        let mut prefix = SymbolEngine::new(config.clone());
        let mut out = EngineOutput::with_capacity(16, 16);
        for (offset, command) in commands.iter().enumerate().take(split) {
            prefix.process(command, offset as u64, &mut out);
            out.clear();
        }
        let snapshot = prefix.snapshot_state();
        let mut restored = SymbolEngine::from_snapshot(config.clone(), snapshot)
            .expect("snapshot round-trips with a matching hash");
        assert_eq!(restored.state_hash(), prefix.state_hash());

        for (offset, command) in commands.iter().enumerate().skip(split) {
            restored.process(command, offset as u64, &mut out);
            out.clear();
        }
        assert_eq!(
            restored.state_hash(),
            full_hash,
            "snapshot at {split} + tail diverged from full replay"
        );
    }
}

#[test]
fn shard_snapshot_seeded_tail_replay_equals_full_replay() {
    let exchange = ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Durable,
        instruments: vec![scenario_config()],
    };
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();

    let mut full = ShardEngine::new(shard, &exchange);
    let mut out = EngineOutput::with_capacity(16, 16);
    for (offset, command) in commands.iter().enumerate() {
        full.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }
    let full_hash = full.state_hash();

    let split = 17;
    let mut prefix = ShardEngine::new(shard, &exchange);
    for (offset, command) in commands.iter().enumerate().take(split) {
        prefix.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }
    let snapshot = prefix.snapshot_state();
    let mut restored = ShardEngine::from_snapshot(&exchange, snapshot).unwrap();
    assert_eq!(restored.state_hash(), prefix.state_hash());
    for (offset, command) in commands.iter().enumerate().skip(split) {
        restored.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }
    assert_eq!(restored.state_hash(), full_hash);
}

#[test]
fn price_time_priority_spans_multiple_levels() {
    let mut engine = SymbolEngine::new(scenario_config());
    let symbol = symbol();
    let commands = [
        builders::order_for(symbol, 1, Side::Sell, 105, 5, 1),
        builders::order_for(symbol, 2, Side::Sell, 105, 5, 2),
        builders::order_for(symbol, 3, Side::Sell, 106, 5, 3),
        builders::order_for(symbol, 4, Side::Buy, 106, 20, 4),
    ];
    let executions = process_all(&mut engine, &commands);

    let fills: Vec<(OrderId, Price, Qty)> = executions
        .iter()
        .filter(|exec| matches!(exec.kind, ExecKind::Filled | ExecKind::PartiallyFilled))
        .map(|exec| (exec.maker_order_id, exec.price, exec.qty))
        .collect();
    assert_eq!(
        fills,
        vec![
            (OrderId(1), Price(105), Qty(5)),
            (OrderId(2), Price(105), Qty(5)),
            (OrderId(3), Price(106), Qty(5)),
        ],
        "fills must follow price, then FIFO time at the same price"
    );
    assert_eq!(engine.book().best_bid(), Some(Price(106)));
    assert_eq!(engine.book().best_ask(), None);
    assert_eq!(engine.book().depth_qty(Side::Buy), 5);
    assert_no_cross(engine.book());
}

#[test]
fn quantity_is_conserved_and_the_book_never_crosses() {
    let mut engine = SymbolEngine::new(scenario_config());
    let log = GoldenLog::standard();
    let executions = process_all(&mut engine, log.commands());

    assert_no_cross(engine.book());
    assert_quantity_conservation(&executions, engine.book());
}

#[test]
fn resting_plus_filled_equals_submitted_without_cancels() {
    let symbol = symbol();
    let mut engine = SymbolEngine::new(scenario_config());
    let commands = [
        builders::order_for(symbol, 1, Side::Sell, 105, 10, 1),
        builders::order_for(symbol, 2, Side::Sell, 105, 4, 2),
        builders::order_for(symbol, 3, Side::Buy, 105, 12, 3),
        builders::order_for(symbol, 4, Side::Buy, 100, 6, 4),
    ];
    let executions = process_all(&mut engine, &commands);
    assert_quantity_conservation(&executions, engine.book());

    let submitted: u64 = commands.iter().map(|command| command.qty.0).sum();
    let resting = engine.book().depth_qty(Side::Buy) + engine.book().depth_qty(Side::Sell);
    let mut filled_by_order: BTreeMap<OrderId, u64> = BTreeMap::new();
    for exec in &executions {
        if matches!(exec.kind, ExecKind::Filled | ExecKind::PartiallyFilled) {
            *filled_by_order.entry(exec.order_id).or_default() += exec.qty.0;
            if exec.maker_order_id != OrderId(0) {
                *filled_by_order.entry(exec.maker_order_id).or_default() += exec.qty.0;
            }
        }
    }
    let filled: u64 = filled_by_order.values().sum();
    assert_eq!(
        filled + resting,
        submitted,
        "every submitted unit is either filled or resting"
    );
}

#[test]
fn duplicate_client_order_id_has_one_effect() {
    let symbol = symbol();
    let client_order_id = "retry-1";
    let order_id = OrderId::derive(symbol, client_order_id);
    let first = Command::new_order(
        symbol,
        Side::Buy,
        OrderType::Limit,
        Price(100),
        Qty(10),
        order_id,
        ParticipantId(7),
        0,
    );
    let mut retry = first.clone();
    retry.timestamp_ns = 1;

    let mut engine = SymbolEngine::new(scenario_config());
    let first_out = process(&mut engine, &first, 0);
    assert_eq!(first_out.executions.len(), 1);
    assert_eq!(first_out.executions[0].kind, ExecKind::Accepted);

    let retry_out = process(&mut engine, &retry, 1);
    assert_eq!(retry_out.executions.len(), 1);
    assert_eq!(retry_out.executions[0].kind, ExecKind::Rejected);
    assert_eq!(
        retry_out.executions[0].reason,
        RejectReason::DuplicateOrderId
    );

    assert_eq!(engine.book().order_count(), 1);
    assert_eq!(engine.book().depth_qty(Side::Buy), 10);
    assert_eq!(engine.dedup().len(), 1);
}

#[test]
fn cancel_then_new_id_is_the_only_way_to_replace() {
    let symbol = symbol();
    let mut engine = SymbolEngine::new(scenario_config());
    let first = builders::order_for(symbol, 1, Side::Buy, 100, 10, 1);
    process(&mut engine, &first, 0);

    let cancel = builders::cancel_for(symbol, 1, 1);
    let cancel_out = process(&mut engine, &cancel, 1);
    assert_eq!(cancel_out.executions[0].kind, ExecKind::Cancelled);
    assert_eq!(engine.book().order_count(), 0);

    let replacement = builders::order_for(symbol, 1, Side::Buy, 100, 10, 2);
    let replacement_out = process(&mut engine, &replacement, 2);
    assert_eq!(replacement_out.executions[0].kind, ExecKind::Accepted);
    let snapshot = engine.book().snapshot();
    assert_eq!(snapshot.orders.len(), 1);
    assert_eq!(snapshot.orders[0].order_id, OrderId(2));

    let stale_cancel = builders::cancel_for(symbol, 1, 1);
    let stale_out = process(&mut engine, &stale_cancel, 3);
    assert_eq!(stale_out.executions[0].kind, ExecKind::Rejected);
    assert_eq!(stale_out.executions[0].reason, RejectReason::UnknownOrder);
}

#[test]
fn cancel_commands_do_not_create_book_orders() {
    let symbol = symbol();
    let mut engine = SymbolEngine::new(scenario_config());
    let cancel = builders::cancel_for(symbol, 1, 42);
    assert_eq!(cancel.kind, CommandKind::Cancel);
    let out = process(&mut engine, &cancel, 0);
    assert_eq!(out.executions[0].reason, RejectReason::UnknownOrder);
    assert_eq!(engine.book().order_count(), 0);
    assert!(out.md.is_empty());
}

/// Every accepted order's submitted quantity is accounted for exactly once:
/// as a fill, a cancellation remainder, or a resting remainder.
fn assert_quantity_conservation(executions: &[ExecutionReport], book: &Book) {
    let mut submitted: BTreeMap<OrderId, u64> = BTreeMap::new();
    for exec in executions {
        if exec.kind == ExecKind::Accepted {
            submitted.insert(exec.order_id, exec.qty.0);
        }
    }

    let snapshot = book.snapshot();
    let mut resting: BTreeMap<OrderId, u64> = BTreeMap::new();
    for order in &snapshot.orders {
        assert!(
            submitted.contains_key(&order.order_id),
            "resting order {:?} was never accepted",
            order.order_id
        );
        resting.insert(order.order_id, order.remaining.0);
    }

    let mut filled: BTreeMap<OrderId, u64> = BTreeMap::new();
    let mut cancelled: BTreeMap<OrderId, u64> = BTreeMap::new();
    for exec in executions {
        match exec.kind {
            ExecKind::Filled | ExecKind::PartiallyFilled => {
                *filled.entry(exec.order_id).or_default() += exec.qty.0;
                if exec.maker_order_id != OrderId(0) {
                    *filled.entry(exec.maker_order_id).or_default() += exec.qty.0;
                }
            }
            ExecKind::Cancelled => {
                *cancelled.entry(exec.order_id).or_default() += exec.qty.0;
            }
            ExecKind::Accepted | ExecKind::Rejected => {}
        }
    }

    for (order_id, quantity) in submitted {
        let filled = filled.get(&order_id).copied().unwrap_or(0);
        let cancelled = cancelled.get(&order_id).copied().unwrap_or(0);
        let resting = resting.get(&order_id).copied().unwrap_or(0);
        assert_eq!(
            filled + cancelled + resting,
            quantity,
            "order {order_id:?}: {filled} filled + {cancelled} cancelled + {resting} resting != {quantity} submitted"
        );
    }
}

#[test]
fn snapshot_hash_reproduces_live_state_hash() {
    let config = scenario_config();
    let commands = GoldenLog::standard().commands().to_vec();
    // Full replay and a handful of prefixes, including the empty book.
    for split in [0usize, 1, 5, 17, commands.len()] {
        let mut engine = SymbolEngine::new(config.clone());
        let mut out = EngineOutput::with_capacity(16, 16);
        for (offset, command) in commands.iter().enumerate().take(split) {
            engine.process(command, offset as u64, &mut out);
            out.clear();
        }
        let snapshot = engine.snapshot_state_without_hash();
        assert_eq!(
            snapshot.state_hash, [0; 32],
            "split {split}: zeroed sentinel"
        );
        assert_eq!(
            hash_symbol_snapshot(&snapshot),
            engine.state_hash(),
            "split {split}: snapshot hash diverged from live state hash"
        );
    }
}

#[test]
fn shard_snapshot_hashes_fill_equals_live() {
    let exchange = ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Durable,
        instruments: vec![scenario_config()],
    };
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();

    let mut engine = ShardEngine::new(shard, &exchange);
    let mut out = EngineOutput::with_capacity(16, 16);
    for (offset, command) in commands.iter().enumerate() {
        engine.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }

    let hashed = engine.snapshot_state();
    let mut unhashed = engine.snapshot_state_without_hash();
    assert!(
        unhashed
            .symbols
            .iter()
            .all(|symbol| symbol.state_hash == [0; 32]),
        "without-hash snapshot must start zeroed"
    );
    fill_symbol_hashes(&mut unhashed);

    assert_eq!(unhashed.shard_id, hashed.shard_id);
    assert_eq!(unhashed.symbols.len(), hashed.symbols.len());
    for (filled, expected) in unhashed.symbols.iter().zip(hashed.symbols.iter()) {
        assert_eq!(filled.symbol, expected.symbol);
        assert_eq!(filled.state_hash, expected.state_hash);
    }
}

#[test]
fn restored_engine_verifies_hash_computed_off_thread() {
    let config = scenario_config();
    let commands = GoldenLog::standard().commands().to_vec();

    let mut engine = SymbolEngine::new(config.clone());
    let mut out = EngineOutput::with_capacity(16, 16);
    for (offset, command) in commands.iter().enumerate() {
        engine.process(command, offset as u64, &mut out);
        out.clear();
    }

    let mut snapshot = engine.snapshot_state_without_hash();
    snapshot.state_hash = hash_symbol_snapshot(&snapshot);
    let restored = SymbolEngine::from_snapshot(config, snapshot).expect("restore");
    assert_eq!(restored.state_hash(), engine.state_hash());
}
