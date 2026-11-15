// Licensed under the Apache License, Version 2.0. See LICENSE.

//! The off-loop snapshot hash must reproduce the live engine hash at scale.
//!
//! The matcher can hand the snapshot writer an unhashed snapshot so the hash
//! stays off the match thread, but only if `hash_symbol_snapshot` is
//! byte-identical to `SymbolEngine::state_hash` for every state the engine can
//! reach. That was not true once: a run at ~170k orders disagreed with the
//! restored engine hash. These tests are the proof gate before the matcher is
//! allowed to stop hashing on the shard thread.
//!
//! The workload is sized to reach the states a short test never sees:
//!
//! - 200k commands per engine, so the dedup window (capacity 8,192 here)
//!   evicts and later re-admits the same order id,
//! - cancels, IOC, FOK, market and post-only orders, so slots are unlinked and
//!   the arena free list is reused,
//! - a ladder spanning both sides of 64-bit bitmap words and books deeper than
//!   64 levels, so level grouping cannot rely on a single word.

use whippet_core::config::{ExchangeConfig, InstrumentConfig, Mode};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_engine::{
    EngineOutput, ShardEngine, SymbolEngine, fill_symbol_hashes, hash_symbol_snapshot,
};

/// Commands per generated engine. Long enough that the dedup table evicts a
/// cancelled order and accepts the same id again after the window passes.
const COMMANDS: u64 = 200_000;

/// Order ids are drawn from a pool around twice the dedup capacity, so ids
/// leave the window and come back.
const ID_POOL: u64 = 16_384;

/// A tiny deterministic PRNG (SplitMix64). The test needs reproducible random
/// engines, not statistical quality, and this keeps the dependency set as is.
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

    fn pick<T: Copy>(&mut self, values: &[T]) -> T {
        values[self.below(values.len() as u64) as usize]
    }
}

fn config() -> InstrumentConfig {
    // A ladder that crosses several 64-bit bitmap words. The book reaches more
    // than 64 occupied levels on a side, so a single-word assumption in the
    // hash would show up.
    let mut config = InstrumentConfig::new("BTC-USD", 1, 1_000, 101_000).expect("static config");
    config.max_orders = 4_096;
    config.snapshot_depth = 20;
    config
}

fn symbol() -> Symbol {
    config().symbol
}

/// Ladder offsets that sit on and around 64-bit word boundaries. A bug that
/// only appears at index 0/63/64/65 of the occupancy bitmap cannot hide.
fn boundary_offsets() -> Vec<i64> {
    vec![0, 1, 62, 63, 64, 65, 126, 127, 128, 129, 190, 191, 192, 193]
}

fn price_for(rng: &mut Rng) -> Price {
    config();
    let base = 1_000;
    let offset = if rng.below(2) == 0 {
        rng.pick(&boundary_offsets())
    } else {
        // Wide enough for a deep book: up to 300 levels per side.
        rng.below(300) as i64
    };
    Price(base + offset)
}

fn generate(seed: u64) -> Vec<Command> {
    let mut rng = Rng::new(seed);
    let mut commands = Vec::with_capacity(COMMANDS as usize);
    for index in 0..COMMANDS {
        let timestamp_ns = index;
        let order_id = OrderId(u128::from(rng.below(ID_POOL)) + 1);
        let participant = ParticipantId(rng.below(4) + 1);
        let roll = rng.below(100);
        let command = match roll {
            // Resting and aggressively priced limits: crosses rest as fills and
            // the remainder joins the book.
            0..=54 => Command::new_order(
                symbol(),
                if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                if rng.below(7) == 0 {
                    OrderType::PostOnly
                } else {
                    OrderType::Limit
                },
                price_for(&mut rng),
                Qty(rng.below(40) + 1),
                order_id,
                participant,
                timestamp_ns,
            ),
            // Cancels: hit resting orders, already-gone orders and ids that
            // left the dedup window.
            55..=69 => Command::cancel(symbol(), order_id, participant, timestamp_ns),
            // IOC keeps no remainder and cancels the rest.
            70..=82 => Command::new_order(
                symbol(),
                if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                OrderType::Ioc,
                price_for(&mut rng),
                Qty(rng.below(40) + 1),
                order_id,
                participant,
                timestamp_ns,
            ),
            // Market orders trade against the book or cancel out of collar.
            83..=91 => Command::new_order(
                symbol(),
                if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                OrderType::Market,
                Price::ZERO,
                Qty(rng.below(60) + 1),
                order_id,
                participant,
                timestamp_ns,
            ),
            // FOK rejects unless the whole quantity is executable.
            _ => Command::new_order(
                symbol(),
                if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                OrderType::Fok,
                price_for(&mut rng),
                Qty(rng.below(40) + 1),
                order_id,
                participant,
                timestamp_ns,
            ),
        };
        commands.push(command);
    }
    commands
}

fn run(seed: u64) -> SymbolEngine {
    let mut engine = SymbolEngine::new(config());
    let mut out = EngineOutput::with_capacity(32, 32);
    for (offset, command) in generate(seed).iter().enumerate() {
        engine.process(command, offset as u64, &mut out);
        out.clear();
    }
    engine
        .check_invariants()
        .expect("the 200k-command workload leaves a consistent engine");
    engine
}

#[test]
fn off_loop_hash_matches_100_random_engines_at_scale() {
    for seed in 0..100u64 {
        let engine = run(seed);
        let live = engine.state_hash();
        let snapshot = engine.snapshot_state_without_hash();
        let off_loop = hash_symbol_snapshot(&snapshot);

        assert_eq!(
            off_loop,
            live,
            "seed {seed}: off-loop hash {} != live hash {} (commands {}, book orders {}, dedup {} entries in {} map slots)",
            hex(&off_loop),
            hex(&live),
            engine.commands(),
            engine.book().order_count(),
            snapshot.dedup.len(),
            engine.dedup().len(),
        );
    }
}

#[test]
fn the_workload_reaches_eviction_arena_reuse_and_deep_books() {
    // If a future change makes the workload degenerate, the hash test above
    // proves nothing. Pin the properties it is supposed to reach.
    let engine = run(0);
    assert_eq!(engine.commands(), COMMANDS);
    assert!(
        engine.dedup().len() < COMMANDS as usize,
        "the workload never left the dedup window"
    );
    assert!(
        engine.book().order_count() > 64,
        "the book never grew past 64 resting orders ({} found)",
        engine.book().order_count()
    );
    let snapshot = engine.book().snapshot();
    let mut levels = std::collections::HashSet::new();
    for order in &snapshot.orders {
        levels.insert((order.side, order.price));
    }
    assert!(
        levels.len() > 16,
        "the book never spanned a real ladder ({} levels)",
        levels.len()
    );
    let lowest = snapshot.orders.iter().map(|o| o.price.0).min().unwrap();
    let highest = snapshot.orders.iter().map(|o| o.price.0).max().unwrap();
    assert!(
        highest - lowest > 64,
        "the occupied ladder never crossed a 64-bit bitmap word (span {})",
        highest - lowest
    );
    // Arena reuse after cancels: ids are drawn from a pool far smaller than the
    // command count, so the same ids are cancelled and re-admitted.
    let deepest = snapshot
        .orders
        .iter()
        .map(|order| order.order_id.0)
        .max()
        .unwrap_or(0);
    assert!(deepest < u128::from(COMMANDS), "no id reuse in the pool");
}

/// Hashing the snapshot must match the live book exactly at the 64-bit bitmap
/// word boundary and on a ladder deep enough to cross several words.
#[test]
fn book_hash_agrees_across_bitmap_words_and_a_deep_ladder() {
    let mut engine = SymbolEngine::new(config());
    let mut out = EngineOutput::with_capacity(8, 8);
    let mut id = 0u128;
    // Ladder index 0..=130 for bids, 4_000..=4_130 for asks: index 63 ends a
    // bitmap word and 64 starts the next, and the book is 131 levels deep.
    for offset in 0..=130i64 {
        for (side, price) in [(Side::Buy, 1_000 + offset), (Side::Sell, 5_000 + offset)] {
            id += 1;
            let command = Command::new_order(
                symbol(),
                side,
                OrderType::Limit,
                Price(price),
                Qty(u64::try_from(offset).unwrap() + 1),
                OrderId(id),
                ParticipantId(1),
                offset as u64,
            );
            engine.process(&command, offset as u64, &mut out);
            out.clear();
        }
    }
    assert_eq!(engine.book().order_count(), 262);
    let live = engine.state_hash();
    assert_eq!(
        hash_symbol_snapshot(&engine.snapshot_state_without_hash()),
        live
    );
    // And the restored engine agrees, so the bytes are not just self-consistent.
    let mut snapshot = engine.snapshot_state_without_hash();
    snapshot.state_hash = hash_symbol_snapshot(&snapshot);
    let restored = SymbolEngine::from_snapshot(config(), snapshot).expect("restores");
    assert_eq!(restored.state_hash(), live);
}

#[test]
fn a_snapshot_without_hash_restores_to_the_live_hash_at_scale() {
    let engine = run(7);
    let mut snapshot = engine.snapshot_state_without_hash();
    assert_eq!(snapshot.state_hash, [0u8; 32]);
    snapshot.state_hash = hash_symbol_snapshot(&snapshot);
    assert_ne!(snapshot.state_hash, [0u8; 32]);
    let restored =
        SymbolEngine::from_snapshot(config(), snapshot).expect("the filled hashes restore");
    assert_eq!(restored.state_hash(), engine.state_hash());
    assert_eq!(restored.book().best_bid(), engine.book().best_bid());
    assert_eq!(restored.book().best_ask(), engine.book().best_ask());
    assert_eq!(restored.book().order_count(), engine.book().order_count());
}

#[test]
fn the_compact_terminal_queue_restores_eviction_parity_at_scale() {
    // The snapshot carries the terminal queue as `u32` indices into the dedup
    // walk rather than as order ids. At this scale the walk is longer than the
    // map (ids were evicted and re-admitted), which is exactly where a wrong
    // index, or resolution in the wrong order, would change which id the
    // restored table evicts next.
    let mut engine = run(3);
    let live_queue = engine.dedup().terminal_order();
    assert!(!live_queue.is_empty(), "the workload never terminalised");
    let walk = engine.dedup().entries_in_order();
    assert!(
        walk.len() > engine.dedup().len(),
        "the workload never re-admitted an evicted id (walk {}, map {})",
        walk.len(),
        engine.dedup().len()
    );
    drop(walk);

    let mut snapshot = engine.snapshot_state_without_hash_compacted();
    assert_eq!(snapshot.dedup_terminal_idx.len(), live_queue.len());
    assert!(
        snapshot.dedup_terminal_idx.len() * 4 < live_queue.len() * 40,
        "the compact form should be far smaller than the ids it replaces"
    );
    snapshot.state_hash = hash_symbol_snapshot(&snapshot);
    let mut restored = SymbolEngine::from_snapshot(config(), snapshot).expect("restores");
    assert_eq!(restored.state_hash(), engine.state_hash());
    assert_eq!(
        restored.dedup().terminal_order(),
        live_queue,
        "the restored eviction queue diverged"
    );
    // Drive both engines through further evictions with the same commands and
    // compare what is evicted. The snapshot is taken from the compacted table,
    // so the restored insertion walk is identical to the live one and the
    // post-tail state hash must match: a dead id re-admitted by the tail is
    // the case that used to diverge.
    let tail = generate(1_003);
    let base = engine.commands();
    let mut out = EngineOutput::with_capacity(32, 32);
    for (index, command) in tail.iter().take(20_000).enumerate() {
        let offset = base + index as u64;
        engine.process(command, offset, &mut out);
        out.clear();
        restored.process(command, offset, &mut out);
        out.clear();
    }
    assert_eq!(restored.dedup().len(), engine.dedup().len());
    for id in 1..=ID_POOL {
        let id = OrderId(u128::from(id));
        assert_eq!(
            restored.dedup().contains(id),
            engine.dedup().contains(id),
            "id {id} membership diverged after the tail"
        );
    }
    assert_eq!(
        restored.dedup().terminal_order(),
        engine.dedup().terminal_order(),
        "the restored eviction queue diverged after the tail"
    );
    assert_eq!(
        restored.state_hash(),
        engine.state_hash(),
        "the restored state hash diverged after the tail"
    );
}

#[test]
fn shard_snapshot_without_hash_matches_the_shard_hash() {
    let mut eth = config();
    eth.symbol = Symbol::new("ETH-USD").expect("static symbol");
    let exchange = ExchangeConfig {
        venue: "test".into(),
        partitions: 1,
        mode: Mode::Durable,
        instruments: vec![config(), eth],
    };
    let shard = symbol().partition(exchange.partitions);
    let mut shard_engine = ShardEngine::new(shard, &exchange);
    let mut out = EngineOutput::with_capacity(32, 32);
    let btc = generate(11);
    for (offset, command) in btc.iter().enumerate() {
        shard_engine
            .process(command, offset as u64, &mut out)
            .unwrap();
        out.clear();
    }

    let live = shard_engine.state_hash();
    let mut snapshot = shard_engine.snapshot_state_without_hash();
    fill_symbol_hashes(&mut snapshot);
    for symbol_snapshot in &snapshot.symbols {
        let live_symbol = shard_engine
            .engine(symbol_snapshot.symbol)
            .expect("snapshot symbol is live")
            .state_hash();
        assert_eq!(
            symbol_snapshot.state_hash, live_symbol,
            "symbol {} off-loop hash diverged",
            symbol_snapshot.symbol
        );
    }
    let restored =
        ShardEngine::from_snapshot(&exchange, snapshot).expect("the filled hashes restore");
    assert_eq!(restored.state_hash(), live);
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
