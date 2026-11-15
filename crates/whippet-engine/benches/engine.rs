// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Criterion benchmarks for the deterministic matcher hot path.
//!
//! These are developer tools, not CI gates. Run them with:
//!
//! ```text
//! cargo bench -p whippet-engine --bench engine
//! ```
//!
//! Every measured routine operates on preallocated storage so the numbers
//! reflect the match loop rather than allocator noise.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use whippet_core::config::InstrumentConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_engine::{Book, EngineOutput, SymbolEngine, hash_symbol_snapshot};

/// Total insert+cancel operations per measured iteration.
const INSERT_CANCEL_OPS: u64 = 1_000_000;
/// Number of aggressive orders in the burst benchmark.
const BURST_OPS: u64 = 10_000;

fn book_config(max_orders: usize) -> InstrumentConfig {
    let mut config = InstrumentConfig::new("BTC-USD", 100, 90, 10_000).expect("static config");
    config.max_orders = max_orders;
    config.snapshot_depth = 20;
    config
}

/// A two-sided book with `levels` occupied price levels per side and
/// `per_level` resting orders at each level.
fn deep_book(levels: usize, per_level: usize) -> Book {
    let config = book_config(levels * per_level * 2 + 8);
    let mut book = Book::new(&config);
    let mut id = 0u128;
    for level in 0..levels {
        for _ in 0..per_level {
            id += 1;
            book.insert(
                OrderId(id),
                ParticipantId(1),
                Side::Buy,
                Price(90 + level as i64),
                Qty(100),
                id as u64,
            )
            .expect("bid insert");
            id += 1;
            book.insert(
                OrderId(id),
                ParticipantId(2),
                Side::Sell,
                Price(200 + level as i64),
                Qty(100),
                id as u64,
            )
            .expect("ask insert");
        }
    }
    book
}

/// One million alternating limit inserts and cancels across a deep ladder.
fn bench_insert_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("book_insert_cancel");
    group.throughput(Throughput::Elements(INSERT_CANCEL_OPS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("1m_ops", |b| {
        b.iter_batched(
            || {
                let mut book = Book::new(&book_config(8_192));
                // Warm the arena and index with two thousand resting orders so
                // the measured loop only touches preallocated storage.
                let mut id = 0u128;
                for level in 0..64i64 {
                    for _ in 0..16 {
                        id += 1;
                        book.insert(
                            OrderId(id),
                            ParticipantId(1),
                            Side::Buy,
                            Price(90 + level),
                            Qty(10),
                            id as u64,
                        )
                        .expect("warm bid");
                        id += 1;
                        book.insert(
                            OrderId(id),
                            ParticipantId(2),
                            Side::Sell,
                            Price(200 + level),
                            Qty(10),
                            id as u64,
                        )
                        .expect("warm ask");
                    }
                }
                book
            },
            |mut book| {
                let mut id = 10_000_000u128;
                for step in 0..(INSERT_CANCEL_OPS / 2) {
                    id += 1;
                    let price = Price(90 + (step % 128) as i64);
                    book.insert(
                        OrderId(id),
                        ParticipantId(1),
                        Side::Buy,
                        price,
                        Qty(1),
                        step,
                    )
                    .expect("insert");
                    book.cancel(OrderId(id), None).expect("cancel");
                    black_box(step);
                }
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Best bid and ask on a deep book, plus the lookup immediately after the
/// cached best level is removed.
fn bench_best_price(c: &mut Criterion) {
    let book = deep_book(2_000, 4);
    let mut group = c.benchmark_group("best_price_lookup");
    group.bench_function("deep_book_bid_ask", |b| {
        b.iter(|| {
            black_box(book.best_bid());
            black_box(book.best_ask());
        });
    });
    group.bench_function("after_best_cancelled", |b| {
        b.iter_batched(
            || {
                let mut book = deep_book(2_000, 4);
                // Remove the current best bid so the next lookup must fall
                // back to the bitmap or an updated cache.
                let best = book.best_bid().expect("best bid");
                let snapshot = book.snapshot();
                let victim = snapshot
                    .orders
                    .iter()
                    .find(|order| order.side == Side::Buy && order.price == best)
                    .expect("order at best bid");
                book.cancel(victim.order_id, None).expect("cancel best");
                book
            },
            |book| {
                black_box(book.best_bid());
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// A burst of aggressive IOC orders sweeping a deep resting ask ladder.
fn bench_aggressive_burst(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggressive_burst");
    group.throughput(Throughput::Elements(BURST_OPS));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("sweep_resting_asks", |b| {
        b.iter_batched(
            || {
                let mut engine = SymbolEngine::new(book_config(64_000));
                let symbol = engine.config().symbol;
                let mut out = EngineOutput::with_capacity(64, 64);
                let mut id = 0u128;
                for level in 0..200i64 {
                    for _ in 0..100 {
                        id += 1;
                        let command = Command::new_order(
                            symbol,
                            Side::Sell,
                            OrderType::Limit,
                            Price(200 + level),
                            Qty(10),
                            OrderId(id),
                            ParticipantId(1),
                            id as u64,
                        );
                        engine.process(&command, id as u64, &mut out);
                        out.clear();
                    }
                }
                (engine, out, symbol, id)
            },
            |(mut engine, mut out, symbol, mut id)| {
                for _ in 0..BURST_OPS {
                    id += 1;
                    let command = Command::new_order(
                        symbol,
                        Side::Buy,
                        OrderType::Ioc,
                        Price(5_000),
                        Qty(10),
                        OrderId(id),
                        ParticipantId(2),
                        id as u64,
                    );
                    engine.process(&command, id as u64, &mut out);
                    out.clear();
                }
                black_box(engine.book().best_ask());
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Fill-or-kill orders against a 50k-order ask ladder. Each FOK needs 10% of
/// the resting quantity, so the liquidity check has to prove fillability
/// without summing the whole crossing side of the book.
fn bench_fok_burst(c: &mut Criterion) {
    /// Resting orders in the ladder: 500 levels of 100 orders.
    const BOOK_ORDERS: u64 = 50_000;
    const LEVELS: i64 = 500;
    const PER_LEVEL: u64 = 100;
    const ORDER_QTY: u64 = 10;
    /// FOK orders submitted per measured iteration. Ten of them consume the
    /// ladder exactly, so every one is fillable.
    const FOK_OPS: u64 = 10;
    /// Ten percent of the ladder's total resting quantity.
    const FOK_QTY: u64 = LEVELS as u64 * PER_LEVEL * ORDER_QTY / 10;

    fn ladder() -> Book {
        let mut book = Book::new(&book_config(BOOK_ORDERS as usize + 8));
        let mut id = 0u128;
        for level in 0..LEVELS {
            for _ in 0..PER_LEVEL {
                id += 1;
                book.insert(
                    OrderId(id),
                    ParticipantId(1),
                    Side::Sell,
                    Price(200 + level),
                    Qty(ORDER_QTY),
                    id as u64,
                )
                .expect("resting ask");
            }
        }
        book
    }

    let book = ladder();
    assert_eq!(book.order_count(), BOOK_ORDERS as usize);

    let mut group = c.benchmark_group("fok_burst");

    // The check in isolation: same question, old and new form, on the same
    // immutable book. This is the pair the early exit replaces.
    group.bench_function("check_only_full_walk", |b| {
        b.iter(|| {
            black_box(
                book.executable_qty_excluding(Side::Sell, Price(600), ParticipantId(2)) >= FOK_QTY,
            );
        });
    });
    group.bench_function("check_only_early_exit", |b| {
        b.iter(|| {
            black_box(book.has_executable_qty_excluding(
                Side::Sell,
                Price(600),
                ParticipantId(2),
                Qty(FOK_QTY),
            ));
        });
    });

    // End to end through the engine, including matching the 10% it consumes.
    group.throughput(Throughput::Elements(FOK_OPS));
    group.sample_size(10);
    group.bench_function("50k_book_10pct", |b| {
        b.iter_batched(
            || {
                let mut engine = SymbolEngine::new(book_config(BOOK_ORDERS as usize + 8));
                let symbol = engine.config().symbol;
                let mut out = EngineOutput::with_capacity(256, 256);
                let mut id = 0u128;
                for level in 0..LEVELS {
                    for _ in 0..PER_LEVEL {
                        id += 1;
                        let command = Command::new_order(
                            symbol,
                            Side::Sell,
                            OrderType::Limit,
                            Price(200 + level),
                            Qty(ORDER_QTY),
                            OrderId(id),
                            ParticipantId(1),
                            id as u64,
                        );
                        engine.process(&command, id as u64, &mut out);
                        out.clear();
                    }
                }
                (engine, out, symbol, id)
            },
            |(mut engine, mut out, symbol, mut id)| {
                for _ in 0..FOK_OPS {
                    id += 1;
                    let command = Command::new_order(
                        symbol,
                        Side::Buy,
                        OrderType::Fok,
                        Price(5_000),
                        Qty(FOK_QTY),
                        OrderId(id),
                        ParticipantId(2),
                        id as u64,
                    );
                    engine.process(&command, id as u64, &mut out);
                    out.clear();
                }
                black_box(engine.book().best_ask());
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// Snapshot construction and the deterministic state hash.
fn bench_snapshot_hash(c: &mut Criterion) {
    let book = deep_book(1_000, 4);
    let mut group = c.benchmark_group("snapshot_and_hash");
    group.sample_size(20);
    group.bench_function("book_snapshot", |b| {
        b.iter(|| black_box(book.snapshot()));
    });
    group.bench_function("book_state_hash", |b| {
        b.iter(|| {
            let mut hasher = blake3::Hasher::new();
            book.hash_into(&mut hasher);
            black_box(*hasher.finalize().as_bytes());
        });
    });
    group.bench_function("snapshot_levels_20", |b| {
        b.iter(|| black_box(book.snapshot_levels(20)));
    });
    group.finish();
}

/// Full engine state hash with both a populated book and a populated dedup
/// table, exercising the allocation-free dedup walk.
fn bench_engine_state_hash(c: &mut Criterion) {
    let mut engine = SymbolEngine::new(book_config(64_000));
    let symbol = engine.config().symbol;
    let mut out = EngineOutput::with_capacity(64, 64);
    let mut id = 0u128;
    for level in 0..200i64 {
        for _ in 0..100 {
            id += 1;
            let command = Command::new_order(
                symbol,
                Side::Buy,
                OrderType::Limit,
                Price(90 + level),
                Qty(10),
                OrderId(id),
                ParticipantId(1),
                id as u64,
            );
            engine.process(&command, id as u64, &mut out);
            out.clear();
        }
    }

    let mut group = c.benchmark_group("engine_state_hash");
    group.sample_size(20);
    group.bench_function("20k_orders_20k_dedup", |b| {
        b.iter(|| black_box(engine.state_hash()));
    });
    group.finish();
}

/// The off-shard hash path: materialize the snapshot once, then measure only
/// `hash_symbol_snapshot` on it. This is what the snapshot writer does after the
/// shard thread stops stalling on `snapshot_state`.
fn bench_snapshot_hash_off_thread(c: &mut Criterion) {
    let mut engine = SymbolEngine::new(book_config(64_000));
    let symbol = engine.config().symbol;
    let mut out = EngineOutput::with_capacity(64, 64);
    let mut id = 0u128;
    for level in 0..200i64 {
        for _ in 0..100 {
            id += 1;
            let command = Command::new_order(
                symbol,
                Side::Buy,
                OrderType::Limit,
                Price(90 + level),
                Qty(10),
                OrderId(id),
                ParticipantId(1),
                id as u64,
            );
            engine.process(&command, id as u64, &mut out);
            out.clear();
        }
    }
    let snapshot = engine.snapshot_state_without_hash();

    let mut group = c.benchmark_group("snapshot_hash");
    group.sample_size(20);
    group.bench_function("from_snapshot_20k_orders_20k_dedup", |b| {
        b.iter(|| black_box(hash_symbol_snapshot(&snapshot)));
    });
    group.finish();
}

/// The periodic snapshot, measured stage by stage at soak size.
///
/// The 60 s soak at 5,000 orders/s on shard 1 (~170k commands) showed a p50 of
/// ~1.1 ms against a p99 of ~0.5 s, and the spike lined up with snapshots. That
/// number was one lump; these stages are what it is made of:
///
/// - `matcher_clone_with_hash`: the old shard-thread cost (live hash + clone),
/// - `matcher_clone_only`: what the shard thread pays now (clone only),
/// - `writer_hash`: the off-loop `hash_symbol_snapshot`,
/// - `writer_serialize`: the snapshot bytes, before fsync,
/// - and, from the recovery test that writes a real file,
///   `crates/whippet/tests/recovery.rs::off_loop_snapshot_stage_split_at_scale`,
///   which also covers fsync + rename (the part a bench in this crate cannot
///   reach).
///
/// The engine here is sized to a soak: dedup at its 100k window cap and a book
/// deep enough that the clone is not trivially small.
fn bench_snapshot_stages(c: &mut Criterion) {
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }
    }

    /// Commands applied before the measurement: a 60 s soak at 5k/s on one
    /// shard is ~300k, and 170k is where the old off-loop hash diverged.
    const COMMANDS: u64 = 170_000;
    /// Ids are drawn from a pool larger than the dedup window (100k for this
    /// config), so the window turns over during the run.
    const ID_POOL: u64 = 120_000;

    let mut config = InstrumentConfig::new("BTC-USD", 100, 40_000, 90_000).expect("static config");
    config.max_orders = 250_000;
    config.snapshot_depth = 20;
    let mut engine = SymbolEngine::new(config);
    let symbol = engine.config().symbol;
    let mut out = EngineOutput::with_capacity(64, 64);
    let mut rng = Rng(0x5ca1_ab1e_5eed_0001);
    for offset in 0..COMMANDS {
        let order_id = OrderId(u128::from(rng.next() % ID_POOL) + 1);
        let command = if rng.next() % 100 < 60 {
            Command::new_order(
                symbol,
                if rng.next().is_multiple_of(2) {
                    Side::Buy
                } else {
                    Side::Sell
                },
                if rng.next().is_multiple_of(3) {
                    OrderType::Ioc
                } else {
                    OrderType::Limit
                },
                Price(40_000 + (rng.next() % 500) as i64 * 100),
                Qty(rng.next() % 20 + 1),
                order_id,
                ParticipantId(rng.next() % 4 + 1),
                offset,
            )
        } else {
            Command::cancel(symbol, order_id, ParticipantId(rng.next() % 4 + 1), offset)
        };
        engine.process(&command, offset, &mut out);
        out.clear();
    }
    let snapshot = engine.snapshot_state_without_hash();
    let book_orders = snapshot.book.orders.len();
    let dedup_entries = snapshot.dedup.len();
    let bytes = bincode::serialize(&snapshot)
        .expect("snapshot serializes")
        .len();
    eprintln!(
        "snapshot_stages: commands={COMMANDS} book_orders={book_orders} \
         dedup_walk={dedup_entries} dedup_map={} bytes={bytes}",
        engine.dedup().len()
    );

    let mut group = c.benchmark_group("snapshot_stages");
    group.sample_size(10);
    group.bench_function("matcher_clone_with_hash", |b| {
        b.iter(|| black_box(engine.snapshot_state()));
    });
    group.bench_function("matcher_clone_only", |b| {
        b.iter(|| black_box(engine.snapshot_state_without_hash()));
    });
    group.bench_function("writer_hash", |b| {
        b.iter(|| black_box(hash_symbol_snapshot(&snapshot)));
    });
    // Serializer cost into a reused buffer. The writer streams the same bytes
    // through a `BufWriter` into the temp file; fsync is only reachable from
    // the recovery test named above.
    let mut buffer = Vec::with_capacity(bytes);
    group.bench_function("writer_serialize", |b| {
        b.iter(|| {
            buffer.clear();
            bincode::serialize_into(&mut buffer, &snapshot).expect("snapshot serializes");
            black_box(buffer.len())
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5));
    targets = bench_insert_cancel, bench_best_price, bench_aggressive_burst, bench_fok_burst, bench_snapshot_hash, bench_engine_state_hash, bench_snapshot_hash_off_thread, bench_snapshot_stages
}
criterion_main!(benches);
