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
use whippet_engine::{Book, EngineOutput, SymbolEngine};

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

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5));
    targets = bench_insert_cancel, bench_best_price, bench_aggressive_burst, bench_snapshot_hash, bench_engine_state_hash
}
criterion_main!(benches);
