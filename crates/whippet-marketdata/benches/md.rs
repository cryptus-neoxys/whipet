// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Criterion benchmarks for the market-data warm path.
//!
//! Run with:
//!
//! ```text
//! cargo bench -p whippet-marketdata --bench md
//! ```
//!
//! `hub_lock_contention` measures the single `Mutex<HubState>` under one
//! writer per symbol. The demo runs at 50-500 orders/s; if this benchmark
//! reports millions of events per second, the lock is not the bottleneck and a
//! per-symbol shard can stay a Phase 2 design.

use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{MdEvent, MdKind, Side};
use whippet_marketdata::MdHub;

fn hub() -> Arc<MdHub> {
    MdHub::new(
        ExchangeConfig::default(),
        "bench".into(),
        "memory".into(),
        None,
    )
}

fn symbol(name: &str) -> Symbol {
    Symbol::new(name).expect("static symbol")
}

fn event(
    kind: MdKind,
    symbol: Symbol,
    seq: u64,
    side: Side,
    price_ticks: i64,
    qty: u64,
    order_id: u128,
) -> MdEvent {
    MdEvent {
        kind,
        symbol,
        seq: Seq(seq),
        cause_offset: seq,
        timestamp_ns: seq,
        side,
        flags: 0,
        price: Price(price_ticks),
        qty: Qty(qty),
        order_id: OrderId(order_id),
        participant: ParticipantId(1),
    }
}

/// One million applied deltas: Add, Reduce, Remove, Trade per cycle. The book
/// stays bounded because each order is removed before the next cycle.
fn apply_1m(c: &mut Criterion) {
    let mut group = c.benchmark_group("hub_apply");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1_000_000));
    group.bench_function("1m_mixed_add_reduce_remove_trade", |b| {
        b.iter(|| {
            let hub = hub();
            let btc = symbol("BTC-USD");
            let mut seq = 0u64;
            for order in 0..250_000u128 {
                let price = 65_000 + (order % 1_000) as i64;
                let side = if order % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let id = order + 1;

                seq += 1;
                hub.apply(&event(MdKind::Add, btc, seq, side, price, 100, id));
                seq += 1;
                hub.apply(&event(MdKind::Reduce, btc, seq, side, price, 40, id));
                seq += 1;
                hub.apply(&event(MdKind::Remove, btc, seq, side, price, 0, id));
                seq += 1;
                hub.apply(&event(
                    MdKind::Trade,
                    btc,
                    seq,
                    side.opposite(),
                    price,
                    10,
                    id,
                ));
            }
            black_box(hub);
        });
    });
    group.finish();
}

/// Build the 20 Hz frame shape used by the dashboard: three symbols, 20 levels
/// per side, and a full 250-trade tape.
fn seeded_hub() -> Arc<MdHub> {
    let hub = hub();
    let config = ExchangeConfig::default();
    let mut seq = 0u64;
    let mut order_id = 0u128;
    for instrument in &config.instruments {
        let mid = instrument.reference_price().0;
        for level in 0..20i64 {
            seq += 1;
            order_id += 1;
            hub.apply(&event(
                MdKind::Add,
                instrument.symbol,
                seq,
                Side::Buy,
                mid - 1 - level,
                10 + level as u64,
                order_id,
            ));
            seq += 1;
            order_id += 1;
            hub.apply(&event(
                MdKind::Add,
                instrument.symbol,
                seq,
                Side::Sell,
                mid + 1 + level,
                10 + level as u64,
                order_id,
            ));
        }
    }
    for index in 0..250usize {
        seq += 1;
        let instrument = &config.instruments[index % config.instruments.len()];
        hub.apply(&event(
            MdKind::Trade,
            instrument.symbol,
            seq,
            Side::Buy,
            instrument.reference_price().0,
            1,
            0,
        ));
    }
    hub
}

fn state_view(c: &mut Criterion) {
    let hub = seeded_hub();

    let mut group = c.benchmark_group("hub_state_view");
    group.sample_size(50);
    group.bench_function("3_symbols_20_levels_250_trades", |b| {
        b.iter(|| black_box(hub.state()));
    });
    group.finish();
}

/// The same 1M-event mixed stream as `hub_apply`, applied 256 events at a
/// time. One hub-lock acquisition and one clock read per batch.
fn apply_batch_1m(c: &mut Criterion) {
    const BATCH: usize = 256;
    let mut group = c.benchmark_group("hub_apply_batch");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1_000_000));
    group.bench_function("1m_mixed_batches_of_256", |b| {
        b.iter(|| {
            let hub = hub();
            let btc = symbol("BTC-USD");
            let mut batch: Vec<MdEvent> = Vec::with_capacity(BATCH);
            let mut seq = 0u64;
            for order in 0..250_000u128 {
                let price = 65_000 + (order % 1_000) as i64;
                let side = if order % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let id = order + 1;

                seq += 1;
                batch.push(event(MdKind::Add, btc, seq, side, price, 100, id));
                seq += 1;
                batch.push(event(MdKind::Reduce, btc, seq, side, price, 40, id));
                seq += 1;
                batch.push(event(MdKind::Remove, btc, seq, side, price, 0, id));
                seq += 1;
                batch.push(event(
                    MdKind::Trade,
                    btc,
                    seq,
                    side.opposite(),
                    price,
                    10,
                    id,
                ));
                if batch.len() >= BATCH {
                    hub.apply_batch(&batch);
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                hub.apply_batch(&batch);
            }
            black_box(hub);
        });
    });
    group.finish();
}

/// Per-subscription frame: one selected symbol and the newest 60 trades from a
/// three-symbol, 250-trade published frame.
fn latest_for(c: &mut Criterion) {
    let hub = seeded_hub();
    hub.publish();
    let selected = [symbol("BTC-USD")];

    let mut group = c.benchmark_group("hub_latest_for");
    group.sample_size(100);
    group.bench_function("3_symbols_1_selected_60_trades", |b| {
        b.iter(|| black_box(hub.latest_for(&selected, 60)));
    });
    group.finish();
}

/// At-least-once replay: the sequence check must reject duplicates and stale
/// records before touching the book.
fn duplicate_stale(c: &mut Criterion) {
    let btc = symbol("BTC-USD");
    let add = event(MdKind::Add, btc, 1, Side::Buy, 65_000, 10, 1);

    let duplicate_hub = hub();
    duplicate_hub.apply(&add);

    let stale_hub = hub();
    let newer = event(MdKind::Add, btc, 2, Side::Buy, 65_001, 10, 2);
    stale_hub.apply(&newer);

    let mut group = c.benchmark_group("hub_duplicate_stale");
    group.sample_size(10);
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("duplicate_add", |b| {
        b.iter(|| {
            for _ in 0..100_000 {
                duplicate_hub.apply(black_box(&add));
            }
        });
    });
    group.bench_function("stale_replay", |b| {
        b.iter(|| {
            for _ in 0..100_000 {
                stale_hub.apply(black_box(&add));
            }
        });
    });
    group.finish();
}

/// Read paths used by `/api/state`, `/api/trades`, and `/metrics`: an `Arc`
/// clone of the latest frame, and a metrics-only snapshot that never touches
/// the book.
fn read_path(c: &mut Criterion) {
    let hub = hub();
    hub.publish();

    let mut group = c.benchmark_group("hub_read_path");
    group.sample_size(100);
    group.bench_function("latest_arc_clone", |b| {
        b.iter(|| black_box(hub.latest()));
    });
    group.bench_function("metrics_snapshot", |b| {
        b.iter(|| black_box(hub.metrics_snapshot()));
    });
    group.finish();
}

/// Contention probe: one writer per symbol, all serialized by the single hub
/// mutex. Compare the reported throughput with the 50-500 orders/s target.
fn contended_apply(c: &mut Criterion) {
    const CYCLES_PER_THREAD: u64 = 100_000;
    const EVENTS_PER_THREAD: u64 = CYCLES_PER_THREAD * 4;

    let mut group = c.benchmark_group("hub_lock_contention");
    group.sample_size(10);
    group.throughput(Throughput::Elements(EVENTS_PER_THREAD * 3));
    group.bench_function("3_symbols_x_100k_cycles", |b| {
        b.iter(|| {
            let hub = hub();
            let config = ExchangeConfig::default();
            thread::scope(|scope| {
                for instrument in &config.instruments {
                    let hub = Arc::clone(&hub);
                    let symbol = instrument.symbol;
                    let mid = instrument.reference_price().0;
                    scope.spawn(move || {
                        for cycle in 0..CYCLES_PER_THREAD {
                            let price = mid + (cycle % 20) as i64;
                            let side = if cycle % 2 == 0 {
                                Side::Buy
                            } else {
                                Side::Sell
                            };
                            let id = u128::from(cycle) + 1;
                            let seq = cycle * 4;
                            hub.apply(&event(MdKind::Add, symbol, seq + 1, side, price, 100, id));
                            hub.apply(&event(MdKind::Reduce, symbol, seq + 2, side, price, 40, id));
                            hub.apply(&event(MdKind::Remove, symbol, seq + 3, side, price, 0, id));
                            hub.apply(&event(
                                MdKind::Trade,
                                symbol,
                                seq + 4,
                                side.opposite(),
                                price,
                                10,
                                id,
                            ));
                        }
                    });
                }
            });
            black_box(hub);
        });
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(8));
    targets = apply_1m, apply_batch_1m, state_view, latest_for, duplicate_stale, read_path, contended_apply
}
criterion_main!(benches);
