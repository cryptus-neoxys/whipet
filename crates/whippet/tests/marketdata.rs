// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Market-data hub integration tests.
//!
//! The hub consumes an at-least-once log. A crash between apply and commit
//! replays the batch, so duplicate deltas must be idempotent. The hub uses the
//! per-symbol `MdEvent::seq` to drop anything at or below the last applied
//! sequence once it has seen a real event.

use whippet_core::api::BookView;
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{MdEvent, MdKind, MdSnapshot, Side};
use whippet_marketdata::MdHub;

fn btc() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

fn event(
    kind: MdKind,
    side: Side,
    price_ticks: i64,
    qty: u64,
    order_id: u128,
    seq: u64,
) -> MdEvent {
    MdEvent {
        kind,
        symbol: btc(),
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

/// The producer's startup image: an empty book at sequence zero. The hub
/// refuses book-state deltas until it has seen one, because a delta cannot
/// establish depth by itself.
fn seed(hub: &MdHub) {
    hub.apply_snapshot(&MdSnapshot {
        symbol: btc(),
        seq: Seq(0),
        cause_offset: 0,
        timestamp_ns: 0,
        last_trade_price: Price(0),
        last_trade_qty: Qty(0),
        bids: Vec::new(),
        asks: Vec::new(),
        orders: Vec::new(),
        flags: 0,
    });
}

fn book(hub: &MdHub) -> BookView {
    hub.state()
        .symbols
        .into_iter()
        .find(|book| book.symbol == btc())
        .expect("BTC-USD book")
}

#[test]
fn duplicate_md_delta_is_idempotent_and_later_seq_applies() {
    let hub = MdHub::new(
        ExchangeConfig::default(),
        "test".into(),
        "memory".into(),
        None,
    );

    seed(&hub);
    let first = event(MdKind::Add, Side::Buy, 65_000, 10, 1, 1);
    hub.apply(&first);
    hub.apply(&first);
    let state = book(&hub);
    assert_eq!(state.seq, Seq(1));
    assert_eq!(state.bids[0].qty, 10, "duplicate add must not double count");
    assert_eq!(state.bids[0].orders, 1);

    // A strictly later sequence is applied.
    hub.apply(&event(MdKind::Add, Side::Buy, 65_000, 5, 2, 2));
    let state = book(&hub);
    assert_eq!(state.seq, Seq(2));
    assert_eq!(state.bids[0].qty, 15);
    assert_eq!(state.bids[0].orders, 2);

    // A stale reduce is ignored, a fresh one is applied.
    hub.apply(&event(MdKind::Reduce, Side::Buy, 65_000, 4, 1, 1));
    assert_eq!(book(&hub).bids[0].qty, 15);
    hub.apply(&event(MdKind::Reduce, Side::Buy, 65_000, 4, 1, 3));
    let state = book(&hub);
    assert_eq!(state.seq, Seq(3));
    assert_eq!(state.bids[0].qty, 9, "15 - (10 - 4)");
    assert_eq!(state.bids[0].orders, 2);

    // Replaying the same reduce after commit must be a no-op.
    hub.apply(&event(MdKind::Reduce, Side::Buy, 65_000, 4, 1, 3));
    assert_eq!(book(&hub).bids[0].qty, 9);

    // Remove applies once, then the duplicate is ignored.
    hub.apply(&event(MdKind::Remove, Side::Buy, 65_000, 0, 1, 4));
    let state = book(&hub);
    assert_eq!(state.seq, Seq(4));
    assert_eq!(state.bids[0].qty, 5);
    assert_eq!(state.bids[0].orders, 1);

    hub.apply(&event(MdKind::Remove, Side::Buy, 65_000, 0, 1, 4));
    let state = book(&hub);
    assert_eq!(state.bids[0].qty, 5);
    assert_eq!(state.bids[0].orders, 1);
}

#[test]
fn the_first_event_after_the_image_applies() {
    let hub = MdHub::new(
        ExchangeConfig::default(),
        "test".into(),
        "memory".into(),
        None,
    );
    seed(&hub);
    // Sequence zero is the image; the first real event must still apply.
    hub.apply(&event(MdKind::Add, Side::Sell, 65_100, 7, 9, 1));
    let state = book(&hub);
    assert_eq!(state.seq, Seq(1));
    assert_eq!(state.asks[0].qty, 7);
    assert_eq!(state.asks[0].orders, 1);
}

#[test]
fn a_book_state_delta_without_an_image_is_refused() {
    let hub = MdHub::new(
        ExchangeConfig::default(),
        "test".into(),
        "memory".into(),
        None,
    );
    hub.apply(&event(MdKind::Add, Side::Sell, 65_100, 7, 9, 1));
    let state = book(&hub);
    assert_eq!(state.seq, Seq(0), "a refused delta must not advance seq");
    assert!(state.asks.is_empty(), "no depth can be built from a delta");
    assert_eq!(hub.events_without_baseline(), 1);
    assert_eq!(
        hub.book_phase(btc()),
        Some(whippet_marketdata::BookPhase::AwaitingSnapshot)
    );

    // The image seeds the book, and the retried delta then applies.
    seed(&hub);
    hub.apply(&event(MdKind::Add, Side::Sell, 65_100, 7, 9, 1));
    assert_eq!(book(&hub).asks[0].qty, 7);
}
