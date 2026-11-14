// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic command builders.
//!
//! Every builder uses the demo symbol `BTC-USD`, participant `1`, and
//! timestamp `0` by default. Tests that need a different symbol, participant,
//! or clock can use the `*_for` variants or mutate the returned [`Command`]
//! (all of its fields are public). Nothing here reads the wall clock.

use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};

/// Default symbol for the builders.
pub const DEFAULT_SYMBOL: &str = "BTC-USD";
/// Default participant for the builders.
pub const DEFAULT_PARTICIPANT: u64 = 1;
/// Default timestamp for the builders. The engine only requires monotonicity
/// for realism, not for correctness; zero keeps scenarios reproducible.
pub const DEFAULT_TIMESTAMP_NS: u64 = 0;

/// The default symbol, parsed once per call.
pub fn default_symbol() -> Symbol {
    Symbol::new(DEFAULT_SYMBOL).expect("static default symbol is valid")
}

fn command(
    symbol: Symbol,
    participant: u64,
    side: Side,
    order_type: OrderType,
    price: i64,
    qty: u64,
    order_id: u128,
) -> Command {
    Command::new_order(
        symbol,
        side,
        order_type,
        Price(price),
        Qty(qty),
        OrderId(order_id),
        ParticipantId(participant),
        DEFAULT_TIMESTAMP_NS,
    )
}

/// A resting limit order on the default symbol.
pub fn order(side: Side, price: i64, qty: u64, order_id: u128) -> Command {
    order_for(
        default_symbol(),
        DEFAULT_PARTICIPANT,
        side,
        price,
        qty,
        order_id,
    )
}

/// A limit order with an explicit symbol and participant.
pub fn order_for(
    symbol: Symbol,
    participant: u64,
    side: Side,
    price: i64,
    qty: u64,
    order_id: u128,
) -> Command {
    command(
        symbol,
        participant,
        side,
        OrderType::Limit,
        price,
        qty,
        order_id,
    )
}

/// A cancel on the default symbol.
pub fn cancel(order_id: u128) -> Command {
    cancel_for(default_symbol(), DEFAULT_PARTICIPANT, order_id)
}

/// A cancel with an explicit symbol and participant.
pub fn cancel_for(symbol: Symbol, participant: u64, order_id: u128) -> Command {
    Command::cancel(
        symbol,
        OrderId(order_id),
        ParticipantId(participant),
        DEFAULT_TIMESTAMP_NS,
    )
}

/// A market order on the default symbol. The price is zero by protocol.
pub fn market(side: Side, qty: u64, order_id: u128) -> Command {
    market_for(default_symbol(), DEFAULT_PARTICIPANT, side, qty, order_id)
}

/// A market order with an explicit symbol and participant.
pub fn market_for(
    symbol: Symbol,
    participant: u64,
    side: Side,
    qty: u64,
    order_id: u128,
) -> Command {
    command(
        symbol,
        participant,
        side,
        OrderType::Market,
        Price::ZERO.0,
        qty,
        order_id,
    )
}

/// An immediate-or-cancel order on the default symbol.
pub fn ioc(side: Side, price: i64, qty: u64, order_id: u128) -> Command {
    order_type(OrderType::Ioc, side, price, qty, order_id)
}

/// An IOC order with an explicit symbol and participant.
pub fn ioc_for(
    symbol: Symbol,
    participant: u64,
    side: Side,
    price: i64,
    qty: u64,
    order_id: u128,
) -> Command {
    command(
        symbol,
        participant,
        side,
        OrderType::Ioc,
        price,
        qty,
        order_id,
    )
}

/// A fill-or-kill order on the default symbol.
pub fn fok(side: Side, price: i64, qty: u64, order_id: u128) -> Command {
    order_type(OrderType::Fok, side, price, qty, order_id)
}

/// A FOK order with an explicit symbol and participant.
pub fn fok_for(
    symbol: Symbol,
    participant: u64,
    side: Side,
    price: i64,
    qty: u64,
    order_id: u128,
) -> Command {
    command(
        symbol,
        participant,
        side,
        OrderType::Fok,
        price,
        qty,
        order_id,
    )
}

/// A post-only order on the default symbol.
pub fn post_only(side: Side, price: i64, qty: u64, order_id: u128) -> Command {
    order_type(OrderType::PostOnly, side, price, qty, order_id)
}

/// A post-only order with an explicit symbol and participant.
pub fn post_only_for(
    symbol: Symbol,
    participant: u64,
    side: Side,
    price: i64,
    qty: u64,
    order_id: u128,
) -> Command {
    command(
        symbol,
        participant,
        side,
        OrderType::PostOnly,
        price,
        qty,
        order_id,
    )
}

fn order_type(order_type: OrderType, side: Side, price: i64, qty: u64, order_id: u128) -> Command {
    command(
        default_symbol(),
        DEFAULT_PARTICIPANT,
        side,
        order_type,
        price,
        qty,
        order_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_use_sensible_defaults() {
        let limit = order(Side::Buy, 100, 10, 7);
        assert_eq!(limit.symbol, default_symbol());
        assert_eq!(limit.participant, ParticipantId(DEFAULT_PARTICIPANT));
        assert_eq!(limit.order_type, OrderType::Limit);
        assert_eq!(limit.price, Price(100));
        assert_eq!(limit.qty, Qty(10));
        assert_eq!(limit.order_id, OrderId(7));
        assert_eq!(limit.timestamp_ns, DEFAULT_TIMESTAMP_NS);
    }

    #[test]
    fn market_has_no_price_and_cancel_is_a_cancel() {
        let market = market(Side::Sell, 5, 1);
        assert_eq!(market.order_type, OrderType::Market);
        assert_eq!(market.price, Price::ZERO);

        let cancel = cancel(9);
        assert_eq!(cancel.kind, whippet_core::protocol::CommandKind::Cancel);
        assert_eq!(cancel.order_id, OrderId(9));
        assert_eq!(cancel.qty, Qty::ZERO);
    }

    #[test]
    fn order_type_builders_select_the_right_discriminant() {
        assert_eq!(ioc(Side::Buy, 100, 1, 1).order_type, OrderType::Ioc);
        assert_eq!(fok(Side::Buy, 100, 1, 1).order_type, OrderType::Fok);
        assert_eq!(
            post_only(Side::Sell, 101, 1, 1).order_type,
            OrderType::PostOnly
        );
    }
}
