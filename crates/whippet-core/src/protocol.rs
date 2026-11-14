// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Whippet wire protocol.
//!
//! Every payload starts with `version: u8` and a kind discriminant. The layout
//! is fixed-width little-endian; no varints, no floats, no implicit padding.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::WIRE_VERSION;
use crate::codec::{Reader, Writer};
use crate::error::{CoreError, Result};
use crate::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy = 1,
    Sell = 2,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }

    pub fn as_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Buy),
            2 => Ok(Self::Sell),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "Side",
                value: other,
            }),
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Side {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "buy" | "b" | "bid" => Ok(Self::Buy),
            "sell" | "s" | "ask" => Ok(Self::Sell),
            other => Err(CoreError::InvalidValue(format!("unknown side: {other}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit = 1,
    Market = 2,
    Ioc = 3,
    Fok = 4,
    PostOnly = 5,
}

impl OrderType {
    pub fn as_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Limit),
            2 => Ok(Self::Market),
            3 => Ok(Self::Ioc),
            4 => Ok(Self::Fok),
            5 => Ok(Self::PostOnly),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "OrderType",
                value: other,
            }),
        }
    }

    pub fn requires_price(self) -> bool {
        !matches!(self, Self::Market)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::Market => "market",
            Self::Ioc => "ioc",
            Self::Fok => "fok",
            Self::PostOnly => "post_only",
        }
    }
}

impl FromStr for OrderType {
    type Err = CoreError;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "limit" | "gtc" => Ok(Self::Limit),
            "market" => Ok(Self::Market),
            "ioc" => Ok(Self::Ioc),
            "fok" => Ok(Self::Fok),
            "post_only" | "post-only" => Ok(Self::PostOnly),
            other => Err(CoreError::InvalidValue(format!(
                "unknown order type: {other}"
            ))),
        }
    }
}

/// Input command kinds. Cancel reuses the fixed `Command` layout so the
/// journal, gateway, and replay tooling have one decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    NewOrder = 1,
    Cancel = 2,
}

impl CommandKind {
    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::NewOrder),
            2 => Ok(Self::Cancel),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "CommandKind",
                value: other,
            }),
        }
    }
}

pub const COMMAND_ENCODED_LEN: usize = 1 + 1 + 16 + 1 + 1 + 8 + 8 + 16 + 8 + 8 + 8 + 2;

/// A client order or cancel command. This is the only thing the engine reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub kind: CommandKind,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Price,
    pub qty: Qty,
    pub order_id: OrderId,
    pub participant: ParticipantId,
    pub timestamp_ns: u64,
    pub nonce: u64,
    pub flags: u16,
}

impl Command {
    #[allow(clippy::too_many_arguments)]
    pub fn new_order(
        symbol: Symbol,
        side: Side,
        order_type: OrderType,
        price: Price,
        qty: Qty,
        order_id: OrderId,
        participant: ParticipantId,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            kind: CommandKind::NewOrder,
            symbol,
            side,
            order_type,
            price,
            qty,
            order_id,
            participant,
            timestamp_ns,
            nonce: 0,
            flags: 0,
        }
    }

    pub fn cancel(
        symbol: Symbol,
        order_id: OrderId,
        participant: ParticipantId,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            kind: CommandKind::Cancel,
            symbol,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Price::ZERO,
            qty: Qty::ZERO,
            order_id,
            participant,
            timestamp_ns,
            nonce: 0,
            flags: 0,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(COMMAND_ENCODED_LEN);
        writer
            .u8(WIRE_VERSION)
            .u8(self.kind as u8)
            .raw(self.symbol.as_bytes())
            .u8(self.side.as_code())
            .u8(self.order_type.as_code())
            .i64(self.price.0)
            .u64(self.qty.0)
            .u128(self.order_id.0)
            .u64(self.participant.0)
            .u64(self.timestamp_ns)
            .u64(self.nonce)
            .u16(self.flags);
        writer.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != WIRE_VERSION {
            return Err(CoreError::InvalidVersion {
                found: version,
                expected: WIRE_VERSION,
            });
        }
        let kind = CommandKind::from_code(reader.u8()?)?;
        let symbol_bytes: [u8; 16] = reader.bytes(16)?.try_into().unwrap();
        let symbol = Symbol::from_wire(symbol_bytes)?;
        let side = Side::from_code(reader.u8()?)?;
        let order_type = OrderType::from_code(reader.u8()?)?;
        let price = Price(reader.i64()?);
        let qty = Qty(reader.u64()?);
        let order_id = OrderId(reader.u128()?);
        let participant = ParticipantId(reader.u64()?);
        let timestamp_ns = reader.u64()?;
        let nonce = reader.u64()?;
        let flags = reader.u16()?;
        reader.finish()?;
        Ok(Self {
            kind,
            symbol,
            side,
            order_type,
            price,
            qty,
            order_id,
            participant,
            timestamp_ns,
            nonce,
            flags,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecKind {
    Accepted = 1,
    Rejected = 2,
    Filled = 3,
    PartiallyFilled = 4,
    Cancelled = 5,
}

impl ExecKind {
    pub fn as_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Accepted),
            2 => Ok(Self::Rejected),
            3 => Ok(Self::Filled),
            4 => Ok(Self::PartiallyFilled),
            5 => Ok(Self::Cancelled),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "ExecKind",
                value: other,
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Filled => "filled",
            Self::PartiallyFilled => "partially_filled",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    None = 0,
    UnknownSymbol = 1,
    InvalidPrice = 2,
    InvalidQty = 3,
    DuplicateOrderId = 4,
    PriceOutOfBand = 5,
    PostOnlyWouldCross = 6,
    FokUnfillable = 7,
    UnknownOrder = 8,
    SelfTradePrevention = 9,
    Halted = 10,
    Internal = 11,
}

impl RejectReason {
    pub fn as_code(self) -> u16 {
        self as u16
    }

    pub fn from_code(code: u16) -> Result<Self> {
        match code {
            0 => Ok(Self::None),
            1 => Ok(Self::UnknownSymbol),
            2 => Ok(Self::InvalidPrice),
            3 => Ok(Self::InvalidQty),
            4 => Ok(Self::DuplicateOrderId),
            5 => Ok(Self::PriceOutOfBand),
            6 => Ok(Self::PostOnlyWouldCross),
            7 => Ok(Self::FokUnfillable),
            8 => Ok(Self::UnknownOrder),
            9 => Ok(Self::SelfTradePrevention),
            10 => Ok(Self::Halted),
            11 => Ok(Self::Internal),
            other => Err(CoreError::InvalidValue(format!(
                "unknown RejectReason code {other}"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::UnknownSymbol => "unknown_symbol",
            Self::InvalidPrice => "invalid_price",
            Self::InvalidQty => "invalid_qty",
            Self::DuplicateOrderId => "duplicate_order_id",
            Self::PriceOutOfBand => "price_out_of_band",
            Self::PostOnlyWouldCross => "post_only_would_cross",
            Self::FokUnfillable => "fok_unfillable",
            Self::UnknownOrder => "unknown_order",
            Self::SelfTradePrevention => "self_trade_prevention",
            Self::Halted => "halted",
            Self::Internal => "internal",
        }
    }
}

pub const EXEC_ENCODED_LEN: usize = 1 + 1 + 16 + 1 + 1 + 8 + 8 + 8 + 16 + 16 + 8 + 8 + 8 + 2 + 2;

pub const FLAG_TAKER: u16 = 1 << 0;
pub const FLAG_RESTED: u16 = 1 << 1;
pub const FLAG_MAKER: u16 = 1 << 2;

/// One execution report. Output IDs are `(symbol, input_offset, event_index)`
/// in spirit; the engine also stamps a monotonic per-symbol `seq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionReport {
    pub kind: ExecKind,
    pub symbol: Symbol,
    pub side: Side,
    pub order_type: OrderType,
    pub price: Price,
    pub qty: Qty,
    pub remaining: Qty,
    pub order_id: OrderId,
    pub maker_order_id: OrderId,
    pub seq: Seq,
    pub cause_offset: u64,
    pub timestamp_ns: u64,
    pub reason: RejectReason,
    pub flags: u16,
}

impl ExecutionReport {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(EXEC_ENCODED_LEN);
        writer
            .u8(WIRE_VERSION)
            .u8(self.kind.as_code())
            .raw(self.symbol.as_bytes())
            .u8(self.side.as_code())
            .u8(self.order_type.as_code())
            .i64(self.price.0)
            .u64(self.qty.0)
            .u64(self.remaining.0)
            .u128(self.order_id.0)
            .u128(self.maker_order_id.0)
            .u64(self.seq.0)
            .u64(self.cause_offset)
            .u64(self.timestamp_ns)
            .u16(self.reason.as_code())
            .u16(self.flags);
        writer.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != WIRE_VERSION {
            return Err(CoreError::InvalidVersion {
                found: version,
                expected: WIRE_VERSION,
            });
        }
        let kind = ExecKind::from_code(reader.u8()?)?;
        let symbol = Symbol::from_wire(reader.bytes(16)?.try_into().unwrap())?;
        let side = Side::from_code(reader.u8()?)?;
        let order_type = OrderType::from_code(reader.u8()?)?;
        let price = Price(reader.i64()?);
        let qty = Qty(reader.u64()?);
        let remaining = Qty(reader.u64()?);
        let order_id = OrderId(reader.u128()?);
        let maker_order_id = OrderId(reader.u128()?);
        let seq = Seq(reader.u64()?);
        let cause_offset = reader.u64()?;
        let timestamp_ns = reader.u64()?;
        let reason = RejectReason::from_code(reader.u16()?)?;
        let flags = reader.u16()?;
        reader.finish()?;
        Ok(Self {
            kind,
            symbol,
            side,
            order_type,
            price,
            qty,
            remaining,
            order_id,
            maker_order_id,
            seq,
            cause_offset,
            timestamp_ns,
            reason,
            flags,
        })
    }

    pub fn is_terminal_for_order(&self) -> bool {
        matches!(
            self.kind,
            ExecKind::Rejected | ExecKind::Filled | ExecKind::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MdKind {
    Add = 1,
    Reduce = 2,
    Remove = 3,
    Trade = 4,
    Bbo = 5,
    Status = 6,
    Snapshot = 7,
}

impl MdKind {
    pub fn as_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Add),
            2 => Ok(Self::Reduce),
            3 => Ok(Self::Remove),
            4 => Ok(Self::Trade),
            5 => Ok(Self::Bbo),
            6 => Ok(Self::Status),
            7 => Ok(Self::Snapshot),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "MdKind",
                value: other,
            }),
        }
    }
}

pub const MD_EVENT_ENCODED_LEN: usize = 1 + 1 + 16 + 8 + 8 + 8 + 1 + 1 + 8 + 8 + 16 + 8;

/// A market-data delta. `cause_offset` is the input offset that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdEvent {
    pub kind: MdKind,
    pub symbol: Symbol,
    pub seq: Seq,
    pub cause_offset: u64,
    pub timestamp_ns: u64,
    pub side: Side,
    pub flags: u8,
    pub price: Price,
    pub qty: Qty,
    pub order_id: OrderId,
    pub participant: ParticipantId,
}

impl MdEvent {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(MD_EVENT_ENCODED_LEN);
        writer
            .u8(WIRE_VERSION)
            .u8(self.kind.as_code())
            .raw(self.symbol.as_bytes())
            .u64(self.seq.0)
            .u64(self.cause_offset)
            .u64(self.timestamp_ns)
            .u8(self.side.as_code())
            .u8(self.flags)
            .i64(self.price.0)
            .u64(self.qty.0)
            .u128(self.order_id.0)
            .u64(self.participant.0);
        writer.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != WIRE_VERSION {
            return Err(CoreError::InvalidVersion {
                found: version,
                expected: WIRE_VERSION,
            });
        }
        let kind = MdKind::from_code(reader.u8()?)?;
        let symbol = Symbol::from_wire(reader.bytes(16)?.try_into().unwrap())?;
        let seq = Seq(reader.u64()?);
        let cause_offset = reader.u64()?;
        let timestamp_ns = reader.u64()?;
        let side = Side::from_code(reader.u8()?)?;
        let flags = reader.u8()?;
        let price = Price(reader.i64()?);
        let qty = Qty(reader.u64()?);
        let order_id = OrderId(reader.u128()?);
        let participant = ParticipantId(reader.u64()?);
        reader.finish()?;
        Ok(Self {
            kind,
            symbol,
            seq,
            cause_offset,
            timestamp_ns,
            side,
            flags,
            price,
            qty,
            order_id,
            participant,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MdLevel {
    pub price: Price,
    pub qty: Qty,
    pub orders: u32,
}

impl MdLevel {
    pub fn encode(&self, writer: &mut Writer) {
        writer.i64(self.price.0).u64(self.qty.0).u32(self.orders);
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            price: Price(reader.i64()?),
            qty: Qty(reader.u64()?),
            orders: reader.u32()?,
        })
    }
}

pub const MD_SNAPSHOT_HEADER_LEN: usize = 1 + 1 + 16 + 8 + 8 + 8 + 8 + 8 + 2 + 2 + 4;

/// One resting order in a full snapshot. The MD hub needs this to apply
/// subsequent reduce/remove deltas after a recovery; the top-N levels alone are
/// not enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MdOrder {
    pub order_id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub participant: ParticipantId,
}

impl MdOrder {
    pub fn encode(&self, writer: &mut Writer) {
        writer
            .u128(self.order_id.0)
            .u8(self.side.as_code())
            .i64(self.price.0)
            .u64(self.qty.0)
            .u64(self.participant.0);
    }

    pub fn decode(reader: &mut Reader<'_>) -> Result<Self> {
        Ok(Self {
            order_id: OrderId(reader.u128()?),
            side: Side::from_code(reader.u8()?)?,
            price: Price(reader.i64()?),
            qty: Qty(reader.u64()?),
            participant: ParticipantId(reader.u64()?),
        })
    }
}

/// A full snapshot, emitted on startup and periodically so a subscriber can
/// recover without replaying the whole partition. `orders` is the complete
/// resting book; `bids`/`asks` are the display top-N levels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdSnapshot {
    pub symbol: Symbol,
    pub seq: Seq,
    pub cause_offset: u64,
    pub timestamp_ns: u64,
    pub last_trade_price: Price,
    pub last_trade_qty: Qty,
    pub bids: Vec<MdLevel>,
    pub asks: Vec<MdLevel>,
    pub orders: Vec<MdOrder>,
}

impl MdSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(
            MD_SNAPSHOT_HEADER_LEN
                + (self.bids.len() + self.asks.len()) * 20
                + self.orders.len() * 37,
        );
        writer
            .u8(WIRE_VERSION)
            .u8(MdKind::Snapshot.as_code())
            .raw(self.symbol.as_bytes())
            .u64(self.seq.0)
            .u64(self.cause_offset)
            .u64(self.timestamp_ns)
            .i64(self.last_trade_price.0)
            .u64(self.last_trade_qty.0)
            .u16(self.bids.len() as u16)
            .u16(self.asks.len() as u16)
            .u32(self.orders.len() as u32);
        for level in &self.bids {
            level.encode(&mut writer);
        }
        for level in &self.asks {
            level.encode(&mut writer);
        }
        for order in &self.orders {
            order.encode(&mut writer);
        }
        writer.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let version = reader.u8()?;
        if version != WIRE_VERSION {
            return Err(CoreError::InvalidVersion {
                found: version,
                expected: WIRE_VERSION,
            });
        }
        let kind = MdKind::from_code(reader.u8()?)?;
        if kind != MdKind::Snapshot {
            return Err(CoreError::InvalidValue(format!(
                "expected snapshot, got {kind:?}"
            )));
        }
        let symbol = Symbol::from_wire(reader.bytes(16)?.try_into().unwrap())?;
        let seq = Seq(reader.u64()?);
        let cause_offset = reader.u64()?;
        let timestamp_ns = reader.u64()?;
        let last_trade_price = Price(reader.i64()?);
        let last_trade_qty = Qty(reader.u64()?);
        let bid_count = reader.u16()? as usize;
        let ask_count = reader.u16()? as usize;
        let order_count = reader.u32()? as usize;
        let mut bids = Vec::with_capacity(bid_count);
        for _ in 0..bid_count {
            bids.push(MdLevel::decode(&mut reader)?);
        }
        let mut asks = Vec::with_capacity(ask_count);
        for _ in 0..ask_count {
            asks.push(MdLevel::decode(&mut reader)?);
        }
        if order_count > reader.remaining() / 37 {
            return Err(CoreError::InvalidValue(format!(
                "snapshot claims {order_count} orders but only {} bytes remain",
                reader.remaining()
            )));
        }
        let mut orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            orders.push(MdOrder::decode(&mut reader)?);
        }
        reader.finish()?;
        Ok(Self {
            symbol,
            seq,
            cause_offset,
            timestamp_ns,
            last_trade_price,
            last_trade_qty,
            bids,
            asks,
            orders,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD").unwrap()
    }

    #[test]
    fn command_round_trip() {
        let command = Command::new_order(
            symbol(),
            Side::Buy,
            OrderType::Limit,
            Price(65_000),
            Qty(10),
            OrderId(42),
            ParticipantId(7),
            1_700_000_000_000_000_000,
        );
        let bytes = command.encode();
        assert_eq!(bytes.len(), COMMAND_ENCODED_LEN);
        assert_eq!(Command::decode(&bytes).unwrap(), command);
    }

    #[test]
    fn cancel_round_trip() {
        let command = Command::cancel(symbol(), OrderId(9), ParticipantId(1), 5);
        let bytes = command.encode();
        assert_eq!(Command::decode(&bytes).unwrap(), command);
    }

    #[test]
    fn execution_round_trip() {
        let report = ExecutionReport {
            kind: ExecKind::PartiallyFilled,
            symbol: symbol(),
            side: Side::Sell,
            order_type: OrderType::Ioc,
            price: Price(64_999),
            qty: Qty(3),
            remaining: Qty(7),
            order_id: OrderId(1),
            maker_order_id: OrderId(2),
            seq: Seq(99),
            cause_offset: 1234,
            timestamp_ns: 55,
            reason: RejectReason::None,
            flags: FLAG_TAKER,
        };
        let bytes = report.encode();
        assert_eq!(bytes.len(), EXEC_ENCODED_LEN);
        assert_eq!(ExecutionReport::decode(&bytes).unwrap(), report);
    }

    #[test]
    fn md_event_round_trip() {
        let event = MdEvent {
            kind: MdKind::Trade,
            symbol: symbol(),
            seq: Seq(10),
            cause_offset: 3,
            timestamp_ns: 4,
            side: Side::Buy,
            flags: 0,
            price: Price(65_001),
            qty: Qty(2),
            order_id: OrderId(5),
            participant: ParticipantId(6),
        };
        let bytes = event.encode();
        assert_eq!(bytes.len(), MD_EVENT_ENCODED_LEN);
        assert_eq!(MdEvent::decode(&bytes).unwrap(), event);
    }

    #[test]
    fn snapshot_round_trip() {
        let snapshot = MdSnapshot {
            symbol: symbol(),
            seq: Seq(11),
            cause_offset: 12,
            timestamp_ns: 13,
            last_trade_price: Price(65_000),
            last_trade_qty: Qty(4),
            bids: vec![MdLevel {
                price: Price(64_999),
                qty: Qty(10),
                orders: 2,
            }],
            asks: vec![
                MdLevel {
                    price: Price(65_001),
                    qty: Qty(8),
                    orders: 1,
                },
                MdLevel {
                    price: Price(65_002),
                    qty: Qty(5),
                    orders: 3,
                },
            ],
            orders: vec![MdOrder {
                order_id: OrderId(1),
                side: Side::Buy,
                price: Price(64_999),
                qty: Qty(10),
                participant: ParticipantId(2),
            }],
        };
        let bytes = snapshot.encode();
        assert_eq!(MdSnapshot::decode(&bytes).unwrap(), snapshot);
    }

    #[test]
    fn rejects_wrong_version() {
        let mut bytes = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 0).encode();
        bytes[0] = 99;
        assert!(Command::decode(&bytes).is_err());
    }
}
