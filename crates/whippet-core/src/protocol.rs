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

/// Instrument session state.
///
/// The session is an engine field, not a per-command policy: it gates new
/// orders until a later [`CommandKind::SetSession`] reopens the instrument.
/// The default is [`SessionState::Open`] so an engine that never sees a
/// session command behaves exactly as it did before sessions existed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    PreOpen = 0,
    #[default]
    Open = 1,
    Closed = 2,
}

impl SessionState {
    pub fn as_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::PreOpen),
            1 => Ok(Self::Open),
            2 => Ok(Self::Closed),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "SessionState",
                value: other,
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreOpen => "pre_open",
            Self::Open => "open",
            Self::Closed => "closed",
        }
    }

    /// This session's bits in an MD status event or snapshot, placed at
    /// [`MD_SESSION_SHIFT`] so they never collide with [`MD_FLAG_HALTED`].
    pub fn to_md_flags(self) -> u8 {
        (self.as_code() << MD_SESSION_SHIFT) & MD_SESSION_MASK
    }

    /// Decode the session bits of an MD status event or snapshot.
    pub fn from_md_flags(flags: u8) -> Result<Self> {
        Self::from_code((flags & MD_SESSION_MASK) >> MD_SESSION_SHIFT)
    }
}

impl fmt::Display for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The two independent status fields of one instrument, together.
///
/// The engine stores `session` and `halted` separately so the snapshot layout
/// stays two fields, but every admission decision depends on the combination.
/// This type is the single place that defines the accepted combination and
/// the MD flag encoding, so the engine, hub, and tests cannot drift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingStatus {
    pub session: SessionState,
    pub halted: bool,
}

impl TradingStatus {
    pub const fn new(session: SessionState, halted: bool) -> Self {
        Self { session, halted }
    }

    /// New orders are admitted only in an open, running instrument. Cancels
    /// are deliberately outside this predicate and remain allowed everywhere.
    pub const fn can_accept_new_orders(self) -> bool {
        matches!(self.session, SessionState::Open) && !self.halted
    }

    pub fn to_md_flags(self) -> u8 {
        (if self.halted { MD_FLAG_HALTED } else { 0 }) | self.session.to_md_flags()
    }

    pub fn from_md_flags(flags: u8) -> Result<Self> {
        Ok(Self {
            session: SessionState::from_md_flags(flags)?,
            halted: flags & MD_FLAG_HALTED != 0,
        })
    }
}

/// Input command kinds. Cancel, halt, resume, session control, and risk limits
/// reuse the fixed `Command` layout so the journal, gateway, and replay tooling
/// have one decoder.
///
/// `Halt`, `Resume`, `SetSession`, and `SetRisk` are operator commands on the
/// same input log as client traffic. Being ordered commands is what makes them
/// replayable: a restored engine reaches the same halt, session, and risk state
/// by replaying the log, and the audit trail is just the record that carried
/// the command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    NewOrder = 1,
    Cancel = 2,
    Halt = 3,
    Resume = 4,
    SetSession = 5,
    SetRisk = 6,
}

impl CommandKind {
    pub fn as_code(self) -> u8 {
        self as u8
    }

    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::NewOrder),
            2 => Ok(Self::Cancel),
            3 => Ok(Self::Halt),
            4 => Ok(Self::Resume),
            5 => Ok(Self::SetSession),
            6 => Ok(Self::SetRisk),
            other => Err(CoreError::UnknownDiscriminant {
                kind: "CommandKind",
                value: other,
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewOrder => "new_order",
            Self::Cancel => "cancel",
            Self::Halt => "halt",
            Self::Resume => "resume",
            Self::SetSession => "set_session",
            Self::SetRisk => "set_risk",
        }
    }
}

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
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

    /// Operator command: halt the instrument until the next resume.
    ///
    /// The unused order fields stay zero (side is `Buy` only because the field
    /// is fixed-width) so the decoder has one shape for every command kind.
    pub fn halt(symbol: Symbol, participant: ParticipantId, timestamp_ns: u64) -> Self {
        Self::control(CommandKind::Halt, symbol, participant, timestamp_ns)
    }

    /// Operator command: resume a halted instrument.
    pub fn resume(symbol: Symbol, participant: ParticipantId, timestamp_ns: u64) -> Self {
        Self::control(CommandKind::Resume, symbol, participant, timestamp_ns)
    }

    /// Operator command: move the instrument to `session`.
    ///
    /// The session code travels in the low byte of `flags`; every other field
    /// keeps the zero side/price/qty/order-id shape of the other operator
    /// commands, so [`COMMAND_ENCODED_LEN`] and the wire layout are unchanged.
    pub fn set_session(
        symbol: Symbol,
        session: SessionState,
        participant: ParticipantId,
        timestamp_ns: u64,
    ) -> Self {
        let mut command = Self::control(CommandKind::SetSession, symbol, participant, timestamp_ns);
        command.flags = u16::from(session.as_code());
        command
    }

    /// Decode the session code carried by a [`CommandKind::SetSession`]
    /// command. Any flag bit outside the session byte is rejected instead of
    /// being ignored: the journal is a trust boundary, and a command that
    /// carries two meanings is a bug, not a value to guess at.
    pub fn session_state(&self) -> Result<SessionState> {
        if self.flags & !u16::from(u8::MAX) != 0 {
            return Err(CoreError::InvalidValue(format!(
                "SetSession flags {:#06x} set bits outside the session code byte",
                self.flags
            )));
        }
        SessionState::from_code(self.flags as u8)
    }

    /// Operator command: set or clear the per-participant risk limit on
    /// `symbol`.
    ///
    /// Reuses the fixed `Command` layout without changing
    /// [`COMMAND_ENCODED_LEN`]:
    /// - `symbol` is the target instrument,
    /// - `participant` is the target participant,
    /// - `qty` carries `max_net_qty` (absolute net position bound; `0`
    ///   disables),
    /// - `price` carries `max_open_buy_notional_cents` in quote cents (`0`
    ///   disables; negative is invalid),
    /// - `side` (`Buy`), `order_type` (`Limit`), `order_id` (`0`), `nonce`
    ///   (`0`), and `flags` (`0`) are unused and keep their canonical control
    ///   values.
    pub fn set_risk(
        symbol: Symbol,
        participant: ParticipantId,
        max_net_qty: u64,
        max_open_buy_notional_cents: i64,
        timestamp_ns: u64,
    ) -> Self {
        let mut command = Self::control(CommandKind::SetRisk, symbol, participant, timestamp_ns);
        command.qty = Qty(max_net_qty);
        command.price = Price(max_open_buy_notional_cents);
        command
    }

    fn control(
        kind: CommandKind,
        symbol: Symbol,
        participant: ParticipantId,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            kind,
            symbol,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Price::ZERO,
            qty: Qty::ZERO,
            order_id: OrderId(0),
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
    SessionNotOpen = 12,
    RiskLimit = 13,
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
            12 => Ok(Self::SessionNotOpen),
            13 => Ok(Self::RiskLimit),
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
            Self::SessionNotOpen => "session_not_open",
            Self::RiskLimit => "risk_limit",
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

/// Status-event flag: the instrument is halted. A resume status event carries
/// `0` in this bit. Consumers should treat `MdKind::Status` with this flag set
/// as "reject new orders until a status event without it".
pub const MD_FLAG_HALTED: u8 = 1 << 0;

/// Where the [`SessionState`] code sits in a status-event or snapshot flag
/// byte. Bit 0 is [`MD_FLAG_HALTED`], so the two bits above it carry the
/// session.
pub const MD_SESSION_SHIFT: u8 = 1;

/// The two status-flag bits that carry [`SessionState`]. Extract with
/// [`SessionState::from_md_flags`]; a snapshot with these bits and the halted
/// bit describes the full venue status.
pub const MD_SESSION_MASK: u8 = 0b11 << MD_SESSION_SHIFT;

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

/// Encoded size of one [`MdLevel`]: `i64 price + u64 qty + u32 orders`.
pub const MD_LEVEL_ENCODED_LEN: usize = 8 + 8 + 4;

/// Encoded size of one [`MdOrder`]: `u128 id + u8 side + i64 price + u64 qty +
/// u64 participant`. The snapshot decoder uses this to reject an order count
/// that cannot fit before reserving memory for it.
pub const MD_ORDER_ENCODED_LEN: usize = 16 + 1 + 8 + 8 + 8;

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
    /// Venue-status bits carried by the snapshot: [`MD_FLAG_HALTED`] plus the
    /// [`SessionState`] bits at [`MD_SESSION_SHIFT`]. Encoded as an optional
    /// trailing byte so older snapshots decode as 0.
    pub flags: u8,
}

impl MdSnapshot {
    pub fn encode(&self) -> Vec<u8> {
        let mut writer = Writer::with_capacity(
            MD_SNAPSHOT_HEADER_LEN
                + (self.bids.len() + self.asks.len()) * MD_LEVEL_ENCODED_LEN
                + self.orders.len() * MD_ORDER_ENCODED_LEN
                + 1,
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
        writer.u8(self.flags);
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
        // Validate each count against the bytes that actually remain before
        // reserving: a 66-byte header can otherwise advertise 65,535 levels
        // and force a multi-megabyte allocation for a frame that cannot exist.
        if bid_count > reader.remaining() / MD_LEVEL_ENCODED_LEN {
            return Err(CoreError::InvalidValue(format!(
                "snapshot claims {bid_count} bid levels but only {} bytes remain",
                reader.remaining()
            )));
        }
        let mut bids = Vec::with_capacity(bid_count);
        for _ in 0..bid_count {
            bids.push(MdLevel::decode(&mut reader)?);
        }
        if ask_count > reader.remaining() / MD_LEVEL_ENCODED_LEN {
            return Err(CoreError::InvalidValue(format!(
                "snapshot claims {ask_count} ask levels but only {} bytes remain",
                reader.remaining()
            )));
        }
        let mut asks = Vec::with_capacity(ask_count);
        for _ in 0..ask_count {
            asks.push(MdLevel::decode(&mut reader)?);
        }
        if order_count > reader.remaining() / MD_ORDER_ENCODED_LEN {
            return Err(CoreError::InvalidValue(format!(
                "snapshot claims {order_count} orders but only {} bytes remain",
                reader.remaining()
            )));
        }
        let mut orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            orders.push(MdOrder::decode(&mut reader)?);
        }
        // The status byte is optional: snapshots written before the control
        // plane decode as running (0), and the decoder still rejects any
        // other trailing bytes.
        let flags = if reader.remaining() >= 1 {
            reader.u8()?
        } else {
            0
        };
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
            flags,
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
    fn halt_and_resume_round_trip() {
        let halt = Command::halt(symbol(), ParticipantId(7), 1_700_000_000_000_000_001);
        assert_eq!(halt.kind, CommandKind::Halt);
        assert_eq!(halt.side, Side::Buy);
        assert_eq!(halt.order_type, OrderType::Limit);
        assert_eq!(halt.price, Price::ZERO);
        assert_eq!(halt.qty, Qty::ZERO);
        assert_eq!(halt.order_id, OrderId(0));
        assert_eq!(halt.participant, ParticipantId(7));
        assert_eq!(halt.nonce, 0);
        assert_eq!(halt.flags, 0);
        let bytes = halt.encode();
        assert_eq!(bytes.len(), COMMAND_ENCODED_LEN);
        assert_eq!(Command::decode(&bytes).unwrap(), halt);

        let resume = Command::resume(symbol(), ParticipantId(7), 1_700_000_000_000_000_002);
        assert_eq!(resume.kind, CommandKind::Resume);
        assert_eq!(resume.price, Price::ZERO);
        assert_eq!(resume.qty, Qty::ZERO);
        assert_eq!(resume.order_id, OrderId(0));
        let bytes = resume.encode();
        assert_eq!(bytes.len(), COMMAND_ENCODED_LEN);
        assert_eq!(Command::decode(&bytes).unwrap(), resume);
    }

    #[test]
    fn command_kind_from_code_covers_every_kind() {
        for (kind, code, name) in [
            (CommandKind::NewOrder, 1u8, "new_order"),
            (CommandKind::Cancel, 2, "cancel"),
            (CommandKind::Halt, 3, "halt"),
            (CommandKind::Resume, 4, "resume"),
            (CommandKind::SetSession, 5, "set_session"),
            (CommandKind::SetRisk, 6, "set_risk"),
        ] {
            assert_eq!(kind.as_code(), code);
            assert_eq!(CommandKind::from_code(code).unwrap(), kind);
            assert_eq!(kind.as_str(), name);
            assert_eq!(kind.to_string(), name);
        }
        assert!(CommandKind::from_code(0).is_err());
        assert!(CommandKind::from_code(7).is_err());
    }

    #[test]
    fn set_risk_round_trip() {
        let command = Command::set_risk(
            symbol(),
            ParticipantId(7),
            500,
            25_000_000,
            1_700_000_000_000_000_004,
        );
        assert_eq!(command.kind, CommandKind::SetRisk);
        assert_eq!(command.symbol, symbol());
        assert_eq!(command.participant, ParticipantId(7));
        assert_eq!(command.qty, Qty(500));
        assert_eq!(command.price, Price(25_000_000));
        assert_eq!(command.side, Side::Buy);
        assert_eq!(command.order_type, OrderType::Limit);
        assert_eq!(command.order_id, OrderId(0));
        assert_eq!(command.nonce, 0);
        assert_eq!(command.flags, 0);

        let bytes = command.encode();
        assert_eq!(bytes.len(), COMMAND_ENCODED_LEN);
        assert_eq!(Command::decode(&bytes).unwrap(), command);
    }

    #[test]
    fn session_state_codes_and_names_round_trip() {
        for (session, code, name) in [
            (SessionState::PreOpen, 0u8, "pre_open"),
            (SessionState::Open, 1, "open"),
            (SessionState::Closed, 2, "closed"),
        ] {
            assert_eq!(session.as_code(), code);
            assert_eq!(SessionState::from_code(code).unwrap(), session);
            assert_eq!(session.as_str(), name);
            assert_eq!(session.to_string(), name);
        }
        assert_eq!(
            SessionState::default(),
            SessionState::Open,
            "the default must preserve pre-session behaviour"
        );
        assert!(SessionState::from_code(3).is_err());

        // serde names are the same snake_case strings the wire/hub uses.
        for session in [
            SessionState::PreOpen,
            SessionState::Open,
            SessionState::Closed,
        ] {
            let json = serde_json::to_string(&session).unwrap();
            assert_eq!(json, format!("\"{}\"", session.as_str()));
            assert_eq!(
                serde_json::from_str::<SessionState>(&json).unwrap(),
                session
            );
        }
    }

    #[test]
    fn session_bits_do_not_collide_with_the_halted_flag() {
        assert_eq!(MD_FLAG_HALTED & MD_SESSION_MASK, 0);
        assert_eq!(SessionState::PreOpen.to_md_flags(), 0b000);
        assert_eq!(SessionState::Open.to_md_flags(), 0b010);
        assert_eq!(SessionState::Closed.to_md_flags(), 0b100);
        for session in [
            SessionState::PreOpen,
            SessionState::Open,
            SessionState::Closed,
        ] {
            // Halted is independent of the session: both compose fully.
            let flags = MD_FLAG_HALTED | session.to_md_flags();
            assert_eq!(flags & MD_FLAG_HALTED, MD_FLAG_HALTED);
            assert_eq!(SessionState::from_md_flags(flags).unwrap(), session);
        }
    }

    #[test]
    fn set_session_round_trip() {
        for session in [
            SessionState::PreOpen,
            SessionState::Open,
            SessionState::Closed,
        ] {
            let command = Command::set_session(
                symbol(),
                session,
                ParticipantId(7),
                1_700_000_000_000_000_003,
            );
            assert_eq!(command.kind, CommandKind::SetSession);
            assert_eq!(command.side, Side::Buy);
            assert_eq!(command.order_type, OrderType::Limit);
            assert_eq!(command.price, Price::ZERO);
            assert_eq!(command.qty, Qty::ZERO);
            assert_eq!(command.order_id, OrderId(0));
            assert_eq!(command.participant, ParticipantId(7));
            assert_eq!(command.nonce, 0);
            assert_eq!(command.flags, u16::from(session.as_code()));
            assert_eq!(command.session_state().unwrap(), session);

            let bytes = command.encode();
            assert_eq!(
                bytes.len(),
                COMMAND_ENCODED_LEN,
                "the fixed wire layout is unchanged"
            );
            let decoded = Command::decode(&bytes).unwrap();
            assert_eq!(decoded, command);
            assert_eq!(decoded.session_state().unwrap(), session);
        }
    }

    #[test]
    fn set_session_rejects_flags_outside_the_session_byte() {
        let mut command = Command::set_session(symbol(), SessionState::Closed, ParticipantId(7), 1);
        command.flags = 0x0102;
        assert!(command.session_state().is_err());
        command.flags = 3;
        assert!(command.session_state().is_err());
    }

    #[test]
    fn reject_reasons_round_trip() {
        let reasons = [
            RejectReason::None,
            RejectReason::UnknownSymbol,
            RejectReason::InvalidPrice,
            RejectReason::InvalidQty,
            RejectReason::DuplicateOrderId,
            RejectReason::PriceOutOfBand,
            RejectReason::PostOnlyWouldCross,
            RejectReason::FokUnfillable,
            RejectReason::UnknownOrder,
            RejectReason::SelfTradePrevention,
            RejectReason::Halted,
            RejectReason::Internal,
            RejectReason::SessionNotOpen,
            RejectReason::RiskLimit,
        ];
        for reason in reasons {
            assert_eq!(RejectReason::from_code(reason.as_code()).unwrap(), reason);
        }
        assert_eq!(RejectReason::SessionNotOpen.as_code(), 12);
        assert_eq!(RejectReason::SessionNotOpen.as_str(), "session_not_open");
        assert_eq!(RejectReason::RiskLimit.as_code(), 13);
        assert_eq!(RejectReason::RiskLimit.as_str(), "risk_limit");
        assert!(RejectReason::from_code(14).is_err());
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
    fn trading_status_combinations_round_trip_and_gate_admission() {
        for session in [
            SessionState::PreOpen,
            SessionState::Open,
            SessionState::Closed,
        ] {
            for halted in [false, true] {
                let status = TradingStatus::new(session, halted);
                assert_eq!(
                    TradingStatus::from_md_flags(status.to_md_flags()),
                    Ok(status)
                );
                assert_eq!(
                    status.can_accept_new_orders(),
                    session == SessionState::Open && !halted,
                    "session {session:?} halted {halted}"
                );
            }
        }
        assert!(TradingStatus::from_md_flags(0b1111_1111).is_err());
    }

    #[test]
    fn halted_status_event_round_trip() {
        let event = MdEvent {
            kind: MdKind::Status,
            symbol: symbol(),
            seq: Seq(1),
            cause_offset: 2,
            timestamp_ns: 3,
            side: Side::Buy,
            flags: MD_FLAG_HALTED | SessionState::Closed.to_md_flags(),
            price: Price::ZERO,
            qty: Qty::ZERO,
            order_id: OrderId(0),
            participant: ParticipantId(4),
        };
        let bytes = event.encode();
        assert_eq!(bytes.len(), MD_EVENT_ENCODED_LEN);
        let decoded = MdEvent::decode(&bytes).unwrap();
        assert_eq!(decoded, event);
        assert_eq!(
            SessionState::from_md_flags(decoded.flags).unwrap(),
            SessionState::Closed
        );
        assert_ne!(decoded.flags & MD_FLAG_HALTED, 0);
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
            flags: MD_FLAG_HALTED | SessionState::Closed.to_md_flags(),
        };
        let bytes = snapshot.encode();
        let decoded = MdSnapshot::decode(&bytes).unwrap();
        assert_eq!(decoded, snapshot);
        assert_eq!(
            SessionState::from_md_flags(decoded.flags).unwrap(),
            SessionState::Closed
        );

        // A snapshot written before the status byte decodes as running.
        let legacy = &bytes[..bytes.len() - 1];
        assert_eq!(MdSnapshot::decode(legacy).unwrap().flags, 0);
    }

    #[test]
    fn rejects_wrong_version() {
        let mut bytes = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 0).encode();
        bytes[0] = 99;
        assert!(Command::decode(&bytes).is_err());
    }
}
