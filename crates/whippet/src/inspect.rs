// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Read-only decoding and filtering for the operator audit and order-trace
//! tools.
//!
//! `whippet audit` and `whippet order-trace` are offline tools: they observe
//! the `audit`, `orders`, and `executions` topics and never append, commit,
//! create, or delete anything. This module is the pure half (decode, match,
//! filter); the journal reads live in [`crate::cli`] behind the [`Journal`]
//! seam so both the Iggy path and the memory-journal tests share one
//! implementation.
//!
//! [`Journal`]: whippet_journal::Journal

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use whippet_core::ids::{OrderId, ParticipantId, Symbol};
use whippet_core::protocol::{Command, CommandKind, ExecKind, ExecutionReport, RejectReason};

/// The audit topic's pinned partition count.
///
/// ADR 0015 routes each audit record to `symbol.partition(4)` and
/// `whippet_journal::Topic::partition_count` pins the topic at four partitions.
pub const AUDIT_TOPIC_PARTITIONS: u32 = 4;

/// The JSON object the gateway appends to the `audit` topic.
///
/// The field set is exactly the gateway's: `operator`, `action`, `symbol`,
/// `halted`, `reason`, `order_offset`, `timestamp_ns`, `service`. Unknown
/// fields are ignored, so adding a field to the record does not break an older
/// reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPayload {
    pub operator: String,
    pub action: String,
    pub symbol: String,
    pub halted: bool,
    /// Operator-supplied free text; the gateway writes JSON `null` when absent.
    #[serde(default)]
    pub reason: Option<String>,
    /// Offset of the operator command in the orders log: the source of truth.
    pub order_offset: u64,
    pub timestamp_ns: u64,
    pub service: String,
}

/// An audit payload plus the topic position it was read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
    pub partition: u32,
    /// Offset within the audit topic partition, not the orders log.
    pub offset: u64,
    #[serde(flatten)]
    pub payload: AuditPayload,
    /// The bytes exactly as read from the journal, kept for diagnostics.
    #[serde(skip)]
    pub raw: Vec<u8>,
}

impl AuditRecord {
    /// Decode one journal payload. The caller supplies the position because the
    /// payload does not carry it.
    pub fn decode(partition: u32, offset: u64, raw: &[u8]) -> Result<Self, serde_json::Error> {
        Ok(Self {
            partition,
            offset,
            payload: serde_json::from_slice(raw)?,
            raw: raw.to_vec(),
        })
    }
}

/// Selection applied after every record on the requested partitions is decoded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditFilter {
    /// Exact operator identity.
    pub operator: Option<String>,
    /// `halt` or `resume`, matched case-insensitively.
    pub action: Option<String>,
    /// Instrument symbol, matched case-insensitively.
    pub symbol: Option<String>,
    /// Minimum audit-topic offset, inclusive.
    pub min_offset: Option<u64>,
    /// Maximum number of records to return, applied after every other filter.
    pub limit: Option<usize>,
}

impl AuditFilter {
    fn matches(&self, record: &AuditRecord) -> bool {
        if let Some(operator) = &self.operator
            && record.payload.operator != *operator
        {
            return false;
        }
        if let Some(action) = &self.action
            && !record.payload.action.eq_ignore_ascii_case(action)
        {
            return false;
        }
        if let Some(symbol) = &self.symbol
            && !record.payload.symbol.eq_ignore_ascii_case(symbol)
        {
            return false;
        }
        if let Some(min_offset) = self.min_offset
            && record.offset < min_offset
        {
            return false;
        }
        true
    }

    /// A human-readable summary for the "nothing matched" line.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(operator) = &self.operator {
            parts.push(format!("operator={operator}"));
        }
        if let Some(action) = &self.action {
            parts.push(format!("action={action}"));
        }
        if let Some(symbol) = &self.symbol {
            parts.push(format!("symbol={symbol}"));
        }
        if let Some(min_offset) = self.min_offset {
            parts.push(format!("min_offset={min_offset}"));
        }
        if let Some(limit) = self.limit {
            parts.push(format!("limit={limit}"));
        }
        if parts.is_empty() {
            "no filter".to_string()
        } else {
            parts.join(" ")
        }
    }
}

/// Apply `filter` to `records`, preserving read order.
pub fn filter_audit(records: &[AuditRecord], filter: &AuditFilter) -> Vec<AuditRecord> {
    let limit = filter.limit.unwrap_or(usize::MAX);
    records
        .iter()
        .filter(|record| filter.matches(record))
        .take(limit)
        .cloned()
        .collect()
}

/// The command kinds an order trace can show from the input log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderEventKind {
    NewOrder,
    Cancel,
    Halt,
    Resume,
}

impl OrderEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewOrder => "new_order",
            Self::Cancel => "cancel",
            Self::Halt => "halt",
            Self::Resume => "resume",
        }
    }
}

impl From<CommandKind> for OrderEventKind {
    fn from(kind: CommandKind) -> Self {
        match kind {
            CommandKind::NewOrder => Self::NewOrder,
            CommandKind::Cancel => Self::Cancel,
            CommandKind::Halt => Self::Halt,
            CommandKind::Resume => Self::Resume,
        }
    }
}

/// One decoded input-log record that names the traced order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderEvent {
    /// Offset in the orders topic partition.
    pub offset: u64,
    pub kind: OrderEventKind,
    pub symbol: Symbol,
    pub participant: ParticipantId,
    /// Price in integer ticks (the protocol calls it a price; the CLI contract
    /// names the column `price_cents`). `NewOrder` only: cancel and control
    /// commands carry no price.
    pub price_cents: Option<i64>,
    /// Quantity in integer units. `NewOrder` only.
    pub qty: Option<u64>,
    pub timestamp_ns: u64,
}

/// How an execution report names the traced order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecRole {
    /// `order_id` is the traced order: its own acceptance, fill, or cancel.
    Order,
    /// `maker_order_id` is the traced order: this is the aggressor's report of
    /// a fill against the traced order's resting quantity.
    Maker,
}

/// One execution report that names the traced order on either side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecEvent {
    /// Per-symbol monotonic execution sequence.
    pub seq: u64,
    /// Orders-log offset that caused the report.
    pub cause_offset: u64,
    pub symbol: Symbol,
    pub kind: ExecKind,
    pub price: i64,
    pub qty: u64,
    /// For [`ExecRole::Maker`], this is the aggressor's remaining quantity: the
    /// wire report does not carry the maker side's remainder.
    pub remaining: u64,
    /// The report's `order_id`.
    pub order_id: OrderId,
    /// The report's `maker_order_id`.
    pub maker_order_id: OrderId,
    pub reason: RejectReason,
    pub timestamp_ns: u64,
    pub role: ExecRole,
}

impl ExecEvent {
    /// The stable execution identity: `(symbol, cause_offset, seq, kind)`.
    pub fn identity(&self) -> (Symbol, u64, u64, ExecKind) {
        (self.symbol, self.cause_offset, self.seq, self.kind)
    }
}

/// One order's reconstructed lifecycle across the orders and executions logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderTrace {
    pub order_id: OrderId,
    pub partition: u32,
    /// Input-log events that name the order, ordered by offset.
    pub orders: Vec<OrderEvent>,
    /// Executions that name the order as aggressor or maker, ordered by
    /// `(cause_offset, seq)`.
    pub executions: Vec<ExecEvent>,
    /// Unique, sorted cause offsets that do not resolve to a decoded
    /// order/cancel record on this partition.
    pub unresolved_cause_offsets: Vec<u64>,
    /// A terminal report (`filled`, `rejected`, `cancelled`) names the order id
    /// itself. A maker-side fill is not terminal evidence: the wire report does
    /// not carry the maker's remaining quantity.
    pub complete: bool,
}

/// Build one order's trace from already-decoded records.
///
/// `orders` is every decoded command on the partition with its orders-topic
/// offset; `executions` is every decoded report. Both are filtered by
/// `symbols` when it is `Some`. Verification is always computed, so a caller
/// can decide whether an unresolved cause offset is fatal (`--verify`).
pub fn build_order_trace(
    order_id: OrderId,
    partition: u32,
    symbols: Option<&[Symbol]>,
    orders: &[(u64, Command)],
    executions: &[ExecutionReport],
) -> OrderTrace {
    let allowed = |symbol: Symbol| symbols.is_none_or(|symbols| symbols.contains(&symbol));

    let mut order_events = Vec::new();
    for (offset, command) in orders {
        if command.order_id != order_id || !allowed(command.symbol) {
            continue;
        }
        let kind = OrderEventKind::from(command.kind);
        let (price_cents, qty) = match kind {
            OrderEventKind::NewOrder => (Some(command.price.0), Some(command.qty.0)),
            OrderEventKind::Cancel | OrderEventKind::Halt | OrderEventKind::Resume => (None, None),
        };
        order_events.push(OrderEvent {
            offset: *offset,
            kind,
            symbol: command.symbol,
            participant: command.participant,
            price_cents,
            qty,
            timestamp_ns: command.timestamp_ns,
        });
    }
    order_events.sort_unstable_by_key(|event| event.offset);

    let mut exec_events = Vec::new();
    for report in executions {
        if !allowed(report.symbol) {
            continue;
        }
        let role = if report.order_id == order_id {
            ExecRole::Order
        } else if report.maker_order_id == order_id {
            ExecRole::Maker
        } else {
            continue;
        };
        exec_events.push(ExecEvent {
            seq: report.seq.0,
            cause_offset: report.cause_offset,
            symbol: report.symbol,
            kind: report.kind,
            price: report.price.0,
            qty: report.qty.0,
            remaining: report.remaining.0,
            order_id: report.order_id,
            maker_order_id: report.maker_order_id,
            reason: report.reason,
            timestamp_ns: report.timestamp_ns,
            role,
        });
    }
    exec_events.sort_unstable_by_key(|event| (event.cause_offset, event.seq));

    // Resolve only the offsets the traced executions actually reference. A
    // full offset->command map cost one allocation per order on a million-
    // record partition to answer a handful of lookups.
    let mut command_at: HashMap<u64, Option<&Command>> = HashMap::with_capacity(exec_events.len());
    for event in &exec_events {
        command_at.entry(event.cause_offset).or_insert(None);
    }
    if !command_at.is_empty() {
        for (offset, command) in orders {
            if let Some(slot) = command_at.get_mut(offset) {
                *slot = Some(command);
            }
        }
    }

    // A cause offset must land on a decoded NewOrder or Cancel record for the
    // same symbol. Halt and resume emit market data, never execution reports,
    // so a control record at the cause offset is evidence of a bad record or a
    // bad reader, not a resolved cause.
    let mut unresolved_cause_offsets = Vec::new();
    for event in &exec_events {
        let resolved = command_at
            .get(&event.cause_offset)
            .and_then(|command| *command)
            .is_some_and(|command| {
                matches!(command.kind, CommandKind::NewOrder | CommandKind::Cancel)
                    && command.symbol == event.symbol
            });
        if !resolved {
            unresolved_cause_offsets.push(event.cause_offset);
        }
    }
    unresolved_cause_offsets.sort_unstable();
    unresolved_cause_offsets.dedup();

    let complete = exec_events.iter().any(|event| {
        event.role == ExecRole::Order
            && matches!(
                event.kind,
                ExecKind::Filled | ExecKind::Rejected | ExecKind::Cancelled
            )
    });

    OrderTrace {
        order_id,
        partition,
        orders: order_events,
        executions: exec_events,
        unresolved_cause_offsets,
        complete,
    }
}

/// Parse an order id as a `u128` decimal or `0x`-prefixed hex.
///
/// A bare hex value is also accepted when it contains a hex letter, because
/// [`OrderId`]'s `Display`/`Serialize` emit 32 hex digits without the prefix. A
/// bare all-digit value is decimal so the documented decimal form is never
/// reinterpreted.
pub fn parse_order_id(value: &str) -> Result<OrderId, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("order id is empty; pass a u128 decimal or 0x-prefixed hex".to_string());
    }
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        if hex.is_empty() {
            return Err(format!("invalid 0x-hex order id: {value}"));
        }
        return u128::from_str_radix(hex, 16)
            .map(OrderId)
            .map_err(|_| format!("invalid 0x-hex order id: {value}"));
    }
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value
            .parse::<u128>()
            .map(OrderId)
            .map_err(|_| format!("order id does not fit in 128 bits: {value}"));
    }
    if value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return u128::from_str_radix(value, 16)
            .map(OrderId)
            .map_err(|_| format!("invalid hex order id: {value}"));
    }
    Err(format!(
        "invalid order id: {value} (expected a u128 decimal or 0x-prefixed hex)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::ids::{Price, Qty, Seq};
    use whippet_core::protocol::{FLAG_TAKER, OrderType, Side};

    const GATEWAY_RECORD: &str = r#"{
        "operator": "lead",
        "action": "halt",
        "symbol": "BTC-USD",
        "halted": true,
        "reason": "volatility spike",
        "order_offset": 42,
        "timestamp_ns": 1700000000000000000,
        "service": "whippet"
    }"#;

    fn record(
        partition: u32,
        offset: u64,
        operator: &str,
        action: &str,
        symbol: &str,
    ) -> AuditRecord {
        AuditRecord {
            partition,
            offset,
            payload: AuditPayload {
                operator: operator.into(),
                action: action.into(),
                symbol: symbol.into(),
                halted: action == "halt",
                reason: None,
                order_offset: offset * 10,
                timestamp_ns: offset,
                service: "whippet".into(),
            },
            raw: Vec::new(),
        }
    }

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD").expect("static symbol")
    }

    #[test]
    fn audit_decodes_the_gateway_payload_and_ignores_extra_fields() {
        let record =
            AuditRecord::decode(2, 7, GATEWAY_RECORD.as_bytes()).expect("valid audit JSON");
        assert_eq!(record.partition, 2);
        assert_eq!(record.offset, 7);
        assert_eq!(record.raw, GATEWAY_RECORD.as_bytes());
        assert_eq!(record.payload.operator, "lead");
        assert_eq!(record.payload.action, "halt");
        assert_eq!(record.payload.symbol, "BTC-USD");
        assert!(record.payload.halted);
        assert_eq!(record.payload.reason.as_deref(), Some("volatility spike"));
        assert_eq!(record.payload.order_offset, 42);
        assert_eq!(record.payload.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(record.payload.service, "whippet");

        // The `--json` shape: the gateway fields plus the topic position, and
        // no dump of the raw payload bytes.
        let json = serde_json::to_value(&record).expect("audit record serializes");
        assert_eq!(json["partition"], 2);
        assert_eq!(json["offset"], 7);
        assert_eq!(json["operator"], "lead");
        assert_eq!(json["halted"], true);
        assert_eq!(json["order_offset"], 42);
        assert!(json.get("raw").is_none());

        // A null reason decodes as None, and a field the reader does not know
        // about is ignored rather than rejected.
        let null_reason = r#"{
            "operator": "risk",
            "action": "resume",
            "symbol": "ETH-USD",
            "halted": false,
            "reason": null,
            "order_offset": 1,
            "timestamp_ns": 2,
            "service": "whippet",
            "trace_id": "abc-123"
        }"#;
        let record = AuditRecord::decode(0, 0, null_reason.as_bytes()).expect("forward-compatible");
        assert_eq!(record.payload.reason, None);
        assert!(!record.payload.halted);
    }

    #[test]
    fn audit_filter_applies_every_axis_and_a_limit() {
        let records = vec![
            record(0, 0, "lead", "halt", "BTC-USD"),
            record(0, 1, "risk", "resume", "BTC-USD"),
            record(1, 0, "lead", "halt", "ETH-USD"),
        ];

        let all = filter_audit(&records, &AuditFilter::default());
        assert_eq!(all.len(), 3);

        let by_operator = filter_audit(
            &records,
            &AuditFilter {
                operator: Some("lead".into()),
                ..AuditFilter::default()
            },
        );
        assert_eq!(by_operator.len(), 2);

        let by_action = filter_audit(
            &records,
            &AuditFilter {
                action: Some("RESUME".into()),
                ..AuditFilter::default()
            },
        );
        assert_eq!(by_action.len(), 1);
        assert_eq!(by_action[0].payload.operator, "risk");

        let by_symbol = filter_audit(
            &records,
            &AuditFilter {
                symbol: Some("btc-usd".into()),
                ..AuditFilter::default()
            },
        );
        assert_eq!(by_symbol.len(), 2);

        // `min_offset` is the audit-topic offset of each record.
        let by_offset = filter_audit(
            &records,
            &AuditFilter {
                min_offset: Some(1),
                ..AuditFilter::default()
            },
        );
        assert_eq!(by_offset.len(), 1);
        assert_eq!(by_offset[0].partition, 0);
        assert_eq!(by_offset[0].offset, 1);

        let limited = filter_audit(
            &records,
            &AuditFilter {
                operator: Some("lead".into()),
                limit: Some(1),
                ..AuditFilter::default()
            },
        );
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].payload.symbol, "BTC-USD");

        let empty = filter_audit(
            &records,
            &AuditFilter {
                operator: Some("nobody".into()),
                ..AuditFilter::default()
            },
        );
        assert!(empty.is_empty());
    }

    #[test]
    fn parse_order_id_accepts_decimal_and_hex_forms() {
        assert_eq!(parse_order_id("170").expect("decimal"), OrderId(170));
        assert_eq!(
            parse_order_id("0xdeadBEEF").expect("0x hex"),
            OrderId(0xdead_beef)
        );
        assert_eq!(
            parse_order_id("000000000000000000000000000000aa").expect("bare hex"),
            OrderId(0xaa)
        );
        assert!(parse_order_id("").is_err());
        assert!(parse_order_id("0x").is_err());
        assert!(parse_order_id("not-an-id").is_err());
        assert!(parse_order_id("340282366920938463463374607431768211456").is_err());
    }

    #[test]
    fn trace_separates_own_events_from_maker_legs() {
        let order_id = OrderId(0xaa);
        let aggressor_id = OrderId(0xbb);
        let symbol = symbol();
        let participant = ParticipantId(7);
        let orders = vec![
            (
                0,
                Command::new_order(
                    symbol,
                    Side::Buy,
                    OrderType::Limit,
                    Price(100),
                    Qty(10),
                    order_id,
                    participant,
                    1,
                ),
            ),
            (
                1,
                Command::new_order(
                    symbol,
                    Side::Sell,
                    OrderType::Limit,
                    Price(100),
                    Qty(4),
                    aggressor_id,
                    ParticipantId(8),
                    2,
                ),
            ),
            (2, Command::cancel(symbol, order_id, participant, 3)),
        ];
        let report = |order_id: OrderId,
                      maker_order_id: OrderId,
                      kind: ExecKind,
                      qty: u64,
                      remaining: u64,
                      seq: u64,
                      cause_offset: u64,
                      flags: u16| ExecutionReport {
            kind,
            symbol,
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Price(100),
            qty: Qty(qty),
            remaining: Qty(remaining),
            order_id,
            maker_order_id,
            seq: Seq(seq),
            cause_offset,
            timestamp_ns: seq,
            reason: RejectReason::None,
            flags,
        };
        let executions = vec![
            report(
                aggressor_id,
                order_id,
                ExecKind::Filled,
                4,
                0,
                0,
                1,
                FLAG_TAKER,
            ),
            report(
                order_id,
                OrderId(0),
                ExecKind::PartiallyFilled,
                1,
                9,
                1,
                0,
                0,
            ),
            report(order_id, OrderId(0), ExecKind::Cancelled, 9, 0, 2, 2, 0),
        ];

        let trace = build_order_trace(order_id, 1, None, &orders, &executions);
        assert_eq!(trace.orders.len(), 2);
        assert_eq!(trace.orders[0].offset, 0);
        assert_eq!(trace.orders[0].kind, OrderEventKind::NewOrder);
        assert_eq!(trace.orders[1].offset, 2);
        assert_eq!(trace.orders[1].kind, OrderEventKind::Cancel);
        assert_eq!(trace.orders[1].price_cents, None);

        // Sorted by the offset that caused each event: the own partial, the
        // maker fill, the own cancel.
        assert_eq!(
            trace
                .executions
                .iter()
                .map(|event| event.cause_offset)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(trace.executions[0].role, ExecRole::Order);
        assert_eq!(trace.executions[1].role, ExecRole::Maker);
        assert_eq!(
            trace.executions[1].identity(),
            (symbol, 1, 0, ExecKind::Filled)
        );
        assert!(trace.complete);
        assert!(trace.unresolved_cause_offsets.is_empty());
    }

    #[test]
    fn trace_reports_missing_and_control_cause_offsets() {
        let order_id = OrderId(1);
        let symbol = symbol();
        let orders = vec![
            (
                0,
                Command::new_order(
                    symbol,
                    Side::Buy,
                    OrderType::Limit,
                    Price(100),
                    Qty(1),
                    order_id,
                    ParticipantId(1),
                    1,
                ),
            ),
            (1, Command::halt(symbol, ParticipantId(2), 2)),
        ];
        let execution = |cause_offset: u64| ExecutionReport {
            kind: ExecKind::Accepted,
            symbol,
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Price(100),
            qty: Qty(1),
            remaining: Qty(1),
            order_id,
            maker_order_id: OrderId(0),
            seq: Seq(cause_offset),
            cause_offset,
            timestamp_ns: cause_offset,
            reason: RejectReason::None,
            flags: 0,
        };
        // Offset 9 is absent; offset 1 is a halt, which never causes an
        // execution report.
        let trace = build_order_trace(order_id, 0, None, &orders, &[execution(9), execution(1)]);
        assert_eq!(trace.unresolved_cause_offsets, vec![1, 9]);
        assert!(!trace.complete);
    }

    #[test]
    fn trace_honours_the_symbol_filter() {
        let order_id = OrderId(5);
        let btc = symbol();
        let eth = Symbol::new("ETH-USD").expect("static symbol");
        let orders = vec![
            (
                0,
                Command::new_order(
                    btc,
                    Side::Buy,
                    OrderType::Limit,
                    Price(100),
                    Qty(1),
                    order_id,
                    ParticipantId(1),
                    1,
                ),
            ),
            (
                1,
                Command::new_order(
                    eth,
                    Side::Buy,
                    OrderType::Limit,
                    Price(100),
                    Qty(1),
                    order_id,
                    ParticipantId(1),
                    2,
                ),
            ),
        ];
        let trace = build_order_trace(order_id, 0, Some(&[btc]), &orders, &[]);
        assert_eq!(trace.orders.len(), 1);
        assert_eq!(trace.orders[0].symbol, btc);
    }
}
