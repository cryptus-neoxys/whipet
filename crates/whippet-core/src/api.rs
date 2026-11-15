// Licensed under the Apache License, Version 2.0. See LICENSE.

//! JSON DTOs for the HTTP API, WebSocket feed, and dashboard.
//!
//! Prices are integer cents and quantities are integer units. The browser
//! formats them; nothing in the API uses a float for money.

use serde::{Deserialize, Serialize};

use crate::config::Mode;
use crate::ids::{OrderId, Seq, Symbol};
use crate::protocol::{OrderType, Side};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: Symbol,
    pub side: Side,
    #[serde(default = "default_order_type")]
    pub order_type: OrderType,
    /// Required for every order type except `market`.
    #[serde(default)]
    pub price_cents: Option<i64>,
    pub qty: u64,
    /// Client-supplied idempotency key. The gateway also deduplicates inside
    /// the engine; this is the business key.
    #[serde(default)]
    pub client_order_id: Option<String>,
    /// Stable participant name. Hashed to the engine's integer participant id.
    #[serde(default)]
    pub participant: Option<String>,
}

fn default_order_type() -> OrderType {
    OrderType::Limit
}

/// Borrowed ingress DTO for the single-order hot path. Same JSON shape as
/// [`OrderRequest`], but `symbol`, `client_order_id`, and `participant` borrow
/// from the request body instead of allocating a `String` each.
///
/// `serde_json` borrows `&str` directly for unescaped input; input with string
/// escapes cannot borrow and is rejected at extraction time, so producers
/// should send plain idempotency keys and participant names (the hot-path
/// norm). The gateway validates a `OrderRequestRef` into the same
/// `ValidatedOrder` as the owned form, so the JSON contract is unchanged.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct OrderRequestRef<'a> {
    pub symbol: &'a str,
    pub side: Side,
    #[serde(default = "default_order_type")]
    pub order_type: OrderType,
    /// Required for every order type except `market`.
    #[serde(default)]
    pub price_cents: Option<i64>,
    pub qty: u64,
    /// Client-supplied idempotency key. The order id is derived from this
    /// slice before any owned conversion, so present keys never allocate.
    #[serde(default)]
    pub client_order_id: Option<&'a str>,
    /// Stable participant name. Hashed to the engine's integer participant id.
    #[serde(default)]
    pub participant: Option<&'a str>,
}

/// Borrowed ingress DTO for the batch hot path. Same JSON shape as
/// [`BatchOrderRequest`]; only the per-order strings borrow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOrderRequestRef<'a> {
    #[serde(borrow)]
    pub orders: Vec<OrderRequestRef<'a>>,
}

/// Iggy 0.10's default `message_bus.max_batch`; the gateway and bench use the
/// same cap so a batch is one append, not a silent split.
pub const MAX_BATCH_ORDERS: usize = 256;

/// Batch ingress request. Phase 1 requires one symbol per batch so the gateway
/// can make exactly one Iggy append; mixed-symbol batches are rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOrderRequest {
    pub orders: Vec<OrderRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOrderAck {
    pub partition: u32,
    /// Durable: the broker offset of the first order. Turbo: the local seq of
    /// the first order; the per-order `offset` fields are authoritative and
    /// are not necessarily contiguous across concurrent batches.
    pub base_offset: u64,
    pub count: u32,
    pub mode: Mode,
    /// `accepted_by_log` (durable) or `matched_unjournaled` (turbo).
    pub guarantee: String,
    pub durable_ack_ms: f64,
    /// One ack per input order, with offsets `base_offset + i`.
    pub orders: Vec<OrderAck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub symbol: Symbol,
    pub order_id: OrderId,
    #[serde(default)]
    pub participant: Option<String>,
}

/// Durable ack label: the order record is in the Iggy log.
pub const GUARANTEE_DURABLE: &str = "accepted_by_log";

/// Turbo ack label: the order was matched, but its record is still in the
/// write-behind window and can be lost on a crash.
pub const GUARANTEE_TURBO: &str = "matched_unjournaled";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAck {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub partition: u32,
    /// Durable: the broker offset. Turbo: the local match sequence.
    pub offset: u64,
    pub mode: Mode,
    pub status: String,
    /// `accepted_by_log` (durable) or `matched_unjournaled` (turbo).
    pub guarantee: String,
    /// Engine reject reason for a turbo ack; `None` when accepted.
    pub reject: Option<String>,
    pub accepted_at_ns: u64,
    /// Milliseconds the gateway spent getting the acknowledgement: the Iggy
    /// append in durable mode, the match in turbo mode.
    pub durable_ack_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAck {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub partition: u32,
    pub offset: u64,
    pub mode: Mode,
    pub status: String,
    pub guarantee: String,
}

/// Operator halt/resume request body. The reason is free text recorded
/// verbatim in the audit log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRequest {
    #[serde(default)]
    pub reason: Option<String>,
}

/// Halt/resume ack.
///
/// `offset` is the command's position in the Orders log (durable) or the
/// matcher's local seq (turbo) — the same offset domain the order acks use.
/// `audit_offset` is the audit record's offset in the Audit topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminAck {
    pub symbol: Symbol,
    /// `halt` or `resume`.
    pub action: String,
    /// The instrument's state after the command.
    pub halted: bool,
    pub partition: u32,
    pub offset: u64,
    /// The operator identity the presented token acted as.
    pub operator: String,
    pub audit_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelView {
    pub price_cents: i64,
    pub qty: u64,
    pub orders: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeView {
    pub symbol: Symbol,
    pub price_cents: i64,
    pub qty: u64,
    pub side: Side,
    pub order_id: OrderId,
    pub ts_ns: u64,
    pub seq: Seq,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookView {
    pub symbol: Symbol,
    pub seq: Seq,
    pub bids: Vec<LevelView>,
    pub asks: Vec<LevelView>,
    pub best_bid_cents: Option<i64>,
    pub best_ask_cents: Option<i64>,
    pub spread_cents: Option<i64>,
    pub mid_cents: Option<f64>,
    pub last_trade_price_cents: Option<i64>,
    pub last_trade_qty: u64,
    pub last_trade_side: Option<Side>,
    pub depth_bid_qty: u64,
    pub depth_ask_qty: u64,
    /// True while the instrument is halted by an operator command. The market
    /// data hub copies it from the engine's halt state.
    #[serde(default)]
    pub halted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsView {
    pub mode: Mode,
    pub orders_ingressed: u64,
    pub orders_matched: u64,
    pub orders_rejected: u64,
    pub executions: u64,
    pub trades: u64,
    pub md_events: u64,
    pub engine_commands: u64,
    pub consumer_lag: u64,
    pub ingress_p50_us: f64,
    pub ingress_p99_us: f64,
    pub match_p50_us: f64,
    pub match_p99_us: f64,
    /// Rolling-window ingress percentiles (process-lifetime values above).
    pub ingress_window_p50_us: f64,
    pub ingress_window_p99_us: f64,
    pub match_window_p50_us: f64,
    pub match_window_p99_us: f64,
    pub window_secs: u64,
    pub throughput_orders_s: f64,
    pub uptime_s: u64,
    pub iggy_connected: bool,
    pub partition_count: u32,
}

impl MetricsView {
    pub fn empty(mode: Mode, partitions: u32) -> Self {
        Self {
            mode,
            orders_ingressed: 0,
            orders_matched: 0,
            orders_rejected: 0,
            executions: 0,
            trades: 0,
            md_events: 0,
            engine_commands: 0,
            consumer_lag: 0,
            ingress_p50_us: 0.0,
            ingress_p99_us: 0.0,
            match_p50_us: 0.0,
            match_p99_us: 0.0,
            ingress_window_p50_us: 0.0,
            ingress_window_p99_us: 0.0,
            match_window_p50_us: 0.0,
            match_window_p99_us: 0.0,
            window_secs: 0,
            throughput_orders_s: 0.0,
            uptime_s: 0,
            iggy_connected: false,
            partition_count: partitions,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateView {
    pub mode: Mode,
    pub venue: String,
    pub symbols: Vec<BookView>,
    pub trades: Vec<TradeView>,
    pub metrics: MetricsView,
    /// Age of the newest market-data event, in milliseconds.
    pub freshness_ms: u64,
    /// Newest per-symbol sequence observed by the MD hub.
    pub sequence: u64,
    pub iggy_addr: String,
    pub journal: String,
    pub guarantee: String,
    pub dashboard_build: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthView {
    pub status: String,
    pub mode: Mode,
    pub iggy_connected: bool,
    pub iggy_addr: String,
    pub journal: String,
    pub shards_ok: bool,
    /// False at the critical disk threshold: ingress is refused until space is
    /// reclaimed or the operator raises the limit deliberately.
    pub storage_ok: bool,
    pub storage_state: String,
    pub version: String,
    pub uptime_s: u64,
    pub partitions: u32,
    /// True when ingress requires a bearer token. False means order submission
    /// and cancel are anonymous; that is a demo posture, not a safe default.
    pub auth_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub error: String,
}

impl ApiError {
    pub fn new(code: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            error: error.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsServerMessage {
    State { state: StateView },
    Trade { trade: TradeView },
    Metrics { metrics: MetricsView },
    Notice { level: String, message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMessage {
    Subscribe { symbols: Vec<Symbol> },
    Ping { sent_at_ns: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_request_defaults_to_limit() {
        let json = r#"{"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":10}"#;
        let request: OrderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.order_type, OrderType::Limit);
    }

    #[test]
    fn batch_order_request_round_trips() {
        let json = r#"{"orders":[
            {"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":10},
            {"symbol":"BTC-USD","side":"sell","price_cents":6500100,"qty":4}
        ]}"#;
        let request: BatchOrderRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.orders.len(), 2);
        assert_eq!(request.orders[0].symbol.as_str(), "BTC-USD");
    }

    #[test]
    fn admin_request_defaults_to_no_reason() {
        let request: AdminRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(request.reason, None);
        let request: AdminRequest = serde_json::from_str(r#"{"reason":"fat finger"}"#).unwrap();
        assert_eq!(request.reason.as_deref(), Some("fat finger"));
    }

    #[test]
    fn ws_messages_are_tagged() {
        let message = WsServerMessage::Notice {
            level: "info".into(),
            message: "connected".into(),
        };
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("\"type\":\"notice\""));
    }

    #[test]
    fn borrowed_request_matches_owned_for_the_same_json() {
        let json = r#"{"symbol":"BTC-USD","side":"buy","order_type":"limit","price_cents":6500000,"qty":10,"client_order_id":"abc-1","participant":"alice"}"#;
        let owned: OrderRequest = serde_json::from_str(json).unwrap();
        let borrowed: OrderRequestRef<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(borrowed.symbol, owned.symbol.as_str());
        assert_eq!(borrowed.side, owned.side);
        assert_eq!(borrowed.order_type, owned.order_type);
        assert_eq!(borrowed.price_cents, owned.price_cents);
        assert_eq!(borrowed.qty, owned.qty);
        assert_eq!(borrowed.client_order_id, owned.client_order_id.as_deref());
        assert_eq!(borrowed.participant, owned.participant.as_deref());
    }

    #[test]
    fn borrowed_request_applies_the_same_defaults_as_owned() {
        let json = r#"{"symbol":"BTC-USD","side":"sell","price_cents":6500100,"qty":4}"#;
        let owned: OrderRequest = serde_json::from_str(json).unwrap();
        let borrowed: OrderRequestRef<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(owned.order_type, OrderType::Limit);
        assert_eq!(borrowed.order_type, OrderType::Limit);
        assert_eq!(borrowed.client_order_id, None);
        assert_eq!(borrowed.participant, None);
        assert_eq!(borrowed.price_cents, owned.price_cents);
    }

    #[test]
    fn borrowed_batch_matches_owned_for_the_same_json() {
        let json = r#"{"orders":[
            {"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":10,"client_order_id":"b-1","participant":"alice"},
            {"symbol":"BTC-USD","side":"sell","price_cents":6500100,"qty":4}
        ]}"#;
        let owned: BatchOrderRequest = serde_json::from_str(json).unwrap();
        let borrowed: BatchOrderRequestRef<'_> = serde_json::from_str(json).unwrap();
        assert_eq!(borrowed.orders.len(), owned.orders.len());
        for (borrowed, owned) in borrowed.orders.iter().zip(owned.orders.iter()) {
            assert_eq!(borrowed.symbol, owned.symbol.as_str());
            assert_eq!(borrowed.side, owned.side);
            assert_eq!(borrowed.order_type, owned.order_type);
            assert_eq!(borrowed.price_cents, owned.price_cents);
            assert_eq!(borrowed.qty, owned.qty);
            assert_eq!(borrowed.client_order_id, owned.client_order_id.as_deref());
            assert_eq!(borrowed.participant, owned.participant.as_deref());
        }
    }
}
