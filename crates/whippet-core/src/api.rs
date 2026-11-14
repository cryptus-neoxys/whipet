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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelRequest {
    pub symbol: Symbol,
    pub order_id: OrderId,
    #[serde(default)]
    pub participant: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderAck {
    pub order_id: OrderId,
    pub symbol: Symbol,
    pub partition: u32,
    pub offset: u64,
    pub mode: Mode,
    pub status: String,
    pub accepted_at_ns: u64,
    /// Milliseconds the gateway spent getting the Iggy acknowledgement.
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
    pub guarantee: String,
    pub dashboard_build: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthView {
    pub status: String,
    pub mode: Mode,
    pub iggy_connected: bool,
    pub iggy_addr: String,
    pub version: String,
    pub uptime_s: u64,
    pub partitions: u32,
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
    fn ws_messages_are_tagged() {
        let message = WsServerMessage::Notice {
            level: "info".into(),
            message: "connected".into(),
        };
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains("\"type\":\"notice\""));
    }
}
