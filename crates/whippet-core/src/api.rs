// Licensed under the Apache License, Version 2.0. See LICENSE.

//! JSON DTOs for the HTTP API, WebSocket feed, and dashboard.
//!
//! Prices are integer cents and quantities are integer units. The browser
//! formats them; nothing in the API uses a float for money.

use serde::{Deserialize, Serialize};

use crate::config::Mode;
use crate::ids::{OrderId, ParticipantId, Seq, Symbol};
use crate::protocol::{MdKind, OrderType, SessionState, Side};

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

/// Amend request body. The order id is unchanged; `price_cents` and `qty` are
/// the replacement price in quote cents and new remaining quantity. The engine
/// refuses a size increase and a price that would cross the opposite best.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmendRequest {
    pub symbol: Symbol,
    pub order_id: OrderId,
    pub price_cents: i64,
    pub qty: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmendAck {
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

/// Operator session-change request body: `{"state":"pre_open"|"open"|"closed"}`
/// with an optional free-text `reason` recorded verbatim in the audit log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRequest {
    /// The session the instrument moves to.
    pub state: SessionState,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Operator risk-limit request body:
/// `{"participant":"desk-a","max_net_qty":500,"max_open_buy_notional_cents":25000000}`
/// plus an optional free-text `reason`.
///
/// Both bounds are required so one request states the whole replacement:
/// omitting a field would otherwise silently disable that bound. Zero
/// disables a bound explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskRequest {
    /// Stable participant name; derived to the engine's integer id at the
    /// edge, exactly as order ingress does.
    pub participant: String,
    /// Absolute net-position bound in quantity units; zero disables.
    pub max_net_qty: u64,
    /// Resting buy notional bound in quote cents; zero disables.
    pub max_open_buy_notional_cents: u64,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Halt/resume/session/risk ack.
///
/// `offset` is the command's position in the Orders log (durable) or the
/// matcher's local seq (turbo) — the same offset domain the order acks use.
/// `audit_offset` is the audit record's offset in the Audit topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminAck {
    pub symbol: Symbol,
    /// `halt`, `resume`, `set_session`, or `set_risk`.
    pub action: String,
    /// The instrument's halt state after a `halt`/`resume` command. A
    /// `set_session` command does not touch the halt flag, so its ack always
    /// reports `false` here — read the live halt state from `/api/state` or
    /// the MD feed instead of this field.
    pub halted: bool,
    /// The instrument's session after a `set_session` command; `None` for
    /// every other action, which does not change the session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionState>,
    /// `set_risk` only: the participant the limits apply to, as presented.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
    /// `set_risk` only: absolute net-position bound after the command; zero
    /// means the bound is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_net_qty: Option<u64>,
    /// `set_risk` only: resting buy notional bound in quote cents after the
    /// command; zero means the bound is disabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_open_buy_notional_cents: Option<u64>,
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
    /// The instrument's trading session, copied by the market data hub from
    /// the engine's status events and snapshots. `open` is the default, so a
    /// frame from a node that predates sessions decodes as trading.
    #[serde(default)]
    pub session: SessionState,
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
    /// True only when every active shard's engine **and** MD task is running.
    pub shards_ok: bool,
    /// Per-shard lifecycle, one entry per active shard. Empty on a node that
    /// runs no local shards; additive, so an older client can ignore it.
    #[serde(default)]
    pub shards: Vec<ShardHealthView>,
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

/// One active shard's engine and market-data lifecycle, as `starting`,
/// `running`, `failed`, or `stopped`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardHealthView {
    pub partition: u32,
    pub engine: String,
    pub md: String,
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

/// One frame of the sequenced market-data feed.
///
/// The dashboard keeps its 20 Hz conflated [`StateView`]; this is the separate
/// contract for a consumer that must see every change, with a sequence scope
/// per symbol:
///
/// 1. Subscribe and read a [`MdFeedSnapshot`] at `seq`.
/// 2. Apply [`MdFeedDelta`] frames with `seq > snapshot.seq` in order.
/// 3. On a gap (or a slow-consumer drop) request a new snapshot; the feed may
///    also emit [`MdFeedResync`] to say "discard local state, resnapshot".
///
/// `seq` is the engine's per-symbol sequence, so it is comparable across
/// snapshots and deltas for the same symbol and never across symbols.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MdFeedMessage {
    Snapshot(MdFeedSnapshot),
    Delta(MdFeedDelta),
    Resync(MdFeedResync),
}

/// Point-in-time image of one symbol's book at `seq`.
///
/// This is the *full* price-level book: every level the hub holds, not the
/// dashboard's depth-limited [`BookView`] window. A consumer that started
/// from a truncated snapshot could never reconstruct the deeper levels from
/// later deltas, so completeness beats frame size here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdFeedSnapshot {
    pub symbol: Symbol,
    /// Per-symbol sequence of the last event folded into this image.
    pub seq: u64,
    /// Input offset that produced `seq`; `0` before the first event.
    pub cause_offset: u64,
    /// Every bid level, best (highest) price first.
    pub bids: Vec<LevelView>,
    /// Every ask level, best (lowest) price first.
    pub asks: Vec<LevelView>,
    pub halted: bool,
}

/// One applied market-data delta in the engine's shape.
///
/// The only transformation from [`crate::protocol::MdEvent`] is that `price`
/// ticks become `price_cents`. Every accepted event advances the symbol's
/// sequence, including `MdKind::Status` and `MdKind::Snapshot`
/// (book-replacing image) events; a consumer that skips a kind would see a
/// hole in `seq`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdFeedDelta {
    pub symbol: Symbol,
    pub seq: u64,
    /// Input offset that caused this event.
    pub cause_offset: u64,
    pub timestamp_ns: u64,
    pub kind: MdKind,
    pub side: Side,
    pub flags: u8,
    /// Price in integer cents, already scaled by the instrument tick size.
    pub price_cents: i64,
    pub qty: u64,
    pub order_id: OrderId,
    pub participant: ParticipantId,
}

/// Instruction to drop local book state and take a fresh snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MdFeedResync {
    pub symbol: Symbol,
    /// The sequence the consumer should snapshot at or after.
    pub seq: u64,
    /// Operator-readable cause, for example `lagged` or `gap`.
    pub reason: String,
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
    fn session_request_parses_the_documented_states() {
        for (json, expected) in [
            (r#"{"state":"pre_open"}"#, SessionState::PreOpen),
            (r#"{"state":"open"}"#, SessionState::Open),
            (r#"{"state":"closed"}"#, SessionState::Closed),
        ] {
            let request: SessionRequest = serde_json::from_str(json).unwrap();
            assert_eq!(request.state, expected);
            assert_eq!(request.reason, None);
        }
        let request: SessionRequest =
            serde_json::from_str(r#"{"state":"closed","reason":"end of day"}"#).unwrap();
        assert_eq!(request.reason.as_deref(), Some("end of day"));
        // Anything else is a 422 at the gateway, not a silent default.
        assert!(serde_json::from_str::<SessionRequest>(r#"{"state":"auction"}"#).is_err());
    }

    #[test]
    fn book_view_defaults_to_an_open_session_for_old_frames() {
        let json = r#"{"symbol":"BTC-USD","seq":1,"bids":[],"asks":[],"best_bid_cents":null,"best_ask_cents":null,"spread_cents":null,"mid_cents":null,"last_trade_price_cents":null,"last_trade_qty":0,"last_trade_side":null,"depth_bid_qty":0,"depth_ask_qty":0,"halted":false}"#;
        let view: BookView = serde_json::from_str(json).unwrap();
        assert_eq!(view.session, SessionState::Open);
    }

    #[test]
    fn admin_ack_carries_the_session_only_for_set_session() {
        let halt = AdminAck {
            symbol: Symbol::new("BTC-USD").unwrap(),
            action: "halt".into(),
            halted: true,
            session: None,
            participant: None,
            max_net_qty: None,
            max_open_buy_notional_cents: None,
            partition: 3,
            offset: 4,
            operator: "ops-lead".into(),
            audit_offset: 5,
        };
        let json = serde_json::to_value(&halt).unwrap();
        // Halt/resume acks keep their exact old shape.
        assert!(json.get("session").is_none());
        assert_eq!(json["halted"], true);
        let ack = AdminAck {
            action: "set_session".into(),
            halted: false,
            session: Some(SessionState::PreOpen),
            ..halt.clone()
        };
        let json = serde_json::to_value(&ack).unwrap();
        assert_eq!(json["session"], "pre_open");
        assert_eq!(json["halted"], false);

        let risk = AdminAck {
            action: "set_risk".into(),
            halted: false,
            session: None,
            participant: Some("desk-a".into()),
            max_net_qty: Some(500),
            max_open_buy_notional_cents: Some(25_000_000),
            ..halt
        };
        let json = serde_json::to_value(&risk).unwrap();
        assert_eq!(json["participant"], "desk-a");
        assert_eq!(json["max_net_qty"], 500);
        assert_eq!(json["max_open_buy_notional_cents"], 25_000_000);
        assert!(json.get("session").is_none());
    }

    #[test]
    fn risk_request_requires_both_bounds_and_ignores_an_absent_reason() {
        let request: RiskRequest = serde_json::from_str(
            r#"{"participant":"desk-a","max_net_qty":500,"max_open_buy_notional_cents":0}"#,
        )
        .unwrap();
        assert_eq!(request.participant, "desk-a");
        assert_eq!(request.max_net_qty, 500);
        assert_eq!(request.max_open_buy_notional_cents, 0);
        assert_eq!(request.reason, None);
        assert!(
            serde_json::from_str::<RiskRequest>(r#"{"participant":"desk-a","max_net_qty":500}"#)
                .is_err(),
            "an omitted bound must not silently become zero"
        );
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
    fn feed_messages_are_tagged_and_round_trip() {
        let delta = MdFeedMessage::Delta(MdFeedDelta {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: 7,
            cause_offset: 11,
            timestamp_ns: 22,
            kind: MdKind::Add,
            side: Side::Buy,
            flags: 0,
            price_cents: 6_500_000,
            qty: 10,
            order_id: OrderId(1),
            participant: ParticipantId(2),
        });
        let json = serde_json::to_string(&delta).unwrap();
        assert!(json.starts_with(r#"{"type":"delta","#), "{json}");
        match serde_json::from_str::<MdFeedMessage>(&json).unwrap() {
            MdFeedMessage::Delta(parsed) => {
                assert_eq!(parsed.seq, 7);
                assert_eq!(parsed.price_cents, 6_500_000);
                assert_eq!(parsed.kind, MdKind::Add);
            }
            other => panic!("expected delta, got {other:?}"),
        }

        let snapshot = MdFeedMessage::Snapshot(MdFeedSnapshot {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: 7,
            cause_offset: 11,
            bids: vec![LevelView {
                price_cents: 6_500_000,
                qty: 10,
                orders: 1,
            }],
            asks: Vec::new(),
            halted: true,
        });
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.starts_with(r#"{"type":"snapshot","#), "{json}");
        match serde_json::from_str::<MdFeedMessage>(&json).unwrap() {
            MdFeedMessage::Snapshot(parsed) => {
                assert_eq!(parsed.bids.len(), 1);
                assert!(parsed.halted);
            }
            other => panic!("expected snapshot, got {other:?}"),
        }

        let resync = MdFeedMessage::Resync(MdFeedResync {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: 9,
            reason: "lagged".into(),
        });
        let json = serde_json::to_string(&resync).unwrap();
        assert!(json.starts_with(r#"{"type":"resync","#), "{json}");
        assert!(matches!(
            serde_json::from_str::<MdFeedMessage>(&json).unwrap(),
            MdFeedMessage::Resync(_)
        ));
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
