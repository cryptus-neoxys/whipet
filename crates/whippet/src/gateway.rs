// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Extension, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use tower_http::compression::CompressionLayer;
use tower_http::compression::predicate::NotForContentType;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;
use tracing::warn;
use whippet_core::api::{
    ApiError, BatchOrderAck, BatchOrderRequest, BatchOrderRequestRef, CancelAck, CancelRequest,
    HealthView, MAX_BATCH_ORDERS, OrderAck, OrderRequest, OrderRequestRef, StateView, TradeView,
    WsClientMessage, WsServerMessage,
};
use whippet_core::headers::{HeaderMode, MessageMeta, SCHEMA_ORDER};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_journal::{JournalMessage, Topic};
use whippet_marketdata::{LatencySummary, Metrics, Stage};

use crate::context::NodeContext;
use crate::ratelimit::RateLimiter;

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

/// Split one request/batch clock: return the microseconds since `cursor` and
/// move the cursor to now. Stages are consecutive slices of the same clock, so
/// a batch records one sample per stage, never one per order.
fn split_elapsed(cursor: &mut Instant) -> u64 {
    let now = Instant::now();
    let micros = now.saturating_duration_since(*cursor).as_micros() as u64;
    *cursor = now;
    micros
}

/// Per-request tracing is off by default: at several thousand requests per
/// second the span overhead buys little over the metrics and the node's own
/// structured logs. Set `WHIPPET_HTTP_TRACE=1` when debugging the gateway.
fn http_trace_enabled() -> bool {
    std::env::var("WHIPPET_HTTP_TRACE")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

pub fn router(ctx: Arc<NodeContext>) -> Router {
    // One limiter shared by HTTP and TCP, so switching transport does not buy
    // a second budget.
    let limiter = Arc::clone(&ctx.rate_limiter);
    let frames = Arc::new(FrameCache::new(Arc::clone(&ctx.metrics)));
    let participants = Arc::new(ParticipantCache::default());
    let ingress = Router::new()
        .route("/api/orders", post(submit_order))
        .route("/api/orders/batch", post(submit_orders))
        .route("/api/cancel", post(cancel_order))
        .layer(middleware::from_fn_with_state(limiter, rate_limit));

    let api = Router::new()
        .merge(ingress)
        .route("/api/state", get(state))
        .route("/api/health", get(health))
        .route("/readyz", get(readyz))
        .route("/api/instruments", get(instruments))
        .route("/api/trades", get(trades))
        .route("/metrics", get(prometheus))
        .route("/ws", get(ws_upgrade));

    let dashboard_dir = ctx.config.dashboard_dir.clone();
    let index = dashboard_dir.join("index.html");
    let api = if index.exists() {
        api.fallback_service(ServeDir::new(&dashboard_dir).not_found_service(ServeFile::new(index)))
    } else {
        api.fallback(missing_dashboard)
    };

    let api = api
        .layer(CompressionLayer::new().compress_when(NotForContentType::new("application/json")))
        .layer(Extension(frames))
        .layer(Extension(participants));
    let api = if http_trace_enabled() {
        api.layer(TraceLayer::new_for_http())
    } else {
        api
    };
    api.with_state(ctx)
}

async fn rate_limit(
    State(limiter): State<Arc<RateLimiter>>,
    connect: Option<Extension<ConnectInfo<SocketAddr>>>,
    request: Request,
    next: Next,
) -> Response {
    // In-process tests and non-TCP callers have no peer address. The limiter is
    // a transport guard, so it does not guess an identity for them.
    if let Some(Extension(ConnectInfo(address))) = connect
        && !limiter.check(address.ip())
    {
        let mut response = (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiError::new(
                "rate_limited",
                "ingress rate limit exceeded; retry shortly",
            )),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return response;
    }
    next.run(request).await
}

/// Upper bound on cached anonymous participant derivations. The table never
/// grows past this: on a full cache it is cleared, so a flood of distinct
/// names costs re-derivation, not memory.
const PARTICIPANT_CACHE_CAP: usize = 4_096;

/// Bounded cache from anonymous participant name to derived id. Deriving is a
/// BLAKE3 hash; caching it keeps the per-order hot path to one hash-map
/// lookup for repeat participants. The authenticated path never touches this:
/// it acts as the single configured participant.
#[derive(Default)]
struct ParticipantCache {
    inner: Mutex<HashMap<String, ParticipantId>>,
}

impl ParticipantCache {
    fn get_or_derive(&self, name: &str) -> ParticipantId {
        if let Some(id) = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(name)
        {
            return *id;
        }
        let derived = ParticipantId::derive(name);
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A concurrent insert may have won the race; reuse it.
        if let Some(existing) = inner.get(name) {
            return *existing;
        }
        if inner.len() >= PARTICIPANT_CACHE_CAP {
            inner.clear();
        }
        inner.insert(name.to_owned(), derived);
        derived
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

/// axum 0.8's `Json` extractor needs `DeserializeOwned`, which rules out the
/// borrowed ingress DTOs. Buffer the body (same 2 MiB cap) and parse with
/// `serde_json::from_slice` so `&str` fields borrow from this buffer.
const JSON_BODY_LIMIT: usize = 2 * 1024 * 1024;

fn payload_too_large() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        Json(ApiError::new(
            "payload_too_large",
            format!("request body must be at most {JSON_BODY_LIMIT} bytes"),
        )),
    )
}

async fn read_json_body(
    headers: &HeaderMap,
    body: Body,
) -> Result<axum::body::Bytes, (StatusCode, Json<ApiError>)> {
    // Mirrors axum's `Json` extractor: only JSON bodies are accepted.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(ApiError::new(
                "unsupported_media_type",
                "content-type must be application/json",
            )),
        ));
    }
    // A declared length over the cap is refused before reading the body. A
    // chunked body declares nothing, so the loop below enforces the cap.
    if headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|declared| declared > JSON_BODY_LIMIT)
    {
        return Err(payload_too_large());
    }
    // Buffer explicitly instead of calling `axum::body::to_bytes`: it reports a
    // limit breach as an opaque body error, so the size case would be
    // indistinguishable from a transport failure (400) without string
    // matching. Reading the stream keeps the 413 explicit.
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|_| bad_request("invalid_body", "failed to buffer the request body"))?;
        if bytes.len() + chunk.len() > JSON_BODY_LIMIT {
            return Err(payload_too_large());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(axum::body::Bytes::from(bytes))
}

fn invalid_json(error: serde_json::Error) -> (StatusCode, Json<ApiError>) {
    // Mirrors axum's `Json` rejection status for an unparsable JSON body.
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(ApiError::new("invalid_json", error.to_string())),
    )
}

/// Refuse new orders when no matcher is running. Without this the gateway
/// acknowledges orders that will never be matched.
fn ensure_shards_ready(ctx: &NodeContext) -> Result<(), (StatusCode, Json<ApiError>)> {
    if ctx.shards_ok() {
        Ok(())
    } else {
        Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new(
                "shard_unavailable",
                "the matching shard is not running; orders are not being accepted",
            )),
        ))
    }
}

async fn submit_order(
    State(ctx): State<Arc<NodeContext>>,
    Extension(participants): Extension<Arc<ParticipantCache>>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<OrderAck> {
    ensure_shards_ready(&ctx)?;
    let started = Instant::now();
    let caller = authenticate(&ctx, &headers)?;
    // The stage budget starts after auth, which is not a stage: this keeps
    // `GatewayParse` to the body read and the JSON parse.
    let mut stage_at = Instant::now();
    let bytes = read_json_body(&headers, body).await?;
    // Fast path: borrow the strings from the body. A body with a JSON escape
    // cannot be borrowed, so fall back to the owned DTO rather than rejecting
    // a request the old API accepted.
    let validated = match serde_json::from_slice::<OrderRequestRef<'_>>(&bytes) {
        Ok(request) => validate_order_ref(&ctx, &request, caller, &participants),
        Err(borrow_error) => match serde_json::from_slice::<OrderRequest>(&bytes) {
            Ok(owned) => {
                let request = OrderRequestRef {
                    symbol: owned.symbol.as_str(),
                    side: owned.side,
                    order_type: owned.order_type,
                    price_cents: owned.price_cents,
                    qty: owned.qty,
                    client_order_id: owned.client_order_id.as_deref(),
                    participant: owned.participant.as_deref(),
                };
                validate_order_ref(&ctx, &request, caller, &participants)
            }
            Err(_) => Err(invalid_json(borrow_error)),
        },
    };
    ctx.metrics
        .record_stage_us(Stage::GatewayParse, split_elapsed(&mut stage_at));
    let validated = validated?;
    let timestamp_ns = now_ns();
    let (order_id, message) = build_order(&validated, timestamp_ns);
    let partition = validated.symbol.partition(ctx.exchange().partitions);
    ctx.metrics
        .record_stage_us(Stage::GatewayEncode, split_elapsed(&mut stage_at));
    let append = ctx
        .group_commit
        .append(Topic::Orders, partition, vec![message])
        .await;
    ctx.metrics
        .record_stage_us(Stage::GatewayAppend, split_elapsed(&mut stage_at));
    let append = append.map_err(journal_unavailable)?;
    let elapsed = started.elapsed();
    ctx.metrics.record_ingress();
    ctx.metrics
        .record_ingress_latency_us(elapsed.as_micros() as u64);

    Ok(Json(OrderAck {
        order_id,
        symbol: validated.symbol,
        partition: append.partition,
        offset: append.base_offset,
        mode: ctx.config.mode,
        status: "accepted_by_log".into(),
        accepted_at_ns: timestamp_ns,
        durable_ack_ms: elapsed.as_secs_f64() * 1_000.0,
    }))
}

/// High-throughput ingress: one symbol, one validation pass, one command/header
/// construction pass, one Iggy append. Offsets in the response are contiguous
/// from `base_offset` because they all come from the same broker batch.
///
/// `?acks=none` (or `acks=false`) omits the per-order `OrderAck` array and
/// returns only the batch summary. The offsets still follow
/// `base_offset + i`; this is the bench and fire-and-forget path. Full acks
/// remain the default for compatibility.
async fn submit_orders(
    State(ctx): State<Arc<NodeContext>>,
    Extension(participants): Extension<Arc<ParticipantCache>>,
    connect: Option<Extension<ConnectInfo<SocketAddr>>>,
    headers: HeaderMap,
    Query(query): Query<BatchQuery>,
    body: Body,
) -> ApiResult<BatchOrderAck> {
    ensure_shards_ready(&ctx)?;
    let include_acks = query.acks.as_deref().is_none_or(|value| {
        !(value.eq_ignore_ascii_case("none") || value.eq_ignore_ascii_case("false"))
    });
    let started = Instant::now();
    let caller = authenticate(&ctx, &headers)?;
    // See `submit_order`: the stage budget starts after auth, and every stage
    // below is a slice of this one clock, shared by the whole batch.
    let mut stage_at = Instant::now();
    let bytes = read_json_body(&headers, body).await?;
    let validated = match serde_json::from_slice::<BatchOrderRequestRef<'_>>(&bytes) {
        Ok(request) => validate_batch(&ctx, &request, caller, &participants),
        Err(borrow_error) => match serde_json::from_slice::<BatchOrderRequest>(&bytes) {
            Ok(owned) => {
                let orders: Vec<OrderRequestRef<'_>> = owned
                    .orders
                    .iter()
                    .map(|order| OrderRequestRef {
                        symbol: order.symbol.as_str(),
                        side: order.side,
                        order_type: order.order_type,
                        price_cents: order.price_cents,
                        qty: order.qty,
                        client_order_id: order.client_order_id.as_deref(),
                        participant: order.participant.as_deref(),
                    })
                    .collect();
                let request = BatchOrderRequestRef { orders };
                validate_batch(&ctx, &request, caller, &participants)
            }
            Err(_) => Err(invalid_json(borrow_error)),
        },
    };
    ctx.metrics
        .record_stage_us(Stage::GatewayParse, split_elapsed(&mut stage_at));
    let (partition, validated) = validated?;
    // The middleware charged one token for the request; charge the rest of the
    // batch so a 256-order request cannot bypass the per-second budget.
    if let Some(Extension(ConnectInfo(address))) = connect {
        let extra = validated.len().saturating_sub(1) as u32;
        if extra > 0 && !ctx.rate_limiter.charge(address.ip(), extra) {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(ApiError::new(
                    "rate_limited",
                    "ingress rate limit exceeded; retry shortly",
                )),
            ));
        }
    }

    // Pass 2: encode one command and header set per order.
    let timestamp_ns = now_ns();
    let mut messages = Vec::with_capacity(validated.len());
    let mut order_ids = Vec::with_capacity(validated.len());
    for order in &validated {
        let (order_id, message) = build_order(order, timestamp_ns);
        order_ids.push(order_id);
        messages.push(message);
    }

    let count = messages.len() as u32;
    ctx.metrics
        .record_stage_us(Stage::GatewayEncode, split_elapsed(&mut stage_at));
    // Route batches through the same per-partition writer as single orders, so
    // one partition has exactly one append path and log order matches arrival
    // order across transports.
    let append = ctx
        .group_commit
        .append(Topic::Orders, partition, messages)
        .await;
    ctx.metrics
        .record_stage_us(Stage::GatewayAppend, split_elapsed(&mut stage_at));
    let append = append.map_err(journal_unavailable)?;
    let elapsed = started.elapsed();
    let durable_ack_ms = elapsed.as_secs_f64() * 1_000.0;
    // Throughput is per order, but latency is sampled once per batch: every
    // order in the batch shares the single durable round trip.
    for _ in &validated {
        ctx.metrics.record_ingress();
    }
    ctx.metrics
        .record_ingress_latency_us(elapsed.as_micros() as u64);

    let orders = if include_acks {
        validated
            .iter()
            .zip(order_ids)
            .enumerate()
            .map(|(index, (order, order_id))| OrderAck {
                order_id,
                symbol: order.symbol,
                partition: append.partition,
                offset: append.base_offset + index as u64,
                mode: ctx.config.mode,
                status: "accepted_by_log".into(),
                accepted_at_ns: timestamp_ns,
                durable_ack_ms,
            })
            .collect()
    } else {
        Vec::new()
    };

    Ok(Json(BatchOrderAck {
        partition: append.partition,
        base_offset: append.base_offset,
        count,
        mode: ctx.config.mode,
        durable_ack_ms,
        orders,
    }))
}

/// Pass 1 of batch ingress: reject mixed symbols, authenticate every
/// participant, and validate every order. Nothing is appended unless the whole
/// batch is acceptable. Returns the target partition and the settled orders.
fn validate_batch<'a>(
    ctx: &NodeContext,
    request: &BatchOrderRequestRef<'a>,
    caller: Option<&str>,
    participants: &ParticipantCache,
) -> Result<(u32, Vec<ValidatedOrder>), (StatusCode, Json<ApiError>)> {
    if request.orders.is_empty() {
        return Err(bad_request(
            "empty_batch",
            "orders must contain at least one order",
        ));
    }
    if request.orders.len() > MAX_BATCH_ORDERS {
        return Err(bad_request(
            "batch_too_large",
            format!("at most {MAX_BATCH_ORDERS} orders per batch"),
        ));
    }
    let symbol = Symbol::new(request.orders[0].symbol).map_err(|_| {
        bad_request(
            "unknown_symbol",
            format!("unknown symbol {}", request.orders[0].symbol),
        )
    })?;
    let partition = symbol.partition(ctx.exchange().partitions);
    let mut validated = Vec::with_capacity(request.orders.len());
    for (index, order) in request.orders.iter().enumerate() {
        let order_symbol = Symbol::new(order.symbol).map_err(|_| {
            with_index(
                index,
                bad_request("unknown_symbol", format!("unknown symbol {}", order.symbol)),
            )
        })?;
        if order_symbol != symbol {
            return Err(bad_request(
                "mixed_symbols",
                format!(
                    "order {index} uses {order_symbol} but the batch started with {symbol}; split the batch by symbol",
                ),
            ));
        }
        let order = validate_order_ref(ctx, order, caller, participants)
            .map_err(|error| with_index(index, error))?;
        validated.push(order);
    }
    Ok((partition, validated))
}

/// The parts of an order that validation has already settled. Building the
/// command from this cannot fail, which keeps the append path branch-free.
struct ValidatedOrder {
    symbol: Symbol,
    side: Side,
    order_type: OrderType,
    price: Price,
    qty: Qty,
    order_id: OrderId,
    participant: ParticipantId,
}

fn validate_order_ref(
    ctx: &NodeContext,
    request: &OrderRequestRef<'_>,
    caller: Option<&str>,
    participants: &ParticipantCache,
) -> Result<ValidatedOrder, (StatusCode, Json<ApiError>)> {
    // `Symbol::new` copies at most 16 bytes and never allocates on success;
    // only the error path formats.
    let symbol = Symbol::new(request.symbol).map_err(|_| {
        bad_request(
            "unknown_symbol",
            format!("unknown symbol {}", request.symbol),
        )
    })?;
    validate_fields(
        ctx,
        symbol,
        request.side,
        request.order_type,
        request.price_cents,
        request.qty,
        request.client_order_id,
        request.participant,
        caller,
        Some(participants),
    )
}

#[allow(clippy::too_many_arguments)]
fn validate_fields(
    ctx: &NodeContext,
    symbol: Symbol,
    side: Side,
    order_type: OrderType,
    price_cents: Option<i64>,
    qty: u64,
    client_order_id: Option<&str>,
    participant: Option<&str>,
    caller: Option<&str>,
    participants: Option<&ParticipantCache>,
) -> Result<ValidatedOrder, (StatusCode, Json<ApiError>)> {
    let instrument = ctx
        .exchange()
        .instrument(symbol)
        .ok_or_else(|| bad_request("unknown_symbol", format!("unknown symbol {symbol}")))?;
    if qty == 0 {
        return Err(bad_request("invalid_qty", "qty must be greater than zero"));
    }
    if qty > 1_000_000_000 {
        return Err(bad_request("invalid_qty", "qty is unreasonably large"));
    }
    let price = if order_type.requires_price() {
        let cents = price_cents.ok_or_else(|| {
            bad_request(
                "missing_price",
                "price_cents is required for this order type",
            )
        })?;
        instrument
            .ticks_from_cents(cents)
            .map_err(|error| bad_request("invalid_price", error.to_string()))?
    } else {
        Price::ZERO
    };
    // Derive from the borrowed key before any owned conversion: a supplied
    // idempotency key never touches the allocator. Only a missing key mints
    // (and owns) a fresh UUID.
    let order_id = match client_order_id.filter(|value| !value.is_empty()) {
        Some(key) => {
            if key.len() > 128 {
                return Err(bad_request(
                    "invalid_client_order_id",
                    "client_order_id must be at most 128 bytes",
                ));
            }
            OrderId::derive(symbol, key)
        }
        None => OrderId::derive(symbol, &uuid::Uuid::new_v4().to_string()),
    };
    let participant = resolve_participant_id(caller, participant, participants)?;
    Ok(ValidatedOrder {
        symbol,
        side,
        order_type,
        price,
        qty: Qty(qty),
        order_id,
        participant,
    })
}

fn build_order(order: &ValidatedOrder, timestamp_ns: u64) -> (OrderId, JournalMessage) {
    let command = Command::new_order(
        order.symbol,
        order.side,
        order.order_type,
        order.price,
        order.qty,
        order.order_id,
        order.participant,
        timestamp_ns,
    );
    let meta = MessageMeta::new(
        order.symbol,
        Seq(0),
        "order",
        SCHEMA_ORDER,
        order.order_id.0,
    );
    (
        order.order_id,
        JournalMessage::new_named(command.encode(), meta.named_pairs(HeaderMode::Full)),
    )
}

/// Shared ingress encoding for the binary TCP path. One fixed-layout command
/// becomes one journal message with the same header set the HTTP path uses, so
/// the engine decodes identical payloads regardless of transport.
pub(crate) fn journal_message_for_command(command: &Command) -> JournalMessage {
    let meta = MessageMeta::new(
        command.symbol,
        Seq(0),
        "order",
        SCHEMA_ORDER,
        command.order_id.0,
    );
    JournalMessage::new_named(command.encode(), meta.named_pairs(HeaderMode::Full))
}

/// Constant-time bearer comparison shared by the HTTP and TCP ingress paths.
pub(crate) fn token_matches(expected: &str, presented: &str) -> bool {
    constant_time_eq(expected.as_bytes(), presented.as_bytes())
}

async fn cancel_order(
    State(ctx): State<Arc<NodeContext>>,
    Extension(participants): Extension<Arc<ParticipantCache>>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<CancelAck> {
    ensure_shards_ready(&ctx)?;
    let caller = authenticate(&ctx, &headers)?;
    // Use the same body contract as the order paths instead of axum's `Json`
    // extractor, whose syntax-error rejection is a bare 400. A malformed cancel
    // body must be the same 422 `invalid_json` an order body gets.
    let bytes = read_json_body(&headers, body).await?;
    let request: CancelRequest = serde_json::from_slice(&bytes).map_err(invalid_json)?;
    ctx.exchange().instrument(request.symbol).ok_or_else(|| {
        bad_request(
            "unknown_symbol",
            format!("unknown symbol {}", request.symbol),
        )
    })?;
    let participant =
        resolve_participant_id(caller, request.participant.as_deref(), Some(&participants))?;
    let timestamp_ns = now_ns();
    let command = Command::cancel(request.symbol, request.order_id, participant, timestamp_ns);
    let partition = request.symbol.partition(ctx.exchange().partitions);
    let meta = MessageMeta::new(
        request.symbol,
        Seq(0),
        "cancel",
        SCHEMA_ORDER,
        request.order_id.0,
    );
    let message = JournalMessage::new_named(command.encode(), meta.named_pairs(HeaderMode::Full));
    let append = ctx
        .group_commit
        .append(Topic::Orders, partition, vec![message])
        .await
        .map_err(journal_unavailable)?;
    Ok(Json(CancelAck {
        order_id: request.order_id,
        symbol: request.symbol,
        partition: append.partition,
        offset: append.base_offset,
        mode: ctx.config.mode,
        status: "cancel_accepted_by_log".into(),
    }))
}

/// `Some(participant)` when the request presented a valid token. `None` means
/// anonymous ingress, which is only possible when no token is configured.
fn authenticate<'a>(
    ctx: &'a NodeContext,
    headers: &HeaderMap,
) -> Result<Option<&'a str>, (StatusCode, Json<ApiError>)> {
    let Some(expected) = ctx.config.api_token.as_deref() else {
        return Ok(None);
    };
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_token);
    match presented {
        Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => {
            Ok(Some(ctx.config.api_participant.as_str()))
        }
        _ => Err((
            StatusCode::UNAUTHORIZED,
            Json(ApiError::new(
                "unauthorized",
                "a valid Authorization: Bearer token is required",
            )),
        )),
    }
}

fn bearer_token(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

/// Compare without early exit so a wrong token cannot be recovered byte by
/// byte from response timing.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right) {
        diff |= a ^ b;
    }
    diff == 0
}

/// Shared participant check for the HTTP ingress paths. Returns the name
/// borrowed from the caller config, the request, or the anonymous fallback:
/// no allocation on any path.
fn check_participant<'a>(
    caller: Option<&'a str>,
    requested: Option<&'a str>,
) -> Result<&'a str, (StatusCode, Json<ApiError>)> {
    let requested = requested.filter(|value| !value.is_empty());
    let name = match caller {
        Some(configured) => {
            if let Some(requested) = requested
                && requested != configured
            {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(ApiError::new(
                        "participant_mismatch",
                        format!(
                            "this token acts as {configured}; request participant {requested} is not allowed"
                        ),
                    )),
                ));
            }
            configured
        }
        None => requested.unwrap_or("anonymous"),
    };
    if name.len() > 64 {
        return Err(bad_request(
            "invalid_participant",
            "participant must be at most 64 bytes",
        ));
    }
    Ok(name)
}

/// Authenticated callers act as the configured participant (derived directly:
/// the name is fixed, so no cache is needed). Anonymous body participants go
/// through the bounded derivation cache when one is supplied.
fn resolve_participant_id(
    caller: Option<&str>,
    requested: Option<&str>,
    participants: Option<&ParticipantCache>,
) -> Result<ParticipantId, (StatusCode, Json<ApiError>)> {
    let name = check_participant(caller, requested)?;
    Ok(match (caller, participants) {
        (None, Some(cache)) => cache.get_or_derive(name),
        _ => ParticipantId::derive(name),
    })
}

fn with_index(
    index: usize,
    (status, Json(error)): (StatusCode, Json<ApiError>),
) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError::new(
            error.code,
            format!("order {index}: {}", error.error),
        )),
    )
}

async fn state(State(ctx): State<Arc<NodeContext>>) -> Json<Arc<StateView>> {
    // The publisher already built the latest conflated frame. Do not rebuild
    // the whole book and tape on every read.
    Json(ctx.hub.latest())
}

async fn health(State(ctx): State<Arc<NodeContext>>) -> Json<HealthView> {
    let metrics = ctx.metrics.snapshot();
    let journal_ready =
        ctx.config.journal == crate::config::JournalKind::Memory || metrics.iggy_connected;
    let shards_ok = ctx.shards_ok();
    Json(HealthView {
        status: if journal_ready && shards_ok {
            "ok".into()
        } else {
            "degraded".into()
        },
        mode: ctx.config.mode,
        iggy_connected: metrics.iggy_connected,
        iggy_addr: ctx.config.iggy_addr.clone(),
        journal: ctx.config.journal.as_str().to_string(),
        shards_ok,
        version: env!("CARGO_PKG_VERSION").into(),
        uptime_s: ctx.started.elapsed().as_secs(),
        partitions: ctx.exchange().partitions,
        auth_required: ctx.config.auth_required(),
    })
}

#[derive(Serialize)]
struct ReadinessView {
    ready: bool,
    journal: bool,
    shards_ok: bool,
}

/// Machine readiness: 200 only when the journal answers and every shard is
/// alive. `/api/health` stays the human-readable JSON view.
async fn readyz(State(ctx): State<Arc<NodeContext>>) -> impl IntoResponse {
    let ingress = ctx.journal.ping().await;
    let engine = ctx.engine_journal.ping().await;
    let journal = ingress && engine;
    let shards_ok = ctx.shards_ok();
    let ready = journal && shards_ok;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadinessView {
            ready,
            journal,
            shards_ok,
        }),
    )
}

async fn instruments(
    State(ctx): State<Arc<NodeContext>>,
) -> Json<whippet_core::config::ExchangeConfig> {
    Json(ctx.exchange().clone())
}

async fn prometheus(State(ctx): State<Arc<NodeContext>>) -> impl IntoResponse {
    let state = ctx.hub.latest();
    let stats = ctx.group_commit.stats();
    let stages = ctx.metrics.stage_snapshot();
    let mut body = prometheus_text(&state.metrics, state.freshness_ms, state.sequence, &stages);
    body.push_str("# HELP whippet_group_commit_batches_total Group-commit appends completed.\n");
    body.push_str("# TYPE whippet_group_commit_batches_total counter\n");
    body.push_str(&format!(
        "whippet_group_commit_batches_total {}\n",
        stats.batches
    ));
    body.push_str("# HELP whippet_group_commit_messages_total Messages coalesced into group-commit appends.\n");
    body.push_str("# TYPE whippet_group_commit_messages_total counter\n");
    body.push_str(&format!(
        "whippet_group_commit_messages_total {}\n",
        stats.messages
    ));
    body.push_str("# HELP whippet_group_commit_max_batch Largest group-commit append seen.\n");
    body.push_str("# TYPE whippet_group_commit_max_batch gauge\n");
    body.push_str(&format!(
        "whippet_group_commit_max_batch {}\n",
        stats.max_batch_seen
    ));
    body.push_str("# HELP whippet_group_commit_partitions Live group-commit writer tasks.\n");
    body.push_str("# TYPE whippet_group_commit_partitions gauge\n");
    body.push_str(&format!(
        "whippet_group_commit_partitions {}\n",
        ctx.group_commit.writer_count()
    ));
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

/// Prometheus exposition. Counters are monotonic; percentiles are gauges
/// because they describe the recent process window, not a cumulative sum.
///
/// `stages` is [`whippet_marketdata::Metrics::stage_snapshot`]: one p50 and one
/// p99 series per stage, with the stage name as the `stage` label.
pub(crate) fn prometheus_text(
    metrics: &whippet_core::api::MetricsView,
    freshness_ms: u64,
    sequence: u64,
    stages: &[(Stage, LatencySummary)],
) -> String {
    let mut out = String::with_capacity(3_072);
    out.push_str(
        "# HELP whippet_orders_ingressed_total Orders accepted by the gateway and appended to Iggy.\n",
    );
    out.push_str("# TYPE whippet_orders_ingressed_total counter\n");
    out.push_str(&format!(
        "whippet_orders_ingressed_total {}\n",
        metrics.orders_ingressed
    ));
    out.push_str("# HELP whippet_orders_matched_total Orders accepted by the matcher.\n");
    out.push_str("# TYPE whippet_orders_matched_total counter\n");
    out.push_str(&format!(
        "whippet_orders_matched_total {}\n",
        metrics.orders_matched
    ));
    out.push_str(
        "# HELP whippet_orders_rejected_total Orders rejected by validation or matching.\n",
    );
    out.push_str("# TYPE whippet_orders_rejected_total counter\n");
    out.push_str(&format!(
        "whippet_orders_rejected_total {}\n",
        metrics.orders_rejected
    ));
    out.push_str("# HELP whippet_executions_total Execution reports produced.\n");
    out.push_str("# TYPE whippet_executions_total counter\n");
    out.push_str(&format!(
        "whippet_executions_total {}\n",
        metrics.executions
    ));
    out.push_str("# HELP whippet_trades_total Trades observed by the MD consumer.\n");
    out.push_str("# TYPE whippet_trades_total counter\n");
    out.push_str(&format!("whippet_trades_total {}\n", metrics.trades));
    out.push_str("# HELP whippet_md_events_total Market-data records applied by the hub.\n");
    out.push_str("# TYPE whippet_md_events_total counter\n");
    out.push_str(&format!("whippet_md_events_total {}\n", metrics.md_events));
    out.push_str("# HELP whippet_engine_commands_total Input commands applied by the matcher.\n");
    out.push_str("# TYPE whippet_engine_commands_total counter\n");
    out.push_str(&format!(
        "whippet_engine_commands_total {}\n",
        metrics.engine_commands
    ));
    out.push_str("# HELP whippet_consumer_lag Records waiting behind the engine consumer.\n");
    out.push_str("# TYPE whippet_consumer_lag gauge\n");
    out.push_str(&format!("whippet_consumer_lag {}\n", metrics.consumer_lag));
    out.push_str("# HELP whippet_ingress_latency_microseconds Gateway append latency.\n");
    out.push_str("# TYPE whippet_ingress_latency_microseconds gauge\n");
    out.push_str(&format!(
        "whippet_ingress_latency_microseconds{{quantile=\"0.5\"}} {}\n",
        metrics.ingress_p50_us
    ));
    out.push_str(&format!(
        "whippet_ingress_latency_microseconds{{quantile=\"0.99\"}} {}\n",
        metrics.ingress_p99_us
    ));
    out.push_str("# HELP whippet_match_latency_microseconds Engine dequeue to execution events.\n");
    out.push_str("# TYPE whippet_match_latency_microseconds gauge\n");
    out.push_str(&format!(
        "whippet_match_latency_microseconds{{quantile=\"0.5\"}} {}\n",
        metrics.match_p50_us
    ));
    out.push_str(&format!(
        "whippet_match_latency_microseconds{{quantile=\"0.99\"}} {}\n",
        metrics.match_p99_us
    ));
    out.push_str("# HELP whippet_throughput_orders_per_second Accepted orders per second.\n");
    out.push_str("# TYPE whippet_throughput_orders_per_second gauge\n");
    out.push_str(&format!(
        "whippet_throughput_orders_per_second {}\n",
        metrics.throughput_orders_s
    ));
    out.push_str("# HELP whippet_uptime_seconds Node uptime.\n");
    out.push_str("# TYPE whippet_uptime_seconds gauge\n");
    out.push_str(&format!("whippet_uptime_seconds {}\n", metrics.uptime_s));
    out.push_str("# HELP whippet_iggy_connected Whether the last Iggy health check succeeded.\n");
    out.push_str("# TYPE whippet_iggy_connected gauge\n");
    out.push_str(&format!(
        "whippet_iggy_connected {}\n",
        u8::from(metrics.iggy_connected)
    ));
    out.push_str("# HELP whippet_partitions Configured trading partitions.\n");
    out.push_str("# TYPE whippet_partitions gauge\n");
    out.push_str(&format!("whippet_partitions {}\n", metrics.partition_count));
    out.push_str("# HELP whippet_mode Operating mode (one-hot).\n");
    out.push_str("# TYPE whippet_mode gauge\n");
    out.push_str(&format!(
        "whippet_mode{{mode=\"{}\"}} 1\n",
        metrics.mode.as_str()
    ));
    out.push_str("# HELP whippet_md_freshness_milliseconds Age of the newest MD event.\n");
    out.push_str("# TYPE whippet_md_freshness_milliseconds gauge\n");
    out.push_str(&format!(
        "whippet_md_freshness_milliseconds {freshness_ms}\n"
    ));
    out.push_str("# HELP whippet_md_sequence Newest per-symbol MD sequence observed.\n");
    out.push_str("# TYPE whippet_md_sequence gauge\n");
    out.push_str(&format!("whippet_md_sequence {sequence}\n"));
    // The stage budget: where a request spends its time between the socket and
    // the published frame. One series per stage even before it has samples, so
    // a graph never interpolates across a missing label.
    out.push_str(
        "# HELP whippet_stage_p50_us Stage latency p50, request intake to visible state.\n",
    );
    out.push_str("# TYPE whippet_stage_p50_us gauge\n");
    for (stage, summary) in stages {
        out.push_str(&format!(
            "whippet_stage_p50_us{{stage=\"{}\"}} {}\n",
            stage.as_str(),
            summary.p50_us
        ));
    }
    out.push_str(
        "# HELP whippet_stage_p99_us Stage latency p99, request intake to visible state.\n",
    );
    out.push_str("# TYPE whippet_stage_p99_us gauge\n");
    for (stage, summary) in stages {
        out.push_str(&format!(
            "whippet_stage_p99_us{{stage=\"{}\"}} {}\n",
            stage.as_str(),
            summary.p99_us
        ));
    }
    out
}

#[derive(Debug, Deserialize)]
struct BatchQuery {
    /// `none` or `false` omits the per-order ack array. Absent means full acks.
    #[serde(default)]
    acks: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TradesQuery {
    #[serde(default = "default_trade_limit")]
    limit: usize,
}

fn default_trade_limit() -> usize {
    100
}

async fn trades(
    State(ctx): State<Arc<NodeContext>>,
    Query(query): Query<TradesQuery>,
) -> Json<Vec<TradeView>> {
    let state = ctx.hub.latest();
    let limit = query.limit.min(state.trades.len());
    Json(state.trades.iter().take(limit).cloned().collect())
}

#[derive(Debug, Deserialize)]
struct WsQuery {
    /// Cap the trade tape in the frame. `0` (the default) keeps the full tape.
    trades: Option<usize>,
}

/// Serialized WS state frames, one per `(filter, sequence, freshness)`.
///
/// The publisher ticks at 20 Hz. Before this cache every subscriber cloned the
/// whole `StateView` and ran `serde_json::to_string` independently, so N clients
/// cost N serializations of the same bytes. Now the first subscriber builds the
/// frame and the rest clone a refcounted buffer.
///
/// A serialization miss is recorded as the `WsFrame` stage; cache hits are not,
/// so the stage measures frame building and nothing else.
struct FrameCache {
    inner: Mutex<FrameCacheInner>,
    metrics: Arc<Metrics>,
}

impl FrameCache {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Mutex::new(FrameCacheInner::default()),
            metrics,
        }
    }
}

#[derive(Default)]
struct FrameCacheInner {
    /// The exact published frame these entries were built from. Pointer
    /// identity is the only correct invalidation key: `sequence` and
    /// `freshness_ms` both stay 0 on a node that has applied no MD event, so
    /// keying on them would serve the first frame forever.
    last_state: Option<Arc<StateView>>,
    frames: HashMap<(Vec<Symbol>, usize), axum::extract::ws::Utf8Bytes>,
}

/// Distinct `(symbols, trade limit)` frames kept per tick. A client can send
/// many `Subscribe` messages; without a bound the per-tick map would grow with
/// the number of distinct filters.
const MAX_CACHED_FRAMES: usize = 16;

impl FrameCache {
    fn frame(
        &self,
        state: &Arc<StateView>,
        symbols: &[Symbol],
        trade_limit: usize,
    ) -> axum::extract::ws::Utf8Bytes {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner
            .last_state
            .as_ref()
            .is_none_or(|last| !Arc::ptr_eq(last, state))
        {
            inner.last_state = Some(Arc::clone(state));
            inner.frames.clear();
        }
        let key = (symbols.to_vec(), trade_limit);
        if let Some(frame) = inner.frames.get(&key) {
            return frame.clone();
        }
        // The unfiltered, untruncated frame is the common case. Serialize the
        // published view directly instead of cloning it first.
        let mut encode_at = Instant::now();
        let text = if symbols.is_empty() && trade_limit == 0 {
            serde_json::to_string(&WsServerMessage::State {
                state: state.as_ref().clone(),
            })
        } else {
            let view = filtered_state(state.as_ref(), symbols, trade_limit);
            serde_json::to_string(&WsServerMessage::State { state: view })
        }
        .unwrap_or_else(|_| {
            "{\"type\":\"notice\",\"level\":\"error\",\"message\":\"encode failed\"}".into()
        });
        self.metrics
            .record_stage_us(Stage::WsFrame, split_elapsed(&mut encode_at));
        let frame = axum::extract::ws::Utf8Bytes::from(text);
        if inner.frames.len() >= MAX_CACHED_FRAMES
            && let Some(evict) = inner.frames.keys().next().cloned()
        {
            // Evict one entry, not the whole cache: a single client with many
            // distinct subscriptions must not force every other subscriber to
            // re-serialize this tick.
            inner.frames.remove(&evict);
        }
        inner.frames.insert(key, frame.clone());
        frame
    }
}

/// Build a frame with only the requested symbols and trade count. An empty
/// symbol list means every symbol. `trade_limit == 0` keeps the full tape.
fn filtered_state(state: &StateView, symbols: &[Symbol], trade_limit: usize) -> StateView {
    let books = if symbols.is_empty() {
        state.symbols.clone()
    } else {
        state
            .symbols
            .iter()
            .filter(|book| symbols.contains(&book.symbol))
            .cloned()
            .collect()
    };
    let trades = if trade_limit == 0 || trade_limit >= state.trades.len() {
        state.trades.clone()
    } else {
        state.trades.iter().take(trade_limit).cloned().collect()
    };
    StateView {
        mode: state.mode,
        venue: state.venue.clone(),
        symbols: books,
        trades,
        metrics: state.metrics.clone(),
        freshness_ms: state.freshness_ms,
        sequence: state.sequence,
        iggy_addr: state.iggy_addr.clone(),
        journal: state.journal.clone(),
        guarantee: state.guarantee.clone(),
        dashboard_build: state.dashboard_build.clone(),
    }
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(ctx): State<Arc<NodeContext>>,
    Extension(cache): Extension<Arc<FrameCache>>,
    Query(query): Query<WsQuery>,
) -> Response {
    // Mirror the TCP connection cap: each subscriber holds a task and an
    // egress stream, so an unbounded upgrade path is a memory/bandwidth DoS.
    let permit = match Arc::clone(&ctx.ws_connections).try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "websocket connection cap reached",
            )
                .into_response();
        }
    };
    let trade_limit = query.trades.unwrap_or(0);
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            ws_loop(socket, ctx, cache, trade_limit).await;
        })
}

async fn ws_loop(
    mut socket: WebSocket,
    ctx: Arc<NodeContext>,
    cache: Arc<FrameCache>,
    trade_limit: usize,
) {
    let mut receiver = ctx.hub.subscribe();
    // Empty means "all symbols", matching the dashboard's default view.
    let mut symbols: Vec<Symbol> = Vec::new();
    let initial = cache.frame(&ctx.hub.latest(), &symbols, trade_limit);
    if socket.send(Message::Text(initial)).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            changed = receiver.changed() => {
                if changed.is_err() {
                    break;
                }
                let state = receiver.borrow().clone();
                let frame = cache.frame(&state, &symbols, trade_limit);
                if socket.send(Message::Text(frame)).await.is_err() {
                    break;
                }
            }
            incoming = socket.next() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<WsClientMessage>(&text) {
                            Ok(WsClientMessage::Subscribe { symbols: requested }) => {
                                symbols = requested;
                                let frame = cache.frame(&ctx.hub.latest(), &symbols, trade_limit);
                                if socket.send(Message::Text(frame)).await.is_err() {
                                    break;
                                }
                            }
                            Ok(WsClientMessage::Ping { sent_at_ns }) => {
                                let message = WsServerMessage::Notice {
                                    level: "pong".into(),
                                    message: sent_at_ns.to_string(),
                                };
                                if send_json(&mut socket, &message).await.is_err() {
                                    break;
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        warn!(error = %error, "websocket receive error");
                        break;
                    }
                }
            }
        }
    }
}

async fn send_json<T: Serialize>(socket: &mut WebSocket, value: &T) -> Result<(), ()> {
    let text = serde_json::to_string(value).map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

async fn missing_dashboard() -> Html<&'static str> {
    Html(
        r#"<!doctype html><html><head><meta charset="utf-8"><title>Whippet</title>
<style>body{font-family:ui-monospace,monospace;background:#0b0e13;color:#d8dee9;padding:3rem;line-height:1.6}
code{color:#7dd3fc}a{color:#a7f3d0}</style></head><body>
<h1>Whippet API is running</h1>
<p>The dashboard assets were not found. Build them with:</p>
<pre><code>cd apps/dashboard &amp;&amp; pnpm install &amp;&amp; pnpm build</code></pre>
<p>Then restart the node, or set <code>WHIPPET_DASHBOARD_DIR</code>.</p>
<p>API: <a href="/api/state">/api/state</a> · <a href="/api/health">/api/health</a></p>
</body></html>"#,
    )
}

fn bad_request(code: &str, message: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    (StatusCode::BAD_REQUEST, Json(ApiError::new(code, message)))
}

fn journal_unavailable(error: whippet_journal::JournalError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiError::new("journal_unavailable", error.to_string())),
    )
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::api::MetricsView;
    use whippet_core::config::Mode;

    fn sample_state() -> StateView {
        let btc = whippet_core::api::BookView {
            symbol: Symbol::new("BTC-USD").unwrap(),
            seq: Seq(1),
            bids: Vec::new(),
            asks: Vec::new(),
            best_bid_cents: None,
            best_ask_cents: None,
            spread_cents: None,
            mid_cents: None,
            last_trade_price_cents: None,
            last_trade_qty: 0,
            last_trade_side: None,
            depth_bid_qty: 0,
            depth_ask_qty: 0,
        };
        let eth = whippet_core::api::BookView {
            symbol: Symbol::new("ETH-USD").unwrap(),
            ..btc.clone()
        };
        StateView {
            mode: Mode::Durable,
            venue: "whippet".into(),
            symbols: vec![btc, eth],
            trades: Vec::new(),
            metrics: MetricsView::empty(Mode::Durable, 16),
            freshness_ms: 3,
            sequence: 9,
            iggy_addr: "iggy:8090".into(),
            journal: "iggy".into(),
            guarantee: "durable".into(),
            dashboard_build: None,
        }
    }

    #[test]
    fn filtered_state_keeps_only_requested_symbols() {
        let state = sample_state();
        let filtered = filtered_state(&state, &[Symbol::new("ETH-USD").unwrap()], 0);
        assert_eq!(filtered.symbols.len(), 1);
        assert_eq!(filtered.symbols[0].symbol.as_str(), "ETH-USD");
        assert_eq!(filtered.sequence, state.sequence);
    }

    #[test]
    fn empty_symbol_filter_keeps_every_symbol() {
        let state = sample_state();
        let filtered = filtered_state(&state, &[], 0);
        assert_eq!(filtered.symbols.len(), 2);
    }

    #[test]
    fn frame_cache_invalidates_without_md_events() {
        // `sequence` and `freshness_ms` both stay 0 on a node that has applied
        // no MD event. Two different published frames must still produce
        // different bytes; keying on those fields alone would freeze the feed.
        let cache = FrameCache::new(Arc::new(Metrics::new(
            Mode::Durable,
            whippet_core::config::ExchangeConfig::default().partitions,
        )));
        let mut first = sample_state();
        first.sequence = 0;
        first.freshness_ms = 0;
        first.metrics.orders_ingressed = 1;
        let mut second = first.clone();
        second.metrics.orders_ingressed = 2;

        let a = cache.frame(&Arc::new(first), &[], 60);
        let b = cache.frame(&Arc::new(second), &[], 60);
        assert_ne!(a, b, "a new published frame must not reuse stale bytes");
    }

    #[test]
    fn frame_cache_reuses_bytes_for_the_same_tick() {
        let cache = FrameCache::new(Arc::new(Metrics::new(Mode::Durable, 1)));
        let state = Arc::new(sample_state());
        let first = cache.frame(&state, &[], 60);
        let second = cache.frame(&state, &[], 60);
        assert_eq!(first, second);
        // A new published Arc invalidates the cache.
        let mut next = (*state).clone();
        next.sequence += 1;
        let third = cache.frame(&Arc::new(next), &[], 60);
        assert_ne!(first, third);
    }

    #[test]
    fn frame_cache_records_ws_frame_only_on_a_serialization_miss() {
        let metrics = Arc::new(Metrics::new(Mode::Durable, 1));
        let cache = FrameCache::new(Arc::clone(&metrics));
        let state = Arc::new(sample_state());
        let ws_frame_samples = || {
            metrics
                .stage_snapshot()
                .into_iter()
                .find(|(stage, _)| *stage == Stage::WsFrame)
                .map(|(_, summary)| summary.samples)
                .expect("ws_frame is always in the snapshot")
        };

        cache.frame(&state, &[], 60);
        assert_eq!(ws_frame_samples(), 1);
        // Same tick, same filter: a cache hit does no serialization, so it must
        // not add a sample.
        cache.frame(&state, &[], 60);
        assert_eq!(ws_frame_samples(), 1);
        // A different filter is a miss and is measured.
        cache.frame(&state, &[Symbol::new("BTC-USD").unwrap()], 60);
        assert_eq!(ws_frame_samples(), 2);
    }

    #[test]
    fn participant_cache_matches_derive_and_reuses_entries() {
        let cache = ParticipantCache::default();
        assert_eq!(cache.get_or_derive("alice"), ParticipantId::derive("alice"));
        assert_eq!(cache.len(), 1);
        // A repeat lookup hits the cache instead of growing it.
        assert_eq!(cache.get_or_derive("alice"), ParticipantId::derive("alice"));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_or_derive("bob"), ParticipantId::derive("bob"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn participant_cache_is_bounded() {
        let cache = ParticipantCache::default();
        for index in 0..PARTICIPANT_CACHE_CAP + 100 {
            let name = format!("participant-{index}");
            assert_eq!(cache.get_or_derive(&name), ParticipantId::derive(&name));
        }
        assert!(
            cache.len() <= PARTICIPANT_CACHE_CAP,
            "cache must never exceed the cap, len = {}",
            cache.len()
        );
    }

    #[test]
    fn prometheus_text_contains_core_series() {
        let metrics = MetricsView::empty(Mode::Durable, 16);
        let text = prometheus_text(&metrics, 42, 7, &[]);
        assert!(text.contains("whippet_orders_ingressed_total 0"));
        assert!(text.contains("whippet_mode{mode=\"durable\"} 1"));
        assert!(text.contains("whippet_md_freshness_milliseconds 42"));
        assert!(text.contains("whippet_md_sequence 7"));
        assert!(text.contains("# TYPE whippet_ingress_latency_microseconds gauge"));
    }

    #[test]
    fn prometheus_text_exposes_one_series_per_stage() {
        let metrics = Metrics::new(Mode::Durable, 16);
        for stage in Stage::ALL {
            metrics.record_stage_us(stage, 7);
        }
        let stages = metrics.stage_snapshot();
        let text = prometheus_text(&MetricsView::empty(Mode::Durable, 16), 0, 0, &stages);

        assert!(text.contains("# HELP whippet_stage_p50_us"));
        assert!(text.contains("# TYPE whippet_stage_p50_us gauge"));
        assert!(text.contains("# HELP whippet_stage_p99_us"));
        assert!(text.contains("# TYPE whippet_stage_p99_us gauge"));
        for stage in Stage::ALL {
            let name = stage.as_str();
            assert!(
                text.contains(&format!("whippet_stage_p50_us{{stage=\"{name}\"}} 7")),
                "missing p50 series for {name}"
            );
            assert!(
                text.contains(&format!("whippet_stage_p99_us{{stage=\"{name}\"}} 7")),
                "missing p99 series for {name}"
            );
        }
        // Exactly one series per stage per quantile, no duplicates.
        assert_eq!(
            text.matches("whippet_stage_p50_us{").count(),
            Stage::ALL.len()
        );
        assert_eq!(
            text.matches("whippet_stage_p99_us{").count(),
            Stage::ALL.len()
        );
        // Every exposed stage value is a non-negative number.
        for line in text
            .lines()
            .filter(|line| line.starts_with("whippet_stage_"))
        {
            let value = line.rsplit(' ').next().expect("value column");
            let value: f64 = value.parse().expect("numeric stage value");
            assert!(value >= 0.0, "negative stage value in {line}");
        }
    }

    #[test]
    fn prometheus_text_reports_unrecorded_stages_as_zero() {
        let metrics = Metrics::new(Mode::Durable, 16);
        let text = prometheus_text(
            &MetricsView::empty(Mode::Durable, 16),
            0,
            0,
            &metrics.stage_snapshot(),
        );
        for stage in Stage::ALL {
            let name = stage.as_str();
            assert!(
                text.contains(&format!("whippet_stage_p50_us{{stage=\"{name}\"}} 0")),
                "an unrecorded stage still needs a p50 series: {name}"
            );
            assert!(text.contains(&format!("whippet_stage_p99_us{{stage=\"{name}\"}} 0")));
        }
    }
}
