// Licensed under the Apache License, Version 2.0. See LICENSE.

//! HTTP gateway integration tests.
//!
//! The router is exercised in-process with `tower::ServiceExt::oneshot`; there
//! is no listener and no network. The journal is the real `MemoryJournal` the
//! gateway writes to, so assertions are about actual appended bytes.
//!
//! The gateway's contract is "accepted by log": it appends every request and
//! acknowledges the offset. Business idempotency lives in the engine, so the
//! duplicate test processes the memory journal through a real `SymbolEngine`
//! instead of pretending the gateway deduplicates.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use whippet::config::NodeConfig;
use whippet::context::NodeContext;
use whippet::gateway;
use whippet_core::api::{
    BatchOrderAck, BatchOrderRequest, BatchOrderRequestRef, CancelAck, HealthView,
    MAX_BATCH_ORDERS, OrderAck, OrderRequest, OrderRequestRef, StateView,
};
use whippet_core::config::Mode;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, CommandKind, ExecKind, MdKind, RejectReason, Side};
use whippet_marketdata::Stage;

use common::TestNode;

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

/// Per-request ingress options: a bearer token and/or a peer address, the way
/// the node's TCP listener supplies `ConnectInfo`.
#[derive(Default, Clone, Copy)]
struct Ingress {
    token: Option<&'static str>,
    peer: Option<SocketAddr>,
}

/// A second context over the same in-memory journal, with config overrides.
/// This keeps auth/rate-limit tests honest without changing the shared harness.
fn node_with(node: &TestNode, configure: impl FnOnce(&mut NodeConfig)) -> Arc<NodeContext> {
    node_with_ws_cap(node, node.ctx.ws_connections.available_permits(), configure)
}

/// Same as [`node_with`], but with a fresh WebSocket subscriber semaphore so a
/// test can use a small cap instead of the node's default.
fn node_with_ws_cap(
    node: &TestNode,
    ws_cap: usize,
    configure: impl FnOnce(&mut NodeConfig),
) -> Arc<NodeContext> {
    let mut config = node.ctx.config.clone();
    configure(&mut config);
    let rate_limiter = Arc::new(whippet::ratelimit::RateLimiter::new(
        config.rate_limit_per_sec,
    ));
    Arc::new(NodeContext {
        config,
        journal: Arc::clone(&node.ctx.journal),
        engine_journals: node.ctx.engine_journals.clone(),
        engine_journal: Arc::clone(&node.ctx.engine_journal),
        group_commit: node.ctx.group_commit.clone(),
        hub: Arc::clone(&node.ctx.hub),
        metrics: Arc::clone(&node.ctx.metrics),
        snapshots: node.ctx.snapshots.clone(),
        started: node.ctx.started,
        shutdown: node.ctx.shutdown.clone(),
        shards_ok: Arc::clone(&node.ctx.shards_ok),
        rate_limiter,
        ws_connections: Arc::new(tokio::sync::Semaphore::new(ws_cap)),
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo: None,
    })
}

async fn send(
    app: &Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
    ingress: Ingress,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = ingress.token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(peer) = ingress.peer {
        builder = builder.extension(ConnectInfo(peer));
    }
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value).expect("serialize request"))
        }
        None => Body::empty(),
    };
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("valid request"))
        .await
        .expect("router responds");
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json response")
    };
    (status, headers, value)
}

async fn post(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let (status, _, value) = send(app, Method::POST, uri, Some(body), Ingress::default()).await;
    (status, value)
}

async fn post_with(
    app: &Router,
    uri: &str,
    body: Value,
    ingress: Ingress,
) -> (StatusCode, HeaderMap, Value) {
    send(app, Method::POST, uri, Some(body), ingress).await
}

async fn get(app: &Router, uri: &str) -> (StatusCode, Value) {
    let (status, _, value) = send(app, Method::GET, uri, None, Ingress::default()).await;
    (status, value)
}

/// Plain-text GET for `/metrics`, whose body is not JSON.
async fn get_text(app: &Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("router responds");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    (
        status,
        String::from_utf8(bytes.to_vec()).expect("utf-8 metrics"),
    )
}

/// Parse the last column of the first exposition line with `prefix`.
fn metric_value(text: &str, prefix: &str) -> f64 {
    text.lines()
        .find(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("missing metric {prefix}"))
        .rsplit(' ')
        .next()
        .expect("value column")
        .parse()
        .expect("numeric metric value")
}

/// Test-only middleware: a real listener has one peer address (127.0.0.1), so
/// the per-IP WebSocket test selects its peer with this header. The node never
/// does this; production peer identity comes from `ConnectInfo`.
async fn with_test_peer(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(peer) = request
        .headers()
        .get("x-test-peer")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<SocketAddr>().ok())
    {
        request.extensions_mut().insert(ConnectInfo(peer));
    }
    next.run(request).await
}

/// Send a raw body so tests can exercise the size and content-type checks the
/// typed `send` helper cannot express.
async fn post_raw(
    app: &Router,
    uri: &str,
    content_type: Option<&str>,
    declared_len: Option<usize>,
    body: Vec<u8>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(Method::POST).uri(uri);
    if let Some(content_type) = content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    if let Some(declared_len) = declared_len {
        builder = builder.header(header::CONTENT_LENGTH, declared_len);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(body)).expect("valid request"))
        .await
        .expect("router responds");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("json response")
    };
    (status, value)
}

/// Perform a raw WebSocket handshake against a live server. The returned socket
/// must stay alive: dropping it frees the subscriber permit.
async fn open_websocket(addr: SocketAddr) -> (tokio::net::TcpStream, u16) {
    open_websocket_as(addr, None).await
}

/// Same as [`open_websocket`], with an optional synthetic peer selector for the
/// per-IP test (see [`with_test_peer`]).
async fn open_websocket_as(addr: SocketAddr, peer: Option<&str>) -> (tokio::net::TcpStream, u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect to the test server");
    let peer_header = peer.map_or_else(String::new, |peer| format!("x-test-peer: {peer}\r\n"));
    let request = format!(
        "GET /ws HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         {peer_header}Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the handshake");

    let mut response = Vec::new();
    let mut chunk = [0u8; 1_024];
    loop {
        let read = stream.read(&mut chunk).await.expect("read the handshake");
        assert!(read > 0, "the server closed before answering the handshake");
        response.extend_from_slice(&chunk[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let status = String::from_utf8_lossy(&response)
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status line");
    (stream, status)
}

#[test]
fn partition_assignment_is_pinned() {
    // The gateway, engine, and simulator all rely on this mapping. A change is
    // a wire-compatibility event, not an implementation detail.
    assert_eq!(Symbol::new("BTC-USD").unwrap().partition(16), 1);
    assert_eq!(Symbol::new("ETH-USD").unwrap().partition(16), 5);
    assert_eq!(Symbol::new("SOL-USD").unwrap().partition(16), 12);
}

#[tokio::test]
async fn accepted_order_is_appended_with_partition_and_offset() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "price_cents": 6_500_000,
            "qty": 10,
            "client_order_id": "gateway-1",
            "participant": "alice",
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let ack: OrderAck = serde_json::from_value(body).expect("order ack");
    assert_eq!(ack.status, "accepted_by_log");
    assert_eq!(ack.mode, Mode::Durable);
    assert_eq!(ack.symbol, symbol());
    assert_eq!(ack.partition, 1, "BTC-USD must stay on partition 1");
    assert_eq!(ack.offset, 0);
    assert_eq!(ack.order_id, OrderId::derive(symbol(), "gateway-1"));

    let records = node.orders(symbol());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].header("symbol"), Some("BTC-USD"));
    assert_eq!(records[0].header("type"), Some("order"));

    let command = Command::decode(&records[0].payload).expect("command payload");
    assert_eq!(command.kind, CommandKind::NewOrder);
    assert_eq!(command.symbol, symbol());
    assert_eq!(command.side, Side::Buy);
    assert_eq!(command.price, Price(65_000));
    assert_eq!(command.qty, Qty(10));
    assert_eq!(command.order_id, ack.order_id);
    assert_eq!(command.participant, ParticipantId::derive("alice"));
}

#[tokio::test]
async fn invalid_orders_are_rejected_without_appending() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let cases = [
        (
            json!({
                "symbol": "DOGE-USD", "side": "buy", "price_cents": 6_500_000, "qty": 1
            }),
            "unknown_symbol",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "price_cents": 6_500_000, "qty": 0
            }),
            "invalid_qty",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "price_cents": 6_500_000, "qty": 1_000_000_001u64
            }),
            "invalid_qty",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "price_cents": 6_500_050, "qty": 1
            }),
            "invalid_price",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "price_cents": 100, "qty": 1
            }),
            "invalid_price",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "qty": 1
            }),
            "missing_price",
        ),
    ];

    for (request, expected_code) in cases {
        let (status, body) = post(&app, "/api/orders", request).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "expected 400 for {expected_code}: {body}"
        );
        assert_eq!(body["code"], expected_code);
    }

    assert!(
        node.orders(symbol()).is_empty(),
        "rejected requests must not reach the journal"
    );
}

#[tokio::test]
async fn duplicate_client_order_id_is_deduped_by_the_engine() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let request = json!({
        "symbol": "BTC-USD",
        "side": "buy",
        "price_cents": 6_500_000,
        "qty": 10,
        "client_order_id": "dup-1",
        "participant": "alice",
    });

    let (first_status, first_body) = post(&app, "/api/orders", request.clone()).await;
    let (second_status, second_body) = post(&app, "/api/orders", request).await;
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    let first: OrderAck = serde_json::from_value(first_body).expect("first ack");
    let second: OrderAck = serde_json::from_value(second_body).expect("second ack");
    assert_eq!(first.order_id, second.order_id);
    assert_eq!(first.offset, 0);
    assert_eq!(second.offset, 1);

    // The gateway appended both attempts; the engine is the business dedup.
    let records = node.orders(symbol());
    assert_eq!(records.len(), 2);

    let processed = node.process_orders(symbol());
    assert_eq!(
        processed
            .executions
            .iter()
            .filter(|exec| exec.kind == ExecKind::Accepted)
            .count(),
        1,
        "exactly one acceptance"
    );
    assert_eq!(
        processed
            .executions
            .iter()
            .filter(|exec| {
                exec.kind == ExecKind::Rejected && exec.reason == RejectReason::DuplicateOrderId
            })
            .count(),
        1,
        "exactly one duplicate rejection"
    );
    assert_eq!(processed.engine.book().order_count(), 1);
    assert_eq!(processed.engine.book().depth_qty(Side::Buy), 10);
    assert_eq!(processed.engine.dedup().len(), 1);
    assert_eq!(
        processed
            .md
            .iter()
            .filter(|event| event.kind == MdKind::Add)
            .count(),
        1,
        "one market-data add for two retries"
    );
}

#[tokio::test]
async fn cancel_endpoint_appends_and_the_engine_removes_the_order() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "price_cents": 6_500_000,
            "qty": 10,
            "client_order_id": "cancel-1",
            "participant": "alice",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let order: OrderAck = serde_json::from_value(body).expect("order ack");

    let (status, body) = post(
        &app,
        "/api/cancel",
        json!({
            "symbol": "BTC-USD",
            "order_id": order.order_id.to_hex(),
            "participant": "alice",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cancel: CancelAck = serde_json::from_value(body).expect("cancel ack");
    assert_eq!(cancel.order_id, order.order_id);
    assert_eq!(cancel.partition, 1);
    assert_eq!(cancel.offset, 1);
    assert_eq!(cancel.status, "cancel_accepted_by_log");

    let records = node.orders(symbol());
    assert_eq!(records.len(), 2);
    let cancel_command = Command::decode(&records[1].payload).expect("cancel payload");
    assert_eq!(cancel_command.kind, CommandKind::Cancel);
    assert_eq!(cancel_command.order_id, order.order_id);
    assert_eq!(cancel_command.participant, ParticipantId::derive("alice"));

    let processed = node.process_orders(symbol());
    assert_eq!(processed.engine.book().order_count(), 0);
    assert!(
        processed
            .executions
            .iter()
            .any(|exec| exec.kind == ExecKind::Cancelled)
    );
}

#[tokio::test]
async fn state_and_health_report_the_memory_journal() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = get(&app, "/api/state").await;
    assert_eq!(status, StatusCode::OK);
    let state: StateView = serde_json::from_value(body).expect("state view");
    assert_eq!(state.journal, "memory");
    assert_eq!(state.venue, "whippet");
    assert_eq!(state.mode, Mode::Durable);
    assert_eq!(state.symbols.len(), 3);
    assert_eq!(state.sequence, 0);

    let (status, body) = get(&app, "/api/health").await;
    assert_eq!(status, StatusCode::OK);
    let health: HealthView = serde_json::from_value(body).expect("health view");
    assert_eq!(health.journal, "memory");
    assert_eq!(health.mode, Mode::Durable);
    assert_eq!(health.partitions, 16);
    assert!(!health.iggy_connected);
    assert!(!health.auth_required);
    // The memory journal is a supported test/dev backend, so readiness is
    // "ok" even though no Iggy connection exists.
    assert_eq!(health.status, "ok");
}

fn limit_order(client_order_id: &str, participant: &str) -> Value {
    json!({
        "symbol": "BTC-USD",
        "side": "buy",
        "order_type": "limit",
        "price_cents": 6_500_000,
        "qty": 1,
        "client_order_id": client_order_id,
        "participant": participant,
    })
}

#[tokio::test]
async fn ingress_requires_a_bearer_token_when_configured() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.api_token = Some("s3cret".into());
        config.api_participant = "desk-1".into();
    }));

    // Missing and wrong tokens are both 401, and nothing reaches the log.
    for token in [None, Some("wrong")] {
        let (status, _, body) = post_with(
            &app,
            "/api/orders",
            limit_order("auth-1", "desk-1"),
            Ingress {
                token,
                ..Ingress::default()
            },
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "unauthorized");
    }
    assert!(node.orders(symbol()).is_empty());

    let (status, _, body) = post_with(
        &app,
        "/api/orders/batch",
        json!({ "orders": [limit_order("auth-batch", "desk-1")] }),
        Ingress::default(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");

    let (status, _, body) = post_with(
        &app,
        "/api/cancel",
        json!({ "symbol": "BTC-USD", "order_id": OrderId::derive(symbol(), "auth-1").to_hex() }),
        Ingress::default(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");

    // A valid token acts as the configured participant even when the body
    // omits it.
    let (status, _, body) = post_with(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "price_cents": 6_500_000,
            "qty": 5,
            "client_order_id": "auth-2",
        }),
        Ingress {
            token: Some("s3cret"),
            ..Ingress::default()
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: OrderAck = serde_json::from_value(body).expect("order ack");
    assert_eq!(ack.offset, 0);
    let records = node.orders(symbol());
    assert_eq!(records.len(), 1);
    let command = Command::decode(&records[0].payload).expect("command payload");
    assert_eq!(command.participant, ParticipantId::derive("desk-1"));

    // Claiming a different participant is a hard 403, not an override.
    let (status, _, body) = post_with(
        &app,
        "/api/orders",
        limit_order("auth-3", "alice"),
        Ingress {
            token: Some("s3cret"),
            ..Ingress::default()
        },
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "participant_mismatch");
    assert_eq!(
        node.orders(symbol()).len(),
        1,
        "rejected request must not append"
    );

    let (status, body) = get(&app, "/api/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["auth_required"], true);
}

#[tokio::test]
async fn authenticated_cancel_uses_the_token_participant() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.api_token = Some("s3cret".into());
        config.api_participant = "desk-1".into();
    }));
    let token = Ingress {
        token: Some("s3cret"),
        ..Ingress::default()
    };

    let (status, _, body) = post_with(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "price_cents": 6_500_000,
            "qty": 10,
            "client_order_id": "cancel-auth-1",
        }),
        token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let order: OrderAck = serde_json::from_value(body).expect("order ack");

    // A body participant that does not match the token is rejected.
    let (status, _, body) = post_with(
        &app,
        "/api/cancel",
        json!({
            "symbol": "BTC-USD",
            "order_id": order.order_id.to_hex(),
            "participant": "alice",
        }),
        token,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "participant_mismatch");
    assert_eq!(node.orders(symbol()).len(), 1);

    let (status, _, body) = post_with(
        &app,
        "/api/cancel",
        json!({ "symbol": "BTC-USD", "order_id": order.order_id.to_hex() }),
        token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cancel: CancelAck = serde_json::from_value(body).expect("cancel ack");
    assert_eq!(cancel.offset, 1);

    let records = node.orders(symbol());
    assert_eq!(records.len(), 2);
    let cancel_command = Command::decode(&records[1].payload).expect("cancel payload");
    assert_eq!(cancel_command.kind, CommandKind::Cancel);
    assert_eq!(
        cancel_command.participant,
        ParticipantId::derive("desk-1"),
        "cancel must carry the authenticated participant"
    );

    let processed = node.process_orders(symbol());
    assert_eq!(processed.engine.book().order_count(), 0);
}

#[tokio::test]
async fn anonymous_cancel_ownership_is_enforced_by_the_engine() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "price_cents": 6_500_000,
            "qty": 10,
            "client_order_id": "owner-1",
            "participant": "alice",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let order: OrderAck = serde_json::from_value(body).expect("order ack");

    // The gateway records the attempt; the engine refuses a foreign owner.
    let (status, body) = post(
        &app,
        "/api/cancel",
        json!({
            "symbol": "BTC-USD",
            "order_id": order.order_id.to_hex(),
            "participant": "bob",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cancel: CancelAck = serde_json::from_value(body).expect("cancel ack");
    assert_eq!(cancel.offset, 1);

    let processed = node.process_orders(symbol());
    assert_eq!(
        processed.engine.book().order_count(),
        1,
        "another participant must not cancel alice's order"
    );
    assert!(
        processed.executions.iter().any(|exec| {
            exec.kind == ExecKind::Rejected && exec.reason == RejectReason::UnknownOrder
        }),
        "the engine rejects a cancel for an order the participant does not own"
    );
}

#[tokio::test]
async fn per_ip_rate_limit_returns_429_with_retry_after() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.rate_limit_per_sec = 1;
    }));
    let peer: SocketAddr = "203.0.113.9:4000".parse().expect("peer address");
    let limited = Ingress {
        peer: Some(peer),
        ..Ingress::default()
    };

    let (status, _, body) =
        post_with(&app, "/api/orders", limit_order("rl-1", "alice"), limited).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, headers, body) =
        post_with(&app, "/api/orders", limit_order("rl-2", "alice"), limited).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body["code"], "rate_limited");
    assert_eq!(
        headers
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    // The bucket is per source address; another peer still has capacity.
    let other = Ingress {
        peer: Some("203.0.113.10:4000".parse().expect("peer address")),
        ..Ingress::default()
    };
    let (status, _, body) =
        post_with(&app, "/api/orders", limit_order("rl-3", "alice"), other).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(node.orders(symbol()).len(), 2, "the 429 must not append");
}

/// The batch budget is charged after parsing and before validation: a batch
/// above the burst must be refused with 429 before the gateway spends any
/// validate work, and nothing may reach the journal.
#[tokio::test]
async fn batch_budget_is_charged_before_validation() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.rate_limit_per_sec = 2;
    }));
    let peer: SocketAddr = "203.0.113.11:4000".parse().expect("peer address");
    let limited = Ingress {
        peer: Some(peer),
        ..Ingress::default()
    };
    let validate_samples = || {
        node.ctx
            .metrics
            .stage_snapshot()
            .into_iter()
            .find(|(candidate, _)| *candidate == Stage::GatewayValidate)
            .map(|(_, summary)| summary.samples)
            .expect("gateway_validate is always in the snapshot")
    };
    assert_eq!(validate_samples(), 0, "a fresh node has no samples");

    // Three orders at a burst of 2: 1 token for the request plus 2 extra for
    // the batch cannot fit, so the handler refuses before validating.
    let orders: Vec<Value> = (0..3)
        .map(|index| limit_order(&format!("pre-charge-{index}"), "alice"))
        .collect();
    let (status, headers, body) = post_with(
        &app,
        "/api/orders/batch",
        json!({ "orders": orders }),
        limited,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["code"], "rate_limited");
    assert_eq!(
        headers
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("1"),
        "the handler's 429 carries the same Retry-After as the middleware"
    );
    assert!(node.orders(symbol()).is_empty(), "the 429 must not append");
    assert_eq!(node.ctx.metrics.snapshot().orders_ingressed, 0);
    assert_eq!(
        validate_samples(),
        0,
        "a rate-limited batch must be refused before validation work"
    );
}

/// A full 256-order batch fits a generous budget and appends all 256 orders.
#[tokio::test]
async fn a_full_batch_fits_a_generous_budget() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.rate_limit_per_sec = 1000;
    }));
    let peer: SocketAddr = "203.0.113.12:4000".parse().expect("peer address");
    let ingress = Ingress {
        peer: Some(peer),
        ..Ingress::default()
    };

    let orders: Vec<Value> = (0..MAX_BATCH_ORDERS)
        .map(|index| limit_order(&format!("full-{index}"), "alice"))
        .collect();
    let (status, _, body) = post_with(
        &app,
        "/api/orders/batch",
        json!({ "orders": orders }),
        ingress,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: BatchOrderAck = serde_json::from_value(body).expect("batch ack");
    assert_eq!(ack.count, MAX_BATCH_ORDERS as u32);
    assert_eq!(node.orders(symbol()).len(), MAX_BATCH_ORDERS);
    assert_eq!(
        node.ctx.metrics.snapshot().orders_ingressed,
        MAX_BATCH_ORDERS as u64,
        "one ingress record per order in the batch"
    );
}

#[tokio::test]
async fn malformed_bodies_share_one_rejection_contract() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let malformed = br#"{"symbol":"BTC-USD","side":"#.to_vec();

    // Truncated JSON: every ingress route answers the same 422, not axum's
    // bare-text 400 on `/api/cancel`.
    for uri in ["/api/orders", "/api/orders/batch", "/api/cancel"] {
        let (status, body) =
            post_raw(&app, uri, Some("application/json"), None, malformed.clone()).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{uri} must reject malformed JSON as 422: {body}"
        );
        assert_eq!(body["code"], "invalid_json", "{uri}");
    }

    // A non-JSON content type is the same 415 on every route.
    for uri in ["/api/orders", "/api/orders/batch", "/api/cancel"] {
        let (status, body) = post_raw(&app, uri, Some("text/plain"), None, b"{}".to_vec()).await;
        assert_eq!(
            status,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{uri} must reject a non-JSON content type as 415: {body}"
        );
        assert_eq!(body["code"], "unsupported_media_type", "{uri}");
    }

    assert!(
        node.orders(symbol()).is_empty(),
        "rejected bodies must not reach the journal"
    );
}

#[tokio::test]
async fn oversize_bodies_are_413_not_400() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let limit = 2 * 1024 * 1024;

    // Exactly at the cap the body is buffered; it fails JSON parsing (422),
    // not the size check.
    let (status, body) = post_raw(
        &app,
        "/api/orders",
        Some("application/json"),
        None,
        vec![b' '; limit],
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_json");

    // One byte over the cap is a payload-size rejection, not a malformed body.
    let (status, body) = post_raw(
        &app,
        "/api/orders",
        Some("application/json"),
        None,
        vec![b' '; limit + 1],
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["code"], "payload_too_large");

    // A declared length over the cap is refused before the body is read.
    let (status, body) = post_raw(
        &app,
        "/api/orders",
        Some("application/json"),
        Some(limit + 1),
        b"{}".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert_eq!(body["code"], "payload_too_large");

    assert!(
        node.orders(symbol()).is_empty(),
        "oversize bodies must not reach the journal"
    );
}

#[tokio::test]
async fn websocket_upgrades_are_capped() {
    let node = TestNode::new();
    // A small test-only cap keeps the test cheap; the production cap is 512 and
    // the guard reads it from the shared semaphore.
    let cap = 2;
    let app = gateway::router(node_with_ws_cap(&node, cap, |_| {}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a test listener");
    let addr = listener.local_addr().expect("local address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // Hold every accepted subscriber open: the permit lives with the socket, so
    // the upgrade above the cap must fail.
    let mut live = Vec::new();
    for index in 0..cap {
        let (stream, status) = open_websocket(addr).await;
        assert_eq!(status, 101, "upgrade {index} under the cap is accepted");
        live.push(stream);
    }
    let (overflow, status) = open_websocket(addr).await;
    assert_eq!(status, 503, "the upgrade above the cap must be refused");
    drop(overflow);
    assert_eq!(live.len(), cap);
}

#[tokio::test]
async fn websocket_per_ip_cap_is_enforced_and_released() {
    let node = TestNode::new();
    // Keep the global cap out of the way: this test is about the per-peer cap.
    let app = gateway::router(node_with_ws_cap(&node, 512, |config| {
        config.ws_per_ip = gateway::WS_PER_IP_LIMIT;
    }))
    .layer(axum::middleware::from_fn(with_test_peer));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a test listener");
    let addr = listener.local_addr().expect("local address");
    let metrics_app = app.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    let peer = "203.0.113.9:4000";
    let other = "203.0.113.10:4000";
    let cap = gateway::WS_PER_IP_LIMIT;

    // One peer fills its sub-limit; the upgrade above it is refused.
    let mut live = Vec::new();
    for index in 0..cap {
        let (stream, status) = open_websocket_as(addr, Some(peer)).await;
        assert_eq!(
            status, 101,
            "upgrade {index} under the per-IP cap is accepted"
        );
        live.push(stream);
    }
    let (overflow, status) = open_websocket_as(addr, Some(peer)).await;
    assert_eq!(
        status, 503,
        "the upgrade above the per-IP cap must be refused"
    );
    drop(overflow);

    // A different peer still gets its own full budget.
    let (other_stream, status) = open_websocket_as(addr, Some(other)).await;
    assert_eq!(status, 101, "a second peer is not blocked by the first");

    // Both the gauge and the rejection counter are exported.
    let (status, text) = get_text(&metrics_app, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        metric_value(&text, "whippet_ws_connections "),
        (cap + 1) as f64,
        "only the accepted upgrades count as active"
    );
    assert_eq!(metric_value(&text, "whippet_ws_rejected_total "), 1.0);

    // Closing a socket releases the per-peer slot (and the global permit), so
    // the same peer can upgrade again.
    live.pop();
    let mut reconnected = None;
    for _ in 0..50 {
        let (stream, status) = open_websocket_as(addr, Some(peer)).await;
        if status == 101 {
            reconnected = Some(stream);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        reconnected.is_some(),
        "dropping a socket must free its per-peer slot"
    );
    drop(reconnected);
    drop(other_stream);
    drop(live);
}

#[tokio::test]
async fn metrics_expose_validate_and_cancel_stages() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let samples = |stage: Stage| {
        node.ctx
            .metrics
            .stage_snapshot()
            .into_iter()
            .find(|(candidate, _)| *candidate == stage)
            .map(|(_, summary)| summary.samples)
            .expect("every stage is in the snapshot")
    };

    // Cancel-only traffic first: the four gateway stages must all be recorded
    // by `/api/cancel`, not just by the order paths.
    let (status, body) = post(
        &app,
        "/api/cancel",
        json!({
            "symbol": "BTC-USD",
            "order_id": OrderId::derive(symbol(), "metrics-cancel").to_hex(),
            "participant": "alice",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for stage in [
        Stage::GatewayParse,
        Stage::GatewayValidate,
        Stage::GatewayEncode,
        Stage::GatewayAppend,
    ] {
        assert_eq!(
            samples(stage),
            1,
            "/api/cancel must record exactly one {} sample",
            stage.as_str()
        );
    }

    // The order paths record validate as their own stage too, once per
    // request, not once per order.
    assert_eq!(
        samples(Stage::GatewayValidate),
        1,
        "the cancel walk above leaves one sample"
    );
    let (status, body) = post(&app, "/api/orders", limit_order("metrics-1", "alice")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(samples(Stage::GatewayValidate), 2);
    let (status, body) = post(
        &app,
        "/api/orders/batch",
        json!({ "orders": [limit_order("metrics-2", "alice"), limit_order("metrics-3", "alice")] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        samples(Stage::GatewayValidate),
        3,
        "a two-order batch is still one validate sample"
    );

    let (status, text) = get_text(&app, "/metrics").await;
    assert_eq!(status, StatusCode::OK);
    for stage in [
        "gateway_parse",
        "gateway_validate",
        "gateway_encode",
        "gateway_append",
    ] {
        for quantile in ["p50", "p99"] {
            let prefix = format!("whippet_stage_{quantile}_us{{stage=\"{stage}\"}}");
            let value = metric_value(&text, &prefix);
            assert!(
                value >= 0.0 && value.is_finite(),
                "stage {stage} must expose a non-negative sample, got {value}"
            );
        }
    }
    assert_eq!(metric_value(&text, "whippet_ws_connections "), 0.0);
    assert_eq!(metric_value(&text, "whippet_ws_rejected_total "), 0.0);
    assert!(text.contains("# TYPE whippet_ws_connections gauge"));
    assert!(text.contains("# TYPE whippet_ws_rejected_total counter"));
}

#[tokio::test]
async fn readyz_tracks_journal_and_shard_health() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = get(&app, "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ready"], true);
    assert_eq!(body["journal"], true);
    assert_eq!(body["shards_ok"], true);

    node.ctx.mark_shard_failed();
    let (status, body) = get(&app, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["ready"], false);
    assert_eq!(body["shards_ok"], false);
}

#[tokio::test]
async fn batch_orders_are_one_append_with_contiguous_offsets() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let orders: Vec<Value> = (0..3)
        .map(|index| {
            json!({
                "symbol": "BTC-USD",
                "side": "buy",
                "order_type": "limit",
                "price_cents": 6_500_000 + index as i64 * 100,
                "qty": 1 + index,
                "client_order_id": format!("batch-{index}"),
                "participant": "alice",
            })
        })
        .collect();

    let (status, body) = post(&app, "/api/orders/batch", json!({ "orders": orders })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: BatchOrderAck = serde_json::from_value(body).expect("batch ack");
    assert_eq!(ack.partition, 1, "BTC-USD must stay on partition 1");
    assert_eq!(ack.base_offset, 0);
    assert_eq!(ack.count, 3);
    assert_eq!(ack.mode, Mode::Durable);
    assert!(ack.durable_ack_ms >= 0.0);
    assert_eq!(ack.orders.len(), 3);
    for (index, order) in ack.orders.iter().enumerate() {
        assert_eq!(order.offset, index as u64);
        assert_eq!(
            order.order_id,
            OrderId::derive(symbol(), &format!("batch-{index}"))
        );
        assert_eq!(order.partition, 1);
    }

    let records = node.orders(symbol());
    assert_eq!(records.len(), 3, "one append produced all three records");
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    for (index, record) in records.iter().enumerate() {
        let command = Command::decode(&record.payload).expect("command payload");
        assert_eq!(command.kind, CommandKind::NewOrder);
        assert_eq!(command.order_id, ack.orders[index].order_id);
        assert_eq!(command.participant, ParticipantId::derive("alice"));
        assert_eq!(record.header("type"), Some("order"));
    }
    assert_eq!(node.ctx.metrics.snapshot().orders_ingressed, 3);
}

#[tokio::test]
async fn batch_acks_can_be_omitted_for_the_bench_path() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders/batch?acks=none",
        json!({
            "orders": [
                limit_order("no-acks-1", "alice"),
                limit_order("no-acks-2", "alice"),
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: BatchOrderAck = serde_json::from_value(body).expect("batch ack");
    assert_eq!(ack.count, 2);
    assert_eq!(ack.base_offset, 0);
    assert!(ack.orders.is_empty(), "acks=none omits the per-order array");
    assert_eq!(node.orders(symbol()).len(), 2);
}

#[tokio::test]
async fn mixed_symbol_batches_are_rejected_before_append() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders/batch",
        json!({
            "orders": [
                limit_order("mixed-1", "alice"),
                {
                    "symbol": "ETH-USD",
                    "side": "buy",
                    "price_cents": 300_000,
                    "qty": 1,
                    "client_order_id": "mixed-2",
                    "participant": "alice",
                }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "mixed_symbols");
    assert!(node.orders(symbol()).is_empty());
    assert!(
        node.orders(Symbol::new("ETH-USD").expect("symbol"))
            .is_empty()
    );
}

#[tokio::test]
async fn one_invalid_order_rejects_the_whole_batch() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders/batch",
        json!({
            "orders": [
                limit_order("all-1", "alice"),
                {
                    "symbol": "BTC-USD",
                    "side": "buy",
                    "price_cents": 6_500_000,
                    "qty": 0,
                    "client_order_id": "all-2",
                    "participant": "alice",
                },
                limit_order("all-3", "alice"),
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_qty");
    let error = body["error"].as_str().expect("error text");
    assert!(
        error.contains("order 1"),
        "error must name the failing index: {error}"
    );
    assert!(
        node.orders(symbol()).is_empty(),
        "no order from the batch may be appended"
    );
}

#[tokio::test]
async fn batches_above_the_iggy_message_cap_are_rejected() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let orders: Vec<Value> = (0..=MAX_BATCH_ORDERS)
        .map(|index| limit_order(&format!("cap-{index}"), "alice"))
        .collect();

    let (status, body) = post(&app, "/api/orders/batch", json!({ "orders": orders })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "batch_too_large");
    assert!(node.orders(symbol()).is_empty());
}

#[test]
fn borrowed_dtos_deserialize_the_same_json_as_owned() {
    // The borrowed hot-path DTOs accept exactly the same JSON (including
    // defaults) as the owned DTOs; they only borrow the strings.
    let json = r#"{"orders":[
        {"symbol":"BTC-USD","side":"buy","order_type":"limit","price_cents":6500000,"qty":10,"client_order_id":"ref-1","participant":"alice"},
        {"symbol":"BTC-USD","side":"sell","price_cents":6500100,"qty":4}
    ]}"#;
    let owned: BatchOrderRequest = serde_json::from_str(json).unwrap();
    let borrowed: BatchOrderRequestRef<'_> = serde_json::from_str(json).unwrap();
    assert_eq!(borrowed.orders.len(), owned.orders.len());
    for (borrowed, owned) in borrowed.orders.iter().zip(owned.orders.iter()) {
        let expected: OrderRequest =
            serde_json::from_value(serde_json::to_value(borrowed).expect("borrowed serializes"))
                .expect("borrowed round-trips through the owned DTO");
        assert_eq!(expected.symbol, owned.symbol);
        assert_eq!(expected.side, owned.side);
        assert_eq!(expected.order_type, owned.order_type);
        assert_eq!(expected.price_cents, owned.price_cents);
        assert_eq!(expected.qty, owned.qty);
        assert_eq!(expected.client_order_id, owned.client_order_id);
        assert_eq!(expected.participant, owned.participant);
    }

    // Defaults match when optional fields are omitted.
    let single = r#"{"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":1}"#;
    let owned: OrderRequest = serde_json::from_str(single).unwrap();
    let borrowed: OrderRequestRef<'_> = serde_json::from_str(single).unwrap();
    assert_eq!(borrowed.order_type, owned.order_type);
    assert_eq!(borrowed.client_order_id, owned.client_order_id.as_deref());
    assert_eq!(borrowed.participant, owned.participant.as_deref());
}

#[tokio::test]
async fn borrowed_ingress_matches_owned_validation_results() {
    // The borrowed ingress path validates to the same acks and the same error
    // codes as the owned contract.
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "price_cents": 6_500_000,
            "qty": 10,
            "client_order_id": "ref-ok-1",
            "participant": "bob",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: OrderAck = serde_json::from_value(body).expect("order ack");
    assert_eq!(ack.order_id, OrderId::derive(symbol(), "ref-ok-1"));
    let records = node.orders(symbol());
    assert_eq!(records.len(), 1);
    let command = Command::decode(&records[0].payload).expect("command payload");
    assert_eq!(command.participant, ParticipantId::derive("bob"));

    // Repeating the same anonymous participant resolves to the same id
    // through the bounded derivation cache.
    let (status, body) = post(
        &app,
        "/api/orders",
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "price_cents": 6_500_000,
            "qty": 1,
            "client_order_id": "ref-ok-2",
            "participant": "bob",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let records = node.orders(symbol());
    assert_eq!(records.len(), 2);
    let second = Command::decode(&records[1].payload).expect("command payload");
    assert_eq!(second.participant, command.participant);

    let cases = [
        (
            json!({"symbol": "BTC-USD", "side": "buy", "price_cents": 6_500_000, "qty": 0}),
            "invalid_qty",
        ),
        (
            json!({"symbol": "BTC-USD", "side": "buy", "qty": 1}),
            "missing_price",
        ),
        (
            json!({"symbol": "NOPE-USD", "side": "buy", "price_cents": 6_500_000, "qty": 1}),
            "unknown_symbol",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "price_cents": 6_500_000, "qty": 1,
                "client_order_id": "x".repeat(129),
            }),
            "invalid_client_order_id",
        ),
        (
            json!({
                "symbol": "BTC-USD", "side": "buy", "price_cents": 6_500_000, "qty": 1,
                "participant": "p".repeat(65),
            }),
            "invalid_participant",
        ),
    ];
    for (request, expected_code) in cases {
        let (status, body) = post(&app, "/api/orders", request).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "expected 400 for {expected_code}: {body}"
        );
        assert_eq!(body["code"], expected_code);
    }
    assert_eq!(
        node.orders(symbol()).len(),
        2,
        "rejected requests must not reach the journal"
    );
}

#[tokio::test]
async fn ingress_fails_closed_when_the_shard_is_down() {
    let node = common::TestNode::new();
    let symbol = whippet_core::ids::Symbol::new("BTC-USD").unwrap();
    let before = node.orders(symbol).len();
    node.ctx
        .shards_ok
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let app = gateway::router(Arc::clone(&node.ctx));

    let (status, body) = post(
        &app,
        "/api/orders",
        json!({"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":1}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "shard_unavailable");
    assert_eq!(
        node.orders(symbol).len(),
        before,
        "a rejected order must not be appended"
    );
}

#[tokio::test]
async fn escaped_json_strings_still_validate() {
    let node = common::TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    // `\u0063` is `c`; the borrowed fast path cannot represent it, so the
    // gateway must fall back to the owned DTO instead of rejecting the body.
    let raw = r#"{"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":1,"participant":"ali\u0063e"}"#;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/orders")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(raw))
                .expect("request"),
        )
        .await
        .expect("router responds");
    assert_eq!(response.status(), StatusCode::OK);
}

#[test]
fn engine_journal_pool_size_is_clamped_into_bounds() {
    let max = whippet::context::MAX_ENGINE_JOURNAL_CLIENTS;
    // Default is the shard count; a zero shard count still yields 1.
    assert_eq!(whippet::context::engine_journal_pool_size(0, None), 1);
    assert_eq!(whippet::context::engine_journal_pool_size(1, None), 1);
    assert_eq!(whippet::context::engine_journal_pool_size(4, None), 4);
    // The default never exceeds the cap.
    assert_eq!(whippet::context::engine_journal_pool_size(1_024, None), max);
    // Overrides clamp into [1, max]: no empty pool, no unbounded pool.
    assert_eq!(whippet::context::engine_journal_pool_size(4, Some(0)), 1);
    assert_eq!(whippet::context::engine_journal_pool_size(4, Some(2)), 2);
    assert_eq!(
        whippet::context::engine_journal_pool_size(4, Some(max + 64)),
        max
    );
}

#[test]
fn engine_journal_for_is_deterministic_and_in_range() {
    let node = common::TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    // The pool is at least 1 and at most the configured maximum.
    assert!(!ctx.engine_journals.is_empty());
    assert!(ctx.engine_journals.len() <= whippet::context::MAX_ENGINE_JOURNAL_CLIENTS);
    for shard in [0u32, 1, 7, 31, u32::MAX] {
        let first = ctx.engine_journal_for(shard);
        // The selection is a pool entry.
        assert!(
            ctx.engine_journals
                .iter()
                .any(|entry| Arc::ptr_eq(entry, first)),
            "shard {shard} selected an entry outside the pool"
        );
        // The same shard selects the same entry every time.
        assert!(
            Arc::ptr_eq(first, ctx.engine_journal_for(shard)),
            "shard {shard} selection is not deterministic"
        );
    }
    // Shard 0 resolves to the observer entry.
    assert!(Arc::ptr_eq(ctx.engine_journal_for(0), &ctx.engine_journal));
}

#[tokio::test]
async fn memory_mode_pool_shares_one_journal_arc() {
    let node = common::TestNode::new();
    let ctx = Arc::clone(&node.ctx);
    // Memory mode cannot multiplex clients: every entry is the same Arc.
    for (index, entry) in ctx.engine_journals.iter().enumerate() {
        assert!(
            Arc::ptr_eq(&ctx.journal, entry),
            "pool entry {index} must be the shared memory journal"
        );
    }
    assert!(Arc::ptr_eq(&ctx.journal, &ctx.engine_journal));
}

#[test]
fn engine_journal_pool_size_covers_one_three_and_sixteen() {
    // The default matches the shard count for the small, medium, and cap
    // pool sizes.
    assert_eq!(whippet::context::engine_journal_pool_size(1, None), 1);
    assert_eq!(whippet::context::engine_journal_pool_size(3, None), 3);
    assert_eq!(whippet::context::engine_journal_pool_size(16, None), 16);
    // The override takes those sizes too, and clamps past the cap.
    assert_eq!(whippet::context::engine_journal_pool_size(16, Some(1)), 1);
    assert_eq!(whippet::context::engine_journal_pool_size(16, Some(3)), 3);
    assert_eq!(whippet::context::engine_journal_pool_size(3, Some(16)), 16);
    assert_eq!(
        whippet::context::engine_journal_pool_size(3, Some(1_024)),
        whippet::context::MAX_ENGINE_JOURNAL_CLIENTS
    );
}

#[test]
fn engine_journal_pending_budget_splits_the_total_with_a_floor() {
    use whippet::context::{
        BYTES_PER_MIB, MAX_ENGINE_JOURNAL_PENDING_MIB, MIN_ENGINE_JOURNAL_PENDING_MIB,
        engine_journal_pending_budget,
    };
    let mib = BYTES_PER_MIB;
    assert_eq!(MIN_ENGINE_JOURNAL_PENDING_MIB, 16);
    assert_eq!(MAX_ENGINE_JOURNAL_PENDING_MIB, 4096);

    // Pool of one keeps the whole default budget.
    let (total, per) = engine_journal_pending_budget(None, 1);
    assert_eq!(total, 256 * mib);
    assert_eq!(per, 256 * mib);

    // Pool of three: 256 MiB / 3 = 89,478,485 bytes (~85.33 MiB) per client;
    // the split is byte-accurate, so nothing of the total is left unspent.
    let (total, per) = engine_journal_pending_budget(None, 3);
    assert_eq!(total, 256 * mib);
    assert_eq!(per, total / 3);
    assert_eq!(per, 89_478_485);

    // Pool of sixteen: the default lands exactly on the 16 MiB floor.
    let (total, per) = engine_journal_pending_budget(None, 16);
    assert_eq!(total, 256 * mib);
    assert_eq!(per, 16 * mib);

    // A total at the floor is smaller than the floor across three clients:
    // each client is still floored at 16 MiB, so the effective spend (48
    // MiB) exceeds the configured total. That is the documented trade: no
    // client is starved.
    let (total, per) = engine_journal_pending_budget(Some(16), 3);
    assert_eq!(total, 16 * mib);
    assert_eq!(per, 16 * mib);

    // Overrides clamp into [16, 4096] MiB before the split.
    assert_eq!(engine_journal_pending_budget(Some(0), 3).0, 16 * mib);
    assert_eq!(
        engine_journal_pending_budget(Some(usize::MAX), 3).0,
        4096 * mib
    );
}

/// A context over the test node's state with a hand-built engine pool, the
/// way an iggy-mode pool is wired: distinct handles per entry and the
/// observer sharing entry 0.
fn ctx_with_engine_pool(
    node: &TestNode,
    pool: Vec<Arc<whippet_journal::Journal>>,
    observer: Arc<whippet_journal::Journal>,
) -> Arc<NodeContext> {
    Arc::new(NodeContext {
        config: node.ctx.config.clone(),
        journal: Arc::clone(&node.ctx.journal),
        engine_journals: pool,
        engine_journal: observer,
        group_commit: node.ctx.group_commit.clone(),
        hub: Arc::clone(&node.ctx.hub),
        metrics: Arc::clone(&node.ctx.metrics),
        snapshots: node.ctx.snapshots.clone(),
        started: node.ctx.started,
        shutdown: node.ctx.shutdown.clone(),
        shards_ok: Arc::clone(&node.ctx.shards_ok),
        rate_limiter: Arc::clone(&node.ctx.rate_limiter),
        ws_connections: Arc::clone(&node.ctx.ws_connections),
        snapshot_lock: Arc::clone(&node.ctx.snapshot_lock),
        turbo: None,
    })
}

#[tokio::test]
async fn engine_journal_for_selects_distinct_entries_when_the_pool_has_them() {
    // Memory mode shares one Arc (`memory_mode_pool_shares_one_journal_arc`),
    // so distinct handles only exist in iggy mode. This test wires two
    // distinct memory journals into the pool to pin the selection contract
    // that mode relies on: shard 0 gets entry 0 (the observer's twin) and
    // shard 1 gets a different handle.
    let node = TestNode::new();
    let first = Arc::new(whippet_journal::Journal::memory());
    let second = Arc::new(whippet_journal::Journal::memory());
    assert!(
        !Arc::ptr_eq(&first, &second),
        "the two pool entries must be distinct journals"
    );
    let ctx = ctx_with_engine_pool(
        &node,
        vec![Arc::clone(&first), Arc::clone(&second)],
        Arc::clone(&first),
    );
    assert!(Arc::ptr_eq(ctx.engine_journal_for(0), &first));
    assert!(Arc::ptr_eq(ctx.engine_journal_for(1), &second));
    assert!(
        !Arc::ptr_eq(ctx.engine_journal_for(0), ctx.engine_journal_for(1)),
        "shards 0 and 1 must select different pool entries here"
    );
    // The observer still shares entry 0, and shard 0 still resolves to it.
    assert!(Arc::ptr_eq(ctx.engine_journal_for(0), &ctx.engine_journal));
}
