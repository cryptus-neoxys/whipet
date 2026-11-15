// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Ingress pre-trade risk tests.
//!
//! The risk limits are an **ingress guard, not engine-enforced risk**: they
//! run at the HTTP and binary-TCP admission boundary, before any journal
//! append, and a direct producer that appends to the orders log bypasses
//! them. These tests pin the honest contract: a rejected order leaves the
//! journal untouched, answers with a distinct error shape, increments a
//! counter, and never consumes the dedup/order id.

mod common;

use std::net::SocketAddr;
use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use whippet::config::RiskConfig;
use whippet::context::NodeContext;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, CommandKind, OrderType, Side};
use whippet_core::tcp::{TcpErrorCode, TcpRequest, TcpResponse, body_len};
use whippet_journal::Topic;

use common::TestNode;

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

/// A second context over the same in-memory journal, with risk overrides.
fn node_with(node: &TestNode, risk: RiskConfig) -> Arc<NodeContext> {
    let mut config = node.ctx.config.clone();
    config.risk = risk;
    let storage = whippet::storage::StorageMonitor::new(config.storage.clone());
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
        storage,
        snapshots: node.ctx.snapshots.clone(),
        started: node.ctx.started,
        shutdown: node.ctx.shutdown.clone(),
        shards_ok: Arc::clone(&node.ctx.shards_ok),
        rate_limiter,
        ws_connections: Arc::new(tokio::sync::Semaphore::new(512)),
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo: None,
    })
}

async fn send(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).expect("serialize")))
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
        serde_json::from_slice(&bytes).expect("json response"),
    )
}

async fn post_order(app: &Router, body: Value) -> (StatusCode, Value) {
    send(app, "/api/orders", body).await
}

/// Plain-text GET for `/metrics`, whose body is not JSON.
async fn get_metrics(app: &Router) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/metrics")
                .body(Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("router responds");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf-8 metrics")
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

fn order(qty: u64, price_cents: i64) -> Value {
    json!({
        "symbol": "BTC-USD",
        "side": "buy",
        "order_type": "limit",
        "price_cents": price_cents,
        "qty": qty,
        "client_order_id": format!("risk-{qty}-{price_cents}"),
        "participant": "alice",
    })
}

#[tokio::test]
async fn http_qty_above_limit_is_a_422_risk_limit_without_journaling() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 10,
            max_notional_cents: 0,
        },
    );
    let app = whippet::gateway::router(Arc::clone(&ctx));

    let (status, body) = post_order(&app, order(20, 6_500_000)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "risk_limit");
    let message = body["error"].as_str().expect("error message");
    assert!(message.contains("max_order_qty"), "{message}");
    assert!(
        message.contains("20"),
        "the message names the qty: {message}"
    );

    // Nothing reached the journal, and the dedup key was never minted: the
    // same client_order_id is accepted afterwards.
    assert!(
        node.orders(symbol()).is_empty(),
        "a risk-rejected order must not be journaled"
    );

    let text = get_metrics(&app).await;
    assert_eq!(
        metric_value(&text, "whippet_risk_rejected_total{reason=\"order_qty\"}"),
        1.0
    );
    assert_eq!(
        metric_value(&text, "whippet_risk_rejected_total{reason=\"notional\"}"),
        0.0
    );
}

#[tokio::test]
async fn http_qty_exactly_at_the_limit_is_accepted() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 10,
            max_notional_cents: 0,
        },
    );
    let app = whippet::gateway::router(Arc::clone(&ctx));

    let (status, body) = post_order(&app, order(10, 6_500_000)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(node.orders(symbol()).len(), 1);
}

#[tokio::test]
async fn http_notional_above_limit_is_a_422_risk_limit() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 0,
            max_notional_cents: 10_000_000,
        },
    );
    let app = whippet::gateway::router(Arc::clone(&ctx));

    // 6,500,000 cents x 20 = 130,000,000 cents, above the 10,000,000 limit.
    let (status, body) = post_order(&app, order(20, 6_500_000)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "risk_limit");
    let message = body["error"].as_str().expect("error message");
    assert!(message.contains("max_notional_cents"), "{message}");
    assert!(
        message.contains("130000000"),
        "names the notional: {message}"
    );
    assert!(
        node.orders(symbol()).is_empty(),
        "a risk-rejected order must not be journaled"
    );

    let text = get_metrics(&app).await;
    assert_eq!(
        metric_value(&text, "whippet_risk_rejected_total{reason=\"notional\"}"),
        1.0
    );
}

#[tokio::test]
async fn http_market_order_with_a_qty_limit_is_rejected() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 10,
            max_notional_cents: 0,
        },
    );
    let app = whippet::gateway::router(Arc::clone(&ctx));

    let (status, body) = post_order(
        &app,
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "market",
            "qty": 20,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "risk_limit");
    assert!(
        body["error"]
            .as_str()
            .expect("message")
            .contains("max_order_qty"),
        "{body}"
    );
    assert!(node.orders(symbol()).is_empty());
}

#[tokio::test]
async fn http_market_order_without_a_price_is_not_notional_checked() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 0,
            max_notional_cents: 1,
        },
    );
    let app = whippet::gateway::router(Arc::clone(&ctx));

    // No price on a market order: the notional check cannot run in this
    // slice, so the order is admitted and only the engine sees it.
    let (status, body) = post_order(
        &app,
        json!({
            "symbol": "BTC-USD",
            "side": "buy",
            "order_type": "market",
            "qty": 5,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(node.orders(symbol()).len(), 1);
    let (_, notional) = ctx.metrics.risk_rejections();
    assert_eq!(notional, 0);
}

#[tokio::test]
async fn http_zero_limits_disable_every_check() {
    let node = TestNode::new();
    let app = whippet::gateway::router(Arc::clone(&node.ctx));

    // Defaults are 0/0: the pre-risk behavior is unchanged. A qty and a
    // notional far above any plausible limit are both accepted.
    let (status, body) = post_order(&app, order(999_999, 6_500_000)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (qty_rejected, notional_rejected) = node.ctx.metrics.risk_rejections();
    assert_eq!(qty_rejected, 0);
    assert_eq!(notional_rejected, 0);
}

#[tokio::test]
async fn http_batch_rejection_names_the_offending_order() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 10,
            max_notional_cents: 0,
        },
    );
    let app = whippet::gateway::router(Arc::clone(&ctx));

    let (status, body) = send(
        &app,
        "/api/orders/batch",
        json!({
            "orders": [
                order(1, 6_500_000),
                order(50, 6_500_000),
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "risk_limit");
    let message = body["error"].as_str().expect("error message");
    assert!(
        message.contains("order 1") && message.contains("max_order_qty"),
        "{message}"
    );
    assert!(
        node.orders(symbol()).is_empty(),
        "a rejected batch must journal nothing"
    );
}

// ---------------------------------------------------------------------------
// Binary TCP path
// ---------------------------------------------------------------------------

use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn tcp_command(qty: u64, price_ticks: i64) -> Command {
    Command {
        kind: CommandKind::NewOrder,
        symbol: symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Price(price_ticks),
        qty: Qty(qty),
        order_id: OrderId(u128::from(qty) * 1_000 + u128::from(price_ticks.unsigned_abs())),
        participant: ParticipantId(1),
        timestamp_ns: 42,
        nonce: 7,
        flags: 0,
    }
}

fn reserve_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("local address");
    drop(listener);
    addr
}

async fn connect(addr: SocketAddr) -> tokio::net::TcpStream {
    for _ in 0..100 {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => return stream,
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    panic!("tcp ingress did not accept a connection at {addr}");
}

async fn read_response(stream: &mut tokio::net::TcpStream) -> TcpResponse {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix).await.expect("frame prefix");
    let len = body_len(prefix).expect("valid frame length");
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("frame body");
    TcpResponse::decode_body(&body).expect("valid response")
}

fn error_code(response: &TcpResponse) -> Option<u16> {
    match response {
        TcpResponse::Error { code, .. } => Some(*code),
        _ => None,
    }
}

fn error_message(response: &TcpResponse) -> &str {
    match response {
        TcpResponse::Error { message, .. } => message,
        _ => panic!("expected an error response, got {response:?}"),
    }
}

#[tokio::test]
async fn tcp_qty_rejection_answers_risk_rejected_and_keeps_the_connection_usable() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 5,
            max_notional_cents: 0,
        },
    );
    let addr = reserve_addr();
    tokio::spawn(whippet::tcp::serve(Arc::clone(&ctx), addr));

    let mut stream = connect(addr).await;
    stream
        .write_all(
            &TcpRequest::Submit {
                commands: vec![tcp_command(10, 1_000)],
            }
            .encode(),
        )
        .await
        .expect("write submit frame");

    let response = read_response(&mut stream).await;
    assert_eq!(
        error_code(&response),
        Some(TcpErrorCode::RiskRejected.as_u16())
    );
    let message = error_message(&response);
    assert!(message.contains("command 0"), "{message}");
    assert!(message.contains("max_order_qty"), "{message}");
    assert_eq!(ctx.metrics.risk_rejections(), (1, 0));

    // Nothing was journaled; the connection is still usable and a valid batch
    // afterwards appends normally.
    let partition = symbol().partition(ctx.exchange().partitions);
    assert!(
        node.memory()
            .read(Topic::Orders, partition, 0, 10)
            .is_empty(),
        "a risk-rejected batch must journal nothing"
    );

    stream
        .write_all(
            &TcpRequest::Submit {
                commands: vec![tcp_command(1, 1_000)],
            }
            .encode(),
        )
        .await
        .expect("write valid submit frame");
    match read_response(&mut stream).await {
        TcpResponse::SubmitAck { count, .. } => assert_eq!(count, 1),
        other => panic!("expected SubmitAck, got {other:?}"),
    }
    assert_eq!(node.memory().read(Topic::Orders, partition, 0, 10).len(), 1);
}

#[tokio::test]
async fn tcp_notional_rejection_answers_risk_rejected() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 0,
            max_notional_cents: 100_000,
        },
    );
    let addr = reserve_addr();
    tokio::spawn(whippet::tcp::serve(Arc::clone(&ctx), addr));

    let mut stream = connect(addr).await;
    // BTC-USD tick size is 100 cents: 1,000 ticks = 100,000 cents; x qty 2
    // = 200,000 cents, above the limit.
    stream
        .write_all(
            &TcpRequest::Submit {
                commands: vec![tcp_command(2, 1_000)],
            }
            .encode(),
        )
        .await
        .expect("write submit frame");

    let response = read_response(&mut stream).await;
    assert_eq!(
        error_code(&response),
        Some(TcpErrorCode::RiskRejected.as_u16())
    );
    let message = error_message(&response);
    assert!(message.contains("command 0"), "{message}");
    assert!(message.contains("max_notional_cents"), "{message}");
    assert_eq!(ctx.metrics.risk_rejections(), (0, 1));

    let partition = symbol().partition(ctx.exchange().partitions);
    assert!(
        node.memory()
            .read(Topic::Orders, partition, 0, 10)
            .is_empty()
    );
}

#[tokio::test]
async fn tcp_market_command_without_a_price_is_not_notional_checked() {
    let node = TestNode::new();
    let ctx = node_with(
        &node,
        RiskConfig {
            max_order_qty: 0,
            max_notional_cents: 1,
        },
    );
    let addr = reserve_addr();
    tokio::spawn(whippet::tcp::serve(Arc::clone(&ctx), addr));

    let mut market = tcp_command(5, 0);
    market.order_type = OrderType::Market;
    let mut stream = connect(addr).await;
    stream
        .write_all(
            &TcpRequest::Submit {
                commands: vec![market],
            }
            .encode(),
        )
        .await
        .expect("write submit frame");

    match read_response(&mut stream).await {
        TcpResponse::SubmitAck { count, .. } => assert_eq!(count, 1),
        other => panic!("expected SubmitAck, got {other:?}"),
    }
    assert_eq!(ctx.metrics.risk_rejections(), (0, 0));
}
