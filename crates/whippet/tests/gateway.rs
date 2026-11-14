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
    BatchOrderAck, CancelAck, HealthView, MAX_BATCH_ORDERS, OrderAck, StateView,
};
use whippet_core::config::Mode;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, CommandKind, ExecKind, MdKind, RejectReason, Side};

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
    let mut config = node.ctx.config.clone();
    configure(&mut config);
    Arc::new(NodeContext {
        config,
        journal: Arc::clone(&node.ctx.journal),
        group_commit: node.ctx.group_commit.clone(),
        hub: Arc::clone(&node.ctx.hub),
        metrics: Arc::clone(&node.ctx.metrics),
        snapshots: node.ctx.snapshots.clone(),
        started: node.ctx.started,
        shutdown: node.ctx.shutdown.clone(),
        shards_ok: Arc::clone(&node.ctx.shards_ok),
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
