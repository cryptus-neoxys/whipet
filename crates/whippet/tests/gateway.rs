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

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use whippet::gateway;
use whippet_core::api::{CancelAck, HealthView, OrderAck, StateView};
use whippet_core::config::Mode;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, CommandKind, ExecKind, MdKind, RejectReason, Side};

use common::TestNode;

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

async fn send(app: &Router, method: Method, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
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

async fn post(app: &Router, uri: &str, body: Value) -> (StatusCode, Value) {
    send(app, Method::POST, uri, Some(body)).await
}

async fn get(app: &Router, uri: &str) -> (StatusCode, Value) {
    send(app, Method::GET, uri, None).await
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
    // The memory journal is a supported test/dev backend, so readiness is
    // "ok" even though no Iggy connection exists.
    assert_eq!(health.status, "ok");
}
