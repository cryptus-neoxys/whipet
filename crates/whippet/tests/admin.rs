// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Operator halt/resume admin endpoint tests.
//!
//! The router is exercised in-process with `tower::ServiceExt::oneshot` over
//! the real memory journal, so assertions are about appended bytes: the Halt
//! and Resume commands land in the Orders topic in offset order, and one JSON
//! audit record per action lands in the Audit topic. The session endpoint
//! follows the same contract with a `set_session` command.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use whippet::auth::ApiToken;
use whippet::config::{JournalKind, NodeConfig};
use whippet::context::NodeContext;
use whippet::gateway;
use whippet::turbo::{TurboConfig, TurboIngress, turbo_ack};
use whippet_core::api::AdminAck;
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{ParticipantId, Symbol};
use whippet_core::protocol::{Command, CommandKind, ExecKind, RejectReason, SessionState};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{Journal, JournalRecord, MemoryJournal, Topic};
use whippet_marketdata::MdHub;

use common::TestNode;

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

/// Operator credential used by the configured-node tests.
const OPERATOR_TOKEN: &str = "op-s3cret";
const OPERATOR_NAME: &str = "ops-lead";

fn operator_tokens() -> Vec<ApiToken> {
    vec![ApiToken {
        token: OPERATOR_TOKEN.into(),
        participant: OPERATOR_NAME.into(),
    }]
}

/// A second context over the same in-memory journal, with config overrides.
/// Same shape as the `node_with` helper in `tests/gateway.rs`.
fn node_with(node: &TestNode, configure: impl FnOnce(&mut NodeConfig)) -> Arc<NodeContext> {
    let mut config = node.ctx.config.clone();
    configure(&mut config);
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
        health: node.ctx.health.clone(),
        active_shards: node.ctx.active_shards.clone(),
        rate_limiter,
        ws_connections: Arc::clone(&node.ctx.ws_connections),
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo: None,
    })
}

fn node_with_operator(node: &TestNode) -> Arc<NodeContext> {
    node_with(node, |config| {
        config.operator_tokens = operator_tokens();
    })
}

/// Operator tokens plus a trading token, so the tests can prove the two
/// namespaces are separate.
fn node_with_operator_and_trader(node: &TestNode) -> Arc<NodeContext> {
    node_with(node, |config| {
        config.api_token = Some("trader-s3cret".into());
        config.api_participant = "desk-1".into();
        config.operator_tokens = operator_tokens();
    })
}

/// POST one admin action with an optional bearer token.
async fn post_admin(
    app: &Router,
    action: &str,
    symbol: &str,
    body: Option<Value>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    post_admin_with_peer(app, action, symbol, body, token, None).await
}

/// Same as [`post_admin`], with an optional synthetic peer for the rate-limit
/// middleware (a real listener supplies `ConnectInfo`; in-process callers do
/// not, and the limiter skips them by design).
async fn post_admin_with_peer(
    app: &Router,
    action: &str,
    symbol: &str,
    body: Option<Value>,
    token: Option<&str>,
    peer: Option<SocketAddr>,
) -> (StatusCode, Value) {
    let uri = format!("/api/admin/instruments/{symbol}/{action}");
    let mut builder = Request::builder().method(Method::POST).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(peer) = peer {
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

async fn submit_order(app: &Router, client_order_id: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/orders")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({
                "symbol": "BTC-USD",
                "side": "buy",
                "order_type": "limit",
                "price_cents": 6_500_000,
                "qty": 1,
                "client_order_id": client_order_id,
                "participant": "alice",
            }))
            .expect("serialize request"),
        ))
        .expect("valid request");
    let response = app.clone().oneshot(request).await.expect("router responds");
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

/// The Audit-topic records for `symbol`, in offset order.
fn audit_records(node: &TestNode, symbol: Symbol) -> Vec<JournalRecord> {
    let partitions = Topic::Audit.partition_count(node.ctx.exchange());
    let partition = symbol.partition(partitions);
    node.memory().read(Topic::Audit, partition, 0, 10_000)
}

fn audit_records_from(
    journal: &MemoryJournal,
    exchange: &ExchangeConfig,
    symbol: Symbol,
) -> Vec<JournalRecord> {
    let partition = symbol.partition(Topic::Audit.partition_count(exchange));
    journal.read(Topic::Audit, partition, 0, 10_000)
}

/// With no operator tokens configured the node refuses every caller with 403
/// `operator_not_configured` — including holders of a *valid trading* token.
#[tokio::test]
async fn admin_without_operator_tokens_is_403_operator_not_configured() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.api_token = Some("trader-s3cret".into());
    }));

    for token in [None, Some("trader-s3cret"), Some(OPERATOR_TOKEN)] {
        let (status, body) = post_admin(
            &app,
            "halt",
            "BTC-USD",
            Some(json!({ "reason": "test" })),
            token,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{token:?}: {body}");
        assert_eq!(body["code"], "operator_not_configured");
    }

    assert!(
        node.orders(symbol()).is_empty(),
        "refused admin requests must not reach the order log"
    );
    assert!(
        audit_records(&node, symbol()).is_empty(),
        "refused admin requests must not reach the audit log"
    );
}

/// With operator tokens configured, a missing, wrong, or *valid trading*
/// token is 401; the valid operator token acts as its own identity.
#[tokio::test]
async fn admin_requires_an_operator_token_when_configured() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator_and_trader(&node));

    for token in [None, Some("wrong-op"), Some("trader-s3cret")] {
        let (status, body) = post_admin(&app, "halt", "BTC-USD", Some(json!({})), token).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}: {body}");
        assert_eq!(body["code"], "unauthorized");
    }
    assert!(node.orders(symbol()).is_empty());

    let (status, body) = post_admin(
        &app,
        "halt",
        "BTC-USD",
        Some(json!({})),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("admin ack");
    assert_eq!(ack.operator, OPERATOR_NAME);
    assert_eq!(
        node.commands(symbol())[0].participant,
        ParticipantId::derive(OPERATOR_NAME),
        "the command carries the operator identity, not a trading participant"
    );
}

#[tokio::test]
async fn unknown_symbols_are_rejected_before_any_append() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator(&node));

    let (status, body) = post_admin(
        &app,
        "halt",
        "DOGE-USD",
        Some(json!({})),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "unknown_symbol");
    assert!(node.orders(symbol()).is_empty());
    assert!(audit_records(&node, symbol()).is_empty());
}

/// The full operator loop: halt, watch a live order get rejected at the
/// engine, resume, watch a new order get accepted — with one audited record
/// per action.
#[tokio::test]
async fn halt_rejects_orders_and_resume_reopens_the_book() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator(&node));
    let operator_ingress = Some(OPERATOR_TOKEN);

    // (1) Halt.
    let (status, body) = post_admin(
        &app,
        "halt",
        "BTC-USD",
        Some(json!({ "reason": "risk event" })),
        operator_ingress,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let halt: AdminAck = serde_json::from_value(body).expect("halt ack");
    assert_eq!(halt.symbol, symbol());
    assert_eq!(halt.action, "halt");
    assert!(halt.halted);
    assert_eq!(halt.partition, symbol().partition(16), "BTC-USD partition");
    assert_eq!(halt.operator, OPERATOR_NAME);

    // The Orders topic holds one Halt command at offset 0, ordered like any
    // client command.
    let records = node.orders(symbol());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].header("type"), Some("halt"));
    let halt_command = Command::decode(&records[0].payload).expect("halt payload");
    assert_eq!(halt_command.kind, CommandKind::Halt);
    assert_eq!(halt_command.symbol, symbol());
    assert_eq!(
        halt_command.participant,
        ParticipantId::derive(OPERATOR_NAME)
    );
    assert!(halt_command.timestamp_ns > 0);
    assert_eq!(halt.offset, 0, "the ack offset is the command's log offset");

    // (2) The Audit topic holds one JSON record linking back to the command.
    let audits = audit_records(&node, symbol());
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].offset, halt.audit_offset);
    assert_eq!(audits[0].header("type"), Some("audit"));
    let audit: Value = serde_json::from_slice(&audits[0].payload).expect("audit json");
    assert_eq!(audit["operator"], OPERATOR_NAME);
    assert_eq!(audit["action"], "halt");
    assert_eq!(audit["symbol"], "BTC-USD");
    assert_eq!(audit["halted"], true);
    assert_eq!(audit["order_offset"], 0);
    assert_eq!(audit["service"], "whippet");
    assert_eq!(audit["reason"], "risk event");
    assert!(audit["timestamp_ns"].as_u64().is_some());

    // (3) A normal order is appended behind the halt and rejected by the
    // engine, which proves the halt was applied in log order.
    let (status, body) = submit_order(&app, "halted-1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "accepted_by_log");
    assert_eq!(body["offset"], 1);

    let processed = node.process_orders(symbol());
    assert!(
        processed
            .executions
            .iter()
            .any(|exec| exec.kind == ExecKind::Rejected && exec.reason == RejectReason::Halted),
        "the engine must reject an order submitted while halted"
    );
    assert_eq!(processed.engine.book().order_count(), 0);

    // (4) Resume.
    let (status, body) =
        post_admin(&app, "resume", "BTC-USD", Some(json!({})), operator_ingress).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let resume: AdminAck = serde_json::from_value(body).expect("resume ack");
    assert_eq!(resume.action, "resume");
    assert!(!resume.halted);
    assert_eq!(resume.offset, 2);
    assert_eq!(resume.audit_offset, 1);

    // (5) A new order is accepted after the resume.
    let (status, body) = submit_order(&app, "resumed-1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 3);

    let processed = node.process_orders(symbol());
    // The offset-1 order is still rejected (it sits behind the halt); the
    // post-resume order is accepted.
    assert_eq!(
        processed
            .executions
            .iter()
            .filter(|exec| exec.kind == ExecKind::Rejected && exec.reason == RejectReason::Halted)
            .count(),
        1,
        "exactly the pre-resume order is rejected as halted"
    );
    assert_eq!(
        processed
            .executions
            .iter()
            .filter(|exec| exec.kind == ExecKind::Accepted)
            .count(),
        1,
    );
    assert_eq!(processed.engine.book().order_count(), 1);
    assert!(
        !processed.engine.is_halted(),
        "the resume command reopens the book"
    );

    // (6) Both actions are audited, in order.
    let audits = audit_records(&node, symbol());
    assert_eq!(audits.len(), 2);
    let resume_audit: Value = serde_json::from_slice(&audits[1].payload).expect("resume audit");
    assert_eq!(resume_audit["action"], "resume");
    assert_eq!(resume_audit["halted"], false);
    assert_eq!(resume_audit["order_offset"], 2);
    assert!(
        resume_audit["reason"].is_null(),
        "an omitted reason is recorded as null"
    );
}

/// Admin endpoints sit inside the ingress router, so the shared per-IP rate
/// limit applies with no special case.
#[tokio::test]
async fn admin_endpoints_are_rate_limited_with_ingress() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.operator_tokens = operator_tokens();
        config.rate_limit_per_sec = 1;
    }));
    let peer: SocketAddr = "203.0.113.9:4000".parse().expect("peer address");

    let (first, first_body) = post_admin_with_peer(
        &app,
        "halt",
        "BTC-USD",
        Some(json!({})),
        Some(OPERATOR_TOKEN),
        Some(peer),
    )
    .await;
    assert_eq!(first, StatusCode::OK, "{first_body}");
    let (second, second_body) = post_admin_with_peer(
        &app,
        "resume",
        "BTC-USD",
        Some(json!({})),
        Some(OPERATOR_TOKEN),
        Some(peer),
    )
    .await;
    assert_eq!(second, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(second_body["code"], "rate_limited");
}

/// A turbo node with one live lane for BTC-USD, drained by a blocking matcher
/// thread that applies each command to a real `ShardEngine` and acks with its
/// local seq. Audit records go through the durable group-commit writer, so
/// they are readable from the journal even in turbo mode.
///
/// Limitation: the lane's write-behind journaling (the shard writer's job in
/// `run_engine_shard`) is not part of this harness, so tests using it assert
/// the lane/ack/audit contract rather than the durable Orders record the
/// write-behind would append.
fn turbo_node() -> (tempfile::TempDir, Arc<NodeContext>, Router) {
    let tempdir = tempfile::tempdir().expect("temp dir");
    let exchange = ExchangeConfig::default();
    let snapshot_dir = tempdir.path().join("snapshots");
    let config = NodeConfig {
        http_addr: "127.0.0.1:0".parse().expect("valid test address"),
        tcp_addr: None,
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        journal: JournalKind::Memory,
        mode: Mode::Turbo,
        exchange: exchange.clone(),
        snapshot_dir: snapshot_dir.clone(),
        snapshot_every: 10_000,
        dashboard_dir: tempdir.path().join("dashboard"),
        build: "test".into(),
        api_token: None,
        api_participant: "api".into(),
        api_tokens: Vec::new(),
        operator_tokens: operator_tokens(),
        metrics_token: None,
        rate_limit_per_sec: 0,
        ws_per_ip: 0,
        risk: whippet::config::RiskConfig::default(),
        turbo: TurboConfig::default(),
        storage: whippet::storage::StorageConfig {
            whippet_dir: tempdir.path().join("data"),
            iggy_dir: None,
            ..whippet::storage::StorageConfig::default()
        },
    };
    let journal = Arc::new(Journal::memory());
    let group_commit = whippet_journal::GroupCommit::new(
        Arc::clone(&journal),
        whippet_journal::GroupCommitConfig {
            max_batch: 256,
            linger: Duration::ZERO,
        },
    );
    let hub = MdHub::new(
        exchange.clone(),
        config.iggy_addr.clone(),
        "memory".into(),
        Some("test".into()),
    );
    let metrics = hub.metrics();
    let (shutdown, _) = tokio::sync::watch::channel(false);

    // One turbo lane for BTC-USD's partition, drained by a blocking shard
    // task that applies each command to a real ShardEngine and acks with its
    // local seq.
    let partition = symbol().partition(exchange.partitions);
    let (turbo, mut receivers) = TurboIngress::new(TurboConfig::default(), [partition]);
    let mut receiver = receivers.remove(&partition).expect("the lane exists");
    let shard_exchange = exchange.clone();
    std::thread::Builder::new()
        .name("admin-turbo-matcher".into())
        .spawn(move || {
            let mut engine =
                ShardEngine::new_with_snapshot_interval(partition, &shard_exchange, 10_000);
            let mut seq = 0u64;
            while let Some(entry) = receiver.blocking_recv() {
                let mut out = EngineOutput::default();
                let _ = engine.process(&entry.command, seq, &mut out);
                let _ = entry.ack.send(turbo_ack(seq, &entry.command, &out));
                seq += 1;
            }
        })
        .expect("spawn the turbo matcher");

    let storage = whippet::storage::StorageMonitor::new(config.storage.clone());
    let ctx = Arc::new(NodeContext {
        config,
        journal: Arc::clone(&journal),
        engine_journals: vec![Arc::clone(&journal)],
        engine_journal: Arc::clone(&journal),
        group_commit,
        hub,
        metrics,
        storage,
        snapshots: whippet::snapshot::SnapshotStore::new(snapshot_dir),
        started: std::time::Instant::now(),
        shutdown,
        shards_ok: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        // This test runs a turbo matcher directly, not the shard lifecycle.
        health: whippet::health::ShardHealth::new(),
        active_shards: Vec::new(),
        rate_limiter: Arc::new(whippet::ratelimit::RateLimiter::new(0)),
        ws_connections: Arc::new(tokio::sync::Semaphore::new(8)),
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo: Some(turbo),
    });

    let app = gateway::router(Arc::clone(&ctx));
    (tempdir, ctx, app)
}

/// The turbo halt path: the halt goes through the shard lane, the matcher
/// applies it, and the ack's offset is the matcher's local seq. The audit
/// record still goes through the durable group-commit writer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_halt_matches_through_the_lane_and_audits() {
    let (_tempdir, ctx, app) = turbo_node();
    let partition = symbol().partition(ctx.exchange().partitions);

    let (status, body) = post_admin(
        &app,
        "halt",
        "BTC-USD",
        Some(json!({ "reason": "turbo drill" })),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("halt ack");
    assert!(ack.halted);
    assert_eq!(ack.action, "halt");
    assert_eq!(ack.operator, OPERATOR_NAME);
    assert_eq!(ack.partition, partition);
    assert_eq!(ack.offset, 0, "the turbo offset is the matcher's local seq");

    // The audit record is durable through the group-commit writer.
    let memory = match &*ctx.journal {
        Journal::Memory(journal) => journal,
        Journal::Iggy(_) => panic!("the turbo test uses the memory journal"),
    };
    let audits = audit_records_from(memory, ctx.exchange(), symbol());
    assert_eq!(audits.len(), 1);
    let audit: Value = serde_json::from_slice(&audits[0].payload).expect("audit json");
    assert_eq!(audit["operator"], OPERATOR_NAME);
    assert_eq!(audit["action"], "halt");
    assert_eq!(audit["symbol"], "BTC-USD");
    assert_eq!(audit["halted"], true);
    assert_eq!(audit["order_offset"], 0);
    assert_eq!(audit["reason"], "turbo drill");
    assert_eq!(audit["service"], "whippet");
    assert_eq!(audits[0].offset, ack.audit_offset);
}

/// The turbo risk path: the `SetRisk` command goes through the same shard lane
/// as a halt, so the ack offset is the matcher's local seq and the audit record
/// is still durable. The durable test above proves the engine enforces the
/// limit; this one pins the turbo lane/ack/audit contract for risk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turbo_risk_matches_through_the_lane_and_audits() {
    let (_tempdir, ctx, app) = turbo_node();
    let partition = symbol().partition(ctx.exchange().partitions);

    let (status, body) = post_admin(
        &app,
        "risk",
        "BTC-USD",
        Some(json!({
            "participant": "alice",
            "max_net_qty": 1,
            "max_open_buy_notional_cents": 0,
            "reason": "turbo risk drill"
        })),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("risk ack");
    assert_eq!(ack.action, "set_risk");
    assert_eq!(ack.operator, OPERATOR_NAME);
    assert_eq!(ack.partition, partition);
    assert_eq!(ack.participant.as_deref(), Some("alice"));
    assert_eq!(ack.max_net_qty, Some(1));
    assert_eq!(ack.max_open_buy_notional_cents, Some(0));
    assert_eq!(ack.offset, 0, "the turbo offset is the matcher's local seq");

    let memory = match &*ctx.journal {
        Journal::Memory(journal) => journal,
        Journal::Iggy(_) => panic!("the turbo test uses the memory journal"),
    };
    let audits = audit_records_from(memory, ctx.exchange(), symbol());
    assert_eq!(audits.len(), 1);
    let audit: Value = serde_json::from_slice(&audits[0].payload).expect("audit json");
    assert_eq!(audit["operator"], OPERATOR_NAME);
    assert_eq!(audit["action"], "set_risk");
    assert_eq!(audit["symbol"], "BTC-USD");
    assert_eq!(audit["participant"], "alice");
    assert_eq!(audit["max_net_qty"], 1);
    assert_eq!(audit["max_open_buy_notional_cents"], 0);
    assert_eq!(audit["order_offset"], 0);
    assert_eq!(audit["reason"], "turbo risk drill");
    assert_eq!(audit["service"], "whippet");
    assert_eq!(audits[0].offset, ack.audit_offset);

    // Two orders behind the command: the first fits the one-lot bound, the
    // second would leave a two-lot worst case. The turbo ack carries the
    // engine's refusal, which proves the lane applied the limit in order.
    let (status, first) = submit_order(&app, "turbo-risk-1").await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["status"], "matched");
    assert_eq!(first["reject"], Value::Null);
    assert_eq!(first["offset"], 1);

    let (status, second) = submit_order(&app, "turbo-risk-2").await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["status"], "rejected");
    assert_eq!(second["reject"], "risk_limit");
    assert_eq!(second["offset"], 2);
}

/// The session endpoint inherits the operator namespace: with no operator
/// tokens configured every caller is 403, including a valid trading token,
/// and nothing reaches either log.
#[tokio::test]
async fn session_without_operator_tokens_is_403_operator_not_configured() {
    let node = TestNode::new();
    let app = gateway::router(node_with(&node, |config| {
        config.api_token = Some("trader-s3cret".into());
    }));

    for token in [None, Some("trader-s3cret"), Some(OPERATOR_TOKEN)] {
        let (status, body) = post_admin(
            &app,
            "session",
            "BTC-USD",
            Some(json!({ "state": "pre_open" })),
            token,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{token:?}: {body}");
        assert_eq!(body["code"], "operator_not_configured");
    }

    assert!(
        node.orders(symbol()).is_empty(),
        "refused session requests must not reach the order log"
    );
    assert!(
        audit_records(&node, symbol()).is_empty(),
        "refused session requests must not reach the audit log"
    );
}

/// The full session loop: pre-open gates new orders, open admits them, closed
/// gates them again — with one audited `set_session` record per change and
/// the engine enforcing each state in log order.
#[tokio::test]
async fn set_session_gates_orders_through_the_log() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator(&node));
    let operator_ingress = Some(OPERATOR_TOKEN);

    // (1) Pre-open.
    let (status, body) = post_admin(
        &app,
        "session",
        "BTC-USD",
        Some(json!({ "state": "pre_open", "reason": "opening auction" })),
        operator_ingress,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("session ack");
    assert_eq!(ack.symbol, symbol());
    assert_eq!(ack.action, "set_session");
    assert_eq!(ack.session, Some(SessionState::PreOpen));
    assert!(!ack.halted, "a session change does not state a halt state");
    assert_eq!(ack.partition, symbol().partition(16), "BTC-USD partition");
    assert_eq!(ack.operator, OPERATOR_NAME);

    // The Orders topic holds one SetSession command at offset 0, ordered like
    // any client command, carrying the session code in its flags.
    let records = node.orders(symbol());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].header("type"), Some("set_session"));
    let session_command = Command::decode(&records[0].payload).expect("session payload");
    assert_eq!(session_command.kind, CommandKind::SetSession);
    assert_eq!(session_command.symbol, symbol());
    assert_eq!(
        session_command.participant,
        ParticipantId::derive(OPERATOR_NAME)
    );
    assert_eq!(
        session_command.session_state().expect("session code"),
        SessionState::PreOpen
    );
    assert_eq!(ack.offset, 0, "the ack offset is the command's log offset");

    // The audit record links back to the command and names the session.
    let audits = audit_records(&node, symbol());
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].offset, ack.audit_offset);
    let audit: Value = serde_json::from_slice(&audits[0].payload).expect("audit json");
    assert_eq!(audit["operator"], OPERATOR_NAME);
    assert_eq!(audit["action"], "set_session");
    assert_eq!(audit["symbol"], "BTC-USD");
    assert_eq!(audit["session"], "pre_open");
    assert_eq!(audit["order_offset"], 0);
    assert_eq!(audit["service"], "whippet");
    assert_eq!(audit["reason"], "opening auction");
    assert!(
        audit.get("halted").is_none(),
        "a session record does not claim a halt state"
    );

    // (2) An order appended behind the pre-open command is rejected by the
    // engine, which proves the session was applied in log order.
    let (status, body) = submit_order(&app, "pre-open-1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 1);

    let processed = node.process_orders(symbol());
    assert!(
        processed
            .executions
            .iter()
            .any(|exec| exec.kind == ExecKind::Rejected
                && exec.reason == RejectReason::SessionNotOpen),
        "the engine must reject an order submitted before the open"
    );
    assert_eq!(processed.engine.book().order_count(), 0);
    assert_eq!(processed.engine.session(), SessionState::PreOpen);

    // (3) Open. The new order is accepted.
    let (status, body) = post_admin(
        &app,
        "session",
        "BTC-USD",
        Some(json!({ "state": "open" })),
        operator_ingress,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("session ack");
    assert_eq!(ack.session, Some(SessionState::Open));
    assert_eq!(ack.offset, 2);

    let (status, body) = submit_order(&app, "open-1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 3);

    let processed = node.process_orders(symbol());
    // The pre-open order is still rejected (it sits behind the command); the
    // post-open order is accepted.
    assert_eq!(
        processed
            .executions
            .iter()
            .filter(|exec| exec.kind == ExecKind::Rejected
                && exec.reason == RejectReason::SessionNotOpen)
            .count(),
        1,
        "exactly the pre-open order is rejected as session_not_open"
    );
    assert_eq!(
        processed
            .executions
            .iter()
            .filter(|exec| exec.kind == ExecKind::Accepted)
            .count(),
        1,
    );
    assert_eq!(processed.engine.book().order_count(), 1);
    assert_eq!(processed.engine.session(), SessionState::Open);

    // (4) Closed. New orders are rejected again.
    let (status, body) = post_admin(
        &app,
        "session",
        "BTC-USD",
        Some(json!({ "state": "closed" })),
        operator_ingress,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("session ack");
    assert_eq!(ack.session, Some(SessionState::Closed));
    assert_eq!(ack.offset, 4);

    let (status, body) = submit_order(&app, "closed-1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 5);

    let processed = node.process_orders(symbol());
    assert!(
        processed
            .executions
            .iter()
            .any(|exec| exec.kind == ExecKind::Rejected
                && exec.reason == RejectReason::SessionNotOpen),
        "the engine must reject an order submitted after the close"
    );
    assert_eq!(processed.engine.book().order_count(), 1);
    assert_eq!(processed.engine.session(), SessionState::Closed);

    // (5) Every change is audited, in order, with its own session.
    let audits = audit_records(&node, symbol());
    assert_eq!(audits.len(), 3);
    for (record, (session, offset)) in
        audits
            .iter()
            .zip([("pre_open", 0u64), ("open", 2), ("closed", 4)])
    {
        let audit: Value = serde_json::from_slice(&record.payload).expect("audit json");
        assert_eq!(audit["action"], "set_session");
        assert_eq!(audit["session"], session);
        assert_eq!(audit["order_offset"], offset);
    }
}

/// Risk limits are appended as one `SetRisk` command and enforced by the
/// engine from that log position onward, not by the gateway's static ingress
/// guard.
#[tokio::test]
async fn set_risk_limits_orders_through_the_log() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator(&node));
    let operator_ingress = Some(OPERATOR_TOKEN);

    let (status, body) = post_admin(
        &app,
        "risk",
        "BTC-USD",
        Some(json!({
            "participant": "alice",
            "max_net_qty": 1,
            "max_open_buy_notional_cents": 0,
            "reason": "desk limit"
        })),
        operator_ingress,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ack: AdminAck = serde_json::from_value(body).expect("risk ack");
    assert_eq!(ack.symbol, symbol());
    assert_eq!(ack.action, "set_risk");
    assert_eq!(ack.participant.as_deref(), Some("alice"));
    assert_eq!(ack.max_net_qty, Some(1));
    assert_eq!(ack.max_open_buy_notional_cents, Some(0));
    assert_eq!(ack.session, None, "a risk change does not claim a session");
    assert!(!ack.halted, "a risk change does not claim a halt state");
    assert_eq!(ack.partition, symbol().partition(16), "BTC-USD partition");
    assert_eq!(ack.operator, OPERATOR_NAME);
    assert_eq!(ack.offset, 0, "the ack offset is the command's log offset");

    // The fixed command layout carries the target participant, the net bound
    // in `qty`, and the notional bound in `price`.
    let records = node.orders(symbol());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].header("type"), Some("set_risk"));
    let command = Command::decode(&records[0].payload).expect("risk payload");
    assert_eq!(command.kind, CommandKind::SetRisk);
    assert_eq!(command.symbol, symbol());
    assert_eq!(command.participant, ParticipantId::derive("alice"));
    assert_eq!(command.qty.0, 1);
    assert_eq!(command.price.0, 0);

    // The audit names the participant and both bounds in operator units.
    let audits = audit_records(&node, symbol());
    assert_eq!(audits.len(), 1);
    assert_eq!(audits[0].offset, ack.audit_offset);
    let audit: Value = serde_json::from_slice(&audits[0].payload).expect("audit json");
    assert_eq!(audit["operator"], OPERATOR_NAME);
    assert_eq!(audit["action"], "set_risk");
    assert_eq!(audit["symbol"], "BTC-USD");
    assert_eq!(audit["participant"], "alice");
    assert_eq!(audit["max_net_qty"], 1);
    assert_eq!(audit["max_open_buy_notional_cents"], 0);
    assert_eq!(audit["order_offset"], 0);
    assert_eq!(audit["reason"], "desk limit");
    assert!(
        audit.get("halted").is_none() && audit.get("session").is_none(),
        "a risk record claims only what it changed"
    );

    // Two orders behind the command: the first fits the one-lot bound, the
    // second would leave a two-lot worst case and is refused by the engine.
    for (client_order_id, offset) in [("risk-1", 1), ("risk-2", 2)] {
        let (status, body) = submit_order(&app, client_order_id).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["offset"], offset);
    }
    let processed = node.process_orders(symbol());
    assert_eq!(
        processed.engine.book().order_count(),
        1,
        "only the first order may rest"
    );
    assert!(
        processed
            .executions
            .iter()
            .any(|exec| exec.kind == ExecKind::Rejected && exec.reason == RejectReason::RiskLimit),
        "the engine must reject the order that breaches the log-ordered limit"
    );
    assert_eq!(
        processed
            .engine
            .risk_table()
            .entry(ParticipantId::derive("alice"))
            .limits
            .max_net_qty,
        1
    );
}

/// A malformed risk request is refused before any append: both bounds are
/// required, the participant must fit the configured bound, and the notional
/// must fit the engine's signed field.
#[tokio::test]
async fn risk_rejects_bad_bodies_before_any_append() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator(&node));
    let operator_ingress = Some(OPERATOR_TOKEN);

    let cases = [
        (
            json!({ "participant": "alice", "max_net_qty": 1 }),
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_json",
        ),
        (
            json!({
                "participant": "",
                "max_net_qty": 1,
                "max_open_buy_notional_cents": 0
            }),
            StatusCode::BAD_REQUEST,
            "invalid_participant",
        ),
        (
            json!({
                "participant": "a".repeat(65),
                "max_net_qty": 1,
                "max_open_buy_notional_cents": 0
            }),
            StatusCode::BAD_REQUEST,
            "invalid_participant",
        ),
        (
            json!({
                "participant": "alice",
                "max_net_qty": 1,
                "max_open_buy_notional_cents": u64::MAX
            }),
            StatusCode::BAD_REQUEST,
            "invalid_notional",
        ),
    ];
    for (body, expected_status, expected_code) in cases {
        let (status, response) = post_admin(
            &app,
            "risk",
            "BTC-USD",
            Some(body.clone()),
            operator_ingress,
        )
        .await;
        assert_eq!(status, expected_status, "{body}: {response}");
        assert_eq!(response["code"], expected_code, "{body}");
    }

    let (status, response) = post_admin(
        &app,
        "risk",
        "BTC-USD",
        Some(json!({
            "participant": "alice",
            "max_net_qty": 1,
            "max_open_buy_notional_cents": 0
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{response}");

    assert!(
        node.orders(symbol()).is_empty(),
        "refused risk requests must not reach the order log"
    );
    assert!(
        audit_records(&node, symbol()).is_empty(),
        "refused risk requests must not reach the audit log"
    );
}

/// A body without one of the three documented states is a 422, never a silent
/// default; an unknown symbol is a 400 before anything is appended.
#[tokio::test]
async fn session_rejects_bad_bodies_before_any_append() {
    let node = TestNode::new();
    let app = gateway::router(node_with_operator(&node));

    let (status, body) = post_admin(
        &app,
        "session",
        "BTC-USD",
        Some(json!({ "state": "auction" })),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "invalid_json");

    let (status, body) = post_admin(
        &app,
        "session",
        "DOGE-USD",
        Some(json!({ "state": "open" })),
        Some(OPERATOR_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "unknown_symbol");

    assert!(node.orders(symbol()).is_empty());
    assert!(audit_records(&node, symbol()).is_empty());
}
