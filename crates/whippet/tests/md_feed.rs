// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Sequenced market-data feed (`GET /ws/md`) integration tests.
//!
//! Same discipline as `gateway.rs`: the real router serves a loopback listener
//! (a WebSocket upgrade cannot be driven through `oneshot`), while the market
//! data arrives by applying `MdEvent`s to the node's real `MdHub`, exactly the
//! way the engine's MD consumer would.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use axum::Router;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tower::ServiceExt;
use whippet::config::{JournalKind, NodeConfig};
use whippet::context::NodeContext;
use whippet::gateway;
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{MdEvent, MdKind, Side};
use whippet_marketdata::MdHub;

use common::TestNode;

fn btc() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

/// A resting-limit add, the simplest delta the engine publishes. `price` is in
/// ticks; BTC-USD's tick is 100 cents, so tick 65_000 is $65,000.00.
fn add_event(seq: u64, price: i64, qty: u64, order: u128) -> MdEvent {
    MdEvent {
        kind: MdKind::Add,
        symbol: btc(),
        seq: Seq(seq),
        cause_offset: seq,
        timestamp_ns: 0,
        side: Side::Buy,
        flags: 0,
        price: Price(price),
        qty: Qty(qty),
        order_id: OrderId(order),
        participant: ParticipantId(1),
    }
}

/// Serve the router on a real loopback listener and return its address.
async fn spawn_server(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a test listener");
    let addr = listener.local_addr().expect("local address");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

/// Raw WebSocket handshake for `/ws/md?{query}`. Returns the stream, the HTTP
/// status, and any response body (the JSON rejection on a 4xx).
async fn open_md_feed(addr: SocketAddr, query: &str) -> (TcpStream, u16, String) {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to the test server");
    let path = if query.is_empty() {
        "/ws/md".to_string()
    } else {
        format!("/ws/md?{query}")
    };
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
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
    // A rejection carries a JSON body with a declared length; read it fully so
    // the caller can assert on the error code.
    let mut body = String::new();
    let declared = declared_body_length(&response);
    if declared > 0 {
        let head_end = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("header terminator")
            + 4;
        body.push_str(&String::from_utf8_lossy(&response[head_end..]));
        while body.len() < declared {
            let read = stream
                .read(&mut chunk)
                .await
                .expect("read the rejection body");
            assert!(read > 0, "the server closed before the full body");
            body.push_str(&String::from_utf8_lossy(&chunk[..read]));
        }
    }
    let status = String::from_utf8_lossy(&response)
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status line");
    (stream, status, body)
}

fn declared_body_length(response: &[u8]) -> usize {
    let head = String::from_utf8_lossy(response);
    head.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())
                .flatten()
        })
        .unwrap_or(0)
}

/// Read one server-side WebSocket text frame. Server frames are unmasked, so
/// the payload is plain UTF-8 after the length header.
async fn read_text_frame(stream: &mut TcpStream) -> String {
    let mut header = [0u8; 2];
    stream
        .read_exact(&mut header)
        .await
        .expect("frame header arrives");
    let opcode = header[0] & 0x0f;
    let mut length = usize::from(header[1] & 0x7f);
    if length == 126 {
        let mut extended = [0u8; 2];
        stream
            .read_exact(&mut extended)
            .await
            .expect("16-bit length");
        length = u16::from_be_bytes(extended) as usize;
    } else if length == 127 {
        let mut extended = [0u8; 8];
        stream
            .read_exact(&mut extended)
            .await
            .expect("64-bit length");
        length = u64::from_be_bytes(extended) as usize;
    }
    let mut payload = vec![0u8; length];
    stream
        .read_exact(&mut payload)
        .await
        .expect("frame payload arrives");
    assert_eq!(opcode, 0x1, "the feed only sends text frames");
    String::from_utf8(payload).expect("frame payload is utf-8")
}

/// Plain-text GET for `/metrics`, whose body is not JSON.
async fn get_text(app: &Router, uri: &str) -> (u16, String) {
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(axum::http::Method::GET)
                .uri(uri)
                .body(axum::body::Body::empty())
                .expect("valid request"),
        )
        .await
        .expect("router responds");
    let status = response.status().as_u16();
    let bytes = http_body_util::BodyExt::collect(response.into_body())
        .await
        .expect("body collects")
        .to_bytes();
    (status, String::from_utf8(bytes.to_vec()).expect("utf-8"))
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

/// A node like [`common::TestNode`], but with an explicit sequenced-feed
/// buffer size so the lag path is reachable in a handful of events instead of
/// a thousand.
fn feed_node(feed_capacity: usize) -> (Arc<NodeContext>, TempDir) {
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
        mode: Mode::Durable,
        exchange,
        snapshot_dir: snapshot_dir.clone(),
        snapshot_every: 10_000,
        dashboard_dir: tempdir.path().join("dashboard"),
        build: "test".into(),
        api_token: None,
        api_participant: "api".into(),
        api_tokens: Vec::new(),
        operator_tokens: Vec::new(),
        metrics_token: None,
        rate_limit_per_sec: 0,
        ws_per_ip: 0,
        turbo: whippet::turbo::TurboConfig::default(),
        storage: whippet::storage::StorageConfig {
            whippet_dir: tempdir.path().join("data"),
            iggy_dir: None,
            ..whippet::storage::StorageConfig::default()
        },
    };
    let journal = Arc::new(whippet_journal::Journal::memory());
    let engine_journals = vec![Arc::clone(&journal), Arc::clone(&journal)];
    let group_commit = whippet_journal::group::GroupCommit::new(
        Arc::clone(&journal),
        whippet_journal::group::GroupCommitConfig {
            max_batch: 256,
            linger: Duration::ZERO,
        },
    );
    let hub = MdHub::with_feed_capacity(
        config.exchange.clone(),
        config.iggy_addr.clone(),
        "memory".into(),
        Some("test".into()),
        feed_capacity,
    );
    let metrics = hub.metrics();
    let (shutdown, _) = watch::channel(false);
    let rate_limiter = Arc::new(whippet::ratelimit::RateLimiter::new(0));
    let ws_connections = Arc::new(tokio::sync::Semaphore::new(512));
    let storage = whippet::storage::StorageMonitor::new(config.storage.clone());
    let ctx = Arc::new(NodeContext {
        config,
        journal: Arc::clone(&journal),
        engine_journals,
        engine_journal: Arc::clone(&journal),
        group_commit,
        hub,
        metrics,
        storage,
        snapshots: whippet::snapshot::SnapshotStore::new(snapshot_dir),
        started: Instant::now(),
        shutdown,
        shards_ok: Arc::new(AtomicBool::new(true)),
        rate_limiter,
        ws_connections,
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo: None,
    });
    (ctx, tempdir)
}

#[tokio::test]
async fn md_feed_sends_a_snapshot_then_every_delta() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let addr = spawn_server(app.clone()).await;

    let (mut stream, status) = {
        let (stream, status, _) = open_md_feed(addr, "symbol=BTC-USD").await;
        (stream, status)
    };
    assert_eq!(status, 101, "the upgrade is accepted");

    // First frame: the snapshot handshake at the hub's current sequence.
    let snapshot = read_text_frame(&mut stream).await;
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(snapshot["symbol"], "BTC-USD");
    assert_eq!(snapshot["seq"], 0, "a fresh hub has applied no events");

    // Deltas are applied the way the engine's MD consumer would: straight
    // into the hub, which sequences and broadcasts them.
    node.ctx.hub.apply(&add_event(5, 65_000, 10, 1));
    let delta = read_text_frame(&mut stream).await;
    let delta: Value = serde_json::from_str(&delta).expect("delta frame");
    assert_eq!(delta["type"], "delta");
    assert_eq!(delta["symbol"], "BTC-USD");
    assert_eq!(delta["seq"], 5, "the delta advances past the snapshot seq");
    assert_eq!(delta["kind"], "add");
    assert_eq!(delta["side"], "buy");
    assert_eq!(delta["price_cents"], 6_500_000, "tick 65_000 at 100 cents");
    assert_eq!(delta["qty"], 10);

    // The stream keeps flowing: the next event arrives as its own frame.
    node.ctx.hub.apply(&add_event(6, 65_100, 3, 2));
    let delta = read_text_frame(&mut stream).await;
    let delta: Value = serde_json::from_str(&delta).expect("delta frame");
    assert_eq!(delta["type"], "delta");
    assert_eq!(delta["seq"], 6);
    assert_eq!(delta["qty"], 3);

    // One subscriber is visible in the metrics, and the hub counted the
    // delivered frames.
    let (status, text) = get_text(&app, "/metrics").await;
    assert_eq!(status, 200);
    assert_eq!(metric_value(&text, "whippet_md_feed_subscribers "), 1.0);
    assert_eq!(metric_value(&text, "whippet_md_feed_sent_total "), 2.0);
    assert_eq!(metric_value(&text, "whippet_md_feed_lagged_total "), 0.0);
    assert_eq!(metric_value(&text, "whippet_md_feed_resyncs_total "), 0.0);
    assert!(text.contains("# TYPE whippet_md_feed_subscribers gauge"));
    assert!(text.contains("# TYPE whippet_md_feed_sent_total counter"));
    assert!(text.contains("# TYPE whippet_md_feed_lagged_total counter"));
    assert!(text.contains("# TYPE whippet_md_feed_resyncs_total counter"));

    drop(stream);
}

#[tokio::test]
async fn md_feed_rejects_unknown_and_missing_symbols() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let addr = spawn_server(app).await;

    for query in ["symbol=DOGE-USD", "symbol=XRP-USD", "symbol="] {
        let (stream, status, body) = open_md_feed(addr, query).await;
        assert_eq!(status, 400, "{query} must be refused, got body {body}");
        let error: Value = serde_json::from_str(&body).expect("json api error");
        assert_eq!(error["code"], "unknown_symbol", "{query}");
        drop(stream);
    }

    // No query parameter at all is the same rejection.
    let (stream, status, body) = open_md_feed(addr, "").await;
    assert_eq!(status, 400);
    let error: Value = serde_json::from_str(&body).expect("json api error");
    assert_eq!(error["code"], "unknown_symbol");
    drop(stream);
}

#[tokio::test]
async fn md_feed_shares_the_ws_connection_cap() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let addr = spawn_server(app).await;

    // Drain the shared subscriber semaphore; the feed must refuse the upgrade
    // with the same JSON shape the conflated /ws uses.
    let drained: Vec<_> = (0..512)
        .map(|_| {
            Arc::clone(&node.ctx.ws_connections)
                .try_acquire_owned()
                .expect("drain one permit")
        })
        .collect();
    let (stream, status, body) = open_md_feed(addr, "symbol=BTC-USD").await;
    assert_eq!(status, 503, "an exhausted cap must refuse the upgrade");
    let error: Value = serde_json::from_str(&body).expect("json api error");
    assert_eq!(error["code"], "ws_capacity");
    assert_eq!(node.ctx.hub.feed_receiver_count(), 0);
    drop(stream);
    drop(drained);
}

#[tokio::test]
async fn md_feed_resyncs_a_lagged_consumer() {
    // Four-frame feed buffer: twenty events through it lose sixteen.
    let (ctx, _tempdir) = feed_node(4);
    let app = gateway::router(Arc::clone(&ctx));
    let addr = spawn_server(app.clone()).await;

    let (mut stream, status) = {
        let (stream, status, _) = open_md_feed(addr, "symbol=BTC-USD").await;
        (stream, status)
    };
    assert_eq!(status, 101);
    eprintln!("DBG: handshake done");
    let snapshot = read_text_frame(&mut stream).await;
    eprintln!("DBG: first frame: {}", &snapshot[..snapshot.len().min(120)]);
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(snapshot["seq"], 0);

    // Apply twenty events synchronously: no runtime turn passes between them,
    // so the subscriber (capacity 4) is guaranteed to fall 16 frames behind.
    eprintln!("DBG: receiver_count={}", ctx.hub.feed_receiver_count());
    for seq in 1..=20u64 {
        ctx.hub
            .apply(&add_event(seq, 65_000 + seq as i64, 1, seq as u128));
    }
    eprintln!("DBG: flood applied, feed_stats={:?}", ctx.hub.feed_stats());

    // The slow-consumer policy is resync, never a stall: first the notice,
    // then a fresh full snapshot to rebuild from.
    let resync = read_text_frame(&mut stream).await;
    let resync: Value = serde_json::from_str(&resync).expect("resync frame");
    assert_eq!(resync["type"], "resync");
    assert_eq!(resync["symbol"], "BTC-USD");
    assert_eq!(resync["reason"], "lagged 16");
    assert_eq!(resync["seq"], 20, "the resync names the sequence to adopt");

    let snapshot = read_text_frame(&mut stream).await;
    let snapshot: Value = serde_json::from_str(&snapshot).expect("resync snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(
        snapshot["seq"], 20,
        "the resnapshot is the hub's current state"
    );
    assert_eq!(
        snapshot["bids"].as_array().map(Vec::len),
        Some(20),
        "one level per applied add"
    );

    // The feed continues from the resnapshot.
    ctx.hub.apply(&add_event(21, 65_100, 1, 99));
    let delta = read_text_frame(&mut stream).await;
    let delta: Value = serde_json::from_str(&delta).expect("delta frame");
    assert_eq!(delta["type"], "delta");
    assert_eq!(delta["seq"], 21);

    // The hub counted the drop, the recovery, and every frame handed to the
    // buffer (twenty in the flood plus one after the resync).
    let (status, text) = get_text(&app, "/metrics").await;
    assert_eq!(status, 200);
    assert_eq!(metric_value(&text, "whippet_md_feed_subscribers "), 1.0);
    assert_eq!(metric_value(&text, "whippet_md_feed_lagged_total "), 1.0);
    assert_eq!(metric_value(&text, "whippet_md_feed_resyncs_total "), 1.0);
    assert_eq!(metric_value(&text, "whippet_md_feed_sent_total "), 21.0);

    drop(stream);
}

#[tokio::test]
async fn md_feed_ends_when_the_client_closes() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let addr = spawn_server(app).await;

    let (mut stream, status) = {
        let (stream, status, _) = open_md_feed(addr, "symbol=BTC-USD").await;
        (stream, status)
    };
    assert_eq!(status, 101);
    let snapshot = read_text_frame(&mut stream).await;
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");

    // The permit lives with the connection. Dropping the client socket must
    // end the serving task and hand the permit back; the event below also
    // wakes the loop into a failed send in case close detection is lazy.
    drop(stream);
    node.ctx.hub.apply(&add_event(1, 65_000, 1, 1));

    let semaphore = Arc::clone(&node.ctx.ws_connections);
    let deadline = Instant::now() + Duration::from_secs(5);
    while semaphore.available_permits() < 512 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        semaphore.available_permits(),
        512,
        "a closed client must release its subscriber permit"
    );
}
