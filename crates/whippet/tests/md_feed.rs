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
use whippet_core::protocol::{MdEvent, MdKind, MdSnapshot, Side};
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

/// Seed the hub with the producer's empty startup image. Book-state deltas
/// are refused until a baseline image exists.
fn seed(hub: &whippet_marketdata::MdHub) {
    hub.apply_snapshot(&MdSnapshot {
        symbol: btc(),
        seq: Seq(0),
        cause_offset: 0,
        timestamp_ns: 0,
        last_trade_price: Price(0),
        last_trade_qty: Qty(0),
        bids: Vec::new(),
        asks: Vec::new(),
        orders: Vec::new(),
        flags: 0,
    });
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

/// A WebSocket client over the raw test socket.
///
/// The HTTP read that consumes the 101 handshake may already contain the first
/// WebSocket frame: the server can write the upgrade response and the snapshot
/// in one turn, and the kernel hands them to one `read`. A real WebSocket
/// client parses the response with a buffered reader and keeps the tail; this
/// helper must do the same, or the first frame is silently swallowed and the
/// test hangs waiting for bytes it already consumed.
struct WsClient {
    stream: TcpStream,
    /// Bytes read past the end of the HTTP response, not yet parsed.
    pending: Vec<u8>,
}

/// Every socket read in these tests is bounded, so a feed regression fails an
/// assertion instead of hanging the suite.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

impl WsClient {
    /// `read_exact` over the pending buffer first, then the socket.
    async fn read_exact(&mut self, buf: &mut [u8]) {
        let mut filled = 0;
        let from_pending = self.pending.len().min(buf.len());
        buf[..from_pending].copy_from_slice(&self.pending[..from_pending]);
        self.pending.drain(..from_pending);
        filled += from_pending;
        while filled < buf.len() {
            let read = tokio::time::timeout(READ_TIMEOUT, self.stream.read(&mut buf[filled..]))
                .await
                .expect("read timed out; the feed stopped sending")
                .expect("socket read");
            assert!(read > 0, "the server closed mid-frame");
            filled += read;
        }
    }

    /// Read one server-side WebSocket text frame. Server frames are unmasked,
    /// so the payload is plain UTF-8 after the length header.
    async fn read_frame(&mut self) -> String {
        let mut header = [0u8; 2];
        self.read_exact(&mut header).await;
        let opcode = header[0] & 0x0f;
        let mut length = usize::from(header[1] & 0x7f);
        if length == 126 {
            let mut extended = [0u8; 2];
            self.read_exact(&mut extended).await;
            length = u16::from_be_bytes(extended) as usize;
        } else if length == 127 {
            let mut extended = [0u8; 8];
            self.read_exact(&mut extended).await;
            length = u64::from_be_bytes(extended) as usize;
        }
        let mut payload = vec![0u8; length];
        self.read_exact(&mut payload).await;
        assert_eq!(opcode, 0x1, "the feed only sends text frames");
        String::from_utf8(payload).expect("frame payload is utf-8")
    }
}

/// Raw WebSocket handshake for `/ws/md?{query}`. Returns the client, the HTTP
/// status, and any response body (the JSON rejection on a 4xx).
async fn open_md_feed(addr: SocketAddr, query: &str) -> (WsClient, u16, String) {
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

    // Read the HTTP response head. Anything past the terminator is either the
    // WebSocket feed the server already started sending, or the tail of a JSON
    // rejection body; both are kept, never discarded.
    let mut head = Vec::new();
    let mut chunk = [0u8; 1_024];
    let head_end = loop {
        let read = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk))
            .await
            .expect("handshake read timed out; the server never answered")
            .expect("handshake read");
        assert!(read > 0, "the server closed before answering the handshake");
        head.extend_from_slice(&chunk[..read]);
        if let Some(end) = head.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let mut tail = head.split_off(head_end);
    let body = match declared_body_length(&head) {
        0 => String::new(),
        declared => {
            let mut body = String::from_utf8_lossy(&tail).into_owned();
            while body.len() < declared {
                let read = tokio::time::timeout(READ_TIMEOUT, stream.read(&mut chunk))
                    .await
                    .expect("rejection body read timed out")
                    .expect("rejection body read");
                assert!(read > 0, "the server closed before the full body");
                body.push_str(&String::from_utf8_lossy(&chunk[..read]));
            }
            // The declared body was fully consumed; nothing behind it.
            tail.clear();
            body
        }
    };
    let status = String::from_utf8_lossy(&head)
        .split_whitespace()
        .nth(1)
        .and_then(|status| status.parse().ok())
        .expect("a status line");
    (
        WsClient {
            stream,
            pending: tail,
        },
        status,
        body,
    )
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
        risk: whippet::config::RiskConfig::default(),
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
        health: whippet::health::ShardHealth::new(),
        active_shards: Vec::new(),
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

    let (mut client, status, _) = open_md_feed(addr, "symbol=BTC-USD").await;
    assert_eq!(status, 101, "the upgrade is accepted");

    // First frame: the snapshot handshake at the hub's current sequence.
    let snapshot = client.read_frame().await;
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(snapshot["symbol"], "BTC-USD");
    assert_eq!(snapshot["seq"], 0, "a fresh hub has applied no events");

    // Before the startup image, a book-state delta is refused: it advances no
    // sequence, is not framed, and is visible as a counter.
    node.ctx.hub.apply(&add_event(4, 65_000, 99, 4));
    let (_, text) = get_text(&app, "/metrics").await;
    assert_eq!(
        metric_value(&text, "whippet_md_events_without_baseline_total "),
        1.0
    );

    // Deltas are applied the way the engine's MD consumer would: straight
    // into the hub, which sequences and broadcasts them. The startup image
    // must exist first; the initial WS frame above is not that image.
    seed(&node.ctx.hub);
    node.ctx.hub.apply(&add_event(5, 65_000, 10, 1));
    let delta = client.read_frame().await;
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
    let delta = client.read_frame().await;
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

    drop(client);
}

#[tokio::test]
async fn md_feed_rejects_unknown_and_missing_symbols() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let addr = spawn_server(app).await;

    for query in ["symbol=DOGE-USD", "symbol=XRP-USD", "symbol="] {
        let (client, status, body) = open_md_feed(addr, query).await;
        assert_eq!(status, 400, "{query} must be refused, got body {body}");
        let error: Value = serde_json::from_str(&body).expect("json api error");
        assert_eq!(error["code"], "unknown_symbol", "{query}");
        drop(client);
    }

    // No query parameter at all is the same rejection.
    let (client, status, body) = open_md_feed(addr, "").await;
    assert_eq!(status, 400);
    let error: Value = serde_json::from_str(&body).expect("json api error");
    assert_eq!(error["code"], "unknown_symbol");
    drop(client);
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
    let (client, status, body) = open_md_feed(addr, "symbol=BTC-USD").await;
    assert_eq!(status, 503, "an exhausted cap must refuse the upgrade");
    let error: Value = serde_json::from_str(&body).expect("json api error");
    assert_eq!(error["code"], "ws_capacity");
    assert_eq!(node.ctx.hub.feed_receiver_count(), 0);
    drop(client);
    drop(drained);
}

#[tokio::test]
// Deliberately the default single-thread runtime: it is what makes the flood
// below deterministic. The twenty `apply` calls run without a runtime turn,
// so the serving task cannot drain frames mid-flood and the subscriber is
// guaranteed to lag by exactly `20 - capacity` frames.
async fn md_feed_resyncs_a_lagged_consumer() {
    // Four-frame feed buffer: twenty events through it lose sixteen.
    let (ctx, _tempdir) = feed_node(4);
    let app = gateway::router(Arc::clone(&ctx));
    let addr = spawn_server(app.clone()).await;

    let (mut client, status, _) = open_md_feed(addr, "symbol=BTC-USD").await;
    assert_eq!(status, 101);
    let snapshot = client.read_frame().await;
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(snapshot["seq"], 0);

    // Apply twenty events synchronously: no runtime turn passes between them,
    // so the subscriber (capacity 4) is guaranteed to fall 16 frames behind.
    seed(&ctx.hub);
    for seq in 1..=20u64 {
        ctx.hub
            .apply(&add_event(seq, 65_000 + seq as i64, 1, seq as u128));
    }

    // The slow-consumer policy is resync, never a stall: first the notice,
    // then a fresh full snapshot to rebuild from.
    let resync = client.read_frame().await;
    let resync: Value = serde_json::from_str(&resync).expect("resync frame");
    assert_eq!(resync["type"], "resync");
    assert_eq!(resync["symbol"], "BTC-USD");
    assert_eq!(resync["reason"], "lagged 16");
    assert_eq!(resync["seq"], 20, "the resync names the sequence to adopt");

    let snapshot = client.read_frame().await;
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
    let delta = client.read_frame().await;
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

    drop(client);
}

#[tokio::test]
async fn md_feed_ends_when_the_client_closes() {
    let node = TestNode::new();
    let app = gateway::router(Arc::clone(&node.ctx));
    let addr = spawn_server(app).await;

    let (mut client, status, _) = open_md_feed(addr, "symbol=BTC-USD").await;
    assert_eq!(status, 101);
    let snapshot = client.read_frame().await;
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot frame");
    assert_eq!(snapshot["type"], "snapshot");

    // The permit lives with the connection. Dropping the client socket must
    // end the serving task and hand the permit back; the event below also
    // wakes the loop into a failed send in case close detection is lazy.
    drop(client);
    seed(&node.ctx.hub);
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
