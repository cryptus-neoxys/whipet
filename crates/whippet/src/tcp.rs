// Licensed under the Apache License, Version 2.0. See LICENSE.
//! Binary TCP order ingress.
//!
//! A second listener next to the HTTP gateway for clients that want to submit
//! fixed-layout commands without JSON parsing or one HTTP request per order.
//! The wire format is [`whippet_core::tcp`]: a four-byte little-endian body
//! length followed by one request or response body. There is no second framing
//! here; this module only owns the transport loop and the ingress rules.
//!
//! One connection is persistent and pipelined. The client may write several
//! `Submit` frames before reading any ack; this loop reads one frame at a time,
//! completes the journal append, writes the ack, and only then reads the next
//! frame. Responses therefore leave in request order, and at most one request
//! per connection is in flight.
//!
//! Authentication mirrors the HTTP path: when `WHIPPET_API_TOKEN` is set the
//! first `Submit` must follow a `Hello` carrying the same token. `Ping` stays
//! available before authentication so a client can measure clock offset while
//! connecting.

use std::io::ErrorKind;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use whippet_core::ids::Symbol;
use whippet_core::protocol::{Command, CommandKind};
use whippet_core::tcp::{TCP_BATCH_MAX, TcpErrorCode, TcpRequest, TcpResponse, body_len};
use whippet_journal::{GroupCommit, Topic};

use crate::context::NodeContext;
use crate::gateway::{journal_message_for_command, token_matches};
use crate::ratelimit::RateLimiter;

/// Idle connections are dropped after this long without a complete frame.
/// A client that wants to keep a connection warm can send a `Ping`.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// A failed `accept` (for example EMFILE) must not spin the loop hot.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// Concurrent connections. Each one holds a task and two small buffers.
const MAX_TCP_CONNECTIONS: usize = 1_024;

/// A client that stops reading acks must not hold a connection forever.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Serve binary order ingress until the task is cancelled.
///
/// The node spawns this next to `axum::serve`; it never returns on its own, so
/// a bind failure is the only error a caller sees. Binding happens inside the
/// function so the caller can log the resolved address after it succeeds.
pub async fn serve(ctx: Arc<NodeContext>, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding TCP ingress {addr}"))?;
    let local = listener.local_addr().unwrap_or(addr);
    info!(
        address = %local,
        auth_required = ctx.config.auth_required(),
        "tcp ingress listening"
    );

    // Same per-IP token bucket as HTTP ingress; the binary path is not a way
    // around the rate limit. A semaphore bounds concurrent connections.
    let limiter = Arc::clone(&ctx.rate_limiter);
    let connections = Arc::new(Semaphore::new(MAX_TCP_CONNECTIONS));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                warn!(error = %error, "tcp ingress accept failed");
                tokio::time::sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let permit = match Arc::clone(&connections).try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(peer = %peer, "tcp ingress connection cap reached; refusing");
                drop(stream);
                continue;
            }
        };
        let connection_ctx = Arc::clone(&ctx);
        let connection_limiter = Arc::clone(&limiter);
        tokio::spawn(async move {
            // A connection error is per-client: the listener keeps serving.
            let _permit = permit;
            if let Err(error) =
                serve_connection(connection_ctx, stream, peer.ip(), connection_limiter).await
            {
                debug!(peer = %peer, error = %error, "tcp ingress connection ended");
            }
        });
    }
}

/// Per-connection state that outlives a single request.
struct ConnectionState {
    authorized: bool,
    peer: IpAddr,
    limiter: Arc<RateLimiter>,
}

impl ConnectionState {
    /// Test/default form: localhost and the limiter disabled.
    #[cfg(test)]
    fn new(ctx: &NodeContext) -> Self {
        Self::with_peer(
            ctx,
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::new(RateLimiter::new(0)),
        )
    }

    fn with_peer(ctx: &NodeContext, peer: IpAddr, limiter: Arc<RateLimiter>) -> Self {
        Self {
            // Anonymous ingress is only possible when no token is configured.
            authorized: !ctx.config.auth_required(),
            peer,
            limiter,
        }
    }
}

/// Read frames, answer them in order, and reuse both buffers for the life of
/// the connection.
async fn serve_connection(
    ctx: Arc<NodeContext>,
    mut stream: TcpStream,
    peer: IpAddr,
    limiter: Arc<RateLimiter>,
) -> std::io::Result<()> {
    // Small request/response frames: do not let Nagle/delayed-ACK add latency.
    let _ = stream.set_nodelay(true);

    let mut state = ConnectionState::with_peer(&ctx, peer, limiter);
    let mut read_buf: Vec<u8> = Vec::with_capacity(1024);
    let mut write_buf: Vec<u8> = Vec::with_capacity(64);
    let mut prefix = [0u8; 4];

    loop {
        match timeout(IDLE_TIMEOUT, stream.read_exact(&mut prefix)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) if error.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(error)) => return Err(error),
            // Idle timeout: close quietly, the client can reconnect.
            Err(_) => return Ok(()),
        }

        let len = match body_len(prefix) {
            Ok(len) => len,
            Err(error) => {
                let response = error_response(TcpErrorCode::BadFrame, error.to_string());
                write_response(&mut stream, &mut write_buf, &response).await?;
                return Ok(());
            }
        };
        read_buf.resize(len, 0);
        match timeout(IDLE_TIMEOUT, stream.read_exact(&mut read_buf)).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) if error.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(error)) => return Err(error),
            Err(_) => return Ok(()),
        }

        let request = match TcpRequest::decode_body(&read_buf) {
            Ok(request) => request,
            Err(error) => {
                // The stream is desynchronised: answer once, then close.
                let response = error_response(TcpErrorCode::BadFrame, error.to_string());
                write_response(&mut stream, &mut write_buf, &response).await?;
                return Ok(());
            }
        };

        let (response, close) = handle_request(&ctx, &ctx.group_commit, &mut state, request).await;
        write_response(&mut stream, &mut write_buf, &response).await?;
        if close {
            return Ok(());
        }
    }
}

/// Handle one decoded frame. `true` in the second slot means the connection
/// must close after the response has been written.
async fn handle_request(
    ctx: &NodeContext,
    group: &GroupCommit,
    state: &mut ConnectionState,
    request: TcpRequest,
) -> (TcpResponse, bool) {
    match request {
        TcpRequest::Ping { client_ns } => (
            TcpResponse::Pong {
                client_ns,
                server_ns: now_ns(),
            },
            false,
        ),
        TcpRequest::Hello { token } => handle_hello(ctx, state, &token),
        TcpRequest::Submit { commands } => handle_submit(ctx, group, state, commands).await,
    }
}

fn handle_hello(
    ctx: &NodeContext,
    state: &mut ConnectionState,
    token: &str,
) -> (TcpResponse, bool) {
    // Rate limit at the handshake, before the token comparison: otherwise a
    // token-guessing client gets unlimited attempts, and `Hello` would be the
    // one ingress frame that skips the shared budget.
    if !state.limiter.check(state.peer) {
        return (
            error_response(
                TcpErrorCode::RateLimited,
                "ingress rate limit exceeded; retry shortly",
            ),
            false,
        );
    }
    // A hello after authorization is a no-op ack: the connection is already
    // trusted, so a second handshake cannot downgrade it.
    if state.authorized {
        return (hello_ack(true), false);
    }
    match ctx.config.api_token.as_deref() {
        // Defensive: an anonymous node starts authorized, so this arm is only
        // reachable if that invariant ever changes.
        None => {
            state.authorized = true;
            (hello_ack(true), false)
        }
        Some(expected) if token_matches(expected, token) => {
            state.authorized = true;
            (hello_ack(true), false)
        }
        Some(_) => (hello_ack(false), true),
    }
}

fn hello_ack(ok: bool) -> TcpResponse {
    TcpResponse::HelloAck {
        ok,
        message: if ok { "ok" } else { "unauthorized" }.to_string(),
    }
}

async fn handle_submit(
    ctx: &NodeContext,
    group: &GroupCommit,
    state: &ConnectionState,
    commands: Vec<Command>,
) -> (TcpResponse, bool) {
    // Charge before auth and shard-health: otherwise an unauthenticated client
    // can send Submit frames at wire speed for free.
    if !state
        .limiter
        .charge(state.peer, commands.len().max(1) as u32)
    {
        return (
            error_response(
                TcpErrorCode::RateLimited,
                "ingress rate limit exceeded; retry shortly",
            ),
            false,
        );
    }

    if !state.authorized {
        return (
            error_response(
                TcpErrorCode::Unauthorized,
                "hello with a valid token is required before submit",
            ),
            true,
        );
    }

    if !ctx.shards_ok() {
        return (
            error_response(
                TcpErrorCode::JournalUnavailable,
                "the matching shard is not running; orders are not being accepted",
            ),
            false,
        );
    }

    let symbol = match validate_batch(ctx, &commands) {
        Ok(symbol) => symbol,
        Err(response) => return (response, false),
    };
    let partition = symbol.partition(ctx.exchange().partitions);
    let started = Instant::now();

    // One journal message per command, one append for the whole batch: the
    // engine sees exactly the bytes the HTTP path writes.
    let mut messages = Vec::with_capacity(commands.len());
    for command in &commands {
        messages.push(journal_message_for_command(command));
    }
    let count = messages.len() as u32;

    match group.append(Topic::Orders, partition, messages).await {
        Ok(append) => {
            let elapsed = started.elapsed();
            for _ in 0..count {
                ctx.metrics.record_ingress();
            }
            // Latency is sampled once per batch: every command shares the one
            // durable append. Group commit may serve other connections in the
            // same append, but this caller's offsets still start at
            // `append.base_offset` and run contiguously for `count` messages.
            ctx.metrics
                .record_ingress_latency_us(elapsed.as_micros() as u64);
            (
                TcpResponse::SubmitAck {
                    partition: append.partition,
                    base_offset: append.base_offset,
                    count,
                    server_ns: now_ns(),
                },
                false,
            )
        }
        Err(error) => {
            // Keep the connection open: nothing was journaled, so the client
            // can retry the same batch.
            warn!(
                partition,
                count,
                error = %error,
                "tcp ingress journal append failed"
            );
            (
                error_response(TcpErrorCode::JournalUnavailable, error.to_string()),
                false,
            )
        }
    }
}

/// Validate a whole batch before anything is journaled. A rejected batch
/// leaves the connection usable.
fn validate_batch(ctx: &NodeContext, commands: &[Command]) -> Result<Symbol, TcpResponse> {
    if commands.is_empty() || commands.len() > TCP_BATCH_MAX {
        return Err(error_response(
            TcpErrorCode::BatchTooLarge,
            format!(
                "submit batch must be 1..={TCP_BATCH_MAX} commands, got {}",
                commands.len()
            ),
        ));
    }

    let symbol = commands[0].symbol;
    if ctx.exchange().instrument(symbol).is_none() {
        return Err(error_response(
            TcpErrorCode::InvalidCommand,
            format!("unknown symbol {symbol}"),
        ));
    }
    for (index, command) in commands.iter().enumerate() {
        if command.symbol != symbol {
            return Err(error_response(
                TcpErrorCode::MixedSymbols,
                format!(
                    "command {index} uses {} but the batch started with {symbol}; split the batch by symbol",
                    command.symbol
                ),
            ));
        }
        match command.kind {
            // A cancel reuses the fixed layout with a zero qty, so only new
            // orders are held to the positive-quantity rule.
            CommandKind::NewOrder if command.qty.0 == 0 => {
                return Err(error_response(
                    TcpErrorCode::InvalidCommand,
                    format!("command {index}: qty must be greater than zero"),
                ));
            }
            CommandKind::NewOrder | CommandKind::Cancel => {}
        }
    }
    Ok(symbol)
}

fn error_response(code: TcpErrorCode, message: impl Into<String>) -> TcpResponse {
    TcpResponse::Error {
        code: code.as_u16(),
        message: message.into(),
    }
}

async fn write_response(
    stream: &mut TcpStream,
    buffer: &mut Vec<u8>,
    response: &TcpResponse,
) -> std::io::Result<()> {
    // `TcpResponse::encode` returns a fresh frame, so the reusable buffer saves
    // the per-write allocation of the copy rather than of the frame itself. An
    // `encode_into(&mut Vec<u8>)` in `whippet-core` would remove the copy too.
    buffer.clear();
    buffer.extend_from_slice(&response.encode());
    match timeout(WRITE_TIMEOUT, stream.write_all(buffer)).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            ErrorKind::TimedOut,
            "client did not read the ack within the write timeout",
        )),
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos() as u64)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener as StdTcpListener;
    use std::sync::atomic::AtomicBool;

    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode, header};
    use tokio::sync::watch;
    use tower::ServiceExt;
    use whippet_core::config::{ExchangeConfig, Mode};
    use whippet_core::ids::{OrderId, ParticipantId, Price, Qty};
    use whippet_core::protocol::{OrderType, Side};
    use whippet_journal::MemoryJournal;
    use whippet_marketdata::MdHub;

    use super::*;
    use crate::config::{JournalKind, NodeConfig};
    use crate::snapshot::SnapshotStore;
    use whippet_journal::Journal;

    /// A node wired entirely in memory, mirroring `tests/common/mod.rs`. That
    /// harness lives in `tests/` and is not reachable from a unit test, so the
    /// same wiring is rebuilt here without touching it.
    struct TestNode {
        ctx: Arc<NodeContext>,
        _dir: tempfile::TempDir,
    }

    fn test_node(api_token: Option<&str>) -> TestNode {
        test_node_with_rate_limit(api_token, 0)
    }

    /// A node whose shared HTTP/TCP limiter allows `per_sec` frames per second
    /// per source address; `0` disables it.
    fn test_node_with_rate_limit(api_token: Option<&str>, per_sec: u32) -> TestNode {
        let dir = tempfile::tempdir().expect("temp dir");
        let exchange = ExchangeConfig::default();
        let config = NodeConfig {
            http_addr: "127.0.0.1:0".parse().expect("valid test address"),
            tcp_addr: None,
            iggy_addr: "127.0.0.1:8090".into(),
            iggy_user: "iggy".into(),
            iggy_password: "iggy".into(),
            journal: JournalKind::Memory,
            mode: Mode::Durable,
            exchange: exchange.clone(),
            snapshot_dir: dir.path().join("snapshots"),
            snapshot_every: 10_000,
            dashboard_dir: dir.path().join("dashboard"),
            build: "test".into(),
            api_token: api_token.map(str::to_string),
            api_participant: "api".into(),
            rate_limit_per_sec: per_sec,
            ws_per_ip: 0,
        };
        let journal = Arc::new(Journal::memory());
        let hub = MdHub::new(exchange, config.iggy_addr.clone(), "memory".into(), None);
        let metrics = hub.metrics();
        let (shutdown, _) = watch::channel(false);
        let group_commit = whippet_journal::GroupCommit::new(
            Arc::clone(&journal),
            whippet_journal::GroupCommitConfig {
                max_batch: TCP_BATCH_MAX,
                linger: Duration::ZERO,
            },
        );
        let rate_limiter = Arc::new(crate::ratelimit::RateLimiter::new(per_sec));
        let ws_connections = Arc::new(Semaphore::new(512));
        let ctx = Arc::new(NodeContext {
            config,
            journal: Arc::clone(&journal),
            engine_journals: vec![Arc::clone(&journal), Arc::clone(&journal)],
            engine_journal: journal,
            group_commit,
            hub,
            metrics,
            snapshots: SnapshotStore::new(dir.path().join("snapshots")),
            started: Instant::now(),
            shutdown,
            shards_ok: Arc::new(AtomicBool::new(true)),
            rate_limiter,
            ws_connections,
            snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        });
        TestNode { ctx, _dir: dir }
    }

    impl TestNode {
        fn memory(&self) -> &MemoryJournal {
            match &*self.ctx.journal {
                Journal::Memory(journal) => journal,
                Journal::Iggy(_) => panic!("test nodes must use the memory journal"),
            }
        }
    }

    fn command(symbol: &str, client_id: u8) -> Command {
        Command {
            kind: CommandKind::NewOrder,
            symbol: Symbol::new(symbol).unwrap(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Price(6_500_000),
            qty: Qty(1),
            order_id: OrderId(u128::from(client_id)),
            participant: ParticipantId(u64::from(client_id)),
            timestamp_ns: 42,
            nonce: 7,
            flags: 0,
        }
    }

    fn error_code(response: &TcpResponse) -> Option<u16> {
        match response {
            TcpResponse::Error { code, .. } => Some(*code),
            _ => None,
        }
    }

    fn submit_ack(response: TcpResponse) -> (u32, u64, u32) {
        match response {
            TcpResponse::SubmitAck {
                partition,
                base_offset,
                count,
                ..
            } => (partition, base_offset, count),
            other => panic!("expected SubmitAck, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unauthorized_submit_is_rejected_and_closes() {
        let node = test_node(Some("s3cret"));
        let mut state = ConnectionState::new(&node.ctx);
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![command("BTC-USD", 1)],
            },
        )
        .await;
        assert!(close, "an unauthorized submit must close the connection");
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::Unauthorized.as_u16())
        );
        let partition = Symbol::new("BTC-USD")
            .unwrap()
            .partition(node.ctx.exchange().partitions);
        assert!(
            node.memory()
                .read(Topic::Orders, partition, 0, 10)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn hello_gates_submit() {
        let node = test_node(Some("s3cret"));
        let mut state = ConnectionState::new(&node.ctx);
        assert!(!state.authorized);

        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "wrong".into(),
            },
        )
        .await;
        assert_eq!(
            response,
            TcpResponse::HelloAck {
                ok: false,
                message: "unauthorized".into(),
            }
        );
        assert!(close, "a mismatched hello must close the connection");
        assert!(!state.authorized);

        let mut state = ConnectionState::new(&node.ctx);
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "s3cret".into(),
            },
        )
        .await;
        assert_eq!(
            response,
            TcpResponse::HelloAck {
                ok: true,
                message: "ok".into(),
            }
        );
        assert!(!close);
        assert!(state.authorized);

        // A hello after authorization is a no-op ack, even with a wrong token.
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "wrong".into(),
            },
        )
        .await;
        assert_eq!(
            response,
            TcpResponse::HelloAck {
                ok: true,
                message: "ok".into(),
            }
        );
        assert!(!close);
        assert!(state.authorized);
    }

    #[tokio::test]
    async fn hello_is_accepted_on_an_anonymous_node() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);
        assert!(state.authorized);
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello { token: "".into() },
        )
        .await;
        assert_eq!(
            response,
            TcpResponse::HelloAck {
                ok: true,
                message: "ok".into(),
            }
        );
        assert!(!close);
    }

    #[tokio::test]
    async fn ping_answers_with_server_time() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Ping { client_ns: 99 },
        )
        .await;
        assert!(!close);
        match response {
            TcpResponse::Pong {
                client_ns,
                server_ns,
            } => {
                assert_eq!(client_ns, 99);
                assert!(server_ns > 0);
            }
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn hello_attempts_are_rate_limited() {
        let node = test_node(Some("s3cret"));
        let limiter = Arc::new(RateLimiter::new(1));
        let peer = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

        // The first guess spends the one token and is refused the normal way.
        let mut state = ConnectionState::with_peer(&node.ctx, peer, Arc::clone(&limiter));
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "wrong".into(),
            },
        )
        .await;
        assert!(close, "a mismatched hello must close the connection");
        assert_eq!(
            response,
            TcpResponse::HelloAck {
                ok: false,
                message: "unauthorized".into(),
            }
        );

        // Guessing cannot continue at wire speed: the next attempt is refused
        // before the token comparison, on a fresh connection from the same IP.
        let mut state = ConnectionState::with_peer(&node.ctx, peer, Arc::clone(&limiter));
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "wrong".into(),
            },
        )
        .await;
        assert!(!close, "a rate-limited hello keeps the connection open");
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::RateLimited.as_u16())
        );
        assert!(!state.authorized);

        // Even the right token is not compared while the budget is spent.
        let mut state = ConnectionState::with_peer(&node.ctx, peer, Arc::clone(&limiter));
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "s3cret".into(),
            },
        )
        .await;
        assert!(!close);
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::RateLimited.as_u16())
        );
        assert!(!state.authorized);
    }

    #[tokio::test]
    async fn ping_stays_available_after_the_budget_is_spent() {
        let node = test_node(Some("s3cret"));
        let limiter = Arc::new(RateLimiter::new(1));
        let peer = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        let mut state = ConnectionState::with_peer(&node.ctx, peer, Arc::clone(&limiter));

        // Spend the budget on a handshake attempt.
        let (response, _) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Hello {
                token: "wrong".into(),
            },
        )
        .await;
        assert_eq!(error_code(&response), None);

        // `Ping` is deliberately unrated: a client must always be able to
        // measure clock offset, even from a throttled address.
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Ping { client_ns: 5 },
        )
        .await;
        assert!(!close);
        match response {
            TcpResponse::Pong {
                client_ns,
                server_ns,
            } => {
                assert_eq!(client_ns, 5);
                assert!(server_ns > 0);
            }
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    /// One valid order through the real router, from `peer`, so the shared
    /// limiter sees the same source address as the TCP frames below.
    async fn post_order(
        ctx: Arc<NodeContext>,
        peer: SocketAddr,
        client_order_id: &str,
    ) -> StatusCode {
        let app = crate::gateway::router(ctx);
        let body = format!(
            r#"{{"symbol":"BTC-USD","side":"buy","price_cents":6500000,"qty":1,"client_order_id":"{client_order_id}","participant":"alice"}}"#
        );
        let request = Request::builder()
            .method("POST")
            .uri("/api/orders")
            .header(header::CONTENT_TYPE, "application/json")
            .extension(ConnectInfo(peer))
            .body(Body::from(body))
            .expect("valid request");
        app.oneshot(request)
            .await
            .expect("router responds")
            .status()
    }

    #[tokio::test]
    async fn http_and_tcp_share_one_rate_limit_budget() {
        let node = test_node_with_rate_limit(None, 1);
        let peer: SocketAddr = "203.0.113.7:4000".parse().expect("peer address");
        assert_eq!(
            post_order(Arc::clone(&node.ctx), peer, "shared-http").await,
            StatusCode::OK
        );

        // Same context, so the same limiter instance and the same source IP:
        // the HTTP request already spent the one token.
        let mut state =
            ConnectionState::with_peer(&node.ctx, peer.ip(), Arc::clone(&node.ctx.rate_limiter));
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![command("BTC-USD", 1)],
            },
        )
        .await;
        assert!(!close);
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::RateLimited.as_u16()),
            "the HTTP request already spent the shared budget"
        );
    }

    #[tokio::test]
    async fn tcp_submit_spends_the_budget_for_http() {
        let node = test_node_with_rate_limit(None, 1);
        let peer: SocketAddr = "203.0.113.8:4000".parse().expect("peer address");
        let mut state =
            ConnectionState::with_peer(&node.ctx, peer.ip(), Arc::clone(&node.ctx.rate_limiter));
        let (response, _) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![command("BTC-USD", 1)],
            },
        )
        .await;
        assert_eq!(
            error_code(&response),
            None,
            "the first submit fits the budget"
        );

        assert_eq!(
            post_order(Arc::clone(&node.ctx), peer, "shared-tcp").await,
            StatusCode::TOO_MANY_REQUESTS,
            "the TCP submit already spent the shared budget"
        );
    }

    /// The whole batch is charged before validation, so a batch above the
    /// bucket burst is refused as `RateLimited` before any validate work.
    #[tokio::test]
    async fn a_batch_above_the_burst_is_rate_limited() {
        let node = test_node(None);
        let limiter = Arc::new(RateLimiter::new(2));
        let peer = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        let mut state = ConnectionState::with_peer(&node.ctx, peer, limiter);

        let commands = vec![
            command("BTC-USD", 1),
            command("BTC-USD", 2),
            command("BTC-USD", 3),
        ];
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit { commands },
        )
        .await;
        assert!(!close, "a rate-limited submit keeps the connection usable");
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::RateLimited.as_u16())
        );
        let partition = Symbol::new("BTC-USD")
            .unwrap()
            .partition(node.ctx.exchange().partitions);
        assert!(
            node.memory()
                .read(Topic::Orders, partition, 0, 10)
                .is_empty(),
            "a rate-limited batch must journal nothing"
        );
    }

    #[tokio::test]
    async fn mixed_symbols_are_rejected() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![command("BTC-USD", 1), command("ETH-USD", 2)],
            },
        )
        .await;
        assert!(!close, "a rejected batch must keep the connection usable");
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::MixedSymbols.as_u16())
        );
        for symbol in ["BTC-USD", "ETH-USD"] {
            let partition = Symbol::new(symbol)
                .unwrap()
                .partition(node.ctx.exchange().partitions);
            assert!(
                node.memory()
                    .read(Topic::Orders, partition, 0, 10)
                    .is_empty(),
                "a rejected batch must journal nothing for {symbol}"
            );
        }
    }

    #[tokio::test]
    async fn empty_and_oversized_batches_are_rejected() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);

        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: Vec::new(),
            },
        )
        .await;
        assert!(!close);
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::BatchTooLarge.as_u16())
        );

        let commands = vec![command("BTC-USD", 1); TCP_BATCH_MAX + 1];
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit { commands },
        )
        .await;
        assert!(!close);
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::BatchTooLarge.as_u16())
        );
    }

    #[tokio::test]
    async fn a_zero_qty_new_order_is_rejected() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);
        let mut order = command("BTC-USD", 1);
        order.qty = Qty::ZERO;
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![order],
            },
        )
        .await;
        assert!(!close);
        assert_eq!(
            error_code(&response),
            Some(TcpErrorCode::InvalidCommand.as_u16())
        );
    }

    #[tokio::test]
    async fn a_valid_batch_is_one_append_with_contiguous_offsets() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);
        let partition = Symbol::new("BTC-USD")
            .unwrap()
            .partition(node.ctx.exchange().partitions);

        let commands = vec![command("BTC-USD", 1), command("BTC-USD", 2)];
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: commands.clone(),
            },
        )
        .await;
        assert!(!close);
        assert_eq!(submit_ack(response), (partition, 0, 2));

        let records = node.memory().read(Topic::Orders, partition, 0, 10);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        let journaled: Vec<Command> = records
            .iter()
            .map(|record| Command::decode(&record.payload).expect("valid command"))
            .collect();
        assert_eq!(journaled, commands);
        assert_eq!(records[0].header("symbol"), Some("BTC-USD"));

        // A second batch continues at the next offset.
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![command("BTC-USD", 3)],
            },
        )
        .await;
        assert!(!close);
        assert_eq!(submit_ack(response), (partition, 2, 1));
    }

    #[tokio::test]
    async fn cancel_commands_are_accepted_with_their_zero_qty() {
        let node = test_node(None);
        let mut state = ConnectionState::new(&node.ctx);
        let symbol = Symbol::new("BTC-USD").unwrap();
        let cancel = Command::cancel(symbol, OrderId(9), ParticipantId(1), 42);
        let (response, close) = handle_request(
            &node.ctx,
            &node.ctx.group_commit,
            &mut state,
            TcpRequest::Submit {
                commands: vec![cancel],
            },
        )
        .await;
        assert!(!close);
        let partition = symbol.partition(node.ctx.exchange().partitions);
        assert_eq!(submit_ack(response), (partition, 0, 1));
    }

    fn reserve_addr() -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
        let addr = listener.local_addr().expect("local address");
        drop(listener);
        addr
    }

    async fn connect(addr: SocketAddr) -> TcpStream {
        for _ in 0..100 {
            match TcpStream::connect(addr).await {
                Ok(stream) => return stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
        panic!("tcp ingress did not accept a connection at {addr}");
    }

    async fn read_response(stream: &mut TcpStream) -> TcpResponse {
        let mut prefix = [0u8; 4];
        stream.read_exact(&mut prefix).await.expect("frame prefix");
        let len = body_len(prefix).expect("valid frame length");
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).await.expect("frame body");
        TcpResponse::decode_body(&body).expect("valid response")
    }

    #[tokio::test]
    async fn pipelined_frames_are_answered_in_order() {
        let node = test_node(Some("s3cret"));
        let addr = reserve_addr();
        tokio::spawn(serve(Arc::clone(&node.ctx), addr));

        let mut stream = connect(addr).await;
        // Write every frame before reading a single ack: the server must still
        // answer in request order, one in flight at a time.
        let mut out = Vec::new();
        out.extend_from_slice(
            &TcpRequest::Hello {
                token: "s3cret".into(),
            }
            .encode(),
        );
        out.extend_from_slice(
            &TcpRequest::Submit {
                commands: vec![command("BTC-USD", 1), command("BTC-USD", 2)],
            }
            .encode(),
        );
        out.extend_from_slice(
            &TcpRequest::Submit {
                commands: vec![command("BTC-USD", 3)],
            }
            .encode(),
        );
        out.extend_from_slice(&TcpRequest::Ping { client_ns: 7 }.encode());
        stream
            .write_all(&out)
            .await
            .expect("write pipelined frames");

        assert_eq!(
            read_response(&mut stream).await,
            TcpResponse::HelloAck {
                ok: true,
                message: "ok".into(),
            }
        );
        assert_eq!(submit_ack(read_response(&mut stream).await).1, 0);
        assert_eq!(submit_ack(read_response(&mut stream).await).1, 2);
        match read_response(&mut stream).await {
            TcpResponse::Pong {
                client_ns,
                server_ns,
            } => {
                assert_eq!(client_ns, 7);
                assert!(server_ns > 0);
            }
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_malformed_frame_gets_a_bad_frame_error_and_a_close() {
        let node = test_node(None);
        let addr = reserve_addr();
        tokio::spawn(serve(Arc::clone(&node.ctx), addr));

        let mut stream = connect(addr).await;
        // A one-byte body with an unknown request kind desynchronises nothing
        // structurally, but it is undecodable, so the server answers and closes.
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_le_bytes());
        frame.push(0xff);
        stream.write_all(&frame).await.expect("write bad frame");

        let response = read_response(&mut stream).await;
        assert_eq!(error_code(&response), Some(TcpErrorCode::BadFrame.as_u16()));

        let mut rest = [0u8; 1];
        match stream.read(&mut rest).await {
            Ok(0) => {}
            Ok(read) => panic!("server must close after a bad frame, read {read} bytes"),
            Err(error)
                if error.kind() == ErrorKind::UnexpectedEof
                    || error.kind() == ErrorKind::ConnectionReset => {}
            Err(error) => panic!("unexpected read error: {error}"),
        }
    }
}
