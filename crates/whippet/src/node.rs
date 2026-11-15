// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use axum::serve::ListenerExt;
use tokio::sync::{Semaphore, watch};
use tokio::time::sleep;
use tracing::{info, warn};
use whippet_journal::group::{GroupCommit, GroupCommitConfig};
use whippet_journal::{IggyJournal, Journal};

use crate::config::{JournalKind, NodeArgs, NodeConfig};
use crate::context::NodeContext;
use crate::engine_runner::{run_engine_shard, run_md_shard};
use crate::gateway;
use crate::ratelimit::RateLimiter;
use crate::snapshot::SnapshotStore;
use crate::tcp;

pub async fn run(args: NodeArgs) -> Result<()> {
    let config = NodeConfig::from_args(args)?;
    let journal = open_journal(&config).await?;
    // The engine gets its own connection. Ingress and matching then cannot
    // block each other on one Iggy client's response loop.
    let engine_journal = match config.journal {
        JournalKind::Iggy => open_journal(&config).await?,
        JournalKind::Memory => Arc::clone(&journal),
    };
    journal
        .ensure_topics(&config.exchange)
        .await
        .context("ensuring Whippet streams and topics")?;

    let dashboard_build = Some(format!("v{} · {}", env!("CARGO_PKG_VERSION"), config.build));
    let hub = whippet_marketdata::MdHub::new(
        config.exchange.clone(),
        config.iggy_addr.clone(),
        config.journal.as_str().to_string(),
        dashboard_build,
    );
    let metrics = hub.metrics();
    let (shutdown, _) = watch::channel(false);
    // One Orders writer per partition, shared by HTTP, TCP, and cancels.
    // Zero linger: the writer drains whatever is already queued when it wakes,
    // so a lone order is flushed immediately and a burst coalesces by arrival.
    let group_commit = GroupCommit::new(
        Arc::clone(&journal),
        GroupCommitConfig {
            max_batch: 256,
            linger: Duration::ZERO,
        },
    );
    let rate_limiter = Arc::new(RateLimiter::new(config.rate_limit_per_sec));
    // The fanout sweep goes to 1,000 subscribers; the cap is a guard, not a
    // product limit. Override it for a larger fanout run.
    let ws_cap = std::env::var("WHIPPET_MAX_WS_CONNECTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WS_CONNECTIONS);
    let ws_connections = Arc::new(Semaphore::new(ws_cap));
    let ctx = Arc::new(NodeContext {
        config: config.clone(),
        journal: Arc::clone(&journal),
        engine_journal,
        group_commit,
        hub: Arc::clone(&hub),
        metrics: Arc::clone(&metrics),
        snapshots: SnapshotStore::new(config.snapshot_dir.clone()),
        started: Instant::now(),
        shutdown,
        shards_ok: Arc::new(AtomicBool::new(true)),
        rate_limiter,
        ws_connections,
    });

    hub.spawn_publisher();
    spawn_health_monitor(Arc::clone(&ctx));

    let mut handles = Vec::new();
    for shard in ctx.active_shards() {
        let engine_ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let result = run_engine_shard(Arc::clone(&engine_ctx), shard).await;
            if let Err(error) = &result {
                tracing::error!(shard, error = %error, "engine shard stopped unexpectedly");
                engine_ctx.mark_shard_failed();
            }
            result
        }));
        let md_ctx = Arc::clone(&ctx);
        handles.push(tokio::spawn(async move {
            let result = run_md_shard(Arc::clone(&md_ctx), shard).await;
            if let Err(error) = &result {
                tracing::error!(shard, error = %error, "MD shard stopped unexpectedly");
                md_ctx.mark_shard_failed();
            }
            result
        }));
    }
    info!(
        shards = ?ctx.active_shards(),
        partitions = config.exchange.partitions,
        mode = %config.mode,
        journal = ?config.journal,
        "Whippet node started"
    );

    if let Some(tcp_addr) = config.tcp_addr {
        let tcp_ctx = Arc::clone(&ctx);
        let mut tcp_shutdown = ctx.shutdown.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                result = tcp::serve(tcp_ctx, tcp_addr) => {
                    if let Err(error) = result {
                        tracing::error!(error = %error, "TCP ingress stopped");
                    }
                }
                _ = tcp_shutdown.changed() => {}
            }
        });
        info!(address = %tcp_addr, "binary TCP ingress listening");
    }

    let app = gateway::router(Arc::clone(&ctx));
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("binding {}", config.http_addr))?
        .tap_io(|stream| {
            // Small request/response frames on the order path: do not let
            // Nagle/delayed-ACK add latency.
            let _ = stream.set_nodelay(true);
        });
    info!(
        address = %config.http_addr,
        dashboard = %config.dashboard_dir.display(),
        auth_required = config.auth_required(),
        rate_limit_per_sec = config.rate_limit_per_sec,
        "HTTP API and dashboard listening"
    );
    if !config.auth_required() {
        warn!(
            "order ingress is anonymous; set WHIPPET_API_TOKEN before exposing this node to another host"
        );
    }

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("serving HTTP")?;

    info!("shutdown requested; flushing snapshots");
    let _ = ctx.shutdown.send(true);
    for handle in handles {
        match tokio::time::timeout(Duration::from_secs(5), handle).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => warn!(error = %error, "a shard task exited with an error"),
            Ok(Err(error)) => warn!(error = %error, "a shard task panicked"),
            Err(_) => warn!("a shard task did not stop within five seconds"),
        }
    }
    info!("Whippet node stopped");
    Ok(())
}

/// Caps concurrent WebSocket subscribers. Each holds a task and an egress
/// stream; this is a memory/bandwidth guard, not a user limit.
const DEFAULT_WS_CONNECTIONS: usize = 2_048;

async fn open_journal(config: &NodeConfig) -> Result<Arc<Journal>> {
    Ok(match config.journal {
        JournalKind::Iggy => {
            info!(
                address = %config.iggy_addr,
                "connecting to Apache Iggy"
            );
            Arc::new(connect_iggy(config).await?)
        }
        JournalKind::Memory => {
            warn!(
                "using the in-memory journal; this is a developer escape hatch, not the demo path"
            );
            Arc::new(Journal::memory())
        }
    })
}

async fn connect_iggy(config: &NodeConfig) -> Result<Journal> {
    let mut delay = Duration::from_millis(250);
    for attempt in 1..=120 {
        match IggyJournal::connect(&config.iggy_addr, &config.iggy_user, &config.iggy_password)
            .await
        {
            Ok(journal) => {
                info!(attempt, "connected to Iggy");
                return Ok(Journal::Iggy(journal));
            }
            Err(error) => {
                if attempt == 120 {
                    bail!("could not connect to Iggy at {}: {error}", config.iggy_addr);
                }
                warn!(
                    attempt,
                    address = %config.iggy_addr,
                    error = %error,
                    "Iggy is not ready; retrying"
                );
                sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(2));
            }
        }
    }
    unreachable!("loop returns or bails")
}

fn spawn_health_monitor(ctx: Arc<NodeContext>) {
    tokio::spawn(async move {
        let mut shutdown = ctx.shutdown.subscribe();
        // Both connections matter: an ingress link that is up while the engine
        // link is down still means orders are accepted and never matched.
        let connected = if ctx.journal.is_iggy() {
            ctx.journal.ping().await && ctx.engine_journal.ping().await
        } else {
            true
        };
        ctx.metrics.set_iggy_connected(connected);
        refresh_lag(&ctx).await;
        loop {
            tokio::select! {
                    _ = shutdown.changed() => return,
                    _ = sleep(Duration::from_secs(5)) => {
                        // Both connections matter: an ingress link that is up while the engine
            // link is down still means orders are accepted and never matched.
            let connected = if ctx.journal.is_iggy() {
                ctx.journal.ping().await && ctx.engine_journal.ping().await
            } else {
                true
            };
                        ctx.metrics.set_iggy_connected(connected);
                        refresh_lag(&ctx).await;
                        if !connected {
                            warn!("Iggy health check failed");
                        }
                    }
                }
        }
    });
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}

async fn refresh_lag(ctx: &NodeContext) {
    // Each `committed`/`lag` pair is one round trip, so walking the shards
    // sequentially costs two round trips per shard every cycle (32 at 16
    // shards). Issue every shard's pair concurrently and sum the results;
    // error handling is unchanged — a shard that fails either call contributes
    // nothing to the total.
    let per_shard =
        futures_util::future::join_all(ctx.active_shards().iter().map(|&shard| async move {
            let consumer = format!("whippet-engine-p{shard}");
            let Ok(Some(committed)) = ctx
                .engine_journal
                .committed(whippet_journal::Topic::Orders, shard, &consumer)
                .await
            else {
                return 0u64;
            };
            ctx.engine_journal
                .lag(whippet_journal::Topic::Orders, shard, committed)
                .await
                .unwrap_or(0)
        }))
        .await;
    let total = per_shard.into_iter().fold(0u64, u64::saturating_add);
    ctx.metrics.set_consumer_lag(total);
}
