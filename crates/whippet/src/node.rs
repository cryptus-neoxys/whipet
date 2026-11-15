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
use whippet_core::config::ExchangeConfig;
use whippet_journal::group::{GroupCommit, GroupCommitConfig};
use whippet_journal::{IggyJournal, Journal};

use crate::config::{JournalKind, NodeArgs, NodeConfig};
use crate::context::{
    BYTES_PER_MIB, NodeContext, engine_journal_pending_budget, engine_journal_pool_size,
};
use crate::engine_runner::{run_engine_shard, run_md_shard};
use crate::gateway;
use crate::ratelimit::RateLimiter;
use crate::snapshot::SnapshotStore;
use crate::storage::{StorageMonitor, StorageSnapshot, StorageState};
use crate::tcp;
use crate::turbo::TurboIngress;

pub async fn run(args: NodeArgs) -> Result<()> {
    let config = NodeConfig::from_args(args)?;
    let journal = open_journal(&config).await?;
    // Engine journal pool: one Iggy client per entry, so a shard's appends
    // and reads cannot head-of-line block another shard on a shared response
    // loop. The default pool size is the number of active shards, clamped to
    // [1, MAX_ENGINE_JOURNAL_CLIENTS]; WHIPPET_ENGINE_JOURNAL_CLIENTS
    // overrides it. Memory mode shares the ingress journal Arc, so extra
    // entries are free there.
    let shard_count = active_shard_count(&config.exchange);
    let clients_override = std::env::var("WHIPPET_ENGINE_JOURNAL_CLIENTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let pool_size = engine_journal_pool_size(shard_count, clients_override);
    // Total client-side pending budget for the engine pool, in MiB
    // (WHIPPET_ENGINE_JOURNAL_PENDING_MIB, default 256), split across the
    // pool with a 16 MiB per-client floor. The ingress client keeps its own
    // budget via `IggyJournal::connect`. In memory mode the budget is unused:
    // every pool entry is the ingress Arc.
    let pending_override = std::env::var("WHIPPET_ENGINE_JOURNAL_PENDING_MIB")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let (pending_total_bytes, pending_per_client_bytes) =
        engine_journal_pending_budget(pending_override, pool_size);
    let engine_journals =
        open_engine_journal_pool(&config, &journal, pool_size, pending_per_client_bytes).await?;
    // The observer (gateway health probe) shares pool entry 0 rather than
    // opening another connection.
    let engine_journal = Arc::clone(&engine_journals[0]);
    info!(
        clients = pool_size,
        shards = shard_count,
        pending_total_mib = pending_total_bytes / BYTES_PER_MIB,
        pending_per_client_mib = pending_per_client_bytes / BYTES_PER_MIB,
        "engine journal client pool ready"
    );
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
    // Turbo builds one bounded lane per active shard before the context, so
    // the gateway and TCP listener can resolve a partition to its lane. The
    // matching receivers move into the shard tasks below.
    let active_shards = {
        let mut shards: Vec<u32> = config
            .exchange
            .instruments
            .iter()
            .map(|instrument| instrument.symbol.partition(config.exchange.partitions))
            .collect();
        shards.sort_unstable();
        shards.dedup();
        shards
    };
    let (turbo, mut turbo_receivers) = if config.mode == whippet_core::config::Mode::Turbo {
        let (ingress, receivers) = TurboIngress::new(config.turbo, active_shards.iter().copied());
        (Some(ingress), receivers)
    } else {
        (None, std::collections::HashMap::new())
    };
    if let Some(turbo) = &turbo {
        info!(
            shards = turbo.shards().count(),
            window_commands = config.turbo.window_commands,
            window_ms = config.turbo.window_ms,
            "turbo ingress ready: acknowledgements are match-complete, not journal-durable"
        );
    }
    // The bounded-storage guard samples both data volumes before the context
    // exists, so the boot log below already reports the state the node starts
    // with. It never deletes anything; it only measures and classifies.
    let storage = StorageMonitor::new(config.storage.clone());
    info!(
        whippet_dir = %config.storage.whippet_dir.display(),
        iggy_dir = config
            .storage
            .iggy_dir
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "not mounted".to_string()),
        warn_pct = config.storage.warn_pct,
        crit_pct = config.storage.crit_pct,
        "storage monitor sampling the node and Iggy data volumes"
    );
    let ctx = Arc::new(NodeContext {
        config: config.clone(),
        journal: Arc::clone(&journal),
        engine_journals,
        engine_journal,
        group_commit,
        hub: Arc::clone(&hub),
        metrics: Arc::clone(&metrics),
        storage,
        snapshots: SnapshotStore::new(config.snapshot_dir.clone()),
        started: Instant::now(),
        shutdown,
        shards_ok: Arc::new(AtomicBool::new(true)),
        rate_limiter,
        ws_connections,
        snapshot_lock: Arc::new(std::sync::Mutex::new(())),
        turbo,
    });

    hub.spawn_publisher();
    spawn_health_monitor(Arc::clone(&ctx));
    spawn_storage_monitor(Arc::clone(&ctx));

    let mut handles = Vec::new();
    for shard in ctx.active_shards() {
        let engine_ctx = Arc::clone(&ctx);
        let turbo_rx = turbo_receivers.remove(&shard);
        handles.push(tokio::spawn(async move {
            let result = run_engine_shard(Arc::clone(&engine_ctx), shard, turbo_rx).await;
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

/// One engine journal client per pool entry. In `iggy` mode each entry is a
/// fresh Iggy TCP connection with its own response loop and its own slice of
/// the pool's pending budget (`IggyJournal::connect_with_pending`); in
/// `memory` mode every entry is the ingress journal's `Arc`, so the pool adds
/// no state and the budget does not multiply. No entry is dropped before
/// `NodeContext` is, so no connection leaks.
async fn open_engine_journal_pool(
    config: &NodeConfig,
    ingress: &Arc<Journal>,
    size: usize,
    pending_bytes: usize,
) -> Result<Vec<Arc<Journal>>> {
    match config.journal {
        JournalKind::Iggy => {
            let mut pool = Vec::with_capacity(size);
            for _ in 0..size {
                // connect_iggy retries until Iggy accepts; each call opens a
                // dedicated client for one pool entry.
                pool.push(Arc::new(
                    connect_iggy_with_pending(config, pending_bytes).await?,
                ));
            }
            Ok(pool)
        }
        JournalKind::Memory => Ok(vec![Arc::clone(ingress); size]),
    }
}

/// Distinct partitions that own a configured symbol; the pool default sizes
/// to this so one client per shard is the common case.
fn active_shard_count(exchange: &ExchangeConfig) -> usize {
    let mut shards: Vec<u32> = exchange
        .instruments
        .iter()
        .map(|instrument| instrument.symbol.partition(exchange.partitions))
        .collect();
    shards.sort_unstable();
    shards.dedup();
    shards.len()
}

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

/// Ingress client: the full default pending budget (`IggyJournal::connect`).
async fn connect_iggy(config: &NodeConfig) -> Result<Journal> {
    connect_iggy_with_pending(config, whippet_journal::DEFAULT_PENDING_MAX_BYTES).await
}

async fn connect_iggy_with_pending(config: &NodeConfig, pending_bytes: usize) -> Result<Journal> {
    let mut delay = Duration::from_millis(250);
    for attempt in 1..=120 {
        match IggyJournal::connect_with_pending(
            &config.iggy_addr,
            &config.iggy_user,
            &config.iggy_password,
            pending_bytes,
        )
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

/// How often the bounded-storage guard re-samples both data volumes. Five
/// seconds matches the Iggy health cycle: fast enough that a soak cannot fill a
/// volume between checks, slow enough to stay out of the log.
const STORAGE_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Re-samples the node and Iggy data volumes and logs only state transitions.
///
/// The first sample happens in [`StorageMonitor::new`]; this task reports the
/// boot state once and then logs only when the aggregate state changes, so a
/// steady disk costs one line at startup rather than one every five seconds.
fn spawn_storage_monitor(ctx: Arc<NodeContext>) {
    tokio::spawn(async move {
        let mut shutdown = ctx.shutdown.subscribe();
        let mut last = None;
        log_storage_state(&ctx, &mut last, ctx.storage.snapshot());
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = sleep(STORAGE_REFRESH_INTERVAL) => {
                    let snapshot = ctx.storage.refresh();
                    log_storage_state(&ctx, &mut last, snapshot);
                }
            }
        }
    });
}

/// One line per state transition, with the numbers an operator needs to act:
/// free bytes and used percent per known volume, plus the thresholds in force.
fn log_storage_state(
    ctx: &NodeContext,
    last: &mut Option<StorageState>,
    snapshot: StorageSnapshot,
) {
    if last.replace(snapshot.state) == Some(snapshot.state) {
        return;
    }
    let whippet = snapshot.whippet;
    let iggy = snapshot.iggy;
    let iggy_free_bytes = iggy.map(|stat| stat.free_bytes);
    let iggy_used_pct = iggy.map(|stat| stat.used_pct);
    let (warn_pct, crit_pct) = (ctx.config.storage.warn_pct, ctx.config.storage.crit_pct);
    match snapshot.state {
        StorageState::Ok => info!(
            state = snapshot.state.as_str(),
            whippet_free_bytes = whippet.free_bytes,
            whippet_used_pct = whippet.used_pct,
            iggy_free_bytes,
            iggy_used_pct,
            warn_pct,
            crit_pct,
            "storage pressure is below the warning threshold"
        ),
        StorageState::Warn => warn!(
            state = snapshot.state.as_str(),
            whippet_available = whippet.available,
            whippet_free_bytes = whippet.free_bytes,
            whippet_used_pct = whippet.used_pct,
            iggy_free_bytes,
            iggy_used_pct,
            warn_pct,
            crit_pct,
            "storage usage is at or above the warning threshold, or a volume is unreadable; ingress stays open"
        ),
        StorageState::Critical => tracing::error!(
            state = snapshot.state.as_str(),
            whippet_free_bytes = whippet.free_bytes,
            whippet_used_pct = whippet.used_pct,
            iggy_free_bytes,
            iggy_used_pct,
            warn_pct,
            crit_pct,
            "storage usage is at or above the critical threshold; readiness and durable admission should fail"
        ),
    }
}

fn spawn_health_monitor(ctx: Arc<NodeContext>) {
    tokio::spawn(async move {
        let mut shutdown = ctx.shutdown.subscribe();
        let connected = health_cycle(&ctx).await;
        ctx.metrics.set_iggy_connected(connected);
        refresh_lag(&ctx).await;
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = sleep(Duration::from_secs(5)) => {
                    let connected = health_cycle(&ctx).await;
                    ctx.metrics.set_iggy_connected(connected);
                    refresh_lag(&ctx).await;
                }
            }
        }
    });
}

/// One health cycle: ping the ingress journal and every engine pool entry
/// concurrently, then refresh the consumer-lag sweep. The result is cached
/// into the single `iggy_connected` metric flag; no per-request path pings
/// the whole pool.
///
/// Every pool entry is its own Iggy connection, so each must answer: an
/// ingress link that is up while any engine link is down still means orders
/// are accepted and never matched.
async fn health_cycle(ctx: &NodeContext) -> bool {
    let connected = if ctx.journal.is_iggy() {
        let mut pings = Vec::with_capacity(ctx.engine_journals.len() + 1);
        pings.push(ctx.journal.ping());
        pings.extend(ctx.engine_journals.iter().map(|journal| journal.ping()));
        let pings = futures_util::future::join_all(pings).await;
        let down = pings.iter().filter(|&&up| !up).count();
        if down > 0 {
            warn!(
                down,
                clients = pings.len(),
                "engine journal pool has unhealthy clients"
            );
        }
        all_healthy(pings)
    } else {
        // Memory mode: every handle is the same Arc and pings always succeed.
        true
    };
    if !connected {
        warn!("Iggy health check failed");
    }
    connected
}

/// The pool (ingress included) is healthy only when every ping came back.
fn all_healthy(pings: impl IntoIterator<Item = bool>) -> bool {
    pings.into_iter().all(|up| up)
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
    //
    // Each shard reads through its own pool entry (`engine_journal_for`),
    // so the sweep no longer funnels every round trip through entry 0 and
    // contends with shard 0. When the pool is smaller than the shard count,
    // modulo selection hands several shards the same journal; that is fine,
    // those round trips stay concurrent.
    let per_shard =
        futures_util::future::join_all(ctx.active_shards().iter().map(|&shard| async move {
            let journal = ctx.engine_journal_for(shard);
            let consumer = format!("whippet-engine-p{shard}");
            let Ok(Some(committed)) = journal
                .committed(whippet_journal::Topic::Orders, shard, &consumer)
                .await
            else {
                return (shard, 0u64);
            };
            let lag = journal
                .lag(whippet_journal::Topic::Orders, shard, committed)
                .await
                .unwrap_or(0);
            (shard, lag)
        }))
        .await;
    // Keep the aggregate for compatibility and the per-shard series so one
    // lagging partition is visible instead of averaged away.
    let mut total = 0u64;
    for (shard, lag) in per_shard {
        ctx.metrics.set_consumer_lag_for_shard(shard, lag);
        total = total.saturating_add(lag);
    }
    ctx.metrics.set_consumer_lag(total);
    // Broker-reported log growth, refreshed on the same five-second cycle.
    // The storage guard acts on free space; this gauge explains what is
    // consuming it and feeds the read-only retention plan.
    for (index, topic) in [
        whippet_journal::Topic::Orders,
        whippet_journal::Topic::Executions,
        whippet_journal::Topic::Md,
    ]
    .into_iter()
    .enumerate()
    {
        match ctx.journal.topic_stats(topic).await {
            Ok((bytes, messages)) => ctx.metrics.set_topic_stats(index, bytes, messages),
            Err(error) => {
                warn!(?topic, error = %error, "topic size query failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::all_healthy;

    #[test]
    fn pool_health_requires_every_ping() {
        // An empty pool is vacuously healthy; the constructors never build one.
        assert!(all_healthy([]));
        assert!(all_healthy([true]));
        assert!(all_healthy([true, true, true]));
        // One dead client downgrades the whole pool, wherever it sits.
        assert!(!all_healthy([false, true, true]));
        assert!(!all_healthy([true, true, false]));
    }
}
