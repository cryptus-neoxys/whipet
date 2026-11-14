// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{info, warn};
use whippet_journal::{IggyJournal, Journal};

use crate::config::{JournalKind, NodeArgs, NodeConfig};
use crate::context::NodeContext;
use crate::engine_runner::{run_engine_shard, run_md_shard};
use crate::gateway;
use crate::snapshot::SnapshotStore;

pub async fn run(args: NodeArgs) -> Result<()> {
    let config = NodeConfig::from_args(args)?;
    let journal = match config.journal {
        JournalKind::Iggy => {
            info!(
                address = %config.iggy_addr,
                "connecting to Apache Iggy"
            );
            Arc::new(connect_iggy(&config).await?)
        }
        JournalKind::Memory => {
            warn!(
                "using the in-memory journal; this is a developer escape hatch, not the demo path"
            );
            Arc::new(Journal::memory())
        }
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
    let ctx = Arc::new(NodeContext {
        config: config.clone(),
        journal: Arc::clone(&journal),
        hub: Arc::clone(&hub),
        metrics: Arc::clone(&metrics),
        snapshots: SnapshotStore::new(config.snapshot_dir.clone()),
        started: Instant::now(),
        shutdown,
    });

    hub.spawn_publisher();
    spawn_health_monitor(Arc::clone(&ctx));

    let mut handles = Vec::new();
    for shard in ctx.active_shards() {
        handles.push(tokio::spawn(run_engine_shard(Arc::clone(&ctx), shard)));
        handles.push(tokio::spawn(run_md_shard(Arc::clone(&ctx), shard)));
    }
    info!(
        shards = ?ctx.active_shards(),
        partitions = config.exchange.partitions,
        mode = %config.mode,
        journal = ?config.journal,
        "Whippet node started"
    );

    let app = gateway::router(Arc::clone(&ctx));
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("binding {}", config.http_addr))?;
    info!(
        address = %config.http_addr,
        dashboard = %config.dashboard_dir.display(),
        "HTTP API and dashboard listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serving HTTP")?;

    info!("shutdown requested; flushing snapshots");
    let _ = ctx.shutdown.send(true);
    for handle in handles {
        if tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .is_err()
        {
            warn!("a shard task did not stop within five seconds");
        }
    }
    info!("Whippet node stopped");
    Ok(())
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
        let connected = ctx.journal.ping().await;
        ctx.metrics.set_iggy_connected(connected);
        loop {
            tokio::select! {
                _ = shutdown.changed() => return,
                _ = sleep(Duration::from_secs(5)) => {
                    let connected = ctx.journal.ping().await;
                    ctx.metrics.set_iggy_connected(connected);
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
