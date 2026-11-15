// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use tokio::sync::{Semaphore, watch};
use whippet_journal::{GroupCommit, Journal};
use whippet_marketdata::{MdHub, Metrics};

use crate::config::NodeConfig;
use crate::ratelimit::RateLimiter;
use crate::snapshot::SnapshotStore;

/// Shared node state. Every task holds an `Arc<NodeContext>`; none of the
/// fields is mutable after startup.
pub struct NodeContext {
    pub config: NodeConfig,
    /// Ingress journal. The gateway and TCP listener append Orders here.
    pub journal: Arc<Journal>,
    /// Engine-side journal. The shard reader/writer and MD consumer use a
    /// separate connection so a slow output append cannot head-of-line block
    /// ingress on the shared Iggy client.
    pub engine_journal: Arc<Journal>,
    /// One writer per `(topic, partition)` for Orders ingress. Every route
    /// (single order, batch, cancel, TCP) uses this so a partition has exactly
    /// one append writer and log order matches arrival order.
    pub group_commit: GroupCommit,
    pub hub: Arc<MdHub>,
    pub metrics: Arc<Metrics>,
    pub snapshots: SnapshotStore,
    pub started: Instant,
    pub shutdown: watch::Sender<bool>,
    pub shards_ok: Arc<AtomicBool>,
    /// One per-IP bucket shared by HTTP and TCP.
    pub rate_limiter: Arc<RateLimiter>,
    /// Caps concurrent WebSocket subscribers.
    pub ws_connections: Arc<Semaphore>,
}

impl NodeContext {
    pub fn mark_shard_failed(&self) {
        self.shards_ok.store(false, Ordering::Relaxed);
    }

    pub fn shards_ok(&self) -> bool {
        self.shards_ok.load(Ordering::Relaxed)
    }

    pub fn exchange(&self) -> &whippet_core::config::ExchangeConfig {
        &self.config.exchange
    }

    /// Partitions that actually own a configured symbol. The other partitions
    /// exist in Iggy for scale-out but have no local writer in this process.
    pub fn active_shards(&self) -> Vec<u32> {
        let mut shards: Vec<u32> = self
            .exchange()
            .instruments
            .iter()
            .map(|instrument| instrument.symbol.partition(self.exchange().partitions))
            .collect();
        shards.sort_unstable();
        shards.dedup();
        shards
    }
}
