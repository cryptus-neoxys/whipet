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
use crate::turbo::TurboIngress;

/// Caps the engine journal pool. Each entry is one Iggy connection; 16
/// matches the demo partition count. Nodes with more shards wrap around via
/// modulo selection instead of opening unbounded connections.
pub const MAX_ENGINE_JOURNAL_CLIENTS: usize = 16;

/// Pool sizing for the engine journal clients: the
/// `WHIPPET_ENGINE_JOURNAL_CLIENTS` override wins when it parses, otherwise
/// the pool matches the number of active shards. Both paths clamp into
/// `[1, MAX_ENGINE_JOURNAL_CLIENTS]` so the pool is never empty and never
/// larger than the cap.
pub fn engine_journal_pool_size(shard_count: usize, override_clients: Option<usize>) -> usize {
    override_clients
        .unwrap_or(shard_count)
        .clamp(1, MAX_ENGINE_JOURNAL_CLIENTS)
}

/// One MiB, used by the pending-budget arithmetic below.
pub const BYTES_PER_MIB: usize = 1024 * 1024;

/// Default total pending-operations budget for the engine pool, in MiB
/// (`WHIPPET_ENGINE_JOURNAL_PENDING_MIB` when unset).
pub const DEFAULT_ENGINE_JOURNAL_PENDING_MIB: usize = 256;

/// Smallest accepted total pending budget, in MiB.
pub const MIN_ENGINE_JOURNAL_PENDING_MIB: usize = 16;

/// Largest accepted total pending budget, in MiB.
pub const MAX_ENGINE_JOURNAL_PENDING_MIB: usize = 4096;

/// Per-client floor. A pool this large can exceed the configured total:
/// `total / pool_size` below the floor is raised back to it, so a client is
/// never starved into swapping its pending frames to the wire.
pub const ENGINE_JOURNAL_PENDING_FLOOR_BYTES: usize = 16 * BYTES_PER_MIB;

/// Split the engine pool's total pending-operations budget across
/// `pool_size` clients.
///
/// `override_mib` is the parsed `WHIPPET_ENGINE_JOURNAL_PENDING_MIB` value
/// (or `None`); it is clamped into
/// `[MIN_ENGINE_JOURNAL_PENDING_MIB, MAX_ENGINE_JOURNAL_PENDING_MIB]`. The
/// total is then divided across the pool with a per-client floor of
/// [`ENGINE_JOURNAL_PENDING_FLOOR_BYTES`]:
/// `per_client = max(16 MiB, total / pool_size)`. Returns
/// `(total_bytes, per_client_bytes)`; the caller logs both and passes the
/// per-client value to `IggyJournal::connect_with_pending`.
pub fn engine_journal_pending_budget(
    override_mib: Option<usize>,
    pool_size: usize,
) -> (usize, usize) {
    let total_mib = override_mib
        .unwrap_or(DEFAULT_ENGINE_JOURNAL_PENDING_MIB)
        .clamp(
            MIN_ENGINE_JOURNAL_PENDING_MIB,
            MAX_ENGINE_JOURNAL_PENDING_MIB,
        );
    let total_bytes = total_mib * BYTES_PER_MIB;
    let per_client_bytes = (total_bytes / pool_size.max(1)).max(ENGINE_JOURNAL_PENDING_FLOOR_BYTES);
    (total_bytes, per_client_bytes)
}

/// Shared node state. Every task holds an `Arc<NodeContext>`; none of the
/// fields is mutable after startup.
pub struct NodeContext {
    pub config: NodeConfig,
    /// Ingress journal. The gateway and TCP listener append Orders here.
    pub journal: Arc<Journal>,
    /// Engine-side journal pool. In `iggy` mode every entry is its own Iggy
    /// client (its own TCP connection and response loop), so one shard's
    /// appends and reads cannot head-of-line block another shard, and neither
    /// can the MD consumer or the lag sweep. Engine shards must select an
    /// entry through [`NodeContext::engine_journal_for`], never by indexing
    /// the pool directly. In `memory` mode every entry is the same `Arc` (the
    /// ingress journal), so the pool is a no-op.
    pub engine_journals: Vec<Arc<Journal>>,
    /// Observer handle into the engine journal pool: a clone of pool entry 0.
    /// The health monitor pings every pool entry and caches the aggregate in
    /// [`Metrics`], so `/readyz` does not use this handle. The consumer-lag
    /// sweep and both shard consumers select through
    /// [`NodeContext::engine_journal_for`]. The observer deliberately shares
    /// the first pool entry rather than opening another connection, so
    /// observing costs no extra client. Never use it for shard traffic; use
    /// [`NodeContext::engine_journal_for`].
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
    /// Serializes snapshot serialization/fsync across shards. Three shard
    /// writers fsyncing at once contend with Iggy's WAL on the same volume.
    pub snapshot_lock: Arc<std::sync::Mutex<()>>,
    /// Turbo ingress lanes and per-shard windows. `None` in durable mode.
    pub turbo: Option<Arc<TurboIngress>>,
}

impl NodeContext {
    /// Deterministic pool selection for an engine shard: `shard_id % len`.
    /// The same shard always gets the same client, and every result is an
    /// entry of the pool. Panics only if the pool is empty, which is a
    /// construction bug; every constructor builds at least one entry.
    pub fn engine_journal_for(&self, shard_id: u32) -> &Arc<Journal> {
        assert!(
            !self.engine_journals.is_empty(),
            "engine journal pool must not be empty"
        );
        &self.engine_journals[shard_id as usize % self.engine_journals.len()]
    }

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
