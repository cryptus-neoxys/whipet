// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use tokio::sync::{Semaphore, watch};
use whippet_journal::{GroupCommit, Journal};
use whippet_marketdata::{MdHub, Metrics};

use crate::config::NodeConfig;
use crate::health::{ShardHealth, ShardRole, ShardState};
use crate::ratelimit::RateLimiter;
use crate::snapshot::SnapshotStore;
use crate::storage::StorageMonitor;
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
    /// Bounded-storage guard. A background task refreshes it every five
    /// seconds; readiness and durable admission read the cached state without
    /// doing I/O. `Critical` means a data volume crossed the configured
    /// threshold; `Warn` is alert-only and does not block ingress.
    pub storage: Arc<StorageMonitor>,
    pub snapshots: SnapshotStore,
    pub started: Instant,
    pub shutdown: watch::Sender<bool>,
    /// Legacy hard-fail latch. Order ingress used to gate on this alone; it is
    /// kept as the external `shards_ok` contract and as a second way for a
    /// test to fail the node closed, but readiness is
    /// [`NodeContext::shards_ready`]: the latch **and** the per-shard
    /// lifecycle. The real node starts it `true` and every failure path
    /// clears it through [`NodeContext::mark_shard_failed`].
    pub shards_ok: Arc<AtomicBool>,
    /// Per-shard, per-role lifecycle. Readiness requires every active shard's
    /// engine and MD role to be `Running`.
    pub health: ShardHealth,
    /// Distinct partitions that own a configured symbol, sorted once at
    /// construction. Readiness is scoped to this set: an unused Iggy partition
    /// has no local task and is never required to run.
    pub active_shards: Vec<u32>,
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

    pub fn mark_shard_failed(&self, shard: u32, role: ShardRole) {
        self.health.set(shard, role, ShardState::Failed);
        self.shards_ok.store(false, Ordering::Relaxed);
    }

    /// Apply one lifecycle transition for `(shard, role)` and return the state
    /// it came from. The transition table lives in
    /// [`crate::health::ShardState::can_transition_to`]; a `Running` after
    /// `Failed` is a bug and is rejected (and asserted in debug builds).
    pub fn mark_shard_state(&self, shard: u32, role: ShardRole, state: ShardState) -> ShardState {
        self.health.set(shard, role, state)
    }

    /// Record a clean stop, unless the role already reported a failure.
    ///
    /// A reader task can fail and exit while the rest of the shard winds down
    /// normally; the shard's outer task then returns `Ok`. That must not
    /// overwrite the `Failed` state, or a failed shard would report `Stopped`
    /// (and the health view would lose the reason).
    pub fn mark_shard_stopped(&self, shard: u32, role: ShardRole) {
        if !self.shard_state(shard, role).is_terminal() {
            self.mark_shard_state(shard, role, ShardState::Stopped);
        }
    }

    /// The lifecycle state of one shard role.
    pub fn shard_state(&self, shard: u32, role: ShardRole) -> ShardState {
        self.health.get(shard, role)
    }

    /// True only when the local shard role is actually in its loop.
    pub fn shard_running(&self, shard: u32, role: ShardRole) -> bool {
        self.shard_state(shard, role) == ShardState::Running
    }

    /// The readiness invariant: the hard-fail latch is intact and every active
    /// shard's engine and MD task is `Running`. A `Starting`, `Failed`, or
    /// `Stopped` role keeps the node not-ready.
    pub fn shards_ready(&self) -> bool {
        self.shards_ok.load(Ordering::Relaxed) && self.health.all_running(&self.active_shards)
    }

    /// Per-shard lifecycle for `/api/health`, one entry per active shard.
    pub fn shard_health_snapshot(&self) -> Vec<whippet_core::api::ShardHealthView> {
        self.health.snapshot(&self.active_shards)
    }

    pub fn exchange(&self) -> &whippet_core::config::ExchangeConfig {
        &self.config.exchange
    }

    /// Partitions that actually own a configured symbol. The other partitions
    /// exist in Iggy for scale-out but have no local writer in this process.
    /// Computed once at construction so readiness never allocates or walks the
    /// instrument table on the ingress path.
    pub fn active_shards(&self) -> &[u32] {
        &self.active_shards
    }
}
