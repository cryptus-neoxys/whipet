// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Shared integration-test harness for the `whippet` crate.
//!
//! `NodeContext` and `NodeConfig` live in this crate, above `whippet-testkit`
//! in the dependency graph. Putting the helper here avoids a dependency cycle
//! and lets every integration test build the real gateway router against a
//! memory journal. No Tokio runtime is spawned here; tests decide when to run
//! the engine directly or start a server.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use tempfile::TempDir;
use tokio::sync::watch;
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::Symbol;
use whippet_core::protocol::{Command, ExecutionReport, MdEvent};
use whippet_engine::{EngineOutput, SymbolEngine};
use whippet_journal::{Journal, JournalRecord, MemoryJournal, Topic};
use whippet_marketdata::MdHub;

use whippet::config::{JournalKind, NodeConfig};
use whippet::context::NodeContext;
use whippet::snapshot::SnapshotStore;

/// A node wired entirely in memory: memory journal, market-data hub, temp
/// snapshot directory, and a shutdown channel that nothing listens to until a
/// test subscribes.
pub struct TestNode {
    pub ctx: Arc<NodeContext>,
    /// Kept alive for the lifetime of the node so the snapshot directory and
    /// dashboard path stay valid.
    pub tempdir: TempDir,
}

impl TestNode {
    pub fn new() -> Self {
        let tempdir = tempfile::tempdir().expect("temp dir");
        let exchange = ExchangeConfig::default();
        let snapshot_dir = tempdir.path().join("snapshots");
        let config = NodeConfig {
            http_addr: "127.0.0.1:0".parse().expect("valid test address"),
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
            rate_limit_per_sec: 0,
        };
        let journal = Arc::new(Journal::memory());
        let hub = MdHub::new(
            config.exchange.clone(),
            config.iggy_addr.clone(),
            "memory".into(),
            Some("test".into()),
        );
        let metrics = hub.metrics();
        let (shutdown, _) = watch::channel(false);
        let ctx = Arc::new(NodeContext {
            config,
            journal,
            hub,
            metrics,
            snapshots: SnapshotStore::new(snapshot_dir),
            started: Instant::now(),
            shutdown,
            shards_ok: Arc::new(AtomicBool::new(true)),
        });
        Self { ctx, tempdir }
    }

    /// The memory journal behind the node's `Journal` handle.
    pub fn memory(&self) -> &MemoryJournal {
        match &*self.ctx.journal {
            Journal::Memory(journal) => journal,
            Journal::Iggy(_) => panic!("TestNode must use the memory journal"),
        }
    }

    /// Every order-topic record for `symbol`, in partition order.
    pub fn orders(&self, symbol: Symbol) -> Vec<JournalRecord> {
        let partition = symbol.partition(self.ctx.exchange().partitions);
        self.memory().read(Topic::Orders, partition, 0, 10_000)
    }

    /// Decode every order-topic record for `symbol`.
    pub fn commands(&self, symbol: Symbol) -> Vec<Command> {
        self.orders(symbol)
            .into_iter()
            .map(|record| Command::decode(&record.payload).expect("valid command payload"))
            .collect()
    }

    /// Process the journaled orders for `symbol` through a fresh engine, in
    /// offset order. This is the honest way to assert engine-level effects
    /// without starting `run_engine_shard` and its snapshot/poll loop.
    pub fn process_orders(&self, symbol: Symbol) -> ProcessedOrders {
        let config = self
            .ctx
            .exchange()
            .instrument(symbol)
            .expect("symbol is configured")
            .clone();
        let mut engine = SymbolEngine::new(config);
        let mut out = EngineOutput::default();
        let mut executions = Vec::new();
        let mut md = Vec::new();
        for record in self.orders(symbol) {
            let command = Command::decode(&record.payload).expect("valid command payload");
            engine.process(&command, record.offset, &mut out);
            executions.extend(out.executions.iter().cloned());
            md.extend(out.md.iter().cloned());
            out.clear();
        }
        ProcessedOrders {
            engine,
            executions,
            md,
        }
    }
}

/// A fresh engine plus the outputs produced while processing a journal tail.
pub struct ProcessedOrders {
    pub engine: SymbolEngine,
    pub executions: Vec<ExecutionReport>,
    pub md: Vec<MdEvent>,
}

impl Default for TestNode {
    fn default() -> Self {
        Self::new()
    }
}
