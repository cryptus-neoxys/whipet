// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Recovery invariant: a snapshot file plus the journal tail must reconstruct
//! exactly the same state as a full replay from an empty engine.
//!
//! This mirrors what the `verify` CLI reports against real Iggy snapshots, but
//! runs entirely in-process against a temporary directory.

use whippet::snapshot::SnapshotStore;
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_testkit::{GoldenLog, scenario_config, standard_shard};

#[test]
fn snapshot_file_seeded_tail_replay_equals_full_replay() {
    let exchange = ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Durable,
        instruments: vec![scenario_config()],
    };
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();
    let mut out = EngineOutput::with_capacity(16, 16);

    let mut full = ShardEngine::new(shard, &exchange);
    for (offset, command) in commands.iter().enumerate() {
        full.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }
    let full_hash = full.state_hash();

    let split = 17;
    let mut prefix = ShardEngine::new(shard, &exchange);
    for (offset, command) in commands.iter().enumerate().take(split) {
        prefix.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }

    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let store = SnapshotStore::new(dir.path());
    store
        .save(&prefix.snapshot_state())
        .expect("snapshot writes atomically");
    let loaded = store
        .load(shard)
        .expect("snapshot loads")
        .expect("snapshot exists");
    let mut restored =
        ShardEngine::from_snapshot(&exchange, loaded).expect("snapshot hash verifies");
    assert_eq!(restored.state_hash(), prefix.state_hash());

    for (offset, command) in commands.iter().enumerate().skip(split) {
        restored.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }
    assert_eq!(
        restored.state_hash(),
        full_hash,
        "snapshot-seeded tail replay diverged from full replay"
    );
}
