// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Recovery invariant: a snapshot file plus the journal tail must reconstruct
//! exactly the same state as a full replay from an empty engine.
//!
//! This mirrors what the `verify` CLI reports against real Iggy snapshots, but
//! runs entirely in-process against a temporary directory.

use std::fs;

use whippet::snapshot::{SequencedSnapshot, SnapshotError, SnapshotStore, SnapshotWriter};
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_testkit::{GoldenLog, scenario_config, standard_shard};

fn test_exchange() -> ExchangeConfig {
    ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Durable,
        instruments: vec![scenario_config()],
    }
}

#[test]
fn snapshot_file_seeded_tail_replay_equals_full_replay() {
    let exchange = test_exchange();
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
    // The shard hands over an unhashed snapshot; the writer fills and verifies
    // the hashes before the atomic rename.
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());
    writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: prefix.snapshot_state_without_hash(),
        })
        .expect("snapshot verifies")
        .expect("snapshot writes atomically");

    let loaded = store
        .load(shard)
        .expect("snapshot loads")
        .expect("snapshot exists");
    assert!(
        loaded
            .symbols
            .iter()
            .all(|symbol| symbol.state_hash != [0u8; 32]),
        "the writer persisted an unhashed symbol"
    );
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

#[test]
fn corrupt_hash_is_rejected_and_previous_snapshot_survives() {
    let exchange = test_exchange();
    let shard = standard_shard(&exchange);
    let commands = GoldenLog::standard().commands().to_vec();
    let mut out = EngineOutput::with_capacity(16, 16);

    let mut engine = ShardEngine::new(shard, &exchange);
    for (offset, command) in commands.iter().enumerate().take(17) {
        engine.process(command, offset as u64, &mut out).unwrap();
        out.clear();
    }
    let good_hash = engine.state_hash();

    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let store = SnapshotStore::new(dir.path());
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());
    let path = writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: engine.snapshot_state_without_hash(),
        })
        .expect("snapshot verifies")
        .expect("snapshot writes");
    let on_disk = fs::read(&path).expect("read written snapshot");

    // A snapshot whose stored hash disagrees with its contents must never
    // become the resume point, and must not replace the previous file.
    let mut corrupted = engine.snapshot_state();
    corrupted.symbols[0].state_hash[0] ^= 0xff;
    match writer.write(SequencedSnapshot {
        sequence: 2,
        snapshot: corrupted,
    }) {
        Err(SnapshotError::HashMismatch { shard: found, .. }) => assert_eq!(found, shard),
        other => panic!("expected HashMismatch, got {other:?}"),
    }
    assert_eq!(
        fs::read(&path).expect("read snapshot after rejection"),
        on_disk,
        "the rejected snapshot rewrote the file"
    );
    assert_eq!(writer.last_written(), Some(1));

    let restored = ShardEngine::from_snapshot(
        &exchange,
        store
            .load(shard)
            .expect("snapshot loads")
            .expect("snapshot still exists"),
    )
    .expect("the previous snapshot is still valid");
    assert_eq!(restored.state_hash(), good_hash);
}
