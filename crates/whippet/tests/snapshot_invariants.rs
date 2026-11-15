// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Snapshot file invariants from the public API.
//!
//! The node relies on a snapshot being a usable resume point before it becomes
//! a file. These tests exercise the writer and the store through
//! `whippet::snapshot`, so a regression cannot hide behind the inline tests'
//! access to private helpers.

use std::fs;

use whippet::snapshot::{
    SequencedSnapshot, SnapshotError, SnapshotInvariants, SnapshotStore, SnapshotWriter,
};
use whippet_core::config::{ExchangeConfig, InstrumentConfig, Mode};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_engine::{EngineOutput, ShardEngine, ShardSnapshot};

fn exchange() -> ExchangeConfig {
    ExchangeConfig {
        venue: "test".into(),
        partitions: 16,
        mode: Mode::Durable,
        instruments: vec![InstrumentConfig::new("BTC-USD", 100, 40_000, 90_000).unwrap()],
    }
}

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").unwrap()
}

fn shard(exchange: &ExchangeConfig) -> u32 {
    symbol().partition(exchange.partitions)
}

/// One symbol, one resting order at offset 0: the smallest real resume point.
fn live_engine(exchange: &ExchangeConfig) -> ShardEngine {
    let mut engine = ShardEngine::new(shard(exchange), exchange);
    let mut out = EngineOutput::with_capacity(8, 8);
    let order = Command::new_order(
        symbol(),
        Side::Buy,
        OrderType::Limit,
        Price(40_000),
        Qty(1),
        OrderId(1),
        ParticipantId(1),
        1,
    );
    engine.process(&order, 0, &mut out).unwrap();
    engine
}

fn temp_path(store: &SnapshotStore, shard_id: u32) -> std::path::PathBuf {
    store.path(shard_id).with_extension("snapshot.tmp")
}

fn clear_hashes(snapshot: &mut ShardSnapshot) {
    for symbol in &mut snapshot.symbols {
        symbol.state_hash = [0u8; 32];
    }
}

#[test]
fn a_valid_v5_snapshot_round_trips_and_loads() {
    let exchange = exchange();
    let shard_id = shard(&exchange);
    let engine = live_engine(&exchange);
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path());
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());

    let path = writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: engine.snapshot_state(),
        })
        .unwrap()
        .expect("a live snapshot is persisted");
    assert_eq!(path, store.path(shard_id));

    let loaded = store.load(shard_id).unwrap().expect("snapshot exists");
    loaded.check_invariants().unwrap();
    let restored = ShardEngine::from_snapshot(&exchange, loaded).unwrap();
    assert_eq!(restored.state_hash(), engine.state_hash());
    assert_eq!(writer.last_written(), Some(1));
}

#[test]
fn save_rejects_an_unhashed_snapshot_and_writes_no_file() {
    let exchange = exchange();
    let shard_id = shard(&exchange);
    let mut snapshot = live_engine(&exchange).snapshot_state();
    clear_hashes(&mut snapshot);

    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path());
    let error = store.save(&snapshot).unwrap_err();
    assert!(
        matches!(error, SnapshotError::UnhashedSymbol { shard, .. } if shard == shard_id),
        "expected UnhashedSymbol, got {error:?}"
    );
    assert!(!store.path(shard_id).exists());
    assert!(!temp_path(&store, shard_id).exists());
}

#[test]
fn write_and_write_unverified_reject_an_unhashed_snapshot() {
    let exchange = exchange();
    let shard_id = shard(&exchange);
    let mut snapshot = live_engine(&exchange).snapshot_state();
    clear_hashes(&mut snapshot);

    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path());
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange);

    let verified = writer.write(SequencedSnapshot {
        sequence: 1,
        snapshot: snapshot.clone(),
    });
    assert!(matches!(
        verified,
        Err(SnapshotError::UnhashedSymbol { .. })
    ));

    let periodic = writer.write_unverified(SequencedSnapshot {
        sequence: 2,
        snapshot,
    });
    assert!(matches!(
        periodic,
        Err(SnapshotError::UnhashedSymbol { .. })
    ));

    assert!(!store.path(shard_id).exists());
    assert_eq!(writer.last_written(), None);
}

#[test]
fn save_rejects_an_empty_or_commandless_snapshot() {
    let exchange = exchange();
    let shard_id = shard(&exchange);
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path());

    let empty = ShardSnapshot {
        shard_id,
        symbols: Vec::new(),
    };
    assert!(matches!(
        store.save(&empty),
        Err(SnapshotError::EmptySnapshot { .. })
    ));
    assert!(!store.path(shard_id).exists());

    // A fresh engine has real hashes but no offset, so there is nothing to
    // resume after and the file would be misleading.
    let fresh = ShardEngine::new(shard_id, &exchange).snapshot_state();
    assert!(matches!(
        store.save(&fresh),
        Err(SnapshotError::NoResumePoint { .. })
    ));
    assert!(!store.path(shard_id).exists());
    assert!(!temp_path(&store, shard_id).exists());
}

#[test]
fn unsorted_and_duplicate_symbols_are_rejected() {
    let exchange = exchange();
    let live = live_engine(&exchange).snapshot_state();

    let mut duplicate = live.clone();
    duplicate.symbols.push(duplicate.symbols[0].clone());

    let mut unsorted = live;
    let mut lower = unsorted.symbols[0].clone();
    let other = Symbol::new("AAA-USD").unwrap();
    lower.symbol = other;
    lower.book.symbol = other;
    unsorted.symbols.push(lower);

    for snapshot in [duplicate, unsorted] {
        assert!(matches!(
            snapshot.check_invariants(),
            Err(SnapshotError::UnsortedSymbols { .. })
        ));
    }
}

#[test]
fn sequence_is_monotonic_and_a_failed_write_does_not_advance() {
    let exchange = exchange();
    let shard_id = shard(&exchange);
    let engine = live_engine(&exchange);
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path());
    let path = store.path(shard_id);
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange);

    writer
        .write(SequencedSnapshot {
            sequence: 5,
            snapshot: engine.snapshot_state(),
        })
        .unwrap()
        .expect("the first snapshot writes");

    // Stale sequences are dropped, not written over newer state.
    fs::remove_file(&path).unwrap();
    let stale = writer
        .write(SequencedSnapshot {
            sequence: 5,
            snapshot: engine.snapshot_state(),
        })
        .unwrap();
    assert!(stale.is_none());
    assert!(!path.exists(), "a stale sequence recreated the snapshot");

    // A newer sequence reaches disk.
    writer
        .write(SequencedSnapshot {
            sequence: 6,
            snapshot: engine.snapshot_state(),
        })
        .unwrap()
        .expect("the newer snapshot writes");

    // A failing newer write must not advance the guard.
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    let error = writer
        .write(SequencedSnapshot {
            sequence: 7,
            snapshot: engine.snapshot_state(),
        })
        .unwrap_err();
    assert!(matches!(error, SnapshotError::Io(_)), "got {error:?}");
    assert_eq!(writer.last_written(), Some(6));
    assert!(!temp_path(&store, shard_id).exists());
}

#[test]
fn a_v4_file_is_rejected_as_bad_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = SnapshotStore::new(dir.path());
    let path = store.path(3);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = fs::File::create(&path).unwrap();
    use std::io::Write;
    file.write_all(b"WHIPSNP1").unwrap();
    file.write_all(&4u32.to_le_bytes()).unwrap();
    file.write_all(&[0u8; 8]).unwrap();
    drop(file);

    match store.load(3) {
        Err(SnapshotError::BadVersion(4)) => {}
        other => panic!("expected BadVersion(4), got {other:?}"),
    }
}
