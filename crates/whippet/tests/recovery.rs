// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Recovery invariant: a snapshot file plus the journal tail must reconstruct
//! exactly the same state as a full replay from an empty engine.
//!
//! This mirrors what the `verify` CLI reports against real Iggy snapshots, but
//! runs entirely in-process against a temporary directory.

use std::fs;

use whippet::snapshot::{SequencedSnapshot, SnapshotError, SnapshotStore, SnapshotWriter};
use whippet_core::config::{ExchangeConfig, Mode};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
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
    // The matcher hands over the live state hash; the final-snapshot writer
    // rebuilds and verifies it before the atomic rename.
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());
    writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: prefix.snapshot_state(),
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
            snapshot: engine.snapshot_state(),
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

/// Snapshot size that the old off-loop hash failed at. The mismatch showed up
/// on a shard with ~170k applied orders, i.e. after the dedup window turned
/// over, so the proof has to run at that scale.
const SCALE_PAIRS: u64 = 110_000;
/// Commands applied by [`scale_commands`]: two per pair, plus the re-admitted
/// ids.
const SCALE_COMMANDS: u64 = SCALE_PAIRS * 2 + 5_000;

/// SplitMix64: the test needs a reproducible workload, not statistical quality.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// A buy ladder that never crosses: every insert rests, then half of them is
/// cancelled. That keeps the book small while driving the dedup table through
/// a full window turnover, which is where the off-loop hash used to diverge.
fn scale_commands(symbol: Symbol) -> Vec<Command> {
    let mut commands = Vec::with_capacity(SCALE_COMMANDS as usize);
    let mut rng = Rng(0x51ee_d0c5_ed00_0001);
    for pair in 0..SCALE_PAIRS {
        let timestamp_ns = pair * 2;
        let order_id = OrderId(u128::from(pair) + 1);
        let price = Price(40_000 + (rng.next() % 4_000) as i64);
        commands.push(Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            price,
            Qty(1),
            order_id,
            ParticipantId(1),
            timestamp_ns,
        ));
        commands.push(Command::cancel(
            symbol,
            order_id,
            ParticipantId(1),
            timestamp_ns + 1,
        ));
    }
    // Re-admit ids the window has already evicted: this is what makes the
    // insertion-order walk longer than the dedup map.
    for (timestamp_ns, id) in (SCALE_PAIRS * 2..).zip(1..=5_000u64) {
        commands.push(Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(44_000 + (id % 40) as i64 * 100),
            Qty(2),
            OrderId(u128::from(id)),
            ParticipantId(2),
            timestamp_ns,
        ));
    }
    commands
}

/// A >100k-command snapshot must verify when the writer fills the hash off the
/// shard thread, both through the in-process restore and through the real
/// `whippet verify` binary that operators run.
#[test]
fn an_off_loop_hashed_snapshot_at_scale_verifies_through_the_cli() {
    use std::process::Command as ProcessCommand;

    let exchange = ExchangeConfig::default();
    let symbol = whippet_core::ids::Symbol::new("BTC-USD").expect("static symbol");
    let shard = symbol.partition(exchange.partitions);
    let commands = scale_commands(symbol);
    assert_eq!(commands.len() as u64, SCALE_COMMANDS);

    let mut engine = ShardEngine::new(shard, &exchange);
    let mut out = EngineOutput::with_capacity(16, 16);
    for (offset, command) in commands.iter().enumerate() {
        engine
            .process(command, offset as u64, &mut out)
            .expect("apply");
        out.clear();
    }
    let live_hash = engine.state_hash();
    let applied = engine
        .engine(symbol)
        .expect("the shard owns BTC-USD")
        .commands();
    assert_eq!(applied, SCALE_COMMANDS, "every command must be applied");

    // Precondition: the dedup window really did turn over, so the snapshot's
    // insertion-order walk is longer than its map. This is the state the old
    // off-loop hash got wrong.
    let dedup = engine.engine(symbol).expect("BTC-USD").dedup();
    assert!(
        dedup.entries_in_order().len() > dedup.len(),
        "workload did not re-admit an evicted id: walk {} entries, map {} slots",
        dedup.entries_in_order().len(),
        dedup.len()
    );

    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let store = SnapshotStore::new(dir.path());
    let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());
    // The matcher's path: clone only, hashes filled off the shard thread.
    //
    // This test doubles as the fsync half of the stage split: the criterion
    // bench in `whippet-engine/benches/engine.rs` covers the in-memory stages,
    // but only a real file reaches `sync_all`. Run it with
    // `cargo test --release -p whippet --test recovery -- --nocapture` to see
    // the whole split. The printed numbers are advisory; the assertions are
    // the gate.
    let live_hash_started = std::time::Instant::now();
    let _ = engine.state_hash();
    let live_hash_us = live_hash_started.elapsed().as_micros();

    let with_hash_started = std::time::Instant::now();
    let _ = engine.snapshot_state();
    let with_hash_us = with_hash_started.elapsed().as_micros();

    let clone_started = std::time::Instant::now();
    let mut snapshot = engine.snapshot_state_without_hash();
    let clone_us = clone_started.elapsed().as_micros();
    assert!(
        snapshot
            .symbols
            .iter()
            .all(|symbol| symbol.state_hash == [0u8; 32])
    );
    let fill_started = std::time::Instant::now();
    writer
        .fill_hashes(&mut snapshot)
        .expect("fill off the loop");
    let fill_us = fill_started.elapsed().as_micros();
    let write_started = std::time::Instant::now();
    let path = writer
        .write_unverified(SequencedSnapshot {
            sequence: 1,
            snapshot,
        })
        .expect("periodic snapshot writes")
        .expect("newer than nothing");
    let write_us = write_started.elapsed().as_micros();
    assert!(path.exists());
    eprintln!(
        "snapshot stage split @ {SCALE_COMMANDS} commands: live_hash={live_hash_us}us \
         snapshot_with_hash={with_hash_us}us clone_only={clone_us}us \
         off_loop_fill={fill_us}us write_serialize_fsync_rename={write_us}us bytes={}",
        fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0)
    );

    let loaded = store
        .load(shard)
        .expect("snapshot loads")
        .expect("snapshot exists");
    let restored = ShardEngine::from_snapshot(&exchange, loaded).expect("snapshot hash verifies");
    assert_eq!(
        restored.state_hash(),
        live_hash,
        "restored shard diverged from the live engine"
    );

    // The operator path: `whippet verify` loads the file from disk, rebuilds
    // every shard and recomputes the hashes.
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_whippet"))
        .args([
            "verify",
            "--snapshot-dir",
            dir.path().to_str().expect("utf-8 temp path"),
            "--partitions",
            "16",
        ])
        .output()
        .expect("run whippet verify");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "whippet verify failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("all state hashes match"),
        "unexpected verify output: {stdout}"
    );
    assert!(
        stdout.contains(&format!("shard {shard:>2}")),
        "verify did not report shard {shard}: {stdout}"
    );
}
