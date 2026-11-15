// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Fail-closed proof for the operator commands.
//!
//! `verify` and `replay` are only useful when a missing or stale snapshot is a
//! failure instead of a quiet "nothing to check". These tests pin the exit
//! semantics: the flag exits non-zero, the lenient path still reports that it
//! verified nothing, `--partitions` selects the routing the snapshots were
//! written with, and a snapshot ahead of the engine's committed offset is
//! rejected before a single tail command is applied.

use std::path::Path;
use std::process::Command as ProcessCommand;

use whippet::cli::{ReplayArgs, replay_with_journal};
use whippet::snapshot::{SequencedSnapshot, SnapshotStore, SnapshotWriter};
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{Journal, JournalMessage, MemoryJournal, Topic};

/// A small exchange so the snapshot files and the test run stay cheap. The
/// partition count also exercises `verify --partitions`, which used to verify
/// every file against the 16-partition default.
fn four_partition_exchange() -> ExchangeConfig {
    ExchangeConfig {
        partitions: 4,
        ..ExchangeConfig::default()
    }
}

fn btc() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

/// Resting buy orders at one price: nothing crosses, so the book stays small
/// while every command still advances `last_offset`.
fn orders(symbol: Symbol, count: u64) -> Vec<Command> {
    (0..count)
        .map(|index| {
            Command::new_order(
                symbol,
                Side::Buy,
                OrderType::Limit,
                Price(40_000),
                Qty(1),
                OrderId(u128::from(index) + 1),
                ParticipantId(1),
                index,
            )
        })
        .collect()
}

/// Apply the first `applied` commands and persist a verified snapshot for the
/// symbol's shard. Returns the shard id.
fn write_snapshot(dir: &Path, exchange: &ExchangeConfig, symbol: Symbol, applied: u64) -> u32 {
    let shard = symbol.partition(exchange.partitions);
    let mut engine = ShardEngine::new(shard, exchange);
    let mut out = EngineOutput::with_capacity(8, 8);
    for (offset, command) in orders(symbol, applied).iter().enumerate() {
        engine
            .process(command, offset as u64, &mut out)
            .expect("apply command");
        out.clear();
    }
    assert!(
        engine.state_hash() != [0u8; 32],
        "the test snapshot must carry a real hash"
    );

    let store = SnapshotStore::new(dir);
    let mut writer = SnapshotWriter::with_exchange(store, exchange.clone());
    writer
        .write(SequencedSnapshot {
            sequence: 1,
            snapshot: engine.snapshot_state(),
        })
        .expect("snapshot verifies")
        .expect("snapshot writes atomically");
    shard
}

fn replay_args(dir: &Path, partition: u32, require_snapshot: bool) -> ReplayArgs {
    ReplayArgs {
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        partition,
        from: None,
        to: None,
        snapshot_dir: dir.to_path_buf(),
        exchange_partitions: 4,
        require_snapshot,
        expect_hash: None,
    }
}

fn verify_binary(dir: &Path, partitions: u32, require_snapshot: bool) -> std::process::Output {
    let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_whippet"));
    command.args([
        "verify",
        "--snapshot-dir",
        dir.to_str().expect("utf-8 temp path"),
        "--partitions",
        &partitions.to_string(),
    ]);
    if require_snapshot {
        command.arg("--require-snapshot");
    }
    command.output().expect("run whippet verify")
}

fn memory_journal(partition: u32, commands: &[Command], committed: u64) -> Journal {
    let memory = MemoryJournal::new();
    let messages: Vec<JournalMessage> = commands
        .iter()
        .map(|command| JournalMessage::new(command.encode(), Vec::new()))
        .collect();
    memory.append(Topic::Orders, partition, &messages);
    memory.commit(
        Topic::Orders,
        partition,
        &format!("whippet-engine-p{partition}"),
        committed,
    );
    Journal::Memory(memory)
}

#[test]
fn verify_require_snapshot_fails_on_an_empty_directory() {
    let dir = tempfile::tempdir().expect("temp snapshot dir");

    let lenient = verify_binary(dir.path(), 4, false);
    let stdout = String::from_utf8_lossy(&lenient.stdout);
    assert!(
        lenient.status.success(),
        "the lenient dev path must still succeed: {}\n{stdout}",
        String::from_utf8_lossy(&lenient.stderr)
    );
    assert!(
        stdout.contains("no snapshots") && stdout.contains("nothing verified"),
        "the lenient path did not state that it verified nothing: {stdout}"
    );

    let strict = verify_binary(dir.path(), 4, true);
    assert!(
        !strict.status.success(),
        "verify --require-snapshot passed on an empty snapshot directory"
    );
    let stderr = String::from_utf8_lossy(&strict.stderr);
    assert!(
        stderr.contains("no snapshots") && stderr.contains(dir.path().to_str().unwrap()),
        "the failure did not name the empty snapshot directory: {stderr}"
    );
}

#[test]
fn verify_with_matching_partitions_accepts_a_four_partition_snapshot() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let mut shards = Vec::new();
    for instrument in &exchange.instruments {
        shards.push(write_snapshot(dir.path(), &exchange, instrument.symbol, 5));
    }
    shards.sort_unstable();
    shards.dedup();

    let output = verify_binary(dir.path(), 4, true);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "verify --partitions 4 failed: {}\n{stdout}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("all state hashes match"),
        "unexpected verify output: {stdout}"
    );
    for shard in shards {
        assert!(
            stdout.contains(&format!("shard {shard:>2}")),
            "verify did not report shard {shard}: {stdout}"
        );
    }
}

#[tokio::test]
async fn replay_require_snapshot_rejects_a_snapshot_with_no_applied_commands() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let symbol = btc();
    let partition = symbol.partition(exchange.partitions);

    // The node's resume rule: a snapshot from a shard that never applied a
    // command is not a resume point, so it must not satisfy --require-snapshot.
    write_snapshot(dir.path(), &exchange, symbol, 0);
    let journal = memory_journal(partition, &orders(symbol, 6), 5);

    let error = replay_with_journal(replay_args(dir.path(), partition, true), journal)
        .await
        .expect_err("a snapshot with no applied commands must fail closed");
    let message = error.to_string();
    assert!(
        message.contains("no usable snapshot") && message.contains("no applied commands"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn replay_require_snapshot_fails_when_the_snapshot_is_ahead_of_the_log_head() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let symbol = btc();
    let partition = symbol.partition(exchange.partitions);

    // The snapshot claims 20 applied commands, but the log head is offset 5.
    // That cannot be a resume point even though the committed offset is lower.
    let shard = write_snapshot(dir.path(), &exchange, symbol, 20);
    assert_eq!(shard, partition);
    let journal = memory_journal(partition, &orders(symbol, 6), 5);

    let args = ReplayArgs {
        to: Some(5),
        require_snapshot: true,
        ..replay_args(dir.path(), partition, true)
    };
    let error = replay_with_journal(args, journal)
        .await
        .expect_err("a snapshot ahead of the log head must fail closed");
    let message = error.to_string();
    assert!(
        message.contains("ahead of the log head"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn replay_require_snapshot_reports_head_and_tail_and_matches_a_full_replay() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let symbol = btc();
    let partition = symbol.partition(exchange.partitions);
    let commands = orders(symbol, 20);

    // The snapshot stops after 10 applied commands; the log has 20 records and
    // the engine has committed through the head, so the tail is 10 records.
    write_snapshot(dir.path(), &exchange, symbol, 10);

    let seeded_args = ReplayArgs {
        to: Some(19),
        ..replay_args(dir.path(), partition, true)
    };
    let seeded = replay_with_journal(seeded_args, memory_journal(partition, &commands, 19))
        .await
        .expect("a snapshot behind the committed offset is a valid resume point");

    assert_eq!(seeded["seeded_from_snapshot"], true);
    assert_eq!(seeded["snapshot_offset"], 9);
    assert_eq!(seeded["committed_offset"], 19);
    assert_eq!(seeded["head"], 19);
    assert_eq!(seeded["tail"], 10);
    assert_eq!(seeded["decoded"], 10);
    assert_eq!(seeded["last_offset"], 19);

    // The same proof scripts/replay.sh makes: the snapshot-seeded tail must
    // reach the same state hash as a full replay from zero.
    let full_args = ReplayArgs {
        from: Some(0),
        to: Some(19),
        ..replay_args(dir.path(), partition, false)
    };
    let full = replay_with_journal(full_args, memory_journal(partition, &commands, 19))
        .await
        .expect("full replay runs");

    assert_eq!(full["seeded_from_snapshot"], false);
    assert_eq!(
        full["state_hash"], seeded["state_hash"],
        "snapshot-seeded replay diverged from the full replay"
    );
    assert_eq!(full["last_offset"], seeded["last_offset"]);
}
