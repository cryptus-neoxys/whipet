// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Retention-plan proofs.
//!
//! `whippet retention-plan` is the read-only half of ADR 0014: it computes the
//! oldest order-log offset that is safe to delete per partition and must never
//! delete, trim, commit, or append. These tests pin the verdicts on the pure
//! policy, the CLI defaults, and the live plan against a memory journal seeded
//! with records and a snapshot written by `SnapshotWriter`.

use std::path::{Path, PathBuf};

use clap::Parser;
use whippet::cli::{RETENTION_PLAN_NOTICE, RetentionPlanArgs, retention_plan_with_journal};
use whippet::snapshot::{SequencedSnapshot, SnapshotStore, SnapshotWriter};
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{Command, OrderType, Side};
use whippet_engine::{EngineOutput, ShardEngine};
use whippet_journal::{Journal, JournalMessage, MemoryJournal, Topic};

/// A four-partition exchange keeps the plan cheap while still exercising the
/// per-shard loop and the symbol routing.
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

fn plan_args(dir: &Path, safety_margin_records: u64, json: bool) -> RetentionPlanArgs {
    RetentionPlanArgs {
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        iggy_password_file: None,
        snapshot_dir: dir.to_path_buf(),
        partitions: 4,
        symbols: None,
        safety_margin_records,
        json,
    }
}

/// Seed a memory journal on the orders partition, commit the engine consumer
/// through `committed`, and record the MD topic offset as a diagnostic.
fn seeded_journal(partition: u32, records: usize, committed: u64) -> Journal {
    let memory = MemoryJournal::new();
    let messages: Vec<JournalMessage> = orders(btc(), records as u64)
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
    memory.commit(
        Topic::Md,
        partition,
        &format!("whippet-md-p{partition}"),
        committed,
    );
    Journal::Memory(memory)
}

fn row_for(
    rows: &[whippet::cli::RetentionPlanRow],
    partition: u32,
) -> &whippet::cli::RetentionPlanRow {
    rows.iter()
        .find(|row| row.partition == partition)
        .unwrap_or_else(|| panic!("no plan row for partition {partition}: {rows:?}"))
}

#[test]
fn retention_plan_args_default_to_a_read_only_dry_run() {
    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        plan: RetentionPlanArgs,
    }

    let cli = TestCli::parse_from(["test"]);
    assert_eq!(cli.plan.iggy_addr, "127.0.0.1:8090");
    assert_eq!(cli.plan.iggy_user, "iggy");
    assert_eq!(cli.plan.snapshot_dir, PathBuf::from("data/snapshots"));
    assert_eq!(cli.plan.partitions, 16);
    assert_eq!(cli.plan.symbols, None);
    assert_eq!(cli.plan.safety_margin_records, 10_000);
    assert!(!cli.plan.json);
    assert!(cli.plan.iggy_password_file.is_none());

    let cli = TestCli::parse_from([
        "test",
        "--snapshot-dir",
        "/tmp/snapshots",
        "--partitions",
        "4",
        "--symbols",
        "BTC-USD,ETH-USD",
        "--safety-margin-records",
        "0",
        "--json",
        "--iggy-addr",
        "127.0.0.1:9999",
        "--iggy-password-file",
        "/tmp/iggy-password",
    ]);
    assert_eq!(cli.plan.snapshot_dir, PathBuf::from("/tmp/snapshots"));
    assert_eq!(cli.plan.partitions, 4);
    assert_eq!(cli.plan.symbols.as_deref(), Some("BTC-USD,ETH-USD"));
    assert_eq!(cli.plan.safety_margin_records, 0);
    assert!(cli.plan.json);
    assert_eq!(cli.plan.iggy_addr, "127.0.0.1:9999");
    assert_eq!(
        cli.plan.iggy_password_file,
        Some(PathBuf::from("/tmp/iggy-password"))
    );
}

#[tokio::test]
async fn an_eligible_plan_uses_the_snapshot_boundary_and_changes_nothing() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let symbol = btc();
    let partition = write_snapshot(dir.path(), &exchange, symbol, 10);
    // 20 orders at offsets 0..=19; the snapshot is a verified baseline through
    // offset 9 and the engine consumer is committed through the head. The MD
    // offset is reported as a diagnostic but does not set the boundary.
    let journal = seeded_journal(partition, 20, 19);

    let before = journal
        .read_payloads(Topic::Orders, partition, "retention-test", 0, 1_000, 0)
        .await
        .expect("read the log")
        .len();
    assert_eq!(before, 20);

    let rows = retention_plan_with_journal(plan_args(dir.path(), 0, false), &journal)
        .await
        .expect("a readable journal and snapshot produce a plan");

    let row = row_for(&rows, partition);
    assert_eq!(row.head, 19);
    assert_eq!(row.engine_committed, Some(19));
    assert_eq!(row.md_topic_committed, Some(19));
    assert_eq!(row.snapshot_offset, Some(9));
    assert_eq!(row.decision, "eligible");
    assert_eq!(row.safe_through, Some(9));
    assert!(
        row.reason.contains("boundary 9") && row.reason.contains("verified snapshot"),
        "unexpected reason: {}",
        row.reason
    );
    assert_eq!(row.action, "none");
    assert_eq!(row.notice, RETENTION_PLAN_NOTICE);

    // A shard with no configured symbol has no consumers and no snapshot, so
    // its plan is blocked rather than silently "nothing to do".
    for other in rows.iter().filter(|row| row.partition != partition) {
        assert_eq!(other.decision, "blocked");
        assert!(
            other.reason.contains("no committed consumer"),
            "unexpected reason for shard {}: {}",
            other.partition,
            other.reason
        );
    }

    // Read-only proof: the log still holds every record and both committed
    // offsets are exactly where they were.
    let after = journal
        .read_payloads(Topic::Orders, partition, "retention-test", 0, 1_000, 0)
        .await
        .expect("read the log again");
    assert_eq!(after.len(), before, "the planner changed the log length");
    assert_eq!(
        journal
            .committed(
                Topic::Orders,
                partition,
                &format!("whippet-engine-p{partition}")
            )
            .await
            .expect("read engine commit"),
        Some(19),
        "the planner moved the engine committed offset"
    );
    assert_eq!(
        journal
            .committed(Topic::Md, partition, &format!("whippet-md-p{partition}"))
            .await
            .expect("read md commit"),
        Some(19),
        "the planner moved the MD committed offset"
    );
    assert_eq!(
        journal
            .committed(
                Topic::Orders,
                partition,
                &format!("whippet-retention-p{partition}")
            )
            .await
            .expect("read the planner consumer"),
        None,
        "the planner created a consumer offset"
    );
}

#[tokio::test]
async fn a_missing_snapshot_blocks_every_shard() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = btc().partition(exchange.partitions);
    let journal = seeded_journal(partition, 20, 19);

    let rows = retention_plan_with_journal(plan_args(dir.path(), 0, false), &journal)
        .await
        .expect("a missing snapshot is a blocked plan, not an I/O failure");

    let row = row_for(&rows, partition);
    assert_eq!(row.decision, "blocked");
    assert_eq!(row.snapshot_offset, None);
    assert_eq!(row.safe_through, None);
    assert!(
        row.reason.contains("no verified snapshot"),
        "unexpected reason: {}",
        row.reason
    );
}

#[tokio::test]
async fn a_missing_md_commit_does_not_constrain_the_orders_boundary() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = write_snapshot(dir.path(), &exchange, btc(), 10);

    // Only the engine consumer has committed. The MD consumer commits on the
    // `md` topic, whose offsets are a different space: its missing watermark is
    // a diagnostic gap, not an orders-log boundary.
    let memory = MemoryJournal::new();
    let messages: Vec<JournalMessage> = orders(btc(), 20)
        .iter()
        .map(|command| JournalMessage::new(command.encode(), Vec::new()))
        .collect();
    memory.append(Topic::Orders, partition, &messages);
    memory.commit(
        Topic::Orders,
        partition,
        &format!("whippet-engine-p{partition}"),
        19,
    );
    let journal = Journal::Memory(memory);

    let rows = retention_plan_with_journal(plan_args(dir.path(), 0, false), &journal)
        .await
        .expect("an unknown MD offset is not an I/O failure");

    let row = row_for(&rows, partition);
    assert_eq!(row.decision, "eligible");
    assert_eq!(row.engine_committed, Some(19));
    assert_eq!(row.md_topic_committed, None);
    assert_eq!(row.safe_through, Some(9));
    assert!(
        row.reason.contains("boundary 9") && row.reason.contains("verified snapshot"),
        "the orders boundary must come from the orders proof: {}",
        row.reason
    );
    assert!(
        !row.reason.contains("md"),
        "the MD topic must not appear in the orders boundary: {}",
        row.reason
    );
}

#[tokio::test]
async fn an_md_offset_behind_the_engine_does_not_block_deletion() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = write_snapshot(dir.path(), &exchange, btc(), 10);

    // The MD topic offset is far behind the orders head because MD records
    // grow faster than orders. Treating it as an orders boundary would retain
    // the log forever; it is a diagnostic column only.
    let memory = MemoryJournal::new();
    let messages: Vec<JournalMessage> = orders(btc(), 20)
        .iter()
        .map(|command| JournalMessage::new(command.encode(), Vec::new()))
        .collect();
    memory.append(Topic::Orders, partition, &messages);
    memory.commit(
        Topic::Orders,
        partition,
        &format!("whippet-engine-p{partition}"),
        19,
    );
    memory.commit(Topic::Md, partition, &format!("whippet-md-p{partition}"), 0);
    let journal = Journal::Memory(memory);

    let rows = retention_plan_with_journal(plan_args(dir.path(), 0, false), &journal)
        .await
        .expect("a lagging MD offset is not an I/O failure");

    let row = row_for(&rows, partition);
    assert_eq!(row.decision, "eligible");
    assert_eq!(row.safe_through, Some(9));
    assert_eq!(row.md_topic_committed, Some(0));
}

#[tokio::test]
async fn a_missing_engine_commit_blocks_even_when_md_has_committed() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = write_snapshot(dir.path(), &exchange, btc(), 10);

    // An MD commit says nothing about the orders log, so without the engine
    // watermark the plan cannot prove any orders range.
    let memory = MemoryJournal::new();
    let messages: Vec<JournalMessage> = orders(btc(), 20)
        .iter()
        .map(|command| JournalMessage::new(command.encode(), Vec::new()))
        .collect();
    memory.append(Topic::Orders, partition, &messages);
    memory.commit(
        Topic::Md,
        partition,
        &format!("whippet-md-p{partition}"),
        19,
    );
    let journal = Journal::Memory(memory);

    let rows = retention_plan_with_journal(plan_args(dir.path(), 0, false), &journal)
        .await
        .expect("a missing engine offset is a blocked plan, not an I/O failure");

    let row = row_for(&rows, partition);
    assert_eq!(row.decision, "blocked");
    assert_eq!(row.engine_committed, None);
    assert_eq!(row.md_topic_committed, Some(19));
    assert_eq!(row.safe_through, None);
    assert!(
        row.reason.contains("no committed consumer")
            && row.reason.contains(&format!("whippet-engine-p{partition}")),
        "the reason must name the consumer with no proof: {}",
        row.reason
    );
}

#[tokio::test]
async fn the_safety_margin_can_block_an_otherwise_eligible_plan() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = write_snapshot(dir.path(), &exchange, btc(), 10);
    let journal = seeded_journal(partition, 20, 19);

    // The boundary is 9; a 10-record margin leaves nothing to delete.
    let rows = retention_plan_with_journal(plan_args(dir.path(), 10, false), &journal)
        .await
        .expect("a blocked plan is a valid result");

    let row = row_for(&rows, partition);
    assert_eq!(row.decision, "blocked");
    assert_eq!(row.safe_through, None);
    assert!(
        row.reason.contains("safety margin consumes the window")
            && row.reason.contains("boundary 9"),
        "unexpected reason: {}",
        row.reason
    );
    assert!(row.head > 9, "the head still proves records exist");
}

#[tokio::test]
async fn a_corrupt_snapshot_is_an_error_instead_of_a_boundary() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = write_snapshot(dir.path(), &exchange, btc(), 10);
    let journal = seeded_journal(partition, 20, 19);

    // Destroy the magic so the snapshot cannot be decoded. The planner must
    // fail rather than fall back to a boundary it cannot verify.
    let store = SnapshotStore::new(dir.path());
    std::fs::write(store.path(partition), b"not a whippet snapshot").expect("corrupt the snapshot");

    let error = retention_plan_with_journal(plan_args(dir.path(), 0, false), &journal)
        .await
        .expect_err("a corrupt snapshot must fail the plan");
    let message = format!("{error:#}");
    assert!(
        message.contains("snapshot"),
        "the failure did not name the snapshot: {message}"
    );
}

#[tokio::test]
async fn the_json_plan_carries_the_read_only_action() {
    let exchange = four_partition_exchange();
    let dir = tempfile::tempdir().expect("temp snapshot dir");
    let partition = write_snapshot(dir.path(), &exchange, btc(), 10);
    let journal = seeded_journal(partition, 20, 19);

    let rows = retention_plan_with_journal(plan_args(dir.path(), 0, true), &journal)
        .await
        .expect("the JSON plan runs");

    let value = serde_json::to_value(row_for(&rows, partition)).expect("row serializes");
    assert_eq!(value["action"], "none");
    assert_eq!(value["notice"], RETENTION_PLAN_NOTICE);
    assert_eq!(value["decision"], "eligible");
    assert_eq!(value["safe_through"], 9);
    assert_eq!(value["head"], 19);
}
