// Licensed under the Apache License, Version 2.0. See LICENSE.

//! `whippet positions` proofs.
//!
//! The positions command reconstructs per-participant, per-symbol positions and
//! cash flow from the orders and executions topics through the `Journal` seam.
//! These tests pin the two-leg attribution (aggressor and resting maker), the
//! cash signs, the unresolved-maker accounting, the symbol and participant
//! filters, the JSON shape, the CLI defaults, and — most importantly — that the
//! command is read-only: no consumer offset moves and no record is appended.

use clap::Parser;
use whippet::cli::{PositionsArgs, positions_with_journal};
use whippet::positions::{PositionReport, build_positions};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{Command, ExecKind, ExecutionReport, OrderType, RejectReason, Side};
use whippet_journal::{Journal, JournalMessage, Topic};

const PARTITION: u32 = 1;

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

fn eth() -> Symbol {
    Symbol::new("ETH-USD").expect("static symbol")
}

fn positions_args(partition: u32) -> PositionsArgs {
    PositionsArgs {
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        iggy_password_file: None,
        partition,
        symbols: Vec::new(),
        participant: None,
        json: false,
    }
}

async fn append(journal: &Journal, topic: Topic, payloads: &[Vec<u8>]) {
    let messages: Vec<JournalMessage> = payloads
        .iter()
        .map(|payload| JournalMessage::new(payload.clone(), Vec::new()))
        .collect();
    journal
        .append(topic, PARTITION, &messages)
        .await
        .expect("append to the memory journal");
}

async fn topic_len(journal: &Journal, topic: Topic) -> usize {
    journal
        .read_payloads(topic, PARTITION, "positions-test", 0, 1_000, 0)
        .await
        .expect("read the memory journal")
        .len()
}

fn new_order(
    symbol: Symbol,
    id: u128,
    participant: u64,
    side: Side,
    price: i64,
    qty: u64,
) -> Command {
    Command::new_order(
        symbol,
        side,
        OrderType::Limit,
        Price(price),
        Qty(qty),
        OrderId(id),
        ParticipantId(participant),
        1,
    )
}

#[allow(clippy::too_many_arguments)]
fn fill(
    seq: u64,
    symbol: Symbol,
    order_id: u128,
    maker_order_id: u128,
    side: Side,
    price: i64,
    qty: u64,
    kind: ExecKind,
) -> ExecutionReport {
    ExecutionReport {
        kind,
        symbol,
        side,
        order_type: OrderType::Limit,
        price: Price(price),
        qty: Qty(qty),
        remaining: Qty(0),
        order_id: OrderId(order_id),
        maker_order_id: OrderId(maker_order_id),
        seq: Seq(seq),
        cause_offset: seq,
        timestamp_ns: seq,
        reason: RejectReason::None,
        flags: 0,
    }
}

/// Participant 7 rests a buy for 5 at 100. Participant 8 sells 3 into it.
fn maker_rules_fixture() -> (Vec<Command>, Vec<ExecutionReport>) {
    let orders = vec![
        new_order(symbol(), 1, 7, Side::Buy, 100, 5),
        new_order(symbol(), 2, 8, Side::Sell, 100, 3),
    ];
    let executions = vec![fill(
        0,
        symbol(),
        2,
        1,
        Side::Sell,
        100,
        3,
        ExecKind::Filled,
    )];
    (orders, executions)
}

#[tokio::test]
async fn positions_reconstruct_both_legs_and_are_read_only() {
    let journal = Journal::memory();
    let (orders, executions) = maker_rules_fixture();
    let order_payloads: Vec<Vec<u8>> = orders.iter().map(Command::encode).collect();
    let execution_payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Orders, &order_payloads).await;
    append(&journal, Topic::Executions, &execution_payloads).await;
    let orders_before = topic_len(&journal, Topic::Orders).await;
    let executions_before = topic_len(&journal, Topic::Executions).await;

    let report = positions_with_journal(positions_args(PARTITION), &journal)
        .await
        .expect("a readable journal produces a report");

    assert_eq!(report.partition, PARTITION);
    assert_eq!(report.symbols, vec![symbol()]);
    assert_eq!(report.unresolved_maker_fills, 0);
    assert_eq!(report.unresolved_aggressor_fills, 0);
    assert_eq!(
        report
            .participants
            .iter()
            .map(|position| position.participant)
            .collect::<Vec<_>>(),
        vec![ParticipantId(7), ParticipantId(8)],
        "participants are sorted by id"
    );

    // The maker's buy is a +3 position paid for with cash; the aggressor's
    // sell is the mirror image.
    let maker = &report.participants[0];
    assert_eq!(maker.symbols.len(), 1);
    assert_eq!(maker.symbols[0].symbol, symbol());
    assert_eq!(maker.net_qty, 3);
    assert_eq!(maker.buy_qty, 3);
    assert_eq!(maker.sell_qty, 0);
    assert_eq!(maker.fills, 1);
    assert_eq!(maker.cash_cents, -300);
    assert_eq!(maker.notional_cents, 300);

    let aggressor = &report.participants[1];
    assert_eq!(aggressor.net_qty, -3);
    assert_eq!(aggressor.buy_qty, 0);
    assert_eq!(aggressor.sell_qty, 3);
    assert_eq!(aggressor.fills, 1);
    assert_eq!(aggressor.cash_cents, 300);
    assert_eq!(aggressor.notional_cents, 300);

    // Read-only: both topics keep every record and the positions consumers
    // never commit.
    assert_eq!(topic_len(&journal, Topic::Orders).await, orders_before);
    assert_eq!(
        topic_len(&journal, Topic::Executions).await,
        executions_before
    );
    for (topic, consumer) in [
        (Topic::Orders, "whippet-positions-orders-p1"),
        (Topic::Executions, "whippet-positions-executions-p1"),
    ] {
        assert_eq!(
            journal
                .committed(topic, PARTITION, consumer)
                .await
                .expect("read positions consumer"),
            None,
            "positions moved a consumer offset on {topic}"
        );
    }
}

#[tokio::test]
async fn positions_symbol_and_participant_filters_select_rows() {
    let journal = Journal::memory();
    let orders = [
        new_order(symbol(), 1, 7, Side::Buy, 100, 2),
        new_order(eth(), 2, 8, Side::Buy, 50, 4),
    ];
    let executions = [
        fill(0, symbol(), 1, 0, Side::Buy, 100, 2, ExecKind::Filled),
        fill(1, eth(), 2, 0, Side::Buy, 50, 4, ExecKind::Filled),
    ];
    let order_payloads: Vec<Vec<u8>> = orders.iter().map(Command::encode).collect();
    let execution_payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Orders, &order_payloads).await;
    append(&journal, Topic::Executions, &execution_payloads).await;

    let mut args = positions_args(PARTITION);
    args.symbols = vec![eth()];
    let report = positions_with_journal(args, &journal)
        .await
        .expect("the filtered scan succeeds");
    assert_eq!(report.symbols, vec![eth()]);
    assert_eq!(report.participants.len(), 1);
    assert_eq!(report.participants[0].participant, ParticipantId(8));

    let mut args = positions_args(PARTITION);
    args.participant = Some(7);
    let report = positions_with_journal(args, &journal)
        .await
        .expect("the participant filter succeeds");
    assert_eq!(report.participants.len(), 1);
    assert_eq!(report.participants[0].participant, ParticipantId(7));
    assert_eq!(
        report.symbols,
        vec![symbol()],
        "the symbol list follows the surviving rows"
    );

    let mut args = positions_args(PARTITION);
    args.participant = Some(999);
    let report = positions_with_journal(args, &journal)
        .await
        .expect("an absent participant is an empty success");
    assert!(report.participants.is_empty());
    assert!(report.symbols.is_empty());
}

#[tokio::test]
async fn positions_count_unknown_makers_and_ignore_reject_cancel() {
    let journal = Journal::memory();
    let orders = [
        new_order(symbol(), 1, 8, Side::Sell, 100, 3),
        new_order(symbol(), 2, 8, Side::Sell, 100, 1),
        new_order(symbol(), 3, 8, Side::Sell, 100, 1),
    ];
    let executions = [
        // The maker id 42 never appears in the orders log.
        fill(0, symbol(), 1, 42, Side::Sell, 100, 3, ExecKind::Filled),
        // Rejections and cancels do not move positions or cash.
        fill(1, symbol(), 2, 0, Side::Sell, 100, 0, ExecKind::Rejected),
        fill(2, symbol(), 3, 0, Side::Sell, 100, 0, ExecKind::Cancelled),
    ];
    let order_payloads: Vec<Vec<u8>> = orders.iter().map(Command::encode).collect();
    let execution_payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Orders, &order_payloads).await;
    append(&journal, Topic::Executions, &execution_payloads).await;

    let report = positions_with_journal(positions_args(PARTITION), &journal)
        .await
        .expect("a readable journal produces a report");

    assert_eq!(report.participants.len(), 1);
    let position = &report.participants[0];
    assert_eq!(position.participant, ParticipantId(8));
    assert_eq!(position.net_qty, -3);
    assert_eq!(position.fills, 1, "reject and cancel are not fills");
    assert_eq!(position.cash_cents, 300);
    assert_eq!(position.unresolved_maker_fills, 1);
    assert_eq!(report.unresolved_maker_fills, 1);
    assert_eq!(report.unresolved_aggressor_fills, 0);
}

#[test]
fn positions_json_shape_is_stable() {
    let (orders, executions) = maker_rules_fixture();
    let indexed: Vec<(u64, Command)> = orders
        .into_iter()
        .enumerate()
        .map(|(offset, command)| (offset as u64, command))
        .collect();
    let report = build_positions(PARTITION, &indexed, &executions, None);
    let json = serde_json::to_value(&report).expect("the report serializes");

    assert_eq!(json["partition"], 1);
    assert_eq!(json["symbols"], serde_json::json!(["BTC-USD"]));
    assert_eq!(json["unresolved_maker_fills"], 0);
    assert_eq!(json["unresolved_aggressor_fills"], 0);
    assert_eq!(json["participants"][0]["participant"], 7);
    assert_eq!(json["participants"][0]["net_qty"], 3);
    assert_eq!(json["participants"][0]["buy_qty"], 3);
    assert_eq!(json["participants"][0]["sell_qty"], 0);
    assert_eq!(json["participants"][0]["fills"], 1);
    assert_eq!(json["participants"][0]["cash_cents"], -300);
    assert_eq!(json["participants"][0]["notional_cents"], 300);
    assert_eq!(json["participants"][0]["symbols"][0]["symbol"], "BTC-USD");
    assert_eq!(json["participants"][1]["participant"], 8);
    assert_eq!(json["participants"][1]["net_qty"], -3);
    assert_eq!(json["participants"][1]["cash_cents"], 300);
}

#[tokio::test]
async fn positions_empty_journal_is_a_successful_empty_report() {
    let journal = Journal::memory();
    let report = positions_with_journal(positions_args(PARTITION), &journal)
        .await
        .expect("an empty journal is a success, not an error");
    assert_eq!(
        report,
        PositionReport {
            partition: PARTITION,
            symbols: Vec::new(),
            participants: Vec::new(),
            unresolved_maker_fills: 0,
            unresolved_aggressor_fills: 0,
        }
    );
}

#[tokio::test]
async fn positions_rejects_partition_zero_before_reading() {
    let journal = Journal::memory();
    let error = positions_with_journal(positions_args(0), &journal)
        .await
        .expect_err("partition 0 is a usage error");
    assert!(error.to_string().contains("--partition"));
}

#[test]
fn positions_args_defaults_and_overrides_parse() {
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        positions: PositionsArgs,
    }

    let cli = Cli::parse_from(["test"]);
    assert_eq!(cli.positions.iggy_addr, "127.0.0.1:8090");
    assert_eq!(cli.positions.iggy_user, "iggy");
    assert_eq!(cli.positions.iggy_password, "iggy");
    assert!(cli.positions.iggy_password_file.is_none());
    assert_eq!(cli.positions.partition, 1);
    assert!(cli.positions.symbols.is_empty());
    assert!(cli.positions.participant.is_none());
    assert!(!cli.positions.json);

    let cli = Cli::parse_from([
        "test",
        "--iggy-addr",
        "127.0.0.1:9999",
        "--partition",
        "5",
        "--symbols",
        "BTC-USD,ETH-USD",
        "--participant",
        "7",
        "--json",
        "--iggy-password-file",
        "/tmp/iggy-password",
    ]);
    assert_eq!(cli.positions.iggy_addr, "127.0.0.1:9999");
    assert_eq!(cli.positions.partition, 5);
    assert_eq!(
        cli.positions.symbols,
        vec![symbol(), eth()],
        "symbols parse as a comma-separated list"
    );
    assert_eq!(cli.positions.participant, Some(7));
    assert!(cli.positions.json);
    assert_eq!(
        cli.positions.iggy_password_file,
        Some(std::path::PathBuf::from("/tmp/iggy-password"))
    );
}
