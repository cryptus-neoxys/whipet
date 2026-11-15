// Licensed under the Apache License, Version 2.0. See LICENSE.

//! `whippet surveillance` proofs.
//!
//! The surveillance command observes the orders and executions topics through
//! the `Journal` seam and prints educational per-participant indicators.
//! These tests pin the counters, the indicator attribution rules, the window
//! filter, the CLI defaults, the `--fail-on-findings` exit semantics, and —
//! most importantly — that the command is read-only: no consumer offset
//! moves and no record is appended or deleted.

use clap::Parser;
use whippet::cli::{SurveillanceArgs, surveillance_with_journal};
use whippet::surveillance::Indicator;
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

fn surveillance_args(min_orders: u64) -> SurveillanceArgs {
    SurveillanceArgs {
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        iggy_password_file: None,
        partition: PARTITION,
        symbols: Vec::new(),
        window_secs: 3_600,
        min_orders,
        cancel_ratio_bps: 7_000,
        large_notional_cents: 10_000_000,
        json: false,
        fail_on_findings: false,
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
        .read_payloads(topic, PARTITION, "surveillance-test", 0, 1_000, 0)
        .await
        .expect("read the memory journal")
        .len()
}

fn new_order(id: u128, participant: u64, ts: u64, price: i64, qty: u64) -> Command {
    Command::new_order(
        symbol(),
        Side::Buy,
        OrderType::Limit,
        Price(price),
        Qty(qty),
        OrderId(id),
        ParticipantId(participant),
        ts,
    )
}

fn cancel_cmd(id: u128, participant: u64, ts: u64) -> Command {
    Command::cancel(symbol(), OrderId(id), ParticipantId(participant), ts)
}

fn exec(
    order_id: u128,
    kind: ExecKind,
    reason: RejectReason,
    ts: u64,
    price: i64,
    qty: u64,
) -> ExecutionReport {
    ExecutionReport {
        kind,
        symbol: symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Price(price),
        qty: Qty(qty),
        remaining: Qty(0),
        order_id: OrderId(order_id),
        maker_order_id: OrderId(0),
        seq: Seq(ts),
        cause_offset: ts,
        timestamp_ns: ts,
        reason,
        flags: 0,
    }
}

/// Three participants:
///
/// - 7 is cancel-heavy: four large orders, three cancelled unfilled.
/// - 8 trades and retries: four orders plus one filled, one small cancel,
///   and a duplicate-order-id rejection on a fifth order.
/// - 9 is the maker side of a self-trade: the engine cancelled their resting
///   order with the self-trade-prevention reason.
fn fixture_orders() -> Vec<Command> {
    vec![
        new_order(1, 7, 100, 1_000_000, 10),
        new_order(2, 7, 101, 1_000_000, 10),
        new_order(3, 7, 102, 1_000_000, 10),
        new_order(4, 7, 103, 1_000_000, 10),
        cancel_cmd(1, 7, 200),
        cancel_cmd(2, 7, 201),
        cancel_cmd(3, 7, 202),
        new_order(10, 8, 300, 1_000_000, 10),
        new_order(11, 8, 301, 10, 10),
        new_order(12, 8, 302, 1_000, 1),
        new_order(13, 8, 303, 1_000, 1),
        cancel_cmd(11, 8, 400),
        new_order(30, 8, 500, 100, 1),
        new_order(20, 9, 600, 1_000, 1),
    ]
}

fn fixture_executions() -> Vec<ExecutionReport> {
    vec![
        exec(10, ExecKind::Filled, RejectReason::None, 700, 100, 5),
        exec(
            20,
            ExecKind::Cancelled,
            RejectReason::SelfTradePrevention,
            800,
            1_000,
            1,
        ),
        exec(
            30,
            ExecKind::Rejected,
            RejectReason::DuplicateOrderId,
            900,
            100,
            0,
        ),
    ]
}

#[tokio::test]
async fn surveillance_reports_counters_and_indicators_and_is_read_only() {
    let journal = Journal::memory();
    let orders_before = topic_len(&journal, Topic::Orders).await;
    let executions_before = topic_len(&journal, Topic::Executions).await;

    let orders: Vec<Vec<u8>> = fixture_orders().iter().map(Command::encode).collect();
    append(&journal, Topic::Orders, &orders).await;
    let executions: Vec<Vec<u8>> = fixture_executions()
        .iter()
        .map(ExecutionReport::encode)
        .collect();
    append(&journal, Topic::Executions, &executions).await;

    // min_orders is lowered to 4 so participant 7's 7_500 bps cancel ratio is
    // judged against the 7_000 bps threshold.
    let report = surveillance_with_journal(surveillance_args(4), &journal)
        .await
        .expect("a readable journal produces a report");

    assert_eq!(report.partition, PARTITION);
    assert_eq!(
        report.window_end_ns, 900,
        "the newest event is the window's now"
    );
    assert_eq!(
        report.window_start_ns, 0,
        "a 3600 s window covers all fixtures"
    );
    assert_eq!(
        report
            .participants
            .iter()
            .map(|p| p.participant.0)
            .collect::<Vec<_>>(),
        vec![7, 8, 9],
        "participants are sorted by id"
    );

    let p7 = &report.participants[0];
    assert_eq!(p7.orders, 4);
    assert_eq!(p7.cancels, 3);
    assert_eq!(p7.fills, 0);
    assert_eq!(p7.rejected, 0);
    assert_eq!(p7.stp_cancels, 0);
    assert_eq!(p7.duplicate_rejects, 0);
    assert_eq!(p7.cancelled_notional_cents, 30_000_000);
    assert_eq!(p7.cancel_ratio_bps, 7_500);
    assert_eq!(
        p7.indicators,
        vec![
            Indicator::HighCancelRatio,
            Indicator::LargeCancelledWithoutFill
        ]
    );

    let p8 = &report.participants[1];
    assert_eq!(p8.orders, 5);
    assert_eq!(p8.cancels, 1);
    assert_eq!(p8.fills, 1);
    assert_eq!(p8.rejected, 1);
    assert_eq!(p8.duplicate_rejects, 1);
    assert_eq!(p8.cancelled_notional_cents, 100);
    assert_eq!(p8.filled_notional_cents, 500);
    assert_eq!(p8.cancel_ratio_bps, 2_000);
    assert_eq!(p8.indicators, vec![Indicator::DuplicateOrderIdRetries]);

    let p9 = &report.participants[2];
    assert_eq!(p9.orders, 1);
    assert_eq!(
        p9.cancels, 0,
        "an engine-initiated STP cancel is not a command cancel"
    );
    assert_eq!(p9.stp_cancels, 1);
    assert_eq!(p9.indicators, vec![Indicator::SelfTradePrevention]);

    // Read-only: both topics keep every record and no surveillance consumer
    // commits an offset.
    assert_eq!(topic_len(&journal, Topic::Orders).await, orders_before + 14);
    assert_eq!(
        topic_len(&journal, Topic::Executions).await,
        executions_before + 3
    );
    for (topic, consumer) in [
        (Topic::Orders, "whippet-surveillance-orders-p1"),
        (Topic::Executions, "whippet-surveillance-executions-p1"),
    ] {
        assert_eq!(
            journal
                .committed(topic, PARTITION, consumer)
                .await
                .expect("read surveillance consumer"),
            None,
            "surveillance moved a consumer offset on {topic}"
        );
    }

    // --fail-on-findings turns the same report into a non-zero exit.
    let failing = SurveillanceArgs {
        fail_on_findings: true,
        ..surveillance_args(4)
    };
    let error = surveillance_with_journal(failing, &journal)
        .await
        .expect_err("--fail-on-findings fails when any indicator is flagged");
    let message = error.to_string();
    assert!(
        message.contains("--fail-on-findings"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("4 indicator(s)"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn surveillance_symbol_filter_restricts_the_scan() {
    let journal = Journal::memory();
    let btc = new_order(1, 7, 100, 100, 1);
    let eth_order = Command::new_order(
        eth(),
        Side::Buy,
        OrderType::Limit,
        Price(100),
        Qty(1),
        OrderId(2),
        ParticipantId(7),
        101,
    );
    let mut orders: Vec<Vec<u8>> = vec![btc.encode(), eth_order.encode()];
    append(&journal, Topic::Orders, &orders).await;
    orders.clear();

    let mut args = surveillance_args(1);
    args.symbols = vec![eth()];
    let report = surveillance_with_journal(args, &journal)
        .await
        .expect("the filtered scan succeeds");
    assert_eq!(report.participants.len(), 1);
    assert_eq!(
        report.participants[0].orders, 1,
        "only the ETH order is scanned"
    );

    // Without the filter both orders are counted.
    let report = surveillance_with_journal(surveillance_args(1), &journal)
        .await
        .expect("the unfiltered scan succeeds");
    assert_eq!(report.participants[0].orders, 2);
}

#[tokio::test]
async fn surveillance_window_excludes_old_events_through_the_cli() {
    let journal = Journal::memory();
    // Newest event at ts = 400e9; a 100 s window starts at 300e9, so the
    // order at ts = 0 is excluded from the counts.
    let orders = [
        new_order(1, 7, 0, 100, 1),
        new_order(2, 7, 400_000_000_000, 100, 1),
    ];
    let payloads: Vec<Vec<u8>> = orders.iter().map(Command::encode).collect();
    append(&journal, Topic::Orders, &payloads).await;

    let mut args = surveillance_args(1);
    args.window_secs = 100;
    let report = surveillance_with_journal(args, &journal)
        .await
        .expect("the windowed scan succeeds");
    assert_eq!(report.window_start_ns, 300_000_000_000);
    assert_eq!(report.window_end_ns, 400_000_000_000);
    assert_eq!(report.participants[0].orders, 1);
}

#[tokio::test]
async fn surveillance_empty_journal_is_a_successful_empty_report() {
    let journal = Journal::memory();
    // No matches is exit 0 even with --fail-on-findings: a vacuous report is
    // not a finding.
    let failing = SurveillanceArgs {
        fail_on_findings: true,
        ..surveillance_args(1)
    };
    let report = surveillance_with_journal(failing, &journal)
        .await
        .expect("an empty journal is a success, not an error");
    assert!(report.participants.is_empty());
    assert_eq!(report.window_start_ns, 0);
    assert_eq!(report.window_end_ns, 0);
}

#[tokio::test]
async fn surveillance_rejects_invalid_invocations_before_connecting() {
    let journal = Journal::memory();

    let mut args = surveillance_args(1);
    args.partition = 0;
    let error = surveillance_with_journal(args, &journal)
        .await
        .expect_err("partition 0 is a usage error");
    assert!(error.to_string().contains("--partition"));

    let mut args = surveillance_args(1);
    args.cancel_ratio_bps = 10_001;
    let error = surveillance_with_journal(args, &journal)
        .await
        .expect_err("a ratio above the bps scale is a usage error");
    assert!(
        error.to_string().contains("cancel_ratio_bps"),
        "unexpected error: {error}"
    );

    let mut args = surveillance_args(1);
    args.window_secs = 0;
    let error = surveillance_with_journal(args, &journal)
        .await
        .expect_err("a zero window is a usage error");
    assert!(
        error.to_string().contains("window_secs"),
        "unexpected error: {error}"
    );
}

#[test]
fn surveillance_args_defaults_and_overrides_parse() {
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        surveillance: SurveillanceArgs,
    }

    let cli = Cli::parse_from(["test"]);
    assert_eq!(cli.surveillance.iggy_addr, "127.0.0.1:8090");
    assert_eq!(cli.surveillance.iggy_user, "iggy");
    assert_eq!(cli.surveillance.iggy_password, "iggy");
    assert!(cli.surveillance.iggy_password_file.is_none());
    assert_eq!(cli.surveillance.partition, 1);
    assert!(cli.surveillance.symbols.is_empty());
    assert_eq!(cli.surveillance.window_secs, 3_600);
    assert_eq!(cli.surveillance.min_orders, 20);
    assert_eq!(cli.surveillance.cancel_ratio_bps, 7_000);
    assert_eq!(cli.surveillance.large_notional_cents, 10_000_000);
    assert!(!cli.surveillance.json);
    assert!(!cli.surveillance.fail_on_findings);

    let cli = Cli::parse_from([
        "test",
        "--iggy-addr",
        "127.0.0.1:9999",
        "--partition",
        "5",
        "--symbols",
        "BTC-USD,ETH-USD",
        "--window-secs",
        "60",
        "--min-orders",
        "10",
        "--cancel-ratio-bps",
        "5000",
        "--large-notional-cents",
        "1000",
        "--json",
        "--fail-on-findings",
        "--iggy-password-file",
        "/tmp/iggy-password",
    ]);
    assert_eq!(cli.surveillance.iggy_addr, "127.0.0.1:9999");
    assert_eq!(cli.surveillance.partition, 5);
    assert_eq!(
        cli.surveillance.symbols,
        vec![symbol(), eth()],
        "symbols parse as a comma-separated list"
    );
    assert_eq!(cli.surveillance.window_secs, 60);
    assert_eq!(cli.surveillance.min_orders, 10);
    assert_eq!(cli.surveillance.cancel_ratio_bps, 5_000);
    assert_eq!(cli.surveillance.large_notional_cents, 1_000);
    assert!(cli.surveillance.json);
    assert!(cli.surveillance.fail_on_findings);
    assert_eq!(
        cli.surveillance.iggy_password_file,
        Some(std::path::PathBuf::from("/tmp/iggy-password"))
    );
}
