// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Read-only operator tooling proofs.
//!
//! `whippet audit` and `whippet order-trace` observe the audit, orders, and
//! executions topics through the `Journal` seam. These tests pin the gateway
//! JSON contract, the malformed-record policy, the filter axes, the maker /
//! aggressor matching, cause-offset verification, and the CLI defaults. They
//! also assert the tools are read-only: no consumer offset moves and no record
//! is appended or deleted.

use clap::Parser;
use whippet::cli::{AuditArgs, OrderTraceArgs, audit_with_journal, order_trace_with_journal};
use whippet::inspect::{AUDIT_TOPIC_PARTITIONS, AuditRecord, ExecRole, OrderEventKind};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{
    Command, ExecKind, ExecutionReport, FLAG_TAKER, OrderType, RejectReason, Side,
};
use whippet_journal::{Journal, JournalMessage, Topic};

/// The exact object `admin_control` appends to the audit topic.
const GATEWAY_RECORD: &str = r#"{"operator":"lead","action":"halt","symbol":"BTC-USD","halted":true,"reason":"volatility spike","order_offset":42,"timestamp_ns":1700000000000000000,"service":"whippet"}"#;

/// A resume with a field this reader does not know about. Unknown fields must
/// not break the reader.
const RESUME_WITH_EXTRA_FIELD: &str = r#"{"operator":"risk","action":"resume","symbol":"BTC-USD","halted":false,"reason":null,"order_offset":43,"timestamp_ns":1700000000000000001,"service":"whippet","trace_id":"abc-123"}"#;

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

fn audit_args(partitions: u32) -> AuditArgs {
    AuditArgs {
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        iggy_password_file: None,
        partitions,
        operator: None,
        action: None,
        symbol: None,
        min_offset: None,
        limit: None,
        strict: false,
        json: false,
    }
}

fn trace_args(order_id: OrderId) -> OrderTraceArgs {
    OrderTraceArgs {
        iggy_addr: "127.0.0.1:8090".into(),
        iggy_user: "iggy".into(),
        iggy_password: "iggy".into(),
        iggy_password_file: None,
        partition: 1,
        order_id,
        symbols: Vec::new(),
        json: false,
        verify: false,
    }
}

async fn append(journal: &Journal, topic: Topic, partition: u32, payloads: &[Vec<u8>]) {
    let messages: Vec<JournalMessage> = payloads
        .iter()
        .map(|payload| JournalMessage::new(payload.clone(), Vec::new()))
        .collect();
    journal
        .append(topic, partition, &messages)
        .await
        .expect("append to the memory journal");
}

async fn topic_len(journal: &Journal, topic: Topic, partition: u32) -> usize {
    journal
        .read_payloads(topic, partition, "inspect-test", 0, 1_000, 0)
        .await
        .expect("read the memory journal")
        .len()
}

async fn audit_records(journal: &Journal, args: AuditArgs) -> Vec<AuditRecord> {
    audit_with_journal(args, journal)
        .await
        .expect("audit read")
        .records
}

#[tokio::test]
async fn audit_lists_gateway_records_and_skips_malformed_json() {
    let journal = Journal::memory();
    append(
        &journal,
        Topic::Audit,
        0,
        &[GATEWAY_RECORD.as_bytes().to_vec()],
    )
    .await;
    append(
        &journal,
        Topic::Audit,
        1,
        &[RESUME_WITH_EXTRA_FIELD.as_bytes().to_vec()],
    )
    .await;
    append(&journal, Topic::Audit, 2, &[b"not-json".to_vec()]).await;

    let report = audit_with_journal(audit_args(4), &journal)
        .await
        .expect("a malformed record is skipped, not fatal");
    assert_eq!(report.partitions, 4);
    assert_eq!(report.scanned, 3);
    assert_eq!(report.decoded, 2);
    assert_eq!(report.malformed, 1);
    assert_eq!(report.records.len(), 2);

    let halt = &report.records[0];
    assert_eq!(halt.partition, 0);
    assert_eq!(halt.offset, 0);
    assert_eq!(halt.raw, GATEWAY_RECORD.as_bytes());
    assert_eq!(halt.payload.operator, "lead");
    assert_eq!(halt.payload.action, "halt");
    assert_eq!(halt.payload.symbol, "BTC-USD");
    assert!(halt.payload.halted);
    assert_eq!(halt.payload.reason.as_deref(), Some("volatility spike"));
    assert_eq!(halt.payload.order_offset, 42);
    assert_eq!(halt.payload.service, "whippet");

    // The extra field is ignored, and a null reason decodes as None.
    let resume = &report.records[1];
    assert_eq!(resume.partition, 1);
    assert_eq!(resume.payload.operator, "risk");
    assert!(!resume.payload.halted);
    assert_eq!(resume.payload.reason, None);

    // Read-only: no consumer watermark was written and every record survives.
    for partition in 0..AUDIT_TOPIC_PARTITIONS {
        assert_eq!(
            journal
                .committed(
                    Topic::Audit,
                    partition,
                    &format!("whippet-audit-p{partition}")
                )
                .await
                .expect("read audit consumer offset"),
            None,
            "audit moved a consumer offset on partition {partition}"
        );
    }
    assert_eq!(topic_len(&journal, Topic::Audit, 0).await, 1);
    assert_eq!(topic_len(&journal, Topic::Audit, 1).await, 1);
    assert_eq!(topic_len(&journal, Topic::Audit, 2).await, 1);

    // --strict turns the same input into a failure, after printing the records
    // that did decode.
    let strict = AuditArgs {
        strict: true,
        ..audit_args(4)
    };
    let error = audit_with_journal(strict, &journal)
        .await
        .expect_err("--strict rejects a malformed audit record");
    assert!(
        error.to_string().contains("--strict"),
        "unexpected error: {error}"
    );

    // The audit topic is pinned at four partitions.
    for partitions in [0, 5] {
        let error = audit_with_journal(audit_args(partitions), &journal)
            .await
            .expect_err("an impossible audit partition count is rejected");
        assert!(
            error.to_string().contains("--partitions"),
            "unexpected error: {error}"
        );
    }
}

#[tokio::test]
async fn audit_filters_by_operator_action_symbol_offset_and_limit() {
    let journal = Journal::memory();
    let halt_btc = r#"{"operator":"lead","action":"halt","symbol":"BTC-USD","halted":true,"reason":null,"order_offset":1,"timestamp_ns":1,"service":"whippet"}"#;
    let resume_btc = r#"{"operator":"risk","action":"resume","symbol":"BTC-USD","halted":false,"reason":"all clear","order_offset":2,"timestamp_ns":2,"service":"whippet"}"#;
    let halt_eth = r#"{"operator":"lead","action":"halt","symbol":"ETH-USD","halted":true,"reason":"wide","order_offset":3,"timestamp_ns":3,"service":"whippet"}"#;
    append(
        &journal,
        Topic::Audit,
        0,
        &[
            halt_btc.as_bytes().to_vec(),
            resume_btc.as_bytes().to_vec(),
            halt_eth.as_bytes().to_vec(),
        ],
    )
    .await;

    assert_eq!(audit_records(&journal, audit_args(1)).await.len(), 3);

    let lead = audit_records(
        &journal,
        AuditArgs {
            operator: Some("lead".into()),
            ..audit_args(1)
        },
    )
    .await;
    assert_eq!(lead.len(), 2);

    let resume = audit_records(
        &journal,
        AuditArgs {
            action: Some("resume".into()),
            ..audit_args(1)
        },
    )
    .await;
    assert_eq!(resume.len(), 1);
    assert_eq!(resume[0].payload.operator, "risk");

    // Symbols are matched case-insensitively.
    let btc = audit_records(
        &journal,
        AuditArgs {
            symbol: Some("btc-usd".into()),
            ..audit_args(1)
        },
    )
    .await;
    assert_eq!(btc.len(), 2);

    // min_offset is the audit-topic offset, inclusive.
    let after_first = audit_records(
        &journal,
        AuditArgs {
            min_offset: Some(1),
            ..audit_args(1)
        },
    )
    .await;
    assert_eq!(after_first.len(), 2);

    let limited = audit_records(
        &journal,
        AuditArgs {
            operator: Some("lead".into()),
            limit: Some(1),
            ..audit_args(1)
        },
    )
    .await;
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].payload.symbol, "BTC-USD");

    let combined = audit_records(
        &journal,
        AuditArgs {
            operator: Some("lead".into()),
            action: Some("halt".into()),
            symbol: Some("ETH-USD".into()),
            ..audit_args(1)
        },
    )
    .await;
    assert_eq!(combined.len(), 1);
    assert_eq!(combined[0].payload.order_offset, 3);

    let nobody = audit_records(
        &journal,
        AuditArgs {
            operator: Some("nobody".into()),
            ..audit_args(1)
        },
    )
    .await;
    assert!(nobody.is_empty());
}

/// The order-trace fixture: order `0xaa` rests, aggressor `0xbb` fills four
/// units against it, then order `0xaa` is cancelled.
fn order_trace_commands() -> Vec<Command> {
    let symbol = symbol();
    let participant = ParticipantId(7);
    vec![
        Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(100),
            Qty(10),
            OrderId(0xaa),
            participant,
            1,
        ),
        Command::new_order(
            symbol,
            Side::Sell,
            OrderType::Limit,
            Price(100),
            Qty(4),
            OrderId(0xbb),
            ParticipantId(8),
            2,
        ),
        Command::cancel(symbol, OrderId(0xaa), participant, 3),
    ]
}

fn report(
    order_id: OrderId,
    maker_order_id: OrderId,
    kind: ExecKind,
    qty: u64,
    remaining: u64,
    seq: u64,
    cause_offset: u64,
) -> ExecutionReport {
    ExecutionReport {
        kind,
        symbol: symbol(),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Price(100),
        qty: Qty(qty),
        remaining: Qty(remaining),
        order_id,
        maker_order_id,
        seq: Seq(seq),
        cause_offset,
        timestamp_ns: seq + 1,
        reason: RejectReason::None,
        flags: 0,
    }
}

#[tokio::test]
async fn order_trace_matches_aggressor_and_maker_legs_in_log_order() {
    let journal = Journal::memory();
    let orders: Vec<Vec<u8>> = order_trace_commands().iter().map(Command::encode).collect();
    append(&journal, Topic::Orders, 1, &orders).await;

    // The engine reports the aggressor's fill against order 0xaa; the maker
    // side is the report whose `maker_order_id` is 0xaa. The own partial and
    // cancel name 0xaa directly.
    let mut maker_leg = report(OrderId(0xbb), OrderId(0xaa), ExecKind::Filled, 4, 0, 0, 1);
    maker_leg.flags = FLAG_TAKER;
    let executions = [
        report(
            OrderId(0xaa),
            OrderId(0),
            ExecKind::PartiallyFilled,
            1,
            9,
            1,
            0,
        ),
        maker_leg,
        report(OrderId(0xaa), OrderId(0), ExecKind::Cancelled, 9, 0, 2, 2),
    ];
    let payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Executions, 1, &payloads).await;

    let trace = order_trace_with_journal(trace_args(OrderId(0xaa)), &journal)
        .await
        .expect("a readable journal produces a trace");

    assert_eq!(trace.order_id, OrderId(0xaa));
    assert_eq!(trace.partition, 1);
    assert_eq!(trace.orders.len(), 2);
    assert_eq!(trace.orders[0].offset, 0);
    assert_eq!(trace.orders[0].kind, OrderEventKind::NewOrder);
    assert_eq!(trace.orders[0].price_cents, Some(100));
    assert_eq!(trace.orders[0].qty, Some(10));
    assert_eq!(trace.orders[1].offset, 2);
    assert_eq!(trace.orders[1].kind, OrderEventKind::Cancel);
    assert_eq!(trace.orders[1].price_cents, None);
    assert_eq!(trace.orders[1].qty, None);

    // Sorted by the offset that caused each event, so the timeline interleaves
    // the own events and the aggressor's maker-side fill correctly.
    assert_eq!(
        trace
            .executions
            .iter()
            .map(|event| event.cause_offset)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        trace
            .executions
            .iter()
            .map(|event| event.role)
            .collect::<Vec<_>>(),
        vec![ExecRole::Order, ExecRole::Maker, ExecRole::Order]
    );
    assert_eq!(
        trace.executions[1].identity(),
        (symbol(), 1, 0, ExecKind::Filled)
    );
    assert_eq!(trace.executions[1].maker_order_id, OrderId(0xaa));
    assert_eq!(trace.executions[1].order_id, OrderId(0xbb));
    assert!(trace.complete, "a cancelled report terminates the order");
    assert!(trace.unresolved_cause_offsets.is_empty());

    // Every cause offset resolves, so --verify passes.
    let verified = OrderTraceArgs {
        verify: true,
        ..trace_args(OrderId(0xaa))
    };
    order_trace_with_journal(verified, &journal)
        .await
        .expect("every cause offset resolves");

    // Read-only: both topics keep every record and no trace consumer commits.
    assert_eq!(topic_len(&journal, Topic::Orders, 1).await, 3);
    assert_eq!(topic_len(&journal, Topic::Executions, 1).await, 3);
    for (topic, consumer) in [
        (Topic::Orders, "whippet-order-trace-orders-p1"),
        (Topic::Executions, "whippet-order-trace-executions-p1"),
    ] {
        assert_eq!(
            journal
                .committed(topic, 1, consumer)
                .await
                .expect("read trace consumer"),
            None
        );
    }
}

#[tokio::test]
async fn order_trace_reports_unresolved_cause_offsets() {
    let journal = Journal::memory();
    let symbol = symbol();
    let order_id = OrderId(0xaa);
    let orders = vec![
        Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(100),
            Qty(1),
            order_id,
            ParticipantId(1),
            1,
        )
        .encode(),
        // A halt never causes an execution report, so a report claiming it as
        // its cause is unresolved even though a command exists at the offset.
        Command::halt(symbol, ParticipantId(2), 2).encode(),
    ];
    append(&journal, Topic::Orders, 1, &orders).await;
    let executions = [
        report(order_id, OrderId(0), ExecKind::Accepted, 1, 1, 0, 9),
        report(order_id, OrderId(0), ExecKind::Accepted, 1, 1, 1, 1),
    ];
    let payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Executions, 1, &payloads).await;

    let trace = order_trace_with_journal(trace_args(order_id), &journal)
        .await
        .expect("an unresolved cause is reported, not fatal without --verify");
    assert_eq!(trace.unresolved_cause_offsets, vec![1, 9]);
    assert!(!trace.complete);

    let verified = OrderTraceArgs {
        verify: true,
        ..trace_args(order_id)
    };
    let error = order_trace_with_journal(verified, &journal)
        .await
        .expect_err("--verify fails on an unresolved cause offset");
    assert!(
        error.to_string().contains("--verify"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn order_trace_with_no_matches_is_an_empty_success() {
    let journal = Journal::memory();
    let commands: Vec<Vec<u8>> = order_trace_commands().iter().map(Command::encode).collect();
    append(&journal, Topic::Orders, 1, &commands).await;

    let trace = order_trace_with_journal(
        OrderTraceArgs {
            verify: true,
            ..trace_args(OrderId(0xdead))
        },
        &journal,
    )
    .await
    .expect("no matching records is a zero exit, not an error");
    assert!(trace.orders.is_empty());
    assert!(trace.executions.is_empty());
    assert!(trace.unresolved_cause_offsets.is_empty());
    assert!(!trace.complete);
}

#[test]
fn audit_args_defaults_and_parse() {
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        audit: AuditArgs,
    }

    let cli = Cli::parse_from(["test"]);
    assert_eq!(cli.audit.iggy_addr, "127.0.0.1:8090");
    assert_eq!(cli.audit.iggy_user, "iggy");
    assert_eq!(cli.audit.iggy_password, "iggy");
    assert!(cli.audit.iggy_password_file.is_none());
    assert_eq!(cli.audit.partitions, AUDIT_TOPIC_PARTITIONS);
    assert_eq!(cli.audit.operator, None);
    assert_eq!(cli.audit.action, None);
    assert_eq!(cli.audit.symbol, None);
    assert_eq!(cli.audit.min_offset, None);
    assert_eq!(cli.audit.limit, None);
    assert!(!cli.audit.strict);
    assert!(!cli.audit.json);

    let cli = Cli::parse_from([
        "test",
        "--iggy-addr",
        "127.0.0.1:9999",
        "--partitions",
        "2",
        "--operator",
        "lead",
        "--action",
        "halt",
        "--symbol",
        "BTC-USD",
        "--min-offset",
        "7",
        "--limit",
        "10",
        "--strict",
        "--json",
        "--iggy-password-file",
        "/tmp/iggy-password",
    ]);
    assert_eq!(cli.audit.iggy_addr, "127.0.0.1:9999");
    assert_eq!(cli.audit.partitions, 2);
    assert_eq!(cli.audit.operator.as_deref(), Some("lead"));
    assert_eq!(cli.audit.action.as_deref(), Some("halt"));
    assert_eq!(cli.audit.symbol.as_deref(), Some("BTC-USD"));
    assert_eq!(cli.audit.min_offset, Some(7));
    assert_eq!(cli.audit.limit, Some(10));
    assert!(cli.audit.strict);
    assert!(cli.audit.json);
    assert_eq!(
        cli.audit.iggy_password_file,
        Some(std::path::PathBuf::from("/tmp/iggy-password"))
    );

    // Only the two operator actions exist.
    assert!(Cli::try_parse_from(["test", "--action", "cancel"]).is_err());
}

#[test]
fn order_trace_args_defaults_and_order_id_forms() {
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        trace: OrderTraceArgs,
    }

    let cli = Cli::parse_from(["test", "--order-id", "0xdeadBEEF"]);
    assert_eq!(cli.trace.iggy_addr, "127.0.0.1:8090");
    assert_eq!(cli.trace.partition, 1);
    assert_eq!(cli.trace.order_id, OrderId(0xdead_beef));
    assert!(cli.trace.symbols.is_empty());
    assert!(!cli.trace.json);
    assert!(!cli.trace.verify);

    // A u128 decimal is the documented alternative to 0x hex.
    let cli = Cli::parse_from(["test", "--order-id", "170"]);
    assert_eq!(cli.trace.order_id, OrderId(170));

    // `OrderId` prints 32 bare hex digits; that form is accepted too.
    let cli = Cli::parse_from(["test", "--order-id", "000000000000000000000000000000aa"]);
    assert_eq!(cli.trace.order_id, OrderId(0xaa));

    let cli = Cli::parse_from([
        "test",
        "--order-id",
        "1",
        "--partition",
        "5",
        "--symbols",
        "BTC-USD,ETH-USD",
        "--json",
        "--verify",
    ]);
    assert_eq!(cli.trace.partition, 5);
    assert_eq!(
        cli.trace.symbols,
        vec![symbol(), Symbol::new("ETH-USD").unwrap()]
    );
    assert!(cli.trace.json);
    assert!(cli.trace.verify);

    // The order id is required, and a malformed one is a parse error.
    assert!(Cli::try_parse_from(["test"]).is_err());
    assert!(Cli::try_parse_from(["test", "--order-id", "nope"]).is_err());
}
