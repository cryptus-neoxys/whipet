// Licensed under the Apache License, Version 2.0. See LICENSE.

//! `whippet ledger` proofs.
//!
//! The ledger projects one partition's orders and executions logs into
//! balanced double-entry entries: four postings per trade, cash and position
//! conserved per entry and across the scan. These tests pin the two-leg
//! attribution, the all-or-nothing unresolved rule, the account fold, the
//! canonical `seq` ordering, reconciliation against `build_positions`, the JSON
//! shape, and — like the positions tests — that the command is read-only.

use std::collections::BTreeMap;

use clap::Parser;
use whippet::cli::{LedgerArgs, ledger_with_journal};
use whippet::ledger::{AccountBalance, Asset, LedgerReport, build_ledger};
use whippet::positions::build_positions;
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

fn sol() -> Symbol {
    Symbol::new("SOL-USD").expect("static symbol")
}

fn symbols() -> [Symbol; 3] {
    [symbol(), eth(), sol()]
}

fn ledger_args(partition: u32) -> LedgerArgs {
    LedgerArgs {
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
        .read_payloads(topic, PARTITION, "ledger-test", 0, 1_000, 0)
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

fn indexed(orders: Vec<Command>) -> Vec<(u64, Command)> {
    orders
        .into_iter()
        .enumerate()
        .map(|(offset, command)| (offset as u64, command))
        .collect()
}

fn account(report: &LedgerReport, participant: u64, asset: Asset) -> Option<&AccountBalance> {
    report
        .accounts
        .iter()
        .find(|account| account.participant == ParticipantId(participant) && account.asset == asset)
}

/// Direct fold of the entries, used to prove the account rows independently of
/// the builder's own accumulator.
fn fold_entries(
    entries: &[whippet::ledger::LedgerEntry],
) -> BTreeMap<(ParticipantId, Asset), (i128, u64)> {
    let mut folded: BTreeMap<(ParticipantId, Asset), (i128, u64)> = BTreeMap::new();
    for entry in entries {
        for posting in &entry.postings {
            let value = folded
                .entry((posting.participant, posting.asset))
                .or_insert((0, 0));
            value.0 += posting.delta;
            value.1 += 1;
        }
    }
    folded
}

/// Participant 7 rests a buy for 5 at 100; participant 8 sells 3 into it.
fn resting_buy_fixture() -> (Vec<Command>, Vec<ExecutionReport>) {
    let orders = vec![
        new_order(symbol(), 1, 7, Side::Buy, 100, 5),
        new_order(symbol(), 2, 8, Side::Sell, 100, 3),
    ];
    let executions = vec![fill(
        10,
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

#[test]
fn a_trade_posts_four_balanced_legs_and_zero_sums() {
    let (orders, executions) = resting_buy_fixture();
    let report = build_ledger(PARTITION, &indexed(orders), &executions, None);
    report.check_invariants().expect("the trade balances");

    assert_eq!(report.entries.len(), 1);
    let entry = &report.entries[0];
    assert_eq!(entry.seq, 10);
    assert_eq!(entry.cause_offset, 10);
    assert_eq!(entry.symbol, symbol());
    assert_eq!(entry.price_cents, 100);
    assert_eq!(entry.qty, 3);
    assert_eq!(
        entry.buyer,
        ParticipantId(7),
        "the resting buy is the buyer"
    );
    assert_eq!(entry.seller, ParticipantId(8));
    assert_eq!(
        entry.postings,
        vec![
            whippet::ledger::Posting {
                participant: ParticipantId(7),
                asset: Asset::Cash,
                delta: -300,
            },
            whippet::ledger::Posting {
                participant: ParticipantId(8),
                asset: Asset::Cash,
                delta: 300,
            },
            whippet::ledger::Posting {
                participant: ParticipantId(7),
                asset: Asset::Position(symbol()),
                delta: 3,
            },
            whippet::ledger::Posting {
                participant: ParticipantId(8),
                asset: Asset::Position(symbol()),
                delta: -3,
            },
        ]
    );

    // Zero sums per asset: the four postings cancel in cash and in the symbol.
    for asset in [Asset::Cash, Asset::Position(symbol())] {
        let sum: i128 = entry
            .postings
            .iter()
            .filter(|posting| posting.asset == asset)
            .map(|posting| posting.delta)
            .sum();
        assert_eq!(sum, 0, "{asset} does not balance");
    }

    assert_eq!(account(&report, 7, Asset::Cash).unwrap().balance, -300);
    assert_eq!(
        account(&report, 7, Asset::Position(symbol()))
            .unwrap()
            .balance,
        3
    );
    assert_eq!(account(&report, 8, Asset::Cash).unwrap().balance, 300);
    assert_eq!(
        account(&report, 8, Asset::Position(symbol()))
            .unwrap()
            .balance,
        -3
    );
    for account in &report.accounts {
        assert_eq!(account.entries, 1);
    }
    assert_eq!(report.accounts.len(), 4);
    assert_eq!(report.unresolved_maker_fills, 0);
    assert_eq!(report.unresolved_aggressor_fills, 0);
}

#[test]
fn partial_fills_accumulate_on_the_maker_and_keep_seq_order() {
    // Maker 7 rests a buy for 5; two takers sell 2 and 3 into it. One report
    // per trade, so one ledger entry per taker, in seq order.
    let orders = indexed(vec![
        new_order(symbol(), 1, 7, Side::Buy, 100, 5),
        new_order(symbol(), 2, 8, Side::Sell, 100, 2),
        new_order(symbol(), 3, 9, Side::Sell, 100, 3),
    ]);
    let executions = vec![
        fill(
            20,
            symbol(),
            2,
            1,
            Side::Sell,
            100,
            2,
            ExecKind::PartiallyFilled,
        ),
        fill(21, symbol(), 3, 1, Side::Sell, 100, 3, ExecKind::Filled),
    ];
    let report = build_ledger(PARTITION, &orders, &executions, None);
    report.check_invariants().expect("both trades balance");

    assert_eq!(
        report
            .entries
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![20, 21],
    );
    let maker = account(&report, 7, Asset::Cash).unwrap();
    assert_eq!(maker.balance, -500);
    assert_eq!(maker.entries, 2, "two postings, one per trade");
    assert_eq!(
        account(&report, 7, Asset::Position(symbol()))
            .unwrap()
            .balance,
        5
    );
    assert_eq!(account(&report, 8, Asset::Cash).unwrap().balance, 200);
    assert_eq!(account(&report, 9, Asset::Cash).unwrap().balance, 300);
    assert_eq!(
        account(&report, 8, Asset::Position(symbol()))
            .unwrap()
            .balance,
        -2
    );
    assert_eq!(
        account(&report, 9, Asset::Position(symbol()))
            .unwrap()
            .balance,
        -3
    );
}

#[test]
fn an_unattributed_side_posts_nothing_and_counts_the_gap() {
    // Maker id 42 is absent from the orders log: the aggressor is known but the
    // trade must not be half-posted.
    let orders = indexed(vec![new_order(symbol(), 2, 8, Side::Sell, 100, 3)]);
    let report = build_ledger(
        PARTITION,
        &orders,
        &[fill(
            10,
            symbol(),
            2,
            42,
            Side::Sell,
            100,
            3,
            ExecKind::Filled,
        )],
        None,
    );
    assert!(report.entries.is_empty(), "no half-posted trade");
    assert!(report.accounts.is_empty());
    assert_eq!(report.unresolved_maker_fills, 1);
    assert_eq!(report.unresolved_aggressor_fills, 0);
    report.check_invariants().expect("an empty ledger balances");

    // Aggressor id 99 is absent: the maker leg alone must not be posted.
    let orders = indexed(vec![new_order(symbol(), 1, 7, Side::Buy, 100, 3)]);
    let report = build_ledger(
        PARTITION,
        &orders,
        &[fill(
            11,
            symbol(),
            99,
            1,
            Side::Sell,
            100,
            3,
            ExecKind::Filled,
        )],
        None,
    );
    assert!(report.entries.is_empty());
    assert_eq!(report.unresolved_maker_fills, 0);
    assert_eq!(report.unresolved_aggressor_fills, 1);
    report.check_invariants().expect("an empty ledger balances");

    // A zero maker id names no counterparty at all.
    let report = build_ledger(
        PARTITION,
        &orders,
        &[fill(
            12,
            symbol(),
            1,
            0,
            Side::Buy,
            100,
            3,
            ExecKind::Filled,
        )],
        None,
    );
    assert!(report.entries.is_empty());
    assert_eq!(report.unresolved_maker_fills, 1);
    assert_eq!(report.unresolved_aggressor_fills, 0);
    report.check_invariants().expect("an empty ledger balances");

    // Both ids absent: both counters move, nothing posts.
    let report = build_ledger(
        PARTITION,
        &orders,
        &[fill(
            13,
            symbol(),
            98,
            99,
            Side::Sell,
            100,
            3,
            ExecKind::Filled,
        )],
        None,
    );
    assert!(report.entries.is_empty());
    assert_eq!(report.unresolved_maker_fills, 1);
    assert_eq!(report.unresolved_aggressor_fills, 1);
    report.check_invariants().expect("an empty ledger balances");
}

#[test]
fn non_fills_zero_qty_and_zero_price_post_nothing() {
    let orders = indexed(vec![
        new_order(symbol(), 1, 7, Side::Buy, 100, 5),
        new_order(symbol(), 2, 8, Side::Sell, 100, 5),
        new_order(symbol(), 4, 9, Side::Buy, 0, 5),
    ]);
    let executions = vec![
        fill(1, symbol(), 2, 1, Side::Sell, 100, 0, ExecKind::Filled),
        fill(2, symbol(), 2, 1, Side::Sell, 100, 0, ExecKind::Rejected),
        fill(3, symbol(), 2, 1, Side::Sell, 100, 0, ExecKind::Cancelled),
        fill(4, symbol(), 4, 1, Side::Buy, 0, 5, ExecKind::Filled),
    ];
    let report = build_ledger(PARTITION, &orders, &executions, None);
    assert!(report.entries.is_empty());
    assert!(report.accounts.is_empty());
    report
        .check_invariants()
        .expect("nothing posted, nothing to break");
}

/// Deterministic xorshift so the workload is reproducible without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Complete-trade workload: every order id in the fills is in the orders log.
fn complete_workload(trades: usize) -> (Vec<Command>, Vec<ExecutionReport>) {
    let mut rng = Rng(0x5eed_1234_9abc_def1);
    let universes = symbols();
    let mut orders = Vec::with_capacity(trades * 2);
    let mut executions = Vec::with_capacity(trades);
    for trade in 0..trades {
        let symbol = universes[trade % universes.len()];
        let aggressor = rng.below(8);
        let maker = (aggressor + 1 + rng.below(7)) % 8;
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let price = 100 + rng.below(50) as i64;
        let qty = 1 + rng.below(10);
        let aggressor_id = 1_000_000 + (trade as u128) * 2;
        let maker_id = aggressor_id + 1;
        orders.push(new_order(symbol, aggressor_id, aggressor, side, price, qty));
        orders.push(new_order(
            symbol,
            maker_id,
            maker,
            side.opposite(),
            price,
            qty,
        ));
        executions.push(fill(
            (trade as u64) + 1,
            symbol,
            aggressor_id,
            maker_id,
            side,
            price,
            qty,
            ExecKind::Filled,
        ));
    }
    (orders, executions)
}

#[test]
fn ledger_reconciles_with_positions_over_the_same_complete_trades() {
    let (orders, executions) = complete_workload(240);
    let orders = indexed(orders);
    let ledger = build_ledger(PARTITION, &orders, &executions, None);
    ledger.check_invariants().expect("the ledger balances");
    let positions = build_positions(PARTITION, &orders, &executions, None);
    assert_eq!(ledger.entries.len(), executions.len());
    assert_eq!(positions.unresolved_maker_fills, 0);
    assert_eq!(positions.unresolved_aggressor_fills, 0);

    // Cash: the ledger's per-participant cash balance is the positions cash
    // flow (a buy pays, a sell receives; the same trade_cents value).
    for position in &positions.participants {
        let cash = account(&ledger, position.participant.0, Asset::Cash)
            .expect("every positions participant has cash postings");
        assert_eq!(
            cash.balance, position.cash_cents,
            "cash mismatch for participant {}",
            position.participant.0
        );
        // Shares: each (participant, symbol) position balance is net_qty.
        for row in &position.symbols {
            let balance = account(&ledger, position.participant.0, Asset::Position(row.symbol))
                .expect("every positions row has a position account")
                .balance;
            assert_eq!(
                balance, row.net_qty,
                "position mismatch for participant {} {}",
                position.participant.0, row.symbol
            );
        }
    }

    // counts: positions counts legs, the ledger counts two legs per entry.
    let legs: u64 = positions
        .participants
        .iter()
        .map(|position| position.fills)
        .sum();
    assert_eq!(legs, (ledger.entries.len() as u64) * 2);

    // No account exists that positions does not know about.
    for account in &ledger.accounts {
        let position = positions
            .participants
            .iter()
            .find(|position| position.participant == account.participant)
            .expect("the ledger participant is in positions");
        match account.asset {
            Asset::Cash => {}
            Asset::Position(symbol) => assert!(
                position.symbols.iter().any(|row| row.symbol == symbol),
                "ledger has a position row positions does not"
            ),
        }
    }
}

#[test]
fn random_workload_conserves_value_and_every_account_folds() {
    let mut rng = Rng(0xdead_beef_0bad_f00d);
    let universes = symbols();
    let mut orders = Vec::new();
    let mut executions = Vec::new();
    let mut posted = 0u64;
    let mut unresolved_maker = 0u64;
    let mut unresolved_aggressor = 0u64;
    for trade in 0..1_000u64 {
        let symbol = universes[(trade % 3) as usize];
        let aggressor_participant = rng.below(8);
        let maker_participant = (aggressor_participant + 1 + rng.below(7)) % 8;
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let price = 50 + rng.below(100) as i64;
        let qty = 1 + rng.below(20);
        let aggressor_id = 1_000_000 + (trade as u128) * 2;
        let maker_id = aggressor_id + 1;

        // ~4% of each side is deliberately absent from the orders log.
        let aggressor_known = rng.below(25) != 0;
        let maker_known = rng.below(25) != 0;
        if aggressor_known {
            orders.push(new_order(
                symbol,
                aggressor_id,
                aggressor_participant,
                side,
                price,
                qty,
            ));
        }
        if maker_known {
            orders.push(new_order(
                symbol,
                maker_id,
                maker_participant,
                side.opposite(),
                price,
                qty,
            ));
        }
        if !aggressor_known {
            unresolved_aggressor += 1;
        }
        if !maker_known {
            unresolved_maker += 1;
        }
        if aggressor_known && maker_known {
            posted += 1;
        }
        executions.push(fill(
            trade + 1,
            symbol,
            aggressor_id,
            maker_id,
            side,
            price,
            qty,
            if trade % 3 == 0 {
                ExecKind::PartiallyFilled
            } else {
                ExecKind::Filled
            },
        ));
    }
    let orders = indexed(orders);
    let report = build_ledger(PARTITION, &orders, &executions, None);
    report.check_invariants().expect("1,000 fills conserve");

    assert_eq!(report.entries.len() as u64, posted);
    assert_eq!(report.unresolved_maker_fills, unresolved_maker);
    assert_eq!(report.unresolved_aggressor_fills, unresolved_aggressor);
    assert_eq!(
        report.accounts.len(),
        fold_entries(&report.entries).len(),
        "every account is a folded posting key"
    );

    // Every balance and posting count matches the independent fold.
    let folded = fold_entries(&report.entries);
    for account in &report.accounts {
        let (balance, count) = folded[&(account.participant, account.asset)];
        assert_eq!(account.balance, balance);
        assert_eq!(account.entries, count);
    }

    // Report-wide conservation: cash sums to zero, each symbol's shares sum to
    // zero.
    let mut cash = 0i128;
    let mut shares: BTreeMap<Symbol, i128> = BTreeMap::new();
    for entry in &report.entries {
        for posting in &entry.postings {
            match posting.asset {
                Asset::Cash => cash += posting.delta,
                Asset::Position(symbol) => *shares.entry(symbol).or_default() += posting.delta,
            }
        }
    }
    assert_eq!(cash, 0);
    assert_eq!(shares.len(), 3);
    for symbol in symbols() {
        assert_eq!(shares[&symbol], 0, "{symbol} shares are not conserved");
    }
}

#[test]
fn the_same_input_twice_and_a_reversed_execution_log_agree() {
    let (orders, executions) = complete_workload(120);
    let orders = indexed(orders);
    let first = build_ledger(PARTITION, &orders, &executions, None);
    let second = build_ledger(PARTITION, &orders, &executions, None);
    assert_eq!(first, second, "the projection is deterministic");

    let mut reversed = executions;
    reversed.reverse();
    let backwards = build_ledger(PARTITION, &orders, &reversed, None);
    assert_eq!(
        first, backwards,
        "seq is the canonical order, so input order does not matter"
    );
    assert!(
        first
            .entries
            .windows(2)
            .all(|pair| pair[0].seq < pair[1].seq)
    );
}

#[test]
fn symbol_filter_restricts_the_scan() {
    let orders = indexed(vec![
        new_order(symbol(), 1, 7, Side::Buy, 100, 2),
        new_order(symbol(), 2, 8, Side::Sell, 100, 2),
        new_order(eth(), 3, 9, Side::Buy, 50, 4),
        new_order(eth(), 4, 10, Side::Sell, 50, 4),
    ]);
    let executions = vec![
        fill(1, symbol(), 2, 1, Side::Sell, 100, 2, ExecKind::Filled),
        fill(2, eth(), 4, 3, Side::Sell, 50, 4, ExecKind::Filled),
    ];
    let all = build_ledger(PARTITION, &orders, &executions, None);
    assert_eq!(all.entries.len(), 2);
    all.check_invariants().expect("both trades balance");

    let filtered = build_ledger(PARTITION, &orders, &executions, Some(&[eth()]));
    assert_eq!(filtered.entries.len(), 1);
    assert_eq!(filtered.entries[0].seq, 2);
    assert_eq!(filtered.entries[0].symbol, eth());
    assert_eq!(filtered.entries[0].buyer, ParticipantId(9));
    assert_eq!(filtered.entries[0].seller, ParticipantId(10));
    assert_eq!(filtered.unresolved_maker_fills, 0);
    assert_eq!(filtered.unresolved_aggressor_fills, 0);
    filtered.check_invariants().expect("still balanced");
}

#[test]
fn ledger_retains_one_participant_without_breaking_the_entries() {
    let (orders, executions) = resting_buy_fixture();
    let mut report = build_ledger(PARTITION, &indexed(orders), &executions, None);
    report.retain_participant(ParticipantId(8));
    assert_eq!(report.accounts.len(), 2);
    assert!(
        report
            .accounts
            .iter()
            .all(|account| account.participant == ParticipantId(8))
    );
    assert_eq!(report.entries.len(), 1);
    report
        .check_invariants()
        .expect("the filtered view still balances");
}

#[tokio::test]
async fn ledger_with_journal_is_read_only_and_emits_the_report() {
    let journal = Journal::memory();
    let (orders, executions) = resting_buy_fixture();
    let order_payloads: Vec<Vec<u8>> = orders.iter().map(Command::encode).collect();
    let execution_payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Orders, &order_payloads).await;
    append(&journal, Topic::Executions, &execution_payloads).await;
    let orders_before = topic_len(&journal, Topic::Orders).await;
    let executions_before = topic_len(&journal, Topic::Executions).await;

    let mut args = ledger_args(PARTITION);
    args.json = true;
    let report = ledger_with_journal(args, &journal)
        .await
        .expect("a readable journal produces a report");

    assert_eq!(report.partition, PARTITION);
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.entries[0].seq, 10);
    assert_eq!(report.entries[0].buyer, ParticipantId(7));
    assert_eq!(report.entries[0].seller, ParticipantId(8));
    assert_eq!(report.accounts.len(), 4);

    // JSON shape: stable enum tagging for assets and explicit postings.
    let json = serde_json::to_value(&report).expect("the report serializes");
    assert_eq!(json["partition"], 1);
    assert_eq!(json["unresolved_maker_fills"], 0);
    assert_eq!(json["unresolved_aggressor_fills"], 0);
    assert_eq!(json["entries"][0]["seq"], 10);
    assert_eq!(json["entries"][0]["symbol"], "BTC-USD");
    assert_eq!(json["entries"][0]["price_cents"], 100);
    assert_eq!(json["entries"][0]["qty"], 3);
    assert_eq!(json["entries"][0]["buyer"], 7);
    assert_eq!(json["entries"][0]["seller"], 8);
    assert_eq!(json["entries"][0]["postings"][0]["asset"], "cash");
    assert_eq!(json["entries"][0]["postings"][0]["delta"], -300);
    assert_eq!(
        json["entries"][0]["postings"][2]["asset"],
        serde_json::json!({"position": "BTC-USD"})
    );
    assert_eq!(json["accounts"][0]["participant"], 7);
    assert_eq!(json["accounts"][0]["asset"], "cash");
    assert_eq!(json["accounts"][0]["balance"], -300);
    assert_eq!(json["accounts"][0]["entries"], 1);
    assert_eq!(
        json["accounts"][1]["asset"],
        serde_json::json!({"position": "BTC-USD"})
    );
    assert_eq!(json["accounts"][1]["balance"], 3);
    assert_eq!(json["accounts"][2]["participant"], 8);
    assert_eq!(json["accounts"][2]["balance"], 300);

    // Read-only: both topics keep every record and the ledger consumers never
    // commit.
    assert_eq!(topic_len(&journal, Topic::Orders).await, orders_before);
    assert_eq!(
        topic_len(&journal, Topic::Executions).await,
        executions_before
    );
    for (topic, consumer) in [
        (Topic::Orders, "whippet-ledger-orders-p1"),
        (Topic::Executions, "whippet-ledger-executions-p1"),
    ] {
        assert_eq!(
            journal
                .committed(topic, PARTITION, consumer)
                .await
                .expect("read ledger consumer"),
            None,
            "ledger moved a consumer offset on {topic}"
        );
    }
}

#[tokio::test]
async fn ledger_skips_undecodable_records_and_still_reports() {
    let journal = Journal::memory();
    let (orders, executions) = resting_buy_fixture();
    let mut order_payloads: Vec<Vec<u8>> = vec![vec![0xff; 8]];
    order_payloads.extend(orders.iter().map(Command::encode));
    let mut execution_payloads: Vec<Vec<u8>> = vec![vec![0x00; 4]];
    execution_payloads.extend(executions.iter().map(ExecutionReport::encode));
    append(&journal, Topic::Orders, &order_payloads).await;
    append(&journal, Topic::Executions, &execution_payloads).await;

    let report = ledger_with_journal(ledger_args(PARTITION), &journal)
        .await
        .expect("undecodable records are counted, not fatal");
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.accounts.len(), 4);
    report
        .check_invariants()
        .expect("the report still balances");
}

#[tokio::test]
async fn ledger_participant_filter_keeps_unresolved_counters_scan_wide() {
    let journal = Journal::memory();
    let orders = [
        new_order(symbol(), 1, 7, Side::Buy, 100, 5),
        new_order(symbol(), 2, 8, Side::Sell, 100, 3),
        new_order(symbol(), 3, 9, Side::Sell, 100, 1),
    ];
    let executions = [
        fill(10, symbol(), 2, 1, Side::Sell, 100, 3, ExecKind::Filled),
        // The maker id 77 is absent, so this trade is not posted.
        fill(11, symbol(), 3, 77, Side::Sell, 100, 1, ExecKind::Filled),
    ];
    let order_payloads: Vec<Vec<u8>> = orders.iter().map(Command::encode).collect();
    let execution_payloads: Vec<Vec<u8>> = executions.iter().map(ExecutionReport::encode).collect();
    append(&journal, Topic::Orders, &order_payloads).await;
    append(&journal, Topic::Executions, &execution_payloads).await;

    let mut args = ledger_args(PARTITION);
    args.participant = Some(7);
    let report = ledger_with_journal(args, &journal)
        .await
        .expect("the participant filter succeeds");
    assert_eq!(report.accounts.len(), 2);
    assert!(
        report
            .accounts
            .iter()
            .all(|account| account.participant == ParticipantId(7))
    );
    assert_eq!(report.entries.len(), 1);
    assert_eq!(report.unresolved_maker_fills, 1, "scan-wide counter kept");
    report
        .check_invariants()
        .expect("the filtered view balances");

    let mut args = ledger_args(PARTITION);
    args.participant = Some(999);
    let report = ledger_with_journal(args, &journal)
        .await
        .expect("an absent participant is an empty success");
    assert!(report.accounts.is_empty());
    assert_eq!(report.unresolved_maker_fills, 1);
    report
        .check_invariants()
        .expect("an empty account list balances");
}

#[tokio::test]
async fn ledger_empty_journal_is_a_successful_empty_report() {
    let journal = Journal::memory();
    let report = ledger_with_journal(ledger_args(PARTITION), &journal)
        .await
        .expect("an empty journal is a success, not an error");
    assert_eq!(
        report,
        LedgerReport {
            partition: PARTITION,
            entries: Vec::new(),
            accounts: Vec::new(),
            unresolved_maker_fills: 0,
            unresolved_aggressor_fills: 0,
        }
    );
}

#[tokio::test]
async fn ledger_rejects_partition_zero_before_reading() {
    let journal = Journal::memory();
    let error = ledger_with_journal(ledger_args(0), &journal)
        .await
        .expect_err("partition 0 is a usage error");
    assert!(error.to_string().contains("--partition"));
}

#[test]
fn ledger_args_defaults_and_overrides_parse() {
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        ledger: LedgerArgs,
    }

    let cli = Cli::parse_from(["test"]);
    assert_eq!(cli.ledger.iggy_addr, "127.0.0.1:8090");
    assert_eq!(cli.ledger.iggy_user, "iggy");
    assert_eq!(cli.ledger.iggy_password, "iggy");
    assert!(cli.ledger.iggy_password_file.is_none());
    assert_eq!(cli.ledger.partition, 1);
    assert!(cli.ledger.symbols.is_empty());
    assert!(cli.ledger.participant.is_none());
    assert!(!cli.ledger.json);

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
    assert_eq!(cli.ledger.iggy_addr, "127.0.0.1:9999");
    assert_eq!(cli.ledger.partition, 5);
    assert_eq!(cli.ledger.symbols, vec![symbol(), eth()]);
    assert_eq!(cli.ledger.participant, Some(7));
    assert!(cli.ledger.json);
    assert_eq!(
        cli.ledger.iggy_password_file,
        Some(std::path::PathBuf::from("/tmp/iggy-password"))
    );
}
