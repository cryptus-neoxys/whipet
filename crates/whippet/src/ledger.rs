// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic double-entry cash/position ledger over one partition's logs.
//!
//! `whippet ledger` folds one partition's `orders` and `executions` logs into
//! balanced journal entries. Every attributed trade becomes one entry with
//! exactly four postings — buyer cash, seller cash, buyer position and seller
//! position — so cash and that symbol's position each sum to zero within the
//! entry. Account balances are the fold of those postings.
//!
//! The logs store each fill price as an integer **ladder tick index**, not
//! quote cents, so [`build_ledger`] converts with the instrument's tick size:
//! `price_ticks * tick_size_cents * qty`. The entry keeps both the raw
//! `price_ticks` and the `tick_size_cents` used, so a posted value can be
//! audited without the config. A symbol with no tick size, or a non-positive
//! one, is an error — never a silent factor of one.
//!
//! This is a **simulation ledger, not clearing or settlement**: posting is T+0,
//! there are no fees, funding, deposits, withdrawals, margin calls, or
//! settlement dates, and an account may run a negative cash balance (the
//! simulation does not model credit). Cash balances are quote cents; position
//! balances are share counts. It reads only the decoded logs and never writes.
//!
//! Participant attribution is the same as [`crate::positions`]: an execution
//! report carries no participant, so a leg is attributed through the
//! `NewOrder` command that created its order id, and the first `NewOrder` per
//! id wins. A trade is posted **all or nothing**: if either side's order id is
//! missing from the decoded orders log, neither leg is posted and the
//! unresolved side is counted. A half-posted trade would break conservation,
//! and a ledger that silently drops one side is worse than one that reports a
//! gap.
//!
//! The module is the pure half: it reads only its arguments — no wall clock,
//! no network, no journal, no logging — so the same decoded logs always
//! produce the same report. The engine stamps `ExecutionReport.seq` per
//! symbol, not per partition, so an entry is identified by `(symbol, seq)` and
//! ordered by `(cause_offset, seq)` — the output order of the logs. The report
//! therefore does not depend on the order in which the decoded executions are
//! handed in. The journal reads live in [`crate::cli`] behind the [`Journal`]
//! seam.
//!
//! [`Journal`]: whippet_journal::Journal

use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;
use whippet_core::ids::{OrderId, ParticipantId, Symbol};
use whippet_core::protocol::{Command, ExecKind, ExecutionReport, Side};

use crate::positions::{TickSizeError, TickSizes, order_index};

/// Printed on every ledger run, to stderr so `--json` output stays
/// machine-readable.
pub const LEDGER_NOTICE: &str = "simulation ledger reconstructed from the orders and executions logs: \
     log tick prices converted to quote cents with the configured tick size \
     (--tick-size-cents overrides the built-in config); T+0 posting, no fees, funding, \
     deposits, withdrawals or settlement dates; read-only";

/// What a posting moves: cash, or one symbol's position.
///
/// The derived `Ord` is the output order: cash sorts before every position,
/// and positions sort by symbol. Orders are stable, so the account list is
/// deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Asset {
    Cash,
    Position(Symbol),
}

impl fmt::Display for Asset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cash => f.write_str("cash"),
            Self::Position(symbol) => write!(f, "{symbol}"),
        }
    }
}

/// One signed movement of one account.
///
/// Postings are the atomic unit of the ledger: no entry changes an account
/// except through a posting, and every entry's postings balance per asset.
/// A cash delta is in quote cents; a position delta is a share count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Posting {
    pub participant: ParticipantId,
    pub asset: Asset,
    pub delta: i128,
}

/// One attributed trade, balanced across its four legs.
///
/// `seq` is the `ExecutionReport.seq` of the fill. The engine stamps it per
/// symbol, so the entry identity is `(symbol, seq)`, not `seq` alone.
/// `cause_offset` is the orders-log offset that produced the report, so an
/// entry can be traced back to its input command.
///
/// `price_ticks` is the fill price exactly as the engine emitted it, a ladder
/// tick index. `tick_size_cents` is the instrument's tick size used for the
/// conversion, and `price_cents` is `price_ticks * tick_size_cents`. Keeping
/// all three makes the entry auditable from the report alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LedgerEntry {
    pub seq: u64,
    pub cause_offset: u64,
    pub symbol: Symbol,
    /// The fill price as an integer ladder tick index (`ExecutionReport.price`).
    pub price_ticks: i64,
    /// The instrument's tick size in quote cents, applied exactly once.
    pub tick_size_cents: i64,
    /// The fill price in quote cents: `price_ticks * tick_size_cents`.
    pub price_cents: i128,
    pub qty: u64,
    pub buyer: ParticipantId,
    pub seller: ParticipantId,
    /// Exactly four, in quote cents / shares: buyer cash
    /// `-price_ticks*tick_size_cents*qty`, seller cash `+...`, buyer position
    /// `+qty`, seller position `-qty`.
    pub postings: Vec<Posting>,
}

/// One account's balance and the number of postings that produced it.
///
/// `entries` counts postings, not ledger entries: a trade posts once to each
/// of its four accounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountBalance {
    pub participant: ParticipantId,
    pub asset: Asset,
    pub balance: i128,
    pub entries: u64,
}

/// The ledger projection for one partition.
///
/// `entries` is sorted by `(cause_offset, seq)`, the logs' output order, and
/// `accounts` is sorted by participant and then asset. Both unresolved
/// counters are scan-wide: they describe gaps in the log, not one
/// participant's accounts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LedgerReport {
    pub partition: u32,
    pub entries: Vec<LedgerEntry>,
    pub accounts: Vec<AccountBalance>,
    /// Trades whose aggressor id is known but whose maker id is not in the
    /// decoded orders log (or is zero), so the trade was not posted.
    pub unresolved_maker_fills: u64,
    /// Trades whose aggressor id is not in the decoded orders log, so the
    /// trade was not posted.
    pub unresolved_aggressor_fills: u64,
}

impl LedgerReport {
    /// Restrict the account view to one participant id.
    ///
    /// Covers the `--participant` filter. Entries stay whole: each one keeps
    /// its four postings, so the report stays balanced and an entry can still
    /// be read as the trade it is. Only the account list is filtered, which
    /// also means the two unresolved counters stay scan-wide, exactly as the
    /// positions report's do.
    pub fn retain_participant(&mut self, participant: ParticipantId) {
        self.accounts
            .retain(|account| account.participant == participant);
    }

    /// Prove the report is a balanced double-entry ledger.
    ///
    /// Checks, in order:
    ///
    /// - every entry has a non-zero `seq`, positive `qty`, positive
    ///   `price_ticks` and positive `tick_size_cents`, and its quoted
    ///   `price_cents` equals `price_ticks * tick_size_cents`;
    /// - entries are strictly ordered by `(cause_offset, seq)`, so no fill
    ///   repeats and the list follows the logs' output order;
    /// - each symbol's `seq` increases along that order. `seq` is stamped per
    ///   symbol by the engine, so two symbols may legitimately carry the same
    ///   `seq`; the entry identity is `(symbol, seq)`;
    /// - every entry has exactly four postings, every posting has a non-zero
    ///   delta, the postings match the trade's four documented legs, and the
    ///   postings sum to zero **per asset** (cash and that symbol's position);
    /// - report-wide, cash postings sum to zero and each symbol's position
    ///   postings sum to zero, so the scan as a whole conserves value and
    ///   shares;
    /// - every account is sorted by participant then asset, appears once, and
    ///   its `balance` and `entries` count equal a direct fold of the postings.
    ///
    /// Account completeness is deliberately not required: after
    /// [`LedgerReport::retain_participant`] the account list is a subset while
    /// the entries stay whole. The checks above still hold for that subset.
    ///
    /// Account and report-total accumulation uses checked `i128` arithmetic, so
    /// an overflow is a broken invariant rather than a wrapped balance. The
    /// price and trade-value products saturate at the `i128` limits, matching
    /// the projection: one absurd record saturates instead of wrapping into a
    /// plausible number.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut previous: Option<(u64, Symbol, u64)> = None;
        // Per-symbol sequence walk: `seq` restarts at 1 for each instrument.
        let mut seq_by_symbol: BTreeMap<Symbol, u64> = BTreeMap::new();
        let mut cash_total: i128 = 0;
        let mut position_totals: BTreeMap<Symbol, i128> = BTreeMap::new();
        // Every posting folded by account, used to prove the account rows.
        let mut folded: BTreeMap<(ParticipantId, Asset), (i128, u64)> = BTreeMap::new();

        for entry in &self.entries {
            if entry.seq == 0 {
                return Err(
                    "entry with seq 0: a fill report's seq is the entry identity, so it cannot be zero"
                        .to_string(),
                );
            }
            let key = (entry.cause_offset, entry.symbol, entry.seq);
            if let Some(previous) = previous
                && key <= previous
            {
                return Err(format!(
                    "duplicate or out-of-order entry (offset {}, {}, seq {}): it is not after the previous entry (offset {}, {}, seq {})",
                    key.0, key.1, key.2, previous.0, previous.1, previous.2
                ));
            }
            previous = Some(key);
            if let Some(previous_seq) = seq_by_symbol.get(&entry.symbol)
                && entry.seq <= *previous_seq
            {
                return Err(format!(
                    "duplicate or out-of-order fill seq {} for {}: the previous {} entry used seq {previous_seq}",
                    entry.seq, entry.symbol, entry.symbol
                ));
            }
            seq_by_symbol.insert(entry.symbol, entry.seq);

            if entry.qty == 0 {
                return Err(format!(
                    "entry {} has qty 0: a zero-quantity fill is skipped, never posted",
                    entry.seq
                ));
            }
            if entry.price_ticks <= 0 {
                return Err(format!(
                    "entry {} has non-positive tick price {}",
                    entry.seq, entry.price_ticks
                ));
            }
            if entry.tick_size_cents <= 0 {
                return Err(format!(
                    "entry {} has non-positive tick size {}",
                    entry.seq, entry.tick_size_cents
                ));
            }
            let price_cents =
                i128::from(entry.price_ticks).saturating_mul(i128::from(entry.tick_size_cents));
            if entry.price_cents != price_cents {
                return Err(format!(
                    "entry {} price {} cents is not {} ticks at {} cents per tick",
                    entry.seq, entry.price_cents, entry.price_ticks, entry.tick_size_cents
                ));
            }
            if entry.postings.len() != 4 {
                return Err(format!(
                    "entry {} has {} posting(s), expected exactly 4",
                    entry.seq,
                    entry.postings.len()
                ));
            }

            let value = entry.price_cents.saturating_mul(i128::from(entry.qty));
            let qty = i128::from(entry.qty);
            let position = Asset::Position(entry.symbol);
            let expected = [
                Posting {
                    participant: entry.buyer,
                    asset: Asset::Cash,
                    delta: -value,
                },
                Posting {
                    participant: entry.seller,
                    asset: Asset::Cash,
                    delta: value,
                },
                Posting {
                    participant: entry.buyer,
                    asset: position,
                    delta: qty,
                },
                Posting {
                    participant: entry.seller,
                    asset: position,
                    delta: -qty,
                },
            ];

            let mut by_asset: BTreeMap<Asset, i128> = BTreeMap::new();
            for posting in &entry.postings {
                if posting.delta == 0 {
                    return Err(format!(
                        "entry {} posts a zero delta for participant {}",
                        entry.seq, posting.participant.0
                    ));
                }
                let sum = by_asset.entry(posting.asset).or_default();
                *sum = sum
                    .checked_add(posting.delta)
                    .ok_or_else(|| format!("entry {} postings overflow i128", entry.seq))?;
                match posting.asset {
                    Asset::Cash => {
                        cash_total = cash_total.checked_add(posting.delta).ok_or_else(|| {
                            format!("report cash total overflows at entry {}", entry.seq)
                        })?;
                    }
                    Asset::Position(symbol) => {
                        let total = position_totals.entry(symbol).or_default();
                        *total = total.checked_add(posting.delta).ok_or_else(|| {
                            format!("{symbol} position total overflows at entry {}", entry.seq)
                        })?;
                    }
                }
                let account = folded
                    .entry((posting.participant, posting.asset))
                    .or_insert((0, 0));
                account.0 = account.0.checked_add(posting.delta).ok_or_else(|| {
                    format!(
                        "account {} {} overflows at entry {}",
                        posting.participant.0, posting.asset, entry.seq
                    )
                })?;
                account.1 = account.1.saturating_add(1);
            }

            if by_asset.len() != 2
                || !by_asset.contains_key(&Asset::Cash)
                || !by_asset.contains_key(&position)
            {
                return Err(format!(
                    "entry {} postings must touch cash and {} only",
                    entry.seq, entry.symbol
                ));
            }
            for (asset, sum) in &by_asset {
                if *sum != 0 {
                    return Err(format!(
                        "entry {} does not balance in {asset}: postings sum to {sum}, expected 0",
                        entry.seq
                    ));
                }
            }

            let mut actual = entry.postings.clone();
            actual.sort_unstable();
            let mut expected = expected.to_vec();
            expected.sort_unstable();
            if actual != expected {
                return Err(format!(
                    "entry {} postings do not match the trade's four legs",
                    entry.seq
                ));
            }
        }

        if cash_total != 0 {
            return Err(format!(
                "report cash postings sum to {cash_total}, expected 0"
            ));
        }
        for (symbol, total) in &position_totals {
            if *total != 0 {
                return Err(format!(
                    "report {symbol} position postings sum to {total}, expected 0"
                ));
            }
        }

        let mut previous_account: Option<(ParticipantId, Asset)> = None;
        for account in &self.accounts {
            let key = (account.participant, account.asset);
            if let Some(previous) = previous_account
                && key <= previous
            {
                return Err(format!(
                    "account {} {} is out of order or duplicated",
                    account.participant.0, account.asset
                ));
            }
            previous_account = Some(key);
            let Some((balance, postings)) = folded.get(&key) else {
                return Err(format!(
                    "account {} {} has no postings in any entry",
                    account.participant.0, account.asset
                ));
            };
            if account.balance != *balance {
                return Err(format!(
                    "account {} {} balance {} does not equal its postings ({balance})",
                    account.participant.0, account.asset, account.balance
                ));
            }
            if account.entries != *postings {
                return Err(format!(
                    "account {} {} counts {} entr(ies), its postings are {}",
                    account.participant.0, account.asset, account.entries, postings
                ));
            }
        }

        Ok(())
    }
}

/// Build one partition's ledger from already-decoded logs.
///
/// `orders` is every decoded command on the partition with its orders-topic
/// offset; `executions` is every decoded execution report. Both are filtered
/// by `symbols` when it is `Some`. `tick_sizes` converts each fill's integer
/// tick price into quote cents; a posted trade whose symbol has no tick size,
/// or a non-positive one, is an error rather than a silent factor of one.
///
/// Attribution and selection rules:
///
/// - Only `filled` and `partially_filled` reports with a positive quantity,
///   a positive tick price and a non-zero `seq` are posted. The remaining kinds
///   do not move balances, and a zero or negative value would create a
///   zero-delta or nonsensical posting that the invariants reject.
/// - The aggressor leg is `report.order_id` with `report.side`; the maker leg
///   is `report.maker_order_id` with the opposite side, at the same price and
///   quantity, because the engine emits one report per trade and never a
///   separate maker fill.
/// - The buyer is the buy leg and the seller the sell leg, so the maker is
///   the buyer when the maker bought.
/// - **All or nothing**: if either order id is absent from the order map (or
///   the maker id is zero) the trade is not posted at all and the missing
///   side(s) are counted. A one-sided posting would break conservation.
/// - A fill is identified by `(symbol, seq)`: the engine stamps `seq` per
///   symbol, so two instruments may legitimately carry the same number. A
///   malformed log that repeats a pair keeps the first entry in the canonical
///   `(cause_offset, seq)` order, so the result is deterministic and
///   `check_invariants` holds.
pub fn build_ledger(
    partition: u32,
    orders: &[(u64, Command)],
    executions: &[ExecutionReport],
    symbols: Option<&[Symbol]>,
    tick_sizes: &TickSizes,
) -> Result<LedgerReport, TickSizeError> {
    let allowed = |symbol: Symbol| symbols.is_none_or(|symbols| symbols.contains(&symbol));
    let order_info = order_index(orders, allowed);

    let mut candidates: Vec<LedgerEntry> = Vec::new();
    let mut unresolved_maker_fills = 0u64;
    let mut unresolved_aggressor_fills = 0u64;

    for report in executions {
        if !matches!(report.kind, ExecKind::Filled | ExecKind::PartiallyFilled)
            || !allowed(report.symbol)
            || report.qty.0 == 0
            || report.price.0 <= 0
            || report.seq.0 == 0
        {
            continue;
        }

        let aggressor = order_info.get(&report.order_id).copied();
        let maker = if report.maker_order_id == OrderId(0) {
            None
        } else {
            order_info.get(&report.maker_order_id).copied()
        };
        if aggressor.is_none() {
            unresolved_aggressor_fills = unresolved_aggressor_fills.saturating_add(1);
        }
        if maker.is_none() {
            unresolved_maker_fills = unresolved_maker_fills.saturating_add(1);
        }
        // All or nothing: count both gaps, then post only a fully attributed
        // trade. The two counters are independent — both can move on one
        // report.
        let (Some(aggressor), Some(maker)) = (aggressor, maker) else {
            continue;
        };

        let (buyer, seller) = match report.side {
            Side::Buy => (aggressor.participant, maker.participant),
            Side::Sell => (maker.participant, aggressor.participant),
        };
        // Price the trade only once both legs are known: an unresolved trade
        // moves no balances, so it does not need a tick size. The factor is
        // applied once, to the price; the value is that quoted price times the
        // quantity.
        let tick_size_cents = tick_sizes.require(report.symbol)?;
        let price_cents = tick_sizes.price_cents(report.symbol, report.price.0)?;
        let value = price_cents.saturating_mul(i128::from(report.qty.0));
        let qty = i128::from(report.qty.0);
        let position = Asset::Position(report.symbol);
        candidates.push(LedgerEntry {
            seq: report.seq.0,
            cause_offset: report.cause_offset,
            symbol: report.symbol,
            price_ticks: report.price.0,
            tick_size_cents,
            price_cents,
            qty: report.qty.0,
            buyer,
            seller,
            postings: vec![
                Posting {
                    participant: buyer,
                    asset: Asset::Cash,
                    delta: -value,
                },
                Posting {
                    participant: seller,
                    asset: Asset::Cash,
                    delta: value,
                },
                Posting {
                    participant: buyer,
                    asset: position,
                    delta: qty,
                },
                Posting {
                    participant: seller,
                    asset: position,
                    delta: -qty,
                },
            ],
        });
    }

    // Sort before de-duplicating so the surviving entry for a repeated
    // `(symbol, seq)` also does not depend on the input order. `cause_offset`
    // is partition-global, so `(cause_offset, seq)` reproduces the order in
    // which the engine appended the reports; sorting by `seq` alone would
    // interleave two symbols whose per-symbol counters happen to line up.
    candidates.sort_by_key(|entry| {
        (
            entry.cause_offset,
            entry.symbol,
            entry.seq,
            entry.price_cents,
            entry.qty,
            entry.buyer.0,
            entry.seller.0,
        )
    });
    let mut entries: Vec<LedgerEntry> = Vec::with_capacity(candidates.len());
    let mut seen: BTreeMap<Symbol, u64> = BTreeMap::new();
    for entry in candidates {
        // Strictly increasing per symbol: the first fill for a `(symbol, seq)`
        // wins, and the map also drops any out-of-order duplicate.
        match seen.get(&entry.symbol) {
            Some(previous) if entry.seq <= *previous => continue,
            _ => {}
        }
        seen.insert(entry.symbol, entry.seq);
        entries.push(entry);
    }

    // One fold produces the accounts; `BTreeMap` iteration is already in
    // participant-then-asset order, which is the documented output order.
    let mut folded: BTreeMap<(ParticipantId, Asset), (i128, u64)> = BTreeMap::new();
    for entry in &entries {
        for posting in &entry.postings {
            let account = folded
                .entry((posting.participant, posting.asset))
                .or_insert((0, 0));
            account.0 = account.0.saturating_add(posting.delta);
            account.1 = account.1.saturating_add(1);
        }
    }
    let accounts = folded
        .into_iter()
        .map(
            |((participant, asset), (balance, entries))| AccountBalance {
                participant,
                asset,
                balance,
                entries,
            },
        )
        .collect();

    Ok(LedgerReport {
        partition,
        entries,
        accounts,
        unresolved_maker_fills,
        unresolved_aggressor_fills,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::ids::{Price, Qty, Seq};
    use whippet_core::protocol::{OrderType, RejectReason};

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD").expect("static symbol")
    }

    fn eth() -> Symbol {
        Symbol::new("ETH-USD").expect("static symbol")
    }

    fn new_order(
        offset: u64,
        id: u128,
        participant: u64,
        side: Side,
        price: i64,
        qty: u64,
    ) -> (u64, Command) {
        (
            offset,
            Command::new_order(
                symbol(),
                side,
                OrderType::Limit,
                Price(price),
                Qty(qty),
                OrderId(id),
                ParticipantId(participant),
                offset,
            ),
        )
    }

    fn fill(
        seq: u64,
        order_id: u128,
        maker_order_id: u128,
        side: Side,
        price: i64,
        qty: u64,
    ) -> ExecutionReport {
        fill_on(
            symbol(),
            seq,
            seq,
            order_id,
            maker_order_id,
            side,
            price,
            qty,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_on(
        symbol: Symbol,
        seq: u64,
        cause_offset: u64,
        order_id: u128,
        maker_order_id: u128,
        side: Side,
        price: i64,
        qty: u64,
    ) -> ExecutionReport {
        ExecutionReport {
            kind: ExecKind::Filled,
            symbol,
            side,
            order_type: OrderType::Limit,
            price: Price(price),
            qty: Qty(qty),
            remaining: Qty(0),
            order_id: OrderId(order_id),
            maker_order_id: OrderId(maker_order_id),
            seq: Seq(seq),
            cause_offset,
            timestamp_ns: seq,
            reason: RejectReason::None,
            flags: 0,
        }
    }

    /// Participant 7 rests a buy for 5 at 100; participant 8 sells 3 into it.
    fn fixture() -> LedgerReport {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
        ];
        let executions = vec![fill(10, 2, 1, Side::Sell, 100, 3)];
        build_ledger(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes")
    }

    fn tick_sizes() -> TickSizes {
        TickSizes::from_iter([(symbol(), 1), (eth(), 1)])
    }

    #[test]
    fn the_tick_size_is_applied_once_to_the_postings() {
        // 3 ticks at 100 cents, qty 2: 600 cents. Applying the factor twice
        // would give 60,000 and not applying it gives 6.
        let tick_sizes = TickSizes::from_iter([(symbol(), 100)]);
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 3, 2),
            new_order(1, 2, 8, Side::Sell, 3, 2),
        ];
        let executions = vec![fill(10, 2, 1, Side::Sell, 3, 2)];
        let report = build_ledger(1, &orders, &executions, None, &tick_sizes)
            .expect("the fixture symbol has a tick size");
        report
            .check_invariants()
            .expect("the converted entry balances");

        let entry = &report.entries[0];
        assert_eq!(entry.price_ticks, 3);
        assert_eq!(entry.tick_size_cents, 100);
        assert_eq!(entry.price_cents, 300);
        assert_eq!(entry.postings[0].delta, -600);
        assert_eq!(entry.postings[1].delta, 600);
        assert_eq!(account_cash(&report, 7), -600);
        assert_eq!(account_cash(&report, 8), 600);
    }

    #[test]
    fn a_missing_or_non_positive_tick_size_is_an_error() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
        ];
        let executions = vec![fill(10, 2, 1, Side::Sell, 100, 3)];

        let error = build_ledger(1, &orders, &executions, None, &TickSizes::default())
            .expect_err("a missing tick size must not default to 1");
        assert_eq!(error, TickSizeError::Missing { symbol: symbol() });

        let zero = TickSizes::from_iter([(symbol(), 0)]);
        let error = build_ledger(1, &orders, &executions, None, &zero)
            .expect_err("a zero tick size is not a factor");
        assert_eq!(
            error,
            TickSizeError::NonPositive {
                symbol: symbol(),
                tick_size_cents: 0,
            }
        );
    }

    fn account_cash(report: &LedgerReport, participant: u64) -> i128 {
        report
            .accounts
            .iter()
            .find(|account| {
                account.participant == ParticipantId(participant) && account.asset == Asset::Cash
            })
            .expect("the participant has a cash account")
            .balance
    }

    #[test]
    fn a_complete_trade_balances_and_accounts_fold_from_the_entries() {
        let report = fixture();
        assert_eq!(report.entries.len(), 1);
        report.check_invariants().expect("the fixture is balanced");
        assert_eq!(report.accounts.len(), 4);
        assert_eq!(
            report.accounts,
            vec![
                AccountBalance {
                    participant: ParticipantId(7),
                    asset: Asset::Cash,
                    balance: -300,
                    entries: 1,
                },
                AccountBalance {
                    participant: ParticipantId(7),
                    asset: Asset::Position(symbol()),
                    balance: 3,
                    entries: 1,
                },
                AccountBalance {
                    participant: ParticipantId(8),
                    asset: Asset::Cash,
                    balance: 300,
                    entries: 1,
                },
                AccountBalance {
                    participant: ParticipantId(8),
                    asset: Asset::Position(symbol()),
                    balance: -3,
                    entries: 1,
                },
            ]
        );
    }

    #[test]
    fn reversing_the_execution_input_does_not_change_the_report() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
            new_order(2, 3, 9, Side::Sell, 100, 2),
        ];
        let forward = vec![
            fill(10, 2, 1, Side::Sell, 100, 3),
            fill(11, 3, 1, Side::Sell, 100, 2),
        ];
        let mut backward = forward.clone();
        backward.reverse();

        let a = build_ledger(1, &orders, &forward, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        let b = build_ledger(1, &orders, &backward, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert_eq!(a, b, "seq, not input order, is the canonical order");
    }

    #[test]
    fn a_missing_counterparty_posts_nothing() {
        let maker_unknown = vec![new_order(0, 2, 8, Side::Sell, 100, 3)];
        let report = build_ledger(
            1,
            &maker_unknown,
            &[fill(10, 2, 99, Side::Sell, 100, 3)],
            None,
            &tick_sizes(),
        )
        .expect("the fixture symbols have tick sizes");
        assert!(report.entries.is_empty());
        assert!(report.accounts.is_empty());
        assert_eq!(report.unresolved_maker_fills, 1);
        assert_eq!(report.unresolved_aggressor_fills, 0);
        report.check_invariants().expect("an empty ledger balances");

        let aggressor_unknown = vec![new_order(0, 1, 7, Side::Buy, 100, 3)];
        let report = build_ledger(
            1,
            &aggressor_unknown,
            &[fill(10, 99, 1, Side::Sell, 100, 3)],
            None,
            &tick_sizes(),
        )
        .expect("the fixture symbols have tick sizes");
        assert!(report.entries.is_empty());
        assert_eq!(report.unresolved_maker_fills, 0);
        assert_eq!(report.unresolved_aggressor_fills, 1);
        report.check_invariants().expect("an empty ledger balances");
    }

    #[test]
    fn invariants_reject_a_tampered_entry() {
        let report = fixture();

        let mut zero_delta = report.clone();
        zero_delta.entries[0].postings[0].delta = 0;
        assert!(
            zero_delta
                .check_invariants()
                .expect_err("a zero posting is rejected")
                .contains("zero delta")
        );

        let mut swapped = report.clone();
        swapped.entries[0].postings[0].delta = 300;
        swapped.entries[0].postings[1].delta = -300;
        assert!(
            swapped
                .check_invariants()
                .expect_err("swapped cash signs are rejected")
                .contains("four legs")
        );

        let mut duplicated = report.clone();
        duplicated.entries.push(report.entries[0].clone());
        assert!(
            duplicated
                .check_invariants()
                .expect_err("a duplicate seq is rejected")
                .contains("duplicate")
        );

        let mut wrong_account = report.clone();
        wrong_account.accounts[0].balance = 0;
        assert!(
            wrong_account
                .check_invariants()
                .expect_err("a stale account balance is rejected")
                .contains("does not equal")
        );

        let mut wrong_price = report.clone();
        wrong_price.entries[0].price_cents = 1;
        assert!(
            wrong_price
                .check_invariants()
                .expect_err("a mis-quoted price is rejected")
                .contains("cents is not")
        );

        let mut zero_seq = report;
        zero_seq.entries[0].seq = 0;
        assert!(
            zero_seq
                .check_invariants()
                .expect_err("seq zero is rejected")
                .contains("seq 0")
        );
    }

    #[test]
    fn a_repeated_seq_posts_once_deterministically() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
            new_order(2, 3, 9, Side::Sell, 100, 3),
        ];
        let executions = vec![
            fill(10, 2, 1, Side::Sell, 100, 3),
            fill(10, 3, 1, Side::Sell, 100, 3),
        ];
        let report = build_ledger(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert_eq!(report.entries.len(), 1, "one seq, one entry");
        report.check_invariants().expect("the survivor balances");

        let mut reversed = executions;
        reversed.reverse();
        assert_eq!(
            report,
            build_ledger(1, &orders, &reversed, None, &tick_sizes())
                .expect("the fixture symbols have tick sizes"),
            "the surviving entry for a repeated seq is input-order independent"
        );
    }

    #[test]
    fn seq_is_per_symbol_so_two_symbols_may_reuse_it() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
            (
                2,
                Command::new_order(
                    eth(),
                    Side::Buy,
                    OrderType::Limit,
                    Price(50),
                    Qty(4),
                    OrderId(3),
                    ParticipantId(9),
                    2,
                ),
            ),
            (
                3,
                Command::new_order(
                    eth(),
                    Side::Sell,
                    OrderType::Limit,
                    Price(50),
                    Qty(4),
                    OrderId(4),
                    ParticipantId(10),
                    3,
                ),
            ),
        ];
        // The engine restarts `seq` per symbol, so both fills carry seq 1.
        let executions = vec![
            fill_on(symbol(), 1, 2, 2, 1, Side::Sell, 100, 3),
            fill_on(eth(), 1, 3, 4, 3, Side::Sell, 50, 4),
        ];
        let report = build_ledger(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert_eq!(
            report
                .entries
                .iter()
                .map(|entry| (entry.symbol, entry.seq, entry.cause_offset))
                .collect::<Vec<_>>(),
            vec![(symbol(), 1, 2), (eth(), 1, 3)],
            "cause_offset, not seq, is the partition-global order"
        );
        report
            .check_invariants()
            .expect("a per-symbol seq collision is legal");
    }

    #[test]
    fn retain_participant_keeps_entries_whole_and_the_report_balanced() {
        let mut report = fixture();
        report.retain_participant(ParticipantId(7));
        assert_eq!(report.accounts.len(), 2);
        assert_eq!(report.entries.len(), 1, "the trade stays readable");
        report
            .check_invariants()
            .expect("the filtered view balances");
    }
}
