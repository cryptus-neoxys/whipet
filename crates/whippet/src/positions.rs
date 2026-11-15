// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Per-participant, per-symbol position and cash-flow reconstruction.
//!
//! `whippet positions` folds one partition's `orders` and `executions` logs
//! into net quantity, buy/sell quantity, fill count, cash flow and traded
//! notional per participant and symbol.
//!
//! The logs store each fill price as an integer **ladder tick index**, not
//! quote cents, so every value here is converted with the instrument's
//! configured tick size: `price_ticks * tick_size_cents * qty`. A symbol with
//! no tick size, or a non-positive one, is an error — never a silent factor of
//! one, because that would misprice every report by the venue's tick and still
//! look plausible. [`TickSizes`] carries the per-symbol sizes; the CLI resolves
//! them from the built-in default config or `--tick-size-cents`.
//!
//! This is a **reconstruction, not a settlement ledger**: it reads only the
//! decoded logs, has no account balances, deposits, fees, or margin, and
//! never writes. Participant attribution depends on the orders log — an
//! execution report carries no participant, so a leg is attributed through the
//! `NewOrder` command that created its order id. A fill whose order id is not
//! in the decoded orders log is unattributable and is counted, not guessed.
//!
//! The module is the pure half: it reads only its arguments — no wall clock,
//! no network, no journal, no logging — so the same decoded logs always
//! produce the same report. The journal reads live in [`crate::cli`] behind the
//! [`Journal`] seam, exactly as the audit, order-trace, and surveillance tools
//! do.
//!
//! [`Journal`]: whippet_journal::Journal

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Serialize;
use whippet_core::ids::{OrderId, ParticipantId, Symbol};
use whippet_core::protocol::{Command, CommandKind, ExecKind, ExecutionReport, Side};

/// Printed on every positions run, to stderr so `--json` output stays
/// machine-readable.
pub const POSITIONS_NOTICE: &str = "reconstruction from the orders and executions logs, not a settlement ledger; \
     prices are log ticks converted to quote cents with the configured tick size \
     (--tick-size-cents overrides the built-in config); maker attribution depends on the orders log; read-only";

/// Per-symbol tick size, used to turn the log's integer tick prices into quote
/// cents.
///
/// The engine's [`Price`] is a ladder index, so `price.0 * qty` is
/// tick×quantity. A field named `..._cents` must multiply by the instrument's
/// tick size exactly once. There is deliberately no default: [`TickSizes::require`]
/// rejects a missing or non-positive entry, so a caller cannot silently report
/// tick units as cents.
///
/// [`Price`]: whippet_core::ids::Price
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickSizes {
    cents: BTreeMap<Symbol, i64>,
}

/// Why a projection could not price a symbol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickSizeError {
    /// The symbol has no tick size in the supplied map.
    Missing { symbol: Symbol },
    /// The supplied tick size is zero or negative.
    NonPositive {
        symbol: Symbol,
        tick_size_cents: i64,
    },
}

impl fmt::Display for TickSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { symbol } => write!(
                f,
                "no tick size for symbol {symbol}: pass --tick-size-cents, or use a symbol in the \
                 built-in default config"
            ),
            Self::NonPositive {
                symbol,
                tick_size_cents,
            } => write!(
                f,
                "tick size for symbol {symbol} must be at least 1 cent; got {tick_size_cents}"
            ),
        }
    }
}

impl std::error::Error for TickSizeError {}

impl TickSizes {
    pub fn new(entries: impl IntoIterator<Item = (Symbol, i64)>) -> Self {
        Self {
            cents: entries.into_iter().collect(),
        }
    }

    pub fn insert(&mut self, symbol: Symbol, tick_size_cents: i64) {
        self.cents.insert(symbol, tick_size_cents);
    }

    pub fn get(&self, symbol: Symbol) -> Option<i64> {
        self.cents.get(&symbol).copied()
    }

    /// The symbol's tick size, or the reason it cannot be priced.
    pub fn require(&self, symbol: Symbol) -> Result<i64, TickSizeError> {
        match self.get(symbol) {
            None => Err(TickSizeError::Missing { symbol }),
            Some(tick_size_cents) if tick_size_cents <= 0 => Err(TickSizeError::NonPositive {
                symbol,
                tick_size_cents,
            }),
            Some(tick_size_cents) => Ok(tick_size_cents),
        }
    }

    /// Convert a price in integer ticks into quote cents.
    ///
    /// Saturating because a corrupt log could carry `i64::MAX` ticks against a
    /// huge tick size; one absurd record must not wrap into a plausible value.
    pub fn price_cents(&self, symbol: Symbol, price_ticks: i64) -> Result<i128, TickSizeError> {
        let tick_size_cents = self.require(symbol)?;
        Ok(i128::from(price_ticks).saturating_mul(i128::from(tick_size_cents)))
    }

    /// Trade value in quote cents: `price_ticks * tick_size_cents * qty`.
    pub fn quote_cents(
        &self,
        symbol: Symbol,
        price_ticks: i64,
        qty: u64,
    ) -> Result<i128, TickSizeError> {
        Ok(self
            .price_cents(symbol, price_ticks)?
            .saturating_mul(i128::from(qty)))
    }
}

impl FromIterator<(Symbol, i64)> for TickSizes {
    fn from_iter<T: IntoIterator<Item = (Symbol, i64)>>(iter: T) -> Self {
        Self::new(iter)
    }
}

/// One participant's reconstructed position in one symbol.
///
/// `cash_cents` is the signed traded cash flow of the attributed legs in quote
/// cents: a buy pays `price_ticks * tick_size_cents * qty`, a sell receives it,
/// and no fee or funding is modelled. `notional_cents` is the unsigned traded
/// value summed over both sides, so it measures turnover rather than exposure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SymbolPosition {
    pub symbol: Symbol,
    /// `buy_qty - sell_qty`.
    pub net_qty: i128,
    pub buy_qty: u64,
    pub sell_qty: u64,
    /// Attributed fill legs, not execution reports: one report can fill both
    /// the aggressor and a resting maker.
    pub fills: u64,
    pub cash_cents: i128,
    pub notional_cents: i128,
    /// Aggressor fills on this row whose `maker_order_id` is not in the decoded
    /// orders log, so the maker leg is missing from the reconstruction.
    pub unresolved_maker_fills: u64,
}

/// One participant's positions across the symbols it traded, plus totals.
///
/// The rows are sorted by symbol; the totals are the sum of the rows, so a
/// participant's net exposure is one field even when it traded several
/// instruments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParticipantPosition {
    pub participant: ParticipantId,
    pub symbols: Vec<SymbolPosition>,
    pub net_qty: i128,
    pub buy_qty: u64,
    pub sell_qty: u64,
    pub fills: u64,
    pub cash_cents: i128,
    pub notional_cents: i128,
    pub unresolved_maker_fills: u64,
}

/// The reconstruction for one partition.
///
/// `symbols` is the sorted set of symbols that produced at least one
/// attributed fill leg. Both unresolved counters are scan-wide: they count
/// reconstruction gaps even when the counterparty could not be attributed, so
/// they are not the sum of the per-participant rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PositionReport {
    pub partition: u32,
    pub symbols: Vec<Symbol>,
    /// Sorted by participant id; each participant's rows are sorted by symbol.
    pub participants: Vec<ParticipantPosition>,
    /// Maker legs whose order id is not in the decoded orders log.
    pub unresolved_maker_fills: u64,
    /// Aggressor legs whose own order id is not in the decoded orders log.
    /// The trade happened but cannot be attributed to a participant.
    pub unresolved_aggressor_fills: u64,
}

impl PositionReport {
    /// Restrict the report to one participant id.
    ///
    /// Covers the `--participant` filter. Participant names are derived at the
    /// gateway ([`ParticipantId::derive`]) and never stored in the logs, so a
    /// name cannot be resolved offline; the id is the only stable key.
    ///
    /// `symbols` is recomputed from the surviving rows. The two unresolved
    /// counters stay scan-wide: they describe the log, not one participant.
    pub fn retain_participant(&mut self, participant: ParticipantId) {
        self.participants
            .retain(|position| position.participant == participant);
        self.symbols = self
            .participants
            .iter()
            .flat_map(|position| position.symbols.iter().map(|row| row.symbol))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
    }
}

/// What the orders log knows about one order id.
///
/// The symbol is the filter authority (an order outside the requested symbol
/// set never enters the map) and the participant is the attribution authority.
/// Side and price are deliberately not cached: the execution report is the
/// trade's authority for the fill side and price, and the maker leg's side is
/// the opposite of the aggressor's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OrderInfo {
    pub(crate) symbol: Symbol,
    pub(crate) participant: ParticipantId,
}

/// Build the order-id → participant attribution map from the decoded orders.
///
/// Shared with the ledger projection so both tools attribute a trade the same
/// way. The symbol is the filter authority (an order outside the requested
/// symbol set never enters the map) and the participant is the attribution
/// authority. **The first `NewOrder` per order id wins**, so a malformed log
/// that re-uses an id keeps one deterministic answer instead of the last write.
pub(crate) fn order_index(
    orders: &[(u64, Command)],
    allowed: impl Fn(Symbol) -> bool,
) -> BTreeMap<OrderId, OrderInfo> {
    let mut order_info: BTreeMap<OrderId, OrderInfo> = BTreeMap::new();
    for (_, command) in orders {
        if command.kind != CommandKind::NewOrder || !allowed(command.symbol) {
            continue;
        }
        order_info.entry(command.order_id).or_insert(OrderInfo {
            symbol: command.symbol,
            participant: command.participant,
        });
    }
    order_info
}

/// Per participant/symbol accumulation.
#[derive(Default)]
struct Accum {
    buy_qty: u64,
    sell_qty: u64,
    fills: u64,
    cash_cents: i128,
    notional_cents: i128,
    unresolved_maker_fills: u64,
}

/// Fold one fill leg into its participant/symbol row.
///
/// The value is quote cents: the engine's price is a ladder tick index, so the
/// instrument's tick size is applied exactly once. The multiplication saturates
/// inside [`TickSizes::quote_cents`], so a corrupt extreme saturates instead of
/// wrapping; both legs of a trade share the same value, so conservation holds
/// even then.
fn apply_leg(
    rows: &mut BTreeMap<(ParticipantId, Symbol), Accum>,
    tick_sizes: &TickSizes,
    participant: ParticipantId,
    symbol: Symbol,
    side: Side,
    price_ticks: i64,
    qty: u64,
) -> Result<(), TickSizeError> {
    let value = tick_sizes.quote_cents(symbol, price_ticks, qty)?;
    let acc = rows.entry((participant, symbol)).or_default();
    match side {
        Side::Buy => {
            acc.buy_qty = acc.buy_qty.saturating_add(qty);
            acc.cash_cents = acc.cash_cents.saturating_sub(value);
        }
        Side::Sell => {
            acc.sell_qty = acc.sell_qty.saturating_add(qty);
            acc.cash_cents = acc.cash_cents.saturating_add(value);
        }
    }
    acc.notional_cents = acc.notional_cents.saturating_add(value);
    acc.fills = acc.fills.saturating_add(1);
    Ok(())
}

/// Build one partition's position report from already-decoded logs.
///
/// `orders` is every decoded command on the partition with its orders-topic
/// offset; `executions` is every decoded execution report. Both are filtered
/// by `symbols` when it is `Some`. `tick_sizes` converts the logs' integer tick
/// prices into quote cents; a symbol with no tick size, or a non-positive one,
/// is an error rather than a silent factor of one.
///
/// Attribution rules:
///
/// - The order map holds `NewOrder` commands only. **The first `NewOrder` per
///   order id wins**, matching the surveillance analysis: a malformed log that
///   re-uses an id keeps one deterministic answer instead of the last write.
/// - Only `filled` and `partially_filled` reports move positions or cash.
///   Accepted, rejected and cancelled reports do not.
/// - One report is one trade with up to two legs. The aggressor leg uses
///   `report.order_id`, `report.side` and `report.qty`; the maker leg uses
///   `report.maker_order_id`, the opposite side, and the same `qty`, because
///   the engine emits one report per trade and never a separate maker fill.
/// - Quantities add on a buy and subtract on a sell. Cash pays
///   `price_ticks * tick_size_cents * qty` on a buy and receives it on a sell;
///   fees and funding are not modelled.
/// - A maker id absent from the order map leaves the aggressor leg counted and
///   increments `unresolved_maker_fills`. An aggressor id absent from the map
///   cannot be attributed to anyone and increments
///   `unresolved_aggressor_fills`.
pub fn build_positions(
    partition: u32,
    orders: &[(u64, Command)],
    executions: &[ExecutionReport],
    symbols: Option<&[Symbol]>,
    tick_sizes: &TickSizes,
) -> Result<PositionReport, TickSizeError> {
    let allowed = |symbol: Symbol| symbols.is_none_or(|symbols| symbols.contains(&symbol));
    let order_info = order_index(orders, allowed);

    let mut rows: BTreeMap<(ParticipantId, Symbol), Accum> = BTreeMap::new();
    let mut unresolved_maker_fills = 0u64;
    let mut unresolved_aggressor_fills = 0u64;

    for report in executions {
        if !matches!(report.kind, ExecKind::Filled | ExecKind::PartiallyFilled) {
            continue;
        }
        if !allowed(report.symbol) || report.qty.0 == 0 {
            continue;
        }
        // The report's symbol chooses the scan; each attributed leg lands on
        // the symbol of the order that names it. They agree in a consistent
        // log, and the order map is the only participant link there is.
        let aggressor = order_info.get(&report.order_id).copied();
        if let Some(info) = aggressor {
            apply_leg(
                &mut rows,
                tick_sizes,
                info.participant,
                info.symbol,
                report.side,
                report.price.0,
                report.qty.0,
            )?;
        } else {
            unresolved_aggressor_fills = unresolved_aggressor_fills.saturating_add(1);
        }

        if report.maker_order_id == OrderId(0) {
            continue;
        }
        match order_info.get(&report.maker_order_id).copied() {
            Some(maker) => apply_leg(
                &mut rows,
                tick_sizes,
                maker.participant,
                maker.symbol,
                report.side.opposite(),
                report.price.0,
                report.qty.0,
            )?,
            None => {
                unresolved_maker_fills = unresolved_maker_fills.saturating_add(1);
                // The gap belongs to the trade the aggressor caused, so record
                // it on that participant's row when the aggressor is known.
                if let Some(info) = aggressor
                    && let Some(acc) = rows.get_mut(&(info.participant, info.symbol))
                {
                    acc.unresolved_maker_fills = acc.unresolved_maker_fills.saturating_add(1);
                }
            }
        }
    }

    let mut participants: BTreeMap<ParticipantId, Vec<SymbolPosition>> = BTreeMap::new();
    let mut report_symbols: BTreeSet<Symbol> = BTreeSet::new();
    for ((participant, symbol), acc) in rows {
        report_symbols.insert(symbol);
        participants
            .entry(participant)
            .or_default()
            .push(SymbolPosition {
                symbol,
                net_qty: i128::from(acc.buy_qty) - i128::from(acc.sell_qty),
                buy_qty: acc.buy_qty,
                sell_qty: acc.sell_qty,
                fills: acc.fills,
                cash_cents: acc.cash_cents,
                notional_cents: acc.notional_cents,
                unresolved_maker_fills: acc.unresolved_maker_fills,
            });
    }

    let participants = participants
        .into_iter()
        .map(|(participant, symbols)| ParticipantPosition {
            participant,
            net_qty: symbols.iter().map(|row| row.net_qty).sum(),
            buy_qty: symbols.iter().map(|row| row.buy_qty).sum(),
            sell_qty: symbols.iter().map(|row| row.sell_qty).sum(),
            fills: symbols.iter().map(|row| row.fills).sum(),
            cash_cents: symbols.iter().map(|row| row.cash_cents).sum(),
            notional_cents: symbols.iter().map(|row| row.notional_cents).sum(),
            unresolved_maker_fills: symbols.iter().map(|row| row.unresolved_maker_fills).sum(),
            symbols,
        })
        .collect();

    Ok(PositionReport {
        partition,
        symbols: report_symbols.into_iter().collect(),
        participants,
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

    fn tick_sizes() -> TickSizes {
        TickSizes::from_iter([(symbol(), 1), (eth(), 1)])
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
        kind: ExecKind,
    ) -> ExecutionReport {
        ExecutionReport {
            kind,
            symbol: symbol(),
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

    fn row(report: &PositionReport, participant: u64) -> &ParticipantPosition {
        report
            .participants
            .iter()
            .find(|position| position.participant == ParticipantId(participant))
            .expect("participant has a position")
    }

    #[test]
    fn resting_buy_maker_and_selling_aggressor_get_opposite_positions() {
        // Maker 7 rests a buy for 5 at 100; aggressor 8 sells 3 into it.
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
        ];
        let executions = vec![fill(0, 2, 1, Side::Sell, 100, 3, ExecKind::Filled)];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");

        let maker = row(&report, 7);
        assert_eq!(maker.net_qty, 3);
        assert_eq!(maker.buy_qty, 3);
        assert_eq!(maker.sell_qty, 0);
        assert_eq!(maker.fills, 1);
        assert_eq!(maker.cash_cents, -300);
        assert_eq!(maker.notional_cents, 300);

        let aggressor = row(&report, 8);
        assert_eq!(aggressor.net_qty, -3);
        assert_eq!(aggressor.buy_qty, 0);
        assert_eq!(aggressor.sell_qty, 3);
        assert_eq!(aggressor.fills, 1);
        assert_eq!(aggressor.cash_cents, 300);
        assert_eq!(aggressor.notional_cents, 300);

        assert_eq!(report.symbols, vec![symbol()]);
        assert_eq!(report.participants.len(), 2);
        assert_eq!(report.unresolved_maker_fills, 0);
        assert_eq!(report.unresolved_aggressor_fills, 0);
    }

    #[test]
    fn one_report_fills_two_legs_without_double_counting_the_trade() {
        // The aggressor's partial fill against two makers is two reports and
        // therefore two trades; each report contributes exactly two legs.
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 7, Side::Buy, 100, 5),
            new_order(2, 3, 8, Side::Sell, 100, 8),
        ];
        let executions = vec![
            fill(0, 3, 1, Side::Sell, 100, 3, ExecKind::PartiallyFilled),
            fill(1, 3, 2, Side::Sell, 100, 5, ExecKind::Filled),
        ];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");

        let maker = row(&report, 7);
        assert_eq!(maker.buy_qty, 8);
        assert_eq!(maker.fills, 2);
        assert_eq!(maker.cash_cents, -800);

        let aggressor = row(&report, 8);
        assert_eq!(aggressor.sell_qty, 8);
        assert_eq!(aggressor.fills, 2);
        assert_eq!(aggressor.cash_cents, 800);

        // Two trades, four legs, no report counted twice.
        assert_eq!(
            report
                .participants
                .iter()
                .map(|position| position.fills)
                .sum::<u64>(),
            4
        );
    }

    #[test]
    fn unknown_maker_keeps_the_aggressor_leg_and_counts_the_gap() {
        let orders = vec![new_order(0, 2, 8, Side::Sell, 100, 3)];
        let executions = vec![fill(0, 2, 99, Side::Sell, 100, 3, ExecKind::Filled)];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");

        let aggressor = row(&report, 8);
        assert_eq!(aggressor.net_qty, -3);
        assert_eq!(aggressor.fills, 1);
        assert_eq!(aggressor.cash_cents, 300);
        assert_eq!(aggressor.unresolved_maker_fills, 1);
        assert_eq!(report.unresolved_maker_fills, 1);
        assert_eq!(report.unresolved_aggressor_fills, 0);
    }

    #[test]
    fn unknown_aggressor_is_counted_and_never_attributed_to_a_guess() {
        let orders = vec![new_order(0, 1, 7, Side::Buy, 100, 3)];
        let executions = vec![fill(0, 99, 1, Side::Sell, 100, 3, ExecKind::Filled)];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");

        // Only the maker leg is attributable.
        assert_eq!(report.participants.len(), 1);
        assert_eq!(row(&report, 7).buy_qty, 3);
        assert_eq!(row(&report, 7).fills, 1);
        assert_eq!(report.unresolved_aggressor_fills, 1);
        assert_eq!(report.unresolved_maker_fills, 0);
    }

    #[test]
    fn non_fill_executions_do_not_move_positions_or_cash() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
        ];
        let executions = vec![
            fill(0, 2, 0, Side::Sell, 100, 0, ExecKind::Rejected),
            fill(1, 2, 0, Side::Sell, 100, 0, ExecKind::Cancelled),
            fill(2, 1, 0, Side::Buy, 100, 0, ExecKind::Accepted),
        ];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert!(report.participants.is_empty());
        assert!(report.symbols.is_empty());
    }

    #[test]
    fn symbol_filter_restricts_the_scan() {
        let btc_order = new_order(0, 1, 7, Side::Buy, 100, 3);
        let eth_order = (
            1,
            Command::new_order(
                eth(),
                Side::Buy,
                OrderType::Limit,
                Price(100),
                Qty(3),
                OrderId(2),
                ParticipantId(7),
                1,
            ),
        );
        let orders = vec![btc_order, eth_order];
        let mut eth_fill = fill(0, 2, 0, Side::Buy, 100, 3, ExecKind::Filled);
        eth_fill.symbol = eth();
        let executions = vec![fill(0, 1, 0, Side::Buy, 100, 3, ExecKind::Filled), eth_fill];

        let all = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert_eq!(all.symbols, vec![symbol(), eth()]);
        let filtered = build_positions(1, &orders, &executions, Some(&[eth()]), &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert_eq!(filtered.symbols, vec![eth()]);
        assert_eq!(filtered.participants.len(), 1);
        assert_eq!(filtered.participants[0].symbols[0].symbol, eth());
    }

    #[test]
    fn order_ids_sort_by_participant_then_symbol_and_totals_add_up() {
        let btc_buy = new_order(0, 1, 8, Side::Buy, 100, 2);
        let btc_sell = new_order(1, 2, 8, Side::Sell, 110, 1);
        let eth_buy = (
            2,
            Command::new_order(
                eth(),
                Side::Buy,
                OrderType::Limit,
                Price(50),
                Qty(4),
                OrderId(3),
                ParticipantId(8),
                2,
            ),
        );
        let small = new_order(3, 4, 7, Side::Buy, 100, 1);
        let orders = vec![btc_buy, btc_sell, eth_buy, small];
        let mut eth_fill = fill(1, 3, 0, Side::Buy, 50, 4, ExecKind::Filled);
        eth_fill.symbol = eth();
        let executions = vec![
            fill(0, 1, 0, Side::Buy, 100, 2, ExecKind::Filled),
            fill(1, 2, 0, Side::Sell, 110, 1, ExecKind::Filled),
            eth_fill,
            fill(3, 4, 0, Side::Buy, 100, 1, ExecKind::Filled),
        ];

        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        assert_eq!(
            report
                .participants
                .iter()
                .map(|position| position.participant.0)
                .collect::<Vec<_>>(),
            vec![7, 8]
        );

        let eight = row(&report, 8);
        assert_eq!(
            eight
                .symbols
                .iter()
                .map(|row| row.symbol.as_str())
                .collect::<Vec<_>>(),
            vec!["BTC-USD", "ETH-USD"]
        );
        assert_eq!(eight.net_qty, 5);
        assert_eq!(eight.buy_qty, 6);
        assert_eq!(eight.sell_qty, 1);
        assert_eq!(eight.cash_cents, -200 + 110 - 200);
        assert_eq!(eight.notional_cents, 200 + 110 + 200);

        let seven = row(&report, 7);
        assert_eq!(seven.net_qty, 1);
        assert_eq!(seven.cash_cents, -100);
    }

    #[test]
    fn first_new_order_for_an_id_wins() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 1, 9, Side::Sell, 100, 5),
        ];
        let executions = vec![fill(0, 1, 0, Side::Buy, 100, 2, ExecKind::Filled)];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");

        assert_eq!(report.participants.len(), 1);
        assert_eq!(row(&report, 7).buy_qty, 2);
        assert_eq!(row(&report, 7).net_qty, 2);
    }

    #[test]
    fn retain_participant_filters_rows_and_recomputes_symbols() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 1),
            new_order(1, 2, 8, Side::Buy, 100, 1),
        ];
        let executions = vec![
            fill(0, 1, 0, Side::Buy, 100, 1, ExecKind::Filled),
            fill(1, 2, 0, Side::Buy, 100, 1, ExecKind::Filled),
        ];
        let mut report = build_positions(1, &orders, &executions, None, &tick_sizes())
            .expect("the fixture symbols have tick sizes");
        report.retain_participant(ParticipantId(8));
        assert_eq!(report.participants.len(), 1);
        assert_eq!(report.participants[0].participant, ParticipantId(8));
        assert_eq!(report.symbols, vec![symbol()]);
    }

    #[test]
    fn a_hundred_cent_tick_scales_value_once() {
        // 3 ticks at 100 cents per tick, qty 2: 600 cents, not 6 or 60,000.
        let tick_sizes = TickSizes::from_iter([(symbol(), 100)]);
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 3, 2),
            new_order(1, 2, 8, Side::Sell, 3, 2),
        ];
        let executions = vec![fill(0, 2, 1, Side::Sell, 3, 2, ExecKind::Filled)];
        let report = build_positions(1, &orders, &executions, None, &tick_sizes)
            .expect("the fixture symbol has a tick size");

        let maker = row(&report, 7);
        assert_eq!(maker.notional_cents, 600);
        assert_eq!(maker.cash_cents, -600);
        let taker = row(&report, 8);
        assert_eq!(taker.notional_cents, 600);
        assert_eq!(taker.cash_cents, 600);
    }

    #[test]
    fn a_missing_or_non_positive_tick_size_is_an_error() {
        let orders = vec![
            new_order(0, 1, 7, Side::Buy, 100, 5),
            new_order(1, 2, 8, Side::Sell, 100, 3),
        ];
        let executions = vec![fill(0, 2, 1, Side::Sell, 100, 3, ExecKind::Filled)];

        let error = build_positions(1, &orders, &executions, None, &TickSizes::default())
            .expect_err("a missing tick size must not default to 1");
        assert_eq!(error, TickSizeError::Missing { symbol: symbol() });

        let zero = TickSizes::from_iter([(symbol(), 0)]);
        let error = build_positions(1, &orders, &executions, None, &zero)
            .expect_err("a zero tick size is not a factor");
        assert_eq!(
            error,
            TickSizeError::NonPositive {
                symbol: symbol(),
                tick_size_cents: 0,
            }
        );
    }
}
