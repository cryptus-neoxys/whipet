// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Golden scenario runner.
//!
//! [`GoldenLog::standard`] is a fixed command sequence that exercises
//! resting orders, multi-level fills, partial fills, cancels, IOC, FOK (both
//! reject and fill), post-only (both reject and rest), market orders, duplicate
//! ids, unknown cancels, and self-trade prevention. Running it through
//! [`SymbolEngine`] or [`ShardEngine`] returns the engine state hash plus
//! execution and market-data summaries.
//!
//! # Updating the golden hash
//!
//! [`GOLDEN_HASH`] is pinned to the current matching semantics. Any change that
//! alters fill order, sequence assignment, market-data emission, dedup state,
//! or the snapshot layout will change it. That is intentional: update the
//! constant in the same commit and explain why the observable behaviour moved.
//! Do not update it to make an unrelated failure disappear.

use std::collections::BTreeMap;

use whippet_core::config::{ExchangeConfig, InstrumentConfig};
use whippet_core::ids::{Price, Qty, Symbol};
use whippet_core::protocol::{
    Command, ExecKind, ExecutionReport, MdEvent, MdKind, MdSnapshot, RejectReason, Side,
};
use whippet_engine::{Book, EngineError, EngineOutput, ShardEngine, SymbolEngine};

use crate::builders;

/// State hash of [`GoldenLog::standard`] run through
/// [`SymbolEngine`] with [`scenario_config`].
///
/// Generated from the current code. Changing matching semantics requires
/// updating this value deliberately (see the module docs).
pub const GOLDEN_HASH: &str = "331a417f7124b27c0ecd090d56675c38b3f89b4a29d6796e8c8ed554d57b96d0";

/// The instrument configuration used for the golden hash.
///
/// The ladder is deliberately small (`90..=200` ticks) so a reader can follow
/// the scenario by hand. `snapshot_depth` is five so the final market-data
/// snapshot stays readable in a failure message.
pub fn scenario_config() -> InstrumentConfig {
    let mut config = InstrumentConfig::new("BTC-USD", 100, 90, 200).expect("static config");
    config.max_orders = 1_024;
    config.snapshot_depth = 5;
    config
}

/// The partition that owns the standard scenario's symbol.
pub fn standard_shard(exchange: &ExchangeConfig) -> u32 {
    builders::default_symbol().partition(exchange.partitions)
}

/// A fixed command log with a deterministic result.
#[derive(Debug, Clone)]
pub struct GoldenLog {
    name: String,
    commands: Vec<Command>,
}

impl GoldenLog {
    pub fn new(name: impl Into<String>, commands: Vec<Command>) -> Self {
        Self {
            name: name.into(),
            commands,
        }
    }

    /// The canonical scenario. See the module docs for what it covers.
    pub fn standard() -> Self {
        Self::new("standard", standard_commands())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn commands(&self) -> &[Command] {
        &self.commands
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Run every command through a single-symbol engine. Offsets are the
    /// command indices, matching a fresh journal partition.
    pub fn run_symbol(&self, config: InstrumentConfig) -> GoldenResult {
        let mut engine = SymbolEngine::new(config);
        let mut out = EngineOutput::with_capacity(32, 32);
        let mut result = GoldenResult::new(self.commands.len());
        for (offset, command) in self.commands.iter().enumerate() {
            engine.process(command, offset as u64, &mut out);
            result.absorb(&out);
            out.clear();
        }
        result.state_hash = engine.state_hash();
        result
            .books
            .insert(engine.config().symbol, FinalState::from_book(engine.book()));
        result
    }

    /// Run every command through the shard that owns its symbol. Returns an
    /// error if a command targets a symbol on another shard.
    pub fn run_shard(
        &self,
        exchange: &ExchangeConfig,
        shard_id: u32,
    ) -> Result<GoldenResult, EngineError> {
        let mut engine = ShardEngine::new(shard_id, exchange);
        let mut out = EngineOutput::with_capacity(32, 32);
        let mut result = GoldenResult::new(self.commands.len());
        for (offset, command) in self.commands.iter().enumerate() {
            engine.process(command, offset as u64, &mut out)?;
            result.absorb(&out);
            out.clear();
        }
        result.state_hash = engine.state_hash();
        let mut symbols: Vec<Symbol> = engine.symbols().collect();
        symbols.sort_unstable();
        for symbol in symbols {
            if let Some(symbol_engine) = engine.engine(symbol) {
                result
                    .books
                    .insert(symbol, FinalState::from_book(symbol_engine.book()));
            }
        }
        Ok(result)
    }
}

/// Everything the golden scenario produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoldenResult {
    pub state_hash: [u8; 32],
    pub commands: usize,
    pub executions: Vec<ExecutionReport>,
    pub md: Vec<MdEvent>,
    pub snapshots: Vec<MdSnapshot>,
    pub books: BTreeMap<Symbol, FinalState>,
}

impl GoldenResult {
    fn new(commands: usize) -> Self {
        Self {
            state_hash: [0; 32],
            commands,
            executions: Vec::new(),
            md: Vec::new(),
            snapshots: Vec::new(),
            books: BTreeMap::new(),
        }
    }

    fn absorb(&mut self, out: &EngineOutput) {
        self.executions.extend(out.executions.iter().cloned());
        self.md.extend(out.md.iter().cloned());
        self.snapshots.extend(out.snapshots.iter().cloned());
    }

    pub fn hash_hex(&self) -> String {
        hex(&self.state_hash)
    }

    pub fn execution_summary(&self) -> ExecutionSummary {
        ExecutionSummary::from_reports(&self.executions)
    }

    pub fn md_summary(&self) -> MdSummary {
        MdSummary::from_events(&self.md, self.snapshots.len())
    }
}

/// Final resting state of one book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalState {
    pub order_count: usize,
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    pub bid_depth_qty: u64,
    pub ask_depth_qty: u64,
    pub last_trade_price: Price,
    pub last_trade_qty: Qty,
}

impl FinalState {
    fn from_book(book: &Book) -> Self {
        let (last_trade_price, last_trade_qty, _) = book.last_trade();
        Self {
            order_count: book.order_count(),
            best_bid: book.best_bid(),
            best_ask: book.best_ask(),
            bid_depth_qty: book.depth_qty(Side::Buy),
            ask_depth_qty: book.depth_qty(Side::Sell),
            last_trade_price,
            last_trade_qty,
        }
    }
}

/// Counts by execution kind, plus reject reasons and traded quantity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub total: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub filled: usize,
    pub partially_filled: usize,
    pub cancelled: usize,
    pub filled_qty: u64,
    pub rejected_by_reason: BTreeMap<&'static str, usize>,
}

impl ExecutionSummary {
    fn from_reports(reports: &[ExecutionReport]) -> Self {
        let mut summary = Self {
            total: reports.len(),
            accepted: 0,
            rejected: 0,
            filled: 0,
            partially_filled: 0,
            cancelled: 0,
            filled_qty: 0,
            rejected_by_reason: BTreeMap::new(),
        };
        for report in reports {
            match report.kind {
                ExecKind::Accepted => summary.accepted += 1,
                ExecKind::Rejected => {
                    summary.rejected += 1;
                    *summary
                        .rejected_by_reason
                        .entry(report.reason.as_str())
                        .or_default() += 1;
                }
                ExecKind::Filled => {
                    summary.filled += 1;
                    summary.filled_qty = summary.filled_qty.saturating_add(report.qty.0);
                }
                ExecKind::PartiallyFilled => {
                    summary.partially_filled += 1;
                    summary.filled_qty = summary.filled_qty.saturating_add(report.qty.0);
                }
                ExecKind::Cancelled => summary.cancelled += 1,
            }
        }
        summary
    }

    pub fn rejects(&self, reason: RejectReason) -> usize {
        self.rejected_by_reason
            .get(reason.as_str())
            .copied()
            .unwrap_or(0)
    }
}

/// Counts by market-data kind, plus traded quantity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MdSummary {
    pub total: usize,
    pub add: usize,
    pub reduce: usize,
    pub remove: usize,
    pub trade: usize,
    pub bbo: usize,
    pub status: usize,
    pub snapshots: usize,
    pub traded_qty: u64,
}

impl MdSummary {
    fn from_events(events: &[MdEvent], snapshots: usize) -> Self {
        let mut summary = Self {
            total: events.len(),
            add: 0,
            reduce: 0,
            remove: 0,
            trade: 0,
            bbo: 0,
            status: 0,
            snapshots,
            traded_qty: 0,
        };
        for event in events {
            match event.kind {
                MdKind::Add => summary.add += 1,
                MdKind::Reduce => summary.reduce += 1,
                MdKind::Remove => summary.remove += 1,
                MdKind::Trade => {
                    summary.trade += 1;
                    summary.traded_qty = summary.traded_qty.saturating_add(event.qty.0);
                }
                MdKind::Bbo => summary.bbo += 1,
                MdKind::Status => summary.status += 1,
                MdKind::Snapshot => summary.snapshots += 1,
            }
        }
        summary
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn standard_commands() -> Vec<Command> {
    let symbol = builders::default_symbol();
    let limit = |participant, side, price, qty, id| {
        builders::order_for(symbol, participant, side, price, qty, id)
    };
    let cancel = |participant, id| builders::cancel_for(symbol, participant, id);

    vec![
        // Two orders at the same price establish FIFO priority; a third sits
        // one level up the ask ladder.
        limit(1, Side::Sell, 105, 10, 1),
        limit(2, Side::Sell, 105, 5, 2),
        limit(3, Side::Sell, 106, 7, 3),
        // A resting bid that is cancelled before it trades.
        limit(4, Side::Buy, 100, 6, 4),
        // Aggressive buy sweeps level 105 and leaves a partial maker.
        limit(5, Side::Buy, 105, 12, 5),
        cancel(4, 4),
        // Trade at 104, then an IOC that partially fills and cancels the rest.
        limit(6, Side::Buy, 104, 3, 6),
        limit(7, Side::Sell, 104, 3, 7),
        limit(8, Side::Buy, 103, 4, 8),
        builders::ioc_for(symbol, 9, Side::Sell, 102, 5, 9),
        // FOK reject, post-only reject, then a normal aggressive buy.
        builders::fok_for(symbol, 10, Side::Buy, 107, 100, 10),
        builders::post_only_for(symbol, 11, Side::Buy, 106, 2, 11),
        limit(12, Side::Buy, 106, 2, 12),
        // A 9-lot sweep across three ask levels.
        limit(13, Side::Sell, 107, 9, 13),
        limit(14, Side::Buy, 107, 9, 14),
        // Duplicate id and unknown cancel both reject without side effects.
        limit(1, Side::Buy, 100, 1, 1),
        cancel(1, 999),
        // A market order with no opposite liquidity is cancelled in full.
        builders::market_for(symbol, 15, Side::Sell, 5, 15),
        // Market buy inside the collar.
        limit(16, Side::Sell, 106, 2, 16),
        builders::market_for(symbol, 17, Side::Buy, 2, 17),
        // Cancel a resting bid.
        limit(18, Side::Buy, 99, 5, 18),
        cancel(18, 18),
        // Trade at 98, leave a resting bid, then sweep it.
        limit(19, Side::Sell, 98, 1, 19),
        limit(20, Side::Buy, 98, 1, 20),
        limit(21, Side::Buy, 98, 3, 21),
        limit(22, Side::Sell, 98, 3, 22),
        // Clear the ask ladder so the self-trade test cannot accidentally hit
        // a cheaper resting ask first.
        cancel(13, 13),
        // Self-trade prevention cancels the resting maker; the aggressor's
        // remainder rests and then trades in price-time order.
        limit(1, Side::Sell, 120, 5, 23),
        limit(1, Side::Buy, 120, 2, 24),
        limit(2, Side::Buy, 120, 1, 25),
        limit(3, Side::Sell, 120, 3, 26),
        // A fresh ask level for the final state.
        limit(32, Side::Sell, 107, 8, 32),
        // A post-only order that does not cross rests; a FOK that is exactly
        // fillable succeeds.
        limit(27, Side::Buy, 105, 4, 27),
        limit(28, Side::Sell, 108, 2, 28),
        builders::post_only_for(symbol, 29, Side::Sell, 111, 2, 29),
        builders::fok_for(symbol, 30, Side::Sell, 105, 4, 30),
        // Leave both sides populated for the final-state assertion.
        limit(31, Side::Buy, 104, 6, 31),
    ]
}
