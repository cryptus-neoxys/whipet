// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic matching engine.
//!
//! The engine is a pure state machine over the input log. It reads no clock,
//! performs no I/O, and never allocates inside the matching loop (output
//! vectors are reused by the caller). Every output carries the input offset
//! that caused it, so replay can be verified against a state hash.

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use whippet_core::config::{ExchangeConfig, InstrumentConfig};
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{
    Command, CommandKind, ExecKind, ExecutionReport, FLAG_MAKER, FLAG_RESTED, FLAG_TAKER, MdEvent,
    MdKind, MdOrder, MdSnapshot, OrderType, RejectReason, Side,
};

use crate::book::{Book, BookError, BookSnapshot, RestingOrder, hash_book_snapshot};
use crate::dedup::{DedupEntry, DedupStatus, DedupTable, hash_dedup_entries};

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("book error: {0}")]
    Book(#[from] BookError),
    #[error("unknown symbol {0} in shard {1}")]
    UnknownSymbol(Symbol, u32),
    #[error("snapshot error: {0}")]
    Snapshot(String),
    #[error("state hash mismatch after restore: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

/// Hard upper bound for a single order quantity. The gateway enforces the same
/// bound; the engine also enforces it because the journal is a trust boundary.
pub const MAX_ORDER_QTY: u64 = 1_000_000_000;

#[derive(Default)]
pub struct EngineOutput {
    pub executions: Vec<ExecutionReport>,
    pub md: Vec<MdEvent>,
    pub snapshots: Vec<MdSnapshot>,
    /// True when this command landed on the engine-state persistence interval.
    /// The node uses it to save `snapshot_state`; the engine does not emit a
    /// full market-data snapshot for this cadence. Market-data snapshots are
    /// published separately on `snapshots`.
    pub state_snapshot_due: bool,
}

impl EngineOutput {
    pub fn with_capacity(executions: usize, md: usize) -> Self {
        Self {
            executions: Vec::with_capacity(executions),
            md: Vec::with_capacity(md),
            snapshots: Vec::new(),
            state_snapshot_due: false,
        }
    }

    pub fn clear(&mut self) {
        self.executions.clear();
        self.md.clear();
        self.snapshots.clear();
        self.state_snapshot_due = false;
    }
}

/// The instrument configuration fields that enter a symbol's state hash.
///
/// [`SymbolSnapshot`] stores these so [`hash_symbol_snapshot`] can reproduce
/// [`SymbolEngine::state_hash`] without consulting the live [`InstrumentConfig`]
/// (and therefore off the shard thread). `symbol` is omitted: it is already
/// carried on [`SymbolSnapshot::symbol`] and [`BookSnapshot::symbol`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolConfigSnapshot {
    pub tick_size_cents: i64,
    pub min_price_ticks: i64,
    pub max_price_ticks: i64,
    pub max_orders: usize,
    pub snapshot_depth: usize,
    pub market_collar_bps: u32,
}

impl SymbolConfigSnapshot {
    pub fn from_config(config: &InstrumentConfig) -> Self {
        Self {
            tick_size_cents: config.tick_size_cents,
            min_price_ticks: config.min_price_ticks,
            max_price_ticks: config.max_price_ticks,
            max_orders: config.max_orders,
            snapshot_depth: config.snapshot_depth,
            market_collar_bps: config.market_collar_bps,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolSnapshot {
    pub symbol: Symbol,
    /// The config fields that [`SymbolEngine::state_hash`] hashes. Present so
    /// the snapshot can be re-hashed without the live config.
    ///
    /// Schema note: this field (and `BookSnapshot::min_price_ticks`) extend the
    /// serialized snapshot. The on-disk format is versioned by the `whippet`
    /// crate's snapshot store; an upgrade that changes the layout must bump
    /// that version there.
    pub config: SymbolConfigSnapshot,
    pub book: BookSnapshot,
    pub dedup: Vec<(OrderId, DedupEntry)>,
    /// Indices into [`SymbolSnapshot::dedup`] for the terminal ids in eviction
    /// order, snapshotted so a restored table evicts at the same point as a
    /// continuous replay. A `u32` is 4 bytes per terminal id, where serde
    /// writes an [`OrderId`] as a 32-character hex string (40 bytes). Not part
    /// of the state hash.
    pub dedup_terminal_idx: Vec<u32>,
    pub exec_seq: Seq,
    pub md_seq: Seq,
    pub last_offset: u64,
    pub last_ts_ns: u64,
    pub commands: u64,
    pub halted: bool,
    pub state_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSnapshot {
    pub shard_id: u32,
    pub symbols: Vec<SymbolSnapshot>,
}

pub struct SymbolEngine {
    config: InstrumentConfig,
    book: Book,
    dedup: DedupTable,
    exec_seq: Seq,
    md_seq: Seq,
    last_offset: u64,
    last_ts_ns: u64,
    commands: u64,
    halted: bool,
    /// How often the node should persist engine state (book + dedup). This
    /// does not emit a market-data snapshot.
    snapshot_every: u64,
    /// How often a full market-data snapshot is emitted on `EngineOutput`.
    md_snapshot_every: u64,
    /// Commands left before the next state persistence, counted down instead
    /// of dividing `commands` on every command. Re-derived whenever
    /// `commands` jumps (restore) or an interval changes.
    until_state_snapshot: u64,
    /// Commands left before the next market-data snapshot. See
    /// [`SymbolEngine::until_state_snapshot`].
    until_md_snapshot: u64,
}

/// Commands left before `commands` next reaches a multiple of `every`.
fn commands_until(every: u64, commands: u64) -> u64 {
    let every = every.max(1);
    let remainder = commands % every;
    if remainder == 0 {
        every
    } else {
        every - remainder
    }
}

/// Soft cap on the dedup table per symbol. The table is the idempotency
/// window, not an audit log: at 5,000 commands/s this is roughly 20 seconds of
/// history. Keeping it bounded keeps the snapshot proportional to the window
/// instead of to the whole run (a 500k-entry snapshot took ~570 ms to write).
const DEDUP_WINDOW: usize = 100_000;

fn dedup_capacity(config: &InstrumentConfig) -> usize {
    config.max_orders.saturating_mul(2).min(DEDUP_WINDOW)
}

impl SymbolEngine {
    pub fn new(config: InstrumentConfig) -> Self {
        let dedup_capacity = dedup_capacity(&config);
        let book = Book::new(&config);
        let mut engine = Self {
            config,
            book,
            dedup: DedupTable::new(dedup_capacity),
            exec_seq: Seq(1),
            md_seq: Seq(1),
            last_offset: 0,
            last_ts_ns: 0,
            commands: 0,
            halted: false,
            snapshot_every: 10_000,
            md_snapshot_every: 100_000,
            until_state_snapshot: 0,
            until_md_snapshot: 0,
        };
        engine.rederive_snapshot_countdowns();
        engine
    }

    /// Recompute the countdowns from `commands`. The cadence is defined as
    /// "every command count that is a multiple of the interval", so a restore
    /// or a mid-stream interval change resumes at exactly the same command.
    fn rederive_snapshot_countdowns(&mut self) {
        self.until_state_snapshot = commands_until(self.snapshot_every, self.commands);
        self.until_md_snapshot = commands_until(self.md_snapshot_every, self.commands);
    }

    pub fn with_snapshot_interval(mut self, every: u64) -> Self {
        self.set_snapshot_interval(every);
        self
    }

    pub fn set_snapshot_interval(&mut self, every: u64) {
        self.snapshot_every = every.max(1);
        self.until_state_snapshot = commands_until(self.snapshot_every, self.commands);
    }

    pub fn with_md_snapshot_interval(mut self, every: u64) -> Self {
        self.set_md_snapshot_interval(every);
        self
    }

    pub fn set_md_snapshot_interval(&mut self, every: u64) {
        self.md_snapshot_every = every.max(1);
        self.until_md_snapshot = commands_until(self.md_snapshot_every, self.commands);
    }

    pub fn config(&self) -> &InstrumentConfig {
        &self.config
    }

    pub fn book(&self) -> &Book {
        &self.book
    }

    pub fn dedup(&self) -> &DedupTable {
        &self.dedup
    }

    pub fn last_offset(&self) -> u64 {
        self.last_offset
    }

    pub fn last_timestamp_ns(&self) -> u64 {
        self.last_ts_ns
    }

    pub fn commands(&self) -> u64 {
        self.commands
    }

    pub fn is_halted(&self) -> bool {
        self.halted
    }

    pub fn set_halted(&mut self, halted: bool) {
        self.halted = halted;
    }

    /// Apply one input command and append its outputs.
    pub fn process(&mut self, command: &Command, input_offset: u64, out: &mut EngineOutput) {
        self.last_offset = input_offset;
        self.last_ts_ns = self.last_ts_ns.max(command.timestamp_ns);
        self.commands = self.commands.saturating_add(1);

        match command.kind {
            CommandKind::Cancel => self.process_cancel(command, out),
            CommandKind::NewOrder => self.process_new_order(command, out),
        }

        out.state_snapshot_due = false;
        if self.until_state_snapshot > 0 {
            self.until_state_snapshot -= 1;
            if self.until_state_snapshot == 0 {
                out.state_snapshot_due = true;
                self.until_state_snapshot = self.snapshot_every;
            }
        }
        if self.until_md_snapshot > 0 {
            self.until_md_snapshot -= 1;
            if self.until_md_snapshot == 0 {
                out.snapshots.push(self.snapshot());
                self.until_md_snapshot = self.md_snapshot_every;
            }
        }
    }

    fn process_cancel(&mut self, command: &Command, out: &mut EngineOutput) {
        let removed = self
            .book
            .cancel(command.order_id, Some(command.participant));
        let Some(removed) = removed else {
            self.push_exec(
                out,
                ExecKind::Rejected,
                command,
                Price::ZERO,
                Qty::ZERO,
                Qty::ZERO,
                OrderId(0),
                RejectReason::UnknownOrder,
                0,
            );
            return;
        };

        self.dedup.update(removed.order_id, DedupStatus::Cancelled);
        self.push_exec(
            out,
            ExecKind::Cancelled,
            command,
            removed.price,
            removed.remaining,
            Qty::ZERO,
            OrderId(0),
            RejectReason::None,
            0,
        );
        self.push_md(
            out,
            MdKind::Remove,
            removed.side,
            removed.price,
            Qty::ZERO,
            removed.order_id,
            removed.participant,
            0,
        );
    }

    fn process_new_order(&mut self, command: &Command, out: &mut EngineOutput) {
        if command.symbol != self.config.symbol {
            self.reject(out, command, RejectReason::UnknownSymbol);
            return;
        }
        if self.halted {
            self.reject(out, command, RejectReason::Halted);
            return;
        }
        if command.qty.is_zero() || command.qty.0 > MAX_ORDER_QTY {
            self.reject(out, command, RejectReason::InvalidQty);
            return;
        }
        if command.order_id.0 == 0 {
            self.reject(out, command, RejectReason::InvalidQty);
            return;
        }
        if command.order_type.requires_price() && command.price.0 <= 0 {
            self.reject(out, command, RejectReason::InvalidPrice);
            return;
        }
        if command.order_type.requires_price() && !self.config.contains_ticks(command.price) {
            self.reject(out, command, RejectReason::PriceOutOfBand);
            return;
        }
        if self.dedup.contains(command.order_id) {
            self.reject(out, command, RejectReason::DuplicateOrderId);
            return;
        }

        let opposite = command.side.opposite();
        let best_opposite = self.book.best_index(opposite);
        let crosses = match command.order_type {
            OrderType::Market => best_opposite.is_some(),
            _ => match (command.side, best_opposite) {
                (Side::Buy, Some(index)) => self.book.price_at(index).0 <= command.price.0,
                (Side::Sell, Some(index)) => self.book.price_at(index).0 >= command.price.0,
                _ => false,
            },
        };

        if command.order_type == OrderType::PostOnly && crosses {
            self.reject(out, command, RejectReason::PostOnlyWouldCross);
            return;
        }

        if command.order_type == OrderType::Fok
            && !self.book.has_executable_qty_excluding(
                opposite,
                command.price,
                command.participant,
                command.qty,
            )
        {
            self.reject(out, command, RejectReason::FokUnfillable);
            return;
        }

        let entry = DedupEntry {
            status: DedupStatus::Accepted,
            input_offset: self.last_offset,
            timestamp_ns: command.timestamp_ns,
            seq: self.exec_seq,
        };
        self.dedup.insert(command.order_id, entry);

        let accepted_flags = if crosses
            || matches!(
                command.order_type,
                OrderType::Market | OrderType::Ioc | OrderType::Fok
            ) {
            FLAG_TAKER
        } else {
            FLAG_RESTED
        };
        self.push_exec(
            out,
            ExecKind::Accepted,
            command,
            command.price,
            command.qty,
            command.qty,
            OrderId(0),
            RejectReason::None,
            accepted_flags,
        );

        let mut remaining = command.qty;
        while !remaining.is_zero() {
            let Some(best_index) = self.book.best_index(opposite) else {
                break;
            };
            let maker_price = self.book.price_at(best_index);
            let price_crosses = match command.order_type {
                OrderType::Market => self.within_collar(command.side, maker_price),
                _ => match command.side {
                    Side::Buy => maker_price.0 <= command.price.0,
                    Side::Sell => maker_price.0 >= command.price.0,
                },
            };
            if !price_crosses {
                break;
            }
            let Some(head) = self.book.head(opposite, best_index) else {
                break;
            };
            let maker = *self.book.slot(head);

            // Cancel-resting self-trade prevention. The resting order is
            // removed, never the incoming one, so the aggressor keeps its
            // price-time priority at its own level if it later rests.
            if maker.participant == command.participant {
                if let Some(removed) = self.book.cancel(maker.order_id, None) {
                    self.dedup.update(maker.order_id, DedupStatus::Cancelled);
                    self.push_maker_exec(
                        out,
                        ExecKind::Cancelled,
                        command,
                        &removed,
                        RejectReason::SelfTradePrevention,
                    );
                    self.push_md(
                        out,
                        MdKind::Remove,
                        removed.side,
                        removed.price,
                        Qty::ZERO,
                        removed.order_id,
                        removed.participant,
                        0,
                    );
                }
                continue;
            }

            let fill = remaining.min(maker.remaining);
            let maker_remaining_after = maker.remaining.saturating_sub(fill);
            let maker_fully_filled = self.book.reduce(head, fill);
            remaining = remaining.saturating_sub(fill);
            self.book.record_trade(maker_price, fill, command.side);

            let kind = if remaining.is_zero() {
                ExecKind::Filled
            } else {
                ExecKind::PartiallyFilled
            };
            self.push_exec(
                out,
                kind,
                command,
                maker_price,
                fill,
                remaining,
                maker.order_id,
                RejectReason::None,
                FLAG_TAKER,
            );

            self.push_md(
                out,
                MdKind::Trade,
                command.side,
                maker_price,
                fill,
                command.order_id,
                command.participant,
                0,
            );

            if maker_fully_filled {
                self.dedup.update(maker.order_id, DedupStatus::Filled);
                self.push_md(
                    out,
                    MdKind::Remove,
                    maker.side,
                    maker_price,
                    Qty::ZERO,
                    maker.order_id,
                    maker.participant,
                    0,
                );
            } else {
                self.push_md(
                    out,
                    MdKind::Reduce,
                    maker.side,
                    maker_price,
                    maker_remaining_after,
                    maker.order_id,
                    maker.participant,
                    0,
                );
            }
        }

        if remaining.is_zero() {
            self.dedup.update(command.order_id, DedupStatus::Filled);
            return;
        }

        if matches!(command.order_type, OrderType::Limit | OrderType::PostOnly) {
            match self.book.insert(
                command.order_id,
                command.participant,
                command.side,
                command.price,
                remaining,
                self.exec_seq.0,
            ) {
                Ok(_) => {
                    self.dedup.update(command.order_id, DedupStatus::Resting);
                    self.push_md(
                        out,
                        MdKind::Add,
                        command.side,
                        command.price,
                        remaining,
                        command.order_id,
                        command.participant,
                        0,
                    );
                }
                Err(_) => {
                    // A full book is an operational failure, not a silent drop.
                    self.dedup.update(command.order_id, DedupStatus::Rejected);
                    self.push_exec(
                        out,
                        ExecKind::Cancelled,
                        command,
                        command.price,
                        remaining,
                        Qty::ZERO,
                        OrderId(0),
                        RejectReason::Internal,
                        FLAG_TAKER,
                    );
                }
            }
        } else {
            // Market/IOC/FOK remainder is cancelled. FOK normally filled above;
            // STP or a collar can still leave a remainder.
            self.dedup.update(command.order_id, DedupStatus::Cancelled);
            self.push_exec(
                out,
                ExecKind::Cancelled,
                command,
                command.price,
                remaining,
                Qty::ZERO,
                OrderId(0),
                RejectReason::None,
                FLAG_TAKER,
            );
        }
    }

    fn within_collar(&self, side: Side, price: Price) -> bool {
        let (last_price, _, _) = self.book.last_trade();
        let reference = if last_price.0 > 0 {
            last_price
        } else if let (Some(bid), Some(ask)) = (self.book.best_bid(), self.book.best_ask()) {
            Price((bid.0 + ask.0) / 2)
        } else {
            self.config.reference_price()
        };
        let collar = reference.0 * i64::from(self.config.market_collar_bps) / 10_000;
        match side {
            Side::Buy => price.0 <= reference.0 + collar,
            Side::Sell => price.0 >= reference.0 - collar,
        }
    }

    fn reject(&mut self, out: &mut EngineOutput, command: &Command, reason: RejectReason) {
        self.dedup.insert(
            command.order_id,
            DedupEntry {
                status: DedupStatus::Rejected,
                input_offset: self.last_offset,
                timestamp_ns: command.timestamp_ns,
                seq: self.exec_seq,
            },
        );
        self.push_exec(
            out,
            ExecKind::Rejected,
            command,
            command.price,
            Qty::ZERO,
            Qty::ZERO,
            OrderId(0),
            reason,
            0,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn push_exec(
        &mut self,
        out: &mut EngineOutput,
        kind: ExecKind,
        command: &Command,
        price: Price,
        qty: Qty,
        remaining: Qty,
        maker_order_id: OrderId,
        reason: RejectReason,
        flags: u16,
    ) {
        let seq = self.exec_seq.next_seq();
        out.executions.push(ExecutionReport {
            kind,
            symbol: command.symbol,
            side: command.side,
            order_type: command.order_type,
            price,
            qty,
            remaining,
            order_id: command.order_id,
            maker_order_id,
            seq,
            cause_offset: self.last_offset,
            timestamp_ns: command.timestamp_ns,
            reason,
            flags,
        });
    }

    fn push_maker_exec(
        &mut self,
        out: &mut EngineOutput,
        kind: ExecKind,
        command: &Command,
        removed: &RestingOrder,
        reason: RejectReason,
    ) {
        let seq = self.exec_seq.next_seq();
        out.executions.push(ExecutionReport {
            kind,
            symbol: command.symbol,
            side: removed.side,
            order_type: OrderType::Limit,
            price: removed.price,
            qty: removed.remaining,
            remaining: Qty::ZERO,
            order_id: removed.order_id,
            maker_order_id: OrderId(0),
            seq,
            cause_offset: self.last_offset,
            timestamp_ns: command.timestamp_ns,
            reason,
            flags: FLAG_MAKER,
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn push_md(
        &mut self,
        out: &mut EngineOutput,
        kind: MdKind,
        side: Side,
        price: Price,
        qty: Qty,
        order_id: OrderId,
        participant: ParticipantId,
        flags: u8,
    ) {
        let seq = self.md_seq.next_seq();
        out.md.push(MdEvent {
            kind,
            symbol: self.config.symbol,
            seq,
            cause_offset: self.last_offset,
            timestamp_ns: self.last_ts_ns,
            side,
            flags,
            price,
            qty,
            order_id,
            participant,
        });
    }

    pub fn snapshot(&self) -> MdSnapshot {
        let (bids, asks) = self.book.snapshot_levels(self.config.snapshot_depth);
        let (last_trade_price, last_trade_qty, _) = self.book.last_trade();
        let book_snapshot = self.book.snapshot();
        let orders = book_snapshot
            .orders
            .iter()
            .map(|order| MdOrder {
                order_id: order.order_id,
                side: order.side,
                price: order.price,
                qty: order.remaining,
                participant: order.participant,
            })
            .collect();
        MdSnapshot {
            symbol: self.config.symbol,
            // The snapshot describes the state through the last emitted MD
            // event, not the next sequence to use. At startup this is 0, so the
            // first event is 1 and the sequence never repeats.
            seq: Seq(self.md_seq.0.saturating_sub(1)),
            cause_offset: self.last_offset,
            timestamp_ns: self.last_ts_ns,
            last_trade_price,
            last_trade_qty,
            bids,
            asks,
            orders,
        }
    }

    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"whippet-engine-v1");
        hasher.update(&self.config.tick_size_cents.to_le_bytes());
        hasher.update(&self.config.min_price_ticks.to_le_bytes());
        hasher.update(&self.config.max_price_ticks.to_le_bytes());
        hasher.update(&(self.config.max_orders as u64).to_le_bytes());
        hasher.update(&(self.config.snapshot_depth as u64).to_le_bytes());
        hasher.update(&u64::from(self.config.market_collar_bps).to_le_bytes());
        hasher.update(&self.commands.to_le_bytes());
        self.book.hash_into(&mut hasher);
        self.dedup.hash_into(&mut hasher);
        hasher.update(&self.exec_seq.0.to_le_bytes());
        hasher.update(&self.md_seq.0.to_le_bytes());
        hasher.update(&self.last_offset.to_le_bytes());
        hasher.update(&self.last_ts_ns.to_le_bytes());
        hasher.update(&[u8::from(self.halted)]);
        *hasher.finalize().as_bytes()
    }

    /// Snapshot after compacting the dedup queues.
    ///
    /// The node uses this path so the saved insertion walk is exactly the live
    /// walk; the plain `snapshot_state` stays for tests that do not need it.
    pub fn snapshot_state_compacted(&mut self) -> SymbolSnapshot {
        self.dedup.compact();
        self.snapshot_state()
    }

    pub fn snapshot_state_without_hash_compacted(&mut self) -> SymbolSnapshot {
        self.dedup.compact();
        self.snapshot_state_without_hash()
    }

    pub fn snapshot_state(&self) -> SymbolSnapshot {
        let mut snapshot = self.snapshot_state_without_hash();
        snapshot.state_hash = self.state_hash();
        snapshot
    }

    /// Build the same snapshot as [`SymbolEngine::snapshot_state`] but leave
    /// `state_hash` zeroed. The caller fills it later, off the shard thread,
    /// with [`hash_symbol_snapshot`].
    pub fn snapshot_state_without_hash(&self) -> SymbolSnapshot {
        // One walk, reused for both the serialized entries and the compact
        // terminal queue: walking the dedup table twice was pure cost.
        let dedup = self.dedup.entries_in_order();
        let dedup_terminal_idx = self.dedup.terminal_indices(&dedup);
        SymbolSnapshot {
            symbol: self.config.symbol,
            config: SymbolConfigSnapshot::from_config(&self.config),
            book: self.book.snapshot(),
            dedup,
            dedup_terminal_idx,
            exec_seq: self.exec_seq,
            md_seq: self.md_seq,
            last_offset: self.last_offset,
            last_ts_ns: self.last_ts_ns,
            commands: self.commands,
            halted: self.halted,
            state_hash: [0; 32],
        }
    }

    pub fn from_snapshot(
        config: InstrumentConfig,
        snapshot: SymbolSnapshot,
    ) -> Result<Self, EngineError> {
        let dedup_capacity = dedup_capacity(&config);
        let book = Book::from_snapshot(&config, snapshot.book)?;
        let mut engine = Self {
            config,
            book,
            dedup: DedupTable::from_entries(
                dedup_capacity,
                snapshot.dedup,
                snapshot.dedup_terminal_idx,
            )
            .map_err(EngineError::Snapshot)?,
            exec_seq: snapshot.exec_seq,
            md_seq: snapshot.md_seq,
            last_offset: snapshot.last_offset,
            last_ts_ns: snapshot.last_ts_ns,
            commands: snapshot.commands,
            halted: snapshot.halted,
            snapshot_every: 10_000,
            md_snapshot_every: 100_000,
            until_state_snapshot: 0,
            until_md_snapshot: 0,
        };
        engine.rederive_snapshot_countdowns();
        let actual = engine.state_hash();
        if actual != snapshot.state_hash {
            return Err(EngineError::HashMismatch {
                expected: hex(&snapshot.state_hash),
                actual: hex(&actual),
            });
        }
        // Re-derive the book from the snapshot once more to ensure the arena
        // order is canonical, then verify the hash again.
        let canonical = engine.snapshot_state();
        if canonical.state_hash != snapshot.state_hash {
            return Err(EngineError::HashMismatch {
                expected: hex(&snapshot.state_hash),
                actual: hex(&canonical.state_hash),
            });
        }
        engine.last_offset = snapshot.last_offset;
        Ok(engine)
    }
}

/// Compute a symbol's state hash from a [`SymbolSnapshot`], byte-identically to
/// [`SymbolEngine::state_hash`].
///
/// This is the off-shard half of snapshot persistence: the shard thread calls
/// [`SymbolEngine::snapshot_state_without_hash`] and hands the snapshot to the
/// writer, which calls this (or [`fill_symbol_hashes`]) instead of walking the
/// live ladder. The field order and the `b"whippet-engine-v1"` domain prefix
/// match [`SymbolEngine::state_hash`] exactly.
/// Hash the dedup state carried by a snapshot.
///
/// The on-disk terminal queue is stored as indices into `snapshot.dedup`; the
/// hash is over the resolved ids so it matches the live table's
/// [`DedupTable::hash_into`] byte for byte. An out-of-range index is hashed as
/// an impossible order id rather than panicking: the restore path rejects it,
/// and a tampered snapshot's recomputed hash will not match the stored one.
fn hash_dedup_snapshot(snapshot: &SymbolSnapshot, hasher: &mut blake3::Hasher) {
    let terminal: Vec<OrderId> = snapshot
        .dedup_terminal_idx
        .iter()
        .map(|index| {
            snapshot
                .dedup
                .get(*index as usize)
                .map(|(order_id, _)| *order_id)
                .unwrap_or(OrderId(u128::MAX))
        })
        .collect();
    hash_dedup_entries(&snapshot.dedup, &terminal, hasher);
}

pub fn hash_symbol_snapshot(snapshot: &SymbolSnapshot) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"whippet-engine-v1");
    let config = &snapshot.config;
    hasher.update(&config.tick_size_cents.to_le_bytes());
    hasher.update(&config.min_price_ticks.to_le_bytes());
    hasher.update(&config.max_price_ticks.to_le_bytes());
    hasher.update(&(config.max_orders as u64).to_le_bytes());
    hasher.update(&(config.snapshot_depth as u64).to_le_bytes());
    hasher.update(&u64::from(config.market_collar_bps).to_le_bytes());
    hasher.update(&snapshot.commands.to_le_bytes());
    hash_book_snapshot(&snapshot.book, &mut hasher);
    hash_dedup_snapshot(snapshot, &mut hasher);
    hasher.update(&snapshot.exec_seq.0.to_le_bytes());
    hasher.update(&snapshot.md_seq.0.to_le_bytes());
    hasher.update(&snapshot.last_offset.to_le_bytes());
    hasher.update(&snapshot.last_ts_ns.to_le_bytes());
    hasher.update(&[u8::from(snapshot.halted)]);
    *hasher.finalize().as_bytes()
}

pub struct ShardEngine {
    shard_id: u32,
    engines: FxHashMap<Symbol, SymbolEngine>,
}

impl ShardEngine {
    pub fn new(shard_id: u32, exchange: &ExchangeConfig) -> Self {
        let mut engines = FxHashMap::default();
        for instrument in &exchange.instruments {
            if instrument.symbol.partition(exchange.partitions) == shard_id {
                engines.insert(instrument.symbol, SymbolEngine::new(instrument.clone()));
            }
        }
        Self { shard_id, engines }
    }

    pub fn new_with_snapshot_interval(
        shard_id: u32,
        exchange: &ExchangeConfig,
        every: u64,
    ) -> Self {
        let mut shard = Self::new(shard_id, exchange);
        for engine in shard.engines.values_mut() {
            engine.set_snapshot_interval(every);
        }
        shard
    }

    pub fn set_snapshot_interval(&mut self, every: u64) {
        for engine in self.engines.values_mut() {
            engine.set_snapshot_interval(every);
        }
    }

    pub fn set_md_snapshot_interval(&mut self, every: u64) {
        for engine in self.engines.values_mut() {
            engine.set_md_snapshot_interval(every);
        }
    }

    pub fn shard_id(&self) -> u32 {
        self.shard_id
    }

    pub fn symbols(&self) -> impl Iterator<Item = Symbol> + '_ {
        self.engines.keys().copied()
    }

    pub fn engine(&self, symbol: Symbol) -> Option<&SymbolEngine> {
        self.engines.get(&symbol)
    }

    pub fn engine_mut(&mut self, symbol: Symbol) -> Option<&mut SymbolEngine> {
        self.engines.get_mut(&symbol)
    }

    pub fn process(
        &mut self,
        command: &Command,
        input_offset: u64,
        out: &mut EngineOutput,
    ) -> Result<(), EngineError> {
        out.clear();
        let engine = self
            .engines
            .get_mut(&command.symbol)
            .ok_or(EngineError::UnknownSymbol(command.symbol, self.shard_id))?;
        engine.process(command, input_offset, out);
        Ok(())
    }

    pub fn snapshot_state(&self) -> ShardSnapshot {
        let mut symbols: Vec<_> = self
            .engines
            .values()
            .map(SymbolEngine::snapshot_state)
            .collect();
        symbols.sort_unstable_by_key(|snapshot| snapshot.symbol);
        ShardSnapshot {
            shard_id: self.shard_id,
            symbols,
        }
    }

    /// Build the shard snapshot with every symbol's `state_hash` zeroed. The
    /// hashes are filled later, off the shard thread, with
    /// [`fill_symbol_hashes`].
    pub fn snapshot_state_without_hash(&self) -> ShardSnapshot {
        let mut symbols: Vec<_> = self
            .engines
            .values()
            .map(SymbolEngine::snapshot_state_without_hash)
            .collect();
        symbols.sort_unstable_by_key(|snapshot| snapshot.symbol);
        ShardSnapshot {
            shard_id: self.shard_id,
            symbols,
        }
    }

    /// Snapshot after compacting every symbol's dedup queues.
    ///
    /// The node snapshots through this path: the saved insertion walk matches
    /// the live walk, so a restored shard hashes identically even after the
    /// tail re-admits an id that was dead at snapshot time.
    pub fn snapshot_state_without_hash_compacted(&mut self) -> ShardSnapshot {
        for engine in self.engines.values_mut() {
            engine.dedup.compact();
        }
        self.snapshot_state_without_hash()
    }

    pub fn from_snapshot(
        exchange: &ExchangeConfig,
        snapshot: ShardSnapshot,
    ) -> Result<Self, EngineError> {
        let expected: Vec<Symbol> = exchange
            .instruments
            .iter()
            .filter(|instrument| {
                instrument.symbol.partition(exchange.partitions) == snapshot.shard_id
            })
            .map(|instrument| instrument.symbol)
            .collect();
        let found: Vec<Symbol> = snapshot
            .symbols
            .iter()
            .map(|symbol| symbol.symbol)
            .collect();
        if expected != found {
            return Err(EngineError::Snapshot(format!(
                "shard {} snapshot symbols {:?} do not match configured symbols {:?}",
                snapshot.shard_id, found, expected
            )));
        }
        let mut engines = FxHashMap::default();
        for symbol_snapshot in snapshot.symbols {
            let expected_shard = symbol_snapshot.symbol.partition(exchange.partitions);
            if expected_shard != snapshot.shard_id {
                return Err(EngineError::Snapshot(format!(
                    "symbol {} belongs to shard {} under {} partitions, but the snapshot is for shard {}",
                    symbol_snapshot.symbol, expected_shard, exchange.partitions, snapshot.shard_id
                )));
            }
            let config = exchange
                .instrument(symbol_snapshot.symbol)
                .ok_or(EngineError::UnknownSymbol(
                    symbol_snapshot.symbol,
                    snapshot.shard_id,
                ))?
                .clone();
            let engine = SymbolEngine::from_snapshot(config, symbol_snapshot)?;
            engines.insert(engine.config.symbol, engine);
        }
        Ok(Self {
            shard_id: snapshot.shard_id,
            engines,
        })
    }

    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"whippet-shard-v1");
        hasher.update(&self.shard_id.to_le_bytes());
        // Hash each symbol engine directly instead of materializing full
        // snapshots. Symbol order is canonical (sorted), so the bytes match the
        // previous snapshot-based computation exactly.
        let mut engines: Vec<_> = self.engines.iter().collect();
        engines.sort_unstable_by_key(|(symbol, _)| **symbol);
        for (_, engine) in engines {
            hasher.update(&engine.state_hash());
        }
        *hasher.finalize().as_bytes()
    }

    /// Replay a sequence of commands from a known offset. Used by tests and by
    /// the `verify` CLI to prove determinism.
    pub fn replay<'a, I>(&mut self, commands: I) -> Result<EngineOutput, EngineError>
    where
        I: IntoIterator<Item = (u64, &'a Command)>,
    {
        let mut out = EngineOutput::with_capacity(16, 16);
        let mut last = EngineOutput::default();
        for (offset, command) in commands {
            self.process(command, offset, &mut out)?;
            std::mem::swap(&mut last, &mut out);
        }
        Ok(last)
    }
}

/// Fill each symbol's `state_hash` in a [`ShardSnapshot`] with
/// [`hash_symbol_snapshot`]. This is the off-shard counterpart to
/// [`ShardEngine::snapshot_state_without_hash`]: the shard thread produces the
/// unhashed snapshot, and the snapshot writer calls this before persisting.
pub fn fill_symbol_hashes(snapshot: &mut ShardSnapshot) {
    for symbol in &mut snapshot.symbols {
        symbol.state_hash = hash_symbol_snapshot(symbol);
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

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::config::InstrumentConfig;

    fn config() -> InstrumentConfig {
        let mut config = InstrumentConfig::new("BTC-USD", 100, 100, 200).unwrap();
        config.max_orders = 1024;
        config.snapshot_depth = 5;
        config
    }

    fn symbol() -> Symbol {
        Symbol::new("BTC-USD").unwrap()
    }

    fn limit(side: Side, price: i64, qty: u64, id: u128, participant: u64) -> Command {
        Command::new_order(
            symbol(),
            side,
            OrderType::Limit,
            Price(price),
            Qty(qty),
            OrderId(id),
            ParticipantId(participant),
            1,
        )
    }

    fn process(engine: &mut SymbolEngine, command: &Command, offset: u64) -> EngineOutput {
        let mut out = EngineOutput::with_capacity(8, 8);
        engine.process(command, offset, &mut out);
        out
    }

    #[test]
    fn resting_order_emits_accepted_and_add() {
        let mut engine = SymbolEngine::new(config());
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Accepted);
        assert_eq!(out.executions[0].flags & FLAG_RESTED, FLAG_RESTED);
        assert_eq!(out.md.len(), 1);
        assert_eq!(out.md[0].kind, MdKind::Add);
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
    }

    #[test]
    fn aggressive_order_fills_price_time_priority() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 100, 7, 2, 2), 1);
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 3, 3), 2);

        let fills: Vec<_> = out
            .executions
            .iter()
            .filter(|exec| {
                matches!(exec.kind, ExecKind::PartiallyFilled | ExecKind::Filled)
                    && exec.maker_order_id != OrderId(0)
            })
            .collect();
        assert_eq!(fills.len(), 2);
        assert_eq!(fills[0].maker_order_id, OrderId(1));
        assert_eq!(fills[0].qty, Qty(5));
        assert_eq!(fills[1].maker_order_id, OrderId(2));
        assert_eq!(fills[1].qty, Qty(5));
        assert_eq!(fills[1].kind, ExecKind::Filled);
        assert_eq!(engine.book().best_ask(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Sell, 0), Qty(2));
    }

    #[test]
    fn partial_fill_rests_remainder() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 2, 2), 1);
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::PartiallyFilled)
        );
        assert_eq!(engine.book().best_bid(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Buy, 0), Qty(6));
    }

    #[test]
    fn ioc_cancels_remainder() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut ioc = limit(Side::Buy, 100, 10, 2, 2);
        ioc.order_type = OrderType::Ioc;
        let out = process(&mut engine, &ioc, 1);
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::Cancelled)
        );
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn fok_rejects_when_not_fillable() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut fok = limit(Side::Buy, 100, 10, 2, 2);
        fok.order_type = OrderType::Fok;
        let out = process(&mut engine, &fok, 1);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::FokUnfillable);
    }

    #[test]
    fn post_only_rejects_if_crossing() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 4, 1, 1), 0);
        let mut post = limit(Side::Buy, 100, 10, 2, 2);
        post.order_type = OrderType::PostOnly;
        let out = process(&mut engine, &post, 1);
        assert_eq!(out.executions[0].reason, RejectReason::PostOnlyWouldCross);
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn duplicate_order_id_is_rejected() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let out = process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 1);
        assert_eq!(out.executions[0].reason, RejectReason::DuplicateOrderId);
        assert_eq!(engine.book().level_qty(Side::Buy, 0), Qty(10));
    }

    #[test]
    fn self_trade_prevention_cancels_resting() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        let out = process(&mut engine, &limit(Side::Buy, 100, 5, 2, 1), 1);
        assert!(out.executions.iter().any(|exec| {
            exec.kind == ExecKind::Cancelled && exec.reason == RejectReason::SelfTradePrevention
        }));
        assert_eq!(engine.book().best_ask(), None);
    }

    #[test]
    fn market_order_fills_and_cancels_remainder() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        let mut market = Command::new_order(
            symbol(),
            Side::Buy,
            OrderType::Market,
            Price::ZERO,
            Qty(10),
            OrderId(2),
            ParticipantId(2),
            2,
        );
        market.timestamp_ns = 2;
        let out = process(&mut engine, &market, 1);
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::PartiallyFilled)
        );
        assert!(
            out.executions
                .iter()
                .any(|exec| exec.kind == ExecKind::Cancelled)
        );
        assert_eq!(engine.book().best_ask(), None);
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn fok_rejects_self_trade_liquidity_without_touching_the_book() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 10, 1, 1), 0);
        let mut fok = limit(Side::Buy, 100, 10, 2, 1);
        fok.order_type = OrderType::Fok;
        let out = process(&mut engine, &fok, 1);
        assert_eq!(out.executions.len(), 1);
        assert_eq!(out.executions[0].kind, ExecKind::Rejected);
        assert_eq!(out.executions[0].reason, RejectReason::FokUnfillable);
        assert_eq!(engine.book().best_ask(), Some(Price(100)));
        assert_eq!(engine.book().level_qty(Side::Sell, 0), Qty(10));
    }

    #[test]
    fn oversized_quantity_is_rejected() {
        let mut engine = SymbolEngine::new(config());
        let mut command = limit(Side::Buy, 100, 1, 1, 1);
        command.qty = Qty(MAX_ORDER_QTY + 1);
        let out = process(&mut engine, &command, 0);
        assert_eq!(out.executions[0].reason, RejectReason::InvalidQty);
        assert!(engine.book().is_empty());
    }

    #[test]
    fn zero_order_id_is_rejected() {
        let mut engine = SymbolEngine::new(config());
        let command = limit(Side::Buy, 100, 1, 0, 1);
        let out = process(&mut engine, &command, 0);
        assert_eq!(out.executions[0].reason, RejectReason::InvalidQty);
    }

    #[test]
    fn cancel_then_replace_loses_time_priority() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 100, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 100, 5, 2, 2), 1);
        process(
            &mut engine,
            &Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2),
            2,
        );
        process(&mut engine, &limit(Side::Sell, 100, 5, 3, 1), 3);
        let out = process(&mut engine, &limit(Side::Buy, 100, 5, 4, 3), 4);
        let fill = out
            .executions
            .iter()
            .find(|exec| exec.kind == ExecKind::Filled)
            .expect("fill");
        assert_eq!(fill.maker_order_id, OrderId(2));
    }

    #[test]
    fn cancel_removes_order() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        let cancel = Command::cancel(symbol(), OrderId(1), ParticipantId(1), 2);
        let out = process(&mut engine, &cancel, 1);
        assert_eq!(out.executions[0].kind, ExecKind::Cancelled);
        assert_eq!(engine.book().best_bid(), None);
    }

    #[test]
    fn state_hash_is_deterministic_and_replayable() {
        let commands: Vec<(u64, Command)> = vec![
            (0, limit(Side::Sell, 105, 10, 1, 1)),
            (1, limit(Side::Sell, 104, 5, 2, 2)),
            (2, limit(Side::Buy, 100, 7, 3, 3)),
            (3, limit(Side::Buy, 104, 8, 4, 4)),
            (4, limit(Side::Buy, 103, 3, 5, 5)),
        ];
        let mut a = SymbolEngine::new(config());
        let mut b = SymbolEngine::new(config());
        for (offset, command) in &commands {
            process(&mut a, command, *offset);
            process(&mut b, command, *offset);
        }
        assert_eq!(a.state_hash(), b.state_hash());
    }

    #[test]
    fn snapshot_restores_exact_state_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 100, 7, 2, 2), 1);
        let snapshot = engine.snapshot_state();
        let restored = SymbolEngine::from_snapshot(config(), snapshot).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
        assert_eq!(restored.book().best_ask(), engine.book().best_ask());
    }

    #[test]
    fn a_reordered_terminal_queue_changes_the_snapshot_hash() {
        // The terminal queue is not derivable from the map: it decides which
        // id is evicted next. Two cancelled orders give a queue with a real
        // order to reorder. If the queue were not hashed, a snapshot whose
        // indices were shuffled at rest would verify and restore a table that
        // evicts differently from a continuous replay.
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 90, 1, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 91, 1, 2, 2), 1);
        for (offset, id) in [(2u64, 1u128), (3, 2)] {
            let cancel = Command::cancel(symbol(), OrderId(id), ParticipantId(id as u64), offset);
            process(&mut engine, &cancel, offset);
        }
        let snapshot = engine.snapshot_state_without_hash_compacted();
        assert_eq!(snapshot.dedup_terminal_idx.len(), 2);
        assert_eq!(hash_symbol_snapshot(&snapshot), engine.state_hash());

        let mut reordered = snapshot.clone();
        reordered.dedup_terminal_idx.reverse();
        assert_ne!(
            hash_symbol_snapshot(&reordered),
            hash_symbol_snapshot(&snapshot),
            "a reordered terminal queue must change the snapshot hash"
        );
        reordered.state_hash = hash_symbol_snapshot(&reordered);
        let restored = SymbolEngine::from_snapshot(config(), reordered)
            .expect("both orderings are internally valid");
        assert_ne!(
            restored.state_hash(),
            engine.state_hash(),
            "verify must be able to see the reordered queue"
        );
    }

    #[test]
    fn malformed_terminal_indices_are_rejected() {
        // Terminal indices are not hashed, so a corrupt list must fail closed
        // here; otherwise a restored table could evict differently from a
        // continuous replay while every hash still matches.
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Buy, 100, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 105, 4, 2, 2), 1);
        let mut snapshot = engine.snapshot_state_without_hash();
        // Out of range, and an index that points at a non-terminal entry.
        snapshot.dedup_terminal_idx = vec![u32::MAX, 0];
        snapshot.state_hash = hash_symbol_snapshot(&snapshot);
        let err = match SymbolEngine::from_snapshot(config(), snapshot) {
            Ok(_) => panic!("a corrupt terminal queue must be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, EngineError::Snapshot(_)), "{err:?}");
    }

    #[test]
    fn state_and_md_snapshot_intervals_are_independent() {
        let mut engine = SymbolEngine::new(config())
            .with_snapshot_interval(10)
            .with_md_snapshot_interval(100);
        let mut out = EngineOutput::with_capacity(8, 8);
        let mut state_due_at = Vec::new();
        let mut md_snapshot_at = Vec::new();

        for offset in 0..100u64 {
            out.clear();
            engine.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 1, 1),
                offset,
                &mut out,
            );
            if out.state_snapshot_due {
                state_due_at.push(engine.commands());
            }
            if !out.snapshots.is_empty() {
                assert_eq!(out.snapshots.len(), 1, "one MD snapshot per due command");
                md_snapshot_at.push(engine.commands());
            }
        }

        assert_eq!(
            state_due_at,
            (1..=10).map(|n| n * 10).collect::<Vec<u64>>(),
            "state persistence is due every ten commands"
        );
        assert_eq!(
            md_snapshot_at,
            vec![100],
            "market-data snapshots follow the separate, slower interval"
        );
    }

    #[test]
    fn md_snapshot_interval_setters_do_not_touch_state_interval() {
        let mut engine = SymbolEngine::new(config());
        engine.set_snapshot_interval(1_000);
        engine.set_md_snapshot_interval(3);
        let mut out = EngineOutput::with_capacity(8, 8);
        let mut state_due = 0usize;
        let mut md_snapshots = 0usize;

        for offset in 0..6u64 {
            out.clear();
            engine.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 1, 1),
                offset,
                &mut out,
            );
            state_due += usize::from(out.state_snapshot_due);
            md_snapshots += out.snapshots.len();
        }

        assert_eq!(state_due, 0, "state interval of 1000 was not reached");
        assert_eq!(md_snapshots, 2, "MD snapshots at commands 3 and 6");
    }

    #[test]
    fn snapshot_intervals_do_not_affect_state_hash() {
        let commands = [
            limit(Side::Sell, 105, 10, 1, 1),
            limit(Side::Buy, 100, 7, 2, 2),
            limit(Side::Buy, 105, 3, 3, 3),
            limit(Side::Sell, 104, 4, 4, 4),
        ];
        let mut eager = SymbolEngine::new(config())
            .with_snapshot_interval(1)
            .with_md_snapshot_interval(1);
        let mut lazy = SymbolEngine::new(config())
            .with_snapshot_interval(u64::MAX)
            .with_md_snapshot_interval(u64::MAX);
        let mut out = EngineOutput::with_capacity(8, 8);
        for (offset, command) in commands.iter().enumerate() {
            eager.process(command, offset as u64, &mut out);
            out.clear();
            lazy.process(command, offset as u64, &mut out);
            out.clear();
        }
        assert_eq!(eager.state_hash(), lazy.state_hash());
    }

    #[test]
    fn snapshot_cadence_resumes_from_the_restored_command_count() {
        let mut engine = SymbolEngine::new(config())
            .with_snapshot_interval(10)
            .with_md_snapshot_interval(10);
        let mut out = EngineOutput::with_capacity(8, 8);
        for offset in 0..7u64 {
            engine.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 1, 1),
                offset,
                &mut out,
            );
            out.clear();
        }
        assert_eq!(engine.commands(), 7);

        let mut restored =
            SymbolEngine::from_snapshot(config(), engine.snapshot_state()).expect("restore");
        restored.set_snapshot_interval(10);
        restored.set_md_snapshot_interval(10);

        // `commands` is 7, so command 10 is the next due one: three commands
        // away, exactly as `commands % interval` would have said before the
        // countdown replaced the division.
        for offset in 7..9u64 {
            restored.process(
                &limit(Side::Buy, 100, 1, u128::from(offset) + 100, 1),
                offset,
                &mut out,
            );
            assert!(!out.state_snapshot_due);
            assert!(out.snapshots.is_empty());
            out.clear();
        }
        restored.process(&limit(Side::Buy, 100, 1, 200, 1), 9, &mut out);
        assert!(out.state_snapshot_due);
        assert_eq!(out.snapshots.len(), 1);
        assert_eq!(restored.commands(), 10);
    }

    #[test]
    fn shard_state_hash_matches_sorted_symbol_hashes() {
        let mut eth_config = config();
        eth_config.symbol = Symbol::new("ETH-USD").unwrap();
        let exchange = ExchangeConfig {
            venue: "test".into(),
            partitions: 2,
            mode: whippet_core::config::Mode::Durable,
            instruments: vec![config(), eth_config],
        };
        let btc = symbol();
        let eth = Symbol::new("ETH-USD").unwrap();
        let shard = btc.partition(exchange.partitions);
        assert_eq!(
            shard,
            eth.partition(exchange.partitions),
            "this test needs both symbols on the same shard"
        );

        let mut engine = ShardEngine::new(shard, &exchange);
        let mut out = EngineOutput::with_capacity(8, 8);
        let commands = [
            (btc, Side::Buy, 100i64),
            (eth, Side::Sell, 101),
            (btc, Side::Sell, 102),
            (eth, Side::Buy, 99),
        ];
        for (offset, (sym, side, price)) in commands.into_iter().enumerate() {
            let command = Command::new_order(
                sym,
                side,
                OrderType::Limit,
                Price(price),
                Qty(5),
                OrderId(offset as u128 + 1),
                ParticipantId(1),
                offset as u64,
            );
            engine.process(&command, offset as u64, &mut out).unwrap();
            out.clear();
        }

        // The shard hash must be exactly the sorted per-symbol state hashes.
        let mut symbols: Vec<Symbol> = engine.symbols().collect();
        symbols.sort_unstable();
        let mut expected = blake3::Hasher::new();
        expected.update(b"whippet-shard-v1");
        expected.update(&shard.to_le_bytes());
        for sym in symbols {
            let state_hash = engine
                .engine(sym)
                .expect("symbol engine")
                .snapshot_state()
                .state_hash;
            expected.update(&state_hash);
        }
        assert_eq!(engine.state_hash(), *expected.finalize().as_bytes());
    }

    #[test]
    fn book_never_crosses() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 106, 4, 2, 2), 1);
        let best_bid = engine.book().best_bid();
        let best_ask = engine.book().best_ask();
        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
            assert!(bid.0 < ask.0, "crossed book: {bid:?} >= {ask:?}");
        }
    }

    #[test]
    fn snapshot_hash_reproduces_live_state_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Sell, 105, 5, 2, 2), 1);
        process(&mut engine, &limit(Side::Buy, 105, 12, 3, 3), 2);
        process(&mut engine, &limit(Side::Buy, 100, 6, 4, 4), 3);
        let snapshot = engine.snapshot_state_without_hash();
        assert_eq!(snapshot.state_hash, [0; 32], "sentinel is left zeroed");
        assert_eq!(hash_symbol_snapshot(&snapshot), engine.state_hash());
    }

    #[test]
    fn snapshot_hash_matches_full_snapshot_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 104, 5, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 104, 7, 2, 2), 1);
        let full = engine.snapshot_state();
        assert_eq!(hash_symbol_snapshot(&full), full.state_hash);
    }

    #[test]
    fn restored_engine_verifies_off_thread_hash() {
        let mut engine = SymbolEngine::new(config());
        process(&mut engine, &limit(Side::Sell, 105, 10, 1, 1), 0);
        process(&mut engine, &limit(Side::Buy, 100, 7, 2, 2), 1);
        let mut snapshot = engine.snapshot_state_without_hash();
        snapshot.state_hash = hash_symbol_snapshot(&snapshot);
        let restored = SymbolEngine::from_snapshot(config(), snapshot).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
    }

    #[test]
    fn fill_symbol_hashes_matches_per_symbol_snapshots() {
        let mut eth_config = config();
        eth_config.symbol = Symbol::new("ETH-USD").unwrap();
        let exchange = ExchangeConfig {
            venue: "test".into(),
            partitions: 2,
            mode: whippet_core::config::Mode::Durable,
            instruments: vec![config(), eth_config],
        };
        let btc = symbol();
        let eth = Symbol::new("ETH-USD").unwrap();
        let shard = btc.partition(exchange.partitions);
        assert_eq!(shard, eth.partition(exchange.partitions));

        let mut engine = ShardEngine::new(shard, &exchange);
        let mut out = EngineOutput::with_capacity(8, 8);
        let commands = [
            (btc, Side::Buy, 100i64),
            (eth, Side::Sell, 101),
            (btc, Side::Sell, 102),
            (eth, Side::Buy, 99),
        ];
        for (offset, (sym, side, price)) in commands.into_iter().enumerate() {
            let command = Command::new_order(
                sym,
                side,
                OrderType::Limit,
                Price(price),
                Qty(5),
                OrderId(offset as u128 + 1),
                ParticipantId(1),
                offset as u64,
            );
            engine.process(&command, offset as u64, &mut out).unwrap();
            out.clear();
        }

        let mut unhashed = engine.snapshot_state_without_hash();
        fill_symbol_hashes(&mut unhashed);
        assert_eq!(unhashed.shard_id, shard);
        for symbol_snapshot in &unhashed.symbols {
            let expected = engine
                .engine(symbol_snapshot.symbol)
                .expect("symbol engine")
                .snapshot_state()
                .state_hash;
            assert_eq!(symbol_snapshot.state_hash, expected);
        }
    }
}
