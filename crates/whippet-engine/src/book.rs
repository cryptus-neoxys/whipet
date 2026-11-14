// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic price-time-priority order book.
//!
//! Structure:
//! - contiguous array price ladder indexed by tick,
//! - occupancy bitmap per side for O(1)-ish best-price search,
//! - cached best bid/ask indices so the common lookup is a single field read,
//! - intrusive doubly linked FIFO per level over a preallocated order arena,
//! - open-addressed `FxHashMap` index from order id to arena slot.
//!
//! The book never reads the wall clock, never iterates a hash map in a
//! decision path, and never allocates once constructed (the arena, free list,
//! and order-id index are all preallocated to the configured capacity).

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use whippet_core::config::InstrumentConfig;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
use whippet_core::protocol::{MdLevel, Side};

pub const NONE: u32 = u32::MAX;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum BookError {
    #[error("price {0} is outside the ladder")]
    PriceOutOfRange(Price),
    #[error("order book is full")]
    BookFull,
    #[error("duplicate order id {0}")]
    DuplicateOrderId(OrderId),
    #[error("order quantity overflow")]
    Overflow,
    #[error("order quantity {0} exceeds the engine limit")]
    InvalidQty(u64),
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Level {
    pub head: u32,
    pub tail: u32,
    pub qty: u64,
    pub orders: u32,
}

impl Level {
    fn empty() -> Self {
        Self {
            head: NONE,
            tail: NONE,
            qty: 0,
            orders: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Slot {
    pub order_id: OrderId,
    pub participant: ParticipantId,
    pub price_idx: u32,
    pub qty: Qty,
    pub remaining: Qty,
    pub side: Side,
    pub prev: u32,
    pub next: u32,
    pub seq: u64,
    pub active: bool,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            order_id: OrderId(0),
            participant: ParticipantId(0),
            price_idx: 0,
            qty: Qty::ZERO,
            remaining: Qty::ZERO,
            side: Side::Buy,
            prev: NONE,
            next: NONE,
            seq: 0,
            active: false,
        }
    }
}

/// A resting order in a snapshot. Reinserting these in order reconstructs the
/// exact FIFO priority of every level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestingOrder {
    pub order_id: OrderId,
    pub participant: ParticipantId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub remaining: Qty,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookSnapshot {
    pub symbol: Symbol,
    pub last_trade_price: Price,
    pub last_trade_qty: Qty,
    pub last_trade_side: Option<Side>,
    pub orders: Vec<RestingOrder>,
}

pub struct Book {
    symbol: Symbol,
    tick_size_cents: i64,
    min_ticks: i64,
    max_ticks: i64,
    bids: Vec<Level>,
    asks: Vec<Level>,
    bid_bitmap: Vec<u64>,
    ask_bitmap: Vec<u64>,
    slots: Vec<Slot>,
    free: Vec<u32>,
    index: FxHashMap<OrderId, u32>,
    /// Cached best occupied ladder indices. `None` means that side is empty.
    /// Kept in sync by `insert` and `unlink`; the bitmap is the fallback when
    /// the cached level empties.
    best_bid_idx: Option<usize>,
    best_ask_idx: Option<usize>,
    order_count: usize,
    last_trade_price: Price,
    last_trade_qty: Qty,
    last_trade_side: Option<Side>,
}

impl Book {
    pub fn new(config: &InstrumentConfig) -> Self {
        let ladder_len = config.ladder_len();
        let words = ladder_len.div_ceil(64);
        let slots = vec![Slot::default(); config.max_orders];
        let mut free = Vec::with_capacity(config.max_orders);
        for index in (0..config.max_orders as u32).rev() {
            free.push(index);
        }
        Self {
            symbol: config.symbol,
            tick_size_cents: config.tick_size_cents,
            min_ticks: config.min_price_ticks,
            max_ticks: config.max_price_ticks,
            bids: vec![Level::empty(); ladder_len],
            asks: vec![Level::empty(); ladder_len],
            bid_bitmap: vec![0; words],
            ask_bitmap: vec![0; words],
            slots,
            free,
            index: FxHashMap::with_capacity_and_hasher(config.max_orders, Default::default()),
            best_bid_idx: None,
            best_ask_idx: None,
            order_count: 0,
            last_trade_price: Price::ZERO,
            last_trade_qty: Qty::ZERO,
            last_trade_side: None,
        }
    }

    pub fn symbol(&self) -> Symbol {
        self.symbol
    }

    pub fn tick_size_cents(&self) -> i64 {
        self.tick_size_cents
    }

    pub fn order_count(&self) -> usize {
        self.order_count
    }

    pub fn is_empty(&self) -> bool {
        self.order_count == 0
    }

    fn price_index(&self, price: Price) -> Result<usize, BookError> {
        if price.0 < self.min_ticks || price.0 > self.max_ticks {
            return Err(BookError::PriceOutOfRange(price));
        }
        Ok((price.0 - self.min_ticks) as usize)
    }

    fn levels(&self, side: Side) -> &Vec<Level> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut Vec<Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn bitmap(&self, side: Side) -> &Vec<u64> {
        match side {
            Side::Buy => &self.bid_bitmap,
            Side::Sell => &self.ask_bitmap,
        }
    }

    fn bitmap_mut(&mut self, side: Side) -> &mut Vec<u64> {
        match side {
            Side::Buy => &mut self.bid_bitmap,
            Side::Sell => &mut self.ask_bitmap,
        }
    }

    pub fn insert(
        &mut self,
        order_id: OrderId,
        participant: ParticipantId,
        side: Side,
        price: Price,
        qty: Qty,
        seq: u64,
    ) -> Result<u32, BookError> {
        if qty.0 == 0 || qty.0 > crate::MAX_ORDER_QTY {
            return Err(BookError::InvalidQty(qty.0));
        }
        let price_idx = self.price_index(price)?;
        let was_empty = self.levels(side)[price_idx].orders == 0;
        if self.levels(side)[price_idx]
            .qty
            .checked_add(qty.0)
            .is_none()
        {
            return Err(BookError::Overflow);
        }
        let Some(slot_index) = self.free.pop() else {
            return Err(BookError::BookFull);
        };

        // One hash probe for the order-id index: `insert` returns the previous
        // slot when the id is already resting, which is exactly the duplicate
        // case. The old code probed twice (`contains_key` then `insert`). The
        // duplicate path restores the displaced mapping and the reserved slot,
        // so a rejected insert leaves the book untouched.
        if let Some(previous) = self.index.insert(order_id, slot_index) {
            self.index.insert(order_id, previous);
            self.free.push(slot_index);
            return Err(BookError::DuplicateOrderId(order_id));
        }

        let prev = match side {
            Side::Buy => self.bids[price_idx].tail,
            Side::Sell => self.asks[price_idx].tail,
        };
        {
            let slot = &mut self.slots[slot_index as usize];
            *slot = Slot {
                order_id,
                participant,
                price_idx: price_idx as u32,
                qty,
                remaining: qty,
                side,
                prev,
                next: NONE,
                seq,
                active: true,
            };
        }

        if prev != NONE {
            self.slots[prev as usize].next = slot_index;
        }
        let level = match side {
            Side::Buy => &mut self.bids[price_idx],
            Side::Sell => &mut self.asks[price_idx],
        };
        if prev == NONE {
            level.head = slot_index;
        }
        level.tail = slot_index;
        level.qty = level.qty.saturating_add(qty.0);
        level.orders += 1;
        self.set_bit(side, price_idx);
        if was_empty && self.is_better_best(side, price_idx) {
            self.set_best(side, Some(price_idx));
        }
        self.order_count += 1;
        debug_assert_eq!(self.best_index(side), self.bitmap_best_index(side));
        Ok(slot_index)
    }

    pub fn order_index(&self, order_id: OrderId) -> Option<u32> {
        self.index.get(&order_id).copied()
    }

    pub(crate) fn slot(&self, index: u32) -> &Slot {
        &self.slots[index as usize]
    }

    pub(crate) fn head(&self, side: Side, price_idx: usize) -> Option<u32> {
        let head = self.levels(side)[price_idx].head;
        (head != NONE).then_some(head)
    }

    pub(crate) fn level_qty(&self, side: Side, price_idx: usize) -> Qty {
        Qty(self.levels(side)[price_idx].qty)
    }

    pub(crate) fn level_orders(&self, side: Side, price_idx: usize) -> u32 {
        self.levels(side)[price_idx].orders
    }

    pub(crate) fn price_at(&self, price_idx: usize) -> Price {
        Price(self.min_ticks + price_idx as i64)
    }

    /// Cancel an order. Returns the removed order, or `None` if unknown.
    pub fn cancel(
        &mut self,
        order_id: OrderId,
        participant: Option<ParticipantId>,
    ) -> Option<RestingOrder> {
        let index = self.order_index(order_id)?;
        if let Some(participant) = participant
            && self.slots[index as usize].participant != participant
        {
            return None;
        }
        let slot = self.slots[index as usize];
        let removed = RestingOrder {
            order_id: slot.order_id,
            participant: slot.participant,
            side: slot.side,
            price: self.price_at(slot.price_idx as usize),
            qty: slot.qty,
            remaining: slot.remaining,
            seq: slot.seq,
        };
        self.unlink(index);
        Some(removed)
    }

    /// Remove an order from the arena and unlink it from its level.
    pub(crate) fn unlink(&mut self, index: u32) {
        let slot = self.slots[index as usize];
        if !slot.active {
            return;
        }
        let price_idx = slot.price_idx as usize;
        let side = slot.side;
        let prev = slot.prev;
        let next = slot.next;
        let remaining = slot.remaining.0;

        let level = match side {
            Side::Buy => &mut self.bids[price_idx],
            Side::Sell => &mut self.asks[price_idx],
        };
        if prev != NONE {
            self.slots[prev as usize].next = next;
        } else {
            level.head = next;
        }
        if next != NONE {
            self.slots[next as usize].prev = prev;
        } else {
            level.tail = prev;
        }
        level.qty = level.qty.saturating_sub(remaining);
        level.orders = level.orders.saturating_sub(1);
        let empty = level.orders == 0;
        if empty {
            let word = price_idx / 64;
            let bit = price_idx % 64;
            let bitmap = match side {
                Side::Buy => &mut self.bid_bitmap,
                Side::Sell => &mut self.ask_bitmap,
            };
            bitmap[word] &= !(1u64 << bit);
            let was_best = match side {
                Side::Buy => self.best_bid_idx == Some(price_idx),
                Side::Sell => self.best_ask_idx == Some(price_idx),
            };
            if was_best {
                // The removed level was the cached best. The next occupied
                // level in matching order is the new best; `next_index` only
                // scans the current bitmap word and upward/downward as needed.
                let recomputed = self.next_index(side, price_idx);
                self.set_best(side, recomputed);
            }
        }

        self.index.remove(&slot.order_id);
        self.slots[index as usize].active = false;
        self.slots[index as usize].next = NONE;
        self.slots[index as usize].prev = NONE;
        self.free.push(index);
        self.order_count = self.order_count.saturating_sub(1);
        debug_assert_eq!(self.best_index(side), self.bitmap_best_index(side));
    }

    /// Reduce a resting order by `fill`. Returns `true` when it was fully
    /// filled and removed.
    pub(crate) fn reduce(&mut self, index: u32, fill: Qty) -> bool {
        let slot = &mut self.slots[index as usize];
        debug_assert!(fill.0 <= slot.remaining.0);
        let applied = fill.min(slot.remaining);
        slot.remaining = slot.remaining.saturating_sub(applied);
        let fully_filled = slot.remaining.is_zero();
        let price_idx = slot.price_idx as usize;
        let side = slot.side;
        {
            let level = &mut self.levels_mut(side)[price_idx];
            level.qty = level.qty.saturating_sub(applied.0);
        }
        if fully_filled {
            self.unlink(index);
        }
        fully_filled
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.best_index(Side::Buy).map(|idx| self.price_at(idx))
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.best_index(Side::Sell).map(|idx| self.price_at(idx))
    }

    pub(crate) fn best_index(&self, side: Side) -> Option<usize> {
        match side {
            Side::Buy => self.best_bid_idx,
            Side::Sell => self.best_ask_idx,
        }
    }

    fn set_best(&mut self, side: Side, index: Option<usize>) {
        match side {
            Side::Buy => self.best_bid_idx = index,
            Side::Sell => self.best_ask_idx = index,
        }
    }

    /// True when `price_idx` is a better price than the cached best, or the
    /// side is currently empty.
    fn is_better_best(&self, side: Side, price_idx: usize) -> bool {
        match side {
            Side::Buy => self.best_bid_idx.is_none_or(|best| price_idx > best),
            Side::Sell => self.best_ask_idx.is_none_or(|best| price_idx < best),
        }
    }

    /// Slow path: scan the occupancy bitmap from the top of the ladder.
    fn bitmap_best_index(&self, side: Side) -> Option<usize> {
        match side {
            Side::Buy => {
                for (word_index, word) in self.bid_bitmap.iter().enumerate().rev() {
                    if *word != 0 {
                        let bit = 63 - word.leading_zeros() as usize;
                        return Some(word_index * 64 + bit);
                    }
                }
                None
            }
            Side::Sell => {
                for (word_index, word) in self.ask_bitmap.iter().enumerate() {
                    if *word != 0 {
                        let bit = word.trailing_zeros() as usize;
                        return Some(word_index * 64 + bit);
                    }
                }
                None
            }
        }
    }

    fn set_bit(&mut self, side: Side, price_idx: usize) {
        let word = price_idx / 64;
        let bit = price_idx % 64;
        self.bitmap_mut(side)[word] |= 1u64 << bit;
    }

    /// Quantity available on `side` at prices that cross `limit` (inclusive).
    /// For a market order pass the ladder extreme.
    pub fn executable_qty(&self, side: Side, limit: Price) -> u64 {
        let mut total = 0u64;
        let mut cursor = self.best_index(side);
        while let Some(price_idx) = cursor {
            let price = self.price_at(price_idx);
            let crosses = match side {
                // We are buying: asks at or below the limit.
                Side::Sell => price.0 <= limit.0,
                // We are selling: bids at or above the limit.
                Side::Buy => price.0 >= limit.0,
            };
            if !crosses {
                break;
            }
            total = total.saturating_add(self.level_qty(side, price_idx).0);
            cursor = self.next_index(side, price_idx);
        }
        total
    }

    /// Next occupied level in matching order: asks ascend, bids descend.
    /// Quantity available on `side` that crosses `limit`, excluding orders
    /// from `excluded`. FOK uses this so self-trade prevention cannot turn a
    /// fill-or-kill order into a cancel with side effects.
    pub fn executable_qty_excluding(
        &self,
        side: Side,
        limit: Price,
        excluded: ParticipantId,
    ) -> u64 {
        let mut total = 0u64;
        let mut cursor = self.best_index(side);
        while let Some(price_idx) = cursor {
            let price = self.price_at(price_idx);
            let crosses = match side {
                Side::Sell => price.0 <= limit.0,
                Side::Buy => price.0 >= limit.0,
            };
            if !crosses {
                break;
            }
            let mut index = self.levels(side)[price_idx].head;
            while index != NONE {
                let slot = &self.slots[index as usize];
                if slot.participant != excluded {
                    total = total.saturating_add(slot.remaining.0);
                }
                index = slot.next;
            }
            cursor = self.next_index(side, price_idx);
        }
        total
    }

    /// Early-exit form of [`Book::executable_qty_excluding`]: true when at
    /// least `needed` crosses `limit` on `side` from participants other than
    /// `excluded`.
    ///
    /// Equivalent to `executable_qty_excluding(side, limit, excluded) >=
    /// needed.0`, but stops walking as soon as the threshold is met. A fillable
    /// FOK only has to prove the first `needed` lots exist, not sum the whole
    /// crossing side of the book.
    pub fn has_executable_qty_excluding(
        &self,
        side: Side,
        limit: Price,
        excluded: ParticipantId,
        needed: Qty,
    ) -> bool {
        if needed.is_zero() {
            return true;
        }
        let mut total = 0u64;
        let mut cursor = self.best_index(side);
        while let Some(price_idx) = cursor {
            let price = self.price_at(price_idx);
            let crosses = match side {
                Side::Sell => price.0 <= limit.0,
                Side::Buy => price.0 >= limit.0,
            };
            if !crosses {
                break;
            }
            let mut index = self.levels(side)[price_idx].head;
            while index != NONE {
                let slot = &self.slots[index as usize];
                if slot.participant != excluded {
                    total = total.saturating_add(slot.remaining.0);
                    if total >= needed.0 {
                        return true;
                    }
                }
                index = slot.next;
            }
            cursor = self.next_index(side, price_idx);
        }
        false
    }

    /// Next occupied level in matching order: asks ascend, bids descend.
    fn next_index(&self, side: Side, from: usize) -> Option<usize> {
        let bitmap = self.bitmap(side);
        match side {
            Side::Sell => {
                let mut word = from / 64;
                let bit = from % 64;
                let mask = if bit == 63 {
                    0
                } else {
                    !((1u64 << (bit + 1)) - 1)
                };
                let mut bits = bitmap[word] & mask;
                loop {
                    if bits != 0 {
                        return Some(word * 64 + bits.trailing_zeros() as usize);
                    }
                    word += 1;
                    if word >= bitmap.len() {
                        return None;
                    }
                    bits = bitmap[word];
                }
            }
            Side::Buy => {
                let mut word = from / 64;
                let bit = from % 64;
                let mask = if bit == 0 { 0 } else { (1u64 << bit) - 1 };
                let mut bits = bitmap[word] & mask;
                loop {
                    if bits != 0 {
                        return Some(word * 64 + 63 - bits.leading_zeros() as usize);
                    }
                    if word == 0 {
                        return None;
                    }
                    word -= 1;
                    bits = bitmap[word];
                }
            }
        }
    }

    pub fn depth_qty(&self, side: Side) -> u64 {
        self.levels(side)
            .iter()
            .fold(0u64, |total, level| total.saturating_add(level.qty))
    }

    /// Top `depth` levels per side, best first.
    pub fn snapshot_levels(&self, depth: usize) -> (Vec<MdLevel>, Vec<MdLevel>) {
        let mut bids = Vec::with_capacity(depth);
        let mut asks = Vec::with_capacity(depth);

        let mut cursor = self.best_index(Side::Buy);
        while let Some(price_idx) = cursor {
            if bids.len() == depth {
                break;
            }
            bids.push(MdLevel {
                price: self.price_at(price_idx),
                qty: self.level_qty(Side::Buy, price_idx),
                orders: self.level_orders(Side::Buy, price_idx),
            });
            cursor = self.next_index(Side::Buy, price_idx);
        }

        let mut cursor = self.best_index(Side::Sell);
        while let Some(price_idx) = cursor {
            if asks.len() == depth {
                break;
            }
            asks.push(MdLevel {
                price: self.price_at(price_idx),
                qty: self.level_qty(Side::Sell, price_idx),
                orders: self.level_orders(Side::Sell, price_idx),
            });
            cursor = self.next_index(Side::Sell, price_idx);
        }
        (bids, asks)
    }

    pub fn last_trade(&self) -> (Price, Qty, Option<Side>) {
        (
            self.last_trade_price,
            self.last_trade_qty,
            self.last_trade_side,
        )
    }

    pub(crate) fn record_trade(&mut self, price: Price, qty: Qty, side: Side) {
        self.last_trade_price = price;
        self.last_trade_qty = qty;
        self.last_trade_side = Some(side);
    }

    pub fn snapshot(&self) -> BookSnapshot {
        let mut orders = Vec::with_capacity(self.order_count);
        for side in [Side::Buy, Side::Sell] {
            let mut cursor = self.best_index(side);
            while let Some(price_idx) = cursor {
                let mut index = self.levels(side)[price_idx].head;
                while index != NONE {
                    let slot = &self.slots[index as usize];
                    if slot.active {
                        orders.push(RestingOrder {
                            order_id: slot.order_id,
                            participant: slot.participant,
                            side: slot.side,
                            price: self.price_at(slot.price_idx as usize),
                            qty: slot.qty,
                            remaining: slot.remaining,
                            seq: slot.seq,
                        });
                    }
                    index = slot.next;
                }
                cursor = self.next_index(side, price_idx);
            }
        }
        BookSnapshot {
            symbol: self.symbol,
            last_trade_price: self.last_trade_price,
            last_trade_qty: self.last_trade_qty,
            last_trade_side: self.last_trade_side,
            orders,
        }
    }

    pub fn from_snapshot(
        config: &InstrumentConfig,
        snapshot: BookSnapshot,
    ) -> Result<Self, BookError> {
        let mut book = Self::new(config);
        book.last_trade_price = snapshot.last_trade_price;
        book.last_trade_qty = snapshot.last_trade_qty;
        book.last_trade_side = snapshot.last_trade_side;
        for order in snapshot.orders {
            book.insert(
                order.order_id,
                order.participant,
                order.side,
                order.price,
                order.remaining,
                order.seq,
            )?;
        }
        Ok(book)
    }

    /// Feed the deterministic state hash. Iterates levels and FIFO order, never
    /// a hash map.
    pub fn hash_into(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"book");
        hasher.update(self.symbol.as_bytes());
        hasher.update(&self.last_trade_price.0.to_le_bytes());
        hasher.update(&self.last_trade_qty.0.to_le_bytes());
        hasher.update(&[match self.last_trade_side {
            None => 0,
            Some(Side::Buy) => 1,
            Some(Side::Sell) => 2,
        }]);
        hasher.update(&(self.order_count as u64).to_le_bytes());
        for side in [Side::Buy, Side::Sell] {
            let mut cursor = self.best_index(side);
            while let Some(price_idx) = cursor {
                let level = &self.levels(side)[price_idx];
                hasher.update(b"L");
                hasher.update(&(price_idx as u64).to_le_bytes());
                hasher.update(&level.qty.to_le_bytes());
                hasher.update(&level.orders.to_le_bytes());
                let mut index = level.head;
                while index != NONE {
                    let slot = &self.slots[index as usize];
                    hasher.update(b"O");
                    hasher.update(&slot.order_id.0.to_le_bytes());
                    hasher.update(&slot.participant.0.to_le_bytes());
                    hasher.update(&slot.remaining.0.to_le_bytes());
                    hasher.update(&slot.seq.to_le_bytes());
                    index = slot.next;
                }
                cursor = self.next_index(side, price_idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use whippet_core::config::InstrumentConfig;

    fn config() -> InstrumentConfig {
        let mut config = InstrumentConfig::new("BTC-USD", 100, 100, 200).unwrap();
        config.max_orders = 1024;
        config
    }

    #[test]
    fn fifo_priority_is_preserved() {
        let mut book = Book::new(&config());
        book.insert(
            OrderId(1),
            ParticipantId(1),
            Side::Buy,
            Price(100),
            Qty(10),
            1,
        )
        .unwrap();
        book.insert(
            OrderId(2),
            ParticipantId(2),
            Side::Buy,
            Price(100),
            Qty(20),
            2,
        )
        .unwrap();
        book.insert(
            OrderId(3),
            ParticipantId(3),
            Side::Buy,
            Price(100),
            Qty(30),
            3,
        )
        .unwrap();

        let head = book.head(Side::Buy, 0).unwrap();
        assert_eq!(book.slot(head).order_id, OrderId(1));
        assert_eq!(book.slot(book.slot(head).next).order_id, OrderId(2));
        assert_eq!(book.level_qty(Side::Buy, 0), Qty(60));
    }

    #[test]
    fn best_price_uses_bitmap() {
        let mut book = Book::new(&config());
        book.insert(
            OrderId(1),
            ParticipantId(1),
            Side::Buy,
            Price(101),
            Qty(1),
            1,
        )
        .unwrap();
        book.insert(
            OrderId(2),
            ParticipantId(1),
            Side::Buy,
            Price(105),
            Qty(1),
            2,
        )
        .unwrap();
        book.insert(
            OrderId(3),
            ParticipantId(1),
            Side::Sell,
            Price(110),
            Qty(1),
            3,
        )
        .unwrap();
        book.insert(
            OrderId(4),
            ParticipantId(1),
            Side::Sell,
            Price(108),
            Qty(1),
            4,
        )
        .unwrap();
        assert_eq!(book.best_bid(), Some(Price(105)));
        assert_eq!(book.best_ask(), Some(Price(108)));
    }

    #[test]
    fn cancel_unlinks_and_clears_bit() {
        let mut book = Book::new(&config());
        book.insert(
            OrderId(1),
            ParticipantId(1),
            Side::Buy,
            Price(100),
            Qty(10),
            1,
        )
        .unwrap();
        book.insert(
            OrderId(2),
            ParticipantId(1),
            Side::Buy,
            Price(100),
            Qty(20),
            2,
        )
        .unwrap();
        let removed = book.cancel(OrderId(1), None).unwrap();
        assert_eq!(removed.remaining, Qty(10));
        assert_eq!(book.best_bid(), Some(Price(100)));
        assert_eq!(book.level_qty(Side::Buy, 0), Qty(20));
        book.cancel(OrderId(2), None).unwrap();
        assert_eq!(book.best_bid(), None);
        assert!(book.is_empty());
    }

    #[test]
    fn reduce_removes_when_full() {
        let mut book = Book::new(&config());
        let index = book
            .insert(
                OrderId(1),
                ParticipantId(1),
                Side::Sell,
                Price(100),
                Qty(10),
                1,
            )
            .unwrap();
        assert!(!book.reduce(index, Qty(4)));
        assert_eq!(book.slot(index).remaining, Qty(6));
        assert_eq!(book.level_qty(Side::Sell, 0), Qty(6));
        assert!(book.reduce(index, Qty(6)));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn snapshot_round_trip_preserves_priority() {
        let mut book = Book::new(&config());
        book.insert(
            OrderId(1),
            ParticipantId(1),
            Side::Buy,
            Price(100),
            Qty(10),
            1,
        )
        .unwrap();
        book.insert(
            OrderId(2),
            ParticipantId(2),
            Side::Buy,
            Price(100),
            Qty(20),
            2,
        )
        .unwrap();
        book.insert(
            OrderId(3),
            ParticipantId(3),
            Side::Sell,
            Price(110),
            Qty(30),
            3,
        )
        .unwrap();
        let snapshot = book.snapshot();
        let restored = Book::from_snapshot(&config(), snapshot).unwrap();
        assert_eq!(restored.best_bid(), Some(Price(100)));
        assert_eq!(restored.best_ask(), Some(Price(110)));
        assert_eq!(restored.level_qty(Side::Buy, 0), Qty(30));
        let head = restored.head(Side::Buy, 0).unwrap();
        assert_eq!(restored.slot(head).order_id, OrderId(1));
        assert_eq!(restored.slot(restored.slot(head).next).order_id, OrderId(2));
    }

    #[test]
    fn executable_qty_walks_levels() {
        let mut book = Book::new(&config());
        book.insert(
            OrderId(1),
            ParticipantId(1),
            Side::Sell,
            Price(101),
            Qty(10),
            1,
        )
        .unwrap();
        book.insert(
            OrderId(2),
            ParticipantId(1),
            Side::Sell,
            Price(102),
            Qty(20),
            2,
        )
        .unwrap();
        book.insert(
            OrderId(3),
            ParticipantId(1),
            Side::Sell,
            Price(103),
            Qty(30),
            3,
        )
        .unwrap();
        assert_eq!(book.executable_qty(Side::Sell, Price(102)), 30);
        assert_eq!(book.executable_qty(Side::Sell, Price(103)), 60);
        assert_eq!(book.executable_qty(Side::Sell, Price(100)), 0);
    }

    fn add(book: &mut Book, id: u128, side: Side, price: i64, qty: u64) {
        book.insert(
            OrderId(id),
            ParticipantId(1),
            side,
            Price(price),
            Qty(qty),
            id as u64,
        )
        .expect("insert");
    }

    #[test]
    fn best_cache_tracks_better_and_worse_inserts() {
        let mut book = Book::new(&config());
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);

        add(&mut book, 1, Side::Buy, 100, 1);
        assert_eq!(book.best_bid(), Some(Price(100)));
        add(&mut book, 2, Side::Buy, 105, 1);
        assert_eq!(book.best_bid(), Some(Price(105)), "better bid must win");
        add(&mut book, 3, Side::Buy, 102, 1);
        assert_eq!(
            book.best_bid(),
            Some(Price(105)),
            "worse bid must not move the cache"
        );
        add(&mut book, 4, Side::Buy, 105, 1);
        assert_eq!(
            book.best_bid(),
            Some(Price(105)),
            "adding to the cached level keeps it best"
        );

        add(&mut book, 5, Side::Sell, 110, 1);
        assert_eq!(book.best_ask(), Some(Price(110)));
        add(&mut book, 6, Side::Sell, 108, 1);
        assert_eq!(book.best_ask(), Some(Price(108)), "better ask must win");
        add(&mut book, 7, Side::Sell, 109, 1);
        assert_eq!(
            book.best_ask(),
            Some(Price(108)),
            "worse ask must not move the cache"
        );
        add(&mut book, 8, Side::Sell, 108, 1);
        assert_eq!(
            book.best_ask(),
            Some(Price(108)),
            "adding to the cached level keeps it best"
        );
    }

    #[test]
    fn cancelling_the_best_level_recomputes_the_cache() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        add(&mut book, 2, Side::Buy, 103, 5);
        add(&mut book, 3, Side::Buy, 101, 5);
        add(&mut book, 4, Side::Buy, 103, 5);
        add(&mut book, 5, Side::Sell, 110, 5);
        add(&mut book, 6, Side::Sell, 108, 5);
        add(&mut book, 7, Side::Sell, 109, 5);
        add(&mut book, 8, Side::Sell, 108, 5);

        // Two orders at the best bid; removing one keeps the level.
        book.cancel(OrderId(2), None).expect("cancel");
        assert_eq!(book.best_bid(), Some(Price(103)));
        book.cancel(OrderId(4), None).expect("cancel");
        assert_eq!(
            book.best_bid(),
            Some(Price(101)),
            "emptying the best level falls back to the bitmap"
        );
        book.cancel(OrderId(3), None).expect("cancel");
        assert_eq!(book.best_bid(), Some(Price(100)));

        // A cancel below the best must not disturb the cache.
        book.cancel(OrderId(1), None).expect("cancel");
        assert_eq!(book.best_bid(), None);

        book.cancel(OrderId(6), None).expect("cancel");
        assert_eq!(book.best_ask(), Some(Price(108)));
        book.cancel(OrderId(8), None).expect("cancel");
        assert_eq!(book.best_ask(), Some(Price(109)));
        book.cancel(OrderId(7), None).expect("cancel");
        assert_eq!(book.best_ask(), Some(Price(110)));
        book.cancel(OrderId(5), None).expect("cancel");
        assert_eq!(book.best_ask(), None);
        assert!(book.is_empty());
    }

    #[test]
    fn filling_the_best_level_recomputes_the_cache() {
        let mut book = Book::new(&config());
        let first = book
            .insert(
                OrderId(1),
                ParticipantId(1),
                Side::Sell,
                Price(105),
                Qty(10),
                1,
            )
            .unwrap();
        let second = book
            .insert(
                OrderId(2),
                ParticipantId(1),
                Side::Sell,
                Price(106),
                Qty(10),
                2,
            )
            .unwrap();
        assert_eq!(book.best_ask(), Some(Price(105)));

        assert!(!book.reduce(first, Qty(4)));
        assert_eq!(book.best_ask(), Some(Price(105)));
        assert!(book.reduce(first, Qty(6)));
        assert_eq!(
            book.best_ask(),
            Some(Price(106)),
            "filling the best order moves the cache down a level"
        );

        assert!(book.reduce(second, Qty(10)));
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn best_cache_handles_bitmap_word_boundaries() {
        // The ladder starts at tick 100, so prices 100/163/164/165 map to
        // indices 0/63/64/65: the first bit of the first word and the last,
        // first, and second bits across a word boundary.
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 1); // index 0
        add(&mut book, 2, Side::Buy, 163, 1); // index 63
        add(&mut book, 3, Side::Buy, 164, 1); // index 64
        add(&mut book, 4, Side::Buy, 165, 1); // index 65
        assert_eq!(book.best_bid(), Some(Price(165)));
        book.cancel(OrderId(4), None).unwrap();
        assert_eq!(book.best_bid(), Some(Price(164)));
        book.cancel(OrderId(3), None).unwrap();
        assert_eq!(book.best_bid(), Some(Price(163)));
        book.cancel(OrderId(2), None).unwrap();
        assert_eq!(book.best_bid(), Some(Price(100)));
        book.cancel(OrderId(1), None).unwrap();
        assert_eq!(book.best_bid(), None);

        add(&mut book, 5, Side::Sell, 165, 1); // index 65
        add(&mut book, 6, Side::Sell, 164, 1); // index 64
        add(&mut book, 7, Side::Sell, 163, 1); // index 63
        add(&mut book, 8, Side::Sell, 100, 1); // index 0
        assert_eq!(book.best_ask(), Some(Price(100)));
        book.cancel(OrderId(8), None).unwrap();
        assert_eq!(book.best_ask(), Some(Price(163)));
        book.cancel(OrderId(7), None).unwrap();
        assert_eq!(book.best_ask(), Some(Price(164)));
        book.cancel(OrderId(6), None).unwrap();
        assert_eq!(book.best_ask(), Some(Price(165)));
        book.cancel(OrderId(5), None).unwrap();
        assert_eq!(book.best_ask(), None);
    }

    #[test]
    fn snapshot_restore_rebuilds_the_best_cache() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        add(&mut book, 2, Side::Buy, 104, 5);
        add(&mut book, 3, Side::Sell, 108, 5);
        add(&mut book, 4, Side::Sell, 110, 5);

        let mut restored = Book::from_snapshot(&config(), book.snapshot()).expect("restore");
        assert_eq!(restored.best_bid(), Some(Price(104)));
        assert_eq!(restored.best_ask(), Some(Price(108)));
        assert_eq!(restored.order_count(), book.order_count());

        // The restored cache must keep tracking removals.
        restored.cancel(OrderId(2), None).expect("cancel best bid");
        assert_eq!(restored.best_bid(), Some(Price(100)));
        restored.cancel(OrderId(3), None).expect("cancel best ask");
        assert_eq!(restored.best_ask(), Some(Price(110)));
    }

    #[test]
    fn derived_views_use_the_cached_best() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        add(&mut book, 2, Side::Buy, 104, 7);
        add(&mut book, 3, Side::Sell, 108, 11);
        add(&mut book, 4, Side::Sell, 110, 13);

        let (bids, asks) = book.snapshot_levels(2);
        assert_eq!(bids[0].price, Price(104));
        assert_eq!(asks[0].price, Price(108));
        assert_eq!(book.executable_qty(Side::Sell, Price(108)), 11);
        assert_eq!(book.executable_qty(Side::Buy, Price(104)), 7);

        book.cancel(OrderId(2), None).unwrap();
        book.cancel(OrderId(3), None).unwrap();
        let (bids, asks) = book.snapshot_levels(2);
        assert_eq!(bids[0].price, Price(100));
        assert_eq!(asks[0].price, Price(110));
        assert_eq!(book.executable_qty(Side::Sell, Price(108)), 0);
        assert_eq!(book.executable_qty(Side::Buy, Price(100)), 5);
    }

    #[test]
    fn duplicate_insert_is_rejected_without_touching_the_book() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        add(&mut book, 2, Side::Sell, 110, 5);
        let before = book.snapshot();
        let original_slot = book.order_index(OrderId(1)).expect("original slot");
        let hash_before = {
            let mut hasher = blake3::Hasher::new();
            book.hash_into(&mut hasher);
            *hasher.finalize().as_bytes()
        };

        let error = book
            .insert(
                OrderId(1),
                ParticipantId(7),
                Side::Sell,
                Price(120),
                Qty(9),
                99,
            )
            .expect_err("duplicate id");
        assert_eq!(error, BookError::DuplicateOrderId(OrderId(1)));

        // The rejected insert must leave the original order, its level, the
        // free list, and the state hash exactly as they were.
        assert_eq!(book.order_index(OrderId(1)), Some(original_slot));
        assert_eq!(book.slot(original_slot).participant, ParticipantId(1));
        assert_eq!(book.level_qty(Side::Buy, 0), Qty(5));
        assert_eq!(book.level_orders(Side::Buy, 0), 1);
        assert_eq!(book.level_qty(Side::Sell, 10), Qty(5));
        assert_eq!(book.order_count(), 2);
        assert_eq!(book.snapshot().orders.len(), before.orders.len());
        let hash_after = {
            let mut hasher = blake3::Hasher::new();
            book.hash_into(&mut hasher);
            *hasher.finalize().as_bytes()
        };
        assert_eq!(hash_before, hash_after);

        // The arena slot reserved by the failed insert is reusable.
        add(&mut book, 3, Side::Buy, 101, 1);
        assert_eq!(book.order_count(), 3);
    }

    #[test]
    fn early_exit_answers_a_deep_book_from_the_first_levels() {
        // 50k resting asks across 500 levels, ten lots each. A FOK that the
        // best few levels can cover must be answered without walking the rest
        // of the ladder.
        let mut config = InstrumentConfig::new("BTC-USD", 100, 100, 600).unwrap();
        config.max_orders = 50_000;
        let mut book = Book::new(&config);
        let mut id = 0u128;
        for level in 0..500i64 {
            for _ in 0..100 {
                id += 1;
                book.insert(
                    OrderId(id),
                    ParticipantId(1),
                    Side::Sell,
                    Price(100 + level),
                    Qty(10),
                    id as u64,
                )
                .expect("insert");
            }
        }
        assert_eq!(book.order_count(), 50_000);

        // One lot and ten thousand lots are both covered by the first levels.
        assert!(book.has_executable_qty_excluding(
            Side::Sell,
            Price(600),
            ParticipantId(2),
            Qty(1)
        ));
        assert!(book.has_executable_qty_excluding(
            Side::Sell,
            Price(600),
            ParticipantId(2),
            Qty(10_000)
        ));
        // Ten thousand lots also sit on the first ten levels, so the early
        // exit is reached long before the 50k-th order.
        assert!(book.has_executable_qty_excluding(
            Side::Sell,
            Price(109),
            ParticipantId(2),
            Qty(10_000)
        ));
        // Asking for more than the whole book has to walk it all and fail.
        assert!(!book.has_executable_qty_excluding(
            Side::Sell,
            Price(600),
            ParticipantId(2),
            Qty(500_001)
        ));
        // A limit below the best ask crosses nothing.
        assert!(!book.has_executable_qty_excluding(
            Side::Sell,
            Price(99),
            ParticipantId(2),
            Qty(1)
        ));
        // Self-trade liquidity never counts toward the threshold.
        assert!(!book.has_executable_qty_excluding(
            Side::Sell,
            Price(600),
            ParticipantId(1),
            Qty(1)
        ));
        // The early exit must agree with the full walk on the same book.
        assert_eq!(
            book.executable_qty_excluding(Side::Sell, Price(600), ParticipantId(2)),
            500_000
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// The early-exit threshold and the full walk must agree for every
        /// query on a randomly shaped book.
        #[test]
        fn early_exit_matches_the_full_walk(
            orders in proptest::collection::vec(
                (any::<bool>(), 100i64..=140, 1u64..=50, 1u64..=3),
                0..60,
            ),
            probes in proptest::collection::vec((100i64..=140, 0u64..=2_000, 1u64..=3), 1..12),
        ) {
            let mut config = InstrumentConfig::new("BTC-USD", 100, 100, 140).unwrap();
            config.max_orders = 128;
            let mut book = Book::new(&config);
            for (index, &(buy, price, qty, participant)) in orders.iter().enumerate() {
                book.insert(
                    OrderId(index as u128 + 1),
                    ParticipantId(participant),
                    if buy { Side::Buy } else { Side::Sell },
                    Price(price),
                    Qty(qty),
                    index as u64,
                )
                .expect("insert");
            }
            for &(limit, needed, participant) in &probes {
                for side in [Side::Buy, Side::Sell] {
                    let full = book.executable_qty_excluding(
                        side,
                        Price(limit),
                        ParticipantId(participant),
                    );
                    prop_assert_eq!(
                        book.has_executable_qty_excluding(
                            side,
                            Price(limit),
                            ParticipantId(participant),
                            Qty(needed),
                        ),
                        full >= needed,
                        "side {:?}, limit {}, needed {}, excluded {}, full walk {}",
                        side,
                        limit,
                        needed,
                        participant,
                        full
                    );
                }
            }
        }
    }
}
