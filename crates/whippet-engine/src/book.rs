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
    #[error("order {0} is not resting for this participant")]
    UnknownOrder(OrderId),
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
    /// Lowest ladder index in ticks. [`Book::hash_into`] writes each level's
    /// ladder index (`price - min_price_ticks`) rather than its absolute price,
    /// so the snapshot stores the offset needed to reproduce that byte-for-byte.
    ///
    /// Schema note: this field (and `SymbolSnapshot::config`) extend the
    /// serialized snapshot. The on-disk format is versioned by the `whippet`
    /// crate's snapshot store (`WHIPSNP1`/version 1); an upgrade that changes
    /// the layout must bump that version there.
    pub min_price_ticks: i64,
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
        //
        // The cheap quantity/price/overflow checks now run first, so an order
        // that is both a duplicate and invalid reports the validation error
        // instead of `DuplicateOrderId`. Matching is unaffected: the engine
        // rejects duplicate ids from its dedup table before an order reaches
        // the book, and the duplicate error is still produced here.
        if let Some(previous) = self.index.insert(order_id, slot_index) {
            self.index.insert(order_id, previous);
            self.free.push(slot_index);
            return Err(BookError::DuplicateOrderId(order_id));
        }

        {
            let slot = &mut self.slots[slot_index as usize];
            *slot = Slot {
                order_id,
                participant,
                price_idx: price_idx as u32,
                qty,
                remaining: qty,
                side,
                prev: NONE,
                next: NONE,
                seq,
                active: true,
            };
        }
        self.attach(slot_index);
        debug_assert_eq!(self.best_index(side), self.bitmap_best_index(side));
        self.order_count += 1;
        Ok(slot_index)
    }

    /// Link an initialized active slot at the tail of its price level and
    /// update the level aggregate, occupancy bit, and cached best.
    ///
    /// The caller has already validated that the level aggregate cannot
    /// overflow and that the slot's `price_idx` and `side` are in range.
    fn attach(&mut self, index: u32) {
        let slot = self.slots[index as usize];
        debug_assert!(slot.active);
        let price_idx = slot.price_idx as usize;
        let side = slot.side;
        let was_empty = self.levels(side)[price_idx].orders == 0;
        let prev = self.levels(side)[price_idx].tail;
        self.slots[index as usize].prev = prev;
        self.slots[index as usize].next = NONE;
        if prev != NONE {
            self.slots[prev as usize].next = index;
        }
        let level = &mut self.levels_mut(side)[price_idx];
        if prev == NONE {
            level.head = index;
        }
        level.tail = index;
        level.qty = level.qty.saturating_add(slot.remaining.0);
        level.orders += 1;
        self.set_bit(side, price_idx);
        if was_empty && self.is_better_best(side, price_idx) {
            self.set_best(side, Some(price_idx));
        }
    }

    /// Unlink an active slot from its level and update the level aggregate,
    /// occupancy bit, and cached best. The arena slot, its index entry, and
    /// the order count are left for the caller: [`Book::unlink`] retires the
    /// slot, while [`Book::amend`] reuses it for the replacement.
    fn detach(&mut self, index: u32) {
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
        debug_assert_eq!(self.best_index(side), self.bitmap_best_index(side));
    }

    /// Atomically replace a resting order's price and remaining quantity.
    ///
    /// The order keeps its id and its arena slot, but it is unlinked from its
    /// old level and linked at the tail of the new one: an amendment always
    /// loses time priority. `new_qty` must be positive and no larger than the
    /// order's current remaining quantity. The engine owns the client-facing
    /// rejection reasons; this method is the book's last-line guard.
    ///
    /// Returns the order as it was before the amendment.
    pub fn amend(
        &mut self,
        order_id: OrderId,
        participant: ParticipantId,
        new_price: Price,
        new_qty: Qty,
        seq: u64,
    ) -> Result<RestingOrder, BookError> {
        let index = self
            .order_index(order_id)
            .ok_or(BookError::UnknownOrder(order_id))?;
        let slot = self.slots[index as usize];
        if !slot.active || slot.participant != participant {
            return Err(BookError::UnknownOrder(order_id));
        }
        if new_qty.0 == 0 || new_qty.0 > crate::MAX_ORDER_QTY {
            return Err(BookError::InvalidQty(new_qty.0));
        }
        if new_qty > slot.remaining {
            return Err(BookError::InvalidQty(new_qty.0));
        }
        let new_price_idx = self.price_index(new_price)?;

        // Validate the new level aggregate before detaching: a same-level
        // amendment must subtract the order's own remaining quantity, and a
        // cross-level amendment adds to a level that does not include it.
        let same_level = slot.price_idx as usize == new_price_idx;
        let target = &self.levels(slot.side)[new_price_idx];
        let base_qty = if same_level {
            target.qty.saturating_sub(slot.remaining.0)
        } else {
            target.qty
        };
        let base_orders = if same_level {
            target.orders.saturating_sub(1)
        } else {
            target.orders
        };
        if base_qty.checked_add(new_qty.0).is_none() || base_orders.checked_add(1).is_none() {
            return Err(BookError::Overflow);
        }

        let removed = RestingOrder {
            order_id: slot.order_id,
            participant: slot.participant,
            side: slot.side,
            price: self.price_at(slot.price_idx as usize),
            qty: slot.qty,
            remaining: slot.remaining,
            seq: slot.seq,
        };

        self.detach(index);
        {
            let slot = &mut self.slots[index as usize];
            slot.price_idx = new_price_idx as u32;
            slot.qty = new_qty;
            slot.remaining = new_qty;
            slot.seq = seq;
        }
        self.attach(index);
        debug_assert_eq!(
            self.best_index(removed.side),
            self.bitmap_best_index(removed.side)
        );
        Ok(removed)
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
        self.detach(index);
        self.index.remove(&slot.order_id);
        self.slots[index as usize].active = false;
        self.slots[index as usize].next = NONE;
        self.slots[index as usize].prev = NONE;
        self.free.push(index);
        self.order_count = self.order_count.saturating_sub(1);
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
            min_price_ticks: self.min_ticks,
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
            let slot = book.insert(
                order.order_id,
                order.participant,
                order.side,
                order.price,
                order.remaining,
                order.seq,
            )?;
            // `insert` treats its quantity as both original and remaining. A
            // partially filled resting order has `qty > remaining`; restore the
            // original so a snapshot round-trip is byte-identical.
            book.slots[slot as usize].qty = order.qty;
        }
        Ok(book)
    }

    /// Check every structural invariant of the book, returning the first
    /// violation.
    ///
    /// This is a diagnostic for tests: it walks the whole ladder, arena, free
    /// list, and order-id index, so it is O(ladder + capacity). It never
    /// panics, and it is never called from a matching or snapshot path.
    ///
    /// Invariants checked:
    ///
    /// - `min_ticks <= max_ticks`, both sides have exactly `ladder_len` levels,
    ///   and both occupancy bitmaps have `ladder_len.div_ceil(64)` words with no
    ///   bits set past the ladder;
    /// - `order_count`, the order-id index length, and the number of active
    ///   slots agree, and no index entry points at an inactive or foreign slot;
    /// - every active slot has `1 <= remaining <= qty`, sits on the side and
    ///   ladder index of the level that links it, and is reachable from that
    ///   level's `head` exactly once through `next` (no cycles, no shared
    ///   slots) with matching `prev` links, ending at [`NONE`] at the level's
    ///   `tail`;
    /// - each level's `orders` and `qty` equal the count and sum of `remaining`
    ///   over its list, an empty level has no list, and a level carries its
    ///   bitmap bit exactly when it is occupied;
    /// - the free list holds every inactive slot exactly once and no active
    ///   slot; the arena therefore has no leaked slots;
    /// - `best_bid_idx`/`best_ask_idx` equal the bitmap-derived best (highest
    ///   occupied bid, lowest occupied ask), or `None` on an empty side;
    /// - the last trade is either entirely unset or has a positive quantity and
    ///   a price inside the ladder.
    pub fn check_invariants(&self) -> Result<(), String> {
        if self.min_ticks > self.max_ticks {
            return Err(format!(
                "ladder bounds are inverted: min {} > max {}",
                self.min_ticks, self.max_ticks
            ));
        }
        let ladder_len = self
            .max_ticks
            .checked_sub(self.min_ticks)
            .and_then(|span| span.checked_add(1))
            .and_then(|len| usize::try_from(len).ok())
            .ok_or_else(|| {
                format!(
                    "ladder [{}..={}] does not fit in a usize",
                    self.min_ticks, self.max_ticks
                )
            })?;
        if self.bids.len() != ladder_len || self.asks.len() != ladder_len {
            return Err(format!(
                "ladder length mismatch: bounds need {ladder_len} levels, bids has {}, asks has {}",
                self.bids.len(),
                self.asks.len()
            ));
        }
        let words = ladder_len.div_ceil(64);
        if self.bid_bitmap.len() != words || self.ask_bitmap.len() != words {
            return Err(format!(
                "bitmap word mismatch: the ladder needs {words} words, bids has {}, asks has {}",
                self.bid_bitmap.len(),
                self.ask_bitmap.len()
            ));
        }
        if ladder_len % 64 != 0 {
            // Bits past the ladder must never be set: a stale bit there would
            // make `next_index`/`bitmap_best_index` disagree with the ladder.
            let valid = (1u64 << (ladder_len % 64)) - 1;
            let tail_set = |bitmap: &[u64]| match bitmap.last() {
                Some(word) => *word & !valid != 0,
                None => false,
            };
            if tail_set(&self.bid_bitmap) || tail_set(&self.ask_bitmap) {
                return Err("occupancy bitmap sets bits past the end of the ladder".into());
            }
        }
        if self.order_count > self.slots.len() {
            return Err(format!(
                "order_count {} exceeds the arena capacity {}",
                self.order_count,
                self.slots.len()
            ));
        }
        if self.index.len() != self.order_count {
            return Err(format!(
                "order_count {} does not match the order-id index length {}",
                self.order_count,
                self.index.len()
            ));
        }

        // `visited[i]` is set for every slot reached from a level head. A
        // second visit is a cycle or a slot shared between levels.
        let mut visited = vec![false; self.slots.len()];
        let mut active_slots = 0usize;

        for side in [Side::Buy, Side::Sell] {
            let levels = self.levels(side);
            let bitmap = self.bitmap(side);
            for (price_idx, level) in levels.iter().enumerate() {
                let word = bitmap
                    .get(price_idx / 64)
                    .ok_or_else(|| format!("{side:?} level {price_idx} has no bitmap word"))?;
                let occupied_bit = (word >> (price_idx % 64)) & 1 == 1;
                let occupied = level.orders > 0;
                if occupied != occupied_bit {
                    return Err(format!(
                        "{side:?} level {price_idx}: orders {} but bitmap bit {}",
                        level.orders,
                        u8::from(occupied_bit)
                    ));
                }
                if !occupied {
                    if level.head != NONE || level.tail != NONE || level.qty != 0 {
                        return Err(format!(
                            "{side:?} level {price_idx} is empty but has head {}, tail {}, qty {}",
                            level.head, level.tail, level.qty
                        ));
                    }
                    continue;
                }
                if level.head == NONE || level.tail == NONE {
                    return Err(format!(
                        "{side:?} level {price_idx} is occupied but its list is missing a head or tail"
                    ));
                }

                let mut cursor = level.head;
                let mut prev = NONE;
                let mut count = 0u32;
                let mut qty = 0u64;
                while cursor != NONE {
                    let slot_index = cursor as usize;
                    let slot = self.slots.get(slot_index).ok_or_else(|| {
                        format!("{side:?} level {price_idx} links out-of-range slot {cursor}")
                    })?;
                    if !slot.active {
                        return Err(format!(
                            "{side:?} level {price_idx} links inactive slot {cursor}"
                        ));
                    }
                    if std::mem::replace(&mut visited[slot_index], true) {
                        return Err(format!(
                            "{side:?} level {price_idx} reaches slot {cursor} twice (cycle or shared slot)"
                        ));
                    }
                    if slot.side != side {
                        return Err(format!(
                            "{side:?} level {price_idx} links slot {cursor} of side {:?}",
                            slot.side
                        ));
                    }
                    if slot.price_idx as usize != price_idx {
                        return Err(format!(
                            "{side:?} level {price_idx} links slot {cursor} at index {}",
                            slot.price_idx
                        ));
                    }
                    if slot.prev != prev {
                        return Err(format!(
                            "{side:?} level {price_idx}: slot {cursor} has prev {} but the walk came from {prev}",
                            slot.prev
                        ));
                    }
                    if slot.remaining.0 == 0 || slot.remaining.0 > slot.qty.0 {
                        return Err(format!(
                            "{side:?} level {price_idx}: slot {cursor} has remaining {} outside 1..={} (qty)",
                            slot.remaining.0, slot.qty.0
                        ));
                    }
                    match self.index.get(&slot.order_id) {
                        Some(index) if *index == cursor => {}
                        other => {
                            return Err(format!(
                                "{side:?} level {price_idx}: slot {cursor} (id {}) maps to {other:?}, not back to itself",
                                slot.order_id
                            ));
                        }
                    }
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| "level order count overflow".to_string())?;
                    qty = qty
                        .checked_add(slot.remaining.0)
                        .ok_or_else(|| format!("{side:?} level {price_idx} quantity overflow"))?;
                    prev = cursor;
                    cursor = slot.next;
                }
                if level.tail != prev {
                    return Err(format!(
                        "{side:?} level {price_idx} tail is {} but the list ends at {prev}",
                        level.tail
                    ));
                }
                if level.orders != count {
                    return Err(format!(
                        "{side:?} level {price_idx} claims {} orders but its list has {count}",
                        level.orders
                    ));
                }
                if level.qty != qty {
                    return Err(format!(
                        "{side:?} level {price_idx} qty is {} but its orders sum to {qty}",
                        level.qty
                    ));
                }
                active_slots = active_slots
                    .checked_add(count as usize)
                    .ok_or_else(|| "active slot count overflow".to_string())?;
            }
        }

        if active_slots != self.order_count {
            return Err(format!(
                "order_count is {} but {active_slots} active slots are linked into levels",
                self.order_count
            ));
        }
        for (slot_index, slot) in self.slots.iter().enumerate() {
            if slot.active && !visited[slot_index] {
                return Err(format!(
                    "active slot {slot_index} (id {}) is not linked into any level",
                    slot.order_id
                ));
            }
        }

        // Free list: every inactive slot exactly once, no active slot.
        let mut freed = vec![false; self.slots.len()];
        for &slot_index in &self.free {
            let Some(slot) = self.slots.get(slot_index as usize) else {
                return Err(format!("free list holds out-of-range slot {slot_index}"));
            };
            if std::mem::replace(&mut freed[slot_index as usize], true) {
                return Err(format!("free list holds slot {slot_index} more than once"));
            }
            if slot.active {
                return Err(format!("free list holds active slot {slot_index}"));
            }
        }
        let expected_free = self
            .slots
            .len()
            .checked_sub(active_slots)
            .ok_or_else(|| "active slots exceed the arena".to_string())?;
        if self.free.len() != expected_free {
            return Err(format!(
                "free list has {} slots but the arena has {expected_free} inactive slots",
                self.free.len()
            ));
        }

        // Index entries must point back at their own active slot. Combined with
        // the length check above, this makes the mapping a bijection.
        for (&order_id, &slot_index) in &self.index {
            let Some(slot) = self.slots.get(slot_index as usize) else {
                return Err(format!(
                    "index maps id {order_id} to out-of-range slot {slot_index}"
                ));
            };
            if !slot.active {
                return Err(format!(
                    "index maps id {order_id} to inactive slot {slot_index}"
                ));
            }
            if slot.order_id != order_id {
                return Err(format!(
                    "index maps id {order_id} to slot {slot_index}, which holds id {}",
                    slot.order_id
                ));
            }
        }

        for side in [Side::Buy, Side::Sell] {
            let cached = self.best_index(side);
            let derived = self.bitmap_best_index(side);
            if cached != derived {
                return Err(format!(
                    "{side:?} cached best is {cached:?} but the bitmap says {derived:?}"
                ));
            }
        }

        match self.last_trade_side {
            None => {
                if !self.last_trade_qty.is_zero() {
                    return Err(format!(
                        "last trade has qty {} but no side",
                        self.last_trade_qty.0
                    ));
                }
            }
            Some(_) => {
                if self.last_trade_qty.is_zero() {
                    return Err("last trade has a side but a zero quantity".into());
                }
                if self.last_trade_price.0 < self.min_ticks
                    || self.last_trade_price.0 > self.max_ticks
                {
                    return Err(format!(
                        "last trade price {} is outside the ladder [{}..={}]",
                        self.last_trade_price.0, self.min_ticks, self.max_ticks
                    ));
                }
            }
        }
        Ok(())
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

/// Reproduce [`Book::hash_into`] over a [`BookSnapshot`] instead of the live
/// ladder. Used to compute a symbol's state hash off the shard thread.
///
/// `Book::snapshot` emits orders in exactly the iteration order `hash_into`
/// walks: bids best-first, then asks best-first, FIFO within a level. So
/// consecutive orders that share a side and price form one level, and the level
/// header (ladder index, summed remaining quantity, order count) is rebuilt from
/// the run. The bytes are identical to hashing the live book.
pub fn hash_book_snapshot(snapshot: &BookSnapshot, hasher: &mut blake3::Hasher) {
    hasher.update(b"book");
    hasher.update(snapshot.symbol.as_bytes());
    hasher.update(&snapshot.last_trade_price.0.to_le_bytes());
    hasher.update(&snapshot.last_trade_qty.0.to_le_bytes());
    hasher.update(&[match snapshot.last_trade_side {
        None => 0,
        Some(Side::Buy) => 1,
        Some(Side::Sell) => 2,
    }]);
    hasher.update(&(snapshot.orders.len() as u64).to_le_bytes());

    let orders = &snapshot.orders;
    let mut index = 0;
    while index < orders.len() {
        let side = orders[index].side;
        let price = orders[index].price;
        let run_start = index;
        let mut qty = 0u64;
        let mut count = 0u32;
        while index < orders.len() && orders[index].side == side && orders[index].price == price {
            qty = qty.saturating_add(orders[index].remaining.0);
            count += 1;
            index += 1;
        }
        // `hash_into` writes the level header before its orders.
        let price_idx = price.0 - snapshot.min_price_ticks;
        hasher.update(b"L");
        hasher.update(&(price_idx as u64).to_le_bytes());
        hasher.update(&qty.to_le_bytes());
        hasher.update(&count.to_le_bytes());
        for order in &orders[run_start..index] {
            hasher.update(b"O");
            hasher.update(&order.order_id.0.to_le_bytes());
            hasher.update(&order.participant.0.to_le_bytes());
            hasher.update(&order.remaining.0.to_le_bytes());
            hasher.update(&order.seq.to_le_bytes());
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
    fn from_snapshot_restores_qty_and_remaining() {
        let mut book = Book::new(&config());
        let index = book
            .insert(
                OrderId(7),
                ParticipantId(1),
                Side::Buy,
                Price(100),
                Qty(10),
                1,
            )
            .expect("insert");
        book.reduce(index, Qty(4));
        let snapshot = book.snapshot();
        assert_eq!(snapshot.orders[0].qty, Qty(10));
        assert_eq!(snapshot.orders[0].remaining, Qty(6));

        let restored = Book::from_snapshot(&config(), snapshot.clone()).expect("restore");
        assert_eq!(
            restored.snapshot().orders[0].qty,
            snapshot.orders[0].qty,
            "the original quantity must survive a snapshot round trip"
        );
        assert_eq!(
            restored.snapshot().orders[0].remaining,
            snapshot.orders[0].remaining
        );
    }

    #[test]
    fn amend_relinks_at_the_tail_of_the_same_level() {
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
            Qty(10),
            2,
        )
        .unwrap();
        let removed = book
            .amend(OrderId(1), ParticipantId(1), Price(100), Qty(4), 3)
            .expect("amend");
        assert_eq!(removed.price, Price(100));
        assert_eq!(removed.remaining, Qty(10));
        assert_eq!(removed.qty, Qty(10));

        let snapshot = book.snapshot();
        assert_eq!(
            snapshot
                .orders
                .iter()
                .map(|order| order.order_id)
                .collect::<Vec<_>>(),
            vec![OrderId(2), OrderId(1)],
            "an amended order goes to the back of its level"
        );
        assert_eq!(snapshot.orders[1].remaining, Qty(4));
        assert_eq!(snapshot.orders[1].seq, 3);
        assert_eq!(book.depth_qty(Side::Buy), 14);
        book.check_invariants().expect("amended book is consistent");
    }

    #[test]
    fn amend_moves_between_levels_and_updates_the_best_cache() {
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
            Price(101),
            Qty(5),
            2,
        )
        .unwrap();
        assert_eq!(book.best_bid(), Some(Price(101)));

        book.amend(OrderId(2), ParticipantId(2), Price(100), Qty(5), 3)
            .expect("amend down");
        assert_eq!(book.best_bid(), Some(Price(100)));
        assert_eq!(book.depth_qty(Side::Buy), 15);
        book.check_invariants().expect("amended book is consistent");

        book.amend(OrderId(1), ParticipantId(1), Price(102), Qty(3), 4)
            .expect("amend up");
        assert_eq!(book.best_bid(), Some(Price(102)));
        assert_eq!(book.depth_qty(Side::Buy), 8);
        book.check_invariants().expect("amended book is consistent");
    }

    #[test]
    fn amend_refuses_a_foreign_order_and_a_size_increase() {
        let mut book = Book::new(&config());
        book.insert(
            OrderId(1),
            ParticipantId(1),
            Side::Sell,
            Price(110),
            Qty(10),
            1,
        )
        .unwrap();
        assert_eq!(
            book.amend(OrderId(1), ParticipantId(2), Price(110), Qty(5), 2)
                .expect_err("a foreign order cannot be amended"),
            BookError::UnknownOrder(OrderId(1))
        );
        assert_eq!(
            book.amend(OrderId(1), ParticipantId(1), Price(110), Qty(11), 2)
                .expect_err("size cannot grow"),
            BookError::InvalidQty(11)
        );
        assert_eq!(
            book.amend(OrderId(1), ParticipantId(1), Price(50), Qty(5), 2)
                .expect_err("the new price is outside the ladder"),
            BookError::PriceOutOfRange(Price(50))
        );
        assert_eq!(book.order_count(), 1);
        assert_eq!(book.depth_qty(Side::Sell), 10);
        book.check_invariants()
            .expect("refusals leave the book untouched");
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

    #[test]
    fn snapshot_hash_matches_live_book_hash() {
        let mut config = InstrumentConfig::new("BTC-USD", 1, 95, 130).unwrap();
        config.max_orders = 256;
        let mut book = Book::new(&config);
        add(&mut book, 1, Side::Buy, 100, 5);
        add(&mut book, 2, Side::Buy, 100, 7);
        add(&mut book, 3, Side::Buy, 104, 3);
        add(&mut book, 4, Side::Sell, 108, 11);
        add(&mut book, 5, Side::Sell, 108, 2);
        add(&mut book, 6, Side::Sell, 110, 13);
        book.reduce(book.order_index(OrderId(1)).unwrap(), Qty(2));
        book.record_trade(Price(104), Qty(2), Side::Buy);

        let snapshot = book.snapshot();
        let mut live = blake3::Hasher::new();
        book.hash_into(&mut live);
        let mut from_snapshot = blake3::Hasher::new();
        hash_book_snapshot(&snapshot, &mut from_snapshot);
        assert_eq!(live.finalize(), from_snapshot.finalize());
    }

    #[test]
    fn check_invariants_accepts_live_book_states() {
        let mut book = Book::new(&config());
        book.check_invariants()
            .expect("an empty book is consistent");

        add(&mut book, 1, Side::Buy, 100, 10);
        add(&mut book, 2, Side::Buy, 100, 5);
        add(&mut book, 3, Side::Sell, 110, 7);
        add(&mut book, 4, Side::Sell, 105, 3);
        book.check_invariants()
            .expect("a populated book is consistent");

        let slot = book.order_index(OrderId(1)).expect("order 1 rests");
        assert!(!book.reduce(slot, Qty(4)));
        book.check_invariants()
            .expect("a partial fill is consistent");
        assert!(book.reduce(slot, Qty(6)));
        book.cancel(OrderId(3), None).expect("cancel");
        book.check_invariants().expect("removals are consistent");

        let restored = Book::from_snapshot(&config(), book.snapshot()).expect("restore");
        restored
            .check_invariants()
            .expect("a restored book is consistent");
    }

    #[test]
    fn check_invariants_detects_a_level_aggregate_mismatch() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 10);
        book.bids[0].qty += 1;
        assert!(book.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_detects_a_missing_bitmap_bit() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Sell, 105, 1);
        book.ask_bitmap[0] &= !(1u64 << 5);
        assert!(book.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_detects_a_stale_best_cache() {
        let mut book = Book::new(&config());
        book.best_bid_idx = Some(0);
        assert!(book.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_detects_a_dangling_index_entry() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        let slot = book.order_index(OrderId(1)).expect("order 1 rests");
        book.index.insert(OrderId(9), slot);
        book.order_count += 1;
        assert!(book.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_detects_an_active_slot_in_the_free_list() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        let slot = book.order_index(OrderId(1)).expect("order 1 rests");
        book.free.push(slot);
        assert!(book.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_detects_a_list_cycle() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        add(&mut book, 2, Side::Buy, 100, 5);
        let head = book.head(Side::Buy, 0).expect("level head");
        book.slots[head as usize].next = head;
        assert!(book.check_invariants().is_err());
    }

    #[test]
    fn check_invariants_detects_remaining_above_the_original_quantity() {
        // Reachable through the public snapshot API: `from_snapshot` trusts the
        // serialized `qty`/`remaining` pair after inserting by `remaining`.
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 10);
        let mut snapshot = book.snapshot();
        snapshot.orders[0].qty = Qty(4);
        let restored = Book::from_snapshot(&config(), snapshot).expect("insert accepts it");
        let error = restored
            .check_invariants()
            .expect_err("remaining 10 > qty 4");
        assert!(error.contains("remaining"), "{error}");
    }

    #[test]
    fn check_invariants_detects_an_inconsistent_last_trade() {
        let mut book = Book::new(&config());
        add(&mut book, 1, Side::Buy, 100, 5);
        let mut snapshot = book.snapshot();
        assert_eq!(snapshot.last_trade_side, None);
        snapshot.last_trade_qty = Qty(5);
        let restored = Book::from_snapshot(&config(), snapshot).expect("insert accepts it");
        let error = restored
            .check_invariants()
            .expect_err("a quantity without a side is invalid");
        assert!(error.contains("last trade"), "{error}");

        let mut snapshot = book.snapshot();
        snapshot.last_trade_side = Some(Side::Sell);
        snapshot.last_trade_qty = Qty::ZERO;
        let restored = Book::from_snapshot(&config(), snapshot).expect("insert accepts it");
        assert!(restored.check_invariants().is_err());

        // A live trade is consistent.
        book.record_trade(Price(100), Qty(5), Side::Buy);
        book.check_invariants().expect("a recorded trade is valid");
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

        /// Structural invariants hold after every insert, cancel, and reduce on
        /// a randomly shaped book, including after the arena reuses slots.
        #[test]
        fn check_invariants_holds_after_random_book_operations(
            ops in proptest::collection::vec(
                (any::<bool>(), 100i64..=130, 1u64..=20, any::<bool>(), 0u32..=8),
                1..200,
            ),
        ) {
            let mut config = InstrumentConfig::new("BTC-USD", 100, 100, 130).unwrap();
            config.max_orders = 64;
            let mut book = Book::new(&config);
            for (index, &(buy, price, qty, cancel, target)) in ops.iter().enumerate() {
                if cancel {
                    book.cancel(OrderId(u128::from(target) + 1), None);
                } else {
                    let _ = book.insert(
                        OrderId(index as u128 + 1),
                        ParticipantId(1),
                        if buy { Side::Buy } else { Side::Sell },
                        Price(price),
                        Qty(qty),
                        index as u64,
                    );
                }
                // Reduce the head of a random level so partial fills and full
                // removals both run through the level accounting.
                if let Some(level_head) = book
                    .head(if buy { Side::Buy } else { Side::Sell }, (price - 100) as usize)
                {
                    let slot = *book.slot(level_head);
                    book.reduce(level_head, Qty(slot.remaining.0.min(u64::from(target))));
                }
                let check = book.check_invariants();
                prop_assert!(check.is_ok(), "after op {index}: {check:?}");
            }
        }
    }
}
