// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Deterministic price-time-priority order book.
//!
//! Structure:
//! - contiguous array price ladder indexed by tick,
//! - occupancy bitmap per side for O(1)-ish best-price search,
//! - intrusive doubly linked FIFO per level over a preallocated order arena,
//! - open-addressed-ish `HashMap` index from order id to arena slot.
//!
//! The book never reads the wall clock, never iterates a `HashMap` in a
//! decision path, and never allocates once constructed (apart from the
//! `HashMap` index growing if the arena capacity is misconfigured).

use std::collections::HashMap;

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
    index: HashMap<OrderId, u32>,
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
            index: HashMap::with_capacity(config.max_orders),
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
        if self.index.contains_key(&order_id) {
            return Err(BookError::DuplicateOrderId(order_id));
        }
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
        self.index.insert(order_id, slot_index);
        self.order_count += 1;
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
        }

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
}
