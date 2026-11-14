// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Durable business deduplication table.
//!
//! Iggy's idempotent producer deduplicates batches inside a bounded producer
//! window. That is not business deduplication: a client retry can arrive with
//! a new producer epoch, and a replay can re-deliver the same order. Whippet
//! therefore keeps its own `client_order_id` table, snapshotted atomically
//! with the book and the input offset.

use std::collections::{HashMap, VecDeque};

use serde::{Deserialize, Serialize};
use whippet_core::ids::{OrderId, Seq};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DedupStatus {
    Accepted = 1,
    Resting = 2,
    Filled = 3,
    Cancelled = 4,
    Rejected = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DedupEntry {
    pub status: DedupStatus,
    pub input_offset: u64,
    pub timestamp_ns: u64,
    pub seq: Seq,
}

pub struct DedupTable {
    capacity: usize,
    map: HashMap<OrderId, DedupEntry>,
    insertion_order: VecDeque<OrderId>,
}

impl DedupTable {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::with_capacity(capacity.min(1_000_000)),
            insertion_order: VecDeque::with_capacity(capacity.min(1_000_000)),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, order_id: OrderId) -> Option<DedupEntry> {
        self.map.get(&order_id).copied()
    }

    pub fn contains(&self, order_id: OrderId) -> bool {
        self.map.contains_key(&order_id)
    }

    /// Record a first observation. Returns the previous entry if the id was
    /// already present; callers treat that as a duplicate.
    pub fn insert(&mut self, order_id: OrderId, entry: DedupEntry) -> Option<DedupEntry> {
        if let Some(previous) = self.map.get(&order_id) {
            return Some(*previous);
        }
        if self.map.len() >= self.capacity
            && let Some(oldest) = self.insertion_order.pop_front()
        {
            self.map.remove(&oldest);
        }
        self.map.insert(order_id, entry);
        self.insertion_order.push_back(order_id);
        None
    }

    pub fn update(&mut self, order_id: OrderId, status: DedupStatus) {
        if let Some(entry) = self.map.get_mut(&order_id) {
            entry.status = status;
        }
    }

    pub fn entries_sorted(&self) -> Vec<(OrderId, DedupEntry)> {
        let mut entries: Vec<_> = self.map.iter().map(|(id, entry)| (*id, *entry)).collect();
        entries.sort_unstable_by_key(|(id, _)| *id);
        entries
    }

    pub fn from_entries(capacity: usize, entries: Vec<(OrderId, DedupEntry)>) -> Self {
        let mut table = Self::new(capacity.max(entries.len()));
        for (order_id, entry) in entries {
            table.insertion_order.push_back(order_id);
            table.map.insert(order_id, entry);
        }
        table
    }

    pub fn hash_into(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"dedup");
        hasher.update(&(self.map.len() as u64).to_le_bytes());
        for (order_id, entry) in self.entries_sorted() {
            hasher.update(&order_id.0.to_le_bytes());
            hasher.update(&[entry.status as u8]);
            hasher.update(&entry.input_offset.to_le_bytes());
            hasher.update(&entry.timestamp_ns.to_le_bytes());
            hasher.update(&entry.seq.0.to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(offset: u64) -> DedupEntry {
        DedupEntry {
            status: DedupStatus::Accepted,
            input_offset: offset,
            timestamp_ns: offset,
            seq: Seq(offset),
        }
    }

    #[test]
    fn duplicate_returns_previous_entry() {
        let mut table = DedupTable::new(16);
        assert!(table.insert(OrderId(1), entry(1)).is_none());
        let previous = table.insert(OrderId(1), entry(2)).unwrap();
        assert_eq!(previous.input_offset, 1);
    }

    #[test]
    fn capacity_evicts_oldest() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(1), entry(1));
        table.insert(OrderId(2), entry(2));
        table.insert(OrderId(3), entry(3));
        assert!(!table.contains(OrderId(1)));
        assert!(table.contains(OrderId(2)));
        assert!(table.contains(OrderId(3)));
    }

    #[test]
    fn hash_is_order_independent_of_map_layout() {
        let mut a = DedupTable::new(8);
        let mut b = DedupTable::new(8);
        a.insert(OrderId(1), entry(1));
        a.insert(OrderId(2), entry(2));
        b.insert(OrderId(2), entry(2));
        b.insert(OrderId(1), entry(1));
        let mut ha = blake3::Hasher::new();
        let mut hb = blake3::Hasher::new();
        a.hash_into(&mut ha);
        b.hash_into(&mut hb);
        assert_eq!(ha.finalize(), hb.finalize());
    }
}
