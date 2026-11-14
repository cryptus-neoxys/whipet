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
        // Evict terminal entries only. A resting order must keep its dedup
        // record even if it is older than the window, otherwise a late retry
        // could rest a second copy. If every entry is active, grow beyond the
        // soft capacity rather than break that invariant.
        let mut attempts = self.insertion_order.len().min(4_096);
        while self.map.len() >= self.capacity && attempts > 0 {
            attempts -= 1;
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            let terminal = self.map.get(&oldest).is_none_or(|entry| {
                matches!(
                    entry.status,
                    DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
                )
            });
            if terminal {
                self.map.remove(&oldest);
            } else {
                self.insertion_order.push_back(oldest);
            }
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

    /// Entries in insertion order. Eviction depends on this order, so the
    /// snapshot and the state hash must preserve it, not sort it.
    pub fn entries_in_order(&self) -> Vec<(OrderId, DedupEntry)> {
        self.insertion_order
            .iter()
            .filter_map(|order_id| self.map.get(order_id).map(|entry| (*order_id, *entry)))
            .collect()
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
        for (order_id, entry) in self.entries_in_order() {
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
    fn capacity_evicts_oldest_terminal_entry() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);
        table.insert(OrderId(2), entry(2));
        table.update(OrderId(2), DedupStatus::Filled);
        table.insert(OrderId(3), entry(3));
        assert!(!table.contains(OrderId(1)));
        assert!(table.contains(OrderId(2)));
        assert!(table.contains(OrderId(3)));
    }

    #[test]
    fn active_entries_survive_eviction() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Resting);
        table.insert(OrderId(2), entry(2));
        table.update(OrderId(2), DedupStatus::Filled);
        table.insert(OrderId(3), entry(3));
        assert!(table.contains(OrderId(1)), "resting order was evicted");
        assert!(
            !table.contains(OrderId(2)),
            "terminal order should be evicted"
        );
        assert!(table.contains(OrderId(3)));
    }

    #[test]
    fn hash_depends_on_insertion_order() {
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
        assert_ne!(ha.finalize(), hb.finalize());
    }

    #[test]
    fn snapshot_restore_preserves_eviction_order() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(5), entry(5));
        table.update(OrderId(5), DedupStatus::Filled);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);
        let mut restored = DedupTable::from_entries(2, table.entries_in_order());
        table.insert(OrderId(9), entry(9));
        restored.insert(OrderId(9), entry(9));
        assert_eq!(table.contains(OrderId(5)), restored.contains(OrderId(5)));
        assert_eq!(table.contains(OrderId(1)), restored.contains(OrderId(1)));
    }
}
