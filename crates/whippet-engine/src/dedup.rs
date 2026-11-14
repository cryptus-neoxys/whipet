// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Durable business deduplication table.
//!
//! Iggy's idempotent producer deduplicates batches inside a bounded producer
//! window. That is not business deduplication: a client retry can arrive with
//! a new producer epoch, and a replay can re-deliver the same order. Whippet
//! therefore keeps its own `client_order_id` table, snapshotted atomically
//! with the book and the input offset.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;
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
    map: FxHashMap<OrderId, DedupEntry>,
    insertion_order: VecDeque<OrderId>,
    /// Terminal entries in transition order. This keeps eviction O(1) even
    /// when the table is full of active resting orders; the old insertion-order
    /// scan could rotate thousands of entries per insert.
    terminal_order: VecDeque<OrderId>,
}

impl DedupTable {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: FxHashMap::with_capacity_and_hasher(capacity.min(1_000_000), Default::default()),
            insertion_order: VecDeque::with_capacity(capacity.min(1_000_000)),
            terminal_order: VecDeque::with_capacity(capacity.min(1_000_000)),
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
        // One hash probe: `insert` hands back the displaced entry, which is
        // exactly the duplicate signal. The old code probed twice (`get` then
        // `insert`). A duplicate restores the displaced entry, so the second
        // probe only happens on that cold path.
        if let Some(previous) = self.map.insert(order_id, entry) {
            self.map.insert(order_id, previous);
            return Some(previous);
        }
        // Evict terminal entries only. A resting order must keep its dedup
        // record even if it is older than the window, otherwise a late retry
        // could rest a second copy. If every entry is active, grow beyond the
        // soft capacity rather than break that invariant.
        //
        // The new entry is already in the map, so the bound is `> capacity`
        // rather than the old `>= capacity` before insertion; the resulting
        // table size and the set of evicted entries are identical.
        while self.map.len() > self.capacity {
            let Some(candidate) = self.terminal_order.pop_front() else {
                break;
            };
            // A stale id can sit in `terminal_order` after being evicted. The
            // entry now in the map is the one just inserted, which the old
            // pre-insert loop could not see, so skip it explicitly.
            if candidate == order_id {
                continue;
            }
            let terminal = self.map.get(&candidate).is_some_and(|entry| {
                matches!(
                    entry.status,
                    DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
                )
            });
            if terminal {
                self.map.remove(&candidate);
            }
        }
        self.insertion_order.push_back(order_id);
        self.compact_if_needed();
        None
    }

    pub fn update(&mut self, order_id: OrderId, status: DedupStatus) {
        let was_terminal = self.map.get(&order_id).is_some_and(|entry| {
            matches!(
                entry.status,
                DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
            )
        });
        if let Some(entry) = self.map.get_mut(&order_id) {
            entry.status = status;
        }
        let is_terminal = matches!(
            status,
            DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
        );
        if is_terminal && !was_terminal {
            self.terminal_order.push_back(order_id);
        }
    }

    fn compact_if_needed(&mut self) {
        if self.insertion_order.len() > self.map.len().saturating_mul(2).max(1_024) {
            let map = &self.map;
            self.insertion_order
                .retain(|order_id| map.contains_key(order_id));
        }
        if self.terminal_order.len() > self.map.len().saturating_mul(2).max(1_024) {
            let map = &self.map;
            self.terminal_order
                .retain(|order_id| map.contains_key(order_id));
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
            if matches!(
                entry.status,
                DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
            ) {
                table.terminal_order.push_back(order_id);
            }
            table.map.insert(order_id, entry);
        }
        table
    }

    pub fn hash_into(&self, hasher: &mut blake3::Hasher) {
        hasher.update(b"dedup");
        hasher.update(&(self.map.len() as u64).to_le_bytes());
        // Walk the insertion-order deque directly. Eviction depends on this
        // order, so the hash must preserve it; the map is lookup-only here and
        // no temporary Vec is allocated.
        for order_id in &self.insertion_order {
            if let Some(entry) = self.map.get(order_id) {
                hasher.update(&order_id.0.to_le_bytes());
                hasher.update(&[entry.status as u8]);
                hasher.update(&entry.input_offset.to_le_bytes());
                hasher.update(&entry.timestamp_ns.to_le_bytes());
                hasher.update(&entry.seq.0.to_le_bytes());
            }
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
    fn duplicate_insert_does_not_evict_at_capacity() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);
        table.insert(OrderId(2), entry(2));

        // At capacity, a brand-new id would evict the terminal entry 1. A
        // duplicate must instead return the stored entry and leave the table,
        // including its eviction order, untouched.
        let previous = table.insert(OrderId(2), entry(9)).expect("duplicate");
        assert_eq!(previous.input_offset, 2);
        assert_eq!(table.get(OrderId(2)).unwrap().input_offset, 2);
        assert!(
            table.contains(OrderId(1)),
            "a duplicate insert must not evict a terminal entry"
        );
        assert_eq!(table.len(), 2);
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
    fn full_table_of_active_orders_grows_without_rotating() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Resting);
        table.insert(OrderId(2), entry(2));
        table.update(OrderId(2), DedupStatus::Resting);
        table.insert(OrderId(3), entry(3));
        assert_eq!(table.len(), 3);
        assert!(table.contains(OrderId(1)));
        assert!(table.contains(OrderId(2)));
        assert!(table.contains(OrderId(3)));
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

    #[test]
    fn hash_into_walks_insertion_order_without_collecting() {
        let mut table = DedupTable::new(8);
        for id in [3u128, 1, 2] {
            table.insert(OrderId(id), entry(id as u64));
        }
        table.update(OrderId(1), DedupStatus::Resting);

        // Independent computation over the insertion-ordered entries. If
        // `hash_into` ever used map iteration order or collected a different
        // sequence, this would diverge.
        let mut expected = blake3::Hasher::new();
        expected.update(b"dedup");
        expected.update(&(table.len() as u64).to_le_bytes());
        for (order_id, entry) in table.entries_in_order() {
            expected.update(&order_id.0.to_le_bytes());
            expected.update(&[entry.status as u8]);
            expected.update(&entry.input_offset.to_le_bytes());
            expected.update(&entry.timestamp_ns.to_le_bytes());
            expected.update(&entry.seq.0.to_le_bytes());
        }

        let mut actual = blake3::Hasher::new();
        table.hash_into(&mut actual);
        assert_eq!(actual.finalize(), expected.finalize());
    }
}
