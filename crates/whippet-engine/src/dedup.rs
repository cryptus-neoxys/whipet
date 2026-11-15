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

    pub fn capacity(&self) -> usize {
        self.capacity
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
        let terminal = matches!(
            entry.status,
            DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
        );
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
        if terminal {
            // A rejected entry is inserted terminal without an `update`
            // transition; it must still be eviction-eligible and present in
            // the snapshot's terminal queue.
            self.terminal_order.push_back(order_id);
        }
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
    ///
    /// A re-admitted id appears once per admission, so `insertion_order` is the
    /// exact upper bound and the buffer is sized for it: `collect` over a
    /// lazy filter grows and copies a multi-megabyte Vec, which is paid on the
    /// shard thread every time it clones the state for the snapshot writer.
    pub fn entries_in_order(&self) -> Vec<(OrderId, DedupEntry)> {
        let mut entries = Vec::with_capacity(self.insertion_order.len());
        for order_id in &self.insertion_order {
            if let Some(entry) = self.map.get(order_id) {
                entries.push((*order_id, *entry));
            }
        }
        entries
    }

    /// Drop ids that were evicted but are still sitting in the order deques.
    ///
    /// `hash_into` already skips them, so this does not change the state hash.
    /// It makes the snapshot walk equal the live walk, which a restored table
    /// needs if one of those ids is re-admitted after the tail.
    pub fn compact(&mut self) {
        let map = &self.map;
        self.insertion_order.retain(|id| map.contains_key(id));
        self.terminal_order.retain(|id| map.contains_key(id));
    }

    /// The terminal ids in eviction order. Not part of the state hash, but
    /// snapshotted (as [`DedupTable::terminal_indices`]) so a restored table
    /// evicts at the same point as a continuous replay.
    pub fn terminal_order(&self) -> Vec<OrderId> {
        self.terminal_order.iter().copied().collect()
    }

    /// The terminal queue as indices into an insertion-order walk.
    ///
    /// The snapshot stores these instead of the terminal ids themselves: a
    /// `u32` is 4 bytes per terminal id, where serde writes an [`OrderId`] as a
    /// 32-character hex string (40 bytes with bincode's length prefix). At the
    /// 100k dedup window that is a multi-megabyte difference, and the writer's
    /// serialize+fsync is the dominant snapshot cost. Indices are not hashed,
    /// so they cannot move the state hash.
    ///
    /// `entries` must be the exact walk the snapshot will carry (see
    /// [`DedupTable::entries_in_order`]): the indices are positions in that
    /// slice. An id that was evicted and re-admitted appears more than once in
    /// the walk, so the index of its **last** occurrence is used. The terminal
    /// queue is then mapped in order, which preserves duplicate queue entries
    /// exactly as the live deque has them.
    ///
    /// Build the walk once and pass it to both this and the snapshot, instead
    /// of walking the table twice.
    pub fn terminal_indices(&self, entries: &[(OrderId, DedupEntry)]) -> Vec<u32> {
        let mut last: FxHashMap<OrderId, u32> = FxHashMap::default();
        last.reserve(entries.len());
        for (index, (order_id, _)) in entries.iter().enumerate() {
            // A walk longer than `u32::MAX` cannot be addressed by the compact
            // form. That is unreachable through a 256 MiB snapshot cap; drop
            // the unreachable positions rather than truncate silently.
            let Ok(index) = u32::try_from(index) else {
                continue;
            };
            last.insert(*order_id, index);
        }
        let mut indices = Vec::with_capacity(self.terminal_order.len());
        for order_id in &self.terminal_order {
            if let Some(index) = last.get(order_id) {
                indices.push(*index);
            }
        }
        indices
    }

    /// Rebuild a table from a snapshot walk and the terminal-queue indices
    /// produced by [`DedupTable::terminal_indices`].
    ///
    /// Out-of-range indices and indices that point at a non-terminal entry are
    /// skipped: a corrupt snapshot must not panic the restore path. Skipping
    /// cannot desynchronise a valid snapshot because [`DedupTable::terminal_indices`]
    /// only emits in-range, terminal ids.
    ///
    /// Known gap, predating the compact terminal form: `entries` is the walk
    /// filtered to the live map, so insertion-order entries for ids that were
    /// already evicted at snapshot time are not carried. If such an id is
    /// re-admitted after the restore it is walked fewer times than a continuous
    /// table would walk it, which can move the *dedup state hash* after the
    /// tail (eviction order is unaffected, and the restored hash at the
    /// snapshot point still matches). Carrying the dead entries would cost an
    /// order id per entry, so they are not part of the snapshot.
    pub fn from_entries(
        capacity: usize,
        entries: Vec<(OrderId, DedupEntry)>,
        terminal_idx: Vec<u32>,
    ) -> Result<Self, String> {
        // Use the configured capacity, not the insertion-walk length: an id
        // that was evicted and re-admitted appears more than once in `entries`,
        // so `entries.len()` can exceed the live map size. Inflating the
        // capacity would make the restored table evict later than a continuous
        // replay, and the two states diverge once the tail is long enough.
        let mut table = Self::new(capacity);
        for (order_id, entry) in &entries {
            table.insertion_order.push_back(*order_id);
            table.map.insert(*order_id, *entry);
        }
        // Rebuild the eviction queue in its original transition order. An id
        // that was evicted and re-admitted appears once per terminal transition
        // in the queue, so this cannot be reconstructed from the insertion walk
        // alone; `terminal_idx` is what carries it.
        //
        // A repeated index is harmless: the second occurrence is stale after
        // the first eviction and is skipped by the eviction walk, exactly as a
        // continuous table with the same queue would behave. It is therefore
        // allowed, while an index outside the walk or one pointing at a
        // non-terminal entry is a corrupt snapshot and fails closed.
        let mut queued: std::collections::HashSet<OrderId> =
            std::collections::HashSet::with_capacity(terminal_idx.len());
        for index in terminal_idx {
            let Some((order_id, entry)) = entries.get(index as usize) else {
                return Err(format!("terminal index {index} is out of range"));
            };
            if !matches!(
                entry.status,
                DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
            ) {
                return Err(format!(
                    "terminal index {index} points at a non-terminal entry"
                ));
            }
            queued.insert(*order_id);
            table.terminal_order.push_back(*order_id);
        }
        // Every terminal id in the map must be in the queue, or the restored
        // table would evict a different set than a continuous replay.
        let terminal_in_map = table
            .map
            .values()
            .filter(|entry| {
                matches!(
                    entry.status,
                    DedupStatus::Filled | DedupStatus::Cancelled | DedupStatus::Rejected
                )
            })
            .count();
        if queued.len() != terminal_in_map {
            return Err(format!(
                "terminal queue covers {} terminal ids but the map has {terminal_in_map}",
                queued.len()
            ));
        }
        Ok(table)
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
        // The terminal queue decides which id is evicted next. It is not
        // derivable from the map, so it must be in the hash: otherwise a
        // snapshot whose (otherwise valid) indices were reordered restores a
        // table that evicts differently while every hash still matches.
        // Stale ids are skipped, matching the snapshot form, which can only
        // resolve ids present in its walk.
        let live = self
            .terminal_order
            .iter()
            .filter(|order_id| self.map.contains_key(order_id))
            .count();
        hasher.update(b"terminal");
        hasher.update(&(live as u64).to_le_bytes());
        for order_id in &self.terminal_order {
            if self.map.contains_key(order_id) {
                hasher.update(&order_id.0.to_le_bytes());
            }
        }
    }
}

/// Reproduce [`DedupTable::hash_into`] over the snapshot form of the table: the
/// entries in insertion order, as returned by [`DedupTable::entries_in_order`].
///
/// The count is the number of **distinct** order ids, not the length of the
/// slice, because that is what [`DedupTable::hash_into`] writes (`map.len()`).
/// The two differ once the idempotency window turns over: an id that is evicted
/// and later re-admitted is walked once per admission but occupies one map
/// slot. A 200k-command run reaches that state routinely, which is why this
/// function must not simply hash `entries.len()`.
pub fn hash_dedup_entries(
    entries: &[(OrderId, DedupEntry)],
    terminal: &[OrderId],
    hasher: &mut blake3::Hasher,
) {
    hasher.update(b"dedup");
    hasher.update(&(distinct_order_ids(entries) as u64).to_le_bytes());
    for (order_id, entry) in entries {
        hasher.update(&order_id.0.to_le_bytes());
        hasher.update(&[entry.status as u8]);
        hasher.update(&entry.input_offset.to_le_bytes());
        hasher.update(&entry.timestamp_ns.to_le_bytes());
        hasher.update(&entry.seq.0.to_le_bytes());
    }
    hasher.update(b"terminal");
    hasher.update(&(terminal.len() as u64).to_le_bytes());
    for order_id in terminal {
        hasher.update(&order_id.0.to_le_bytes());
    }
}

/// Number of distinct order ids in an insertion-order walk.
///
/// `entries_in_order` yields one item per admission, so a re-admitted id
/// appears more than once. This runs on the snapshot writer thread, once per
/// persistence interval, so a sort is cheaper and far more predictable than a
/// set of a hundred thousand ids.
fn distinct_order_ids(entries: &[(OrderId, DedupEntry)]) -> usize {
    if entries.len() < 2 {
        return entries.len();
    }
    let mut ids: Vec<OrderId> = entries.iter().map(|(order_id, _)| *order_id).collect();
    ids.sort_unstable_by_key(|order_id| order_id.0);
    ids.dedup();
    ids.len()
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
    fn compact_preserves_the_hash_and_drops_dead_ids() {
        let mut table = DedupTable::new(2);
        for id in 1..=3u64 {
            table.insert(OrderId(u128::from(id)), entry(id));
            table.update(OrderId(u128::from(id)), DedupStatus::Filled);
        }
        assert!(table.insertion_order.len() > table.len());
        let mut before = blake3::Hasher::new();
        table.hash_into(&mut before);

        table.compact();
        let mut after = blake3::Hasher::new();
        table.hash_into(&mut after);
        assert_eq!(before.finalize(), after.finalize());
        assert_eq!(table.insertion_order.len(), table.len());
        assert_eq!(table.terminal_order.len(), table.len());
    }

    #[test]
    fn snapshot_restore_preserves_capacity_and_eviction() {
        let mut table = DedupTable::new(4);
        for id in 1..=4u64 {
            table.insert(OrderId(u128::from(id)), entry(id));
            table.update(OrderId(u128::from(id)), DedupStatus::Filled);
        }
        // Evict id 1, then re-admit it: the insertion walk is now longer than
        // the map, which is the case that used to inflate the restored capacity.
        table.insert(OrderId(5), entry(5));
        table.update(OrderId(5), DedupStatus::Filled);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);

        let walk = table.entries_in_order();
        assert!(walk.len() > table.len(), "test precondition: re-admission");

        let terminal_idx = table.terminal_indices(&walk);
        let mut restored =
            DedupTable::from_entries(4, walk, terminal_idx).expect("valid terminal indices");
        assert_eq!(restored.capacity(), 4, "restored capacity must not inflate");
        assert_eq!(restored.len(), table.len());

        // Both tables now evict the same id under the same insert.
        table.insert(OrderId(6), entry(6));
        restored.insert(OrderId(6), entry(6));
        for id in 1..=6u64 {
            assert_eq!(
                table.contains(OrderId(u128::from(id))),
                restored.contains(OrderId(u128::from(id))),
                "id {id} must be present in both tables"
            );
        }
    }

    #[test]
    fn snapshot_restore_preserves_eviction_order() {
        let mut table = DedupTable::new(2);
        table.insert(OrderId(5), entry(5));
        table.update(OrderId(5), DedupStatus::Filled);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);
        let walk = table.entries_in_order();
        let terminal_idx = table.terminal_indices(&walk);
        let mut restored =
            DedupTable::from_entries(2, walk, terminal_idx).expect("valid terminal indices");
        table.insert(OrderId(9), entry(9));
        restored.insert(OrderId(9), entry(9));
        assert_eq!(table.contains(OrderId(5)), restored.contains(OrderId(5)));
        assert_eq!(table.contains(OrderId(1)), restored.contains(OrderId(1)));
    }

    #[test]
    fn terminal_indices_restore_the_same_queue_after_an_evicted_id_is_readmitted() {
        // Id 1 is terminal, evicted, then re-admitted and terminal again. The
        // walk contains it twice, so the compact form must resolve the id via
        // its last occurrence, and the restored queue must match the live one
        // transition for transition.
        let mut table = DedupTable::new(4);
        for id in 1..=4u64 {
            table.insert(OrderId(u128::from(id)), entry(id));
            table.update(OrderId(u128::from(id)), DedupStatus::Filled);
        }
        table.insert(OrderId(5), entry(5));
        table.update(OrderId(5), DedupStatus::Filled);
        table.insert(OrderId(1), entry(9));
        table.update(OrderId(1), DedupStatus::Filled);

        let walk = table.entries_in_order();
        assert_eq!(
            walk.iter()
                .filter(|(order_id, _)| *order_id == OrderId(1))
                .count(),
            2,
            "test precondition: the re-admitted id appears twice"
        );
        let live_queue = table.terminal_order();
        let terminal_idx = table.terminal_indices(&walk);

        // Every index resolves to the id it encodes, in queue order.
        let resolved: Vec<OrderId> = terminal_idx
            .iter()
            .map(|index| walk[*index as usize].0)
            .collect();
        assert_eq!(resolved, live_queue);

        let mut restored =
            DedupTable::from_entries(4, walk, terminal_idx).expect("valid terminal indices");
        assert_eq!(
            restored.terminal_order(),
            live_queue,
            "the restored eviction queue diverged"
        );

        // And it evicts the same id as the continuous table on the next insert.
        table.insert(OrderId(6), entry(6));
        restored.insert(OrderId(6), entry(6));
        assert_eq!(restored.len(), table.len());
        for id in 1..=6u64 {
            assert_eq!(
                table.contains(OrderId(u128::from(id))),
                restored.contains(OrderId(u128::from(id))),
                "id {id} differs between continuous and restored tables"
            );
        }
        assert_eq!(restored.terminal_order(), table.terminal_order());
    }

    #[test]
    fn malformed_terminal_indices_are_rejected() {
        let mut table = DedupTable::new(4);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);
        table.insert(OrderId(2), entry(2));
        let walk = table.entries_in_order();

        // 0 is a terminal entry, 1 is a non-terminal entry, 9 and u32::MAX are
        // out of range. A corrupt terminal queue must fail closed, not restore
        // a table that evicts differently from a continuous replay.
        let restored = DedupTable::from_entries(4, walk, vec![0, 1, 9, u32::MAX]);
        assert!(
            restored.is_err(),
            "a corrupt terminal queue must be rejected"
        );
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
        let terminal = table.terminal_order();
        expected.update(b"terminal");
        expected.update(&(terminal.len() as u64).to_le_bytes());
        for order_id in &terminal {
            expected.update(&order_id.0.to_le_bytes());
        }

        let mut actual = blake3::Hasher::new();
        table.hash_into(&mut actual);
        assert_eq!(actual.finalize(), expected.finalize());
    }

    /// Hash the table the way the snapshot writer does: over the insertion-order
    /// walk, with no access to the map.
    fn snapshot_hash(table: &DedupTable) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        let terminal = table.terminal_order();
        hash_dedup_entries(&table.entries_in_order(), &terminal, &mut hasher);
        hasher.finalize()
    }

    fn live_hash(table: &DedupTable) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        table.hash_into(&mut hasher);
        hasher.finalize()
    }

    #[test]
    fn snapshot_hash_matches_after_an_evicted_id_is_readmitted() {
        // An id that leaves the window and comes back is walked twice by
        // `hash_into` but occupies one map slot. Hashing the walk's length
        // instead of the distinct ids made the off-loop hash disagree with the
        // live one at ~170k orders, so pin it.
        let mut table = DedupTable::new(2);
        table.insert(OrderId(1), entry(1));
        table.update(OrderId(1), DedupStatus::Filled);
        table.insert(OrderId(2), entry(2));
        table.update(OrderId(2), DedupStatus::Filled);
        table.insert(OrderId(3), entry(3));
        assert!(!table.contains(OrderId(1)), "id 1 should have been evicted");

        table.insert(OrderId(1), entry(9));
        assert_eq!(table.len(), 2, "admitting id 1 evicted another terminal id");
        assert!(table.contains(OrderId(1)));
        let walk = table.entries_in_order();
        assert_eq!(
            walk.iter()
                .filter(|(order_id, _)| *order_id == OrderId(1))
                .count(),
            2,
            "the re-admitted id should appear twice in the walk"
        );
        assert!(walk.len() > table.len(), "the walk is longer than the map");
        assert_eq!(snapshot_hash(&table), live_hash(&table));
    }

    #[test]
    fn snapshot_hash_matches_over_a_full_window_turnover() {
        // Keep re-admitting a rotating id while evicting terminal entries, so
        // the walk stays longer than the map.
        let mut table = DedupTable::new(8);
        for round in 0..200u64 {
            let order_id = OrderId(u128::from(round % 12) + 1);
            table.insert(order_id, entry(round));
            table.update(order_id, DedupStatus::Filled);
            if round % 3 == 0 {
                table.insert(OrderId(u128::from(round) + 100), entry(round));
            }
        }
        assert!(table.entries_in_order().len() > table.len());
        assert_eq!(snapshot_hash(&table), live_hash(&table));
    }
}
