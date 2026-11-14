# ADR 0003: Matching engine data structures

- **Status:** Accepted
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0001](0001-workspace-and-crate-boundaries.md), [ADR 0004](0004-iggy-topics-partitions-durability.md)

## Context

The matcher is the part of Whippet that must be deterministic, fast, and
explainable. It is a single-writer state machine over an ordered input log. It
must not read the wall clock, perform I/O, take locks, or allocate in the match
loop. The same input sequence must produce the same executions, the same market
data, and the same state hash on every run.

The book also has to survive a demo with three symbols and a wide price range
without becoming a sparse-map benchmark. The default instruments are:

- `BTC-USD`, tick 100 cents, ladder 40,000–90,000 ticks;
- `ETH-USD`, tick 10 cents, ladder 15,000–45,000 ticks;
- `SOL-USD`, tick 1 cent, ladder 5,000–35,000 ticks.

A 50,001-level ladder per side is small enough to allocate once and index
directly. That makes an array ladder practical, not just theoretically nice.

## Decision

Use a fixed-capacity, index-based book per symbol:

1. **Contiguous price ladder.** `Book` owns `Vec<Level>` for bids and asks,
   indexed by `price - min_price_ticks`. `Level` stores `head`, `tail`, `qty`,
   and `orders`. No `BTreeMap` in the default path.
2. **Occupancy bitmap.** Each side has a `Vec<u64>` bitmap, one bit per price
   level. Best bid is the highest set bit; best ask is the lowest. The code uses
   `leading_zeros`/`trailing_zeros` to find them, and `next_index` walks the
   bitmap to the next occupied level.
3. **Intrusive FIFO per level.** Each level has a doubly linked list of arena
   indices. `Slot` stores `prev`, `next`, `order_id`, `participant`, `price_idx`,
   `qty`, `remaining`, `side`, `seq`, and `active`. The arena is a preallocated
   `Vec<Slot>` with a free list. `u32::MAX` is the null index.
4. **Order index.** A `HashMap<OrderId, u32>` maps an order id to its arena slot.
   It is preallocated with `max_orders` capacity. It is used for lookup and
   cancel; it is never iterated in a decision path.
5. **Integer prices and quantities.** `Price` is an integer tick index; `Qty` is
   an integer unit count. The wire format and the matcher never use floats.
   Conversion to cents happens at the API/display edge.
6. **Deterministic output ordering.** The engine writes into a caller-owned
   `EngineOutput` with reusable vectors. Every execution and market-data event
   carries the input offset that caused it. The engine does not generate ids
   from randomness or time.
7. **Durable business dedup.** `DedupTable` is a bounded
   `HashMap<OrderId, DedupEntry>` with insertion-order eviction. Capacity is
   `max_orders * 2`. It records `Accepted`, `Resting`, `Filled`, `Cancelled`, or
   `Rejected`, plus the input offset, timestamp, and sequence. It is snapshotted
   with the book and the input offset, not separately.
8. **State hash and snapshots.** `SymbolEngine::state_hash` is BLAKE3 over a
   domain-separated encoding of the book, dedup table, execution sequence,
   market-data sequence, last offset, last timestamp, and halt flag. The shard
   hash is BLAKE3 over the sorted per-symbol hashes. Snapshots are bincode with
   a magic header and version, written to a temp file, fsynced, and renamed.
   Restore recomputes the hash and rejects a mismatch. The `verify` and
   `replay` CLI commands exercise the same hash path.

Implemented order semantics:

- limit (resting, GTC), market with a collar (`market_collar_bps`, default 500
  bps), IOC, FOK, post-only, cancel;
- cancel/replace is not a separate command; a replace is a cancel plus a new
  order, and the new order loses priority;
- cancel-resting self-trade prevention: the resting order is removed and the
  aggressor continues;
- halt/resume is represented in engine state (`halted`), not as a wall-clock
  check;
- stop/stop-limit, iceberg, pegged, trailing, auctions, and multi-leg orders are
  out of scope.

## Consequences

Positive:

- Best-price lookup is a bitmap scan over a few words, not a tree walk.
- FIFO priority is explicit and reconstructible: a snapshot lists resting orders
  in priority order, and reinsertion restores the exact queue.
- The arena and free list avoid per-order `Box`/`Arc` allocation and make slot
  reuse predictable.
- The state hash makes replay verification a first-class test, not a manual
  inspection.
- The dedup table is business-level. It does not rely on Iggy's bounded
  idempotent-producer window.

Negative:

- The ladder is bounded by `min_price_ticks`/`max_price_ticks`. A symbol whose
  price leaves the configured range is rejected as `PriceOutOfBand`; adding a
  new range requires a config change and, for an existing book, a migration.
- The bitmap scans words linearly from the best end. For a very sparse ladder
  with a distant best price, a tree can be better. The default ladders are dense
  enough that this is not the bottleneck.
- `HashMap` is not allocation-free if the arena capacity is misconfigured. The
  book documents this and treats a full arena as `BookFull`; it does not grow
  silently.
- The dedup table evicts by insertion order. A very old order id can be
  forgotten while still theoretically relevant; the capacity is sized to the
  book and the demo's retention, not to an unbounded audit window.
- Snapshot restore verifies the hash twice (once after construction, once after
  canonical re-derivation), which costs time at startup but catches ordering
  mistakes early.

## Alternatives considered

- **`BTreeMap<Price, Level>` per side.** Simpler for sparse books and unbounded
  ranges, but it allocates per level, has pointer-chasing, and makes best-price
  lookup a tree operation. Rejected for the default path; a per-symbol fallback
  remains a reasonable future option for wide tick ranges.
- **`VecDeque` per price level.** Easy FIFO, but cancel in the middle is O(n) and
  the queue owns order data, which makes snapshots and arena reuse awkward.
  Rejected.
- **`Slab`/generational arena crate.** Close to the chosen design, but it adds a
  dependency and hides the free-list and index types that the snapshot format
  needs to be explicit about. Rejected; the hand-rolled arena is small.
- **`HashMap` for the price ladder.** O(1) average lookup but no ordered best
  price and no deterministic iteration. Rejected.
- **Floats for prices.** Rejected: rounding is a correctness bug in a matching
  engine.
- **Rely on Iggy's idempotent producer for dedup.** Rejected: it deduplicates
  batches within a bounded producer window, not business order ids across
  restarts and replays.

## Status

Accepted for Phase 1. The engine and book APIs are internal to the workspace;
the wire protocol is the public contract. Any change to the ladder
representation, the snapshot format, the state-hash encoding, or the
dedup-eviction policy requires a new ADR and a migration note, because it breaks
replay verification.
