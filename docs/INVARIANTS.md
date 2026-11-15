# Whippet invariants

This file is the catalogue of properties the system relies on, where each one
is enforced, and what remains unproven. It is intentionally blunt: an invariant
without an enforcement site and a test is a wish, not a guarantee.

## 1. Order book (`whippet-engine::book`)

`Book::check_invariants()` is the executable definition:

- Ladder bounds are ordered and `bids.len() == asks.len() == ladder_len`;
  bitmap word counts match the ladder length and no bit past the end is set.
- Every active slot is reachable from exactly one level list, with consistent
  `prev`/`next`, the right side and price index, no cycle, and a terminating
  tail.
- `1 <= remaining <= qty`; `index[order_id]` points back at the slot;
  `order_count == index.len() == active slots`; the free list holds only
  inactive, unique slots and no active slot is free.
- Each level's `orders`/`qty` equal the list count and summed `remaining`.
  An occupied level has its bitmap bit; an empty level has no list and no bit.
- The cached best bid/ask equal the bitmap-derived best or `None`.
- `last_trade_side` is set iff `last_trade_qty > 0`, with a price in the ladder.

Enforced by `tests/invariants.rs` (5,000 random commands, checked after every
command), the golden/property workloads, and focused violation tests. The
check is test-only; the release matching path does not walk the book.

## 2. Dedup and order lifecycle (`whippet-engine::dedup`)

Status is a state machine:

```text
Accepted -> Resting -> Filled | Cancelled
Accepted -> Filled | Cancelled
Rejected is creation-only (never accepted, never transitions)
Filled / Cancelled / Rejected are terminal
```

- `DedupStatus::can_transition` defines the table; `DedupTable::update`
  debug-asserts it and refuses illegal edges in release.
- `insert` only creates `Accepted` or `Rejected`; a duplicate insert restores
  the previous entry and never mutates it.
- `terminal_order` contains every live terminal id exactly once and nothing
  else; `insertion_order` may retain dead ids before compaction but has no
  consecutive duplicate and covers every live id.
- The soft capacity may be exceeded only while every live entry is active;
  eviction never removes a resting order.
- `from_entries` rejects malformed terminal indices, non-terminal targets, and
  duplicate indices; the live state hash/eviction order is preserved.

Cross-structure invariants (`SymbolEngine::check_invariants`):

- every book order has a `Resting` dedup entry, and every `Resting` entry is in
  the book;
- no `Accepted` entry remains after `process` returns;
- terminal ids are never in the book;
- `dedup.len() == resting + terminal`.

The BookFull flow was corrected to store `Cancelled` (the report the client
sees), not `Rejected`.

## 3. Instrument status (`whippet-core::protocol`)

`TradingStatus { session, halted }` is the single definition of the combination:

- `SessionState`: `pre_open`/`open`/`closed`; default `open`.
- `can_accept_new_orders()` is true only for an open, non-halted instrument.
  Cancels are allowed in every combination; halt is checked before session so
  a halted market reports `halted` rather than `session_not_open`.
- Status MD events and snapshots carry the two fields in one flag byte
  (`halted` bit 0, session bits 1–2); `TradingStatus::to_md_flags`/
  `from_md_flags` round-trip all six combinations.
- Session and halt changes are log-ordered `CommandKind` records in the orders
  log, so replay and snapshot restore reproduce them; the state hash covers
  both.

## 4. Shard lifecycle and readiness (`whippet::health`)

Each active shard has two roles (engine, MD), each with:

```text
Starting -> Running -> Stopped
Starting/Running -> Failed        (terminal)
Failed/Stopped are terminal until process restart
```

`ShardHealth` stores one atomic per `(shard, role)`; illegal transitions are
debug-asserted and refused in release, so `Failed -> Running` cannot happen.
Readiness is exactly:

> the node is ready iff the shard latch is open and **every** active shard's
> engine **and** MD role is `Running`.

A fresh context is `Starting`, so it is not ready; a failed or stopped role
keeps the node out of rotation. `/api/health` and `/readyz` expose the per-shard
`starting`/`running`/`failed`/`stopped` states. Tests cover the 4×4 transition
table, a real shard task going `Starting → ready → Stopped → not ready`, and
failure paths that mark only the failing role.

## 5. Snapshot and journal

- The snapshot file is `WHIPSNP1` + version `5` + bincode; load rejects bad
  magic, an old version, the wrong shard id, a hash mismatch, oversized
  lengths, trailing bytes, and a snapshot ahead of the log head.
- The writer only persists a final snapshot after rebuilding and verifying it;
  `last_written` is monotonic; a write is temp-file + fsync + rename.
- The dedup terminal queue and session are part of the state hash; the
  terminal queue is validated on restore.
- The engine consumer commits only after the orders, executions, and MD
  appends land; the commit watermark never moves backwards and is flushed at
  most 256 records / 100 ms behind.
- The reader rejects a gap/overlap in a batch and fails the shard closed.
- The turbo write-behind appends exactly one order record per seq in order;
  seq == broker offset is the startup/proof precondition; admissions are
  released only after the orders append is durable.

## 6. Ingress and configuration

- Credential namespaces are disjoint: a token cannot be both a trading token
  and an operator token (or a metrics token); duplicate trading tokens and
  duplicate operator tokens are rejected at startup. Operator tokens are
  HTTP-only; TCP `Submit` rejects `Halt`/`Resume`/`SetSession`.
- Batches are same-symbol, non-empty, at most 256 orders; the whole batch is
  validated before any append; TCP participant ids must match the token's
  derived id.
- Optional per-order quantity/notional limits are enforced before the append
  (HTTP 422 `risk_limit`, TCP `RiskRejected`); both zero disables them. They
  are an admission guard, not engine-enforced risk.
- Rate limits are charged before validation; a full turbo window rejects with
  `window_full`, never a durable fallback; storage-critical rejects with
  `storage_pressure`.
- Config invalids fail startup: partitions outside 1–256, turbo without Iggy,
  inverted disk thresholds, malformed token pairs, unknown symbols.

## 7. What is not proven

- Engine invariants are checked in tests, not on the release hot path.
- The ingress risk guard is bypassable by a direct log producer; engine-level
  exposure/account state is not implemented.
- Position reconstruction is not settlement: no balances, fees, margin, or
  clearing, and unknown maker legs are reported rather than inferred.
- Retention deletion, TLS, consumer groups, multi-node HA, and native-Linux
  steady state remain future work.
