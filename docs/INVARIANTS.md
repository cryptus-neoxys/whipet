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

Execution output (`EngineOutput::check_invariants` and
`SymbolEngine::check_output_invariants`, test-only):

- execution and MD seqs are nonzero and strictly increasing in their own
  spaces, and the last seq is one below the engine's next counter;
- one output is one command: every event carries that command's `cause_offset`
  (MD events carry the venue timestamp), symbols match;
- one `Accepted` per order id before any terminal; a terminal is last and only
  one is emitted; a rejection is the sole report, has no MD, zero qty,
  zero maker id, and a reason;
- every fill is positive, taker-side, `FLAG_TAKER`, has a nonzero maker id, and
  has exactly one `Trade` MD event (same offset/price/qty/side) and one
  maker-side `Reduce`/`Remove`;
- accepted quantity is conserved: fills plus an `Add` remainder, or one
  terminal cancel remainder, equal the accepted qty;
- role flags are exclusive; `Accepted` is TAKER or RESTED; an STP maker cancel
  is `FLAG_MAKER`; a client cancel reports the removed order's side/price.

It found and fixed two bugs: a cancel report used the fixed-layout command side
instead of the removed order's side, and a post-acceptance cancel hardcoded
`FLAG_TAKER` for a passively admitted order. See ADR 0024.

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

## 3.1 Engine-enforced risk (`whippet-engine::risk`)

Per-participant limits are engine state, not an edge filter:

- `SetRisk` is a `CommandKind` record on the same ordered orders log as client
  traffic. The engine applies it at the command's log position, so replay and
  snapshot restore reach the same limits and exposure. `RiskTable::set_limits`
  preserves the participant's existing exposure; zero disables a bound.
- `RiskLimits { max_net_qty, max_open_buy_notional_cents }` bounds the
  worst-case position (`net + open buy` for a buy candidate, `net - open sell`
  for a sell candidate, charged the candidate's full quantity) and the quote
  cents in resting buys (charged the candidate's full quantity even if it
  would cross). Only `Limit`/`PostOnly` buys are charged notional.
- Admission runs after the order's own validity/fillability checks and before
  the dedup id is accepted: a refused order produces one `risk_limit`
  rejection and no book, MD, dedup, or exposure mutation.
- Exposure moves only with the book and fills: a rest adds open quantity and
  buy notional, a cancel/STP removal subtracts them, a taker or maker fill
  moves `net_qty` (the maker's resting quantity leaves with the fill), and an
  opposite fill that nets to zero prunes a flat unrestricted entry.
- `RiskTable::check_invariants` requires strictly ordered participant entries
  and no empty entry; `check_against_orders` re-derives every participant's
  open quantity and buy notional from the book at the instrument's tick size
  and requires an exact match in both directions. Both run in
  `SymbolEngine::check_invariants` (test/diagnostic) and on snapshot restore
  (cold path).
- Risk limits and exposure are in the `whippet-engine-v2` state-hash domain
  and in snapshot format v6, so a dropped `SetRisk` or a stale exposure cannot
  hash or restore as a valid engine.
- The operator API is `POST /api/admin/instruments/{symbol}/risk` with
  `participant`, `max_net_qty`, and `max_open_buy_notional_cents` (both bounds
  required, zero disables). It is operator-only, appended before the audit
  record, and the audit names the participant and both bounds.

The static ingress guard (`whippet::config::RiskConfig`) remains a separate,
stateless per-order filter. Engine risk is the authority for a participant's
accumulated position and open interest; the ingress guard is not.

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

- The snapshot file is `WHIPSNP1` + version `6` + bincode; load rejects bad
  magic, an old version, the wrong shard id, a hash mismatch, oversized
  lengths, trailing bytes, and a snapshot ahead of the log head.
- Before any file exists, `ShardSnapshot::check_invariants` requires: a
  non-empty, strictly sorted symbol list; `book.symbol == symbol.symbol`; both
  sequences at least 1; a zero-command symbol has offset 0 and no dedup; every
  dedup offset/timestamp within `last_offset`/`last_ts_ns`; terminal indices in
  range, terminal, unique, and covering every terminal entry; at least one
  symbol with `commands > 0` (no resume point otherwise); and a non-zero
  `state_hash` on every symbol. The check runs in `save` before the temp file
  is opened, so a bad snapshot cannot exist on disk even transiently.
- Both the verified (`write`) and periodic (`write_unverified`) paths require
  filled hashes; `fill_hashes` is the only way to leave the unhashed phase, and
  a supplied hash that disagrees with the recomputation is rejected rather
  than repaired. The verified path additionally rebuilds the shard and compares
  the state hash before the rename.
- `last_written` advances only after a successful rename. A sequence at or
  below `last_written` is a no-op with no filesystem work; a failed newer write
  leaves the watermark and the previous file untouched. A failed write, flush,
  sync, or rename removes the temp file.
- A commandless snapshot written by an older node or by hand is still rejected
  by the reader (`--require-snapshot` fails closed), even though the current
  writer refuses to produce one.
- The dedup terminal queue, session, and per-symbol risk table are part of the
  state hash; the terminal queue and the risk table are validated on restore,
  and risk open exposure is re-derived from the book at the instrument's tick
  size.
- The engine consumer commits only after the orders, executions, and MD
  appends land, and only for a batch that actually consumed an input record:
  the empty shutdown batch carries `applied = false`, so it cannot claim
  offset 0 of an empty log as durable. The same rule gates cadence snapshots.
  `CommitWatermark` enforces `committed <= applied`, neither offset moves
  backwards, a partial commit leaves the range and its onset pending, and the
  delay bound is measured from the first uncommitted record (256 records /
  100 ms). `CommitRetry` retries with 100 ms–2 s backoff and a 30-attempt
  diagnostic budget; it never stops retrying, so a broker outage costs commit
  lag, not availability. See ADR 0021.
- The journal offset contract is explicit on both backends: offsets are
  contiguous from zero per `(topic, partition)`, timestamps are non-decreasing,
  and a commit is the inclusive last-applied offset. Commits are monotonic and
  bounded by the head; a commit on an empty/unknown partition, a backwards
  commit, or an ahead-of-head claim stores nothing (debug: panic with the
  reason; release: drop/warn). An ahead-of-head claim is never clamped to the
  head, because the caller may not have applied that record. Rejecting stores
  replays more, never less. `MemoryJournal::check_invariants` verifies the log
  and every commit; header keys fold last-wins like Iggy. See ADR 0023.
- The reader rejects a gap/overlap in a batch and fails the shard closed.
- The turbo write-behind appends exactly one order record per seq in order;
  seq == broker offset is the startup/proof precondition; admissions are
  released only after the orders append is durable. An admission is
  `Queued -> Completed` (durable append) or `Queued -> Abandoned` (drop), and
  exactly-once release is structural through its owned permit. The window
  invariant is `in_flight == queued.len()`,
  `available permits + in_flight == capacity`, strictly increasing unique ids,
  non-decreasing timestamps, and `drops == count_drops + age_drops`.
  `AdmitError::{CountFull, AgeFull}` is carried through
  `EnqueueError::WindowFull(_)`; the HTTP/TCP wire answer stays `window_full`
  and there is no durable fallback.

## 6. Ingress and configuration

- Credential namespaces are disjoint: a token cannot be both a trading token
  and an operator token (or a metrics token); duplicate trading tokens and
  duplicate operator tokens are rejected at startup. Operator tokens are
  HTTP-only; TCP `Submit` rejects `Halt`/`Resume`/`SetSession`/`SetRisk`.
- Batches are same-symbol, non-empty, at most 256 orders; the whole batch is
  validated before any append; TCP participant ids must match the token's
  derived id.
- Optional per-order quantity/notional limits are enforced before the append
  (HTTP 422 `risk_limit`, TCP `RiskRejected`); both zero disables them. They
  are a stateless admission guard; accumulated per-participant exposure is
  enforced by the engine (section 3.1).
- Operator risk changes are HTTP-only, operator-token-only, and log-ordered
  like halt/session; TCP `Submit` rejects `SetRisk`.
- Rate limits are charged before validation; a full turbo window rejects with
  `window_full`, never a durable fallback; storage-critical rejects with
  `storage_pressure`.
- Config invalids fail startup: partitions outside 1–256, turbo without Iggy,
  inverted disk thresholds, malformed token pairs, unknown symbols.
- Wire decoders are total over untrusted bytes: every input yields a typed
  error or a canonical value, a successful decode re-encodes byte-identically
  (the legacy snapshot form without the trailing status byte is the only
  documented exception), and arbitrary/truncated/trailing/oversized input
  never panics or reads out of bounds. TCP frames are exact-length (no trailing
  bytes), booleans are 0/1, count prefixes are checked against the remaining
  bytes before reserving, and string truncation lands on a UTF-8 boundary.
  Proptest round-trip, mutation, and arbitrary-byte suites cover every wire
  type (~7,400 generated cases per run plus exhaustive version/reason sweeps).
  The price-unit rule is ADR 0025; the decoder properties live in
  `crates/whippet-core/tests/decoders.rs`.

## 8. Market-data hub and feed

- Each symbol has a monotonic `BookPhase`:
  `AwaitingSnapshot -> Seeded`. A book-state delta (`Add`/`Reduce`/`Remove`/
  `Trade`/`Bbo`) before a baseline image is refused, counted by
  `events_without_baseline`, and does not advance `seq`; `seq` advances iff the
  event applied. `Status` may apply before the image (it carries no depth).
  A snapshot at the current seq is accepted while awaiting (a replayed status
  may have advanced seq without depth); a stale snapshot is refused.
- Starting the MD consumer at `committed + 1` can replay up to 256 records /
  100 ms of deltas into a fresh hub before the producer's new image reaches the
  head. Dropping them is lossless: the new image's seq covers them; a fresh
  log always starts with the image.
- `BookState::check_invariants` requires positive level quantities and order
  counts, tick-grid prices, best bid < best ask, depth totals equal to level
  sums, and, for a complete order snapshot, level aggregates equal to the refs
  posted at that price. Reduce-to-zero is a cancel (the ref and empty level
  leave); an add overflow is refused whole, not saturated.
- `MdHub::check_invariants` requires configured symbols/ticks, every book
  valid, `state.sequence == max(book.seq)`, a bounded tape whose per-symbol
  seq strictly increases (the tape is global; seq spaces are per symbol), and
  `feed_sent == 0` when no subscriber ever attached. Every accepted event is
  one feed frame; a refused event is not a hole because seq did not advance.
- `check_invariants` is test-only; the gate and counter are the production
  behavior. The refused-delta count is exposed as
  `whippet_md_events_without_baseline_total` on `/metrics`.

## 9. Double-entry ledger projection

`whippet ledger` folds the decoded `orders` and `executions` logs into accounts;
it is a projection, not settlement. For every attributed fill report:

- identity is the engine `seq`; `cause_offset` links it to the input command;
- exactly four postings: buyer cash `-price_cents*qty`, seller cash
  `+price_cents*qty`, buyer position `+qty`, seller position `-qty`, where
  `price_cents = price_ticks * tick_size_cents` (the log stores tick indices,
  not cents; a missing tick size is an error, never a factor of one);
- attribution is all-or-nothing: an unknown aggressor or maker (including
  `maker_order_id == 0`) posts neither leg and increments the matching
  unresolved counter;
- postings sum to zero per entry per asset; report-wide cash and each symbol's
  position sum to zero;
- entries are ordered by `seq`; a repeated seq keeps a canonical first entry,
  so input order cannot change the result;
- every account balance/count equals a checked direct fold of the postings.
  `ledger_with_journal` refuses to print a report that fails.

Over complete trades, ledger cash/position balances equal `positions`
`cash_cents`/`net_qty`. The model is T+0 simulation: no fees, funding,
deposits, withdrawals, margin, or clearing. See ADR 0022.

## 10. What is not proven

- Engine invariants are checked in tests, not on the release hot path.
- Snapshot writer invariants are structural and verified by a rebuild on the
  final path, but a periodic (`write_unverified`) snapshot is not rebuilt
  before its rename; its hashes are filled from the same state and the next
  verified snapshot or replay is the cross-check.
- Commit state is in-process: a broker outage can extend commit lag up to the
  retry cadence, and only a restart proves what was acknowledged.
- The ingress risk guard is bypassable by a direct log producer. Engine risk
  closes that gap for the participants an operator has configured, but it
  tracks only net quantity and resting buy notional: there is no cash,
  margin, fees, funding, or cross-symbol aggregation.
- Position reconstruction and the ledger are projections, not settlement: no
  fees, funding, margin, or clearing; unknown legs are reported rather than
  inferred, and the ledger excludes a whole trade with an unresolved leg.
- The MD baseline counter is not yet wired to `/api/health` or `/metrics`; a
  failed initial image append leaves the hub empty and counted, and matching
  is unaffected, but an operator must read the accessor.
- The Iggy journal backend cannot locally validate a commit against its head;
  the memory backend is the enforced contract and the engine only commits
  applied offsets.
- Retention deletion, TLS, consumer groups, multi-node HA, and native-Linux
  steady state remain future work.
