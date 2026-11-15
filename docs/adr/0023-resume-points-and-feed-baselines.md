# ADR 0023: Resume points and feed baselines

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md) (topics and offsets),
  [ADR 0016](0016-sequenced-price-level-market-data-feed.md) (MD feed),
  [ADR 0021](0021-durable-path-state-machines.md) (commit watermark),
  [INVARIANTS.md](../INVARIANTS.md)

## Context

Two consumers resume from a stored offset and build state from what follows:

- the engine applies the `orders` topic from a snapshot/committed offset;
- the MD hub applies the `md` topic from its committed offset and seeds each
  symbol from the producer's full image.

Both had implicit contracts. The in-memory journal accepted a commit for an
empty partition or an offset past the head, and a backwards commit. The hub
accepted an `Add`/`Reduce`/`Remove`/`Trade` delta before any baseline image and
built a plausible but wrong book. Neither failure is visible in a healthy run;
both surface after a restart as a stall or a wrong feed.

## Decision

### Journal commit contract (both backends)

A commit is the offset of the **last record the consumer fully applied**
(inclusive), so `0 <= committed <= head` and `None` before the first commit.
Commits are monotonic per `(topic, partition, consumer)`.

- The Iggy backend stores what the broker accepts; the engine only commits
  applied offsets.
- The memory backend (the test backend the semantics are measured against)
  enforces the contract: a commit on an unknown/empty partition, an empty
  consumer name, a backwards offset, or an offset ahead of the head **stores
  nothing**. Debug builds panic with the reason; release drops the commit and
  warns. An ahead-of-head claim is never clamped to the head: the caller's
  claim is inconsistent, so treating the head as applied could skip a record.
- Dropping a commit replays more, never less; the engine's dedup and the MD
  per-symbol sequence make replay idempotent.
- `MemoryJournal::check_invariants` proves contiguous offsets, non-decreasing
  timestamps, unique header keys (last wins, like Iggy), and every commit
  bounded by its log head. Header keys fold last-wins, matching Iggy's map.

`lag = head - committed` is the same identity on both backends; `lag(…, 0)` is
the head. An empty append reports `base_offset == log.len()` and `count == 0`;
the Iggy backend cannot report a meaningful base offset for a zero-message
send and returns 0, which no caller reads because `count == 0`.

### Market-data baseline phase

```text
AwaitingSnapshot --image seq >= book.seq--> Seeded
Seeded           --image seq >  book.seq--> Seeded   (replaces)
any              --image seq <  book.seq--> unchanged (stale)
AwaitingSnapshot --Status-----------------> AwaitingSnapshot (applies)
AwaitingSnapshot --Add/Reduce/Remove/Trade/Bbo--> unchanged (refused, counted)
```

`BookPhase` is monotonic. A book-state delta without a baseline is ignored,
`events_without_baseline` increments, and **seq does not advance**: seq advances
iff the event applied. That keeps `hub.sequence == max(book.seq)` and prevents
an ignored event from making every later event look stale. A status event is
allowed before the image (it carries no book state). An image at the current
seq is accepted while awaiting, because a replayed status may have advanced the
sequence without depth.

This matters after a crash: the MD consumer can replay up to 256 records /
100 ms of deltas into a fresh hub before the engine appends its new image at
the head. Dropping those deltas is lossless; the new image's seq covers them. A
fresh log always starts with the image, so no legitimate first event is lost.

### Book and feed invariants

`BookState::check_invariants` requires positive level quantities and order
counts, tick-grid prices, best bid < best ask, depth totals equal to level sums,
and (for a complete order snapshot) each level equal to the refs posted there.
`MdHub::check_invariants` requires configured symbols/ticks, every book valid,
`state.sequence == max(book.seq)`, a bounded tape with per-symbol increasing
seq, and `feed_sent == 0` when no subscriber ever attached. A reduce to zero is
a cancel; an add overflow is refused whole rather than saturating away from the
sum of its refs.

## Alternatives rejected

- **Clamp an ahead-of-head commit to the head:** claims an unapplied record and
  can skip it on restart. Ignoring is conservative.
- **Accept deltas before the image and hope they are complete:** depth built
  from deltas alone is wrong by construction, and the error persists.
- **Advance seq on a refused event:** hides the refusal and makes later
  legitimate events look stale.
- **Store feed counters per symbol:** the hub keeps one bounded broadcast
  buffer; per-symbol queues would multiply memory by subscribers.

## Consequences

- A restart may replay more records than before; that is the safe direction and
  is already idempotent.
- A hub that never receives an image stays empty and exposes the count instead
  of showing a false book. The producer's initial image append remains
  warn-only; that counter is the alarm to add to health/metrics.
- The feed's sequence is now exactly the applied sequence; a rejected event is
  not a hole.
