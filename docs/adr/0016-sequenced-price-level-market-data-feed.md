# ADR 0016: Sequenced price-level market-data feed

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0003](0003-matching-engine-data-structures.md) (book and MD events),
  [ADR 0009](0009-performance-budget-and-optimization-order.md) (hot-path discipline),
  [ADR 0010](0010-shard-thread-and-journal-pipeline.md) (shard pipeline),
  [ROADMAP §4.13](../ROADMAP-AND-IMPROVEMENT-PLAN.md),
  [ARCHITECTURE.md](../ARCHITECTURE.md), [OPERATIONS.md](../OPERATIONS.md)

## Context

The dashboard consumes a conflated full-state `StateView` over `GET /ws` at
20 Hz. That is the right contract for a browser: a slow client can miss
intermediate frames and the next frame is a complete state. It is the wrong
contract for an external consumer that needs every change, a per-symbol
sequence it can audit, and a defined resynchronization path.

The MD hub already observes every applied `MdEvent` in the MD log with a
per-symbol `seq`. The missing piece was a fan-out that preserves those events
without making the hub's apply path pay for subscribers that do not exist.

## Decision

Add a second, additive WebSocket endpoint:

```text
GET /ws/md?symbol=BTC-USD
```

### Wire contract

The first frame is a `snapshot`; every subsequent frame is a `delta`; a
`resync` notice precedes a replacement snapshot.

```json
{"type":"snapshot","symbol":"BTC-USD","seq":42,"cause_offset":1234,
 "bids":[{"price_cents":6500000,"qty":10,"orders":1}],
 "asks":[...],"halted":false}
{"type":"delta","symbol":"BTC-USD","seq":43,"cause_offset":1235,
 "timestamp_ns":...,"kind":"add","side":"buy","flags":0,
 "price_cents":6500000,"qty":10,"order_id":"...","participant":"..."}
{"type":"resync","symbol":"BTC-USD","seq":60,"reason":"lagged 5"}
```

- **Full-depth price-level snapshot.** The snapshot is the hub's complete bid
  and ask level maps at `seq`, not the dashboard's 20-level window. A consumer
  that starts from a truncated image could never recover the deeper levels.
- **Every accepted event is a delta.** Adds, reduces, removes, trades, BBO,
  Status, and engine snapshots all advance the per-symbol `seq` and are
  forwarded; skipping a kind would look like a lost frame. Duplicates and
  stale events rejected by the sequence guard are not forwarded.
- **Resync, never a stall.** The hub fans out through a bounded broadcast
  capacity (`MD_FEED_CAPACITY`, 1024). When a subscriber lags, it receives a
  `resync` naming the new `seq` and then a fresh snapshot; delivery continues
  from there. The hub never blocks on a subscriber and never silently drops a
  frame in the middle of a sequence. A client that sees a gap it cannot
  explain should reconnect and take a new snapshot.
- **Admission** shares the global `/ws` subscriber cap (one permit from the
  same semaphore). The per-IP sub-limit remains a `/ws` concern.

### Cost on the hot path

`MdHub::apply_batch` checks `feed.receiver_count()` once after releasing the
state lock. With no subscriber it allocates nothing, serializes nothing, and
does not touch the feed. With a subscriber, each accepted event is serialized
once into an `Arc<str>` and broadcast; N subscribers cost N refcount bumps and
N socket writes, not N serializations. Measured on the dev profile: ~7.7M
events/s with no subscriber (~129 ns/event), ~1.6M events/s with one
subscriber (~636 ns/event including JSON and broadcast).

### Metrics

`whippet_md_feed_subscribers`, `whippet_md_feed_sent_total`,
`whippet_md_feed_lagged_total`, `whippet_md_feed_resyncs_total`.

## Alternatives rejected

- **Reuse `/ws` with a "raw" flag:** conflation is the dashboard's contract;
  mixing modes makes both worse and still lacks a per-symbol sequence.
- **Per-subscriber queues in the hub:** unbounded memory and a slow client can
  stall or OOM the node.
- **Replay from the MD log per connection:** correct but heavy; the hub
  already has the state and the live events.
- **Order-by-order snapshot:** the hub stores price levels, not the full order
  map. A price-level feed that says so is honest; an order-level feed needs a
  new hub state and is a separate feature.

## Consequences

- The feed is **price-level (market-by-price)**, not order-by-order. Deltas
  carry order ids for diagnostics, but the snapshot is levels.
- A lagged consumer's resync loses the intermediate deltas by design; that is
  the documented slow-consumer policy, and `whippet_md_feed_resyncs_total`
  makes it visible.
- The feed's snapshot size is proportional to the number of occupied levels,
  which is bounded by the instrument ladder but can be large; it is a
  handshake, not a per-event cost.
- `/ws` remains unchanged for the dashboard; the two feeds have separate
  contracts and separate metrics.
- Authenticated feed access is not part of this ADR; `/ws/md` inherits the
  node's current WebSocket exposure, and an operator can put it behind the same
  proxy/token boundary as `/ws`.
