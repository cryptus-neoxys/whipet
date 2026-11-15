# Architecture

This document describes the Phase 1 durable slice as it is built, not the
target architecture. Where the implementation is deliberately smaller than the
research blueprint, it says so.

## Shape of the system

One process hosts the gateway, the engine tasks, the market-data consumers, and
the HTTP/WebSocket API. One Iggy broker holds the log. The dashboard is a
static bundle served by the same process.

```text
                       ┌───────────────────────────────┐
   HTTP POST /api/orders│ gateway                       │
   ────────────────────►│ validate, derive ids, append  │
                       └───────────────┬───────────────┘
                                       │
                     whippet-trading/orders (16 partitions)
                                       │
              ┌────────────────────────┴────────────────────────┐
              │ engine shard for partition 1                     │
              │ engine shard for partition 5                      │
              │ engine shard for partition 12                     │
              │ (one logical writer per partition)               │
              └───────┬──────────────────────────────┬──────────┘
                      │                              │
        whippet-trading/executions           whippet-md/md
                      │                              │
                      │                    ┌─────────┴─────────┐
                      │                    │ MD consumer tasks │
                      │                    │ per partition     │
                      │                    └─────────┬─────────┘
                      │                              │
                      │                    ┌─────────▼─────────┐
                      │                    │ MdHub cache       │
                      │                    │ 20 Hz state frame │
                      │                    └─────────┬─────────┘
                      │                              │
                      ▼                              ▼
                future consumers              /api/state, /ws
```

The gateway does not wait for an execution. It returns `accepted_by_log` once
the order record is confirmed by Iggy. The engine consumes the record later,
matches it, and publishes executions and market data. That is why the order
acknowledgement and the fill are two separate observations.

## Crates

| Crate | Contents | Hot path? |
|---|---|---|
| `whippet-core` | `Symbol`, `OrderId`, `Price`, `Qty`, `Seq`; fixed-layout protocol codecs; JSON DTOs for HTTP/WS; message headers; `Mode` and instrument config; benchmark manifest types. | Types only |
| `whippet-engine` | `Book` (array ladder, occupancy bitmap, intrusive FIFO over a preallocated arena), `DedupTable`, `SymbolEngine`, `ShardEngine`, snapshots and state hashes. | Yes |
| `whippet-journal` | `IggyJournal` (the only async Iggy client code), topic layout, `Journal` enum, in-memory journal for tests. | Edge |
| `whippet-marketdata` | `MdHub`, per-symbol `BookState`, 250-entry trade tape, `Metrics`, 20 Hz publisher. | Warm |
| `whippet-sim` | Seeded `Simulator` and four scenarios. | Offline |
| `whippet-bench` | Open-loop pacer, HDR histograms, manifest collection and writing. | Offline |
| `whippet-testkit` | Deterministic command builders, golden log, and proptest strategies for engine tests. | Tests only |
| `whippet` | Binary and library: `node`, `sim`, `bench`, `healthcheck`, `verify`, `replay`, `doctor`; gateway routes; engine and MD runners; snapshot store. | Yes |

The engine crate has no dependency on Tokio, Iggy, Axum, or the wall clock. The
journal crate is the only place that calls Iggy.

## Protocol

Every payload starts with a one-byte wire version (`1`) and a kind
discriminant. Layouts are fixed-width little-endian with no varints, no floats,
and no implicit padding.

| Payload | Encoded size | Notes |
|---|---|---|
| `Command` (new order or cancel) | 78 bytes | Symbol is a 16-byte NUL-padded ASCII field; order id is 128 bits; price is `i64` ticks; quantity is `u64` units. |
| `ExecutionReport` | 104 bytes | Kind, symbol, side, type, price, qty, remaining, order id, maker order id, per-symbol seq, `cause_offset`, timestamp, reject reason, flags. |
| `MdEvent` | 84 bytes | Add, Reduce, Remove, Trade, BBO, Status; per-symbol seq and `cause_offset`. |
| `MdSnapshot` | 66 bytes + 20 bytes per level + 37 bytes per resting order | Full resting-order set for recovery, plus display top-N levels and last trade. |

Prices are integer ticks everywhere. The instrument maps ticks to cents:
BTC-USD uses 100-cent ticks on a 40,000-90,000 tick ladder, ETH-USD uses
10-cent ticks, SOL-USD uses 1-cent ticks. The browser formats cents; the engine
never sees a float.

## Transport

There are two ingress transports with the same durable semantics:

| Transport | Port | Use | Cost per order |
|---|---|---|---|
| HTTP/JSON (`POST /api/orders`, `/api/orders/batch`) | `8080` | Dashboard, simulator, curl, control plane | JSON parse + one Iggy append per request or batch |
| Binary TCP | `8081` (`WHIPPET_TCP_ADDR`) | Throughput clients, benchmark | Fixed 78-byte command frames, one Iggy append per batch |

The TCP frame is `[u32 body_len][u8 kind][body]`. `Submit` carries up to 256
fixed-layout `Command` payloads of one symbol; `Hello` carries an optional
bearer token when the node requires one; `Ping` measures liveness. A `Submit`
is answered with one ack carrying `partition`, `base_offset`, and `count`; order
`i` has offset `base_offset + i` because the whole batch is one append. The
wire types live in `crates/whippet-core/src/tcp.rs`.

Both transports call the same encoding and append path, so the engine cannot
tell how an order arrived. TCP does not change durability: the ack is sent
after the Iggy append confirms. TLS is not terminated in-process in Phase 1;
put a proxy in front of the TCP port if the network is untrusted.

### Group commit

A single-order client still sends one command per request. The gateway and the
TCP listener therefore run a per-partition **group commit**: concurrent
submissions for the same `(topic, partition)` are queued to one writer task,
drained into one Iggy append of up to 256 messages, and answered with
`base_offset + i` per submission. The writer drains whatever is already queued
when it wakes and does not add a fixed linger, so a lone order at a low rate is
flushed immediately while a burst is coalesced by arrival concurrency.

Ordering is preserved because there is exactly one writer per partition. A
failed append is retried as a whole; the engine deduplicates order ids, so an
at-least-once retry cannot double-apply a command. `/metrics` exposes
`whippet_group_commit_batches_total`, `..._messages_total`, `..._max_batch`, and
`..._partitions`.

## Topics

Three streams separate trust domains. The layout is pinned in
`crates/whippet-journal/src/topic.rs`; a running deployment does not get to
choose different counts by accident.

| Stream | Topic | Partitions | Key | Durability | Retention | Phase 1 use |
|---|---|---|---|---|---|---|
| `whippet-trading` | `orders` | 16 | symbol | persisted | 24 h | Gateway produces; engine consumes. |
| `whippet-trading` | `executions` | 16 | symbol | persisted | 7 d | Engine produces; no consumer yet. |
| `whippet-md` | `md` | 16 | symbol | replicated | 1 h | Engine produces; MD hub consumes. |
| `whippet-ops` | `alerts` | 4 | symbol | persisted | 30 d | Created, unused. |
| `whippet-ops` | `audit` | 4 | service | persisted | 90 d | Created, unused. |
| `whippet-ops` | `commands` | 1 | admin | persisted | 7 d | Created, unused. |

Topic creation also sets 64 MiB segments, an 8 GiB maximum for the three hot
topics (2 GiB for the ops topics), a 1024-message / 1 MiB flush threshold, and
consumer-offset durability matching the topic durability. Existing topics are
left alone on restart. Partition count is therefore a create-time contract:
changing `WHIPPET_PARTITIONS` after the topics exist has no effect until the
topics and their data are recreated.

"Replicated" on a single node records the requested policy; there is no second
node to replicate to. It is not an HA guarantee in this build.

## Partition keying

`Symbol::partition` is FNV-1a over the symbol bytes modulo the partition count.
The gateway, engine, and simulator use the same function. The replay CLI takes
the partition explicitly. The append uses `Partitioning::partition_id`, not a
server-side hash, so the client and server cannot disagree about the target.

With 16 partitions:

| Symbol | Partition |
|---|---|
| BTC-USD | 1 |
| ETH-USD | 5 |
| SOL-USD | 12 |

Only partitions that own a configured symbol get an engine shard: one matcher
thread plus its reader and writer tasks. The node starts three engine shards
and three MD consumer tasks, not sixteen. The other
partitions exist for future scale-out. If two configured symbols hash to the
same partition, one engine owns both books; that is supported, but the current
three-symbol set does not collide.

The partition is fixed per symbol. A symbol cannot be split across partitions,
and there is no rebalancing.

## Matching engine

`SymbolEngine::process` is a synchronous function:

1. Update `last_offset` and `last_ts_ns`.
2. Dispatch to cancel or new order.
3. Validate: symbol, halt state, quantity, price, ladder bounds, duplicate id,
   post-only crossing, FOK fillability.
4. Record the order in the dedup table.
5. Match against the opposite side in price-time priority.
6. Emit executions and MD events into the caller's reused `EngineOutput`.
7. If the command count hits the snapshot interval, emit an MD snapshot.

The book is an array of levels indexed by tick, one occupancy bitmap per side,
and an intrusive doubly linked FIFO per level over a preallocated slot arena.
Best price is a bitmap scan; matching walks levels and FIFO order. The order
index is a `HashMap<OrderId, u32>` built with capacity up front.

No `await`, no I/O, no wall-clock read, and no random number generation happen
inside `process`. Timestamps come from the command. Event order is the input
order. The only clock in the trading path is the gateway's timestamp when it
builds the command, and it is informational.

Implemented order types: limit (GTC), market with a 500 bps collar, IOC, FOK,
and post-only. Cancel removes a resting order. Self-trade prevention cancels
the resting order and emits a maker cancellation; the aggressor keeps its
remaining quantity. There are no stop, iceberg, pegged, trailing, auction, or
multi-leg orders. A `halted` flag exists in the engine state, but no API or log
command sets it in Phase 1.

## Single writer, not a pinned thread

Each active partition has exactly one matcher thread, and that thread is the
only code that touches the partition's books, so there is no lock and no
concurrent writer. The matcher runs on a dedicated OS thread
(`whippet-shard-<id>`), not a Tokio task. It is still **not pinned to a core**,
and the node still does not busy-poll; core pinning, `SYS_NICE`, and NUMA work
remain Phase-2/Linux work. The single-writer property now comes from thread
ownership plus bounded channels, not from core affinity.

The async boundary has moved out of the match path entirely: an async reader
task polls the orders log and feeds the matcher through a bounded input ring,
and an async writer task drains the matcher's bounded output ring, appends
executions and MD, and commits the offset. The matcher never awaits and never
calls the journal. Details in [ADR 0010](adr/0010-shard-thread-and-journal-pipeline.md).

### Shard thread and rings

[ADR 0010](adr/0010-shard-thread-and-journal-pipeline.md) is implemented: each
active shard is a three-stage pipeline on a dedicated OS thread.

```text
read_orders (async task, orders client)
        │  bounded input ring (reader → matcher, capacity 2)
        ▼
whippet-shard-<id> (dedicated OS thread; owns ShardEngine; no I/O, no await)
        │  bounded output ring (matcher → writer, capacity 2)
        ▼
run_writer (async task; appends executions/MD, commits, forwards snapshots)
```

The design rules:

- **One dedicated OS thread per active shard.** Three active partitions mean
  three matcher threads. The thread owns the engine; it never awaits and never
  calls the journal. Timestamps come from the command, as before.
- **Recycle channel.** The matcher hands each polled buffer back to the
  reader, which refills it with `read_payloads_into` instead of allocating a
  `Vec` per batch.
- **The reader validates the whole batch.** The first offset must equal the
  requested offset and the offset span must equal the record count; a gap or
  an overlap marks the shard failed instead of silently skipping an order.
- **The writer owns the async journal edge.** It joins the Executions and MD
  appends, commits the consumer offset, and forwards MD snapshots. An output
  append is retried up to 30 times with backoff; on exhaustion the shard is
  marked failed and the applied offset and commit are not advanced, so a
  restart replays the tail instead of skipping orders whose outputs were
  never published.
- **Snapshot gate.** An `appended_offset` atomic records the highest offset
  whose outputs are in the journal. A periodic snapshot is taken only once
  the gate is open for the matcher's current offset; a due snapshot that
  would run ahead of the writer is deferred to the next batch boundary where
  the gate is open. The snapshot never claims a resume offset whose outputs
  were lost.
- **Live hash.** The matcher calls `snapshot_state()`, which computes the
  engine's own `state_hash`; the writer saves those hashes and, for the final
  snapshot, rebuilds the shard to verify them before the atomic rename. An
  off-loop hash was tried and reverted: at ~170k orders it disagreed with the
  restored engine on one shard, so the node does not use it. The snapshot file
  format is version 2 (`SymbolSnapshot` carries the per-symbol config,
  `BookSnapshot` carries `min_price_ticks`); version 1 files are rejected.
- **Per-shard metrics.** Each matcher records into its own lock-free
  `ShardMetrics`, merged into the global `Metrics` once per second and once
  on exit, instead of taking a global histogram lock per command.

Backpressure, failure, and shutdown:

- **Input ring full:** the reader's `send` awaits until the matcher drains.
  Nothing is dropped; log order is preserved.
- **Output ring full:** the matcher blocks in `blocking_send` until the writer
  drains space. Outputs are never dropped — a dropped execution or MD event
  would break the at-least-once contract and `md_seq` continuity.
- **Matcher panic:** the thread join detects the dead thread, the shard is
  marked failed, and the shard errors out. A panicked matcher never leaves the
  node hanging on a ring that will not drain.
- **Graceful shutdown:** the reader exits on the shutdown watch and drops its
  sender, closing the matcher input; the matcher sends its final batch with
  the final snapshot; the writer appends and commits the remaining outputs and
  persists the final snapshot inside the shutdown budget. The bounded rings
  make the drain finite.

## Market data

The engine emits MBO-style deltas per symbol:

- `Add`: a new resting order at a price level.
- `Reduce`: the remaining quantity of a resting order changed.
- `Remove`: the order left the book.
- `Trade`: a fill, with the aggressor's side and the taker order id.
- `Bbo`, `Status`: defined in the protocol, not emitted by the current engine.
- `Snapshot`: the full resting-order set plus top-20 display levels per side and the last trade. Periodic MD snapshots are coarse (every 1,000,000 commands); the hub normally rebuilds from deltas, and a startup snapshot seeds a fresh process.

The gateway also exposes `POST /api/orders/batch` for up to 256 orders of one
symbol. The batch is validated as a whole and written with one Iggy append;
`?acks=none` returns only the batch summary. This is the throughput path. The
single-order path remains the latency path: one request, one append, one ack.

The MD consumer applies records to a `BTreeMap`-backed cache per symbol. A
snapshot replaces the levels; a delta mutates them. The cache keeps 20 levels
per side for the view and a 250-trade tape. The hub publishes one full
`StateView` over a `watch` channel at 20 Hz. Browsers never receive raw deltas
in Phase 1, so a slow browser cannot slow the engine. It can miss intermediate
frames; the next frame is a full state.

`md_seq` is monotonic per symbol. `cause_offset` is the input offset that
produced the event. The state view's `sequence` is the maximum `md_seq`
observed across symbols; because each symbol has its own sequence space, it is
a display value, not a global order.

## Observability

The node exposes `GET /metrics` in Prometheus text format. Counters cover
orders, executions, trades, MD records, and engine commands. Gauges cover
ingress and match latency quantiles, throughput, Iggy connectivity, partition
count, mode, MD freshness, and MD sequence. Uptime is exposed as a monotonic
counter. The values come from the same process-lifetime HDR histograms as
`/api/state`, so the latency quantiles are cumulative, not windowed.
`whippet_consumer_lag` is a placeholder set to zero after each MD batch; it is
not a computed lag. The full series list is in
[OPERATIONS.md](OPERATIONS.md#prometheus-metrics).

## Recovery

The engine state per symbol is: the book, the dedup table, `exec_seq`,
`md_seq`, `last_offset`, `last_ts_ns`, the command count, the halt flag, and a
BLAKE3 state hash.

A shard snapshot contains all symbol states for one partition and is written
to `data/snapshots/partition-<N>.snapshot`:

- Magic `WHIPSNP1`, format version `2`, then a bincode payload. Version 1
  files are rejected: `SymbolSnapshot` now carries the per-symbol config and
  `BookSnapshot` carries `min_price_ticks`, so the state hash can be
  reproduced from the snapshot alone. An existing deployment must rebuild
  snapshots (replay, or a graceful-shutdown final snapshot) before upgrading.
- Written to a temporary file, `fsync`ed, then renamed over the previous
  snapshot. A reader never sees a partial file.
- Triggered when a symbol reaches `WHIPPET_SNAPSHOT_EVERY` commands (default
  10,000) and after the batch that crossed the interval commits — and only
  once every output up to that offset is in the journal
  (`appended_offset` gate); a snapshot that comes due while the writer is
  behind is deferred to the next safe batch boundary. A graceful shutdown
  also writes a final snapshot.

On startup, for each active shard:

1. Load `partition-<N>.snapshot` if it exists.
2. Restore every symbol engine and recompute its state hash. A mismatch aborts
   that shard with an error; the node does not silently use a corrupt
   snapshot.
3. Set the next input offset to the maximum symbol `last_offset` in the
   snapshot plus one.
4. If there is no snapshot (or the snapshot has no applied commands), start at
   offset zero and replay the whole log. The broker's committed offset seeds
   the commit watermark, not the resume point: starting after it with an empty
   engine would skip every earlier order.
5. Replay from that offset. The engine applies commands in log order.

The dedup table is snapshotted with the book and the input offset. It is a
business-level table keyed by the derived `OrderId`, not Iggy's producer
idempotence. Capacity is `min(2 * max_orders, 100,000)` per symbol with
FIFO eviction; an order id older than the retained window can be accepted
again.

The state hash covers the book levels and FIFO order, the dedup entries
(sorted by order id), both sequence counters, the last offset, the last
timestamp, and the halt flag. The shard hash covers the shard id and each
symbol hash in symbol order. `whippet verify` loads each snapshot, rebuilds the
engines, and prints the recomputed hash.

`whippet replay` rebuilds an engine from the order log without touching the
live consumers. It reads with a fixed consumer name, `whippet-replay-p<N>`,
and never commits an offset. With `--from`, it starts at that offset and
ignores any snapshot. Without `--from`, it restores the partition snapshot and
replays the tail after the snapshot's maximum `last_offset`. `--to` is
inclusive. `--expect-hash` turns a hash mismatch into a non-zero exit.

On a local single-node run (M4, Docker Desktop, dev-loop), partition 1 had a
snapshot at offset 108466. Full replay from offset 0 and snapshot-seeded replay
from offset 108467 both produced:

```text
state_hash = ceddf1c94cfdb121541ae726d4001fb43daaf92dc54a8ef19216761a5330e510
```

Equal hashes mean the snapshot state plus the replayed tail matches a replay
of the whole log for this input. It is a determinism check, not a crash test
and not a benchmark. The exact commands are in
[DEMO.md](DEMO.md#replay-the-log).

Publishing outputs and committing the input offset are not atomic. The
pipeline is at-least-once. If the engine dies after appending executions or
MD but before committing, or if it restarts from a snapshot older than the
committed offset, the tail is replayed and outputs can be published twice.
The engine state converges because replay is deterministic; downstream
consumers must deduplicate by `(symbol, cause_offset, seq)`. The MD cache
converges at the next snapshot, but the visible trade tape can contain a
duplicate. See [FAILURE-MODES.md](FAILURE-MODES.md).

## What is deliberately simple

- **One process, one broker.** Gateway, engine, MD hub, and API share a
  process. Process isolation is logical, not a network boundary.
- **Two ingress transports.** HTTP/JSON for the dashboard, simulator, and
  curl; binary TCP for the throughput path. No QUIC or FIX session layer yet.
- **No pinned cores.** The matcher has a dedicated OS thread, but no core
  pinning, no `SYS_NICE`, no busy-poll, no hugepages, no NUMA work.
- **`BTreeMap` in the MD cache.** The price ladder lives only in the engine.
- **bincode snapshots.** Simple and versioned, not zero-copy or
  platform-stable across schema changes.
- **Named consumers, not consumer groups.** Each engine and MD task has a
  fixed consumer name and commits manually. There is no group membership or
  rebalancing.
- **Full-state frames at 20 Hz.** No per-client delta protocol, no conflation
  policy knobs, no sequence-gap recovery in the browser.
- **No authentication or transport security.** Bootstrap credentials only.
- **Single node.** `persisted` means the local disk; there is no replica and
  no failover.
- **One market-data path.** The dashboard consumes `md`; the node has no
  separate replay or audit consumer. The offline `whippet replay` CLI is a
  separate correctness tool, not a live consumer.
- **Limited order semantics.** Market collar, cancel-resting STP, and the five
  order types above. No auctions, halts, or complex order types.

## Phase boundaries

**Phase 0: foundation.** Workspace, protocol types, deterministic matcher,
journal abstraction, MD hub, simulator, bench harness, snapshot format.

**Phase 1: durable slice (this build).** Gateway HTTP/WS, Iggy
orders/executions/md, per-partition engine tasks, snapshots and verification,
replay CLI, Prometheus endpoint, dashboard Live/Bench/About, simulator, bench
CLI, healthcheck.

**Phase 2: depth.** Turbo mode, surveillance, the audit archive and replay
scrubber, PATs and scoped users, TLS, stream encryption, consumer-group fanout
and conflation, the full benchmark matrix, and failure-mode tests with proof
artifacts.

**Phase 3: HA and spectacle.** Three Iggy nodes with VSR, random-phase
failover, chaos scenarios, replay UI, and headline Linux benchmark runs.

**Phase 4: community.** Upstream Iggy issues and PRs, reproduction scripts,
and public write-ups.

## Related documents

- [EXCHANGE-101.md](EXCHANGE-101.md): order types, price-time priority, and
  the market-data model in plain terms.
- [ROADMAP.md](ROADMAP.md): phase gates.
- [docs/adr/README.md](adr/README.md): the decision records behind the crate
  layout, matcher data structures, topic layout, dashboard stack, benchmark
  protocol, single-node scope, and security model.
- [OPERATIONS.md](OPERATIONS.md): configuration, retention, backup, and
  metrics.
- [FAILURE-MODES.md](FAILURE-MODES.md): current failure behavior and proof
  status.
