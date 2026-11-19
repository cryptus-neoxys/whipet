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
bearer token when the node requires one; `Ping` measures liveness. A durable
`Submit` is answered with one ack carrying `partition`, `base_offset`, and
`count`; order `i` has offset `base_offset + i` because the whole batch is one
append. In turbo mode `base_offset` is the first local seq and the per-order
seqs are not necessarily contiguous across concurrent batches; the fixed ack
frame keeps its shape, and a full window returns `WindowFull`. The
wire types live in `crates/whippet-core/src/tcp.rs`.

Both transports call the same encoding and append path, so the engine cannot
tell how an order arrived. TCP does not change durability: the ack is sent
after the Iggy append confirms. TLS is not terminated in-process in Phase 1;
put a proxy in front of the TCP port if the network is untrusted.

### Turbo mode

`WHIPPET_MODE=turbo` keeps the same matcher but changes where its input comes
from and when the client is told. [ADR 0013](adr/0013-turbo-mode.md) is the
design record; the implementation is:

```text
HTTP/TCP -> TurboIngress (one bounded lane + window per active shard)
         -> feed_turbo -> input ring -> matcher thread (sole sequencer)
         -> ack {seq, accepted, reject}          (before any journal I/O)
         -> output ring -> writer: orders, executions, md
         -> release window slot -> applied offset -> commit
```

- The matcher assigns the local `seq` (0 on an empty log,
  `snapshot_last_offset + 1` after a restore), calls the same
  `ShardEngine::process`, and sends the ack immediately. The journal message
  was built once at ingress, so the write-behind appends exactly the bytes the
  durable path would have appended.
- `TurboWindow` bounds the crash loss by count and age; a full window rejects
  with `window_full` and never falls back to durable.
- The writer appends `orders` first-class alongside executions and MD, and
  releases the window permits only after all three appends are durable.
- A restart replays the journaled tail to the log head before the lane opens.
  With no snapshot, a non-empty orders log is a startup error: the local seq
  domain must be anchored to an empty log.
- Acks carry `guarantee: "matched_unjournaled"`; durable acks carry
  `"accepted_by_log"`. `/metrics` exposes
  `whippet_turbo_uncommitted_window{shard}`,
  `whippet_turbo_journal_lag{shard}` (the same set in this design),
  `whippet_turbo_window_drops_total`, and the
  `whippet_turbo_ack_latency_microseconds` summary.

### Instrument control plane

Halt/resume, session changes, and risk limits are `CommandKind::Halt`/
`Resume`/`SetSession`/`SetRisk` records in the **orders** log, not a side
channel. The matcher processes them in log order, so a restore or a full replay
reproduces the halted, session, and risk state and the state hash covers it. The
engine rejects new orders with `Rejected/Halted` while halted, allows cancels,
and emits a `MdKind::Status` event (`MD_FLAG_HALTED`) that the MD hub applies to
`BookView.halted`. The engine's state snapshot carries `halted`; the MD
snapshot carries the same bit in its optional trailing status byte, so the hub
learns a restored halt from the startup append without an extra status record
or an MD-sequence bump.

The HTTP endpoints are operator-only (`WHIPPET_OPERATOR_TOKENS`, a namespace
separate from trading tokens); the binary TCP channel rejects operator
commands. One JSON audit record per accepted request lands in the `audit`
topic; see [ADR 0015](adr/0015-audited-instrument-control-plane.md) and
[ADR 0026](adr/0026-engine-enforced-risk-limits.md). Engine risk bounds each
participant's worst-case net position and resting buy notional; the static
ingress guard is only a per-order filter.

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
2. Dispatch to cancel, amend, or new order.
3. Validate: symbol, halt/session state, quantity, price, ladder bounds,
   duplicate id, post-only crossing, FOK fillability, and the participant's
   engine risk bounds. An amend also checks ownership, no size increase, and
   no crossing price (ADR 0027).
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
and post-only. Cancel removes a resting order. Amend atomically replaces a
resting order's price and remaining quantity under the same id, losing time
priority, refusing size increases and crossing prices. Self-trade prevention
cancels the resting order and emits a maker cancellation; the aggressor keeps
its remaining quantity. There are no stop, iceberg, pegged, trailing, auction,
or multi-leg orders. Halt, session, and risk changes are log-ordered operator
commands with an audited HTTP control plane; see ADRs 0015, 0018, and 0026.

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
- **Off-loop hash.** The matcher calls `snapshot_state_without_hash_compacted`,
  which compacts the dedup queues first so the saved walk matches the live
  one; the blocking writer fills the per-symbol hashes (a supplied non-zero
  hash that disagrees is rejected, not repaired) and verifies the final snapshot
  by rebuilding it. The terminal-eviction queue is hashed with the live ids in
  eviction order, so a reordered-but-valid queue changes the hash. The earlier
  ~170k-order mismatch was a dedup-count bug: the off-loop hash used the
  insertion-walk length where the live hash uses the distinct-id count, and a
  re-admitted id made them differ. The fix is pinned by a 100-engine ×
  200k-command test. The snapshot file format is version 6 (per-symbol config, risk table,
  `BookSnapshot.min_price_ticks`, and the dedup terminal-eviction order stored
  as `u32` indices into the dedup walk instead of full `OrderId`s); versions 3
  and older are rejected.
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

The stable output identity is in the **payload**, not the routing envelope:
executions and MD records carry `(symbol, cause_offset, seq, kind)`. The
envelope's routing headers carry `symbol`, `seq`, `type`, and `schema` but not
`cause_offset`, so a consumer that must deduplicate duplicates has to decode
the fixed payload. Keeping the full identity in one canonical place avoids
paying for a duplicate header on every output record.

## Sequenced market-data feed

The dashboard's `/ws` stream is deliberately conflated: one full `StateView` at
20 Hz, and a slow browser simply misses intermediate frames. External
consumers get a separate additive endpoint, `GET /ws/md?symbol=...`, with a
different contract:

- one full-depth **price-level** snapshot at the hub's current per-symbol
  `seq`, then every accepted `MdEvent` as a `delta` in sequence order;
- a `resync` notice followed by a fresh snapshot when the bounded broadcast
  buffer lags, so a slow consumer never stalls the hub and never silently
  loses a frame mid-sequence;
- zero feed work on the MD apply path when no subscriber is attached, and one
  JSON serialization per event shared by all subscribers when one is.

The feed mirrors the order book at price-level granularity; it is not an
order-by-order feed. See [ADR 0016](adr/0016-sequenced-price-level-market-data-feed.md).

## Invariants and state machines

The engine's book/dedup and order lifecycle invariants are executable in
`Book::check_invariants`, `DedupTable::check_invariants`, and
`SymbolEngine::check_invariants`; the dedup lifecycle table is enforced by
`DedupTable::update` (ADR 0019). Each active shard's engine and MD tasks move
through `Starting -> Running -> Stopped` or `Failed` (terminal), and readiness
requires every active role to be Running (ADR 0020). The durable path has three
explicit state machines: the snapshot writer (save-time structural and hash
invariants, rename-only advance), the commit watermark/retry (committed never
ahead of applied, bounded drift, retries that never stop), and the turbo
admission window (Queued to Completed/Abandoned, exactly-once permit release)
(ADR 0021). The journal enforces its offset/commit contract in the test backend
(ADR 0023), the MD hub refuses book-state deltas until a baseline image seeds
the symbol, and `whippet ledger` folds the logs into a double-entry projection
whose balances reconcile with positions, converting tick prices to quote cents
through an explicit tick-size map (ADR 0022/0025). Wire decoders are total over
untrusted bytes and re-encode canonically, pinned by the core proptest suite. The full catalogue and what
remains unproven is [INVARIANTS.md](INVARIANTS.md).

## Observability

The node exposes `GET /metrics` in Prometheus text format. Counters cover
orders, executions, trades, MD records, and engine commands. Gauges cover
ingress and match latency quantiles, throughput, Iggy connectivity, partition
count, mode, MD freshness, and MD sequence. Uptime is exposed as a monotonic
counter. Latency quantiles are rolling-window views merged from the per-shard
histograms (`whippet_stage_p50_us` / `whippet_stage_p99_us` plus the
ingress/match/turbo summaries); they describe recent behavior, not process
lifetime, and a spot value is not a benchmark.
`whippet_consumer_lag` is the computed lag summed over the active shards:
every five seconds the health monitor reads each shard's committed offset and
head through that shard's engine-pool client and reports the total. The full
series list is in
[OPERATIONS.md](OPERATIONS.md#prometheus-metrics).

## Recovery

The engine state per symbol is: the book, the dedup table, the risk table,
`exec_seq`, `md_seq`, `last_offset`, `last_ts_ns`, the command count, the halt
flag, the session, and a BLAKE3 state hash.

A shard snapshot contains all symbol states for one partition and is written
to `data/snapshots/partition-<N>.snapshot`:

- Magic `WHIPSNP1`, format version `6`, then a bincode payload. The
  terminal-eviction order (added in v4) is stored as `u32` indices into the
  dedup walk instead of full `OrderId`s, so the file is smaller than v3: the
  recovery test's 225,000-command workload (95,000 terminal ids) writes a
  7,940,220-byte v4 file (measured with `cargo test --release -p whippet
  --test recovery`; file size only, not timing). Format history: v2 added the
  per-symbol config and `BookSnapshot.min_price_ticks`, v4 the compact
  eviction order, v5 the session byte, and v6 the per-symbol risk table.
  Version 5 and older files are rejected with `BadVersion`. Because the
  snapshot carries the full per-symbol state (config, book, dedup, session,
  risk), the state hash is reproducible from it alone. There is no in-binary migration
  and no automatic fallback: delete or move the old snapshot files (or
  `make reset`), start the new node so it full-replays from 0, and let it
  write v6. The terminal queue and risk table are part of the state hash
  (`DedupTable::hash_into` hashes the live terminal ids in eviction order and
  `RiskTable::hash_into` hashes participant entries in key order), so a
  reordered-but-valid `dedup_terminal_idx` changes the snapshot hash and
  `whippet verify` fails. Snapshots are taken
  from a compacted dedup walk (`snapshot_state_without_hash_compacted`), so a
  restored walk is exact even if an id that was dead at snapshot time is
  re-admitted by the tail. Restore rejects malformed indices (out of range,
  pointing at a non-terminal entry, or a terminal id missing from the queue);
  repeated indices for the same id are allowed. Decoding enforces a 256 MiB
  cap: an oversized file is `TooLarge`, not `BadMagic`, and the bincode
  payload is decoded with a size limit, trailing-byte rejection, and a
  top-level symbol-count prefix check before allocating. The writer's
  serialize/fsync remains the dominant snapshot cost (v3-era stage split at
  170k commands / 68k dedup entries: clone-only 0.28 ms on the shard,
  hash 5.35 ms on the writer, serialize (+fsync) 38.7 ms; see
  [PERFORMANCE.md](PERFORMANCE.md#phase-15e-implemented-dev-loop-measured)).
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
terminal-first eviction (active resting orders are never evicted to make room).
The window is count-based, not a time TTL: its wall-clock span shrinks as
throughput rises. An order id older than the retained window can be accepted
again, so the table bounds duplicates but does not promise unlimited
idempotency.

The state hash covers the instrument config (tick size, ladder bounds,
max orders, snapshot depth, market collar), the book levels and FIFO order,
the dedup entries in insertion order, the terminal-eviction queue in eviction
order, the command count, both sequence counters, the last offset, the last
timestamp, and the halt flag. The shard hash covers the shard id and each
symbol hash in symbol order. `whippet verify` loads each snapshot, rebuilds the
engines, and prints the recomputed hash.

`whippet replay` rebuilds an engine from the order log without touching the
live consumers. It reads with a fixed consumer name, `whippet-replay-p<N>`,
and never commits an offset. With `--from`, it starts at that offset and
ignores any snapshot. Without `--from`, it restores the partition snapshot and
replays the tail after the snapshot's maximum `last_offset`. `--to` is
inclusive. `--expect-hash` turns a hash mismatch into a non-zero exit.
Both `verify` and `replay` accept `--symbols` (`WHIPPET_SYMBOLS`); the
universe must match the node's, or a valid snapshot is rejected for a symbol
the node never wrote. `verify` takes `--partitions`, `replay` takes
`--partition` plus `--exchange-partitions`, and the partition count must match
the layout the snapshots were written with. `whippet verify
--require-snapshot` requires a snapshot for every active shard (a shard that
owns at least one configured symbol), not just one file. `whippet replay
--require-snapshot` requires the run to be seeded from a snapshot that is a
real resume point, fails when any record was skipped or undecodable, fails
when `snapshot_offset > head` or `snapshot_offset >= head` (there must be a
tail to replay), and reports `snapshot_offset`, `committed_offset`, `head`,
and `tail` in the summary. The commit watermark may lag the snapshot; only
the log head bounds it. Node startup applies the same head bound: a snapshot
ahead of its partition's head is rejected before the reader waits on a tail
that will never arrive.

On a local single-node run (M4, Docker Desktop, dev-loop), partition 1 had a
snapshot at offset 158548. Full replay from offset 0 and snapshot-seeded replay
from offset 158549 both produced:

```text
state_hash = c2b9838ba8438245697c93b5396113cbeb38b3a136ae174842f11b1e39536739
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
- **Scoped bearer tokens, no transport security.** `WHIPPET_API_TOKENS`
  binds each token to one participant, and `WHIPPET_METRICS_TOKEN` can protect
  `/metrics`. TLS, stream encryption, and Iggy scoped users/PATs are still
  Phase 2; the node uses the bootstrap Iggy account today.
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
