# Performance

This document is a performance model and an optimization order, not a results
page. It answers three questions:

1. Where does time go on the durable path, and which parts are actually
   measured?
2. Which bottleneck should be fixed first, and what would "fixed" look like?
3. What must be true before a number can be published?

The only benchmark result the repository documents is the smoke run in
[BENCHMARKS.md](BENCHMARKS.md). The baseline sweep in this document is a local,
one-repetition regression baseline. Both are `dev-loop`. Neither is a headline
result, and neither supports a sub-millisecond durable claim.

## Summary

- **The durable ingress path is the knee.** Single-order HTTP plus one Iggy
  append per order saturates between 200 and 2,000 orders/s in the local
  baseline. At 1,000/s the p99 is 86.7 ms; at 2,000/s the p50 is 88.6 ms; at
  5,000/s the harness records 31,924 errors.
- **Batch ingress is the first optimization.** One HTTP request and one append
  for N same-symbol orders removes the per-order broker round trip. It is a
  throughput optimization, not a latency optimization: the first order's ack
  waits for the whole batch.
- **The matcher is not the current end-to-end knee.** It is a synchronous,
  allocation-free state machine in steady state. The engine micro-optimizations
  (best-price cache, `FxHash`) are second because they improve match time, not
  the durable-ingress queue.
- **Fanout serialization is the MD-side knee, but not yet.** The hub publishes
  one frame at 20 Hz; each WebSocket subscriber deep-clones and serializes it
  independently. At one subscriber this is small. At 100 it is the cost.
- **No sub-millisecond durable end-to-end claim.** The matching-only design
  target is a target. The durable path is millisecond-scale in the baseline, and
  there are no stage timestamps to attribute it more finely.

## Measured inputs

### The repository's documented smoke run

The only documented benchmark result is the smoke run in
[BENCHMARKS.md](BENCHMARKS.md#smoke-run-smoke-not-a-benchmark): Apple M4,
Docker Desktop, HTTP JSON limit orders, three symbols, 16 partitions, 200
orders/s, 10 s measured, 2 s warmup, one repetition, 2,000 samples, 0 errors.

| Metric | Value |
|---|---:|
| p50 | 1,948 µs |
| p90 | 2,567 µs |
| p99 | 3,445 µs |
| p99.9 | 27,423 µs |
| max | 34,495 µs |
| scheduler slip p50 | 1,095 µs |
| scheduler slip p99 | 1,996 µs |

It is a smoke test, not a benchmark. The p99.9 is backed by roughly two samples.
Do not quote it as a matcher, broker, or end-to-end result.

### The baseline sweep (the "before" for batch ingress)

Four local runs, all on Docker Desktop with the simulator stopped. Each is
single-order HTTP (`POST /api/orders`), 8 s measured, 2 s warmup, one
repetition, `batch_size = 1`, `subscribers = 0`, scope `dev-loop`. The
manifests are under `artifacts/bench/` (gitignored); `git_sha` is `unknown` in
all four. Treat them as a regression baseline, not a published result.

| Rate (orders/s) | Samples | p50 (µs) | p99 (µs) | p99.9 (µs) | Max (µs) | Errors | Slip p50 (µs) | Slip p99 (µs) | Local manifest |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 200 | 1,600 | 2,305 | 6,307 | 16,095 | 16,575 | 0 | 1,482 | 2,569 | `9cb7a210-31a4-4915-aa37-5335a106957e.json` |
| 1,000 | 8,000 | 1,483 | 86,719 | 131,967 | 135,039 | 0 | 414 | 1,416 | `c5edad39-c472-461f-b881-48eedf91b2c9.json` |
| 2,000 | 16,000 | 88,639 | 655,871 | 705,535 | 715,775 | 0 | 174 | 1,373 | `1032c716-dec6-4743-a871-a68e216bddb3.json` |
| 5,000 | 40,000 | 1,085,439 | 1,675,263 | 1,807,359 | 1,830,911 | 31,924 | 0 | 9 | `cd463bbe-c64d-4201-81a8-bf92f0834a4f.json` |

How to read it:

- **200/s is the stable baseline.** p50 2.3 ms, p99 6.3 ms, no errors. Most of
  the p50 is scheduler slip (1.48 ms), not service time.
- **1,000/s is the tail collapse.** p50 is still 1.48 ms, but p99 is 86.7 ms.
  The queue exists; it has not yet swallowed the median.
- **2,000/s is saturated.** p50 88.6 ms, p99 655.9 ms, no errors. The system is
  queueing, not dropping.
- **5,000/s is past the knee.** 31,924 errors and a goodput of 3,147.6/s. The
  harness records an in-flight-cap skip twice (once as a failed sample, once as
  a spawn error), so the error count is an upper bound. In-flight-cap skips are
  recorded with `latency = slip`, which is near zero; HTTP failures carry their
  own completion time. Either way, the percentiles describe a saturated run, not
  a service-time distribution.

Two cautions:

- **The p50 is not monotonic.** The 200/s run has a higher p50 than the
  1,000/s run because its scheduler slip is higher (1,482 µs vs 414 µs).
  Latency is measured from the scheduled deadline, so it includes generator
  slip. Do not read the p50 column as a service-time curve.
- **This is one repetition per rate, eight seconds each.** It is enough to see
  the knee, not enough to publish a percentile. The repository's own policy in
  [BENCHMARKS.md](BENCHMARKS.md#method) requires five repetitions, a warmup, a
  method card, and a manifest before a number is published.

The repository's documented smoke run and the baseline-200 run are different
200/s runs. The difference (p50 1,948 vs 2,305 µs; p99 3,445 vs 6,307 µs) is
run-to-run variance on a dev-loop host. Neither run can arbitrate the other.

### What the baseline does not tell you

The harness measures `completion - scheduled_deadline` for one HTTP request. It
does not stamp the gateway, the broker, the engine poll, the match, the MD
consumer, the hub, or the WebSocket. `stages` is empty in the manifest. So the
baseline tells you that the whole request path has a knee. It does not, by
itself, prove which stage owns the knee.

The strongest structural fact is that the gateway awaits exactly one I/O
operation before it acknowledges: the Iggy append. That makes the append the
prime suspect, not a proven culprit. A stage timer is the next measurement, not
a conclusion.

A rough envelope, only as arithmetic: at 200/s, p50 latency minus p50 slip is
about 0.82 ms and p99 minus p99 slip is about 3.74 ms. At 1,000/s the same
subtraction gives about 1.07 ms at p50 and 85.3 ms at p99. These are differences
of percentiles, not a measured service-time distribution. Do not quote them as
the append cost.

### Measured after the first optimization tranche (dev-loop)

Fresh stack, simulator stopped, mixed limit/IOC/cancel, 10 s measured, 3 s
warmup, one repetition, default snapshots. The first table is the pre-batch
single-order baseline; the second is the post-tranche batch sweep.

| Rate | Batch | p50 | p99 | Errors |
|---:|---:|---:|---:|---:|
| 1,000/s | 1 | 543 µs | 1,027 µs | 0 |
| 1,000/s | 10 | 2,501 µs | 4,875 µs | 0 |
| 5,000/s | 1 | 838,143 µs | 1,008,639 µs | 0 |
| 5,000/s | 10 | 1,762 µs | 4,695 µs | 0 |
| 5,000/s | 100 | 8,231 µs | 24,351 µs | 0 |
| 5,000/s | 256 | 19,407 µs | 44,991 µs | 0 |

Batch p50 includes the wait for the batch to fill; per-order amortized p50 is
roughly `p50 / batch`. Batch 10 at 5,000/s is the useful dev-loop result:
**p50 1.76 ms, p99 4.70 ms, 0 errors**. Batch 1 at 5,000/s queues. These are
one-repetition Docker Desktop numbers, not headline results.


## Stage budget

Status legend:

- **measured** — a number exists in a repo document or a local manifest, with
  its scope attached.
- **estimated** — a cost class inferred from the code structure; no timing.
- **unknown** — no isolated data. The stage must be instrumented before it can
  be optimized or claimed.

| # | Stage | Code path | Cost drivers (structural) | Status |
|---:|---|---|---|---|
| 1 | Gateway parse/validate | `submit_order` / `submit_orders` in `crates/whippet/src/gateway.rs`; Axum `Json<OrderRequest>`; symbol, qty, price, client-id, participant checks | JSON decode; linear instrument lookup over three instruments; integer tick conversion; auth and rate-limit middleware | measured only as part of the whole request (baseline p50 1.48–2.31 ms at 200–1,000/s); not isolated |
| 2 | Order-id / participant hash | `OrderId::derive`, `ParticipantId::derive` in `crates/whippet-core/src/ids.rs` | Two BLAKE3 hashes over short inputs; 16-byte and 8-byte truncation | estimated (two short hashes); no isolated timing |
| 3 | Header / command encode | `Command::encode` (78 B); `MessageMeta::pairs` (7 string pairs); `JournalMessage` | One 78-byte payload `Vec`; seven header values plus a `Vec`; keys are cached in the journal, values are still strings; 32-hex `trace_id` formatting | estimated; 78 B and 7 headers are documented in [ARCHITECTURE.md](ARCHITECTURE.md#protocol) |
| 4 | Iggy append ack | `IggyJournal::append_owned` → `send_messages` → await the first confirmation | One TCP round trip for a batch; cached header keys and topic identifiers; owned payload (no copy); 256-message chunks; broker persistence and flush policy | measured only as part of the whole request; not isolated. This is the only awaited I/O before the ack. |
| 5 | Engine poll/decode | `run_engine_shard` → `journal.read_payloads(Topic::Orders, 1024, 50 ms)` → `Command::decode` | Long poll with `min_count = 1`; up to 1024 records; payload-only read skips header TLV parse; one fixed-layout decode per record | unknown |
| 6 | Match | `ShardEngine::process` → `SymbolEngine::process` | Best-price lookup; order-index and dedup hash lookups; FIFO walk per fill; FOK walks every resting order at crossing levels; output `Vec` pushes | unknown; the live `whippet_match_latency_microseconds` gauge exists, but no published value |
| 7 | Execution / MD encode | `execution_message`, `md_message`, `snapshot_message` in `engine_runner.rs` | 104-byte or 84-byte payload `Vec` plus seven header `String`s per event; a sweep can emit several events per order | estimated; 104 B and 84 B are documented in [ARCHITECTURE.md](ARCHITECTURE.md#protocol) |
| 8 | Iggy append (engine output) | `append_with_retry` for `executions` and `md` in parallel, then `commit_with_retry` | Two appends in parallel plus one offset commit per non-empty poll batch; cached header keys; 256-message chunks | unknown; structurally two parallel round trips plus one commit per batch |
| 9 | MD consume | `run_md_shard` → `journal.read_payloads(Topic::Md, 1024, 100 ms)` → `MdHub::apply` | Payload-only read; one global hub `Mutex` acquisition and one clock read per event; `BTreeMap`/`HashMap` updates; one commit per batch | unknown |
| 10 | 20 Hz state build | `MdHub::spawn_publisher` → `publish` → `build_publish_state` | Full `BookView` build for each symbol (20 levels per side plus depth folds), 250-trade tape clone, metrics snapshot (three locks), `watch` replace; `/api/state` and `/metrics` now read the published `Arc` instead of rebuilding | ~0.73 µs build; ~6.9 ns `latest()` read; 20 Hz documented in [ARCHITECTURE.md](ARCHITECTURE.md#market-data) |
| 11 | WS / JSON delivery | `ws_loop` → deep clone of `StateView` → `serde_json::to_string` → text frame | One deep clone and one serialization per subscriber per tick; uncompressed text; 64 KiB frame cap | unknown; structurally O(subscribers) serialization and O(subscribers) egress |

The first four rows are the durable-ack path. Rows 5–8 are the engine path.
Rows 9–11 are the market-data path. They are three different latency paths; a
number from one is not a number for another.

## The two paths (and the fanout path)

### 1. Single-order durable ack vs batch ingress

**Single-order durable ack**

```text
client
  │  POST /api/orders   (JSON)
  ▼
gateway: parse → validate → derive order id + participant → encode 78 B + 7 headers
  │  append(1 message)
  ▼
Iggy orders partition p: broker append, one confirmation
  │
  ▼
HTTP 200 { status: "accepted_by_log", offset, durable_ack_ms }
```

This path ends at the log. It does not include matching. `durable_ack_ms` is the
gateway's wall-clock time for validation plus the append confirmation, as
[DEMO.md](DEMO.md#beat-1-place-an-order) says. One order costs one broker round
trip. This is the path the baseline sweep measures.

**Batch ingress**

```text
client
  │  POST /api/orders/batch   { orders: [N], one symbol }
  ▼
gateway: validate all → encode N → append(N messages) → one confirmation
  │
  ▼
HTTP 200 { base_offset, count, orders: [N acks with base_offset + i] }
```

This path amortizes one broker round trip over N orders. The route exists in
this working tree (`POST /api/orders/batch`), is capped at 256 orders, requires
one symbol per batch, validates the whole batch before appending anything, and
returns contiguous offsets from `base_offset`.

It is a throughput path, not a latency path:

- The first order's ack waits for the whole batch to be appended. The last
  order sees the smallest added delay.
- Because the client sends an explicit batch, there is no server-side
  aggregation window. A future background aggregator would add one.
- A mixed-symbol batch is rejected: one batch must be one partition and one
  append, which preserves the per-symbol ordering contract.

The gateway side is present. The benchmark harness is not yet driving it: in
this tree `WorkloadSpec`, `BenchConfig`, the CLI `--batch-size` flag, and the
HTTP batch body are wired. The batch rows in the matrix are measured in the
dev-loop table above; the Linux matrix is still pending.

### 2. Matching-only vs end-to-end

**Matching-only**

```text
engine task: poll → decode → ShardEngine::process → EngineOutput
```

This is the synchronous matcher. It has no `await`, no I/O, no broker, and no
wall-clock read inside `SymbolEngine::process`. The live
`whippet_match_latency_microseconds` gauge wraps this call. The design target is
sub-millisecond; no published result exists. The engine microbench in
`crates/whippet-engine/benches/engine.rs` is the tool for this path, not the
HTTP harness.

**End-to-end**

```text
client → gateway ack → orders log → engine poll → match
       → executions log + md log → MD poll → hub apply
       → 20 Hz frame → WS JSON → browser
```

There is no single clock that spans this path. The manifest's `stages` array is
empty. `MdEvent.timestamp_ns` is the input command's timestamp, not the time the
MD hub applied the event. `freshness_ms` is the age of the newest MD event at
the hub, not a per-order latency.

Two structural delays sit on this path:

- The engine poll has `max_wait = 50 ms` and the MD poll has `max_wait = 100 ms`,
  but both use `min_count = 1`, so these are idle bounds, not fixed per-order
  delays.
- The hub publishes at 20 Hz, so an event applied just after a tick can wait up
  to one publish interval (50 ms) before it is visible in a frame. This is a
  rendering cadence, not a matching cost.

Do not subtract the gateway ack time from the WebSocket frame time and call the
result "end-to-end latency". They are timestamps for different events, and the
current records do not link them with a single clock.

### 3. Fanout amplification

```text
1 hub frame per 50 ms
  → watch::Sender<Arc<StateView>>
  → N WebSocket tasks
  → N × (deep clone + serde_json::to_string + text frame send)
```

The `watch` channel is the isolation mechanism: a slow browser misses
intermediate frames, and the engine never waits for it. The cost model is:

```text
serializations per second = 20 × N
egress bytes per second    ≈ 20 × N × frame_bytes
```

`frame_bytes` is not measured. It grows with symbols × depth plus the 250-trade
tape, and each WebSocket message is capped at 64 KiB. At N = 1 the cost is
small. At N = 100 it is 2,000 serializations/s of a full state frame, and that
is the MD-side knee. There is no fanout workload in the harness today
(`subscribers` is hardcoded to 0), so this is a model, not a measurement.

## Hot-path rules

The rules are stated for the match loop, then checked against the code. The
distinction matters: the engine crate satisfies them; the journal edge around it
does not, and the edge is where the baseline knee lives.

| Rule | Where it must hold | Current status | Gap or evidence |
|---|---|---|---|
| No allocation in the match loop | `SymbolEngine::process`, `Book` operations | Holds in steady state. Output vectors are caller-owned and reused; the arena, free list, and index are preallocated. | `Book::snapshot` and `state_hash` allocate, but only on the snapshot interval and shutdown. Journal-edge message construction allocates per event: `MessageMeta::pairs` (7 `String`s), `execution_message`/`md_message` (payload `Vec` + 7 `String`s), `build_order`. |
| No syscall in the match loop | `SymbolEngine::process` | Holds. The matcher performs no I/O. | The shard loop around it reads the clock once per record (`Instant::now()` in `engine_runner.rs`) and the hub reads it once per MD event (`now_ns()` in `MdHub::apply`). On Linux these are vDSO clock reads, not syscalls, but they are still work on the serial path. |
| No lock in the match loop | `SymbolEngine::process` | Holds. One task owns each partition's books; the engine crate has no lock. | `Metrics::record_match_latency_us` locks a `Mutex<Histogram>` per command in the shard loop. `MdHub` takes a global `Mutex` per MD event. Both are outside the pure matcher, but both are on a single-writer path. |
| No async in the engine | `whippet-engine` | Holds. The crate has no Tokio dependency. The shard task awaits only at the journal edge: poll, append, commit. | None in the engine. The async boundary is the journal edge by design. |
| Fixed headers and payload layout | Protocol and journal edge | Holds. Payloads are fixed-width little-endian with no varints, floats, or padding: 78 B command, 104 B execution, 84 B MD event. Seven string headers carry routing and diagnostics. The engine never parses headers. | `IggyJournal::read` parses headers for every record. A payload-only `read_payloads` path exists in the journal, but the engine and MD runners in this tree still call `read`. The header-key cache removes repeated `HeaderKey` parsing, but the header values are still `String`s. |
| Per-partition single writer | Engine task per active partition | Holds logically. One task per active partition, no concurrent writer, no lock on the book. | The task is a Tokio task on a multi-thread runtime, not a pinned OS thread. It can migrate cores between awaits. Pinning, busy-poll, and SPSC rings are later Linux work. |

### Periodic costs that are not in the per-order budget

These do not show up in a steady-state per-order measurement, but they can own a
tail sample:

- **Snapshot build.** Every `WHIPPET_SNAPSHOT_EVERY` commands (default 10,000),
  `engine.snapshot_state()` clones the book and the dedup table on the shard
  task, then `spawn_blocking` writes the file. `state_hash()` iterates the book
  and the dedup table. At the default dedup capacity (500,000 entries per
  symbol), this is a large clone. It is a periodic tail-latency source, not a
  per-order cost.
- **Initial MD snapshot.** At startup, each shard publishes a full MD snapshot
  before it begins consuming. This is a startup cost, not a steady-state cost.
- **Health monitor.** Every five seconds the node pings Iggy and refreshes
  consumer lag with `committed` plus `lag` per active shard. That is several
  broker round trips every five seconds. It is small, but it is not free.
- **Snapshot restore.** `from_snapshot` verifies the state hash twice: once after
  construction and once after canonical re-derivation. This is a startup cost
  that catches ordering mistakes early.

## Current bottlenecks, ranked

The order below is the order in
[ADR 0009](adr/0009-performance-budget-and-optimization-order.md). Each rank
names the evidence, the next optimization, the expected effect, and what would
falsify the ranking.

### Rank 1 — HTTP JSON plus one Iggy append per order (dominant durable-ingress cost)

**Evidence.** The gateway awaits exactly one I/O operation before it
acknowledges: `journal.append(Topic::Orders, partition, &[message])`. The
baseline sweep shows the whole request path collapsing between 200 and
2,000 orders/s. At 1,000/s the p99 is 86.7 ms; at 2,000/s the p50 is 88.6 ms;
at 5,000/s there are 31,924 errors. The batch route exists, and the post-tranche
sweep above shows the knee moving from ~1k/s to at least 5k/s with batch 10.

**Why it is dominant.** It is the only awaited I/O on the ack path, and it is
paid once per order. The JSON parse, the two BLAKE3 hashes, the 78-byte encode,
and the seven header strings are real costs, but they are CPU work that can
overlap with other tasks. The broker round trip is the per-order cost that does
not amortize.

**Measured.** The batch path is wired and the dev-loop sweep is above. The next
optimization is server-side group commit for single-order clients and a binary
TCP ingress, not another batch-size sweep:

- Gateway: `POST /api/orders/batch`, one symbol, at most 256 orders, one append,
  all-or-nothing validation, contiguous offsets. This route is present in the
  working tree.
- Harness: wire `--batch-size N` to the batch body, record `workload.batch_size`,
  and report orders/s separately from requests/s. The CLI flag exists; the
  `BenchConfig` and manifest wiring do not yet.
- Journal: use the payload-only read path, the owned append path, and the cached
  header keys on the shard runners. These exist in the journal crate but are not
  all wired into `engine_runner.rs`.

**Expected effect.** One broker round trip per N orders instead of one per
order. Throughput rises until the broker or disk saturates. The first order's
ack pays the whole-batch append; the last order sees the smallest added delay.
This is a throughput win, not a sub-millisecond latency claim.

**What would falsify it.** If stage timers show that JSON parsing or HTTP
handling dominates the request and the append is a small fraction, then the next
optimization is transport, not batching. There are no stage timers today, which
is why the first measurement is part of the first optimization.

### Rank 2 — Journal fast paths and per-event work on the engine edge

**Evidence.** The shard loop does up to two appends plus one offset commit per
non-empty poll batch. At low rates a batch is one order, so the downstream path
pays up to three broker round trips per order. `IggyJournal::read` parses every
record's user headers even though the engine and MD consumers only decode
payloads. `execution_message` and `md_message` allocate a payload `Vec` and
seven header `String`s per event. The shard loop records a match-latency sample
through a `Mutex<Histogram>` per command. The batch endpoint is present, but the
engine's output path still has these per-event costs.

**Next optimization.** Use the fast paths that already exist, then measure:

- Switch the engine and MD runners from `read` to `read_payloads`.
- Use `append_owned` where the caller can give up the messages.
- Pre-encode or cache header values, not just header keys.
- Move the match-latency histogram record to the batch boundary, or use a
  per-shard histogram that the shard task owns without a mutex.
- Consider bounded commit coalescing (commit every K batches or every T ms).
  This reduces round trips but widens the replay window on a crash, so it needs
  a failure-mode review before it ships.

**Expected effect.** Lower CPU and allocation per event; fewer broker round
trips per batch. Commit coalescing trades a wider at-least-once replay window
for fewer commits. It does not change the matching semantics.

**What would falsify it.** If a profile shows the match or the MD hub dominating
after batch ingress lands, move the engine or MD work up. The journal edge is
the first suspect because it is the only place with awaited I/O per batch.

### Rank 3 — Engine best-price lookup and hash maps

**Evidence.** The book has a contiguous ladder, an occupancy bitmap, and a FIFO
arena. Before the cache, `best_index` scanned bitmap words from the best end on
every call. The BTC-USD ladder is 50,001 levels (782 `u64` words); ETH-USD and
SOL-USD are 30,001 levels (469 words). `process` calls `best_index` for the
cross check and again for each fill. FOK's `executable_qty_excluding` walks
every resting order at every crossing level. Before the change, the order index
and dedup table used the default SipHash hasher.

The working tree now has a cached best bid/ask index with a bitmap fallback,
`FxHashMap` in the book and dedup table, and a criterion bench at
`crates/whippet-engine/benches/engine.rs`. A same-period A/B on a loaded host
(old sources stashed vs. new) measured:

| Bench | Before | After | Change |
|---|---:|---:|---:|
| `book_insert_cancel/1m_ops` | 15.14 ms | 8.69 ms | −43% |
| `best_price_lookup/deep_book_bid_ask` | 37.3 ns | 0.312 ns | −99% |
| `aggressive_burst/10k_ioc` | 1.344 ms | 515 µs | −62% |
| `engine_state_hash/20k_orders_20k_dedup` | 2.833 ms | 2.560 ms | −10.5% |

The host was running other builds (load average 4–9), so these are directionally
strong but not headline numbers. The best-price win is algorithmic and
reproduced in every run; the insert/burst wins come from `FxHashMap`.

**Next optimization.** The dedup table now has a terminal-eviction queue instead
of a bounded insertion-order scan. If FOK shows up in a profile, consider a
per-level aggregate that subtracts the excluded participant's quantity instead
of walking every order. If snapshot pauses show up, move snapshot construction
off the engine task and reduce the state-snapshot cadence further.

**Expected effect.** Lower match latency and lower CPU per command. The
end-to-end durable path does not improve until rank 1 and rank 2 land, because
the matcher is not the queue.

**What would falsify it.** If match p99 is already a small fraction of the
end-to-end envelope, stop optimizing the matcher and spend the time on the
ingress or MD path.

### Rank 4 — MD hub lock, clock reads, and per-subscriber serialization

**Evidence.** `MdHub::apply` takes one global `Mutex<HubState>` per MD event and
calls `now_ns()` per event. `build_publish_state` builds every symbol view and
clones the 250-trade tape on each tick. `Metrics::snapshot` locks the ingress
histogram, the match histogram, and the rate estimator. Each WebSocket task
deep-clones the `StateView` and runs `serde_json::to_string` independently.
`GET /api/state`, `GET /api/trades`, and `GET /metrics` rebuild a frame instead
of reading the last one.

The working tree now has a `latest()` accessor, a dirty flag with a 45 ms
coalescing window, and a `metrics_snapshot()` path. `/api/state` and `/metrics`
use `latest()`; the WebSocket loop still clones and serializes per subscriber.
Measured on the same loaded host: the 3-symbol × 20-level × 250-trade state
build was ~753 ns before and ~734 ns after (noise-level); `/api/state` via
`latest()` is ~6.9 ns (an `Arc` clone), and metrics-only reads are ~109 ns
instead of a full frame build.

**Next optimization.** Build one serialized frame per tick and broadcast the
bytes to subscribers, and filter by `WsClientMessage::Subscribe`. Move the
clock read to the tick. If the hub lock shows contention with three MD
consumers, shard it by symbol or use an `RwLock`.

**Expected effect.** Serialization becomes O(1) per tick instead of O(N); egress
remains O(N). This matters at 10+ subscribers and at three or more active MD
partitions. At one subscriber it is not the knee.

**What would falsify it.** If a fanout run at 1, 10, and 100 subscribers shows
flat CPU and flat per-frame cost, leave the hub alone until the subscriber count
grows.

### Rank 5 — TCP/binary ingress and background producer batching (later)

**Evidence.** The only ingress is HTTP JSON. The manifest's `payload_bytes` is a
fixed 96-byte label, not a measured request size. The manifest notes that the
intended Linux headline path is TCP on a native host, but that path does not
exist.

**Next optimization.** Only after ranks 1–4 and a native Linux runner: add a
binary TCP or QUIC ingress that writes the fixed 78-byte `Command` directly, and
a background producer that batches appends. This is a new protocol surface with
new framing, auth, backpressure, and failure modes.

**Expected effect.** Removes JSON parsing and HTTP overhead, and amortizes
appends in the background. Do not claim a number: the current evidence points at
the broker round trip, not the JSON parser, so TCP may help less than expected
until the append path is fixed.

**What would falsify it.** If batch ingress already reaches the broker or disk
limit, TCP is not the next bottleneck. Measure first.

## Benchmark matrix: before and after

Every row needs a before run, an after run, the same host, the same seed, the
same stack, and a manifest. A number without a manifest is an anecdote, per
[BENCHMARKS.md](BENCHMARKS.md#publishing-a-run). The repository's publication
rules are in [benchmarks/README.md](benchmarks/README.md).

| ID | Question | Command | Before artifact | After artifact | Status |
|---|---|---|---|---|---|
| L1-single | Did the single-order durable path regress? | `cargo run --release -p whippet -- bench --gateway http://127.0.0.1:8080 --rate 1000 --duration 30 --warmup 5 --repetitions 5 --label L1-single` | Baseline manifest from the pre-change commit; local `artifacts/bench/<run-id>.json` | `artifacts/bench/<run-id>.json` with `workload.batch_size = 1`; publish under `docs/benchmarks/<date>-<host>-<scope>-l1-single/` | Harness supports it |
| L1-batch-1 | Does a batch of one match the single-order path? | Same as L1-single with `--batch-size 1 --label L1-batch-1` | L1-single manifest | Manifest with `workload.batch_size = 1` and the batch route | Supported; dev-loop measured above |
| L1-batch-10 | Does ten orders per append move the knee? | Same with `--batch-size 10 --label L1-batch-10` | L1-single manifest | Manifest with `workload.batch_size = 10`; report orders/s, not requests/s | Supported; dev-loop measured above |
| L1-batch-100 | Does one hundred orders per append saturate the broker differently? | Same with `--batch-size 100 --label L1-batch-100` | L1-single manifest | Manifest with `workload.batch_size = 100` | Supported; dev-loop measured above |
| L1-batch-1000 | Where does the batch path itself break? | Same with `--batch-size 1000 --label L1-batch-1000` (or the cap, if the route caps at 256) | L1-single manifest | Manifest with the actual batch size and the cap documented | Blocked: the gateway caps a batch at 256 |
| Engine-only | Did the best-price cache and `FxHash` help the match loop? | `cargo bench -p whippet-engine --bench engine -- --save-baseline before` (before) then `cargo bench -p whippet-engine --bench engine -- --baseline before` (after) | `target/criterion/<group>/<bench>/estimates.json` on the pre-change commit | Same paths on the changed commit; keep both baselines | Bench target exists |
| MD apply/build | Did the hub read path help? | `cargo bench -p whippet-marketdata --bench md -- --save-baseline before` then `--baseline before` | Criterion estimates for `hub_apply` and `hub_state_view` | Same paths on the changed commit | Bench target exists |
| Fanout-1 | What does one subscriber cost? | `cargo run --release -p whippet -- bench --fanout 1 --rate 10000 --duration 60 --warmup 5 --repetitions 5 --label fanout-1` (proposed) | Manifest with `workload.subscribers = 1`, `/metrics` scrape, frames/s and bytes/s | Same on the changed commit | Not implemented: no fanout workload, `subscribers` is 0 |
| Fanout-10 | Does serialization scale with subscribers? | Same with `--fanout 10 --label fanout-10` | Manifest with `workload.subscribers = 10` and egress counters | Same on the changed commit | Not implemented |
| Fanout-100 | Where is the MD-side knee? | Same with `--fanout 100 --label fanout-100` | Manifest with `workload.subscribers = 100` and egress counters | Same on the changed commit | Not implemented |

Notes that make the matrix honest:

- **L1-single is a rate sweep, not one run.** Run 200, 1,000, 2,000, and 5,000
  orders/s; each rate is a separate manifest and a separate before/after pair.
  The baseline table above is that sweep for the "before" side.
- **A batch is one HTTP request.** The harness's `latency` is the batch ack
  latency, not per-order latency. Report `orders_per_second = requests_per_second
  × batch_size` and label the latency column "batch ack". If per-order latency
  matters, add per-order timestamps to the request and response; do not relabel
  the batch sample.
- **The gateway caps a batch at 256.** A "1,000" row is either a planned cap
  increase or a documented rejection. Do not silently send 1,000 and report the
  error count as throughput.
- **The engine bench is a microbench, not a system bench.** It answers "did the
  match loop get faster", not "did the exchange get faster". Keep the two
  results separate.
- **The fanout row needs a WebSocket load generator.** The current harness is
  HTTP-only. A `whippet fanout` tool or a `--fanout` mode must open N sockets,
  count frames and bytes, and write a manifest with `subscribers = N`. Until
  that exists, the fanout model is untested.
- **Every run needs a method card.** Use the template in
  [benchmarks/README.md](benchmarks/README.md#method-card-template). State the
  host, runtime, commit, image digest, rate, duration, repetitions, durability,
  batch size, and encryption.
- **Before and after are a pair.** Run them back-to-back on the same host, with
  the simulator stopped, and keep both manifests. Comparing a new run to a
  week-old run is not a before/after.

## What not to optimize yet

| Area | Why not now | What would change the answer |
|---|---|---|
| Cluster consensus (VSR, 3-node HA) | Not implemented. It changes the durability model and adds a replication round trip. It is Phase 3, and it is not the measured knee. | A measured single-node path that is stable at the target rate, plus a Phase 3 requirement for failover. |
| Dashboard rendering | The browser renders a 20 Hz full-state frame. It cannot backpressure the engine: the hub uses a conflated `watch` channel and slow clients miss frames. Browser rendering is not on the durable path. | A measured browser-side frame cost that makes the UI the bottleneck. The server-side per-subscriber serialization is a separate row and is ranked 4. |
| TLS | Phase 2 security work. It adds handshake and per-byte crypto at the edge, but the baseline knee is the broker append, not the TLS record path. There is no TLS path to measure yet. | A measured ingress path where the append is no longer dominant, or a security requirement that makes TLS mandatory before the next run. |
| Stream encryption (AES-256-GCM-SIV) | Phase 2, not implemented. Per-message crypto would add cost, but it is not on the current path and there is no measurement. | An encryption implementation and a before/after run that isolates its per-byte cost. |
| JSON parser or SIMD JSON | No profile evidence. The append round trip is the prime suspect, and the JSON body is small. Replacing the parser before stage timers exist is guessing. | A stage timer that shows JSON parse is a material fraction of the request. |
| Snapshot format or BLAKE3 state hash | Correctness tools, not per-order costs. Snapshot build is periodic and can own a tail sample, but the first fix is to measure it, not to replace the format. | A p99 sample that correlates with the snapshot interval, or a snapshot build that blocks the shard for a visible duration. |
| CPU pinning, NUMA, hugepages | Dev-loop cannot show them, and Phase 1 explicitly does not pin. They belong to a native Linux headline run. | A native Linux runner with a quiet host and a manifest that records governor, SMT, and isolated cores. |
| Iggy broker configuration tuning (memory pool, sharding, WAL) | A tested profile regressed durable ingress on the 16 GB Docker Desktop VM. The log was large, so it is a strong signal, not a clean A/B. The default stack keeps Iggy's default tuning. | A clean-log, same-host A/B with batch ingress already in place, and a profile that beats the defaults at the target rate. |
| The in-memory journal | A test and developer escape hatch, not the demo path. Optimizing it would not improve a published run. | A decision to use the memory journal for a supported workload. |

### Measured configuration note: Iggy 0.10 profile

On the 16 GB Docker Desktop VM, an Iggy 0.10 profile was tested with:

```text
IGGY_MEMORY_POOL_SIZE=1GiB
IGGY_SHARDING_CPU_ALLOCATION=4
IGGY_SHARDING_PIN_CORES=false
IGGY_SHARDING_PARTITION_IO_CAPACITY=32
IGGY_PARTITION_WAL_BYTES_MAX=136MiB
```

At roughly 1,000 orders/s, durable ingress measured p50 ~63 ms and p99 ~574 ms.
The default Iggy configuration, in the no-snapshot isolation run, measured p50
~1.2 ms and p99 ~120 ms. That isolation run is separate from the four-rate
baseline above. The log was also large, so this is a strong signal, not a clean
A/B: the run does not isolate the profile from log size, segment state, or host
noise.

The default Compose stack keeps Iggy's default tuning; the optional profile is
documented in [deploy/iggy/iggy.toml](../deploy/iggy/iggy.toml) and is not the
default. No committed manifest accompanies this note; it is an operator
observation, not a publishable run. Do not present these numbers as a headline
result. Do not tune the broker before batch ingress and the journal fast paths;
revisit it with a clean log and a controlled A/B.

## How to profile

### Build for profiling

The release profile already has `debug = 1`, `lto = "thin"`,
`codegen-units = 1`, and `panic = "abort"`. Build with:

```sh
cargo build --release -p whippet
```

`debug = 1` gives line information in profiles. `panic = "abort"` means some
unwinding-based tools cannot capture a backtrace, but CPU profilers still work.
`lto = "thin"` inlines across crates, so a flamegraph may attribute a cost to the
caller rather than the inlined callee. Keep that in mind when reading frames.

### CPU profiles

**`cargo flamegraph`** is the quickest CPU profile:

```sh
cargo install flamegraph   # once
cargo flamegraph --release -p whippet -- node
```

On Linux it uses `perf`; on macOS it uses `dtrace`, which may require additional
permissions. The output is an SVG flamegraph. Profile a release build, not the
dev profile, and keep the SVG next to the run if it supports a conclusion.

**`samply`** is a good macOS/Linux alternative that opens the Firefox Profiler:

```sh
samply record -- ./target/release/whippet node
```

It shows CPU profiles and can show off-CPU time depending on the platform. It is
useful when the question is "where did the thread go" rather than "which
function is hot".

**`perf`** is Linux-only:

```sh
perf record -F 999 -g -- ./target/release/whippet node
perf report
```

Check `kernel.perf_event_paranoid`; a restrictive setting hides kernel and
some user samples. `perf stat` answers a different question: cycles,
instructions, cache misses, and context switches per unit of work. Use it to
compare two builds, not to find a call stack.

**`pprof`** is not a dependency today. Add the `pprof` crate behind a
`profiling` feature, start a guard around the node loop, and generate a
flamegraph on a signal or at shutdown. Do not ship the feature in the default
build. `cargo pprof` is an option if it is installed, but the in-process crate
is the reproducible route.

### Async runtime

**`tokio-console`** is a development-only tool. It is not wired into this
crate today; add a `tokio-console` feature that installs `console-subscriber`
and initializes it in `main`, then run:

```sh
RUSTFLAGS="--cfg tokio_unstable" cargo run --features tokio-console -p whippet -- node
tokio-console http://127.0.0.1:6669
```

It requires `console-subscriber` and a `tokio_unstable` build. It shows task
poll times, wakeups, and scheduling, which is exactly what the engine and MD
tasks need. It also adds instrumentation overhead and exposes internals: never
enable it in a release or production build, and never quote a latency number
from a tokio-console build.

### Microbenchmarks

The engine bench exists:

```sh
cargo bench -p whippet-engine --bench engine
```

Criterion writes estimates and reports under `target/criterion/`. Use named
baselines for a before/after:

```sh
cargo bench -p whippet-engine --bench engine -- --save-baseline before
# change one thing
cargo bench -p whippet-engine --bench engine -- --baseline before
```

The groups are `book_insert_cancel`, `best_price_lookup`, and
`aggressive_burst`. They use preallocated storage so the numbers reflect the
match loop rather than allocator noise.

The MD hub already has `cargo bench -p whippet-marketdata --bench md`; add the same pattern for fanout
frame serialization. A criterion bench is not a system benchmark; it answers a
narrow question and it should say so in its name.

### Distributions and scheduler slip

`whippet bench` already records HDR histograms with three significant figures
and a 60-second maximum, and it records scheduler slip separately. Read both:

```sh
python3 -m json.tool artifacts/bench/<run-id>.json | less
```

If `throughput.scheduler_slip.p99` is comparable to `latency.p99`, the run is
generator-bound. Fix the generator or lower the rate before interpreting the
latency tail. The baseline's 200/s run is a good example: its slip p50 is
1,482 µs, so most of its p50 is the pacer, not the exchange.

The harness does not export raw `.hdr` files. If a conclusion needs the raw
distribution, add an export and put it under `raw/`, with the histogram bounds
and significant digits in the method card. Do not average percentiles across
runs; the manifest keeps every repetition for a reason.

`/metrics` exposes cumulative process-lifetime percentiles. They are useful for
a live view and for a restart-relative comparison, not for a benchmark. The
harness histograms and the server histograms are separate; do not mix them.

### Allocations, syscalls, and off-CPU time

- **Allocations:** use `heaptrack` on Linux, `dhat`/Valgrind, or a counting
  global allocator behind a feature. Count allocations per order and per event.
  The expected hot spots are `MessageMeta::pairs`, the payload `Vec` in
  `execution_message`/`md_message`, and the header `String`s in the journal
  append path.
- **Syscalls:** `strace -c -f -p <pid>` for a short window confirms the append
  and commit round trips. Do not run it continuously; it changes the timing.
- **Off-CPU:** `perf sched`, `bpftrace`, or `samply` can show whether a task is
  waiting on the broker, the runtime, or the filesystem. A CPU flamegraph alone
  will not show a task that is blocked on I/O.
- **Network:** `ss -ti` and `tcpdump` can confirm Nagle is off (the client uses
  `with_no_delay`) and show round-trip time. Do not infer broker time from a
  packet capture without matching it to the request.

### Pinning and isolation (native Linux headline only)

For a headline run, pin the node and the generator to different cores, set the
governor to `performance`, and record the isolation in the manifest. Phase 1
does not do this; a dev-loop run with `taskset` is still `dev-loop`.

### Stage instrumentation

The current manifest has no stage timestamps. To get them, add `tracing` spans
or per-stage HDR histograms at these boundaries:

- gateway: parse, validate, encode, append, response;
- engine: poll return, decode, process, encode, append executions, append MD,
  commit;
- MD: poll return, decode, apply, build, serialize, send.

Record them at batch boundaries, not inside `SymbolEngine::process`. Do not add
a clock read to the match loop to measure the match loop; measure around it.
Populate the manifest's `stages` array only when the timestamps exist. An empty
array is honest; a fabricated one is not.

## Guardrails

A performance change is admissible only if it preserves the correctness story:

- `cargo test --workspace` passes, including the engine property and golden
  tests.
- `whippet verify --snapshot-dir data/snapshots --partitions 16` recomputes the
  snapshot hashes.
- `whippet replay --expect-hash <hex>` reproduces the state hash for a known log.
- The state hash for the same input log is unchanged, unless the change is an
  intentional state-format change with a migration note and a new ADR.
- The at-least-once contract is unchanged. If commit coalescing or batching
  widens the replay window, document the new duplicate window in
  [FAILURE-MODES.md](FAILURE-MODES.md).
- The topic layout and partition keying are unchanged. A change there is a data
  layout change, not a performance change.

If a change cannot show a before/after manifest, it is not yet a performance
result. It may still be a correctness or maintainability improvement; label it
as such.

## Related documents

- [BENCHMARKS.md](BENCHMARKS.md): method, run matrix, scope labels, the smoke
  run.
- [BENCHMARK-MANIFEST.md](BENCHMARK-MANIFEST.md): every manifest field.
- [benchmarks/README.md](benchmarks/README.md): how to publish a run without
  hand-editing numbers.
- [ARCHITECTURE.md](ARCHITECTURE.md): the data flow, the engine, the MD hub, and
  the recovery model.
- [FAILURE-MODES.md](FAILURE-MODES.md): what a crash or a replay does to the
  at-least-once contract.
- [OPERATIONS.md](OPERATIONS.md): configuration, retention, metrics, and the
  single-node caveats.
- [adr/0003-matching-engine-data-structures.md](adr/0003-matching-engine-data-structures.md):
  the ladder, bitmap, FIFO, and dedup choices.
- [adr/0004-iggy-topics-partitions-durability.md](adr/0004-iggy-topics-partitions-durability.md):
  the topic layout and durability policy.
- [adr/0006-benchmark-protocol.md](adr/0006-benchmark-protocol.md): the
  open-loop, deadline-based measurement protocol.
- [adr/0009-performance-budget-and-optimization-order.md](adr/0009-performance-budget-and-optimization-order.md):
  the decision this document implements.
