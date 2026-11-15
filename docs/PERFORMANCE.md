# Performance

> Legend: every measurement table states its class (`microbench`, `smoke`,
> `soak`, `dev-loop`, `headline`), repetition count, and environment.
> Structural matrices (stage budget, hot-path rules, bottleneck ranks, run
> matrix) are plans and models, not measurements.

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

*Class: `smoke`; repetitions: 1; environment: Apple M4, Docker Desktop.*

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

*Class: `dev-loop` (regression baseline); repetitions: 1 per rate; environment: Docker Desktop, simulator stopped.*

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

*Class: `dev-loop`; repetitions: 1 per row; environment: dev-loop host, simulator stopped, fresh stack.*

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


### Sprint A tranche (measured)

The next tranche attacks the remaining per-order wait and the warm paths:

| Change | Where | Expected effect | Status |
|---|---|---|---|
| Per-partition group commit | `whippet-journal::group`, gateway single-order and cancel, TCP submit | Concurrent single-order appends become one Iggy append per partition; removes the one-RTT-per-order wait at moderate concurrency | Measured below |
| Binary TCP ingress | `whippet-core::tcp`, `whippet::tcp` | Removes JSON parse and HTTP framing from the throughput path; fixed 78-byte commands | Measured below |
| MD batch apply | `MdHub::apply_batch` | One hub lock and one clock read per poll batch instead of per event | 2.49× batch apply in the criterion bench (loaded host) |
| WS serialize once | gateway `FrameCache`, `Subscribe`, `?trades=N` | One serialization per `(filter, tick)` instead of per subscriber; smaller tape | Implemented; fanout benchmark pending |
| Routing headers | `MessageMeta::routing_pairs` | Executions/MD carry four header pairs instead of seven | Implemented |
| Snapshot writer pipeline | `SnapshotWriter`, sequenced snapshots | Snapshot serialization/fsync leaves the shard loop; stale writes rejected | Implemented; 15 s cadence, soak measured |
| Decoupled offset commit | `CommitWatermark` | Commit no longer blocks every poll; window is 256 records or 100 ms | Replay proof rerun below |

Final sweep on the new tree (dev-loop, Docker Desktop, simulator stopped, one
repetition, 8 s measured, 2 s warmup). Every row has 0 errors.

*Class: `dev-loop`; repetitions: 1 per row; environment: Docker Desktop, simulator stopped.*

| Rate | Batch | p50 | p99 | p99.9 | Before (p50) |
|---:|---:|---:|---:|---:|---:|
| 1,000/s | 1 | 538 µs | 9,567 µs | 20,335 µs | 543 µs |
| 1,000/s | 10 | 3,509 µs | 16,831 µs | 36,447 µs | 2,501 µs |
| 1,000/s | 100 | 9,839 µs | 17,695 µs | 17,855 µs | 15,295 µs |
| 1,000/s | 256 | 13,127 µs | 20,511 µs | 22,655 µs | 54,175 µs |
| 5,000/s | 1 | 2,030 µs | 28,159 µs | 67,647 µs | 838,143 µs |
| 5,000/s | 10 | 1,054 µs | 9,799 µs | 27,487 µs | 1,762 µs |
| 5,000/s | 100 | 4,363 µs | 246,399 µs | 325,375 µs | 8,231 µs |
| 5,000/s | 256 | 7,779 µs | 14,671 µs | 19,519 µs | 19,407 µs |

The `5,000/s batch 100` p99 is a one-repetition outlier, not a trend. The
harness now also records scheduler slip, so `service = latency - slip` is
available per sample. On the final sweep:

*Same runs as the sweep table above: class `dev-loop`; repetitions: 1; environment: Docker Desktop.*

| Rate | Batch | service p50 | service p99 | slip p50 |
|---:|---:|---:|---:|---:|
| 1,000/s | 1 | 444 µs | 9,416 µs | 94 µs |
| 5,000/s | 1 | 1,969 µs | 22,760 µs | 61 µs |
| 5,000/s | 10 | 985 µs | 9,177 µs | 69 µs |
| 5,000/s | 256 | 6,989 µs | 10,540 µs | 790 µs |

At 5,000/s batch 1 the pacer contributes ~5.4 ms of the 28 ms p99; the rest is
the server, dominated by the Iggy append latency distribution on Docker
Desktop. The 1,000/s run is labeled `generator-bound`; treat its p50 as an
upper bound on the server.

TCP (same host, `--transport tcp`):

*Class: `dev-loop`; repetitions: 1 per row; environment: Docker Desktop, same host as the sweep above.*

| Path | p50 | p99 | Errors |
|---|---:|---:|---:|
| Batch 1, 5,000/s, 32 connections | 1,722 µs | 49,311 µs | 0 |
| Batch 10, 5,000/s | 799 µs | 17,103 µs | 0 |
| Batch 10, 10,000/s | 598 µs | 10,647 µs | 0 |

A later run on the same tree measured TCP batch 10 at 10,000/s at p50 473 µs
and p99 24,079 µs; the spread between repetitions is host noise, not a code
change.

Group commit coalesced up to 78 messages per append in the 5,000/s
single-order run (62,155 messages in 33,240 appends). The 5,000/s
single-order path was p50 838 ms with 40k errors before the tranche.

The 5,000/s single-order path was p50 838 ms with 40k errors before group
commit. The replay proof was rerun after the tranche: full and snapshot-seeded
replay both end at `c2b9838ba8438245697c93b5396113cbeb38b3a136ae174842f11b1e39536739`
from snapshot offset 158548 (tail 271).

### Phase 1.5c (implemented; dev-loop measured)

Phase 1.5c continues the order fixed by
[ADR 0009](adr/0009-performance-budget-and-optimization-order.md) and is
specified — and now implemented — in
[ADR 0010](adr/0010-shard-thread-and-journal-pipeline.md). It has four
workstreams. The microbenchmarks measured with this tranche are listed below;
the dev-loop rows below are a first before/after, one repetition each, and the
p50 is within host noise while the tail moved.

| Workstream | Change | Expected effect | Status |
|---|---|---|---|
| Shard thread and rings | One dedicated OS thread (`whippet-shard-<id>`) per active shard; bounded input ring (reader → matcher) and output ring (matcher → writer), capacity 2, plus a recycle channel that returns the polled buffer to the reader; the matcher does no I/O and no await; the reader/writer tasks own the journal edge | Removes the runtime migration and interleaving jitter from the match path; bounds in-flight memory per shard; backpressure when a ring fills instead of unbounded queueing | Implemented ([ADR 0010](adr/0010-shard-thread-and-journal-pipeline.md)); end-to-end sweep pending |
| Snapshot hash | The matcher calls `snapshot_state_without_hash_compacted` (compacted walk, hashes zeroed); the writer fills the hashes and verifies the final snapshot by rebuilding. The terminal-eviction queue is hashed with the live ids in eviction order. Snapshots are gated by `appended_offset`; cadence is 30 s (5–300). | Removes the hash from the match path; the final snapshot cannot claim a bad hash. The earlier scale mismatch was a dedup-count bug, now fixed and pinned by a 100-engine × 200k test | Implemented |
| Per-shard metrics | Each matcher records into its own lock-free `ShardMetrics` (counters + local histogram), merged into the global `Metrics` once per second and on exit | Removes one `Mutex<Histogram>` lock per command from every shard's serial path | Implemented; merge cost measured below |
| Gateway/journal allocation pass | Per-event allocations on the edges: `execution_message`/`md_message` payload `Vec` + header `String`s remain the target; the reader's per-batch read `Vec` is now recycled | Lower CPU and allocation per event; fewer transient allocations per output | Partially implemented (recycle channel); message-construction pass pending |

Measured with this tranche (criterion, loaded host — directionally useful, not
headline numbers):

*Class: `microbench`; repetitions: criterion defaults (unspecified — not reproducible as published); environment: loaded shared host.*

- **Off-loop hash cost.** `hash_symbol_snapshot` measures ~2.51 ms on a book
  of 20,000 orders plus a 20,000-entry dedup table. That is the work the match
  path used to pay on the snapshot interval and now pays on the blocking
  snapshot writer instead.
- **Per-shard metrics merge.** Merging a shard's histogram into the global
  `Metrics` costs ~7.4 µs per 256-sample merge, paid once per second per
  matcher instead of one lock acquisition per command.

Dev-loop after the shard thread (Docker Desktop, simulator stopped, 8 s
measured, 2 s warmup, one repetition). Before is the final Sprint A sweep on
the same host:

*Class: `dev-loop`; repetitions: 1 per row; environment: Docker Desktop, same host.*

| Path | Before p50 / p99 | After p50 / p99 |
|---|---:|---:|
| HTTP single order, 5,000/s | 2,030 / 28,159 µs | 1,772 / 4,299 µs |
| HTTP batch 10, 5,000/s | 1,054 / 9,799 µs | 1,127 / 3,657 µs |
| TCP batch 10, 10,000/s | 598 / 10,647 µs | 527 / 3,921 µs |

The p50 differences are inside one-repetition host noise; the p99 collapse is
the shard thread plus the per-shard metrics. These
are not headline numbers and the Linux matrix is still the publishable
artifact. Per [ADR 0009](adr/0009-performance-budget-and-optimization-order.md),
a change without a manifest is not a performance result; the manifests are in
`artifacts/bench` and are local.

### Phase 1.5e (implemented; dev-loop measured)

Phase 1.5e separates the ingress and engine Iggy clients, shares one sharded
rate limiter across HTTP and TCP, caps WebSocket connections, verifies the
final snapshot at write time, and moves the last stage sample off the matcher
thread.

Snapshot stage split at 170k commands / 68k dedup entries (targeted release
bench, shared host, v3 format):

*Class: `microbench` (targeted stage timing); repetitions: unspecified — not reproducible as published; environment: shared host.*

| Stage | Thread | Time |
|---|---|---:|
| `matcher_clone_with_hash` (old path) | shard | 5.55 ms |
| `matcher_clone_only` (new path) | shard | **0.28 ms** |
| `writer_hash` (`hash_symbol_snapshot`) | writer | 5.35 ms |
| `writer_serialize` (+fsync) | writer | **38.7 ms** |

The clone was the shard stall; the writer's serialize/fsync dominates the
periodic tail. At 225k commands the v3-era split was clone 0.4–0.6 ms, hash
8.1–8.3 ms, write 33.8–38.1 ms. Snapshot format v4 stores the terminal-eviction order as
`u32` indices into the dedup walk instead of full `OrderId`s, so the file is
smaller than v3; versions 3 and older are rejected. The terminal queue is part
of the state hash, and snapshots are taken from a compacted walk. The recovery
test's 225,000-command workload (95,000 terminal ids) writes a 7,940,220-byte
v4 file (measured with `cargo test --release -p whippet --test recovery`;
file size only, not timing). Serialize/fsync remains
the dominant snapshot cost.

Dev-loop soak after the split (fresh stack, 5,000/s, batch 10, 90 s): p50
**2.22 ms**, p99 **76.5 ms**, 0 errors over 450,000 orders. *(Class: `soak`
(short); repetitions: 1; environment: dev-loop host, fresh stack.)* The p99 is
the periodic snapshot path (clone/serialize/fsync), not matching; the off-loop
hash is restored and the cadence is 30 s. `engine_process` stays at 4 µs and
`md_apply` at 3 µs. A full-vs-seeded replay with a 33.9k-record tail matches
after the dedup terminal-queue fix. A 10-minute soak still fails the 5% drift
gate on Docker Desktop; the next step is a copy-on-write snapshot, then the
Linux run.

### Phase 1.5d (implemented)

Phase 1.5d is specified in
[ADR 0011](adr/0011-fault-injection-and-stage-budget.md). It does not chase a
new p50; it makes the current pipeline provable and attributable.

| Workstream | Change | Why | Status |
|---|---|---|---|
| Fault-injection harness | `JournalOps` seam plus a fake journal; tests for output-append failure, matcher panic, ring full, and reader gap/overlap | The shard pipeline's failure handling is code without a test; a regression could restore the silent "gateway acks, engine stops matching" mode | In progress |
| Message-construction allocation pass | Static header names and reused payload buffers for executions/MD | The remaining per-event allocation on the produce path | In progress |
| Stage histograms | `whippet_stage_p50_us{stage=...}` / `p99` for gateway parse/validate/encode/append, engine process/append/commit, MD apply, WS frame | The harness sees send/wire; the node does not expose where the durable p50 goes | In progress |
| Soak, crash, fanout-with-ingress | `make soak`, `make crash-test`, `--ingress-rate` on the fanout harness | Long-run drift, crash replay, and whether 100 subscribers slow the engine at 10k/s are all unmeasured | In progress |

No end-to-end numbers are claimed for this phase until the soak and the
Linux matrix run. The current measured rows above remain the last recorded
dev-loop results.

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
| 5 | Engine poll/decode | `read_orders` → `journal.read_payloads_into(Topic::Orders, 1024, 50 ms)` → `Command::decode` (in `run_matcher`) | Long poll with `min_count = 1`; up to 1024 records; payload-only read skips header TLV parse; one fixed-layout decode per record; the polled buffer is recycled via the recycle channel; the whole batch is offset-validated | unknown |
| 6 | Match | `ShardEngine::process` → `SymbolEngine::process` | Best-price lookup; order-index and dedup hash lookups; FIFO walk per fill; FOK walks every resting order at crossing levels; output `Vec` pushes | unknown; the live `whippet_match_latency_microseconds` gauge exists, but no published value |
| 7 | Execution / MD encode | `execution_message`, `md_message`, `snapshot_message` in `engine_runner.rs` | 104-byte or 84-byte payload `Vec` plus seven header `String`s per event; a sweep can emit several events per order | estimated; 104 B and 84 B are documented in [ARCHITECTURE.md](ARCHITECTURE.md#protocol) |
| 8 | Iggy append (engine output) | `run_writer` → `append_with_retry` for `executions` and `md` in parallel (bounded, 30 attempts), then the watermark commit | Two joined appends per batch plus one coalesced offset commit; cached header keys; 256-message chunks; on retry exhaustion the shard stops and its offset does not advance | unknown; structurally two parallel round trips plus one commit per batch |
| 9 | MD consume | `run_md_shard` → `journal.read_payloads(Topic::Md, 1024, 100 ms)` → `MdHub::apply` | Payload-only read; one global hub `Mutex` acquisition and one clock read per event; `BTreeMap`/`HashMap` updates; one commit per batch | unknown |
| 10 | 20 Hz state build | `MdHub::spawn_publisher` → `publish` → `build_publish_state` | Full `BookView` build for each symbol (20 levels per side plus depth folds), 250-trade tape clone, metrics snapshot (three locks), `watch` replace; `/api/state` and `/metrics` now read the published `Arc` instead of rebuilding | ~0.73 µs build; ~6.9 ns `latest()` read; 20 Hz documented in [ARCHITECTURE.md](ARCHITECTURE.md#market-data) |
| 11 | WS / JSON delivery | `ws_loop` → `FrameCache` entry for `(symbols, trade-limit)` → refcounted buffer send → text frame | One serialization per distinct `(filter, tick)` per tick, shared to subscribers as a refcounted buffer; the cache holds at most 16 distinct frames per tick; uncompressed text; 64 KiB frame cap | unknown; structurally one serialization per distinct filter per tick plus O(subscribers) egress |

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
  → FrameCache: one serde_json::to_string per distinct (filter, tick), capped at 16 entries
  → N WebSocket tasks
  → N × (refcounted buffer send + text frame write)
```

The `watch` channel is the isolation mechanism: a slow browser misses
intermediate frames, and the engine never waits for it. The cost model is:

```text
serializations per second = 20 × distinct (filter, tick) pairs  (≤ 16 per tick)
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
| No allocation in the match loop | `SymbolEngine::process`, `Book` operations | Holds in steady state. Output vectors are caller-owned and reused; the arena, free list, and index are preallocated. | The snapshot-interval state clone and live `state_hash` still allocate and run between batches on the matcher thread, not per command. Journal-edge message construction allocates per event: `MessageMeta::pairs` (7 `String`s), `execution_message`/`md_message` (payload `Vec` + 7 `String`s), `build_order`. The reader's polled buffer is recycled, not reallocated per batch. |
| No syscall in the match loop | `SymbolEngine::process` | Holds. The matcher performs no I/O. | The matcher thread reads the clock once per record for the latency sample (`Instant::now()` in `run_matcher`) and the hub reads it once per MD event (`now_ns()` in `MdHub::apply`). On Linux these are vDSO clock reads, not syscalls, but they are still work on the serial path. |
| No lock in the match loop | `SymbolEngine::process` | Holds for the per-command path. The matcher thread owns each partition's books; the engine crate has no lock. | Match latency and counters go into the per-shard lock-free `ShardMetrics`, merged into the global `Metrics` once per second. One `EngineProcess` stage sample per batch still takes the global stage-histogram mutex, so the rule is per batch, not per command. `MdHub::apply_batch` takes the hub mutex once per poll batch. |
| No async in the engine | `whippet-engine` | Holds. The crate has no Tokio dependency, and the matcher thread has no async context at all. | The async boundary lives in the `read_orders` and `run_writer` tasks around the matcher; the matcher never awaits. |
| Fixed headers and payload layout | Protocol and journal edge | Holds. Payloads are fixed-width little-endian with no varints, floats, or padding: 78 B command, 104 B execution, 84 B MD event. Orders and MD snapshots carry the full seven headers; executions and MD events carry four routing headers. The engine never parses headers. | The engine and MD runners use the payload-only `read_payloads` path, so no header TLV parse or `String` materialisation happens on the consume path. Header keys are cached; header values are still `String`s on the produce path. |
| Per-partition single writer | Matcher thread per active partition | Holds. One dedicated OS thread (`whippet-shard-<id>`) per active partition, no concurrent writer, no lock on the book. | The thread is not pinned to a core; the OS can still migrate it. Core pinning, busy-poll, and NUMA work remain later Linux work, but the rings (bounded channels, capacity 2) are in place. |

### Periodic costs that are not in the per-order budget

These do not show up in a steady-state per-order measurement, but they can own a
tail sample:

- **Snapshot build.** Every `WHIPPET_SNAPSHOT_EVERY` commands (default 10,000),
  the matcher calls `snapshot_state_without_hash_compacted()`, which compacts
  the dedup queues and clones the book and the dedup walk between batches with
  the hashes left zeroed; the blocking snapshot writer then fills the hashes
  off the match path and writes the file. The snapshot is
  gated by `appended_offset`, so it is only taken once every output up to the
  matcher's offset is in the journal. At the default dedup cap
  (`min(2 * max_orders, 100,000)` = 100,000 entries per symbol with the default
  `max_orders`), the clone is large; the hash that used to accompany it
  on the match path is ~2.51 ms on a 20k/20k book (measured above). It is a
  periodic tail-latency source, not a per-order cost.
- **Initial MD snapshot.** At startup, each shard publishes a full MD snapshot
  before it begins consuming. This is a startup cost, not a steady-state cost.
- **Health monitor.** Every five seconds the node pings the ingress client and
  every engine-pool client concurrently, caches one aggregate flag (which
  `/readyz` reads without journal I/O), and refreshes consumer lag with
  `committed` plus `lag` per active shard through that shard's pool client.
  That is several broker round trips every five seconds. It is small, but it
  is not free. The engine pool's total pending-operations budget is
  `WHIPPET_ENGINE_JOURNAL_PENDING_MIB` (default 256, clamped 16–4096), split
  as `max(16 MiB, total / pool_size)` per client.
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

**Measured and addressed.** Batch ingress, per-partition group commit, and the
binary TCP listener all landed and are measured above. The 5,000/s single-order
path went from a standing queue to p50 ~2 ms. The next optimization is the
dedicated shard thread with a bounded ring, which removes the remaining async
edge between the matcher and the journal:

- Gateway: `POST /api/orders/batch` and `POST /api/orders` both go through the
  per-partition group writer; a batch is one append, all-or-nothing validation,
  contiguous offsets.
- Harness: `--batch-size N` drives the batch body, `workload.batch_size` is
  recorded, and orders/s is reported separately from requests/s.
- Journal: the shard runners use `read_payloads`/`read_payloads_into`, the
  owned append path, and cached header keys; executions/MD carry routing
  headers.
- Engine loop: the shard is now the [ADR 0010](adr/0010-shard-thread-and-journal-pipeline.md)
  pipeline. The reader task polls into the input ring while the matcher works
  on the previous batch, and the writer's joined Executions/MD appends
  overlap the matcher's next batch, so matching and the broker round trips
  overlap without awaits on the match path.

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

**Addressed.** The runners use `read_payloads`; gateway and TCP use
`append_owned` (via group commit); executions/MD carry routing headers; the
commit is coalesced to 256 records or 100 ms with a documented replay window;
the output appends are joined in `run_writer` while the matcher proceeds on
the input ring.

**Done since.** The match-latency histogram is now per-shard (`ShardMetrics`,
lock-free, merged once per second), and the state-hash computation moved into
the snapshot writer, both measured in
[Phase 1.5c](#phase-15c-implemented-end-to-end-sweep-pending) above.

**Next optimization.** The remaining per-event costs are message construction:
`execution_message`/`md_message` still allocate a payload `Vec` and header
`String`s per event. A profile should decide whether that pass is next.

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
cross check and again for each fill. FOK's old `executable_qty_excluding` walked
every resting order at every crossing level; the engine now uses an early-exit
check. Before the change, the order index and dedup table used the default
SipHash hasher.

The working tree now has a cached best bid/ask index with a bitmap fallback,
`FxHashMap` in the book and dedup table, and a criterion bench at
`crates/whippet-engine/benches/engine.rs`. A same-period A/B on a loaded host
(old sources stashed vs. new) measured:

*Class: `microbench`; repetitions: criterion defaults (unspecified — not reproducible as published); environment: loaded host (load average 4–9).*

| Bench | Before | After | Change |
|---|---:|---:|---:|
| `book_insert_cancel/1m_ops` | 15.14 ms | 8.69 ms | −43% |
| `best_price_lookup/deep_book_bid_ask` | 37.3 ns | 0.312 ns | −99% |
| `aggressive_burst/10k_ioc` | 1.344 ms | 515 µs | −62% |
| `engine_state_hash/20k_orders_20k_dedup` | 2.833 ms | 2.560 ms | −10.5% |
| `fok_burst/check_only` (50k-order book) | 74.4 µs | 6.81 µs | −91% |
| `fok_burst/50k_book_10pct` (end to end) | 1.974 ms | 1.060 ms | −46% |

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

**Addressed.** `FrameCache` serializes one frame per `(filter, tick)` and every
subscriber clones a refcounted buffer; `Subscribe` filters symbols and
`?trades=N` caps the tape. `apply_batch` takes the hub lock once per poll.

**Next optimization.** Build a fanout workload (1/10/100 subscribers) and
measure frames/s and bytes/s. If the hub lock contends at 16 active shards,
shard it by symbol or use an `RwLock`.

**Expected effect.** Serialization becomes O(1) per tick instead of O(N); egress
remains O(N). This matters at 10+ subscribers and at three or more active MD
partitions. At one subscriber it is not the knee.

**What would falsify it.** If a fanout run at 1, 10, and 100 subscribers shows
flat CPU and flat per-frame cost, leave the hub alone until the subscriber count
grows.

### Rank 5 — TCP/binary ingress and background producer batching (later)

**Evidence.** HTTP JSON was the only ingress; the manifest's `payload_bytes` is
still a fixed label rather than a measured request size.

**Addressed in part.** Binary TCP ingress is implemented and measured (p50 598 µs
at 10,000 orders/s, batch 10). The remaining transport work is QUIC/TLS and a
multi-connection fan-in benchmark. Background producer batching is the
per-partition group commit.

**Expected effect.** TCP removes JSON parsing and HTTP framing; group commit
amortizes the append. The dev-loop result shows the batch shape matters more
than the transport: batch 10 amortizes the round trip, while batch 1 is limited
by one request in flight per connection.

**What would falsify it.** If the achieved rate is flat across transports at the
same batch size, the broker append rate is the limit and transport work is not
the next bottleneck. Measure on native Linux before claiming a transport win.

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
  HTTP and TCP ingress only. A `whippet fanout` tool or a `--fanout` mode must
  open N sockets, count frames and bytes, and write a manifest with
  `subscribers = N`. Until that exists, the fanout model is untested.
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
| Dashboard rendering | The browser renders a 20 Hz full-state frame. It cannot backpressure the engine: the hub uses a conflated `watch` channel and slow clients miss frames. Browser rendering is not on the durable path. The frame cache and per-row memo work are already in. | A measured browser-side frame cost that makes the UI the bottleneck. |
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
observation, not a publishable run. *(Class: operator observation, not a
result; repetitions: 1; environment: 16 GB Docker Desktop VM with a large
log.)* Do not present these numbers as a headline
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

`/metrics` exposes rolling-window latency percentiles. They are useful for
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

## Turbo mode (dev-loop, one run per mode)

Turbo is a different guarantee, not a faster durable path, so the two rows
are not interchangeable and the manifests record the mode. Both runs below use
the same 60 s soak shape on the same Docker Desktop host (3 active shards,
5,000 orders/s offered, 300,000 orders), one repetition each. They are
`dev-loop` smoke output, not a headline result.

*Class: `dev-loop` (`smoke`); repetitions: 1 per mode; environment: Docker Desktop, same host.*

| Mode | p50 | p99 | p99.9 | Offered | Errored | Ack point |
|---|---|---|---|---|---|---|
| durable | 1,173 µs | 30,991 µs | 184,831 µs | 300,000 | 0 | after the Iggy append |
| turbo | 256 µs | 3,495 µs | 229,887 µs | 300,000 | 2,230 | after the match (`matched_unjournaled`) |

Both summary p50s include the first sample, when the bench and node are still
warming; the steady 15 s samples were 861–879 µs p50 in durable and 231–390 µs
p50 in turbo. The durable row is the final build; the turbo row is the last
turbo run of the same coalesced writer. They are separate one-repetition runs
on a dev host, not a controlled A/B.

The turbo errors are not transport failures. They are HTTP 503 `window_full`
rejections: 1,052 commands hit the **age** bound
(`whippet_turbo_window_drops_total{reason="age"}`); the count bound never
bound. Goodput was 297,770/300,000. A burst that outlives the 50 ms window is
rejected rather than acked into a window that cannot be journaled in time;
clients retry. On this Docker Desktop host, scheduler/WAL stalls exceed 50 ms,
so the default bound is binding at 5,000 orders/s offered. A larger
`WHIPPET_TURBO_WINDOW_MS` trades a wider crash-loss window for fewer rejects;
the defaults are the guarantee, not a promise of zero backpressure. A native
Linux run is required before any claim that the bound is non-binding.

A single-order turbo smoke ack measured 88 µs. These are one-run observations;
no claim is made about a Linux headline or sub-millisecond end-to-end durable
latency. The differential test, not this table, is what proves turbo and
durable match identically.

## Rolling-window soak view (dev-loop)

The `/api/state` percentiles now include a rolling 10 s window, and the soak
summary records `window_p50_us`/`window_p99_us` per sample plus the disk-free
gauges. A 120 s durable run at 5,000 orders/s (batch 10, 12 samples) shows the
log-growth degradation without a restart:

| Metric | Value |
|---|---|
| Client-side request p99 | 667,135 µs |
| Server window p99, median across samples | 168,127 µs |
| Server window p99, max | 816,639 µs |
| Server window p50, median | 1,042 µs |
| Iggy volume free, start → end | 18.19 GB → 16.49 GB |

The window median is far above the 1–4 ms seen in the first minute of a fresh
run on the same host, so this is real drift, not a lifetime-histogram artifact.
The soak verdict is `skipped` because the drift gate requires at least 20
samples; it is a 2-minute dev-loop observation, not a sustained proof. Disk
consumption was about 14 MB/s at this offered rate, which is why the
bounded-storage guard and the retention design are Phase A work.

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
