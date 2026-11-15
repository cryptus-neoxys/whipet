# Benchmarks

The benchmark method is part of the product. A latency number without a host,
commit, configuration, sample count, and manifest is not a result.

## Status

There are **no headline Linux numbers** in this repository yet. Every recorded
run is a `dev-loop` smoke test on an M4 with Docker Desktop. They are included
because they are useful for regression and because hiding them would be worse
than labeling them. They are not benchmark results.

The harness now measures a mixed L1 order flow instead of a single passive
limit-order stream:

- resting limit orders, IOC orders that never rest, and cancels that reference
  real tracked order ids,
- a bounded per-symbol open-order working set, so a long run cannot fill the
  engine's order arena and silently turn into a book-full measurement,
- batched ingress (`POST /api/orders/batch`, up to 256 orders per request) with
  coordinated-omission-corrected batch latency,
- a one-second-window steady-state check reported in the manifest.

The primary experiment is the **batch-size sweep** (1 / 10 / 100 / 256 orders
per request). The dev-loop results are below; the native Linux sweep is still
pending. The gateway caps a batch at 256 because one batch is one Iggy append
and Iggy 0.10's default `message_bus.max_batch` is 256.

HTTP JSON is still the default transport. `--transport tcp` drives the binary
length-prefixed ingress instead, with per-frame stage timestamps. A final
dev-loop batch-size sweep, a group-commit measurement, and a TCP measurement
are recorded below; there are still no headline numbers. A 20 Hz WebSocket
fanout harness (`examples/fanout.rs`, `scripts/bench-fanout.sh`) measures `/ws`
egress across `N` concurrent subscribers, with an optional real-ingress driver
(`--ingress-rate`) and a no-subscriber baseline comparison; replay, failover,
and soak workloads are in the matrix with repeatable scripts
(`scripts/soak.sh`, `scripts/crash-test.sh`); the soak has dev-loop results below. The
run-matrix table says which rows the harness can execute today.

## Dev-loop result: final batch-size sweep (after)

Fresh stack, simulator stopped, mixed limit/IOC/cancel flow, `make bench-sweep`
at 1,000 and 5,000 orders/s, batches 1 / 10 / 100 / 256, 8 s measured, 2 s
warmup, one repetition, Docker Desktop on M4. All runs reported 0 errors. Each
batch's completion is recorded once per order, so p50 includes the wait for the
batch to fill.

```sh
WHIPPET_SWEEP_RATES=1000,5000 WHIPPET_SWEEP_BATCHES=1,10,100,256 \
WHIPPET_SWEEP_DURATION=8 WHIPPET_SWEEP_WARMUP=2 WHIPPET_SWEEP_REPETITIONS=1 \
make bench-sweep
```

| Rate | Batch | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 1,000/s | 1 | 538 µs | 9,567 µs | 20,335 µs |
| 1,000/s | 10 | 3,509 µs | 16,831 µs | 36,447 µs |
| 1,000/s | 100 | 9,839 µs | 17,695 µs | 17,855 µs |
| 1,000/s | 256 | 13,127 µs | 20,511 µs | 22,655 µs |
| 5,000/s | 1 | 2,030 µs | 28,159 µs | 67,647 µs |
| 5,000/s | 10 | 1,054 µs | 9,799 µs | 27,487 µs |
| 5,000/s | 100 | 4,363 µs | 246,399 µs | 325,375 µs |
| 5,000/s | 256 | 7,779 µs | 14,671 µs | 19,519 µs |

This sweep supersedes the earlier 10 s / 3 s warmup table. The key before/after
changes:

- **5,000/s, batch 1:** p50 838,143 µs with roughly 40k errors before group
  commit; now p50 2,030 µs, p99 28,159 µs, 0 errors.
- **5,000/s, batch 10:** p50 1,762 µs → 1,054 µs.
- **5,000/s, batch 256:** p50 19,407 µs → 7,779 µs.

At 1,000/s the single-order path is still the best p50 (538 µs), and p50 rises
with batch size because each order waits for a full batch. At 5,000/s batch 10
is the best p50 (1,054 µs); batch 256 trades a higher p50 (7,779 µs) for fewer
requests and lower per-order CPU.

The `5,000/s, batch 100` row has p99 246,399 µs and p99.9 325,375 µs while its
p50 is 4,363 µs. Treat that tail as a one-repetition outlier, likely snapshot
I/O or host noise on Docker Desktop, not a trend. An 8-second run cannot
resolve a p99 difference; the native Linux L1 run is the publishable artifact.

These are one-repetition dev-loop numbers on a shared laptop. They are not
headline results.

### After the dedicated shard thread (dev-loop)

Same host, simulator stopped, 8 s measured, 2 s warmup, one repetition. The
shard thread, off-loop snapshot hash, and per-shard metrics mainly moved the
tail; p50 is within host noise:

| Path | Before p50 / p99 | After p50 / p99 |
|---|---:|---:|
| HTTP single order, 5,000/s | 2,030 / 28,159 µs | 1,772 / 4,299 µs |
| HTTP batch 10, 5,000/s | 1,054 / 9,799 µs | 1,127 / 3,657 µs |
| TCP batch 10, 10,000/s | 598 / 10,647 µs | 527 / 3,921 µs |


## Method

**Open-loop pacing.** Request `i` is due at `t0 + i * period`. A dedicated
pacer thread sleeps until shortly before each deadline and spins the last
40 µs, then hands the scheduled tick to the request runtime. The spin never
runs on a Tokio worker, so it does not compete with the request tasks. The
generator does not wait for a response before scheduling the next request. A
slow system therefore builds a queue; it does not slow the generator.

**Generator-bound runs.** The generator records `wake_time - deadline` as
scheduler slip. If slip p99 is more than 5% of the measured p99, the run is
labelled `generator-bound` and the manifest note says the latency tail
includes generator lateness. That is a validity signal, not a result.

**Overload.** In-flight requests are capped at `--max-in-flight`. If the cap is
saturated, the slot is dropped as overload: it is **not** an error and it does
**not** produce a latency sample. The manifest records
`overloaded_slots`/`overloaded_orders`, `scheduled_samples` exceeds
`completed_samples` by the dropped count, and the run is labelled
`invalid-overload`. A run that drops slots is not a latency measurement.

**Latency from the deadline.** For each completed request:

```text
latency = completion_time - scheduled_deadline
```

This is the coordinated-omission correction. Measuring from the send would hide
time spent waiting before the request left.

**Batch requests.** With `--batch-size N` (`N > 1`) each request carries `N`
same-symbol orders to `/api/orders/batch`. Batches are paced at
`rate / N`, so the offered order rate is unchanged. When the batch completes,
the harness records `completion_time - deadline` **once per order** and counts
`N` completed samples. A batch whose response does not acknowledge exactly `N`
orders is counted as `N` errors, not `N` successes.

Cancels remain individual `POST /api/cancel` requests because the batch
endpoint accepts orders only. A slot's samples always add up to `N` order
events: orders go in the batch, cancels go as single requests.

**Binary TCP ingress.** `--transport tcp --tcp-addr HOST:PORT` (default address
`127.0.0.1:8081`) sends the same generated commands over the length-prefixed
binary frame instead of HTTP JSON. The frame layout is
`[u32 body_len][u8 kind][body]`; a `Submit` body is a `u16` command count
followed by fixed-layout `Command` records. The HTTP path is unchanged and
stays the default.

- One persistent `TcpStream` per worker connection with `TCP_NODELAY`, not one
  connection per request. The default pool is four connections; set
  `WHIPPET_BENCH_TCP_CONNECTIONS` to change it. Batches are round-robined across
  the pool.
- `--batch-size N` maps to up to `N` commands in one `TcpRequest::Submit`
  frame. All commands in a frame share one symbol. Orders and cancels both fit
  the command layout, so a slot's cancels travel in the same frame and the
  tracked-open-id logic is identical to HTTP.
- The server handles one frame at a time per connection (read, append, write
  the ack, then read the next). The harness therefore does **not** write a long
  train of frames ahead. Each connection keeps at most
  `64 KiB / frame_bytes` Submit frames outstanding (8 frames at batch 100, 3 at
  batch 256) and a dedicated reader task drains acks continuously. Acks are
  matched FIFO; TCP preserves order and the server processes frames in order.
- On a `SubmitAck` the harness records one latency sample per order in the
  frame: `ack_received - scheduled_deadline`. A `count` that does not match the
  frame's command count is an error for every order in the frame. An `Error`
  response is an error for every order in the frame and the run continues.
- If `WHIPPET_API_TOKEN` is set, each connection sends `Hello` first and
  requires a successful `HelloAck`.

**TCP stage timestamps.** Every TCP frame carries four stamps:

| Stamp | Meaning |
|---|---|
| T0 | scheduled deadline |
| T1 | frame written (`write_all` returned) |
| T2 | ack bytes read from the socket |
| T3 | ack body decoded |

The manifest `stages` array (top-level and per repetition) holds one
`StageLatency` per non-empty stage:

| Stage | Definition | Units |
|---|---|---|
| `send_us` | T1 - T0 | microseconds |
| `wire_us` | T2 - T1 | microseconds |
| `parse_us` | T3 - T2 | microseconds |
| `server_offset_ns` | ack `server_ns` - client wall clock at T2 | nanoseconds |

`send_us` includes generator handoff and queueing before the write, not just
the syscall. `wire_us` is the client-observed round trip from write return to
ack read; it includes the server's append and any queueing on the connection.
`server_offset_ns` is a best-effort clock comparison: it includes the clock
offset between the two hosts **and** the one-way network delay, so it is not a
one-way latency and must not be reported as one. Stage summaries are recorded
once per frame, not once per order; the latency summary is still recorded once
per order.

**Scheduler slip.** The generator records `wake_time - deadline` separately.
Slip is a first-class result and is also written to the manifest's
`throughput.scheduler_slip`. See the generator-bound rule above.

**Request preparation.** Each JSON body is serialized once, before the request
task starts, and written with `.body(bytes)`. The client allows at least 1,024
idle pooled connections (or the in-flight cap, whichever is larger) and
disables Nagle with `tcp_nodelay`; each request times out at 5 seconds. Those
settings keep HTTP plumbing from becoming the variable in the batch experiment.

**Workload mix.** Every ingress event is one of:

| Event | Default share | Behaviour |
|---|---|---|
| Limit | remainder (80%) | `OrderType::Limit`, price `mid - 10..mid + 10` ticks |
| IOC | 10% (`ioc_ratio`) | `OrderType::Ioc`, never rests |
| Cancel | 10% (`cancel_ratio`) | `POST /api/cancel` for a tracked open id |

Limit prices are drawn from `-3..=10` ticks around the instrument reference
price. Positive offsets rest on the book; zero and negative offsets cross and
trade, so the resting book does not grow monotonically. Buy and sell alternate,
quantity is 1-50, and every order gets a unique participant
(`bench-<rep>-<seq>`), so the engine's self-trade prevention never fires
between benchmark orders.

If a cancel is selected and no open id is tracked, the event falls back to a new
resting limit order. The manifest reports the achieved mix, including fallbacks
and forced conversions, because the cap and the fallback can move the mix away
from the configured ratios.

**Bounded open-order working set.** The harness tracks passive limit ids in a
bounded per-symbol `VecDeque` (at least 512, at most 50,000 per symbol, sized
from the rate). Cancels take the newest tracked id: price-time priority fills
the oldest order first, so the newest id is the one most likely to still be
resting. When the working set is full, a resting limit is replaced by a cancel
(single-order mode) or an IOC order (batch mode). That bounds the resting book
for long runs; the manifest records `forced_cancel`, `forced_ioc`, and
`tracked_id_evictions` so the substitution is visible.

**Steady state.** Measured samples are bucketed into one-second windows by
scheduled deadline. The pooled p99 of the first ten windows is compared with
the pooled p99 of the last ten. If the drift exceeds ±5%, the run prints a
warning and the manifest note says it is not steady state. Fewer than 20
windows (a run shorter than 20 seconds) is reported as **skipped**, not passed.

**HDR histograms.** Latency and slip go into HDR histograms with three
significant digits and a 60-second maximum. The manifest reports min, mean,
p50, p90, p99, p99.9, p99.99, and max in microseconds. Percentiles are never
averaged across runs.

**Warmup.** Warmup slots are scheduled and sent but excluded from the measured
samples. The CLI default is 5 seconds. In batch mode only whole warmup batches
are excluded, so the measured sample count is exact.

**Repetitions.** The harness runs `--repetitions` times (default 5). The
headline in the manifest is the repetition with the median p99, not the best
run. Every repetition is kept in the `repetitions` array.

**Manifest.** Every run writes `artifacts/bench/<run-id>.json` with the host,
commit, dirty flag, optional image digest, workload (including `batch_size`),
throughput, latency, per-repetition results, notes, and scope labels. The
field-by-field reference is
[BENCHMARK-MANIFEST.md](BENCHMARK-MANIFEST.md).

**Scope labels.** `scope_label()` returns `headline` only when the host reports
`virtualization = "native"` and the OS is Linux. Docker Desktop, OrbStack,
Linux VMs, macOS, and unknown hosts are `dev-loop`. The harness itself refuses
to report `native` from inside a container (`/.dockerenv` or a container
cgroup), whatever `WHIPPET_VIRTUALIZATION` says.

## What the harness does today

- Sends HTTP JSON orders to `--gateway` (default `http://127.0.0.1:8080`),
  either one order per request or `--batch-size` orders per request.
- Sends the same workload over the binary TCP ingress with `--transport tcp`,
  pipelined over a small persistent connection pool, and records per-frame
  stage timestamps.
- Mixes resting limit orders, IOC orders, and cancels of tracked open ids.
- Bounds the tracked open-order working set per symbol.
- Rotates symbols in the configured list. The default is the three-symbol demo
  set.
- Generates a unique `client_order_id` and participant per order, so repeated
  runs and repeated participants do not collide.
- Caps in-flight requests at 4,096 and times out each request at 5 seconds.
  A saturated cap drops the slot as overload and labels the run invalid; it
  never invents a latency sample.
- Excludes warmup samples.
- Runs the absolute-deadline pacer on a dedicated OS thread and labels a run
  `generator-bound` when slip p99 exceeds 5% of measured p99.
- Compares the first and last ten one-second windows for steady state.
- Writes one JSON manifest with the achieved mix, overload count, and the
  steady-state result.
- Measures the 20 Hz conflated WebSocket fanout path (`examples/fanout.rs`)
  across `N` concurrent `/ws` subscribers, recording frames/s, bytes/s,
  inter-frame gap percentiles, stalled subscribers (>1 s gap), and `/metrics`
  deltas in a fanout manifest.

What it does not do:

- No QUIC or FIX ingress. HTTP JSON is the default; binary TCP is opt-in.
- No binary payload sizing. `payload_bytes` is a fixed 96 in the manifest, not
  a measured request size; the JSON body is larger and a TCP frame is larger
  still. Treat it as a label, not a measurement.
- No HTTP stage timestamps. The TCP path fills `stages`; an HTTP run leaves it
  empty because reqwest does not expose the frame-write and ack-read instants.
- No confidence intervals. `p50_ci95` and `p99_ci95` are null.
- No raw `.hdr` files. The manifest's summaries are the raw artifact.
- No fill notifications. A cancel can race a fill and be rejected; those
  rejections appear in `error_count`.
- No replay, or failover workloads; the fanout runner measures the 20 Hz
  `/ws` feed rather than the dossier's 100k/s raw-tick F2 workload.
  `scripts/crash-test.sh` (kill-and-replay on one node) and `scripts/soak.sh`
  (10-minute constant-rate proof) exist as repeatable proofs; both are
  results-pending.
- No CPU pinning or isolation inside the harness. The Linux runner applies
  `taskset` when asked and records governor and SMT state.
- No server-side metrics correlation; the gateway's own histograms are
  cumulative and separate from the harness histograms.

## Run matrix

The matrix is the target protocol from the research dossier. The last column
is the honest status of this harness.

| ID | Workload | Vary | Duration | Harness support today |
|---|---|---|---|---|
| L1 | 64 B orders, 1 symbol | 1k / 5k / 10k / 25k / 50k / 100k orders/s | 5 min x 5 | Partial. HTTP JSON by default, binary TCP with `--transport tcp`, mixed limit/IOC/cancel flow, optional batching, TCP stage timestamps. Payload size is not 64 B on the wire. |
| L2 | 256 B, 10 symbols | 10k / 50k orders/s | 5 min x 5 | Partial. Only three instruments exist; the runner labels the row `L2-3sym`. |
| F1 | 10k orders/s | 1 / 10 / 100 / 1000 subscribers | 5 min x 5 | Partial. `examples/fanout.rs` (`scripts/bench-fanout.sh`) measures `/ws` fanout across `N` subscribers (scales to 1,000) with batched `--ingress-rate` ingress (`--ingress-batch`, `WHIPPET_FANOUT_INGRESS_BATCH`) and envelope-or-full `--parse` validation; a 10k/s comparison pair has not been recorded yet. |
| F2 | Fixed 100k egress/s | 10 / 100 subscribers | 5 min x 5 | No. |
| R1 | 10M replay | 0 / mid / tail; 1 / 10 / 100 consumers | drain x 5 | No. `whippet replay` exists for correctness, not replay throughput. |
| R2 | Replay + live 10k/s | 1 config | 5 min x 5 | No. |
| C1 | 10k/s, 3-node | 20 random kill phases | 30 s each | No. Single node only; `scripts/crash-test.sh` proves kill-and-replay on one node, results pending. |
| S1 | 80% SLO knee soak | 1 config | 30 min x 3 | Partial. `scripts/soak.sh` runs a constant-rate proof with a 5% drift gate; the dev-loop 180 s run fails the gate (broker append growth), Linux pending. |

Running L1 with the current harness produces a valid HTTP-ingress measurement,
not the binary-ingress L1 in the dossier. Label it accordingly.

## Local dev-loop baseline (before the workload-mix change)

Recorded on an Apple M4 with Docker Desktop, simulator stopped, single-order
HTTP ingress, 8 seconds measured, one repetition. **`dev-loop`, not a result.**
The harness at that time sent passive limit orders only; there were no IOC or
cancel events.

| Rate | p50 | p99 | Errors |
|---|---|---|---|
| 200 /s | 2,305 µs | 6,307 µs | 0 |
| 1,000 /s | 1,483 µs | 86,719 µs | 0 |
| 2,000 /s | 88,639 µs | 655,871 µs | 0 |
| 5,000 /s | 1,085,439 µs | 1,675,263 µs | 31,924 |

The knee is around 1k orders/s: one HTTP request plus one Iggy durable append
per order is the dominant ingress cost. At 2k/s the queue is already growing
(p50 above 80 ms); at 5k/s the generator is dropping slots and the error count
is large. This is why the batch-size sweep below is the primary experiment: it
changes the number of durable appends per request without changing the offered
order rate.

Do not quote these numbers in a README, a slide, or a comparison table. They
exist to show the shape of the knee and to compare a local run against itself.

## Workload-mix smoke (after the change)

Two single-order runs on the same Docker Desktop stack, fresh volumes,
simulator stopped, 8 s measured, 2 s warmup, one repetition,
`WHIPPET_SNAPSHOT_EVERY=1000`. These are `dev-loop` smoke data, not results.

| Run | Rate | p50 | p99 | Samples | Errors | Labels |
|---|---|---|---|---|---|---|
| Before (passive limits only) | 200 /s | 2,305 µs | 6,307 µs | 1,600 | 0 | `dev-loop` |
| After (limit/IOC/cancel) | 200 /s | 1,393 µs | 8,131 µs | 1,600 | 0 | `dev-loop`, `generator-bound` |
| Before (passive limits only) | 1,000 /s | 1,483 µs | 86,719 µs | 8,000 | 0 | `dev-loop` |
| After (limit/IOC/cancel) | 1,000 /s | 569 µs | 23,119 µs | 8,000 | 0 | `dev-loop` |

Read the delta carefully:

- The before rows were measured with the old harness (pacer on a Tokio worker,
  passive limit orders only, no batching) and the old node image. The after
  rows use the dedicated pacer thread, pre-serialized request bodies, the
  mixed workload, and the current node image. The delta is the harness plus
  the workload plus the node, not one isolated change.
- The 200/s after run is labelled `generator-bound`: its slip p99 of 1,140 µs
  is 14% of its 8,131 µs latency p99, so part of that tail is the pacer. The
  1,000/s after run is not labelled.
- Docker Desktop variance is large. One 8-second run is a smoke, not a result.
- The achieved mix at 1,000/s is about 62% limit, 10% IOC, and 29% cancel with
  roughly 1,900 forced cancels. The working-set cap (512 ids per symbol at
  this rate) fills in about two seconds, after which the cap converts resting
  limits into cancels. That is the bound working, and the manifest says so.

## Batch-size sweep (the primary after-change experiment)

The sweep changes only how many orders share one HTTP request and one durable
append. Two entry points exist:

```sh
make bench-sweep            # scripts/bench-sweep.sh: rates x batch sizes
scripts/bench-sweep.sh      # default rates 1k/2k/5k, batches 1/10/100/256
scripts/bench.sh sweep      # single rate (1k/s), batches 1/10/100/256
scripts/bench-tcp.sh        # TCP ingress: rates 1k/5k/10k, batches 1/10/100
```

`scripts/bench-sweep.sh` reads `WHIPPET_SWEEP_RATES`,
`WHIPPET_SWEEP_BATCHES`, `WHIPPET_SWEEP_DURATION`, `WHIPPET_SWEEP_WARMUP`, and
`WHIPPET_SWEEP_REPETITIONS`. Its default matrix is 3 rates x 4 batch sizes,
one repetition each, 10 seconds measured and 2 seconds of warmup. The single
rate variant in `scripts/bench.sh` uses 8 seconds measured, to match the
before-change baseline shape:

| Label | Batch | Request rate at 1k orders/s |
|---|---|---|
| `batch-1` | 1 | 1,000 /s |
| `batch-10` | 10 | 100 /s |
| `batch-100` | 100 | 10 /s |
| `batch-256` | 256 | 3.9 /s |

Start from a fresh stack (`scripts/reset.sh`, then bring up `iggy` and `node`).
A node that has been running for a while carries a large restored order book;
snapshots of that book stall the shard and dominate a short run, which makes
the batch sizes indistinguishable. `scripts/bench.sh` passes
`WHIPPET_SNAPSHOT_EVERY` to both the node and the bench container, so the
manifest note records it; `scripts/bench-sweep.sh` does not, so set the same
value on the node yourself and write it down. A fresh stack is what makes the
comparison meaningful.

Equivalent manual commands:

```sh
cargo run --release -p whippet-bench --example live_bench -- \
  --gateway http://127.0.0.1:8080 --rate 1000 --duration 8 --warmup 2 \
  --repetitions 1 --label batch-10 --batch-size 10
```

or, inside the rebuilt demo image:

```sh
docker compose -f deploy/compose.yaml --profile bench run --rm bench bench \
  --gateway http://node:8080 --rate 1000 --duration 8 --warmup 2 \
  --repetitions 1 --batch-size 10 --label batch-10 \
  --output-dir /artifacts/bench
```

What to compare, per batch size:

- achieved order rate against 1,000/s (the batch must not change offered load),
- p50/p99 from the manifest (recorded once per order, from the batch deadline),
- `error_count` (a batch that does not acknowledge every order is an error),
- `scheduler_slip` (if slip p99 is more than 5% of latency p99, the run is
  labelled `generator-bound` and the tail is not the node's),
- `overloaded_slots` (a saturated in-flight cap drops slots and labels the run
  `invalid-overload`; a run with drops is not a latency measurement),
- the steady-state note (a sweep shorter than 20 seconds reports the check as
  skipped; use the 5-minute L1 run for the steady-state claim),
- the achieved mix note (batch mode still emits individual cancels).

### Measured dev-loop sweep

The primary after table at the top of this document is the current measured
sweep (1,000 and 5,000 orders/s, batches 1 / 10 / 100 / 256, 8 s measured, 2 s
warmup, one repetition, 0 errors). It supersedes the earlier 1,000/s-only table
from the previous tranche. What the table does and does not say:

- Achieved throughput is the offered rate at every batch size. Batching changes
  the request count, not the offered load.
- p50 rises with batch size at 1,000/s because every order in a batch inherits
  the completion of the whole durable append. That is the coordinated-omission
  corrected batch latency, not a single-order median regression.
- p99 is dominated by Docker Desktop stalls and is not monotonic across batch
  sizes. An 8-second run cannot resolve a p99 difference; the 5-minute native
  Linux L1 run is the measurement for the tail. The `5,000/s, batch 100` tail
  (p99 246 ms) is a one-repetition outlier, not a trend.
- Warmup is trimmed to whole batches, so a batch-256 run's measured sample
  count can exceed `rate x duration`. `achieved_per_sec` uses the real measured
  span.

The same sweep with the default snapshot interval (every 1,000 commands)
produced p50 of 7-48 ms and p99 up to 1.1 s in an earlier tranche. That is
snapshot I/O of the order book, not ingress, and it makes the batch sizes
indistinguishable. Always record the snapshot setting next to the manifest.

The batch sweep is only meaningful on the same host with the same commit. A
Docker Desktop sweep is `dev-loop` and cannot be a headline.

## Dev-loop result: group commit and TCP ingress

**`dev-loop`, not a result.** Fresh stack, simulator stopped, Docker Desktop on
an M4, 8 s measured, 2 s warmup, one repetition. The numbers exist to show the
shape of the group-commit and binary-ingress paths and to catch regressions.
They are not headline results and must not be quoted as transport wins.

### Group commit (HTTP, single order)

`--batch-size 1`, so the client sends one order per request and the node's group
commit decides how many requests share one durable append.

| Rate | p50 | p99 | p99.9 | Errors | Achieved | `group_commit_max_batch` | Appends |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000/s | 572 µs | 1,167 µs | — | 0 | 1,000/s | 1 | — |
| 5,000/s | 2,411 µs | 77,887 µs | 133,631 µs | 0 | 5,000/s | 78 | 33,240 appends for 62,155 messages |

At 1,000/s the queue never builds, so group commit stays at one command per
append. At 5,000/s the same single-order client shape now batches up to 78
commands per append (62,155 messages in 33,240 appends, about 1.87 messages per
append) and holds the offered rate with zero errors. The final batch-size sweep
above measured the same 5,000/s single-order shape at p50 2,030 µs and p99
28,159 µs; this table is the run that also captured
`group_commit_max_batch`. Before group commit, the 5,000/s single-order shape
queued at p50 838 ms with roughly 40k errors; that was the old batch-1 failure
mode, not a latency result.

### Replay determinism

The replay proof was rerun after the change. Full replay and snapshot-seeded
replay both end at
`0f90672d75d2af2e028f3a0532836a361d24b85c03cad8636a3ad89c0d29e218` from
snapshot offset 17038 (tail 16600). This is a correctness check, not a latency
measurement.

## TCP-ingress sweep (dev-loop measured, headline pending)

The TCP sweep is the binary-ingress counterpart to the HTTP batch sweep. It
runs the same mixed workload and changes only the transport and the number of
commands per frame:

```sh
scripts/bench-tcp.sh
# equivalent to:
WHIPPET_SWEEP_TRANSPORT=tcp WHIPPET_TCP_ADDR=node:8081 \
WHIPPET_SWEEP_RATES=1000,5000,10000 WHIPPET_SWEEP_BATCHES=1,10,100 \
WHIPPET_SWEEP_LABEL_PREFIX=dev-loop-tcp scripts/bench-sweep.sh
```

The default matrix is 3 rates (1k / 5k / 10k orders/s) x 3 batch sizes (1 / 10
/ 100 commands per Submit frame), 10 s measured, 2 s warmup, one repetition,
labels `dev-loop-tcp-b<N>-r<R>`. The simulator is stopped and consumer lag is
drained between runs exactly as in the HTTP sweep. Set
`WHIPPET_BENCH_TCP_CONNECTIONS` to change the connection pool (default 4).

Manual single run against a native node:

```sh
cargo run --release -p whippet-bench --example live_bench -- \
  --transport tcp --tcp-addr 127.0.0.1:8081 \
  --rate 1000 --duration 8 --warmup 2 --repetitions 1 \
  --batch-size 10 --label tcp-batch-10
```

or with the CLI:

```sh
cargo run --release -p whippet -- bench \
  --transport tcp --tcp-addr 127.0.0.1:8081 \
  --rate 1000 --duration 30 --warmup 5 \
  --repetitions 5 --batch-size 10 --label L1-tcp
```

### Measured dev-loop runs

Same host and shape as the group-commit table: fresh stack, simulator stopped,
Docker Desktop on an M4, 8 s measured, 2 s warmup, one repetition. **`dev-loop`,
not a result.**

| Batch | Rate | Connections | p50 | p99 | p99.9 | Errors | Achieved |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5,000/s | 4 | 1,284 ms | — | — | — | 3,160/s (connection-limited; do not feature) |
| 1 | 5,000/s | 16 | 2,369 µs | 456,959 µs | — | 0 | — |
| 1 | 5,000/s | 32 | 1,722 µs | 49,311 µs | — | 0 | — |
| 10 | 5,000/s | 4 (default) | 799 µs | 17,103 µs | — | 0 | — |
| 10 | 10,000/s | 4 (default) | 598 µs | 10,647 µs | 22,095 µs | 0 | 10,000/s |
| 10 | 10,000/s | 4 (default, later run) | 473 µs | 24,079 µs | 56,159 µs | 0 | 10,000/s |

TCP is one request in flight per connection by design, so batch-1 throughput
scales with connection count. The 4-connection batch-1 row is connection-limited
at 3,160/s and should not be featured. The 16- and 32-connection rows show the
shape; the 16-connection p99 (457 ms) is far worse than the 32-connection p99
(49 ms), so do not pick a connection count from p50 alone. Batch 10 amortizes
the round trip and is the useful TCP shape: 799 µs p50 / 17.1 ms p99 at
5,000/s, and 598 µs p50 / 10.6 ms p99 / 22.1 ms p99.9 at 10,000/s with zero
errors and the full offered rate. A later run on the same tree measured 473 µs
p50 / 24.1 ms p99 at 10,000/s; the spread between repetitions is host noise.

Exact commands for the rows above:

```sh
docker compose -f deploy/compose.yaml --profile bench run --rm --user "$(id -u):$(id -g)" \
  -e WHIPPET_VIRTUALIZATION=docker-desktop bench bench --gateway http://node:8080 \
  --rate 5000 --duration 8 --warmup 2 --repetitions 1 --batch-size 1 --label gc-5000

docker compose -f deploy/compose.yaml --profile bench run --rm --user "$(id -u):$(id -g)" \
  -e WHIPPET_VIRTUALIZATION=docker-desktop -e WHIPPET_BENCH_TCP_CONNECTIONS=32 \
  bench bench --gateway http://node:8080 --tcp-addr node:8081 --transport tcp \
  --rate 5000 --duration 8 --warmup 2 --repetitions 1 --batch-size 1 --label tcp-b1-r5000-c32

docker compose -f deploy/compose.yaml --profile bench run --rm --user "$(id -u):$(id -g)" \
  -e WHIPPET_VIRTUALIZATION=docker-desktop bench bench --gateway http://node:8080 \
  --tcp-addr node:8081 --transport tcp --rate 10000 --duration 8 --warmup 2 \
  --repetitions 1 --batch-size 10 --label tcp-b10-r10000
```

The 16- and 4-connection rows use the same command with
`WHIPPET_BENCH_TCP_CONNECTIONS=16` and `WHIPPET_BENCH_TCP_CONNECTIONS=4`.

**Headline status: pending.** These are dev-loop numbers. A publishable TCP L1
needs the native Linux runner with the simulator stopped. When it runs, report:

- the latency summary (per order) and `error_count`,
- `stages` p50/p99 for `send_us`, `wire_us`, `parse_us`, and
  `server_offset_ns`,
- `workload.transport` (`tcp`) and the connection-count note,
- `scheduler_slip` and the `generator-bound` label,
- the steady-state note (skipped for runs shorter than 20 seconds).

Do not compare a TCP dev-loop run with an HTTP dev-loop run and call the delta a
transport win. Different hosts, connection pools, and server builds change the
result; the comparison is only meaningful on the same native host, commit, and
snapshot setting.

## WebSocket fanout (20 Hz conflated feed — dev-loop measured)

The gateway publishes conflated market-data state frames over `/ws` at 20 Hz
(50 ms nominal period) using a shared `FrameCache` keyed by
`(symbols, trade_limit, sequence, freshness_ms)`. The fanout harness
(`crates/whippet-bench/examples/fanout.rs`) measures subscriber frame delivery,
byte throughput, and inter-frame gap percentiles as the subscriber count scales.

### Dev-loop result

Docker Desktop on M4, node and simulator running, `?trades=60`, 8 s measured,
2 s warmup, one run per subscriber count. The frame cache means the server
serializes once per `(filter, tick)`; egress is per subscriber.

| Subscribers | Frames/s | Aggregate MB/s | Gap p50 | Gap p99 | Stalled |
|---:|---:|---:|---:|---:|---:|
| 1 | 20.0 | 0.29 | 50.0 ms | 52.4 ms | 0 |
| 10 | 198.8 | 2.88 | 50.1 ms | 74.8 ms | 0 |
| 100 | 2,000.0 | 29.42 | 50.3 ms | 59.6 ms | 0 |

At 100 subscribers the node was at ~5.6% CPU while also serving the simulator,
and no subscriber stalled. The 10-subscriber gap p99 (74.8 ms) is worse than
the 100-subscriber one (59.6 ms); this is scheduler noise on a shared laptop,
not a scaling trend. These are `dev-loop`, one-run numbers, not headline
results.

With the ingress driver batched (`--ingress-batch 10`, 10,000 orders/s) and
envelope parsing, 500 subscribers delivered 20 fps and ~245 KB/s/client with
0 ingress errors and 0 stalled clients. A 1,000-subscriber run fails through
Docker Desktop's host port proxy with a socket reset before the node's 2,048
WebSocket cap is reached; the node itself is not the limit. Treat 1,000 as a
native-Linux/direct-network target, not a Docker Desktop result.

### Method

- **Connection and subscription.** The harness opens `--subscribers N`
  concurrent WebSocket connections (`tokio-tungstenite`, `TCP_NODELAY`) to
  `/ws?trades=T` before starting the measurement clock. There is no upper
  cap; the sweep scales to 1,000 subscribers. Each subscriber is one
  lightweight task that retains no frame payload, so the measurement stays
  server-bound. When `--symbols` is supplied, each subscriber immediately
  sends `{"type":"subscribe","symbols":[...]}` (`WsClientMessage::Subscribe`).
- **Frame validation (`--parse`).** `envelope` (default) decodes only the
  `"type"` tag plus identifying fields (`sequence` for state,
  `level`/`message` for notice, `symbol`/`seq` for trade, `orders_ingressed`
  for metrics) and never builds a `StateView`; state symbol/trade counts are
  left at zero and bytes are still counted per frame. `full` keeps the
  complete `WsServerMessage` decode. The mode is recorded in the manifest
  (`parse_mode`) and in the notes.
- **Warmup and measured window.** Every subscriber drains frames for
  `--warmup S` seconds and then records measurements for `--duration S`
  seconds. Each incoming text or binary frame is parsed as `WsServerMessage`
  (`state`, `notice`, `trade`, `metrics`) to verify wire validity and then
  dropped immediately; frames are never stored in memory.
- **Inter-frame gap and stall detection.** Each subscriber records the
  microsecond gap between consecutive frames in an HDR histogram (3 significant
  digits, 60 s ceiling). If any inter-frame gap or trailing silence inside the
  measured window exceeds **1 second (1,000,000 µs)**, that subscriber is
  flagged as `stalled = true`, `stalled_clients` is incremented, and the
  manifest adds the `stalled-subscriber` label.
- **Ingress driver.** With `--ingress-rate R` (`WHIPPET_FANOUT_INGRESS_RATE`
  in `scripts/bench-fanout.sh`) the harness posts `R` same-symbol ladder
  limit orders/s for the whole run (warmup + measured) while the subscribers
  listen. With `--ingress-batch N` (`WHIPPET_FANOUT_INGRESS_BATCH`, default
  1) each slot posts `N` orders to `POST /api/orders/batch` with the
  `{"orders": [...]}` shape, paced at `R / N` slots/s so the offered order
  rate is unchanged; a batch whose ack does not confirm exactly `N` orders
  counts `N` errors. The single-order 10k/s shape is past the durable knee and
  produced ~27% errors; batch 10 is the valid 10k/s shape. Every order carries
  a unique client id and participant, so repeated runs never collide. The
  manifest records the target rate, batch size, achieved rate, sent/ok/error
  counts, and the `/metrics` deltas across the run in a new `ingress` field.
  With `R = 0` (default) the driver is off and the manifest says so; use that
  shape as the no-subscriber baseline.
- **Server `/metrics` scrape.** Immediately before warmup and after the
  measured window, the harness scrapes `GET /metrics` on the gateway and
  records `whippet_orders_ingressed_total`, `whippet_md_sequence`, and
  `process_cpu_seconds_total` (if the endpoint exposes it; skipped cleanly when
  absent or unreachable), along with their deltas across the run.
- **Baseline comparison.** Pass a quiet-book manifest with
  `--baseline-metrics PATH` (`WHIPPET_FANOUT_BASELINE_METRICS` in the script).
  The manifest echoes the baseline's `/metrics` deltas in a `baseline` field
  and adds a note comparing them with this run's deltas, so the report shows
  whether subscribers slowed the engine.

### Flags and commands

| Flag | Default | Meaning |
|---|---|---|
| `--gateway` | `http://127.0.0.1:8080` | Gateway HTTP or WebSocket base URL |
| `--subscribers` | `10` | Concurrent WebSocket subscribers (`N >= 1`, no upper cap; scales to 1,000) |
| `--duration` | `30` | Measured window in seconds |
| `--warmup` | `5` | Warmup window in seconds (excluded from counts/histograms) |
| `--trades` | `0` | Trade tape limit passed as `/ws?trades=N` (`0` = full tape) |
| `--ingress-rate` | `0` | Real ingress load in orders/s for warmup + measured (`0` = off) |
| `--ingress-batch` | `1` | Orders per ingress request: `1` = `POST /api/orders`, `N > 1` = `POST /api/orders/batch` paced at `R / N` slots/s |
| `--parse` | `envelope` | Frame validation: `envelope` (type tag + identifying fields) or `full` (`WsServerMessage` decode) |
| `--baseline-metrics` | *(none)* | Path to a no-subscriber baseline manifest; the manifest echoes its `/metrics` deltas with a comparison note |
| `--symbols` | *(all)* | Optional comma-separated symbol filter sent as `Subscribe` |
| `--output-dir` | `artifacts/bench` | Directory for `<run-id>.json` |
| `--label` | `fanout` | Workload label recorded in the manifest |

Sweep 1 / 10 / 100 / 1,000 subscribers for 30 s each (simulator stopped,
label `dev-loop-fanout`, 10k orders/s ingress in batches of 10, envelope
parse), writing to `artifacts/bench`:

```sh
scripts/bench-fanout.sh
# equivalent defaults:
# WHIPPET_FANOUT_SUBSCRIBERS=1,10,100,1000 WHIPPET_FANOUT_INGRESS_RATE=10000 \
# WHIPPET_FANOUT_INGRESS_BATCH=10 WHIPPET_FANOUT_PARSE=envelope \
# WHIPPET_FANOUT_LABEL=dev-loop-fanout \
# WHIPPET_FANOUT_OUTPUT_DIR=./artifacts/bench scripts/bench-fanout.sh
```

Manual single runs:

```sh
cargo run --release -p whippet-bench --example fanout -- \
  --gateway http://127.0.0.1:8080 \
  --subscribers 10 \
  --duration 30 \
  --warmup 5 \
  --trades 0 \
  --parse envelope \
  --ingress-rate 10000 \
  --ingress-batch 10 \
  --label dev-loop-fanout \
  --output-dir artifacts/bench

# Filtered to a single symbol and 25 trades:
cargo run --release -p whippet-bench --example fanout -- \
  --gateway http://127.0.0.1:8080 \
  --subscribers 100 \
  --duration 30 \
  --warmup 5 \
  --trades 25 \
  --parse envelope \
  --symbols BTC-USD \
  --label dev-loop-fanout \
  --output-dir artifacts/bench

# 1,000-subscriber server-bound shape (envelope parse, batched 10k/s ingress):
cargo run --release -p whippet-bench --example fanout -- \
  --gateway http://127.0.0.1:8080 \
  --subscribers 1000 \
  --duration 30 \
  --warmup 5 \
  --trades 0 \
  --parse envelope \
  --ingress-rate 10000 \
  --ingress-batch 10 \
  --label dev-loop-fanout \
  --output-dir artifacts/bench
```

Comparison mode (quiet book vs batched 10k/s ingress, same subscriber count):

```sh
# 1. Quiet-book baseline: driver off. This manifest is the baseline.
WHIPPET_FANOUT_SUBSCRIBERS=1 WHIPPET_FANOUT_INGRESS_RATE=0 \
  scripts/bench-fanout.sh
# 2. Loaded run: same subscribers, batched driver on, baseline attached.
WHIPPET_FANOUT_SUBSCRIBERS=1 WHIPPET_FANOUT_INGRESS_RATE=10000 \
  WHIPPET_FANOUT_INGRESS_BATCH=10 \
  WHIPPET_FANOUT_BASELINE_METRICS=artifacts/bench/<baseline-run-id>.json \
  scripts/bench-fanout.sh
```

The second manifest's `baseline` field echoes the first run's
`orders_ingressed_delta` / `md_sequence_delta` and its notes say whether the
subscriber path slowed the engine. Single-run equivalent:

```sh
cargo run --release -p whippet-bench --example fanout -- \
  --gateway http://127.0.0.1:8080 \
  --subscribers 1 \
  --duration 30 \
  --warmup 5 \
  --parse envelope \
  --ingress-rate 10000 \
  --ingress-batch 10 \
  --baseline-metrics artifacts/bench/<baseline-run-id>.json \
  --label dev-loop-fanout-ingress \
  --output-dir artifacts/bench
```

### Fanout manifest shape

Because fanout measures continuous server-push frames rather than open-loop
request/ack slots, `examples/fanout.rs` writes `FanoutManifest`
(`kind = "fanout"`) to `<output-dir>/<run-id>.json` without altering the core
ingress `BenchManifest` schema:

```json
{
  "schema_version": 1,
  "kind": "fanout",
  "run_id": "<uuid>",
  "created_at": "2026-11-15T00:00:00Z",
  "git_sha": "<commit>",
  "git_dirty": false,
  "image_digest": null,
  "scope": "dev-loop",
  "label": "dev-loop-fanout",
  "host": { "...": "same HostInfo as BenchManifest" },
  "gateway": "http://127.0.0.1:8080",
  "subscribers": 10,
  "trades": 0,
  "symbols": ["BTC-USD"],
  "parse_mode": "envelope",
  "warmup_secs": 5,
  "duration_secs": 30,
  "measured_secs": 30.0,
  "frames_per_sec": 0.0,
  "frames_per_sec_per_client": 0.0,
  "bytes_per_sec_per_client": 0.0,
  "aggregate_bytes_per_sec": 0.0,
  "total_frames": 0,
  "total_bytes": 0,
  "stalled_clients": 0,
  "gap_us": {
    "count": 0,
    "min": 0.0,
    "mean": 0.0,
    "p50": 0.0,
    "p90": 0.0,
    "p99": 0.0,
    "p999": 0.0,
    "p9999": 0.0,
    "max": 0.0,
    "p50_ci95": null,
    "p99_ci95": null
  },
  "clients": [
    {
      "client_id": 0,
      "frames": 0,
      "bytes": 0,
      "frames_per_sec": 0.0,
      "bytes_per_sec": 0.0,
      "gap_p50_us": 0.0,
      "gap_p99_us": 0.0,
      "gap_max_us": 0.0,
      "stalled": false,
      "parse_errors": 0
    }
  ],
  "metrics": {
    "before": {
      "orders_ingressed_total": 0,
      "md_sequence": 0,
      "process_cpu_seconds_total": null
    },
    "after": {
      "orders_ingressed_total": 0,
      "md_sequence": 0,
      "process_cpu_seconds_total": null
    },
    "orders_ingressed_delta": 0,
    "md_sequence_delta": 0,
    "process_cpu_seconds_delta": null
  },
  "ingress": {
    "target_rate_per_sec": 0,
    "batch_size": 1,
    "sent": 0,
    "ok": 0,
    "errors": 0,
    "elapsed_secs": 0.0,
    "achieved_per_sec": 0.0
  },
  "baseline": null,
  "notes": [],
  "labels": ["dev-loop", "fanout"]
}
```

With `--baseline-metrics`, `baseline` is
`{"source": "<path>", "orders_ingressed_delta": <n>, "md_sequence_delta": <n>}`
and the notes carry the subscriber-overhead comparison.

### Results

**Status: pending.** No `dev-loop-fanout` numbers have been recorded with
the server-bound shape yet (envelope parse, `--ingress-rate 10000`
`--ingress-batch 10`). The sweep matrix is 1 / 10 / 100 / 1,000
subscribers, 30 s measured each; run `scripts/bench-fanout.sh` and link the
resulting manifests from `artifacts/bench/` before quoting any fanout
numbers. The 1,000-subscriber row is new: it must show zero stalls with the
envelope parser before the client is called server-bound at that scale.

| Subscribers | Frames/s | Aggregate MB/s | Gap p50 | Gap p99 | Stalled |
|---:|---:|---:|---:|---:|---:|
| 1 | pending | pending | pending | pending | pending |
| 10 | pending | pending | pending | pending | pending |
| 100 | pending | pending | pending | pending | pending |
| 1,000 | pending | pending | pending | pending | pending |

Neither the `--ingress-rate 10000 --ingress-batch 10` comparison pair nor a
native Linux fanout run has been recorded yet either.

## Soak (constant-rate proof — dev-loop measured)

`scripts/soak.sh` starts the stack, stops the simulator, runs the HTTP
batch-10 workload at 5,000 orders/s for 10 minutes, samples `/api/state`
every 10 s (server ingress p50/p99, consumer lag, node RSS when Docker
exposes it), and writes a JSON summary to `artifacts/bench/soak-<ts>.json`.
The bench container's own manifest lands next to it. The script fails when
the mean p99 of the last 10% of samples drifts more than 5% from the first
10%, or when a shard reports failure (`shards_ok: false` on `/api/health`).

```sh
scripts/soak.sh
```

Knobs (`WHIPPET_SOAK_RATE`, `WHIPPET_SOAK_BATCH`, `WHIPPET_SOAK_DURATION`,
`WHIPPET_SOAK_WARMUP`, `WHIPPET_SOAK_INTERVAL`, `WHIPPET_SOAK_OUTPUT_DIR`)
keep the same defaults (5,000/s, batch 10, 600 s, 5 s warmup, 10 s polls).
The drift arithmetic lives in `crates/whippet-bench/src/soak.rs`
(`soak_drift_p99`, `soak_verdict`, unit-tested); the script mirrors it in
Python for the shell-side summary.

**Dev-loop result (fresh stack, 5,000/s, batch 10, 180 s).** p50 grew from
1.1 ms to 5.0 ms and p99 from 7 ms to ~390 ms; node RSS grew from 119 MiB to
282 MiB; consumer lag spiked to ~3,800 around snapshot boundaries. The run
failed the 5% drift gate. The stage budget explains it: at the end,
`gateway_append` was p50 5.0 ms / p99 448 ms, `engine_append` p50 1.9 ms, while
`engine_process` stayed at 4 µs and `md_apply` at 3 µs. The growth is the Iggy
append round trip as the log grows, not matcher CPU.

Two Iggy changes were measured on the same host. Disabling the one-minute
message cleaner removed the early periodic spikes but not the monotonic growth.
A 1 GiB WAL plus a 200 µs WAL group-commit delay reduced the 120 s run to
p50 1.2 → 3.0 ms and p99 4.7 → 227 ms. The default stack keeps the cleaner
off and the larger WAL because both were measured better here; re-measure on
native Linux before treating them as tuned.

A cold first run after `make reset` can be much slower than the next one
(6.2 ms p50 / 810 ms p99 versus 1.3 ms / 7.3 ms for the same workload): the
1 GiB WAL allocation and page cache are cold. Warm the stack before measuring.

The honest reading: the durable path is **not steady-state at 5,000/s for
minutes on Docker Desktop**. The next work is per-role Iggy clients (ingress vs
engine output), log-size/retention, and the Linux run with a fresh log. Do not
quote these as headline results.

## Crash recovery (kill -9 proof — method recorded, results pending)

`scripts/crash-test.sh` runs a short workload (1,000 orders/s, batch 10,
20 s), kills the node with `docker compose kill node` (SIGKILL, the Compose
equivalent of `kill -9`), restarts it, waits for `/api/health` readiness,
then runs `make replay` and asserts both replay hashes match. Raw replay
output goes to `artifacts/bench/crash-<ts>.log` with a JSON summary
(`crash-<ts>.json`) next to it holding both hashes and the verdict.

```sh
scripts/crash-test.sh
```

Knobs: `WHIPPET_CRASH_RATE`, `WHIPPET_CRASH_BATCH`,
`WHIPPET_CRASH_DURATION`, `WHIPPET_CRASH_OUTPUT_DIR`.

**Status: pending.** No crash-test summary has been recorded yet.

## Running a benchmark

Inside the demo image, on any host:

```sh
make bench
# or
scripts/bench.sh smoke   # 200/s, 10 s, 1 rep
scripts/bench.sh sweep   # 1k/s, batch 1 / 10 / 100 / 256
scripts/bench.sh full    # L1: 1k/s, 30 s, 5 reps
```

Workload knobs for the script:

| Variable | Default | Meaning |
|---|---|---|
| `WHIPPET_BENCH_BATCH_SIZE` | 1 | orders per request / commands per TCP frame, 1..=256 |
| `WHIPPET_BENCH_IOC_RATIO` | 0.10 | IOC share of ingress events |
| `WHIPPET_BENCH_CANCEL_RATIO` | 0.10 | cancel share of ingress events |
| `WHIPPET_BENCH_SEED` | `0x5eed0001` | RNG seed; fixed so runs compare |
| `WHIPPET_BENCH_TRANSPORT` | `http` | `http` or `tcp` (`bench.sh`) |
| `WHIPPET_BENCH_TCP_ADDR` | `node:8081` | TCP ingress inside Compose (`bench.sh`) |
| `WHIPPET_SWEEP_TRANSPORT` | `http` | `http` or `tcp` (`bench-sweep.sh`) |
| `WHIPPET_TCP_ADDR` | `node:8081` | TCP ingress inside Compose (`bench-sweep.sh`) |
| `WHIPPET_BENCH_TCP_CONNECTIONS` | 4 | persistent TCP connections per repetition |

The ratios are environment knobs because the CLI does not expose them yet; the
effective values and every override are recorded in the manifest notes.

Manual, with explicit settings:

```sh
cargo run --release -p whippet -- bench \
  --gateway http://127.0.0.1:8080 \
  --rate 1000 --duration 30 --warmup 5 \
  --repetitions 5 --batch-size 1 --label L1
```

Inside Compose:

```sh
docker compose -f deploy/compose.yaml exec node \
  whippet bench --rate 1000 --duration 30 --warmup 5 --repetitions 5 --label L1
```

The CLI prints one summary line and the manifest path. The manifest is written
under `artifacts/bench/`. If the container path is not mounted to the host,
copy it out before resetting the stack.

### Native Linux matrix

The headline path is a bare-metal Linux host:

```sh
scripts/bench-linux.sh l1    # 1 symbol, 1k/s, 5 min x 5
scripts/bench-linux.sh l2    # 3 configured symbols, 10k/s, 5 min x 5
scripts/bench-linux.sh all
```

The runner:

- refuses to run as a headline inside a container or VM
  (`WHIPPET_ALLOW_VIRTUALIZED=1` downgrades it to a labelled `dev-loop` run),
- records `lscpu`, kernel, `/proc/cmdline`, CPU count, and memory next to the
  manifest,
- checks the CPU governor and SMT state and warns when they are not
  `performance` / off,
- applies `taskset` when `WHIPPET_PIN_CPUS` is set and records
  `WHIPPET_ISOLATED_CORES`,
- passes `WHIPPET_TRANSPORT` (default `http`) and `WHIPPET_TCP_ADDR` (default
  `127.0.0.1:8081`) through to the harness, so the same matrix can run over
  binary TCP,
- labels L2 as `L2-3sym` because the demo exchange has three instruments, not
  the dossier's ten.

For a `dev-loop` run on macOS or inside a container, set
`WHIPPET_VIRTUALIZATION=docker-desktop` (or `orbstack`, `linux-vm`) so the
manifest records the environment explicitly. The scope label is `dev-loop`
either way on a non-native-Linux host.

## Publishing a run

1. Run at least five repetitions with a warmup, on a native Linux host, with
   the simulator stopped and no other load on the machine.
2. Keep the machine quiet; do not run another benchmark against the same node.
3. Copy the manifest, unchanged, into a run directory under
   `docs/benchmarks/`, together with the `scripts/bench-linux.sh` environment
   capture.
4. Write the method card next to it. Use the template in
   [docs/benchmarks/README.md](benchmarks/README.md).
5. Report the median-p99 repetition as the headline, plus the min/max spread
   across repetitions and the sample count.
6. Report the steady-state note. If the check was skipped or flagged, say so;
   do not call the run steady state.
7. Label the scope. If it is not native Linux, it is `dev-loop`. A container or
   VM run is never a headline, even if the numbers look good.
8. Link the raw manifest. Do not retype numbers into a table without linking
   the source.

Never hand-edit a manifest. If a field is wrong, fix the harness and rerun.

## Known gaps before a headline run

- Record the real request and response sizes, not a fixed 96.
- Add bootstrap confidence intervals.
- Add fill notifications so a cancel can distinguish "raced a fill" from a
  real rejection.
- Run the TCP sweep on native Linux and publish it. The TCP path and its stage
  timestamps exist, and there is a dev-loop measurement above, but no headline
  TCP row yet.
- Run `scripts/bench-fanout.sh` (and the native Linux fanout sweep, the
  `--ingress-rate 10000` comparison pair, the soak, and the crash test) and
  record the manifests; add replay throughput and failover workloads.
- Export raw histogram data alongside the manifest if a consumer needs it.
- The open-order working set bounds the harness's tracked ids; aggressive
  limit remainders are not tracked and can still rest. The steady-state check
  and `error_count` are the signals if that matters for a long run.

Until those exist, the only defensible statement is: the harness, manifest, and
scope labels work; the M4/Docker numbers are dev-loop smoke data; there are no
headline results.
