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

The transport is still HTTP JSON. Fanout, replay, failover, and soak workloads
are in the matrix but not implemented. The run-matrix table says which rows the
harness can execute today.

## Dev-loop result after the first optimization tranche

Fresh stack, simulator stopped, mixed limit/IOC/cancel flow, 10 s measured, 3 s
warmup, one repetition, Docker Desktop on M4, default snapshots. `n` is orders;
each batch's completion time is recorded once per order, so p50 includes the
wait for the batch to fill. Per-order amortized p50 is roughly `p50 / batch`.

| Rate | Batch | p50 | p99 | Samples | Errors | Per-order p50 |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000/s | 1 | 543 µs | 1,027 µs | 10,000 | 0 | 543 µs |
| 1,000/s | 10 | 2,501 µs | 4,875 µs | 10,000 | 0 | 250 µs |
| 1,000/s | 100 | 15,295 µs | 27,887 µs | 10,000 | 0 | 153 µs |
| 1,000/s | 256 | 54,175 µs | 70,399 µs | 10,184 | 0 | 212 µs |
| 5,000/s | 1 | 838,143 µs | 1,008,639 µs | 47,811 | 0 | 838 ms (queue) |
| 5,000/s | 10 | 1,762 µs | 4,695 µs | 50,000 | 0 | 179 µs |
| 5,000/s | 100 | 8,231 µs | 24,351 µs | 50,000 | 0 | 82 µs |
| 5,000/s | 256 | 19,407 µs | 44,991 µs | 50,152 | 0 | 76 µs |

The useful dev-loop result is **batch 10 at 5,000 orders/s: p50 1.76 ms,
p99 4.70 ms, 0 errors**, versus batch 1 at 5,000/s, which queues (p50 838 ms)
and previously failed. Batch 256 lowers per-order CPU further but raises
per-order wall latency because each client waits for a full batch.

These are one-repetition dev-loop numbers on a shared laptop. They are not
headline results. They exist to show the batch path and the measurement
harness work; the Linux matrix is still the publishable artifact.

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

What it does not do:

- No TCP, QUIC, or FIX ingress. The transport is HTTP JSON.
- No binary payload sizing. `payload_bytes` is a fixed 96 in the manifest, not
  a measured request size; the JSON body is larger and a batch body is larger
  still. Treat it as a label, not a measurement.
- No stage timestamps. The `stages` array is always empty in this build; there
  are no T0/T1/T2/T3 stamps.
- No confidence intervals. `p50_ci95` and `p99_ci95` are null.
- No raw `.hdr` files. The manifest's summaries are the raw artifact.
- No fill notifications. A cancel can race a fill and be rejected; those
  rejections appear in `error_count`.
- No fanout, replay, failover, or soak workloads.
- No CPU pinning or isolation inside the harness. The Linux runner applies
  `taskset` when asked and records governor and SMT state.
- No server-side metrics correlation; the gateway's own histograms are
  cumulative and separate from the harness histograms.

## Run matrix

The matrix is the target protocol from the research dossier. The last column
is the honest status of this harness.

| ID | Workload | Vary | Duration | Harness support today |
|---|---|---|---|---|
| L1 | 64 B orders, 1 symbol | 1k / 5k / 10k / 25k / 50k / 100k orders/s | 5 min x 5 | Partial. HTTP JSON, mixed limit/IOC/cancel flow, optional batching. Payload size is not 64 B on the wire. |
| L2 | 256 B, 10 symbols | 10k / 50k orders/s | 5 min x 5 | Partial. Only three instruments exist; the runner labels the row `L2-3sym`. |
| F1 | 10k orders/s | 1 / 10 / 100 / 1000 subscribers | 5 min x 5 | No. `subscribers` is always 0; there is no fanout workload. |
| F2 | Fixed 100k egress/s | 10 / 100 subscribers | 5 min x 5 | No. |
| R1 | 10M replay | 0 / mid / tail; 1 / 10 / 100 consumers | drain x 5 | No. `whippet replay` exists for correctness, not replay throughput. |
| R2 | Replay + live 10k/s | 1 config | 5 min x 5 | No. |
| C1 | 10k/s, 3-node | 20 random kill phases | 30 s each | No. Single node. |
| S1 | 80% SLO knee soak | 1 config | 30 min x 3 | No. No knee-finding or soak mode. |

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

Fresh stack, simulator stopped, snapshots disabled
(`WHIPPET_SNAPSHOT_EVERY=0`), 1,000 orders/s, 8 s measured, 2 s warmup, one
repetition, fixed seed `0x5eed0001`. **`dev-loop`, not a result.**

| Batch | Request rate | p50 | p99 | Samples | Errors | Labels |
|---|---|---|---|---|---|---|
| 1 | 1,000 /s | 569 µs | 66,111 µs | 8,000 | 0 | `dev-loop` |
| 10 | 100 /s | 2,503 µs | 62,623 µs | 8,000 | 0 | `dev-loop` |
| 100 | 10 /s | 16,671 µs | 29,391 µs | 8,000 | 0 | `dev-loop`, `generator-bound` |
| 256 | 3.9 /s | 43,807 µs | 99,391 µs | 8,208 | 0 | `dev-loop` |

What the table does and does not say:

- Achieved throughput is 1,000 orders/s at every batch size. Batching changes
  the request count, not the offered load.
- p50 rises with batch size because every order in a batch inherits the
  completion of the whole durable append. That is the coordinated-omission
  corrected batch latency, not a single-order median regression.
- p99 is dominated by Docker Desktop stalls and is not monotonic across batch
  sizes. An 8-second run cannot resolve a p99 difference; the 5-minute native
  Linux L1 run is the measurement for the tail.
- The batch-100 run is labelled `generator-bound` (slip p99 2,341 µs is 8% of
  latency p99). Its tail is partly the pacer.
- The batch-256 run has 8,208 measured samples because warmup is trimmed to
  whole batches (7 batches, 1,792 orders), so its measured span is 8.208 s.
  `achieved_per_sec` uses that real span.

The same sweep with the default snapshot interval (every 1,000 commands)
produced p50 of 7-48 ms and p99 up to 1.1 s. That is snapshot I/O of the order
book, not ingress, and it makes the batch sizes indistinguishable. Always
record the snapshot setting next to the manifest.

The batch sweep is only meaningful on the same host with the same commit. A
Docker Desktop sweep is `dev-loop` and cannot be a headline.

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
| `WHIPPET_BENCH_BATCH_SIZE` | 1 | orders per HTTP request, 1..=256 |
| `WHIPPET_BENCH_IOC_RATIO` | 0.10 | IOC share of ingress events |
| `WHIPPET_BENCH_CANCEL_RATIO` | 0.10 | cancel share of ingress events |
| `WHIPPET_BENCH_SEED` | `0x5eed0001` | RNG seed; fixed so runs compare |

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
- Add stage timestamps and populate `stages`.
- Add bootstrap confidence intervals.
- Add fill notifications so a cancel can distinguish "raced a fill" from a
  real rejection.
- Add a binary transport path for the hot-path claim.
- Add fanout, replay, failover, and soak workloads.
- Export raw histogram data alongside the manifest if a consumer needs it.
- The open-order working set bounds the harness's tracked ids; aggressive
  limit remainders are not tracked and can still rest. The steady-state check
  and `error_count` are the signals if that matters for a long run.

Until those exist, the only defensible statement is: the harness, manifest, and
scope labels work; the M4/Docker numbers are dev-loop smoke data; there are no
headline results.
