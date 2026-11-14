# Benchmarks

The benchmark method is part of the product. A latency number without a host,
commit, configuration, sample count, and manifest is not a result.

## Status

There are **no headline Linux numbers** in this repository yet. The only
recorded run is a three-second smoke test on an M4 with Docker Desktop. It is
included below because it is useful for regression and because hiding it would
be worse than labeling it. It is not a benchmark result.

The harness currently measures one workload shape: HTTP `POST /api/orders`
with limit orders. Fanout, replay throughput, failover, and soak runs are in
the matrix but not implemented. The run-matrix table says which rows the
harness can execute today.

## Method

**Open-loop pacing.** Request `i` is due at `t0 + i * period`. The generator
sleeps until shortly before the deadline and spins the last few hundred
microseconds. It does not wait for a response before scheduling the next
request. A slow system therefore builds a queue; it does not slow the
generator.

**Latency from the deadline.** For each completed request:

```text
latency = completion_time - scheduled_deadline
```

This is the coordinated-omission correction. Measuring from the send would
hide time spent waiting before the request left.

**Scheduler slip.** The generator records `wake_time - deadline` separately.
Slip is a first-class result. If slip is comparable to latency, the run is
generator-bound and the latency tail says more about the pacer than about the
system.

**HDR histograms.** Latency and slip go into HDR histograms with three
significant digits and a 60-second maximum. The manifest reports min, mean,
p50, p90, p99, p99.9, p99.99, and max in microseconds. Percentiles are never
averaged across runs.

**Warmup.** Warmup ticks are scheduled and sent but excluded from the measured
samples. The CLI default is 5 seconds.

**Repetitions.** The harness runs `--repetitions` times (default 5). The
headline in the manifest is the repetition with the median p99, not the best
run. Every repetition is kept in the `repetitions` array.

**Manifest.** Every run writes `artifacts/bench/<run-id>.json` with the host,
commit, dirty flag, optional image digest, workload, throughput, latency,
per-repetition results, notes, and scope labels. The field-by-field reference
is [BENCHMARK-MANIFEST.md](BENCHMARK-MANIFEST.md).

**Scope labels.** `scope_label()` returns `headline` only when the host reports
`virtualization = "native"` and the OS is Linux. Docker Desktop, OrbStack,
Linux VMs, macOS, and unknown hosts are `dev-loop`. The manifest records the
label; the docs must use it.

## What the harness does today

- Sends HTTP JSON orders to `--gateway` (default `http://127.0.0.1:8080`).
- Uses limit orders only, with prices 0-10 ticks around the instrument
  reference price, alternating buy and sell, quantity 1-50.
- Rotates symbols in the configured list. The default is the three-symbol
  demo set.
- Generates a unique `client_order_id` per run, repetition, and tick, so a
  repeated run does not collide with the engine's dedup table.
- Caps in-flight requests at 4,096 and times out each request at 5 seconds.
- Excludes warmup samples.
- Writes one JSON manifest.

What it does not do:

- No TCP, QUIC, or FIX ingress. The transport is HTTP JSON.
- No binary payload sizing. `payload_bytes` is a fixed 96 in the manifest, not
  a measured request size; the JSON body is larger. Treat it as a label, not a
  measurement, until the harness records the real size.
- No stage timestamps. The `stages` array is always empty in this build; there
  are no T0/T1/T2/T3 stamps.
- No confidence intervals. `p50_ci95` and `p99_ci95` are null.
- No raw `.hdr` files. The manifest's summaries are the raw artifact.
- No steady-state drift check. The 10-second-window / 5% drift rule in the
  research dossier is not implemented.
- No fanout, replay, failover, or soak workloads.
- No CPU pinning, isolation, or `SYS_NICE`.
- No server-side metrics correlation; the gateway's own histograms are
  cumulative and separate from the harness histograms.
- The in-flight-cap skip is counted twice in `error_count` (once as a failed
  sample, once as a spawn error). Treat `error_count` as an upper bound until
  the harness is fixed.

## Run matrix

The matrix is the target protocol from the research dossier. The last column
is the honest status of this harness.

| ID | Workload | Vary | Duration | Harness support today |
|---|---|---|---|---|
| L1 | 64 B orders, 1 symbol | 1k / 5k / 10k / 25k / 50k / 100k orders/s | 5 min x 5 | Partial. HTTP JSON, three symbols available, rate is a flag. Payload size is not 64 B on the wire. |
| L2 | 256 B, 10 symbols | 10k / 50k orders/s | 5 min x 5 | No. Only three instruments exist and payload size is fixed. |
| F1 | 10k orders/s | 1 / 10 / 100 / 1000 subscribers | 5 min x 5 | No. `subscribers` is always 0; there is no fanout workload. |
| F2 | Fixed 100k egress/s | 10 / 100 subscribers | 5 min x 5 | No. |
| R1 | 10M replay | 0 / mid / tail; 1 / 10 / 100 consumers | drain x 5 | No. `whippet replay` exists for correctness, not replay throughput. |
| R2 | Replay + live 10k/s | 1 config | 5 min x 5 | No. |
| C1 | 10k/s, 3-node | 20 random kill phases | 30 s each | No. Single node. |
| S1 | 80% SLO knee soak | 1 config | 30 min x 3 | No. No knee-finding or soak mode. |

Running L1 with the current harness produces a valid HTTP-ingress measurement,
not the binary-ingress L1 in the dossier. Label it accordingly.

## Smoke run: smoke, not a benchmark

This is the one recorded run. It is a smoke test: one repetition, three
seconds of measured traffic, one second of warmup, on a laptop with Docker
Desktop. It exists to show that the harness and manifest work end to end.

| Field | Value |
|---|---|
| Label | `smoke` |
| Scope | `dev-loop` |
| Host | Apple M4, 10 cores, 16 GB, macOS kernel 27.2.0 |
| Container runtime | Docker 29.9.2 |
| Commit | `8eca5ff` (dirty tree) |
| Workload | HTTP JSON limit orders, three symbols, 16 partitions |
| Rate | 200 orders/s |
| Duration | 3 s measured, 1 s warmup |
| Repetitions | 1 |
| Samples | 600 |
| Errors | 0 |
| p50 | 3,257 µs |
| p90 | 6,143 µs |
| p99 | 54,495 µs |
| p99.9 | 64,159 µs |
| Max | 64,159 µs |
| Scheduler slip p50 | 1,333 µs |
| Scheduler slip p99 | 6,227 µs |

Why this is not a result:

- One repetition, three seconds, no steady-state check.
- Generator slip is a large fraction of the latency. The p99 is partly the
  pacer, not the exchange.
- Docker Desktop on macOS adds VM and filesystem noise.
- HTTP JSON ingress is the convenience path, not the binary hot path.
- The sample count is far too small for p99.9, let alone p99.99.

Do not quote these numbers in a README, a slide, or a comparison table. If you
need a first look on your own machine, run the same command and compare it to
itself, not to another system.

## Running a benchmark

```sh
make bench
```

Manual, with explicit settings:

```sh
cargo run --release -p whippet -- bench \
  --gateway http://127.0.0.1:8080 \
  --rate 1000 --duration 30 --warmup 5 \
  --repetitions 5 --label L1
```

Inside Compose:

```sh
docker compose -f deploy/compose.yaml exec node \
  whippet bench --rate 1000 --duration 30 --warmup 5 --repetitions 5 --label L1
```

The CLI prints one summary line and the manifest path. The manifest is written
under `artifacts/bench/`. If the container path is not mounted to the host,
copy it out before resetting the stack.

For a `dev-loop` run, set `WHIPPET_VIRTUALIZATION=docker-desktop` (or
`orbstack`, `linux-vm`) so the manifest records the environment explicitly.
The scope label is `dev-loop` either way on a non-native-Linux host.

## Publishing a run

1. Run at least five repetitions with a warmup.
2. Keep the machine quiet; do not run a simulator or another benchmark
   against the same node.
3. Copy the manifest, unchanged, into a run directory under
   `docs/benchmarks/`.
4. Write the method card next to it. Use the template in
   [docs/benchmarks/README.md](benchmarks/README.md).
5. Report the median-p99 repetition as the headline, plus the min/max spread
   across repetitions and the sample count.
6. Label the scope. If it is not native Linux, it is `dev-loop`.
7. Link the raw manifest. Do not retype numbers into a table without linking
   the source.

Never hand-edit a manifest. If a field is wrong, fix the harness and rerun.

## Known gaps before a headline run

- Add a native Linux runner and record `lscpu`, governor, SMT, and isolation.
- Record the real request and response sizes, not a fixed 96.
- Add stage timestamps and populate `stages`.
- Add bootstrap confidence intervals.
- Add the steady-state drift check.
- Add a binary transport path for the hot-path claim.
- Add fanout, replay, failover, and soak workloads.
- Export raw histogram data alongside the manifest if a consumer needs it.

Until those exist, the only defensible statement is: the harness, manifest,
and scope labels work; the M4/Docker numbers are dev-loop smoke data; there
are no headline results.
