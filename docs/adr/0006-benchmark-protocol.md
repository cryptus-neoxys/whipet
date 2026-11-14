# ADR 0006: Benchmark protocol

- **Status:** Accepted; Phase 1 implementation is a subset
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0005](0005-dashboard-stack-and-rendering.md), [ADR 0007](0007-single-node-v1-cluster-phase-3.md)

## Context

A latency number without a method is an anecdote. Whippet's benchmark has to
answer four questions before it publishes anything:

1. Was the load generated open-loop, or did the generator slow down when the
   system slowed down?
2. Is the reported latency measured from the intended send time, or only from
   the moment the request happened to leave?
3. Is the distribution reported with enough resolution and enough samples to
   support the percentile claim?
4. Can someone else reproduce the run from the manifest?

The project also has a laptop problem. The development machine is an Apple M4
with 16 GB, running Docker Desktop. That is useful for regression testing and
for catching gross errors, but it is not a headline platform. The benchmark
must label it as a dev-loop result automatically, not rely on the author to
remember.

## Decision

### Open-loop, absolute-deadline pacing

The harness schedules every request at an absolute deadline:

```text
deadline_i = start + i * (1 s / rate)
```

It sleeps until shortly before the deadline, spins for the last few hundred
microseconds, and sends. The measured latency is:

```text
latency_i = completion_i - deadline_i
```

not `completion_i - send_i`. This is the coordinated-omission correction: a
request that is delayed by the system still carries the delay in its latency
sample. The harness also records:

```text
scheduler_slip_i = actual_send_i - deadline_i
```

If the generator cannot keep up, `scheduler_slip` grows and the run says so. If
the in-flight semaphore is full, the harness records a failed sample with
`latency = slip` and increments the error count instead of silently dropping the
tick. This is not a full HdrHistogram `record_correct` backfill of every missed
deadline; it is the simpler deadline-based accounting that catches the common
coordinated-omission bias. A future revision can add explicit backfill and
report both.

### HDR histograms

Latency and scheduler slip are recorded in `hdrhistogram::Histogram<u64>` with:

- microsecond units;
- three significant figures;
- a 60-second maximum, with values clamped to 59,999,999 µs.

The summary reports count, min, mean, p50, p90, p99, p99.9, p99.99, and max.
The manifest schema already has optional `p50_ci95` and `p99_ci95` fields, but
Phase 1 leaves them `null`; bootstrap confidence intervals are not implemented
yet. Do not describe a Phase 1 result as having a confidence interval.

### Five repetitions and a median headline

The default is **5 repetitions**, each with a warmup (default 5 s) excluded from
the measured samples and a measured duration (default 30 s). The headline is the
repetition with the **median p99**, never the best run. The manifest keeps every
repetition, so a reader can see the spread.

The current defaults in `BenchConfig` are:

| Setting | Default |
|---|---|
| Rate | 1,000 orders/s |
| Duration | 30 s |
| Warmup | 5 s |
| Repetitions | 5 |
| Max in flight | 4,096 |
| Request timeout | 5,000 ms |
| Transport | HTTP (`POST /api/orders`) |
| Payload | 96 bytes (JSON order) |
| Seed | `0x5eed_0001` |

### Manifest

Every run writes a JSON manifest (`BenchManifest`, schema version 1) with:

- `run_id`, `created_at`, `git_sha`, `git_dirty`, optional `image_digest`;
- host: hostname, OS, kernel, CPU model, physical/logical cores, memory,
  virtualization, Docker version, governor, SMT, isolated cores;
- workload: name, mode, transport, payload bytes, symbols, partitions, rate,
  duration, warmup, repetitions, subscribers, seed, durability,
  consumer-offset durability, batch size, encryption;
- throughput: target, achieved, goodput, errors, scheduled/completed samples,
  scheduler-slip summary;
- latency: the HDR summary;
- stages: reserved for stage breakdown (empty in Phase 1);
- every repetition, notes, and labels.

`scope_label()` returns `headline` only for a native Linux host. Docker Desktop,
OrbStack, and Linux VMs return `dev-loop`. The M4 result is therefore a
**dev-loop** measurement by construction. It is never a headline claim, and the
project does not publish its numbers as a comparison with Iggy's own benchmark
results.

### What Phase 1 does not measure

- No TCP ingress benchmark. The harness uses HTTP; the manifest notes that the
  intended Linux headline path is TCP on a native host, but that path is not
  implemented yet.
- No stage breakdown. `stages` is empty; there are no T0/T1/T2/T3 timestamps and
  no `quanta`/TSC calibration.
- No bootstrap confidence intervals.
- No per-subscriber fanout benchmark.
- No failover or cluster benchmark. Phase 1 is single-node.
- No encryption or batching A/B. The manifest records the fields, but the
  harness does not vary them.

These are Phase 2/3 items, not hidden claims. A result that depends on them
must wait until they exist.

## Consequences

Positive:

- The harness cannot accidentally report a closed-loop number as open-loop; the
  deadline is the measurement origin.
- Scheduler slip is a first-class output. A generator that falls behind
  invalidates the run visibly instead of inflating throughput.
- The manifest makes a run reproducible and forces hardware, commit, and
  durability labeling.
- The dev-loop label is automatic, so a laptop result cannot be presented as a
  headline by accident.

Negative:

- The pacer is a Tokio task with a spin tail. It is adequate for the demo's
  rates but is not a kernel-bypass load generator; very high rates need a
  dedicated pinned generator thread.
- The HTTP transport adds JSON encoding and a TCP stack to the measured path.
  That is a convenience path, not the low-latency claim.
- The `mpsc` collector channel is bounded at one million samples. A very long
  run at a very high rate can apply backpressure; the current defaults do not.
- Median-of-five is a robust headline, but it is not a confidence interval. The
  manifest says so by leaving the CI fields null.
- The harness records a failed sample when the in-flight limit is reached, which
  mixes generator saturation into latency. That is intentional (it is a
  coordinated-omission symptom), but it must be read alongside `error_count`.

## Alternatives considered

- **Closed-loop request/response pacing.** Simple, but it hides coordinated
  omission: when the system slows down, the generator slows down too. Rejected.
- **`wrk2` or another external load generator.** Good HTTP benchmarking, but it
  cannot generate Whippet order semantics, per-symbol mixes, or a manifest with
  the exchange's mode/durability fields. Rejected for the primary harness.
- **Criterion/iai-callgrind.** Excellent for microbenchmarks of the matcher and
  codec. Not a system-level open-loop harness. Kept as a future complement, not
  a replacement.
- **JMeter/Gatling.** Heavy, JVM-based, and not designed for microsecond
  latency. Rejected.
- **Publish the best of five runs.** Better-looking, dishonest. Rejected;
  median p99 is the headline.
- **Publish M4 numbers as headline.** Rejected; the manifest labels them
  `dev-loop`, and the documentation must do the same.

## Status

Accepted. Phase 1 implements open-loop deadline pacing, HDR summaries, five
repetitions, the manifest, and the dev-loop label. Stage breakdown, bootstrap
CIs, TCP ingress, fanout, encryption A/B, and failover are not implemented and
must not be claimed. Any change to the measurement origin, the repetition
policy, or the headline statistic requires a new ADR.
