# ADR 0011: Fault injection, stage budgets, and soak gates

- **Status:** Proposed — implementation in progress
- **Date:** 2026-11-15
- **Supersedes:** none
- **Related:** [ADR 0009](0009-performance-budget-and-optimization-order.md),
  [ADR 0010](0010-shard-thread-and-journal-pipeline.md),
  [PERFORMANCE.md](../PERFORMANCE.md), [FAILURE-MODES.md](../FAILURE-MODES.md),
  [BENCHMARKS.md](../BENCHMARKS.md)

## Context

Phase 1.5c moved the matcher onto a dedicated OS thread with bounded rings,
moved the snapshot hash into the blocking writer, and made match metrics
per-shard. The measured dev-loop tails improved (TCP batch 10 at 10,000/s:
p99 10.6 ms → 3.9 ms), but three things remain unproven:

1. **Failure behavior.** The new pipeline has explicit failure handling — a
   bounded output-append retry that stops the shard, an `appended_offset` gate
   on periodic snapshots, ring backpressure, and a matcher-panic join — but no
   test injects those failures. A regression could silently restore the
   "gateway acknowledges, engine stops matching" mode.
2. **Stage attribution.** The harness records client-side send and wire time.
   The node exposes end-to-end ingress and match percentiles, but not where the
   time goes inside a request or a batch. Optimization decisions are therefore
   partly guesswork.
3. **Long-run behavior.** No soak or crash proof is automated. The replay
   proof is run by hand, and the dedup table, snapshot cadence, and commit
   window have not been exercised for ten minutes under load.

## Decision

### 1. Test doubles for the journal edge

Introduce the smallest seam that lets the shard pipeline run against a fake
journal:

- A `JournalOps` trait (async `append`, `commit`, `committed`,
  `read_payloads_into`, `is_iggy`, `ping`) implemented for `Journal`.
- The reader and writer take the trait, not the concrete `Journal`, so tests
  can fail a chosen topic on demand.
- The fake lives in test support, not in the production append path. No
  test-only branch is added to `IggyJournal`.

Tests cover: output append fails once (no commit until success, no output gap),
output append fails forever (shard marked failed, error returned, applied offset
not committed), matcher panic (join detects, shard marked failed), ring full
(producers block, memory bounded), and reader gap/overlap (shard marked failed).

### 2. Stage histograms

Add a `Stage` enum and per-stage histograms to `Metrics`:

`GatewayParse`, `GatewayValidate`, `GatewayEncode`, `GatewayAppend`,
`EngineProcess`, `EngineAppend`, `EngineCommit`, `MdApply`, `WsFrame`.

One sample per request or batch, not per order. Expose p50/p99 as
`whippet_stage_p50_us{stage="..."}` and `whippet_stage_p99_us{stage="..."}` in
`/metrics`. `MetricsView` does not change, so the dashboard and `/api/state`
keep their shape.

### 3. Soak and crash gates

- `scripts/soak.sh`: ten minutes at 5,000 orders/s, HTTP batch 10, simulator
  stopped, sampling `/api/state` every ten seconds. Fails on p99 drift >5%
  between the first and last tenth of samples, a failed shard, or unbounded
  dedup growth.
- `scripts/crash-test.sh`: run a short workload, kill the node, restart, wait
  for readiness, run `make replay`, and assert both replay hashes match.
- `make soak` and `make crash-test` are the entry points.

### 4. Fanout under ingress

The fanout harness gains `--ingress-rate R`: it drives real orders at R/s while
N WebSocket clients measure frames, bytes, and gaps. The manifest records the
ingress rate, achieved rate, error count, and `/metrics` deltas, so the report
can show whether subscribers slowed the engine.

## Consequences

- Failure modes become executable specifications. A regression in the shard
  pipeline fails CI rather than showing up as a silent stall in a demo.
- The stage budget becomes measurable. The next optimization is chosen from
  p50/p99 per stage instead of from a whole-request number.
- The soak and crash scripts are slow; they are release gates, not per-commit
  gates. `make ci` stays fast.
- The `JournalOps` trait adds one generic parameter to the reader and writer.
  The concrete `Journal` path keeps the same behavior; the trait is not
  object-safe if it uses `async fn`, so the functions stay generic.
- Stage histograms add one clock read per request or batch. The batch path
  shares one `Instant` across its orders.

## Alternatives rejected

- **Rely on live Iggy for failure tests.** Slow, flaky, and hard to make
  deterministic. A fake journal exercises the exact error path in
  milliseconds.
- **Skip stage timers and profile instead.** A profiler shows CPU, not the
  awaited Iggy round trip that dominates the durable p50. Stage histograms are
  the only way to attribute wall time on the ack path.
- **Run the soak on every commit.** Ten minutes per run is too slow for the
  inner loop; a nightly or release gate is enough.
- **A production `FailingJournal` variant.** It would put test-only behavior in
  the shipped enum. Test support is cleaner.

## Status

Implementation in progress. No number in this ADR is a measurement; the
measured figures live in [PERFORMANCE.md](../PERFORMANCE.md) and
[BENCHMARKS.md](../BENCHMARKS.md) with their scope labels.
