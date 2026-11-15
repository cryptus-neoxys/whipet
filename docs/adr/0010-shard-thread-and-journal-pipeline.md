# ADR 0010: Shard thread and journal pipeline

- **Status:** Accepted — implemented
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0009](0009-performance-budget-and-optimization-order.md),
  [ADR 0003](0003-matching-engine-data-structures.md),
  [ARCHITECTURE.md](../ARCHITECTURE.md#single-writer-not-a-pinned-thread),
  [PERFORMANCE.md](../PERFORMANCE.md#phase-15c-implemented-end-to-end-sweep-pending),
  [FAILURE-MODES.md](../FAILURE-MODES.md#matrix)

## Context

After the first optimization tranche recorded in [ADR 0009](0009-performance-budget-and-optimization-order.md),
the matcher is still a Tokio task that awaits the journal. The tranche removed
the sequential round trips — polling and appends now overlap through the joined
poll/prefetch in `run_engine_shard` — but the structure is unchanged:

- **The matcher shares a runtime.** The shard task is a Tokio task on the
  multi-thread runtime. Between batches it can migrate cores, and any other
  task on the runtime (gateway, MD hub, WebSocket, health monitor) can run
  between the matcher's polls. This is jitter the matcher does not cause and
  cannot control.
- **The snapshot hash still runs on the loop.** `engine.snapshot_state()`
  clones the book and the dedup table and computes the state hash on the shard
  task before the handoff to the snapshot writer. Every
  `WHIPPET_SNAPSHOT_EVERY` commands, that work owns the loop.
- **The metrics histogram is global.** `Metrics::record_match_latency_us`
  locks a shared `Mutex<Histogram>` once per command. It is outside the pure
  matcher but on the serial path of every shard, and all shards contend for
  the same lock.

The single-writer property is logical, not physical: there is one task per
partition, but nothing pins it to a core or keeps other work off its thread.

## Decision

Restructure each active shard as a three-stage pipeline on a dedicated OS
thread, as implemented in `crates/whippet/src/engine_runner.rs`:

```text
read_orders (async task, owns the orders journal client)
        │  bounded input ring (reader → matcher, capacity 2)
        ▼
whippet-shard-<id> (dedicated OS thread, one per active shard, owns ShardEngine)
        │  bounded output ring (matcher → writer, capacity 2)
        ▼
run_writer (async task, owns the journal client for outputs)
```

- **One dedicated OS thread per active shard** (`std::thread::Builder`, name
  `whippet-shard-<id>`). The matcher thread owns `ShardEngine`. It never
  awaits and never calls the journal. It is the only code that touches the
  partition's books, so the single-writer property has a thread behind it,
  not just a task.
- **Bounded input ring (reader → matcher, capacity 2).** The async
  `read_orders` task polls `read_payloads_into` and hands one poll's worth of
  records to the matcher. It validates every offset in the batch: the first
  offset must equal the requested offset and the span must equal the record
  count, so a gap or an overlap marks the shard failed instead of silently
  skipping an order.
- **Recycle channel.** The matcher hands the polled buffer back to the reader,
  which refills it with `read_payloads_into` instead of allocating a `Vec`
  per batch.
- **Bounded output ring (matcher → writer, capacity 2).** Executions, MD
  events, and MD snapshots flow to the writer in match order.
- **The writer owns the async journal edge.** `run_writer` joins the
  Executions and MD appends, commits the consumer offset, and forwards
  snapshots. An output append is retried up to 30 times (`APPEND_MAX_ATTEMPTS`)
  with backoff and a shutdown watch; on exhaustion the shard is marked failed
  and the applied offset and the commit are **not** advanced. Advancing them
  would let a restart skip orders whose executions or MD never reached the
  log — previously a silent "gateway acks, engine stops matching" mode.
- **Appended-offset snapshot gate.** An `appended_offset` atomic holds the
  highest offset whose executions and MD are actually in the journal. A
  periodic snapshot is only taken once every output up to the matcher's
  current offset is appended; a snapshot that comes due while the writer is
  behind is deferred and taken at the next batch boundary where the gate is
  open. The snapshot can therefore never claim a resume offset whose outputs
  were lost.
- **The snapshot hash moves off the shard.** The matcher calls
  `snapshot_state()` with the live hash; the blocking writer saves it and
  verifies the final snapshot by rebuilding. (An off-loop hash was tried and
  reverted after a ~170k-order shard disagreed with the restored engine.) The clone still happens
  on the matcher thread at the snapshot interval; the hash and the file write
  happen on the writer.
- **Per-shard metrics.** Each matcher records into its own `ShardMetrics`
  (lock-free counters and a local histogram), merged into the global
  `Metrics` once per second and once on exit. No lock per command.
- **Snapshot format version 2.** `SymbolSnapshot` now carries the per-symbol
  config and `BookSnapshot` carries `min_price_ticks`, so the hash can be
  reproduced from the snapshot alone. Version 1 files are rejected.

### Backpressure and shutdown rules

- **When the input ring fills, the reader applies backpressure**: `send`
  awaits until the matcher drains. Nothing is dropped or skipped; log order is
  preserved by construction. Ring capacity (2 batches) is the backpressure
  boundary, not a tunable queue.
- **When the output ring fills, the matcher blocks** (`blocking_send`) until
  the writer drains space. Outputs are never dropped, because a dropped
  execution or MD event breaks the at-least-once contract and the per-symbol
  `md_seq` continuity.
- **A matcher panic marks the shard failed rather than hang.** A panicked
  matcher thread leaves a shard that can neither match nor commit; the join
  detects the dead thread, marks the shard failed, and the shard errors out
  instead of waiting forever on a ring that will never drain.
- **Graceful shutdown drains the rings.** Shutdown closes the reader, which
  closes the matcher input; the matcher sends its final batch with the final
  snapshot and exits, the writer appends and commits the remaining outputs,
  and the final snapshot is persisted inside the shutdown budget. The bounded
  rings make the drain finite.

## Consequences

Positive:

- **Better jitter and core locality.** The matcher runs on its own thread; the
  runtime cannot migrate it or interleave other tasks between polls.
- **Bounded memory per shard.** The rings cap in-flight work independent of
  the poll batch size, and the recycle channel removes the per-batch read
  allocation.
- **The per-command path sheds its periodic and shared costs.** The snapshot
  hash and file write happen on the blocking writer; the per-command metrics
  lock is gone; the snapshot clone runs between batches, not per record.
- **Cleaner match-loop invariants.** "No I/O, no await, no lock in the match
  loop" is now a property of a thread that has no async context at all.
- **The silent stop is closed.** A wedged output topic no longer stalls
  matching forever while the gateway keeps acknowledging: the bounded append
  retry fails the shard, and the applied offset and commit are not advanced,
  so a restart replays from the last durable point instead of skipping orders.

Negative:

- **One thread per active shard** (three in this deployment). Idle shards hold
  a parked thread; that is acceptable at this scale and revisited if the active
  partition count grows.
- **Backpressure replaces buffering.** When a ring fills, throughput limits to
  the slowest stage — the writer's broker round trips — instead of letting
  reads queue unboundedly. That is the intended behavior, but it changes where
  a burst absorbs.
- **A wedged output topic now stops its shard.** The pre-change behavior
  matched forever and silently; the new behavior is loud but is still a stop.
  Operators see degraded health and must restart the node.
- **New failure surface.** Thread panic detection, ring drain on shutdown, the
  final-snapshot verification and the bounded append retry each need fault handling
  that the single-task design did not have. The code paths exist; the
  panic-injection and ring-full fault tests are still pending
  ([FAILURE-MODES.md](../FAILURE-MODES.md#matrix)).
- **Snapshot format version 2.** `SymbolSnapshot` carries the per-symbol
  config and `BookSnapshot` carries `min_price_ticks`, so the hash is
  reproducible from the snapshot alone. Version 1 files are rejected; an
  existing deployment must rebuild snapshots (replay or a graceful-shutdown
  final snapshot) before upgrading.
- **The state-hash contract must survive the move.** The hash computed in the
  snapshot writer must equal the hash the matcher's engine would have computed;
  `whippet verify` and the replay determinism check are the guards.

Neutral:

- The microbenchmarks that came with the implementation (the hash
  cost and the per-shard merge cost) are recorded in
  [PERFORMANCE.md](../PERFORMANCE.md#phase-15c-implemented-end-to-end-sweep-pending). No end-to-end before/after
  sweep exists yet, so the latency effects remain structural expectations,
  not measurements, per [ADR 0009](0009-performance-budget-and-optimization-order.md).
- The topic layout, partition keying, and at-least-once contract are unchanged
  by this ADR. The snapshot format is not: version 2 is a data-layout change,
  covered above.

## Alternatives considered

- **`spawn_blocking` per batch.** Rejected: it pays thread-pool handoff and
  scheduling overhead per batch, keeps the matcher on a shared pool where it
  can migrate, and still does not give the matcher a stable core or protect it
  from pool contention.
- **A second Tokio runtime per shard.** Rejected: it isolates the shard from
  the main runtime, but the match path still has async I/O on it (poll,
  append, commit are awaits on that runtime), so the matcher still yields and
  migrates. The problem is I/O on the match path, not which runtime hosts it.
- **A full actor framework.** Rejected: dependency and supervision complexity
  for three shards. The pipeline is three stages with explicit rings; a
  framework would buy supervision the node already implements at shard level
  (mark failed, degraded health) without matching the shutdown discipline.

## Status

Accepted — implemented. The thread-and-rings pipeline is in
`crates/whippet/src/engine_runner.rs`: the matcher thread (`whippet-shard-<id>`)
owns the engine, the reader and writer tasks own the journal edge, the rings
and the recycle channel are bounded at capacity 2, the snapshot hash runs on
the blocking writer, metrics are per-shard, and the snapshot format is version
2.

Still open, and required before the latency effects can be claimed: the
end-to-end before/after sweep described in
[PERFORMANCE.md](../PERFORMANCE.md#phase-15c-implemented-end-to-end-sweep-pending), and the panic-injection and
ring-full fault tests recorded in
[FAILURE-MODES.md](../FAILURE-MODES.md#matrix). A later ADR supersedes this
one if the shutdown discipline, the snapshot gate, or the failure semantics
change.
