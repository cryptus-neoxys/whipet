# ADR 0020: Explicit shard lifecycle and the readiness invariant

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0010](0010-shard-thread-and-journal-pipeline.md) (shard pipeline),
  [ADR 0011](0011-fault-injection-and-stage-budget.md) (fault injection),
  [INVARIANTS.md](../INVARIANTS.md), [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

Readiness was one `AtomicBool` named `shards_ok` that started `true` and was
only flipped on a failure. A node could report ready before its engine or MD
tasks had started, and health could not say which shard or role was missing.
The implicit state machine — each active shard runs one engine task and one MD
task, each Starting/Running/Failed/Stopped — was not represented anywhere.

## Decision

### Per-role state machine

`crates/whippet/src/health.rs` defines:

```text
Starting -> Running -> Stopped
Starting/Running -> Failed        (terminal)
Failed/Stopped are terminal until the process restarts
```

`ShardState` (with stable string/code forms) and `ShardRole` (`Engine`, `Md`)
are stored as one `AtomicU8` per `(shard, role)` in `ShardHealth`. Transitions
are debug-asserted and refused in release, so a `Failed` role can never become
`Running` again in the same process. An unknown byte decodes as `Failed` (fail
closed). Out-of-range shard ids are a debug assertion and read as `Failed`.

### Readiness invariant

> The node is ready iff the shard latch is open and every active shard's
> **engine and MD** role is `Running`.

A fresh context is `Starting`, so it is not ready. Any `Failed` or `Stopped`
role keeps the node out of rotation. `/readyz` returns 503 with the per-shard
states; `/api/health` reports `shards_ok` plus a `shards` array of
`{partition, engine, md}` strings. Ingress (`ensure_shards_ready`, TCP submit)
uses the same predicate, so an unready node rejects orders before any append.

### Task wiring

- `run_engine_shard` sets `Starting` at entry, `Running` after restore, the
  committed-offset read, turbo anchor/tail replay, and the initial MD append,
  then starts the rings/reader/matcher/writer. It sets `Stopped` on clean
  shutdown and `Failed` on every error path.
- `run_md_shard` sets `Starting` before its committed-offset read, `Running` at
  loop entry, `Stopped` on shutdown, and `Failed` on error.
- The node spawner marks only the role that failed. A reader that self-fails
  while the shard body returns `Ok` keeps `Failed` (it is not relabelled
  `Stopped`), so readiness stays false.
- The initial MD snapshot append remains warn-only (pre-existing): the engine
  role can still become `Running` with an empty book if that append fails; the
  first order's MD will populate the hub.

## Alternatives rejected

- **Keep the bool:** cannot distinguish Starting from Running, cannot name the
  failing role, and starts ready.
- **One state per shard instead of per role:** the engine can be running while
  the MD consumer is dead; readiness must require both.
- **Restart a failed role in-process:** recovery semantics (snapshot, offsets,
  ownership) belong to a deliberate restart, not a health toggle.
- **A mutex-guarded map of states:** the readiness check is consulted by
  ingress; atomics keep it lock-free.

## Consequences

- Readiness is stricter: a node is not ready until both roles of every active
  shard have completed startup. This is the correct signal for a load balancer.
- One atomic per role, transitions only at startup/shutdown/failure; no
  per-command cost.
- A stopped role is terminal for the process; restarting the node is the
  recovery action.
- `shards_ok` remains for existing clients and the soak sampler; it is now
  derived from the explicit state rather than a latch that starts true.
