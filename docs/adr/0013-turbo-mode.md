# ADR 0013: Turbo mode — local sequencer, direct fanout, bounded uncommitted window

- **Status:** Proposed
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0002](0002-durable-default-turbo-toggle.md) (durable default, turbo deferred),
  [ADR 0004](0004-iggy-topics-partitions-durability.md) (topic layout),
  [ADR 0010](0010-shard-thread-and-journal-pipeline.md) (shard thread and rings),
  [ADR 0011](0011-fault-injection-and-stage-budget.md) (fault injection),
  [ADR 0012](0012-per-role-journals-and-ingress-guards.md) (per-role journals),
  [ARCHITECTURE.md](../ARCHITECTURE.md), [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

`durable` is the default and the only enabled mode (Phase 1). Its ingress is
append-bound: the gateway awaits exactly one Iggy round trip before the client
sees an ack, so the acknowledgement point is the broker, not the matcher.

The shard pipeline from [ADR 0010](0010-shard-thread-and-journal-pipeline.md)
already separates matching from I/O. In `crates/whippet/src/engine_runner.rs`
the matcher (`run_matcher`) is a dedicated OS thread that owns the
`ShardEngine`, never awaits, and never calls the journal. `read_orders` feeds
it from the orders log through a bounded input ring; `run_writer` drains its
output ring, appends executions and MD, and commits the offset. The matcher
would not change in turbo. What changes is *where its input comes from* and
*when the client is told*.

Turbo is therefore not "durable but faster". It is a different guarantee:
the client is acknowledged after the match, on the strength of process-local
state, and the log is filled in behind by a write-behind journal. A node crash
inside a bounded window loses acked orders. Durable never loses acked orders.
No amount of speed tuning turns one into the other, and the product claims must
not blur them. [ADR 0002](0002-durable-default-turbo-toggle.md) pinned the
language: mode changes sequencing and acknowledgement, never matching
semantics.

`whippet_core::config::Mode::Turbo`, `Mode::as_str`, and `Mode::guarantee`
already exist so the type surface does not change. `NodeConfig::from_args` in
`crates/whippet/src/config.rs` currently rejects `--mode turbo`; this ADR
specifies what replaces that rejection. Nothing here is implemented yet.

## Decision

Turbo keeps the same shard thread and the same matcher. The pipeline becomes:

```text
client -> gateway (HTTP/TCP)
        -> per-partition local sequencer   (assigns seq, appends to window)
        -> input ring -> matcher thread    (identical ShardEngine::process)
        -> direct fanout: MD -> hub apply; ack -> client ("matched_unjournaled")
        -> write-behind journal writer (async task, the existing run_writer edge)
               orders + executions + md appends, offset commit, snapshot gate
```

### Local sequencer and the offset question

Turbo needs a per-command order and identifier that exist *before* the broker
assigns an offset. The per-partition **local sequencer** is a monotonically
increasing `u64` sequence, starting at 0 per shard, assigned at ingress in
arrival order. It is the turbo equivalent of the durable path's broker offset:

- It is handed to `ShardEngine::process` as the per-record offset, so
  `last_offset`, `cause_offset`, and the state hash live in the seq domain.
- It is written into the `MessageMeta` seq header (and duplicated in the
  command message's header pairs) when the write-behind appends the command to
  the `orders` topic, so replay can recover it.
- `ExecutionReport.cause_offset` and `MdEvent.cause_offset` are the local seq
  at match time. Consumers' dedup key `(symbol, cause_offset, seq)` therefore
  stays meaningful across a restart, because replay re-derives the same
  cause_offset from the log's seq headers.

**Invariant the write-behind must hold:** one journal writer per partition,
appends strictly in seq order, contiguous records, no gaps and no skips. Then
seq order equals log order, and the seq↔offset mapping is derivable from the
log alone. A detected gap or overlap fails the shard, exactly as `read_orders`
fails the shard on a durable-path gap today. This invariant is what makes
turbo replayable at all.

Consequence to be explicit about: a turbo **state hash is in the seq domain**.
It equals the durable-path hash for the same commands only when the seq space
and the broker offset space coincide (single deployment, log started empty, one
command per record, contiguous). The differential test below relies on exactly
that precondition and states it.

### Write-behind journal writer

`run_writer` keeps its role — the only async journal edge for the shard — and
gains one input: the uncommitted window. It drains the window in three
appends per slice (`orders`, `executions`, `md`, chunked at the existing
256-message limit), reuses `append_with_retry` with its existing 30-attempt
bound, commits the `whippet-engine-p<N>` offset after the `orders` append
lands, and advances the existing `appended_offset` gate. The snapshot gate is
unchanged in kind: a snapshot is taken only once every output *and* every
windowed command up to the matcher's current seq is durable. A snapshot never
claims a seq whose commands or outputs were lost.

### Bounded uncommitted window

The window holds commands that are matched and acked but not yet in the Iggy
log. It is bounded by both:

- **Count:** 10,000 commands per shard.
- **Age:** 50 ms from ingress assignment to journal append.

Both are configurable (`WHIPPET_TURBO_WINDOW_COMMANDS`,
`WHIPPET_TURBO_WINDOW_MS`), both have documented defaults, and the binding
constraint at any instant is whichever hits first. The 50 ms bound forces the
write-behind to flush as soon as the oldest entry ages out, so a quiet market
does not accumulate a stale window, and a burst is capped by the count bound.

### Ack after match, labelled

Turbo acknowledges after `ShardEngine::process` returns for the command, before
any journal I/O. Every turbo ack carries an explicit guarantee label. The
proposed (not-yet-implemented) wire shape is a `guarantee` field on the order
ack:

- `durable`: `"accepted_by_log"` — the order is in the Iggy log at the returned
  partition and offset.
- `turbo`: `"matched_unjournaled"` — the order has been matched by the engine;
  it is not yet in the log and may be lost if the node dies before the
  write-behind catches up.

The label is also the dashboard badge text: `StateView` already carries `mode`
and `guarantee`, and `Mode::guarantee()` already has the turbo string, so the
dashboard contract does not change.

### Backpressure rejects; no silent fallback

When the window is full, ingress **rejects** the order with an explicit
`window_full` error (HTTP 503 on `POST /api/orders` and `/api/orders/batch`; a
N-ack frame on TCP). It never silently downgrades to durable, never blocks
indefinitely, and never drops a command without telling the client. Mode is
fixed at startup: a node runs durable or turbo for its whole lifetime, and a
mode change is a restart, not a runtime flip.

## Guarantee matrix

| Property | `durable` | `turbo` |
|---|---|---|
| Acknowledgement point | After the Iggy append | After the match |
| Durable at ack | The order record is in the Iggy `orders` log; partition and offset are returned to the client | Nothing at Iggy. The command, its outputs, and the engine's derived state exist in process memory (window + matcher state) |
| Lost if the node crashes | None within Iggy's durability contract on a clean single-node write | Every command in the uncommitted window and all outputs derived from it: at most 10,000 commands, or 50 ms of ingress, whichever is smaller at crash time |
| Duplicates possible | At-least-once outputs on replay of the commit window; dedup by `(symbol, cause_offset, seq)` | Everything durable can produce, **plus**: a client that times out during a crash and resubmits can be accepted again, because the dedup table is rebuilt from a log that never saw the lost window |
| Matching semantics | Identical | Identical — same `SymbolEngine::process`, same order types, same rejects |
| What the state hash proves | The engine state is exactly reproducible from the log alone | The live hash covers state that includes the unjournaled window; after a crash the restored hash is re-derived from the log and the pre-crash tail hash is not reproducible |
| Replayable from the log | Yes, always | Yes, once the write-behind catches up; the lost tail is absent from the log and stays absent |

The state-hash row deserves the blunt statement: in turbo, an equal hash
proves determinism, not durability. In durable it proves both.

## Crash and recovery

Recovery code is shared with durable and does not change in shape:

1. The shard loads `data/snapshots/partition-<N>.snapshot` (format v2) and
   verifies the state hash as today.
2. The resume point is `resume_offset`'s maximum symbol `last_offset`, which in
   turbo is a **local seq**, recovered from the seq headers on replay, not from
   log positions.
3. Replay applies commands from the log in order; the engine re-derives the
   **durable state**, i.e. the state as of the last journaled command. The
   turbo ack state — including everything in the lost window — is gone and is
   not reconstructed.
4. The uncommitted window is lost and must be **reported, not hidden**: at
   startup, the shard logs that any commands matched after the last journaled
   record may have been lost, with the bound (window count/time configuration)
   rather than the exact set, because the acked high-water seq lived only in
   volatile memory.
5. Clients that held a `matched_unjournaled` ack for a lost order are entitled
   to nothing. Any retry produces a new order id (unless the client resubmits
   identical idempotency fields, which the rebuilt dedup table will accept —
   that is the documented duplicate case above).

Turbo does not start accepting ingress until recovery replay completes. There
is no "start matching immediately, reconcile later" shortcut.

## Differential testing

The property that makes turbo a fair comparison rather than a second product:

> The same command log fed through durable and through turbo must produce
> identical execution sequences, and identical state hashes, for every prefix
> that durable has acked (and turbo has journaled).

The test, built on the `whippet-testkit` golden log and the `JournalOps` seam
from [ADR 0011](0011-fault-injection-and-stage-budget.md):

1. Run the golden log through the durable path (log → `read_orders` → matcher).
   Record every `ExecutionReport` and the final state hash.
2. Run the same log through the turbo path (local sequencer → matcher →
   write-behind against the fake journal). Record the same.
3. Assert: identical execution sequence (kind, per-symbol seq, cause_offset,
   payload bytes), identical final state hash.
4. **Precondition, stated in the test:** both runs start from an empty log, so
   the local seq space and the broker offset space coincide and the hashes are
   directly comparable. The test must assert this precondition, not assume it.
5. The turbo tail (matched but not yet journaled) is excluded from comparison
   until the fake journal confirms the write-behind drained the window; after
   the drain, the journaled turbo log must be byte-identical to the durable
   orders log for the same commands.

Crash-window tests (window full, journal down mid-window, shard panic) use the
same fake-journal harness and assert the failure modes below, not latency.

## Backpressure and failure modes

| Failure | Behaviour |
|---|---|
| Window full (ingress faster than write-behind) | Ingress rejects with `window_full` (HTTP 503; TCP N-ack). `window_drops` increments. The matcher keeps working on the window it has. No silent fallback to durable. |
| Journal down (append failures) | The write-behind retries with the existing `append_with_retry` budget (30 attempts, backoff). The window fills, ingress rejects. If attempts are exhausted the shard is marked failed and ingress fails closed (readiness goes false) rather than accepting orders into a window that can never drain. |
| Snapshot writer down (`snapshot_tx` closed) | Same as durable: `run_writer` marks the shard failed. Turbo adds nothing here; a dead snapshot writer is a dead recovery point. |
| Shard panic | `join_matcher` detects the dead thread, marks the shard failed, and errors out. The window dies with the process — this is the crash case, and the recovery section applies. |
| Graceful shutdown | The write-behind drains the window inside the shutdown budget. If it cannot (journal unreachable), the node logs the uncommitted count before exit; that log line is the loss report. |

## API and CLI

- `WHIPPET_MODE=turbo` (environment) and `--mode turbo` (CLI) enable the mode.
  `NodeConfig::from_args` in `crates/whippet/src/config.rs` replaces the
  Phase-1 rejection with validation: turbo requires the Iggy journal (the
  in-memory journal is a test-only configuration, not a supported deployment),
  and the two window knobs parse as positive integers.
- `/api/state` and `/ws` need no change: `StateView.mode` and
  `StateView.guarantee` already carry `Mode::as_str` and `Mode::guarantee`,
  and the dashboard badge reads those fields.
- The order **ack shape gains a `guarantee` field** — *proposal in this ADR;
  do not implement in this phase.* Values and semantics are in the decision
  above. It is an additive JSON field on the HTTP ack; if it is later added to
  the fixed TCP ack frame, that is a wire-version bump and needs its own ADR
  note.
- The benchmark manifest already records `mode`; turbo rows must never be
  compared against durable rows in one chart without the mode label.

### Metrics the implementation must expose

All four are required before turbo ships; they are the visible window that the
guarantee text describes:

| Series | Type | Meaning |
|---|---|---|
| `whippet_turbo_uncommitted_window` | gauge | Commands currently matched and acked but not journaled, per shard (or summed; pick one and document it) |
| `whippet_turbo_journal_lag` | gauge | Distance between the matcher's current seq and the last seq appended to the orders log, in commands |
| `whippet_turbo_window_drops_total` | counter | Commands rejected because the window was full |
| `whippet_turbo_ack_latency_microseconds` | histogram | Gateway receive → match complete, per acked command |

They belong in the existing per-shard `ShardMetrics` / global `Metrics`
structures in `crates/whippet-marketdata` (counters and gauges merged once per
second, as the match-latency histogram already is), surfaced through
`GET /metrics` and documented in [OPERATIONS.md](../OPERATIONS.md). No numbers
are claimed here; the first turbo benchmark run publishes them under the rules
in [PERFORMANCE.md](../PERFORMANCE.md).

## Files the next phase touches

- `crates/whippet/src/config.rs` — `NodeConfig::from_args`: accept turbo,
  window knobs, journal validation.
- `crates/whippet/src/engine_runner.rs` — `run_engine_shard`: mode-specific
  ingress edge (sequencer ring vs `read_orders`); `run_writer`: drain the
  window, append orders/executions/md, commit, gate; `OutputBatch` extended to
  carry window entries; `run_matcher`'s matching logic unchanged.
- `crates/whippet/src/gateway.rs` (`submit_order`, `submit_orders`) and
  `crates/whippet/src/tcp.rs` — mode-aware ingress and `window_full` rejects.
- `crates/whippet/src/context.rs` — `NodeContext` carries the mode and the
  per-shard sequencer/window handles.
- `crates/whippet-marketdata` (`Metrics`, `ShardMetrics`) — the four metrics.
- `crates/whippet-journal` — no new topics; the same three topics from
  [ADR 0004](0004-iggy-topics-partitions-durability.md) are reused, and the
  write-behind reuses the existing append/commit paths.
- `crates/whippet-core/src/api.rs` — the `guarantee` ack field is specified
  here but **not implemented** in this phase.

## Consequences

Positive:

- The sub-millisecond matching claim can finally be measured end to end: the
  ack path contains no broker round trip and no awaits after ingress.
- The guarantee is explicit and labelled at every surface — ack, dashboard,
  metrics — so no consumer can confuse the two modes by accident.
- Recovery, snapshots, replay, and verification are shared with durable; turbo
  adds an ingress edge and a write-behind loop, not a second engine.
- The bounded window turns "how much can you lose" from a shrug into a number
  an operator configures and a gauge they can watch.

Negative:

- A second acknowledgement contract means a second set of failure modes, a
  second differential test, and honest documentation that turbo acks can be
  lost. This is real work; [ADR 0002](0002-durable-default-turbo-toggle.md)
  said as much and was right.
- The seq-domain state hash means turbo hashes are not directly comparable to
  durable hashes outside the differential test's precondition. Tooling that
  compares hashes across modes must know that.
- Backpressure-rejects is a worse client experience than queue-and-wait under
  burst. It is chosen because queue-and-wait hides the loss window until the
  crash happens.
- The write-behind's single-writer, contiguous-append invariant is load-bearing
  and easy to break with an innocent-looking batching change. It needs a test,
  not just this paragraph.

## Alternatives considered

- **Write-behind every N commands** (journal in fixed-size chunks instead of a
  count+age window). Simpler, but it does not bound loss by time: a slow market
  would hold the same N commands indefinitely. Rejected: the age bound is the
  part that makes the loss window honest.
- **Ack after append with local batching** — that is durable with group
  commit, which Phase 1.5 already implemented and measured. It keeps the
  broker round trip on the ack path and cannot reach the sub-millisecond
  matching claim. Rejected as "turbo": it is the durable path, correctly
  labelled as such.
- **A separate turbo topic** (e.g. `whippet-trading/orders-turbo`). Isolates
  turbo traffic, but creates two logs for one exchange: replay and recovery
  would need to merge them, and [ADR 0004](0004-iggy-topics-partitions-durability.md)
  makes partition counts a create-time contract that must not grow by accident.
  Rejected: one log per stream, one truth.
- **Silent fallback to durable when the journal is down.** Preserves
  availability, but silently changes the guarantee the client was given in the
  opposite direction (slower, stronger). A mode flip that clients cannot see
  is worse than an explicit reject. Rejected without reservation.
- **Per-order client choice of mode.** Already rejected in
  [ADR 0002](0002-durable-default-turbo-toggle.md); nothing has changed.

## Status

Proposed. Nothing here is implemented. Before turbo ships it needs: the
sequencer and window in the shard pipeline, the four metrics, the
differential test with its precondition asserted, the failure-mode tests from
the table above, and a benchmark run labelled `turbo` under the publication
rules in [PERFORMANCE.md](../PERFORMANCE.md). Until then, `durable` remains
the default and only enabled mode, and no document may describe turbo as
available.
