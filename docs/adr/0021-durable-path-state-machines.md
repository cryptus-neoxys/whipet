# ADR 0021: Durable-path state machines — snapshot, commit, turbo admission

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0002](0002-durable-default-turbo-toggle.md) (durable default),
  [ADR 0004](0004-iggy-topics-partitions-durability.md) (topics and durability),
  [ADR 0013](0013-turbo-mode.md) (turbo mode),
  [INVARIANTS.md](../INVARIANTS.md), [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

The durable path has three progress markers, each with an implicit lifecycle:

- the snapshot file on disk, which claims "state through offset N is durable";
- the consumer offset committed to Iggy, which claims "everything through N was
  applied and acknowledged";
- the turbo admission window slot, which claims "this command is admitted but
  its order record may not be durable yet".

Each was a set of fields with rules in call sites. A snapshot could be written
without hashes or without a resume point; a commit could be acknowledged out of
order or ahead of what was applied; a turbo admission could be released twice
or never. None of these failures is visible in a throughput number, and all of
them turn a restart into a wrong resume point.

## Decision

### Snapshot writer

`ShardSnapshot` must satisfy `SnapshotInvariants::check_invariants` before any
file exists. The check is structural and cheap: non-empty strictly sorted
symbols; `book.symbol == symbol.symbol`; sequences at least 1; a zero-command
symbol has offset 0 and no dedup state; dedup offsets/timestamps within
`last_offset`/`last_ts_ns`; terminal indices in range, terminal, unique, and
covering every terminal entry; at least one symbol with `commands > 0`; and a
non-zero `state_hash` on every symbol.

- `SnapshotStore::save` runs the check **before** `create_dir_all`/opening the
  temp file, so an invalid snapshot is never transiently on disk.
- Both `write` (verified rebuild) and `write_unverified` (periodic) require the
  check, so periodic snapshots carry hashes too. `fill_hashes` is the only way
  to leave the unhashed phase, and a supplied hash that disagrees with the
  recomputation is rejected.
- `last_written` advances only after the rename. A sequence at or below it is a
  no-op with no filesystem work; a failed write/flush/sync/rename removes the
  temp file and leaves the previous resume point.
- The reader is unchanged and still rejects a commandless file from an older
  node or a hand edit: the writer guard does not weaken reader fail-closed
  coverage.

### Commit watermark and retry

```text
applied   --note_applied-->  non-decreasing; first uncommitted record arms the delay clock
committed --note_committed--> non-decreasing, clamped to applied
pending  <=> applied > committed
pending_since is Some <=> pending
```

- A commit never claims records that are not durable (`committed <= applied`).
  A backwards applied offset is debug-asserted and ignored; an over-shooting
  commit is debug-asserted and clamped.
- The delay bound is measured from the onset of the pending range, not from the
  last commit: the applied-offset lag is bounded by construction, and a busy
  path keeps the same 256-record / 100 ms cadence.
- `CommitRetry` doubles from 100 ms to a 2 s cap, saturates at 30 attempts for
  diagnostics, and never stops retrying: a broken broker costs commit lag, not
  availability. A success resets the schedule, not the log throttle.

### Turbo admission window

```text
Queued --writer: orders append durable--> Completed
Queued --Drop: send failure / disconnect / stop--> Abandoned
```

The lifecycle marker exists only in debug builds; release enforces exactly-once
release structurally through the semaphore permit (`Option::take`). The writer
completes admissions in seq order so the queue front is an O(1) pop; the
fallback search is the abnormal abandoned/out-of-order path.

`TurboWindow::check_invariants` requires `in_flight == queued.len()`,
`available permits + in_flight == capacity`, strictly increasing unique ids,
non-decreasing timestamps, `oldest_queued_ns == front`, and
`drops == count_drops + age_drops`. `try_admit` returns
`Result<TurboAdmission, AdmitError>` with `CountFull`/`AgeFull`;
`EnqueueError::WindowFull(AdmitError)` carries the reason to metrics and tests,
while HTTP still answers `window_full` and TCP its `WindowFull` N-ack. A full
window never falls back to durable mode.

## Alternatives rejected

- **Trust the hash/offset alone:** a self-consistent but invalid snapshot (no
  resume point, unsorted symbols) still loads and produces a wrong boot.
- **Validate only in debug:** a release node can persist the same bad file.
- **Commit per batch:** correct but pays a broker round trip per batch; the
  bounded drift is the point of the watermark.
- **Stop retrying after the budget:** the commit is the only durable progress
  marker; stopping would strand a healthy matcher behind a transient broker
  error.
- **A turbo admission without an owned permit:** release would depend on a
  separate counter and could double-free or leak.

## Consequences

- Every new snapshot field or commit rule must extend the invariant checks and
  their focused tests; a check that cannot fail is documented as unreachable.
- The snapshot path pays one O(snapshot) structural walk on the writer thread;
  the matcher hot path pays nothing.
- The commit path pays two debug assertions and no release branches beyond the
  existing comparisons.
- The turbo admission release path stays allocation-free; the lifecycle state
  compiles out of release.
