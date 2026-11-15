# ADR 0014: Bounded storage and replay-safe retention

- **Status:** Proposed (retention rule); storage guard landing as the first implementation step in the same Phase A slice
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md) (topic layout, retention values, manual commits),
  [ADR 0010](0010-shard-thread-and-journal-pipeline.md) (shard pipeline),
  [ADR 0012](0012-per-role-journals-and-ingress-guards.md) (health, resume point),
  [ROADMAP §3.4, §4.3, §4.4, Phase A](../ROADMAP-AND-IMPROVEMENT-PLAN.md),
  [OPERATIONS.md](../OPERATIONS.md) ("Topic creation", "Log growth and retention", "Backup and restore"),
  [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

Topic creation records finite retention and size bounds, but nothing trims the log today.

- `crates/whippet-journal/src/topic.rs` (`Topic::retention_secs`): orders 24 h, executions 7 d, md 1 h,
  alerts 30 d, audit 90 d, commands 7 d. `crates/whippet-journal/src/lib.rs`
  (`topic_create_options`): 8 GiB max topic size for orders/executions/md, 2 GiB for ops topics,
  64 MiB segments, flush thresholds 1,024 messages / 1 MiB, durability and consumer-offset
  durability per [ADR 0004](0004-iggy-topics-partitions-durability.md).
- In Iggy 0.10 that policy is enforced only by the messages cleaner, which deletes the oldest
  **sealed** segments per partition, best-effort, once per interval (default 1 m). The active
  segment is never touched.
- The default stack disables the cleaner
  (`IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=false`), because its once-per-minute run
  produced append-latency spikes in the dev-loop soak. With it off, the policies are recorded
  but never enforced: the log grows until the volume is full. See
  [OPERATIONS.md](../OPERATIONS.md) ("Log growth and retention").
- `IggyJournal::ensure_topics` leaves existing topics alone (except a partition-count check
  that fails startup on mismatch); changing `WHIPPET_PARTITIONS` or a retention value after
  the topics exist has no effect until the topics are recreated.
- Snapshots are recovery accelerators, not backups ([OPERATIONS.md](../OPERATIONS.md) "Backup and
  restore"). A snapshot file is only meaningful with its `last_offset` and state hash
  (`whippet verify`, `whippet replay --require-snapshot` report `snapshot_offset`,
  `committed_offset`, `head`, `tail`). Format is v6 (`WHIPSNP1`); v5 and older are rejected.
  Consumers commit manually (`CommitPolicy::Manual`): the engine commits only after executions
  **and** MD appends land; the MD consumer commits after applying. A crash between output
  append and offset commit replays at-least-once.
- The reader already fails closed on a gap: [FAILURE-MODES.md](../FAILURE-MODES.md) ("Log gap /
  retention truncation") — the shard validates the first offset and the offset span of each
  polled batch and stops instead of applying a partial log. Retention shorter than a
  consumer's downtime is therefore a data-loss design decision, not a tuning tweak.

Rule for this ADR: **a segment can only be deleted when every consumer and every valid
recovery baseline is past it.** Until that proof exists, retain.

## Decision

Two parts. Part 1 is the Phase A guard to be implemented in the same slice as this ADR.
Part 2 is the retention-eligibility rule: design only, not implemented, and the cleaner
stays off until it is.

### Part 1 — Bounded-storage guard (to be implemented in this slice)

The node monitors free space on the Whippet data volume (`WHIPPET_SNAPSHOT_DIR` filesystem)
and on the Iggy data volume (mounted read-only into the node container) via `statvfs`,
and exposes:

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `whippet_disk_free_bytes` | gauge | `volume` (e.g. `snapshot`, `iggy`) | Free bytes per monitored mount |
| `whippet_disk_used_pct` | gauge | `volume` | Used percent per monitored mount |

Thresholds (configurable, validated `warn < crit`, each 1..=99):

- `WHIPPET_DISK_WARN_PCT` (default 70) — warn: gauge/log only, no behaviour change.
- `WHIPPET_DISK_CRIT_PCT` (default 85) — critical: fail-closed, no deletion.

At critical on **either** volume:

- `GET /readyz` returns 503 (same fail-closed posture as the existing cached journal/shard
  flag in `crates/whippet/src/gateway.rs::readyz`; the disk check joins that aggregate).
- New orders and cancels are rejected with HTTP 503 `storage_pressure`
  (same error-envelope shape as the existing `journal_unavailable` in `gateway.rs`;
  TCP returns the corresponding error code). Nothing already accepted is dropped;
  matching and replay continue so the node can still drain.
- **No data is deleted automatically.** Deletion remains an explicit operator action
  (see Consequences/runbook).

A stat failure is visible (`available="false"` on the series / a log line) but does not
by itself block ingress: an unobservable disk must not halt trading, and a full disk
is still caught by the append-error path (`journal_unavailable`, shard-failed guard)
that already exists.

### Part 2 — Retention eligibility (design, NOT implemented)

When deletion is ever enabled (cleaner re-enabled or an explicit trim tool), a sealed
segment range is eligible for deletion **only when ALL hold, evaluated per partition**:

(a) Every active consumer's committed offset is beyond the range. Consumers are the
    engine shard consumer (`whippet-engine-p<N>` on `orders`) and the MD consumer on `md`,
    read via the existing `IggyJournal::committed` / `lag` path — never an assumed head.
(b) The newest **verified** snapshot for the partition has `last_offset` beyond the range
    (`whippet verify` hash check passes), **or** an archived log/segment set covering the
    range exists and has been verified restorable.
(c) The configured maximum downtime + safety margin window is covered by a valid baseline:
    a consumer offline for the whole window must still resume from snapshot + retained log.
(d) Audit/business retention minimums are satisfied (per-topic floor: e.g. audit 90 d,
    executions 7 d — the `Topic::retention_secs` values are the floor, not the target).

If any condition cannot be proven (unknown consumer offset, unverified snapshot, missing
archive), **retain**. Never delete the active segment. Deletion is logged with the range,
the baseline snapshot hash and `last_offset`, the consumer offsets observed, and the
operator identity if manual.

Minimum window arithmetic (per partition, then take the max across partitions for the topic):

```text
retain_window = max(consumer_span, snapshot_span + downtime_margin, audit_minimum)
```

- `consumer_span`: head minus oldest committed consumer offset, in time.
- `snapshot_span`: age of the newest verified snapshot for the partition.
- `downtime_margin`: configured maximum supported broker/node downtime plus an explicit
  safety margin for recovery and operator intervention.
- `audit_minimum`: the topic's business/audit floor (`Topic::retention_secs`).

Metrics and alerts needed before any deletion is enabled (names follow the suggested
[ROADMAP §8](../ROADMAP-AND-IMPROVEMENT-PLAN.md#8-suggested-metrics-to-add-or-refine)
series; none of these exist in `/metrics` today except `whippet_consumer_lag`):

- `whippet_iggy_log_bytes{topic,partition}` — storage growth per partition.
- `whippet_volume_free_bytes{volume}` (or the Part 1 `whippet_disk_free_bytes`) — volume headroom.
- Oldest consumer lag (records and age) per partition — extends the existing
  `whippet_consumer_lag` sweep, which today sums over active shards.
- Snapshot age per partition — time since the newest verified snapshot.
- Retention-blocked gauge with a reason label (e.g.
  `whippet_retention_blocked{partition,reason="consumer|snapshot|downtime|audit"}`) —
  1 while any eligibility condition fails, so "cleaner would delete but must not" is visible.

## Alternatives rejected

- **Delete by age/size alone without consumer/snapshot proof.** This is what re-enabling
  the cleaner with defaults would do. Rejected: a consumer down longer than the expiry
  (or a snapshot older than the deleted range) hits the fail-closed gap guard and cannot
  recover from the log alone; `replay --expect-hash` no longer sees the full log.
- **Re-enable the cleaner with defaults and accept the spikes.** Rejected: the spikes were
  measured (soak p99 growth attributed to `gateway_append`); re-enabling without the
  eligibility rule trades one failure (unbounded growth) for two (latency spikes plus
  possible unrecoverable gaps).
- **Snapshot-only recovery with no log.** Rejected: snapshots are per-shard, not a global
  point-in-time, and carry no business record between `last_offset` and the head; without
  the log there is no at-least-once tail replay and no audit trail.
- **Unlimited growth.** Rejected: this is the status quo the guard fixes — the volume
  fills and the broker fails opaquely instead of the node failing closed with
  `storage_pressure`.

## Consequences

**Implemented now:** none of this ADR. Recorded retention values, the disabled cleaner, the
gap guard, manual commits, and snapshot verify/replay guards are implemented (see Context);
the disk guard and every retention-deletion path are not. No metric, env var, or error
code named here exists in the tree yet — `grep` for `storage_pressure`,
`WHIPPET_DISK_WARN_PCT`, `whippet_iggy_log_bytes`, and `statvfs` finds nothing.

**Proposed:** Part 1 guard (warn/crit thresholds, `/readyz` + 503 `storage_pressure`,
no auto-delete) lands in the same Phase A slice; Part 2 eligibility rule stays
design-only with the cleaner off.

Runbook (pause safely, keep the proof):

1. On critical: ingress rejects with 503; matching continues. Do **not** delete segments
   to recover space.
2. Preserve the volume: stop the stack before copying; never copy a live Iggy volume;
   cold-copy the Iggy volume and `WHIPPET_SNAPSHOT_DIR` per "Backup and restore".
3. Verify snapshots: `whippet verify --require-snapshot --symbols ... --partitions ...`;
   then `whippet replay --require-snapshot --expect-hash <hex>` against a known log.
4. Archive or reset only with explicit data-loss approval: archiving means a verified
   restorable copy; `make reset` deletes the log, broker-stored consumer offsets, and
   snapshots. Either needs a named approver and a recorded hash.
5. Recovery: restore `partition-<N>.snapshot` files, start the node (each shard verifies
   its hash; mismatch fails the shard closed), and confirm `/readyz` 200 and
   `whippet_consumer_lag` draining before re-enabling ingress. If the log was truncated
   past a committed offset, the shard fails closed on the gap — restore from archive
   or reset; there is no partial-apply path.

## Status

Proposed. The retention-eligibility rule is design-only: no deletion path may be enabled
until the four conditions, the window arithmetic, and the five metrics/alerts above are
implemented and tested (including a disk-full simulation and a retention-boundary-crossed-
while-consumer-offline test per ROADMAP §4.5). The Part 1 storage guard is the first
implementation step and lands with its thresholds, metrics, `/readyz` wiring, and
`storage_pressure` rejects in the same Phase A slice.
