# Whippet Operator Runbook

One safe sequence for boot, health, disk pressure, snapshot/verify, replay,
restore, crash recovery, control plane, market-data feed, retention planning,
upgrade, and reset. Scattered source of truth:
[OPERATIONS.md](OPERATIONS.md), [FAILURE-MODES.md](FAILURE-MODES.md),
[PERFORMANCE.md](PERFORMANCE.md), [BENCHMARKS.md](BENCHMARKS.md),
[ADR 0013](adr/0013-turbo-mode.md), [ADR 0014](adr/0014-bounded-storage-and-replay-safe-retention.md),
[ADR 0015](adr/0015-audited-instrument-control-plane.md),
[ADR 0016](adr/0016-sequenced-price-level-market-data-feed.md).

Conventions used below: `make` targets from the repo `Makefile`;
`scripts/{crash-test,soak,replay,verify-demo}.sh`; `whippet` CLI
(`verify`, `replay`, `retention-plan`, `doctor`, `healthcheck`).
Nothing here deletes data except the explicitly marked `make reset` path.

## 1. Boot and health

```sh
make secrets   # generates root credentials; node reads them from the secret file
make up        # builds and starts iggy + node + sim, waits for readiness
make doctor    # read-only preflight: Iggy topics, partition counts, heads, snapshots, health
```

`make doctor` is read-only. Run it before a demo and after any restart.

Probes:

- `GET /api/health` returns HTTP 200 with `status: "ok"` or `"degraded"`.
  It is `ok` only when the journal is reachable, every shard task is alive,
  and storage is below critical. Fields `shards_ok`, `storage_ok`,
  `storage_state` name the failing component.
- `GET /readyz` returns 200 only when the journal is reachable, every shard
  task is alive, and no monitored volume is at the critical threshold;
  otherwise 503. Use it for container readiness. It reads cached flags and
  performs no journal I/O.
- `docker compose -f deploy/compose.yaml exec node whippet healthcheck`
  prints the health JSON and exits non-zero unless status is `ok`.

What degraded / not-ready means:

- Journal unreachable: gateway returns 503 `journal_unavailable`; engine and
  MD polls retry; health monitor marks `degraded` on the next five-second
  check. Recovery depends on the Iggy client reconnecting; restart the node
  if it does not.
- A shard task dead (panic, writer retry exhaustion, ring deadlock guard,
  corrupt snapshot): `shards_ok: false`, degraded health, failed readiness.
  Restart and replay; do not expect self-healing.
- Storage at critical: see section 2.
- A corrupt snapshot or a shard that fails closed on a log gap reports
  degraded instead of serving a silent "up but not matching" node.

Useful log lines (`docker compose -f deploy/compose.yaml logs -f node iggy`):
`Iggy is not ready; retrying`, `restored shard from snapshot`,
`engine shard running`, `snapshot written`, `order poll failed; retrying`,
`append failed; retrying`, `offset commit failed; batch will replay`.

## 2. Disk pressure

Monitored volumes: `WHIPPET_DATA_DIR` (`/data`, snapshots) and
`WHIPPET_IGGY_DATA_DIR` (`/iggy-data`, broker volume, mounted read-only so
the node can measure but never delete). Sampled with `statvfs` every five
seconds; the worst known volume sets the state.

- `WHIPPET_DISK_WARN_PCT` default 70: warn state, alert only, ingress stays open.
- `WHIPPET_DISK_CRIT_PCT` default 85: critical state. `/readyz` returns 503
  with `storage: false`; `/api/health` reports `degraded` with
  `storage_state: "critical"`; HTTP order/cancel paths return 503
  `storage_pressure`; TCP returns the `StoragePressure` error code.
  Matching continues on already-admitted work. Nothing is journaled and
  **nothing is deleted**.

Gauges: `whippet_disk_free_bytes{volume}`, `whippet_disk_used_pct{volume}`,
`whippet_disk_monitor_ok{volume}`, `whippet_storage_warning`,
`whippet_storage_critical`. A failed `statvfs` marks that volume
`available=false` and contributes warn only; a transient mount problem never
stops ingress.

Safe response on critical (link: [ADR 0014](adr/0014-bounded-storage-and-replay-safe-retention.md)):

1. Let the critical state do its job: ingress is already refusing new work.
   Do not force orders through.
2. Preserve the volume. Stop the stack before copying; never copy a live
   Iggy volume. Cold-copy the Iggy volume and `WHIPPET_SNAPSHOT_DIR`.
3. Verify snapshots (`make verify-strict`) and replay against a known log
   (`make replay`).
4. Archive what must be kept, then delete data only with explicit approval.
   Nothing deletes automatically: the default stack disables the Iggy
   messages cleaner, so recorded retention policies are never enforced and
   the log grows until the volume is full.

## 3. Snapshot and verify

Snapshots live in `WHIPPET_SNAPSHOT_DIR` (`data/snapshots` in the container)
as `partition-<N>.snapshot` files. Format is v4 (`WHIPSNP1` magic); v3 and
older are rejected with `BadVersion`. Each file is written to a temporary
file, `fsync`ed, and renamed, so an individual file is never partial. A set
of shard files is not a global point-in-time; shards snapshot independently,
which is acceptable because each partition is an independent state machine.

`make verify` (lenient dev path: healthcheck, crossing-pair demo check,
`whippet verify` without `--require-snapshot`) prints
"no snapshots; nothing verified" on a fresh stack. `make verify-strict`
adds `--require-snapshot` and is the fail-closed proof:

- Exits non-zero unless **every active shard** (a shard that owns at least
  one configured symbol) has a snapshot file. A partial set is not a proof.
- `--symbols` (`WHIPPET_SYMBOLS`) must match the node's universe;
  `--partitions` must match the node's partition count (a snapshot written
  with one count must be verified with the same count, not the 16-partition
  default).
- No fallback: no in-binary migration, no automatic fallback to an older
  file or to a full replay. Recovery always requires operator action.

## 4. Replay proof

```sh
make replay      # fail-closed: seeded run passes --require-snapshot
make replay-dev  # lenient: allows a full replay as the "seeded" run
```

Raw form: `scripts/replay.sh` (stops the simulator so the log is stable,
keeps the node up, runs a full replay from offset 0 and a snapshot-seeded
replay, compares hashes). Env: `WHIPPET_REPLAY_PARTITION` (default 1),
`WHIPPET_PARTITIONS` (default 16), `WHIPPET_SYMBOLS`.

- Full vs seeded: both must print the same `state_hash` and end at the same
  offset. The full run reports `seeded_from_snapshot: false`, `from: 0`;
  the seeded run reports `seeded_from_snapshot: true` with
  `snapshot_offset`, `committed_offset`, `head`, `tail`.
- `skipped == 0` on both runs: any skipped or undecodable record fails the
  proof; the replay did not cover the whole log.
- `tail` (records after the snapshot) must be non-zero and fully decoded
  (`decoded == tail`); a snapshot at the log head proves nothing.
- `snapshot_offset >= head` fails: a snapshot ahead of the log is refused.
  The commit watermark may lag the snapshot; only the log head bounds it.
- `whippet replay --require-snapshot` also refuses a missing snapshot, a
  snapshot with no applied commands, and an unseeded run.

`whippet replay` never writes a snapshot; it only reads the log and the
existing files, and its consumer never commits offsets.

## 5. Restore

Source of truth is the log; snapshots are recovery accelerators, not
backups. To restore:

1. Stop the node: `docker compose -f deploy/compose.yaml stop node`.
2. Keep a copy of the current snapshot directory, then put the
   `partition-<N>.snapshot` files back into `WHIPPET_SNAPSHOT_DIR`.
3. Start the node. Each shard verifies its hash before use; a mismatch
   aborts that shard with an error. The node never silently uses a corrupt
   file.
4. Confirm `/readyz` 200 and consumer lag draining before re-enabling ingress.

Semantics to respect:

- A snapshot ahead of the log is refused at startup (same bound as replay).
  A foreign or stale file cannot claim records that are not in the log.
- To rebuild a shard from scratch, delete its snapshot; the engine starts at
  offset zero and replays the whole log (the committed offset only seeds the
  commit watermark). That is a full replay.
- If the log was truncated past a committed offset, the reader hits a gap
  and the shard fails closed. Restore from archive or `make reset`; there is
  no partial-apply path.

To take a consistent-enough copy for a demo: stop the node, `cp -a
data/snapshots` to a dated backup, start the node. If `data/` is not mounted
to the host, copy it out of the container instead. To cold-copy the Iggy
volume, stop the whole stack first.

## 6. Crash recovery

Durable mode is **at-least-once with deterministic state**, not
exactly-once output:

- The output append (executions/MD) and the input-offset commit are separate
  operations. A crash between them replays the batch from the last durable
  point; executions and MD from the replayed batch can be appended a second
  time. Engine state converges because replay is deterministic.
- Duplicate outputs carry identical identities; downstream consumers must
  deduplicate by `(symbol, cause_offset, seq, kind)`.
  `tests/crash_idempotency.rs` forces append-before-commit, restarts from the
  snapshot, and asserts the duplicate outputs deduplicate to the single-run
  effects with the same state hash.
- Output/input rings are capacity 2 and block rather than drop; a stalled
  writer propagates backpressure to the reader instead of skipping events.
- The dedup table is bounded (`min(2 * max_orders, 100,000)` entries per
  symbol; 100,000 with the default instruments; terminal entries evicted
  last). Inside the window, a duplicate `client_order_id` is rejected with
  no second effect. An id older than the window can be accepted again after
  eviction; the window is count-based, not a time guarantee.

Turbo mode (`WHIPPET_MODE=turbo`, link: [ADR 0013](adr/0013-turbo-mode.md)):

- Ack point is after `ShardEngine::process`, before any journal I/O, with
  `guarantee: "matched_unjournaled"`. Matching semantics, books, state hash,
  and snapshot format are identical to durable.
- Crash window: commands admitted but not yet appended are lost, bounded by
  `WHIPPET_TURBO_WINDOW_COMMANDS` (default 10,000) and
  `WHIPPET_TURBO_WINDOW_MS` (default 50); whichever is hit first binds. A
  full window rejects with HTTP 503 `window_full` (TCP `WindowFull`); nothing
  is journaled and the node never silently falls back to durable.
- Turbo restart from a snapshot replays the journaled tail from
  `snapshot_offset + 1` through the log head before ingress opens. A restart
  with no snapshot requires an empty orders log; a non-empty log without a
  snapshot is a startup error. Switching modes on an existing log requires
  `make reset`.
- A turbo state hash covers state that includes the unjournaled window; after
  a crash the restored hash is re-derived from the log. Equal hashes prove
  determinism, not durability, in turbo.

Executable proof: `make crash-test` (`scripts/crash-test.sh`) runs a short
workload (defaults 1,000 orders/s, batch 10, 20 s), SIGKILLs the node
(`docker compose kill node`), restarts it, waits for `/api/health`, then runs
`make replay` and fails unless both replay hashes match. Raw output and a
JSON summary land in `artifacts/bench/`. Status as documented: method
recorded, results pending — see section 12.

## 7. Control plane

Halt and resume are operator-only HTTP endpoints (link:
[ADR 0015](adr/0015-audited-instrument-control-plane.md)):

```sh
curl -X POST http://127.0.0.1:8080/api/admin/instruments/BTC-USD/halt \
  -H "Authorization: Bearer $WHIPPET_OPERATOR_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"reason":"maintenance"}'
curl -X POST http://127.0.0.1:8080/api/admin/instruments/BTC-USD/resume \
  -H "Authorization: Bearer $WHIPPET_OPERATOR_TOKEN" -d '{}'
```

- Authorization is a separate namespace: `WHIPPET_OPERATOR_TOKENS` holds
  `token:operator-name` pairs. Operator tokens cannot submit orders; trading
  tokens cannot halt/resume. No operator tokens configured means 403 for
  every admin request; missing/invalid means 401.
- The command is appended to the same orders log on the symbol's partition,
  so it takes effect at that log position and replays deterministically. A
  halted instrument rejects new orders with `halted`, allows cancels, and
  shows `halted: true` in `/api/state` and the dashboard badge.
- Every accepted request appends one JSON audit record to the `audit` topic
  (partition `symbol.partition(4)`). If that append fails, the endpoint
  returns 500 `audit_unavailable` because the order-log command may already
  be applied. The missing audit record is an operational incident, not a
  silent success.
- TCP `Submit` rejects Halt/Resume; the control plane is HTTP-only.

## 8. Market-data feed

Two separate contracts (link: [ADR 0016](adr/0016-sequenced-price-level-market-data-feed.md)):

- `GET /ws` is the dashboard's conflated full-state stream at ~20 Hz. A slow
  client misses intermediate frames; on reconnect it fetches `/api/state`
  and opens the socket again. There is no sequence-gap detection on this
  stream.
- `GET /ws/md?symbol=BTC-USD` is the sequenced price-level feed: one
  full-depth `snapshot` at the current per-symbol `seq`, then every accepted
  `delta` in sequence order. If a client falls behind the hub's bounded feed
  buffer (capacity 1024), the server sends a `resync` notice naming the new
  sequence and a fresh snapshot; a client that sees an unexplained gap should
  reconnect. The feed is market-by-price (levels), not order-by-order.
- Admission shares the global WebSocket subscriber cap
  (`WHIPPET_MAX_WS_CONNECTIONS`, default 2048).
- Metrics: `whippet_md_feed_subscribers`, `whippet_md_feed_sent_total`,
  `whippet_md_feed_lagged_total`, `whippet_md_feed_resyncs_total`. With no
  feed subscriber the MD apply path does no feed work.

```sh
websocat 'ws://127.0.0.1:8080/ws/md?symbol=BTC-USD'
```

## 9. Retention plan

`make retention-plan` (`whippet retention-plan --snapshot-dir
/data/snapshots --partitions 16`) computes the oldest orders-log offset that
is safe to delete from live evidence: the engine consumer's committed offset
and the newest **verified** snapshot. It is read-only by construction: it
never deletes, trims, commits, or appends.

- Boundary is `min(engine committed, verified snapshot max last_offset)`;
  `safe_through = boundary - WHIPPET_RETENTION_SAFETY_MARGIN_RECORDS`
  (default 10,000).
- Blocks (valid result, exit 0) when there is no committed engine consumer,
  no verified snapshot, a boundary at offset 0, the head is behind the
  boundary, or the margin consumes the window. Only connect/snapshot errors
  fail.
- The MD consumer's offset is shown as `md-topic` for diagnostics and is not
  part of the orders boundary. `--json` emits one object per shard with
  `"action": "none"`.

This is a guard, not retention: it tells the operator where deletion would
be safe before the volume fills, and it never trims segments. The replay-safe
deletion rule and the minimum log window are in
[ADR 0014](adr/0014-bounded-storage-and-replay-safe-retention.md). Enabling
any deletion path requires the four eligibility conditions (consumers past
the range, verified snapshot or archive past the range, downtime window
covered, audit minimums met) plus the window arithmetic and its metrics —
none of which are implemented yet.

## 10. Upgrade and reset

Pins (`docs/OPERATIONS.md` "Upgrade pins"): Iggy server image
`apache/iggy:0.10.0`, Rust SDK `iggy = "=0.12.0"`, wire protocol 0.12.0, Rust
toolchain 1.95.0, edition 2024. Server and SDK must speak the same wire
version; mixed versions are refused, not negotiated. Do not use `latest`.

Upgrade procedure: read the Iggy release notes, bump server image and SDK pin
together, `make reset && make up`, then `make demo`, `make verify`, and a
`whippet replay --expect-hash` check against a known log. Record the new
image digest in `WHIPPET_IMAGE_DIGEST`.

Snapshot version: v4 current; v3 and older rejected with `BadVersion`. A
snapshot-format change is a data migration, not a rolling upgrade: delete or
move the old files (or reset the data) and let the new node full-replay from
0 and write v4.

`make reset` is destructive: it deletes the Iggy log, broker-stored consumer
offsets, and snapshots in the named volume. It keeps host `data/`,
`artifacts/`, and secrets unless `scripts/reset.sh --data`/`--all` is passed.
Safe archive before reset: cold-copy the Iggy volume with the whole stack
stopped, `cp -a data/snapshots` to a dated backup, record which hash the
backup verifies against. No secrets in git: credentials live in generated
secret files read via `WHIPPET_IGGY_PASSWORD_FILE`, never in committed
config.

## 11. Incident checklist

| Symptom | First command | What to preserve | Do not do |
|---|---|---|---|
| `/readyz` 503, `storage_state: critical` | `curl -s localhost:8080/api/health`; `curl -s localhost:8080/metrics \| grep whippet_disk` | Iggy volume + snapshot dir (cold copy, stack stopped) | Delete segments or re-enable the cleaner to "recover space" |
| `/api/health` degraded, `shards_ok: false` | `docker compose -f deploy/compose.yaml logs --tail=200 node`; `make doctor` | Node + Iggy logs, snapshot files, last known hashes | Restart repeatedly without capturing logs; delete snapshots |
| Gateway 503 `journal_unavailable` | `docker compose -f deploy/compose.yaml ps`; Iggy logs | Iggy volume, consumer offsets (do not reset) | Submit orders expecting them to queue; assume nothing was accepted |
| Suspected duplicate executions after a crash | `make replay` (hashes must match, `skipped == 0`) | Replay output, log head/tail, snapshot offsets | Treat the second append as a second business effect; skip dedup by `(symbol, cause_offset, seq)` |
| Snapshot verify fails / `BadVersion` | `make verify-strict`; check node universe vs `--symbols/--partitions` | The failing file (move, do not overwrite) | Let the node "fall back" — there is no fallback; delete blindly |
| Snapshot ahead of log head | Compare `snapshot_offset` vs `head` in replay output | Both the snapshot dir and the log | Start the node on the mismatched pair; edit offsets |
| Log gap / retention truncation | `make doctor`; shard logs for the gap error | Everything: volume, snapshots, offsets | Replay partially or skip the gap range |
| Halt did not take effect | Check the command's position in the orders log; `/api/state` `halted` flag | Audit topic records, order offsets | Retry trading-token requests; use TCP for admin |
| `audit_unavailable` (500) after halt/resume | Inspect node log for operator/action/symbol/order offset | The order-log offset (command may be applied) | Assume the halt failed or retry blindly without checking |
| `/ws` stale (>3 s without a frame) | `/api/state`, `/metrics` (`whippet_md_freshness_milliseconds`) | Hub/consumer logs | Treat the last frame as live |
| `/ws/md` gap or unexpected `resync` | Reconnect and take a new snapshot; check `whippet_md_feed_resyncs_total` | Client seq log | Assume the missed deltas arrive later |
| Turbo `window_full` burst | `whippet_turbo_uncommitted_window`, journal lag, append latency | Window/mode config, journal health | Raise the window as a tuning tweak without treating it as a loss-window decision |

## 12. Evidence status

What is measured on dev-loop vs what is not proven. Do not add numbers that
are not already in the linked docs.

Measured on dev-loop (Apple M4, Docker Desktop; class `dev-loop`, not
headline):

- Batch-size sweep, single repetitions: HTTP batch shapes and the TCP batch-10
  10,000 orders/s shape are recorded in
  [BENCHMARKS.md](BENCHMARKS.md#dev-loop-result-final-batch-size-sweep-after)
  and [README](README.md#current-dev-loop-reading).
- Group commit coalesced up to 78 messages per append at 5,000 orders/s
  single-order ingress
  ([BENCHMARKS.md](BENCHMARKS.md#dev-loop-result-group-commit-and-tcp-ingress)).
- Soak: 180 s at 5,000 orders/s batch 10 grew ingress p50 1.1 → 5.0 ms and
  p99 ~7 → ~390 ms with node RSS 119 → 282 MiB; fails the 5% drift gate. The
  stage budget attributes the growth to the Iggy append round trip
  (`gateway_append` p99 448 ms) with matcher CPU unchanged (`engine_process`
  4 µs)
  ([BENCHMARKS.md](BENCHMARKS.md#soak-constant-rate-proof--dev-loop-measured)).
  The durable path is not steady-state at 5,000/s for minutes on Docker Desktop.
- Fanout: 1/10/100-subscriber `/ws` shapes recorded; 500 subscribers showed a
  stalled client and ~1% ingress errors; 1,000 hits Docker Desktop host-port
  proxy resets before the node cap
  ([BENCHMARKS.md](BENCHMARKS.md#websocket-fanout-20-hz-conflated-feed--dev-loop-measured)).
- Replay determinism: one local check where full replay from 0 and
  snapshot-seeded replay from offset 158548 both ended at
  `c2b9838b…6739` ([README](README.md#correctness-check)). One run, one log,
  one build — not a crash test, not a benchmark.
- MD feed hot path: ~7.7M events/s with no subscriber, ~1.6M events/s with
  one subscriber, dev profile ([ADR 0016](adr/0016-sequenced-price-level-market-data-feed.md#cost-on-the-hot-path)).

Unproven (planned, pending, or explicitly not claimed):

- Native Linux headline numbers: none exist in the repository. Only a native
  Linux host can produce a `headline` label
  ([BENCHMARKS.md](BENCHMARKS.md#status)).
- Retention deletion: nothing deletes segments yet; the eligibility rule,
  window arithmetic, and deletion metrics are design-only
  ([ADR 0014](adr/0014-bounded-storage-and-replay-safe-retention.md)).
- Crash-test and soak proofs: `scripts/crash-test.sh` and `scripts/soak.sh`
  exist as repeatable methods; no crash-test summary has been recorded yet
  ([BENCHMARKS.md](BENCHMARKS.md#crash-recovery-kill--9-proof--method-recorded-results-pending)).
- HA: single node only; no failover, no measured RPO/RTO.
- Sub-millisecond durable path: not published, not supported by any recorded
  run ([PERFORMANCE.md](PERFORMANCE.md)).
