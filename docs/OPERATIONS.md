# Operations

Single-node, single-broker operations for the Phase 1 durable slice. There is
no HA story in this build; the single-node caveats are part of the contract,
not a footnote.

Operator sequence (boot, disk pressure, verify, replay, restore, crash
recovery, incidents): [RUNBOOK.md](RUNBOOK.md).

## Configuration

The node reads these from flags or environment variables. Every value has a
demo-safe default; `deploy/compose.yaml` sets the container paths and the Iggy
credentials explicitly.

| Variable | Default | Meaning |
|---|---|---|
| `WHIPPET_HTTP_ADDR` | `0.0.0.0:8080` | HTTP and WebSocket listen address. |
| `WHIPPET_TCP_ADDR` | `0.0.0.0:8081` | Binary ingress listen address. Set `off` to disable. |
| `WHIPPET_IGGY_ADDR` | `127.0.0.1:8090` | Iggy TCP address. |
| `WHIPPET_IGGY_USER` | `iggy` | Iggy username. Compose must pass a non-default account. |
| `WHIPPET_IGGY_PASSWORD` | `iggy` | Iggy password. Compose must pass a non-default secret. |
| `WHIPPET_IGGY_PASSWORD_FILE` | unset | Read the Iggy password from a file instead of the environment. Compose points this at `/run/secrets/iggy_root_password`; the container entrypoint also exports the value. |
| `WHIPPET_API_TOKEN` | unset | Optional legacy bearer token for `/api/orders`, `/api/orders/batch`, and `/api/cancel`, bound to `WHIPPET_API_PARTICIPANT`. Prefer `WHIPPET_API_TOKENS` when more than one desk submits orders. |
| `WHIPPET_API_PARTICIPANT` | `api` | Participant identity bound to the legacy single token. A request naming a different participant is rejected with 403. |
| `WHIPPET_RATE_LIMIT_PER_SEC` | `0` | Optional per-IP ingress limit; `0` disables. Shared by HTTP and TCP, charged per order before validation: a batch reserves `N` tokens for its `N` orders up front, and a batch larger than the per-second budget is rejected. A rejected request returns HTTP 429 `rate_limited` with a `Retry-After: 1` header (TCP returns the `RateLimited` error code). |
| `WHIPPET_MAX_WS_CONNECTIONS` | `2048` | Concurrent WebSocket subscriber cap. |
| `WHIPPET_WS_PER_IP` | `0` | Per-IP WebSocket subscriber sub-limit; `0` disables it. The fanout benchmark leaves it off because all subscribers share one host. |
| `WHIPPET_JOURNAL` | `iggy` | `iggy` for the demo, `memory` for tests and brokerless development. |
| `WHIPPET_MODE` | `durable` | `durable` or `turbo`. Turbo requires `WHIPPET_JOURNAL=iggy`, acknowledges after the match (`matched_unjournaled`), and can lose at most the bounded uncommitted window on a crash. Mode is fixed at startup; switching modes on an existing log requires a reset (see [Turbo mode](#turbo-mode)). |
| `WHIPPET_SYMBOLS` | `BTC-USD,ETH-USD,SOL-USD` | Comma-separated subset of the built-in instruments. Unknown symbols are rejected. |
| `WHIPPET_PARTITIONS` | `16` | Trading partitions, 1-256. This is a create-time contract; see below. |
| `WHIPPET_SNAPSHOT_DIR` | `data/snapshots` | Directory for `partition-<N>.snapshot` files. |
| `WHIPPET_SNAPSHOT_EVERY` | `10000` | Commands per symbol between state snapshots. The node also enforces a minimum interval (default 30 s, `WHIPPET_SNAPSHOT_MIN_INTERVAL_SECS`, clamped 5–300 s), so a high-throughput run snapshots at most once per interval. Values below 1 are clamped to 1. |
| `WHIPPET_ENGINE_JOURNAL_CLIENTS` | active shard count | Engine-side Iggy connections in the per-shard pool, 1–16, clamped to that range. Defaults to the active shard count (one connection per active shard). `engine_journal_for(shard)` maps each shard deterministically to one pool client; the shard reader/writer tasks and both MD consumers select through it, and the consumer-lag sweep reads each active shard's `committed`/`lag` through its own pool client — never the ingress client. The `engine_journal` observer handle shares pool entry 0 and opens no extra connection; neither the health monitor nor the lag sweep reads through it. |
| `WHIPPET_ENGINE_JOURNAL_PENDING_MIB` | `256` | Total client-side pending-operations budget for the engine pool, in MiB, clamped to 16–4096. Split as `max(16 MiB, total / pool_size)` per client and passed to `IggyJournal::connect_with_pending`. A large pool can therefore exceed the configured total rather than starve a client. The ingress client keeps its own 256 MiB budget via `IggyJournal::connect`. Unused in `memory` journal mode, where every pool entry is the ingress `Arc`. |
| `WHIPPET_TURBO_WINDOW_COMMANDS` | `10000` | Turbo only: commands allowed in one shard's uncommitted window. Validated at startup (1–1,000,000); a zero or oversized value is a startup error. |
| `WHIPPET_TURBO_WINDOW_MS` | `50` | Turbo only: maximum age of the oldest unjournaled command in one shard's window, in ms (1–60,000). The binding limit is whichever of count or age is hit first. |
| `WHIPPET_API_TOKENS` | unset | Scoped tokens as `token:participant` pairs, comma separated (for example `tok-a:alice,tok-b:bob`). Each token authorizes only its participant: the gateway derives the participant id from the name, so self-trade prevention and attribution see distinct desks. Tokens cannot contain `:`. Merged with `WHIPPET_API_TOKEN`. |
| `WHIPPET_METRICS_TOKEN` | unset | Optional bearer token for `GET /metrics`. Health and readiness stay unauthenticated for probes. |
| `WHIPPET_OPERATOR_TOKENS` | unset | Operator credentials for the instrument control plane, as `token:operator-name` pairs, comma separated. Separate from `WHIPPET_API_TOKENS`: operator tokens cannot submit orders, and trading tokens cannot halt/resume. With none configured, every admin request is 403 `operator_not_configured`. |
| `WHIPPET_DATA_DIR` | `/data` | Directory on the node's own data volume; the storage monitor samples the filesystem that holds it. |
| `WHIPPET_IGGY_DATA_DIR` | `/iggy-data` | Read-only mount of Iggy's data volume. Resolved once at startup; a missing path disables the Iggy sample instead of failing the node. |
| `WHIPPET_RISK_MAX_ORDER_QTY` | `0` | Optional per-order quantity cap at HTTP/TCP ingress. `0` disables. A violation returns HTTP 422 `risk_limit` or TCP `RiskRejected` before any append. |
| `WHIPPET_RISK_MAX_NOTIONAL_CENTS` | `0` | Optional per-order notional cap (`price × qty`, priced orders only) at HTTP/TCP ingress. `0` disables. Market orders are not notional-checked in this slice. |
| `WHIPPET_DISK_WARN_PCT` | `70` | Volume usage percentage that raises the warning state. 1–99 and strictly below the critical value. |
| `WHIPPET_DISK_CRIT_PCT` | `85` | Volume usage percentage that raises the critical state: readiness fails and new orders/cancels are refused with `storage_pressure`. 1–99 and strictly above the warning value. |
| `WHIPPET_HTTP_TRACE` | unset | Set to `1` to enable per-request `tower-http` trace spans; off by default for throughput. |

Concurrent single-order submissions on one partition are coalesced into one
Iggy append by a per-partition group-commit writer (up to 256 messages). The
writer does not add a fixed linger; it drains what is queued when it wakes. The
effect is visible in `/metrics` as `whippet_group_commit_batches_total` versus
`whippet_group_commit_messages_total`.

`POST /api/orders/batch` accepts up to 256 orders of one symbol and performs one
Iggy append. The batch is charged against the rate limit before validation, so
a large batch cannot spend validation work outside its budget; the 429
response carries `Retry-After: 1`. Mixed-symbol batches are rejected with
`mixed_symbols`; an invalid order rejects the whole batch. `?acks=none`
returns only the batch summary
(partition, base offset, count) and is intended for the bench and fire-and-forget
clients.
| `WHIPPET_DASHBOARD_DIR` | `apps/dashboard/dist` | Static dashboard assets. If `index.html` is missing, the API serves a build-instructions page. |
| `WHIPPET_VENUE` | `whippet` | Venue name in the state view and headers. |
| `WHIPPET_BUILD` | `dev` | Build label shown on the dashboard. |

Other processes:

| Variable | Default | Meaning |
|---|---|---|
| `WHIPPET_GATEWAY` | `http://127.0.0.1:8080` | Simulator target. |
| `WHIPPET_IMAGE_DIGEST` | unset | Recorded in the bench manifest. |
| `WHIPPET_VIRTUALIZATION` | platform default | Recorded in the bench manifest; set it to `docker-desktop` when running the harness on a Mac. |
| `WHIPPET_ISOLATED_CORES` | unset | Recorded in the bench manifest. |
| `RUST_LOG` | `whippet=info,...` | Tracing filter. |

`WHIPPET_JOURNAL=memory` is an explicit developer escape hatch. It has no
durability, no broker, and no cross-process consumers. The demo and every
published result use `iggy`.

## Turbo mode

`WHIPPET_MODE=turbo` (or `--mode turbo`) acknowledges an order after
`ShardEngine::process` returns, before the write-behind has appended it. The
ack carries `guarantee: "matched_unjournaled"`; durable acks carry
`"accepted_by_log"`. Matching semantics, the books, the state hash, and the
snapshot format are identical to durable. The difference is the crash window:
commands admitted but not yet appended are lost, bounded by
`WHIPPET_TURBO_WINDOW_COMMANDS` and `WHIPPET_TURBO_WINDOW_MS`.

- **Ingress.** HTTP and TCP admit the command through a per-shard bounded lane.
  A full window returns HTTP 503 `window_full` (TCP error code `WindowFull`)
  with nothing journaled; the node never silently falls back to durable.
- **Write-behind.** The shard writer appends `orders` first, then executions
  and MD, and only then releases the window slot, advances the applied offset,
  and commits. A failed orders append stops the shard exactly like a failed
  output append.
- **Recovery.** A turbo restart from a snapshot replays the journaled tail
  from `snapshot_offset + 1` through the log head before accepting ingress, so
  the engine is at the log head when the lane opens. A restart with no
  snapshot requires an empty orders log (the seq domain must be anchored); a
  non-empty log without a snapshot is a startup error.
- **Mode switch.** Switching durable ↔ turbo on an existing log is not
  supported; `make reset` (or a new topic set) is required. The snapshot does
  not carry the mode, so the node cannot detect a mismatch: with Compose, run
  `WHIPPET_MODE=turbo docker compose up -d node` (not a bare
  `docker compose restart`, which re-renders the default `${WHIPPET_MODE}` and
  can recreate the container in durable mode).
- **Metrics.** `whippet_turbo_uncommitted_window{shard}`,
  `whippet_turbo_journal_lag{shard}` (the same set in this design),
  `whippet_turbo_window_drops_total{reason="count"|"age"}`, and the
  `whippet_turbo_ack_latency_microseconds` summary.
- **Backpressure is expected, not a failure.** On a host whose journal stalls
  exceed the age bound (Docker Desktop routinely does), bursts are rejected
  with `window_full`; clients retry. The dev-loop 5,000 orders/s run saw
  1,052/300,000 commands rejected by the age bound and zero by the count
  bound. Raise `WHIPPET_TURBO_WINDOW_MS` only as an explicit loss-window
  decision.

## Topic creation

On startup the node ensures three streams and six topics exist:

| Stream | Topic | Partitions | Durability | Retention | Max size |
|---|---|---|---|---|---|
| `whippet-trading` | `orders` | `WHIPPET_PARTITIONS` (16) | persisted | 24 h | 8 GiB |
| `whippet-trading` | `executions` | `WHIPPET_PARTITIONS` (16) | persisted | 7 d | 8 GiB |
| `whippet-md` | `md` | `WHIPPET_PARTITIONS` (16) | replicated | 1 h | 8 GiB |
| `whippet-ops` | `alerts` | 4 | persisted | 30 d | 2 GiB |
| `whippet-ops` | `audit` | 4 | persisted | 90 d | 2 GiB |
| `whippet-ops` | `commands` | 1 | persisted | 7 d | 2 GiB |

Creation also sets 64 MiB segments, a flush threshold of 1,024 messages or
1 MiB, and consumer-offset durability matching the topic durability.

Existing topics are left alone. Changing `WHIPPET_PARTITIONS` or a retention
value after the topics exist has no effect until the topics are recreated.
Partition count is part of the data layout: the symbol-to-partition hash
changes with the count, so a change requires `make reset` or an explicit
migration, not a restart.

`alerts`, `audit`, and `commands` are reserved. No Phase 1 producer writes them
and no consumer reads them.

## Retention and disk

| Data | Location | Retention |
|---|---|---|
| Iggy log | Iggy data volume, managed by Compose | Per topic: 24 h orders, 7 d executions, 1 h MD, 7-90 d ops. |
| Snapshots | `WHIPPET_SNAPSHOT_DIR` (`data/snapshots` in the container) | Until deleted. One file per active partition. |
| Bench manifests | `artifacts/bench/<run-id>.json` | Until deleted. |

Rough sizing, not a measurement:

- An order payload is 78 bytes plus seven headers and Iggy record overhead. At
  400 orders/s, a 24-hour orders topic is a few GB of payload, before segment
  overhead. At 200 orders/s it is roughly half that.
- Executions are 104 bytes each and an order can produce several reports.
  Seven days of executions is usually the largest retention cost. Size it from
  your own order-to-execution ratio.
- MD events are 84 bytes each. A snapshot carries the full resting-order set plus the display levels; size it from the book, not a fixed header. One hour of MD at a few hundred orders/s is
  hundreds of MB, depending on how many events each order produces.
- A snapshot is proportional to resting orders plus dedup entries. The dedup
  table is capped at 100,000 entries per symbol (roughly 20 s of history at 5,000 commands/s), which bounds the snapshot size. Measure the files with
  `du -h data/snapshots`; do not assume a fixed size.
- Full MD snapshots are deliberately coarse (every 1,000,000 commands, plus
  startup). The MD hub normally rebuilds from the delta stream; periodic full
  snapshots exist for recovery, not for every UI frame.

On macOS, keep Iggy data in a named Docker volume, not a bind mount to the host
filesystem. Bind-mounted I/O is slow and changes the failure behavior.

These retention values are recorded at topic creation, but in Iggy 0.10 they
are enforced only by the messages cleaner — and the default stack disables
it, so nothing trims the log. See
[Log growth and retention](#log-growth-and-retention) for the exact state.

## Pre-trade ingress risk

The gateway and TCP listener enforce two optional per-order limits before any
journal append: `WHIPPET_RISK_MAX_ORDER_QTY` and
`WHIPPET_RISK_MAX_NOTIONAL_CENTS` (both `0` = disabled). Rejections are explicit
— HTTP 422 `risk_limit`, TCP `RiskRejected` — and counted in
`whippet_risk_rejected_total{reason}`. This is an admission guard, not an
engine-enforced risk control: a producer that appends directly to the orders
topic bypasses it, and the engine remains the matching authority. Market orders
are quantity-checked but not notional-checked. See
[ADR 0017](adr/0017-ingress-risk-and-position-reconstruction.md).

## Position reconstruction (read-only)

`whippet positions --partition 1 [--symbols ...] [--participant N] [--json]`
reconstructs per-participant, per-symbol net/buy/sell quantity, fills, cash
flow, and turnover from the orders and executions logs. One execution report is
one trade; the maker leg is the opposite side and is attributed through the
orders log. Unknown order ids are reported as unresolved counters, never
inferred. It is a reconstruction, not a settlement ledger: no balances,
deposits, fees, funding, margin, or clearing. It never appends or commits.

## Surveillance indicators (read-only)

`whippet surveillance` scans the orders and executions logs for one partition
and reports per-participant, windowed indicators: a high cancel ratio, large
cancelled orders with no fill, self-trade-prevention events, and duplicate
order-id retries. It is deterministic and read-only, and its output is
explicitly **educational, not a regulatory finding**.

```sh
whippet surveillance --partition 1 --window-secs 3600
whippet surveillance --partition 1 --min-orders 50 --cancel-ratio-bps 8000 --json
```

The window uses the newest event timestamp in the scanned log as "now"; no
wall clock is read. `--fail-on-findings` exits non-zero when any participant
is flagged (for monitoring/CI), while an empty window is a successful exit.
See `docs/RUNBOOK.md` for the operator sequence.

## Instrument control plane

Halt and resume are operator-only HTTP endpoints:

```sh
curl -X POST http://127.0.0.1:8080/api/admin/instruments/BTC-USD/halt \
  -H "Authorization: Bearer $WHIPPET_OPERATOR_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"reason":"maintenance"}'
curl -X POST http://127.0.0.1:8080/api/admin/instruments/BTC-USD/resume \
  -H "Authorization: Bearer $WHIPPET_OPERATOR_TOKEN" -d '{}'
```

The response is `AdminAck`: `symbol`, `action`, `halted`, `partition`,
`offset` (orders log), `operator`, `audit_offset`. Authorization is a separate
namespace (`WHIPPET_OPERATOR_TOKENS`), never a trading token; no operator
tokens configured means 403, missing/invalid means 401. The command is appended
to the same orders log as client commands, so a restarted engine replays the
halt state. A halted instrument rejects new orders with `halted`, allows
cancels, and shows `halted: true` in `/api/state` and the dashboard badge.
Each accepted request appends a JSON audit record to the `audit` topic; if that
append fails the endpoint returns 500 `audit_unavailable` because the command
may already be applied. See [ADR 0015](adr/0015-audited-instrument-control-plane.md).

TCP `Submit` rejects Halt/Resume commands; the control plane is HTTP-only.

## Disk capacity and admission

The node samples free space on two volumes with `statvfs` every five seconds:

- `WHIPPET_DATA_DIR` (`/data`): snapshots and node-owned files, normally read-write.
- `WHIPPET_IGGY_DATA_DIR` (`/iggy-data`): the broker's volume, mounted
  **read-only** so the node can measure it and cannot delete from it.

`used_pct = ceil((total - free) * 100 / total)`, counting reserved blocks as
used. The worst known volume sets the state:

- **ok** — below `WHIPPET_DISK_WARN_PCT`.
- **warn** — at or above warn (alert only; ingress stays open).
- **critical** — at or above `WHIPPET_DISK_CRIT_PCT`. `/readyz` returns 503
  with `storage: false`; `/api/health` reports `degraded` and
  `storage_state: "critical"`; HTTP order/cancel paths return 503
  `storage_pressure`; TCP returns the `StoragePressure` error code. Nothing is
  journaled and nothing is deleted.

A failed `statvfs` marks that volume `available=false` and contributes **warn**
only, so a transient mount problem cannot stop ingress. The gauges
`whippet_disk_monitor_ok{volume}`, `whippet_disk_free_bytes{volume}`,
`whippet_disk_used_pct{volume}`, `whippet_storage_warning`, and
`whippet_storage_critical` expose the state; see
[the metrics table](#prometheus-metrics).

### Retention plan (read-only)

`whippet retention-plan` computes the oldest orders-log offset that is safe to
delete from the live evidence: the engine consumer's committed offset and the
newest **verified** snapshot. It never deletes, trims, commits, or appends.

```sh
docker compose -f deploy/compose.yaml exec -T node \
  whippet retention-plan --snapshot-dir /data/snapshots --partitions 16
```

The boundary is `min(engine committed, verified snapshot max last_offset)`;
`safe_through = boundary - WHIPPET_RETENTION_SAFETY_MARGIN_RECORDS` (default
10,000). It blocks when there is no committed engine consumer, no verified
snapshot, a boundary at offset 0, the head is behind the boundary, or the
margin consumes the window. The MD consumer's offset is shown as `md-topic`
for diagnostics and is **not** part of the orders boundary. A `blocked` plan is
a valid result and exits 0; only connect/snapshot errors fail. `--json` emits
one object per shard with `"action": "none"`.

This is a guard, not retention. It refuses work before the volume fills and
gives the operator time to act; it never trims segments. The safe response is
to pause ingress (the critical state does it), preserve the volume, run
`make verify-strict`, archive what must be kept, and only then delete data with
explicit approval. The replay-safe deletion rule and the minimum log window are
in [ADR 0014](adr/0014-bounded-storage-and-replay-safe-retention.md).

## Iggy tuning

The default stack keeps the memory-pool and sharding defaults, but overrides
three values (see [Log growth and retention](#log-growth-and-retention)): the
messages cleaner off, a 1 GiB partition WAL, and a 200 µs WAL group-commit
delay. We tested a smaller profile (`IGGY_MEMORY_POOL_SIZE=1GiB`, four
shards, no core pinning, `partition_io_capacity=32`, 136 MiB WAL) and it
regressed durable-ingress latency on a 16 GB Docker Desktop VM, especially
after a broker restart. `deploy/iggy/iggy.toml` contains the alternative as a
documented example for a memory-constrained 3-node VM; do not treat it as a
benchmark-tuned profile without measuring it on the target host.

## Log growth and retention

The log grows with every accepted order and every execution and MD record
Whippet appends. The growth is measurable: in the dev-loop soak
(`scripts/soak.sh`, fresh stack, 5,000 orders/s, batch 10), HTTP ingress p50
grew from 1.1 ms to 5.0 ms and p99 from 7 ms to ~390 ms over 180 s while the
log grew, with node RSS up from 119 MiB to 282 MiB. The stage budget shows
the cost is the Iggy append round trip (`gateway_append` p99 448 ms) with
matcher CPU unchanged (`engine_process` 4 µs), and a run with the larger WAL
measured below grew less (p50 1.2 → 3.0 ms, p99 4.7 → 227 ms over 120 s).
Full numbers and caveats are in [the soak section of
BENCHMARKS.md](BENCHMARKS.md#soak-constant-rate-proof--dev-loop-measured).
The durable path is not steady-state at 5,000 orders/s for minutes on Docker
Desktop; do not quote these as headline results.

Retention state in the default stack:

- Topic creation records a finite per-topic policy (24 h orders, 7 d
  executions, 1 h MD, 30 d/90 d ops) and a max topic size (8 GiB, 2 GiB ops),
  with 64 MiB segments (see [Topic creation](#topic-creation)).
- In Iggy 0.10, that policy is enforced only by the messages cleaner, which
  deletes the oldest sealed segments per partition, best-effort, once per
  interval (default 1 m). The active segment is never touched.
- The default stack disables that cleaner
  (`IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED=false`), so the policies
  are recorded but never enforced: nothing trims the log, and it grows until
  the volume is full. Disabling was measured to remove the cleaner's
  once-per-minute append-latency spikes; it did not remove the growth with
  log size.

The demo discipline is `make reset` before a measured run, not a background
cleanup. A fresh log also removes the cold-start effect: the first run after
`make reset` measured 6.2 ms p50 / 810 ms p99 against 1.3 ms / 7.3 ms for the
same workload immediately after (cold 1 GiB WAL allocation and page cache).
Warm the stack before measuring.

Knobs that matter, and what the default stack does:

| Knob | Active value | Iggy 0.10 default | Why |
|---|---|---|---|
| `IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED` | `false` | `true`, interval 1 m | Disabling removed the cleaner's periodic append-latency spikes in the soak. With it off, no retention policy is enforced. |
| `IGGY_PARTITION_WAL_BYTES_MAX` | `1GiB` | `256 MiB` | Fewer checkpoints and rotations as the log grows; measured with the knob below to grow latency less over a 120 s soak run. |
| `IGGY_PARTITION_WAL_GROUP_COMMIT_DELAY_MICROS` | `200` | `0` | Amortises WAL device writes: one durability barrier covers a group of prepares instead of one per prepare, for up to this much ack latency. |
| `IGGY_PARTITION_IO_CAPACITY` | not set | `16` | Active partition file jobs per shard. The alternative profile in `deploy/iggy/iggy.toml` uses 32. |
| Segment size | 64 MiB per topic, set at creation | 1 GiB | Retention deletes only sealed segments, so segment size bounds the granularity of any future cleanup. |

Risk of enabling expiry enforcement: do not turn the cleaner on, or shorten
the per-topic expiry, without a retention/replay design. The engine and MD
consumers keep their committed offsets in Iggy. If retention deletes a
segment range before a consumer's committed offset, that consumer has a gap
it cannot reconstruct from the log alone, and `whippet replay --expect-hash`
no longer sees the full log. This is the same boundary as under
[Single-node caveats](#single-node-caveats): retention bounds recovery. A
retention shorter than a consumer's downtime is a data-loss design decision,
not a tuning tweak.

## Reset

```sh
make reset
```

Raw:

```sh
docker compose -f deploy/compose.yaml down -v
scripts/reset.sh --data   # optional: also remove host data/ and artifacts/bench
```

`make reset` deletes the Iggy log, the consumer offsets stored in Iggy, and the
snapshots in the named volume. It keeps host `data/`, `artifacts/`, and secrets
unless you pass `--data`/`--all` to `scripts/reset.sh`. It does not touch the
source tree or the dashboard build.

## Preflight

`whippet doctor` is read-only. It connects to Iggy with the node credentials,
checks all six topics and their partition counts, prints partition heads,
verifies snapshot files and state hashes when present, and checks the node
health endpoint. Run it before a demo or after a restart:

```sh
make doctor
# or, with an explicit gateway:
whippet doctor --gateway http://127.0.0.1:8080 --iggy-addr 127.0.0.1:8090
```

## Logs

```sh
docker compose -f deploy/compose.yaml logs -f node iggy
```

Useful node log lines:

- `Iggy is not ready; retrying` with an attempt number and address.
- `created Iggy stream` and `created Iggy topic`.
- `restored shard from snapshot` with the state hash.
- `engine shard running` with the start offset and consumer name.
- `snapshot written` with the path and hash.
- `order poll failed; retrying`, `append failed; retrying`, and
  `offset commit failed; batch will replay`.

For more detail, set `RUST_LOG` in the node service environment. The default
filter is `whippet=info,whippet_engine=info,whippet_journal=info,whippet_marketdata=info,whippet_bench=info`.
There are no per-order log lines on the hot path.

## Health and metrics

`GET /readyz` returns 200 only when the journal is reachable, every shard task
is alive, and no monitored volume is at the critical disk threshold; otherwise
503. Use it for container readiness. It reads cached flags and performs no
journal I/O: every five seconds the health monitor pings the ingress client and
every engine-pool client concurrently and caches the aggregate, refreshes the
consumer-lag sweep, and the storage monitor samples both data volumes.
`GET /api/health` returns HTTP 200 with `status: "ok"` or `"degraded"`; it is
`ok` only when the journal is reachable (or the in-memory journal is selected),
every shard task is alive, and storage is below critical. `shards_ok`,
`storage_ok`, and `storage_state` report the components. A corrupt
snapshot or a shard task panic therefore degrades health instead of leaving a
silent "up but not matching" node. The health monitor pings all pool entries
because every entry is its own Iggy connection: an ingress link that is up
while any engine link is down still means orders are accepted and never
matched. `degraded` means the last ping failed; it does not mean the process
is down.

```sh
docker compose -f deploy/compose.yaml exec node whippet healthcheck
```

`whippet healthcheck` prints the JSON and exits non-zero unless the status is
`ok`. A container healthcheck should use it.

### Sequenced market-data feed

`GET /ws/md?symbol=BTC-USD` is a price-level feed separate from the dashboard's
conflated `/ws` stream. It sends one full-depth `snapshot` at the current
per-symbol `seq`, then every accepted `delta` in sequence order. If a client
falls behind the hub's bounded feed buffer, the server sends a `resync` notice
naming the new sequence and a fresh snapshot; a client that sees an
unexplained gap should reconnect. The feed shares the global WebSocket
subscriber cap and is a market-by-price feed, not order-by-order.

```sh
websocat 'ws://127.0.0.1:8080/ws/md?symbol=BTC-USD'
```

Metrics: `whippet_md_feed_subscribers`, `whippet_md_feed_sent_total`,
`whippet_md_feed_lagged_total`, `whippet_md_feed_resyncs_total`. With no feed
subscriber the MD apply path does no feed work. See
[ADR 0016](adr/0016-sequenced-price-level-market-data-feed.md).

### Prometheus metrics

`GET /metrics` returns the Prometheus text exposition format
(`text/plain; version=0.0.4; charset=utf-8`). The `obs` Compose profile scrapes
this endpoint. A minimal scrape config:

```yaml
scrape_configs:
  - job_name: whippet
    scrape_interval: 5s
    static_configs:
      - targets: ["node:8080"]
```

Series:

| Series | Type | Meaning |
|---|---|---|
| `whippet_orders_ingressed_total` | counter | Orders accepted by the gateway and appended to Iggy. |
| `whippet_orders_matched_total` | counter | Orders the matcher accepted (not rejected). |
| `whippet_orders_rejected_total` | counter | Orders rejected by validation or matching. |
| `whippet_executions_total` | counter | Execution reports produced, including accepts, partial fills, fills, cancels, and rejects. |
| `whippet_trades_total` | counter | Trade events observed by the MD consumer. |
| `whippet_md_events_total` | counter | MD records applied by the hub, including snapshots. |
| `whippet_engine_commands_total` | counter | Input commands applied by the matcher. |
| `whippet_consumer_lag` | gauge | Records waiting behind the engine consumers, summed over the active shards. Every five seconds the monitor reads each shard's committed offset and head through that shard's engine-pool client. |
| `whippet_consumer_lag_by_shard{shard}` | gauge | Per-shard lag, so one lagging partition is visible instead of averaged away. |
| `whippet_input_ring_occupancy{shard}` / `whippet_output_ring_occupancy{shard}` / `whippet_ring_capacity{shard}` | gauge | Bounded-ring backpressure: queued batches and capacity, reported once per second by the matcher. |
| `whippet_ingress_latency_microseconds{quantile="0.5"}` | gauge | Gateway append path p50, process-lifetime histogram. |
| `whippet_ingress_latency_microseconds{quantile="0.99"}` | gauge | Gateway append path p99, process-lifetime histogram. |
| `whippet_match_latency_microseconds{quantile="0.5"}` | gauge | Matcher service-time p50, process-lifetime histogram. |
| `whippet_match_latency_microseconds{quantile="0.99"}` | gauge | Matcher service-time p99, process-lifetime histogram. |
| `whippet_ingress_window_p50_us` / `whippet_ingress_window_p99_us` | gauge | Rolling 10 s gateway ingress percentiles; use these to detect a recent degradation. |
| `whippet_match_window_p50_us` / `whippet_match_window_p99_us` | gauge | Rolling 10 s matcher service-time percentiles. |
| `whippet_latency_window_secs` | gauge | Length of the rolling latency window. |
| `whippet_iggy_log_bytes{topic}` / `whippet_iggy_log_messages{topic}` | gauge | Broker-reported bytes and message count for `orders`, `executions`, and `md`, refreshed every five seconds. |
| `whippet_disk_free_bytes{volume="whippet"|"iggy"}` / `whippet_disk_used_pct{volume=...}` | gauge | Free bytes and used percentage of the monitored data volumes. |
| `whippet_disk_monitor_ok{volume}` | gauge | 1 when the volume usage was read; 0 means the mount is unreadable (warn only, ingress stays open). |
| `whippet_storage_warning` / `whippet_storage_critical` | gauge | 1 when a volume is at or above the warning/critical threshold. Critical refuses new orders and fails readiness. |
| `whippet_throughput_orders_per_second` | gauge | Gateway acceptance rate over the last sample window. |
| `whippet_uptime_seconds` | counter | Process uptime. Monotonic until restart. |
| `whippet_iggy_connected` | gauge | 1 if the last Iggy ping succeeded, else 0. |
| `whippet_partitions` | gauge | Configured trading partitions. |
| `whippet_mode{mode="durable"}` | gauge | One-hot operating mode. |
| `whippet_md_freshness_milliseconds` | gauge | Age of the newest MD event. |
| `whippet_md_sequence` | gauge | Maximum per-symbol MD sequence observed. |
| `whippet_md_feed_subscribers` | gauge | Active sequenced feed receivers. |
| `whippet_md_feed_sent_total` | counter | Delta frames broadcast to feed subscribers. |
| `whippet_md_feed_lagged_total` | counter | Feed receivers that fell behind the bounded broadcast. |
| `whippet_md_feed_resyncs_total` | counter | Resync notices sent (fresh snapshot follows). |

Caveats:

- The unqualified latency quantiles are process-lifetime HDR histograms. Use
  the `*_window_*` series for current behavior; the lifetime values are for
  long-run context, not benchmark results.
- Counters reset when the node restarts. Prometheus handles the reset, but a
  rate over a restart boundary is meaningless.
- `whippet_consumer_lag` is the per-shard sweep described above, not a
  placeholder. Do not alert on it across restarts: the committed offsets reset
  with the broker state.
- `/metrics` is unauthenticated unless `WHIPPET_METRICS_TOKEN` is set. Publish
  it only on the Compose network, or put it behind a reverse proxy and set the
  token. Do not expose it to the internet.
- The exposition types `whippet_uptime_seconds` as a counter. It is monotonic
  per process, which is the property that matters.

## Resource expectations

These are design expectations, not measurements. Use `docker stats` and the
manifest host data for real numbers.

- **Iggy:** one broker with the default memory pool. Give Docker Desktop at
  least 8 GB for a comfortable demo; the research dossier recommends 12 GB for
  the future 3-node profile. Durable writes are fsync-bound.
- **Node:** one task per active partition (three with the default symbols),
  one task per active MD partition, plus the HTTP server. Each book arena is
  sized for 250,000 orders; the three default books plus dedup tables are a few
  hundred megabytes. Allow a gigabyte. The exact RSS depends on resting order
  count and dedup occupancy.
- **Dashboard:** static files. The server does no per-client rendering; it
  publishes one state frame at 20 Hz and each WebSocket connection clones it.
- **Disk:** dominated by the Iggy log retention above, then snapshots.

## Single-node caveats

- `persisted` means the single Iggy node wrote the record to its local disk.
  It survives a node process restart. It does not survive disk loss, host
  loss, or a bad `docker compose down -v`.
- `replicated` on the MD topic records the requested policy. With one node
  there is no peer, so it is not a second copy and it is not an HA guarantee.
- There is no leader election, fencing, or failover. If the broker is down, the
  node retries and the gateway returns 503; it does not promote anything.
- Consumer offsets live in Iggy. Deleting the Iggy volume resets them.
- Retention bounds recovery. A consumer that is down longer than the topic
  retention cannot reconstruct the missing window from the log alone.
- Command timestamps come from the gateway's wall clock. They are
  informational; ordering is the log order, not the timestamp.
- Cross-symbol atomicity does not exist. Each partition is an independent
  state machine.

## Backup and restore

The log is the source of truth. Snapshots are a recovery accelerator, not a
backup of the business record.

To take a consistent-enough copy for a demo:

```sh
docker compose -f deploy/compose.yaml stop node
cp -a data/snapshots /path/to/backup/snapshots-$(date +%Y%m%d)
docker compose -f deploy/compose.yaml start node
```

If `data/` is not mounted to the host, copy it out of the container instead:

```sh
docker compose -f deploy/compose.yaml cp node:/app/data/snapshots ./snapshots-backup
```

Each snapshot file is written to a temporary file, `fsync`ed, and renamed, so
an individual file is never partial. A set of shard files is not a global
point-in-time: shards snapshot independently. That is acceptable here because
each partition is an independent state machine.

A snapshot is only meaningful with its `last_offset` and state hash.
`whippet verify` checks the hash against a rebuilt engine. `whippet replay
--expect-hash <hex>` checks that the log still produces the expected state.
Neither command proves that the snapshot is the newest possible state.

Fail-closed verification and replay:

- `whippet verify --require-snapshot` exits non-zero unless every active
  shard (a shard that owns at least one configured symbol) has a snapshot
  file; a partial set is not a proof. `--symbols` (`WHIPPET_SYMBOLS`) must
  match the node's universe.
- `whippet verify --partitions 4` checks shards `0..4` with a 4-partition
  exchange layout (a snapshot written with one partition count must be
  verified with the same count, not the 16-partition default).
- `whippet replay --require-snapshot` refuses to fall back to a full replay
  from zero when the snapshot is missing, unusable (no applied commands),
  or not before the log head (`snapshot_offset > head` and
  `snapshot_offset >= head` both fail); it also fails when any record was
  skipped or undecodable or the run was not seeded. Use `--symbols`
  (`WHIPPET_SYMBOLS`) and `--exchange-partitions` to match the node's layout.
  The replay summary reports `snapshot_offset`, `committed_offset`, `head`,
  and `tail` so an empty or stale snapshot is visible. The commit watermark
  may lag the snapshot; only the log head bounds it.

Restore:

1. Stop the node.
2. Put the `partition-<N>.snapshot` files back in `WHIPPET_SNAPSHOT_DIR`.
3. Start the node. Each shard verifies its hash before using the snapshot. A
   mismatch aborts that shard with an error; the node does not silently use a
   corrupt file. `whippet replay` never writes a snapshot; it only reads the
   log and the existing files.

To rebuild a shard from scratch, delete the snapshot; the engine starts at
offset zero and replays the whole log (the committed offset only seeds the
commit watermark). That is a full replay. If the log has been truncated by
retention, replay from zero hits a gap and the shard fails closed; the safe
reset for the demo is `make reset`.

Upgrade path for the snapshot format: v3 and older files are rejected with
`BadVersion`, and a file that fails verification fails its shard closed (the
shard is marked failed). There is no in-binary migration and no automatic
fallback to a previous file or to a full replay. Delete or move the old
snapshot files (or `make reset`), start the new node so it full-replays from
0, and let it write v4. Recovery always requires operator action; the node
never recovers silently.

If you want to cold-copy the Iggy volume, stop the whole stack first. Do not
copy a live volume.

## Upgrade pins

| Artifact | Pin |
|---|---|
| Iggy server image | `apache/iggy:0.10.0` |
| Rust SDK | `iggy = "=0.12.0"` |
| Wire protocol | 0.12.0 |
| Rust toolchain | 1.95.0 |
| Edition | 2024 |
| License | Apache-2.0 |

The server and SDK must speak the same wire version. Mixed versions are
refused by Iggy, not silently negotiated. Do not use `latest` or `edge` tags.

Upgrade procedure:

1. Read the Iggy release notes for the server and the Rust SDK.
2. Bump the server image and the SDK pin together.
3. Run `make reset && make up`, then `make demo`, `make verify`, and a
   `whippet replay --expect-hash` check against a known log.
4. Record the new image digest in `WHIPPET_IMAGE_DIGEST` for new manifests.
5. Update this table and the manifest docs.

Rollback: pin both artifacts back. Snapshots use format version 4 with a
`WHIPSNP1` magic and bincode; versions 3 and older are rejected with
`BadVersion`, and a snapshot-format change is a data migration, not
a rolling upgrade: delete or move the old files (or reset the data) and let
the new node full-replay from 0 and write v4.

The phase gates for larger changes are in [ROADMAP.md](ROADMAP.md).
