# Whippet

An independent Apache Iggy showcase: a small exchange where a deterministic
price-time matcher reads orders from a partitioned log and writes executions
and market data back to it.

## Status

**Phase 1: durable vertical slice.** The whole path runs on one machine:

- HTTP and WebSocket gateway on port 8080.
- Orders appended to Iggy `whippet-trading/orders`, partitioned by symbol
  hash.
- One dedicated matcher OS thread per active partition: async reader task →
  bounded input ring → matcher thread (the single writer) → bounded output
  ring → async writer task. The book is an array price ladder with an
  occupancy bitmap and FIFO levels.
- Execution reports and market-data deltas appended to Iggy `executions` and
  `whippet-md/md`.
- A market-data hub that consumes the MD log and publishes a conflated state
  frame at 20 Hz.
- A React/TanStack dashboard, a seeded simulator, an open-loop bench harness,
  snapshot verification, and a replay CLI.
- Batch ingress (`POST /api/orders/batch`, up to 256 orders per Iggy append),
  optional bearer auth, per-IP rate limiting, readiness, and `whippet doctor`.
- A Prometheus text endpoint at `GET /metrics`.

Turbo mode is implemented: `WHIPPET_MODE=turbo` acknowledges after the match,
with a bounded write-behind window and an explicit `matched_unjournaled`
guarantee. What is **not** in this build: 3-node VSR/HA, surveillance, the
audit archive and replay scrubber, PATs, TLS, stream encryption, FIX,
cross-symbol atomicity, clearing, and headline Linux benchmark numbers. The
`alerts`, `audit`, and `commands` topics are created but have no producers or
consumers yet.

## 60-second quickstart

Docker must be running. Port 8080 must be free. The first build takes
a few minutes; after that the stack comes up in seconds.

```sh
git clone https://github.com/cryptus-neoxys/whipet.git
cd whippet
make up
```

Open <http://localhost:8080>.

```sh
make demo     # scripted crossing order and trade
make bench    # open-loop benchmark; longer than the quickstart
make bench-sweep  # batch-size sweep: 1/10/100/256 orders per HTTP request
make verify   # verify snapshot state hashes
make doctor   # read-only preflight: Iggy topics, snapshots, health
make down     # stop the stack
make reset    # stop and delete named volumes; keeps secrets and local artifacts
```

Raw equivalents, if you prefer not to use the Makefile:

```sh
docker compose -f deploy/compose.yaml up -d --build
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml logs -f node
docker compose -f deploy/compose.yaml down
```

`make reset` is the destructive one: it runs `down -v` and removes the named
volumes. Raw form:

```sh
docker compose -f deploy/compose.yaml down -v
scripts/reset.sh --data   # optional: also remove data/ and artifacts/bench
```

The API is unauthenticated by default and intended for localhost. Set
`WHIPPET_API_TOKEN` and `WHIPPET_RATE_LIMIT_PER_SEC` before exposing it beyond
your machine; see [SECURITY.md](SECURITY.md).

Two Iggy 0.10 details are handled by `deploy/compose.yaml`, not by the app:
Iggy needs `io_uring`, so the container runs with
`security_opt: ["seccomp=unconfined"]`, and it is started with non-default root
credentials. The node reads those credentials from `WHIPPET_IGGY_USER` and
`WHIPPET_IGGY_PASSWORD`. Do not run the demo with `iggy/iggy`.

If the browser shows "Whippet API is running" instead of the dashboard, the
static assets were not built. See [DEMO.md](docs/DEMO.md#dashboard-assets-are-missing).

## Five demo beats

Run these against a fresh stack (`make reset && make up`). The simulator is part
of the default stack, so the book may already have depth; use `make demo` for
the deterministic scripted version, or stop the simulator with
`docker compose -f deploy/compose.yaml stop sim` if you want a quiet book.
Values that change per run are marked `<...>`.

### 1. Place an order

BTC-USD has a 100-cent tick and a $40,000-$90,000 ladder, so `6500000` means
$65,000.00.

```sh
curl -s -X POST http://localhost:8080/api/orders \
  -H 'content-type: application/json' \
  -d '{"symbol":"BTC-USD","side":"buy","order_type":"limit",
       "price_cents":6500000,"qty":10,
       "client_order_id":"readme-1","participant":"demo-buyer"}'
```

Expected shape:

```json
{
  "order_id": "<32 hex chars>",
  "symbol": "BTC-USD",
  "partition": 1,
  "offset": "<offset>",
  "mode": "durable",
  "status": "accepted_by_log",
  "guarantee": "accepted_by_log",
  "reject": null,
  "accepted_at_ns": 1760000000000000000,
  "durable_ack_ms": 1.4
}
```

Every ack carries `guarantee` (`accepted_by_log` in durable mode,
`matched_unjournaled` in turbo mode) and `reject` (set only when turbo
rejects the command, e.g. `window_full`).

`accepted_by_log` means the record is in Iggy. It does not mean the order has
been matched. Matching happens after the engine consumes the record.

For throughput, `POST /api/orders/batch` accepts up to 256 orders of one symbol
and performs one Iggy append. `?acks=none` returns only the batch summary. See
[DEMO.md](docs/DEMO.md#batch-ingress).

### 2. See the fill in the tape

Submit a sell that crosses the resting bid. Use a different participant:
self-trade prevention cancels the resting order instead of filling it.

```sh
curl -s -X POST http://localhost:8080/api/orders \
  -H 'content-type: application/json' \
  -d '{"symbol":"BTC-USD","side":"sell","order_type":"limit",
       "price_cents":6500000,"qty":4,
       "client_order_id":"readme-2","participant":"demo-seller"}'

sleep 0.2
curl -s 'http://localhost:8080/api/trades?limit=3'
```

Expected: one trade with `price_cents: 6500000`, `qty: 4`, `side: "sell"`,
and the taker's `order_id`. The tape is newest first. The `side` is the
aggressor's side.

### 3. Inspect the offset

The acknowledgement's `partition` and `offset` are the Iggy partition and
per-partition record offset in `whippet-trading/orders`. The second order
should land at the next offset on the same partition.

```sh
curl -s http://localhost:8080/api/state | python3 -c '
import json, sys
s = json.load(sys.stdin)
btc = next(b for b in s["symbols"] if b["symbol"] == "BTC-USD")
print({
    "mode": s["mode"],
    "md_sequence": s["sequence"],
    "freshness_ms": s["freshness_ms"],
    "btc_seq": btc["seq"],
    "best_bid_cents": btc["best_bid_cents"],
    "best_ask_cents": btc["best_ask_cents"],
})'
```

The engine stamps the input offset as `cause_offset` on every execution and
market-data record it produces. Offsets are per partition and are not
guaranteed contiguous after retention or recovery, so never use an offset as
a count. `md_sequence` in the state view is a display value: the maximum
per-symbol MD sequence, not a global sequence.

### 4. Run a simulator scenario

```sh
docker compose -f deploy/compose.yaml exec node \
  whippet sim --scenario flash_crash --rate 400 --duration 30 --seed 7
```

Expected log lines: `simulator started`, a progress line every five seconds
with `sent`, `errors`, `dropped`, and `open_orders`, then `simulator stopped`.
The dashboard should show the tape and the book moving within a second.

Scenarios: `calm_open`, `volatile_news`, `flash_crash`, `liquidity_drought`.
The simulator is seeded and deterministic. Restarting it with the same seed
regenerates the same order ids, and the engine rejects them as duplicates.
Use a new `--seed` for a new flow. Without `--seed`, the simulator uses a fresh
run nonce, so a restart does not replay old client order ids.

### 5. Produce a bench manifest

```sh
make bench
```

Expected output:

```text
<label>  n=<samples>  p50=<...>µs  p99=<...>µs  p99.9=<...>µs  throughput=<...>/s  errors=0  scope=dev-loop
manifest: artifacts/bench/<run-id>.json
```

The manifest is the result. A number without one is an anecdote. On an M4 with
Docker Desktop the scope is `dev-loop`; it is not a headline benchmark. See
[BENCHMARKS.md](docs/BENCHMARKS.md).

Audit and the replay scrubber are Phase 2. The replay CLI is a correctness
tool, not a UI: it re-applies the order log to a fresh engine and prints the
state hash. See [Correctness check](#correctness-check).

## Why Iggy

- **Per-symbol ordering is the matching contract.** The gateway hashes the
  symbol to a fixed partition and pins the partition id. The engine for that
  partition is the only writer to its books.
- **The gateway acknowledges after the log write.** In durable mode the order
  is in Iggy before the client gets `accepted_by_log`.
- **Executions and market data are logs too.** The dashboard is one consumer;
  an audit or surveillance consumer can read the same records later without
  changing the trading path.
- **Offsets and manual commits make recovery explicit.** Each engine and MD
  consumer commits the last offset it applied. Restart resumes from the
  snapshot or the committed offset.
- **Headers carry routing and diagnostics.** Every message has `symbol`,
  `seq`, `venue`, `type`, `schema`, `trace_id`, and `codec` headers; the
  payload stays fixed-layout little-endian.
- **Iggy is not in the match loop.** The matcher is a synchronous state
  machine. The journal edge is async: poll, append, commit.

Not used yet: consumer groups, idempotent producers, PATs, TLS, stream
encryption, and VSR clustering. The current consumers are named consumers with
manual offsets, not group members.

## Architecture

```text
 client
   │  HTTP POST /api/orders        WebSocket /ws
   ▼
┌──────────────────────────────────────────────────────────────┐
│ gateway (Axum)                                               │
│ validate · derive order id · attach headers · append         │
└───────────────┬──────────────────────────────────────────────┘
                │ partition = fnv1a(symbol) % 16
                ▼
┌──────────────────────────────────────────────────────────────┐
│ Iggy stream whippet-trading                                  │
│ topic orders           WHIPPET_PARTITIONS, persisted, 24 h    │
└───────────────┬──────────────────────────────────────────────┘
                │ named consumer whippet-engine-p<N>, manual offset
                ▼ reader task (async, polls the orders log)
┌──────────────────────────────────────────────────────────────┐
│ engine shard for partition N (one logical writer)            │
│ bounded input ring: reader → matcher (capacity 2)            │
│ whippet-shard-N: dedicated matcher OS thread, no await/I/O   │
│ ShardEngine::process -> SymbolEngine -> Book + DedupTable    │
│ bounded output ring: matcher → writer (capacity 2)           │
└───────┬───────────────────────────────┬──────────────────────┘
                ▼ writer task (async: appends outputs, commits)
        │ executions                    │ market data
        ▼                               ▼
┌───────────────────────┐      ┌───────────────────────────────┐
│ whippet-trading       │      │ whippet-md/md                 │
│ executions            │      │ WHIPPET_PARTITIONS, replicated, 1h │
│ persisted, 7 d        │      └───────────────┬───────────────┘
└───────────────────────┘                      │ consumer whippet-md-p<N>
                                               ▼
                                  ┌──────────────────────────────┐
                                  │ MdHub: per-symbol book cache │
                                  │ 20 Hz conflated state frame  │
                                  └──────────────┬───────────────┘
                                                 │ watch
                                                 ▼
                                  ┌──────────────────────────────┐
                                  │ GET /api/state · WS /ws      │
                                  │ GET /metrics · TanStack UI   │
                                  └──────────────────────────────┘
```

The full crate layout, partition keying, snapshot format, and phase boundaries
are in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Honest claims

| Claim | Status in this build |
|---|---|
| Deterministic price-time matching over an ordered input log | Implemented. Integer ticks, no floats, property tests for book invariants and replay determinism. |
| Per-partition ordering by symbol | Implemented. Symbol hashed to a fixed partition, partition id pinned on append. |
| Durable order ingress | Implemented on one node. The gateway returns after the Iggy append confirmation. `persisted` protects against a process restart, not disk or host loss. |
| Executions and market data through Iggy | Implemented. Both are published to the log; the MD hub consumes the MD topic. Executions have no Phase 1 consumer. |
| Snapshot restore with a state hash | Implemented. `whippet verify` recomputes each snapshot hash. |
| Replay reproduces the state hash | Implemented. `whippet replay` rebuilds an engine from the order log. One local dev-loop check: full replay from offset 0 and snapshot-seeded replay from a snapshot at offset 158548 both ended at `c2b9838b...6739`. One run, not a benchmark. |
| Sub-millisecond matching | Not published. It is a design target and a live in-process metric only. No benchmark result exists. |
| Low-single-digit-millisecond durable path on Linux | Not measured. No headline numbers yet. |
| No acknowledged loss across one node failure | Not implemented. Single node, no failover. |
| End-to-end once-only delivery | Not claimed. The pipeline is at-least-once; the engine deduplicates order ids within a bounded count-based window (an evicted id can be re-admitted). |
| Turbo mode | Implemented. Match-complete ack (`matched_unjournaled`), bounded per-shard window (10,000 commands / 50 ms defaults), write-behind orders/executions/MD, tail replay on restart. The window, not the log, is the crash-loss bound. |
| HA / 3-node VSR | Not implemented. |
| Surveillance, audit archive, replay scrubber, PATs, TLS, encryption | Not implemented. |

## Implementation status

| Capability | Status | Notes |
|---|---|---|
| Durable order ingress (Iggy append before ack) | Implemented | Gateway returns `accepted_by_log` after the append confirms. |
| Batch ingress + binary TCP | Implemented | Up to 256 same-symbol orders per append; TCP on port 8081. |
| Per-partition group commit | Implemented | Concurrent single-order appends coalesced, up to 256 per append. |
| Dedicated matcher thread + bounded rings | Implemented | Reader → input ring → matcher OS thread → output ring → writer; see [ARCHITECTURE.md](docs/ARCHITECTURE.md). |
| Snapshot v5 + verify | Implemented | `whippet verify` recomputes the state hash, including the terminal-eviction queue. |
| Replay | Implemented | `whippet replay` rebuilds from the full log or from a snapshot plus tail. |
| Turbo mode | Implemented | `WHIPPET_MODE=turbo`: match-complete ack (`matched_unjournaled`), bounded window; see [ADR 0013](docs/adr/0013-turbo-mode.md). |
| Scoped tokens + metrics auth | Implemented | `WHIPPET_API_TOKENS` binds each token to one participant; `WHIPPET_METRICS_TOKEN` can protect `/metrics`. |
| Consumer groups | Planned | Named consumers with manual offsets today; groups are Phase 2 work. |
| Ingress pre-trade risk | Implemented | Optional per-order quantity/notional caps (`WHIPPET_RISK_MAX_*`), enforced before the Iggy append, with 422 `risk_limit` / TCP `RiskRejected` and rejection metrics. An admission guard, not engine-enforced risk. |
| Position reconstruction | Implemented (read-only) | `whippet positions` reconstructs per-participant/symbol net quantity, buy/sell, fills, and cash flow from the orders + executions logs. Not a settlement ledger. |
| Surveillance indicators | Implemented (educational) | `whippet surveillance` reports windowed cancel-ratio, large-cancel-without-fill, self-trade-prevention, and duplicate-id indicators per participant. Read-only; not a regulatory finding. |
| Sequenced market-data feed | Implemented | `GET /ws/md?symbol=...` sends a full-depth price-level snapshot, then every delta with a per-symbol `seq`, and resyncs a lagged consumer with a fresh snapshot. Separate from the conflated dashboard `/ws`. |
| Durable-path state machines | Implemented | Save-time snapshot invariants and hash requirement, commit watermark/retry (`committed <= applied`, bounded drift), and turbo admission (`Queued -> Completed/Abandoned`, exactly-once permit, counted full-window reasons). See [INVARIANTS.md](docs/INVARIANTS.md) and ADR 0021. |
| Explicit shard lifecycle and readiness | Implemented | Each active shard's engine and MD role is `Starting`/`Running`/`Failed`/`Stopped` (atomics); readiness requires every active role to be Running, and `/api/health` names the failing partition/role. See [INVARIANTS.md](docs/INVARIANTS.md). |
| Instrument session state | Implemented | `POST /api/admin/instruments/{symbol}/session` moves an instrument between `pre_open`/`open`/`closed`; only `open` admits new orders, cancels stay allowed, the change is log-ordered/audited and survives replay (snapshot v5). |
| Instrument halt/resume | Implemented | Operator-only HTTP control plane (`WHIPPET_OPERATOR_TOKENS`), log-ordered with orders, audited in the `audit` topic, `halted` in `/api/state` and the dashboard. Cancels remain allowed; new orders reject with `halted`. |
| Retention enforcement | Planned | Policies are recorded but the cleaner is off; ADR 0014 defines the replay-safe rule, and the read-only `whippet retention-plan` computes the safe boundary from consumer commits + a verified snapshot. Nothing deletes segments yet. |
| Surveillance / audit archive | Planned | Phase 2. |
| HA / 3-node VSR | Planned | Phase 3; single node today. |
| TLS | Planned | Phase 2; localhost default today. |
| FIX | Planned | After the internal order contract is stable. |

## Correctness check

Three commands check the state and rebuild it from different starting points:

```sh
# 1. verify the snapshot on disk
whippet verify --snapshot-dir data/snapshots --partitions 16

# 2. replay partition 1 from the beginning of the log
whippet replay --iggy-addr 127.0.0.1:8090 --partition 1 --from 0

# 3. replay the same partition from the snapshot, then the tail
whippet replay --iggy-addr 127.0.0.1:8090 --partition 1 --snapshot-dir data/snapshots
```

On a local single-node run (M4, Docker Desktop, dev-loop), the snapshot was
written at offset 158548 and both replays ended with:

```text
state_hash = c2b9838ba8438245697c93b5396113cbeb38b3a136ae174842f11b1e39536739
```

The first replay reports `"seeded_from_snapshot": false` and `"from": 0`. The
second reports `"seeded_from_snapshot": true`, `"snapshot_offset": 158548`, and
`"from": 158549`. Equal hashes mean the snapshot state plus the replayed tail is
the same state as replaying the whole log. That is a determinism check for
this log and this build. It is not a crash test, not an audit proof, and not a
benchmark.

Inside the Compose network, use `--iggy-addr iggy:8090` and the same
`WHIPPET_IGGY_USER` and `WHIPPET_IGGY_PASSWORD` as the node. The replay
consumer does not commit offsets, so it does not disturb the live engine or MD
consumers. If the node runs a non-default universe or partition count, pass
the same `--symbols` (`WHIPPET_SYMBOLS`) and `--exchange-partitions` the node
uses; `make verify` / `make replay` read `WHIPPET_PARTITIONS` (default 16)
and `scripts/replay.sh` replays partition `WHIPPET_REPLAY_PARTITION`
(default 1).

The examples assume `whippet` is on your `PATH`. Without it, use
`cargo run --release -p whippet -- replay ...`, or run the binary in the node
container with `docker compose -f deploy/compose.yaml exec node whippet ...`.

## Current dev-loop reading

Fresh stack, simulator stopped, mixed limit/IOC/cancel L1 flow, 8 s measured,
2 s warmup, one repetition, Docker Desktop on M4, default snapshots:

| Path | p50 | p99 | Errors |
|---|---:|---:|---:|
| HTTP single order, 1,000 orders/s | 538 µs | 9,567 µs | 0 |
| HTTP single order, 5,000 orders/s (group commit) | 1,772 µs | 4,299 µs | 0 |
| HTTP batch 10, 5,000 orders/s | 1,127 µs | 3,657 µs | 0 |
| HTTP batch 256, 5,000 orders/s | 7,779 µs | 14,671 µs | 0 |
| TCP batch 1, 5,000 orders/s, 32 connections | 1,722 µs | 49,311 µs | 0 |
| TCP batch 10, 10,000 orders/s | 527 µs | 3,921 µs | 0 |

The 5,000 orders/s single-order row used to queue at p50 838 ms with tens of
thousands of errors; per-partition group commit coalesced up to 78 messages per
append and removed the queue. Batch p50 includes waiting for the batch to fill;
per-order amortized p50 is roughly `p50 / batch`. These are **dev-loop**,
one-repetition numbers on a shared laptop. They are not headline results and do
not support a sub-millisecond durable claim. The Linux matrix is the publishable
artifact.

## Measured-numbers policy

- Every published number comes from a run directory under
  `docs/benchmarks/` with a method card and the machine-written manifest.
- M4, Docker Desktop, OrbStack, and Linux VMs are `dev-loop`. Only a native
  Linux host can produce a `headline` label.
- No claim that the durable path is sub-millisecond. No p99.99 from fewer than
  one million samples. No averaged percentiles. No comparison across different
  batching, durability, or encryption settings without labels.
- A single short run is a smoke test, not a benchmark. The one smoke run in
  [BENCHMARKS.md](docs/BENCHMARKS.md) is labeled as such.

## Repo map

| Path | Role |
|---|---|
| `crates/whippet-core/` | Fixed-point ids, wire protocol, JSON DTOs, topic headers, config, benchmark manifest. |
| `crates/whippet-engine/` | Price ladder, bitmap, FIFO book, dedup table, deterministic `SymbolEngine` and `ShardEngine`, snapshots. |
| `crates/whippet-journal/` | The only async Iggy client code: topic bootstrap, appends, polls, manual offset commits. Also an in-memory journal for tests. |
| `crates/whippet-marketdata/` | MD state machine, 250-trade tape, 20 Hz conflated publisher, process metrics. |
| `crates/whippet-sim/` | Seeded market simulator with four scenarios. |
| `crates/whippet-bench/` | Open-loop absolute-deadline harness, HDR histograms, manifest writer. |
| `crates/whippet-testkit/` | Deterministic test helpers for the engine. No runtime code. |
| `crates/whippet/` | The `whippet` binary: `node`, `sim`, `bench`, `healthcheck`, `verify`, `replay`, `doctor`. |
| `apps/dashboard/` | React 19, TanStack Router, uPlot, Vite. Built to `apps/dashboard/dist`. |
| `deploy/compose.yaml` | Single-node stack: Iggy, node, dashboard. The `obs` profile adds Prometheus. |
| `schemas/bench-manifest.schema.json` | JSON Schema for the benchmark manifest. |
| `docs/` | Architecture, demo, operations, failure modes, glossary, benchmarks. |
| `docs/adr/` | Architecture decision records for the Phase 1 choices. |
| `docs/research/` | The research dossier and upstream backlog that led to this build. |

## Docs

- [ARCHITECTURE.md](docs/ARCHITECTURE.md): crates, data flow, topics, recovery.
- [DEMO.md](docs/DEMO.md): exact commands, expected output, dashboard reading,
  troubleshooting.
- [OPERATIONS.md](docs/OPERATIONS.md): configuration, retention, disk, reset,
  backup, upgrade pins.
- [RUNBOOK.md](docs/RUNBOOK.md): operator sequence — boot, disk pressure,
  verify, replay, restore, crash recovery, control plane, feed, incidents.
- [FAILURE-MODES.md](docs/FAILURE-MODES.md): what happens today when a
  component fails, and what is only planned.
- [BENCHMARKS.md](docs/BENCHMARKS.md): method, run matrix, scope labels, the
  smoke run.
- [PERFORMANCE.md](docs/PERFORMANCE.md): stage budget, measured baseline,
  ranked bottlenecks, and the before/after experiment matrix.
- [BENCHMARK-MANIFEST.md](docs/BENCHMARK-MANIFEST.md): every manifest field and
  the JSON Schema.
- [docs/benchmarks/README.md](docs/benchmarks/README.md): how to add a run
  directory without hand-editing numbers.
- [GLOSSARY.md](docs/GLOSSARY.md): exchange, log, and benchmark terms.
- [EXCHANGE-101.md](docs/EXCHANGE-101.md): the market-structure minimum needed
  to read the dashboard and the code.
- [ROADMAP.md](docs/ROADMAP.md): the phase gates in detail.
- [docs/adr/README.md](docs/adr/README.md): architecture decision records.
- [GOVERNANCE.md](GOVERNANCE.md) and [CITATION.cff](CITATION.cff): project
  process and citation metadata.
- [Research ideation](docs/research/00-ideation.md),
  [research dossier](docs/research/01-research-dossier.md), and
  [upstream backlog](docs/research/02-upstream-backlog.md): the design record.

## Roadmap

**Phase 2: depth.** Turbo mode (local match, Iggy write-behind, bounded
uncommitted window), surveillance alerts, the audit archive and replay
scrubber, PATs and scoped users, TLS, AES-256-GCM-SIV stream encryption,
consumer-group fanout and conflation, the full benchmark matrix, and
failure-mode tests with proof artifacts.

**Phase 3: HA and spectacle.** Three Iggy nodes with VSR, random-phase failover,
chaos scenarios, polished replay and latency views, and headline Linux
benchmark runs.

**Phase 4: community.** Upstream Iggy issues and PRs from the backlog,
reproduction scripts, and a public write-up. See
[ROADMAP.md](docs/ROADMAP.md).

## License and attribution

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Whippet is an independent showcase. Apache Iggy is a project of the Apache
Software Foundation. Whippet is not affiliated with, sponsored by, or endorsed
by the Apache Software Foundation. "Apache", "Iggy", and the Apache feather
logo are trademarks of the Apache Software Foundation; no logo or trademark is
used to imply endorsement.
