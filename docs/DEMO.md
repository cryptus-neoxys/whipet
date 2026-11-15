# Demo

This is the manual demo. It assumes the stack is up and the dashboard is
reachable at <http://localhost:8080>.

```sh
make up
```

If you want a clean book, run `make reset && make up`, then stop the simulator:

```sh
docker compose -f deploy/compose.yaml stop sim
```

Otherwise the simulator may leave liquidity on the book, and your limit orders
can match it instead of resting.

## Check the node

```sh
curl -s http://localhost:8080/api/health | python3 -m json.tool
```

Expected:

```json
{
  "status": "ok",
  "mode": "durable",
  "iggy_connected": true,
  "iggy_addr": "iggy:8090",
  "journal": "iggy",
  "version": "0.1.0",
  "uptime_s": 12,
  "partitions": 16
}
```

`status` is `degraded` when the last Iggy ping failed. The HTTP endpoint still
answers; it does not pretend the broker is healthy.

`whippet healthcheck` does the same check and exits non-zero when the status is
not `ok`:

```sh
docker compose -f deploy/compose.yaml exec node whippet healthcheck
```

## API surface

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/orders` | Submit a new order. |
| `POST` | `/api/orders/batch` | Up to 256 same-symbol orders in one Iggy append; `?acks=none` omits the per-order array. |
| `POST` | `/api/cancel` | Cancel a resting order by id. |
| `GET` | `/readyz` | 200 only when the journal is reachable and every shard is alive. |
| `GET` | `/api/state` | Full market-data and metrics state. |
| `GET` | `/api/health` | Liveness plus Iggy connectivity. |
| `GET` | `/api/instruments` | Configured symbols, tick sizes, ladders, collars. |
| `GET` | `/api/trades?limit=N` | Newest-first trade tape, default 100. |
| `GET` | `/metrics` | Prometheus text exposition. |
| `GET` | `/ws?trades=N` | WebSocket; state frames at 20 Hz, optional trade-tape cap and `Subscribe` filtering. |

Errors use `{"code": "...", "error": "..."}`. Validation errors are HTTP 400;
an Iggy append failure is HTTP 503 with code `journal_unavailable`.

The same orders can be sent over the binary TCP listener on `8081`
(`WHIPPET_TCP_ADDR`): a length-prefixed frame carries up to 256 fixed-layout
78-byte commands of one symbol, and one ack returns the partition, base offset,
and count. When `WHIPPET_API_TOKEN` is set, the client must send a `Hello` frame
with the token before `Submit`. The benchmark drives this path with
`--transport tcp`; see [BENCHMARKS.md](BENCHMARKS.md#tcp-ingress-sweep-dev-loop-measured-headline-pending).

## Beat 1: place an order

BTC-USD has a 100-cent tick and a ladder from $40,000 to $90,000. `6500000`
cents is $65,000.00.

```sh
curl -s -X POST http://localhost:8080/api/orders \
  -H 'content-type: application/json' \
  -d '{
    "symbol": "BTC-USD",
    "side": "buy",
    "order_type": "limit",
    "price_cents": 6500000,
    "qty": 10,
    "client_order_id": "demo-buy-1",
    "participant": "demo-buyer"
  }' | python3 -m json.tool
```

Expected shape:

```json
{
  "order_id": "6f1c0c2f0d5f4f6a8f0e2d1c3b4a5968",
  "symbol": "BTC-USD",
  "partition": 1,
  "offset": 0,
  "mode": "durable",
  "status": "accepted_by_log",
  "accepted_at_ns": 1763123456789012345,
  "durable_ack_ms": 1.42
}
```

`partition` is 1 because BTC-USD hashes to partition 1 of 16. `offset` is the
per-partition record offset in `whippet-trading/orders`; it is 0 only on an
empty log. `durable_ack_ms` is the gateway's wall-clock time for validation
plus the Iggy append confirmation.

`accepted_by_log` is not a fill. The engine consumes the record after the
gateway returns.

## Beat 2: see the fill in the tape

Submit a sell at the same price. Use a different participant name:
self-trade prevention cancels the resting order instead of filling it.

```sh
curl -s -X POST http://localhost:8080/api/orders \
  -H 'content-type: application/json' \
  -d '{
    "symbol": "BTC-USD",
    "side": "sell",
    "order_type": "limit",
    "price_cents": 6500000,
    "qty": 4,
    "client_order_id": "demo-sell-1",
    "participant": "demo-seller"
  }' | python3 -m json.tool

sleep 0.2
curl -s 'http://localhost:8080/api/trades?limit=3' | python3 -m json.tool
```

Expected trade:

```json
[
  {
    "symbol": "BTC-USD",
    "price_cents": 6500000,
    "qty": 4,
    "side": "sell",
    "order_id": "<the demo-sell-1 order id>",
    "ts_ns": 1763123456790012345,
    "seq": 3
  }
]
```

The trade is at the maker's price, 6,500,000 cents. `side` is the aggressor's
side. `order_id` is the taker. The resting order keeps 6 units at the same
price.

The tape is newest first and holds the last 250 trades observed by the MD hub.
It is a view, not an audit record. The `executions` topic is the durable
record.

## Beat 3: inspect the offset

The acknowledgement from beat 1 and the acknowledgement from beat 2 are both
on partition 1. The second offset should be the first plus one, unless the
simulator or another client wrote in between.

```sh
curl -s http://localhost:8080/api/state | python3 -c '
import json, sys
s = json.load(sys.stdin)
btc = next(b for b in s["symbols"] if b["symbol"] == "BTC-USD")
print({
    "mode": s["mode"],
    "journal": s["journal"],
    "md_sequence": s["sequence"],
    "freshness_ms": s["freshness_ms"],
    "btc_seq": btc["seq"],
    "best_bid_cents": btc["best_bid_cents"],
    "best_ask_cents": btc["best_ask_cents"],
    "last_trade_price_cents": btc["last_trade_price_cents"],
})'
```

Expected shape:

```python
{
  "mode": "durable",
  "journal": "iggy",
  "md_sequence": 5,
  "freshness_ms": 37,
  "btc_seq": 5,
  "best_bid_cents": 6500000,
  "best_ask_cents": None,
  "last_trade_price_cents": 6500000,
}
```

How to read it:

- `offset` in an order acknowledgement is the Iggy offset of the input
  command. It is per partition and not guaranteed contiguous after retention
  or recovery.
- Every execution and MD record carries that input offset as `cause_offset`.
  The field is on the wire, not in the JSON state view.
- `btc_seq` is the per-symbol MD sequence, starting at 1. It counts MD events,
  not orders.
- `md_sequence` is the maximum per-symbol sequence across books. It is a
  display value, not a global order.
- `freshness_ms` is the age of the newest MD event. A value above 3,000 makes
  the dashboard show a stale banner.

To inspect the input record itself, use the Iggy CLI inside the broker
container. The exact command depends on the Iggy 0.10 CLI; the topic is
`whippet-trading/orders`, partition 1, and the offset is the one in the
acknowledgement.

## Beat 4: run a simulator scenario

```sh
docker compose -f deploy/compose.yaml exec node \
  whippet sim --scenario flash_crash --rate 400 --duration 30 --seed 7
```

Expected log lines:

```text
INFO simulator started rate=400 scenario=flash_crash seed=7 gateway=http://127.0.0.1:8080
INFO simulator progress sent=2000 errors=0 dropped=0 open_orders=731 elapsed_s=5
INFO simulator progress sent=4000 errors=0 dropped=0 open_orders=1422 elapsed_s=10
INFO simulator stopped sent=12000 errors=0 dropped=0
```

The counts depend on the scenario and the book. `dropped` is non-zero when the
simulator's `max_in_flight` (default 64) is exhausted; dropped actions are not
sent, not queued.

Scenarios:

| Name | Behavior |
|---|---|
| `calm_open` | 2% market, 4% IOC, 6% post-only, 20% cancel, 1-tick spread, random-walk mid. |
| `volatile_news` | 10% market, 8% IOC, 28% cancel, 4-tick spread, alternating drift. |
| `flash_crash` | Four phases over 4,000 actions: calm, sell-heavy crash (32% market, 82% sell bias, -4 tick drift), drought, recovery. |
| `liquidity_drought` | 42% cancel, 8-tick spread, low market-order rate. |

The simulator is seeded with ChaCha20. The same seed and scenario produce the
same action sequence. The client order id is derived from the seed and a
sequence counter, so restarting with the same seed replays the same order ids.
The engine's dedup table rejects them. Use a new `--seed` when you want a new
flow against an existing log.

The simulator sends HTTP requests to the gateway. It measures gateway
acceptance, not matching latency. It does not maintain an acknowledged ledger;
`sent` means the gateway returned 2xx, and `errors` counts non-2xx or transport
errors.

## Beat 5: produce a bench manifest

```sh
make bench
```

A short manual run, useful for a first look:

```sh
make bench-smoke
```

`make bench-smoke` runs the bench service, which mounts `./artifacts` at
`/artifacts`, stops the simulator for a quiet measurement, and restores it
afterwards.

Expected output:

```text
smoke  n=1600  p50=3200µs  p99=41000µs  p99.9=60000µs  throughput=200/s  errors=0  scope=dev-loop
manifest: artifacts/bench/<run-id>.json
```

The numbers above are illustrative. The scope is `dev-loop` on an M4 with
Docker Desktop. The manifest is written to `artifacts/bench/` on the host.

Inspect it:

```sh
python3 -m json.tool artifacts/bench/<run-id>.json | head -60
```

The CLI defaults are 1,000 orders/s, 30 seconds, 5 seconds warmup, 5
repetitions, label `L1`, output `artifacts/bench`. `make bench` may pass a
smaller profile; the command and manifest path it prints are authoritative.

A single short run is a smoke test, not a benchmark. The method, the run
matrix, and the one recorded smoke run are in [BENCHMARKS.md](BENCHMARKS.md).

## Verify snapshots

```sh
make verify
```

Raw:

```sh
docker compose -f deploy/compose.yaml exec node \
  whippet verify --snapshot-dir data/snapshots --partitions 16
```

Expected on a node that has processed at least 10,000 commands per symbol, or
that was stopped gracefully:

```text
shard  1  last_offset=<N>  hash=<64 hex chars>  [BTC-USD@<N>]
shard  5  last_offset=<N>  hash=<64 hex chars>  [ETH-USD@<N>]
shard 12  last_offset=<N>  hash=<64 hex chars>  [SOL-USD@<N>]
verified 3 shard snapshot(s); all state hashes match
```

The default interval is 10,000 commands per symbol. A short demo may have no
snapshots yet:

```text
no snapshots in data/snapshots (the node writes one every N commands)
```

Stop the node with `docker compose stop node` (SIGTERM) to force a final
snapshot. `make verify` checks the stored hash against a rebuilt engine. It
does not replay the log and it does not prove that the snapshot is the latest
possible state.

## Replay the log

`whippet replay` re-applies the order log to a fresh engine and prints a JSON
summary with the final state hash. It reads with its own consumer name and
never commits an offset, so it does not disturb the live engine or MD
consumers.

From the host, against the published Iggy port:

```sh
whippet replay --iggy-addr 127.0.0.1:8090 --partition 1 --from 0
```

Inside the Compose network:

```sh
docker compose -f deploy/compose.yaml exec node \
  whippet replay --iggy-addr iggy:8090 --partition 1 --from 0
```

Expected shape (the counts depend on the log; the hash is the checked value):

```text
{
  "partition": 1,
  "seeded_from_snapshot": false,
  "snapshot_offset": null,
  "from": 0,
  "to": null,
  "last_offset": <head offset>,
  "decoded": <number of decoded records>,
  "skipped": 0,
  "state_hash": "c2b9838ba8438245697c93b5396113cbeb38b3a136ae174842f11b1e39536739"
}
```

Now seed from the snapshot and replay only the tail:

```sh
whippet replay --iggy-addr 127.0.0.1:8090 --partition 1 \
  --snapshot-dir data/snapshots
```

Expected shape (snapshot offset 158548 is from the local check; the head offset and count depend on the log):

```text
{
  "partition": 1,
  "seeded_from_snapshot": true,
  "snapshot_offset": 158548,
  "from": 158549,
  "to": null,
  "last_offset": <head offset>,
  "decoded": <number of decoded tail records>,
  "skipped": 0,
  "state_hash": "c2b9838ba8438245697c93b5396113cbeb38b3a136ae174842f11b1e39536739"
}
```

The exact offsets and decoded counts depend on the log. The equality is the
point: replaying the whole log and replaying the snapshot plus its tail end at
the same state hash. On a local single-node run (M4, Docker Desktop,
dev-loop), the snapshot was at offset 158548 and both runs ended at the hash
above. This is one determinism check, not a crash test, not an audit proof,
and not a benchmark.

Flags:

- `--from N` starts at `N` and ignores the snapshot. If you want a snapshot
  seed, omit `--from`; the CLI does not apply both.
- `--to N` stops after `N`, inclusive.
- `--snapshot-dir` defaults to `data/snapshots`.
- `--expect-hash <hex>` exits non-zero unless the final hash matches, which is
  useful in a script.
- `--require-snapshot` fails closed: the run must be seeded from a snapshot
  that is a real resume point, no record may be skipped or undecodable, and
  `snapshot_offset` must be before the log head (`snapshot_offset >= head`
  fails). The summary reports `snapshot_offset`, `committed_offset`, `head`,
  and `tail`. The commit watermark may lag the snapshot; only the head bounds
  it. `scripts/replay.sh` runs this gate and also fails when either run skips
  records, when the seeded run's `head < snapshot_offset`, or when the seeded
  run's `tail != decoded`.
- `--iggy-user` and `--iggy-password` default to `iggy`/`iggy`. The CLI also
  reads `WHIPPET_IGGY_USER` and `WHIPPET_IGGY_PASSWORD`, the same variables as
  the node. Export the Compose credentials before running it from the host.

The replay engine uses the default instrument set. If you changed
`WHIPPET_PARTITIONS`, pass `--exchange-partitions N` so the symbol-to-partition
mapping matches the log. If you changed `WHIPPET_SYMBOLS`, pass `--symbols`
with the same list (or export it), or a valid snapshot is rejected for a
symbol the node never wrote. `verify --partitions` / `--symbols` follow the
same rule.

Order types and matching semantics are summarized in
[EXCHANGE-101.md](EXCHANGE-101.md).

## Reading the dashboard

The header has three routes: Live (`/`), Bench (`/bench`), and About
(`/about`). The connection pill shows `CONNECTING`, `LIVE`, `RECONNECTING`,
`OFFLINE`, or `STALE`. `STALE` means the last state frame is older than three
seconds while the socket is still open.

The Live page:

- **Metrics strip.** Order throughput is the gateway acceptance rate over the
  last sample window. Ingress latency is the gateway append path. Match
  latency is the synchronous engine call only. Executions, trades, and rejects
  are process-lifetime counters. MD fanout counts records applied by the hub.
  The node tile shows uptime and Iggy connectivity.
- **Symbol list.** One row per configured symbol, with best bid/ask and last
  trade.
- **Order ticket.** Limit, market, IOC, FOK, and post-only. The price is in
  dollars; the gateway receives cents. "Send 12-order burst" submits twelve
  small limit orders around the mid. The ticket reports the partition and
  offset of each accepted order.
- **Ladder.** Top 20 levels per side, best first, with cumulative depth bars.
- **Depth chart.** Cumulative bid and ask quantity.
- **Trade tape.** Newest first, filtered to the selected symbol.
- **Charts.** Ingress p50/p99, match p99, and throughput history sampled from
  WebSocket state frames at most twice per second. The history holds 240
  points.

The metrics are live and conflated. Lifetime percentiles are cumulative; use the rolling-window series (`ingress_window_p99_us`, `match_window_p99_us`) for current behavior. They
are not benchmark results and they are not windowed percentiles. `consumer
lag` is the computed per-shard sweep (committed offset to head, summed over
the active shards, refreshed every five seconds).

The Bench page describes the method and the run matrix. It also shows the live
dev-loop metrics. The About page restates the scope and the pinned versions.

The dashboard does not show a per-order status timeline or a replay scrubber.
Those are Phase 2.

## Troubleshooting

### Iggy is not ready

The node retries the initial Iggy connection up to 120 times with exponential
backoff capped at two seconds, roughly four minutes. During that time it logs
`Iggy is not ready; retrying`. Check the broker:

```sh
docker compose -f deploy/compose.yaml ps
docker compose -f deploy/compose.yaml logs --tail=100 iggy
```

If Iggy exits immediately, read the next two sections.

### io_uring or seccomp errors

Iggy 0.10 uses `io_uring` on Linux. A container with the default seccomp
profile can fail with `Operation not permitted` or an io_uring setup error.
The Compose service must set:

```yaml
security_opt:
  - seccomp=unconfined
```

It may also need a raised memlock limit. This is a Compose responsibility; the
application cannot grant it at runtime.

### Iggy authentication fails

The node defaults to `iggy`/`iggy` for local bare-metal development. The demo
Compose stack is expected to create a non-default root account and pass
`WHIPPET_IGGY_USER` and `WHIPPET_IGGY_PASSWORD`. If the node logs an
authentication error, the two sides disagree. Do not "fix" it by enabling
default root credentials.

### Orders return 401 or 403

If `WHIPPET_API_TOKEN` is set, every order/cancel request needs
`Authorization: Bearer <token>`. A body `participant` that differs from
`WHIPPET_API_PARTICIPANT` returns 403. The dashboard has no token input yet;
use curl for authenticated demos.

### Orders return 503

```json
{"code": "journal_unavailable", "error": "iggy error: ..."}
```

The gateway could not append to Iggy. The order was not accepted. Check
`/api/health` and the broker logs. There is no local retry queue in the
gateway; the client must retry.

### Orders rest but never fill

Check the ladder and the tick size:

```sh
curl -s http://localhost:8080/api/instruments | python3 -m json.tool
```

A BTC-USD price must be a multiple of 100 cents and inside $40,000-$90,000.
A buy below the best ask rests; a sell above the best bid rests. Market orders
use a 500 bps collar around the last trade, the mid, or the reference price,
and can be cancelled if the book is too far away. Post-only orders are
rejected if they would cross. FOK orders are rejected if the full quantity is
not executable.

### Dashboard assets are missing

The API serves a plain page that says "Whippet API is running" when
`apps/dashboard/dist/index.html` does not exist. Build the assets:

```sh
cd apps/dashboard
pnpm install
pnpm build
```

Then restart the node, or point `WHIPPET_DASHBOARD_DIR` at the built directory.
The Compose build is expected to do this for you.

### Port 8080 or 8090 is in use

```sh
lsof -nP -iTCP:8080 -sTCP:LISTEN
lsof -nP -iTCP:8090 -sTCP:LISTEN
```

Stop the conflicting process or change the published ports in
`deploy/compose.yaml`. The node still binds `0.0.0.0:8080` inside the
container; `WHIPPET_HTTP_ADDR` changes the in-container address, not the host
mapping.

### `whippet verify` finds no snapshots

Snapshots are written every 10,000 commands per symbol and on graceful
shutdown. Either run more traffic or stop the node with SIGTERM:

```sh
docker compose -f deploy/compose.yaml stop node
docker compose -f deploy/compose.yaml start node
```

Do not delete a snapshot while the engine consumer has a later committed
offset unless you intend to rebuild. See
[OPERATIONS.md](OPERATIONS.md#backup-and-restore).

### Simulator orders are rejected as duplicates

The simulator derives order ids from the seed and a sequence counter.
Restarting with the same seed regenerates the same ids, and the engine rejects
them with `duplicate_order_id`. Use a new `--seed`, or reset the log.

### Bench p99 is dominated by scheduler slip

The manifest has a `scheduler_slip` summary. On Docker Desktop the generator
can be late by milliseconds, which shows up in the latency tail because
latency is measured from the scheduled deadline. This is a dev-loop
measurement. Do not present it as a broker or matcher result.

### Dashboard says stale

`freshness_ms` is the age of the newest MD event. Above 3,000 ms the Live page
shows a stale banner and the connection pill shows `STALE`. Check the node
logs, the MD consumer, and Iggy. The dashboard does not keep a fake "live"
state.
