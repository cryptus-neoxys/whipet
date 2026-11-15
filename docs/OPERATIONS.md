# Operations

Single-node, single-broker operations for the Phase 1 durable slice. There is
no HA story in this build; the single-node caveats are part of the contract,
not a footnote.

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
| `WHIPPET_API_TOKEN` | unset | Optional bearer token for `/api/orders`, `/api/orders/batch`, and `/api/cancel`. |
| `WHIPPET_API_PARTICIPANT` | `api` | Participant identity used when a bearer token is configured. |
| `WHIPPET_RATE_LIMIT_PER_SEC` | `0` | Optional per-IP request limit; `0` disables. |
| `WHIPPET_JOURNAL` | `iggy` | `iggy` for the demo, `memory` for tests and brokerless development. |
| `WHIPPET_MODE` | `durable` | Only `durable` is enabled. `turbo` is rejected at startup with a Phase 2 message. |
| `WHIPPET_SYMBOLS` | `BTC-USD,ETH-USD,SOL-USD` | Comma-separated subset of the built-in instruments. Unknown symbols are rejected. |
| `WHIPPET_PARTITIONS` | `16` | Trading partitions, 1-256. This is a create-time contract; see below. |
| `WHIPPET_SNAPSHOT_DIR` | `data/snapshots` | Directory for `partition-<N>.snapshot` files. |
| `WHIPPET_SNAPSHOT_EVERY` | `10000` | Commands per symbol between state snapshots. The node also enforces a 15-second minimum interval, so a high-throughput run snapshots at most once per 15 s. Values below 1 are clamped to 1. |
| `WHIPPET_HTTP_TRACE` | unset | Set to `1` to enable per-request `tower-http` trace spans; off by default for throughput. |

Concurrent single-order submissions on one partition are coalesced into one
Iggy append by a per-partition group-commit writer (up to 256 messages). The
writer does not add a fixed linger; it drains what is queued when it wakes. The
effect is visible in `/metrics` as `whippet_group_commit_batches_total` versus
`whippet_group_commit_messages_total`.

`POST /api/orders/batch` accepts up to 256 orders of one symbol and performs one
Iggy append. Mixed-symbol batches are rejected with `mixed_symbols`; an invalid
order rejects the whole batch. `?acks=none` returns only the batch summary
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

## Topic creation

On startup the node ensures three streams and six topics exist:

| Stream | Topic | Partitions | Durability | Retention | Max size |
|---|---|---|---|---|---|
| `whippet-trading` | `orders` | `WHIPPET_PARTITIONS` (16) | persisted | 24 h | 8 GiB |
| `whippet-trading` | `executions` | 16 | persisted | 7 d | 8 GiB |
| `whippet-md` | `md` | 16 | replicated | 1 h | 8 GiB |
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

## Iggy tuning

The default stack keeps Iggy's 0.10 defaults. We tested a smaller profile
(`IGGY_MEMORY_POOL_SIZE=1GiB`, four shards, no core pinning,
`partition_io_capacity=32`, 136 MiB WAL) and it regressed durable-ingress
latency on a 16 GB Docker Desktop VM, especially after a broker restart. The
defaults are the measured-better configuration here. `deploy/iggy/iggy.toml`
contains the alternative as a documented example for a memory-constrained
3-node VM; do not treat it as a benchmark-tuned profile without measuring it on
the target host.

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

`GET /readyz` returns 200 only when the journal is reachable and every shard
task is alive; otherwise 503. Use it for container readiness. `GET /api/health`
returns HTTP 200 with `status: "ok"` or `"degraded"`. It is
`ok` only when the journal is reachable (or the in-memory journal is selected)
and every shard task is alive; `shards_ok` reports the latter. A corrupt
snapshot or a shard task panic therefore degrades health instead of leaving a
silent "up but not matching" node. The
node pings Iggy at startup and every five seconds. `degraded` means the last
ping failed; it does not mean the process is down.

```sh
docker compose -f deploy/compose.yaml exec node whippet healthcheck
```

`whippet healthcheck` prints the JSON and exits non-zero unless the status is
`ok`. A container healthcheck should use it.

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
| `whippet_consumer_lag` | gauge | Placeholder, set to 0 after each MD batch. Not a computed lag. |
| `whippet_ingress_latency_microseconds{quantile="0.5"}` | gauge | Gateway append path p50, cumulative histogram. |
| `whippet_ingress_latency_microseconds{quantile="0.99"}` | gauge | Gateway append path p99, cumulative histogram. |
| `whippet_match_latency_microseconds{quantile="0.5"}` | gauge | Synchronous engine call p50, cumulative histogram. |
| `whippet_match_latency_microseconds{quantile="0.99"}` | gauge | Synchronous engine call p99, cumulative histogram. |
| `whippet_throughput_orders_per_second` | gauge | Gateway acceptance rate over the last sample window. |
| `whippet_uptime_seconds` | counter | Process uptime. Monotonic until restart. |
| `whippet_iggy_connected` | gauge | 1 if the last Iggy ping succeeded, else 0. |
| `whippet_partitions` | gauge | Configured trading partitions. |
| `whippet_mode{mode="durable"}` | gauge | One-hot operating mode. |
| `whippet_md_freshness_milliseconds` | gauge | Age of the newest MD event. |
| `whippet_md_sequence` | gauge | Maximum per-symbol MD sequence observed. |

Caveats:

- The latency quantiles come from process-lifetime HDR histograms. They are
  cumulative, not windowed, and they are not benchmark results.
- Counters reset when the node restarts. Prometheus handles the reset, but a
  rate over a restart boundary is meaningless.
- `whippet_consumer_lag` is not wired to a real lag calculation yet. Do not
  alert on it.
- `/metrics` has no authentication in Phase 1. Publish it only on the Compose
  network, or put it behind a reverse proxy. Do not expose it to the internet.
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

Restore:

1. Stop the node.
2. Put the `partition-<N>.snapshot` files back in `WHIPPET_SNAPSHOT_DIR`.
3. Start the node. Each shard verifies its hash before using the snapshot. A
   mismatch aborts that shard with an error; the node does not silently use a
   corrupt file.

To rebuild a shard from scratch, you must remove both the snapshot and the
stale committed offset, or recreate the topic. Deleting only the snapshot
while the engine consumer has a later committed offset starts the engine after
that offset, with no state before it. That is a partial rebuild, not a full
one. The safe reset is `make reset`.

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

Rollback: pin both artifacts back. Snapshots use format version 1 with a
`WHIPSNP1` magic and bincode. A snapshot-format change is a data migration, not
a rolling upgrade; reset the data if the format is not compatible.

The phase gates for larger changes are in [ROADMAP.md](ROADMAP.md).
