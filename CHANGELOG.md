# Changelog

All notable changes to Whippet are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Whippet is pre-1.0; minor releases may change internal APIs.

## [Unreleased]

### Fixed

- Snapshot terminal indices are validated on restore: out-of-range,
  non-terminal, and missing entries are rejected instead of silently skipped,
  while repeated indices for the same id are allowed. The terminal-eviction
  queue is hashed with the live ids in eviction order, so a reordered-but-valid
  queue changes the snapshot hash and `whippet verify` fails.
- The dedup queues are compacted before a snapshot, so a restored table's
  insertion walk matches the live one even if an evicted id is re-admitted
  after the tail.
- A partially filled resting order's original quantity is restored with its
  remaining quantity; a snapshot round-trip is now exact.
- `whippet verify --require-snapshot` requires a snapshot for every active
  shard, and `replay` bounds the snapshot by the log head rather than the
  commit watermark.
- Node startup rejects a snapshot ahead of its partition's log head.
- Snapshot writes are serialized across shards; concurrent fsyncs were
  contending with Iggy's WAL.
- The health monitor pings the ingress client and every engine-pool client
  concurrently and caches one flag; `/readyz` reads the cached flag with no
  journal I/O, and the lag sweep reads each shard through its per-shard pool
  client. New `WHIPPET_ENGINE_JOURNAL_PENDING_MIB` (default 256, clamped
  16–4096): the total pending-operations budget for the engine pool, split as
  `max(16 MiB, total / pool_size)` per client; the ingress client keeps its
  own 256 MiB.
- `whippet replay --require-snapshot` fails when any record was skipped or
  undecodable, when the run was not seeded, or when `snapshot_offset >= head`;
  the summary reports `snapshot_offset`, `committed_offset`, `head`, and
  `tail`. `scripts/replay.sh` fails on `skipped != 0` for either run and on
  `tail != decoded` / `head < snapshot_offset` for the seeded run.
- Oversized snapshot files decode as `TooLarge` (not `BadMagic`); decoding
  enforces a bincode size limit, rejects trailing bytes, and validates the
  top-level symbol-count prefix before allocating.
- `verify`/`verify-strict` use `WHIPPET_PARTITIONS` (default 16);
  `WHIPPET_REPLAY_PARTITION` still defaults to 1.
- TCP charges the batch before auth/shard-health, and a batch is size-checked
  before the rate-limit charge; a request without a peer identity is charged
  the full batch.

### Added

- Audited instrument halt/resume: operator-only `POST /api/admin/instruments/{symbol}/halt`
  and `/resume` (`WHIPPET_OPERATOR_TOKENS`, separate from trading tokens),
  log-ordered `CommandKind::Halt/Resume`, `Rejected/Halted` for new orders while
  cancels stay allowed, a Status MD event and `BookView.halted`, a JSON audit
  record in the `audit` topic, and replay-safe state (ADR 0015). TCP rejects
  operator commands.
- Scoped ingress tokens (`WHIPPET_API_TOKENS=token:participant,...`). Each
  token authorizes only its participant; the HTTP and TCP paths derive the
  participant id from the bound name and reject a request that claims another
  identity. `WHIPPET_METRICS_TOKEN` optionally protects `GET /metrics`; health
  and readiness stay open for probes. The legacy single token still works.
- Turbo mode (`WHIPPET_MODE=turbo`, requires the Iggy journal). Ingress
  acknowledges after `ShardEngine::process` returns with
  `guarantee: "matched_unjournaled"`; a per-shard bounded window
  (`WHIPPET_TURBO_WINDOW_COMMANDS`, default 10,000;
  `WHIPPET_TURBO_WINDOW_MS`, default 50) bounds the crash loss, and a full
  window returns `window_full` (HTTP 503, TCP `WindowFull`) instead of falling
  back. The write-behind appends orders, executions, and MD before releasing
  the window slot. On restart a snapshot is replayed to the log head before
  ingress opens; a non-empty log with no snapshot is a startup error. Metrics:
  `whippet_turbo_uncommitted_window{shard}`, `whippet_turbo_journal_lag{shard}`,
  `whippet_turbo_window_drops_total`,
  `whippet_turbo_ack_latency_microseconds`. The differential test proves
  identical executions, MD, and state hash against the durable path on the
  golden log.
- Snapshot format v4 carries the dedup terminal-eviction order as `u32`
  indices into a compacted dedup walk, so a restored table evicts at the same
  point as a continuous replay and the walk is exact even after a
  re-admission tail. The dedup capacity is no longer inflated by the
  insertion-walk length; a 33.9k-record replay tail now matches full replay.
  Versions 3 and older are rejected with `BadVersion`: there is no in-binary
  migration and no automatic fallback.
- The off-loop snapshot hash is restored with the distinct-id dedup count fix
  and pinned by a 100-engine × 200k-command stress test.
- Snapshot cadence is configurable (`WHIPPET_SNAPSHOT_MIN_INTERVAL_SECS`,
  default 30 s, 5–300 s).
- Per-IP WebSocket sub-limit (`WHIPPET_WS_PER_IP`, default off), plus
  `whippet_ws_connections` / `whippet_ws_rejected_total`.
- `verify` and `replay` accept `--symbols` (`WHIPPET_SYMBOLS`) so the snapshot
  universe matches the node's; `replay` takes `--exchange-partitions` and
  `verify` takes `--partitions` for non-default layouts.
- `GatewayValidate` and cancel stage samples; TCP batch rate cost.
- Turbo design in ADR 0013.

### Added

- Separate Iggy connections for ingress and the engine, so a slow output
  append cannot head-of-line block the gateway.
- Shared sharded per-IP rate limiter for HTTP and TCP, charged per order;
  TCP `Hello` is rate limited; WebSocket connection cap
  (`WHIPPET_MAX_WS_CONNECTIONS`, default 2,048).
- Snapshot write-time verification for the final snapshot; periodic snapshots
  fill hashes and are verified on restore.
- Per-shard stage histograms merged once per second; `EngineProcess` no longer
  takes a global lock per batch.
- Fanout harness `--parse envelope|full`, `--ingress-batch`, and a
  1,000-subscriber sweep.
- `make soak`, `make crash-test`, and a log-growth/retention operations
  section.

- Binary TCP ingress on `WHIPPET_TCP_ADDR` (default `0.0.0.0:8081`) with a
  length-prefixed fixed-layout frame, optional token handshake, and the same
  one-append-per-batch semantics as HTTP.
- Per-partition group commit for Orders ingress; every route (single order,
  batch, cancel, TCP) shares one writer per partition.
- `MdHub::apply_batch`, `latest_for`, and cached depth totals.
- `MessageMeta::routing_pairs` (symbol/seq/type/schema) for executions and MD.
- Sequenced snapshot writer off the shard task; commit watermark (256 records
  or 100 ms) with retry backoff.
- WS frame cache: one serialized frame per `(filter, tick)`, `Subscribe`, and
  `?trades=N`.
- `POST /api/orders/batch` for up to 256 orders of one symbol in a single Iggy
  append, with optional `?acks=none`.
- Optional bearer-token ingress (`WHIPPET_API_TOKEN`), participant ownership,
  per-IP rate limiting, `GET /readyz`, and `whippet doctor`.
- `make bench-sweep` / `scripts/bench-sweep.sh` for batch-size sweeps.
- `docs/PERFORMANCE.md` and ADR 0009.

### Changed

- The engine starts at offset zero when there is no snapshot (or the snapshot
  has no applied commands), instead of after the committed offset.
- The shard loop joins the executions and MD appends with the next orders poll,
  so matching and the broker round trip overlap instead of running in series.
- Hot path: cached best bid/ask indices, `FxHashMap` order/dedup maps,
  payload-only journal reads, cached header keys and topic identifiers, owned
  append, 256-message append chunking, parallel executions/MD appends, and
  coarser state/MD snapshot cadence.
- Recovery: uncertain Iggy lanes are resolved or abandoned and retried once;
  offset commits no longer re-apply a batch.
- The benchmark labels container runs `dev-loop` and records git/image
  provenance; the pacer and in-flight handling are being hardened for validity.
- The default Compose stack keeps Iggy defaults after a measured regression with
  the 1 GiB/4-shard/136 MiB-WAL profile.

## [0.1.0] - 2026-11-14

The Phase 1 durable slice: gateway -> Iggy `orders` -> deterministic matcher ->
Iggy `executions`/`md` -> market-data hub -> dashboard. Single-node only. This is
not a production release.

### Added

- Rust workspace with `whippet-core`, `whippet-engine`, `whippet-journal`,
  `whippet-marketdata`, `whippet-sim`, `whippet-bench`, `whippet-testkit`, and
  the `whippet` binary/CLI.
- Deterministic testkit with command builders, a golden command log and stable
  state hash, and reusable proptest strategies for valid commands.
- Fixed-layout little-endian protocol for new orders, cancels, execution
  reports, market-data events, and market-data snapshots, with typed headers for
  symbol, sequence, venue, type, schema, trace id, and codec.
- Deterministic price-time-priority matcher: limit, market with a collar, IOC,
  FOK, post-only, and cancel; cancel-resting self-trade prevention; integer
  ticks and quantities.
- Order book with a contiguous price ladder, occupancy bitmap, intrusive FIFO
  over a preallocated arena, and an order-id index.
- Durable business dedup table keyed by order id, snapshotted with the book and
  the input offset.
- BLAKE3 state hashing, bincode snapshots with a magic/version header,
  fsync-and-rename writes, and hash verification on restore.
- `whippet verify` to load snapshots and report shard state hashes.
- Iggy journal adapter with the three-stream/six-topic layout, low-level topic
  creation, append, poll, and manual consumer-offset commits.
- In-memory journal for tests and brokerless development
  (`WHIPPET_JOURNAL=memory`).
- Node orchestration: engine and market-data consumer tasks per active shard,
  health monitoring, retry/backoff, graceful shutdown, and snapshot flush.
- Gateway HTTP API: `POST /api/orders`, `POST /api/cancel`, `GET /api/state`,
  `GET /api/health`, `GET /api/instruments`, `GET /api/trades`, `GET /metrics`,
  and `WS /ws`.
- Prometheus text metrics on the gateway: counters, gauges, consumer lag, and
  ingress/match p50/p99 gauges derived from in-process HDR histograms.
- Market-data hub with a per-symbol book cache, 20 Hz conflated state
  publication, a 250-trade visible tape, and HDR ingress/match metrics.
- Dashboard with TanStack Router, React, uPlot latency/throughput charts, and a
  Canvas2D cumulative depth chart; Live, Bench, and About routes.
- Seeded deterministic simulator with `calm_open`, `volatile_news`,
  `flash_crash`, and `liquidity_drought` scenarios.
- Open-loop benchmark harness with absolute-deadline pacing, scheduler-slip
  reporting, HDR histograms, five repetitions, and a JSON manifest with a
  `dev-loop`/`headline` scope label.
- CLI subcommands: `node`, `sim`, `bench`, `healthcheck`, `verify`, and
  `replay`.
- `whippet replay` to rebuild a partition from the order log, either from a
  snapshot tail or an explicit offset range, with an optional expected state
  hash.
- Dockerfile and a Compose demo stack (`deploy/compose.yaml`) with pinned Iggy
  0.10.0, the node, and the simulator; only the dashboard port is published.
- Optional observability profile with Prometheus and Grafana, plus a `make`
  entrypoint for the demo, benchmark, verification, and reset workflows.
- GitHub Actions CI for Rust formatting, clippy, tests, dashboard build, and
  Compose configuration validation.
- Apache-2.0 license, NOTICE with upstream attribution, SECURITY, CONTRIBUTING,
  CODE_OF_CONDUCT, ADRs, and GitHub issue/PR templates.

### Changed

- `durable` is the default and only enabled mode. `--mode turbo` is rejected
  with an explicit "Phase 2" message.
- The Iggy Rust SDK is pinned to `=0.12.0`; the demo server is pinned to
  `apache/iggy:0.10.0`.
- The dashboard is built separately and served by the node from
  `apps/dashboard/dist`; the node serves a build-instructions page when the
  assets are missing.
- The simulator and benchmark use HTTP ingress; the low-latency TCP ingress
  path is not implemented yet.

### Not yet

- `turbo` mode: local sequencer, write-behind journal, and the bounded loss
  window. Phase 2.
- 3-node HA cluster, RF3, failover tests, and zero-loss-across-a-node-failure
  claims. Phase 3.
- Surveillance, audit hash chain, replay scrubber, and state verification UI.
- Scoped PATs, per-service identities, TLS, AES-256-GCM-SIV at rest, one-shot
  root bootstrap, and authenticated dashboard.
- TCP benchmark ingress, stage latency breakdown, bootstrap confidence
  intervals, per-subscriber fanout, encryption A/B, and failover benchmarks.
- Binary WebSocket frames, server-side conflation/drop counters, and a resync
  path.
- Production hardening of any kind. See [SECURITY.md](SECURITY.md).

[Unreleased]: https://github.com/cryptus-neoxys/whipet/commits/main
