# Changelog

All notable changes to Whippet are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Whippet is pre-1.0; minor releases may change internal APIs.

## [Unreleased]

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

[Unreleased]: https://github.com/cryptus-neoxys/whippet/commits/main
