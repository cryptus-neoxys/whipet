# Roadmap

The roadmap is deliberately phase-gated. A phase ships when its demo works and its claims are
backed by artifacts.

## Phase 0 — foundation (done)

- Cargo workspace, pinned Rust 1.95, Apache-2.0 license and NOTICE.
- Protocol types, fixed-layout codecs, error model.
- Deterministic matcher with ladder + bitmap + intrusive FIFO, dedup table, snapshots, state hash.
- Iggy journal adapter with pinned server 0.10.0 and SDK 0.12.0.
- ADRs, upstream friction log, benchmark manifest schema, CI skeleton.

## Phase 1 — durable vertical slice (done)

- Gateway: HTTP order/cancel ingress, validation, idempotency key derivation, WS state feed.
- Engine: one single-writer task per active partition; snapshot + tail replay; manual commits.
- Iggy topics: `orders` and `executions` persisted; `md` replicated; ops topics created.
- Market data: snapshot/delta application, conflated 20 Hz state frames, trade tape.
- Dashboard: Live, Bench, About; ladder, depth canvas, latency and throughput charts.
- Simulator: seeded calm/volatile/flash-crash/drought scenarios.
- Bench: open-loop pacer, coordinated-omission correction, HDR histograms, 5-rep manifest.
- One-command Docker demo; Prometheus endpoint; replay/verify CLI.

## Phase 1.5 — hardening and measured optimization (in progress)

- Batch ingress, optional bearer auth, per-IP rate limits, readiness, `doctor`.
- Journal fast path: payload-only reads, cached header keys/identifiers, owned
  append, 256-message chunking, uncertain-lane recovery.
- Engine hot path: cached best bid/ask, `FxHashMap`, snapshot cadence, dedup
  eviction, snapshot/hash cost.
- MD warm path: latest-frame reads, batch apply, smaller frames.
- Bench validity: batch-size sweep, cancel/IOC mix, pacer/slip, provenance.
- Linux headline benchmark and the full L1/L2/F1/F2/R1/R2 matrix.

## Phase 2 — depth

- `turbo` mode: local sequencer, direct fanout, Iggy write-behind, visible uncommitted window.
- Consumer-group fan-out for many MD subscribers; per-subscriber lag; slow-client resync.
- Surveillance: spoofing/layering/ratio alerts, idempotent alert consumption.
- Audit: hash-chained archive, replay API, state-hash verification report.
- Replay scrubber: 0.1×–100×, LIVE/REPLAY/FOLLOW state machine, gap recovery.
- Security: scoped PATs, one stream per trust domain, TLS, AES-256-GCM-SIV at rest, one-shot root
  bootstrap, no root credentials in app containers.
- Full benchmark suite: L1/L2, F1/F2, R1/R2, S1; raw artifacts and Linux reproduction.
- Failure-mode tests: crash between publish and commit, duplicate replay, slow consumer, Iggy down.

## Phase 3 — spectacle

- 3-node Iggy VSR cluster, RF3, persisted durability, random-phase failover tests.
- Chaos profile: pause/kill primary, network delay, disk pressure, 10× burst.
- Polished UI: depth heatmap, replay scrubber, ops/audit pages, failover strip plot.
- Demo video/GIF, launch post, reproducible Linux headline run.

## Phase 4 — community

- Upstream issues/PRs from `docs/UPSTREAM.md`.
- Powered By submission, Discord/mailing-list post, blog proposal, lightning talk.
- Benchmark uploads and a public methodology page.

## Non-goals

Exactly-once delivery, zero RTO, contiguous offsets, cross-partition transactions, production
cluster maturity, regulatory compliance, HSM/KMS, and sub-millisecond durable end-to-end latency.
