# Whippet — Research Dossier & Architecture Blueprint

Date: 2026-11-14
Status: research complete; architecture proposed; three decisions pending (UI stack, default mode, cluster-in-v1)
Repo: `~/Documents/repos/github.com/cryptus-neoxys/whippet`

---

## 0. Executive summary — the ten findings that change the design

1. **Sub-millisecond durable end-to-end is not real.** Iggy 0.10's own published numbers on an i4i.4xlarge are p50 0.69 ms / p99 1.06 ms for `replicated`, and p50 1.99 ms / p99 2.47 ms for `persisted`; a 3-node cluster is p50 0.95 / p99 1.28 ms replicated and p50 1.71 / p99 2.19 ms persisted. On an M4 Docker VM, expect a fraction of the throughput and single-digit-to-tens-of-ms tails. Whippet must split the claim: **sub-millisecond matching, low-single-digit-millisecond durable path on Linux, and dev-loop-only M4 numbers.**
2. **The strongest showcase is the latency/durability trade-off itself.** Run two explicit modes: `durable` (Iggy inline, quorum ack, replayable, honest low-ms) and `turbo` (local sequencer + matcher, Iggy write-behind, sub-ms matching, bounded loss window). A dashboard toggle makes the trade-off visible. That is more credible and more interesting than pretending one path does everything.
3. **Iggy is a partitioned log, not a queue.** Ordering is per partition. Partition by symbol, fix the partition count (12–24), and never treat offsets as contiguous—they can skip after retention/recovery.
4. **`replicated` and `persisted` are different guarantees, and consumer-offset durability is independent.** For orders, executions, and audit, set both `durability=persisted` and `consumer_offset_durability=persisted`, use manual commits after processing, and snapshot matcher state + dedup table atomically with the input offset.
5. **Idempotent producers are batch-level, not business dedup.** Producer identity/sequences live in memory, cover the last five batches per partition, 4,096 producers, 24 hours. Whippet needs its own durable `(symbol, client_order_id)` dedup table.
6. **Single-writer per symbol, array price ladder + occupancy bitmap + intrusive FIFO.** Integer ticks, no allocation/syscalls/locks in the match loop, deterministic state machine over the input log. This is the exchange-core/LMAX pattern and the right one.
7. **Sync thread-per-core matcher; async only at the Iggy edge.** The matcher is a pinned, busy-polling thread fed by a bounded SPSC ring; a separate journal thread owns the async Iggy client. No Tokio, tracing, or `await` in the hot path.
8. **Benchmark like a latency shop, not a blog.** Open-loop pacing, coordinated-omission correction, TSC timestamps with stage breakdown, HDR histograms, 5 repetitions, manifest, per-subscriber fanout, random-phase failover kills. M4/Docker results are regression data, never headline claims.
9. **Security claims need precision.** Iggy stream encryption is AES-256-GCM-SIV, not plain GCM; it is at-rest only. There is no client mTLS (issue #3930). `read_topics` also grants consumer-group admin. Use one stream per trust domain, one PAT per service, a one-shot root bootstrap, and TLS on every transport.
10. **The research surfaced a dozen upstream gaps — this is the merit engine.** Inert compression option, missing high-level topic knobs, TLS validation defaults, compose version drift, no latency histograms, HTTP second-class support, and more. Each becomes an issue/PR; the showcase becomes a sustained contribution pipeline, not a one-off.

---

## 1. The claim we can honestly make

| Claim | Scope | Status |
|---|---|---|
| Sub-millisecond **matching** | Matcher dequeue → execution events, in-process, pinned core | Target to validate (p99 < 1 ms; aim < 100 µs) |
| Sub-millisecond **turbo end-to-end** | Gateway ingress → client execution/MD, Iggy write-behind | Target on Linux; M4/Docker will be slower and labeled |
| Low-single-digit-ms **durable end-to-end** | Gateway ingress → Iggy quorum ack → execution, `persisted` | Realistic on Linux; publish measured p50/p99/p99.9 |
| Zero acknowledged loss across **one node failure** | 3 nodes, RF3, both policies `persisted`, idempotent producer, manual commits | Testable and claimable, with proof artifacts |
| **Not** claimed | Exactly-once delivery, zero RTO, contiguous offsets, cross-partition transactions, power-loss proof on macOS, production-grade cluster maturity | Explicit non-goals in README |

The README headline should read like: **"Sub-millisecond matching. Millisecond-durable streaming. Every number reproducible."** Not "sub-millisecond exchange on Iggy."

---

## 2. Architecture

### 2.1 The core idea

A symbol-sharded deterministic matching engine with Iggy as the durable log, replay source, and fanout backbone. The system has two explicitly labeled operating modes so the latency/durability frontier is a feature, not a hidden compromise.

```
Clients (WS / FIX-ish)
        │
        ▼
┌─────────────────────────────┐
│ Gateway (async, multi-conn) │  auth, session seq, validation, rate limit
└──────────────┬──────────────┘
               │ route by symbol
               ▼
┌─────────────────────────────┐
│ Symbol shard (pinned core)  │  single writer per book
│  sequencer → matcher        │  array ladder + bitmap + FIFO
└───────┬─────────────┬───────┘
        │             │
        │             ├──────────────► direct MD ring ──► MD fanout ──► WS/SSE
        │             │
        ▼             ▼
   Iggy orders   Iggy executions / md / alerts / audit
        │             │
        ▼             ▼
   replay/audit   surveillance, dashboard, archiver
```

### 2.2 Two modes, one log

**`durable` (default demo, integrity story).**
Gateway publishes orders to the `orders` topic (partitioned by symbol); the matcher consumes the partition in order; executions and market data are published to Iggy; downstream consumers fan out, alert, and archive. The gateway acknowledges after the Iggy quorum commit (or after observing the execution, depending on the client contract). This is the purest "Iggy is the backbone" mode. It is replayable from the log alone and supports the zero-loss failover demo.

**`turbo` (latency story).**
Gateway hands orders to the symbol shard through a bounded local SPSC ring; the matcher decides immediately; executions and market data go to the live fanout ring and are journaled to Iggy asynchronously. The gateway acknowledges after the match. The log still receives every order and execution; replay and audit still work. The documented trade-off: if the node dies before the journal catches up, the in-flight window can be lost. The dashboard shows the window size and the mode badge.

**Why both.** A showcase that only runs Iggy inline cannot honestly claim sub-millisecond end-to-end. A showcase that only runs write-behind cannot claim Iggy is the synchronous backbone. Running both, with a visible toggle and a guarantee matrix, is the most honest and the most technically interesting version.

### 2.3 Shard model

- One OS thread per symbol shard, pinned to a core; symbols hashed onto shards.
- Each shard owns: sequencer, order book, dedup table, snapshot state, and a bounded output ring.
- Partition count is fixed (16 default; 8 on the M4 profile, 24 for the HA profile). One partition per symbol is tempting but creates metadata, WAL, and VSR-group overhead; hash symbols onto partitions instead.
- Cross-symbol atomicity, portfolio risk, and clearing are out of scope. Say so.

### 2.4 Components

| Component | Responsibility | Hot path? |
|---|---|---|
| `gateway` | Client sessions, auth, order validation, rate limits, idempotency, routing | Yes (ingress) |
| `shard` / `engine` | Sequencer, deterministic matcher, book, dedup, snapshots | Yes (core) |
| `journal` | Async Iggy producer/consumer, batching, retries, offset commits | Edge |
| `md` | Market-data snapshot/delta cache, conflation, WS/SSE fanout, replay | Warm |
| `surveillance` | Post-trade spoofing/layering/ratio alerts | Warm |
| `audit` | Hash-chained archival, replay, state verification | Cold/warm |
| `sim` | Seeded order-flow generator and scenario player | Benchmark |
| `dashboard` | Live/replay/ops/audit/bench UI | Cold |
| `bench` | Open-loop harness, HDR histograms, manifests | Benchmark |
| `cli` | `whippet demo`, `bench`, `replay`, `verify`, `chaos` | Control |

### 2.5 Data flow and topics

Use one stream per trust domain (Iggy `read_topics` also grants consumer-group admin, so blast-radius separation matters).

| Stream | Topic | Partitions | Key | Durability | Retention | Producers | Consumers |
|---|---|---|---|---|---|---|---|
| `whippet-trading` | `orders` | 16 | symbol | persisted | 24 h | gateway | matcher, audit |
| `whippet-trading` | `executions` | 16 | symbol | persisted | 7 d | matcher | gateway, md, surveillance, audit |
| `whippet-md` | `md` | 16 | symbol | replicated | 1 h | matcher/md | md fanout, dashboard |
| `whippet-ops` | `alerts` | 4 | symbol | persisted | 30 d | surveillance | dashboard, audit |
| `whippet-ops` | `audit` | 4 | service | persisted | 90 d | all | audit verifier |
| `whippet-ops` | `commands` | 1 | admin | persisted | 7 d | CLI/dashboard | node, simulator |

Headers on every message: `symbol`, `seq`, `venue`, `type`, `schema`, `trace_id`, `codec`. Payload is fixed-layout little-endian POD; headers stay small.

### 2.6 Recovery model

- Snapshot every ~100k commands (or 30 s) to disk/S3: book state, dedup table, last input offset, state hash.
- On restart: load snapshot, replay the tail from the recorded offset, verify the state hash.
- Order IDs in output events are `(symbol, input_offset, event_index)` so replay is deterministic.
- Publishing outputs and committing the consumer offset are not atomic; the pipeline is at-least-once and downstream dedups. State and dedup table are snapshotted together with the offset.

---

## 3. Matching engine

### 3.1 Book structure

- **Price ladder**: contiguous array indexed by tick; `best_bid`/`best_ask` found with `leading_zeros`/`trailing_zeros` over an occupancy bitmap.
- **Per-level FIFO**: intrusive doubly linked list of order indices into a preallocated order arena (u32 indices, no `Box`/`Arc`).
- **Order index**: hash map from `order_id` to arena slot, open-addressed and preallocated; no allocation in the loop.
- **Fallback**: `BTreeMap` for sparse/wide tick ranges; choose per symbol at creation.

### 3.2 Semantics

Implement: limit (Day/GTC), market with collar, IOC, FOK, cancel, cancel/replace (new ID, loses priority), post-only, one self-trade-prevention policy (cancel resting), opening/closing call auction with max-executable-volume and tie-breaks, halt/resume. Time events are commands injected through the log—never read the wall clock inside the engine.

Cut: stop/stop-limit, iceberg, pegged, trailing, multi-leg/spreads, pro-rata, RFQ, options, margin, clearing/settlement, fee tiers.

### 3.3 Determinism rules

- Integer ticks and quantities; no floats in event generation or matching.
- No `HashMap` iteration in decision paths; no wall-clock reads; no random IDs inside the engine.
- Canonical event order: `(timestamp, symbol, seq)`; single writer per book.
- State hash after every snapshot; replay-hash CI compares output hashes against a golden run.
- Property tests: no crossed book, price-time priority, conservation of quantity, cancel/replace invariants, idempotent duplicate orders, auction invariants.

### 3.4 Market data

- Canonical MBO per symbol: `OrderAdd`, `OrderCancel`, `OrderReplace`, `Trade`, `BBO`, `Status`, `Snapshot`, `Heartbeat`.
- `md_seq` is a monotonic per-symbol sequence; `cause_seq` is the input offset. Clients detect gaps and request a snapshot.
- Snapshots every ~1 s or 1k events; deltas at full rate; per-client conflation is explicit (`conflated=true`, `dropped_events=n`) and never applied to audit.
- Separate trades from book deltas.

---

## 4. Apache Iggy playbook

### 4.1 Version pin

| Artifact | Version |
|---|---|
| Server | `apache/iggy:0.10.0` (pin digest) |
| Rust SDK | `iggy = 0.12.0` |
| Wire protocol | 0.12.2 (server and SDK must match; mixed versions are refused) |
| Cluster compose | Do not inherit the repo's `0.9.0` default; pin explicitly |

### 4.2 Producer/consumer baseline

- **Ingress**: TCP with `with_no_delay()`; one producer per symbol partition (`partition_id` or `messages_key`); `DirectConfig { batch_length: 64–256, linger: 0 }` for latency, or background `{ num_shards: 1, OrderedSharding, linger: 100–500 µs, batch_size: 1–4 MiB, max_in_flight: 4 }` for throughput.
- **Consumers**: consumer groups with `PollOptions { count: 1000, max_wait: 1 s, commit: Manual }` and manual commits after processing; `prefetch_bytes` 64 MiB for fanout. Use `PartitionReader`/grouped fetch for multi-symbol reads.
- **Replay**: `PollingStrategy::{offset, timestamp, first, last, next}`; isolated replay consumer with `allow_replay()` and manual commits so live group offsets are untouched.
- **Headers**: symbol, sequence, type, schema, trace, codec. Payload is raw bytes; compression is app-side (lz4) with a codec header because server-side compression is inert.

### 4.3 Server configuration that matters

| Area | Recommendation |
|---|---|
| Sharding | 4 shards single-node M4 profile; 2 per node in HA; `pin_cores=true` on bare metal; `partition_io_capacity=32` |
| Memory pool | 1 GiB per node on 16 GB; do not leave the 4 GiB default in a 3-node Compose |
| Partitions | `wal_bytes_max=136 MiB`; `prepare_queue_depth=32`; `validate_checksum=true` for audit; measure its cost |
| Polls | `deferred_poll_max_pending=4096`; per-session 256; 4 MiB read / 32 MiB inflight |
| Topic | `segment_size=64 MiB`; `max_topic_size=8 GiB`; expiry per table above |
| Transports | TCP for hot path; HTTP only for metrics/admin; WebSocket for browser; QUIC optional, not default |
| Cluster | RF3, identical TOML, static IPs (not service names), PSK ≥32 B, `heartbeat_timeout=2s` for demos |

### 4.4 Known gaps (upstream backlog)

1. `compression_algorithm` is stored and reported but never applied.
2. High-level producer topic creation cannot set segment size, offset durability, replication factor, or flush thresholds.
3. `TopicCreateOptions` is a 12-field struct with no builder.
4. QUIC/WebSocket TLS validation defaults to `false`; TCP defaults to `true`.
5. `docker-compose.cluster.yml` defaults to `0.9.0` while the protocol requires 0.12.2.
6. No consumer-group offset semantics example/test across restart.
7. No runnable idempotent-producer restart/reconnect example.
8. `/metrics` has no producer-send or poll-latency histograms.
9. HTTP transport lacks consumer groups, fetch/`PartitionReader`, and `resolve_operation`.
10. `DirectConfig::linger_time` is an inter-call minimum gap, not Kafka linger.
11. Empty payloads are rejected, so tombstones need a dummy byte.
12. MCP HTTP transport has no client authentication.

Each is a candidate issue/PR. Prioritize docs, examples, tests, and small SDK ergonomics first; avoid large protocol changes while new.

---

## 5. Benchmark protocol

### 5.1 Claims and scopes

- **Match latency**: matcher dequeue → execution events; p50/p99/p99.9; target p99 < 1 ms, aim < 100 µs.
- **Turbo end-to-end**: gateway ingress → client execution/MD; target p99 < 1 ms on Linux.
- **Durable end-to-end**: gateway ingress → Iggy quorum ack → execution; publish measured p50/p99/p99.9; no sub-ms claim.
- **Throughput**: orders/s and MB/s at the SLO knee; fanout messages/s; replay messages/s.
- **Recovery**: unavailability window, acked-loss count, duplicate count, time-to-lag-zero.

### 5.2 Timestamping

Benchmark-only header, feature-gated off by default. Stamps: T0 generator scheduled/sent, T1 gateway ingress, T2 matcher decision, T3 MD enqueue, T3b socket write, T4 subscriber receive. Derive `L_e2e`, `L_gw`, `L_md`, `L_wire`, `L_queue`, `sched_slip`. Use `quanta` TSC with periodic recalibration to `CLOCK_MONOTONIC_RAW`; no `clock_gettime` per message. Single host/VM needs no PTP; cross-node absolutes require PTP or are refused. Docker Desktop has no PHC—say so.

### 5.3 Open-loop generation

Absolute-deadline pacer: `t_i = t0 + i × period`; coarse sleep then 20–50 µs spin; record `t_done − t_i`, not `t_done − t_send`; backfill skipped samples with `record_correct`. One pinned thread per generator; bounded in-flight; never block on responses. `sched_slip` is a first-class metric; a late generator invalidates the run.

### 5.4 Metrics and presentation

Publish sample count, 95% bootstrap CI, p50/p99/p99.9/p99.99/max, stage breakdown, SLO knee, goodput, fanout amplification, replay throughput, failover distribution, consumer lag. Never headline means or averaged percentiles; never p99.99 from <1M samples; never compare different batching/durability/encryption settings without labeling.

### 5.5 Run matrix

| ID | Workload | Vary | Duration |
|---|---|---|---|
| L1 | 64 B orders, 1 symbol | 1k/5k/10k/25k/50k/100k/s | 5 min ×5 |
| L2 | 256 B, 10 symbols | 10k/50k/s | 5 min ×5 |
| F1 | 10k orders/s | 1/10/100/1000 subscribers | 5 min ×5 |
| F2 | Fixed 100k egress/s | 10/100 subscribers | 5 min ×5 |
| R1 | 10M replay | 0/mid/tail; 1/10/100 consumers | drain ×5 |
| R2 | Replay + live 10k/s | 1 config | 5 min ×5 |
| C1 | 10k/s, 3-node | 20 random kill phases | 30 s each |
| S1 | 80% SLO knee soak | 1 config | 30 min ×3 |

### 5.6 Reproducibility

Warmup 60 s; verify steady state (10 s-window p99 drift <5%); 5 repetitions; publish all runs with median headline and min/max spread. Manifest: commit SHA, image digest, `lscpu`, kernel, Docker/VM, governor, SMT, isolation flags, core map, payload, batch, transport, durability, seed. `make bench` emits raw `.hdr` + JSON + parser. Linux headline; M4/Docker labeled `dev-loop`.

---

## 6. Dashboard and observability

### 6.1 Stack decision

**Recommended:** Rust backend (Axum) + **Leptos CSR/WASM** dashboard for the pure-Rust constraint, with custom Canvas2D renderers for depth, ladder, and latency heatmap. Serve the WASM bundle from Axum; one binary in release.

**Pragmatic alternative:** Vite + TypeScript + uPlot + Lightweight Charts + custom Canvas2D. Faster to build, better chart ecosystem, 100–150 KB gzip, but breaks literal "pure Rust."

The core services stay 100% Rust either way. Decision pending.

### 6.2 Pages

- **Live**: order book depth/ladder, trades tape, latency percentiles, throughput, consumer lag, mode badge (`durable`/`turbo`), freshness.
- **Replay**: timeline scrubber, 0.1×–100×, `LIVE`/`REPLAY`/`FOLLOW` state machine, offset/time, behind-live-by, sequence check, handoff to live.
- **Ops**: Iggy cluster health, partitions, replicas, WAL, consumer groups, queue depth, frame drops.
- **Audit**: hash chain, state hash, verify button, replay report.
- **Bench**: run history, latency-vs-rate curve with knee, stage breakdown, fanout table, failover strip plot, raw artifact links.

### 6.3 Rendering discipline

- Conflate state, never events: 10–30 Hz for book/DOM, 4–10 Hz watchlist, 1 Hz hidden tabs; trades/fills/audit lossless.
- One multiplexed WebSocket per tab; SSE fallback for read-only public feed; binary frames; heartbeat, jittered backoff, resubscribe, sequence-gap recovery.
- Write outside the framework render loop; mark dirty symbols; flush once per `requestAnimationFrame`; virtualize the ladder; CSS transforms for depth bars.
- Backpressure: `watch` for latest snapshots, bounded `mpsc` for events; on overflow close with "lagged" and resubscribe from last sequence; export `ws_queue_depth`, `conflated_total`, `dropped_events_total`, `resync_total`.

### 6.4 Instrumentation

- Per-stage HDR histograms (ingress, match, publish, fanout, encode), counters (orders, trades, drops, reconnects, resyncs), gauges (book depth, queue depth, consumer lag).
- Hot path: per-core atomics + sharded histograms; snapshot to Prometheus every second; no per-order spans, no per-message logs, no high-cardinality labels.
- Iggy `/metrics` requires a bearer PAT; alert on `frame_drops_total{variant="forward_client_send"}`, `partitions_below_write_quorum`, `partitions_under_replicated`, disk reserve.
- On-demand `pprof` flamegraphs; `cargo flamegraph`; `tokio-console` dev only; `criterion`/`iai-callgrind` in CI.
- Credits, freshness timestamps, sequence numbers, drop counts, and methodology links on the dashboard. No gauges, 3D, rainbow heatmaps, or smoothed ticks.

---

## 7. Reliability, durability, chaos

### 7.1 Guarantee statement

**Scope:** 3 Iggy nodes, RF3, `durability=persisted`, `consumer_offset_durability=persisted`, idempotent producer, manual commits, one symbol per partition.

Whippet may claim: per-partition order; no acknowledged order lost across one node failure; producer retry dedup within Iggy's retained window; at-least-once consumer delivery; effectively-once matcher state via durable `(symbol, client_order_id)` dedup; per-symbol ordered market data with sequence/checksum; replay reproduces the state hash.

Whippet must not claim: exactly-once delivery; zero loss for unacknowledged in-flight requests; zero RTO; survival of two simultaneous failures; cross-partition transactions; linearizable reads; contiguous offsets; production-grade cluster maturity.

### 7.2 Failure-mode matrix

| Component | Failure | Expected | Proof |
|---|---|---|---|
| Gateway | SIGKILL/restart | TCP reset, re-register, no duplicate effect | Kill -9 under load; one effect per `client_order_id` |
| Gateway | Primary failover | Retryable errors, idempotent retry returns original offset | Kill actual primary; acked offsets readable |
| Matcher | Crash after apply, before commit | Restart at snapshot+offset; duplicates deduped; hash converges | Kill between handler and commit; compare hash |
| Matcher | Duplicate/stale input | Idempotent apply; reject stale epoch/sequence | Replay batch 10×; one trade per order |
| MD | Crash/slow client | Resume from offset; sequence gap detected; slow client disconnected + resync | `toxiproxy` bandwidth 1 KB/s; gap=0 after resync |
| Surveillance | Duplicate/replayed events | Dedup by event ID; alerts idempotent | Replay audit; alert count unchanged |
| Audit | Crash mid-ingest | At-least-once ingest; upsert by key; resume from offset | Kill audit; row count and hash match |
| Simulator | Crash | Sent-vs-acked ledger; unacked excluded from loss claim | Reconcile; report acked RPO only |
| Dashboard | Iggy down | Stale badge, no fake "live"; backoff | Kill Iggy; assert stale and recovery |
| Iggy partition | Quorum loss | No primary; retryable errors; no acks; recover after restore | Isolate two nodes; assert errors and convergence |

### 7.3 Chaos plan

- Kill the **actual** partition primary, not node 0; random phases; 20 trials.
- Freeze primary (`docker pause`) longer than heartbeat; verify fencing, no split-brain.
- Kill consumer mid-batch; verify replay + dedup + state hash.
- Network partition/delay (`iptables`, `tc netem`, `toxiproxy`); two-node quorum keeps writing, isolated node repairs.
- Disk pressure (`fallocate`, `stress-ng --hdd`); verify refusals, no acks, no corruption, recovery.
- 10× burst; p99 bounded, lag drains, explicit backpressure.
- Clock skew (`libfaketime`); auth/TLS/audit behavior, not consensus claims.
- Whole-cluster crash: `persisted` vs `replicated` negative control.
- Every scenario scripted with `run_id`, commit, image digest, config hash, CPU set, seed; `trap cleanup EXIT`; JSON timeline.

### 7.4 Proof artifacts

Acked-offset ledger vs readable offsets, producer sequences vs stored batches, state hash before/after, audit row count/hash, phase-timestamped metric snapshots. Export `orders_acked_total`, `orders_deduped_total`, `acked_missing_total`, `duplicate_effects_total`, `offset_regression_total`, `matcher_state_hash`, `md_seq_gap_total`, `failover_seconds`, `time_to_lag_zero`.

---

## 8. Market simulator

### 8.1 Model

Three layers:

1. **Arrival clock**: per-symbol multivariate Hawkes, branching ratio 0.7–0.9, cross-excitation, regime multipliers, intensity caps.
2. **Book**: queue-reactive model, 5–10 levels per side, state-dependent insert/cancel/trade intensities, geometric depth + jitter, 1–3 tick spread.
3. **Agents**: market makers (Avellaneda–Stoikov, inventory limits, quote refresh, crisis pull), noise traders (90–95% limit, lognormal body + Pareto tail, sign persistence), momentum (EMA crossover, regime-dependent aggression), informed (Kyle-style latent value), execution algos (TWAP/VWAP).

Anchors: limit orders 90–95%; order-to-trade 10–50:1 liquid, 3–10:1 illiquid; average order size ~2× average trade. Treat as illustrative.

### 8.2 Regimes and scenarios

4-state Markov chain: calm, normal, volatile, crisis; scales intensity, spread, cancel rate, momentum, impact. Exogenous news jumps. Correlated symbols via factor model `r_i = β_i·F + ε_i`; cross-impact `Λ_ij = β_iβ_jλ_market + δ_ijλ_idio`.

Ship scenarios: `calm_open`, `volatile_news`, `flash_crash` (sell metaorder + liquidity withdrawal + momentum + halt/auction + recovery), `liquidity_drought`. Flash crash needs halts, price bands, reopening auction.

### 8.3 Determinism

128-bit master seed; per-stream seeds `BLAKE3(master || scenario_hash || symbol || agent || purpose || counter)`; counter-based RNG (Philox/ChaCha), not thread-local; integer nanoseconds/ticks/quantities; canonical event order; single writer per symbol; manifest with git SHA, scenario hash, seed, parameter hash, RNG, schema, event-log checksum; replay by regeneration and recorded log; mismatch is a hard failure.

### 8.4 Workload matrix

Symbol mix uniform vs Zipf 1.1–1.3 + hot partition; steady vs Hawkes burst (10× 1 s, 100× 100 ms); batch 1/10/100/1000; payload 64 B/256 B/1 KB; fanout 1/3/5/10 groups; replay 1M–100M; dedup/encryption/cluster A/B; one symbol at 40–60% of flow. Publish msg/s and MB/s.

### 8.5 Honest claims

" Synthetic, market-like workload," "reproduces selected stylized facts under stated parameters," "not calibrated to any venue," "not a forecast," "results apply only to this scenario." Validate return autocorrelation ~0, volatility clustering, Hill α≈3, concave impact, book shape, order-flow long memory, cancel/replace ratios. Publish the parameter card and negative results.

---

## 9. Security and multi-tenancy

### 9.1 Model

- **Streams = trust domains**: `whippet-trading`, `whippet-md`, `whippet-ops`, `whippet-sim`.
- **Identity**: one Iggy user + one PAT per service instance; no shared PAT; no root PAT in app containers.
- **Transit**: TLS on TCP/WS/HTTP/QUIC via a local CA; TCP for internal latency; HTTP only for dashboard/admin.
- **At rest**: AES-256-GCM-SIV for orders, executions, alerts, audit; `md` clear (public). One KEK from a secret; per-stream data keys; decide encryption at stream creation.
- **Order path**: client order ID, participant, symbol, side, price, qty, timestamp, nonce, Ed25519 signature; gateway verifies client and re-signs for the matcher; matcher validates price bands, dedups, rejects stale timestamps.
- **Audit**: hash chain over (stream, partition, offset, envelope hash); Ed25519-signed checkpoints outside Iggy.

### 9.2 Permission matrix

| Principal | Grants | Not granted |
|---|---|---|
| bootstrap root | all; one-shot init only | not mounted into apps |
| `svc-gateway` | send `trading.orders`; read `trading.executions` | manage topics; read orders |
| `svc-matching` | read `trading.orders`; send `trading.executions`, `md.md` | send orders; manage |
| `svc-md` | read `md.md` | orders, executions |
| `svc-surveillance` | read orders/executions/md; send `ops.alerts` | write orders |
| `svc-audit` | read business streams; send `ops.audit` | manage/delete |
| `svc-sim` | send `sim` | real streams |
| `svc-dashboard` | read alerts/read-model | orders, users |
| `svc-bench` | manage/send/read `bench` | prod streams |
| human-admin | manage users/streams; read servers | root |

### 9.3 Threats

In scope: unauthorized order injection, service spoofing, replay/tampering, market-data leakage, consumer impersonation, DoS at the edge. Controls: gateway auth + signatures, scoped PATs, TLS, AEAD, hash chain, dedup, rate/size limits, network isolation. Out of scope: native mTLS (not supported), HSM/KMS, host/root compromise, volumetric DDoS, per-user quotas, compliance (MiFID II/SOC 2), formal exactly-once, SaaS tenant administration, key escrow, side channels.

### 9.4 Secrets and Compose

`scripts/gen-secrets.sh` writes `secrets/` 0700/files 0600: root password, KEK, JWT secret, cluster PSK, PATs, TLS keys. `.gitignore` and `.dockerignore` both exclude it. Compose file secrets, not `environment:` (visible in `docker inspect`). Entrypoint reads `/run/secrets/*` and execs. PATs 7 d demo / 24 h bench; rotation script + 401 verification. Root bootstrap one-shot; never `--with-default-root-credentials` or `iggy/iggy`. Publish only gateway/dashboard; keep Iggy internal.

---

## 10. Repo, DevEx, launch

### 10.1 Layout

```
whippet/
├── Cargo.toml  rust-toolchain.toml  Cargo.lock
├── LICENSE  NOTICE  README.md  CONTRIBUTING.md  SECURITY.md
├── CODE_OF_CONDUCT.md  GOVERNANCE.md  CHANGELOG.md  CITATION.cff
├── .github/workflows/{ci,bench-nightly,release,docs}.yml
├── .github/{ISSUE_TEMPLATE,PULL_REQUEST_TEMPLATE.md,dependabot.yml}
├── crates/
│   ├── whippet-core/        # fixed-point types, protocol, errors
│   ├── whippet-engine/      # deterministic matcher; no async, no I/O
│   ├── whippet-gateway/     # auth, validation, idempotency, backpressure
│   ├── whippet-md/          # snapshot+delta, WS/SSE fanout, replay
│   ├── whippet-surveillance/
│   ├── whippet-audit/       # hash chain, replay, state verification
│   ├── whippet-sim/         # seeded order flow / scenarios
│   ├── whippet-bench/       # open-loop harness, HDR histograms
│   ├── whippet-testkit/     # proptest strategies, golden logs
│   └── whippet/             # multicall CLI
├── apps/dashboard/          # Leptos/WASM or TS client
├── deploy/{compose.yaml,compose.ha.yaml,iggy/,prometheus/,grafana/}
├── docker/Dockerfile
├── docs/{ARCHITECTURE,EXCHANGE-101,BENCHMARKS,DEMO,OPERATIONS,
│         FAILURE-MODES,GLOSSARY,UPSTREAM}.md
├── docs/adr/0001..0007-*.md
├── docs/benchmarks/2026-11-m4/{METHOD.md,results.json,charts/}
├── examples/{market-maker,replay-audit}/
├── scripts/{demo.sh,verify-demo.sh,bench.sh,reset.sh}
└── justfile
```

One binary, many subcommands (`gateway`, `engine`, `md`, `surveillance`, `audit`, `sim`, `dashboard`, `bench`); separate Compose services for process isolation.

### 10.2 README order

1. Title, one-line pitch, badges, hero SVG (hot vs durable path), 20-second GIF.
2. "See it in 60 seconds" — three commands, expected output, ports.
3. Five demo beats: place order → fill → kill Iggy node → replay audit → surveillance alert.
4. "Why Iggy" — partition by symbol, consumer groups, offset replay, headers, idempotent producer, TLS + stream encryption, 3-node VSR. One line: "Iggy is not in the matching hot path in turbo mode; it is the durable log."
5. Architecture — C4-ish diagram, latency budget table labeled "design budget to validate."
6. Benchmarks — measured table, hardware/commit/config, NOOP/direct baseline, reproduce command.
7. Correctness — deterministic replay hash, crash/failover tests, exactly-once scope.
8. What works / what doesn't — honest matrix.
9. Repo map, docs links, ADRs.
10. Roadmap, contributing, upstream credit, license/trademark.

Avoid: benchmark tables without hardware/commit/method; "sub-ms" from Docker Desktop; 20-box diagrams; features before the demo; "production-grade"; "exactly-once" without qualification; 10-step quickstarts; unenforced badges; AI-sounding prose.

### 10.3 Compose profiles

- `default`: 1 Iggy + `whippet node` + dashboard + simulator → `http://localhost:8080`.
- `ha`: 3 Iggy nodes (VSR) + node + dashboard.
- `bench`: load generator, host network, dedicated CPU set.
- `obs`: Prometheus + Grafana provisioned.
- `chaos`: toxiproxy/pumba + scripts.

Pin `apache/iggy:0.10.0` and SDK `=0.12.0`; healthchecks (`iggy ping`, `whippet healthcheck`); `depends_on: condition: service_healthy`; named volume `/app/local_data`, never macOS bind mount; publish only gateway/dashboard; `SYS_NICE`, `seccomp=unconfined`, `memlock=-1`; document 12 GB Docker VM for HA.

### 10.4 CI gates

PR: fmt, clippy `-D warnings`, nextest, doctests, cargo-deny, machete, sort, typos, SPDX, compose smoke, `verify-demo.sh`, replay determinism, docs link check. Nightly: criterion/iai microbench, Linux e2e bench with artifacts. Release: multi-arch, SBOM, cosign. Not worth it: benchmark PR gating, mutation testing, 100% coverage, Windows, full e2e every PR.

### 10.5 Launch

Tag `v0.1.0`; post once in Iggy Discord showcase with GIF + three bullets + one specific question; submit Powered By; subscribe `dev-subscribe@iggy.apache.org`, send one plain-text `[DISCUSS]` design finding; r/rust and Show HN with different framing; This Week in Rust. Week 1: reply within 24 h, fix quickstart friction, publish `BENCHMARKS.md`, open upstream issues. Month 1: 2–3 small PRs, pitch a guest post after a merge, lightning talk, roadmap update. No star asks, no cross-post spam, no DMs.

### 10.6 Upstream merit loop

`docs/UPSTREAM.md` from day one: version, symptom, minimal repro. Search issues; reproduce on clean release; issue with version/platform/logs/expected/actual/proposal; wait for assignment; one small PR at a time; license header; local fmt/clippy/test/machete/sort/typos; link issue; respond to every comment; don't rebase for freshness. After merge: bump Whippet, delete the workaround, write five lines.

---

## 11. Risks and open decisions

| Risk | Mitigation |
|---|---|
| Iggy pre-1.0 churn (protocol 0.12.2, cluster young) | Pin image digest + SDK; compatibility matrix; upgrade ADR; no `latest`/`edge` |
| Dual-mode complexity | Shared engine; mode only changes seq/journal/ack path; one guarantee matrix; default `durable` |
| UI scope | Leptos/WASM custom canvas or thin TS client; 5 pages; no framework sprawl |
| M4 benchmark limits | Linux reproduction script; label M4 `dev-loop`; publish raw artifacts |
| Cluster on 16 GB | HA profile optional; 2 shards/node, 1 GiB pool; document 12 GB VM |
| Scope creep (surveillance, auctions, FIX) | Minimum credible semantics; ADR for cuts; "what works/doesn't" table |
| Apache branding | Apache-2.0 + NOTICE; "independent showcase, not affiliated/endorsed"; no feather/logo implication |
| Fake-looking demo | Deterministic sim, honest guarantees, visible freshness/sequence/drops, replay hash |

**Decisions pending:**

1. **UI stack:** Leptos/WASM (pure Rust, slower UI iteration) vs. TypeScript SPA (faster, better charts). Recommendation: Leptos for the constraint, custom Canvas2D.
2. **Default mode:** `durable` (Iggy-inline integrity story) vs. `turbo` (sub-ms latency story). Recommendation: `durable` default, `turbo` toggle.
3. **Cluster in v1:** ship single-node first and add HA in Phase 3, or build HA from day one? Recommendation: single-node v1, HA Phase 3.

---

## 12. Build phases

**Phase 0 — foundation (days):** repo, license/NOTICE, ADRs, Compose skeleton, CI, `docs/UPSTREAM.md`, benchmark manifest format, protocol types.

**Phase 1 — durable vertical slice:** gateway, engine, Iggy orders/executions, MD snapshots/deltas, dashboard Live, simulator, basic bench, `demo.sh`. Demo: place order, see fill, replay from offset.

**Phase 2 — depth:** turbo mode, fanout/conflation, surveillance, audit/hash chain, replay scrubber, auth/PATs/TLS/encryption, full benchmark suite, failure-mode tests.

**Phase 3 — spectacle:** HA cluster + chaos scenarios, polished UI (depth/ladder/heatmap/latency), docs, demo video, README, launch.

**Phase 4 — community:** upstream PRs from `docs/UPSTREAM.md`, Powered By submission, Discord/mailing-list/blog, benchmark uploads, roadmap.

---

## 13. Sources

**Exchange/matching:** [LMAX](https://martinfowler.com/articles/lmax.html) · [exchange-core](https://github.com/exchange-core/exchange-core) · [Disruptor](https://lmax-exchange.github.io/disruptor/) · [CME MDP recovery](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457672425/MDP+3.0+-+MBP+and+MBOFD+Market+Recovery) · [Nasdaq ITCH](https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf) · [MoldUDP64](https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/moldudp64.pdf) · [Aeron Cluster](https://aeron.io/docs/cluster-quickstart/replicated-state-machines/) · [SBE](https://github.com/aeron-io/simple-binary-encoding)

**Iggy:** [Docs](https://iggy.apache.org/docs/) · [Configuration](https://iggy.apache.org/docs/server/configuration/) · [Durability](https://iggy.apache.org/docs/server/durability/) · [Cluster durability](https://iggy.apache.org/docs/clustering/durability/) · [VSR](https://iggy.apache.org/docs/clustering/vsr/) · [Security](https://iggy.apache.org/docs/server/security/) · [Monitoring](https://iggy.apache.org/docs/server/monitoring/) · [Benchmarking](https://iggy.apache.org/docs/server/benchmarking/) · [0.10.0 release](https://iggy.apache.org/blogs/2026/11/09/release-0.10.0) · [Transparent benchmarks](https://iggy.apache.org/blogs/2025/02/17/transparent-benchmarks/) · [Idempotent producers](https://iggy.apache.org/docs/sdk/idempotent-producers/) · [High-level SDK](https://iggy.apache.org/docs/sdk/rust/high-level-sdk/)

**Low latency:** [Rust perf book](https://nnethercote.github.io/perf-book/build-configuration.html) · [rtrb](https://docs.rs/rtrb/) · [quanta](https://docs.rs/quanta/) · [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram_rust) · [Gil Tene](https://www.infoq.com/presentations/latency-pitfalls/) · [wrk2](https://github.com/giltene/wrk2) · [coordinated omission](https://www.scylladb.com/2021/04/22/on-coordinated-omission) · [low-latency guide](https://rigtorp.se/low-latency-guide/)

**Simulation:** [Kyle 1985](https://www.jstor.org/stable/1913210) · [Avellaneda–Stoikov](https://math.nyu.edu/inmemoriam/avellaneda/HighFrequencyTrading.pdf) · [Queue-reactive](https://arxiv.org/abs/1312.0563) · [Hawkes in finance](https://arxiv.org/abs/1502.04592) · [Inverse cubic law](https://arxiv.org/abs/cond-mat/9803374) · [Flash Crash](https://onlinelibrary.wiley.com/doi/10.1111/jofi.12498)

**Dashboard/DevEx:** [uPlot](https://github.com/leeoniya/uPlot) · [Lightweight Charts](https://www.tradingview.com/blog/en/tradingview-lightweight-charts-version-5-50837/) · [Leptos](https://book.leptos.dev/) · [Grafana best practices](https://grafana.com/docs/grafana/latest/visualizations/dashboards/build-dashboards/best-practices/) · [Gobble](https://github.com/inventhq/Gobble) · [vibes](https://github.com/run-vibes/vibes) · [Iggy CONTRIBUTING](https://github.com/apache/iggy/blob/master/CONTRIBUTING.md) · [Become a committer](https://iggy.apache.org/community/become-a-committer/) · [Powered By](https://iggy.apache.org/powered-by/)
