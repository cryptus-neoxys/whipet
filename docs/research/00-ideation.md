# Apache Iggy Showcase — Ideation, Ranking, Top 3

Date: 2026-11-14
Status: decision pending (project + name)

---

## 0. Fixed parameters

- **Home:** `~/Documents/repos/github.com/cryptus-neoxys/<repo-name>` (standalone repo)
- **Stack:** pure Rust services, Rust SDK for Iggy
- **Runtime:** Docker-first, one command up, a few commands for demo; separate profiles for bench/chaos/cluster
- **UI:** in scope, but Iggy capability coverage comes first
- **Performance:** honest benchmarks mandatory; latency percentiles + throughput curves + methodology
- **Time:** no artificial deadline; ship in demoable phases, quality over rush
- **Positioning:** showcase for the Iggy community, complement to upstream PRs, not a SaaS

Hardware available: Apple M4, 10 cores, 16 GB RAM, macOS 27.2, Docker 29.9.2 (daemon currently stopped), 77 GB free.
Implication: headline Iggy numbers (io_uring, microsecond p99) are a Linux story. We benchmark honestly on the M4/Docker Linux VM and ship a one-command Linux reproduction for the community.

---

## 1. How we explored

Wide net across domains where a message log is structurally necessary, not decorative:

- capital markets, adtech, payments, betting
- gaming, esports, motorsport
- robotics, IoT, smart cities, energy, logistics, aerospace
- observability, data infra, devtools, durable workflows
- collaboration, messaging, AI agents
- security

Then filtered by five things: does the domain genuinely need low latency or high throughput, does it exercise Iggy's distinctive features, can it be demoed in 30 seconds, is it distinct from Gobble/vibes/existing examples, and can we build it well in pure Rust + Docker.

---

## 2. Longlist (28 concepts)

| # | Concept | Domain | Core mechanic | Iggy angle | Verdict |
|---|---------|--------|---------------|------------|---------|
| 1 | **Whippet** — exchange stack | capital markets | order gateway → matching engine → market-data fanout → surveillance/audit | per-symbol ordering, consumer groups, replay, headers, dedup, cluster failover, encryption | **Top 3** |
| 2 | **Gavel** — real-time bidding exchange | adtech | bid request fanout to DSPs → auctioneer → win/billing events | independent consumer offsets, hard deadline, dedup, replay | **Top 3** |
| 3 | **Slipstream** — race telemetry | motorsport | car sensors → team/race-control/fan consumers → steward replay | sustained rate, per-car ordering, multi-speed consumers, retention | **Top 3** |
| 4 | **Aegis** — DDoS detection & mitigation | security | flow events → detectors → mitigation commands → attack timeline | massive throughput, low-latency detection, replay | Top 10 |
| 5 | **Arena** — multiplayer game state fanout | gaming | 128 Hz ticks → player/spectator relays → highlight replay | per-match ordering, fanout, replay | Top 10 |
| 6 | **Hive** — drone fleet digital twin | robotics/IoT | telemetry + command streams → geofence/anomaly consumers → incident replay | per-device ordering, retention, multi-tenant, encryption | Top 10 |
| 7 | **Oddsline** — live sports odds & settlement | betting | odds ticks → bettor fanout; bets → settlement; cash-out | ordering per market, fanout, replay, dedup | Top 10 |
| 8 | **Hammer** — live auction house | commerce | per-lot bids → bidder fanout → anti-snipe → settlement | strict ordering, fanout, replay | Top 10 |
| 9 | **Sentinel** — payment fraud/risk | fintech | payments → parallel scorers → decisions → case replay | per-account ordering, parallel consumers, dedup, replay | Top 10 |
| 10 | **Flow** — smart city traffic | civic | intersection sensors → signal commands → emergency preemption | command/telemetry duality, ordering, replay, retention | Top 10 |
| 11 | Observability pipeline | devtools | agents → aggregation → alerts → query API | throughput, consumer groups, retention | Gobble-adjacent |
| 12 | Collaborative document/whiteboard | collaboration | CRDT ops → per-doc ordering → late-joiner replay | ordering, replay, fanout | Latency good, throughput weak |
| 13 | Chat/messaging backbone | messaging | per-conversation streams → multi-device fanout → history | ordering, fanout, replay | Vanilla |
| 14 | Ride-hailing dispatch & surge | mobility | driver/rider events → geo partitioning → pricing | ordering, fanout, low latency | Simulation-heavy |
| 15 | Supply-chain digital twin | logistics | shipment events → geofence/ETA → exception replay | multi-tenant, retention, replay | Simulation-heavy |
| 16 | Smart grid / energy market | energy | meter streams → balancing → outage detection → settlement | throughput, retention, replay | Domain-heavy |
| 17 | Banking ledger / event-sourced core | fintech | accounts, transfers, sagas, audit | ordering, replay, exactly-once concerns | Correctness > spectacle |
| 18 | CDC / real-time ETL | data infra | DB changes → stream → materialized views | connectors, throughput | Connector-dependent |
| 19 | Durable workflow orchestrator | devtools | task streams, timers, retries, replay | consumer groups, offsets | Deep but not visual |
| 20 | Job queue / work distribution | infra | competing consumers, retries, DLQ | consumer groups, ordering | Useful, not flashy |
| 21 | Live leaderboard backend | gaming | score ingestion → windowed aggregation → fanout | throughput, partitioning | Too simple alone |
| 22 | Feature store updates | ML infra | events → features → serving | throughput, replay | Analytics-adjacent |
| 23 | AI agent swarm bus | AI | task/result exchange, tool calls, replay | ordering, replay | Overlaps vibes |
| 24 | Space mission telemetry | aerospace | satellite constellation → ground stations → anomalies | retention, replay, multi-tenant | Cool, simulation-heavy |
| 25 | Event-sourced board games | gaming | turns, replay, spectators | ordering, replay | Small scale |
| 26 | Iggy Flight Recorder | tooling | record/replay/time-travel any stream + UI | offsets, timestamps, retention | Tooling, not a system |
| 27 | Iggy Mission Control | tooling | live cluster/benchmark observability | metrics, admin | Tooling, not a system |
| 28 | Iggy Playground | education | interactive streams/topics/partitions | feature pedagogy | Not a scale showcase |

---

## 3. Scoring model

Weights chosen to match the brief: capability coverage and performance fit dominate; demo and distinctness matter; feasibility keeps us shipping.

| Criterion | Weight | What a 5 means |
|-----------|--------|----------------|
| **C1 — Iggy capability coverage** | 25% | Exercises 8+ distinctive features naturally: partitions/ordering, consumer groups, offsets/replay, headers, batching, multi-transport, retention, dedup, auth, encryption, cluster |
| **C2 — Performance-story fit** | 25% | The domain genuinely cannot work without ultra-low latency or very high throughput; produces a compelling benchmark narrative |
| **C3 — Demo wow & legibility** | 20% | A viewer understands the value in 30 seconds; live UI with visual drama; short quickstart |
| **C4 — Distinctness** | 15% | Not Gobble, not vibes, not an existing Iggy example; fresh domain or fresh mechanic |
| **C5 — Build feasibility & robustness** | 15% | Pure Rust + Docker, no heavy external services/ML, deterministic simulation, low flake risk |

Scores are 1–5; weighted total is out of 5.

---

## 4. Top 10, scored

| Rank | Concept | C1 | C2 | C3 | C4 | C5 | Weighted |
|------|---------|----|----|----|----|----|----------|
| 1 | **Whippet** (exchange) | 5 | 5 | 4 | 5 | 3 | **4.50** |
| 2 | **Gavel** (RTB) | 4 | 5 | 4 | 4 | 4 | **4.25** |
| 3 | **Slipstream** (motorsport) | 4 | 4 | 5 | 5 | 3 | **4.20** |
| 4 | **Aegis** (DDoS) | 4 | 5 | 4 | 4 | 3 | **4.10** |
| 5 | **Arena** (game fanout) | 4 | 4 | 5 | 4 | 3 | **4.05** |
| 6 | **Hive** (drone fleet) | 5 | 3 | 4 | 5 | 3 | **4.00** |
| 6 | **Oddsline** (sports odds) | 4 | 4 | 4 | 4 | 4 | **4.00** |
| 8 | **Hammer** (auction house) | 4 | 2 | 5 | 5 | 4 | **3.85** |
| 9 | **Sentinel** (fraud/risk) | 4 | 4 | 3 | 4 | 4 | **3.80** |
| 9 | **Flow** (smart city) | 4 | 3 | 5 | 5 | 2 | **3.80** |

Subjective notes behind the numbers:

- **Aegis** has an elite perf story but detection heuristics risk feeling like a toy IDS.
- **Arena** is a great demo but the game client + relay + spectator work is heavy.
- **Hive** is throughput-heavy, not latency-critical; the map simulation is significant.
- **Oddsline** is solid but mechanically overlaps Whippet and Gavel.
- **Hammer** is the most fun demo and the weakest high-throughput candidate.
- **Sentinel** is valuable but visually flat.
- **Flow** is the heaviest build for the weakest latency justification.

---

## 5. Top 3 — deep dives

### 5.1 Whippet — sub-millisecond exchange stack (rank 1, 4.50)

**Pitch.** A complete electronic exchange pipeline where Iggy is the spine: order ingress, deterministic matching, market-data fanout, real-time surveillance, and auditable replay. One order in, many market-data messages out, with p99 latency held flat as throughput climbs.

**What the viewer sees.**
- Live order book and trades tape for multiple symbols.
- A load dial: TPS climbs from 10K to 1M+; the latency chart stays flat until the knee.
- Consumer lag panel: market-data subscribers fan out independently.
- A "kill a consumer" button: it resumes exactly where it left off.
- A "kill the leader" button on a 3-node cluster: new leader elected, no acknowledged message lost.
- A replay scrubber: replay any symbol's day at 1×/10×/100×.

**Why it needs Iggy.**
- Price-time priority requires strict per-symbol ordering → partitioned topics.
- Market data must fan out to many independent subscribers → consumer groups with per-consumer offsets.
- Audit, surveillance, and backtesting are replay workloads → offset/timestamp polling, retention.
- Retransmits and duplicate order IDs → server-side dedup.
- Order flow confidentiality → per-stream AES-256-GCM encryption.
- Service identity and least privilege → PATs and per-stream permissions.
- Failover without loss → VSR cluster with persisted durability.

**Benchmark story.**
- Ingress orders/sec vs. p99 end-to-end latency, sweeping batch size and partition count.
- Fanout amplification: 1 order → N book deltas/trades; subscribers 1/10/100/1000.
- Replay throughput: messages/sec while catching up from offset and timestamp.
- Failover: unavailability window and message loss (target: zero acknowledged loss).
- Burst recovery: 10× traffic spike, consumer lag curve and drain time.
- Disclose hardware, transport, payload size, durability policy, and macOS-vs-Linux attenuation.

**Build risk.** Matching-engine correctness (mitigate with property tests and a deterministic simulator), multi-service orchestration, and cluster networking in Docker on macOS.

**Name.** Whippet — the fastest-accelerating sighthound; a quiet nod to Iggy the Italian Greyhound. Alternatives: Courser, Tickline, Orderflow, Tempo.

---

### 5.2 Gavel — real-time bidding under a hard deadline (rank 2, 4.25)

**Pitch.** A programmatic ad exchange where every bid request fans out to many bidders and must be decided inside a 10 ms budget. Iggy carries the request fanout, the bid collection, the win notices, and the billing trail.

**What the viewer sees.**
- Live auction waterfall: request → N bids → winner, with each bidder's latency.
- Deadline compliance chart: p99 decision time vs. the 10 ms line.
- Add/remove bidders live; each keeps its own offset and never blocks the others.
- Spend pacing and win-rate panels.
- Replay a disputed auction from the log.

**Why it needs Iggy.**
- Fanout to independent bidders without coupling → consumer groups / independent consumers.
- Hard per-request deadline → low-latency transports (QUIC/TCP) and batching.
- At-least-once delivery with retries → dedup on request ID.
- Billing reconciliation and dispute replay → retention + offset/timestamp replay.
- Auction metadata → message headers.

**Benchmark story.** Bid requests/sec, fanout factor, p99 decision latency, deadline-miss rate, consumer lag under bidder slowdown, replay throughput.

**Build risk.** Perceived overlap with Gobble (both adtech). Mitigate by framing it as a live auction, not analytics: Gobble measures what happened; Gavel decides in real time under a deadline.

**Name.** Gavel — the auction hammer. Alternatives: Bidline, Hammer, Deadline.

---

### 5.3 Slipstream — race telemetry at full tilt (rank 3, 4.20)

**Pitch.** A Formula-1-style telemetry backbone: 20 cars streaming hundreds of sensor channels at 100–200 Hz, consumed simultaneously by race control, team engineers, a fan-facing aggregator, and a steward replay service.

**What the viewer sees.**
- Live track map with cars moving, throttle/brake traces, tire and fuel state.
- Incident detection flags from race control.
- A spectator counter: fan out to 100K simulated viewers without slowing the engineers.
- "Replay the last lap" at variable speed.
- A throughput chart showing raw sensor rate vs. curated fan rate.

**Why it needs Iggy.**
- Sustained high-rate ingestion → batched/background producers, multiple partitions.
- Per-car ordering → partition by car.
- Consumers at different speeds with independent offsets → teams, fans, stewards.
- Post-race investigation → long retention and replay.
- Channel metadata and delta encoding → message headers.
- Optional encryption for team-only telemetry.

**Benchmark story.** Raw messages/sec (cars × channels × Hz), fanout amplification, consumer lag per group, replay speed, p99 latency from sensor to race-control alert.

**Build risk.** The heaviest UI/simulation lift of the three; telemetry semantics must feel credible or it reads as a toy.

**Name.** Slipstream — the aerodynamic tow a car uses to overtake. Alternatives: Apex, Chicane, Redline, Paddock.

---

## 6. Recommendation

**Build Whippet first.**

It is the only concept that scores 5/5 on both capability coverage and performance fit. It has the cleanest "why Iggy" argument in the set: ordering, fanout, replay, dedup, encryption, auth, and cluster failover all appear because the domain demands them, not because a checklist says so. It also produces the strongest engineering artifacts — benchmark methodology, property-tested matching logic, a chaos/failover story — which is exactly the kind of work that earns committer trust.

Gavel is the best "second act": simpler to build, deadline story is crisp, and it reuses the benchmark harness. Slipstream is the best "spectacle" play and the strongest social/Discord asset, but it is a heavier UI build with a slightly weaker decision-latency story.

If visual impact matters more than engineering depth, choose Slipstream. If speed-to-demo matters most, choose Gavel.

---

## 7. If Whippet: build blueprint

### 7.1 Services (Cargo workspace)

```
whippet/
  Cargo.toml                 # workspace
  crates/
    protocol/                # order, trade, book-delta types; zero-copy codecs
    gateway/                 # QUIC/TCP order ingress, auth, validation, batching
    matcher/                 # deterministic price-time priority engine
    marketdata/              # snapshot/delta builder, WS/SSE fanout, replay
    surveillance/            # spoofing/layering/position alerts
    audit/                   # archival + replay API
    simulator/               # market participants, bursts, scenario scripts
    dashboard/               # Axum + WS/SSE + static UI assets
    bench/                   # end-to-end latency/throughput harness (HDR histograms)
  ui/                        # lightweight frontend (plain TS or Leptos/WASM)
  docker/                    # Dockerfiles, compose, cluster configs
  docs/                      # architecture, benchmarks, chaos runbooks, ADRs
  scripts/                   # demo, bench, chaos, replay
```

### 7.2 Iggy topics

| Topic | Partitions | Key | Retention | Purpose |
|-------|-----------|-----|-----------|---------|
| `orders` | 16 (symbol hash) | symbol | short | ingress order flow |
| `trades` | 16 | symbol | long | executions |
| `book-deltas` | 16 | symbol | short | market-data updates |
| `alerts` | 4 | symbol | medium | surveillance findings |
| `audit` | 4 | service | long | immutable event trail |
| `commands` | 1 | admin | short | replay/control |

Headers on every message: `symbol`, `seq`, `venue`, `type`, `schema`, `trace_id`, `compression`.

### 7.3 Iggy feature matrix

- Streams/topics/partitions with symbol-affinity partitioning
- Consumer groups: matcher, market-data distributors, surveillance, audit
- Offset and timestamp replay; first/last N
- Batching, linger time, direct vs. background producers
- Message headers and zero-copy binary payloads
- Server-side dedup on order IDs
- Multiple transports: QUIC ingress, TCP internal, WebSocket dashboard, HTTP admin
- Retention policies per topic
- PATs and per-stream permissions
- AES-256-GCM stream encryption for order flow
- 3-node VSR cluster with persisted durability and failover demo
- Prometheus metrics; optional MCP "ops assistant" profile

### 7.4 Docker profiles

- `default`: iggy + gateway + matcher + marketdata + surveillance + dashboard + simulator → `http://localhost:8080`
- `bench`: adds bench harness, deterministic load, raw result export
- `cluster`: 3 Iggy nodes + services, VSR config, failover scripts
- `observability`: Prometheus + Grafana (optional)
- `chaos`: kill/restart scripts, consumer pause/resume, network delay

### 7.5 Benchmark plan

1. Ingress: orders/sec at fixed p99; sweep batch size (1/10/100/1000) and partitions (1/4/16/64).
2. End-to-end: gateway ingress → market-data egress; p50/p99/p99.9/p99.99; TCP vs. QUIC; native macOS vs. Docker Linux VM.
3. Fanout: 1 order → N updates; subscribers 1/10/100/1000; per-subscriber lag.
4. Replay: 1M messages from offset and timestamp; catch-up throughput.
5. Failover: kill leader; unavailability window; acknowledged-message loss (target zero).
6. Burst: 10× spike; lag curve and drain time.
7. Resources: CPU, RSS, disk I/O per service.

Publish raw results (CSV/JSON), hardware disclosure, and a Linux reproduction command. Link to Iggy's benchmarking platform for community comparison.

### 7.6 Phases

- **Phase 1 — vertical slice (demoable):** Iggy + gateway + matcher + marketdata + dashboard + simulator; Docker one-command; basic bench. Live book/tape.
- **Phase 2 — depth:** surveillance, audit/replay, headers, dedup, auth, encryption, multi-transport, consumer-group scaling, full bench suite.
- **Phase 3 — spectacle:** polished UI (depth heatmap, latency charts, replay scrubber), chaos scenarios, 3-node failover, demo video/GIF, docs.
- **Phase 4 — community:** upstream PRs for gaps found, publish benchmarks, Discord showcase post, blog proposal.

### 7.7 Upstream merit loop

Keep a running `docs/upstream-findings.md` while building. Every SDK ergonomics issue, missing metric, docs gap, or bug becomes an issue/PR against `apache/iggy`. This turns the showcase into a steady stream of visible contributions — the actual currency of the committer path. Given the existing Python/Node SDK work, Rust SDK and server-side gaps are the natural next surface.

---

## 8. Decisions needed

1. **Project:** Whippet, Gavel, or Slipstream? Recommendation: Whippet.
2. **Name:** Whippet (recommended), Courser, Tickline, or Orderflow.
3. **UI stack:** lightweight static frontend served by Rust (fastest) vs. Leptos/WASM (all-Rust, more build time).
4. **Cluster demo:** include 3-node VSR failover in v1, or ship single-node first and add cluster in Phase 3? Recommendation: single-node v1, cluster in Phase 3.
