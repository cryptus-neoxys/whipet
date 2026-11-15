# Whipet: Current-State Review and Improvement Plan

**Review date:** 15 November 2026  
**Repository:** <https://github.com/cryptus-neoxys/whipet>  
**Branch reviewed:** `main`  
**Document status:** Proposed engineering plan; not yet adopted or committed by the maintainers.

> This plan is intended to complement the existing README, architecture, operations, performance, benchmark, and failure-mode documents. It does not replace the system's current durability contract. Priorities are ordered to stabilise and prove the Phase 1 durable path before expanding scope or introducing a faster, weaker-durability mode.

---

## 1. Executive assessment

Whipet is already more than a toy order-book demo. It has a useful and coherent Phase 1 vertical slice:

- A Rust workspace with a deterministic matching core separated from the async runtime and Iggy client.
- A durable order-command log in Apache Iggy, partitioned by symbol hash.
- A price-time-priority matcher with fixed-point prices/quantities, bounded price ladders, FIFO queues per price level, an occupancy bitmap, and order-ID lookup.
- A reader / matcher / writer pipeline with bounded queues and a dedicated OS thread for each active matching shard.
- Execution reports and market-data events appended back to Iggy.
- Snapshot verification and replay tooling.
- HTTP/JSON and binary TCP ingress, batch submission, a simulator, a React dashboard, WebSocket state updates, a benchmark harness, Prometheus metrics, and operational scripts.

The strongest architectural decision is that the matching core is deterministic and isolated from Tokio, Iggy, wall-clock time, and network I/O. The second is that the current operating mode gives accepted orders a clear durable-log boundary: `accepted_by_log` means the command was appended to Iggy; it does **not** mean that the matcher has executed it or that the client has received a fill.

The biggest current weakness is not the matching algorithm. The available M4/Docker evidence points to the **durable ingress / Iggy append and growing-log path** as the current bottleneck. A documented 180-second, 5,000-order/s batch-10 soak saw ingress P99 grow from roughly 7 ms to roughly 390 ms while matcher processing remained around 4 µs; the document attributes the regression to Iggy append latency as the log grew. The repository correctly labels this a `dev-loop` smoke result, not a publishable headline benchmark.

The project is therefore at a useful but specific stage: **the durable single-node exchange vertical slice exists; its sustained performance, recovery semantics, retention lifecycle, and operational observability still need to be demonstrated and hardened.** The immediate objective should be to make the durable mode correct, measurable, and stable under sustained load—not to optimise a benchmark number in isolation.

### Overall assessment by dimension

| Dimension | Assessment | Main reason |
|---|---|---|
| Matching-core design | Strong foundation | Deterministic, single-writer state per shard, fixed-point arithmetic and compact structures. |
| Durable order path | Implemented, needs sustained proof | Iggy-first append and group commit; current soak shows log-growth/append latency drift. |
| Concurrency model | Good foundation | Dedicated matcher threads and bounded input/output rings; queue capacities and backpressure need careful measurement. |
| Market-data delivery | Functional but simplified | 20 Hz conflated full-state updates are suitable for a dashboard, not a complete exchange feed. |
| Recovery | Partial / at-least-once | Snapshot and replay exist, but output append and input-offset commit are separate operations. |
| Idempotency | Needs stronger contract | Deduplication is bounded; old client order IDs can eventually fall outside the dedup window. |
| Observability | Incomplete | Consumer-lag metric is explicitly a placeholder; latency quantiles are process-lifetime cumulative. |
| Retention / storage lifecycle | Critical gap | Cleaner is disabled to avoid periodic latency spikes, so topic retention is recorded but not enforced. |
| Performance evidence | In progress | Useful M4/Docker smoke results exist; repeated benchmark runs, long soak, fan-out measurements and native-Linux results remain outstanding. |
| Product completeness | Phase 1 scope only | No account ledger, risk/credit controls, clearing, FIX, active command plane, or HA. |
| Security / operations | Demo-safe, not production-ready | Optional auth, no TLS termination in the process, and `/metrics` is unauthenticated. |

---

## 2. What exists today

### 2.1 Runtime and data flow

The current durable path is conceptually:

```text
HTTP / binary TCP clients
        |
        v
Gateway validation + symbol-to-partition routing
        |
        v
Iggy: whippet-trading / orders (persisted)
        |
        v
Per-partition reader -> bounded input ring
        |
        v
Dedicated single-writer matching thread
        |
        v
Bounded output ring
        |
        v
Per-partition writer -> Iggy executions + Iggy market data
        |                         |
        v                         v
Input offset commit        Market-data hub / dashboard
```

The matcher is not called synchronously on the HTTP request path. The gateway acknowledges durable log acceptance, then the asynchronous engine processes the command. The system should keep this distinction explicit in API names, metrics, dashboard copy, and performance reports.

The runtime uses separate Iggy connections for ingress and engine work. Per-partition ingress group commit coalesces concurrent single-order writes into appends of up to 256 messages without a fixed linger. The batch endpoint can submit up to 256 same-symbol commands in one append. A binary TCP endpoint is available on port 8081 and supports the fixed-layout protocol.

### 2.2 Matching functionality

The current matcher supports the Phase 1 core:

- Limit GTC orders.
- Market orders with a price collar.
- IOC, FOK and post-only behaviour.
- Partial fills and multi-level execution.
- Cancel.
- Self-trade prevention using the documented cancel-resting policy.
- Price-time priority.
- Integer tick / quantity representation rather than floating point.
- Order-ID deduplication and state hashing.
- Bounded instrument price ladders, price-level FIFO queues, occupancy bitmap and an order index.

This is a strong deterministic base for a showcase. It is not yet a complete market microstructure implementation. Stop orders, iceberg/reserve quantity, pegged orders, trailing stops, auctions, amend/replace, multi-leg orders, account-level risk, and clearing/settlement are not part of the current Phase 1 path. The `halted` state exists in the model, but a complete operator command path to halt/resume an instrument is not yet present.

### 2.3 Market data and dashboard

The engine writes market-data events and snapshots to Iggy. The market-data hub applies deltas into an in-memory view and exposes a conflated state stream to the dashboard at about 20 Hz. The current design is effective for a human-facing UI: the browser receives the latest view rather than every intermediate book mutation.

It should not be described as a complete low-latency market-data distribution service. The current browser-facing path does not provide a resumable raw sequenced delta feed with per-client gap detection and replay. The full-state dashboard update cadence intentionally loses intermediate states. BBO/status variants exist in the protocol, but the current engine does not emit the complete set of venue-control events.

### 2.4 Persistence, snapshot and replay

The repository has useful foundations:

- Durable Iggy command log.
- Snapshot files with versioning and BLAKE3 state hashes.
- Temporary-file write, `fsync`, and rename for snapshot replacement.
- Snapshot verification on restore.
- Replay CLI and deterministic replay tests.
- Offset-aware snapshot / replay logic.

The guarantee remains **at-least-once processing with deterministic state reconstruction**, not end-to-end exactly-once output. If the engine appends execution/market-data output and crashes before committing the corresponding input offset, that input may be replayed and output may be appended again. Downstream consumers need stable event identity and deduplication. This is an acceptable and honest Phase 1 trade-off if it is clearly documented and tested.

### 2.5 Development, benchmark and operations tooling

The project includes `make` targets for build, demo, benchmarks, TCP, fan-out, soak, crash testing, verification, replay, health/doctor, observability and experimental HA work. It has a seeded simulator with several market scenarios, run manifests, and an open-loop load-generator approach.

The documentation describes one initial release (`0.1.0`) and substantial work under the Unreleased changelog section. The public repository currently has no open product issues or milestones; the visible open pull requests are dependency-update pull requests. A prioritised project roadmap would make the next phase much easier to coordinate.

---

## 3. Performance reality: what the evidence says

### 3.1 Do not use the earlier theoretical ceiling as the current system result

Microbenchmarks of an isolated matcher, or published Iggy throughput on a large Linux server, are not the throughput of this complete M4/Docker application. The current system's measured ceiling must be reported from its actual gateway → Iggy → matcher → output → consumer path.

The repository's benchmark documents deliberately label the current Apple Silicon + Docker Desktop runs as `dev-loop` smoke measurements. They are useful for finding regressions, not yet suitable for a headline claim. The current data should be presented with that qualification.

### 3.2 Recorded M4 / Docker evidence

The benchmark documents include these representative results:

| Test | Offered rate | P50 | P99 | Interpretation |
|---|---:|---:|---:|---|
| Earlier single-order HTTP run | 1,000 orders/s | ~1.5 ms | ~86.7 ms | Queueing/append latency knee already visible. |
| Earlier single-order HTTP run | 2,000 orders/s | ~88.6 ms | ~655.9 ms | Overloaded; not a sustainable operating point. |
| Earlier single-order HTTP run | 5,000 orders/s | ~1.09 s | ~1.68 s | Generator/overload failure; do not quote as capacity. |
| After dedicated matcher thread, single HTTP | 5,000 orders/s | ~1.77 ms | ~4.30 ms | Short, single-repetition smoke run. |
| After dedicated matcher thread, HTTP batch 10 | 5,000 orders/s | ~1.13 ms | ~3.66 ms | Batch acknowledgement latency, not per-order latency. |
| After dedicated matcher thread, TCP batch 10 | 10,000 orders/s | ~0.53 ms | ~3.92 ms | Short, single-repetition smoke run. |
| Documented 180-second soak, batch 10 | 5,000 orders/s | P50 ~1.1 -> 5.0 ms | P99 ~7 -> 390 ms | Not steady-state; ingress append latency rises as the log grows. |

The later short tests show the value of the dedicated matcher-thread and binary/batch ingress work. They do not invalidate the soak result. An eight-second measurement can look good while a three-minute run degrades significantly.

**Important reporting rule:** batch request latency is not the same thing as per-order latency. The batch response has its own acknowledgement semantics, and batch formation / submission changes the measured interval. Report batch request latency, throughput per order, and (where possible) individual command completion/processing latency separately.

### 3.3 The current bottleneck is likely outside the matcher

The documented soak attributes the large P99 increase to the Iggy append round trip (`gateway_append`), while matcher processing remained around 4 µs. That suggests the first performance investigation should focus on:

1. Iggy WAL growth and fsync/group-commit behaviour.
2. The fact that the messages cleaner is disabled and logs keep growing.
3. The relationship between partition count, shard count, active symbols and Iggy CPU allocation.
4. The number and size of records emitted per order (executions, market data, snapshots).
5. Queue depth and end-to-end backpressure.
6. Whether the load generator is saturating or slipping under offered load.

Do not start by rewriting the matching algorithm or adding more matching threads. First demonstrate that the matcher is not the bottleneck under the workload that exhibits the latency growth.

### 3.4 Retention is an operational blocker, not a tuning footnote

Topic creation records finite retention policies and maximum sizes, but the current Docker stack disables the Iggy messages cleaner because periodic cleaning produced append-latency spikes. As documented, the policies therefore are not enforced: the log grows until the volume is full.

Simply re-enabling the cleaner is unsafe. If it deletes a segment range before a consumer's committed offset, the engine/market-data consumer can no longer resume from the log alone; full replay also becomes incomplete. Retention must be designed together with snapshots, consumer lag, recovery windows and archival.

The immediate requirement is a bounded-storage policy with explicit safety margins and alerts. The strategic requirement is a replay-safe retention/archive scheme.

---

## 4. Critical findings and recommended priorities

### P0 — fix now: correctness of documentation and visible repository quality

#### 4.1 Broken repository links

The repository is named `whipet`, but the README clone instructions and Cargo repository/homepage metadata point to `cryptus-neoxys/whippet`. The latter path currently returns 404. Fix all references to the actual repository URL. Verify badges, issue links, docs links and CI links at the same time.

#### 4.2 Correct stale and contradictory documentation

There are several current-state discrepancies to reconcile:

- README architecture prose describes a synchronous matcher call on the task path, while the current architecture/ADR documents describe the dedicated OS thread and bounded reader/matcher/writer pipeline.
- The failure-mode document describes a 500,000-entry dedup default, while the current code and operations document describe a 100,000-entry cap per symbol.
- The performance document contains older hot-path/fan-out descriptions that should be reconciled with the newer `FrameCache`/shared-frame implementation and current benchmark harness.
- The README, architecture and performance documents should identify which features are implemented, experimental, planned or intentionally out of scope.

**Acceptance:** links resolve; the README diagram matches the current pipeline; dedup capacity matches the code; every performance claim names the commit/configuration/test and is labelled measured, estimated or planned.

### P0 — fix now: bounded storage and honest durability operations

#### 4.3 Define a safe disk-growth policy

Until replay-safe retention exists:

- Export Iggy topic/partition log bytes and Docker volume free bytes.
- Alert at configurable warning and critical thresholds (for example, 70% and 85% volume use; thresholds should be configurable, not hard-coded assumptions).
- Fail readiness or stop accepting new durable orders before the volume reaches an unsafe level.
- Document the operator's safe response: pause ingress, preserve the volume, snapshot/verify, archive or reset only with explicit data-loss approval.
- Add a disk-full simulation to the failure test suite.

Do not silently delete old log segments to recover space.

#### 4.4 Design retention around replay boundaries

A retention design should calculate the minimum log window from:

- The oldest committed consumer offset.
- The oldest valid snapshot baseline for each shard.
- The maximum supported broker / node downtime.
- A safety margin for recovery and operator intervention.
- Audit and business retention requirements.

A segment is only eligible for deletion after all relevant consumers have advanced beyond it and a verified snapshot/archive provides a valid recovery baseline. If the application cannot prove those conditions, it must retain the segment.

### P1 — next: prove correctness under failure

#### 4.5 Turn the failure-mode matrix into executable, repeatable tests

The documented failure modes already identify the right scenarios, but several are implemented without a completed fault-injection test. Add deterministic tests and `make crash-test` coverage for:

1. Gateway process killed immediately before Iggy append.
2. Gateway killed after append succeeds but before the HTTP/TCP response reaches the client.
3. Matcher killed after reading input but before producing output.
4. Matcher killed after execution/MD append succeeds but before input-offset commit.
5. Iggy unavailable during ingress, output append, and offset commit.
6. Output ring full / writer stalled / append retry exhausted.
7. Corrupt, truncated, old-version and missing snapshot.
8. Disk nearly full and disk full.
9. Duplicate client order ID before and after dedup eviction.
10. Retention boundary crossed while a consumer is offline.

For every test, assert the recovered book hash, order state, execution count, emitted event identities, input offsets and documented duplicate behaviour. Preserve the test run's Iggy and node logs as CI artifacts.

#### 4.6 Make output idempotency explicit

The current append-output / commit-input boundary can create duplicate execution and market-data records after a crash. Do not claim exactly-once delivery. Give every deterministic output a stable identity, for example:

`(venue, shard_or_partition, input_offset, output_sequence, event_kind)`

Persist that identity in the event envelope and require downstream accounting, market-data archival and analytics consumers to deduplicate it. Verify that retries do not create duplicate business effects.

Longer term, investigate an outbox/checkpoint protocol or transactional capability if the broker supports it and it is worth the complexity. Until then, at-least-once plus consumer deduplication is the honest contract.

#### 4.7 Strengthen client order-ID deduplication semantics

The current dedup structure is bounded (100,000 entries per symbol in the operations documentation/code path). A count-based window is not an unlimited idempotency guarantee: after eviction, an old order ID can potentially be accepted again. The effective time window depends on throughput and terminal-order mix; it should not be advertised as a fixed duration.

Define the contract for:

- Duplicate IDs while an order is live.
- Duplicate IDs after fill/cancel/reject.
- Retry after gateway timeout / lost response.
- Retry after process restart and snapshot restore.
- Retry after dedup eviction.

Options to evaluate:

1. Participant-scoped monotonic client sequence numbers with persisted high-watermarks and a bounded recent-ID cache.
2. A durable idempotency ledger with time/sequence-based compaction.
3. Explicitly scoped idempotency TTL, surfaced in API documentation and metrics.

Do not enlarge the in-memory table blindly; that can increase snapshot size and memory footprint without defining a durable guarantee.

### P1 — next: make performance results trustworthy

#### 4.8 Separate the benchmark layers

Publish distinct benchmark suites and tables for:

- **Matcher-only:** prebuilt books and command arrays, no Iggy, network or async runtime.
- **Engine pipeline:** Iggy consumer → bounded input → matcher → bounded output, with output sink controlled.
- **Durable end-to-end:** gateway → persisted Iggy append → matcher → output append → offset commit.
- **Market-data fan-out:** event ingestion to 1 / 10 / 100 / 1,000 / 10,000 subscribers, reporting per-client send/drop/coalesce behaviour.
- **Recovery:** snapshot load, log replay, replay throughput and maximum recovery point.

Record offered rate, accepted rate, matched rate, fills/s, trades/s, events/s, p50/p95/p99/p99.9, error/drop count, queue depth, CPU, RSS, disk bytes and Iggy WAL/append metrics.

#### 4.9 Make every headline result reproducible

For a publishable result:

- Run at least five measured repetitions after warm-up; retain all runs, not only the best.
- Use a fresh or explicitly documented log state.
- Separate cold-start and warm steady-state results.
- Use open-loop load generation and verify the generator maintained the intended offered rate.
- Run 10–15 minute tests for sustained capacity; run longer soak tests for retention and memory growth.
- Include machine model/RAM, macOS version, Docker Desktop version, VM CPU/RAM, image digest, Iggy and SDK versions, Rust version, commit SHA, shard/partition layout, payload mix, batch size, durability policy and volume type in the manifest.
- Clearly label Apple Silicon/Docker Desktop numbers as such. Run native ARM64 Linux benchmarks separately before generalising the result.

The current TCP batch-10 10,000 orders/s result is promising as a short smoke measurement, not proof of sustained 10,000 orders/s. The documented 5,000 orders/s soak already shows why short measurements are insufficient.

#### 4.10 Make workload profiles explicit

Maintain at least these repeatable profiles:

| Profile | Mix / intent |
|---|---|
| Insert-heavy | Mostly non-crossing GTC limits; stresses book insertion and memory/indexes. |
| Match-heavy | Marketable limits/market/IOC that cross resting liquidity; stresses matching and output generation. |
| Cancel-heavy | High cancel ratio; stresses order index and level unlinking. |
| Mixed L1 | Existing representative mix (e.g. resting limits, IOC, cancels); preserve as the default public benchmark. |
| Multi-level sweep | Aggressor orders consume multiple price levels; measures fills/trades per command. |
| Hot-symbol | All traffic targets one symbol; exposes single-writer ceiling. |
| Distributed-symbol | Traffic spread across independent symbols; measures shard scaling. |
| Fan-out | Fixed event rate with subscriber counts and slow-client profiles. |

Always report one hot symbol separately from multi-symbol aggregate throughput. A multi-shard aggregate is not evidence that a single hot order book can process the same rate.

### P1 — next: repair observability and backpressure visibility

#### 4.11 Replace placeholder consumer lag

`whippet_consumer_lag` is documented as a placeholder set to zero after each MD batch. Replace it with real per-consumer/per-partition lag:

- Head offset minus committed/consumed offset, expressed in records.
- Age of the oldest unprocessed input record.
- Last successfully committed input offset.
- Output append retry count and age of oldest pending output.
- Input and output ring occupancy / capacity.

Expose aggregate and per-shard forms. Do not aggregate away the one lagging partition that determines recovery time.

#### 4.12 Make latency metrics useful for operations

The current exported quantiles come from process-lifetime HDR histograms. Add windowed histograms or Prometheus histogram buckets so operators can detect a recent degradation without restarting the process. Keep a clearly named lifetime metric only if it adds value.

Instrument these stages independently:

- Gateway decode/validation.
- Gateway Iggy append and wait-for-group-commit.
- Iggy read/poll and input queue wait.
- Matcher service time and batch size.
- Output ring wait.
- Execution append and MD append.
- Input offset commit.
- Snapshot clone, hash, serialization, fsync and rename.
- Market-data apply, view construction and WebSocket fan-out.

Avoid per-order logging or a global metrics lock on the hot path. Keep shard-local counters/histograms and aggregate them off the matcher thread.

### P2 — plan: improve the market-data contract

#### 4.13 Separate dashboard state from exchange feed

Keep the 20 Hz conflated full-state stream for the dashboard; it is appropriate for UI rendering. Add a separate sequenced delta-feed API for consumers that need every change:

- Per-symbol sequence numbers and a documented sequence scope.
- Snapshot-at-sequence handshake.
- Delta replay or resync after a detected gap.
- Explicit slow-consumer policy (bounded queue, disconnect/resnapshot, or coalescing only where allowed).
- Stable event IDs and duplicate handling.
- Separate channels for trades, book deltas, BBO and instrument status.

Do not make the UI's conflation strategy the contract for external market-data consumers.

#### 4.14 Measure fan-out before optimising it further

The shared serialized-frame cache is a useful optimisation and the repository now records partial dev-loop fan-out measurements: a 500-subscriber run with 10,000 orders/s batched ingress delivered about 20 fps and roughly 245 KB/s per client with no ingress errors or stalled clients. A 1,000-subscriber run currently fails at Docker Desktop host-port proxy socket resets before the node reaches its configured 2,048-subscriber cap. Treat these as useful boundary observations, not a repeatable fan-out capacity claim. Publish repeated CPU, allocations, bytes/sec, event-to-send latency, queue depth, dropped/coalesced updates and subscriber disconnect results for 1/10/100/500/1,000 subscribers; include slow consumers and large books, and repeat on native Linux/direct networking.

### P2 — plan: complete the exchange simulation domain

The next domain features should be chosen deliberately; adding every order type is not the same as making a credible exchange.

Suggested sequence:

1. **Instrument control plane:** halt/resume, trading status, price-band changes, session state and operator audit trail. The existing reserved commands/audit/alerts topics can become active only once command ownership, authorization, ordering and replay are defined.
2. **Participant/account model:** participant identity, account balances, positions, buying power, permissions and account-level limits.
3. **Pre-trade risk:** max order notional, price collars, credit/position limits, fat-finger controls, self-trade policy configuration and kill switch.
4. **Ledger and clearing simulation:** immutable double-entry cash/position movements, trade capture, settlement lifecycle and reconciliation against execution IDs.
5. **Order lifecycle expansion:** amend/replace, stop orders, iceberg/reserve quantity and auction phases—only where they support the demo's learning goals.
6. **Surveillance and audit:** order-to-trade reconstruction, wash/self-trade patterns, spoofing indicators as educational signals, and replayable audit trail. Avoid claiming regulatory-grade surveillance without validation.

For a showcase, a correct account/risk/ledger story is likely more valuable than a long list of obscure order types.

### P2 — plan: scale and tune the deployment

#### 4.15 Prove native Linux before core pinning and aggressive I/O tuning

The M4 Docker VM is a useful local development target, but benchmark it separately from native Linux. Iggy has a Linux `io_uring` path; ensure the container is native `linux/arm64`, and keep the Iggy data directory in a Docker named volume rather than a macOS bind mount.

After native-Linux measurements, test:

- Iggy shard count and `cpu_allocation`.
- Matcher worker count versus active partitions.
- CPU affinity / core pinning and `SYS_NICE` only where supported and measured.
- WAL size and group-commit delay.
- Segment size and retention/cleaner strategy.
- SSD write/flush latency and fsync behaviour.

Do not tune every knob simultaneously. Use a one-variable-at-a-time matrix and preserve known-good profiles.

#### 4.16 Revisit static partitioning and instrument mapping

The current FNV symbol hash modulo a fixed partition count is simple and deterministic. It also means:

- A partition-count change changes the symbol mapping and requires reset/migration.
- A hot symbol cannot be split across matchers without changing ordering semantics.
- Uneven symbol activity can leave some workers idle and one worker saturated.

For Phase 1, keep the model and expose per-partition throughput/lag. For a later multi-node design, introduce an explicit instrument directory and a controlled migration protocol (freeze, snapshot, transfer, epoch bump, resume). Avoid changing ownership of an active book without a defined sequencing barrier.

### P3 — later: security, protocols and high availability

These are important, but should follow the single-node correctness and operations gates:

- TLS for HTTP, WebSocket and binary TCP, or a clearly documented trusted reverse-proxy boundary.
- Strong participant authentication, token rotation, scoped permissions and rate limits per participant as well as per IP.
- Protect metrics and admin endpoints; do not expose unauthenticated `/metrics` publicly.
- Audit sensitive operator actions.
- Stream encryption / key management if the threat model requires it.
- FIX gateway only after order semantics and session/recovery behaviour are stable.
- Multi-node Iggy VSR / 3-node deployment only after crash recovery, idempotency, retention, observability and backup/restore are proven on one node.
- Cross-symbol atomic operations only with an explicit distributed transaction/coordination model; do not imply global atomicity from partitioned logs.

---

## 5. Quick wins: recommended order of execution

These are deliberately small, high-leverage tasks that should be tackled before new large features.

| Order | Task | Why it is quick / valuable | Done when |
|---:|---|---|---|
| 1 | Fix all `/whippet` links to `/whipet` | Broken clone and manifest links undermine first-run experience. | README clone command, Cargo metadata and badges resolve to the real repo. |
| 2 | Reconcile README and architecture diagrams | Current README prose is behind the dedicated-thread implementation. | One source-of-truth diagram and matching descriptions in README/ARCHITECTURE/ADR. |
| 3 | Correct dedup documentation | Failure-mode docs and code disagree on the default capacity. | All docs and tests agree on the actual cap and its semantics. |
| 4 | Mark all benchmark numbers by class | Prevents smoke runs being mistaken for capacity claims. | Every table says `smoke`, `soak`, `microbench`, or `headline`; includes repeat count and environment. |
| 5 | Add actual consumer lag + ring occupancy metrics | Immediate visibility into the most important pipeline failure modes. | No placeholder zero; per-shard lag/age/occupancy available and tested. |
| 6 | Add volume/log-growth gauges and disk-full protection | Prevents silent unbounded storage exhaustion. | Warning/critical alerts and deterministic admission/degraded behaviour are tested. |
| 7 | Run five repetitions of current HTTP/TCP/batch profiles | Verifies whether the new thread/batch path is consistently better. | Raw manifests retained, median and spread published. |
| 8 | Run 10–15 minute 5k/s soak on fresh and warm logs | Tests the current known failure point after recent changes. | Bounded latency/queue/RSS/log-growth profile, with no dropped or unaccounted commands. |
| 9 | Complete one crash test: output append before input commit | Highest-value exactly-once misconception / duplicate-event scenario. | Replayed output is deduplicated by stable event identity; book hash and effects match. |
| 10 | Complete and repeat the fan-out sweep | Partial dev-loop data exists at 500 subscribers; 1,000 hits Docker Desktop host-port proxy resets. | Repeated results for 1/10/100/500/1,000 subscribers and slow clients recorded; separate Docker proxy limits from node limits. |

The existing dependency-update pull requests should be reviewed and merged in small batches after CI, formatting, MSRV, image and benchmark smoke checks. Do not merge a dependency wave solely because it is automated; confirm SDK/server compatibility and reproduce the normal test matrix.

---

## 6. Proposed phased roadmap

### Phase A — Stabilise the Phase 1 durable slice

**Goal:** correct semantics, bounded resource use, trustworthy observability and repeatable M4 results.

Deliverables:

- Repository links and docs reconciled.
- Deduplication contract stated and tested.
- Real consumer lag / ring occupancy / stage latency metrics.
- Disk capacity alerts and explicit overload / admission behaviour.
- Five-repeat short benchmark suite and 10–15 minute soak.
- Completed crash matrix for gateway, matcher, writer, commit and broker failures.
- Stable output event identity and consumer deduplication guidance.
- Recovery runbook: snapshot, verify, replay, restore, archive and safe reset.

**Exit gate:** a 10-minute sustained run at a declared target rate (start with 5,000 accepted commands/s on the M4 Docker profile) has no unexplained drops, bounded queues, no unbounded latency growth, no unbounded RSS growth, and a verified recovery after an injected crash. Publish all percentiles and machine/configuration details. If it fails, report the measured knee; do not raise the target by weakening correctness.

### Phase B — Establish real performance envelopes

**Goal:** identify the throughput ceiling at each layer and improve the bottleneck with evidence.

Deliverables:

- Matcher-only microbenchmark matrix.
- Engine-pipeline benchmark with controlled sinks.
- Durable end-to-end benchmark with persisted Iggy.
- Open-loop HTTP vs TCP vs batch comparisons.
- One-hot-symbol vs multi-symbol scaling.
- Fan-out benchmark through at least 1,000 subscribers, including slow consumers.
- Native ARM64 Linux benchmark profile, separate from Docker Desktop.
- Tuned Iggy / matcher shard profiles, with manifests and repeatability.

**Exit gate:** each public throughput statement identifies whether it is commands/s, matched orders/s, fills/s, trades/s, Iggy records/s or subscriber deliveries/s. P50/P99/P99.9 and sustained duration accompany every throughput number.

### Phase C — Exchange semantics and market-data quality

**Goal:** make the simulation behave like a small exchange, not only a fast order book.

Deliverables:

- Instrument halt/resume and session control through an audited command plane.
- Participant and account model.
- Pre-trade risk controls and kill switch.
- Double-entry cash/position ledger and settlement simulation.
- Sequenced market-data delta feed with snapshot/resume/gap recovery.
- Trade tape and BBO/status event completeness.
- Selected additional order types (amend/replace, stop, iceberg, auction) based on demonstrable use cases.
- Audit and basic surveillance pipeline.

**Exit gate:** deterministic end-to-end scenarios show account balances, positions, risk rejections, executions, market data and ledger entries all reconcile to the same trade identities after restart/replay.

### Phase D — Security and multi-node evolution

**Goal:** move from a local showcase to a credible distributed systems demonstration.

Deliverables:

- TLS and scoped participant/operator authentication.
- Metrics/admin endpoint protection and audit of administrative changes.
- Explicit partition ownership and migration protocol.
- Three-node Iggy/VSR experiment with leader changes and network partitions.
- Failover/recovery tests with measured RPO/RTO, not assumed zero-loss/zero-downtime claims.
- FIX gateway or other external protocol adapter only after the internal contract is stable.

**Exit gate:** publish failure-injection evidence for node loss, leader election, partition/network isolation, restart and consumer recovery. State the actual durability and availability guarantees; do not label the system HA merely because three brokers start.

---

## 7. Benchmark and correctness acceptance checklist

### Throughput and latency

- [ ] Measure matcher-only, engine pipeline, durable end-to-end and fan-out separately.
- [ ] Use a dedicated open-loop generator and report offered vs accepted rate.
- [ ] Report p50, p95, p99 and p99.9, plus error/drop count.
- [ ] Include one hot symbol and distributed symbols.
- [ ] Include insert-heavy, match-heavy, cancel-heavy and multi-level sweep workloads.
- [ ] Run at least five repetitions for published results.
- [ ] Run a sustained 10–15 minute test, followed by a longer soak for resource growth.
- [ ] Record CPU, RSS, swap, disk writes, Iggy WAL growth, log bytes, queue depth and per-shard lag.
- [ ] Record machine, OS, Docker VM, image digest, Rust/Iggy versions, config and commit SHA.
- [ ] Do not compare batch acknowledgement latency directly with single-order latency.

### Matching and recovery correctness

- [ ] Verify price-time priority under concurrent ingress.
- [ ] Verify deterministic ordering within a symbol/partition.
- [ ] Verify partial fills and multi-level sweeps.
- [ ] Verify IOC/FOK/post-only/market collar and self-trade policy.
- [ ] Verify duplicate order IDs across retry, restart, snapshot and eviction boundaries.
- [ ] Verify replay produces the same state hash.
- [ ] Verify output event identity prevents duplicate business effects after reprocessing.
- [ ] Verify no input offset advances beyond successfully persisted output.
- [ ] Verify snapshots at active traffic and while writes are delayed.
- [ ] Verify retention never removes the only replay path to a valid snapshot.

### Operations and security

- [ ] Disk warning and critical alerts work.
- [ ] Disk-full behaviour is safe and deterministic.
- [ ] Broker outage degrades readiness and is visible in metrics.
- [ ] Metrics endpoint is not publicly exposed without protection.
- [ ] Secrets are not shipped as defaults in production examples.
- [ ] TLS/auth/rate-limit boundaries are documented.
- [ ] Recovery, backup, restore and reset commands are unambiguous.

---

## 8. Suggested metrics to add or refine

| Metric | Type | Labels / scope | Purpose |
|---|---|---|---|
| `whippet_input_lag_records` | Gauge | shard, partition | Broker head minus consumed/committed position. |
| `whippet_input_oldest_age_seconds` | Gauge | shard, partition | Age of oldest unprocessed command. |
| `whippet_input_ring_occupancy` | Gauge | shard | Input ring backpressure. |
| `whippet_output_ring_occupancy` | Gauge | shard | Output writer backpressure. |
| `whippet_output_append_retries_total` | Counter | shard, topic | Broker write instability. |
| `whippet_uncommitted_output_records` | Gauge | shard | Output produced but input offset not yet committed. |
| `whippet_match_service_seconds` | Histogram | shard, command type | Matcher service-time distribution. |
| `whippet_gateway_append_seconds` | Histogram | protocol, batch size | Durable ingress latency. |
| `whippet_engine_append_seconds` | Histogram | topic, batch size | Execution/MD append latency. |
| `whippet_snapshot_stage_seconds` | Histogram | stage | Clone/hash/serialize/fsync/rename cost. |
| `whippet_iggy_log_bytes` | Gauge | topic, partition | Storage growth. |
| `whippet_volume_free_bytes` | Gauge | volume | Prevent volume exhaustion. |
| `whippet_ws_subscribers` | Gauge | subscription type | Fan-out scale. |
| `whippet_ws_send_queue_depth` | Gauge | subscription class | Slow consumer visibility. |
| `whippet_ws_coalesced_total` | Counter | subscription class | Conflation/drop semantics. |
| `whippet_md_sequence_gaps_total` | Counter | symbol, consumer | Detect market-data feed gaps. |

Use bounded label cardinality. Do not label metrics with order IDs, account IDs, client IDs or arbitrary user input.

---

## 9. Architecture guardrails

These principles should be kept explicit as the code evolves:

1. **One logical writer per order book.** Parallelise across independent instruments/partitions, not within a single book unless a rigorously sequenced design is introduced.
2. **No broker/network/disk awaits in the matching critical section.** The bounded pipeline is a good separation; preserve it.
3. **Backpressure must be explicit.** Never silently drop accepted commands, executions or required audit events. Market-data UI coalescing is a separate, documented policy.
4. **Durability contracts must be named.** Durable acceptance, matching, execution publication and client notification are different milestones.
5. **At-least-once is not exactly-once.** Use stable event IDs and downstream deduplication unless a real transactional boundary is implemented.
6. **Snapshots are recovery accelerators, not backups.** Maintain an archive/backup plan and verify restore from it.
7. **Never turn on retention without a recovery proof.** Retention, checkpoints and consumer offsets form one system.
8. **Benchmark the whole claimed path.** Matcher ops/s, order commands/s, fills/s, Iggy records/s and broadcast deliveries/s must never be conflated.
9. **Keep a simple known-good profile.** Experimental Iggy and CPU tuning belongs in named benchmark profiles, not silently in the default Compose configuration.
10. **Prove single-node correctness before HA.** Three nodes amplify operational and recovery complexity; they do not repair ambiguous single-node semantics.

---

## 10. Recommended first milestone

Create a milestone titled **“Phase 1.1 — Durable Path Stabilisation”** and assign the first ten quick wins in Section 5. Keep the milestone deliberately small: no new order types, FIX gateway, cross-symbol atomicity or multi-node HA until it exits.

Suggested issue labels:

- `correctness`
- `durability`
- `performance-evidence`
- `observability`
- `operations`
- `market-data`
- `exchange-domain`
- `security`
- `ha-experimental`

A useful first issue should be the retention/recovery design, because it is connected to the current observed latency drift and to the risk of unbounded disk usage. In parallel, land the broken-link/documentation fixes and real lag metrics; these are low-risk and immediately improve the project's credibility.

---

## 11. Source references reviewed

The following repository files are the primary basis for this assessment. Keep these links updated as documents move:

- [README.md](https://github.com/cryptus-neoxys/whipet/blob/main/README.md) — product scope, quickstart, current Phase 1 status and API semantics.
- [CHANGELOG.md](https://github.com/cryptus-neoxys/whipet/blob/main/CHANGELOG.md) — released and unreleased implementation changes.
- [docs/ARCHITECTURE.md](https://github.com/cryptus-neoxys/whipet/blob/main/docs/ARCHITECTURE.md) — runtime, crates, data flow and ownership model.
- [docs/ADR](https://github.com/cryptus-neoxys/whipet/tree/main/docs/adr) — architecture decision records, including the dedicated matcher-thread direction.
- [docs/OPERATIONS.md](https://github.com/cryptus-neoxys/whipet/blob/main/docs/OPERATIONS.md) — config, topics, retention, Iggy tuning, health and metrics.
- [docs/PERFORMANCE.md](https://github.com/cryptus-neoxys/whipet/blob/main/docs/PERFORMANCE.md) — hot-path decisions and optimisation history.
- [docs/BENCHMARKS.md](https://github.com/cryptus-neoxys/whipet/blob/main/docs/BENCHMARKS.md) — measured smoke tests, soak results, workload and benchmark caveats.
- [docs/FAILURE-MODES.md](https://github.com/cryptus-neoxys/whipet/blob/main/docs/FAILURE-MODES.md) — failure guarantees and gaps in fault-injection coverage.
- [deploy/compose.yaml](https://github.com/cryptus-neoxys/whipet/blob/main/deploy/compose.yaml) — local runtime and Iggy deployment configuration.
- [Cargo.toml](https://github.com/cryptus-neoxys/whipet/blob/main/Cargo.toml) — workspace metadata and dependency versions.
- [Pull requests](https://github.com/cryptus-neoxys/whipet/pulls) and [issues](https://github.com/cryptus-neoxys/whipet/issues) — current external work queue.

### Review limitations

This is a public-repository review of `main` and its visible documentation, changelog, benchmark records, operational configuration and selected implementation details as of 15 November 2026. The repository was not cloned and built in this review environment, and no benchmark or test suite was executed locally. Treat source-level conclusions as a reasoned code/documentation review, not a fresh CI result. Before adopting this plan, run the current CI and confirm each cited implementation detail against the exact commit being planned.
