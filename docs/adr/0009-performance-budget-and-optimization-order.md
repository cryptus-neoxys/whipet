# ADR 0009: Performance budget and optimization order

- **Status:** Accepted
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0003](0003-matching-engine-data-structures.md),
  [ADR 0004](0004-iggy-topics-partitions-durability.md),
  [ADR 0006](0006-benchmark-protocol.md),
  [ADR 0007](0007-single-node-v1-cluster-phase-3.md),
  [ADR 0008](0008-security-model-and-scoped-identities.md),
  [PERFORMANCE.md](../PERFORMANCE.md), [BENCHMARKS.md](../BENCHMARKS.md),
  [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

Whippet's Phase 1 durable slice is correct enough to demonstrate, but it has no
headline performance result. The only documented benchmark result is the smoke
run in [BENCHMARKS.md](../BENCHMARKS.md): 200 orders/s, one repetition, 2,000
samples, `dev-loop`. A local four-rate baseline sweep is more useful for
choosing the next optimization, but it is still one repetition per rate, eight
seconds each, on Docker Desktop with the simulator stopped. It is a regression
baseline, not a benchmark.

The baseline, single-order HTTP, `batch_size = 1`:

| Rate | p50 | p99 | Errors | Reading |
|---:|---:|---:|---:|---|
| 200/s | 2,305 µs | 6,307 µs | 0 | Stable enough for a baseline |
| 1,000/s | 1,483 µs | 86,719 µs | 0 | Tail collapse; p50 still low |
| 2,000/s | 88,639 µs | 655,871 µs | 0 | Saturated queue |
| 5,000/s | 1,085,439 µs | 1,675,263 µs | 31,924 | Past the knee; goodput 3,147.6/s |

The shape is a queue, not a matching slowdown. The gateway awaits exactly one
I/O operation before it acknowledges: one Iggy append per order.

After the first tranche (batch route, journal fast paths, engine cache,
snapshot cadence), the same dev-loop sweep measured:

| Rate | Batch | p50 | p99 | Errors |
|---:|---:|---:|---:|---:|
| 1,000/s | 1 | 543 µs | 1,027 µs | 0 |
| 5,000/s | 10 | 1,762 µs | 4,695 µs | 0 |
| 5,000/s | 100 | 8,231 µs | 24,351 µs | 0 |

Batch p50 includes waiting for the batch to fill; per-order amortized p50 is
roughly `p50 / batch`. This is one dev-loop repetition, not a headline result.

### Measured configuration note: Iggy 0.10 profile

A tested Iggy 0.10 profile (`IGGY_MEMORY_POOL_SIZE=1GiB`,
`IGGY_SHARDING_CPU_ALLOCATION=4`, `IGGY_SHARDING_PIN_CORES=false`,
`IGGY_SHARDING_PARTITION_IO_CAPACITY=32`,
`IGGY_PARTITION_WAL_BYTES_MAX=136MiB`) regressed durable ingress on the 16 GB
Docker Desktop VM: at roughly 1,000 orders/s, p50 ~63 ms and p99 ~574 ms,
against default p50 ~1.2 ms and p99 ~120 ms in the no-snapshot isolation run
(a separate run from the four-rate baseline above). The log was large, so this
is a strong signal, not a clean A/B. The default Compose stack keeps Iggy's
default tuning; the optional profile is documented in
[deploy/iggy/iggy.toml](../../deploy/iggy/iggy.toml) and is not the default.
No committed manifest accompanies this note, and these numbers are not a
headline result.

At the same time, the matcher is the part that is most tempting to optimize. It
is a synchronous state machine with an array ladder, an occupancy bitmap, a FIFO
arena, and a dedup table. It is not the measured end-to-end knee. Optimizing it
first would produce a better microbenchmark and no better durable ingress.

The project also has a correctness contract that a performance change can break
without looking like a correctness change:

- the same input log must produce the same state hash;
- replay must reproduce the snapshot plus tail;
- the pipeline is at-least-once, and a crash between output append and offset
  commit can duplicate outputs;
- the topic layout and per-symbol partition keying are a data contract.

This ADR fixes the order of work so that the first optimization targets the
measured knee, every step has a before/after artifact, and no step silently
trades correctness for speed.

## Decision

### 1. Correctness before micro-optimization

A performance change is admissible only after it passes the existing
correctness gates:

- `cargo test --workspace`, including the engine property and golden tests;
- `whippet verify --snapshot-dir data/snapshots --partitions 16`;
- `whippet replay --expect-hash <hex>` against a known log;
- an unchanged state hash for the same input log, unless the change is an
  intentional state-format change with a migration note and a new ADR;
- an unchanged at-least-once contract, or an explicitly documented wider replay
  window in [FAILURE-MODES.md](../FAILURE-MODES.md).

A change that cannot show a before/after manifest is not a performance result.
It may be a correctness or maintainability improvement; it must be labeled as
one.

### 2. Batch ingress and journal fast paths first

The first work targets the measured knee:

- **Gateway batch ingress.** `POST /api/orders/batch`: one symbol per batch, at
  most 256 orders (the Iggy default message-batch limit), one validation pass,
  one command/header construction pass, one Iggy append, contiguous offsets from
  `base_offset`. The whole batch is validated before anything is appended; a bad
  order rejects the batch, not a partial prefix. A mixed-symbol batch is
  rejected because it would require more than one partition and more than one
  append.
- **Harness wiring.** `whippet bench --batch-size N` posts the batch body,
  records `workload.batch_size`, and reports orders/s separately from
  requests/s. The CLI, config, and HTTP body are wired; the dev-loop sweep is
  above.
- **Journal fast paths.** The engine and MD consumers use the payload-only read
  path; the gateway uses the owned append path; routing header keys and topic
  identifiers are cached; appends are chunked at 256; uncertain lanes are
  resolved or abandoned. The engine output path still uses the borrowed append
  because its retry loop must retain the messages; that is the next allocation
  target.
- **Bounded commit coalescing.** Consider committing every K batches or every T
  milliseconds instead of every batch. This reduces broker round trips but
  widens the replay window on a crash. It ships only with a failure-mode review
  and a documented duplicate window.

Batch ingress is a throughput optimization, not a latency optimization. The
first order's ack waits for the whole batch append; the last order sees the
smallest added delay. The ADR does not authorize a claim that batching makes an
individual order faster.

### 3. Engine best-price cache and `FxHash` second

Once the ingress path has a measured before/after, improve the match loop:

- Cache the best bid and best ask ladder indices, update them on insert and
  unlink, and fall back to the occupancy bitmap when the cached level empties.
  Keep a property test that the cached best equals the bitmap scan after every
  insert, cancel, reduce, and unlink.
- Use `FxHash` for the order-index, dedup, and shard maps. The keys are
  BLAKE3-derived order ids and participant ids, not attacker-chosen map keys, and
  the engine is single-writer. Keep the state hash iterating levels and FIFO
  order, never a hash map.
- Measure with the criterion bench at `crates/whippet-engine/benches/engine.rs`
  before and after. If FOK appears in a profile, consider a per-level aggregate
  that excludes a participant's quantity instead of walking every resting order.

This is second because it improves match latency, not the durable-ingress
queue. If match p99 is already a small fraction of the end-to-end envelope,
further engine work is deferred.

### 4. MD latest-frame read path third

Once the ingress and match paths have measurements, fix the MD read path:

- Serve `GET /api/state`, `GET /api/trades`, the initial WebSocket frame, and
  the metrics view from the last published frame where possible, not from a
  fresh full rebuild.
- Build and serialize one frame per 20 Hz tick, then broadcast the serialized
  bytes (or an `Arc`) to subscribers. Today each WebSocket task deep-clones the
  `StateView` and runs `serde_json::to_string` independently, so serialization
  is O(subscribers) per tick.
- Keep the dirty flag and the publish coalescing window. Move the clock read to
  the tick rather than once per applied event.
- If the hub lock shows contention with three or more MD consumers, shard the
  hub by symbol or use an `RwLock`. Do not shard it without a measurement.

This is third because at one subscriber the cost is small. It becomes the knee
at 10+ subscribers, which is a fanout question, not the current demo question.

### 5. TCP/binary ingress and background producer batching later

Only after the first four steps and a native Linux runner:

- Add a binary TCP or QUIC ingress that writes the fixed 78-byte `Command`
  directly, without JSON.
- Add a background producer that batches appends across requests.

This is a new protocol surface: framing, auth, backpressure, partial writes, and
new failure modes. It needs its own ADR. The current evidence points at the
broker round trip, not the JSON parser, so TCP may help less than expected until
the append path is fixed.

### 6. No sub-millisecond durable end-to-end claim

No document, README, slide, dashboard label, or manifest note may claim
sub-millisecond durable end-to-end latency.

- The matching-only design target remains a target. The live
  `whippet_match_latency_microseconds` gauge is not an end-to-end measurement.
- The durable path is millisecond-scale in the baseline, and the baseline is
  `dev-loop`.
- The only path to a headline claim is a native Linux runner, five repetitions,
  a method card, a machine-written manifest, and a stage breakdown that includes
  gateway, append, poll, match, output append, MD apply, state build, and
  delivery. None of that exists today.

## Consequences

Positive:

- The first optimization targets the measured knee instead of the most
  interesting code. The baseline makes the queue visible; the batch path
  addresses it directly.
- Every step has a before/after artifact and a correctness gate, so a speedup
  cannot silently change the state hash or the replay result.
- The order keeps cluster, TLS, encryption, and dashboard work out of the
  critical path until the single-node durable path is stable.
- The ADR makes the "no sub-ms durable claim" rule explicit, so a match-loop
  microbenchmark cannot be promoted into an end-to-end claim.

Negative:

- Batch ingress changes acknowledgement semantics. One append produces N acks;
  the first order's ack pays the whole-batch append; a client that needs
  per-order latency must not use large batches.
- All-or-nothing validation means one bad order rejects the whole batch. That is
  a deliberate trade for one append and contiguous offsets.
- Bounded commit coalescing widens the at-least-once replay window and can
  increase duplicate outputs after a crash. It needs a failure-mode review, not
  just a throughput win.
- `FxHash` is not collision-resistant in general. The threat model here is a
  single-writer engine with BLAKE3-derived keys, not an adversarial map-key
  service, but the assumption must be stated.
- The best-price cache adds invalidation complexity. The bitmap fallback and the
  property test are mandatory, not optional.
- A cached MD frame must be invalidated on every apply and snapshot, and
  freshness must remain meaningful when no new events arrive.
- TCP/binary ingress is a new surface and a new failure model. Deferring it is a
  cost, but doing it first would be a larger bet on an unmeasured cause.
- The "no sub-ms durable claim" rule limits what marketing can say. That is the
  point.

Neutral:

- The order can change if stage timers show a different knee. Changing the order
  requires a new ADR; changing a measurement origin or a publication rule
  requires a new ADR too.
- The baseline is one repetition per rate and cannot support a percentile claim.
  It is enough to choose the next optimization, not enough to publish.
- A native Linux runner is still required for a headline result. This ADR does
  not create one.

## Alternatives considered

- **Optimize the matcher first.** The matcher is the most interesting code and
  has the clearest microbenchmark. Rejected: it is not the measured end-to-end
  knee, and a faster match loop would not move the 1,000/s p99 or the 2,000/s
  p50 in the baseline.
- **TCP/binary ingress first.** It removes JSON and HTTP, which sounds like the
  obvious win. Rejected for now: it is a larger protocol surface, and the
  baseline does not isolate JSON from the broker append. Batch the existing HTTP
  path first; then measure whether transport is still material.
- **Cluster consensus first.** Rejected: VSR is Phase 3, it is not implemented,
  and it changes the durability model rather than the per-order cost.
- **TLS or stream encryption first.** Rejected: both are Phase 2 security work.
  They add per-byte cost, but the measured knee is the append round trip, and
  neither path exists to measure.
- **Keep one append per order and tune the broker.** Rejected as the first move:
  the baseline shows the queue collapsing at 1,000–2,000 orders/s, and the
  per-order round trip is the structural cost. Broker tuning may help later, but
  it does not remove the round trip.
- **Tune the Iggy memory pool, sharding, and WAL first.** Rejected for now: the
  tested 1 GiB / 4-shard / 136 MiB WAL profile regressed durable ingress on the
  16 GB Docker Desktop VM, and the log was large, so the comparison is
  confounded. Revisit it with a clean log and a controlled A/B after batch
  ingress and the journal fast paths.
- **Commit every order for the smallest replay window.** Rejected: it adds a
  third broker round trip per order at low rates. The current batch commit is a
  better default; coalescing is a measured trade, not a default.
- **Claim sub-millisecond durable latency from the match gauge.** Rejected:
  matching-only is not end-to-end. The durable path includes the gateway append,
  the engine poll, the output appends, the MD poll, the hub, and the delivery.
- **Publish the baseline sweep as a headline result.** Rejected: it is
  `dev-loop`, one repetition, eight seconds, with no method card and no
  committed manifest. It is a regression baseline.
- **Use a generic HTTP load generator (`wrk2`, `oha`) for the system run.**
  Rejected for the same reason as [ADR 0006](0006-benchmark-protocol.md): it
  cannot generate Whippet order semantics, per-symbol mixes, or a manifest with
  the exchange's mode and durability fields. It remains a useful cross-check for
  a pure HTTP throughput number, not the primary harness.

## Status

Accepted. This ADR records the order of work and the correctness and measurement
gates, not a set of results. Implementation is incremental. The first tranche
(batch route, payload-only journal reads, header-key cache, owned append,
best-price cache, `FxHash`, MD latest-frame accessor) may land before the
before/after measurements exist; until those measurements exist, none of it is a
performance claim.

Any change to the optimization order, the measurement origin, or the rule
against a sub-millisecond durable claim requires a new ADR that supersedes this
one.
