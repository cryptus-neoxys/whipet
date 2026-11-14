# ADR 0001: Workspace and crate boundaries

- **Status:** Accepted
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0003](0003-matching-engine-data-structures.md), [ADR 0004](0004-iggy-topics-partitions-durability.md)

## Context

Whippet is a single-node showcase in Phase 1. The repository must support four
different jobs at once:

1. a deterministic, allocation-free matching core that can be tested without a
   broker, a clock, or a network;
2. an Apache Iggy adapter that is the only place in the system allowed to own
   async I/O and an Iggy client;
3. a market-data cache and browser fanout path;
4. developer tooling: a simulator, a benchmark harness, a CLI, and a dashboard.

The research blueprint proposed separate `whippet-gateway`, `whippet-md`,
`whippet-surveillance`, and `whippet-audit` crates. Phase 1 did not need that
many deployable units. It did need hard compile-time boundaries around the
matcher and the Iggy edge, because those are the parts most likely to acquire
accidental dependencies.

The workspace uses Rust 1.95, edition 2024, and one pinned Iggy SDK
(`iggy = "=0.12.0"`). A workspace member that depends on `iggy` is making a
statement about where async I/O lives.

## Decision

Keep the workspace small and make the dependency direction explicit:

```text
whippet-core
  ├── whippet-engine
  ├── whippet-journal
  ├── whippet-marketdata
  ├── whippet-sim
  └── whippet-bench

whippet-testkit
  ├── whippet-core
  └── whippet-engine

whippet (binary + orchestration)
  ├── whippet-core
  ├── whippet-engine
  ├── whippet-journal
  ├── whippet-marketdata
  ├── whippet-sim
  └── whippet-bench
```

`whippet-engine` has a dev-dependency on `whippet-testkit`, which in turn
depends on `whippet-engine`. That is an intentional test-only cycle: Cargo
allows it, the normal dependency graph stays acyclic, and the shared strategies
are worth more than a strict layering diagram. `TestNode` lives in
`crates/whippet/tests/common/mod.rs` because it needs `NodeContext` from the
`whippet` crate; putting it in the testkit would create a real cycle.

The members and their responsibilities are:

| Crate | Responsibility | Forbidden dependencies |
|---|---|---|
| `whippet-core` | Fixed-layout protocol codec, IDs, instrument and exchange config, API DTOs, message headers, benchmark manifest schema | `tokio`, `iggy`, `axum`, wall-clock reads in codecs |
| `whippet-engine` | Price ladder, order book, deterministic matcher, dedup table, snapshots, state hash | `tokio`, `iggy`, `axum`, `tracing`, wall-clock reads |
| `whippet-journal` | The only Iggy client: stream/topic bootstrap, append, poll, manual offset commit; an in-memory journal for tests | `whippet-engine`, `whippet-marketdata` |
| `whippet-marketdata` | Per-symbol market-data cache, conflated state publisher, in-process metrics and HDR histograms | `iggy`, `whippet-engine` |
| `whippet-sim` | Seeded scenario generator; emits `OrderRequest`/`CancelRequest` values | `tokio`, `iggy`, `whippet` |
| `whippet-bench` | Open-loop HTTP harness, HDR summaries, manifest emission | `iggy`, `whippet-engine` |
| `whippet-testkit` | Shared command builders, a golden command log with a stable state hash, and reusable proptest strategies | `tokio`, `iggy`, `axum`, `whippet` |
| `whippet` | Binary, CLI, node orchestration, gateway HTTP/WS, engine and MD runners, snapshot store | Business logic that belongs in a library crate |

The gateway is a module (`crates/whippet/src/gateway.rs`), not a crate, in
Phase 1. It is still a separate task boundary: it owns client connections,
validation, and the Iggy append, while the engine consumes the log.

Two rules matter more than the table:

- **Only `whippet-journal` may import `iggy`.** If another crate needs a
  journal operation, it goes through the `Journal` enum or a new method on the
  adapter, not through a direct SDK call.
- **`whippet-engine` is a pure state machine.** It takes a `Command` and an
  input offset, writes into a caller-owned `EngineOutput`, and returns. It does
  not read the clock, spawn tasks, log per message, or allocate in the match
  loop.

## Consequences

Positive:

- The matcher can be tested with `cargo test -p whippet-engine` and property
  tests without Docker, Iggy, or a Tokio runtime.
- The Iggy dependency is auditable in one crate. Upgrading the pinned SDK is a
  contained change.
- The memory journal lets the gateway and engine runners be exercised in CI
  without a broker (`WHIPPET_JOURNAL=memory`).
- The normal dependency graph is acyclic and shallow, which keeps incremental
  builds fast enough for a single-maintainer project. The engine's test-only
  dependency on the testkit is the one deliberate exception.

Negative:

- `whippet` is a large orchestration crate. Gateway, node, runners, snapshots,
  and CLI share one compile unit.
- `whippet-testkit` depends on `whippet-engine`, and the engine's tests depend on
  the testkit. The cycle is dev-only and intentional, but it means the testkit
  cannot be used to test the engine's private internals without care.
- The gateway cannot be deployed or scaled independently. That is acceptable
  for a single-node demo and will be revisited when HA lands.
- The crate names do not match the research blueprint one-for-one, so older
  design documents need a mapping when read alongside the code.

## Alternatives considered

- **One crate for everything.** Simplest to start, but it would let the matcher
  reach for Tokio or Iggy without a compile error. Rejected because the
  determinism boundary is the main architectural claim.
- **A microcrate per component, including gateway and MD fanout.** Closer to the
  research blueprint, but it adds manifest churn and cross-crate boilerplate
  before there is a second deployable unit. Rejected for Phase 1; revisit if
  gateway or fanout needs independent deployment.
- **A separate gateway binary/service now.** Would make the latency path more
  realistic, but it adds a network hop and a second process to the demo before
  the durable slice is proven. Rejected for Phase 1.
- **Put protocol types in `whippet` and let libraries depend on the binary
  crate.** Rejected: library crates must not depend on a binary crate, and the
  protocol is the shared contract.

## Status

Accepted for Phase 1. Revisit when any of the following happens: the gateway
becomes independently deployable, HA/3-node work starts, or `whippet-testkit`
needs a shared integration fixture that would turn its dev-only cycle into a
real one. Any new workspace member or a change to the "only `whippet-journal`
imports `iggy`" rule requires a new ADR.
