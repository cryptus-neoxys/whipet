# Architecture Decision Records

An ADR is required for changes to matching semantics, wire protocol, topic layout, durability
policy, security model, or benchmark protocol. ADRs are immutable once accepted; a later ADR
supersedes an earlier one.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-workspace-and-crate-boundaries.md) | Workspace and crate boundaries | Accepted |
| [0002](0002-durable-default-turbo-toggle.md) | Durable default, turbo toggle | Accepted |
| [0003](0003-matching-engine-data-structures.md) | Matching engine data structures | Accepted |
| [0004](0004-iggy-topics-partitions-durability.md) | Iggy topics, partitions, durability | Accepted |
| [0005](0005-dashboard-stack-and-rendering.md) | Dashboard stack and rendering | Accepted |
| [0006](0006-benchmark-protocol.md) | Benchmark protocol | Accepted |
| [0007](0007-single-node-v1-cluster-phase-3.md) | Single-node v1, cluster Phase 3 | Accepted |
| [0008](0008-security-model-and-scoped-identities.md) | Security model and scoped identities | Accepted for Phase 1 demo posture; Phase 2 target proposed |
| [0009](0009-performance-budget-and-optimization-order.md) | Performance budget and optimization order | Accepted |
| [0010](0010-shard-thread-and-journal-pipeline.md) | Shard thread and journal pipeline | Accepted — implemented (fault-injection tests pending) |
| [0011](0011-fault-injection-and-stage-budget.md) | Fault injection, stage budgets, and soak gates | Proposed — implementation in progress |
| [0012](0012-per-role-journals-and-ingress-guards.md) | Per-role + per-shard engine journal pool, shared ingress guards, and snapshot verification | Accepted — implemented |
| [0013](0013-turbo-mode.md) | Turbo mode: local sequencer, direct fanout, bounded uncommitted window | Accepted (implemented; startup anchor, tail replay, per-shard window gauge) |
| [0014](0014-bounded-storage-and-replay-safe-retention.md) | Bounded storage guard and replay-safe retention rule | Proposed (guard implemented; deletion rule design-only — never delete without consumer + verified-baseline proof) |
| [0015](0015-audited-instrument-control-plane.md) | Audited instrument halt/resume control plane | Accepted (implemented; log-ordered commands, operator tokens, audit topic) |
| [0016](0016-sequenced-price-level-market-data-feed.md) | Sequenced price-level market-data feed | Accepted (implemented; `/ws/md`, snapshot + deltas + resync) |
| [0017](0017-ingress-risk-and-position-reconstruction.md) | Ingress pre-trade risk and read-only position reconstruction | Accepted (per-order qty/notional guard + `whippet positions`; engine-enforced risk/ledger proposed) |
| [0018](0018-instrument-session-state.md) | Instrument session state | Accepted (log-ordered `set_session`, snapshot v5, hub/dashboard visibility) |
| [0019](0019-executable-engine-invariants.md) | Executable engine invariants and the order lifecycle | Accepted (book/dedup/engine checks, dedup transition table) |
| [0020](0020-explicit-shard-lifecycle.md) | Explicit shard lifecycle and readiness | Accepted (per-role state machine, readiness = all engine+MD running) |

Template:

```
# NNNN — Title

- Status: Proposed | Accepted | Superseded by NNNN
- Date: YYYY-MM-DD
- Deciders: ...
- Related: ...

## Context
## Decision
## Consequences
## Alternatives considered
```
