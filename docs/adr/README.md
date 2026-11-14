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
| [0008](0008-security-model-and-scoped-identities.md) | Security model and scoped identities | Accepted |

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
