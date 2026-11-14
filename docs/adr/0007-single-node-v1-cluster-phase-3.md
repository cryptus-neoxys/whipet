# ADR 0007: Single-node v1, cluster in Phase 3

- **Status:** Accepted
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md), [ADR 0006](0006-benchmark-protocol.md), [ADR 0008](0008-security-model-and-scoped-identities.md)

## Context

A 3-node Apache Iggy cluster with RF3 is the right long-term demonstration of
the durability story: zero acknowledged loss across one node failure, a real
primary election, and a visible failover window. It is also a large amount of
work: static addressing, shared secrets, identical TOML, VSR behavior, volume
handling, random primary kills, proof artifacts, and a machine with enough
memory to run three nodes plus the gateway, engine, dashboard, and simulator.

Phase 1 had none of that. It had one node, one Iggy process, one gateway, and a
16 GB laptop. Building HA first would have delayed the durable vertical slice
and produced a demo that was harder to debug than the thing it was meant to
showcase.

The project also has a credibility rule: do not claim cluster results that were
never measured. A "3-node" badge on a single-node process would be worse than no
cluster at all.

## Decision

**0.1.0 is single-node. The 3-node HA profile is Phase 3.**

What "single-node" means concretely:

- One `whippet node` process and one Iggy server. The gateway, engine shards,
  MD consumers, and dashboard API share the process.
- One copy of each partition. `persisted` means the local Iggy log is durable;
  it does **not** mean replicated across nodes.
- No supported cluster profile, no VSR claim, no failover test, and no
  zero-loss-across-a-node-failure claim. `deploy/compose.ha.yaml` is an
  **experimental, unvalidated** three-node skeleton: it has not been run
  end-to-end, the Phase 1 client is not cluster-aware, and it must not be
  described as tested HA.
- The topic layout, partition count, manual-commit policy, and snapshot format
  are written so a future cluster profile can reuse them, but Phase 1 does not
  exercise the cluster path.
- The README, dashboard, and docs must say "single-node" wherever durability or
  availability is discussed. "Durable" is not a synonym for "highly available."

Phase 3 adds:

- a 3-node Iggy profile with RF3 and identical configuration;
- `durability=persisted` and `consumer_offset_durability=persisted` on the
  business topics, with manual commits;
- a static-address Compose profile (service-name discovery is not assumed);
- random-phase kills of the actual partition primary, not just node 0;
- proof artifacts: acked-offset ledger, readable offsets after failover,
  duplicate-effect count, state hash before/after, time-to-lag-zero;
- a failover section in the benchmark manifest and docs, labeled separately
  from single-node latency.

Phase 2 may land turbo, fanout/conflation, surveillance, audit, and the security
work before Phase 3. The cluster does not have to wait for all of Phase 2, but it
must not ship without the security baseline in
[ADR 0008](0008-security-model-and-scoped-identities.md).

## Consequences

Positive:

- The Phase 1 demo is debuggable on one machine. A failure has one log, one
  process, and one set of snapshots.
- The project can make a precise claim: durable on one node, replayable from the
  log, no HA claim.
- The topic and commit design is cluster-ready, so Phase 3 is an operational
  profile plus proof work, not a protocol rewrite.
- The laptop does not need to run three Iggy nodes, Prometheus, Grafana, and a
  simulator at once.

Negative:

- The strongest durability demo, zero acknowledged loss across one node
  failure, is not available in 0.1.0.
- "Durable" can be misread as "replicated." The documentation has to repeat the
  distinction, and the dashboard must not imply HA.
- Phase 3 will expose assumptions that a single node hides: static addressing,
  quorum behavior, leader routing, volume recovery, and clock/latency
  differences between nodes.
- A cluster profile is not just a Compose file. It needs a test plan and
  artifacts, or it is a screenshot, not a result.

## Alternatives considered

- **Build HA from day one.** Rejected: too much operational surface before the
  durable slice worked, and the laptop cannot comfortably host the full stack.
- **Ship single-node forever.** Rejected: the durability story needs a cluster
  eventually, and the research and upstream work already point at VSR.
- **Run a 3-node cluster with default credentials and no TLS to save time.**
  Rejected: it would teach the wrong lesson and create a security mess that
  Phase 2 would have to undo.
- **Call a single-node `replicated` topic "HA."** Rejected: false. The topic
  metadata requests `replicated`, but one node cannot replicate.
- **Use a hosted/managed Iggy cluster for the demo.** Rejected: the project is
  an independent showcase and must be reproducible locally.

## Status

Accepted. 0.1.0 is single-node. No cluster result exists, and none may be
published until the Phase 3 exit criteria above are met. Phase 3 starts only
after the Phase 2 security baseline and a machine profile that can run three
nodes with documented memory limits.
