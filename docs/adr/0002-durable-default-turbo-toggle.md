# ADR 0002: Durable by default, turbo behind an explicit Phase 2 toggle

- **Status:** Accepted; `durable` implemented in Phase 1, `turbo` deferred to Phase 2
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md), [ADR 0007](0007-single-node-v1-cluster-phase-3.md)

## Context

Whippet exists to make the latency/durability trade-off visible. Two paths are
possible:

- **Durable:** the gateway appends an order to the Iggy `orders` topic and
  acknowledges after the write. The matcher consumes the partition in order and
  publishes executions and market data back to Iggy. The log is the source of
  truth; replay from the log alone is possible.
- **Turbo:** the gateway hands the order to a local sequencer and the matcher
  decides immediately. Executions and market data go to the live fanout and are
  journaled to Iggy asynchronously. The gateway acknowledges after the match.
  The log still receives every order and execution, but there is a bounded
  in-flight window that a node crash can lose.

The honest headline is not "sub-millisecond end-to-end on Iggy." Iggy's own
published durable numbers are low-millisecond on Linux, and a laptop Docker VM
is slower still. The honest headline is "sub-millisecond matching,
millisecond-durable streaming," with two labeled modes.

Phase 1 needed one path that could be built, tested, and explained without
pretending the other path already existed.

## Decision

**`durable` is the default and the only enabled mode in 0.1.0.** `turbo` is
Phase 2.

The mode is represented in `whippet_core::config::Mode`:

- `Mode::Durable` is the `Default`.
- `Mode::Turbo` exists in the type so the protocol, dashboard badge, and
  guarantee strings do not need a breaking change later.
- `NodeConfig::from_args` rejects `--mode turbo` with an explicit message:
  "turbo mode is Phase 2; the durable slice is the default and only enabled
  path in this release."

The durable path is:

```text
client -> gateway POST /api/orders
       -> Iggy whippet-trading/orders (partition = symbol)
       -> engine shard poll
       -> deterministic match
       -> Iggy executions + md
       -> MD consumer -> MdHub -> 20 Hz WS state
```

The gateway acknowledgement is `accepted_by_log`: it means the order is in the
Iggy log at the returned partition and offset, not that it has matched. The
dashboard shows the durable ack time and the match latency separately.

The turbo trade-off, when it lands, is explicit:

| Property | `durable` | `turbo` |
|---|---|---|
| Acknowledgement point | After Iggy append | After match |
| Log position in the ack path | Yes | No |
| Replayable from the log | Yes | Yes, once the write-behind catches up |
| Acked order lost if the node dies | Within Iggy's durability contract, not on a clean single-node write | Possible for the unjournaled in-flight window |
| Matching semantics | Identical | Identical |
| Intended claim | Low-single-digit ms on Linux; replayable | Sub-millisecond target on Linux; bounded loss window |

The mode changes sequencing and acknowledgement, never matching semantics. A
limit order that would rest in `durable` rests in `turbo`; a FOK that would
reject in `durable` rejects in `turbo`. This is what makes the toggle a fair
comparison rather than two products.

## Consequences

Positive:

- Phase 1 can make a claim it can prove: every acknowledged order is in the
  log, and the log can be replayed.
- Recovery is simpler. The engine restarts from a snapshot plus the committed
  offset; there is no separate unjournaled state to reconcile.
- The dashboard's mode badge and guarantee text already exist, so Phase 2 can
  add turbo without changing the wire contract.
- Benchmark results can label the mode, which prevents accidental comparisons
  between two different durability contracts.

Negative:

- Durable ingress pays at least one Iggy round trip before the client sees an
  ack. On a laptop this is visible; on a native Linux host it is the intended
  low-millisecond path, not a sub-millisecond one.
- The demo is less flashy than a turbo-only demo. That is deliberate.
- `Mode::Turbo` is dead code in 0.1.0 except for the rejection path and the
  dashboard's type surface. Dead code that documents a contract is acceptable;
  a half-working turbo path is not.
- Phase 2 must build and test a second acknowledgement path, a bounded ring,
  and a write-behind journal. That is real work, not a flag flip.

## Alternatives considered

- **Turbo by default.** Better demo latency, weaker integrity story, and the
  in-flight loss window would be the first thing a reviewer asks about.
  Rejected: the project's credibility depends on not hiding that trade-off.
- **Durable only, forever.** Simpler, but it gives up the most interesting part
  of the showcase and cannot support the sub-millisecond matching claim
  end-to-end. Rejected as a permanent position; accepted for Phase 1.
- **Two separate binaries or deployments.** Clean isolation, but doubles the
  packaging and documentation surface before there is a reason. Rejected for
  now.
- **Per-order client choice between modes.** Tempting, but it makes the
  guarantee matrix per-request and much harder to explain. A node-level mode is
  easier to test and to label on a chart. Rejected for Phase 2 planning.
- **Call durable "exactly-once."** False. The pipeline is at-least-once with
  business dedup in the engine. Rejected.

## Status

Accepted. `durable` is implemented and is the default in 0.1.0. `turbo` is
**Phase 2** and must not be described as available. Before turbo ships it needs:
a bounded SPSC ingress ring, a local sequencer, a write-behind journal with a
visible window size, a mode-aware benchmark, and a guarantee statement that
names the loss window.
