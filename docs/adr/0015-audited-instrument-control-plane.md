# ADR 0015: Audited instrument control plane

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md) (topic layout),
  [ADR 0009](0009-performance-budget-and-optimization-order.md) (hot-path discipline),
  [ADR 0010](0010-shard-thread-and-journal-pipeline.md) (shard pipeline),
  [ADR 0012](0012-per-role-journals-and-ingress-guards.md) (ingress guards),
  [ADR 0014](0014-bounded-storage-and-replay-safe-retention.md) (bounded storage),
  [ROADMAP Phase C](../ROADMAP-AND-IMPROVEMENT-PLAN.md),
  [ARCHITECTURE.md](../ARCHITECTURE.md), [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

The engine has always carried a `halted` flag in its state and snapshots, but
there was no command path to set it: an operator could only change it out of
band, which is neither authorized nor replayable. The reserved `commands`,
`audit`, and `alerts` topics existed but had no producers. The improvement plan
makes the instrument control plane the first Phase C item, with the explicit
precondition that command ownership, authorization, ordering, and replay be
defined before those topics become active.

An instrument halt is a risk control, not a performance feature. It must be
impossible for a trading client to halt a market, impossible for a halt to be
applied outside the ordered log, and impossible for a restart to forget a halt
that the log recorded.

## Decision

### Commands are log-ordered, not out-of-band

Halt and resume are `CommandKind::Halt` and `CommandKind::Resume` in the same
`Command` wire layout, appended to the **orders** topic on the symbol's
partition. The matcher processes them in log order with every other command;
the engine's `halted` flag, `last_offset`, `last_ts_ns`, `commands`, and MD
sequence all advance deterministically. Replaying the orders log from zero (or
from a snapshot plus tail) reproduces the halt state, and the state hash covers
it. A separate `commands` topic would introduce a second ordering domain with
no atomic relationship to the orders it gates, so it is not used for this.

The engine semantics are:

- Halt rejects **new orders** with `RejectReason::Halted` and emits a
  `MdKind::Status` event with `MD_FLAG_HALTED`.
- Cancels remain allowed while halted, so participants can withdraw risk.
  Resume is never blocked.
- Repeating a halt (or resuming a running instrument) is book-idempotent but
  re-emits Status for operator confirmation.
- The state snapshot already carries `halted`; the MD snapshot carries a
  trailing status byte (`MD_FLAG_HALTED`) so the MD hub learns a restored halt
  state from the same startup append. Older MD snapshots decode as running.

### Authorization is a separate operator namespace

`WHIPPET_OPERATOR_TOKENS` holds `token:operator-name` pairs. Operator tokens
are **not** trading tokens: they cannot submit orders, and trading tokens
cannot reach the admin endpoints. A node with no operator tokens returns 403
`operator_not_configured` for every admin request, checked before any
credential inspection. A missing or invalid token returns 401. Tokens are
compared constant-time over the full set.

The endpoints are HTTP-only:

```text
POST /api/admin/instruments/{symbol}/halt
POST /api/admin/instruments/{symbol}/resume
```

The binary TCP order channel rejects `Halt`/`Resume` with `InvalidCommand`; a
TCP submit must never be able to halt a market. The command's participant is
`ParticipantId::derive(operator_name)`, so the engine's existing participant
model applies and the operator identity is visible in execution/audit paths.

### Audit is append-only and explicit about failure

Every accepted admin request appends one JSON record to the `audit` topic at
`symbol.partition(4)` with:

`operator`, `action`, `symbol`, `halted`, `reason`, `order_offset`,
`timestamp_ns`, `service`.

The order-log command is appended first: it is the source of truth. If the
audit append then fails, the endpoint returns 500 `audit_unavailable` and says
the command may already be applied; the node logs the operator, action, symbol,
and order offset. The audit record being missing is an operational incident,
not a silent success. A retried request produces another audit record; the
halt/resume itself is idempotent.

### Turbo mode

A turbo node sends the operator command through the same bounded lane as an
order (`turbo_enqueue`/`turbo_await`), so the matcher's single sequencer
assigns its seq and the write-behind journals it. The audit append still goes
directly to the audit topic. A turbo halt is acknowledged after matching with
`matched_unjournaled`, like any other turbo command.

### Dashboard

`BookView.halted` is fed by the MD hub from Status events, and the dashboard
shows a text `HALTED` badge on the book and symbol list. The badge is
accessible (text, not color-only).

## Alternatives rejected

- **Direct in-memory `set_halted`:** fast, but not authorized, not ordered with
  orders, and not replayed; removed as a production path.
- **Separate control topic:** a halt and a concurrent order have no shared
  order, so "halt took effect before order X" is undefined.
- **Reusing trading tokens for admin:** conflates order submission with risk
  control and gives every trading client halt authority.
- **TCP admin frames:** the TCP channel's credential scope is order submission;
  adding a second credential class there widens the attack surface without a
  demonstrated need.
- **MD-only status flip:** the MD topic is derived output; the authoritative
  state must be the orders log and the engine, not the feed.

## Consequences

- A halt takes effect at its position in the log, not at request arrival. In
  durable mode that includes the append latency; in turbo it is the match.
- Operator commands consume an order offset/seq and are visible in the orders
  log, the state hash, and the audit topic. Capacity planning must include
  them (they are rare).
- A lost response can produce a duplicate audit record; consumers must treat
  audit records as at-least-once and deduplicate by the order-log offset or
  `(operator, action, order_offset)` if needed.
- `commands`, `alerts`, and richer session/price-band controls are still
  future work; this ADR covers halt/resume only.
- The audit topic has no query CLI yet; it is readable through the journal and
  will be surfaced by the surveillance/audit phase.
