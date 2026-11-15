# ADR 0026: Engine-enforced per-participant risk limits

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0015](0015-audited-instrument-control-plane.md) (operator
  control plane), [ADR 0017](0017-ingress-risk-and-position-reconstruction.md)
  (ingress guard and positions), [ADR 0018](0018-instrument-session-state.md)
  (log-ordered instrument state), [ADR 0021](0021-durable-path-state-machines.md)
  (snapshot/commit state machines), [ADR 0023](0023-resume-points-and-feed-baselines.md)
  (resume points), [INVARIANTS.md](../INVARIANTS.md),
  [OPERATIONS.md](../OPERATIONS.md)

## Context

The only pre-trade risk in the system was a stateless ingress filter: a
per-order quantity cap and a per-order notional cap applied at the gateway and
TCP listener. It cannot see a participant's accumulated position or resting
interest, so a participant could stay inside every per-order cap and still
build unbounded exposure across many orders. It was also bypassable by any
producer that wrote directly to the orders log.

Risk is state, not a request property. It has to be ordered with the commands
that create exposure, survive replay, and be enforced by the same component
that owns the book. The engine is the only place where "this order would take
the participant past its limit" is decidable without racing the matcher.

## Decision

Per-participant limits are engine state, changed by an ordered
`CommandKind::SetRisk` record on the same orders log as client traffic.

### Command layout

`SetRisk` reuses the fixed `Command` layout, like halt/resume/session:

- `participant` is the target participant;
- `qty` carries `max_net_qty` (absolute position bound; `0` disables);
- `price` carries `max_open_buy_notional_cents` in quote cents (`0` disables;
  negative is invalid and ignored on replay);
- `side`, `order_type`, `order_id`, `nonce`, and `flags` keep their canonical
  control values.

The engine applies the command at its log position. Replacing limits preserves
the participant's existing exposure, so tightening a bound immediately gates
new risk without inventing or erasing a position already taken. Clearing every
bound prunes a flat entry; a participant with no entry is unrestricted.

### Admission

`RiskTable::check_new_order` runs after the order's own validity, session,
halt, and fillability checks, and before the dedup id is accepted. A refusal
produces one `RejectReason::RiskLimit` report and mutates nothing: no book,
dedup, MD, or exposure change.

The checks are deliberately conservative:

- **Net quantity.** A buy candidate is charged against `net + open buy + qty`;
  a sell candidate against `net - open sell - qty`. Both the long and short
  side of the bound are checked, so a participant may cover a short and go long
  up to the same limit. A resting order counts because it may fill; the
  candidate counts its full quantity.
- **Resting buy notional.** Only `Limit` and `PostOnly` buys are charged, and a
  crossing limit is charged its full quantity before matching. Admission never
  relies on an execution that has not happened yet. Market/IOC/FOK buys add no
  resting notional.

### Exposure

Exposure moves only with the book and fills, never from an out-of-band
mutation:

- a new resting order adds its remaining quantity (and, for a buy, its quote
  notional);
- a client cancel or self-trade-prevention removal subtracts the removed
  remainder;
- a taker fill moves `net_qty` by the fill;
- a maker fill removes the filled quantity from open interest and moves
  `net_qty` by the same amount, so a partial maker fill reduces open exposure
  exactly once;
- an opposite fill that nets a flat, unrestricted entry to zero prunes it.

The risk table is a `BTreeMap` keyed by participant so hashing is
insertion-order independent.

### Hash and snapshot

Risk limits and exposure are part of the engine state. The hash domain is
bumped to `whippet-engine-v2` and the tail covers the risk table, so a restored
engine that dropped a `SetRisk` cannot hash like the live engine. Snapshot
format v6 adds the per-symbol risk table; v5 and older are rejected with
`BadVersion` and there is no compatibility shim.

On restore and in `SymbolEngine::check_invariants`, the risk table checks its
own ordering/emptiness rules and re-derives every participant's open quantity
and buy notional from the book at the instrument's tick size. Net position is
not derivable from resting orders, so it is checked by conservation in the
engine's own mutation paths, not by the book mirror.

### Control plane and audit

`POST /api/admin/instruments/{symbol}/risk` requires an operator token and
accepts:

```json
{
  "participant": "desk-a",
  "max_net_qty": 500,
  "max_open_buy_notional_cents": 25000000,
  "reason": "desk limit"
}
```

Both bounds are required: omitting one would otherwise silently disable it.
The command is appended to the orders log first (durable group commit or turbo
lane), then one audit record with `action: "set_risk"`, `participant`,
`max_net_qty`, `max_open_buy_notional_cents`, `reason`, and `order_offset` is
appended to the audit topic. The ack carries the same three risk fields; the
halt/session fields are absent because the action does not change them. TCP
`Submit` rejects `SetRisk`, as it does the other operator commands.

## Alternatives rejected

- **Gateway-only counters:** not ordered with matching, not replayed, and two
  gateway instances would each hold half the truth. A direct log producer
  bypasses them entirely.
- **Risk command on a separate topic:** no shared order with the orders it
  gates; a replay could admit an order before the limit that should have
  refused it.
- **Per-order caps only:** cannot bound accumulated exposure; the exact gap
  this ADR closes.
- **Cash/margin accounting in the matcher:** needs fees, funding, and
  settlement, none of which exist. Quantity and resting-buy-notional bounds
  are the parts that are decidable from the book and the log today.

## Consequences

- Risk changes consume an orders-log offset and are part of the state hash;
  they are rare, but they are history and must be audited.
- v5 snapshots are rejected; upgrade means reset or full replay from an empty
  log, exactly like the v5 session cut.
- The golden hash moves for the deliberate domain/table change; the standard
  log contains no `SetRisk`, so matching and sequencing are unchanged.
- The ingress guard remains as a cheap stateless filter. It is not the
  authority for accumulated risk, and the docs say so.
- Not covered: cash, margin, fees, funding, cross-symbol aggregation,
  short-borrow constraints, and per-order price collars beyond the existing
  market collar. Risk is enforced per participant per symbol.
