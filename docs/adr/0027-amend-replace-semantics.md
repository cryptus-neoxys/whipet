# ADR 0027: Amend/replace semantics

- **Status:** Accepted (implemented 2026-11-19)
- **Date:** 2026-11-19
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md) (ordered
  input log), [ADR 0018](0018-instrument-session-state.md) (session state),
  [ADR 0019](0019-executable-engine-invariants.md) (order lifecycle),
  [ADR 0024](0024-execution-output-invariants.md) (execution provenance),
  [ADR 0026](0026-engine-enforced-risk-limits.md) (risk limits),
  [INVARIANTS.md](../INVARIANTS.md), [EXCHANGE-101.md](../EXCHANGE-101.md)

## Context

The engine could create and cancel an order but not change one. A participant
that wanted a different price or a smaller quantity had to cancel and submit a
new order. That is two log records, two gateway requests, two risk admissions,
and a window in which the order does not exist. It also mints a new order id,
so downstream projections cannot relate the replacement to the original.

The engine already owns the resting order, its risk exposure, the execution
sequence, and the market-data sequence. The change has to be ordered with them:
a replay must reach the same book, risk table, and output sequence without
consulting a side channel.

## Decision

`CommandKind::Amend = 7` is an **atomic cancel-replace** of one resting order.
The order keeps its id, loses time priority, and is reinserted at the tail of
its new price level.

### Command layout

`Amend` reuses the fixed `Command` layout; [`COMMAND_ENCODED_LEN`] is unchanged:

- `symbol` is the instrument;
- `order_id` is the resting order to change;
- `participant` is the owner; a non-owner is treated as an unknown order;
- `price` is the new limit price in ladder ticks;
- `qty` is the new **remaining** quantity;
- `side`, `order_type`, `nonce`, and `flags` keep their canonical control
  values. The book, not the request, is authoritative for the order's side.

The gateway endpoint is `POST /api/amend`:

```json
{
  "symbol": "BTC-USD",
  "order_id": "5f0c...",
  "price_cents": 6750000,
  "qty": 3,
  "participant": "desk-a"
}
```

`price_cents` is converted to ticks with the instrument's configured tick size,
exactly like a new order.

### Admission rules

An amend is refused, with no state change, when:

- the order is not resting on this instrument, or the participant does not own
  it (`unknown_order`);
- the instrument is halted (`halted`) or its session is not `open`
  (`session_not_open`). An amend can raise resting buy notional by moving a bid
  up, so it is new risk, not risk withdrawal. Cancels remain available in every
  state;
- the new price is not positive (`invalid_price`) or outside the ladder
  (`price_out_of_band`);
- the new quantity is zero (`invalid_qty`) or greater than the order's current
  remaining quantity (`amend_qty_increase`). Increasing size is a new order;
  it must not be smuggled through the amend path;
- the new price would cross the opposite best (`amend_would_cross`). Amend is a
  resting-order operation, never a hidden taker. A participant that wants to
  cross cancels and submits a new order;
- the engine risk table refuses the projected exposure (`risk_limit`).

The no-crossing rule also prevents a self-trade on amend: the replacement can
never be marketable, so it cannot meet a resting order, including the
participant's own.

### Atomicity

The engine validates the whole amendment before touching the book. On success:

1. the book unlinks the old order from its level and relinks the same id at the
   tail of the new level, with the new quantity and the amend's execution
   sequence;
2. the risk table releases the old open contribution and charges the new one;
3. one `ExecKind::Amended` execution report carries the new price and quantity;
4. one `MdKind::Replace` market-data event carries the same id and new state.

On any refusal the book, dedup table, risk table, and market-data sequence are
untouched. A refusal emits one `Rejected` report and **does not overwrite the
resting order's dedup record**: the order is still live, so its status remains
`Resting`. The rejection reports zero quantity, matching the existing rule that
a rejection carries no quantity.

### Risk

`RiskTable::check_amend` projects the participant's exposure after removing the
old order and adding the replacement, then applies the same bounds as
`check_new_order`: worst-case long/short for `max_net_qty`, and resting buy
notional for `max_open_buy_notional_cents`. Because the size cannot increase,
the net bound can only tighten or stay the same; the notional bound can still
bind when a buy moves up. The check is read-only, so a refusal mutates nothing.

### Wire additions

- `ExecKind::Amended = 6`
- `MdKind::Replace = 8`
- `RejectReason::AmendQtyIncrease = 14`
- `RejectReason::AmendWouldCross = 15`

All are additive. Existing commands, execution reports, and market-data events
keep their codes. Snapshot format v6 is unchanged: an amended order is just a
resting order at its new price and quantity, and the risk table already carries
the open exposure. The engine state-hash domain is unchanged.

### Market data

`Replace` means "this resting order now has this price and quantity, at the
back of its level". A consumer applies it as remove-then-add for the same id.
It is not `Reduce` followed by `Add`: that pair would assert the order left the
book, and the output invariant that forbids re-adding a removed id within one
output would have to be weakened. `Replace` keeps the stronger statement.

## Alternatives rejected

- **Client-side cancel + new order:** two log records, two risk admissions, a
  gap where the order does not exist, and a new id that downstream projections
  cannot relate to the old one. It is also not atomic if the second request
  fails.
- **Allow size increases:** would turn amend into a second admission path with
  a different identity lifecycle. A new order is clearer and simpler.
- **Allow crossing amends:** would make amend a taker, needing fill, trade, and
  maker-side events under the amend command's id. The order would then have
  both resting and taking roles under one id, complicating the execution
  invariants for little benefit.
- **Emit `Remove` + `Add`:** a consumer would observe a moment with no order,
  and the engine's output invariant that an id cannot be re-added after a
  removal would have to be relaxed. `Replace` is the truthful single event.
- **New out-of-band admin endpoint:** amend is client traffic, not an operator
  action. It belongs on the orders log with the other client commands.

## Consequences

- An amend consumes one orders-log offset and is replayed like any other
  command. It is part of the deterministic history.
- A rejected amend of a live order does not consume a dedup transition; a
  retry with the same target id is still possible.
- Time priority is lost on every amend, including a pure size reduction. This
  is the standard cancel-replace trade-off and is stated in the docs so clients
  do not expect an in-place reduction to keep queue position.
- The golden scenario contains no amends, so `GOLDEN_HASH` is unchanged.
- Old consumers that decode market-data events must be upgraded to understand
  `Replace`; an unknown kind is a decode error, not a silent skip. The
  dashboard consumes aggregated state, not raw `MdKind` values, so it needs no
  change.
- Not covered: side changes, order-type changes, quantity increases,
  stop/iceberg/auction orders, and amend-by-client-order-id. Those are separate
  decisions.
