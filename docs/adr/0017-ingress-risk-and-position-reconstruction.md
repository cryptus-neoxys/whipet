# ADR 0017: Ingress pre-trade risk and read-only position reconstruction

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0015](0015-audited-instrument-control-plane.md) (operator identity),
  [ROADMAP Phase C](../ROADMAP-AND-IMPROVEMENT-PLAN.md),
  [ARCHITECTURE.md](../ARCHITECTURE.md), [OPERATIONS.md](../OPERATIONS.md)

## Context

Participants have stable `ParticipantId` identities (`ParticipantId::derive` of
the token's name) and the engine attributes every fill and every resting order
to one. There is no account balance, position, or pre-trade risk layer. The
improvement plan's Phase C asks for a participant/account model, pre-trade
risk, and a ledger, while warning that cross-symbol cash is partitioned across
shards and cannot be made globally atomic without a coordination layer.

Two smaller slices are safe and useful before engine-enforced account state:

1. per-order quantity/notional limits at the ingress boundary;
2. a read-only reconstruction of positions and cash flow from the logs.

Both are honest partial steps, not a settlement ledger.

## Decision

### Ingress pre-trade risk (per order)

The gateway and binary TCP listener enforce two optional limits before the
orders-log append is built:

- `WHIPPET_RISK_MAX_ORDER_QTY` — maximum quantity of one order.
- `WHIPPET_RISK_MAX_NOTIONAL_CENTS` — maximum `price × qty` for a priced order.

Both default to `0` (disabled), so the existing behavior is unchanged unless
an operator opts in. A violation is rejected with HTTP 422 `risk_limit` or TCP
`RiskRejected` (wire code 11), before any append, order-id minting, or dedup
work. Market orders have no price at validation and are not notional-checked in
this slice; they are still quantity-checked. Rejections are counted as
`whippet_risk_rejected_total{reason="order_qty"|"notional"}`.

**This is an ingress guard, not an engine-enforced risk control.** A producer
that appends directly to the orders topic bypasses it; the engine remains the
matching authority. It exists so a normal HTTP/TCP client cannot submit an
obviously oversized order, and so the operator can see the rejection rate.

### Read-only position reconstruction

`whippet positions` reconstructs, per participant and symbol:

- net quantity (`buy - sell`), buy/sell quantities, fill count;
- cash flow at execution prices (`buys pay`, `sells receive`) and turnover;
- unresolved maker and aggressor fills when an order id is absent from the
  orders log.

One execution report is one trade: the aggressor leg uses `report.side`, the
maker leg uses the opposite side and the same quantity/price, and both are
attributed through the orders log. The tool reads from offset 0 with
zero-wait batches and never appends, commits, creates, or trims.

It is a **reconstruction, not a settlement ledger**: no deposits, balances,
fees, funding, margin, or clearing; participant names are not stored in the
log, so the numeric id is the only stable key; a missing order record is
reported, never guessed.

## Alternatives rejected

- **Engine-enforced position/cash limits now:** requires account state in the
  engine, a snapshot format bump, and a decision about cross-symbol cash
  atomicity. The ingress guard and the reconstruction are useful and safe
  before that design.
- **Gateway-held positions for pre-trade checks:** racy across concurrent
  requests and workers; two orders can each pass a position check that the
  pair violates. The engine is the only serial authority per symbol.
- **Silently clamping oversized orders:** hides client mistakes and changes
  the order the client asked for. Reject explicitly.
- **Inferring an unknown maker from the aggressor:** would fabricate a
  participant attribution. Report the gap instead.

## Consequences

- The risk limits do not protect against a compromised/direct log producer;
  they are an admission guard and an observability signal.
- The positions tool's maker attribution depends on the orders log and reports
  unresolved ids rather than inferring them. Cash flow is trade-value only.
- Engine-enforced exposure limits, account balances, fees, margin, and
  settlement remain future work; they need account state in the snapshot and a
  cross-symbol coordination decision.
- The per-order limits are static venue-wide values in this slice. Per-
  participant limits need the account model and are not implied by this ADR.
