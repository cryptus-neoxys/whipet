# Exchange 101

This is the minimum market structure needed to read the dashboard and the code. It is not a
regulatory or venue-specific specification.

## The book

A limit order says "buy up to N units at no more than P" or "sell up to N units at no less than P".
The exchange keeps two sorted sides:

- **bids** — resting buy orders, best (highest) price first;
- **asks** — resting sell orders, best (lowest) price first.

The **best bid** is the highest price anyone will pay; the **best ask** is the lowest price anyone
will accept. The **spread** is `best ask − best bid`. A book is **crossed** if the best bid is at or
above the best ask; a correct matcher never leaves a crossed book at rest.

## Price-time priority

When an incoming order can trade, it trades with the best-priced opposite order first. At the same
price, the order that arrived first trades first. Whippet implements this with:

- a contiguous array price ladder indexed by integer tick;
- an occupancy bitmap per side for the best price;
- an intrusive FIFO linked list per price level over a preallocated order arena.

There are no floats in the protocol or the engine. A price is an integer tick; the instrument maps
ticks to quote currency.

## Order types in this build

| Type | Behaviour |
|---|---|
| `limit` (GTC) | Match what crosses, rest the remainder at the limit price. |
| `market` | Match against the book within the market collar, cancel any remainder. |
| `ioc` | Match what crosses immediately, cancel any remainder. |
| `fok` | Reject unless the full quantity is immediately executable. |
| `post_only` | Reject if the order would cross; otherwise rest. |

Cancel (`POST /api/cancel`) removes a resting order by id and is allowed in
every halt/session state, because withdrawing risk must stay available.

## Amend/replace

`POST /api/amend` atomically replaces a resting order's price and remaining
quantity. The order keeps its id, moves to the back of its new price level, and
therefore loses time priority (ADR 0027).

- The new quantity must be positive and no larger than the current remaining
  quantity. A size increase is a new order, not a replacement.
- The new price must not cross the opposite best. Amend never takes liquidity;
  cancel and submit a new order when a cross is intended.
- Only the owner can amend. A non-owner gets `unknown_order`, the same answer
  as an unknown id.
- Amend is new risk, so it is gated by halt and session like a new order. A
  cancel remains available in every state.

Stop orders, icebergs, pegged orders, auctions, and multi-leg strategies are
out of scope for this build.

## Self-trade prevention

Whippet uses **cancel resting**: if the incoming order would trade with a resting order from the
same participant, the resting order is cancelled and the incoming order continues. The incoming
order keeps its own priority if it later rests.

## Execution reports

Every accepted order produces an `accepted` report. Fills produce `partially_filled` or `filled`
reports with the maker order id. A successful amend produces `amended` with the new price and
remaining quantity. A cancelled remainder produces `cancelled`. A rejected order produces
`rejected` with a reason. Reports carry the input offset that caused them, so replay can
reconstruct the same sequence.

## Market data

Whippet publishes **MBO** (market-by-order) deltas:

- `add` — a new resting order;
- `reduce` — a resting order's remaining quantity changed;
- `remove` — a resting order left the book;
- `replace` — a resting order kept its id but moved to a new price/quantity and
  lost priority; apply it as remove-then-add for the same id;
- `trade` — a fill happened;
- `snapshot` — the top N levels per side, used for recovery and late joiners.

Every event has a per-symbol `seq` and a `cause_offset` (the input log offset). The dashboard applies
deltas to a cache and renders a conflated snapshot at 20 Hz. The audit path, when it lands, consumes
the log itself rather than the UI cache.

## Where Iggy fits

Iggy is the durable log and the fan-out backbone:

```
gateway -> orders (persisted) -> matcher -> executions (persisted)
                                      \--> md (replicated) -> MD hub -> dashboard
```

The matcher is synchronous and deterministic. The Iggy edge is async. In the default `durable` mode,
the gateway acknowledges after the order is appended to Iggy; the matcher consumes the partition in
order and commits its consumer offset after publishing executions and market data. In `turbo` mode
(Phase 2), the gateway would hand orders to the shard locally and journal write-behind; the trade-off
is a bounded uncommitted window.

## What Whippet deliberately simplifies

- No cross-symbol atomicity, portfolio risk, clearing, settlement, or margin.
- No FIX gateway, auction, or opening/closing price discovery.
- No pro-rata allocation, fee tiers, or maker/taker rebates.
- No production cluster. Single-node is v1; 3-node VSR is Phase 3.
- No exactly-once claim. Delivery is at-least-once; the engine deduplicates business order ids.
