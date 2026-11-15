# ADR 0025: Price units — ticks on the wire, quote cents in projections

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0017](0017-ingress-risk-and-position-reconstruction.md) (positions),
  [ADR 0022](0022-double-entry-ledger-projection.md) (ledger),
  [INVARIANTS.md](../INVARIANTS.md)

## Context

`whippet-core::ids::Price` is an **integer tick index**, not a quote-currency
amount: `InstrumentConfig::ticks_from_cents` converts an HTTP/TCP
`price_cents` request field into a tick index at ingress, the engine's ladder
and `ExecutionReport.price` stay in ticks, and `cents_from_ticks` converts back
for display. `positions` and the new ledger folded `report.price.0 * qty` and
named the result `cash_cents`. With a 100-cent tick, that understated cash and
notional by 100×. The error was invisible because the projections reconciled
with each other, not with the quote currency.

## Decision

- The wire and engine keep tick prices. No wire-format change.
- Every projection that turns a price into value takes an explicit tick-size
  map (`TickSizes`) and computes
  `quote_cents = price_ticks * tick_size_cents * qty`, exactly once, in
  checked/saturating `i128` arithmetic.
- A missing or non-positive tick size is an error (`TickSizeError`), never a
  silent factor of one. An unresolved trade is not priced at all.
- `LedgerEntry` carries `price_ticks`, `tick_size_cents`, and `price_cents`, so
  an entry can be audited without the config; postings use `price_cents * qty`.
- `whippet positions` and `whippet ledger` gain `--tick-size-cents`. If it is
  absent, the CLI resolves each priced symbol from the built-in
  `ExchangeConfig::default()`; a symbol outside that config and without the
  flag is rejected with a message naming the symbol and flag. A deployment
  whose node config differs from the built-in one must pass the flag.
- Field names `cash_cents`/`notional_cents` stay; they now mean quote cents.

## Alternatives rejected

- **Put cents in `ExecutionReport.price`:** a wire-format change and a
  conversion on the matching hot path; the engine's ladder is natively ticks.
- **Default a missing tick size to 1:** silently reintroduces the 100× error
  for any custom symbol.
- **Embed tick size in every log record:** redundant per record and a format
  change; the log is per-exchange and the CLI can be told the sizes.
- **Report tick units and rename the fields:** honest but forces every
  consumer to know the instrument configuration; cents are the useful unit for
  a ledger.

## Consequences

- Existing JSON consumers of `positions` see the same field names with corrected
  values (100× larger for a 100-cent tick); this is a bug fix, not a schema
  break.
- `build_positions`/`build_ledger` return `Result` and require `TickSizes`;
  callers must make the unit explicit.
- The engine-driven reconciliation test derives expected cents from the same
  instrument config, so a future unit regression fails before it reaches a
  report.
