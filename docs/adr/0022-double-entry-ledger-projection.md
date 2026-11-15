# ADR 0022: Double-entry ledger as a deterministic log projection

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0017](0017-ingress-risk-and-position-reconstruction.md) (read-only positions),
  [ADR 0004](0004-iggy-topics-partitions-durability.md) (topics),
  [INVARIANTS.md](../INVARIANTS.md), [ARCHITECTURE.md](../ARCHITECTURE.md)

## Context

`whippet positions` reconstructs net quantity and signed cash flow per
participant, but it is deliberately not a ledger: it has no accounts, no
balances, and it can post an aggressor leg whose maker is unresolved. The
roadmap's Phase C exit gate asks for balances, positions, executions, and
ledger entries that reconcile to the same trade identities after a restart.
Putting settlement state inside the matching engine would put account writes
on the hot path and force a snapshot-format bump for every account change.

## Decision

The ledger is a **pure projection** over one partition's decoded `orders` and
`executions` logs, in `crates/whippet/src/ledger.rs`:

- One `LedgerEntry` per attributed fill report, identified by the report's
  engine `seq` and linked to its input by `cause_offset`.
- Exactly four postings per entry: buyer cash `-price*qty`, seller cash
  `+price*qty`, buyer position `+qty`, seller position `-qty`. Cash and each
  symbol's position are separate assets, and each sums to zero per entry.
- Attribution is **all-or-nothing**: if either the aggressor or the maker order
  id is absent from the decoded orders log (including `maker_order_id == 0`),
  neither leg posts; the matching unresolved counter increments. A half-posted
  trade cannot balance.
- Entries are ordered by `seq`; a repeated `seq` (a malformed log) keeps the
  first in a canonical tuple order, so input iteration order cannot change the
  result.
- `LedgerReport::check_invariants` proves per-entry balance, report-wide zero
  sums, unique ordered entries, and that every account balance/count equals a
  checked direct fold of the postings. `ledger_with_journal` refuses to print a
  report that fails.
- `whippet ledger` mirrors `whippet positions` (`--partition`, `--symbols`,
  `--participant`, `--json`) and reads only; it never commits an offset.

The model is T+0 settlement for simulation: no fees, funding, deposits,
withdrawals, margin, or clearing. `build_positions` remains the per-participant
flow reconstruction; over complete trades its `cash_cents` and `net_qty` equal
the ledger's cash and position balances, which the reconciliation test pins.

## Alternatives rejected

- **Ledger state inside `SymbolEngine`:** account writes on the matching hot
  path, a snapshot v6 for every new account field, and replay coupled to
  settlement policy.
- **A separate broker consumer with its own commit:** duplicates the
  at-least-once problem and makes the ledger's checkpoint another resume point
  to verify. A pure fold over the existing logs is restartable by definition.
- **Inferring an unknown maker:** invents a counterparty and breaks
  conservation. Reporting the gap is the honest answer.
- **Posting the aggressor leg alone when the maker is unknown:** balances stop
  summing to zero and the report can no longer be reconciled.

## Consequences

- The ledger can be recomputed at any time from offset 0; its correctness is
  bounded by the orders attribution map and the execution reports.
- An unresolved leg is visible (counter) and excludes the whole trade, so
  balances stay conservative rather than plausible.
- The CLI is O(log size) and offline; it is not a settlement service and does
  not gate trading.
- A future engine-enforced account model would need its own ADR and snapshot
  version; this projection is the reference it must reconcile against.
