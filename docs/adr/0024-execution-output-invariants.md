# ADR 0024: Execution output provenance invariants

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0019](0019-executable-engine-invariants.md) (book/dedup invariants),
  [ADR 0022](0022-double-entry-ledger-projection.md) (ledger consumes fills),
  [INVARIANTS.md](../INVARIANTS.md)

## Context

`SymbolEngine::process` returns one `EngineOutput` per input: execution reports,
market-data events, optional full snapshots, and a state-snapshot flag. The
book/dedup checks prove the engine's internal state, but not that the output a
client and the feed receive agrees with it. A missing `Trade` event, a fill
without its maker-side `Remove`, a role flag that contradicts the admission, or
a cancel report with the wrong side are invisible to the state hash and to the
book invariants. The ledger makes this sharper: it reconstructs cash and
positions from execution reports, so a report whose side/qty/role is wrong
becomes wrong balances.

## Decision

Two test-only checks:

- `EngineOutput::check_invariants` — output-internal, O(executions + md +
  snapshots), including sequence continuity in the two independent spaces,
  one command's provenance, acceptance-before-terminal ordering, fill shape,
  exactly one trade and one maker-side reduce/remove per fill, add/reduce/
  remove shape, rejection shape, and role-flag exclusivity.
- `SymbolEngine::check_output_invariants(command, offset, out)` — adds the
  engine facts: every event's `cause_offset` is the input offset, reports use
  the command timestamp, MD uses the venue clock, symbols match, and the last
  seq in each space is one below the engine's next counter.

Documented legitimate exceptions:

- The maker fill is not a second report; it is `maker_order_id` on the taker's
  fill plus the paired maker-side `Remove`/`Reduce`.
- `FLAG_RESTED` is an admission-time role, not a promise to rest; an STP can
  clear the opposite side so a taker-flagged limit adds a remainder.
- A `Remove` carries qty 0; a `Reduce` carries the new remaining qty. A maker
  from an earlier command has no preceding `Add` in this output.
- A cadence snapshot may accompany a rejection; snapshots are not `md` deltas.
- A client cancel has no role flag and reports the removed order's side/price.

The checks run after every command in the 5,000-command randomized workload,
the `arb_flow` proptest, and the property workloads, and a dedicated test
compares full execution/MD vectors (including seqs) from two identical runs.

## Bugs it found

1. **Cancel report side:** `process_cancel` reported `command.side` (always
   `Buy` in the fixed cancel layout) while the paired `Remove` reported the
   resting order's side. A ledger folding that report would put the seller on
   the wrong side. It now reports `removed.side`.
2. **Post-acceptance cancel role:** the BookFull/remainder cancel path hardcoded
   `FLAG_TAKER`, contradicting a passively admitted (`FLAG_RESTED`) limit that
   could not rest. It now carries the admission's role flags.

## Alternatives rejected

- **Infer output correctness from the book state:** the book can be correct
  while a report/event is missing or mislabelled; the output is a separate
  contract.
- **A shadow matching model:** duplicates the matcher and can share its bugs.
- **Run the full check in release:** it is O(output) per command and would tax
  the hot path; the single-writer design, deterministic replay, and the ledger
  reconciliation are the release guards.

## Consequences

- Any new execution kind, MD event, or role flag must extend the checks; a
  check that cannot fail is documented as an unreachable invariant.
- The ledger and surveillance projections can assume a fill's taker side,
  maker id, and trade event agree, or a test fails first.
- Zero release cost: the checks are compiled into test builds only.
