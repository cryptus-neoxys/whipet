# ADR 0019: Executable engine invariants and the order lifecycle

- **Status:** Accepted (implemented 2026-11-15)
- **Date:** 2026-11-15
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0003](0003-matching-engine-data-structures.md) (book structures),
  [ADR 0009](0009-performance-budget-and-optimization-order.md) (hot-path discipline),
  [INVARIANTS.md](../INVARIANTS.md), [ARCHITECTURE.md](../ARCHITECTURE.md)

## Context

The matcher's correctness depends on structural invariants that were only in
the authors' heads: the price-ladder arena, the intrusive FIFO lists, the
occupancy bitmap, the cached best index, the free list, the order-id index,
and the dedup map/queues all have to agree. The order lifecycle
(`Accepted -> Resting -> Filled|Cancelled`) was implicit in call sites, and a
mis-ordered update could corrupt eviction order or the book without any test
noticing. The BookFull path even stored `Rejected` while reporting `Cancelled`,
which the lifecycle table would have caught.

## Decision

### Executable invariants

- `Book::check_invariants()` walks the ladder, lists, arena, free list, index,
  bitmap, cached bests, and level aggregates. It is a public diagnostic, O(book),
  and never runs on the release matching path.
- `DedupTable::check_invariants()` checks the soft cap, terminal queue
  (exactly once per live terminal id, no dead entries), insertion walk
  (no consecutive duplicate, every live id present), and creation rules.
- `SymbolEngine::check_invariants()` checks the cross-structure agreement:
  `Resting` ⇔ in the book; no `Accepted` after `process`; terminal ids absent
  from the book; counts agree.
- Tests call these after every command in a 5,000-command randomized workload
  and at the end of the golden, property, and 200k-command snapshot workloads.
  Focused tests construct each violation through internal mutations or the
  public `from_snapshot`/`from_entries` APIs and assert a clear error.

### Explicit order lifecycle

```text
Accepted -> Resting | Filled | Cancelled
Resting  -> Filled | Cancelled
Rejected is creation-only
Filled / Cancelled / Rejected are terminal
```

`DedupStatus::can_transition` is the table. `DedupTable::update` debug-asserts
it and refuses an illegal edge in release; `insert` only creates `Accepted` or
`Rejected`. The BookFull flow now stores `Cancelled` (matching the execution
report) instead of `Rejected`.

### No release hot-path cost

The checks are test-only. Production matching does not walk the book or the
dedup table to validate itself; the single-writer design and the state hash
are the production guard. If a cheap assertion is ever needed in release, it
must be O(1) and justified by a measured budget, not added here.

## Alternatives rejected

- **Run `check_invariants` after every command in debug builds:** O(book) per
  command makes the debug test suite and dev loop unusable; the randomized
  test gives the same coverage at the boundaries that matter.
- **A shadow model checked against the engine:** duplicates the matcher and
  can share its bugs; structural checks are independent.
- **Trust the state hash alone:** it detects divergence between two runs, not
  a corrupt-but-self-consistent structure in one run.
- **Keep the Bool/lifecycle implicit:** that is how the BookFull mismatch
  survived.

## Consequences

- Every new book/dedup field or flow must extend the invariant checks; a
  check that cannot be made to fail is documented as such.
- The randomized workload and the golden hash are the regression gates.
- The checks are diagnostics, not a runtime proof; release behavior is
  guarded by tests, review, and the deterministic replay hash.
