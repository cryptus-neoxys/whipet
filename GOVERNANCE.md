# Governance

Whippet is an independent open-source showcase, not an Apache Software Foundation project. It is
not governed by the ASF and makes no ASF endorsement claim. Apache Iggy is a project of the ASF and
is used here as a pinned upstream dependency.

## Decision making

- The maintainer makes final decisions and is accountable for the repository.
- Significant design changes are recorded as ADRs in [`docs/adr/`](adr/). An ADR is required for
  changes to the matching semantics, the wire protocol, topic layout, durability policy, or the
  benchmark protocol.
- Small fixes and documentation improvements use ordinary pull requests and lazy consensus.
- Correctness and honesty outrank feature count. A benchmark claim without a manifest and a method
  is rejected.

## Contribution path

1. Open an issue for bugs, design questions, or non-trivial features.
2. For upstream Apache Iggy friction, follow [`docs/UPSTREAM.md`](UPSTREAM.md): reproduce on the
   pinned release, search for duplicates, and file upstream before changing Whippet around it.
3. Keep pull requests small, green, and single-purpose.
4. Run the local checks in [`CONTRIBUTING.md`](../CONTRIBUTING.md).

## Maintainership

Maintainer status is earned through sustained, high-quality contributions and review. There is no
CLA. Contributions are accepted under the Apache License, Version 2.0.

## Code of conduct

Participation is covered by [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md).

## Trademarks

Apache, Apache Iggy, Iggy, and the Apache feather logo are trademarks of the Apache Software
Foundation. Whippet does not use them to imply endorsement.
