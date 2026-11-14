## Which issue

<!-- Link the issue or ADR. Use "Closes #N" when the PR should close it. -->
Closes #

## Rationale

<!-- Why is this change needed? What problem does it solve, and for whom? -->

## What changed

<!-- A short, concrete list. Call out protocol, topic, security, or benchmark changes. -->

-
-

## Local execution

<!-- Paste the commands you ran and the relevant result. Do not claim a number without a manifest. -->

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Dashboard, when touched:

```bash
cd apps/dashboard
pnpm typecheck
pnpm build
```

Notes:

<!-- Iggy version, OS, Docker/VM, and whether the result is a dev-loop run. -->

## AI usage

<!-- Required when AI tools were used. Write "None" otherwise. -->

- Tools used:
- Scope (including generated code):
- How it was verified:
- Can you explain every line? Yes/No:

## Checklist

- [ ] The change is small and green; it builds and passes the checks above.
- [ ] The PR has one purpose and links its issue or ADR.
- [ ] Tests cover the new behavior, or the PR explains why no test is needed.
- [ ] Docs are updated; links resolve and YAML parses.
- [ ] An ADR is added or updated for a protocol, topic, durability, security, or benchmark-method change.
- [ ] No secrets, credentials, `.env`, `data/`, `artifacts/`, or benchmark raw files are included.
- [ ] No `docs/issues/` drafts or other local-only notes are included.
- [ ] I have not claimed a benchmark or cluster result that was not measured.
- [ ] I understand the change and can defend it in review.
- [ ] The contribution is licensed under Apache-2.0 (no CLA required).
