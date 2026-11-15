# Whippet — agent notes

Rust workspace (8 crates): `crates/whippet`, `whippet-core`, `whippet-engine`,
`whippet-journal`, `whippet-marketdata`, `whippet-sim`, `whippet-bench`, `whippet-testkit`.

## Skills (project-local, `.agents/skills/`)

- pstack core: `poteto-mode` is the default working style. Before commit run
  `deslop`; before review run `no-comments`.
- cursor-team-kit complement (manually mirrored, untracked by skills CLI):
  `deslop`, `control-cli`, `control-ui`, `thermo-nuclear-code-quality-review`,
  `pr-review-canvas`, `fix-ci`, `loop-on-ci`, `verify-this`, `check-compiler-errors`, etc.
- mattpocock pack: `tdd`, `diagnosing-bugs`, `code-review`, `to-spec`, `to-tickets`, `implement`.
- Model policy: `.agents/rules/pstack-models.md` — all roles `inherit-parent`,
  so everything runs on the current T3 Code provider (OpenCode / OpenAI / Antigravity).

## Agent skills

### Issue tracker

Issues live in GitHub Issues on `cryptus-neoxys/whipet`, read and written with the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles use their own names as label strings. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one glossary (`docs/GLOSSARY.md`) and one ADR log (`docs/adr/`). See `docs/agents/domain.md`.

## Checks

- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
