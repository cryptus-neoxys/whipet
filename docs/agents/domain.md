# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **single-context**: one glossary and one ADR log, both under `docs/`.

## Before exploring, read these

- **`docs/GLOSSARY.md`**: terms used in the code, the dashboard, and the docs (MBO, tick, shard, durable vs turbo, and so on).
- **`docs/adr/`**: read ADRs that touch the area you're about to work in. The index is `docs/adr/README.md`; ADRs are immutable once accepted, so a later ADR supersedes an earlier one rather than editing it.

If either is missing, **proceed silently**. Don't flag the absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

```
/
├── docs/
│   ├── GLOSSARY.md
│   ├── adr/
│   │   ├── README.md          ← index; add every new ADR here
│   │   ├── 0001-workspace-and-crate-boundaries.md
│   │   └── …
│   └── agents/                ← this directory: skill configuration
└── crates/
    ├── whippet/               ← binary: CLI, gateway, journal runner
    ├── whippet-core/          ← protocol types, ids, config
    ├── whippet-engine/        ← deterministic matcher
    ├── whippet-journal/       ← Iggy edge
    ├── whippet-marketdata/    ← MD fanout + metrics
    ├── whippet-sim/           ← simulator
    ├── whippet-bench/         ← benchmark harness
    └── whippet-testkit/       ← test support
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `docs/GLOSSARY.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0003 (matching engine data structures), but worth reopening because…_
