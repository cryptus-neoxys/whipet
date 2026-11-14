# Whippet

An independent Apache Iggy showcase: a sub-millisecond matching engine with millisecond-durable streaming, built to make the latency/durability trade-off visible and measurable.

> **Status: research phase.** No implementation yet. The architecture and research dossier are in [`docs/research/`](docs/research/).

## Research artifacts

- [Ideation and selection](docs/research/00-ideation.md) — 28 concepts, top 10 ranked, top 3 chosen.
- [Research dossier and architecture blueprint](docs/research/01-research-dossier.md) — exchange architecture, Iggy playbook, benchmark protocol, failure matrix, simulator, security, DevEx.
- [Upstream contribution backlog](docs/research/02-upstream-backlog.md) — the Iggy gaps found during research, prioritized for Apache merit.
- [Issue pack](docs/issues/README.md) — five ready-to-file Iggy issues, plus a dedup/coordination log for the rest.

## What Whippet will demonstrate

- Per-symbol ordering and a deterministic price-time-priority matcher.
- Iggy as the durable log, replay source, and fanout backbone.
- Two explicit modes: `durable` (Iggy inline, quorum ack) and `turbo` (local match, Iggy write-behind).
- Consumer groups, offset/timestamp replay, headers, deduplication, PATs, TLS, AES-256-GCM-SIV stream encryption, and a 3-node VSR cluster profile.
- Open-loop benchmarks with HDR histograms, stage breakdowns, per-subscriber fanout, and random-phase failover tests.
- A seeded, deterministic market simulator with calm, volatile, flash-crash, and liquidity-drought scenarios.

## License and attribution

Apache-2.0. Whippet is an independent showcase. Apache Iggy is a project of the Apache Software Foundation. Whippet is not affiliated with or endorsed by the ASF.
