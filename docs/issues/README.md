# Whippet issue pack — Apache Iggy

Five verified, non-duplicate issues derived from building Whippet. Each file contains a copy-paste GitHub issue body plus internal notes that should not be pasted.

**Provenance:** local checkout `9ba0ab2ef` is a fork commit on upstream `15cd1627f` (PR #4848, 2026-11-13). Upstream `master` was `c78da20` on 2026-11-14, 33 commits ahead of that base. Re-grep line numbers before filing; some metrics lines moved, and MCP hardening (#4896) merged after this checkout.

---

## Ready to file

| # | Issue | Type | Area | Priority | Effort | Status |
|---|-------|------|------|----------|--------|--------|
| 001 | Rust SDK: high-level producer/consumer cannot pass full `TopicCreateOptions` | Feature | Rust SDK | P2 | M | Ready |
| 002 | Cluster Docker Compose defaults to 0.9.0, which rejects 0.10.0 binary clients | Bug | Docker / cluster | P2 | XS–S | Ready |
| 003 | Expose send commit latency on `/metrics`, matching fetch histograms | Feature | Server / observability | P3 | S–M | Ready |
| 004 | Rust examples: add an idempotent producer restart/resend demo | Docs / example | Rust SDK | P2 | M | Ready |
| 005 | QUIC connection strings cannot set the TLS server name | Bug | Rust SDK / TLS | P2 | S–M | Coordinate with #4872 first |

**Suggested filing order:** 002 → 001 → 003 → 004 → 005. Start with the smallest, least controversial fix (Compose), then the SDK ergonomics issue, then the server metric with its clear fetch precedent, then the example, then the QUIC follow-up after #4872.

---

## Dedup and coordination log

These candidates were investigated and deliberately **not** turned into new issues.

| Candidate | Verdict | Canonical reference / reason |
|-----------|---------|------------------------------|
| Server-side compression option is inert | Drop | [#18](https://github.com/apache/iggy/issues/18) tracks it. The catalog already says “Stored and reported, not applied to messages” (`core/common/src/types/options/catalog.rs:128`), and docs were clarified in [#3366](https://github.com/apache/iggy/pull/3366). No new issue or docs PR needed. |
| Empty payloads are rejected | Drop | [#4878](https://github.com/apache/iggy/issues/4878) is open, assigned, and contains a full cross-SDK implementation checklist. Do not duplicate. |
| HTTP transport lacks consumer groups | Drop | Intentional. The server refuses `ConsumerKind::ConsumerGroup` in `core/server/src/http/wire.rs` (`refuse_consumer_group`), and `core/sdk/README.md` documents HTTP as standalone-consumer-only. |
| HTTP client treats a consumer group as a plain consumer | Coordinate | [#4408](https://github.com/apache/iggy/issues/4408) is open and assigned. Server-side refusal landed in [#4348](https://github.com/apache/iggy/pull/4348) six days later. Comment with the `wire.rs` evidence and ask whether the remaining scope is Java/C# error parity or the issue can close. |
| Consumer-offset semantics across restart are under-tested | Drop | `core/sdk/README.md` has a detailed “Positions and commits” section, and `core/integration/tests/server/consumer_offset_recovery.rs` covers replicated/persisted crash recovery. |
| TLS validation defaults are inconsistent (TCP true, QUIC/WS false) | Coordinate | [#4210](https://github.com/apache/iggy/issues/4210) and PR [#4872](https://github.com/apache/iggy/pull/4872) are open; #4883 covers cross-SDK option parity. The QUIC default is consistent with the server’s ephemeral self-signed certificate and the client’s lack of a CA-file field. Comment on #4872 with the exact matrix; do not file a default-flip issue. |
| MCP HTTP transport has no client authentication | Drop — already fixed | [#4896](https://github.com/apache/iggy/pull/4896) merged 2026-11-13 19:08 UTC, after this checkout. It adds `http.auth_token`, refuses non-loopback binds without a token, refuses open CORS without a token, and adds bearer middleware. |
| `DirectConfig::linger_time` is misnamed | Drop | The primary docs already define it precisely as the minimum gap between sequential direct sends (`core/sdk/src/clients/producer_config.rs:255-264`). Only legacy stream-builder wrappers use the vague “interval between messages sent” wording; open PR [#4275](https://github.com/apache/iggy/pull/4275) removes those. |
| `TopicCreateOptions` lacks a builder | Fold into 001 | The builder is a secondary scope item in the topic-options issue, not a standalone request. `bon` is already a workspace dependency. |
| “No latency histograms” | Reframe into 003 | Fetch already exports `readiness_push_to_fire_seconds` and `fetch_fire_to_reply_seconds`. The real gap is the send/append path, which has no latency histogram (only request counters and WAL pressure counters). |
| Docker Compose cluster version drift | **Filed as 002** | Verified: `server-0.10.0` is the latest release (2026-11-07), and its own `docker-compose.cluster.yml` still defaults to 0.9.0. |
| Idempotent producer example | **Filed as 004** | The README has a minimal in-session `BatchProducer` snippet and prose about restart, but no runnable restart/resend example. |
| Python high-level producer lacks idempotent mode | Hold | Real parity gap, but the Python producer is a separate PyO3 port; adding idempotence requires plumbing producer ID/epoch/sequence state, not just a flag. Revisit after 001/004. |

---

## Issue format notes (from Apache Iggy's templates)

- **Flow:** issue first → maintainer agrees on approach → assignment → code. New functionality without an approved issue can be closed.
- **Bug form:** Bug description (required), Affected area/component, Deployment, Versions, Hardware/environment, Sample code, Logs, Iggy server config, Reproduction, Contribution, Good first issue.
- **Feature form:** Description (required), Affected area/component, Proposed solution, Alternatives considered, Contribution, Good first issue.
- **Titles:** specific sentence, often `Component: symptom` or an imperative feature phrase. No Conventional Commits on issues.
- **Labels:** the template applies `bug`/`feature`/`question`; the area dropdown auto-applies `server`, `rust`, `python`, `mcp`, `performance`, `config`, `api`, `docs`, etc. Do not add area labels manually.
- **Assignment:** ticking “Contribution” auto-assigns the reporter, but that is not maintainer approval. CONTRIBUTING still requires agreement/assignment before coding.
- **AI disclosure:** issues have no AI field. The PR body has an **AI Usage** section: tools, scope (including generated code), local verification, and whether you can explain every line.
- **PR shape:** `type(scope): subject` under 72 chars; sections “Which issue does this PR address? / Rationale / What changed? / Local Execution / AI Usage”; one PR, one purpose; link `Closes #N`.

---

## Why these five

- **001** is the highest-leverage SDK fix: it unblocks real applications (including Whippet) and aligns with the existing Python/Node SDK work.
- **002** is the cheapest high-visibility fix: a small default change plus a note, preventing a guaranteed new-user failure in the cluster entrypoint.
- **003** closes a real observability asymmetry with a clear precedent and a contained insertion point.
- **004** turns a headline 0.10 feature into a runnable, restart-safe example and completes the prose-only guidance.
- **005** is a TLS correctness/ergonomics bug that a reviewer already flagged; coordinate with #4872 so it lands cleanly.

Everything else is either already tracked, intentional, documented, or fixed upstream. Filing those would add noise, not merit.
