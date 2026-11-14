# Upstream contribution backlog

Purpose: convert the friction found while building Whippet into a steady stream of Apache Iggy contributions. This is the merit engine behind the showcase.

Rules:

- Issue first; reproduce on a clean pinned release; include version, platform, logs, expected/actual, and a proposed approach.
- Wait for assignment before a PR. One small, single-purpose PR at a time.
- Run `fmt`, `clippy`, tests, `machete`, `sort`, `typos` locally; link the issue; respond to every comment; do not rebase for freshness.
- Do not bundle unrelated fixes. Do not open a large protocol change while new.
- After merge: bump Whippet, delete the workaround, and write five lines about what changed.

## Prioritized items

| # | Item | Type | Evidence | Why it matters | Proposed action | Effort | Priority |
|---|------|------|----------|----------------|-----------------|--------|----------|
| 1 | `compression_algorithm` is inert | Bug / docs | Catalog says "Stored and reported, not applied to messages" (`core/common/src/types/options/catalog.rs:128`); examples use app-side lz4 | Every newcomer assumes server-side compression works; a showcase cannot claim it | Either implement disk/network compression or remove/rename the option and document the app-side codec workaround | L (implement) / S (clarify) | High |
| 2 | High-level producer cannot set important topic knobs | Enhancement | `IggyProducerBuilder::create_topic_if_not_exists` exposes only partitions, expiry, max size, and `topic_durability` | Segment size, offset durability, replication factor, and flush thresholds materially affect latency and durability | Add `.topic_options(TopicCreateOptions)` or a typed builder | S | High |
| 3 | `TopicCreateOptions` has no builder | Ergonomics | 12-field struct, unlike `DirectConfig`/`BackgroundConfig` | Error-prone with `..Default::default()`; easy to set the wrong durability | Add a `bon`/typed builder | S | Medium |
| 4 | TLS validation defaults are inconsistent | Bug / security | TCP `tls_validate_certificate=true`; QUIC and WebSocket default `false` (`core/common/src/types/configuration/{quic,websocket}_config/*.rs`) | Silent insecure defaults in two transports | Default true; require explicit opt-out; add tests/docs | S | High |
| 5 | Cluster Compose version drift | Bug / docs | `docker-compose.cluster.yml` defaults `IGGY_VERSION=0.9.0` while the checkout speaks protocol 0.12.0 | New users hit an incompatible cluster immediately | Pin or derive the version; add a compatibility note | S | High |
| 6 | No runnable idempotent-producer restart example | Docs / example | Release notes show the builder, but not producer-ID persistence, epoch/sequence recovery, or restart dedup | Idempotence is a headline 0.10 feature; users need a working pattern | Add a Rust example with restart, retained operation resolution, and dedup assertions | M | High |
| 7 | Consumer-group offset semantics across restart are under-tested | Test / docs | `OnRead` vs `Manual` vs `Auto`; offsets can skip after recovery; `OnRead` is at-most-once | Easy to lose or duplicate messages silently; critical for replay/audit | Integration test + a docs table of guarantees per commit policy | M | High |
| 8 | `/metrics` has no latency histograms | Enhancement | Metrics are counters/gauges; only `iggy-bench` reports p99 | Operators cannot see producer-send or poll latency in production | Propose opt-in histograms (p50/p99/p99.9) or document external measurement | M / L | Medium |
| 9 | HTTP is a second-class transport | Enhancement / docs | No consumer groups, fetch/`PartitionReader`, `next_producer_id`, or `resolve_operation` | Users assume transport parity | Document "control plane only" and/or implement the missing surface | L | Medium |
| 10 | `DirectConfig::linger_time` is misnamed | Docs / ergonomics | It is an inter-call minimum gap, not Kafka-style batching linger | Invites the wrong batching model and misleading benchmarks | Rename (breaking) or clarify with examples | S | Medium |
| 11 | Empty payloads are rejected | Enhancement | `IggyMessage` requires a non-empty payload | Tombstones/control records need a dummy byte | Allow length 0 with explicit semantics | S / M | Low |
| 12 | MCP HTTP transport has no client auth | Security | HTTP MCP server has no client authentication | Local demo is fine; any exposure is not | Add auth or document localhost-only binding | M | Medium |

## Suggested first three PRs

1. **Cluster Compose version pin + compatibility note** — docs-only, low risk, immediate new-user value.
2. **TLS validation defaults** — small code change with tests; security-relevant and easy to review.
3. **High-level producer `.topic_options()`** — SDK ergonomics that directly unblocks Whippet and other real applications; aligns with the existing Python/Node SDK work.

Keep the consumer-group offset test/docs and the idempotent-producer example as the next wave; both are high-value but need a bit more design discussion.

## What not to do

- Do not open a large PR for server-side compression, HTTP consumer groups, or protocol changes without a maintainer-approved issue and design.
- Do not combine unrelated cleanups.
- Do not claim "exactly-once" in docs or examples; Iggy's guarantees are at-least-once plus idempotent producer dedup within a bounded window.
- Do not post benchmark numbers without hardware, version, methodology, and raw artifacts.
