# ADR 0004: Iggy topics, partitions, and durability

- **Status:** Accepted
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0002](0002-durable-default-turbo-toggle.md), [ADR 0008](0008-security-model-and-scoped-identities.md)

## Context

Whippet treats Apache Iggy as a partitioned log, not a queue. Ordering is per
partition. The topic layout is part of the product contract: partition counts,
durability, retention, and commit policy determine what the system can claim.
Leaving them to server defaults would make the demo's guarantees accidental.

The layout is defined in `crates/whippet-journal/src/topic.rs` and created by
`IggyJournal::ensure_topics`. Topics are created with the low-level
`create_topic` API because the high-level producer's
`create_topic_if_not_exists` cannot pass the full `TopicCreateOptions` (see
`docs/UPSTREAM.md`, item U-006, and the local issue pack 001).

Phase 1 is single-node. The layout is written so a future 3-node profile can
use the same streams and topics without a wire change, but Phase 1 makes no
replication claim.

## Decision

Use three streams and six topics:

| Stream | Topic | Partitions | Key | Durability | Consumer-offset durability | Retention | Producer (Phase 1) | Consumer (Phase 1 / planned) |
|---|---|---:|---|---|---|---:|---|---|
| `whippet-trading` | `orders` | 16 | symbol | `persisted` | `persisted` | 24 h | gateway | engine shard for each active partition |
| `whippet-trading` | `executions` | 16 | symbol | `persisted` | `persisted` | 7 d | engine | none in Phase 1; gateway, surveillance, audit planned |
| `whippet-md` | `md` | 16 | symbol | `replicated` | `replicated` | 1 h | engine | MD consumer per active shard -> `MdHub` |
| `whippet-ops` | `alerts` | 4 | symbol | `persisted` | `persisted` | 30 d | none in Phase 1; surveillance planned | dashboard, audit planned |
| `whippet-ops` | `audit` | 4 | service | `persisted` | `persisted` | 90 d | none in Phase 1; audit planned | audit verifier planned |
| `whippet-ops` | `commands` | 1 | admin | `persisted` | `persisted` | 7 d | none in Phase 1; CLI/simulator planned | node, simulator planned |

Additional fixed properties:

- **Partition count is 16 by default.** `WHIPPET_PARTITIONS` / `--partitions`
  accepts 1–256. Orders, executions, and market data use the exchange partition
  count; alerts and audit use 4; commands use 1. There is no per-host profile
  yet; 16 is the only default.
- **Partitioning key is the symbol.** The gateway computes
  `symbol.partition(exchange.partitions)` and appends to that partition. Each
  active engine shard polls its own `orders` partition. Executions and market
  data are written back to the same shard partition. The symbol is also carried
  in the message headers, but the partition is the ordering boundary.
- **Topic options are explicit.** Segment size is 64 MiB. `orders`,
  `executions`, and `md` request an 8 GiB maximum topic size; ops topics request
  2 GiB. `messages_required_to_save` is 1,024 and
  `size_of_messages_required_to_save` is 1 MiB. Existing topics are left alone
  except for a partition-count check: a mismatch fails startup instead of
  silently routing to missing partitions. Segment size, durability, retention,
  and flush thresholds are not re-validated in Phase 1.
- **Consumers commit manually.** `PollOptions` uses
  `CommitPolicy::Manual`. The engine commits the last processed order offset
  only after the execution batch and the market-data batch have both been
  appended. The MD consumer commits after applying the batch. On a failed append
  or commit, the batch is retried; the pipeline is at-least-once and downstream
  effects must deduplicate.
- **Offsets are log positions, not counts.** Iggy offsets can skip after
  retention, recovery, or other log maintenance. `next_offset =
  last_committed + 1` is a position to poll from, not an assertion that every
  offset in between exists or that `offset - first_offset` equals the number of
  events. Phase 1 does not yet detect a retention gap when an old committed
  offset is no longer available; a replay or audit path must treat that as a
  resync condition, not as an empty topic.
- **Market data is `replicated`, the business topics are `persisted`.** On a
  single node this is a metadata request, not a replication guarantee. Phase 3
  is where `replicated` and RF3 become meaningful. See
  [ADR 0007](0007-single-node-v1-cluster-phase-3.md).
- **Three streams are a coarse trust split.** `whippet-trading`, `whippet-md`,
  and `whippet-ops` separate business orders/executions, public market data, and
  operational topics. Phase 2 adds scoped identities and may split further per
  trust domain; see [ADR 0008](0008-security-model-and-scoped-identities.md).

## Consequences

Positive:

- Per-symbol ordering is guaranteed by construction: one partition per symbol
  hash, one writer per partition, one engine shard per partition.
- The layout is replayable and auditable. Orders, executions, and market data
  are all in the log, not only in process memory.
- Manual commits make the at-least-once boundary explicit. A crash between
  processing and commit replays the batch; the dedup table absorbs the duplicate
  business effect.
- Retention is differentiated by value: orders 24 h, executions 7 d, market
  data 1 h, alerts 30 d, audit 90 d, commands 7 d.
- The same layout can be used by a future cluster profile without changing
  topic names or partition keys.

Negative:

- Six topics and three streams are created even when a topic has no producer or
  consumer in Phase 1. This is deliberate (the layout is the contract) but it
  costs metadata and makes the dashboard's "empty" topics look unfinished.
- The 16-partition default is fixed at topic creation. Changing it later is a
  migration, not a config toggle. Iggy's partition-resize policy is not used in
  Phase 1.
- A single node cannot honor `replicated` in the way a cluster can. The word
  "replicated" in the topic table must not be read as an HA claim.
- Retention is time-based message expiry. It is not an archiver. Audit's 90-day
  window is a demo retention, not a compliance control.
- Offset gaps are real. Any code that assumes contiguous offsets will
  eventually under-count or mis-detect a gap. The current consumers avoid that
  assumption, but the replay UI does not exist yet.

## Alternatives considered

- **One partition per symbol.** Perfect locality and the simplest possible
  ordering story, but it creates per-symbol WAL, metadata, and (in a cluster)
  VSR-group overhead. Hash symbols onto a fixed partition count instead.
- **One stream for everything.** Fewer names and simpler bootstrap, but it
  destroys the blast-radius separation between trading, market data, and ops.
  Rejected.
- **Let the server defaults choose partitions/durability/retention.** Rejected:
  the guarantees would be invisible in code review and could change with a
  server upgrade.
- **Auto-commit consumer offsets.** Rejected: it hides the at-least-once window
  and can commit an offset before the execution or market-data output is
  durable.
- **Treat offsets as a contiguous sequence.** Rejected: Iggy explicitly does not
  guarantee that after retention/recovery, and the resulting gap detector would
  be wrong.
- **Use Iggy's idempotent producer as the only dedup layer.** Rejected: it is
  batch-level and bounded. Whippet keeps its own `(symbol, client_order_id)`
  dedup table in the engine.

## Status

Accepted for Phase 1. Topic names, partition counts, durability, retention, and
manual-commit policy are a product contract. Changing any of them requires a new
ADR and a migration plan. The cluster profile in Phase 3 may add RF3 and a
3-node Compose file, but it must not silently change this table.
