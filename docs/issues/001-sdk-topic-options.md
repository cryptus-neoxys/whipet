# 001 — Rust SDK: let the high-level producer and consumer pass full `TopicCreateOptions` when auto-creating a topic

**Status:** ready to file
**Type:** Feature request
**Area:** Rust SDK (`iggy`)
**Priority:** P2
**Effort:** M
**Duplicate check:** searched `create_topic_if_not_exists`, `TopicCreateOptions`, `topic options`, `producer builder`, `topic creation`, `topic_durability`, `resource options`, `builder` in issues and PRs (open and closed). No duplicate. Closest related: #4019 (closed, Python high-level producer), #3893 (open, Python/Rust API alignment), #4093 (open, IggyStream docs), #4275 (open, IggyStream API docs, touches the stream-builder files but does not add topic options). No open PR touches `producer_builder.rs`.

---

## GitHub issue body

### Description

The high-level Rust producer enables topic auto-creation by default, but its builder only exposes partitions, message expiry, max size, and message durability. When the topic does not exist, `init()` fills the remaining fields of `TopicCreateOptions` with `Default`, so create-only options are silently fixed at server defaults and several cannot be changed later.

The consumer/stream builder is stricter still: it only exposes the partition count.

This is surprising for the default path. A user who writes the recommended code gets a topic without the segment size, consumer-offset durability, replication factor, flush thresholds, preallocation, or partition-resize policy they need. The workaround — pre-create the topic with the low-level `create_topic` API and then build the producer — is non-obvious and easy to miss.

### Affected area/component

- Rust SDK (`iggy`), high-level producer and consumer/stream builders.
- Python SDK has the same limitation in its high-level producer.

### Current behavior

- `IggyProducerBuilder::create_topic_if_not_exists(partitions_count, message_expiry, max_size)` is the only bulk setter (`core/sdk/src/clients/producer_builder.rs:163-176`).
- `topic_durability()` was added later as a one-off (`producer_builder.rs:181-186`).
- `ProducerCore::init()` builds `TopicCreateOptions` with those four fields and `..TopicCreateOptions::default()` (`core/sdk/src/clients/producer.rs:152-160`). It converts `IggyExpiry::ServerDefault` and `MaxTopicSize::ServerDefault` to `None` (`:154-157`).
- The `init()` docs say “Other settings come from `TopicCreateOptions::default`” (`producer.rs:993-999`).
- `TopicCreateOptions` (`core/common/src/types/options/mod.rs:1176-1214` at the verified checkout, `:1380-1429` on current master) has many fields. Most are `CreateOnly` in the catalog, including `segment_size`, `consumer_offset_durability`, `partition_resize_policy`, flush thresholds, `preallocate_segments`, and `replication_factor`; master adds `segment_max_age`, `tiered_storage`, `local_retention_age`, and `local_retention_size`.
- `IggyStream`/consumer auto-creation exposes only `partitions_count` (`core/sdk/src/stream_builder/build/build_stream_topic.rs:86-89`), and the config type that owns that path is `IggyConsumerConfig`, not `IggyStreamConfig`.
- Python accepts only `topic_partitions_count`, `topic_message_expiry`, `topic_max_size`, and `topic_durability` in the high-level producer (`foreign/python/src/client.rs:1745-1748`), even though it already has a full `TopicOptions` type.

### Expected behavior

A caller can supply the full `TopicCreateOptions` when enabling auto-creation, so the created topic matches what they would have passed to the low-level `create_topic` API.

### Proposed solution

1. Store one `TopicCreateOptions` in `IggyProducerBuilder`, initialized to preserve today's behavior (`partitions_count: Some(1)`, `durability: Replicated`, the rest default).
2. Keep the existing `create_topic_if_not_exists(partitions, expiry, max)` and `topic_durability()` methods working; have them mutate the stored bundle. Preserve the current `ServerDefault → None` mapping for expiry and max size so last-call-wins does not send an invalid or oversized value.
3. Add a method such as `topic_create_options(options: TopicCreateOptions)` (or `create_topic_if_not_exists_with(...)`) that enables auto-creation and supplies the full bundle. Document last-call-wins precedence between the legacy setters and the new method.
4. In `ProducerCore::init()`, pass the stored bundle directly instead of rebuilding a literal with `..Default::default()`.
5. Add the same `topic_options` field to `IggyConsumerConfig` (where stream/topic auto-creation lives) and `IggyProducerConfig`, and pass it through the stream builder. Do not put it on `IggyStreamConfig`; that type is only a pair of consumer/producer configs.
6. Optional second scope item: add `TopicCreateOptions::builder()` via `bon`, already a workspace dependency. Keep it opt-in; do not make it a prerequisite.
7. Follow-up: add `topic_options: TopicOptions | None` to the Python `client.producer()` signature, mutually exclusive with the legacy kwargs, then regenerate stubs.

### Alternatives considered

- **Document pre-creation as the intended workflow.** This is the status quo and the reason the gap exists. It is acceptable for advanced users but poor for the default path, especially since auto-creation is on by default.
- **Add individual builder methods for every field.** This duplicates `TopicCreateOptions` and will drift as the catalog grows. One options bundle is simpler and future-proof.
- **Do nothing and rely on `raw`.** `raw` is not reachable through the producer builder; the builder exposes no options map.

### Additional context

- The refactor is contained: `IggyProducer::new` is `pub(crate)`, and the public API change is additive.
- Preserve `partitions_count: Some(1)` explicitly; `TopicCreateOptions::default()` uses `None`, and the server default is currently 1 but should not be relied on.
- The issue should not hardcode the current field count; the catalog is growing.
- PR #4275 touches the stream-builder config/build files for documentation. Coordinate to avoid conflicts; it does not implement this feature.

### Contribution

I can implement this in the Rust SDK and add the Python parity follow-up. I will wait for maintainer agreement on the method name and precedence rules before opening a PR.

---

## Internal notes (do not paste)

### Evidence commands

```bash
sed -n '150,215p' core/sdk/src/clients/producer_builder.rs
sed -n '130,175p' core/sdk/src/clients/producer.rs
sed -n '985,1010p' core/sdk/src/clients/producer.rs
sed -n '1170,1215p' core/common/src/types/options/mod.rs
```

### Files to touch

- `core/sdk/src/clients/producer_builder.rs`
- `core/sdk/src/clients/producer.rs`
- `core/common/src/types/options/mod.rs` (only if adding the builder)
- `core/integration/tests/sdk/producer/mod.rs`
- `core/sdk/src/stream_builder/config/config_iggy_consumer.rs`
- `core/sdk/src/stream_builder/config/config_iggy_producer.rs`
- `core/sdk/src/stream_builder/build/build_iggy_producer.rs`
- `core/sdk/src/stream_builder/build/build_stream_topic.rs`
- Python follow-up: `foreign/python/src/client.rs`, `foreign/python/apache_iggy.pyi`, `foreign/python/tests/test_producer.py`

### Test plan

1. Integration: create a topic with non-default `segment_size`, `messages_required_to_save`, `partition_resize_policy`, `replication_factor`, and compression; assert the created topic's resolved options.
2. Regression: the legacy three-argument method still creates the requested partition count with other defaults, including `ServerDefault` expiry/max-size handling.
3. Precedence: calling a legacy setter after `topic_create_options` (and vice versa) produces the documented last-call-wins result.
4. Builder: default and full-field construction, plus a doc test.
5. Python: `topic_options=` path, mutual-exclusion error, regenerated stubs.

Commands: `cargo test -p iggy_common`, `cargo test -p iggy --lib`, the integration harness target, `cargo run --bin stub_gen`, `pytest foreign/python/tests`.

### Risk

Maintainers may view pre-creation as the intended workflow. Frame the issue as ergonomics for the default path, not a correctness bug. The API is additive; no wire change. Coordinate with #4275 on the stream-builder files.
