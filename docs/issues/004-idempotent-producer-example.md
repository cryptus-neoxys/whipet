# 004 — Rust examples: add an idempotent producer restart/resend demo

**Status:** ready to file
**Type:** Feature request / documentation example
**Area:** Rust SDK / examples
**Priority:** P2
**Effort:** M
**Duplicate check:** searched `idempotent`, `producer restart`, `producer reconnect`, `send_producer_messages`, `producer example` in issues and PRs. No SDK example issue or PR. #4513 (closed) and #4522 (open) are Kafka-gateway dedup/EOS, not Rust examples. #4691 added the feature and README prose but no runnable example.

---

## GitHub issue body

### Description

Idempotent producers landed in #4691, but there is no runnable restart/resend example. `core/sdk/README.md` has a minimal in-session `BatchProducer` + `send_producer_messages` snippet and prose that says a restarted client must store its producer state and resend the last batch before moving to the next epoch. The restart sequence exists only as prose; no code demonstrates the state machine, the resend, or the fencing errors.

A user who wants restart-safe idempotence has to assemble the hard parts themselves: write-ahead state, the last batch's base sequence and confirmed offset, resending with the old epoch/sequence, and handling `ProducerUnknown`, `ProducerSequenceOutOfOrder`, and `ProducerEpochFenced`.

### Affected area/component

- Rust SDK documentation and `examples/rust`
- Idempotent producers

### Current behavior

- `.idempotent()` is available on `IggyProducerBuilder` (`core/sdk/src/clients/producer_builder.rs:208-213`), and the producer ID is fetched during `init()` (`core/sdk/src/clients/producer.rs:165-167`, `:304-310`).
- The high-level producer’s identity and sequences are private (`core/sdk/src/clients/producer_idempotence.rs:43-61`); there is no builder input for producer ID, epoch, or sequence. A process restart with `.idempotent()` therefore gets a new producer ID.
- The restart-safe path is `BatchProducer` plus `send_producer_messages` (`core/common/src/traits/message_client.rs:166-173`; `BatchProducer` is in the prelude at `core/sdk/src/prelude.rs:53`).
- `core/sdk/README.md:668-685` is the low-level `BatchProducer` snippet; `:687-689` is the high-level `.idempotent()` prose; `:647` warns that a restarted client must store and resend its state.
- No example exists in `examples/rust/src/`; `examples/rust/README.md` has no idempotence section.
- Released Rust, Python, C++, and PHP SDKs, the CLI, the Kafka gateway, and the connectors runtime refuse producer batches; released Swift does too (`core/sdk/README.md:693-695`; `producer_builder.rs:200-207`). The example must use the current checkout/server so its consumer can poll the output.

### Proposed solution

Add `examples/rust/src/idempotent-producer/` with a producer (two modes) and a small consumer:

- **Setup:** TCP only; create the stream and topic with `Durability::Persisted` (a topic option), one partition, and `messages_required_to_save = Some(1)`.
- **State file:** JSON with `producer_id`, `epoch`, `next_sequence`, and the last batch (`base_sequence`, confirmed `base_offset`, payloads). Write the pending state before `send_producer_messages`; update it after the ack.
- **Default run:** take `next_producer_id()` once, send batches with `BatchProducer::new(id, epoch, sequence)`, and checkpoint the state.
- **`--resume`:** reload the state; resend the last batch first with the old ID/epoch/sequence. Compare `response.confirmations[0].base_offset` with the stored offset and print “duplicate suppressed, no new write” when they match. Then bump the epoch, reset sequence to 0, and continue. If the crash happened before the send landed, the resend writes the batch and returns a new offset; handle that as a separate “unknown outcome” path rather than asserting equality blindly.
- **`--resend-old`:** demonstrate fencing. Force producer loss (fresh data directory or a fabricated producer ID) and use a non-zero base sequence, because a sequence-0 batch from an unknown producer is accepted as a new producer. Show `ProducerUnknown` after the retained window, `ProducerSequenceOutOfOrder` for a gap, and `ProducerEpochFenced` for a stale epoch.
- **Consumer:** poll from offset 0 and print count/payloads; the point is that the count does not grow after a successful resend. Make clear that Iggy deduplicates producer writes, not consumer processing, so application-level exactly-once effects still need an idempotent sink.
- **Encryption:** use no encryptor, or keep the plaintext and pass it again on resend. Never resend already-encrypted messages; `send_producer_messages` encrypts in place.

Register the binaries in `examples/rust/Cargo.toml`, add an “Idempotent producer” section to `examples/rust/README.md`, and link it from `core/sdk/README.md`.

### Alternatives considered

- **Only expand the README prose.** Better than nothing, but it still would not demonstrate state persistence, resend, or fencing errors.
- **Extend `.idempotent()` to accept a producer ID/epoch/sequence.** This is a larger API change and may be the right long-term direction, but the example can land first using `BatchProducer`.
- **Rely on the Kafka gateway idempotence docs.** Those are Kafka-specific and do not show the Iggy SDK path.

### Additional context

- Retained dedup window: 5 batches per producer, 4,096 producers, 24 hours idle expiry (`core/partitions/src/producer_state.rs:35,40,45`).
- Core behavior is already covered by `core/integration/tests/cluster/idempotent_producer.rs`; the example is for users, not a substitute for tests.
- The example should explicitly say the state file is illustrative, not production guidance.
- The released-consumer limitation is not specific to Swift; call out the full list so users do not file “consumer sees nothing” bugs.

### Contribution

I can implement the example and the README updates. I will wait for agreement on the location and the state-file format before opening a PR.

---

## Internal notes (do not paste)

### Files to touch

- `examples/rust/src/idempotent-producer/producer/main.rs`
- `examples/rust/src/idempotent-producer/consumer/main.rs`
- `examples/rust/Cargo.toml` — `[[example]]` entries
- `examples/rust/README.md` — new section with exact commands
- `core/sdk/README.md` — link to the example

### Test plan

1. `cargo build -p iggy_examples --examples`.
2. Start a fresh server from the same checkout with default root credentials.
3. Run send → `--resume` twice; verify the confirmed base offset is identical and the consumer count is unchanged.
4. Run the next epoch; verify the consumer count increases by one.
5. Force producer loss and run `--resend-old` with a non-zero sequence; verify `ProducerUnknown`.
6. Repeat with a 3-node cluster and a node restart if time permits.

### Risk

- The example uses the low-level path while the SDK direction is the high-level producer. Label it clearly and explain why.
- Never claim `.idempotent()` survives a process restart.
- If maintainers prefer to teach `send_producer_messages` only in docs, downgrade to a README expansion and keep the example as a follow-up.
