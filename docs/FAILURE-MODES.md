# Failure modes

What the Phase 1 build does when something fails. This is a behavior map, not
a claim that every path has been fault-injected. The proof column distinguishes
three things:

- **Tested**: an automated unit, property, or integration test covers the
  mechanism.
- **Implemented, untested**: the code path exists and is exercised in normal
  operation, but there is no crash or fault-injection test.
- **Planned**: not implemented in this build.

No failure test results are invented here. The only replay evidence quoted is
the single local determinism check in [DEMO.md](DEMO.md#replay-the-log).

## Matrix

| Failure | Current behavior | Guarantee today | Proof |
|---|---|---|---|
| **Gateway process crash** | In-flight HTTP requests fail. If the append completed but the response was lost, the order is in the log and the client saw an error. A retry with the same `client_order_id` derives the same `OrderId`; the engine rejects it as a duplicate, so there is no second effect. | At-most-one business effect per `client_order_id` while the dedup entry is retained. The client must reconcile an ambiguous acknowledgement; there is no client-side once-only response. | Implemented. Duplicate-id rejection is unit-tested (`duplicate_order_id_is_rejected`). No gateway crash test. |
| **Engine crash after publishing executions/MD, before committing the input offset** | The batch is replayed from the last durable point. Executions and MD records from the replayed batch can be appended a second time. The engine state converges because replay is deterministic. The MD hub ignores duplicate MD sequences; executions have no Phase 1 consumer. | At-least-once output. Engine state is deterministic; downstream consumers must deduplicate by `(symbol, cause_offset, seq)`. No end-to-end once-only guarantee. | Implemented error path. `replay_is_deterministic` and `snapshot_restores_exact_state_hash` cover determinism. No kill-between-publish-and-commit test. |
| **Engine restart from a snapshot older than the committed offset** | The engine prefers the snapshot's maximum `last_offset` over the committed offset, so the tail after the snapshot is replayed. Outputs after the snapshot can be published twice. This is the normal crash window when the last snapshot is older than the last commit. | Same as above: deterministic state, at-least-once output, downstream dedup required. | Implemented by design. No crash test. |
| **Duplicate input** | The engine's dedup table, keyed by the derived `OrderId`, rejects the second command with `duplicate_order_id` and emits no fill. Capacity is `2 * max_orders` per symbol (500,000 by default). Eviction prefers terminal entries; active resting orders are protected. The window is still count-based, so a very old filled id can be re-accepted after eviction. | One effect per order id inside the retained window. An id older than the window can be accepted again. | Tested (`duplicate_order_id_is_rejected`, `capacity_evicts_oldest_terminal_entry`). |
| **Stale input** | There is no epoch, session sequence, or wall-clock staleness check. A command with a new `client_order_id` is accepted and matched in log order even if its `timestamp_ns` is old. The timestamp is informational. Price validation still applies: tick multiple, ladder bounds, and the 500 bps market collar. | Ordering is the log order, not the timestamp. Stale commands are not rejected in Phase 1. | Not implemented by design. No test because there is no mechanism. |
| **MD consumer commit failure** | The runner retries the offset commit only; it does not re-apply the batch. If the process dies before the commit, the batch is replayed, but the hub ignores records with `seq <=` the last applied sequence, so levels, the tape, and metrics do not double-count. | The view converges without duplicate effects. A snapshot is still the recovery point. | Implemented. Duplicate delta/trade and stale-snapshot tests. |
| **MD client slow or disconnected (browser)** | The node publishes one full state frame over a `watch` channel at 20 Hz. A slow client misses intermediate frames; it does not block the engine or other clients. On reconnect the client fetches `/api/state` and opens the socket again. There is no sequence-gap detection or per-client resync protocol. | The client sees the latest full state, not every event. The dashboard shows `STALE` after three seconds without a frame. | Implemented. No automated slow-client or reconnect test. |
| **Iggy down** | Gateway appends fail; the order is not accepted and the client gets HTTP 503 `journal_unavailable`. Engine and MD polls log a warning and retry after 250 ms. Engine output appends are retried with 100 ms to 2 s backoff up to 30 attempts; on exhaustion the shard is marked failed and stops, and its applied offset and commit do not advance. The health monitor marks `degraded` on the next five-second check. The dashboard keeps the last state and shows a stale banner. There is no local retry queue. | No silent acceptance while the broker is unavailable. No loss of an acknowledged order caused by a broker outage, because the gateway does not acknowledge, and a wedged output topic cannot silently stop matching while the gateway keeps acknowledging. Recovery depends on the Iggy client reconnecting; restart the node if it does not. | Implemented error paths. No broker-outage fault-injection test. |
| **Batch ingress validation failure** | The gateway charges the batch against the rate limit before validation, then validates every order before appending. One invalid order rejects the whole batch with its index; no partial batch is written. A batch over 256 orders is rejected (`max_batch`), and a batch larger than the per-second budget is rejected with 429 `rate_limited` and a `Retry-After: 1` header. | No partial business effect from a rejected batch. A client can split and retry valid orders. | Implemented. Gateway batch tests. |
| **Log gap / retention truncation** | The reader task validates the whole polled batch, not just its first record: the first offset must equal the requested offset and the offset span must equal the record count. If the broker has already dropped the requested range, or the batch contains an interior gap or overlap, the shard is marked failed and stops with a clear gap error; it does not apply a partial log to the book. | No silent partial state. Recovery requires a snapshot or a reset. | Implemented guard; no Docker retention test. |
| **Group-commit append failure** | The per-partition writer retries the whole drained batch. Every waiter in that batch sees the same outcome; no waiter is dropped. A retry can duplicate messages, and the engine deduplicates by order id. | At-least-once ingress; no silent loss, no per-waiter ack skew. | Implemented. `group_commit` tests cover contiguous offsets and split batches. |
| **Commit window (256 records or 100 ms)** | The shard commits its applied offset in the background. A crash can replay up to the window from the last snapshot. Books are deterministic, so the state hash matches; execution records may be duplicated, and MD consumers drop duplicates by per-symbol sequence. | At-least-once outputs; bounded replay window. | Implemented. Replay proof reruns after the change. |
| **TCP client disconnects before the ack** | The order may already be in the log. The client retries with the same client order id; the gateway derives the same order id and the engine rejects the duplicate without matching it twice. | At-most-once business effect per order id, at-least-once log record. | Implemented. TCP tests cover ack offsets; no automated disconnect test yet. |
| **Stale snapshot write** | The snapshot writer carries a monotonic sequence and refuses to overwrite a newer file. A dropped snapshot is logged; the next one supersedes it. | An older snapshot can never replace newer state. | Implemented. `snapshot::tests::stale_snapshot_never_overwrites_newer_state`. |
| **Iggy restart under load** | The SDK can retain an in-flight mutation with an unknown outcome and block its lane. The journal resolves the named operation with a short deadline, falls back to abandoning it, and retries the operation once. The node reconnects and resumes from the last committed offset; outputs may be duplicated and are deduplicated downstream. | No indefinite lane block; at-least-once output. | Implemented; live restart was observed and the workaround is in `IggyJournal::recover_operation`. No automated broker-restart test yet. |
| **Iggy disk or volume loss** | The log and the consumer offsets are gone. Snapshots alone do not contain the missing tail. | No protection. `persisted` is a local-disk promise, not a backup. | Not protected. Use a cold volume copy if you need one. |
| **Snapshot corruption** | `ShardEngine::from_snapshot` recomputes the hash and returns an error on mismatch. The shard task exits with that error, marks the node unhealthy, and the file is not used. `whippet verify` reports the failure. | A corrupt snapshot is never silently applied; the node reports degraded health instead of claiming to match. | Tested (`snapshot_round_trip_and_hash_verification`, `snapshot_restores_exact_state_hash`). |
| **Empty or stale snapshot** | `whippet verify --require-snapshot` fails closed when a shard has no snapshot file instead of reporting success with zero shards. `whippet replay --require-snapshot` refuses the silent fallback to a full replay from zero; the replay summary reports the broker's committed offset and the tail length after the snapshot, so a missing file or a snapshot older than the committed offset is visible. The node itself still starts at offset zero when there is no snapshot (the committed offset only seeds the commit watermark); if retention has truncated the log, the reader hits a gap and the shard fails closed. | No silent "verified nothing" or "replayed everything" when the snapshot is absent or stale. | Implemented guards; no Docker retention test. |
| **Input ring full** ([ADR 0010](adr/0010-shard-thread-and-journal-pipeline.md) pipeline) | The reader's `send` onto the input ring (capacity 2) awaits until the matcher drains a batch. Nothing is dropped or skipped; log order is preserved by construction. Throughput limits to the matcher until it catches up. | No silent loss at the reader; backpressure, not buffering. | Implemented. No ring-full fault-injection test yet. |
| **Output ring full** | The matcher blocks in `blocking_send` on the output ring (capacity 2) until the writer drains space. Executions and MD events are never dropped, because a dropped output breaks the at-least-once contract and `md_seq` continuity. The input ring fills as a consequence, which propagates backpressure to the reader. | At-least-once output preserved; no skipped events under writer backpressure. | Implemented. No ring-full fault-injection test yet. |
| **Matcher thread panic** | A panicked matcher thread cannot match or commit. The shard's `join` detects the dead thread, the shard is marked failed, and the shard errors out instead of waiting on a ring that will never drain. Restart and replay recover from the snapshot plus the log. | No hang; the shard fails visibly and loudly, and recovery is the normal at-least-once path. | Implemented (panic is surfaced through the thread join). No panic-injection fault test yet. |
| **Output append fails on one topic** | The writer retries the failing append up to 30 times with 100 ms to 2 s backoff and a shutdown watch. On exhaustion the shard is marked failed and stops; the applied offset and the consumer commit are **not** advanced, so a restart replays from the last durable point instead of skipping orders whose executions or MD never reached the log. | No silent stop: a wedged output topic degrades the node visibly, and no order is skipped or lost on restart. Before this bound, an endlessly retrying append stalled matching while the gateway kept acknowledging new orders. | Implemented error path; no fault-injection test yet. |
| **Snapshot hash mismatch** | The matcher calls `snapshot_state_without_hash`; the blocking writer fills the hashes with `hash_symbol_snapshot`. The **final** (shutdown) snapshot is rebuilt and verified before the atomic rename; periodic snapshots fill and save without the rebuild. Every snapshot is verified on restore. The earlier ~170k-order mismatch was the dedup count (walk length vs distinct ids); fixed and pinned by a 100-engine × 200k-command test. | A wrong hash cannot become the authoritative resume state. | Implemented; periodic write-time verification is deliberately off to protect the p99. |
| **Engine journal link loss** | The node opens a separate Iggy connection pool for the engine (`WHIPPET_ENGINE_JOURNAL_CLIENTS`, one client per active shard by default; `engine_journal_for(shard)` maps each shard deterministically). The health monitor pings both the ingress link and the engine pool, and `/readyz` requires both. A poll failure is retried for ~10 s, then the shard is marked failed and ingress returns 503, so orders are not acknowledged while nothing can match them. | No acked-but-unmatched orders after the failure budget. | Implemented; no two-broker fault test yet. |
| **Snapshot write failure** | A failed snapshot write is logged and matching continues: the order log is canonical. The next snapshot retries. A dead snapshot writer (channel closed) still marks the shard failed, because there is no resume point at all. | A transient disk fault degrades recovery, not matching. | Implemented; no fault-injection test yet. |
| **Simulator crash** | The simulator has counters for sent, errors, and dropped, but no acknowledged ledger. Unacknowledged actions are simply not counted. An explicit `--seed` reproduces the same order ids; without `--seed`, a fresh run nonce keeps a restart from replaying old ids. | No loss claim is made from simulator counters. Use a new seed or omit it for a fresh flow. | Implemented. Determinism is tested (`same_seed_same_actions`). No crash test. |
| **Dashboard stale** | `freshness_ms` is the age of the newest MD event. Above 3,000 ms the Live page shows a stale banner and the connection pill shows `STALE`. The dashboard does not fake a live state. | The operator can see staleness. | Implemented. No automated browser test. |
| **Partition count changed after topics exist** | `ensure_topics` compares the existing partition count with the configured count and refuses to start on a mismatch. It does not silently route new orders into a different layout. | Nothing is migrated. Reset the Iggy volume or keep the original count. | Implemented guard; no Docker test. |

## What is deliberately not claimed

- Once-only delivery, or once-only client responses.
- Zero loss for an unacknowledged in-flight request.
- Zero RTO.
- Survival of two failures, or of any failure on a single node.
- Contiguous offsets.
- Cross-partition transactions.
- Linearizable reads.
- That a snapshot is a backup.

## Proof that would be needed

These are the artifacts Phase 2 should produce before the corresponding rows
can move from "implemented, untested" to "proven":

- **Gateway crash:** kill -9 the gateway under load, retry every ambiguous
  request with the same client id, and compare the acknowledged-offset ledger
  with the readable log. Assert one effect per id.
- **Engine crash window:** kill the engine between the output append and the
  offset commit. Restart. Compare the state hash with a clean replay, count
  duplicate execution records by `(symbol, cause_offset, seq)`, and show that
  the MD levels converge.
- **Duplicate replay:** replay the same batch ten times and assert one trade
  per order and an unchanged state hash.
- **Slow MD client:** throttle a subscriber, disconnect it, reconnect, and
  show that the node's publish rate and the engine are unaffected and that the
  client resyncs to the latest state.
- **Iggy outage:** stop the broker, submit orders, assert 503 and no
  acknowledgement, restart the broker, and show that the node resumes without
  restarting or losing committed state.
- **Disk loss:** delete the Iggy volume, restart, and document exactly what is
  lost. This is a negative control, not a recovery demo.

The existing tests live in `crates/whippet-engine/tests/properties.rs`, the
engine unit tests, the journal tests, and the snapshot tests. They prove
matching invariants, dedup behavior, deterministic replay, and snapshot hash
restoration. They do not prove crash safety.
