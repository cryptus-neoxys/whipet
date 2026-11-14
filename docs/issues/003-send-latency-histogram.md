# 003 — Expose send commit latency on `/metrics`, matching the existing fetch histograms

**Status:** ready to file
**Type:** Feature request
**Area:** Server / observability
**Priority:** P3
**Effort:** S–M
**Duplicate check:** searched `send latency`, `produce latency`, `append latency`, `write latency`, `ack latency`, `latency histogram`, `send_messages metrics` in issues and PRs. No duplicate. #19 is the original “Implement & expose custom server metrics for Prometheus” issue; #4683 added the fetch histograms, not send.

---

## GitHub issue body

### Description

`/metrics` exposes two fetch-path latency histograms, `readiness_push_to_fire_seconds` and `fetch_fire_to_reply_seconds`, but the send path has no latency histogram. Operators can chart how long a fetch waits and how long it takes to reply, yet no series measures the produce acknowledgement latency itself. A p99 regression caused by consensus, WAL fsync, queueing, or a slow replica is only indirectly visible through WAL pressure counters and consumer lag; the end-to-end signal is client RTT, which mixes in network and client scheduling.

The asymmetry looks unintentional and makes the server harder to operate under load.

### Affected area/component

- Iggy server, shard metrics, `/metrics` scrape surface
- Performance / observability

### Current behavior

- `readiness_push_to_fire_seconds` is declared and registered in `core/shard/src/metrics.rs:397,1409` at the verified checkout.
- `fetch_fire_to_reply_seconds` is declared and registered in `core/shard/src/metrics.rs:1453,1510`.
- The send path has no latency histogram. The only per-request series is `client_requests_total{command}` (`core/shard/src/metrics.rs:1402-1406`, `ClientRequestMetrics` at `:1544`). WAL pressure counters such as `partition_wal_batches`, `partition_wal_prepares`, `partition_wal_group_commit_waits`, and `partition_wal_queued_bytes` exist, but they are not a latency distribution.
- The send dispatch has a timing seam: `core/server/src/dispatch/partition.rs:369` submits, `:387-389` spawns a task that awaits the committed reply, and `:417-424` sends it to the client. Nothing reads the monotonic clock.
- The fetch analogue is `core/server/src/dispatch/fetch.rs:189`.
- Client-side benchmarking measures produce latency, but the server does not export it.

Line numbers are from the verified checkout `9ba0ab2ef`; on upstream `c78da20` the metrics declarations moved (for example, `readiness_push_to_fire_seconds` is around `:398/:1478` and `ClientRequestMetrics` around `:1628`).

### Proposed solution

Add one histogram that mirrors the existing fetch pattern:

- Add `send_submit_to_reply: Histogram` to `ShardMetrics`, initialize it with `latency_histogram()`, and register it as `send_submit_to_reply_seconds` with help text such as “time from partition submit to its reply”.
- In `relay_partition_reply`, capture `shard.bus.monotonic_micros()` immediately before `partition_submit_attached`, but only when the operation is `SendMessages`. Carry the timestamp into the spawned task and record the duration after `await_partition_submit` returns a reply, before `send_host_frame`.
- Decide explicitly whether error replies count. If the metric is meant to measure commit latency, filter on a successful reply and name/help it accordingly; if it includes errors, say so and do not call it “committed.”
- This measures the dispatch round trip, not transport write time, and covers TCP, QUIC, and HTTP because HTTP uses the same dispatch path.
- Do not sample or gate it. `prometheus_client` histograms use a handful of atomics, cardinality is one histogram per shard, and recording happens in the already-spawned reply task, off the connection's drain loop.

### Alternatives considered

- **Instrument the partition/WAL owner with `append_latency_seconds`.** This would separate disk attribution from consensus and queueing, but it touches the persistence hot path and is a higher-risk change. It can be a follow-up if operators need disk-level attribution.
- **Rely on client-side benchmarks.** This is what users do today, but it mixes in network and client scheduling and is not available in production.
- **Add per-message tracing spans.** OTLP trace export exists but is off by default and batched; per-message spans are too expensive for the hot path.
- **Add a failure/outcome dimension instead.** Useful, but orthogonal: a new `client_request_errors_total{command,code}`-style metric can be a separate proposal.

### Additional context

- The metric should describe what it measures precisely. “Submit to reply” is accurate; “append latency” is not, because the WAL owner is a different stage.
- Consensus latency dominates in a cluster, so this metric will not isolate WAL disk stalls. Note that in the help text or docs.
- The fetch histograms are the precedent; matching their bucket configuration and registration style keeps the scrape surface consistent.

### Contribution

I can implement the histogram, the dispatch timing, and the tests. This touches the dispatch hot path, so I will wait for design agreement and assignment before opening a PR.

---

## Internal notes (do not paste)

### Files to touch

- `core/shard/src/metrics.rs` — field, initialization, registration, unit test.
- `core/server/src/dispatch/partition.rs` — capture and record around the send submit/reply.
- `core/server/src/dispatch/fetch.rs` — reference for the timing pattern.
- `core/integration/tests/server/topic_metrics_vsr.rs` (or a sibling) — integration test.

### Test plan

1. Unit in `core/shard/src/metrics.rs`: record two durations; scrape; assert `send_submit_to_reply_seconds_count 2` and bucket boundaries; record a fetch duration and assert it does not move the send histogram.
2. Integration: send a batch over TCP and HTTP; scrape authenticated `/metrics`; sum the per-shard `send_submit_to_reply_seconds_count`; assert +1.
3. Denial: use a **pre-submit** denial (RBAC refusal, unresolved namespace, or routing refusal) and assert no increment. A partition-plane error that returns a reply will increment if the metric counts all replies; document the chosen semantics.
4. Store a consumer offset and assert no increment.
5. Run `iggy-bench` before/after to confirm no throughput regression.

### Risk

- Maintainers may view produce latency as a client concern. Frame it as operator attribution and cite the fetch histograms as precedent.
- The name/help must say “submit to reply,” not “append” or “client ack.”
- Adding a clock read on the send path is measurable; keep it to one `monotonic_micros()` call per qualifying send and record in the reply task. If maintainers object, make it opt-in via config.
- Request design approval before coding; this is a concurrency-sensitive dispatch change.
