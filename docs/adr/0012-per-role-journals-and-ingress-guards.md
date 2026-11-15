# ADR 0012: Per-role journals, shared ingress guards, and snapshot verification

- **Status:** Accepted — implemented
- **Date:** 2026-11-15
- **Related:** [ADR 0010](0010-shard-thread-and-journal-pipeline.md),
  [ADR 0011](0011-fault-injection-and-stage-budget.md),
  [PERFORMANCE.md](../PERFORMANCE.md), [FAILURE-MODES.md](../FAILURE-MODES.md)

## Context

The shard thread isolated matching from the journal, but the gateway and the
engine still shared one Iggy client. A slow execution/MD append could
head-of-line block order ingress on that client. Separately, HTTP and TCP each
built their own rate-limit bucket, the WebSocket upgrade path had no cap, and
the snapshot hash was only verified on restore.

## Decision

1. **Two journal clients.** `NodeContext` carries `journal` (ingress: gateway
   and TCP) and `engine_journal` (shard reader/writer, MD consumer, lag). The
   memory journal shares one instance; Iggy opens two connections.
2. **One rate limiter.** A single sharded per-IP token bucket is shared by HTTP
   and TCP. It is charged per order, not per request, so a 256-order batch
   costs 256 tokens. TCP `Hello` is charged; `Ping` is not.
3. **Caps.** TCP connections are capped at 1,024; WebSocket subscribers at
   `WHIPPET_MAX_WS_CONNECTIONS` (default 2,048). Both release permits through
   RAII when the connection ends.
4. **Snapshot verification.** The writer fills the per-symbol hashes and
   verifies the **final** snapshot by rebuilding the shard before the atomic
   rename. Periodic snapshots fill and save without the rebuild; restore
   verifies every snapshot, so a bad periodic file is rejected rather than
   used.
5. **Health covers both links.** The health monitor pings both journals and
   `/readyz` requires both. A persistent poll failure (~10 s) marks the shard
   failed and ingress returns 503.
6. **Resume point.** No snapshot, or a snapshot with no applied commands,
   resumes at offset zero. The broker's committed offset seeds only the commit
   watermark.

## Consequences

- A slow engine output append no longer delays a gateway append.
- A client cannot double its budget by switching transports.
- A crash during a periodic snapshot may leave an unverified file; restore
  rejects it and recovery falls back to the previous valid snapshot or a full
  replay.
- A transient snapshot write failure degrades recovery, not matching; the log
  is canonical.
- The final snapshot pays a rebuild; periodic snapshots do not, to protect the
  p99.

## Alternatives rejected

- **One client with priority lanes.** The SDK has no per-request priority, so
  a second connection is the simplest isolation.
- **Verify every snapshot.** The rebuild at 100k dedup entries is a
  multi-hundred-millisecond spike on the blocking writer; final-only keeps the
  guarantee where it matters most.
- **Per-order rate limiting in middleware only.** The body size is unknown
  before parsing; the batch handler charges the remainder.
