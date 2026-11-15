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

| # | Decision | Detail |
|---|---|---|
| 1 | **Per-role + per-shard engine journal pool** | `NodeContext` carries `journal` (ingress: gateway and TCP) and an engine pool of `WHIPPET_ENGINE_JOURNAL_CLIENTS` Iggy clients (default = active shard count, clamped 1–16). `engine_journal_for(shard)` maps each shard deterministically to one pool client; the shard reader/writer tasks, both MD consumers, and the consumer-lag sweep (`committed`/`lag` per active shard, summed into `whippet_consumer_lag`) all select through it, never the ingress client. The pool's total pending-operations budget is `WHIPPET_ENGINE_JOURNAL_PENDING_MIB` (default 256 MiB, clamped 16–4096), split as `max(16 MiB, total / pool_size)` per client; the ingress client keeps its own 256 MiB. The `engine_journal` observer handle shares pool entry 0 and opens no extra connection; neither the health monitor nor the lag sweep reads through it. The memory journal shares one instance; Iggy opens the pool connections. |
| 2 | **One rate limiter** | A single sharded per-IP token bucket is shared by HTTP and TCP. It is charged per order before validation, not per request, so a 256-order batch reserves 256 tokens up front and a batch larger than the per-second budget is rejected. TCP `Hello` is charged; `Ping` is not. A rejected request returns 429 `rate_limited` with `Retry-After: 1` (TCP: the `RateLimited` error code). |
| 3 | **Caps** | TCP connections are capped at 1,024; WebSocket subscribers at `WHIPPET_MAX_WS_CONNECTIONS` (default 2,048) plus a per-IP sub-limit. Both release permits through RAII when the connection ends. |
| 4 | **Snapshot verification** | The writer fills the per-symbol hashes and verifies the **final** snapshot by rebuilding the shard before the atomic rename. Periodic snapshots fill and save without the rebuild; restore verifies every snapshot, so a bad periodic file is rejected rather than used. Snapshot format is v4 (terminal-eviction order as `u32` indices into a compacted dedup walk); the terminal queue is hashed with the live ids, so a reordered-but-valid queue changes the hash and fails verification. Restore rejects out-of-range, non-terminal, and missing indices; repeated indices for the same id are allowed. Versions 3 and older are rejected. |
| 5 | **Health covers both links** | The health monitor pings the ingress client and every engine pool client concurrently and caches one aggregate flag; `/readyz` reads the cached flag and performs no journal I/O. A persistent poll failure (~10 s) marks the shard failed and ingress returns 503. |
| 6 | **Resume point** | No snapshot, or a snapshot with no applied commands, resumes at offset zero. The broker's committed offset seeds only the commit watermark. `verify --require-snapshot` requires a snapshot for every active shard, and `replay --require-snapshot` fails closed (unseeded run, any skipped record, or `snapshot_offset >= head`) instead of silently falling back; replay reports `snapshot_offset`, `committed_offset`, `head`, and `tail`. |

## Consequences

- A slow engine output append no longer delays a gateway append, and one
  shard's slow output lane no longer head-of-line blocks the other shards:
  each shard has its own engine connection by default.
- Connection count is `1` (ingress) + `WHIPPET_ENGINE_JOURNAL_CLIENTS`
  (default = active shard count, max 16) Iggy connections per node, plus the
  health-monitor pings on the same clients. Size the broker's connection
  budget accordingly.
- A client cannot double its budget by switching transports, and cannot spend
  validation work outside its budget: the batch is charged before validation.
- Snapshot writes are atomic (temp file, fsync, rename), so a crash mid-write
  leaves the previous file in place. A persisted file that fails verification
  fails its shard closed instead: there is no automatic fallback to a previous
  file or to a full replay, and `whippet replay` never writes a snapshot.
  Recovery requires operator action (delete or move the files, or `make
  reset`, then full-replay from 0).
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
