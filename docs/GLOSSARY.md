# Glossary

Terms used in the code, the dashboard, and the docs. Definitions are for this
project, not for every venue or vendor.

## Market structure

**MBO (market by order).** A feed that publishes changes to individual resting
orders: add, reduce, remove. Whippet's `md` topic is MBO. It lets a consumer
reconstruct price levels, not just the top of book.

**MBP (market by price).** A feed that publishes aggregated price levels. A
consumer sees quantity at a price but not which orders make it up. Whippet
does not publish an MBP feed; the dashboard builds levels from MBO events.

**Price-time priority.** The matching rule: best price first; at the same
price, the order that arrived first trades first. Whippet implements this with
a price ladder, an occupancy bitmap, and a FIFO list per level.

**Tick.** The smallest price increment for an instrument, stored as an integer.
BTC-USD uses 100-cent ticks, ETH-USD 10-cent ticks, SOL-USD 1-cent ticks. No
float ever reaches the engine.

**Ladder.** The contiguous array of price levels from the instrument's minimum
to maximum tick. The ladder index is the price in ticks; the bitmap marks
which levels are occupied. A price outside the ladder is rejected.

**Spread.** Best ask minus best bid. A crossed book (best bid at or above best
ask) is a bug; the matcher never leaves one at rest.

**Self-trade prevention (STP).** Whippet uses cancel-resting: if an incoming
order would trade with a resting order from the same participant, the resting
order is cancelled and the incoming order continues.

**Order id.** A 128-bit id derived with BLAKE3 from `(symbol,
client_order_id)`. The same client id always maps to the same order id, which
is what makes retries and the dedup table work.

## Log and delivery

**Partition.** An ordered append-only sequence inside a topic. Ordering is
guaranteed within a partition, not across partitions. Whippet hashes the symbol
to a partition and pins the partition id on append.

**Offset.** The position of a record inside one partition. Offsets are
per-partition and are not guaranteed contiguous after retention or recovery.
Never use an offset as a count.

**Consumer group.** A named group of consumers that split a topic's partitions
and share progress. Whippet Phase 1 does not use groups: each engine and MD
task is a fixed named consumer with its own manual offset. The terms are not
interchangeable.

**Manual commit.** The consumer stores the offset only after it has applied
the batch. Whippet commits after publishing executions and MD (engine) or after
applying MD records (hub). A crash between work and commit means the batch can
be replayed.

**At-least-once.** A record can be delivered more than once, never silently
dropped by the log. Whippet is at-least-once. The engine deduplicates order
ids; downstream execution/MD consumers must deduplicate by `(symbol,
cause_offset, seq)`.

**Quorum ack.** An acknowledgement that a write reached a quorum of replicas.
Whippet does not use a quorum in this build. The gateway acknowledges after a
single-node Iggy append. The phrase appears in the research docs as a future
cluster property, not as a current claim.

**Persisted vs replicated.** Two Iggy durability policies. `persisted` waits
for the local disk before acknowledging; `replicated` waits for replication to
the cluster quorum. They are separate policies, not synonyms. On a single node
there are no replicas, so `replicated` is a requested policy, not a second
copy. Orders and executions are `persisted`; MD requests `replicated`.

**VSR (viewstamped replication).** The consensus protocol behind an Iggy
cluster. Not in this build; Phase 3.

**PAT (personal access token).** A scoped Iggy credential. Not in this build;
Phase 2.

**Cause offset.** The input offset that produced an output event. Every
execution and MD record carries it, so replay can tie an output back to the
command that caused it.

**Replay.** Re-applying the input log to a fresh engine. `whippet replay`
rebuilds a partition's state and prints a hash. It is not the same as the
dashboard replay scrubber, which is Phase 2.

## State and recovery

**Snapshot.** A serialized copy of a shard's books, dedup table, risk table,
session/halt state, sequence counters, and last input offset. Whippet writes
one file per active partition, atomically via temp file, `fsync`, and rename.
A snapshot is a recovery accelerator, not a backup of the log.

**State hash.** A BLAKE3 digest over the book levels and FIFO order, the dedup
entries, the per-participant risk table (limits and exposure), both sequence
counters, the last offset, the last timestamp, the halt flag, and the session.
The shard hash combines the per-symbol hashes. Equal hashes after replay mean
the engine reached the same state.

**Dedup table.** Whippet's business-level table of order ids and their status.
It is snapshotted with the book and the input offset. Iggy's idempotent
producer dedup is a different mechanism and is not used here. Capacity is
`min(2 * max_orders, 100,000)` entries per symbol with terminal-first
eviction. The window is count-based, not a time TTL, and an id evicted from
the window can be re-admitted.

**Engine risk.** Per-participant, per-symbol limits enforced by the matcher:
`max_net_qty` bounds the worst-case absolute position (net position plus
resting orders on the side that could fill) and
`max_open_buy_notional_cents` bounds quote cents in resting `Limit`/`PostOnly`
buys. Limits arrive as log-ordered `SetRisk` commands and are part of the
state hash and snapshot. Distinct from the **ingress guard**, a stateless
per-order quantity/notional filter at the gateway.

**Exposure.** What a participant currently has at risk in one symbol: net
position (buys filled minus sells filled), open buy/sell quantity in the book,
and open buy notional. It moves only with rests, cancels, and fills.

**Single writer.** Exactly one task applies commands to a partition's books.
Whippet gets this from one dedicated matcher OS thread per active shard, not
from thread pinning. There is no lock in the engine.

**Conflation.** Replacing intermediate states with the latest one. The MD hub
publishes one full state frame at 20 Hz instead of every event. Trades are
retained in the visible tape window; the log remains the lossless record.

## Benchmarking

**Open-loop.** A generator that schedules requests at absolute deadlines and
does not wait for responses before scheduling the next one. Closed-loop
generators slow down when the system slows down, which hides queueing.

**Coordinated omission.** The error where a benchmark stops issuing requests
while the system is stalled, so the stall never appears in the latency
distribution. Whippet measures latency from the scheduled deadline, not from
the send, and records generator lateness.

**Scheduler slip.** How late the generator itself was relative to the absolute
deadline. It is a first-class metric. A run with large slip is generator-bound,
not system-bound.

**HDR histogram.** A histogram with bounded relative error that records high
dynamic range without storing every sample. Whippet uses it for p50 through
p99.99 and max. Percentiles are never averaged across runs.

**SLO knee.** The throughput point where latency starts rising sharply. The
useful capacity is near the knee, not the maximum rate the system can absorb
while queueing without bound.

**Goodput.** Throughput of successful work. Whippet's manifest reports both
achieved rate and goodput, plus an error count.

**Dev-loop vs headline.** `dev-loop` is a measurement on a development
machine, Docker Desktop, OrbStack, or a Linux VM. It is useful for regression
and relative comparisons. `headline` is reserved for a native Linux host with
the method card and manifest. The manifest's `scope_label()` makes the
distinction; M4 and Docker Desktop are never headline.

**Smoke.** A short single-repetition run that checks the harness and the
stack work and records the rough shape of a result. Not a benchmark.

**Soak.** A sustained constant-rate run (minutes or longer) that checks for
latency drift, queue growth, RSS growth, and log growth. A soak that fails
the drift gate is evidence of non-steady-state, not capacity.

**Microbench.** A narrow criterion measurement of one component (matcher,
hub apply, snapshot stage) with no broker, network, or async runtime on the
measured path. It answers "did this code get faster", not "did the exchange
get faster".
