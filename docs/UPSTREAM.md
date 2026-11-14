# Upstream friction log

This is the running record of friction found while building Whippet on Apache
Iggy. It exists so that a bug, a docs gap, or an ergonomics problem becomes a
reproducible upstream issue instead of a workaround buried in this repository.

Whippet is an independent Apache-2.0 showcase. It is not affiliated with,
sponsored by, or endorsed by the Apache Software Foundation. Apache Iggy is a
project of the Apache Software Foundation.

## Verified baseline

| Item | Value |
|---|---|
| Whippet | `0.1.0` (Phase 1 durable slice) |
| Iggy server | `apache/iggy:0.10.0` (released 2026-11-07) |
| Iggy Rust SDK | `iggy = "=0.12.0"` |
| Binary protocol | `0.12.0` on the pinned server |
| Host used for the friction log | Apple M4, 16 GB, Docker Desktop 29.9.2, macOS (Darwin 27.2.0) |
| Verification date | 2026-11-14 |
| Local Iggy checkout | fork commit `9ba0ab2ef` on upstream `15cd1627f` (PR #4848, 2026-11-13) |
| Upstream `master` at review | `c78da20` (2026-11-14), 33 commits ahead of that base |

Line numbers in the upstream tree move. Re-grep before filing; do not paste a
line number from this log into an issue without checking it against the pinned
release or current `master`.

## Filing rules

1. **Issue first.** Search open and closed issues and pull requests. If the
   problem is already tracked, comment with new evidence instead of filing a
   duplicate. For a code change, wait for maintainer agreement and assignment;
   new functionality without an approved issue can be closed.
2. **Reproduce on the pinned release.** Use `apache/iggy:0.10.0` and SDK
   `0.12.0`, not a random local build. If the behavior differs on current
   `master`, say so and give both commits.
3. **One issue, one problem.** Do not bundle a docs gap, an SDK ergonomics
   request, and a server bug in one report. Do not combine unrelated cleanups in
   one pull request.
4. **Include the evidence.** Version, platform, deployment shape, logs,
   expected/actual behavior, a minimal reproduction, and a proposed approach.
   Redact credentials and host-specific paths.
5. **Do not paste private notes.** The local issue drafts in `docs/issues/` are
   working notes and are intentionally not committed. Summarize the evidence;
   do not copy internal speculation or unverified claims.
6. **Run the local checks before a PR.** `fmt`, `clippy`, tests, `machete`,
   `sort`, and `typos` in the upstream checkout. Link the issue. Respond to every
   review comment. Do not rebase for freshness.
7. **Security issues go private.** Use the upstream security process, not a
   public issue. For Whippet itself, see [SECURITY.md](../SECURITY.md).
8. **No benchmark claims without a manifest.** A number needs hardware, version,
   method, and raw artifacts. Do not use a Docker Desktop result as a headline.
9. **No ASF endorsement claim.** Whippet is a showcase, not an official Apache
   project. Do not imply otherwise in an issue, PR, or launch post.

Status vocabulary: `open` (not filed or filed and unresolved), `coordinate`
(waiting on another issue/PR), `fixed` (resolved upstream or by a documented
workaround), `drop` (investigated and deliberately not filed), `workaround`
(app-side mitigation for an upstream gap, not an upstream bug).

---

## U-001: Iggy 0.10 needs `io_uring`, and Docker's default seccomp profile blocks it

- **Date:** 2026-11-13
- **Version:** server `0.10.0`, Docker Desktop 29.9.2
- **Status:** open (docs / Compose onboarding)

**Symptom.** The server exits during startup under the default Docker seccomp
profile. The container needs `--security-opt seccomp=unconfined` (or a custom
profile that allows the required `io_uring` syscalls). Without it, a first-time
user sees a startup failure before any Whippet code runs.

**Minimal repro.**

```bash
docker run --rm -p 8090:8090 \
  -e IGGY_ROOT_USERNAME=iggy \
  -e IGGY_ROOT_PASSWORD=whippet-local \
  apache/iggy:0.10.0
```

The server fails on the default profile. Adding
`--security-opt seccomp=unconfined` lets it start.

**Impact.** This is the first command in the quickstart. A failure here costs
more trust than a later performance surprise. The workaround also weakens the
container's syscall filtering, so users need to know why it is required and what
the narrower alternative is.

**Proposed action.** Document the requirement in the Docker/Compose quickstart
and in the server configuration docs; add a startup check with an actionable
error instead of a raw syscall failure; consider a documented seccomp profile
that allows only the needed syscalls. A no-`io_uring` fallback is a larger design
change and should be discussed separately.

**Whippet workaround.** `deploy/compose.yaml` sets
`security_opt: [seccomp=unconfined]` and documents why. This is an onboarding
workaround, not a fix.

---

## U-002: Root credentials are required, and the default-credentials flag is discouraged

- **Date:** 2026-11-13
- **Version:** server `0.10.0`
- **Status:** open (docs / Compose onboarding)

**Symptom.** A fresh server refuses to start without explicit root credentials.
The `--with-default-root-credentials` path exists but is discouraged because it
creates a known `iggy` / `iggy` account. Compose examples that omit the
environment variables fail, and examples that use the flag teach an insecure
default.

**Minimal repro.** Start `apache/iggy:0.10.0` with no root username/password
environment variables and no default-credentials flag. The server refuses to
start. Start it with `--with-default-root-credentials` and it starts with a
well-known account.

**Impact.** Good security default, bad onboarding. Users either cargo-cult the
insecure flag or copy an environment block that does not match the image's
variable names. The failure is easy to misread as an image or volume problem.

**Proposed action.** Make explicit root credentials the only documented path;
name the required variables in the startup error; update Compose examples to use
a secret or an explicit `.env` value; keep the default-credentials flag for
disposable local tests only, with a warning that says exactly that.

**Whippet workaround.** `deploy/compose.yaml` uses `IGGY_ROOT_USERNAME` plus a
Compose secret for `IGGY_ROOT_PASSWORD`, and the Whippet entrypoint reads
`WHIPPET_IGGY_PASSWORD_FILE`. Local `docker run` instructions use an explicit
throwaway password.

---

## U-003: Wildcard binding requires `IGGY_NODE_ADVERTISED_ADDRESS`, or published ports are unreachable

- **Date:** 2026-11-13 (re-verified 2026-11-14)
- **Version:** server `0.10.0`, Docker Desktop 29.9.2
- **Status:** open (docs / Compose onboarding)

**Symptom.** When a listener binds `0.0.0.0`, the server requires
`IGGY_NODE_ADVERTISED_ADDRESS` (or an equivalent advertised address) to tell
clients where to reconnect. Without it, startup refuses the wildcard
configuration or falls back to a loopback default inside the container; in the
fallback case the published port is unreachable even though the first connection
succeeds. The logs warn for the WebSocket, QUIC, and HTTP listeners first, then
the TCP listener also ends up on loopback, which makes the actual failure look
unrelated to the missing variable.

**Minimal repro.** Run the container with a wildcard TCP bind and a published
port, but no advertised address:

```bash
docker run --rm -p 8090:8090 \
  --security-opt seccomp=unconfined \
  -e IGGY_ROOT_USERNAME=iggy \
  -e IGGY_ROOT_PASSWORD=whippet-local \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
  apache/iggy:0.10.0
```

Connect a client from the host. If the server started with a loopback advertised
address, the client is sent to loopback and the published host port is
unreachable. Setting `IGGY_NODE_ADVERTISED_ADDRESS=127.0.0.1:8090` (or `iggy`
inside a Compose network) fixes it.

**Impact.** Container onboarding fails in a way that looks like a networking or
firewall problem. The multi-transport warning sequence is confusing: users see
warnings for transports they are not using before the TCP symptom appears.

**Proposed action.** Document `IGGY_NODE_ADVERTISED_ADDRESS` next to the bind
address in the Docker/Compose docs; derive a sensible advertised address from
the bind when it is a wildcard, or fail fast with one clear message; align the
warning behavior across transports. The Compose/onboarding item should show the
variable, not only mention it in prose.

**Whippet workaround.** `deploy/compose.yaml` sets
`IGGY_NODE_ADVERTISED_ADDRESS: iggy` and the node dials `iggy:8090`. Local
`docker run` instructions set `127.0.0.1:8090`.

---

## U-004: Transient `namespace not routable` WARN during first consumer polls

- **Date:** 2026-11-14
- **Version:** server `0.10.0`, SDK `0.12.0`
- **Status:** open (docs / UX)

**Symptom.** After creating streams and topics on a fresh server, the server
logs repeated warnings while the first consumers poll:

```text
WARN partition_read: namespace not routable (not materialised yet or deleted) namespace_raw=...
```

The condition is transient: the namespace materialises and the polls succeed.
Whippet's retry loop handles it, but the log line reads like data loss or a
deleted namespace, not like a startup race.

**Minimal repro.** Start a fresh `apache/iggy:0.10.0`, create a topic with 16
partitions, and immediately start consumers polling those partitions. Watch the
server log during the first poll window.

**Impact.** A new user sees a warning that sounds catastrophic before the first
successful read. It generates support noise, encourages unnecessary restarts,
and makes a healthy startup look broken.

**Proposed action.** Document the materialization window in the server or SDK
docs; or have the server suppress/annotate the warning for a just-created
namespace (for example, log it at debug/info during a grace period and escalate
only if it persists). A regression test that creates a 16-partition topic and
polls immediately would keep the behavior honest.

**Whippet workaround.** The engine and MD consumers retry polls with a short
sleep and only log a warning after the retry fails. No data is lost; the
transient warning is upstream.

---

## U-005: HTTP JWT uses a random secret when none is configured

- **Date:** 2026-11-14
- **Version:** server `0.10.0`
- **Status:** open (docs / config)

**Symptom.** With HTTP enabled and no JWT configuration, the server logs:

```text
WARN JWT encoding and decoding secrets are not configured - generated a random secret... JWT tokens will be invalidated on server restart.
```

The server still starts and issues tokens, but the signing secret is ephemeral.
Every restart invalidates existing sessions.

**Minimal repro.** Start a 0.10 server with the HTTP listener enabled and no
`http.jwt` encoding/decoding secret. Observe the warning. Authenticate, restart
the server, and reuse the token; it is rejected.

**Impact.** This matters for the `obs` profile and for any HTTP-authenticated
dashboard, automation, or health probe. A random secret per restart is a
reasonable secure default for a throwaway local server, but it is a poor
operational default if the docs do not say how to make sessions stable.

**Proposed action.** Document the required `http.jwt` configuration for stable
sessions; add it to the Compose examples for any profile that uses HTTP auth;
make the warning state the consequence in one sentence ("tokens will not
survive a restart") and link the config key. Consider failing closed in a
production-oriented profile, or persisting the generated secret only when the
operator explicitly opts in.

**Whippet workaround.** Phase 1 uses TCP for the data path and does not depend
on HTTP JWT sessions. The `obs` profile must set a stable secret before it is
used for anything beyond a local demo.

---

## U-006: High-level producer cannot pass full `TopicCreateOptions`

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0`, server `0.10.0`
- **Status:** open (local issue pack 001; ready to file)

**Symptom.** `IggyProducerBuilder::create_topic_if_not_exists` exposes only
partitions, message expiry, max size, and (later) topic durability. The remaining
`TopicCreateOptions` fields are filled from `Default`, so create-only settings
such as segment size, consumer-offset durability, replication factor, flush
thresholds, and preallocation cannot be set through the recommended high-level
path. The consumer/stream builder is stricter still.

**Minimal repro.** On a fresh stream, call
`create_topic_if_not_exists(...)` with the high-level producer, then inspect the
created topic. The fields the builder did not expose are server defaults, not the
values the caller needed. Pre-creating the topic with the low-level
`create_topic` API and then building the producer works around it.

**Impact.** Whippet's topic layout is a product contract (segment size, offset
durability, retention, flush thresholds). The high-level path silently fixes
those knobs at defaults, so an application that follows the recommended API
cannot reproduce Whippet's layout without dropping to the low-level client. The
workaround is non-obvious and easy to miss in review.

**Proposed action.** Store a full `TopicCreateOptions` in the producer builder;
add an additive `topic_create_options(...)` (or equivalent) that enables
auto-creation with the full bundle; keep the legacy setters working with
documented last-call-wins precedence; pass the bundle through the
consumer/stream config as well. The local issue pack 001 has the detailed API
proposal and test plan; file it after re-checking the upstream tree.

**Whippet workaround.** `IggyJournal::ensure_topics` uses the low-level
`create_topic` with explicit `TopicCreateOptions`.

---

## U-007: `TopicCreateOptions` has no builder

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0`
- **Status:** folded into issue pack 001 (open)

**Symptom.** `TopicCreateOptions` is a large struct with many optional fields and
no builder. Construction relies on `..Default::default()`, which is terse but
easy to misread when durability, segment size, and flush thresholds are the
point of the call.

**Minimal repro.** Construct a non-default topic with segment size, consumer
offset durability, replication factor, and flush thresholds; compare the result
with the builder-based `DirectConfig`/`BackgroundConfig` APIs.

**Impact.** Error-prone and verbose. A wrong field is a silent behavioral change,
not a compile error, because most fields are optional.

**Proposed action.** Fold this into issue pack 001 rather than filing a separate
request. Add an opt-in `bon` builder (already a workspace dependency upstream)
after the full-options plumbing lands. Keep it additive.

**Whippet workaround.** A single private `topic_create_options` function in the
journal adapter constructs the bundle once, so the rest of the code does not
repeat the struct literal.

---

## U-008: SDK README's minimum-protocol statement is ambiguous against the pinned server

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0`, server `0.10.0`
- **Status:** open (docs question)

**Symptom.** The SDK `0.12.0` README says the minimum binary protocol is
`0.12.2` and that mixed versions are unsupported, while the pinned
`apache/iggy:0.10.0` server reports protocol `0.12.0` and the SDK connected and
completed authenticated operations successfully. The statement is ambiguous: it
is not clear whether it describes a future SDK build, a server release line, or
a hard rejection rule.

**Minimal repro.** Read the compatibility section of `core/sdk/README.md` in the
`0.12.0` SDK, then connect that SDK to `apache/iggy:0.10.0` over TCP and perform
an authenticated request. The connection succeeds despite the README's minimum
statement.

**Impact.** Users cannot tell which server/SDK pairs are supported. They may
upgrade a server unnecessarily, pin an older SDK, or assume a working pair is
unsupported. This is a docs ambiguity, not a demonstrated incompatibility; do
not file it as a bug that claims the pair is broken.

**Proposed action.** Publish a compatibility matrix covering server release,
binary protocol version, SDK release, and the supported mixed-version policy.
Clarify whether the minimum is a patch-level floor, a release-line floor, or a
runtime rejection. Link the matrix from the SDK README and the release notes.
Ask upstream to clarify before asserting incompatibility.

**Whippet workaround.** Pin both sides (`apache/iggy:0.10.0`, `iggy = "=0.12.0"`)
and record the exact pair in the README, manifest, and this log. Do not use
`latest` or `edge`.

---

## U-009: `/metrics` has no send/append latency histogram

- **Date:** 2026-11-14
- **Version:** server `0.10.0`
- **Status:** open (local issue pack 003; ready to file)

**Symptom.** `/metrics` exposes fetch-path histograms
(`readiness_push_to_fire_seconds`, `fetch_fire_to_reply_seconds`) but the send
path has no latency histogram. It has request counters and WAL pressure
counters, which are not a distribution. A p99 regression caused by consensus,
fsync, queueing, or a slow replica is only visible indirectly through client
RTT, which mixes in network and client scheduling.

**Minimal repro.** Start a 0.10 server, send a batch, scrape the authenticated
`/metrics` endpoint, and search for a send/append/commit latency histogram. The
fetch histograms are present; the send path has no equivalent.

**Impact.** Operators cannot chart producer acknowledgement latency from the
server. This is an observability asymmetry with a clear existing precedent on
the fetch path.

**Proposed action.** Add a histogram such as `send_submit_to_reply_seconds`
mirroring the fetch instrumentation; define whether error replies count; record
in the reply task, not the connection drain loop. The local issue pack 003 has
the dispatch timing seam and test plan.

**Whippet workaround.** The gateway records ingress latency around the Iggy
append and the dashboard charts it. That is a client-side view, not a server
metric.

---

## U-010: No runnable idempotent-producer restart/resend example

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0`, server `0.10.0`
- **Status:** open (local issue pack 004; ready to file)

**Symptom.** Idempotent producers are a headline feature, but the README has
only a minimal in-session `BatchProducer` snippet and prose about restart. There
is no runnable example of persisting producer id/epoch/sequence, resending the
last batch, handling an unknown outcome, or demonstrating the fencing errors.
The high-level producer's identity and sequences are private, so a process
restart with `.idempotent()` gets a new producer id.

**Minimal repro.** Search the Rust examples for an idempotent restart/resend
demo. Run the README snippet, then restart the process and try to continue the
same producer state. There is no state file, no resend, and no example of
`ProducerUnknown`, `ProducerSequenceOutOfOrder`, or `ProducerEpochFenced`.

**Impact.** Users cannot adopt the feature safely. The prose is easy to misread
as "restart-safe by default," which it is not. A wrong implementation can
duplicate or lose the business effect even when the producer path is idempotent.

**Proposed action.** Add a Rust example with a write-ahead state file, a
`--resume` path that resends the last batch with the old id/epoch/sequence, an
epoch bump, a consumer that asserts the count does not grow after a successful
resend, and a fencing demo. Label the state file illustrative, not production
guidance. The local issue pack 004 has the full design.

**Whippet workaround.** Whippet keeps its own durable `(symbol,
client_order_id)` dedup table in the engine. It does not rely on the Iggy
producer window for business deduplication.

---

## U-011: Cluster Compose defaults to 0.9.0, which rejects 0.10.0 binary clients

- **Date:** 2026-11-14
- **Version:** `docker-compose.cluster.yml` on `server-0.10.0`, client protocol
  `0.12.0`
- **Status:** open (local issue pack 002; ready to file)

**Symptom.** The three-node cluster Compose file pins
`apache/iggy:${IGGY_VERSION:-0.9.0}`. The 0.10.0 client speaks binary protocol
`0.12.0`; the 0.9.0 server speaks `0.11.0` and rejects authentication with an
incompatible-protocol error. The healthchecks use an unauthenticated `iggy ping`,
which never exercises the login gate, so the stack looks healthy until the first
authenticated command.

**Minimal repro.** Check out `server-0.10.0`, set the root password and cluster
shared secret, run `docker compose -f docker-compose.cluster.yml up -d`, wait for
the three healthchecks, then run an authenticated client command. The client is
rejected at login. `iggy ping` still succeeds.

**Impact.** A new-user trap in the cluster entrypoint: the default stack is
incompatible with the clients from the same release. It also wastes debugging
time because the healthcheck gives a false positive.

**Proposed action.** Default to `0.10.0`; add a header note that the image tag
must match the server release whose protocol the client uses; document that
0.10 has no in-place data migration and that switching versions requires fresh
volumes; add a CI guard that compares the default tag with the server version.
Do not use `latest`.

**Whippet workaround.** Whippet does not use the upstream cluster Compose file.
Its single-node demo pins `apache/iggy:0.10.0` explicitly.

---

## U-012: QUIC connection strings cannot set the TLS server name

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0` (upstream `master` `c78da20`)
- **Status:** coordinate with PR #4872 before filing (local issue pack 005)

**Symptom.** The QUIC client hardcodes `server_name: "localhost"` when it is
constructed from a connection string, and `QuicConnectionStringOptions` has no
field for the TLS server name. With certificate validation enabled, a
certificate issued for `broker.example.com` fails hostname verification. The
builder API can set the name, but the documented connection-string path cannot.

**Minimal repro.** Start a TLS-enabled Iggy server whose certificate SAN is not
`localhost`. Connect with a QUIC connection string that enables validation. The
connection fails hostname verification. Dropping to the builder and setting the
server name manually works.

**Impact.** Connection strings are the zero-config path, and they are unusable
for remote TLS with a real certificate. The tempting workaround is to disable
validation, which is unsafe. This is a TLS correctness and ergonomics bug, not a
demonstrated MITM vulnerability.

**Proposed action.** Add a server-name key (`tls_domain` for parity with TCP, or
`tls_server_name`) to the QUIC connection-string options, propagate it into
`QuicClientConfig`, and default to the connection-string hostname when it is a
DNS name. Add unit and integration tests with a non-localhost SAN. Coordinate
with PR #4872 first because it already touches the parser and TLS propagation;
do not file a competing change while it is in review.

**Whippet workaround.** Phase 1 uses TCP, not QUIC. The local issue pack 005 is
on hold until #4872 merges or the author agrees on the key name.

---

## U-013: App-side topic partition-count validation (workaround, not an upstream bug)

- **Date:** 2026-11-14
- **Version:** Whippet `0.1.0`, SDK `0.12.0`, server `0.10.0`
- **Status:** workaround (app-side; not filed upstream)

**Symptom.** Iggy does not expose an idempotent "ensure this topic exists with
exactly these options" operation. `ensure_topics` creates a topic if it is
missing and leaves an existing topic alone. If an existing topic was created
with a different partition count, the node would otherwise start, route to
partitions that do not exist, and fail later with a confusing poll error.

**Minimal repro.** Create a topic with 8 partitions, then start a Whippet node
configured for 16. Without the check, the node would start against a partition
count that does not match its routing table and fail later with a confusing poll
or routing error. With the validation, startup fails immediately with a message
that names the topic, the actual count, and the expected count.

**Impact.** This is a fail-fast improvement, not an upstream defect. The
underlying gap is the same one behind U-006/U-007: no idempotent ensure-topic
API that can assert the full option bundle. The app-side check only covers the
partition count; it does not verify segment size, durability, retention, or
flush thresholds.

**Proposed action.** No upstream issue. Revisit when Iggy exposes an idempotent
ensure-topic-with-options API; then delete the workaround. Until then, the check
in `crates/whippet-journal/src/lib.rs` is the honest behavior: fail at startup
rather than silently run against the wrong layout.

---

## U-014: Empty payloads are rejected (already tracked)

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0`
- **Status:** drop — tracked upstream as #4878

**Symptom.** `IggyMessage` requires a non-empty payload. A tombstone or a
control record needs a dummy byte.

**Minimal repro.** Send a zero-length payload. The SDK rejects it.

**Impact.** Tombstones and control records are awkward, but Whippet does not
need them in Phase 1.

**Proposed action.** Do not file a duplicate. #4878 is open, assigned, and has a
cross-SDK implementation checklist. Track it and remove any Whippet dummy-byte
workaround when it lands.

---

## U-015: Consumer-group offset semantics across restart (already documented)

- **Date:** 2026-11-14
- **Version:** SDK `0.12.0`
- **Status:** drop — documented and tested upstream

**Symptom.** A candidate gap was "no consumer-group offset restart example."
Investigation found the SDK README's "Positions and commits" section and the
integration test `consumer_offset_recovery.rs`, which covers replicated and
persisted crash recovery.

**Minimal repro.** Not applicable; the documentation and test exist. The real
constraint is that `OnRead` is at-most-once and offsets can skip after recovery,
which is a semantic warning, not a missing example.

**Impact.** No upstream issue. Whippet uses manual commits after processing and
its own dedup table, which is the conservative choice.

**Proposed action.** Do not file. Link the upstream docs from Whippet's own
operations notes when those notes are written.

---

## Open questions for upstream

1. What is the supported server/SDK compatibility matrix, and is the SDK
   README's minimum-protocol statement a runtime rejection or a docs floor?
   (U-008)
2. Can the QUIC connection-string parser and the #4872 work share one server-name
   key, or should U-012 be a follow-up? (U-012)
3. Is the `namespace not routable` warning expected to be user-visible during
   first-poll materialization, or should it be debug-level until a grace period
   expires? (U-004)
4. Should the server fail closed when HTTP JWT is enabled without a stable
   secret, or is the random-secret fallback the intended local default? (U-005)
5. Will an idempotent ensure-topic-with-options API land near the high-level
   producer work, or should U-006/U-007 be split into a server-side and an
   SDK-side change? (U-006, U-007, U-013)

---

## U-016: `message_bus.max_batch` caps batch ingress at 256 and the env override is undocumented

- **Date:** 2026-11-14
- **Version:** server `0.10.0`, SDK `0.12.0`
- **Status:** open — docs/validation

**Symptom.** The 0.10 default config reports `message_bus.max_batch: 256`. A
batch-ingress path wants 1,000 orders per Iggy append. Setting
`IGGY_MESSAGE_BUS_MAX_BATCH=1024` made the server refuse to load its config
(`Provided configuration value is invalid`) without naming the key, the allowed
range, or the accepted spelling.

**Minimal repro.** Start `apache/iggy:0.10.0` with
`IGGY_MESSAGE_BUS_MAX_BATCH=1024`; the server exits during config load. Remove
the variable and it starts with `max_batch: 256`.

**Impact.** Batch ingress is capped at 256 messages per append. That is still
enough to remove the one-request/one-append bottleneck, but a documented limit
and a supported override would let operators tune for throughput.

**Proposed action.** Document `message_bus.max_batch`, its valid range, and the
env spelling; or accept a larger value with a clear error when it is out of
range. Whippet caps the gateway batch at 256 in the meantime and does not split
silently.

**Workaround.** Gateway `POST /api/orders/batch` rejects more than 256 orders
with a clear error; the bench sweep uses 1/10/100/256.

---

## U-017: A broker restart under load leaves SDK mutation lanes blocked until the app resolves or abandons the operation

- **Date:** 2026-11-14
- **Version:** server `0.10.0`, SDK `0.12.0`
- **Status:** open — docs/example/ergonomics

**Symptom.** After recreating the Iggy container while the node was appending
and committing, consumer-offset commits failed forever with
`iggy error: Lane blocked by operation <op> with an unknown outcome`. Retrying
the same commit does not clear the lane. The SDK requires
`resolve_operation(op, deadline)` or `abandon_operation(op)` before the lane
can make progress. The error text names the operation but not the required
recovery call.

**Minimal repro.** Run a consumer with manual commits, restart the server under
load, and keep retrying `store_consumer_offset`. The lane stays blocked.

**Impact.** A single broker restart can stall a consumer indefinitely even
after the broker is healthy again. For a trading path this looks like "up but
not matching." An app that does not know the retained-operation API has no
obvious way out except restarting the client.

**Proposed action.** Document the recovery pattern next to the manual-commit
example, and consider a high-level `commit_with_recovery` helper or an error
message that says "call resolve_operation/abandon_operation". Whippet now
resolves the named operation with a short deadline, then abandons it if the
outcome cannot be proved, and retries the operation once.

**Workaround.** `whippet-journal` recovers the operation and retries once; see
`IggyJournal::recover_operation`.
