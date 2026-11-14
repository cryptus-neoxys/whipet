# ADR 0008: Security model and scoped identities

- **Status:** Phase 1 demo posture accepted; Phase 2 target proposed
- **Date:** 2026-11-14
- **Deciders:** Whippet maintainers
- **Related:** [ADR 0004](0004-iggy-topics-partitions-durability.md), [ADR 0007](0007-single-node-v1-cluster-phase-3.md), [SECURITY.md](../../SECURITY.md)

## Context

Whippet is a local showcase, not a production exchange. Phase 1 needs to be
honest about that. The current code has a working durable slice, but it also has
a demo security posture:

- The node connects to Iggy with a username and password from the environment:
  `WHIPPET_IGGY_USER` / `WHIPPET_IGGY_PASSWORD`. The defaults in
  `NodeArgs` are `iggy` / `iggy`.
- The local demo uses the Iggy **bootstrap account**. There are no scoped
  users, no PATs, and no per-service identity.
- Iggy TCP is plaintext. The client is built with `with_tcp()` and
  `with_no_delay()`; there is no TLS configuration.
- The gateway and dashboard are unauthenticated. `WHIPPET_HTTP_ADDR` defaults
  to `0.0.0.0:8080`, and the router uses `CorsLayer::permissive()`.
- There is no stream encryption. Orders, executions, market data, alerts, and
  audit payloads are plaintext fixed-layout bytes in Iggy.
- Snapshots are plaintext bincode files on disk.
- There is no client mTLS. Apache Iggy does not currently support client mTLS,
  and Whippet does not claim it.
- There is no audit hash chain, surveillance service, rate limiter, or
  authenticated `/metrics` proxy in Phase 1.

These are acceptable only because the intended deployment is a laptop on a
trusted network with synthetic data. They are not acceptable for a public
endpoint.

## Decision

### Phase 1: explicit demo posture

Phase 1 is **demo-only**. The supported deployment is:

- localhost or a trusted private network;
- synthetic orders and market data, no real customer or personal data;
- the Iggy bootstrap account from environment variables;
- no TLS, no PATs, no at-rest encryption, no dashboard auth;
- no public exposure of Iggy, the gateway, the dashboard, snapshots, or
  benchmark artifacts.

The repository must say this in `README.md`, `SECURITY.md`, and the dashboard's
About page. The absence of a security feature is not a TODO that can be ignored
at deployment time; it is a boundary on what the demo may be used for.

### Phase 2: scoped identities and transport security

Phase 2 replaces the demo posture with:

1. **Scoped PATs, one per service.** `svc-gateway`, `svc-matching`, `svc-md`,
   `svc-surveillance`, `svc-audit`, `svc-sim`, `svc-dashboard`, and `svc-bench`
   each get a personal access token with the minimum stream/topic grants. No
   shared PAT, and no root PAT in an application container.
2. **One stream per trust domain.** The current three-stream split
   (`whippet-trading`, `whippet-md`, `whippet-ops`) is the coarse Phase 1
   boundary. Phase 2 refines it so a compromised service cannot read or write
   another domain's topics. `read_topics` also grants consumer-group admin in
   Iggy, so blast-radius separation is a security requirement, not just
   tidiness.
3. **TLS on every transport.** TCP, HTTP, WebSocket, and QUIC use a local CA or
   a properly configured certificate. Certificate validation stays on. The QUIC
   server-name gap (see `docs/UPSTREAM.md`, item U-012) must be fixed or worked
   around before QUIC is used with a non-localhost certificate.
4. **AES-256-GCM-SIV at rest.** Orders, executions, alerts, and audit are
   encrypted at the stream level with a KEK from a secret and per-stream data
   keys. Market data may remain clear because it is public. Encryption is
   decided at stream creation; it is not a per-message toggle.
5. **One-shot root bootstrap.** The root account is used once to create users,
   PATs, streams, and grants. It is then removed from the runtime environment.
   The demo must not use `--with-default-root-credentials` or `iggy/iggy`.
6. **Secrets as files, not environment.** Compose file secrets or a mounted
   secret directory, mode 0600, excluded from `.gitignore` and `.dockerignore`.
   Environment variables are visible in `docker inspect` and crash dumps.
7. **Dashboard authentication and read-only scope.** The dashboard gets an
   authenticated read path and does not receive order-entry or admin grants.
   CORS is restricted to the dashboard origin, not `permissive()`.
8. **Audit hash chain.** A hash chain over `(stream, partition, offset,
   envelope hash)` with signed checkpoints outside Iggy. This is Phase 2/3
   work; Phase 1 has no audit claim.

### No client mTLS claim

Whippet does **not** implement client mTLS and must not claim it. If a
deployment needs mutual authentication, terminate it at a reverse proxy or
service mesh in front of the gateway and use a scoped PAT between the gateway
and Iggy. Do not describe Iggy's TLS support as client certificate
authentication.

## Threat model

**Assets:** order flow, execution reports, market data, snapshots, PATs and
bootstrap credentials, benchmark artifacts, and the integrity of the log.

**Trust boundaries:**

- browser -> gateway (untrusted client, no auth in Phase 1);
- gateway -> Iggy (plaintext TCP in Phase 1, one bootstrap account);
- engine -> filesystem (plaintext snapshots);
- operator -> containers (environment variables, volumes, Docker socket);
- node -> dashboard static assets (same origin, permissive CORS in Phase 1).

**In scope for Phase 1 hardening and security reports:**

- unauthorized order injection or cancellation through the gateway;
- malformed input that panics, over-reads, or corrupts the engine;
- replay or tampering with order/execution/market-data messages;
- leakage of market data or snapshots to an unauthorized reader;
- consumer impersonation or offset tampering;
- secret leakage from the repository, logs, or container configuration;
- dashboard XSS or unsafe rendering of exchange data;
- dependency and supply-chain issues in the pinned crates.

**Out of scope in Phase 1:**

- host or root compromise, kernel exploits, Docker escape;
- volumetric DDoS and network-level attacks;
- HSM/KMS integration, key escrow, or formal key management;
- regulatory compliance (MiFID II, SOC 2, etc.);
- exactly-once delivery or formal verification;
- client mTLS (not supported);
- multi-tenant SaaS administration.

## Consequences

Positive:

- The project cannot accidentally present a demo as production-ready; the
  boundary is written down in three places.
- Phase 2 has a concrete target: scoped PATs, TLS, AES-256-GCM-SIV, one-shot
  root bootstrap, authenticated dashboard.
- The threat model names what is actually being protected, so security reports
  can be triaged instead of argued about.

Negative:

- The Phase 1 demo is not safe to expose. That limits its usefulness as a
  public hosted demo until Phase 2 lands.
- Scoped PATs and per-stream grants add operational work: rotation, bootstrap,
  and permission tests.
- TLS and at-rest encryption add configuration and a performance cost that must
  be measured, not assumed.
- The security work competes with turbo, surveillance, and HA for Phase 2 time.

## Alternatives considered

- **Ship the demo with default credentials and document "change me."**
  Rejected: users copy the path of least resistance, and a public demo would be
  compromised immediately.
- **Use one root PAT for every service.** Rejected: a single leaked token gives
  full stream and consumer-group admin.
- **Add TLS but keep plaintext at rest.** Better than nothing, but it leaves
  orders and executions readable on disk and in backups. Rejected for the Phase
  2 target.
- **Use client mTLS for service identity.** Not supported by Iggy today.
  Rejected; use scoped PATs and, if needed, a proxy/mesh.
- **Rely on network isolation alone.** Necessary but not sufficient. A
  compromised service inside the network still needs least privilege.
- **Encrypt market data too.** It is public by design; encrypting it adds cost
  without a clear threat benefit. Rejected for `md`; business topics are
  encrypted.

## Status

Phase 1 demo posture is **accepted only for local, synthetic, trusted-network
use**. The Phase 2 target is **proposed**, not implemented. Until it lands, the
project must not claim production readiness, TLS, PATs, at-rest encryption,
dashboard authentication, audit integrity, or client mTLS.
