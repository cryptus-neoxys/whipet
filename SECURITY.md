# Security policy

Whippet is a research and showcase project. **It is not production-ready.** The
Phase 1 release is a single-node demo with a deliberately limited security
posture. Please read the current posture before deploying or reporting.

## Supported versions

| Version | Supported | Notes |
|---|---|---|
| `0.1.x` | Yes | Phase 1 durable slice; security fixes land on `main` and the next `0.1.x` patch |
| `< 0.1.0` | No | Pre-release research snapshots; no support |

There is no LTS branch. A fix may require upgrading to the latest `0.1.x`
release. The Iggy server and SDK are pinned separately; see
[ADR 0004](docs/adr/0004-iggy-topics-partitions-durability.md).

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Use GitHub's private vulnerability reporting for this repository:

<https://github.com/cryptus-neoxys/whippet/security/advisories/new>

If private reporting is unavailable, open a minimal public issue asking for a
private channel. Do not include exploit details, credentials, customer data, or
a working proof of concept in that issue.

Please include:

- the Whippet version or commit (`git rev-parse HEAD`);
- the affected component (gateway, engine, journal adapter, dashboard,
  simulator, bench, deployment);
- the deployment shape (single node, local Docker, exposed port, etc.);
- a minimal reproduction and the observed impact;
- whether the issue also affects Apache Iggy upstream.

This is a volunteer project. Reports are handled on a best-effort basis; there
is no SLA and no bug bounty. We will aim to acknowledge a report, agree on a
disclosure timeline, and credit the reporter if they want credit.

### Upstream Iggy

Whippet is an independent project, not an Apache Software Foundation project.
If the issue is in Apache Iggy itself, report it through the Apache Iggy
security process rather than through Whippet. Do not send Iggy security details
to a public Whippet issue.

## Scope

In scope:

- `crates/whippet-core`, `whippet-engine`, `whippet-journal`,
  `whippet-marketdata`, `whippet-sim`, `whippet-bench`, and `whippet`;
- the HTTP/WebSocket gateway and dashboard in `apps/dashboard`;
- the snapshot format and the `verify` CLI;
- deployment files and scripts in this repository;
- dependency and supply-chain issues in the pinned workspace.

Out of scope:

- Apache Iggy server or SDK vulnerabilities (report upstream);
- Docker Desktop, the host OS, the kernel, or container escape;
- volumetric DDoS, physical access, or social engineering;
- third-party services and editors;
- missing features that are already documented as Phase 2 or later.

## Current demo security posture (Phase 1)

This is the honest state of `0.1.x`. It is suitable for localhost or a trusted
private network with synthetic data only.

| Area | Phase 1 state |
|---|---|
| Iggy identity | The node uses the Iggy bootstrap account from `WHIPPET_IGGY_USER` / `WHIPPET_IGGY_PASSWORD`. The code defaults to `iggy` / `iggy`. This is a demo account, not a secret. |
| Scoped identities | None. No PATs, no per-service users, no least-privilege grants. |
| Transport | Iggy TCP is plaintext. There is no TLS on TCP, HTTP, WebSocket, or QUIC. |
| At rest | No stream encryption. Iggy data and Whippet snapshot files are plaintext. |
| Binary TCP ingress | Same bearer token as HTTP when `WHIPPET_API_TOKEN` is set: the client must send a `Hello` frame with the token before `Submit`. No TLS in-process; terminate at a proxy for untrusted networks. Disable with `WHIPPET_TCP_ADDR=off`. |
| Dashboard and API | Unauthenticated by default. `WHIPPET_HTTP_ADDR` defaults to `0.0.0.0:8080`. Set `WHIPPET_API_TOKEN` to require a bearer token on order/cancel ingress; the dashboard itself remains unauthenticated. There is no CORS wildcard; the dashboard is same-origin and Vite development proxies `/api` and `/ws`. |
| Participant identity | With `WHIPPET_API_TOKEN` set, the gateway derives the participant from `WHIPPET_API_PARTICIPANT` and rejects a body participant that differs (403). Without a token, the body participant is accepted, so a caller can act as another participant, including triggering cancel-resting self-trade prevention. Do not expose anonymous ingress to untrusted clients. |
| Client mTLS | Not implemented and not claimed. Apache Iggy does not currently support client mTLS. |
| Audit | No hash chain, no signed checkpoints, no audit verifier. |
| Surveillance | Not implemented. |
| Rate limiting | Optional per-IP token bucket (`WHIPPET_RATE_LIMIT_PER_SEC`, default off), shared by HTTP and TCP and charged per order; plus global TCP (1,024) and WebSocket (2,048) connection caps. There is no per-participant quota. |
| Cluster | Single node. No RF3, no failover, no zero-loss-across-a-node-failure claim. |

### Known gaps

1. **Default-ish demo credentials.** The node defaults to `iggy` / `iggy` when
   the environment variables are unset. The server must be started with matching
   explicit credentials, but the defaults are still a copy-paste hazard.
2. **No TLS.** Credentials and order flow cross the network in plaintext.
3. **No PATs or scoped users.** One bootstrap account can read and write every
   Whippet stream and manage consumer groups.
4. **Unauthenticated dashboard and API.** Anyone who can reach port 8080 can
   read state and submit orders or cancels.
5. **No permissive CORS.** The dashboard is served same-origin and there is no
   wildcard CORS layer; a separate frontend needs its own proxy.
6. **No encryption at rest.** Iggy segments and snapshots are readable by anyone
   with filesystem or volume access.
7. **No client mTLS.** Do not describe the deployment as mutually authenticated.
8. **Rate limiting is optional and per-IP.** With `WHIPPET_RATE_LIMIT_PER_SEC`
   unset there is no limit; when set, a client is charged per order and the
   budget is shared across transports. The 2 MiB body cap returns 413. The
   gateway is still not a security boundary against a determined client.
9. **No audit integrity.** There is no hash chain or external checkpoint, so a
   log tamper is not detectable by Whippet itself.
10. **No HA.** A single node loss can lose data that was not durably written or
    replicated.

These gaps are tracked in [ADR 0008](docs/adr/0008-security-model-and-scoped-identities.md).
They are not permission to expose the demo.

## Threat model

### Assets

- order flow and execution reports;
- market data and the order book;
- snapshots and the state hash;
- Iggy credentials, PATs, and future TLS keys;
- benchmark artifacts and manifests;
- the integrity and replayability of the log.

### Trust boundaries

- **Browser -> gateway:** untrusted client, no authentication in Phase 1.
- **Gateway -> Iggy:** plaintext TCP, one bootstrap account in Phase 1.
- **Engine -> filesystem:** plaintext snapshots, local process trust.
- **Operator -> containers:** environment variables, volumes, and the Docker
  socket are highly privileged.
- **Dashboard -> API:** same origin today, permissive CORS, no auth.

### In scope for reports

- authentication or authorization bypass (when auth exists);
- order injection, cancellation, or replay that causes an unintended effect;
- malformed input that panics, over-reads, or corrupts engine state;
- path traversal or arbitrary file write/read through the gateway or CLI;
- secret leakage in logs, snapshots, artifacts, or the repository;
- dashboard XSS or unsafe rendering of exchange-controlled strings;
- supply-chain compromise in a pinned dependency.

### Out of scope

- host/root compromise, kernel exploits, Docker escape;
- volumetric DDoS and network flooding;
- HSM/KMS, key escrow, and formal key management;
- regulatory compliance (MiFID II, SOC 2, PCI, etc.);
- exactly-once delivery or formal verification;
- client mTLS (not supported);
- multi-tenant SaaS administration.

## What not to expose publicly

Do not publish or port-forward any of the following on an untrusted network:

- Iggy TCP `8090` (or any Iggy transport);
- the Whippet gateway and dashboard on `8080`;
- the snapshot directory (`data/snapshots` by default);
- `artifacts/`, `bench-results/`, or raw `.hdr` files;
- `.env`, `secrets/`, TLS keys, PATs, or bootstrap credentials;
- the Docker socket or a container shell.

If you need a remote demo, put it behind a reverse proxy that terminates TLS and
requires authentication, bind Whippet to loopback, restrict CORS, change every
credential, use ephemeral synthetic data, and accept that it is still a demo.
None of that makes it production-ready.

## Safe demo checklist

- [ ] Run on localhost or a trusted private network.
- [ ] Change `WHIPPET_IGGY_USER` / `WHIPPET_IGGY_PASSWORD` from the defaults.
- [ ] Set `WHIPPET_API_TOKEN` and `WHIPPET_RATE_LIMIT_PER_SEC` before exposing the API beyond localhost.
- [ ] Do not use `--with-default-root-credentials` or `iggy/iggy` for a shared
      environment.
- [ ] Keep Iggy and the gateway off the public internet.
- [ ] Use synthetic data only; no customer, personal, or regulated data.
- [ ] Do not commit `.env`, `secrets/`, `data/`, `artifacts/`, or benchmark raw
      files.
- [ ] Delete volumes and snapshots when changing server versions; Iggy 0.10
      does not migrate an older data directory in place.
- [ ] Treat every benchmark number from Docker Desktop or an M4 laptop as a
      `dev-loop` result, not a headline.

## Disclosure and credit

We prefer coordinated disclosure. We will work with the reporter on a timeline
that fits the severity and the available maintainer time. If a report is
accepted, we will credit the reporter in the advisory unless they ask us not to.

Whippet is an independent Apache-2.0 project. It is not affiliated with,
sponsored by, or endorsed by the Apache Software Foundation. Apache Iggy is a
project of the Apache Software Foundation.
