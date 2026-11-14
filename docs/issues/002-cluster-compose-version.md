# 002 — Cluster Docker Compose defaults to 0.9.0, which rejects 0.10.0 binary clients

**Status:** ready to file
**Type:** Bug report
**Area:** Docker / clustering
**Priority:** P2
**Effort:** XS–S (fix + verification)
**Duplicate check:** searched `docker-compose.cluster`, `IGGY_VERSION`, `cluster compose`, `0.9.0` in issues and PRs. No duplicate. #4499 added the file. #4609 is an open PR pinning the Helm chart to 0.10.0 — same class of drift, different file, cite as related evidence.

---

## GitHub issue body

### Bug description

The three-node cluster Compose file pins `apache/iggy:${IGGY_VERSION:-0.9.0}`. The latest server release is `server-0.10.0` (2026-11-07), and the `server-0.10.0` tag still ships the same 0.9.0 default. A 0.10.0 client speaks binary protocol 0.12.0; the 0.9.0 server speaks 0.11.0 and rejects authentication with `IncompatibleProtocol`. The cluster healthchecks pass because they use an unauthenticated `iggy ping`, which never exercises the login gate, so the mismatch only appears on the first authenticated client command.

This is a new-user trap in the repo's three-node Compose entrypoint and a release-artifact correctness bug.

### Affected area/component

- Clustering / replication
- Docker Compose deployment

### Deployment

Docker Compose (`docker-compose.cluster.yml`), three Iggy nodes.

### Versions

- Compose default: `apache/iggy:0.9.0`
- Current release: `server-0.10.0` (2026-11-07)
- 0.10.0 binary protocol: 0.12.0 (minimum accepted: 0.12.0)
- 0.9.0 binary protocol: 0.11.0

### Reproduction

1. Check out `server-0.10.0` (or current master).
2. Set the required root password and cluster shared secret.
3. Run `docker compose -f docker-compose.cluster.yml up -d`.
4. Wait for the three healthchecks to pass.
5. Run an authenticated client command, for example `iggy --tcp-server-address 127.0.0.1:8090 --username iggy --password "$IGGY_ROOT_PASSWORD" me`.
6. The client is rejected with an incompatible-protocol error. An unauthenticated `iggy ping` succeeds and does not show the problem.

### Expected behavior

The default Compose stack runs the server version that matches the checkout's clients, so a fresh `docker compose up` works without an `IGGY_VERSION` override.

### Proposed solution

- Change the default to `0.10.0`: `image: apache/iggy:${IGGY_VERSION:-0.10.0}`.
- Add a header note: the image tag must match the server release whose binary protocol the client uses; 0.9.0 cannot authenticate 0.10.x binary clients. Override `IGGY_VERSION` only with a matching client.
- Because 0.10.0 has no in-place data migration, document that switching versions requires fresh volumes (`docker compose ... down -v`); 0.10.0 refuses to start on an older data directory rather than migrating it.
- Optional CI guard: compare the default tag with `core/server/Cargo.toml` (or the latest release) so this does not drift again.
- Do not use `latest`; mutable tags make the demo and benchmarks non-reproducible.

### Additional context

- `docker-compose.cluster.yml:48` uses the 0.9.0 default.
- Unlike the single-node `docker-compose.yml`, which builds from the checkout, the cluster Compose pulls a fixed released image.
- The file was added in #4499 when 0.9.0 was current and was not bumped for 0.10.0.
- The Helm chart has the same drift: `helm/charts/iggy/Chart.yaml` pins `appVersion: "0.9.0-edge.6"`, and open PR #4609 fixes it.
- The healthcheck at `docker-compose.cluster.yml:118` is `iggy ping`, an unauthenticated PING; it passes regardless of the server version.
- Changing master fixes the next release, not the already-published `server-0.10.0` tag. A release note or a 0.10.1 re-cut may be needed if the tag itself should be corrected.

### Contribution

I can open the default change, the compatibility note, and a verification script. I will wait for assignment before the PR.

---

## Internal notes (do not paste)

### Release facts verified 2026-11-14

- `gh release list -R apache/iggy` shows `server-0.10.0` as Latest, published 2026-11-07.
- Docker Hub has `apache/iggy:0.10.0` (pushed 2026-11-07) and `0.9.0` (pushed 2026-09-18).
- `git show server-0.10.0:docker-compose.cluster.yml` still has `IGGY_VERSION:-0.9.0`.
- Local checkout server is 0.10.0; binary protocol is 0.12.0 with min 0.12.0 (patch zeroed by the compatibility check).
- `server-0.9.0` ships protocol 0.11.0; a 0.12.0 client is rejected at login, not at PING.
- `server-0.10.0:core/server/README.md` documents that there is no in-place upgrade or storage conversion; old data directories are refused.

### Test plan

1. `docker compose -f docker-compose.cluster.yml config` with the new default.
2. Start with the required secrets; confirm all three healthchecks.
3. Confirm the same `IGGY_CLUSTER_*`/`IGGY_SHARDING_*` env vars boot with the 0.10.0 image; do not assume a one-line change is sufficient.
4. Build the current CLI, run the authenticated command, then create a stream/topic and produce/consume.
5. Reproduce the old failure with `IGGY_VERSION=0.9.0` and confirm the login rejection.
6. Test the volume path: an existing 0.9.0 volume against a 0.10.0 server fails closed; document `down -v` as the reset.

### Risk

Changing the default makes existing 0.9.0 volumes fail closed against 0.10.0. Document the reset path. Users intentionally on 0.9.0 can still override `IGGY_VERSION`.
