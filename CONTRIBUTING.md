# Contributing to Whippet

Whippet is an independent Apache-2.0 showcase for Apache Iggy. Contributions are
welcome, but the project is deliberately small and opinionated: a deterministic
matching core, a durable Iggy path, and an honest benchmark. Please read this
before opening a pull request.

By contributing, you agree that your contribution is licensed under the
Apache License 2.0. **No CLA is required.** A DCO sign-off (`git commit -s`) is
welcome but not enforced.

## What the project is (and is not)

Phase 1 is a single-node durable slice:

```text
gateway HTTP/WS -> Iggy orders -> deterministic matcher
                -> Iggy executions/md -> MD hub -> TanStack dashboard
```

Implemented: gateway, engine, journal adapter, market-data hub, simulator,
benchmark harness, CLI, dashboard.

Not implemented: turbo mode, HA/3-node cluster, surveillance, audit/hash chain,
PATs/scoped identities, TLS, at-rest encryption, authenticated dashboard,
replay UI, failover tests. Do not describe these as available.

If you are unsure whether a change fits the current phase, open an issue first.

## Prerequisites

- **Rust 1.95.** The repository pins `1.95.0` in `rust-toolchain.toml`; rustup
  will select it automatically.
- **Node 25 and pnpm** for the dashboard.
- **Docker** for a local Apache Iggy server. The pinned demo server is
  `apache/iggy:0.10.0`; the Rust SDK is `iggy = "=0.12.0"`.
- Linux or macOS. Windows is not tested in Phase 1.

The default demo stack is in `deploy/compose.yaml`. It runs Iggy, the node, and
the simulator, and publishes only the dashboard on port 8080. Generate the demo
secrets once, then bring it up:

```bash
./scripts/gen-secrets.sh
docker compose -f deploy/compose.yaml up --build -d
```

The generated `.env` and secret files are mode 0600 (the `secrets/` directory
is 0700), and both are gitignored. If you prefer to run Iggy on its own while
developing the Rust side, use the `docker run` command below.

## Local setup

```bash
git clone https://github.com/cryptus-neoxys/whippet.git
cd whippet

# Rust workspace
cargo build

# Dashboard
cd apps/dashboard
pnpm install --frozen-lockfile
pnpm build
cd ../..
```

### Start Iggy locally

The 0.10 server needs explicit root credentials and, under the default Docker
seccomp profile, `io_uring` access. This is a known onboarding gap; see
[`docs/UPSTREAM.md`](docs/UPSTREAM.md), items U-001 and U-002.

```bash
docker run --rm --name whippet-iggy \
  -p 8090:8090 \
  --security-opt seccomp=unconfined \
  -e IGGY_ROOT_USERNAME=iggy \
  -e IGGY_ROOT_PASSWORD=whippet-local \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
  -e IGGY_NODE_ADVERTISED_ADDRESS=127.0.0.1:8090 \
  apache/iggy:0.10.0
```

If your image uses different variable names for the root bootstrap, use the
names documented by that release. Do not use `--with-default-root-credentials`
for anything shared, and do not use `iggy/iggy` outside a throwaway local
container.

### Run the node

```bash
export WHIPPET_IGGY_PASSWORD=whippet-local
cargo run -p whippet -- node
```

The node connects to `127.0.0.1:8090`, creates the streams and topics, and
serves the API and dashboard at <http://localhost:8080>. If the dashboard assets
are missing, the node serves a page telling you to build them.

Useful environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `WHIPPET_HTTP_ADDR` | `0.0.0.0:8080` | Gateway and dashboard bind address |
| `WHIPPET_IGGY_ADDR` | `127.0.0.1:8090` | Iggy TCP address |
| `WHIPPET_IGGY_USER` | `iggy` | Demo bootstrap user |
| `WHIPPET_IGGY_PASSWORD` | `iggy` | Demo bootstrap password |
| `WHIPPET_JOURNAL` | `iggy` | `iggy` or `memory` (tests/dev only) |
| `WHIPPET_MODE` | `durable` | `turbo` is rejected until Phase 2 |
| `WHIPPET_PARTITIONS` | `16` | 1–256 |
| `WHIPPET_SNAPSHOT_DIR` | `data/snapshots` | Snapshot directory |

The container entrypoint also accepts `WHIPPET_IGGY_PASSWORD_FILE`; Compose uses
it to read the root password from a mounted secret instead of an environment
variable.

### Drive it

```bash
# Deterministic simulator
cargo run -p whippet -- sim --rate 400 --scenario calm_open

# Health and snapshot verification
cargo run -p whippet -- healthcheck
cargo run -p whippet -- verify

# Replay one partition from the snapshot tail (or pass --from 0 for a full replay)
cargo run -p whippet -- replay --partition 1

# Open-loop benchmark (dev-loop on a laptop)
cargo run -p whippet --release -- bench \
  --rate 1000 --duration 30 --warmup 5 --repetitions 5
```

Benchmark results are written to `artifacts/bench/`. They are local artifacts;
do not commit them. A Docker Desktop or M4 result is a `dev-loop` measurement,
not a headline claim. See [ADR 0006](docs/adr/0006-benchmark-protocol.md).

## Checks before you push

Run these from the repository root:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Engine property tests are part of the workspace test run; you can run them
directly:

```bash
cargo test -p whippet-engine --test properties
```

Dashboard:

```bash
cd apps/dashboard
pnpm typecheck
pnpm build
```

CI runs the same Rust and dashboard checks in `.github/workflows/ci.yml`,
plus a Compose config validation. Run them locally before pushing; a pull
request that has not been built and tested locally is not ready for review.

Do not run `cargo fmt` or `clippy --fix` across the whole workspace as part of an
unrelated change. Keep formatting and refactoring separate from behavior
changes.

## Commit style

Use a short conventional prefix and an imperative subject:

```text
feat: add post-only rejection reason
fix: commit the MD offset after the batch is applied
docs: explain the durable ack contract
refactor: move snapshot codec into whippet-core
test: cover FOK rejection on a crossed book
chore: pin the Iggy SDK to 0.12.0
```

Rules:

- **Small and green.** Each commit should build and pass the checks above. Do
  not mix a refactor, a behavior change, and a docs rewrite in one commit.
- **One purpose per pull request.** Link the issue or ADR that explains why.
- **No generated or runtime data.** Do not commit `target/`, `data/`,
  `artifacts/`, `bench-results/`, `*.hdr`, dashboard `dist/`, `node_modules/`,
  `.env`, or `secrets/`.
- **No local issue drafts.** `docs/issues/` is intentionally gitignored. Do not
  add it to git; the upstream issues are filed separately.
- **No secrets.** Never commit credentials, PATs, TLS keys, tokens, or a real
  `.env`.

## When an ADR is required

Write or update an ADR in `docs/adr/` for a change that affects:

- a new workspace crate or a change to the crate dependency direction;
- the topic layout, partition count, durability, retention, or commit policy;
- the durable/turbo acknowledgement contract or matching semantics;
- the security posture, identity model, or transport/at-rest encryption;
- the benchmark measurement origin, repetition policy, or headline statistic;
- the dashboard's conflation model or public API contract.

Use the existing ADRs as the format: context, decision, consequences,
alternatives, status. A large change without an ADR will be asked to add one
before review.

## Upstream Apache Iggy contributions

Whippet is also a source of upstream Iggy fixes. The rule is **issue first**:

1. Search open and closed issues and pull requests.
2. Reproduce on the pinned release (`apache/iggy:0.10.0`, SDK `0.12.0`), not on a
   random local build.
3. File one issue with version, platform, deployment, logs, expected/actual,
   minimal reproduction, and a proposed approach.
4. Wait for maintainer agreement and assignment before coding. New
   functionality without an approved issue can be closed.
5. One small, single-purpose pull request. Link the issue. Do not bundle
   unrelated cleanups.
6. Run the upstream checks locally: `fmt`, `clippy`, tests, `machete`, `sort`,
   `typos`.
7. Respond to every review comment. Do not rebase for freshness.

Record the friction and its status in [`docs/UPSTREAM.md`](docs/UPSTREAM.md).
Do not paste private notes or unverified claims into an upstream issue.

## Pull requests

Use the pull request template. It asks for the issue, rationale, what changed,
local execution, AI usage, and a short checklist. Keep the description factual:
what works, what does not, and what you measured. Do not claim benchmark numbers
without a manifest, and do not claim cluster results from a single node.

## AI-assisted contributions

AI assistance is allowed. Disclose it in the **AI usage** section of the pull
request: which tools you used, what they generated, how you verified it, and
whether you can explain every line. You are responsible for the change,
including license and correctness. Do not submit code you cannot defend in
review.

## Code of conduct

Participation is covered by [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Be
specific, be kind, and assume good faith.

## License and attribution

Whippet is licensed under Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
Apache Iggy is a project of the Apache Software Foundation. Whippet is an
independent showcase and is not affiliated with, sponsored by, or endorsed by
the ASF.
