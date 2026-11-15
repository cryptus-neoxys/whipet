SHELL := /bin/bash
COMPOSE := docker compose -f deploy/compose.yaml

.PHONY: help secrets up down demo bench bench-smoke bench-sweep bench-tcp bench-fanout bench-matcher soak crash-test crash-matrix retention-plan audit order-trace surveillance verify verify-strict verify-dev replay replay-strict replay-dev doctor reset logs build dashboard ci obs chaos ha ps

help:
	@printf '%s\n' \
		'make up          generate secrets if needed, build, start iggy + node + sim' \
		'make down        stop the default stack' \
		'make demo        submit a crossing sell/buy pair and print the trade' \
		'make bench       full L1 benchmark (profile bench)' \
		'make bench-smoke short benchmark sanity run' \
		'make bench-sweep batch-size sweep (1/10/100/256)' \
		'make bench-tcp   binary TCP sweep (1k/5k/10k)' \
		'make bench-fanout WebSocket fanout (1/10/100 subscribers)' \
		'make bench-matcher criterion benches for the matcher, journal, and MD hub' \
		'make soak         10-minute steady-state soak' \
		'make crash-test   kill/restart/replay proof (Docker stack)' \
		'make crash-matrix Rust failure-injection tests (append-before-commit, writer failure)' \
		'make retention-plan read-only safe-deletion plan (never deletes)' \
		'make audit       list operator halt/resume audit records (read-only)' \
		'make order-trace ORDER=<id> reconstruct one order across the logs (read-only)' \
		'make surveillance  educational cancel/STP/duplicate indicators (read-only)' \
		'make verify      healthcheck, crossing pair, snapshot hash check (lenient dev path; WHIPPET_PARTITIONS overrides 16)' \
		'make verify-strict fail-closed verify: exit non-zero when no snapshot exists (WHIPPET_PARTITIONS)' \
		'make verify-dev  alias for the lenient verify path' \
		'make replay      full vs snapshot-seeded replay; seeded run must be seeded' \
		'make replay-strict alias for replay (fail-closed proof)' \
		'make replay-dev  lenient replay: allows a full replay as the seeded run' \
		'make doctor      read-only preflight: Iggy topics, snapshots, health' \
		'make reset       down -v (keeps local files and secrets)' \
		'make logs        follow default stack logs' \
		'make build       build the whippet image' \
		'make dashboard   build the dashboard locally' \
		'make ci          fmt, clippy, tests, dashboard build, compose config' \
		'make obs         Prometheus + Grafana (profile obs)' \
		'make chaos       experimental chaos helper' \
		'make ha          experimental 3-node Iggy skeleton'

secrets:
	@scripts/gen-secrets.sh

up: secrets
	@$(COMPOSE) up --build -d --wait --wait-timeout 240 || { \
		echo "make up failed. If the node log says it could not restore a snapshot after a binary change, run 'make reset'." >&2; \
		exit 1; \
	}
	@printf '\nWhippet: http://localhost:8080\n'

down:
	@docker compose -f deploy/compose.yaml -f deploy/compose.obs.yaml --profile "*" down --remove-orphans >/dev/null 2>&1 || true
	@docker compose -f deploy/compose.yaml -f deploy/compose.chaos.yaml --profile "*" down --remove-orphans >/dev/null 2>&1 || true
	$(COMPOSE) --profile "*" down --remove-orphans

demo:
	@scripts/demo.sh

bench:
	@scripts/bench.sh full

bench-smoke:
	@scripts/bench.sh smoke

bench-sweep:
	@scripts/bench-sweep.sh

bench-tcp:
	@scripts/bench-tcp.sh

bench-fanout:
	@scripts/bench-fanout.sh

soak:
	@scripts/soak.sh

crash-test:
	@scripts/crash-test.sh

# Deterministic failure-injection tests: no Docker required.
crash-matrix:
	cargo test -p whippet --test crash_idempotency --test shard_failures

# Read-only retention plan: consumers + verified snapshot -> safe offset.
retention-plan:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet retention-plan --snapshot-dir /data/snapshots --partitions $${WHIPPET_PARTITIONS:-16}

# Read-only operator views over the logs.
audit:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet audit --partitions 4

order-trace:
	@test -n "$(ORDER)" || { echo "order-trace: set ORDER=<decimal|0xhex order id>"; exit 2; }
	@$(COMPOSE) exec -T node /usr/local/bin/whippet order-trace --partition $${WHIPPET_TRACE_PARTITION:-1} --order-id $(ORDER)

surveillance:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet surveillance --partition $${WHIPPET_TRACE_PARTITION:-1} --window-secs $${WHIPPET_SURVEILLANCE_WINDOW_SECS:-3600}

# Criterion microbenchmarks. Developer tools, not CI gates.
bench-matcher:
	cargo bench -p whippet-engine --bench engine
	cargo bench -p whippet-journal --bench append
	cargo bench -p whippet-marketdata --bench md

# `verify` stays lenient so a fresh stack can be inspected before the first
# snapshot is written; it prints "no snapshots; nothing verified". Use
# `verify-strict` (or `make verify-strict`) for the fail-closed proof.
verify:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet healthcheck --gateway http://127.0.0.1:8080
	@scripts/verify-demo.sh
	@$(COMPOSE) exec -T node /usr/local/bin/whippet verify --snapshot-dir /data/snapshots --partitions $${WHIPPET_PARTITIONS:-16}

verify-dev: verify

verify-strict:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet healthcheck --gateway http://127.0.0.1:8080
	@scripts/verify-demo.sh
	@$(COMPOSE) exec -T node /usr/local/bin/whippet verify --snapshot-dir /data/snapshots --partitions $${WHIPPET_PARTITIONS:-16} --require-snapshot

# scripts/replay.sh is fail-closed by default: the seeded replay passes
# --require-snapshot and the comparison rejects an unseeded run.
replay:
	@scripts/replay.sh

replay-strict: replay

replay-dev:
	@WHIPPET_REPLAY_REQUIRE_SNAPSHOT=0 scripts/replay.sh

doctor:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet doctor

reset:
	@scripts/reset.sh

logs:
	$(COMPOSE) logs -f --tail=100

build:
	docker build -f docker/Dockerfile -t whippet:0.1.0 .

dashboard:
	cd apps/dashboard && pnpm install --frozen-lockfile && pnpm build

ci:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace --all-targets
	cd apps/dashboard && pnpm install --frozen-lockfile && pnpm build
	@scripts/gen-secrets.sh >/dev/null
	docker compose -f deploy/compose.yaml config -q
	docker compose -f deploy/compose.yaml -f deploy/compose.obs.yaml --profile obs config -q
	docker compose -f deploy/compose.yaml -f deploy/compose.chaos.yaml --profile chaos config -q
	docker compose -f deploy/compose.ha.yaml config -q

obs:
	$(COMPOSE) -f deploy/compose.obs.yaml --profile obs up -d
	@printf 'Prometheus: http://localhost:9091\nGrafana:    http://localhost:3001\n'

chaos:
	@scripts/chaos.sh

ha:
	@scripts/gen-secrets.sh
	docker compose -f deploy/compose.ha.yaml up --build -d

ps:
	$(COMPOSE) ps
