SHELL := /bin/bash
COMPOSE := docker compose -f deploy/compose.yaml

.PHONY: help secrets up down demo bench bench-smoke bench-sweep bench-tcp verify replay doctor reset logs build dashboard ci obs chaos ha ps

help:
	@printf '%s\n' \
		'make up          generate secrets if needed, build, start iggy + node + sim' \
		'make down        stop the default stack' \
		'make demo        submit a crossing sell/buy pair and print the trade' \
		'make bench       full L1 benchmark (profile bench)' \
		'make bench-smoke short benchmark sanity run' \
		'make bench-sweep batch-size sweep (1/10/100/256)' \
		'make bench-tcp   binary TCP sweep (1k/5k/10k)' \
		'make verify      healthcheck, crossing pair, snapshot hash check' \
		'make replay      full vs snapshot-seeded replay; hashes must match' \
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

verify:
	@$(COMPOSE) exec -T node /usr/local/bin/whippet healthcheck --gateway http://127.0.0.1:8080
	@scripts/verify-demo.sh
	@$(COMPOSE) exec -T node /usr/local/bin/whippet verify --snapshot-dir /data/snapshots --partitions 16

replay:
	@scripts/replay.sh

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
