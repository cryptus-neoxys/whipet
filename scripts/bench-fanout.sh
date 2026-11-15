#!/usr/bin/env bash
# 20 Hz WebSocket fanout sweep (1 / 10 / 100 subscribers, 30 s measured each).
#
#   scripts/bench-fanout.sh
#   WHIPPET_FANOUT_SUBSCRIBERS=1,10,100 WHIPPET_FANOUT_DURATION=30 \
#     scripts/bench-fanout.sh
#
# Starts the Compose stack (iggy + node) if the gateway is not already healthy,
# stops the simulator for the duration of the sweep, runs 1 / 10 / 100
# subscribers for 30 s each with label dev-loop-fanout, and writes fanout
# manifests to artifacts/bench. With WHIPPET_FANOUT_INGRESS_RATE=R the harness
# also posts R orders/s at POST /api/orders for the whole run and records the
# achieved rate, error count, and /metrics deltas in the manifest.
#
# Environment overrides:
#   WHIPPET_GATEWAY             gateway base URL (default http://127.0.0.1:8080)
#   WHIPPET_FANOUT_SUBSCRIBERS  comma-separated subscriber counts (default 1,10,100)
#   WHIPPET_FANOUT_DURATION     measured seconds per run (default 30)
#   WHIPPET_FANOUT_WARMUP       warmup seconds per run (default 5)
#   WHIPPET_FANOUT_TRADES       trade tape cap on /ws?trades=N (default 0 = full tape)
#   WHIPPET_FANOUT_SYMBOLS      optional comma-separated Subscribe filter (default all)
#   WHIPPET_FANOUT_INGRESS_RATE real ingress load in orders/s via POST /api/orders (default 0 = off)
#   WHIPPET_FANOUT_BASELINE_METRICS path to a no-subscriber baseline manifest for the comparison note (default empty)
#   WHIPPET_FANOUT_LABEL        manifest label (default dev-loop-fanout)
#   WHIPPET_FANOUT_OUTPUT_DIR   output directory (default ./artifacts/bench)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml")
GATEWAY="${WHIPPET_GATEWAY:-http://127.0.0.1:8080}"
SUBSCRIBERS="${WHIPPET_FANOUT_SUBSCRIBERS:-1,10,100}"
DURATION="${WHIPPET_FANOUT_DURATION:-30}"
WARMUP="${WHIPPET_FANOUT_WARMUP:-5}"
TRADES="${WHIPPET_FANOUT_TRADES:-0}"
SYMBOLS="${WHIPPET_FANOUT_SYMBOLS:-}"
INGRESS_RATE="${WHIPPET_FANOUT_INGRESS_RATE:-0}"
BASELINE_METRICS="${WHIPPET_FANOUT_BASELINE_METRICS:-}"
LABEL="${WHIPPET_FANOUT_LABEL:-dev-loop-fanout}"
OUTPUT_DIR="${WHIPPET_FANOUT_OUTPUT_DIR:-${ROOT}/artifacts/bench}"
RESTART_SIM=0

usage() {
    cat <<'EOF'
usage: scripts/bench-fanout.sh

Runs the 20 Hz WebSocket fanout harness for 1 / 10 / 100 subscribers (30 s
measured, 5 s warmup each) with label dev-loop-fanout, writing JSON manifests
to artifacts/bench.

Comparison mode: run once with WHIPPET_FANOUT_INGRESS_RATE=0 (quiet book) and
once with WHIPPET_FANOUT_INGRESS_RATE=10000, passing the first manifest with
WHIPPET_FANOUT_BASELINE_METRICS so the second manifest records whether
subscribers slowed the engine.

Environment: WHIPPET_GATEWAY, WHIPPET_FANOUT_SUBSCRIBERS,
WHIPPET_FANOUT_DURATION, WHIPPET_FANOUT_WARMUP, WHIPPET_FANOUT_TRADES,
WHIPPET_FANOUT_SYMBOLS, WHIPPET_FANOUT_INGRESS_RATE,
WHIPPET_FANOUT_BASELINE_METRICS, WHIPPET_FANOUT_LABEL, WHIPPET_FANOUT_OUTPUT_DIR.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

mkdir -p "${OUTPUT_DIR}"

GIT_SHA="$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git -C "${ROOT}" status --porcelain 2>/dev/null || true)" ]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi
IMAGE_DIGEST="$(docker image inspect whippet:0.1.0 --format '{{.Id}}' 2>/dev/null || echo unknown)"
export WHIPPET_GIT_SHA="${GIT_SHA}"
export WHIPPET_GIT_DIRTY="${GIT_DIRTY}"
export WHIPPET_IMAGE_DIGEST="${IMAGE_DIGEST}"

cleanup() {
    if [ "${RESTART_SIM}" -eq 1 ]; then
        "${COMPOSE[@]}" start sim >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if ! curl -fsS --max-time 2 "${GATEWAY}/api/health" >/dev/null 2>&1; then
    command -v docker >/dev/null 2>&1 || {
        echo "bench-fanout: gateway ${GATEWAY} is not reachable and docker is unavailable" >&2
        exit 1
    }
    "${ROOT}/scripts/gen-secrets.sh" >/dev/null
    echo "bench-fanout: starting iggy + node"
    "${COMPOSE[@]}" up -d --wait --wait-timeout 180 iggy node
    export WHIPPET_VIRTUALIZATION="${WHIPPET_VIRTUALIZATION:-docker-desktop}"
fi

if command -v docker >/dev/null 2>&1 \
    && "${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx sim; then
    echo "bench-fanout: stopping simulator for a quiet measurement"
    "${COMPOSE[@]}" stop sim >/dev/null
    RESTART_SIM=1
    export WHIPPET_VIRTUALIZATION="${WHIPPET_VIRTUALIZATION:-docker-desktop}"
fi

curl -fsS --max-time 5 "${GATEWAY}/api/health" >/dev/null || {
    echo "bench-fanout: node is not healthy on ${GATEWAY}" >&2
    exit 1
}

cargo build --release -p whippet-bench --example fanout

IFS=',' read -r -a subscriber_list <<<"${SUBSCRIBERS}"
for subs in "${subscriber_list[@]}"; do
    echo "bench-fanout: label=${LABEL} subscribers=${subs} duration=${DURATION}s warmup=${WARMUP}s trades=${TRADES} symbols=${SYMBOLS:-all} ingress_rate=${INGRESS_RATE}/s baseline=${BASELINE_METRICS:-none}"
    args=(
        --gateway "${GATEWAY}"
        --subscribers "${subs}"
        --duration "${DURATION}"
        --warmup "${WARMUP}"
        --trades "${TRADES}"
        --ingress-rate "${INGRESS_RATE}"
        --output-dir "${OUTPUT_DIR}"
        --label "${LABEL}"
    )
    if [ -n "${SYMBOLS}" ]; then
        args+=(--symbols "${SYMBOLS}")
    fi
    if [ -n "${BASELINE_METRICS}" ]; then
        args+=(--baseline-metrics "${BASELINE_METRICS}")
    fi
    cargo run --quiet --release -p whippet-bench --example fanout -- "${args[@]}"
done

echo "bench-fanout: manifests in ${OUTPUT_DIR}"
ls -1 "${OUTPUT_DIR}"/*.json 2>/dev/null | tail -n 10 || true
