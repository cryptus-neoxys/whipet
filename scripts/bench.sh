#!/usr/bin/env bash
# Run the open-loop benchmark inside the demo image.
#
#   scripts/bench.sh smoke   # short sanity run
#   scripts/bench.sh full    # L1: 1k orders/s, 30 s, 5 repetitions
#   scripts/bench.sh all     # smoke then full (default)
#
# Manifests are written to ./artifacts/bench on the host. The simulator is
# stopped for the duration of the run and restarted afterwards if it was
# running, so background demo traffic does not pollute the measurement.
#
# Docker Desktop is a demo environment, not a benchmark host. Treat these
# numbers as smoke-test output; the manifest records the error count and the
# run scope so a headline claim cannot be made from it by accident.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-all}"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml")
RESTART_SIM=0

case "${MODE}" in
    smoke|full|all) ;;
    -h|--help)
        sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
    *)
        echo "usage: $0 [smoke|full|all]" >&2
        exit 2
        ;;
esac

command -v docker >/dev/null 2>&1 || {
    echo "bench: docker is required" >&2
    exit 1
}

mkdir -p "${ROOT}/artifacts/bench"

GIT_SHA="$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git -C "${ROOT}" status --porcelain 2>/dev/null || true)" ]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi
IMAGE_DIGEST="$(docker image inspect whippet:0.1.0 --format '{{.Id}}' 2>/dev/null || echo unknown)"
export GIT_SHA GIT_DIRTY IMAGE_DIGEST

cleanup() {
    if [ "${RESTART_SIM}" -eq 1 ]; then
        "${COMPOSE[@]}" start sim >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

"${ROOT}/scripts/gen-secrets.sh" >/dev/null

echo "bench: starting iggy + node"
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 iggy node

if "${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx sim; then
    echo "bench: stopping simulator for a quiet measurement"
    "${COMPOSE[@]}" stop sim >/dev/null
    RESTART_SIM=1
fi

curl -fsS --max-time 5 http://localhost:8080/api/health >/dev/null \
    || {
        echo "bench: node is not healthy on http://localhost:8080" >&2
        exit 1
    }

run_bench() {
    local label="$1" rate="$2" duration="$3" warmup="$4" repetitions="$5"
    # A fresh seed keeps order ids unique across repeated runs against a node
    # that restored its engine state from a snapshot.
    local seed="${WHIPPET_BENCH_SEED:-$(date +%s)}"
    echo "bench: ${label} rate=${rate}/s duration=${duration}s warmup=${warmup}s reps=${repetitions} seed=${seed}"
    # Run as the invoking user so the bind-mounted ./artifacts directory is
    # writable on Linux CI as well as Docker Desktop.
    "${COMPOSE[@]}" --profile bench run --rm --user "$(id -u):$(id -g)" \
        -e WHIPPET_GIT_SHA="${GIT_SHA}" \
        -e WHIPPET_GIT_DIRTY="${GIT_DIRTY}" \
        -e WHIPPET_IMAGE_DIGEST="${IMAGE_DIGEST}" \
        -e WHIPPET_VIRTUALIZATION=docker-desktop \
        bench \
        bench \
        --gateway http://node:8080 \
        --rate "${rate}" \
        --duration "${duration}" \
        --warmup "${warmup}" \
        --repetitions "${repetitions}" \
        --seed "${seed}" \
        --mode durable \
        --output-dir /artifacts/bench \
        --label "${label}"
}

if [ "${MODE}" = "smoke" ] || [ "${MODE}" = "all" ]; then
    run_bench smoke 200 10 2 1
fi

if [ "${MODE}" = "full" ] || [ "${MODE}" = "all" ]; then
    run_bench L1 1000 30 5 5
fi

echo "bench: manifests in ${ROOT}/artifacts/bench"
ls -1 "${ROOT}/artifacts/bench"/*.json 2>/dev/null | tail -n 10 || true
