#!/usr/bin/env bash
# Run the open-loop benchmark inside the demo image.
#
#   scripts/bench.sh smoke   # short sanity run
#   scripts/bench.sh sweep   # batch-size sweep: 1 / 10 / 100 / 256 at 1k orders/s
#   scripts/bench.sh full    # L1: 1k orders/s, 30 s, 5 repetitions
#   scripts/bench.sh all     # smoke then sweep (default)
#
# Workload knobs (environment):
#   WHIPPET_BENCH_BATCH_SIZE    orders per HTTP request, 1..=256 (default 1)
#   WHIPPET_BENCH_IOC_RATIO     IOC share of ingress events (default 0.10)
#   WHIPPET_BENCH_CANCEL_RATIO  cancel share of ingress events (default 0.10)
#   WHIPPET_BENCH_SEED          RNG seed, fixed by default so runs compare
#   WHIPPET_BENCH_TRANSPORT     http (default) or tcp
#   WHIPPET_BENCH_TCP_ADDR      TCP ingress inside compose (default node:8081)
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
BATCH_SIZE="${WHIPPET_BENCH_BATCH_SIZE:-1}"
IOC_RATIO="${WHIPPET_BENCH_IOC_RATIO:-0.10}"
CANCEL_RATIO="${WHIPPET_BENCH_CANCEL_RATIO:-0.10}"
SEED="${WHIPPET_BENCH_SEED:-$((0x5eed0001))}"
TRANSPORT="${WHIPPET_BENCH_TRANSPORT:-http}"
TCP_ADDR="${WHIPPET_BENCH_TCP_ADDR:-node:8081}"
# Snapshots of the order book stall the shard. The demo default is every 1000
# commands; a short ingress run includes those stalls. Set 0 to isolate
# ingress, and the manifest note records which setting was used.
SNAPSHOT_EVERY="${WHIPPET_SNAPSHOT_EVERY:-1000}"
export WHIPPET_SNAPSHOT_EVERY="${SNAPSHOT_EVERY}"

usage() {
    cat <<'EOF'
usage: scripts/bench.sh [smoke|sweep|full|all]

  smoke   short sanity run: 200 orders/s, 10 s measured, 2 s warmup, 1 rep
  sweep   batch-size sweep at 1k orders/s: 1, 10, 100, 256 (8 s measured)
  full    L1: 1k orders/s, 30 s measured, 5 s warmup, 5 repetitions
  all     smoke then sweep (default)

Environment: WHIPPET_BENCH_BATCH_SIZE, WHIPPET_BENCH_IOC_RATIO,
WHIPPET_BENCH_CANCEL_RATIO, WHIPPET_BENCH_SEED, WHIPPET_BENCH_TRANSPORT,
WHIPPET_BENCH_TCP_ADDR.

Docker Desktop runs are dev-loop measurements, never headline results.
EOF
}

case "${MODE}" in
    smoke|sweep|full|all) ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        usage >&2
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
    local batch="${6:-${BATCH_SIZE}}"
    echo "bench: ${label} transport=${TRANSPORT} rate=${rate}/s duration=${duration}s warmup=${warmup}s reps=${repetitions} batch=${batch} ioc=${IOC_RATIO} cancel=${CANCEL_RATIO} snapshot_every=${SNAPSHOT_EVERY} seed=${SEED}"
    # Run as the invoking user so the bind-mounted ./artifacts directory is
    # writable on Linux CI as well as Docker Desktop.
    "${COMPOSE[@]}" --profile bench run --rm --user "$(id -u):$(id -g)" \
        -e WHIPPET_GIT_SHA="${GIT_SHA}" \
        -e WHIPPET_GIT_DIRTY="${GIT_DIRTY}" \
        -e WHIPPET_IMAGE_DIGEST="${IMAGE_DIGEST}" \
        -e WHIPPET_VIRTUALIZATION=docker-desktop \
        -e WHIPPET_BENCH_IOC_RATIO="${IOC_RATIO}" \
        -e WHIPPET_BENCH_CANCEL_RATIO="${CANCEL_RATIO}" \
        -e WHIPPET_SNAPSHOT_EVERY="${SNAPSHOT_EVERY}" \
        bench \
        bench \
        --gateway http://node:8080 \
        --transport "${TRANSPORT}" \
        --tcp-addr "${TCP_ADDR}" \
        --rate "${rate}" \
        --duration "${duration}" \
        --warmup "${warmup}" \
        --repetitions "${repetitions}" \
        --seed "${SEED}" \
        --batch-size "${batch}" \
        --mode durable \
        --output-dir /artifacts/bench \
        --label "${label}"
}

if [ "${MODE}" = "smoke" ] || [ "${MODE}" = "all" ]; then
    run_bench smoke 200 10 2 1
fi

if [ "${MODE}" = "sweep" ] || [ "${MODE}" = "all" ]; then
    # The gateway caps a batch at 256 orders (one Iggy message batch). The
    # sweep keeps rate x duration fixed so only the request shape changes.
    for batch in 1 10 100 256; do
        run_bench "batch-${batch}" 1000 8 2 1 "${batch}"
    done
fi

if [ "${MODE}" = "full" ]; then
    run_bench L1 1000 30 5 5
fi

echo "bench: manifests in ${ROOT}/artifacts/bench"
ls -1 "${ROOT}/artifacts/bench"/*.json 2>/dev/null | tail -n 10 || true
echo "bench: Docker Desktop output is dev-loop; see docs/BENCHMARKS.md"
