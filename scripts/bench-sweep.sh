#!/usr/bin/env bash
# Batch-ingress sweep. This is the primary before/after experiment for the
# durable ingress bottleneck:
#
#   scripts/bench-sweep.sh
#   WHIPPET_SWEEP_RATES=1000,5000 WHIPPET_SWEEP_BATCHES=1,10,100,256 \
#     scripts/bench-sweep.sh
#   WHIPPET_SWEEP_TRANSPORT=tcp WHIPPET_TCP_ADDR=node:8081 \
#     scripts/bench-sweep.sh
#
# The simulator is stopped for the duration so background demo traffic does
# not pollute the measurement. Every run writes a manifest under
# artifacts/bench and is labeled dev-loop by the container runtime.
#
# Environment:
#   WHIPPET_SWEEP_RATES         comma-separated rates (default 1000,2000,5000)
#   WHIPPET_SWEEP_BATCHES       comma-separated batch sizes (default 1,10,100,256)
#   WHIPPET_SWEEP_TRANSPORT     http (default) or tcp
#   WHIPPET_TCP_ADDR            TCP ingress inside compose (default node:8081)
#   WHIPPET_SWEEP_LABEL_PREFIX  manifest label prefix (default sweep)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml")
RATES="${WHIPPET_SWEEP_RATES:-1000,2000,5000}"
BATCHES="${WHIPPET_SWEEP_BATCHES:-1,10,100,256}"
DURATION="${WHIPPET_SWEEP_DURATION:-10}"
WARMUP="${WHIPPET_SWEEP_WARMUP:-2}"
REPETITIONS="${WHIPPET_SWEEP_REPETITIONS:-1}"
TRANSPORT="${WHIPPET_SWEEP_TRANSPORT:-http}"
TCP_ADDR="${WHIPPET_TCP_ADDR:-node:8081}"
LABEL_PREFIX="${WHIPPET_SWEEP_LABEL_PREFIX:-sweep}"
RESTART_SIM=0

command -v docker >/dev/null 2>&1 || {
    echo "sweep: docker is required" >&2
    exit 1
}

mkdir -p "${ROOT}/artifacts/bench"
"${ROOT}/scripts/gen-secrets.sh" >/dev/null

GIT_SHA="$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git -C "${ROOT}" status --porcelain 2>/dev/null || true)" ]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi
IMAGE_DIGEST="$(docker image inspect whippet:0.1.0 --format '{{.Id}}' 2>/dev/null || echo unknown)"

wait_for_quiet() {
    # The next config must start from a drained engine, otherwise the previous
    # run's backlog is charged to the next batch size.
    local attempts=0
    while [ "${attempts}" -lt 120 ]; do
        local state lag
        state="$(curl -fsS --max-time 5 http://localhost:8080/api/state 2>/dev/null || true)"
        if [ -z "${state}" ]; then
            sleep 1
            attempts=$((attempts + 1))
            continue
        fi
        lag="$(printf '%s' "${state}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metrics"]["consumer_lag"])' 2>/dev/null || echo 999)"
        if [ "${lag}" = "0" ]; then
            sleep 2
            return 0
        fi
        sleep 1
        attempts=$((attempts + 1))
    done
    echo "sweep: warning - consumer lag did not reach zero before the next run" >&2
}

cleanup() {
    if [ "${RESTART_SIM}" -eq 1 ]; then
        "${COMPOSE[@]}" start sim >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d --wait --wait-timeout 180 iggy node
if "${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx sim; then
    "${COMPOSE[@]}" stop sim >/dev/null
    RESTART_SIM=1
fi
curl -fsS --max-time 5 http://localhost:8080/api/health >/dev/null || {
    echo "sweep: node is not healthy" >&2
    exit 1
}

IFS=',' read -r -a rate_list <<<"${RATES}"
IFS=',' read -r -a batch_list <<<"${BATCHES}"
for rate in "${rate_list[@]}"; do
    for batch in "${batch_list[@]}"; do
        label="${LABEL_PREFIX}-b${batch}-r${rate}"
        seed="$(date +%s)${batch}${rate}"
        echo "sweep: ${label} transport=${TRANSPORT} rate=${rate}/s batch=${batch}"
        "${COMPOSE[@]}" --profile bench run --rm --user "$(id -u):$(id -g)" \
            -e WHIPPET_GIT_SHA="${GIT_SHA}" \
            -e WHIPPET_GIT_DIRTY="${GIT_DIRTY}" \
            -e WHIPPET_IMAGE_DIGEST="${IMAGE_DIGEST}" \
            -e WHIPPET_VIRTUALIZATION=docker-desktop \
            bench bench \
            --gateway http://node:8080 \
            --transport "${TRANSPORT}" \
            --tcp-addr "${TCP_ADDR}" \
            --rate "${rate}" \
            --batch-size "${batch}" \
            --duration "${DURATION}" \
            --warmup "${WARMUP}" \
            --repetitions "${REPETITIONS}" \
            --seed "${seed}" \
            --mode durable \
            --output-dir /artifacts/bench \
            --label "${label}"
        wait_for_quiet
    done
done

echo "sweep: manifests in ${ROOT}/artifacts/bench"
