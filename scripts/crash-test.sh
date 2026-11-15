#!/usr/bin/env bash
# Crash proof: run a short workload, SIGKILL the node, restart it, wait for
# readiness, and prove both replay hashes still match.
#
#   scripts/crash-test.sh
#
# The node is killed with `docker compose kill node` (SIGKILL, the Compose
# equivalent of `kill -9`), restarted with `up -d node`, and polled on
# /api/health until it is ready. `scripts/replay.sh` then runs the full and
# snapshot-seeded replays; the script fails unless both print the same
# state_hash. Raw replay output goes to artifacts/bench/crash-<ts>.log with a
# JSON summary next to it.
#
# Environment overrides:
#   WHIPPET_CRASH_RATE       orders/s for the pre-kill workload (default 1000)
#   WHIPPET_CRASH_BATCH      orders per request (default 10)
#   WHIPPET_CRASH_DURATION   pre-kill workload seconds (default 20)
#   WHIPPET_CRASH_OUTPUT_DIR output directory (default ./artifacts/bench)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml")
RATE="${WHIPPET_CRASH_RATE:-1000}"
BATCH="${WHIPPET_CRASH_BATCH:-10}"
DURATION="${WHIPPET_CRASH_DURATION:-20}"
OUTPUT_DIR="${WHIPPET_CRASH_OUTPUT_DIR:-${ROOT}/artifacts/bench}"
RESTART_SIM=0

usage() {
    cat <<'EOF'
usage: scripts/crash-test.sh

Runs a short workload, kills the node (SIGKILL), restarts it, waits for
readiness, and asserts both replay hashes match. Output goes to
artifacts/bench.

Environment: WHIPPET_CRASH_RATE, WHIPPET_CRASH_BATCH,
WHIPPET_CRASH_DURATION, WHIPPET_CRASH_OUTPUT_DIR.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

command -v docker >/dev/null 2>&1 || {
    echo "crash-test: docker is required" >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || {
    echo "crash-test: python3 is required" >&2
    exit 1
}

mkdir -p "${OUTPUT_DIR}"

GIT_SHA="$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git -C "${ROOT}" status --porcelain 2>/dev/null || true)" ]; then
    GIT_DIRTY=true
else
    GIT_DIRTY=false
fi
IMAGE_DIGEST="$(docker image inspect whippet:0.1.0 --format '{{.Id}}' 2>/dev/null || echo unknown)"

cleanup() {
    if [ "${RESTART_SIM}" -eq 1 ]; then
        "${COMPOSE[@]}" start sim >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

"${ROOT}/scripts/gen-secrets.sh" >/dev/null

echo "crash-test: starting iggy + node"
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 iggy node

if "${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx sim; then
    echo "crash-test: stopping simulator for a quiet measurement"
    "${COMPOSE[@]}" stop sim >/dev/null
    RESTART_SIM=1
fi

curl -fsS --max-time 5 http://localhost:8080/api/health >/dev/null || {
    echo "crash-test: node is not healthy" >&2
    exit 1
}

echo "crash-test: pre-kill workload rate=${RATE}/s batch=${BATCH} duration=${DURATION}s"
"${COMPOSE[@]}" --profile bench run --rm --user "$(id -u):$(id -g)" \
    -e WHIPPET_GIT_SHA="${GIT_SHA}" \
    -e WHIPPET_GIT_DIRTY="${GIT_DIRTY}" \
    -e WHIPPET_IMAGE_DIGEST="${IMAGE_DIGEST}" \
    -e WHIPPET_VIRTUALIZATION=docker-desktop \
    bench bench \
    --gateway http://node:8080 \
    --rate "${RATE}" \
    --batch-size "${BATCH}" \
    --duration "${DURATION}" \
    --warmup 2 \
    --repetitions 1 \
    --mode durable \
    --output-dir /artifacts/bench \
    --label crash

echo "crash-test: killing the node (SIGKILL)"
"${COMPOSE[@]}" kill node

echo "crash-test: restarting the node"
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 node

echo "crash-test: waiting for readiness"
ready=0
for _ in $(seq 1 60); do
    if curl -fsS --max-time 5 http://localhost:8080/api/health >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 2
done
if [ "${ready}" -ne 1 ]; then
    echo "crash-test: FAIL - node did not become ready after restart" >&2
    exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPLAY_LOG="${OUTPUT_DIR}/crash-${STAMP}.log"
SUMMARY="${OUTPUT_DIR}/crash-${STAMP}.json"

echo "crash-test: running replay (full vs snapshot-seeded)"
set +e
"${ROOT}/scripts/replay.sh" >"${REPLAY_LOG}" 2>&1
REPLAY_STATUS=$?
set -e
cat "${REPLAY_LOG}"

python3 - "${REPLAY_LOG}" "${SUMMARY}" "${REPLAY_STATUS}" "${RATE}" "${BATCH}" "${DURATION}" "${GIT_SHA}" "${GIT_DIRTY}" <<'PY'
import json, sys
from datetime import datetime, timezone

log_path, summary_path = sys.argv[1], sys.argv[2]
replay_status = int(sys.argv[3])
rate, batch, duration = int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
git_sha, git_dirty = sys.argv[7], sys.argv[8] == "true"

full_hash = seeded_hash = None
with open(log_path) as handle:
    for line in handle:
        line = line.strip()
        if line.startswith("full:"):
            for token in line.split():
                if token.startswith("hash="):
                    full_hash = token.split("=", 1)[1]
        elif line.startswith("seeded:"):
            for token in line.split():
                if token.startswith("hash="):
                    seeded_hash = token.split("=", 1)[1]

match = (
    replay_status == 0
    and full_hash is not None
    and full_hash == seeded_hash
)
summary = {
    "kind": "crash",
    "created_at": datetime.now(timezone.utc).isoformat(),
    "git_sha": git_sha,
    "git_dirty": git_dirty,
    "workload": {"rate_per_sec": rate, "batch_size": batch, "duration_secs": duration},
    "kill": "docker compose kill node (SIGKILL)",
    "full_hash": full_hash,
    "seeded_hash": seeded_hash,
    "verdict": "pass" if match else "fail",
}
with open(summary_path, "w") as handle:
    json.dump(summary, handle, indent=2)
print(json.dumps(summary, indent=2))
sys.exit(0 if match else 1)
PY
STATUS=$?

echo "crash-test: replay log in ${REPLAY_LOG}, summary in ${SUMMARY}"
exit "${STATUS}"
