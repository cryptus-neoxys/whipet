#!/usr/bin/env bash
# Soak proof: constant-rate HTTP batch-10 ingress at 5,000 orders/s for 10
# minutes while sampling /api/state every 10 s.
#
#   scripts/soak.sh
#
# Starts the Compose stack (iggy + node), stops the simulator, runs the
# batch-10 workload in the background for the full soak, and polls /api/state
# (server ingress p50/p99, consumer lag) plus node RSS when Docker exposes it.
# Writes a JSON summary to the output directory and fails when the mean p99 of
# the last 10% of samples drifts more than 5% from the first 10%, or when a
# shard reports failure.
#
# Environment overrides:
#   WHIPPET_SOAK_RATE        orders/s (default 5000)
#   WHIPPET_SOAK_BATCH       orders per request (default 10)
#   WHIPPET_SOAK_DURATION    soak seconds (default 600)
#   WHIPPET_SOAK_WARMUP      bench warmup seconds (default 5)
#   WHIPPET_SOAK_INTERVAL    state poll seconds (default 10)
#   WHIPPET_SOAK_OUTPUT_DIR  output directory (default ./artifacts/bench)
#   WHIPPET_GATEWAY          gateway base URL on the host (default http://localhost:8080)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml")
GATEWAY="${WHIPPET_GATEWAY:-http://localhost:8080}"
RATE="${WHIPPET_SOAK_RATE:-5000}"
BATCH="${WHIPPET_SOAK_BATCH:-10}"
DURATION="${WHIPPET_SOAK_DURATION:-600}"
WARMUP="${WHIPPET_SOAK_WARMUP:-5}"
INTERVAL="${WHIPPET_SOAK_INTERVAL:-10}"
OUTPUT_DIR="${WHIPPET_SOAK_OUTPUT_DIR:-${ROOT}/artifacts/bench}"
RESTART_SIM=0
BENCH_PID=""

usage() {
    cat <<'EOF'
usage: scripts/soak.sh

Runs HTTP batch-10 ingress at 5,000 orders/s for 10 minutes, sampling
/api/state every 10 s, and writes a JSON summary to artifacts/bench.
Fails if p99 degrades >5% between the first and last 10% of samples or a
shard reports failure.

Environment: WHIPPET_SOAK_RATE, WHIPPET_SOAK_BATCH, WHIPPET_SOAK_DURATION,
WHIPPET_SOAK_WARMUP, WHIPPET_SOAK_INTERVAL, WHIPPET_SOAK_OUTPUT_DIR,
WHIPPET_GATEWAY.
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

command -v docker >/dev/null 2>&1 || {
    echo "soak: docker is required" >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || {
    echo "soak: python3 is required" >&2
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
    if [ -n "${BENCH_PID}" ] && kill -0 "${BENCH_PID}" 2>/dev/null; then
        kill "${BENCH_PID}" 2>/dev/null || true
    fi
    if [ "${RESTART_SIM}" -eq 1 ]; then
        "${COMPOSE[@]}" start sim >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

"${ROOT}/scripts/gen-secrets.sh" >/dev/null

echo "soak: starting iggy + node"
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 iggy node

if "${COMPOSE[@]}" ps --status running --services 2>/dev/null | grep -qx sim; then
    echo "soak: stopping simulator for a quiet measurement"
    "${COMPOSE[@]}" stop sim >/dev/null
    RESTART_SIM=1
fi

curl -fsS --max-time 5 "${GATEWAY}/api/health" >/dev/null || {
    echo "soak: node is not healthy on ${GATEWAY}" >&2
    exit 1
}

SAMPLES="$(mktemp)"
BENCH_LOG="$(mktemp)"
echo "soak: rate=${RATE}/s batch=${BATCH} duration=${DURATION}s interval=${INTERVAL}s"

# The bench container writes its own manifest to the shared artifacts volume;
# this script only samples /api/state while it runs.
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
    --warmup "${WARMUP}" \
    --repetitions 1 \
    --mode durable \
    --output-dir /artifacts/bench \
    --label soak >"${BENCH_LOG}" 2>&1 &
BENCH_PID=$!

NODE_CID="$("${COMPOSE[@]}" ps -q node 2>/dev/null || true)"

count=0
while kill -0 "${BENCH_PID}" 2>/dev/null || [ "${count}" -eq 0 ]; do
    t=$((count * INTERVAL))
    state="$(curl -fsS --max-time 5 "${GATEWAY}/api/state" 2>/dev/null || true)"
    health="$(curl -fsS --max-time 5 "${GATEWAY}/api/health" 2>/dev/null || true)"
    rss=""
    if [ -n "${NODE_CID}" ]; then
        rss="$(docker stats --no-stream --format '{{.MemUsage}}' "${NODE_CID}" 2>/dev/null | awk '{print $1}' || true)"
    fi
    python3 - "${t}" "${state}" "${health}" "${rss}" >>"${SAMPLES}" <<'PY'
import json, sys

t, state_raw, health_raw, rss_raw = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
try:
    metrics = json.loads(state_raw)["metrics"]
except Exception:
    metrics = {}
try:
    shards_ok = json.loads(health_raw)["shards_ok"]
except Exception:
    shards_ok = None
print(json.dumps({
    "t": int(t),
    "p50_us": metrics.get("ingress_p50_us"),
    "p99_us": metrics.get("ingress_p99_us"),
    "lag": metrics.get("consumer_lag"),
    "rss": rss_raw or None,
    "shards_ok": shards_ok,
}))
PY
    count=$((count + 1))
    if ! kill -0 "${BENCH_PID}" 2>/dev/null; then
        break
    fi
    sleep "${INTERVAL}"
done

BENCH_STATUS=0
wait "${BENCH_PID}" || BENCH_STATUS=$?
BENCH_PID=""
echo "soak: bench exited with status ${BENCH_STATUS}"
tail -n 5 "${BENCH_LOG}" || true

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY="${OUTPUT_DIR}/soak-${STAMP}.json"

INTERVAL="${INTERVAL}" python3 - "${SAMPLES}" "${SUMMARY}" "${BENCH_STATUS}" "${RATE}" "${BATCH}" "${DURATION}" "${GIT_SHA}" "${GIT_DIRTY}" <<'PY'
import json, os, sys

samples_path, summary_path = sys.argv[1], sys.argv[2]
bench_status = int(sys.argv[3])
rate, batch, duration = int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
git_sha, git_dirty = sys.argv[7], sys.argv[8] == "true"

samples = []
with open(samples_path) as handle:
    for line in handle:
        line = line.strip()
        if line:
            samples.append(json.loads(line))

p99 = [s["p99_us"] for s in samples if isinstance(s.get("p99_us"), (int, float))]
edge = max(1, int(len(p99) * 0.10))
if len(p99) >= 5:
    first = sum(p99[:edge]) / edge
    last = sum(p99[-edge:]) / edge
    drift = 0.0 if first == 0 else (last - first) / first * 100.0
else:
    first = last = drift = None

shards = [s["shards_ok"] for s in samples if s.get("shards_ok") is not None]
shards_ok = all(shards) if shards else None
failed = (
    bench_status != 0
    or shards_ok is False
    or (drift is not None and drift > 5.0)
)

summary = {
    "kind": "soak",
    "created_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
    "git_sha": git_sha,
    "git_dirty": git_dirty,
    "rate_per_sec": rate,
    "batch_size": batch,
    "duration_secs": duration,
    "interval_secs": int(os.environ.get("INTERVAL", "10")),
    "samples": len(samples),
    "first_p99_us": first,
    "last_p99_us": last,
    "drift_pct": drift,
    "shards_ok": shards_ok,
    "bench_exit": bench_status,
    "verdict": "fail" if failed else ("pass" if drift is not None else "skipped"),
    "samples_log": samples,
}
with open(summary_path, "w") as handle:
    json.dump(summary, handle, indent=2)
print(json.dumps({k: v for k, v in summary.items() if k != "samples_log"}, indent=2))
sys.exit(1 if failed else 0)
PY
STATUS=$?

echo "soak: summary in ${SUMMARY}"
rm -f "${SAMPLES}" "${BENCH_LOG}"
exit "${STATUS}"
