#!/usr/bin/env bash
# Native Linux headline runner for the full benchmark matrix.
#
#   scripts/bench-linux.sh          # everything below (default)
#   scripts/bench-linux.sh l1       # L1: 1 symbol, rates x batches
#   scripts/bench-linux.sh l2       # L2: 3 configured symbols at 10k/s
#   scripts/bench-linux.sh f1       # F1: WebSocket fanout 1/10/100/1000
#   scripts/bench-linux.sh r1       # R1: deterministic replay proof
#   scripts/bench-linux.sh r2       # R2: replay + live 10k/s proof
#   scripts/bench-linux.sh all      # l1 then l2 then f1 then r1 then r2
#
# This script is deliberately strict. A headline number needs a bare-metal Linux
# host, a quiet machine, a performance governor, and no SMT sharing. The script
# records what it found, refuses to call a container or VM run a headline, and
# writes the raw manifests plus the environment capture to
# docs/benchmarks/2026-11-linux/ with a manifest index and a README.md method
# card. It never commits artifacts; it prints the path at the end.
#
# Matrix (defaults; override with environment):
#   L1  1 symbol (BTC-USD), rates 1k/5k/10k/25k/50k x batches 1/10/100/256
#   L2  3 configured symbols (BTC-USD,ETH-USD,SOL-USD) at 10k/s x batches
#   F1  fanout 1/10/100/1000 subs, --parse envelope,
#       --ingress-rate 10000 --ingress-batch 10
#   R1  scripts/replay.sh (full vs snapshot-seeded, hashes must match)
#   R2  replay + live 10k/s batch-10 proof (background load + replay)
#
# Environment:
#   WHIPPET_GATEWAY          node URL (default http://127.0.0.1:8080)
#   WHIPPET_BIN              existing release binary (default target/release/whippet)
#   WHIPPET_PIN_CPUS         taskset CPU list for the generator, e.g. 2-5
#   WHIPPET_ISOLATED_CORES   isolcpus list to record in the manifest
#   WHIPPET_ALLOW_VIRTUALIZED=1  run anyway, labelled dev-loop, never headline
#   WHIPPET_BENCH_SEED       RNG seed (fixed by default so runs compare)
#   WHIPPET_BENCH_IOC_RATIO  IOC share of ingress events (default 0.10)
#   WHIPPET_BENCH_CANCEL_RATIO  cancel share of ingress events (default 0.10)
#   WHIPPET_TRANSPORT        http (default) or tcp
#   WHIPPET_TCP_ADDR         TCP ingress address (default 127.0.0.1:8081)
#   WHIPPET_L1_RATES         comma-separated L1 rates (default 1000,5000,10000,25000,50000)
#   WHIPPET_L1_BATCHES       comma-separated L1 batches (default 1,10,100,256)
#   WHIPPET_L2_RATE          L2 rate (default 10000)
#   WHIPPET_L2_BATCHES       comma-separated L2 batches (default 1,10,100,256)
#   WHIPPET_FANOUT_SUBSCRIBERS  comma-separated fanout counts (default 1,10,100,1000)
#   WHIPPET_FANOUT_DURATION    fanout measured seconds per run (default 300)
#   WHIPPET_FANOUT_WARMUP      fanout warmup seconds per run (default 60)
#   WHIPPET_DURATION         measured seconds for L1/L2 (default 300)
#   WHIPPET_WARMUP           warmup seconds for L1/L2 (default 60)
#   WHIPPET_REPETITIONS      repetitions for L1/L2 headline rows (default 5)
#   WHIPPET_LINUX_RUN_DIR    run directory (default docs/benchmarks/2026-11-linux)
#   WHIPPET_LINUX_NO_RESET=1 skip the fresh-stack reset before the matrix
#
# The node itself runs on the same host (Docker Compose iggy+node by default).
# The script resets the stack (make reset equivalent) before the matrix and
# stops the simulator, unless WHIPPET_LINUX_NO_RESET=1 is set.

set -euo pipefail

# Every fanout subscriber comes from one host, so disable the per-IP WebSocket
# sub-limit; the global cap still applies.
export WHIPPET_WS_PER_IP=0

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-all}"
GATEWAY="${WHIPPET_GATEWAY:-http://127.0.0.1:8080}"
RUN_DIR="${WHIPPET_LINUX_RUN_DIR:-${ROOT}/docs/benchmarks/2026-11-linux}"
RAW="${RUN_DIR}/raw"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DURATION="${WHIPPET_DURATION:-300}"
WARMUP="${WHIPPET_WARMUP:-60}"
REPETITIONS="${WHIPPET_REPETITIONS:-5}"
L1_RATES="${WHIPPET_L1_RATES:-1000,5000,10000,25000,50000}"
L1_BATCHES="${WHIPPET_L1_BATCHES:-1,10,100,256}"
L2_RATE="${WHIPPET_L2_RATE:-10000}"
L2_BATCHES="${WHIPPET_L2_BATCHES:-1,10,100,256}"
FANOUT_SUBS="${WHIPPET_FANOUT_SUBSCRIBERS:-1,10,100,1000}"
FANOUT_DURATION="${WHIPPET_FANOUT_DURATION:-300}"
FANOUT_WARMUP="${WHIPPET_FANOUT_WARMUP:-60}"
SEED="${WHIPPET_BENCH_SEED:-$((0x5eed0001))}"
TRANSPORT="${WHIPPET_TRANSPORT:-http}"
TCP_ADDR="${WHIPPET_TCP_ADDR:-127.0.0.1:8081}"
VIRTUALIZATION="native"
VIRT_KIND=""

log() { printf 'bench-linux: %s\n' "$*"; }
die() { printf 'bench-linux: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
usage: scripts/bench-linux.sh [l1|l2|f1|r1|r2|all]

Runs the full L1/L2/F1/R1/R2 matrix natively on Linux. Refuses a headline run
from a container or VM. See the header of this script for the environment knobs.
EOF
}

case "${MODE}" in
    l1|l2|f1|r1|r2|all) ;;
    -h|--help)
        usage
        exit 0
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac

[ "$(uname -s)" = "Linux" ] || die "this runner is for native Linux; use scripts/bench.sh on other hosts"
command -v cargo >/dev/null 2>&1 || die "cargo is required"
command -v curl >/dev/null 2>&1 || die "curl is required to health-check the node"

# --- virtualization gate -----------------------------------------------------
# A container is never bare metal. Neither is a VM, a WSL guest, or a host
# whose kernel says it is virtualized. The hypervisor flag in /proc/cpuinfo is
# checked explicitly: KVM, Hyper-V, and VMware all set it.
if [ -f /.dockerenv ]; then
    VIRTUALIZATION="container"
    VIRT_KIND="docker"
fi
if grep -qaE 'docker|containerd|kubepods|lxc' /proc/1/cgroup 2>/dev/null; then
    VIRTUALIZATION="container"
    VIRT_KIND="${VIRT_KIND:-container}"
fi
if grep -q '^flags.*\bhypervisor\b' /proc/cpuinfo 2>/dev/null; then
    VIRTUALIZATION="linux-vm"
    VIRT_KIND="${VIRT_KIND:-hypervisor-flag}"
fi
if command -v systemd-detect-virt >/dev/null 2>&1; then
    detected="$(systemd-detect-virt 2>/dev/null || true)"
    case "${detected}" in
        ""|none) ;;
        *)
            VIRTUALIZATION="linux-vm"
            VIRT_KIND="${detected}"
            ;;
    esac
fi

SCOPE="headline"
if [ "${VIRTUALIZATION}" != "native" ]; then
    if [ "${WHIPPET_ALLOW_VIRTUALIZED:-0}" != "1" ]; then
        die "refusing a headline run: this host is virtualized (${VIRT_KIND:-unknown}). Run on bare metal, or set WHIPPET_ALLOW_VIRTUALIZED=1 for a dev-loop run."
    fi
    VIRTUALIZATION="linux-vm"
    SCOPE="dev-loop"
    log "WARNING: virtualized host (${VIRT_KIND:-unknown}); the manifest will say scope=dev-loop and must not be quoted as a headline."
fi

# --- run directory -----------------------------------------------------------
mkdir -p "${RAW}"

# --- environment capture -----------------------------------------------------
capture() {
    local name="$1"
    shift
    { "$@" 2>&1 || true; } >"${RAW}/linux-${STAMP}-${name}.txt"
}
capture lscpu lscpu
capture kernel uname -a
capture cmdline cat /proc/cmdline
capture cpuinfo sh -c 'grep -m1 "model name" /proc/cpuinfo; grep -c ^processor /proc/cpuinfo'
capture meminfo sh -c 'grep -E "^(MemTotal|SwapTotal):" /proc/meminfo'
capture virt sh -c 'systemd-detect-virt 2>/dev/null || echo none'
capture cpu-flags sh -c 'grep -m1 "^flags" /proc/cpuinfo'
capture governor sh -c 'cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null | sort | uniq -c || echo unknown'
capture smt sh -c 'cat /sys/devices/system/cpu/smt/active 2>/dev/null || echo unknown'
capture isolcpus sh -c 'grep -o "isolcpus=[^ ]*" /proc/cmdline || echo "isolcpus=(cmdline none) WHIPPET_ISOLATED_CORES=${WHIPPET_ISOLATED_CORES:-unset}"'
capture numactl sh -c 'command -v numactl >/dev/null 2>&1 && numactl --hardware || echo "numactl not installed"'
capture docker-version sh -c 'command -v docker >/dev/null 2>&1 && docker version || echo "docker not installed"'
log "environment capture: ${RAW}/linux-${STAMP}-*.txt"

# --- host checks -------------------------------------------------------------
GOVERNOR="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
if [ "${GOVERNOR}" != "performance" ]; then
    log "WARNING: CPU governor is '${GOVERNOR}', not 'performance'; latency will vary with frequency scaling."
fi

SMT="$(cat /sys/devices/system/cpu/smt/active 2>/dev/null || echo unknown)"
case "${SMT}" in
    1) log "WARNING: SMT is active; disable it for a headline run (echo off > /sys/devices/system/cpu/smt/control)." ;;
    unknown) log "SMT state is not readable on this host." ;;
    *) ;;
esac

if grep -q '^flags.*\bhypervisor\b' /proc/cpuinfo 2>/dev/null; then
    log "WARNING: hypervisor CPU flag is present; this host is virtualized."
fi

if [ -n "${WHIPPET_PIN_CPUS:-}" ]; then
    command -v taskset >/dev/null 2>&1 || die "WHIPPET_PIN_CPUS is set but taskset is not installed"
    PIN=(taskset -c "${WHIPPET_PIN_CPUS}")
    log "pinning the generator to CPUs ${WHIPPET_PIN_CPUS}"
else
    PIN=()
    log "no CPU pinning: set WHIPPET_PIN_CPUS to a CPU list that does not include the node's cores."
fi

log "kernel: $(uname -r); logical CPUs: $(nproc)"
log "virtualization: ${VIRTUALIZATION}${VIRT_KIND:+ (${VIRT_KIND})}"

# --- build and provenance ----------------------------------------------------
BIN="${WHIPPET_BIN:-${ROOT}/target/release/whippet}"
if [ ! -x "${BIN}" ]; then
    log "building ${BIN}"
    cargo build --release -p whippet --manifest-path "${ROOT}/Cargo.toml"
fi

export WHIPPET_VIRTUALIZATION="${VIRTUALIZATION}"
export WHIPPET_GIT_SHA="$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git -C "${ROOT}" status --porcelain 2>/dev/null || true)" ]; then
    export WHIPPET_GIT_DIRTY=true
else
    export WHIPPET_GIT_DIRTY=false
fi
export WHIPPET_BENCH_IOC_RATIO="${WHIPPET_BENCH_IOC_RATIO:-0.10}"
export WHIPPET_BENCH_CANCEL_RATIO="${WHIPPET_BENCH_CANCEL_RATIO:-0.10}"
if [ -n "${WHIPPET_ISOLATED_CORES:-}" ]; then
    export WHIPPET_ISOLATED_CORES
fi
if command -v docker >/dev/null 2>&1; then
    IMAGE_DIGEST="$(docker image inspect whippet:0.1.0 --format '{{.Id}}' 2>/dev/null || echo unknown)"
else
    IMAGE_DIGEST="native-binary"
fi
export WHIPPET_IMAGE_DIGEST="${IMAGE_DIGEST}"
log "image digest: ${IMAGE_DIGEST}"

# --- fresh stack, simulator stopped ------------------------------------------
if [ "${WHIPPET_LINUX_NO_RESET:-0}" != "1" ]; then
    if command -v docker >/dev/null 2>&1; then
        log "fresh stack: scripts/reset.sh (down -v, keeps local files and secrets)"
        "${ROOT}/scripts/reset.sh"
        "${ROOT}/scripts/gen-secrets.sh" >/dev/null
        log "starting iggy + node"
        docker compose -f "${ROOT}/deploy/compose.yaml" up -d --wait --wait-timeout 180 iggy node
    else
        log "docker not installed; skipping stack reset (native binary mode)"
    fi
else
    log "WHIPPET_LINUX_NO_RESET=1: keeping the current stack"
fi

if command -v docker >/dev/null 2>&1; then
    if docker compose -f "${ROOT}/deploy/compose.yaml" ps --status running --services 2>/dev/null | grep -qx sim; then
        log "stopping simulator for a quiet measurement"
        docker compose -f "${ROOT}/deploy/compose.yaml" stop sim >/dev/null
    fi
fi

curl -fsS --max-time 5 "${GATEWAY}/api/health" >/dev/null \
    || die "node is not healthy at ${GATEWAY}"

# --- matrix ------------------------------------------------------------------
run_case() {
    local label="$1" rate="$2" symbols="$3" batch="$4"
    log "case ${label}: transport ${TRANSPORT}, ${rate}/s, ${DURATION}s measured, ${WARMUP}s warmup, ${REPETITIONS} repetitions, symbols ${symbols}, batch ${batch}"
    "${PIN[@]}" "${BIN}" bench \
        --gateway "${GATEWAY}" \
        --transport "${TRANSPORT}" \
        --tcp-addr "${TCP_ADDR}" \
        --rate "${rate}" \
        --duration "${DURATION}" \
        --warmup "${WARMUP}" \
        --repetitions "${REPETITIONS}" \
        --seed "${SEED}" \
        --symbols "${symbols}" \
        --batch-size "${batch}" \
        --mode durable \
        --output-dir "${RAW}" \
        --label "${label}"
}

run_l1() {
    IFS=',' read -r -a rates <<<"${L1_RATES}"
    IFS=',' read -r -a batches <<<"${L1_BATCHES}"
    for rate in "${rates[@]}"; do
        for batch in "${batches[@]}"; do
            run_case "linux-L1-b${batch}-r${rate}" "${rate}" "BTC-USD" "${batch}"
        done
    done
}

run_l2() {
    # The demo exchange configures three instruments, not the dossier's ten.
    # The label says so; the manifest records the symbol list.
    log "L2 note: running the three configured instruments (L2-3sym), not the dossier's ten-symbol L2."
    IFS=',' read -r -a batches <<<"${L2_BATCHES}"
    for batch in "${batches[@]}"; do
        run_case "linux-L2-3sym-b${batch}-r${L2_RATE}" "${L2_RATE}" "BTC-USD,ETH-USD,SOL-USD" "${batch}"
    done
}

run_f1() {
    # 1,000 subscribers needs a direct network. Docker Desktop's host port
    # proxy fails around 1,000 concurrent WebSockets before the node's 2,048
    # cap is reached; run this row on native Linux with a direct gateway.
    command -v cargo >/dev/null 2>&1 || die "cargo is required for the fanout harness"
    cargo build --release -p whippet-bench --example fanout
    IFS=',' read -r -a subs <<<"${FANOUT_SUBS}"
    for sub in "${subs[@]}"; do
        case "${sub}" in
            1000) log "fanout 1000: requires a direct network; Docker Desktop host proxy fails near this count" ;;
        esac
        log "fanout: subscribers=${sub} duration=${FANOUT_DURATION}s warmup=${FANOUT_WARMUP}s parse=envelope ingress-rate=10000 ingress-batch=10"
        # shellcheck disable=SC2086
        "${PIN[@]}" cargo run --quiet --release -p whippet-bench --example fanout -- \
            --gateway "${GATEWAY}" \
            --subscribers "${sub}" \
            --duration "${FANOUT_DURATION}" \
            --warmup "${FANOUT_WARMUP}" \
            --trades 0 \
            --parse envelope \
            --ingress-rate 10000 \
            --ingress-batch 10 \
            --output-dir "${RAW}" \
            --label "linux-F1"
    done
}

run_r1() {
    log "R1: deterministic replay proof (full vs snapshot-seeded)"
    "${ROOT}/scripts/replay.sh" 2>&1 | tee "${RAW}/linux-${STAMP}-replay-R1.log"
}

run_r2() {
    # R2: replay + live 10k/s. A background batch-10 load runs while the full
    # and snapshot-seeded replays execute; both hashes must still match.
    log "R2: replay + live 10k/s batch-10 proof"
    "${PIN[@]}" "${BIN}" bench \
        --gateway "${GATEWAY}" \
        --transport "${TRANSPORT}" \
        --tcp-addr "${TCP_ADDR}" \
        --rate 10000 \
        --duration "${DURATION}" \
        --warmup 5 \
        --repetitions 1 \
        --seed "${SEED}" \
        --symbols "BTC-USD" \
        --batch-size 10 \
        --mode durable \
        --output-dir "${RAW}" \
        --label "linux-R2-live" &
    LIVE_PID=$!
    set +e
    "${ROOT}/scripts/replay.sh" 2>&1 | tee "${RAW}/linux-${STAMP}-replay-R2.log"
    REPLAY_STATUS=${PIPESTATUS[0]}
    set -e
    wait "${LIVE_PID}" || true
    if [ "${REPLAY_STATUS}" -ne 0 ]; then
        die "R2 replay proof failed; see ${RAW}/linux-${STAMP}-replay-R2.log"
    fi
    log "R2: replay hashes matched under live load"
}

if [ "${MODE}" = "l1" ] || [ "${MODE}" = "all" ]; then
    run_l1
fi

if [ "${MODE}" = "l2" ] || [ "${MODE}" = "all" ]; then
    run_l2
fi

if [ "${MODE}" = "f1" ] || [ "${MODE}" = "all" ]; then
    run_f1
fi

if [ "${MODE}" = "r1" ] || [ "${MODE}" = "all" ]; then
    run_r1
fi

if [ "${MODE}" = "r2" ] || [ "${MODE}" = "all" ]; then
    run_r2
fi

# --- manifest index + method card --------------------------------------------
{
    echo "# Linux headline run ${STAMP}"
    echo
    echo "scope: ${SCOPE} (virtualization=${VIRTUALIZATION}${VIRT_KIND:+/${VIRT_KIND}})"
    echo "commit: ${WHIPPET_GIT_SHA} dirty=${WHIPPET_GIT_DIRTY}"
    echo "image digest: ${IMAGE_DIGEST}"
    echo
    echo "## Raw artifacts"
    echo
    (cd "${RAW}" && ls -1 | sort)
} >"${RUN_DIR}/MANIFESTS.txt"

cat >"${RUN_DIR}/README.md" <<EOF
# Linux headline run ${STAMP}

## Scope

- Scope label: ${SCOPE}
- Virtualization gate: refused unless bare metal (no \`/.dockerenv\`, no
  hypervisor CPU flag, no container cgroup, \`systemd-detect-virt\` none).
  \`WHIPPET_ALLOW_VIRTUALIZED=1\` downgrades to \`dev-loop\`, never headline.
- Host: see \`raw/linux-${STAMP}-lscpu.txt\`, \`raw/linux-${STAMP}-kernel.txt\`,
  \`raw/linux-${STAMP}-meminfo.txt\`, \`raw/linux-${STAMP}-cpuinfo.txt\`.
- Governor: \`raw/linux-${STAMP}-governor.txt\` (headline needs \`performance\`).
- SMT: \`raw/linux-${STAMP}-smt.txt\` (headline needs off).
- Isolcpus: \`raw/linux-${STAMP}-isolcpus.txt\`
  (\`WHIPPET_ISOLATED_CORES=\${WHIPPET_ISOLATED_CORES:-unset}\`).
- NUMA: \`raw/linux-${STAMP}-numactl.txt\`.
- Docker: \`raw/linux-${STAMP}-docker-version.txt\`.
- Commit: ${WHIPPET_GIT_SHA}, dirty: ${WHIPPET_GIT_DIRTY}.
- Image digest: ${IMAGE_DIGEST}.

## Method

- Fresh stack (\`scripts/reset.sh\`) before the matrix unless
  \`WHIPPET_LINUX_NO_RESET=1\`; simulator stopped; node healthy at build time.
- L1: 1 symbol (BTC-USD), rates ${L1_RATES} x batches ${L1_BATCHES},
  ${DURATION}s measured, ${WARMUP}s warmup, ${REPETITIONS} repetitions.
- L2: 3 configured symbols (BTC-USD,ETH-USD,SOL-USD) at ${L2_RATE}/s x batches
  ${L2_BATCHES} (labelled \`L2-3sym\`, not the dossier's ten-symbol L2).
- F1: fanout ${FANOUT_SUBS} subscribers, \`--parse envelope --ingress-rate 10000
  --ingress-batch 10\`, ${FANOUT_DURATION}s measured, ${FANOUT_WARMUP}s warmup.
  The 1,000-subscriber row needs a direct network: Docker Desktop's host port
  proxy fails around 1,000 concurrent WebSockets before the node's 2,048 cap.
- R1: \`scripts/replay.sh\` (full vs snapshot-seeded; hashes must match).
- R2: replay under live 10k/s batch-10 load (background bench + replay).
- Transport: ${TRANSPORT} (\`WHIPPET_TRANSPORT\`), TCP addr ${TCP_ADDR}.
- Seed: ${SEED}; IOC ratio: ${WHIPPET_BENCH_IOC_RATIO};
  cancel ratio: ${WHIPPET_BENCH_CANCEL_RATIO}.
- Pinning: \`WHIPPET_PIN_CPUS=\${WHIPPET_PIN_CPUS:-unset}\`.

## Result

Results pending. The manifests in \`raw/\` are the source of truth; no numbers
are quoted here until the run completes. Report the median-p99 repetition as
the headline plus the min/max spread, and link each raw manifest.

## Artifacts

- \`raw/\`: bench manifests, fanout manifests, replay logs, environment
  captures (\`linux-${STAMP}-*.txt\`).
- \`MANIFESTS.txt\`: file index for this run.
- \`README.md\`: this method card.

Command:

\`\`\`sh
scripts/bench-linux.sh all
\`\`\`
EOF

log "manifests and environment captures in ${RAW}"
ls -1 "${RAW}"/linux-"${STAMP}"-*.txt 2>/dev/null || true
ls -1 "${RAW}"/*.json 2>/dev/null | tail -n 10 || true
if [ "${SCOPE}" = "headline" ]; then
    log "scope: headline-eligible (bare metal, native Linux). Check the manifest notes before publishing."
else
    log "scope: dev-loop only. A virtualized run is not a headline result."
fi
log "run directory (not committed automatically): ${RUN_DIR}"
log "review MANIFESTS.txt and README.md, then commit the directory explicitly if the run is publishable."
