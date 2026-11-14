#!/usr/bin/env bash
# Native Linux runner for the L1/L2 benchmark matrix.
#
#   scripts/bench-linux.sh l1     # 1 symbol, 1k orders/s, 5 min x 5
#   scripts/bench-linux.sh l2     # 3 configured symbols, 10k orders/s, 5 min x 5
#   scripts/bench-linux.sh all    # l1 then l2 (default)
#
# This script is deliberately strict. A headline number needs a bare-metal Linux
# host, a quiet machine, a performance governor, and no SMT sharing. The script
# records what it found, refuses to call a container or VM run a headline, and
# writes the raw manifest plus the environment capture to artifacts/bench/.
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
#   WHIPPET_L1_BATCH         orders per request for L1 (default 1)
#   WHIPPET_L2_BATCH         orders per request for L2 (default 1)
#   WHIPPET_DURATION         measured seconds (default 300)
#   WHIPPET_WARMUP           warmup seconds (default 30)
#   WHIPPET_REPETITIONS      repetitions (default 5)
#
# The node itself is not started here: run it on the same host (or point
# WHIPPET_GATEWAY at it) and make sure no simulator or other benchmark shares
# the machine.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-all}"
GATEWAY="${WHIPPET_GATEWAY:-http://127.0.0.1:8080}"
OUT="${ROOT}/artifacts/bench"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DURATION="${WHIPPET_DURATION:-300}"
WARMUP="${WHIPPET_WARMUP:-30}"
REPETITIONS="${WHIPPET_REPETITIONS:-5}"
SEED="${WHIPPET_BENCH_SEED:-$((0x5eed0001))}"
TRANSPORT="${WHIPPET_TRANSPORT:-http}"
TCP_ADDR="${WHIPPET_TCP_ADDR:-127.0.0.1:8081}"
VIRTUALIZATION="native"
VIRT_KIND=""

log() { printf 'bench-linux: %s\n' "$*"; }
die() { printf 'bench-linux: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
usage: scripts/bench-linux.sh [l1|l2|all]

Runs the L1/L2 matrix natively on Linux. Refuses a headline run from a
container or VM. See the header of this script for the environment knobs.
EOF
}

case "${MODE}" in
    l1|l2|all) ;;
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
# whose kernel says it is virtualized.
if [ -f /.dockerenv ]; then
    VIRTUALIZATION="container"
    VIRT_KIND="docker"
fi
if grep -qaE 'docker|containerd|kubepods|lxc' /proc/1/cgroup 2>/dev/null; then
    VIRTUALIZATION="container"
    VIRT_KIND="${VIRT_KIND:-container}"
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

if [ "${VIRTUALIZATION}" != "native" ]; then
    if [ "${WHIPPET_ALLOW_VIRTUALIZED:-0}" != "1" ]; then
        die "refusing a headline run: this host is virtualized (${VIRT_KIND:-unknown}). Run on bare metal, or set WHIPPET_ALLOW_VIRTUALIZED=1 for a dev-loop run."
    fi
    VIRTUALIZATION="linux-vm"
    log "WARNING: virtualized host (${VIRT_KIND:-unknown}); the manifest will say scope=dev-loop and must not be quoted as a headline."
fi

# --- environment capture -----------------------------------------------------
mkdir -p "${OUT}"
capture() {
    local name="$1"
    shift
    { "$@" 2>&1 || true; } >"${OUT}/linux-${STAMP}-${name}.txt"
}
capture lscpu lscpu
capture kernel uname -a
capture cmdline cat /proc/cmdline
capture cpuinfo sh -c 'grep -m1 "model name" /proc/cpuinfo; grep -c ^processor /proc/cpuinfo'
capture meminfo sh -c 'grep -E "^(MemTotal|SwapTotal):" /proc/meminfo'
capture virt sh -c 'systemd-detect-virt 2>/dev/null || echo none'
log "environment capture: ${OUT}/linux-${STAMP}-*.txt"

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
        --output-dir "${OUT}" \
        --label "${label}"
}

if [ "${MODE}" = "l1" ] || [ "${MODE}" = "all" ]; then
    run_case L1 1000 "BTC-USD" "${WHIPPET_L1_BATCH:-1}"
fi

if [ "${MODE}" = "l2" ] || [ "${MODE}" = "all" ]; then
    # The demo exchange configures three instruments, not the dossier's ten.
    # The label says so; the manifest records the symbol list.
    log "L2 note: running the three configured instruments (L2-3sym), not the dossier's ten-symbol L2."
    run_case L2-3sym 10000 "BTC-USD,ETH-USD,SOL-USD" "${WHIPPET_L2_BATCH:-1}"
fi

log "manifests and environment captures in ${OUT}"
ls -1 "${OUT}"/linux-"${STAMP}"-*.txt 2>/dev/null || true
ls -1 "${OUT}"/*.json 2>/dev/null | tail -n 10 || true
if [ "${VIRTUALIZATION}" = "native" ]; then
    log "scope: headline-eligible (bare metal, native Linux). Check the manifest notes before publishing."
else
    log "scope: dev-loop only. A virtualized run is not a headline result."
fi
