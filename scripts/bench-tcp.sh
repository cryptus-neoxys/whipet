#!/usr/bin/env bash
# TCP-ingress sweep over the binary length-prefixed path.
#
#   scripts/bench-tcp.sh
#
# Defaults: rates 1k / 5k / 10k orders/s, batch 1 / 10 / 100 commands per
# Submit frame, 10 s measured, 2 s warmup, one repetition, label prefix
# dev-loop-tcp. Every run goes through scripts/bench-sweep.sh, so the simulator
# is stopped, the node is health-checked, and consumer lag is drained between
# runs.
#
# Environment overrides:
#   WHIPPET_SWEEP_RATES         rates (default 1000,5000,10000)
#   WHIPPET_SWEEP_BATCHES       batch sizes (default 1,10,100)
#   WHIPPET_SWEEP_DURATION      measured seconds (default 10)
#   WHIPPET_SWEEP_WARMUP        warmup seconds (default 2)
#   WHIPPET_SWEEP_REPETITIONS   repetitions (default 1)
#   WHIPPET_TCP_ADDR            TCP ingress inside compose (default node:8081)
#   WHIPPET_SWEEP_LABEL_PREFIX  manifest label prefix (default dev-loop-tcp)
#
# A Docker Desktop run is dev-loop data, never a headline result. TCP results
# stay pending until the same sweep runs on native Linux with the simulator
# stopped.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export WHIPPET_SWEEP_TRANSPORT=tcp
export WHIPPET_SWEEP_RATES="${WHIPPET_SWEEP_RATES:-1000,5000,10000}"
export WHIPPET_SWEEP_BATCHES="${WHIPPET_SWEEP_BATCHES:-1,10,100}"
export WHIPPET_SWEEP_DURATION="${WHIPPET_SWEEP_DURATION:-10}"
export WHIPPET_SWEEP_WARMUP="${WHIPPET_SWEEP_WARMUP:-2}"
export WHIPPET_SWEEP_REPETITIONS="${WHIPPET_SWEEP_REPETITIONS:-1}"
export WHIPPET_TCP_ADDR="${WHIPPET_TCP_ADDR:-node:8081}"
export WHIPPET_SWEEP_LABEL_PREFIX="${WHIPPET_SWEEP_LABEL_PREFIX:-dev-loop-tcp}"

exec "${ROOT}/scripts/bench-sweep.sh"
