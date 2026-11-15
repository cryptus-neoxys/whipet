#!/usr/bin/env bash
# Prove deterministic replay for partition 1 (BTC-USD with 16 partitions).
#
# Runs two replays against the same order log:
#   1. full replay from offset 0 (intentionally not snapshot-seeded)
#   2. snapshot-seeded replay (starts at the snapshot tail)
#
# Both must print the same state_hash and end at the same offset. The seeded
# run passes --require-snapshot, so a missing snapshot, a snapshot with no
# applied commands, a snapshot at the log head, or a snapshot ahead of the
# engine's committed offset exits non-zero instead of quietly degrading to a
# second full replay. The full replay starts at offset 0 by design and cannot
# pass that flag (`seeded_from_snapshot` is false by definition).
#
# The simulator is stopped for the duration so no new orders arrive, then
# restarted if it was running. The node stays up: stopping it flushes a
# snapshot at the head, which would make the seeded replay decode no tail.
#
# Set WHIPPET_REPLAY_REQUIRE_SNAPSHOT=0 for the lenient dev path (also exposed
# as `make replay-dev`).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml")
PARTITION="${WHIPPET_REPLAY_PARTITION:-1}"
PARTITIONS="${WHIPPET_PARTITIONS:-16}"
REQUIRE_SNAPSHOT="${WHIPPET_REPLAY_REQUIRE_SNAPSHOT:-1}"
SIM_WAS_RUNNING=0

command -v docker >/dev/null 2>&1 || {
    echo "replay: docker is required" >&2
    exit 1
}
command -v python3 >/dev/null 2>&1 || {
    echo "replay: python3 is required" >&2
    exit 1
}

"${ROOT}/scripts/gen-secrets.sh" >/dev/null

# Iggy and the node must be up. The one-off replay container uses the node
# service definition (network, volume, secret, env).
"${COMPOSE[@]}" up -d --wait --wait-timeout 180 iggy node

running="$("${COMPOSE[@]}" ps --status running --services 2>/dev/null || true)"
if grep -qx sim <<<"${running}"; then
    SIM_WAS_RUNNING=1
fi

cleanup() {
    if [ "${SIM_WAS_RUNNING}" -eq 1 ]; then
        "${COMPOSE[@]}" start sim >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [ "${SIM_WAS_RUNNING}" -eq 1 ]; then
    echo "replay: stopping simulator so the log is stable"
    "${COMPOSE[@]}" stop sim >/dev/null
fi

run_replay() {
    "${COMPOSE[@]}" run --rm --no-deps \
        -e WHIPPET_IGGY_ADDR=iggy:8090 \
        node replay \
        --partition "${PARTITION}" \
        --snapshot-dir /data/snapshots \
        --exchange-partitions "${PARTITIONS}" \
        "$@"
}

echo "replay: full replay from offset 0 (intentionally not snapshot-seeded)"
full_json="$(run_replay --from 0)"

if [ "${REQUIRE_SNAPSHOT}" -eq 1 ]; then
    echo "replay: snapshot-seeded replay (fail-closed: --require-snapshot)"
    seeded_json="$(run_replay --require-snapshot)"
else
    echo "replay: snapshot-seeded replay (lenient: a full replay is accepted)"
    seeded_json="$(run_replay)"
fi

printf '%s\n' "full replay:" "${full_json}" "snapshot-seeded replay:" "${seeded_json}"

python3 - "${full_json}" "${seeded_json}" "${REQUIRE_SNAPSHOT}" <<'PY'
import json
import sys


def parse(raw):
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end < start:
        raise SystemExit("replay: no JSON object in replay output")
    return json.loads(raw[start : end + 1])


full = parse(sys.argv[1])
seeded = parse(sys.argv[2])
require_snapshot = sys.argv[3] == "1"
print(
    f"full:   hash={full['state_hash']} last_offset={full['last_offset']} "
    f"decoded={full['decoded']} seeded={full['seeded_from_snapshot']}"
)
print(
    f"seeded: hash={seeded['state_hash']} last_offset={seeded['last_offset']} "
    f"decoded={seeded['decoded']} seeded={seeded['seeded_from_snapshot']} "
    f"snapshot_offset={seeded['snapshot_offset']} "
    f"committed_offset={seeded['committed_offset']} "
    f"head={seeded['head']} tail={seeded['tail']}"
)
if full["state_hash"] != seeded["state_hash"]:
    print("replay: FAIL - state hashes differ", file=sys.stderr)
    raise SystemExit(1)
if full["last_offset"] != seeded["last_offset"]:
    print("replay: FAIL - replays ended at different offsets", file=sys.stderr)
    raise SystemExit(1)
if not seeded["seeded_from_snapshot"]:
    if require_snapshot:
        print(
            "replay: FAIL - --require-snapshot was requested but the run was not "
            "seeded from a snapshot",
            file=sys.stderr,
        )
        raise SystemExit(1)
    print(
        "replay: note - no snapshot was available; the seeded replay was a full replay",
        file=sys.stderr,
    )
elif seeded["decoded"] == 0:
    print(
        "replay: FAIL - the snapshot-seeded replay decoded no tail commands; "
        "the snapshot is at the log head and proves nothing",
        file=sys.stderr,
    )
    raise SystemExit(1)
print("replay: ok - both hashes match")
PY
