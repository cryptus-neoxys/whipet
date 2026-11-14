#!/usr/bin/env bash
# Stop every Whippet Compose stack and delete its named volumes.
#
#   scripts/reset.sh             # down -v, keep local files and secrets
#   scripts/reset.sh --data      # also remove data/ and artifacts/bench/
#   scripts/reset.sh --all       # --data plus the rest of artifacts/
#   scripts/reset.sh --secrets   # also remove .env and secrets/ (destructive)
#
# `make reset` calls this with no flags. Secrets are kept by default so the
# next `make up` can reuse them; deleting secrets requires an explicit flag.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOVE_DATA=0
REMOVE_ARTIFACTS=0
REMOVE_SECRETS=0

for arg in "$@"; do
    case "${arg}" in
        --data)
            REMOVE_DATA=1
            ;;
        --all)
            REMOVE_DATA=1
            REMOVE_ARTIFACTS=1
            ;;
        --secrets)
            REMOVE_SECRETS=1
            ;;
        -h|--help)
            sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown argument: ${arg}" >&2
            exit 2
            ;;
    esac
done

if ! command -v docker >/dev/null 2>&1; then
    echo "reset: docker is required" >&2
    exit 1
fi

down() {
    local file="$1"
    shift
    echo "reset: docker compose -f ${file#"${ROOT}/"} down -v"
    docker compose -f "${file}" "$@" down -v --remove-orphans
}

# Default stack, including every profile in that file.
down "${ROOT}/deploy/compose.yaml" --profile "*"

# Override files add services to the same project. Tear them down explicitly
# so obs/chaos containers and volumes are not left behind.
if [ -f "${ROOT}/deploy/compose.obs.yaml" ]; then
    docker compose -f "${ROOT}/deploy/compose.yaml" -f "${ROOT}/deploy/compose.obs.yaml" \
        --profile "*" down -v --remove-orphans
fi
if [ -f "${ROOT}/deploy/compose.chaos.yaml" ]; then
    docker compose -f "${ROOT}/deploy/compose.yaml" -f "${ROOT}/deploy/compose.chaos.yaml" \
        --profile "*" down -v --remove-orphans
fi

# The HA skeleton is a separate project.
if [ -f "${ROOT}/deploy/compose.ha.yaml" ]; then
    down "${ROOT}/deploy/compose.ha.yaml"
fi

if [ "${REMOVE_DATA}" -eq 1 ]; then
    rm -rf "${ROOT}/data" "${ROOT}/artifacts/bench"
    echo "reset: removed data/ and artifacts/bench/"
fi

if [ "${REMOVE_ARTIFACTS}" -eq 1 ]; then
    rm -rf "${ROOT}/artifacts"
    echo "reset: removed artifacts/"
fi

if [ "${REMOVE_SECRETS}" -eq 1 ]; then
    rm -f "${ROOT}/.env"
    rm -rf "${ROOT}/secrets"
    echo "reset: removed .env and secrets/"
fi

echo "reset: done"
