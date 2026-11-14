#!/usr/bin/env bash
# EXPERIMENTAL chaos helper. Not part of the demo path and not validated.
#
#   scripts/chaos.sh --yes toxiproxy   # start the Toxiproxy control plane
#   scripts/chaos.sh --yes pause       # pause Iggy for 5s, then unpause
#
# Toxiproxy is not in the order path until the node is pointed at it:
#   WHIPPET_IGGY_ADDR=toxiproxy:8666
# Change that in a local Compose override and restart the node. The default
# stack keeps the direct iggy:8090 path.
#
# Pumba is commented out in deploy/compose.chaos.yaml because it needs the
# Docker socket, which is root-equivalent on the host. Run it only on a
# disposable machine.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "${ROOT}/deploy/compose.yaml" -f "${ROOT}/deploy/compose.chaos.yaml")
ACTION="${1:-help}"
CONFIRM="${2:-}"

if [ "${ACTION}" = "--yes" ]; then
    ACTION="${2:-help}"
    CONFIRM="--yes"
fi

cat >&2 <<'WARN'
chaos: WARNING - this is an experimental skeleton.
chaos: It can interrupt the demo and is not part of any validated claim.
WARN

if [ "${ACTION}" != "help" ] && [ "${CONFIRM}" != "--yes" ]; then
    echo "chaos: refusing to act without --yes" >&2
    echo "chaos: try: scripts/chaos.sh --yes toxiproxy" >&2
    exit 2
fi

case "${ACTION}" in
    toxiproxy)
        "${COMPOSE[@]}" --profile chaos up -d toxiproxy
        cat <<'EOF'
chaos: Toxiproxy API is at http://localhost:8474
chaos: Create a proxy for Iggy TCP:
  curl -sS -X POST http://localhost:8474/proxies \
    -H 'content-type: application/json' \
    -d '{"name":"iggy-tcp","listen":"0.0.0.0:8666","upstream":"iggy:8090","enabled":true}'
chaos: Then point the node at toxiproxy:8666 and restart it. The default
chaos: stack still uses the direct iggy:8090 path.
EOF
        ;;
    pause)
        echo "chaos: pausing iggy for 5 seconds"
        "${COMPOSE[@]}" pause iggy
        sleep 5
        "${COMPOSE[@]}" unpause iggy
        echo "chaos: iggy unpaused; watch /api/health for the degraded window"
        ;;
    help|*)
        cat <<'EOF'
usage: scripts/chaos.sh --yes <action>

actions:
  toxiproxy   start the Toxiproxy control plane (profile chaos)
  pause       pause Iggy for 5 seconds, then unpause
  help        this message

The default demo does not use any of this. See deploy/compose.chaos.yaml.
EOF
        ;;
esac
