#!/usr/bin/env bash
# End-to-end smoke test for a running demo stack.
#
#   1. GET /api/health and require status=ok.
#   2. Submit a crossing sell/buy pair with distinct participants.
#   3. Poll /api/state until a trade containing one of those order ids appears.
#
# Exits non-zero on any failure. Use `make verify` for the snapshot hash check
# as well.

set -euo pipefail

BASE_URL="${WHIPPET_URL:-http://localhost:8080}"
SYMBOL="${WHIPPET_VERIFY_SYMBOL:-BTC-USD}"
PRICE_CENTS="${WHIPPET_VERIFY_PRICE_CENTS:-6900000}"
QTY="${WHIPPET_VERIFY_QTY:-3}"
TIMEOUT_SECS="${WHIPPET_VERIFY_TIMEOUT_SECS:-15}"
STAMP="$(date +%s%N)"

fail() {
    echo "verify: $*" >&2
    exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

health="$(curl -fsS --max-time 5 "${BASE_URL}/api/health")" \
    || fail "GET ${BASE_URL}/api/health failed; is the stack up?"

python3 - "${health}" <<'PY' || fail "health endpoint did not report status=ok"
import json
import sys

health = json.loads(sys.argv[1])
print(
    f"health: status={health['status']} mode={health['mode']} "
    f"iggy_connected={health['iggy_connected']} partitions={health['partitions']}"
)
raise SystemExit(0 if health.get("status") == "ok" else 1)
PY

post_order() {
    local side="$1" client_id="$2" participant="$3"
    local body response status
    body="$(python3 - "${SYMBOL}" "${side}" "${PRICE_CENTS}" "${QTY}" "${client_id}" "${participant}" <<'PY'
import json
import sys

symbol, side, price, qty, client_id, participant = sys.argv[1:]
print(json.dumps({
    "symbol": symbol,
    "side": side,
    "order_type": "limit",
    "price_cents": int(price),
    "qty": int(qty),
    "client_order_id": client_id,
    "participant": participant,
}))
PY
)"
    if ! response="$(curl -sS --max-time 10 -w $'\n%{http_code}' \
        -X POST -H 'content-type: application/json' \
        --data "${body}" "${BASE_URL}/api/orders")"; then
        fail "POST /api/orders failed for ${side}"
    fi
    status="${response##*$'\n'}"
    response="${response%$'\n'*}"
    if [[ "${status}" != 2* ]]; then
        fail "POST /api/orders returned HTTP ${status}: ${response}"
    fi
    printf '%s' "${response}"
}

sell_ack="$(post_order sell "verify-sell-${STAMP}" "verify-seller-${STAMP}")"
buy_ack="$(post_order buy "verify-buy-${STAMP}" "verify-buyer-${STAMP}")"

sell_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])' <<<"${sell_ack}")"
buy_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["order_id"])' <<<"${buy_ack}")"

echo "orders: sell=${sell_id} buy=${buy_id}"

deadline=$((SECONDS + TIMEOUT_SECS))
last_state=""
while (( SECONDS < deadline )); do
    last_state="$(curl -fsS --max-time 5 "${BASE_URL}/api/state")" \
        || fail "GET /api/state failed"
    if python3 - "${last_state}" "${sell_id}" "${buy_id}" <<'PY'
import json
import sys

state = json.loads(sys.argv[1])
sell_id, buy_id = sys.argv[2:]
trade = next(
    (item for item in state.get("trades", []) if item.get("order_id") in (sell_id, buy_id)),
    None,
)
if trade:
    print(
        f"trade: {trade['qty']} {trade['symbol']} @ {trade['price_cents']} cents "
        f"side={trade['side']} order_id={trade['order_id']} seq={trade['seq']}"
    )
    raise SystemExit(0)
raise SystemExit(1)
PY
    then
        echo "verify: ok"
        exit 0
    fi
    sleep 0.5
done

echo "verify: no trade for either order appeared within ${TIMEOUT_SECS}s" >&2
echo "verify: last state: ${last_state}" >&2
exit 1
