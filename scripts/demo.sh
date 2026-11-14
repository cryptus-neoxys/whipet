#!/usr/bin/env bash
# Place a crossing sell/buy pair and show the acknowledgement, book state, and
# the resulting trade.
#
#   scripts/demo.sh
#   WHIPPET_URL=http://localhost:8080 scripts/demo.sh
#
# The two orders use different participants so the engine's self-trade
# prevention does not cancel the resting order.

set -euo pipefail

BASE_URL="${WHIPPET_URL:-http://localhost:8080}"
SYMBOL="${WHIPPET_DEMO_SYMBOL:-BTC-USD}"
PRICE_CENTS="${WHIPPET_DEMO_PRICE_CENTS:-6900000}"
QTY="${WHIPPET_DEMO_QTY:-5}"
WAIT_SECS="${WHIPPET_DEMO_WAIT_SECS:-10}"
STAMP="$(date +%s%N)"
SELL_CLIENT_ID="demo-sell-${STAMP}"
BUY_CLIENT_ID="demo-buy-${STAMP}"
SELL_PARTICIPANT="demo-seller-${STAMP}"
BUY_PARTICIPANT="demo-buyer-${STAMP}"

die() {
    echo "demo: $*" >&2
    exit 1
}

need() {
    command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need curl
need python3

post_json() {
    local path="$1"
    local body="$2"
    local response status
    if ! response="$(curl -sS --max-time 10 -w $'\n%{http_code}' \
        -X POST -H 'content-type: application/json' \
        --data "${body}" "${BASE_URL}${path}")"; then
        die "curl failed for POST ${path}"
    fi
    status="${response##*$'\n'}"
    response="${response%$'\n'*}"
    if [[ "${status}" != 2* ]]; then
        die "POST ${path} returned HTTP ${status}: ${response}"
    fi
    printf '%s' "${response}"
}

make_order_body() {
    local side="$1" client_id="$2" participant="$3"
    python3 - "${SYMBOL}" "${side}" "${PRICE_CENTS}" "${QTY}" "${client_id}" "${participant}" <<'PY'
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
}

order_id_of() {
    python3 -c 'import json, sys; print(json.load(sys.stdin)["order_id"])'
}

print_ack() {
    local label="$1" ack="$2"
    python3 - "${label}" "${ack}" <<'PY'
import json
import sys

label, raw = sys.argv[1], sys.argv[2]
ack = json.loads(raw)
print(
    f"{label}: order_id={ack['order_id']} partition={ack['partition']} "
    f"offset={ack['offset']} durable_ack_ms={ack['durable_ack_ms']:.2f} "
    f"status={ack['status']}"
)
PY
}

health="$(curl -fsS --max-time 5 "${BASE_URL}/api/health")" \
    || die "node is not reachable at ${BASE_URL}; run 'make up' first"
python3 - "${health}" <<'PY'
import json
import sys

health = json.loads(sys.argv[1])
print(f"health: status={health['status']} mode={health['mode']} iggy_connected={health['iggy_connected']}")
if health.get("status") != "ok":
    raise SystemExit("node is degraded; start Iggy and retry")
PY

echo "placing sell ${QTY} ${SYMBOL} @ ${PRICE_CENTS} cents (${SELL_CLIENT_ID})"
sell_ack="$(post_json /api/orders "$(make_order_body sell "${SELL_CLIENT_ID}" "${SELL_PARTICIPANT}")")"
print_ack "sell ack" "${sell_ack}"
SELL_ORDER_ID="$(printf '%s' "${sell_ack}" | order_id_of)"

echo "placing buy  ${QTY} ${SYMBOL} @ ${PRICE_CENTS} cents (${BUY_CLIENT_ID})"
buy_ack="$(post_json /api/orders "$(make_order_body buy "${BUY_CLIENT_ID}" "${BUY_PARTICIPANT}")")"
print_ack "buy ack " "${buy_ack}"
BUY_ORDER_ID="$(printf '%s' "${buy_ack}" | order_id_of)"

state=""
trade_found=0
deadline=$((SECONDS + WAIT_SECS))
while (( SECONDS < deadline )); do
    state="$(curl -fsS --max-time 5 "${BASE_URL}/api/state")" \
        || die "could not read /api/state"
    if result="$(python3 - "${state}" "${SELL_ORDER_ID}" "${BUY_ORDER_ID}" "${SYMBOL}" <<'PY'
import json
import sys

state = json.loads(sys.argv[1])
sell_id, buy_id, symbol = sys.argv[2:]
trade = next(
    (item for item in state.get("trades", []) if item.get("order_id") in (sell_id, buy_id)),
    None,
)
book = next((item for item in state.get("symbols", []) if item.get("symbol") == symbol), None)
if trade:
    print(
        f"trade: {trade['qty']} {trade['symbol']} @ {trade['price_cents']} cents "
        f"side={trade['side']} order_id={trade['order_id']} seq={trade['seq']}"
    )
if book:
    print(
        f"book: {symbol} best_bid={book['best_bid_cents']} "
        f"best_ask={book['best_ask_cents']} last_trade={book['last_trade_price_cents']} "
        f"bids={len(book['bids'])} asks={len(book['asks'])}"
    )
print(f"trades observed: {state.get('metrics', {}).get('trades', '?')}")
raise SystemExit(0 if trade else 1)
PY
)"; then
        printf '%s\n' "${result}"
        trade_found=1
        break
    fi
    sleep 0.5
done

if [ "${trade_found}" -ne 1 ]; then
    echo "demo: no trade containing either demo order appeared within ${WAIT_SECS}s" >&2
    echo "demo: last state: ${state}" >&2
    exit 1
fi

echo "demo: ok"
