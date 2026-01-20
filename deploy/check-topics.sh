#!/usr/bin/env sh
set -eu

nats_bin="${NATS_BIN:-$HOME/.local/bin/nats}"
server="${NATS_SERVER:-nats://localhost:4222}"

symbols="${SYMBOLS:-BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT}"
exchanges="${EXCHANGES:-binance,bybit,bitget,okx,coinbase,kraken}"

price_timeout="${PRICE_TIMEOUT_SECONDS:-1}"
orderbook_timeout="${ORDERBOOK_TIMEOUT_SECONDS:-1}"
volume_timeout="${VOLUME_TIMEOUT_SECONDS:-5}"
chainlink_timeout="${CHAINLINK_TIMEOUT_SECONDS:-2}"

if [ ! -x "$nats_bin" ]; then
  echo "nats cli not found at $nats_bin" >&2
  exit 2
fi

if ! command -v timeout >/dev/null 2>&1; then
  echo "timeout command not found" >&2
  exit 2
fi

now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "ts=$now server=$server"

fail=0

for sym in $(printf '%s' "$symbols" | tr ',' ' '); do
  sym_lower=$(printf "%s" "$sym" | tr '[:upper:]' '[:lower:]')
  for ex in $(printf '%s' "$exchanges" | tr ',' ' '); do
    for suffix in "" "_volume" "_orderbook"; do
      case "$suffix" in
        "_volume") t="$volume_timeout" ;;
        "_orderbook") t="$orderbook_timeout" ;;
        *) t="$price_timeout" ;;
      esac
      subj="${ex}_${sym_lower}${suffix}"
      if timeout "${t}s" "$nats_bin" sub --count 1 -s "$server" "$subj" >/dev/null 2>&1; then
        echo "OK $subj"
      else
        echo "NO DATA $subj"
        fail=1
      fi
    done
  done
  subj="chainlink_${sym_lower}"
  if timeout "${chainlink_timeout}s" "$nats_bin" sub --count 1 -s "$server" "$subj" >/dev/null 2>&1; then
    echo "OK $subj"
  else
    echo "NO DATA $subj"
    fail=1
  fi
  echo "---"
done

exit "$fail"
