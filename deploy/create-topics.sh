#!/usr/bin/env sh
set -eu

BROKER_CONTAINER="${BROKER_CONTAINER:-pm-market-data-redpanda}"
BROKER_ADDR="${BROKER_ADDR:-redpanda:9092}"

if [ -n "${SYMBOLS:-}" ]; then
  SYMBOLS_INPUT="$SYMBOLS"
elif [ "$#" -gt 0 ]; then
  SYMBOLS_INPUT="$*"
else
  SYMBOLS_INPUT="BTCUSDT"
fi

SYMBOLS_INPUT=$(printf "%s" "$SYMBOLS_INPUT" | tr ',' ' ')

wait_for_redpanda() {
  attempts=0
  until docker exec "$BROKER_CONTAINER" rpk cluster info -X brokers="$BROKER_ADDR" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ]; then
      echo "redpanda not ready after 60s" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_for_redpanda

for SYMBOL in $SYMBOLS_INPUT; do
  SYMBOL=$(printf "%s" "$SYMBOL" | tr '[:lower:]' '[:upper:]' | tr -d '_/ -')
  if [ -z "$SYMBOL" ]; then
    SYMBOL="BTCUSDT"
  fi
  SYMBOL_LOWER=$(printf "%s" "$SYMBOL" | tr '[:upper:]' '[:lower:]')
  if [ -z "$SYMBOL_LOWER" ]; then
    SYMBOL_LOWER="btcusdt"
  fi

  BINANCE_TOPIC="binance_${SYMBOL_LOWER}"
  BINANCE_VOLUME_TOPIC="binance_${SYMBOL_LOWER}_volume"
  BINANCE_ORDERBOOK_TOPIC="binance_${SYMBOL_LOWER}_orderbook"
  BYBIT_TOPIC="bybit_${SYMBOL_LOWER}"
  BYBIT_VOLUME_TOPIC="bybit_${SYMBOL_LOWER}_volume"
  BYBIT_ORDERBOOK_TOPIC="bybit_${SYMBOL_LOWER}_orderbook"
  BITGET_TOPIC="bitget_${SYMBOL_LOWER}"
  BITGET_VOLUME_TOPIC="bitget_${SYMBOL_LOWER}_volume"
  BITGET_ORDERBOOK_TOPIC="bitget_${SYMBOL_LOWER}_orderbook"
  OKX_TOPIC="okx_${SYMBOL_LOWER}"
  OKX_VOLUME_TOPIC="okx_${SYMBOL_LOWER}_volume"
  OKX_ORDERBOOK_TOPIC="okx_${SYMBOL_LOWER}_orderbook"
  COINBASE_TOPIC="coinbase_${SYMBOL_LOWER}"
  COINBASE_VOLUME_TOPIC="coinbase_${SYMBOL_LOWER}_volume"
  COINBASE_ORDERBOOK_TOPIC="coinbase_${SYMBOL_LOWER}_orderbook"
  KRAKEN_TOPIC="kraken_${SYMBOL_LOWER}"
  KRAKEN_VOLUME_TOPIC="kraken_${SYMBOL_LOWER}_volume"
  KRAKEN_ORDERBOOK_TOPIC="kraken_${SYMBOL_LOWER}_orderbook"
  CHAINLINK_TOPIC="chainlink_${SYMBOL_LOWER}"

  docker exec "$BROKER_CONTAINER" rpk topic create "$BINANCE_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BINANCE_VOLUME_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BINANCE_ORDERBOOK_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BYBIT_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BYBIT_VOLUME_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BYBIT_ORDERBOOK_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BITGET_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BITGET_VOLUME_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$BITGET_ORDERBOOK_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$OKX_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$OKX_VOLUME_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$OKX_ORDERBOOK_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$COINBASE_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$COINBASE_VOLUME_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$COINBASE_ORDERBOOK_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$KRAKEN_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$KRAKEN_VOLUME_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$KRAKEN_ORDERBOOK_TOPIC" -X brokers="$BROKER_ADDR" || true
  docker exec "$BROKER_CONTAINER" rpk topic create "$CHAINLINK_TOPIC" -X brokers="$BROKER_ADDR" || true
  echo "topics ensured for ${SYMBOL}"
done
