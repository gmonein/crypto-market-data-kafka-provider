#!/usr/bin/env sh
set -eu

if [ "$#" -lt 1 ]; then
  echo "usage: run-crawlers.sh <symbol> [up|down|restart]" >&2
  exit 1
fi

symbol_raw="$1"
action="${2:-up}"

symbol_norm=$(printf "%s" "$symbol_raw" | tr '[:lower:]' '[:upper:]' | tr -d '_/ -')
if [ -z "$symbol_norm" ]; then
  symbol_norm="BTCUSDT"
fi
symbol_lower=$(printf "%s" "$symbol_norm" | tr '[:upper:]' '[:lower:]')

repo_dir=$(CDPATH= cd "$(dirname "$0")/.." && pwd)
cd "$repo_dir"

export MARKET_SYMBOL="$symbol_norm"
project_name="pm-market-data-${symbol_lower}"
infra_project="pm-market-data-infra"
compose_file="$repo_dir/docker-compose.yml"
followers_services_default="
  binance-price binance-volume binance-orderbook
  bybit-price bybit-volume bybit-orderbook
  bitget-price bitget-volume bitget-orderbook
  okx-price okx-volume okx-orderbook
  gate-price gate-volume gate-orderbook
  kucoin-futures-price kucoin-futures-volume kucoin-futures-orderbook
  chainlink-price
"
followers_services="${FOLLOWER_SERVICES:-$followers_services_default}"

if ! docker network inspect pm-net >/dev/null 2>&1; then
  docker network create pm-net >/dev/null
fi

case "$action" in
  up)
    COMPOSE_PROJECT_NAME="$infra_project" \
      docker compose -f "$compose_file" --profile infra up -d --build
    COMPOSE_PROJECT_NAME="$project_name" \
      docker compose -f "$compose_file" --profile followers up -d --build $followers_services
    ;;
  down)
    COMPOSE_PROJECT_NAME="$project_name" \
      docker compose -f "$compose_file" --profile followers down
    stop_nats="${STOP_NATS:-${STOP_REDPANDA:-0}}"
    if [ "$stop_nats" = "1" ]; then
      COMPOSE_PROJECT_NAME="$infra_project" \
        docker compose -f "$compose_file" --profile infra down
    fi
    ;;
  restart)
    COMPOSE_PROJECT_NAME="$project_name" \
      docker compose -f "$compose_file" --profile followers down
    COMPOSE_PROJECT_NAME="$infra_project" \
      docker compose -f "$compose_file" --profile infra up -d --build
    COMPOSE_PROJECT_NAME="$project_name" \
      docker compose -f "$compose_file" --profile followers up -d --build $followers_services
    ;;
  *)
    echo "usage: run-crawlers.sh <symbol> [up|down|restart]" >&2
    exit 1
    ;;
esac
