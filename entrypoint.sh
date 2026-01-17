#!/bin/sh

SERVICE=$1
shift

case "$SERVICE" in
  binance-price) exec ./binance-price "$@" ;;
  binance-volume) exec ./binance-volume "$@" ;;
  coinbase-price) exec ./coinbase-price "$@" ;;
  coinbase-volume) exec ./coinbase-volume "$@" ;;
  kraken-price) exec ./kraken-price "$@" ;;
  kraken-volume) exec ./kraken-volume "$@" ;;
  okx-price) exec ./okx-price "$@" ;;
  okx-volume) exec ./okx-volume "$@" ;;
  bybit-price) exec ./bybit-price "$@" ;;
  bybit-volume) exec ./bybit-volume "$@" ;;
  bitget-price) exec ./bitget-price "$@" ;;
  bitget-volume) exec ./bitget-volume "$@" ;;
  binance-orderbook) exec ./binance-orderbook "$@" ;;
  bybit-orderbook) exec ./bybit-orderbook "$@" ;;
  bitget-orderbook) exec ./bitget-orderbook "$@" ;;
  okx-orderbook) exec ./okx-orderbook "$@" ;;
  coinbase-orderbook) exec ./coinbase-orderbook "$@" ;;
  kraken-orderbook) exec ./kraken-orderbook "$@" ;;
  chainlink-price) exec ./chainlink-price "$@" ;;
  *)
    echo "Unknown service: $SERVICE" >&2
    exit 1
    ;;
 esac
