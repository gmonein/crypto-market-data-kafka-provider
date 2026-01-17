# pm-market-data

Market-data ingestion for Polymarket trading. This repo publishes exchange prices, volumes, and orderbook depth plus a Chainlink price stream into Redpanda.

## What is in this repo

- Exchange price followers: `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`
- Exchange volume followers: `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`
- Exchange orderbook followers (depth): `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`
- Chainlink price follower (Polymarket RTDS)

Note: `polymarket-positions` moved to `pm-execution` (wallet execution/monitoring).
Note: snapshots/consensus live in `pm-market-consensus`, and Polymarket books/trades live in `pm-polymarket-markets`.

## Quick start (Docker Compose)

```bash
docker compose -f docker-compose.yml up -d
```

Optional: run for a different symbol:

```bash
MARKET_SYMBOL=ETHUSDT docker compose -f docker-compose.yml up -d
```

Redpanda Console is available at `http://localhost:8080`.

## Symbols and topics

All services accept `SYMBOL` (or `PM_SYMBOL`) to select which market to follow. In Docker Compose, use `MARKET_SYMBOL` which maps to `SYMBOL`.

Topic naming rules:

- BTC default (legacy):
  - `binance_btcusd`, `binance_volume`, `binance_orderbook`
  - `bybit_btcusd`, `bybit_volume`, `bybit_orderbook`
  - `bitget_btcusd`, `bitget_volume`, `bitget_orderbook`
  - `okx_btcusd`, `okx_volume`, `okx_orderbook`
  - `coinbase_btcusd`, `coinbase_volume`, `coinbase_orderbook`
  - `kraken_btcusd`, `kraken_volume`, `kraken_orderbook`
  - `chainlink_btcusd`, `chainlink_orderbook`

- Non-BTC symbols (example: ETHUSDT):
  - `binance_ethusdt`, `binance_ethusdt_volume`, `binance_ethusdt_orderbook`
  - `bybit_ethusdt`, `bybit_ethusdt_volume`, `bybit_ethusdt_orderbook`
  - `bitget_ethusdt`, `bitget_ethusdt_volume`, `bitget_ethusdt_orderbook`
  - `okx_ethusdt`, `okx_ethusdt_volume`, `okx_ethusdt_orderbook`
  - `coinbase_ethusdt`, `coinbase_ethusdt_volume`, `coinbase_ethusdt_orderbook`
  - `kraken_ethusdt`, `kraken_ethusdt_volume`, `kraken_ethusdt_orderbook`
  - `chainlink_ethusdt`, `chainlink_ethusdt_orderbook`

You can override any topic with `-topic` flags on the individual commands.

## Running a single service

```bash
docker run --rm --network=host \
  -e SYMBOL=ETHUSDT \
  pm-market-data \
  binance-price -brokers localhost:9092
```

Or run locally:

```bash
SYMBOL=ETHUSDT go run ./cmd/binance-price -brokers localhost:9092
```

## Notes / limitations

- Exchange orderbook followers emit full top-N snapshots on each update (bids/asks arrays) and fall back to REST polling during WS stalls.

## Specs

See:

- `specs/MARKET_DATA.md`
