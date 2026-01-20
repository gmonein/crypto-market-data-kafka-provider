# pm-market-data

Market-data ingestion for Polymarket trading. This repo publishes exchange prices, volumes, and orderbook depth plus a Chainlink price stream into NATS.

## What is in this repo

- Exchange price followers: `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`
- Exchange volume followers: `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`
- Exchange orderbook followers (depth): `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`
- Chainlink price follower (Polymarket RTDS)

Note: `polymarket-positions` moved to `pm-execution` (wallet execution/monitoring).
Note: snapshots/consensus live in `pm-market-consensus`, and Polymarket books/trades live in `pm-polymarket-markets`.

## Quick start (Docker Compose)

```bash
docker network create pm-net
docker compose --profile infra --profile followers up -d
```

Optional: run for a different symbol:

```bash
MARKET_SYMBOL=ETHUSDT docker compose --profile infra --profile followers up -d
```

NATS monitoring is available at `http://localhost:8222`.

## Symbols and subjects

All services accept `SYMBOL` (or `PM_SYMBOL`) to select which market to follow. In Docker Compose, use `MARKET_SYMBOL` which maps to `SYMBOL`.

Subject naming rules:

- Subjects are always `exchange_<symbol_lower>` with optional suffixes.
  - Price: `binance_btcusdt`
  - Volume: `binance_btcusdt_volume`
  - Orderbook: `binance_btcusdt_orderbook`

Example (ETHUSDT):

- `binance_ethusdt`, `binance_ethusdt_volume`, `binance_ethusdt_orderbook`
- `bybit_ethusdt`, `bybit_ethusdt_volume`, `bybit_ethusdt_orderbook`
- `bitget_ethusdt`, `bitget_ethusdt_volume`, `bitget_ethusdt_orderbook`
- `okx_ethusdt`, `okx_ethusdt_volume`, `okx_ethusdt_orderbook`
- `coinbase_ethusdt`, `coinbase_ethusdt_volume`, `coinbase_ethusdt_orderbook`
- `kraken_ethusdt`, `kraken_ethusdt_volume`, `kraken_ethusdt_orderbook`
- `chainlink_ethusdt`

You can override any subject with `-topic` flags on the individual commands.

## Running a single service

Make sure NATS is running on `nats://localhost:4222`.

```bash
docker run --rm --network=host \
  -e SYMBOL=ETHUSDT \
  pm-market-data \
  binance-price -brokers nats://localhost:4222
```

Or run locally:

```bash
SYMBOL=ETHUSDT go run ./cmd/binance-price -brokers nats://localhost:4222
```

## Notes / limitations

- Exchange orderbook followers emit full top-N snapshots on each update (bids/asks arrays) and fall back to REST polling during WS stalls.

## Specs

See:

- `specs/MARKET_DATA.md`
