# pm-market-data

Market-data ingestion for Polymarket trading. This repo publishes exchange prices, volumes, and orderbook depth plus a Chainlink price stream into NATS.

## What is in this repo

- Exchange price followers: `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`, `gate`, `mexc` (contract), `kucoin-futures`
- Exchange volume followers: `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`, `gate`, `mexc` (contract), `kucoin-futures`
- Exchange orderbook followers (depth): `binance`, `bitget`, `bybit`, `okx`, `coinbase`, `kraken`, `gate`, `mexc` (contract), `kucoin-futures`
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
- `gate_ethusdt`, `gate_ethusdt_volume`, `gate_ethusdt_orderbook`
- `mexc_ethusdt`, `mexc_ethusdt_volume`, `mexc_ethusdt_orderbook`
- `kucoinfutures_ethusdt`, `kucoinfutures_ethusdt_volume`, `kucoinfutures_ethusdt_orderbook`
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

## Payload metadata fields

All market-data payloads keep legacy compact keys (`T`, `p`, `v`, `bb`, `ba`, etc.) and now include:

- `event_ts_ms`: exchange/source event timestamp (ms)
- `recv_ts_ms`: local receive timestamp right after raw message read (ms)
- `publish_ts_ms`: local timestamp right before publish (ms)
- `ingest_lag_ms`: `max(recv_ts_ms - event_ts_ms, 0)`
- `source_seq`: monotonic per-process sequence number
- `source_event_id`: venue-provided event/sequence/checksum id when available, otherwise `""`

Example price payload:

```json
{
  "T": 1765037935846,
  "p": "89921.57",
  "bb": "89921.56",
  "ba": "89921.58",
  "bq": "0.12",
  "aq": "0.08",
  "s": "BTCUSDT",
  "event_ts_ms": 1765037935846,
  "recv_ts_ms": 1765037935851,
  "publish_ts_ms": 1765037935852,
  "ingest_lag_ms": 5,
  "source_seq": 12877,
  "source_event_id": "3192775512"
}
```

Example volume payload:

```json
{
  "T": 1765037919999,
  "v": "0.0445",
  "s": "BTCUSDT",
  "event_ts_ms": 1765037919999,
  "recv_ts_ms": 1765037920001,
  "publish_ts_ms": 1765037920001,
  "ingest_lag_ms": 2,
  "source_seq": 901,
  "source_event_id": "1765037919123"
}
```

Example orderbook (features mode) payload:

```json
{
  "T": 1765037935846,
  "s": "BTCUSDT",
  "bb": "89921.56",
  "ba": "89921.58",
  "bq": "0.12",
  "aq": "0.08",
  "mid": 89921.57,
  "spr": 0.02,
  "sb": 2.2,
  "mic": 89921.56,
  "i1": 0.2,
  "i5": 0.08,
  "i10": 0.04,
  "bd5": 3.2,
  "ad5": 2.9,
  "bd10": 5.1,
  "ad10": 4.8,
  "event_ts_ms": 1765037935846,
  "recv_ts_ms": 1765037935848,
  "publish_ts_ms": 1765037935848,
  "ingest_lag_ms": 2,
  "source_seq": 45520,
  "source_event_id": "1029384756"
}
```

## Notes / limitations

- Exchange orderbook followers emit full top-N snapshots on each update (bids/asks arrays) and fall back to REST polling during WS stalls.
- Set `ORDERBOOK_MODE=rest` to use REST polling only; adjust polling with `ORDERBOOK_REST_INTERVAL_MS` (default 1000ms). Coinbase/Kraken orderbooks are configured to use REST in `docker-compose.yml`.

## Specs

See:

- `specs/MARKET_DATA.md`
