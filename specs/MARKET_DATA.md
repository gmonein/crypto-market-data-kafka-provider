# pm-market-data spec

## Scope

This repo ingests spot market data and publishes it to Redpanda:

- Price followers: Binance, Bitget, Bybit, OKX, Coinbase, Kraken
- Volume followers: Binance, Bitget, Bybit, OKX, Coinbase, Kraken
- Orderbook followers: Binance, Bitget, Bybit, OKX, Coinbase, Kraken
- Chainlink price follower (Polymarket RTDS)

## Symbol selection

- Env vars: `SYMBOL` or `PM_SYMBOL` (default: `BTCUSDT`)
- Docker Compose uses `MARKET_SYMBOL`, mapped to `SYMBOL`
- Symbols are normalized by removing separators (`-`, `_`, `/`, spaces) and uppercasing

## Topic naming

Legacy BTC topics (when symbol is BTCUSDT/BTCUSD):

- `binance_btcusd`, `binance_volume`, `binance_orderbook`
- `bybit_btcusd`, `bybit_volume`, `bybit_orderbook`
- `bitget_btcusd`, `bitget_volume`, `bitget_orderbook`
- `okx_btcusd`, `okx_volume`, `okx_orderbook`
- `coinbase_btcusd`, `coinbase_volume`, `coinbase_orderbook`
- `kraken_btcusd`, `kraken_volume`, `kraken_orderbook`
- `chainlink_btcusd`

Non-BTC symbols (example: ETHUSDT):

- `binance_ethusdt`, `binance_ethusdt_volume`, `binance_ethusdt_orderbook`
- `bybit_ethusdt`, `bybit_ethusdt_volume`, `bybit_ethusdt_orderbook`
- `bitget_ethusdt`, `bitget_ethusdt_volume`, `bitget_ethusdt_orderbook`
- `okx_ethusdt`, `okx_ethusdt_volume`, `okx_ethusdt_orderbook`
- `coinbase_ethusdt`, `coinbase_ethusdt_volume`, `coinbase_ethusdt_orderbook`
- `kraken_ethusdt`, `kraken_ethusdt_volume`, `kraken_ethusdt_orderbook`
- `chainlink_ethusdt`

All services accept `-topic` to override the default.

## Payload schemas

### Price topics

All price topics use:

```json
{ "T": 1765037935846, "p": "89921.57", "bb": "89921.56", "ba": "89921.58", "bq": "0.12", "aq": "0.08", "s": "BTCUSDT" }
```

Notes:

- `bb`/`ba`/`bq`/`aq` are empty strings when the exchange stream does not supply best bid/ask data.

### Volume topics

All volume topics emit per-second volume buckets:

```json
{ "T": 1765037919999, "v": "0.0445", "s": "BTCUSDT" }
```

`T` is the end-of-second timestamp in milliseconds.

### Orderbook topics

Orderbook messages always contain a full snapshot of the top-N levels:

```json
{
  "T": 1765037935846,
  "s": "BTCUSDT",
  "bb": "89921.56",
  "ba": "89921.58",
  "bq": "0.12",
  "aq": "0.08",
  "bids": [["89921.56","0.12"], ["89921.55","0.20"]],
  "asks": [["89921.58","0.08"], ["89921.59","0.14"]],
  "snapshot": true
}
```

Notes:

- `bids` are sorted best-to-worse (highest to lowest).
- `asks` are sorted best-to-worse (lowest to highest).
- `snapshot` is true for feed snapshots or REST fallback refreshes, false for deltas.
- WebSocket deltas are applied to an in-memory book; every update emits a full top-N snapshot.
- If no WebSocket update arrives for ~2 seconds, a REST snapshot is fetched every second until WS traffic resumes.

Depth options by exchange:

- Binance (USD-M futures): `-depth` 5, 10, 20
- Bybit: `-depth` 1, 50, 200
- Bitget: `-depth` 5, 15 (or 0 for full if supported)
- OKX: `-depth` 5, 50 (or 0 for full if supported)
- Coinbase: `-depth` controls output depth; WS is level2 updates
- Kraken: `-depth` supports exchange-defined values (e.g. 10/25/100/500/1000)

### Chainlink price topic

```json
{ "value": 89946.74897627215, "timestamp": 1765037614000, "s": "BTCUSDT" }
```

## Operational notes

- Docker Compose runs Redpanda + Redpanda Console at `http://localhost:8080`.
- REST fallback ensures orderbook snapshots continue during WS stalls.
