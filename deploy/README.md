# Deployment (rsync + docker compose)

This setup syncs the repo to the `am` host and runs all crawlers via a single
profile-based Docker Compose file.

## Sync to the host

```bash
./deploy/rsync-am.sh
```

Override the destination if needed:

```bash
./deploy/rsync-am.sh am:/home/g/polymarket-services/pm-market-data/
```

## Run crawlers per symbol

Start:

```bash
ssh am 'cd /home/g/polymarket-services/pm-market-data && ./deploy/run-crawlers.sh BTCUSDT up'
```

Stop followers:

```bash
ssh am 'cd /home/g/polymarket-services/pm-market-data && ./deploy/run-crawlers.sh BTCUSDT down'
```

Stop followers and NATS (infra):

```bash
ssh am 'cd /home/g/polymarket-services/pm-market-data && STOP_NATS=1 ./deploy/run-crawlers.sh BTCUSDT down'
```

Notes:
- `run-crawlers.sh` ensures the shared `pm-net` network exists.
- NATS runs once in the `pm-market-data-infra` project; followers run per-symbol projects.

## Env

Place a `.env` file in the repo root on the host (Docker Compose loads it automatically). Example:

```bash
ORDERBOOK_OUTPUT=features
COINBASE_API_KEY=
COINBASE_API_SECRET=
COINBASE_API_PASSPHRASE=
```
