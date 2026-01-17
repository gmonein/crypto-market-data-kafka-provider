# AWS deployment (docker-compose + systemd)

This setup runs Redpanda separately from the market-data followers so you can scale symbols via systemd.

## Prereqs

- Docker Engine + docker compose plugin
- Repo cloned on the host (examples assume `/opt/pm-market-data`)

## Optional env file

Create `/etc/pm-market-data/pm-market-data.env` for shared environment values:

```bash
COINBASE_API_KEY=
COINBASE_API_SECRET=
COINBASE_API_PASSPHRASE=
```

## Install systemd units

```bash
sudo cp deploy/systemd/pm-market-data-kafka.service /etc/systemd/system/
sudo cp deploy/systemd/pm-market-data@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## Start Redpanda

```bash
sudo systemctl enable --now pm-market-data-kafka.service
```

## Start followers per symbol

```bash
sudo systemctl enable --now pm-market-data@BTCUSDT
sudo systemctl enable --now pm-market-data@ETHUSDT
sudo systemctl enable --now pm-market-data@SOLUSDT
sudo systemctl enable --now pm-market-data@XRPUSDT
```

The unit runs `deploy/create-topics.sh` for each symbol before starting containers.

## Logs

```bash
journalctl -u pm-market-data@BTCUSDT -f
```

## Notes

- If you install the repo elsewhere, update `WorkingDirectory` and paths in `deploy/systemd/*.service`.
- You can create topics manually with:

```bash
./deploy/create-topics.sh BTCUSDT ETHUSDT SOLUSDT XRPUSDT
```
