# Grafana Artifacts

This folder contains starter Grafana assets for monitoring the new timing metadata fields emitted by `pm-market-data`.

## Contents

- `dashboards/pm-market-data-latency.json`: dashboard panels for ingest lag and publish delay
- `alerts/pm-market-data-latency-rules.yaml`: Prometheus-style alert rules

## Assumptions

These assets assume you export/derive Prometheus metrics from emitted payload metadata:

- `pm_market_data_ingest_lag_ms`
- `pm_market_data_publish_delay_ms`

with labels such as `exchange`, `symbol`, and `stream`.

If your metric names differ, update panel queries and alert expressions accordingly.
