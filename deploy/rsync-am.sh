#!/usr/bin/env sh
set -eu

remote_host="${1:-am}"
remote_path="${2:-/home/g/polymarket-services/pm-market-data/}"
symbols="${SYMBOLS_OVERRIDE:-BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT}"
action="${CRAWLER_ACTION:-restart}"

repo_dir=$(CDPATH= cd "$(dirname "$0")/.." && pwd)

ssh "$remote_host" "mkdir -p '$remote_path'"

rsync -az --delete \
  --exclude ".git/" \
  --exclude ".env" \
  --exclude "redpanda-data/" \
  --exclude "deploy/vpn/" \
  --exclude "*.log" \
  "$repo_dir/" "${remote_host}:${remote_path}"

ssh "$remote_host" "cd '$remote_path' && \
  if [ ! -f .env ]; then touch .env; fi && \
  if ! docker network inspect pm-net >/dev/null 2>&1; then docker network create pm-net >/dev/null; fi && \
  for sym in \$(printf '%s' \"${symbols}\" | tr ',' ' '); do \
    ./deploy/run-crawlers.sh \"\${sym}\" \"${action}\"; \
  done"
