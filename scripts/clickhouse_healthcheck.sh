#!/usr/bin/env bash
# ClickHouse healthcheck — runs `SELECT 1` against the local cluster.
#
# Default targets the docker-compose service started from the repo root
# (``docker compose up -d clickhouse``). Override host / port / user /
# password / database via the same env vars the service reads.

set -euo pipefail

HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
PORT="${CLICKHOUSE_PORT:-9000}"
USER="${CLICKHOUSE_USER:-default}"
PASSWORD="${CLICKHOUSE_PASSWORD:-}"
DATABASE="${CLICKHOUSE_DATABASE:-market}"

if command -v clickhouse-client >/dev/null 2>&1; then
    exec clickhouse-client \
        --host "${HOST}" \
        --port "${PORT}" \
        --user "${USER}" \
        --password "${PASSWORD}" \
        --database "${DATABASE}" \
        --query "SELECT 1"
fi

if command -v docker >/dev/null 2>&1 && \
   docker compose ps clickhouse --status running 2>/dev/null | grep -q clickhouse; then
    exec docker compose exec -T clickhouse \
        clickhouse-client \
            --user "${USER}" \
            --password "${PASSWORD}" \
            --database "${DATABASE}" \
            --query "SELECT 1"
fi

echo "clickhouse-client not found and docker compose service is not running" >&2
exit 1
