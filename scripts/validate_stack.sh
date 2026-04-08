#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

DEFAULT_PROJECT_NAME="$(basename "${ROOT_DIR}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${DEFAULT_PROJECT_NAME}}"

echo "Container status:"
docker compose ps

echo "Kafka topics:"
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list

echo "ClickHouse tables:"
docker compose exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM realtime_bi"

echo "Sample KPI rows:"
docker compose exec -T clickhouse clickhouse-client --query "SELECT * FROM realtime_bi.kpi_revenue ORDER BY event_date DESC LIMIT 5"

echo "Tab 1 dataset rows by source:"
if docker compose exec -T clickhouse clickhouse-client --query "EXISTS TABLE realtime_bi.tab1_descriptive_member_monthly" | grep -q "^1$"; then
  docker compose exec -T clickhouse clickhouse-client --query "SELECT source, count() FROM realtime_bi.tab1_descriptive_member_monthly GROUP BY source ORDER BY source"
else
  echo "tab1_descriptive_member_monthly is not created yet."
fi

echo "FastAPI health check:"
curl -fsS http://localhost:8000/health || true

echo
echo "FastAPI month options:"
curl -fsS http://localhost:8000/api/v1/month-options || true

echo
echo "FastAPI Tab1 month options:"
curl -fsS http://localhost:8000/api/v1/tab1/month-options || true

echo
echo "Replay status:"
curl -fsS http://localhost:8000/api/v1/replay/status || true

echo
echo "Frontend dashboard health check:"
curl -fsSI http://localhost:3000 | head -n 1 || true
