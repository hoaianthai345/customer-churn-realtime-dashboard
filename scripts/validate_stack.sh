#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export COMPOSE_PROJECT_NAME="realtime-bi"

echo "Container status:"
docker compose ps

echo "Kafka topics:"
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list

echo "ClickHouse tables:"
docker compose exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM realtime_bi"

echo "Sample KPI rows:"
docker compose exec -T clickhouse clickhouse-client --query "SELECT * FROM realtime_bi.kpi_revenue ORDER BY event_date DESC LIMIT 5"

echo "Superset health check:"
curl -fsS http://localhost:8088/health || true
