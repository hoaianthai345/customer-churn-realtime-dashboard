# Demo Script

1. Start stack:

```bash
bash scripts/setup_local.sh
bash scripts/run_pipeline.sh
```

2. Validate KPI tables:

```bash
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM realtime_bi.kpi_revenue ORDER BY event_date DESC LIMIT 10"
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM realtime_bi.kpi_activity ORDER BY event_date DESC LIMIT 10"
docker compose exec clickhouse clickhouse-client --query "SELECT * FROM realtime_bi.kpi_churn_risk ORDER BY event_date DESC LIMIT 10"
```

3. Open Superset at `http://localhost:8088`.
4. Connect ClickHouse datasource.
5. Build three dashboards: revenue, activity, churn risk.
