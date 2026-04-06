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

3. Check API quickly:

```bash
curl http://localhost:8000/health
curl "http://localhost:8000/api/v1/month-options"
curl "http://localhost:8000/api/v1/tab1/month-options"
curl "http://localhost:8000/api/v1/tab1/descriptive?year=2017&month=3&dimension=age"
curl "http://localhost:8000/api/v1/replay/status"
```

4. Open dashboard at `http://localhost:3000`.
5. Use the global slicer (month/year by last_expire_date).
6. Bấm nút `Replay user logs from 2017-03-01`.
7. Validate live refresh via WebSocket status badge + Replay status.
