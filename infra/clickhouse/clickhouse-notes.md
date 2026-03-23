# ClickHouse Notes

- Database: `realtime_bi`
- Init SQL is mounted from `infra/clickhouse/init`.
- KPI tables use `ReplacingMergeTree(processed_at)` to keep latest micro-batch result per day.

Useful checks:

```sql
SELECT count(*) FROM realtime_bi.fact_transactions_rt;
SELECT * FROM realtime_bi.kpi_revenue ORDER BY event_date DESC LIMIT 20;
```
