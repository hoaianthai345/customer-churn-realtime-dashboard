# Superset Notes

1. Open `http://localhost:8088`.
2. Login with credentials in `.env`.
3. Add ClickHouse connection with SQLAlchemy URI:

```text
clickhouse+http://default:@clickhouse:8123/realtime_bi
```

4. Create datasets from:
- `kpi_revenue`
- `kpi_activity`
- `kpi_churn_risk`
