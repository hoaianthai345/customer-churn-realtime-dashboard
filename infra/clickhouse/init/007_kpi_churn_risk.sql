CREATE TABLE IF NOT EXISTS realtime_bi.kpi_churn_risk (
  event_date Date,
  high_risk_users UInt64,
  avg_risk_score Float64,
  processed_at DateTime
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date);
