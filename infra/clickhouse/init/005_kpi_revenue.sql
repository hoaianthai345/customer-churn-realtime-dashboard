CREATE TABLE IF NOT EXISTS realtime_bi.kpi_revenue (
  event_date Date,
  total_revenue Float64,
  total_transactions UInt64,
  cancel_count UInt64,
  auto_renew_count UInt64,
  processed_at DateTime
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date);
