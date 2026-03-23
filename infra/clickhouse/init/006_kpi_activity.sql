CREATE TABLE IF NOT EXISTS realtime_bi.kpi_activity (
  event_date Date,
  active_users UInt64,
  total_listening_secs Float64,
  avg_unique_songs Float64,
  processed_at DateTime
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date);
