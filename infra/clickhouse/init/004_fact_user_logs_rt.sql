CREATE TABLE IF NOT EXISTS realtime_bi.fact_user_logs_rt (
  msno String,
  log_date Date,
  num_25 Int32,
  num_50 Int32,
  num_75 Int32,
  num_985 Int32,
  num_100 Int32,
  num_unq Int32,
  total_secs Float64,
  processed_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(log_date)
ORDER BY (log_date, msno);
