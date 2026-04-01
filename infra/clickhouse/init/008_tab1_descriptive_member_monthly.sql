CREATE TABLE IF NOT EXISTS realtime_bi.tab1_descriptive_member_monthly (
  snapshot_month Date,
  msno String,
  last_expire_date Date,
  churned UInt8,
  is_auto_renew UInt8,
  survival_days Int32,
  age_bucket LowCardinality(String),
  gender_bucket LowCardinality(String),
  txn_freq_bucket LowCardinality(String),
  skip_ratio_bucket LowCardinality(String),
  price_segment LowCardinality(String),
  loyalty_segment LowCardinality(String),
  active_segment LowCardinality(String),
  discovery_ratio Float64,
  skip_ratio Float64,
  source LowCardinality(String),
  processed_at DateTime
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(snapshot_month)
ORDER BY (snapshot_month, msno, source);
