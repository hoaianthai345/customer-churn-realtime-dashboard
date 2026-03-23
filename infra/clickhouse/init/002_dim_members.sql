CREATE TABLE IF NOT EXISTS realtime_bi.dim_members (
  msno String,
  city Nullable(Int32),
  bd Nullable(Int32),
  gender String,
  registered_via Nullable(Int32),
  registration_init_time Date
)
ENGINE = MergeTree
ORDER BY (msno);
