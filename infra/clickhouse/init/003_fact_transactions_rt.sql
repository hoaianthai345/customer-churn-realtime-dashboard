CREATE TABLE IF NOT EXISTS realtime_bi.fact_transactions_rt (
  msno String,
  payment_method_id Int32,
  payment_plan_days Int32,
  plan_list_price Float64,
  actual_amount_paid Float64,
  is_auto_renew UInt8,
  transaction_date Date,
  membership_expire_date Date,
  is_cancel UInt8,
  processed_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_date)
ORDER BY (transaction_date, msno);
