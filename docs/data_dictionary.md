# Data Dictionary

## members_v3

- `msno` (String): user id
- `city` (Int): city id
- `bd` (Int): age-like field from source
- `gender` (String): user gender
- `registered_via` (Int): registration channel
- `registration_init_time` (Date): registration date

## transactions_v2

- `msno` (String)
- `payment_method_id` (Int)
- `payment_plan_days` (Int)
- `plan_list_price` (Float)
- `actual_amount_paid` (Float)
- `is_auto_renew` (UInt8)
- `transaction_date` (Date)
- `membership_expire_date` (Date)
- `is_cancel` (UInt8)

## user_logs_v2

- `msno` (String)
- `date` (Date)
- `num_25`, `num_50`, `num_75`, `num_985`, `num_100` (Int)
- `num_unq` (Int)
- `total_secs` (Float)

## train_v2

- `msno` (String)
- `is_churn` (0 or 1)
