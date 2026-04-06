# Event Contracts

## Common

- Kafka key: `msno` as UTF-8 bytes.
- Kafka value: JSON payload encoded UTF-8.

## transaction_events payload

```json
{
  "msno": "user_001",
  "payment_method_id": 41,
  "payment_plan_days": 30,
  "plan_list_price": 149,
  "actual_amount_paid": 149,
  "is_auto_renew": 1,
  "transaction_date": "2017-03-23",
  "membership_expire_date": "2017-04-23",
  "is_cancel": 0
}
```

## user_log_events payload

```json
{
  "msno": "user_001",
  "date": "2017-03-31",
  "num_25": 3,
  "num_50": 0,
  "num_75": 1,
  "num_985": 1,
  "num_100": 181,
  "num_unq": 150,
  "total_secs": 46240.281
}
```

## member_events payload

```json
{
  "msno": "user_001",
  "city": 5,
  "bd": 19,
  "gender": "male",
  "registered_via": 9,
  "registration_init_time": "2011-09-17"
}
```
