# KPI Definitions

## kpi_revenue

- `total_revenue`: sum of `actual_amount_paid` by `transaction_date`
- `total_transactions`: count of rows by `transaction_date`
- `cancel_count`: count where `is_cancel = 1`
- `auto_renew_count`: count where `is_auto_renew = 1`

## kpi_activity

- `active_users`: distinct `msno` by `date`
- `total_listening_secs`: sum of `total_secs` by `date`
- `avg_unique_songs`: average `num_unq` by `date`

## kpi_churn_risk

Rule-based risk score:

`risk_score = 0.4 * is_cancel + 0.3 * (1 - is_auto_renew) + 0.3 * low_activity_flag`

- `low_activity_flag = 1` if `total_secs < LOW_ACTIVITY_THRESHOLD_SECS`, else `0`
- `high_risk_users`: count where `risk_score >= CHURN_HIGH_RISK_THRESHOLD`
- `avg_risk_score`: average risk score by `event_date`
