# Dashboard Design

## Revenue Dashboard

- Line: `sum(total_revenue)` by `event_date`
- Line: `sum(total_transactions)` by `event_date`
- Bar: `sum(cancel_count)` by `event_date`
- Bar: `sum(auto_renew_count)` by `event_date`

## Activity Dashboard

- Line: `sum(active_users)` by `event_date`
- Line: `sum(total_listening_secs)` by `event_date`
- Line: `avg(avg_unique_songs)` by `event_date`

## Churn Risk Dashboard

- Line: `sum(high_risk_users)` by `event_date`
- Line: `avg(avg_risk_score)` by `event_date`
- Table: risk by city (optional extension from joined dataset)
- Table: risk by registration channel (optional extension)
