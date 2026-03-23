# Architecture

## End-to-End Flow

1. Batch CSV files are cleaned into `data/processed`.
2. Replay producers publish processed rows to Kafka topics.
3. Spark Structured Streaming consumes Kafka events.
4. Spark writes raw events into ClickHouse fact tables.
5. Spark computes daily KPIs and writes serving tables.
6. Superset reads KPI tables for near real-time dashboards.

## Components

- Kafka: event transport layer.
- Spark Structured Streaming: transformation and KPI computation.
- ClickHouse: serving layer for low-latency analytics.
- Superset: dashboard and BI exploration.

## Topics

- `member_events`
- `transaction_events`
- `user_log_events`
- `churn_label_events` (optional)
