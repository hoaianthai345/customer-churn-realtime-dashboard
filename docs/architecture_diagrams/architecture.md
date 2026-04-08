# Architecture

## End-to-End Flow

1. Batch CSV files are cleaned into `data/processed`.
2. Static datasets (`members`, `transactions`) are loaded once into ClickHouse.
3. User-log replay producer publishes `user_log_events` to Kafka.
4. Spark Structured Streaming consumes user-log events.
5. Spark writes user-log fact rows and realtime KPIs into ClickHouse.
6. Batch precompute materializes Tab 1 historical base (`history_precompute`, pre-2017).
7. Replay + Spark update realtime log-driven tables from March 2017 onward.
8. Batch materializer refreshes Tab 1 realtime base (`realtime_2017_plus`) from fact tables.
9. FastAPI reads KPI and Tab 1 materialized tables, exposing REST + websocket endpoints.
10. The frontend dashboard consumes API endpoints and renders near real-time dashboards.

## Components

- Kafka: event transport layer.
- Spark Structured Streaming: transformation and KPI computation.
- ClickHouse: serving layer for low-latency analytics.
- Batch materializers: prepare Tab 1 descriptive datasets for low-cost querying.
- FastAPI: backend API and push channel (WebSocket).
- Frontend dashboard: interactive dashboard UI and live update rendering.

## Topics

- `member_events`
- `user_log_events`
- `churn_label_events` (optional)
