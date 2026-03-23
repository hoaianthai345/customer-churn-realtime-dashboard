# Realtime BI Demo (Kafka + Spark + ClickHouse + Superset)

Near real-time BI pipeline that replays historical KKBox CSV data into Kafka, computes streaming KPIs with Spark Structured Streaming, stores serving tables in ClickHouse, and visualizes KPIs in Superset.

## 1) Prerequisites

- Docker and Docker Compose
- Python 3.10+
- `7z` command (optional, for extracting raw source files)

## 2) Project Flow

`CSV Replay Producers -> Kafka -> Spark Structured Streaming -> ClickHouse -> Superset`

## 3) Quick Start

```bash
bash scripts/setup_local.sh
bash scripts/run_pipeline.sh
```

Open services:

- Kafka broker: `localhost:29092`
- ClickHouse HTTP: `localhost:8123`
- Superset: `http://localhost:8088`

## 4) Main Scripts

- `scripts/setup_local.sh`: Install Python deps and prepare data folders.
- `scripts/run_pipeline.sh`: Start infra, create topics, run preprocessing, start Spark jobs, then producers.
- `scripts/stop_pipeline.sh`: Stop all containers.
- `scripts/reset_pipeline.sh`: Stop and delete containers/volumes/checkpoints.
- `scripts/validate_stack.sh`: Basic health checks for Kafka, ClickHouse, Superset.

## 5) Producer Commands

```bash
python3 apps/producers/replay_transactions.py
python3 apps/producers/replay_user_logs.py
```

Run all:

```bash
python3 apps/producers/run_all_producers.py
```

## 6) Spark Jobs

Execute inside Spark container:

```bash
docker compose exec spark bash apps/streaming/run/run_transaction_job.sh
docker compose exec spark bash apps/streaming/run/run_activity_job.sh
docker compose exec spark bash apps/streaming/run/run_churn_risk_job.sh
```

Or run all:

```bash
docker compose exec spark bash apps/streaming/run/run_all_jobs.sh
```

## 7) Notes

- This is a near real-time simulation (`1 historical day ~= 2 seconds`).
- For a faster local demo, use a small data subset in `data/sample` and point producers to it.
# customer-churn-realtime-dashboard
