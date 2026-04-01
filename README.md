# Realtime BI Demo (Kafka + Spark + ClickHouse + FastAPI + Next.js)

Near real-time BI pipeline where static datasets are preloaded into ClickHouse and historical `user_logs` are replayed into Kafka for realtime simulation, then Spark Structured Streaming computes KPIs, FastAPI serves APIs/WebSocket, and Next.js visualizes results.

## 1) Prerequisites

- Docker and Docker Compose
- Python 3.10+
- `7z` command (optional, for extracting raw source files)

## 2) Project Flow

`Batch Preload (members + transactions) -> User-log Replay Producer -> Kafka -> Spark Structured Streaming -> ClickHouse -> FastAPI (REST + WebSocket) -> Next.js`

## 3) Quick Start

```bash
bash scripts/setup_local.sh
bash scripts/run_pipeline.sh
```

Then open dashboard and click `Replay user logs from 2017-03-01` when you want to start simulation.

Open services:

- Kafka broker: `localhost:29092`
- ClickHouse HTTP: `localhost:8123`
- FastAPI docs endpoint root: `http://localhost:8000/health`
- Next.js dashboard: `http://localhost:3000`

## 4) Main Scripts

- `scripts/setup_local.sh`: Install Python deps and prepare data folders.
- `scripts/run_pipeline.sh`: Start infra, reuse cached cleaned data, ensure Spark jobs are running, and prepare history layer.
- `scripts/stop_pipeline.sh`: Stop all containers.
- `scripts/reset_pipeline.sh`: Stop and delete containers/volumes/checkpoints.
- `scripts/validate_stack.sh`: Basic health checks for Kafka, ClickHouse, FastAPI, Next.js.

## 5) Producer Commands

```bash
python3 -m apps.batch.bootstrap_transactions
python3 -m apps.producers.replay_user_logs
```

Run all:

```bash
python3 -m apps.producers.run_all_producers
```

## 6) Spark Jobs

Execute inside Spark container:

```bash
docker compose exec spark bash apps/streaming/run/run_activity_job.sh
docker compose exec spark bash apps/streaming/run/run_churn_risk_job.sh
```

Or run all:

```bash
docker compose exec spark bash apps/streaming/run/run_all_jobs.sh
```

## 7) API Endpoints

- `GET /health`
- `GET /api/v1/month-options`
- `GET /api/v1/dashboard/snapshot?year=YYYY&month=MM`
- `GET /api/v1/tab1/month-options`
- `GET /api/v1/tab1/descriptive?year=YYYY&month=MM&dimension=age|gender|txn_freq|skip_ratio`
- `GET /api/v1/replay/status`
- `POST /api/v1/replay/start?force_reset=true&replay_start_date=2017-03-01`
- `WS /ws/kpi?year=YYYY&month=MM`

## 8) Notes

- This is a near real-time simulation (`1 historical day ~= 2 seconds`).
- For a faster local demo, use a small data subset in `data/sample` and point producers to it.
- Local pipeline defaults to replay from `2017-03-01` (`REPLAY_START_DATE`), triggered by Replay button or replay API.
- Tab 1 descriptive data is split into two layers:
  - `history_precompute`: prebuilt once from `TAB1_PRECOMPUTE_START_DATE` to `TAB1_REALTIME_START_DATE`.
  - `realtime_2017_plus`: rebuilt from ClickHouse realtime fact tables after replay.
- Materialized table: `realtime_bi.tab1_descriptive_member_monthly`.
- Replay is disabled by default in startup for faster app boot.
- To auto replay on startup (optional):
  - `HOST_AUTO_REPLAY=1 bash scripts/run_pipeline.sh`

## 9) Team Coding Guide

- `docs/team_coding_guide.md`: Coding and architecture rules for contributors (event-driven, multi-node, testing checklist).
- `docs/tab1_data_strategy.md`: Data design for Tab 1 with precompute history + realtime materialization.
# customer-churn-realtime-dashboard
