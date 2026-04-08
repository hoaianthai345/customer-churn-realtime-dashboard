# Realtime BI Demo (Kafka + Spark + ClickHouse + FastAPI + Vite React)

Near real-time BI pipeline where static datasets are preloaded into ClickHouse and historical `user_logs` are replayed into Kafka for realtime simulation, then Spark Structured Streaming computes KPIs, FastAPI serves APIs/WebSocket, and the Vite React dashboard visualizes results.

## 1) Prerequisites

- Docker and Docker Compose
- Python 3.10+
- `7z` command (optional, for extracting raw source files)

## 2) Project Flow

`Batch Preload (members + transactions) -> User-log Replay Producer -> Kafka -> Spark Structured Streaming -> ClickHouse -> FastAPI (REST + WebSocket) -> Vite React dashboard`

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
- Dashboard UI: `http://localhost:3000`

## 3.1) First Demo Run

Artifact-backed demo mode is the stable path for presentation and does not require Kafka, Spark, or ClickHouse to be live.

```bash
bash scripts/run_demo.sh
```

If the demo stack is already up and you only want a readiness check:

```bash
bash scripts/validate_demo.sh
```

Demo notes:

- fixed demo month: `2017-04`
- demo UI runs at `http://localhost:3000`
- demo API runs at `http://localhost:8000`
- replay is intentionally disabled in demo mode so the story stays deterministic

## 4) Main Scripts

- `scripts/setup_local.sh`: Install Python deps and prepare data folders.
- `scripts/run_pipeline.sh`: Start infra, reuse cached cleaned data, ensure Spark jobs are running, and prepare history layer.
- `scripts/run_demo.sh`: Start the artifact-backed demo stack and validate that it is presentation-ready.
- `scripts/validate_demo.sh`: Verify API, frontend, and frontend proxy for the demo stack.
- `scripts/stop_pipeline.sh`: Stop all containers.
- `scripts/reset_pipeline.sh`: Stop and delete containers/volumes/checkpoints.
- `scripts/validate_stack.sh`: Basic health checks for Kafka, ClickHouse, FastAPI, and the frontend dashboard.

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

## 9) Docs Structure

- `docs/README.md`: entry point and reading order for all documentation.
- `docs/system_description/project_desc.md`: product scope, maturity, and conflicts to keep in mind.
- `docs/system_description/team_coding_guide.md`: coding and architecture rules for contributors.
- `docs/system_description/tab1_data_strategy.md`: data design for Tab 1 with precompute history + realtime materialization.
- `docs/system_description/kkbox_feature_catalog.md`: canonical feature semantics for the KKBOX batch feature store.
- `docs/system_description/kkbox_tab2_predictive_pipeline.md`: train/score contract and artifact outputs for Tab 2.
- `docs/system_description/predictive.md`: grounded business narrative for Tab 2.
- `docs/system_description/prescriptive.md`: grounded business narrative for Tab 3.
- `docs/architecture_diagrams/architecture.md`: end-to-end runtime architecture.
- `docs/report_and_slides/kkbox_pipeline_descriptions.md`: pipeline descriptions for report diagrams.
- `docs/report_and_slides/kkbox_report_diagrams.md`: diagram inventory for report writing and slide presentation.
- `docs/report_and_slides/demo_script.md`: presentation script and operator checklist for the first artifact-backed demo.
# customer-churn-realtime-dashboard
