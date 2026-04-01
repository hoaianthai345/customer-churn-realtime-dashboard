#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

DEFAULT_PROJECT_NAME="$(basename "${ROOT_DIR}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${DEFAULT_PROJECT_NAME}}"
export HOST_KAFKA_BOOTSTRAP_SERVERS="${HOST_KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"
export HOST_CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST:-localhost}"
export HOST_CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT:-8123}"
export HOST_REPLAY_START_DATE="${HOST_REPLAY_START_DATE:-2017-03-01}"
export HOST_REPLAY_MAX_DAYS="${HOST_REPLAY_MAX_DAYS:-90}"
export HOST_SKIP_TAB1_HISTORY_PRECOMPUTE="${HOST_SKIP_TAB1_HISTORY_PRECOMPUTE:-0}"
export HOST_FORCE_PREPROCESS="${HOST_FORCE_PREPROCESS:-0}"
export HOST_FORCE_BOOTSTRAP_MEMBERS="${HOST_FORCE_BOOTSTRAP_MEMBERS:-0}"
export HOST_FORCE_BOOTSTRAP_TRANSACTIONS="${HOST_FORCE_BOOTSTRAP_TRANSACTIONS:-0}"
export HOST_AUTO_REPLAY="${HOST_AUTO_REPLAY:-0}"
export HOST_REPLAY_FORCE_RESET="${HOST_REPLAY_FORCE_RESET:-1}"

processed_files=(
  "data/processed/members_clean.csv"
  "data/processed/transactions_clean.csv"
  "data/processed/user_logs_clean.csv"
  "data/processed/train_clean.csv"
)

processed_ready=1
for file_path in "${processed_files[@]}"; do
  if [[ ! -s "${file_path}" ]]; then
    processed_ready=0
    break
  fi
done

echo "Starting infrastructure services..."
docker compose up -d zookeeper kafka clickhouse spark api web

echo "Waiting for Kafka and ClickHouse to warm up..."
sleep 8

echo "Waiting for Kafka broker to be ready..."
kafka_ready=0
for attempt in $(seq 1 30); do
  if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
    kafka_ready=1
    break
  fi
  sleep 2
done

if [[ "${kafka_ready}" != "1" ]]; then
  echo "Kafka is not ready after waiting. Check container logs: docker compose logs kafka" >&2
  exit 1
fi

echo "Waiting for ClickHouse to be ready..."
clickhouse_ready=0
for attempt in $(seq 1 30); do
  if docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
    clickhouse_ready=1
    break
  fi
  sleep 2
done

if [[ "${clickhouse_ready}" != "1" ]]; then
  echo "ClickHouse is not ready after waiting. Check container logs: docker compose logs clickhouse" >&2
  exit 1
fi

echo "Creating Kafka topics..."
docker compose exec -T kafka bash -lc 'if [ -f /opt/project/infra/kafka/create_topics.sh ]; then bash /opt/project/infra/kafka/create_topics.sh; else kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic transaction_events --replication-factor 1 --partitions 3; kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic user_log_events --replication-factor 1 --partitions 3; kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic member_events --replication-factor 1 --partitions 3; fi'

echo "Verifying required Kafka topics..."
required_topics=(
  "${TOPIC_MEMBER_EVENTS:-member_events}"
  "${TOPIC_TRANSACTION_EVENTS:-transaction_events}"
  "${TOPIC_USER_LOG_EVENTS:-user_log_events}"
  "${TOPIC_CHURN_LABEL_EVENTS:-churn_label_events}"
)

for topic in "${required_topics[@]}"; do
  if ! docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic "${topic}" >/dev/null 2>&1; then
    echo "Missing Kafka topic after create step: ${topic}" >&2
    echo "Kafka topic bootstrap failed. Check container logs: docker compose logs kafka" >&2
    exit 1
  fi
done

if [[ "${HOST_FORCE_PREPROCESS}" == "1" || "${processed_ready}" == "0" ]]; then
  echo "Running preprocessing scripts..."
  python3 -m apps.batch.clean_members
  python3 -m apps.batch.clean_transactions
  python3 -m apps.batch.clean_user_logs
  python3 -m apps.batch.clean_train
else
  echo "Using cached processed CSV files (skip preprocess)."
fi

dim_member_count="$(docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM realtime_bi.dim_members" 2>/dev/null | tr -d '\r' | tr -d '\n' || true)"
if [[ -z "${dim_member_count}" ]]; then
  dim_member_count="0"
fi

if [[ "${HOST_FORCE_BOOTSTRAP_MEMBERS}" == "1" || "${dim_member_count}" == "0" ]]; then
  echo "Bootstrapping member dimension to ClickHouse..."
  CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST}" \
  CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT}" \
  KAFKA_BOOTSTRAP_SERVERS="${HOST_KAFKA_BOOTSTRAP_SERVERS}" \
  python3 -m apps.producers.bootstrap_members
else
  echo "dim_members already has ${dim_member_count} rows (skip bootstrap)."
fi

fact_transaction_count="$(docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM realtime_bi.fact_transactions_rt" 2>/dev/null | tr -d '\r' | tr -d '\n' || true)"
if [[ -z "${fact_transaction_count}" ]]; then
  fact_transaction_count="0"
fi

kpi_revenue_count="$(docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM realtime_bi.kpi_revenue" 2>/dev/null | tr -d '\r' | tr -d '\n' || true)"
if [[ -z "${kpi_revenue_count}" ]]; then
  kpi_revenue_count="0"
fi

if [[ "${HOST_FORCE_BOOTSTRAP_TRANSACTIONS}" == "1" ]]; then
  echo "Force bootstrapping transactions + revenue KPI to ClickHouse..."
  CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST}" \
  CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT}" \
  python3 -m apps.batch.bootstrap_transactions --force
elif [[ "${fact_transaction_count}" == "0" ]]; then
  echo "Bootstrapping transactions + revenue KPI to ClickHouse..."
  CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST}" \
  CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT}" \
  python3 -m apps.batch.bootstrap_transactions
elif [[ "${kpi_revenue_count}" == "0" ]]; then
  echo "fact_transactions_rt already has data; rebuilding missing kpi_revenue..."
  CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST}" \
  CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT}" \
  python3 -m apps.batch.bootstrap_transactions --rebuild-kpi-only
else
  echo "fact_transactions_rt already has ${fact_transaction_count} rows and kpi_revenue has ${kpi_revenue_count} rows (skip bootstrap)."
fi

if [[ "${HOST_SKIP_TAB1_HISTORY_PRECOMPUTE}" != "1" ]]; then
  echo "Precomputing Tab 1 history dataset (before realtime boundary)..."
  CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST}" \
  CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT}" \
  python3 -m apps.batch.precompute_tab1_history
else
  echo "Skipping Tab 1 history precompute."
fi

start_spark_job_if_missing() {
  local process_pattern="$1"
  local run_cmd="$2"
  local job_label="$3"
  if docker compose exec -T spark bash -lc "pgrep -f '${process_pattern}' >/dev/null"; then
    echo "${job_label} already running"
    return
  fi

  docker compose exec -d spark bash -lc "nohup ${run_cmd} >/tmp/${job_label}.log 2>&1 &"
  sleep 2

  if docker compose exec -T spark bash -lc "pgrep -f '${process_pattern}' >/dev/null"; then
    echo "started ${job_label}"
    return
  fi

  echo "Failed to start ${job_label}. Recent log:" >&2
  docker compose exec -T spark bash -lc "tail -n 120 /tmp/${job_label}.log || true" >&2
  exit 1
}

echo "Ensuring Spark streaming jobs are running..."
start_spark_job_if_missing "[c]hurn_risk_job.py" "bash apps/streaming/run/run_churn_risk_job.sh" "churn_risk_job"
start_spark_job_if_missing "[a]ctivity_kpi_job.py" "bash apps/streaming/run/run_activity_job.sh" "activity_kpi_job"

sleep 8

if [[ "${HOST_AUTO_REPLAY}" == "1" ]]; then
  echo "Running user-log replay from ${HOST_REPLAY_START_DATE} (HOST_AUTO_REPLAY=1)..."
  if [[ "${HOST_REPLAY_FORCE_RESET}" == "1" ]]; then
    echo "Resetting realtime serving tables before replay..."
    docker compose exec -T clickhouse clickhouse-client --query "TRUNCATE TABLE IF EXISTS realtime_bi.fact_user_logs_rt"
    docker compose exec -T clickhouse clickhouse-client --query "TRUNCATE TABLE IF EXISTS realtime_bi.kpi_activity"
    docker compose exec -T clickhouse clickhouse-client --query "TRUNCATE TABLE IF EXISTS realtime_bi.kpi_churn_risk"
    docker compose exec -T clickhouse clickhouse-client --query "ALTER TABLE realtime_bi.tab1_descriptive_member_monthly DELETE WHERE source = 'realtime_2017_plus'" || true
  fi

  KAFKA_BOOTSTRAP_SERVERS="${HOST_KAFKA_BOOTSTRAP_SERVERS}" REPLAY_START_DATE="${HOST_REPLAY_START_DATE}" MAX_REPLAY_DAYS="${HOST_REPLAY_MAX_DAYS}" python3 -m apps.producers.replay_user_logs

  echo "Waiting for replayed user logs to be ingested into ClickHouse..."
  logs_ready=0
  for attempt in $(seq 1 60); do
    log_count="$(docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM realtime_bi.fact_user_logs_rt WHERE log_date >= toDate('${HOST_REPLAY_START_DATE}')" 2>/dev/null | tr -d '\r' | tr -d '\n' || true)"
    if [[ -n "${log_count}" && "${log_count}" != "0" ]]; then
      logs_ready=1
      echo "fact_user_logs_rt rows from ${HOST_REPLAY_START_DATE}: ${log_count}"
      break
    fi
    sleep 2
  done
  if [[ "${logs_ready}" != "1" ]]; then
    echo "No replayed user logs ingested yet from ${HOST_REPLAY_START_DATE}; skip realtime materialization for now."
    exit 1
  fi

  CLICKHOUSE_HOST="${HOST_CLICKHOUSE_HOST}" \
  CLICKHOUSE_HTTP_PORT="${HOST_CLICKHOUSE_HTTP_PORT}" \
  python3 -m apps.batch.materialize_tab1_realtime --force
else
  echo "Auto replay disabled. Use Replay button on web (or POST /api/v1/replay/start)."
fi

echo "Stack is ready."
echo "Dashboard URL: http://localhost:3000"
echo "API URL: http://localhost:8000/health"
