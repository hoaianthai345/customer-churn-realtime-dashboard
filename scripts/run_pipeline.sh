#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export COMPOSE_PROJECT_NAME="realtime-bi"

echo "Starting infrastructure services..."
docker compose up -d zookeeper kafka clickhouse spark superset

echo "Waiting for Kafka and ClickHouse to warm up..."
sleep 15

echo "Creating Kafka topics..."
docker compose exec -T kafka bash -lc 'if [ -f /opt/project/infra/kafka/create_topics.sh ]; then bash /opt/project/infra/kafka/create_topics.sh; else kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic transaction_events --replication-factor 1 --partitions 3; kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic user_log_events --replication-factor 1 --partitions 3; kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic member_events --replication-factor 1 --partitions 3; fi'

echo "Running preprocessing scripts..."
python3 apps/batch/clean_members.py
python3 apps/batch/clean_transactions.py
python3 apps/batch/clean_user_logs.py
python3 apps/batch/clean_train.py

echo "Bootstrapping member dimension to ClickHouse..."
python3 apps/producers/bootstrap_members.py

echo "Starting Spark streaming jobs (detached)..."
docker compose exec -d spark bash apps/streaming/run/run_transaction_job.sh
docker compose exec -d spark bash apps/streaming/run/run_activity_job.sh
docker compose exec -d spark bash apps/streaming/run/run_churn_risk_job.sh

sleep 8

echo "Running replay producers..."
python3 apps/producers/replay_transactions.py
python3 apps/producers/replay_user_logs.py

echo "Pipeline run finished."
