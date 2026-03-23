#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC_MEMBER_EVENTS="${TOPIC_MEMBER_EVENTS:-member_events}"
TOPIC_TRANSACTION_EVENTS="${TOPIC_TRANSACTION_EVENTS:-transaction_events}"
TOPIC_USER_LOG_EVENTS="${TOPIC_USER_LOG_EVENTS:-user_log_events}"
TOPIC_CHURN_LABEL_EVENTS="${TOPIC_CHURN_LABEL_EVENTS:-churn_label_events}"

create_topic() {
  local topic="$1"
  echo "Ensuring topic exists: ${topic}"
  kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --replication-factor 1 \
    --partitions 3
}

create_topic "${TOPIC_MEMBER_EVENTS}"
create_topic "${TOPIC_TRANSACTION_EVENTS}"
create_topic "${TOPIC_USER_LOG_EVENTS}"
create_topic "${TOPIC_CHURN_LABEL_EVENTS}"

echo "Kafka topics are ready."
