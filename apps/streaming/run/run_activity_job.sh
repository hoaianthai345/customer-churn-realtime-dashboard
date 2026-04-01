#!/usr/bin/env bash
set -euo pipefail

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.clickhouse:clickhouse-jdbc:0.6.2,org.apache.httpcomponents.client5:httpclient5:5.2.1,org.apache.httpcomponents.core5:httpcore5:5.2.4,org.apache.httpcomponents.core5:httpcore5-h2:5.2.4,org.slf4j:slf4j-simple:2.0.13"

export PYTHONPATH="/opt/project:${PYTHONPATH:-}"

/opt/spark/bin/spark-submit \
  --packages "${PACKAGES}" \
  apps/streaming/jobs/activity_kpi_job.py
