#!/usr/bin/env bash
set -euo pipefail

bash apps/streaming/run/run_activity_job.sh &
ACT_PID=$!

bash apps/streaming/run/run_churn_risk_job.sh &
CHURN_PID=$!

wait "${ACT_PID}" "${CHURN_PID}"
