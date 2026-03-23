# AI Agent Guide

This file mirrors the implementation prompt in `implementation_guide.md` section 13.

## Objective

Build a near real-time BI demo pipeline:

`CSV Replay -> Kafka -> Spark Structured Streaming -> ClickHouse -> Superset`

## Required Outputs

- Docker Compose stack for Kafka, Spark, ClickHouse, Superset
- ClickHouse init SQL tables (`dim_members`, `fact_*`, `kpi_*`)
- Batch cleaning scripts
- Replay producer scripts (2-second replay by historical day)
- Spark streaming jobs for revenue/activity/churn-risk KPIs
- Shell scripts for setup/run/stop/reset/validate
- Project docs for architecture, events, KPIs, demo flow

## Churn Risk Formula

`risk_score = 0.4 * is_cancel + 0.3 * (1 - is_auto_renew) + 0.3 * low_activity_flag`

`low_activity_flag = 1 if total_secs < LOW_ACTIVITY_THRESHOLD_SECS else 0`
