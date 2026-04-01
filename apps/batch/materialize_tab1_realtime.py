#!/usr/bin/env python3
import argparse
import http.client  # noqa: F401
import logging
import os
from datetime import date, datetime
from typing import List

import clickhouse_connect

from apps.producers.common.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


TAB1_TABLE = "tab1_descriptive_member_monthly"
TAB1_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS realtime_bi.tab1_descriptive_member_monthly (
  snapshot_month Date,
  msno String,
  last_expire_date Date,
  churned UInt8,
  is_auto_renew UInt8,
  survival_days Int32,
  age_bucket LowCardinality(String),
  gender_bucket LowCardinality(String),
  txn_freq_bucket LowCardinality(String),
  skip_ratio_bucket LowCardinality(String),
  price_segment LowCardinality(String),
  loyalty_segment LowCardinality(String),
  active_segment LowCardinality(String),
  discovery_ratio Float64,
  skip_ratio Float64,
  source LowCardinality(String),
  processed_at DateTime
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(snapshot_month)
ORDER BY (snapshot_month, msno, source)
"""


def _parse_date(value: str, env_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date for {env_name}: {value}. Expected YYYY-MM-DD") from exc


def _connect_clickhouse():
    settings = get_settings()
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def _existing_rows(client, realtime_start: date) -> int:
    sql = f"""
    SELECT count()
    FROM realtime_bi.{TAB1_TABLE}
    WHERE source = 'realtime_2017_plus'
      AND snapshot_month >= toDate('{realtime_start.isoformat()}')
    """
    result = client.query(sql)
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def _delete_existing(client, realtime_start: date) -> None:
    delete_sql = f"""
    ALTER TABLE realtime_bi.{TAB1_TABLE}
    DELETE
    WHERE source = 'realtime_2017_plus'
      AND snapshot_month >= toDate('{realtime_start.isoformat()}')
    """
    client.command(delete_sql, settings={"mutations_sync": 2})


def _load_snapshot_months(client, realtime_start: date) -> List[date]:
    sql = f"""
    SELECT DISTINCT toStartOfMonth(log_date) AS snapshot_month
    FROM realtime_bi.fact_user_logs_rt
    WHERE log_date >= toDate('{realtime_start.isoformat()}')
    ORDER BY snapshot_month
    """
    result = client.query(sql)
    months = [row[0] for row in result.result_rows if row and row[0] is not None]
    if not months:
        logger.warning(
            "No snapshot month found in fact_user_logs_rt from %s. "
            "Run replay_user_logs before materializing realtime Tab 1.",
            realtime_start,
        )
    return months


def _insert_month_rows(client, realtime_start: date, snapshot_month: date) -> None:
    month_iso = snapshot_month.isoformat()
    insert_sql = f"""
    INSERT INTO realtime_bi.{TAB1_TABLE}
    (
      snapshot_month,
      msno,
      last_expire_date,
      churned,
      is_auto_renew,
      survival_days,
      age_bucket,
      gender_bucket,
      txn_freq_bucket,
      skip_ratio_bucket,
      price_segment,
      loyalty_segment,
      active_segment,
      discovery_ratio,
      skip_ratio,
      source,
      processed_at
    )
    WITH
    tx_last AS (
      SELECT
        msno,
        argMax(membership_expire_date, tuple(transaction_date, membership_expire_date)) AS last_expire_date,
        argMax(is_cancel, tuple(transaction_date, membership_expire_date)) AS churned,
        argMax(is_auto_renew, tuple(transaction_date, membership_expire_date)) AS is_auto_renew,
        count() AS tx_count,
        avg(plan_list_price / greatest(payment_plan_days, 1)) AS avg_price_per_day,
        min(transaction_date) AS first_tx_date
      FROM realtime_bi.fact_transactions_rt
      WHERE membership_expire_date >= toDate('{realtime_start.isoformat()}')
        AND toStartOfMonth(membership_expire_date) = toDate('{month_iso}')
      GROUP BY msno
    ),
    log_last AS (
      SELECT
        msno,
        sum(num_25 + num_50) AS skip_num,
        sum(num_25 + num_50 + num_75 + num_985 + num_100) AS play_num,
        sum(num_unq) AS disc_num,
        sum(total_secs) AS total_secs,
        count() AS active_days
      FROM realtime_bi.fact_user_logs_rt
      WHERE toStartOfMonth(log_date) = toDate('{month_iso}')
      GROUP BY msno
    )
    SELECT
      toDate('{month_iso}') AS snapshot_month,
      tx.msno AS msno,
      tx.last_expire_date AS last_expire_date,
      toUInt8(tx.churned) AS churned,
      toUInt8(tx.is_auto_renew) AS is_auto_renew,
      greatest(
        ifNull(
          dateDiff('day', dm.registration_init_time, tx.last_expire_date),
          dateDiff('day', tx.first_tx_date, tx.last_expire_date)
        ),
        0
      ) AS survival_days,
      multiIf(
        dm.bd >= 10 AND dm.bd <= 20, '15_20',
        dm.bd >= 21 AND dm.bd <= 30, '21_30',
        dm.bd >= 31 AND dm.bd <= 40, '31_40',
        dm.bd > 40 AND dm.bd <= 100, '41_plus',
        'unknown'
      ) AS age_bucket,
      if(lowerUTF8(dm.gender) IN ('male', 'female'), lowerUTF8(dm.gender), 'unknown') AS gender_bucket,
      multiIf(
        tx.tx_count <= 2, 'low',
        tx.tx_count <= 6, 'medium',
        'high'
      ) AS txn_freq_bucket,
      multiIf(
        ifNull(logs.play_num, 0) = 0, 'low_skip',
        (logs.skip_num / logs.play_num) < 0.2, 'low_skip',
        (logs.skip_num / logs.play_num) < 0.5, 'mid_skip',
        'high_skip'
      ) AS skip_ratio_bucket,
      multiIf(
        tx.avg_price_per_day < 4.5, 'deal_<4.5',
        tx.avg_price_per_day < 8.0, 'standard_4.5_8',
        'premium_>=8'
      ) AS price_segment,
      multiIf(
        greatest(
          ifNull(
            dateDiff('day', dm.registration_init_time, tx.last_expire_date),
            dateDiff('day', tx.first_tx_date, tx.last_expire_date)
          ),
          0
        ) < 90, 'new_<90d',
        greatest(
          ifNull(
            dateDiff('day', dm.registration_init_time, tx.last_expire_date),
            dateDiff('day', tx.first_tx_date, tx.last_expire_date)
          ),
          0
        ) < 365, 'growing_90_365d',
        'loyal_>=365d'
      ) AS loyalty_segment,
      multiIf(
        ifNull(logs.active_days, 0) = 0, 'inactive_<15m',
        (logs.total_secs / logs.active_days) < 900, 'inactive_<15m',
        (logs.total_secs / logs.active_days) < 3600, 'casual_15m_1h',
        'engaged_>=1h'
      ) AS active_segment,
      if(ifNull(logs.play_num, 0) > 0, logs.disc_num / logs.play_num, 0.0) AS discovery_ratio,
      if(ifNull(logs.play_num, 0) > 0, logs.skip_num / logs.play_num, 0.0) AS skip_ratio,
      'realtime_2017_plus' AS source,
      now() AS processed_at
    FROM tx_last tx
    LEFT JOIN realtime_bi.dim_members dm USING (msno)
    LEFT JOIN log_last logs USING (msno)
    """
    client.command(insert_sql)


def _insert_realtime_rows(client, realtime_start: date) -> int:
    months = _load_snapshot_months(client, realtime_start)
    if not months:
        return 0

    for snapshot_month in months:
        logger.info("Materializing realtime snapshot month: %s", snapshot_month)
        _insert_month_rows(client, realtime_start, snapshot_month)

    return len(months)


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize Tab 1 realtime rows from ClickHouse facts")
    parser.add_argument("--force", action="store_true", help="Delete existing realtime rows and rebuild")
    args = parser.parse_args()

    realtime_start = _parse_date(os.getenv("TAB1_REALTIME_START_DATE", "2017-03-01"), "TAB1_REALTIME_START_DATE")
    client = _connect_clickhouse()
    client.command(TAB1_TABLE_DDL)

    existing = _existing_rows(client, realtime_start)
    if existing > 0 and not args.force:
        logger.info(
            "Realtime rows already exist: %s rows with source=realtime_2017_plus from %s. Skip.",
            existing,
            realtime_start,
        )
        return

    if existing > 0 and args.force:
        logger.info("Deleting existing realtime rows from %s...", realtime_start)
        _delete_existing(client, realtime_start)

    logger.info("Materializing Tab 1 realtime rows from facts (realtime_start=%s)...", realtime_start)
    months_loaded = _insert_realtime_rows(client, realtime_start)
    inserted = _existing_rows(client, realtime_start)
    logger.info("Tab 1 realtime materialization completed: %s rows across %s months", inserted, months_loaded)


if __name__ == "__main__":
    main()
