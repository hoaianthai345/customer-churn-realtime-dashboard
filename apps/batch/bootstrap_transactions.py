#!/usr/bin/env python3
import argparse
import csv
import http.client  # noqa: F401
import logging
import os
from datetime import date, datetime
from typing import Optional, Tuple

import clickhouse_connect

from apps.producers.common.config import get_settings
from apps.producers.common.serializers import transaction_payload

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FACT_COLUMNS = [
    "msno",
    "payment_method_id",
    "payment_plan_days",
    "plan_list_price",
    "actual_amount_paid",
    "is_auto_renew",
    "transaction_date",
    "membership_expire_date",
    "is_cancel",
    "processed_at",
]


def _parse_date(value: str) -> Optional[date]:
    text = (value or "").strip()
    if not text:
        return None
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


def _connect_clickhouse():
    settings = get_settings()
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def _table_count(client, table_name: str) -> int:
    result = client.query(f"SELECT count() FROM realtime_bi.{table_name}")
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def _load_fact_transactions(client, chunk_size: int) -> Tuple[int, int]:
    settings = get_settings()
    source_path = settings.transactions_clean_path
    now_ts = datetime.utcnow().replace(microsecond=0)
    inserted = 0
    dropped = 0
    chunk = []

    logger.info("Loading transactions from %s ...", source_path)
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            payload = transaction_payload(row)
            if not payload["msno"]:
                dropped += 1
                continue

            tx_date = _parse_date(payload.get("transaction_date") or "")
            expire_date = _parse_date(payload.get("membership_expire_date") or "")
            if tx_date is None or expire_date is None:
                dropped += 1
                continue

            chunk.append(
                [
                    payload["msno"],
                    int(payload.get("payment_method_id") or 0),
                    int(payload.get("payment_plan_days") or 0),
                    float(payload.get("plan_list_price") or 0.0),
                    float(payload.get("actual_amount_paid") or 0.0),
                    int(payload.get("is_auto_renew") or 0),
                    tx_date,
                    expire_date,
                    int(payload.get("is_cancel") or 0),
                    now_ts,
                ]
            )

            if len(chunk) >= chunk_size:
                client.insert(table="fact_transactions_rt", data=chunk, column_names=FACT_COLUMNS)
                inserted += len(chunk)
                logger.info("Inserted %s transaction rows ...", inserted)
                chunk = []

    if chunk:
        client.insert(table="fact_transactions_rt", data=chunk, column_names=FACT_COLUMNS)
        inserted += len(chunk)

    logger.info("Transactions load completed. inserted=%s dropped=%s", inserted, dropped)
    return inserted, dropped


def _rebuild_revenue_kpi(client) -> int:
    client.command("TRUNCATE TABLE IF EXISTS realtime_bi.kpi_revenue")
    client.command(
        """
        INSERT INTO realtime_bi.kpi_revenue
        (
          event_date,
          total_revenue,
          total_transactions,
          cancel_count,
          auto_renew_count,
          processed_at
        )
        SELECT
          transaction_date AS event_date,
          sum(actual_amount_paid) AS total_revenue,
          count() AS total_transactions,
          sum(if(is_cancel = 1, 1, 0)) AS cancel_count,
          sum(if(is_auto_renew = 1, 1, 0)) AS auto_renew_count,
          now() AS processed_at
        FROM realtime_bi.fact_transactions_rt
        GROUP BY event_date
        """
    )
    return _table_count(client, "kpi_revenue")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap transactions and revenue KPI into ClickHouse")
    parser.add_argument("--force", action="store_true", help="Truncate fact_transactions_rt before loading CSV")
    parser.add_argument(
        "--rebuild-kpi-only",
        action="store_true",
        help="Skip fact load and rebuild kpi_revenue from current fact_transactions_rt",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=int(os.getenv("TX_BOOTSTRAP_CHUNK_SIZE", "50000")),
        help="Rows per ClickHouse insert batch",
    )
    args = parser.parse_args()

    if args.force and args.rebuild_kpi_only:
        raise ValueError("--force and --rebuild-kpi-only cannot be used together")
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be > 0")

    client = _connect_clickhouse()

    if args.force:
        logger.info("Force mode: truncating fact_transactions_rt ...")
        client.command("TRUNCATE TABLE IF EXISTS realtime_bi.fact_transactions_rt")

    if not args.rebuild_kpi_only:
        existing = _table_count(client, "fact_transactions_rt")
        if existing > 0 and not args.force:
            logger.info(
                "fact_transactions_rt already has %s rows, skip fact load (use --force to reload).",
                existing,
            )
        else:
            _load_fact_transactions(client, chunk_size=args.chunk_size)
    else:
        logger.info("Skipping fact load (--rebuild-kpi-only).")

    kpi_rows = _rebuild_revenue_kpi(client)
    logger.info("Rebuilt kpi_revenue: %s rows", kpi_rows)


if __name__ == "__main__":
    main()
