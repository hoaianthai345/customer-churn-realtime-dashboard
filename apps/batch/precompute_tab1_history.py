#!/usr/bin/env python3
import argparse
import http.client  # noqa: F401
import logging
import os
from datetime import date, datetime, timezone
from typing import Dict, List, Set, Tuple

import clickhouse_connect
import numpy as np
import pandas as pd

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

INSERT_COLUMNS = [
    "snapshot_month",
    "msno",
    "last_expire_date",
    "churned",
    "is_auto_renew",
    "survival_days",
    "age_bucket",
    "gender_bucket",
    "txn_freq_bucket",
    "skip_ratio_bucket",
    "price_segment",
    "loyalty_segment",
    "active_segment",
    "discovery_ratio",
    "skip_ratio",
    "source",
    "processed_at",
]


def _parse_date(value: str, env_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date for {env_name}: {value}. Expected YYYY-MM-DD") from exc


def _read_env_dates() -> Tuple[date, date]:
    start = _parse_date(os.getenv("TAB1_PRECOMPUTE_START_DATE", "2016-01-01"), "TAB1_PRECOMPUTE_START_DATE")
    realtime_start = _parse_date(os.getenv("TAB1_REALTIME_START_DATE", "2017-03-01"), "TAB1_REALTIME_START_DATE")
    if start >= realtime_start:
        raise ValueError("TAB1_PRECOMPUTE_START_DATE must be earlier than TAB1_REALTIME_START_DATE")
    return start, realtime_start


def _connect_clickhouse():
    settings = get_settings()
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )


def _bucket_age(series: pd.Series) -> pd.Series:
    valid = series.where((series >= 10) & (series <= 100))
    return pd.Series(
        np.select(
            [
                valid.le(20),
                valid.le(30),
                valid.le(40),
                valid.gt(40),
            ],
            [
                "15_20",
                "21_30",
                "31_40",
                "41_plus",
            ],
            default="unknown",
        ),
        index=series.index,
    )


def _bucket_gender(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.lower()
    return pd.Series(np.where(normalized.isin(["male", "female"]), normalized, "unknown"), index=series.index)


def _bucket_txn_freq(series: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [series.le(2), series.le(6)],
            ["low", "medium"],
            default="high",
        ),
        index=series.index,
    )


def _bucket_skip_ratio(series: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [series.lt(0.2), series.lt(0.5)],
            ["low_skip", "mid_skip"],
            default="high_skip",
        ),
        index=series.index,
    )


def _bucket_price(series: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [series.lt(4.5), series.lt(8.0)],
            ["deal_<4.5", "standard_4.5_8"],
            default="premium_>=8",
        ),
        index=series.index,
    )


def _bucket_loyalty(series: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [series.lt(90), series.lt(365)],
            ["new_<90d", "growing_90_365d"],
            default="loyal_>=365d",
        ),
        index=series.index,
    )


def _bucket_activity(series: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [series.lt(900), series.lt(3600)],
            ["inactive_<15m", "casual_15m_1h"],
            default="engaged_>=1h",
        ),
        index=series.index,
    )


def _load_tx_features(path: str, start: date, end: date) -> pd.DataFrame:
    tx = pd.read_csv(
        path,
        usecols=[
            "msno",
            "payment_plan_days",
            "plan_list_price",
            "actual_amount_paid",
            "is_auto_renew",
            "transaction_date",
            "membership_expire_date",
            "is_cancel",
        ],
        parse_dates=["transaction_date", "membership_expire_date"],
    )
    tx = tx.loc[
        (tx["membership_expire_date"] >= pd.Timestamp(start))
        & (tx["membership_expire_date"] < pd.Timestamp(end))
    ].copy()
    if tx.empty:
        return tx

    tx["snapshot_month"] = tx["membership_expire_date"].dt.to_period("M").dt.to_timestamp()
    tx["payment_plan_days"] = tx["payment_plan_days"].replace(0, np.nan)
    tx["price_per_day"] = (tx["plan_list_price"] / tx["payment_plan_days"]).replace([np.inf, -np.inf], np.nan)
    tx["price_per_day"] = tx["price_per_day"].fillna(tx["plan_list_price"])
    tx = tx.sort_values(["msno", "snapshot_month", "transaction_date", "membership_expire_date"])

    grouped = tx.groupby(["msno", "snapshot_month"], as_index=False).agg(
        tx_count=("msno", "size"),
        avg_price_per_day=("price_per_day", "mean"),
        first_tx_date=("transaction_date", "min"),
        last_tx_date=("transaction_date", "max"),
    )

    latest = tx.drop_duplicates(subset=["msno", "snapshot_month"], keep="last")[
        ["msno", "snapshot_month", "membership_expire_date", "is_cancel", "is_auto_renew"]
    ].rename(
        columns={
            "membership_expire_date": "last_expire_date",
            "is_cancel": "churned",
        }
    )

    features = grouped.merge(latest, on=["msno", "snapshot_month"], how="inner")
    return features


def _load_members(path: str, msnos: Set[str], chunk_size: int) -> pd.DataFrame:
    chunks: List[pd.DataFrame] = []
    for chunk in pd.read_csv(
        path,
        usecols=["msno", "bd", "gender", "registration_init_time"],
        chunksize=chunk_size,
    ):
        subset = chunk.loc[chunk["msno"].isin(msnos)].copy()
        if subset.empty:
            continue
        subset["registration_init_time"] = pd.to_datetime(subset["registration_init_time"], errors="coerce")
        chunks.append(subset)
    if not chunks:
        return pd.DataFrame(columns=["msno", "bd", "gender", "registration_init_time"])
    members = pd.concat(chunks, ignore_index=True)
    return members.drop_duplicates(subset=["msno"], keep="last")


def _load_log_features(path: str, msnos: Set[str], start: date, end: date, chunk_size: int) -> pd.DataFrame:
    chunks: List[pd.DataFrame] = []
    for chunk in pd.read_csv(
        path,
        usecols=[
            "msno",
            "date",
            "num_25",
            "num_50",
            "num_75",
            "num_985",
            "num_100",
            "num_unq",
            "total_secs",
        ],
        chunksize=chunk_size,
    ):
        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")
        subset = chunk.loc[
            (chunk["msno"].isin(msnos))
            & (chunk["date"] >= pd.Timestamp(start))
            & (chunk["date"] < pd.Timestamp(end))
        ].copy()
        if subset.empty:
            continue

        subset["skip_num"] = subset["num_25"] + subset["num_50"]
        subset["play_num"] = (
            subset["num_25"] + subset["num_50"] + subset["num_75"] + subset["num_985"] + subset["num_100"]
        )
        subset["disc_num"] = subset["num_unq"]
        subset["snapshot_month"] = subset["date"].dt.to_period("M").dt.to_timestamp()

        grouped = subset.groupby(["msno", "snapshot_month"], as_index=False).agg(
            skip_num=("skip_num", "sum"),
            play_num=("play_num", "sum"),
            disc_num=("disc_num", "sum"),
            total_secs=("total_secs", "sum"),
            active_days=("msno", "size"),
        )
        chunks.append(grouped)

    if not chunks:
        return pd.DataFrame(columns=["msno", "snapshot_month", "skip_num", "play_num", "disc_num", "total_secs", "active_days"])
    combined = pd.concat(chunks, ignore_index=True)
    return combined.groupby(["msno", "snapshot_month"], as_index=False).sum(numeric_only=True)


def _build_snapshot_rows(tx: pd.DataFrame, members: pd.DataFrame, logs: pd.DataFrame) -> pd.DataFrame:
    df = tx.merge(members, on="msno", how="left").merge(logs, on=["msno", "snapshot_month"], how="left")

    for col in ["last_expire_date", "registration_init_time", "first_tx_date", "snapshot_month"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["skip_num", "play_num", "disc_num", "total_secs", "active_days", "bd", "tx_count", "avg_price_per_day"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["skip_num", "play_num", "disc_num", "total_secs", "active_days"]:
        df[col] = df[col].fillna(0.0)

    reg_days = (df["last_expire_date"] - df["registration_init_time"]).dt.days
    fallback_days = (df["last_expire_date"] - df["first_tx_date"]).dt.days
    df["survival_days"] = reg_days.fillna(fallback_days).fillna(0).clip(lower=0).astype(int)

    df["skip_ratio"] = np.where(df["play_num"] > 0, df["skip_num"] / df["play_num"], 0.0)
    df["discovery_ratio"] = np.where(df["play_num"] > 0, df["disc_num"] / df["play_num"], 0.0)
    df["avg_daily_secs"] = np.where(df["active_days"] > 0, df["total_secs"] / df["active_days"], 0.0)

    df["age_bucket"] = _bucket_age(df["bd"])
    df["gender_bucket"] = _bucket_gender(df["gender"])
    df["txn_freq_bucket"] = _bucket_txn_freq(df["tx_count"])
    df["skip_ratio_bucket"] = _bucket_skip_ratio(df["skip_ratio"])
    df["price_segment"] = _bucket_price(df["avg_price_per_day"].fillna(0.0))
    df["loyalty_segment"] = _bucket_loyalty(df["survival_days"])
    df["active_segment"] = _bucket_activity(df["avg_daily_secs"])

    out = df[
        [
            "snapshot_month",
            "msno",
            "last_expire_date",
            "churned",
            "is_auto_renew",
            "survival_days",
            "age_bucket",
            "gender_bucket",
            "txn_freq_bucket",
            "skip_ratio_bucket",
            "price_segment",
            "loyalty_segment",
            "active_segment",
            "discovery_ratio",
            "skip_ratio",
        ]
    ].copy()
    out["snapshot_month"] = out["snapshot_month"].dt.date
    out["last_expire_date"] = out["last_expire_date"].dt.date
    out["churned"] = out["churned"].fillna(0).astype(int).clip(lower=0, upper=1)
    out["is_auto_renew"] = out["is_auto_renew"].fillna(0).astype(int).clip(lower=0, upper=1)
    out["source"] = "history_precompute"
    out["processed_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    out = out.dropna(subset=["snapshot_month", "last_expire_date"])
    out = out.drop_duplicates(subset=["snapshot_month", "msno"], keep="last")
    return out


def _existing_rows(client, start: date, end: date) -> int:
    sql = f"""
    SELECT count() AS row_count
    FROM realtime_bi.{TAB1_TABLE}
    WHERE source = 'history_precompute'
      AND snapshot_month >= toDate('{start.isoformat()}')
      AND snapshot_month < toDate('{end.isoformat()}')
    """
    result = client.query(sql)
    if not result.result_rows:
        return 0
    return int(result.result_rows[0][0])


def _upsert_rows(client, rows: pd.DataFrame, start: date, end: date, force: bool) -> None:
    existing = _existing_rows(client, start, end)
    if existing > 0 and not force:
        logger.info("History precompute already exists: %s rows in [%s, %s). Skip.", existing, start, end)
        return

    if existing > 0 and force:
        logger.info("Deleting existing history precompute rows in [%s, %s)...", start, end)
        delete_sql = f"""
        ALTER TABLE realtime_bi.{TAB1_TABLE}
        DELETE
        WHERE source = 'history_precompute'
          AND snapshot_month >= toDate('{start.isoformat()}')
          AND snapshot_month < toDate('{end.isoformat()}')
        """
        client.command(delete_sql, settings={"mutations_sync": 2})

    if rows.empty:
        logger.warning("No history rows to insert after feature engineering. Skip.")
        return

    payload = rows[INSERT_COLUMNS].values.tolist()
    client.insert(
        table=TAB1_TABLE,
        data=payload,
        column_names=INSERT_COLUMNS,
    )
    logger.info("Inserted %s history rows into realtime_bi.%s", len(payload), TAB1_TABLE)


def main() -> None:
    parser = argparse.ArgumentParser(description="Precompute Tab 1 descriptive base rows for history window")
    parser.add_argument("--force", action="store_true", help="Rebuild history rows in ClickHouse range")
    parser.add_argument("--chunk-size", type=int, default=500_000, help="Chunk size for CSV scanning")
    args = parser.parse_args()

    start, end = _read_env_dates()
    settings = get_settings()
    transactions_path = str(settings.transactions_clean_path)
    members_path = str(settings.members_clean_path)
    logs_path = str(settings.user_logs_clean_path)
    client = _connect_clickhouse()
    client.command(TAB1_TABLE_DDL)

    existing = _existing_rows(client, start, end)
    if existing > 0 and not args.force:
        logger.info("History precompute already exists: %s rows in [%s, %s). Skip heavy CSV scan.", existing, start, end)
        return

    logger.info("Precomputing Tab 1 history rows for [%s, %s)", start, end)
    tx_features = _load_tx_features(transactions_path, start, end)
    if tx_features.empty:
        logger.warning("No transactions found in history window. Nothing to materialize.")
        return

    msnos = set(tx_features["msno"].astype(str).tolist())
    logger.info("History candidate users: %s", len(msnos))

    members = _load_members(members_path, msnos, args.chunk_size)
    logs = _load_log_features(logs_path, msnos, start, end, args.chunk_size)
    snapshot_rows = _build_snapshot_rows(tx_features, members, logs)

    _upsert_rows(client, snapshot_rows, start, end, args.force)


if __name__ == "__main__":
    main()
