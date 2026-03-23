#!/usr/bin/env python3
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

from apps.streaming.common.clickhouse_writer import write_df_to_clickhouse
from apps.streaming.common.spark_session import create_spark_session

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def _members_csv_path() -> str:
    processed_dir = os.getenv("PROCESSED_DATA_DIR", f"{PROJECT_ROOT}/data/processed")
    filename = os.getenv("MEMBERS_CLEAN_FILE", "members_clean.csv")
    path = Path(processed_dir) / filename
    if str(path).startswith("/opt/project"):
        local_path = Path(str(path).replace("/opt/project/", f"{PROJECT_ROOT}/", 1))
        if local_path.exists():
            return str(local_path)
    return str(path)


def main() -> None:
    spark = create_spark_session("member-bootstrap-job")

    members_df = (
        spark.read.option("header", True)
        .csv(_members_csv_path())
        .withColumn("city", F.col("city").cast("int"))
        .withColumn("bd", F.col("bd").cast("int"))
        .withColumn("registered_via", F.col("registered_via").cast("int"))
        .withColumn("registration_init_time", F.to_date("registration_init_time"))
    )

    write_df_to_clickhouse(members_df, "dim_members", mode="append")
    spark.stop()


if __name__ == "__main__":
    main()
