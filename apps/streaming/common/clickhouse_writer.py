import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


CLICKHOUSE_JDBC_URL = os.getenv("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://localhost:8123/realtime_bi")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DRIVER = os.getenv("CLICKHOUSE_JDBC_DRIVER", "com.clickhouse.jdbc.ClickHouseDriver")


def write_df_to_clickhouse(df: DataFrame, table_name: str, mode: str = "append") -> None:
    (
        df.write.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("dbtable", table_name)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .mode(mode)
        .save()
    )


def write_batch_to_clickhouse(batch_df: DataFrame, _batch_id: int, table_name: str) -> None:
    if batch_df.rdd.isEmpty():
        return
    write_df_to_clickhouse(batch_df, table_name, mode="append")
