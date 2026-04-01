#!/usr/bin/env python3
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from apps.streaming.common.checkpointing import checkpoint_path
from apps.streaming.common.clickhouse_writer import (
    CLICKHOUSE_DRIVER,
    CLICKHOUSE_JDBC_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    write_batch_to_clickhouse,
)
from apps.streaming.common.schemas import user_log_event_schema
from apps.streaming.common.spark_session import create_spark_session
from apps.streaming.common.transforms import parse_kafka_json

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def load_transaction_profile(spark) -> DataFrame:
    query = """
      (SELECT
          msno,
          toFloat64(argMax(is_cancel, transaction_date)) AS is_cancel,
          toFloat64(argMax(is_auto_renew, transaction_date)) AS is_auto_renew
       FROM fact_transactions_rt
       GROUP BY msno) tx_profile
    """
    return (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("dbtable", query)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .load()
    )


def main() -> None:
    spark = create_spark_session("churn-risk-job")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("TOPIC_USER_LOG_EVENTS", "user_log_events")
    trigger_interval = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")
    low_activity_threshold = float(os.getenv("LOW_ACTIVITY_THRESHOLD_SECS", "1800"))
    high_risk_threshold = float(os.getenv("CHURN_HIGH_RISK_THRESHOLD", "0.6"))

    source_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = parse_kafka_json(source_df, user_log_event_schema, value_alias="user_log")
    tx_profile_df = load_transaction_profile(spark).cache()
    _ = tx_profile_df.count()

    def write_churn_kpi(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        activity_features = (
            batch_df.groupBy("msno", F.col("date").alias("event_date"))
            .agg(F.sum(F.col("total_secs")).alias("total_secs"))
            .select("msno", "event_date", "total_secs")
        )

        feature_df = (
            activity_features.join(F.broadcast(tx_profile_df), on="msno", how="left")
            .fillna({"total_secs": 0.0, "is_cancel": 0.0, "is_auto_renew": 1.0})
            .withColumn(
                "low_activity_flag",
                F.when(F.col("total_secs") < F.lit(low_activity_threshold), F.lit(1.0)).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "risk_score",
                F.lit(0.4) * F.col("is_cancel")
                + F.lit(0.3) * (F.lit(1.0) - F.col("is_auto_renew"))
                + F.lit(0.3) * F.col("low_activity_flag"),
            )
        )

        kpi_df = (
            feature_df.groupBy("event_date")
            .agg(
                F.sum(F.when(F.col("risk_score") >= F.lit(high_risk_threshold), F.lit(1)).otherwise(F.lit(0))).alias(
                    "high_risk_users"
                ),
                F.avg("risk_score").alias("avg_risk_score"),
            )
            .withColumn("processed_at", F.current_timestamp())
            .select("event_date", "high_risk_users", "avg_risk_score", "processed_at")
        )

        write_batch_to_clickhouse(kpi_df, batch_id, "kpi_churn_risk")

    churn_query = (
        parsed_df.writeStream.outputMode("append")
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_path("churn_kpi"))
        .foreachBatch(write_churn_kpi)
        .start()
    )

    spark.streams.awaitAnyTermination()
    churn_query.stop()


if __name__ == "__main__":
    main()
