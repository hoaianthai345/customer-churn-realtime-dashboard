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
from apps.streaming.common.schemas import transaction_event_schema
from apps.streaming.common.spark_session import create_spark_session
from apps.streaming.common.transforms import parse_kafka_json

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def load_activity_snapshot(spark) -> DataFrame:
    query = """
      (SELECT
          msno,
          log_date AS event_date,
          sum(total_secs) AS total_secs
       FROM fact_user_logs_rt
       GROUP BY msno, log_date) activity
    """
    return (
        spark.read.format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("driver", CLICKHOUSE_DRIVER)
        .option("dbtable", query)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .load()
        .withColumn("event_date", F.to_date("event_date"))
    )


def main() -> None:
    spark = create_spark_session("churn-risk-job")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("TOPIC_TRANSACTION_EVENTS", "transaction_events")
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

    parsed_df = parse_kafka_json(source_df, transaction_event_schema, value_alias="transaction")

    def write_churn_kpi(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        tx_features = batch_df.select(
            "msno",
            F.col("transaction_date").alias("event_date"),
            F.col("is_cancel").cast("double").alias("is_cancel"),
            F.col("is_auto_renew").cast("double").alias("is_auto_renew"),
        )

        activity_snapshot = load_activity_snapshot(spark)

        feature_df = (
            tx_features.join(activity_snapshot, on=["msno", "event_date"], how="left")
            .fillna({"total_secs": 0.0})
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
