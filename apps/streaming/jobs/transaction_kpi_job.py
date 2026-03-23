#!/usr/bin/env python3
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame

from apps.streaming.common.checkpointing import checkpoint_path
from apps.streaming.common.clickhouse_writer import write_batch_to_clickhouse
from apps.streaming.common.schemas import transaction_event_schema
from apps.streaming.common.spark_session import create_spark_session
from apps.streaming.common.transforms import parse_kafka_json, prepare_transaction_fact, transaction_kpi_from_batch

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def main() -> None:
    spark = create_spark_session("transaction-kpi-job")

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("TOPIC_TRANSACTION_EVENTS", "transaction_events")
    trigger_interval = os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds")

    source_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = parse_kafka_json(source_df, transaction_event_schema, value_alias="transaction")
    fact_df = prepare_transaction_fact(parsed_df)

    def write_fact(batch_df: DataFrame, batch_id: int) -> None:
        write_batch_to_clickhouse(batch_df, batch_id, "fact_transactions_rt")

    def write_kpi(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        kpi_df = transaction_kpi_from_batch(batch_df)
        write_batch_to_clickhouse(kpi_df, batch_id, "kpi_revenue")

    fact_query = (
        fact_df.writeStream.outputMode("append")
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_path("transaction_fact"))
        .foreachBatch(write_fact)
        .start()
    )

    kpi_query = (
        parsed_df.writeStream.outputMode("append")
        .trigger(processingTime=trigger_interval)
        .option("checkpointLocation", checkpoint_path("transaction_kpi"))
        .foreachBatch(write_kpi)
        .start()
    )

    spark.streams.awaitAnyTermination()
    fact_query.stop()
    kpi_query.stop()


if __name__ == "__main__":
    main()
