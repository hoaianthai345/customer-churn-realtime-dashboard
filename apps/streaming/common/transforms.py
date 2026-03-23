from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def parse_kafka_json(source_df: DataFrame, schema: StructType, value_alias: str = "payload") -> DataFrame:
    return (
        source_df.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), schema).alias(value_alias), F.col("kafka_key"))
        .select(f"{value_alias}.*", "kafka_key")
    )


def prepare_transaction_fact(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("processed_at", F.current_timestamp())
        .withColumn("is_auto_renew", F.col("is_auto_renew").cast("int"))
        .withColumn("is_cancel", F.col("is_cancel").cast("int"))
        .drop("kafka_key")
    )


def prepare_user_log_fact(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", F.current_timestamp()).withColumnRenamed("date", "log_date").drop("kafka_key")


def transaction_kpi_from_batch(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(F.col("transaction_date").alias("event_date"))
        .agg(
            F.sum(F.col("actual_amount_paid")).alias("total_revenue"),
            F.count(F.lit(1)).alias("total_transactions"),
            F.sum(F.when(F.col("is_cancel") == 1, F.lit(1)).otherwise(F.lit(0))).alias("cancel_count"),
            F.sum(F.when(F.col("is_auto_renew") == 1, F.lit(1)).otherwise(F.lit(0))).alias("auto_renew_count"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def activity_kpi_from_batch(df: DataFrame) -> DataFrame:
    return (
        df.groupBy(F.col("date").alias("event_date"))
        .agg(
            F.countDistinct("msno").alias("active_users"),
            F.sum(F.col("total_secs")).alias("total_listening_secs"),
            F.avg(F.col("num_unq")).alias("avg_unique_songs"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )
