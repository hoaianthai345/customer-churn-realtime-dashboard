import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def create_spark_session(app_name: str) -> SparkSession:
    spark_master = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
