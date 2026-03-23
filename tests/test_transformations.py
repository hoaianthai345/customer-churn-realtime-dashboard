import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from apps.streaming.common.transforms import activity_kpi_from_batch, transaction_kpi_from_batch


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test-transformations").getOrCreate()
    yield session
    session.stop()


def test_transaction_kpi_from_batch(spark):
    data = [
        ("u1", 100.0, 0, 1, "2017-03-01"),
        ("u2", 50.0, 1, 0, "2017-03-01"),
    ]
    df = spark.createDataFrame(data, ["msno", "actual_amount_paid", "is_cancel", "is_auto_renew", "transaction_date"])
    out = transaction_kpi_from_batch(df).collect()[0].asDict()
    assert out["total_revenue"] == pytest.approx(150.0)
    assert out["total_transactions"] == 2
    assert out["cancel_count"] == 1
    assert out["auto_renew_count"] == 1


def test_activity_kpi_from_batch(spark):
    data = [
        ("u1", "2017-03-01", 100.0, 10),
        ("u2", "2017-03-01", 50.0, 30),
    ]
    df = spark.createDataFrame(data, ["msno", "date", "total_secs", "num_unq"])
    out = activity_kpi_from_batch(df).collect()[0].asDict()
    assert out["active_users"] == 2
    assert out["total_listening_secs"] == pytest.approx(150.0)
    assert out["avg_unique_songs"] == pytest.approx(20.0)
