import pytest

pytest.importorskip("pyspark")

from apps.streaming.common import clickhouse_writer


def test_clickhouse_writer_defaults_exist():
    assert clickhouse_writer.CLICKHOUSE_JDBC_URL.startswith("jdbc:clickhouse://")
    assert clickhouse_writer.CLICKHOUSE_DRIVER == "com.clickhouse.jdbc.ClickHouseDriver"
