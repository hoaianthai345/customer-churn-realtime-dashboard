import pytest

pytest.importorskip("pyspark")

from apps.streaming.common import schemas


def test_transaction_schema_fields():
    names = [field.name for field in schemas.transaction_event_schema.fields]
    assert names == [
        "msno",
        "payment_method_id",
        "payment_plan_days",
        "plan_list_price",
        "actual_amount_paid",
        "is_auto_renew",
        "transaction_date",
        "membership_expire_date",
        "is_cancel",
    ]


def test_user_log_schema_fields():
    names = [field.name for field in schemas.user_log_event_schema.fields]
    assert names == [
        "msno",
        "date",
        "num_25",
        "num_50",
        "num_75",
        "num_985",
        "num_100",
        "num_unq",
        "total_secs",
    ]
