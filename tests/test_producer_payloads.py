from apps.producers.common.serializers import member_payload, transaction_payload, user_log_payload


def test_member_payload_defaults_gender():
    payload = member_payload(
        {
            "msno": "u1",
            "city": "1",
            "bd": "0",
            "gender": "",
            "registered_via": "9",
            "registration_init_time": "2011-01-01",
        }
    )
    assert payload["msno"] == "u1"
    assert payload["gender"] == "unknown"


def test_transaction_payload_types():
    payload = transaction_payload(
        {
            "msno": "u1",
            "payment_method_id": "41",
            "payment_plan_days": "30",
            "plan_list_price": "149",
            "actual_amount_paid": "149",
            "is_auto_renew": "1",
            "transaction_date": "2017-03-01",
            "membership_expire_date": "2017-04-01",
            "is_cancel": "0",
        }
    )
    assert isinstance(payload["payment_plan_days"], int)
    assert isinstance(payload["actual_amount_paid"], float)


def test_user_log_payload_types():
    payload = user_log_payload(
        {
            "msno": "u1",
            "date": "2017-03-01",
            "num_25": "1",
            "num_50": "2",
            "num_75": "3",
            "num_985": "4",
            "num_100": "5",
            "num_unq": "10",
            "total_secs": "90.5",
        }
    )
    assert payload["num_unq"] == 10
    assert payload["total_secs"] == 90.5
