from apps.producers.common.utils import filter_batches_from_start_date


def test_filter_batches_from_start_date():
    batches = [
        ("2016-12-31", [{"msno": "u1"}]),
        ("2017-01-01", [{"msno": "u2"}]),
        ("2017-01-05", [{"msno": "u3"}]),
    ]
    filtered = filter_batches_from_start_date(batches, "2017-01-01")
    assert [event_date for event_date, _ in filtered] == ["2017-01-01", "2017-01-05"]


def test_filter_batches_from_start_date_invalid_start():
    batches = [
        ("2016-12-31", [{"msno": "u1"}]),
        ("2017-01-01", [{"msno": "u2"}]),
    ]
    filtered = filter_batches_from_start_date(batches, "2017/01/01")
    assert filtered == batches
