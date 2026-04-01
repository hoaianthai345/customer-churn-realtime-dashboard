from datetime import date

from apps.producers.bootstrap_members import _parse_member_date


def test_parse_member_date_iso():
    assert _parse_member_date("2017-03-01") == date(2017, 3, 1)


def test_parse_member_date_yyyymmdd():
    assert _parse_member_date("20170301") == date(2017, 3, 1)


def test_parse_member_date_invalid():
    assert _parse_member_date("2017/03/01") is None
