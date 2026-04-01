from datetime import date

import pandas as pd
import pytest

from apps.api_fastapi.main import _km_points_from_rows, _tab1_segment_filter_clause, _validate_iso_date
from apps.batch.precompute_tab1_history import _build_snapshot_rows, _bucket_age, _bucket_price, _load_tx_features


def test_bucket_age_ranges():
    series = pd.Series([18, 25, 35, 50, 0, 120])
    buckets = _bucket_age(series).tolist()
    assert buckets == ["15_20", "21_30", "31_40", "41_plus", "unknown", "unknown"]


def test_bucket_price_ranges():
    series = pd.Series([3.9, 6.2, 12.0])
    buckets = _bucket_price(series).tolist()
    assert buckets == ["deal_<4.5", "standard_4.5_8", "premium_>=8"]


def test_km_points_basic_curve():
    rows = [
        ("group_a", 10, 1, 2),  # 2 events at day 10
        ("group_a", 20, 0, 1),  # 1 censored at day 20
        ("group_a", 30, 1, 1),  # 1 event at day 30
    ]
    curve = _km_points_from_rows(rows)
    assert len(curve) == 1
    points = curve[0]["points"]
    assert points[0]["survival_prob"] == 1.0
    assert points[1]["day"] == 10
    assert points[1]["survival_prob"] == 0.5
    assert points[3]["day"] == 30
    assert points[3]["survival_prob"] == 0.0


def test_segment_filter_requires_both_values():
    with pytest.raises(ValueError):
        _tab1_segment_filter_clause("price_segment", None)


def test_validate_iso_date():
    assert _validate_iso_date("2017-01-01") == "2017-01-01"
    with pytest.raises(ValueError):
        _validate_iso_date("2017/01/01")


def test_build_snapshot_rows_merges_logs_by_month():
    tx = pd.DataFrame(
        [
            {
                "snapshot_month": pd.Timestamp("2017-01-01"),
                "msno": "u1",
                "last_expire_date": pd.Timestamp("2017-01-31"),
                "churned": 0,
                "is_auto_renew": 1,
                "tx_count": 2,
                "avg_price_per_day": 5.0,
                "first_tx_date": pd.Timestamp("2016-12-15"),
                "last_tx_date": pd.Timestamp("2017-01-20"),
            },
            {
                "snapshot_month": pd.Timestamp("2017-02-01"),
                "msno": "u1",
                "last_expire_date": pd.Timestamp("2017-02-28"),
                "churned": 1,
                "is_auto_renew": 0,
                "tx_count": 1,
                "avg_price_per_day": 3.5,
                "first_tx_date": pd.Timestamp("2016-12-15"),
                "last_tx_date": pd.Timestamp("2017-02-14"),
            },
        ]
    )
    members = pd.DataFrame(
        [
            {
                "msno": "u1",
                "bd": 25,
                "gender": "male",
                "registration_init_time": pd.Timestamp("2016-01-01"),
            }
        ]
    )
    logs = pd.DataFrame(
        [
            {
                "msno": "u1",
                "snapshot_month": pd.Timestamp("2017-01-01"),
                "skip_num": 2.0,
                "play_num": 10.0,
                "disc_num": 5.0,
                "total_secs": 2000.0,
                "active_days": 2.0,
            },
            {
                "msno": "u1",
                "snapshot_month": pd.Timestamp("2017-02-01"),
                "skip_num": 8.0,
                "play_num": 10.0,
                "disc_num": 2.0,
                "total_secs": 300.0,
                "active_days": 1.0,
            },
        ]
    )

    out = _build_snapshot_rows(tx, members, logs)
    assert len(out) == 2

    jan_row = out.loc[out["snapshot_month"] == pd.Timestamp("2017-01-01").date()].iloc[0]
    feb_row = out.loc[out["snapshot_month"] == pd.Timestamp("2017-02-01").date()].iloc[0]

    assert jan_row["skip_ratio_bucket"] == "mid_skip"
    assert feb_row["skip_ratio_bucket"] == "high_skip"
    assert jan_row["active_segment"] == "casual_15m_1h"
    assert feb_row["active_segment"] == "inactive_<15m"


def test_load_tx_features_keeps_monthly_grain(tmp_path):
    csv_path = tmp_path / "tx.csv"
    csv_path.write_text(
        "\n".join(
            [
                "msno,payment_plan_days,plan_list_price,actual_amount_paid,is_auto_renew,transaction_date,membership_expire_date,is_cancel",
                "u1,30,149,149,1,2016-12-25,2017-01-05,0",
                "u1,30,149,149,1,2017-01-25,2017-02-05,1",
                "u2,30,149,149,0,2017-01-20,2017-02-18,0",
            ]
        ),
        encoding="utf-8",
    )

    features = _load_tx_features(str(csv_path), date(2016, 1, 1), date(2017, 3, 1))
    assert len(features) == 3
    assert features["snapshot_month"].nunique() == 2
    assert sorted(features["snapshot_month"].dt.strftime("%Y-%m").unique().tolist()) == ["2017-01", "2017-02"]
