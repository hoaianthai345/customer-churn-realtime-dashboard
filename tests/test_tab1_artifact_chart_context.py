from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from apps.api_fastapi.artifact_backend import build_tab1_descriptive_payload


TARGET_MONTH = 201704


def _write_required_tab1_artifacts(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(json.dumps({"artifact_type": "tab1_descriptive"}), encoding="utf-8")

    pd.DataFrame(
        [
            {
                "msno": "u1",
                "age_segment": "25-34",
                "price_segment": "Standard 4.5-6.5",
                "loyalty_segment": "Loyal 365d+",
                "active_segment": "Active 6-15 logs",
                "discovery_ratio": 0.4,
                "skip_ratio": 0.2,
                "is_churn": 0,
                "churn_rate": 0.0,
                "is_auto_renew": 1,
                "expected_renewal_amount": 100.0,
            },
            {
                "msno": "u2",
                "age_segment": "35-44",
                "price_segment": "Free Trial Zero Pay",
                "loyalty_segment": "New < 30d",
                "active_segment": "Light 1-5 logs",
                "discovery_ratio": 0.3,
                "skip_ratio": 0.6,
                "is_churn": 0,
                "churn_rate": 0.0,
                "is_auto_renew": 0,
                "expected_renewal_amount": 50.0,
            },
        ]
    ).to_parquet(root / "tab1_snapshot_201704.parquet", index=False)

    pd.DataFrame(
        [
            {"target_month": 201703, "historical_churn_rate": 4.5, "total_expiring_users": 100, "median_survival_days_proxy": 600.0, "auto_renew_rate": 88.0, "total_expected_renewal_amount": 10000.0},
            {"target_month": 201704, "historical_churn_rate": 2.0, "total_expiring_users": 2, "median_survival_days_proxy": 610.0, "auto_renew_rate": 50.0, "total_expected_renewal_amount": 150.0},
        ]
    ).to_parquet(root / "tab1_kpis_monthly.parquet", index=False)

    pd.DataFrame(columns=["target_month", "dimension", "dimension_value", "day", "survival_prob", "at_risk", "events"]).to_parquet(
        root / "tab1_km_curves.parquet",
        index=False,
    )
    pd.DataFrame(columns=["segment_type", "segment_value", "users", "churn_rate_pct", "retain_rate_pct"]).to_parquet(
        root / "tab1_segment_mix.parquet",
        index=False,
    )
    pd.DataFrame(columns=["discovery_ratio", "skip_ratio", "users", "churn_rate_pct"]).to_parquet(
        root / "tab1_boredom_scatter.parquet",
        index=False,
    )

    pd.DataFrame(
        [
            {
                "target_month": 201703,
                "month_label": "2017-03",
                "total_expiring_users": 100,
                "historical_churn_rate": 10.0,
                "overall_median_survival": 600.0,
                "auto_renew_rate": 88.0,
                "total_expected_renewal_amount": 10000.0,
                "historical_revenue_at_risk": 1000.0,
                "apru": 100.0,
                "new_paid_users": 10,
                "churned_users": 10,
                "net_movement": 0,
            }
        ]
    ).to_parquet(root / "trend_monthly_summary.parquet", index=False)

    pd.DataFrame(
        [
            {"target_month": 201703, "value_tier": "Free Trial", "risk_customer_segment": "At Risk", "subscribers": 5},
            {"target_month": 201704, "value_tier": "Free Trial", "risk_customer_segment": "At Risk", "subscribers": 1},
        ]
    ).to_parquet(root / "snapshot_risk_heatmap_all.parquet", index=False)


@pytest.fixture()
def tab1_artifact_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "artifacts_tab1_descriptive"
    _write_required_tab1_artifacts(root)
    monkeypatch.setenv("TAB1_ARTIFACTS_DIR", str(root))
    return root


def test_tab1_payload_uses_latest_available_artifact_month_for_chart_context(tab1_artifact_root: Path) -> None:
    payload = build_tab1_descriptive_payload(date(2017, 4, 1), prefer_cache=False)

    assert payload["meta"]["churn_breakdown_month"] == "2017-03"
    assert payload["churn_breakdown"]["churned_users"] == 10
    assert payload["churn_breakdown"]["renewed_users"] == 90

    assert payload["meta"]["risk_heatmap_month"] == "2017-03"
    at_risk_cell = next(
        row for row in payload["risk_heatmap"] if row["value_tier"] == "Free Trial" and row["risk_segment"] == "At Risk"
    )
    assert at_risk_cell["users"] == 5


def test_tab1_cached_payload_is_rehydrated_with_chart_context_month(tab1_artifact_root: Path) -> None:
    cache_dir = tab1_artifact_root / "payload_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "tab1_descriptive_201704__dim-age__seg-none__val-none.json"
    cache_file.write_text(
        json.dumps(
            {
                "meta": {
                    "month": "2017-04",
                    "dimension": "age_segment",
                    "segment_filter": {"segment_type": None, "segment_value": None},
                    "trend_scope": "overall",
                    "previous_month": "2017-03",
                    "artifact_mode": "artifact_backed",
                    "artifact_dir": str(tab1_artifact_root),
                },
                "kpis": {
                    "total_expiring_users": 2,
                    "historical_churn_rate": 0.0,
                    "overall_median_survival": 610.0,
                    "auto_renew_rate": 50.0,
                    "total_expected_renewal_amount": 150.0,
                    "historical_revenue_at_risk": 0.0,
                },
                "previous_kpis": None,
                "monthly_trend": [
                    {
                        "target_month": 201703,
                        "month_label": "2017-03",
                        "total_expiring_users": 100,
                        "historical_churn_rate": 10.0,
                        "overall_median_survival": 600.0,
                        "auto_renew_rate": 88.0,
                        "total_expected_renewal_amount": 10000.0,
                        "historical_revenue_at_risk": 1000.0,
                        "apru": 100.0,
                        "new_paid_users": 10,
                        "churned_users": 10,
                        "net_movement": 0,
                    }
                ],
                "churn_breakdown": {
                    "renewed_users": 2,
                    "churned_users": 0,
                    "renewed_rate": 100.0,
                    "churned_rate": 0.0,
                },
                "risk_heatmap": [
                    {"value_tier": "Free Trial", "risk_segment": "At Risk", "users": 1}
                ],
                "km_curve": [],
                "segment_mix": [],
                "boredom_scatter": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_tab1_descriptive_payload(date(2017, 4, 1), prefer_cache=True)

    assert payload["meta"]["churn_breakdown_month"] == "2017-03"
    assert payload["churn_breakdown"]["churned_users"] == 10

    assert payload["meta"]["risk_heatmap_month"] == "2017-03"
    at_risk_cell = next(
        row for row in payload["risk_heatmap"] if row["value_tier"] == "Free Trial" and row["risk_segment"] == "At Risk"
    )
    assert at_risk_cell["users"] == 5
