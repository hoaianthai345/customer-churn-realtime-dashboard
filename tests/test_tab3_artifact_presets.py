from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from apps.api_fastapi.artifact_backend import (
    build_tab3_prescriptive_payload,
    resolve_tab3_artifacts_dir,
)


TARGET_MONTH = 201704


def _risk_shift_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "probability_bin": "0.0-0.1",
                "users": 80,
                "state": "baseline",
                "target_month": TARGET_MONTH,
                "bin_type": "probability_bin",
            },
            {
                "probability_bin": "0.0-0.1",
                "users": 95,
                "state": "simulated",
                "target_month": TARGET_MONTH,
                "bin_type": "probability_bin",
            },
        ]
    )


def _sensitivity_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"lever_name": "manual_to_auto", "share": 0.2, "retained_revenue_delta_30d": 1200.0},
            {"lever_name": "upsell", "share": 0.2, "retained_revenue_delta_30d": 900.0},
            {"lever_name": "engagement", "share": 0.2, "retained_revenue_delta_30d": 700.0},
        ]
    )


def _write_prescriptive_case(case_dir: Path, summary_payload: dict[str, object]) -> None:
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / f"tab3_scenario_summary_{TARGET_MONTH}.json").write_text(
        json.dumps(summary_payload),
        encoding="utf-8",
    )
    pd.DataFrame({"lever_name": ["combined"], "net_value_after_cost_30d": [4200.0]}).to_parquet(
        case_dir / f"tab3_lever_summary_{TARGET_MONTH}.parquet",
        index=False,
    )
    _risk_shift_df().to_parquet(case_dir / f"tab3_population_risk_shift_{TARGET_MONTH}.parquet", index=False)
    _sensitivity_df().to_parquet(case_dir / f"tab3_sensitivity_{TARGET_MONTH}.parquet", index=False)


def _write_monte_carlo_case(case_dir: Path, deterministic_summary: dict[str, object]) -> None:
    case_dir.mkdir(parents=True, exist_ok=True)
    (case_dir / f"tab3_monte_carlo_summary_{TARGET_MONTH}.json").write_text(
        json.dumps(
            {
                "n_iterations": 100,
                "seed": 42,
                "beta_concentration": 50.0,
                "probability_scenario_beats_baseline": 0.88,
                "probability_net_positive": 0.79,
                "monte_carlo_metrics": {
                    "net_value_after_cost_30d": {
                        "mean": 4100.0,
                        "std": 120.0,
                        "p05": 3900.0,
                        "p25": 4010.0,
                        "p50": 4095.0,
                        "p75": 4170.0,
                        "p95": 4300.0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (case_dir / f"tab3_deterministic_summary_{TARGET_MONTH}.json").write_text(
        json.dumps(deterministic_summary),
        encoding="utf-8",
    )
    (case_dir / "manifest.json").write_text(
        json.dumps({"metadata": {"population_users": 120, "simulation_unit_count": 100}}),
        encoding="utf-8",
    )


def _summary_payload(*, auto_share: float, upsell_share: float, engagement_share: float, retained_revenue: float) -> dict[str, object]:
    return {
        "population_users": 120,
        "baseline_avg_churn_probability": 0.31,
        "simulated_avg_churn_probability": 0.22,
        "baseline_retained_revenue_30d": 10000.0,
        "simulated_retained_revenue_30d": retained_revenue,
        "saved_revenue_from_risk_reduction_30d": 1500.0,
        "incremental_upsell_revenue_30d": 900.0,
        "campaign_cost_30d": 400.0,
        "net_value_after_cost_30d": retained_revenue - 10000.0 - 400.0,
        "scenario_config": {
            "manual_to_auto_share": auto_share,
            "upsell_share": upsell_share,
            "engagement_share": engagement_share,
            "manual_to_auto_cost_per_user": 0.0,
            "upsell_cost_per_user": 0.0,
            "engagement_cost_per_user": 0.0,
        },
    }


@pytest.fixture()
def tab3_artifact_roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    prescriptive_root = tmp_path / "artifacts_tab3_prescriptive"
    monte_carlo_root = tmp_path / "artifacts_tab3_monte_carlo"

    balanced_summary = _summary_payload(auto_share=0.20, upsell_share=0.15, engagement_share=0.25, retained_revenue=11900.0)
    aggressive_summary = _summary_payload(auto_share=0.35, upsell_share=0.30, engagement_share=0.40, retained_revenue=12800.0)

    _write_prescriptive_case(prescriptive_root, balanced_summary)
    _write_prescriptive_case(prescriptive_root / "scenarios" / "aggressive-growth", aggressive_summary)
    (prescriptive_root / "scenario_catalog.json").write_text(
        json.dumps(
            {
                "default_scenario_id": "balanced-demo",
                "scenarios": [
                    {
                        "scenario_id": "balanced-demo",
                        "label": "Balanced demo",
                        "description": "Default preset for stakeholder walkthrough.",
                        "artifact_subdir": ".",
                        "monte_carlo_subdir": ".",
                        "scenario_inputs": {"auto_shift_pct": 20.0, "upsell_shift_pct": 15.0, "skip_shift_pct": 25.0},
                    },
                    {
                        "scenario_id": "aggressive-growth",
                        "label": "Aggressive growth",
                        "description": "Push stronger conversion and engagement levers.",
                        "artifact_subdir": "scenarios/aggressive-growth",
                        "monte_carlo_subdir": "scenarios/aggressive-growth",
                        "scenario_inputs": {"auto_shift_pct": 35.0, "upsell_shift_pct": 30.0, "skip_shift_pct": 40.0},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    _write_monte_carlo_case(
        monte_carlo_root,
        {
            "baseline_churn_prob_pct": 31.0,
            "scenario_churn_prob_pct": 22.0,
            "baseline_retained_revenue_30d": 10000.0,
            "scenario_retained_revenue_30d": 11900.0,
            "saved_revenue_from_risk_reduction_30d": 1500.0,
            "incremental_upsell_revenue_30d": 900.0,
            "campaign_cost_30d": 400.0,
            "net_value_after_cost_30d": 1400.0,
        },
    )
    _write_monte_carlo_case(
        monte_carlo_root / "scenarios" / "aggressive-growth",
        {
            "baseline_churn_prob_pct": 31.0,
            "scenario_churn_prob_pct": 18.0,
            "baseline_retained_revenue_30d": 10000.0,
            "scenario_retained_revenue_30d": 12800.0,
            "saved_revenue_from_risk_reduction_30d": 1900.0,
            "incremental_upsell_revenue_30d": 1300.0,
            "campaign_cost_30d": 500.0,
            "net_value_after_cost_30d": 2300.0,
        },
    )
    (monte_carlo_root / "scenario_catalog.json").write_text(
        json.dumps(
            {
                "default_scenario_id": "balanced-demo",
                "scenarios": [
                    {"scenario_id": "balanced-demo", "monte_carlo_subdir": "."},
                    {"scenario_id": "aggressive-growth", "monte_carlo_subdir": "scenarios/aggressive-growth"},
                ],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("TAB3_ARTIFACTS_DIR", str(prescriptive_root))
    monkeypatch.setenv("TAB3_MONTE_CARLO_ARTIFACTS_DIR", str(monte_carlo_root))
    return prescriptive_root, monte_carlo_root


def test_tab3_payload_loads_selected_scenario_from_catalog(tab3_artifact_roots: tuple[Path, Path]) -> None:
    payload = build_tab3_prescriptive_payload(date(2017, 4, 1), scenario_id="aggressive-growth")

    assert payload["meta"]["scenario_id"] == "aggressive-growth"
    assert payload["meta"]["scenario_label"] == "Aggressive growth"
    assert payload["scenario_inputs"] == {
        "auto_shift_pct": 35.0,
        "upsell_shift_pct": 30.0,
        "skip_shift_pct": 40.0,
    }
    assert payload["kpis"]["optimized_projected_revenue"] == pytest.approx(12800.0)
    assert payload["monte_carlo"]["enabled"] is True
    assert any(
        item["scenario_id"] == "aggressive-growth" and item["has_monte_carlo"] is True
        for item in payload["meta"]["available_scenarios"]
    )


def test_tab3_payload_rejects_unknown_scenario(tab3_artifact_roots: tuple[Path, Path]) -> None:
    with pytest.raises(ValueError, match="Scenario preset"):
        build_tab3_prescriptive_payload(date(2017, 4, 1), scenario_id="unknown-case")


def test_resolve_tab3_artifacts_dir_prefers_catalog_bundle(tmp_path: Path) -> None:
    plain_dir = tmp_path / "data" / "artifacts_tab3_prescriptive"
    catalog_dir = tmp_path / "data" / "artifacts_tab3_prescriptive 2"

    _write_prescriptive_case(plain_dir, _summary_payload(auto_share=0.30, upsell_share=0.20, engagement_share=0.25, retained_revenue=11100.0))
    _write_prescriptive_case(catalog_dir, _summary_payload(auto_share=0.20, upsell_share=0.15, engagement_share=0.25, retained_revenue=11900.0))
    (catalog_dir / "scenario_catalog.json").write_text(
        json.dumps({"default_scenario_id": "balanced-demo", "scenarios": [{"scenario_id": "balanced-demo"}]}),
        encoding="utf-8",
    )

    resolved = resolve_tab3_artifacts_dir(root_hint=tmp_path, score_month=TARGET_MONTH)

    assert resolved == catalog_dir.resolve()
