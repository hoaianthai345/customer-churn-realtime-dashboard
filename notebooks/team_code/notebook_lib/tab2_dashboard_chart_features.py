from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .common import ensure_output_dir, resolve_tab2_artifacts_dir, write_json


TAB2_SCORE_COLUMNS = (
    "msno",
    "churn_probability",
    "expected_renewal_amount",
    "expected_revenue_at_risk_30d",
    "risk_band",
)


def _read_parquet_subset(path: Path, columns: tuple[str, ...]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay parquet: {path}")
    available = set(pq.read_schema(path).names)
    selected = [column for column in columns if column in available]
    if not selected:
        raise ValueError(f"Khong tim thay cot can thiet trong parquet: {path}")
    return pd.read_parquet(path, columns=selected)


def _to_executive_risk_band(series: pd.Series) -> pd.Series:
    bands = series.fillna("Unknown").astype(str)
    normalized = np.select(
        [
            bands.isin(["Very High", "High"]),
            bands.isin(["Medium", "Low"]),
            bands.eq("Very Low"),
        ],
        ["High", "Medium", "Low"],
        default="Unknown",
    )
    return pd.Series(normalized, index=series.index, dtype="object")


def _priority_quadrant(prob_bin: pd.Series, renewal_amount: pd.Series) -> pd.Series:
    values = np.select(
        [
            (prob_bin >= 0.5) & (renewal_amount >= 100.0),
            (prob_bin >= 0.5) & (renewal_amount < 100.0),
            (prob_bin < 0.5) & (renewal_amount >= 100.0),
        ],
        ["VIP Rescue", "Price Sensitive", "Core Loyal"],
        default="Casual",
    )
    return pd.Series(values, index=prob_bin.index, dtype="object")


def build_executive_value_risk_matrix(
    tab2_artifacts_root_hint: str | Path | None = None,
    *,
    score_month: int = 201704,
) -> pd.DataFrame:
    tab2_dir = resolve_tab2_artifacts_dir(tab2_artifacts_root_hint, score_month=score_month)
    scored_df = _read_parquet_subset(tab2_dir / f"tab2_test_scored_{score_month}.parquet", TAB2_SCORE_COLUMNS).copy()

    scored_df["churn_probability"] = pd.to_numeric(scored_df["churn_probability"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    scored_df["expected_renewal_amount"] = pd.to_numeric(scored_df["expected_renewal_amount"], errors="coerce").fillna(0.0).clip(lower=0.0)
    scored_df["expected_revenue_at_risk_30d"] = pd.to_numeric(scored_df["expected_revenue_at_risk_30d"], errors="coerce").fillna(0.0).clip(lower=0.0)
    scored_df["risk_band"] = scored_df["risk_band"].fillna("Unknown").astype(str)
    scored_df["prob_bin"] = scored_df["churn_probability"].round(1)

    grouped = (
        scored_df.groupby(["prob_bin", "expected_renewal_amount", "risk_band"], as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
        )
        .sort_values(["prob_bin", "expected_renewal_amount", "user_count"], ascending=[True, True, False])
        .reset_index(drop=True)
    )

    grouped["risk_tier"] = _to_executive_risk_band(grouped["risk_band"])
    grouped["display_size"] = np.sqrt(grouped["user_count"].clip(lower=1)).astype("float64")
    grouped["priority_quadrant"] = _priority_quadrant(grouped["prob_bin"], grouped["expected_renewal_amount"])
    grouped["target_month"] = score_month
    grouped["month_label"] = f"{str(score_month)[:4]}-{str(score_month)[4:6]}"
    return grouped[
        [
            "target_month",
            "month_label",
            "prob_bin",
            "expected_renewal_amount",
            "risk_band",
            "risk_tier",
            "user_count",
            "revenue_at_risk",
            "display_size",
            "priority_quadrant",
        ]
    ]


def run_tab2_dashboard_chart_features(
    tab2_artifacts_root_hint: str | Path | None = None,
    output_dir: str | Path | None = None,
    *,
    score_month: int = 201704,
) -> dict[str, Any]:
    tab2_dir = resolve_tab2_artifacts_dir(tab2_artifacts_root_hint or output_dir, score_month=score_month)
    output_dir = ensure_output_dir(output_dir or tab2_dir)
    matrix_df = build_executive_value_risk_matrix(tab2_dir, score_month=score_month)

    output_paths = {
        "executive_value_risk_matrix": output_dir / f"tab2_executive_value_risk_matrix_{score_month}.parquet",
        "manifest": output_dir / "tab2_dashboard_chart_features_manifest.json",
    }
    matrix_df.to_parquet(output_paths["executive_value_risk_matrix"], index=False)
    write_json(
        output_paths["manifest"],
        {
            "score_month": score_month,
            "artifact_dir": tab2_dir,
            "output_dir": output_dir,
            "outputs": {"executive_value_risk_matrix": output_paths["executive_value_risk_matrix"].name},
            "matrix_rows": int(len(matrix_df)),
        },
    )

    return {
        "artifact_dir": tab2_dir,
        "output_dir": output_dir,
        "executive_value_risk_matrix": matrix_df,
        "output_paths": output_paths,
    }
