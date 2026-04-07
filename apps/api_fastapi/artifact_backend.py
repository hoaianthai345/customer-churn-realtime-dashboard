from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd


DEFAULT_MODEL_PARAMS: dict[str, float] = {
    "base_prob": 0.05,
    "weight_manual": 0.30,
    "weight_low_activity": 0.25,
    "weight_high_skip": 0.15,
    "weight_low_discovery": 0.10,
    "weight_cancel_signal": 0.15,
    "prob_min": 0.01,
    "prob_max": 0.99,
    "cltv_base_months": 6.0,
    "cltv_retention_months": 6.0,
    "cltv_txn_gain": 0.03,
    "risk_horizon_months": 6.0,
    "hazard_base": 1.0,
    "hazard_churn_weight": 1.5,
    "hazard_skip_weight": 0.3,
    "hazard_low_activity_weight": 0.2,
}

DEFAULT_SCENARIO_CONFIG: dict[str, float] = {
    "manual_to_auto_share": 0.30,
    "upsell_share": 0.20,
    "engagement_share": 0.25,
    "manual_to_auto_cost_per_user": 0.0,
    "upsell_cost_per_user": 0.0,
    "engagement_cost_per_user": 0.0,
}

DEFAULT_SENSITIVITY_SHARES = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50]

ALLOWED_SEGMENT_TYPES = {"price_segment", "loyalty_segment", "active_segment"}


def discover_project_root(start: Path | None = None) -> Path:
    start = (start or Path(__file__).resolve()).resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "apps").exists() and (candidate / "data").exists():
            return candidate
    return start.parent


def _build_upward_candidates(base_dir: Path, relative_parts: Iterable[str]) -> list[Path]:
    parts = tuple(relative_parts)
    return [parent.joinpath(*parts) for parent in [base_dir.resolve(), *base_dir.resolve().parents]]


def _resolve_dir(
    *,
    env_var: str,
    root_hint: str | Path | None,
    required_files: Iterable[str],
    local_candidates: list[tuple[str, ...]],
) -> Path:
    base_dir = discover_project_root()
    candidates: list[Path] = []

    env_text_raw = os.getenv(env_var)

    if env_text_raw:
        env_path = Path(env_text_raw)
        candidates.append(env_path)
        for rel in local_candidates:
            candidates.append(env_path.joinpath(*rel))

    if root_hint is not None:
        root_path = Path(root_hint)
        candidates.append(root_path)
        for rel in local_candidates:
            candidates.append(root_path.joinpath(*rel))

    for rel in local_candidates:
        candidates.extend(_build_upward_candidates(base_dir, rel))

    seen: set[Path] = set()
    required = tuple(required_files)
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen or not candidate.exists():
            continue
        seen.add(candidate)
        if all((candidate / name).exists() for name in required):
            return candidate

    required_text = ", ".join(required)
    raise FileNotFoundError(f"Khong tim thay artifact dir can cac file: {required_text}")


def resolve_feature_store_dir(root_hint: str | Path | None = None, score_month: int = 201704) -> Path:
    return _resolve_dir(
        env_var="FEATURE_STORE_DIR",
        root_hint=root_hint,
        required_files=(
            "train_features_bi_all.parquet",
            f"test_features_bi_{score_month}_full.parquet",
            "feature_columns.csv",
            "bi_dimension_columns.csv",
        ),
        local_candidates=[
            ("data", "artifacts", "feature_store"),
            ("artifacts", "feature_store"),
            ("feature_store",),
        ],
    )


def resolve_tab2_artifacts_dir(root_hint: str | Path | None = None, score_month: int = 201704) -> Path:
    return _resolve_dir(
        env_var="TAB2_ARTIFACTS_DIR",
        root_hint=root_hint,
        required_files=(
            f"tab2_test_scored_{score_month}.parquet",
            "tab2_validation_metrics.json",
            "tab2_model_summary.json",
        ),
        local_candidates=[
            ("data", "artifacts_tab2_predictive"),
            ("data", "artifacts", "tab2_predictive"),
            ("artifacts", "tab2_predictive"),
            ("artifacts_tab2_predictive",),
            ("data", "artifacts", "_smoke_test", "tab2"),
        ],
    )


def resolve_tab3_artifacts_dir(root_hint: str | Path | None = None, score_month: int = 201704) -> Path:
    return _resolve_dir(
        env_var="TAB3_ARTIFACTS_DIR",
        root_hint=root_hint,
        required_files=(
            f"tab3_scenario_summary_{score_month}.json",
            f"tab3_lever_summary_{score_month}.parquet",
            f"tab3_population_risk_shift_{score_month}.parquet",
        ),
        local_candidates=[
            ("data", "artifacts_tab3_prescriptive"),
            ("data", "artifacts", "tab3_prescriptive"),
            ("artifacts", "tab3_prescriptive"),
            ("artifacts_tab3_prescriptive",),
            ("data", "artifacts", "_smoke_test", "tab3"),
        ],
    )


@lru_cache(maxsize=32)
def _read_parquet_cached(path_text: str) -> pd.DataFrame:
    return pd.read_parquet(path_text)


def read_parquet_copy(path: str | Path) -> pd.DataFrame:
    return _read_parquet_cached(str(Path(path).resolve())).copy()


@lru_cache(maxsize=32)
def _read_json_cached(path_text: str) -> dict[str, Any]:
    return json.loads(Path(path_text).read_text(encoding="utf-8"))


def read_json_copy(path: str | Path) -> dict[str, Any]:
    return deepcopy(_read_json_cached(str(Path(path).resolve())))


def month_start_to_yyyymm(month_start: date) -> int:
    return month_start.year * 100 + month_start.month


def previous_yyyymm(target_month: int) -> int:
    year = target_month // 100
    month = target_month % 100
    if month == 1:
        return (year - 1) * 100 + 12
    return year * 100 + (month - 1)


def yyyymm_to_label(target_month: int) -> str:
    text = str(int(target_month)).zfill(6)
    return f"{text[:4]}-{text[4:6]}"


def available_tab2_months(root_hint: str | Path | None = None) -> list[str]:
    try:
        root = resolve_tab2_artifacts_dir(root_hint=root_hint)
    except FileNotFoundError:
        return []
    months: set[str] = set()
    for path in root.glob("tab2_*_scored_*.parquet"):
        month_text = path.stem.rsplit("_", 1)[-1]
        if month_text.isdigit() and len(month_text) == 6:
            months.add(yyyymm_to_label(int(month_text)))
    return sorted(months)


def _load_tab2_scored_df(tab2_dir: Path, target_month: int) -> pd.DataFrame:
    candidates = [
        tab2_dir / f"tab2_test_scored_{target_month}.parquet",
        tab2_dir / f"tab2_valid_scored_{target_month}.parquet",
    ]
    for path in candidates:
        if path.exists():
            return read_parquet_copy(path)
    raise FileNotFoundError(f"Khong tim thay scored artifact cho thang {target_month} trong {tab2_dir}")


def _validate_segment_filter(segment_type: Optional[str], segment_value: Optional[str]) -> None:
    if not segment_type and not segment_value:
        return
    if not segment_type or not segment_value:
        raise ValueError("segment_type và segment_value phải đi cùng nhau")
    if segment_type not in ALLOWED_SEGMENT_TYPES:
        raise ValueError(f"segment_type khong hop le: {segment_type}")


def _filter_by_segment(df: pd.DataFrame, segment_type: Optional[str], segment_value: Optional[str]) -> pd.DataFrame:
    _validate_segment_filter(segment_type, segment_value)
    if not segment_type or not segment_value:
        return df.reset_index(drop=True)
    if segment_type not in df.columns:
        raise ValueError(f"Khong tim thay cot segment trong artifact: {segment_type}")
    return df[df[segment_type].fillna("Unknown").astype(str) == segment_value].reset_index(drop=True)


def _safe_probability(series: pd.Series) -> pd.Series:
    return series.fillna(0).astype("float32").clip(lower=1e-4, upper=1 - 1e-4)


def _safe_amount(series: pd.Series) -> pd.Series:
    return series.fillna(0).astype("float32").clip(lower=0.0)


def _mean_or_default(series: pd.Series, default: float) -> float:
    valid = series.dropna()
    if valid.empty:
        return float(default)
    return float(valid.mean())


def _first_non_empty_median(frames: list[pd.Series], default: float = 0.0) -> float:
    for series in frames:
        valid = series.dropna()
        if not valid.empty:
            return float(valid.median())
    return float(default)


def risk_band_from_probability(probabilities: pd.Series) -> pd.Series:
    probs = probabilities.astype("float32").clip(lower=0.0, upper=1.0)
    return pd.Series(
        np.select(
            [
                probs >= 0.80,
                probs >= 0.60,
                probs >= 0.40,
                probs >= 0.20,
            ],
            ["Very High", "High", "Medium", "Low"],
            default="Very Low",
        ),
        index=probabilities.index,
    )


def _derive_primary_risk_driver(df: pd.DataFrame) -> pd.Series:
    renewal = df.get("renewal_segment", pd.Series("Unknown", index=df.index)).fillna("Unknown").astype(str)
    skip_seg = df.get("skip_segment", pd.Series("Unknown", index=df.index)).fillna("Unknown").astype(str)
    discovery_seg = df.get("discovery_segment", pd.Series("Unknown", index=df.index)).fillna("Unknown").astype(str)
    price_seg = df.get("price_segment", pd.Series("Unknown", index=df.index)).fillna("Unknown").astype(str)
    loyalty_seg = df.get("loyalty_segment", pd.Series("Unknown", index=df.index)).fillna("Unknown").astype(str)

    drivers = np.select(
        [
            renewal.eq("Pay_Manual"),
            skip_seg.eq("High >= 50%"),
            discovery_seg.eq("Habit < 20%"),
            price_seg.isin(["Deal Hunter < 4.5", "Free Trial / Zero Pay"]),
            loyalty_seg.isin(["New < 30d", "Growing 30-179d"]),
        ],
        [
            "Manual Renewal",
            "High Skip Ratio",
            "Low Discovery",
            "Price Sensitivity",
            "Early Lifecycle",
        ],
        default="Mixed Behavioral Risk",
    )
    return pd.Series(drivers, index=df.index, dtype="object")


def _build_predictive_matrix(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    work = df.copy()
    if "bi_segment_name" not in work.columns:
        work["bi_segment_name"] = "Unknown"
    work["strategic_segment"] = work["bi_segment_name"].fillna("Unknown").astype(str)
    work["primary_risk_driver"] = _derive_primary_risk_driver(work)

    grouped = (
        work.groupby("strategic_segment", as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            avg_churn_prob=("churn_probability", "mean"),
            total_future_cltv=("expected_retained_revenue_30d", "sum"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
            driver_mode=("primary_risk_driver", lambda s: s.mode().iat[0] if not s.mode().empty else "Unknown"),
        )
        .sort_values(["revenue_at_risk", "avg_churn_prob"], ascending=[False, False])
        .reset_index(drop=True)
    )
    grouped["avg_churn_prob_pct"] = grouped["avg_churn_prob"] * 100.0
    grouped["avg_future_cltv"] = grouped["total_future_cltv"] / grouped["user_count"].clip(lower=1)

    cltv_mid = float(grouped["avg_future_cltv"].median()) if not grouped.empty else 0.0
    risk_mid = float(grouped["avg_churn_prob_pct"].median()) if not grouped.empty else 0.0
    grouped["quadrant"] = np.select(
        [
            (grouped["avg_future_cltv"] >= cltv_mid) & (grouped["avg_churn_prob_pct"] >= risk_mid),
            (grouped["avg_future_cltv"] < cltv_mid) & (grouped["avg_churn_prob_pct"] >= risk_mid),
            (grouped["avg_future_cltv"] >= cltv_mid) & (grouped["avg_churn_prob_pct"] < risk_mid),
        ],
        ["Must Save", "At Risk", "Core Value"],
        default="Monitor",
    )
    grouped = grouped.rename(columns={"driver_mode": "primary_risk_driver"})
    return grouped.to_dict(orient="records")


def _build_revenue_leakage(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    work = df.copy()
    work["primary_risk_driver"] = _derive_primary_risk_driver(work)
    grouped = (
        work.groupby("primary_risk_driver", as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
        )
        .rename(columns={"primary_risk_driver": "risk_driver"})
        .sort_values("revenue_at_risk", ascending=False)
        .reset_index(drop=True)
    )
    return grouped.to_dict(orient="records")


def _build_forecast_decay(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty or "price_segment" not in df.columns:
        return []

    focus_segments = [
        "Deal Hunter < 4.5",
        "Standard 4.5-6.5",
        "Premium >= 6.5",
    ]
    rows: list[dict[str, Any]] = []
    for segment in focus_segments:
        scoped = df[df["price_segment"].fillna("Unknown").astype(str) == segment]
        if scoped.empty:
            continue
        avg_prob = float(scoped["churn_probability"].mean())
        monthly_retention = float(np.clip(1.0 - avg_prob, 0.05, 0.99))
        for month_num in range(1, 13):
            rows.append(
                {
                    "month_num": month_num,
                    "timeline": f"T+{month_num}",
                    "segment": segment,
                    "retention_pct": (monthly_retention**month_num) * 100.0,
                }
            )
    return rows


def _zero_predictive_kpis() -> dict[str, Any]:
    return {
        "forecasted_churn_rate": 0.0,
        "predicted_revenue_at_risk": 0.0,
        "predicted_total_future_cltv": 0.0,
        "top_segment": "N/A",
        "top_segment_risk": 0.0,
        "top_segment_user_count": 0,
        "forecasted_churn_delta_pp_vs_prev_month": 0.0,
    }


def _compute_predictive_kpis(df: pd.DataFrame, matrix_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if df.empty:
        return _zero_predictive_kpis()

    total_users = max(len(df), 1)
    top = matrix_rows[0] if matrix_rows else None
    return {
        "forecasted_churn_rate": float(df["churn_probability"].mean() * 100.0),
        "predicted_revenue_at_risk": float(df["expected_revenue_at_risk_30d"].sum()),
        "predicted_total_future_cltv": float(df["expected_retained_revenue_30d"].sum()),
        "top_segment": str(top["strategic_segment"]) if top else "N/A",
        "top_segment_risk": float(top["revenue_at_risk"]) if top else 0.0,
        "top_segment_user_count": int(top["user_count"]) if top else 0,
        "forecasted_churn_delta_pp_vs_prev_month": 0.0,
    }


def build_tab2_predictive_payload(
    month_start: date,
    segment_type: Optional[str] = None,
    segment_value: Optional[str] = None,
    root_hint: str | Path | None = None,
) -> dict[str, Any]:
    target_month = month_start_to_yyyymm(month_start)
    tab2_dir = resolve_tab2_artifacts_dir(root_hint=root_hint, score_month=target_month)
    scored_df = _load_tab2_scored_df(tab2_dir, target_month)
    metrics = read_json_copy(tab2_dir / "tab2_validation_metrics.json")
    model_summary = read_json_copy(tab2_dir / "tab2_model_summary.json")
    filtered = _filter_by_segment(scored_df, segment_type, segment_value)

    matrix_rows = _build_predictive_matrix(filtered)
    current_kpis = _compute_predictive_kpis(filtered, matrix_rows)

    previous_month = previous_yyyymm(target_month)
    previous_df: pd.DataFrame | None = None
    try:
        previous_df = _filter_by_segment(_load_tab2_scored_df(tab2_dir, previous_month), segment_type, segment_value)
    except FileNotFoundError:
        previous_df = None

    previous_kpis = (
        _compute_predictive_kpis(previous_df, _build_predictive_matrix(previous_df))
        if previous_df is not None and not previous_df.empty
        else _zero_predictive_kpis()
    )
    current_kpis["forecasted_churn_delta_pp_vs_prev_month"] = (
        current_kpis["forecasted_churn_rate"] - previous_kpis["forecasted_churn_rate"]
    )

    return {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "previous_month": yyyymm_to_label(previous_month),
            "sample_user_count": int(len(filtered)),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            "artifact_dir": str(tab2_dir),
            "artifact_mode": "model_backed",
        },
        "model_params": {
            **deepcopy(DEFAULT_MODEL_PARAMS),
            "feature_count": int(metrics.get("feature_count", model_summary.get("feature_count", 0))),
            "use_calibrated_output": bool(metrics.get("use_calibrated_output", False)),
            "source_mode": "artifact_backed",
        },
        "kpis": current_kpis,
        "previous_kpis": previous_kpis,
        "value_risk_matrix": matrix_rows,
        "revenue_leakage": _build_revenue_leakage(filtered),
        "forecast_decay": _build_forecast_decay(filtered),
        "prescriptions": matrix_rows[:200],
        "feature_group_importance": model_summary.get("top_feature_groups", []),
    }


def load_feature_snapshot(feature_store_dir: str | Path, score_month: int) -> pd.DataFrame:
    feature_store_dir = Path(feature_store_dir)
    score_path = feature_store_dir / f"test_features_bi_{score_month}_full.parquet"
    if score_path.exists():
        df = read_parquet_copy(score_path)
        if "target_month" in df.columns:
            df = df[df["target_month"] == score_month].reset_index(drop=True)
        return df

    master_path = feature_store_dir / "bi_feature_master.parquet"
    if master_path.exists():
        df = read_parquet_copy(master_path)
        return df[df["target_month"] == score_month].reset_index(drop=True)

    raise FileNotFoundError(
        f"Khong tim thay snapshot BI cho score_month={score_month} trong feature store."
    )


def _prepare_baseline_dataframe(feature_df: pd.DataFrame, scored_df: pd.DataFrame) -> pd.DataFrame:
    required_scored = {"msno", "target_month", "churn_probability", "expected_renewal_amount"}
    required_feature = {"msno", "target_month"}
    missing_scored = sorted(required_scored - set(scored_df.columns))
    missing_feature = sorted(required_feature - set(feature_df.columns))
    if missing_scored:
        raise ValueError(f"Tab 2 scored artifact thieu cot: {missing_scored}")
    if missing_feature:
        raise ValueError(f"feature snapshot thieu cot: {missing_feature}")

    feature_columns = [
        "msno",
        "target_month",
        "renewal_segment",
        "price_segment",
        "loyalty_segment",
        "active_segment",
        "skip_segment",
        "discovery_segment",
        "rfm_segment",
        "bi_segment_name",
        "is_auto_renew",
        "is_manual_renew",
        "deal_hunter_flag",
        "free_trial_flag",
        "high_skip_flag",
        "low_discovery_flag",
        "content_fatigue_flag",
        "skip_ratio",
        "discovery_ratio",
        "expected_renewal_amount",
        "amt_per_day",
        "rfm_total_score",
    ]
    feature_only_cols = [col for col in feature_columns if col in feature_df.columns and col not in scored_df.columns]
    merged = scored_df.merge(
        feature_df[["msno", "target_month", *feature_only_cols]],
        on=["msno", "target_month"],
        how="left",
    )

    merged["baseline_churn_probability"] = _safe_probability(merged["churn_probability"])
    merged["expected_renewal_amount"] = _safe_amount(merged["expected_renewal_amount"])
    merged["risk_band"] = (
        merged["risk_band"].fillna("Unknown").astype(str)
        if "risk_band" in merged.columns
        else risk_band_from_probability(merged["baseline_churn_probability"])
    )

    for col in (
        "renewal_segment",
        "price_segment",
        "loyalty_segment",
        "active_segment",
        "skip_segment",
        "discovery_segment",
        "rfm_segment",
        "bi_segment_name",
    ):
        if col not in merged.columns:
            merged[col] = "Unknown"
        merged[col] = merged[col].fillna("Unknown").astype(str)

    merged["skip_ratio"] = merged.get("skip_ratio", pd.Series(0.0, index=merged.index)).fillna(0).astype("float32").clip(lower=0.0, upper=1.0)
    merged["discovery_ratio"] = merged.get("discovery_ratio", pd.Series(0.0, index=merged.index)).fillna(0).astype("float32").clip(lower=0.0, upper=1.0)

    if "is_manual_renew" in merged.columns:
        merged["is_manual_renew"] = merged["is_manual_renew"].fillna(0).astype("int8")
    elif "renewal_segment" in merged.columns:
        merged["is_manual_renew"] = merged["renewal_segment"].eq("Pay_Manual").astype("int8")
    elif "is_auto_renew" in merged.columns:
        merged["is_manual_renew"] = (1 - merged["is_auto_renew"].fillna(0).astype("int8")).clip(lower=0, upper=1)
    else:
        merged["is_manual_renew"] = 0

    merged["deal_hunter_flag"] = (
        merged["deal_hunter_flag"].fillna(0).astype("int8")
        if "deal_hunter_flag" in merged.columns
        else merged["price_segment"].eq("Deal Hunter < 4.5").astype("int8")
    )
    merged["free_trial_flag"] = (
        merged["free_trial_flag"].fillna(0).astype("int8")
        if "free_trial_flag" in merged.columns
        else (
            merged["price_segment"].eq("Free Trial / Zero Pay")
            | merged["expected_renewal_amount"].le(0)
        ).astype("int8")
    )
    merged["high_skip_flag"] = (
        merged["high_skip_flag"].fillna(0).astype("int8")
        if "high_skip_flag" in merged.columns
        else merged["skip_ratio"].ge(0.5).astype("int8")
    )
    merged["low_discovery_flag"] = (
        merged["low_discovery_flag"].fillna(0).astype("int8")
        if "low_discovery_flag" in merged.columns
        else merged["discovery_ratio"].lt(0.2).astype("int8")
    )
    merged["content_fatigue_flag"] = (
        merged["content_fatigue_flag"].fillna(0).astype("int8")
        if "content_fatigue_flag" in merged.columns
        else (merged["high_skip_flag"].eq(1) & merged["low_discovery_flag"].eq(1)).astype("int8")
    )

    merged["eligible_auto"] = merged["is_manual_renew"].eq(1).astype("int8")
    merged["eligible_upsell"] = (merged["deal_hunter_flag"].eq(1) | merged["free_trial_flag"].eq(1)).astype("int8")
    merged["eligible_engagement"] = (
        merged["high_skip_flag"].eq(1)
        | merged["low_discovery_flag"].eq(1)
        | merged["content_fatigue_flag"].eq(1)
    ).astype("int8")
    merged["baseline_revenue_at_risk_30d"] = (
        merged["baseline_churn_probability"] * merged["expected_renewal_amount"]
    ).astype("float32")
    merged["baseline_retained_revenue_30d"] = (
        (1 - merged["baseline_churn_probability"]) * merged["expected_renewal_amount"]
    ).astype("float32")
    return merged


def estimate_lever_parameters(df: pd.DataFrame) -> dict[str, float]:
    population_default = _mean_or_default(df["baseline_churn_probability"], default=0.5)

    manual_mean = _mean_or_default(
        df.loc[df["eligible_auto"] == 1, "baseline_churn_probability"],
        default=population_default,
    )
    auto_mean = _mean_or_default(
        df.loc[df["renewal_segment"].eq("Pay_Auto-Renew"), "baseline_churn_probability"],
        default=population_default,
    )
    auto_gap = max(manual_mean - auto_mean, 0.0)
    auto_prob_delta_if_treated = -float(np.clip(auto_gap, 0.0, 0.15))

    fatigue_mean = _mean_or_default(
        df.loc[df["eligible_engagement"] == 1, "baseline_churn_probability"],
        default=population_default,
    )
    healthy_mean = _mean_or_default(
        df.loc[df["eligible_engagement"] == 0, "baseline_churn_probability"],
        default=population_default,
    )
    engagement_gap = max(fatigue_mean - healthy_mean, 0.0)
    engagement_prob_delta_if_treated = -float(np.clip(engagement_gap, 0.0, 0.18))

    positive_amounts = df.loc[df["expected_renewal_amount"] > 0, "expected_renewal_amount"]
    standard_amounts = df.loc[df["price_segment"].eq("Standard 4.5-6.5"), "expected_renewal_amount"]
    premium_amounts = df.loc[df["price_segment"].eq("Premium >= 6.5"), "expected_renewal_amount"]
    standard_target_amount = _first_non_empty_median([standard_amounts, positive_amounts], default=0.0)
    premium_target_amount = _first_non_empty_median([premium_amounts, positive_amounts], default=standard_target_amount)
    premium_target_amount = max(premium_target_amount, standard_target_amount)

    deal_mean = _mean_or_default(
        df.loc[df["eligible_upsell"] == 1, "baseline_churn_probability"],
        default=population_default,
    )
    paid_stable_mean = _mean_or_default(
        df.loc[df["eligible_upsell"] == 0, "baseline_churn_probability"],
        default=population_default,
    )
    upsell_group_gap = float(np.clip(paid_stable_mean - deal_mean, -0.15, 0.12))

    return {
        "auto_prob_delta_if_treated": auto_prob_delta_if_treated,
        "engagement_prob_delta_if_treated": engagement_prob_delta_if_treated,
        "upsell_group_gap": upsell_group_gap,
        "standard_target_amount": float(standard_target_amount),
        "premium_target_amount": float(premium_target_amount),
    }


def _clip_share(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return float(np.clip(float(value), 0.0, 1.0))


def _normalize_config(config: dict[str, Any] | None) -> dict[str, float]:
    normalized = deepcopy(DEFAULT_SCENARIO_CONFIG)
    if config:
        normalized.update(config)
    for key in ("manual_to_auto_share", "upsell_share", "engagement_share"):
        normalized[key] = _clip_share(normalized.get(key))
    for key in ("manual_to_auto_cost_per_user", "upsell_cost_per_user", "engagement_cost_per_user"):
        normalized[key] = float(max(float(normalized.get(key, 0.0)), 0.0))
    return normalized


def _upsell_target_amount(df: pd.DataFrame, params: dict[str, float]) -> pd.Series:
    current_amount = df["expected_renewal_amount"].astype("float32")
    standard_target = float(params["standard_target_amount"])
    premium_target = float(params["premium_target_amount"])
    base_target = np.where(
        df["free_trial_flag"].eq(1),
        standard_target,
        np.maximum(current_amount.to_numpy(dtype="float32"), standard_target),
    )
    high_value_mask = (
        df["rfm_segment"].eq("High Value")
        | df.get("rfm_total_score", pd.Series(0, index=df.index)).fillna(0).ge(7)
    )
    base_target = np.where(high_value_mask, np.maximum(base_target, premium_target), base_target)
    return pd.Series(base_target, index=df.index, dtype="float32")


def simulate_prescriptive_scenario(
    baseline_df: pd.DataFrame,
    scenario_config: dict[str, Any] | None = None,
    lever_parameters: dict[str, float] | None = None,
) -> pd.DataFrame:
    config = _normalize_config(scenario_config)
    params = lever_parameters or estimate_lever_parameters(baseline_df)
    df = baseline_df.copy()

    df["auto_treatment_weight"] = df["eligible_auto"].astype("float32") * config["manual_to_auto_share"]
    df["engagement_treatment_weight"] = df["eligible_engagement"].astype("float32") * config["engagement_share"]
    df["upsell_treatment_weight"] = df["eligible_upsell"].astype("float32") * config["upsell_share"]

    df["auto_prob_delta_if_treated"] = float(params["auto_prob_delta_if_treated"])
    df["engagement_prob_delta_if_treated"] = float(params["engagement_prob_delta_if_treated"])

    target_amount = _upsell_target_amount(df, params)
    df["upsell_target_amount_if_treated"] = target_amount
    df["upsell_amount_delta_if_treated"] = (target_amount - df["expected_renewal_amount"]).clip(lower=0.0).astype("float32")
    uplift_ratio = (
        df["upsell_amount_delta_if_treated"] / np.maximum(target_amount.to_numpy(dtype="float32"), 1.0)
    ).astype("float32")
    price_shock_penalty = np.clip(0.08 * uplift_ratio, 0.0, 0.10)
    df["upsell_prob_delta_if_treated"] = np.clip(float(params["upsell_group_gap"]) + price_shock_penalty, -0.15, 0.12).astype("float32")

    df["auto_probability_delta"] = (df["auto_treatment_weight"] * df["auto_prob_delta_if_treated"]).astype("float32")
    df["engagement_probability_delta"] = (df["engagement_treatment_weight"] * df["engagement_prob_delta_if_treated"]).astype("float32")
    df["upsell_probability_delta"] = (df["upsell_treatment_weight"] * df["upsell_prob_delta_if_treated"]).astype("float32")
    df["upsell_amount_delta"] = (df["upsell_treatment_weight"] * df["upsell_amount_delta_if_treated"]).astype("float32")

    df["simulated_expected_renewal_amount"] = (df["expected_renewal_amount"] + df["upsell_amount_delta"]).astype("float32")
    df["simulated_churn_probability"] = np.clip(
        df["baseline_churn_probability"]
        + df["auto_probability_delta"]
        + df["engagement_probability_delta"]
        + df["upsell_probability_delta"],
        1e-4,
        1 - 1e-4,
    ).astype("float32")
    df["simulated_revenue_at_risk_30d"] = (df["simulated_churn_probability"] * df["simulated_expected_renewal_amount"]).astype("float32")
    df["price_only_retained_revenue_30d"] = (
        (1 - df["baseline_churn_probability"]) * df["simulated_expected_renewal_amount"]
    ).astype("float32")
    df["simulated_retained_revenue_30d"] = (
        (1 - df["simulated_churn_probability"]) * df["simulated_expected_renewal_amount"]
    ).astype("float32")
    df["risk_effect_revenue_delta_30d"] = (
        df["simulated_retained_revenue_30d"] - df["price_only_retained_revenue_30d"]
    ).astype("float32")
    df["upsell_effect_revenue_delta_30d"] = (
        df["price_only_retained_revenue_30d"] - df["baseline_retained_revenue_30d"]
    ).astype("float32")
    df["total_retained_revenue_delta_30d"] = (
        df["simulated_retained_revenue_30d"] - df["baseline_retained_revenue_30d"]
    ).astype("float32")
    df["simulated_risk_band"] = risk_band_from_probability(df["simulated_churn_probability"])

    df["manual_to_auto_cost"] = (df["auto_treatment_weight"] * config["manual_to_auto_cost_per_user"]).astype("float32")
    df["upsell_cost"] = (df["upsell_treatment_weight"] * config["upsell_cost_per_user"]).astype("float32")
    df["engagement_cost"] = (df["engagement_treatment_weight"] * config["engagement_cost_per_user"]).astype("float32")
    df["campaign_cost"] = (df["manual_to_auto_cost"] + df["upsell_cost"] + df["engagement_cost"]).astype("float32")
    return df


def summarize_scenario(member_level_df: pd.DataFrame, scenario_config: dict[str, float], lever_parameters: dict[str, float]) -> dict[str, Any]:
    summary = {
        "score_month": int(member_level_df["target_month"].iloc[0]),
        "population_users": int(member_level_df["msno"].nunique()),
        "eligible_auto_users": int(member_level_df["eligible_auto"].sum()),
        "eligible_upsell_users": int(member_level_df["eligible_upsell"].sum()),
        "eligible_engagement_users": int(member_level_df["eligible_engagement"].sum()),
        "impacted_auto_user_equivalent": float(member_level_df["auto_treatment_weight"].sum()),
        "impacted_upsell_user_equivalent": float(member_level_df["upsell_treatment_weight"].sum()),
        "impacted_engagement_user_equivalent": float(member_level_df["engagement_treatment_weight"].sum()),
        "baseline_avg_churn_probability": float(member_level_df["baseline_churn_probability"].mean()),
        "simulated_avg_churn_probability": float(member_level_df["simulated_churn_probability"].mean()),
        "baseline_revenue_at_risk_30d": float(member_level_df["baseline_revenue_at_risk_30d"].sum()),
        "simulated_revenue_at_risk_30d": float(member_level_df["simulated_revenue_at_risk_30d"].sum()),
        "baseline_retained_revenue_30d": float(member_level_df["baseline_retained_revenue_30d"].sum()),
        "simulated_retained_revenue_30d": float(member_level_df["simulated_retained_revenue_30d"].sum()),
        "saved_revenue_from_risk_reduction_30d": float(member_level_df["risk_effect_revenue_delta_30d"].clip(lower=0).sum()),
        "revenue_loss_from_risk_increase_30d": float((-member_level_df["risk_effect_revenue_delta_30d"]).clip(lower=0).sum()),
        "incremental_upsell_revenue_30d": float(member_level_df["upsell_effect_revenue_delta_30d"].sum()),
        "campaign_cost_30d": float(member_level_df["campaign_cost"].sum()),
        "scenario_config": deepcopy(scenario_config),
        "lever_parameters": deepcopy(lever_parameters),
    }
    summary["revenue_at_risk_delta_30d"] = summary["simulated_revenue_at_risk_30d"] - summary["baseline_revenue_at_risk_30d"]
    summary["retained_revenue_delta_30d"] = summary["simulated_retained_revenue_30d"] - summary["baseline_retained_revenue_30d"]
    summary["net_value_after_cost_30d"] = summary["retained_revenue_delta_30d"] - summary["campaign_cost_30d"]
    return summary


def build_population_risk_shift(member_level_df: pd.DataFrame) -> pd.DataFrame:
    bins = np.linspace(0.0, 1.0, 11)
    labels = [f"{left:.1f}-{right:.1f}" for left, right in zip(bins[:-1], bins[1:])]
    baseline_bucket = pd.cut(member_level_df["baseline_churn_probability"], bins=bins, labels=labels, include_lowest=True)
    simulated_bucket = pd.cut(member_level_df["simulated_churn_probability"], bins=bins, labels=labels, include_lowest=True)

    baseline_dist = baseline_bucket.value_counts(sort=False).rename_axis("probability_bin").reset_index(name="users")
    baseline_dist["state"] = "baseline"
    simulated_dist = simulated_bucket.value_counts(sort=False).rename_axis("probability_bin").reset_index(name="users")
    simulated_dist["state"] = "simulated"

    bin_df = pd.concat([baseline_dist, simulated_dist], ignore_index=True)
    bin_df["target_month"] = int(member_level_df["target_month"].iloc[0])
    bin_df["bin_type"] = "probability_bin"
    return bin_df


def build_sensitivity_table(
    baseline_df: pd.DataFrame,
    lever_parameters: dict[str, float],
    shares: list[float] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    sensitivity_shares = shares or list(DEFAULT_SENSITIVITY_SHARES)
    lever_to_share_key = {
        "manual_to_auto": "manual_to_auto_share",
        "upsell": "upsell_share",
        "engagement": "engagement_share",
    }

    for lever_name, share_key in lever_to_share_key.items():
        for share in sensitivity_shares:
            scenario_config = deepcopy(DEFAULT_SCENARIO_CONFIG)
            scenario_config[share_key] = _clip_share(share)
            member_level = simulate_prescriptive_scenario(
                baseline_df,
                scenario_config=scenario_config,
                lever_parameters=lever_parameters,
            )
            summary = summarize_scenario(member_level, _normalize_config(scenario_config), lever_parameters)
            rows.append(
                {
                    "lever_name": lever_name,
                    "share": float(share),
                    "simulated_avg_churn_probability": summary["simulated_avg_churn_probability"],
                    "revenue_at_risk_delta_30d": summary["revenue_at_risk_delta_30d"],
                    "retained_revenue_delta_30d": summary["retained_revenue_delta_30d"],
                    "saved_revenue_from_risk_reduction_30d": summary["saved_revenue_from_risk_reduction_30d"],
                    "incremental_upsell_revenue_30d": summary["incremental_upsell_revenue_30d"],
                    "campaign_cost_30d": summary["campaign_cost_30d"],
                    "net_value_after_cost_30d": summary["net_value_after_cost_30d"],
                }
            )
    return pd.DataFrame(rows)


@lru_cache(maxsize=8)
def _cached_baseline_frame(
    feature_store_dir_text: str,
    tab2_dir_text: str,
    target_month: int,
) -> pd.DataFrame:
    feature_df = load_feature_snapshot(feature_store_dir_text, target_month)
    scored_df = _load_tab2_scored_df(Path(tab2_dir_text), target_month)
    return _prepare_baseline_dataframe(feature_df, scored_df)


def _sensitivity_payload(sensitivity_df: pd.DataFrame) -> list[dict[str, Any]]:
    if sensitivity_df.empty:
        return []
    label_map = {
        "manual_to_auto": "Auto-Renew Conversion",
        "upsell": "Upsell to Standard/Premium",
        "engagement": "Reduce Skip / Improve Discovery",
    }
    rows: list[dict[str, Any]] = []
    for lever_name, group in sensitivity_df.groupby("lever_name"):
        non_zero = group[group["share"] > 0].copy()
        if non_zero.empty:
            value = 0.0
        else:
            value = float((non_zero["retained_revenue_delta_30d"] / (non_zero["share"] * 100.0)).median())
        rows.append({"strategy": label_map.get(lever_name, lever_name), "revenue_impact_per_1pct": value})
    rows.sort(key=lambda item: item["revenue_impact_per_1pct"], reverse=True)
    return rows


def _risk_histogram_payload(risk_shift_df: pd.DataFrame) -> list[dict[str, Any]]:
    if risk_shift_df.empty:
        return []
    base = risk_shift_df[risk_shift_df["state"] == "baseline"].set_index("probability_bin")["users"]
    scenario = risk_shift_df[risk_shift_df["state"] == "simulated"].set_index("probability_bin")["users"]
    total_base = max(float(base.sum()), 1.0)
    total_scenario = max(float(scenario.sum()), 1.0)
    rows: list[dict[str, Any]] = []
    for label in sorted(set(base.index).union(set(scenario.index))):
        start_text, end_text = str(label).split("-")
        rows.append(
            {
                "bin_start": float(start_text),
                "bin_end": float(end_text),
                "baseline_density": float(base.get(label, 0.0) / total_base),
                "scenario_density": float(scenario.get(label, 0.0) / total_scenario),
            }
        )
    return rows


def build_tab3_prescriptive_payload(
    month_start: date,
    *,
    segment_type: Optional[str] = None,
    segment_value: Optional[str] = None,
    feature_store_root_hint: str | Path | None = None,
    tab2_root_hint: str | Path | None = None,
    auto_shift_pct: float = 20.0,
    upsell_shift_pct: float = 15.0,
    skip_shift_pct: float = 25.0,
) -> dict[str, Any]:
    target_month = month_start_to_yyyymm(month_start)
    feature_store_dir = resolve_feature_store_dir(root_hint=feature_store_root_hint, score_month=target_month)
    tab2_dir = resolve_tab2_artifacts_dir(root_hint=tab2_root_hint, score_month=target_month)
    tab3_dir = resolve_tab3_artifacts_dir(root_hint=tab2_root_hint, score_month=target_month)
    baseline_df = _cached_baseline_frame(str(feature_store_dir), str(tab2_dir), target_month).copy()
    filtered = _filter_by_segment(baseline_df, segment_type, segment_value)

    if filtered.empty:
        return {
            "meta": {
                "month": yyyymm_to_label(target_month),
                "sample_user_count": 0,
                "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
                "artifact_mode": "model_backed",
                "artifact_dir": str(tab3_dir),
            },
            "model_params": {**deepcopy(DEFAULT_MODEL_PARAMS), "source_mode": "artifact_backed"},
            "scenario_inputs": {
                "auto_shift_pct": float(auto_shift_pct),
                "upsell_shift_pct": float(upsell_shift_pct),
                "skip_shift_pct": float(skip_shift_pct),
            },
            "kpis": {
                "baseline_avg_hazard": 0.0,
                "scenario_avg_hazard": 0.0,
                "baseline_churn_prob_pct": 0.0,
                "scenario_churn_prob_pct": 0.0,
                "optimized_projected_revenue": 0.0,
                "baseline_revenue": 0.0,
                "saved_revenue": 0.0,
                "incremental_upsell": 0.0,
            },
            "hazard_histogram": [],
            "financial_waterfall": [],
            "sensitivity_roi": [],
        }

    scenario_config = _normalize_config(
        {
            "manual_to_auto_share": auto_shift_pct / 100.0,
            "upsell_share": upsell_shift_pct / 100.0,
            "engagement_share": skip_shift_pct / 100.0,
        }
    )
    lever_parameters = estimate_lever_parameters(filtered)
    member_level_df = simulate_prescriptive_scenario(
        filtered,
        scenario_config=scenario_config,
        lever_parameters=lever_parameters,
    )
    summary = summarize_scenario(member_level_df, scenario_config, lever_parameters)
    risk_shift_df = build_population_risk_shift(member_level_df)
    sensitivity_df = build_sensitivity_table(filtered, lever_parameters=lever_parameters)

    return {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "sample_user_count": int(len(member_level_df)),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            "artifact_mode": "model_backed",
            "artifact_dir": str(tab3_dir),
        },
        "model_params": {**deepcopy(DEFAULT_MODEL_PARAMS), "source_mode": "artifact_backed"},
        "scenario_inputs": {
            "auto_shift_pct": float(auto_shift_pct),
            "upsell_shift_pct": float(upsell_shift_pct),
            "skip_shift_pct": float(skip_shift_pct),
        },
        "kpis": {
            "baseline_avg_hazard": float(summary["baseline_avg_churn_probability"]),
            "scenario_avg_hazard": float(summary["simulated_avg_churn_probability"]),
            "baseline_churn_prob_pct": float(summary["baseline_avg_churn_probability"] * 100.0),
            "scenario_churn_prob_pct": float(summary["simulated_avg_churn_probability"] * 100.0),
            "optimized_projected_revenue": float(summary["simulated_retained_revenue_30d"]),
            "baseline_revenue": float(summary["baseline_retained_revenue_30d"]),
            "saved_revenue": float(summary["saved_revenue_from_risk_reduction_30d"]),
            "incremental_upsell": float(summary["incremental_upsell_revenue_30d"]),
        },
        "hazard_histogram": _risk_histogram_payload(risk_shift_df),
        "financial_waterfall": [
            {"name": "Current Baseline Revenue", "value": float(summary["baseline_retained_revenue_30d"])},
            {"name": "Saved Revenue from Retention", "value": float(summary["saved_revenue_from_risk_reduction_30d"])},
            {"name": "Incremental Revenue from Upsell", "value": float(summary["incremental_upsell_revenue_30d"])},
            {"name": "Optimized Projected Revenue", "value": float(summary["simulated_retained_revenue_30d"])},
        ],
        "sensitivity_roi": _sensitivity_payload(sensitivity_df),
    }
