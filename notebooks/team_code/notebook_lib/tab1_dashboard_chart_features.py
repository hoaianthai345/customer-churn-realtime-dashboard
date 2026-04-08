from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from .common import (
    ensure_output_dir,
    make_yyyymm_label,
    resolve_feature_store_dir,
    resolve_tab1_artifacts_dir,
    write_json,
)


TAB1_VISIBLE_VALUE_TIERS = ("Free Trial", "Deal Hunter", "Standard")
TAB1_RISK_SEGMENT_ORDER = ("At Risk", "Watchlist", "Stable")
TAB1_EXCLUDED_TREND_MONTHS = frozenset({201704})

TREND_SOURCE_COLUMNS = (
    "target_month",
    "msno",
    "loyalty_segment",
    "is_churn",
    "is_auto_renew",
    "expected_renewal_amount",
    "membership_age_days",
)

RISK_SOURCE_COLUMNS = (
    "target_month",
    "msno",
    "price_segment",
    "free_trial_flag",
    "deal_hunter_flag",
    "churn_rate",
    "is_churn",
)


def _read_parquet_subset(path: Path, columns: tuple[str, ...]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=list(columns))

    available = set(pq.read_schema(path).names)
    selected = [column for column in columns if column in available]
    if not selected:
        return pd.DataFrame(columns=list(columns))
    return pd.read_parquet(path, columns=selected)


def _coerce_numeric(series: pd.Series | None, *, fill_value: float = 0.0) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    return pd.to_numeric(series, errors="coerce").fillna(fill_value)


def _yyyymm_to_timestamp(target_month: pd.Series) -> pd.Series:
    return pd.to_datetime(target_month.astype("int32").astype(str) + "01", format="%Y%m%d", errors="coerce")


def _load_trend_source_frame(feature_store_dir: Path) -> pd.DataFrame:
    train_df = _read_parquet_subset(feature_store_dir / "train_features_bi_all.parquet", TREND_SOURCE_COLUMNS)
    if train_df.empty:
        raise FileNotFoundError("Khong tim thay du lieu trend cho Tab 1 trong train_features_bi_all.parquet.")
    return train_df


def _load_risk_source_frame(feature_store_dir: Path, tab1_artifact_dir: Path, score_month: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    train_df = _read_parquet_subset(feature_store_dir / "train_features_bi_all.parquet", RISK_SOURCE_COLUMNS)
    if not train_df.empty:
        frames.append(train_df)

    score_df = _read_parquet_subset(feature_store_dir / f"test_features_bi_{score_month}_full.parquet", RISK_SOURCE_COLUMNS)
    if not score_df.empty:
        frames.append(score_df)

    snapshot_df = _read_parquet_subset(tab1_artifact_dir / f"tab1_snapshot_{score_month}.parquet", RISK_SOURCE_COLUMNS)
    if not snapshot_df.empty:
        snapshot_df["target_month"] = score_month
        frames = [frame.loc[pd.to_numeric(frame["target_month"], errors="coerce") != score_month].copy() for frame in frames]
        frames.append(snapshot_df)

    if not frames:
        raise FileNotFoundError("Khong tim thay du lieu risk heatmap cho Tab 1.")

    return pd.concat(frames, ignore_index=True)


def _build_value_tier(work: pd.DataFrame) -> pd.Series:
    price_segment = work["price_segment"].fillna("").astype(str) if "price_segment" in work.columns else pd.Series("", index=work.index)
    free_trial_flag = _coerce_numeric(work["free_trial_flag"], fill_value=0.0) if "free_trial_flag" in work.columns else pd.Series(0.0, index=work.index)
    deal_hunter_flag = _coerce_numeric(work["deal_hunter_flag"], fill_value=0.0) if "deal_hunter_flag" in work.columns else pd.Series(0.0, index=work.index)
    values = np.select(
        [
            (free_trial_flag >= 0.5) | price_segment.str.contains("Free Trial", case=False, regex=False),
            (deal_hunter_flag >= 0.5) | price_segment.str.contains("Deal Hunter", case=False, regex=False),
        ],
        ["Free Trial", "Deal Hunter"],
        default="Standard",
    )
    return pd.Series(values, index=work.index, dtype="object")


def _build_risk_customer_segment(work: pd.DataFrame) -> pd.Series:
    if "churn_rate" in work.columns:
        churn_rate = _coerce_numeric(work["churn_rate"], fill_value=-1.0)
        valid_mask = churn_rate >= 0.0
        if valid_mask.any() and float(churn_rate[valid_mask].max()) > 1.0:
            churn_rate.loc[valid_mask] = churn_rate.loc[valid_mask] / 100.0
    elif "is_churn" in work.columns:
        churn_rate = _coerce_numeric(work["is_churn"], fill_value=0.0)
    else:
        churn_rate = pd.Series(0.0, index=work.index)

    values = np.select(
        [churn_rate >= 0.25, churn_rate > 0.0],
        ["At Risk", "Watchlist"],
        default="Stable",
    )
    return pd.Series(values, index=work.index, dtype="object")


def build_trend_monthly_summary(
    feature_store_root_hint: str | Path | None = None,
    *,
    score_month: int = 201704,
) -> pd.DataFrame:
    feature_store_dir = resolve_feature_store_dir(feature_store_root_hint, score_month=score_month)
    work = _load_trend_source_frame(feature_store_dir).copy()
    work["target_month"] = pd.to_numeric(work["target_month"], errors="coerce").astype("Int64")
    work = work.dropna(subset=["target_month", "msno"]).copy()
    work["target_month"] = work["target_month"].astype("int32")
    work = work.loc[~work["target_month"].isin(TAB1_EXCLUDED_TREND_MONTHS)].copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                "target_month",
                "month_label",
                "analysis_month",
                "subscribers",
                "revenue",
                "apru",
                "churn_rate",
                "new_paid_users",
                "churned_users",
                "churned_users_diverging",
                "net_movement",
                "auto_renew_rate",
                "overall_median_survival",
            ]
        )

    work["expected_renewal_amount"] = _coerce_numeric(work["expected_renewal_amount"], fill_value=0.0).clip(lower=0.0)
    work["is_churn"] = _coerce_numeric(work["is_churn"], fill_value=0.0).clip(lower=0.0, upper=1.0)
    work["is_auto_renew"] = _coerce_numeric(work["is_auto_renew"], fill_value=0.0).clip(lower=0.0, upper=1.0)
    work["membership_age_days"] = _coerce_numeric(work["membership_age_days"], fill_value=0.0).clip(lower=1.0)
    work["loyalty_segment"] = work["loyalty_segment"].fillna("Unknown").astype(str)

    grouped = (
        work.groupby("target_month", as_index=False)
        .agg(
            subscribers=("msno", "nunique"),
            revenue=("expected_renewal_amount", "sum"),
            churn_rate=("is_churn", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0.0).mean() * 100.0)),
            new_paid_users=("loyalty_segment", lambda s: int(s.eq("New < 30d").sum())),
            churned_users=("is_churn", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0.0).sum())),
            auto_renew_rate=("is_auto_renew", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0.0).mean() * 100.0)),
            overall_median_survival=("membership_age_days", lambda s: float(pd.to_numeric(s, errors="coerce").dropna().median() if pd.to_numeric(s, errors="coerce").notna().any() else 0.0)),
        )
        .sort_values("target_month")
        .reset_index(drop=True)
    )

    grouped["subscribers"] = grouped["subscribers"].astype(int)
    grouped["churned_users"] = grouped["churned_users"].astype(int)
    grouped["new_paid_users"] = grouped["new_paid_users"].astype(int)
    grouped["net_movement"] = grouped["new_paid_users"] - grouped["churned_users"]
    grouped["churned_users_diverging"] = grouped["churned_users"] * -1
    grouped["apru"] = np.where(grouped["subscribers"] > 0, grouped["revenue"] / grouped["subscribers"], np.nan)
    grouped["month_label"] = make_yyyymm_label(grouped["target_month"])
    grouped["analysis_month"] = _yyyymm_to_timestamp(grouped["target_month"])
    return grouped[
        [
            "target_month",
            "month_label",
            "analysis_month",
            "subscribers",
            "revenue",
            "apru",
            "churn_rate",
            "new_paid_users",
            "churned_users",
            "churned_users_diverging",
            "net_movement",
            "auto_renew_rate",
            "overall_median_survival",
        ]
    ]


def build_snapshot_risk_heatmap_all(
    feature_store_root_hint: str | Path | None = None,
    tab1_artifacts_root_hint: str | Path | None = None,
    *,
    score_month: int = 201704,
) -> pd.DataFrame:
    feature_store_dir = resolve_feature_store_dir(feature_store_root_hint, score_month=score_month)
    tab1_artifact_dir = resolve_tab1_artifacts_dir(tab1_artifacts_root_hint or feature_store_root_hint, score_month=score_month)
    work = _load_risk_source_frame(feature_store_dir, tab1_artifact_dir, score_month).copy()
    work["target_month"] = pd.to_numeric(work["target_month"], errors="coerce").astype("Int64")
    work = work.dropna(subset=["target_month", "msno"]).copy()
    work["target_month"] = work["target_month"].astype("int32")
    work["value_tier"] = _build_value_tier(work)
    work["risk_customer_segment"] = _build_risk_customer_segment(work)

    grouped = (
        work.groupby(["target_month", "value_tier", "risk_customer_segment"], as_index=False)
        .agg(subscribers=("msno", "nunique"))
    )

    frames: list[pd.DataFrame] = []
    for target_month in sorted(grouped["target_month"].astype(int).unique().tolist()):
        scoped = grouped.loc[grouped["target_month"] == target_month].copy()
        heatmap_index = pd.MultiIndex.from_product(
            [TAB1_VISIBLE_VALUE_TIERS, TAB1_RISK_SEGMENT_ORDER],
            names=["value_tier", "risk_customer_segment"],
        )
        scoped = (
            scoped.set_index(["value_tier", "risk_customer_segment"])
            .reindex(heatmap_index, fill_value=0)
            .reset_index()
        )
        scoped["target_month"] = target_month
        scoped["month_label"] = make_yyyymm_label(pd.Series([target_month])).iloc[0]
        scoped["analysis_month"] = _yyyymm_to_timestamp(pd.Series([target_month])).iloc[0]
        scoped["value_tier_order"] = scoped["value_tier"].map({label: idx for idx, label in enumerate(TAB1_VISIBLE_VALUE_TIERS, start=1)}).astype(int)
        scoped["risk_segment_order"] = scoped["risk_customer_segment"].map({label: idx for idx, label in enumerate(TAB1_RISK_SEGMENT_ORDER, start=1)}).astype(int)
        scoped["subscribers"] = scoped["subscribers"].astype(int)
        frames.append(scoped)

    if not frames:
        return pd.DataFrame(
            columns=[
                "target_month",
                "month_label",
                "analysis_month",
                "value_tier",
                "risk_customer_segment",
                "subscribers",
                "value_tier_order",
                "risk_segment_order",
            ]
        )

    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["target_month", "value_tier_order", "risk_segment_order"])
        .reset_index(drop=True)
    )[
        [
            "target_month",
            "month_label",
            "analysis_month",
            "value_tier",
            "risk_customer_segment",
            "subscribers",
            "value_tier_order",
            "risk_segment_order",
        ]
    ]


def run_tab1_dashboard_chart_features(
    feature_store_root_hint: str | Path | None = None,
    tab1_artifacts_root_hint: str | Path | None = None,
    output_dir: str | Path | None = None,
    *,
    score_month: int = 201704,
) -> dict[str, Any]:
    tab1_artifact_dir = resolve_tab1_artifacts_dir(tab1_artifacts_root_hint or feature_store_root_hint, score_month=score_month)
    output_dir = ensure_output_dir(output_dir or tab1_artifact_dir)
    feature_store_dir = resolve_feature_store_dir(feature_store_root_hint, score_month=score_month)

    trend_df = build_trend_monthly_summary(feature_store_root_hint=feature_store_dir, score_month=score_month)
    risk_heatmap_df = build_snapshot_risk_heatmap_all(
        feature_store_root_hint=feature_store_dir,
        tab1_artifacts_root_hint=tab1_artifact_dir,
        score_month=score_month,
    )

    output_paths = {
        "trend_monthly_summary": output_dir / "trend_monthly_summary.parquet",
        "snapshot_risk_heatmap_all": output_dir / "snapshot_risk_heatmap_all.parquet",
        "manifest": output_dir / "tab1_dashboard_chart_features_manifest.json",
    }

    trend_df.to_parquet(output_paths["trend_monthly_summary"], index=False)
    risk_heatmap_df.to_parquet(output_paths["snapshot_risk_heatmap_all"], index=False)
    write_json(
        output_paths["manifest"],
        {
            "score_month": score_month,
            "feature_store_dir": feature_store_dir,
            "artifact_dir": tab1_artifact_dir,
            "output_dir": output_dir,
            "outputs": {name: path.name for name, path in output_paths.items() if name != "manifest"},
            "trend_rows": int(len(trend_df)),
            "risk_heatmap_rows": int(len(risk_heatmap_df)),
            "visible_value_tiers": list(TAB1_VISIBLE_VALUE_TIERS),
            "risk_segment_order": list(TAB1_RISK_SEGMENT_ORDER),
        },
    )

    return {
        "feature_store_dir": feature_store_dir,
        "artifact_dir": tab1_artifact_dir,
        "output_dir": output_dir,
        "trend_monthly_summary": trend_df,
        "snapshot_risk_heatmap_all": risk_heatmap_df,
        "output_paths": output_paths,
    }
