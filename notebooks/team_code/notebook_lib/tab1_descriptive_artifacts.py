from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .common import (
    ensure_columns,
    ensure_output_dir,
    make_yyyymm_label,
    resolve_feature_store_dir,
    write_manifest,
)


TAB1_DIMENSIONS = ("age_segment", "gender_profile", "txn_freq_bucket", "skip_ratio_bucket")
TAB1_SEGMENTS = ("price_segment", "loyalty_segment", "active_segment")


def load_bi_feature_store(feature_store_dir: str | Path) -> pd.DataFrame:
    feature_store_dir = Path(feature_store_dir)
    bi_master_path = feature_store_dir / "bi_feature_master.parquet"
    if bi_master_path.exists():
        return pd.read_parquet(bi_master_path)

    train_path = feature_store_dir / "train_features_bi_all.parquet"
    test_candidates = sorted(feature_store_dir.glob("test_features_bi_*_full.parquet"))
    if train_path.exists() and test_candidates:
        parts = [pd.read_parquet(train_path), *[pd.read_parquet(path) for path in test_candidates]]
        return pd.concat(parts, ignore_index=True)

    raise FileNotFoundError(
        "Khong tim thay bi_feature_master.parquet hoac fallback BI parquet files trong feature store."
    )


def _bucket_txn_freq_proxy(series: pd.Series) -> pd.Series:
    values = series.fillna(-1).astype("float32")
    return pd.Series(
        np.select(
            [
                values <= 0,
                values <= 2,
                values <= 6,
            ],
            ["unknown", "low", "medium"],
            default="high",
        ),
        index=series.index,
    )


def _bucket_skip_ratio(series: pd.Series) -> pd.Series:
    values = series.fillna(0).astype("float32")
    return pd.Series(
        np.select(
            [
                values < 0.2,
                values < 0.5,
            ],
            ["low_skip", "mid_skip"],
            default="high_skip",
        ),
        index=series.index,
    )


def add_tab1_features(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "msno",
        "target_month",
        "is_churn",
        "is_auto_renew",
        "membership_age_days",
        "expected_renewal_amount",
        "skip_ratio",
        "discovery_ratio",
        "price_segment",
        "loyalty_segment",
        "active_segment",
        "age_segment",
        "gender_profile",
    ]
    ensure_columns(df, required, "BI feature store")

    out = df.copy()
    out["target_month"] = out["target_month"].astype("int32")
    out["month_label"] = make_yyyymm_label(out["target_month"])
    out["is_churn"] = out["is_churn"].fillna(0).astype("int8")
    out["is_auto_renew"] = out["is_auto_renew"].fillna(0).astype("int8")
    out["expected_renewal_amount"] = out["expected_renewal_amount"].fillna(0).clip(lower=0).astype("float32")
    out["membership_age_days"] = out["membership_age_days"].fillna(-1).astype("float32")
    out["survival_days_proxy"] = out["membership_age_days"].clip(lower=1).astype("int32")
    out["skip_ratio"] = out["skip_ratio"].fillna(0).clip(lower=0, upper=1).astype("float32")
    out["discovery_ratio"] = out["discovery_ratio"].fillna(0).clip(lower=0, upper=1).astype("float32")

    txn_freq_basis = (
        out["historical_transaction_rows"]
        if "historical_transaction_rows" in out.columns
        else out["transaction_count"]
        if "transaction_count" in out.columns
        else pd.Series(-1, index=out.index, dtype="float32")
    )
    out["txn_freq_bucket"] = _bucket_txn_freq_proxy(txn_freq_basis)
    out["skip_ratio_bucket"] = _bucket_skip_ratio(out["skip_ratio"])

    for col in ("age_segment", "gender_profile", "price_segment", "loyalty_segment", "active_segment"):
        out[col] = out[col].fillna("Unknown").astype(str)

    return out


def _km_points(frame: pd.DataFrame, duration_col: str, event_col: str) -> list[dict]:
    if frame.empty:
        return [{"day": 0, "survival_prob": 1.0, "at_risk": 0, "events": 0}]

    grouped = (
        frame.groupby(duration_col, as_index=False)
        .agg(events=(event_col, "sum"), users=(event_col, "size"))
        .sort_values(duration_col)
        .reset_index(drop=True)
    )
    grouped["censored"] = grouped["users"] - grouped["events"]

    at_risk = int(grouped["users"].sum())
    survival = 1.0
    points = [{"day": 0, "survival_prob": 1.0, "at_risk": at_risk, "events": 0}]

    for row in grouped.itertuples(index=False):
        event_count = int(row.events)
        user_count = int(row.users)
        censored_count = int(row.censored)
        day_value = int(getattr(row, duration_col))
        if at_risk > 0 and event_count > 0:
            survival *= 1.0 - (event_count / at_risk)
        points.append(
            {
                "day": day_value,
                "survival_prob": float(round(survival, 6)),
                "at_risk": at_risk,
                "events": event_count,
            }
        )
        at_risk -= user_count
        at_risk = max(at_risk, 0)
    return points


def build_kpi_artifact(df: pd.DataFrame) -> pd.DataFrame:
    kpi = (
        df.groupby(["target_month", "month_label"], as_index=False)
        .agg(
            total_expiring_users=("msno", "nunique"),
            historical_churn_rate=("is_churn", "mean"),
            median_survival_days_proxy=("survival_days_proxy", "median"),
            auto_renew_rate=("is_auto_renew", "mean"),
            total_expected_renewal_amount=("expected_renewal_amount", "sum"),
        )
        .sort_values("target_month")
        .reset_index(drop=True)
    )
    return kpi


def build_km_artifact(df: pd.DataFrame, dimensions: Iterable[str] = TAB1_DIMENSIONS) -> pd.DataFrame:
    rows: list[dict] = []
    for target_month, month_df in df.groupby("target_month", sort=True):
        month_label = month_df["month_label"].iloc[0]
        for dimension in dimensions:
            if dimension not in month_df.columns:
                continue
            for dimension_value, group_df in month_df.groupby(dimension, sort=True):
                for point in _km_points(group_df, "survival_days_proxy", "is_churn"):
                    rows.append(
                        {
                            "target_month": int(target_month),
                            "month_label": month_label,
                            "dimension": dimension,
                            "dimension_value": str(dimension_value),
                            **point,
                        }
                    )
    return pd.DataFrame(rows)


def build_segment_mix_artifact(df: pd.DataFrame, segments: Iterable[str] = TAB1_SEGMENTS) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    total_users = df.groupby("target_month")["msno"].nunique().rename("total_users").reset_index()
    for segment in segments:
        if segment not in df.columns:
            continue
        segment_df = (
            df.groupby(["target_month", segment], as_index=False)
            .agg(users=("msno", "nunique"))
            .rename(columns={segment: "segment_value"})
        )
        segment_df["segment_type"] = segment
        rows.append(segment_df)

    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["target_month", "segment_value", "users", "segment_type"]
    )
    out = out.merge(total_users, on="target_month", how="left")
    out["user_share"] = np.where(out["total_users"] > 0, out["users"] / out["total_users"], 0.0)
    out["month_label"] = make_yyyymm_label(out["target_month"])
    return out.drop(columns=["total_users"]).sort_values(
        ["target_month", "segment_type", "users"], ascending=[True, True, False]
    ).reset_index(drop=True)


def build_boredom_scatter_artifact(df: pd.DataFrame) -> pd.DataFrame:
    bins = np.linspace(0.0, 1.0, 6)
    labels = ["0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"]
    out = df.copy()
    out["discovery_bin"] = (
        pd.cut(out["discovery_ratio"], bins=bins, labels=labels, include_lowest=True)
        .astype(str)
        .replace("nan", "Unknown")
    )
    out["skip_bin"] = (
        pd.cut(out["skip_ratio"], bins=bins, labels=labels, include_lowest=True)
        .astype(str)
        .replace("nan", "Unknown")
    )
    grouped = (
        out.groupby(["target_month", "discovery_bin", "skip_bin"], as_index=False)
        .agg(
            users=("msno", "nunique"),
            avg_churn_rate=("is_churn", "mean"),
            avg_expected_renewal_amount=("expected_renewal_amount", "mean"),
        )
    )
    grouped["month_label"] = make_yyyymm_label(grouped["target_month"])
    return grouped


def run_tab1_descriptive_artifacts(
    feature_store_root_hint: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict:
    feature_store_dir = resolve_feature_store_dir(feature_store_root_hint)
    output_dir = ensure_output_dir(output_dir or "artifacts_tab1_descriptive")

    raw_bi_df = load_bi_feature_store(feature_store_dir)
    descriptive_df = add_tab1_features(raw_bi_df)

    kpi_df = build_kpi_artifact(descriptive_df)
    km_df = build_km_artifact(descriptive_df)
    segment_mix_df = build_segment_mix_artifact(descriptive_df)
    boredom_df = build_boredom_scatter_artifact(descriptive_df)

    latest_month = int(descriptive_df["target_month"].max())
    latest_snapshot_df = descriptive_df[descriptive_df["target_month"] == latest_month].reset_index(drop=True)

    output_paths = {
        "kpis": output_dir / "tab1_kpis_monthly.parquet",
        "km_curves": output_dir / "tab1_km_curves.parquet",
        "segment_mix": output_dir / "tab1_segment_mix.parquet",
        "boredom_scatter": output_dir / "tab1_boredom_scatter.parquet",
        "latest_snapshot": output_dir / f"tab1_snapshot_{latest_month}.parquet",
    }

    kpi_df.to_parquet(output_paths["kpis"], index=False)
    km_df.to_parquet(output_paths["km_curves"], index=False)
    segment_mix_df.to_parquet(output_paths["segment_mix"], index=False)
    boredom_df.to_parquet(output_paths["boredom_scatter"], index=False)
    latest_snapshot_df.to_parquet(output_paths["latest_snapshot"], index=False)

    manifest_path = write_manifest(
        output_dir=output_dir,
        notebook_name="kkbox-descriptive-tab.ipynb",
        artifact_type="tab1_descriptive",
        input_paths={"feature_store_dir": feature_store_dir},
        output_paths=output_paths,
        metadata={
            "available_months": sorted(descriptive_df["target_month"].astype(int).unique().tolist()),
            "row_count": int(len(descriptive_df)),
            "txn_freq_bucket_basis": (
                "historical_transaction_rows_proxy"
                if "historical_transaction_rows" in descriptive_df.columns
                else "transaction_count_proxy"
                if "transaction_count" in descriptive_df.columns
                else "missing"
            ),
            "dimensions": list(TAB1_DIMENSIONS),
            "segments": list(TAB1_SEGMENTS),
        },
    )
    output_paths["manifest"] = manifest_path

    return {
        "feature_store_dir": feature_store_dir,
        "output_dir": output_dir,
        "kpi_df": kpi_df,
        "km_df": km_df,
        "segment_mix_df": segment_mix_df,
        "boredom_df": boredom_df,
        "latest_snapshot_df": latest_snapshot_df,
        "output_paths": output_paths,
    }
