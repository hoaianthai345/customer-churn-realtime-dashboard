from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd

from .common import (
    ensure_columns,
    ensure_output_dir,
    resolve_feature_store_dir,
    resolve_tab2_artifacts_dir,
    risk_band_from_probability,
    write_json,
    write_manifest,
)


DEFAULT_SCORE_MONTH = 201704
DEFAULT_SENSITIVITY_SHARES = [0.05, 0.10, 0.20, 0.30, 0.40, 0.50]
DEFAULT_SCENARIO_CONFIG = {
    "manual_to_auto_share": 0.30,
    "upsell_share": 0.20,
    "engagement_share": 0.25,
    "manual_to_auto_cost_per_user": 0.0,
    "upsell_cost_per_user": 0.0,
    "engagement_cost_per_user": 0.0,
}


def _clip_share(value: float | int | None) -> float:
    if value is None:
        return 0.0
    return float(np.clip(float(value), 0.0, 1.0))


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


def load_feature_snapshot(feature_store_dir: str | Path, score_month: int) -> pd.DataFrame:
    feature_store_dir = Path(feature_store_dir)
    score_path = feature_store_dir / f"test_features_bi_{score_month}_full.parquet"
    if score_path.exists():
        df = pd.read_parquet(score_path)
        if "target_month" in df.columns:
            df = df[df["target_month"] == score_month].reset_index(drop=True)
        return df

    master_path = feature_store_dir / "bi_feature_master.parquet"
    if master_path.exists():
        df = pd.read_parquet(master_path)
        return df[df["target_month"] == score_month].reset_index(drop=True)

    raise FileNotFoundError(
        f"Khong tim thay snapshot BI cho score_month={score_month} trong feature store."
    )


def _prepare_baseline_dataframe(feature_df: pd.DataFrame, scored_df: pd.DataFrame) -> pd.DataFrame:
    ensure_columns(
        scored_df,
        ("msno", "target_month", "churn_probability", "expected_renewal_amount"),
        "Tab 2 scored artifact",
    )
    ensure_columns(feature_df, ("msno", "target_month"), "feature snapshot")

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
    if "risk_band" not in merged.columns:
        merged["risk_band"] = risk_band_from_probability(merged["baseline_churn_probability"])
    else:
        merged["risk_band"] = merged["risk_band"].fillna("Unknown").astype(str)

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

    if "skip_ratio" not in merged.columns:
        merged["skip_ratio"] = 0.0
    if "discovery_ratio" not in merged.columns:
        merged["discovery_ratio"] = 0.0
    merged["skip_ratio"] = merged["skip_ratio"].fillna(0).astype("float32").clip(lower=0.0, upper=1.0)
    merged["discovery_ratio"] = merged["discovery_ratio"].fillna(0).astype("float32").clip(lower=0.0, upper=1.0)

    if "is_manual_renew" in merged.columns:
        merged["is_manual_renew"] = merged["is_manual_renew"].fillna(0).astype("int8")
    elif "renewal_segment" in merged.columns:
        merged["is_manual_renew"] = merged["renewal_segment"].eq("Pay_Manual").astype("int8")
    elif "is_auto_renew" in merged.columns:
        merged["is_manual_renew"] = (1 - merged["is_auto_renew"].fillna(0).astype("int8")).clip(lower=0, upper=1)
    else:
        merged["is_manual_renew"] = 0

    if "deal_hunter_flag" not in merged.columns:
        if "price_segment" in merged.columns:
            merged["deal_hunter_flag"] = merged["price_segment"].eq("Deal Hunter < 4.5").astype("int8")
        elif "amt_per_day" in merged.columns:
            merged["deal_hunter_flag"] = (
                (merged["amt_per_day"].fillna(0) > 0) & (merged["amt_per_day"].fillna(0) < 4.5)
            ).astype("int8")
        else:
            merged["deal_hunter_flag"] = 0
    else:
        merged["deal_hunter_flag"] = merged["deal_hunter_flag"].fillna(0).astype("int8")

    if "free_trial_flag" not in merged.columns:
        merged["free_trial_flag"] = (
            merged["price_segment"].eq("Free Trial / Zero Pay")
            | merged["expected_renewal_amount"].le(0)
        ).astype("int8")
    else:
        merged["free_trial_flag"] = merged["free_trial_flag"].fillna(0).astype("int8")

    if "high_skip_flag" not in merged.columns:
        merged["high_skip_flag"] = merged["skip_ratio"].ge(0.5).astype("int8")
    else:
        merged["high_skip_flag"] = merged["high_skip_flag"].fillna(0).astype("int8")

    if "low_discovery_flag" not in merged.columns:
        merged["low_discovery_flag"] = merged["discovery_ratio"].lt(0.2).astype("int8")
    else:
        merged["low_discovery_flag"] = merged["low_discovery_flag"].fillna(0).astype("int8")

    if "content_fatigue_flag" not in merged.columns:
        merged["content_fatigue_flag"] = (
            merged["high_skip_flag"].eq(1) & merged["low_discovery_flag"].eq(1)
        ).astype("int8")
    else:
        merged["content_fatigue_flag"] = merged["content_fatigue_flag"].fillna(0).astype("int8")

    merged["eligible_auto"] = merged["is_manual_renew"].eq(1).astype("int8")
    merged["eligible_upsell"] = (
        merged["deal_hunter_flag"].eq(1) | merged["free_trial_flag"].eq(1)
    ).astype("int8")
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


def estimate_lever_parameters(df: pd.DataFrame) -> dict:
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
    standard_amounts = df.loc[
        df["price_segment"].eq("Standard 4.5-6.5"), "expected_renewal_amount"
    ]
    premium_amounts = df.loc[
        df["price_segment"].eq("Premium >= 6.5"), "expected_renewal_amount"
    ]
    standard_target_amount = _first_non_empty_median(
        [standard_amounts, positive_amounts],
        default=0.0,
    )
    premium_target_amount = _first_non_empty_median(
        [premium_amounts, positive_amounts],
        default=standard_target_amount,
    )
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
        "manual_mean_probability": manual_mean,
        "auto_mean_probability": auto_mean,
        "fatigue_mean_probability": fatigue_mean,
        "healthy_mean_probability": healthy_mean,
        "deal_mean_probability": deal_mean,
        "paid_stable_mean_probability": paid_stable_mean,
    }


def _normalize_config(config: dict | None) -> dict:
    normalized = deepcopy(DEFAULT_SCENARIO_CONFIG)
    if config:
        normalized.update(config)
    for key in (
        "manual_to_auto_share",
        "upsell_share",
        "engagement_share",
    ):
        normalized[key] = _clip_share(normalized.get(key))
    for key in (
        "manual_to_auto_cost_per_user",
        "upsell_cost_per_user",
        "engagement_cost_per_user",
    ):
        normalized[key] = float(max(float(normalized.get(key, 0.0)), 0.0))
    return normalized


def _upsell_target_amount(df: pd.DataFrame, params: dict) -> pd.Series:
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
    scenario_config: dict | None = None,
    lever_parameters: dict | None = None,
) -> pd.DataFrame:
    config = _normalize_config(scenario_config)
    params = lever_parameters or estimate_lever_parameters(baseline_df)
    df = baseline_df.copy()

    df["auto_treatment_weight"] = (
        df["eligible_auto"].astype("float32") * config["manual_to_auto_share"]
    )
    df["engagement_treatment_weight"] = (
        df["eligible_engagement"].astype("float32") * config["engagement_share"]
    )
    df["upsell_treatment_weight"] = (
        df["eligible_upsell"].astype("float32") * config["upsell_share"]
    )

    df["auto_prob_delta_if_treated"] = float(params["auto_prob_delta_if_treated"])
    df["engagement_prob_delta_if_treated"] = float(params["engagement_prob_delta_if_treated"])

    target_amount = _upsell_target_amount(df, params)
    df["upsell_target_amount_if_treated"] = target_amount
    df["upsell_amount_delta_if_treated"] = (
        (target_amount - df["expected_renewal_amount"]).clip(lower=0.0).astype("float32")
    )

    uplift_ratio = (
        df["upsell_amount_delta_if_treated"]
        / np.maximum(target_amount.to_numpy(dtype="float32"), 1.0)
    ).astype("float32")
    price_shock_penalty = np.clip(0.08 * uplift_ratio, 0.0, 0.10)
    df["upsell_prob_delta_if_treated"] = (
        np.clip(float(params["upsell_group_gap"]) + price_shock_penalty, -0.15, 0.12)
    ).astype("float32")

    df["auto_probability_delta"] = (
        df["auto_treatment_weight"] * df["auto_prob_delta_if_treated"]
    ).astype("float32")
    df["engagement_probability_delta"] = (
        df["engagement_treatment_weight"] * df["engagement_prob_delta_if_treated"]
    ).astype("float32")
    df["upsell_probability_delta"] = (
        df["upsell_treatment_weight"] * df["upsell_prob_delta_if_treated"]
    ).astype("float32")
    df["upsell_amount_delta"] = (
        df["upsell_treatment_weight"] * df["upsell_amount_delta_if_treated"]
    ).astype("float32")

    df["simulated_expected_renewal_amount"] = (
        df["expected_renewal_amount"] + df["upsell_amount_delta"]
    ).astype("float32")
    df["simulated_churn_probability"] = np.clip(
        df["baseline_churn_probability"]
        + df["auto_probability_delta"]
        + df["engagement_probability_delta"]
        + df["upsell_probability_delta"],
        1e-4,
        1 - 1e-4,
    ).astype("float32")
    df["simulated_revenue_at_risk_30d"] = (
        df["simulated_churn_probability"] * df["simulated_expected_renewal_amount"]
    ).astype("float32")
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

    df["manual_to_auto_cost"] = (
        df["auto_treatment_weight"] * config["manual_to_auto_cost_per_user"]
    ).astype("float32")
    df["upsell_cost"] = (
        df["upsell_treatment_weight"] * config["upsell_cost_per_user"]
    ).astype("float32")
    df["engagement_cost"] = (
        df["engagement_treatment_weight"] * config["engagement_cost_per_user"]
    ).astype("float32")
    df["campaign_cost"] = (
        df["manual_to_auto_cost"] + df["upsell_cost"] + df["engagement_cost"]
    ).astype("float32")
    return df


def summarize_scenario(member_level_df: pd.DataFrame, scenario_config: dict, lever_parameters: dict) -> dict:
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
        "saved_revenue_from_risk_reduction_30d": float(
            member_level_df["risk_effect_revenue_delta_30d"].clip(lower=0).sum()
        ),
        "revenue_loss_from_risk_increase_30d": float(
            (-member_level_df["risk_effect_revenue_delta_30d"]).clip(lower=0).sum()
        ),
        "incremental_upsell_revenue_30d": float(
            member_level_df["upsell_effect_revenue_delta_30d"].sum()
        ),
        "campaign_cost_30d": float(member_level_df["campaign_cost"].sum()),
        "scenario_config": deepcopy(scenario_config),
        "lever_parameters": deepcopy(lever_parameters),
    }
    summary["revenue_at_risk_delta_30d"] = (
        summary["simulated_revenue_at_risk_30d"] - summary["baseline_revenue_at_risk_30d"]
    )
    summary["retained_revenue_delta_30d"] = (
        summary["simulated_retained_revenue_30d"] - summary["baseline_retained_revenue_30d"]
    )
    summary["net_value_after_cost_30d"] = (
        summary["retained_revenue_delta_30d"] - summary["campaign_cost_30d"]
    )
    return summary


def build_lever_summary(
    baseline_df: pd.DataFrame,
    scenario_config: dict,
    lever_parameters: dict,
) -> pd.DataFrame:
    lever_configs = {
        "combined": scenario_config,
        "manual_to_auto": {
            **DEFAULT_SCENARIO_CONFIG,
            "manual_to_auto_share": scenario_config["manual_to_auto_share"],
            "manual_to_auto_cost_per_user": scenario_config["manual_to_auto_cost_per_user"],
        },
        "upsell": {
            **DEFAULT_SCENARIO_CONFIG,
            "upsell_share": scenario_config["upsell_share"],
            "upsell_cost_per_user": scenario_config["upsell_cost_per_user"],
        },
        "engagement": {
            **DEFAULT_SCENARIO_CONFIG,
            "engagement_share": scenario_config["engagement_share"],
            "engagement_cost_per_user": scenario_config["engagement_cost_per_user"],
        },
    }

    rows: list[dict] = []
    for lever_name, lever_config in lever_configs.items():
        member_level = simulate_prescriptive_scenario(
            baseline_df,
            scenario_config=lever_config,
            lever_parameters=lever_parameters,
        )
        summary = summarize_scenario(member_level, _normalize_config(lever_config), lever_parameters)
        rows.append(
            {
                "lever_name": lever_name,
                "population_users": summary["population_users"],
                "baseline_avg_churn_probability": summary["baseline_avg_churn_probability"],
                "simulated_avg_churn_probability": summary["simulated_avg_churn_probability"],
                "baseline_revenue_at_risk_30d": summary["baseline_revenue_at_risk_30d"],
                "simulated_revenue_at_risk_30d": summary["simulated_revenue_at_risk_30d"],
                "revenue_at_risk_delta_30d": summary["revenue_at_risk_delta_30d"],
                "baseline_retained_revenue_30d": summary["baseline_retained_revenue_30d"],
                "simulated_retained_revenue_30d": summary["simulated_retained_revenue_30d"],
                "retained_revenue_delta_30d": summary["retained_revenue_delta_30d"],
                "saved_revenue_from_risk_reduction_30d": summary["saved_revenue_from_risk_reduction_30d"],
                "incremental_upsell_revenue_30d": summary["incremental_upsell_revenue_30d"],
                "campaign_cost_30d": summary["campaign_cost_30d"],
                "net_value_after_cost_30d": summary["net_value_after_cost_30d"],
                "share": float(
                    max(
                        lever_config.get("manual_to_auto_share", 0.0),
                        lever_config.get("upsell_share", 0.0),
                        lever_config.get("engagement_share", 0.0),
                    )
                ),
            }
        )
    return pd.DataFrame(rows)


def build_segment_impact(member_level_df: pd.DataFrame) -> pd.DataFrame:
    segment_col = "bi_segment_name" if "bi_segment_name" in member_level_df.columns else "renewal_segment"
    grouped = (
        member_level_df.groupby(["target_month", segment_col], as_index=False)
        .agg(
            users=("msno", "nunique"),
            baseline_avg_churn_probability=("baseline_churn_probability", "mean"),
            simulated_avg_churn_probability=("simulated_churn_probability", "mean"),
            baseline_revenue_at_risk_30d=("baseline_revenue_at_risk_30d", "sum"),
            simulated_revenue_at_risk_30d=("simulated_revenue_at_risk_30d", "sum"),
            baseline_retained_revenue_30d=("baseline_retained_revenue_30d", "sum"),
            simulated_retained_revenue_30d=("simulated_retained_revenue_30d", "sum"),
            campaign_cost_30d=("campaign_cost", "sum"),
        )
        .rename(columns={segment_col: "segment_name"})
        .sort_values("simulated_revenue_at_risk_30d", ascending=False)
        .reset_index(drop=True)
    )
    grouped["revenue_at_risk_delta_30d"] = (
        grouped["simulated_revenue_at_risk_30d"] - grouped["baseline_revenue_at_risk_30d"]
    )
    grouped["retained_revenue_delta_30d"] = (
        grouped["simulated_retained_revenue_30d"] - grouped["baseline_retained_revenue_30d"]
    )
    return grouped


def build_population_risk_shift(member_level_df: pd.DataFrame) -> pd.DataFrame:
    bins = np.linspace(0.0, 1.0, 11)
    labels = [f"{left:.1f}-{right:.1f}" for left, right in zip(bins[:-1], bins[1:])]

    baseline_bucket = pd.cut(
        member_level_df["baseline_churn_probability"],
        bins=bins,
        labels=labels,
        include_lowest=True,
    )
    simulated_bucket = pd.cut(
        member_level_df["simulated_churn_probability"],
        bins=bins,
        labels=labels,
        include_lowest=True,
    )

    baseline_dist = (
        baseline_bucket.value_counts(sort=False)
        .rename_axis("probability_bin")
        .reset_index(name="users")
    )
    baseline_dist["state"] = "baseline"

    simulated_dist = (
        simulated_bucket.value_counts(sort=False)
        .rename_axis("probability_bin")
        .reset_index(name="users")
    )
    simulated_dist["state"] = "simulated"

    risk_band_baseline = (
        member_level_df["risk_band"].fillna("Unknown").value_counts().rename_axis("risk_band").reset_index(name="users")
    )
    risk_band_baseline["state"] = "baseline"
    risk_band_simulated = (
        member_level_df["simulated_risk_band"]
        .fillna("Unknown")
        .value_counts()
        .rename_axis("risk_band")
        .reset_index(name="users")
    )
    risk_band_simulated["state"] = "simulated"

    bin_df = pd.concat([baseline_dist, simulated_dist], ignore_index=True)
    bin_df["target_month"] = int(member_level_df["target_month"].iloc[0])
    band_df = pd.concat([risk_band_baseline, risk_band_simulated], ignore_index=True)
    band_df["target_month"] = int(member_level_df["target_month"].iloc[0])

    band_df = band_df.rename(columns={"risk_band": "probability_bin"})
    band_df["bin_type"] = "risk_band"
    bin_df["bin_type"] = "probability_bin"
    return pd.concat([bin_df, band_df], ignore_index=True, sort=False)


def build_sensitivity_table(
    baseline_df: pd.DataFrame,
    lever_parameters: dict,
    shares: list[float] | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
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


def run_tab3_prescriptive_artifacts(
    feature_store_root_hint: str | Path | None = None,
    tab2_root_hint: str | Path | None = None,
    output_dir: str | Path | None = None,
    score_month: int = DEFAULT_SCORE_MONTH,
    scenario_config: dict | None = None,
) -> dict:
    feature_store_dir = resolve_feature_store_dir(feature_store_root_hint, score_month=score_month)
    tab2_dir = resolve_tab2_artifacts_dir(tab2_root_hint, score_month=score_month)
    output_dir = ensure_output_dir(output_dir or "artifacts_tab3_prescriptive")

    feature_snapshot = load_feature_snapshot(feature_store_dir, score_month=score_month)
    scored_df = pd.read_parquet(tab2_dir / f"tab2_test_scored_{score_month}.parquet")
    baseline_df = _prepare_baseline_dataframe(feature_snapshot, scored_df)

    lever_parameters = estimate_lever_parameters(baseline_df)
    normalized_config = _normalize_config(scenario_config)
    member_level_df = simulate_prescriptive_scenario(
        baseline_df,
        scenario_config=normalized_config,
        lever_parameters=lever_parameters,
    )
    summary_payload = summarize_scenario(member_level_df, normalized_config, lever_parameters)
    lever_summary_df = build_lever_summary(
        baseline_df,
        scenario_config=normalized_config,
        lever_parameters=lever_parameters,
    )
    segment_impact_df = build_segment_impact(member_level_df)
    risk_shift_df = build_population_risk_shift(member_level_df)
    sensitivity_df = build_sensitivity_table(baseline_df, lever_parameters=lever_parameters)

    output_paths = {
        "member_level": output_dir / f"tab3_scenario_member_level_{score_month}.parquet",
        "summary": output_dir / f"tab3_scenario_summary_{score_month}.json",
        "lever_summary": output_dir / f"tab3_lever_summary_{score_month}.parquet",
        "segment_impact": output_dir / f"tab3_segment_impact_{score_month}.parquet",
        "risk_shift": output_dir / f"tab3_population_risk_shift_{score_month}.parquet",
        "sensitivity": output_dir / f"tab3_sensitivity_{score_month}.parquet",
    }

    member_level_df.to_parquet(output_paths["member_level"], index=False)
    write_json(output_paths["summary"], summary_payload)
    lever_summary_df.to_parquet(output_paths["lever_summary"], index=False)
    segment_impact_df.to_parquet(output_paths["segment_impact"], index=False)
    risk_shift_df.to_parquet(output_paths["risk_shift"], index=False)
    sensitivity_df.to_parquet(output_paths["sensitivity"], index=False)

    manifest_path = write_manifest(
        output_dir=output_dir,
        notebook_name="kkbox-simulation-2.ipynb",
        artifact_type="tab3_prescriptive",
        input_paths={
            "feature_store_dir": feature_store_dir,
            "feature_snapshot": feature_store_dir / f"test_features_bi_{score_month}_full.parquet",
            "tab2_artifacts_dir": tab2_dir,
            "tab2_test_scored": tab2_dir / f"tab2_test_scored_{score_month}.parquet",
        },
        output_paths=output_paths,
        metadata={
            "score_month": score_month,
            "scenario_config": normalized_config,
            "lever_parameters": lever_parameters,
            "population_users": int(member_level_df["msno"].nunique()),
        },
    )
    output_paths["manifest"] = manifest_path

    return {
        "feature_store_dir": feature_store_dir,
        "tab2_artifacts_dir": tab2_dir,
        "output_dir": output_dir,
        "baseline_df": baseline_df,
        "member_level_df": member_level_df,
        "summary_payload": summary_payload,
        "lever_summary": lever_summary_df,
        "segment_impact": segment_impact_df,
        "risk_shift": risk_shift_df,
        "sensitivity": sensitivity_df,
        "output_paths": output_paths,
    }
