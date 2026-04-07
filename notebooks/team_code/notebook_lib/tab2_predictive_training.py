from __future__ import annotations

from pathlib import Path
import json

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import average_precision_score, log_loss, roc_auc_score

from .common import (
    ensure_columns,
    ensure_output_dir,
    resolve_feature_store_dir,
    risk_band_from_probability,
    write_manifest,
)


DEFAULT_TRAIN_MONTHS = [201701, 201702]
DEFAULT_VALID_MONTH = 201703
DEFAULT_SCORE_MONTH = 201704
DEFAULT_RANDOM_SEED = 3228


def evaluate_predictions(y_true: pd.Series, y_prob: np.ndarray) -> dict:
    y_prob = np.clip(np.asarray(y_prob, dtype="float64"), 1e-15, 1 - 1e-15)
    metrics = {
        "log_loss": float(log_loss(y_true, y_prob)),
        "pr_auc": float(average_precision_score(y_true, y_prob)),
        "positive_rate": float(np.mean(y_true)),
        "prediction_mean": float(np.mean(y_prob)),
    }
    try:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
    except ValueError:
        metrics["roc_auc"] = None
    return metrics


def make_feature_group_map(features: list[str]) -> dict[str, str]:
    payment_features = {
        "payment_method_id",
        "payment_plan_days",
        "plan_list_price",
        "actual_amount_paid",
        "is_auto_renew",
        "discount",
        "is_discount",
        "amt_per_day",
        "expected_renewal_amount",
        "discount_ratio",
        "payment_to_list_ratio",
        "price_gap",
        "price_gap_per_plan_day",
    }
    churn_history_features = {
        "last_1_is_churn",
        "last_2_is_churn",
        "last_3_is_churn",
        "last_4_is_churn",
        "last_5_is_churn",
        "churn_rate",
        "churn_count",
        "transaction_count",
        "historical_transaction_rows",
        "historical_cancel_count",
        "historical_cancel_rate",
        "historical_auto_renew_rate",
        "weighted_recent_churn",
        "recent_churn_events",
        "days_since_previous_transaction",
        "churn_rate_x_transaction_count",
    }
    listening_features = {
        "count",
        "num_25_sum",
        "num_50_sum",
        "num_75_sum",
        "num_985_sum",
        "num_100_sum",
        "num_unq_sum",
        "total_secs_sum",
        "listen_events_sum",
        "skip_events_sum",
        "skip_ratio",
        "discovery_ratio",
        "completion_ratio",
        "replay_ratio",
        "days_since_last_listen",
        "capped_log_share",
        "secs_per_log",
        "unique_per_log",
        "avg_secs_per_unique",
        "secs_per_plan_day",
        "uniques_per_plan_day",
        "logs_per_plan_day",
        "secs_per_paid_amount",
        "weighted_completion_sum",
        "weighted_completion_per_log",
    }
    loyalty_features = {
        "city",
        "gender",
        "registered_via",
        "age",
        "has_valid_age",
        "days_to_expire",
        "membership_age_days",
        "tenure_months",
        "remaining_plan_ratio",
        "transaction_day",
        "expire_day",
        "registration_year",
        "registration_month",
        "registration_day",
    }
    segment_features = {
        "age_segment_code",
        "price_segment_code",
        "loyalty_segment_code",
        "active_segment_code",
        "skip_segment_code",
        "discovery_segment_code",
        "renewal_segment_code",
        "rfm_segment_code",
        "deal_hunter_flag",
        "free_trial_flag",
        "content_fatigue_flag",
        "high_skip_flag",
        "low_discovery_flag",
        "is_manual_renew",
        "auto_renew_discount_interaction",
        "rfm_total_score",
        "rfm_recency_score",
        "rfm_frequency_score",
        "rfm_monetary_score",
    }

    group_map: dict[str, str] = {}
    for feature in features:
        if feature in payment_features:
            group_map[feature] = "payment_value"
        elif feature in churn_history_features:
            group_map[feature] = "churn_history"
        elif feature in listening_features:
            group_map[feature] = "listening_behavior"
        elif feature in loyalty_features:
            group_map[feature] = "loyalty_member"
        elif feature in segment_features:
            group_map[feature] = "segment_flags"
        else:
            group_map[feature] = "other"
    return group_map


def select_feature_columns(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> list[str]:
    feature_exclude = {
        "msno",
        "is_churn",
        "transaction_date",
        "expire_date",
        "target_month",
        "last_expire_month",
        "transaction_month",
        "expire_month",
    }
    return [
        col
        for col in feature_columns
        if col in train_df.columns and col in test_df.columns and col not in feature_exclude
    ]


def attach_tab2_outputs(
    df: pd.DataFrame,
    raw_prob: np.ndarray,
    final_prob: np.ndarray,
    extra_dimension_cols: list[str],
) -> pd.DataFrame:
    out = df[["msno", "target_month"]].copy()
    if "is_churn" in df.columns:
        out["is_churn"] = df["is_churn"].astype("int8")

    renewal_amount = df["expected_renewal_amount"].fillna(0).clip(lower=0).astype("float32")
    out["expected_renewal_amount"] = renewal_amount
    out["churn_probability_raw"] = np.asarray(raw_prob, dtype="float32")
    out["churn_probability"] = np.asarray(final_prob, dtype="float32")
    out["risk_percentile"] = pd.Series(out["churn_probability"]).rank(method="average", pct=True).astype("float32")
    out["risk_decile"] = np.ceil(out["risk_percentile"] * 10).clip(1, 10).astype("int8")
    out["risk_band"] = risk_band_from_probability(out["churn_probability"])
    out["expected_revenue_at_risk_30d"] = (out["churn_probability"] * renewal_amount).astype("float32")
    out["expected_retained_revenue_30d"] = ((1 - out["churn_probability"]) * renewal_amount).astype("float32")

    keep_cols = [col for col in extra_dimension_cols if col in df.columns]
    if keep_cols:
        out = pd.concat([out.reset_index(drop=True), df[keep_cols].reset_index(drop=True)], axis=1)
    return out


def run_tab2_predictive_training(
    feature_store_root_hint: str | Path | None = None,
    output_dir: str | Path | None = None,
    train_months: list[int] | None = None,
    valid_month: int = DEFAULT_VALID_MONTH,
    score_month: int = DEFAULT_SCORE_MONTH,
    random_seed: int = DEFAULT_RANDOM_SEED,
) -> dict:
    train_months = train_months or list(DEFAULT_TRAIN_MONTHS)
    output_dir = ensure_output_dir(output_dir or "artifacts_tab2_predictive")
    feature_store_dir = resolve_feature_store_dir(feature_store_root_hint, score_month=score_month)

    train_df = pd.read_parquet(feature_store_dir / "train_features_bi_all.parquet")
    test_df = pd.read_parquet(feature_store_dir / f"test_features_bi_{score_month}_full.parquet")
    feature_columns = pd.read_csv(feature_store_dir / "feature_columns.csv")["feature"].tolist()
    bi_dimensions = pd.read_csv(feature_store_dir / "bi_dimension_columns.csv")["dimension"].tolist()

    ensure_columns(train_df, ("msno", "target_month", "is_churn", "expected_renewal_amount"), "train BI parquet")
    ensure_columns(test_df, ("msno", "target_month", "expected_renewal_amount"), "test BI parquet")

    feature_cols = select_feature_columns(train_df, test_df, feature_columns)
    if not feature_cols:
        raise ValueError("Khong con feature nao de train sau khi validate schema va loai bo cot time-id.")

    train_fit_df = train_df[train_df["target_month"].isin(train_months)].reset_index(drop=True)
    valid_df = train_df[train_df["target_month"] == valid_month].reset_index(drop=True)
    full_train_df = train_df[train_df["target_month"].isin(train_months + [valid_month])].reset_index(drop=True)
    score_df = test_df[test_df["target_month"] == score_month].reset_index(drop=True)

    if train_fit_df.empty or valid_df.empty or full_train_df.empty or score_df.empty:
        raise ValueError(
            "Time split rong. Kiem tra lai target_month trong feature store hoac notebook config."
        )

    X_train = train_fit_df[feature_cols].fillna(-1)
    y_train = train_fit_df["is_churn"].astype("int8")
    X_valid = valid_df[feature_cols].fillna(-1)
    y_valid = valid_df["is_churn"].astype("int8")
    X_full = full_train_df[feature_cols].fillna(-1)
    y_full = full_train_df["is_churn"].astype("int8")
    X_score = score_df[feature_cols].fillna(-1)

    pos = float(y_train.sum())
    neg = float(len(y_train) - y_train.sum())
    scale_pos_weight = max(neg / max(pos, 1.0), 1.0)

    lgb_params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "learning_rate": 0.03,
        "num_leaves": 96,
        "min_data_in_leaf": 120,
        "feature_fraction": 0.85,
        "bagging_fraction": 0.85,
        "bagging_freq": 1,
        "lambda_l1": 0.5,
        "lambda_l2": 2.0,
        "scale_pos_weight": scale_pos_weight,
        "verbosity": -1,
        "seed": random_seed,
    }

    train_set = lgb.Dataset(X_train, label=y_train)
    valid_set = lgb.Dataset(X_valid, label=y_valid, reference=train_set)
    model_valid = lgb.train(
        params=lgb_params,
        train_set=train_set,
        num_boost_round=2000,
        valid_sets=[train_set, valid_set],
        valid_names=["train", "valid"],
        callbacks=[lgb.early_stopping(stopping_rounds=100), lgb.log_evaluation(period=100)],
    )

    valid_raw_pred = np.clip(model_valid.predict(X_valid, num_iteration=model_valid.best_iteration), 1e-15, 1 - 1e-15)
    raw_metrics = evaluate_predictions(y_valid, valid_raw_pred)

    use_calibrated = False
    calibrator = None
    calibrated_metrics = raw_metrics
    final_valid_pred = valid_raw_pred
    if y_valid.nunique() > 1:
        calibrator = IsotonicRegression(out_of_bounds="clip")
        calibrator.fit(valid_raw_pred, y_valid)
        valid_calibrated_pred = np.clip(calibrator.transform(valid_raw_pred), 1e-15, 1 - 1e-15)
        calibrated_metrics = evaluate_predictions(y_valid, valid_calibrated_pred)
        use_calibrated = calibrated_metrics["log_loss"] < raw_metrics["log_loss"]
        final_valid_pred = valid_calibrated_pred if use_calibrated else valid_raw_pred

    best_iteration = int(model_valid.best_iteration or 200)
    model_full = lgb.train(
        params=lgb_params,
        train_set=lgb.Dataset(X_full, label=y_full),
        num_boost_round=best_iteration,
        valid_sets=[lgb.Dataset(X_full, label=y_full)],
        valid_names=["full_train"],
        callbacks=[lgb.log_evaluation(period=200)],
    )

    score_raw_pred = np.clip(model_full.predict(X_score, num_iteration=best_iteration), 1e-15, 1 - 1e-15)
    if use_calibrated and calibrator is not None:
        score_final_pred = np.clip(calibrator.transform(score_raw_pred), 1e-15, 1 - 1e-15)
    else:
        score_final_pred = score_raw_pred

    extra_dimension_cols = [
        "price_segment",
        "renewal_segment",
        "loyalty_segment",
        "active_segment",
        "skip_segment",
        "discovery_segment",
        "rfm_segment",
        "bi_segment_name",
        "gender_profile",
    ]
    valid_scored = attach_tab2_outputs(valid_df, valid_raw_pred, final_valid_pred, extra_dimension_cols)
    score_scored = attach_tab2_outputs(score_df, score_raw_pred, score_final_pred, extra_dimension_cols)

    if "bi_segment_name" not in score_scored.columns:
        score_scored["bi_segment_name"] = "Unknown"

    segment_risk_summary = (
        score_scored.groupby(["target_month", "bi_segment_name"], as_index=False)
        .agg(
            users=("msno", "nunique"),
            avg_churn_probability=("churn_probability", "mean"),
            total_expected_revenue_at_risk_30d=("expected_revenue_at_risk_30d", "sum"),
            total_expected_retained_revenue_30d=("expected_retained_revenue_30d", "sum"),
            total_expected_renewal_amount=("expected_renewal_amount", "sum"),
        )
        .sort_values(["total_expected_revenue_at_risk_30d", "avg_churn_probability"], ascending=[False, False])
        .reset_index(drop=True)
    )

    feature_group_map = make_feature_group_map(feature_cols)
    feature_importance = pd.DataFrame(
        {
            "feature": feature_cols,
            "importance_gain": model_full.feature_importance(importance_type="gain"),
            "importance_split": model_full.feature_importance(importance_type="split"),
        }
    )
    feature_importance["feature_group"] = feature_importance["feature"].map(feature_group_map)
    feature_importance = feature_importance.sort_values("importance_gain", ascending=False).reset_index(drop=True)

    feature_group_importance = (
        feature_importance.groupby("feature_group", as_index=False)
        .agg(
            importance_gain=("importance_gain", "sum"),
            importance_split=("importance_split", "sum"),
            feature_count=("feature", "count"),
        )
        .sort_values("importance_gain", ascending=False)
        .reset_index(drop=True)
    )

    validation_metrics_payload = {
        "train_months": train_months,
        "valid_month": valid_month,
        "score_month": score_month,
        "best_iteration": best_iteration,
        "scale_pos_weight": float(scale_pos_weight),
        "use_calibrated_output": bool(use_calibrated),
        "raw_metrics": raw_metrics,
        "calibrated_metrics": calibrated_metrics,
        "selected_metrics": calibrated_metrics if use_calibrated else raw_metrics,
        "feature_count": len(feature_cols),
    }
    model_summary_payload = {
        "feature_store_dir": str(feature_store_dir),
        "output_dir": str(output_dir),
        "feature_count": len(feature_cols),
        "feature_dimensions_available": bi_dimensions,
        "scored_validation_rows": int(len(valid_scored)),
        "scored_test_rows": int(len(score_scored)),
        "top_feature_groups": feature_group_importance.to_dict(orient="records"),
        "excluded_features": sorted(set(feature_columns) - set(feature_cols)),
    }

    output_paths = {
        "validation_metrics": output_dir / "tab2_validation_metrics.json",
        "feature_columns_used": output_dir / "tab2_feature_columns_used.csv",
        "feature_importance": output_dir / "tab2_feature_importance_lightgbm.csv",
        "feature_group_importance": output_dir / "tab2_feature_group_importance.csv",
        "valid_scored": output_dir / f"tab2_valid_scored_{valid_month}.parquet",
        "test_scored": output_dir / f"tab2_test_scored_{score_month}.parquet",
        "segment_risk_summary": output_dir / f"tab2_segment_risk_summary_{score_month}.parquet",
        "model_summary": output_dir / "tab2_model_summary.json",
        "model": output_dir / "tab2_lightgbm_model.txt",
    }

    output_paths["validation_metrics"].write_text(
        json.dumps(validation_metrics_payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    output_paths["feature_columns_used"].write_text(
        "feature\n" + "\n".join(feature_cols) + "\n",
        encoding="utf-8",
    )
    feature_importance.to_csv(output_paths["feature_importance"], index=False)
    feature_group_importance.to_csv(output_paths["feature_group_importance"], index=False)
    valid_scored.to_parquet(output_paths["valid_scored"], index=False)
    score_scored.to_parquet(output_paths["test_scored"], index=False)
    segment_risk_summary.to_parquet(output_paths["segment_risk_summary"], index=False)
    output_paths["model_summary"].write_text(
        json.dumps(model_summary_payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    model_full.save_model(str(output_paths["model"]))

    manifest_path = write_manifest(
        output_dir=output_dir,
        notebook_name="kkbox-train-predictive-tab.ipynb",
        artifact_type="tab2_predictive",
        input_paths={
            "feature_store_dir": feature_store_dir,
            "train_bi": feature_store_dir / "train_features_bi_all.parquet",
            "test_bi": feature_store_dir / f"test_features_bi_{score_month}_full.parquet",
            "feature_columns": feature_store_dir / "feature_columns.csv",
        },
        output_paths=output_paths,
        metadata={
            "train_months": train_months,
            "valid_month": valid_month,
            "score_month": score_month,
            "feature_count": len(feature_cols),
            "best_iteration": best_iteration,
            "use_calibrated_output": bool(use_calibrated),
        },
    )
    output_paths["manifest"] = manifest_path

    return {
        "feature_store_dir": feature_store_dir,
        "output_dir": output_dir,
        "feature_cols": feature_cols,
        "valid_scored": valid_scored,
        "score_scored": score_scored,
        "segment_risk_summary": segment_risk_summary,
        "feature_importance": feature_importance,
        "feature_group_importance": feature_group_importance,
        "validation_metrics_payload": validation_metrics_payload,
        "model_summary_payload": model_summary_payload,
        "output_paths": output_paths,
    }
