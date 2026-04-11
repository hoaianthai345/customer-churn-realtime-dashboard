from __future__ import annotations

import json
import os
import re
from calendar import monthrange
from copy import deepcopy
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


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
DEFAULT_TAB3_SCENARIO_ID = "default"

ALLOWED_SEGMENT_TYPES = {"price_segment", "loyalty_segment", "active_segment"}
TAB1_DIMENSION_FIELD_MAP = {
    "age": "age_segment",
    "age_bucket": "age_segment",
    "age_segment": "age_segment",
    "gender": "gender_profile",
    "gender_bucket": "gender_profile",
    "gender_profile": "gender_profile",
    "txn_freq": "txn_freq_bucket",
    "txn_freq_bucket": "txn_freq_bucket",
    "skip_ratio": "skip_ratio_bucket",
    "skip_ratio_bucket": "skip_ratio_bucket",
}
TAB1_VISIBLE_VALUE_TIERS = ("Free Trial", "Deal Hunter", "Standard")
TAB1_RISK_SEGMENT_ORDER = ("At Risk", "Watchlist", "Stable")
TAB1_EXCLUDED_TREND_MONTHS = frozenset({201704})
TAB1_BEHAVIOR_BIN_EDGES = [0.0, 0.2, 0.4, 0.6, 0.8, 1.000001]
TAB1_BEHAVIOR_BIN_LABELS = ("0-20%", "20-40%", "40-60%", "60-80%", "80-100%")
TAB1_BEHAVIOR_BIN_MIDPOINTS = {
    "0-20%": 0.10,
    "20-40%": 0.30,
    "40-60%": 0.50,
    "60-80%": 0.70,
    "80-100%": 0.90,
}

TAB2_EXECUTIVE_RISK_ORDER = ("High", "Medium", "Low", "Unknown")
TAB2_EXECUTIVE_RISK_LONG_LABELS = {
    "High": "High (Prob >= 0.6)",
    "Medium": "Medium (Prob 0.4 - 0.6)",
    "Low": "Low (Prob < 0.4)",
    "Unknown": "Unknown",
}
TAB2_EXECUTIVE_MATRIX_MIN_USER_COUNT = 10
TAB2_EXECUTIVE_MATRIX_MAX_RENEWAL_AMOUNT = 250.0
TAB2_FEATURE_GROUP_LABELS = {
    "segment_flags": "Dấu hiệu phân khúc",
    "payment_value": "Thanh toán & giá trị",
    "churn_history": "Lịch sử churn",
    "listening_behavior": "Hành vi nghe",
    "loyalty_member": "Vòng đời thành viên",
    "other": "Tín hiệu khác",
    "base_risk": "Nền rủi ro",
}
TAB2_RECOMMENDED_ACTIONS = {
    "Manual Renewal": "Ưu đãi chuyển sang auto-renew và nhắc thanh toán trước kỳ gia hạn.",
    "High Skip Ratio": "Làm mới playlist và gợi ý nội dung hợp gu để kéo khách quay lại nghe.",
    "Low Discovery": "Đẩy khám phá nội dung mới trên home feed và chiến dịch tái kích hoạt.",
    "Price Sensitivity": "Đề xuất gói linh hoạt hoặc voucher giữ chân ngắn hạn.",
    "Early Lifecycle": "Chuỗi onboarding 14 ngày và nhắc giá trị sớm sau đăng ký.",
    "Mixed Behavioral Risk": "CSKH gọi lại kết hợp ưu đãi cá nhân hóa cho nhóm rủi ro hỗn hợp.",
}

TAB2_SCORE_COLUMNS = (
    "msno",
    "target_month",
    "bi_segment_name",
    "renewal_segment",
    "skip_segment",
    "discovery_segment",
    "price_segment",
    "loyalty_segment",
    "active_segment",
    "rfm_segment",
    "risk_band",
    "churn_probability",
    "expected_renewal_amount",
    "expected_revenue_at_risk_30d",
    "expected_retained_revenue_30d",
)

TAB3_SCORED_COLUMNS = (
    "msno",
    "target_month",
    "risk_band",
    "churn_probability",
    "expected_renewal_amount",
)

TAB1_SNAPSHOT_DAILY_COLUMNS = (
    "msno",
    "churn_rate",
    "survival_days_proxy",
    "is_auto_renew",
    "is_manual_renew",
    "expire_day",
    "expected_renewal_amount",
    "actual_amount_paid",
    "total_secs_sum",
    "skip_ratio",
    "discovery_ratio",
    "high_skip_flag",
    "low_discovery_flag",
    "content_fatigue_flag",
    "active_segment",
)

TAB1_PREEXPIRY_PULSE_DAILY_COLUMNS = (
    "event_date",
    "total_revenue",
    "total_transactions",
    "high_risk_users",
    "avg_risk_score",
    "active_users",
    "total_listening_secs",
    "target_month",
    "target_month_label",
    "context_month",
    "context_month_label",
    "cohort_size",
    "series_mode",
)

FEATURE_SNAPSHOT_COLUMNS = (
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
)


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
    preferred_files: Iterable[str] | None = None,
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
    preferred = tuple(preferred_files or ())
    valid_candidates: list[Path] = []
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen or not candidate.exists():
            continue
        seen.add(candidate)
        if all((candidate / name).exists() for name in required):
            valid_candidates.append(candidate)

    if preferred:
        for candidate in valid_candidates:
            if all((candidate / name).exists() for name in preferred):
                return candidate

    if valid_candidates:
        return valid_candidates[0]

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


def resolve_tab1_artifacts_dir(root_hint: str | Path | None = None, score_month: int = 201704) -> Path:
    return _resolve_dir(
        env_var="TAB1_ARTIFACTS_DIR",
        root_hint=root_hint,
        required_files=(
            f"tab1_snapshot_{score_month}.parquet",
            "tab1_kpis_monthly.parquet",
            "tab1_km_curves.parquet",
            "tab1_segment_mix.parquet",
            "tab1_boredom_scatter.parquet",
            "manifest.json",
        ),
        local_candidates=[
            ("data", "artifacts_tab1_descriptive"),
            ("data", "artifacts", "tab1_descriptive"),
            ("artifacts", "tab1_descriptive"),
            ("artifacts_tab1_descriptive",),
            ("data", "artifacts", "_smoke_test", "tab1"),
        ],
    )


def resolve_tab1_preexpiry_pulse_dir(root_hint: str | Path | None = None, score_month: int = 201704) -> Path:
    return _resolve_dir(
        env_var="TAB1_PREEXPIRY_PULSE_DIR",
        root_hint=root_hint,
        required_files=(
            f"tab1_preexpiry_pulse_daily_{score_month}.parquet",
            f"tab1_preexpiry_pulse_summary_{score_month}.json",
            "manifest.json",
        ),
        local_candidates=[
            ("data", "artifacts_tab1_preexpiry_pulse"),
            ("data", "artifacts", "tab1_preexpiry_pulse"),
            ("artifacts", "tab1_preexpiry_pulse"),
            ("artifacts_tab1_preexpiry_pulse",),
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
            ("data", "artifacts_tab3_prescriptive 2"),
            ("data", "artifacts", "tab3_prescriptive 2"),
            ("artifacts", "tab3_prescriptive 2"),
            ("artifacts_tab3_prescriptive 2",),
            ("data", "artifacts_tab3_prescriptive"),
            ("data", "artifacts", "tab3_prescriptive"),
            ("artifacts", "tab3_prescriptive"),
            ("artifacts_tab3_prescriptive",),
            ("data", "artifacts", "_smoke_test", "tab3"),
        ],
        preferred_files=("scenario_catalog.json",),
    )


def resolve_tab3_monte_carlo_dir(root_hint: str | Path | None = None, score_month: int = 201704) -> Path:
    return _resolve_dir(
        env_var="TAB3_MONTE_CARLO_ARTIFACTS_DIR",
        root_hint=root_hint,
        required_files=(
            f"tab3_monte_carlo_summary_{score_month}.json",
            f"tab3_deterministic_summary_{score_month}.json",
            "manifest.json",
        ),
        local_candidates=[
            ("data", "artifacts_tab3_monte_carlo 2"),
            ("data", "artifacts", "tab3_monte_carlo 2"),
            ("artifacts", "tab3_monte_carlo 2"),
            ("artifacts_tab3_monte_carlo 2",),
            ("data", "artifacts_tab3_monte_carlo"),
            ("data", "artifacts", "tab3_monte_carlo"),
            ("artifacts", "tab3_monte_carlo"),
            ("artifacts_tab3_monte_carlo",),
            ("data", "artifacts", "_smoke_test", "tab3_monte_carlo"),
        ],
        preferred_files=("scenario_catalog.json",),
    )


def _required_tab3_prescriptive_files(score_month: int) -> tuple[str, ...]:
    return (
        f"tab3_scenario_summary_{score_month}.json",
        f"tab3_lever_summary_{score_month}.parquet",
        f"tab3_population_risk_shift_{score_month}.parquet",
    )


def _required_tab3_monte_carlo_files(score_month: int) -> tuple[str, ...]:
    return (
        f"tab3_monte_carlo_summary_{score_month}.json",
        f"tab3_deterministic_summary_{score_month}.json",
        "manifest.json",
    )


def _normalize_scenario_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return normalized or text


def _scenario_inputs_from_payload(
    *,
    scenario_config: dict[str, Any] | None = None,
    scenario_inputs: dict[str, Any] | None = None,
) -> dict[str, float]:
    if isinstance(scenario_inputs, dict):
        return {
            "auto_shift_pct": float(scenario_inputs.get("auto_shift_pct", 0.0) or 0.0),
            "upsell_shift_pct": float(scenario_inputs.get("upsell_shift_pct", 0.0) or 0.0),
            "skip_shift_pct": float(scenario_inputs.get("skip_shift_pct", 0.0) or 0.0),
        }

    config = scenario_config or {}
    return {
        "auto_shift_pct": float(config.get("manual_to_auto_share", 0.0) or 0.0) * 100.0,
        "upsell_shift_pct": float(config.get("upsell_share", 0.0) or 0.0) * 100.0,
        "skip_shift_pct": float(config.get("engagement_share", 0.0) or 0.0) * 100.0,
    }


def _load_tab3_scenario_catalog(root_dir: Path) -> dict[str, Any] | None:
    path = root_dir / "scenario_catalog.json"
    if not path.exists():
        return None

    payload = read_json_copy(path)
    raw_scenarios = payload.get("scenarios")
    if not isinstance(raw_scenarios, list):
        return None

    scenarios: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for raw_entry in raw_scenarios:
        if not isinstance(raw_entry, dict):
            continue
        scenario_id = _normalize_scenario_id(
            raw_entry.get("scenario_id") or raw_entry.get("id") or raw_entry.get("key")
        )
        if not scenario_id or scenario_id in seen_ids:
            continue
        seen_ids.add(scenario_id)

        scenario_config = raw_entry.get("scenario_config")
        scenario_inputs = raw_entry.get("scenario_inputs")
        scenarios.append(
            {
                "scenario_id": scenario_id,
                "label": str(raw_entry.get("label") or raw_entry.get("name") or scenario_id),
                "description": str(raw_entry.get("description") or "").strip() or None,
                "artifact_subdir": str(raw_entry.get("artifact_subdir") or ".").strip() or ".",
                "monte_carlo_subdir": str(raw_entry.get("monte_carlo_subdir") or ".").strip() or ".",
                "scenario_config": deepcopy(scenario_config) if isinstance(scenario_config, dict) else {},
                "scenario_inputs": _scenario_inputs_from_payload(
                    scenario_config=scenario_config if isinstance(scenario_config, dict) else None,
                    scenario_inputs=scenario_inputs if isinstance(scenario_inputs, dict) else None,
                ),
            }
        )

    if not scenarios:
        return None

    default_scenario_id = _normalize_scenario_id(payload.get("default_scenario_id")) or scenarios[0]["scenario_id"]
    if default_scenario_id not in seen_ids:
        default_scenario_id = scenarios[0]["scenario_id"]

    return {
        "default_scenario_id": default_scenario_id,
        "scenarios": scenarios,
    }


def _resolve_tab3_case_dir(
    root_dir: Path,
    *,
    score_month: int,
    scenario_id: str | None,
    catalog: dict[str, Any] | None,
    required_files: Iterable[str],
    subdir_field: str,
) -> tuple[Path, dict[str, Any] | None]:
    if catalog is None:
        normalized_id = _normalize_scenario_id(scenario_id)
        if normalized_id and normalized_id != DEFAULT_TAB3_SCENARIO_ID:
            raise ValueError(f"Scenario preset khong ton tai trong artifact catalog: {scenario_id}")
        return root_dir, None

    scenario_map = {
        str(entry["scenario_id"]): entry for entry in catalog.get("scenarios", []) if isinstance(entry, dict)
    }
    selected_id = _normalize_scenario_id(scenario_id) or str(catalog.get("default_scenario_id") or DEFAULT_TAB3_SCENARIO_ID)
    selected_entry = scenario_map.get(selected_id)
    if selected_entry is None:
        valid_ids = ", ".join(sorted(scenario_map.keys()))
        raise ValueError(f"Scenario preset khong hop le: {scenario_id}. Co san: {valid_ids}")

    subdir_text = str(selected_entry.get(subdir_field) or ".").strip() or "."
    candidate_dir = root_dir if subdir_text in {".", "./"} else (root_dir / subdir_text).resolve()
    if not all((candidate_dir / name).exists() for name in required_files):
        raise FileNotFoundError(
            f"Khong tim thay artifact cho scenario '{selected_id}' trong {candidate_dir}"
        )
    return candidate_dir, selected_entry


def _build_tab3_scenario_options(
    *,
    selected_entry: dict[str, Any] | None,
    selected_summary: dict[str, Any],
    prescriptive_catalog: dict[str, Any] | None,
    monte_carlo_catalog: dict[str, Any] | None,
    monte_carlo_enabled: bool,
) -> list[dict[str, Any]]:
    if prescriptive_catalog is None:
        return [
            {
                "scenario_id": DEFAULT_TAB3_SCENARIO_ID,
                "label": "Default scenario",
                "description": "Artifact preset mac dinh cho demo.",
                "scenario_inputs": _scenario_inputs_from_payload(
                    scenario_config=selected_summary.get("scenario_config")
                    if isinstance(selected_summary.get("scenario_config"), dict)
                    else None
                ),
                "is_default": True,
                "has_monte_carlo": bool(monte_carlo_enabled),
            }
        ]

    mc_ids = {
        str(entry["scenario_id"])
        for entry in (monte_carlo_catalog or {}).get("scenarios", [])
        if isinstance(entry, dict) and entry.get("scenario_id")
    }
    default_id = str(prescriptive_catalog.get("default_scenario_id") or DEFAULT_TAB3_SCENARIO_ID)
    rows: list[dict[str, Any]] = []
    for entry in prescriptive_catalog.get("scenarios", []):
        if not isinstance(entry, dict):
            continue
        scenario_id = str(entry.get("scenario_id") or "")
        if not scenario_id:
            continue
        is_selected = selected_entry is not None and scenario_id == str(selected_entry.get("scenario_id"))
        rows.append(
            {
                "scenario_id": scenario_id,
                "label": str(entry.get("label") or scenario_id),
                "description": entry.get("description"),
                "scenario_inputs": deepcopy(entry.get("scenario_inputs") or {}),
                "is_default": scenario_id == default_id,
                "has_monte_carlo": scenario_id in mc_ids if monte_carlo_catalog is not None else bool(monte_carlo_enabled if is_selected else False),
            }
        )
    return rows


@lru_cache(maxsize=64)
def _parquet_columns(path_text: str) -> tuple[str, ...]:
    return tuple(pq.read_schema(path_text).names)


@lru_cache(maxsize=64)
def _read_parquet_cached(path_text: str, columns: tuple[str, ...] | None = None) -> pd.DataFrame:
    kwargs: dict[str, Any] = {}
    if columns:
        kwargs["columns"] = list(columns)
    return pd.read_parquet(path_text, **kwargs)


def read_parquet_copy(path: str | Path, columns: Iterable[str] | None = None) -> pd.DataFrame:
    resolved = str(Path(path).resolve())
    selected_columns: tuple[str, ...] | None = None
    if columns is not None:
        available = set(_parquet_columns(resolved))
        selected_columns = tuple(column for column in columns if column in available)
        if not selected_columns:
            raise ValueError(f"Khong tim thay cot nao trong parquet: {resolved}")
    return _read_parquet_cached(resolved, selected_columns).copy()


@lru_cache(maxsize=32)
def _read_json_cached(path_text: str) -> dict[str, Any]:
    return json.loads(Path(path_text).read_text(encoding="utf-8"))


def read_json_copy(path: str | Path) -> dict[str, Any]:
    return deepcopy(_read_json_cached(str(Path(path).resolve())))


@lru_cache(maxsize=16)
def _read_csv_cached(path_text: str) -> pd.DataFrame:
    return pd.read_csv(path_text)


def read_csv_copy(path: str | Path) -> pd.DataFrame:
    return _read_csv_cached(str(Path(path).resolve())).copy()


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


def yyyymm_to_month_start(target_month: int) -> date:
    text = str(int(target_month)).zfill(6)
    return date(int(text[:4]), int(text[4:6]), 1)


def next_month_start(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def _cache_token(value: Any) -> str:
    if value is None:
        return "none"
    text = str(value).strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return slug or "none"


def _payload_cache_dir(artifact_dir: Path) -> Path:
    return artifact_dir / "payload_cache"


def _tab1_payload_cache_path(
    artifact_dir: Path,
    *,
    target_month: int,
    dimension: str,
    segment_type: Optional[str],
    segment_value: Optional[str],
) -> Path:
    return _payload_cache_dir(artifact_dir) / (
        f"tab1_descriptive_{target_month}"
        f"__dim-{_cache_token(dimension)}"
        f"__seg-{_cache_token(segment_type)}"
        f"__val-{_cache_token(segment_value)}.json"
    )


def _tab2_payload_cache_path(
    artifact_dir: Path,
    *,
    target_month: int,
    segment_type: Optional[str],
    segment_value: Optional[str],
) -> Path:
    return _payload_cache_dir(artifact_dir) / (
        f"tab2_predictive_{target_month}"
        f"__seg-{_cache_token(segment_type)}"
        f"__val-{_cache_token(segment_value)}.json"
    )


def _snapshot_payload_cache_path(artifact_dir: Path, *, target_month: int) -> Path:
    return _payload_cache_dir(artifact_dir) / f"dashboard_snapshot_{target_month}.json"


def _load_tab1_preexpiry_pulse_daily_df(pulse_dir: Path, target_month: int) -> pd.DataFrame:
    path = pulse_dir / f"tab1_preexpiry_pulse_daily_{target_month}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab1 preexpiry pulse daily cho thang {target_month} trong {pulse_dir}")
    df = read_parquet_copy(path, columns=TAB1_PREEXPIRY_PULSE_DAILY_COLUMNS)
    if "target_month" in df.columns:
        scoped = pd.to_numeric(df["target_month"], errors="coerce").astype("Int64")
        df = df.assign(target_month=scoped)
        df = df.loc[df["target_month"] == target_month].copy()
    return df.reset_index(drop=True)


def _load_tab1_preexpiry_pulse_summary(pulse_dir: Path, target_month: int) -> dict[str, Any]:
    path = pulse_dir / f"tab1_preexpiry_pulse_summary_{target_month}.json"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab1 preexpiry pulse summary cho thang {target_month} trong {pulse_dir}")
    return read_json_copy(path)


def load_tab1_preexpiry_pulse_payload(
    target_month: int,
    *,
    root_hint: str | Path | None = None,
) -> dict[str, Any] | None:
    try:
        pulse_dir = resolve_tab1_preexpiry_pulse_dir(root_hint=root_hint, score_month=target_month)
    except FileNotFoundError:
        return None

    daily_df = _load_tab1_preexpiry_pulse_daily_df(pulse_dir, target_month)
    if daily_df.empty:
        return None
    summary = _load_tab1_preexpiry_pulse_summary(pulse_dir, target_month)

    daily = daily_df.copy()
    daily["event_date"] = pd.to_datetime(daily["event_date"], errors="coerce")
    daily = daily.dropna(subset=["event_date"]).sort_values("event_date").reset_index(drop=True)
    if daily.empty:
        return None

    for column in (
        "total_revenue",
        "total_transactions",
        "high_risk_users",
        "avg_risk_score",
        "active_users",
        "total_listening_secs",
    ):
        daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0.0)

    for column in ("total_transactions", "high_risk_users", "active_users"):
        daily[column] = daily[column].round().astype(int)
    daily["event_date"] = daily["event_date"].dt.date.astype(str)

    context_month_value = summary.get("context_month")
    if context_month_value is None and "context_month" in daily.columns and daily["context_month"].notna().any():
        context_month_value = int(pd.to_numeric(daily["context_month"], errors="coerce").dropna().iloc[0])
    context_month = int(context_month_value) if context_month_value is not None else previous_yyyymm(target_month)
    context_month_start = yyyymm_to_month_start(context_month)
    context_month_end = next_month_start(context_month_start)

    series_mode = str(summary.get("series_mode") or daily.get("series_mode", pd.Series(["pre_expiry_context"])).iloc[0] or "pre_expiry_context")

    return {
        "meta": {
            "series_mode": series_mode,
            "context_month": str(summary.get("context_month_label") or yyyymm_to_label(context_month)),
            "context_month_start": context_month_start.isoformat(),
            "context_month_end_exclusive": context_month_end.isoformat(),
            "pulse_artifact_dir": str(pulse_dir),
            "pulse_as_of": summary.get("generated_at_utc"),
        },
        "revenue_series": daily.loc[:, ["event_date", "total_revenue", "total_transactions"]].to_dict(orient="records"),
        "risk_series": daily.loc[:, ["event_date", "high_risk_users", "avg_risk_score"]].to_dict(orient="records"),
        "activity_series": daily.loc[:, ["event_date", "active_users", "total_listening_secs"]].to_dict(orient="records"),
    }


def overlay_tab1_preexpiry_pulse(
    snapshot_payload: dict[str, Any],
    *,
    target_month: int,
    root_hint: str | Path | None = None,
) -> dict[str, Any]:
    pulse_payload = load_tab1_preexpiry_pulse_payload(target_month, root_hint=root_hint)
    if pulse_payload is None:
        return snapshot_payload

    payload = deepcopy(snapshot_payload)
    meta = dict(payload.get("meta") or {})
    meta.update(pulse_payload.get("meta") or {})
    payload["meta"] = meta
    payload["revenue_series"] = pulse_payload.get("revenue_series", [])
    payload["risk_series"] = pulse_payload.get("risk_series", [])
    payload["activity_series"] = pulse_payload.get("activity_series", [])
    return payload


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


def available_tab1_months(root_hint: str | Path | None = None) -> list[str]:
    try:
        root = resolve_tab1_artifacts_dir(root_hint=root_hint)
    except FileNotFoundError:
        return []
    months: set[str] = set()
    for path in root.glob("tab1_snapshot_*.parquet"):
        month_text = path.stem.rsplit("_", 1)[-1]
        if month_text.isdigit() and len(month_text) == 6:
            months.add(yyyymm_to_label(int(month_text)))
    return sorted(months)


def available_tab3_months(root_hint: str | Path | None = None) -> list[str]:
    months: set[str] = set()
    try:
        prescriptive_root = resolve_tab3_artifacts_dir(root_hint=root_hint)
    except FileNotFoundError:
        prescriptive_root = None
    if prescriptive_root is not None:
        for path in prescriptive_root.glob("tab3_*_201*.parquet"):
            month_text = path.stem.rsplit("_", 1)[-1]
            if month_text.isdigit() and len(month_text) == 6:
                months.add(yyyymm_to_label(int(month_text)))
        for path in prescriptive_root.glob("tab3_*_201*.json"):
            month_text = path.stem.rsplit("_", 1)[-1]
            if month_text.isdigit() and len(month_text) == 6:
                months.add(yyyymm_to_label(int(month_text)))
    try:
        monte_carlo_root = resolve_tab3_monte_carlo_dir(root_hint=root_hint)
    except FileNotFoundError:
        monte_carlo_root = None
    if monte_carlo_root is not None:
        for path in monte_carlo_root.glob("tab3_*_201*.parquet"):
            month_text = path.stem.rsplit("_", 1)[-1]
            if month_text.isdigit() and len(month_text) == 6:
                months.add(yyyymm_to_label(int(month_text)))
        for path in monte_carlo_root.glob("tab3_*_201*.json"):
            month_text = path.stem.rsplit("_", 1)[-1]
            if month_text.isdigit() and len(month_text) == 6:
                months.add(yyyymm_to_label(int(month_text)))
    return sorted(months)


def _tab1_dimension_column(dimension: str) -> str:
    normalized = (dimension or "").strip().lower()
    return TAB1_DIMENSION_FIELD_MAP.get(normalized, "age_segment")


def _load_tab1_snapshot_df(
    tab1_dir: Path,
    target_month: int,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    path = tab1_dir / f"tab1_snapshot_{target_month}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab1 snapshot artifact cho thang {target_month} trong {tab1_dir}")
    return read_parquet_copy(path, columns=columns)


def _load_tab1_monthly_kpis(tab1_dir: Path) -> pd.DataFrame:
    return read_parquet_copy(tab1_dir / "tab1_kpis_monthly.parquet")


def _load_tab1_km_curves(tab1_dir: Path) -> pd.DataFrame:
    return read_parquet_copy(tab1_dir / "tab1_km_curves.parquet")


def _load_tab1_trend_monthly_summary_df(tab1_dir: Path) -> pd.DataFrame:
    path = tab1_dir / "trend_monthly_summary.parquet"
    if not path.exists():
        return pd.DataFrame()
    return read_parquet_copy(path)


def _load_tab1_snapshot_risk_heatmap_df(tab1_dir: Path) -> pd.DataFrame:
    path = tab1_dir / "snapshot_risk_heatmap_all.parquet"
    if not path.exists():
        return pd.DataFrame()
    return read_parquet_copy(path)


def _load_tab2_executive_value_risk_matrix_df(tab2_dir: Path, target_month: int) -> pd.DataFrame:
    path = tab2_dir / f"tab2_executive_value_risk_matrix_{target_month}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return read_parquet_copy(path)


def _frame_series_or_default(frame: pd.DataFrame, column: str, default: Any) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(default, index=frame.index)


def _coerce_rate_pct(value: Any) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0.0
    numeric = float(numeric)
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    return numeric


def _build_tab1_churn_breakdown(total_expiring_users: int, historical_churn_rate: float) -> dict[str, Any]:
    total_users = max(int(total_expiring_users), 0)
    churn_rate_pct = float(np.clip(historical_churn_rate, 0.0, 100.0))
    churned_users = int(round(total_users * churn_rate_pct / 100.0))
    churned_users = min(max(churned_users, 0), total_users)
    renewed_users = total_users - churned_users
    renewed_rate = 100.0 - churn_rate_pct if total_users > 0 else 0.0
    return {
        "renewed_users": renewed_users,
        "churned_users": churned_users,
        "renewed_rate": renewed_rate,
        "churned_rate": churn_rate_pct,
    }


def _resolve_tab1_chart_context_month(
    target_month: int,
    monthly_trend: Iterable[dict[str, Any]] | None,
) -> int:
    available_months: list[int] = []
    for point in monthly_trend or []:
        month_value = pd.to_numeric(pd.Series([point.get("target_month")]), errors="coerce").iloc[0]
        if pd.isna(month_value):
            continue
        month_number = int(month_value)
        if month_number <= target_month:
            available_months.append(month_number)
    return max(available_months) if available_months else target_month


def _find_tab1_monthly_trend_point(
    monthly_trend: Iterable[dict[str, Any]] | None,
    target_month: int,
) -> dict[str, Any] | None:
    for point in monthly_trend or []:
        month_value = pd.to_numeric(pd.Series([point.get("target_month")]), errors="coerce").iloc[0]
        if pd.isna(month_value):
            continue
        if int(month_value) == target_month:
            return point
    return None


def _apply_tab1_chart_artifact_context(
    payload: dict[str, Any],
    *,
    tab1_dir: Path,
    target_month: int,
    segment_type: Optional[str],
    segment_value: Optional[str],
) -> dict[str, Any]:
    meta = payload.setdefault("meta", {})
    monthly_trend = payload.get("monthly_trend") or []

    chart_context_month = _resolve_tab1_chart_context_month(target_month, monthly_trend)
    meta["churn_breakdown_month"] = yyyymm_to_label(chart_context_month)

    chart_context_point = _find_tab1_monthly_trend_point(monthly_trend, chart_context_month)
    if chart_context_point is not None:
        total_users = int(pd.to_numeric(pd.Series([chart_context_point.get("total_expiring_users")]), errors="coerce").fillna(0).iloc[0])
        churn_rate_pct = _coerce_rate_pct(chart_context_point.get("historical_churn_rate", 0.0))
        payload["churn_breakdown"] = _build_tab1_churn_breakdown(total_users, churn_rate_pct)

    risk_heatmap_month = target_month
    if not segment_type and not segment_value:
        precomputed_risk_heatmap = _normalize_tab1_precomputed_risk_heatmap(
            _load_tab1_snapshot_risk_heatmap_df(tab1_dir),
            chart_context_month,
        )
        if precomputed_risk_heatmap:
            payload["risk_heatmap"] = precomputed_risk_heatmap
            risk_heatmap_month = chart_context_month

    meta["risk_heatmap_month"] = yyyymm_to_label(risk_heatmap_month)
    return payload


def _tab1_normalized_churn_probability(frame: pd.DataFrame) -> pd.Series:
    if "churn_rate" in frame.columns:
        churn_probability = pd.to_numeric(frame["churn_rate"], errors="coerce").fillna(0.0)
    elif "is_churn" in frame.columns:
        churn_probability = pd.to_numeric(frame["is_churn"], errors="coerce").fillna(0.0)
    else:
        churn_probability = pd.Series(0.0, index=frame.index, dtype="float64")

    churn_probability = churn_probability.where(churn_probability >= 0.0, 0.0).astype("float64")
    high_mask = churn_probability > 1.0
    churn_probability.loc[high_mask] = churn_probability.loc[high_mask] / 100.0
    return churn_probability.clip(lower=0.0, upper=1.0)


def _build_tab1_kpis_from_frame(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "total_expiring_users": 0,
            "historical_churn_rate": 0.0,
            "overall_median_survival": 0.0,
            "auto_renew_rate": 0.0,
            "total_expected_renewal_amount": 0.0,
            "historical_revenue_at_risk": 0.0,
        }

    churn_probability = _tab1_normalized_churn_probability(frame)
    expected_renewal_amount = (
        pd.to_numeric(frame["expected_renewal_amount"], errors="coerce").fillna(0.0).clip(lower=0.0)
        if "expected_renewal_amount" in frame.columns
        else pd.Series(0.0, index=frame.index, dtype="float64")
    )
    if "expected_revenue_at_risk_30d" in frame.columns:
        revenue_at_risk = pd.to_numeric(frame["expected_revenue_at_risk_30d"], errors="coerce").fillna(0.0).clip(lower=0.0)
    else:
        revenue_at_risk = expected_renewal_amount * churn_probability

    overall_median_survival = 0.0
    if "survival_days_proxy" in frame.columns:
        survival_days = pd.to_numeric(frame["survival_days_proxy"], errors="coerce").dropna()
        if not survival_days.empty:
            overall_median_survival = float(survival_days.median())

    auto_renew_rate = 0.0
    if "is_auto_renew" in frame.columns:
        auto_renew_rate = float(pd.to_numeric(frame["is_auto_renew"], errors="coerce").fillna(0.0).mean() * 100.0)

    return {
        "total_expiring_users": int(frame["msno"].nunique()) if "msno" in frame.columns else int(len(frame)),
        "historical_churn_rate": float(churn_probability.mean() * 100.0),
        "overall_median_survival": overall_median_survival,
        "auto_renew_rate": auto_renew_rate,
        "total_expected_renewal_amount": float(expected_renewal_amount.sum()),
        "historical_revenue_at_risk": float(revenue_at_risk.sum()),
    }


def _build_tab1_behavior_clusters(frame: pd.DataFrame) -> list[dict[str, Any]]:
    required_columns = {"msno", "discovery_ratio", "skip_ratio"}
    if frame.empty or not required_columns.issubset(frame.columns):
        return []

    discovery_ratio = pd.to_numeric(frame["discovery_ratio"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    skip_ratio = pd.to_numeric(frame["skip_ratio"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    expected_renewal_amount = (
        pd.to_numeric(frame["expected_renewal_amount"], errors="coerce").fillna(0.0).clip(lower=0.0)
        if "expected_renewal_amount" in frame.columns
        else pd.Series(0.0, index=frame.index, dtype="float64")
    )
    churn_probability = _tab1_normalized_churn_probability(frame)

    clustered = pd.DataFrame(
        {
            "msno": frame["msno"].fillna("").astype(str),
            "discovery_bin": pd.cut(
                discovery_ratio,
                bins=TAB1_BEHAVIOR_BIN_EDGES,
                labels=TAB1_BEHAVIOR_BIN_LABELS,
                include_lowest=True,
                right=False,
            ),
            "skip_bin": pd.cut(
                skip_ratio,
                bins=TAB1_BEHAVIOR_BIN_EDGES,
                labels=TAB1_BEHAVIOR_BIN_LABELS,
                include_lowest=True,
                right=False,
            ),
            "expected_renewal_amount": expected_renewal_amount,
            "churn_probability": churn_probability,
            "revenue_at_risk": expected_renewal_amount * churn_probability,
        }
    )
    clustered = clustered.dropna(subset=["discovery_bin", "skip_bin"]).copy()
    if clustered.empty:
        return []
    clustered["discovery_bin"] = clustered["discovery_bin"].astype(str)
    clustered["skip_bin"] = clustered["skip_bin"].astype(str)

    grouped = (
        clustered.groupby(["discovery_bin", "skip_bin"], as_index=False)
        .agg(
            users=("msno", "nunique"),
            avg_expected_renewal_amount=("expected_renewal_amount", "mean"),
            churn_rate_pct=("churn_probability", "mean"),
            revenue_at_risk=("revenue_at_risk", "sum"),
        )
        .sort_values(["revenue_at_risk", "users", "churn_rate_pct"], ascending=[False, False, False])
        .reset_index(drop=True)
    )
    grouped["churn_rate_pct"] = grouped["churn_rate_pct"] * 100.0
    grouped["discovery_ratio"] = grouped["discovery_bin"].map(TAB1_BEHAVIOR_BIN_MIDPOINTS).fillna(0.0)
    grouped["skip_ratio"] = grouped["skip_bin"].map(TAB1_BEHAVIOR_BIN_MIDPOINTS).fillna(0.0)
    grouped["cluster_label"] = grouped.apply(
        lambda row: f"Kham pha {row['discovery_bin']} • Bo qua {row['skip_bin']}",
        axis=1,
    )

    top_n = min(len(grouped), max(5, int(np.ceil(len(grouped) * 0.2))))
    return grouped.head(top_n).to_dict(orient="records")


def _build_tab1_monthly_trend_from_feature_store(
    monthly_kpis: pd.DataFrame,
    *,
    root_hint: str | Path | None = None,
    segment_type: Optional[str] = None,
    segment_value: Optional[str] = None,
) -> list[dict[str, Any]]:
    feature_store_dir = resolve_feature_store_dir(root_hint=root_hint, score_month=201704)
    feature_df = read_parquet_copy(
        feature_store_dir / "train_features_bi_all.parquet",
        columns=(
            "target_month",
            "msno",
            "loyalty_segment",
            "is_churn",
            "is_auto_renew",
            "expected_renewal_amount",
            "price_segment",
            "active_segment",
        ),
    )
    filtered = _filter_by_segment(feature_df, segment_type, segment_value)
    if filtered.empty:
        return []

    monthly_lookup = (
        monthly_kpis.set_index("target_month")["median_survival_days_proxy"].to_dict()
        if "median_survival_days_proxy" in monthly_kpis.columns
        else {}
    )
    grouped = (
        filtered.groupby("target_month", as_index=False)
        .agg(
            total_expiring_users=("msno", "nunique"),
            historical_churn_rate=("is_churn", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0.0).mean() * 100.0)),
            auto_renew_rate=("is_auto_renew", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0.0).mean() * 100.0)),
            total_expected_renewal_amount=("expected_renewal_amount", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0.0).clip(lower=0.0).sum())),
            new_paid_users=("loyalty_segment", lambda s: int(s.fillna("").astype(str).eq("New < 30d").sum())),
            churned_users=("is_churn", lambda s: int(pd.to_numeric(s, errors="coerce").fillna(0.0).sum())),
        )
        .sort_values("target_month")
        .reset_index(drop=True)
    )

    rows: list[dict[str, Any]] = []
    for row in grouped.itertuples(index=False):
        target_month = int(getattr(row, "target_month"))
        if target_month in TAB1_EXCLUDED_TREND_MONTHS:
            continue
        total_users = int(getattr(row, "total_expiring_users", 0) or 0)
        total_expected_renewal_amount = float(getattr(row, "total_expected_renewal_amount", 0.0) or 0.0)
        new_paid_users = int(getattr(row, "new_paid_users", 0) or 0)
        churned_users = int(getattr(row, "churned_users", 0) or 0)
        historical_churn_rate = float(getattr(row, "historical_churn_rate", 0.0) or 0.0)
        rows.append(
            {
                "target_month": target_month,
                "month_label": yyyymm_to_label(target_month),
                "total_expiring_users": total_users,
                "historical_churn_rate": historical_churn_rate,
                "overall_median_survival": float(monthly_lookup.get(target_month, 0.0) or 0.0),
                "auto_renew_rate": float(getattr(row, "auto_renew_rate", 0.0) or 0.0),
                "total_expected_renewal_amount": total_expected_renewal_amount,
                "historical_revenue_at_risk": float(total_expected_renewal_amount * historical_churn_rate / 100.0),
                "apru": float(total_expected_renewal_amount / total_users) if total_users > 0 else None,
                "new_paid_users": new_paid_users,
                "churned_users": churned_users,
                "net_movement": int(new_paid_users - churned_users),
            }
        )
    return rows


def _normalize_tab1_precomputed_monthly_trend(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    work = df.copy()
    target_month = pd.to_numeric(_frame_series_or_default(work, "target_month", np.nan), errors="coerce").astype("Int64")
    work = work.assign(target_month=target_month).dropna(subset=["target_month"]).copy()
    if work.empty:
        return []

    work["target_month"] = work["target_month"].astype("int32")
    work = work.loc[~work["target_month"].isin(TAB1_EXCLUDED_TREND_MONTHS)].copy()
    if work.empty:
        return []

    work["month_label"] = _frame_series_or_default(work, "month_label", "").astype(str)
    work["month_label"] = work["month_label"].where(work["month_label"].str.len() > 0, work["target_month"].map(yyyymm_to_label))
    work["total_expiring_users"] = pd.to_numeric(
        _frame_series_or_default(work, "total_expiring_users", _frame_series_or_default(work, "subscribers", 0)),
        errors="coerce",
    ).fillna(0).astype(int)
    work["historical_churn_rate"] = pd.to_numeric(
        _frame_series_or_default(work, "historical_churn_rate", _frame_series_or_default(work, "churn_rate", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    low_mask = work["historical_churn_rate"].between(0.0, 1.0, inclusive="both")
    work.loc[low_mask, "historical_churn_rate"] = work.loc[low_mask, "historical_churn_rate"] * 100.0
    work["overall_median_survival"] = pd.to_numeric(
        _frame_series_or_default(work, "overall_median_survival", _frame_series_or_default(work, "median_survival_days_proxy", 0.0)),
        errors="coerce",
    ).fillna(0.0)
    work["auto_renew_rate"] = pd.to_numeric(_frame_series_or_default(work, "auto_renew_rate", 0.0), errors="coerce").fillna(0.0)
    auto_mask = work["auto_renew_rate"].between(0.0, 1.0, inclusive="both")
    work.loc[auto_mask, "auto_renew_rate"] = work.loc[auto_mask, "auto_renew_rate"] * 100.0
    work["total_expected_renewal_amount"] = pd.to_numeric(
        _frame_series_or_default(work, "total_expected_renewal_amount", _frame_series_or_default(work, "revenue", np.nan)),
        errors="coerce",
    )
    work["historical_revenue_at_risk"] = pd.to_numeric(
        _frame_series_or_default(work, "historical_revenue_at_risk", np.nan),
        errors="coerce",
    )
    risk_mask = work["historical_revenue_at_risk"].isna() & work["total_expected_renewal_amount"].notna()
    work.loc[risk_mask, "historical_revenue_at_risk"] = (
        work.loc[risk_mask, "total_expected_renewal_amount"] * work.loc[risk_mask, "historical_churn_rate"] / 100.0
    )
    work["apru"] = pd.to_numeric(_frame_series_or_default(work, "apru", np.nan), errors="coerce")
    apru_mask = work["apru"].isna() & work["total_expected_renewal_amount"].notna() & (work["total_expiring_users"] > 0)
    work.loc[apru_mask, "apru"] = work.loc[apru_mask, "total_expected_renewal_amount"] / work.loc[apru_mask, "total_expiring_users"]
    work["new_paid_users"] = pd.to_numeric(_frame_series_or_default(work, "new_paid_users", np.nan), errors="coerce")
    work["churned_users"] = pd.to_numeric(_frame_series_or_default(work, "churned_users", np.nan), errors="coerce")
    churned_mask = work["churned_users"].isna()
    work.loc[churned_mask, "churned_users"] = np.round(
        work.loc[churned_mask, "total_expiring_users"] * work.loc[churned_mask, "historical_churn_rate"] / 100.0
    )
    work["net_movement"] = pd.to_numeric(_frame_series_or_default(work, "net_movement", np.nan), errors="coerce")
    net_mask = work["net_movement"].isna() & work["new_paid_users"].notna()
    work.loc[net_mask, "net_movement"] = work.loc[net_mask, "new_paid_users"] - work.loc[net_mask, "churned_users"]

    ordered = work.sort_values("target_month").reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for row in ordered.itertuples(index=False):
        total_expected_renewal_amount = getattr(row, "total_expected_renewal_amount", np.nan)
        rows.append(
            {
                "target_month": int(getattr(row, "target_month")),
                "month_label": str(getattr(row, "month_label")),
                "total_expiring_users": int(getattr(row, "total_expiring_users", 0) or 0),
                "historical_churn_rate": float(getattr(row, "historical_churn_rate", 0.0) or 0.0),
                "overall_median_survival": float(getattr(row, "overall_median_survival", 0.0) or 0.0),
                "auto_renew_rate": float(getattr(row, "auto_renew_rate", 0.0) or 0.0),
                "total_expected_renewal_amount": None if pd.isna(total_expected_renewal_amount) else float(total_expected_renewal_amount),
                "historical_revenue_at_risk": None
                if pd.isna(getattr(row, "historical_revenue_at_risk", np.nan))
                else float(getattr(row, "historical_revenue_at_risk")),
                "apru": None if pd.isna(getattr(row, "apru", np.nan)) else float(getattr(row, "apru")),
                "new_paid_users": None if pd.isna(getattr(row, "new_paid_users", np.nan)) else int(round(float(getattr(row, "new_paid_users")))),
                "churned_users": None if pd.isna(getattr(row, "churned_users", np.nan)) else int(round(float(getattr(row, "churned_users")))),
                "net_movement": None if pd.isna(getattr(row, "net_movement", np.nan)) else int(round(float(getattr(row, "net_movement")))),
            }
        )
    return rows


def _normalize_tab1_precomputed_risk_heatmap(df: pd.DataFrame, target_month: int) -> list[dict[str, Any]]:
    if df.empty:
        return []

    work = df.copy()
    if "target_month" in work.columns:
        scoped_month = pd.to_numeric(work["target_month"], errors="coerce").astype("Int64")
        work = work.assign(target_month=scoped_month)
        work = work.loc[work["target_month"] == target_month].copy()
    if work.empty:
        return []

    if "risk_customer_segment" in work.columns and "risk_segment" not in work.columns:
        work = work.rename(columns={"risk_customer_segment": "risk_segment"})
    if "subscribers" in work.columns and "users" not in work.columns:
        work = work.rename(columns={"subscribers": "users"})
    if "value_tier" not in work.columns or "risk_segment" not in work.columns:
        return []

    work["users"] = pd.to_numeric(_frame_series_or_default(work, "users", 0), errors="coerce").fillna(0).astype(int)
    grouped = (
        work.groupby(["value_tier", "risk_segment"], as_index=False)
        .agg(users=("users", "sum"))
        .set_index(["value_tier", "risk_segment"])
        .reindex(
            pd.MultiIndex.from_product(
                [TAB1_VISIBLE_VALUE_TIERS, TAB1_RISK_SEGMENT_ORDER],
                names=["value_tier", "risk_segment"],
            ),
            fill_value=0,
        )
        .reset_index()
    )
    grouped["users"] = grouped["users"].astype(int)
    return grouped.to_dict(orient="records")


def _normalize_tab2_executive_value_risk_matrix(df: pd.DataFrame, target_month: int) -> list[dict[str, Any]]:
    if df.empty:
        return []

    work = df.copy()
    if "target_month" in work.columns:
        scoped_month = pd.to_numeric(work["target_month"], errors="coerce").astype("Int64")
        work = work.assign(target_month=scoped_month)
        work = work.loc[work["target_month"] == target_month].copy()
    if work.empty:
        return []

    work["prob_bin"] = pd.to_numeric(_frame_series_or_default(work, "prob_bin", 0.0), errors="coerce").fillna(0.0)
    work["expected_renewal_amount"] = pd.to_numeric(_frame_series_or_default(work, "expected_renewal_amount", 0.0), errors="coerce").fillna(0.0)
    work["user_count"] = pd.to_numeric(_frame_series_or_default(work, "user_count", 0), errors="coerce").fillna(0).astype(int)
    work["revenue_at_risk"] = pd.to_numeric(_frame_series_or_default(work, "revenue_at_risk", 0.0), errors="coerce").fillna(0.0)
    if "risk_tier" not in work.columns and "risk_band" in work.columns:
        work["risk_tier"] = _to_executive_risk_band(work["risk_band"])
    work = work.loc[
        (work["expected_renewal_amount"] <= TAB2_EXECUTIVE_MATRIX_MAX_RENEWAL_AMOUNT)
        & (work["user_count"] >= TAB2_EXECUTIVE_MATRIX_MIN_USER_COUNT)
    ].copy()
    if work.empty:
        return []
    if "display_size" not in work.columns:
        work["display_size"] = np.sqrt(work["user_count"].clip(lower=1)).astype("float64")
    if "priority_quadrant" not in work.columns:
        work["priority_quadrant"] = np.select(
            [
                (work["prob_bin"] >= 0.5) & (work["expected_renewal_amount"] >= 100.0),
                (work["prob_bin"] >= 0.5) & (work["expected_renewal_amount"] < 100.0),
                (work["prob_bin"] < 0.5) & (work["expected_renewal_amount"] >= 100.0),
            ],
            ["VIP Rescue", "Price Sensitive", "Core Loyal"],
            default="Casual",
        )

    return (
        work[
            [
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
        .sort_values(["prob_bin", "expected_renewal_amount", "user_count"], ascending=[True, True, False])
        .to_dict(orient="records")
    )


def _build_tab1_monthly_trend_from_kpis(
    tab1_dir: Path,
    monthly_kpis: pd.DataFrame,
    *,
    root_hint: str | Path | None = None,
    segment_type: Optional[str] = None,
    segment_value: Optional[str] = None,
) -> list[dict[str, Any]]:
    try:
        feature_rows = _build_tab1_monthly_trend_from_feature_store(
            monthly_kpis,
            root_hint=root_hint,
            segment_type=segment_type,
            segment_value=segment_value,
        )
    except (FileNotFoundError, ValueError):
        feature_rows = []

    if feature_rows:
        return feature_rows

    if monthly_kpis.empty:
        return []

    rows: list[dict[str, Any]] = []
    ordered = monthly_kpis.sort_values("target_month").reset_index(drop=True)
    for row in ordered.itertuples(index=False):
        target_month = int(getattr(row, "target_month"))
        if target_month in TAB1_EXCLUDED_TREND_MONTHS:
            continue
        churn_rate_pct = _coerce_rate_pct(getattr(row, "historical_churn_rate", 0.0))
        if churn_rate_pct >= 99.0:
            try:
                snapshot_df = _load_tab1_snapshot_df(tab1_dir, target_month, columns=("churn_rate",))
                churn_rate_clean = pd.to_numeric(snapshot_df["churn_rate"], errors="coerce")
                churn_rate_clean = churn_rate_clean[churn_rate_clean >= 0]
                if not churn_rate_clean.empty:
                    churn_rate_pct = float(churn_rate_clean.mean() * 100.0)
            except FileNotFoundError:
                pass

        auto_renew_rate = _coerce_rate_pct(getattr(row, "auto_renew_rate", 0.0))
        total_users = int(getattr(row, "total_expiring_users", 0) or 0)
        revenue_value = pd.to_numeric(pd.Series([getattr(row, "total_expected_renewal_amount", np.nan)]), errors="coerce").iloc[0]
        total_expected_renewal_amount = None if pd.isna(revenue_value) else float(revenue_value)
        historical_revenue_at_risk = (
            float(total_expected_renewal_amount * churn_rate_pct / 100.0)
            if total_expected_renewal_amount is not None
            else None
        )
        apru = (
            float(total_expected_renewal_amount / total_users)
            if total_expected_renewal_amount is not None and total_users > 0
            else None
        )
        rows.append(
            {
                "target_month": target_month,
                "month_label": yyyymm_to_label(target_month),
                "total_expiring_users": total_users,
                "historical_churn_rate": churn_rate_pct,
                "overall_median_survival": float(getattr(row, "median_survival_days_proxy", 0.0) or 0.0),
                "auto_renew_rate": auto_renew_rate,
                "total_expected_renewal_amount": total_expected_renewal_amount,
                "historical_revenue_at_risk": historical_revenue_at_risk,
                "apru": apru,
                "new_paid_users": None,
                "churned_users": int(round(total_users * churn_rate_pct / 100.0)),
                "net_movement": None,
            }
        )
    return rows


def _build_tab1_risk_heatmap(snapshot_df: pd.DataFrame) -> list[dict[str, Any]]:
    if snapshot_df.empty or "msno" not in snapshot_df.columns:
        return []

    price_segment = snapshot_df["price_segment"].fillna("").astype(str) if "price_segment" in snapshot_df.columns else pd.Series("", index=snapshot_df.index)
    free_trial_flag = (
        pd.to_numeric(snapshot_df["free_trial_flag"], errors="coerce").fillna(0.0)
        if "free_trial_flag" in snapshot_df.columns
        else pd.Series(0.0, index=snapshot_df.index)
    )
    deal_hunter_flag = (
        pd.to_numeric(snapshot_df["deal_hunter_flag"], errors="coerce").fillna(0.0)
        if "deal_hunter_flag" in snapshot_df.columns
        else pd.Series(0.0, index=snapshot_df.index)
    )
    churn_rate = (
        pd.to_numeric(snapshot_df["churn_rate"], errors="coerce").fillna(-1.0)
        if "churn_rate" in snapshot_df.columns
        else pd.Series(-1.0, index=snapshot_df.index)
    )

    value_tier = pd.Series(
        np.select(
            [
                (free_trial_flag >= 0.5) | price_segment.str.contains("Free Trial", case=False, regex=False),
                (deal_hunter_flag >= 0.5) | price_segment.str.contains("Deal Hunter", case=False, regex=False),
            ],
            ["Free Trial", "Deal Hunter"],
            default="Standard",
        ),
        index=snapshot_df.index,
    )
    risk_segment = pd.Series(
        np.select(
            [churn_rate >= 0.25, churn_rate > 0.0],
            ["At Risk", "Watchlist"],
            default="Stable",
        ),
        index=snapshot_df.index,
    )

    grouped = (
        pd.DataFrame(
            {
                "msno": snapshot_df["msno"].fillna("").astype(str),
                "value_tier": value_tier,
                "risk_segment": risk_segment,
            }
        )
        .groupby(["value_tier", "risk_segment"], as_index=False)
        .agg(users=("msno", "nunique"))
    )

    heatmap_index = pd.MultiIndex.from_product(
        [TAB1_VISIBLE_VALUE_TIERS, TAB1_RISK_SEGMENT_ORDER],
        names=["value_tier", "risk_segment"],
    )
    grouped = grouped.set_index(["value_tier", "risk_segment"]).reindex(heatmap_index, fill_value=0).reset_index()
    grouped["users"] = grouped["users"].astype(int)
    return grouped.to_dict(orient="records")


def _build_tab1_daily_snapshot_series(
    snapshot_df: pd.DataFrame,
    month_start: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if snapshot_df.empty or "expire_day" not in snapshot_df.columns:
        return [], [], []

    days_in_month = monthrange(month_start.year, month_start.month)[1]
    work = snapshot_df.copy()

    def numeric_series(column: str, *, fill_value: float = 0.0) -> pd.Series:
        if column not in work.columns:
            return pd.Series(fill_value, index=work.index, dtype="float64")
        return pd.to_numeric(work[column], errors="coerce").fillna(fill_value)

    expire_day = numeric_series("expire_day")
    work = work.loc[expire_day.between(1, days_in_month)].copy()
    if work.empty:
        return [], [], []

    expire_day = pd.to_numeric(work["expire_day"], errors="coerce").fillna(0).astype(int)
    expected_renewal_amount = numeric_series("expected_renewal_amount").clip(lower=0.0)
    actual_amount_paid = numeric_series("actual_amount_paid").clip(lower=0.0)
    activity_secs = numeric_series("total_secs_sum").clip(lower=0.0)
    skip_ratio = numeric_series("skip_ratio").clip(lower=0.0, upper=1.0)
    discovery_ratio = numeric_series("discovery_ratio").clip(lower=0.0, upper=1.0)
    high_skip_flag = numeric_series("high_skip_flag").clip(lower=0.0, upper=1.0)
    low_discovery_flag = numeric_series("low_discovery_flag").clip(lower=0.0, upper=1.0)
    content_fatigue_flag = numeric_series("content_fatigue_flag").clip(lower=0.0, upper=1.0)

    if "is_manual_renew" in work.columns:
        manual_renew_signal = numeric_series("is_manual_renew").clip(lower=0.0, upper=1.0)
    else:
        manual_renew_signal = 1.0 - numeric_series("is_auto_renew").clip(lower=0.0, upper=1.0)

    revenue_proxy = expected_renewal_amount.where(expected_renewal_amount > 0.0, actual_amount_paid)
    risk_score = (
        0.45 * skip_ratio
        + 0.25 * (1.0 - discovery_ratio)
        + 0.15 * manual_renew_signal
        + 0.10 * high_skip_flag
        + 0.03 * low_discovery_flag
        + 0.02 * content_fatigue_flag
    ).clip(lower=0.0, upper=1.0)
    risk_threshold = float(np.clip(risk_score.quantile(0.75), 0.40, 0.70)) if not risk_score.empty else 0.55

    if "active_segment" in work.columns:
        active_segment = work["active_segment"].fillna("").astype(str)
        is_active_user = (activity_secs > 0.0) | active_segment.isin(("Light 1-5 logs", "Active 6-15 logs", "Heavy > 15 logs"))
    else:
        is_active_user = activity_secs > 0.0

    work = work.assign(
        expire_day=expire_day,
        revenue_proxy=revenue_proxy,
        activity_secs=activity_secs,
        risk_score=risk_score,
        high_risk_user=(risk_score >= risk_threshold).astype("int8"),
        active_user=is_active_user.astype("int8"),
        cohort_user=1,
    )

    grouped = (
        work.groupby("expire_day", as_index=False)
        .agg(
            total_revenue=("revenue_proxy", "sum"),
            total_transactions=("cohort_user", "sum"),
            high_risk_users=("high_risk_user", "sum"),
            avg_risk_score=("risk_score", "mean"),
            active_users=("active_user", "sum"),
            total_listening_secs=("activity_secs", "sum"),
        )
        .sort_values("expire_day")
    )

    grouped = (
        pd.DataFrame({"expire_day": np.arange(1, days_in_month + 1, dtype=int)})
        .merge(grouped, on="expire_day", how="left")
        .fillna(0.0)
    )
    grouped["event_date"] = [
        date(month_start.year, month_start.month, int(day)).isoformat()
        for day in grouped["expire_day"].tolist()
    ]
    grouped["avg_risk_score"] = grouped["avg_risk_score"] * 100.0

    for column in ("total_transactions", "high_risk_users", "active_users"):
        grouped[column] = grouped[column].round().astype(int)

    revenue_series = grouped.loc[:, ["event_date", "total_revenue", "total_transactions"]].to_dict(orient="records")
    risk_series = grouped.loc[:, ["event_date", "high_risk_users", "avg_risk_score"]].to_dict(orient="records")
    activity_series = grouped.loc[:, ["event_date", "active_users", "total_listening_secs"]].to_dict(orient="records")
    return revenue_series, risk_series, activity_series


def _empty_tab1_payload(
    *,
    target_month: int,
    dimension: str,
    segment_type: Optional[str],
    segment_value: Optional[str],
    artifact_dir: Path,
) -> dict[str, Any]:
    return {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "dimension": _tab1_dimension_column(dimension),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            "trend_scope": "filtered" if segment_type and segment_value else "overall",
            "previous_month": None,
            "churn_breakdown_month": yyyymm_to_label(target_month),
            "risk_heatmap_month": yyyymm_to_label(target_month),
            "artifact_mode": "artifact_backed",
            "artifact_dir": str(artifact_dir),
        },
        "kpis": {
            "total_expiring_users": 0,
            "historical_churn_rate": 0.0,
            "overall_median_survival": 0.0,
            "auto_renew_rate": 0.0,
            "total_expected_renewal_amount": 0.0,
            "historical_revenue_at_risk": 0.0,
        },
        "previous_kpis": None,
        "monthly_trend": [],
        "churn_breakdown": {
            "renewed_users": 0,
            "churned_users": 0,
            "renewed_rate": 0.0,
            "churned_rate": 0.0,
        },
        "risk_heatmap": [],
        "km_curve": [],
        "segment_mix": [],
        "boredom_scatter": [],
    }


def build_tab1_descriptive_payload(
    month_start: date,
    *,
    dimension: str = "age",
    segment_type: Optional[str] = None,
    segment_value: Optional[str] = None,
    root_hint: str | Path | None = None,
    prefer_cache: bool = True,
) -> dict[str, Any]:
    target_month = month_start_to_yyyymm(month_start)
    tab1_dir = resolve_tab1_artifacts_dir(root_hint=root_hint, score_month=target_month)
    if prefer_cache:
        cache_path = _tab1_payload_cache_path(
            tab1_dir,
            target_month=target_month,
            dimension=dimension,
            segment_type=segment_type,
            segment_value=segment_value,
        )
        if cache_path.exists():
            return _apply_tab1_chart_artifact_context(
                read_json_copy(cache_path),
                tab1_dir=tab1_dir,
                target_month=target_month,
                segment_type=segment_type,
                segment_value=segment_value,
            )
    snapshot_df = _load_tab1_snapshot_df(tab1_dir, target_month)
    filtered = _filter_by_segment(snapshot_df, segment_type, segment_value)
    dimension_col = _tab1_dimension_column(dimension)
    monthly_kpis = _load_tab1_monthly_kpis(tab1_dir)
    previous_month = previous_yyyymm(target_month)
    previous_kpis: dict[str, Any] | None = None
    previous_month_label: str | None = None
    monthly_row = monthly_kpis[monthly_kpis["target_month"] == target_month]
    precomputed_trend = (
        _normalize_tab1_precomputed_monthly_trend(_load_tab1_trend_monthly_summary_df(tab1_dir))
        if not segment_type and not segment_value
        else []
    )
    monthly_trend = precomputed_trend or _build_tab1_monthly_trend_from_kpis(
        tab1_dir,
        monthly_kpis,
        root_hint=root_hint,
        segment_type=segment_type,
        segment_value=segment_value,
    )
    trend_scope = "filtered" if segment_type and segment_value and monthly_trend else "overall"

    if filtered.empty:
        return _empty_tab1_payload(
            target_month=target_month,
            dimension=dimension,
            segment_type=segment_type,
            segment_value=segment_value,
            artifact_dir=tab1_dir,
        )

    if dimension_col not in filtered.columns:
        raise ValueError(f"Khong tim thay cot dimension trong artifact Tab 1: {dimension_col}")

    work = filtered.copy()
    current_kpis = _build_tab1_kpis_from_frame(work)
    previous_columns = {
        "msno",
        "churn_rate",
        "is_churn",
        "survival_days_proxy",
        "is_auto_renew",
        "expected_renewal_amount",
        "expected_revenue_at_risk_30d",
    }
    if segment_type in ALLOWED_SEGMENT_TYPES:
        previous_columns.add(segment_type)
    try:
        previous_snapshot = _load_tab1_snapshot_df(tab1_dir, previous_month, columns=tuple(sorted(previous_columns)))
    except FileNotFoundError:
        previous_snapshot = pd.DataFrame()
    if not previous_snapshot.empty:
        previous_filtered = _filter_by_segment(previous_snapshot, segment_type, segment_value)
        if not previous_filtered.empty:
            previous_kpis = _build_tab1_kpis_from_frame(previous_filtered)
            previous_month_label = yyyymm_to_label(previous_month)
    if previous_kpis is None:
        prior_points = [point for point in monthly_trend if int(point.get("target_month", 0) or 0) < target_month]
        if prior_points:
            previous_point = prior_points[-1]
            previous_kpis = {
                "total_expiring_users": int(previous_point.get("total_expiring_users", 0) or 0),
                "historical_churn_rate": float(previous_point.get("historical_churn_rate", 0.0) or 0.0),
                "overall_median_survival": float(previous_point.get("overall_median_survival", 0.0) or 0.0),
                "auto_renew_rate": float(previous_point.get("auto_renew_rate", 0.0) or 0.0),
                "total_expected_renewal_amount": float(previous_point.get("total_expected_renewal_amount", 0.0) or 0.0),
                "historical_revenue_at_risk": float(previous_point.get("historical_revenue_at_risk", 0.0) or 0.0),
            }
            previous_month_label = str(previous_point.get("month_label") or yyyymm_to_label(int(previous_point["target_month"])))

    segment_frames: list[pd.DataFrame] = []
    for current_segment in sorted(ALLOWED_SEGMENT_TYPES):
        if current_segment not in work.columns:
            continue
        grouped = (
            work.assign(segment_value=work[current_segment].fillna("Unknown").astype(str))
            .groupby("segment_value", as_index=False)
            .agg(
                users=("msno", "nunique"),
                churn_rate_pct=("churn_rate", lambda s: float(s[s >= 0].mean() * 100.0) if (s >= 0).any() else 0.0),
            )
        )
        grouped["retain_rate_pct"] = 100.0 - grouped["churn_rate_pct"]
        grouped["segment_type"] = current_segment
        segment_frames.append(grouped[["segment_type", "segment_value", "users", "churn_rate_pct", "retain_rate_pct"]])

    segment_mix = (
        pd.concat(segment_frames, ignore_index=True)
        .sort_values(["segment_type", "segment_value"])
        .reset_index(drop=True)
        .to_dict(orient="records")
        if segment_frames
        else []
    )
    behavior_clusters = _build_tab1_behavior_clusters(work)

    if segment_type or segment_value:
        km_curve: list[dict[str, Any]] = []
    else:
        km_df = _load_tab1_km_curves(tab1_dir)
        scoped_km = (
            km_df[(km_df["target_month"] == target_month) & (km_df["dimension"] == dimension_col)]
            .sort_values(["dimension_value", "day"])
            .reset_index(drop=True)
        )
        km_curve = []
        for dimension_value, group in scoped_km.groupby("dimension_value", sort=True):
            points = []
            for _, row in group.iterrows():
                points.append(
                    {
                        "day": int(row["day"]),
                        "survival_prob": float(row["survival_prob"]),
                        "at_risk": int(row["at_risk"]),
                        "events": int(row["events"]),
                    }
                )
            km_curve.append({"dimension_value": str(dimension_value), "points": points})
    churn_breakdown = _build_tab1_churn_breakdown(
        int(current_kpis["total_expiring_users"]),
        float(current_kpis["historical_churn_rate"]),
    )
    precomputed_risk_heatmap = (
        _normalize_tab1_precomputed_risk_heatmap(_load_tab1_snapshot_risk_heatmap_df(tab1_dir), target_month)
        if not segment_type and not segment_value
        else []
    )
    risk_heatmap = precomputed_risk_heatmap or _build_tab1_risk_heatmap(work)

    return _apply_tab1_chart_artifact_context(
        {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "dimension": dimension_col,
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            "trend_scope": trend_scope,
            "previous_month": previous_month_label,
            "artifact_mode": "artifact_backed",
            "artifact_dir": str(tab1_dir),
        },
        "kpis": current_kpis,
        "previous_kpis": previous_kpis,
        "monthly_trend": monthly_trend,
        "churn_breakdown": churn_breakdown,
        "risk_heatmap": risk_heatmap,
        "km_curve": km_curve,
        "segment_mix": segment_mix,
        "boredom_scatter": behavior_clusters,
        },
        tab1_dir=tab1_dir,
        target_month=target_month,
        segment_type=segment_type,
        segment_value=segment_value,
    )


def build_dashboard_snapshot_payload(
    month_start: date,
    *,
    root_hint: str | Path | None = None,
    prefer_cache: bool = True,
) -> dict[str, Any]:
    target_month = month_start_to_yyyymm(month_start)
    tab1_dir = resolve_tab1_artifacts_dir(root_hint=root_hint, score_month=target_month)
    if prefer_cache:
        cache_path = _snapshot_payload_cache_path(tab1_dir, target_month=target_month)
        if cache_path.exists():
            return overlay_tab1_preexpiry_pulse(
                read_json_copy(cache_path),
                target_month=target_month,
                root_hint=root_hint,
            )
    snapshot_df = _load_tab1_snapshot_df(tab1_dir, target_month, columns=TAB1_SNAPSHOT_DAILY_COLUMNS)
    monthly = _load_tab1_monthly_kpis(tab1_dir)
    monthly = monthly[monthly["target_month"] == target_month]
    churn_rate_clean = snapshot_df["churn_rate"].where(snapshot_df["churn_rate"] >= 0) if "churn_rate" in snapshot_df.columns else pd.Series(dtype="float64")
    revenue_series, risk_series, activity_series = _build_tab1_daily_snapshot_series(snapshot_df, month_start)

    if not monthly.empty:
        row = monthly.iloc[0]
        historical_churn_rate = float(row.get("historical_churn_rate", 0.0))
        auto_renew_rate = float(row.get("auto_renew_rate", 0.0))
        if historical_churn_rate <= 1.0:
            historical_churn_rate *= 100.0
        if auto_renew_rate <= 1.0:
            auto_renew_rate *= 100.0
        if historical_churn_rate >= 99.0 and not churn_rate_clean.empty and churn_rate_clean.notna().any():
            historical_churn_rate = float(churn_rate_clean.mean() * 100.0)
        metrics = {
            "total_expiring_users": int(row.get("total_expiring_users", 0)),
            "historical_churn_rate": historical_churn_rate,
            "median_survival_days": float(row.get("median_survival_days_proxy", 0.0)),
            "auto_renew_rate": auto_renew_rate,
        }
    else:
        metrics = {
            "total_expiring_users": int(snapshot_df["msno"].nunique()),
            "historical_churn_rate": float(churn_rate_clean.mean() * 100.0) if not churn_rate_clean.empty and churn_rate_clean.notna().any() else 0.0,
            "median_survival_days": float(snapshot_df["survival_days_proxy"].dropna().median()) if snapshot_df["survival_days_proxy"].notna().any() else 0.0,
            "auto_renew_rate": float(snapshot_df["is_auto_renew"].mean() * 100.0),
        }

    manifest_path = tab1_dir / "manifest.json"
    file_times = [path.stat().st_mtime for path in [tab1_dir / "tab1_kpis_monthly.parquet", tab1_dir / f"tab1_snapshot_{target_month}.parquet"] if path.exists()]
    if manifest_path.exists():
        file_times.append(manifest_path.stat().st_mtime)
    as_of = datetime.fromtimestamp(max(file_times), tz=timezone.utc).isoformat() if file_times else datetime.now(timezone.utc).isoformat()
    if month_start.month == 12:
        next_month = month_start.replace(year=month_start.year + 1, month=1)
    else:
        next_month = month_start.replace(month=month_start.month + 1)

    payload = {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "month_start": month_start.isoformat(),
            "month_end_exclusive": next_month.isoformat(),
            "as_of": as_of,
            "artifact_mode": "artifact_backed",
            "series_mode": "expire_day_proxy",
            "artifact_dir": str(tab1_dir),
        },
        "metrics": metrics,
        "revenue_series": revenue_series,
        "risk_series": risk_series,
        "activity_series": activity_series,
    }
    return overlay_tab1_preexpiry_pulse(payload, target_month=target_month, root_hint=root_hint)


def _load_tab2_scored_df(
    tab2_dir: Path,
    target_month: int,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    candidates = [
        tab2_dir / f"tab2_test_scored_{target_month}.parquet",
        tab2_dir / f"tab2_valid_scored_{target_month}.parquet",
    ]
    for path in candidates:
        if path.exists():
            return read_parquet_copy(path, columns=columns)
    raise FileNotFoundError(f"Khong tim thay scored artifact cho thang {target_month} trong {tab2_dir}")


def _load_tab2_segment_summary_df(tab2_dir: Path, target_month: int) -> pd.DataFrame:
    path = tab2_dir / f"tab2_segment_risk_summary_{target_month}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab2 segment summary cho thang {target_month} trong {tab2_dir}")
    return read_parquet_copy(path)


def _load_tab3_segment_impact_df(tab3_dir: Path, target_month: int) -> pd.DataFrame:
    path = tab3_dir / f"tab3_segment_impact_{target_month}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab3 segment impact cho thang {target_month} trong {tab3_dir}")
    return read_parquet_copy(path)


def _load_tab3_population_risk_shift_df(tab3_dir: Path, target_month: int) -> pd.DataFrame:
    path = tab3_dir / f"tab3_population_risk_shift_{target_month}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab3 population risk shift cho thang {target_month} trong {tab3_dir}")
    return read_parquet_copy(path)


def _load_tab3_sensitivity_df(tab3_dir: Path, target_month: int) -> pd.DataFrame:
    path = tab3_dir / f"tab3_sensitivity_{target_month}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab3 sensitivity cho thang {target_month} trong {tab3_dir}")
    return read_parquet_copy(path)


def _load_tab3_scenario_summary(tab3_dir: Path, target_month: int) -> dict[str, Any]:
    path = tab3_dir / f"tab3_scenario_summary_{target_month}.json"
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay tab3 scenario summary cho thang {target_month} trong {tab3_dir}")
    return read_json_copy(path)


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


def _apply_sample_limit(df: pd.DataFrame, sample_limit: Optional[int]) -> pd.DataFrame:
    if sample_limit is None or sample_limit <= 0 or len(df) <= sample_limit:
        return df.reset_index(drop=True)
    if "msno" not in df.columns:
        return df.head(sample_limit).reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(df["msno"].fillna("").astype(str), index=False)
    sampled_index = hashed.sort_values(kind="stable").index[:sample_limit]
    return df.loc[sampled_index].reset_index(drop=True)


def _artifact_segment_parts(segment_name: str) -> dict[str, str]:
    parts = [part.strip() for part in str(segment_name).split("|")]
    fields = {
        "loyalty_segment": "Unknown",
        "renewal_segment": "Unknown",
        "price_segment": "Unknown",
        "discovery_segment": "Unknown",
    }
    for key, value in zip(fields.keys(), parts):
        fields[key] = value or "Unknown"
    return fields


def _with_artifact_segment_columns(df: pd.DataFrame, source_col: str) -> pd.DataFrame:
    if df.empty:
        enriched = df.copy()
        for key in ("loyalty_segment", "renewal_segment", "price_segment", "discovery_segment"):
            enriched[key] = pd.Series(dtype="object")
        return enriched

    parts_df = pd.DataFrame([_artifact_segment_parts(value) for value in df[source_col].fillna("Unknown").astype(str)])
    return pd.concat([df.reset_index(drop=True), parts_df], axis=1)


def _filter_artifact_segment_summary(
    df: pd.DataFrame,
    source_col: str,
    segment_type: Optional[str],
    segment_value: Optional[str],
) -> pd.DataFrame:
    _validate_segment_filter(segment_type, segment_value)
    if not segment_type or not segment_value:
        return _with_artifact_segment_columns(df, source_col).reset_index(drop=True)

    enriched = _with_artifact_segment_columns(df, source_col)
    if segment_type not in enriched.columns:
        return enriched.iloc[0:0].copy()
    return enriched[enriched[segment_type].fillna("Unknown").astype(str) == segment_value].reset_index(drop=True)


def _safe_probability(series: pd.Series) -> pd.Series:
    return series.fillna(0).astype("float32").clip(lower=1e-4, upper=1 - 1e-4)


def _safe_amount(series: pd.Series) -> pd.Series:
    return series.fillna(0).astype("float32").clip(lower=0.0)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _mean_or_default(series: pd.Series, default: float) -> float:
    valid = series.dropna()
    if valid.empty:
        return float(default)
    return float(valid.mean())


def _weighted_mean(values: Iterable[float], weights: Iterable[float], default: float = 0.0) -> float:
    values_arr = np.asarray(list(values), dtype="float64")
    weights_arr = np.asarray(list(weights), dtype="float64")
    total_weight = float(weights_arr.sum())
    if values_arr.size == 0 or total_weight <= 0:
        return float(default)
    return float(np.average(values_arr, weights=weights_arr))


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


def _recommended_action_for_driver(driver: str) -> str:
    return TAB2_RECOMMENDED_ACTIONS.get(driver, TAB2_RECOMMENDED_ACTIONS["Mixed Behavioral Risk"])


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
    grouped["recommended_action"] = grouped["primary_risk_driver"].map(_recommended_action_for_driver)
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


def _primary_risk_driver_from_segment_fields(row: pd.Series) -> str:
    renewal = str(row.get("renewal_segment") or "Unknown")
    discovery = str(row.get("discovery_segment") or "Unknown")
    price = str(row.get("price_segment") or "Unknown")
    loyalty = str(row.get("loyalty_segment") or "Unknown")

    if renewal == "Pay_Manual":
        return "Manual Renewal"
    if discovery == "Habit < 20%":
        return "Low Discovery"
    if price in {"Deal Hunter < 4.5", "Free Trial / Zero Pay"}:
        return "Price Sensitivity"
    if loyalty in {"New < 30d", "Growing 30-179d"}:
        return "Early Lifecycle"
    return "Mixed Behavioral Risk"


def _build_predictive_matrix_from_segment_summary(summary_df: pd.DataFrame) -> list[dict[str, Any]]:
    if summary_df.empty:
        return []

    work = summary_df.copy()
    work["strategic_segment"] = work["bi_segment_name"].fillna("Unknown").astype(str)
    work["user_count"] = work["users"].fillna(0).astype(int)
    work["avg_churn_prob"] = work["avg_churn_probability"].fillna(0.0).astype("float64")
    work["avg_churn_prob_pct"] = work["avg_churn_prob"] * 100.0
    work["total_future_cltv"] = work["total_expected_retained_revenue_30d"].fillna(0.0).astype("float64")
    work["revenue_at_risk"] = work["total_expected_revenue_at_risk_30d"].fillna(0.0).astype("float64")
    work["avg_future_cltv"] = work["total_future_cltv"] / work["user_count"].clip(lower=1)
    work["primary_risk_driver"] = work.apply(_primary_risk_driver_from_segment_fields, axis=1)

    cltv_mid = float(work["avg_future_cltv"].median()) if not work.empty else 0.0
    risk_mid = float(work["avg_churn_prob_pct"].median()) if not work.empty else 0.0
    work["quadrant"] = np.select(
        [
            (work["avg_future_cltv"] >= cltv_mid) & (work["avg_churn_prob_pct"] >= risk_mid),
            (work["avg_future_cltv"] < cltv_mid) & (work["avg_churn_prob_pct"] >= risk_mid),
            (work["avg_future_cltv"] >= cltv_mid) & (work["avg_churn_prob_pct"] < risk_mid),
        ],
        ["Must Save", "At Risk", "Core Value"],
        default="Monitor",
    )
    work["recommended_action"] = work["primary_risk_driver"].map(_recommended_action_for_driver)

    ordered = work.sort_values(["revenue_at_risk", "avg_churn_prob"], ascending=[False, False]).reset_index(drop=True)
    columns = [
        "strategic_segment",
        "user_count",
        "avg_churn_prob",
        "avg_churn_prob_pct",
        "avg_future_cltv",
        "total_future_cltv",
        "revenue_at_risk",
        "primary_risk_driver",
        "recommended_action",
        "quadrant",
    ]
    return ordered[columns].to_dict(orient="records")


def _build_predictive_kpis_from_summary_rows(matrix_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not matrix_rows:
        return _zero_predictive_kpis()

    total_users = sum(max(int(row.get("user_count", 0)), 0) for row in matrix_rows)
    weighted_prob = _weighted_mean(
        [float(row.get("avg_churn_prob", 0.0)) for row in matrix_rows],
        [max(float(row.get("user_count", 0)), 0.0) for row in matrix_rows],
        default=0.0,
    )
    top = matrix_rows[0]
    return {
        "forecasted_churn_rate": float(weighted_prob * 100.0),
        "predicted_revenue_at_risk": float(sum(float(row.get("revenue_at_risk", 0.0)) for row in matrix_rows)),
        "predicted_total_future_cltv": float(sum(float(row.get("total_future_cltv", 0.0)) for row in matrix_rows)),
        "top_segment": str(top.get("strategic_segment") or "N/A"),
        "top_segment_risk": float(top.get("revenue_at_risk", 0.0)),
        "top_segment_user_count": max(int(top.get("user_count", 0)), 0),
        "forecasted_churn_delta_pp_vs_prev_month": 0.0,
    }


def _build_previous_predictive_kpis_from_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    selected_metrics = metrics.get("selected_metrics", {})
    prev_rate = float(selected_metrics.get("prediction_mean", 0.0) or 0.0)
    if prev_rate <= 1.0:
        prev_rate *= 100.0
    previous = _zero_predictive_kpis()
    previous["forecasted_churn_rate"] = prev_rate
    return previous


def _build_forecast_decay_from_summary_rows(matrix_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    focus_rows = matrix_rows[:4]
    rows: list[dict[str, Any]] = []
    for row in focus_rows:
        segment = str(row.get("strategic_segment") or "Unknown")
        avg_prob = float(row.get("avg_churn_prob", 0.0))
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


def _build_revenue_leakage_from_summary_rows(matrix_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not matrix_rows:
        return []
    grouped: dict[str, dict[str, Any]] = {}
    for row in matrix_rows:
        driver = str(row.get("primary_risk_driver") or "Unknown")
        current = grouped.setdefault(driver, {"risk_driver": driver, "user_count": 0, "revenue_at_risk": 0.0})
        current["user_count"] += max(int(row.get("user_count", 0)), 0)
        current["revenue_at_risk"] += float(row.get("revenue_at_risk", 0.0))
    return sorted(grouped.values(), key=lambda item: float(item["revenue_at_risk"]), reverse=True)


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


def _to_executive_risk_long_label(series: pd.Series) -> pd.Series:
    return _to_executive_risk_band(series).map(TAB2_EXECUTIVE_RISK_LONG_LABELS).fillna(TAB2_EXECUTIVE_RISK_LONG_LABELS["Unknown"])


def _build_risk_band_mix(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty or "risk_band" not in df.columns:
        return []

    work = df.copy()
    work["executive_risk_band"] = _to_executive_risk_band(work["risk_band"])
    grouped = (
        work.groupby("executive_risk_band", as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
        )
        .rename(columns={"executive_risk_band": "band"})
    )

    total_risk = float(grouped["revenue_at_risk"].sum()) if not grouped.empty else 0.0
    grouped["revenue_share_pct"] = (
        grouped["revenue_at_risk"] / total_risk * 100.0 if total_risk > 0 else 0.0
    )

    order = {band: idx for idx, band in enumerate(TAB2_EXECUTIVE_RISK_ORDER)}
    grouped["sort_key"] = grouped["band"].map(order).fillna(99)
    grouped = grouped.sort_values("sort_key").drop(columns=["sort_key"]).reset_index(drop=True)
    return grouped.to_dict(orient="records")


def _load_tab2_feature_importance_df(tab2_dir: Path) -> pd.DataFrame:
    for file_name in ("tab2_feature_importance_lightgbm.csv", "tab2_feature_importance_xgboost.csv"):
        path = tab2_dir / file_name
        if path.exists():
            return read_csv_copy(path)
    raise FileNotFoundError(f"Khong tim thay feature importance CSV trong {tab2_dir}")


def _build_feature_group_waterfall(feature_importance_df: pd.DataFrame, total_risk: float) -> list[dict[str, Any]]:
    if feature_importance_df.empty or total_risk <= 0:
        return []

    grouped = (
        feature_importance_df.groupby("feature_group", as_index=False)
        .agg(
            importance_gain=("importance_gain", "sum"),
            importance_split=("importance_split", "sum"),
            feature_count=("feature", "nunique"),
        )
        .sort_values("importance_gain", ascending=False)
        .reset_index(drop=True)
    )
    grouped = grouped[grouped["importance_gain"] > 0].reset_index(drop=True)
    total_gain = float(grouped["importance_gain"].sum()) if not grouped.empty else 0.0
    if total_gain <= 0:
        return []

    base_risk_ratio = 0.30
    base_risk_val = float(total_risk * base_risk_ratio)
    explained_risk_val = float(total_risk - base_risk_val)
    rows: list[dict[str, Any]] = []
    rows.append(
        {
            "feature_group": "base_risk",
            "display_name": TAB2_FEATURE_GROUP_LABELS["base_risk"],
            "contribution": base_risk_val,
            "contribution_pct": float(base_risk_ratio * 100.0),
            "importance_gain": 0.0,
            "importance_split": 0,
            "feature_count": 0,
        }
    )
    for row in grouped.itertuples(index=False):
        feature_group = str(getattr(row, "feature_group") or "other")
        contribution = float(explained_risk_val * float(getattr(row, "importance_gain")) / total_gain)
        rows.append(
            {
                "feature_group": feature_group,
                "display_name": TAB2_FEATURE_GROUP_LABELS.get(feature_group, feature_group.replace("_", " ").title()),
                "contribution": contribution,
                "contribution_pct": float(contribution / total_risk * 100.0) if total_risk > 0 else 0.0,
                "importance_gain": float(getattr(row, "importance_gain", 0.0) or 0.0),
                "importance_split": int(getattr(row, "importance_split", 0) or 0),
                "feature_count": int(getattr(row, "feature_count", 0) or 0),
            }
        )
    return rows


def _build_executive_value_risk_matrix(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    work = df.copy()
    work["prob_bin"] = pd.to_numeric(work["churn_probability"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).round(1)
    work["expected_renewal_amount"] = pd.to_numeric(work["expected_renewal_amount"], errors="coerce").fillna(0.0).clip(lower=0.0)
    grouped = (
        work.groupby(["prob_bin", "expected_renewal_amount", "risk_band"], as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
        )
        .sort_values(["prob_bin", "expected_renewal_amount", "user_count"], ascending=[True, True, False])
        .reset_index(drop=True)
    )
    grouped = grouped.loc[
        (grouped["expected_renewal_amount"] <= TAB2_EXECUTIVE_MATRIX_MAX_RENEWAL_AMOUNT)
        & (grouped["user_count"] >= TAB2_EXECUTIVE_MATRIX_MIN_USER_COUNT)
    ].copy()
    if grouped.empty:
        return []
    grouped["risk_tier"] = _to_executive_risk_band(grouped["risk_band"])
    grouped["display_size"] = np.sqrt(grouped["user_count"].clip(lower=1)).astype("float64")
    grouped["priority_quadrant"] = np.select(
        [
            (grouped["prob_bin"] >= 0.5) & (grouped["expected_renewal_amount"] >= 100.0),
            (grouped["prob_bin"] >= 0.5) & (grouped["expected_renewal_amount"] < 100.0),
            (grouped["prob_bin"] < 0.5) & (grouped["expected_renewal_amount"] >= 100.0),
        ],
        ["VIP Rescue", "Price Sensitive", "Core Loyal"],
        default="Casual",
    )
    return grouped.to_dict(orient="records")


def _build_revenue_flow_sankey(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"nodes": [], "links": []}

    renewal_map = {
        "Pay_Manual": "Thủ công",
        "Pay_Auto-Renew": "Tự động",
    }
    rfm_map = {
        "High Value": "RFM: Cao",
        "Mid Value": "RFM: Vừa",
        "Low Value": "RFM: Thấp",
        "Unclassified": "RFM: Khác",
    }
    price_map = {
        "Free Trial / Zero Pay": "Dùng thử",
        "Deal Hunter < 4.5": "Săn ưu đãi",
        "Standard 4.5-6.5": "Tiêu chuẩn",
        "Premium >= 6.5": "Cao cấp",
    }

    work = df.copy()
    work["renewal_bucket"] = work["renewal_segment"].map(renewal_map)
    work["rfm_bucket"] = work["rfm_segment"].map(rfm_map)
    work["price_bucket"] = work["price_segment"].map(price_map)
    work["risk_bucket"] = _to_executive_risk_long_label(work["risk_band"])
    work = work.dropna(subset=["renewal_bucket", "rfm_bucket", "price_bucket"])
    work = work[work["risk_bucket"].isin(TAB2_EXECUTIVE_RISK_LONG_LABELS.values())].reset_index(drop=True)

    if work.empty:
        return {"nodes": [], "links": []}

    node_names = [
        "Thủ công",
        "Tự động",
        "RFM: Cao",
        "RFM: Vừa",
        "RFM: Thấp",
        "RFM: Khác",
        "Dùng thử",
        "Săn ưu đãi",
        "Tiêu chuẩn",
        "Cao cấp",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["High"],
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Medium"],
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Low"],
    ]
    node_colors = {
        "Thủ công": "#1f2937",
        "Tự động": "#94a3b8",
        "RFM: Cao": "#64748b",
        "RFM: Vừa": "#94a3b8",
        "RFM: Thấp": "#cbd5e1",
        "RFM: Khác": "#e2e8f0",
        "Dùng thử": "#38bdf8",
        "Săn ưu đãi": "#f59e0b",
        "Tiêu chuẩn": "#0f766e",
        "Cao cấp": "#1d4ed8",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Low"]: "#10b981",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Medium"]: "#f59e0b",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["High"]: "#ef4444",
    }
    link_colors = {
        TAB2_EXECUTIVE_RISK_LONG_LABELS["High"]: "rgba(239, 68, 68, 0.36)",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Medium"]: "rgba(245, 158, 11, 0.34)",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Low"]: "rgba(16, 185, 129, 0.34)",
        TAB2_EXECUTIVE_RISK_LONG_LABELS["Unknown"]: "rgba(148, 163, 184, 0.28)",
    }
    node_index = {name: idx for idx, name in enumerate(node_names)}

    def grouped_links(source_col: str, target_col: str) -> list[dict[str, Any]]:
        group_cols = list(dict.fromkeys([source_col, target_col, "risk_bucket"]))
        grouped = (
            work.groupby(group_cols, as_index=False)
            .agg(value=("expected_revenue_at_risk_30d", "sum"))
            .sort_values(["value", "risk_bucket"], ascending=[False, True])
            .reset_index(drop=True)
        )
        return [
            {
                "source": node_index[str(row[source_col])],
                "target": node_index[str(row[target_col])],
                "value": float(row["value"]),
                "color": link_colors.get(str(row["risk_bucket"]), link_colors[TAB2_EXECUTIVE_RISK_LONG_LABELS["Unknown"]]),
                "risk_tier": str(row["risk_bucket"]),
            }
            for _, row in grouped.iterrows()
            if float(row["value"]) > 0
        ]

    return {
        "nodes": [{"name": name, "color": node_colors.get(name)} for name in node_names],
        "links": [
            *grouped_links("renewal_bucket", "rfm_bucket"),
            *grouped_links("rfm_bucket", "price_bucket"),
            *grouped_links("price_bucket", "risk_bucket"),
        ],
    }


def _build_price_paradox(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    price_map = {
        "Free Trial / Zero Pay": "Dùng thử (0đ)",
        "Deal Hunter < 4.5": "Săn ưu đãi",
        "Standard 4.5-6.5": "Gói Tiêu chuẩn",
        "Premium >= 6.5": "Gói Cao cấp",
    }
    work = df.copy()
    work["price_bucket"] = work["price_segment"].map(price_map)
    work = work.dropna(subset=["price_bucket"]).reset_index(drop=True)
    if work.empty:
        return []

    grouped = (
        work.groupby("price_bucket", as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
            churn_rate_pct=("churn_probability", lambda s: float(s.mean() * 100.0)),
        )
    )
    order = {"Dùng thử (0đ)": 0, "Săn ưu đãi": 1, "Gói Tiêu chuẩn": 2, "Gói Cao cấp": 3}
    grouped["sort_key"] = grouped["price_bucket"].map(order).fillna(99)
    grouped = grouped.sort_values("sort_key").drop(columns=["sort_key"]).reset_index(drop=True)
    return grouped.to_dict(orient="records")


def _build_habit_funnel(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    active_map = {
        "Heavy > 15 logs": "1. Nhóm Nhiệt huyết (>15 ngày active)",
        "Active 6-15 logs": "2. Nhóm Ổn định (6-15 ngày active)",
        "Light 1-5 logs": "3. Nhóm Nguội lạnh (1-5 ngày active)",
    }
    work = df.copy()
    work["habit_stage"] = work["active_segment"].map(active_map)
    work = work.dropna(subset=["habit_stage"]).reset_index(drop=True)
    if work.empty:
        return []

    grouped = (
        work.groupby("habit_stage", as_index=False)
        .agg(
            user_count=("msno", "nunique"),
            revenue_at_risk=("expected_revenue_at_risk_30d", "sum"),
        )
    )
    order = {
        "1. Nhóm Nhiệt huyết (>15 ngày active)": 0,
        "2. Nhóm Ổn định (6-15 ngày active)": 1,
        "3. Nhóm Nguội lạnh (1-5 ngày active)": 2,
    }
    grouped["sort_key"] = grouped["habit_stage"].map(order).fillna(99)
    grouped = grouped.sort_values("sort_key").drop(columns=["sort_key"]).reset_index(drop=True)

    top_users = max(int(grouped["user_count"].max()), 1)
    grouped["share_of_top_pct"] = grouped["user_count"] / top_users * 100.0
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


def _build_revenue_loss_outlook(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    expected_renewal_amount = pd.to_numeric(df["expected_renewal_amount"], errors="coerce").fillna(0.0).clip(lower=0.0)
    churn_probability = pd.to_numeric(df["churn_probability"], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0)
    if expected_renewal_amount.sum() <= 0:
        return []

    rows: list[dict[str, Any]] = []
    total_expected = float(expected_renewal_amount.sum())
    for horizon in (3, 6, 12):
        cumulative_loss_rate = 1.0 - np.power(1.0 - churn_probability, horizon)
        projected_revenue_loss = float((expected_renewal_amount * cumulative_loss_rate).sum())
        rows.append(
            {
                "horizon_months": horizon,
                "horizon_label": f"{horizon} tháng",
                "projected_revenue_loss": projected_revenue_loss,
                "projected_loss_share_pct": float(projected_revenue_loss / total_expected * 100.0) if total_expected > 0 else 0.0,
            }
        )
    return rows


def _zero_predictive_kpis() -> dict[str, Any]:
    return {
        "forecasted_churn_rate": 0.0,
        "high_flight_risk_users": 0,
        "predicted_revenue_at_risk": 0.0,
        "predicted_total_future_cltv": 0.0,
        "safe_revenue": 0.0,
        "top_segment": "N/A",
        "top_segment_risk": 0.0,
        "top_segment_user_count": 0,
        "forecasted_churn_delta_pp_vs_prev_month": 0.0,
    }


def _compute_predictive_kpis(df: pd.DataFrame, matrix_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if df.empty:
        return _zero_predictive_kpis()

    top = matrix_rows[0] if matrix_rows else None
    high_risk_mask = pd.to_numeric(df["churn_probability"], errors="coerce").fillna(0.0) >= 0.60
    return {
        "forecasted_churn_rate": float(high_risk_mask.mean() * 100.0),
        "high_flight_risk_users": int(high_risk_mask.sum()),
        "predicted_revenue_at_risk": float(df["expected_revenue_at_risk_30d"].sum()),
        "predicted_total_future_cltv": float(df["expected_retained_revenue_30d"].sum()),
        "safe_revenue": float(df["expected_retained_revenue_30d"].sum()),
        "top_segment": str(top["strategic_segment"]) if top else "N/A",
        "top_segment_risk": float(top["revenue_at_risk"]) if top else 0.0,
        "top_segment_user_count": int(top["user_count"]) if top else 0,
        "forecasted_churn_delta_pp_vs_prev_month": 0.0,
    }


def build_tab2_predictive_payload(
    month_start: date,
    segment_type: Optional[str] = None,
    segment_value: Optional[str] = None,
    sample_limit: Optional[int] = None,
    root_hint: str | Path | None = None,
    prefer_cache: bool = True,
) -> dict[str, Any]:
    target_month = month_start_to_yyyymm(month_start)
    tab2_dir = resolve_tab2_artifacts_dir(root_hint=root_hint, score_month=target_month)
    if prefer_cache and sample_limit in (None, 0):
        cache_path = _tab2_payload_cache_path(
            tab2_dir,
            target_month=target_month,
            segment_type=segment_type,
            segment_value=segment_value,
        )
        if cache_path.exists():
            return read_json_copy(cache_path)
    summary_df = _load_tab2_segment_summary_df(tab2_dir, target_month)
    scored_df = _load_tab2_scored_df(tab2_dir, target_month, columns=TAB2_SCORE_COLUMNS)
    metrics = read_json_copy(tab2_dir / "tab2_validation_metrics.json")
    model_summary = read_json_copy(tab2_dir / "tab2_model_summary.json")
    try:
        feature_importance_df = _load_tab2_feature_importance_df(tab2_dir)
    except FileNotFoundError:
        feature_importance_df = pd.DataFrame(columns=["feature", "importance_gain", "importance_split", "feature_group"])
    filtered_summary = _filter_artifact_segment_summary(summary_df, "bi_segment_name", segment_type, segment_value)
    filtered_scored = _filter_by_segment(scored_df, segment_type, segment_value)

    matrix_rows = _build_predictive_matrix_from_segment_summary(filtered_summary)
    precomputed_executive_matrix = (
        _normalize_tab2_executive_value_risk_matrix(_load_tab2_executive_value_risk_matrix_df(tab2_dir, target_month), target_month)
        if not segment_type and not segment_value
        else []
    )
    executive_matrix_rows = precomputed_executive_matrix or _build_executive_value_risk_matrix(filtered_scored)
    current_kpis = _compute_predictive_kpis(filtered_scored, matrix_rows)

    previous_month = previous_yyyymm(target_month)
    previous_kpis = _build_previous_predictive_kpis_from_metrics(metrics)
    current_kpis["forecasted_churn_delta_pp_vs_prev_month"] = (
        current_kpis["forecasted_churn_rate"] - previous_kpis["forecasted_churn_rate"]
    )

    return {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "previous_month": yyyymm_to_label(previous_month),
            "sample_user_count": int(
                model_summary.get("scored_test_rows", 0)
                if not segment_type and not segment_value
                else (filtered_summary["users"].sum() if not filtered_summary.empty else 0)
            ),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            "artifact_dir": str(tab2_dir),
            "artifact_mode": "summary_backed",
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
        "executive_value_risk_matrix": executive_matrix_rows,
        "revenue_leakage": _build_revenue_leakage(filtered_scored),
        "forecast_decay": _build_forecast_decay(filtered_scored),
        "revenue_loss_outlook": _build_revenue_loss_outlook(filtered_scored),
        "prescriptions": matrix_rows[:200],
        "risk_band_mix": _build_risk_band_mix(filtered_scored),
        "feature_group_waterfall": _build_feature_group_waterfall(
            feature_importance_df,
            float(current_kpis["predicted_revenue_at_risk"]),
        ),
        "revenue_flow_sankey": _build_revenue_flow_sankey(filtered_scored),
        "price_paradox": _build_price_paradox(filtered_scored),
        "habit_funnel": _build_habit_funnel(filtered_scored),
        "feature_group_importance": feature_importance_df.to_dict(orient="records"),
    }


def load_feature_snapshot(
    feature_store_dir: str | Path,
    score_month: int,
    columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    feature_store_dir = Path(feature_store_dir)
    score_path = feature_store_dir / f"test_features_bi_{score_month}_full.parquet"
    if score_path.exists():
        df = read_parquet_copy(score_path, columns=columns)
        if "target_month" in df.columns:
            df = df[df["target_month"] == score_month].reset_index(drop=True)
        return df

    master_path = feature_store_dir / "bi_feature_master.parquet"
    if master_path.exists():
        df = read_parquet_copy(master_path, columns=columns)
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
    feature_df = load_feature_snapshot(feature_store_dir_text, target_month, columns=FEATURE_SNAPSHOT_COLUMNS)
    scored_df = _load_tab2_scored_df(Path(tab2_dir_text), target_month, columns=TAB3_SCORED_COLUMNS)
    return _prepare_baseline_dataframe(feature_df, scored_df)


def _build_baseline_frame(
    feature_store_dir_text: str,
    tab2_dir_text: str,
    target_month: int,
    sample_limit: Optional[int],
) -> pd.DataFrame:
    if sample_limit is None or sample_limit <= 0:
        return _cached_baseline_frame(feature_store_dir_text, tab2_dir_text, target_month).copy()

    feature_df = load_feature_snapshot(feature_store_dir_text, target_month, columns=FEATURE_SNAPSHOT_COLUMNS)
    scored_df = _load_tab2_scored_df(Path(tab2_dir_text), target_month, columns=TAB3_SCORED_COLUMNS)
    scored_df = _apply_sample_limit(scored_df, sample_limit)
    feature_df = feature_df[feature_df["msno"].isin(scored_df["msno"])].reset_index(drop=True)
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


def _default_monte_carlo_payload() -> dict[str, Any]:
    return {
        "enabled": False,
        "artifact_dir": None,
        "n_iterations": 0,
        "seed": None,
        "beta_concentration": None,
        "population_users": 0,
        "simulation_unit_count": 0,
        "probability_scenario_beats_baseline": None,
        "probability_net_positive": None,
        "deterministic_summary": {},
        "summary_metrics": [],
        "net_value_distribution": [],
    }


def _build_monte_carlo_net_value_distribution(mc_dir: Path, target_month: int) -> list[dict[str, Any]]:
    path = mc_dir / f"tab3_monte_carlo_runs_{target_month}.parquet"
    if not path.exists():
        return []

    try:
        runs_df = pd.read_parquet(path, columns=["net_value_after_cost_30d"])
    except Exception:
        return []

    values = pd.to_numeric(runs_df.get("net_value_after_cost_30d"), errors="coerce").dropna()
    if values.empty:
        return []

    numeric = values.to_numpy(dtype=float)
    lower = float(np.min(numeric))
    upper = float(np.max(numeric))

    if np.isclose(lower, upper):
        total = int(numeric.size)
        return [
            {
                "bucket_start": lower,
                "bucket_end": upper,
                "bucket_mid": lower,
                "bucket_label": f"{lower:,.0f}",
                "run_count": total,
                "share_pct": 100.0,
            }
        ]

    bin_count = int(np.clip(np.sqrt(numeric.size), 10, 20))
    counts, edges = np.histogram(numeric, bins=bin_count)
    total = max(int(counts.sum()), 1)

    rows: list[dict[str, Any]] = []
    for index, count in enumerate(counts):
        bucket_start = float(edges[index])
        bucket_end = float(edges[index + 1])
        rows.append(
            {
                "bucket_start": bucket_start,
                "bucket_end": bucket_end,
                "bucket_mid": float((bucket_start + bucket_end) / 2.0),
                "bucket_label": f"{bucket_start:,.0f} -> {bucket_end:,.0f}",
                "run_count": int(count),
                "share_pct": float(count / total * 100.0),
            }
        )
    return rows


def _build_monte_carlo_payload(mc_dir: Path, target_month: int) -> dict[str, Any]:
    summary = read_json_copy(mc_dir / f"tab3_monte_carlo_summary_{target_month}.json")
    deterministic = read_json_copy(mc_dir / f"tab3_deterministic_summary_{target_month}.json")
    manifest = read_json_copy(mc_dir / "manifest.json")
    metrics_map = summary.get("monte_carlo_metrics", {})
    metric_label_map = {
        "baseline_retained_revenue_30d": "Baseline Revenue",
        "scenario_retained_revenue_30d": "Scenario Revenue",
        "incremental_upsell_revenue_30d": "Incremental Upsell Revenue",
        "saved_revenue_from_risk_reduction_30d": "Saved Revenue from Risk Reduction",
        "campaign_cost_30d": "Campaign Cost",
        "net_value_after_cost_30d": "Net Value After Cost",
        "baseline_churn_prob_pct": "Baseline Churn %",
        "scenario_churn_prob_pct": "Scenario Churn %",
    }
    metric_order = [
        "baseline_retained_revenue_30d",
        "scenario_retained_revenue_30d",
        "incremental_upsell_revenue_30d",
        "saved_revenue_from_risk_reduction_30d",
        "campaign_cost_30d",
        "net_value_after_cost_30d",
        "baseline_churn_prob_pct",
        "scenario_churn_prob_pct",
    ]
    ordered_keys = [key for key in metric_order if key in metrics_map] + [
        key for key in metrics_map.keys() if key not in metric_order
    ]
    summary_metrics = []
    for key in ordered_keys:
        value = metrics_map.get(key, {})
        if not isinstance(value, dict):
            continue
        summary_metrics.append(
            {
                "metric": metric_label_map.get(key, key),
                "column": key,
                "mean": float(value.get("mean", 0.0)),
                "std": float(value.get("std", 0.0)),
                "p05": float(value.get("p05", 0.0)),
                "p25": float(value.get("p25", 0.0)),
                "p50": float(value.get("p50", 0.0)),
                "p75": float(value.get("p75", 0.0)),
                "p95": float(value.get("p95", 0.0)),
            }
        )

    return {
        "enabled": True,
        "artifact_dir": str(mc_dir),
        "n_iterations": int(summary.get("n_iterations", 0)),
        "seed": int(summary.get("seed", 0)) if summary.get("seed") is not None else None,
        "beta_concentration": float(summary.get("beta_concentration", 0.0))
        if summary.get("beta_concentration") is not None
        else None,
        "population_users": int(manifest.get("metadata", {}).get("population_users", 0)),
        "simulation_unit_count": int(manifest.get("metadata", {}).get("simulation_unit_count", 0)),
        "probability_scenario_beats_baseline": float(summary.get("probability_scenario_beats_baseline", 0.0)),
        "probability_net_positive": float(summary.get("probability_net_positive", 0.0)),
        "deterministic_summary": deterministic,
        "summary_metrics": summary_metrics,
        "net_value_distribution": _build_monte_carlo_net_value_distribution(mc_dir, target_month),
    }


def _risk_histogram_payload(risk_shift_df: pd.DataFrame) -> list[dict[str, Any]]:
    if risk_shift_df.empty:
        return []
    scoped = risk_shift_df[risk_shift_df["bin_type"].fillna("probability_bin").astype(str) == "probability_bin"].copy()
    if scoped.empty:
        return []
    base = scoped[scoped["state"] == "baseline"].set_index("probability_bin")["users"]
    scenario = scoped[scoped["state"] == "simulated"].set_index("probability_bin")["users"]
    total_base = max(float(base.sum()), 1.0)
    total_scenario = max(float(scenario.sum()), 1.0)
    rows: list[dict[str, Any]] = []
    for label in sorted(set(base.index).union(set(scenario.index))):
        if "-" not in str(label):
            continue
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
    scenario_id: Optional[str] = None,
    feature_store_root_hint: str | Path | None = None,
    tab2_root_hint: str | Path | None = None,
    sample_limit: Optional[int] = None,
    auto_shift_pct: float = 20.0,
    upsell_shift_pct: float = 15.0,
    skip_shift_pct: float = 25.0,
) -> dict[str, Any]:
    target_month = month_start_to_yyyymm(month_start)
    tab3_root = resolve_tab3_artifacts_dir(root_hint=tab2_root_hint, score_month=target_month)
    prescriptive_catalog = _load_tab3_scenario_catalog(tab3_root)
    tab3_dir, selected_scenario = _resolve_tab3_case_dir(
        tab3_root,
        score_month=target_month,
        scenario_id=scenario_id,
        catalog=prescriptive_catalog,
        required_files=_required_tab3_prescriptive_files(target_month),
        subdir_field="artifact_subdir",
    )
    summary = _load_tab3_scenario_summary(tab3_dir, target_month)
    risk_shift_df = _load_tab3_population_risk_shift_df(tab3_dir, target_month)
    sensitivity_df = _load_tab3_sensitivity_df(tab3_dir, target_month)

    monte_carlo_catalog = None
    try:
        mc_root = resolve_tab3_monte_carlo_dir(root_hint=tab2_root_hint, score_month=target_month)
        monte_carlo_catalog = _load_tab3_scenario_catalog(mc_root)
        mc_dir, _ = _resolve_tab3_case_dir(
            mc_root,
            score_month=target_month,
            scenario_id=(selected_scenario or {}).get("scenario_id") if selected_scenario else scenario_id,
            catalog=monte_carlo_catalog,
            required_files=_required_tab3_monte_carlo_files(target_month),
            subdir_field="monte_carlo_subdir",
        )
        monte_carlo = _build_monte_carlo_payload(mc_dir, target_month)
    except (FileNotFoundError, ValueError):
        mc_dir = None
        monte_carlo = _default_monte_carlo_payload()

    scenario_config = _normalize_config(summary.get("scenario_config", {}))
    if monte_carlo.get("enabled"):
        deterministic_summary = monte_carlo.get("deterministic_summary", {})
        if deterministic_summary:
            baseline_churn_pct = deterministic_summary.get(
                "baseline_churn_prob_pct",
                deterministic_summary.get("baseline_avg_churn_probability", 0.0) * 100.0,
            )
            scenario_churn_pct = deterministic_summary.get(
                "scenario_churn_prob_pct",
                deterministic_summary.get("simulated_avg_churn_probability", 0.0) * 100.0,
            )
            summary["baseline_avg_churn_probability"] = float(baseline_churn_pct) / 100.0
            summary["simulated_avg_churn_probability"] = float(scenario_churn_pct) / 100.0
            summary["baseline_retained_revenue_30d"] = float(
                deterministic_summary.get("baseline_retained_revenue_30d", summary["baseline_retained_revenue_30d"])
            )
            summary["simulated_retained_revenue_30d"] = float(
                deterministic_summary.get(
                    "scenario_retained_revenue_30d",
                    deterministic_summary.get("simulated_retained_revenue_30d", summary["simulated_retained_revenue_30d"]),
                )
            )
            summary["saved_revenue_from_risk_reduction_30d"] = float(
                deterministic_summary.get(
                    "saved_revenue_from_risk_reduction_30d", summary["saved_revenue_from_risk_reduction_30d"]
                )
            )
            summary["incremental_upsell_revenue_30d"] = float(
                deterministic_summary.get("incremental_upsell_revenue_30d", summary["incremental_upsell_revenue_30d"])
            )
            summary["campaign_cost_30d"] = float(
                deterministic_summary.get("campaign_cost_30d", summary["campaign_cost_30d"])
            )
            summary["net_value_after_cost_30d"] = float(
                deterministic_summary.get("net_value_after_cost_30d", summary["net_value_after_cost_30d"])
            )
            summary["retained_revenue_delta_30d"] = (
                summary["simulated_retained_revenue_30d"] - summary["baseline_retained_revenue_30d"]
            )

    available_scenarios = _build_tab3_scenario_options(
        selected_entry=selected_scenario,
        selected_summary=summary,
        prescriptive_catalog=prescriptive_catalog,
        monte_carlo_catalog=monte_carlo_catalog,
        monte_carlo_enabled=bool(monte_carlo.get("enabled")),
    )
    selected_scenario_id = (
        str((selected_scenario or {}).get("scenario_id") or available_scenarios[0]["scenario_id"])
        if available_scenarios
        else DEFAULT_TAB3_SCENARIO_ID
    )
    selected_scenario_label = (
        str((selected_scenario or {}).get("label") or available_scenarios[0]["label"])
        if available_scenarios
        else "Default scenario"
    )
    selected_scenario_description = (selected_scenario or {}).get("description")

    return {
        "meta": {
            "month": yyyymm_to_label(target_month),
            "sample_user_count": int(summary.get("population_users", monte_carlo.get("population_users", 0) or 0)),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            "artifact_mode": "summary_backed",
            "artifact_dir": str(tab3_dir),
            "monte_carlo_artifact_dir": str(mc_dir) if mc_dir is not None else None,
            "scenario_id": selected_scenario_id,
            "scenario_label": selected_scenario_label,
            "scenario_description": selected_scenario_description,
            "available_scenarios": available_scenarios,
        },
        "model_params": {**deepcopy(DEFAULT_MODEL_PARAMS), "source_mode": "artifact_backed"},
        "scenario_inputs": {
            "auto_shift_pct": float(scenario_config.get("manual_to_auto_share", 0.0) * 100.0),
            "upsell_shift_pct": float(scenario_config.get("upsell_share", 0.0) * 100.0),
            "skip_shift_pct": float(scenario_config.get("engagement_share", 0.0) * 100.0),
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
            "campaign_cost": float(summary["campaign_cost_30d"]),
            "net_value_after_cost": float(summary["net_value_after_cost_30d"]),
        },
        "hazard_histogram": _risk_histogram_payload(risk_shift_df),
        "financial_waterfall": [
            {"name": "Current Baseline Revenue", "value": float(summary["baseline_retained_revenue_30d"])},
            {"name": "Saved Revenue from Retention", "value": float(summary["saved_revenue_from_risk_reduction_30d"])},
            {"name": "Incremental Revenue from Upsell", "value": float(summary["incremental_upsell_revenue_30d"])},
            {"name": "Scenario Revenue", "value": float(summary["simulated_retained_revenue_30d"])},
            {"name": "Campaign Cost", "value": float(-summary["campaign_cost_30d"])},
            {"name": "Net Value After Cost", "value": float(summary["net_value_after_cost_30d"])},
        ],
        "sensitivity_roi": _sensitivity_payload(sensitivity_df),
        "monte_carlo": monte_carlo,
    }
