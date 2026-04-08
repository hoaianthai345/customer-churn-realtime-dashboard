#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import http.client  # noqa: F401
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import clickhouse_connect
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from .artifact_backend import (
    available_tab1_months,
    available_tab2_months,
    available_tab3_months,
    build_dashboard_snapshot_payload,
    build_tab1_descriptive_payload,
    build_tab2_predictive_payload,
    build_tab3_prescriptive_payload,
)


def _first_day_of_month(year: int, month: int) -> date:
    return date(year=year, month=month, day=1)


def _next_month_start(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def _shift_month(month_start: date, delta_months: int) -> date:
    month_index = month_start.year * 12 + (month_start.month - 1) + delta_months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _date_to_iso(value: date) -> str:
    return value.isoformat()


def _datetime_to_iso(value: Optional[datetime]) -> str:
    if value is None:
        return "1970-01-01T00:00:00"
    if value.tzinfo is None:
        return value.replace(tzinfo=None).isoformat()
    return value.isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _sql_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return min(max(value, min_value), max_value)


def _median(values: Iterable[float]) -> float:
    sorted_values = sorted(float(v) for v in values)
    if not sorted_values:
        return 0.0
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


def _mode_or_default(values: Iterable[str], default: str = "Unknown") -> str:
    counted = Counter(str(v) for v in values if str(v))
    if not counted:
        return default
    return counted.most_common(1)[0][0]


def _cors_origins_from_env() -> list[str]:
    raw = os.getenv("API_CORS_ORIGINS")
    if raw:
        return [origin.strip() for origin in raw.split(",") if origin.strip()]
    return [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://0.0.0.0:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ]


@dataclass(frozen=True)
class PredictiveModelParams:
    base_prob: float = 0.05
    weight_manual: float = 0.30
    weight_low_activity: float = 0.25
    weight_high_skip: float = 0.15
    weight_low_discovery: float = 0.10
    weight_cancel_signal: float = 0.15
    prob_min: float = 0.01
    prob_max: float = 0.99
    cltv_base_months: float = 6.0
    cltv_retention_months: float = 6.0
    cltv_txn_gain: float = 0.03
    risk_horizon_months: float = 6.0
    hazard_base: float = 1.0
    hazard_churn_weight: float = 1.5
    hazard_skip_weight: float = 0.3
    hazard_low_activity_weight: float = 0.2


TAB1_DIMENSION_FIELD_MAP = {
    "age": "age_bucket",
    "age_bucket": "age_bucket",
    "gender": "gender_bucket",
    "gender_bucket": "gender_bucket",
    "txn_freq": "txn_freq_bucket",
    "txn_freq_bucket": "txn_freq_bucket",
    "skip_ratio": "skip_ratio_bucket",
    "skip_ratio_bucket": "skip_ratio_bucket",
}

TAB1_SEGMENT_FIELDS = {
    "price_segment",
    "loyalty_segment",
    "active_segment",
}


def _tab1_dimension_column(dimension: str) -> str:
    normalized = (dimension or "").strip().lower()
    return TAB1_DIMENSION_FIELD_MAP.get(normalized, "age_bucket")


def _tab1_segment_filter_clause(segment_type: Optional[str], segment_value: Optional[str]) -> str:
    if not segment_type and not segment_value:
        return ""
    if not segment_type or not segment_value:
        raise ValueError("segment_type and segment_value must be provided together")
    field = segment_type.strip()
    if field not in TAB1_SEGMENT_FIELDS:
        raise ValueError(f"Invalid segment_type: {segment_type}")
    safe_value = _sql_quote(segment_value.strip())
    return f" AND {field} = '{safe_value}'"


def _km_points_from_rows(rows: List[Tuple[Any, ...]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[int, Dict[str, int]]] = {}
    for row in rows:
        dimension_value = str(row[0] if row[0] is not None else "unknown")
        survival_day = max(_safe_int(row[1]), 0)
        churned = _safe_int(row[2])
        users = max(_safe_int(row[3]), 0)
        dim_bucket = grouped.setdefault(dimension_value, {})
        day_bucket = dim_bucket.setdefault(survival_day, {"events": 0, "censored": 0})
        if churned == 1:
            day_bucket["events"] += users
        else:
            day_bucket["censored"] += users

    curves: List[Dict[str, Any]] = []
    for dimension_value, day_map in sorted(grouped.items(), key=lambda item: item[0]):
        at_risk = sum(points["events"] + points["censored"] for points in day_map.values())
        survival = 1.0
        points = [{"day": 0, "survival_prob": 1.0}]
        for day in sorted(day_map):
            event_count = day_map[day]["events"]
            censored_count = day_map[day]["censored"]
            if at_risk > 0 and event_count > 0:
                survival *= 1.0 - (event_count / at_risk)
            points.append(
                {
                    "day": day,
                    "survival_prob": round(survival, 6),
                    "at_risk": at_risk,
                    "events": event_count,
                }
            )
            at_risk -= event_count + censored_count
        curves.append({"dimension_value": dimension_value, "points": points})
    return curves


def _load_tab1_month_options() -> List[str]:
    sql = """
    SELECT DISTINCT snapshot_month
    FROM realtime_bi.tab1_descriptive_member_monthly
    ORDER BY snapshot_month
    """
    try:
        _, rows = _query_rows(sql)
    except Exception:
        return []

    options: List[str] = []
    for row in rows:
        value = row[0]
        if isinstance(value, date):
            options.append(value.strftime("%Y-%m"))
        else:
            options.append(str(value)[:7])
    return options


def _build_tab1_descriptive(
    month_start: date,
    dimension: str,
    segment_type: Optional[str],
    segment_value: Optional[str],
) -> Dict[str, Any]:
    dim_col = _tab1_dimension_column(dimension)
    filter_clause = _tab1_segment_filter_clause(segment_type, segment_value)
    month_iso = month_start.isoformat()

    base_sql = f"""
    SELECT
      snapshot_month,
      msno,
      churned,
      is_auto_renew,
      survival_days,
      age_bucket,
      gender_bucket,
      txn_freq_bucket,
      skip_ratio_bucket,
      price_segment,
      loyalty_segment,
      active_segment,
      discovery_ratio,
      skip_ratio
    FROM realtime_bi.tab1_descriptive_member_monthly
    WHERE snapshot_month = toDate('{month_iso}')
    {filter_clause}
    """

    count_row = _query_first_row(f"SELECT count() AS row_count FROM ({base_sql})")
    row_count = _safe_int(count_row.get("row_count"))
    if row_count == 0:
        return {
            "meta": {
                "month": month_start.strftime("%Y-%m"),
                "dimension": dim_col,
                "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
            },
            "kpis": {
                "total_expiring_users": 0,
                "historical_churn_rate": 0.0,
                "overall_median_survival": 0.0,
                "auto_renew_rate": 0.0,
            },
            "km_curve": [],
            "segment_mix": [],
            "boredom_scatter": [],
        }

    kpi_sql = f"""
    SELECT
      count() AS total_expiring_users,
      avg(churned) * 100 AS historical_churn_rate,
      quantileExact(0.5)(survival_days) AS overall_median_survival,
      avg(is_auto_renew) * 100 AS auto_renew_rate
    FROM ({base_sql})
    """
    kpi_row = _query_first_row(kpi_sql)

    km_sql = f"""
    SELECT
      {dim_col} AS dimension_value,
      survival_days,
      churned,
      count() AS users
    FROM ({base_sql})
    GROUP BY dimension_value, survival_days, churned
    ORDER BY dimension_value, survival_days
    """
    _, km_rows = _query_rows(km_sql)
    km_curve = _km_points_from_rows(km_rows)

    segment_sql = f"""
    WITH base AS ({base_sql})
    SELECT
      segment_type,
      segment_value,
      count() AS users,
      avg(churned) * 100 AS churn_rate_pct,
      (1 - avg(churned)) * 100 AS retain_rate_pct
    FROM
    (
      SELECT 'price_segment' AS segment_type, price_segment AS segment_value, churned FROM base
      UNION ALL
      SELECT 'loyalty_segment' AS segment_type, loyalty_segment AS segment_value, churned FROM base
      UNION ALL
      SELECT 'active_segment' AS segment_type, active_segment AS segment_value, churned FROM base
    )
    GROUP BY segment_type, segment_value
    ORDER BY segment_type, segment_value
    """
    seg_cols, seg_rows = _query_rows(segment_sql)
    segment_mix = [{seg_cols[idx]: row[idx] for idx in range(len(seg_cols))} for row in seg_rows]

    scatter_sql = f"""
    SELECT
      round(discovery_ratio, 2) AS discovery_ratio,
      round(skip_ratio, 2) AS skip_ratio,
      count() AS users,
      avg(churned) * 100 AS churn_rate_pct
    FROM ({base_sql})
    GROUP BY discovery_ratio, skip_ratio
    ORDER BY users DESC
    LIMIT 800
    """
    scatter_cols, scatter_rows = _query_rows(scatter_sql)
    boredom_scatter = [{scatter_cols[idx]: row[idx] for idx in range(len(scatter_cols))} for row in scatter_rows]

    return {
        "meta": {
            "month": month_start.strftime("%Y-%m"),
            "dimension": dim_col,
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
        },
        "kpis": {
            "total_expiring_users": _safe_int(kpi_row.get("total_expiring_users")),
            "historical_churn_rate": _safe_float(kpi_row.get("historical_churn_rate")),
            "overall_median_survival": _safe_float(kpi_row.get("overall_median_survival")),
            "auto_renew_rate": _safe_float(kpi_row.get("auto_renew_rate")),
        },
        "km_curve": km_curve,
        "segment_mix": segment_mix,
        "boredom_scatter": boredom_scatter,
    }


def _load_model_cohort_rows(
    month_start: date,
    segment_type: Optional[str],
    segment_value: Optional[str],
    sample_limit: int,
) -> List[Dict[str, Any]]:
    month_iso = month_start.isoformat()
    filter_clause = _tab1_segment_filter_clause(segment_type, segment_value)
    sql = f"""
    WITH tab1_latest AS (
      SELECT
        msno,
        argMax(churned, processed_at) AS churned,
        argMax(is_auto_renew, processed_at) AS is_auto_renew,
        argMax(price_segment, processed_at) AS price_segment,
        argMax(loyalty_segment, processed_at) AS loyalty_segment,
        argMax(active_segment, processed_at) AS active_segment,
        argMax(discovery_ratio, processed_at) AS discovery_ratio,
        argMax(skip_ratio, processed_at) AS skip_ratio
      FROM realtime_bi.tab1_descriptive_member_monthly
      WHERE snapshot_month = toDate('{month_iso}')
      GROUP BY msno
    ),
    tx_month AS (
      SELECT
        msno,
        avg(actual_amount_paid) AS avg_paid,
        avg(greatest(payment_plan_days, 1)) AS avg_plan_days,
        count() AS txn_count
      FROM realtime_bi.fact_transactions_rt
      WHERE toStartOfMonth(membership_expire_date) = toDate('{month_iso}')
      GROUP BY msno
    ),
    logs_month AS (
      SELECT
        msno,
        avg(total_secs) AS avg_daily_secs
      FROM realtime_bi.fact_user_logs_rt
      WHERE toStartOfMonth(log_date) = toDate('{month_iso}')
      GROUP BY msno
    )
    SELECT
      t.msno,
      t.churned,
      t.is_auto_renew,
      t.price_segment,
      t.loyalty_segment,
      t.active_segment,
      t.discovery_ratio,
      t.skip_ratio,
      ifNull(tx.avg_paid, 0.0) AS avg_paid,
      ifNull(tx.avg_plan_days, 30.0) AS avg_plan_days,
      ifNull(tx.txn_count, 1.0) AS txn_count,
      ifNull(logs.avg_daily_secs, 0.0) AS avg_daily_secs
    FROM tab1_latest t
    LEFT JOIN tx_month tx USING (msno)
    LEFT JOIN logs_month logs USING (msno)
    WHERE 1 = 1
    {filter_clause}
    ORDER BY cityHash64(t.msno)
    LIMIT {int(sample_limit)}
    """
    cols, rows = _query_rows_safe(sql)
    if not cols or not rows:
        return []
    return [{cols[idx]: row[idx] for idx in range(len(cols))} for row in rows]


def _score_model_row(row: Dict[str, Any], params: PredictiveModelParams) -> Dict[str, Any]:
    churned = 1.0 if _safe_float(row.get("churned")) >= 0.5 else 0.0
    is_auto_renew = 1.0 if _safe_float(row.get("is_auto_renew")) >= 0.5 else 0.0
    avg_daily_secs = max(_safe_float(row.get("avg_daily_secs")), 0.0)
    skip_ratio = _clamp(_safe_float(row.get("skip_ratio")), 0.0, 1.0)
    discovery_ratio = _clamp(_safe_float(row.get("discovery_ratio")), 0.0, 1.0)
    avg_paid = max(_safe_float(row.get("avg_paid")), 0.0)
    avg_plan_days = max(_safe_float(row.get("avg_plan_days"), 30.0), 1.0)
    txn_count = max(_safe_float(row.get("txn_count"), 1.0), 1.0)

    manual = 1.0 - is_auto_renew
    low_activity = 1.0 if avg_daily_secs < 1800 else 0.0
    high_skip = 1.0 if skip_ratio > 0.5 else 0.0
    low_discovery = 1.0 if discovery_ratio < 0.15 else 0.0
    cancel_signal = churned

    churn_probability = _clamp(
        params.base_prob
        + params.weight_manual * manual
        + params.weight_low_activity * low_activity
        + params.weight_high_skip * high_skip
        + params.weight_low_discovery * low_discovery
        + params.weight_cancel_signal * cancel_signal,
        params.prob_min,
        params.prob_max,
    )

    daily_price_est = max(avg_paid / avg_plan_days, 0.0)
    predicted_future_cltv = max(
        avg_paid
        * (params.cltv_base_months + (1.0 - churn_probability) * params.cltv_retention_months)
        * (1.0 + txn_count * params.cltv_txn_gain),
        0.0,
    )
    revenue_at_risk = max(churn_probability * avg_paid * params.risk_horizon_months, 0.0)
    hazard_ratio_proxy = max(
        params.hazard_base
        + params.hazard_churn_weight * churn_probability
        + params.hazard_skip_weight * high_skip
        + params.hazard_low_activity_weight * low_activity,
        0.1,
    )

    payment_mode = "Auto-Renew" if is_auto_renew >= 0.5 else "Manual Renewal"
    if discovery_ratio < 0.15:
        discovery_segment = "Habit Listener"
    elif discovery_ratio < 0.35:
        discovery_segment = "Balanced Listener"
    else:
        discovery_segment = "Explorer Listener"
    price_segment = str(row.get("price_segment") or "unknown")
    strategic_segment = f"{price_segment} / {payment_mode} / {discovery_segment}"

    risk_driver_scores = {
        "Manual Renewal": params.weight_manual * manual,
        "Low Activity": params.weight_low_activity * low_activity,
        "High Skip Ratio": params.weight_high_skip * high_skip,
        "Low Discovery": params.weight_low_discovery * low_discovery,
        "Cancel Signal": params.weight_cancel_signal * cancel_signal,
    }
    primary_risk_driver = max(risk_driver_scores.items(), key=lambda item: item[1])[0]

    return {
        "msno": str(row.get("msno") or ""),
        "price_segment": price_segment,
        "payment_mode": payment_mode,
        "strategic_segment": strategic_segment,
        "primary_risk_driver": primary_risk_driver,
        "avg_daily_secs": avg_daily_secs,
        "skip_ratio": skip_ratio,
        "discovery_ratio": discovery_ratio,
        "avg_paid": avg_paid,
        "daily_price_est": daily_price_est,
        "txn_count": txn_count,
        "churn_probability": churn_probability,
        "predicted_future_cltv": predicted_future_cltv,
        "revenue_at_risk": revenue_at_risk,
        "hazard_ratio_proxy": hazard_ratio_proxy,
        "manual_flag": manual,
        "low_activity_flag": low_activity,
        "high_skip_flag": high_skip,
        "low_discovery_flag": low_discovery,
        "cancel_signal_flag": cancel_signal,
        "deal_flag": 1.0 if price_segment == "deal_<4.5" else 0.0,
    }


def _build_segment_summary(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = row["strategic_segment"]
        bucket = grouped.setdefault(
            key,
            {
                "strategic_segment": key,
                "user_count": 0,
                "sum_churn_prob": 0.0,
                "sum_future_cltv": 0.0,
                "sum_revenue_at_risk": 0.0,
                "risk_drivers": [],
            },
        )
        bucket["user_count"] += 1
        bucket["sum_churn_prob"] += _safe_float(row.get("churn_probability"))
        bucket["sum_future_cltv"] += _safe_float(row.get("predicted_future_cltv"))
        bucket["sum_revenue_at_risk"] += _safe_float(row.get("revenue_at_risk"))
        bucket["risk_drivers"].append(str(row.get("primary_risk_driver") or "Unknown"))

    summary: List[Dict[str, Any]] = []
    for bucket in grouped.values():
        users = max(_safe_int(bucket.get("user_count")), 1)
        avg_churn_prob = bucket["sum_churn_prob"] / users
        avg_future_cltv = bucket["sum_future_cltv"] / users
        revenue_at_risk = bucket["sum_revenue_at_risk"]
        summary.append(
            {
                "strategic_segment": bucket["strategic_segment"],
                "user_count": users,
                "avg_churn_prob": avg_churn_prob,
                "avg_churn_prob_pct": avg_churn_prob * 100.0,
                "avg_future_cltv": avg_future_cltv,
                "total_future_cltv": bucket["sum_future_cltv"],
                "revenue_at_risk": revenue_at_risk,
                "primary_risk_driver": _mode_or_default(bucket["risk_drivers"], default="Unknown"),
            }
        )

    cltv_mid = _median([_safe_float(item.get("avg_future_cltv")) for item in summary])
    churn_mid = 50.0
    for item in summary:
        high_value = _safe_float(item.get("avg_future_cltv")) >= cltv_mid
        high_risk = _safe_float(item.get("avg_churn_prob_pct")) >= churn_mid
        if high_value and high_risk:
            quadrant = "Must Save"
        elif (not high_value) and high_risk:
            quadrant = "Let Go"
        elif high_value and (not high_risk):
            quadrant = "Loyal Core"
        else:
            quadrant = "Stable Low-Tier"
        item["quadrant"] = quadrant

    return sorted(summary, key=lambda item: _safe_float(item.get("revenue_at_risk")), reverse=True)


def _compute_predictive_kpis(rows: List[Dict[str, Any]], summary: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {
            "forecasted_churn_rate": 0.0,
            "predicted_revenue_at_risk": 0.0,
            "predicted_total_future_cltv": 0.0,
            "top_segment": "N/A",
            "top_segment_risk": 0.0,
            "top_segment_user_count": 0,
        }

    total_users = len(rows)
    forecasted_churn_rate = sum(_safe_float(row.get("churn_probability")) for row in rows) * 100.0 / total_users
    predicted_revenue_at_risk = sum(_safe_float(row.get("revenue_at_risk")) for row in rows)
    predicted_total_future_cltv = sum(
        _safe_float(row.get("predicted_future_cltv")) for row in rows if _safe_float(row.get("churn_probability")) <= 0.5
    )

    top = summary[0] if summary else None
    return {
        "forecasted_churn_rate": forecasted_churn_rate,
        "predicted_revenue_at_risk": predicted_revenue_at_risk,
        "predicted_total_future_cltv": predicted_total_future_cltv,
        "top_segment": str(top.get("strategic_segment")) if top else "N/A",
        "top_segment_risk": _safe_float(top.get("revenue_at_risk")) if top else 0.0,
        "top_segment_user_count": _safe_int(top.get("user_count")) if top else 0,
    }


def _build_revenue_leakage(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("primary_risk_driver") or "Unknown")
        bucket = grouped.setdefault(key, {"risk_driver": key, "user_count": 0, "revenue_at_risk": 0.0})
        bucket["user_count"] += 1
        bucket["revenue_at_risk"] += _safe_float(row.get("revenue_at_risk"))
    return sorted(grouped.values(), key=lambda item: _safe_float(item["revenue_at_risk"]), reverse=True)


def _build_forecast_decay(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    focus_groups = {
        "standard_4.5_8": "Standard",
        "deal_<4.5": "Deal Hunter",
        "premium_>=8": "Premium",
    }
    by_price: Dict[str, List[float]] = {key: [] for key in focus_groups.keys()}
    for row in rows:
        price_segment = str(row.get("price_segment") or "")
        if price_segment in by_price:
            by_price[price_segment].append(_safe_float(row.get("churn_probability")))

    decay_rows: List[Dict[str, Any]] = []
    for price_segment, probs in by_price.items():
        if not probs:
            continue
        avg_prob = sum(probs) / len(probs)
        monthly_retention = _clamp(1.0 - avg_prob * 0.85, 0.05, 0.99)
        for month_num in range(1, 13):
            decay_rows.append(
                {
                    "month_num": month_num,
                    "timeline": f"T+{month_num}",
                    "segment": focus_groups[price_segment],
                    "retention_pct": (monthly_retention**month_num) * 100.0,
                }
            )
    return decay_rows


def _simulate_scenario_rows(
    rows: List[Dict[str, Any]],
    params: PredictiveModelParams,
    auto_shift_pct: float,
    upsell_shift_pct: float,
    skip_shift_pct: float,
) -> Dict[str, Any]:
    if not rows:
        return {
            "baseline_probabilities": [],
            "scenario_probabilities": [],
            "baseline_hazards": [],
            "scenario_hazards": [],
            "baseline_revenue": 0.0,
            "saved_revenue": 0.0,
            "incremental_upsell": 0.0,
            "optimized_revenue": 0.0,
            "baseline_avg_hazard": 0.0,
            "scenario_avg_hazard": 0.0,
            "baseline_churn_prob_pct": 0.0,
            "scenario_churn_prob_pct": 0.0,
        }

    auto_shift = _clamp(auto_shift_pct, 0.0, 100.0) / 100.0
    upsell_shift = _clamp(upsell_shift_pct, 0.0, 100.0) / 100.0
    skip_shift = _clamp(skip_shift_pct, 0.0, 100.0) / 100.0

    baseline_probabilities: List[float] = []
    scenario_probabilities: List[float] = []
    baseline_hazards: List[float] = []
    scenario_hazards: List[float] = []
    baseline_revenue = 0.0
    saved_revenue = 0.0
    incremental_upsell = 0.0

    for row in rows:
        baseline_prob = _safe_float(row.get("churn_probability"))
        baseline_hazard = _safe_float(row.get("hazard_ratio_proxy"))
        manual_flag = _safe_float(row.get("manual_flag"))
        low_activity = _safe_float(row.get("low_activity_flag"))
        low_discovery = _safe_float(row.get("low_discovery_flag"))
        cancel_signal = _safe_float(row.get("cancel_signal_flag"))
        skip_ratio = _safe_float(row.get("skip_ratio"))

        manual_after = max(manual_flag * (1.0 - auto_shift), 0.0)
        skip_after = _clamp(skip_ratio * (1.0 - skip_shift), 0.0, 1.0)
        high_skip_after = 1.0 if skip_after > 0.5 else 0.0

        scenario_prob = _clamp(
            params.base_prob
            + params.weight_manual * manual_after
            + params.weight_low_activity * low_activity
            + params.weight_high_skip * high_skip_after
            + params.weight_low_discovery * low_discovery
            + params.weight_cancel_signal * cancel_signal,
            params.prob_min,
            params.prob_max,
        )
        scenario_hazard = max(
            params.hazard_base
            + params.hazard_churn_weight * scenario_prob
            + params.hazard_skip_weight * high_skip_after
            + params.hazard_low_activity_weight * low_activity,
            0.1,
        )

        daily_price_est = _safe_float(row.get("daily_price_est"))
        baseline_revenue += daily_price_est * 30.0
        saved_revenue += max(baseline_prob - scenario_prob, 0.0) * daily_price_est * params.risk_horizon_months * 30.0
        incremental_upsell += upsell_shift * _safe_float(row.get("deal_flag")) * daily_price_est * 30.0 * 0.8

        baseline_probabilities.append(baseline_prob)
        scenario_probabilities.append(scenario_prob)
        baseline_hazards.append(baseline_hazard)
        scenario_hazards.append(scenario_hazard)

    optimized_revenue = baseline_revenue + saved_revenue + incremental_upsell

    return {
        "baseline_probabilities": baseline_probabilities,
        "scenario_probabilities": scenario_probabilities,
        "baseline_hazards": baseline_hazards,
        "scenario_hazards": scenario_hazards,
        "baseline_revenue": baseline_revenue,
        "saved_revenue": saved_revenue,
        "incremental_upsell": incremental_upsell,
        "optimized_revenue": optimized_revenue,
        "baseline_avg_hazard": (sum(baseline_hazards) / len(baseline_hazards)) if baseline_hazards else 0.0,
        "scenario_avg_hazard": (sum(scenario_hazards) / len(scenario_hazards)) if scenario_hazards else 0.0,
        "baseline_churn_prob_pct": (
            (sum(baseline_probabilities) / len(baseline_probabilities)) * 100.0 if baseline_probabilities else 0.0
        ),
        "scenario_churn_prob_pct": (
            (sum(scenario_probabilities) / len(scenario_probabilities)) * 100.0 if scenario_probabilities else 0.0
        ),
    }


def _build_hazard_histogram(
    baseline_hazards: List[float],
    scenario_hazards: List[float],
    bins: int = 24,
) -> List[Dict[str, Any]]:
    if not baseline_hazards and not scenario_hazards:
        return []

    values = baseline_hazards + scenario_hazards
    min_value = min(values)
    max_value = max(values)
    if abs(max_value - min_value) < 1e-9:
        max_value = min_value + 1.0
    step = (max_value - min_value) / bins

    base_counts = [0] * bins
    scenario_counts = [0] * bins
    for value in baseline_hazards:
        idx = min(int((value - min_value) / step), bins - 1)
        base_counts[idx] += 1
    for value in scenario_hazards:
        idx = min(int((value - min_value) / step), bins - 1)
        scenario_counts[idx] += 1

    total_base = max(sum(base_counts), 1)
    total_scenario = max(sum(scenario_counts), 1)
    histogram: List[Dict[str, Any]] = []
    for idx in range(bins):
        left = min_value + idx * step
        right = left + step
        histogram.append(
            {
                "bin_start": left,
                "bin_end": right,
                "baseline_density": base_counts[idx] / total_base,
                "scenario_density": scenario_counts[idx] / total_scenario,
            }
        )
    return histogram


def _build_sensitivity_analysis(rows: List[Dict[str, Any]], params: PredictiveModelParams) -> List[Dict[str, Any]]:
    baseline = _simulate_scenario_rows(rows, params, auto_shift_pct=0.0, upsell_shift_pct=0.0, skip_shift_pct=0.0)
    baseline_revenue = _safe_float(baseline.get("optimized_revenue"))

    auto_scenario = _simulate_scenario_rows(rows, params, auto_shift_pct=1.0, upsell_shift_pct=0.0, skip_shift_pct=0.0)
    upsell_scenario = _simulate_scenario_rows(rows, params, auto_shift_pct=0.0, upsell_shift_pct=1.0, skip_shift_pct=0.0)
    skip_scenario = _simulate_scenario_rows(rows, params, auto_shift_pct=0.0, upsell_shift_pct=0.0, skip_shift_pct=1.0)

    analysis = [
        {
            "strategy": "Auto-Renew Conversion",
            "revenue_impact_per_1pct": _safe_float(auto_scenario.get("optimized_revenue")) - baseline_revenue,
        },
        {
            "strategy": "Upsell to Standard Plan",
            "revenue_impact_per_1pct": _safe_float(upsell_scenario.get("optimized_revenue")) - baseline_revenue,
        },
        {
            "strategy": "Reduce Skip Behavior",
            "revenue_impact_per_1pct": _safe_float(skip_scenario.get("optimized_revenue")) - baseline_revenue,
        },
    ]
    return sorted(analysis, key=lambda item: _safe_float(item["revenue_impact_per_1pct"]), reverse=True)


def _build_tab2_predictive(
    month_start: date,
    segment_type: Optional[str],
    segment_value: Optional[str],
    sample_limit: int,
    params: PredictiveModelParams,
) -> Dict[str, Any]:
    raw_rows = _load_model_cohort_rows(month_start, segment_type, segment_value, sample_limit)
    scored_rows = [_score_model_row(row, params) for row in raw_rows]
    segment_summary = _build_segment_summary(scored_rows)
    kpis = _compute_predictive_kpis(scored_rows, segment_summary)
    leakage = _build_revenue_leakage(scored_rows)
    forecast_decay = _build_forecast_decay(scored_rows)

    previous_month = _shift_month(month_start, -1)
    previous_raw_rows = _load_model_cohort_rows(previous_month, segment_type, segment_value, sample_limit)
    previous_scored_rows = [_score_model_row(row, params) for row in previous_raw_rows]
    previous_summary = _build_segment_summary(previous_scored_rows)
    previous_kpis = _compute_predictive_kpis(previous_scored_rows, previous_summary)
    churn_delta_pp = kpis["forecasted_churn_rate"] - previous_kpis["forecasted_churn_rate"]

    return {
        "meta": {
            "month": month_start.strftime("%Y-%m"),
            "previous_month": previous_month.strftime("%Y-%m"),
            "sample_user_count": len(scored_rows),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
        },
        "model_params": asdict(params),
        "kpis": {**kpis, "forecasted_churn_delta_pp_vs_prev_month": churn_delta_pp},
        "previous_kpis": previous_kpis,
        "value_risk_matrix": segment_summary,
        "revenue_leakage": leakage,
        "forecast_decay": forecast_decay,
        "prescriptions": segment_summary[:200],
    }


def _build_tab3_prescriptive(
    month_start: date,
    segment_type: Optional[str],
    segment_value: Optional[str],
    sample_limit: int,
    params: PredictiveModelParams,
    auto_shift_pct: float,
    upsell_shift_pct: float,
    skip_shift_pct: float,
) -> Dict[str, Any]:
    raw_rows = _load_model_cohort_rows(month_start, segment_type, segment_value, sample_limit)
    scored_rows = [_score_model_row(row, params) for row in raw_rows]
    scenario = _simulate_scenario_rows(scored_rows, params, auto_shift_pct, upsell_shift_pct, skip_shift_pct)
    histogram = _build_hazard_histogram(
        baseline_hazards=scenario["baseline_hazards"],
        scenario_hazards=scenario["scenario_hazards"],
        bins=24,
    )
    sensitivity = _build_sensitivity_analysis(scored_rows, params)

    return {
        "meta": {
            "month": month_start.strftime("%Y-%m"),
            "sample_user_count": len(scored_rows),
            "segment_filter": {"segment_type": segment_type, "segment_value": segment_value},
        },
        "model_params": asdict(params),
        "scenario_inputs": {
            "auto_shift_pct": _clamp(auto_shift_pct, 0.0, 100.0),
            "upsell_shift_pct": _clamp(upsell_shift_pct, 0.0, 100.0),
            "skip_shift_pct": _clamp(skip_shift_pct, 0.0, 100.0),
        },
        "kpis": {
            "baseline_avg_hazard": scenario["baseline_avg_hazard"],
            "scenario_avg_hazard": scenario["scenario_avg_hazard"],
            "baseline_churn_prob_pct": scenario["baseline_churn_prob_pct"],
            "scenario_churn_prob_pct": scenario["scenario_churn_prob_pct"],
            "optimized_projected_revenue": scenario["optimized_revenue"],
            "baseline_revenue": scenario["baseline_revenue"],
            "saved_revenue": scenario["saved_revenue"],
            "incremental_upsell": scenario["incremental_upsell"],
        },
        "hazard_histogram": histogram,
        "financial_waterfall": [
            {"name": "Current Baseline Revenue", "value": scenario["baseline_revenue"]},
            {"name": "Saved Revenue from Retention", "value": scenario["saved_revenue"]},
            {"name": "Incremental Revenue from Upsell", "value": scenario["incremental_upsell"]},
            {"name": "Optimized Projected Revenue", "value": scenario["optimized_revenue"]},
        ],
        "sensitivity_roi": sensitivity,
    }


def _get_clickhouse_client():
    env_host = os.getenv("CLICKHOUSE_HOST", "").strip()
    if env_host.lower() in {"disabled", "clickhouse-disabled", "none", "artifact-only"}:
        raise RuntimeError("ClickHouse is disabled by configuration")
    hosts = [host for host in [env_host, "clickhouse", "localhost"] if host]
    unique_hosts: List[str] = []
    for host in hosts:
        if host not in unique_hosts:
            unique_hosts.append(host)

    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DB", "realtime_bi")

    last_error: Optional[Exception] = None
    for host in unique_hosts:
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:  # pragma: no cover
            last_error = exc
    raise RuntimeError(f"Cannot connect to ClickHouse on hosts={unique_hosts}. Last error={last_error}")


def _query_rows(sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    result = _get_clickhouse_client().query(sql)
    return result.column_names, result.result_rows


def _query_first_row(sql: str) -> Dict[str, Any]:
    cols, rows = _query_rows(sql)
    if not rows:
        return {}
    return {col: rows[0][idx] for idx, col in enumerate(cols)}


def _query_rows_safe(sql: str) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    try:
        return _query_rows(sql)
    except Exception:
        return [], []


def _query_first_row_safe(sql: str) -> Dict[str, Any]:
    try:
        return _query_first_row(sql)
    except Exception:
        return {}


def _clickhouse_disabled() -> bool:
    return os.getenv("CLICKHOUSE_HOST", "").strip().lower() in {"disabled", "clickhouse-disabled", "none", "artifact-only"}


def _month_bounds(year: Optional[int], month: Optional[int]) -> Tuple[date, date]:
    if year is not None and month is not None:
        start = _first_day_of_month(year, month)
        return start, _next_month_start(start)

    latest_row = _query_first_row_safe(
        """
        SELECT toStartOfMonth(max(membership_expire_date)) AS latest_month
        FROM realtime_bi.fact_transactions_rt
        """
    )
    latest_month = latest_row.get("latest_month")
    if latest_month is None:
        today = datetime.now(timezone.utc).date()
        start = _first_day_of_month(today.year, today.month)
        return start, _next_month_start(start)

    latest = latest_month if isinstance(latest_month, date) else datetime.strptime(str(latest_month), "%Y-%m-%d").date()
    start = _first_day_of_month(latest.year, latest.month)
    return start, _next_month_start(start)


def _load_month_options() -> List[str]:
    # Prefer months that actually have realtime KPI rows so default dashboard view is not empty.
    kpi_sql = """
    SELECT month_start
    FROM
    (
      SELECT DISTINCT toStartOfMonth(event_date) AS month_start
      FROM realtime_bi.kpi_revenue
      UNION DISTINCT
      SELECT DISTINCT toStartOfMonth(event_date) AS month_start
      FROM realtime_bi.kpi_churn_risk
      UNION DISTINCT
      SELECT DISTINCT toStartOfMonth(event_date) AS month_start
      FROM realtime_bi.kpi_activity
    )
    ORDER BY month_start
    """

    kpi_rows: List[Tuple[Any, ...]] = []
    try:
        _, kpi_rows = _query_rows(kpi_sql)
    except Exception:
        kpi_rows = []

    if kpi_rows:
        options: List[str] = []
        for row in kpi_rows:
            value = row[0]
            if isinstance(value, date):
                options.append(value.strftime("%Y-%m"))
            else:
                options.append(str(value)[:7])
        return options

    sql = """
    SELECT month_start
    FROM
    (
      SELECT DISTINCT snapshot_month AS month_start
      FROM realtime_bi.tab1_descriptive_member_monthly
      UNION DISTINCT
      SELECT DISTINCT toStartOfMonth(last_expire_date) AS month_start
      FROM
      (
        SELECT
          msno,
          argMax(membership_expire_date, transaction_date) AS last_expire_date
        FROM realtime_bi.fact_transactions_rt
        GROUP BY msno
      )
    )
    ORDER BY month_start
    """
    try:
        _, rows = _query_rows(sql)
    except Exception:
        fallback_sql = """
        WITH last_tx AS (
          SELECT
            msno,
            argMax(membership_expire_date, transaction_date) AS last_expire_date
          FROM realtime_bi.fact_transactions_rt
          GROUP BY msno
        )
        SELECT DISTINCT toStartOfMonth(last_expire_date) AS month_start
        FROM last_tx
        ORDER BY month_start
        """
        _, rows = _query_rows_safe(fallback_sql)

    options: List[str] = []
    for row in rows:
        value = row[0]
        if isinstance(value, date):
            options.append(value.strftime("%Y-%m"))
        else:
            options.append(str(value)[:7])
    return options


REPLAY_DEFAULT_START_DATE = os.getenv("TAB1_REALTIME_START_DATE", os.getenv("REPLAY_START_DATE", "2017-03-01"))
REPLAY_DEFAULT_KAFKA_BOOTSTRAP = os.getenv("REPLAY_KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
REPLAY_DEFAULT_MAX_DAYS = int(os.getenv("REPLAY_MAX_DAYS", "90"))

_REPLAY_STATE_LOCK = threading.Lock()
_REPLAY_THREAD: Optional[threading.Thread] = None
_REPLAY_STATE: Dict[str, Any] = {
    "status": "idle",
    "step": "idle",
    "started_at": None,
    "finished_at": None,
    "duration_sec": None,
    "error": None,
    "progress": 0.0,
    "replay_start_date": REPLAY_DEFAULT_START_DATE,
    "force_reset": True,
    "max_replay_days": REPLAY_DEFAULT_MAX_DAYS,
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_iso_date(value: str) -> str:
    text = (value or "").strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
        return parsed.isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid replay_start_date: {value}. Expected YYYY-MM-DD") from exc


def _snapshot_replay_state() -> Dict[str, Any]:
    with _REPLAY_STATE_LOCK:
        return dict(_REPLAY_STATE)


def _update_replay_state(**kwargs: Any) -> None:
    with _REPLAY_STATE_LOCK:
        _REPLAY_STATE.update(kwargs)


def _replay_reset_realtime_tables(replay_start_date: str) -> None:
    client = _get_clickhouse_client()

    # Realtime simulation only replays user_logs; keep preloaded static tables.
    client.command("TRUNCATE TABLE IF EXISTS realtime_bi.fact_user_logs_rt")
    client.command("TRUNCATE TABLE IF EXISTS realtime_bi.kpi_activity")
    client.command("TRUNCATE TABLE IF EXISTS realtime_bi.kpi_churn_risk")

    exists_rows = client.query("EXISTS TABLE realtime_bi.tab1_descriptive_member_monthly").result_rows
    tab1_exists = bool(exists_rows and exists_rows[0][0] == 1)
    if tab1_exists:
        delete_sql = f"""
        ALTER TABLE realtime_bi.tab1_descriptive_member_monthly
        DELETE
        WHERE source = 'realtime_2017_plus'
          AND snapshot_month >= toDate('{replay_start_date}')
        """
        client.command(delete_sql, settings={"mutations_sync": 2})


def _run_replay_subprocess(step: str, cmd: List[str], env: Dict[str, str], progress: float) -> None:
    _update_replay_state(step=step, progress=progress)
    subprocess.run(cmd, check=True, env=env)


def _wait_for_user_log_ingestion(replay_start_date: str, timeout_sec: int = 120, poll_sec: int = 2) -> int:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        row = _query_first_row_safe(
            f"""
            SELECT count() AS row_count
            FROM realtime_bi.fact_user_logs_rt
            WHERE log_date >= toDate('{replay_start_date}')
            """
        )
        row_count = _safe_int(row.get("row_count"))
        if row_count > 0:
            return row_count
        time.sleep(poll_sec)
    return 0


def _run_replay_worker(replay_start_date: str, force_reset: bool) -> None:
    started_ts = time.time()
    _update_replay_state(
        status="running",
        step="reset_realtime" if force_reset else "replay_user_logs",
        started_at=_utc_now_iso(),
        finished_at=None,
        duration_sec=None,
        error=None,
        progress=0.05,
        replay_start_date=replay_start_date,
        force_reset=force_reset,
        max_replay_days=REPLAY_DEFAULT_MAX_DAYS,
    )

    try:
        if force_reset:
            _replay_reset_realtime_tables(replay_start_date)
            _update_replay_state(step="replay_user_logs", progress=0.2)

        replay_env = dict(os.environ)
        replay_env["KAFKA_BOOTSTRAP_SERVERS"] = REPLAY_DEFAULT_KAFKA_BOOTSTRAP
        replay_env["REPLAY_START_DATE"] = replay_start_date
        replay_env["MAX_REPLAY_DAYS"] = str(REPLAY_DEFAULT_MAX_DAYS)

        _run_replay_subprocess(
            step="replay_user_logs",
            cmd=[sys.executable, "-m", "apps.producers.replay_user_logs"],
            env=replay_env,
            progress=0.78,
        )
        _update_replay_state(step="wait_log_ingestion", progress=0.84)
        ingested_rows = _wait_for_user_log_ingestion(replay_start_date=replay_start_date)
        if ingested_rows <= 0:
            raise RuntimeError(
                "No replayed user logs were ingested into fact_user_logs_rt. "
                "Ensure activity_kpi_job is running, then replay again."
            )
        _run_replay_subprocess(
            step="materialize_tab1_realtime",
            cmd=[sys.executable, "-m", "apps.batch.materialize_tab1_realtime", "--force"],
            env=dict(os.environ),
            progress=0.92,
        )

        _update_replay_state(
            status="succeeded",
            step="completed",
            progress=1.0,
            finished_at=_utc_now_iso(),
            duration_sec=round(time.time() - started_ts, 2),
            error=None,
        )
    except subprocess.CalledProcessError as exc:
        _update_replay_state(
            status="failed",
            step="failed",
            finished_at=_utc_now_iso(),
            duration_sec=round(time.time() - started_ts, 2),
            error=f"Command failed (exit={exc.returncode}): {' '.join(exc.cmd)}",
        )
    except Exception as exc:  # pragma: no cover
        _update_replay_state(
            status="failed",
            step="failed",
            finished_at=_utc_now_iso(),
            duration_sec=round(time.time() - started_ts, 2),
            error=str(exc),
        )


@dataclass
class DashboardSnapshot:
    meta: Dict[str, Any]
    metrics: Dict[str, Any]
    revenue_series: List[Dict[str, Any]]
    risk_series: List[Dict[str, Any]]
    activity_series: List[Dict[str, Any]]


def _build_snapshot(year: Optional[int], month: Optional[int]) -> DashboardSnapshot:
    month_start, next_month = _month_bounds(year, month)
    start_iso = _date_to_iso(month_start)
    next_iso = _date_to_iso(next_month)

    tab1_metric_sql = f"""
    SELECT
      count() AS total_expiring_users,
      avg(churned) * 100 AS historical_churn_rate,
      quantileExact(0.5)(survival_days) AS median_survival_days,
      avg(is_auto_renew) * 100 AS auto_renew_rate
    FROM realtime_bi.tab1_descriptive_member_monthly
    WHERE snapshot_month = toDate('{start_iso}')
    """
    tab1_metric_row = _query_first_row_safe(tab1_metric_sql)

    metric_sql = f"""
    WITH scoped AS (
      SELECT
        msno,
        argMax(membership_expire_date, transaction_date) AS last_expire_date,
        argMax(is_cancel, transaction_date) AS last_is_cancel,
        argMax(is_auto_renew, transaction_date) AS last_is_auto_renew
      FROM realtime_bi.fact_transactions_rt
      WHERE membership_expire_date >= toDate('{start_iso}')
        AND membership_expire_date < toDate('{next_iso}')
      GROUP BY msno
    )
    SELECT
      count() AS total_expiring_users,
      avg(toFloat64(last_is_cancel)) * 100 AS historical_churn_rate,
      avg(toFloat64(last_is_auto_renew)) * 100 AS auto_renew_rate
    FROM scoped
    """
    metric_row = tab1_metric_row if _safe_int(tab1_metric_row.get("total_expiring_users")) > 0 else _query_first_row_safe(metric_sql)

    survival_sql = f"""
    WITH scoped AS (
      SELECT
        msno,
        argMax(membership_expire_date, transaction_date) AS last_expire_date
      FROM realtime_bi.fact_transactions_rt
      WHERE membership_expire_date >= toDate('{start_iso}')
        AND membership_expire_date < toDate('{next_iso}')
      GROUP BY msno
    )
    SELECT
      quantileExactIf(
        0.5
      )(
        dateDiff('day', dm.registration_init_time, s.last_expire_date),
        s.last_expire_date >= toDate('{start_iso}')
        AND s.last_expire_date < toDate('{next_iso}')
      ) AS median_survival_days
    FROM scoped s
    LEFT JOIN realtime_bi.dim_members dm USING (msno)
    """
    survival_row = (
        {"median_survival_days": tab1_metric_row.get("median_survival_days")}
        if _safe_float(tab1_metric_row.get("median_survival_days")) > 0
        else _query_first_row_safe(survival_sql)
    )

    revenue_sql = f"""
    SELECT
      event_date,
      sum(total_revenue) AS total_revenue,
      sum(total_transactions) AS total_transactions
    FROM realtime_bi.kpi_revenue
    WHERE event_date >= toDate('{start_iso}')
      AND event_date < toDate('{next_iso}')
    GROUP BY event_date
    ORDER BY event_date
    """
    rev_cols, rev_rows = _query_rows_safe(revenue_sql)
    revenue_series = [{rev_cols[idx]: row[idx] for idx in range(len(rev_cols))} for row in rev_rows]

    risk_sql = f"""
    SELECT
      event_date,
      sum(high_risk_users) AS high_risk_users,
      avg(avg_risk_score) AS avg_risk_score
    FROM realtime_bi.kpi_churn_risk
    WHERE event_date >= toDate('{start_iso}')
      AND event_date < toDate('{next_iso}')
    GROUP BY event_date
    ORDER BY event_date
    """
    risk_cols, risk_rows = _query_rows_safe(risk_sql)
    risk_series = [{risk_cols[idx]: row[idx] for idx in range(len(risk_cols))} for row in risk_rows]

    activity_sql = f"""
    SELECT
      event_date,
      sum(active_users) AS active_users,
      sum(total_listening_secs) AS total_listening_secs
    FROM realtime_bi.kpi_activity
    WHERE event_date >= toDate('{start_iso}')
      AND event_date < toDate('{next_iso}')
    GROUP BY event_date
    ORDER BY event_date
    """
    activity_cols, activity_rows = _query_rows_safe(activity_sql)
    activity_series = [{activity_cols[idx]: row[idx] for idx in range(len(activity_cols))} for row in activity_rows]

    as_of_row = _query_first_row_safe(
        """
        SELECT greatest(
          ifNull((SELECT max(processed_at) FROM realtime_bi.kpi_revenue), toDateTime('1970-01-01 00:00:00')),
          ifNull((SELECT max(processed_at) FROM realtime_bi.kpi_activity), toDateTime('1970-01-01 00:00:00')),
          ifNull((SELECT max(processed_at) FROM realtime_bi.kpi_churn_risk), toDateTime('1970-01-01 00:00:00'))
        ) AS as_of
        """
    )

    metrics = {
        "total_expiring_users": _safe_int(metric_row.get("total_expiring_users")),
        "historical_churn_rate": _safe_float(metric_row.get("historical_churn_rate")),
        "median_survival_days": _safe_float(survival_row.get("median_survival_days")),
        "auto_renew_rate": _safe_float(metric_row.get("auto_renew_rate")),
    }

    for point in revenue_series:
        if isinstance(point["event_date"], date):
            point["event_date"] = point["event_date"].isoformat()
    for point in risk_series:
        if isinstance(point["event_date"], date):
            point["event_date"] = point["event_date"].isoformat()
    for point in activity_series:
        if isinstance(point["event_date"], date):
            point["event_date"] = point["event_date"].isoformat()

    return DashboardSnapshot(
        meta={
            "month": month_start.strftime("%Y-%m"),
            "month_start": month_start.isoformat(),
            "month_end_exclusive": next_month.isoformat(),
            "as_of": _datetime_to_iso(as_of_row.get("as_of")),
        },
        metrics=metrics,
        revenue_series=revenue_series,
        risk_series=risk_series,
        activity_series=activity_series,
    )


app = FastAPI(title="Realtime BI API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins_from_env(),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/month-options")
def month_options() -> Dict[str, Any]:
    if _clickhouse_disabled():
        artifact_month_sets = [
            set(months)
            for months in [available_tab1_months(), available_tab2_months(), available_tab3_months()]
            if months
        ]
        if not artifact_month_sets:
            months = []
        elif len(artifact_month_sets) == 1:
            months = sorted(artifact_month_sets[0])
        else:
            months = sorted(set.intersection(*artifact_month_sets))
    else:
        base_months = _load_month_options()
        months = sorted(
            set(base_months).union(set(available_tab1_months())).union(set(available_tab2_months()))
        )
    return {"months": months}


@app.get("/api/v1/tab1/month-options")
def tab1_month_options() -> Dict[str, Any]:
    base_months = [] if _clickhouse_disabled() else _load_tab1_month_options()
    months = sorted(set(base_months).union(set(available_tab1_months())))
    return {"months": months}


@app.get("/api/v1/dashboard/snapshot")
def dashboard_snapshot(
    year: Optional[int] = Query(default=None, ge=2000, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
) -> Dict[str, Any]:
    try:
        if _clickhouse_disabled():
            month_start, _ = _month_bounds(year, month)
            return build_dashboard_snapshot_payload(month_start=month_start)
        snapshot = _build_snapshot(year, month)
        if (
            snapshot.metrics.get("total_expiring_users", 0) <= 0
            and not snapshot.revenue_series
            and not snapshot.risk_series
            and not snapshot.activity_series
        ):
            month_start, _ = _month_bounds(year, month)
            return build_dashboard_snapshot_payload(month_start=month_start)
        return asdict(snapshot)
    except Exception:
        try:
            month_start, _ = _month_bounds(year, month)
            return build_dashboard_snapshot_payload(month_start=month_start)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/api/v1/tab1/descriptive")
def tab1_descriptive(
    year: Optional[int] = Query(default=None, ge=2000, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    dimension: str = Query(default="age"),
    segment_type: Optional[str] = Query(default=None),
    segment_value: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    try:
        month_start, _ = _month_bounds(year, month)
        try:
            return build_tab1_descriptive_payload(
                month_start=month_start,
                dimension=dimension,
                segment_type=segment_type,
                segment_value=segment_value,
            )
        except FileNotFoundError as exc:
            if _clickhouse_disabled():
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            try:
                return _build_tab1_descriptive(month_start, dimension, segment_type, segment_value)
            except RuntimeError as runtime_exc:
                if "ClickHouse is disabled by configuration" in str(runtime_exc):
                    raise HTTPException(status_code=404, detail=str(exc)) from runtime_exc
                raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/v1/tab2/predictive")
def tab2_predictive(
    year: Optional[int] = Query(default=None, ge=2000, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    segment_type: Optional[str] = Query(default=None),
    segment_value: Optional[str] = Query(default=None),
    sample_limit: int = Query(default=120000, ge=1000, le=400000),
    base_prob: float = Query(default=0.05, ge=0.0, le=1.0),
    weight_manual: float = Query(default=0.30, ge=0.0, le=2.0),
    weight_low_activity: float = Query(default=0.25, ge=0.0, le=2.0),
    weight_high_skip: float = Query(default=0.15, ge=0.0, le=2.0),
    weight_low_discovery: float = Query(default=0.10, ge=0.0, le=2.0),
    weight_cancel_signal: float = Query(default=0.15, ge=0.0, le=2.0),
    prob_min: float = Query(default=0.01, ge=0.0, le=1.0),
    prob_max: float = Query(default=0.99, ge=0.0, le=1.0),
    cltv_base_months: float = Query(default=6.0, ge=0.0, le=24.0),
    cltv_retention_months: float = Query(default=6.0, ge=0.0, le=24.0),
    cltv_txn_gain: float = Query(default=0.03, ge=0.0, le=1.0),
    risk_horizon_months: float = Query(default=6.0, ge=1.0, le=24.0),
    hazard_base: float = Query(default=1.0, ge=0.0, le=10.0),
    hazard_churn_weight: float = Query(default=1.5, ge=0.0, le=10.0),
    hazard_skip_weight: float = Query(default=0.3, ge=0.0, le=10.0),
    hazard_low_activity_weight: float = Query(default=0.2, ge=0.0, le=10.0),
) -> Dict[str, Any]:
    if prob_min > prob_max:
        raise HTTPException(status_code=400, detail="prob_min must be <= prob_max")

    params = PredictiveModelParams(
        base_prob=base_prob,
        weight_manual=weight_manual,
        weight_low_activity=weight_low_activity,
        weight_high_skip=weight_high_skip,
        weight_low_discovery=weight_low_discovery,
        weight_cancel_signal=weight_cancel_signal,
        prob_min=prob_min,
        prob_max=prob_max,
        cltv_base_months=cltv_base_months,
        cltv_retention_months=cltv_retention_months,
        cltv_txn_gain=cltv_txn_gain,
        risk_horizon_months=risk_horizon_months,
        hazard_base=hazard_base,
        hazard_churn_weight=hazard_churn_weight,
        hazard_skip_weight=hazard_skip_weight,
        hazard_low_activity_weight=hazard_low_activity_weight,
    )

    try:
        month_start, _ = _month_bounds(year, month)
        try:
            return build_tab2_predictive_payload(
                month_start=month_start,
                segment_type=segment_type,
                segment_value=segment_value,
                sample_limit=sample_limit,
            )
        except FileNotFoundError:
            return _build_tab2_predictive(month_start, segment_type, segment_value, sample_limit, params)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/v1/tab3/prescriptive")
def tab3_prescriptive(
    year: Optional[int] = Query(default=None, ge=2000, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
    segment_type: Optional[str] = Query(default=None),
    segment_value: Optional[str] = Query(default=None),
    scenario_id: Optional[str] = Query(default=None),
    sample_limit: int = Query(default=120000, ge=1000, le=400000),
    auto_shift_pct: float = Query(default=20.0, ge=0.0, le=100.0),
    upsell_shift_pct: float = Query(default=15.0, ge=0.0, le=100.0),
    skip_shift_pct: float = Query(default=25.0, ge=0.0, le=100.0),
    base_prob: float = Query(default=0.05, ge=0.0, le=1.0),
    weight_manual: float = Query(default=0.30, ge=0.0, le=2.0),
    weight_low_activity: float = Query(default=0.25, ge=0.0, le=2.0),
    weight_high_skip: float = Query(default=0.15, ge=0.0, le=2.0),
    weight_low_discovery: float = Query(default=0.10, ge=0.0, le=2.0),
    weight_cancel_signal: float = Query(default=0.15, ge=0.0, le=2.0),
    prob_min: float = Query(default=0.01, ge=0.0, le=1.0),
    prob_max: float = Query(default=0.99, ge=0.0, le=1.0),
    cltv_base_months: float = Query(default=6.0, ge=0.0, le=24.0),
    cltv_retention_months: float = Query(default=6.0, ge=0.0, le=24.0),
    cltv_txn_gain: float = Query(default=0.03, ge=0.0, le=1.0),
    risk_horizon_months: float = Query(default=6.0, ge=1.0, le=24.0),
    hazard_base: float = Query(default=1.0, ge=0.0, le=10.0),
    hazard_churn_weight: float = Query(default=1.5, ge=0.0, le=10.0),
    hazard_skip_weight: float = Query(default=0.3, ge=0.0, le=10.0),
    hazard_low_activity_weight: float = Query(default=0.2, ge=0.0, le=10.0),
) -> Dict[str, Any]:
    if prob_min > prob_max:
        raise HTTPException(status_code=400, detail="prob_min must be <= prob_max")

    params = PredictiveModelParams(
        base_prob=base_prob,
        weight_manual=weight_manual,
        weight_low_activity=weight_low_activity,
        weight_high_skip=weight_high_skip,
        weight_low_discovery=weight_low_discovery,
        weight_cancel_signal=weight_cancel_signal,
        prob_min=prob_min,
        prob_max=prob_max,
        cltv_base_months=cltv_base_months,
        cltv_retention_months=cltv_retention_months,
        cltv_txn_gain=cltv_txn_gain,
        risk_horizon_months=risk_horizon_months,
        hazard_base=hazard_base,
        hazard_churn_weight=hazard_churn_weight,
        hazard_skip_weight=hazard_skip_weight,
        hazard_low_activity_weight=hazard_low_activity_weight,
    )

    try:
        month_start, _ = _month_bounds(year, month)
        try:
            return build_tab3_prescriptive_payload(
                month_start=month_start,
                segment_type=segment_type,
                segment_value=segment_value,
                scenario_id=scenario_id,
                sample_limit=sample_limit,
                auto_shift_pct=auto_shift_pct,
                upsell_shift_pct=upsell_shift_pct,
                skip_shift_pct=skip_shift_pct,
            )
        except FileNotFoundError:
            return _build_tab3_prescriptive(
                month_start=month_start,
                segment_type=segment_type,
                segment_value=segment_value,
                sample_limit=sample_limit,
                params=params,
                auto_shift_pct=auto_shift_pct,
                upsell_shift_pct=upsell_shift_pct,
                skip_shift_pct=skip_shift_pct,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/v1/replay/status")
def replay_status() -> Dict[str, Any]:
    return _snapshot_replay_state()


@app.post("/api/v1/replay/start")
def replay_start(
    force_reset: bool = Query(default=True),
    replay_start_date: str = Query(default=REPLAY_DEFAULT_START_DATE),
) -> Dict[str, Any]:
    global _REPLAY_THREAD
    try:
        validated_start_date = _validate_iso_date(replay_start_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    with _REPLAY_STATE_LOCK:
        if _REPLAY_STATE.get("status") in {"queued", "running"}:
            raise HTTPException(status_code=409, detail="Replay is already running")
        _REPLAY_STATE.update(
            {
                "status": "queued",
                "step": "queued",
                "started_at": _utc_now_iso(),
                "finished_at": None,
                "duration_sec": None,
                "error": None,
                "progress": 0.0,
                "replay_start_date": validated_start_date,
                "force_reset": force_reset,
            }
        )

    _REPLAY_THREAD = threading.Thread(
        target=_run_replay_worker,
        args=(validated_start_date, force_reset),
        daemon=True,
        name="replay-user-logs-thread",
    )
    _REPLAY_THREAD.start()
    return _snapshot_replay_state()


@app.websocket("/ws/kpi")
async def ws_kpi(
    websocket: WebSocket,
    year: Optional[int] = Query(default=None, ge=2000, le=2100),
    month: Optional[int] = Query(default=None, ge=1, le=12),
) -> None:
    await websocket.accept()
    push_interval = float(os.getenv("WS_PUSH_INTERVAL_SEC", "3"))
    last_signature: Optional[Tuple[str, int]] = None

    try:
        while True:
            snapshot = await asyncio.to_thread(_build_snapshot, year, month)
            payload = asdict(snapshot)
            signature = (
                payload["meta"]["as_of"],
                payload["metrics"]["total_expiring_users"],
            )
            if signature != last_signature:
                await websocket.send_json(payload)
                last_signature = signature
            await asyncio.sleep(push_interval)
    except WebSocketDisconnect:
        return
    except Exception as exc:  # pragma: no cover
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close()
