#!/usr/bin/env python3
from __future__ import annotations

import os
from datetime import date
from typing import Dict, Iterable, List, Optional, cast

import clickhouse_connect
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def _shift_month(month_start: date, delta_months: int) -> date:
    month_index = month_start.year * 12 + (month_start.month - 1) + delta_months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _fmt_number(value: float) -> str:
    return f"{value:,.0f}"


def _fmt_percent(value: float) -> str:
    return f"{value:.2f}%"


def _safe_mode(series: pd.Series, default: str = "Unknown") -> str:
    mode = series.mode(dropna=True)
    if mode.empty:
        return default
    return str(mode.iloc[0])


@st.cache_resource
def get_clickhouse_client():
    env_host = os.getenv("CLICKHOUSE_HOST", "").strip()
    candidates = [host for host in [env_host, "clickhouse", "localhost"] if host]

    seen = set()
    hosts = []
    for host in candidates:
        if host not in seen:
            hosts.append(host)
            seen.add(host)

    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DB", "realtime_bi")

    last_error = None
    for host in hosts:
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

    raise RuntimeError(f"Cannot connect to ClickHouse on hosts={hosts}. Last error: {last_error}")


@st.cache_data(ttl=30)
def query_df(sql: str) -> pd.DataFrame:
    result = get_clickhouse_client().query(sql)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


@st.cache_data(ttl=120)
def get_available_last_expire_months() -> List[date]:
    sql = """
    WITH last_tx AS (
      SELECT
        msno,
        argMax(membership_expire_date, transaction_date) AS last_expire_date
      FROM fact_transactions_rt
      GROUP BY msno
    )
    SELECT DISTINCT toStartOfMonth(last_expire_date) AS expire_month
    FROM last_tx
    ORDER BY expire_month
    """
    df = query_df(sql)
    if df.empty:
        return []
    return [pd.to_datetime(value).date() for value in df["expire_month"].tolist()]


def month_slicer_sidebar(available_months: List[date]) -> Dict[str, object]:
    st.sidebar.header("Global Slicer")
    st.sidebar.caption("Time context by last_expire_date (month/year)")

    years = sorted({month.year for month in available_months})
    default_year = years[-1]
    selected_year = st.sidebar.selectbox("Year", years, index=len(years) - 1)

    months_of_year = [month for month in available_months if month.year == selected_year]
    selected_month = st.sidebar.selectbox(
        "Month",
        months_of_year,
        index=len(months_of_year) - 1,
        format_func=lambda x: x.strftime("%m/%Y"),
    )

    with st.sidebar.expander("Performance", expanded=False):
        sample_limit = st.slider("Max users in dashboard cohort", min_value=20000, max_value=200000, value=80000, step=10000)

    st.sidebar.info(f"Selected: {selected_month.strftime('%m/%Y')}")
    return {"month_start": selected_month, "sample_limit": int(sample_limit)}


@st.cache_data(ttl=120)
def load_user_cohort(month_start: date, sample_limit: int) -> pd.DataFrame:
    month_literal = month_start.isoformat()
    sql = f"""
    WITH last_tx AS (
      SELECT
        msno,
        argMax(membership_expire_date, transaction_date) AS last_expire_date,
        max(transaction_date) AS last_transaction_date,
        argMax(is_cancel, transaction_date) AS last_is_cancel,
        argMax(is_auto_renew, transaction_date) AS last_is_auto_renew,
        avg(actual_amount_paid) AS avg_paid,
        sum(actual_amount_paid) AS total_paid,
        avg(payment_plan_days) AS avg_plan_days,
        count() AS txn_count,
        avg(toFloat64(is_auto_renew)) AS auto_renew_ratio,
        avg(toFloat64(is_cancel)) AS cancel_ratio
      FROM fact_transactions_rt
      GROUP BY msno
    ),
    activity AS (
      SELECT
        msno,
        avg(total_secs) AS avg_daily_secs,
        avg(
          (num_25 + num_50 + num_75 + num_985)
          / greatest(num_25 + num_50 + num_75 + num_985 + num_100, 1)
        ) AS skip_ratio,
        avg(
          num_unq
          / greatest(num_25 + num_50 + num_75 + num_985 + num_100, 1)
        ) AS discovery_ratio
      FROM fact_user_logs_rt
      GROUP BY msno
    )
    SELECT
      tx.msno AS msno,
      tx.last_expire_date,
      tx.last_transaction_date,
      tx.last_is_cancel,
      tx.last_is_auto_renew,
      tx.avg_paid,
      tx.total_paid,
      tx.avg_plan_days,
      tx.txn_count,
      tx.auto_renew_ratio,
      tx.cancel_ratio,
      ifNull(a.avg_daily_secs, 0.0) AS avg_daily_secs,
      ifNull(a.skip_ratio, 0.0) AS skip_ratio,
      ifNull(a.discovery_ratio, 0.0) AS discovery_ratio,
      dm.bd AS age,
      ifNull(dm.gender, 'unknown') AS gender,
      dm.registration_init_time,
      dateDiff('day', dm.registration_init_time, tx.last_expire_date) AS survival_days
    FROM last_tx tx
    LEFT JOIN activity a USING (msno)
    LEFT JOIN dim_members dm USING (msno)
    WHERE toStartOfMonth(tx.last_expire_date) = toDate('{month_literal}')
    ORDER BY tx.last_expire_date DESC, tx.msno
    LIMIT {int(sample_limit)}
    """
    return query_df(sql)


def _label_from_bins(series: pd.Series, bins: Iterable[float], labels: Iterable[str]) -> pd.Series:
    return pd.cut(series, bins=bins, labels=labels, include_lowest=True).astype("object")


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    numeric_cols = [
        "last_is_cancel",
        "last_is_auto_renew",
        "avg_paid",
        "total_paid",
        "avg_plan_days",
        "txn_count",
        "auto_renew_ratio",
        "cancel_ratio",
        "avg_daily_secs",
        "skip_ratio",
        "discovery_ratio",
        "age",
        "survival_days",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["last_expire_date"] = pd.to_datetime(out["last_expire_date"], errors="coerce")
    out["registration_init_time"] = pd.to_datetime(out["registration_init_time"], errors="coerce")

    out["age"] = out["age"].where(out["age"].between(15, 80))
    out["survival_days"] = out["survival_days"].fillna(0).clip(lower=1)
    out["skip_ratio"] = out["skip_ratio"].fillna(0).clip(lower=0, upper=1)
    out["discovery_ratio"] = out["discovery_ratio"].fillna(0).clip(lower=0, upper=1)
    out["avg_daily_secs"] = out["avg_daily_secs"].fillna(0).clip(lower=0)
    out["avg_paid"] = out["avg_paid"].fillna(0).clip(lower=0)
    out["total_paid"] = out["total_paid"].fillna(0).clip(lower=0)
    out["avg_plan_days"] = out["avg_plan_days"].fillna(30).clip(lower=1)
    out["txn_count"] = out["txn_count"].fillna(1).clip(lower=1)

    out["daily_price_est"] = (out["avg_paid"] / out["avg_plan_days"]).clip(lower=0)
    out["churn_flag"] = out["last_is_cancel"].fillna(0).clip(lower=0, upper=1)
    out["auto_renew_flag"] = out["last_is_auto_renew"].fillna(0).clip(lower=0, upper=1)

    age_segment = _label_from_bins(
        out["age"],
        bins=[0, 20, 30, 45, 65, 100],
        labels=["Age 15-20", "Age 21-30", "Age 31-45", "Age 46-65", "Age 65+"],
    )
    out["age_segment"] = age_segment.fillna("Age Unknown")

    gender_norm = out["gender"].fillna("unknown").astype(str).str.strip().str.lower()
    out["gender_profile"] = gender_norm.map({"male": "Male", "female": "Female"}).fillna("Unknown")

    txn_freq_segment = _label_from_bins(
        out["txn_count"],
        bins=[0, 2, 5, 1000000],
        labels=["Low Frequency", "Medium Frequency", "High Frequency"],
    )
    out["txn_freq_segment"] = txn_freq_segment.fillna("Low Frequency")

    skip_segment = _label_from_bins(
        out["skip_ratio"],
        bins=[0, 0.2, 0.5, 1.0],
        labels=["Low Skip", "Medium Skip", "High Skip (>50%)"],
    )
    out["skip_segment"] = skip_segment.fillna("Low Skip")
    out["boredom_segment"] = out["skip_segment"]

    discovery_segment = _label_from_bins(
        out["discovery_ratio"],
        bins=[0, 0.15, 0.35, 1.0],
        labels=["Habit Listener", "Balanced Listener", "Explorer Listener"],
    )
    out["discovery_segment"] = discovery_segment.fillna("Habit Listener")

    price_segment = _label_from_bins(
        out["daily_price_est"],
        bins=[-1, 2, 4.5, 8, 1000000],
        labels=[
            "Free Trial (<2/day)",
            "Deal Hunter (2-4.5/day)",
            "Standard (4.5-8/day)",
            "Premium (>8/day)",
        ],
    )
    out["price_segment"] = price_segment.fillna("Standard (4.5-8/day)")

    loyalty_segment = _label_from_bins(
        out["txn_count"],
        bins=[0, 2, 5, 1000000],
        labels=["New", "Growing", "Loyal"],
    )
    out["loyalty_segment"] = loyalty_segment.fillna("New")

    active_segment = _label_from_bins(
        out["avg_daily_secs"],
        bins=[-1, 1800, 7200, 1000000000],
        labels=["Low Activity", "Medium Activity", "High Activity"],
    )
    out["active_segment"] = active_segment.fillna("Low Activity")

    out["payment_mode"] = "Manual Renewal"
    out.loc[out["auto_renew_flag"] >= 0.5, "payment_mode"] = "Auto-Renew"
    out["churn_status"] = "Retain"
    out.loc[out["churn_flag"] >= 0.5, "churn_status"] = "Churn"

    low_activity = (out["avg_daily_secs"] < 1800).astype(float)
    high_skip = (out["skip_ratio"] > 0.5).astype(float)
    low_discovery = (out["discovery_ratio"] < 0.15).astype(float)
    manual = (1 - out["auto_renew_flag"]).astype(float)
    cancel_signal = out["churn_flag"].astype(float)

    out["churn_probability"] = (
        0.05
        + 0.30 * manual
        + 0.25 * low_activity
        + 0.15 * high_skip
        + 0.10 * low_discovery
        + 0.15 * cancel_signal
    ).clip(lower=0.01, upper=0.99)

    out["predicted_future_cltv"] = (
        out["avg_paid"] * (6 + (1 - out["churn_probability"]) * 6) * (1 + out["txn_count"] * 0.03)
    ).clip(lower=0)
    out["revenue_at_risk"] = (out["churn_probability"] * out["avg_paid"] * 6).clip(lower=0)
    out["hazard_ratio_proxy"] = (1 + 1.5 * out["churn_probability"] + 0.3 * high_skip + 0.2 * low_activity).clip(lower=0.1)

    risk_driver_scores = pd.DataFrame(
        {
            "Manual Renewal": 0.30 * manual,
            "Low Activity": 0.25 * low_activity,
            "High Skip Ratio": 0.20 * high_skip,
            "Low Discovery": 0.10 * low_discovery,
            "Cancel Signal": 0.15 * cancel_signal,
        }
    )
    out["primary_risk_driver"] = risk_driver_scores.idxmax(axis=1)

    out["strategic_segment"] = (
        out["price_segment"].astype(str)
        + " / "
        + out["payment_mode"].astype(str)
        + " / "
        + out["discovery_segment"].astype(str)
    )

    return out


def compute_descriptive_kpis(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {
            "total_expiring_users": 0,
            "historical_churn_rate": 0.0,
            "median_survival_days": 0.0,
            "auto_renew_rate": 0.0,
        }
    return {
        "total_expiring_users": float(df["msno"].nunique()),
        "historical_churn_rate": float(df["churn_flag"].mean() * 100),
        "median_survival_days": float(df["survival_days"].median()),
        "auto_renew_rate": float(df["auto_renew_flag"].mean() * 100),
    }


def build_km_curve(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows = []
    group_sizes = df[group_col].value_counts().head(8)

    for group_name in group_sizes.index:
        gdf = df[df[group_col] == group_name].copy()
        gdf["survival_days"] = gdf["survival_days"].round().astype(int).clip(lower=1)
        life = (
            gdf.groupby("survival_days", as_index=False)
            .agg(events=("churn_flag", "sum"), total=("msno", "count"))
            .sort_values("survival_days")
        )

        at_risk = float(len(gdf))
        survival_prob = 1.0
        rows.append({"group": group_name, "tenure_days": 0, "survival_prob": 100.0})

        for _, record in life.iterrows():
            if at_risk <= 0:
                break
            events = float(record["events"])
            if events > 0:
                survival_prob *= max(0.0, 1.0 - events / at_risk)
            rows.append(
                {
                    "group": group_name,
                    "tenure_days": int(record["survival_days"]),
                    "survival_prob": survival_prob * 100.0,
                }
            )
            at_risk -= float(record["total"])

    curve = pd.DataFrame(rows)
    if curve.empty:
        return curve

    filtered = curve[(curve["tenure_days"] % 15 == 0) | (curve["tenure_days"] <= 30) | (curve["tenure_days"] == 0)]
    return filtered.sort_values(["group", "tenure_days"])


def build_segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    summary = (
        df.groupby("strategic_segment", as_index=False)
        .agg(
            user_count=("msno", "count"),
            avg_churn_prob=("churn_probability", "mean"),
            avg_future_cltv=("predicted_future_cltv", "mean"),
            total_future_cltv=("predicted_future_cltv", "sum"),
            revenue_at_risk=("revenue_at_risk", "sum"),
            primary_risk_driver=("primary_risk_driver", _safe_mode),
        )
        .sort_values("revenue_at_risk", ascending=False)
    )
    return summary


def compute_predictive_kpis(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {
            "forecasted_churn_rate": 0.0,
            "predicted_revenue_at_risk": 0.0,
            "predicted_total_future_cltv": 0.0,
            "top_segment": "N/A",
            "top_segment_risk": 0.0,
        }

    segment_summary = build_segment_summary(df)
    top_segment = segment_summary.iloc[0]["strategic_segment"] if not segment_summary.empty else "N/A"
    top_segment_risk = float(segment_summary.iloc[0]["revenue_at_risk"]) if not segment_summary.empty else 0.0

    return {
        "forecasted_churn_rate": float(df["churn_probability"].mean() * 100),
        "predicted_revenue_at_risk": float(df["revenue_at_risk"].sum()),
        "predicted_total_future_cltv": float(df[df["churn_probability"] <= 0.5]["predicted_future_cltv"].sum()),
        "top_segment": str(top_segment),
        "top_segment_risk": float(top_segment_risk),
    }


def render_descriptive_tab(base_df: pd.DataFrame) -> None:
    st.subheader("Tab 1 - Descriptive Analysis")
    st.caption("Question: What is happening with customer lifecycle right now?")

    dimension_map = {
        "Age": "age_segment",
        "Gender/Profile": "gender_profile",
        "Transaction Frequency": "txn_freq_segment",
        "Boredom (Skip Ratio)": "boredom_segment",
    }
    segment_axis_map = {
        "price_segment": "Price Segment",
        "loyalty_segment": "Loyalty Segment",
        "active_segment": "Active Segment",
    }

    c1, c2, c3 = st.columns(3)
    selected_dimension_label = c1.selectbox("Survival split dimension", list(dimension_map.keys()))
    selected_segment_axis = c2.selectbox(
        "Segment axis (100% stacked)",
        list(segment_axis_map.keys()),
        format_func=lambda x: segment_axis_map[x],
    )

    segment_values = sorted(base_df[selected_segment_axis].dropna().astype(str).unique().tolist())
    selected_focus_segment = c3.selectbox("Cross-filter focus segment", ["All"] + segment_values)

    df = base_df.copy()
    if selected_focus_segment != "All":
        df = df[df[selected_segment_axis].astype(str) == selected_focus_segment]
        st.info(f"Cross-filter applied: {selected_segment_axis} = {selected_focus_segment}")

    kpi = compute_descriptive_kpis(df)
    kcol1, kcol2, kcol3, kcol4 = st.columns(4)
    kcol1.metric("Total Expiring Users", _fmt_number(kpi["total_expiring_users"]))
    kcol2.metric("Historical Churn Rate", _fmt_percent(kpi["historical_churn_rate"]))
    kcol3.metric("Overall Median Survival", f"{kpi['median_survival_days']:.0f} days")
    kcol4.metric("Auto-Renew Rate", _fmt_percent(kpi["auto_renew_rate"]))

    selected_dimension_col = dimension_map[selected_dimension_label]
    km_df = build_km_curve(df, selected_dimension_col)
    if km_df.empty:
        st.warning("Not enough data to draw survival curve.")
    else:
        fig_km = px.line(
            km_df,
            x="tenure_days",
            y="survival_prob",
            color="group",
            title="Dynamic Kaplan-Meier style Survival Curve",
        )
        fig_km.update_layout(xaxis_title="Tenure (days)", yaxis_title="Survival Probability (%)")
        st.plotly_chart(fig_km, use_container_width=True)

    stacked = (
        df.groupby([selected_segment_axis, "churn_status"], as_index=False)
        .agg(user_count=("msno", "count"))
        .sort_values(selected_segment_axis)
    )
    if not stacked.empty:
        stacked["ratio"] = stacked["user_count"] / stacked.groupby(selected_segment_axis)["user_count"].transform("sum")
        fig_stack = px.bar(
            stacked,
            x="ratio",
            y=selected_segment_axis,
            color="churn_status",
            orientation="h",
            title="Customer Basket Split (100% Stacked)",
            color_discrete_map={"Churn": "#B22222", "Retain": "#2E8B57"},
        )
        fig_stack.update_layout(barmode="stack", xaxis_title="Ratio", yaxis_title="Segment")
        fig_stack.update_xaxes(tickformat=".0%")
        st.plotly_chart(fig_stack, use_container_width=True)

    scatter_df = df.copy()
    if len(scatter_df) > 5000:
        scatter_df = scatter_df.sample(5000, random_state=42)

    if not scatter_df.empty:
        fig_scatter = px.scatter(
            scatter_df,
            x="discovery_ratio",
            y="skip_ratio",
            size="revenue_at_risk",
            color="churn_probability",
            hover_data=["strategic_segment", "price_segment", "payment_mode"],
            color_continuous_scale="Reds",
            title='Behavior Matrix: "Content Boredom" Interaction',
            size_max=40,
        )
        fig_scatter.add_vline(x=0.15, line_dash="dot")
        fig_scatter.add_hline(y=0.5, line_dash="dot")
        fig_scatter.update_layout(
            xaxis_title="Discovery Ratio",
            yaxis_title="Skip Ratio",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)


def render_predictive_tab(current_df: pd.DataFrame, previous_df: Optional[pd.DataFrame]) -> None:
    st.subheader("Tab 2 - Predictive Analysis")
    st.caption("Forecast risk, value, and strategic segment priorities.")

    current_kpi = compute_predictive_kpis(current_df)
    previous_kpi = compute_predictive_kpis(previous_df) if previous_df is not None and not previous_df.empty else None

    churn_delta = None
    if previous_kpi is not None and previous_kpi["forecasted_churn_rate"] > 0:
        churn_delta = f"{current_kpi['forecasted_churn_rate'] - previous_kpi['forecasted_churn_rate']:+.2f} pp"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Forecasted 30-Day Churn Rate", _fmt_percent(current_kpi["forecasted_churn_rate"]), delta=churn_delta)
    m2.metric("Predicted Revenue at Risk", _fmt_number(current_kpi["predicted_revenue_at_risk"]))
    m3.metric("Total Predicted Future CLTV", _fmt_number(current_kpi["predicted_total_future_cltv"]))
    m4.metric("Top Flight-Risk Segment", current_kpi["top_segment"])
    st.caption(f"Top segment risk value: {_fmt_number(current_kpi['top_segment_risk'])}")

    segment_summary = build_segment_summary(current_df)
    if segment_summary.empty:
        st.warning("No segment summary available for predictive tab.")
        return

    seg = segment_summary.copy()
    seg["avg_churn_prob_pct"] = seg["avg_churn_prob"] * 100
    x_mid = float(seg["avg_future_cltv"].median())
    y_mid = 50.0

    def _quadrant(row: pd.Series) -> str:
        high_value = row["avg_future_cltv"] >= x_mid
        high_risk = row["avg_churn_prob_pct"] >= y_mid
        if high_value and high_risk:
            return "Must Save"
        if (not high_value) and high_risk:
            return "Let Go"
        if high_value and (not high_risk):
            return "Loyal Core"
        return "Stable Low-Tier"

    seg["quadrant"] = seg.apply(_quadrant, axis=1)

    fig_quad = px.scatter(
        seg,
        x="avg_future_cltv",
        y="avg_churn_prob_pct",
        size="user_count",
        color="quadrant",
        hover_data=["strategic_segment", "revenue_at_risk", "primary_risk_driver"],
        title="Value vs Risk Scatter Quadrant",
        size_max=60,
    )
    fig_quad.add_vline(x=x_mid, line_dash="dot")
    fig_quad.add_hline(y=y_mid, line_dash="dot")
    fig_quad.update_layout(xaxis_title="Predicted Future CLTV", yaxis_title="Churn Probability (%)")
    st.plotly_chart(fig_quad, use_container_width=True)

    treemap_df = seg.copy()
    fig_tree = px.treemap(
        treemap_df,
        path=["primary_risk_driver", "strategic_segment"],
        values="revenue_at_risk",
        color="revenue_at_risk",
        color_continuous_scale="OrRd",
        title="Predicted Revenue Leakage Treemap",
    )
    st.plotly_chart(fig_tree, use_container_width=True)

    decay_rows = []
    focus_groups = [
        "Standard (4.5-8/day)",
        "Deal Hunter (2-4.5/day)",
        "Free Trial (<2/day)",
    ]
    for group_name in focus_groups:
        gdf = current_df[current_df["price_segment"] == group_name]
        if gdf.empty:
            continue
        avg_prob = float(gdf["churn_probability"].mean())
        monthly_retention = min(0.99, max(0.05, 1 - avg_prob * 0.85))
        for month in range(1, 13):
            decay_rows.append(
                {
                    "month_num": month,
                    "timeline": f"T+{month}",
                    "segment": group_name,
                    "retention_pct": (monthly_retention**month) * 100,
                }
            )

    decay_df = pd.DataFrame(decay_rows)
    if not decay_df.empty:
        fig_decay = px.line(
            decay_df,
            x="month_num",
            y="retention_pct",
            color="segment",
            markers=True,
            title="Forecasted Survival Decay (3-6-12 months)",
        )
        fig_decay.update_layout(xaxis_title="Future Month", yaxis_title="Projected Retained Users (%)")
        st.plotly_chart(fig_decay, use_container_width=True)

    insights = (
        seg[["strategic_segment", "user_count", "avg_churn_prob_pct", "revenue_at_risk", "primary_risk_driver"]]
        .rename(
            columns={
                "strategic_segment": "Segment Name",
                "user_count": "User Count",
                "avg_churn_prob_pct": "Average Churn Prob (%)",
                "revenue_at_risk": "Revenue at Risk",
                "primary_risk_driver": "Primary Risk Driver",
            }
        )
        .sort_values("Revenue at Risk", ascending=False)
    )
    st.markdown("**Actionable Insights (Strategic Prescriptions)**")
    st.dataframe(insights, use_container_width=True, hide_index=True)
    st.download_button(
        "Export Strategic Prescriptions (CSV)",
        data=insights.to_csv(index=False).encode("utf-8"),
        file_name="strategic_prescriptions.csv",
        mime="text/csv",
    )


def simulate_scenario(df: pd.DataFrame, auto_shift: int, upsell_shift: int, skip_shift: int) -> Dict[str, object]:
    cohort = df.copy()
    manual_mask = (cohort["auto_renew_flag"] < 0.5).astype(float)
    deal_mask = cohort["price_segment"].isin(["Free Trial (<2/day)", "Deal Hunter (2-4.5/day)"]).astype(float)
    skip_mask = (cohort["skip_ratio"] > 0.5).astype(float)
    low_activity = (cohort["avg_daily_secs"] < 1800).astype(float)
    low_discovery = (cohort["discovery_ratio"] < 0.15).astype(float)
    cancel_signal = cohort["churn_flag"].astype(float)

    manual_after = (manual_mask * (1 - auto_shift / 100.0)).clip(lower=0)
    skip_after = (cohort["skip_ratio"] * (1 - skip_shift / 100.0)).clip(lower=0, upper=1)
    high_skip_after = (skip_after > 0.5).astype(float)

    scenario_prob = (
        0.05
        + 0.30 * manual_after
        + 0.25 * low_activity
        + 0.15 * high_skip_after
        + 0.10 * low_discovery
        + 0.15 * cancel_signal
    ).clip(lower=0.01, upper=0.99)

    baseline_prob = cohort["churn_probability"]

    baseline_hazard = cohort["hazard_ratio_proxy"]
    scenario_hazard = (1 + 1.5 * scenario_prob + 0.3 * high_skip_after + 0.2 * low_activity).clip(lower=0.1)

    baseline_revenue = float((cohort["daily_price_est"] * 30).sum())
    saved_revenue = float(((baseline_prob - scenario_prob).clip(lower=0) * cohort["daily_price_est"] * 180).sum())
    incremental_upsell = float((upsell_shift / 100.0) * (deal_mask * cohort["daily_price_est"] * 30 * 0.8).sum())
    optimized_revenue = baseline_revenue + saved_revenue + incremental_upsell

    return {
        "baseline_prob": baseline_prob,
        "scenario_prob": scenario_prob,
        "baseline_hazard": baseline_hazard,
        "scenario_hazard": scenario_hazard,
        "baseline_revenue": baseline_revenue,
        "saved_revenue": saved_revenue,
        "incremental_upsell": incremental_upsell,
        "optimized_revenue": optimized_revenue,
    }


def render_prescriptive_tab(df: pd.DataFrame) -> None:
    st.subheader("Tab 3 - Prescriptive Simulation")
    st.caption("Adjust sliders to evaluate risk and financial impact under a simulated scenario.")

    c1, c2, c3 = st.columns(3)
    auto_shift = c1.slider("Shift Manual -> Auto-Renew (%)", min_value=0, max_value=100, value=20, step=1)
    upsell_shift = c2.slider("Shift Deal/Trial -> Standard (%)", min_value=0, max_value=100, value=15, step=1)
    skip_shift = c3.slider("Reduce High-Skip users (%)", min_value=0, max_value=100, value=25, step=1)

    scenario = simulate_scenario(df, auto_shift, upsell_shift, skip_shift)

    baseline_prob = scenario["baseline_prob"]
    scenario_prob = scenario["scenario_prob"]
    baseline_hazard = scenario["baseline_hazard"]
    scenario_hazard = scenario["scenario_hazard"]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Baseline Avg Hazard", f"{float(baseline_hazard.mean()):.3f}")
    k2.metric(
        "Scenario Avg Hazard",
        f"{float(scenario_hazard.mean()):.3f}",
        delta=f"{float(scenario_hazard.mean() - baseline_hazard.mean()):+.3f}",
    )
    k3.metric(
        "Scenario Churn Probability",
        _fmt_percent(float(scenario_prob.mean() * 100)),
        delta=f"{float((scenario_prob.mean() - baseline_prob.mean()) * 100):+.2f} pp",
    )
    k4.metric("Optimized Projected Revenue", _fmt_number(float(scenario["optimized_revenue"])))

    hz_df = pd.DataFrame(
        {
            "hazard_ratio": pd.concat([baseline_hazard, scenario_hazard], axis=0).astype(float),
            "distribution": ["Baseline"] * len(baseline_hazard) + ["Scenario"] * len(scenario_hazard),
        }
    )
    if len(hz_df) > 30000:
        hz_df = hz_df.sample(30000, random_state=42)

    fig_density = px.histogram(
        hz_df,
        x="hazard_ratio",
        color="distribution",
        histnorm="probability density",
        barmode="overlay",
        opacity=0.55,
        nbins=50,
        title="Population Hazard Shift",
        color_discrete_map={"Baseline": "#9CA3AF", "Scenario": "#1F77B4"},
    )
    fig_density.update_layout(xaxis_title="Hazard Ratio", yaxis_title="Density")
    st.plotly_chart(fig_density, use_container_width=True)

    fig_waterfall = go.Figure(
        go.Waterfall(
            orientation="v",
            measure=["absolute", "relative", "relative", "total"],
            x=[
                "Current Baseline Revenue",
                "Saved Revenue from Retention",
                "Incremental Revenue from Upsell",
                "Optimized Projected Revenue",
            ],
            y=[
                float(scenario["baseline_revenue"]),
                float(scenario["saved_revenue"]),
                float(scenario["incremental_upsell"]),
                0,
            ],
            connector={"line": {"color": "rgb(90, 90, 90)"}},
        )
    )
    fig_waterfall.update_layout(title="Financial Impact Analysis (Waterfall)", showlegend=False)
    st.plotly_chart(fig_waterfall, use_container_width=True)

    base = simulate_scenario(df, 0, 0, 0)["optimized_revenue"]
    auto_roi = simulate_scenario(df, 1, 0, 0)["optimized_revenue"] - base
    upsell_roi = simulate_scenario(df, 0, 1, 0)["optimized_revenue"] - base
    skip_roi = simulate_scenario(df, 0, 0, 1)["optimized_revenue"] - base

    sensitivity_df = pd.DataFrame(
        {
            "strategy": [
                "Auto-Renew Conversion",
                "Upsell to Standard Plan",
                "Reduce Skip Behavior",
            ],
            "revenue_impact_per_1pct": [auto_roi, upsell_roi, skip_roi],
        }
    ).sort_values("revenue_impact_per_1pct", ascending=True)

    fig_tornado = px.bar(
        sensitivity_df,
        x="revenue_impact_per_1pct",
        y="strategy",
        orientation="h",
        title="Sensitivity Analysis - ROI per Strategy (per +1% shift)",
        color="revenue_impact_per_1pct",
        color_continuous_scale="Blues",
    )
    fig_tornado.update_layout(xaxis_title="Revenue Impact", yaxis_title="Strategy")
    st.plotly_chart(fig_tornado, use_container_width=True)


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&display=swap');
        html, body, [class*="css"]  {
            font-family: "Space Grotesk", sans-serif;
        }
        .stApp {
            background: linear-gradient(180deg, #f7f9fb 0%, #eef3f8 100%);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title="KKBox Realtime BI (Streamlit)", layout="wide")
    apply_theme()
    st.title("KKBox Realtime BI Dashboard")
    st.caption("Global slicer + 3 functional tabs: descriptive, predictive, prescriptive simulation.")

    try:
        available_months = get_available_last_expire_months()
    except Exception as exc:
        st.error(f"Cannot load month options from ClickHouse: {exc}")
        st.stop()

    if not available_months:
        st.warning("No transaction data found. Run pipeline first.")
        st.stop()

    slicer = month_slicer_sidebar(available_months)
    selected_month = cast(date, slicer["month_start"])
    sample_limit = cast(int, slicer["sample_limit"])

    try:
        raw_current = load_user_cohort(selected_month, sample_limit)
    except Exception as exc:
        st.error(f"Cannot load cohort data: {exc}")
        st.stop()

    current_df = prepare_features(raw_current)
    if current_df.empty:
        st.warning("No users in selected period.")
        st.stop()
    if "msno" not in current_df.columns:
        st.error(f"Unexpected cohort schema: missing `msno`. Columns={list(current_df.columns)}")
        st.stop()

    previous_month = _shift_month(selected_month, -1)
    previous_df = pd.DataFrame()
    if previous_month in available_months:
        try:
            previous_df = prepare_features(load_user_cohort(previous_month, sample_limit))
        except Exception:
            previous_df = pd.DataFrame()

    st.sidebar.metric("Users in current cohort", _fmt_number(float(current_df["msno"].nunique())))

    tab1, tab2, tab3 = st.tabs(
        [
            "Tab 1 - Descriptive Analysis",
            "Tab 2 - Predictive Analysis",
            "Tab 3 - Prescriptive Simulation",
        ]
    )

    with tab1:
        render_descriptive_tab(current_df)
    with tab2:
        render_predictive_tab(current_df, previous_df if not previous_df.empty else None)
    with tab3:
        render_prescriptive_tab(current_df)


if __name__ == "__main__":
    main()
