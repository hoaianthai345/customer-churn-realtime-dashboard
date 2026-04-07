"use client";

import { useEffect, useMemo, useRef, useState } from "react";

type MetricPayload = {
  total_expiring_users: number;
  historical_churn_rate: number;
  median_survival_days: number;
  auto_renew_rate: number;
};

type SeriesPoint = {
  event_date: string;
  [key: string]: string | number;
};

type SnapshotPayload = {
  meta: {
    month: string;
    month_start: string;
    month_end_exclusive: string;
    as_of: string;
  };
  metrics: MetricPayload;
  revenue_series: SeriesPoint[];
  risk_series: SeriesPoint[];
  activity_series: SeriesPoint[];
};

type ReplayStatus = {
  status: "idle" | "queued" | "running" | "succeeded" | "failed";
  step: string;
  started_at: string | null;
  finished_at: string | null;
  duration_sec: number | null;
  error: string | null;
  progress: number;
  replay_start_date: string;
  force_reset: boolean;
};

type Tab1Dimension = "age" | "gender" | "txn_freq" | "skip_ratio";
type SegmentType = "price_segment" | "loyalty_segment" | "active_segment";

type Tab1Kpis = {
  total_expiring_users: number;
  historical_churn_rate: number;
  overall_median_survival: number;
  auto_renew_rate: number;
};

type KmPoint = {
  day: number;
  survival_prob: number;
  at_risk?: number;
  events?: number;
};

type KmSeries = {
  dimension_value: string;
  points: KmPoint[];
};

type SegmentMixRow = {
  segment_type: SegmentType;
  segment_value: string;
  users: number;
  churn_rate_pct: number;
  retain_rate_pct: number;
};

type BoredomPoint = {
  discovery_ratio: number;
  skip_ratio: number;
  users: number;
  churn_rate_pct: number;
};

type Tab1Payload = {
  meta: {
    month: string;
    dimension: string;
    segment_filter: {
      segment_type: SegmentType | null;
      segment_value: string | null;
    };
  };
  kpis: Tab1Kpis;
  km_curve: KmSeries[];
  segment_mix: SegmentMixRow[];
  boredom_scatter: BoredomPoint[];
};

type SegmentFilterState = {
  segmentType: SegmentType | null;
  segmentValue: string | null;
};

type ModelParamState = {
  base_prob: number;
  weight_manual: number;
  weight_low_activity: number;
  weight_high_skip: number;
  weight_low_discovery: number;
  weight_cancel_signal: number;
  prob_min: number;
  prob_max: number;
  cltv_base_months: number;
  cltv_retention_months: number;
  cltv_txn_gain: number;
  risk_horizon_months: number;
  hazard_base: number;
  hazard_churn_weight: number;
  hazard_skip_weight: number;
  hazard_low_activity_weight: number;
};

type PredictiveKpis = {
  forecasted_churn_rate: number;
  predicted_revenue_at_risk: number;
  predicted_total_future_cltv: number;
  top_segment: string;
  top_segment_risk: number;
  top_segment_user_count: number;
  forecasted_churn_delta_pp_vs_prev_month: number;
};

type PredictiveMatrixRow = {
  strategic_segment: string;
  user_count: number;
  avg_churn_prob: number;
  avg_churn_prob_pct: number;
  avg_future_cltv: number;
  total_future_cltv: number;
  revenue_at_risk: number;
  primary_risk_driver: string;
  quadrant: string;
};

type RevenueLeakageRow = {
  risk_driver: string;
  user_count: number;
  revenue_at_risk: number;
};

type ForecastDecayPoint = {
  month_num: number;
  timeline: string;
  segment: string;
  retention_pct: number;
};

type PredictivePayload = {
  meta: {
    month: string;
    previous_month: string;
    sample_user_count: number;
    segment_filter: {
      segment_type: SegmentType | null;
      segment_value: string | null;
    };
  };
  model_params: ModelParamState;
  kpis: PredictiveKpis;
  previous_kpis: PredictiveKpis;
  value_risk_matrix: PredictiveMatrixRow[];
  revenue_leakage: RevenueLeakageRow[];
  forecast_decay: ForecastDecayPoint[];
  prescriptions: PredictiveMatrixRow[];
};

type ScenarioInputs = {
  auto_shift_pct: number;
  upsell_shift_pct: number;
  skip_shift_pct: number;
};

type PrescriptiveKpis = {
  baseline_avg_hazard: number;
  scenario_avg_hazard: number;
  baseline_churn_prob_pct: number;
  scenario_churn_prob_pct: number;
  optimized_projected_revenue: number;
  baseline_revenue: number;
  saved_revenue: number;
  incremental_upsell: number;
};

type HazardHistogramPoint = {
  bin_start: number;
  bin_end: number;
  baseline_density: number;
  scenario_density: number;
};

type WaterfallPoint = {
  name: string;
  value: number;
};

type SensitivityPoint = {
  strategy: string;
  revenue_impact_per_1pct: number;
};

type PrescriptivePayload = {
  meta: {
    month: string;
    sample_user_count: number;
    segment_filter: {
      segment_type: SegmentType | null;
      segment_value: string | null;
    };
  };
  model_params: ModelParamState;
  scenario_inputs: ScenarioInputs;
  kpis: PrescriptiveKpis;
  hazard_histogram: HazardHistogramPoint[];
  financial_waterfall: WaterfallPoint[];
  sensitivity_roi: SensitivityPoint[];
};

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";
const WS_BASE = process.env.NEXT_PUBLIC_WS_BASE_URL ?? "ws://localhost:8000";
const DEFAULT_REPLAY_START_DATE = process.env.NEXT_PUBLIC_REPLAY_START_DATE ?? "2017-03-01";
const KM_COLORS = ["#22d3ee", "#4ade80", "#f59e0b", "#f43f5e", "#a78bfa", "#06b6d4"];
const DIMENSION_LABELS: Record<Tab1Dimension, string> = {
  age: "Age",
  gender: "Gender/Profile",
  txn_freq: "Transaction Frequency",
  skip_ratio: "Skip Ratio",
};
const SEGMENT_LABELS: Record<SegmentType, string> = {
  price_segment: "Price Segment",
  loyalty_segment: "Loyalty Segment",
  active_segment: "Active Segment",
};
const DEFAULT_MODEL_PARAMS: ModelParamState = {
  base_prob: 0.05,
  weight_manual: 0.3,
  weight_low_activity: 0.25,
  weight_high_skip: 0.15,
  weight_low_discovery: 0.1,
  weight_cancel_signal: 0.15,
  prob_min: 0.01,
  prob_max: 0.99,
  cltv_base_months: 6,
  cltv_retention_months: 6,
  cltv_txn_gain: 0.03,
  risk_horizon_months: 6,
  hazard_base: 1,
  hazard_churn_weight: 1.5,
  hazard_skip_weight: 0.3,
  hazard_low_activity_weight: 0.2,
};
const DEFAULT_SCENARIO_INPUTS: ScenarioInputs = {
  auto_shift_pct: 20,
  upsell_shift_pct: 15,
  skip_shift_pct: 25,
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat("en-US").format(Math.round(value));
}

function formatPct(value: number): string {
  return `${value.toFixed(2)}%`;
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "TWD",
    maximumFractionDigits: 0,
  }).format(value);
}

function normalizeMonths(...monthSets: string[][]): string[] {
  const set = new Set<string>();
  for (const months of monthSets) {
    for (const month of months) {
      if (/^\d{4}-\d{2}$/.test(month)) {
        set.add(month);
      }
    }
  }
  return Array.from(set).sort((a, b) => b.localeCompare(a));
}

function toYearMonth(monthText: string): { year: number; month: number } {
  const [year, month] = monthText.split("-").map((value) => Number(value));
  return { year, month };
}

function hasTab1Data(payload: Tab1Payload | null): boolean {
  if (!payload) return false;
  return (
    payload.kpis.total_expiring_users > 0 ||
    payload.km_curve.length > 0 ||
    payload.segment_mix.length > 0 ||
    payload.boredom_scatter.length > 0
  );
}

function hasTab2Data(payload: PredictivePayload | null): boolean {
  if (!payload) return false;
  return (
    payload.meta.sample_user_count > 0 ||
    payload.value_risk_matrix.length > 0 ||
    payload.revenue_leakage.length > 0 ||
    payload.forecast_decay.length > 0
  );
}

function hasTab3Data(payload: PrescriptivePayload | null): boolean {
  if (!payload) return false;
  return payload.meta.sample_user_count > 0 || payload.hazard_histogram.length > 0;
}

function appendSegmentFilter(params: URLSearchParams, filter: SegmentFilterState): void {
  if (filter.segmentType && filter.segmentValue) {
    params.set("segment_type", filter.segmentType);
    params.set("segment_value", filter.segmentValue);
  }
}

function appendModelParams(params: URLSearchParams, modelParams: ModelParamState): void {
  for (const [key, value] of Object.entries(modelParams)) {
    params.set(key, String(value));
  }
}

export default function HomePage() {
  const [activeTab, setActiveTab] = useState<"tab1" | "tab2" | "tab3">("tab1");
  const [monthOptions, setMonthOptions] = useState<string[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string>("");
  const [snapshot, setSnapshot] = useState<SnapshotPayload | null>(null);
  const [wsStatus, setWsStatus] = useState<"connecting" | "open" | "closed">("closed");
  const [replayStatus, setReplayStatus] = useState<ReplayStatus | null>(null);

  const [tab1Dimension, setTab1Dimension] = useState<Tab1Dimension>("age");
  const [tab1Data, setTab1Data] = useState<Tab1Payload | null>(null);
  const [tab1Loading, setTab1Loading] = useState<boolean>(false);

  const [segmentFilter, setSegmentFilter] = useState<SegmentFilterState>({ segmentType: null, segmentValue: null });

  const [modelParams, setModelParams] = useState<ModelParamState>(DEFAULT_MODEL_PARAMS);
  const [tab2Data, setTab2Data] = useState<PredictivePayload | null>(null);
  const [tab2Loading, setTab2Loading] = useState<boolean>(false);

  const [scenarioInputs, setScenarioInputs] = useState<ScenarioInputs>(DEFAULT_SCENARIO_INPUTS);
  const [tab3Data, setTab3Data] = useState<PrescriptivePayload | null>(null);
  const [tab3Loading, setTab3Loading] = useState<boolean>(false);

  const wsRef = useRef<WebSocket | null>(null);
  const lastReplayStatusRef = useRef<string>("idle");

  const hasMonth = selectedMonth.length === 7;
  const yearMonth = useMemo(() => (hasMonth ? toYearMonth(selectedMonth) : null), [hasMonth, selectedMonth]);
  const tab1HasData = hasTab1Data(tab1Data);
  const tab2HasData = hasTab2Data(tab2Data);
  const tab3HasData = hasTab3Data(tab3Data);

  const tab1SegmentGroups = useMemo(() => {
    const grouped: Record<SegmentType, SegmentMixRow[]> = {
      price_segment: [],
      loyalty_segment: [],
      active_segment: [],
    };
    for (const row of tab1Data?.segment_mix ?? []) {
      grouped[row.segment_type].push(row);
    }
    for (const key of Object.keys(grouped) as SegmentType[]) {
      grouped[key] = grouped[key].sort((a, b) => Number(b.users) - Number(a.users));
    }
    return grouped;
  }, [tab1Data]);

  const tab2DecaySummary = useMemo(() => {
    const summary: Record<string, Record<number, number>> = {};
    for (const row of tab2Data?.forecast_decay ?? []) {
      if (!summary[row.segment]) {
        summary[row.segment] = {};
      }
      summary[row.segment][row.month_num] = row.retention_pct;
    }
    return summary;
  }, [tab2Data]);

  const tab3MaxDensity = useMemo(() => {
    return Math.max(
      0.0001,
      ...(tab3Data?.hazard_histogram ?? []).flatMap((point) => [Number(point.baseline_density), Number(point.scenario_density)]),
    );
  }, [tab3Data]);

  const tab3MaxRoi = useMemo(() => {
    return Math.max(1, ...(tab3Data?.sensitivity_roi ?? []).map((row) => Math.abs(Number(row.revenue_impact_per_1pct) || 0)));
  }, [tab3Data]);

  const loadMonthOptions = () => {
    Promise.all([
      fetch(`${API_BASE}/api/v1/month-options`).then((res) => res.json()).catch(() => ({ months: [] as string[] })),
      fetch(`${API_BASE}/api/v1/tab1/month-options`).then((res) => res.json()).catch(() => ({ months: [] as string[] })),
    ])
      .then(([kpiData, tab1MonthData]) => {
        const kpiMonths: string[] = Array.isArray(kpiData.months) ? kpiData.months : [];
        const tab1Months: string[] = Array.isArray(tab1MonthData.months) ? tab1MonthData.months : [];
        const months = normalizeMonths(kpiMonths, tab1Months);
        setMonthOptions(months);
        if (!selectedMonth && months.length > 0) {
          setSelectedMonth(months[0]);
        } else if (selectedMonth && !months.includes(selectedMonth) && months.length > 0) {
          setSelectedMonth(months[0]);
        }
      })
      .catch(() => {
        setMonthOptions([]);
      });
  };

  const loadReplayStatus = () => {
    fetch(`${API_BASE}/api/v1/replay/status`)
      .then((res) => res.json())
      .then((payload: ReplayStatus) => {
        const previousStatus = lastReplayStatusRef.current;
        lastReplayStatusRef.current = payload.status;
        setReplayStatus(payload);
        if (previousStatus !== "succeeded" && payload.status === "succeeded") {
          loadMonthOptions();
        }
      })
      .catch(() => undefined);
  };

  useEffect(() => {
    loadMonthOptions();
    loadReplayStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const timer = setInterval(() => {
      loadReplayStatus();
    }, 4000);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!yearMonth) return;
    const url = `${API_BASE}/api/v1/dashboard/snapshot?year=${yearMonth.year}&month=${yearMonth.month}`;
    fetch(url)
      .then((res) => res.json())
      .then((payload: SnapshotPayload) => setSnapshot(payload))
      .catch(() => undefined);
  }, [yearMonth]);

  useEffect(() => {
    if (!yearMonth) return;

    const params = new URLSearchParams({
      year: String(yearMonth.year),
      month: String(yearMonth.month),
      dimension: tab1Dimension,
    });
    appendSegmentFilter(params, segmentFilter);

    setTab1Loading(true);
    fetch(`${API_BASE}/api/v1/tab1/descriptive?${params.toString()}`)
      .then(async (res) => {
        const payload = await res.json();
        if (!res.ok) throw new Error(String(payload?.detail ?? "tab1 load failed"));
        return payload as Tab1Payload;
      })
      .then((payload) => setTab1Data(payload))
      .catch(() => {
        setTab1Data(null);
      })
      .finally(() => setTab1Loading(false));
  }, [yearMonth, tab1Dimension, segmentFilter.segmentType, segmentFilter.segmentValue]);

  useEffect(() => {
    if (!yearMonth) return;

    const params = new URLSearchParams({
      year: String(yearMonth.year),
      month: String(yearMonth.month),
      sample_limit: "120000",
    });
    appendSegmentFilter(params, segmentFilter);
    appendModelParams(params, modelParams);

    setTab2Loading(true);
    fetch(`${API_BASE}/api/v1/tab2/predictive?${params.toString()}`)
      .then(async (res) => {
        const payload = await res.json();
        if (!res.ok) throw new Error(String(payload?.detail ?? "tab2 load failed"));
        return payload as PredictivePayload;
      })
      .then((payload) => setTab2Data(payload))
      .catch(() => {
        setTab2Data(null);
      })
      .finally(() => setTab2Loading(false));
  }, [
    yearMonth,
    segmentFilter.segmentType,
    segmentFilter.segmentValue,
    modelParams.base_prob,
    modelParams.weight_manual,
    modelParams.weight_low_activity,
    modelParams.weight_high_skip,
    modelParams.weight_low_discovery,
    modelParams.weight_cancel_signal,
    modelParams.prob_min,
    modelParams.prob_max,
    modelParams.cltv_base_months,
    modelParams.cltv_retention_months,
    modelParams.cltv_txn_gain,
    modelParams.risk_horizon_months,
    modelParams.hazard_base,
    modelParams.hazard_churn_weight,
    modelParams.hazard_skip_weight,
    modelParams.hazard_low_activity_weight,
  ]);

  useEffect(() => {
    if (!yearMonth) return;

    const params = new URLSearchParams({
      year: String(yearMonth.year),
      month: String(yearMonth.month),
      sample_limit: "120000",
      auto_shift_pct: String(scenarioInputs.auto_shift_pct),
      upsell_shift_pct: String(scenarioInputs.upsell_shift_pct),
      skip_shift_pct: String(scenarioInputs.skip_shift_pct),
    });
    appendSegmentFilter(params, segmentFilter);
    appendModelParams(params, modelParams);

    setTab3Loading(true);
    fetch(`${API_BASE}/api/v1/tab3/prescriptive?${params.toString()}`)
      .then(async (res) => {
        const payload = await res.json();
        if (!res.ok) throw new Error(String(payload?.detail ?? "tab3 load failed"));
        return payload as PrescriptivePayload;
      })
      .then((payload) => setTab3Data(payload))
      .catch(() => {
        setTab3Data(null);
      })
      .finally(() => setTab3Loading(false));
  }, [
    yearMonth,
    segmentFilter.segmentType,
    segmentFilter.segmentValue,
    scenarioInputs.auto_shift_pct,
    scenarioInputs.upsell_shift_pct,
    scenarioInputs.skip_shift_pct,
    modelParams.base_prob,
    modelParams.weight_manual,
    modelParams.weight_low_activity,
    modelParams.weight_high_skip,
    modelParams.weight_low_discovery,
    modelParams.weight_cancel_signal,
    modelParams.prob_min,
    modelParams.prob_max,
    modelParams.cltv_base_months,
    modelParams.cltv_retention_months,
    modelParams.cltv_txn_gain,
    modelParams.risk_horizon_months,
    modelParams.hazard_base,
    modelParams.hazard_churn_weight,
    modelParams.hazard_skip_weight,
    modelParams.hazard_low_activity_weight,
  ]);

  useEffect(() => {
    if (!yearMonth) return;

    if (wsRef.current) {
      wsRef.current.close();
    }

    const wsUrl = `${WS_BASE}/ws/kpi?year=${yearMonth.year}&month=${yearMonth.month}`;
    setWsStatus("connecting");
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setWsStatus("open");
    ws.onclose = () => setWsStatus("closed");
    ws.onerror = () => setWsStatus("closed");
    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as SnapshotPayload;
        if (payload?.metrics) {
          setSnapshot(payload);
        }
      } catch (_) {
        // Ignore malformed payloads
      }
    };

    return () => {
      ws.close();
    };
  }, [yearMonth]);

  const replayBusy = replayStatus?.status === "queued" || replayStatus?.status === "running";

  const toggleSegmentFilter = (segmentType: SegmentType, segmentValue: string) => {
    setSegmentFilter((prev) => {
      if (prev.segmentType === segmentType && prev.segmentValue === segmentValue) {
        return { segmentType: null, segmentValue: null };
      }
      return { segmentType, segmentValue };
    });
  };

  const updateModelParam = (key: keyof ModelParamState, value: number) => {
    setModelParams((prev) => ({ ...prev, [key]: value }));
  };

  const updateScenarioInput = (key: keyof ScenarioInputs, value: number) => {
    setScenarioInputs((prev) => ({ ...prev, [key]: value }));
  };

  const triggerReplay = async () => {
    try {
      const params = new URLSearchParams({
        force_reset: "true",
        replay_start_date: DEFAULT_REPLAY_START_DATE,
      });
      const response = await fetch(`${API_BASE}/api/v1/replay/start?${params.toString()}`, { method: "POST" });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload?.detail ?? "Replay start failed");
      }
      setReplayStatus(payload as ReplayStatus);
    } catch (error) {
      const errorText = error instanceof Error ? error.message : "Replay start failed";
      setReplayStatus((prev) => ({
        status: "failed",
        step: prev?.step ?? "start_failed",
        started_at: prev?.started_at ?? null,
        finished_at: new Date().toISOString(),
        duration_sec: prev?.duration_sec ?? null,
        error: errorText,
        progress: prev?.progress ?? 0,
        replay_start_date: prev?.replay_start_date ?? DEFAULT_REPLAY_START_DATE,
        force_reset: true,
      }));
    }
  };

  const kmSeries = tab1Data?.km_curve ?? [];
  const kmMaxDay = Math.max(1, ...kmSeries.flatMap((series) => series.points.map((point) => Number(point.day) || 0)));
  const kmViewWidth = 680;
  const kmViewHeight = 280;
  const kmPadding = { left: 48, right: 20, top: 20, bottom: 34 };
  const kmPlotWidth = kmViewWidth - kmPadding.left - kmPadding.right;
  const kmPlotHeight = kmViewHeight - kmPadding.top - kmPadding.bottom;
  const kmX = (day: number) => kmPadding.left + (clamp(day, 0, kmMaxDay) / kmMaxDay) * kmPlotWidth;
  const kmY = (survival: number) => kmPadding.top + (1 - clamp(survival, 0, 1)) * kmPlotHeight;

  const scatterPoints = tab1Data?.boredom_scatter ?? [];
  const scatterMaxUsers = Math.max(1, ...scatterPoints.map((point) => Number(point.users) || 0));
  const scatterViewWidth = 680;
  const scatterViewHeight = 280;
  const scatterPadding = { left: 48, right: 24, top: 16, bottom: 34 };
  const scatterPlotWidth = scatterViewWidth - scatterPadding.left - scatterPadding.right;
  const scatterPlotHeight = scatterViewHeight - scatterPadding.top - scatterPadding.bottom;
  const scatterX = (ratio: number) => scatterPadding.left + clamp(ratio, 0, 1) * scatterPlotWidth;
  const scatterY = (ratio: number) => scatterPadding.top + (1 - clamp(ratio, 0, 1)) * scatterPlotHeight;
  const bubbleColor = (churnRate: number) => {
    const level = clamp((Number(churnRate) || 0) / 100, 0, 1);
    const hue = 120 - level * 120;
    return `hsl(${hue} 75% 48%)`;
  };

  return (
    <main className="page">
      <section className="hero">
        <div>
          <p className="eyebrow">Realtime BI Dashboard</p>
          <h1>KKBox Churn Intelligence</h1>
          <p className="subtitle">
            Global slicer + 3 tabs: Descriptive, Predictive, Prescriptive simulation (with model parameters).
          </p>
        </div>
        <div className="controls">
          <label htmlFor="month-select">Global Slicer - last_expire_date (month/year)</label>
          <select
            id="month-select"
            value={selectedMonth}
            onChange={(e) => setSelectedMonth(e.target.value)}
            disabled={monthOptions.length === 0}
          >
            {monthOptions.map((month) => (
              <option value={month} key={month}>
                {month}
              </option>
            ))}
          </select>
          <button className="action-btn" onClick={triggerReplay} disabled={replayBusy}>
            {replayBusy ? "Replay is running..." : `Replay user logs from ${DEFAULT_REPLAY_START_DATE}`}
          </button>
          <p className={`ws-badge ws-${wsStatus}`}>WS: {wsStatus}</p>
          <p className="replay-info">
            Replay: {replayStatus?.status ?? "idle"} | Step: {replayStatus?.step ?? "idle"} | Progress:{" "}
            {Math.round((replayStatus?.progress ?? 0) * 100)}%
          </p>
          {replayStatus?.error ? <p className="replay-error">{replayStatus.error}</p> : null}
        </div>
      </section>

      <section className="tab-switch tab-switch-3">
        <button
          type="button"
          className={`tab-btn ${activeTab === "tab1" ? "is-active" : ""}`}
          onClick={() => setActiveTab("tab1")}
        >
          Tab 1: Descriptive Analysis
        </button>
        <button
          type="button"
          className={`tab-btn ${activeTab === "tab2" ? "is-active" : ""}`}
          onClick={() => setActiveTab("tab2")}
        >
          Tab 2: Predictive Analysis
        </button>
        <button
          type="button"
          className={`tab-btn ${activeTab === "tab3" ? "is-active" : ""}`}
          onClick={() => setActiveTab("tab3")}
        >
          Tab 3: Prescriptive Simulation
        </button>
      </section>

      {activeTab === "tab1" ? (
        <>
          <section className="tab1-toolbar panel">
            <div className="tab1-toolbar-item">
              <label htmlFor="tab1-dimension">Kaplan-Meier Dimension</label>
              <select
                id="tab1-dimension"
                value={tab1Dimension}
                onChange={(e) => setTab1Dimension(e.target.value as Tab1Dimension)}
              >
                {(Object.keys(DIMENSION_LABELS) as Tab1Dimension[]).map((dimension) => (
                  <option value={dimension} key={dimension}>
                    {DIMENSION_LABELS[dimension]}
                  </option>
                ))}
              </select>
            </div>
            <div className="tab1-toolbar-item">
              <p className="filter-label">Cross-filter from Segment Mix</p>
              {segmentFilter.segmentType && segmentFilter.segmentValue ? (
                <div className="filter-chip-wrap">
                  <span className="filter-chip">
                    {SEGMENT_LABELS[segmentFilter.segmentType]}: <strong>{segmentFilter.segmentValue}</strong>
                  </span>
                  <button
                    type="button"
                    className="chip-clear-btn"
                    onClick={() => setSegmentFilter({ segmentType: null, segmentValue: null })}
                  >
                    Clear
                  </button>
                </div>
              ) : (
                <p className="filter-empty">No filter applied.</p>
              )}
            </div>
          </section>

          <section className="kpi-grid">
            <article className="kpi-card">
              <h3>Total Expiring Users</h3>
              <p>{tab1Data ? formatNumber(tab1Data.kpis.total_expiring_users) : "-"}</p>
            </article>
            <article className="kpi-card">
              <h3>Historical Churn Rate</h3>
              <p>{tab1Data ? formatPct(tab1Data.kpis.historical_churn_rate) : "-"}</p>
            </article>
            <article className="kpi-card">
              <h3>Overall Median Survival</h3>
              <p>{tab1Data ? `${formatNumber(tab1Data.kpis.overall_median_survival)} days` : "-"}</p>
            </article>
            <article className="kpi-card">
              <h3>Auto-Renew Rate</h3>
              <p>{tab1Data ? formatPct(tab1Data.kpis.auto_renew_rate) : "-"}</p>
            </article>
          </section>

          {tab1Loading ? (
            <section className="panel">
              <h2>Loading Tab 1...</h2>
            </section>
          ) : null}

          {!tab1Loading && !tab1HasData && hasMonth ? (
            <section className="panel">
              <h2>No Tab 1 Data In Selected Month</h2>
              <p>
                Khong co du lieu tab 1 cho thang <strong>{selectedMonth}</strong>. Hay doi month hoac replay.
              </p>
            </section>
          ) : null}

          <section className="panel-grid tab1-grid">
            <article className="panel">
              <h2>Dynamic Kaplan-Meier Survival Curve</h2>
              <svg className="viz-svg" viewBox={`0 0 ${kmViewWidth} ${kmViewHeight}`} role="img" aria-label="KM curve">
                {[0, 0.25, 0.5, 0.75, 1].map((ratio) => (
                  <g key={`km-y-${ratio}`}>
                    <line
                      x1={kmPadding.left}
                      y1={kmY(ratio)}
                      x2={kmPadding.left + kmPlotWidth}
                      y2={kmY(ratio)}
                      stroke="rgba(148, 163, 184, 0.2)"
                      strokeWidth="1"
                    />
                    <text x={8} y={kmY(ratio) + 4} className="axis-label">
                      {Math.round(ratio * 100)}%
                    </text>
                  </g>
                ))}
                {[0, 0.25, 0.5, 0.75, 1].map((tick) => {
                  const day = Math.round(kmMaxDay * tick);
                  return (
                    <g key={`km-x-${tick}`}>
                      <line
                        x1={kmX(day)}
                        y1={kmPadding.top}
                        x2={kmX(day)}
                        y2={kmPadding.top + kmPlotHeight}
                        stroke="rgba(148, 163, 184, 0.12)"
                        strokeWidth="1"
                      />
                      <text x={kmX(day) - 8} y={kmViewHeight - 10} className="axis-label">
                        {day}
                      </text>
                    </g>
                  );
                })}
                {kmSeries.map((series, idx) => {
                  const color = KM_COLORS[idx % KM_COLORS.length];
                  const path = series.points
                    .map((point, pointIdx) => {
                      const x = kmX(Number(point.day) || 0);
                      const y = kmY(Number(point.survival_prob) || 0);
                      return `${pointIdx === 0 ? "M" : "L"} ${x} ${y}`;
                    })
                    .join(" ");
                  return (
                    <g key={`km-line-${series.dimension_value}`}>
                      <path d={path} stroke={color} strokeWidth="2.5" fill="none" />
                      {series.points.map((point) => (
                        <circle
                          key={`km-dot-${series.dimension_value}-${point.day}`}
                          cx={kmX(Number(point.day) || 0)}
                          cy={kmY(Number(point.survival_prob) || 0)}
                          r="3"
                          fill={color}
                        />
                      ))}
                    </g>
                  );
                })}
              </svg>
              <div className="legend-row">
                {kmSeries.map((series, idx) => (
                  <span key={`legend-${series.dimension_value}`} className="legend-item">
                    <span className="legend-dot" style={{ backgroundColor: KM_COLORS[idx % KM_COLORS.length] }} />
                    {series.dimension_value}
                  </span>
                ))}
              </div>
            </article>

            <article className="panel">
              <h2>Segment Mix (100% Stacked) - Click To Filter</h2>
              <div className="segment-groups">
                {(Object.keys(tab1SegmentGroups) as SegmentType[]).map((segmentType) => (
                  <div key={`segment-group-${segmentType}`} className="segment-group">
                    <h3>{SEGMENT_LABELS[segmentType]}</h3>
                    {tab1SegmentGroups[segmentType].length === 0 ? <p className="segment-empty">No rows</p> : null}
                    {tab1SegmentGroups[segmentType].map((row) => {
                      const selected =
                        segmentFilter.segmentType === row.segment_type && segmentFilter.segmentValue === row.segment_value;
                      const retain = clamp(Number(row.retain_rate_pct) || 0, 0, 100);
                      const churn = clamp(Number(row.churn_rate_pct) || 0, 0, 100);
                      return (
                        <button
                          type="button"
                          key={`segment-row-${row.segment_type}-${row.segment_value}`}
                          className={`segment-row ${selected ? "is-selected" : ""}`}
                          onClick={() => toggleSegmentFilter(row.segment_type, row.segment_value)}
                        >
                          <div className="segment-row-head">
                            <span>{row.segment_value}</span>
                            <span>{formatNumber(Number(row.users) || 0)} users</span>
                          </div>
                          <div className="stacked-track">
                            <div className="stacked-retain" style={{ width: `${retain}%` }} />
                            <div className="stacked-churn" style={{ width: `${churn}%` }} />
                          </div>
                          <div className="segment-row-foot">
                            <span>Retain {retain.toFixed(1)}%</span>
                            <span>Churn {churn.toFixed(1)}%</span>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                ))}
              </div>
            </article>

            <article className="panel">
              <h2>Boredom Matrix (Discovery vs Skip)</h2>
              <svg
                className="viz-svg"
                viewBox={`0 0 ${scatterViewWidth} ${scatterViewHeight}`}
                role="img"
                aria-label="Boredom scatter"
              >
                {[0, 0.25, 0.5, 0.75, 1].map((ratio) => (
                  <g key={`scatter-grid-${ratio}`}>
                    <line
                      x1={scatterPadding.left}
                      y1={scatterY(ratio)}
                      x2={scatterPadding.left + scatterPlotWidth}
                      y2={scatterY(ratio)}
                      stroke="rgba(148, 163, 184, 0.15)"
                      strokeWidth="1"
                    />
                    <line
                      x1={scatterX(ratio)}
                      y1={scatterPadding.top}
                      x2={scatterX(ratio)}
                      y2={scatterPadding.top + scatterPlotHeight}
                      stroke="rgba(148, 163, 184, 0.15)"
                      strokeWidth="1"
                    />
                    <text x={scatterX(ratio) - 10} y={scatterViewHeight - 10} className="axis-label">
                      {ratio.toFixed(2)}
                    </text>
                    <text x={8} y={scatterY(ratio) + 4} className="axis-label">
                      {ratio.toFixed(2)}
                    </text>
                  </g>
                ))}
                {scatterPoints.map((point, idx) => {
                  const users = Number(point.users) || 0;
                  const radius = 4 + Math.sqrt(users / scatterMaxUsers) * 14;
                  return (
                    <g key={`scatter-point-${idx}`}>
                      <circle
                        cx={scatterX(Number(point.discovery_ratio) || 0)}
                        cy={scatterY(Number(point.skip_ratio) || 0)}
                        r={radius}
                        fill={bubbleColor(Number(point.churn_rate_pct) || 0)}
                        fillOpacity="0.78"
                        stroke="rgba(15, 23, 42, 0.65)"
                        strokeWidth="1"
                      />
                    </g>
                  );
                })}
              </svg>
              <p className="scatter-note">
                Bubble size = users, color = churn rate (green low - red high). Upper-left is healthier than lower-right.
              </p>
            </article>
          </section>
        </>
      ) : null}

      {activeTab === "tab2" ? (
        <>
          <section className="panel model-panel">
            <h2>Predictive Model Controls</h2>
            <div className="param-grid">
              <label>
                Base Prob
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="1"
                  value={modelParams.base_prob}
                  onChange={(e) => updateModelParam("base_prob", Number(e.target.value))}
                />
              </label>
              <label>
                Weight Manual
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="2"
                  value={modelParams.weight_manual}
                  onChange={(e) => updateModelParam("weight_manual", Number(e.target.value))}
                />
              </label>
              <label>
                Weight Low Activity
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="2"
                  value={modelParams.weight_low_activity}
                  onChange={(e) => updateModelParam("weight_low_activity", Number(e.target.value))}
                />
              </label>
              <label>
                Weight High Skip
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="2"
                  value={modelParams.weight_high_skip}
                  onChange={(e) => updateModelParam("weight_high_skip", Number(e.target.value))}
                />
              </label>
              <label>
                Weight Low Discovery
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="2"
                  value={modelParams.weight_low_discovery}
                  onChange={(e) => updateModelParam("weight_low_discovery", Number(e.target.value))}
                />
              </label>
              <label>
                Weight Cancel Signal
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="2"
                  value={modelParams.weight_cancel_signal}
                  onChange={(e) => updateModelParam("weight_cancel_signal", Number(e.target.value))}
                />
              </label>
              <label>
                Prob Min
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="1"
                  value={modelParams.prob_min}
                  onChange={(e) => updateModelParam("prob_min", Number(e.target.value))}
                />
              </label>
              <label>
                Prob Max
                <input
                  type="number"
                  step="0.01"
                  min="0"
                  max="1"
                  value={modelParams.prob_max}
                  onChange={(e) => updateModelParam("prob_max", Number(e.target.value))}
                />
              </label>
            </div>
            <p className="panel-note">
              Trong artifact-backed mode, Tab 2 ưu tiên output từ model đã train. Các tham số trên chỉ còn vai trò tương thích ngược và sẽ không override scored artifact production.
            </p>
          </section>

          <section className="kpi-grid">
            <article className="kpi-card">
              <h3>Forecasted 30-Day Churn Rate</h3>
              <p>{tab2Data ? formatPct(tab2Data.kpis.forecasted_churn_rate) : "-"}</p>
              <small>
                Delta vs {tab2Data?.meta.previous_month ?? "prev"}: {tab2Data ? tab2Data.kpis.forecasted_churn_delta_pp_vs_prev_month.toFixed(2) : "-"} pp
              </small>
            </article>
            <article className="kpi-card">
              <h3>Predicted Revenue at Risk</h3>
              <p>{tab2Data ? formatCurrency(tab2Data.kpis.predicted_revenue_at_risk) : "-"}</p>
              <small>Sample users: {tab2Data ? formatNumber(tab2Data.meta.sample_user_count) : "-"}</small>
            </article>
            <article className="kpi-card">
              <h3>Total Expected Retained Revenue (30d)</h3>
              <p>{tab2Data ? formatCurrency(tab2Data.kpis.predicted_total_future_cltv) : "-"}</p>
            </article>
            <article className="kpi-card">
              <h3>Top Flight-Risk Segment</h3>
              <p className="segment-title">{tab2Data?.kpis.top_segment ?? "-"}</p>
              <small>
                Risk: {tab2Data ? formatCurrency(tab2Data.kpis.top_segment_risk) : "-"} | Users:{" "}
                {tab2Data ? formatNumber(tab2Data.kpis.top_segment_user_count) : "-"}
              </small>
            </article>
          </section>

          {tab2Loading ? (
            <section className="panel">
              <h2>Loading Tab 2...</h2>
            </section>
          ) : null}

          {!tab2Loading && !tab2HasData && hasMonth ? (
            <section className="panel">
              <h2>No Tab 2 Data In Selected Month</h2>
              <p>
                Chua co du lieu predictive cho thang <strong>{selectedMonth}</strong>. Hay doi month hoac replay/materialize.
              </p>
            </section>
          ) : null}

          <section className="panel-grid tab2-grid">
            <article className="panel">
              <h2>Value vs Risk Segment Matrix</h2>
              <table>
                <thead>
                  <tr>
                    <th>Segment</th>
                    <th>Users</th>
                    <th>Avg Churn %</th>
                    <th>Avg Retained Revenue (30d)</th>
                    <th>Quadrant</th>
                  </tr>
                </thead>
                <tbody>
                  {(tab2Data?.value_risk_matrix ?? []).slice(0, 12).map((row) => (
                    <tr key={`matrix-${row.strategic_segment}`}>
                      <td>{row.strategic_segment}</td>
                      <td>{formatNumber(Number(row.user_count) || 0)}</td>
                      <td>{formatPct(Number(row.avg_churn_prob_pct) || 0)}</td>
                      <td>{formatCurrency(Number(row.avg_future_cltv) || 0)}</td>
                      <td>{row.quadrant}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </article>

            <article className="panel">
              <h2>Revenue at Risk by Driver</h2>
              <div className="leak-list">
                {(tab2Data?.revenue_leakage ?? []).map((row) => {
                  const maxRisk = Math.max(
                    1,
                    ...(tab2Data?.revenue_leakage ?? []).map((item) => Number(item.revenue_at_risk) || 0),
                  );
                  const width = ((Number(row.revenue_at_risk) || 0) / maxRisk) * 100;
                  return (
                    <div className="leak-row" key={`leak-${row.risk_driver}`}>
                      <div className="leak-head">
                        <strong>{row.risk_driver}</strong>
                        <span>{formatCurrency(Number(row.revenue_at_risk) || 0)}</span>
                      </div>
                      <div className="leak-track">
                        <div className="leak-fill" style={{ width: `${width}%` }} />
                      </div>
                      <small>{formatNumber(Number(row.user_count) || 0)} users</small>
                    </div>
                  );
                })}
              </div>
            </article>

            <article className="panel">
              <h2>Forecasted Retention Decay (T+1/T+3/T+6/T+12)</h2>
              <table>
                <thead>
                  <tr>
                    <th>Segment</th>
                    <th>T+1</th>
                    <th>T+3</th>
                    <th>T+6</th>
                    <th>T+12</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(tab2DecaySummary).map(([segment, points]) => (
                    <tr key={`decay-${segment}`}>
                      <td>{segment}</td>
                      <td>{points[1] ? formatPct(points[1]) : "-"}</td>
                      <td>{points[3] ? formatPct(points[3]) : "-"}</td>
                      <td>{points[6] ? formatPct(points[6]) : "-"}</td>
                      <td>{points[12] ? formatPct(points[12]) : "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </article>
          </section>

          <section className="panel">
            <h2>Actionable Insights (Strategic Prescriptions)</h2>
            <table>
              <thead>
                <tr>
                  <th>Segment Name</th>
                  <th>User Count</th>
                  <th>Average Churn Prob (%)</th>
                  <th>Revenue at Risk</th>
                  <th>Primary Risk Driver</th>
                </tr>
              </thead>
              <tbody>
                {(tab2Data?.prescriptions ?? []).slice(0, 20).map((row) => (
                  <tr key={`prescription-${row.strategic_segment}`}>
                    <td>{row.strategic_segment}</td>
                    <td>{formatNumber(Number(row.user_count) || 0)}</td>
                    <td>{formatPct(Number(row.avg_churn_prob_pct) || 0)}</td>
                    <td>{formatCurrency(Number(row.revenue_at_risk) || 0)}</td>
                    <td>{row.primary_risk_driver}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </>
      ) : null}

      {activeTab === "tab3" ? (
        <>
          <section className="panel scenario-panel">
            <h2>Control Panel (Scenario Inputs)</h2>
            <div className="slider-grid">
              <label>
                Shift Manual to Auto-Renew: <strong>{scenarioInputs.auto_shift_pct}%</strong>
                <input
                  type="range"
                  min="0"
                  max="100"
                  step="1"
                  value={scenarioInputs.auto_shift_pct}
                  onChange={(e) => updateScenarioInput("auto_shift_pct", Number(e.target.value))}
                />
              </label>
              <label>
                Shift Deal to Standard: <strong>{scenarioInputs.upsell_shift_pct}%</strong>
                <input
                  type="range"
                  min="0"
                  max="100"
                  step="1"
                  value={scenarioInputs.upsell_shift_pct}
                  onChange={(e) => updateScenarioInput("upsell_shift_pct", Number(e.target.value))}
                />
              </label>
              <label>
                Reduce High-Skip Users: <strong>{scenarioInputs.skip_shift_pct}%</strong>
                <input
                  type="range"
                  min="0"
                  max="100"
                  step="1"
                  value={scenarioInputs.skip_shift_pct}
                  onChange={(e) => updateScenarioInput("skip_shift_pct", Number(e.target.value))}
                />
              </label>
            </div>
          </section>

          <section className="kpi-grid">
            <article className="kpi-card">
              <h3>Baseline Avg Risk</h3>
              <p>{tab3Data ? tab3Data.kpis.baseline_avg_hazard.toFixed(3) : "-"}</p>
            </article>
            <article className="kpi-card">
              <h3>Scenario Avg Risk</h3>
              <p>{tab3Data ? tab3Data.kpis.scenario_avg_hazard.toFixed(3) : "-"}</p>
              <small>
                Delta: {tab3Data ? (tab3Data.kpis.scenario_avg_hazard - tab3Data.kpis.baseline_avg_hazard).toFixed(3) : "-"}
              </small>
            </article>
            <article className="kpi-card">
              <h3>Scenario Churn Probability</h3>
              <p>{tab3Data ? formatPct(tab3Data.kpis.scenario_churn_prob_pct) : "-"}</p>
              <small>
                Baseline: {tab3Data ? formatPct(tab3Data.kpis.baseline_churn_prob_pct) : "-"}
              </small>
            </article>
            <article className="kpi-card">
              <h3>Optimized Projected Revenue</h3>
              <p>{tab3Data ? formatCurrency(tab3Data.kpis.optimized_projected_revenue) : "-"}</p>
            </article>
          </section>

          {tab3Loading ? (
            <section className="panel">
              <h2>Loading Tab 3...</h2>
            </section>
          ) : null}

          {!tab3Loading && !tab3HasData && hasMonth ? (
            <section className="panel">
              <h2>No Tab 3 Data In Selected Month</h2>
              <p>
                Chua co du lieu simulation cho thang <strong>{selectedMonth}</strong>. Hay doi month hoac replay/materialize.
              </p>
            </section>
          ) : null}

          <section className="panel-grid tab3-grid">
            <article className="panel">
              <h2>Population Risk Shift (Baseline vs Scenario)</h2>
              <div className="hazard-histogram">
                {(tab3Data?.hazard_histogram ?? []).map((point, idx) => {
                  const baseHeight = (Number(point.baseline_density) / tab3MaxDensity) * 100;
                  const scenarioHeight = (Number(point.scenario_density) / tab3MaxDensity) * 100;
                  return (
                    <div className="hazard-bin" key={`haz-${idx}`}>
                      <div className="hazard-bar baseline" style={{ height: `${baseHeight}%` }} />
                      <div className="hazard-bar scenario" style={{ height: `${scenarioHeight}%` }} />
                    </div>
                  );
                })}
              </div>
              <p className="scatter-note">Gray = baseline, blue = scenario. Cột dịch xuống thấp hơn thể hiện phân bố rủi ro tốt hơn sau can thiệp.</p>
            </article>

            <article className="panel">
              <h2>Financial Impact Analysis</h2>
              <table>
                <thead>
                  <tr>
                    <th>Component</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {(tab3Data?.financial_waterfall ?? []).map((row) => (
                    <tr key={`wf-${row.name}`}>
                      <td>{row.name}</td>
                      <td>{formatCurrency(Number(row.value) || 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </article>

            <article className="panel">
              <h2>Sensitivity Analysis (ROI per +1%)</h2>
              <div className="roi-list">
                {(tab3Data?.sensitivity_roi ?? []).map((row) => {
                  const value = Number(row.revenue_impact_per_1pct) || 0;
                  const width = (Math.abs(value) / tab3MaxRoi) * 100;
                  return (
                    <div className="roi-row" key={`roi-${row.strategy}`}>
                      <div className="roi-head">
                        <strong>{row.strategy}</strong>
                        <span>{formatCurrency(value)}</span>
                      </div>
                      <div className="roi-track">
                        <div className={`roi-fill ${value >= 0 ? "positive" : "negative"}`} style={{ width: `${width}%` }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </article>
          </section>
        </>
      ) : null}

      <footer className="footer">
        <p>As of: {snapshot?.meta.as_of ?? "-"}</p>
      </footer>
    </main>
  );
}
