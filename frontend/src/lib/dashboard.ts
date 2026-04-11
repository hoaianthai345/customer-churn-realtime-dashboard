export type MetricPayload = {
  total_expiring_users: number;
  historical_churn_rate: number;
  median_survival_days: number;
  auto_renew_rate: number;
};

export type SeriesPoint = {
  event_date: string;
  [key: string]: string | number;
};

export type SnapshotPayload = {
  meta: {
    month: string;
    month_start: string;
    month_end_exclusive: string;
    as_of: string;
    context_month?: string;
    context_month_start?: string;
    context_month_end_exclusive?: string;
    artifact_mode?: string;
    series_mode?: string;
    artifact_dir?: string | null;
    pulse_artifact_dir?: string | null;
    pulse_as_of?: string | null;
  };
  metrics: MetricPayload;
  revenue_series: SeriesPoint[];
  risk_series: SeriesPoint[];
  activity_series: SeriesPoint[];
};

export type PulsePoint = {
  event_date: string;
  total_revenue: number;
  total_transactions: number;
  high_risk_users: number;
  avg_risk_score: number;
  active_users: number;
  total_listening_secs: number;
};

export type PulseReplayStatus = "idle" | "playing" | "paused" | "completed";

export type PulseReplayFrame = {
  point: PulsePoint | null;
  dateLabel: string | null;
  status: PulseReplayStatus;
};

export type ReplayStatus = {
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

export type Tab1Dimension = "age" | "gender" | "txn_freq" | "skip_ratio";
export type SegmentType = "price_segment" | "loyalty_segment" | "active_segment";

export type Tab1Kpis = {
  total_expiring_users: number;
  historical_churn_rate: number;
  overall_median_survival: number;
  auto_renew_rate: number;
  total_expected_renewal_amount?: number;
  historical_revenue_at_risk?: number;
};

export type Tab1MonthlyTrendPoint = {
  target_month: number;
  month_label: string;
  total_expiring_users: number;
  historical_churn_rate: number;
  overall_median_survival: number;
  auto_renew_rate: number;
  total_expected_renewal_amount?: number | null;
  historical_revenue_at_risk?: number | null;
  apru?: number | null;
  new_paid_users?: number | null;
  churned_users?: number | null;
  net_movement?: number | null;
};

export type Tab1ChurnBreakdown = {
  renewed_users: number;
  churned_users: number;
  renewed_rate: number;
  churned_rate: number;
};

export type Tab1RiskHeatmapCell = {
  value_tier: string;
  risk_segment: string;
  users: number;
};

export type KmPoint = {
  day: number;
  survival_prob: number;
  at_risk?: number;
  events?: number;
};

export type KmSeries = {
  dimension_value: string;
  points: KmPoint[];
};

export type SegmentMixRow = {
  segment_type: SegmentType;
  segment_value: string;
  users: number;
  churn_rate_pct: number;
  retain_rate_pct: number;
};

export type BoredomPoint = {
  discovery_ratio: number;
  skip_ratio: number;
  users: number;
  churn_rate_pct: number;
  revenue_at_risk?: number;
  avg_expected_renewal_amount?: number;
  cluster_label?: string;
  discovery_bin?: string;
  skip_bin?: string;
};

export type Tab1Payload = {
  meta: {
    month: string;
    dimension: string;
    trend_scope?: "overall" | "filtered";
    previous_month?: string | null;
    churn_breakdown_month?: string | null;
    risk_heatmap_month?: string | null;
    segment_filter: {
      segment_type: SegmentType | null;
      segment_value: string | null;
    };
  };
  kpis: Tab1Kpis;
  previous_kpis?: Tab1Kpis | null;
  monthly_trend?: Tab1MonthlyTrendPoint[];
  churn_breakdown?: Tab1ChurnBreakdown;
  risk_heatmap?: Tab1RiskHeatmapCell[];
  km_curve: KmSeries[];
  segment_mix: SegmentMixRow[];
  boredom_scatter: BoredomPoint[];
};

export type SegmentFilterState = {
  segmentType: SegmentType | null;
  segmentValue: string | null;
};

export type ModelParamState = {
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

export type PredictiveKpis = {
  forecasted_churn_rate: number;
  high_flight_risk_users: number;
  predicted_revenue_at_risk: number;
  predicted_total_future_cltv: number;
  safe_revenue: number;
  top_segment: string;
  top_segment_risk: number;
  top_segment_user_count: number;
  forecasted_churn_delta_pp_vs_prev_month: number;
};

export type PredictiveMatrixRow = {
  strategic_segment: string;
  user_count: number;
  avg_churn_prob: number;
  avg_churn_prob_pct: number;
  avg_future_cltv: number;
  total_future_cltv: number;
  revenue_at_risk: number;
  primary_risk_driver: string;
  recommended_action?: string;
  quadrant: string;
};

export type PredictiveExecutiveMatrixBubble = {
  prob_bin: number;
  expected_renewal_amount: number;
  risk_band: string;
  risk_tier: string;
  user_count: number;
  revenue_at_risk: number;
  display_size: number;
  priority_quadrant?: string;
};

export type RevenueLeakageRow = {
  risk_driver: string;
  user_count: number;
  revenue_at_risk: number;
};

export type ForecastDecayPoint = {
  month_num: number;
  timeline: string;
  segment: string;
  retention_pct: number;
};

export type RevenueLossOutlookPoint = {
  horizon_months: number;
  horizon_label: string;
  projected_revenue_loss: number;
  projected_loss_share_pct: number;
};

export type RiskBandMixRow = {
  band: string;
  user_count: number;
  revenue_at_risk: number;
  revenue_share_pct: number;
};

export type FeatureGroupImportanceRow = {
  feature_group: string;
  importance_gain: number;
  importance_split: number;
  feature_count: number;
};

export type FeatureGroupWaterfallPoint = {
  feature_group: string;
  display_name: string;
  contribution: number;
  contribution_pct: number;
  importance_gain: number;
  importance_split: number;
  feature_count: number;
};

export type SankeyNode = {
  name: string;
  color?: string | null;
  stage?: string | null;
};

export type SankeyLink = {
  source: number;
  target: number;
  value: number;
  color?: string | null;
  risk_tier?: string | null;
};

export type SankeyPayload = {
  nodes: SankeyNode[];
  links: SankeyLink[];
};

export type PriceParadoxRow = {
  price_bucket: string;
  user_count: number;
  revenue_at_risk: number;
  churn_rate_pct: number;
};

export type HabitFunnelRow = {
  habit_stage: string;
  user_count: number;
  revenue_at_risk: number;
  share_of_top_pct: number;
};

export type PredictivePayload = {
  meta: {
    month: string;
    previous_month: string;
    sample_user_count: number;
    artifact_mode?: string;
    artifact_dir?: string | null;
    segment_filter: {
      segment_type: SegmentType | null;
      segment_value: string | null;
    };
  };
  model_params: ModelParamState;
  kpis: PredictiveKpis;
  previous_kpis: PredictiveKpis;
  value_risk_matrix: PredictiveMatrixRow[];
  executive_value_risk_matrix?: PredictiveExecutiveMatrixBubble[];
  revenue_leakage: RevenueLeakageRow[];
  forecast_decay: ForecastDecayPoint[];
  revenue_loss_outlook?: RevenueLossOutlookPoint[];
  prescriptions: PredictiveMatrixRow[];
  risk_band_mix: RiskBandMixRow[];
  feature_group_waterfall: FeatureGroupWaterfallPoint[];
  feature_group_importance?: FeatureGroupImportanceRow[];
  revenue_flow_sankey: SankeyPayload;
  price_paradox: PriceParadoxRow[];
  habit_funnel: HabitFunnelRow[];
};

export type ScenarioInputs = {
  auto_shift_pct: number;
  upsell_shift_pct: number;
  skip_shift_pct: number;
};

export type ScenarioPreset = {
  scenario_id: string;
  label: string;
  description?: string | null;
  scenario_inputs: ScenarioInputs;
  is_default?: boolean;
  has_monte_carlo?: boolean;
};

export type PrescriptiveKpis = {
  baseline_avg_hazard: number;
  scenario_avg_hazard: number;
  baseline_churn_prob_pct: number;
  scenario_churn_prob_pct: number;
  optimized_projected_revenue: number;
  baseline_revenue: number;
  saved_revenue: number;
  incremental_upsell: number;
  campaign_cost?: number;
  net_value_after_cost?: number;
};

export type HazardHistogramPoint = {
  bin_start: number;
  bin_end: number;
  baseline_density: number;
  scenario_density: number;
};

export type WaterfallPoint = {
  name: string;
  value: number;
};

export type SensitivityPoint = {
  strategy: string;
  revenue_impact_per_1pct: number;
};

export type MonteCarloMetric = {
  metric: string;
  column?: string;
  mean: number;
  std: number;
  p05: number;
  p25: number;
  p50: number;
  p75: number;
  p95: number;
};

export type MonteCarloDistributionPoint = {
  bucket_start: number;
  bucket_end: number;
  bucket_mid: number;
  bucket_label: string;
  run_count: number;
  share_pct: number;
};

export type MonteCarloPayload = {
  enabled: boolean;
  artifact_dir: string | null;
  n_iterations: number;
  seed: number | null;
  beta_concentration?: number | null;
  population_users?: number;
  simulation_unit_count?: number;
  probability_scenario_beats_baseline: number | null;
  probability_net_positive: number | null;
  deterministic_summary: Record<string, number>;
  summary_metrics: MonteCarloMetric[];
  net_value_distribution: MonteCarloDistributionPoint[];
};

export type PrescriptivePayload = {
  meta: {
    month: string;
    sample_user_count: number;
    segment_filter: {
      segment_type: SegmentType | null;
      segment_value: string | null;
    };
    scenario_id?: string;
    scenario_label?: string;
    scenario_description?: string | null;
    available_scenarios?: ScenarioPreset[];
  };
  model_params: ModelParamState;
  scenario_inputs: ScenarioInputs;
  kpis: PrescriptiveKpis;
  hazard_histogram: HazardHistogramPoint[];
  financial_waterfall: WaterfallPoint[];
  sensitivity_roi: SensitivityPoint[];
  monte_carlo: MonteCarloPayload;
};

export const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "/backend";
export const DEFAULT_REPLAY_START_DATE = import.meta.env.VITE_REPLAY_START_DATE ?? "2017-03-01";
export const DEMO_MODE = import.meta.env.VITE_DEMO_MODE === "1";
export const DEFAULT_DEMO_MONTH = import.meta.env.VITE_DEFAULT_MONTH ?? "2017-04";

export const DIMENSION_LABELS: Record<Tab1Dimension, string> = {
  age: "Độ tuổi",
  gender: "Giới tính",
  txn_freq: "Tần suất thanh toán",
  skip_ratio: "Tỷ lệ bỏ qua bài hát",
};

export const SEGMENT_LABELS: Record<SegmentType, string> = {
  price_segment: "Nhóm gói cước",
  loyalty_segment: "Nhóm gắn bó",
  active_segment: "Nhóm hoạt động",
};

export const DEFAULT_MODEL_PARAMS: ModelParamState = {
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

export const DEFAULT_SCENARIO_INPUTS: ScenarioInputs = {
  auto_shift_pct: 20,
  upsell_shift_pct: 15,
  skip_shift_pct: 25,
};

export function resolveWsBase(): string {
  const explicitBase = import.meta.env.VITE_WS_BASE_URL;
  if (explicitBase) {
    return explicitBase;
  }

  if (typeof window === "undefined") {
    return "ws://localhost:8000";
  }

  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.host}/backend`;
}

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat("vi-VN").format(Math.round(value));
}

export function formatPct(value: number, digits = 2): string {
  return `${value.toFixed(digits)}%`;
}

export function formatCurrency(value: number): string {
  return new Intl.NumberFormat("vi-VN", {
    style: "currency",
    currency: "TWD",
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatCompactCurrency(value: number): string {
  return new Intl.NumberFormat("vi-VN", {
    style: "currency",
    currency: "TWD",
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(value);
}

export function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("vi-VN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

export function formatMonthLabel(monthText: string | null | undefined): string {
  if (!monthText || !/^\d{4}-\d{2}$/.test(monthText)) return "-";
  const [year, month] = monthText.split("-").map(Number);
  return `Tháng ${String(month).padStart(2, "0")}/${year}`;
}

export function formatSignedNumber(value: number, digits = 0): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(digits)}`;
}

export function latestSeriesValue(points: SeriesPoint[], key: string): number {
  if (!points.length) return 0;
  return Number(points[points.length - 1]?.[key] ?? 0);
}

export function previousSeriesValue(points: SeriesPoint[], key: string): number {
  if (points.length < 2) return 0;
  return Number(points[points.length - 2]?.[key] ?? 0);
}

export function trendDelta(points: SeriesPoint[], key: string): number {
  return latestSeriesValue(points, key) - previousSeriesValue(points, key);
}

export function buildSnapshotPulse(snapshot: SnapshotPayload | null): PulsePoint[] {
  const rows = new Map<string, PulsePoint>();

  for (const point of snapshot?.revenue_series ?? []) {
    rows.set(point.event_date, {
      ...(rows.get(point.event_date) ?? {
        event_date: point.event_date,
        total_revenue: 0,
        total_transactions: 0,
        high_risk_users: 0,
        avg_risk_score: 0,
        active_users: 0,
        total_listening_secs: 0,
      }),
      total_revenue: Number(point.total_revenue ?? 0),
      total_transactions: Number(point.total_transactions ?? 0),
    });
  }

  for (const point of snapshot?.risk_series ?? []) {
    rows.set(point.event_date, {
      ...(rows.get(point.event_date) ?? {
        event_date: point.event_date,
        total_revenue: 0,
        total_transactions: 0,
        high_risk_users: 0,
        avg_risk_score: 0,
        active_users: 0,
        total_listening_secs: 0,
      }),
      high_risk_users: Number(point.high_risk_users ?? 0),
      avg_risk_score: Number(point.avg_risk_score ?? 0),
    });
  }

  for (const point of snapshot?.activity_series ?? []) {
    rows.set(point.event_date, {
      ...(rows.get(point.event_date) ?? {
        event_date: point.event_date,
        total_revenue: 0,
        total_transactions: 0,
        high_risk_users: 0,
        avg_risk_score: 0,
        active_users: 0,
        total_listening_secs: 0,
      }),
      active_users: Number(point.active_users ?? 0),
      total_listening_secs: Number(point.total_listening_secs ?? 0),
    });
  }

  return Array.from(rows.values()).sort((a, b) => a.event_date.localeCompare(b.event_date));
}

export function formatPulseDateLabel(value: string | null | undefined): string {
  if (!value) return "-";
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return value;
  return `Ngày ${match[3]}/${match[2]}/${match[1]}`;
}

export function normalizeMonths(...monthSets: string[][]): string[] {
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

export function toYearMonth(monthText: string): { year: number; month: number } {
  const [year, month] = monthText.split("-").map((value) => Number(value));
  return { year, month };
}

export function hasTab1Data(payload: Tab1Payload | null): boolean {
  if (!payload) return false;
  return (
    payload.kpis.total_expiring_users > 0 ||
    (payload.monthly_trend?.length ?? 0) > 0 ||
    (payload.risk_heatmap?.length ?? 0) > 0 ||
    payload.km_curve.length > 0 ||
    payload.segment_mix.length > 0 ||
    payload.boredom_scatter.length > 0
  );
}

export function hasTab2Data(payload: PredictivePayload | null): boolean {
  if (!payload) return false;
  return (
    payload.meta.sample_user_count > 0 ||
    (payload.executive_value_risk_matrix?.length ?? 0) > 0 ||
    payload.value_risk_matrix.length > 0 ||
    payload.revenue_leakage.length > 0 ||
    payload.forecast_decay.length > 0
  );
}

export function hasTab3Data(payload: PrescriptivePayload | null): boolean {
  if (!payload) return false;
  return payload.meta.sample_user_count > 0 || payload.hazard_histogram.length > 0 || payload.monte_carlo?.enabled === true;
}

export function appendSegmentFilter(params: URLSearchParams, filter: SegmentFilterState): void {
  if (filter.segmentType && filter.segmentValue) {
    params.set("segment_type", filter.segmentType);
    params.set("segment_value", filter.segmentValue);
  }
}

export function appendModelParams(params: URLSearchParams, modelParams: ModelParamState): void {
  for (const [key, value] of Object.entries(modelParams)) {
    params.set(key, String(value));
  }
}

export function buildKmChartData(series: KmSeries[]): Array<Record<string, number | string>> {
  const points = new Map<number, Record<string, number | string>>();

  for (const entry of series) {
    for (const point of entry.points) {
      const row = points.get(point.day) ?? { day: point.day };
      row[entry.dimension_value] = Number(point.survival_prob) * 100;
      points.set(point.day, row);
    }
  }

  return Array.from(points.values()).sort((a, b) => Number(a.day) - Number(b.day));
}

export function buildForecastDecayChartData(points: ForecastDecayPoint[]): Array<Record<string, number | string>> {
  const rows = new Map<string, Record<string, number | string>>();

  for (const point of points) {
    const label = point.timeline || `T+${point.month_num}`;
    const row = rows.get(label) ?? { timeline: label, month_num: point.month_num };
    row[point.segment] = Number(point.retention_pct);
    rows.set(label, row);
  }

  return Array.from(rows.values()).sort((a, b) => Number(a.month_num) - Number(b.month_num));
}

export function buildHazardChartData(points: HazardHistogramPoint[]): Array<Record<string, number | string>> {
  return points.map((point) => ({
    bucket: `${point.bin_start.toFixed(2)}-${point.bin_end.toFixed(2)}`,
    baseline: Number(point.baseline_density) || 0,
    scenario: Number(point.scenario_density) || 0,
  }));
}

export function bubbleColor(churnRate: number): string {
  const level = clamp((Number(churnRate) || 0) / 100, 0, 1);
  const hue = 145 - level * 145;
  return `hsl(${hue} 72% 48%)`;
}
