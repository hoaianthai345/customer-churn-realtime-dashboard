import { Fragment, memo, useMemo } from "react";
import {
  Area,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import ChartCard from "@/components/dashboard/ChartCard";
import DescriptivePulsePanel from "@/components/dashboard/DescriptivePulsePanel";
import InsightCard from "@/components/dashboard/InsightCard";
import StatePanel from "@/components/dashboard/StatePanel";
import {
  SEGMENT_LABELS,
  clamp,
  formatCompactCurrency,
  formatCurrency,
  formatMonthLabel,
  formatNumber,
  formatPct,
  type PulseReplayFrame,
  type SegmentFilterState,
  type SnapshotPayload,
  type Tab1Dimension,
  type Tab1Payload,
} from "@/lib/dashboard";

type DescriptiveTabProps = {
  data: Tab1Payload | null;
  snapshot: SnapshotPayload | null;
  loading: boolean;
  onReplayFrameChange?: (frame: PulseReplayFrame) => void;
  hidePulsePanel?: boolean;
  error: string | null;
  selectedMonth: string;
  dimension: Tab1Dimension;
  onDimensionChange: (dimension: Tab1Dimension) => void;
  segmentFilter: SegmentFilterState;
  onToggleSegmentFilter: (segmentType: keyof typeof SEGMENT_LABELS, segmentValue: string) => void;
  onClearFilter: () => void;
};

const CHURN_FLOOR_PCT = 0;
const CHURN_WARNING_PCT = 4.5;
const CHURN_CRITICAL_PCT = 5.5;
const CHURN_DONUT_COLORS = ["#10b981", "#ef4444"];
const BEHAVIOR_CLUSTER_COLORS = ["#0f172a", "#1d4ed8", "#2563eb", "#60a5fa", "#93c5fd"];

function toFiniteNumbers(values: Array<number | null | undefined>): number[] {
  return values.filter((value): value is number => typeof value === "number" && Number.isFinite(value));
}

function roundDownToStep(value: number, step: number): number {
  return Math.floor(value / step) * step;
}

function roundUpToStep(value: number, step: number): number {
  return Math.ceil(value / step) * step;
}

function pickAxisStep(range: number): number {
  if (range <= 1) return 0.1;
  if (range <= 5) return 0.25;
  if (range <= 10) return 0.5;
  if (range <= 30) return 1;
  if (range <= 100) return 5;
  if (range <= 500) return 20;
  if (range <= 2_000) return 50;
  if (range <= 10_000) return 200;
  if (range <= 50_000) return 1_000;
  return 5_000;
}

function computeDynamicDomain(
  values: Array<number | null | undefined>,
  options: {
    paddingRatio?: number;
    minSpan?: number;
    clampMin?: number;
    clampMax?: number;
    anchorMin?: number;
    anchorMax?: number;
    step?: number;
  } = {},
): [number, number] {
  const {
    paddingRatio = 0.12,
    minSpan = 1,
    clampMin,
    clampMax,
    anchorMin,
    anchorMax,
    step,
  } = options;
  const finite = toFiniteNumbers(values);

  let minValue = finite.length ? Math.min(...finite) : anchorMin ?? clampMin ?? 0;
  let maxValue = finite.length ? Math.max(...finite) : anchorMax ?? clampMax ?? minValue + minSpan;

  if (anchorMin != null) minValue = Math.min(minValue, anchorMin);
  if (anchorMax != null) maxValue = Math.max(maxValue, anchorMax);

  const spread = maxValue - minValue;
  const padding = spread > 0 ? spread * paddingRatio : minSpan * 0.5;
  let lower = minValue - padding;
  let upper = maxValue + padding;

  if (upper - lower < minSpan) {
    const midpoint = (upper + lower) / 2;
    lower = midpoint - minSpan / 2;
    upper = midpoint + minSpan / 2;
  }

  if (clampMin != null) lower = Math.max(lower, clampMin);
  if (clampMax != null) upper = Math.min(upper, clampMax);

  const effectiveStep = step ?? pickAxisStep(upper - lower);
  lower = roundDownToStep(lower, effectiveStep);
  upper = roundUpToStep(upper, effectiveStep);

  if (clampMin != null) lower = Math.max(lower, clampMin);
  if (clampMax != null) upper = Math.min(upper, clampMax);

  if (upper - lower < minSpan) {
    upper = lower + minSpan;
    if (clampMax != null && upper > clampMax) {
      upper = clampMax;
      lower = Math.max(clampMin ?? Number.NEGATIVE_INFINITY, upper - minSpan);
    }
  }

  return [Number(lower.toFixed(4)), Number(upper.toFixed(4))];
}

function normalizeRatioToPct(value: number | null | undefined): number {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) return 0;
  return clamp(numeric <= 1 ? numeric * 100 : numeric, 0, 100);
}

function DescriptiveTab({
  data,
  snapshot,
  loading,
  onReplayFrameChange,
  hidePulsePanel = false,
  error,
  selectedMonth,
  segmentFilter,
  onToggleSegmentFilter,
}: DescriptiveTabProps) {
  const monthlyTrend = useMemo(
    () => [...(data?.monthly_trend ?? [])].sort((a, b) => a.target_month - b.target_month),
    [data?.monthly_trend],
  );
  const trendSeries = useMemo(
    () =>
      monthlyTrend.map((point) => {
        const totalUsers = Math.max(Number(point.total_expiring_users ?? 0), 0);
        const churnRate = clamp(Number(point.historical_churn_rate ?? 0), 0, 100);
        const churnedUsers = Math.min(
          totalUsers,
          Math.max(0, Math.round(Number(point.churned_users ?? (totalUsers * churnRate) / 100))),
        );
        const newPaidUsers = point.new_paid_users == null ? null : Math.max(Number(point.new_paid_users), 0);
        const totalExpectedRenewalAmount =
          point.total_expected_renewal_amount == null ? null : Math.max(Number(point.total_expected_renewal_amount), 0);
        return {
          ...point,
          total_expected_renewal_amount: totalExpectedRenewalAmount,
          historical_revenue_at_risk:
            point.historical_revenue_at_risk == null
              ? totalExpectedRenewalAmount == null
                ? null
                : (totalExpectedRenewalAmount * churnRate) / 100
              : Math.max(Number(point.historical_revenue_at_risk), 0),
          churned_users: churnedUsers,
          renewed_users: Math.max(totalUsers - churnedUsers, 0),
          new_paid_users: newPaidUsers,
          net_movement:
            point.net_movement == null && newPaidUsers == null ? null : Number(point.net_movement ?? (newPaidUsers ?? 0) - churnedUsers),
        };
      }),
    [monthlyTrend],
  );
  const hasMultiMonthTrend = trendSeries.length > 1;
  const hasRevenueTrend = trendSeries.some(
    (point) => point.total_expected_renewal_amount != null || point.historical_revenue_at_risk != null,
  );
  const hasApruTrend = trendSeries.some((point) => point.apru != null);
  const hasNewVsChurnTrend = trendSeries.some((point) => point.new_paid_users != null);
  const latestRevenueRiskPoint = trendSeries[trendSeries.length - 1] ?? null;

  const segmentGroups = useMemo(() => {
    const grouped: Record<keyof typeof SEGMENT_LABELS, Array<Tab1Payload["segment_mix"][number]>> = {
      price_segment: [],
      loyalty_segment: [],
      active_segment: [],
    };

    for (const row of data?.segment_mix ?? []) {
      grouped[row.segment_type].push(row);
    }

    for (const key of Object.keys(grouped) as Array<keyof typeof SEGMENT_LABELS>) {
      grouped[key] = grouped[key].sort((a, b) => Number(b.users) - Number(a.users));
    }

    return grouped;
  }, [data?.segment_mix]);

  const hottestSegment = useMemo(
    () => [...(data?.segment_mix ?? [])].sort((a, b) => Number(b.churn_rate_pct) - Number(a.churn_rate_pct))[0] ?? null,
    [data?.segment_mix],
  );
  const biggestSegment = useMemo(
    () => [...(data?.segment_mix ?? [])].sort((a, b) => Number(b.users) - Number(a.users))[0] ?? null,
    [data?.segment_mix],
  );
  const riskiestBehavior = useMemo(
    () =>
      [...(data?.boredom_scatter ?? [])].sort(
        (a, b) =>
          Number(b.revenue_at_risk ?? 0) - Number(a.revenue_at_risk ?? 0) ||
          Number(b.churn_rate_pct) - Number(a.churn_rate_pct),
      )[0] ?? null,
    [data?.boredom_scatter],
  );
  const behaviorFocusPoints = useMemo(
    () =>
      [...(data?.boredom_scatter ?? [])]
        .sort(
          (a, b) =>
            Number(b.revenue_at_risk ?? 0) - Number(a.revenue_at_risk ?? 0) ||
            Number(b.users ?? 0) - Number(a.users ?? 0),
        )
        .slice(0, 5),
    [data?.boredom_scatter],
  );
  const behaviorScatterPoints = useMemo(
    () =>
      behaviorFocusPoints.map((point) => ({
        ...point,
        discovery_pct: normalizeRatioToPct(point.discovery_ratio),
        skip_pct: normalizeRatioToPct(point.skip_ratio),
      })),
    [behaviorFocusPoints],
  );
  const selectedFilterLabel =
    segmentFilter.segmentType && segmentFilter.segmentValue
      ? `${SEGMENT_LABELS[segmentFilter.segmentType]}: ${segmentFilter.segmentValue}`
      : null;
  const trendScopeNote = useMemo(() => {
    if (data?.meta.trend_scope === "filtered") {
      return "Chuỗi nhiều tháng đang bám theo bộ lọc hiện tại.";
    }
    if (selectedFilterLabel) {
      return "Chuỗi nhiều tháng vẫn phản ánh toàn bộ khách theo từng tháng; bộ lọc hiện tại chỉ áp vào snapshot.";
    }
    return "Chuỗi nhiều tháng phản ánh toàn bộ nhóm khách sắp hết hạn theo từng tháng.";
  }, [data?.meta.trend_scope, selectedFilterLabel]);
  const churnBreakdown = useMemo(() => {
    if (data?.churn_breakdown) return data.churn_breakdown;

    const totalUsers = Math.max(Number(data?.kpis.total_expiring_users ?? 0), 0);
    const churnRate = clamp(Number(data?.kpis.historical_churn_rate ?? 0), 0, 100);
    const churnedUsers = Math.min(totalUsers, Math.round((totalUsers * churnRate) / 100));
    return {
      renewed_users: totalUsers - churnedUsers,
      churned_users: churnedUsers,
      renewed_rate: totalUsers > 0 ? 100 - churnRate : 0,
      churned_rate: churnRate,
    };
  }, [data?.churn_breakdown, data?.kpis.historical_churn_rate, data?.kpis.total_expiring_users]);
  const churnDonutData = useMemo(
    () =>
      [
        { name: "Gia hạn / còn ở lại", value: churnBreakdown.renewed_users },
        { name: "Rời bỏ", value: churnBreakdown.churned_users },
      ].filter((row) => row.value > 0),
    [churnBreakdown.churned_users, churnBreakdown.renewed_users],
  );
  const churnBreakdownMonthLabel = data?.meta.churn_breakdown_month
    ? formatMonthLabel(data.meta.churn_breakdown_month)
    : formatMonthLabel(data?.meta.month ?? selectedMonth);
  const riskHeatmapMonthLabel = data?.meta.risk_heatmap_month
    ? formatMonthLabel(data.meta.risk_heatmap_month)
    : formatMonthLabel(data?.meta.month ?? selectedMonth);
  const usesPriorChurnBreakdownArtifact =
    !!data?.meta.churn_breakdown_month && data.meta.churn_breakdown_month !== data.meta.month;
  const usesPriorRiskHeatmapArtifact =
    !!data?.meta.risk_heatmap_month && data.meta.risk_heatmap_month !== data.meta.month;
  const churnBandCeiling = useMemo(
    () => Math.max(CHURN_CRITICAL_PCT + 0.8, ...trendSeries.map((point) => Number(point.historical_churn_rate ?? 0) + 0.3)),
    [trendSeries],
  );
  const revenueYAxisDomain = useMemo(
    () =>
      computeDynamicDomain(
        trendSeries.flatMap((point) => [point.total_expected_renewal_amount, point.historical_revenue_at_risk]),
        {
          paddingRatio: 0.12,
          minSpan: 20_000,
          clampMin: 0,
        },
      ),
    [trendSeries],
  );
  const apruYAxisDomain = useMemo(
    () =>
      computeDynamicDomain(
        trendSeries.map((point) => point.apru),
        {
          paddingRatio: 0.16,
          minSpan: 20,
          clampMin: 0,
        },
      ),
    [trendSeries],
  );
  const churnYAxisDomain = useMemo(() => {
    const [lower, upper] = computeDynamicDomain(
      trendSeries.map((point) => Number(point.historical_churn_rate ?? 0)),
      {
        paddingRatio: 0.18,
        minSpan: 1.2,
        clampMin: 0,
        anchorMax:
          Math.max(...trendSeries.map((point) => Number(point.historical_churn_rate ?? 0)), 0) < CHURN_WARNING_PCT
            ? CHURN_WARNING_PCT + 0.3
            : churnBandCeiling,
        step: 0.2,
      },
    );
    return {
      lower,
      upper,
      greenUpper: Math.min(CHURN_WARNING_PCT, upper),
      amberLower: Math.max(lower, CHURN_WARNING_PCT),
      amberUpper: Math.min(CHURN_CRITICAL_PCT, upper),
      redLower: Math.max(lower, CHURN_CRITICAL_PCT),
    };
  }, [churnBandCeiling, trendSeries]);
  const riskHeatmapRows = useMemo(() => {
    const rows = data?.risk_heatmap ?? [];
    const maxUsers = Math.max(1, ...rows.map((row) => Number(row.users ?? 0)));
    return rows.map((row) => ({
      ...row,
      opacity: 0.18 + (Number(row.users ?? 0) / maxUsers) * 0.82,
    }));
  }, [data?.risk_heatmap]);

  if (loading) {
    return <StatePanel title="Đang tải dữ liệu hiện trạng" description="Đang lấy đường xu hướng giữ chân, cơ cấu nhóm khách, nhịp dữ liệu theo ngày và tín hiệu hành vi cho tháng đang xem." variant="loading" />;
  }

  if (!data) {
    return (
      <StatePanel
        title="Không có dữ liệu hiện trạng"
        description={
          error
            ? `Không đọc được dữ liệu hiện trạng cho tháng ${selectedMonth}. Chi tiết: ${error}`
            : `Chưa có dữ liệu hiện trạng cho tháng ${selectedMonth}.`
        }
        variant={error ? "error" : "empty"}
      />
    );
  }

  return (
    <div className="space-y-5">
      {hasMultiMonthTrend ? (
        <>
          <div className="grid gap-5 xl:grid-cols-2 2xl:grid-cols-3">
            <ChartCard
              title="Expiring Subscriber Trend"
              subtitle={`Bám theo chart quy mô khách sắp hết hạn trong file mẫu. ${trendScopeNote}`}
              className="min-h-[340px]"
            >
              <ResponsiveContainer width="100%" height={292}>
                <BarChart data={trendSeries} margin={{ top: 12, right: 8, left: 8, bottom: 8 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                  <XAxis dataKey="month_label" tick={{ fontSize: 12 }} tickFormatter={(value) => formatMonthLabel(String(value))} />
                  <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => formatNumber(Number(value))} />
                  <Tooltip
                    labelFormatter={(value) => formatMonthLabel(String(value))}
                    formatter={(value: number) => [formatNumber(Number(value)), "Khách sắp hết hạn"]}
                  />
                  <Bar dataKey="total_expiring_users" name="Khách sắp hết hạn" fill="#2563eb" radius={[10, 10, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard
              title="Doanh thu sắp gia hạn và phần đang bị đe dọa"
              subtitle="Vùng xanh là doanh thu sắp gia hạn. Đường đỏ cho thấy phần doanh thu lịch sử dễ mất nếu xu hướng rời bỏ lặp lại."
              action={
                latestRevenueRiskPoint?.historical_revenue_at_risk != null ? (
                  <div className="rounded-full bg-rose-50 px-3 py-1.5 text-right">
                    <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-rose-600">At risk</div>
                    <div className="text-sm font-semibold text-rose-700">
                      {formatCompactCurrency(Number(latestRevenueRiskPoint.historical_revenue_at_risk))}
                    </div>
                  </div>
                ) : null
              }
              className="min-h-[340px]"
            >
              {hasRevenueTrend ? (
                <ResponsiveContainer width="100%" height={292}>
                  <ComposedChart data={trendSeries} margin={{ top: 12, right: 8, left: 8, bottom: 8 }}>
                    <defs>
                      <linearGradient id="descriptiveRevenueTrend" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#2563eb" stopOpacity={0.28} />
                        <stop offset="95%" stopColor="#2563eb" stopOpacity={0.02} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                    <XAxis dataKey="month_label" tick={{ fontSize: 12 }} tickFormatter={(value) => formatMonthLabel(String(value))} />
                    <YAxis domain={revenueYAxisDomain} tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompactCurrency(Number(value))} />
                    <Tooltip
                      labelFormatter={(value) => formatMonthLabel(String(value))}
                      formatter={(value: number, name: string) => {
                        if (name === "historical_revenue_at_risk") {
                          return [formatCurrency(Number(value)), "Doanh thu bị đe dọa"];
                        }
                        return [formatCurrency(Number(value)), "Doanh thu sắp gia hạn"];
                      }}
                    />
                    <Area
                      type="monotone"
                      dataKey="total_expected_renewal_amount"
                      name="Doanh thu sắp gia hạn"
                      stroke="#2563eb"
                      fill="url(#descriptiveRevenueTrend)"
                      strokeWidth={2.5}
                    />
                    <Line
                      type="monotone"
                      dataKey="historical_revenue_at_risk"
                      name="Doanh thu bị đe dọa"
                      stroke="#dc2626"
                      strokeWidth={2.5}
                      dot={{ r: 4, fill: "#dc2626" }}
                    />
                  </ComposedChart>
                </ResponsiveContainer>
              ) : (
                <StatePanel title="Chưa có revenue trend" description="Nguồn hiện tại chưa trả về chuỗi doanh thu kỳ vọng theo tháng." />
              )}
            </ChartCard>

            <ChartCard
              title="APRU Trend"
              subtitle="APRU theo tháng của nhóm khách sắp hết hạn, giữ cùng ý đồ đọc nhanh như mẫu HTML."
              className="min-h-[340px]"
            >
              {hasApruTrend ? (
                <ResponsiveContainer width="100%" height={292}>
                  <LineChart data={trendSeries} margin={{ top: 12, right: 8, left: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                    <XAxis dataKey="month_label" tick={{ fontSize: 12 }} tickFormatter={(value) => formatMonthLabel(String(value))} />
                    <YAxis domain={apruYAxisDomain} tick={{ fontSize: 12 }} />
                    <Tooltip
                      labelFormatter={(value) => formatMonthLabel(String(value))}
                      formatter={(value: number) => [formatCurrency(Number(value)), "APRU"]}
                    />
                    <Line type="monotone" dataKey="apru" name="APRU" stroke="#0f172a" strokeWidth={2.5} dot={{ r: 4, fill: "#0f172a" }} />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <StatePanel title="Chưa có APRU trend" description="Nguồn hiện tại chưa trả về APRU nhiều tháng cho Tab 1." />
              )}
            </ChartCard>
          </div>

          <div className="grid gap-5 xl:grid-cols-[minmax(0,1.22fr)_minmax(0,0.78fr)] 2xl:grid-cols-[minmax(0,1.28fr)_minmax(0,0.72fr)]">
            <ChartCard
              title="Churn Rate Trend with Risk Band"
              subtitle="Bám theo chart churn band của file mẫu: xanh là ổn, vàng là cần chú ý, đỏ là vượt ngưỡng."
              className="min-h-[340px]"
            >
              <ResponsiveContainer width="100%" height={292}>
                <LineChart data={trendSeries} margin={{ top: 12, right: 8, left: 8, bottom: 8 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                  {churnYAxisDomain.lower < churnYAxisDomain.greenUpper ? (
                    <ReferenceArea y1={churnYAxisDomain.lower} y2={churnYAxisDomain.greenUpper} fill="rgba(16,185,129,0.08)" />
                  ) : null}
                  {churnYAxisDomain.amberLower < churnYAxisDomain.amberUpper ? (
                    <ReferenceArea y1={churnYAxisDomain.amberLower} y2={churnYAxisDomain.amberUpper} fill="rgba(245,158,11,0.15)" />
                  ) : null}
                  {churnYAxisDomain.redLower < churnYAxisDomain.upper ? (
                    <ReferenceArea y1={churnYAxisDomain.redLower} y2={churnYAxisDomain.upper} fill="rgba(239,68,68,0.12)" />
                  ) : null}
                  <XAxis dataKey="month_label" tick={{ fontSize: 12 }} tickFormatter={(value) => formatMonthLabel(String(value))} />
                  <YAxis domain={[churnYAxisDomain.lower, churnYAxisDomain.upper]} tick={{ fontSize: 12 }} tickFormatter={(value) => `${Number(value).toFixed(1)}%`} />
                  <Tooltip
                    labelFormatter={(value) => formatMonthLabel(String(value))}
                    formatter={(value: number) => [formatPct(Number(value), 1), "Tỷ lệ rời bỏ"]}
                  />
                  <Line
                    type="monotone"
                    dataKey="historical_churn_rate"
                    name="Tỷ lệ rời bỏ"
                    stroke="#0f172a"
                    strokeWidth={2.5}
                    dot={{ r: 5, fill: "#0f172a" }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard
              title="New vs Churned Users"
              subtitle="Dùng chuỗi khách mới và khách rời bỏ Jan–Mar từ feature store để bám logic trend của notebook."
              className="min-h-[340px]"
            >
              {hasNewVsChurnTrend ? (
                <ResponsiveContainer width="100%" height={292}>
                  <ComposedChart data={trendSeries} margin={{ top: 12, right: 8, left: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                    <XAxis dataKey="month_label" tick={{ fontSize: 12 }} tickFormatter={(value) => formatMonthLabel(String(value))} />
                    <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => formatNumber(Number(value))} />
                    <ReferenceLine y={0} stroke="rgba(148,163,184,0.7)" />
                    <Tooltip
                      labelFormatter={(value) => formatMonthLabel(String(value))}
                      formatter={(value: number, name: string) => {
                        if (name === "new_paid_users") return [formatNumber(Number(value)), "Khách mới"];
                        if (name === "churned_users") return [formatNumber(Number(value)), "Khách rời bỏ"];
                        return [formatNumber(Number(value)), "Net movement"];
                      }}
                    />
                    <Legend />
                    <Bar dataKey="new_paid_users" name="Khách mới" fill="#2563eb" radius={[8, 8, 0, 0]} />
                    <Bar dataKey="churned_users" name="Khách rời bỏ" fill="#ef4444" radius={[8, 8, 0, 0]} />
                    <Line type="monotone" dataKey="net_movement" name="Net movement" stroke="#0f172a" strokeWidth={2.5} dot={{ r: 4 }} />
                  </ComposedChart>
                </ResponsiveContainer>
              ) : (
                <StatePanel title="Chưa có new vs churned trend" description="Nguồn hiện tại chưa có chuỗi khách mới theo tháng để dựng đúng chart này." />
              )}
            </ChartCard>
          </div>
        </>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-[320px_minmax(0,1fr)] 2xl:grid-cols-[360px_minmax(0,1fr)]">
        <ChartCard
          title="Churn Rate Donut"
          subtitle={
            selectedFilterLabel
              ? usesPriorChurnBreakdownArtifact
                ? `${selectedFilterLabel} • donut đang bám artifact ${churnBreakdownMonthLabel}.`
                : `Snapshot hiện tại đang bám theo ${selectedFilterLabel}.`
              : usesPriorChurnBreakdownArtifact
                ? `Cơ cấu giữ lại và rời bỏ đang bám artifact ${churnBreakdownMonthLabel}.`
                : "Cơ cấu giữ lại và rời bỏ của tháng đang xem."
          }
          className="min-h-[300px]"
        >
          {churnDonutData.length ? (
            <div className="relative h-[292px]">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie data={churnDonutData} dataKey="value" nameKey="name" innerRadius={64} outerRadius={92} paddingAngle={3}>
                    {churnDonutData.map((row, index) => (
                      <Cell key={row.name} fill={CHURN_DONUT_COLORS[index % CHURN_DONUT_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip formatter={(value: number) => [formatNumber(Number(value)), "Số khách"]} />
                  <Legend verticalAlign="bottom" height={42} />
                </PieChart>
              </ResponsiveContainer>

              <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">Tỷ lệ rời bỏ</p>
                <p className="mt-2 font-display text-[1.7rem] font-semibold tracking-[-0.05em] text-slate-950">
                  {formatPct(churnBreakdown.churned_rate, 1)}
                </p>
                <p className="mt-2 text-center text-xs leading-5 text-slate-500">
                  {formatNumber(churnBreakdown.churned_users)} rời bỏ • {formatNumber(churnBreakdown.renewed_users)} còn ở lại
                </p>
              </div>
            </div>
          ) : (
            <StatePanel title="Chưa có churn donut" description="Tháng hiện tại chưa đủ dữ liệu để chia cơ cấu giữ lại và rời bỏ." />
          )}
        </ChartCard>

        <ChartCard
          title="Value Tier × Risk Customer Segment"
          subtitle={
            usesPriorRiskHeatmapArtifact
              ? `Phân khúc khách hàng và khả năng rời bỏ đang bám artifact ${riskHeatmapMonthLabel}.`
              : "Phân khúc khách hàng và khả năng rời bỏ"
          }
          className="min-h-[340px]"
        >
          {riskHeatmapRows.length ? (
            <div className="grid gap-3">
              <div className="grid grid-cols-[144px_repeat(3,minmax(0,1fr))] gap-3">
                <div />
                {["At Risk", "Watchlist", "Stable"].map((segment) => (
                  <div key={segment} className="text-center text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                    {segment}
                  </div>
                ))}
                {["Free Trial", "Deal Hunter", "Standard"].map((tier) => (
                  <Fragment key={tier}>
                    <div className="flex items-center text-sm font-medium text-slate-700">{tier}</div>
                    {["At Risk", "Watchlist", "Stable"].map((segment) => {
                      const cell = riskHeatmapRows.find((row) => row.value_tier === tier && row.risk_segment === segment);
                      const users = Number(cell?.users ?? 0);
                      return (
                        <div
                          key={`${tier}-${segment}`}
                          className="flex min-h-[88px] items-center justify-center rounded-[20px] border border-slate-200 bg-blue-600/10 px-3 text-center"
                          style={{ backgroundColor: `rgba(37,99,235,${cell?.opacity ?? 0.08})` }}
                        >
                          <div>
                            <div className="text-lg font-semibold text-slate-950">{formatNumber(users)}</div>
                            <div className="mt-1 text-[11px] uppercase tracking-[0.16em] text-slate-600">khách</div>
                          </div>
                        </div>
                      );
                    })}
                  </Fragment>
                ))}
              </div>
            </div>
          ) : (
            <StatePanel title="Chưa có risk heatmap" description="Snapshot hiện tại chưa đủ dữ liệu để chia theo value tier và risk segment." />
          )}
        </ChartCard>
      </div>

      <div className="grid gap-5 xl:grid-cols-[minmax(0,0.72fr)_minmax(0,1.28fr)] 2xl:grid-cols-[minmax(0,0.68fr)_minmax(0,1.32fr)]">
        <ChartCard
          title="Nhóm khách cần theo dõi ngay"
          subtitle="Giữ mỗi nhóm một vài đại diện nổi bật để người xem bấm lọc và hiểu ngay phạm vi vấn đề."
          className="min-h-[300px]"
        >
          <div className="space-y-4">
            {(Object.keys(segmentGroups) as Array<keyof typeof SEGMENT_LABELS>).map((segmentType) => {
              const rows = segmentGroups[segmentType];
              const visibleRows = rows.slice(0, 2);

              return (
                <div key={segmentType}>
                  <div className="mb-2 flex items-center justify-between gap-3">
                    <p className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">{SEGMENT_LABELS[segmentType]}</p>
                    {rows.length > visibleRows.length ? (
                      <span className="text-xs text-muted-foreground">+{rows.length - visibleRows.length} nhóm phụ</span>
                    ) : null}
                  </div>
                  <div className="space-y-2">
                    {visibleRows.length === 0 ? (
                      <p className="rounded-2xl bg-slate-50 px-4 py-3 text-sm text-muted-foreground">Chưa có dữ liệu</p>
                    ) : null}
                    {visibleRows.map((row) => {
                      const selected =
                        segmentFilter.segmentType === row.segment_type && segmentFilter.segmentValue === row.segment_value;
                      const retain = clamp(Number(row.retain_rate_pct) || 0, 0, 100);
                      const churn = clamp(Number(row.churn_rate_pct) || 0, 0, 100);

                      return (
                        <button
                          key={`${row.segment_type}-${row.segment_value}`}
                          type="button"
                          onClick={() => onToggleSegmentFilter(row.segment_type, row.segment_value)}
                          className={`w-full rounded-[20px] border px-4 py-3 text-left transition ${
                            selected ? "border-slate-950 bg-slate-950 text-white" : "border-slate-200 bg-slate-50 hover:border-slate-300"
                          }`}
                        >
                          <div className="flex items-center justify-between gap-3">
                            <strong className="font-medium">{row.segment_value}</strong>
                            <span className={`text-xs ${selected ? "text-white/70" : "text-muted-foreground"}`}>{formatNumber(row.users)} khách</span>
                          </div>
                          <div className="mt-3 flex h-2 overflow-hidden rounded-full bg-white/50">
                            <div className="h-full bg-emerald-500" style={{ width: `${retain}%` }} />
                            <div className="h-full bg-rose-500" style={{ width: `${churn}%` }} />
                          </div>
                          <div className={`mt-2 flex items-center justify-between text-xs ${selected ? "text-white/70" : "text-muted-foreground"}`}>
                            <span>Giữ lại {retain.toFixed(1)}%</span>
                            <span>Rời bỏ {churn.toFixed(1)}%</span>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        </ChartCard>

        <ChartCard
          title="Top cụm hành vi đang đe dọa doanh thu"
          subtitle=""
          action={<span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">Top {behaviorFocusPoints.length || 0} cụm</span>}
          className="min-h-[360px]"
        >
          {behaviorScatterPoints.length ? (
            <ResponsiveContainer width="100%" height={324}>
              <ScatterChart margin={{ top: 16, right: 12, bottom: 12, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis
                  type="number"
                  dataKey="discovery_pct"
                  name="Tỷ lệ khám phá"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => formatPct(Number(value), 0)}
                  domain={[0, 100]}
                />
                <YAxis
                  type="number"
                  dataKey="skip_pct"
                  name="Tỷ lệ bỏ qua"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => formatPct(Number(value), 0)}
                  domain={[0, 100]}
                />
                <ZAxis dataKey="revenue_at_risk" range={[160, 980]} />
                <Tooltip
                  formatter={(value: number, name) => {
                    if (name === "churn_rate_pct") return [formatPct(Number(value), 1), "Tỷ lệ rời bỏ"];
                    if (name === "revenue_at_risk") return [formatCurrency(Number(value)), "Doanh thu bị đe dọa"];
                    if (name === "users") return [formatNumber(Number(value)), "Số khách"];
                    if (name === "discovery_pct") return [formatPct(Number(value), 0), "Mức khám phá"];
                    if (name === "skip_pct") return [formatPct(Number(value), 0), "Mức bỏ qua"];
                    return Number(value).toFixed(2);
                  }}
                  labelFormatter={(_, payload) => String(payload?.[0]?.payload?.cluster_label ?? "Cụm hành vi")}
                />
                <Scatter data={behaviorScatterPoints}>
                  {behaviorScatterPoints.map((point, index) => (
                    <Cell key={`scatter-${index}`} fill={BEHAVIOR_CLUSTER_COLORS[index % BEHAVIOR_CLUSTER_COLORS.length]} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có ma trận hành vi" description="Tháng hiện tại chưa có đủ dữ liệu hành vi để xác định nhóm khách đang chán dịch vụ." />
          )}
        </ChartCard>
      </div>

      <div className={hidePulsePanel ? "grid gap-4 md:grid-cols-3" : "grid gap-5 xl:grid-cols-[minmax(0,1.2fr)_320px] 2xl:grid-cols-[minmax(0,1.15fr)_360px]"}>
        {!hidePulsePanel ? <DescriptivePulsePanel onReplayFrameChange={onReplayFrameChange} snapshot={snapshot} /> : null}

        <div className={hidePulsePanel ? "grid gap-4 md:grid-cols-3" : "grid gap-4 self-start"}>
          <InsightCard
            type="warning"
            title="Điều gì đang nổi lên?"
            description={
              hottestSegment ? (
                <>
                  Nhóm <strong>{hottestSegment.segment_value}</strong> đang có tỷ lệ rời bỏ cao nhất ở mức{" "}
                  <strong>{formatPct(Number(hottestSegment.churn_rate_pct), 1)}</strong>.
                </>
              ) : (
                "Chưa đủ dữ liệu để xác định nhóm khách đang đáng lo nhất."
              )
            }
          />
          <InsightCard
            type="insight"
            title="Vì sao cần chú ý?"
            description={
              biggestSegment ? (
                <>
                  Nhóm <strong>{biggestSegment.segment_value}</strong> hiện đông nhất với{" "}
                  <strong>{formatNumber(biggestSegment.users)}</strong> khách, nên bất kỳ biến động nhỏ nào cũng lan rất nhanh.
                </>
              ) : (
                "Chưa đủ dữ liệu để đánh giá mức độ lan rộng của vấn đề."
              )
            }
          />
          <InsightCard
            type="action"
            title="Nên làm gì trước?"
            description={
              riskiestBehavior ? (
                <>
                  Ưu tiên cụm <strong>{riskiestBehavior.cluster_label ?? "hành vi xấu nhất"}</strong> vì đang đe dọa khoảng{" "}
                  <strong>{formatCurrency(Number(riskiestBehavior.revenue_at_risk ?? 0))}</strong> doanh thu.
                </>
              ) : (
                "Chưa có tín hiệu hành vi đủ mạnh để đề xuất hành động ưu tiên."
              )
            }
          />
        </div>
      </div>
    </div>
  );
}

export default memo(DescriptiveTab);
