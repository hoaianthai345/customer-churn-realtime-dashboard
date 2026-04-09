import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Funnel,
  FunnelChart,
  Legend,
  Line,
  Pie,
  PieChart,
  ReferenceLine,
  ResponsiveContainer,
  Sankey,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import ChartCard from "@/components/dashboard/ChartCard";
import InsightCard from "@/components/dashboard/InsightCard";
import StatePanel from "@/components/dashboard/StatePanel";
import { Button } from "@/components/ui/button";
import {
  DEMO_MODE,
  formatCompactCurrency,
  formatCurrency,
  formatMonthLabel,
  formatNumber,
  formatPct,
  type PredictiveExecutiveMatrixBubble,
  type ModelParamState,
  type PredictivePayload,
} from "@/lib/dashboard";

type PredictiveTabProps = {
  data: PredictivePayload | null;
  loading: boolean;
  error: string | null;
  selectedMonth: string;
  currentFilterLabel: string;
  modelParams: ModelParamState;
  onModelParamChange: (key: keyof ModelParamState, value: number) => void;
};

type WaterfallChartPoint = {
  name: string;
  start: number;
  amount: number;
  total: number;
  contributionPct: number;
  featureCount: number;
  isTotal: boolean;
};

type MatrixTooltipProps = {
  active?: boolean;
  payload?: Array<{ payload: PredictiveExecutiveMatrixBubble }>;
};

type WaterfallTooltipProps = {
  active?: boolean;
  payload?: Array<{ payload: WaterfallChartPoint }>;
};

type RevenueFlowTooltipProps = {
  active?: boolean;
  payload?: Array<{ payload?: any; value?: number; name?: string }>;
};

type RevenueFlowNodeProps = {
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  payload?: {
    name?: string;
    color?: string | null;
    value?: number;
    depth?: number;
  };
};

type RevenueFlowLinkProps = {
  sourceX?: number;
  sourceY?: number;
  sourceControlX?: number;
  targetX?: number;
  targetY?: number;
  targetControlX?: number;
  linkWidth?: number;
  payload?: {
    value?: number;
    color?: string | null;
    risk_tier?: string | null;
    source?: { name?: string; depth?: number };
    target?: { name?: string; depth?: number };
  };
};

const RISK_BAND_COLORS: Record<string, string> = {
  Low: "#00CC96",
  Medium: "#FECB52",
  High: "#EF553B",
  Unknown: "#94a3b8",
};
const MATRIX_RISK_BAND_COLORS: Record<string, string> = {
  "Very Low": "#27ae60",
  Low: "#00CC96",
  Medium: "#FECB52",
  High: "#EF553B",
  "Very High": "#d35400",
  Unknown: "#94a3b8",
};
const RISK_BAND_LABELS: Record<string, string> = {
  Low: "Low",
  Medium: "Medium",
  High: "High",
  Unknown: "Unknown",
};

const WATERFALL_COLORS = ["#334155", "#475569", "#64748b", "#94a3b8", "#0f766e", "#f59e0b"];
const FUNNEL_COLORS = ["#10b981", "#f59e0b", "#ef4444"];
const OUTLOOK_BAR_COLORS = ["#0f766e", "#2563eb", "#e11d48"];
const SANKEY_STAGE_HEADERS = ["Thanh toán", "RFM", "Gói giá", "Rủi ro"];
const SANKEY_STAGE_LEGEND = [
  { label: "Node thanh toán", color: "#1f2937" },
  { label: "Node RFM", color: "#94a3b8" },
  { label: "Node gói giá", color: "#38bdf8" },
  { label: "Node rủi ro", color: "#ef4444" },
];
const SANKEY_RISK_LEGEND = [
  { label: "Luồng High", color: "#ef4444" },
  { label: "Luồng Medium", color: "#f59e0b" },
  { label: "Luồng Low", color: "#10b981" },
];

export default function PredictiveTab({
  data,
  loading,
  error,
  selectedMonth,
  currentFilterLabel,
  modelParams,
  onModelParamChange,
}: PredictiveTabProps) {
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const initialLoading = loading && !data;
  const switchingDataset = loading && !!data;
  const revenueLossOutlook = useMemo(() => data?.revenue_loss_outlook ?? [], [data?.revenue_loss_outlook]);
  const executiveMatrix = useMemo(() => data?.executive_value_risk_matrix ?? [], [data?.executive_value_risk_matrix]);
  const riskBandMix = useMemo(
    () => (data?.risk_band_mix ?? []).map((row) => ({ ...row, band_label: RISK_BAND_LABELS[row.band] ?? row.band })),
    [data?.risk_band_mix],
  );
  const matrixMidpoints = useMemo(() => ({ risk: 0.5, value: 100 }), []);
  const matrixValueCeiling = useMemo(
    () => Math.max(140, ...executiveMatrix.map((row) => Number(row.expected_renewal_amount ?? 0) + 12)),
    [executiveMatrix],
  );
  const waterfallData = useMemo(() => buildWaterfallSeries(data?.feature_group_waterfall ?? []), [data?.feature_group_waterfall]);
  const topPrescription = data?.prescriptions?.[0] ?? null;
  const priorityRows = useMemo(() => (data?.prescriptions ?? []).slice(0, 6), [data?.prescriptions]);
  const topRiskBand = useMemo(
    () => [...riskBandMix].sort((a, b) => Number(b.revenue_at_risk) - Number(a.revenue_at_risk))[0] ?? null,
    [riskBandMix],
  );
  const topLeakageDriver = useMemo(
    () => [...(data?.revenue_leakage ?? [])].sort((a, b) => Number(b.revenue_at_risk) - Number(a.revenue_at_risk))[0] ?? null,
    [data?.revenue_leakage],
  );
  const topHabitStage = useMemo(
    () => [...(data?.habit_funnel ?? [])].sort((a, b) => Number(b.revenue_at_risk) - Number(a.revenue_at_risk))[0] ?? null,
    [data?.habit_funnel],
  );
  const mustSaveSummary = useMemo(
    () =>
      (data?.value_risk_matrix ?? []).reduce(
        (acc, row) => {
          if (row.quadrant !== "Must Save") return acc;
          acc.user_count += Number(row.user_count ?? 0);
          acc.revenue_at_risk += Number(row.revenue_at_risk ?? 0);
          return acc;
        },
        { user_count: 0, revenue_at_risk: 0 },
      ),
    [data?.value_risk_matrix],
  );
  const contentTransitionClass = switchingDataset
    ? "transition-all duration-300 opacity-60"
    : "transition-all duration-300 opacity-100";

  const flashActionMessage = (message: string) => {
    setActionMessage(message);
    if (typeof window !== "undefined") {
      window.setTimeout(() => {
        setActionMessage((current) => (current === message ? null : current));
      }, 2400);
    }
  };

  const handleExportPriorityCsv = () => {
    if (typeof window === "undefined" || !priorityRows.length) return;
    const headers = ["Segment", "Quadrant", "Users", "Revenue at Risk", "CLTV", "Primary Risk Driver", "Recommended Action"];
    const rows = priorityRows.map((row) => [
      row.strategic_segment,
      row.quadrant,
      String(row.user_count),
      String(Math.round(row.revenue_at_risk)),
      String(Math.round(row.avg_future_cltv)),
      row.primary_risk_driver,
      row.recommended_action ?? "",
    ]);
    const csv = [headers, ...rows]
      .map((row) => row.map((cell) => `"${String(cell).replaceAll('"', '""')}"`).join(","))
      .join("\n");
    const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `kkbox-risk-priority-${data?.meta.month ?? selectedMonth}.csv`;
    link.click();
    window.URL.revokeObjectURL(url);
    flashActionMessage("Đã tải xuống danh sách ưu tiên.");
  };

  const handleCopyAlertSummary = async () => {
    if (!priorityRows.length || typeof navigator === "undefined" || !navigator.clipboard) return;
    const lines = [
      `KKBOX Risk Alert - ${formatMonthLabel(selectedMonth)}`,
      `Must Save revenue: ${formatCurrency(mustSaveSummary.revenue_at_risk)} trên ${formatNumber(mustSaveSummary.user_count)} khách.`,
      `Top segment: ${topPrescription?.strategic_segment ?? "N/A"} - ${formatCurrency(topPrescription?.revenue_at_risk ?? 0)} rủi ro.`,
      "",
      ...priorityRows.slice(0, 3).map(
        (row, index) =>
          `${index + 1}. ${row.strategic_segment}: ${formatCurrency(row.revenue_at_risk)} | Driver: ${row.primary_risk_driver} | Action: ${row.recommended_action ?? "Follow-up"}`
      ),
    ].join("\n");
    try {
      await navigator.clipboard.writeText(lines);
      flashActionMessage("Đã copy alert summary cho Email/Slack.");
    } catch {
      flashActionMessage("Không thể copy tự động. Hãy thử lại trong tab đang hoạt động.");
    }
  };

  if (initialLoading) {
    return (
      <StatePanel
        title="Đang tải lớp dự báo"
        description="Đang nạp bộ dự báo, ma trận ưu tiên, dòng doanh thu có nguy cơ mất và xu hướng giữ chân."
        variant="loading"
      />
    );
  }

  if (!data) {
    return (
      <StatePanel
        title="Không có dữ liệu dự báo"
        description={
          error
            ? `Không đọc được dữ liệu dự báo cho tháng ${selectedMonth}. Chi tiết: ${error}`
            : `Chưa có dữ liệu dự báo cho tháng ${selectedMonth}.`
        }
        variant={error ? "error" : "empty"}
      />
    );
  }

  return (
    <div className="space-y-6" aria-busy={switchingDataset}>
      <section className="rounded-[30px] border border-white/70 bg-[linear-gradient(135deg,rgba(15,23,42,0.96),rgba(30,41,59,0.92))] p-5 text-white shadow-[0_24px_60px_-34px_rgba(15,23,42,0.55)]">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
          <div className="max-w-3xl">
            <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-cyan-200/80">Bảng dự báo rủi ro</p>
            <h3 className="mt-2 font-display text-[1.7rem] font-semibold tracking-[-0.04em] text-white">
              Dự báo 30 ngày tới cho {formatMonthLabel(selectedMonth)}
            </h3>
            <p className="mt-3 text-sm leading-6 text-slate-200">
              {DEMO_MODE
                ? `Bảng này dùng bộ dự báo theo tháng cho ${formatNumber(data.meta.sample_user_count)} khách của ${formatMonthLabel(selectedMonth)}, giúp so sánh rủi ro và doanh thu cần giữ lại trên cùng một mặt bằng phân tích.`
                : `Đang dự báo trên phạm vi ${currentFilterLabel} với ${formatNumber(data.meta.sample_user_count)} khách đã được chấm điểm.`}
            </p>
          </div>

          <div className="grid gap-3 sm:grid-cols-3">
            <ExecutiveMetaCard
              label={DEMO_MODE ? "Phạm vi phân tích" : "Phạm vi đang xem"}
              value={DEMO_MODE ? "Toàn bộ khách của tháng" : currentFilterLabel}
              detail={DEMO_MODE ? "Đổi tháng để nạp bộ dự báo tương ứng" : undefined}
            />
            <ExecutiveMetaCard
              label="Nhóm đáng lo nhất"
              value={topPrescription?.strategic_segment ?? "Chưa có"}
              detail={topPrescription ? `${formatCurrency(topPrescription.revenue_at_risk)} doanh thu có nguy cơ mất` : undefined}
            />
            <ExecutiveMetaCard
              label="Nguồn dự báo"
              value={DEMO_MODE ? "Bộ dự báo theo tháng" : data.meta.artifact_mode ?? "Tính tại chỗ"}
              detail={DEMO_MODE ? "Cập nhật theo tháng đang chọn" : "Đồng bộ với cấu hình dự báo hiện tại"}
            />
          </div>
        </div>

        <div className="mt-5 flex flex-col gap-3 border-t border-white/10 pt-4 xl:flex-row xl:items-center xl:justify-between">
          <div className="text-sm text-slate-200">
            {actionMessage ?? "Có thể xuất danh sách ưu tiên cho CSKH hoặc copy nhanh summary để gửi Email/Slack."}
          </div>
          <div className="flex flex-wrap gap-2">
            <Button variant="secondary" className="rounded-full bg-white text-slate-900 hover:bg-slate-100" onClick={handleExportPriorityCsv} disabled={!priorityRows.length}>
              Export CSV
            </Button>
            <Button variant="outline" className="rounded-full border-white/30 bg-transparent text-white hover:bg-white/10" onClick={handleCopyAlertSummary} disabled={!priorityRows.length}>
              Copy Alert Summary
            </Button>
          </div>
        </div>
      </section>

      {switchingDataset ? (
        <div className="rounded-[22px] border border-cyan-200 bg-cyan-50/80 px-4 py-3 text-sm text-cyan-900">
          Giữ nguyên bố cục hiện tại trong lúc nạp bộ dự báo cho <strong>{formatMonthLabel(selectedMonth)}</strong>. Các biểu đồ sẽ cập nhật ngay khi dữ liệu mới sẵn sàng.
        </div>
      ) : null}

      <div className={`grid gap-5 xl:grid-cols-2 2xl:grid-cols-[320px_minmax(0,1.15fr)_minmax(0,0.95fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Phân bổ Dòng tiền Rủi ro"
          subtitle="Tóm gọn 3 tầng rủi ro đang giữ phần lớn doanh thu có nguy cơ mất."
          className="min-h-[340px]"
        >
          {riskBandMix.length ? (
            <div className="relative h-[286px]">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={riskBandMix}
                    dataKey="revenue_at_risk"
                    nameKey="band_label"
                    innerRadius={58}
                    outerRadius={92}
                    paddingAngle={3}
                  >
                    {riskBandMix.map((row) => (
                      <Cell key={row.band} fill={RISK_BAND_COLORS[row.band] ?? RISK_BAND_COLORS.Unknown} />
                    ))}
                  </Pie>
                  <Tooltip
                    formatter={(value: number) => formatCurrency(Number(value))}
                    contentStyle={{ borderRadius: 16, borderColor: "rgba(148,163,184,0.2)" }}
                  />
                  <Legend verticalAlign="bottom" height={32} />
                </PieChart>
              </ResponsiveContainer>

              <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
                <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">Tổng rủi ro</p>
                <p className="mt-1 font-display text-xl font-semibold tracking-[-0.05em] text-slate-950">
                  {formatCompactCurrency(data.kpis.predicted_revenue_at_risk)}
                </p>
                <p className="mt-1 text-xs text-slate-600">{topRiskBand?.band_label ?? "Unknown"} đang chiếm tỷ trọng lớn nhất</p>
              </div>
            </div>
          ) : (
            <StatePanel title="Chưa có cơ cấu theo mức rủi ro" description="Không có phân bổ doanh thu rủi ro theo từng mức nguy cơ." />
          )}
        </ChartCard>

        <ChartCard
          title="Ma trận Vị thế Khách hàng theo Giá trị và Rủi ro"
          subtitle="Nhìn nhanh nhóm nào vừa giá trị cao vừa rủi ro cao. Ô Must Save là phần doanh thu cần giữ bằng mọi giá trong quý này."
          action={
            <div className="rounded-full bg-rose-50 px-3 py-1.5 text-right">
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-rose-600">Must Save</div>
              <div className="text-sm font-semibold text-rose-700">
                {formatCompactCurrency(mustSaveSummary.revenue_at_risk)}
              </div>
            </div>
          }
          className="min-h-[360px]"
        >
          {executiveMatrix.length ? (
            <div className="relative h-[340px]">
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ top: 18, right: 18, bottom: 16, left: 10 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                  <XAxis dataKey="prob_bin" tick={{ fontSize: 12 }} domain={[0, 1]} tickFormatter={(value) => `${Math.round(Number(value) * 100)}%`} />
                  <YAxis
                    dataKey="expected_renewal_amount"
                    tick={{ fontSize: 12 }}
                    domain={[0, matrixValueCeiling]}
                    tickFormatter={(value) => formatCompactCurrency(Number(value))}
                  />
                  <ZAxis dataKey="display_size" range={[120, 1000]} />
                  <ReferenceLine x={matrixMidpoints.risk} stroke="rgba(100,116,139,0.65)" strokeDasharray="7 7" />
                  <ReferenceLine y={matrixMidpoints.value} stroke="rgba(100,116,139,0.65)" strokeDasharray="7 7" />
                  <Tooltip content={<MatrixTooltip />} cursor={{ strokeDasharray: "4 4" }} />
                  <Scatter data={executiveMatrix}>
                    {executiveMatrix.map((row, index) => (
                      <Cell
                        key={`${row.risk_band}-${row.prob_bin}-${row.expected_renewal_amount}-${index}`}
                        fill={MATRIX_RISK_BAND_COLORS[row.risk_band] ?? MATRIX_RISK_BAND_COLORS.Unknown}
                        stroke="rgba(15,23,42,0.12)"
                      />
                    ))}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>

              <div className="pointer-events-none absolute inset-0">
                <div className="absolute left-3 top-3 rounded-full bg-white/90 px-3 py-1 text-[11px] font-semibold tracking-[0.08em] text-emerald-700">
                  KHÁCH NÒNG CỐT
                </div>
                <div className="absolute right-3 top-3 rounded-full bg-white/90 px-3 py-1 text-[11px] font-semibold tracking-[0.08em] text-rose-700">
                  VIP NGUY CƠ
                </div>
                <div className="absolute bottom-3 left-3 rounded-full bg-white/90 px-3 py-1 text-[11px] font-semibold tracking-[0.08em] text-sky-700">
                  KHÁCH VÃNG LAI
                </div>
                <div className="absolute bottom-3 right-3 rounded-full bg-white/90 px-3 py-1 text-[11px] font-semibold tracking-[0.08em] text-amber-700">
                  NHÓM NHẠY CẢM GIÁ
                </div>
              </div>
            </div>
          ) : (
            <StatePanel title="Chưa có ma trận ưu tiên" description="Tháng hiện tại chưa có đủ nhóm khách để dựng ma trận giá trị và rủi ro." />
          )}
        </ChartCard>

        <ChartCard
          title="Phân rã Tác động Rủi ro theo Nhóm Đặc trưng"
          subtitle="Waterfall phân tách phần nền rủi ro và các nhóm đặc trưng đang giải thích phần còn lại."
          className="min-h-[340px]"
        >
          {waterfallData.length ? (
            <ResponsiveContainer width="100%" height={292}>
              <BarChart data={waterfallData} margin={{ top: 12, right: 12, left: 8, bottom: 60 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis dataKey="name" interval={0} angle={-18} textAnchor="end" height={66} tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompactCurrency(Number(value))} />
                <Tooltip content={<WaterfallTooltip />} />
                <Bar dataKey="start" stackId="risk" fill="transparent" />
                <Bar dataKey="amount" stackId="risk" radius={[10, 10, 0, 0]}>
                  {waterfallData.map((row, index) => (
                    <Cell
                      key={`${row.name}-${index}`}
                      fill={row.isTotal ? "#b91c1c" : WATERFALL_COLORS[index % WATERFALL_COLORS.length]}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có bảng nguyên nhân" description="Nguồn dữ liệu hiện tại chưa trả về mức đóng góp của từng nhóm yếu tố." />
          )}
        </ChartCard>
      </div>

      <div className={`grid gap-5 xl:grid-cols-2 2xl:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)_minmax(0,0.9fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Dòng chảy Thất thoát Doanh thu"
          subtitle="Theo dõi từ Vận hành -> Giá trị -> Gói cước -> Rủi ro"
          className="min-h-[380px] xl:col-span-2 2xl:col-span-1"
        >
          {data.revenue_flow_sankey.nodes.length && data.revenue_flow_sankey.links.length ? (
            <div className="space-y-3">
              <div className="grid grid-cols-4 gap-2 text-center text-[11px] font-semibold uppercase tracking-[0.2em] text-slate-500">
                {SANKEY_STAGE_HEADERS.map((label) => (
                  <span key={label}>{label}</span>
                ))}
              </div>

              <div className="rounded-[22px] border border-slate-200/80 bg-slate-50/80 px-2 py-2">
                <ResponsiveContainer width="100%" height={296}>
                  <Sankey
                    data={data.revenue_flow_sankey}
                    sort={false}
                    nodePadding={24}
                    nodeWidth={16}
                    linkCurvature={0.45}
                    node={<RevenueFlowNode />}
                    link={<RevenueFlowLink />}
                    margin={{ top: 8, right: 102, bottom: 8, left: 102 }}
                  >
                    <Tooltip content={<RevenueFlowTooltip />} />
                  </Sankey>
                </ResponsiveContainer>
              </div>

              <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-slate-600">
                {SANKEY_STAGE_LEGEND.map((item) => (
                  <LegendPill key={item.label} label={item.label} color={item.color} />
                ))}
                <span className="mx-1 hidden h-4 w-px bg-slate-200 sm:block" />
                {SANKEY_RISK_LEGEND.map((item) => (
                  <LegendPill key={item.label} label={item.label} color={item.color} />
                ))}
              </div>
            </div>
          ) : (
            <StatePanel title="Chưa có dòng chảy doanh thu" description="Không đủ dữ liệu để dựng luồng thất thoát doanh thu theo các tầng phân loại." />
          )}
        </ChartCard>

        <ChartCard
          title="Phân tích Tương quan giữa Phân đoạn Giá và Rủi ro Rời bỏ"
          subtitle="So sánh quy mô khách, dòng tiền rủi ro và xác suất rời bỏ giữa các nhóm giá."
          className="min-h-[340px]"
        >
          {data.price_paradox.length ? (
            <ResponsiveContainer width="100%" height={286}>
              <ComposedChart data={data.price_paradox} margin={{ top: 12, right: 18, bottom: 8, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis dataKey="price_bucket" tick={{ fontSize: 12 }} />
                <YAxis yAxisId="left" tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompactNumber(Number(value))} />
                <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12 }} tickFormatter={(value) => `${Number(value).toFixed(0)}%`} />
                <Tooltip
                  formatter={(value: number, name: string) => {
                    if (name === "Tỷ lệ rời bỏ") return formatPct(Number(value), 1);
                    if (name === "Doanh thu rủi ro") return formatCurrency(Number(value));
                    return formatNumber(Number(value));
                  }}
                />
                <Legend />
                <Bar yAxisId="left" dataKey="user_count" name="Số khách" fill="#bfdbfe" radius={[8, 8, 0, 0]} />
                <Bar yAxisId="left" dataKey="revenue_at_risk" name="Doanh thu rủi ro" fill="#334155" radius={[8, 8, 0, 0]} />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="churn_rate_pct"
                  name="Tỷ lệ rời bỏ"
                  stroke="#ef4444"
                  strokeWidth={2.5}
                  dot={{ r: 4 }}
                />
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có dữ liệu theo nhóm giá" description="Nguồn dữ liệu hiện tại chưa có đủ nhóm giá để dựng biểu đồ này." />
          )}
        </ChartCard>

        <ChartCard
          title="Phễu Phân hóa Mức độ Hoạt động của Khách hàng"
          subtitle="Theo dõi mức độ suy giảm tương tác trước khi khách rời bỏ."
          className="min-h-[340px] border-rose-200/70 bg-[linear-gradient(145deg,rgba(255,255,255,1),rgba(255,241,242,0.9))]"
        >
          {data.habit_funnel.length ? (
            <div className="space-y-3">
              <ResponsiveContainer width="100%" height={208}>
                <FunnelChart>
                  <Tooltip formatter={(value: number) => formatNumber(Number(value))} />
                  <Funnel data={data.habit_funnel} dataKey="user_count" nameKey="habit_stage" isAnimationActive>
                    {data.habit_funnel.map((row, index) => (
                      <Cell key={row.habit_stage} fill={FUNNEL_COLORS[index % FUNNEL_COLORS.length]} />
                    ))}
                  </Funnel>
                </FunnelChart>
              </ResponsiveContainer>

              {data.habit_funnel.slice(0, 3).map((row, index) => (
                <div key={row.habit_stage} className="rounded-[18px] border border-white/70 bg-white/80 px-4 py-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex items-center gap-2">
                      <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: FUNNEL_COLORS[index % FUNNEL_COLORS.length] }} />
                      <span className="text-sm font-medium text-slate-900">{row.habit_stage}</span>
                    </div>
                    <span className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                      {formatPct(row.share_of_top_pct, 0)}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <StatePanel title="Chưa có phễu hành vi" description="Không có đủ dữ liệu hành vi để dựng các mức suy giảm thói quen." />
          )}
        </ChartCard>
      </div>

      <div className={`grid gap-5 xl:grid-cols-[minmax(0,1.35fr)_320px] 2xl:grid-cols-[minmax(0,1.25fr)_360px] ${contentTransitionClass}`}>
        <ChartCard
          title="Doanh thu dự kiến mất đi trong 3, 6, 12 tháng tới"
          subtitle="Thay đường decay bằng ba mốc tiền tệ tích lũy để ban điều hành nhìn nhanh mức tổn thất nếu không can thiệp."
          className="min-h-[340px]"
        >
          {revenueLossOutlook.length ? (
            <ResponsiveContainer width="100%" height={286}>
              <BarChart data={revenueLossOutlook} margin={{ top: 12, right: 18, bottom: 8, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis dataKey="horizon_label" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompactCurrency(Number(value))} />
                <Tooltip
                  formatter={(value: number, name: string, item: { payload?: { projected_loss_share_pct?: number } }) => {
                    if (name === "projected_revenue_loss") {
                      return [formatCurrency(Number(value)), `Mất đi • ${(item.payload?.projected_loss_share_pct ?? 0).toFixed(1)}% base`];
                    }
                    return formatCurrency(Number(value));
                  }}
                />
                <Bar dataKey="projected_revenue_loss" name="Doanh thu mất đi" radius={[12, 12, 0, 0]}>
                  {revenueLossOutlook.map((row, index) => (
                    <Cell key={row.horizon_label} fill={OUTLOOK_BAR_COLORS[index % OUTLOOK_BAR_COLORS.length]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có outlook doanh thu" description="Tháng này chưa có bảng doanh thu dự kiến mất đi theo các mốc 3, 6 và 12 tháng." />
          )}
        </ChartCard>

        <div className="grid gap-4 self-start">
          <InsightCard
            type="warning"
            title="Nhóm cần giữ ngay"
            description={
              topPrescription ? (
                <>
                  <strong>{topPrescription.strategic_segment}</strong> đang kéo theo{" "}
                  <strong>{formatCurrency(topPrescription.revenue_at_risk)}</strong> doanh thu rủi ro.
                  {topPrescription.quadrant === "Must Save" ? " Đây là nhóm Must Save." : ""}
                </>
              ) : (
                "Chưa đủ dữ liệu để chỉ ra nhóm cần ưu tiên cứu trước."
              )
            }
          />
          <InsightCard
            type="insight"
            title="Tác nhân chính"
            description={
              topLeakageDriver ? (
                <>
                  <strong>{topLeakageDriver.risk_driver}</strong> đang dẫn đầu phần thất thoát với{" "}
                  <strong>{formatCurrency(topLeakageDriver.revenue_at_risk)}</strong>. Đây là nguyên nhân chính khiến nhóm khách có xác suất rời bỏ cao hơn mặt bằng.
                </>
              ) : (
                "Chưa đủ dữ liệu để xác định tác nhân kéo rủi ro lên mạnh nhất."
              )
            }
          />
          <InsightCard
            type="action"
            title="Hành động khuyến nghị"
            description={
              topPrescription ? (
                <>
                  <strong>{topPrescription.recommended_action ?? "Chưa có gợi ý"}</strong>
                  {topHabitStage ? ` Nhóm hành vi cần theo dõi sát nhất hiện là ${topHabitStage.habit_stage.toLowerCase()}.` : ""}
                </>
              ) : (
                "Chưa đủ dữ liệu để gợi ý đòn can thiệp sớm."
              )
            }
          />
        </div>
      </div>

      <ChartCard
        title="Bảng ưu tiên hành động cho đội giữ chân"
        subtitle="Danh sách này gom nhóm cần cứu trước, lý do chính và hành động nên giao ngay cho CSKH/Growth."
        action={
          <div className="rounded-full bg-slate-100 px-3 py-1.5 text-right">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Must Save</div>
            <div className="text-sm font-semibold text-slate-900">
              {formatCompactCurrency(mustSaveSummary.revenue_at_risk)}
            </div>
          </div>
        }
        className={contentTransitionClass}
      >
        {priorityRows.length ? (
          <div className="overflow-x-auto">
            <table className="min-w-full border-separate border-spacing-y-2">
              <thead>
                <tr className="text-left text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">
                  <th className="px-3 py-2">Nhóm khách</th>
                  <th className="px-3 py-2">Quadrant</th>
                  <th className="px-3 py-2">Doanh thu rủi ro</th>
                  <th className="px-3 py-2">
                    <span className="cursor-help underline decoration-dotted underline-offset-4" title="CLTV là giá trị doanh thu kỳ vọng còn giữ được từ nhóm khách này trong kỳ dự báo hiện tại.">
                      CLTV
                    </span>
                  </th>
                  <th className="px-3 py-2">
                    <span className="cursor-help underline decoration-dotted underline-offset-4" title="Primary Risk Driver là tín hiệu chính khiến nhóm này có xác suất rời bỏ cao hơn mặt bằng.">
                      Primary Risk Driver
                    </span>
                  </th>
                  <th className="px-3 py-2">Recommended Action</th>
                </tr>
              </thead>
              <tbody>
                {priorityRows.map((row) => (
                  <tr key={row.strategic_segment} className="rounded-[18px] bg-slate-50 text-sm text-slate-700">
                    <td className="rounded-l-[18px] px-3 py-3">
                      <div className="font-medium text-slate-950">{row.strategic_segment}</div>
                      <div className="mt-1 text-xs text-slate-500">{formatNumber(row.user_count)} khách</div>
                    </td>
                    <td className="px-3 py-3">
                      <span
                        className={`rounded-full px-2.5 py-1 text-xs font-semibold ${
                          row.quadrant === "Must Save"
                            ? "bg-rose-100 text-rose-700"
                            : row.quadrant === "Core Value"
                              ? "bg-emerald-100 text-emerald-700"
                              : "bg-amber-100 text-amber-700"
                        }`}
                      >
                        {row.quadrant}
                      </span>
                    </td>
                    <td className="px-3 py-3 font-medium text-slate-950">{formatCurrency(row.revenue_at_risk)}</td>
                    <td className="px-3 py-3">{formatCurrency(row.avg_future_cltv)}</td>
                    <td className="px-3 py-3">{row.primary_risk_driver}</td>
                    <td className="rounded-r-[18px] px-3 py-3">{row.recommended_action ?? "Chưa có gợi ý."}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <StatePanel title="Chưa có danh sách ưu tiên" description="Nguồn hiện tại chưa đủ dữ liệu để dựng bảng hành động cho đội giữ chân." />
        )}
      </ChartCard>

      {DEMO_MODE ? (
        <div className={`rounded-[22px] bg-slate-50 p-4 text-sm leading-6 text-slate-600 ${contentTransitionClass}`}>
          Tab này đang dùng bộ dự báo chuẩn hóa theo tháng để giữ mặt bằng so sánh nhất quán giữa các kỳ. Khi cần hiệu chỉnh trực tiếp tham số mô hình, có thể chuyển sang luồng phân tích chuyên sâu.
        </div>
      ) : (
        <details className={`rounded-[28px] border border-white/70 bg-white/88 p-5 shadow-[0_18px_40px_-30px_rgba(15,23,42,0.34)] backdrop-blur ${contentTransitionClass}`}>
          <summary className="cursor-pointer list-none text-sm font-semibold text-slate-900">
            Xem cấu hình mô hình
          </summary>
          <div className="mt-5 rounded-[22px] bg-slate-50 p-4">
            <p className="text-sm leading-6 text-slate-600">
              Các tham số bên dưới được giữ để tương thích với API hiện tại và cho phép tinh chỉnh khi chạy ở chế độ đầy đủ.
            </p>
            <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {(
                [
                  ["base_prob", "Xác suất gốc", 0.01],
                  ["weight_manual", "Hệ số gia hạn thủ công", 0.01],
                  ["weight_low_activity", "Hệ số ít hoạt động", 0.01],
                  ["weight_high_skip", "Hệ số bỏ qua nhiều", 0.01],
                  ["weight_low_discovery", "Hệ số khám phá thấp", 0.01],
                  ["weight_cancel_signal", "Hệ số tín hiệu hủy", 0.01],
                  ["prob_min", "Ngưỡng thấp nhất", 0.01],
                  ["prob_max", "Ngưỡng cao nhất", 0.01],
                ] as Array<[keyof ModelParamState, string, number]>
              ).map(([key, label, step]) => (
                <label key={key} className="space-y-2">
                  <span className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">{label}</span>
                  <input
                    type="number"
                    step={step}
                    value={modelParams[key]}
                    onChange={(event) => onModelParamChange(key, Number(event.target.value))}
                    className="h-11 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm outline-none transition focus:border-slate-400"
                  />
                </label>
              ))}
            </div>
          </div>
        </details>
      )}
    </div>
  );
}

function ExecutiveMetaCard({
  label,
  value,
  detail,
  compact = false,
}: {
  label: string;
  value: string;
  detail?: string;
  compact?: boolean;
}) {
  return (
    <div className="rounded-[22px] border border-white/10 bg-white/10 p-4 backdrop-blur">
      <p className="text-[11px] font-semibold uppercase tracking-[0.22em] text-slate-300">{label}</p>
      <p className={`mt-2 font-display font-semibold tracking-[-0.03em] text-white ${compact ? "text-sm break-all" : "text-base"}`}>{value}</p>
      {detail ? <p className="mt-2 text-xs leading-5 text-slate-300/90">{detail}</p> : null}
    </div>
  );
}

function MatrixTooltip({ active, payload }: MatrixTooltipProps) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;

  return (
    <div className="rounded-[18px] border border-slate-200 bg-white/95 p-4 shadow-lg">
      <p className="max-w-[260px] text-sm font-semibold text-slate-950">{row.risk_band}</p>
      <div className="mt-3 space-y-1.5 text-xs text-slate-600">
        <p>{formatPct(Number(row.prob_bin) * 100, 0)} xác suất rời bỏ</p>
        <p>{formatCurrency(Number(row.expected_renewal_amount))} mức chi tiêu</p>
        <p>{formatNumber(Number(row.user_count))} khách</p>
        <p>{formatCurrency(Number(row.revenue_at_risk))} doanh thu rủi ro</p>
      </div>
    </div>
  );
}

function WaterfallTooltip({ active, payload }: WaterfallTooltipProps) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;

  return (
    <div className="rounded-[18px] border border-slate-200 bg-white/95 p-4 shadow-lg">
      <p className="text-sm font-semibold text-slate-950">{row.name}</p>
      <div className="mt-3 space-y-1.5 text-xs text-slate-600">
        <p>{formatCurrency(Number(row.total))} cộng dồn</p>
        <p>{formatCurrency(Number(row.amount))} mức đóng góp</p>
        <p>{formatPct(Number(row.contributionPct), 1)} trong tổng rủi ro</p>
        {!row.isTotal ? <p>{formatNumber(Number(row.featureCount))} biến đang được mô hình dùng</p> : null}
      </div>
    </div>
  );
}

function buildWaterfallSeries(points: PredictivePayload["feature_group_waterfall"]): WaterfallChartPoint[] {
  let running = 0;
  const rows = points.map((point) => {
    const amount = Number(point.contribution) || 0;
    const row = {
      name: point.display_name,
      start: running,
      amount,
      total: running + amount,
      contributionPct: Number(point.contribution_pct) || 0,
      featureCount: Number(point.feature_count) || 0,
      isTotal: false,
    };
    running += amount;
    return row;
  });

  if (running > 0) {
    rows.push({
      name: "Tổng rủi ro",
      start: 0,
      amount: running,
      total: running,
      contributionPct: 100,
      featureCount: 0,
      isTotal: true,
    });
  }

  return rows;
}

function formatCompactNumber(value: number): string {
  return new Intl.NumberFormat("vi-VN", { notation: "compact", maximumFractionDigits: 1 }).format(Number(value) || 0);
}

function RevenueFlowTooltip({ active, payload }: RevenueFlowTooltipProps) {
  if (!active || !payload?.length) return null;
  const raw = payload[0]?.payload as any;
  const detail = raw?.payload;

  if (detail?.source && detail?.target) {
    return (
      <div className="rounded-[18px] border border-slate-200 bg-white/95 p-4 shadow-lg">
        <p className="text-sm font-semibold text-slate-950">
          {detail.source.name} {"->"} {detail.target.name}
        </p>
        <div className="mt-3 space-y-1.5 text-xs text-slate-600">
          <p>{formatCurrency(Number(detail.value))} doanh thu rủi ro</p>
          {detail.risk_tier ? <p>{detail.risk_tier}</p> : null}
        </div>
      </div>
    );
  }

  if (detail?.name) {
    return (
      <div className="rounded-[18px] border border-slate-200 bg-white/95 p-4 shadow-lg">
        <p className="text-sm font-semibold text-slate-950">{detail.name}</p>
        <div className="mt-3 space-y-1.5 text-xs text-slate-600">
          <p>{formatCurrency(Number(detail.value))} tổng doanh thu đang đi qua node này</p>
        </div>
      </div>
    );
  }

  return null;
}

function RevenueFlowNode({ x = 0, y = 0, width = 0, height = 0, payload }: RevenueFlowNodeProps) {
  const fill = payload?.color ?? "#94a3b8";
  const depth = Number(payload?.depth ?? 0);
  const anchor = depth === 0 ? "end" : "start";
  const labelX = anchor === "end" ? x - 10 : x + width + 10;
  const centerY = y + height / 2;
  const compactValue = Number(payload?.value ?? 0) > 0 ? formatCompactCurrency(Number(payload?.value ?? 0)) : null;
  const showValue = Boolean(compactValue) && height >= 24;

  return (
    <g>
      <rect x={x} y={y} width={width} height={height} rx={6} fill={fill} fillOpacity={0.94} stroke="rgba(255,255,255,0.95)" strokeWidth={1} />
      <text
        x={labelX}
        y={centerY - (showValue ? 5 : 0)}
        textAnchor={anchor}
        fontSize={11}
        fontWeight={700}
        fill="#0f172a"
        paintOrder="stroke"
        stroke="rgba(255,255,255,0.96)"
        strokeWidth={4}
      >
        {payload?.name}
      </text>
      {showValue ? (
        <text
          x={labelX}
          y={centerY + 9}
          textAnchor={anchor}
          fontSize={10}
          fontWeight={600}
          fill="#475569"
          paintOrder="stroke"
          stroke="rgba(255,255,255,0.96)"
          strokeWidth={4}
        >
          {compactValue}
        </text>
      ) : null}
    </g>
  );
}

function RevenueFlowLink({
  sourceX = 0,
  sourceY = 0,
  sourceControlX = 0,
  targetX = 0,
  targetY = 0,
  targetControlX = 0,
  linkWidth = 0,
  payload,
}: RevenueFlowLinkProps) {
  const stroke = payload?.color ?? "rgba(51,65,85,0.25)";
  const path = `M${sourceX},${sourceY} C${sourceControlX},${sourceY} ${targetControlX},${targetY} ${targetX},${targetY}`;
  const showValueLabel = Number(payload?.target?.depth ?? -1) >= 3 && Number(linkWidth) >= 10;
  const labelX = (sourceX + targetX) / 2;
  const labelY = (sourceY + targetY) / 2 - 4;

  return (
    <g>
      <path d={path} fill="none" stroke={stroke} strokeWidth={linkWidth} strokeLinecap="round" />
      {showValueLabel ? (
        <text
          x={labelX}
          y={labelY}
          textAnchor="middle"
          fontSize={10}
          fontWeight={700}
          fill="#0f172a"
          paintOrder="stroke"
          stroke="rgba(255,255,255,0.96)"
          strokeWidth={4}
        >
          {formatCompactCurrency(Number(payload?.value ?? 0))}
        </text>
      ) : null}
    </g>
  );
}

function LegendPill({ label, color }: { label: string; color: string }) {
  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white/80 px-3 py-1.5">
      <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: color }} />
      <span className="text-[11px] font-medium text-slate-700">{label}</span>
    </div>
  );
}
