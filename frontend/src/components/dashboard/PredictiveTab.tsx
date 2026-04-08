import { useMemo } from "react";
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
  LineChart,
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
import {
  DEMO_MODE,
  buildForecastDecayChartData,
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
const DECAY_LINE_COLORS = ["#0f766e", "#2563eb", "#f59e0b", "#e11d48"];

export default function PredictiveTab({
  data,
  loading,
  error,
  selectedMonth,
  currentFilterLabel,
  modelParams,
  onModelParamChange,
}: PredictiveTabProps) {
  const initialLoading = loading && !data;
  const switchingDataset = loading && !!data;
  const decayData = useMemo(() => buildForecastDecayChartData(data?.forecast_decay ?? []), [data?.forecast_decay]);
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
  const contentTransitionClass = switchingDataset
    ? "transition-all duration-300 opacity-60"
    : "transition-all duration-300 opacity-100";

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
      </section>

      {switchingDataset ? (
        <div className="rounded-[22px] border border-cyan-200 bg-cyan-50/80 px-4 py-3 text-sm text-cyan-900">
          Giữ nguyên bố cục hiện tại trong lúc nạp bộ dự báo cho <strong>{formatMonthLabel(selectedMonth)}</strong>. Các biểu đồ sẽ cập nhật ngay khi dữ liệu mới sẵn sàng.
        </div>
      ) : null}

      <div className={`grid gap-5 xl:grid-cols-[360px_minmax(0,1.15fr)_minmax(0,0.95fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Phân bổ Dòng tiền Rủi ro"
          subtitle="Tóm gọn 3 tầng rủi ro đang giữ phần lớn doanh thu có nguy cơ mất."
          className="min-h-[300px]"
        >
          {riskBandMix.length ? (
            <div className="relative h-[250px]">
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
          subtitle="Bubble matrix theo notebook: xác suất rời bỏ, mức chi tiêu và quy mô khách được gom lại trên cùng một mặt phẳng."
          className="min-h-[320px]"
        >
          {executiveMatrix.length ? (
            <div className="relative h-[310px]">
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
          className="min-h-[300px]"
        >
          {waterfallData.length ? (
            <ResponsiveContainer width="100%" height={260}>
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

      <div className={`grid gap-5 xl:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)_minmax(0,0.9fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Dòng chảy Thất thoát Doanh thu"
          subtitle="Theo dõi từ Vận hành -> Giá trị -> Gói cước -> Rủi ro"
          className="min-h-[300px]"
        >
          {data.revenue_flow_sankey.nodes.length && data.revenue_flow_sankey.links.length ? (
            <ResponsiveContainer width="100%" height={250}>
              <Sankey data={data.revenue_flow_sankey} nodePadding={28} linkCurvature={0.45} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
                <Tooltip formatter={(value: number) => formatCurrency(Number(value))} />
              </Sankey>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có dòng chảy doanh thu" description="Không đủ dữ liệu để dựng luồng thất thoát doanh thu theo các tầng phân loại." />
          )}
        </ChartCard>

        <ChartCard
          title="Phân tích Tương quan giữa Phân đoạn Giá và Rủi ro Rời bỏ"
          subtitle="So sánh quy mô khách, dòng tiền rủi ro và xác suất rời bỏ giữa các nhóm giá."
          className="min-h-[300px]"
        >
          {data.price_paradox.length ? (
            <ResponsiveContainer width="100%" height={250}>
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
          className="min-h-[300px] border-rose-200/70 bg-[linear-gradient(145deg,rgba(255,255,255,1),rgba(255,241,242,0.9))]"
        >
          {data.habit_funnel.length ? (
            <div className="space-y-3">
              <ResponsiveContainer width="100%" height={170}>
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

      <div className={`grid gap-5 xl:grid-cols-[minmax(0,1.25fr)_360px] ${contentTransitionClass}`}>
        <ChartCard
          title="Sau vài tháng, nhóm nào xấu đi nhanh?"
          subtitle="So sánh độ rơi giữ chân giữa các nhóm giá chính."
          className="min-h-[300px]"
        >
          {decayData.length ? (
            <ResponsiveContainer width="100%" height={250}>
              <LineChart data={decayData}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis dataKey="timeline" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} domain={[0, 100]} tickFormatter={(value) => `${value}%`} />
                <Tooltip formatter={(value: number, name: string) => [formatPct(Number(value), 1), shortenDecaySegmentLabel(String(name))]} />
                <Legend formatter={(value) => shortenDecaySegmentLabel(String(value))} />
                {Object.keys(decayData[0] ?? {})
                  .filter((key) => !["timeline", "month_num"].includes(key))
                  .slice(0, 4)
                  .map((segment, index) => (
                    <Line
                      key={segment}
                      type="monotone"
                      dataKey={segment}
                      stroke={DECAY_LINE_COLORS[index % DECAY_LINE_COLORS.length]}
                      strokeWidth={3}
                      dot={false}
                      activeDot={{ r: 4 }}
                    />
                  ))}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có đường xu hướng" description="Tháng này chưa có bảng dự báo suy giảm giữ chân theo thời gian." />
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
                  <strong>{formatCurrency(topLeakageDriver.revenue_at_risk)}</strong>.
                </>
              ) : (
                "Chưa đủ dữ liệu để xác định tác nhân kéo rủi ro lên mạnh nhất."
              )
            }
          />
          <InsightCard
            type="action"
            title="Tín hiệu hành vi"
            description={
              topHabitStage ? (
                <>
                  <strong>{topHabitStage.habit_stage}</strong> hiện có{" "}
                  <strong>{formatNumber(topHabitStage.user_count)}</strong> khách cần theo dõi sát.
                </>
              ) : (
                "Chưa đủ dữ liệu hành vi để gợi ý đòn can thiệp sớm."
              )
            }
          />
        </div>
      </div>

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

function shortenDecaySegmentLabel(segment: string): string {
  const labelMap: Record<string, string> = {
    "Free Trial / Zero Pay": "Dùng thử",
    "Deal Hunter < 4.5": "Deal",
    "Standard 4.5-6.5": "Standard",
    "Premium >= 6.5": "Premium",
  };
  return labelMap[segment] ?? segment;
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
