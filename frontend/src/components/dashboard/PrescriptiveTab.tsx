import { useMemo } from "react";
import { GaugeCircle, SlidersHorizontal } from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Slider } from "@/components/ui/slider";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import ChartCard from "@/components/dashboard/ChartCard";
import InsightCard from "@/components/dashboard/InsightCard";
import StatePanel from "@/components/dashboard/StatePanel";
import {
  DEMO_MODE,
  buildHazardChartData,
  formatCurrency,
  formatNumber,
  formatPct,
  type MonteCarloMetric,
  type PrescriptivePayload,
  type ScenarioInputs,
} from "@/lib/dashboard";

type PrescriptiveTabProps = {
  data: PrescriptivePayload | null;
  loading: boolean;
  error: string | null;
  selectedMonth: string;
  currentFilterLabel: string;
  scenarioInputs: ScenarioInputs;
  selectedScenarioId: string | null;
  onSelectScenario: (scenarioId: string) => void;
  onScenarioInputChange: (key: keyof ScenarioInputs, value: number) => void;
};

export default function PrescriptiveTab({
  data,
  loading,
  error,
  selectedMonth,
  currentFilterLabel,
  scenarioInputs,
  selectedScenarioId,
  onSelectScenario,
  onScenarioInputChange,
}: PrescriptiveTabProps) {
  const initialLoading = loading && !data;
  const switchingPreset = loading && !!data;
  const hazardData = buildHazardChartData(data?.hazard_histogram ?? []);
  const monteCarloDistribution = data?.monte_carlo?.net_value_distribution ?? [];
  const monteCarloProbabilityData = useMemo(
    () =>
      [
        {
          label: "Thắng mức gốc",
          value: data?.monte_carlo?.probability_scenario_beats_baseline,
          fill: "#0f766e",
          note: "Xác suất phương án vượt mức hiện tại",
        },
        {
          label: "Vẫn có lãi",
          value: data?.monte_carlo?.probability_net_positive,
          fill: "#2563eb",
          note: "Xác suất giá trị ròng sau chi phí vẫn dương",
        },
      ]
        .filter((row) => row.value != null)
        .map((row) => ({
          ...row,
          value_pct: Number(row.value) * 100,
        })),
    [data?.monte_carlo?.probability_net_positive, data?.monte_carlo?.probability_scenario_beats_baseline],
  );
  const monteCarloConfidenceMetrics = useMemo(() => {
    const metrics = data?.monte_carlo?.summary_metrics ?? [];
    const priorityColumns = [
      "net_value_after_cost_30d",
      "saved_revenue_from_risk_reduction_30d",
      "incremental_upsell_revenue_30d",
      "scenario_churn_prob_pct",
    ];
    const selected = priorityColumns
      .map((column) => metrics.find((metric) => metric.column === column))
      .filter((metric): metric is MonteCarloMetric => Boolean(metric));

    return selected.length ? selected : metrics.slice(0, 4);
  }, [data?.monte_carlo?.summary_metrics]);
  const availableScenarios = data?.meta.available_scenarios ?? [];
  const currentScenario =
    availableScenarios.find((scenario) => scenario.scenario_id === (selectedScenarioId ?? data?.meta.scenario_id)) ??
    availableScenarios[0] ??
    null;
  const leadingStrategy =
    [...(data?.sensitivity_roi ?? [])].sort(
      (a, b) => Number(b.revenue_impact_per_1pct) - Number(a.revenue_impact_per_1pct),
    )[0] ?? null;
  const churnDeltaPp =
    Number(data?.kpis.scenario_churn_prob_pct ?? 0) - Number(data?.kpis.baseline_churn_prob_pct ?? 0);
  const churnDeltaAbsPp = Math.abs(churnDeltaPp);
  const contentTransitionClass = switchingPreset
    ? "transition-all duration-300 opacity-60"
    : "transition-all duration-300 opacity-100";

  if (initialLoading) {
    return (
      <StatePanel
        title="Đang tải dữ liệu kịch bản"
        description="Đang lấy phương án can thiệp, hiệu quả tài chính, dịch chuyển rủi ro và mức độ chắc chắn của từng phương án."
        variant="loading"
      />
    );
  }

  if (!data) {
    return (
      <StatePanel
        title="Không có dữ liệu kịch bản"
        description={
          error
            ? `Không đọc được dữ liệu kịch bản cho tháng ${selectedMonth}. Chi tiết: ${error}`
            : `Chưa có dữ liệu kịch bản cho tháng ${selectedMonth}.`
        }
        variant={error ? "error" : "empty"}
      />
    );
  }

  return (
    <div className="space-y-5" aria-busy={switchingPreset}>
      <div className={`grid gap-5 xl:grid-cols-[minmax(0,1.62fr)_320px] 2xl:grid-cols-[minmax(0,1.5fr)_360px] ${contentTransitionClass}`}>
        <ChartCard
          title="Phương án này có đáng triển khai không?"
          subtitle="Giữ một waterfall tài chính làm tâm điểm để nhìn ngay lợi ích giữ lại, bán thêm và chi phí cuối cùng cộng lại thành gì."
          className="min-h-[320px]"
        >
          {data.financial_waterfall.length ? (
            <ResponsiveContainer width="100%" height={284}>
              <BarChart data={data.financial_waterfall}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis dataKey="name" tick={{ fontSize: 12 }} angle={-12} textAnchor="end" height={72} />
                <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompact(value)} />
                <Tooltip formatter={(value: number) => formatCurrency(Number(value))} />
                <Bar dataKey="value" radius={[10, 10, 0, 0]}>
                  {data.financial_waterfall.map((row, index) => (
                    <Cell key={`${row.name}-${index}`} fill={Number(row.value) >= 0 ? "#0f766e" : "#e11d48"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có dòng tác động tài chính" description="Kịch bản hiện tại chưa có đủ dữ liệu để dựng dòng tài chính." />
          )}
        </ChartCard>

        <section className="rounded-[28px] border border-white/70 bg-white/88 p-5 shadow-[0_18px_40px_-30px_rgba(15,23,42,0.34)] backdrop-blur">
          <div className="flex items-center justify-between gap-3">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">Bộ điều khiển</p>
              <h3 className="mt-2 font-display text-xl font-semibold tracking-[-0.03em] text-foreground">Chọn phương án trình bày</h3>
            </div>
            <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-slate-950 text-white">
              <SlidersHorizontal className="h-4 w-4" />
            </div>
          </div>

          <div className="mt-6 grid gap-3">
            <ScenarioSlider
              label="Chuyển sang tự gia hạn"
              value={scenarioInputs.auto_shift_pct}
              onChange={(value) => onScenarioInputChange("auto_shift_pct", value)}
              tone="bg-cyan-500"
              disabled={DEMO_MODE}
            />
            <ScenarioSlider
              label="Đẩy khách lên gói cao hơn"
              value={scenarioInputs.upsell_shift_pct}
              onChange={(value) => onScenarioInputChange("upsell_shift_pct", value)}
              tone="bg-emerald-500"
              disabled={DEMO_MODE}
            />
            <ScenarioSlider
              label="Giảm nhóm bỏ qua nhiều"
              value={scenarioInputs.skip_shift_pct}
              onChange={(value) => onScenarioInputChange("skip_shift_pct", value)}
              tone="bg-amber-500"
              disabled={DEMO_MODE}
            />
          </div>

          {DEMO_MODE && availableScenarios.length > 0 ? (
            <div className="mt-5 grid gap-3">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">Kịch bản có sẵn</p>
                <Select value={currentScenario?.scenario_id ?? ""} onValueChange={onSelectScenario}>
                  <SelectTrigger className="mt-3 h-12 rounded-2xl border-slate-200 bg-white" disabled={switchingPreset}>
                    <SelectValue placeholder="Chọn kịch bản" />
                  </SelectTrigger>
                  <SelectContent>
                    {availableScenarios.map((scenario) => (
                      <SelectItem key={scenario.scenario_id} value={scenario.scenario_id}>
                        {scenario.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="rounded-[22px] border border-slate-200 bg-slate-50 p-4 text-sm leading-6 text-slate-600">
                <p className="font-medium text-slate-950">{currentScenario?.label ?? data.meta.scenario_label ?? "Kịch bản mặc định"}</p>
                <p className="mt-2">
                  {currentScenario?.description ?? data.meta.scenario_description ?? "Kịch bản này đã được chuẩn hóa cho tháng đang chọn và sẵn sàng để đối chiếu với các phương án khác."}
                </p>
              </div>
            </div>
          ) : null}

          <div className="mt-5 rounded-[24px] bg-slate-950 p-5 text-white">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-white/60">Khả năng vẫn có lãi</p>
                <p className="mt-3 font-display text-4xl font-semibold tracking-[-0.06em]">
                  {data.monte_carlo.enabled
                    ? formatPct((data.monte_carlo.probability_net_positive ?? 0) * 100, 1)
                    : formatCurrency(data.kpis.net_value_after_cost ?? 0)}
                </p>
              </div>
              <GaugeCircle className="h-10 w-10 text-white/60" />
            </div>
            <p className="mt-4 text-sm leading-6 text-white/70">
              {data.monte_carlo.enabled
                ? "Đây là xác suất phương án vẫn tạo giá trị ròng dương sau khi trừ chi phí."
                : "Kịch bản này chưa có mô phỏng nhiều lần, nên đang hiển thị giá trị ròng ước tính."}
            </p>
          </div>

          <div className="mt-5 rounded-[22px] bg-slate-50 p-4 text-sm leading-6 text-slate-600">
            {DEMO_MODE ? (
              <>
                Mỗi preset là một phương án hành động khác nhau; chọn preset để đổi tình huống, còn thanh kéo phản ánh mức can thiệp của phương án đang xem.
              </>
            ) : (
              <>
                Kịch bản hiện tại đang chạy trên phạm vi <strong>{currentFilterLabel}</strong> và vẫn dùng đúng cấu trúc tham số của backend hiện có.
              </>
            )}
          </div>
        </section>
      </div>

      {switchingPreset ? (
        <div className="rounded-[22px] border border-cyan-200 bg-cyan-50/80 px-4 py-3 text-sm text-cyan-900">
          Giao diện đang giữ nguyên trong lúc nạp kịch bản <strong>{currentScenario?.label ?? "mới"}</strong> để tránh nhấp nháy. Các biểu đồ sẽ tự cập nhật ngay khi dữ liệu mới trả về.
        </div>
      ) : null}

      <div className={`grid gap-5 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Tóm tắt để ra quyết định"
          subtitle="Nhìn nhanh mức gốc, phần giữ lại thêm và giá trị ròng để quyết định có nên làm hay không."
          className="min-h-[280px]"
        >
          <div className="grid gap-3 sm:grid-cols-2">
            <DecisionBox label="Doanh thu hiện tại" value={formatCurrency(data.kpis.baseline_revenue)} tone="neutral" />
            <DecisionBox label="Doanh thu giữ lại thêm" value={formatCurrency(data.kpis.saved_revenue)} tone="positive" />
            <DecisionBox label="Doanh thu bán thêm" value={formatCurrency(data.kpis.incremental_upsell)} tone="positive" />
            <DecisionBox label="Giá trị ròng sau chi phí" value={formatCurrency(data.kpis.net_value_after_cost ?? 0)} tone="accent" />
          </div>
        </ChartCard>

        <ChartCard
          title="Biện pháp nào đáng scale?"
          subtitle="Mỗi 1% cải thiện đem lại thêm bao nhiêu doanh thu để biết nên ưu tiên mở rộng biện pháp nào."
          className="min-h-[280px]"
        >
          {data.sensitivity_roi.length ? (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={data.sensitivity_roi} layout="vertical" margin={{ left: 12, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis type="number" tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompact(value)} />
                <YAxis type="category" dataKey="strategy" tick={{ fontSize: 12 }} width={120} />
                <Tooltip formatter={(value: number) => formatCurrency(Number(value))} />
                <Bar dataKey="revenue_impact_per_1pct" radius={[0, 10, 10, 0]}>
                  {data.sensitivity_roi.map((row, index) => (
                    <Cell key={`${row.strategy}-${index}`} fill={Number(row.revenue_impact_per_1pct) >= 0 ? "#0f766e" : "#e11d48"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có bảng hiệu quả tăng thêm" description="Nguồn dữ liệu hiện tại chưa có tóm tắt độ nhạy của từng biện pháp." />
          )}
        </ChartCard>
      </div>

      <div className={`grid gap-5 xl:grid-cols-[420px_minmax(0,1fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Xác suất Monte Carlo"
          subtitle={`Tổng hợp từ ${formatNumber(data.monte_carlo.n_iterations)} lần mô phỏng để đọc nhanh khả năng thắng và khả năng vẫn có lãi.`}
          className="min-h-[320px]"
        >
          {data.monte_carlo.enabled && monteCarloProbabilityData.length ? (
            <div className="space-y-4">
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={monteCarloProbabilityData} layout="vertical" margin={{ top: 8, right: 16, bottom: 8, left: 8 }}>
                  <CartesianGrid horizontal={false} strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                  <XAxis type="number" domain={[0, 100]} tick={{ fontSize: 12 }} tickFormatter={(value) => `${Number(value)}%`} />
                  <YAxis type="category" dataKey="label" width={112} tick={{ fontSize: 12 }} />
                  <ReferenceLine x={50} stroke="rgba(148,163,184,0.9)" strokeDasharray="6 6" />
                  <Tooltip formatter={(value: number) => [formatPct(Number(value), 1), "Xác suất"]} />
                  <Bar dataKey="value_pct" radius={[0, 10, 10, 0]}>
                    {monteCarloProbabilityData.map((row) => (
                      <Cell key={row.label} fill={row.fill} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>

              <div className="grid gap-3">
                {monteCarloProbabilityData.map((row) => (
                  <div key={row.label} className="rounded-[18px] border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                    <div className="flex items-center justify-between gap-3">
                      <span className="font-medium text-slate-950">{row.label}</span>
                      <span className="rounded-full px-3 py-1 text-xs font-semibold text-white" style={{ backgroundColor: row.fill }}>
                        {formatPct(row.value_pct, 1)}
                      </span>
                    </div>
                    <p className="mt-2 leading-6">{row.note}</p>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <StatePanel title="Chưa có xác suất Monte Carlo" description="Kịch bản này chưa có đầu ra mô phỏng để đọc xác suất outcome." />
          )}
        </ChartCard>

        <ChartCard
          title="Phân bố giá trị ròng qua các lần mô phỏng"
          subtitle="Histogram của Net Value After Cost cho thấy phương án đang dao động quanh vùng lời hay lỗ mạnh đến mức nào."
          className="min-h-[320px]"
        >
          {data.monte_carlo.enabled && monteCarloDistribution.length ? (
            <ResponsiveContainer width="100%" height={272}>
              <BarChart data={monteCarloDistribution} margin={{ top: 12, right: 12, bottom: 74, left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                <XAxis dataKey="bucket_label" interval={0} angle={-22} textAnchor="end" height={78} tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => formatNumber(Number(value))} />
                <Tooltip
                  labelFormatter={(_, payload) => {
                    const row = payload?.[0]?.payload;
                    if (!row) return "";
                    return `${formatCurrency(Number(row.bucket_start))} đến ${formatCurrency(Number(row.bucket_end))}`;
                  }}
                  formatter={(value: number, name, item) => {
                    if (name === "run_count") {
                      const row = item?.payload;
                      return [`${formatNumber(Number(value))} lần chạy • ${formatPct(Number(row?.share_pct ?? 0), 1)}`, "Số lần mô phỏng"];
                    }
                    return [value, name];
                  }}
                />
                <Bar dataKey="run_count" radius={[8, 8, 0, 0]}>
                  {monteCarloDistribution.map((row, index) => (
                    <Cell
                      key={`${row.bucket_start}-${row.bucket_end}-${index}`}
                      fill={
                        row.bucket_end <= 0
                          ? "#ef4444"
                          : row.bucket_start >= 0
                            ? "#0f766e"
                            : "#f59e0b"
                      }
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <StatePanel title="Chưa có phân bố mô phỏng" description="Nguồn hiện tại chưa trả về histogram của giá trị ròng Monte Carlo." />
          )}
        </ChartCard>
      </div>

      <div className={`grid gap-5 xl:grid-cols-[minmax(0,1.18fr)_minmax(0,0.82fr)] ${contentTransitionClass}`}>
        <ChartCard
          title="Dải bất định P05-P95"
          subtitle="Dùng dải xác suất để thấy phương án dao động rộng hay hẹp trên từng chỉ số trọng yếu."
          className="min-h-[330px]"
        >
          {data.monte_carlo.enabled && monteCarloConfidenceMetrics.length ? (
            <div className="space-y-4">
              {monteCarloConfidenceMetrics.map((metric) => (
                <MonteCarloBandRow
                  key={metric.column ?? metric.metric}
                  metric={metric}
                  deterministicValue={resolveDeterministicMetricValue(metric, data)}
                />
              ))}
            </div>
          ) : (
            <StatePanel title="Chưa có dải bất định" description="Kịch bản này chưa có percentile Monte Carlo cho các chỉ số chính." />
          )}
        </ChartCard>

        <div className="grid gap-4 self-start">
          <InsightCard
            type="insight"
            title="Monte Carlo đang nói gì?"
            description={
              data.monte_carlo.enabled ? (
                <>
                  Kịch bản này đã được chạy <strong>{formatNumber(data.monte_carlo.n_iterations)}</strong> lần mô phỏng để kiểm tra độ ổn định, không chỉ nhìn một giá trị trung bình.
                </>
              ) : (
                "Kịch bản hiện tại chưa có đầu ra Monte Carlo để kiểm tra độ chắc chắn."
              )
            }
          />
          <InsightCard
            type="warning"
            title="Điểm cần canh"
            description={
              data.monte_carlo.enabled ? (
                <>
                  Nếu histogram còn cắt qua vùng âm hoặc dải P05-P95 quá rộng, đội nên coi đây là phương án có độ biến động cao dù giá trị kỳ vọng vẫn dương.
                </>
              ) : (
                "Khi chưa có mô phỏng, nên coi ước tính hiện tại là một điểm tham chiếu chứ chưa phải vùng chắc chắn."
              )
            }
          />
        </div>
      </div>

      <div className={`grid gap-4 xl:grid-cols-3 ${contentTransitionClass}`}>
        <InsightCard
          type="success"
          title="Phương án thay đổi điều gì?"
          description={
            <>
              Tỷ lệ rời bỏ{" "}
              {churnDeltaPp < 0 ? (
                <>
                  giảm từ <strong>{formatPct(data.kpis.baseline_churn_prob_pct)}</strong> xuống{" "}
                  <strong>{formatPct(data.kpis.scenario_churn_prob_pct)}</strong>, tức cải thiện khoảng{" "}
                  <strong>{churnDeltaAbsPp.toFixed(1)} điểm %</strong>.
                </>
              ) : churnDeltaPp > 0 ? (
                <>
                  tăng từ <strong>{formatPct(data.kpis.baseline_churn_prob_pct)}</strong> lên{" "}
                  <strong>{formatPct(data.kpis.scenario_churn_prob_pct)}</strong>, tức xấu đi khoảng{" "}
                  <strong>{churnDeltaAbsPp.toFixed(1)} điểm %</strong>.
                </>
              ) : (
                <>
                  giữ nguyên ở <strong>{formatPct(data.kpis.scenario_churn_prob_pct)}</strong>.
                </>
              )}
            </>
          }
        />
        <InsightCard
          type="insight"
          title="Vì sao đáng quan tâm?"
          description={
            DEMO_MODE ? (
              <>
                Đây là lớp chốt quyết định cho tháng đang xem, giúp cân bằng giữa giá trị ròng, xác suất sinh lời và mức tác động trước khi triển khai.
              </>
            ) : (
              <>
                Đây là lớp giúp chốt quyết định cho phạm vi <strong>{currentFilterLabel}</strong>. Nếu có mô phỏng nhiều lần, bạn còn biết thêm mức độ chắc chắn trước khi triển khai thật.
              </>
            )
          }
        />
        <InsightCard
          type="action"
          title="Nên làm gì trước?"
          description={
            leadingStrategy ? (
              <>
                Ưu tiên mở rộng <strong>{leadingStrategy.strategy}</strong> trước vì đây là biện pháp có tác động tăng thêm cao nhất trên mỗi 1% cải thiện.
              </>
            ) : (
              <>
                Ưu tiên biện pháp có hiệu quả tăng thêm cao nhất trước, sau đó chốt phương án có <strong>khả năng vẫn có lãi</strong> vượt ngưỡng chấp nhận của đội.
              </>
            )
          }
        />
      </div>

      <details className={`rounded-[28px] border border-white/70 bg-white/88 p-5 shadow-[0_18px_40px_-30px_rgba(15,23,42,0.34)] backdrop-blur ${contentTransitionClass}`}>
        <summary className="cursor-pointer list-none text-sm font-semibold text-slate-900">Xem lớp phân tích bổ sung</summary>
        <p className="mt-2 text-sm leading-6 text-slate-600">
          Giữ phần này cho các biểu đồ giải thích phụ, còn Monte Carlo đã được đẩy lên vùng chính để đọc quyết định ngay trên màn đầu.
        </p>

        <div className="mt-5">
          <ChartCard
            title="Dịch chuyển mức rủi ro toàn bộ khách"
            subtitle="So sánh trước và sau can thiệp để xem rủi ro có thật sự dồn về vùng an toàn hơn không."
            className="min-h-[300px]"
          >
            {hazardData.length ? (
              <ResponsiveContainer width="100%" height={250}>
                <LineChart data={hazardData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
                  <XAxis dataKey="bucket" tick={{ fontSize: 10 }} angle={-20} textAnchor="end" height={72} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="baseline" name="Trước can thiệp" stroke="#94a3b8" strokeWidth={2.5} dot={false} />
                  <Line type="monotone" dataKey="scenario" name="Sau can thiệp" stroke="#2563eb" strokeWidth={2.5} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <StatePanel title="Chưa có biểu đồ dịch chuyển rủi ro" description="Kịch bản này chưa có đủ dữ liệu để so sánh phân bố rủi ro trước và sau can thiệp." />
            )}
          </ChartCard>
        </div>
      </details>
    </div>
  );
}

function ScenarioSlider({
  label,
  value,
  onChange,
  tone,
  disabled,
}: {
  label: string;
  value: number;
  onChange: (value: number) => void;
  tone: string;
  disabled?: boolean;
}) {
  return (
    <div className="rounded-[22px] bg-slate-50 p-4">
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm font-medium text-slate-900">{label}</p>
        <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-slate-700">{value}%</span>
      </div>
      <Slider
        className="mt-5"
        value={[value]}
        min={0}
        max={100}
        step={1}
        disabled={disabled}
        onValueChange={(values) => onChange(values[0] ?? 0)}
      />
      <div className={`mt-4 h-1.5 rounded-full ${tone}`} style={{ width: `${value}%` }} />
    </div>
  );
}

function DecisionBox({ label, value, tone }: { label: string; value: string; tone: "neutral" | "positive" | "accent" }) {
  const toneClass =
    tone === "positive" ? "bg-emerald-50 text-emerald-800" : tone === "accent" ? "bg-cyan-50 text-cyan-800" : "bg-slate-50 text-slate-800";

  return (
    <div className={`rounded-[22px] border border-slate-200 p-4 ${toneClass}`}>
      <p className="text-xs font-semibold uppercase tracking-[0.18em]">{label}</p>
      <p className="mt-3 font-display text-2xl font-semibold tracking-[-0.05em]">{value}</p>
    </div>
  );
}

function formatConfidenceMetric(metric: string): string {
  const metricMap: Record<string, string> = {
    "Scenario churn %": "Tỷ lệ rời bỏ theo phương án",
    "Baseline churn %": "Tỷ lệ rời bỏ hiện tại",
    "Net value after cost": "Giá trị ròng sau chi phí",
    "Saved Revenue": "Doanh thu giữ lại thêm",
    "Saved Revenue from Risk Reduction": "Doanh thu giữ lại thêm",
    "Incremental Upsell Revenue": "Doanh thu bán thêm",
    "Scenario Revenue": "Doanh thu theo phương án",
    "Baseline Revenue": "Doanh thu mức gốc",
    "Campaign Cost": "Chi phí triển khai",
  };

  return metricMap[metric] ?? metric;
}

function formatCompact(value: number): string {
  return new Intl.NumberFormat("vi-VN", { notation: "compact", maximumFractionDigits: 1 }).format(Number(value) || 0);
}

function resolveDeterministicMetricValue(metric: MonteCarloMetric, data: PrescriptivePayload): number | null {
  switch (metric.column) {
    case "net_value_after_cost_30d":
      return Number(data.kpis.net_value_after_cost ?? 0);
    case "saved_revenue_from_risk_reduction_30d":
      return Number(data.kpis.saved_revenue ?? 0);
    case "incremental_upsell_revenue_30d":
      return Number(data.kpis.incremental_upsell ?? 0);
    case "scenario_retained_revenue_30d":
      return Number(data.kpis.optimized_projected_revenue ?? 0);
    case "baseline_retained_revenue_30d":
      return Number(data.kpis.baseline_revenue ?? 0);
    case "campaign_cost_30d":
      return Number(data.kpis.campaign_cost ?? 0);
    case "scenario_churn_prob_pct":
      return Number(data.kpis.scenario_churn_prob_pct ?? 0);
    case "baseline_churn_prob_pct":
      return Number(data.kpis.baseline_churn_prob_pct ?? 0);
    default:
      return null;
  }
}

function formatMonteCarloMetricValue(metric: MonteCarloMetric, value: number): string {
  const isPct = Boolean(metric.column?.includes("churn_prob_pct")) || metric.metric.toLowerCase().includes("churn");
  return isPct ? formatPct(value, 2) : formatCurrency(value);
}

function MonteCarloBandRow({
  metric,
  deterministicValue,
}: {
  metric: MonteCarloMetric;
  deterministicValue: number | null;
}) {
  const bandSpan = Math.max(metric.p95 - metric.p05, Math.abs(metric.p50) * 0.05, 1e-6);
  const normalize = (value: number) => Math.min(100, Math.max(0, ((value - metric.p05) / bandSpan) * 100));
  const middleStart = normalize(metric.p25);
  const middleEnd = normalize(metric.p75);
  const medianPosition = normalize(metric.p50);
  const deterministicPosition = deterministicValue == null ? null : normalize(deterministicValue);
  const toneClass = metric.column?.includes("churn_prob_pct") ? "bg-blue-600" : "bg-emerald-600";

  return (
    <div className="rounded-[22px] border border-slate-200 bg-slate-50 p-4">
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="font-medium text-slate-950">{formatConfidenceMetric(metric.metric)}</p>
          <p className="mt-1 text-xs uppercase tracking-[0.18em] text-muted-foreground">
            P50 {formatMonteCarloMetricValue(metric, metric.p50)} • Mean {formatMonteCarloMetricValue(metric, metric.mean)}
          </p>
        </div>
        {deterministicValue != null ? (
          <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-slate-700">
            Deterministic {formatMonteCarloMetricValue(metric, deterministicValue)}
          </span>
        ) : null}
      </div>

      <div className="mt-4">
        <div className="relative h-4 rounded-full bg-slate-200/90">
          <div className="absolute inset-y-0 left-0 rounded-full bg-slate-300" style={{ width: "100%" }} />
          <div
            className="absolute inset-y-0 rounded-full bg-slate-500/55"
            style={{ left: `${middleStart}%`, width: `${Math.max(middleEnd - middleStart, 2)}%` }}
          />
          <div className="absolute top-1/2 h-7 w-[2px] -translate-y-1/2 bg-slate-950" style={{ left: `${medianPosition}%` }} />
          {deterministicPosition != null ? (
            <div
              className={`absolute top-1/2 h-4 w-4 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-white shadow ${toneClass}`}
              style={{ left: `${deterministicPosition}%` }}
            />
          ) : null}
        </div>

        <div className="mt-3 flex items-center justify-between gap-3 text-xs text-slate-600">
          <span>P05 {formatMonteCarloMetricValue(metric, metric.p05)}</span>
          <span>P95 {formatMonteCarloMetricValue(metric, metric.p95)}</span>
        </div>
      </div>
    </div>
  );
}
