import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  BrainCircuit,
  CalendarClock,
  DollarSign,
  GaugeCircle,
  LifeBuoy,
  Sparkles,
  Wand2,
} from "lucide-react";
import DashboardHeader from "@/components/dashboard/DashboardHeader";
import DescriptiveTab from "@/components/dashboard/DescriptiveTab";
import KPICard from "@/components/dashboard/KPICard";
import PredictiveTab from "@/components/dashboard/PredictiveTab";
import PrescriptiveTab from "@/components/dashboard/PrescriptiveTab";
import TabNavigation, { type TabId, type TabNavigationItem } from "@/components/dashboard/TabNavigation";
import { useDashboardData } from "@/hooks/useDashboardData";
import {
  DASHBOARD_COPY,
  DIMENSION_LABELS,
  buildSnapshotPulse,
  formatCompactCurrency,
  formatCurrency,
  formatMonthLabel,
  formatNumber,
  formatPct,
  formatPulseDateLabel,
  type PulseReplayFrame,
} from "@/lib/dashboard";

type MetricCardConfig = {
  title: string;
  value: string;
  subtitle: string;
  change?: number;
  changeLabel?: string;
  icon: typeof Activity;
  variant: "default" | "danger" | "success" | "warning" | "accent";
  footer?: string;
};

function relativeChange(current: number | null | undefined, previous: number | null | undefined): number | undefined {
  if (current == null || previous == null) return undefined;
  if (!Number.isFinite(current) || !Number.isFinite(previous)) return undefined;
  if (Math.abs(previous) < 1e-9) return current === 0 ? 0 : undefined;
  return ((current - previous) / Math.abs(previous)) * 100;
}

export default function Index() {
  const [activeTab, setActiveTab] = useState<TabId>("descriptive");
  const [descriptiveReplayFrame, setDescriptiveReplayFrame] = useState<PulseReplayFrame>({
    point: null,
    dateLabel: null,
    status: "idle",
  });
  const dashboard = useDashboardData(activeTab);
  const descriptivePulse = useMemo(() => buildSnapshotPulse(dashboard.snapshot), [dashboard.snapshot]);
  const latestDescriptivePulse = descriptivePulse[descriptivePulse.length - 1] ?? null;
  const currentDescriptivePulse = descriptiveReplayFrame.point ?? latestDescriptivePulse;
  const currentDescriptivePulseDateLabel =
    descriptiveReplayFrame.dateLabel ??
    (latestDescriptivePulse ? formatPulseDateLabel(latestDescriptivePulse.event_date) : null);

  useEffect(() => {
    setDescriptiveReplayFrame({
      point: null,
      dateLabel: null,
      status: "idle",
    });
  }, [dashboard.snapshot?.meta.month]);

  const handleDescriptiveReplayFrameChange = useCallback((frame: PulseReplayFrame) => {
    setDescriptiveReplayFrame((previous) => {
      const previousDate = previous.point?.event_date ?? null;
      const nextDate = frame.point?.event_date ?? null;

      if (
        previousDate === nextDate &&
        previous.dateLabel === frame.dateLabel &&
        previous.status === frame.status
      ) {
        return previous;
      }

      return frame;
    });
  }, []);

  const heroCards = useMemo(
    () => [
      {
        label: "Tỷ lệ rời bỏ thực tế",
        value: dashboard.snapshot ? formatPct(dashboard.snapshot.metrics.historical_churn_rate) : "-",
        note: dashboard.snapshot
          ? `${formatNumber(dashboard.snapshot.metrics.total_expiring_users)} khách sắp hết hạn trong nhóm đang xem`
          : "Đang chờ dữ liệu tổng quan",
      },
      {
        label: "Khách cần giữ ngay",
        value: currentDescriptivePulse ? formatNumber(Number(currentDescriptivePulse.high_risk_users ?? 0)) : "-",
        note: currentDescriptivePulseDateLabel
          ? `${currentDescriptivePulseDateLabel} • cập nhật theo nhịp trong tháng`
          : "Đang chờ chuỗi dữ liệu nguy cơ",
      },
      {
        label: "Doanh thu có nguy cơ mất",
        value: dashboard.tab2Data ? formatCompactCurrency(dashboard.tab2Data.kpis.predicted_revenue_at_risk) : "-",
        note: dashboard.tab2Data ? dashboard.tab2Data.kpis.top_segment : "Đang chờ dữ liệu dự báo",
      },
      {
        label: "Giá trị ròng của kịch bản",
        value: dashboard.tab3Data ? formatCompactCurrency(dashboard.tab3Data.kpis.net_value_after_cost ?? 0) : "-",
        note: dashboard.tab3Data?.monte_carlo?.enabled
          ? `${formatPct((dashboard.tab3Data.monte_carlo.probability_net_positive ?? 0) * 100)} khả năng kịch bản vẫn có lãi`
          : "Ước tính nhanh theo kịch bản đang chọn",
      },
    ],
    [
      currentDescriptivePulse,
      currentDescriptivePulseDateLabel,
      dashboard.snapshot,
      dashboard.tab2Data,
      dashboard.tab3Data,
    ],
  );

  const metricCards = useMemo<MetricCardConfig[]>(() => {
    if (activeTab === "descriptive") {
      const previousKpis = dashboard.tab1Data?.previous_kpis ?? null;
      const previousMonthLabel = dashboard.tab1Data?.meta.previous_month
        ? formatMonthLabel(dashboard.tab1Data.meta.previous_month)
        : undefined;

      return [
        {
          title: "Tỷ lệ rời bỏ lịch sử",
          value: dashboard.tab1Data ? formatPct(dashboard.tab1Data.kpis.historical_churn_rate) : "-",
          subtitle: "Tỷ lệ không quay lại trong 30 ngày sau khi hết hạn",
          change: relativeChange(
            dashboard.tab1Data?.kpis.historical_churn_rate,
            previousKpis?.historical_churn_rate,
          ),
          changeLabel: previousMonthLabel,
          icon: AlertTriangle,
          variant: "danger",
          footer: dashboard.tab1Data ? `${formatNumber(dashboard.tab1Data.kpis.total_expiring_users)} khách trong nhóm đang theo dõi` : undefined,
        },
        {
          title: "Thời gian gắn bó trung vị",
          value: dashboard.tab1Data ? `${formatNumber(dashboard.tab1Data.kpis.overall_median_survival)} d` : "-",
          subtitle: "Cho biết khách thường duy trì được bao lâu trước khi rời đi",
          icon: CalendarClock,
          variant: "default",
        },
        {
          title: "Tỷ lệ tự gia hạn",
          value: dashboard.tab1Data ? formatPct(dashboard.tab1Data.kpis.auto_renew_rate) : "-",
          subtitle: "Nhóm có khả năng giữ lại tự nhiên tốt hơn",
          change: relativeChange(
            dashboard.tab1Data?.kpis.auto_renew_rate,
            previousKpis?.auto_renew_rate,
          ),
          changeLabel: previousMonthLabel,
          icon: Sparkles,
          variant: "success",
          footer: dashboard.currentFilterLabel,
        },
        {
          title: "Doanh thu đang bị đe dọa",
          value: dashboard.tab1Data
            ? formatCompactCurrency(Number(dashboard.tab1Data.kpis.historical_revenue_at_risk ?? 0))
            : "-",
          subtitle: "Ước tính từ xu hướng rời bỏ lịch sử nhân với doanh thu sắp gia hạn",
          change: relativeChange(
            dashboard.tab1Data?.kpis.historical_revenue_at_risk,
            previousKpis?.historical_revenue_at_risk,
          ),
          changeLabel: previousMonthLabel,
          icon: DollarSign,
          variant: "warning",
          footer: dashboard.tab1Data
            ? `Tổng doanh thu sắp gia hạn ${formatCompactCurrency(Number(dashboard.tab1Data.kpis.total_expected_renewal_amount ?? 0))}`
            : undefined,
        },
      ];
    }

    if (activeTab === "predictive") {
      return [
        {
          title: "Tỷ lệ rời bỏ dự báo",
          value: dashboard.tab2Data ? formatPct(dashboard.tab2Data.kpis.forecasted_churn_rate) : "-",
          subtitle: "Ước tính cho 30 ngày tới của nhóm khách đang chấm điểm",
          change: dashboard.tab2Data?.kpis.forecasted_churn_delta_pp_vs_prev_month,
          changeLabel: dashboard.tab2Data?.meta.previous_month ? formatMonthLabel(dashboard.tab2Data.meta.previous_month) : undefined,
          icon: BrainCircuit,
          variant: "danger",
        },
        {
          title: "Khách nguy cơ cao",
          value: dashboard.tab2Data ? formatNumber(dashboard.tab2Data.kpis.high_flight_risk_users) : "-",
          subtitle: "Nhóm cần ưu tiên giữ chân ngay ở thời điểm này",
          icon: LifeBuoy,
          variant: "warning",
          footer: dashboard.tab2Data ? `${formatNumber(dashboard.tab2Data.meta.sample_user_count)} khách đã được chấm điểm` : undefined,
        },
        {
          title: "Doanh thu có nguy cơ mất",
          value: dashboard.tab2Data ? formatCurrency(dashboard.tab2Data.kpis.predicted_revenue_at_risk) : "-",
          subtitle: "Phần doanh thu 30 ngày tới dễ hụt nếu không can thiệp",
          icon: DollarSign,
          variant: "warning",
          footer: dashboard.tab2Data ? dashboard.tab2Data.kpis.top_segment : undefined,
        },
        {
          title: "Doanh thu tương đối an toàn",
          value: dashboard.tab2Data ? formatCurrency(dashboard.tab2Data.kpis.safe_revenue) : "-",
          subtitle: "Phần doanh thu hiện chưa nằm trong nhóm rủi ro cao",
          icon: GaugeCircle,
          variant: "success",
          footer: dashboard.currentFilterLabel,
        },
      ];
    }

    const churnDelta = dashboard.tab3Data
      ? dashboard.tab3Data.kpis.scenario_churn_prob_pct - dashboard.tab3Data.kpis.baseline_churn_prob_pct
      : undefined;

    return [
      {
        title: "Tỷ lệ rời bỏ gốc",
        value: dashboard.tab3Data ? formatPct(dashboard.tab3Data.kpis.baseline_churn_prob_pct) : "-",
        subtitle: "Nếu chưa có biện pháp can thiệp",
        icon: BarChart3,
        variant: "default",
      },
      {
        title: "Tỷ lệ rời bỏ sau can thiệp",
        value: dashboard.tab3Data ? formatPct(dashboard.tab3Data.kpis.scenario_churn_prob_pct) : "-",
        subtitle: "Sau khi áp dụng phương án đang chọn",
        change: churnDelta,
        changeLabel: "mức gốc",
        icon: Wand2,
        variant: churnDelta != null && churnDelta <= 0 ? "success" : "danger",
      },
      {
        title: "Doanh thu dự kiến giữ lại",
        value: dashboard.tab3Data ? formatCurrency(dashboard.tab3Data.kpis.optimized_projected_revenue) : "-",
        subtitle: "Gồm phần doanh thu giữ được và phần bán thêm",
        icon: DollarSign,
        variant: "success",
        footer: dashboard.tab3Data ? `Giữ lại thêm ${formatCurrency(dashboard.tab3Data.kpis.saved_revenue)}` : undefined,
      },
      {
        title: "Giá trị ròng sau chi phí",
        value: dashboard.tab3Data ? formatCurrency(dashboard.tab3Data.kpis.net_value_after_cost ?? 0) : "-",
        subtitle: "Đã trừ toàn bộ chi phí triển khai",
        icon: Sparkles,
        variant: (dashboard.tab3Data?.kpis.net_value_after_cost ?? 0) >= 0 ? "accent" : "warning",
      },
    ];
  }, [
    activeTab,
    dashboard.currentFilterLabel,
    dashboard.snapshot,
    dashboard.tab1Data,
    dashboard.tab2Data,
    dashboard.tab3Data,
    currentDescriptivePulse,
    currentDescriptivePulseDateLabel,
  ]);

  const focusCard = useMemo(() => {
    if (activeTab === "descriptive") {
      return {
        title: "Điểm cần nhìn ngay",
        value: dashboard.currentFilterLabel,
        note: `Đang chia nhóm theo ${DIMENSION_LABELS[dashboard.tab1Dimension]}`,
      };
    }

    if (activeTab === "predictive") {
      return {
        title: "Nhóm dễ mất doanh thu nhất",
        value: dashboard.tab2Data?.kpis.top_segment ?? "-",
        note: dashboard.tab2Data
          ? `${formatCurrency(dashboard.tab2Data.kpis.top_segment_risk)} doanh thu rủi ro trên ${formatNumber(dashboard.tab2Data.kpis.top_segment_user_count)} khách`
          : "Đang chờ dữ liệu dự báo",
      };
    }

    return {
      title: "Mức tự tin của kịch bản",
      value: dashboard.tab3Data?.monte_carlo?.enabled
        ? formatPct((dashboard.tab3Data.monte_carlo.probability_scenario_beats_baseline ?? 0) * 100)
        : dashboard.tab3Data
          ? formatCurrency(dashboard.tab3Data.kpis.net_value_after_cost ?? 0)
          : "-",
      note: dashboard.tab3Data?.monte_carlo?.enabled
        ? "Khả năng phương án này tốt hơn mức hiện tại"
        : "Ước tính nhanh khi chưa dùng mô phỏng nhiều lần",
    };
  }, [activeTab, dashboard.currentFilterLabel, dashboard.tab1Dimension, dashboard.tab2Data, dashboard.tab3Data]);

  const predictiveStandby = !dashboard.tab2Loading && !dashboard.tab2HasData && !dashboard.tab2Error;
  const prescriptiveStandby = !dashboard.tab3Loading && !dashboard.tab3HasData && !dashboard.tab3Error;

  const navigationItems = useMemo<TabNavigationItem[]>(
    () => [
      {
        id: "descriptive",
        label: "Hiện trạng",
        description: "Đọc nhanh sức khỏe nhóm khách",
        badge: dashboard.tab1Loading ? "Nạp" : dashboard.tab1HasData ? formatNumber(dashboard.tab1Data?.kpis.total_expiring_users ?? 0) : "Lỗi",
        state: dashboard.tab1Loading ? "loading" : dashboard.tab1HasData ? "ready" : "missing",
      },
      {
        id: "predictive",
        label: "Dự báo",
        description: "Nhìn trước rủi ro và doanh thu",
        badge: dashboard.tab2Loading
          ? "Nạp"
          : dashboard.tab2HasData
            ? formatNumber(dashboard.tab2Data?.meta.sample_user_count ?? 0)
            : predictiveStandby
              ? "Chờ"
              : "Lỗi",
        state: dashboard.tab2Loading ? "loading" : dashboard.tab2HasData ? "ready" : predictiveStandby ? "standby" : "missing",
      },
      {
        id: "prescriptive",
        label: "Hành động",
        description: "So sánh các phương án can thiệp",
        badge: dashboard.tab3Loading
          ? "Nạp"
          : dashboard.tab3HasData
            ? formatNumber(dashboard.tab3Data?.meta.sample_user_count ?? 0)
            : prescriptiveStandby
              ? "Chờ"
              : "Lỗi",
        state: dashboard.tab3Loading ? "loading" : dashboard.tab3HasData ? "ready" : prescriptiveStandby ? "standby" : "missing",
      },
    ],
    [
      dashboard.tab1Data,
      dashboard.tab1HasData,
      dashboard.tab1Loading,
      dashboard.tab2Data,
      dashboard.tab2Error,
      dashboard.tab2HasData,
      dashboard.tab2Loading,
      dashboard.tab3Data,
      dashboard.tab3Error,
      dashboard.tab3HasData,
      dashboard.tab3Loading,
      predictiveStandby,
      prescriptiveStandby,
    ],
  );

  const readinessCards = useMemo(
    () => [
      {
        label: "Tháng đang xem",
        value: dashboard.selectedMonth ? formatMonthLabel(dashboard.selectedMonth) : dashboard.monthOptions[0] ? formatMonthLabel(dashboard.monthOptions[0]) : "Đang chờ",
        note: dashboard.monthOptions.length ? `${dashboard.monthOptions.length} mốc tháng có sẵn` : "Đang chờ danh sách tháng",
      },
      {
        label: "Hiện trạng",
        value: dashboard.tab1Loading ? "Đang tải" : dashboard.tab1HasData ? "Sẵn sàng" : "Thiếu dữ liệu",
        note: dashboard.tab1HasData
          ? `${formatNumber(dashboard.tab1Data?.kpis.total_expiring_users ?? 0)} khách sắp hết hạn`
          : dashboard.tab1Error ?? "Chưa có dữ liệu hiện trạng",
      },
      {
        label: "Dự báo",
        value: dashboard.tab2Loading ? "Đang tải" : dashboard.tab2HasData ? "Sẵn sàng" : predictiveStandby ? "Chờ mở" : "Thiếu dữ liệu",
        note: dashboard.tab2HasData
          ? `${formatNumber(dashboard.tab2Data?.meta.sample_user_count ?? 0)} khách đã được chấm điểm`
          : predictiveStandby
            ? "Mở tab Dự báo để nạp bộ kết quả tương ứng với tháng đang chọn"
            : dashboard.tab2Error ?? "Chưa có dữ liệu dự báo",
      },
      {
        label: "Hành động",
        value: dashboard.tab3Loading ? "Đang tải" : dashboard.tab3HasData ? "Sẵn sàng" : prescriptiveStandby ? "Chờ mở" : "Thiếu dữ liệu",
        note: dashboard.tab3Data?.monte_carlo?.enabled
          ? `${formatNumber(dashboard.tab3Data.monte_carlo.n_iterations)} lần mô phỏng`
          : prescriptiveStandby
            ? "Mở tab Hành động để nạp bộ kịch bản tương ứng với tháng đang chọn"
            : dashboard.tab3Error ?? "Chưa có dữ liệu kịch bản",
      },
    ],
    [
      dashboard.monthOptions,
      dashboard.selectedMonth,
      dashboard.tab1Data,
      dashboard.tab1Error,
      dashboard.tab1HasData,
      dashboard.tab1Loading,
      dashboard.tab2Data,
      dashboard.tab2Error,
      dashboard.tab2HasData,
      dashboard.tab2Loading,
      dashboard.tab3Data,
      dashboard.tab3Error,
      dashboard.tab3HasData,
      dashboard.tab3Loading,
      predictiveStandby,
      prescriptiveStandby,
    ],
  );

  const activeCopy = DASHBOARD_COPY[activeTab];
  const isRefreshing =
    dashboard.tab1Loading || dashboard.tab2Loading || dashboard.tab3Loading || dashboard.wsStatus === "connecting";

  return (
    <div className="min-h-screen bg-[linear-gradient(180deg,#eef6f7_0%,#f7fafc_18%,#f8fafc_100%)] text-foreground">
      <div className="w-full px-4 py-6 sm:px-6 lg:px-8 xl:px-10 2xl:px-12">
        <DashboardHeader
          selectedMonth={dashboard.selectedMonth}
          monthOptions={dashboard.monthOptions}
          onMonthChange={dashboard.setSelectedMonth}
          onRefresh={dashboard.triggerRefresh}
          isRefreshing={isRefreshing}
          demoMode={dashboard.demoMode}
          dataModeLabel={dashboard.dataModeLabel}
          lastUpdatedLabel={dashboard.lastUpdatedLabel}
          currentFilterLabel={dashboard.currentFilterLabel}
          replayStatus={dashboard.replayStatus}
          replayBusy={dashboard.replayBusy}
          replayProgressPct={dashboard.replayProgressPct}
          onReplay={dashboard.triggerReplay}
        />

        <div className="mt-6 grid gap-5 xl:grid-cols-[264px_minmax(0,1fr)] 2xl:grid-cols-[288px_minmax(0,1fr)]">
          <aside className="space-y-5">
            <div className="rounded-[28px] border border-white/70 bg-white/88 p-4 shadow-[0_18px_40px_-30px_rgba(15,23,42,0.34)] backdrop-blur">
              <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">Điều hướng</p>
              <h2 className="mt-2 font-display text-xl font-semibold tracking-[-0.04em] text-slate-950">3 lớp phân tích chính</h2>
              <p className="mt-2 text-sm leading-6 text-slate-600">Đi từ hiện trạng sang dự báo và chốt phương án hành động.</p>
              <div className="mt-4">
                <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} items={navigationItems} />
              </div>
            </div>

            <div className="rounded-[28px] border border-white/70 bg-white/88 p-4 shadow-[0_18px_40px_-30px_rgba(15,23,42,0.34)] backdrop-blur">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">Tình trạng dữ liệu</p>
                  <h3 className="mt-2 font-display text-xl font-semibold tracking-[-0.04em] text-slate-950">Các lớp đã sẵn sàng đến đâu</h3>
                </div>
                <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">{dashboard.lastUpdatedLabel}</span>
              </div>
              <div className="mt-4 space-y-3">
                {readinessCards.map((card) => (
                  <div key={card.label} className="rounded-[20px] bg-slate-50 px-4 py-3">
                    <div className="flex items-center justify-between gap-3">
                      <p className="text-sm font-medium text-slate-900">{card.label}</p>
                      <span className="rounded-full bg-white px-3 py-1 text-xs font-semibold text-slate-700">{card.value}</span>
                    </div>
                    <p className="mt-2 text-sm leading-6 text-slate-600">{card.note}</p>
                  </div>
                ))}
              </div>
            </div>
          </aside>

          <main className="space-y-5">
            <section className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_320px] 2xl:grid-cols-[minmax(0,1fr)_360px]">
              <div className="rounded-[30px] border border-white/70 bg-white/88 p-5 shadow-[0_20px_48px_-34px_rgba(15,23,42,0.34)] backdrop-blur">
                <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">{activeCopy.kicker}</p>
                <h2 className="mt-3 font-display text-[2rem] font-semibold tracking-[-0.05em] text-slate-950">{activeCopy.title}</h2>
                <p className="mt-3 max-w-3xl text-sm leading-7 text-slate-600">{activeCopy.description}</p>
                <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  {heroCards.map((card) => (
                    <OverviewCard key={card.label} label={card.label} value={card.value} note={card.note} />
                  ))}
                </div>
              </div>

              <div className="rounded-[30px] border border-cyan-100 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(236,254,255,0.9))] p-5 shadow-[0_20px_48px_-34px_rgba(8,145,178,0.28)]">
                <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-cyan-700">Trọng tâm hiện tại</p>
                <h3 className="mt-3 font-display text-xl font-semibold tracking-[-0.05em] text-slate-950">{focusCard.title}</h3>
                <p className="mt-5 font-display text-[2.3rem] font-semibold tracking-[-0.07em] text-slate-950">{focusCard.value}</p>
                <p className="mt-3 text-sm leading-7 text-slate-600">{focusCard.note}</p>
              </div>
            </section>

            <section className="grid gap-4 md:grid-cols-2 2xl:grid-cols-4">
              {metricCards.map((card) => (
                <KPICard
                  key={card.title}
                  title={card.title}
                  value={card.value}
                  subtitle={card.subtitle}
                  change={card.change}
                  changeLabel={card.changeLabel}
                  icon={card.icon}
                  variant={card.variant}
                  footer={card.footer}
                />
              ))}
            </section>

            {activeTab === "descriptive" ? (
              <DescriptiveTab
                data={dashboard.tab1Data}
                snapshot={dashboard.snapshot}
                loading={dashboard.tab1Loading}
                onReplayFrameChange={handleDescriptiveReplayFrameChange}
                error={dashboard.tab1Error}
                selectedMonth={dashboard.selectedMonth}
                dimension={dashboard.tab1Dimension}
                onDimensionChange={dashboard.setTab1Dimension}
                segmentFilter={dashboard.segmentFilter}
                onToggleSegmentFilter={dashboard.toggleSegmentFilter}
                onClearFilter={dashboard.clearSegmentFilter}
              />
            ) : null}

            {activeTab === "predictive" ? (
              <PredictiveTab
                data={dashboard.tab2Data}
                loading={dashboard.tab2Loading}
                error={dashboard.tab2Error}
                selectedMonth={dashboard.selectedMonth}
                currentFilterLabel={dashboard.currentFilterLabel}
                modelParams={dashboard.modelParams}
                onModelParamChange={dashboard.updateModelParam}
              />
            ) : null}

            {activeTab === "prescriptive" ? (
              <PrescriptiveTab
                data={dashboard.tab3Data}
                loading={dashboard.tab3Loading}
                error={dashboard.tab3Error}
                selectedMonth={dashboard.selectedMonth}
                currentFilterLabel={dashboard.currentFilterLabel}
                scenarioInputs={dashboard.scenarioInputs}
                selectedScenarioId={dashboard.selectedScenarioId}
                onSelectScenario={dashboard.selectScenarioPreset}
                onScenarioInputChange={dashboard.updateScenarioInput}
              />
            ) : null}
          </main>
        </div>
      </div>
    </div>
  );
}

function OverviewCard({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="rounded-[22px] border border-slate-200/80 bg-slate-50/72 px-4 py-3">
      <p className="text-[11px] font-semibold uppercase tracking-[0.2em] text-muted-foreground">{label}</p>
      <p className="mt-2 font-display text-[1.55rem] font-semibold tracking-[-0.05em] text-slate-950">{value}</p>
      <p className="mt-1.5 text-sm leading-6 text-slate-600">{note}</p>
    </div>
  );
}
