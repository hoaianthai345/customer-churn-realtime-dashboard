import { memo, useEffect, useMemo, useState } from "react";
import { Pause, Play, RotateCcw, SkipForward } from "lucide-react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import ChartCard from "@/components/dashboard/ChartCard";
import StatePanel from "@/components/dashboard/StatePanel";
import { Button } from "@/components/ui/button";
import {
  DEMO_MODE,
  buildSnapshotPulse,
  formatCompactCurrency,
  formatCurrency,
  formatMonthLabel,
  formatNumber,
  formatPct,
  formatPulseDateLabel,
  type PulseReplayFrame,
  type PulseReplayStatus,
  type SnapshotPayload,
} from "@/lib/dashboard";

type DescriptivePulsePanelProps = {
  onReplayFrameChange?: (frame: PulseReplayFrame) => void;
  snapshot: SnapshotPayload | null;
};

function DescriptivePulsePanel({ onReplayFrameChange, snapshot }: DescriptivePulsePanelProps) {
  const pulseData = useMemo(() => buildSnapshotPulse(snapshot), [snapshot]);
  const [replayIndex, setReplayIndex] = useState(0);
  const [replayStatus, setReplayStatus] = useState<PulseReplayStatus>("idle");
  const targetMonthLabel = useMemo(() => formatMonthLabel(snapshot?.meta.month), [snapshot?.meta.month]);
  const contextMonthLabel = useMemo(
    () => formatMonthLabel(snapshot?.meta.context_month ?? snapshot?.meta.month),
    [snapshot?.meta.context_month, snapshot?.meta.month],
  );
  const usesPreExpiryContext =
    snapshot?.meta.series_mode === "pre_expiry_context" &&
    !!snapshot?.meta.context_month &&
    snapshot.meta.context_month !== snapshot.meta.month;
  const usesExpireDayProxy = snapshot?.meta.series_mode === "expire_day_proxy";

  useEffect(() => {
    if (!pulseData.length) {
      setReplayIndex(0);
      setReplayStatus("idle");
      return;
    }

    if (!DEMO_MODE || pulseData.length === 1) {
      setReplayIndex(pulseData.length - 1);
      setReplayStatus("completed");
      return;
    }

    setReplayIndex(0);
    setReplayStatus("playing");
  }, [pulseData]);

  useEffect(() => {
    if (replayStatus !== "playing" || !DEMO_MODE || pulseData.length <= 1) {
      return;
    }

    const lastIndex = pulseData.length - 1;
    if (replayIndex >= lastIndex) {
      setReplayStatus("completed");
      return;
    }

    const timerId = window.setTimeout(() => {
      setReplayIndex((current) => Math.min(current + 1, lastIndex));
    }, 500);
    return () => window.clearTimeout(timerId);
  }, [pulseData, replayIndex, replayStatus]);

  const visiblePulseData = useMemo(() => {
    if (!pulseData.length) {
      return [];
    }
    if (DEMO_MODE && pulseData.length > 1) {
      return pulseData.slice(0, replayIndex + 1);
    }
    return pulseData;
  }, [pulseData, replayIndex]);

  const currentPulsePoint = visiblePulseData[visiblePulseData.length - 1] ?? pulseData[pulseData.length - 1] ?? null;
  const currentReplayDateLabel = currentPulsePoint ? formatPulseDateLabel(currentPulsePoint.event_date) : null;
  const replayRunning = replayStatus === "playing";
  const replayCompleted = replayStatus === "completed";
  const replayPaused = replayStatus === "paused";
  const showReplayControls = DEMO_MODE && pulseData.length > 1;

  useEffect(() => {
    onReplayFrameChange?.({
      point: currentPulsePoint,
      dateLabel: currentReplayDateLabel,
      status: replayStatus,
    });
  }, [currentPulsePoint, currentReplayDateLabel, onReplayFrameChange, replayStatus]);

  const restartReplay = () => {
    if (!pulseData.length) {
      setReplayIndex(0);
      setReplayStatus("idle");
      return;
    }
    if (!showReplayControls) {
      setReplayIndex(pulseData.length - 1);
      setReplayStatus("completed");
      return;
    }
    setReplayIndex(0);
    setReplayStatus("playing");
  };

  const pauseReplay = () => {
    if (replayRunning) {
      setReplayStatus("paused");
    }
  };

  const resumeReplay = () => {
    if (!pulseData.length) {
      return;
    }
    if (replayIndex >= pulseData.length - 1) {
      setReplayStatus("completed");
      return;
    }
    setReplayStatus("playing");
  };

  const jumpToFinalDay = () => {
    if (!pulseData.length) {
      setReplayIndex(0);
      setReplayStatus("idle");
      return;
    }
    setReplayIndex(pulseData.length - 1);
    setReplayStatus("completed");
  };

  return (
    <ChartCard
      title="Nhịp vận hành bên trong tháng"
      subtitle={
        usesPreExpiryContext
          ? `${contextMonthLabel} là bối cảnh vận hành dùng để ra quyết định cho cohort sắp hết hạn của ${targetMonthLabel}.`
          : replayRunning
          ? "Mỗi 0,5 giây hệ thống chuyển sang ngày kế tiếp để mô phỏng cách số liệu vận động trong tháng."
          : "Đọc doanh thu theo ngày cùng với số khách nguy cơ cao để thấy nhịp biến động thực tế."
      }
    >
      {visiblePulseData.length ? (
        <div className="space-y-4">
          <div className="flex flex-wrap items-start justify-between gap-3 rounded-[22px] bg-slate-50 p-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted-foreground">
                {replayRunning ? "Ngày đang mô phỏng" : "Ngày mới nhất đang có"}
              </p>
              <p className="mt-2 font-display text-2xl font-semibold tracking-[-0.04em] text-slate-950">
                {currentReplayDateLabel ?? "Chưa có điểm ngày"}
              </p>
              <p className="mt-2 text-sm leading-6 text-slate-600">
                {usesPreExpiryContext
                  ? `Chuỗi này chạy theo ${contextMonthLabel}, là tháng liền trước ${targetMonthLabel}.`
                  : usesExpireDayProxy
                    ? "Chuỗi ngày hiện được dựng từ proxy expire_day bên trong tháng mục tiêu."
                    : "Chuỗi theo ngày đang phản ánh nhịp thay đổi bên trong tháng được chọn."}
              </p>
            </div>
            {replayRunning ? (
              <div className="rounded-full border border-slate-200 bg-white px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-700">
                0,5 giây / 1 ngày dữ liệu
              </div>
            ) : replayCompleted ? (
              <div className="rounded-full border border-emerald-200 bg-emerald-50 px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-700">
                Đã chạy xong
              </div>
            ) : replayPaused ? (
              <div className="rounded-full border border-amber-200 bg-amber-50 px-4 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-amber-700">
                Đang tạm dừng
              </div>
            ) : null}
          </div>

          {showReplayControls ? (
            <div className="flex flex-wrap gap-3">
              {replayRunning ? (
                <Button variant="outline" className="rounded-full border-slate-300" onClick={pauseReplay}>
                  <Pause className="h-4 w-4" />
                  Tạm dừng
                </Button>
              ) : replayPaused ? (
                <Button className="rounded-full bg-slate-950 text-white hover:bg-slate-800" onClick={resumeReplay}>
                  <Play className="h-4 w-4" />
                  Tiếp tục
                </Button>
              ) : null}

              {!replayRunning ? (
                <Button className="rounded-full bg-slate-950 text-white hover:bg-slate-800" onClick={restartReplay}>
                  {replayCompleted ? <RotateCcw className="h-4 w-4" /> : <Play className="h-4 w-4" />}
                  {replayCompleted ? "Chạy lại" : "Bắt đầu mô phỏng"}
                </Button>
              ) : null}

              <Button variant="outline" className="rounded-full border-slate-300" onClick={jumpToFinalDay}>
                <SkipForward className="h-4 w-4" />
                Ngày cuối
              </Button>
            </div>
          ) : null}

          {currentPulsePoint ? (
            <div className="grid gap-3 md:grid-cols-3">
              <div className="rounded-[22px] border border-slate-200 bg-white px-4 py-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">Doanh thu trong ngày</p>
                <p className="mt-2 font-display text-2xl font-semibold tracking-[-0.04em] text-slate-950">
                  {formatCurrency(Number(currentPulsePoint.total_revenue ?? 0))}
                </p>
                <p className="mt-2 text-sm text-slate-600">{formatNumber(Number(currentPulsePoint.total_transactions ?? 0))} lượt gia hạn phát sinh</p>
              </div>
              <div className="rounded-[22px] border border-slate-200 bg-white px-4 py-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">Khách nguy cơ cao</p>
                <p className="mt-2 font-display text-2xl font-semibold tracking-[-0.04em] text-slate-950">
                  {formatNumber(Number(currentPulsePoint.high_risk_users ?? 0))}
                </p>
                <p className="mt-2 text-sm text-slate-600">Mức rủi ro trung bình {formatPct(Number(currentPulsePoint.avg_risk_score ?? 0), 1)}</p>
              </div>
              <div className="rounded-[22px] border border-slate-200 bg-white px-4 py-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">Khách còn hoạt động</p>
                <p className="mt-2 font-display text-2xl font-semibold tracking-[-0.04em] text-slate-950">
                  {formatNumber(Number(currentPulsePoint.active_users ?? 0))}
                </p>
                <p className="mt-2 text-sm text-slate-600">
                  {formatNumber(Number(currentPulsePoint.total_listening_secs ?? 0))} giây nghe nhạc được ghi nhận
                </p>
              </div>
            </div>
          ) : null}

          <ResponsiveContainer width="100%" height={320}>
            <AreaChart data={visiblePulseData}>
              <defs>
                <linearGradient id="pulseRevenue" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#0f766e" stopOpacity={0.28} />
                  <stop offset="95%" stopColor="#0f766e" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.24)" />
              <XAxis dataKey="event_date" tick={{ fontSize: 12 }} />
              <YAxis yAxisId="left" tick={{ fontSize: 12 }} tickFormatter={(value) => formatCompactCurrency(Number(value))} />
              <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 12 }} />
              <Tooltip
                formatter={(value: number, name) => {
                  if (name === "total_revenue") return formatCurrency(Number(value));
                  return formatNumber(Number(value));
                }}
                labelFormatter={(value) => formatPulseDateLabel(String(value))}
              />
              <Legend />
              <Area
                yAxisId="left"
                type="monotone"
                dataKey="total_revenue"
                stroke="#0f766e"
                fill="url(#pulseRevenue)"
                name="Doanh thu"
                isAnimationActive={false}
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="high_risk_users"
                stroke="#e11d48"
                strokeWidth={2.5}
                dot={false}
                name="Khách nguy cơ cao"
                isAnimationActive={false}
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="active_users"
                stroke="#2563eb"
                strokeWidth={2}
                dot={false}
                name="Khách còn hoạt động"
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <StatePanel title="Chưa có chuỗi dữ liệu theo ngày" description="Bộ dữ liệu hiện tại chưa có đủ số liệu theo ngày để dựng nhịp vận hành trong tháng." />
      )}
    </ChartCard>
  );
}

export default memo(DescriptivePulsePanel);
