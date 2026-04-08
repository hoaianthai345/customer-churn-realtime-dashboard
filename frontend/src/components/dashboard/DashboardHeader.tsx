import { Activity, CalendarRange, RefreshCw, Rocket, Waves } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { formatMonthLabel, type ReplayStatus } from "@/lib/dashboard";

type DashboardHeaderProps = {
  selectedMonth: string;
  monthOptions: string[];
  onMonthChange: (month: string) => void;
  onRefresh: () => void;
  isRefreshing?: boolean;
  demoMode: boolean;
  dataModeLabel: string;
  lastUpdatedLabel: string;
  currentFilterLabel: string;
  replayStatus: ReplayStatus | null;
  replayBusy: boolean;
  replayProgressPct: number;
  onReplay: () => void;
};

const modeToneMap = {
  "Ảnh chụp theo kỳ": "bg-amber-100 text-amber-800",
  "Trực tuyến": "bg-emerald-100 text-emerald-800",
  "Đồng bộ định kỳ": "bg-sky-100 text-sky-800",
};

const replayStatusMap: Record<NonNullable<ReplayStatus["status"]>, string> = {
  idle: "Sẵn sàng",
  queued: "Đang chờ chạy",
  running: "Đang chạy",
  succeeded: "Đã hoàn tất",
  failed: "Có lỗi",
};

function formatReplayStep(step: string | null | undefined): string | null {
  if (!step) return null;

  const knownSteps: Record<string, string> = {
    start_failed: "khởi động không thành công",
    bootstrap_members: "nạp dữ liệu khách hàng",
    replay_transactions: "phát lại giao dịch",
    replay_user_logs: "phát lại hành vi nghe nhạc",
    materialize_snapshot: "cập nhật ảnh chụp dữ liệu",
    finished: "đã xong",
  };

  return knownSteps[step] ?? step.replaceAll("_", " ");
}

export default function DashboardHeader({
  selectedMonth,
  monthOptions,
  onMonthChange,
  onRefresh,
  isRefreshing,
  demoMode,
  dataModeLabel,
  lastUpdatedLabel,
  currentFilterLabel,
  replayStatus,
  replayBusy,
  replayProgressPct,
  onReplay,
}: DashboardHeaderProps) {
  const toneClass = modeToneMap[dataModeLabel as keyof typeof modeToneMap] ?? "bg-slate-100 text-slate-700";
  const replayMessage = replayStatus
    ? [replayStatusMap[replayStatus.status], formatReplayStep(replayStatus.step)].filter(Boolean).join(" • ")
    : "Sẵn sàng";
  const displayMonth = selectedMonth ? formatMonthLabel(selectedMonth) : "Đang chờ";

  return (
    <header className="overflow-hidden rounded-[34px] border border-white/70 bg-white/85 shadow-[0_26px_70px_-42px_rgba(15,23,42,0.5)] backdrop-blur-xl">
      <div className="relative overflow-hidden px-6 py-6 sm:px-8">
        <div className="absolute inset-y-0 right-0 hidden w-[34rem] bg-[radial-gradient(circle_at_center,rgba(34,211,238,0.12),transparent_62%)] lg:block" />

        <div className="relative flex flex-col gap-6">
          <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
            <div className="max-w-3xl">
              <div className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white/80 px-3 py-1 text-xs font-semibold uppercase tracking-[0.24em] text-slate-500">
                <Waves className="h-3.5 w-3.5" />
                Bảng điều khiển giữ chân khách hàng
              </div>
              <h1 className="mt-4 font-display text-3xl font-semibold tracking-[-0.05em] text-slate-950 sm:text-4xl">
                Bảng điều hành rời bỏ khách hàng KKBOX
              </h1>
              <p className="mt-3 max-w-2xl text-sm leading-7 text-slate-600 sm:text-base">
                Theo dõi rủi ro rời bỏ, doanh thu dễ mất và thứ tự ưu tiên hành động theo từng tháng phân tích.
              </p>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 xl:min-w-[420px]">
              <div className="rounded-[24px] border border-slate-200 bg-slate-50/80 p-4">
                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                  <Activity className="h-3.5 w-3.5" />
                  Nguồn dữ liệu
                </div>
                <div className="mt-3 flex items-center justify-between gap-3">
                  <span className={`inline-flex rounded-full px-3 py-1 text-xs font-semibold ${toneClass}`}>{dataModeLabel}</span>
                  <span className="text-xs text-slate-500">Cập nhật theo nguồn dữ liệu hiện hành</span>
                </div>
                <p className="mt-3 text-sm text-slate-600">Cập nhật gần nhất: {lastUpdatedLabel}</p>
              </div>

              <div className="rounded-[24px] border border-slate-200 bg-slate-50/80 p-4">
                <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                  <Rocket className="h-3.5 w-3.5" />
                  Phạm vi phân tích
                </div>
                <p className="mt-3 font-display text-xl font-semibold tracking-[-0.04em] text-slate-950">{displayMonth}</p>
                <p className="mt-2 text-sm leading-6 text-slate-600">{currentFilterLabel}</p>
              </div>
            </div>
          </div>

          <div className="flex flex-col gap-4 border-t border-slate-200/80 pt-5 xl:flex-row xl:items-center xl:justify-between">
            <div className="grid gap-3 md:grid-cols-[minmax(240px,280px)_1fr] xl:min-w-[560px]">
              <div className="rounded-[22px] border border-slate-200 bg-slate-50/80 px-4 py-3">
                <div className="mb-2 flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">
                  <CalendarRange className="h-3.5 w-3.5" />
                  Tháng dữ liệu
                </div>
                <Select value={selectedMonth} onValueChange={onMonthChange} disabled={monthOptions.length === 0}>
                  <SelectTrigger className="h-10 border-0 bg-transparent px-0 font-medium text-slate-950 shadow-none focus:ring-0">
                    <SelectValue placeholder="Chọn tháng dữ liệu" />
                  </SelectTrigger>
                  <SelectContent>
                    {monthOptions.map((month) => (
                      <SelectItem key={month} value={month}>
                        {formatMonthLabel(month)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="rounded-[22px] border border-slate-200 bg-slate-50/80 px-4 py-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Tình trạng dữ liệu</p>
                    <p className="mt-2 text-sm text-slate-600">{replayMessage}</p>
                  </div>
                  {!demoMode ? (
                    <Button onClick={onReplay} disabled={replayBusy} className="rounded-full bg-slate-950 px-5 text-white hover:bg-slate-800">
                      {replayBusy ? "Đang chạy" : "Chạy lại dữ liệu"}
                    </Button>
                  ) : null}
                </div>
                {!demoMode ? <Progress className="mt-4 h-2 bg-slate-200" value={replayProgressPct} /> : null}
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {!demoMode ? (
                <Button
                  variant="outline"
                  onClick={onRefresh}
                  disabled={isRefreshing}
                  className="rounded-full border-slate-300 bg-white px-5 text-slate-700 hover:bg-slate-50"
                >
                  <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
                  Làm mới dữ liệu
                </Button>
              ) : null}
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
