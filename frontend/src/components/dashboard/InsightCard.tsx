import type { ReactNode } from "react";
import { AlertTriangle, CheckCircle2, Lightbulb, LucideIcon, TrendingDown } from "lucide-react";
import { cn } from "@/lib/utils";

interface InsightCardProps {
  type: "warning" | "insight" | "action" | "success";
  title: string;
  description: ReactNode;
  className?: string;
}

const config: Record<string, { icon: LucideIcon; borderColor: string; bgColor: string; iconColor: string }> = {
  warning: { icon: AlertTriangle, borderColor: "border-l-risk-high", bgColor: "bg-red-50/50", iconColor: "text-risk-high" },
  insight: { icon: Lightbulb, borderColor: "border-l-chart-blue", bgColor: "bg-blue-50/50", iconColor: "text-chart-blue" },
  action: { icon: CheckCircle2, borderColor: "border-l-primary", bgColor: "bg-accent/50", iconColor: "text-primary" },
  success: { icon: TrendingDown, borderColor: "border-l-risk-low", bgColor: "bg-emerald-50/50", iconColor: "text-risk-low" },
};

export default function InsightCard({ type, title, description, className = "" }: InsightCardProps) {
  const { icon: Icon, borderColor, bgColor, iconColor } = config[type];
  return (
    <div className={cn("rounded-[24px] border border-white/70 p-5 shadow-[0_16px_40px_-32px_rgba(15,23,42,0.45)]", borderColor, bgColor, className)}>
      <div className="flex gap-3">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-2xl bg-white/80">
          <Icon className={`h-4 w-4 shrink-0 ${iconColor}`} />
        </div>
        <div>
          <p className="font-display text-lg font-semibold tracking-[-0.03em] text-foreground">{title}</p>
          <div className="mt-2 text-sm leading-6 text-muted-foreground">{description}</div>
        </div>
      </div>
    </div>
  );
}
