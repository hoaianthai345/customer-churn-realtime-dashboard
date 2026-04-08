import type { ReactNode } from "react";
import { LucideIcon, Minus, TrendingDown, TrendingUp } from "lucide-react";
import { cn } from "@/lib/utils";

interface KPICardProps {
  title: string;
  value: string;
  subtitle?: string;
  change?: number;
  changeLabel?: string;
  icon: LucideIcon;
  variant?: "default" | "danger" | "success" | "warning" | "accent";
  footer?: ReactNode;
  className?: string;
}

const variantStyles = {
  default: "border-slate-200 bg-white text-foreground",
  danger: "border-rose-200 bg-[linear-gradient(145deg,rgba(255,255,255,1),rgba(255,238,242,0.9))] text-foreground",
  success: "border-emerald-200 bg-[linear-gradient(145deg,rgba(255,255,255,1),rgba(236,253,245,0.92))] text-foreground",
  warning: "border-amber-200 bg-[linear-gradient(145deg,rgba(255,255,255,1),rgba(255,247,237,0.92))] text-foreground",
  accent: "border-cyan-200 bg-[linear-gradient(145deg,rgba(255,255,255,1),rgba(236,254,255,0.95))] text-foreground",
};

const iconVariantStyles = {
  default: "bg-slate-950 text-white",
  danger: "bg-rose-100 text-risk-high",
  success: "bg-emerald-100 text-risk-low",
  warning: "bg-amber-100 text-risk-medium",
  accent: "bg-cyan-100 text-cyan-700",
};

export default function KPICard({
  title,
  value,
  subtitle,
  change,
  changeLabel,
  icon: Icon,
  variant = "default",
  footer,
  className = "",
}: KPICardProps) {
  const TrendIcon = change === undefined ? Minus : change > 0 ? TrendingUp : change < 0 ? TrendingDown : Minus;
  const trendColor =
    change === undefined
      ? "text-muted-foreground"
      : change > 0
        ? variant === "success"
          ? "text-risk-low"
          : "text-risk-high"
        : change < 0
          ? variant === "danger"
            ? "text-risk-low"
            : "text-risk-low"
          : "text-muted-foreground";

  return (
    <article
      className={cn(
        "group rounded-[26px] border p-5 shadow-[0_18px_42px_-30px_rgba(15,23,42,0.35)] transition-all duration-200 hover:-translate-y-0.5 hover:shadow-[0_28px_50px_-30px_rgba(15,23,42,0.45)]",
        variantStyles[variant],
        className,
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">{title}</p>
          <p className="font-display text-[2rem] font-semibold tracking-[-0.04em] text-foreground">{value}</p>
          {subtitle ? <p className="max-w-[24ch] text-sm leading-5 text-muted-foreground">{subtitle}</p> : null}
        </div>
        <div className={cn("rounded-2xl p-3 shadow-sm", iconVariantStyles[variant])}>
          <Icon className="h-5 w-5" />
        </div>
      </div>
      {change !== undefined && (
        <div className={cn("mt-4 flex items-center gap-1.5 text-xs font-medium", trendColor)}>
          <TrendIcon className="h-3.5 w-3.5" />
          <span>{Math.abs(change).toFixed(1)}%</span>
          {changeLabel ? <span className="font-normal text-muted-foreground">so với {changeLabel}</span> : null}
        </div>
      )}
      {footer ? <div className="mt-4 border-t border-black/5 pt-4 text-xs text-muted-foreground">{footer}</div> : null}
    </article>
  );
}
