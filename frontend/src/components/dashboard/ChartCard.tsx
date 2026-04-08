import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface ChartCardProps {
  title: string;
  subtitle?: string;
  children: ReactNode;
  action?: ReactNode;
  className?: string;
}

export default function ChartCard({ title, subtitle, children, action, className = "" }: ChartCardProps) {
  return (
    <section
      className={cn(
        "flex min-h-[280px] flex-col rounded-[28px] border border-white/60 bg-white/88 p-4 shadow-[0_18px_40px_-30px_rgba(15,23,42,0.34)] backdrop-blur-xl sm:p-5",
        className,
      )}
    >
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <h3 className="font-display text-[1.05rem] font-semibold tracking-[-0.03em] text-foreground sm:text-xl">{title}</h3>
          {subtitle ? <p className="mt-1 max-w-[60ch] text-sm leading-6 text-muted-foreground">{subtitle}</p> : null}
        </div>
        {action}
      </div>
      <div className="min-h-0 flex-1">{children}</div>
    </section>
  );
}
