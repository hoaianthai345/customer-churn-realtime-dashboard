import { BarChart3, BrainCircuit, LoaderCircle, Wand2 } from "lucide-react";
import { cn } from "@/lib/utils";

export type TabId = "descriptive" | "predictive" | "prescriptive";

export type TabNavigationItem = {
  id: TabId;
  label: string;
  description: string;
  badge: string;
  state: "ready" | "loading" | "missing" | "standby";
};

interface TabNavigationProps {
  activeTab: TabId;
  onTabChange: (tab: TabId) => void;
  items: TabNavigationItem[];
}

const tabs: { id: TabId; icon: typeof BarChart3 }[] = [
  { id: "descriptive", icon: BarChart3 },
  { id: "predictive", icon: BrainCircuit },
  { id: "prescriptive", icon: Wand2 },
];

export default function TabNavigation({ activeTab, onTabChange, items }: TabNavigationProps) {
  return (
    <div className="flex flex-col gap-2">
      {tabs.map((tab) => {
        const item = items.find((entry) => entry.id === tab.id);
        if (!item) return null;
        const isActive = activeTab === tab.id;
        const isLoading = item.state === "loading";
        const badgeClass = isActive
          ? "bg-white/12 text-white"
          : item.state === "loading"
            ? "bg-cyan-50 text-cyan-700"
            : item.state === "missing"
              ? "bg-rose-50 text-rose-700"
              : item.state === "standby"
                ? "bg-amber-50 text-amber-700"
                : "bg-slate-100 text-slate-600";
        return (
          <button
            key={tab.id}
            onClick={() => onTabChange(tab.id)}
            className={cn(
              "flex items-center gap-3 rounded-[22px] border px-4 py-4 text-left transition-all duration-200",
              isActive
                ? "border-slate-900 bg-slate-950 text-white shadow-[0_20px_40px_-24px_rgba(15,23,42,0.55)]"
                : "border-white/70 bg-white/80 text-foreground shadow-[0_16px_32px_-30px_rgba(15,23,42,0.35)] hover:border-slate-200 hover:bg-white",
            )}
          >
            <div className={cn("flex h-11 w-11 items-center justify-center rounded-2xl", isActive ? "bg-white/12" : "bg-slate-100")}>
              <tab.icon className="h-5 w-5" />
            </div>
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className={cn("font-display text-lg font-semibold tracking-[-0.03em]", isActive ? "text-white" : "text-foreground")}>
                  {item.label}
                </span>
                {isLoading ? <LoaderCircle className="h-3.5 w-3.5 animate-spin" /> : null}
              </div>
              <p className={cn("mt-1 text-sm", isActive ? "text-white/75" : "text-muted-foreground")}>{item.description}</p>
            </div>
            <span
              className={cn(
                "inline-flex min-w-14 items-center justify-center rounded-full px-3 py-1 text-xs font-semibold",
                badgeClass,
              )}
            >
              {item.badge}
            </span>
          </button>
        );
      })}
    </div>
  );
}
