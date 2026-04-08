import { AlertTriangle, DatabaseZap, LoaderCircle } from "lucide-react";

type StatePanelProps = {
  title: string;
  description: string;
  variant?: "loading" | "error" | "empty";
};

export default function StatePanel({ title, description, variant = "empty" }: StatePanelProps) {
  const Icon = variant === "loading" ? LoaderCircle : variant === "error" ? AlertTriangle : DatabaseZap;

  return (
    <div className="rounded-[28px] border border-dashed border-border bg-white/80 p-10 text-center shadow-[0_16px_40px_-28px_rgba(15,23,42,0.45)] backdrop-blur">
      <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-2xl bg-slate-950 text-white">
        <Icon className={`h-6 w-6 ${variant === "loading" ? "animate-spin" : ""}`} />
      </div>
      <h3 className="mt-5 font-display text-xl font-semibold text-foreground">{title}</h3>
      <p className="mx-auto mt-2 max-w-xl text-sm leading-6 text-muted-foreground">{description}</p>
    </div>
  );
}
