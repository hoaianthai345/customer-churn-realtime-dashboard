export function SkeletonKPI() {
  return (
    <div className="glass-card rounded-xl p-5 space-y-3">
      <div className="flex justify-between">
        <div className="space-y-2">
          <div className="skeleton-pulse h-3 w-20" />
          <div className="skeleton-pulse h-7 w-28" />
          <div className="skeleton-pulse h-3 w-16" />
        </div>
        <div className="skeleton-pulse h-10 w-10 rounded-lg" />
      </div>
      <div className="skeleton-pulse h-3 w-24" />
    </div>
  );
}

export function SkeletonChart({ height = "h-64" }: { height?: string }) {
  return (
    <div className="chart-container">
      <div className="flex justify-between mb-4">
        <div className="space-y-1">
          <div className="skeleton-pulse h-4 w-32" />
          <div className="skeleton-pulse h-3 w-48" />
        </div>
        <div className="skeleton-pulse h-6 w-6 rounded" />
      </div>
      <div className={`skeleton-pulse ${height} w-full rounded-lg`} />
    </div>
  );
}
