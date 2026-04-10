import { startTransition, useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  API_BASE,
  DEFAULT_DEMO_MONTH,
  DEFAULT_MODEL_PARAMS,
  DEFAULT_REPLAY_START_DATE,
  DEFAULT_SCENARIO_INPUTS,
  DEMO_MODE,
  SEGMENT_LABELS,
  buildSnapshotPulse,
  appendModelParams,
  appendSegmentFilter,
  formatMonthLabel,
  formatPulseDateLabel,
  formatTimestamp,
  hasTab1Data,
  hasTab2Data,
  hasTab3Data,
  normalizeMonths,
  resolveWsBase,
  toYearMonth,
  type ModelParamState,
  type PredictivePayload,
  type PrescriptivePayload,
  type ReplayStatus,
  type ScenarioInputs,
  type SegmentFilterState,
  type SegmentType,
  type SnapshotPayload,
  type Tab1Dimension,
  type Tab1Payload,
} from "@/lib/dashboard";
import type { TabId } from "@/components/dashboard/TabNavigation";

async function readJson<T>(input: RequestInfo | URL, init?: RequestInit): Promise<T> {
  const response = await fetch(input, init);
  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    const detail =
      typeof payload?.detail === "string"
        ? payload.detail
        : typeof payload?.message === "string"
          ? payload.message
          : "Không tải được dữ liệu";
    throw new Error(detail);
  }

  return payload as T;
}

export function useDashboardData(activeTab: TabId) {
  const [monthOptions, setMonthOptions] = useState<string[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string>(DEMO_MODE ? DEFAULT_DEMO_MONTH : "");
  const [snapshot, setSnapshot] = useState<SnapshotPayload | null>(null);
  const [snapshotError, setSnapshotError] = useState<string | null>(null);
  const [wsStatus, setWsStatus] = useState<"connecting" | "open" | "closed">("closed");
  const [replayStatus, setReplayStatus] = useState<ReplayStatus | null>(null);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [lastPolledAt, setLastPolledAt] = useState<string | null>(null);

  const [tab1Dimension, setTab1Dimension] = useState<Tab1Dimension>("age");
  const [tab1Data, setTab1Data] = useState<Tab1Payload | null>(null);
  const [tab1Loading, setTab1Loading] = useState(false);
  const [tab1Error, setTab1Error] = useState<string | null>(null);

  const [segmentFilter, setSegmentFilter] = useState<SegmentFilterState>({ segmentType: null, segmentValue: null });

  const [modelParams, setModelParams] = useState<ModelParamState>(DEFAULT_MODEL_PARAMS);
  const [tab2Data, setTab2Data] = useState<PredictivePayload | null>(null);
  const [tab2Loading, setTab2Loading] = useState(false);
  const [tab2Error, setTab2Error] = useState<string | null>(null);

  const [scenarioInputs, setScenarioInputs] = useState<ScenarioInputs>(DEFAULT_SCENARIO_INPUTS);
  const [selectedScenarioId, setSelectedScenarioId] = useState<string | null>(null);
  const [tab3Data, setTab3Data] = useState<PrescriptivePayload | null>(null);
  const [tab3Loading, setTab3Loading] = useState(false);
  const [tab3Error, setTab3Error] = useState<string | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const lastReplayStatusRef = useRef<string>("idle");

  const hasMonth = selectedMonth.length === 7;
  const yearMonth = useMemo(() => (hasMonth ? toYearMonth(selectedMonth) : null), [hasMonth, selectedMonth]);
  const monthLabel = useMemo(() => formatMonthLabel(selectedMonth), [selectedMonth]);
  const tab1HasData = useMemo(() => hasTab1Data(tab1Data), [tab1Data]);
  const tab2HasData = useMemo(() => hasTab2Data(tab2Data), [tab2Data]);
  const tab3HasData = useMemo(() => hasTab3Data(tab3Data), [tab3Data]);
  const snapshotPulse = useMemo(() => buildSnapshotPulse(snapshot), [snapshot]);
  const snapshotPulseLastDateLabel = useMemo(() => {
    const lastPoint = snapshotPulse[snapshotPulse.length - 1] ?? null;
    return lastPoint ? formatPulseDateLabel(lastPoint.event_date) : null;
  }, [snapshotPulse]);
  const currentFilterLabel = useMemo(() => {
    if (!segmentFilter.segmentType || !segmentFilter.segmentValue) return "Toàn bộ khách hàng";
    return `${SEGMENT_LABELS[segmentFilter.segmentType]}: ${segmentFilter.segmentValue}`;
  }, [segmentFilter.segmentType, segmentFilter.segmentValue]);
  const dataModeLabel = DEMO_MODE ? "Ảnh chụp theo kỳ" : wsStatus === "open" ? "Trực tuyến" : "Đồng bộ định kỳ";
  const lastUpdatedCaption =
    (snapshot?.meta.series_mode === "pre_expiry_context" || snapshot?.meta.series_mode === "expire_day_proxy") &&
    snapshotPulseLastDateLabel
      ? "Dữ liệu hiển thị đến"
      : "Cập nhật gần nhất";
  const lastUpdatedLabel = useMemo(
    () =>
      (snapshot?.meta.series_mode === "pre_expiry_context" || snapshot?.meta.series_mode === "expire_day_proxy") &&
      snapshotPulseLastDateLabel
        ? snapshotPulseLastDateLabel
        : formatTimestamp(snapshot?.meta.as_of ?? lastPolledAt),
    [lastPolledAt, snapshot?.meta.as_of, snapshot?.meta.series_mode, snapshotPulseLastDateLabel],
  );
  const replayBusy = replayStatus?.status === "queued" || replayStatus?.status === "running";
  const replayProgressPct = Math.round((replayStatus?.progress ?? 0) * 100);

  const loadMonthOptions = () => {
    Promise.all([
      readJson<{ months: string[] }>(`${API_BASE}/api/v1/month-options`).catch(() => ({ months: [] })),
      readJson<{ months: string[] }>(`${API_BASE}/api/v1/tab1/month-options`).catch(() => ({ months: [] })),
    ])
      .then(([kpiData, tab1MonthData]) => {
        const kpiMonths = Array.isArray(kpiData.months) ? kpiData.months : [];
        const tab1Months = Array.isArray(tab1MonthData.months) ? tab1MonthData.months : [];
        const months = normalizeMonths(kpiMonths, tab1Months);
        const effectiveMonths = months.length === 0 && DEMO_MODE ? [DEFAULT_DEMO_MONTH] : months;
        setMonthOptions(effectiveMonths);

        if (!selectedMonth && effectiveMonths.length > 0) {
          setSelectedMonth(effectiveMonths[0]);
        } else if (selectedMonth && !effectiveMonths.includes(selectedMonth) && effectiveMonths.length > 0) {
          setSelectedMonth(effectiveMonths[0]);
        }
      })
      .catch(() => {
        if (DEMO_MODE) {
          setMonthOptions([DEFAULT_DEMO_MONTH]);
          if (!selectedMonth) {
            setSelectedMonth(DEFAULT_DEMO_MONTH);
          }
        } else {
          setMonthOptions([]);
        }
      });
  };

  const loadReplayStatus = () => {
    readJson<ReplayStatus>(`${API_BASE}/api/v1/replay/status`)
      .then((payload) => {
        const previousStatus = lastReplayStatusRef.current;
        lastReplayStatusRef.current = payload.status;
        setReplayStatus(payload);

        if (previousStatus !== "succeeded" && payload.status === "succeeded") {
          loadMonthOptions();
        }
      })
      .catch(() => undefined);
  };

  useEffect(() => {
    loadMonthOptions();
    if (!DEMO_MODE) {
      loadReplayStatus();
    }
  }, []);

  useEffect(() => {
    if (DEMO_MODE) {
      return;
    }
    const timer = window.setInterval(() => {
      setRefreshVersion((value) => value + 1);
      loadReplayStatus();
    }, 30000);

    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!yearMonth) return;

    const url = `${API_BASE}/api/v1/dashboard/snapshot?year=${yearMonth.year}&month=${yearMonth.month}`;
    readJson<SnapshotPayload>(url)
      .then((payload) => {
        setSnapshot(payload);
        setSnapshotError(null);
        setLastPolledAt(new Date().toISOString());
      })
      .catch((error: unknown) => {
        setSnapshotError(error instanceof Error ? error.message : "Không tải được ảnh chụp dữ liệu");
      });
  }, [refreshVersion, yearMonth]);

  useEffect(() => {
    setSelectedScenarioId(null);
  }, [selectedMonth]);

  useEffect(() => {
    if (!yearMonth) return;

    const params = new URLSearchParams({
      year: String(yearMonth.year),
      month: String(yearMonth.month),
      dimension: tab1Dimension,
    });
    appendSegmentFilter(params, segmentFilter);

    setTab1Loading(true);
    readJson<Tab1Payload>(`${API_BASE}/api/v1/tab1/descriptive?${params.toString()}`)
      .then((payload) => {
        setTab1Data(payload);
        setTab1Error(null);
      })
      .catch((error: unknown) => {
        setTab1Data(null);
        setTab1Error(error instanceof Error ? error.message : "Không tải được dữ liệu tab hiện trạng");
      })
      .finally(() => setTab1Loading(false));
  }, [refreshVersion, yearMonth, tab1Dimension, segmentFilter.segmentType, segmentFilter.segmentValue]);

  useEffect(() => {
    if (activeTab !== "predictive") return;
    if (!yearMonth) return;

    const params = new URLSearchParams({
      year: String(yearMonth.year),
      month: String(yearMonth.month),
      sample_limit: "120000",
    });
    if (!DEMO_MODE) {
      appendSegmentFilter(params, segmentFilter);
    }
    appendModelParams(params, modelParams);

    setTab2Loading(true);
    readJson<PredictivePayload>(`${API_BASE}/api/v1/tab2/predictive?${params.toString()}`)
      .then((payload) => {
        setTab2Data(payload);
        setTab2Error(null);
      })
      .catch((error: unknown) => {
        setTab2Data(null);
        setTab2Error(error instanceof Error ? error.message : "Không tải được dữ liệu tab dự báo");
      })
      .finally(() => setTab2Loading(false));
  }, [
    activeTab,
    refreshVersion,
    yearMonth,
    segmentFilter.segmentType,
    segmentFilter.segmentValue,
    modelParams.base_prob,
    modelParams.weight_manual,
    modelParams.weight_low_activity,
    modelParams.weight_high_skip,
    modelParams.weight_low_discovery,
    modelParams.weight_cancel_signal,
    modelParams.prob_min,
    modelParams.prob_max,
    modelParams.cltv_base_months,
    modelParams.cltv_retention_months,
    modelParams.cltv_txn_gain,
    modelParams.risk_horizon_months,
    modelParams.hazard_base,
    modelParams.hazard_churn_weight,
    modelParams.hazard_skip_weight,
    modelParams.hazard_low_activity_weight,
  ]);

  useEffect(() => {
    if (activeTab !== "prescriptive") return;
    if (!yearMonth) return;

    const params = new URLSearchParams({
      year: String(yearMonth.year),
      month: String(yearMonth.month),
      sample_limit: "120000",
      auto_shift_pct: String(scenarioInputs.auto_shift_pct),
      upsell_shift_pct: String(scenarioInputs.upsell_shift_pct),
      skip_shift_pct: String(scenarioInputs.skip_shift_pct),
    });
    if (selectedScenarioId) {
      params.set("scenario_id", selectedScenarioId);
    }
    if (!DEMO_MODE) {
      appendSegmentFilter(params, segmentFilter);
    }
    appendModelParams(params, modelParams);

    setTab3Loading(true);
    readJson<PrescriptivePayload>(`${API_BASE}/api/v1/tab3/prescriptive?${params.toString()}`)
      .then((payload) => {
        setTab3Data(payload);
        setTab3Error(null);
        const payloadScenarioId = payload.meta?.scenario_id ?? null;
        if (payloadScenarioId && payloadScenarioId !== selectedScenarioId) {
          setSelectedScenarioId(payloadScenarioId);
        }
        const nextInputs = payload.scenario_inputs;
        setScenarioInputs((prev) =>
          prev.auto_shift_pct === nextInputs.auto_shift_pct &&
          prev.upsell_shift_pct === nextInputs.upsell_shift_pct &&
          prev.skip_shift_pct === nextInputs.skip_shift_pct
            ? prev
            : nextInputs,
        );
      })
      .catch((error: unknown) => {
        setTab3Data(null);
        setTab3Error(error instanceof Error ? error.message : "Không tải được dữ liệu tab kịch bản");
      })
      .finally(() => setTab3Loading(false));
  }, [
    activeTab,
    refreshVersion,
    yearMonth,
    segmentFilter.segmentType,
    segmentFilter.segmentValue,
    selectedScenarioId,
    scenarioInputs.auto_shift_pct,
    scenarioInputs.upsell_shift_pct,
    scenarioInputs.skip_shift_pct,
    modelParams.base_prob,
    modelParams.weight_manual,
    modelParams.weight_low_activity,
    modelParams.weight_high_skip,
    modelParams.weight_low_discovery,
    modelParams.weight_cancel_signal,
    modelParams.prob_min,
    modelParams.prob_max,
    modelParams.cltv_base_months,
    modelParams.cltv_retention_months,
    modelParams.cltv_txn_gain,
    modelParams.risk_horizon_months,
    modelParams.hazard_base,
    modelParams.hazard_churn_weight,
    modelParams.hazard_skip_weight,
    modelParams.hazard_low_activity_weight,
  ]);

  useEffect(() => {
    if (!yearMonth) return;
    if (DEMO_MODE) {
      setWsStatus("closed");
      return;
    }

    if (wsRef.current) {
      wsRef.current.close();
    }

    const wsUrl = `${resolveWsBase()}/ws/kpi?year=${yearMonth.year}&month=${yearMonth.month}`;
    setWsStatus("connecting");
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setWsStatus("open");
    ws.onclose = () => setWsStatus("closed");
    ws.onerror = () => setWsStatus("closed");
    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data) as SnapshotPayload;
        if (payload?.metrics) {
          setSnapshot(payload);
        }
      } catch {
        // Ignore malformed payloads from the websocket channel.
      }
    };

    return () => {
      ws.close();
    };
  }, [yearMonth]);

  const updateModelParam = (key: keyof ModelParamState, value: number) => {
    setModelParams((prev) => ({ ...prev, [key]: value }));
  };

  const updateScenarioInput = (key: keyof ScenarioInputs, value: number) => {
    if (DEMO_MODE) {
      return;
    }
    setScenarioInputs((prev) => ({ ...prev, [key]: value }));
  };

  const selectScenarioPreset = useCallback((scenarioId: string) => {
    if (scenarioId === selectedScenarioId) {
      return;
    }

    const presets = tab3Data?.meta?.available_scenarios ?? [];
    const matchedPreset = presets.find((preset) => preset.scenario_id === scenarioId);
    startTransition(() => {
      setSelectedScenarioId(scenarioId);
      if (matchedPreset) {
        setScenarioInputs(matchedPreset.scenario_inputs);
      }
    });
  }, [selectedScenarioId, tab3Data]);

  const toggleSegmentFilter = useCallback((segmentType: SegmentType, segmentValue: string) => {
    setSegmentFilter((prev) => {
      if (prev.segmentType === segmentType && prev.segmentValue === segmentValue) {
        return { segmentType: null, segmentValue: null };
      }

      return { segmentType, segmentValue };
    });
  }, []);

  const clearSegmentFilter = useCallback(() => {
    setSegmentFilter({ segmentType: null, segmentValue: null });
  }, []);

  const triggerReplay = async () => {
    try {
      const params = new URLSearchParams({
        force_reset: "true",
        replay_start_date: DEFAULT_REPLAY_START_DATE,
      });
      const payload = await readJson<ReplayStatus>(`${API_BASE}/api/v1/replay/start?${params.toString()}`, {
        method: "POST",
      });
      setReplayStatus(payload);
    } catch (error: unknown) {
      const errorText = error instanceof Error ? error.message : "Không thể chạy lại dữ liệu";
      setReplayStatus((prev) => ({
        status: "failed",
        step: prev?.step ?? "start_failed",
        started_at: prev?.started_at ?? null,
        finished_at: new Date().toISOString(),
        duration_sec: prev?.duration_sec ?? null,
        error: errorText,
        progress: prev?.progress ?? 0,
        replay_start_date: prev?.replay_start_date ?? DEFAULT_REPLAY_START_DATE,
        force_reset: true,
      }));
    }
  };

  const triggerRefresh = () => {
    setRefreshVersion((value) => value + 1);
  };

  return {
    demoMode: DEMO_MODE,
    defaultReplayStartDate: DEFAULT_REPLAY_START_DATE,
    monthOptions,
    selectedMonth,
    setSelectedMonth,
    snapshot,
    snapshotError,
    wsStatus,
    replayStatus,
    refreshVersion,
    lastPolledAt,
    lastUpdatedCaption,
    lastUpdatedLabel,
    dataModeLabel,
    replayBusy,
    replayProgressPct,
    monthLabel,
    yearMonth,
    hasMonth,
    tab1Dimension,
    setTab1Dimension,
    tab1Data,
    tab1Loading,
    tab1Error,
    tab1HasData,
    segmentFilter,
    setSegmentFilter,
    currentFilterLabel,
    toggleSegmentFilter,
    clearSegmentFilter,
    modelParams,
    updateModelParam,
    tab2Data,
    tab2Loading,
    tab2Error,
    tab2HasData,
    scenarioInputs,
    updateScenarioInput,
    selectedScenarioId,
    selectScenarioPreset,
    tab3Data,
    tab3Loading,
    tab3Error,
    tab3HasData,
    triggerReplay,
    triggerRefresh,
  };
}
