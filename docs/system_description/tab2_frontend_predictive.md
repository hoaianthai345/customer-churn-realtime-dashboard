# Tab 2 (Predictive) – Frontend Dataflow & Logic

Tài liệu này mô tả **Tab 2: Dự báo** ở **frontend**: dataflow, điều kiện nạp dữ liệu, và logic hiển thị chart/card.

## 1) Scope

- Chỉ cover **frontend UI + fetch + mapping dữ liệu vào chart**.
- Không cover chi tiết mô hình/feature engineering (xem `project-realtime-bi/docs/system_description/predictive.md` và `project-realtime-bi/docs/system_description/kkbox_tab2_predictive_pipeline.md`).

## 2) File “source of truth” ở frontend

- Wiring UI + tab routing: `project-realtime-bi/frontend/src/pages/Index.tsx`
- Data loading & state: `project-realtime-bi/frontend/src/hooks/useDashboardData.ts`
- Tab 2 UI (charts + logic): `project-realtime-bi/frontend/src/components/dashboard/PredictiveTab.tsx`
- Type contract payload: `project-realtime-bi/frontend/src/lib/dashboard.ts`

## 3) Tab 2 được nạp khi nào? (lazy-load)

Tab 2 **chỉ fetch khi user mở tab** (id `predictive`).

1. `Index.tsx` giữ state `activeTab` (`"descriptive" | "predictive" | "prescriptive"`).
2. `useDashboardData(activeTab)` có effect cho Tab 2:
   - `if (activeTab !== "predictive") return;`
   - `if (!yearMonth) return;`
3. Vì vậy, ở trạng thái chưa mở tab, UI sẽ hiện badge `"Chờ"`/`"Chờ mở"` và **không gọi API Tab 2**.

Điều này giải thích vì sao Tab 2 có “standby state” (không phải lỗi): tab chưa được kích hoạt để fetch.

## 4) Dataflow tổng quát (UI → API → charts)

### 4.1. Nguồn tham số đầu vào (frontend state)

Các state ảnh hưởng trực tiếp đến fetch Tab 2:

- `selectedMonth` → map sang `{ year, month }` bằng `toYearMonth("YYYY-MM")`.
- `segmentFilter` (global filter):
  - `segment_type`, `segment_value` sẽ được append vào query nếu **không phải demo mode**.
- `modelParams`:
  - luôn append đầy đủ key/value bằng `appendModelParams(...)`.
- `refreshVersion`:
  - ở non-demo, timer 30s sẽ tăng `refreshVersion` để polling.

### 4.2. Endpoint & query params

Frontend gọi:

- `GET ${API_BASE}/api/v1/tab2/predictive?${params}`

Trong đó `API_BASE = import.meta.env.VITE_API_BASE_URL ?? "/backend"`.

Các query chính:

- `year`, `month`
- `sample_limit=120000`
- (non-demo) `segment_type`, `segment_value`
- toàn bộ `modelParams.*` (VD: `base_prob`, `prob_min`, `cltv_base_months`, …)

### 4.3. Loading / error state

Trong `useDashboardData.ts`:

- set `tab2Loading=true` trước khi fetch
- on success: `tab2Data=payload`, `tab2Error=null`
- on failure: `tab2Data=null`, `tab2Error=...`
- finally: `tab2Loading=false`

Trong `PredictiveTab.tsx`:

- `initialLoading = loading && !data` → show `StatePanel(variant="loading")`
- `switchingDataset = loading && !!data` → giữ layout cũ và giảm opacity để báo đang chuyển dataset

## 5) Contract dữ liệu Tab 2 (fields frontend dùng)

Type: `PredictivePayload` trong `frontend/src/lib/dashboard.ts`.

Các field chính được UI dùng trực tiếp:

- `meta.month`, `meta.previous_month`, `meta.sample_user_count`, `meta.artifact_mode`
- `kpis.*` (đặc biệt `predicted_revenue_at_risk`)
- `risk_band_mix[]` (Pie chart)
- `executive_value_risk_matrix[]` (Scatter/Bubble matrix)
- `feature_group_waterfall[]` (Waterfall)
- `revenue_flow_sankey.nodes[]` + `revenue_flow_sankey.links[]` (Sankey)
- `price_paradox[]` (ComposedChart bar+line)
- `habit_funnel[]` (Funnel)
- `revenue_loss_outlook[]` (Outlook bar chart)
- `prescriptions[]` (Priority table + export/copy)
- `value_risk_matrix[]` (tính tổng Must Save)
- `revenue_leakage[]` (chỉ dùng để pick “tác nhân chính” cho InsightCard)

Lưu ý: `hasTab2Data(...)` hiện check “có data” dựa trên `sample_user_count`, `executive_value_risk_matrix`, `value_risk_matrix`, `revenue_leakage`, `forecast_decay` — nên một số chart có thể empty nhưng Tab 2 vẫn được xem là “ready”.

## 6) Logic từng khối UI trong PredictiveTab

### 6.1. Executive header (“Bảng dự báo rủi ro”)

Mục tiêu: tóm tắt nhanh phạm vi dự báo + call-to-action cho CSKH/Growth.

Computed:

- `topPrescription = data.prescriptions?.[0] ?? null`
- `priorityRows = data.prescriptions.slice(0, 6)`
- `mustSaveSummary`: reduce `data.value_risk_matrix` với `quadrant === "Must Save"`

Actions:

- **Export CSV**
  - Xuất `priorityRows` ra CSV với headers cố định.
  - Filename: `kkbox-risk-priority-${data.meta.month ?? selectedMonth}.csv`.
- **Copy Alert Summary**
  - Copy một đoạn text alert (Must Save summary + top 3 rows) vào clipboard.

### 6.2. Chart: “Phân bổ Dòng tiền Rủi ro” (Pie)

Nguồn dữ liệu: `risk_band_mix[]` (được map thêm `band_label`).

- Pie `dataKey="revenue_at_risk"`, `nameKey="band_label"`.
- Màu theo `band` (`Low/Medium/High/Unknown`) từ `RISK_BAND_COLORS`.
- Tooltip format tiền.
- Overlay ở giữa hiển thị **Tổng rủi ro** = `kpis.predicted_revenue_at_risk`.

Empty-state: nếu `riskBandMix.length === 0`.

### 6.3. Chart: “Ma trận Vị thế Khách hàng theo Giá trị và Rủi ro” (Scatter/Bubble)

Nguồn dữ liệu: `executive_value_risk_matrix[]`.

- X-axis: `prob_bin` (0→1), tick hiển thị theo %.
- Y-axis: `expected_renewal_amount`, domain `[0, 250]`.
- Z-axis: `display_size` (điều khiển bubble size).
- Reference lines:
  - `x = 0.5` (mốc rủi ro 50%)
  - `y = 100` (mốc giá trị NT$100)
- Màu bubble theo `risk_band` từ `MATRIX_RISK_BAND_COLORS`.
- Tooltip (`MatrixTooltip`) hiển thị churn prob bin, spend, user_count, revenue_at_risk, và `priority_quadrant` nếu có.
- Overlay nhãn quadrant (4 góc): “KHÁCH NÒNG CỐT”, “VIP NGUY CƠ”, “KHÁCH VÃNG LAI”, “NHÓM NHẠY CẢM GIÁ”.

Empty-state: nếu `executiveMatrix.length === 0`.

### 6.4. Chart: “Phân rã Tác động Rủi ro theo Nhóm Đặc trưng” (Waterfall)

Nguồn dữ liệu: `feature_group_waterfall[]`.

Logic build series: `buildWaterfallSeries(...)`

- Dùng biến `running` để tạo `start` (cộng dồn trước đó) và `amount` (đóng góp).
- Push thêm dòng “Tổng rủi ro” (total) nếu tổng `running > 0`.
- Render bằng `BarChart` stackId `"risk"`:
  - `start` bar trong suốt (để tạo offset)
  - `amount` bar có màu, row total tô đỏ.
- Tooltip (`WaterfallTooltip`) hiển thị:
  - tổng cộng dồn (`total`)
  - mức đóng góp (`amount`)
  - `contributionPct`
  - `featureCount` (chỉ cho row không phải total)

Empty-state: nếu `waterfallData.length === 0`.

### 6.5. Chart: “Dòng chảy Thất thoát Doanh thu” (Sankey)

Nguồn dữ liệu: `revenue_flow_sankey: { nodes, links }`.

- Stage headers cố định: `["Thanh toán", "RFM", "Gói giá", "Rủi ro"]`.
- `Sankey` dùng custom:
  - `node={<RevenueFlowNode />}`: hiển thị node label + (tuỳ chiều cao) giá trị compact.
  - `link={<RevenueFlowLink />}`: tô màu theo `payload.color` và chỉ hiển thị label value khi:
    - link đi vào tầng cuối (`target.depth >= 3`)
    - và `linkWidth >= 10`
- Tooltip (`RevenueFlowTooltip`) phân biệt:
  - hover link: hiển thị `source -> target`, value, và `risk_tier` (nếu có)
  - hover node: hiển thị `node.name` + tổng value qua node

Empty-state: nếu `nodes.length === 0` hoặc `links.length === 0`.

### 6.6. Chart: “Phân tích Tương quan giữa Phân đoạn Giá và Rủi ro Rời bỏ” (ComposedChart)

Nguồn dữ liệu: `price_paradox[]`.

- Bar (yAxis left): `user_count`, `revenue_at_risk`
- Line (yAxis right): `churn_rate_pct`
- Tooltip format:
  - churn rate → `formatPct(..., 1)`
  - revenue → `formatCurrency(...)`
  - user count → `formatNumber(...)`

Empty-state: nếu `price_paradox.length === 0`.

### 6.7. Chart: “Phễu Phân hóa Mức độ Hoạt động của Khách hàng” (Funnel)

Nguồn dữ liệu: `habit_funnel[]`.

- Funnel dùng `dataKey="user_count"` theo `habit_stage`.
- Màu theo `FUNNEL_COLORS`.
- Bên dưới hiển thị top 3 stage (theo thứ tự trong payload) kèm `share_of_top_pct`.

Empty-state: nếu `habit_funnel.length === 0`.

### 6.8. Chart: “Doanh thu dự kiến mất đi trong 3, 6, 12 tháng tới” (Outlook bar)

Nguồn dữ liệu: `revenue_loss_outlook[]`.

- X-axis: `horizon_label`
- Bar: `projected_revenue_loss`
- Tooltip: ngoài value tiền còn hiển thị `projected_loss_share_pct` (tỷ lệ % so với base).

Empty-state: nếu `revenueLossOutlook.length === 0`.

### 6.9. Insight cards (bên phải outlook)

Nguồn:

- “Nhóm cần giữ ngay” → `topPrescription`
- “Tác nhân chính” → `topLeakageDriver` (max `revenue_leakage.revenue_at_risk`)
- “Hành động khuyến nghị” → `topPrescription.recommended_action` (+ stage hành vi nổi bật `topHabitStage`)

## 7) Demo mode vs non-demo mode (khác biệt quan trọng)

- `DEMO_MODE = import.meta.env.VITE_DEMO_MODE === "1"`
- Trong fetch Tab 2:
  - **Demo mode**: không append `segmentFilter` (luôn xem full cohort tháng).
  - **Non-demo**: append `segmentFilter` nếu có.
- Trong UI:
  - Demo mode hiển thị note “bộ dự báo theo tháng”.
  - Non-demo có thêm khối `<details>` “Xem cấu hình mô hình” để chỉnh `modelParams` và trigger refetch.

## 8) Khi cần debug nhanh Tab 2

Checklist:

1. Mở Tab “Dự báo” để trigger fetch (Tab 2 là lazy-load).
2. Kiểm tra `selectedMonth` có hợp lệ `YYYY-MM` (đủ 7 ký tự).
3. Xác nhận endpoint `GET /api/v1/tab2/predictive` trả về đủ các field chart đang cần:
   - `risk_band_mix`, `executive_value_risk_matrix`, `feature_group_waterfall`, `revenue_flow_sankey`, …
4. Nếu chart cụ thể bị trống: kiểm tra condition `array.length`/`nodes+links` và payload tương ứng.

