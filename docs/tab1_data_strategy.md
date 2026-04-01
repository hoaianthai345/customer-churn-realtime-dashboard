# Tab 1 Data Strategy (History Precompute + Realtime)

## Goal

Giảm tải CPU cho dashboard Tab 1 bằng cách tách dữ liệu thành 2 lớp:

1. `history_precompute`: xử lý trước (offline) cho giai đoạn trước realtime.
2. `realtime_2017_plus`: giả lập realtime từ 2017-03 trở đi (log stream), materialize lại từ fact tables.

## Serving Table

`realtime_bi.tab1_descriptive_member_monthly`

Grain: `1 row / user / snapshot_month`.

Các cột chính phục vụ Tab 1:

- KPI: `churned`, `is_auto_renew`, `survival_days`, `snapshot_month`
- KM dimension: `age_bucket`, `gender_bucket`, `txn_freq_bucket`, `skip_ratio_bucket`
- Stacked bars: `price_segment`, `loyalty_segment`, `active_segment`
- Scatter: `discovery_ratio`, `skip_ratio`

## Run Flow

`scripts/run_pipeline.sh` tự gọi:

1. `python3 -m apps.batch.precompute_tab1_history` (history window)
2. đảm bảo Spark streaming jobs chạy một lần (không spawn trùng job)

Replay realtime (`2017-03+`) được trigger theo nhu cầu:

- Nút Replay trên web
- Hoặc `POST /api/v1/replay/start`

## Config

Trong `.env`:

- `TAB1_PRECOMPUTE_START_DATE=2016-01-01`
- `TAB1_REALTIME_START_DATE=2017-03-01`
- `REPLAY_START_DATE=2017-03-01`

Skip khi cần chạy nhanh:

- `HOST_SKIP_TAB1_HISTORY_PRECOMPUTE=1 bash scripts/run_pipeline.sh`
- `HOST_AUTO_REPLAY=1 bash scripts/run_pipeline.sh` nếu muốn auto replay khi startup

## API for Tab 1

- `GET /api/v1/tab1/month-options`
- `GET /api/v1/tab1/descriptive?year=YYYY&month=MM&dimension=age|gender|txn_freq|skip_ratio`
- Optional cross-filter:
  - `segment_type=price_segment|loyalty_segment|active_segment`
  - `segment_value=<value>`

Response gồm:

- `kpis`: total expiring, churn rate, median survival, auto-renew rate
- `km_curve`: Kaplan-Meier points theo dimension đã chọn
- `segment_mix`: 100% stacked data cho 3 segment groups
- `boredom_scatter`: bins của `discovery_ratio` x `skip_ratio`
