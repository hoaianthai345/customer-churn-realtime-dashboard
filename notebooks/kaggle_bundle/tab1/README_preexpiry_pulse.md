# Tab 1 Pre-Expiry Pulse

Notebook:

- `tab1/kkbox-preexpiry-pulse-context.ipynb`

Muc tieu:

- lay cohort cua `target_month` tu feature store canonical;
- dung raw `transactions_v2` va `user_logs_v2` cua thang truoc de dung chuoi theo ngay;
- xuat artifact context de backend co the doc vao pulse panel ma khong bi lech logic thoi gian.

Contract output:

- `artifacts_tab1_preexpiry_pulse/tab1_preexpiry_pulse_daily_<TARGET_MONTH>.parquet`
- `artifacts_tab1_preexpiry_pulse/tab1_preexpiry_pulse_summary_<TARGET_MONTH>.json`
- `artifacts_tab1_preexpiry_pulse/manifest.json`
