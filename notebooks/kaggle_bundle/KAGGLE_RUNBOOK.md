# Kaggle Runbook

## 1. Mục tiêu

Runbook này giúp mang 4 notebook chính lên Kaggle mà không phải chỉnh lại logic.

Phạm vi:

- Tab 1 descriptive
- Tab 2 predictive training + scoring
- Tab 3 deterministic prescriptive simulation
- Tab 3 Monte Carlo simulation
- Tab 3 multi-scenario preset catalog cho web demo

## 2. Chuẩn bị trước khi upload

Anh cần có ít nhất 2 Kaggle Dataset:

1. `feature_store`
2. `tab2_predictive_artifacts`

Chi tiết tên file mong đợi nằm ở `KAGGLE_DATASETS.md`.

## 3. Tạo notebook trên Kaggle

Tạo notebook mới trên Kaggle và attach các dataset:

1. Dataset chứa feature store
2. Dataset chứa artifact Tab 2 nếu chạy Tab 3

Kernel:

- Python 3

Accelerator:

- Không bắt buộc

Internet:

- Không bắt buộc

## 4. Chạy theo từng tab

### Tab 1

Notebook:

- `tab1/kkbox-descriptive-tab.ipynb`

Input:

- feature store canonical

Biến cần kiểm tra ở đầu notebook:

- `FEATURE_STORE_ROOT_HINT = None`
- `OUTPUT_DIR = 'artifacts_tab1_descriptive'`

Khi nào cần sửa `FEATURE_STORE_ROOT_HINT`:

- chỉ sửa nếu tên dataset Kaggle của anh không khớp các đường dẫn notebook tự dò

Output:

- `artifacts_tab1_descriptive/`

### Tab 2

Notebook:

- `tab2/kkbox-train-predictive-tab.ipynb`

Input:

- feature store canonical

Biến cần kiểm tra:

- `FEATURE_STORE_ROOT_HINT = None`
- `OUTPUT_DIR = 'artifacts_tab2_predictive'`
- `TRAIN_MONTHS = [201701, 201702]`
- `VALID_MONTH = 201703`
- `SCORE_MONTH = 201704`

Output:

- `artifacts_tab2_predictive/`

Sau khi chạy xong:

- nên download nguyên thư mục output hoặc publish nó thành Kaggle Dataset mới
- Tab 3 sẽ dùng trực tiếp dataset này

### Tab 3 deterministic

Notebook:

- `tab3/kkbox-simulation-2.ipynb`
- `tab3/kkbox-simulation-scenario-catalog.ipynb` nếu muốn build nhiều preset cùng lúc cho demo

Input:

- feature store canonical
- artifact Tab 2

Biến cần kiểm tra:

- `FEATURE_STORE_ROOT_HINT = None`
- `TAB2_ARTIFACTS_ROOT_HINT = None`
- `OUTPUT_DIR = 'artifacts_tab3_prescriptive'`
- `SCORE_MONTH = 201704`

Output:

- `artifacts_tab3_prescriptive/`
- nếu dùng notebook catalog: có thêm `scenario_catalog.json`
- nếu dùng notebook catalog: có thêm `scenarios/<scenario_id>/...`

### Tab 3 Monte Carlo

Notebook:

- `tab3/kkbox-simulation-monte-carlo.ipynb`
- `tab3/kkbox-simulation-monte-carlo-catalog.ipynb` nếu muốn build uncertainty cho nhiều preset cùng lúc

Input:

- feature store canonical
- artifact Tab 2

Biến cần kiểm tra:

- `FEATURE_STORE_ROOT_HINT = None`
- `TAB2_ARTIFACTS_ROOT_HINT = None`
- `OUTPUT_DIR = 'artifacts_tab3_monte_carlo'`
- `SCORE_MONTH = 201704`
- `MC_CONFIG['n_iterations']`

Khuyến nghị:

- smoke test nhanh: `50` hoặc `100`
- chạy báo cáo chính thức: `500` hoặc `1000`

Output:

- `artifacts_tab3_monte_carlo/`
- nếu dùng notebook catalog: có thêm `scenario_catalog.json`
- nếu dùng notebook catalog: có thêm `scenarios/<scenario_id>/...`

### Tab 3 preset catalog cho web demo

Mục tiêu:

- chạy sẵn nhiều bộ tham số trên Kaggle
- publish artifact một lần
- web demo chỉ đổi `scenario_id` và load lại payload, không simulate lại ở runtime

Notebook deterministic:

- `tab3/kkbox-simulation-scenario-catalog.ipynb`

Notebook Monte Carlo:

- `tab3/kkbox-simulation-monte-carlo-catalog.ipynb`

Biến cần kiểm tra:

- `OUTPUT_DIR`
- `DEFAULT_SCENARIO_ID`
- `SCENARIO_CASES`
- `MC_CONFIG` với notebook Monte Carlo catalog

Contract output thêm:

- `scenario_catalog.json`
- default scenario được ghi ngay tại root `OUTPUT_DIR`
- các scenario còn lại nằm trong `OUTPUT_DIR/scenarios/<scenario_id>/`

Contract catalog:

- `default_scenario_id`
- `scenarios[].scenario_id`
- `scenarios[].label`
- `scenarios[].description`
- `scenarios[].artifact_subdir` hoặc `monte_carlo_subdir`
- `scenarios[].scenario_inputs`

## 5. Khi nào cần sửa root hint

Nếu notebook không tự tìm được dataset, sửa trực tiếp trong cell config:

Ví dụ:

```python
FEATURE_STORE_ROOT_HINT = '/kaggle/input/ten-dataset-feature-store'
TAB2_ARTIFACTS_ROOT_HINT = '/kaggle/input/ten-dataset-tab2'
```

Không cần sửa code logic bên dưới.

## 6. Checklist pass nhanh

### Tab 1

- có `manifest.json`
- có `tab1_snapshot_201704.parquet`
- có `tab1_kpis_monthly.parquet`

### Tab 2

- có `tab2_test_scored_201704.parquet`
- có `tab2_validation_metrics.json`
- có `tab2_model_summary.json`

### Tab 3 deterministic

- có `tab3_scenario_summary_201704.json`
- có `tab3_scenario_member_level_201704.parquet`
- nếu dùng catalog: có `scenario_catalog.json`

### Tab 3 Monte Carlo

- có `tab3_monte_carlo_summary_201704.json`
- có `tab3_monte_carlo_runs_201704.parquet`
- có `tab3_monte_carlo_percentiles_201704.parquet`
- nếu dùng catalog: có `scenario_catalog.json`

## 7. Output nên publish lại thành Kaggle Dataset

Nên publish riêng:

1. `artifacts_tab2_predictive`
2. `artifacts_tab3_prescriptive`
3. `artifacts_tab3_monte_carlo` nếu dùng cho báo cáo chính thức

Nếu web demo cần nhiều preset:

1. publish thư mục deterministic có `scenario_catalog.json`
2. publish thư mục Monte Carlo có `scenario_catalog.json`
3. copy nguyên folder về `project-realtime-bi/data/artifacts_tab3_prescriptive/` và `project-realtime-bi/data/artifacts_tab3_monte_carlo/`
4. backend sẽ tự đọc theo `scenario_id`, frontend chỉ cần chọn preset

Lý do:

- dễ reuse cho notebook khác
- không phải rerun lại notebook nặng
- dễ handoff cho agent hoặc thành viên khác
