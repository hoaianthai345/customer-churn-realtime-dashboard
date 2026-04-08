# Kaggle Datasets

## 1. Dataset bắt buộc

### Dataset A. Feature store canonical

Notebook nào cần:

- Tab 1
- Tab 2
- Tab 3 deterministic
- Tab 3 Monte Carlo

Tên dataset Kaggle khuyến nghị:

- `kkbox-feature-store`

Notebook hiện có thể tự dò một số đường dẫn Kaggle phổ biến như:

- `/kaggle/input/kkbox-feature-store`
- `/kaggle/input/kkbox-feature-store/feature_store`
- `/kaggle/input/kkbox-churn-feature-store`
- `/kaggle/input/kkbox-churn-feature-store/feature_store`
- `/kaggle/input/kkbox-churn-output`
- `/kaggle/input/kkbox-churn-output/feature_store`

Các file tối thiểu cần có:

- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

Nên có thêm:

- `bi_feature_master.parquet`
- `submission_alignment_201704.csv`

### Dataset B. Tab 2 predictive artifacts

Notebook nào cần:

- Tab 3 deterministic
- Tab 3 Monte Carlo

Tên dataset Kaggle khuyến nghị:

- `kkbox-tab2-predictive`

Notebook hiện có thể tự dò một số đường dẫn Kaggle phổ biến như:

- `/kaggle/input/kkbox-tab2-predictive`
- `/kaggle/input/kkbox-tab2-predictive/artifacts_tab2_predictive`
- `/kaggle/input/kkbox-tab2-predictive-artifacts`
- `/kaggle/input/kkbox-tab2-predictive-artifacts/artifacts_tab2_predictive`

Các file tối thiểu cần có:

- `tab2_test_scored_201704.parquet`
- `tab2_validation_metrics.json`
- `tab2_model_summary.json`

Nên có thêm:

- `tab2_feature_columns_used.csv`
- `tab2_feature_importance_lightgbm.csv`
- `tab2_feature_group_importance.csv`
- `tab2_segment_risk_summary_201704.parquet`
- `tab2_lightgbm_model.txt`

## 2. Dataset tùy chọn

### Dataset C. Tab 3 prescriptive artifacts

Dùng khi:

- muốn đọc lại output deterministic để vẽ chart/report mà không rerun simulation
- muốn web demo load nhiều preset precompute bằng `scenario_id`

Tên dataset khuyến nghị:

- `kkbox-tab3-prescriptive`

### Dataset D. Tab 3 Monte Carlo artifacts

Dùng khi:

- muốn đọc lại output Monte Carlo để làm phụ lục hoặc dashboard uncertainty
- muốn web demo load uncertainty đúng với từng preset precompute

Tên dataset khuyến nghị:

- `kkbox-tab3-monte-carlo`

## 3. Mapping local -> Kaggle

### Feature store local

Nguồn local hiện tại:

- `project-realtime-bi/data/artifacts/feature_store/`

### Tab 2 artifacts local

Nguồn local hiện tại:

- `project-realtime-bi/data/artifacts_tab2_predictive/`

### Tab 3 deterministic local

Nguồn local hiện tại:

- `project-realtime-bi/data/artifacts_tab3_prescriptive/`
- nếu dùng preset catalog: giữ nguyên cả `scenario_catalog.json` và thư mục `scenarios/`

### Tab 3 Monte Carlo local

Nếu muốn publish bản smoke test trước:

- `project-realtime-bi/data/artifacts/_smoke_test/tab3_monte_carlo/`
- nếu dùng preset catalog: giữ nguyên cả `scenario_catalog.json` và thư mục `scenarios/`

## 4. Nếu tên dataset không khớp

Không cần sửa code lớn.

Chỉ sửa ở cell config đầu notebook:

```python
FEATURE_STORE_ROOT_HINT = '/kaggle/input/ten-dataset-thuc-te'
TAB2_ARTIFACTS_ROOT_HINT = '/kaggle/input/ten-dataset-thuc-te'
```

## 5. Khuyến nghị publish

Để tiện chạy lại và handoff, nên publish theo thứ tự:

1. `kkbox-feature-store`
2. `kkbox-tab2-predictive`
3. `kkbox-tab3-prescriptive`
4. `kkbox-tab3-monte-carlo`
