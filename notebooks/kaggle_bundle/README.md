# Kaggle Bundle

Thư mục này là bộ tối thiểu để mang project lên Kaggle.

Mục tiêu:

- gom đúng các notebook đã tự chứa;
- tránh phải dò lại notebook nào là bản mới nhất;
- chốt rõ dataset input nào cần mount;
- chốt thứ tự chạy để sinh đủ artifact cho demo và báo cáo.

## Cấu trúc

- `tab1/kkbox-descriptive-tab.ipynb`
- `tab2/kkbox-train-predictive-tab.ipynb`
- `tab3/kkbox-simulation-2.ipynb`
- `tab3/kkbox-simulation-monte-carlo.ipynb`
- `tab3/kkbox-simulation-scenario-catalog.ipynb`
- `tab3/kkbox-simulation-monte-carlo-catalog.ipynb`
- `KAGGLE_RUNBOOK.md`
- `KAGGLE_DATASETS.md`

## Thứ tự chạy

1. Chuẩn bị dataset `feature_store`
2. Chạy `tab1/kkbox-descriptive-tab.ipynb`
3. Chạy `tab2/kkbox-train-predictive-tab.ipynb`
4. Chạy `tab3/kkbox-simulation-2.ipynb`
5. Nếu cần uncertainty / phụ lục nâng cao, chạy thêm `tab3/kkbox-simulation-monte-carlo.ipynb`

Luồng precompute nhiều preset cho web demo:

1. Chuẩn bị dataset `feature_store`
2. Chạy `tab2/kkbox-train-predictive-tab.ipynb`
3. Chạy `tab3/kkbox-simulation-scenario-catalog.ipynb`
4. Chạy `tab3/kkbox-simulation-monte-carlo-catalog.ipynb`
5. Publish lại 2 thư mục output để web demo load theo `scenario_id`

## Ghi chú

- Các notebook trong bundle này đều là bản tự chứa, không cần import `.py` helper ngoài notebook.
- Nếu tên dataset Kaggle khác với tên khuyến nghị trong `KAGGLE_DATASETS.md`, chỉ cần sửa biến:
  - `FEATURE_STORE_ROOT_HINT`
  - `TAB2_ARTIFACTS_ROOT_HINT`
- Hai notebook `*-catalog.ipynb` sẽ tạo thêm `scenario_catalog.json` và các thư mục con `scenarios/<scenario_id>/` để backend/frontend load preset precompute.
- Không nên dùng bundle này để rebuild raw feature engineering nặng. Bundle này dành cho giai đoạn dùng artifact canonical đã có.
