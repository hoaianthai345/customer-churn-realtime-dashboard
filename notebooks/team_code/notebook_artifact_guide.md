# Notebook Artifact Guide

Tai lieu nay mo ta:

- moi notebook trong `team_code/` dung de lam gi;
- notebook nao phu thuoc vao notebook nao;
- artifact nao duoc sinh ra sau khi run;
- cach lay artifact ve may sau khi notebook chay xong.

## 1. Thu tu run de co du artifact

Thu tu khuyen nghi:

1. `features_prep` / `train_churn_pipeline.ipynb`
2. `team_code/tab1/kkbox-descriptive-tab.ipynb`
3. `team_code/tab2/kkbox-train-predictive-tab.ipynb`
4. `team_code/tab3/kkbox-simulation-2.ipynb`
5. `team_code/tab3/kkbox-simulation-monte-carlo.ipynb` neu can them lop uncertainty cho Tab 3

Logic phu thuoc:

- `Tab 1` doc truc tiep feature store da duoc xuat boi `features_prep`.
- `Tab 2` train model va score tren feature store do.
- `Tab 3` doc feature store + artifact scored cua `Tab 2`, khong rebuild raw data nua.

## 2. Dau vao canonical sau `features_prep`

Sau khi `features_prep` chay xong, feature store can co it nhat:

- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

Neu co them:

- `bi_feature_master.parquet`

thi `Tab 1` co the doc truc tiep file tong hop nay.

## 3. Mo ta tung notebook

### 3.1. Tab 1: `team_code/tab1/kkbox-descriptive-tab.ipynb`

Vai tro:

- sinh artifact cho Tab 1 descriptive;
- scope chi gom KPI, Kaplan-Meier, segment mix, boredom scatter;
- khong dua forecasting/risk simulation vao Tab 1.

Notebook hien tai da tu chua logic can thiet trong code cell, khong con phu thuoc runtime vao `.py` helper.

Thu muc output mac dinh:

- `artifacts_tab1_descriptive/`

Artifact duoc sinh ra:

- `tab1_kpis_monthly.parquet`
- `tab1_km_curves.parquet`
- `tab1_segment_mix.parquet`
- `tab1_boredom_scatter.parquet`
- `tab1_snapshot_<latest_month>.parquet`
- `manifest.json`

Y nghia:

- `tab1_kpis_monthly.parquet`: KPI theo thang.
- `tab1_km_curves.parquet`: du lieu Kaplan-Meier theo dimension.
- `tab1_segment_mix.parquet`: ty trong segment theo thang.
- `tab1_boredom_scatter.parquet`: scatter grid cho `skip_ratio x discovery_ratio`.
- `tab1_snapshot_<latest_month>.parquet`: snapshot descriptive cua thang moi nhat.
- `manifest.json`: metadata input/output cua notebook.

### 3.2. Tab 2: `team_code/tab2/kkbox-train-predictive-tab.ipynb`

Vai tro:

- train predictive churn model tren feature store;
- score validation month va score month;
- xuat scored artifact de UI/backend doc truc tiep.

Notebook hien tai da tu chua logic train/score trong code cell, khong con phu thuoc runtime vao `.py` helper.

Thu muc output mac dinh:

- `artifacts_tab2_predictive/`

Artifact duoc sinh ra:

- `tab2_validation_metrics.json`
- `tab2_feature_columns_used.csv`
- `tab2_feature_importance_lightgbm.csv`
- `tab2_feature_group_importance.csv`
- `tab2_valid_scored_201703.parquet`
- `tab2_test_scored_201704.parquet`
- `tab2_segment_risk_summary_201704.parquet`
- `tab2_model_summary.json`
- `tab2_lightgbm_model.txt`
- `manifest.json`

Y nghia:

- `tab2_validation_metrics.json`: metric validation va thong tin calibration.
- `tab2_feature_columns_used.csv`: danh sach feature thuc su dua vao model.
- `tab2_feature_importance_lightgbm.csv`: importance theo tung feature.
- `tab2_feature_group_importance.csv`: importance theo nhom business feature.
- `tab2_valid_scored_201703.parquet`: scored output cho validation month.
- `tab2_test_scored_201704.parquet`: scored output chinh de dung cho Tab 2 / Tab 3.
- `tab2_segment_risk_summary_201704.parquet`: tong hop risk theo segment.
- `tab2_model_summary.json`: tom tat model va output.
- `tab2_lightgbm_model.txt`: file model da train.
- `manifest.json`: metadata input/output cua notebook.

### 3.3. Tab 3: `team_code/tab3/kkbox-simulation-2.ipynb`

Vai tro:

- sinh artifact cho prescriptive simulation;
- doc baseline risk tu output `Tab 2`;
- simulate 3 lever kinh doanh:
  - `Manual -> Auto-Renew`
  - `Deal / Free Trial -> Standard / Premium`
  - `High Skip / Low Discovery -> healthier engagement`

Notebook hien tai da tu chua logic deterministic prescriptive simulation trong code cell, khong con phu thuoc runtime vao `.py` helper.

Thu muc output mac dinh:

- `artifacts_tab3_prescriptive/`

Artifact duoc sinh ra:

- `tab3_scenario_member_level_201704.parquet`
- `tab3_scenario_summary_201704.json`
- `tab3_lever_summary_201704.parquet`
- `tab3_segment_impact_201704.parquet`
- `tab3_population_risk_shift_201704.parquet`
- `tab3_sensitivity_201704.parquet`
- `manifest.json`

Y nghia:

- `tab3_scenario_member_level_201704.parquet`: ket qua member-level sau simulation.
- `tab3_scenario_summary_201704.json`: tong hop scenario baseline vs simulated.
- `tab3_lever_summary_201704.parquet`: so sanh combined scenario va tung lever rieng.
- `tab3_segment_impact_201704.parquet`: tac dong theo segment.
- `tab3_population_risk_shift_201704.parquet`: dich chuyen phan bo risk truoc/sau simulation.
- `tab3_sensitivity_201704.parquet`: sensitivity theo muc effort/share cua tung lever.
- `manifest.json`: metadata input/output cua notebook.

### 3.4. Tab 3 Monte Carlo: `team_code/tab3/kkbox-simulation-monte-carlo.ipynb`

Vai tro:

- mo rong Tab 3 tu deterministic scenario sang `portfolio Monte Carlo simulation`;
- dung baseline risk tu output `Tab 2`;
- mo phong phan bo revenue / churn sau can thiep duoi bat dinh tham so;
- phu hop de dua vao phu luc hoac phan sensitivity nang cao trong bao cao.

Notebook hien tai da tu chua logic trong code cell, khong con phu thuoc runtime vao `.py` helper.

Thu muc output mac dinh:

- `artifacts_tab3_monte_carlo/`

Artifact duoc sinh ra:

- `tab3_mc_member_level_201704.parquet`
- `tab3_mc_units_201704.parquet`
- `tab3_monte_carlo_runs_201704.parquet`
- `tab3_monte_carlo_percentiles_201704.parquet`
- `tab3_monte_carlo_summary_201704.json`
- `tab3_deterministic_summary_201704.json`
- `manifest.json`

Y nghia:

- `tab3_mc_member_level_201704.parquet`: member-level scenario sau deterministic simulation.
- `tab3_mc_units_201704.parquet`: simulation unit da duoc aggregate de Monte Carlo chay nhanh.
- `tab3_monte_carlo_runs_201704.parquet`: ket qua tung lan chay Monte Carlo.
- `tab3_monte_carlo_percentiles_201704.parquet`: bang percentile cho KPI chinh.
- `tab3_monte_carlo_summary_201704.json`: summary mean/p05/p50/p95 va xac suat net positive.
- `tab3_deterministic_summary_201704.json`: summary deterministic de doi chieu voi Monte Carlo.
- `manifest.json`: metadata input/output cua notebook.

## 4. Thu muc artifact mac dinh

Mac dinh, sau khi run xong se co 4 thu muc:

- `artifacts_tab1_descriptive/`
- `artifacts_tab2_predictive/`
- `artifacts_tab3_prescriptive/`
- `artifacts_tab3_monte_carlo/`

Neu can doi cho luu artifact, sua bien `OUTPUT_DIR` trong notebook truoc khi run.

## 5. Cach tai artifact ve may

Co 3 cach de lay artifact ve.

### Cach 1. Lay truc tiep tu folder output

Neu dang chay local Jupyter/VS Code tren may:

- mo thu muc output;
- copy ca folder artifact ra vi tri mong muon.

Vi du:

- copy `artifacts_tab1_descriptive/`
- copy `artifacts_tab2_predictive/`
- copy `artifacts_tab3_prescriptive/`
- copy `artifacts_tab3_monte_carlo/`

### Cach 2. Nen artifact thanh file zip roi tai ve

Neu muon gom artifact de gui nhom hoac upload len repo/cloud, co the zip tung folder.

Lenh mau:

```bash
zip -r tab1_descriptive_artifacts.zip artifacts_tab1_descriptive
zip -r tab2_predictive_artifacts.zip artifacts_tab2_predictive
zip -r tab3_prescriptive_artifacts.zip artifacts_tab3_prescriptive
zip -r tab3_monte_carlo_artifacts.zip artifacts_tab3_monte_carlo
```

Neu muon gom tat ca:

```bash
zip -r kkbox_all_artifacts.zip \
  artifacts_tab1_descriptive \
  artifacts_tab2_predictive \
  artifacts_tab3_prescriptive \
  artifacts_tab3_monte_carlo
```

### Cach 3. Tao zip ngay trong notebook

Co the them mot cell cuoi notebook:

```python
from pathlib import Path
import shutil

artifact_dir = Path("artifacts_tab2_predictive")
zip_path = shutil.make_archive(
    base_name=str(artifact_dir),
    format="zip",
    root_dir=str(artifact_dir.parent),
    base_dir=artifact_dir.name,
)
zip_path
```

Doi `artifact_dir` thanh:

- `Path("artifacts_tab1_descriptive")`
- `Path("artifacts_tab2_predictive")`
- `Path("artifacts_tab3_prescriptive")`

Sau khi cell chay xong, notebook se sinh file `.zip` cung cap voi folder artifact.

## 6. Cach kiem tra artifact da du chua

Kiem tra nhanh:

- trong moi folder phai co `manifest.json`;
- `Tab 1` phai co 5 file parquet chinh;
- `Tab 2` phai co scored parquet + metrics json + model file;
- `Tab 3` phai co summary json + member-level parquet + sensitivity parquet.

Lenh check nhanh:

```bash
ls artifacts_tab1_descriptive
ls artifacts_tab2_predictive
ls artifacts_tab3_prescriptive
```

## 7. Luu y khi run tren moi truong khac nhau

- Neu feature store khong nam o vi tri mac dinh, sua `FEATURE_STORE_ROOT_HINT` trong notebook.
- Neu artifact `Tab 2` khong nam o vi tri mac dinh, sua `TAB2_ARTIFACTS_ROOT_HINT` trong notebook `Tab 3`.
- `Tab 3` bat buoc can `tab2_test_scored_201704.parquet` truoc khi run.
- `manifest.json` la file dau tien nen xem khi can debug vi no ghi lai input/output cua moi notebook.

## 8. Checklist run hoan chinh

1. Run `features_prep` xong va kiem tra feature store da co day du parquet/csv.
2. Run `Tab 1` va kiem tra `artifacts_tab1_descriptive/`.
3. Run `Tab 2` va kiem tra `artifacts_tab2_predictive/`.
4. Run `Tab 3` va kiem tra `artifacts_tab3_prescriptive/`.
5. Zip artifact neu can nop bai, share nhom hoac dua vao pipeline san pham.
