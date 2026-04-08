# KKBOX Pipeline Descriptions For Drawing

Tai lieu nay duoc viet lai de ve dung cac pipeline moi nhat cua du an.
Muc tieu la giup nhom ve slide / report theo dung boundary implementation hien tai, tranh ve sai nhu:

- Tab 2 va Tab 3 da production full trong app runtime.
- Tab 2 la survival / Cox pipeline.
- Tab 3 la optimization engine da hieu chuan.
- Realtime serving layer thay the feature store batch.

Tai lieu nay viet theo kieu `box -> arrow -> box`, co the dung truc tiep de ve hinh.

## 1. Nguon su that va thu tu uu tien

Neu co mau thuan giua cac tai lieu, uu tien theo thu tu sau:

1. `project-realtime-bi/docs/system_description/project_desc.md`
2. `project-realtime-bi/docs/system_description/tab1_data_strategy.md`
3. `project-realtime-bi/docs/system_description/kkbox_tab2_predictive_pipeline.md`
4. `team_code/notebook_artifact_guide.md`
5. `team_code/tab1/*.ipynb`, `team_code/tab2/*.ipynb`, `team_code/tab3/*.ipynb`

## 2. Boundary can tach ro khi ve

Hien tai du an co 2 boundary khac nhau va khong duoc tron vao nhau:

### 2.1. Runtime app / dashboard boundary

Day la stack chay san pham trong repo:

- Kafka
- Spark Structured Streaming
- ClickHouse
- FastAPI
- frontend dashboard

Boundary nay grounded nhat o Tab 1.
Tab 2 va Tab 3 trong app runtime van can duoc xem la surface san pham / huong tich hop, khong nen ve nhu da noi truc tiep model artifact vao production neu implementation do chua xong.

### 2.2. Notebook analytics / artifact boundary

Day la boundary moi nhat cua phan phan tich:

- `features_prep` tao feature store canonical.
- Notebook Tab 1 sinh descriptive artifacts.
- Notebook Tab 2 train model va sinh predictive artifacts.
- Notebook Tab 3 doc feature store + Tab 2 scored artifact de sinh prescriptive artifacts.

Boundary nay la noi dung dung nhat neu ve methodology hoc may / business analytics.

## 3. Nguyen tac ve hinh

- Khong ve Tab 2 nhu `Cox survival model` hay `hazard model` neu notebook hien tai khong train nhu vay.
- Khong doi ten `expected_revenue_at_risk_30d` thanh `CLTV`.
- Khong ve Tab 3 nhu `decision engine` hay `optimization engine`.
- Khong ve realtime ClickHouse layer nhu nguon su that cao nhat cho feature semantics.
- Neu ve hinh chung toan bai, phai tach `offline feature store` va `realtime serving layer`.
- Neu ve hinh methodology, phai dung flow notebook artifact moi nhat thay vi prototype cu.

## 4. Diagram 1: End-to-End system pipeline

### Muc dich

Dung o slide mo dau de cho thay bai co 3 lop:

1. offline feature engineering;
2. realtime serving cho dashboard;
3. analytics / product surfaces.

### Cach ve de nghi

Ve 3 lane ngang:

- Lane 1: Offline batch analytics
- Lane 2: Realtime serving
- Lane 3: API / UI / product tabs

### Chuoi box nen ve

`KKBOX raw files`
-> `Offline cleaning + monthly feature engineering`
-> `Feature store canonical`

song song:

`members + transactions preload`
-> `ClickHouse static tables`

`user_logs replay`
-> `Kafka`
-> `Spark Structured Streaming`
-> `ClickHouse fact + KPI tables`

sau do:

`Feature store canonical + ClickHouse serving`
-> `FastAPI`
-> `frontend dashboard`
-> `Tab 1 / Tab 2 / Tab 3`

### Y nghia tung box

- `KKBOX raw files`: `members_v3`, `transactions_v2`, `user_logs_v2`, `train_v2`.
- `Offline cleaning + monthly feature engineering`: buoc tao snapshot feature theo thang.
- `Feature store canonical`: nguon dau vao cho notebook analytics.
- `members + transactions preload`: preload static data vao serving layer.
- `user_logs replay`: mo phong near real-time.
- `Kafka -> Spark -> ClickHouse`: lop serving cho dashboard.
- `FastAPI -> frontend dashboard`: lop API va giao dien.
- `Tab 1 / Tab 2 / Tab 3`: ba mat san pham.

### Note can ghi tren hinh

- `Offline feature store la source-of-truth cho semantics modeling`
- `Realtime serving layer chu yeu grounded cho Tab 1`
- `Tab 2 / Tab 3 artifact pipeline hien tai nam o notebook boundary`

## 5. Diagram 2: Offline feature store pipeline

### Muc dich

Dung cho phan methodology data engineering.
Tra loi cau hoi: lam sao di tu raw KKBOX den grain `1 row / 1 user / 1 target_month`.

### Cach ve de nghi

Ve tu trai sang phai voi 7 cum lon:

1. raw tables
2. cleaning
3. monthly cohort builder
4. anchor transaction selection
5. previous-month user log aggregation
6. feature assembly + labeling
7. feature store outputs

### Chuoi box nen ve

`members_v3 + transactions_v2 + user_logs_v2 + train_v2`
-> `Cleaning + normalization`
-> `Monthly expiring cohort builder`
-> `Anchor transaction selection`
-> `Previous-month log aggregation`
-> `Feature assembly + churn labeling`
-> `Feature store outputs`

### Mo ta tung box

#### Box 1. `Cleaning + normalization`

- chuan hoa kieu ngay thang
- guard chia 0 cho ratio
- xu ly age khong hop le
- dong bo snapshot semantics theo `target_month`

#### Box 2. `Monthly expiring cohort builder`

- lap qua tung `target_month`
- population la expiring cohort cua thang do
- grain dau ra la `1 user / 1 target_month`

#### Box 3. `Anchor transaction selection`

- lay lich su giao dich truoc snapshot
- tim block giao dich gan expiry nhat
- chon 1 anchor transaction theo tie-break
- chi giu row hop le cho target month

#### Box 4. `Previous-month log aggregation`

- tong hop log cua thang truoc snapshot
- tinh `count`, `num_unq_sum`, `total_secs_sum`
- tinh `skip_ratio`, `discovery_ratio`, `completion_ratio`, `replay_ratio`

#### Box 5. `Feature assembly + churn labeling`

- payment / value features
- churn history / renewal history
- listening behavior features
- member / segment / flag features
- label `is_churn`

#### Box 6. `Feature store outputs`

- `train_features_all.parquet`
- `test_features_201704_full.parquet`
- `bi_feature_master.parquet`
- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

### Note can ghi tren hinh

- `Label semantics: renewal < 30 ngay => non-churn`
- `Feature store canonical la dau vao cua Tab 1 / Tab 2 / Tab 3 notebooks`

## 6. Diagram 3: Notebook artifact chain

### Muc dich

Day la diagram quan trong nhat neu muon ve dung pipeline analytics hien tai nhat cua nhom.
No cho thay quan he phu thuoc giua `features_prep`, `Tab 1`, `Tab 2`, `Tab 3`.

### Cach ve de nghi

Ve 4 box lon theo chuoi:

`features_prep`
-> `Tab 1 notebook`
-> `Tab 2 notebook`
-> `Tab 3 notebook`

Trong do:

- Tab 1 doc feature store.
- Tab 2 doc feature store.
- Tab 3 doc feature store + Tab 2 scored artifacts.

### Chuoi box nen ve

`features_prep`
-> `Feature store canonical`

tu day tach 2 nhanh:

`Feature store canonical`
-> `Tab 1 descriptive notebook`
-> `artifacts_tab1_descriptive`

`Feature store canonical`
-> `Tab 2 predictive notebook`
-> `artifacts_tab2_predictive`

sau do:

`Feature store canonical + artifacts_tab2_predictive`
-> `Tab 3 prescriptive notebook`
-> `artifacts_tab3_prescriptive`

### Note can ghi tren hinh

- `Tab 3 khong rebuild raw data nua`
- `Tab 2 va Tab 3 hien tai canonical nhat o notebook artifact flow`
- `Day la pipeline dung nhat de ve methodology va artifact generation`

## 7. Diagram 4: Runtime serving pipeline for dashboard

### Muc dich

Dung de giai thich kien truc near real-time trong repo.
Nen nhan manh day la serving boundary, khong phai notebook training boundary.

### Cach ve de nghi

Ve 2 nhanh hop vao ClickHouse:

- preload static data
- replay user logs

### Chuoi box nen ve

Nhanh 1:

`members + transactions`
-> `Batch preload`
-> `ClickHouse static tables`

Nhanh 2:

`user_logs replay`
-> `Kafka topic`
-> `Spark Structured Streaming`
-> `ClickHouse fact + realtime KPI tables`

Hop:

`ClickHouse`
-> `Batch materializer`
-> `realtime_bi.tab1_descriptive_member_monthly`
-> `FastAPI`
-> `frontend dashboard`

### Y nghia nghiep vu

- preload giu nen thanh vien va giao dich
- replay user logs mo phong dong su kien
- Spark tinh fact va KPI
- materializer tong hop thanh serving table cho Tab 1 query nhanh

### Note can ghi tren hinh

- `Runtime serving grounded nhat o Tab 1`
- `Khong thay the feature store batch`
- `Khong nen ve nhu model-serving pipeline cho Tab 2 / Tab 3 neu chua implement`

## 8. Diagram 5: Tab 1 descriptive pipeline

### Muc dich

Dung cho phan dashboard descriptive.
Tab 1 la lop grounded nhat, scope chi gom descriptive analytics.

### Cach ve de nghi

Neu ve theo product runtime:

`tab1_descriptive_member_monthly`
-> `Metric layer`
-> `Charts`
-> `Tab 1 dashboard`

Neu ve theo notebook artifact:

`Feature store canonical`
-> `Tab 1 descriptive notebook`
-> `Tab 1 artifacts`
-> `Tab 1 dashboard / report charts`

### Chuoi box nen ve

Version runtime:

`realtime_bi.tab1_descriptive_member_monthly`
-> `KPI + KM + segment mix + boredom scatter`
-> `Descriptive dashboard`

Version notebook:

`train_features_bi_all.parquet + bi_feature_master.parquet`
-> `Tab 1 descriptive notebook`
-> `tab1_kpis_monthly / tab1_km_curves / tab1_segment_mix / tab1_boredom_scatter`

### Mo ta tung box

#### Box 1. `Metric layer`

- `Total Expiring Users`
- `Historical Churn Rate`
- `Auto-Renew Rate`
- `Median Survival`

#### Box 2. `Charts`

- Kaplan-Meier theo dimension
- 100% stacked segment bar
- boredom scatter theo `discovery_ratio x skip_ratio`
- trend theo month

### Note can ghi tren hinh

- `Tab 1 chi la descriptive layer`
- `Khong dua risk_score hay revenue_at_risk vao trung tam cua Tab 1`

## 9. Diagram 6: Tab 2 predictive artifact pipeline

### Muc dich

Day la hinh methodology predictive dung nhat hien tai.
No phai cho thay ro su khac nhau giua:

1. feature input layer
2. model train / validation layer
3. scoring artifact layer

### Cach ve de nghi

Ve theo 7 box:

`Feature store`
-> `Feature selection`
-> `Time split`
-> `LightGBM training`
-> `Probability calibration`
-> `Score expiring cohort`
-> `Predictive artifacts`

### Chuoi box nen ve

`train_features_bi_all.parquet + test_features_bi_201704_full.parquet`
-> `Feature selection from feature_columns.csv`
-> `Train 201701-201702 / valid 201703 / retrain 201701-201703`
-> `LightGBM churn classifier`
-> `Calibration`
-> `Score 201704 expiring cohort`
-> `artifacts_tab2_predictive`

### Mo ta tung box

#### Box 1. `Feature selection from feature_columns.csv`

- giu feature co trong train va test
- loai bo ID
- loai bo cot downstream cua Tab 1
- giu 4 nhom chinh: payment, churn history, listening, loyalty / flags

#### Box 2. `Time split`

- train fit: `201701-201702`
- validation: `201703`
- retrain full train: `201701-201703`
- score month: `201704`

#### Box 3. `LightGBM churn classifier`

- binary classification
- objective la `churn_probability`
- early stopping tren validation month

#### Box 4. `Calibration`

- calibration tren validation prediction
- chi giu ban calibrated neu logloss tot hon

#### Box 5. `Score 201704 expiring cohort`

- tao `churn_probability`
- tao `risk_percentile`, `risk_decile`, `risk_band`
- tinh `expected_revenue_at_risk_30d`
- tinh `expected_retained_revenue_30d`

#### Box 6. `artifacts_tab2_predictive`

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

### Note can ghi tren hinh

- `Day la churn classification pipeline, khong phai Cox survival pipeline`
- `Khong doi expected_revenue_at_risk_30d thanh CLTV`
- `Tab 2 canonical nhat hien tai la notebook artifact flow`

## 10. Diagram 7: Tab 3 prescriptive artifact pipeline

### Muc dich

Day la hinh methodology cho lop prescriptive simulation.
Boundary dung la:

- nhan state tu feature store
- nhan baseline risk tu Tab 2 scored artifact
- ap business levers
- sinh ket qua so sanh baseline vs scenario

### Cach ve de nghi

Ve 5 box:

`Canonical inputs`
-> `Scenario levers`
-> `Simulation engine`
-> `Scenario outputs`
-> `Tab 3 charts`

### Chuoi box nen ve

`Feature store canonical + tab2_test_scored_201704.parquet`
-> `Scenario levers`
-> `Prescriptive simulation notebook`
-> `artifacts_tab3_prescriptive`
-> `Tab 3 dashboard / report charts`

### Mo ta tung box

#### Box 1. `Canonical inputs`

- feature state: `price_segment`, `renewal_segment`, `loyalty_segment`, `active_segment`
- behavior ratios: `skip_ratio`, `discovery_ratio`
- business flags: `deal_hunter_flag`, `free_trial_flag`, `content_fatigue_flag`, `is_manual_renew`
- predictive baseline: `churn_probability`, `risk_band`, `expected_revenue_at_risk_30d`

#### Box 2. `Scenario levers`

- `Manual -> Auto-Renew`
- `Deal / Free Trial -> Standard / Premium`
- `High Skip / Low Discovery -> healthier engagement`

#### Box 3. `Prescriptive simulation notebook`

- thay doi state / flags theo scenario config
- uoc tinh lai risk profile
- tinh retained revenue, revenue at risk, campaign cost, ROI
- so sanh baseline vs scenario

#### Box 4. `artifacts_tab3_prescriptive`

- `tab3_scenario_member_level_201704.parquet`
- `tab3_scenario_summary_201704.json`
- `tab3_lever_summary_201704.parquet`
- `tab3_segment_impact_201704.parquet`
- `tab3_population_risk_shift_201704.parquet`
- `tab3_sensitivity_201704.parquet`
- `manifest.json`

### Note can ghi tren hinh

- `Tab 3 doc baseline risk tu Tab 2 artifact`
- `Tab 3 khong rebuild raw data`
- `Tab 3 la simulation layer, khong phai optimization engine da hieu chuan`

## 11. Diagram 8: Product maturity / current-state map

### Muc dich

Neu can mot hinh de tranh bi hoi "phan nao da chay that, phan nao dang la huong tich hop", dung hinh nay.

### Cach ve de nghi

Ve 3 cot:

- Cot 1: `Grounded in runtime`
- Cot 2: `Canonical in notebook artifacts`
- Cot 3: `Target productization`

### Noi dung nen dat trong tung cot

#### Cot 1. `Grounded in runtime`

- Kafka -> Spark -> ClickHouse -> FastAPI -> frontend dashboard
- Tab 1 serving table
- Tab 1 API va dashboard

#### Cot 2. `Canonical in notebook artifacts`

- feature store outputs
- Tab 1 descriptive artifacts
- Tab 2 predictive artifacts
- Tab 3 prescriptive artifacts

#### Cot 3. `Target productization`

- import Tab 2 scored artifact vao serving layer
- noi Tab 3 simulation vao baseline risk model-backed
- dong bo artifact contract voi backend / UI

### Note can ghi tren hinh

- `Khong nham notebook-canonical voi runtime-canonical`
- `Tab 2 / Tab 3 dang o giai doan artifact-first, chua phai serving-first`

## 12. Bo diagram toi thieu neu chi duoc ve 4 hinh

Neu bao cao ngan, nen uu tien 4 hinh sau:

1. `End-to-End System Pipeline`
2. `Offline Feature Store Pipeline`
3. `Tab 2 Predictive Artifact Pipeline`
4. `Tab 3 Prescriptive Artifact Pipeline`

## 13. Bo diagram day du neu bao cao ky thuat hon

Neu muon day du va dung hien trang nhat, nen ve:

1. `End-to-End System Pipeline`
2. `Offline Feature Store Pipeline`
3. `Notebook Artifact Chain`
4. `Runtime Serving Pipeline`
5. `Tab 1 Descriptive Pipeline`
6. `Tab 2 Predictive Artifact Pipeline`
7. `Tab 3 Prescriptive Artifact Pipeline`
8. `Product Maturity / Current-State Map`

## 14. Cach dat ten tren slide

Nen dung:

- `End-to-End System Pipeline`
- `Offline Feature Store Pipeline`
- `Notebook Artifact Generation Flow`
- `Near Real-Time Serving Pipeline`
- `Descriptive Analytics Pipeline (Tab 1)`
- `Predictive Artifact Pipeline (Tab 2)`
- `Prescriptive Simulation Artifact Pipeline (Tab 3)`
- `Current-State Product Maturity Map`

Khong nen dung:

- `AI Engine Pipeline`
- `Cox Survival Pipeline` neu dang ve Tab 2 hien tai
- `Decision Optimization Engine` neu dang ve Tab 3 hien tai
- `Full Production ML Lifecycle` neu chua tich hop artifact vao serving layer
