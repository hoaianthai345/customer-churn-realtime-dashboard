# KKBOX Predictive Dashboard

## 1. Vai tro cua tai lieu

Tai lieu nay mo ta nghiep vu cho Tab 2: predictive analysis.

Tai lieu nay khong phai nguon su that cao nhat cho feature semantics. Thu tu uu tien khi co mau thuan:

1. `docs/system_description/kkbox_feature_catalog.md`
2. `project-realtime-bi/notebooks/feature_prep.ipynb`
3. `docs/system_description/kkbox_tab2_predictive_pipeline.md`
4. `team_code/tab2/kkbox-train-predictive-tab.ipynb`
5. file nay `docs/system_description/predictive.md`

Muc tieu cua file nay la:

- mo ta dung bai toan kinh doanh cua Tab 2;
- chot ro phan nao da grounded tu notebook va feature store;
- tranh overclaim cac output chua duoc train va xuat artifact that.

## 2. Bai toan kinh doanh cua Tab 2

Tab 2 tra loi 4 cau hoi:

1. Trong ky gia han ke tiep, user nao co nguy co churn cao nhat?
2. Trong cohort sap het han, doanh thu nao dang o muc rui ro cao nhat?
3. Rui ro dang tap trung o segment nao?
4. Dau la cac nhom feature co suc anh huong lon nhat den du bao churn?

Noi dung trung tam cua Tab 2 la:

- churn classification cho expiring cohort;
- revenue-at-risk estimation cho chu ky gia han tiep theo;
- segment prioritization cho retention;
- global model explanation o muc feature group.

## 3. Pham vi phan tich da duoc chot

### 3.1. Grain

- `1 row / 1 msno / 1 target_month`

### 3.2. Population

- chi phan tich `expiring cohort` cua tung `target_month`

### 3.3. Label

Label `is_churn` phai giong feature catalog:

- `is_churn = 0` neu co renewal hop le trong vong duoi `30` ngay tinh tu `expire_date`, bao gom ca gap am
- `is_churn = 1` neu khong co renewal hop le, hoac renewal dau tien xay ra sau `>= 30` ngay

### 3.4. Moc thoi gian dang dung cho notebook train

Theo notebook Tab 2 hien tai:

- train fit: `201701`, `201702`
- validation: `201703`
- retrain full train: `201701-201703`
- score: `201704`

Vi vay, Tab 2 dang duoc dat theo bai toan:

- dung lich su den het `201703`
- du bao churn cho cohort sap het han cua `201704`

## 4. Dau vao grounded

### 4.1. Dau vao canonical

Pipeline batch trong `feature_prep.ipynb` dung o buoc xuat feature store, khong train model trong notebook nay.

Feature store canonical xuat ra cac artifact:

- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

### 4.2. Nhom feature grounded

Nhung nhom feature da co co so ro rang trong `feature_prep.ipynb`:

- payment / value:
  - `is_auto_renew`
  - `expected_renewal_amount`
  - `amt_per_day`
  - `discount_ratio`
  - `payment_to_list_ratio`
- churn history / renewal history:
  - `last_k_is_churn`
  - `churn_rate`
  - `weighted_recent_churn`
  - `transaction_count`
- listening behavior:
  - `skip_ratio`
  - `discovery_ratio`
  - `completion_ratio`
  - `days_since_last_listen`
  - `count`
- member / loyalty:
  - `age`
  - `membership_age_days`
  - `tenure_months`
  - `remaining_plan_ratio`
- BI flags / segments:
  - `price_segment`
  - `renewal_segment`
  - `loyalty_segment`
  - `active_segment`
  - `skip_segment`
  - `discovery_segment`
  - `rfm_segment`
  - `is_manual_renew`
  - `deal_hunter_flag`
  - `free_trial_flag`
  - `content_fatigue_flag`
  - `high_skip_flag`
  - `low_discovery_flag`
  - `bi_segment_name`

## 5. Dau ra predictive da grounded

Notebook `team_code/tab2/kkbox-train-predictive-tab.ipynb` da chot mot pipeline train churn classifier tren feature store batch.

Nhung output duoc xem la grounded neu notebook duoc run va xuat artifact:

- `churn_probability_raw`
- `churn_probability`
- `risk_percentile`
- `risk_decile`
- `risk_band`
- `expected_revenue_at_risk_30d`
- `expected_retained_revenue_30d`
- `feature_importance`
- `feature_group_importance`

Cong thuc co the trinh bay trong bao cao:

- `expected_revenue_at_risk_30d = churn_probability * expected_renewal_amount`
- `expected_retained_revenue_30d = (1 - churn_probability) * expected_renewal_amount`

Risk band trong notebook train hien tai dang bucket hoa:

- `Very High`: `churn_probability >= 0.80`
- `High`: `>= 0.60`
- `Medium`: `>= 0.40`
- `Low`: `>= 0.20`
- `Very Low`: `< 0.20`

Neu bao cao muon dung nguong khac, phai ghi ro do la KPI trinh bay rieng, khong phai `risk_band` canonical.

## 6. Nhung gi co the ke trong bao cao

### 6.1. Executive KPIs

Co the dung:

- `Projected Churn Rate`
- `Predicted Revenue at Risk`
- `Projected Retained Revenue`
- `Top Risk Segment`

Neu can them KPI "High Flight-Risk Users", phai chot ro nguong tu probability hoac theo `risk_band`.

### 6.2. Phan tich segment

Co the aggregate theo:

- `price_segment`
- `renewal_segment`
- `loyalty_segment`
- `active_segment`
- `skip_segment`
- `discovery_segment`
- `bi_segment_name`

Metric nen dung:

- user count
- average `churn_probability`
- tong `expected_revenue_at_risk_30d`
- tong `expected_retained_revenue_30d`

### 6.3. Goc nhin hanh vi va product

Co the dung cac bien sau de ke cau chuyen nghiep vu:

- `days_since_last_listen`
- `skip_ratio`
- `discovery_ratio`
- `content_fatigue_flag`
- `high_skip_flag`
- `low_discovery_flag`

Nhung nen xem day la "behavioral risk signal", khong nen goi la "causal proof".

### 6.4. Driver view

Grounded nhat hien nay la:

- `feature_importance`
- `feature_group_importance`

Neu chua co SHAP, chi nen noi:

- "nhom feature nao co suc anh huong lon nhat den mo hinh"

khong nen noi:

- "ly do chinh xac vi sao tung user roi bo"
- "dong gop tien mat cua tung feature" neu chua co attribution pipeline rieng

## 7. Nhung diem khong nen overclaim

Nhung noi dung sau chua duoc xem la grounded chi dua tren bo notebook va docs hien tai:

- `predicted_future_cltv`
- `hazard_ratio`
- `cox_survival_curve`
- `per-user SHAP explanation`
- `primary_risk_driver` o muc chinh thuc cho tung customer
- `feature importance quy doi truc tiep ra revenue loss`

Neu can dua vao bao cao, phai gan nhan ro:

- `future-state`
- `experimental`
- hoac `optional extension`

## 8. Bo chart nghiep vu de xuat

Neu viet bao cao theo dung pham vi hien tai, Tab 2 nen uu tien:

1. KPI cards:
   - projected churn rate
   - predicted revenue at risk
   - projected retained revenue
   - top segment at risk
2. Risk band / decile distribution
3. Value vs risk matrix
4. Revenue leakage treemap theo segment
5. Feature group importance
6. Engagement watchlist theo `days_since_last_listen`, `skip_ratio`, `discovery_ratio`

## 9. Cach dat ten va wording

Nen dung:

- `Predictive churn classifier`
- `Expiring cohort forecast`
- `Expected revenue at risk`
- `Feature-group importance`
- `Behavioral risk signals`

Khong nen dung neu chua co artifact tuong ung:

- `AI explainability engine`
- `CLTV model`
- `Hazard model`
- `Root-cause proof`

## 10. Tuyen bo cuoi cung

Tab 2 hien da co mot duong di nghiep vu ro rang:

- feature store batch tao input layer canonical;
- notebook train Tab 2 tao churn probability va revenue-at-risk artifacts;
- bao cao co the viet duoc theo huong predictive churn cho expiring cohort.

Nhung de giu dong bo voi he thong va notebook:

- chi nen ke nhung output da co artifact ro rang;
- tach bach giua "du bao churn" va "future CLTV / hazard";
- xem SHAP va per-user driver la mo rong ve sau, khong phai phan da chot.
