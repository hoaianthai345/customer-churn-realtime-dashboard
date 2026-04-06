# KKBOX Tab 2 Predictive Pipeline

## 1. Muc tieu

Tab 2 la lop du bao cho expiring cohort, dung de tra loi 3 cau hoi:

- user nao co xac suat churn cao nhat trong cua so 30 ngay sau expiry;
- doanh thu nao dang o muc rui ro cao nhat trong ky gia han toi;
- nhom feature nao dang la driver chinh cua rui ro churn.

Pipeline nay khong rebuild feature tu raw transaction hay raw user log. Nguon su that cua dau vao van la feature store batch da duoc xuat tu [train_churn_pipeline.ipynb](/Users/anhoaithai/Documents/AHT/2.%20AREAS/UEH/Ki%CC%80%206/He%CC%A3%CC%82%20ho%CC%82%CC%83%20tro%CC%9B%CC%A3%20qua%CC%89n%20tri%CC%A3%20tho%CC%82ng%20minh/Project/infiniteWing/KKBOX%20churn/train_churn_pipeline.ipynb).

## 2. Grain va label

- Grain train va score: `1 dong / 1 msno / 1 target_month`.
- Population: expiring cohort cua tung `target_month`.
- Label: `is_churn`.

Semantics label phai giong feature catalog:

- `is_churn = 0` neu user co renewal hop le trong vong duoi `30` ngay tinh tu `expire_date`, bao gom ca renewal som nen `gap` am.
- `is_churn = 1` neu khong co renewal hop le, hoac renewal dau tien den sau `>= 30` ngay.

## 3. Dau vao canonical

Notebook Tab 2 phai doc truc tiep tu feature store da rerun:

- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

Neu co `bi_feature_master.parquet` thi co the doc them de sanity check, nhung train notebook khong phu thuoc vao file do.

## 4. Nhom feature duoc phep dung

Feature cho model duoc lay tu `feature_columns.csv`, ket hop voi phan nhom nghiep vu sau:

### 4.1. Payment va value

- `payment_method_id`
- `payment_plan_days`
- `plan_list_price`
- `actual_amount_paid`
- `is_auto_renew`
- `discount`
- `is_discount`
- `amt_per_day`
- `expected_renewal_amount`
- `discount_ratio`
- `payment_to_list_ratio`

### 4.2. Lich su churn va hanh vi gia han

- `last_1_is_churn` den `last_5_is_churn`
- `churn_rate`
- `churn_count`
- `transaction_count`
- `historical_transaction_rows`
- `historical_cancel_count`
- `historical_cancel_rate`
- `historical_auto_renew_rate`
- `weighted_recent_churn`
- `recent_churn_events`
- `days_since_previous_transaction`

### 4.3. Listening behavior

- `count`
- `num_25_sum`, `num_50_sum`, `num_75_sum`, `num_985_sum`, `num_100_sum`
- `num_unq_sum`
- `total_secs_sum`
- `listen_events_sum`
- `skip_events_sum`
- `skip_ratio`
- `discovery_ratio`
- `completion_ratio`
- `replay_ratio`
- `days_since_last_listen`
- `capped_log_share`
- `secs_per_log`
- `unique_per_log`
- `avg_secs_per_unique`

### 4.4. Loyalty / member context

- `city`
- `gender`
- `registered_via`
- `age`
- `has_valid_age`
- `days_to_expire`
- `membership_age_days`
- `tenure_months`
- `remaining_plan_ratio`

### 4.5. Segment code va BI flags

- `age_segment_code`
- `price_segment_code`
- `loyalty_segment_code`
- `active_segment_code`
- `skip_segment_code`
- `discovery_segment_code`
- `renewal_segment_code`
- `rfm_segment_code`
- `deal_hunter_flag`
- `free_trial_flag`
- `content_fatigue_flag`
- `high_skip_flag`
- `low_discovery_flag`
- `is_manual_renew`
- `auto_renew_discount_interaction`
- `churn_rate_x_transaction_count`

Khong dung cac cot downstream cua Tab 1 nhu `risk_score`, `customer_segment`, `safe_base_revenue`, `revenue_at_risk_30d` de train model Tab 2.

## 5. Time split cho train va score

Voi bo du lieu hien tai:

- train fit: `target_month in [201701, 201702]`
- validation: `target_month = 201703`
- retrain full train: `target_month in [201701, 201702, 201703]`
- score: `target_month = 201704`

Ly do:

- giu dung thu tu thoi gian;
- tranh leakage giua thang train va validation;
- giong setup product hien tai: dung lich su de du bao cohort thang 4/2017.

## 6. Mo hinh va output duoc phep overclaim

Notebook Tab 2 nay train mo hinh `predictive churn classification`.

Canonical outputs duoc phep tao:

- `churn_probability_raw`
- `churn_probability`
- `risk_percentile`
- `risk_decile`
- `risk_band`
- `expected_revenue_at_risk_30d = churn_probability * expected_renewal_amount`
- `expected_retained_revenue_30d = (1 - churn_probability) * expected_renewal_amount`

Optional outputs neu co du thu vien va runtime:

- `top_feature_importance`
- `feature_group_importance`
- `primary_risk_driver` theo SHAP sample hoac contribution proxy

Nhung cot sau khong duoc goi la output that neu notebook chua train rieng:

- `predicted_future_cltv`
- `hazard_ratio`
- `cox_survival_curve`

Neu can 3 cot nay, phai co notebook / service rieng va artifact rieng.

## 7. Train flow trong notebook Kaggle

### 7.1. Load va validate schema

- resolve `feature_store/` tu Kaggle input dataset;
- load `train_features_bi_all.parquet`, `test_features_bi_201704_full.parquet`;
- load `feature_columns.csv`;
- fail fast neu thieu cot bat buoc: `msno`, `target_month`, `is_churn`, `expected_renewal_amount`.

### 7.2. Chon feature list

- bat dau tu `feature_columns.csv`;
- giu lai chi cac cot thuc su ton tai trong train va test;
- loai bo `msno`, `is_churn`, `transaction_date`, `expire_date`;
- loai bo cac cot string downstream neu co.

### 7.3. Train LightGBM

- objective: binary classification;
- metric uu tien: `binary_logloss`, sau do moi den `roc_auc`, `pr_auc`;
- `scale_pos_weight` duoc tinh tu positive rate cua train split;
- early stopping tren validation month `201703`.

### 7.4. Probability calibration

- fit calibration tren validation prediction;
- neu calibration lam log loss tot hon thi xuat ban calibrated;
- neu khong, giu raw probability.

### 7.5. Retrain va score

- retrain model tren full train `201701-201703`;
- score cho `201704`;
- tinh `risk_percentile`, `risk_decile`, `risk_band`;
- tinh `expected_revenue_at_risk_30d`.

## 8. Artifact xuat ra

Notebook phai xuat vao `artifacts_tab2_predictive/` tren Kaggle working directory:

- `tab2_validation_metrics.json`
- `tab2_feature_columns_used.csv`
- `tab2_feature_importance_lightgbm.csv`
- `tab2_feature_group_importance.csv`
- `tab2_valid_scored_201703.parquet`
- `tab2_test_scored_201704.parquet`
- `tab2_model_summary.json`
- `tab2_lightgbm_model.txt`

Optional:

- `tab2_valid_scored_with_driver_sample.parquet`
- `tab2_test_scored_with_driver_sample.parquet`

## 9. Mapping vao Tab 2 UI

### 9.1. Leaderboard / ranking

Dung:

- `msno`
- `churn_probability`
- `expected_renewal_amount`
- `expected_revenue_at_risk_30d`
- `risk_band`
- `price_segment`
- `renewal_segment`
- `loyalty_segment`

### 9.2. Segment risk view

Aggregate theo:

- `price_segment`
- `renewal_segment`
- `active_segment`
- `skip_segment`
- `bi_segment_name`

Metric:

- average `churn_probability`
- sum `expected_revenue_at_risk_30d`
- user count

### 9.3. Driver view

Neu co SHAP / contribution proxy:

- `primary_risk_driver`
- `feature_group_importance`

Neu chua co SHAP:

- chi hien `feature_group_importance` o muc toan cuc.

## 10. Gioi han can ghi ro

- Pipeline nay la model churn classifier cho expiring cohort, khong phai survival model.
- Khong duoc doi ten `expected_revenue_at_risk_30d` thanh `CLTV`.
- Khong duoc doi `feature_group_importance` thanh row-level explanation neu chua co SHAP.
- Notebook nay phai dung output feature store da rerun; khong build lai cohort tu raw transaction / raw logs.
