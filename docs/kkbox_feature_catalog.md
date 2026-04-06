# KKBOX Feature Catalog

## 1. Muc dich va nguon su that

Tai lieu nay mo ta bo feature duoc tao boi notebook [train_churn_pipeline.ipynb](/Users/anhoaithai/Documents/AHT/2. AREAS/UEH/Kì 6/Hệ hỗ trợ quản trị thông minh/Project/infiniteWing/KKBOX%20churn/train_churn_pipeline.ipynb), da doi chieu voi [train_churn_pipeline_fix_report.md](/Users/anhoaithai/Documents/AHT/2. AREAS/UEH/Kì 6/Hệ hỗ trợ quản trị thông minh/Project/infiniteWing/KKBOX%20churn/train_churn_pipeline_fix_report.md) va canh gioi nghiep vu trong [project_desc.md](/Users/anhoaithai/Documents/AHT/2. AREAS/UEH/Kì 6/Hệ hỗ trợ quản trị thông minh/Project/project-realtime-bi/docs/project_desc.md).

Thu tu uu tien khi co mau thuan:

1. Notebook + fix report la nguon su that cho logic feature store batch.
2. `project_desc.md` la nguon su that cho muc tieu san pham va UI.
3. Cac tai lieu khac chi co gia tri tham khao neu khong mau thuan voi hai nguon tren.

Tai lieu nay chi mo ta:

- Grain va semantics cua snapshot feature.
- Quy tac lam sach va tinh toan feature da co trong pipeline.
- Cach nhom feature phuc vu Tab 1, Tab 2, Tab 3.
- Nhung khoang trong va xung dot nghiep vu can chot.

Tai lieu nay khong mac dinh rang cac mo hinh du bao da ton tai chi vi `project_desc.md` neu yeu cau. Neu notebook chua tao ra mot cot, cot do duoc xem la chua co trong feature store batch hien tai.

## 2. Dau ra cua pipeline

Notebook tao cac artifact sau trong `artifacts/feature_store/`:

- `train_features_all.parquet`: tap train numeric cho modeling.
- `test_features_201704_full.parquet`: tap score numeric cho snapshot `201704`.
- `bi_feature_master.parquet`: bang master BI gom cac snapshot `201701` den `201704`.
- `train_features_bi_all.parquet`: tap train da duoc enrich them dimension semantic cho BI.
- `test_features_bi_201704_full.parquet`: tap test da enrich them dimension semantic cho BI.
- `feature_columns.csv`: danh sach cot numeric cho modeling.
- `bi_dimension_columns.csv`: danh sach dimension semantic uu tien cho dashboard.

## 3. Grain, snapshot va label

### 3.1. Grain co ban

Grain cua feature store la `1 dong / 1 msno / 1 target_month`.

Moi dong la mot snapshot trang thai cua mot user tai thoi diem quan sat lien quan den mot ky sap het han, khong phai toan bo lich su cua user.

### 3.2. Cach chon anchor transaction

Voi moi `target_month`, pipeline:

1. Lay lich su giao dich cua user truoc thang do.
2. Chon giao dich hop le cuoi cung trong lich su bang logic pha tie theo ngay giao dich, gia, do dai goi, payment method va cancel.
3. Chi giu giao dich neu `expire_month == target_month`.
4. Dung giao dich nay lam anchor de tao snapshot.

### 3.3. Dinh nghia label churn

`is_churn` duoc gan bang cach nhin 30 ngay sau `expire_date` cua anchor transaction:

- `is_churn = 0` neu user co giao dich gia han hop le trong vong duoi `30` ngay, bao gom ca truong hop gia han som truoc `expire_date` nen `gap` am.
- `is_churn = 1` neu khong co giao dich gia han, hoac khoang cach gia han `>= 30` ngay.

### 3.4. Moc thoi gian snapshot da duoc sua

Fix report da chot lai semantics sau:

- `snapshot_dt` la ngay cuoi cung cua thang truoc `target_month`.
- `days_to_expire = expire_dt - snapshot_dt`
- `membership_age_days = snapshot_dt - registration_dt`
- `days_since_last_listen = snapshot_dt - last_log_dt`

Day la thay doi quan trong vi ban cu tinh theo `transaction_date` cua giao dich neo, lam sai nghia recency va tenure.

## 4. Quy tac lam sach du lieu

Pipeline hien tai dang dung cac quy tac sau:

- `bd` chi duoc xem la hop le khi nam trong khoang `15-65`; ngoai khoang nay xem nhu thieu du lieu.
- `gender` duoc map: `male -> 1`, `female -> 2`, con lai `0`.
- `total_secs` trong `user_logs` bi cap trong khoang `0..86400` cho moi dong log.
- Neu `membership_expire_date < transaction_date` trong cung mot giao dich, pipeline sua `membership_expire_date = transaction_date` va gan co `invalid_expire_before_txn = 1`.
- Cac phep chia cho `0` dung `safe_divide`, mac dinh tra ve `0.0` hoac `-1` tuy nhom feature.
- Pipeline fail-fast neu:
  - co `days_since_last_listen < 0` du `last_log_date` hop le;
  - co `days_to_expire` nam ngoai khoang `[0, 31]`;
  - co `membership_age_days < 0` du `registration_init_time` hop le;
  - label frame cua mot thang rong hoac duplicate `msno`;
  - test cohort build ra khong khop exact set voi `sample_submission_v2.csv`.

## 5. Nhom feature chi tiet

### 5.1. Snapshot va label cohort

Day la cac cot xac dinh bo canh cua dong snapshot:

- `msno`: khoa user.
- `target_month`: thang snapshot dang phan tich.
- `is_churn`: label churn trong cua so 30 ngay sau het han.
- `expire_date`: ngay het han cua anchor transaction.
- `transaction_date`: ngay thanh toan cua anchor transaction.
- `transaction_month`: thang cua `transaction_date`.
- `expire_month`: thang cua `expire_date`.
- `last_expire_month`: alias cua `expire_month`, dung cho global slicer theo ky het han.
- `transaction_day`, `expire_day`: thanh phan ngay trong thang.
- `is_expiring_user`: co dinh = `1`, phuc vu KPI dem tap user sap het han.

### 5.2. Feature giao dich hien tai

Nhom nay mo ta hop dong hien tai cua snapshot:

- `payment_method_id`
- `payment_plan_days`
- `plan_list_price`
- `actual_amount_paid`
- `is_auto_renew`
- `invalid_expire_before_txn`
- `discount = plan_list_price - actual_amount_paid`
- `is_discount`
- `amt_per_day = actual_amount_paid / payment_plan_days`
- `expected_renewal_amount`: uu tien `actual_amount_paid`, neu khong co thi fallback `plan_list_price`
- `price_gap = plan_list_price - actual_amount_paid`
- `discount_ratio = price_gap / plan_list_price`
- `payment_to_list_ratio = actual_amount_paid / plan_list_price`
- `days_to_expire = expire_dt - snapshot_dt`
- `remaining_plan_ratio = days_to_expire / payment_plan_days`
- `price_gap_per_plan_day = price_gap / payment_plan_days`

Goc nhin nghiep vu:

- `expected_renewal_amount` la cot nen de tinh `Revenue at Risk`.
- `is_auto_renew`, `amt_per_day`, `discount_ratio` la cac bien quan trong cho retention va simulation.

### 5.3. Lich su giao dich va lich su churn

Nhom nay duoc tinh tu lich su truoc snapshot:

- `last_1_is_churn` den `last_5_is_churn`: 5 nhan churn gan nhat, thieu lich su thi `-1`.
- `churn_rate`
- `churn_count`
- `transaction_count`: so chu ky lich su da co nhan.
- `historical_transaction_rows`: tong so dong transaction da quan sat toi moc snapshot.
- `historical_paid_total`
- `historical_paid_mean`
- `historical_list_price_mean`
- `historical_cancel_count`
- `historical_cancel_rate`
- `historical_auto_renew_rate`
- `days_since_previous_transaction`
- `recent_churn_events`: tong churn trong cua so 5 ky gan nhat sau khi thay `-1 -> 0`.
- `weighted_recent_churn`: tong co trong so uu tien ky moi hon.
- `churn_rate_x_transaction_count`: feature interaction giua do dai lich su va xu huong churn.

Goc nhin nghiep vu:

- Nhom nay phan biet ro user moi, user co lich su on dinh, va user co dau hieu rut lui lap lai.
- `historical_auto_renew_rate` va `historical_cancel_rate` co gia tri cao cho segmentation va predictive modeling.

### 5.4. Aggregate `user_logs` cua thang truoc

Pipeline aggregate log cua `PREVIOUS_MONTH[target_month]` de dong bo grain ve `user-month`.

Cot aggregate co san:

- `num_25_mean`, `num_50_mean`, `num_75_mean`, `num_985_mean`, `num_100_mean`, `num_unq_mean`, `total_secs_mean`
- `num_25_sum`, `num_50_sum`, `num_75_sum`, `num_985_sum`, `num_100_sum`, `num_unq_sum`, `total_secs_sum`
- `count`: so dong log trong thang truoc.
- `last_log_date`: ngay nghe gan nhat trong thang truoc.
- `capped_log_count`: so dong bi cap `total_secs`.

### 5.5. Feature hanh vi nghe nhac suy dien

Nhung cot nay duoc tinh tu aggregate log:

- `secs_per_log`
- `unique_per_log`
- `num100_per_log`
- `listen_events_sum = num_25_sum + num_50_sum + num_75_sum + num_985_sum + num_100_sum`
- `skip_events_sum = num_25_sum + num_50_sum + num_75_sum`
- `listen_events_per_log`
- `weighted_completion_sum = 0.25*num_25_sum + 0.50*num_50_sum + 0.75*num_75_sum + 0.985*num_985_sum + 1.0*num_100_sum`
- `weighted_completion_per_log`
- `completion_ratio = weighted_completion_sum / listen_events_sum`
- `skip_ratio = skip_events_sum / listen_events_sum`
- `discovery_ratio = num_unq_sum / listen_events_sum`
- `replay_ratio = 1 - discovery_ratio`
- `avg_secs_per_unique = total_secs_sum / num_unq_sum`
- `secs_per_plan_day = total_secs_sum / payment_plan_days`
- `uniques_per_plan_day = num_unq_sum / payment_plan_days`
- `logs_per_plan_day = count / payment_plan_days`
- `secs_per_paid_amount = total_secs_sum / actual_amount_paid`
- `days_since_last_listen = snapshot_dt - last_log_dt`
- `capped_log_share = capped_log_count / count`

Goc nhin nghiep vu:

- `skip_ratio` va `discovery_ratio` la hai proxy chinh cho insight "nham chan noi dung".
- `completion_ratio` giup phan biet viec nghe het bai va hanh vi bo giua chung.
- `days_since_last_listen` va `count` la bien recency/frequency phu hop cho survival va churn scoring.

### 5.6. Feature member va calendar

Nhom nay duoc enrich tu `members_v3.csv`:

- `city`
- `bd`
- `age = bd` sau khi da clean, neu thieu thi `-1`
- `has_valid_age`
- `gender`: ma so `0/1/2`
- `gender_profile`: nhan semantic `Unknown/Male/Female`
- `registered_via`
- `registration_init_time`
- `registration_year`
- `registration_month`
- `registration_day`
- `membership_age_days = snapshot_dt - registration_dt`
- `tenure_months = membership_age_days / 30`

### 5.7. Flags va interaction phuc vu BI / simulation

Notebook tao san cac cot logic cho segmentation va scenario:

- `is_manual_renew = 1` neu `is_auto_renew = 0`
- `high_skip_flag = 1` neu `skip_ratio >= 0.5`
- `low_discovery_flag = 1` neu `discovery_ratio < 0.2`
- `deal_hunter_flag = 1` neu `0 < amt_per_day < 4.5`
- `free_trial_flag = 1` neu `expected_renewal_amount <= 0`
- `content_fatigue_flag = 1` neu vua `high_skip` vua `low_discovery`
- `auto_renew_discount_interaction = is_auto_renew * is_discount`

Day la lop feature quan trong cho:

- Tab 1 scatter + segmentation.
- Tab 2 revenue at risk va top flight-risk segment.
- Tab 3 simulation chuyen doi manual -> auto, deal -> standard, skip high -> skip low.

### 5.8. RFM score

Notebook co mot lop RFM don gian:

- `rfm_recency_score`
  - `3` neu co nghe va `days_since_last_listen <= 7`
  - `2` neu `<= 21`
  - `1` neu con co nghe
  - `0` neu khong co listening data
- `rfm_frequency_score`
  - `3` neu `count > 15`
  - `2` neu `count > 5`
  - `1` neu `count > 0`
  - `0` neu khong co log
- `rfm_monetary_score`
  - `3` neu `expected_renewal_amount >= 150`
  - `2` neu `>= 100`
  - `1` neu `> 0`
  - `0` neu bang `0`
- `rfm_total_score = recency + frequency + monetary`
- `rfm_segment_code`
- `rfm_segment`

Mapping `rfm_segment`:

- `0 -> Unclassified`
- `1 -> Low Value`
- `2 -> Mid Value`
- `3 -> High Value`

### 5.9. Segment semantic cho BI

Notebook da dong goi san cac segment code + label nhu sau.

### Age

- `age_segment_code`
- `age_segment`

Nguong:

- `15-20`
- `21-25`
- `26-35`
- `36-50`
- `51-65`
- `Unknown`

### Price

- `price_segment_code`
- `price_segment`

Nguong:

- `Free Trial / Zero Pay`
- `Deal Hunter < 4.5`
- `Standard 4.5-6.5`
- `Premium >= 6.5`
- `Unknown`

### Loyalty

- `loyalty_segment_code`
- `loyalty_segment`

Nguong:

- `New < 30d`
- `Growing 30-179d`
- `Established 180-364d`
- `Loyal >= 365d`
- `Unknown`

### Activity

- `active_segment_code`
- `active_segment`

Nguong:

- `Inactive`
- `Light 1-5 logs`
- `Active 6-15 logs`
- `Heavy > 15 logs`
- `Unknown`

### Skip

- `skip_segment_code`
- `skip_segment`

Nguong:

- `No Listening Data`
- `Low < 20%`
- `Medium 20-50%`
- `High >= 50%`

### Discovery

- `discovery_segment_code`
- `discovery_segment`

Nguong:

- `No Listening Data`
- `Habit < 20%`
- `Balanced 20-50%`
- `Explore >= 50%`

### Renewal

- `renewal_segment_code`
- `renewal_segment`

Gia tri:

- `Pay_Auto-Renew`
- `Pay_Manual`
- `Unknown`

### Segment tong hop

- `rfm_segment_code`
- `rfm_segment`
- `bi_segment_name = loyalty_segment | renewal_segment | price_segment | discovery_segment`

`BI_DIMENSION_COLUMNS` dang uu tien cac cot:

- `target_month`
- `last_expire_month`
- `age_segment`
- `gender_profile`
- `renewal_segment`
- `price_segment`
- `loyalty_segment`
- `active_segment`
- `skip_segment`
- `discovery_segment`
- `rfm_segment`
- `bi_segment_name`

## 6. Mapping vao use case san pham

### 6.1. Tab 1: Descriptive Analysis

Co the su dung truc tiep tu feature store:

- KPI:
  - `Total Expiring Users`: dem `is_expiring_user`
  - `Historical Churn Rate`: trung binh `is_churn`
  - `Auto-Renew Rate`: trung binh `is_auto_renew`
  - `Overall Median Survival`: can tinh them tu `membership_age_days` hoac survival layer rieng
- Kaplan-Meier dimensions:
  - `age_segment`
  - `gender_profile`
  - `skip_segment`
  - Can thiet ke them dimension cho "transaction frequency" neu muon dung dung mo ta san pham
- 100% stacked bars:
  - `price_segment`
  - `loyalty_segment`
  - `active_segment`
- Scatter boredom:
  - `discovery_ratio`
  - `skip_ratio`
  - `is_churn` hoac metric risk downstream de to mau

### 6.2. Tab 2: Predictive Analysis

Feature store batch hien tai moi cung cap dau vao cho mo hinh, chua cung cap dau ra du bao chinh thuc. Co the dung cac nhom sau lam input:

- Thanh toan: `is_auto_renew`, `expected_renewal_amount`, `discount_ratio`, `amt_per_day`
- Lich su churn: `last_k_is_churn`, `churn_rate`, `weighted_recent_churn`
- Hanh vi nghe: `skip_ratio`, `discovery_ratio`, `completion_ratio`, `days_since_last_listen`
- Loyalty / tenure: `membership_age_days`, `tenure_months`, `loyalty_segment`
- Value / RFM: `rfm_total_score`, `historical_paid_total`, `historical_paid_mean`

Chua co san trong notebook:

- `churn_probability`
- `predicted_future_cltv`
- `hazard_ratio`
- `primary_risk_driver`
- `shap_values`

Vi vay, Tab 2 hien chi co the dung feature store nay lam input layer, sau do noi them model serving layer.

### 6.3. Tab 3: Prescriptive Simulation

Feature store batch da co cac bien can de mo phong:

- Chuyen `Pay_Manual -> Pay_Auto-Renew`: dung `is_manual_renew`, `renewal_segment`
- Upsell `Deal Hunter / Free Trial -> Standard/Premium`: dung `price_segment`, `deal_hunter_flag`, `free_trial_flag`, `expected_renewal_amount`
- Giam met moi noi dung: dung `high_skip_flag`, `low_discovery_flag`, `content_fatigue_flag`, `skip_ratio`, `discovery_ratio`

Nhung Tab 3 van chua co trong notebook:

- cong thuc hazard/HR chinh thuc tu mo hinh Cox
- cong thuc saved revenue / incremental revenue da duoc hieu chuan
- sensitivity / ROI per 1% shift da duoc hoc tu du lieu

## 7. Khoang trong va xung dot can chot

Day la cac diem chua thong nhat giua feature store batch, mo ta san pham va serving layer hien tai:

### 7.1. "Transaction Frequency" chua co dinh nghia canonical trong notebook

`project_desc.md` muon dung dimension "Tan suat giao dich" cho Kaplan-Meier. Tuy nhien notebook batch khong tao cot segment ten `txn_freq_bucket`.

Hien tai co 3 lua chon khac nhau:

- Dung `transaction_count` roi bucket hoa them.
- Dung `historical_transaction_rows` roi bucket hoa them.
- Dung `active_segment`, nhung day la tan suat log nghe, khong phai tan suat giao dich.

Can chot 1 dinh nghia canonical de Tab 1 va feature store khop nhau.

### 7.2. Segment cua notebook batch dang khac segment cua realtime Tab 1

Serving layer realtime hien tai dang bucket hoa khac notebook o nhieu diem:

- `age`: notebook dung `15-20 / 21-25 / 26-35 / 36-50 / 51-65`, trong khi realtime layer dung `15_20 / 21_30 / 31_40 / 41_plus`.
- `price_segment`: notebook dung moc `4.5` va `6.5`, trong khi realtime layer dung `4.5` va `8.0`.
- `loyalty_segment`: notebook dung `30 / 180 / 365 ngay`, trong khi realtime layer dung `90 / 365 ngay`.
- `active_segment`: notebook bucket theo `count` log trong thang, trong khi realtime layer bucket theo `tong giay nghe / active day`.

Neu khong chot 1 bo threshold chung, Tab 1 batch view va realtime view se so sanh sai nhau.

### 7.3. Product doc mo ta model "da co", nhung notebook batch chua sinh output model

`project_desc.md` mo ta:

- Classification churn probability
- BG/NBD + Gamma-Gamma CLTV
- Cox hazard ratio

Nhung notebook batch hien tai chi dung o tang feature store + label. Day la khoang trong ve implementation, khong phai feature da co.

## 8. De xuat chuan hoa

De giam sai lech ve sau, nen chot:

1. Notebook batch nay la nguon su that cho feature store offline.
2. Realtime serving layer phai bucket hoa theo cung threshold voi batch, hoac tai lieu phai noi ro do la hai metric khac nhau.
3. Tab 2 va Tab 3 chi nen goi la "proxy" neu chua thay mo hinh hoc may that.
4. Neu muon goi ten "Transaction Frequency" tren UI, can tao them 1 cot segment ro rang ngay trong feature store.
