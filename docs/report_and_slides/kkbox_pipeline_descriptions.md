# KKBOX Pipeline Descriptions For Drawing

Tai lieu nay viet theo kieu "box -> arrow -> box" de co the dung truc tiep khi ve diagram cho bao cao.

Nguyen tac can giu khi ve:

- `docs/system_description/kkbox_feature_catalog.md` la nguon su that cao nhat cho feature semantics.
- `docs/system_description/project_desc.md` chi dung de giu context san pham va boundary implementation.
- Khong ve Tab 2 hay Tab 3 nhu da co mo hinh survival/Cox neu artifact thuc te chua co.
- Neu can ghi chu tren slide, uu tien dung tu "feature store", "predictive scoring", "simulation layer", tranh overclaim "AI engine" neu chua co bang chung implementation.

## 1. Pipeline tong the cua bai

### Muc dich

Day la pipeline tong hop cap cao nhat, dung o slide mo dau de cho thay du an co 3 lop:

1. lop du lieu goc va feature engineering offline;
2. lop serving near real-time cho dashboard;
3. lop analytics san pham cho Tab 1, Tab 2, Tab 3.

### Cach ve de nghi

Ve 3 lane ngang:

- Lane 1: Offline batch layer
- Lane 2: Realtime serving layer
- Lane 3: Product analytics layer

### Chuoi box nen ve

`KKBOX raw files`
-> `Batch cleaning + monthly feature engineering`
-> `Feature store`

song song:

`User log replay`
-> `Kafka`
-> `Spark Structured Streaming`
-> `ClickHouse serving tables`

sau do hop lai:

`Feature store + ClickHouse`
-> `FastAPI`
-> `Next.js dashboard`
-> `Tab 1 / Tab 2 / Tab 3`

### Y nghia tung box

- `KKBOX raw files`: `members_v3`, `transactions_v2`, `user_logs_v2`, `train_v2`.
- `Batch cleaning + monthly feature engineering`: notebook batch tao snapshot cohort theo thang.
- `Feature store`: dau vao canonical cho BI va predictive model.
- `User log replay`: phat lai log theo thoi gian de mo phong near real-time.
- `Kafka + Spark + ClickHouse`: lop serving cho dashboard.
- `FastAPI + Next.js`: lop API va UI.
- `Tab 1 / Tab 2 / Tab 3`: 3 surface san pham cua bai.

### Note can ghi tren hinh

- `Feature semantics: canonical from feature catalog`
- `Realtime layer phuc vu dashboard, khong thay the feature store batch`

## 2. Pipeline feature engineering offline

### Muc dich

Day la pipeline quan trong nhat ve nghiep vu, nen dung o phan data understanding / methodology.
No tra loi cau hoi: tu du lieu KKBOX goc, lam sao tao ra `1 row / 1 msno / 1 target_month`.

### Cach ve de nghi

Ve tu trai sang phai, co 6 cum lon:

1. Input raw tables
2. Data cleaning
3. Anchor transaction selection
4. Previous-month log aggregation
5. Feature assembly
6. Labeling + export

### Chuoi box nen ve

`members_v3 + transactions_v2 + user_logs_v2 + train_v2`
-> `Cleaning rules`
-> `Monthly cohort builder`
-> `Anchor transaction selection`
-> `Previous-month user_log aggregation`
-> `Feature assembly`
-> `Churn labeling`
-> `Parquet / CSV artifacts`

### Mo ta chi tiet de viet trong box

#### Box 1. `Cleaning rules`

Nen ghi cac bullet ngan:

- chuan hoa kieu du lieu ngay thang
- loai bo age khong hop le
- tinh ratio va feature suy dien co guard chia 0
- dong bo moc snapshot theo `target_month`

#### Box 2. `Monthly cohort builder`

Nen ghi:

- lap qua cac `target_month`
- population la expiring cohort cua thang do
- grain dau ra la `1 user / 1 target_month`

#### Box 3. `Anchor transaction selection`

Nen ghi:

- lay lich su giao dich truoc `target_month`
- tim block giao dich cua ngay cuoi cung
- chon 1 anchor transaction bang tuple tie-break
- chi giu row neu `expire_month == target_month`

#### Box 4. `Previous-month user_log aggregation`

Nen ghi:

- chi tong hop log cua thang truoc snapshot
- tinh `count`, `total_secs_sum`, `num_unq_sum`
- tinh ratio: `skip_ratio`, `discovery_ratio`, `completion_ratio`, `replay_ratio`

#### Box 5. `Feature assembly`

Nen tach 4 nhom:

- payment / value features
- churn history / renewal history
- listening behavior features
- member / segment / flag features

#### Box 6. `Churn labeling`

Nen ghi:

- tim renewal hop le dau tien sau `expire_date`
- `gap < 30` la non-churn, bao gom ca `gap` am
- `no renewal` hoac `gap >= 30` la churn

#### Box 7. `Parquet / CSV artifacts`

Nen liet ke:

- `train_features_all.parquet`
- `test_features_201704_full.parquet`
- `bi_feature_master.parquet`
- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

### Note can ghi tren hinh

- `Canonical semantics come from kkbox_feature_catalog.md`
- `Negative renewal gap is still non-churn`

## 3. Pipeline serving near real-time

### Muc dich

Pipeline nay dung de giai thich kien truc he thong va tai sao dashboard co the cap nhat theo replay.

### Cach ve de nghi

Ve 2 nhanh:

- nhanh preload static data
- nhanh replay user logs

sau do hop vao ClickHouse va di len API/UI.

### Chuoi box nen ve

Nhanh 1:

`members + transactions`
-> `Batch preload`
-> `ClickHouse static tables`

Nhanh 2:

`user_logs replay`
-> `Kafka topic: user_log_events`
-> `Spark Structured Streaming`
-> `fact tables + realtime KPI tables in ClickHouse`

Hop:

`ClickHouse`
-> `Batch materializer`
-> `tab1_descriptive_member_monthly`
-> `FastAPI`
-> `Next.js dashboard`

### Y nghia nghiep vu

- preload static data de dashboard co nen thanh vien va giao dich
- replay log de mo phong near real-time behavior
- Spark tinh fact va KPI tu dong
- materializer tong hop lai thanh bang de Tab 1 query nhanh

### Box can danh dau rieng

- `Batch preload`: one-time load cho static tables
- `Replay`: mo phong dong thoi gian tu `2017-03`
- `Batch materializer`: khong train model, chi tong hop serving table

### Note can ghi tren hinh

- `Realtime layer phuc vu Tab 1 va dashboard serving`
- `Khong thay the monthly feature store batch`

## 4. Pipeline Tab 1: Descriptive analysis

### Muc dich

Pipeline nay dung de ve logic cua Tab 1, tu serving table ra chart.

### Cach ve de nghi

Ve theo truc:

`Serving table`
-> `Metric layer`
-> `Visualization layer`

### Chuoi box nen ve

`Feature store / Tab 1 serving table`
-> `Tab 1 business metrics`
-> `Kaplan-Meier + stacked bar + scatter`
-> `Descriptive dashboard`

### Mo ta tung box

#### Box 1. `Feature store / Tab 1 serving table`

Nen ghi:

- grain `1 row / user / snapshot_month`
- cac cot segment: `price_segment`, `loyalty_segment`, `active_segment`, `skip_segment`
- cac cot ratio: `discovery_ratio`, `skip_ratio`

#### Box 2. `Tab 1 business metrics`

Nen ghi:

- `Total Expiring Users`
- `Historical Churn Rate`
- `Auto-Renew Rate`
- `Median Survival`
- customer composition / movement metrics neu can

#### Box 3. `Visualization layer`

Nen ghi:

- Kaplan-Meier by dimension
- 100% stacked segment bar
- boredom scatter
- trend by month

### Note can ghi tren hinh

- `Tab 1 uu tien descriptive analytics, khong phai predictive model`
- `Neu co risk coloring thi do la downstream analytic layer, khong phai feature canonical`

## 5. Pipeline Tab 2: Predictive analysis

### Muc dich

Pipeline nay dung cho phan methodology predictive churn.
No nen ve ro su khac nhau giua:

- feature input layer;
- model training layer;
- scoring output layer.

### Cach ve de nghi

Ve theo 3 cum:

1. input feature store
2. model train + validation
3. score + export artifact

### Chuoi box nen ve

`Feature store BI parquet`
-> `Feature selection from feature_columns.csv`
-> `Time-based split`
-> `LightGBM churn classifier`
-> `Probability calibration`
-> `Score 201704 expiring cohort`
-> `Tab 2 predictive artifacts`

### Mo ta tung box

#### Box 1. `Feature store BI parquet`

Nen ghi:

- doc `train_features_bi_all.parquet`
- doc `test_features_bi_201704_full.parquet`
- khong rebuild raw feature tu `transactions` hay `user_logs`

#### Box 2. `Feature selection from feature_columns.csv`

Nen ghi:

- giu feature numeric / flag co trong train va test
- loai bo ID va cot downstream cua Tab 1
- giu nhom payment, churn history, listening, loyalty, segment code

#### Box 3. `Time-based split`

Nen ghi:

- train: `201701-201702`
- validation: `201703`
- retrain full train: `201701-201703`
- score: `201704`

#### Box 4. `LightGBM churn classifier`

Nen ghi:

- binary classification
- objective: churn probability cho expiring cohort
- early stopping tren validation month

#### Box 5. `Probability calibration`

Nen ghi:

- isotonic calibration tren validation prediction
- chi giu ban calibrated neu log loss tot hon

#### Box 6. `Score 201704 expiring cohort`

Nen ghi:

- tao `churn_probability`
- tao `risk_decile`, `risk_band`
- tinh `expected_revenue_at_risk_30d`

#### Box 7. `Tab 2 predictive artifacts`

Nen ghi:

- scored parquet
- validation metrics json
- feature importance
- feature group importance
- model summary

### Note can ghi tren hinh

- `Day la churn classifier, khong phai Cox survival model`
- `Khong doi ten expected_revenue_at_risk_30d thanh CLTV`

## 6. Pipeline Tab 3: Prescriptive simulation

### Muc dich

Pipeline nay dung de giai thich lop simulation cua bai.
Can ve theo dung boundary: Tab 3 nhan feature va predictive score, sau do ap kich ban kinh doanh de uoc tinh tac dong.

### Cach ve de nghi

Ve theo 4 box lon:

1. canonical inputs
2. scenario lever layer
3. impact engine
4. simulation outputs

### Chuoi box nen ve

`Feature store + Tab 2 risk outputs`
-> `Scenario levers`
-> `Impact estimation layer`
-> `Scenario comparison outputs`

### Mo ta tung box

#### Box 1. `Feature store + Tab 2 risk outputs`

Nen ghi:

- canonical feature: `price_segment`, `renewal_segment`, `skip_ratio`, `discovery_ratio`
- business flag: `deal_hunter_flag`, `free_trial_flag`, `content_fatigue_flag`, `is_manual_renew`
- predictive input: `churn_probability`, `expected_revenue_at_risk_30d`
- neu chua co model serving that thi thay bang risk proxy va ghi ro tren hinh

#### Box 2. `Scenario levers`

Nen ghi:

- `Manual -> Auto-Renew`
- `Deal / Free Trial -> Standard / Premium`
- `High Skip -> Lower Skip`
- `Low Discovery -> Higher Discovery`

#### Box 3. `Impact estimation layer`

Nen ghi:

- ap rule thay doi segment / behavior gia dinh
- tinh lai risk profile theo scenario
- uoc tinh revenue retained / revenue recovered

#### Box 4. `Scenario comparison outputs`

Nen ghi:

- baseline vs scenario churn risk
- baseline vs scenario revenue at risk
- waterfall saved revenue
- scenario ranking

### Note can ghi tren hinh

- `Neu chua co mo hinh Cox that, ghi ro day la simulation / proxy layer`
- `Tab 3 khong duoc ve nhu optimization engine da hieu chuan`

## 7. Pipeline danh cho phan evaluation trong bao cao

### Muc dich

Neu anh muon ve them 1 hinh rieng cho methodology, day la pipeline danh gia mo hinh Tab 2.

### Chuoi box nen ve

`Validation scored data`
-> `ROC-AUC / PR-AUC / Logloss`
-> `Calibration check`
-> `Risk decile lift`
-> `Interpretation for management`

### Y nghia

- khong chi bao cao accuracy
- phai cho thay calibration va kha nang ranking customer risk
- noi ket qua ky thuat voi quyet dinh kinh doanh

## 8. Bo pipeline toi thieu neu bao cao ngan

Neu anh chi ve 4 hinh pipeline, nen chon:

1. Pipeline tong the cua bai
2. Pipeline feature engineering offline
3. Pipeline Tab 2 predictive analysis
4. Pipeline Tab 3 prescriptive simulation

Neu bao cao ky thuat hon, them:

5. Pipeline serving near real-time
6. Pipeline Tab 1 descriptive analysis
7. Pipeline evaluation cho Tab 2

## 9. Cach dat ten tren slide

Nen dat ten ngan, ro, de giang vien nhin la hieu:

- `End-to-End System Pipeline`
- `Monthly Feature Engineering Pipeline`
- `Near Real-Time Serving Pipeline`
- `Descriptive Analytics Pipeline (Tab 1)`
- `Predictive Churn Pipeline (Tab 2)`
- `Prescriptive Simulation Pipeline (Tab 3)`
- `Model Evaluation Pipeline`

Khong nen dat ten:

- `AI Pipeline`
- `Big Data Intelligent Engine`
- `Full ML Lifecycle` neu trong bai chua cover het phan do
