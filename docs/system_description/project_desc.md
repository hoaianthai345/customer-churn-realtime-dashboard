# KKBOX Churn Intelligence Dashboard

## 1. Muc dich cua tai lieu

Tai lieu nay la file context tong hop cho nhung agent va contributor xu ly repo nay ve sau.

Muc tieu:

- Giu duoc boi canh kinh doanh va pham vi san pham.
- Xac dinh ro stack canonical, luong du lieu va file nguon su that.
- Tach bach phan da implement, phan dang la proxy, va phan con la backlog.
- Giam viec tai sinh tai lieu cu khong con dong bo.

Neu co mau thuan giua cac tai lieu, hay xu ly theo dung pham vi cua tung tai lieu thay vi co gang bat mot file giai quyet tat ca.

## 2. Du an trong 1 cau

Day la mot he thong near real-time BI cho bai toan churn cua KKBOX, ket hop:

- offline feature engineering tren du lieu goc KKBOX;
- replay streaming cho `user_logs`;
- Spark + ClickHouse cho serving layer;
- FastAPI + frontend dashboard cho dashboard.

## 3. Nguon su that va thu tu uu tien

### 3.1. Runtime va kien truc he thong

Nguon su that:

- `README.md`
- `docs/architecture_diagrams/architecture.md`
- `docs/system_description/tab1_data_strategy.md`

Nhung file nay quy dinh:

- stack canonical;
- cach chay pipeline;
- bang serving va API hien tai;
- pham vi realtime cua Tab 1.

### 3.2. Feature semantics va snapshot logic

Nguon su that:

- `docs/system_description/kkbox_feature_catalog.md`
- `../infiniteWing/KKBOX churn/train_churn_pipeline.ipynb`
- `../infiniteWing/KKBOX churn/train_churn_pipeline_fix_report.md`

Nhung file nay quy dinh:

- grain cua feature store batch;
- logic label churn;
- cleaning rules;
- cac feature va threshold semantic cua batch pipeline.

### 3.3. Product scope va context chung

Nguon su that:

- file nay `docs/system_description/project_desc.md`

File nay quy dinh:

- muc tieu kinh doanh;
- pham vi tab san pham;
- phan nao da co, phan nao chi la proxy, phan nao chua lam;
- nhung conflict can duoc xu ly khi lam tiep.

## 4. Kien truc canonical hien tai

Stack canonical cua repo:

- Kafka
- Spark Structured Streaming
- ClickHouse
- FastAPI
- Vite React dashboard

Luong xu ly tong quat:

`Batch preload (members + transactions) -> replay user logs -> Kafka -> Spark -> ClickHouse -> FastAPI -> frontend dashboard`

Luu y quan trong:

- Khong xem Superset la stack canonical nua.
- Cac docs prompt/implementation cu theo huong Superset da bi loai bo.
- `apps/dashboard_streamlit/` co the van ton tai nhu mot prototype hoac nhanh cu, nhung khong phai duong san pham chinh.

## 5. Hai lop du lieu can phan biet ro

Repo va project hien tai co 2 lop du lieu lien quan nhau nhung khong dong nhat:

### 5.1. Lop offline feature store

Nguon:

- notebook `train_churn_pipeline.ipynb` nam o thu muc song song voi repo.

Muc dich:

- tao snapshot feature theo thang;
- aggregate `user_logs` cua thang truoc;
- enrich `members`;
- tao feature numeric va semantic cho modeling va BI.

Dau ra:

- `train_features_all.parquet`
- `test_features_201704_full.parquet`
- `bi_feature_master.parquet`
- `train_features_bi_all.parquet`
- `test_features_bi_201704_full.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

### 5.2. Lop realtime serving

Nguon:

- batch preload `members` + `transactions`;
- replay `user_logs` vao Kafka;
- Spark viet fact va KPI vao ClickHouse;
- batch materializer tao bang Tab 1 realtime.

Bang serving quan trong:

- `realtime_bi.tab1_descriptive_member_monthly`

Muc dich:

- phuc vu dashboard Tab 1 voi do tre thap;
- tach `history_precompute` va `realtime_2017_plus`.

## 6. Muc tieu kinh doanh

He thong nay giai quyet 3 nhom cau hoi:

1. Minh bach hoa hanh vi va vong doi khach hang tu du lieu raw.
2. Phat hien som tap user co nguy co churn.
3. Ho tro quyet dinh retention va product tuning bang phan tich va simulation.

Nguoi dung muc tieu:

- CMO / Marketing lead
- CPO / Product lead
- CFO / Finance lead
- Nhom Data: DE, DS, DA

## 7. Pham vi san pham canonical

Ung dung co 1 global time context va 3 tab chinh.

### 7.1. Global slicer

Muc dich:

- loc theo thang cohort het han / snapshot month.

Luu y ve semantics:

- trong offline feature store, context nay gan voi `target_month` va `last_expire_month`;
- trong serving layer Tab 1, context nay gan voi `snapshot_month` va `last_expire_date`.

### 7.2. Tab 1: Descriptive Analysis

Day la phan grounded nhat va gan voi implementation hien tai nhat.

Muc tieu:

- tra loi "chuyen gi dang xay ra voi vong doi khach hang?"

Noi dung chinh:

- KPI: total expiring users, historical churn rate, median survival, auto-renew rate
- Kaplan-Meier theo dimension
- 100% stacked bar theo segment
- boredom scatter theo `discovery_ratio` va `skip_ratio`

Tai lieu lien quan:

- `docs/system_description/tab1_data_strategy.md`
- `docs/system_description/kkbox_feature_catalog.md`

### 7.3. Tab 2: Predictive Analysis

Muc tieu san pham:

- du bao churn probability;
- tinh revenue at risk;
- uoc tinh future CLTV;
- nhin segment risk/value cho cap quan tri.

Trang thai hien tai:

- feature input layer da co trong offline feature store;
- API hien tai co scoring va tong hop predictive theo cong thuc proxy;
- chua co bang chung rang day la output tu mo hinh train chinh thuc.

Do do:

- khong duoc mo ta `churn_probability`, `predicted_future_cltv`, `hazard_ratio` la model output thuc su neu chua thay implementation train/serving ro rang;
- hien tai can xem Tab 2 la product surface co mot phan proxy analytics.

### 7.4. Tab 3: Prescriptive Simulation

Muc tieu san pham:

- cho phep dieu chinh scenario nhu manual -> auto-renew, deal -> standard, high skip -> lower skip;
- uoc tinh tac dong tai chinh va tac dong risk profile.

Trang thai hien tai:

- feature flags va segment can thiet da co trong feature store;
- API hien tai co scenario simulation va hazard histogram theo cong thuc proxy;
- chua co mo hinh Cox chinh thuc noi vao simulation.

Vi vay:

- Tab 3 hien la simulation huong san pham;
- chua duoc xem la decision engine da hieu chuan.

## 8. Trang thai implementation hien tai

### 8.1. Da grounded va nen uu tien giu dong bo

- runtime stack Kafka + Spark + ClickHouse + FastAPI + frontend dashboard
- luong replay `user_logs`
- Tab 1 serving table va API
- offline feature catalog tu notebook batch
- cleaning rules, snapshot semantics, segment semantic cua batch notebook

### 8.2. Da co nhung can gan nhan "proxy" thay vi "model that"

- predictive scoring trong `apps/api_fastapi/main.py`
- future CLTV tinh theo cong thuc heuristic
- `hazard_ratio_proxy`
- scenario simulation cho Tab 3

### 8.3. Chua canonical hoac chua hoan tat

- classification model train/serve chinh thuc
- CLTV model chinh thuc
- Cox model chinh thuc
- SHAP / primary risk driver tu model that
- mot dinh nghia duy nhat cho "transaction frequency"
- dong bo threshold segment giua offline batch va realtime serving

## 9. Conflict dang mo can agent sau luon kiem tra

### 9.1. Transaction frequency chua co dinh nghia canonical

Product muon co dimension "transaction frequency" cho Kaplan-Meier.

Nhung hien tai:

- batch notebook chua tao `txn_freq_bucket` canonical;
- realtime Tab 1 co `txn_freq_bucket` rieng trong serving layer;
- `active_segment` cua batch lai la listening activity, khong phai transaction frequency.

Bat ky ai lam tiep Tab 1 deu phai chot lai dinh nghia nay truoc khi mo rong.

### 9.2. Threshold segment batch va realtime dang lech nhau

Vi du:

- `age`
- `price_segment`
- `loyalty_segment`
- `active_segment`

Batch notebook va realtime materializer dang bucket hoa khac nhau. Neu khong dong bo, cung mot ten chart co the tra ve 2 nghia khac nhau.

### 9.3. Product brief va implementation dang khac nhau o muc model maturity

Product brief co the mo ta nhu da co:

- classification
- CLTV
- Cox / hazard ratio

Nhung implementation hien tai moi chac chan o muc:

- feature store input;
- heuristic scoring / proxy dashboard logic.

## 10. Nguyen tac lam viec cho agent sau

1. Neu thay doi stack runtime, cap nhat `README.md` va `docs/architecture_diagrams/architecture.md`.
2. Neu thay doi snapshot logic, cleaning rule, feature names, segment threshold batch, cap nhat `docs/system_description/kkbox_feature_catalog.md` va notebook lien quan.
3. Neu thay doi product scope, cap nhat file nay `docs/system_description/project_desc.md`.
4. Neu thay doi Tab 1 serving logic, cap nhat `docs/system_description/tab1_data_strategy.md`.
5. Khong tai sinh docs Superset / implementation prompt cu lam nguon su that moi.
6. Khong goi cac metric proxy la ML output that neu chua co pipeline train + serving tuong ung.
7. Khi thay doi segment threshold, uu tien dong bo ca:
   - batch feature store
   - realtime materializer
   - API / UI labels
8. Neu gap mau thuan nghiep vu, ghi ro conflict thay vi tu y chon mot nghia moi ma khong danh dau.

## 11. File va thu muc quan trong

- `README.md`: cach chay va service endpoints
- `docs/architecture_diagrams/architecture.md`: end-to-end runtime architecture
- `docs/system_description/tab1_data_strategy.md`: Tab 1 serving design
- `docs/system_description/kkbox_feature_catalog.md`: feature semantics tu notebook batch
- `docs/report_and_slides/kkbox_report_diagrams.md`: inventory diagram cho bao cao va slide
- `apps/api_fastapi/main.py`: API va logic proxy cho Tab 2 / Tab 3
- `apps/batch/materialize_tab1_realtime.py`: realtime Tab 1 materialization
- `frontend/src/pages/Index.tsx`: UI dashboard canonical
- `../infiniteWing/KKBOX churn/train_churn_pipeline.ipynb`: offline batch feature engineering notebook
- `../infiniteWing/KKBOX churn/train_churn_pipeline_fix_report.md`: ghi chu cac fix nghiep vu va data-quality

## 12. Tuyen bo cuoi cung ve pham vi hien tai

Can hieu du an nay nhu sau:

- Day la mot dashboard churn intelligence co nen tang realtime BI that.
- Tab 1 la phan on dinh va ro nghia nhat hien nay.
- Tab 2 va Tab 3 la huong san pham dung, nhung implementation hien tai van co thanh phan proxy.
- Offline feature store va realtime serving can duoc tiep tuc dong bo de tranh "cung ten metric, khac nghia du lieu".
