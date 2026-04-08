# Productization Plan Sau Khi Co Ket Qua Cac File Notebook

## Muc tieu

Tai lieu nay dung de chot truoc cac phan viec tiep theo sau khi cac notebook da xuat ket qua, de nhom co the chuyen ngay sang san pham hoan chinh ma khong can quay lai tranh luan ve kien truc hay pham vi.

Muc tieu cuoi cung:

- artifact tu notebook duoc dua vao he thong chinh thuc
- dashboard 3 tab dung du lieu dung scope
- API va UI chay tren contract on dinh
- co the run end-to-end thanh mot san pham demo hoan chinh

## 1. Dau vao can co truoc khi bat dau

### 1.1. Output tu notebook feature store

Can co day du cac file:

- `train_features_all.parquet`
- `train_features_bi_all.parquet`
- `test_features_201704_full.parquet`
- `test_features_bi_201704_full.parquet`
- `bi_feature_master.parquet`
- `feature_columns.csv`
- `bi_dimension_columns.csv`

### 1.2. Output tu notebook Tab 2 predictive

Can co day du cac file:

- `tab2_validation_metrics.json`
- `tab2_feature_columns_used.csv`
- `tab2_feature_importance_lightgbm.csv`
- `tab2_feature_group_importance.csv`
- `tab2_valid_scored_201703.parquet`
- `tab2_test_scored_201704.parquet`
- `tab2_model_summary.json`
- `tab2_lightgbm_model.txt`

### 1.3. Dinh huong cho Tab 3

Notebook Tab 3 nen duoc xem la prototype nghiep vu. Ban production cua Tab 3 khong nen doc lai raw CSV hay tu dung cohort rieng, ma nen dung:

- feature store batch lam state + segment + behavior baseline
- output Tab 2 lam baseline risk
- backend simulation de tinh scenario impact

## 2. Nguyen tac trien khai

- Khong de backend doan file artifact nao la file moi nhat.
- Khong de Tab 2 tiep tuc dung duong heuristic lam duong chinh neu da co model output that.
- Khong de Tab 3 tiep tuc dung prototype logic neu da co baseline risk tu Tab 2.
- Khong de Tab 1 lan sang predictive scope.
- Mọi artifact dua vao product deu phai co schema va contract ro rang.

## 3. Pha 1: Freeze artifact contract

### Muc tieu

Chot mot contract on dinh de moi lan notebook rerun xong la backend co the dung ngay.

### Viec can lam

- Tao hai thu muc canonical:
- `artifacts/feature_store/`
- `artifacts/tab2_predictive/`

- Them `manifest.json` cho tung nhom artifact, bao gom:
- artifact version
- notebook version hoac run date
- thang train / validation / score
- danh sach file
- hash model hoac model id
- danh sach cot quan trong

- Chot quy uoc ten file:
- khong dat ten mo ho
- khong de nhieu phien ban lung tung trong cung 1 folder production

### Dau ra

- Contract artifact co the duoc doc tu dong boi batch loader va API.

## 4. Pha 2: Import artifact vao serving layer

### Muc tieu

Bien ket qua notebook thanh input chinh thuc cua he thong production.

### Viec can lam

- Tao batch loader moi trong `project-realtime-bi/apps/batch/` de:
- validate schema parquet/csv
- import Tab 2 scored parquet vao ClickHouse
- build aggregate cho dashboard

- Tao them bang ClickHouse moi trong `project-realtime-bi/infra/clickhouse/init/`:
- `009_tab2_predictive_member_monthly.sql`
- `010_tab2_segment_summary_monthly.sql`

- Neu can precompute baseline cho Tab 3:
- `011_tab3_baseline_monthly.sql`

### Dau ra

- ClickHouse co bang scored cho Tab 2
- co bang aggregate theo segment cho UI dung truc tiep

## 5. Pha 3: Chuyen Tab 2 tu proxy sang model-backed

### Muc tieu

Tab 2 phai dung output model that, khong con dung cong thuc heuristic lam duong chinh.

### Viec can lam

- Sua `project-realtime-bi/apps/api_fastapi/main.py`:
- bo `_score_model_row` heuristic khoi duong chinh
- doc truc tiep du lieu tu bang Tab 2 scored
- tra ve:
  - `churn_probability`
  - `risk_band`
  - `expected_revenue_at_risk_30d`
  - `expected_retained_revenue_30d`
  - `feature_group_importance`

- Co the giu fallback heuristic mode cho local demo, nhung:
- phai gan nhan ro
- khong dung lam duong product mac dinh

### Dau ra

- `/api/v1/tab2/predictive` dung du lieu model-backed
- Tab 2 tren UI khong con la predictive proxy

## 6. Pha 4: Rewire Tab 3 theo baseline Tab 2

### Muc tieu

Tab 3 phai simulate tren baseline risk that, khong build logic rieng tach roi he thong.

### Viec can lam

- Backend Tab 3 doc:
- state + segment tu feature store / serving layer
- baseline risk tu Tab 2 scored table

- Simulate 3 lever chinh:
- manual -> auto-renew
- deal/free trial -> standard/premium
- high skip / low discovery -> healthy listening behavior

- Tinh lai:
- baseline revenue at risk
- simulated revenue at risk
- saved revenue
- incremental upsell revenue
- scenario churn change
- sensitivity by intervention

- `campaign_cost`, `gross_gain`, `net_roi` chi mo khi da co cost model that.

### Dau ra

- `/api/v1/tab3/prescriptive` dung baseline model-backed
- Tab 3 tro thanh simulation co logic dong bo voi Tab 2

## 7. Pha 5: Don lai Tab 1 dung scope descriptive

### Muc tieu

Tab 1 chi con lam descriptive analysis, khong lan sang predictive.

### Viec can lam

- Giu dung 4 nhom noi dung:
- KPI descriptive
- Kaplan-Meier
- 100% stacked bar
- boredom scatter

- Khong de cac noi dung sau lam trung tam cua Tab 1:
- `risk_score`
- `projected_risk_flag`
- `revenue_at_risk_30d`
- `customer_segment`

- Neu van muon hien 1 so business overlay o Tab 1:
- phai danh dau ro la heuristic overlay
- khong goi do la predictive model output

### Dau ra

- Tab 1 dung scope canonical
- de tach biet ro voi Tab 2

## 8. Pha 6: Dong bo semantic giua batch va realtime

### Muc tieu

Tranh tinh trang cung ten metric nhung khac nghia du lieu.

### Viec can lam

- Chot lai threshold canonical cho:
- age
- price
- loyalty
- activity

- Chot dinh nghia canonical cho `transaction frequency`:
- dung `transaction_count`
- hoac `historical_transaction_rows`
- hoac ghi ro day la dimension chi co o serving layer realtime

- Neu chua dong bo duoc ngay:
- cap nhat tai lieu no ro batch va realtime la 2 lop semantics khac nhau

### Dau ra

- batch view va realtime view co the so sanh dung
- giam risk lech nghiep vu khi viet bao cao va demo

## 9. Pha 7: Noi dashboard frontend vao payload that

### Muc tieu

UI phai phan anh dung maturity cua he thong.

### Viec can lam

- Sua `project-realtime-bi/frontend/src/pages/Index.tsx` va `project-realtime-bi/frontend/src/hooks/useDashboardData.ts`:
- Tab 2 dung API model-backed
- Tab 3 dung API simulation moi
- wording tren UI phu hop voi data that

- Neu fallback mode van ton tai:
- UI phai hien ro la fallback

- Neu model-backed da co:
- loai bo wording `proxy`, `heuristic` khoi UI chinh

### Dau ra

- Dashboard frontend hien thi 3 tab day du va dung contract

## 10. Pha 8: Gan vao one-command pipeline

### Muc tieu

Sau khi co artifact, viec dua len stack phai thanh mot quy trinh de chay lai duoc.

### Viec can lam

- Cap nhat `project-realtime-bi/scripts/run_pipeline.sh` de:
- load feature store artifact neu co
- import Tab 2 artifact neu co
- materialize summary table cho UI

- Cap nhat `project-realtime-bi/scripts/validate_stack.sh` de check them:
- Tab 2 table co du lieu
- endpoint Tab 2 hop le
- endpoint Tab 3 hop le
- summary table co row count dung

### Dau ra

- Sau khi copy artifact vao dung cho, chi can chay pipeline la stack san sang

## 11. Pha 9: Test va acceptance criteria

### Muc tieu

Dam bao he thong on dinh va co the ban giao / demo khong loi logic.

### Viec can lam

- Them test moi trong `project-realtime-bi/tests/`:
- `test_feature_artifact_schema.py`
- `test_tab2_artifact_loader.py`
- `test_tab2_api_contract.py`
- `test_tab3_simulation_contract.py`

- Acceptance criteria can chot:
- Tab 1 tra dung KPI theo month
- Tab 2 tra score that tu artifact
- Tab 3 doi slider thi metrics doi theo baseline Tab 2
- pipeline chay end-to-end khong can sua tay nhieu cho

### Dau ra

- co bo test can ban cho artifact, API, simulation
- co checklist acceptance de demo va ban giao

## 12. Pha 10: Chot docs va thong diep san pham

### Muc tieu

Tai lieu phai dong bo voi implementation that.

### Viec can lam

- Cap nhat cac file:
- `project-realtime-bi/docs/system_description/project_desc.md`
- `project-realtime-bi/docs/system_description/kkbox_feature_catalog.md`
- `project-realtime-bi/docs/system_description/kkbox_tab2_predictive_pipeline.md`
- `project-realtime-bi/docs/system_description/predictive.md`
- `project-realtime-bi/docs/system_description/prescriptive.md`
- `project-realtime-bi/docs/system_description/tab1_data_strategy.md`

- Sua `README.md` de:
- tro dung duong dan docs
- mo ta dung maturity cua Tab 2 / Tab 3
- mo ta dung run flow moi sau khi co artifact

### Dau ra

- docs, API, UI va pipeline noi cung mot cau chuyen

## 13. Thu tu uu tien de lam ngay

Neu muon dat duoc san pham hoan chinh nhanh nhat, nen lam theo thu tu:

1. Freeze artifact contract
2. Viet loader Tab 2 vao ClickHouse
3. Sua API Tab 2 sang model-backed
4. Noi Tab 3 vao baseline Tab 2
5. Don lai scope Tab 1
6. Sua UI frontend
7. Cap nhat script pipeline va validate
8. Them test
9. Chot docs

## 14. Definition of Done

San pham duoc xem la hoan chinh khi:

- feature store artifact duoc load vao he thong khong can can thiep tay
- Tab 2 dung output model that, khong con la duong heuristic mac dinh
- Tab 3 simulate tren baseline risk that tu Tab 2
- Tab 1 giu dung scope descriptive
- `bash scripts/run_pipeline.sh` + buoc import artifact co the dua stack len trang thai demo hoan chinh
- co bo validate va test toi thieu cho artifact, API va simulation

## 15. Ghi chu quan trong

- `apps/dashboard_streamlit/` chi nen xem la prototype tham khao, khong phai duong product chinh.
- Nguon su that cho feature semantics van la `infiniteWing/KKBOX churn/train_churn_pipeline.ipynb`.
- Neu chua co cost model that, khong nen overclaim ROI, net gain, hay decision engine.
- Neu chua dong bo semantic batch va realtime, phai noi ro do la 2 lop metric khac nhau.
