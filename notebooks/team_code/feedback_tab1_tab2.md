# Feedback Tab 1, Tab 2 va tai lieu lien quan

## Muc tieu

File nay tong hop feedback de chinh sua mo ta nghiep vu va pham vi implementation cua Tab 1 va Tab 2 sao cho khop voi:

- scope canonical cua du an
- feature store batch hien tai
- serving layer va API dang ton tai

## 1. Van de hien tai

- Scope giua Tab 1, Tab 2, Tab 3 dang bi lan vao nhau.
- Tai lieu `Prescriptive.md` hien dung de mo ta Tab 3 simulation, nhung neu lay no lam co so de noi cho Tab 1 hoac Tab 2 thi se sai pham vi.
- Tab 1 notebook hien khong con la descriptive thuần. No da chen them nhieu logic risk va du bao nhu `risk_score`, `projected_risk_flag`, `revenue_at_risk_30d`, `customer_segment`.
- Tab 2 hien chua co bang chung la mot mo hinh predictive da train va serve chinh thuc. Phan hien tai trong repo la scoring proxy theo cong thuc heuristic.
- Semantics giua feature store batch va serving layer Tab 1 realtime chua dong bo hoan toan, dac biet o cac bucket segmentation va dimension `transaction frequency`.

## 2. Diem manh

- Huong tiep can `feature-store first` la dung. Notebook pipeline batch da tach ro lop feature cho modeling va lop feature semantic cho BI.
- Nhom da tan dung dung nhieu cot co that trong pipeline nhu `expected_renewal_amount`, `skip_ratio`, `discovery_ratio`, `price_segment`, `loyalty_segment`, `active_segment`, `renewal_segment`, `rfm_segment`.
- Tab 1 notebook co kha nang dien giai business tot, nhat la o phan cohort, renewal revenue, customer composition va lifecycle.
- `Prescriptive.md` viet kha ro ve luong simulation, baseline, re-predict va ROI. Neu giu dung vai tro Tab 3 thi day la mot tai lieu co gia tri.

## 3. Diem sai hoac chua khop

### 3.1. Sai pham vi giua cac tab

- Theo scope canonical cua du an:
- Tab 1 la descriptive analysis.
- Tab 2 la predictive analysis.
- Tab 3 la prescriptive simulation.

- Vi vay, `Prescriptive.md` khong nen duoc dung de dai dien cho Tab 1 hoac Tab 2. Tai lieu nay nen duoc giu dung vai tro Tab 3.

### 3.2. Tab 1 dang vuot scope descriptive

- Scope canonical cua Tab 1 chi nen gom:
- KPI descriptive
- Kaplan-Meier theo dimension
- 100% stacked bar theo segment
- boredom scatter theo `discovery_ratio` va `skip_ratio`

- Trong notebook Tab 1 hien co them cac lop logic:
- `risk_score`
- `projected_risk_flag`
- `safe_base_revenue`
- `revenue_at_risk_30d`
- `at_risk_flag`
- `customer_segment`

- Cac bien nay khong con la descriptive thuần nua. Day la lop business rule du bao risk, hop ly hon neu dat o Tab 2, hoac neu van de o Tab 1 thi phai gan nhan ro la overlay heuristic, khong phai phan descriptive canonical.

### 3.3. Tab 2 chua phai predictive model that

- Notebook `train_churn_pipeline.ipynb` hien dung o tang xuat feature store, khong train LightGBM, khong xuat churn probability, khong xuat CLTV, khong xuat hazard ratio.
- Feature store batch hien tai moi cung cap input layer cho model, chu chua cung cap output predictive chinh thuc.
- API Tab 2 trong repo dang tinh:
- `churn_probability`
- `predicted_future_cltv`
- `hazard_ratio_proxy`

- Nhung cac gia tri nay dang duoc tinh bang cong thuc heuristic, khong phai ket qua tu model serving chuan.
- Mot diem can luu y la scoring hien tai con dung ca `churned` lam mot tin hieu dau vao. Ve nghia nghiep vu, dieu nay cang cho thay day la lop proxy analytics, khong nen mo ta nhu model predictive da duoc huan luyen bai ban.

### 3.4. Chua dong bo semantic giua batch va realtime Tab 1

- Batch feature store dung cac semantic va threshold rieng cho:
- `age_segment`
- `price_segment`
- `loyalty_segment`
- `active_segment`

- Serving layer Tab 1 realtime lai dang bucket hoa theo bo threshold khac.
- Hien tai `transaction frequency` trong Tab 1 realtime co `txn_freq_bucket`, nhung notebook batch khong tao san cot canonical nay trong `BI_DIMENSION_COLUMNS`.

- He qua la:
- cung ten metric nhung khac nghia du lieu
- kho so sanh giua batch view va realtime view
- de gay nham khi viet tai lieu nghiep vu

### 3.5. Ten nguon tai lieu chua dong bo

- Tab 1 notebook dang nhac den `features-prep.ipynb`.
- Nguon canonical hien tai cua feature store la `infiniteWing/KKBOX churn/train_churn_pipeline.ipynb`.

- Can doi ten tai lieu tham chieu cho dung de tranh lam cac thanh vien sau hieu nham pipeline nao moi la nguon su that.

## 4. Goi y sua

### 4.1. Chot lai ranh gioi cua tung tab

- Tab 1:
- Giữ dung vai tro descriptive analysis.
- Chi mo ta nhung gi dang xay ra voi cohort va lifecycle.
- Khong goi cac chi so risk/revenue at risk la phan descriptive canonical.

- Tab 2:
- Mo ta la predictive proxy analytics o giai doan hien tai.
- Noi ro la dang dung feature input tu feature store + cong thuc heuristic trong API.
- Khong duoc goi `churn_probability`, `predicted_future_cltv`, `hazard_ratio` la output tu model that neu chua co pipeline train + serve tuong ung.

- Tab 3:
- Giu `Prescriptive.md` cho simulation.
- Goi ro day la simulation huong san pham, chua phai decision engine da hieu chuan.

### 4.2. Don lai Tab 1 notebook

- Tach cac phan sau ra khoi Tab 1, hoac chuyen sang Tab 2:
- `risk_score`
- `projected_risk_flag`
- `revenue_at_risk_30d`
- `safe_base_revenue`
- `customer_segment`

- Neu van giu cac phan nay trong notebook Tab 1 de phuc vu trinh bay, can ghi ro:
- day la lop business-rule overlay
- khong phai metric predictive tu model train chinh thuc
- khong nam trong scope descriptive canonical cua dashboard product

### 4.3. Viet lai mo ta Tab 2 cho dung hien trang

- Nen viet theo cau truc:
- Input: feature store batch + serving table hien co
- Logic hien tai: heuristic scoring trong API
- Output hien tai: predictive proxy metrics
- Backlog: classification model that, CLTV model that, Cox model that, SHAP / risk drivers that

- Cach viet nay se dung hon ve mat nghiep vu va tranh overclaim.

### 4.4. Dong bo semantic giua batch va realtime

- Can chot 1 bo threshold canonical cho:
- age
- price
- loyalty
- activity

- Neu chua the dong bo ngay, tai lieu phai noi ro:
- batch feature store va realtime serving dang la hai lop semantics khac nhau
- khong duoc so sanh truc tiep nhu cung mot dinh nghia metric

### 4.5. Chot dinh nghia `transaction frequency`

- Hien tai Tab 1 muon dung dimension transaction frequency cho Kaplan-Meier, nhung batch notebook chua co cot canonical.
- Nhom can quyet dinh mot trong cac huong:
- tao bucket tu `transaction_count`
- tao bucket tu `historical_transaction_rows`
- hoac tiep tuc de no chi ton tai trong realtime serving, nhung phai ghi ro day la dimension rieng cua serving layer

### 4.6. Dong bo ten nguon su that

- Doi moi cho dang ghi `features-prep.ipynb` thanh `train_churn_pipeline.ipynb`.
- Neu can, bo sung them tham chieu den:
- `project-realtime-bi/docs/project_desc.md`
- `project-realtime-bi/docs/kkbox_feature_catalog.md`
- `project-realtime-bi/docs/tab1_data_strategy.md`

## 5. Ket luan

- Tab 1 chi dung khi giu no o pham vi descriptive.
- Tab 2 hien chi dung neu duoc mo ta la predictive proxy, khong phai model that.
- `Prescriptive.md` nen duoc xem la tai lieu cho Tab 3, khong nen dung de mo ta Tab 1 hoac Tab 2.
- De tranh lech ve sau, can chot lai boundary giua ba tab va dong bo lai semantics giua batch feature store va realtime serving.
