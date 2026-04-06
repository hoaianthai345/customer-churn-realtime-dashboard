# KKBOX Prescriptive Simulation

## 1. Vai tro cua tai lieu

Tai lieu nay mo ta nghiep vu cho Tab 3: prescriptive simulation.

Tai lieu nay phai duoc doc cung voi:

1. `docs/system_description/kkbox_feature_catalog.md`
2. `project-realtime-bi/notebooks/feature_prep.ipynb`
3. `docs/system_description/kkbox_tab2_predictive_pipeline.md`
4. `team_code/tab2/kkbox-train-predictive-tab.ipynb`
5. `team_code/tab3/kkbox-simulation-2.ipynb`

Thu tu uu tien:

- feature catalog va `feature_prep.ipynb` la nguon su that cho semantics feature;
- notebook Tab 2 la nguon tham chieu cho predictive artifacts;
- notebook Tab 3 la prototype nghiep vu de tham khao, khong mac dinh la contract canonical.

## 2. Bai toan kinh doanh cua Tab 3

Tab 3 tra loi cau hoi:

- Neu tac dong vao mot lever kinh doanh cu the, doanh thu va risk profile se thay doi nhu the nao?

Ba nhom lever chinh da co co so nghiep vu tu feature store:

1. `Manual -> Auto-Renew`
2. `Deal / Free Trial -> Standard / Premium`
3. `High Skip / Low Discovery -> Hanh vi nghe lanh manh hon`

Tab 3 khong phai la tab "du bao xem se churn hay khong".
Tab 3 la lop "neu lam X thi ket qua baseline co the thay doi ra sao".

## 3. Dau vao grounded

### 3.1. Dau vao canonical tu feature store

Nhung cot da co co so ro rang trong `feature_prep.ipynb` va feature catalog:

- `is_manual_renew`
- `renewal_segment`
- `price_segment`
- `expected_renewal_amount`
- `skip_ratio`
- `discovery_ratio`
- `high_skip_flag`
- `low_discovery_flag`
- `content_fatigue_flag`
- `deal_hunter_flag`
- `free_trial_flag`
- `loyalty_segment`
- `rfm_segment`
- `bi_segment_name`

### 3.2. Dau vao nen co neu Tab 2 da duoc train

Neu notebook Tab 2 da run va xuat artifact, Tab 3 co the doc them:

- `churn_probability`
- `risk_band`
- `risk_decile`
- `expected_revenue_at_risk_30d`
- `expected_retained_revenue_30d`

Day la huong nen uu tien cho mot ban Tab 3 dong bo hon:

- feature store cung cap state + segment + behavioral flag
- Tab 2 cung cap risk baseline
- Tab 3 ap kich ban kinh doanh len risk baseline do

## 4. Logic nghiep vu nen chot cho Tab 3

### 4.1. Baseline

Chon mot cohort muc tieu:

- theo `target_month`
- theo segment filter neu can

Baseline phai la trang thai chua tac dong:

- doanh thu ky vong hien tai
- churn risk hien tai
- revenue at risk hien tai

### 4.2. Levers

#### Lever 1. Auto-renew conversion

Canh nghiep vu:

- doi tuong: nhom `is_manual_renew = 1` hoac `renewal_segment = Pay_Manual`
- hanh dong: mo phong mot ty le user duoc chuyen sang auto-renew
- ky vong: giam friction thanh toan, giam risk churn

#### Lever 2. Upsell / anti-discount

Canh nghiep vu:

- doi tuong: `deal_hunter_flag = 1` hoac `free_trial_flag = 1`
- hanh dong: mo phong mot ty le user duoc day len `Standard` hoac `Premium`
- ky vong: tang ARPU, nhung co the gay `price shock`

Vi vay, khi viet bao cao phai noi ro:

- upsell co 2 tac dong nguoc chieu
- tang gia tri hop dong
- nhung co the lam tang risk neu thiet ke sai

#### Lever 3. Giam met moi noi dung

Canh nghiep vu:

- doi tuong: `high_skip_flag`, `low_discovery_flag`, `content_fatigue_flag`
- hanh dong: mo phong viec cai thien recommendation / playlist / reactivation
- ky vong: giam boredom, giam risk profile

### 4.3. Impact estimation

Sau khi tac dong lever, can tinh lai:

- risk profile
- revenue at risk
- retained revenue
- phan revenue duoc cuu van

Neu su dung notebook Tab 2 da train:

- impact estimation nen dua tren `churn_probability` moi hoac scenario probability moi

Neu chi co prototype:

- phai ghi ro day la `proxy simulation`

## 5. Nhung gi notebook Tab 3 hien tai that su dang lam

Notebook `team_code/tab3/kkbox-simulation-2.ipynb` hien tai la mot prototype co gia tri tham khao nghiep vu, nhung chua nen xem la contract canonical.

No dang:

- rebuild cohort tu raw `transactions + logs + members`
- tao `baseline_hr`
- mo phong 3 lever:
  - auto-renew
  - upsell
  - skip reduction
- tinh:
  - `baseline_rev`
  - `retention_rev`
  - `upsell_rev`
  - `projected_rev`
- ve:
  - hazard shift
  - financial waterfall
  - sensitivity ranking
  - Monte Carlo distribution

No khong giai quyet tron ven cac diem sau:

- khong doc truc tiep feature store canonical lam input chinh
- khong dung predictive artifact Tab 2 lam baseline risk canonical
- khong co cost model / CAC grounded
- khong co ROI rong dung nghia tai chinh

Vi vay, notebook nay nen duoc xem la:

- `prototype nghiep vu`
- `reference de viet cau chuyen simulation`

khong nen xem la:

- `prescriptive engine da chot`

## 6. Dau ra grounded co the bao cao

Neu viet bao cao theo pham vi an toan, Tab 3 nen dung cac output sau:

- baseline revenue
- scenario projected revenue
- saved revenue from retention
- incremental revenue from upsell
- scenario churn/risk change
- sensitivity ranking theo lever

Nhung output nay co the ton tai o 2 muc:

- muc prototype tu notebook Tab 3
- hoac muc dong bo hon neu sau nay dung predictive artifact Tab 2 lam baseline

## 7. Nhung gi chua nen overclaim

Nhung noi dung sau chua nen goi la canonical neu chi dua tren docs va notebooks hien tai:

- `campaign_cost`
- `gross_gain`
- `net_roi`
- `baseline_6m_value`
- `6-month customer value engine`
- `calibrated Cox hazard model`
- `causal uplift model`
- `optimal next best action per user`

Neu muon dua vao bao cao, phai danh dau ro:

- `gia dinh`
- `proxy`
- `future-state`

## 8. Bo KPI va chart nen dung cho bao cao

### 8.1. Nen uu tien

1. `Scenario Comparison`
   - baseline vs simulated revenue
   - baseline vs simulated risk
2. `Financial Waterfall`
   - baseline revenue
   - saved revenue
   - incremental upsell revenue
   - projected revenue
3. `Sensitivity Ranking`
   - lever nao tao gia tri lon nhat tren moi 1% effort
4. `Population Risk Shift`
   - phan bo risk / hazard truoc va sau scenario

### 8.2. Chi nen dua vao phu luc hoac gan nhan prototype

- Monte Carlo revenue distribution
- hazard histogram / hazard shift neu van dang dua tren coefficient prototype

## 9. Wording nen dung

Nen dung:

- `prescriptive simulation`
- `scenario analysis`
- `proxy impact estimation`
- `saved revenue`
- `incremental upsell revenue`
- `sensitivity by intervention`

Khong nen dung neu chua co mo hinh / cost layer tuong ung:

- `decision engine`
- `ROI engine`
- `causal optimizer`
- `production Cox simulator`

## 10. Tuyen bo cuoi cung

Tab 3 co huong nghiep vu dung:

- dung feature store de xac dinh lever tac dong;
- dung lop predictive de xac dinh baseline risk;
- dung scenario engine de so sanh baseline va future-state.

Nhung voi bo notebooks va docs hien tai, cach mo ta dung nhat la:

- Tab 3 la `prescriptive simulation layer`
- co co so nghiep vu tu feature store
- co prototype notebook de tham khao
- nhung nhieu chi so tai chinh nang hon nhu `ROI rong`, `6M value`, `causal effect` van chua duoc chot thanh contract canonical.
