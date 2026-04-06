# KKBOX Report Diagram Guide

## 1. Muc tieu

Tai lieu nay liet ke cac diagram nen dua vao bao cao de trinh bay du an KKBOX theo dung semantics hien tai:

- feature store batch la nguon su that cho offline analytics;
- Tab 1 la descriptive analysis tren expiring cohort;
- Tab 2 la predictive layer du bao churn probability va expected revenue at risk;
- Tab 3 la simulation / what-if layer, khong duoc overclaim la survival model chinh thuc neu chua co artifact that.

Tai lieu nay uu tien diagram phuc vu thuyet trinh va bao cao, khong phai danh sach chart cho dashboard.

## 2. Thu tu ke chuyen de xep slide

Thu tu de nghi:

1. Bai toan va pham vi cohort
2. Pipeline du lieu va logic label
3. Tab 1: Hien trang cohort da sap expire
4. Tab 2: Du bao rui ro churn va doanh thu rui ro
5. Tab 3: Gia lap tac dong chien dich
6. Ket luan va khuyen nghi hanh dong

## 3. Diagram bat buoc

### D1. End-to-End Architecture

- Muc tieu:
  - cho nguoi doc thay dong chay du lieu tu raw source den 3 tab san pham.
- Loai diagram:
  - architecture / pipeline block diagram.
- Noi dung:
  - `transactions.csv + transactions_v2.csv`
  - `user_logs.csv + user_logs_v2.csv`
  - `members_v3.csv`
  - `train_churn_pipeline.ipynb`
  - `feature_store/`
  - `Tab 1 Descriptive`
  - `Tab 2 Predictive`
  - `Tab 3 Simulation`
- Thong diep chinh:
  - toan bo 3 tab phai dung chung mot lop feature store canonical.
- Ghi chu:
  - khong ve Tab 2 va Tab 3 nhu da co model survival/Cox that neu artifact do chua ton tai.

### D2. Snapshot va Label Timeline

- Muc tieu:
  - giai thich tai sao snapshot duoc khoa truoc `target_month` va label nhin 30 ngay sau expiry.
- Loai diagram:
  - timeline.
- Noi dung:
  - `PREVIOUS_MONTH[target_month]` cho user logs
  - `snapshot_dt = ngay cuoi cung cua thang truoc`
  - anchor transaction cua `target_month`
  - `expire_date`
  - cua so 30 ngay sau expiry de gan `is_churn`
- Thong diep chinh:
  - tranh leakage va giu grain `1 msno / 1 target_month`.

### D3. Anchor Transaction Selection Logic

- Muc tieu:
  - giai thich user nao duoc dua vao expiring cohort va giao dich nao duoc chon lam neo.
- Loai diagram:
  - flowchart / decision flow.
- Noi dung:
  - lay history transaction truoc `target_month`
  - tim block giao dich cung ngay giao dich cuoi
  - pha tie theo tuple business rule
  - chi giu neu `expire_month == target_month`
  - dung giao dich do lam anchor
- Thong diep chinh:
  - report nay phan tich expiring cohort, khong phai toan bo active base.

### D4. Label Assignment Logic

- Muc tieu:
  - giai thich ro `is_churn = 0/1`.
- Loai diagram:
  - decision tree.
- Noi dung:
  - neu khong co renewal hop le -> churn
  - neu renewal dau tien co `gap < 30` -> non-churn
  - neu renewal dau tien co `gap >= 30` -> churn
  - renew som truoc `expire_date` van la non-churn
- Thong diep chinh:
  - label la retention trong cua so 30 ngay sau expiry.

## 4. Diagram cho Tab 1

### D5. Expiring Cohort Trend

- Muc tieu:
  - tom tat quy mo va xu huong cohort qua cac thang.
- Loai diagram:
  - line + bar combo.
- Truc / metric:
  - x: `target_month`
  - bar: `expiring_subscribers`
  - line: `forward_churn_rate`
- Dau vao:
  - `bi_feature_master.parquet`
- Thong diep chinh:
  - quy mo cohort va churn rate khong di chuyen cung chieu.

### D6. New vs Churned Users by Month

- Muc tieu:
  - the hien dong chay user moi vao va user rot ra trong expiring cohort.
- Loai diagram:
  - diverging bar chart.
- Truc / metric:
  - x: `target_month`
  - y am: `churned_users`
  - y duong: `new_users`
- Thong diep chinh:
  - nhin nhanh muc do “bu vao / mat di” qua tung thang.

### D7. Customer Composition

- Muc tieu:
  - phan ra cohort thanh `New / Returning / Retained / Churned`.
- Loai diagram:
  - horizontal bar chart.
- Dau vao:
  - `customer_composition` downstream cua Tab 1.
- Thong diep chinh:
  - cohort thang nay dang duoc dan dat boi nhom nao.

### D8. Customer Movement Sankey

- Muc tieu:
  - giai thich chuyen dich tu cycle truoc sang cycle hien tai.
- Loai diagram:
  - Sankey.
- Nguon -> dich:
  - `previous_customer_state` -> `customer_composition`
- Thong diep chinh:
  - thay duong di tu “Previously Renewed / Previously Churned / First Cycle” sang trang thai hien tai.

### D9. Revenue Structure of Expiring Cohort

- Muc tieu:
  - tach doanh thu cohort sap gia han thanh cac lop y nghia nghiep vu.
- Loai diagram:
  - waterfall hoac stage bar chart.
- Metric:
  - `revenue_up_for_renewal`
  - `safe_base_revenue`
  - `silent_expiring_revenue`
  - `revenue_at_risk_30d`
  - `realized_lost_revenue_30d`
- Thong diep chinh:
  - doanh thu “co the mat” va doanh thu “da mat” la 2 lop khac nhau.

### D10. Pareto Revenue Distribution

- Muc tieu:
  - cho thay doanh thu tap trung o nhom user nao.
- Loai diagram:
  - bar + cumulative line Pareto chart.
- Truc / metric:
  - x: `revenue_decile`
  - bar: tong `revenue_up_for_renewal`
  - line: `cumulative_revenue_share`
- Thong diep chinh:
  - mot so nho user co the dong gop phan lon doanh thu sap gia han.

### D11. Segment Risk View for Tab 1

- Muc tieu:
  - cho thay nhom nao trong descriptive view dang co dau hieu met moi / rui ro.
- Loai diagram:
  - 100% stacked bar hoac clustered bar.
- Segment nen ve:
  - `price_segment`
  - `loyalty_segment`
  - `active_segment`
  - `skip_segment`
- Metric:
  - user count
  - `realized_lost_revenue_30d` hoac `revenue_at_risk_30d`
- Thong diep chinh:
  - phan khuc nao dang xuat hien nhieu user / revenue rui ro.

### D12. Boredom Scatter

- Muc tieu:
  - the hien relation giua kham pha noi dung va bo bai.
- Loai diagram:
  - scatter plot.
- Truc:
  - x: `discovery_ratio`
  - y: `skip_ratio`
  - size: `expected_renewal_amount`
  - color: `is_churn` hoac metric risk downstream
- Thong diep chinh:
  - nhom `skip cao + discovery thap` la proxy cho fatigue.

## 5. Diagram cho Tab 2

### D13. Predictive Pipeline Diagram

- Muc tieu:
  - giai thich luong train model cua Tab 2.
- Loai diagram:
  - block diagram.
- Noi dung:
  - `train_features_bi_all.parquet`
  - train split `201701-201702`
  - validation `201703`
  - score `201704`
  - LightGBM
  - calibration
  - scored parquet + artifact
- Thong diep chinh:
  - Tab 2 la feature-store-first classification pipeline.

### D14. Validation Performance

- Muc tieu:
  - chung minh model co gia tri tot hon random ranking.
- Loai diagram:
  - metric table + bar chart.
- Metric:
  - `log_loss`
  - `roc_auc`
  - `pr_auc`
  - `prediction_mean`
- Ghi chu:
  - neu co raw va calibrated probability, hien ca 2 de giai thich calibration.

### D15. Probability Calibration

- Muc tieu:
  - cho thay xac suat du bao co “doc duoc” hay khong.
- Loai diagram:
  - reliability curve / calibration plot.
- Truc:
  - x: du bao probability bucket
  - y: churn rate quan sat
- Thong diep chinh:
  - probability da duoc hieu chuan de dung cho revenue-at-risk.

### D16. Risk Decile Chart

- Muc tieu:
  - cho thay top decile co tap trung churn va revenue rui ro hay khong.
- Loai diagram:
  - bar + line combo.
- Truc / metric:
  - x: `risk_decile`
  - bar 1: user count
  - bar 2: `expected_revenue_at_risk_30d`
  - line: average `churn_probability`
- Thong diep chinh:
  - model co kha nang rank dung nhom rui ro cao.

### D17. Top Flight-Risk Segment Heatmap

- Muc tieu:
  - tim segment nao vua dong user lon vua co doanh thu rui ro cao.
- Loai diagram:
  - heatmap.
- Chieu:
  - hang: `renewal_segment` hoac `price_segment`
  - cot: `loyalty_segment` hoac `active_segment`
- Gia tri:
  - average `churn_probability`
  - tong `expected_revenue_at_risk_30d`
- Thong diep chinh:
  - giup uu tien segment can can thiệp truoc.

### D18. Feature Group Importance

- Muc tieu:
  - giai thich model dang nhin vao nhom tin hieu nao.
- Loai diagram:
  - horizontal bar chart.
- Nhom:
  - `payment_value`
  - `churn_history`
  - `listening_behavior`
  - `loyalty_member`
  - `segment_flags`
- Ghi chu:
  - day la global importance, khong phai row-level explanation.

### D19. Top User Risk Leaderboard

- Muc tieu:
  - minh hoa output hanh dong cua Tab 2.
- Loai diagram:
  - table.
- Cot:
  - `msno`
  - `churn_probability`
  - `expected_renewal_amount`
  - `expected_revenue_at_risk_30d`
  - `risk_band`
  - `price_segment`
  - `renewal_segment`
- Thong diep chinh:
  - model output co the chuyen thanh danh sach uu tien giu chan.

## 6. Diagram cho Tab 3

### D20. Simulation Lever Map

- Muc tieu:
  - tom tat 3 don bay can thiep nghiep vu.
- Loai diagram:
  - strategy map / intervention tree.
- Don bay:
  - `Pay_Manual -> Pay_Auto-Renew`
  - `Deal Hunter / Free Trial -> Standard / Premium`
  - `High Skip / Low Discovery -> healthier engagement`
- Nguon feature:
  - `renewal_segment`
  - `price_segment`
  - `deal_hunter_flag`
  - `free_trial_flag`
  - `skip_ratio`
  - `discovery_ratio`
  - `content_fatigue_flag`

### D21. What-If Waterfall

- Muc tieu:
  - cho thay tac dong cua mot scenario len doanh thu ky vong.
- Loai diagram:
  - waterfall.
- Metric:
  - baseline revenue
  - delta revenue
  - simulated revenue
- Ghi chu:
  - neu Tab 3 hien van la proxy, can ghi ro tren slide: “simulation proxy, chua phai Cox artifact production”.

### D22. Scenario Comparison Matrix

- Muc tieu:
  - so sanh nhieu campaign option tren cung mot mat bang.
- Loai diagram:
  - bubble chart hoac matrix table.
- Chieu:
  - x: muc dau tu / conversion assumption
  - y: incremental revenue
  - size: affected users
  - color: campaign type
- Thong diep chinh:
  - chien dich nao cho hieu qua / quy mo tot nhat.

### D23. Monte Carlo Distribution

- Muc tieu:
  - cho thay uncertainty cua doanh thu sau simulation.
- Loai diagram:
  - histogram.
- Metric:
  - phan phoi doanh thu simulated
  - `P10`
  - `P50`
  - `P90`
- Thong diep chinh:
  - khong chi co mot con so ky vong, ma co mot day kich ban rui ro.

## 7. Diagram phu luc de nghi

### D24. Data Quality Guardrail Table

- Muc tieu:
  - chung minh pipeline co guard cho quality issue.
- Dang trinh bay:
  - table.
- Noi dung:
  - `days_to_expire` phai nam trong `[0, 31]`
  - `membership_age_days >= 0`
  - khong duplicate `msno-target_month`
  - `sample_submission_v2.csv` khop test cohort
- Thong diep chinh:
  - so lieu dau vao da duoc khoa va kiem tra.

### D25. Canonical vs Proxy Boundary

- Muc tieu:
  - tranh hoi dong hieu nham feature / model nao da ton tai that.
- Loai diagram:
  - 3-box comparison.
- 3 cot:
  - da co trong feature store canonical
  - da co trong downstream notebook
  - chua co artifact production
- Noi dung nen dua:
  - `is_churn`
  - `price_segment`
  - `churn_probability`
  - `expected_revenue_at_risk_30d`
  - `hazard_ratio`
  - `predicted_future_cltv`

## 8. Bo toi thieu neu bao cao ngan

Neu bao cao chi co 8-10 slide, uu tien ve:

1. `D1` End-to-End Architecture
2. `D2` Snapshot va Label Timeline
3. `D5` Expiring Cohort Trend
4. `D7` Customer Composition
5. `D9` Revenue Structure of Expiring Cohort
6. `D13` Predictive Pipeline Diagram
7. `D16` Risk Decile Chart
8. `D17` Top Flight-Risk Segment Heatmap
9. `D20` Simulation Lever Map
10. `D21` What-If Waterfall

## 9. Ghi chu ve cach dat ten trong bao cao

- Dung ten `expiring cohort` nhat quan trong toan bo report.
- Dung `expected_revenue_at_risk_30d` cho Tab 2, khong doi ten thanh `CLTV`.
- Dung `simulation proxy` neu Tab 3 chua noi vao Cox model artifact that.
- Dung `feature store canonical` khi noi ve source-of-truth offline.
