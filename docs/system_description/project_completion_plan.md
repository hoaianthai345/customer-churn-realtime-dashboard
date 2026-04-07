# Kế Hoạch Hoàn Thiện Dự Án

## 1. Mục tiêu của tài liệu

Tài liệu này chốt kế hoạch triển khai các bước tiếp theo để đưa dự án từ trạng thái:

- đã có semantics dữ liệu tương đối rõ;
- đã có notebook batch và notebook Kaggle tách riêng;
- đã có narrative cho 3 tab;

đến trạng thái:

- chạy được ổn định từ dữ liệu gốc đến artifact;
- có đầu ra nhất quán giữa notebook, tài liệu và dashboard;
- đủ chắc để demo, viết báo cáo và tiếp tục productize.

Tài liệu này không override semantics feature. Nếu có mâu thuẫn:

- ưu tiên `kkbox_feature_catalog.md` cho định nghĩa feature và label;
- ưu tiên `kkbox_tab2_predictive_pipeline.md` cho contract Tab 2;
- ưu tiên file này cho thứ tự triển khai và tiêu chí hoàn thành.

## 2. Nguyên tắc thực hiện

1. Chỉ dùng một nguồn sự thật cho semantics batch: `kkbox_feature_catalog.md`.
2. Không để notebook, báo cáo và UI kể ba câu chuyện khác nhau.
3. Ưu tiên hoàn thiện đường dữ liệu canonical trước khi tối ưu UI.
4. Tab 2 và Tab 3 phải dựa trên feature store đã chốt, không tự build lại semantics từ raw.
5. Mọi output chưa có model thật phải được gắn nhãn rõ là `proxy` hoặc `prototype`.

## 3. Trạng thái hiện tại cần xuất phát

### 3.1. Đã có

- Feature catalog và snapshot semantics đã được chốt lại.
- Rule label churn đã sửa theo hướng `gap âm => is_churn = 0`.
- `feature_prep.ipynb` đã được vá các lỗi ép kiểu và validation gãy.
- Đã có cache aggregate `user_logs` theo tháng để tránh chạy lại quá lâu.
- Ba notebook trong `team_code/tab1`, `team_code/tab2`, `team_code/tab3` đã được tách thành bản tự chứa để chạy trên Kaggle.
- Đã có feature store canonical trong `project-realtime-bi/data/artifacts/feature_store/`.
- Đã có smoke test end-to-end cho 3 tab trên bộ artifact local hiện tại trong `project-realtime-bi/data/artifacts/_smoke_test/`.
- Tài liệu hệ thống đã được gom lại thành các nhóm `system_description`, `architecture_diagrams`, `report_and_slides`.

### 3.2. Chưa hoàn tất

- Chưa có một lần chạy full ổn định trên Kaggle kèm log và artifact chuẩn hóa để dùng làm baseline chính thức.
- Tab 3 vẫn còn mang tính prototype nghiệp vụ, chưa được refactor hoàn toàn theo pipeline canonical.
- Một số threshold và dimension giữa batch và lớp phục vụ realtime vẫn chưa đồng bộ tuyệt đối.
- Chưa có checklist phát hành nội bộ cho demo, báo cáo và handoff.

## 4. Mục tiêu hoàn thiện theo workstream

## 4.1. Workstream A: Chốt đường dữ liệu canonical

### Mục tiêu

Tạo được một bộ artifact batch ổn định, lặp lại được, làm đầu vào duy nhất cho Tab 1, Tab 2 và Tab 3.

### Việc cần làm

1. Chạy lại `feature_prep.ipynb` hoặc `feature_prep_from_cache.ipynb` trên Kaggle với cấu hình ổn định.
2. Lưu đầy đủ artifact batch chuẩn:
   - `train_features_all.parquet`
   - `test_features_201704_full.parquet`
   - `train_features_bi_all.parquet`
   - `test_features_bi_201704_full.parquet`
   - `bi_feature_master.parquet`
   - `feature_columns.csv`
   - `bi_dimension_columns.csv`
   - `submission_alignment_201704.csv`
3. Chụp lại thống kê cohort:
   - số dòng theo từng `target_month`
   - số `msno` unique
   - tỷ lệ churn theo tháng
   - số lượng dòng bị cleaning flag
4. Chốt thư mục lưu artifact dùng chung cho toàn dự án.

### Tiêu chí hoàn thành

- Có một thư mục artifact chuẩn có thể dùng lại.
- Có log hoặc file summary chứng minh run thành công.
- Không còn cell fail do `NaN`, `IntCastingNaNError` hay `sample_submission` mismatch.

## 4.2. Workstream B: Ổn định notebook Kaggle cho 3 tab

### Mục tiêu

Ba notebook của `team_code` chạy độc lập, rõ đầu vào và rõ đầu ra.

### Việc cần làm

1. Chạy smoke test cho:
   - `tab1/kkbox-descriptive-tab.ipynb`
   - `tab2/kkbox-train-predictive-tab.ipynb`
   - `tab3/kkbox-simulation-2.ipynb`
2. Với mỗi notebook, ghi rõ:
   - input cần mount trên Kaggle
   - output artifact sẽ sinh ra ở đâu
   - thứ tự chạy phụ thuộc lẫn nhau
3. Loại bỏ mọi đoạn code hoặc note còn ám chỉ dùng helper `.py` ngoài notebook.
4. Tạo một cell `sanity check` đầu notebook để fail sớm nếu thiếu artifact bắt buộc.

### Tiêu chí hoàn thành

- Mỗi notebook có thể chạy độc lập trên Kaggle.
- Có phần mô tả “input / output / run order” ngay trong notebook hoặc README.
- Không còn phụ thuộc vào `team_code/notebook_lib`.
- Có ít nhất một lần smoke test pass với output summary lưu lại trong artifact.

## 4.3. Workstream C: Hoàn thiện Tab 1 theo dữ liệu canonical

### Mục tiêu

Đảm bảo Tab 1 là lớp descriptive đáng tin cậy nhất và làm chuẩn kể chuyện cho toàn hệ thống.

### Việc cần làm

1. Xác nhận tất cả metric Tab 1 đều lấy đúng từ feature store canonical hoặc derived metric đã được định nghĩa rõ.
2. Chốt lại dimension còn mở:
   - `txn_freq_bucket`
   - cách mapping giữa batch segment và dimension trình bày
3. Kiểm tra lại các chart chính:
   - KPI tổng quan
   - Kaplan-Meier
   - stacked composition
   - boredom scatter
4. Ghi rõ metric nào là upstream canonical, metric nào là downstream business metric.

### Tiêu chí hoàn thành

- Tab 1 không còn conflict semantics lớn với feature catalog.
- Có bảng mapping `metric -> source field -> transformation`.

## 4.4. Workstream D: Hoàn thiện Tab 2 predictive

### Mục tiêu

Biến Tab 2 từ mức “có pipeline mô tả” thành mức “có artifact predictive nhất quán, dùng được để demo và làm đầu vào cho Tab 3”.

### Việc cần làm

1. Chạy notebook Tab 2 trên bộ feature store chuẩn.
2. Xuất đủ artifact:
   - validation metrics
   - scored validation
   - scored test
   - feature importance
   - feature group importance
3. Kiểm tra lại:
   - calibration có thực sự cải thiện hay không
   - `risk_band` có ngưỡng thống nhất hay không
   - `expected_revenue_at_risk_30d` có khớp logic tài liệu hay không
4. Chốt các trường được phép dùng trong báo cáo:
   - `churn_probability`
   - `risk_decile`
   - `risk_band`
   - `expected_revenue_at_risk_30d`
   - `expected_retained_revenue_30d`
5. Gắn nhãn rõ phần nào là model output thật, phần nào chưa có như `future_cltv`, `hazard_ratio`, `SHAP per-user`.

### Tiêu chí hoàn thành

- Có artifact score cho `201704`.
- Có metric validation đủ để bảo vệ mô hình ở mức báo cáo.
- Tài liệu `predictive.md` khớp với artifact thật đang có.

## 4.5. Workstream E: Refactor Tab 3 prescriptive

### Mục tiêu

Đưa Tab 3 về đúng kiến trúc mong muốn: simulation dựa trên feature store canonical và baseline risk từ Tab 2, thay vì rebuild logic rời rạc từ raw.

### Việc cần làm

1. Thiết kế lại input canonical cho Tab 3:
   - feature store canonical
   - artifact score từ Tab 2
2. Refactor notebook Tab 3 để:
   - không build lại cohort từ raw nếu không cần thiết
   - dùng đúng segment và flag từ feature store
   - dùng `churn_probability` hoặc output predictive làm baseline
3. Chia rõ 3 mức output:
   - grounded now
   - proxy simulation
   - future-state
4. Chốt các lever canonical:
   - `manual -> auto-renew`
   - `deal/free trial -> standard/premium`
   - `high skip / low discovery -> healthier engagement`
5. Nếu chưa có cost model thật, không tính `net ROI`; chỉ dừng ở:
   - saved revenue
   - incremental upsell revenue
   - projected revenue
   - risk shift

### Tiêu chí hoàn thành

- Tab 3 không còn lệch semantics với feature catalog.
- Có một notebook prescriptive kể đúng cùng câu chuyện với Tab 2.
- Báo cáo không còn phải dựa vào các giả định mơ hồ về Cox hay ROI nếu chưa có model thật.

## 4.6. Workstream F: Đồng bộ tài liệu và report narrative

### Mục tiêu

Đảm bảo tài liệu kỹ thuật, tài liệu nghiệp vụ và slide đều kể cùng một câu chuyện.

### Việc cần làm

1. Rà soát toàn bộ `docs/system_description/`.
2. Chuẩn hóa tiếng Việt có dấu cho các tài liệu còn lại nếu cần.
3. Đồng bộ 4 nhóm file:
   - `kkbox_feature_catalog.md`
   - `project_desc.md`
   - `predictive.md`
   - `prescriptive.md`
4. Bổ sung một file outline demo hoặc release note nội bộ.
5. Chốt bộ diagram và pipeline diagram dùng cho báo cáo cuối.

### Tiêu chí hoàn thành

- Không còn file nào mô tả quá mức maturity hiện có.
- Agent sau chỉ cần đọc `README.md` và vài file canonical là có thể tiếp tục làm việc.

## 4.7. Workstream G: Kiểm thử, reproducibility và bàn giao

### Mục tiêu

Làm cho dự án dễ chạy lại, dễ kiểm tra và dễ bàn giao cho người khác.

### Việc cần làm

1. Tạo checklist run:
   - chuẩn bị dữ liệu
   - chạy feature prep
   - chạy Tab 1
   - chạy Tab 2
   - chạy Tab 3
2. Tạo smoke test tối thiểu cho:
   - schema artifact
   - số cột bắt buộc
   - một vài validation business quan trọng
3. Ghi rõ chiến lược cache:
   - cache aggregate log
   - thư mục artifact nặng
   - quy ước không commit dữ liệu nặng vào git
4. Chốt `.gitignore`, cấu trúc thư mục output và naming artifact.
5. Viết tài liệu handoff ngắn cho contributor hoặc agent tiếp theo.

### Tiêu chí hoàn thành

- Có thể rerun theo checklist mà không mò lại logic cũ.
- Người khác có thể dùng đúng artifact mà không phải đoán nguồn nào là canonical.

## 5. Thứ tự ưu tiên thực hiện

## P0: Phải làm trước

1. Chạy ổn định feature prep và chốt artifact batch chuẩn.
2. Chạy được notebook Tab 2 trên artifact chuẩn.
3. Gắn lại Tab 3 vào đúng input canonical.

## P1: Nên làm ngay sau đó

1. Chốt semantics và metric của Tab 1.
2. Đồng bộ toàn bộ tài liệu nghiệp vụ.
3. Tạo checklist run và smoke test cơ bản.

## P2: Làm để hoàn thiện chất lượng bàn giao

1. Tối ưu thời gian chạy và cache.
2. Chuẩn hóa toàn bộ tiếng Việt có dấu trong docs còn lại.
3. Viết release note nội bộ, slide outline và demo flow cuối.

## 6. Definition of Done cho dự án

Dự án được xem là đủ hoàn thiện cho mục tiêu học thuật + demo sản phẩm khi đạt đồng thời các điều kiện sau:

1. Có một bộ feature store canonical chạy được và lưu artifact đầy đủ.
2. Tab 1 có descriptive artifact nhất quán với feature catalog.
3. Tab 2 có model output thật ở mức churn prediction cho cohort `201704`.
4. Tab 3 dùng đúng feature store và baseline risk từ Tab 2 để mô phỏng scenario.
5. Tài liệu không còn overclaim so với notebook và artifact thực tế.
6. Có bộ diagram, pipeline description và script trình bày đủ để viết báo cáo và demo.
7. Có checklist chạy lại dự án mà không cần truy ngược lịch sử hội thoại.

## 7. Đề xuất bước làm ngay tiếp theo

Nếu chỉ chọn một chuỗi công việc ngắn nhất để đi tiếp, nên làm theo thứ tự:

1. Chạy `feature_prep_from_cache.ipynb` để chốt artifact batch.
2. Chạy notebook Tab 2 và chốt scored artifact `201704`.
3. Refactor notebook Tab 3 để đọc feature store + output Tab 2.
4. Kiểm tra lại narrative của Tab 1, Tab 2, Tab 3 bằng cùng một bộ artifact.
5. Chốt slide, diagram và demo script từ bộ output cuối cùng.
