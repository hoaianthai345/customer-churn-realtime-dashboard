# KKBOX Feature Catalog

  

## 1. Phạm vi

  

Tài liệu này mô tả bộ feature được tạo trong notebook https://www.kaggle.com/code/hoianthi/features-prep.

  

Notebook tập trung vào:

  

- Tạo snapshot churn theo tháng

- Aggregate `user_logs` của tháng trước

- Merge thông tin `members`

- Tạo feature số cho modeling

- Tạo feature semantic cho dashboard BI theo mô tả trong [project_desc. Md](/Users/anhoaithai/Documents/AHT/2. AREAS/UEH/Kì 6/Hệ hỗ trợ quản trị thông minh/Project/project-realtime-bi/docs/project_desc. Md)

  

## 2. Đầu ra của notebook

  

- `artifacts/feature_store/train_features_all.parquet`: tập train numeric, giữ định dạng để model đọc trực tiếp

- `artifacts/feature_store/test_features_201704_full.parquet`: tập score numeric cho tháng `201704`

- `artifacts/feature_store/bi_feature_master.parquet`: bảng master cho BI, gồm snapshot `201701` đến `201704`

- `artifacts/feature_store/train_features_bi_all.parquet`: tập train có thêm các cột semantic cho BI

- `artifacts/feature_store/test_features_bi_201704_full.parquet`: tập score có thêm các cột semantic cho BI

- `artifacts/feature_store/feature_columns.csv`: danh sách cột numeric cho modeling

- `artifacts/feature_store/bi_dimension_columns.csv`: danh sách cột dimension semantic cho dashboard

  

## 3. Quy tắc làm sạch dữ liệu

  

- `bd` chỉ được giữ nếu nằm trong khoảng `15-65`; ngoài khoảng này được đánh dấu là unknown

- Ghi chú nghiệp vụ: cần thống nhất thêm với cả nhóm. Đề xuất hiện tại là `15-65` vì đây là nhóm khách hàng mục tiêu cần quan tâm; các trường hợp nhỏ hơn, lớn hơn hoặc không xác định được xem như thiếu thông tin khách hàng

- `total_secs` trong `user_logs` không vượt quá `86,400` giây mỗi ngày (`24 * 60 * 60`)

- `membership_expire_date < transaction_date` được chỉnh về bằng `transaction_date` và gắn cờ `invalid_expire_before_txn`

- Giải thích: đây là một case sai logic nghiệp vụ trong cùng một giao dịch, ví dụ `transaction_date = 2017-03-05` nhưng `membership_expire_date = 2017-02-28`; điều này khác với trường hợp user hết hạn rồi quay lại gia hạn ở một giao dịch khác

- Các phép chia có mẫu số bằng `0` được thay bằng giá trị mặc định `0` hoặc `-1` tùy nhóm feature

  

## 4. Nhóm feature được tạo

  

Để tránh hiểu nhầm khi mỗi người có thể có cách hiểu nghiệp vụ khác nhau, phần này thống nhất cách hiểu các thuật ngữ được dùng bên dưới.

  

**`snapshot`** 
là một bản ghi “ảnh chụp trạng thái của 1 user tại 1 thời điểm quan sát”, không phải toàn bộ lịch sử của user. Cụ thể, thời điểm quan sát đó là `target_month`.

  

- Với mỗi user và mỗi `target_month`, pipeline chọn 1 giao dịch mốc

- Giao dịch mốc là giao dịch hợp lệ cuối cùng có `expire_month = target_month`, `is_cancel = 0`, và `transaction_month < target_month`

- Từ giao dịch mốc này, pipeline tạo ra 1 dòng feature

Ví dụ:
  

- Snapshot `201703` của user A dùng giao dịch hết hạn trong `03/2017`

- Log nghe nhạc dùng từ `02/2017`

- Label churn được gán bằng cách nhìn xem sau ngày hết hạn đó, trong `30` ngày tiếp theo user có gia hạn hợp lệ hay không


**`Aggregate`** 
 Trong pipeline này, aggregate user_logs của tháng trước nghĩa là:

  - Dữ liệu gốc user_logs có rất nhiều dòng cho cùng một user trong
    Nhiều ngày
  - Ta gom lại theo msno và theo tháng
  - Rồi tính các thống kê như sum, mean, count

  Ví dụ:
  User A trong tháng 201702 có 10 dòng log mỗi ngày khác nhau. Ví dụ như total_secs_sum (hoặc là mean)

  Sau khi aggregate, thay vì 10 dòng rời rạc, ta còn 1 dòng cho user A với mục đích:

  - Giảm dữ liệu từ mức log chi tiết xuống mức user-month
  - Tạo feature để model và BI dùng được
  - đồng bộ grain với snapshot 1 user x 1 target_month
  

### 4.1. Snapshot và label churn theo tháng

  

- `target_month`: tháng snapshot đang phân tích

- `is_churn`: nhãn churn trong cửa sổ `30` ngày sau mốc hết hạn

- `expire_date`: ngày hết hạn của giao dịch được chọn làm mốc

- `transaction_date`: ngày thanh toán của giao dịch mốc

- `last_expire_month`: tháng hết hạn, phục vụ global slicer

- `is_expiring_user`: gán cố định bằng `1`, dùng để đếm số user hết hạn trong snapshot đó

- Giải thích: thay vì chỉ dùng `COUNT(*)`, cột cờ này giúp measure trong BI dễ tái sử dụng hơn; nếu sau này cần loại một số row khỏi mẫu phân tích, chỉ cần đổi cờ về `0`

  

### 4.2. Feature thanh toán và giao dịch hiện tại

  

- `payment_method_id`: phương thức thanh toán ở snapshot

- `payment_plan_days`: số ngày của gói hiện tại

- `plan_list_price`: giá niêm yết

- `actual_amount_paid`: số tiền thực trả

- `is_auto_renew`: có bật auto-renew hay không

- `invalid_expire_before_txn`: có dữ liệu lỗi ngày hết hạn hay không; cột này chỉ để kiểm tra chất lượng dữ liệu

- `discount = plan_list_price - actual_amount_paid`

- `is_discount`: bằng `1` nếu có giảm giá

- `amt_per_day = actual_amount_paid / payment_plan_days`

- `expected_renewal_amount`: giá trị doanh thu kỳ vọng của lần gia hạn kế tiếp; lấy `actual_amount_paid` nếu có, ngược lại lấy `plan_list_price`

- `price_gap`: khoảng cách giữa giá niêm yết và số tiền thực trả

- `discount_ratio = (plan_list_price - actual_amount_paid) / plan_list_price`

- `payment_to_list_ratio = actual_amount_paid / plan_list_price`

- `days_to_expire`: số ngày từ `transaction_date` đến `expire_date`

- `remaining_plan_ratio = days_to_expire / payment_plan_days`

- `price_gap_per_plan_day = price_gap / payment_plan_days`

  

### 4.3. Feature lịch sử giao dịch và churn

  

- `last_1_is_churn` đến `last_5_is_churn`: lịch sử churn của 5 chu kỳ gần nhất

- `churn_rate`: tỷ lệ churn lịch sử của user

- `churn_count`: tổng số lần churn trong lịch sử được quan sát

- `transaction_count`: số chu kỳ lịch sử đã có nhãn

- `historical_transaction_rows`: tổng số dòng transaction đã có đến mốc hiện tại

- `historical_paid_total`: tổng tiền đã thanh toán lịch sử

- `historical_paid_mean`: chi tiêu trung bình mỗi giao dịch

- `historical_list_price_mean`: giá niêm yết trung bình lịch sử

- `historical_cancel_count`: tổng số lần cancel

- `historical_cancel_rate`: tỷ lệ cancel trên tổng giao dịch

- `historical_auto_renew_rate`: tỷ lệ auto-renew trong lịch sử

- `days_since_previous_transaction`: khoảng cách ngày so với giao dịch trước

- `recent_churn_events`: tổng số sự kiện churn gần đây

- `weighted_recent_churn`: churn lịch sử có trọng số ưu tiên cho các chu kỳ mới

- `churn_rate_x_transaction_count`: tương tác giữa tần suất churn và độ dài lịch sử

  

### 4.4. Aggregate `user_logs` của tháng trước

  

Nhóm này được tính trên tháng trước `target_month`.

  

- `num_25_mean`, `num_50_mean`, `num_75_mean`, `num_985_mean`, `num_100_mean`, `num_unq_mean`, `total_secs_mean`: trung bình theo dòng log

- `num_25_sum`, `num_50_sum`, `num_75_sum`, `num_985_sum`, `num_100_sum`, `num_unq_sum`, `total_secs_sum`: tổng hành vi nghe

- `count`: tổng số dòng log của user trong tháng trước

- `last_log_date`: ngày nghe nhạc gần nhất của user trong tháng trước

- `capped_log_count`: số dòng log bị cap `total_secs`

  

### 4.5. Feature hành vi nghe nhạc suy diễn

  

- `secs_per_log = total_secs_sum / count`

- `unique_per_log = num_unq_sum / count`

- `num100_per_log = num_100_sum / count`

- `listen_events_sum = num_25_sum + num_50_sum + num_75_sum + num_985_sum + num_100_sum`

- `skip_events_sum = num_25_sum + num_50_sum + num_75_sum`

- `listen_events_per_log = listen_events_sum / count`

- `weighted_completion_sum = 0.25*num_25_sum + 0.50*num_50_sum + 0.75*num_75_sum + 0.985*num_985_sum + 1.0*num_100_sum`

- `weighted_completion_per_log = weighted_completion_sum / count`

- `completion_ratio = weighted_completion_sum / listen_events_sum`

- `skip_ratio = skip_events_sum / listen_events_sum`

- `discovery_ratio = num_unq_sum / listen_events_sum`

- `replay_ratio = 1 - discovery_ratio`

- `avg_secs_per_unique = total_secs_sum / num_unq_sum`

- `secs_per_plan_day = total_secs_sum / payment_plan_days`

- `uniques_per_plan_day = num_unq_sum / payment_plan_days`

- `logs_per_plan_day = count / payment_plan_days`

- `secs_per_paid_amount = total_secs_sum / actual_amount_paid`

- `days_since_last_listen = transaction_date - last_log_date`

- `capped_log_share = capped_log_count / count`

  

### 4.6. Feature member và nhân khẩu học

  

- `city`: mã thành phố

- `bd`: tuổi đã được làm sạch

- `age`: biến tuổi sau cleaning, unknown = `-1`

- `has_valid_age`: có tuổi hợp lệ hay không

- `gender`: giới tính mã hóa, `0 = unknown`, `1 = male`, `2 = female`

- `gender_profile`: nhãn semantic của giới tính

- `registered_via`: kênh đăng ký

- `registration_init_time`: ngày đăng ký gốc

- `registration_year`, `registration_month`, `registration_day`: các cột thời gian tách từ ngày đăng ký

- `membership_age_days = transaction_date - registration_init_time`

- `tenure_months = membership_age_days / 30`

  

### 4.7. Feature RFM

  

- `rfm_recency_score`: điểm recency dựa trên `days_since_last_listen`

- `rfm_frequency_score`: điểm frequency dựa trên `count`

- `rfm_monetary_score`: điểm monetary dựa trên `expected_renewal_amount`

- `rfm_total_score`: tổng ba thành phần RFM

- `rfm_segment_code`: mã nhóm RFM

- `rfm_segment`: nhãn nhóm RFM

  

Quy tắc scoring hiện tại:

  

- Recency: `3` nếu nghe trong `<= 7` ngày, `2` nếu `<= 21`, `1` nếu có nghe nhưng xa hơn, `0` nếu không có log

- Frequency: `3` nếu `count > 15`, `2` nếu `6-15`, `1` nếu `1-5`, `0` nếu bằng `0`

- Monetary: `3` nếu `expected_renewal_amount >= 150`, `2` nếu `100-149`, `1` nếu `1-99`, `0` nếu bằng `0`

  

### 4.8. Segment semantic cho BI

  

- `age_segment_code`, `age_segment`

- `price_segment_code`, `price_segment`

- `loyalty_segment_code`, `loyalty_segment`

- `active_segment_code`, `active_segment`

- `skip_segment_code`, `skip_segment`

- `discovery_segment_code`, `discovery_segment`

- `renewal_segment_code`, `renewal_segment`

- `rfm_segment_code`, `rfm_segment`

- `bi_segment_name`: segment tổng hợp theo công thức `loyalty | renewal | price | discovery`

  

Quy tắc chia nhóm:

  

- `price_segment`

- `Free Trial / Zero Pay`: `amt_per_day <= 0`

- `Deal Hunter < 4.5`: `0 < amt_per_day < 4.5`

- `Standard 4.5-6.5`: `4.5 <= amt_per_day < 6.5`

- `Premium > 6.5`: `amt_per_day >= 6.5`

  

- `loyalty_segment`

- `New < 30d`

- `Growing 30-179d`

- `Established 180-364d`

- `Loyal >= 365d`

  

- `active_segment`

- `Inactive`: `count = 0`

- `Light 1-5 logs`

- `Active 6-15 logs`

- `Heavy > 15 logs`

  

- `skip_segment`

- `Low < 20%`

- `Medium 20-50%`

- `High > 50%`

- `No Listening Data`

  

- `discovery_segment`

- `Habit < 20%`

- `Balanced 20-50%`

- `Explore > 50%`

- `No Listening Data`

  

- `renewal_segment`

- `Pay_Auto-Renew`

- `Pay_Manual`

  

### 4.9. Flag điều khiển cho simulation và data quality

  

- `is_manual_renew`: biến đổi `is_auto_renew` thành cờ thuận cho BI

- `high_skip_flag`: user có `skip_ratio >= 0.5`

- `low_discovery_flag`: user có `discovery_ratio < 0.2`

- `deal_hunter_flag`: user có `0 < amt_per_day < 4.5`

- `free_trial_flag`: user có `expected_renewal_amount <= 0`

- `content_fatigue_flag`: user vừa có `skip_ratio` cao vừa có `discovery_ratio` thấp

- `auto_renew_discount_interaction`: tương tác giữa auto-renew và sử dụng discount

  

## 5. Mapping vào dashboard BI

  

### 5.1. Tab 1: Descriptive Analysis

  

- `Total Expiring Users`: đếm `is_expiring_user` theo `last_expire_month`

- `Historical Churn Rate`: trung bình `is_churn`

- `Overall Median Survival`: median của `membership_age_days`

- `Auto-Renew Rate`: trung bình `is_auto_renew`

- `Kaplan-Meier dimensions`: sử dụng `age_segment`, `gender_profile`, `active_segment`, `skip_segment`

- `100% Stacked Bar`: sử dụng `price_segment`, `loyalty_segment`, `active_segment` + `is_churn`

- `Behavior Scatter`: sử dụng `discovery_ratio`, `skip_ratio`, `expected_renewal_amount`, `is_churn`

  

### 5.2. Tab 2: Predictive Analysis

  

Notebook này chỉ tạo feature đầu vào cho mô hình, chưa tạo đầu ra dự báo. Các feature quan trọng để đưa vào mô hình classification, Cox, và CLTV gồm:

  

- `skip_ratio`, `discovery_ratio`, `completion_ratio`

- `expected_renewal_amount`, `discount_ratio`, `amt_per_day`

- `historical_auto_renew_rate`, `historical_cancel_rate`, `weighted_recent_churn`

- `membership_age_days`, `days_since_last_listen`, `rfm_total_score`

- `price_segment_code`, `loyalty_segment_code`, `active_segment_code`

  

Sau khi có mô hình, BI có thể tính:

  

- `Predicted Revenue at Risk = churn_probability * expected_renewal_amount`

- `Top Flight-Risk Segment`: group theo `bi_segment_name`

- `Value vs Risk`: dùng `Predicted Future CLTV` và `churn_probability`

  

### 5.3. Tab 3: Prescriptive Simulation

  

Ba cần điều khiển trong mô tả dashboard đã có sẵn feature đầu vào:

  

- Chuyển user từ manual sang auto-renew: `renewal_segment`, `is_manual_renew`

- Upsell từ deal/trial lên giá chuẩn: `price_segment`, `deal_hunter_flag`, `free_trial_flag`

- Giảm skip ratio: `skip_ratio`, `high_skip_flag`, `content_fatigue_flag`

  

Nghĩa là tab simulation có thể bắt đầu từ feature store này, sau đó nối thêm hệ số tác động từ model Cox hoặc rule engine.
