Tên dự án: KKBox Churn Intelligence Dashboard (K-CID)
Đối tượng đọc: C-Level (CEO, CFO), Giám đốc Marketing (CMO), Giám đốc Sản phẩm (CPO), Đội ngũ Data (DE, DS, DA).

TỔNG QUAN DỰ ÁN (EXECUTIVE SUMMARY)
1.1. Mục tiêu kinh doanh (Business Objectives)
KKBox đang đối mặt với bài toán tối ưu hóa vòng đời khách hàng (Customer Lifetime Value - LTV). Hệ thống K-CID được xây dựng nhằm:
Minh bạch hóa dữ liệu: Chuyển đổi hàng trăm triệu dòng log thô (18M+ rows) thành các chỉ số hành vi có ý nghĩa quản trị.
Dự báo chủ động: Nhận diện sớm tập khách hàng có nguy cơ Rời bỏ (Churn) trong 30 ngày tới.
Hỗ trợ quyết định (Prescriptive): Cung cấp công cụ giả lập (Simulation) để đo lường ROI của các chiến dịch Marketing/Product trước khi thực thi.
1.2. Chân dung người dùng (User Personas)
CMO / Trưởng phòng Marketing: Cần biết ai sắp rời đi để tung khuyến mãi (win-back campaign), đánh giá xem nên dồn ngân sách vào tệp người dùng nào (Ví dụ: Tập trung chốt Auto-renew).
CPO / Trưởng phòng Sản phẩm: Cần xem các chỉ số tương tác âm nhạc (skip_ratio, discovery_ratio) để tinh chỉnh thuật toán gợi ý (Recommendation Engine) tránh hiện tượng "nhàm chán nội dung".
CFO / Trưởng phòng Tài chính: Quan tâm đến dòng tiền (Revenue at Risk) và sự ổn định của LTV.


Ứng dụng BI sẽ có 1 Thanh Filter toàn cục (Global Slicer) và 3 Tabs chức năng chính.

Global Slicer (Bộ lọc áp dụng cho toàn hệ thống)
Time Context: Lọc theo tháng/năm của last_expire_date (Ví dụ: Chỉ xem tập khách hàng sẽ hết hạn gói cước vào Tháng 3/2017).
TAB 1: DESCRIPTIVE ANALYSIS (TỔNG QUAN HIỆN TRẠNG & HÀNH VI)
Mục đích: Trả lời câu hỏi "Chuyện gì đang xảy ra với vòng đời khách hàng?"
1. Khối High-Level KPIs (Thẻ chỉ số nằm ngang ở trên cùng):
Total Expiring Users: Tổng số user sẽ hết hạn trong kỳ lọc.
Historical Churn Rate: Tỷ lệ rời bỏ thực tế (%).
Overall Median Survival: Thời gian sống sót trung vị tổng thể (Ví dụ: ~365 ngày).
Auto-Renew Rate: Tỷ lệ bật Tự động gia hạn (%).
2. Khối Visualizations:
Biểu đồ 1: Dynamic Kaplan-Meier Survival Curve (Line Chart)
Mô tả: Biểu đồ đường cong sinh tồn.
Tương tác: Có một Dropdown list (Menu thả xuống) ngay trên góc biểu đồ để chọn Dimension (Chiều phân tích):[Độ tuổi | Giới tính/Hồ sơ | Tần suất giao dịch | Sự chán nản (Skip Ratio)]. Khi chọn chiều nào, biểu đồ tự động tách thành các đường tương ứng (như code đã vẽ).
Biểu đồ 2: Phân rổ Khách hàng (100% Stacked Bar Chart)
Mô tả: Trục Y là các phân khúc được Rời rạc hóa (Binning) từ code: price_segment, loyalty_segment, active_segment. Trục X là Tỷ lệ % (Màu Đỏ = Churn, Màu Xanh = Retain).
Tương tác (Cross-filtering): Khi User click vào thanh "Săn deal < 4.5đ", toàn bộ Tab 1 sẽ hiển thị dữ liệu riêng của nhóm săn deal này.
Biểu đồ 3: Ma trận Hành vi "Nhàm chán nội dung" (Scatter Plot)
Cơ sở: Thể hiện insight "Hiệu ứng giao cắt" của teammate.
Trục X: discovery_ratio (Tỷ lệ khám phá).
Trục Y: skip_ratio (Tỷ lệ chuyển bài).
Trực quan: Các bong bóng là các User/Nhóm User. Bong bóng màu đỏ sậm (Churn cao) sẽ tập trung ở góc "Khám phá thấp + Skip cao".

TAB 2: PREDICTIVE ANALYSIS (DỰ BÁO RỦI RO & THỜI GIAN SỐNG SÓT)
Requirement: CẦN CÓ CÁC MÔ HÌNH SAU (để add vào tab Data Pipeline luôn)
Mô hình Classification (XGBoost/LightGBM): Dự báo Xác suất Rời bỏ trong 30 ngày tới (Probability of Churn: 0% - 100%). Thay vì điểm rủi ro chung chung, đây là xác suất tuyệt đối.
Mô hình Regression/Lifetimes (BG/NBD + Gamma-Gamma): Dự báo Giá trị Vòng đời Khách hàng tương lai (Predicted Future CLTV). Tính toán xem trong 6-12 tháng tới, một user/segment sẽ mang về bao nhiêu doanh thu (NTD).
Mô hình Cox Proportional Hazards (Đã có): Dùng để tính toán sức ảnh hưởng của các biến (Feature Importance/Hazard Ratio).
Mục tiêu nghiệp vụ: Cung cấp bức tranh dự phóng về Doanh thu và Tỷ lệ giữ chân trong 30-90 ngày tới. Phân loại CSDL thành các nhóm chiến lược để C-Level quyết định phân bổ ngân sách Retention.
1. Khối Dự phóng Tài chính & KPI (Predictive KPIs)
Không hiển thị số liệu hiện tại, chỉ hiển thị số liệu DỰ BÁO của tương lai.
Forecasted 30-Day Churn Rate: Tỷ lệ rời bỏ dự kiến trong tháng tới (Ví dụ: 5.2%). Có mũi tên đỏ/xanh biểu thị xu hướng so với tháng hiện tại.
Predicted Revenue at Risk (NTD): Tổng dòng tiền dự kiến thất thoát.
Công thức: ∑ (Xác suất Churn của User × Phí gia hạn dự kiến).
Total Predicted Future CLTV: Tổng giá trị vòng đời tương lai của tập khách hàng có xác suất ở lại > 50%. (Định giá sức khỏe của CSDL hiện tại).
Top Flight-Risk Segment: Gọi tên phân khúc đang có nguy cơ rụng cao nhất kèm số tiền rủi ro (Ví dụ: Sinh viên / Manual Pay / Deal-hunter - Risk: 15.2M NTD).

2. Khối Visualizations (Biểu đồ phân tích Cấp quản trị)
Biểu đồ 1: Ma trận Giá trị vs. Rủi ro (Value vs. Risk Scatter Quadrant)
Mục đích: Công cụ cốt lõi để C-Level quyết định "Nên cứu ai, bỏ ai?".
Trục X: Predicted Future CLTV (Từ Thấp đến Cao).
Trục Y: Churn Probability % (Từ 0% đến 100%).
Trực quan hóa: Mỗi điểm (Bubble) trên biểu đồ đại diện cho một Phân khúc khách hàng (Segment) chứ không phải từng cá nhân (Bubble size = Số lượng User). Biểu đồ được chia làm 4 góc phần tư:
Góc trên - Phải (High Value, High Risk): Must Save (Phải giữ bằng mọi giá). Đây là nhóm VIP đang có dấu hiệu bỏ đi.
Góc trên - Trái (Low Value, High Risk): Let Go (Cho đi luôn). Nhóm săn deal, đóng tiền ít nhưng nguy cơ rụng cực cao. Đổ tiền marketing vào đây là lỗ.
Góc dưới - Phải (High Value, Low Risk): Loyal Core (Lõi trung thành). Nhóm tự động gia hạn, sinh lời ổn định.
Góc dưới - Trái (Low Value, Low Risk): Stable Low-Tier.
Biểu đồ 2: Phân bổ Thất thoát Doanh thu theo Yếu tố (Predicted Revenue Leakage Treemap)
Mục đích: Trả lời câu hỏi "Dòng tiền Revenue at Risk đang bị chảy ra từ những lỗ hổng nào của sản phẩm/chính sách?".
Trực quan hóa: Biểu đồ Treemap (Hình chữ nhật chia khối). Kích thước của mỗi khối vuông tỷ lệ thuận với số tiền Revenue at Risk.
Chiều phân tích (Dimension): Khối to nhất sẽ nhóm theo các biến trọng yếu nhất phát hiện từ mô hình Cox. Ví dụ:
Khối 1 (Chiếm 60% diện tích): Nhóm thanh toán Thủ công (Manual Renewal).
Khối 2 (Chiếm 25% diện tích): Nhóm có Tỷ lệ Skip bài > 50% (Chán nản thuật toán).
Khối 3 (Chiếm 15% diện tích): Nhóm độ tuổi 15-20.
Tương tác: Click vào một khối (Ví dụ khối "Skip > 50%") để xem chi tiết nhóm này cấu thành từ những ai.
Biểu đồ 3: Đường cong Rớt phễu Dự phóng (Forecasted Survival Decay Line Chart)
Mục đích: Thay vì nhìn Kaplan-Meier của quá khứ (Tab 1), C-Level nhìn thấy viễn cảnh tương lai. Nếu không làm gì cả, lượng user Active hiện tại sẽ rụng như thế nào trong 3-6-12 tháng tới?
Trực quan hóa:
Trục X: Timeline các tháng trong tương lai (T+1, T+2... T+12).
Trục Y: % Số lượng User dự kiến còn giữ lại được.
Vẽ 3 đường line đại diện cho 3 nhóm: Tệp trả giá chuẩn, Tệp săn deal, và Tệp Free Trial. Biểu đồ sẽ cho CEO thấy rõ: Tệp Free Trial rớt thẳng đứng về 0 sau tháng T+1, trong khi tệp giá chuẩn duy trì độ lài ổn định.
Biểu đồ phễu
3. Khối Actionable Insights (Tự động hóa báo cáo)
Thay vì để một bảng Data Table thô liệt kê từng user, BI tool sẽ tổng hợp thành một bảng Strategic Prescriptions (Khuyến nghị cấp phân khúc):
Cấu trúc Bảng:
Segment Name (Ví dụ: VIP - Manual Pay - Low Discovery).
User Count (Số lượng User).
Average Churn Prob (Xác suất rụng trung bình).
Revenue at Risk (Tiền rủi ro).
Primary Risk Driver (Nguyên nhân chính đẩy xác suất rụng lên cao - dựa trên SHAP values của XGBoost hoặc Hazard Ratio của Cox).
Chức năng Export: Dành cho CMO/Data Team tải cục Segment này về để map vào hệ thống CRM chạy chiến dịch.

TAB 3: PRESCRIPTIVE SIMULATION (MÔ PHỎNG & TỐI ƯU HÓA HÀNH ĐỘNG)
Mục tiêu nghiệp vụ: Cho phép C-Level điều chỉnh các tham số giả định (Giảm tỷ lệ User có thói quen xấu, tăng tỷ lệ thanh toán tự động) để đo lường mức độ tác động đến Doanh thu và Hệ số rủi ro tổng thể.
Làm rõ định nghĩa: "Rủi ro" ở đây được định lượng chính xác là Hazard Ratio (HR). Giả lập ở đây là bài toán tính toán lại HR trung bình của toàn hệ thống khi phân bổ lại cấu trúc tệp khách hàng.
1. Khối Control Panel (Tham số đầu vào - Input Parameters):
Người dùng tương tác bằng các thanh trượt (Sliders) để thiết lập kịch bản giả định (Scenario):
Tác động Thương mại (Commercial):
Slider 1: Chuyển đổi [ X ] % lượng User từ Pay_Manual sang Pay_Auto-Renew.
Slider 2: Chuyển đổi [ Y ] % lượng User đang dùng gói Trial/Deal (<4.5đ/ngày) sang gói Giá chuẩn (>=4.5đ).
Tác động Sản phẩm (Product Engagement):
Slider 3: Chuyển đổi [ Z ] % lượng User có hành vi Skip_High (>50%) (Chán nản) xuống mốc Skip_Low (<20%) (Hài lòng).
2. Khối Visualizations (Kết quả đầu ra - Output Metrics):
Biểu đồ 1: Population Hazard Shift (Overlaid Histogram / Mật độ phân bổ rủi ro)
Định nghĩa lại: Trục X là điểm Hazard Ratio (HR). Trục Y là % lượng User.
Trực quan hóa: Biểu đồ thể hiện 2 đường cong mật độ (Density curve). Đường màu xám là Hiện trạng (Baseline). Khi kéo thanh trượt (Ví dụ tăng Auto-renew), hệ thống tính toán lại HR mới cho tệp User được chuyển đổi. Đường màu xanh (Scenario) sẽ xuất hiện và dịch chuyển về phía bên trái (phía có HR thấp hơn).
Ý nghĩa nghiệp vụ: Cung cấp bằng chứng trực quan về việc kịch bản giả định làm giảm thiểu mức độ rủi ro chung của toàn bộ CSDL như thế nào.
Biểu đồ 2: Financial Impact Analysis (Waterfall Chart)
Bổ sung tính toán:
Cột 1: Current Baseline Revenue (Doanh thu của kỳ hiện tại).
Cột 2: Saved Revenue from Retention (Doanh thu giữ lại được nhờ việc dịch chuyển HR xuống mức an toàn, kéo dài số ngày sống sót dự kiến × giá trị gói cước ngày).
Cột 3: Incremental Revenue from Upsell (Doanh thu tăng thêm từ việc ép User mua gói Giá chuẩn - từ Slider 2).
Cột 4: Optimized Projected Revenue (Tổng doanh thu kỳ vọng sau kịch bản).
Biểu đồ 3: Sensitivity Analysis - ROI per Strategy (Tornado/Bar Chart)
Bổ sung phân tích: Trong các thanh trượt vừa kéo, hành động nào mang lại hiệu quả cao nhất?
Trực quan hóa: Trục Y liệt kê 3 chiến lược (Auto-Renew, Upsell Giá chuẩn, Giảm Skip Ratio). Trục X hiển thị chỉ số Revenue Impact per 1% Shift (Cứ 1% User được chuyển đổi thành công ở mỗi mục thì mang lại bao nhiêu tiền).
Ý nghĩa nghiệp vụ: Trả lời trực tiếp câu hỏi nguồn lực của Ban Giám đốc: Nên dồn ngân sách cho Marketing để chốt Auto-Renew hay dồn cho IT/Product để tinh chỉnh thuật toán gợi ý bài hát nhằm giảm Skip Ratio? (Dữ liệu mô hình Cox cho thấy Pay_Auto-Renew làm giảm rủi ro 37.3%, đây sẽ là chiến lược có độ nhạy cao nhất).

Extract: Trích xuất định kỳ từ 4 bảng members, transactions, user_logs, train (khóa chính msno).
Transform:
Data Cleaning: Giới hạn tuổi 15-65, chặn các log thời gian > 86,400 giây/ngày, xử lý lỗi logic ngày hết hạn < ngày thanh toán.
Feature Engineering & Aggregation: Tính toán quy tắc RFM (Recency, Frequency, Monetary). Rời rạc hóa (Binning) các biến liên tục thành các tệp khách hàng: Loyalty_segment, Price_segment, Skip_segment (Thấp/TB/Cao), Discovery_segment (Thói quen/Cân bằng/Khám phá).
Modeling Scoring: Chạy mô hình phân tích sinh tồn Cox Proportional Hazards để chấm điểm Risk Score cho từng User.
Load: Đẩy dữ liệu đã được tổng hợp (Aggregated Master Table) vào Data Warehouse (VD: BigQuery, Snowflake) để kết nối trực tiếp với ứng dụng.
