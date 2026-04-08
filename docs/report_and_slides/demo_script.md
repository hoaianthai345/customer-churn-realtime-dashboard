# Demo Script

## 1. Mục tiêu của buổi demo đầu tiên

Mục tiêu không phải là khoe toàn bộ pipeline realtime.
Mục tiêu là chứng minh sản phẩm đã có một dashboard churn intelligence hoàn chỉnh, ổn định, và kể được câu chuyện quản trị xuyên suốt qua 3 tầng:

1. Descriptive: hiện trạng cohort sắp hết hạn
2. Predictive: doanh thu nào đang ở vùng rủi ro cao nhất
3. Prescriptive: nếu can thiệp thì giá trị kỳ vọng thay đổi ra sao

Buổi demo đầu tiên nên dùng `artifact-backed demo mode` để tránh rủi ro runtime và giữ câu chuyện nhất quán.

## 2. Operator Checklist

### 2.1. Khởi động demo

```bash
bash scripts/run_demo.sh
```

Nếu stack đã chạy sẵn:

```bash
bash scripts/validate_demo.sh
```

### 2.2. Điều kiện sẵn sàng trước khi trình bày

- `http://localhost:3000` mở được dashboard
- `http://localhost:8000/health` trả `{"status":"ok"}`
- month options chỉ ra `2017-04`
- Tab 1, Tab 2, Tab 3 đều load được
- header hiển thị `Artifact` hoặc trạng thái tương đương của demo mode

### 2.3. Những gì cần nhớ trước khi nói

- demo month cố định là `2017-04`
- đây là demo ổn định theo artifact, không phải live replay
- backend contract và logic nghiệp vụ vẫn giữ nguyên
- chọn demo mode để ưu tiên độ ổn định và tính nhất quán của câu chuyện

## 3. Kịch bản trình bày 5-7 phút

## 3.1. Mở đầu

Nói ngắn:

> Đây là dashboard churn intelligence cho bài toán thuê bao âm nhạc. Nhóm chia bài toán thành 3 lớp: descriptive để nhìn hiện trạng, predictive để ưu tiên rủi ro, và prescriptive để so sánh kịch bản can thiệp.

Thao tác:

- mở `http://localhost:3000`
- chỉ vào hero section và executive KPI strip
- nhấn mạnh đây là dashboard đã sẵn sàng cho góc nhìn quản trị, không phải chỉ là notebook phân tích

## 3.2. Tab 1 - Descriptive

Nói:

> Ở tab này, mục tiêu là trả lời câu hỏi cohort sắp hết hạn đang ở trạng thái nào. Chúng tôi nhìn vào churn rate lịch sử, survival pattern, segment mix, và tín hiệu hành vi như boredom hoặc discovery.

Thao tác:

- giữ month là `2017-04`
- vào `Descriptive`
- chỉ lần lượt:
  - KPI cards
  - Kaplan-Meier / survival chart
  - segment mix
  - boredom-discovery scatter

Điểm chốt:

> Tab 1 giúp xác định vấn đề đang nằm ở đâu trước khi đi sang dự báo và ra quyết định.

## 3.3. Tab 2 - Predictive

Nói:

> Sau khi thấy hiện trạng, tab predictive trả lời câu hỏi doanh thu nào đang ở vùng rủi ro cao nhất. Ở đây nhóm dùng scored artifact từ pipeline predictive để xếp hạng rủi ro và lượng hoá revenue at risk.

Thao tác:

- chuyển sang `Predictive`
- chỉ vào:
  - forecasted churn rate
  - predicted revenue at risk
  - value-risk matrix
  - top risk segment
  - forecast decay

Điểm chốt:

> Tab này biến churn từ một tỷ lệ chung thành một bài toán ưu tiên hoá theo giá trị kinh doanh.

## 3.4. Tab 3 - Prescriptive

Nói:

> Sau khi biết nhóm nào rủi ro và quan trọng, tab prescriptive cho phép thử các kịch bản can thiệp như tăng auto-renew, upsell, hoặc cải thiện engagement, rồi so sánh baseline với optimized outcome.

Thao tác:

- chuyển sang `Prescriptive`
- điều chỉnh nhẹ 1 đến 2 scenario controls
- chỉ vào:
  - baseline vs optimized KPI
  - financial waterfall
  - hazard/risk distribution
  - sensitivity analysis
  - Monte Carlo / confidence summary

Điểm chốt:

> Tab này trả lời câu hỏi “nếu làm thì đáng hay không”, thay vì dừng ở mức “ai có rủi ro”.

## 3.5. Kết thúc

Nói:

> Giá trị chính của sản phẩm là kết nối 3 tầng phân tích trong cùng một dashboard: thấy hiện trạng, lượng hoá rủi ro kinh doanh, và thử kịch bản hành động. Với buổi demo đầu tiên, nhóm cố ý đóng gói theo artifact-backed mode để câu chuyện ổn định và sẵn sàng trình bày.

## 4. Fallback plan nếu có sự cố

Nếu dashboard mở chậm:

- refresh trình duyệt 1 lần
- chạy lại `bash scripts/validate_demo.sh`

Nếu API không lên:

- chạy lại `bash scripts/run_demo.sh`

Nếu bị hỏi vì sao không demo replay:

> Với buổi trình bày đầu tiên, nhóm ưu tiên artifact-backed mode để giữ nội dung ổn định. Realtime pipeline vẫn là một phần của hệ thống, nhưng hôm nay mục tiêu là trình bày sản phẩm hoàn chỉnh và logic quản trị xuyên suốt.

## 5. Câu trả lời ngắn cho các câu hỏi dễ gặp

Nếu bị hỏi “đây có phải dữ liệu thật không?”:

> Đây là artifact sinh từ pipeline phân tích canonical của nhóm trên dữ liệu KKBOX, sau đó được backend phục vụ lại theo contract sản phẩm.

Nếu bị hỏi “vì sao chỉ có tháng 2017-04?”:

> Đây là snapshot demo ổn định nhất để trình bày end-to-end. Mục tiêu hiện tại là chuẩn hoá trải nghiệm demo trước, sau đó mới mở rộng thêm phạm vi tháng và tính năng.

Nếu bị hỏi “logic nghiệp vụ có đổi khi đổi giao diện không?”:

> Không. Giao diện đã thay mới hoàn toàn nhưng contract API và logic nghiệp vụ backend được giữ nguyên.
