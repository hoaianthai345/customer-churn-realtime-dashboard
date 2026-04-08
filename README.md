 ---

  🚀 KKBox Real-time Customer Churn BI Dashboard

  https://customer-churn-realtime-dashboard.vercel.app

  Hệ thống Near Real-time Business Intelligence (BI) chuyên sâu cho bài toán dự báo và phân tích tỷ lệ
  rời bỏ khách hàng (Churn) của dịch vụ âm nhạc KKBox. Dự án kết hợp sức mạnh của Big Data Streaming
  (Spark, Kafka), OLAP Database (ClickHouse) và Modern Web (FastAPI, React).

  🌟 Tính năng chính

  Hệ thống được thiết kế xoay quanh 3 trụ cột phân tích (Tab):

  📊 Tab 1: Descriptive Analysis (Phân tích mô tả)
   * KPIs Real-time: Theo dõi tổng số user sắp hết hạn, tỷ lệ churn lịch sử, tỷ lệ tự động gia hạn.
   * Phân tích Survival: Biểu đồ Kaplan-Meier theo các phân khúc (Age, City, Payment Method).
   * Boredom Analysis: Scatter plot phân tích hành vi "chán nản" dựa trên discovery_ratio và
     skip_ratio từ log stream.

  🔮 Tab 2: Predictive Analysis (Phân tích dự báo)
   * Risk Scoring: Đánh giá xác suất rời bỏ của từng khách hàng.
   * Revenue at Risk: Ước tính doanh thu có nguy cơ bị mất.
   * Future CLTV: Dự đoán giá trị vòng đời khách hàng trong tương lai.

  🧪 Tab 3: Prescriptive Simulation (Giả lập chiến lược)
   * Scenario Analysis: Giả lập thay đổi các yếu tố (ví dụ: chuyển từ thanh toán thủ công sang tự
     động) để xem tác động tới tỷ lệ Churn và doanh thu.
   * Impact Estimation: Ước tính hiệu quả của các chiến dịch retention trước khi triển khai thực tế.

  ---

  🏗️ Kiến trúc hệ thống (Architecture)

  Hệ thống sử dụng mô hình kiến trúc hiện đại cho phép xử lý dữ liệu quy mô lớn với độ trễ thấp:

   1 graph LR
   2     A[Raw CSV Data] --> B[Batch Preload]
   3     C[User Logs Replay] --> D[Kafka]
   4     D --> E[Spark Streaming]
   5     E --> F[(ClickHouse)]
   6     B --> F
   7     F --> G[FastAPI]
   8     G --> H[Vite + React Dashboard]

  Tech Stack chi tiết:
   * Data Ingestion: Kafka (Event Streaming), Python Producers (Replay log data).
   * Processing: Apache Spark Structured Streaming (Transformations & KPI computation).
   * Storage (OLAP): ClickHouse - cho phép truy vấn aggregation cực nhanh trên hàng triệu bản ghi.
   * Backend: FastAPI (Python) - Cung cấp RESTful API và WebSocket cho dữ liệu real-time.
   * Frontend: React (TypeScript), Vite, Tailwind CSS, Shadcn/UI, Recharts.
   * Infrastructure: Docker & Docker Compose, AWS (Hosting Backend & Database).

  ---

  📂 Cấu trúc thư mục

   1 ├── apps/
   2 │   ├── api_fastapi/        # Backend API (Python)
   3 │   ├── batch/              # Batch processing & Materialization scripts
   4 │   ├── producers/          # Kafka producers (Replay events)
   5 │   └── streaming/          # Spark Structured Streaming jobs
   6 ├── frontend/               # React + Vite Application
   7 ├── infra/                  # Cấu hình Docker cho Kafka, Spark, ClickHouse
   8 ├── docs/                   # Tài liệu chi tiết về kiến trúc và dữ liệu
   9 └── notebooks/              # Feature Engineering & EDA

  ---

  🛠️ Hướng dẫn cài đặt (Local Development)

  1. Yêu cầu hệ thống
   * Docker & Docker Compose
   * Python 3.9+
   * Node.js & npm/bun

  2. Khởi chạy Infrastructure
   1 # Khởi động Kafka, ClickHouse, Spark
   2 docker-compose up -d

  3. Cài đặt Backend & Run Pipeline

   1 # Cài đặt thư viện
   2 pip install -r requirements.txt
   3
   4 # Khởi tạo dữ liệu và chạy pipeline
   5 bash scripts/run_pipeline.sh

  4. Khởi chạy Frontend
   1 cd frontend
   2 npm install
   3 npm run dev

  ---

  🚀 Triển khai (Deployment)

   * Frontend: Được deploy tự động trên Vercel tại:
     https://customer-churn-realtime-dashboard.vercel.app/
     (https://customer-churn-realtime-dashboard.vercel.app/)
   * Backend: Chạy trên hạ tầng AWS, đảm bảo khả năng mở rộng và độ ổn định cao cho các task xử lý dữ
     liệu nặng.

  ---

  📝 License & Contact
   * Dataset: KKBox Churn Prediction (Kaggle).
   * Author: An Hoai Thai & UEH Smart Decision Support System Team.

  ---
  Dự án này là một phần của nghiên cứu về Hệ hỗ trợ quản trị thông minh (UEH).
