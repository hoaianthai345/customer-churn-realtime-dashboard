Tài liệu này giải thích chi tiết cấu trúc, logic vận hành và ý nghĩa kinh doanh của **Hệ thống Mô phỏng Chiến lược Churn (Prescriptive Simulation Engine V2.0)** dựa trên bộ Feature Store đã xây dựng.

---

### 1. Tổng quan Luồng Dữ liệu (Data Flow)
Hệ thống vận hành theo quy trình:
**Feature Store** $\rightarrow$ **Baseline Model (ML Pass 1)** $\rightarrow$ **User Interventions (Sliders)** $\rightarrow$ **Simulated Model (ML Pass 2)** $\rightarrow$ **Financial Impact.**

---

### 2. Chi tiết các Hàm và Logic Xử lý

#### A. Hàm `load_and_prep_baseline()`
*   **Input (Dữ liệu đầu vào):** File `bi_feature_master.parquet` từ Feature Store.
*   **Cách tính:** 
    1. Lọc lấy snapshot của tháng gần nhất (`target_month` max).
    2. Gọi `MOCK_ML_PREDICT_PROBA` lần 1 để lấy xác suất Churn hiện tại (`baseline_churn_prob`).
    3. Tính **Hệ số tích lũy 6 tháng** (`baseline_6m_multiplier`): Dùng công thức chuỗi hình học hữu hạn để ước tính một khách hàng sẽ ở lại bao nhiêu tháng trong nửa năm tới dựa trên xác suất churn.
*   **Output (Đầu ra):** DataFrame `df_base` chứa trạng thái "Yên phận" của toàn bộ danh sách khách hàng.
*   **Ý nghĩa:** Thiết lập "điểm mốc" (Baseline) để so sánh. Nếu không làm gì cả, đây là số tiền tối đa ta sẽ thu được.

#### B. Hàm `MOCK_ML_PREDICT_PROBA(df)`
*   **Input:** DataFrame chứa các Feature (Segment, Amount, Logs...).
*   **Cách tính:** 
    *   **[FALLBACK]:** Hiện tại dùng logic cộng dồn trọng số (Heuristic). 
    *   **[REAL MODEL]:** Cần thay bằng lệnh `model.predict_proba()` từ XGBoost/LightGBM thật của nhóm.
*   **Output:** Mảng xác suất Churn (0.0 đến 1.0) cho từng user.
*   **Ý nghĩa:** Đây là "Bộ não" của hệ thống. Nó phản ứng với mọi thay đổi của dữ liệu. Ví dụ: Nếu ta tăng giá gói, hàm này phải trả về xác suất Churn cao hơn.

#### C. Hàm `simulate_scenarios(inputs)`
Đây là trung tâm điều khiển của Dashboard.
*   **Input (Tham số từ người dùng - Sliders):**
    *   `shift_auto_pct`: % khách hàng Manual muốn chuyển sang Auto-renew.
    *   `auto_renew_cost_per_user` (CAC): Chi phí Marketing bỏ ra cho mỗi người (Voucher, Ads).
    *   `shift_upsell_pct`: % khách hàng mua Deal muốn chuyển lên gói Standard 149 NTD.
    *   `upsell_cost_per_user`: Chi phí thuyết phục nâng cấp.
*   **Cách tính (Logic can thiệp):**
    1. **Tác động:** Chọn ngẫu nhiên một lượng khách hàng mục tiêu và thay đổi giá trị Feature của họ (Ví dụ: Đổi cột `renewal_segment` từ 'Manual' sang 'Auto').
    2. **Tái dự báo (Re-predict):** Đẩy dữ liệu đã bị thay đổi này vào `MOCK_ML_PREDICT_PROBA` lần 2. **Đây là bước quan trọng nhất** để thấy được tác động của hành động lên hành vi khách hàng.
    3. **Tính ROI:** 
        *   `Gross Gain` = (Tổng doanh thu 6 tháng sau khi can thiệp) - (Tổng doanh thu 6 tháng Baseline).
        *   `Net ROI` = Gross Gain - (Tổng chi phí Marketing).
*   **Output:** Một Dictionary chứa các con số tài chính tổng hợp (Net ROI, Campaign Cost...).

---

### 3. Hệ thống Chỉ số (KPIs)

| Chỉ số | Cách tính | Ý nghĩa Kinh doanh |
| :--- | :--- | :--- |
| **Baseline 6M Value** | $\sum (Amount \times Multiplier_{base})$ | Doanh thu kỳ vọng trong 6 tháng nếu không thực hiện chiến dịch nào. |
| **Gross Gain** | $Value_{simulated} - Value_{baseline}$ | Tổng giá trị thặng dư tạo ra nhờ giữ chân khách hàng thành công và tăng giá bán. |
| **Campaign Cost** | $\sum (Users_{impacted} \times CAC)$ | Tổng ngân sách Marketing cần phê duyệt để thực hiện kịch bản này. |
| **Net ROI** | $Gross Gain - Cost$ | Lợi nhuận ròng thực tế. Chỉ số này > 0 thì chiến dịch mới khả thi. |
| **Churn Shock** | $Prob_{new} - Prob_{old}$ | (Ngầm định trong model) Sự gia tăng rủi ro khi ép khách hàng tăng giá. |

---

### 4. Biểu đồ và Ý nghĩa (Visualization Guide)

#### Biểu đồ: Financial Waterfall Chart (Phân rã ROI)
*   **Cột 1 (Baseline):** Điểm xuất phát tài chính của công ty.
*   **Cột 2 (Growth/Retention):** Phần doanh thu tăng thêm. Nếu cột này xanh và cao, chứng tỏ chiến thuật can thiệp vào Feature (như Auto-renew) rất hiệu quả về mặt giữ chân.
*   **Cột 3 (Cost - Màu đỏ):** Khoản hụt đi do chi phí Marketing. 
*   **Cột 4 (Total):** Kết quả cuối cùng. 
*   **Ý nghĩa:** Giúp C-Level nhìn thấy ngay lập tức: **"Chúng ta tiêu X tiền Marketing để đổi lấy Y tiền doanh thu giữ lại, cuối cùng lãi Z đồng"**.

---

### 5. Hướng dẫn Thay thế Model thật (Integration Note)

Trong code, hãy tìm các block comment **[FALLBACK]**. Cậu cần thực hiện các thay đổi sau để Dashboard chạy trên kết quả thật của nhóm:

1.  **Trong hàm `MOCK_ML_PREDICT_PROBA`:**
    *   Load model đã train: `clf = joblib.load('my_xgboost_model.pkl')`.
    *   Xác định đúng danh sách feature model cần: `cols = pd.read_csv('feature_columns.csv')['feature'].tolist()`.
    *   Trả về: `return clf.predict_proba(df[cols])[:, 1]`.

2.  **Trong Dashboard UI (Streamlit/Dash):**
    *   Gắn các tham số `shift_...` vào các thanh trượt (Sliders).
    *   Gắn các tham số `cost_...` vào các ô nhập số (Number Input).
    *   Mỗi khi người dùng buông tay khỏi thanh trượt, gọi lại hàm `simulate_scenarios` để cập nhật Waterfall Chart ngay lập tức.

Code gợi ý
```python
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ==============================================================================
# BƯỚC 1: HÀM MÔ PHỎNG MÔ HÌNH MACHINE LEARNING (ML MODEL FALLBACK)
# ==============================================================================
# /// CHÚ Ý QUAN TRỌNG: KHI CÓ MÔ HÌNH THẬT, HÃY XÓA HÀM NÀY VÀ THAY BẰNG:
# /// probabilities = real_model.predict_proba(df[feature_cols])[:, 1]
# ==============================================================================
def MOCK_ML_PREDICT_PROBA(df):
    """
    Hàm này giả lập một mô hình ML. Nó nhận vào DataFrame các features 
    và trả về mảng xác suất Churn [0.0 -> 1.0] dựa trên trạng thái của feature.
    """
    base_risk = 0.15 
    
    # ML Model học được các patterns (Giả lập trọng số)
    manual_penalty = np.where(df['renewal_segment'] == 'Pay_Manual', 0.25, 0)
    skip_penalty = np.where(df['skip_segment'] == 'High > 50%', 0.10, 0)
    deal_penalty = np.where(df['price_segment'] == 'Deal Hunter < 4.5', 0.20, 0)
    inactive_penalty = np.where(df['active_segment'] == 'Inactive', 0.30, 0)
    loyalty_bonus = np.where(df['loyalty_segment'] == 'Loyal >= 365d', -0.15, 0)
    rfm_bonus = np.where(df['rfm_segment'] == 'High Value', -0.10, 0)
    
    # Giả lập sự tương tác: Upsell (Tăng giá) đột ngột sẽ làm tăng nguy cơ churn
    upsell_shock = np.where((df['expected_renewal_amount'] == 149) & 
                            (df['price_segment'] == 'Deal Hunter < 4.5'), 0.25, 0)

    raw_prob = base_risk + manual_penalty + skip_penalty + deal_penalty + \
               inactive_penalty + loyalty_bonus + rfm_bonus + upsell_shock
               
    np.random.seed(42)
    noise = np.random.normal(0, 0.02, len(df))
    return np.clip(raw_prob + noise, 0.01, 0.99)

# ==============================================================================
# BƯỚC 2: LOAD DATA VÀ TÍNH TOÁN BASELINE (HIỆN TRẠNG TRƯỚC KHI TÁC ĐỘNG)
# ==============================================================================
def load_and_prep_baseline():
    FEATURE_STORE_PATH = Path('./artifacts/feature_store/bi_feature_master.parquet')
    
    if FEATURE_STORE_PATH.exists():
        df = pd.read_parquet(FEATURE_STORE_PATH)
        df_sim = df[df['target_month'] == df['target_month'].max()].copy()
    else:
        print("-> CẢNH BÁO: Không tìm thấy Feature Store. Đang dùng Mock Data.")
        df_sim = pd.DataFrame({
            'msno': range(10000),
            'renewal_segment': np.random.choice(['Pay_Auto-Renew', 'Pay_Manual'], 10000, p=[0.4, 0.6]),
            'skip_segment': np.random.choice(['Low < 20%', 'Medium 20-50%', 'High > 50%'], 10000),
            'price_segment': np.random.choice(['Standard 4.5-6.5', 'Deal Hunter < 4.5'], 10000, p=[0.7, 0.3]),
            'expected_renewal_amount': np.where(np.random.rand(10000) > 0.3, 149.0, 99.0),
            'loyalty_segment': np.random.choice(['New < 30d', 'Loyal >= 365d'], 10000),
            'active_segment': np.random.choice(['Active 6-15 logs', 'Inactive'], 10000),
            'rfm_segment': np.random.choice(['Mid Value', 'High Value'], 10000)
        })

    # /// BƯỚC THỰC THI MODEL LẦN 1: TÍNH XÁC SUẤT GỐC ///
    df_sim['baseline_churn_prob'] = MOCK_ML_PREDICT_PROBA(df_sim)
    
    # Tính giá trị kỳ vọng T6 (6-Month Projected Value) thay vì vô hạn
    # Công thức: Tổng( Doanh thu * (1 - Churn)^tháng_t ) cho t từ 1 đến 6
    p_retain = 1 - df_sim['baseline_churn_prob']
    df_sim['baseline_6m_multiplier'] = p_retain * (1 - p_retain**6) / (1 - p_retain + 1e-9)
    df_sim['baseline_6m_value'] = df_sim['baseline_6m_multiplier'] * df_sim['expected_renewal_amount']
    
    return df_sim

# ==============================================================================
# BƯỚC 3: SIMULATION ENGINE (ĐỘNG CƠ MÔ PHỎNG CÓ TÍNH CHI PHÍ VÀ ROI)
# ==============================================================================
class PrescriptiveSimulation:
    def __init__(self, df_baseline):
        self.df_base = df_baseline.copy()
        self.colors = {'baseline': '#95A5A6', 'retention': '#1ABC9C', 'upsell': '#3498DB', 'cost': '#E74C3C'}

    def simulate_scenarios(self, inputs):
        """
        inputs: dict chứa các thông số từ C-Level Sliders trên Dashboard
        """
        df_sim = self.df_base.copy()
        
        cost_total = 0.0
        
        # ----------------------------------------------------------------------
        # KỊCH BẢN 1: CHIẾN DỊCH CHUYỂN ĐỔI AUTO-RENEW
        # ----------------------------------------------------------------------
        shift_auto_pct = inputs.get('shift_auto_pct', 0)
        auto_cac = inputs.get('auto_renew_cost_per_user', 0) # Tiền mồi (vd: tặng voucher 20 NTD)
        
        mask_auto = (df_sim['renewal_segment'] == 'Pay_Manual')
        target_indices = df_sim[mask_auto].sample(frac=shift_auto_pct/100, random_state=42).index
        
        # Thay đổi Features
        df_sim.loc[target_indices, 'renewal_segment'] = 'Pay_Auto-Renew'
        cost_total += len(target_indices) * auto_cac

        # ----------------------------------------------------------------------
        # KỊCH BẢN 2: CHIẾN DỊCH UPSELL LÊN GÓI CHUẨN (149 NTD)
        # ----------------------------------------------------------------------
        shift_upsell_pct = inputs.get('shift_upsell_pct', 0)
        upsell_cac = inputs.get('upsell_cost_per_user', 0)
        
        mask_upsell = (df_sim['price_segment'] == 'Deal Hunter < 4.5')
        target_upsell_indices = df_sim[mask_upsell].sample(frac=shift_upsell_pct/100, random_state=42).index
        
        # Thay đổi Features (Tăng tiền, đổi segment)
        df_sim.loc[target_upsell_indices, 'expected_renewal_amount'] = 149.0
        df_sim.loc[target_upsell_indices, 'price_segment'] = 'Standard 4.5-6.5'
        cost_total += len(target_upsell_indices) * upsell_cac

        # ----------------------------------------------------------------------
        # BƯỚC THỰC THI MODEL LẦN 2: TÍNH LẠI XÁC SUẤT SAU KHI THAY ĐỔI FEATURE
        # ----------------------------------------------------------------------
        # /// QUAN TRỌNG: Ở ĐÂY MODEL TỰ ĐỘNG BẮT ĐƯỢC TÁC DỤNG PHỤ CỦA UPSELL ///
        df_sim['simulated_churn_prob'] = MOCK_ML_PREDICT_PROBA(df_sim)
        
        # ----------------------------------------------------------------------
        # BƯỚC TÍNH TOÁN TÀI CHÍNH (FINANCIAL CALCULATION - TẦM NHÌN 6 THÁNG)
        # ----------------------------------------------------------------------
        p_retain_sim = 1 - df_sim['simulated_churn_prob']
        df_sim['simulated_6m_multiplier'] = p_retain_sim * (1 - p_retain_sim**6) / (1 - p_retain_sim + 1e-9)
        df_sim['simulated_6m_value'] = df_sim['simulated_6m_multiplier'] * df_sim['expected_renewal_amount']
        
        baseline_total_value = self.df_base['baseline_6m_value'].sum()
        simulated_total_value = df_sim['simulated_6m_value'].sum()
        
        gross_gain = simulated_total_value - baseline_total_value
        net_roi = gross_gain - cost_total
        
        return {
            'baseline_6m_value': baseline_total_value,
            'simulated_6m_value': simulated_total_value,
            'gross_gain': gross_gain,
            'campaign_cost': cost_total,
            'net_roi': net_roi,
            'df_result': df_sim
        }

    def plot_financial_waterfall(self, res):
        fig = go.Figure(go.Waterfall(
            name="Tài chính 6 Tháng", orientation="v",
            measure=["absolute", "relative", "relative", "total"],
            x=["Doanh thu Baseline<br>(6 Tháng)", "Tăng trưởng Gộp<br>(Giữ chân + Upsell)", 
               "Chi phí Chiến dịch<br>(Marketing Cost)", "Lợi nhuận Ròng Kỳ vọng<br>(Net Projected)"],
            y=[res['baseline_6m_value'], res['gross_gain'], -res['campaign_cost'], res['baseline_6m_value'] + res['net_roi']],
            connector={"line":{"color":"#BDC3C7", "width": 2, "dash": "dot"}},
            decreasing={"marker":{"color": self.colors['cost']}},
            increasing={"marker":{"color": self.colors['retention']}},
            totals={"marker":{"color": "#2C3E50"}},
            text=[f"{v/1e6:,.1f}M" for v in [res['baseline_6m_value'], res['gross_gain'], -res['campaign_cost'], res['baseline_6m_value'] + res['net_roi']]],
            textposition="outside",
            textfont=dict(size=14, family="Arial", color="black")
        ))
        fig.update_layout(title='Biểu đồ: Phân rã ROI Chiến lược (Tầm nhìn 6 tháng)', plot_bgcolor='white')
        return fig

# ==============================================================================
# BƯỚC 4: THỰC THI DASHBOARD
# ==============================================================================
if __name__ == "__main__":
    print("\n--- KHỞI TẠO HỆ THỐNG SIMULATION V2.0 ---")
    df_base = load_and_prep_baseline()
    engine = PrescriptiveSimulation(df_base)
    
    # INPUT TỪ UI STREAMLIT CỦA C-LEVEL
    SIMULATION_INPUTS = {
        'shift_auto_pct': 40.0,            # Mục tiêu: Đưa 40% người dùng Manual sang Auto
        'auto_renew_cost_per_user': 15.0,  # Chi phí: Tặng voucher 15 NTD/người để thuyết phục
        
        'shift_upsell_pct': 20.0,          # Mục tiêu: Ép 20% Deal Hunter mua gói Standard
        'upsell_cost_per_user': 5.0,       # Chi phí: Chạy Ads thông báo, tốn 5 NTD/người
    }
    
    print("\nĐang chạy mô phỏng qua Model...")
    results = engine.simulate_scenarios(SIMULATION_INPUTS)
    
    print("\n=== BÁO CÁO C-LEVEL (TẦM NHÌN DOANH THU 6 THÁNG) ===")
    print(f"1. Hiện trạng (Doanh thu yên phận):     {results['baseline_6m_value']:,.0f} NTD")
    print(f"2. Tăng thu (Nhờ giữ chân & Tăng giá): +{results['gross_gain']:,.0f} NTD")
    print(f"3. Chi phí phải bỏ ra (Marketing CAC): -{results['campaign_cost']:,.0f} NTD")
    print("-" * 55)
    if results['net_roi'] > 0:
        print(f"-> KẾT LUẬN: CHIẾN DỊCH LÃI (NET ROI): +{results['net_roi']:,.0f} NTD")
    else:
        print(f"-> CẢNH BÁO: CHIẾN DỊCH LỖ (NET ROI):  {results['net_roi']:,.0f} NTD (KHUYÊN KHÔNG NÊN CHẠY!)")
    
    # Hiển thị biểu đồ (Nếu chạy trong Jupyter/Kaggle)
    fig_waterfall = engine.plot_financial_waterfall(results)
    fig_waterfall.show()
```

### 6. Các biểu đồ còn lại

Lưu ý nhỏ: Ở phiên bản mới (sử dụng Xác suất Churn từ Machine Learning thay vì hệ số Hazard của Cox), thuật ngữ của biểu đồ số 1 sẽ đổi từ "Hazard Shift" thành **"Risk Probability Shift"**.

Dưới đây là tài liệu mô tả cho 3 biểu đồ còn lại:

---

### Biểu đồ 2: Phân bố Dịch chuyển Rủi ro (Population Risk Shift)
*Biểu đồ vùng (Area Chart) so sánh tỷ trọng khách hàng theo từng mức độ rủi ro.*

*   **Input (Đầu vào):** 
    *   Cột `baseline_churn_prob` (Xác suất rời bỏ trước can thiệp).
    *   Cột `simulated_churn_prob` (Xác suất rời bỏ sau can thiệp).
*   **Cách tính:** 
    *   Chia dải xác suất (0% - 100%) thành các bin nhỏ (VD: 0-5%, 5-10%...).
    *   Đếm phần trăm (`% Users`) rơi vào từng bin ở 2 trạng thái Baseline (Hiện trạng) và Simulated (Kỳ vọng).
*   **Output (Đầu ra):** Hai vùng màu chồng lên nhau. Trục X là Mức độ rủi ro Churn (%), Trục Y là Tỷ trọng khách hàng (%).
*   **Ý nghĩa Kinh doanh (Business Value):**
    *   Biểu đồ Tài chính (Waterfall) chỉ cho sếp thấy "Tiền", còn biểu đồ này cho sếp thấy **"Sức khỏe của tập khách hàng"**.
    *   Nếu chiến dịch thành công, vùng màu của Simulated sẽ **dịch chuyển mạnh về phía bên trái** (tức là dồn khách hàng về vùng an toàn có rủi ro thấp). 
    *   Giúp phát hiện rủi ro phân cực: Giả sử doanh thu tăng, nhưng biểu đồ lại cho thấy một lượng lớn khách hàng bị đẩy sang vùng rủi ro > 80% (do bị ép Upsell), sếp sẽ nhận ra chiến dịch này đang vắt kiệt khách hàng và không bền vững.

---

### Biểu đồ 3: Phân tích Độ nhạy & Ưu tiên Nguồn lực (Sensitivity & ROI per 1% Effort)
*Biểu đồ thanh ngang (Horizontal Bar Chart) xếp hạng hiệu quả của từng chiến lược.*

*   **Input (Đầu vào):** Hàm `simulate_scenarios()` sẽ chạy ngầm dưới background nhiều lần.
*   **Cách tính:** 
    *   Hệ thống tự động chạy kịch bản 1: Chỉ kéo slider Auto-Renew lên **1%** (giữ các slider khác bằng 0), ghi nhận `Net ROI`.
    *   Chạy kịch bản 2: Chỉ kéo slider Upsell lên **1%**, ghi nhận `Net ROI`.
    *   Chạy kịch bản 3: Chỉ kéo slider Giảm Skip lên **1%**, ghi nhận `Net ROI`.
*   **Output (Đầu ra):** Các thanh ngang thể hiện số tiền thực tế mang về (Lợi nhuận ròng) cho mỗi **1% nỗ lực chuyển đổi**.
*   **Ý nghĩa Kinh doanh (Business Value):**
    *   Đây là biểu đồ giải quyết bài toán **Phân bổ ngân sách (Resource Allocation)**. Nguồn lực công ty là có hạn, không thể làm tất cả mọi thứ cùng lúc.
    *   Câu trả lời cho sếp: *"Nếu tháng này chúng ta chỉ có đủ nhân sự để cải thiện 1 chỉ số duy nhất thêm 1%, thì việc ép đội Tech giảm Skip Rate mang lại 200 triệu, trong khi đưa cho Sale đi Upsell chỉ mang về 50 triệu. Vậy hãy dồn toàn lực cho Tech!"*

---

### Biểu đồ 4: Phân phối Rủi ro Tài chính - Monte Carlo (Financial Risk Distribution)
*Biểu đồ Histogram hình chuông với các mốc P10, P50, P90.*

*   **Input (Đầu vào):** 
    *   `simulated_6m_value`: Lợi nhuận kỳ vọng cuối cùng từ kịch bản sếp vừa tạo.
    *   `Volatility (%)`: Độ biến động của thị trường (Ví dụ: Đối thủ ra gói cước mới làm tỷ lệ nhiễu tăng 8-10%). Tham số này sếp có thể tự chỉnh trên UI.
*   **Cách tính:** 
    *   Sử dụng phân phối chuẩn (`np.random.normal`), cho máy tính chạy giả lập **10,000 lần** kịch bản tài chính đó với các mức độ chênh lệch ngẫu nhiên.
    *   Cắt lấy 3 mốc: 
        *   `P10`: Mốc mà chỉ có 10% kịch bản cho kết quả tệ hơn (Trường hợp rất Xấu).
        *   `P50`: Mức trung vị (Kỳ vọng chuẩn).
        *   `P90`: Mốc mà chỉ có 10% kịch bản tốt hơn (Trường hợp Đột phá).
*   **Output (Đầu ra):** Một dải hình chuông mô tả dải doanh thu có thể đạt được, kèm 2 đường nét đứt cảnh báo mốc P10 và P90.
*   **Ý nghĩa Kinh doanh (Business Value):**
    *   CFO (Giám đốc Tài chính) rất ghét việc Data Scientist đưa ra đúng "1 con số duy nhất" (Single-point estimate) vì thực tế đời không như mơ.
    *   Biểu đồ này là công cụ **Quản trị rủi ro (Risk Management)**. Nó nói với CFO rằng: *"Mô hình báo lãi 1 tỷ, nhưng do độ nhiễu của thị trường là 8%, sếp hãy chuẩn bị tâm lý cho tình huống P10 (Xấu nhất) là chúng ta chỉ thu về 700 triệu. Kế hoạch dòng tiền nên dựa vào con số 700 triệu này để an toàn tuyệt đối."*

Code gợi ý:
```python
    # ==========================================================================
    # BIỂU ĐỒ 2: DỊCH CHUYỂN RỦI RO (POPULATION RISK SHIFT)
    # ==========================================================================
    def plot_risk_shift(self, df_result):
        """
        df_result: lấy từ kết quả trả về của hàm simulate_scenarios()['df_result']
        """
        # Chuyển đổi dữ liệu để vẽ biểu đồ đè (Overlay)
        df_plot = pd.DataFrame({
            'Baseline (Hiện trạng)': df_result['baseline_churn_prob'],
            'Simulated (Kỳ vọng)': df_result['simulated_churn_prob']
        }).melt(var_name='Kịch bản', value_name='Xác suất Churn')

        fig = px.histogram(
            df_plot, 
            x='Xác suất Churn', 
            color='Kịch bản',
            barmode='overlay',
            nbins=50,
            marginal='box', # Thêm boxplot nhỏ ở trên cùng để sếp dễ nhìn Median
            color_discrete_map={
                'Baseline (Hiện trạng)': self.colors['baseline'], 
                'Simulated (Kỳ vọng)': self.colors['retention']
            },
            title='Biểu đồ 2: Dịch chuyển Cấu trúc Rủi ro (Risk Probability Shift)'
        )
        
        fig.update_layout(
            plot_bgcolor='white',
            yaxis_title='Số lượng Khách hàng',
            xaxis_title='Xác suất Rời bỏ (Churn Probability)',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig.update_xaxes(tickformat='.0%', gridcolor='#F2F3F4')
        fig.update_yaxes(gridcolor='#F2F3F4')
        
        return fig

    # ==========================================================================
    # BIỂU ĐỒ 3: PHÂN TÍCH ĐỘ NHẠY (SENSITIVITY & ROI PER 1%)
    # ==========================================================================
    def plot_sensitivity(self, cac_inputs):
        """
        cac_inputs: dict chứa chi phí CAC hiện tại để tính ROI cho đúng thực tế.
        Ví dụ: {'auto_cac': 15.0, 'upsell_cac': 5.0, 'skip_cac': 20.0}
        """
        # Chạy máy mô phỏng 3 lần, mỗi lần chỉ nhích đúng 1% cho 1 chiến lược
        res_auto = self.simulate_scenarios({
            'shift_auto_pct': 1.0, 
            'auto_renew_cost_per_user': cac_inputs.get('auto_cac', 15.0)
        })
        res_upsell = self.simulate_scenarios({
            'shift_upsell_pct': 1.0, 
            'upsell_cost_per_user': cac_inputs.get('upsell_cac', 5.0)
        })
        
        # (Giả định cậu có code kịch bản giảm Skip Rate trong simulate_scenarios)
        # Nếu chưa có, ta dùng một biến số dummy để minh họa
        res_skip = self.simulate_scenarios({
            'shift_skip_pct': 1.0,  # Ép đội Tech làm giảm nhóm High-Skip 1%
            'skip_cost_per_user': cac_inputs.get('skip_cac', 20.0) # Chi phí Tech/Voucher
        })

        # Gom kết quả
        data = pd.DataFrame({
            'Chiến lược': [
                'Thuyết phục Auto-Renew (Sale/Mkt)', 
                'Ép gói Giá chuẩn Upsell (Mkt)', 
                'Cải thiện Recommend System (Tech)'
            ],
            'Net_ROI': [res_auto['net_roi'], res_upsell['net_roi'], res_skip['net_roi']]
        }).sort_values('Net_ROI', ascending=True)

        fig = px.bar(
            data, 
            x='Net_ROI', 
            y='Chiến lược', 
            orientation='h',
            text_auto='.0f',
            color='Net_ROI', 
            color_continuous_scale='Teal',
            title='Biểu đồ 3: Phân tích Độ nhạy (Lợi nhuận ròng mang lại từ mỗi 1% Nỗ lực)'
        )
        
        fig.update_layout(
            plot_bgcolor='white', 
            coloraxis_showscale=False,
            xaxis=dict(title="Net ROI sinh ra từ 1% chuyển đổi (NTD)"),
            yaxis=dict(title=""),
            font=dict(size=14)
        )
        fig.update_traces(texttemplate='+ %{x:,.0f} NTD', textposition='outside')
        
        return fig

    # ==========================================================================
    # BIỂU ĐỒ 4: PHÂN PHỐI RỦI RO TÀI CHÍNH (MONTE CARLO)
    # ==========================================================================
    def plot_monte_carlo(self, projected_total_value, volatility=8.0, iterations=10000):
        """
        projected_total_value: Kết quả doanh thu cuối cùng (baseline_6m_value + net_roi)
        volatility: % nhiễu loạn thị trường (mặc định 8%)
        """
        np.random.seed(42)
        std_dev = projected_total_value * (volatility / 100)
        
        # Chạy giả lập 10,000 kịch bản ngẫu nhiên
        mc_results = np.random.normal(loc=projected_total_value, scale=std_dev, size=iterations)

        # Tính các phân vị quan trọng cho CFO
        p10 = np.percentile(mc_results, 10)
        p50 = np.percentile(mc_results, 50)
        p90 = np.percentile(mc_results, 90)

        df_mc = pd.DataFrame({'Doanh thu': mc_results})

        fig = px.histogram(
            df_mc, 
            x='Doanh thu', 
            nbins=100, 
            title=f'Biểu đồ 4: Phân phối Rủi ro Dòng tiền (Monte Carlo - Nhiễu {volatility}%)',
            color_discrete_sequence=['#2C3E50']
        )
        
        fig.update_layout(
            showlegend=False, 
            plot_bgcolor='white', 
            yaxis=dict(title="Tần suất (Số Kịch bản)"),
            xaxis=dict(title="Doanh thu Kỳ vọng 6 Tháng (NTD)")
        )

        # Thêm các đường V-Line cảnh báo
        fig.add_vline(x=p10, line_dash="dash", line_color=self.colors['cost'], 
                      annotation_text=f"Trường hợp XẤU (P10): {p10/1e6:.1f}M", 
                      annotation_position="top left")
                      
        fig.add_vline(x=p50, line_dash="solid", line_color='white', 
                      annotation_text=f"KỲ VỌNG (P50): {p50/1e6:.1f}M",
                      annotation_position="top")

        fig.add_vline(x=p90, line_dash="dash", line_color=self.colors['retention'], 
                      annotation_text=f"Trường hợp TỐT (P90): {p90/1e6:.1f}M", 
                      annotation_position="top right")
        
        return fig
```

### Cách gọi các hàm này trong khối `__main__` (Thực thi)

Sau khi cậu gọi hàm tính toán `simulate_scenarios`, cậu truyền kết quả của nó vào các hàm vẽ biểu đồ:

```python
    # ... (Tiếp nối code ở phần Main trước)
    
    print("\nĐang tạo các biểu đồ phân tích...")
    
    # 1. Gọi biểu đồ Waterfall (Có sẵn từ lần trước)
    fig_waterfall = engine.plot_financial_waterfall(results)
    
    # 2. Gọi biểu đồ Risk Shift
    fig_risk = engine.plot_risk_shift(results['df_result'])
    
    # 3. Gọi biểu đồ Sensitivity (Truyền CAC hiện tại vào)
    cac_params = {
        'auto_cac': SIMULATION_INPUTS['auto_renew_cost_per_user'],
        'upsell_cac': SIMULATION_INPUTS['upsell_cost_per_user'],
        'skip_cac': 10.0 # Chi phí giả định cho đội Tech
    }
    fig_sensitivity = engine.plot_sensitivity(cac_params)
    
    # 4. Gọi biểu đồ Monte Carlo
    projected_net_value = results['baseline_6m_value'] + results['net_roi']
    fig_mc = engine.plot_monte_carlo(projected_net_value, volatility=8.0)
    
    # Hiển thị
    fig_waterfall.show()
    fig_risk.show()
    fig_sensitivity.show()
    fig_mc.show()
```

---

### Tổng kết 4 Biểu đồ trên 1 Dashboard:
Nếu cậu đặt 4 biểu đồ này cạnh nhau, cậu đã kể một câu chuyện dữ liệu (Data Storytelling) hoàn hảo cho Ban Giám Đốc:
1.  **Waterfall:** "Chiến lược này lãi hay lỗ bao nhiêu tiền?"
2.  **Risk Shift:** "Cấu trúc khách hàng khỏe lên hay yếu đi?"
3.  **Sensitivity:** "Nên ưu tiên việc gì trước để tối ưu ROI?"
4.  **Monte Carlo:** "Đâu là giới hạn an toàn để dự phòng tài chính?"

