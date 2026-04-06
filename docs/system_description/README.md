# System Description

Folder này chứa các tài liệu mô tả hệ thống ở mức nghiệp vụ, dữ liệu, và pipeline. Đây là nhóm docs cần đọc đầu tiên nếu muốn tiếp tục phát triển sản phẩm.

## Thứ Tự Đọc Để Lấy Context

1. `kkbox_feature_catalog.md`
2. `project_desc.md`
3. `tab1_data_strategy.md`
4. `kkbox_tab2_predictive_pipeline.md`
5. `predictive.md`
6. `prescriptive.md`
7. `data_dictionary.md`
8. `team_coding_guide.md`

## Vai Trò Từng File

- `kkbox_feature_catalog.md`: nguồn sự thật cao nhất cho feature semantics, snapshot logic, segment threshold batch, và boundary grounded/proxy.
- `project_desc.md`: context sản phẩm, maturity hiện tại, conflict cần ghi nhớ, và guardrail cho agent sau.
- `tab1_data_strategy.md`: thiết kế dữ liệu và serving strategy cho Tab 1.
- `kkbox_tab2_predictive_pipeline.md`: contract train/validation/score và artifact đầu ra cho Tab 2.
- `predictive.md`: mô tả nghiệp vụ và chart logic cho Tab 2, đã hạ mức maturity theo notebook hiện có.
- `prescriptive.md`: mô tả nghiệp vụ và scenario logic cho Tab 3, có ghi rõ phần grounded và phần prototype.
- `data_dictionary.md`: field gốc của dataset KKBOX.
- `team_coding_guide.md`: quy ước code, naming, và review.

## Nguyên Tắc Ưu Tiên

- Nếu `predictive.md` hoặc `prescriptive.md` mâu thuẫn với `kkbox_feature_catalog.md`, ưu tiên `kkbox_feature_catalog.md`.
- `project_desc.md` dùng để giữ context sản phẩm, không được để override semantics feature batch.
