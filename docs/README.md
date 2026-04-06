# Docs Index

Tài liệu trong `docs/` được gom thành 3 nhóm để dễ tìm, dễ đọc theo luồng, và dễ giữ đồng bộ khi cập nhật:

- `system_description/`: semantics feature, phạm vi sản phẩm, pipeline, và mô tả nghiệp vụ cho từng tab.
- `architecture_diagrams/`: luồng runtime, event flow, và contract kiến trúc.
- `report_and_slides/`: tài liệu để viết báo cáo, vẽ diagram, và sắp xếp slide.

## Đọc Nhanh Theo Mục Đích

Nếu cần hiểu hệ thống canonical:

1. `system_description/kkbox_feature_catalog.md`
2. `system_description/project_desc.md`
3. `architecture_diagrams/architecture.md`
4. `system_description/tab1_data_strategy.md`
5. `system_description/kkbox_tab2_predictive_pipeline.md`
6. `system_description/predictive.md`
7. `system_description/prescriptive.md`

Nếu cần viết báo cáo và làm slide:

1. `report_and_slides/kkbox_report_diagrams.md`
2. `report_and_slides/kkbox_pipeline_descriptions.md`
3. `report_and_slides/demo_script.md`
4. đối chiếu lại terminology với `system_description/kkbox_feature_catalog.md`

## File Canonical

- Feature semantics và snapshot logic: `system_description/kkbox_feature_catalog.md`
- Product scope, maturity, và conflict đang mở: `system_description/project_desc.md`
- Runtime architecture: `architecture_diagrams/architecture.md`
- Tab 2 train/score contract: `system_description/kkbox_tab2_predictive_pipeline.md`
- Tab 2 business narrative grounded: `system_description/predictive.md`
- Tab 3 business narrative grounded: `system_description/prescriptive.md`
