# Report And Slides

Folder này chứa tài liệu hỗ trợ viết báo cáo, sắp xếp nội dung trình bày, và demo.

## File Chính

- `kkbox_pipeline_descriptions.md`: mô tả từng pipeline theo kiểu box-arrow-box để vẽ diagram.
- `kkbox_report_diagrams.md`: danh sách diagram nên vẽ, theo thứ tự kể chuyện.
- `demo_script.md`: kịch bản demo, operator checklist, và talk track cho buổi trình bày đầu tiên.

## Luồng Đọc Nhanh Để Làm Slide

1. đọc `kkbox_report_diagrams.md`
2. đọc `kkbox_pipeline_descriptions.md` để vẽ lại các pipeline chính
3. chốt thông điệp bằng cách đối chiếu `../system_description/predictive.md` và `../system_description/prescriptive.md`
4. đối chiếu lại terminology với `../system_description/kkbox_feature_catalog.md`

## Luồng Chạy Demo

1. chạy `../../scripts/run_demo.sh`
2. nếu stack đã chạy rồi, dùng `../../scripts/validate_demo.sh`
3. mở `demo_script.md` và đi đúng thứ tự màn hình đã chốt

## Lưu Ý

- Folder này dùng để kể chuyện. Nếu cần semantics canonical, quay lại `../system_description/`.
- Diagram và pipeline trong folder này phải nói cùng ngôn ngữ với `kkbox_feature_catalog.md`.
