# Stage 8.4c — Expiry và dọn Storage

Worker chỉ chạy khi `MEDIA_UPLOAD_ENABLED=true`; khi tắt, không quét DB hay gọi Storage. Worker hết hạn intent theo lô tối đa 16, không phụ thuộc credential Gateway còn active. `pending`/`verifying` chỉ chuyển terminal sang `expired`; `rejected` và `expired` giữ nguyên kết quả và đường dẫn.

Lịch dọn bắt đầu không sớm hơn `GREATEST(intent_expires_at, latest_upload_grant_expires_at)`. Worker xóa bằng Storage API xác thực, chỉ dùng `storage_path` đã lưu. Lần xóa thành công đầu tiên mở cửa sổ đối soát bổ sung; sau cửa sổ đó worker xóa lại rồi mới đánh dấu xác nhận và giải phóng quota. Cửa sổ chỉ bao phủ grant/PUT còn hiệu lực theo cấu hình, không đảm bảo phát hiện ghi trễ vô hạn.

Lỗi Storage được retry hữu hạn theo backoff mũ có trần. Sau số lần tối đa, `last_cleanup_error=CLEANUP_EXHAUSTED`, `next_cleanup_at=NULL`; quota vẫn bị giữ. Vận hành cần kiểm tra object và trạng thái DB bằng tài khoản quản trị, xóa thủ công đúng object trong bucket `media-video` nếu phù hợp, sau đó xác nhận absence và cập nhật `cleanup_confirmed_at`/`next_cleanup_at` theo quy trình DBA đã duyệt. Không sửa `validation_status` hoặc đường dẫn để giải phóng quota.
