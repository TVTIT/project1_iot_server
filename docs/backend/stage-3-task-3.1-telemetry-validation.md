# Task 3.1 — Telemetry DTO và validation thuần

**Ngày thực hiện:** 15/10/2026.  
**Baseline:** `b263004`, cộng các thay đổi Task 3.1 chưa commit.  
**Trạng thái:** Hoàn thành 3.1.1–3.1.7 trong phạm vi xử lý thuần và unit tests. Chưa nối telemetry ingress vào server.

Phạm vi theo [kế hoạch Task 3.1](../../../docs/backend_plan/task_3.1_detail_plan.md) và [protocol MQTT v1](../protocols/mqtt-v1.md). Package mới là [`src/internal/telemetry/`](../../src/internal/telemetry/), xử lý dữ liệu thuần trong Go.

## 1. Phân công tuần tự

Theo yêu cầu người dùng, toàn bộ phần triển khai và kiểm tra được giao lần lượt cho subagent **GPT-6-Luna**. Mỗi lượt nhận đầu vào từ lượt trước, phạm vi file được sửa và tiêu chí hoàn thành cụ thể; không có hai subagent sửa mã cùng lúc.

| Lượt | Subagent | Công việc và phạm vi file được giao trong `src/internal/telemetry/` |
| --- | --- | --- |
| 1 | `telemetry_foundation` | 3.1.1–3.1.2: `model.go`, `topic.go`, `errors.go` và test model/topic. Kiểu dữ liệu, giới hạn và topic parser dùng lại identifier validator. |
| 2 | `telemetry_decoder` | 3.1.3: `decode.go`, `decode_test.go`. Decode JSON, field bắt buộc, số hữu hạn, UUID, count và topic binding. |
| 3 | `telemetry_time_ranges` | 3.1.4: `validation.go`, `validation_test.go`. Sequence/interval/span overflow, RFC3339, UTC và future skew. |
| 4 | `telemetry_foundation` | 3.1.5: `message.go`, `message_test.go`. Entrypoint validation, snapshot/hash, sample expansion và accessor trả bản sao. |
| 5 | `telemetry_decoder` | 3.1.6: `errors.go`, `errors_test.go`, `ack.go`, `ack_test.go`. Phân loại lỗi và tạo ACK DTO từ kết quả commit đã xác nhận. |
| 6 | `telemetry_time_ranges` | 3.1.7: test còn thiếu, kiểm tra tích hợp package, format/lint/build; giới hạn sửa chức năng vào kiểm format UUID bằng thư viện hiện có. |

Các prompt đều yêu cầu giữ nguyên file ngoài phạm vi, không commit, không thêm dependency, không thao tác `.env`, database hoặc broker. Root đối chiếu kết quả từng phần và cập nhật tài liệu bàn giao.

## 2. API và cách sử dụng

Các API chính:

- `Limits.Validate()` và `NewValidator(Limits)`: kiểm giới hạn byte, số mẫu, span và future skew do caller truyền. Không đọc environment hoặc thay default runtime.
- `ParseTopic(string)`: nhận đúng `gateways/<gateway_id>/telemetry/<sensor_id>`; giữ nguyên topic và identity, dùng lại `gateway.ValidateGatewayID`/`ValidateSensorID`.
- `Validator.Validate(topic, payload, receivedAt)`: trả `*Message` hợp lệ hoặc `nil` cùng lỗi. Timestamp nhận message do caller truyền; không gọi đồng hồ ngầm.
- `Message.Topic()`, `Batch()`, `RawPayload()`, `PayloadHash()`, `ReceivedAt()`, `Samples()`, `FirstSample()` và `LastSample()`: cung cấp dữ liệu đã kiểm tra cho task lưu trữ tiếp theo. Các slice trả về là bản sao.
- `ClassifyFailure(error)`: phân biệt input/conflict, retry có backoff, kiểm đồng hồ, can thiệp provisioning/legacy, commit chưa xác nhận và ACK transfer chưa xác nhận. Hỗ trợ lỗi được wrap; lỗi không biết giữ trạng thái unknown.
- `NewCommitACK(gatewayID, messageID, outcome, confirmedAt)`: chỉ tạo DTO cho `CommitOutcomeCommitted` hoặc `CommitOutcomeDuplicateCommitted`; cả hai dùng wire `status=committed`.

`BatchDTO` là dữ liệu chưa được bảo đảm hợp lệ nếu caller tự tạo hoặc tự unmarshal. Luồng nhận telemetry phải qua `Validator.Validate`; parse thành công không tạo kết quả commit. Thời gian trong DTO giữ biểu diễn payload ban đầu; timestamp của sample candidates được chuẩn hóa UTC.

Profile tổng hợp được truyền trong unit tests: 65.536 payload bytes, tối đa 1.000 samples, sample span 60s và future skew 2m. Đây chưa phải cấu hình được nối vào server.

## 3. Hành vi đã triển khai

1. Kiểm byte bound trước copy/decode. Decoder chỉ nhận một object với đủ các field protocol; từ chối unknown/duplicate fields, null, sai kiểu và JSON document dư. Integer fields không nhận fraction/exponent/string; mẫu phải là float64 hữu hạn.
2. Giới hạn cả count khai báo và số phần tử thực tế. Không dùng count chưa kiểm để cấp phát; decode samples dừng khi vượt maximum.
3. Kiểm Gateway/Sensor trong payload khớp topic; UUID không nil, đúng RFC variant, message UUID chỉ v4/v7. Không tự sửa ID hoặc tạo tài nguyên từ payload.
4. Kiểm arithmetic trước cộng/nhân: signed BIGINT sequence, microsecond sang duration, span và timestamp cuối. Timestamp yêu cầu RFC3339 có timezone, tối đa sáu chữ số phần thập phân, năm UTC 0001–9999; không làm tròn input sai.
5. Kiểm future skew ở mẫu cuối, cho phép đúng ngưỡng và dữ liệu cũ. Không kiểm dedup/overlap hoặc xác định boot mới hơn trong logic thuần này.
6. Snapshot giữ bytes gốc; hash là lowercase SHA-256 của đúng bytes đó, topic lưu riêng. Thay whitespace/key order làm đổi hash. Mẫu được tạo đủ theo thứ tự và mang provenance của message.
7. ACK DTO có đúng năm field protocol. Zero/unknown commit outcome và identity/time không hợp lệ bị từ chối. Caller vẫn phải xác nhận commit thật hoặc đọc ledger committed trước khi gọi builder.

Caller không được sửa payload đồng thời khi `Validate` đang chạy. Sau khi hàm trả về, thay đổi buffer nguồn hoặc các slice accessor không làm đổi snapshot/candidates đã lưu bên trong `Message`.

## 4. Kiểm tra và kết quả

Các lệnh Go chạy từ `main_src/src/`, với `GOCACHE=/tmp/task31-go-build`, `GOMODCACHE=/home/trung/go/pkg/mod`, `GOPROXY=off`; lint dùng cache tạm khi cần. Không tải hoặc thay dependency.

| Kiểm tra | Kết quả |
| --- | --- |
| `go test -race -count=1 ./internal/telemetry ./internal/gateway` | PASS. Gồm unit tests telemetry và Gateway; các test database có fixture gate không được bật. |
| `go vet ./internal/telemetry` | PASS. |
| `golangci-lint run ./internal/telemetry/... --timeout=5m` | PASS, 0 issues sau khi sửa comment/empty block mà revive phát hiện. Không đổi cấu hình lint. |
| `go build ./...` | PASS. |
| `gofmt -l internal/telemetry/*.go` | Không có file chưa format. |
| `git diff --check` | Không có lỗi ở diff tracked; file mới được kiểm thêm whitespace và liên kết khi bàn giao. |

Normal path kiểm từng mẫu của batch 100 samples, first sequence 12.501, interval 10.000 us: sample cuối là sequence **12.600**, tại **t0 + 990ms**. Các nhóm ca âm/biên kiểm count, JSON/null, UUID/topic, payload/sample bounds, arithmetic/time, future skew và nil Message khi input sai. Tests cũng kiểm raw hash cố định, đổi whitespace/key order, mutation isolation và ACK DTO đúng năm field.

Trong lượt cuối, đã thêm `uuid.Validate` trước `uuid.Parse` cùng regression cho chuỗi UUID 38 ký tự có hai ký tự bọc sai: thư viện Parse riêng lẻ có thể chỉ đọc 36 ký tự ở giữa. Dùng lại thư viện hiện có, không thêm bộ xác minh UUID riêng.

Một lượt bổ sung ngoài acceptance là `GOARCH=386 go test ./internal/telemetry` dừng khi chạy với `signal: bad system call`; không tính PASS hoặc bằng chứng runtime 32 bit. Test biểu thức vượt MaxInt32 đã được sửa để tránh chuyển hằng số vượt miền int trên 32 bit. Không mở rộng ma trận kiểm tra hoặc thay môi trường để xử lý lượt bổ sung này.

## 5. Bàn giao và phần chưa thực hiện

- Task 3.2 nhận message identity, hash/topic, raw sample candidates và metadata để triển khai resource resolution, ledger, overlap và transaction persistence.
- Task 3.3 dùng sample cuối đã kiểm để ghi Sensor reported state và temporal metadata trong cùng transaction. Unit tests hiện tại chưa chứng minh Twin writes.
- Task 3.4 nối config, snapshot từ callback, retained-message rejection, queue/workers, kết quả transaction và publish ACK. Khi tích hợp, giữ quyền sở hữu buffer rõ ràng và tránh copy thêm ở các lớp không cần giữ riêng dữ liệu.
- `NewCommitACK` là quy tắc chuyển đổi dữ liệu; unit tests không chứng minh database đã commit hoặc ACK thực sự được publish sau commit.

Chưa chạy database/broker E2E, migration, ACL/TLS acceptance, workload 100 Hz, remote CI hoặc runtime trên board. Các acceptance U01–U15 của Stage 3 vẫn chưa được nghiệm thu bằng Task 3.1. Không đổi module dependencies, cấu hình deployment hoặc composition của server.

## 6. Cập nhật toolchain/dependency sau review — 15/10/2026

Đây là thay đổi tiếp theo theo yêu cầu người dùng sau lượt review, bổ sung vào kết quả Task 3.1 ở trên. Lượt quét trước với Go 1.27.1 và `x/net v0.59.0` thoát mã 3, báo 7 advisory ở mức symbol trong standard library, 6 ở mức imported package và 1 ở mức module. Kết quả đó không phải bằng chứng mọi điều kiện khai thác đều tồn tại trong deployment.

Đã xác nhận Go local là **1.27.2** và Dockerfile do người dùng sửa dùng `golang:1.27.2-alpine`. Container tạm từ chính image này chạy `go version` trả **go1.27.2 linux/amd64**; container tự xóa sau khi chạy. Đây là kiểm tra builder, chưa phải rebuild/redeploy backend image. [Release notes Go 1.27.2](https://go.dev/doc/devel/release#go1.27.2) ghi các bản vá bảo mật tương ứng.

Các thay đổi bổ sung:

- Toàn bộ **10** vị trí `actions/setup-go` trong `.github/workflows/ci.yml` dùng `1.27.2`; đồng bộ hai assertion phiên bản trong unit tests CI và mô tả CI ở README.
- `src/go.mod` yêu cầu Go `1.27.2`.
- Truy vấn `go list -m -json golang.org/x/net@latest` qua Go module proxy chính thức trả **v0.61.0**, phát hành 09/10/2026; đã pin exact version này trong `go.mod` và cập nhật checksum. Xem [x/net v0.61.0](https://pkg.go.dev/golang.org/x/net@v0.61.0).
- Dependency graph yêu cầu nâng kèm `x/crypto v0.58.0`, `x/sync v0.24.0`, `x/sys v0.49.0`, `x/text v0.43.0`. `go mod tidy` cũng phân loại lại `puddle/v2`, `x/crypto`, `x/sys` thành direct dependencies theo imports vốn đã có; version của `puddle/v2` giữ nguyên.

Kiểm tra bổ sung trên Go 1.27.2:

| Kiểm tra | Kết quả |
| --- | --- |
| `go test -race -count=1 ./...` | PASS; các test native integration có fixture gate không được bật. |
| `go vet ./...` | PASS. |
| `golangci-lint run --timeout=5m` | PASS, 0 issues. |
| `go build ./...` | PASS. |
| Build CGO=0 server linux/amd64 và Gateway linux/armv7 như CI | PASS, chỉ build; chưa chạy binary trên board ARM. |
| `go mod verify` | PASS, all modules verified. |
| Unit tests CI guard/feature/smoke và ba harness Stage 2 liên quan workflow | PASS, 119 tests. Đây là Python unit tests, không phải native Compose E2E. |
| `govulncheck -show verbose ./...` | PASS, exit 0: 0 vulnerability ở mức symbol, 0 ở imported package. |

Lượt quét vẫn ghi **GO-2026-5932** ở mức module `x/crypto`: cảnh báo package `openpgp` không còn được duy trì. Dự án không import package đó; scanner không tìm thấy đường gọi tới phần lỗi. Không coi cảnh báo module này là lỗ hổng đã xác nhận trên đường chạy hiện tại hoặc lý do loại bỏ các package crypto khác đang sử dụng.

Chưa chạy GitHub Actions từ xa, full native integration hoặc triển khai lại server cho lần nâng phiên bản này. Các phần chưa nghiệm thu của Stage 3 giữ nguyên trạng thái.
