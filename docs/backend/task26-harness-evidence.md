# Task 2.6 — Harness hợp nhất và bằng chứng giới hạn

Đây là hồ sơ kiểm chứng **fixture**, không phải production runbook hay nghiệm
thu toàn bộ G1. Báo cáo nghiệm thu triển khai và kết quả rà soát Task 2.6 được ghi tại
[stage-2-task-2.6-acceptance.md](stage-2-task-2.6-acceptance.md). Kiến trúc được chọn nằm trong
[task26-g1-architecture.md](task26-g1-architecture.md); kế hoạch authoritative nằm
ngoài repo tại `../../../docs/backend_plan/task_2.6_detail_plan.md`.

## Chạy lại từ nguồn hiện tại

Yêu cầu Linux, Python 3, OpenSSL, Docker Compose và Go đúng `src/go.mod`.
Không cần binary dựng sẵn, không đọc `.env`, không mount production store.

```sh
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/tests/task26_credential_spike.py --unit
python3 scripts/tests/task26_credential_spike.py --scenario all \
  --artifact-root /tmp/opencode/task26-clean-build
go -C src test -race -count=1 ./internal/mqttcredential ./internal/httpserver \
  ./internal/mosquittoreload ./internal/config
go -C src test -race ../scripts/tests/task260_ingress_gate.go \
  ../scripts/tests/task260_ingress_gate_test.go
go -C src test -race ../scripts/tests/task260_revoke_reconcile_snapshot.go \
  ../scripts/tests/task260_revoke_reconcile_snapshot_test.go
go -C src vet ./...
git diff --check
```

Entrypoint duy nhất hỗ trợ `dynsec`, `revoke`, `startupgate`,
`maintenancerotate`, hoặc `all` (mặc định). Các module `task26_*_cases.py` giữ
assertions độc lập; `task26_harness_helpers.py` chia sẻ framing/correlation/TLS
oracle và builder. Snapshot/gate được build lại từ file Go hiện tại, trong thư
mục fixture mới, `CGO_ENABLED=0`, dưới module `src`; output tồn tại sẵn bị từ chối.
Không dùng cache binary lịch sử. Unit builder chạy thật trong thư mục rỗng,
không skip khi thiếu executable.

Giữ bốn Compose fixture vì topology khác nhau là **đối chứng cần thiết**, không
phải bốn public harness: DynSec capability, ungated PG negative control, PID1
startup gate và host-orchestrated maintenance. Không gộp host orchestration với
PID1 để tránh tuyên bố sai lifetime ownership. Go helper compile/test theo từng
file pair, không compile chung directory có hai `package main`.

Mỗi run có directory mới mode0700, scenario artifacts và `owner.json` riêng.
Registry ghi đúng project/fixture/compose; khi harness bị SIGKILL, lấy ba giá trị
đó để chạy cleanup đã ghi trong registry (chỉ owned project). Sau đó khôi phục
ownership và xóa **đúng fixture đã sinh**. Không global prune. SIGTERM/INT và lỗi
thông thường chạy `finally`. `TASK26_FAIL_AFTER_PREPARE=<scenario>` là failure
seam để kiểm tra cleanup. Transcript được harness tự ghi và scan, không phụ
thuộc một log cũ tồn tại. Artifacts chỉ chứa boolean/metadata/redacted logs,
không full native JSON, plaintext secret, hash hay private key.

## Mapping assertions cũ sang suite mới

| Nhóm lịch sử | Scenario / checks giữ lại |
|---|---|
| DynSec | Offline stdin bootstrap, default deny, no-password/no-role create→disable→password/role→enable, A/B send/receive ACL, backend/control separation, rotate old/new/session kick, lost create/rotate/disable application reply, retry/readback, bounded correlation/concurrency, non-retained replies, wrong CA/hostname, restart persistence, save-failure RAM rollback |
| PG revoke | Rollback/commit/replay, unique job and cross-target identity conflict, new-connection receipt observation, sticky DB revoke, chmod0500 RAM-only warning/pending, old credential accepted after **ungated** restart, operator repair/retry, repeated restart, DB-offline unknown |
| Startup gate | Ungated old-store negative control, lifetime nonce, continuous old CONNECT on two routes, crash before bind, child SIGKILL, controller absent, repeated restart with restored old store, all-revokes replay (including snapshot_observed), save/DB/corrupt-authority CLOSED, management loopback denial from host and same bridge |
| Maintenance | Real PG advisory ownership/competing lease denial, route close/drain EOF, cold new-process/new-secret positive/old negative/B positive, device_updated separate, Gateway control denial, save failure with old internal acceptance/new failure, before-probe/after-verify/after-finalize SIGKILL, finalize DB outage, metadata-only replay, one-time release after confirmed finalization, continuous two-route outcome classification |
| Static witness / filesystem compiler (retired) | Historical counterexamples/source conclusions below, **not rerunnable** with removed experimental source; Task 2.5 production regressions remain untouched |

## Source provenance và phản chứng không được mất

Mosquitto 2.0.18 pinned image digest:
`d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9`.
PG17.6 pinned digest:
`ef257d85f76e48da1c64832459b59fcaba1a4dac97bf5d7450c77753542eee94`.
Source review lịch sử dùng [commit v2.0.18
3923526c6b4c048bbecad2506c4c9963bc46cd36](https://github.com/eclipse-mosquitto/mosquitto/tree/3923526c6b4c048bbecad2506c4c9963bc46cd36),
release tarball SHA256
`d665fe7d0032881b1371a47f34169ee4edab67903b2cd2b4c083822823f4448a`;
image history ghi cùng version/download SHA, reviewed files byte-match release.
Đây **không** là reproducible binary build/GPG/toolchain attestation.

- Static: `src/signals.c:42–48` chỉ đặt flag SIGHUP; `src/loop.c:232–241`
  cleanup/init/apply không kiểm return; `security_default.c:151–200,1047–1069`
  giải phóng table trước init; `743–943` parse incremental/drop malformed entry;
  `src/security.c:755–843` failed init cũng có thể deny. `lib/misc_mosq.c:240–275`
  read failure không có EOF có thể spin. Negative witness không xác định complete
  loaded generation. Phản chứng **original→witness→original** là restore cố ý,
  không spontaneous stale load; witness reject trong khi original accept.
  Missing/unreadable/malformed final và crash mất witness cũng không chứng minh
  final success/rollback. Backend health, disk digest và signal ACK không bổ sung
  target generation receipt. Production không có old plaintext oracle.
- DynSec `plugins/dynamic-security/plugin.c:228–320,745–748`: shared QoS0,
  non-retained correlated responses; `583–640`: save **void**, open/rename chỉ
  log, unchecked fwrite/fclose, không file/directory fsync. `clients.c:474–543`
  disable RAM/kick/save/success; `655–703` password mutation có hash-failure gap;
  `965–1018` getClient chỉ đọc RAM. `324–471` create không đọc disabled field;
  no-password mặc định invalid, không role. `auth.c:155–195` disabled deny;
  invalid password defer chỉ chắc reject trong single-plugin/no-fallback/anonymous
  denied fixture. `acl.c:35–119` không substitute `%u` như static ACL.
  `apps/mosquitto_ctrl/dynsec.c:553–775` offline stdin bootstrap có broad role,
  phải remove role/default allow. Control ACL giới hạn topic, **không từng JSON
  command**; manager vẫn toàn quyền broker, cần app guards khi triển khai.
- Phản chứng thật: `/security`0500, UID1883 → disable SUCCESS, RAM disabled,
  session kicked/fresh denied, log `File is not writable`; SIGKILL/restart lại
  accept old credential. PG intent không tự chặn ungated listener. Snapshot
  boolean là **persisted snapshot observed**, không power-loss guarantee.
- Compiler fail-closed cũ dùng metadata filesystem **TESTDBSEAM**, không PG:
  sync/stage/rename trước exec, unavailable/corrupt authority CLOSED; actual
  wrapper SIGKILL sau stage trước rename đã thử. Happy reprovision writable
  không giải quyết failed password save rồi premature active. Đã thay bằng
  real-PG và physical-gate evidence, không dùng seam để tuyên bố DB correctness.

## Giới hạn chung / không suy rộng

- Lost MQTT reply là discard **sau** correlated response; lost COMMIT receipt là
  new-connection confirmation seam, **không** wire packet-loss/true ambiguous
  in-flight COMMIT. Missing row chỉ absent_observed, không chắc not-performed.
- Startup suite single writer; serializable transaction không atomic với TCP
  bind. Nonce proof thuộc một lifetime; revoke sau snapshot là pending operation
  mới. Maintenance advisory acquisition không chứng minh fencing/lease-loss
  atomic với mutation/OPEN; child death sau OPEN dùng explicit close ACK, không
  production automatic detection. Post-OPEN DB-loss policy còn chưa chốt.
- Two-route CONNECT probes là sampled race evidence, không universal scheduling
  proof; TCP_CLOSED không gọi là auth rejection. Rathole chỉ TCP simulation.
- Host maintenance orchestration không phải PID1 controller; trusted local
  operator nằm ngoài Gateway threat model. PG dùng disposable postgres bootstrap
  role; least-privilege production DB role **NOT RUN**.
- Physical USB/encrypted NVS chưa thử; local0600 file chỉ fixture handoff.
  Broker active không đồng nghĩa device_updated; replay delivery unknown, mất
  secret cần operation rerotate mới, không escrow/old rollback.
- NOT RUN: hostile/concurrent full mutation workflows, post-OPEN partition,
  actual COMMIT wire-loss, all-session/will/retained semantics, mọi crash/IO
  boundary, OOM/EIO/ENOSPC mới, host reboot/power loss, backup restore,
  production topology/bypass audit, actual Rathole/board, full Auth/DB acceptance.
  Strict native parser unit tests không tương đương integration file-race test.
- Baseline CI lịch sử ở HEAD `b856ecfb947d7825dcfcc63e02ae62c5ca514b88`
  [run37110877029](https://github.com/TVTIT/project1_iot_server/actions/runs/37110877029)
  không phải CI cho các untracked spike này. Không coverage ≥80% hoặc full G1 PASS.

## Hồ sơ consolidation

Baseline trước cleanup: 48 discovery tests +6 framing tests PASS; bốn Docker
fixture cũ chạy thật PASS (negative counterexamples đúng kỳ vọng).
Log: `/tmp/opencode/task26-consolidation-baseline.log`.
Builder test RED trước implementation: `/tmp/opencode/task26-build-red.log`.
Suite mới và kết quả sau cleanup ghi tại `/tmp/opencode/task26-consolidation.log`.
Kết quả: 50 discovery tests +6 framing tests PASS; all four Docker scenarios
PASS từ build directory mới, helper snapshot/gate build tự động; selected Go
race (bốn package và hai helper file pairs), vet toàn `src`/helpers, Python
compile, gofmt và tracked/untracked whitespace checks PASS. Failure seam revoke
exit1 đúng kỳ vọng và owned cleanup PASS tại
`/tmp/opencode/task26-consolidation-failure.log`. Lint/type checker riêng và
coverage threshold không chạy; không dùng các check này để claim production.
Các số sample/nonce/project phụ thuộc từng run, không sao chép thành guarantee.
Sáu narrative report cũ được thay bằng tài liệu này và bản architecture ngắn;
không giữ command obsolete như runbook. Chỉ cleanup test/docs, không sửa Task2.5
production harness, `.env`, API/runtime, migrations, deploy hay credential thật.
