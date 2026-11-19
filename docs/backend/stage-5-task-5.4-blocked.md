# Task 5.4 — pre-implementation security-design clarification

**HISTORICAL — các blocker đã được giải quyết; không còn là trạng thái hiện hành.**
Quyết định ACL đã được duyệt; regression legacy/current-composition đã được xử lý
theo hợp đồng mới và các kiểm tra local cần thiết PASS. Ngày 24/10/2026, Stage 5
**tạm dừng theo kế hoạch sau khi hoàn thành Task 5.4**, không phải dừng vì blocker
này. Xem [mốc bàn giao hiện hành](stage-5-task-5.4-publisher-acl-and-expiry.md).
Nội dung bên dưới giữ nguyên làm lịch sử; toàn bộ Stage 5 chưa hoàn tất.

Baseline inspected: `develop`, HEAD `bcce3fec29100e9ce2230a3c8ea926202f9faab0`,
with the authorized uncommitted Task 5.3 changes and narrow Gateway shutdown fix.
Read the full external Stage 5 plan, Task 5.0–5.3 handoffs and
`/tmp/opencode/gateway-shutdown/handoff.md`. No inherited file was edited.

## Decision required before implementation

The assignment requires fail-closed ACL/dependency validation while preserving
independent feature flags and data/management principal separation. The current
composition supplies no management client when the credential API is disabled:

- `src/internal/config/credential.go:30–39`: returns before loading management
  identity/configuration when `MQTT_CREDENTIAL_API_ENABLED=false`.
- `src/cmd/server/credential.go:219–222`: disabled credential composition is inert.
- `src/internal/mqttingress/transport.go:683–695`: existing reusable transport uses
  MQTT 3.1.1 (protocol version 4).
- `transport.go:718–737,740–763`: readiness is based on a positive current-lifetime
  subscription acknowledgment, not readback of effective client/role/default ACLs.
- `scripts/mqtt-bootstrap.py:226–248`: backend bootstrap currently grants telemetry
  receive/subscribe/unsubscribe and ACK publish only. Manager has separate control
  permissions. Neither data permission nor absence of broader grants can be
  established merely by authenticating and subscribing to responses.

MQTT 3.1.1 SUBACK/PUBACK is insufficient to prove the required exact ACL policy:
SUBACK says nothing about command-publish authority, receive ACLs or extra grants;
PUBACK is not an exact-ACL readback and is not execution confirmation.

**Question:** May Stage 5 require a separate read-only management-principal ACL
verification path, independent of `MQTT_CREDENTIAL_API_ENABLED`, before enabling
dispatch/readiness? This would load bounded management connection configuration,
read the backend client/roles/default access, reject missing or extra permissions,
and never mutate ACLs at runtime. Commands remain on the data principal. The
explicit operator procedure for updating an existing store remains separate and
must be verified on an owned isolated fixture. If privileged management access in
the Stage 5 runtime is not approved, specify the authoritative ACL evidence and
freshness/lifecycle contract to use instead. Do not substitute an unchecked flag
or treat SUBACK alone as exact-ACL verification.

This changes startup prerequisites and privilege exposure, so it is not silently
chosen here. Stopped as required on substantive unresolved security design.

## Checks actually run / limitations

- `git status --short`, `git log -1 --oneline`: verified baseline/inherited edits.
- `docker info --format '{{.ServerVersion}}'`: successful, daemon version 29.9.2.
- `command -v go`, `command -v golangci-lint`: both available.
- Source inspection only; **no executable RED/GREEN, native fixture, Go suite,
  static analysis, Python guards or Task 5.4 acceptance run**. Earlier-task PASS
  evidence is inherited context, not new verification.
- Inspection mistakes retained: initial glob searched external `docs` instead of
  `main_src/docs` (no matches); corrected glob found all handoffs. Attempted read
  of `src/cmd/server/credentials.go` failed; actual file is `credential.go`, then
  read successfully. Neither is RED or a product failure.
- No production/test/config/ACL/store/credential/volume changes; this handoff is
  the sole file added. No subagents, commits, pushes, remote CI or deployment
  mutations. Existing Gateway scheduler completion delivery against `done` is
  untouched.

## Resume boundaries

After the decision, resume Task 5.4 tests-first, including required isolated native
DB+Mosquitto proof and owned cleanup. Publisher, expiry, ACL update procedure,
configuration and lifecycle remain unimplemented by this task. Task 5.5 is still
separately assigned; no Gateway runtime/SQLite/configuration changes were made.
Task 5.6 result ingestion and Task 5.7 E2E/CI remain out of scope. No full Stage 5
completion claim.
