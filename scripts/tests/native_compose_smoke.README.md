# Native Compose smoke preparation

## Latest phased startup result (supersedes historical preparation status)

`--startup-only` now executes real phased startup, while `execute_native()` and
HTTP acceptance remain closed. Latest evidence:
`/tmp/opencode/startup-envoy-verified-1791860800` (and timestamped run), runner exit 0, phase `nginx`.
PostgreSQL, GoTrue and backend healthy; role provisioner, application migrations
and compatibility migration exit 0; schema19 exact; Mosquitto started; Envoy admin `/ready` probe
verified using `/bin/bash` `/dev/tcp/127.0.0.1/9901` returning `HTTP/1.1 200 OK` (no curl/wget dependency);
Nginx started and `/readyz` probe verified; endpoint mapped loopback port dynamically discovered.
Backend runtime image: `sha256:366a35416707d6135fd01aeef621fd69c9e6f4088e76a983bd8d11811350f8a7`.
Independent exact absence checks confirmed 10 containers (9 stack services + 1 helper), 1 network, and 3 named volumes removed.
Status is `STARTUP_VERIFIED_NOT_HTTP_ACCEPTED`. HTTP acceptance remains closed as the next bounded task.

This slice is **PREPARED_NOT_EXECUTED**. It is not Compose parity evidence and
does not implement a complete native executor. `execute_native()` refuses every
gate value. The CLI only generates files. Three opt-in Python preflight functions
now exist: `dry_effective_config`, `build_native_image`, and
`probe_envoy_toolchain`; none starts the application stack or invokes bootstrap.

## Allocation-only continuation (2026-10-13)

`generate_fixture_tls`, `ResourceRegistry`, `allocate_bootstrap_store`,
`allocation_readback`, and `allocation_roundtrip` implement a separate narrow
slice. They do not enable `execute_native`, start Compose, or prove HTTP.
The historical preparation/next-task sections below describe the earlier slice;
TLS/bootstrap allocation is now implemented but **native acceptance is blocked**.

- TLS: fresh two-day CA/server certificates; SAN `mosquitto`, `localhost`, and
  loopback IP. CA key stays outside the broker certificate directory, mode 0600.
  Server key grants only named UID 1883 read access through filesystem ACL (no
  world access). Backend effective-model allowlist still mounts only `ca.crt`.
- Registry: protected fsync/atomic durable intent before create/CLI invocation;
  task/run/nonce/name/image/labels/kind, then immutable ID/creation/labels. Docker
  volumes have no ID: exact name + creation timestamp + full labels are the
  immutable receipt. Daemon errors never count as absence. Replacements veto
  removal. Lost replies retain recovery authority; they cannot yield success.
- Bootstrap uses the reviewed CLI and protected separate random credentials,
  `--container --config --recovery-dir`, finite execution/grace/join lifecycle,
  and cancellation scopes read-only. No full runner pipeline. Child receipts
  reconcile before exact parent cleanup. Escalated/unjoined child vetoes cleanup
  and keeps inputs. Helpers use network none, UID 1883, readonly mounts, no logs,
  dropped capabilities; own helper now masks image-declared volumes with tmpfs.
- One native attempt only, evidence directory:
  `/tmp/opencode/ncs-allocation-c578f3831ffca952` (private, not uploadable).
  Actual TLS generation passed; reviewed CLI returned `store_created` and
  `NOT_READY`, with a verified bootstrap nonce and pinned cached image.
  **UID-1883 helper readback failed**; stdout was not persisted in this first
  attempt, so the precise failing shell check is unresolved. A regression now
  requires private readback output persistence. No second native attempt made.
- Exact parent cleanup passed: helper
  `a302ea559f20c45be5ab2b8fe64ec28b5e1c7ec6147b002fdd46207a9eadcb8a`
  and volume `ncs_715870ef91b8_store` were revalidated/removed; separate exact
  inspect returned `No such container` / `no such volume`. Receipts remain
  private with creation metadata and removal markers. Bootstrap inputs remain
  protected because the slice failed.
- **Additional blocker:** cached Mosquitto image declares anonymous volumes
  `/mosquitto/data` and `/mosquitto/log`. Reviewed bootstrap helpers do not mask
  these and remove containers without `-v`. Their anonymous-volume IDs are not
  in parent receipts; this slice cannot safely enumerate/delete them or claim
  zero leftover volumes. Fix requires a separately authorized bootstrap boundary
  change. No wildcard cleanup, prune, or foreign-resource access was performed.
- TDD: initial 24 retained tests PASS, five new tests RED then GREEN; cancellation
  and durable-attempt tests raised total to 32. Implicit-volume/private-output
  regression separately failed RED, then implemented (33 tests final).

Only `artifacts/summary.json` is the existing upload allowlist, still
`PREPARED_NOT_EXECUTED`. Receipts, raw metadata, TLS material, logs, store and env
stay under `private/`. No native success summary was published for this failed
attempt. SIGKILL/power loss requires protected offline recovery; no guarantee of
automatic cleanup is claimed.

### Narrow readback and helper-volume fix (subsequent continuation)

The historical failure above is retained, not a statement about the current
implementation. Fresh private captures reproduced exit 1 at `phase=server_key`:
UID/GID were 1883 and stat reported the correct ACL-masked 0750/0640 fixture
metadata. The pinned image's BusyBox shell `test -r` uses mode-bit checks that
do not recognize the named-user POSIX ACL. Actual kernel `open/read` through
`cat ... >/dev/null` succeeds. Readback now uses that real access check without
printing the key, changing ACLs, granting world access, or widening backend
mounts. Store stat still requires exactly 1883:1883 0700/0600.

Production `allocate_helper` now masks both image VOLUME paths for all five
allocations (two native init, ownership, writer, reader) with bounded 1 MiB
tmpfs, noexec/nosuid/nodev, 0700, and each helper's UID/GID. Expectations are
persisted before create. Each helper inspect checks `HostConfig.Tmpfs` and
physical `Mounts`, rejecting any unplanned volume; lost-reply/cancellation
cleanup still uses nonce and immutable ID and authoritative absence, never
`rm -v`. Current smoke reader separately persists and validates mount metadata.

Final bounded roundtrip evidence: `/tmp/opencode/allocation-final-f0ba38057c3b2976`
(private, not uploadable). Real TLS, real CLI `store_created`/`NOT_READY`, and
UID1883 readback all passed, exit 0; all six access/stat/read phase markers
completed. Reader metadata contains the two tmpfs masks and only the exact
named store volume; production helper receipt validation passed every allocation
and subsequent removal. Both parent receipts have `removed=true` after exact
re-inspection and authoritative absence. No anonymous volumes were attributable
to these new masked allocations. Historical unknown anonymous-volume IDs were
not enumerated/deleted; **zero historical leftovers is not claimed**.

TDD: bootstrap prevention RED (one failure, one error), then GREEN; actual
readback regression RED (one failure), then GREEN. All 125 bootstrap, smoke,
and CLI lifecycle unit tests pass (smoke 35, bootstrap 39, lifecycle 51), with
no removals/skips. Complete stack/HTTP/API/CI acceptance remains closed.

Scope blocker for later parent work: `dynsec_bootstrap_cli.py` still has a
separate `readback_and_verify_store` create command without tmpfs masks. The
allocation-only path does not invoke it. Fix that helper and its receipt tests
under separate file authorization before running the full parent pipeline.

Next implement **phased startup and migration health**:
PostgreSQL healthy -> roles zero exit -> real migrations zero exit/schema 19 ->
GoTrue healthy -> compatibility migration zero exit -> backend readiness, with
preallocation receipts and exact cancellation cleanup. HTTP/login acceptance is
a later separate task.

```sh
python3 -m unittest scripts/tests/test_native_compose_smoke.py
python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/new-smoke-fixture
```

Use a new directory outside the CI artifact upload tree. `private/` contains the
generated secrets and runtime configuration; upload **only** the allowlisted
`artifacts/summary.json`. Do not upload logs, Compose JSON, receipts, stores,
certificates, env files, login responses, or Docker inspect output.

## Fixture design

- Root `docker-compose.yml` remains authoritative, with its real migration
  mounts, commands, health checks, and `src/Dockerfile` backend build target.
- The checked-in `.override.yml` is a **template fragment**, not directly usable
  with Compose. Preparation combines it with per-service isolation stanzas.
- Compose >= 2.24.4 is required for `!override`/`!reset`. No older-version fallback.
- Explicit generated `--env-file`, fixed minimal process environment, private
  HOME/DOCKER_CONFIG, and `COMPOSE_DISABLE_ENV_FILE=1` prevent deployment env
  inheritance. Every root interpolation input is covered explicitly; a new
  missing key fails preparation rather than accepting a root default.
- Nine approved services: PostgreSQL, role provisioner, application migrations,
  GoTrue, Auth compatibility migration, Mosquitto, backend, Envoy, Nginx.
  Envoy's Storage startup dependency is removed; existing unused Envoy clusters
  remain untouched. No tunnel, Storage, or host backend binary is started.
- Root Rathole, Storage and Cloudflare services are explicitly removed with
  `!reset null`, rather than hidden by profiles or merely omitted from `up`.
  Top-level volumes/networks use `!override` to exclude legacy/storage resources.
  Newly added services therefore remain visible and are rejected by the gate.
- All approved services reset fixed container names and published ports. Only
  Nginx publishes port `0` on loopback; its actual port must be discovered from
  verified container metadata later. Project names and volume names are unique.
- Bootstrap store is external: Compose cannot create it. The deferred adapter
  must attest a fresh volume, `store_created`, `NOT_READY`, and matching name.
  This is a logical interface, not an assumed CLI report field layout.
- Real TLS files are intentionally not generated yet. A bounded next executor
  must produce CA/server files with SAN `mosquitto`, make them readable by UID
  1883 without exposing keys, and never copy deployment certificates.
- Cleanup functions only return exact commands after verifying immutable
  identities, creation timestamps and ownership labels for every receipt. They
  do not enumerate or delete anything. A later executor must reverify just before
  each removal, enforce deadlines, and fail on cleanup veto/failure. Volumes use
  exact unique name plus creation timestamp because Docker volumes have no ID.

## Smallest next task after bootstrap GREEN and parent approval

Implement and test one bounded native adapter **before enabling a gate**:

1. Check supported Compose version, then render sanitized `config --format json`
   privately. Reject unapproved dependency activation, host binary/deployment
   mounts, fixed names/ports, incorrect build target, missing service labels,
   and changes to real migration commands. The current preparation does not
   claim that the effective merged Compose model has been validated.
2. Generate protected TLS/bootstrap input. Invoke only the approved bootstrap
   CLI, with `--pull=never`, protected stdin/config and its reviewed receipt
   contract. Verify fresh store ownership and NOT_READY before consumption.
3. Build backend and allocate/start services in bounded phases. The current
   returned `up --no-deps` command is an allowlisted plan, **not** sufficient
   orchestration: the adapter must wait for PostgreSQL health, role-provisioner
   zero exit, real application migration zero exit/schema version 19, GoTrue
   health, compatibility migration zero exit, then backend readiness. Record
   immutable receipts immediately on allocation, including partial failures.
4. Create real Auth users through the existing Admin API privately; seed DB
   memberships/admin grant explicitly. Execute `ACCEPTANCE` through Nginx and
   Envoy with password login, not forged human JWTs. Assert correct inventory
   fields (`gateway_id`, `name`, `description`, `created_at`) and sensor fields
   (`gateway_id`, `sensor_id`, `name`, `unit`, `entity_id`, `created_at`).
   Operator/viewer sensor writes return 403; nonmember/cross-Gateway owner and
   admin without owner membership return 404. Verify DB unchanged on denials.
5. Always perform exact receipt-owned cleanup, including bootstrap volume, and
   verify absence within a global deadline. Only then design a separate safe
   native result schema; preparation status must never become native PASS.

Potential runtime blockers still require evidence: Envoy startup without Storage,
UID-1883 mount permissions, upstream image availability, actual migration startup
order, backend readiness, and bootstrap ownership-label adaptation. No native
complete native runtime acceptance is claimed here.

## Sequential preflight evidence (2026-10-13)

- Focused TDD: existing 19 tests passed, new gates failed RED before implementation.
- Real effective JSON: exact nine-service/dependency/mount/resource gate passed.
  Captured JSON remains private and contains secrets; never upload it.
- Real root Compose backend target build passed in a finite 600-second budget,
  without a host binary bind or toolchain substitution. Source commit
  `6ab161d60366bceca5ba5d866795763a416fc9a5` plus uncommitted worktree changes;
  this is not clean-checkout/CI evidence.
- Backend image ID:
  `sha256:0d59e97dceb3e109e218235e1435c83b82e115e7af2485cc8fd390b7d7b3f3cd`.
- Envoy pinned-tag local image ID:
  `sha256:57e14a549d7bd43c8d3f6d03e8cfa653e037d4b38e133acd9b54f38c524401b4`.
  A fresh networkless/read-only/nonroot toolchain probe found `openssl` and `sed`.
  One exact container receipt was revalidated and removed; Docker returned
  authoritative `No such container`. No fixture networks or volumes were allocated.
- Evidence directories (unique; protected inputs are not artifacts):
  `/tmp/opencode/ncs-build-de21a069e0567114` and
  `/tmp/opencode/ncs-envoy-07ae9f55d4dcba67`.

**Not a runtime PASS and no concrete production blocker established.** TLS,
bootstrap receipt adaptation, phased migration/health waits, real Auth login,
API acceptance and full stack cleanup remain unimplemented. Build output/cache
is retained; the backend build image is not a running container. The toolchain
probe is not proof of Envoy routing or startup without Storage. Signal-safe
allocation recovery is not yet provided by this probe; do not use it as a CI
cancellation gate or claim SIGKILL guarantees. Full runtime gate stays disabled.
# Phased startup continuation (HTTP gate remains closed)

## Positive HTTP login and positive slice verification (2026-10-13)

`python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/NEW-DIRECTORY --positive-http-only`
executes phased stack startup through Nginx, followed by the positive-only slice:
1. Provisions platform admin and Gateway owner accounts via GoTrue admin API using `SERVICE_ROLE_KEY` inside private fixture context only.
2. Authenticates both accounts via GoTrue password token grant `/auth/v1/token?grant_type=password` routed through Nginx -> Envoy -> GoTrue.
3. Obtains and checks standard `authenticated` role JWTs without leaking tokens or credential material into stdout, summary artifacts, or logs.
4. Seeds authoritative DB relationships in PostgreSQL: `platform_admins`, `gateways`, `user_gateways` (owner), `twin_entities`, and `twin_states`.
5. Platform admin executes `GET /v1/admin/gateways` (HTTP 200) and asserts presence of the seeded Gateway without credentials or secrets.
6. Gateway owner executes `PUT /v1/gateways/{gateway_id}/sensors/{sensor_id}` (HTTP 201 Created) and re-executes idempotently (HTTP 200 OK).
7. Verifies authoritative DB guarantees: matching `sensors`, `twin_entities`, and `twin_relationships` (`hasSensor`) rows.
8. Reconciles and performs exact receipt cleanup (10 containers, 1 network, 3 named volumes).
Status classification is explicitly `POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED` with `http_accepted: false`. Full RBAC denials and gate remain closed for the next task.

`python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/NEW-DIRECTORY --startup-only`
uses fresh protected inputs and local images only. It builds the reviewed backend
Dockerfile target, starts each dependency in order, requires successful real
migrations and the exact tracked schema through version 19, and waits for health
or readiness before the next phase. Only Nginx publishes a dynamic loopback port.
Storage, tunnels and the old credential controller are not activated.

The only successful startup result is `STARTUP_VERIFIED_NOT_HTTP_ACCEPTED` after
exact receipt cleanup. Preparation/build alone are not startup verification.
HTTP login, authorization and provisioning acceptance remain a separate task.
Each Compose container/network/volume has an exact-name intent written before
allocation; runtime image, labels, mounts and security are verified. Image VOLUME
paths must be covered by named mounts or tmpfs, never anonymous volumes.
Removal uses verified IDs, consumers before infrastructure and volumes. No
`down -v`, prune, historical anonymous-volume enumeration or deployment input is
used. Daemon errors are not absence. Incomplete cleanup retains protected evidence
and vetoes success; SIGKILL cannot execute finally/recovery and requires explicit
receipt-driven recovery by an operator.

Further bounded evidence under `/tmp/opencode/startup-health-final-1791860590`:
PostgreSQL and GoTrue healthy; provisioner, application migrations and compatibility
migration all exit 0; exact schema19 verified. Mosquitto failed before backend
activation because the protected generated broker config lacked UID1883 read
ACL (the generated TLS key ACL already worked). The fixture now grants the
broker config ACL without changing production config. Exact absence independently
verified for 7 containers, 1 network and 3 named volumes. This failed run is not
startup acceptance. No HTTP acceptance or CI job has been enabled.

Native continuation evidence (2026-10-13): parent bootstrap CLI with the remaining
readback masks succeeded, including collision refusal and cleanup. A phased run
reached GoTrue allocation after PostgreSQL health, role provisioner exit 0,
application migrations exit 0 and schema19 exact verification. It stopped on an
image-default-user receipt mismatch (`supabase` vs empty model user), not a
production migration defect. The expectation now derives the image default user;
exact receipt recovery removed the run's resources. This is **not** startup or
HTTP acceptance. Run evidence is protected under
`/tmp/opencode/startup-phased-1791860464`; never publish raw config or logs.

## RBAC/JWT denial acceptance status

The positive native slice remains the only executed HTTP slice. Its successful
result is **not** RBAC/JWT acceptance. Unit coverage now includes full row-image
comparison helpers for denied sensor writes and rejects acceptance summaries
unless every required case is present. The current native runner does not yet
provision and exercise the complete operator/viewer/nonmember/cross-Gateway and
missing/invalid/expired-token matrix; it does not emit an RBAC acceptance report.
Do not use `--positive-http-only` as a substitute or claim `http_accepted: true`.

The policy response contracts were checked against the Go handlers/tests:
same-Gateway non-owner sensor writes are 403 `forbidden` / `access denied`;
nonmember, cross-Gateway owner, and platform-admin-without-membership are 404
`not_found` / `resource not found`; absent/invalid/expired bearer credentials are
401. Exact per-denial database snapshots must include rows, not only counts.
No full native rerun was performed in this continuation; no current RBAC native
case statuses, denial-mutation guarantees, or cleanup counts are claimed. CI
wiring and clean-checkout acceptance remain unverified.

## Bounded role-denial slice

`--role-http-only` performs the phased native startup and the existing positive
login/owner slice, then provisions real GoTrue users for owner, operator, viewer,
nonmember, cross-Gateway owner, and platform admin via the private Admin API.
Each receives a password-grant human token through the fixture's Nginx/Envoy
route; tokens are never recorded. It executes four non-admin inventory GETs
(403), operator/viewer sensor PUTs for both new and existing sensor IDs (403),
and nonmember/cross-Gateway-owner/platform-admin-without-membership PUTs for
both targets (404). Each of the ten denied PUTs has an ordered full-row snapshot
before and after across sensors, twin entities, relationships, and states.
Snapshot inputs use psql bound variables (`-v`), not SQL interpolation.

Example (must be a new path under `/tmp/opencode`):

```sh
python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/NEW-ROLE-RUN --role-http-only
```

Only a completed run with exact owned cleanup emits
`ROLE_HTTP_VERIFIED_NOT_JWT_ACCEPTED` and `http_accepted: false`. Any mismatch
or cleanup failure exits nonzero and retains private evidence. This is not the
missing/invalid/expired JWT matrix, full acceptance, remote CI, or deployment
Compose parity.

Bounded native roundtrip completed at
`/tmp/opencode/role-http-verified-20261013-b830` (private fixture; do not
upload). Real nine-service startup, positive HTTP slice, 14 role-denial
requests, 10 before/after snapshot pairs (20 complete row snapshots), and exact
cleanup all passed. Observed role status counts: inventory 403 x4; sensor PUT
operator 403 x2, viewer 403 x2, nonmember 404 x2, cross-Gateway owner 404 x2,
platform admin without target membership 404 x2. Positive API statuses were
inventory 200, owner sensor create 201, retry 200. Cleanup removed 10 containers,
1 network, and 3 named volumes. Result status is
`ROLE_HTTP_VERIFIED_NOT_JWT_ACCEPTED`; `http_accepted` remained false for that
role-only run. The JWT matrix and full mode below were completed separately.

## JWT denial verification

`--full-http` executes the positive slice, all 14 role denials, and nine
missing/invalid/expired JWT requests. Each JWT condition is tested against the
inventory GET and both new and existing sensor PUT targets. Every denied PUT
compares complete ordered sensor, twin entity, relationship, and state row
images before and after the request. The expired fixture is signed in memory
with the run's private HS256 key and carries authenticated role, audience,
issuer, subject, and algorithm claims; the invalid fixture retains the token
format but has an altered signature. Neither token nor key is written to
artifacts.

Only the full matrix plus exact receipt cleanup (10 containers, 1 network, 3
volumes) can produce `RBAC_JWT_HTTP_ACCEPTED` and `http_accepted: true`. Invoke
with a new private evidence directory, for example:

```sh
python3 scripts/tests/native_compose_smoke.py --prepare /tmp/opencode/NEW-JWT-RUN --full-http
```

The unit suite and syntax checks do not establish native acceptance. The current
continuation has not performed a fresh native run or GitHub Actions execution;
CI wiring and clean-checkout verification remain pending.
