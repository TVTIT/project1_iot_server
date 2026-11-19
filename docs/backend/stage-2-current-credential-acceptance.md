# Current simplified credential acceptance

Run from the repository root:

```sh
sh scripts/test-stage2-mqtt-credentials.sh
```

The entrypoint uses the existing owned `task266_provision` fixture with
`current=True`: pinned TimescaleDB/Mosquitto, direct broker TLS listener, current
source-built `cmd/server`, and pinned Nginx TLS proxy. It never reads deployment
`.env`, mounts a controller socket or opens a gate. Its native broker store reuses
the production bootstrap's exact manager/data-role construction, not manager
permissions on the data principal. All resources are disposable and cleanup is
verified by the existing fixture. The entrypoint rejects missing completion;
there is no optional-skip success path.

## Current deployment contract (explicitly approved)

- Compose owns Mosquitto directly. Broker TLS authentication/ACLs remain required.
- Backend owns credential API authorization, startup readiness, credential work
  and bounded shutdown. It does **not** own a global MQTT ingress gate.
- Backend errors/shutdown need not close the independent broker. API refusal,
  readiness failure and target credential denial are not global network isolation.
- Actual checks: ready startup; real JWT/admin-SQL role denials with no mutation;
  provision, metadata, secret-free replay, rotate/new-password success/old-password
  denial, revoke/password denial; correct CA/SAN versus wrong CA/SAN/password;
  allowed command subscription/delivery and telemetry delivery; cross-namespace
  subscription/publish and control-topic subscription denial (including backend
  data-principal denial); per-request authorization-checker failure; rejected
  manager authentication gives readiness/API 503 without mutation; retained
  revoked metadata on restart; graceful process exit; disabled API without broker
  or credential mounts; no-store headers and DB/process/proxy secret exclusion.

This is ordinary-path plus focused security acceptance, not an exhaustive
failure/crash/persistence matrix or proof of full production readiness. Static
host-built binaries mounted into the pinned image do not establish Dockerfile
multi-stage build parity. Nginx forwarding uses locally signed fixture JWTs with
the real verifier, not a live Supabase login.

## Retained legacy component-only contract

```sh
sh scripts/test-legacy-mqtt-controller.sh
```

This separately invokes the retained `task265b_lifecycle.py` native component
fixture. `BrokerLifecycle` inside `mosquitto-credential-controller` owns its child
and proxy listeners. The authorized test driver exercises private TLS/auth,
receipt/epoch checks, CLOSED/verified OPEN/drain, child failure and restart CLOSED.
It is **not** the current deployment supervisor and gives no backend-fault gate
protection. Component execution is not required for Task 5.4 acceptance and is
not claimed PASS without an actual run.

The original `task2612_standalone.acceptance(current=False)` and other legacy
fixtures remain historical tests, not the default deployment acceptance path.
Their backend-triggered global CLOSED/recovery assertions were intentionally
withdrawn from the current contract; new and old scenarios are not equivalent.
Original failing logs remain under `/tmp/opencode/task54/` and
`/tmp/opencode/task54-gated-investigation/`. CI's credential job now runs the
current entrypoint rather than deferring its execution; no legacy native coverage
artifact or unsupported legacy PASS is claimed.
