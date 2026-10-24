#!/usr/bin/env python3
"""Isolated Compose preflight and startup-only verification; HTTP gate closed.

Read-only effective-model inspection, bounded real Dockerfile build, and an
exact-owned networkless Envoy toolchain probe are available as Python functions.
The CLI optionally verifies phased startup. No deployment env is read.
"""
from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import http.client
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import ssl
import subprocess
import sys
import time
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
ROOT_COMPOSE = ROOT / "docker-compose.yml"
TEMPLATE = Path(__file__).with_suffix(".override.yml")
PAPER_P0_TEMPLATE = Path(__file__).with_name("compose.paper-p0.override.yml")
SERVICES = ("postgres", "supabase-role-provisioner", "application-migrations",
            "supabase-auth", "supabase-compat-migration", "mosquitto",
            "backend", "supabase-envoy", "nginx")
# Terminal state of the currently supported schema. The fixture records every
# migration under migrations/ (versions 4 and 5 do not write history rows) and
# must reach exactly this version:name pair last. Pinning the terminal instead
# of accepting any suffix keeps a missing, extra, renamed, or wrong-version
# history failing closed; a newly supported migration must update this pin
# explicitly rather than being silently accepted.
EXPECTED_SCHEMA_TERMINAL = "23:sensor_control_intent"


def expected_schema_history(directory=None):
    """Exact ``version:name`` history the fixture must record, version-ordered.

    Derived from the migration files so a supported migration is reflected
    automatically; the pinned terminal in :data:`EXPECTED_SCHEMA_TERMINAL` is
    enforced separately by :func:`schema_history_verified`.
    """
    migrations = ROOT / "migrations" if directory is None else Path(directory)
    return ",".join(
        str(int(p.name.split("_")[0])) + ":" + p.name.split("_", 1)[1].removesuffix(".up.sql")
        for p in sorted(migrations.glob("0*.up.sql"))
        if int(p.name.split("_")[0]) not in (4, 5)
    )


def schema_history_verified(returncode, stdout, expected, terminal=EXPECTED_SCHEMA_TERMINAL):
    """True only when the recorded history is exact and reaches the pinned terminal.

    A nonzero returncode, a history that differs from ``expected`` (missing,
    extra, or renamed rows), or an ``expected`` that does not end at the pinned
    terminal are all rejected.
    """
    return returncode == 0 and stdout == expected and expected.endswith(terminal)


BUDGETS = {"config": 30, "build": 600, "up": 180, "bootstrap": 120,
           "inspect": 15, "cleanup": 60, "http": 10, "acceptance": 180}
# Explicit reviewed values, including inputs from inactive root services. Never
# inherit shell values or silently accept a newly added root interpolation key.
DEFAULTS = dict(
    API_DOMAIN="localhost", POSTGRES_USER="postgres", POSTGRES_DB="smoke",
    SUPABASE_PUBLIC_URL="http://localhost", DASHBOARD_USERNAME="fixture",
    GOTRUE_SITE_URL="http://localhost", GOTRUE_DISABLE_SIGNUP="true",
    SUPABASE_JWT_AUDIENCE="authenticated", GOTRUE_JWT_ISSUER="http://localhost/auth/v1",
    JWT_EXPIRY="3600", GLOBAL_S3_BUCKET="fixture-unused", SERVER_ENV="production",
    CORS_ALLOWED_ORIGINS="", DATABASE_MAX_CONNS="5", DATABASE_MIN_CONNS="1",
    DATABASE_CONNECT_TIMEOUT="10s", READINESS_TIMEOUT="2s", AUTHORIZATION_TIMEOUT="2s",
    ADMIN_MAX_BODY_BYTES="16384", HTTP_READ_HEADER_TIMEOUT="5s", HTTP_READ_TIMEOUT="15s",
    HTTP_WRITE_TIMEOUT="55s", HTTP_IDLE_TIMEOUT="60s", SHUTDOWN_TIMEOUT="55s",
    MQTT_BROKER_URL="ssl://mosquitto:8883", MQTT_CLIENT_ID="smoke_backend",
    MQTT_USERNAME="backend_service", MQTT_QUEUE_CAPACITY="256", MQTT_WORKER_COUNT="4",
    MQTT_MAX_PAYLOAD_BYTES="1048576", MQTT_CREDENTIAL_API_ENABLED="true",
    INGRESS_GATE_ENABLED="true", MQTT_TLS_CA_FILE="/mosquitto/config/certs/ca.crt",
    MQTT_INTERNAL_MANAGEMENT_URL="ssl://mosquitto:8883",
    # Task 3.4 telemetry ingress keys referenced by the root compose. They are
    # safe local defaults mirroring the compose `:-` fallbacks and .env.example;
    # require_inputs fails closed unless every root interpolation key is explicit.
    MQTT_INGRESS_ENABLED="false", MQTT_CONNECT_TIMEOUT="5s",
    MQTT_SUBSCRIBE_TIMEOUT="3s", MQTT_PUBLISH_TIMEOUT="3s",
    MQTT_RECONNECT_MAX_INTERVAL="30s", TELEMETRY_MAX_SAMPLES_PER_MESSAGE="1000",
    TELEMETRY_MAX_BATCH_DURATION="60s", TELEMETRY_MAX_FUTURE_SKEW="2m",
    TELEMETRY_DB_TIMEOUT="5s",
    MQTT_DYNSEC_MANAGER_USERNAME="credential_manager",
    MQTT_DYNSEC_JSON_PATH="/mosquitto/dynsec/dynamic-security.json",
    MQTT_DYNSEC_WRITER_UID="1883",
    MQTT_DYNSEC_IMAGE_REFERENCE="eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9",
    MQTT_DYNSEC_IMAGE_CONFIG_ID="sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4",
    MQTT_DYNSEC_PLATFORM="linux/amd64", MQTT_DYNSEC_TIMEOUT="3s",
    MQTT_DYNSEC_QUEUE_SIZE="16", MQTT_DYNSEC_MAX_INFLIGHT="1",
    MQTT_DYNSEC_MAX_PAYLOAD_BYTES="65536", MQTT_DYNSEC_MAX_SNAPSHOT_BYTES="1048576",
    MQTT_CREDENTIAL_DB_TIMEOUT="2s", MQTT_CREDENTIAL_FINALIZE_TIMEOUT="5s",
    MQTT_CREDENTIAL_RECONCILE_TIMEOUT="2m", MQTT_CREDENTIAL_RECONCILE_BATCH_SIZE="64",
    MQTT_CREDENTIAL_RECONCILE_MAX_PAGES="16", MQTT_CREDENTIAL_OPERATION_TIMEOUT="10s",
    MQTT_CREDENTIAL_REQUEST_TIMEOUT="40s", MQTT_CREDENTIAL_RECOVERY_TIMEOUT="5s",
    # Stage 4 Read Bounds
    HISTORY_QUERY_TIMEOUT="5s", HISTORY_MAX_RANGE="744h", HISTORY_RAW_WINDOW="10s",
    HISTORY_DEFAULT_POINTS="1000", HISTORY_MAX_POINTS="2000", HISTORY_MIN_POINTS="100",
    TWIN_LIST_DEFAULT_LIMIT="50", TWIN_LIST_MAX_LIMIT="200",
    STAGE5_ENABLED="false", STAGE5_REQUEST_TIMEOUT="5s", STAGE5_COMMAND_TTL="60s",
    STAGE5_MAX_PENDING_OUTBOX="10000", STAGE5_MAX_RECEIPTS="100000",
    STAGE5_MQTT_CLIENT_ID="backend_control", STAGE5_PUBLISH_WORKERS="1",
    STAGE5_SCAN_BATCH="32", STAGE5_RESULT_QUEUE="128", STAGE5_PAYLOAD_BYTES="8192",
    STAGE5_SCAN_INTERVAL="1s", STAGE5_EXPIRE_INTERVAL="1s", STAGE5_RETRY_BACKOFF="1s",
    STAGE5_PUBLISH_TIMEOUT="3s", STAGE5_ACL_TIMEOUT="5s", STAGE5_CONNECT_TIMEOUT="5s",
    STAGE5_SUBSCRIBE_TIMEOUT="3s", STAGE5_RECONNECT_MAX_INTERVAL="30s",
    TWIN_RELATIONSHIP_DEFAULT_LIMIT="50", TWIN_RELATIONSHIP_MAX_LIMIT="200",
    SUPABASE_JWT_CLOCK_SKEW="30s", CLOUDFLARE_TUNNEL_TOKEN="fixture-unused-not-a-token",
    SUPABASE_JWT_MODE="hs256", SUPABASE_JWKS_URL="", SUPABASE_JWKS_CACHE_TTL="60s",
    AUTH_UNIX_SOCKET_PATH="",
    SUPABASE_JWKS_HTTP_TIMEOUT="2s", SUPABASE_JWKS_REFRESH_COOLDOWN="5s",
    # User Management & Auth Admin bounds
    AUTH_ADMIN_BASE_URL="http://supabase-envoy:8000",
    AUTH_ADMIN_TIMEOUT="10s", AUTH_ADMIN_MAX_BODY_BYTES="16384",
    USER_LIST_MAX_USERS="1000", USER_LIST_MAX_MEMBERSHIPS="10000",
    USER_LIST_MAX_RESPONSE_BYTES="2097152", SUPABASE_SERVICE_ROLE_KEY="")


@dataclass(frozen=True)
class Fixture:
    base: Path
    private: Path
    artifacts: Path
    env_file: Path
    override: Path
    project: str
    values: dict[str, str]
    artifacts_dev: int = 0
    artifacts_ino: int = 0
    paper_p0: bool = False
    paper_p1: str | None = None
    paper_p2_spike: bool = False
    paper_p2: bool = False


def fixture_services(fixture):
    if fixture.paper_p2:
        return ('postgres', 'supabase-role-provisioner', 'application-migrations', 'supabase-auth',
                'supabase-compat-migration', 'mosquitto', 'supabase-storage', 'supabase-envoy',
                'auth-socket-init', 'backend', 'nginx')
    if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2_spike:
        return SERVICES[:-2] + ('supabase-storage',) + SERVICES[-2:]
    return SERVICES


def private_write(path, text):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(text)


def durable_json(path, data):
    """Protected atomic receipts; directory fsync precedes allocation."""
    tmp = path.with_name(path.name + "." + secrets.token_hex(8))
    private_write(tmp, json.dumps(data))
    with tmp.open("rb") as stream:
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def generate_fixture_tls(fixture):
    """Fresh two-day CA/server; only server key is UID-1883 readable via ACL."""
    certs = fixture.private / "certs"
    ca_key = fixture.private / "ca.key"
    server_key = certs / "server.key"
    for path in (ca_key, server_key):
        private_write(path, "")
    def run(args):
        try:
            result = subprocess.run(args, capture_output=True, timeout=15,
                                    env=process_env(fixture), check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise ValueError("fixture_tls_failed") from None
        if result.returncode:
            raise ValueError("fixture_tls_failed")
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", "/CN=Native smoke fixture CA", "-keyout", str(ca_key), "-out", str(certs / "ca.crt"),
         "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign"])
    run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=mosquitto",
         "-keyout", str(server_key), "-out", str(fixture.private / "server.csr")])
    ext = fixture.private / "tls.ext"
    private_write(ext, "subjectAltName=DNS:mosquitto,DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    run(["openssl", "x509", "-req", "-in", str(fixture.private / "server.csr"), "-CA", str(certs / "ca.crt"),
         "-CAkey", str(ca_key), "-set_serial", str(secrets.randbits(128)), "-days", "2",
         "-extfile", str(ext), "-out", str(certs / "server.crt")])
    os.chmod(server_key, 0o600)
    run(["setfacl", "-m", "u:1883:r--", str(server_key)])
    run(["setfacl", "-m", "u:1883:r-x", str(certs)])
    run(["setfacl", "-m", "u:1883:r--", str(fixture.private / 'mosquitto.conf')])
    for path in (certs / "ca.crt", certs / "server.crt"):
        os.chmod(path, 0o644)
    run(["openssl", "verify", "-CAfile", str(certs / "ca.crt"), str(certs / "server.crt")])
    return True


def canonicalize_helper_mounts(target):
    if type(target) is not dict:
        raise ValueError("helper_mount_receipt_invalid")
    if "mounts" in target or "tmpfs" in target:
        if "mounts" not in target or "tmpfs" not in target:
            raise ValueError("helper_mount_receipt_invalid")
        raw_mounts = target["mounts"]
        raw_tmpfs = target["tmpfs"]
    elif "Mounts" in target or "HostConfig" in target:
        if "Mounts" not in target or "HostConfig" not in target:
            raise ValueError("helper_mount_receipt_invalid")
        raw_mounts = target["Mounts"]
        host_config = target["HostConfig"]
        if type(host_config) is not dict or "Tmpfs" not in host_config:
            raise ValueError("helper_mount_receipt_invalid")
        raw_tmpfs = host_config["Tmpfs"]
    else:
        raise ValueError("helper_mount_receipt_invalid")

    if type(raw_mounts) is not list:
        raise ValueError("helper_mount_receipt_invalid")
    if type(raw_tmpfs) is not dict:
        raise ValueError("helper_mount_receipt_invalid")

    seen_destinations = set()
    normalized_mounts = []
    for m in raw_mounts:
        if type(m) is not dict:
            raise ValueError("helper_mount_receipt_invalid")
        dest = m.get("Destination")
        if type(dest) is not str or not dest:
            raise ValueError("helper_mount_receipt_invalid")
        rw = m.get("RW")
        if type(rw) is not bool:
            raise ValueError("helper_mount_receipt_invalid")
        if dest in seen_destinations:
            raise ValueError("helper_mount_receipt_invalid")
        seen_destinations.add(dest)
        normalized_mounts.append(dict(m))

    for k, v in raw_tmpfs.items():
        if type(k) is not str or type(v) is not str:
            raise ValueError("helper_mount_receipt_invalid")

    sorted_mounts = sorted(normalized_mounts, key=lambda x: x["Destination"])
    return {"mounts": sorted_mounts, "tmpfs": dict(raw_tmpfs)}


def check_helper_mount_receipt(stored_receipt, current_obj):
    expected = canonicalize_helper_mounts(stored_receipt)
    current = canonicalize_helper_mounts(current_obj)
    if current == expected:
        return
    expected_mounts = expected["mounts"]
    current_mounts = current["mounts"]
    if len(current_mounts) != len(expected_mounts):
        raise ValueError("helper_mount_count_mismatch")
    if [m["Destination"] for m in current_mounts] != [m["Destination"] for m in expected_mounts]:
        raise ValueError("helper_mount_destinations_mismatch")
    if current_mounts != expected_mounts:
        raise ValueError("helper_mount_attributes_mismatch")
    if current["tmpfs"] != expected["tmpfs"]:
        raise ValueError("helper_tmpfs_mismatch")
    raise ValueError("helper_mount_replacement_refused")


class ResourceRegistry:
    """Exact-name intent before allocation; immutable receipt before use/delete."""
    def __init__(self, fixture, runner=subprocess.run):
        self.fixture, self.runner = fixture, runner
        self.items = []

    def command(self, args, input=None):
        try:
            return self.runner(["docker", *args], input=input, capture_output=True, timeout=BUDGETS['cleanup'] if 'rm' in args else 10,
                               env=process_env(self.fixture), check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise ValueError("resource_inspect_unresolved") from None

    def save(self, item):
        durable_json(item["path"], {k: v for k, v in item.items() if k != "path"})

    def intent(self, kind, name, image, labels):
        if kind not in {"container", "volume", "network"} or not name.startswith(self.fixture.project + "_") and not name.startswith(self.fixture.project + "-"):
            raise ValueError("resource_intent_rejected")
        item = {"kind": kind, "name": name, "image": image, "labels": dict(labels),
                "task": "native-compose-allocation", "run": self.fixture.project,
                "nonce": secrets.token_hex(16), "receipt": None, "allocation_attempted": False,
                "path": self.fixture.private / ("resource-" + secrets.token_hex(8) + ".json")}
        self.save(item)
        self.items.append(item)
        return item

    def resolve(self, item):
        result = self.command([item["kind"], "inspect", item["name"]])
        if result.returncode:
            if (("no such " + item["kind"]).encode() in result.stderr.lower()
                    or item['kind'] == 'network' and ('network ' + item['name'] + ' not found').encode() in result.stderr.lower()):
                return None
            raise ValueError("resource_inspect_unresolved")
        try:
            rows = json.loads(result.stdout)
            if len(rows) != 1 or not isinstance(rows[0], dict):
                raise ValueError()
            obj = rows[0]
            if item["kind"] == "container":
                labels = obj.get("Config", {}).get("Labels") or {}
                identity, created = obj["Id"], obj["Created"]
                valid = (obj["Name"] == "/" + item["name"] and obj["Image"] == item["image"]
                         and re.fullmatch(r"[a-f0-9]{64}", identity))
            elif item["kind"] == "volume":
                labels = obj.get("Labels") or {}
                identity, created = obj["Name"], obj["CreatedAt"]
                valid = identity == item["name"] and obj.get("Driver") == "local" and not obj.get("Options")
            else:
                labels = obj.get("Labels") or {}
                identity, created = obj["Id"], obj["Created"]
                valid = obj["Name"] == item["name"] and obj.get("Driver") == "bridge" and not obj.get("Internal")
            if not valid or not created or any(labels.get(k) != v for k, v in item["labels"].items()):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise ValueError("resource_ownership_refused") from None
        rec = {"id": identity, "created": created, "labels": labels}
        if item.get("helper_mount_receipt") is not None:
            check_helper_mount_receipt(item["helper_mount_receipt"], obj)
        if item["receipt"] is not None and item["receipt"] != rec:
            raise ValueError("resource_replacement_refused")
        item["receipt"] = rec
        if item.get("expectation") is not None:
            verify_runtime_receipt(obj, item["expectation"])
        self.save(item)
        return obj

    def remove(self, item):
        if self.resolve(item) is None:
            return
        result = self.command([item["kind"], "rm", *(["--force"] if item["kind"] == "container" else []), item["receipt"]["id"]])
        if result.returncode or self.resolve(item) is not None:
            raise ValueError("resource_cleanup_unresolved")
        item["removed"] = True
        self.save(item)

    def allocating(self, item):
        item["allocation_attempted"] = True
        self.save(item)


def bootstrap_lifecycle():
    # Read-only reuse: never call run_pipeline, which deletes its success volume.
    spec = importlib.util.spec_from_file_location("ncs_bootstrap_lifecycle", Path(__file__).with_name("dynsec_bootstrap_cli.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def allocate_bootstrap_store(fixture, registry, lifecycle):
    image = lifecycle.verify_pinned_image()
    name = fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"]
    labels = {"test.task": "native-compose-allocation", "test.run": fixture.project,
              "bootstrap.managed": "true", "com.docker.compose.volume": "mosquitto_dynsec"}
    item = registry.intent("volume", name, image, labels)
    lifecycle.check_volume_absent(name)
    config = fixture.private / "bootstrap.json"
    private_write(config, json.dumps({"volume_name": name,
        "manager": fixture.values["MQTT_DYNSEC_MANAGER_USERNAME"], "backend": fixture.values["MQTT_USERNAME"],
        "manager_password": fixture.values["MQTT_DYNSEC_MANAGER_PASSWORD"], "backend_password": fixture.values["MQTT_PASSWORD"],
        "labels": {"test.task": labels["test.task"], "test.run": labels["test.run"]}}))
    registry.allocating(item)
    output = lifecycle.execute_bootstrap(config, timeout=45, secrets_to_scan=(
        fixture.values["MQTT_PASSWORD"], fixture.values["MQTT_DYNSEC_MANAGER_PASSWORD"]))
    if output.get("status") != "store_created" or output.get("volume_name") != name or output.get("runtime_status") != "NOT_READY":
        raise ValueError("bootstrap_contract_rejected")
    obj = registry.resolve(item)
    if obj is None or not re.fullmatch(r"[a-f0-9]{32}", (obj.get("Labels") or {}).get("bootstrap.nonce", "")):
        raise ValueError("bootstrap_nonce_rejected")
    item["labels"]["bootstrap.nonce"] = obj["Labels"]["bootstrap.nonce"]
    registry.save(item)
    verify_bootstrap_result(fixture, {"volume": name, "fresh_volume": True, "store_created": True, "readiness": "NOT_READY"})
    return item


def allocation_readback(fixture, registry, volume):
    """One read-only UID-1883 helper: TLS access and store metadata, not broker."""
    labels = {"test.task": "native-compose-allocation", "test.run": fixture.project,
              "allocation.nonce": secrets.token_hex(16)}
    item = registry.intent("container", fixture.project + "-reader", volume["image"], labels)
    if registry.resolve(item) is not None:
        raise ValueError("helper_collision")
    command = ["create", "--pull=never", "--name", item["name"], "--network", "none", "--read-only",
               "--user", "1883:1883", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--log-driver", "none",
               "--tmpfs", "/mosquitto/data:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883", "--tmpfs", "/mosquitto/log:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883"]
    for key, value in labels.items():
        command += ["--label", key + "=" + value]
    command += ["--mount", "type=volume,src=" + volume["name"] + ",dst=/store,readonly",
                "--mount", "type=bind,src=" + str(fixture.private / "certs") + ",dst=/certs,readonly",
                "--entrypoint", "/bin/sh", item["image"], "-ec",
                 'echo phase=server_key >&2; id >&2; stat -c "%u:%g:%a" /certs /certs/server.key >&2; cat /certs/server.key >/dev/null; echo phase=server_crt >&2; cat /certs/server.crt >/dev/null; echo phase=ca_crt >&2; cat /certs/ca.crt >/dev/null; echo phase=ca_key_absent >&2; test ! -e /certs/ca.key; echo phase=store_stat >&2; stat -c "%u:%g:%a" /store /store/dynamic-security.json; echo phase=store_read >&2; cat /store/dynamic-security.json']
    registry.allocating(item)
    result = registry.command(command)
    cid = result.stdout.decode().strip()
    if result.returncode or not re.fullmatch(r"[a-f0-9]{64}", cid):
        raise ValueError("helper_allocation_unresolved")
    obj = registry.resolve(item)
    if obj is None or obj["Id"] != cid:
        raise ValueError("helper_receipt_rejected")
    verify_readback_mounts(fixture, volume, obj)
    item["helper_mount_receipt"] = canonicalize_helper_mounts(obj)
    registry.save(item)
    private_write(fixture.private / "readback-mounts.json", json.dumps({
        "id": cid, "mounts": obj["Mounts"], "tmpfs": obj["HostConfig"]["Tmpfs"]}))
    result = registry.command(["start", "--attach", cid])
    private_write(fixture.private / "readback-output.json", json.dumps({
        "exit": result.returncode, "stdout": result.stdout.decode(errors="replace"),
        "stderr": result.stderr.decode(errors="replace")}))
    obj = registry.resolve(item)
    if result.returncode or obj["State"]["Running"] or obj["State"]["ExitCode"] != 0:
        raise ValueError("allocation_readback_failed")
    lines = result.stdout.splitlines()
    if lines[:2] != [b"1883:1883:700", b"1883:1883:600"]:
        raise ValueError("store_permissions_rejected")
    store = json.loads(b"\n".join(lines[2:]))
    if (set(c["username"] for c in store["clients"]) != {fixture.values["MQTT_USERNAME"], fixture.values["MQTT_DYNSEC_MANAGER_USERNAME"]}
            or len(store["clients"]) != 2 or any(v is not False for v in store["defaultACLAccess"].values())):
        raise ValueError("store_metadata_rejected")
    private_write(fixture.private / "store-readback.json", json.dumps(store))
    return True


def verify_readback_mounts(fixture, volume, obj):
    expected = {p: 'rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883'
                for p in ('/mosquitto/data', '/mosquitto/log')}
    mounts = obj.get("Mounts", [])
    physical = {m.get("Destination"): (m.get("Type"), m.get("Name") if m.get("Type") == "volume" else m.get("Source"), m.get("RW"))
                for m in mounts if m.get("Type") != "tmpfs"}
    if (obj.get("HostConfig", {}).get("Tmpfs") != expected
            or physical != {"/store": ("volume", volume["name"], False),
                            "/certs": ("bind", str(fixture.private / "certs"), False)}
            or len(mounts) != len({m.get("Destination") for m in mounts})
            or any(m.get("Type") == "tmpfs" and m.get("Destination") not in expected for m in mounts)):
        raise ValueError("readback_mounts_rejected")
    return True


def allocation_roundtrip(fixture):
    """Narrow native proof only; signal restoration and exact cleanup gate success."""
    lifecycle = bootstrap_lifecycle()
    registry = ResourceRegistry(fixture)
    failure = None
    with lifecycle.cancellation_scope() as state:
        try:
            generate_fixture_tls(fixture)
            volume = allocate_bootstrap_store(fixture, registry, lifecycle)
            state.checkpoint()
            with lifecycle.defer_cancellation():
                allocation_readback(fixture, registry, volume)
            state.checkpoint()
        except BaseException as exc:
            failure = exc
        finally:
            state.unwinding = True
            if isinstance(failure, lifecycle.IncompleteCleanup):
                raise ValueError("allocation_child_unresolved_evidence_retained") from None
            if registry.items:
                volume = registry.items[0]
                # Reconcile child termination receipts before touching its inputs.
                try:
                    lifecycle.reconcile_child_receipts(fixture.private, volume["name"], volume["labels"])
                    deadline = time.monotonic() + BUDGETS["cleanup"]
                    for item in reversed(registry.items):
                        if time.monotonic() >= deadline:
                            raise ValueError("allocation_cleanup_deadline")
                        if item["allocation_attempted"]:
                            registry.remove(item)
                except Exception:
                    raise ValueError("allocation_cleanup_unresolved_evidence_retained") from None
            if failure is not None:
                raise failure
            if state.signum is not None:
                raise lifecycle.RunnerInterrupted(state.signum)
    config = fixture.private / "bootstrap.json"
    config.unlink()
    return {"slice_status": "ALLOCATION_TLS_BOOTSTRAP_VERIFIED", "full_smoke_status": "PREPARED_NOT_EXECUTED",
            "runtime_status": "NOT_READY", "exact_cleanup_verified": True, "stack_started": False}


def require_inputs(text, values):
    required = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", text))
    if required - values.keys():
        raise ValueError("missing_interpolation")
    for key in required:
        if re.search(r"\$\{" + key + r"\:\?", text) and not values[key]:
            raise ValueError("missing_interpolation")


def jwt(secret, role):
    def encode(data):
        return base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode()).decode().rstrip("=")
    data = encode({"alg": "HS256", "typ": "JWT"}) + "." + encode({
        "role": role, "iss": DEFAULTS["GOTRUE_JWT_ISSUER"], "iat": int(time.time()),
        "exp": int(time.time()) + 3600})
    signature = base64.urlsafe_b64encode(hmac.new(secret.encode(), data.encode(), hashlib.sha256).digest()).decode().rstrip("=")
    return data + "." + signature


def paper_p2_support():
    spec = importlib.util.spec_from_file_location('paper_p2_support', Path(__file__).with_name('paper_p2_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def prepare(base, paper_p0=False, paper_p1=None, metrics_enabled=True, paper_p2_spike=False, paper_p2=False):
    if paper_p2 and (paper_p0 or paper_p1 or paper_p2_spike):
        raise ValueError('paper_p2_mode_rejected')
    if paper_p2_spike and (paper_p0 or paper_p1):
        raise ValueError('paper_p2_mode_rejected')
    if paper_p1 not in (None, 'baseline', 'optimized') or paper_p0 and paper_p1:
        raise ValueError('paper_p1_mode_rejected')
    paper_profile = paper_p0 or paper_p1 is not None or paper_p2_spike or paper_p2
    # Reject ambiguous env-file syntax and symlinked parents before writing.
    base = Path(base).absolute()
    if any(p.is_symlink() for p in (base, *base.parents)) or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(base)):
        raise ValueError("unsafe_fixture_path")
    base.mkdir(mode=0o700)
    private, artifacts = base / "private", base / "artifacts"
    for path in (private, artifacts, private / "certs", private / "home", private / "docker"):
        path.mkdir(mode=0o700)
    project = "ncs_" + secrets.token_hex(6)
    values = dict(DEFAULTS)
    if paper_profile:
        values.update(MQTT_INGRESS_ENABLED='true', DATABASE_MAX_CONNS='10',
                      MQTT_MAX_PAYLOAD_BYTES='65536')
    values['METRICS_ENABLED'] = 'true' if (paper_p1 or paper_p2) and metrics_enabled else 'false'
    if paper_p2:
        values.update(SUPABASE_JWT_MODE='rs256', SUPABASE_JWKS_URL='http://supabase-envoy:8000/auth/v1/.well-known/jwks.json',
                      AUTH_UNIX_SOCKET_PATH='/run/iot-auth/auth.sock', GOTRUE_JWT_ISSUER='https://localhost/auth/v1',
                      SUPABASE_PUBLIC_URL='https://localhost', GOTRUE_SITE_URL='https://localhost',
                      CORS_ALLOWED_ORIGINS='https://client.example.test')
    for key in ("POSTGRES_PASSWORD", "AUTH_DB_PASSWORD", "STORAGE_DB_PASSWORD",
                "BACKEND_DB_PASSWORD", "JWT_SECRET", "MQTT_PASSWORD",
                "MQTT_DYNSEC_MANAGER_PASSWORD", "DASHBOARD_PASSWORD"):
        values[key] = secrets.token_hex(32)
    values.update(
        ANON_KEY=jwt(values["JWT_SECRET"], "anon"),
        SERVICE_ROLE_KEY=jwt(values["JWT_SECRET"], "service_role"),
        DATABASE_URL="postgres://iot_backend_app:" + values["BACKEND_DB_PASSWORD"] + "@postgres:5432/smoke?sslmode=disable",
        MQTT_CLIENT_ID=project + "_backend", MQTT_DYNSEC_BOOTSTRAP_VOLUME=project + "_store",
        MQTT_TLS_CA_SOURCE=str(private / "certs" / "ca.crt"), NCS_RUN=project,
        NCS_BROKER_CONFIG=str(private / "mosquitto.conf"), NCS_CERT_DIR=str(private / "certs"))
    template = TEMPLATE.read_text()
    require_inputs(ROOT_COMPOSE.read_text() + template, values)
    env_file, override = private / "fixture.env", private / "compose.yml"
    private_write(env_file, "".join(f"{key}={value}\n" for key, value in sorted(values.items())))
    # Each generated stanza has a unique service key. Merge template properties
    # under the same stanza rather than emitting duplicate YAML mappings.
    resource_offset = template.index("\nvolumes: !override\n") + 1
    extras = template[template.index("  nginx:"):resource_offset]
    blocks = re.split(r"(?m)^  ([a-z][a-z-]+):\n", extras)
    extra_by_service = dict(zip(blocks[1::2], blocks[2::2]))
    stanzas = ["services:\n  rathole: !reset null\n  cloudflared: !reset null\n"]
    if not paper_profile:
        stanzas.append("  supabase-storage: !reset null\n")
    services = SERVICES[:-2] + ('supabase-storage',) + SERVICES[-2:] if paper_profile else SERVICES
    for service in services:
        stanzas.append(f"  {service}:\n    container_name: !reset null\n    restart: 'no'\n")
        if service != "nginx":
            stanzas.append("    ports: !override []\n")
        stanzas.append(f"    labels:\n      io.iot.native-smoke.run: ${{NCS_RUN:?required}}\n      io.iot.native-smoke.role: {service}\n      test.task: native-compose-startup\n      test.run: ${{NCS_RUN:?required}}\n")
        stanzas.append(extra_by_service.get(service, ""))
        if service == 'backend' and paper_p1:
            stanzas.append('    environment:\n      METRICS_ENABLED: ${METRICS_ENABLED:?required}\n')
    private_write(override, "".join(stanzas) + template[resource_offset:])
    # Copy only the reviewed broker config, never deployment certificates.
    private_write(private / "mosquitto.conf", (ROOT / "config/mosquitto/mosquitto.conf").read_text())
    art_stat = artifacts.stat()
    fixture = Fixture(base, private, artifacts, env_file, override, project, values,
                      art_stat.st_dev, art_stat.st_ino, paper_p0, paper_p1, paper_p2_spike, paper_p2)
    if paper_p2_spike or paper_p2:
        paper_p2_support().prepare_spike(sys.modules[__name__], fixture)
    if paper_p2:
        paper_p2_support().prepare_perimeter(sys.modules[__name__], fixture)
    return fixture


def process_env(fixture):
    return {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(fixture.private / "home"),
            "DOCKER_CONFIG": str(fixture.private / "docker"), "COMPOSE_DISABLE_ENV_FILE": "1"}


def require_compose_version(version):
    match = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", version)
    if not match or tuple(map(int, match.groups())) < (2, 24, 4):
        raise ValueError("compose_override_version_unsupported")


def compose_command(fixture, action):
    # Returned commands are deferred plans, not permission to execute them.
    prefix = ["docker", "compose", "--project-directory", str(ROOT), "--project-name", fixture.project,
              "--env-file", str(fixture.env_file), "-f", str(ROOT_COMPOSE), "-f", str(fixture.override)]
    if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2_spike or fixture.paper_p2:
        prefix += ['-f', str(PAPER_P0_TEMPLATE)]
    if fixture.paper_p2_spike or fixture.paper_p2:
        prefix += ['-f', str(fixture.private / 'compose.p2.yml')]
    options = {"config": ["config", "--format", "json"],
               "build": ["build", "backend"],
                "up": ["up", "--detach", "--no-deps", *fixture_services(fixture)]}
    if action not in options:
        raise ValueError("unsupported_compose_action")
    return prefix + options[action]


def verify_bootstrap_result(fixture, result):
    # Fixed logical adapter contract, not an assumption about CLI JSON spelling.
    expected = {"volume": fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"],
                "fresh_volume": True, "store_created": True, "readiness": "NOT_READY"}
    if (result != expected or result.get("fresh_volume") is not True
            or result.get("store_created") is not True):
        raise ValueError("bootstrap_contract_rejected")


def validate_effective_model(fixture, model):
    """Fail closed on the actual merged model, before any Docker allocation.

    Bind sources are an exact reviewed allowlist, not a directory-prefix grant.
    Versioned upstream tags below are the root's selected pins (not latest).
    """
    services = model.get("services", {})
    if set(services) != set(fixture_services(fixture)):
        raise ValueError("effective_services_rejected")
    expected_images = {
        "postgres": "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a",
        "mosquitto": DEFAULTS["MQTT_DYNSEC_IMAGE_REFERENCE"],
        "nginx": "nginx:1.25-alpine", "supabase-auth": "supabase/gotrue:v2.196.0",
        "supabase-envoy": "envoyproxy/envoy:v1.39.1"}
    if fixture.paper_p2:
        expected_images['auth-socket-init'] = DEFAULTS['MQTT_DYNSEC_IMAGE_REFERENCE']
    for role in ("supabase-role-provisioner", "application-migrations", "supabase-compat-migration"):
        expected_images[role] = expected_images["postgres"]
    binds = {
        "nginx": {str(ROOT / "config/nginx/nginx.conf.template"): "/etc/nginx/templates/default.conf.template"},
        "postgres": {str(ROOT / "migrations"): "/docker-entrypoint-initdb.d"},
        "application-migrations": {str(ROOT / "migrations"): "/migrations", str(ROOT / "scripts/run-migrations.sh"): "/run-migrations.sh"},
        "supabase-role-provisioner": {str(ROOT / "migrations/000000_configure_supabase_roles.sh"): "/migrations/000000_configure_supabase_roles.sh"},
        "supabase-compat-migration": {
            str(ROOT / "migrations/000004_supabase_compat.up.sql"): "/migrations/000004_supabase_compat.up.sql",
            str(ROOT / "migrations/000021_admin_user_management.up.sql"): "/migrations/000021_admin_user_management.up.sql",
            str(ROOT / "migrations/000022_gateway_membership_revocation.up.sql"): "/migrations/000022_gateway_membership_revocation.up.sql",
            str(ROOT / "migrations/000023_sensor_control_intent.up.sql"): "/migrations/000023_sensor_control_intent.up.sql",
        },
        "supabase-envoy": {str(ROOT / "config/envoy" / name): target for name, target in (
            ("envoy.yaml", "/etc/envoy/envoy.yaml"), ("cds.yaml", "/etc/envoy/cds.yaml"),
            ("lds.template.yaml", "/etc/envoy/lds.template.yaml"), ("docker-entrypoint.sh", "/docker-entrypoint.sh"))},
        "mosquitto": {fixture.values["NCS_BROKER_CONFIG"]: "/mosquitto/config/mosquitto.conf", fixture.values["NCS_CERT_DIR"]: "/mosquitto/config/certs"},
        "backend": {fixture.values["MQTT_TLS_CA_SOURCE"]: "/mosquitto/config/certs/ca.crt"}, "supabase-auth": {}}
    volume_mounts = {
        "postgres": {("postgres_data", "/var/lib/postgresql/data", False)},
        "mosquitto": {("mosquitto_dynsec", "/mosquitto/dynsec", False), ("mosquitto_data", "/mosquitto/data", False)},
        "backend": {("mosquitto_dynsec", "/mosquitto/dynsec", True)}}
    dependencies = {
        "postgres": {}, "mosquitto": {},
        "supabase-role-provisioner": {"postgres": "service_healthy"},
        "application-migrations": {"postgres": "service_healthy", "supabase-role-provisioner": "service_completed_successfully"},
        "supabase-auth": {"supabase-role-provisioner": "service_completed_successfully", "application-migrations": "service_completed_successfully"},
        "supabase-compat-migration": {"postgres": "service_healthy", "supabase-auth": "service_healthy", "application-migrations": "service_completed_successfully"},
        "backend": {"postgres": "service_healthy", "mosquitto": "service_started", "application-migrations": "service_completed_successfully", "supabase-compat-migration": "service_completed_successfully"},
        "supabase-envoy": {"supabase-auth": "service_healthy"},
        "nginx": {"backend": "service_started", "supabase-envoy": "service_started"}}
    if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2_spike or fixture.paper_p2:
        expected_images['supabase-storage'] = 'supabase/storage-api:v1.74.0'
        binds['supabase-storage'] = {}
        volume_mounts['supabase-storage'] = {('storage_data', '/var/lib/storage', False)}
        dependencies['supabase-storage'] = {'supabase-role-provisioner': 'service_completed_successfully'}
        dependencies['supabase-envoy']['supabase-storage'] = 'service_healthy'
    if fixture.paper_p2:
        binds['auth-socket-init'] = {}
        binds['nginx'] = {str(ROOT / 'config/nginx/paper-p2.conf.template'): '/etc/nginx/templates/default.conf.template',
                          str(fixture.private / 'p2-tls'): '/etc/nginx/p2-tls'}
        volume_mounts['auth-socket-init'] = {('auth_socket', '/run/iot-auth', False)}
        volume_mounts['backend'].add(('auth_socket', '/run/iot-auth', False))
        volume_mounts['nginx'] = {('auth_socket', '/run/iot-auth', True)}
        dependencies['auth-socket-init'] = {}
        dependencies['backend'].update({'supabase-envoy': 'service_started', 'auth-socket-init': 'service_completed_successfully'})
    for name, service in services.items():
        if (service.get("container_name") or service.get("restart") != "no"
                or service.get("privileged") or service.get("devices") or service.get("cap_add")
                or service.get("network_mode") or service.get("pid") or service.get("env_file")
                or set(service.get("networks", {})) != {"iot_net"}
                or service.get("labels", {}).get("io.iot.native-smoke.run") != fixture.project
                or service.get("labels", {}).get("io.iot.native-smoke.role") != name):
            raise ValueError("effective_isolation_rejected")
        if name != "backend" and service.get("image") != expected_images[name]:
            raise ValueError("effective_image_rejected")
        if {k: v.get("condition") for k, v in service.get("depends_on", {}).items()} != dependencies[name]:
            raise ValueError("effective_dependencies_rejected")
        ports = service.get("ports", [])
        if name == "nginx":
            if (len(ports) != 1 or ports[0].get("host_ip") != "127.0.0.1"
                    or str(ports[0].get("published")) != "0" or ports[0].get("target") != (443 if fixture.paper_p2 else 80)
                    or ports[0].get("protocol") != "tcp"):
                raise ValueError("effective_ports_rejected")
        elif ports:
            raise ValueError("effective_ports_rejected")
        actual_binds, actual_volumes = {}, set()
        for mount in service.get("volumes", []):
            if mount.get("type") == "bind" and mount.get("read_only") is True:
                actual_binds[mount.get("source")] = mount.get("target")
            elif mount.get("type") == "volume":
                actual_volumes.add((mount.get("source"), mount.get("target"), mount.get("read_only", False)))
            else:
                raise ValueError("effective_mounts_rejected")
        if (actual_binds != binds[name] or actual_volumes != volume_mounts.get(name, set())
                or len(service.get("volumes", [])) != len(actual_binds) + len(actual_volumes)):
            raise ValueError("effective_mounts_rejected")
    backend = services["backend"]
    if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2_spike or fixture.paper_p2:
        env = backend.get('environment', {})
        expected = {'MQTT_INGRESS_ENABLED': 'true', 'STAGE5_ENABLED': 'false',
                    'MQTT_CREDENTIAL_RUNTIME_ENABLED': 'false', 'MQTT_CREDENTIAL_API_ENABLED': 'true',
                    'DATABASE_MAX_CONNS': '10', 'MQTT_QUEUE_CAPACITY': '256',
                    'MQTT_WORKER_COUNT': '4', 'MQTT_MAX_PAYLOAD_BYTES': '65536'}
        if (any(str(env.get(k)).lower() != v for k, v in expected.items())
                or backend.get('cpus') != 2 or str(backend.get('mem_limit')) != '4294967296'):
            raise ValueError('paper_p0_profile_rejected')
        if fixture.paper_p1 and str(env.get('METRICS_ENABLED')).lower() != fixture.values['METRICS_ENABLED']:
            raise ValueError('paper_p1_profile_rejected')
        if fixture.paper_p2:
            paper_p2_support().validate_perimeter(fixture, services)
    if backend.get("build", {}).get("context") != str(ROOT / "src") or backend["build"].get("dockerfile") != "Dockerfile" or backend["build"].get("target") != "backend":
        raise ValueError("effective_build_rejected")
    for name in ("backend", "mosquitto"):
        service = services[name]
        if service.get("user") != "1883:1883" or service.get("cap_drop") != ["ALL"] or "no-new-privileges:true" not in service.get("security_opt", []):
            raise ValueError("effective_security_rejected")
    if backend.get("read_only") is not True:
        raise ValueError("effective_rootfs_rejected")
    commands = {"application-migrations": ["/bin/sh", "/run-migrations.sh"],
                "supabase-role-provisioner": ["/bin/sh", "/migrations/000000_configure_supabase_roles.sh"],
                "supabase-compat-migration": ["/bin/sh", "-ec", 'psql -h postgres -U "postgres" -d "smoke" -f /migrations/000004_supabase_compat.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000021_admin_user_management.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000022_gateway_membership_revocation.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000023_sensor_control_intent.up.sql']}
    if any(services[name].get("command") != command for name, command in commands.items()):
        raise ValueError("effective_migrations_rejected")
    volumes = model.get("volumes", {})
    expected_volumes = {'postgres_data', 'mosquitto_data', 'mosquitto_dynsec'}
    if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2_spike or fixture.paper_p2:
        expected_volumes.add('storage_data')
    if fixture.paper_p2:
        expected_volumes.add('auth_socket')
    if set(volumes) != expected_volumes or set(model.get("networks", {})) != {"iot_net"}:
        raise ValueError("effective_resources_rejected")
    for name in sorted(expected_volumes):
        role = 'store' if name == 'mosquitto_dynsec' else name
        volume = volumes[name]
        if volume.get("name") != fixture.project + "_" + role:
            raise ValueError("effective_names_rejected")
        if name == "mosquitto_dynsec":
            if volume.get("external") is not True:
                raise ValueError("effective_bootstrap_volume_rejected")
        elif volume.get("labels", {}).get("io.iot.native-smoke.run") != fixture.project:
            raise ValueError("effective_labels_rejected")
    network = model["networks"]["iot_net"]
    if network.get("name") != fixture.project + "_iot_net" or network.get("labels", {}).get("io.iot.native-smoke.run") != fixture.project or network.get("external"):
        raise ValueError("effective_network_rejected")
    return True


def dry_effective_config(fixture, runner=subprocess.run):
    """Read-only Docker commands; raw config/output remains private, never printed."""
    def capture(command):
        try:
            result = runner(command, capture_output=True, timeout=BUDGETS["config"],
                            env=process_env(fixture), check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise ValueError("compose_preflight_failed") from None
        if result.returncode:
            raise ValueError("compose_preflight_failed")
        return result.stdout.decode()
    require_compose_version(capture(["docker", "compose", "version", "--short"]).strip())
    raw = capture(compose_command(fixture, "config"))
    private_write(fixture.private / "effective.json", raw)
    try:
        model = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("effective_json_rejected") from None
    validate_effective_model(fixture, model)
    return model


def execute_native(fixture, gate=None):
    # There is intentionally no supported gate until bootstrap GREEN and parent
    # approval. Adding a gate later must not bypass effective-config/receipt checks.
    raise RuntimeError("native_execution_deferred")


JOBS = {'supabase-role-provisioner', 'application-migrations', 'supabase-compat-migration', 'auth-socket-init'}


def startup_state(service, state):
    if service in JOBS:
        if state.get('Running'):
            return False
        if state.get('Status') == 'created':
            return False
        if state.get('ExitCode') != 0:
            raise ValueError('migration_nonzero')
        return True
    health = state.get('Health', {}).get('Status')
    if health == 'unhealthy' or state.get('Status') in ('exited', 'dead'):
        raise ValueError('health_failed')
    return state.get('Running') is True and health == 'healthy'


def verify_runtime_receipt(obj, expectation):
    host = obj.get('HostConfig', {})
    physical = {(m['Destination'], m['Type'], m.get('Name') if m['Type'] == 'volume' else m.get('Source'), m['RW'])
                for m in obj.get('Mounts', []) if m['Type'] != 'tmpfs'}
    if (physical != set(map(tuple, expectation['mounts']))
            or host.get('Tmpfs', {}) != expectation['tmpfs']
            or obj.get('Config', {}).get('User', '') != expectation['user']
            or bool(host.get('ReadonlyRootfs')) != expectation['readonly']
            or (host.get('CapDrop') or []) != expectation['cap_drop']
            or (host.get('SecurityOpt') or []) != expectation['security_opt']
            or host.get('Privileged') or host.get('Devices')
            or set(obj.get('NetworkSettings', {}).get('Networks', {})) != {expectation['network']}):
        raise ValueError('runtime_receipt_rejected')


def startup_only(fixture, registry=None, positive_http=False, role_http=False, full_http=False):
    """Phased real startup. Optionally runs positive HTTP smoke slice."""
    lifecycle = bootstrap_lifecycle()
    if registry is None:
        registry = ResourceRegistry(fixture)
    phase, failure, evidence = 'preflight', None, {}
    containers = {}
    images = {}
    cleanup_verified = False

    try:
        phase = 'preflight'
        model = dry_effective_config(fixture)
        for name, service in model['services'].items():
            if name == 'backend':
                continue
            result = registry.command(['image', 'inspect', service['image']])
            if result.returncode:
                raise ValueError('local_image_unavailable')
            images[name] = json.loads(result.stdout)[0]['Id']
        phase = 'build'
        build_native_image(fixture, prevalidated=True)
        result = registry.command(['image', 'inspect', fixture.project + '-backend'])
        if result.returncode:
            raise ValueError('backend_build_image_unresolved')
        images['backend'] = json.loads(result.stdout)[0]['Id']
        if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2:
            # Same Dockerfile builder, but the real Gateway target, not a host binary.
            fd = os.open(fixture.private / 'gateway-build-output.txt', os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                result = subprocess.run(['docker', 'build', '--target', 'gateway', '-t',
                    fixture.project + '-gateway', '-f', str(ROOT / 'src/Dockerfile'), str(ROOT / 'src')],
                    stdout=stream, stderr=subprocess.STDOUT, timeout=BUDGETS['build'],
                    env=process_env(fixture), check=False)
            if result.returncode:
                raise ValueError('native_dockerfile_build_blocked')
            result = registry.command(['image', 'inspect', fixture.project + '-gateway'])
            if result.returncode:
                raise ValueError('backend_build_image_unresolved')
            images['gateway'] = json.loads(result.stdout)[0]['Id']

        with lifecycle.cancellation_scope() as state:
            try:
                phase = 'bootstrap'
                generate_fixture_tls(fixture)
                volume = allocate_bootstrap_store(fixture, registry, lifecycle)
                with lifecycle.defer_cancellation():
                    allocation_readback(fixture, registry, volume)
                state.checkpoint()
                # All intents precede Compose allocation (including implicit resources).
                for kind, definitions in (('network', model['networks']), ('volume', model['volumes'])):
                    for role, resource in definitions.items():
                        if resource.get('external'):
                            continue
                        labels = dict(resource['labels'])
                        labels['com.docker.compose.project'] = fixture.project
                        labels['com.docker.compose.' + kind] = role
                        item = registry.intent(kind, resource['name'], None, labels)
                        if registry.resolve(item) is not None:
                            raise ValueError('resource_collision')
                for name, service in model['services'].items():
                    image_config = json.loads(registry.command(['image', 'inspect', images[name]]).stdout)[0]['Config']
                    labels = dict(service['labels'])
                    labels.update({'com.docker.compose.project': fixture.project, 'com.docker.compose.service': name,
                                   'com.docker.compose.oneoff': 'False', 'com.docker.compose.container-number': '1'})
                    item = registry.intent('container', fixture.project + '-' + name + '-1', images[name], labels)
                    mounts = [(m['target'], m['type'], model['volumes'][m['source']]['name'] if m['type'] == 'volume' else m['source'], not m.get('read_only', False)) for m in service.get('volumes', [])]
                    tmpfs = dict(t.split(':', 1) for t in service.get('tmpfs', []))
                    item['expectation'] = {'mounts': mounts, 'tmpfs': tmpfs, 'user': service.get('user', image_config.get('User', '')),
                        'readonly': service.get('read_only', False), 'cap_drop': service.get('cap_drop', []),
                        'security_opt': service.get('security_opt', []), 'network': fixture.project + '_iot_net'}
                    registry.save(item)
                    if registry.resolve(item) is not None:
                        raise ValueError('resource_collision')
                    containers[name] = item
                # Image-declared VOLUMEs must all be accounted for before create/up.
                for name, item in containers.items():
                    img = json.loads(registry.command(['image', 'inspect', images[name]]).stdout)[0]
                    covered = {m[0] for m in item['expectation']['mounts']} | set(item['expectation']['tmpfs'])
                    if set(img.get('Config', {}).get('Volumes') or {}) - covered:
                        raise ValueError('anonymous_volume_prevention_failed')
                for name in fixture_services(fixture):
                    phase = name
                    state.checkpoint()
                    for item in registry.items:
                        if item['kind'] != 'container' or item is containers[name]:
                            registry.allocating(item)
                    command = compose_command(fixture, 'config')[:-3] + ['up', '--detach', '--no-deps', '--no-build', '--pull', 'never', name]
                    # config prefix ends in config --format json.
                    try:
                        result = subprocess.run(command, capture_output=True, timeout=BUDGETS['up'], env=process_env(fixture), check=False)
                    except (OSError, subprocess.TimeoutExpired):
                        raise ValueError('compose_phase_unresolved') from None
                    # Even a lost/nonzero reply is reconciled by the predeclared exact names.
                    for item in registry.items:
                        if item['allocation_attempted']:
                            registry.resolve(item)
                    if result.returncode:
                        raise ValueError('compose_phase_failed')
                    deadline = time.monotonic() + BUDGETS['up']
                    while True:
                        state.checkpoint()
                        obj = registry.resolve(containers[name])
                        if obj is None:
                            raise ValueError('phase_container_absent')
                        if name == 'nginx' and fixture.paper_p2:
                            ready = registry.command(['exec', obj['Id'], 'nginx', '-t']).returncode == 0
                        else:
                            ready = startup_state(name, obj['State']) if name in JOBS or name in ('postgres', 'supabase-auth', 'backend', 'supabase-storage') else probe_startup(registry, name, obj)
                        if ready:
                            evidence[name] = {'image_id': obj['Image'], 'id': obj['Id'], 'exit': obj['State'].get('ExitCode'), 'health': obj['State'].get('Health', {}).get('Status', 'probe_ready')}
                            break
                        if time.monotonic() >= deadline:
                            raise ValueError('phase_health_timeout')
                        time.sleep(1)
                    if name == 'application-migrations':
                        sql = "SELECT string_agg(version::text || ':' || name, ',' ORDER BY version) FROM schema_migrations WHERE version <= 20"
                        result = registry.command(['exec', containers['postgres']['receipt']['id'], 'psql', '-U', 'postgres', '-d', 'smoke', '-At', '-c', sql])
                        # Auth-dependent migrations 21/22 are applied in the compat phase.
                        expected = ','.join(row for row in expected_schema_history().split(',')
                                            if int(row.split(':')[0]) <= 20)
                        if not schema_history_verified(result.returncode, result.stdout.decode().strip(), expected, terminal='20:stage3_telemetry_ingestion'):
                            raise ValueError('schema_exact_rejected')
                        evidence['schema_pre_auth_exact'] = True
                    if name == 'supabase-compat-migration':
                        # Root psql command lacks ON_ERROR_STOP; exit0 alone cannot
                        # prove that its FK statements actually took effect.
                        sql = "SELECT count(*) FROM pg_constraint WHERE conname = 'profiles_id_fkey' AND conrelid = 'public.profiles'::regclass AND confrelid = 'auth.users'::regclass AND contype = 'f' AND EXISTS (SELECT 1 FROM schema_migrations WHERE version = 4 AND name = 'supabase_compat')"
                        result = registry.command(['exec', containers['postgres']['receipt']['id'], 'psql', '-U', 'postgres', '-d', 'smoke', '-At', '-c', sql])
                        if result.returncode or result.stdout.strip() != b'1':
                            raise ValueError('compat_constraints_rejected')
                        # Migrations 21/22 run in the post-Auth compatibility phase; verify that
                        # trigger prerequisites existed and the exact terminal schema23 is recorded.
                        trigger_sql = "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON t.tgrelid = c.oid JOIN pg_namespace n ON c.relnamespace = n.oid WHERE n.nspname = 'auth' AND c.relname = 'users' AND t.tgname = 'on_auth_user_created'"
                        result = registry.command(['exec', containers['postgres']['receipt']['id'], 'psql', '-U', 'postgres', '-d', 'smoke', '-At', '-c', trigger_sql])
                        if result.returncode or result.stdout.strip() != b'1':
                            raise ValueError('compat_constraints_rejected')
                        # Version 4 was checked above; exclude it from the application history.
                        schema_sql = "SELECT string_agg(version::text || ':' || name, ',' ORDER BY version) FROM schema_migrations WHERE version <> 4"
                        result = registry.command(['exec', containers['postgres']['receipt']['id'], 'psql', '-U', 'postgres', '-d', 'smoke', '-At', '-c', schema_sql])
                        expected = expected_schema_history()
                        if not schema_history_verified(result.returncode, result.stdout.decode().strip(), expected):
                            raise ValueError('schema_exact_rejected')
                        evidence['schema_exact'] = True
                    durable_json(fixture.private / 'startup-progress.json', {'phase': phase, 'evidence': evidence})
                obj = registry.resolve(containers['nginx'])
                ports = obj['NetworkSettings']['Ports'].get('443/tcp' if fixture.paper_p2 else '80/tcp') or []
                if len(ports) != 1 or ports[0]['HostIp'] != '127.0.0.1' or not 0 < int(ports[0]['HostPort']) < 65536:
                    raise ValueError('mapped_endpoint_rejected')
                url = ('https' if fixture.paper_p2 else 'http') + '://127.0.0.1:' + ports[0]['HostPort']
                endpoint = HTTPSEndpoint(url, fixture.private / 'p2-ca.crt') if fixture.paper_p2 else url
                evidence['endpoint'] = url
                if fixture.paper_p2:
                    phase = 'paper_p2'
                    evidence['paper_p2'] = paper_p2_support().run_full(sys.modules[__name__], fixture,
                        registry, endpoint, containers, images, state)
                elif fixture.paper_p2_spike:
                    phase = 'paper_p2_spike'
                    state.checkpoint()
                    evidence['paper_p2_spike'] = paper_p2_support().run_spike(
                        sys.modules[__name__], fixture, registry, endpoint, containers, images)
                elif fixture.paper_p1:
                    phase = 'paper_p1'
                    state.checkpoint()
                    evidence['paper_p1'] = run_paper_p1(fixture, registry, endpoint, containers, images, state)
                elif fixture.paper_p0:
                    phase = 'paper_p0'
                    state.checkpoint()
                    evidence['paper_p0'] = run_paper_p0(fixture, registry, endpoint, containers, images, state)
                    durable_json(fixture.private / 'paper-p0-progress.json', evidence['paper_p0'])
                if positive_http or role_http or full_http:
                    phase = 'positive_http'
                    state.checkpoint()
                    http_evidence = run_positive_http_slice(fixture, registry, endpoint, containers['postgres']['receipt']['id'])
                    evidence['positive_http'] = http_evidence
                    if role_http or full_http:
                        phase = 'role_http'
                        evidence['role_http'] = run_role_http_slice(fixture, registry, endpoint, containers['postgres']['receipt']['id'])
                    if full_http:
                        phase = 'jwt_http'
                        role_evidence = evidence['role_http']
                        evidence['jwt_http'] = run_jwt_denial_slice(
                            fixture, registry, endpoint, containers['postgres']['receipt']['id'],
                            role_evidence['gateway_id'], role_evidence['existing_sensor'])
                    durable_json(fixture.private / 'positive-http-progress.json', {'phase': phase, 'evidence': evidence})
            except BaseException as exc:
                failure = exc
                code = sanitize_failure_code(exc)
                try:
                    durable_json(fixture.private / 'startup-failure.json', {
                        'phase': phase, 'error_type': sanitize_failure_type(exc),
                        'error_code': code})
                    if phase in containers and containers[phase]['allocation_attempted']:
                        obj = registry.resolve(containers[phase])
                        if obj:
                            durable_json(fixture.private / 'failed-phase-state.json', obj['State'])
                            log = registry.command(['logs', '--tail', '30', obj['Id']])
                            durable_json(fixture.private / 'failed-phase-private-log.json', {
                                'stdout': log.stdout.decode(errors='replace'), 'stderr': log.stderr.decode(errors='replace')})
                except Exception:
                    pass
            finally:
                state.unwinding = True
                primary_failure = failure
                primary_phase_saved = phase if primary_failure is not None else None
                primary_code_saved = sanitize_failure_code(primary_failure) if primary_failure is not None else None
                cleanup_step = None
                cleanup_target_kind = None
                cleanup_err_code = "unclassified"
                try:
                    cleanup_step = "reconcile_child_receipts"
                    if isinstance(failure, lifecycle.IncompleteCleanup):
                        cleanup_err_code = "child_cleanup_unresolved"
                        raise ValueError('child_cleanup_unresolved')
                    if registry.items:
                        lifecycle.reconcile_child_receipts(fixture.private, registry.items[0]['name'], registry.items[0]['labels'])
                    # Consumers first, then infrastructure, then inputs. No down/prune.
                    ordered = sorted(reversed(registry.items), key=lambda i: {'container': 0, 'network': 1, 'volume': 2}[i['kind']])
                    cleanup_deadline = time.monotonic() + BUDGETS['cleanup'] * 2
                    for item in ordered:
                        cleanup_step = "check_deadline"
                        cleanup_target_kind = None
                        if time.monotonic() >= cleanup_deadline:
                            cleanup_err_code = "startup_cleanup_deadline"
                            raise ValueError('startup_cleanup_deadline')
                        if item['allocation_attempted']:
                            cleanup_step = "remove_resource"
                            cleanup_target_kind = item['kind']
                            registry.remove(item)
                    cleanup_verified = True
                except Exception as cl_exc:
                    cleanup_verified = False
                    try:
                        durable_json(fixture.artifacts / 'startup-summary.json', {'status': 'CLEANUP_UNRESOLVED', 'phase': phase, 'http_accepted': False})
                    except Exception:
                        pass
                    raw_cl_code = sanitize_failure_code(cl_exc)
                    if raw_cl_code in ALLOWED_FAILURE_ERROR_CODES and raw_cl_code != "unclassified":
                        cleanup_err_code = raw_cl_code
                    cleanup_failure = ValueError('startup_cleanup_unresolved_evidence_retained')
                    fail_report = record_failure_report(
                        fixture,
                        phase="cleanup",
                        exc=cleanup_failure,
                        cleanup_verified=False,
                        primary_phase=primary_phase_saved,
                        primary_error_code=primary_code_saved,
                        cleanup_step=cleanup_step or "reconcile_child_receipts",
                        cleanup_error_code=cleanup_err_code,
                        resource_kind=cleanup_target_kind,
                    )
                    raise FailureDiagnostic(fail_report, cause=cleanup_failure) from cl_exc
                if state.signum is not None:
                    failure = lifecycle.RunnerInterrupted(state.signum)
    except FailureDiagnostic:
        raise
    except BaseException as exc:
        failure = exc

    cleanup_counts = {k: sum(i['kind'] == k and i.get('removed', False) for i in registry.items) for k in ('container', 'network', 'volume')}
    if fixture.paper_p2_spike:
        path = fixture.artifacts / 'spike.json'
        result = json.loads(path.read_text()) if path.exists() else {'schema_version': 1}
        result.update(status='FAIL' if failure or not cleanup_verified else 'PASS',
                      cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts)
        durable_json(path, result)
    if fixture.paper_p1 or fixture.paper_p2:
        path = fixture.artifacts / 'run.json'
        run = json.loads(path.read_text()) if path.exists() else {'schema_version': 1, 'run_id': fixture.project, 'mode': fixture.paper_p1}
        run.update(status='FAIL' if failure or not cleanup_verified else 'PASS',
                   cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                   error=sanitize_failure_code(failure) if failure else None)
        durable_json(path, run)
    if failure:
        fail_report = record_failure_report(fixture, phase=phase, exc=failure, cleanup_verified=cleanup_verified)
        raise FailureDiagnostic(fail_report, cause=failure) from failure

    try:
        if fixture.paper_p2:
            if cleanup_counts != {'container': 19, 'network': 1, 'volume': 7} or not evidence['paper_p2'].get('full_p2_accepted'):
                raise ValueError('cleanup_acceptance_incomplete')
            report = {'schema_version': 1, 'status': 'PAPER_P2_ACCEPTED', 'project': fixture.project,
                      'native_executed': True, 'full_p2_accepted': True,
                      'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts}
        elif fixture.paper_p2_spike:
            report = {'schema_version': 1, 'status': 'PAPER_P2_SPIKE_ACCEPTED', 'project': fixture.project,
                      'native_executed': True, 'full_p2_accepted': False,
                      'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts}
        elif fixture.paper_p1:
            if cleanup_counts != {'container': 15, 'network': 1, 'volume': 6}:
                raise ValueError('cleanup_acceptance_incomplete')
            report = {'schema_version': 1, 'status': 'PAPER_P1_ACCEPTED', 'project': fixture.project,
                      'native_executed': True, 'exact_cleanup_verified': True, 'cleanup_counts': cleanup_counts}
        elif fixture.paper_p0:
            report = {'schema_version': 1, 'status': 'PAPER_P0_ACCEPTED',
                      'project': fixture.project, 'services': list(fixture_services(fixture)),
                      'native_executed': True, 'exact_cleanup_verified': True,
                      'cleanup_counts': cleanup_counts, 'evidence': evidence}
            if cleanup_counts != {'container': 15, 'network': 1, 'volume': 6}:
                raise ValueError('cleanup_acceptance_incomplete')
            validate_paper_p0_evidence(evidence['paper_p0'])
        elif full_http:
            report = full_http_summary(fixture, evidence, cleanup_counts)
            report['backend_image_id'] = images['backend']
            report['phase'] = phase
        elif role_http:
            report = role_http_summary(fixture, evidence, cleanup_counts)
            report['backend_image_id'] = images['backend']
            report['phase'] = phase
        elif positive_http:
            report = positive_summary(fixture, evidence=evidence, cleanup_counts=cleanup_counts)
            report['backend_image_id'] = images['backend']
            report['phase'] = phase
        else:
            report = {'status': 'STARTUP_VERIFIED_NOT_HTTP_ACCEPTED', 'phase': phase,
                      'http_accepted': False, 'exact_cleanup_verified': True, 'evidence': evidence, 'backend_image_id': images['backend'],
                      'cleanup_counts': cleanup_counts}
        durable_json(fixture.artifacts / 'startup-summary.json', report)
        return report
    except BaseException as exc:
        fail_report = record_failure_report(
            fixture,
            phase="report_validation",
            exc=exc,
            cleanup_verified=cleanup_verified,
        )
        raise FailureDiagnostic(fail_report, cause=exc) from exc


def probe_startup(registry, name, obj):
    if obj['State'].get('Status') in ('exited', 'dead'):
        raise ValueError('health_failed')
    if not obj['State'].get('Running'):
        return False
    # Envoy container only has /bin/bash (no curl/wget). Query Envoy admin /ready endpoint via /dev/tcp.
    # Read the status line under bounded timeout and check for HTTP/1.1 200 OK.
    commands = {'mosquitto': ['sh', '-ec', 'cat /mosquitto/dynsec/dynamic-security.json >/dev/null && grep -q ":22B3 .* 0A " /proc/net/tcp'],
                'supabase-envoy': ['/bin/bash', '-ec', 'exec 3<>/dev/tcp/127.0.0.1/9901 && printf "GET /ready HTTP/1.1\\r\\nHost: 127.0.0.1\\r\\nConnection: close\\r\\n\\r\\n" >&3 && IFS= read -r -t 2 line <&3 && case "$line" in "HTTP/1.1 200 OK"*) exit 0;; *) exit 1;; esac'],
                'nginx': ['wget', '-q', '-O', '/dev/null', 'http://127.0.0.1/readyz']}
    return registry.command(['exec', obj['Id'], *commands[name]]).returncode == 0


def build_native_image(fixture, runner=subprocess.run, prevalidated=False):
    """Bounded build preflight only; never allocates runtime fixture resources.

    A build failure is a stop boundary, not permission to substitute a host Go
    toolchain or change the production Dockerfile. Output remains protected.
    """
    if not prevalidated:
        dry_effective_config(fixture, runner=runner)
    output = fixture.private / "build-output.txt"
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        try:
            result = runner(compose_command(fixture, "build"), stdout=stream,
                            stderr=subprocess.STDOUT, timeout=BUDGETS["build"],
                            env=process_env(fixture), check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise ValueError("native_build_unresolved") from None
    if result.returncode:
        raise ValueError("native_dockerfile_build_blocked")
    return True


def probe_envoy_toolchain(fixture):
    """Exact owned, networkless image probe: no stack, credentials or mounts.

    Root Envoy entrypoint needs openssl and sed before starting the gateway.
    Do not install packages or replace the entrypoint when either is absent.
    """
    model = dry_effective_config(fixture)
    def run(args):
        try:
            return subprocess.run(["docker", *args], capture_output=True,
                                  env=process_env(fixture), timeout=15, check=False)
        except (OSError, subprocess.TimeoutExpired):
            raise ValueError("envoy_probe_unresolved") from None
    def inspect(kind, identity):
        result = run([kind, "inspect", identity])
        if result.returncode:
            raise ValueError("envoy_probe_inspect_failed")
        items = json.loads(result.stdout)
        if len(items) != 1:
            raise ValueError("envoy_probe_inspect_failed")
        return items[0]
    image = inspect("image", model["services"]["supabase-envoy"]["image"])["Id"]
    name = fixture.project + "-envoy-toolchain"
    absent = run(["container", "inspect", name])
    if absent.returncode == 0 or b"no such container" not in absent.stderr.lower():
        raise ValueError("envoy_probe_collision_or_unknown")
    labels = {"test.task": "native-compose-smoke", "test.run": fixture.project,
              "io.iot.native-smoke.role": "envoy-toolchain"}
    create = ["create", "--pull=never", "--name", name, "--network", "none",
              "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
              "--log-driver", "none", "--user", "65534:65534"]
    for key, value in labels.items():
        create += ["--label", key + "=" + value]
    create += ["--entrypoint", "/bin/sh", image, "-c", "command -v openssl >/dev/null && command -v sed >/dev/null"]
    allocation = run(create)
    cid = allocation.stdout.decode().strip()
    if allocation.returncode or not re.fullmatch(r"[a-f0-9]{64}", cid):
        # Lost allocation reply is NOT authority to remove a resource by name.
        raise ValueError("envoy_probe_allocation_unresolved")
    receipt = inspect("container", cid)
    def verify(obj):
        config = obj.get("Config", {})
        host = obj.get("HostConfig", {})
        if (obj.get("Id") != cid or obj.get("Image") != image or obj.get("Name") != "/" + name
                or any(config.get("Labels", {}).get(k) != v for k, v in labels.items())
                or config.get("User") != "65534:65534" or obj.get("Mounts")
                or host.get("NetworkMode") != "none" or host.get("ReadonlyRootfs") is not True
                or host.get("CapDrop") != ["ALL"]):
            raise ValueError("envoy_probe_ownership_refused")
    verify(receipt)
    private_write(fixture.private / "envoy-probe-receipt.json", json.dumps({
        "id": cid, "image": image, "created": receipt["Created"], "labels": labels}))
    exit_code = None
    try:
        result = run(["start", "--attach", cid])
        after = inspect("container", cid)
        verify(after)
        if after.get("State", {}).get("Running"):
            raise ValueError("envoy_probe_unresolved")
        exit_code = after["State"]["ExitCode"]
        if result.returncode != exit_code:
            raise ValueError("envoy_probe_exit_unresolved")
    finally:
        current = inspect("container", cid)
        verify(current)
        if current["Created"] != receipt["Created"]:
            raise ValueError("envoy_probe_replacement_refused")
        if run(["container", "rm", "--force", cid]).returncode:
            raise ValueError("envoy_probe_cleanup_failed")
        gone = run(["container", "inspect", cid])
        if gone.returncode == 0 or b"no such container" not in gone.stderr.lower():
            raise ValueError("envoy_probe_cleanup_unresolved")
    return {"image_id": image, "toolchain_available": exit_code == 0,
            "exit_code": exit_code, "owned_containers_removed": 1, "absence_verified": True}


@dataclass(frozen=True)
class Receipt:
    kind: str
    identity: str
    project: str
    role: str
    created_at: str


def receipt_metadata(receipt):
    return {"kind": receipt.kind, "identity": receipt.identity, "created_at": receipt.created_at,
            "run_label": receipt.project, "role_label": receipt.role, "compose_project": receipt.project}


def cleanup_commands(receipts, inspect, veto=False):
    # Plan only. A later executor must re-inspect immediately before each removal
    # with a bounded timeout, and stop on any mismatch or removal failure.
    if veto or len({(r.kind, r.identity) for r in receipts}) != len(receipts):
        raise ValueError("cleanup_veto")
    for r in receipts:
        if (r.kind not in {"container", "network", "volume"}
                or not re.fullmatch(r"ncs_[a-f0-9]{12}", r.project)
                or not re.fullmatch(r"\d{4}-\d\d-\d\dT[0-9:.]+Z", r.created_at)
                or r.role not in {*SERVICES, "store", "postgres_data", "mosquitto_data", "iot_net"}):
            raise ValueError("invalid_receipt")
        expected_name = r.project + "_" + r.role
        if (r.kind == "volume" and r.identity != expected_name
                or r.kind != "volume" and not re.fullmatch(r"[a-f0-9]{64}", r.identity)
                or inspect(r) != receipt_metadata(r)):
            raise ValueError("ownership_refused")
    ordered = sorted(receipts, key=lambda r: {"container": 0, "network": 1, "volume": 2}[r.kind])
    return [["docker", r.kind, "rm", *(["--force"] if r.kind == "container" else []), r.identity] for r in ordered]


PENDING = ("effective_compose_isolation", "tls_uid1883", "bootstrap_cli_receipt",
           "real_migrations_exit_zero_and_schema_exact", "dockerfile_backend_image",
           "nginx_envoy_password_login", "admin_inventory", "nonadmin_inventory_denials",
           "owner_sensor_create_retry", "operator_viewer_nonmember_sensor_denials",
           "cross_gateway_no_db_mutation", "missing_invalid_expired_jwt", "no_secret_fields",
           "exact_owned_cleanup")

# Deferred HTTP adapter inputs. Real users must be created with GoTrue Admin API
# using a protected service-role config, then password login THROUGH Nginx/Envoy.
# The database fixture grants platform_admins and user_gateways explicitly; it
# must not grant admin to all users. Requests use in-process headers/body, never
# shell argv. Denial cases additionally compare before/after SQL sensor state.
ACCEPTANCE = (
    ("login", "POST", "/auth/v1/token?grant_type=password", "all_humans", 200),
    ("inventory", "GET", "/v1/admin/gateways", "platform_admin", 200),
    ("forbidden", "GET", "/v1/admin/gateways", "owner", 403),
    ("forbidden", "GET", "/v1/admin/gateways", "operator", 403),
    ("forbidden", "GET", "/v1/admin/gateways", "viewer", 403),
    ("forbidden", "GET", "/v1/admin/gateways", "nonmember", 403),
    ("sensor_create", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "owner", 201),
    ("sensor_retry", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "owner", 200),
    ("forbidden", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "operator", 403),
    ("forbidden", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "viewer", 403),
    ("not_found", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "nonmember", 404),
    ("not_found", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "other_gateway_owner", 404),
    ("not_found", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "admin_without_membership", 404),
    ("unauthorized", "GET", "/v1/admin/gateways", "missing_invalid_expired_jwt", 401),
    ("unauthorized", "PUT", "/v1/gateways/{gateway_id}/sensors/{sensor_id}", "missing_invalid_expired_jwt", 401),
)
SENSOR_PAYLOAD = {"name": "Smoke sample", "unit": "C"}


def summary(fixture):
    return {"schema_version": 1, "status": "PREPARED_NOT_EXECUTED", "project": fixture.project,
            "services": list(SERVICES), "pending_assertions": list(PENDING), "native_executed": False}


def assert_safe_data(data):
    dumped = json.dumps(data)
    if re.search(r"password|credential|secret|token|hash|\bjwt\b", dumped, re.I):
        raise ValueError("secret_response_rejected")


ALLOWED_FAILURE_STATUSES = {"STARTUP_FAILED"}
ALLOWED_FAILURE_PHASES = {
    "prepare", "preflight", "build", "bootstrap",
    *SERVICES, 'supabase-storage', 'auth-socket-init', 'paper_p0', 'paper_p1', 'paper_p2_spike', 'paper_p2',
    "positive_http", "role_http", "jwt_http",
    "cleanup", "report_validation"
}
ALLOWED_FAILURE_ERROR_TYPES = {
    "ValueError", "RuntimeError", "OSError", "TimeoutExpired",
    "RunnerInterrupted", "IncompleteCleanup", "Exception"
}
ALLOWED_FAILURE_ERROR_CODES = {
    'paper_p2_mode_rejected', 'paper_p2_spike_failed', 'paper_p2_perimeter_failed',
    'paper_p2_rotation_failed', 'paper_p2_acceptance_failed',
    'paper_p1_mode_rejected', 'paper_p1_profile_rejected', 'paper_p1_snapshot_rejected',
    'paper_p1_resources_rejected', 'paper_p1_subscription_failed', 'paper_p1_scrape_failed',
    'paper_p1_auth_failed', 'paper_p1_gateway_failed', 'paper_p1_reconciliation_failed',
    'paper_p1_replay_failed', 'paper_p1_read_failed',
    'paper_p0_profile_rejected', 'paper_p0_quota_rejected', 'paper_p0_stats_failed',
    'paper_p0_gateway_failed', 'paper_p0_storage_failed', 'paper_p0_seed_failed',
    'paper_p0_read_failed', 'paper_p0_acceptance_incomplete', 'paper_p0_metadata_failed',
    "unsafe_fixture_path", "compose_override_version_unsupported",
    "unsupported_compose_action", "missing_interpolation",
    "compose_preflight_failed", "effective_json_rejected",
    "effective_services_rejected", "effective_isolation_rejected",
    "effective_image_rejected", "effective_dependencies_rejected",
    "effective_ports_rejected", "effective_mounts_rejected",
    "effective_build_rejected", "effective_security_rejected",
    "effective_rootfs_rejected", "effective_migrations_rejected",
    "effective_resources_rejected", "effective_names_rejected",
    "effective_bootstrap_volume_rejected", "effective_labels_rejected",
    "effective_network_rejected",
    "local_image_unavailable", "backend_build_image_unresolved",
    "native_dockerfile_build_blocked", "native_build_unresolved",
    "fixture_tls_failed", "bootstrap_contract_rejected",
    "bootstrap_nonce_rejected", "store_permissions_rejected",
    "store_metadata_rejected", "readback_mounts_rejected",
    "helper_collision", "helper_allocation_unresolved",
    "helper_receipt_rejected", "allocation_readback_failed",
    "allocation_child_unresolved_evidence_retained",
    "allocation_cleanup_deadline", "allocation_cleanup_unresolved_evidence_retained",
    "resource_collision", "resource_intent_rejected",
    "resource_inspect_unresolved", "resource_ownership_refused",
    "resource_replacement_refused", "helper_mount_replacement_refused",
    "helper_mount_count_mismatch", "helper_mount_destinations_mismatch",
    "helper_mount_attributes_mismatch", "helper_tmpfs_mismatch",
    "helper_mount_receipt_invalid",
    "resource_cleanup_unresolved", "anonymous_volume_prevention_failed",
    "compose_phase_unresolved", "compose_phase_failed",
    "phase_container_absent", "phase_health_timeout",
    "health_failed", "migration_nonzero",
    "runtime_receipt_rejected", "schema_exact_rejected",
    "compat_constraints_rejected", "mapped_endpoint_rejected",
    "admin_user_creation_failed", "admin_user_uuid_invalid",
    "login_failed", "token_format_invalid",
    "token_claims_decode_failed", "token_claims_mismatch",
    "db_variable_invalid", "db_psql_execution_failed",
    "denial_mutated_database", "admin_inventory_row_mismatch",
    "sensor_create_body_mismatch", "sensor_create_entity_mismatch",
    "sensor_retry_body_mismatch", "db_sensor_row_count_mismatch",
    "db_twin_entity_count_mismatch", "db_twin_relationship_count_mismatch",
    "positive_acceptance_incomplete", "role_acceptance_incomplete",
    "jwt_acceptance_incomplete", "cleanup_acceptance_incomplete",
    "response_contract_failed", "inventory_contract_failed",
    "sensor_contract_failed", "secret_response_rejected",
    "role_case_inventory_owner_contract_failed",
    "role_case_inventory_operator_contract_failed",
    "role_case_inventory_viewer_contract_failed",
    "role_case_inventory_nonmember_contract_failed",
    "role_case_operator_sensor_new_contract_failed",
    "role_case_operator_sensor_existing_contract_failed",
    "role_case_viewer_sensor_new_contract_failed",
    "role_case_viewer_sensor_existing_contract_failed",
    "role_case_nonmember_sensor_new_contract_failed",
    "role_case_nonmember_sensor_existing_contract_failed",
    "role_case_cross_gateway_owner_sensor_new_contract_failed",
    "role_case_cross_gateway_owner_sensor_existing_contract_failed",
    "role_case_platform_admin_without_membership_sensor_new_contract_failed",
    "role_case_platform_admin_without_membership_sensor_existing_contract_failed",
    "child_cleanup_unresolved", "startup_cleanup_deadline",
    "startup_cleanup_unresolved_evidence_retained",
    "runner_interrupted", "cleanup_veto",
    "invalid_receipt", "ownership_refused",
    "envoy_probe_unresolved", "envoy_probe_inspect_failed",
    "envoy_probe_collision_or_unknown", "envoy_probe_allocation_unresolved",
    "envoy_probe_ownership_refused", "envoy_probe_exit_unresolved",
    "envoy_probe_replacement_refused", "envoy_probe_cleanup_failed",
    "envoy_probe_cleanup_unresolved", "native_execution_deferred",
    "http_body_too_large", "unsafe_summary", "unsafe_failure_report", "unclassified"
}

ALLOWED_CLEANUP_STEPS = {
    "reconcile_child_receipts",
    "remove_resource",
    "check_deadline",
}

ALLOWED_RESOURCE_KINDS = {
    "container",
    "network",
    "volume",
}

FALLBACK_FAILURE_DIAGNOSTIC = {
    "schema_version": 1,
    "status": "STARTUP_FAILED",
    "phase": "preflight",
    "error_type": "ValueError",
    "error_code": "unsafe_failure_report",
    "http_accepted": False,
    "cleanup_verified": False,
    "primary_phase": None,
    "primary_error_code": None,
    "cleanup_step": None,
    "cleanup_error_code": None,
    "resource_kind": None,
}


class FailureDiagnostic(ValueError):
    def __init__(self, report: dict, cause: BaseException | None = None):
        if type(report) is dict:
            code = report.get("error_code", "unclassified")
            clean_code = code if type(code) is str and code in ALLOWED_FAILURE_ERROR_CODES else "unclassified"
            self.report = dict(report)
        else:
            clean_code = "unclassified"
            self.report = {}
        super().__init__(clean_code)
        self.cause = cause


def _trusted_exception_types() -> dict[type, str]:
    types = {
        ValueError: "ValueError",
        RuntimeError: "RuntimeError",
        OSError: "OSError",
        subprocess.TimeoutExpired: "TimeoutExpired",
    }
    try:
        lifecycle = bootstrap_lifecycle()
        types[lifecycle.RunnerInterrupted] = "RunnerInterrupted"
        types[lifecycle.IncompleteCleanup] = "IncompleteCleanup"
    except Exception:
        pass
    return types


def sanitize_failure_type(exc: BaseException) -> str:
    trusted = _trusted_exception_types()
    return trusted.get(type(exc), "Exception")


def sanitize_failure_code(exc: BaseException) -> str:
    exc_type = type(exc)
    if exc_type is ValueError or exc_type is RuntimeError or exc_type is OSError:
        args = getattr(exc, "args", ())
        if args and type(args[0]) is str:
            raw_msg = args[0]
            if raw_msg in ALLOWED_FAILURE_ERROR_CODES:
                return raw_msg
            if raw_msg.startswith("http_request_failed:"):
                return "response_contract_failed"
        return "unclassified"
    try:
        lifecycle = bootstrap_lifecycle()
        if exc_type is lifecycle.RunnerInterrupted:
            return "runner_interrupted"
        if exc_type is lifecycle.IncompleteCleanup:
            return "child_cleanup_unresolved"
    except Exception:
        pass
    return "unclassified"


def is_fixture_verified(fixture) -> bool:
    try:
        if fixture is None:
            return False
        base = fixture.base
        if not base.exists() or base.is_symlink() or not base.is_dir():
            return False
        if any(p.is_symlink() for p in (base, *base.parents)):
            return False
        private, artifacts = fixture.private, fixture.artifacts
        if not private.exists() or private.is_symlink() or not private.is_dir():
            return False
        if not artifacts.exists() or artifacts.is_symlink() or not artifacts.is_dir():
            return False
        if not fixture.override.exists() or fixture.override.is_symlink():
            return False
        if not fixture.env_file.exists() or fixture.env_file.is_symlink():
            return False
        if getattr(fixture, "artifacts_dev", 0) and getattr(fixture, "artifacts_ino", 0):
            st = artifacts.stat()
            if st.st_dev != fixture.artifacts_dev or st.st_ino != fixture.artifacts_ino:
                return False
        return True
    except Exception:
        return False


def validate_failure_report(report: dict) -> None:
    if type(report) is not dict:
        raise ValueError("unsafe_failure_report")
    required_keys = {
        "schema_version", "status", "phase", "error_type", "error_code",
        "http_accepted", "cleanup_verified",
        "primary_phase", "primary_error_code",
        "cleanup_step", "cleanup_error_code", "resource_kind",
    }
    if set(report.keys()) != required_keys:
        raise ValueError("unsafe_failure_report")
    schema_ver = report.get("schema_version")
    if type(schema_ver) is not int or schema_ver != 1:
        raise ValueError("unsafe_failure_report")
    for key in ("status", "phase", "error_type", "error_code"):
        val = report.get(key)
        if type(val) is not str:
            raise ValueError("unsafe_failure_report")
    if report.get("status") not in ALLOWED_FAILURE_STATUSES:
        raise ValueError("unsafe_failure_report")
    phase = report.get("phase")
    if phase not in ALLOWED_FAILURE_PHASES:
        raise ValueError("unsafe_failure_report")
    if report.get("error_type") not in ALLOWED_FAILURE_ERROR_TYPES:
        raise ValueError("unsafe_failure_report")
    if report.get("error_code") not in ALLOWED_FAILURE_ERROR_CODES:
        raise ValueError("unsafe_failure_report")
    if type(report.get("http_accepted")) is not bool or report.get("http_accepted") is not False:
        raise ValueError("unsafe_failure_report")
    if type(report.get("cleanup_verified")) is not bool:
        raise ValueError("unsafe_failure_report")

    primary_phase = report.get("primary_phase")
    primary_error_code = report.get("primary_error_code")
    cleanup_step = report.get("cleanup_step")
    cleanup_error_code = report.get("cleanup_error_code")
    resource_kind = report.get("resource_kind")

    for key, val in (
        ("primary_phase", primary_phase),
        ("primary_error_code", primary_error_code),
        ("cleanup_step", cleanup_step),
        ("cleanup_error_code", cleanup_error_code),
        ("resource_kind", resource_kind),
    ):
        if val is not None and type(val) is not str:
            raise ValueError("unsafe_failure_report")

    if phase == "cleanup":
        if report.get("cleanup_verified") is not False:
            raise ValueError("unsafe_failure_report")
        if cleanup_step is None or cleanup_step not in ALLOWED_CLEANUP_STEPS:
            raise ValueError("unsafe_failure_report")
        if cleanup_error_code is None or cleanup_error_code not in ALLOWED_FAILURE_ERROR_CODES:
            raise ValueError("unsafe_failure_report")
        if resource_kind is not None and resource_kind not in ALLOWED_RESOURCE_KINDS:
            raise ValueError("unsafe_failure_report")
        if resource_kind is not None and cleanup_step != "remove_resource":
            raise ValueError("unsafe_failure_report")
        if (primary_phase is None) != (primary_error_code is None):
            raise ValueError("unsafe_failure_report")
        if primary_phase is not None and primary_phase not in ALLOWED_FAILURE_PHASES:
            raise ValueError("unsafe_failure_report")
        if primary_error_code is not None and primary_error_code not in ALLOWED_FAILURE_ERROR_CODES:
            raise ValueError("unsafe_failure_report")
    else:
        if (
            primary_phase is not None
            or primary_error_code is not None
            or cleanup_step is not None
            or cleanup_error_code is not None
            or resource_kind is not None
        ):
            raise ValueError("unsafe_failure_report")

    assert_safe_data(report)


def sanitize_failure_phase(phase: str, code: str = "") -> str:
    if type(phase) is str and phase in ALLOWED_FAILURE_PHASES:
        return phase
    return "preflight"


def record_failure_report(
    fixture,
    phase: str,
    exc: BaseException,
    cleanup_verified: bool = False,
    primary_phase: str | None = None,
    primary_error_code: str | None = None,
    cleanup_step: str | None = None,
    cleanup_error_code: str | None = None,
    resource_kind: str | None = None,
) -> dict:
    safe_code = sanitize_failure_code(exc)
    safe_phase = sanitize_failure_phase(phase, safe_code)
    safe_type = sanitize_failure_type(exc)

    if safe_phase == "cleanup":
        safe_cleanup_step = cleanup_step if cleanup_step in ALLOWED_CLEANUP_STEPS else "reconcile_child_receipts"
        safe_cleanup_code = cleanup_error_code if cleanup_error_code in ALLOWED_FAILURE_ERROR_CODES else "unclassified"
        safe_resource_kind = resource_kind if resource_kind in ALLOWED_RESOURCE_KINDS else None
        if primary_phase and primary_phase in ALLOWED_FAILURE_PHASES:
            safe_primary_phase = primary_phase
            safe_primary_code = primary_error_code if primary_error_code in ALLOWED_FAILURE_ERROR_CODES else "unclassified"
        else:
            safe_primary_phase = None
            safe_primary_code = None
        safe_cleanup_verified = False
    else:
        safe_primary_phase = None
        safe_primary_code = None
        safe_cleanup_step = None
        safe_cleanup_code = None
        safe_resource_kind = None
        safe_cleanup_verified = True if cleanup_verified is True else False

    report = {
        "schema_version": 1,
        "status": "STARTUP_FAILED",
        "phase": safe_phase,
        "error_type": safe_type,
        "error_code": safe_code,
        "http_accepted": False,
        "cleanup_verified": safe_cleanup_verified,
        "primary_phase": safe_primary_phase,
        "primary_error_code": safe_primary_code,
        "cleanup_step": safe_cleanup_step,
        "cleanup_error_code": safe_cleanup_code,
        "resource_kind": safe_resource_kind,
    }
    validate_failure_report(report)
    if is_fixture_verified(fixture):
        try:
            durable_json(fixture.artifacts / "smoke-failure.json", report)
            if fixture.paper_p1 or fixture.paper_p2:
                path = fixture.artifacts / 'run.json'
                run = json.loads(path.read_text()) if path.exists() else {
                    'schema_version': 1, 'run_id': fixture.project, 'mode': fixture.paper_p1}
                run.update(status='FAIL', cleanup_verified=safe_cleanup_verified, error=safe_code)
                durable_json(path, run)
        except Exception:
            pass
    return report


def positive_summary(fixture, evidence=None, cleanup_counts=None):
    rep = {
        "schema_version": 1,
        "status": "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED",
        "project": fixture.project,
        "services": list(SERVICES),
        "pending_assertions": list(PENDING),
        "native_executed": True,
        "http_accepted": False,
        "exact_cleanup_verified": True,
        "positive_verifications": {
            "login_admin": True,
            "login_owner": True,
            "admin_inventory": True,
            "owner_sensor_create": True,
            "owner_sensor_retry": True,
            "database_guarantees_verified": True,
        },
        "evidence": evidence or {},
        "cleanup_counts": cleanup_counts or {"container": 0, "network": 0, "volume": 0},
    }
    return rep


def role_http_summary(fixture, evidence, cleanup_counts):
    role = evidence.get('role_http', {})
    expected = {c['label']: c['status'] for c in role_http_cases()}
    if role.get('role_cases') != expected or role.get('role_snapshot_comparisons') != 10:
        raise ValueError('role_acceptance_incomplete')
    return {"schema_version": 1, "status": "ROLE_HTTP_VERIFIED_NOT_JWT_ACCEPTED",
            "project": fixture.project, "services": list(SERVICES), "pending_assertions": list(PENDING),
            "native_executed": True, "http_accepted": False, "exact_cleanup_verified": True,
            "positive_verifications": {"login_admin": True, "login_owner": True, "admin_inventory": True,
                                       "owner_sensor_create": True, "owner_sensor_retry": True},
            "role_cases": expected, "role_snapshot_comparisons": 10,
            "evidence": {"positive_http_statuses": {
                "admin_inventory": evidence['positive_http']['admin_inventory_status'],
                "owner_sensor_create": evidence['positive_http']['owner_sensor_create_status'],
                "owner_sensor_retry": evidence['positive_http']['owner_sensor_retry_status'],
                "db_guarantees_verified": evidence['positive_http']['db_guarantees_verified']},
                         "role_case_count": 14,
                         "role_denials_verified": True}, "cleanup_counts": cleanup_counts}


def validate_summary(report):
    allowed_statuses = {"PREPARED_NOT_EXECUTED", "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED", "ROLE_HTTP_VERIFIED_NOT_JWT_ACCEPTED", "RBAC_JWT_HTTP_ACCEPTED"}
    if report.get("status") not in allowed_statuses or report.get("schema_version") != 1:
        raise ValueError("unsafe_summary")
    if not re.fullmatch(r"ncs_[a-f0-9]{12}", report.get("project", "")):
        raise ValueError("unsafe_summary")
    if report.get("services") != list(SERVICES) or report.get("pending_assertions") != list(PENDING):
        raise ValueError("unsafe_summary")
    if report["status"] == "PREPARED_NOT_EXECUTED":
        if report.get("native_executed") is not False:
            raise ValueError("unsafe_summary")
        if set(report) != {"schema_version", "status", "project", "services", "pending_assertions", "native_executed"}:
            raise ValueError("unsafe_summary")
    elif report["status"] == "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED":
        if report.get("native_executed") is not True or report.get("http_accepted") is not False:
            raise ValueError("unsafe_summary")
        if not report.get("exact_cleanup_verified"):
            raise ValueError("unsafe_summary")
        required_keys = {"schema_version", "status", "project", "services", "pending_assertions",
                         "native_executed", "http_accepted", "exact_cleanup_verified",
                         "positive_verifications", "evidence", "cleanup_counts",
                         "backend_image_id", "phase"}
        if set(report) != required_keys:
            raise ValueError("unsafe_summary")
        assert_safe_data(report["evidence"])
    elif report["status"] == "ROLE_HTTP_VERIFIED_NOT_JWT_ACCEPTED":
        expected = {c['label']: c['status'] for c in role_http_cases()}
        if (report.get('http_accepted') is not False or report.get('native_executed') is not True
                or report.get('role_cases') != expected or report.get('role_snapshot_comparisons') != 10
                or report.get('exact_cleanup_verified') is not True):
            raise ValueError('unsafe_summary')
        assert_safe_data(report.get('evidence', {}))
    elif report["status"] == "RBAC_JWT_HTTP_ACCEPTED":
        if (report.get("http_accepted") is not True or report.get("native_executed") is not True
                or report.get("exact_cleanup_verified") is not True
                or report.get("role_cases") != {c["label"]: c["status"] for c in role_http_cases()}
                or report.get("jwt_cases") != {c["label"]: c["status"] for c in jwt_http_cases()}
                or report.get("role_snapshot_comparisons") != 10
                or report.get("jwt_snapshot_comparisons") != 6
                or report.get("cleanup_counts") != {"container": 10, "network": 1, "volume": 3}):
            raise ValueError("unsafe_summary")
        assert_safe_data(report.get("evidence", {}))


@dataclass(frozen=True)
class HTTPSEndpoint:
    url: str
    ca_file: Path


def http_request(base_url: str | HTTPSEndpoint, method: str, path: str, *, token: str = "", api_key: str = "",
                 headers: dict | None = None, body: dict | None = None, timeout: float = 10.0) -> tuple[int, dict, dict | str]:
    parsed_url = urllib.parse.urlparse(base_url.url if isinstance(base_url, HTTPSEndpoint) else base_url)
    host = parsed_url.hostname or "127.0.0.1"
    port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)
    if parsed_url.scheme == 'https':
        context = ssl.create_default_context(cafile=str(base_url.ca_file)) if isinstance(base_url, HTTPSEndpoint) else ssl.create_default_context()
        conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=context)
    else:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    if token:
        req_headers["Authorization"] = "Bearer " + token
    if api_key:
        req_headers["apikey"] = api_key
    payload = json.dumps(body).encode() if body is not None else None
    try:
        conn.request(method, path, body=payload, headers=req_headers)
        response = conn.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError("http_body_too_large")
        resp_headers = dict(response.getheaders())
        try:
            parsed_body = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
        except json.JSONDecodeError:
            parsed_body = raw.decode('utf-8', errors='replace')
        return response.status, resp_headers, parsed_body
    except (OSError, http.client.HTTPException) as exc:
        raise ValueError(f"http_request_failed: {type(exc).__name__}") from None
    finally:
        conn.close()


def create_user_via_admin_api(base_url: str, service_key: str, email: str, password: str, timeout: float = 10.0) -> str:
    status, _, body = http_request(base_url, "POST", "/auth/v1/admin/users",
                                   token=service_key, api_key=service_key,
                                   body={"email": email, "password": password, "email_confirm": True},
                                   timeout=timeout)
    if status != 200 or not isinstance(body, dict) or not body.get("id"):
        raise ValueError("admin_user_creation_failed")
    uid = body["id"]
    if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", uid):
        raise ValueError("admin_user_uuid_invalid")
    return uid


def login_user_for_token(base_url: str, anon_key: str, email: str, password: str, timeout: float = 10.0) -> str:
    status, _, body = http_request(base_url, "POST", "/auth/v1/token?grant_type=password",
                                   api_key=anon_key, body={"email": email, "password": password},
                                   timeout=timeout)
    assert_login(status, body if isinstance(body, dict) else {})
    token = body["access_token"]
    # Verify claims without retaining token in logs or exceptions
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("token_format_invalid")
    padding = b"=" * (-len(parts[1]) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(parts[1].encode() + padding))
    except Exception:
        raise ValueError("token_claims_decode_failed") from None
    if claims.get("role") != "authenticated" or claims.get("aud") != "authenticated":
        raise ValueError("token_claims_mismatch")
    return token


def db_psql(registry, postgres_cid: str, sql_commands: str, variables=None) -> str:
    args = ["exec", "-i", postgres_cid, "psql", "-U", "postgres", "-d", "smoke", "-v", "ON_ERROR_STOP=1", "-At"]
    for key, value in (variables or {}).items():
        if (not re.fullmatch(r"[a-z_]+", key) or not isinstance(value, str)
                or len(value) > 256 or any(ord(ch) < 32 or ord(ch) == 127 for ch in value)):
            raise ValueError("db_variable_invalid")
        args += ["-v", key + "=" + value]
    if variables:
        result = registry.command(args + ["-f", "-"], input=sql_commands.encode())
    else:
        result = registry.command(args + ["-c", sql_commands])
    if result.returncode:
        diagnostic = getattr(registry, "fixture", None)
        if diagnostic is not None:
            try:
                private_write(diagnostic.private / ("db-error-" + secrets.token_hex(6) + ".txt"),
                             result.stderr.decode(errors="replace")[:4096])
            except OSError:
                pass
        raise ValueError("db_psql_execution_failed")
    return result.stdout.decode()


def assert_unchanged(before, after):
    """Compare complete ordered row images for every protected table."""
    required = {"sensors", "twin_entities", "twin_relationships", "twin_states"}
    if (not isinstance(before, dict) or not isinstance(after, dict)
            or set(before) != required or set(after) != required
            or before != after):
        raise ValueError("denial_mutated_database")
    return True


def denial_snapshot(registry, postgres_cid, gateway_id, sensor_id):
    if not all(isinstance(v, str) and re.fullmatch(r"[A-Za-z0-9_:-]+", v) for v in (gateway_id, sensor_id)):
        raise ValueError("fixture_identifier_invalid")
    sql = """
SELECT json_build_object(
 'sensors', COALESCE((SELECT json_agg(to_jsonb(x) ORDER BY x.gateway_id,x.sensor_id) FROM public.sensors x WHERE x.gateway_id=:'gateway_id' AND x.sensor_id=:'sensor_id'),'[]'::json),
 'twin_entities', COALESCE((SELECT json_agg(to_jsonb(x) ORDER BY x.entity_id) FROM public.twin_entities x WHERE x.gateway_id=:'gateway_id' OR x.entity_id LIKE 'urn:ngsi-ld:Sensor:'||:'gateway_id'||':%'),'[]'::json),
 'twin_relationships', COALESCE((SELECT json_agg(to_jsonb(r) ORDER BY r.source_entity_id,r.relationship_type,r.target_entity_id) FROM public.twin_relationships r WHERE r.source_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id=:'gateway_id') OR r.target_entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id=:'gateway_id')),'[]'::json)
 , 'twin_states', COALESCE((SELECT json_agg(to_jsonb(s) ORDER BY s.entity_id) FROM public.twin_states s WHERE s.entity_id IN (SELECT id FROM public.twin_entities WHERE gateway_id=:'gateway_id' OR entity_id LIKE 'urn:ngsi-ld:Sensor:'||:'gateway_id'||':%')),'[]'::json)
)::text;
"""
    raw = db_psql(registry, postgres_cid, sql, {"gateway_id": gateway_id, "sensor_id": sensor_id}).strip()
    try:
        result = json.loads(raw)
    except (ValueError, TypeError):
        raise ValueError("denial_snapshot_invalid") from None
    if not isinstance(result, dict) or set(result) != {"sensors", "twin_entities", "twin_relationships", "twin_states"}:
        raise ValueError("denial_snapshot_invalid")
    return result


def acceptance_summary(fixture, cases, cleanup_counts):
    required = {case[0] + ":" + case[3] for case in ACCEPTANCE if case[0] != "login"}
    if (not isinstance(cases, dict) or set(cases) != required
            or any(not isinstance(status, int) for status in cases.values())):
        raise ValueError("acceptance_incomplete")
    return {"schema_version": 1, "status": "RBAC_JWT_HTTP_ACCEPTED",
            "project": fixture.project, "services": list(SERVICES),
            "native_executed": True, "http_accepted": True,
            "exact_cleanup_verified": True, "cases": cases,
            "cleanup_counts": cleanup_counts}


def run_positive_http_slice(fixture, registry, endpoint: str, postgres_cid: str) -> dict:
    service_key = fixture.values["SERVICE_ROLE_KEY"]
    anon_key = fixture.values["ANON_KEY"]

    admin_email = f"admin_{secrets.token_hex(6)}@example.invalid"
    admin_pwd = secrets.token_urlsafe(24)
    owner_email = f"owner_{secrets.token_hex(6)}@example.invalid"
    owner_pwd = secrets.token_urlsafe(24)

    admin_uid = create_user_via_admin_api(endpoint, service_key, admin_email, admin_pwd)
    owner_uid = create_user_via_admin_api(endpoint, service_key, owner_email, owner_pwd)

    gw_id = "gw_" + secrets.token_hex(6)
    gw_name = "Positive Smoke Gateway " + secrets.token_hex(4)
    gw_entity_id = "urn:ngsi-ld:Gateway:" + gw_id
    gw_uuid = str(secrets.token_hex(16))
    gw_uuid = f"{gw_uuid[:8]}-{gw_uuid[8:12]}-{gw_uuid[12:16]}-{gw_uuid[16:20]}-{gw_uuid[20:]}"

    seed_sql = f"""
INSERT INTO public.profiles (id, full_name) VALUES ('{admin_uid}', 'Smoke Admin') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.profiles (id, full_name) VALUES ('{owner_uid}', 'Smoke Owner') ON CONFLICT (id) DO NOTHING;
INSERT INTO public.platform_admins (user_id) VALUES ('{admin_uid}');
INSERT INTO public.gateways (gateway_id, name, description) VALUES ('{gw_id}', '{gw_name}', 'Positive Smoke Seed');
INSERT INTO public.user_gateways (user_id, gateway_id, role) VALUES ('{owner_uid}', '{gw_id}', 'owner');
INSERT INTO public.twin_entities (id, entity_id, entity_type, name, attributes, gateway_id)
VALUES ('{gw_uuid}', '{gw_entity_id}', 'Gateway', '{gw_name}', '{{}}'::jsonb, '{gw_id}');
INSERT INTO public.twin_states (entity_id) VALUES ('{gw_uuid}');
"""
    db_psql(registry, postgres_cid, seed_sql)

    admin_token = login_user_for_token(endpoint, anon_key, admin_email, admin_pwd)
    owner_token = login_user_for_token(endpoint, anon_key, owner_email, owner_pwd)

    inv_status, inv_headers, inv_body = http_request(endpoint, "GET", "/v1/admin/gateways", token=admin_token)
    assert_response("inventory", inv_status, inv_body)
    items = inv_body.get("items", [])
    seeded_item = next((it for it in items if it.get("gateway_id") == gw_id), None)
    if not seeded_item or seeded_item.get("name") != gw_name:
        raise ValueError("admin_inventory_row_mismatch")

    sensor_id = "temp_sensor_" + secrets.token_hex(4)
    sensor_path = f"/v1/gateways/{gw_id}/sensors/{sensor_id}"
    sensor_payload = {"name": "Temperature Sensor", "unit": "C"}

    create_status, create_headers, create_body = http_request(endpoint, "PUT", sensor_path, token=owner_token, body=sensor_payload)
    assert_response("sensor_create", create_status, create_body)
    if create_body.get("gateway_id") != gw_id or create_body.get("sensor_id") != sensor_id:
        raise ValueError("sensor_create_body_mismatch")
    expected_entity = f"urn:ngsi-ld:Sensor:{gw_id}:{sensor_id}"
    if create_body.get("entity_id") != expected_entity:
        raise ValueError("sensor_create_entity_mismatch")

    retry_status, retry_headers, retry_body = http_request(endpoint, "PUT", sensor_path, token=owner_token, body=sensor_payload)
    assert_response("sensor_retry", retry_status, retry_body)
    if retry_body.get("gateway_id") != gw_id or retry_body.get("sensor_id") != sensor_id:
        raise ValueError("sensor_retry_body_mismatch")

    chk_sensor_sql = f"SELECT count(*) FROM public.sensors WHERE gateway_id='{gw_id}' AND sensor_id='{sensor_id}' AND name='{sensor_payload['name']}';"
    cnt = db_psql(registry, postgres_cid, chk_sensor_sql).strip()
    if cnt != "1":
        raise ValueError("db_sensor_row_count_mismatch")

    chk_twin_sql = f"SELECT count(*) FROM public.twin_entities WHERE entity_id='{expected_entity}' AND entity_type='Sensor' AND gateway_id='{gw_id}';"
    cnt = db_psql(registry, postgres_cid, chk_twin_sql).strip()
    if cnt != "1":
        raise ValueError("db_twin_entity_count_mismatch")

    chk_rel_sql = f"SELECT count(*) FROM public.twin_relationships r JOIN public.twin_entities e ON e.id = r.target_entity_id WHERE r.source_entity_id = '{gw_uuid}' AND r.relationship_type = 'hasSensor' AND e.entity_id = '{expected_entity}';"
    cnt = db_psql(registry, postgres_cid, chk_rel_sql).strip()
    if cnt != "1":
        raise ValueError("db_twin_relationship_count_mismatch")

    evidence = {
        "admin_inventory_status": inv_status,
        "owner_sensor_create_status": create_status,
        "owner_sensor_retry_status": retry_status,
        "db_guarantees_verified": True,
        "gateway_id_tested": gw_id,
        "sensor_id_tested": sensor_id,
    }
    return evidence


def role_http_cases():
    cases = []
    for role in ("owner", "operator", "viewer", "nonmember"):
        cases.append({"label": "inventory_" + role, "role": role, "method": "GET", "target": "inventory", "status": 403})
    for role, status in (("operator", 403), ("viewer", 403), ("nonmember", 404),
                         ("cross_gateway_owner", 404), ("platform_admin_without_membership", 404)):
        for kind in ("new", "existing"):
            cases.append({"label": role + "_sensor_" + kind, "role": role, "method": "PUT",
                          "target": "sensor", "sensor_kind": kind, "status": status})
    return tuple(cases)


def jwt_http_cases():
    cases = []
    for condition in ("missing", "invalid", "expired"):
        cases.append({"label": "inventory_" + condition, "condition": condition,
                      "method": "GET", "sensor_kind": None, "status": 401})
        for kind in ("new", "existing"):
            cases.append({"label": "sensor_" + kind + "_" + condition, "condition": condition,
                          "method": "PUT", "sensor_kind": kind, "status": 401})
    return tuple(cases)


def generate_jwt_denial_tokens(fixture):
    secret = fixture.values["JWT_SECRET"]
    now = int(time.time())
    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")
    def sign(claims):
        body = encode({"alg": "HS256", "typ": "JWT"}) + "." + encode(claims)
        sig = base64.urlsafe_b64encode(hmac.new(secret.encode(), body.encode(), hashlib.sha256).digest()).decode().rstrip("=")
        return body + "." + sig
    uid = "00000000-0000-4000-8000-000000000001"
    claims = {"role": "authenticated", "aud": "authenticated", "iss": fixture.values["GOTRUE_JWT_ISSUER"],
              "sub": uid, "iat": now - 7200, "exp": now - 3600}
    expired = sign(claims)
    parts = expired.split(".")
    sig = ("A" if parts[2][0] != "A" else "B") + parts[2][1:]
    return {"missing": "", "invalid": ".".join(parts[:2] + [sig]), "expired": expired}


def run_jwt_denial_slice(fixture, registry, endpoint, postgres_cid, gateway_id, existing_sensor):
    tokens = generate_jwt_denial_tokens(fixture)
    outcomes, snapshots = {}, 0
    new_base = "jwt_new_" + secrets.token_hex(4)
    for case in jwt_http_cases():
        token = tokens[case["condition"]]
        if case["method"] == "GET":
            status, _, body = http_request(endpoint, "GET", "/v1/admin/gateways", token=token)
        else:
            sensor = existing_sensor if case["sensor_kind"] == "existing" else new_base + "_" + case["condition"]
            path = f"/v1/gateways/{gateway_id}/sensors/{sensor}"
            before = denial_snapshot(registry, postgres_cid, gateway_id, sensor)
            status, _, body = http_request(endpoint, "PUT", path, token=token,
                                           body={"name": "Denied JWT mutation", "unit": "K"})
        assert_response("unauthorized", status, body)
        outcomes[case["label"]] = status
        if case["method"] == "PUT":
            after = denial_snapshot(registry, postgres_cid, gateway_id, sensor)
            assert_unchanged(before, after)
            snapshots += 1
        durable_json(fixture.private / "jwt-http-progress.json", {"completed": outcomes,
                                                                    "snapshot_comparisons": snapshots})
    return {"jwt_cases": outcomes, "jwt_case_count": len(outcomes),
            "jwt_snapshot_comparisons": snapshots, "jwt_denials_verified": True}


def full_http_summary(fixture, evidence, cleanup_counts):
    role = evidence.get("role_http", {})
    jwt = evidence.get("jwt_http", {})
    expected_roles = {c["label"]: c["status"] for c in role_http_cases()}
    expected_jwt = {c["label"]: c["status"] for c in jwt_http_cases()}
    positive = evidence.get("positive_http", {})
    if (role.get("role_cases") != expected_roles or role.get("role_snapshot_comparisons") != 10):
        raise ValueError("role_acceptance_incomplete")
    if jwt.get("jwt_cases") != expected_jwt or jwt.get("jwt_snapshot_comparisons") != 6:
        raise ValueError("jwt_acceptance_incomplete")
    if (positive.get("admin_inventory_status") != 200 or positive.get("owner_sensor_create_status") != 201
            or positive.get("owner_sensor_retry_status") != 200 or positive.get("db_guarantees_verified") is not True):
        raise ValueError("positive_acceptance_incomplete")
    if cleanup_counts != {"container": 10, "network": 1, "volume": 3}:
        raise ValueError("cleanup_acceptance_incomplete")
    return {"schema_version": 1, "status": "RBAC_JWT_HTTP_ACCEPTED", "project": fixture.project,
            "services": list(SERVICES), "pending_assertions": list(PENDING), "native_executed": True, "http_accepted": True,
            "exact_cleanup_verified": True, "positive_verifications": {"login_admin": True,
            "login_owner": True, "admin_inventory": True, "owner_sensor_create": True,
            "owner_sensor_retry": True}, "role_cases": expected_roles, "role_snapshot_comparisons": 10,
            "jwt_cases": expected_jwt, "jwt_snapshot_comparisons": 6,
            "evidence": {"positive_http_statuses": {k: positive[k] for k in
            ("admin_inventory_status", "owner_sensor_create_status", "owner_sensor_retry_status")},
            "role_case_count": 14, "auth_denial_case_count": 9}, "cleanup_counts": cleanup_counts}


P1_COUNTERS = ('Received', 'ReceivedBytes', 'AcceptedBytes', 'Accepted', 'RejectedNotRunning',
    'RejectedStopping', 'RejectedRetained', 'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
    'Processed', 'ValidationRejected', 'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
    'ACKBuildFailures', 'Committed', 'DuplicateCommitted', 'SamplesNew', 'ACKPublished', 'ACKPublishFailures')
P1_GAUGES = ('QueueDepth', 'QueueCapacity', 'QueueHighWater', 'InFlight')
P1_RATES = {'received_delivery_msg_per_s': 'Received', 'new_committed_msg_per_s': 'Committed',
            'new_committed_samples_per_s': 'SamplesNew'}


def paper_p1_rates(before, after):
    rates = dict.fromkeys(P1_RATES)
    if not before or not after or before['process_instance'] != after['process_instance']:
        return rates
    elapsed = after['elapsed_seconds'] - before['elapsed_seconds']
    if elapsed <= 0 or not before.get('ingress') or not after.get('ingress'):
        return rates
    delta = {key: after['ingress'][field] - before['ingress'][field] for key, field in P1_RATES.items()}
    if any(value < 0 for value in delta.values()):
        return rates
    return {key: value / elapsed for key, value in delta.items()}


def paper_p1_metrics_csv(path, samples):
    import csv
    fields = ('schema_version', 'timestamp_utc', 'elapsed_seconds', 'process_instance', 'phase',
              'group', 'metric', 'kind', 'unit', 'value', 'delta', 'rate_per_s', 'availability', 'error')
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        previous = None
        for sample in samples:
            snap = sample['snapshot']
            base = {'schema_version': 1, 'phase': sample['phase']}
            if snap is None:
                writer.writerow(dict(base, error=sample.get('error', 'scrape_failed')))
                previous = None
                continue
            base.update(timestamp_utc=snap['observed_at'], elapsed_seconds=snap['elapsed_seconds'],
                        process_instance=snap['process_instance'])
            rates = paper_p1_rates(previous, snap)
            valid_delta = all(v is not None for v in rates.values())
            def row(group, metric, kind, unit, value, **extra):
                if value is not None and (type(value) not in (int, float) or not math.isfinite(value)):
                    raise ValueError('paper_p1_snapshot_rejected')
                writer.writerow(dict(base, group=group, metric=metric, kind=kind, unit=unit,
                                     value=value, availability='available', **extra))
            ing = snap.get('ingress') or {}
            for name in P1_COUNTERS + P1_GAUGES:
                if name not in ing:
                    continue
                delta = ing[name] - previous['ingress'][name] if valid_delta and name in previous['ingress'] and name in P1_COUNTERS else None
                row('ingress', name, 'counter' if name in P1_COUNTERS else 'gauge',
                    'bytes' if name.endswith('Bytes') else 'count', ing[name], delta=delta,
                    rate_per_s=delta / (snap['elapsed_seconds'] - previous['elapsed_seconds']) if delta is not None and delta >= 0 else None)
            for name, value in rates.items():
                row('ingress', name, 'rate', 'samples/s' if 'samples' in name else 'messages/s', value)
            histograms = [('ingress', key, value) for key, value in ing.get('Durations', {}).items()
                          if key in ('queue_wait', 'validation', 'persistence', 'ack_publish')]
            auth = snap.get('auth', {})
            for name in ('allow', 'deny'):
                if name in auth:
                    row('auth', name, 'counter', 'count', auth[name])
            if 'verify' in auth:
                histograms.append(('auth', 'verify', auth['verify']))
            unix = snap.get('unix_auth') or {}
            for name in ('allow', 'deny'):
                if name in unix:
                    row('unix_auth', name, 'counter', 'count', unix[name])
            if 'verify' in unix:
                histograms.append(('unix_auth', 'verify', unix['verify']))
            for name in ('unix_auth_handler', 'client_https'):
                if snap.get(name) is not None:
                    histograms.append((name, 'duration', snap[name]))
            cache = snap.get('jwks_cache') or {}
            for name in ('hits', 'misses', 'refresh_success', 'refresh_failure'):
                if name in cache:
                    row('jwks_cache', name, 'counter', 'count', cache[name])
            for route in snap.get('http', []):
                # Export only router templates, never a sender-supplied URI.
                key = route['route']
                if route['scope'] not in ('business', 'setup', 'scrape', 'health'):
                    raise ValueError('paper_p1_snapshot_rejected')
                if key != 'unmatched' and not re.fullmatch(r'(GET|POST|PUT|PATCH|DELETE) /(healthz|readyz|v1/[a-z_/:-]+)', key):
                    raise ValueError('paper_p1_snapshot_rejected')
                histograms.append(('http_' + route['scope'], key, route['duration']))
            for group, name, h in histograms:
                row(group, name + '.count', 'histogram', 'count', h['count'])
                row(group, name + '.sum', 'histogram', 'seconds', h['sum_seconds'])
                row(group, name + '.overflow', 'histogram', 'count', h['overflow'])
                for upper, count in zip(h['upper_seconds'], h['buckets']):
                    row(group, name + '.bucket_upper_ms_' + str(upper * 1000), 'histogram_bucket', 'count', count)
            for key in ('acquired', 'idle', 'total', 'max', 'acquire_count', 'empty_acquire_count', 'canceled_acquire_count', 'acquire_seconds'):
                if key in snap.get('db', {}):
                    row('db', key, 'pool_stat', 'seconds' if key.endswith('seconds') else 'count', snap['db'][key])
            for key in ('ingress', 'http', 'auth', 'db', 'command', 'dispatch_e2e', 'result', 'batch_wait', 'websocket'):
                status = snap.get('availability', {}).get(key)
                if status in ('available', 'disabled', 'not_implemented'):
                    writer.writerow(dict(base, group=key, metric='availability', availability=status))
            previous = snap


def paper_p0_quota(host):
    nano = host.get('NanoCpus', 0)
    period, quota = host.get('CpuPeriod', 0), host.get('CpuQuota', 0)
    if (not (nano == 2000000000 or nano == 0 and period > 0 and quota == 2 * period)
            or host.get('Memory') != 4294967296):
        raise ValueError('paper_p0_quota_rejected')
    return {'cpus': 2, 'memory_bytes': 4294967296}


def paper_p0_stats(registry, containers, phase):
    roles = [n for n in fixture_services(registry.fixture) if n not in JOBS]
    started = datetime.now(timezone.utc).isoformat()
    ids = [containers[n]['receipt']['id'] for n in roles]
    result = registry.command(['stats', '--no-stream', '--format', '{{json .}}', *ids])
    if result.returncode:
        raise ValueError('paper_p0_stats_failed')
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    if len(rows) != len(roles):
        raise ValueError('paper_p0_stats_failed')
    by_name = {r['Name']: r for r in rows}
    return {'phase': phase, 'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
            'services': {n: {k: by_name[containers[n]['name']][k]
                             for k in ('CPUPerc', 'MemUsage', 'MemPerc')}
                         for n in roles}}


def paper_p0_gateway_cli(fixture, registry, image, volume, env_file, ca_file, tag, args,
                         state, during=None, tick=None, timeout=45):
    """Finite Gateway CLI under the existing exact-owned registry, no --rm leaks."""
    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    item = registry.intent('container', fixture.project + '-gateway-' + tag, image, labels)
    if registry.resolve(item) is not None:
        raise ValueError('resource_collision')
    mounts = [('/data/outbox', 'volume', volume['name'], True),
              ('/data/ca/ca.crt', 'bind', str(ca_file), False)]
    tmpfs = {p: 'rw,noexec,nosuid,nodev,size=8m,mode=0700,uid=1883,gid=1883'
             for p in ('/tmp', '/mosquitto/data', '/mosquitto/log')}
    item['expectation'] = {'mounts': mounts, 'tmpfs': tmpfs, 'user': '1883:1883',
        'readonly': True, 'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'],
        'network': fixture.project + '_iot_net'}
    registry.save(item)
    command = ['create', '--pull=never', '--name', item['name'], '--network', fixture.project + '_iot_net',
               '--user', '1883:1883', '--read-only', '--cap-drop', 'ALL',
               '--security-opt', 'no-new-privileges:true', '--env-file', str(env_file)]
    for key, value in labels.items():
        command += ['--label', key + '=' + value]
    for path, options in tmpfs.items():
        command += ['--tmpfs', path + ':' + options]
    command += ['--mount', 'type=volume,src=' + volume['name'] + ',dst=/data/outbox',
                '--mount', 'type=bind,src=' + str(ca_file) + ',dst=/data/ca/ca.crt,readonly', image, *args]
    registry.allocating(item)
    result = registry.command(command)
    obj = registry.resolve(item)
    if result.returncode or obj is None or result.stdout.decode().strip() != obj['Id']:
        raise ValueError('helper_allocation_unresolved')
    if registry.command(['start', obj['Id']]).returncode:
        raise ValueError('paper_p0_gateway_failed')
    if during:
        during()
    deadline = time.monotonic() + timeout
    while True:
        state.checkpoint()
        obj = registry.resolve(item)
        if not obj['State']['Running']:
            break
        if tick:
            tick()
        if time.monotonic() >= deadline:
            raise ValueError('paper_p0_gateway_failed')
        time.sleep(0.2)
    output = registry.command(['logs', obj['Id']])
    private_write(fixture.private / ('gateway-' + tag + '.log'),
                  (output.stdout + output.stderr).decode(errors='replace'))
    if obj['State']['ExitCode'] or output.returncode:
        raise ValueError('paper_p0_gateway_failed')
    return output.stdout.decode()


def validate_paper_p0_evidence(evidence):
    if (evidence.get('checks') != {'login_users': 3, 'gateway_graphs': 2, 'raw_samples': 200,
            'ledgers': 2, 'temporal_observations': 2, 'application_acks': 2,
            'empty_outboxes': 2, 'cross_gateway_status': 404, 'positive_bracket': [200, 200],
            'http_raw_points': 100, 'storage_status': 200}
            or evidence.get('backend_quota') != {'cpus': 2, 'memory_bytes': 4294967296}
            or len(evidence.get('snapshots', [])) != 4):
        raise ValueError('paper_p0_acceptance_incomplete')


def paper_p0_git(fixture, *args, runner=subprocess.run):
    """Read metadata from this trusted source root, never change Git config."""
    if args not in {('rev-parse', 'HEAD'), ('branch', '--show-current'), ('status', '--porcelain')}:
        raise ValueError('paper_p0_metadata_failed')
    command = ['git', '--no-optional-locks', '-c', 'safe.directory=' + str(ROOT),
               '-C', str(ROOT), *args]
    try:
        result = runner(command, capture_output=True, timeout=10,
                        env=process_env(fixture), check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise ValueError('paper_p0_metadata_failed') from None
    if result.returncode:
        raise ValueError('paper_p0_metadata_failed')
    return result.stdout.decode().strip()


def paper_p0_temporal_query(first, last):
    """Match the existing /temporal UTC-Z input contract, without changing Go."""
    start = datetime.fromisoformat(first) - timedelta(seconds=1)
    end = datetime.fromisoformat(last) + timedelta(seconds=1)
    if start.utcoffset() is None or end.utcoffset() is None:
        raise ValueError('paper_p0_read_failed')
    def utc_z(value):
        return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
    return urllib.parse.urlencode({'attrs': 'measurement', 'timerel': 'between',
                                  'timeAt': utc_z(start), 'endTimeAt': utc_z(end)})


def prepare_paper_workload(fixture, registry, endpoint, containers, images, state):
    # Reuse the Stage 3 synthetic profile and independent SQL oracle on THIS DB.
    # No other runner/main is invoked and no sentinel desired/config is seeded.
    from stage3_uplink_runner import default_specs

    pg = containers['postgres']['receipt']['id']
    backend = registry.resolve(containers['backend'])
    quota = paper_p0_quota(backend['HostConfig'])
    effective = json.loads((fixture.private / 'effective.json').read_text())
    service_resources = {}
    for name, item in containers.items():
        if name in JOBS:
            continue
        host = registry.resolve(item)['HostConfig']
        service_resources[name] = {k: host.get(k, 0) for k in
                                  ('NanoCpus', 'CpuQuota', 'CpuPeriod', 'Memory')}
    durable_json(fixture.private / 'paper-p0-resources.json',
                 {'backend_quota': quota, 'service_resources': service_resources})
    storage_status, _, storage_body = http_request(endpoint, 'GET', '/storage/v1/bucket',
        token=fixture.values['SERVICE_ROLE_KEY'], api_key=fixture.values['SERVICE_ROLE_KEY'])
    if storage_status != 200 or not isinstance(storage_body, list):
        raise ValueError('paper_p0_storage_failed')
    if http_request(endpoint, 'GET', '/readyz')[0] != 200:
        raise ValueError('health_failed')

    users, tokens = {}, {}
    for role in ('admin', 'a', 'b'):
        state.checkpoint()
        email, password = role + '_' + secrets.token_hex(6) + '@example.invalid', secrets.token_urlsafe(24)
        users[role] = create_user_via_admin_api(endpoint, fixture.values['SERVICE_ROLE_KEY'], email, password)
        tokens[role] = login_user_for_token(endpoint, fixture.values['ANON_KEY'], email, password)
    db_psql(registry, pg, "INSERT INTO platform_admins(user_id) VALUES (:'uid');", {'uid': users['admin']})
    specs = [replace(s, gateway_id='gw_p0_' + role + '_' + secrets.token_hex(4),
                     gateway_reported={}, gateway_desired={}, gateway_reported_version=0,
                     gateway_desired_version=0, sensor_desired={}, sensor_desired_version=0)
             for role, s in zip(('a', 'b'), default_specs())]
    passwords = []
    for role, spec in zip(('a', 'b'), specs):
        state.checkpoint()
        status, _, body = http_request(endpoint, 'POST', '/v1/digital-twins', token=tokens['admin'],
            body={'entity_type': 'Gateway', 'gateway_id': spec.gateway_id,
                  'name': spec.gateway_name, 'owner_user_id': users[role]})
        if status != 201 or body.get('entity_id') != spec.gateway_urn:
            raise ValueError('paper_p0_seed_failed')
        status, _, body = http_request(endpoint, 'PUT',
            f'/v1/gateways/{spec.gateway_id}/sensors/{spec.sensor_id}', token=tokens[role],
            body={'name': spec.sensor_name, 'unit': spec.sensor_unit})
        if status != 201 or body.get('entity_id') != spec.sensor_urn:
            raise ValueError('paper_p0_seed_failed')
        status, headers, body = http_request(endpoint, 'POST',
            f'/v1/admin/gateways/{spec.gateway_id}/mqtt-credential', token=tokens['admin'],
            headers={'Idempotency-Key': secrets.token_hex(16)}, timeout=40)
        if (status != 201 or headers.get('Cache-Control') != 'no-store'
                or body.get('status') != 'active' or body.get('username') != spec.gateway_id
                or not body.get('password')):
            raise ValueError('paper_p0_seed_failed')
        passwords.append(body['password'])
    sensor_path = '/v1/digital-twins/' + urllib.parse.quote(specs[0].sensor_urn, safe=':')
    status, _, body = http_request(endpoint, 'GET', sensor_path, token=tokens['a'])
    durable_json(fixture.private / 'paper-p0-read-progress.json',
                 {'step': 'initial_sensor_detail', 'http_status': status})
    if status != 200 or body.get('id') != specs[0].sensor_urn:
        raise ValueError('paper_p0_read_failed')
    return pg, quota, effective, service_resources, storage_status, tokens, specs, passwords, sensor_path


def run_paper_p0(fixture, registry, endpoint, containers, images, state):
    from stage3_uplink_runner import simulator_envelope, build_gateway_assert_sql, synthetic_value, gateway_env
    from stage3_mqtt_ingress_runner import parse_run_summary
    pg, quota, effective, service_resources, storage_status, tokens, specs, passwords, sensor_path = prepare_paper_workload(
        fixture, registry, endpoint, containers, images, state)

    snapshots = [paper_p0_stats(registry, containers, 'ready_before_uplink')]
    durable_json(fixture.private / 'paper-p0-observations.json', snapshots)
    messages = []
    for role, spec, password in zip(('a', 'b'), specs, passwords):
        state.checkpoint()
        labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
        volume = registry.intent('volume', fixture.project + '_outbox_' + role, None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        result = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, volume['name']])
        if result.returncode or registry.resolve(volume) is None:
            raise ValueError('helper_allocation_unresolved')
        env = gateway_env(spec.gateway_id, password)
        env['GATEWAY_MQTT_BROKER_URL'] = 'ssl://mosquitto:8883'
        env_file = fixture.private / ('gateway-' + role + '.env')
        private_write(env_file, ''.join(k + '=' + v + '\n' for k, v in env.items()))
        out = paper_p0_gateway_cli(fixture, registry, images['gateway'], volume, env_file,
            fixture.private / 'certs/ca.crt', role, ['run', '--batches', '1', '--completion-timeout', '30s'], state,
            during=lambda: snapshots.append(paper_p0_stats(registry, containers, 'gateway_' + role + '_run')))
        durable_json(fixture.private / 'paper-p0-observations.json', snapshots)
        summary = parse_run_summary(out)
        expected_summary = {'completed': 'true', 'enqueued': '1', 'app_acknowledged': '1',
                            'pending_rows': '0', 'pending_bytes': '0', 'quarantined_rows': '0', 'quarantined_bytes': '0'}
        if any(summary.get(k) != v for k, v in expected_summary.items()):
            raise ValueError('paper_p0_gateway_failed')
        count = paper_p0_gateway_cli(fixture, registry, images['gateway'], volume, env_file,
            fixture.private / 'certs/ca.crt', role + '-count', ['count'], state)
        if count.strip() != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
            raise ValueError('paper_p0_gateway_failed')
        # The shared assertion verifies per-message raw provenance, values,
        # sample timestamps, ledger, temporal and reported/desired versions.
        db_psql(registry, pg, build_gateway_assert_sql(spec, simulator_envelope(), 'paper_p0_batch'))
        info = json.loads(db_psql(registry, pg, """
SELECT json_build_object('gateway_id', gateway_id, 'message_id', message_id,
 'sample_count', sample_count, 'boot_id', boot_id) FROM processed_messages WHERE gateway_id=:'gw';
""", {'gw': spec.gateway_id}).strip())
        messages.append(info)
    snapshots.append(paper_p0_stats(registry, containers, 'drained'))
    durable_json(fixture.private / 'paper-p0-observations.json', snapshots)
    # Prove that no setup or uplink write manufactured Stage 5 confirmation.
    durable_json(fixture.private / 'paper-p0-read-progress.json', {'step': 'reported_configuration_absent'})
    if db_psql(registry, pg, "SELECT count(*) FROM twin_states WHERE reported_state ? 'configuration';").strip() != '0':
        raise ValueError('paper_p0_read_failed')
    times = json.loads(db_psql(registry, pg, """
SELECT json_build_object('first', min(measured_at), 'last', max(measured_at))
FROM telemetry WHERE gateway_id=:'gw';
""", {'gw': specs[0].gateway_id}).strip())
    query = paper_p0_temporal_query(times['first'], times['last'])
    durable_json(fixture.private / 'paper-p0-read-progress.json', {'step': 'temporal_measurement'})
    status, _, body = http_request(endpoint, 'GET', sensor_path + '/temporal?' + query, token=tokens['a'])
    points = body.get('measurement', {}).get('values', [])
    # Only allowlisted counters/statuses; never persist a response body or token.
    durable_json(fixture.private / 'paper-p0-read-progress.json',
                 {'step': 'temporal_measurement', 'http_status': status, 'point_count': len(points)})
    if (status != 200 or len(points) != 100
            or any(abs(float(p[0]) - synthetic_value(i)) > 1e-9 for i, p in enumerate(points))):
        raise ValueError('paper_p0_read_failed')
    bracket = []
    for target in (specs[0], specs[1], specs[0]):
        status, _, body = http_request(endpoint, 'GET', '/v1/digital-twins/' + target.sensor_urn, token=tokens['a'])
        bracket.append(status)
        durable_json(fixture.private / 'paper-p0-read-progress.json',
                     {'step': 'cross_gateway_bracket', 'http_statuses': bracket})
        if status == 200 and body.get('id') != specs[0].sensor_urn:
            raise ValueError('paper_p0_read_failed')
        if status == 404 and body.get('error', {}).get('code') != 'not_found':
            raise ValueError('paper_p0_read_failed')
    if bracket != [200, 404, 200]:
        raise ValueError('paper_p0_read_failed')
    durable_json(fixture.private / 'paper-p0-read-progress.json', {'step': 'git_metadata'})
    mem = next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:'))
    evidence = {'backend_quota': quota, 'service_resources': service_resources, 'snapshots': snapshots,
        'messages': messages, 'revision': paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'branch': paper_p0_git(fixture, 'branch', '--show-current'),
        'dirty': bool(paper_p0_git(fixture, 'status', '--porcelain')), 'tools': {
            'docker_engine': registry.command(['version', '--format', '{{.Server.Version}}']).stdout.decode().strip(),
            'compose': registry.command(['compose', 'version', '--short']).stdout.decode().strip(),
            'python': sys.version.split()[0]}, 'host': {'kernel': os.uname().release,
            'architecture': os.uname().machine, 'logical_cpus': os.cpu_count(), 'memory_bytes': int(mem) * 1024},
        'images': {n: {'reference': effective['services'][n].get('image', 'source:src/Dockerfile#backend'), 'id': images[n]}
                   for n in fixture_services(fixture)}, 'gateway_image_id': images['gateway'],
        'checks': {'login_users': 3, 'gateway_graphs': 2, 'raw_samples': 200, 'ledgers': 2,
            'temporal_observations': 2, 'application_acks': 2, 'empty_outboxes': 2,
            'cross_gateway_status': bracket[1], 'positive_bracket': [bracket[0], bracket[2]],
            'http_raw_points': len(points), 'storage_status': storage_status}}
    validate_paper_p0_evidence(evidence)
    return evidence


def cli_arguments():
    return ("--prepare", "--startup-only", "--positive-http-only", "--role-http-only", "--full-http", "--paper-p0", "--paper-p1", "--mode")


def paper_p1_resources_csv(path, snapshots, limits):
    import csv
    units = {'B': 1, 'kB': 1000, 'MB': 1000000, 'GB': 1000000000,
             'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3}
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=('schema_version', 'phase', 'started_at', 'finished_at',
            'service', 'cpu_percent', 'memory_used_bytes', 'cpu_limit_nanocpus', 'memory_limit_bytes'))
        writer.writeheader()
        for snapshot in snapshots:
            for service, stats in snapshot['services'].items():
                match = re.fullmatch(r'([0-9.]+)([A-Za-z]+) / .+', stats['MemUsage'])
                if not match or match[2] not in units:
                    raise ValueError('paper_p1_resources_rejected')
                writer.writerow({'schema_version': 1, 'phase': snapshot['phase'],
                    'started_at': snapshot['started_at'], 'finished_at': snapshot['finished_at'], 'service': service,
                    'cpu_percent': float(stats['CPUPerc'].rstrip('%')),
                    'memory_used_bytes': int(float(match[1]) * units[match[2]]),
                    'cpu_limit_nanocpus': limits[service]['NanoCpus'], 'memory_limit_bytes': limits[service]['Memory']})


def paper_p1_subscription_target(fixture, spec, password, tag):
    # subscribeLiteral permits these exact existing filters, not arbitrary
    # narrower filters. Never broaden either principal's broker policy.
    if tag == 'telemetry':
        return fixture.values['MQTT_USERNAME'], fixture.values['MQTT_PASSWORD'], 'gateways/+/telemetry/#'
    if tag == 'ack':
        return spec.gateway_id, password, 'gateways/' + spec.gateway_id + '/acks/#'
    raise ValueError('paper_p1_subscription_failed')


def paper_p1_suback_ready(data):
    match = re.search(rb'Subscribed \(mid:\s*\d+\):\s*(\d+)', data)
    if b'received SUBACK' not in data or match is None:
        return False
    if int(match[1]) != 1:
        raise ValueError('paper_p1_subscription_failed')
    return True


def paper_p1_subscriber(fixture, registry, broker, spec, password, tag):
    """Use the existing broker CLI, TLS/DynSec and private bounded debug output.

    A pseudo-tty makes CLI debug lines line-buffered so SUBACK can be observed
    before publishing. No new port, resource registry or MQTT implementation.
    """
    username, subscription_password, topic = paper_p1_subscription_target(fixture, spec, password, tag)
    path = fixture.private / ('p1-sub-' + tag + '.log')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stream = os.fdopen(fd, 'wb')
    command = ['docker', 'exec', '-t', broker, 'mosquitto_sub', '-d', '-h', 'mosquitto', '-p', '8883',
        '--cafile', '/mosquitto/config/certs/ca.crt', '-u', username, '-P', subscription_password,
        '-q', '1', '-t', topic, '-C', '1', '-W', '60']
    proc = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT, env=process_env(fixture))
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            data = path.read_bytes()
            if paper_p1_suback_ready(data):
                return proc, stream, path
            if proc.poll() is not None or len(data) > 131072:
                break
            time.sleep(0.05)
        raise ValueError('paper_p1_subscription_failed')
    except BaseException:
        proc.terminate()
        proc.wait(timeout=5)
        stream.close()
        raise


def paper_p1_read_subscription(subscription):
    proc, stream, path = subscription
    try:
        proc.wait(timeout=8)
        if proc.returncode or path.stat().st_size > 131072:
            raise ValueError('paper_p1_subscription_failed')
        payloads = [line.strip() for line in path.read_bytes().splitlines() if line.startswith(b'{')]
        if len(payloads) != 1:
            raise ValueError('paper_p1_subscription_failed')
        return payloads[0]
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
        stream.close()


def paper_p1_count_db(registry, pg):
    return json.loads(db_psql(registry, pg, """
SELECT json_build_object('ledger', (SELECT count(*) FROM processed_messages),
 'raw', (SELECT count(*) FROM telemetry), 'temporal', (SELECT count(*) FROM twin_temporal_values),
 'configuration', (SELECT count(*) FROM twin_states WHERE reported_state ? 'configuration'));
""").strip())


def run_paper_p1(fixture, registry, endpoint, containers, images, state, *, extra=None, before_workload=None):
    from stage3_uplink_runner import (gateway_env, simulator_envelope, synthetic_value, SIM_BASE, SIM_PERIOD,
                                     build_gateway_assert_sql, parse_ingress_summary)
    from stage3_mqtt_ingress_runner import parse_run_summary, verify_ack
    pg, quota, effective, limits, storage_status, tokens, specs, passwords, sensor_path = prepare_paper_workload(
        fixture, registry, endpoint, containers, images, state)
    enabled = fixture.values['METRICS_ENABLED'] == 'true'
    run = {'schema_version': 1, 'run_id': fixture.project, 'status': 'FAIL', 'cleanup_verified': False,
        'mode': fixture.paper_p1, 'effective_optimizations': [], 'modes_equivalent': True,
        'instrumentation_enabled': enabled, 'revision': paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'branch': paper_p0_git(fixture, 'branch', '--show-current'),
        'dirty': bool(paper_p0_git(fixture, 'status', '--porcelain')),
        'backend_quota': quota, 'service_resources': limits,
        'config': {'queue_capacity': 256, 'workers': 4, 'max_payload_bytes': 65536, 'db_max_conns': 10,
                   'stage5_enabled': False, 'gateway_count': 2, 'sensor_count': 2, 'viewer_count': 0,
                   'gateway_concurrency': 1, 'batches_per_gateway': 20, 'samples_per_batch': 100, 'frequency_hz': 100},
        'capabilities': {'ingress': 'available', 'metrics': 'available' if enabled else 'disabled',
                         'command': 'disabled', 'result': 'not_implemented', 'websocket': 'not_implemented'},
        'images': {n: {'reference': effective['services'][n].get('image', 'source:src/Dockerfile#backend'), 'id': images[n]}
                   for n in fixture_services(fixture)}, 'gateway_image_id': images['gateway'],
        'tools': {'docker_engine': registry.command(['version', '--format', '{{.Server.Version}}']).stdout.decode().strip(),
                  'compose': registry.command(['compose', 'version', '--short']).stdout.decode().strip(), 'python': sys.version.split()[0]},
        'host': {'kernel': os.uname().release, 'architecture': os.uname().machine, 'logical_cpus': os.cpu_count(),
                 'memory_bytes': int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemTotal:'))) * 1024,
                 'db_load_generator_same_host': True}}
    if extra is not None:
        run.update(extra)
        run['status'] = 'FAIL'
    durable_json(fixture.artifacts / 'run.json', run)
    auth_samples = before_workload(pg, tokens, specs, sensor_path, run) if before_workload is not None else []
    samples, snapshots, gateway_results = auth_samples, [], []
    last_scrape = 0

    def scrape(phase, required=False):
        nonlocal last_scrape
        if not enabled:
            return None
        last_scrape = time.monotonic()
        try:
            status, _, snap = http_request(endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'], timeout=2)
            if (status != 200 or snap.get('schema_version') != 1 or not snap.get('process_instance')
                    or snap.get('availability', {}).get('command') != 'disabled' or not isinstance(snap.get('ingress'), dict)):
                raise ValueError('paper_p1_snapshot_rejected')
            samples.append({'phase': phase, 'snapshot': snap})
        except Exception:
            samples.append({'phase': phase, 'snapshot': None, 'error': 'scrape_failed'})
            if required:
                raise ValueError('paper_p1_scrape_failed') from None
            snap = None
        paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', samples)
        return snap

    # Native auth checks through the same proxy; no service-role business token.
    statuses = [http_request(endpoint, 'GET', '/v1/admin/metrics', token=token)[0]
                for token in ('', 'invalid', tokens['a'], tokens['admin'])]
    if statuses != [401, 401, 403, 200 if enabled else 404]:
        raise ValueError('paper_p1_auth_failed')
    run['metrics_auth_statuses'] = statuses
    before_db = paper_p1_count_db(registry, pg)
    before = scrape('before_workload', required=True)
    if not enabled:
        paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', [
            {'phase': 'disabled', 'snapshot': None, 'error': 'metrics_disabled'}])
    snapshots.append(paper_p0_stats(registry, containers, 'before_workload'))
    broker = containers['mosquitto']['receipt']['id']
    captured = None
    window_start, window_end = None, None
    for role, spec, password in zip(('a', 'b'), specs, passwords):
        state.checkpoint()
        labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
        volume = registry.intent('volume', fixture.project + '_outbox_' + role, None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        result = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, volume['name']])
        if result.returncode or registry.resolve(volume) is None:
            raise ValueError('helper_allocation_unresolved')
        env = gateway_env(spec.gateway_id, password)
        env['GATEWAY_MQTT_BROKER_URL'] = 'ssl://mosquitto:8883'
        env_file = fixture.private / ('gateway-' + role + '.env')
        private_write(env_file, ''.join(k + '=' + v + '\n' for k, v in env.items()))
        subscription = paper_p1_subscriber(fixture, registry, broker, spec, password, 'telemetry') if role == 'a' else None
        try:
            def during_gateway():
                nonlocal window_start
                if window_start is None:
                    window_start = time.monotonic()
                    run['started_at'] = datetime.now(timezone.utc).isoformat()
                snapshots.append(paper_p0_stats(registry, containers, 'gateway_' + role + '_running'))
            def tick():
                if enabled and time.monotonic() - last_scrape >= 1:
                    scrape('gateway_' + role + '_running')
            out = paper_p0_gateway_cli(fixture, registry, images['gateway'], volume, env_file,
                fixture.private / 'certs/ca.crt', role, ['run', '--batches', '20', '--completion-timeout', '30s', '--measure'], state,
                tick=tick, timeout=80,
                during=during_gateway)
            window_end = time.monotonic()
            run['finished_at'] = datetime.now(timezone.utc).isoformat()
            if subscription:
                captured = paper_p1_read_subscription(subscription)
        finally:
            if subscription and subscription[0].poll() is None:
                subscription[0].terminate(); subscription[0].wait(timeout=5); subscription[1].close()
        summary = parse_run_summary(out)
        if any(summary.get(k) != v for k, v in {'enqueued': '20', 'app_acknowledged': '20', 'completed': 'true',
                'pending_rows': '0', 'pending_bytes': '0', 'quarantined_rows': '0', 'quarantined_bytes': '0'}.items()):
            raise ValueError('paper_p1_gateway_failed')
        serialization = re.search(r'serialization summary: unique_bytes=(\d+) attempted_bytes=(\d+) unique_samples=(\d+)', out)
        measurement = re.search(r'measurement summary: (.+)', out)
        if not serialization or not measurement:
            raise ValueError('paper_p1_gateway_failed')
        measured = dict(re.findall(r'(\w+)=([\w.]+)', measurement[1]))
        allowed = ('samples', 'p50_ms', 'p95_ms', 'min_ms', 'max_ms', 'first_publishes', 'retry_attempts',
                   'missing_origins', 'overflow_samples', 'gen_duration_ms', 'publish_drain_duration_ms', 'total_duration_ms')
        if any(key not in measured for key in allowed) or measured['samples'] != '20' or measured['missing_origins'] != '0' or measured['overflow_samples'] != '0':
            raise ValueError('paper_p1_gateway_failed')
        gateway_results.append({'unique_messages': 20, 'publish_attempts': int(summary['publish_attempts']),
            'application_acks': 20, 'unique_payload_bytes': int(serialization[1]),
            'attempted_payload_bytes': int(serialization[2]), 'unique_samples': int(serialization[3]),
            'ack_measurement': {key: float(measured[key]) for key in allowed}})
        count = paper_p0_gateway_cli(fixture, registry, images['gateway'], volume, env_file,
            fixture.private / 'certs/ca.crt', role + '-count', ['count'], state)
        if count.strip() != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
            raise ValueError('paper_p1_gateway_failed')
        scrape('gateway_' + role + '_drained', required=True)
    run['workload_window_seconds'] = window_end - window_start
    run['workload_window_boundary'] = 'first_gateway_container_started_to_last_gateway_exit_includes_between_gateway_controller_gaps'
    after = scrape('after_workload', required=True)
    after_db = paper_p1_count_db(registry, pg)
    delta_db = {k: after_db[k] - before_db[k] for k in before_db}
    if delta_db != {'ledger': 40, 'raw': 4000, 'temporal': 40, 'configuration': 0}:
        raise ValueError('paper_p1_reconciliation_failed')
    run['db_delta'] = delta_db
    # Retain shared last-batch contract assertions; check every raw row via join.
    for spec in specs:
        last = json.loads(db_psql(registry, pg, "SELECT row_to_json(x) FROM (SELECT message_id,first_sequence,boot_id FROM processed_messages WHERE gateway_id=:'gw' ORDER BY first_sequence DESC LIMIT 1) x;", {'gw': spec.gateway_id}).strip())
        env = replace(simulator_envelope(), message_id=last['message_id'], boot_id=last['boot_id'],
                      first_sequence=int(last['first_sequence']), last_value=synthetic_value(int(last['first_sequence']) + 99), expected_reported_version=20)
        db_psql(registry, pg, build_gateway_assert_sql(spec, env, 'paper_p1_last_batch', expected_messages=20))
    bad_rows = db_psql(registry, pg, f"""
WITH first_times AS (SELECT gateway_id,message_id,min(measured_at) AS first_at FROM telemetry GROUP BY gateway_id,message_id)
SELECT count(*) FROM telemetry t LEFT JOIN processed_messages p USING(gateway_id,message_id)
JOIN first_times f USING(gateway_id,message_id)
WHERE p.message_id IS NULL OR t.sensor_id<>p.sensor_id OR t.boot_id<>p.boot_id
 OR t.sequence_num<>p.first_sequence+t.sample_index OR t.sample_index NOT BETWEEN 0 AND 99
 OR abs(t.value-({SIM_BASE}+(CASE WHEN mod(t.sequence_num,{SIM_PERIOD})>{SIM_PERIOD//2} THEN {SIM_PERIOD}-mod(t.sequence_num,{SIM_PERIOD}) ELSE mod(t.sequence_num,{SIM_PERIOD}) END)::double precision/100))>1e-9
 OR t.measured_at<>f.first_at+t.sample_index*10000*interval '1 microsecond';
""").strip()
    if bad_rows != '0':
        raise ValueError('paper_p1_reconciliation_failed')
    # Byte-exact replay from the real serialized telemetry, never reconstructed.
    if captured is None or len(captured) > 65536:
        raise ValueError('paper_p1_replay_failed')
    msg = json.loads(captured)
    subscription = paper_p1_subscriber(fixture, registry, broker, specs[0], passwords[0], 'ack')
    try:
        pub = registry.command(['exec', '-i', broker, 'mosquitto_pub', '-h', 'mosquitto', '-p', '8883',
            '--cafile', '/mosquitto/config/certs/ca.crt', '-u', specs[0].gateway_id, '-P', passwords[0],
            '-q', '1', '-t', specs[0].telemetry_topic, '-s'], input=captured)
        ack = paper_p1_read_subscription(subscription)
    finally:
        if subscription[0].poll() is None:
            subscription[0].terminate(); subscription[0].wait(timeout=5)
        subscription[1].close()
    if pub.returncode:
        raise ValueError('paper_p1_replay_failed')
    verify_ack(msg['message_id'], ack)
    final = scrape('after_replay_drain', required=True)
    if paper_p1_count_db(registry, pg) != after_db:
        raise ValueError('paper_p1_reconciliation_failed')
    if enabled:
        if before['process_instance'] != final['process_instance']:
            raise ValueError('paper_p1_reconciliation_failed')
        counter_delta = {k: final['ingress'][k] - before['ingress'][k] for k in P1_COUNTERS}
        if (counter_delta['Committed'] != 40 or counter_delta['SamplesNew'] != 4000 or counter_delta['DuplicateCommitted'] < 1
                or counter_delta['ACKPublished'] < 41 or final['ingress']['InFlight'] or final['ingress']['QueueDepth']):
            raise ValueError('paper_p1_reconciliation_failed')
        run['counter_delta'] = counter_delta
        run['rates_workload_window'] = paper_p1_rates(before, after)
    run['gateways'] = gateway_results
    run['offered'] = {key: sum(g[key] for g in gateway_results) for key in ('unique_messages', 'unique_samples', 'publish_attempts', 'application_acks', 'unique_payload_bytes', 'attempted_payload_bytes')}
    run['offered']['mean_payload_bytes_per_message'] = run['offered']['unique_payload_bytes'] / 40
    run['replay'] = {'messages': 1, 'payload_bytes': len(captured), 'application_ack': True, 'db_unchanged': True}
    run['controller_window_rates'] = {'new_committed_msg_per_s': 40 / run['workload_window_seconds'],
                                     'new_committed_samples_per_s': 4000 / run['workload_window_seconds']}
    run['gateway_publish_drain_window_seconds'] = sum(g['ack_measurement']['publish_drain_duration_ms'] for g in gateway_results) / 1000
    run['gateway_active_window_rates'] = {'unique_msg_per_s': 40 / run['gateway_publish_drain_window_seconds'],
                                        'unique_samples_per_s': 4000 / run['gateway_publish_drain_window_seconds']}
    # Query one batch range (100 points), keeping canonical API limits intact.
    times = json.loads(db_psql(registry, pg, "SELECT json_build_object('first',min(measured_at),'last',max(measured_at)) FROM telemetry WHERE gateway_id=:'gw' AND message_id=:'mid';", {'gw': specs[0].gateway_id, 'mid': msg['message_id']}).strip())
    query = paper_p0_temporal_query(times['first'], times['last'])
    status, _, body = http_request(endpoint, 'GET', sensor_path + '/temporal?' + query, token=tokens['a'])
    points = body.get('measurement', {}).get('values', [])
    bounds = urllib.parse.parse_qs(query)
    expected_values = json.loads(db_psql(registry, pg, "SELECT json_agg(value ORDER BY measured_at,sequence_num) FROM telemetry WHERE gateway_id=:'gw' AND measured_at>=:'first'::timestamptz AND measured_at<=:'last'::timestamptz;", {'gw':specs[0].gateway_id,'first':bounds['timeAt'][0],'last':bounds['endTimeAt'][0]}).strip())
    if (status != 200 or len(points) < 100 or len(points) != len(expected_values)
            or any(abs(float(point[0])-value)>1e-9 for point,value in zip(points,expected_values))):
        raise ValueError('paper_p1_read_failed')
    bracket = [http_request(endpoint, 'GET', '/v1/digital-twins/' + s.sensor_urn, token=tokens['a'])[0] for s in (specs[0], specs[1], specs[0])]
    if bracket != [200, 404, 200]:
        raise ValueError('paper_p1_read_failed')
    run['reads'] = {'temporal_status': status, 'temporal_points': len(points), 'positive_denial_bracket': bracket, 'storage_status': storage_status}
    snapshots.append(paper_p0_stats(registry, containers, 'drained'))
    paper_p1_resources_csv(fixture.artifacts / 'resources.csv', snapshots, limits)
    # Counters predating METRICS_ENABLED remain authoritative in the off run.
    backend = containers['backend']['receipt']['id']
    if registry.command(['stop', '--time', '15', backend]).returncode:
        raise ValueError('paper_p1_gateway_failed')
    logs = registry.command(['logs', '--tail', '30', backend])
    summary = parse_ingress_summary((logs.stdout + logs.stderr).decode(errors='replace'))
    if summary.get('committed') != 40 or summary.get('samples_new') != 4000 or summary.get('duplicate_committed', 0) < 1:
        raise ValueError('paper_p1_reconciliation_failed')
    run['ingress_shutdown_counts'] = {key: summary.get(key) for key in ('accepted', 'committed', 'duplicate_committed', 'samples_new', 'ack_published', 'validation_rejected', 'persist_failures')}
    run['status'] = 'WORKLOAD_VERIFIED_CLEANUP_PENDING'
    durable_json(fixture.artifacts / 'run.json', run)
    return run


def provision_role_identities(fixture, registry, endpoint, postgres_cid, gw_id, other_gw_id, existing_sensor):
    service_key, anon_key = fixture.values["SERVICE_ROLE_KEY"], fixture.values["ANON_KEY"]
    identities, tokens = {}, {}
    for role in ("platform_admin", "owner", "operator", "viewer", "nonmember", "cross_gateway_owner"):
        email, password = f"{role}_{secrets.token_hex(6)}@example.invalid", secrets.token_urlsafe(24)
        uid = create_user_via_admin_api(endpoint, service_key, email, password)
        identities[role] = uid
        tokens[role] = login_user_for_token(endpoint, anon_key, email, password)
    # Fixture-generated identifiers only. Values still pass through psql -v
    # quoting, not string interpolation into SQL.
    statements = []
    for role, uid in identities.items():
        statements.append("INSERT INTO public.profiles (id,full_name) VALUES (:'uid_" + role + "', 'Role smoke') ON CONFLICT (id) DO NOTHING;")
    statements += [
        "INSERT INTO public.platform_admins(user_id) VALUES (:'uid_platform_admin');",
        "INSERT INTO public.gateways(gateway_id,name,description) VALUES (:'gateway_id','Role smoke gateway','fixture');",
        "INSERT INTO public.gateways(gateway_id,name,description) VALUES (:'other_gateway_id','Role smoke other gateway','fixture');",
        "INSERT INTO public.user_gateways(user_id,gateway_id,role) VALUES (:'uid_owner',:'gateway_id','owner'), (:'uid_operator',:'gateway_id','operator'), (:'uid_viewer',:'gateway_id','viewer'), (:'uid_cross_gateway_owner',:'other_gateway_id','owner');",
    ]
    for key, gw, name in (("gateway", gw_id, "Role smoke gateway"), ("other", other_gw_id, "Role smoke other gateway")):
        entity_uuid = secrets.token_hex(16)
        entity_uuid = f"{entity_uuid[:8]}-{entity_uuid[8:12]}-{entity_uuid[12:16]}-{entity_uuid[16:20]}-{entity_uuid[20:]}"
        statements += ["INSERT INTO public.twin_entities(id,entity_id,entity_type,name,attributes,gateway_id) VALUES (:'entity_" + key + "','urn:ngsi-ld:Gateway:'||:'" + ("gateway_id" if key == "gateway" else "other_gateway_id") + "','Gateway',:'name_" + key + "','{}'::jsonb,:'" + ("gateway_id" if key == "gateway" else "other_gateway_id") + "');",
                       "INSERT INTO public.twin_states(entity_id) VALUES (:'entity_" + key + "');"]
        identities["entity_" + key] = entity_uuid
    params = {"gateway_id": gw_id, "other_gateway_id": other_gw_id, "name_gateway": "Role smoke gateway", "name_other": "Role smoke other gateway", "sensor_id": existing_sensor}
    params.update({"uid_" + r: uid for r, uid in identities.items() if r in ("platform_admin", "owner", "operator", "viewer", "nonmember", "cross_gateway_owner")})
    params.update({"entity_" + k: identities["entity_" + k] for k in ("gateway", "other")})
    # Existing sensor/twin fixture gives the denied update a real target.
    statements += ["INSERT INTO public.sensors(gateway_id,sensor_id,name,unit) VALUES (:'gateway_id',:'sensor_id','Existing sensor','C');",
                   "INSERT INTO public.twin_entities(id,entity_id,entity_type,name,attributes,gateway_id) VALUES (:'sensor_entity','urn:ngsi-ld:Sensor:'||:'gateway_id'||':'||:'sensor_id','Sensor','Existing sensor','{}'::jsonb,:'gateway_id');",
                   "INSERT INTO public.twin_states(entity_id) VALUES (:'sensor_entity');",
                   "INSERT INTO public.twin_relationships(source_entity_id,relationship_type,target_entity_id) VALUES (:'entity_gateway','hasSensor',:'sensor_entity');",
                   "INSERT INTO public.sensors(gateway_id,sensor_id,name,unit) VALUES (:'other_gateway_id',:'sensor_id','Existing sensor','C');",
                   "INSERT INTO public.twin_entities(id,entity_id,entity_type,name,attributes,gateway_id) VALUES (:'other_sensor_entity','urn:ngsi-ld:Sensor:'||:'other_gateway_id'||':'||:'sensor_id','Sensor','Existing sensor','{}'::jsonb,:'other_gateway_id');",
                   "INSERT INTO public.twin_states(entity_id) VALUES (:'other_sensor_entity');",
                   "INSERT INTO public.twin_relationships(source_entity_id,relationship_type,target_entity_id) VALUES (:'entity_other','hasSensor',:'other_sensor_entity');"]
    params["sensor_entity"] = secrets.token_hex(16)
    val = params["sensor_entity"]
    params["sensor_entity"] = f"{val[:8]}-{val[8:12]}-{val[12:16]}-{val[16:20]}-{val[20:]}"
    val = secrets.token_hex(16)
    params["other_sensor_entity"] = f"{val[:8]}-{val[8:12]}-{val[12:16]}-{val[16:20]}-{val[20:]}"
    db_psql(registry, postgres_cid, "\n".join(statements), params)
    return tokens


def run_role_http_slice(fixture, registry, endpoint, postgres_cid):
    gateway_id = "gw_role_" + secrets.token_hex(5)
    other_gateway_id = "gw_other_" + secrets.token_hex(5)
    existing_sensor = "existing_" + secrets.token_hex(4)
    new_sensor = "new_" + secrets.token_hex(4)
    tokens = provision_role_identities(fixture, registry, endpoint, postgres_cid, gateway_id, other_gateway_id, existing_sensor)
    outcomes, snapshots = {}, 0
    for case in role_http_cases():
        role = "platform_admin" if case["role"] == "platform_admin_without_membership" else case["role"]
        if case["target"] == "inventory":
            status, _, body = http_request(endpoint, "GET", "/v1/admin/gateways", token=tokens[role])
            outcomes[case["label"]] = status
            durable_json(fixture.private / 'role-http-progress.json', {"completed": outcomes, "snapshot_comparisons": snapshots})
            try:
                assert_response("forbidden", status, body)
            except ValueError:
                raise ValueError("role_case_" + case["label"] + "_contract_failed") from None
        else:
            # This identity owns other_gateway_id but attempts to mutate the
            # main Gateway, so the route must not treat ownership as global.
            target_gateway = gateway_id
            sensor = new_sensor + "_" + case["label"] if case["sensor_kind"] == "new" else existing_sensor
            path = f"/v1/gateways/{target_gateway}/sensors/{sensor}"
            before = denial_snapshot(registry, postgres_cid, target_gateway, sensor)
            status, _, body = http_request(endpoint, "PUT", path, token=tokens[role],
                                           body={"name": "Denied mutation", "unit": "K"})
            outcomes[case["label"]] = status
            durable_json(fixture.private / 'role-http-progress.json', {"completed": outcomes, "snapshot_comparisons": snapshots})
            try:
                assert_response("forbidden" if case["status"] == 403 else "not_found", status, body)
            except ValueError:
                raise ValueError("role_case_" + case["label"] + "_contract_failed") from None
            after = denial_snapshot(registry, postgres_cid, target_gateway, sensor)
            assert_unchanged(before, after)
            snapshots += 1
    return {"role_cases": outcomes, "role_snapshot_comparisons": snapshots,
            "gateway_id": gateway_id, "existing_sensor": existing_sensor,
            "role_case_count": len(outcomes), "role_denials_verified": True}


def assert_login(status, body):
    # Token response stays private; this assertion returns no token or body.
    if status != 200 or not isinstance(body.get("access_token"), str) or not body["access_token"]:
        raise ValueError("login_failed")


def assert_response(case, status, body):
    expected = {"inventory": 200, "sensor_create": 201, "sensor_retry": 200,
                "forbidden": 403, "not_found": 404, "unauthorized": 401}
    if case not in expected or status != expected[case] or not isinstance(body, dict):
        raise ValueError("response_contract_failed")
    if re.search(r"password|credential|secret|token|hash", json.dumps(body), re.I):
        raise ValueError("secret_response_rejected")
    if case == "inventory":
        fields = {"gateway_id", "name", "description", "created_at"}
        if (set(body) != {"items", "next_cursor"} or not isinstance(body["items"], list)
                or any(not isinstance(item, dict) or set(item) != fields for item in body["items"])
                or body["next_cursor"] is not None and not isinstance(body["next_cursor"], str)):
            raise ValueError("inventory_contract_failed")
    if case.startswith("sensor_") and set(body) != {"gateway_id", "sensor_id", "name", "unit", "entity_id", "created_at"}:
        raise ValueError("sensor_contract_failed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", type=Path, required=True, help="New private fixture directory, outside artifacts")
    parser.add_argument('--startup-only', action='store_true', help='Real phased stack startup; HTTP acceptance remains closed')
    parser.add_argument('--positive-http-only', action='store_true', help='Phased stack startup and positive HTTP login/admin/owner sensor test only')
    parser.add_argument('--role-http-only', action='store_true', help='Phased startup, positive slice, and bounded role denials; JWT acceptance remains false')
    parser.add_argument('--full-http', action='store_true', help='Phased startup, positive proofs, role and JWT denials, and exact cleanup acceptance')
    parser.add_argument('--paper-p0', action='store_true', help='Isolated P0 quota, Storage, Auth/API and two-Gateway uplink smoke')
    parser.add_argument('--paper-p1', action='store_true', help='Bounded metrics and artifact workload, same isolated lifecycle as P0')
    parser.add_argument('--paper-p2-spike', action='store_true', help='P2.1 compatibility only; not P2 acceptance')
    parser.add_argument('--paper-p2', action='store_true', help='P2 isolated HTTPS/Unix perimeter checks')
    parser.add_argument('--mode', choices=('baseline', 'optimized'))
    parser.add_argument('--metrics-off', action='store_true', help='P1 overhead control: no new instrumentation or scraper')
    args = parser.parse_args()
    if args.paper_p2 and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.startup_only,
                             args.positive_http_only, args.role_http_only, args.full_http, args.mode, args.metrics_off)):
        parser.error('P2 cannot be combined with other execution modes')
    if args.paper_p2_spike and any((args.paper_p0, args.paper_p1, args.startup_only,
                                  args.positive_http_only, args.role_http_only, args.full_http,
                                  args.mode, args.metrics_off)):
        parser.error('P2 spike cannot be combined with other execution modes')
    if (args.paper_p0 or args.paper_p1) and (any((args.startup_only, args.positive_http_only, args.role_http_only, args.full_http)) or args.paper_p0 and args.paper_p1):
        parser.error('paper mode cannot be combined with other execution modes')
    if args.paper_p1 != (args.mode is not None) or args.metrics_off and not args.paper_p1:
        parser.error('--paper-p1 requires --mode; --mode/--metrics-off require --paper-p1')
    fixture = None
    try:
        fixture = prepare(args.prepare, paper_p0=args.paper_p0, paper_p1=args.mode,
                          metrics_enabled=not args.metrics_off, paper_p2_spike=args.paper_p2_spike, paper_p2=args.paper_p2)
    except BaseException as exc:
        report = {
            "schema_version": 1,
            "status": "STARTUP_FAILED",
            "phase": "prepare",
            "error_type": sanitize_failure_type(exc),
            "error_code": sanitize_failure_code(exc),
            "http_accepted": False,
            "cleanup_verified": False,
            "primary_phase": None,
            "primary_error_code": None,
            "cleanup_step": None,
            "cleanup_error_code": None,
            "resource_kind": None,
        }
        try:
            validate_failure_report(report)
            payload = json.dumps(report)
        except BaseException:
            payload = json.dumps(FALLBACK_FAILURE_DIAGNOSTIC)
        print(payload)
        return 1

    try:
        if args.paper_p0 or args.paper_p1 or args.paper_p2_spike or args.paper_p2:
            report = startup_only(fixture)
            # Only this new mode removes its own generated secret inputs after
            # successful owned-resource cleanup; failure inputs remain private.
            if not is_fixture_verified(fixture):
                raise ValueError('unsafe_fixture_path')
            shutil.rmtree(fixture.private)
            print(json.dumps(report))
            return 0
        if args.startup_only:
            report = startup_only(fixture)
            print(json.dumps(report))
            return 0
        if args.positive_http_only:
            report = startup_only(fixture, positive_http=True)
            print(json.dumps(report))
            return 0
        if args.role_http_only:
            report = startup_only(fixture, role_http=True)
            print(json.dumps(report))
            return 0
        if args.full_http:
            report = startup_only(fixture, full_http=True)
            try:
                validate_summary(report)
            except BaseException as exc:
                fail_report = record_failure_report(
                    fixture,
                    phase="report_validation",
                    exc=exc,
                    cleanup_verified=report.get("exact_cleanup_verified") is True,
                )
                raise FailureDiagnostic(fail_report, cause=exc) from exc
            print(json.dumps(report))
            return 0

        # Default prepare-only run
        try:
            report = summary(fixture)
            validate_summary(report)
            private_write(fixture.artifacts / "summary.json", json.dumps(report, indent=2) + "\n")
            print(json.dumps(report))
            return 0
        except BaseException as exc:
            fail_report = record_failure_report(fixture, phase="report_validation", exc=exc, cleanup_verified=False)
            raise FailureDiagnostic(fail_report, cause=exc) from exc
    except FailureDiagnostic as diag:
        out = None
        try:
            rep = getattr(diag, "report", None)
            if type(rep) is dict:
                validate_failure_report(rep)
                out = json.dumps(rep)
        except BaseException:
            out = None
        if out is None:
            out = json.dumps(FALLBACK_FAILURE_DIAGNOSTIC)
        print(out)
        return 1
    except BaseException as exc:
        out = None
        try:
            fail_report = record_failure_report(fixture, phase="preflight", exc=exc, cleanup_verified=False)
            if type(fail_report) is dict:
                validate_failure_report(fail_report)
                out = json.dumps(fail_report)
        except BaseException:
            out = None
        if out is None:
            out = json.dumps(FALLBACK_FAILURE_DIAGNOSTIC)
        print(out)
        return 1


if __name__ == "__main__":
    sys.exit(main())
