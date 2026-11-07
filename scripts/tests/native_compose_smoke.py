#!/usr/bin/env python3
"""Isolated Compose preflight and startup-only verification; HTTP gate closed.

Read-only effective-model inspection, bounded real Dockerfile build, and an
exact-owned networkless Envoy toolchain probe are available as Python functions.
The CLI optionally verifies phased startup. No deployment env is read.
"""
from __future__ import annotations

import argparse
import csv
import base64
import copy
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
import resource
import secrets
import shutil
import ssl
import subprocess
import sys
import time
import traceback
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
EXPECTED_SCHEMA_TERMINAL = "24:telemetry_ledger_timestamp"


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
    CORS_ALLOWED_ORIGINS="", WEBSOCKET_ENABLED="false", WS_MAX_SESSIONS="32",
    WS_SESSION_REFERENCE_BYTES="8388608",
    WS_MAX_SUBSCRIPTIONS_PER_SESSION="16", WS_GLOBAL_EVENT_SLOTS="64",
    WS_GLOBAL_EVENT_BYTES="8388608", WS_SESSION_EVENT_SLOTS="64",
    WS_SESSION_EVENT_BYTES="262144", WS_MAX_FRAME_BYTES="66560",
    WS_READ_FRAME_BYTES="4096", WS_REQUEST_RATE_PER_SECOND="10",
    WS_RECHECK_INTERVAL="5s", WS_RECHECK_SWEEP_BUDGET="2s",
    WS_RECHECK_CONCURRENCY="2", WS_FANOUT_MODE="indexed", WS_HEARTBEAT_INTERVAL="20s",
    WS_PONG_TIMEOUT="45s", WS_WRITE_TIMEOUT="5s", DATABASE_MAX_CONNS="5", DATABASE_MIN_CONNS="1",
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
    # P4 shaper values are private synthetic inputs for the isolated fixture;
    # do not inherit these (or any other values) from the caller environment.
    MQTT_SHAPER_ENABLED="false", MQTT_SHAPER_RATE_PER_SECOND="2",
    MQTT_SHAPER_BURST="4", MQTT_SHAPER_MAX_GATEWAYS="4096",
    MQTT_SHAPER_IDLE_RETENTION="10m", MQTT_INGRESS_BUFFERED_BYTES="16777216",
    # P5a microbatch values are explicit fixture inputs matching the root
    # Compose defaults; the isolated fixture remains opt-in/default-off.
    MQTT_MICROBATCH_ENABLED="false", MQTT_MICROBATCH_MAX_MESSAGES="32",
    MQTT_MICROBATCH_MAX_ROWS="500", MQTT_MICROBATCH_MAX_PAYLOAD_BYTES="4194304",
    MQTT_MICROBATCH_MAX_WAIT="1s",
    # P5b RAM metadata cache is independently opt-in with explicit local caps.
    MQTT_RAM_DEDUP_ENABLED="false", MQTT_RAM_DEDUP_MAX_ENTRIES="100000",
    MQTT_RAM_DEDUP_MAX_BYTES="67108864",
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
    STAGE5_SCAN_BATCH="32", STAGE5_PUBLISH_QUEUE_CAPACITY="32", STAGE5_WAKEUP_ENABLED="false",
    STAGE5_RESULT_QUEUE="128", STAGE5_RESULT_WORKERS="2", STAGE5_PAYLOAD_BYTES="8192",
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
    paper_p3: bool = False
    paper_p4: bool = False
    paper_p4_mode: str | None = None
    paper_p5a: bool = False
    paper_p5a_mode: str | None = None
    paper_p5a_dataset: str | None = None
    paper_p3_microbatch: bool = False
    paper_p5a_shutdown: bool = False
    paper_p5b_cache_smoke: bool = False
    paper_p5b_control_regression: bool = False
    paper_p5b: bool = False
    paper_p5b_cache_mode: str | None = None
    paper_p6: bool = False
    paper_p6_mode: str | None = None
    paper_p6_dataset: str | None = None
    paper_p6_recovery_smoke: bool = False
    paper_p7: bool = False
    paper_p7_mode: str | None = None
    paper_p8_sessions: bool = False
    paper_p8_slow: bool = False
    paper_p8_load: bool = False
    paper_p8_e1: bool = False
    paper_p8_e2: bool = False
    paper_p8_e2_workload: str = 'steady_burst'
    paper_p8_e2_context: str | None = None
    paper_p8_e2_reference: str | None = None
    paper_p8_profile: str = 'pilot'
    paper_p8_official_opt_in: bool = False
    paper_p8_e2_offered_rate: int = 100
    paper_p8_e2_duration_seconds: int = 10
    paper_p8_e2_burst_messages: int = 0
    paper_p8_e2_samples_per_message: int = 1
    paper_p8_e3: str | None = None
    paper_p8_e3_workload: str = 'mixed'
    paper_p8_e3_command_count: int = 4
    paper_p8_e3_schedule: str = 'smoke'
    paper_p8_e4: str | None = None
    paper_p8_e5: str | None = None
    paper_p8_e5_context: str | None = None
    paper_p8_e5_reference: str | None = None
    paper_p8_recovery: bool = False


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


# 8MiB cap for total diagnostics file (separate from 2MiB side/HTTP caps)
DIAGNOSTICS_FILE_CAP = 8 * 1024 * 1024


def paper_p8_e2_persist_diagnostics(path, payload):
    """Persist diagnostics payload with 8MiB size enforcement.

    Uses the same serialization as durable_json (json.dumps without separators).
    Raises ValueError('diagnostics_file_size_limit') if serialized size exceeds 8MiB.
    """
    actual_size = len(json.dumps(payload).encode())
    if actual_size > DIAGNOSTICS_FILE_CAP:
        raise ValueError('diagnostics_file_size_limit')
    durable_json(path, payload)


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
    remote_profile = getattr(fixture, 'remote_network_config', None)
    if remote_profile:
        san = ','.join([*(f'DNS:{name}' for name in remote_profile['required_san_dns']),
                        *(f'IP:{address}' for address in remote_profile['required_san_ips'])])
    else:
        san = 'DNS:mosquitto,DNS:localhost,IP:127.0.0.1'
    private_write(ext, f"subjectAltName={san}\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
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

    def command(self, args, input=None, *, timeout=None):
        try:
            return self.runner(["docker", *args], input=input, capture_output=True, timeout=(timeout if timeout is not None else BUDGETS['cleanup'] if 'rm' in args else 10),
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

    def resolve(self, item, *, timeout=None, read_only=False):
        target = copy.deepcopy(item) if read_only else item
        args = [target["kind"], "inspect", target["name"]]
        result = self.command(args) if timeout is None else self.command(args, timeout=timeout)
        if result.returncode:
            if (("no such " + target["kind"]).encode() in result.stderr.lower()
                    or target['kind'] == 'network' and ('network ' + target['name'] + ' not found').encode() in result.stderr.lower()):
                return None
            raise ValueError("resource_inspect_unresolved")
        try:
            rows = json.loads(result.stdout)
            if len(rows) != 1 or not isinstance(rows[0], dict):
                raise ValueError()
            obj = rows[0]
            if target["kind"] == "container":
                labels = obj.get("Config", {}).get("Labels") or {}
                identity, created = obj["Id"], obj["Created"]
                valid = (obj["Name"] == "/" + target["name"] and obj["Image"] == target["image"]
                         and re.fullmatch(r"[a-f0-9]{64}", identity))
            elif target["kind"] == "volume":
                labels = obj.get("Labels") or {}
                identity, created = obj["Name"], obj["CreatedAt"]
                valid = identity == target["name"] and obj.get("Driver") == "local" and not obj.get("Options")
            else:
                labels = obj.get("Labels") or {}
                identity, created = obj["Id"], obj["Created"]
                valid = obj["Name"] == target["name"] and obj.get("Driver") == "bridge" and not obj.get("Internal")
            if not valid or not created or any(labels.get(k) != v for k, v in target["labels"].items()):
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise ValueError("resource_ownership_refused") from None
        rec = {"id": identity, "created": created, "labels": labels}
        if target.get("helper_mount_receipt") is not None:
            check_helper_mount_receipt(target["helper_mount_receipt"], obj)
        if target["receipt"] is not None and target["receipt"] != rec:
            raise ValueError("resource_replacement_refused")
        target["receipt"] = rec
        if target.get("expectation") is not None:
            verify_runtime_receipt(obj, target["expectation"])
        if not read_only:
            item["receipt"] = target["receipt"]
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


def paper_p7_support():
    spec = importlib.util.spec_from_file_location('paper_p7_support', Path(__file__).with_name('paper_p7_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_support():
    spec = importlib.util.spec_from_file_location('paper_p8_support', Path(__file__).with_name('paper_p8_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_bundle():
    spec = importlib.util.spec_from_file_location('paper_p8_bundle', Path(__file__).parents[1] / 'paper_p8_bundle.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_e1_begin_collection(request, session_state, fixture, endpoint, registry,
                                 containers, prepared):
    if request.get('phase_id') != 'pilot':
        raise ValueError('e1_phase_id_mismatch')
    # Load the bundle module (not this loader function) before deriving timing
    # from the exact manifests used to prepare/export the E1 bundle.
    bundle = paper_p8_bundle()
    e1_lifecycle = bundle._derive_e1_lifecycle({
        'viewer': prepared['viewer_manifest'],
        'telemetry': prepared['telemetry_manifest']})
    lifecycle_deadline = session_state.get('deadline_monotonic')
    # The budget requires lifecycle + 60s bounded result-copy/import; 30s is
    # reserved separately for cleanup. No operator wait is assumed.
    e1_support = paper_p8_e1_support()
    collector_budget = e1_support.remaining_collector_budget(
        lifecycle_deadline, e1_lifecycle, clock=time.monotonic)
    return e1_support.begin_remote_collection(
        sys.modules[__name__], fixture, endpoint, registry, containers,
        prepared, phase_id=request['phase_id'],
        collector_lifetime_budget_seconds=collector_budget,
        workload_lifecycle_seconds=e1_lifecycle,
        session_deadline_monotonic=lifecycle_deadline)


def paper_p8_remote_support():
    spec = importlib.util.spec_from_file_location('paper_p8_remote', Path(__file__).with_name('paper_p8_remote.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_slow_support():
    spec = importlib.util.spec_from_file_location('paper_p8_slow_support', Path(__file__).with_name('paper_p8_slow_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_e1_support():
    spec = importlib.util.spec_from_file_location('paper_p8_e1_support', Path(__file__).with_name('paper_p8_e1_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_e2_support():
    spec = importlib.util.spec_from_file_location('paper_p8_e2_support', Path(__file__).with_name('paper_p8_e2_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_e3_support():
    spec = importlib.util.spec_from_file_location('paper_p8_e3_fixture', Path(__file__).with_name('paper_p8_e3_fixture.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_e4_support():
    spec = importlib.util.spec_from_file_location('paper_p8_e4_support', Path(__file__).with_name('paper_p8_e4_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p8_e5_support():
    spec = importlib.util.spec_from_file_location('paper_p8_e5_fixture', Path(__file__).with_name('paper_p8_e5_fixture.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def paper_p8_recovery_support():
    test_dir = str(Path(__file__).resolve().parent)
    if test_dir not in sys.path:
        sys.path.insert(0, test_dir)
    spec = importlib.util.spec_from_file_location('paper_p8_recovery_fixture', Path(__file__).with_name('paper_p8_recovery_fixture.py'))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def build_paper_p8_e3_gateway(fixture, go_binary):
    binary = fixture.private / 'bin' / 'gateway'
    log = fixture.private / 'gateway-build.log'
    reproducible_flags = ['-trimpath'] if getattr(fixture, 'paper_p8_e5', None) else []
    with log.open('xb') as stream:
        result = subprocess.run([go_binary, 'build', *reproducible_flags, '-o', str(binary), './cmd/gateway'],
            cwd=ROOT / 'src', stdout=stream, stderr=subprocess.STDOUT,
            timeout=BUDGETS['build'], env=process_env(fixture), check=False)
    if result.returncode or not binary.is_file():
        raise ValueError('gateway_build_failed')
    return binary


def build_paperload(fixture, go_binary):
    binary = fixture.private / 'bin' / 'paperload'
    log = fixture.private / 'paperload-build.log'
    reproducible_flags = ['-trimpath'] if getattr(fixture, 'paper_p8_e5', None) else []
    with log.open('xb') as stream:
        result = subprocess.run([go_binary, 'build', *reproducible_flags, '-o', str(binary), './cmd/paperload'],
            cwd=ROOT / 'src', stdout=stream, stderr=subprocess.STDOUT,
            timeout=BUDGETS['build'], env=process_env(fixture), check=False)
    if result.returncode or not binary.is_file():
        raise ValueError('paperload_build_failed')
    return binary


def dispatch_paper_p8_load(fixture, smoke, endpoint, registry, containers, images, state,
                           paperload_binary, gateway_binary=None):
    if getattr(fixture, 'paper_p8_recovery', False):
        return paper_p8_recovery_support().run(smoke, fixture, endpoint, registry, containers,
            images, state, paperload_binary, gateway_binary or fixture.private / 'bin' / 'gateway')
    if getattr(fixture, 'paper_p8_e5', None):
        return paper_p8_e5_support().run(smoke, fixture, endpoint, registry, containers,
            images, state, paperload_binary, gateway_binary or fixture.private / 'bin' / 'gateway',
            fixture.paper_p8_e5, fixture.paper_p8_e5_context, getattr(fixture, 'paper_p8_e5_reference', None))
    if getattr(fixture, 'paper_p8_e4', None):
        return paper_p8_e4_support().run(smoke, fixture, endpoint, registry, containers,
                                         images, state, paperload_binary,
                                         gateway_binary or fixture.private / 'bin' / 'gateway',
                                         fixture.paper_p8_e4)
    if getattr(fixture, 'paper_p8_e3', None):
        return paper_p8_e3_support().run(smoke, fixture, endpoint, registry, containers,
                                         images, state, paperload_binary,
                                          gateway_binary or fixture.private / 'bin' / 'gateway', fixture.paper_p8_e3,
                                           getattr(fixture, 'paper_p8_e3_workload', 'mixed'),
                                           command_count=getattr(fixture, 'paper_p8_e3_command_count', 4),
                                           schedule=getattr(fixture, 'paper_p8_e3_schedule', 'smoke'),
                                           profile=getattr(fixture, 'paper_p8_profile', 'pilot'),
                                           official_opt_in=getattr(fixture, 'paper_p8_official_opt_in', False))
    if getattr(fixture, 'paper_p8_e2', False):
        return paper_p8_e2_support().run(smoke, fixture, endpoint, registry, containers, images,
                                         state, paperload_binary)
    if getattr(fixture, 'paper_p8_e1', False):
        return paper_p8_e1_support().run(smoke, fixture, endpoint, registry, containers, images,
                                         state, paperload_binary,
                                         viewers=getattr(fixture, 'paper_p8_e1_viewers', 64),
                                         principals=getattr(fixture, 'paper_p8_e1_principals', 2),
                                         density=getattr(fixture, 'paper_p8_e1_density', 'mixed'),
                                         messages_per_target=getattr(fixture, 'paper_p8_e1_messages_per_target', 5))
    return paper_p8_support().run_e2_telemetry100(smoke, fixture, endpoint, registry, containers,
                                                   images, state, paperload_binary)


def dispatch_paper_fixture(fixture, helper, *args):
    """Keep P7 handshake, reconnect, and finite telemetry lanes separate from P2 behavior."""
    if fixture.paper_p7:
        mode = helper.pilot_mode(fixture)
        if mode in ('overhead_zero', 'overhead_two'):
            return helper.run_overhead(*args)
        if mode == 'lifecycle':
            return helper.run_lifecycle(*args)
        if mode == 'slow_viewer':
            return helper.run_slow_viewer(*args)
        if mode == 'reconnect':
            return helper.run_reconnect(*args)
        if mode in ('telemetry_pilot', 'single_path'):
            return helper.run_telemetry_pilot(*args)
        return helper.run(*args)
    if fixture.paper_p2:
        return helper.run_full(*args)
    raise ValueError('paper_fixture_mode_missing')


def paper_p4_support():
    spec = importlib.util.spec_from_file_location('paper_p4_support', Path(__file__).with_name('paper_p4_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p5a_support():
    spec = importlib.util.spec_from_file_location('paper_p5a_support', Path(__file__).with_name('paper_p5a_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def paper_p6_support():
    spec = importlib.util.spec_from_file_location('paper_p6_support', Path(__file__).with_name('paper_p6_support.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def prepare(base, paper_p0=False, paper_p1=None, metrics_enabled=True, paper_p2_spike=False,
            paper_p2=False, paper_p7=False, paper_p7_mode=None, paper_p3=False, paper_p4=None, paper_p5a=None,
            paper_p3_microbatch=False, paper_p5a_shutdown=False, paper_p5b_cache_smoke=False,
            paper_p5b_control_regression=False, paper_p5b=None, paper_p6=None,
             paper_p8_sessions=False, paper_p8_slow=False, paper_p8_load=False, paper_p8_e1=False,
             paper_p8_e2=False, paper_p8_e2_workload='steady_burst',
              paper_p8_e2_context=None, paper_p8_e2_reference=None,
              paper_p8_profile='pilot', paper_p8_official_opt_in=False,
              paper_p8_e2_offered_rate=100, paper_p8_e2_duration_seconds=10,
              paper_p8_e2_burst_messages=0,
              paper_p8_e2_samples_per_message=1,
              paper_p8_e3=None, paper_p8_e3_workload='mixed', paper_p8_e3_command_count=4,
              paper_p8_e3_schedule='smoke', paper_p8_e4=None,
               paper_p8_e5=None, paper_p8_e5_context=None, paper_p8_e5_reference=None,
                paper_p8_recovery=False, remote_network_config=None,
                paper_p8_e1_viewers=64, paper_p8_e1_principals=2,
                paper_p8_e1_density='mixed', paper_p8_e1_messages_per_target=5):
    if remote_network_config is not None:
        paper_p2 = True
        # Re-validate the canonical profile's exact input fields before it affects config.
        raw_profile = {key: remote_network_config[key] for key in paper_p8_remote_support().CONFIG_KEYS}
        remote_network_config = paper_p8_remote_support().validate_config(raw_profile)
    if paper_p8_recovery:
        if any((paper_p8_e5, paper_p8_e5_context, paper_p8_e5_reference,
                paper_p0, paper_p1, paper_p2_spike, paper_p2, paper_p7, paper_p3,
                paper_p4, paper_p5a, paper_p5b, paper_p6)):
            raise ValueError('paper_p8_recovery_selector_conflict')
        if paper_p8_load or any((paper_p8_sessions, paper_p8_slow, paper_p8_e1, paper_p8_e2,
                                 paper_p8_e3, paper_p8_e4)):
            raise ValueError('paper_p8_recovery_selector_conflict')
        paper_p8_e5 = 'optimized'
    if paper_p8_load and (paper_p8_e1 or paper_p8_e2 or paper_p8_e3 or paper_p8_e4 or paper_p8_e5):
        raise ValueError('paper_p8_mode_rejected')
    if paper_p8_e1 or paper_p8_e2 or paper_p8_e3 or paper_p8_e4 or paper_p8_e5:
        paper_p8_load = True
    if paper_p8_e2:
        if paper_p8_profile not in ('pilot','official') or (paper_p8_profile == 'official' and not paper_p8_official_opt_in):
            raise ValueError('official_profile_opt_in_required')
        if (paper_p8_profile == 'official' and (paper_p8_e2_workload != 'steady_burst'
                or paper_p8_e2_offered_rate not in (100,1000,5000,20000)
                or type(paper_p8_e2_duration_seconds) is not int or not 1 <= paper_p8_e2_duration_seconds <= 7200
                or type(paper_p8_e2_burst_messages) is not int or not 0 <= paper_p8_e2_burst_messages <= 100000)):
            raise ValueError('paper_p8_e2_official_schedule_rejected')
        if paper_p8_e2_workload not in ('steady_burst', 'equal_samples'):
            raise ValueError('paper_p8_e2_workload_rejected')
        if paper_p8_e2_workload == 'steady_burst':
            if paper_p8_e2_context is not None or paper_p8_e2_reference is not None:
                raise ValueError('paper_p8_e2_context_rejected')
        elif paper_p8_e2_workload == 'equal_samples':
            if paper_p8_e2_context is None:
                raise ValueError('paper_p8_e2_context_required')
            ctx_path = Path(paper_p8_e2_context).absolute()
            if ctx_path.is_symlink() or any(p.is_symlink() for p in ctx_path.parents):
                raise ValueError('paper_p8_e2_context_symlink_rejected')
            if not ctx_path.is_file():
                raise ValueError('paper_p8_e2_context_missing')
            st = ctx_path.lstat()
            if st.st_uid != os.getuid() or st.st_mode & 0o077:
                raise ValueError('paper_p8_e2_context_permissions_rejected')
            if st.st_size > 65536:
                raise ValueError('paper_p8_e2_context_size_rejected')
            try:
                ctx_data = json.loads(ctx_path.read_text(encoding='utf-8'))
            except Exception as exc:
                raise ValueError('paper_p8_e2_context_invalid') from exc
            paper_p8_e2_support().validate_e2_equal_samples_context(ctx_data, check_freshness=True)
            if paper_p8_e2_reference is not None:
                ref_path = Path(paper_p8_e2_reference).absolute()
                if ref_path.is_symlink() or any(p.is_symlink() for p in ref_path.parents):
                    raise ValueError('paper_p8_e2_reference_symlink_rejected')
                if not ref_path.is_file():
                    raise ValueError('paper_p8_e2_reference_missing')
                rst = ref_path.lstat()
                if rst.st_uid != os.getuid() or rst.st_mode & 0o077:
                    raise ValueError('paper_p8_e2_reference_permissions_rejected')
                if rst.st_size > 8 << 20:
                    raise ValueError('paper_p8_e2_reference_size_rejected')
                try:
                    ref_data = json.loads(ref_path.read_text(encoding='utf-8'))
                except Exception as exc:
                    raise ValueError('paper_p8_e2_reference_invalid') from exc
                if not isinstance(ref_data, dict) or ref_data.get('experiment') != 'E2':
                    raise ValueError('paper_p8_e2_reference_invalid')
                if ref_data.get('workload') != 'equal_samples' and ref_data.get('workload_type') != 'equal_samples':
                    raise ValueError('paper_p8_e2_reference_invalid')
    else:
        if paper_p8_e2_workload != 'steady_burst' or paper_p8_e2_context is not None or paper_p8_e2_reference is not None:
            raise ValueError('paper_p8_mode_rejected')
    if (paper_p8_e5 not in (None, 'optimized', 'shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling', 'fanout_scan')
            or bool(paper_p8_e5) != bool(paper_p8_e5_context) and not paper_p8_recovery):
        raise ValueError('paper_p8_e5_context_required')
    if paper_p8_e5_reference and (not paper_p8_e5 or paper_p8_e5 == 'optimized'):
        raise ValueError('paper_p8_e5_reference_rejected')
    if paper_p8_e3 not in (None, 'polling', 'wakeup'):
        raise ValueError('paper_p8_mode_rejected')
    if paper_p8_e3_workload not in ('mixed', 'low_load') or (paper_p8_e3_workload != 'mixed' and not paper_p8_e3):
        raise ValueError('paper_p8_mode_rejected')
    if (type(paper_p8_e3_command_count) is not int or not 1 <= paper_p8_e3_command_count <= 30
            or paper_p8_e3_schedule not in ('smoke', 'steady')
            or (paper_p8_e3_schedule == 'smoke' and paper_p8_e3_command_count != 4)
            or (not paper_p8_e3 and (paper_p8_e3_command_count != 4 or paper_p8_e3_schedule != 'smoke'))):
        raise ValueError('paper_p8_e3_schedule_rejected')
    if paper_p8_e4 not in (None, 'on', 'off'):
        raise ValueError('paper_p8_mode_rejected')
    if sum((bool(paper_p8_sessions), bool(paper_p8_slow), bool(paper_p8_load and not (paper_p8_e1 or paper_p8_e2 or paper_p8_e3 or paper_p8_e4 or paper_p8_e5)),
             bool(paper_p8_e1), bool(paper_p8_e2), bool(paper_p8_e3), bool(paper_p8_e4), bool(paper_p8_e5))) > 1:
        raise ValueError('paper_p8_mode_rejected')
    if paper_p8_sessions or paper_p8_slow or paper_p8_load:
        if any((paper_p0, paper_p1, paper_p2_spike, paper_p2 and remote_network_config is None,
                paper_p7, paper_p3, paper_p4,
                paper_p5a is not None, paper_p5b is not None, paper_p6 is not None, not metrics_enabled)):
            raise ValueError('paper_p8_mode_rejected')
        paper_p2 = True
    if paper_p8_recovery:
        if any((paper_p0, paper_p1, paper_p2_spike, paper_p7, paper_p3, paper_p4,
                paper_p5a is not None, paper_p5b is not None, paper_p6 is not None, not metrics_enabled)):
            raise ValueError('paper_p8_recovery_selector_conflict')
        paper_p8_load = True
        paper_p2 = True
    if paper_p3_microbatch and (not paper_p3 or not metrics_enabled or paper_p4 or paper_p5a):
        raise ValueError('paper_p3_microbatch_mode_rejected')
    if paper_p7:
        if (paper_p2 or paper_p0 or paper_p1 or paper_p2_spike or paper_p3 or paper_p4
                or paper_p5a is not None or paper_p5b is not None or
                (paper_p6 is not None and paper_p7_mode != 'p6_ws')):
            raise ValueError('paper_p7_mode_rejected')
        if paper_p7_mode not in (None, 'handshake_only', 'telemetry_pilot', 'single_path', 'reconnect', 'lifecycle', 'slow_viewer', 'overhead_zero', 'overhead_two', 'p6_ws'):
            raise ValueError('paper_p7_mode_rejected')
        if paper_p7_mode == 'p6_ws' and paper_p6 is None:
            paper_p6 = ('wake-up', '100')
        paper_p2 = True  # Reuse P2 HTTPS/Auth topology, not P2 assertions or probes.
    elif paper_p7_mode is not None:
        raise ValueError('paper_p7_mode_rejected')
    if paper_p5a is not None:
        if (not isinstance(paper_p5a, tuple) or len(paper_p5a) != 2 or not metrics_enabled
                or paper_p0 or paper_p1 or paper_p2_spike or paper_p2 or paper_p3 or paper_p4):
            raise ValueError('paper_p5a_mode_rejected')
        mode, dataset = paper_p5a
        # Validate the complete selector before allocating any fixture paths.
        paper_p5a_support().profile(mode, dataset, shutdown_smoke=paper_p5a_shutdown,
                                    cache_smoke=paper_p5b_cache_smoke)
        paper_p2 = True  # Reuse P2's HTTPS/Unix and TLS/DynSec topology.
    if paper_p5b is not None:
        if (not isinstance(paper_p5b, tuple) or len(paper_p5b) != 2 or not metrics_enabled
                or paper_p0 or paper_p1 or paper_p2_spike or paper_p2 or paper_p3 or paper_p4
                or paper_p5a is not None or paper_p5a_shutdown or paper_p5b_cache_smoke):
            raise ValueError('paper_p5b_mode_rejected')
        cache_mode, dataset = paper_p5b
        paper_p5a_support().p5b_profile(cache_mode, dataset)
        mode = 'on'  # P5b compares cache only; microbatch remains enabled in both arms.
        paper_p5a = (mode, dataset)
        paper_p2 = True
    if paper_p6 is not None:
        if (not isinstance(paper_p6, tuple) or len(paper_p6) != 2 or not metrics_enabled
                or paper_p0 or paper_p1 or paper_p2_spike
                or (paper_p2 and not (paper_p7 and paper_p7_mode == 'p6_ws')) or paper_p3 or paper_p4
                or paper_p5a is not None or paper_p5b is not None
                or (paper_p2 and paper_p7_mode != 'p6_ws')):
            raise ValueError('paper_p6_mode_rejected')
        mode, dataset = paper_p6
        paper_p6_support().profile(mode, dataset)
        paper_p2 = True
    if paper_p4 is not None:
        if (paper_p4 not in ('baseline', 'optimized') or not metrics_enabled or paper_p0 or paper_p1
                or paper_p2_spike or paper_p2 or paper_p3):
            raise ValueError('paper_p4_mode_rejected')
        paper_p2 = True  # Reuse P2's isolated perimeter topology, not its workload.
    if paper_p3:
        if paper_p0 or paper_p1 or paper_p2_spike or paper_p2 or not metrics_enabled:
            raise ValueError('paper_p3_mode_rejected')
        paper_p2 = True  # Reuse the P2 Auth/HTTPS/socket topology, not its workload.
    if paper_p2 and (paper_p0 or paper_p1 or paper_p2_spike):
        raise ValueError('paper_p2_mode_rejected')
    if paper_p2_spike and (paper_p0 or paper_p1):
        raise ValueError('paper_p2_mode_rejected')
    if paper_p1 not in (None, 'baseline', 'optimized') or paper_p0 and paper_p1:
        raise ValueError('paper_p1_mode_rejected')
    paper_profile = paper_p0 or paper_p1 is not None or paper_p2_spike or paper_p2
    e1_resource_overlay = None
    if paper_p8_e1:
        if paper_p8_profile not in ('pilot', 'official'):
            raise ValueError('e1_resource_gate_denied:profile')
        if paper_p8_e1_viewers > 64 and (paper_p8_profile != 'official' or not paper_p8_official_opt_in):
            raise ValueError('e1_resource_gate_denied:official_opt_in_required')
        if Path('/proc/meminfo').is_file():
            meminfo = Path('/proc/meminfo').read_text()
            match = re.search(r'^MemAvailable:\s+(\d+)\s+kB$', meminfo, re.MULTILINE)
            memory_available = int(match.group(1)) * 1024 if match else None
        else:
            memory_available = None
        fd_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if fd_soft == resource.RLIM_INFINITY:
            fd_soft = 2**63 - 1
        if memory_available is None:
            raise ValueError('e1_resource_gate_denied:host_memory_unavailable')
        if type(fd_soft) is not int:
            raise ValueError('e1_resource_gate_denied:host_fd_unavailable')
        for source_dir in (ROOT / 'scripts', ROOT / 'scripts/tests'):
            if str(source_dir) not in sys.path:
                sys.path.insert(0, str(source_dir))
        import paper_p8_e1_support as e1_support
        e1_resource_overlay = e1_support.e1_resource_overlay(paper_p8_e1_viewers,
            official_opt_in=paper_p8_profile == 'official' and paper_p8_official_opt_in,
            host_limits={'fd_soft': fd_soft, 'memory_available_bytes': memory_available})
    # Reject ambiguous env-file syntax and symlinked parents before writing.
    base = Path(base).absolute()
    if any(p.is_symlink() for p in (base, *base.parents)) or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(base)):
        raise ValueError("unsafe_fixture_path")
    base.mkdir(mode=0o700)
    private, artifacts = base / "private", base / "artifacts"
    for path in (private, artifacts, private / "certs", private / "home", private / "docker"):
        path.mkdir(mode=0o700)
    if paper_p8_recovery:
        for source_dir in (ROOT / 'scripts', ROOT / 'scripts/tests'):
            if str(source_dir) not in sys.path:
                sys.path.insert(0, str(source_dir))
        driver_spec = importlib.util.spec_from_file_location('paper_p8_e5_driver_recovery', ROOT / 'scripts/paper_p8_e5.py')
        driver = importlib.util.module_from_spec(driver_spec)
        driver_spec.loader.exec_module(driver)
        context_root = private / 'p8-recovery-context'
        context_root.mkdir(mode=0o700)
        _, paper_p8_e5_context = driver.prepare_repeat(context_root, 1, 813)
    project = "ncs_" + secrets.token_hex(6)
    values = dict(DEFAULTS)
    if remote_network_config is not None:
        values['API_DOMAIN'] = remote_network_config['advertised_host']
    if paper_profile:
        values.update(MQTT_INGRESS_ENABLED='true', DATABASE_MAX_CONNS='10',
                      MQTT_MAX_PAYLOAD_BYTES='65536')
    values['METRICS_ENABLED'] = 'true' if (paper_p1 or paper_p2 or paper_p4 or paper_p5a or paper_p6 or paper_p8_e4 or
        paper_p7 and paper_p7_mode in ('overhead_zero', 'overhead_two', 'p6_ws')) and metrics_enabled else 'false'
    if paper_p7 and paper_p7_mode == 'single_path':
        values.update(paper_p7_support().single_path_profile())
    elif paper_p7 and paper_p7_mode in ('telemetry_pilot', 'reconnect', 'lifecycle', 'slow_viewer', 'overhead_zero', 'overhead_two'):
        # Isolated P7 telemetry pilot or reconnect: explicit bounded ingress/microbatch/dedup profile.
        values.update(paper_p7_support().telemetry_pilot_profile())
    if paper_p2:
        values.update(SUPABASE_JWT_MODE='rs256', SUPABASE_JWKS_URL='http://supabase-envoy:8000/auth/v1/.well-known/jwks.json',
                      AUTH_UNIX_SOCKET_PATH='/run/iot-auth/auth.sock', GOTRUE_JWT_ISSUER='https://localhost/auth/v1',
                      SUPABASE_PUBLIC_URL='https://localhost', GOTRUE_SITE_URL='https://localhost',
                      CORS_ALLOWED_ORIGINS='https://client.example.test')
    values['WEBSOCKET_ENABLED'] = 'true' if (paper_p7 or paper_p8_sessions or paper_p8_slow or paper_p8_e1 or paper_p8_e5) else 'false'
    if paper_p8_sessions:
        values.update(paper_p8_support().profile())
    if paper_p8_slow:
        values.update(paper_p8_support().profile())
    if paper_p8_e1:
        values.update(paper_p8_support().profile())
        values['WS_MAX_SESSIONS'] = str(e1_resource_overlay['proposed']['WS_MAX_SESSIONS'])
        values['WS_SESSION_REFERENCE_BYTES'] = str(e1_resource_overlay['proposed']['WS_SESSION_REFERENCE_BYTES'])
        values.update(WS_HEARTBEAT_INTERVAL='2s', WS_PONG_TIMEOUT='4s', WS_WRITE_TIMEOUT='1s',
                      WS_RECHECK_INTERVAL='5s', WS_RECHECK_SWEEP_BUDGET='2s', WS_RECHECK_CONCURRENCY='2',
                      WS_FANOUT_MODE='indexed')
    if paper_p8_e2:
        if paper_p8_e2_workload == 'equal_samples':
            values.update(MQTT_SHAPER_ENABLED='false',
                          MQTT_MICROBATCH_ENABLED='true', MQTT_MICROBATCH_MAX_MESSAGES='32',
                          MQTT_MICROBATCH_MAX_ROWS='500', MQTT_MICROBATCH_MAX_PAYLOAD_BYTES='4194304',
                          MQTT_MICROBATCH_MAX_WAIT='50ms', MQTT_RAM_DEDUP_ENABLED='false',
                          STAGE5_ENABLED='false', STAGE5_WAKEUP_ENABLED='false', WEBSOCKET_ENABLED='false')
        else:
            shaper_rate = (paper_p8_e2_offered_rate + 1) // 2 if paper_p8_profile == 'official' else 2
            shaper_burst = max((paper_p8_e2_burst_messages + 1)//2, shaper_rate) if paper_p8_profile == 'official' else 4
            microbatch_rows = min(500, max(1, shaper_rate * paper_p8_e2_samples_per_message)) if paper_p8_profile == 'official' else 500
            values.update(MQTT_SHAPER_ENABLED='true', MQTT_SHAPER_RATE_PER_SECOND=str(shaper_rate), MQTT_SHAPER_BURST=str(shaper_burst),
                          MQTT_MICROBATCH_ENABLED='true', MQTT_MICROBATCH_MAX_MESSAGES='32',
                          MQTT_MICROBATCH_MAX_ROWS=str(microbatch_rows), MQTT_MICROBATCH_MAX_PAYLOAD_BYTES='4194304',
                          MQTT_MICROBATCH_MAX_WAIT='50ms', MQTT_RAM_DEDUP_ENABLED='false',
                          STAGE5_ENABLED='false', STAGE5_WAKEUP_ENABLED='false', WEBSOCKET_ENABLED='false')
    if paper_p8_e3:
        values.update(STAGE5_ENABLED='true', DATABASE_MAX_CONNS='12',
                      STAGE5_WAKEUP_ENABLED='true' if paper_p8_e3 == 'wakeup' else 'false',
                      MQTT_SHAPER_ENABLED='true', MQTT_SHAPER_RATE_PER_SECOND='2', MQTT_SHAPER_BURST='4',
                      MQTT_MICROBATCH_ENABLED='true', MQTT_MICROBATCH_MAX_MESSAGES='32',
                      MQTT_MICROBATCH_MAX_ROWS='500', MQTT_MICROBATCH_MAX_PAYLOAD_BYTES='4194304',
                       MQTT_MICROBATCH_MAX_WAIT='50ms', MQTT_RAM_DEDUP_ENABLED='false', WEBSOCKET_ENABLED='false')
    if paper_p8_e4:
        values.update(STAGE5_ENABLED='false', STAGE5_WAKEUP_ENABLED='false', MQTT_SHAPER_ENABLED='false',
                      MQTT_MICROBATCH_ENABLED='true', MQTT_MICROBATCH_MAX_MESSAGES='32',
                      MQTT_MICROBATCH_MAX_ROWS='500', MQTT_MICROBATCH_MAX_PAYLOAD_BYTES='4194304',
                      MQTT_MICROBATCH_MAX_WAIT='50ms', MQTT_RAM_DEDUP_ENABLED='true' if paper_p8_e4 == 'on' else 'false',
                       WEBSOCKET_ENABLED='true')
    if paper_p8_e5:
        scripts_dir = str(ROOT / 'scripts')
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        import paper_p8_e5_support as e5
        values.update(e5.e5_arm_profile(paper_p8_e5))
        values['METRICS_ENABLED'] = 'true'
    if paper_p3:
        values.update(STAGE5_ENABLED='true', DATABASE_MAX_CONNS='12')
        if paper_p3_microbatch:
            values['MQTT_MICROBATCH_ENABLED'] = 'true'
        if paper_p5b_control_regression:
            if not paper_p3_microbatch:
                raise ValueError('paper_p5b_control_requires_p5a_control')
            values.update(MQTT_RAM_DEDUP_ENABLED='true', MQTT_RAM_DEDUP_MAX_ENTRIES='100000',
                          MQTT_RAM_DEDUP_MAX_BYTES='67108864')
    if paper_p4:
        values.update(paper_p4_support().profile(paper_p4))
        values.update(MQTT_INGRESS_ENABLED='true', DATABASE_MAX_CONNS='10', MQTT_MAX_PAYLOAD_BYTES='65536',
                       MQTT_QUEUE_CAPACITY='256', MQTT_WORKER_COUNT='4', STAGE5_ENABLED='false')
    if paper_p5a:
        values.update(paper_p5a_support().p5b_profile(paper_p5b[0], dataset) if paper_p5b else
                      paper_p5a_support().profile(mode, dataset, shutdown_smoke=paper_p5a_shutdown,
                                                  cache_smoke=paper_p5b_cache_smoke))
    if paper_p6:
        values.update(paper_p6_support().profile(mode, dataset))
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
    nginx_main_config = None
    if paper_p8_e1:
        nginx_main_config = private / 'nginx.conf'
        proposal = e1_resource_overlay['proposed']
        private_write(nginx_main_config,
            f"worker_processes {proposal['nginx_worker_processes']};\n"
            f"worker_rlimit_nofile {proposal['nginx_process_fd_soft']};\n"
            "events {\n"
            f"    worker_connections {proposal['nginx_worker_connections']};\n"
            "}\n"
            "http {\n"
            "    include /etc/nginx/mime.types;\n"
            "    default_type application/octet-stream;\n"
            "    sendfile on;\n"
            "    include /etc/nginx/conf.d/*.conf;\n"
            "}\n")
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
        if service == 'mosquitto' and paper_p8_load:
            stanzas.append('    ports: !override\n      - target: 8883\n        published: "0"\n        host_ip: "127.0.0.1"\n        protocol: tcp\n')
        elif service != "nginx":
            stanzas.append("    ports: !override []\n")
        stanzas.append(f"    labels:\n      io.iot.native-smoke.run: ${{NCS_RUN:?required}}\n      io.iot.native-smoke.role: {service}\n      test.task: native-compose-startup\n      test.run: ${{NCS_RUN:?required}}\n")
        stanzas.append(extra_by_service.get(service, ""))
        if paper_p8_e1 and service in ('nginx', 'backend', 'mosquitto'):
            proposal = e1_resource_overlay['proposed']
            nofile = (proposal['nginx_process_fd_soft'] if service == 'nginx' else
                      proposal['backend_process_fd_limit'] if service == 'backend' else
                      proposal['broker_process_fd_limit'])
            stanzas.append(f"    ulimits: !override\n      nofile:\n        soft: {nofile}\n        hard: {nofile}\n")
        if paper_p8_e1 and service == 'nginx':
            stanzas.append(f"    volumes: !override\n      - type: bind\n        source: {nginx_main_config}\n"
                           "        target: /etc/nginx/nginx.conf\n        read_only: true\n"
                           "        bind:\n          create_host_path: false\n")
        if service == 'backend' and paper_p1:
            stanzas.append('    environment:\n      METRICS_ENABLED: ${METRICS_ENABLED:?required}\n')
    private_write(override, "".join(stanzas) + template[resource_offset:])
    # Copy only the reviewed broker config, never deployment certificates.
    broker_config = (ROOT / "config/mosquitto/mosquitto.conf").read_text()
    if (paper_p4 or paper_p5a or paper_p6) and 'log_type debug' not in broker_config:
        # Mosquitto 2.0.18 emits the broker-side Denied PUBLISH audit line only
        # at debug level. This is confined to the disposable acceptance fixture.
        broker_config = broker_config.rstrip() + '\nlog_type debug\n'
    private_write(private / "mosquitto.conf", broker_config)
    art_stat = artifacts.stat()
    fixture = Fixture(base, private, artifacts, env_file, override, project, values,
                      art_stat.st_dev, art_stat.st_ino, paper_p0, paper_p1, paper_p2_spike, paper_p2, paper_p3,
                      paper_p4 is not None, paper_p4, paper_p5a is not None,
                      mode if paper_p5a else None, dataset if paper_p5a else None,
                       paper_p3_microbatch, paper_p5a_shutdown, paper_p5b_cache_smoke,
                        paper_p5b_control_regression)
    object.__setattr__(fixture, 'paper_p7', paper_p7)
    object.__setattr__(fixture, 'paper_p7_mode', paper_p7_mode or 'handshake_only')
    object.__setattr__(fixture, 'paper_p8_sessions', paper_p8_sessions)
    object.__setattr__(fixture, 'paper_p8_slow', paper_p8_slow)
    object.__setattr__(fixture, 'paper_p8_load', paper_p8_load)
    object.__setattr__(fixture, 'paper_p8_e2', paper_p8_e2)
    object.__setattr__(fixture, 'paper_p8_e2_workload', paper_p8_e2_workload)
    object.__setattr__(fixture, 'paper_p8_e2_context', str(paper_p8_e2_context) if paper_p8_e2_context else None)
    object.__setattr__(fixture, 'paper_p8_e2_reference', str(paper_p8_e2_reference) if paper_p8_e2_reference else None)
    object.__setattr__(fixture, 'paper_p8_profile', paper_p8_profile)
    object.__setattr__(fixture, 'paper_p8_official_opt_in', bool(paper_p8_official_opt_in))
    object.__setattr__(fixture, 'paper_p8_e2_offered_rate', paper_p8_e2_offered_rate)
    object.__setattr__(fixture, 'paper_p8_e2_duration_seconds', paper_p8_e2_duration_seconds)
    object.__setattr__(fixture, 'paper_p8_e2_burst_messages', paper_p8_e2_burst_messages)
    object.__setattr__(fixture, 'paper_p8_e2_samples_per_message', paper_p8_e2_samples_per_message)
    object.__setattr__(fixture, 'paper_p8_e3', paper_p8_e3)
    object.__setattr__(fixture, 'paper_p8_e3_workload', paper_p8_e3_workload)
    object.__setattr__(fixture, 'paper_p8_e3_command_count', paper_p8_e3_command_count)
    object.__setattr__(fixture, 'paper_p8_e3_schedule', paper_p8_e3_schedule)
    object.__setattr__(fixture, 'paper_p8_e4', paper_p8_e4)
    object.__setattr__(fixture, 'paper_p8_e4_cache_mode', paper_p8_e4)
    object.__setattr__(fixture, 'paper_p8_e5', paper_p8_e5)
    object.__setattr__(fixture, 'paper_p8_e5_context', str(paper_p8_e5_context) if paper_p8_e5_context else None)
    object.__setattr__(fixture, 'paper_p8_e5_reference', str(paper_p8_e5_reference) if paper_p8_e5_reference else None)
    object.__setattr__(fixture, 'paper_p8_recovery', paper_p8_recovery)
    object.__setattr__(fixture, 'paper_p8_e1', paper_p8_e1)
    object.__setattr__(fixture, 'e1_resource_overlay', e1_resource_overlay)
    object.__setattr__(fixture, 'e1_nginx_main_config', str(nginx_main_config) if nginx_main_config else None)
    object.__setattr__(fixture, 'paper_p8_e1_viewers', paper_p8_e1_viewers)
    object.__setattr__(fixture, 'paper_p8_e1_principals', paper_p8_e1_principals)
    object.__setattr__(fixture, 'paper_p8_e1_density', paper_p8_e1_density)
    object.__setattr__(fixture, 'paper_p8_e1_messages_per_target', paper_p8_e1_messages_per_target)
    object.__setattr__(fixture, 'remote_network_config', remote_network_config)
    if paper_p5b is not None:
        object.__setattr__(fixture, 'paper_p5b', True)
        object.__setattr__(fixture, 'paper_p5b_cache_mode', paper_p5b[0])
    if paper_p6 is not None:
        object.__setattr__(fixture, 'paper_p6', True)
        object.__setattr__(fixture, 'paper_p6_mode', paper_p6[0])
        object.__setattr__(fixture, 'paper_p6_dataset', paper_p6[1])
    if paper_p2_spike or paper_p2:
        paper_p2_support().prepare_spike(sys.modules[__name__], fixture)
    if paper_p2:
        paper_p2_support().prepare_perimeter(sys.modules[__name__], fixture)
    if paper_p5a:
        metadata = (paper_p5a_support().p5b_run_metadata(paper_p5b[0], dataset,
                                                         effective_environment=fixture.values,
                                                         source_revision=paper_p0_git(fixture, 'rev-parse', 'HEAD'),
                                                         dirty_worktree=bool(paper_p0_git(fixture, 'status', '--porcelain')))
                    if paper_p5b else
                    paper_p5a_support().run_metadata(mode, dataset, shutdown_smoke=paper_p5a_shutdown,
                                                     cache_smoke=paper_p5b_cache_smoke,
                                                     effective_environment=fixture.values))
        metadata.update(schema_version=1, run_id=fixture.project, status='PREPARED_NOT_EXECUTED',
                        native_executed=False)
        durable_json(fixture.artifacts / 'run.json', metadata)
    if paper_p6:
        metadata = paper_p6_support().run_metadata(
            paper_p6[0], paper_p6[1], effective_environment=fixture.values,
            source_revision=paper_p0_git(fixture, 'rev-parse', 'HEAD'),
            dirty_worktree=bool(paper_p0_git(fixture, 'status', '--porcelain')))
        metadata.update(run_id=fixture.project)
        durable_json(fixture.artifacts / 'run.json', metadata)
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


def paper_p3_acceptance_report(project, evidence, cleanup_verified, microbatch_enabled=False,
                               cleanup_counts=None, p5b_cache_enabled=False):
    p3_ok = cleanup_verified is True and evidence.get('full_p3_accepted') is True
    regression_ok = (not microbatch_enabled or
                     evidence.get('microbatch_control_regression_accepted') is True)
    accepted = p3_ok and regression_ok
    return {'schema_version': 1,
            'status': ('PAPER_P5B_CONTROL_PRESERVATION_ACCEPTED' if p5b_cache_enabled else
                       'PAPER_P5A_CONTROL_REGRESSION_ACCEPTED' if microbatch_enabled else 'PAPER_P3_ACCEPTED')
                      if accepted else 'FAIL',
            'project': project, 'native_executed': True,
            'full_p3_accepted': evidence.get('full_p3_accepted') is True,
            'microbatch_enabled': microbatch_enabled,
            'ram_dedup_enabled': p5b_cache_enabled,
            'microbatch_control_regression_accepted': (evidence.get('microbatch_control_regression_accepted')
                                                        if microbatch_enabled else None),
            'effective_optimizations': (['mqtt_microbatch', 'mqtt_ram_dedup'] if p5b_cache_enabled else
                                        ['mqtt_microbatch'] if microbatch_enabled else []),
            'exact_cleanup_verified': cleanup_verified is True,
            'cleanup_counts': cleanup_counts or {}}


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
            str(ROOT / "migrations/000024_telemetry_ledger_timestamp.up.sql"): "/migrations/000024_telemetry_ledger_timestamp.up.sql",
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
    if getattr(fixture, 'paper_p8_e1', False):
        binds['nginx'][fixture.e1_nginx_main_config] = '/etc/nginx/nginx.conf'
    paper_p8_remote_support().validate_effective_ports(services,
        remote_config=getattr(fixture, 'remote_network_config', None), paper_p2=fixture.paper_p2,
        paper_p8_load=getattr(fixture, 'paper_p8_load', False))
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
        if getattr(fixture, 'paper_p8_e1', False) and name in ('nginx', 'backend', 'mosquitto'):
            proposal = fixture.e1_resource_overlay['proposed']
            expected_nofile = (proposal['nginx_process_fd_soft'] if name == 'nginx' else
                proposal['backend_process_fd_limit'] if name == 'backend' else
                proposal['broker_process_fd_limit'])
            nofile = service.get('ulimits', {}).get('nofile', {})
            if nofile.get('soft') != expected_nofile or nofile.get('hard') != expected_nofile:
                raise ValueError('e1_resource_overlay_compose_ulimit_mismatch:' + name)
    backend = services["backend"]
    if getattr(fixture, 'paper_p8_e1', False):
        env = backend.get('environment', {})
        if (str(env.get('WS_MAX_SESSIONS')) != str(fixture.e1_resource_overlay['proposed']['WS_MAX_SESSIONS'])
                or str(env.get('WS_SESSION_REFERENCE_BYTES')) != str(fixture.e1_resource_overlay['proposed']['WS_SESSION_REFERENCE_BYTES'])):
            raise ValueError('e1_resource_overlay_backend_environment_mismatch')
    if fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2_spike or fixture.paper_p2 or fixture.paper_p6 or fixture.paper_p7 or getattr(fixture, 'paper_p8_e5', None):
        env = backend.get('environment', {})
        expected = {'MQTT_INGRESS_ENABLED': 'true', 'STAGE5_ENABLED': 'true' if fixture.paper_p3 or fixture.paper_p6 or getattr(fixture, 'paper_p8_e3', None) or getattr(fixture, 'paper_p8_e5', None) else 'false',
                    'MQTT_CREDENTIAL_RUNTIME_ENABLED': 'false', 'MQTT_CREDENTIAL_API_ENABLED': 'true',
                     'DATABASE_MAX_CONNS': '12' if fixture.paper_p3 or fixture.paper_p6 or getattr(fixture, 'paper_p8_e3', None) else '11' if getattr(fixture, 'paper_p8_e5', None) else '10', 'MQTT_QUEUE_CAPACITY': '256',
                'MQTT_WORKER_COUNT': '1' if (fixture.paper_p7 and fixture.paper_p7_mode in ('telemetry_pilot', 'single_path', 'reconnect', 'lifecycle', 'slow_viewer', 'overhead_zero', 'overhead_two')) else '4',
                    'MQTT_MAX_PAYLOAD_BYTES': '65536'}
        if (any(str(env.get(k)).lower() != v for k, v in expected.items())
                or backend.get('cpus') != 2 or str(backend.get('mem_limit')) != '4294967296'):
            raise ValueError('paper_p0_profile_rejected')
        if fixture.paper_p3_microbatch and str(env.get('MQTT_MICROBATCH_ENABLED')).lower() != 'true':
            raise ValueError('paper_p3_microbatch_profile_rejected')
        if getattr(fixture, 'paper_p8_e4', None):
            expected_e4 = {'STAGE5_ENABLED': 'false', 'STAGE5_WAKEUP_ENABLED': 'false',
                'WEBSOCKET_ENABLED': 'true', 'MQTT_SHAPER_ENABLED': 'false',
                'MQTT_MICROBATCH_ENABLED': 'true',
                'MQTT_RAM_DEDUP_ENABLED': 'true' if fixture.paper_p8_e4 == 'on' else 'false',
                'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
                'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304', 'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
            if any(str(env.get(k)).lower() != v for k, v in expected_e4.items()):
                raise ValueError('paper_p8_e4_profile_rejected')
        if getattr(fixture, 'paper_p8_e5', None):
            import paper_p8_e5_support as e5
            expected_e5 = e5.e5_arm_profile(fixture.paper_p8_e5)
            expected_e5['METRICS_ENABLED'] = 'true'
            if any(str(env.get(k, '')).lower() != str(v).lower() for k, v in expected_e5.items()):
                raise ValueError('paper_p8_e5_profile_rejected')
        if fixture.paper_p7 and fixture.paper_p7_mode in ('telemetry_pilot', 'single_path', 'reconnect', 'lifecycle', 'slow_viewer', 'overhead_zero', 'overhead_two'):
            microbatch_expected = 'false' if fixture.paper_p7_mode == 'single_path' else 'true'
            if any(str(env.get(key)).lower() != value for key, value in {
                    'MQTT_MICROBATCH_ENABLED': microbatch_expected, 'MQTT_RAM_DEDUP_ENABLED': 'true',
                    'MQTT_RAM_DEDUP_MAX_ENTRIES': '100000',
                    'MQTT_RAM_DEDUP_MAX_BYTES': '67108864',
                    'MQTT_WORKER_COUNT': '1'}.items()):
                raise ValueError('paper_p7_telemetry_profile_rejected')
        if fixture.paper_p5b_control_regression and any(
                str(env.get(key)).lower() != fixture.values[key].lower()
                for key in ('MQTT_RAM_DEDUP_ENABLED', 'MQTT_RAM_DEDUP_MAX_ENTRIES', 'MQTT_RAM_DEDUP_MAX_BYTES')):
            raise ValueError('paper_p5b_control_profile_rejected')
        if (fixture.paper_p1 or fixture.paper_p4 or fixture.paper_p5a or fixture.paper_p6 or
                fixture.paper_p7 and fixture.paper_p7_mode in ('overhead_zero', 'overhead_two', 'p6_ws')) and str(env.get('METRICS_ENABLED')).lower() != fixture.values['METRICS_ENABLED']:
            raise ValueError('paper_p1_profile_rejected')
        if fixture.paper_p2:
            paper_p2_support().validate_perimeter(fixture, services)
        if fixture.paper_p4:
            paper_p4_support().validate_effective_environment(fixture, env)
        if fixture.paper_p5b:
            paper_p5a_support().p5b_run_metadata(fixture.paper_p5b_cache_mode,
                fixture.paper_p5a_dataset, effective_environment=env)
        elif fixture.paper_p5a:
            paper_p5a_support().validate_effective_profile(env, fixture.paper_p5a_mode,
                fixture.paper_p5a_dataset, shutdown_smoke=fixture.paper_p5a_shutdown,
                cache_smoke=fixture.paper_p5b_cache_smoke)
        if fixture.paper_p6:
            paper_p6_support().validate_effective_profile(env, fixture.paper_p6_mode,
                                                          fixture.paper_p6_dataset)
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
                "supabase-compat-migration": ["/bin/sh", "-ec", 'psql -h postgres -U "postgres" -d "smoke" -f /migrations/000004_supabase_compat.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000021_admin_user_management.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000022_gateway_membership_revocation.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000023_sensor_control_intent.up.sql && psql -h postgres -U "postgres" -d "smoke" -f /migrations/000024_telemetry_ledger_timestamp.up.sql']}
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


def requires_gateway_image(fixture):
    return bool(fixture.paper_p0 or fixture.paper_p1 or fixture.paper_p2 or
                getattr(fixture, 'paper_p8_load', False) or
                (fixture.paper_p7 and fixture.paper_p7_mode in ('telemetry_pilot', 'single_path', 'reconnect', 'lifecycle', 'slow_viewer', 'overhead_zero', 'overhead_two')))


def _load_e5_reference(path, fixture):
    path = Path(path)
    try:
        st = path.lstat()
        if path.is_symlink() or not path.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o077 or st.st_size > 2_000_000:
            raise ValueError('e5_reference_file_rejected')
        report = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        raise ValueError('e5_reference_artifact_invalid') from None
    if (not isinstance(report, dict) or report.get('status') != 'PASS' or report.get('arm') != 'optimized'
            or report.get('cleanup_verified') is not True or report.get('full_p8_accepted') is not False
            or not paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True)):
        raise ValueError('e5_reference_artifact_rejected')
    current = paper_p8_support()._source_fingerprint(ROOT)
    old = report.get('source_fingerprint')
    if (not isinstance(old, dict) or not old.get('revision') or not old.get('dirty_source_sha256')
            or old.get('revision') != current.get('revision')
            or old.get('dirty_source_sha256') != current.get('dirty_source_sha256')):
        raise ValueError('e5_reference_source_mismatch')
    context_path = Path(fixture.paper_p8_e5_context)
    context_stat = context_path.lstat()
    if context_path.is_symlink() or not context_path.is_file() or context_stat.st_uid != os.getuid() or context_stat.st_mode & 0o077 or context_stat.st_size > 16384:
        raise ValueError('e5_dataset_context_not_private')
    reference_context = report.get('context') or report.get('dataset_context')
    context = json.loads(context_path.read_text(encoding='utf-8'))
    dataset_path = Path(context.get('dataset_path', ''))
    dataset_stat = dataset_path.lstat()
    dataset_raw = dataset_path.read_bytes()
    if dataset_path.is_symlink() or dataset_stat.st_uid != os.getuid() or dataset_stat.st_mode & 0o077 or len(dataset_raw) > 2_000_000 or hashlib.sha256(dataset_raw).hexdigest() != context.get('sha256'):
        raise ValueError('e5_reference_context_mismatch')
    metadata = report.get('metadata') if isinstance(report.get('metadata'), dict) else {}
    workload = metadata.get('workload') if isinstance(metadata.get('workload'), dict) else {}
    if reference_context:
        matches = all(reference_context.get(k) == context.get(k) for k in ('sha256', 'seed', 'anchor'))
    else:
        matches = (workload.get('journal_sha256') == context.get('sha256')
            and metadata.get('seed') == context.get('seed')
            and (workload.get('telemetry') or {}).get('dataset_anchor') == context.get('anchor'))
    if not matches:
        raise ValueError('e5_reference_context_mismatch')
    ids = report.get('image_ids') or (report.get('metadata') or {}).get('image_ids')
    if not isinstance(ids, dict) or any(not isinstance(ids.get(k), str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', ids[k]) for k in ('backend', 'gateway')):
        raise ValueError('e5_reference_images_invalid')
    return {'image_ids': ids, 'report': report}


def _reuse_e5_image(registry, image_ids, role, tag, create_tag=True):
    expected = image_ids.get(role)
    if not isinstance(expected, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', expected):
        raise ValueError('e5_reference_images_invalid')
    inspected = registry.command(['image', 'inspect', expected])
    if inspected.returncode:
        raise ValueError('e5_reference_image_unavailable')
    try:
        actual = json.loads(inspected.stdout)[0].get('Id')
    except (ValueError, IndexError, TypeError, AttributeError):
        raise ValueError('e5_reference_image_unavailable') from None
    if actual != expected:
        raise ValueError('e5_reference_image_mismatch')
    if create_tag:
        existing = registry.command(['image', 'inspect', tag])
        if existing.returncode == 0:
            try:
                existing_id = json.loads(existing.stdout)[0].get('Id')
            except (ValueError, IndexError, TypeError, AttributeError):
                raise ValueError('e5_reference_target_tag_collision') from None
            if existing_id != expected:
                raise ValueError('e5_reference_target_tag_collision')
        else:
            tagged = registry.command(['image', 'tag', expected, tag])
            if tagged.returncode:
                raise ValueError('e5_reference_tag_failed')
            verify = registry.command(['image', 'inspect', tag])
            try:
                if verify.returncode or json.loads(verify.stdout)[0].get('Id') != expected:
                    raise ValueError('e5_reference_tag_failed')
            except (ValueError, IndexError, TypeError, AttributeError):
                raise ValueError('e5_reference_tag_failed') from None
    return expected


def _load_e2_reference(path, fixture):
    context_path = Path(fixture.paper_p8_e2_context)
    st = context_path.lstat()
    if context_path.is_symlink() or not context_path.is_file() or st.st_uid != os.getuid() or (st.st_mode & 0o077) or st.st_size > 65536:
        raise ValueError('paper_p8_e2_context_invalid')
    context = json.loads(context_path.read_text(encoding='utf-8'))
    validated_ctx = paper_p8_e2_support().validate_e2_equal_samples_context(context, check_freshness=False)
    current_fp = paper_p8_support()._source_fingerprint(ROOT)
    return paper_p8_e2_support().validate_e2_reference(path, validated_ctx, current_source_fp=current_fp)


def _reuse_e2_binary(fixture, e2_reference):
    bin_path = e2_reference.get('binary_path')
    expected_sha256 = e2_reference.get('binary_sha256')
    if not bin_path or not expected_sha256:
        raise ValueError('e2_reference_binary_missing')
    bin_file = Path(bin_path)
    if bin_file.is_symlink() or not bin_file.is_file() or bin_file.stat().st_uid != os.getuid() or (bin_file.stat().st_mode & 0o077):
        raise ValueError('e2_reference_binary_missing')
    raw = bin_file.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('e2_reference_binary_mismatch')
    (fixture.private / 'bin').mkdir(mode=0o700, exist_ok=True)
    dest = fixture.private / 'bin' / 'paperload'
    dest.write_bytes(raw)
    dest.chmod(0o700)
    (fixture.artifacts / 'bin').mkdir(mode=0o700, exist_ok=True)
    retained = fixture.artifacts / 'bin' / 'paperload'
    retained.write_bytes(raw)
    retained.chmod(0o700)
    if hashlib.sha256(dest.read_bytes()).hexdigest() != expected_sha256:
        raise ValueError('e2_reference_binary_mismatch')
    return dest


def _retain_e2_binary(fixture, paperload_binary):
    if not paperload_binary or not Path(paperload_binary).is_file():
        return
    (fixture.artifacts / 'bin').mkdir(mode=0o700, exist_ok=True)
    retained = fixture.artifacts / 'bin' / 'paperload'
    raw = Path(paperload_binary).read_bytes()
    retained.write_bytes(raw)
    retained.chmod(0o700)



def startup_only(fixture, registry=None, positive_http=False, role_http=False, full_http=False,
                 hold_callback=None, finalization_callback=None, pre_cleanup_callback=None):
    """Phased real startup; optional hold returns through the same cleanup finally."""
    lifecycle = bootstrap_lifecycle()
    if registry is None:
        registry = ResourceRegistry(fixture)
    phase, failure, evidence = 'preflight', None, {}
    containers = {}
    images = {}
    paperload_binary = None
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
        reuse_e5 = bool(getattr(fixture, 'paper_p8_e5_reference', None))
        reuse_e2 = bool(getattr(fixture, 'paper_p8_e2', False) and
                        getattr(fixture, 'paper_p8_e2_workload', '') == 'equal_samples' and
                        getattr(fixture, 'paper_p8_e2_reference', None))
        if getattr(fixture, 'paper_p8_e5', None) not in (None, 'optimized') and not reuse_e5:
            raise ValueError('e5_reference_required')
        if reuse_e5:
            reference = _load_e5_reference(fixture.paper_p8_e5_reference, fixture)
            for name, image_id in images.items():
                if reference['image_ids'].get(name) != image_id:
                    raise ValueError('e5_reference_image_mismatch')
            phase = 'build'
            _reuse_e5_image(registry, reference['image_ids'], 'backend', fixture.project + '-backend')
            images['backend'] = reference['image_ids']['backend']
        elif reuse_e2:
            e2_ref = _load_e2_reference(fixture.paper_p8_e2_reference, fixture)
            for name, image_id in images.items():
                ref_img = e2_ref['image_ids'].get(name)
                if ref_img is not None and ref_img != image_id:
                    raise ValueError('e2_reference_image_mismatch')
            phase = 'build'
            _reuse_e5_image(registry, e2_ref['image_ids'], 'backend', fixture.project + '-backend')
            inspect_res = registry.command(['image', 'inspect', fixture.project + '-backend'])
            if inspect_res.returncode:
                raise ValueError('e2_reference_image_unavailable')
            actual_backend_id = json.loads(inspect_res.stdout)[0].get('Id')
            if actual_backend_id != e2_ref['image_ids']['backend']:
                raise ValueError('e2_reference_image_mismatch')
            images['backend'] = actual_backend_id
        else:
            phase = 'build'
            build_native_image(fixture, prevalidated=True)
            result = registry.command(['image', 'inspect', fixture.project + '-backend'])
            if result.returncode:
                raise ValueError('backend_build_image_unresolved')
            images['backend'] = json.loads(result.stdout)[0]['Id']
        if requires_gateway_image(fixture):
            # Same Dockerfile builder, but the real Gateway target, not a host binary.
            if reuse_e5:
                _reuse_e5_image(registry, reference['image_ids'], 'gateway', fixture.project + '-gateway', create_tag=False)
                images['gateway'] = reference['image_ids']['gateway']
            elif reuse_e2:
                _reuse_e5_image(registry, e2_ref['image_ids'], 'gateway', fixture.project + '-gateway', create_tag=False)
                inspect_res = registry.command(['image', 'inspect', e2_ref['image_ids']['gateway']])
                if inspect_res.returncode:
                    raise ValueError('e2_reference_image_unavailable')
                actual_gateway_id = json.loads(inspect_res.stdout)[0].get('Id')
                if actual_gateway_id != e2_ref['image_ids']['gateway']:
                    raise ValueError('e2_reference_image_mismatch')
                images['gateway'] = actual_gateway_id
            else:
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
        paperload_binary = None
        if getattr(fixture, 'paper_p8_load', False):
            if reuse_e2:
                paperload_binary = _reuse_e2_binary(fixture, e2_ref)
            else:
                go_binary = shutil.which('go')
                if not go_binary:
                    raise ValueError('paperload_build_failed')
                (fixture.private / 'bin').mkdir(mode=0o700, exist_ok=True)
                paperload_binary = build_paperload(fixture, go_binary)
                if getattr(fixture, 'paper_p8_e3', None) or getattr(fixture, 'paper_p8_e4', None) or getattr(fixture, 'paper_p8_e5', None):
                    gateway_binary = build_paper_p8_e3_gateway(fixture, go_binary)
                if getattr(fixture, 'paper_p8_e2', False) and getattr(fixture, 'paper_p8_e2_workload', '') == 'equal_samples':
                    _retain_e2_binary(fixture, paperload_binary)

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
                remote_profile = getattr(fixture, 'remote_network_config', None)
                if remote_profile:
                    https_port = paper_p8_remote_support().validate_runtime_mapping(
                        'nginx', obj['NetworkSettings'].get('Ports') or {}, remote_profile)
                    broker = registry.resolve(containers['mosquitto'])
                    mqtt_port = paper_p8_remote_support().validate_runtime_mapping(
                        'mosquitto', broker['NetworkSettings'].get('Ports') or {}, remote_profile)
                    ca_file = fixture.private / 'p2-ca.crt'
                    paper_p8_remote_support().probe_tls(remote_profile['bind_ip'], https_port,
                        remote_profile['advertised_host'], ca_file)
                    paper_p8_remote_support().probe_tls(remote_profile['bind_ip'], mqtt_port,
                        remote_profile['advertised_host'], fixture.private / 'certs' / 'ca.crt')
                    url = f"https://{remote_profile['bind_ip']}:{https_port}"
                    evidence['remote_endpoints'] = {
                        'https': remote_profile['advertised_https'],
                        'mqtt_tls': remote_profile['advertised_mqtt_tls'],
                        'internal_https_probe': url,
                        'runtime_bind_ip': remote_profile['bind_ip'],
                        'runtime_https_port': https_port, 'runtime_mqtt_tls_port': mqtt_port,
                        'https_hostname_verified': True, 'mqtt_tls_hostname_verified': True}
                else:
                    ports = obj['NetworkSettings']['Ports'].get('443/tcp' if fixture.paper_p2 else '80/tcp') or []
                    if len(ports) != 1 or ports[0]['HostIp'] != '127.0.0.1' or not 0 < int(ports[0]['HostPort']) < 65536:
                        raise ValueError('mapped_endpoint_rejected')
                    url = ('https' if fixture.paper_p2 else 'http') + '://127.0.0.1:' + ports[0]['HostPort']
                endpoint = HTTPSEndpoint(url, fixture.private / 'p2-ca.crt') if fixture.paper_p2 else url
                evidence['endpoint'] = url
                if fixture.paper_p5a:
                    phase = 'paper_p5a'
                    evidence['paper_p5a'] = paper_p5a_support().run(
                        sys.modules[__name__], fixture, endpoint, registry, containers, images, state)
                elif fixture.paper_p6 and fixture.paper_p7_mode == 'p6_ws':
                    phase = 'paper_p6_ws'
                    evidence['paper_p6_ws'] = paper_p7_support().run_p6_ws(
                        sys.modules[__name__], fixture, endpoint, registry, containers, images, state)
                elif fixture.paper_p6:
                    phase = 'paper_p6'
                    if fixture.paper_p6_recovery_smoke:
                        evidence['paper_p6_recovery'] = paper_p6_support().run_recovery_smoke(
                            sys.modules[__name__], fixture, endpoint, registry, containers, images, state)
                    else:
                        evidence['paper_p6'] = paper_p6_support().run(
                            sys.modules[__name__], fixture, endpoint, registry, containers, images, state)
                elif fixture.paper_p4:
                    phase = 'paper_p4'
                    evidence['paper_p4'] = paper_p4_support().run(sys.modules[__name__], fixture,
                                                                  endpoint, registry, containers, images, state)
                elif fixture.paper_p3:
                    phase = 'paper_p3'
                    import paper_p3_support
                    evidence['paper_p3'] = paper_p3_support.run(sys.modules[__name__], fixture,
                        registry, endpoint, containers, images, state)
                elif fixture.paper_p7:
                    phase = 'paper_p7'
                    evidence['paper_p7'] = dispatch_paper_fixture(fixture, paper_p7_support(),
                        sys.modules[__name__], fixture, registry, endpoint, containers, images, state)
                elif fixture.paper_p8_sessions:
                    phase = 'paper_p8_sessions'
                    evidence['paper_p8_sessions'] = paper_p8_support().run(
                        sys.modules[__name__], fixture, endpoint, registry, containers, images, state)
                elif fixture.paper_p8_slow:
                    phase = 'paper_p8_slow_viewer'
                    evidence['paper_p8_slow'] = paper_p8_slow_support().run(
                        sys.modules[__name__], fixture, endpoint, registry, containers, images, state)
                elif getattr(fixture, 'paper_p8_load', False) and not getattr(fixture, 'paper_p8_prepare_only', False):
                    if getattr(fixture, 'paper_p8_e5', None):
                        phase = 'paper_p8_e5_' + fixture.paper_p8_e5
                        evidence['paper_p8_e5'] = dispatch_paper_p8_load(
                            fixture, sys.modules[__name__], endpoint, registry, containers, images, state,
                            paperload_binary, gateway_binary)
                    elif getattr(fixture, 'paper_p8_e4', None):
                        phase = 'paper_p8_e4_' + fixture.paper_p8_e4
                        evidence['paper_p8_e4'] = dispatch_paper_p8_load(
                            fixture, sys.modules[__name__], endpoint, registry, containers, images,
                            state, paperload_binary, gateway_binary)
                    elif getattr(fixture, 'paper_p8_e3', None):
                        phase = 'paper_p8_e3_' + fixture.paper_p8_e3
                        evidence['paper_p8_e3'] = dispatch_paper_p8_load(
                            fixture, sys.modules[__name__], endpoint, registry, containers, images,
                            state, paperload_binary, gateway_binary)
                    elif getattr(fixture, 'paper_p8_e1', False):
                        phase = 'paper_p8_e1'
                        evidence['paper_p8_e1'] = dispatch_paper_p8_load(
                            fixture, sys.modules[__name__], endpoint, registry, containers, images,
                            state, paperload_binary)
                    elif getattr(fixture, 'paper_p8_e2', False):
                        phase = 'paper_p8_e2_pilot'
                        evidence['paper_p8_e2'] = dispatch_paper_p8_load(
                            fixture, sys.modules[__name__], endpoint, registry, containers, images,
                            state, paperload_binary)
                    else:
                        phase = 'paper_p8_e2_telemetry100'
                        evidence['paper_p8_load'] = dispatch_paper_p8_load(
                            fixture, sys.modules[__name__], endpoint, registry, containers, images,
                            state, paperload_binary)
                elif fixture.paper_p2 and not getattr(fixture, 'paper_p8_prepare_only', False):
                    phase = 'paper_p2'
                    evidence['paper_p2'] = dispatch_paper_fixture(fixture, paper_p2_support(),
                        sys.modules[__name__], fixture,
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
                # P8 foreground adapter hook: preparation can complete before
                # READY; returning (or raising) still enters exact-receipt cleanup.
                if hold_callback is not None:
                    phase = 'ready_hold'
                    hold_callback(fixture=fixture, registry=registry, containers=containers,
                                  images=images, evidence=evidence, endpoint=endpoint, state=state,
                                  paperload_binary=paperload_binary)
            except BaseException as exc:
                failure = exc
                code = sanitize_failure_code(exc)
                try:
                    durable_json(fixture.private / 'startup-failure.json', {
                        'phase': phase, 'error_type': sanitize_failure_type(exc),
                        'error_code': code})
                    durable_json(fixture.private / 'startup-exception-private.json', {
                        'type': type(exc).__name__, 'message': str(exc)[:2048],
                        'traceback': traceback.format_exc()[-12000:]})
                    if phase in containers and containers[phase]['allocation_attempted']:
                        obj = registry.resolve(containers[phase])
                        if obj:
                            durable_json(fixture.private / 'failed-phase-state.json', obj['State'])
                            health = obj['State'].get('Health', {})
                            durable_json(fixture.private / 'failed-phase-health-private.json', {
                                'status': str(health.get('Status', ''))[:64],
                                'log': [{key: str(entry.get(key, ''))[:2048] for key in
                                         ('Start', 'End', 'ExitCode', 'Output')}
                                        for entry in health.get('Log', [])[-8:] if isinstance(entry, dict)]})
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
                if pre_cleanup_callback is not None:
                    try:
                        pre_cleanup_callback()
                    except Exception:
                        pass  # Diagnostics must not prevent owned-resource cleanup.
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
                        try:
                            durable_json(fixture.private / 'resource-receipts.json', [
                                {k: (str(v) if isinstance(v, Path) else v) for k, v in item.items()}
                                for item in registry.items
                            ])
                        except Exception:
                            pass
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
                    if finalization_callback is not None:
                        try:
                            finalization_callback(cleanup_verified=False,
                                cleanup_error=sanitize_failure_code(cl_exc),
                                workload_error=primary_failure)
                        except Exception:
                            pass
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
                # Session-state persistence is not Docker cleanup. Keep the
                # verified resource result intact if this optional hook fails.
                if finalization_callback is not None:
                    finalization_callback(cleanup_verified=True, cleanup_error=None,
                                          workload_error=primary_failure)
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
    if fixture.paper_p1 or fixture.paper_p2 or fixture.paper_p3 or fixture.paper_p4 or fixture.paper_p5a or fixture.paper_p6 or fixture.paper_p8_sessions or fixture.paper_p8_slow or getattr(fixture, 'paper_p8_load', False):
        path = fixture.artifacts / 'run.json'
        run = json.loads(path.read_text()) if path.exists() else {
            'schema_version': 1, 'run_id': fixture.project,
            'mode': (fixture.paper_p5a_mode if fixture.paper_p5a else
                     fixture.paper_p4_mode if fixture.paper_p4 else fixture.paper_p1)}
        prepare_only = getattr(fixture, 'paper_p8_prepare_only', False)
        remote_session = (json.loads((fixture.private / 'session.json').read_text())
                          if prepare_only and (fixture.private / 'session.json').is_file() else {})
        remote_outcome = remote_session.get('workload_outcome', 'NOT_RUN')
        prepare_experiment = remote_session.get('experiment',
            'E5' if getattr(fixture, 'paper_p8_e5_prepare_only', False) else
            'E4' if getattr(fixture, 'paper_p8_e4_prepare_only', False) else
            'E3' if getattr(fixture, 'paper_p8_e3_prepare_only', False) else
            'E2' if getattr(fixture, 'paper_p8_e2', False) else 'E1')
        e1_cleanup_accepted = (cleanup_verified if prepare_only else
            paper_p8_e1_cleanup_acceptance(fixture, evidence.get('paper_p8_e1', {}),
                cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e1', False) else None)
        e2_cleanup_accepted = (paper_p8_e2_cleanup_acceptance(
            fixture, evidence.get('paper_p8_e2', {}), cleanup_counts, cleanup_verified, registry)
            if getattr(fixture, 'paper_p8_e2', False) else None)
        e3_cleanup_accepted = (paper_p8_e3_cleanup_acceptance(
            fixture, evidence.get('paper_p8_e3', {}), cleanup_counts, cleanup_verified, registry)
            if getattr(fixture, 'paper_p8_e3', None) else None)
        e4_cleanup_accepted = (cleanup_verified if prepare_only and prepare_experiment == 'E4' else
            paper_p8_e4_cleanup_acceptance(
                fixture, evidence.get('paper_p8_e4', {}), cleanup_counts, cleanup_verified, registry)
            if getattr(fixture, 'paper_p8_e4', None) else None)
        e5_cleanup_accepted = (cleanup_verified if prepare_only and prepare_experiment == 'E5' else None)
        run.update(status=(paper_p8_prepare_only_run_status(prepare_experiment, failed=bool(failure),
                           cleanup_verified=cleanup_verified, remote_outcome=remote_outcome) if prepare_only else
                           'WORKLOAD_COMPLETE_PENDING_CLEANUP' if fixture.paper_p7_mode == 'p6_ws'
                           and not failure and cleanup_verified else
                           'FAIL' if failure or not cleanup_verified or e1_cleanup_accepted is False
                             or e2_cleanup_accepted is False or e3_cleanup_accepted is False
                             or e4_cleanup_accepted is False or e5_cleanup_accepted is False else 'PASS'),
                   cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                   error=sanitize_failure_code(failure) if failure else None)
        if fixture.paper_p8_sessions:
            p8_path = fixture.artifacts / 'p8-sessions.json'
            p8 = json.loads(p8_path.read_text()) if p8_path.exists() else {}
            p8.update(cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                      status='PASS' if not failure and cleanup_verified and p8.get('status') == 'PASS' else 'FAIL')
            run['p8_sessions'] = p8
            durable_json(fixture.artifacts / 'p8-sessions.json', p8)
        if fixture.paper_p8_slow:
            p8_path = fixture.artifacts / 'p8-slow-viewer.json'
            p8 = json.loads(p8_path.read_text()) if p8_path.exists() else {}
            p8.update(cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                      status='PASS' if not failure and cleanup_verified and p8.get('status') == 'PASS' else 'FAIL')
            run['p8_slow_viewer'] = p8
            durable_json(p8_path, p8)
        if getattr(fixture, 'paper_p8_load', False):
            phase_id = remote_session.get('phase_id') if prepare_only else None
            is_e2_equal = bool(getattr(fixture, 'paper_p8_e2', False) and
                               getattr(fixture, 'paper_p8_e2_workload', 'steady_burst') == 'equal_samples')
            e2_target_name = (
                f'p8-e2-{phase_id}-remote-result.json' if is_e2_equal and prepare_only and remote_outcome == 'VALIDATED' and phase_id
                else 'p8-e2-equal-samples-pilot.json' if is_e2_equal and prepare_only
                else 'p8-e2-pilot.json'
            )
            p8_path = fixture.artifacts / ('p8-recovery.json' if getattr(fixture, 'paper_p8_recovery', False) else
                                            f'p8-e5-{fixture.paper_p8_e5}.json' if getattr(fixture, 'paper_p8_e5', None)
                                            else paper_p8_e4_artifact_name(fixture) if getattr(fixture, 'paper_p8_e4', None)
                                            else paper_p8_e3_artifact_name(fixture) if getattr(fixture, 'paper_p8_e3', None)
                                           else 'p8-e1-pilot.json' if getattr(fixture, 'paper_p8_e1', False)
                                           else e2_target_name if getattr(fixture, 'paper_p8_e2', False)
                                           else 'p8-e2-smoke.json')
            staged_recovery = fixture.private / 'p8-recovery-result.json'
            staged_e5 = fixture.private / 'p8-e5-result.json'
            if getattr(fixture, 'paper_p8_e5', None):
                p8 = paper_p8_e5_post_cleanup_artifact(
                    fixture, p8_path=p8_path, staged_e5=staged_e5,
                    remote_outcome=remote_outcome, failure=failure,
                    cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                )
                run['p8_e5'] = p8
            else:
                p8 = (json.loads(p8_path.read_text()) if getattr(fixture, 'paper_p8_e4_remote_warm', False) and p8_path.exists() else
                      json.loads(p8_path.read_text()) if prepare_only and prepare_experiment == 'E3' and p8_path.exists() else
                      json.loads(p8_path.read_text()) if is_e2_equal and prepare_only and p8_path.exists() else
                      {'status':'PASS' if remote_outcome == 'VALIDATED' and not failure and cleanup_verified else
                       'PREPARATION_ONLY' if remote_outcome == 'NOT_RUN' and not failure and cleanup_verified else 'FAIL',
                       'full_p8_accepted':False,'workload_status':remote_outcome,
                       'exact_cleanup_verified':cleanup_verified,
                       'cleanup_counts':cleanup_counts}
                      if prepare_only else
                      json.loads(staged_recovery.read_text()) if getattr(fixture, 'paper_p8_recovery', False) and staged_recovery.exists()
                      else json.loads(p8_path.read_text()) if p8_path.exists() else {})
                if getattr(fixture, 'paper_p8_recovery', False):
                    p8['workload_status'] = p8.get('status')
                    p8['full_p8_accepted'] = False
                    p8['exact_cleanup_verified'] = cleanup_verified
                    p8['cleanup_counts'] = cleanup_counts
                    phase_pass = bool(p8.get('phases')) and all(
                        isinstance(row, dict) and row.get('status') == 'PASS'
                        for row in p8['phases'].values()) and p8.get('all_children_stopped') is True
                    p8.update(cleanup_verified=cleanup_verified,
                        status='PASS' if not failure and cleanup_verified and phase_pass and
                        p8.get('workload_status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP' else 'FAIL')
                    durable_json(p8_path, p8)
                    run['p8_recovery'] = p8
                elif getattr(fixture, 'paper_p8_e4', None):
                    if not getattr(fixture, 'paper_p8_e4_remote_warm', False):
                        p8['workload_status'] = p8.get('status')
                        p8['full_p8_accepted'] = False
                        p8['exact_cleanup_verified'] = cleanup_verified
                        p8['cleanup_counts'] = cleanup_counts
                elif getattr(fixture, 'paper_p8_e3', None):
                    p8['workload_status'] = p8.get('status')
                    p8['full_p8_accepted'] = False
                    p8['lane_marker'] = 'PAPER_P8_E3_PILOT'
                elif fixture.paper_p8_e1 and not prepare_only:
                    p8['workload_status'] = p8.get('status')
                    p8['full_p8_accepted'] = False
                elif getattr(fixture, 'paper_p8_e2', False):
                    p8['workload_status'] = p8.get('status')
                    p8['full_p8_accepted'] = False
                if not getattr(fixture, 'paper_p8_recovery', False):
                    if getattr(fixture, 'paper_p8_e4_remote_warm', False):
                        p8.update(cleanup_verified=cleanup_verified, exact_cleanup_verified=cleanup_verified,
                                  cleanup_counts=cleanup_counts)
                        p8['status'] = 'PASS' if (not failure and
                            paper_p8_e4_warm_subset_acceptance(p8, cleanup_verified,
                                {'unique_count': getattr(fixture, 'paper_p8_e4_unique_count', 20),
                                 'replay_count': getattr(fixture, 'paper_p8_e4_replay_count', 10)})) else 'FAIL'
                    else:
                        p8.update(cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                            status='PASS' if (not failure and cleanup_verified and ((prepare_only and remote_outcome in ('VALIDATED', 'NOT_RUN')) or
                                (paper_p8_e4_cleanup_acceptance(fixture, evidence.get('paper_p8_e4', {}),
                                   cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e4', None) else
                                paper_p8_e3_cleanup_acceptance(fixture, evidence.get('paper_p8_e3', {}),
                                  cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e3', None) else
                                paper_p8_e1_cleanup_acceptance(fixture, evidence.get('paper_p8_e1', {}),
                                 cleanup_counts, cleanup_verified, registry) if fixture.paper_p8_e1 else
                               paper_p8_e2_cleanup_acceptance(fixture, evidence.get('paper_p8_e2', {}),
                                 cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e2', False)
                                   else p8.get('status') == 'PASS'))) else 'FAIL')
                    if getattr(fixture, 'paper_p8_e3_prepare_only', False):
                        p8.update(workload_status='NOT_RUN', full_p8_accepted=False,
                                  generator_colocated=True, physical_ab='NOT RUN')
                    durable_json(p8_path, p8)
                if getattr(fixture, 'paper_p8_recovery', False):
                    run['p8_recovery'] = p8
                elif getattr(fixture, 'paper_p8_e4', None):
                    run['p8_e4'] = p8
                elif getattr(fixture, 'paper_p8_e3', None):
                    run['p8_e3'] = p8
                elif fixture.paper_p8_e1:
                    run['p8_e1_preparation' if prepare_only else 'p8_e1'] = p8
                elif getattr(fixture, 'paper_p8_e2', False):
                    run['p8_e2'] = p8
                    if is_e2_equal:
                        img_dict = {name: val for name, val in images.items()}
                        bin_bytes = Path(paperload_binary).read_bytes() if paperload_binary and Path(paperload_binary).is_file() else None
                        bin_hash = hashlib.sha256(bin_bytes).hexdigest() if bin_bytes else p8.get('binary_sha256')
                        p8.update(
                            image_ids=img_dict,
                            images={name: {'id': val} for name, val in img_dict.items()},
                            binary_sha256=bin_hash,
                            paperload_binary_sha256=bin_hash,
                            binary_path='bin/paperload',
                            source_fingerprint=paper_p8_support()._source_fingerprint(ROOT),
                            cleanup_provenance={'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts},
                            exact_cleanup_verified=cleanup_verified,
                            cleanup_verified=cleanup_verified,
                        )
                    p8.update(cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                        status='PASS' if (not failure and cleanup_verified and ((prepare_only and remote_outcome in ('VALIDATED', 'NOT_RUN')) or
                            (paper_p8_e5_acceptance(p8, evidence.get('paper_p8_e5', {}), cleanup_verified) if getattr(fixture, 'paper_p8_e5', None) else
                            paper_p8_e4_cleanup_acceptance(fixture, evidence.get('paper_p8_e4', {}),
                               cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e4', None) else
                            paper_p8_e3_cleanup_acceptance(fixture, evidence.get('paper_p8_e3', {}),
                              cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e3', None) else
                            paper_p8_e1_cleanup_acceptance(fixture, evidence.get('paper_p8_e1', {}),
                             cleanup_counts, cleanup_verified, registry) if fixture.paper_p8_e1 else
                           paper_p8_e2_cleanup_acceptance(fixture, evidence.get('paper_p8_e2', {}),
                             cleanup_counts, cleanup_verified, registry) if getattr(fixture, 'paper_p8_e2', False)
                               else p8.get('status') == 'PASS'))) else 'FAIL')
                    durable_json(p8_path, p8)
                if getattr(fixture, 'paper_p8_e3_prepare_only', False):
                    p8.update(workload_status='NOT_RUN', full_p8_accepted=False,
                              generator_colocated=True, physical_ab='NOT RUN')
                durable_json(p8_path, p8)
            if getattr(fixture, 'paper_p8_recovery', False):
                run['p8_recovery'] = p8
            elif getattr(fixture, 'paper_p8_e5', None):
                run['p8_e5'] = p8
            elif getattr(fixture, 'paper_p8_e4', None):
                run['p8_e4'] = p8
            elif getattr(fixture, 'paper_p8_e3', None):
                run['p8_e3'] = p8
            elif fixture.paper_p8_e1:
                run['p8_e1_preparation' if prepare_only else 'p8_e1'] = p8
            elif getattr(fixture, 'paper_p8_e2', False):
                run['p8_e2'] = p8
        if fixture.paper_p4:
            run.update(effective_optimizations=(['mqtt_shaper']
                       if fixture.values['MQTT_SHAPER_ENABLED'] == 'true' else []))
        if fixture.paper_p5a:
            accepted = (not failure and cleanup_verified and
                        evidence.get('paper_p5a', {}).get('full_p5a_workload_accepted') is True and
                        (not fixture.paper_p5b or
                         evidence.get('paper_p5a', {}).get('duplicate_replay', {}).get('status') == 'PASS') and
                        paper_p5a_cleanup_acceptance(fixture, evidence['paper_p5a'],
                                                     cleanup_counts, cleanup_verified, registry))
            if fixture.paper_p5b:
                run.update(status='PAPER_P5B_PROFILE_WORKLOAD_ACCEPTED' if accepted else 'FAIL',
                           p5b_profile_workload_accepted=accepted,
                           p5b_comparison_accepted=False,
                           p5b_pending_phases=['four_profile_reconciliation'])
            else:
                run.update(status='PAPER_P5A_WORKLOAD_ACCEPTED' if accepted else 'FAIL')
            run.update(
                       native_executed=True, cleanup_verified=cleanup_verified,
                        exact_cleanup_verified=accepted, cleanup_counts=cleanup_counts,
                        error=sanitize_failure_code(failure) if failure else None)
            if fixture.paper_p5b_cache_smoke:
                run['p5b_cache_functional_accepted'] = accepted
        if fixture.paper_p6 and fixture.paper_p7_mode != 'p6_ws':
            if fixture.paper_p6_recovery_smoke:
                path = fixture.artifacts / 'p6-recovery.json'
                run = json.loads(path.read_text()) if path.exists() else {'native_executed': True}
                run['exact_cleanup_verified'] = cleanup_verified
                accepted = not failure and cleanup_verified and paper_p6_support().recovery_evidence_valid(run)
                run.update(status='PAPER_P6_RECOVERY_ACCEPTED' if accepted else 'FAIL',
                           cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                           error=sanitize_failure_code(failure) if failure else None)
                durable_json(path, run)
            else:
                checks = run.get('checks', {})
                accepted = (not failure and cleanup_verified and
                            checks.get('command_overlap_verified') is True and
                            checks.get('telemetry_accounting_exact') is True and
                            checks.get('schema_24_verified') is True and
                            checks.get('outboxes_drained') is True and
                            checks.get('commands_succeeded') is True and
                            checks.get('summaries_match_offered_and_acked') is True)
                run.update(status='PAPER_P6_WORKLOAD_ACCEPTED' if accepted else 'FAIL',
                           native_executed=True, exact_cleanup_verified=cleanup_verified,
                           cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                           acceptance={'status': 'ACCEPTED' if accepted else 'FAIL',
                                       'command_overlap_verified': checks.get('command_overlap_verified') is True,
                                       'telemetry_accounting_exact': checks.get('telemetry_accounting_exact') is True,
                                       'schema_24_verified': checks.get('schema_24_verified') is True,
                                       'exact_cleanup_verified': cleanup_verified},
                           error=sanitize_failure_code(failure) if failure else None)
        if fixture.paper_p3:
            accepted = (not failure and cleanup_verified and
                        run.get('full_p3_accepted') is True and
                        (not fixture.paper_p3_microbatch or
                         run.get('microbatch_control_regression_accepted') is True))
            run.update(status=(('PAPER_P5B_CONTROL_PRESERVATION_ACCEPTED'
                                if fixture.paper_p5b_control_regression else
                                'PAPER_P5A_CONTROL_REGRESSION_ACCEPTED') if fixture.paper_p3_microbatch
                               and accepted else 'PAPER_P3_ACCEPTED' if accepted else 'FAIL'),
                       native_executed=True, full_p3_accepted=run.get('full_p3_accepted') is True,
                       microbatch_enabled=fixture.values['MQTT_MICROBATCH_ENABLED'] == 'true',
                       ram_dedup_enabled=fixture.values['MQTT_RAM_DEDUP_ENABLED'] == 'true',
                       p5b_control_configuration_preservation_accepted=(
                           run.get('microbatch_control_regression_accepted') is True
                           if fixture.paper_p5b_control_regression else None),
                       microbatch_control_regression_accepted=(run.get('microbatch_control_regression_accepted')
                                                               if fixture.paper_p3_microbatch else None),
                       exact_cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                       error=sanitize_failure_code(failure) if failure else None)
        durable_json(path, run)
    if failure:
        if fixture.paper_p4 or fixture.paper_p5a or fixture.paper_p6:
            # Keep the diagnostic private and structural: exception text and
            # frame locals can contain MQTT credentials or telemetry payloads.
            frames = traceback.extract_tb(failure.__traceback__)
            name = ('p6-failure-stack.json' if fixture.paper_p6 else
                    'p5a-failure-stack.json' if fixture.paper_p5a else 'p4-failure-stack.json')
            durable_json(fixture.private / name, {
                "exception_type": type(failure).__name__,
                "frames": [{"file": Path(frame.filename).name,
                            "function": frame.name, "line": frame.lineno}
                           for frame in frames[-20:]],
            })
        fail_report = record_failure_report(fixture, phase=phase, exc=failure, cleanup_verified=cleanup_verified)
        raise FailureDiagnostic(fail_report, cause=failure) from failure

    try:
        if fixture.paper_p5a:
            if not paper_p5a_cleanup_acceptance(fixture, evidence.get('paper_p5a', {}),
                                                cleanup_counts, cleanup_verified, registry):
                raise ValueError('paper_p5a_acceptance_incomplete')
            report = {'schema_version': 1, 'status': 'PAPER_P5A_WORKLOAD_ACCEPTED',
                      'project': fixture.project, 'mode': fixture.paper_p5a_mode,
                      'dataset': fixture.paper_p5a_dataset, 'native_executed': True,
                      'full_p5a_accepted': False, 'workload_accepted': True,
                      'exact_cleanup_verified': True, 'cleanup_counts': cleanup_counts}
        elif fixture.paper_p4:
            if not paper_p4_cleanup_acceptance(fixture, evidence.get('paper_p4', {}),
                                               cleanup_counts, cleanup_verified, registry):
                raise ValueError('paper_p4_acceptance_incomplete')
            report = {'schema_version': 1, 'status': 'PAPER_P4_ACCEPTED', 'project': fixture.project,
                      'mode': fixture.paper_p4_mode, 'native_executed': True,
                      'full_p4_accepted': True, 'effective_optimizations':
                      ['mqtt_shaper'] if fixture.values['MQTT_SHAPER_ENABLED'] == 'true' else [],
                      'exact_cleanup_verified': True, 'cleanup_counts': cleanup_counts}
        elif fixture.paper_p6:
            if fixture.paper_p7_mode == 'p6_ws':
                p6ws = evidence.get('paper_p6_ws', {})
                run = json.loads((fixture.artifacts / 'run.json').read_text())
                if (not paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry)
                        or p6ws.get('mode') != 'paper_p7_p6_ws' or p6ws.get('full_p7_accepted') is not False
                        or run.get('status') != 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
                        or run.get('acceptance', {}).get('exact_cleanup_verified') is not False):
                    raise ValueError('paper_p7_p6_ws_acceptance_incomplete')
                report = {'schema_version': 1, 'status': 'PAPER_P7_P6_WS_REGRESSION_ACCEPTED',
                    'project': fixture.project, 'mode': 'p6_ws', 'native_executed': True,
                    'full_p7_accepted': False, 'exact_cleanup_verified': cleanup_verified,
                    'cleanup_counts': cleanup_counts, 'p6_reference': 'p6-reference.json',
                    'evidence': p6ws}
            elif fixture.paper_p6_recovery_smoke:
                run = json.loads((fixture.artifacts / 'p6-recovery.json').read_text())
                if run.get('status') != 'PAPER_P6_RECOVERY_ACCEPTED':
                    raise ValueError('paper_p6_recovery_acceptance_incomplete')
                report = {'schema_version': 1, 'status': 'PAPER_P6_RECOVERY_ACCEPTED',
                          'project': fixture.project, 'native_executed': True,
                          'exact_cleanup_verified': True, 'cleanup_counts': cleanup_counts,
                          'checks': run.get('checks')}
            else:
                run = json.loads((fixture.artifacts / 'run.json').read_text())
                if run.get('status') != 'PAPER_P6_WORKLOAD_ACCEPTED':
                    raise ValueError('paper_p6_acceptance_incomplete')
                report = {'schema_version': 1, 'status': 'PAPER_P6_WORKLOAD_ACCEPTED',
                          'project': fixture.project, 'native_executed': True,
                          'workload_accepted': True, 'exact_cleanup_verified': True,
                          'cleanup_counts': cleanup_counts, 'checks': run.get('checks')}
        elif fixture.paper_p3:
            report = paper_p3_acceptance_report(fixture.project, evidence['paper_p3'], cleanup_verified,
                fixture.paper_p3_microbatch, cleanup_counts, fixture.paper_p5b_control_regression)
            if report['status'] == 'FAIL':
                raise ValueError('cleanup_acceptance_incomplete')
        elif fixture.paper_p7:
            if not paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
                raise ValueError('cleanup_acceptance_incomplete')
            if fixture.paper_p7_mode == 'single_path':
                p7_pilot = evidence['paper_p7']
                report = {'schema_version': 1, 'status': 'PAPER_P7_SINGLE_PATH_ACCEPTED',
                          'project': fixture.project, 'native_executed': True,
                          'full_p7_accepted': False, 'single_path_accepted': True,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'evidence': p7_pilot.get('evidence', {})}
            elif fixture.paper_p7_mode == 'telemetry_pilot':
                p7_pilot = evidence['paper_p7']
                report = {'schema_version': 1, 'status': 'PAPER_P7_TELEMETRY_PILOT_ACCEPTED',
                          'project': fixture.project, 'native_executed': True,
                          'full_p7_accepted': False, 'telemetry_pilot_accepted': True,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'evidence': p7_pilot.get('evidence', {})}
            elif fixture.paper_p7_mode == 'reconnect':
                p7_rec = evidence['paper_p7']
                report = {'schema_version': 1, 'status': 'PAPER_P7_RECONNECT_ACCEPTED',
                          'project': fixture.project, 'native_executed': True,
                          'full_p7_accepted': False, 'reconnect_accepted': True,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'evidence': p7_rec.get('evidence', {})}
            elif fixture.paper_p7_mode == 'lifecycle':
                p7_lifecycle = evidence['paper_p7']
                from paper_p7_support import finalize_lifecycle_cleanup
                lifecycle_accepted = finalize_lifecycle_cleanup(p7_lifecycle, cleanup_verified)
                report = {'schema_version': 1,
                          'status': 'PAPER_P7_LIFECYCLE_ACCEPTED' if lifecycle_accepted else 'PAPER_P7_LIFECYCLE_PARTIAL',
                          'project': fixture.project, 'native_executed': True,
                          'full_p7_accepted': False, 'lifecycle_accepted': lifecycle_accepted,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'evidence': p7_lifecycle.get('evidence', {})}
            elif fixture.paper_p7_mode == 'slow_viewer':
                slow = evidence['paper_p7']
                report = {'schema_version': 1, 'status': 'PAPER_P7_SLOW_VIEWER_ACCEPTED',
                          'project': fixture.project, 'native_executed': True,
                          'full_p7_accepted': False, 'slow_viewer_accepted': True,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'evidence': slow.get('evidence', {}), 'counts': slow.get('counts', {}),
                          'application_ack_count': slow.get('application_ack_count'),
                          'healthy_peer_events': slow.get('healthy_peer_events'),
                           'capture_proof': slow.get('capture_proof'),
                           'close_evidence': slow.get('close_evidence'),
                           'precleanup_proof_artifact': slow.get('precleanup_proof_artifact'),
                           'capture_finalization_status': slow.get('capture_finalization_status'),
                           'probe_close_corroboration': slow.get('probe_close_corroboration'),
                           'manual_publisher_outbox': 'N/A'}
            elif fixture.paper_p7_mode in ('overhead_zero', 'overhead_two'):
                overhead = evidence['paper_p7']
                report = {'schema_version': 1,
                    'status': 'PAPER_P7_OVERHEAD_ACCEPTED', 'project': fixture.project,
                    'native_executed': True, 'full_p7_accepted': False,
                    'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                    'mode': overhead['mode'], 'run_json': 'run.json',
                    'metrics_csv': 'metrics.csv', 'resources_csv': 'resources.csv',
                    'evidence': overhead.get('evidence', {})}
            else:
                p7_handshake = evidence['paper_p7']['handshake']
                report = {'schema_version': 1, 'status': 'PAPER_P7_HANDSHAKE_ACCEPTED',
                      'project': fixture.project, 'native_executed': True,
                      'handshake_accepted': True, 'full_p7_accepted': False,
                      'exact_cleanup_verified': True, 'cleanup_counts': cleanup_counts,
                      'backend_image_id': images['backend'],
                      'tool_image_id': p7_handshake.get('tool_qualification', {}).get('tool_image_id', ''),
                      'revision': p7_handshake.get('source_qualification', {}).get('revision', ''),
                      'dirty_source_build_hash': p7_handshake.get('source_qualification', {}).get('dirty_source_build_hash', ''),
                      'effective_ws_flag': True,
                      'session_cap': 32,
                      'checks': {
                          'native': p7_handshake['native'],
                          'browser': p7_handshake['browser'],
                          'negative_count': len(p7_handshake['negative']),
                          'leak_check': p7_handshake['leak_check'],
                          'perimeter_isolation': p7_handshake.get('perimeter_isolation', {}),
                      }}
        elif fixture.paper_p8_sessions:
            p8 = json.loads((fixture.artifacts / 'p8-sessions.json').read_text())
            report = {'schema_version': 1,
                      'status': 'PAPER_P8_3A2_SESSION_LANE_PASS'
                      if p8.get('status') == 'PASS' and p8.get('cleanup_verified') is True else 'FAIL',
                      'project': fixture.project, 'native_executed': True,
                      'p8_3a2_lane_pass': p8.get('status') == 'PASS',
                      'exact_cleanup_verified': cleanup_verified,
                      'cleanup_counts': cleanup_counts, 'artifact': 'p8-sessions.json'}
            if report['status'] == 'FAIL':
                raise ValueError('paper_p8_lane_acceptance_incomplete')
        elif fixture.paper_p8_slow:
            p8 = json.loads((fixture.artifacts / 'p8-slow-viewer.json').read_text())
            report = {'schema_version': 1,
                      'status': 'PAPER_P8_3A3_SLOW_VIEWER_PROBE' if p8.get('status') == 'PASS' else 'FAIL',
                      'project': fixture.project, 'native_executed': True,
                      'p8_3a3_probe_pass': p8.get('status') == 'PASS',
                      'full_p8_accepted': False, 'exact_cleanup_verified': cleanup_verified,
                      'cleanup_counts': cleanup_counts, 'artifact': 'p8-slow-viewer.json'}
            if report['status'] == 'FAIL':
                raise ValueError('paper_p8_slow_viewer_probe_incomplete')
        elif getattr(fixture, 'paper_p8_load', False):
            if getattr(fixture, 'paper_p8_e4_remote_warm', False):
                name = paper_p8_e4_artifact_name(fixture)
                p8 = json.loads((fixture.artifacts / name).read_text())
                p8.update(cleanup_verified=cleanup_verified,
                          exact_cleanup_verified=cleanup_verified,
                          cleanup_counts=cleanup_counts)
                accepted = paper_p8_e4_warm_subset_acceptance(p8, cleanup_verified,
                    {'unique_count': getattr(fixture, 'paper_p8_e4_unique_count', 20),
                     'replay_count': getattr(fixture, 'paper_p8_e4_replay_count', 10)})
                p8['status'] = 'PASS' if accepted else p8.get('status', 'FAIL')
                durable_json(fixture.artifacts / name, p8)
                report = paper_p8_e4_startup_summary(fixture, p8, accepted,
                    cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts,
                    artifact_name=name)
                if not accepted:
                    raise ValueError('paper_p8_e4_warm_subset_incomplete')
            elif getattr(fixture, 'paper_p8_prepare_only', False):
                report = paper_p8_prepare_only_report(fixture, remote_outcome=remote_outcome,
                    cleanup_verified=cleanup_verified, cleanup_counts=cleanup_counts)
            elif getattr(fixture, 'paper_p8_recovery', False):
                name = 'p8-recovery.json'
                p8 = json.loads((fixture.artifacts / name).read_text())
                accepted = (p8.get('status') == 'PASS' and p8.get('lane_marker') == 'PAPER_P8_RECOVERY_PILOT'
                    and p8.get('full_p8_accepted') is False and p8.get('cleanup_verified') is True
                    and cleanup_verified is True)
                report = {'schema_version': 1,
                    'status': 'PAPER_P8_RECOVERY_PILOT' if accepted else 'FAIL',
                    'lane': 'PAPER_P8_RECOVERY_PILOT', 'project': fixture.project,
                    'native_executed': True, 'recovery_pilot_pass': accepted,
                    'full_p8_accepted': False, 'exact_cleanup_verified': cleanup_verified,
                    'cleanup_counts': cleanup_counts, 'artifact': name}
                if not accepted:
                    raise ValueError('paper_p8_recovery_incomplete')
            elif getattr(fixture, 'paper_p8_e5', None):
                name = f'p8-e5-{fixture.paper_p8_e5}.json'
                p8 = json.loads((fixture.artifacts / name).read_text())
                accepted = (p8.get('status') == 'PASS' and p8.get('lane_marker') == 'PAPER_P8_E5_ARM_PILOT'
                            and p8.get('full_p8_accepted') is False and p8.get('cleanup_verified') is True
                            and cleanup_verified is True and paper_p8_e5_acceptance(p8, evidence.get('paper_p8_e5', {}), cleanup_verified))
                report = {'schema_version': 1, 'status': 'PAPER_P8_E5_ARM_PILOT' if accepted else 'FAIL',
                          'lane': 'PAPER_P8_E5_ARM_PILOT', 'arm': fixture.paper_p8_e5,
                          'project': fixture.project, 'native_executed': True, 'p8_e5_arm_pilot_pass': accepted,
                          'full_p8_accepted': False, 'exact_cleanup_verified': cleanup_verified,
                          'cleanup_counts': cleanup_counts, 'artifact': name}
                if not accepted:
                    raise ValueError('paper_p8_e5_arm_pilot_incomplete')
            elif getattr(fixture, 'paper_p8_e4', None):
                name = paper_p8_e4_artifact_name(fixture)
                p8 = json.loads((fixture.artifacts / name).read_text())
                cold_status = p8.get('coldrestart')
                cold_ok = cold_status == ('PASS' if fixture.paper_p8_e4_cache_mode == 'on' else 'NOT APPLICABLE')
                accepted = (p8.get('status') == 'PASS'
                            and p8.get('workload_status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
                            and p8.get('lane_marker') == 'PAPER_P8_E4_PILOT_COMPLETE'
                            and p8.get('full_p8_accepted') is False and cold_ok
                            and isinstance(p8.get('new_ws_event_proof'), dict)
                            and p8['new_ws_event_proof'].get('status') == 'PASS'
                            and isinstance(p8.get('conflicts'), dict) and p8['conflicts'].get('status') == 'PASS'
                            and isinstance(p8.get('durable_gateway_reopen'), dict)
                            and p8['durable_gateway_reopen'].get('status') == 'PASS'
                            and p8.get('cleanup_verified') is True and cleanup_verified is True
                            and paper_p8_e4_cleanup_acceptance(fixture, evidence.get('paper_p8_e4', {}),
                                                               cleanup_counts, cleanup_verified, registry))
                report = {'schema_version': 1,
                          'status': 'PAPER_P8_E4_PILOT' if accepted else 'FAIL',
                          'lane': 'PAPER_P8_E4_PILOT', 'cache_mode': fixture.paper_p8_e4,
                          'project': fixture.project, 'native_executed': True,
                          'p8_e4_pilot_pass': accepted, 'full_p8_accepted': False,
                          'coldrestart': p8.get('coldrestart'),
                          'conflicts': (p8['conflicts'].get('status') if isinstance(p8.get('conflicts'), dict)
                                        else p8.get('conflicts')),
                          'durable_gateway_reopen': (p8['durable_gateway_reopen'].get('status')
                              if isinstance(p8.get('durable_gateway_reopen'), dict)
                              else p8.get('durable_gateway_reopen')),
                          'new_ws_event_proof': (p8['new_ws_event_proof'].get('status')
                              if isinstance(p8.get('new_ws_event_proof'), dict)
                              else p8.get('new_ws_event_proof')),
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'artifact': name}
                if not accepted:
                    raise ValueError('paper_p8_e4_warm_pilot_incomplete')
            elif getattr(fixture, 'paper_p8_e3', None):
                low_load = getattr(fixture, 'paper_p8_e3_workload', 'mixed') == 'low_load'
                name = paper_p8_e3_artifact_name(fixture)
                p8 = json.loads((fixture.artifacts / name).read_text())
                if prepare_only:
                    accepted = (p8.get('status') == 'PASS' and p8.get('workload_status') == 'NOT_RUN'
                                and p8.get('full_p8_accepted') is False and cleanup_verified is True)
                    report = {'schema_version': 1,
                              'status': 'PAPER_P8_E3_PREPARATION_ONLY' if accepted else 'FAIL',
                              'experiment': 'E3', 'arm': fixture.paper_p8_e3,
                              'workload': getattr(fixture, 'paper_p8_e3_workload', 'mixed'),
                              'run_id': p8.get('run_id'), 'phase_id': p8.get('phase_id'),
                              'project': fixture.project, 'native_executed': True,
                              'workload_status': 'NOT_RUN', 'full_p8_accepted': False,
                              'generator_colocated': True, 'physical_ab': 'NOT RUN',
                              'exact_cleanup_verified': cleanup_verified,
                              'cleanup_counts': cleanup_counts, 'artifact': name}
                    if not accepted:
                        raise ValueError('paper_p8_e3_preparation_cleanup_failed')
                else:
                    accepted = (p8.get('status') == 'PASS' and p8.get('full_p8_accepted') is False
                                and p8.get('cleanup_verified') is True and cleanup_verified is True
                                and paper_p8_e3_cleanup_acceptance(fixture, evidence.get('paper_p8_e3', {}),
                                                                   cleanup_counts, cleanup_verified, registry))
                    report = {'schema_version': 1,
                              'status': 'PAPER_P8_E3_PILOT' if accepted else 'FAIL',
                              'lane': 'PAPER_P8_E3_PILOT', 'arm': fixture.paper_p8_e3,
                              'workload': getattr(fixture, 'paper_p8_e3_workload', 'mixed'),
                              'project': fixture.project, 'native_executed': True,
                              'p8_e3_pilot_pass': accepted, 'full_p8_accepted': False,
                              'representative_probes_pending': p8.get('representative_probes_pending'),
                              'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                              'artifact': name}
                    if not accepted:
                        raise ValueError('paper_p8_e3_pilot_incomplete')
            elif getattr(fixture, 'paper_p8_e1', False):
                p8 = json.loads((fixture.artifacts / 'p8-e1-pilot.json').read_text())
                accepted = paper_p8_e1_report_acceptance(p8, cleanup_verified, e1_cleanup_accepted)
                report = {'schema_version': 1, 'status': 'PAPER_P8_E1_PILOT' if accepted else 'FAIL',
                          'project': fixture.project, 'native_executed': True,
                          'p8_e1_pilot_pass': accepted, 'full_p8_accepted': False,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'artifact': 'p8-e1-pilot.json'}
                if not accepted:
                    raise ValueError('paper_p8_e1_pilot_incomplete')
            elif getattr(fixture, 'paper_p8_e2', False):
                p8 = json.loads((fixture.artifacts / 'p8-e2-pilot.json').read_text())
                accepted = paper_p8_e2_report_acceptance(p8, cleanup_verified)
                report = {'schema_version': 1, 'status': 'PAPER_P8_E2_PILOT' if accepted else 'FAIL',
                          'project': fixture.project, 'native_executed': True,
                          'p8_e2_pilot_pass': accepted, 'full_p8_accepted': False,
                          'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
                          'artifact': 'p8-e2-pilot.json'}
                if not accepted:
                    raise ValueError('paper_p8_e2_pilot_incomplete')
            else:
                p8 = json.loads((fixture.artifacts / 'p8-e2-smoke.json').read_text())
                report = {'schema_version': 1,
                          'status': 'PAPER_P8_4F_E2_TELEMETRY100_SMOKE' if p8.get('status') == 'PASS' else 'FAIL',
                          'project': fixture.project, 'native_executed': True,
                          'p8_4f_e2_telemetry100_smoke_pass': p8.get('status') == 'PASS',
                          'full_p8_accepted': False, 'exact_cleanup_verified': cleanup_verified,
                          'cleanup_counts': cleanup_counts, 'artifact': 'p8-e2-smoke.json'}
                if report['status'] == 'FAIL':
                    raise ValueError('paper_p8_e2_smoke_incomplete')
        elif fixture.paper_p2:
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
    *SERVICES, 'supabase-storage', 'auth-socket-init', 'paper_p0', 'paper_p1', 'paper_p2_spike', 'paper_p2', 'paper_p3', 'paper_p4', 'paper_p5a',
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
    'paper_p3_mode_rejected', 'paper_p3_acceptance_failed', 'paper_p3_microbatch_mode_rejected',
    'paper_p3_microbatch_profile_rejected',
    'paper_p4_mode_rejected', 'paper_p4_profile_rejected', 'paper_p4_fixture_required',
    'paper_p4_metrics_unavailable', 'paper_p4_stats_failed', 'paper_p4_acceptance_failed',
    'paper_p4_broker_profile_rejected',
    'paper_p4_gateway_summary_rejected', 'paper_p4_outbox_not_drained',
    'paper_p4_snapshot_not_stopped', 'paper_p4_pending_snapshot_failed',
    'paper_p4_duplicate_replay_failed', 'paper_p4_metrics_window_rejected',
    'paper_p4_observer_capture_failed',
    'paper_p4_deadline_exceeded', 'paper_p4_accounting_rejected', 'paper_p4_artifact_rejected',
    'paper_p4_namespace_payload_unavailable', 'paper_p4_acceptance_incomplete',
    'paper_p5a_mode_rejected', 'paper_p5a_dataset_rejected', 'paper_p5a_profile_rejected',
    'paper_p5a_quota_rejected', 'paper_p5a_fixture_required', 'paper_p5a_acceptance_failed',
    'paper_p5a_acceptance_incomplete', 'paper_p5a_accounting_rejected',
    'paper_p5a_artifact_rejected', 'paper_p5a_metrics_unavailable',
    'paper_p5a_gateway_summary_rejected', 'paper_p5a_observer_capture_failed',
    'paper_p5a_duplicate_replay_failed', 'paper_p5a_read_regression_failed',
    'paper_p5a_shutdown_profile_rejected', 'paper_p5a_shutdown_publish_failed',
    'paper_p5a_shutdown_partial_group_unproven', 'paper_p5a_shutdown_signal_failed',
    'paper_p5a_shutdown_deadline_failed', 'paper_p5a_shutdown_persistence_failed',
    'paper_p5a_shutdown_bytes_not_drained',
    'paper_p5a_deadline_exceeded',
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
    "paper_p5b_conflict_sensor_provision_failed",
    "paper_p5b_cold_replay_reconciliation_failed", "paper_p5b_functional_smoke_failed",
    "p8_prepare_report_artifact_missing", "p8_prepare_report_validation_missing",
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


def db_psql(registry, postgres_cid: str, sql_commands: str, variables=None, *, timeout=None) -> str:
    args = ["exec", "-i", postgres_cid, "psql", "-U", "postgres", "-d", "smoke", "-v", "ON_ERROR_STOP=1", "-At"]
    for key, value in (variables or {}).items():
        if (not re.fullmatch(r"[a-z_]+", key) or not isinstance(value, str)
                or len(value) > 256 or any(ord(ch) < 32 or ord(ch) == 127 for ch in value)):
            raise ValueError("db_variable_invalid")
        args += ["-v", key + "=" + value]
    if variables:
        result = registry.command(args + ["-f", "-"], input=sql_commands.encode(), **({'timeout': timeout} if timeout is not None else {}))
    else:
        result = registry.command(args + ["-c", sql_commands], **({'timeout': timeout} if timeout is not None else {}))
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


def p8_owned_storage_guard(fixture, registry, postgres_item, *, growth_limit_bytes,
                           minimum_free_bytes=64 << 20, db_size_reader=None,
                           interval_seconds=1.0):
    """Return a finite A-local guard scoped to this fixture and its owned DB."""
    try:
        from scripts.paper_p8_resources import OwnedStorageGuard
    except ModuleNotFoundError:
        sys.path.insert(0, str(ROOT / 'scripts'))
        from paper_p8_resources import OwnedStorageGuard

    owned_paths = [fixture.private, fixture.artifacts]
    guard = OwnedStorageGuard(owned_paths, growth_threshold_bytes=growth_limit_bytes,
        minimum_free_bytes=minimum_free_bytes, interval_seconds=interval_seconds,
        allowed_root=fixture.base, max_samples=128)
    db_baseline = None
    samples = []
    last_sample_mono = None
    last_result = None

    def check():
        nonlocal db_baseline, last_sample_mono, last_result
        if last_sample_mono is not None and time.monotonic() - last_sample_mono < interval_seconds:
            return last_result
        container = registry.resolve(postgres_item)
        if container is None or not postgres_item.get('receipt'):
            raise ValueError('owned_postgres_identity_unavailable')
        cid = postgres_item['receipt']['id']
        if db_size_reader is None:
            db_size = int(db_psql(registry, cid,
                "SELECT pg_database_size('smoke')::bigint").strip())
        else:
            db_size = int(db_size_reader(registry, cid))
        if db_size < 0:
            raise ValueError('owned_postgres_size_invalid')
        current = guard._measure()
        if not current['available']:
            raise ValueError('owned_fixture_storage_unavailable')
        if db_baseline is None:
            db_baseline = db_size
        file_base = sum(guard.baseline['sizes_bytes'].values())
        file_now = sum(current['sizes_bytes'].values())
        growth = max(0, file_now - file_base) + max(0, db_size - db_baseline)
        reason = ('owned_storage_growth_threshold' if growth >= growth_limit_bytes else
                  'owned_storage_minimum_free_space'
                  if current['free_bytes'] <= minimum_free_bytes else None)
        sample = {'at_unix_ns': time.time_ns(), 'scope': 'fixture_private_artifacts_and_owned_smoke_database',
            'file_sizes_bytes': {'fixture_private': current['sizes_bytes'][str(fixture.private)],
                                 'fixture_artifacts': current['sizes_bytes'][str(fixture.artifacts)]},
            'database_bytes': db_size,
            'database_growth_bytes': max(0, db_size - db_baseline),
            'free_bytes': current['free_bytes'], 'growth_bytes': growth, 'stop_reason': reason}
        samples.append(sample)
        if len(samples) > 128:
            del samples[:-128]
        report = {'status': 'threshold' if reason else 'sampling',
            'scope': sample['scope'], 'growth_limit_bytes': growth_limit_bytes,
            'minimum_free_bytes': minimum_free_bytes, 'interval_seconds': interval_seconds,
            'baseline': {'file_sizes_bytes': {'fixture_private': guard.baseline['sizes_bytes'][str(fixture.private)],
                'fixture_artifacts': guard.baseline['sizes_bytes'][str(fixture.artifacts)]},
                'database_bytes': db_baseline}, 'terminal': sample,
            'samples': samples[-128:], 'remote_b_action': 'operator_stops_B_locally'}
        durable_json(fixture.private / 'owned-storage-guard.json', report)
        last_sample_mono = time.monotonic()
        last_result = {'ok': reason is None, 'reason': reason, 'report': report}
        return last_result
    return check


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
    'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited', 'ShaperStateCapacity',
    'Processed', 'ValidationRejected', 'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
    'ACKBuildFailures', 'Committed', 'DuplicateCommitted', 'SamplesNew', 'ACKPublished', 'ACKPublishFailures')
P1_GAUGES = ('QueueDepth', 'QueueCapacity', 'QueueHighWater', 'InFlight',
    'BufferedPayloadBytes', 'BufferedPayloadCapacityBytes', 'BufferedPayloadHighWaterBytes',
    'ShaperEnabled', 'ShaperEntries', 'ShaperCapacity')
P4_INGRESS_FIELDS = frozenset(('ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited', 'ShaperStateCapacity',
    'BufferedPayloadBytes', 'BufferedPayloadCapacityBytes', 'BufferedPayloadHighWaterBytes',
    'ShaperEnabled', 'ShaperEntries', 'ShaperCapacity'))
P5A_PERSISTENCE_FIELDS = {
    'single_attempts': ('counter', 'operations'), 'group_attempts': ('counter', 'operations'),
    'group_members': ('counter', 'messages'), 'group_rows': ('counter', 'samples'),
    'group_payload_bytes': ('counter', 'bytes'), 'fallback_attempts': ('counter', 'operations'),
    'begin_attempts': ('counter', 'operations'), 'begin_succeeded': ('counter', 'operations'),
    'confirmed_commits': ('counter', 'operations'), 'rollback_attempts': ('counter', 'operations'),
    'confirmed_rollbacks': ('counter', 'operations'), 'commit_unconfirmed': ('counter', 'operations'),
    'copy_calls': ('counter', 'operations'), 'copy_returned_rows': ('counter', 'rows'),
    'confirmed_new_rows': ('counter', 'rows'),
}
P5A_MICROBATCH_FIELDS = {
    'enabled': ('gauge', 'boolean'), 'max_messages': ('gauge', 'messages'),
    'max_rows': ('gauge', 'samples'), 'max_payload_bytes': ('gauge', 'bytes'),
    'max_wait_ns': ('gauge', 'ns'), 'groups': ('counter', 'operations'),
    'members': ('counter', 'messages'), 'rows': ('counter', 'samples'),
    'payload_bytes': ('counter', 'bytes'), 'max_members': ('gauge', 'messages'),
    'flush_messages': ('counter', 'operations'), 'flush_rows': ('counter', 'operations'),
    'flush_bytes': ('counter', 'operations'), 'flush_timer': ('counter', 'operations'),
    'flush_stop': ('counter', 'operations'), 'standalone_oversized_members': ('counter', 'messages'),
}
P5A_MICROBATCH_CONFIG_FIELDS = frozenset((
    'enabled', 'max_messages', 'max_rows', 'max_payload_bytes', 'max_wait_ns'))
RAM_DEDUP_COUNTER_FIELDS = (
    'lookup_hit_total', 'lookup_miss_total', 'lookup_expired_total',
    'lookup_metadata_mismatch_total', 'lookup_disabled_bypass_total',
    'publication_stored_total', 'publication_existing_total',
    'publication_metadata_mismatch_total', 'publication_full_total',
    'publication_invalid_skip_total', 'publication_disabled_bypass_total',
    'capacity_bypass_total', 'rotations_total', 'database_calls_avoided_total',
)
RAM_DEDUP_GAUGE_FIELDS = ('entries', 'logical_bytes', 'current_generation_entries',
                          'previous_generation_entries')
RAM_DEDUP_CONFIG_FIELDS = ('enabled', 'ttl_seconds', 'max_entries', 'max_bytes')
RAM_DEDUP_FIELD_UNITS = {
    **{name: ('counter', 'operations') for name in RAM_DEDUP_COUNTER_FIELDS},
    'entries': ('gauge', 'entries'), 'logical_bytes': ('gauge', 'bytes'),
    'current_generation_entries': ('gauge', 'entries'),
    'previous_generation_entries': ('gauge', 'entries'),
    'enabled': ('gauge', 'boolean'), 'ttl_seconds': ('gauge', 'seconds'),
    'max_entries': ('gauge', 'entries'), 'max_bytes': ('gauge', 'bytes'),
}
P1_RATES = {'received_delivery_msg_per_s': 'Received', 'new_committed_msg_per_s': 'Committed',
            'new_committed_samples_per_s': 'SamplesNew'}

CONTROL_EXPORT_FIELDS = {
    'command_admission': {
        **{name: ('counter', 'count') for name in ('accepted', 'replay', 'noop', 'rejected')},
        **{name: ('histogram', 'seconds') for name in ('admission',)},
    },
    'command_dispatch': {
        **{name: ('counter', 'count') for name in (
            'puback', 'publish_failures', 'notification_signals_accepted',
            'notification_signals_coalesced', 'scans', 'scan_errors', 'enqueued', 'queue_full',
            'published_mark_successful_calls', 'published_mark_errors')},
        **{name: ('gauge', 'count') for name in (
            'queue_depth', 'queue_capacity', 'live_publish_workers', 'in_flight')},
        **{name: ('histogram', 'seconds') for name in (
            'discovery', 'queue_wait', 'publish', 'prepare_db_recheck', 'published_mark')},
    },
    'command_result': {
        **{name: ('counter', 'count') for name in (
            'accepted', 'duplicate', 'late', 'rejected', 'db_error', 'queue_full')},
        **{name: ('histogram', 'seconds') for name in (
            'queue_wait', 'processing', 'received_to_confirmed_commit')},
    },
    'command_timeline': {
        **{name: ('counter', 'count') for name in (
            'missing_origin', 'missing_dispatch', 'missing_result', 'missing_commit_to_discovery',
            'missing_commit_to_enqueue', 'overflow')},
        **{name: ('gauge', 'count') for name in ('entries', 'capacity', 'pending_origins')},
        'ttl_seconds': ('gauge', 'seconds'),
        **{name: ('histogram', 'seconds') for name in (
            'd_dispatch', 'd_e2e_succeeded', 'd_e2e_failed',
            'commit_to_first_discovery', 'commit_to_first_enqueue')},
    },
}
CONTROL_AVAILABILITY = {
    'command_admission': 'command', 'command_dispatch': 'dispatch_e2e',
    'command_result': 'result', 'command_timeline': 'dispatch_e2e',
}


def paper_p1_histogram_window(before, after, group, name):
    """Return same-process, non-cumulative histogram deltas and bucket-bound percentiles."""
    result = {'availability': 'unavailable', 'count': None, 'overflow': None, 'buckets': None,
              'upper_seconds': None, 'percentile_method': None, 'p50_seconds': None,
              'p95_seconds': None, 'p99_seconds': None}
    if not isinstance(after, dict):
        return result
    avail_key = CONTROL_AVAILABILITY.get(group)
    availability = (after.get('availability') or {}).get(avail_key) if avail_key else None
    before_availability = ((before.get('availability') or {}).get(avail_key)
                           if isinstance(before, dict) and avail_key else None)
    if availability is None and isinstance(after.get(group), dict):
        availability = 'available'
    if before_availability is None and isinstance(before, dict) and isinstance(before.get(group), dict):
        before_availability = 'available'
    if availability == 'disabled' or before_availability == 'disabled':
        result['availability'] = 'disabled'
        return result
    if availability == 'not_implemented' or before_availability == 'not_implemented':
        result['availability'] = 'not_implemented'
        return result
    if availability != 'available' or before_availability != 'available' or not isinstance(before, dict):
        return result
    marker = after.get('process_instance')
    if not isinstance(marker, str) or not marker or before.get('process_instance') != marker:
        return result
    old = (before.get(group) or {}).get(name)
    new = (after.get(group) or {}).get(name)
    def valid(hist):
        if not isinstance(hist, dict):
            return False
        count, overflow = hist.get('count'), hist.get('overflow')
        bounds, buckets = hist.get('upper_seconds'), hist.get('buckets')
        return (type(count) is int and count >= 0 and type(overflow) is int and 0 <= overflow <= count
                and isinstance(bounds, list) and isinstance(buckets, list) and len(bounds) == len(buckets)
                and all(type(x) in (int, float) and math.isfinite(x) and x > 0 for x in bounds)
                and bounds == sorted(set(bounds))
                and all(type(x) is int and x >= 0 for x in buckets)
                and sum(buckets) + overflow == count)
    if not valid(old) or not valid(new) or old['upper_seconds'] != new['upper_seconds']:
        return result
    delta_buckets = [n - o for o, n in zip(old['buckets'], new['buckets'])]
    delta_count, delta_overflow = new['count'] - old['count'], new['overflow'] - old['overflow']
    if delta_count < 0 or delta_overflow < 0 or any(n < 0 for n in delta_buckets):
        return result
    if sum(delta_buckets) + delta_overflow != delta_count:
        return result
    result.update(availability='available', count=delta_count, overflow=delta_overflow,
                  buckets=delta_buckets, upper_seconds=list(new['upper_seconds']),
                  percentile_method='upper_bucket_approximation')
    in_range = sum(delta_buckets)
    for percentile in (50, 95, 99):
        rank = max(1, math.ceil(delta_count * percentile / 100)) if delta_count else None
        value = None
        if rank is not None:
            if rank > in_range:
                value = 'infinity'
            else:
                cumulative = 0
                for upper, bucket_count in zip(new['upper_seconds'], delta_buckets):
                    cumulative += bucket_count
                    if cumulative >= rank:
                        value = upper
                        break
        result[f'p{percentile}_seconds'] = value
    return result


def paper_p1_rates(before, after):
    rates = dict.fromkeys(P1_RATES)
    if not before or not after or before['process_instance'] != after['process_instance']:
        return rates
    elapsed = after['elapsed_seconds'] - before['elapsed_seconds']
    if (type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0
            or not before.get('ingress') or not after.get('ingress')):
        return rates
    if any(field not in before['ingress'] or field not in after['ingress']
           or type(before['ingress'][field]) not in (int, float)
           or type(after['ingress'][field]) not in (int, float)
           for field in P1_RATES.values()):
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
                if value is not None and type(value) not in (int, float, bool):
                    raise ValueError('paper_p1_snapshot_rejected')
                if type(value) in (int, float) and not math.isfinite(value):
                    raise ValueError('paper_p1_snapshot_rejected')
                writer.writerow(dict(base, group=group, metric=metric, kind=kind, unit=unit,
                                     value=value, availability='available', **extra))
            def unavailable(group, metric, kind, unit):
                writer.writerow(dict(base, group=group, metric=metric, kind=kind, unit=unit,
                                     availability='unavailable'))
            ing = snap.get('ingress') or {}
            for name in P1_COUNTERS + P1_GAUGES:
                if name not in ing:
                    if name in P4_INGRESS_FIELDS:
                        writer.writerow(dict(base, group='ingress', metric=name,
                                             kind='counter' if name in P1_COUNTERS else 'gauge',
                                             unit='bytes' if name.endswith('Bytes') else
                                             'boolean' if name == 'ShaperEnabled' else 'count',
                                             availability='unavailable'))
                    continue
                if ing[name] is None and name in P4_INGRESS_FIELDS:
                    writer.writerow(dict(base, group='ingress', metric=name,
                                         kind='counter' if name in P1_COUNTERS else 'gauge',
                                         unit='bytes' if name.endswith('Bytes') else
                                         'boolean' if name == 'ShaperEnabled' else 'count',
                                         availability='unavailable'))
                    continue
                delta = ing[name] - previous['ingress'][name] if valid_delta and name in previous['ingress'] and name in P1_COUNTERS else None
                row('ingress', name, 'counter' if name in P1_COUNTERS else 'gauge',
                    'bytes' if name.endswith('Bytes') else 'boolean' if name == 'ShaperEnabled' else 'count', ing[name], delta=delta,
                    rate_per_s=delta / (snap['elapsed_seconds'] - previous['elapsed_seconds']) if delta is not None and delta >= 0 else None)
            persistence = snap.get('persistence')
            previous_persistence = previous.get('persistence') if previous else None
            persistence_delta_valid = (previous is not None
                and snap.get('process_instance') == previous.get('process_instance')
                and type(snap.get('elapsed_seconds')) in (int, float)
                and type(previous.get('elapsed_seconds')) in (int, float)
                and snap['elapsed_seconds'] > previous['elapsed_seconds'])
            for name, (kind, unit) in P5A_PERSISTENCE_FIELDS.items():
                if not isinstance(persistence, dict) or type(persistence.get(name)) not in (int, float):
                    unavailable('persistence', name, kind, unit)
                    continue
                value = persistence[name]
                delta = None
                if persistence_delta_valid and isinstance(previous_persistence, dict):
                    old = previous_persistence.get(name)
                    if type(old) in (int, float) and value >= old:
                        delta = value - old
                row('persistence', name, kind, unit, value, delta=delta)

            microbatch = ing.get('microbatch')
            microbatch_availability = snap.get('availability', {}).get('group_persistence')
            if microbatch_availability not in ('available', 'disabled'):
                microbatch_availability = 'available' if isinstance(microbatch, dict) else 'unavailable'
            previous_microbatch = ((previous.get('ingress') or {}).get('microbatch')
                                   if previous and isinstance(previous.get('ingress'), dict) else None)
            microbatch_delta_valid = (persistence_delta_valid and previous is not None
                                      and snap.get('process_instance') == previous.get('process_instance'))
            for name, (kind, unit) in P5A_MICROBATCH_FIELDS.items():
                value = microbatch.get(name) if isinstance(microbatch, dict) else None
                config_value_available = (microbatch_availability == 'disabled'
                                          and name in P5A_MICROBATCH_CONFIG_FIELDS)
                if ((microbatch_availability != 'available' and not config_value_available)
                        or type(value) not in (int, float, bool)):
                    status = microbatch_availability if microbatch_availability == 'disabled' else 'unavailable'
                    writer.writerow(dict(base, group='ingress_microbatch', metric=name, kind=kind, unit=unit,
                                         availability=status))
                    continue
                delta = None
                if kind == 'counter' and microbatch_delta_valid and isinstance(previous_microbatch, dict):
                    old = previous_microbatch.get(name)
                    if type(old) in (int, float) and value >= old:
                        delta = value - old
                row('ingress_microbatch', name, kind, unit, value, delta=delta)
            cache = snap.get('ram_dedup')
            cache_availability = snap.get('availability', {}).get('ram_dedup')
            if cache_availability not in ('available', 'disabled', 'unavailable'):
                cache_availability = 'available' if isinstance(cache, dict) else 'unavailable'
            previous_cache = previous.get('ram_dedup') if previous else None
            cache_delta_valid = (persistence_delta_valid and previous is not None
                                 and snap.get('process_instance') == previous.get('process_instance'))
            for name, (kind, unit) in RAM_DEDUP_FIELD_UNITS.items():
                value = cache.get(name) if isinstance(cache, dict) else None
                valid_type = type(value) is bool if name == 'enabled' else type(value) is int
                config_available = (cache_availability == 'disabled'
                                    and name in RAM_DEDUP_CONFIG_FIELDS
                                    and valid_type)
                if ((cache_availability != 'available' and not config_available)
                        or not valid_type):
                    availability = ('disabled' if cache_availability == 'disabled'
                                    and name not in RAM_DEDUP_CONFIG_FIELDS else 'unavailable')
                    writer.writerow(dict(base, group='ram_dedup', metric=name, kind=kind, unit=unit,
                                         availability=availability))
                    continue
                delta = None
                if kind == 'counter' and cache_delta_valid and isinstance(previous_cache, dict):
                    old = previous_cache.get(name)
                    if type(old) is int and value >= old:
                        delta = value - old
                row('ram_dedup', name, kind, unit, value, delta=delta)
            for name, value in rates.items():
                row('ingress', name, 'rate', 'samples/s' if 'samples' in name else 'messages/s', value)
            histograms = [('ingress', key, value) for key, value in ing.get('Durations', {}).items()
                          if key in ('queue_wait', 'validation', 'persistence', 'ack_publish')]
            for key in ('batch_wait', 'group_persistence'):
                histogram = (ing.get('Durations') or {}).get(key)
                if microbatch_availability == 'available' and isinstance(histogram, dict):
                    histograms.append(('ingress_durations', key, histogram))
                else:
                    for suffix, unit in (('count', 'count'), ('sum', 'seconds'), ('overflow', 'count')):
                        unavailable('ingress_durations', key + '.' + suffix, 'histogram', unit)
            for group, field_specs in CONTROL_EXPORT_FIELDS.items():
                control = snap.get(group) or {}
                group_availability = (snap.get('availability') or {}).get(CONTROL_AVAILABILITY[group])
                if group_availability is None and isinstance(snap.get(group), dict):
                    group_availability = 'available'
                for key, (kind, unit) in field_specs.items():
                    value = control.get(key)
                    if kind == 'histogram':
                        if not (isinstance(value, dict) and group_availability == 'available'):
                            status = group_availability if group_availability in ('disabled', 'not_implemented') else 'unavailable'
                            for suffix, hist_unit in (('count', 'count'), ('sum', 'seconds'),
                                                      ('overflow', 'count')):
                                writer.writerow(dict(base, group=group, metric=key + '.' + suffix,
                                                     kind='histogram', unit=hist_unit, availability=status))
                        window = paper_p1_histogram_window(previous, snap, group, key)
                        if window['availability'] == 'available':
                            row(group, key + '.window.count', 'histogram_window', 'count', window['count'])
                            row(group, key + '.window.overflow', 'histogram_window', 'count', window['overflow'])
                            lower = 0.0
                            for upper, count in zip(window['upper_seconds'], window['buckets']):
                                row(group, key + '.window.bucket_upper_seconds_' + str(upper),
                                    'histogram_bucket_window', 'count', count)
                                row(group, key + '.window.bucket_resolution_seconds_' + str(upper),
                                    'histogram_bucket_resolution', 'seconds', upper - lower)
                                lower = upper
                            row(group, key + '.window.overflow_is_infinity', 'histogram_metadata',
                                'boolean', window['overflow'] > 0)
                            for p in (50, 95, 99):
                                percentile = window[f'p{p}_seconds']
                                writer.writerow(dict(base, group=group, metric=key + f'.window.p{p}_seconds',
                                                     kind='histogram_percentile', unit='seconds',
                                                     value=percentile, availability='available'))
                            writer.writerow(dict(base, group=group, metric=key + '.window.percentile_method',
                                                 kind='histogram_metadata', unit='method',
                                                 value=window['percentile_method'], availability='available'))
                        else:
                            writer.writerow(dict(base, group=group, metric=key + '.window',
                                                 kind='histogram_window', unit='count',
                                                 availability=window['availability']))
                        if isinstance(value, dict) and group_availability == 'available':
                            histograms.append((group, key, value))
                    elif group_availability != 'available' or type(value) not in (int, float, bool):
                        status = group_availability if group_availability in ('disabled', 'not_implemented') else 'unavailable'
                        writer.writerow(dict(base, group=group, metric=key, kind=kind, unit=unit,
                                             availability=status))
                    else:
                        row(group, key, kind, unit, value)
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
            # P7 WebSocket fields are a fixed allowlist; never recurse through arbitrary payloads.
            ws = snap.get('websocket_metrics') or {}
            ws_status = (snap.get('availability') or {}).get('websocket', 'unavailable')
            ws_fields = {'sessions': ('gauge','count'), 'subscriptions': ('gauge','count'),
                         'global_queued_work': ('gauge','count'), 'global_queued_bytes': ('gauge','bytes'),
                         'session_queued_in_flight_bytes': ('gauge','bytes'), 'submissions': ('counter','count'),
                         'new_events': ('counter','count'), 'build_failures': ('counter','count'),
                         'deliveries_enqueued': ('counter','count'), 'telemetry_writes_completed': ('counter','count'),
                         'control_writes_completed': ('counter','count'), 'drops': ('counter','count'),
                         'slow_closes': ('counter','count'), 'auth_denials': ('counter','count'),
                         'origin_denials': ('counter','count'), 'expiries': ('counter','count'),
                         'revocations': ('counter','count'), 'origin_missing': ('counter','count')}
            for key, (kind, unit) in ws_fields.items():
                value = ws.get(key)
                if ws_status == 'available' and type(value) in (int, float):
                    row('websocket', key, kind, unit, value)
                else:
                    writer.writerow(dict(base, group='websocket', metric=key, kind=kind, unit=unit,
                                         availability=ws_status if ws_status in ('disabled','unavailable') else 'unavailable'))
            for key in ('confirmed_to_submit_seconds','confirmed_to_dispatch_seconds','confirmed_to_write_seconds'):
                h = ws.get(key)
                if ws_status == 'available' and isinstance(h, dict):
                    row('websocket', key + '.count', 'histogram', 'count', h['count'])
                    row('websocket', key + '.sum', 'histogram', 'seconds', h['sum_seconds'])
                    row('websocket', key + '.overflow', 'histogram', 'count', h['overflow'])
                    for upper, count in zip(h['upper_seconds'], h['buckets']):
                        row('websocket', key + '.bucket_upper_ms_' + str(upper * 1000), 'histogram_bucket', 'count', count)
                else:
                    for suffix, unit in (('count','count'),('sum','seconds'),('overflow','count')):
                        writer.writerow(dict(base,group='websocket',metric=key+'.'+suffix,kind='histogram',unit=unit,availability=ws_status))
            for key in ('acquired', 'idle', 'total', 'max', 'acquire_count', 'empty_acquire_count', 'canceled_acquire_count', 'acquire_seconds'):
                if key in snap.get('db', {}):
                    row('db', key, 'pool_stat', 'seconds' if key.endswith('seconds') else 'count', snap['db'][key])
            for key in ('ingress', 'http', 'auth', 'db', 'command', 'dispatch_e2e', 'result',
                         'persistence', 'group_persistence', 'batch_wait', 'websocket'):
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
                         state, during=None, tick=None, timeout=45, allow_incomplete=False,
                         diagnostic=None):
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
    started = time.monotonic()
    deadline = started + timeout
    next_progress = started + 10.0
    if tag.startswith('p6-'):
        print('P6_PROGRESS event=gateway_started tag=%s elapsed_seconds=0.000 remaining_seconds=%.3f' %
              (tag, timeout), flush=True)

    def capture_diagnostic(reason):
        """Best-effort exact-owned, bounded private evidence; never replace failure."""
        try:
            current = registry.resolve(item)
            if current is None:
                raise ValueError('container_unavailable')
            evidence = {
                'tag': tag,
                'reason': reason,
                'elapsed_seconds': round(max(0.0, time.monotonic() - started), 3),
                'budget_remaining_seconds': round(max(0.0, deadline - time.monotonic()), 3),
                'container': {key: current.get('State', {}).get(key) for key in
                              ('Status', 'Running', 'ExitCode', 'StartedAt', 'FinishedAt')},
            }
            logs = registry.command(['logs', '--tail', '1000', current['Id']])
            evidence['logs_returncode'] = logs.returncode
            smoke_text = (logs.stdout + logs.stderr).decode(errors='replace')
            private_write(fixture.private / ('gateway-' + tag + '-diagnostic.log'),
                          smoke_text[-65536:])
            private_write(fixture.private / ('gateway-' + tag + '-diagnostic.json'),
                          json.dumps(evidence, sort_keys=True) + '\n')
        except Exception:
            # Do not expose exception strings (which may contain command arguments).
            try:
                private_write(fixture.private / ('gateway-' + tag + '-diagnostic-error.txt'),
                              'diagnostic_collection_failed\n')
            except Exception:
                pass
        if diagnostic is not None:
            try:
                diagnostic(reason)
            except Exception:
                try:
                    private_write(fixture.private / ('gateway-' + tag + '-diagnostic-hook-error.txt'),
                                  'diagnostic_hook_failed\n')
                except Exception:
                    pass

    if during:
        try:
            during()
        except BaseException:
            capture_diagnostic('callback_exception')
            raise
        if time.monotonic() >= deadline:
            capture_diagnostic('timeout_after_callback')
            raise ValueError('paper_p0_gateway_failed')
    while True:
        state.checkpoint()
        obj = registry.resolve(item)
        if not obj['State']['Running']:
            break
        if tick:
            try:
                tick()
            except BaseException:
                capture_diagnostic('tick_exception')
                raise
        if time.monotonic() >= deadline:
            capture_diagnostic('timeout')
            raise ValueError('paper_p0_gateway_failed')
        now = time.monotonic()
        if tag.startswith('p6-') and now >= next_progress:
            print('P6_PROGRESS event=gateway_running tag=%s elapsed_seconds=%.3f remaining_seconds=%.3f' %
                  (tag, max(0.0, now - started), max(0.0, deadline - now)), flush=True)
            next_progress = now + 10.0
        time.sleep(0.2)
    output = registry.command(['logs', obj['Id']])
    private_write(fixture.private / ('gateway-' + tag + '.log'),
                  (output.stdout + output.stderr).decode(errors='replace'))
    if obj['State']['ExitCode'] or output.returncode:
        if not allow_incomplete:
            raise ValueError('paper_p0_gateway_failed')
        from stage3_mqtt_ingress_runner import parse_run_summary
        try:
            incomplete = parse_run_summary(output.stdout.decode(errors='replace'))
        except RuntimeError:
            raise ValueError('paper_p0_gateway_failed') from None
        if incomplete.get('completed') != 'false' or int(incomplete.get('pending_rows', '0')) <= 0:
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


def prepare_paper_workload(fixture, registry, endpoint, containers, images, state, gateway_ids=None):
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
    if gateway_ids is not None:
        if (not isinstance(gateway_ids, (list, tuple)) or len(gateway_ids) != 2 or
                any(not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', value)
                    for value in gateway_ids) or len(set(gateway_ids)) != 2):
            raise ValueError('paper_workload_gateway_ids_invalid')
        selected_gateway_ids = tuple(gateway_ids)
    else:
        selected_gateway_ids = tuple('gw_p0_' + role + '_' + secrets.token_hex(4) for role in ('a', 'b'))
    specs = [replace(s, gateway_id=gateway_id,
                     gateway_reported={}, gateway_desired={}, gateway_reported_version=0,
                     gateway_desired_version=0, sensor_desired={}, sensor_desired_version=0)
             for gateway_id, s in zip(selected_gateway_ids, default_specs())]
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
    if fixture.paper_p5b_cache_smoke:
        # The moved-ID case must remain a valid same-Gateway telemetry target;
        # provision a second Sensor through the ordinary owner API path.
        for role, spec in zip(('a', 'b'), specs):
            conflict_sensor_id = spec.sensor_id + '-conflict'
            status, _, body = http_request(endpoint, 'PUT',
                f'/v1/gateways/{spec.gateway_id}/sensors/{urllib.parse.quote(conflict_sensor_id, safe="")}',
                token=tokens[role], body={'name': 'Cache conflict sensor', 'unit': spec.sensor_unit})
            if status != 201 or body.get('entity_id') != f'urn:ngsi-ld:Sensor:{spec.gateway_id}:{conflict_sensor_id}':
                raise ValueError('paper_p5b_conflict_sensor_seed_failed')
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
    return ("--prepare", "--startup-only", "--positive-http-only", "--role-http-only", "--full-http",
            "--paper-p0", "--paper-p1", "--paper-p3", "--p5a-control-regression", "--p5b-control-regression",
            "--paper-p4", "--paper-p5a", "--paper-p5b", "--cache-mode", "--mode", "--dataset")


def paper_p4_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    """P4 cleanup is exact against this run's owned registry, not guessed totals."""
    items = list(registry.items)
    kinds = ('container', 'network', 'volume')
    if not items:
        return False
    for item in items:
        receipt = item.get('receipt')
        if (item.get('kind') not in kinds or item.get('run') != fixture.project
                or item.get('task') != 'native-compose-allocation'
                or item.get('allocation_attempted') is not True or item.get('removed') is not True
                or not isinstance(receipt, dict) or set(receipt) != {'id', 'created', 'labels'}
                or not isinstance(receipt.get('created'), str) or not receipt['created']
                or not isinstance(receipt.get('labels'), dict)
                or any(receipt['labels'].get(key) != value for key, value in item.get('labels', {}).items())):
            return False
        identity = receipt.get('id')
        if item['kind'] == 'volume':
            if identity != item.get('name'):
                return False
        elif not isinstance(identity, str) or not re.fullmatch(r'[a-f0-9]{64}', identity):
            return False
    registered = {kind: sum(item.get('kind') == kind for item in items)
                  for kind in kinds}
    removed = {kind: sum(item.get('kind') == kind and item.get('removed') is True for item in items)
               for kind in kinds}
    return (evidence.get('full_p4_accepted') is True and cleanup_verified is True
            and cleanup_counts == removed and removed == registered)


def paper_p5a_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    """The workload marker is intentionally narrower than full P5a acceptance."""
    accepted = (evidence.get('full_p5a_workload_accepted') is True and cleanup_verified is True
            and paper_p4_cleanup_acceptance(
                fixture, {'full_p4_accepted': True}, cleanup_counts, cleanup_verified, registry))
    if fixture.paper_p5b_cache_smoke:
        from paper_p5a_support import cache_smoke_acceptance_ready
        accepted = accepted and cache_smoke_acceptance_ready({
            'warm_replay': evidence.get('warm_replay'),
            'conflicts': evidence.get('conflict_assertions'),
            'cold_replay': evidence.get('cold_replay'),
            'exact_cleanup_verified': cleanup_verified is True})
        evidence['p5b_cache_functional_accepted'] = accepted
        evidence['exact_cleanup_verified'] = cleanup_verified is True
    return accepted


def paper_p8_e1_validation_valid(evidence):
    result = evidence.get('validation')
    return (isinstance(result, dict) and result.get('valid') is True
            and isinstance(result.get('failures'), list) and not result['failures'])


def paper_p8_e1_report_acceptance(evidence, cleanup_verified, owned_cleanup_accepted):
    return (evidence.get('status') == 'PASS' and paper_p8_e1_validation_valid(evidence)
            and evidence.get('full_p8_accepted') is False and cleanup_verified is True
            and owned_cleanup_accepted is True)


def paper_p8_write_private_csv(path, header, rows):
    """Create a private CSV artifact without following links or widening mode."""
    path = Path(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, 'w', newline='') as output:
        os.fchmod(output.fileno(), 0o600)
        writer = csv.writer(output)
        writer.writerow(header)
        writer.writerows(rows)


def paper_p8_e1_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    """E1 is a workload pilot only; require helper validation and exact owned cleanup."""
    workload = evidence.get('workload')
    mapping = evidence.get('mapping') if isinstance(evidence, dict) else None
    expected_unique = sum(mapping.get('messages_per_target', {}).values()) if isinstance(mapping, dict) else None
    return (evidence.get('status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
            and paper_p8_e1_validation_valid(evidence)
            and isinstance(mapping, dict)
            and evidence.get('sockets') == mapping.get('counts', {}).get('sockets')
            and isinstance(workload, dict)
            and isinstance(workload.get('telemetry'), dict)
            and workload['telemetry'].get('generated_unique') == expected_unique
            and evidence.get('full_p8_accepted') is not True and cleanup_verified is True
            and paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True},
                                            cleanup_counts, cleanup_verified, registry))


def paper_p8_e2_validate_fixture_flags(effective, *, workload='steady_burst', e2_support=None,
                                      fixture=None):
    if e2_support is None:
        e2_support = paper_p8_e2_support()
    if workload == 'equal_samples':
        allowed = ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
            'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
            'MQTT_SHAPER_RATE_PER_SECOND', 'MQTT_SHAPER_BURST', 'MQTT_MICROBATCH_MAX_MESSAGES',
            'MQTT_MICROBATCH_MAX_ROWS', 'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT')
        env = effective.get('services', {}).get('backend', {}).get('environment', {})
        flags = {key: env[key] for key in allowed if key in env}
        expected = {
            'WEBSOCKET_ENABLED': 'false', 'STAGE5_ENABLED': 'false',
            'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'false',
            'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'false',
            'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
            'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms'
        }
        normalized = {key: str(value).lower() for key, value in flags.items()}
        if any(normalized.get(key) != value for key, value in expected.items()):
            raise ValueError('p8_e2_effective_fixture_flags_mismatch')
        return flags
    return e2_support._validate_fixture_flags(effective, fixture)


def paper_p8_e2_profiles_valid(evidence):
    profiles = evidence.get('profiles') if isinstance(evidence, dict) else None
    if not isinstance(profiles, list) or len(profiles) != 2:
        return False
    workload_type = evidence.get('workload_type') or evidence.get('workload')
    if workload_type == 'equal_samples':
        e2_mod = paper_p8_e2_support()
        if hasattr(e2_mod, 'paper_p8_e2_equal_samples_profiles_valid'):
            return e2_mod.paper_p8_e2_equal_samples_profiles_valid(evidence)
        found = set()
        for profile in profiles:
            if not isinstance(profile, dict):
                return False
            samples = profile.get('samples_per_message')
            validation = profile.get('validation')
            workloads = profile.get('workloads')
            if (samples not in (100, 1) or samples in found or profile.get('status') != 'PASS'
                    or not isinstance(validation, dict) or validation.get('valid') is not True
                    or validation.get('failures') != [] or not isinstance(workloads, dict)):
                return False
            found.add(samples)
            for arm in ('A', 'B'):
                workload = workloads.get(arm)
                telemetry = workload.get('telemetry') if isinstance(workload, dict) else None
                if not isinstance(telemetry, dict) or telemetry.get('pending_messages') != 0:
                    return False
                if telemetry.get('application_acks') != telemetry.get('generated_unique'):
                    return False
        return found == {100, 1}

    found = set()
    for profile in profiles:
        if not isinstance(profile, dict):
            return False
        samples = profile.get('samples_per_message')
        validation = profile.get('validation')
        workloads = profile.get('workloads')
        if (samples not in (100, 1) or samples in found or profile.get('status') != 'PASS'
                or not isinstance(validation, dict) or validation.get('valid') is not True
                or validation.get('failures') != [] or not isinstance(workloads, dict)):
            return False
        found.add(samples)
        for arm, expected in (('A', 8), ('B', 16)):
            workload = workloads.get(arm)
            telemetry = workload.get('telemetry') if isinstance(workload, dict) else None
            if (not isinstance(telemetry, dict) or telemetry.get('generated_unique') != expected
                    or telemetry.get('application_acks') != expected or telemetry.get('pending_messages') != 0):
                return False
    return found == {100, 1}


def paper_p8_e2_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    return (isinstance(evidence, dict) and evidence.get('status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
            and evidence.get('full_p8_accepted') is False and paper_p8_e2_profiles_valid(evidence)
            and cleanup_verified is True
            and paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True},
                                            cleanup_counts, cleanup_verified, registry))


def paper_p8_e2_report_acceptance(evidence, cleanup_verified):
    return (isinstance(evidence, dict) and evidence.get('status') == 'PASS'
            and evidence.get('workload_status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
            and evidence.get('full_p8_accepted') is False
            and paper_p8_e2_profiles_valid(evidence) and evidence.get('cleanup_verified') is True
            and cleanup_verified is True)


def paper_p8_prepare_only_report(fixture, *, remote_outcome, cleanup_verified, cleanup_counts):
    """Build the final report for an E1/E2 held session without assuming a workload ran."""
    session_path = fixture.private / 'session.json'
    remote_session = json.loads(session_path.read_text()) if session_path.is_file() else {}
    experiment = remote_session.get('experiment')
    if experiment not in ('E1', 'E2', 'E3', 'E4', 'E5'):
        experiment = 'E2' if getattr(fixture, 'paper_p8_e2', False) else 'E1'
    if experiment == 'E2':
        phase_id = remote_session.get('phase_id')
        workload = remote_session.get('workload') or getattr(fixture, 'paper_p8_e2_workload', 'steady_burst')
        if remote_outcome == 'VALIDATED' and isinstance(phase_id, str):
            artifact = f'p8-e2-{phase_id}-remote-result.json'
        elif workload == 'equal_samples':
            artifact = 'p8-e2-equal-samples-pilot.json'
        else:
            artifact = 'p8-e2-pilot.json'
        evidence_name = artifact
        workload_field = 'p8_e2_workload_verified'
    elif experiment == 'E3':
        artifact = paper_p8_e3_artifact_name(fixture)
        if remote_outcome == 'VALIDATED':
            artifact = artifact.replace('.json', '-remote-result.json')
        evidence_name = artifact
        workload_field = 'p8_e3_workload_verified'
    elif experiment == 'E4':
        artifact = paper_p8_e4_artifact_name(fixture)
        evidence_name = artifact
        workload_field = 'p8_e4_workload_verified'
    elif experiment == 'E5':
        artifact = paper_p8_e5_artifact_name(fixture)
        evidence_name = artifact
        workload_field = 'p8_e5_workload_verified'
    else:
        # Keep E1's established gate: its pilot summary is the acceptance input.
        artifact = 'p8-e1-remote-result.json'
        evidence_name = 'p8-e1-pilot.json'
        workload_field = 'p8_e1_workload_verified'

    artifact_path = fixture.artifacts / evidence_name
    if not artifact_path.is_file():
        raise ValueError('p8_prepare_report_artifact_missing')
    evidence = json.loads(artifact_path.read_text())
    validation = evidence.get('validation')
    if experiment in ('E2', 'E3') and remote_outcome == 'VALIDATED' and (not isinstance(validation, dict)
                                           or validation.get('valid') is not True):
        raise ValueError('p8_prepare_report_validation_missing')
    if remote_outcome == 'NOT_RUN' and cleanup_verified:
        label = f'PAPER_P8_{experiment}_PREPARATION_ONLY'
    elif remote_outcome == 'VALIDATED' and cleanup_verified and evidence.get('status') == 'PASS':
        label = f'PAPER_P8_{experiment}_WORKLOAD_VERIFIED'
    else:
        label = f'PAPER_P8_{experiment}_WORKLOAD_VALIDATION_FAILED'
    return {'schema_version': 1, 'status': label, 'project': fixture.project,
            'native_executed': True, workload_field: remote_outcome == 'VALIDATED',
            'workload_outcome': remote_outcome, 'full_p8_accepted': False,
            'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': cleanup_counts,
            'artifact': artifact}


def paper_p8_prepare_only_run_status(experiment, *, failed, cleanup_verified, remote_outcome):
    prefix = experiment if experiment in ('E1', 'E2', 'E3', 'E4', 'E5') else 'E1'
    if not failed and cleanup_verified and remote_outcome == 'VALIDATED':
        return f'{prefix}_WORKLOAD_VERIFICATION_PASS'
    if not failed and cleanup_verified and remote_outcome == 'NOT_RUN':
        return f'{prefix}_PREPARATION_ONLY_CLEANUP'
    if remote_outcome == 'VALIDATION_FAILED':
        return f'{prefix}_WORKLOAD_VALIDATION_FAILED'
    return 'FAIL'


def paper_p8_e4_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    validation = evidence.get('validation') if isinstance(evidence, dict) else None
    delta = evidence.get('database_delta') if isinstance(evidence, dict) else None
    raw = evidence.get('raw_verification') if isinstance(evidence, dict) else None
    unique = (evidence.get('workloads') or {}).get('unique', {}) if isinstance(evidence, dict) else {}
    replay = (evidence.get('workloads') or {}).get('replay', {}) if isinstance(evidence, dict) else {}
    ut, rt = unique.get('telemetry', {}), replay.get('telemetry', {})
    unique_count = getattr(fixture, 'paper_p8_e4_unique_count', 20)
    replay_count = getattr(fixture, 'paper_p8_e4_replay_count', 10)
    cold_status = evidence.get('coldrestart') if isinstance(evidence, dict) else None
    expected_cold = 'PASS' if getattr(fixture, 'paper_p8_e4_cache_mode', None) == 'on' else 'NOT APPLICABLE'
    return (isinstance(evidence, dict) and evidence.get('status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
            and evidence.get('lane_marker') == 'PAPER_P8_E4_PILOT_COMPLETE'
            and cold_status == expected_cold
            and evidence.get('full_p8_accepted') is False
            and isinstance(evidence.get('new_ws_event_proof'), dict)
            and evidence['new_ws_event_proof'].get('status') == 'PASS'
            and isinstance(validation, dict) and validation.get('valid') is True and validation.get('failures') == []
            and evidence.get('runner_stopped') is True
            and isinstance(evidence.get('conflicts'), dict) and evidence['conflicts'].get('status') == 'PASS'
            and isinstance(evidence.get('durable_gateway_reopen'), dict)
            and evidence['durable_gateway_reopen'].get('status') == 'PASS'
            and isinstance(delta, dict) and delta.get('ledger') == unique_count
            and delta.get('raw') == unique_count * 100
            and delta.get('temporal') == unique_count
            and isinstance(raw, list) and len(raw) == 2
            and all(isinstance(row, dict) and row.get('verified_exact') is True for row in raw)
            and ut.get('generated_unique') == unique_count and ut.get('application_acks') == unique_count
            and ut.get('ignored_valid_acks') == 1 and ut.get('pending_messages') == 0
            and rt.get('replay_offered') == replay_count and rt.get('replay_application_acks') == replay_count
            and cleanup_verified is True
            and paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True},
                                            cleanup_counts, cleanup_verified, registry))


def paper_p8_e4_conflicts_acceptance(conflicts):
    """Strict acceptance of E4 representative conflict rejection evidence."""
    if not isinstance(conflicts, dict) or conflicts.get('status') != 'PASS':
        return False
    cases = conflicts.get('cases')
    if not isinstance(cases, dict):
        return False
    required = {'same_id_changed_payload', 'different_first_sequence_overlap'}
    if set(cases) != required:
        return False
    for name in required:
        c = cases[name]
        if (not isinstance(c, dict) or c.get('status') != 'PASS'
                or c.get('schema_valid') is not True
                or c.get('positive_control_ack_count') != 1
                or c.get('conflict_ack_count') != 0
                or c.get('database_unchanged') is not True
                or not c.get('expected_rejection_reason')
                or c.get('observed_rejection_reason') != c.get('expected_rejection_reason')
                or not isinstance(c.get('guard_precondition'), dict)
                or c['guard_precondition'].get('matched') is not True):
            return False
    return True


def paper_p8_e4_durable_reopen_acceptance(dgr):
    """Strict acceptance of gateway reopen proofs following durable-fault."""
    if not isinstance(dgr, dict) or dgr.get('status') != 'PASS':
        return False
    fault = dgr.get('fault')
    reopen = dgr.get('reopen')
    if not isinstance(fault, dict) or fault.get('status') != 'PASS' or fault.get('phase') != 'durable-fault':
        return False
    if not isinstance(reopen, dict) or reopen.get('status') != 'PASS' or reopen.get('phase') != 'durable-reopen':
        return False

    pending = fault.get('pending')
    pending_before = reopen.get('pending_before')
    if not isinstance(pending, dict) or pending != pending_before:
        return False
    payload_sha = pending.get('payload_sha256')
    msg_id = pending.get('message_id')
    if (not isinstance(payload_sha, str) or re.fullmatch(r'[0-9a-f]{64}', payload_sha) is None
            or not isinstance(msg_id, str) or not msg_id
            or pending.get('sample_count') != 100 or pending.get('interval_us') != 10000):
        return False

    hdr = pending.get('header')
    hdr_before = pending_before.get('header')
    if (not isinstance(hdr, dict) or not isinstance(hdr_before, dict) or hdr != hdr_before
            or hdr.get('gateway_id') != pending.get('gateway_id')
            or not isinstance(hdr.get('gateway_id'), str) or not hdr['gateway_id']
            or hdr.get('sensor_id') != pending.get('sensor_id')
            or not isinstance(hdr.get('sensor_id'), str) or not hdr['sensor_id']
            or hdr.get('boot_id') != pending.get('boot_id')
            or not isinstance(hdr.get('boot_id'), str) or not hdr['boot_id']
            or hdr.get('first_sequence') != pending.get('sequence')
            or type(hdr.get('first_sequence')) is not int
            or hdr.get('sample_count') != 100
            or hdr.get('sample_interval_us') != 10000
            or not isinstance(hdr.get('measured_at'), str) or not hdr['measured_at']):
        return False
    try:
        datetime.fromisoformat(hdr['measured_at'].replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return False

    fault_sql = fault.get('sql')
    if (not isinstance(fault_sql, dict) or fault_sql.get('status') != 'PASS'
            or fault_sql.get('raw_count') != 100
            or fault_sql.get('message_id') != msg_id
            or fault_sql.get('payload_sha256') != payload_sha):
        return False

    ledger_fp = fault_sql.get('ledger_fingerprint')
    if not isinstance(ledger_fp, str) or re.fullmatch(r'[0-9a-f]{32}', ledger_fp) is None:
        return False

    if reopen.get('joined') is not True:
        return False

    if (fault.get('main_dataset_unchanged') is not True
            or reopen.get('main_dataset_unchanged') is not True):
        return False

    binding_sha = fault.get('binding_sha256')
    if (not isinstance(binding_sha, str) or re.fullmatch(r'[0-9a-f]{64}', binding_sha) is None
            or reopen.get('binding_sha256') != binding_sha):
        return False

    expected_delta = {'ledger': 1, 'raw': 100, 'temporal': 1, 'configuration': 0}
    if fault.get('database_delta') != expected_delta or reopen.get('database_delta') != expected_delta:
        return False

    def _valid_db_snapshot(s):
        if not isinstance(s, dict):
            return False
        for k in ('ledger', 'raw', 'temporal', 'configuration'):
            if type(s.get(k)) is not int or s[k] < 0:
                return False
        fp = s.get('fingerprint')
        if not isinstance(fp, str) or re.fullmatch(r'[0-9a-f]{32}', fp) is None:
            return False
        return True

    fault_before = fault.get('database_before')
    fault_after = fault.get('database_after')
    reopen_before = reopen.get('database_before')
    reopen_after = reopen.get('database_after')

    if (not _valid_db_snapshot(fault_before) or not _valid_db_snapshot(fault_after)
            or not _valid_db_snapshot(reopen_before) or not _valid_db_snapshot(reopen_after)):
        return False

    if fault_after != reopen_before:
        return False

    fault_stage_delta = {k: fault_after[k] - fault_before[k] for k in expected_delta}
    reopen_stage_delta = {k: reopen_after[k] - reopen_before[k] for k in expected_delta}
    if fault_stage_delta != expected_delta or reopen_stage_delta != expected_delta:
        return False

    expected_cumulative_delta = {'ledger': 2, 'raw': 200, 'temporal': 2, 'configuration': 0}
    cumulative_delta = {k: reopen_after[k] - fault_before[k] for k in expected_cumulative_delta}
    if cumulative_delta != expected_cumulative_delta:
        return False

    reopen_sql = reopen.get('sql')
    if (not isinstance(reopen_sql, dict) or reopen_sql.get('status') != 'PASS'
            or reopen_sql.get('raw_count') != 100
            or reopen_sql.get('main_dataset_unchanged') is not True
            or reopen_sql.get('auxiliary_delta') != {'ledger': 2, 'raw': 200, 'temporal': 2}
            or reopen_sql.get('interval_us') != 10000
            or type(reopen_sql.get('first_sequence')) is not int
            or reopen_sql.get('last_sequence') != reopen_sql['first_sequence'] + 99
            or not isinstance(reopen_sql.get('first_timestamp'), str) or not reopen_sql['first_timestamp']
            or not isinstance(reopen_sql.get('last_timestamp'), str) or not reopen_sql['last_timestamp']):
        return False

    if reopen.get('pending_rows') != 0 or reopen.get('quarantined_rows') != 0:
        return False

    if type(reopen.get('application_acks')) is not int or reopen['application_acks'] < 2:
        return False

    return True


def paper_p8_e4_startup_summary(fixture, p8, accepted, cleanup_verified, cleanup_counts, artifact_name):
    cache_off = getattr(fixture, 'paper_p8_e4_cache_mode', None) == 'off'
    cold_subset = p8.get('workload_status') == 'COLD_REPLAY_SUBSET_VALIDATED'
    durable_subset = p8.get('workload_status') == 'DURABLE_REOPEN_SUBSET_VALIDATED'
    conflicts_subset = p8.get('workload_status') in ('CONFLICTS_SUBSET_VALIDATED', 'PILOT_COMPLETE_VALIDATED')
    pilot_complete = p8.get('workload_status') == 'PILOT_COMPLETE_VALIDATED'
    lane = ('PAPER_P8_E4_PILOT_COMPLETE' if pilot_complete else
            'PAPER_P8_E4_CONFLICTS_SUBSET' if conflicts_subset else
            'PAPER_P8_E4_DURABLE_REOPEN_SUBSET' if durable_subset else
            'PAPER_P8_E4_COLD_REPLAY_SUBSET' if cold_subset else
            'PAPER_P8_E4_CACHE_OFF_SUBSET' if cache_off else 'PAPER_P8_E4_WARM_SUBSET')
    durable_status = (p8['durable_gateway_reopen'].get('status')
                      if isinstance(p8.get('durable_gateway_reopen'), dict)
                      else p8.get('durable_gateway_reopen', 'NOT RUN'))
    conflicts_status = (p8['conflicts'].get('status')
                        if isinstance(p8.get('conflicts'), dict)
                        else p8.get('conflicts', 'NOT RUN'))
    return {'schema_version': 1,
        'status': ('PAPER_P8_E4_PILOT_COMPLETE_VALIDATED' if pilot_complete else
                   'PAPER_P8_E4_CONFLICTS_SUBSET_VALIDATED' if conflicts_subset else
                   'PAPER_P8_E4_DURABLE_REOPEN_SUBSET_VALIDATED' if durable_subset else
                   'PAPER_P8_E4_COLD_REPLAY_SUBSET_VALIDATED' if cold_subset else
                   'PAPER_P8_E4_CACHE_OFF_SUBSET_VALIDATED' if cache_off else
                   'PAPER_P8_E4_WARM_SUBSET_VALIDATED') if accepted else 'FAIL',
        'lane': lane, 'cache_mode': getattr(fixture, 'paper_p8_e4', None),
        'project': getattr(fixture, 'project', None), 'native_executed': True,
        'p8_e4_warm_subset_validated': accepted, 'full_p8_accepted': False,
        'workload_outcome': ('PILOT_COMPLETE_VALIDATED' if pilot_complete else
                             'CONFLICTS_SUBSET_VALIDATED' if conflicts_subset else
                             'DURABLE_REOPEN_SUBSET_VALIDATED' if durable_subset else
                             'COLD_REPLAY_SUBSET_VALIDATED' if cold_subset else
                             'CACHE_OFF_SUBSET_VALIDATED' if cache_off else
                             'WARM_SUBSET_VALIDATED') if accepted else p8.get('workload_status'),
        'coldrestart': p8.get('coldrestart'),
        'durable_gateway_reopen': durable_status,
        'conflicts': conflicts_status, 'exact_cleanup_verified': cleanup_verified,
        'cleanup_counts': cleanup_counts, 'artifact': artifact_name}


def paper_p8_e4_warm_subset_acceptance(evidence, cleanup_verified, expected_counts=None):
    workloads = evidence.get('workloads') if isinstance(evidence, dict) else None
    unique = workloads.get('unique', {}) if isinstance(workloads, dict) else {}
    replay = workloads.get('replay', {}) if isinstance(workloads, dict) else {}
    unique_workload, replay_workload = unique.get('telemetry'), replay.get('telemetry')
    ut = unique_workload.get('telemetry') if isinstance(unique_workload, dict) else None
    rt = replay_workload.get('telemetry') if isinstance(replay_workload, dict) else None
    delta = evidence.get('database_delta') if isinstance(evidence, dict) else None
    raw = evidence.get('raw_verification') if isinstance(evidence, dict) else None
    ws = evidence.get('new_ws_event_proof') if isinstance(evidence, dict) else None
    validation = evidence.get('validation') if isinstance(evidence, dict) else None
    cache = evidence.get('cache_window') if isinstance(evidence, dict) else None
    selected = (expected_counts if isinstance(expected_counts, dict) else
        evidence.get('selected_counts') if isinstance(evidence, dict) else None)
    if selected is None:
        selected = {'unique_count': 20, 'replay_count': 10}  # legacy pilot report contract
    unique_count, replay_count = selected.get('unique_count'), selected.get('replay_count')
    counts_valid = (type(unique_count) is int and unique_count >= 2 and unique_count % 2 == 0
                    and type(replay_count) is int and 1 <= replay_count <= unique_count)
    cold_subset = (isinstance(evidence, dict) and
                   evidence.get('workload_status') == 'COLD_REPLAY_SUBSET_VALIDATED')
    expiry_subset = (isinstance(evidence, dict) and
                     evidence.get('workload_status') == 'TTL_EXPIRY_SUBSET_VALIDATED')
    cache_off = isinstance(evidence, dict) and evidence.get('cache_mode') == 'off'
    expected_marker = ('PAPER_P8_E4_CACHE_OFF_SUBSET' if cache_off else
                       'PAPER_P8_E4_TTL_EXPIRY_SUBSET' if expiry_subset else 'PAPER_P8_E4_WARM_SUBSET')
    expected_outcome = ('CACHE_OFF_SUBSET_VALIDATED' if cache_off else
                        'TTL_EXPIRY_SUBSET_VALIDATED' if expiry_subset else 'WARM_SUBSET_VALIDATED')
    expected_cold = 'NOT APPLICABLE' if cache_off else 'NOT RUN'
    expected_phases = ['unique', 'replay', 'cold-replay'] if cold_subset else ['unique', 'replay']
    if expiry_subset:
        expected_phases.append('expiry-replay')
    if cold_subset:
        expected_marker = 'PAPER_P8_E4_COLD_REPLAY_SUBSET'
        expected_outcome = 'COLD_REPLAY_SUBSET_VALIDATED'
        expected_cold = 'PASS'
    windows = evidence.get('counter_windows') if isinstance(evidence, dict) else None
    replay_window = windows.get('replay') if isinstance(windows, dict) else None
    dbproof = replay_window.get('database_proof') if isinstance(replay_window, dict) else None
    cache_valid = (isinstance(cache, dict) and type(cache.get('attempts')) is int
        and cache['attempts'] > 0 and type(cache.get('transaction_commits')) is int
        and cache['transaction_commits'] >= replay_count and cache.get('database_duplicates') == cache['transaction_commits']
        and type(cache.get('confirmed_new_rows')) is int and cache['confirmed_new_rows'] == 0
        and type(cache.get('copy_calls')) is int and cache['copy_calls'] == 0
        and isinstance(dbproof, dict) and dbproof.get('exact_equal') is True
        and isinstance(dbproof.get('unique_ack'), list) and len(dbproof['unique_ack']) == 2
        and dbproof['unique_ack'] == dbproof.get('replay_end')) if cache_off else (
        isinstance(cache, dict) and type(cache.get('warm_lookup_hits')) is int
        and cache['warm_lookup_hits'] >= replay_count and type(cache.get('repository_calls_avoided')) is int
        and cache['repository_calls_avoided'] >= replay_count)
    cold_ws = (isinstance(ws, dict) and isinstance(ws.get('replay'), dict)
        and ws['replay'] == {'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
            'telemetry_writes_completed': 0, 'drops': 0})
    cold_window = evidence.get('cold_cache_window') if isinstance(evidence, dict) else None
    cold_deltas = cold_window.get('persistence_delta') if isinstance(cold_window, dict) else None
    replacement = evidence.get('replacement_proof') if isinstance(evidence, dict) else None
    cold_valid = (isinstance(cold_window, dict) and cold_window.get('status') == 'PASS'
        and cold_window.get('old_process_instance') and cold_window.get('process_instance')
        and cold_window.get('old_process_instance') != cold_window.get('process_instance')
        and cold_window.get('cache_entries_before') == 0
        and type(cold_window.get('cache_miss_delta')) is int and cold_window['cache_miss_delta'] > 0
        and cold_window.get('cache_hit_delta') == 0
        and isinstance(cold_deltas, dict) and type(cold_deltas.get('begin_attempts')) is int
        and cold_deltas['begin_attempts'] > 0 and type(cold_deltas.get('confirmed_commits')) is int
        and cold_deltas['confirmed_commits'] > 0 and cold_deltas.get('confirmed_new_rows') == 0
        and cold_deltas.get('copy_calls') == 0 and cold_window.get('application_ack_count') == replay_count
        and cold_window.get('database_unchanged') is True
        and isinstance(replacement, dict) and replacement.get('metrics_readback') is True
        and replacement.get('process_instance') == cold_window.get('process_instance'))
    cold_proven = bool(cold_valid and isinstance(ws, dict) and isinstance(ws.get('cold-replay'), dict)
        and ws['cold-replay'] == {'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
            'telemetry_writes_completed': 0, 'drops': 0}
        and evidence.get('cold_replay_journal_prefix_validated') is True)
    cold_genuinely_proven = bool(isinstance(evidence, dict)
        and 'cold-replay' in evidence.get('phase_history', []) and cold_proven)
    expiry = evidence.get('ttl_expiry') if isinstance(evidence, dict) else None
    expiry_proven = (isinstance(expiry, dict) and expiry.get('status') == 'PASS'
        and isinstance(expiry.get('evidence'), dict) and expiry['evidence'].get('status') == 'PASS'
        and expiry['evidence'].get('confirmed_age_seconds', 0) >= 600)
    if expiry_subset:
        expected_cold = 'NOT RUN'
    durable_subset = (isinstance(evidence, dict) and
                      (evidence.get('workload_status') in ('DURABLE_REOPEN_SUBSET_VALIDATED', 'PILOT_COMPLETE_VALIDATED')
                       or isinstance(evidence.get('durable_gateway_reopen'), dict)))
    conflicts_subset = (isinstance(evidence, dict) and
                        (evidence.get('workload_status') in ('CONFLICTS_SUBSET_VALIDATED', 'PILOT_COMPLETE_VALIDATED')
                         or isinstance(evidence.get('conflicts'), dict)))
    pilot_complete = (isinstance(evidence, dict) and
                      evidence.get('workload_status') == 'PILOT_COMPLETE_VALIDATED')
    if pilot_complete:
        expected_marker = 'PAPER_P8_E4_PILOT_COMPLETE'
        expected_outcome = 'PILOT_COMPLETE_VALIDATED'
        expected_cold = 'PASS' if cold_genuinely_proven else 'NOT RUN'
        expected_phases = (['unique', 'replay', 'cold-replay'] if cold_genuinely_proven else ['unique', 'replay']) + ['durable-fault', 'durable-reopen', 'conflicts']
    elif conflicts_subset:
        expected_marker = 'PAPER_P8_E4_CONFLICTS_SUBSET'
        expected_outcome = 'CONFLICTS_SUBSET_VALIDATED'
        expected_cold = 'NOT APPLICABLE' if cache_off else 'PASS' if cold_genuinely_proven else 'NOT RUN'
        expected_phases = (['unique', 'replay', 'cold-replay'] if cold_genuinely_proven else ['unique', 'replay'])
        if durable_subset:
            expected_phases = expected_phases + ['durable-fault', 'durable-reopen']
        expected_phases = expected_phases + ['conflicts']
    elif durable_subset:
        expected_marker = 'PAPER_P8_E4_DURABLE_REOPEN_SUBSET'
        expected_outcome = 'DURABLE_REOPEN_SUBSET_VALIDATED'
        expected_cold = 'NOT APPLICABLE' if cache_off else 'PASS' if cold_genuinely_proven else 'NOT RUN'
        expected_phases = (['unique', 'replay', 'cold-replay', 'durable-fault', 'durable-reopen']
                           if cold_genuinely_proven else ['unique', 'replay', 'durable-fault', 'durable-reopen'])
    durable_ok = (paper_p8_e4_durable_reopen_acceptance(evidence.get('durable_gateway_reopen')) if durable_subset
                  else evidence.get('durable_gateway_reopen') == 'NOT RUN')
    conflicts_ok = (paper_p8_e4_conflicts_acceptance(evidence.get('conflicts')) if conflicts_subset
                    else evidence.get('conflicts') == 'NOT RUN')
    journal_ok = (isinstance(evidence, dict) and isinstance(evidence.get('journal_sha256'), str)
        and re.fullmatch(r'[0-9a-f]{64}', evidence['journal_sha256']) is not None)
    return (isinstance(evidence, dict) and evidence.get('status') == 'PASS'
        and evidence.get('lane_marker') == expected_marker
        and evidence.get('workload_status') == expected_outcome
        and evidence.get('phase_history') == expected_phases
        and evidence.get('full_p8_accepted') is False
        and evidence.get('coldrestart') == expected_cold
        and (not expiry_subset or expiry_proven)
        and durable_ok
        and conflicts_ok
        and evidence.get('physical_ab') == 'NOT RUN'
        and evidence.get('generator_colocated') is True
        and evidence.get('runner_stopped') is True
        and journal_ok
        and evidence.get('journal_continuity') is True
        and evidence.get('replay_journal_prefix_validated') is True
        and (not (cold_subset or (durable_subset and cold_genuinely_proven))
             or evidence.get('cold_replay_journal_prefix_validated') is True)
        and evidence.get('cleanup_verified') is True and cleanup_verified is True
        and counts_valid and isinstance(delta, dict) and delta == {
            'ledger': unique_count, 'raw': unique_count * 100, 'temporal': unique_count, 'configuration': 0}
        and isinstance(validation, dict) and validation.get('valid') is True and validation.get('failures') == []
        and cache_valid
        and isinstance(raw, list) and len(raw) == 2
        and isinstance(raw, list) and len(raw) == 2
        and all(isinstance(row, dict) and row.get('raw_identity_value_mismatches') == 0 and
            ((row.get('verified_exact') is True and row.get('ledger_count') == unique_count // 2) or
             (row.get('verified_sample') is True and
              row.get('ledger_count') == min(unique_count // 2, 10) and
              isinstance(row.get('identity_ranges'), list) and len(row['identity_ranges']) == min(unique_count // 2, 10)))
            for row in raw)
        and isinstance(ut, dict) and isinstance(rt, dict)
        and ut.get('generated_unique') == unique_count and ut.get('application_acks') == unique_count
        and ut.get('ignored_valid_acks') == 1 and type(ut.get('retries')) is int and ut['retries'] >= 1
        and ut.get('pending_messages') == 0 and ut.get('pending_bytes') == 0
        and rt.get('replay_offered') == replay_count and rt.get('replay_application_acks') == replay_count
        and isinstance(ws, dict) and ws.get('status') == 'PASS'
        and (not cache_off or cold_ws)
        and (not (cold_subset or (durable_subset and cold_genuinely_proven))
             or (cold_valid and isinstance(ws.get('cold-replay'), dict)
                 and ws['cold-replay'] == {'submissions': 0, 'new_events': 0, 'deliveries_enqueued': 0,
                     'telemetry_writes_completed': 0, 'drops': 0}))
        and all(isinstance(row, dict) and row.get('validation_valid') is True for row in (unique, replay)))


def paper_p8_e4_cold_restart_status(evidence, cleanup_verified, expected_counts=None):
    """A cold restart is PASS only when the finalized E4 cold subset validates."""
    if (not isinstance(evidence, dict)
            or evidence.get('workload_status') != 'COLD_REPLAY_SUBSET_VALIDATED'
            or evidence.get('lane_marker') != 'PAPER_P8_E4_COLD_REPLAY_SUBSET'
            or evidence.get('coldrestart') != 'PASS'
            or not paper_p8_e4_warm_subset_acceptance(evidence, cleanup_verified, expected_counts)):
        return 'FAIL'
    return 'PASS'


def paper_p8_e4_preparation_metadata(phase_run_ids):
    """Summarize telemetry phases and total manifest count including the viewer."""
    if not isinstance(phase_run_ids, dict) or not phase_run_ids:
        raise ValueError('p8_e4_phase_run_ids_rejected')
    phases = list(phase_run_ids)
    if phases not in (['unique', 'replay'], ['unique', 'replay', 'cold-replay'],
                      ['unique', 'replay', 'expiry-replay']):
        raise ValueError('p8_e4_phase_run_ids_rejected')
    return {'phase_inputs': phases, 'manifest_count': len(phases) + 1}


def paper_p8_e4_artifact_name(fixture):
    return f"p8-e4-{fixture.paper_p8_e4}.json"


def paper_p8_e5_acceptance(report, evidence, cleanup_verified):
    """Require complete E5 workload proof and registry-owned cleanup; never P8 acceptance."""
    phases = report.get('phases') if isinstance(report, dict) else None
    initial = phases.get('initial', {}) if isinstance(phases, dict) else {}
    warm = phases.get('warm_replay', {}) if isinstance(phases, dict) else {}
    delta = initial.get('database_delta', {})
    commands = report.get('control_reconciliation', {}) if isinstance(report, dict) else {}
    accounting = report.get('accounting', {}) if isinstance(report, dict) else {}
    validation = report.get('validation', {}) if isinstance(report, dict) else {}
    raw = report.get('raw_verification', []) if isinstance(report, dict) else []
    viewer = report.get('viewer', {}) if isinstance(report, dict) else {}
    metadata = report.get('metadata', {}) if isinstance(report, dict) else {}
    workload = metadata.get('workload', {}) if isinstance(metadata, dict) else {}
    schedule = workload.get('schedule', {}) if isinstance(workload, dict) else {}
    telemetry = workload.get('telemetry', {}) if isinstance(workload, dict) else {}
    initial_count = schedule.get('initial_imported_messages', telemetry.get('messages', 20))
    warm_count = schedule.get('warm_replay_prefix', 10)
    samples_per_message = telemetry.get('samples_per_message', 100)
    return (isinstance(report, dict) and isinstance(evidence, dict)
        and all(type(value) is int and value > 0
                for value in (initial_count, warm_count, samples_per_message))
        and report.get('status') in ('WORKLOAD_COMPLETE_PENDING_CLEANUP', 'PASS')
        and report.get('workload_status', report.get('status')) == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
        and report.get('arm') == evidence.get('arm')
        and validation.get('valid') is True and validation.get('failures') == []
        and report.get('full_p8_accepted') is False and cleanup_verified is True
        and report.get('processes_stopped') is True and report.get('runner_stopped') is True
        and not report.get('cleanup_errors')
        and (initial.get('e4') or {}).get('journal_entry_count') == initial_count
        and (warm.get('e4') or {}).get('journal_entry_count') == warm_count
        and all(delta.get(k) == v for k, v in {'ledger': initial_count,
            'raw': initial_count * samples_per_message, 'temporal': initial_count}.items())
        and accounting.get('imported_input') == initial_count
        and accounting.get('warm_replay') == warm_count
        and len(raw) == 2 and all(row.get('verified_exact') is True for row in raw)
        and viewer.get('ready_servers') == 2 and viewer.get('subscriptions') == 3
        and isinstance(commands, dict) and commands.get('status') == 'PASS'
        and commands.get('verified_receipts') == 4 and commands.get('verified_configurations') == 2
        and accounting.get('command_receipts_verified') == 4
        and accounting.get('command_configurations_verified') == 2
        and isinstance(accounting.get('ingress_delta'), dict)
        and isinstance(report.get('mechanism_metrics'), dict)
        and metadata.get('workload') and metadata.get('effective_flags')
        and report.get('lane_marker') == 'PAPER_P8_E5_ARM_PILOT')


def paper_p8_e5_artifact_name(fixture):
    return f'p8-e5-{fixture.paper_p8_e5}.json'


def paper_p8_hold_callbacks_required(args):
    """Return True when the lifecycle hold must install begin/finish callbacks.

    Every prepare-hold experiment, E5 included, owns a phase collector and an
    importer, so a missing selector here silently degrades the run to a bare
    hold with no collection or import.
    """
    return any(getattr(args, name, False) for name in (
        'p8_e1_prepare_hold', 'p8_e2_prepare_hold', 'p8_e3_prepare_hold',
        'p8_e4_prepare_hold', 'p8_e5_prepare_hold'))


def paper_p8_e5_post_run_status(final_session, evidence):
    """Return ``(lifecycle_ok, workload_outcome)`` for a finished E5 hold session.

    A never-begun preparation stays PREPARATION_ONLY and reports ``NOT_RUN``;
    a partial or unverified run keeps the INCOMPLETE/FAIL label written by its
    artifact and is never rewritten to ``NOT_RUN``.
    """
    if not isinstance(final_session, dict) or not isinstance(evidence, dict):
        raise ValueError('e5_post_run_evidence_rejected')
    history = final_session.get('phase_history') or []
    if evidence.get('status') == 'PREPARATION_ONLY':
        ok = (final_session.get('phase') == 'CLOSED'
              and final_session.get('workload_outcome') == 'NOT_RUN'
              and final_session.get('collector_ready') is not True)
        return ok, final_session.get('workload_outcome', 'NOT_RUN')
    ok = (final_session.get('phase') == 'CLOSED'
          and history == ['initial', 'warm_replay', 'control']
          and final_session.get('workload_outcome') == 'VALIDATED'
          and final_session.get('cleanup_outcome') == 'VERIFIED'
          and evidence.get('cleanup_verified') is True
          and paper_p8_e5_acceptance(evidence, {'arm': evidence.get('arm')},
                                     evidence.get('cleanup_verified') is True))
    return ok, evidence.get('workload_status')


def paper_p8_e5_begin_phase(smoke, fixture, endpoint, registry, containers, prepared, phase):
    """Start one ordered E5 A-side phase: baseline snapshot and bounded collector."""
    import paper_p8_e5_remote as e5_remote
    if not isinstance(phase, str):
        raise ValueError('e5_phase_id_rejected')
    return e5_remote.begin_remote_phase(smoke, fixture, endpoint, registry, containers,
                                        prepared, phase=phase)


def paper_p8_e5_finish_phase(smoke, fixture, endpoint, registry, containers, prepared,
                             results_path, artifacts, phase):
    """Import one copied B phase result, verify it on A, and persist evidence.

    The imported file must be the exact ``incoming/<phase>/results.json`` the
    operator copied into the fixture private tree; nothing named by B is
    executed or opened.
    """
    import paper_p8_e5_import as e5_import
    import paper_p8_e5_remote as e5_remote
    if not isinstance(phase, str):
        raise ValueError('e5_phase_id_rejected')
    incoming = e5_import.phase_incoming_dir(fixture.private, phase)
    expected = (Path(incoming) / 'results.json').resolve()
    supplied = Path(results_path).absolute().resolve(strict=True)
    if supplied != expected:
        raise ValueError('e5_result_path_mismatch')
    imported = e5_import.import_phase_result(prepared, fixture.private, phase=phase)
    result = e5_remote.finish_remote_phase(
        smoke, fixture, endpoint, registry, containers, prepared,
        phase=phase, b_projection=imported['projection'])
    artifacts = Path(artifacts)
    stem = f'p8-e5-{prepared["arm"]}-{phase}'
    generator_csv = e5_import.copy_generator_resources(
        fixture.private, phase, artifacts / f'{stem}-resources-generator.csv')
    paper_p8_write_private_csv(artifacts / f'{stem}-server-metrics.csv',
        ('phase', 'snapshot_json'), ((phase, json.dumps(row, sort_keys=True))
            for row in result.get('collector_metrics') or []))
    paper_p8_write_private_csv(artifacts / f'{stem}-resources.csv',
        ('phase', 'sample_json'), ((phase, json.dumps(row, sort_keys=True))
            for row in result.get('resource_samples') or []))
    summary = {
        'schema_version': 1, 'experiment': 'E5', 'phase': phase,
        'session_id': imported['session_id'], 'run_id': imported['run_id'],
        'arm': prepared['arm'], 'status': 'COMPLETE',
        'b_projection_status': imported['projection'].get('status'),
        'import': {key: imported.get(key) for key in (
            'source_bundle_sha256', 'source_inventory_sha256', 'bound_manifest_sha256',
            'bound_gateway_environment_sha256', 'bound_run_spec_sha256',
            'bound_e5_context_sha256', 'bound_e5_phase_plan_sha256', 'bound_dataset_sha256',
            'binding_sha256', 'results_sha256', 'results_bytes', 'projection_sha256',
            'resource_samples_sha256', 'resources_generator', 'runner_status', 'provenance')},
        'collector': {
            'metrics_snapshots': len(result.get('collector_metrics') or []),
            'resource_samples': len(result.get('resource_samples') or []),
            'generator_samples': len(result.get('collector_generator_samples') or []),
            'errors': list(result.get('collector_errors') or []),
            'generator_csv': generator_csv,
        },
        'result': result,
        'full_p8_accepted': False, 'physical_ab': 'NOT RUN',
    }
    report_path = artifacts / f'{stem}-phase.json'
    durable_json(report_path, summary)
    result['report'] = str(report_path)
    return {
        'phase_id': phase,
        'workload_outcome': 'VALIDATED',
        'validation_valid': True,
        'validation_failures': [],
        'results_sha256': imported['results_sha256'],
        'report': str(report_path),
    }


def paper_p8_e5_finalize_report(smoke, fixture, prepared, final_state, *, cleanup_verified):
    """Build and persist the E5 final artifact without ever claiming acceptance.

    A never-begun preparation keeps its PREPARATION_ONLY artifact. A partial run
    is written as INCOMPLETE/FAIL, never as NOT_RUN.
    """
    import paper_p8_e5_import as e5_import
    import paper_p8_e5_remote as e5_remote
    artifacts = Path(fixture.artifacts)
    artifact_path = artifacts / paper_p8_e5_artifact_name(fixture)
    state = final_state if isinstance(final_state, dict) else {}
    history = list(state.get('phase_history') or [])
    phase_results = prepared.get('phase_results') or {}
    # Executed phases follow the immutable plan order, never alphabetical order,
    # so the artifact stays comparable with the declared phase plan.
    expected = ['initial', 'warm_replay', 'control']
    observed = set(history) | set(phase_results)
    executed = [phase for phase in expected if phase in observed] + sorted(
        observed - set(expected))
    base = {
        'schema_version': 1, 'experiment': 'E5', 'arm': prepared.get('arm'),
        'session_id': prepared.get('session_id'), 'status': 'FAIL',
        'workload_status': 'INCOMPLETE', 'phase_history': history,
        'executed_phases': executed, 'cleanup_verified': bool(cleanup_verified),
        'full_p8_accepted': False, 'physical_ab': 'NOT RUN', 'reason': None,
    }
    if history != expected or set(phase_results) != set(expected):
        base['reason'] = 'e5_incomplete_phase_history'
        durable_json(artifact_path, base)
        return base
    try:
        receipts = {phase: e5_import.read_receipt(fixture.private, phase)
                    for phase in expected}
        control = phase_results['control']
        projection = control.get('b_projection') or {}
        correlation = e5_remote.expected_viewer_correlation(prepared, receipts, projection)
        continuous = e5_remote.validate_continuous_viewer(
            projection.get('viewer'), prepared['context'], phase_results['initial'],
            phase_results['initial'].get('ws_window'),
            phase_results['warm_replay'].get('ws_window'), control.get('ws_window'),
            control.get('admin_after'),
            readiness_projection=projection.get('readiness_projection'),
            lifecycle_proof=projection.get('lifecycle_proof'),
            expected_correlation=correlation)
        declared = e5_remote.build_b_declared(
            smoke, prepared, control, expected_correlation=correlation)
        report = e5_remote.finalize(
            smoke, fixture, prepared, phase_results, continuous,
            b_declared=declared, cleanup_verified=bool(cleanup_verified),
            expected_correlation=correlation)
    except (ValueError, OSError, KeyError) as exc:
        base['reason'] = str(exc.args[0])[:120] if exc.args else 'e5_finalize_failed'
        durable_json(artifact_path, base)
        return base
    report['phase_history'] = history
    report['executed_phases'] = executed
    report['cleanup_outcome'] = state.get('cleanup_outcome')
    durable_json(artifact_path, report)
    return report


def paper_p8_e5_post_cleanup_artifact(fixture, *, p8_path, staged_e5, remote_outcome, failure,
                                      cleanup_verified, cleanup_counts):
    """Resolve, merge and persist the E5 artifact during post-cleanup in startup_only."""
    staged_path = Path(staged_e5)
    artifact_path = Path(p8_path)
    if staged_path.exists():
        p8 = json.loads(staged_path.read_text())
    elif artifact_path.exists():
        p8 = json.loads(artifact_path.read_text())
    else:
        p8 = {}

    is_prepare_only = bool(getattr(fixture, 'paper_p8_prepare_only', False))

    if not p8.get('workload_status'):
        p8['workload_status'] = p8.get('status', remote_outcome if is_prepare_only else 'NOT_RUN')
    p8['lane_marker'] = 'PAPER_P8_E5_ARM_PILOT'
    p8['full_p8_accepted'] = False
    p8['exact_cleanup_verified'] = cleanup_verified
    p8['cleanup_counts'] = cleanup_counts

    p8.update(cleanup_verified=cleanup_verified, exact_cleanup_verified=cleanup_verified,
              cleanup_counts=cleanup_counts)
    if is_prepare_only and (p8.get('status') == 'PREPARATION_ONLY' or remote_outcome == 'NOT_RUN'):
        p8['status'] = 'PREPARATION_ONLY' if (not failure and cleanup_verified and remote_outcome == 'NOT_RUN' and p8.get('status') == 'PREPARATION_ONLY') else 'FAIL'
    else:
        arm = p8.get('arm') or getattr(fixture, 'paper_p8_e5', None)
        p8_accepted = bool(not failure and cleanup_verified
                           and paper_p8_e5_acceptance(p8, {'arm': arm}, cleanup_verified))
        p8['status'] = 'PASS' if p8_accepted else 'FAIL'

    durable_json(artifact_path, p8)
    return p8


def paper_p8_e3_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    expected_count = getattr(fixture, 'paper_p8_e3_command_count', 4)
    validation = evidence.get('validation') if isinstance(evidence, dict) else None
    accounting = evidence.get('accounting') if isinstance(evidence, dict) else None
    control = accounting.get('control') if isinstance(accounting, dict) else None
    reconciliation = evidence.get('control_reconciliation') if isinstance(evidence, dict) else None
    receipts = reconciliation.get('receipt_verification') if isinstance(reconciliation, dict) else None
    configurations = reconciliation.get('rest_configuration') if isinstance(reconciliation, dict) else None
    executors = reconciliation.get('sqlite_executors') if isinstance(reconciliation, dict) else None
    raw = evidence.get('raw_verification') if isinstance(evidence, dict) else None
    timeline_window = evidence.get('control_timeline_window') if isinstance(evidence, dict) else None
    probes = evidence.get('representative_probes') if isinstance(evidence, dict) else None
    probe_operations = probes.get('operations') if isinstance(probes, dict) else None
    expected_probes = {
        'offline_admission': 202,
        'same_key_replay': 202,
        'busy': 409,
        'stale': 409,
        'online_fresh_apply': 202,
        'noop': 200,
        'noop_replay': 200,
    }
    probes_valid = (
        isinstance(probes, dict) and probes.get('valid') is True and probes.get('failures') == []
        and isinstance(probe_operations, list) and len(probe_operations) == len(expected_probes)
        and {row.get('kind') for row in probe_operations if isinstance(row, dict)} == set(expected_probes)
        and all(isinstance(row, dict) and row.get('expected') == expected_probes.get(row.get('kind'))
                and row.get('actual') == expected_probes.get(row.get('kind'))
                and row.get('match') is True for row in probe_operations)
        and probes.get('auxiliary_command_count') == 1
        and probes.get('receipt_count') == 2
        and probes.get('outside_fresh_command_measurement') is True
        and isinstance(probes.get('offline_qualification'), dict)
        and probes['offline_qualification'].get('executor_running') is False
        and probes['offline_qualification'].get('receipt_verified') is True
        and probes['offline_qualification'].get('terminal_status') in ('pending', 'published')
        and probes['offline_qualification'].get('outbox_status') in ('pending', 'published')
        and probes['offline_qualification'].get('same_key_replay_bytes_match') is True
        and probes['offline_qualification'].get('delivery_guarantee') == 'none'
    )
    return (isinstance(evidence, dict)
            and evidence.get('status') == 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
            and evidence.get('arm') == getattr(fixture, 'paper_p8_e3', None)
            and evidence.get('workload', 'mixed') == getattr(fixture, 'paper_p8_e3_workload', 'mixed')
            and evidence.get('full_p8_accepted') is False
            and isinstance(validation, dict) and validation.get('valid') is True
            and validation.get('failures') == []
            and evidence.get('runner_stopped') is True
            and isinstance(control, dict) and control.get('planned') == expected_count
            and control.get('sent') == expected_count and control.get('pending') == 0
            and isinstance(receipts, dict) and receipts.get('verified') is True
            and isinstance(receipts.get('verified_idempotency_keys'), list)
            and len(receipts['verified_idempotency_keys']) == expected_count
            and isinstance(timeline_window, dict)
            and timeline_window.get('availability') == 'available'
            and isinstance(timeline_window.get('histograms'), dict)
            and timeline_window['histograms'].get('d_dispatch', {}).get('count') == expected_count
            and timeline_window['histograms'].get('d_e2e_succeeded', {}).get('count') == expected_count
            and timeline_window['histograms'].get('pending_origins') == 0
            and timeline_window['histograms'].get('missing_origin') == 0
            and timeline_window['histograms'].get('missing_dispatch') == 0
            and timeline_window['histograms'].get('overflow') == 0
            and isinstance(configurations, dict) and len(configurations) == 2
            and all(isinstance(value, dict) for value in configurations.values())
            and isinstance(executors, list) and len(executors) == 2
            and isinstance(raw, list) and len(raw) == 2
            and all(isinstance(row, dict) and row.get('verified_exact') is True for row in raw)
            and probes_valid
            and cleanup_verified is True
            and paper_p4_cleanup_acceptance(fixture, {'full_p4_accepted': True},
                                            cleanup_counts, cleanup_verified, registry))


def paper_p8_e3_artifact_name(fixture):
    low_load = getattr(fixture, 'paper_p8_e3_workload', 'mixed') == 'low_load'
    return f"p8-e3-{fixture.paper_p8_e3}{'-low-load' if low_load else ''}.json"


def paper_p8_e3_selector(args):
    if args.p8_e3_prepare_hold:
        return args.p8_arm
    return args.paper_p8_e3


def paper_p8_e3_lifecycle_outcome_accepted(state):
    """Only preparation-only closure or imported, validated workload may exit zero."""
    if not isinstance(state, dict) or state.get('phase') != 'CLOSED':
        return False
    outcome = state.get('workload_outcome')
    if outcome == 'NOT_RUN':
        return state.get('collector_ready') is not True and state.get('begun_at') is None
    imported = state.get('imported_result')
    return outcome == 'VALIDATED' and isinstance(imported, dict) and imported.get('validation_valid') is True


def paper_p7_cleanup_acceptance(fixture, evidence, cleanup_counts, cleanup_verified, registry):
    """P7 handshake acceptance requires verified exact cleanup and complete handshake evidence."""
    if not cleanup_verified:
        return False
    items = list(registry.items)
    kinds = ('container', 'network', 'volume')
    if not items:
        return False
    for item in items:
        receipt = item.get('receipt')
        if (item.get('kind') not in kinds or item.get('run') != fixture.project
                or item.get('task') != 'native-compose-allocation'
                or item.get('allocation_attempted') is not True or item.get('removed') is not True
                or not isinstance(receipt, dict) or set(receipt) != {'id', 'created', 'labels'}
                or not isinstance(receipt.get('created'), str) or not receipt['created']
                or not isinstance(receipt.get('labels'), dict)
                or any(receipt['labels'].get(key) != value for key, value in item.get('labels', {}).items())):
            return False
        identity = receipt.get('id')
        if item['kind'] == 'volume':
            if identity != item.get('name'):
                return False
        elif not isinstance(identity, str) or not re.fullmatch(r'[a-f0-9]{64}', identity):
            return False
    registered = {kind: sum(item.get('kind') == kind for item in items) for kind in kinds}
    removed = {kind: sum(item.get('kind') == kind and item.get('removed') is True for item in items) for kind in kinds}
    if cleanup_counts != removed or removed != registered:
        return False

    if fixture.paper_p7_mode == 'p6_ws':
        p6ws = evidence.get('paper_p6_ws', {}) if isinstance(evidence, dict) else {}
        return (p6ws.get('status') == 'PASS' and p6ws.get('mode') == 'paper_p7_p6_ws'
                and p6ws.get('full_p7_accepted') is False
                and p6ws.get('exact_cleanup_verified') is False)
    p7 = evidence.get('paper_p7', {}) if isinstance(evidence, dict) else {}
    if p7.get('status') != 'PASS':
        return False
    if fixture.paper_p7_mode == 'single_path':
        p7['exact_cleanup_verified'] = cleanup_verified is True
        return paper_p7_support().single_path_result_ok(p7)
    if fixture.paper_p7_mode == 'telemetry_pilot':
        p7['exact_cleanup_verified'] = cleanup_verified is True
        return paper_p7_support().telemetry_pilot_result_ok(p7)
    if fixture.paper_p7_mode == 'reconnect':
        p7['exact_cleanup_verified'] = cleanup_verified is True
        return paper_p7_support().reconnect_result_ok(p7)
    if fixture.paper_p7_mode == 'lifecycle':
        return paper_p7_support().finalize_lifecycle_cleanup(p7, cleanup_verified)
    if fixture.paper_p7_mode == 'slow_viewer':
        p7['precleanup_proof_path'] = str(fixture.artifacts / 'p7-slow-viewer-proof.json')
        return paper_p7_support().finalize_slow_viewer_cleanup(p7, cleanup_verified)
    if fixture.paper_p7_mode in ('overhead_zero', 'overhead_two'):
        p7['exact_cleanup_verified'] = cleanup_verified is True
        return paper_p7_support().overhead_result_ok(p7)
    if p7.get('mode') != 'paper_p7_handshake_only':
        return False
    perimeter = p7.get('perimeter', {})
    if perimeter.get('status') != 'PASS' or not perimeter.get('https_configured') or not perimeter.get('auth_request_enabled'):
        return False
    handshake = p7.get('handshake', {})
    if handshake.get('status') != 'PASS':
        return False
    native = handshake.get('native', {})
    if native.get('http_status') != 101 or native.get('subprotocol') != 'iot.telemetry.v1':
        return False
    browser = handshake.get('browser', {})
    if browser.get('status') != 'PASS' or browser.get('event') != 'open' or browser.get('selected_protocol') != 'iot.telemetry.v1':
        return False
    negatives = handshake.get('negative', {})
    required_negatives = {'missing_credentials', 'bad_token', 'conflicting_credentials',
                          'denied_origin', 'missing_app_protocol', 'expired_jwt',
                          'rest_subprotocol_only', 'browser_denied_origin'}
    if not required_negatives.issubset(set(negatives.keys())):
        return False
    if any(neg.get('status') != 'PASS' for neg in negatives.values()):
        return False
    leak_check = handshake.get('leak_check', {})
    if leak_check.get('status') != 'PASS' or leak_check.get('leaks_found_count') != 0:
        return False
    perimeter = handshake.get('perimeter_isolation', {})
    if perimeter.get('status') != 'PASS':
        return False
    return True


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
                    'cpu_limit_nanocpus': limits.get(service, {}).get('NanoCpus'),
                    'memory_limit_bytes': limits.get(service, {}).get('Memory')})


def paper_p1_subscription_target(fixture, spec, password, tag, *, kind=None):
    # subscribeLiteral permits these exact existing filters, not arbitrary
    # narrower filters. Never broaden either principal's broker policy.
    selection = tag if kind is None else kind
    if selection == 'telemetry':
        return fixture.values['MQTT_USERNAME'], fixture.values['MQTT_PASSWORD'], 'gateways/+/telemetry/#'
    if selection == 'ack':
        return spec.gateway_id, password, 'gateways/' + spec.gateway_id + '/acks/#'
    raise ValueError('paper_p1_subscription_failed')


def paper_p1_suback_ready(data):
    match = re.search(rb'Subscribed \(mid:\s*\d+\):\s*(\d+)', data)
    if b'received SUBACK' not in data or match is None:
        return False
    if int(match[1]) != 1:
        raise ValueError('paper_p1_subscription_failed')
    return True


def paper_p1_subscription_command(broker, tag, topic, *, message_count, message_timeout):
    client_id = 'p1-observer-' + tag + '-' + str(os.getpid())
    script = ('stty -echo; printf \'OBSERVER_READY\\n\'; '
              'IFS= read -r user; IFS= read -r password; '
              'mosquitto_sub -d -h mosquitto -p 8883 '
              '--cafile /mosquitto/config/certs/ca.crt -u "$user" -P "$password" '
              '-i ' + client_id + ' -q 1 -t ' + topic + ' -C ' + str(message_count) +
              ' -W ' + str(message_timeout))
    return ['docker', 'exec', '-it', broker, 'sh', '-ec', script]


def paper_p1_open_echo_disabled_pty():
    import pty
    import termios
    master_fd, slave_fd = pty.openpty()
    try:
        terminal = termios.tcgetattr(slave_fd)
        terminal[3] &= ~termios.ECHO
        termios.tcsetattr(slave_fd, termios.TCSANOW, terminal)
        return master_fd, slave_fd
    except BaseException:
        os.close(master_fd)
        os.close(slave_fd)
        raise


def paper_p1_send_credentials_after_ready(proc, path, master_fd, username, password,
                                         *, timeout=5, max_bytes=131072):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        data = path.read_bytes()
        if len(data) > max_bytes:
            raise ValueError('paper_p1_subscription_failed')
        if b'OBSERVER_READY' in data:
            view = memoryview((username + '\n' + password + '\n').encode())
            while view:
                view = view[os.write(master_fd, view):]
            return
        if proc.poll() is not None:
            break
        time.sleep(0.05)
    raise ValueError('paper_p1_subscription_failed')


def paper_p1_subscriber(fixture, registry, broker, spec, password, tag, *, message_count=1,
                        message_timeout=60, subscription_kind=None):
    """Use the existing broker CLI, TLS/DynSec and private bounded debug output.

    A pseudo-tty makes CLI debug lines line-buffered so SUBACK can be observed
    before publishing. Credential input is sent only after terminal echo is
    disabled; each observer gets a distinct client ID from the ingress client.
    No new port, resource registry or MQTT implementation.
    """
    username, subscription_password, topic = paper_p1_subscription_target(
        fixture, spec, password, tag, kind=subscription_kind)
    path = fixture.private / ('p1-sub-' + tag + '.log')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    stream = os.fdopen(fd, 'wb')
    if type(message_count) is not int or not 1 <= message_count <= 100:
        stream.close()
        raise ValueError('paper_p1_subscription_failed')
    if type(message_timeout) is not int or not 1 <= message_timeout <= 90:
        stream.close()
        raise ValueError('paper_p1_subscription_failed')
    command = paper_p1_subscription_command(broker, tag, topic,
                                             message_count=message_count,
                                             message_timeout=message_timeout)
    master_fd, slave_fd = paper_p1_open_echo_disabled_pty()
    try:
        proc = subprocess.Popen(command, stdin=slave_fd, stdout=stream, stderr=subprocess.STDOUT,
                                env=process_env(fixture))
    except BaseException:
        os.close(master_fd)
        os.close(slave_fd)
        stream.close()
        raise
    os.close(slave_fd)
    try:
        paper_p1_send_credentials_after_ready(proc, path, master_fd, username, subscription_password)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            data = path.read_bytes()
            if paper_p1_suback_ready(data):
                return proc, stream, path, master_fd
            if proc.poll() is not None or len(data) > 131072:
                break
            time.sleep(0.05)
        raise ValueError('paper_p1_subscription_failed')
    except BaseException:
        paper_p1_stop_subscription((proc, stream, path, master_fd))
        raise


def paper_p1_subscription_messages(data):
    """Extract bounded payload/topic pairs from the existing mosquitto_sub CLI log."""
    messages = []
    topic = None
    for line in data.splitlines():
        match = re.search(rb'received PUBLISH .*\'([^\']+)\'', line)
        if match:
            topic = match.group(1).decode('utf-8', errors='strict')
        elif topic is not None and line.lstrip().startswith(b'{'):
            payload = line.lstrip()
            messages.append((topic, payload))
            topic = None
            if len(messages) > 100:
                raise ValueError('paper_p1_subscription_failed')
    return messages


def paper_p1_capture_subscription(subscription, expected_topic, *, timeout=5, max_bytes=131072,
                                  after_count=0):
    if type(after_count) is not int or after_count < 0:
        raise ValueError('paper_p1_subscription_failed')
    proc, _, path = subscription[:3]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.stat().st_size > max_bytes:
            raise ValueError('paper_p1_subscription_failed')
        messages = paper_p1_subscription_messages(path.read_bytes())
        matching_ordinal = 0
        for topic, payload in messages:
            if topic == expected_topic:
                ordinal = matching_ordinal
                matching_ordinal += 1
                if ordinal < after_count:
                    continue
                if not payload or len(payload) > 65536:
                    raise ValueError('paper_p1_subscription_failed')
                return payload
        if proc.poll() is not None:
            return None
        time.sleep(0.05)
    return None


def paper_p1_subscription_match_count(subscription, expected_topic, *, max_bytes=131072):
    """Return the current matching-PUBLISH ordinal boundary for one subscription."""
    return sum(topic == expected_topic for topic, _ in
               paper_p1_subscription_log_messages(subscription, max_bytes=max_bytes))


def paper_p1_subscription_log_messages(subscription, *, max_bytes=131072):
    _, _, path = subscription[:3]
    if path.stat().st_size > max_bytes:
        raise ValueError('paper_p1_subscription_failed')
    return paper_p1_subscription_messages(path.read_bytes())


def paper_p1_stop_subscription(subscription):
    proc, stream, _, *terminal = subscription
    try:
        if proc.poll() is None:
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    finally:
        if proc.stdin is not None and not proc.stdin.closed:
            try:
                proc.stdin.close()
            except BrokenPipeError:
                pass
        if not stream.closed:
            stream.close()
        for fd in terminal:
            try:
                os.close(fd)
            except OSError:
                pass


def paper_p1_read_subscription(subscription):
    proc, stream, path = subscription[:3]
    try:
        proc.wait(timeout=8)
        if proc.returncode or path.stat().st_size > 131072:
            raise ValueError('paper_p1_subscription_failed')
        payloads = [payload for _, payload in paper_p1_subscription_messages(path.read_bytes())]
        if len(payloads) != 1:
            raise ValueError('paper_p1_subscription_failed')
        return payloads[0]
    finally:
        paper_p1_stop_subscription(subscription)


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
            if subscription:
                paper_p1_stop_subscription(subscription)
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
        paper_p1_stop_subscription(subscription)
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


def paper_p8_bind_initialized_session_deadline(session, prepared=None):
    """Copy the supervisor-owned deadline after initialization, never from cancellation state."""
    deadline = session['state'].get('deadline_monotonic')
    session['session_deadline_monotonic'] = deadline
    if prepared is not None:
        prepared['session_deadline_monotonic'] = deadline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", type=Path, required=True, help="New private fixture directory, outside artifacts")
    parser.add_argument('--startup-only', action='store_true', help='Real phased stack startup; HTTP acceptance remains closed')
    parser.add_argument('--p8-lifecycle-hold', action='store_true',
                        help='P8 foreground hold; isolated E1-E4 preparation paths')
    parser.add_argument('--p8-e1-prepare-hold', action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('--p8-e1-bundle-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e1-viewers', type=int, default=64, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e1-principals', type=int, default=2, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e1-density', choices=('mixed','sparse','dense','overlap'), default='mixed', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e1-messages-per-target', type=int, default=5, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-prepare-hold', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-bundle-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-samples-per-message', choices=('100', '1'), help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-workload', choices=('steady_burst', 'equal_samples'), default='steady_burst', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-offered-rate', type=int, choices=(100, 1000, 5000, 20000), default=100, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-duration-seconds', type=int, default=10, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-burst-messages', type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-pending-messages', type=int, default=64, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-pending-bytes', type=int, default=1 << 20, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-retry-mode', choices=('bounded_backoff_v1',), help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-retry-initial-interval-ms', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-retry-max-interval-ms', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-retry-jitter-percent', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-retry-rate-per-second', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-retry-burst', type=int, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-context', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e2-reference', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e3-prepare-hold', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e3-bundle-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e3-workload', choices=('low_load', 'mixed'), help=argparse.SUPPRESS)
    parser.add_argument('--p8-e3-command-count', type=int, default=4, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e3-schedule', choices=('smoke', 'steady'), default='smoke', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-prepare-hold', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-bundle-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-cache-mode', choices=('on', 'off'), default='on', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-unique-count', type=int, default=20, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-replay-count', type=int, default=10, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-cold-replay', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-expiry-replay', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e4-durable-reopen', action='store_true',
                        help='include sequential durable Gateway SQLite fault-stop/reopen phases after base E4 phases')
    parser.add_argument('--p8-e4-conflicts', action='store_true',
                        help='include representative conflict rejection phase after base/durable phases')
    parser.add_argument('--p8-e5-prepare-hold', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-e5-bundle-root', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e5-context', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-e5-reference', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--p8-experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    parser.add_argument('--p8-arm', default='lifecycle')
    parser.add_argument('--p8-seed', type=int, default=0)
    parser.add_argument('--p8-profile', choices=('pilot', 'official'), default='pilot', help=argparse.SUPPRESS)
    parser.add_argument('--p8-official-opt-in', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--p8-deadline-seconds', type=float, default=900)
    parser.add_argument('--p8-remote-config', type=Path,
                        help='Strict opt-in private IPv4 LAN profile JSON for isolated P8 server startup')
    parser.add_argument('--positive-http-only', action='store_true', help='Phased stack startup and positive HTTP login/admin/owner sensor test only')
    parser.add_argument('--role-http-only', action='store_true', help='Phased startup, positive slice, and bounded role denials; JWT acceptance remains false')
    parser.add_argument('--full-http', action='store_true', help='Phased startup, positive proofs, role and JWT denials, and exact cleanup acceptance')
    parser.add_argument('--paper-p0', action='store_true', help='Isolated P0 quota, Storage, Auth/API and two-Gateway uplink smoke')
    parser.add_argument('--paper-p1', action='store_true', help='Bounded metrics and artifact workload, same isolated lifecycle as P0')
    parser.add_argument('--paper-p2-spike', action='store_true', help='P2.1 compatibility only; not P2 acceptance')
    parser.add_argument('--paper-p2', action='store_true', help='P2 isolated HTTPS/Unix perimeter checks')
    parser.add_argument('--paper-p7', action='store_true', help='P7 isolated HTTPS/Auth topology; P7 probes only')
    parser.add_argument('--paper-p7-mode', choices=('handshake_only', 'telemetry_pilot', 'single_path', 'reconnect', 'lifecycle', 'slow_viewer', 'overhead_zero', 'overhead_two', 'p6_ws'),
                         help='P7 lane; default handshake_only preserves historical behavior; single_path runs microbatch OFF; reconnect runs ReconnectCatchupClient')
    parser.add_argument('--paper-p8-sessions', action='store_true', help='Run the bounded native 64-session WSS prerequisite lane')
    parser.add_argument('--paper-p8-slow-viewer', action='store_true', help='Run the finite three-peer P8 slow-viewer lane')
    parser.add_argument('--paper-p8-load', action='store_true', help='Run only the local E2 telemetry100 20-batch paperload smoke')
    parser.add_argument('--paper-p8-e1', action='store_true', help='Run the bounded P8 E1 WebSocket + MQTT pilot workload')
    parser.add_argument('--paper-p8-e2', action='store_true', help='Run the bounded P8 E2 shaper + microbatch telemetry pilot')
    parser.add_argument('--paper-p8-e2-workload', choices=('steady_burst', 'equal_samples'), default='steady_burst', help=argparse.SUPPRESS)
    parser.add_argument('--paper-p8-e2-context', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--paper-p8-e2-reference', type=Path, help=argparse.SUPPRESS)
    parser.add_argument('--paper-p8-e3', choices=('polling', 'wakeup'), help='Run the bounded P8 E3 fresh-command pilot arm')
    parser.add_argument('--paper-p8-e3-low-load', action='store_true', help='Use the bounded low-load E3 profile; requires --paper-p8-e3')
    parser.add_argument('--paper-p8-e3-command-count', type=int, choices=range(1, 31), default=4)
    parser.add_argument('--paper-p8-e3-schedule', choices=('smoke', 'steady'), default='smoke')
    parser.add_argument('--paper-p8-e4', choices=('on', 'off'), help='Run the bounded P8 E4 warm RAM-dedup pilot arm')
    parser.add_argument('--paper-p8-e5', choices=('optimized', 'shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling', 'fanout_scan'), help='Run one bounded E5 native mechanism arm')
    parser.add_argument('--paper-p8-e5-context', type=Path, help='Private repeat context JSON required by E5')
    parser.add_argument('--paper-p8-e5-reference', type=Path, help='Successful optimized E5 artifact required for immutable ablation image reuse')
    parser.add_argument('--paper-p8-recovery', action='store_true', help='Run the bounded native P8.13 recovery fixture using a fresh E5 optimized input context')
    parser.add_argument('--paper-p3', action='store_true', help='P3 isolated Sensor control loop using the P2 HTTPS profile')
    parser.add_argument('--p5a-control-regression', action='store_true',
                        help='With --paper-p3, opt in to microbatch and run the focused command/result preservation regression')
    parser.add_argument('--p5b-control-regression', action='store_true',
                        help='With --paper-p3 --p5a-control-regression, enable schema-24 RAM dedup during the P3 regression')
    parser.add_argument('--paper-p4', action='store_true', help='P4 isolated finite two-Gateway ingress workload')
    parser.add_argument('--paper-p5a', action='store_true', help='Run the finite isolated P5a MQTT/Gateway workload')
    parser.add_argument('--paper-p5a-shutdown-smoke', action='store_true',
                        help='Add one isolated on/1 partial-group SIGTERM drain smoke')
    parser.add_argument('--paper-p5b-cache-smoke', action='store_true',
                        help='Opt in to schema-24 P5b cache profile on the existing P5a fixture (on/1 only)')
    parser.add_argument('--paper-p5b', action='store_true',
                        help='Select the comparable schema-24 P5b RAM-cache workload profile')
    parser.add_argument('--paper-p6', action='store_true',
                        help='Prepare a comparable schema-24 P6 profile and workload specification; no workload execution')
    parser.add_argument('--p6-recovery-smoke', action='store_true',
                        help='Run one isolated P6 backend restart and broker reconnect recovery scenario, not a load arm')
    parser.add_argument('--cache-mode', choices=('off', 'on'),
                        help='P5b RAM-cache mode; independent of the fixed microbatch-on setting')
    parser.add_argument('--mode', choices=('baseline', 'optimized', 'off', 'on'))
    parser.add_argument('--dataset', choices=('100', '1'))
    parser.add_argument('--dispatch-mode', choices=('polling', 'wake-up'),
                        help='P6 dispatch arm; only valid with --paper-p6')
    parser.add_argument('--metrics-off', action='store_true', help='P1 overhead control: no new instrumentation or scraper')
    args = parser.parse_args()
    remote_profile = None
    if args.p8_remote_config is not None:
        if not args.p8_lifecycle_hold:
            parser.error('--p8-remote-config requires --p8-lifecycle-hold')
        try:
            remote_profile = paper_p8_remote_support().load_config(args.p8_remote_config)
            paper_p8_remote_support().preflight_bind(remote_profile)
        except ValueError as exc:
            parser.error(str(exc))
    if args.p8_lifecycle_hold:
        if any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2, args.paper_p7,
                args.paper_p3, args.paper_p4, args.paper_p5a, args.paper_p5b, args.paper_p6,
                args.paper_p8_sessions, args.paper_p8_slow_viewer, args.paper_p8_load, args.paper_p8_e1,
                args.paper_p8_e2, args.paper_p8_e3, args.paper_p8_e4, args.paper_p8_e5,
                args.paper_p8_recovery, args.paper_p8_e3_low_load,
                args.startup_only, args.positive_http_only, args.role_http_only,
                args.full_http, args.metrics_off, args.mode, args.dataset, args.cache_mode,
                args.dispatch_mode, args.paper_p7_mode, args.paper_p5a_shutdown_smoke,
                args.paper_p5b_cache_smoke, args.p6_recovery_smoke, args.p5a_control_regression,
                args.p5b_control_regression)):
            parser.error('--p8-lifecycle-hold cannot be combined with another fixture lane')
        if args.p8_e1_prepare_hold and (args.p8_experiment != 'E1' or args.p8_e1_bundle_root is None):
            parser.error('--p8-e1-prepare-hold requires E1 and --p8-e1-bundle-root')
        if args.p8_e1_prepare_hold and (args.p8_profile == 'official' and not args.p8_official_opt_in):
            parser.error('official E1 requires --p8-official-opt-in')
        if args.p8_profile == 'official' and not args.p8_official_opt_in:
            parser.error('official P8 profile requires explicit --p8-official-opt-in')
        if (args.p8_profile != 'pilot' or args.p8_official_opt_in) and not any((
                args.p8_e1_prepare_hold, args.p8_e2_prepare_hold, args.p8_e3_prepare_hold,
                args.p8_e4_prepare_hold, args.p8_e5_prepare_hold)):
            parser.error('P8 profile options require a P8 prepare-hold lane')
        if args.p8_e2_prepare_hold:
            if args.p8_experiment != 'E2' or args.p8_e2_bundle_root is None or args.p8_e2_samples_per_message is None:
                parser.error('--p8-e2-prepare-hold requires E2, --p8-e2-bundle-root and an explicit sample profile')
            if args.p8_e2_workload == 'steady_burst':
                if args.p8_e2_context is not None or args.p8_e2_reference is not None:
                    parser.error('--p8-e2-prepare-hold steady_burst does not accept context or reference')
            elif args.p8_e2_workload == 'equal_samples':
                if args.p8_e2_context is None:
                    parser.error('--p8-e2-prepare-hold equal_samples requires --p8-e2-context')
        if args.p8_e3_prepare_hold and (args.p8_experiment != 'E3' or args.p8_e3_bundle_root is None
                or args.p8_arm not in ('polling', 'wakeup') or args.p8_e3_workload is None):
            parser.error('--p8-e3-prepare-hold requires E3, dispatch arm, workload and bundle root')
        if args.p8_e3_prepare_hold and (not 1 <= args.p8_e3_command_count <= 30
                or args.p8_e3_schedule not in ('smoke', 'steady')
                or (args.p8_e3_schedule == 'smoke' and args.p8_e3_command_count != 4)):
            parser.error('E3 command count/schedule is invalid')
        if args.p8_e4_prepare_hold and (args.p8_experiment != 'E4' or args.p8_e4_bundle_root is None
                or args.p8_arm != 'warm' or args.p8_remote_config is None):
            parser.error('--p8-e4-prepare-hold requires E4 warm arm, remote config and bundle root')
        if args.p8_e4_cold_replay and (not args.p8_e4_prepare_hold
                or args.p8_e4_cache_mode != 'on' or args.p8_experiment != 'E4'):
            parser.error('--p8-e4-cold-replay requires E4 prepare hold and cache mode on')
        if args.p8_e4_expiry_replay and (not args.p8_e4_prepare_hold
                or args.p8_e4_cache_mode != 'on' or args.p8_e4_cold_replay
                or args.p8_e4_durable_reopen or args.p8_e4_conflicts):
            parser.error('--p8-e4-expiry-replay requires E4 prepare hold/cache on and excludes cold, durable, and conflicts')
        if args.p8_e4_durable_reopen and (not args.p8_e4_prepare_hold or args.p8_experiment != 'E4'):
            parser.error('--p8-e4-durable-reopen requires E4 prepare hold')
        if args.p8_e4_conflicts and (not args.p8_e4_prepare_hold or args.p8_experiment != 'E4'):
            parser.error('--p8-e4-conflicts requires E4 prepare hold')
        if args.p8_e5_prepare_hold and (args.p8_experiment != 'E5' or args.p8_e5_bundle_root is None
                or args.p8_arm not in ('optimized', 'shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling', 'fanout_scan')
                or args.p8_e5_context is None or args.p8_remote_config is None
                or (args.p8_arm != 'optimized' and args.p8_e5_reference is None)
                or (args.p8_arm == 'optimized' and args.p8_e5_reference is not None)):
            parser.error('--p8-e5-prepare-hold requires E5, valid arm, remote config, context, and reference for ablation only')
        if sum((args.p8_e1_prepare_hold, args.p8_e2_prepare_hold,
                args.p8_e3_prepare_hold, args.p8_e4_prepare_hold,
                args.p8_e5_prepare_hold)) > 1:
            parser.error('only one P8 experiment may use lifecycle hold')
        if (not math.isfinite(args.p8_deadline_seconds) or not 1 <= args.p8_deadline_seconds <= 86400
                or not -(2**31) <= args.p8_seed < 2**31
                or not re.fullmatch(r'[A-Za-z0-9_-]{1,32}', args.p8_arm)):
            parser.error('invalid P8 lifecycle arm/deadline')
    elif (args.p8_e1_prepare_hold or args.p8_e1_bundle_root is not None
          or args.p8_e2_prepare_hold or args.p8_e2_bundle_root is not None
          or args.p8_e2_samples_per_message is not None or args.p8_e2_workload != 'steady_burst'
          or args.p8_e2_context is not None or args.p8_e2_reference is not None
          or args.p8_e3_prepare_hold or args.p8_e3_bundle_root is not None or args.p8_e3_workload is not None
          or args.p8_e4_prepare_hold or args.p8_e4_bundle_root is not None
          or args.p8_e5_prepare_hold or args.p8_e5_bundle_root is not None
          or args.p8_e5_context is not None or args.p8_e5_reference is not None):
        parser.error('P8 E1/E2/E3/E4/E5 prepare hold requires --p8-lifecycle-hold')
    elif (args.p8_remote_config is not None or args.p8_experiment != 'E1' or args.p8_arm != 'lifecycle' or args.p8_seed != 0
          or args.p8_deadline_seconds != 900):
        parser.error('P8 lifecycle metadata requires --p8-lifecycle-hold')
    if args.paper_p8_e3_low_load and args.paper_p8_e3 is None:
        parser.error('--paper-p8-e3-low-load requires --paper-p8-e3 polling|wakeup')
    if (args.paper_p8_sessions or args.paper_p8_slow_viewer or args.paper_p8_load or args.paper_p8_e1 or args.paper_p8_e2 or args.paper_p8_e3 or args.paper_p8_e4 or args.paper_p8_e5 or args.paper_p8_recovery) and any((args.paper_p8_e3 is not None and args.paper_p8_e1, args.paper_p0, args.paper_p1, args.paper_p2_spike,
            args.paper_p2, args.paper_p7, args.paper_p3, args.paper_p4, args.paper_p5a,
            args.paper_p5b, args.paper_p6, args.startup_only, args.positive_http_only,
            args.role_http_only, args.full_http, args.metrics_off)):
        parser.error('P8 probe selectors are isolated opt-in fixture lanes')
    if bool(args.paper_p8_e5) != bool(args.paper_p8_e5_context):
        parser.error('--paper-p8-e5 requires --paper-p8-e5-context, and context requires E5')
    if args.paper_p8_e5_reference and (not args.paper_p8_e5 or args.paper_p8_e5 == 'optimized'):
        parser.error('--paper-p8-e5-reference is only valid for E5 ablation arms')
    if args.paper_p8_e5 and args.paper_p8_e5 != 'optimized' and not args.paper_p8_e5_reference:
        parser.error('E5 ablation requires --paper-p8-e5-reference')
    if args.paper_p8_recovery and any((args.paper_p8_sessions, args.paper_p8_slow_viewer, args.paper_p8_load,
            args.paper_p8_e1, args.paper_p8_e2, args.paper_p8_e3, args.paper_p8_e4, args.paper_p8_e5,
            args.paper_p8_e5_context, args.paper_p8_e5_reference)):
        parser.error('--paper-p8-recovery conflicts with other P8 experiment selectors')
    if sum((args.paper_p8_sessions, args.paper_p8_slow_viewer, args.paper_p8_load, args.paper_p8_e1, args.paper_p8_e2, args.paper_p8_e3 is not None, args.paper_p8_e4 is not None, args.paper_p8_e5 is not None, args.paper_p8_recovery)) > 1:
        parser.error('select only one P8 probe lane')
    if args.paper_p3 and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2,
                              args.startup_only, args.positive_http_only, args.role_http_only,
                              args.full_http, args.paper_p4, args.paper_p5a, args.mode, args.dataset, args.metrics_off)):
        parser.error('P3 cannot be combined with other execution modes')
    if args.p5a_control_regression and not args.paper_p3:
        parser.error('--p5a-control-regression requires --paper-p3')
    if args.p5b_control_regression and not (args.paper_p3 and args.p5a_control_regression):
        parser.error('--p5b-control-regression requires --paper-p3 --p5a-control-regression')
    if args.paper_p2 and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.startup_only,
                             args.positive_http_only, args.role_http_only, args.full_http, args.mode, args.metrics_off)):
        parser.error('P2 cannot be combined with other execution modes')
    if args.paper_p7_mode is not None and not args.paper_p7:
        parser.error('--paper-p7-mode requires --paper-p7')
    if args.paper_p7 and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2,
                              args.paper_p3, args.paper_p4, args.paper_p5a, args.paper_p5b,
                              args.paper_p6 and args.paper_p7_mode != 'p6_ws',
                              args.positive_http_only, args.role_http_only, args.full_http, args.metrics_off)):
        parser.error('P7 cannot be combined with other execution modes')
    if args.paper_p2_spike and any((args.paper_p0, args.paper_p1, args.startup_only,
                                  args.positive_http_only, args.role_http_only, args.full_http,
                                  args.mode, args.metrics_off)):
        parser.error('P2 spike cannot be combined with other execution modes')
    if (args.paper_p0 or args.paper_p1) and (any((args.startup_only, args.positive_http_only, args.role_http_only, args.full_http)) or args.paper_p0 and args.paper_p1):
        parser.error('paper mode cannot be combined with other execution modes')
    if args.paper_p4 and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2,
                              args.paper_p3, args.paper_p5a, args.startup_only, args.positive_http_only,
                              args.role_http_only, args.full_http, args.paper_p5a, args.dataset, args.metrics_off)):
        parser.error('P4 cannot be combined with other execution modes or metrics-off')
    if args.paper_p5a and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2,
                               args.paper_p3, args.paper_p4, args.startup_only, args.positive_http_only,
                               args.role_http_only, args.full_http, args.metrics_off)):
        parser.error('P5a cannot be combined with other execution modes or metrics-off')
    if args.paper_p5b and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2,
                               args.paper_p3, args.paper_p4, args.paper_p5a, args.startup_only,
                               args.positive_http_only, args.role_http_only, args.full_http,
                               args.paper_p5a_shutdown_smoke, args.paper_p5b_cache_smoke, args.metrics_off)):
        parser.error('P5b cannot be combined with other execution modes')
    if args.paper_p6 and any((args.paper_p0, args.paper_p1, args.paper_p2_spike, args.paper_p2,
                              args.paper_p3, args.paper_p4, args.paper_p5a, args.paper_p5b,
                              args.startup_only, args.positive_http_only, args.role_http_only,
                              args.full_http, args.metrics_off, args.mode, args.cache_mode)):
        parser.error('P6 cannot be combined with other execution modes')
    if args.paper_p6 and args.paper_p7_mode != 'p6_ws' and (args.dispatch_mode not in ('polling', 'wake-up') or args.dataset is None):
        parser.error('--paper-p6 requires --dispatch-mode polling|wake-up and --dataset 100|1')
    if args.p6_recovery_smoke and not args.paper_p6:
        parser.error('--p6-recovery-smoke requires --paper-p6')
    if args.p6_recovery_smoke and args.dispatch_mode != 'wake-up':
        parser.error('--p6-recovery-smoke requires --paper-p6 --dispatch-mode wake-up')
    if args.dispatch_mode is not None and not args.paper_p6:
        parser.error('--dispatch-mode requires --paper-p6')
    if args.paper_p1 and args.mode is None:
        parser.error('--paper-p1 requires --mode')
    if args.paper_p4 and args.mode is None:
        parser.error('--paper-p4 requires --mode')
    if args.paper_p5a and (args.mode not in ('off', 'on') or args.dataset is None):
        parser.error('--paper-p5a requires --mode off|on and --dataset 100|1')
    if args.paper_p5b and (args.cache_mode not in ('off', 'on') or args.dataset is None):
        parser.error('--paper-p5b requires --cache-mode off|on and --dataset 100|1')
    if args.paper_p5a_shutdown_smoke and not (args.paper_p5a and args.mode == 'on' and args.dataset == '1'):
        parser.error('--paper-p5a-shutdown-smoke requires --paper-p5a --mode on --dataset 1')
    if args.paper_p5b_cache_smoke and not (args.paper_p5a and args.mode == 'on' and args.dataset == '1'
                                           and not args.paper_p5a_shutdown_smoke):
        parser.error('--paper-p5b-cache-smoke requires --paper-p5a --mode on --dataset 1 without shutdown smoke')
    if args.dataset is not None and not args.paper_p5a:
        if not args.paper_p5b and not args.paper_p6:
            parser.error('--dataset requires --paper-p5a or --paper-p5b')
    if args.mode is not None and not args.paper_p1 and not args.paper_p4 and not args.paper_p5a:
        parser.error('--mode requires --paper-p1, --paper-p4 or --paper-p5a')
    if args.cache_mode is not None and not args.paper_p5b:
        parser.error('--cache-mode requires --paper-p5b')
    if args.metrics_off and not args.paper_p1:
        parser.error('--metrics-off requires --paper-p1')
    fixture = None
    try:
        fixture = prepare(args.prepare, paper_p0=args.paper_p0,
                          paper_p1=args.mode if args.paper_p1 else None,
                          metrics_enabled=not args.metrics_off, paper_p2_spike=args.paper_p2_spike,
                           paper_p2=args.paper_p2 or remote_profile is not None, paper_p7=args.paper_p7,
                          paper_p7_mode=args.paper_p7_mode, paper_p3=args.paper_p3,
                           paper_p3_microbatch=args.p5a_control_regression,
                           paper_p5b_control_regression=args.p5b_control_regression,
                           paper_p4=args.mode if args.paper_p4 else None,
                            paper_p5a=(args.mode, args.dataset) if args.paper_p5a else None,
                            paper_p5b=(args.cache_mode, args.dataset) if args.paper_p5b else None,
                              paper_p5a_shutdown=args.paper_p5a_shutdown_smoke,
                              paper_p5b_cache_smoke=args.paper_p5b_cache_smoke,
                                paper_p6=(('wake-up', '100') if args.paper_p7 and args.paper_p7_mode == 'p6_ws' else
                                  (args.dispatch_mode, args.dataset) if args.paper_p6 else None),
                             paper_p8_sessions=args.paper_p8_sessions,
                             paper_p8_slow=args.paper_p8_slow_viewer,
                              paper_p8_load=args.paper_p8_load,
                                  paper_p8_e1=(args.paper_p8_e1 or args.p8_e1_prepare_hold),
                                  paper_p8_e1_viewers=args.p8_e1_viewers,
                                  paper_p8_e1_principals=args.p8_e1_principals,
                                  paper_p8_e1_density=args.p8_e1_density,
                                  paper_p8_e1_messages_per_target=args.p8_e1_messages_per_target,
                                 paper_p8_e2=(args.paper_p8_e2 or args.p8_e2_prepare_hold),
                                 paper_p8_e2_workload=(args.p8_e2_workload if args.p8_e2_prepare_hold else getattr(args, 'paper_p8_e2_workload', 'steady_burst')),
                                 paper_p8_e2_context=(args.p8_e2_context if args.p8_e2_prepare_hold else getattr(args, 'paper_p8_e2_context', None)),
                                  paper_p8_e2_reference=(args.p8_e2_reference if args.p8_e2_prepare_hold else getattr(args, 'paper_p8_e2_reference', None)),
                                  paper_p8_profile=args.p8_profile,
                                  paper_p8_official_opt_in=args.p8_official_opt_in,
                                  paper_p8_e2_offered_rate=args.p8_e2_offered_rate,
                                  paper_p8_e2_duration_seconds=args.p8_e2_duration_seconds,
                                  paper_p8_e2_burst_messages=args.p8_e2_burst_messages,
                                  paper_p8_e2_samples_per_message=int(args.p8_e2_samples_per_message or 1),
                                   paper_p8_e3=paper_p8_e3_selector(args),
                                   paper_p8_e3_workload=(args.p8_e3_workload if args.p8_e3_prepare_hold else
                                       'low_load' if args.paper_p8_e3_low_load else 'mixed'),
                                   paper_p8_e3_command_count=args.p8_e3_command_count if args.p8_e3_prepare_hold else args.paper_p8_e3_command_count,
                                   paper_p8_e3_schedule=args.p8_e3_schedule if args.p8_e3_prepare_hold else args.paper_p8_e3_schedule,
                                  paper_p8_e4=(args.paper_p8_e4 or
                                      args.p8_e4_cache_mode if args.p8_e4_prepare_hold else args.paper_p8_e4),
                                  paper_p8_e5=(args.paper_p8_e5 or (args.p8_arm if args.p8_e5_prepare_hold else None)),
                                  paper_p8_e5_context=(args.paper_p8_e5_context or (args.p8_e5_context if args.p8_e5_prepare_hold else None)),
                                  paper_p8_e5_reference=(args.paper_p8_e5_reference or (args.p8_e5_reference if args.p8_e5_prepare_hold else None)),
                                  paper_p8_recovery=args.paper_p8_recovery,
                          remote_network_config=remote_profile)
        if fixture.paper_p6:
            object.__setattr__(fixture, 'paper_p6_recovery_smoke', args.p6_recovery_smoke)
        object.__setattr__(fixture, 'paper_p8_prepare_only',
                           args.p8_e1_prepare_hold or args.p8_e2_prepare_hold
                           or args.p8_e3_prepare_hold or args.p8_e4_prepare_hold
                           or args.p8_e5_prepare_hold)
        object.__setattr__(fixture, 'paper_p8_e3_prepare_only', args.p8_e3_prepare_hold)
        object.__setattr__(fixture, 'paper_p8_e4_prepare_only', args.p8_e4_prepare_hold)
        object.__setattr__(fixture, 'paper_p8_e4_cold_replay', args.p8_e4_cold_replay)
        object.__setattr__(fixture, 'paper_p8_e4_expiry_replay', args.p8_e4_expiry_replay)
        object.__setattr__(fixture, 'paper_p8_e4_unique_count', args.p8_e4_unique_count)
        object.__setattr__(fixture, 'paper_p8_e4_replay_count', args.p8_e4_replay_count)
        object.__setattr__(fixture, 'paper_p8_e5_prepare_only', args.p8_e5_prepare_hold)
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
        if args.p8_lifecycle_hold:
            import paper_p8_supervisor as supervisor
            lock = supervisor.acquire_lock(fixture.private)
            session = {}
            def hold_ready_fixture(*, fixture, registry, containers, images, evidence, endpoint, state,
                                   paperload_binary):
                session.update({'registry': registry, 'containers': containers, 'endpoint': endpoint})
                receipts = [{key: item.get(key) for key in ('kind', 'name', 'image', 'labels',
                            'task', 'run', 'nonce', 'receipt', 'allocation_attempted')}
                            for item in registry.items]
                prepared_bundle = None
                if (args.p8_e1_prepare_hold or args.p8_e2_prepare_hold
                        or args.p8_e3_prepare_hold or args.p8_e4_prepare_hold
                        or args.p8_e5_prepare_hold):
                    helper = paper_p8_e1_support()
                    session_id = secrets.token_hex(16)
                    experiment = args.p8_experiment
                    bundle_root = (args.p8_e1_bundle_root if experiment == 'E1' else
                                    args.p8_e2_bundle_root if experiment == 'E2' else
                                    args.p8_e3_bundle_root if experiment == 'E3' else
                                    args.p8_e4_bundle_root if experiment == 'E4' else
                                    args.p8_e5_bundle_root)
                    if experiment == 'E1':
                        prepared = helper.prepare(sys.modules[__name__], fixture, endpoint, registry,
                            containers, images, state, session_id=session_id, phase='pilot',
                            mode='remote', arm=args.p8_arm,
                            output_root=fixture.private / 'p8-e1-prepared',
                            profile=args.p8_profile, official_opt_in=args.p8_official_opt_in,
                            viewers=args.p8_e1_viewers, principals=args.p8_e1_principals,
                            density=args.p8_e1_density,
                            messages_per_target=args.p8_e1_messages_per_target)
                        e1_lifecycle = prepared.get('viewer_manifest', {}).get('limits', {}).get('wall_seconds')
                        prepared_bundle = helper.export_prepared_bundle(prepared,
                            output_leaf=bundle_root, wait_seconds=90, workload_seconds=e1_lifecycle)
                    elif experiment == 'E2':
                        e2 = paper_p8_e2_support()
                        e2_workload = getattr(args, 'p8_e2_workload', 'steady_burst')
                        retry_fields = ('p8_e2_retry_mode', 'p8_e2_retry_initial_interval_ms',
                            'p8_e2_retry_max_interval_ms', 'p8_e2_retry_jitter_percent',
                            'p8_e2_retry_rate_per_second', 'p8_e2_retry_burst')
                        supplied_retry = [getattr(args, field, None) for field in retry_fields]
                        retry_policy = None
                        if any(value is not None for value in supplied_retry):
                            if any(value is None for value in supplied_retry):
                                raise ValueError('e2_retry_policy_fields_rejected')
                            retry_policy = e2.validate_retry_policy(dict(zip(
                                ('mode', 'initial_interval_ms', 'max_interval_ms', 'jitter_percent',
                                 'rate_per_second', 'burst'), supplied_retry)),
                                pending_messages=args.p8_e2_pending_messages)
                        e2_phase_id = (f'equal-samples-{args.p8_e2_samples_per_message}'
                                       if e2_workload == 'equal_samples'
                                       else f'samples-{args.p8_e2_samples_per_message}')
                        output_root = (fixture.private / f'p8-e2-equal-prepared-{args.p8_e2_samples_per_message}'
                                       if e2_workload == 'equal_samples'
                                       else fixture.private / f'p8-e2-prepared-{args.p8_e2_samples_per_message}')
                        if e2_workload == 'equal_samples':
                            ctx = e2.validate_e2_equal_samples_context(
                                json.loads(Path(args.p8_e2_context).read_text()), check_freshness=True)
                            duration_seconds = ctx['duration_seconds']
                            wall_budget = duration_seconds + 20 + 10
                            prepared = e2.prepare_remote(
                                sys.modules[__name__], fixture, endpoint, registry,
                                containers, images, state,
                                session_id=session_id,
                                samples_per_message=int(args.p8_e2_samples_per_message),
                                output_root=output_root,
                                bundle_root=bundle_root,
                                arm=args.p8_arm,
                                workload=e2_workload,
                                context_path=args.p8_e2_context,
                                reference_path=args.p8_e2_reference,
                                context=ctx,
                                check_freshness=True,
                            )
                        else:
                            prepared = e2.prepare_remote(
                                sys.modules[__name__], fixture, endpoint, registry,
                                containers, images, state,
                                session_id=session_id,
                                samples_per_message=int(args.p8_e2_samples_per_message),
                                output_root=output_root,
                                bundle_root=bundle_root,
                                arm=args.p8_arm,
                                workload='steady_burst',
                                profile=args.p8_profile,
                                official_opt_in=args.p8_official_opt_in,
                                offered_rate=args.p8_e2_offered_rate,
                                duration_seconds=args.p8_e2_duration_seconds,
                                burst_messages=args.p8_e2_burst_messages,
                                pending_messages=args.p8_e2_pending_messages,
                                 pending_bytes=args.p8_e2_pending_bytes,
                                 retry_policy=retry_policy,
                            )
                        prepared['e2_support'] = e2
                        prepared['workload'] = e2_workload
                        if e2_workload == 'equal_samples':
                            prepared['phase_id'] = e2_phase_id
                            prepared['context_path'] = args.p8_e2_context
                            prepared['reference_path'] = args.p8_e2_reference
                            prepared['duration_seconds'] = duration_seconds
                            prepared['wall_budget'] = wall_budget
                            durable_json(fixture.artifacts / 'p8-e2-equal-samples-pilot.json', {
                                'schema_version': 1, 'status': 'PREPARATION_ONLY',
                                'workload_status': 'NOT_RUN', 'experiment': 'E2',
                                'workload': 'equal_samples',
                                'samples_per_message': int(args.p8_e2_samples_per_message),
                                'run_id': prepared.get('run_id'), 'session_id': session_id,
                                'phase_id': e2_phase_id, 'full_p8_accepted': False,
                                'generator_colocated': True, 'physical_ab': 'NOT RUN'})
                        prepared_bundle = prepared['bundle']
                    elif experiment == 'E3':
                        e3 = paper_p8_e3_support()
                        prepared = e3.prepare(sys.modules[__name__], fixture, endpoint, registry,
                            containers, images, state, arm=args.p8_arm,
                            workload=args.p8_e3_workload, remote_profile=remote_profile,
                            session_id=session_id,
                            command_count=args.p8_e3_command_count,
                            schedule=args.p8_e3_schedule,
                            profile=args.p8_profile,
                            official_opt_in=args.p8_official_opt_in)
                        prepared_bundle = e3.export_prepared_bundle(prepared, output_leaf=bundle_root)
                        prepared['e3_support'] = e3
                        durable_json(fixture.artifacts / paper_p8_e3_artifact_name(fixture), {
                            'schema_version': 1, 'status': 'PREPARATION_ONLY',
                            'workload_status': 'NOT_RUN', 'experiment': 'E3',
                            'arm': args.p8_arm, 'workload': args.p8_e3_workload,
                            'run_id': prepared['run_id'], 'session_id': session_id,
                            'phase_id': prepared['phase_id'], 'profile': prepared['profile'],
                            'effective_flags': e3._validate_e3_fixture_flags(
                                prepared['effective'], prepared['arm']),
                            'full_p8_accepted': False, 'generator_colocated': True,
                            'physical_ab': 'NOT RUN'})
                    if experiment == 'E4':
                        bundle_parent = Path(bundle_root).absolute().parent
                        if any(item.is_symlink() for item in (bundle_parent, *bundle_parent.parents)):
                            raise ValueError('p8_e4_bundle_parent_symlink_rejected')
                        bundle_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        bundle_parent_stat = bundle_parent.stat()
                        if (bundle_parent_stat.st_uid != os.getuid()
                                or bundle_parent_stat.st_mode & 0o077):
                            raise ValueError('p8_e4_bundle_parent_permissions_rejected')
                        e4 = paper_p8_e4_support()
                        prepared = e4.prepare(sys.modules[__name__], fixture, endpoint, registry,
                            containers, images, state, args.p8_e4_cache_mode,
                            remote_profile=remote_profile, session_id=session_id,
                            include_cold_replay=args.p8_e4_cold_replay,
                            include_expiry_replay=args.p8_e4_expiry_replay,
                            include_durable_reopen=args.p8_e4_durable_reopen,
                            include_conflicts=args.p8_e4_conflicts,
                            unique_count=args.p8_e4_unique_count,
                            replay_count=args.p8_e4_replay_count,
                            profile=args.p8_profile, official_opt_in=args.p8_official_opt_in,
                            session_seconds=args.p8_deadline_seconds)
                        prepared_bundle = e4.export_prepared_bundle(prepared, output_leaf=bundle_root)
                        prepared['e4_support'] = e4
                        phase_names = (['unique', 'replay'] + (['cold-replay'] if prepared.get('include_cold_replay') else [])
                            + (['expiry-replay'] if prepared.get('include_expiry_replay') else []))
                        prepared['phase_run_ids'] = {phase: json.loads(path.read_text())['run_id']
                            for phase, path in zip(phase_names, prepared['manifests'])}
                        phase_metadata = paper_p8_e4_preparation_metadata(prepared['phase_run_ids'])
                        prepared['bundle_root'] = Path(bundle_root)
                        object.__setattr__(fixture, 'paper_p8_e4_remote_warm', True)
                        durable_json(fixture.artifacts / paper_p8_e4_artifact_name(fixture), {
                            'schema_version': 1, 'status': 'PREPARATION_ONLY',
                            'workload_status': 'NOT_RUN', 'experiment': 'E4',
                            'arm': prepared['arm'], 'cache_mode': prepared['cache_mode'],
                            'run_id': prepared['run_id'], 'session_id': session_id,
                            'phase_id': prepared['phase_id'], 'profile': prepared['profile'],
                            'mode': prepared['mode'], 'schema': prepared['schema'],
                            'quota': prepared['quota'], 'effective_flags': prepared['effective_flags'],
                            **phase_metadata, 'viewer_experiment': 'E1',
                            'journal_binding': 'same B-owned outputs/e4-journal.sqlite reference',
                            'coldrestart': 'NOT RUN', 'durable_gateway_reopen': 'NOT RUN',
                            'conflicts': 'NOT RUN', 'full_p8_accepted': False,
                            'generator_colocated': True, 'physical_ab': 'NOT RUN'})
                    elif experiment == 'E5':
                        bundle_parent = Path(bundle_root).absolute().parent
                        if any(item.is_symlink() for item in (bundle_parent, *bundle_parent.parents)):
                            raise ValueError('p8_e5_bundle_parent_symlink_rejected')
                        bundle_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                        bundle_parent_stat = bundle_parent.stat()
                        if (bundle_parent_stat.st_uid != os.getuid()
                                or bundle_parent_stat.st_mode & 0o077):
                            raise ValueError('p8_e5_bundle_parent_permissions_rejected')
                        import paper_p8_e5_remote as e5_remote
                        prepared = e5_remote.prepare(
                            sys.modules[__name__], fixture, endpoint, registry,
                            containers, images, state, arm=args.p8_arm,
                            context_path=args.p8_e5_context,
                            reference_path=args.p8_e5_reference,
                            remote_profile=remote_profile,
                        )
                        prepared['session_id'] = session_id
                        prepared['phase_id'] = 'initial'
                        initial_manifest = json.loads(Path(prepared['manifests']['initial']).read_text())
                        prepared['run_id'] = initial_manifest['run_id']
                        prepared_bundle = e5_remote.export_prepared_bundle(
                            prepared, output_leaf=bundle_root,
                            deadline_seconds=args.p8_deadline_seconds,
                            session_id=session_id,
                        )
                        prepared['bundle_root'] = Path(bundle_root)
                        object.__setattr__(fixture, 'paper_p8_e5', args.p8_arm)
                        durable_json(fixture.artifacts / paper_p8_e5_artifact_name(fixture), {
                            'schema_version': 1, 'status': 'PREPARATION_ONLY',
                            'workload_status': 'NOT_RUN', 'experiment': 'E5',
                            'arm': prepared['arm'],
                            'run_id': prepared['run_id'], 'session_id': session_id,
                            'phase_id': 'initial', 'schema': prepared['schema'],
                            'quota': prepared['quota'], 'effective_flags': prepared['effective_flags'],
                            'full_p8_accepted': False, 'physical_ab': 'NOT RUN'})
                    prepared['bundle_sha256'] = hashlib.sha256(
                        (Path(bundle_root) / 'bundle.json').read_bytes()).hexdigest()
                    bundle_metadata = json.loads((Path(bundle_root) / 'bundle.json').read_text())
                    prepared['bundle_inventory'] = [{'path':row['path'],'sha256':row['sha256']}
                                                    for row in bundle_metadata['inputs']]
                    prepared['bundle_session'] = bundle_metadata['session']
                    session['prepared'] = prepared
                    artifacts = [{'kind': 'client_bundle', 'path': str(bundle_root),
                                  'session_id': session_id, 'run_id': prepared['run_id'],
                                  'phase': 'pilot' if experiment == 'E1' else prepared['phase_id'],
                                    'manifest_count': (4 if experiment == 'E5' else
                                        phase_metadata['manifest_count'] if experiment == 'E4'
                                        else 3 if experiment == 'E3' else 2), 'gateways': 2,
                                  'sensors': 2 if experiment in ('E2', 'E4', 'E5') else 4,
                                   'users': 2 if experiment in ('E1', 'E4', 'E5') else 0,
                                   'viewers': 2 if experiment in ('E4', 'E5') else 64 if experiment == 'E1' else 0,
                                  'file_count': prepared_bundle['file_count'],
                                  'total_bytes': prepared_bundle['total_bytes'],
                                  'sha256': prepared['bundle_sha256'],
                                  'schema': prepared['schema'],
                                  'quota': prepared['quota'],
                                  **({'dispatch_mode': prepared['arm'],
                                      'workload': prepared['workload'],
                                      'executor_count': len(prepared['gateway_env_files']),
                                      'executor_gateway_ids': [
                                          json.loads(Path(p).read_text())['GATEWAY_ID']
                                          for p in prepared['gateway_env_files']],
                                      'executor_client_ids': [
                                          json.loads(Path(p).read_text())['GATEWAY_MQTT_CLIENT_ID']
                                          for p in prepared['gateway_env_files']]}
                                     if experiment == 'E3' else {}),
                                  'effective_flags': (helper.safe_effective_flags(prepared['effective'])
                                      if experiment == 'E1' else
                                       paper_p8_e2_validate_fixture_flags(prepared['effective'],
                                           workload=prepared.get('workload', 'steady_burst'),
                                           e2_support=prepared['e2_support'], fixture=fixture)
                                       if experiment == 'E2' else
                                       prepared['e3_support']._validate_e3_fixture_flags(
                                           prepared['effective'], prepared['arm']) if experiment == 'E3' else
                                       prepared['effective_flags']),
                                  'storage_status': prepared['storage']['status'] if isinstance(prepared.get('storage'), dict) else prepared.get('storage_status', 'healthy')}]
                    session['state'] = supervisor.initialize(fixture.private,
                        experiment=experiment, arm=args.p8_arm, seed=args.p8_seed,
                        deadline_seconds=args.p8_deadline_seconds, receipts=receipts,
                        image_metadata=images, artifacts=artifacts,
                        preparation_status=f'{experiment}_PREPARED', session_id=session_id,
                        profile=args.p8_profile, official_opt_in=args.p8_official_opt_in,
                         source_metadata={**paper_p8_support()._source_fingerprint(ROOT),
                             'dirty_worktree': bool(paper_p0_git(fixture, 'status', '--porcelain')),
                             'qualification': 'local source and dirty state; not a release provenance claim'},
                        endpoint_metadata={'https': remote_profile['advertised_https'],
                            'mqtt_tls': remote_profile['advertised_mqtt_tls'],
                            'bind_ip': remote_profile['bind_ip']},
                        project=fixture.project, lock=lock,
                         cold_replay_enabled=args.p8_e4_cold_replay,
                         expiry_replay_enabled=args.p8_e4_expiry_replay,
                        e4_phase_plan=(['unique','replay']
                            + (['cold-replay'] if args.p8_e4_cold_replay else [])
                            + (['durable-fault','durable-reopen'] if args.p8_e4_durable_reopen else [])
                            + (['conflicts'] if args.p8_e4_conflicts else [])
                            + (['expiry-replay'] if args.p8_e4_expiry_replay else [])) if experiment == 'E4' else None,
                        e5_phase_plan=['initial', 'warm_replay', 'control'] if experiment == 'E5' else None,
                        workload=(prepared.get('workload') if experiment == 'E2' else None))
                else:
                    session['state'] = supervisor.initialize(fixture.private, experiment=args.p8_experiment,
                        arm=args.p8_arm, seed=args.p8_seed, deadline_seconds=args.p8_deadline_seconds,
                        receipts=receipts, image_metadata=images,
                        profile=args.p8_profile, official_opt_in=args.p8_official_opt_in,
                        endpoint_metadata=({'https': remote_profile['advertised_https'],
                            'mqtt_tls': remote_profile['advertised_mqtt_tls'],
                            'bind_ip': remote_profile['bind_ip']} if remote_profile else {}),
                        project=fixture.project, lock=lock)
                paper_p8_bind_initialized_session_deadline(session)
                if paper_p8_hold_callbacks_required(args):
                    def begin_collection(request):
                        if experiment == 'E2':
                            paper_p8_bind_initialized_session_deadline(session, prepared)
                        if experiment == 'E5':
                            return paper_p8_e5_begin_phase(
                                sys.modules[__name__], fixture, endpoint, registry, containers,
                                prepared, request.get('phase_id'))
                        if experiment == 'E4':
                            if request.get('phase_id') == 'conflicts':
                                return prepared['e4_support'].begin_remote_conflicts_phase(
                                    sys.modules[__name__], fixture, registry, prepared)
                            if request.get('phase_id') in ('durable-fault','durable-reopen'):
                                return prepared['e4_support'].begin_remote_durable_phase(
                                    sys.modules[__name__], registry, prepared, request['phase_id'])
                            if request.get('phase_id') not in ('unique', 'replay', 'cold-replay', 'expiry-replay'):
                                raise ValueError('p8_e4_phase_id_rejected')
                            return prepared['e4_support'].begin_remote_phase(
                                sys.modules[__name__], fixture, endpoint, registry, containers,
                                prepared, request['phase_id'])
                        if experiment == 'E3':
                            if request.get('phase_id') != prepared['phase_id']:
                                raise ValueError('e3_phase_id_mismatch')
                            return paper_p8_e3_support().begin_remote_collection(
                                sys.modules[__name__], fixture, endpoint, registry, containers,
                                prepared, phase_id=request['phase_id'])
                        if experiment == 'E1':
                            return paper_p8_e1_begin_collection(request, session['state'],
                                fixture, endpoint, registry, containers, prepared)
                        if experiment == 'E2':
                            if request.get('phase_id') != prepared['phase_id']:
                                raise ValueError('e2_phase_id_mismatch')
                            return prepared['e2_support'].begin_remote_collection(
                                sys.modules[__name__], fixture, endpoint, registry, containers,
                                prepared, phase_id=request['phase_id'])
                    def replace_for_cold_replay(request, session_state):
                        if (experiment != 'E4' or request.get('phase_id') != 'cold-replay'
                                or prepared.get('include_cold_replay') is not True
                                or prepared.get('cache_mode') != 'on'):
                            raise ValueError('p8_e4_cold_restart_not_enabled')
                        warm_window = prepared.get('phase_windows', {}).get('replay', {})
                        old_metrics = warm_window.get('admin_after') if isinstance(warm_window, dict) else None
                        old_process = old_metrics.get('process_instance') if isinstance(old_metrics, dict) else None
                        if not old_process:
                            raise ValueError('p8_e4_cold_old_process_unavailable')
                        replacement = prepared['e4_support']._cold.replace_owned_backend(
                            sys.modules[__name__], fixture, registry, containers, state)
                        receipts = [{key: item.get(key) for key in ('kind', 'name', 'image', 'labels',
                            'task', 'run', 'nonce', 'receipt', 'allocation_attempted')}
                            for item in registry.items]
                        supervisor.refresh_owned_receipts(fixture.private, session_state, receipts)
                        durable_json(fixture.private / 'resource-receipts.json', [
                            {key: (str(value) if isinstance(value, Path) else value)
                             for key, value in item.items()} for item in registry.items])
                        deadline = time.monotonic() + 45
                        last_metrics = None
                        while time.monotonic() < deadline:
                            state.checkpoint()
                            try:
                                last_metrics = prepared['e4_support']._support._admin_metrics(
                                    sys.modules[__name__], endpoint, prepared['tokens']['admin'])
                            except (OSError, ValueError):
                                time.sleep(.5)
                                continue
                            cache = last_metrics.get('ram_dedup') if isinstance(last_metrics, dict) else None
                            if (last_metrics.get('process_instance') != old_process
                                    and isinstance(cache, dict) and type(cache.get('entries')) is int
                                    and cache.get('entries') == 0):
                                break
                            time.sleep(.5)
                        cache = last_metrics.get('ram_dedup') if isinstance(last_metrics, dict) else None
                        if (not isinstance(last_metrics, dict)
                                or last_metrics.get('process_instance') == old_process
                                or not isinstance(cache, dict) or type(cache.get('entries')) is not int
                                or cache.get('entries') != 0):
                            raise ValueError('p8_e4_cold_replacement_readiness_unproven')
                        proof = {'replacement': replacement['replacement'],
                            'old_process_instance': old_process,
                            'process_instance': last_metrics['process_instance'],
                            'cache_entries': cache['entries'], 'metrics_readback': True}
                        prepared['cold_old_metrics'] = old_metrics
                        prepared['cold_replacement_proof'] = proof
                        session_state['replacement_proof'] = proof
                        return {'replacement_verified': True, 'receipts': receipts,
                                'replacement_proof': proof}
                    def finish_import(request):
                        if experiment == 'E5':
                            return paper_p8_e5_finish_phase(
                                sys.modules[__name__], fixture, endpoint, registry, containers,
                                prepared, request.get('results'), fixture.artifacts,
                                request.get('phase_id'))
                        if experiment == 'E4':
                            phase = request.get('phase_id')
                            if phase == 'conflicts':
                                result = prepared['e4_support'].finish_remote_conflicts_phase(
                                    sys.modules[__name__], fixture, registry, prepared,
                                    request.get('results'))
                                report_path = fixture.artifacts / f'p8-e4-{prepared["cache_mode"]}-conflicts-phase.json'
                                durable_json(report_path, {'schema_version':1,'experiment':'E4',
                                    'session_id':session_id,'run_id':prepared['run_id'],
                                    'result':result,'full_p8_accepted':False,'physical_ab':'NOT RUN'})
                                result['report'] = str(report_path)
                                return result
                            if phase in ('durable-fault','durable-reopen'):
                                result = prepared['e4_support'].finish_remote_durable_phase(
                                    sys.modules[__name__], fixture, registry, prepared,
                                    request.get('results'), phase=phase)
                                report_path = fixture.artifacts / f'p8-e4-{prepared["cache_mode"]}-{phase}-phase.json'
                                durable_json(report_path, {'schema_version':1,'experiment':'E4',
                                    'session_id':session_id,'run_id':prepared['durable_probe']['run_id'],
                                    'result':result,'full_p8_accepted':False,'physical_ab':'NOT RUN'})
                                result['report'] = str(report_path)
                                return result
                            result = prepared['e4_support'].finish_remote_phase(
                                sys.modules[__name__], fixture, endpoint, registry, containers,
                                prepared, request.get('results'), phase=phase)
                            report_path = fixture.artifacts / f'p8-e4-{prepared["cache_mode"]}-{phase}-phase.json'
                            phase_data = prepared.get('phase_results', {}).get(phase, {})
                            durable_json(report_path, {'schema_version': 1, 'experiment': 'E4',
                                'phase': phase, 'session_id': session_id,
                                'run_id': prepared['phase_run_ids'][phase],
                                'cache_mode': prepared['cache_mode'], 'result': result,
                                'telemetry': (phase_data.get('telemetry') or {}).get('telemetry'),
                                'viewer': {key: phase_data.get('viewer', {}).get(key) for key in
                                    ('connected_distinct_viewers', 'subscribed_distinct_viewers',
                                      'receiving_distinct_viewers', 'unique_events')},
                                'e4_headers': (phase_data.get('telemetry', {}).get('e4') or {}).get('safe_header_samples', []),
                                'full_p8_accepted': False, 'physical_ab': 'NOT RUN'})
                            result['report'] = str(report_path)
                            return result
                        if experiment == 'E3':
                            helper = paper_p8_e3_support()
                            result = helper.finish_remote_collection(sys.modules[__name__], fixture,
                                endpoint, registry, containers, prepared, request.get('results'),
                                phase_id=request['phase_id'], paperload_binary=paperload_binary)
                            report_path = fixture.artifacts / paper_p8_e3_artifact_name(fixture).replace('.json', '-remote-result.json')
                            durable_json(report_path, result)
                            return {'workload_outcome': result['workload_outcome'],
                                    'validation_valid': result['validation']['valid'],
                                    'validation_failures': result['validation']['failures'],
                                    'report': str(report_path)}
                        helper = paper_p8_e1_support() if experiment == 'E1' else prepared['e2_support']
                        if experiment == 'E1':
                            if request.get('phase_id') != 'pilot':
                                raise ValueError('e1_phase_id_mismatch')
                            result = helper.finish_remote_collection(sys.modules[__name__], fixture,
                                endpoint, registry, containers, prepared, request.get('results'),
                                phase_id=request['phase_id'], paperload_binary=paperload_binary)
                            report_path = fixture.artifacts / 'p8-e1-remote-result.json'
                        else:
                            result = helper.finish_remote_collection(sys.modules[__name__], fixture,
                                endpoint, registry, containers, prepared, request.get('results'),
                                phase_id=request['phase_id'], paperload_binary=paperload_binary)
                            report_path = (fixture.artifacts / f'p8-e2-{request["phase_id"]}-remote-result.json'
                                           if request.get('phase_id') in ('equal-samples-1', 'equal-samples-100')
                                           else fixture.artifacts / f'p8-e2-samples-{prepared["samples_per_message"]}-remote-result.json')
                        durable_json(report_path, {
                            'schema_version': 1, 'session_id': prepared['session_id'],
                            'run_id': prepared['run_id'], 'phase_id': request['phase_id'],
                            'arm': prepared['common']['arm'] if experiment == 'E1' else prepared['arm'],
                            'experiment': experiment,
                            'schema': prepared['schema'], 'quota': prepared['quota'],
                            'effective_flags': (helper.safe_effective_flags(prepared['effective'])
                                if experiment == 'E1' else paper_p8_e2_validate_fixture_flags(prepared['effective'],
                                    workload=prepared.get('workload', 'steady_burst'), e2_support=helper,
                                    fixture=fixture)),
                            'source': session['state'].get('source_metadata'),
                            'images': images, 'bundle_sha256': prepared['bundle_sha256'],
                            **({'manifest_sha256': [hashlib.sha256(path.read_bytes()).hexdigest()
                                                for path in prepared['manifest_paths']],
                                'mapping': prepared['mapping']} if experiment == 'E1' else {}), **result})
                        if experiment == 'E1':
                            paper_p8_write_private_csv(fixture.artifacts / 'p8-e1-remote-metrics.csv',
                            ('phase', 'snapshot_json'), ((row.get('phase'),
                                json.dumps(row.get('snapshot'), sort_keys=True))
                                for row in result['collector_metrics']))
                            paper_p8_write_private_csv(fixture.artifacts / 'p8-e1-remote-resources.csv',
                            ('phase', 'sample_json'), ((row.get('phase'), json.dumps(row, sort_keys=True))
                                for row in result['resource_samples']))
                        return {'workload_outcome':result['workload_outcome'],
                                'validation_valid':result['validation']['valid'],
                                'validation_failures':result['validation']['failures'],
                                'report':str(report_path)}
                    prepared = session.get('prepared') or {}
                    declared_growth = []
                    candidates = prepared.get('manifests')
                    if isinstance(candidates, dict):
                        for manifest in candidates.values():
                            if isinstance(manifest, dict):
                                value = (manifest.get('limits') or {}).get('disk_growth_bytes')
                                if type(value) is int and value > 0:
                                    declared_growth.append(value)
                    growth_limit = min(declared_growth) if declared_growth else 64 << 20
                    owned_guard = p8_owned_storage_guard(fixture, registry, containers['postgres'],
                        growth_limit_bytes=growth_limit)
                    supervisor.hold(fixture.private, lock=lock,
                                    on_begin=begin_collection, on_finish=finish_import,
                                    on_next=replace_for_cold_replay if experiment == 'E4' else None,
                                    owned_guard=owned_guard)
                else:
                    supervisor.hold(fixture.private, lock=lock)
            def finalize_session(*, cleanup_verified, cleanup_error, workload_error):
                if not (fixture.private / 'session.json').exists():
                    return
                collector = (session.get('prepared', {}).get('remote_collector')
                             or session.get('prepared', {}).get('collector'))
                if collector is not None and not args.p8_e2_prepare_hold and not session.get('e2_diagnostics_flushed'):
                    collector.stop(None)
                for phase_collector in session.get('prepared', {}).get('remote_collectors', {}).values():
                    phase_collector.stop(None)
                state = supervisor.read_state(fixture.private)
                if state.get('phase') != 'FINALIZING':
                    outcome = state.get('workload_outcome', 'NOT_RUN')
                    if workload_error and outcome == 'NOT_RUN':
                        outcome = 'WORKLOAD_FAILED'
                    state = supervisor.begin_finalizing(fixture.private, outcome)
                supervisor.finalize(fixture.private, state, cleanup_verified=cleanup_verified,
                                    cleanup_error=cleanup_error)
                if getattr(fixture, 'paper_p8_e4_remote_warm', False):
                    e4 = session.get('prepared', {}).get('e4_support')
                    prepared = session.get('prepared', {})
                    final_state = supervisor.read_state(fixture.private)
                    evidence = e4.finalize_remote_report(prepared, final_state,
                        cleanup_verified=cleanup_verified, cleanup_counts={})
                    evidence.update(e4.remote_report_source(sys.modules[__name__], fixture, prepared))
                    durable_json(fixture.artifacts / paper_p8_e4_artifact_name(fixture), evidence)
                e5_prepared = session.get('prepared') if args.p8_e5_prepare_hold else None
                if e5_prepared and (e5_prepared.get('phase_snapshots')
                                    or (supervisor.read_state(fixture.private).get('phase_history') or [])):
                    session['e5_final'] = paper_p8_e5_finalize_report(
                        sys.modules[__name__], fixture, e5_prepared,
                        supervisor.read_state(fixture.private),
                        cleanup_verified=cleanup_verified)
            def collect_before_cleanup():
                prepared = session.get('prepared') or {}
                if not args.p8_e2_prepare_hold or not prepared.get('collector'):
                    return
                state = supervisor.read_state(fixture.private)
                metadata = state.get('source_metadata') if isinstance(state.get('source_metadata'), dict) else {}
                prepared['source_provenance'] = {k: metadata[k] for k in
                    ('revision', 'branch', 'dirty_worktree', 'dirty', 'dirty_source_sha256',
                     'qualification') if k in metadata and
                    (type(metadata[k]) in (str, bool) and len(str(metadata[k])) <= 160)}
                prepared['stop_cause'] = state.get('stop_reason') or state.get('workload_outcome', 'NOT_RUN')
                prepared['manifest_sha256'] = [hashlib.sha256(Path(p).read_bytes()).hexdigest()
                    for p in prepared.get('manifest_paths', []) if Path(p).is_file()]
                prepared['bundle_sha256'] = prepared.get('bundle_sha256')
                diagnostics_path = fixture.artifacts / 'p8-e2-diagnostics.json'
                try:
                    diagnostics = prepared['e2_support'].failure_diagnostics(
                        prepared, state.get('workload_outcome', 'NOT_RUN'), smoke=sys.modules[__name__],
                        endpoint=session.get('endpoint'), registry=session.get('registry'),
                        containers=session.get('containers', {}),
                        persist_callback=lambda payload: paper_p8_e2_persist_diagnostics(diagnostics_path, payload))
                    session['e2_diagnostics_flushed'] = True
                except Exception:
                    diagnostics = {'schema_version': 1, 'experiment': 'E2',
                        'full_p8_accepted': False, 'snapshot_status': 'unavailable',
                        'error_reason': 'diagnostics_flush_failed'}
                try:
                    paper_p8_e2_persist_diagnostics(diagnostics_path, diagnostics)
                except ValueError as exc:
                    if str(exc) == 'diagnostics_file_size_limit':
                        diagnostics = {'schema_version': 1, 'experiment': 'E2',
                            'full_p8_accepted': False, 'snapshot_status': 'unavailable',
                            'error_reason': 'diagnostics_file_size_limit'}
                        try:
                            paper_p8_e2_persist_diagnostics(diagnostics_path, diagnostics)
                        except Exception:
                            pass
                    else:
                        raise
                except Exception:
                    try:
                        durable_json(diagnostics_path, {
                            'schema_version': 1, 'experiment': 'E2', 'full_p8_accepted': False,
                            'snapshot_status': 'unavailable', 'error_reason': 'diagnostics_persist_failed'})
                    except Exception:
                        pass
            try:
                report = startup_only(fixture, hold_callback=hold_ready_fixture,
                                      finalization_callback=finalize_session,
                                      pre_cleanup_callback=collect_before_cleanup)
            finally:
                lock.close()
            e3_lifecycle_ok = True
            if args.p8_e3_prepare_hold:
                final_session = supervisor.read_state(fixture.private)
                e3_lifecycle_ok = paper_p8_e3_lifecycle_outcome_accepted(final_session)
                report['workload_outcome'] = final_session.get('workload_outcome', 'NOT_RUN')
                if not e3_lifecycle_ok:
                    report['status'] = 'FAIL'
            if args.p8_e4_prepare_hold:
                final_session = supervisor.read_state(fixture.private)
                if getattr(fixture, 'paper_p8_e4_remote_warm', False):
                    expected_phases = (['unique', 'replay', 'cold-replay']
                        if final_session.get('cold_replay_enabled') is True else ['unique', 'replay'])
                    if args.p8_e4_expiry_replay:
                        expected_phases.append('expiry-replay')
                    durable_enabled = args.p8_e4_durable_reopen
                    if durable_enabled:
                        expected_phases += ['durable-fault','durable-reopen']
                    conflicts_enabled = args.p8_e4_conflicts
                    if conflicts_enabled:
                        expected_phases += ['conflicts']
                    e3_lifecycle_ok = (final_session.get('phase') == 'CLOSED'
                        and final_session.get('phase_history') == expected_phases
                        and final_session.get('workload_outcome') == 'VALIDATED'
                        and final_session.get('cleanup_outcome') == 'VERIFIED')
                    if durable_enabled:
                        e3_lifecycle_ok = (e3_lifecycle_ok
                            and isinstance(final_session.get('imported_result'), dict)
                            and final_session['imported_result'].get('validation_valid') is True)
                    if conflicts_enabled:
                        e3_lifecycle_ok = (e3_lifecycle_ok
                            and isinstance(final_session.get('imported_result'), dict)
                            and final_session['imported_result'].get('validation_valid') is True)
                    cache_off = getattr(fixture, 'paper_p8_e4_cache_mode', 'on') == 'off'
                    cold_enabled = final_session.get('cold_replay_enabled') is True
                    cold_status = 'NOT RUN'
                    if cold_enabled:
                        finalized = json.loads((fixture.artifacts / paper_p8_e4_artifact_name(fixture)).read_text())
                        cold_status = paper_p8_e4_cold_restart_status(finalized,
                            finalized.get('cleanup_verified') is True,
                            {'unique_count': getattr(fixture, 'paper_p8_e4_unique_count', 20),
                             'replay_count': getattr(fixture, 'paper_p8_e4_replay_count', 10)})
                        e3_lifecycle_ok = e3_lifecycle_ok and cold_status == 'PASS'
                    pilot_complete = conflicts_enabled and durable_enabled and cold_enabled
                    report['workload_outcome'] = (('PILOT_COMPLETE_VALIDATED' if pilot_complete else
                        'CONFLICTS_SUBSET_VALIDATED' if conflicts_enabled else
                        'DURABLE_REOPEN_SUBSET_VALIDATED' if durable_enabled else
                        'COLD_REPLAY_SUBSET_VALIDATED' if cold_enabled else
                        'CACHE_OFF_SUBSET_VALIDATED' if cache_off else
                        'WARM_SUBSET_VALIDATED') if e3_lifecycle_ok else final_session.get('workload_outcome'))
                    report['full_p8_accepted'] = False
                    report['coldrestart'] = (cold_status if cold_enabled else
                        'NOT APPLICABLE' if cache_off else 'NOT RUN')
                    if not durable_enabled:
                        report['durable_gateway_reopen'] = 'NOT RUN'
                    elif report.get('durable_gateway_reopen') in (None, 'NOT RUN'):
                        finalized = json.loads((fixture.artifacts / paper_p8_e4_artifact_name(fixture)).read_text())
                        report['durable_gateway_reopen'] = (
                            finalized['durable_gateway_reopen'].get('status')
                            if isinstance(finalized.get('durable_gateway_reopen'), dict)
                            else finalized.get('durable_gateway_reopen', 'NOT RUN'))
                    if not conflicts_enabled:
                        report['conflicts'] = 'NOT RUN'
                    elif report.get('conflicts') in (None, 'NOT RUN'):
                        finalized = json.loads((fixture.artifacts / paper_p8_e4_artifact_name(fixture)).read_text())
                        report['conflicts'] = (
                            finalized['conflicts'].get('status')
                            if isinstance(finalized.get('conflicts'), dict)
                            else finalized.get('conflicts', 'NOT RUN'))
                else:
                    e3_lifecycle_ok = (final_session.get('phase') == 'CLOSED'
                        and final_session.get('workload_outcome') == 'NOT_RUN'
                        and final_session.get('collector_ready') is not True)
                    report['workload_outcome'] = final_session.get('workload_outcome', 'NOT_RUN')
                if not e3_lifecycle_ok:
                    report['status'] = 'FAIL'
            if args.p8_e5_prepare_hold:
                final_session = supervisor.read_state(fixture.private)
                evidence = json.loads(
                    (fixture.artifacts / paper_p8_e5_artifact_name(fixture)).read_text())
                e3_lifecycle_ok, workload_outcome = paper_p8_e5_post_run_status(
                    final_session, evidence)
                if workload_outcome is not None:
                    report['workload_outcome'] = workload_outcome
                report['full_p8_accepted'] = False
                report['physical_ab'] = 'NOT RUN'
                if not e3_lifecycle_ok:
                    report['status'] = 'FAIL'
            print(json.dumps(report))
            return 0 if (report.get('exact_cleanup_verified', report.get('cleanup_verified'))
                         and report.get('status') not in ('FAIL', 'STARTUP_FAILED')
                         and e3_lifecycle_ok) else 1
        if (args.paper_p0 or args.paper_p1 or args.paper_p2_spike or args.paper_p2 or args.paper_p7 or args.paper_p3
                   or args.paper_p4 or args.paper_p5a or args.paper_p5b or args.paper_p6 or args.paper_p8_sessions or args.paper_p8_slow_viewer or args.paper_p8_load or args.paper_p8_e1 or args.paper_p8_e2 or args.paper_p8_e3 or args.paper_p8_e4 or args.paper_p8_e5 or args.paper_p8_recovery):
            report = startup_only(fixture)
            # Only this new mode removes its own generated secret inputs after
            # successful owned-resource cleanup; failure inputs remain private.
            if not is_fixture_verified(fixture):
                raise ValueError('unsafe_fixture_path')
            if not args.paper_p7 and not args.paper_p8_sessions and not args.paper_p8_slow_viewer and not args.paper_p8_load and not args.paper_p8_e1 and not args.paper_p8_e2 and not args.paper_p8_e3 and not args.paper_p8_e4 and not args.paper_p8_e5 and not args.paper_p8_recovery:
                shutil.rmtree(fixture.private)
            else:
                try:
                    fixture.private.chmod(0o700)
                except OSError:
                    pass
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
