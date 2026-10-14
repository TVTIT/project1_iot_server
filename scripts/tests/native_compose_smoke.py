#!/usr/bin/env python3
"""Isolated Compose preflight and startup-only verification; HTTP gate closed.

Read-only effective-model inspection, bounded real Dockerfile build, and an
exact-owned networkless Envoy toolchain probe are available as Python functions.
The CLI optionally verifies phased startup. No deployment env is read.
"""
from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import hashlib
import hmac
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
ROOT_COMPOSE = ROOT / "docker-compose.yml"
TEMPLATE = Path(__file__).with_suffix(".override.yml")
SERVICES = ("postgres", "supabase-role-provisioner", "application-migrations",
            "supabase-auth", "supabase-compat-migration", "mosquitto",
            "backend", "supabase-envoy", "nginx")
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
    SUPABASE_JWT_CLOCK_SKEW="30s", CLOUDFLARE_TUNNEL_TOKEN="fixture-unused-not-a-token")


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
        if item.get('helper_mount_receipt') is not None:
            if {'mounts': obj.get('Mounts'), 'tmpfs': obj.get('HostConfig', {}).get('Tmpfs')} != item['helper_mount_receipt']:
                raise ValueError('helper_mount_replacement_refused')
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
    item['helper_mount_receipt'] = {'mounts': obj['Mounts'], 'tmpfs': obj['HostConfig']['Tmpfs']}
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


def prepare(base):
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
    stanzas = ["services:\n  rathole: !reset null\n  supabase-storage: !reset null\n  cloudflared: !reset null\n"]
    for service in SERVICES:
        stanzas.append(f"  {service}:\n    container_name: !reset null\n    restart: 'no'\n")
        if service != "nginx":
            stanzas.append("    ports: !override []\n")
        stanzas.append(f"    labels:\n      io.iot.native-smoke.run: ${{NCS_RUN:?required}}\n      io.iot.native-smoke.role: {service}\n      test.task: native-compose-startup\n      test.run: ${{NCS_RUN:?required}}\n")
        stanzas.append(extra_by_service.get(service, ""))
    private_write(override, "".join(stanzas) + template[resource_offset:])
    # Copy only the reviewed broker config, never deployment certificates.
    private_write(private / "mosquitto.conf", (ROOT / "config/mosquitto/mosquitto.conf").read_text())
    art_stat = artifacts.stat()
    return Fixture(base, private, artifacts, env_file, override, project, values,
                   art_stat.st_dev, art_stat.st_ino)


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
    options = {"config": ["config", "--format", "json"],
               "build": ["build", "backend"],
               "up": ["up", "--detach", "--no-deps", *SERVICES]}
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
    if set(services) != set(SERVICES):
        raise ValueError("effective_services_rejected")
    expected_images = {
        "postgres": "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a",
        "mosquitto": DEFAULTS["MQTT_DYNSEC_IMAGE_REFERENCE"],
        "nginx": "nginx:1.25-alpine", "supabase-auth": "supabase/gotrue:v2.196.0",
        "supabase-envoy": "envoyproxy/envoy:v1.39.1"}
    for role in ("supabase-role-provisioner", "application-migrations", "supabase-compat-migration"):
        expected_images[role] = expected_images["postgres"]
    binds = {
        "nginx": {str(ROOT / "config/nginx/nginx.conf.template"): "/etc/nginx/templates/default.conf.template"},
        "postgres": {str(ROOT / "migrations"): "/docker-entrypoint-initdb.d"},
        "application-migrations": {str(ROOT / "migrations"): "/migrations", str(ROOT / "scripts/run-migrations.sh"): "/run-migrations.sh"},
        "supabase-role-provisioner": {str(ROOT / "migrations/000000_configure_supabase_roles.sh"): "/migrations/000000_configure_supabase_roles.sh"},
        "supabase-compat-migration": {str(ROOT / "migrations/000004_supabase_compat.up.sql"): "/migrations/000004_supabase_compat.up.sql"},
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
        "supabase-auth": {"supabase-role-provisioner": "service_completed_successfully"},
        "supabase-compat-migration": {"postgres": "service_healthy", "supabase-auth": "service_healthy", "application-migrations": "service_completed_successfully"},
        "backend": {"postgres": "service_healthy", "mosquitto": "service_started", "application-migrations": "service_completed_successfully", "supabase-compat-migration": "service_completed_successfully"},
        "supabase-envoy": {"supabase-auth": "service_healthy"},
        "nginx": {"backend": "service_started", "supabase-envoy": "service_started"}}
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
                    or str(ports[0].get("published")) != "0" or ports[0].get("target") != 80
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
                "supabase-compat-migration": ["/bin/sh", "-ec", 'psql -h postgres -U "postgres" -d "smoke" -f /migrations/000004_supabase_compat.up.sql']}
    if any(services[name].get("command") != command for name, command in commands.items()):
        raise ValueError("effective_migrations_rejected")
    volumes = model.get("volumes", {})
    if set(volumes) != {"postgres_data", "mosquitto_data", "mosquitto_dynsec"} or set(model.get("networks", {})) != {"iot_net"}:
        raise ValueError("effective_resources_rejected")
    for name, role in (("postgres_data", "postgres_data"), ("mosquitto_data", "mosquitto_data"), ("mosquitto_dynsec", "store")):
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


JOBS = {'supabase-role-provisioner', 'application-migrations', 'supabase-compat-migration'}


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
                for name in SERVICES:
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
                        ready = startup_state(name, obj['State']) if name in JOBS or name in ('postgres', 'supabase-auth', 'backend') else probe_startup(registry, name, obj)
                        if ready:
                            evidence[name] = {'image_id': obj['Image'], 'id': obj['Id'], 'exit': obj['State'].get('ExitCode'), 'health': obj['State'].get('Health', {}).get('Status', 'probe_ready')}
                            break
                        if time.monotonic() >= deadline:
                            raise ValueError('phase_health_timeout')
                        time.sleep(1)
                    if name == 'application-migrations':
                        sql = "SELECT string_agg(version::text || ':' || name, ',' ORDER BY version) FROM schema_migrations"
                        result = registry.command(['exec', containers['postgres']['receipt']['id'], 'psql', '-U', 'postgres', '-d', 'smoke', '-At', '-c', sql])
                        expected = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql') for p in sorted((ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
                        if result.returncode or result.stdout.decode().strip() != expected or not expected.endswith('19:mqtt_simplified_journal'):
                            raise ValueError('schema19_exact_rejected')
                        evidence['schema19'] = True
                    if name == 'supabase-compat-migration':
                        # Root psql command lacks ON_ERROR_STOP; exit0 alone cannot
                        # prove that its FK statements actually took effect.
                        sql = "SELECT count(*) FROM pg_constraint WHERE conname = 'profiles_id_fkey' AND conrelid = 'public.profiles'::regclass AND confrelid = 'auth.users'::regclass AND contype = 'f'"
                        result = registry.command(['exec', containers['postgres']['receipt']['id'], 'psql', '-U', 'postgres', '-d', 'smoke', '-At', '-c', sql])
                        if result.returncode or result.stdout.strip() != b'1':
                            raise ValueError('compat_constraints_rejected')
                    durable_json(fixture.private / 'startup-progress.json', {'phase': phase, 'evidence': evidence})
                obj = registry.resolve(containers['nginx'])
                ports = obj['NetworkSettings']['Ports'].get('80/tcp') or []
                if len(ports) != 1 or ports[0]['HostIp'] != '127.0.0.1' or not 0 < int(ports[0]['HostPort']) < 65536:
                    raise ValueError('mapped_endpoint_rejected')
                endpoint = 'http://127.0.0.1:' + ports[0]['HostPort']
                evidence['endpoint'] = endpoint
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
                try:
                    if isinstance(failure, lifecycle.IncompleteCleanup):
                        raise ValueError('child_cleanup_unresolved')
                    if registry.items:
                        lifecycle.reconcile_child_receipts(fixture.private, registry.items[0]['name'], registry.items[0]['labels'])
                    # Consumers first, then infrastructure, then inputs. No down/prune.
                    ordered = sorted(reversed(registry.items), key=lambda i: {'container': 0, 'network': 1, 'volume': 2}[i['kind']])
                    cleanup_deadline = time.monotonic() + BUDGETS['cleanup'] * 2
                    for item in ordered:
                        if time.monotonic() >= cleanup_deadline:
                            raise ValueError('startup_cleanup_deadline')
                        if item['allocation_attempted']:
                            registry.remove(item)
                    cleanup_verified = True
                except Exception as cl_exc:
                    cleanup_verified = False
                    try:
                        durable_json(fixture.artifacts / 'startup-summary.json', {'status': 'CLEANUP_UNRESOLVED', 'phase': phase, 'http_accepted': False})
                    except Exception:
                        pass
                    cleanup_failure = ValueError('startup_cleanup_unresolved_evidence_retained')
                    fail_report = record_failure_report(fixture, phase="cleanup", exc=cleanup_failure, cleanup_verified=False)
                    raise FailureDiagnostic(fail_report, cause=cleanup_failure) from cl_exc
                if state.signum is not None:
                    failure = lifecycle.RunnerInterrupted(state.signum)
    except FailureDiagnostic:
        raise
    except BaseException as exc:
        failure = exc

    cleanup_counts = {k: sum(i['kind'] == k and i.get('removed', False) for i in registry.items) for k in ('container', 'network', 'volume')}
    if failure:
        fail_report = record_failure_report(fixture, phase=phase, exc=failure, cleanup_verified=cleanup_verified)
        raise FailureDiagnostic(fail_report, cause=failure) from failure

    try:
        if full_http:
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
           "real_migrations_exit_zero_and_schema19", "dockerfile_backend_image",
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
    *SERVICES,
    "positive_http", "role_http", "jwt_http",
    "cleanup", "report_validation"
}
ALLOWED_FAILURE_ERROR_TYPES = {
    "ValueError", "RuntimeError", "OSError", "TimeoutExpired",
    "RunnerInterrupted", "IncompleteCleanup", "Exception"
}
ALLOWED_FAILURE_ERROR_CODES = {
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
    "resource_cleanup_unresolved", "anonymous_volume_prevention_failed",
    "compose_phase_unresolved", "compose_phase_failed",
    "phase_container_absent", "phase_health_timeout",
    "health_failed", "migration_nonzero",
    "runtime_receipt_rejected", "schema19_exact_rejected",
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

FALLBACK_FAILURE_DIAGNOSTIC = {
    "schema_version": 1,
    "status": "STARTUP_FAILED",
    "phase": "preflight",
    "error_type": "ValueError",
    "error_code": "unsafe_failure_report",
    "http_accepted": False,
    "cleanup_verified": False,
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
    required_keys = {"schema_version", "status", "phase", "error_type", "error_code",
                     "http_accepted", "cleanup_verified"}
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
    if report.get("phase") not in ALLOWED_FAILURE_PHASES:
        raise ValueError("unsafe_failure_report")
    if report.get("error_type") not in ALLOWED_FAILURE_ERROR_TYPES:
        raise ValueError("unsafe_failure_report")
    if report.get("error_code") not in ALLOWED_FAILURE_ERROR_CODES:
        raise ValueError("unsafe_failure_report")
    if type(report.get("http_accepted")) is not bool or report.get("http_accepted") is not False:
        raise ValueError("unsafe_failure_report")
    if type(report.get("cleanup_verified")) is not bool:
        raise ValueError("unsafe_failure_report")
    assert_safe_data(report)


def sanitize_failure_phase(phase: str, code: str = "") -> str:
    if type(phase) is str and phase in ALLOWED_FAILURE_PHASES:
        return phase
    return "preflight"


def record_failure_report(fixture, phase: str, exc: BaseException, cleanup_verified: bool = False) -> dict:
    safe_code = sanitize_failure_code(exc)
    safe_phase = sanitize_failure_phase(phase, safe_code)
    safe_type = sanitize_failure_type(exc)
    report = {
        "schema_version": 1,
        "status": "STARTUP_FAILED",
        "phase": safe_phase,
        "error_type": safe_type,
        "error_code": safe_code,
        "http_accepted": False,
        "cleanup_verified": True if cleanup_verified is True else False,
    }
    validate_failure_report(report)
    if is_fixture_verified(fixture):
        try:
            durable_json(fixture.artifacts / "smoke-failure.json", report)
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


def http_request(base_url: str, method: str, path: str, *, token: str = "", api_key: str = "",
                 headers: dict | None = None, body: dict | None = None, timeout: float = 10.0) -> tuple[int, dict, dict | str]:
    parsed_url = urllib.parse.urlparse(base_url)
    host = parsed_url.hostname or "127.0.0.1"
    port = parsed_url.port or (443 if parsed_url.scheme == "https" else 80)
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
        parsed_body = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
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


def cli_arguments():
    return ("--prepare", "--startup-only", "--positive-http-only", "--role-http-only", "--full-http")


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
    args = parser.parse_args()
    fixture = None
    try:
        fixture = prepare(args.prepare)
    except BaseException as exc:
        report = {
            "schema_version": 1,
            "status": "STARTUP_FAILED",
            "phase": "prepare",
            "error_type": sanitize_failure_type(exc),
            "error_code": sanitize_failure_code(exc),
            "http_accepted": False,
            "cleanup_verified": False,
        }
        try:
            validate_failure_report(report)
            payload = json.dumps(report)
        except BaseException:
            payload = json.dumps(FALLBACK_FAILURE_DIAGNOSTIC)
        print(payload)
        return 1

    try:
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
