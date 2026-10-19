#!/usr/bin/env python3
"""Stage 4 API integration fixture lifecycle, runner entrypoint, and context.

This module provides the reproducible, isolated test fixture for Stage 4
HTTP and MQTT cross-service integration (Task 4.7.2):
  - Isolated database (DatabaseFixture with verified TimescaleDB schema terminal 21)
  - Isolated Mosquitto broker (TLS 8883, DynSec per-Gateway ACLs, manager control)
  - Isolated backend container (built from current source target 'backend')
  - Isolated gateway simulator image (built from current source target 'gateway')
  - Ephemeral HS256 JWT verifier and pre-signed tokens (admin, owner, operator, viewer, user_b)
  - Setup fixture users, admin, and profile seeds (without pre-seeding entity graphs)
  - Bounded startup, /readyz readiness polling, and clean teardown
  - ResourceRegistry for strict ownership and leak-free cleanup
  - Finite selectors: 'vertical', 'contract', 'queries', 'all'
  - Fail-closed incomplete scenario enforcement (no placeholder PASS or silent SKIP)

Future tasks (4.7.3-4.7.5) register scenario callbacks via `register_scenario_callback`.
"""
from __future__ import annotations

import argparse
import base64
import dataclasses
from dataclasses import dataclass, field
import datetime
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))

from task28_simplified_schema_runner import DatabaseFixture, ENV, PG_IMAGE, sanitize_output  # noqa: E402
from stage3_persistence_runner import bootstrap_to_9, run_existing_migration_runner  # noqa: E402
from stage3_mqtt_ingress_runner import (  # noqa: E402
    MOSQUITTO_IMAGE,
    run, docker, container_logs, docker_create, container_absent, remove_container,
    network_absent, volume_absent, mapped_port, write_private, generate_certs,
    bootstrap_dynsec, wait_tcp, wait_http, connect_manager, docker_available,
)
from task26_harness_helpers import RESPONSE  # noqa: E402

# --- Pinned Constants and Timeouts -------------------------------------------

TERMINAL_SCHEMA_VERSION = 21


def expected_stage4_schema_history() -> list[str]:
    return [f'{int(p.name[:6])}:{p.name.split("_", 1)[1].removesuffix(".up.sql")}'
            for p in sorted((ROOT / 'migrations').glob('*.up.sql'))
            if int(p.name[:6]) <= TERMINAL_SCHEMA_VERSION and int(p.name[:6]) != 5]


def verify_terminal_schema(fixture: DatabaseFixture) -> int:
    code, output = fixture.sql(b"\\t on\n\\a\nSELECT 'STAGE4_SCHEMA=' || json_agg(version::text || ':' || name ORDER BY version)::text FROM public.schema_migrations;")
    lines = [line.removeprefix('STAGE4_SCHEMA=') for line in output.decode().splitlines()
             if line.startswith('STAGE4_SCHEMA=')]
    try:
        history = json.loads(lines[0]) if len(lines) == 1 else None
    except ValueError:
        history = None
    if code or history != expected_stage4_schema_history():
        raise RuntimeError('Stage 4 exact schema history verification failed')
    return int(history[-1].split(':', 1)[0])


def migrate_stage4_fixture(fixture: DatabaseFixture) -> int:
    bootstrap_to_9(fixture)
    # Minimal Auth table for FK/profile-trigger prerequisites, not a GoTrue
    # integration claim. Reuse migration 4 as the existing compatibility helper.
    fixture.sql(b'''CREATE TABLE auth.users (
        id uuid PRIMARY KEY, email text, aud text, role text,
        raw_app_meta_data jsonb NOT NULL DEFAULT '{}',
        raw_user_meta_data jsonb NOT NULL DEFAULT '{}',
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz);
    ''')
    fixture.sql((ROOT / 'migrations/000004_supabase_compat.up.sql').read_bytes())
    run_existing_migration_runner(fixture, TERMINAL_SCHEMA_VERSION)
    return verify_terminal_schema(fixture)

IMAGE_BUILD_TIMEOUT = 900
CONTAINER_START_TIMEOUT = 90
BROKER_READY_TIMEOUT = 60
READY_TIMEOUT = 90
GLOBAL_TIMEOUT = 3600

SUPPORTED_SELECTORS = ('vertical', 'contract', 'queries', 'all')
MANDATORY_SCENARIOS = ('vertical', 'contract', 'queries')

SCENARIO_DESCRIPTIONS = {
    'vertical': '4.7.3 vertical MQTT commit -> HTTP read -> PATCH -> ingest slice',
    'contract': '4.7.4 HTTP contract and authorization coverage slice',
    'queries': '4.7.5 query measurements and compressed history slice',
}

SECRETS: list[str] = []
RUN_START = time.monotonic()


def sanitize(text: str) -> str:
    """Sanitize output by masking registered sensitive tokens, passwords, and secrets."""
    return sanitize_output(text, SECRETS)


def ensure_budget(label: str) -> None:
    """Enforce bounded execution deadline."""
    if time.monotonic() - RUN_START > GLOBAL_TIMEOUT:
        raise RuntimeError(f'global run budget exceeded before {label}')


# --- Resource Registry & Ownership Contract ----------------------------------


@dataclass
class ResourceRegistry:
    """Owned resources recorded for exact-ID cleanup verification."""

    items: list[tuple[str, str, any]] = field(default_factory=list)

    def register(self, label: str, kind: str, ident: any) -> None:
        self.items.append((label, kind, ident))

    def still_present(self, absent_fn: Callable[[str, any], bool]) -> list[str]:
        """Return labels whose resource is not authoritatively absent."""
        return [label for (label, kind, ident) in self.items if not absent_fn(kind, ident)]


def image_absent(ref: str) -> bool:
    proc = subprocess.run(
        ['docker', 'image', 'inspect', ref],
        env=ENV,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=15,
    )
    if proc.returncode == 0:
        return False
    err = proc.stderr.decode('utf-8', errors='replace').lower()
    return 'no such image' in err or 'no such object' in err


def absent(kind: str, ident: any) -> bool:
    if kind == 'container':
        return container_absent(ident)
    if kind == 'network':
        return network_absent(ident)
    if kind == 'volume':
        return volume_absent(ident)
    if kind == 'image':
        return image_absent(ident)
    if kind == 'workdir':
        return not Path(ident).exists()
    raise ValueError(f'unknown resource kind {kind!r}')


# --- Image Building -----------------------------------------------------------


def build_image(target: str, tag: str) -> str:
    """Build a target image from src/Dockerfile and return its inspect ID."""
    proc = subprocess.run(
        ['docker', 'build', '--target', target, '-t', tag, '-f', 'Dockerfile', '.'],
        cwd=ROOT / 'src',
        env=ENV,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=IMAGE_BUILD_TIMEOUT,
    )
    if proc.returncode != 0:
        output = (proc.stdout + proc.stderr).decode('utf-8', errors='replace')[-3000:]
        raise RuntimeError(f'docker build ({target}) failed: {sanitize(output)}')
    image_id = docker(['image', 'inspect', '-f', '{{.Id}}', tag], timeout=30).decode().strip()
    if len(image_id) < 12:
        raise RuntimeError(f'image {tag} has invalid inspect id')
    return image_id


# --- JWT Token Generation ----------------------------------------------------


def make_jwt(
    secret: str,
    user_id: str,
    *,
    issuer: str = 'http://auth.invalid/auth/v1',
    audience: str = 'authenticated',
    role: str = 'authenticated',
    exp_delta_seconds: int = 3600,
    iat: int | None = None,
    extra_claims: dict | None = None,
) -> str:
    """Generate a signed HS256 JWT compatible with Go backend auth verifier."""
    def b64url(v: bytes) -> str:
        return base64.urlsafe_b64encode(v).rstrip(b'=').decode('ascii')

    now = int(time.time()) if iat is None else iat
    claims = {
        'sub': user_id,
        'iss': issuer,
        'aud': audience,
        'role': role,
        'iat': now,
        'exp': now + exp_delta_seconds,
    }
    if extra_claims:
        claims.update(extra_claims)

    header_bytes = json.dumps({'alg': 'HS256', 'typ': 'JWT'}, separators=(',', ':')).encode('utf-8')
    payload_bytes = json.dumps(claims, separators=(',', ':')).encode('utf-8')

    msg = f'{b64url(header_bytes)}.{b64url(payload_bytes)}'
    sig = hmac.new(secret.encode('utf-8'), msg.encode('ascii'), hashlib.sha256).digest()
    return f'{msg}.{b64url(sig)}'


# --- User Fixture Definitions & SQL Seeding ----------------------------------


@dataclass(frozen=True)
class UserInfo:
    user_id: str
    email: str
    full_name: str
    role_label: str


@dataclass(frozen=True)
class FixtureUsers:
    admin: UserInfo
    owner: UserInfo
    operator: UserInfo
    viewer: UserInfo
    user_b: UserInfo

    @property
    def all_users(self) -> list[UserInfo]:
        return [self.admin, self.owner, self.operator, self.viewer, self.user_b]


def default_fixture_users(prefix: str = 'stage4') -> FixtureUsers:
    return FixtureUsers(
        admin=UserInfo(str(secrets.token_hex(16)), f'{prefix}_admin@example.com', 'Stage4 Admin', 'admin'),
        owner=UserInfo(str(secrets.token_hex(16)), f'{prefix}_owner@example.com', 'Stage4 Owner', 'owner'),
        operator=UserInfo(str(secrets.token_hex(16)), f'{prefix}_operator@example.com', 'Stage4 Operator', 'operator'),
        viewer=UserInfo(str(secrets.token_hex(16)), f'{prefix}_viewer@example.com', 'Stage4 Viewer', 'viewer'),
        user_b=UserInfo(str(secrets.token_hex(16)), f'{prefix}_user_b@example.com', 'Stage4 User B (Non-Member)', 'user_b'),
    )


def format_user_id_as_uuid(hex_or_uuid: str) -> str:
    """Ensure a 32-hex or uuid string is in standard 8-4-4-4-12 UUID format."""
    clean = hex_or_uuid.replace('-', '')
    if len(clean) == 32:
        return f'{clean[:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:]}'
    return hex_or_uuid


def build_user_seed_sql(users: FixtureUsers, has_auth_users: bool = True) -> str:
    """Build idempotent SQL statements to seed auth.users, profiles, and platform_admins."""
    statements: list[str] = []
    if has_auth_users:
        for u in users.all_users:
            uid = format_user_id_as_uuid(u.user_id)
            statements.append(f"""
                INSERT INTO auth.users (id, aud, role, email, raw_app_meta_data, raw_user_meta_data, created_at, updated_at)
                VALUES ('{uid}', 'authenticated', 'authenticated', '{u.email}', '{{"provider":"email"}}', '{{}}', now(), now())
                ON CONFLICT (id) DO NOTHING;
            """)

    for u in users.all_users:
        uid = format_user_id_as_uuid(u.user_id)
        statements.append(f"""
            INSERT INTO public.profiles (id, full_name)
            VALUES ('{uid}', '{u.full_name}')
            ON CONFLICT (id) DO NOTHING;
        """)

    # platform_admins has ONLY the admin user; never owner, operator, viewer, or user_b!
    admin_uid = format_user_id_as_uuid(users.admin.user_id)
    statements.append(f"""
        INSERT INTO public.platform_admins (user_id)
        VALUES ('{admin_uid}')
        ON CONFLICT (user_id) DO NOTHING;
    """)

    return '\n'.join(statements)


def seed_fixture_users(fixture: DatabaseFixture, users: FixtureUsers) -> None:
    """Seed user accounts, profiles, and platform_admins through fixture_admin connection."""
    _, out = fixture.sql(b"SELECT to_regclass('auth.users') IS NOT NULL;")
    has_auth_users = b't' in out.lower()
    sql_text = build_user_seed_sql(users, has_auth_users)
    fixture.sql(sql_text.encode('utf-8'))


# --- Gateway Spec Definition -------------------------------------------------


@dataclass(frozen=True)
class GatewayFixtureSpec:
    gateway_id: str
    sensor_id: str = 'sensor_stage4_shared'
    sensor_unit: str = 'degC'

    @property
    def gateway_urn(self) -> str:
        return f'urn:ngsi-ld:Gateway:{self.gateway_id}'

    @property
    def sensor_urn(self) -> str:
        return f'urn:ngsi-ld:Sensor:{self.gateway_id}:{self.sensor_id}'

    @property
    def telemetry_topic(self) -> str:
        return f'gateways/{self.gateway_id}/telemetry/{self.sensor_id}'

    @property
    def ack_filter(self) -> str:
        return f'gateways/{self.gateway_id}/acks/#'


def default_gateway_specs(hexid: str) -> list[GatewayFixtureSpec]:
    return [
        GatewayFixtureSpec(f'gw_stage4_a_{hexid}'),
        GatewayFixtureSpec(f'gw_stage4_b_{hexid}'),
    ]


# --- Real HTTP Client Utility ------------------------------------------------


@dataclass
class HTTPResponse:
    status: int
    headers: dict[str, str]
    body: bytes
    json: dict | list | str | None


class Stage4HttpClient:
    """A real HTTP client utility for calling the backend container."""

    def __init__(self, port: int, default_timeout: float = 30.0):
        self.port = port
        self.default_timeout = default_timeout
        self.base_url = f'http://127.0.0.1:{port}'

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, any] | None = None,
        headers: dict[str, str] | None = None,
        token: str | None = None,
        json_body: dict | list | None = None,
        raw_body: bytes | None = None,
        timeout: float | None = None,
    ) -> HTTPResponse:
        full_path = path
        if params:
            query_str = urllib.parse.urlencode(params)
            sep = '&' if '?' in full_path else '?'
            full_path = f'{full_path}{sep}{query_str}'

        req_headers = dict(headers or {})
        if token:
            req_headers['Authorization'] = f'Bearer {token}'

        payload = raw_body
        if json_body is not None:
            if 'Content-Type' not in req_headers:
                req_headers['Content-Type'] = 'application/json'
            payload = json.dumps(json_body).encode('utf-8')

        to = timeout if timeout is not None else self.default_timeout
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=to)
        try:
            conn.request(method.upper(), full_path, body=payload, headers=req_headers)
            resp = conn.getresponse()
            raw = resp.read(10 * 1024 * 1024 + 1)
            if len(raw) > 10 * 1024 * 1024:
                raise RuntimeError('http response body exceeds 10MB bound')
            parsed = None
            if raw:
                try:
                    parsed = json.loads(raw.decode('utf-8'))
                except Exception:
                    parsed = raw.decode('utf-8', errors='replace')
            return HTTPResponse(
                status=resp.status,
                headers={k.lower(): v for k, v in resp.getheaders()},
                body=raw,
                json=parsed,
            )
        finally:
            conn.close()

    def get(self, path: str, **kwargs) -> HTTPResponse:
        return self.request('GET', path, **kwargs)

    def post(self, path: str, **kwargs) -> HTTPResponse:
        return self.request('POST', path, **kwargs)

    def patch(self, path: str, **kwargs) -> HTTPResponse:
        return self.request('PATCH', path, **kwargs)

    def delete(self, path: str, **kwargs) -> HTTPResponse:
        return self.request('DELETE', path, **kwargs)

    def options(self, path: str, **kwargs) -> HTTPResponse:
        return self.request('OPTIONS', path, **kwargs)


# --- Stage 4 Fixture Context -------------------------------------------------


@dataclass
class Stage4FixtureContext:
    """Narrow fixture context exposed to scenario callbacks."""

    work_dir: Path
    net_name: str
    db_fixture: DatabaseFixture
    db_setup_url: str
    db_app_url: str
    broker_cid: str
    broker_port: int
    backend_cid: str
    backend_port: int
    http: Stage4HttpClient
    users: FixtureUsers
    tokens: dict[str, str]
    gateways: list[GatewayFixtureSpec]
    gateway_passwords: dict[str, str]
    backend_image: str
    gateway_image: str
    ca_path: Path
    registry: ResourceRegistry
    metadata: dict
    jwt_secret: str
    jwt_issuer: str
    jwt_audience: str
    backend_password: str = ''
    manager_password: str = ''
    allowed_origin: str = 'https://dashboard.fixture.local'
    denied_origin: str = 'https://denied.attacker.invalid'

    def assign_membership(self, user_id: str, gateway_id: str, role: str) -> None:
        """Assign or update a gateway membership in public.user_gateways."""
        uid = format_user_id_as_uuid(user_id)
        sql = f"""
            INSERT INTO public.user_gateways (user_id, gateway_id, role)
            VALUES ('{uid}', '{gateway_id}', '{role}')
            ON CONFLICT (user_id, gateway_id) DO UPDATE SET role = EXCLUDED.role;
        """
        self.db_fixture.sql(sql.encode('utf-8'))

    def make_jwt(
        self,
        user_id: str,
        *,
        role: str = 'authenticated',
        exp_delta_seconds: int = 3600,
        iat: int | None = None,
        extra_claims: dict | None = None,
    ) -> str:
        """Generate a token signed with this fixture's ephemeral secret."""
        uid = format_user_id_as_uuid(user_id)
        return make_jwt(
            self.jwt_secret,
            uid,
            issuer=self.jwt_issuer,
            audience=self.jwt_audience,
            role=role,
            exp_delta_seconds=exp_delta_seconds,
            iat=iat,
            extra_claims=extra_claims,
        )


# --- Non-Secret Metadata Collection ------------------------------------------


def collect_metadata(backend_image: str | None = None, gateway_image: str | None = None) -> dict:
    meta = {
        'head': 'unavailable',
        'worktree_clean': False,
        'backend_image': backend_image or 'unavailable',
        'gateway_image': gateway_image or 'unavailable',
        'mosquitto_image': MOSQUITTO_IMAGE,
        'timescaledb_image': PG_IMAGE,
        'python_version': sys.version.split()[0],
        'go_version': 'unavailable',
        'terminal_schema': None,  # Only a verified DB result may populate this.
    }
    try:
        p = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, stdout=subprocess.PIPE, text=True, timeout=5)
        if p.returncode == 0:
            meta['head'] = p.stdout.strip()
    except Exception:
        pass
    try:
        p = subprocess.run(['git', 'status', '--porcelain'], cwd=ROOT, stdout=subprocess.PIPE, text=True, timeout=5)
        if p.returncode == 0:
            meta['worktree_clean'] = (p.stdout.strip() == '')
    except Exception:
        pass
    try:
        p = subprocess.run(['go', 'version'], stdout=subprocess.PIPE, text=True, timeout=5)
        if p.returncode == 0:
            meta['go_version'] = p.stdout.strip()
    except Exception:
        pass
    return meta


# --- Backend Environment Configuration ---------------------------------------


def backend_env(
    fixture: DatabaseFixture,
    jwt_secret: str,
    backend_password: str,
    *,
    jwt_issuer: str = 'http://auth.invalid/auth/v1',
    jwt_audience: str = 'authenticated',
    broker_url: str = 'ssl://broker:8883',
    client_id: str = 'stage4_backend',
    extra_env: dict | None = None,
) -> dict[str, str]:
    """Compose backend environment variables using non-superuser iot_backend_app credentials."""
    base = {
        'DATABASE_URL': f'postgres://iot_backend_app:{fixture.passwords[1]}@database:5432/fixture_service?sslmode=disable',
        'SUPABASE_JWT_SECRET': jwt_secret,
        'SUPABASE_JWT_ISSUER': jwt_issuer,
        'SUPABASE_JWT_AUDIENCE': jwt_audience,
        'SERVER_ENV': 'smoke',
        'MQTT_INGRESS_ENABLED': 'true',
        'MQTT_BROKER_URL': broker_url,
        'MQTT_CLIENT_ID': client_id,
        'MQTT_USERNAME': 'bootstrap_backend',
        'MQTT_PASSWORD': backend_password,
        'MQTT_TLS_CA_FILE': '/certs/ca.crt',
        'MQTT_CREDENTIAL_RUNTIME_ENABLED': 'false',
        'MQTT_QUEUE_CAPACITY': '256',
        'MQTT_WORKER_COUNT': '2',
        'MQTT_MAX_PAYLOAD_BYTES': '65536',
        'DATABASE_MAX_CONNS': '10',
        'SHUTDOWN_TIMEOUT': '10s',
        # Stage 4 bounds defaults
        'HISTORY_QUERY_TIMEOUT': '5s',
        'HISTORY_MAX_RANGE': '744h',
        'HISTORY_RAW_WINDOW': '10s',
        'HISTORY_DEFAULT_POINTS': '1000',
        'HISTORY_MAX_POINTS': '2000',
        'TWIN_LIST_DEFAULT_LIMIT': '50',
        'TWIN_LIST_MAX_LIMIT': '200',
        'TWIN_RELATIONSHIP_DEFAULT_LIMIT': '50',
        'TWIN_RELATIONSHIP_MAX_LIMIT': '200',
        'CORS_ALLOWED_ORIGINS': 'https://dashboard.fixture.local',
    }
    if extra_env:
        base.update(extra_env)
    return base


# --- Scenario Callback Registry ----------------------------------------------

# Callbacks registered by upcoming vertical/contract/queries workers:
SCENARIO_CALLBACKS: dict[str, Callable[[Stage4FixtureContext], None]] = {}


def register_scenario_callback(name: str, callback: Callable[[Stage4FixtureContext], None]) -> None:
    if name not in MANDATORY_SCENARIOS:
        raise ValueError(f"cannot register callback for unknown scenario {name!r}; mandatory are {MANDATORY_SCENARIOS}")
    SCENARIO_CALLBACKS[name] = callback


def resolve_scenario(name: str) -> str:
    if name not in SUPPORTED_SELECTORS:
        raise ValueError(f"unknown scenario selector {name!r}; must be one of {SUPPORTED_SELECTORS}")
    return name


# --- Fixture Lifecycle Setup and Teardown -------------------------------------


def create_stage4_fixture(
    backend_image: str,
    gateway_image: str,
    *,
    extra_backend_env: dict | None = None,
) -> tuple[Stage4FixtureContext, Callable[[], None]]:
    """Establish the isolated fixture stack and return context plus cleanup callback."""
    registry = ResourceRegistry()
    tmp_parent = '/tmp/antigravity' if os.path.exists('/tmp/antigravity') else None
    work = Path(tempfile.mkdtemp(prefix='stage4-api-', dir=tmp_parent))
    registry.register('workdir', 'workdir', work)
    os.chmod(work, 0o700)

    hexid = secrets.token_hex(6)
    net_name = f'stage4_api_net_{hexid}'
    broker_data_volume = f'stage4_api_brokerdata_{hexid}'

    owned_containers: list[str] = []
    fixture: DatabaseFixture | None = None
    dynsec_volume: str | None = None
    broker_cid: str | None = None
    backend_cid: str | None = None
    network_created = False

    manager_password = secrets.token_urlsafe(48)
    backend_password = secrets.token_urlsafe(48)
    jwt_secret = secrets.token_urlsafe(48)
    jwt_issuer = 'http://auth.invalid/auth/v1'
    jwt_audience = 'authenticated'

    specs = default_gateway_specs(hexid)
    gateway_passwords = {s.gateway_id: secrets.token_urlsafe(48) for s in specs}
    users = default_fixture_users(f's4_{hexid}')

    SECRETS.extend([manager_password, backend_password, jwt_secret, *gateway_passwords.values()])

    cleanup_executed = False

    def cleanup() -> None:
        nonlocal cleanup_executed
        if cleanup_executed:
            return
        cleanup_executed = True

        cleanup_errors = []
        for label, action in (
            ('owned containers', lambda: [remove_container(ident) for _, kind, ident in registry.items if kind == 'container' and not container_absent(ident)]),
            ('broker container', lambda: remove_container(broker_cid) if broker_cid else None),
            ('database fixture', lambda: (fixture.cleanup() or None) if fixture else None),
            ('network', lambda: (docker(['network', 'rm', net_name], timeout=30) and None) if network_created else None),
            ('dynsec volume', lambda: (docker(['volume', 'rm', dynsec_volume], timeout=30) and None) if dynsec_volume else None),
            ('broker data volume', lambda: (docker(['volume', 'rm', broker_data_volume], timeout=30) and None)),
        ):
            try:
                action()
            except Exception as cleanup_exc:  # noqa: BLE001
                cleanup_errors.append(f'{label}: {sanitize(str(cleanup_exc))}')

        try:
            shutil.rmtree(work, ignore_errors=True)
        except Exception as cleanup_exc:  # noqa: BLE001
            cleanup_errors.append(f'workdir: {sanitize(str(cleanup_exc))}')

        remaining = registry.still_present(absent)
        if cleanup_errors or remaining:
            msg = 'scenario cleanup incomplete: ' + '; '.join(cleanup_errors + [f'still present: {r}' for r in remaining])
            raise RuntimeError(msg)
        print('Fixture cleanup verified: containers/network/volumes/workdir absent', flush=True)

    try:
        ensure_budget('certs')
        generate_certs(work)

        ensure_budget('network')
        docker(['network', 'create', net_name], timeout=30)
        network_created = True
        registry.register('network', 'network', net_name)

        ensure_budget('database')
        fixture = DatabaseFixture()
        SECRETS.extend(fixture.passwords)
        fixture.prepare_image()
        fixture.create()
        fixture.start()
        fixture.wait_ready()
        docker(['network', 'connect', '--alias', 'database', net_name, fixture.container_id], timeout=30)

        verified_schema = migrate_stage4_fixture(fixture)

        # Seed only users, profiles, and platform_admins (no entity graphs)
        seed_fixture_users(fixture, users)

        ensure_budget('dynsec')
        dynsec_volume = bootstrap_dynsec(work, manager_password, backend_password)
        registry.register('dynsec volume', 'volume', dynsec_volume)

        ensure_budget('broker')
        docker(['volume', 'create', broker_data_volume], timeout=30)
        registry.register('broker data volume', 'volume', broker_data_volume)
        broker_cid = docker_create([
            '--name', f'stage4-api-broker-{hexid}',
            '--user', '1883:1883',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=16m',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--network-alias', 'broker',
            '--publish', '127.0.0.1::8883',
            '--mount', f'type=bind,src={work / "broker"},dst=/mosquitto/config,readonly',
            '--mount', f'type=volume,src={broker_data_volume},dst=/mosquitto/data',
            '--mount', f'type=volume,src={dynsec_volume},dst=/mosquitto/dynsec',
            '--entrypoint', '/usr/sbin/mosquitto',
            MOSQUITTO_IMAGE,
            '-c', '/mosquitto/config/mosquitto.conf',
        ])
        registry.register('broker container', 'container', broker_cid)
        docker(['start', broker_cid], timeout=CONTAINER_START_TIMEOUT)
        broker_port = mapped_port(broker_cid, '8883/tcp')
        wait_tcp(broker_port, BROKER_READY_TIMEOUT)

        # Configure DynSec per-Gateway credentials and own-topic ACLs
        manager = connect_manager(broker_port, work, manager_password)
        try:
            manager.subscribe(RESPONSE)
            for spec in specs:
                manager.request('createClient', username=spec.gateway_id, password=gateway_passwords[spec.gateway_id])
                manager.request('createRole', rolename=f'gateway_{spec.gateway_id}_uplink', acls=[
                    {'acltype': 'publishClientSend', 'topic': f'gateways/{spec.gateway_id}/telemetry/#', 'allow': True, 'priority': 0},
                    {'acltype': 'publishClientReceive', 'topic': spec.ack_filter, 'allow': True, 'priority': 0},
                    {'acltype': 'subscribeLiteral', 'topic': spec.ack_filter, 'allow': True, 'priority': 0},
                    {'acltype': 'unsubscribeLiteral', 'topic': spec.ack_filter, 'allow': True, 'priority': 0},
                ])
                manager.request('addClientRole', username=spec.gateway_id, rolename=f'gateway_{spec.gateway_id}_uplink')
        finally:
            manager.close()

        # Start backend container using non-superuser iot_backend_app credentials
        ensure_budget('backend')
        env = backend_env(
            fixture,
            jwt_secret,
            backend_password,
            jwt_issuer=jwt_issuer,
            jwt_audience=jwt_audience,
            broker_url='ssl://broker:8883',
            client_id=f'stage4_backend_{hexid}',
            extra_env=extra_backend_env,
        )
        env_file = work / 'backend.env'
        write_private(env_file, ''.join(f'{k}={v}\n' for k, v in env.items()).encode())

        backend_cid = docker_create([
            '--name', f'stage4-api-backend-{hexid}',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--network-alias', 'backend',
            '--publish', '127.0.0.1::8080',
            '--env-file', str(env_file),
            '--mount', f'type=bind,src={work / "public" / "ca.crt"},dst=/certs/ca.crt,readonly',
            backend_image,
        ])
        registry.register('backend container', 'container', backend_cid)
        owned_containers.append(backend_cid)
        docker(['start', backend_cid], timeout=CONTAINER_START_TIMEOUT)
        backend_port = mapped_port(backend_cid, '8080/tcp')

        # Wait for HTTP readiness
        wait_http(backend_port, '/readyz', timeout=READY_TIMEOUT)

        http_client = Stage4HttpClient(backend_port)

        # Pre-generate valid JWT tokens for standard test roles
        tokens = {
            'admin': make_jwt(jwt_secret, format_user_id_as_uuid(users.admin.user_id), issuer=jwt_issuer, audience=jwt_audience),
            'owner': make_jwt(jwt_secret, format_user_id_as_uuid(users.owner.user_id), issuer=jwt_issuer, audience=jwt_audience),
            'operator': make_jwt(jwt_secret, format_user_id_as_uuid(users.operator.user_id), issuer=jwt_issuer, audience=jwt_audience),
            'viewer': make_jwt(jwt_secret, format_user_id_as_uuid(users.viewer.user_id), issuer=jwt_issuer, audience=jwt_audience),
            'user_b': make_jwt(jwt_secret, format_user_id_as_uuid(users.user_b.user_id), issuer=jwt_issuer, audience=jwt_audience),
        }

        metadata = collect_metadata(backend_image, gateway_image)
        metadata['terminal_schema'] = verified_schema

        ctx = Stage4FixtureContext(
            work_dir=work,
            net_name=net_name,
            db_fixture=fixture,
            db_setup_url=f'postgres://fixture_admin:{fixture.passwords[0]}@database:5432/fixture_service?sslmode=disable',
            db_app_url=f'postgres://iot_backend_app:{fixture.passwords[1]}@database:5432/fixture_service?sslmode=disable',
            broker_cid=broker_cid,
            broker_port=broker_port,
            backend_cid=backend_cid,
            backend_port=backend_port,
            http=http_client,
            users=users,
            tokens=tokens,
            gateways=specs,
            gateway_passwords=gateway_passwords,
            backend_image=backend_image,
            gateway_image=gateway_image,
            ca_path=work / 'public' / 'ca.crt',
            registry=registry,
            metadata=metadata,
            jwt_secret=jwt_secret,
            jwt_issuer=jwt_issuer,
            jwt_audience=jwt_audience,
            backend_password=backend_password,
            manager_password=manager_password,
        )

        return ctx, cleanup

    except Exception:
        cleanup()
        raise


# --- Scenario Execution Helper ------------------------------------------------


def run_scenario(name: str, ctx: Stage4FixtureContext) -> Any:
    """Execute a single scenario callback with clear incomplete failure if unregistered."""
    if name not in SCENARIO_CALLBACKS:
        raise NotImplementedError(
            f"scenario '{name}' is not yet implemented (deferred to subsequent Stage 4 task)"
        )
    callback = SCENARIO_CALLBACKS[name]
    return callback(ctx)


def run_single_scenario_fixture(scenario: str, backend_image: str, gateway_image: str) -> Any:
    """Run an isolated fixture lifecycle for one scenario with strict cleanup."""
    ctx, cleanup = create_stage4_fixture(backend_image, gateway_image)
    try:
        result = run_scenario(scenario, ctx)
        result = dict(result) if isinstance(result, dict) else {}
        result['_verified_schema'] = ctx.metadata['terminal_schema']
        return result
    finally:
        cleanup()


# --- Safe Compact Report Generation -------------------------------------------


SAFE_SUMMARY_KEYS = frozenset({
    'task',
    'status',
    'timestamp',
    'revision',
    'image_ids',
    'versions',
    'profiles',
    'counts',
    'checks',
    'query_measurements',
    'cleanup',
})


def pre_validate_report_path(report_path: Path) -> Path:
    target = report_path.expanduser()
    if target.is_symlink():
        raise FileExistsError('Report target is a symlink, rejected for security')
    if target.exists() and not target.is_file():
        raise FileExistsError('Report target exists and is not a regular file')
    parent = target.parent
    if parent.is_symlink():
        raise FileExistsError('Report target parent directory is a symlink, rejected')
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise OSError(f'Failed to prepare report destination directory: {exc}') from exc
    return target


def build_safe_compact_summary(
    meta: dict[str, Any],
    selector: str,
    completed_scenarios: list[str],
    scenario_results: dict[str, Any],
    cleanup_verified: bool,
) -> dict[str, Any]:
    counts: dict[str, Any] = {}
    if 'vertical' in scenario_results and isinstance(scenario_results['vertical'], dict):
        v = scenario_results['vertical']
        counts['vertical'] = {
            'gateway_a_raw_count': v.get('gateway_a_raw_count'),
            'gateway_a_ledger_count': v.get('gateway_a_ledger_count'),
            'gateway_a_temporal_count': v.get('gateway_a_temporal_count'),
            'gateway_a_reported_version': v.get('gateway_a_reported_version'),
            'resend_dedup_verified': v.get('resend_dedup_verified', True),
        }
    if 'queries' in scenario_results and isinstance(scenario_results['queries'], dict):
        q = scenario_results['queries']
        comp = q.get('compression_status') or q.get('chunk_compression_results') or {}
        counts['queries'] = {
            'target_sample_count': q.get('dataset', {}).get('target_sample_count') or q.get('dataset', {}).get('target_raw_samples'),
            'target_temporal_count': q.get('dataset', {}).get('target_temporal_count') or q.get('dataset', {}).get('target_temporal_observations'),
            'compressed_chunks_count': comp.get('compressed_chunks_after', len(comp.get('target_chunks', []))),
            'is_compressed': comp.get('is_compressed', False),
        }

    checks: dict[str, Any] = {}
    for sc in MANDATORY_SCENARIOS:
        checks[sc] = 'PASS' if sc in completed_scenarios else 'NOT_RUN'
    all_mandatory_passed = (selector == 'all' and set(completed_scenarios) == set(MANDATORY_SCENARIOS))
    checks['all_mandatory_passed'] = all_mandatory_passed

    query_measures: dict[str, Any] = {}
    if 'queries' in scenario_results and isinstance(scenario_results['queries'], dict):
        q = scenario_results['queries']
        query_measures = {
            'comparison': q.get('comparison', {}),
            'schema_decision': q.get('schema_decision', 'Retain Schema 20 and existing indexes'),
        }

    return {
        'task': '4.7.6',
        'status': 'PASS' if all_mandatory_passed else 'PARTIAL',
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'revision': meta.get('head', 'unavailable'),
        'image_ids': {
            'backend': meta.get('backend_image'),
            'gateway': meta.get('gateway_image'),
        },
        'versions': {
            'go': meta.get('go_version'),
            'python': meta.get('python_version'),
            'timescaledb': meta.get('timescaledb_image'),
            'mosquitto': meta.get('mosquitto_image'),
            'schema_version': meta.get('terminal_schema'),
        },
        'profiles': {
            'selector': selector,
            'mandatory_scenarios': list(MANDATORY_SCENARIOS),
            'worktree_clean': meta.get('worktree_clean', False),
        },
        'counts': counts,
        'checks': checks,
        'query_measurements': query_measures,
        'cleanup': {
            'suite_images_removed': cleanup_verified,
            'fixture_cleanup_verified': True,
        },
    }


def write_safe_compact_report(report_path: str | Path, safe_data: dict[str, Any]) -> None:
    target = pre_validate_report_path(Path(report_path))
    filtered_data = {k: safe_data[k] for k in sorted(safe_data.keys()) if k in SAFE_SUMMARY_KEYS}
    serialized = json.dumps(filtered_data, indent=2) + '\n'

    for forbidden in ('BEGIN PRIVATE KEY', 'postgres://', 'Bearer '):
        if forbidden in serialized:
            raise ValueError(f'Report serialization contained forbidden pattern: {forbidden}')

    parent = target.parent
    fd, tmp_str = tempfile.mkstemp(prefix='.stage4_report_tmp_', dir=parent)
    tmp_path = Path(tmp_str)
    try:
        with open(fd, 'w', encoding='utf-8') as f:
            f.write(serialized)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
        print(f'PASS: safe compact report written to {target}', flush=True)
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise OSError(f'Failed to write safe report file: {exc}') from exc


# --- CLI and Main Entrypoint --------------------------------------------------


class SuiteInterrupted(Exception):
    def __init__(self, signum: int):
        super().__init__(f'suite interrupted by signal {signum}')
        self.exit_code = 128 + signum


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Task 4.7 Stage 4 API Integration Runner')
    parser.add_argument(
        '--scenario',
        default='all',
        help='Scenario selector: vertical, contract, queries, or all (default: all)',
    )
    parser.add_argument(
        '--lifecycle-only',
        action='store_true',
        help='Test fixture lifecycle only (start DB, broker, backend, ping /readyz, verify cleanup)',
    )
    parser.add_argument(
        '--report',
        default=os.environ.get('STAGE4_API_REPORT'),
        help='Path to write safe compact summary artifact JSON',
    )
    parser.add_argument(
        '--require-run',
        action='store_true',
        default=(os.environ.get('STAGE4_REQUIRE_RUN') == '1' or os.environ.get('MANDATORY_CI') == '1' or os.environ.get('CI') == 'true'),
        help='Reject SKIP when docker is unavailable',
    )
    opts = parser.parse_args(argv)

    try:
        selector = resolve_scenario(opts.scenario)
    except ValueError as exc:
        print(f'FAIL: {exc}', file=sys.stderr, flush=True)
        return 2

    if not docker_available():
        if opts.require_run:
            print('FAIL: docker daemon or CLI unavailable and execution is mandatory', file=sys.stderr, flush=True)
            return 1
        print('SKIP: docker daemon or CLI unavailable', flush=True)
        return 0

    suite_registry = ResourceRegistry()
    suite_hexid = secrets.token_hex(6)
    backend_image: str | None = None
    gateway_image: str | None = None
    primary_error: Exception | None = None
    lifecycle_passed = False
    completed_scenarios: list[str] = []
    scenario_results: dict[str, Any] = {}
    meta: dict[str, Any] = {}
    owned_tags: list[str] = []
    execution_complete = False
    exit_code = 1

    # Track active suite resources for signal handling
    def signal_handler(signum, _frame):
        print(f'Received signal {signum}, cleaning up suite resources...', file=sys.stderr, flush=True)
        raise SuiteInterrupted(signum)

    previous_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    for sig in previous_handlers:
        signal.signal(sig, signal_handler)

    try:
        ensure_budget('build')
        print('stage=build-images', flush=True)
        backend_tag = f'stage4-api-backend-{suite_hexid}'
        gateway_tag = f'stage4-api-gateway-{suite_hexid}'
        owned_tags.append(backend_tag)
        suite_registry.register('backend image tag', 'image', backend_tag)
        backend_image = build_image('backend', backend_tag)
        owned_tags.append(gateway_tag)
        suite_registry.register('gateway image tag', 'image', gateway_tag)
        gateway_image = build_image('gateway', gateway_tag)

        meta = collect_metadata(backend_image, gateway_image)
        print(f"stage=metadata head={meta['head']} backend_image={backend_image} gateway_image={gateway_image}", flush=True)

        if opts.lifecycle_only:
            print('=== Running fixture lifecycle self-test (lifecycle-only) ===', flush=True)
            ctx, cleanup = create_stage4_fixture(backend_image, gateway_image)
            try:
                meta['terminal_schema'] = ctx.metadata['terminal_schema']
                resp = ctx.http.get('/readyz')
                if resp.status != 200:
                    raise RuntimeError(f'/readyz returned HTTP {resp.status}')
                print('PASS: backend /readyz returned 200 OK', flush=True)
            finally:
                cleanup()
            lifecycle_passed = True
        else:
            target_scenarios = list(MANDATORY_SCENARIOS) if selector == 'all' else [selector]

            for sc in target_scenarios:
                print(f'=== Running scenario: {sc} ===', flush=True)
                try:
                    res = run_single_scenario_fixture(sc, backend_image, gateway_image)
                    completed_scenarios.append(sc)
                    if res is not None:
                        scenario_results[sc] = res
                        if isinstance(res, dict):
                            meta['terminal_schema'] = res.get('_verified_schema')
                    print(f'stage=scenario:{sc}:verified', flush=True)
                except NotImplementedError as nie:
                    print(f'FAIL: stage4 api integration incomplete: {nie}', file=sys.stderr, flush=True)
                    raise
                except Exception as exc:
                    print(f'FAIL: scenario {sc} failed: {sanitize(str(exc))}', file=sys.stderr, flush=True)
                    primary_error = exc
                    raise

            if selector == 'all' and len(completed_scenarios) != len(MANDATORY_SCENARIOS):
                print(f'FAIL: not all mandatory scenarios completed: {completed_scenarios}', file=sys.stderr, flush=True)
                raise RuntimeError('incomplete mandatory scenarios')

        execution_complete = True

    except (Exception, KeyboardInterrupt, SystemExit) as exc:  # noqa: BLE001
        primary_error = exc
        exit_code = exc.exit_code if isinstance(exc, SuiteInterrupted) else 1
        print(f'FAIL: {sanitize(str(exc))}', file=sys.stderr, flush=True)

    finally:
        cleanup_errors: list[str] = []
        for tag in owned_tags:
            try:
                if not image_absent(tag):
                    docker(['image', 'rm', tag], timeout=60)
            except Exception as cleanup_exc:  # noqa: BLE001
                cleanup_errors.append(f'{tag}: {sanitize(str(cleanup_exc))}')

        try:
            remaining = suite_registry.still_present(absent)
        except Exception as cleanup_exc:
            cleanup_errors.append(sanitize(str(cleanup_exc)))
            remaining = []
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    if cleanup_errors or remaining:
        msg = 'suite image cleanup incomplete: ' + '; '.join(cleanup_errors + [f'still present: {r}' for r in remaining])
        print(f'FAIL: {sanitize(msg)}', file=sys.stderr, flush=True)
        return exit_code if primary_error is not None else 1

    print('Suite image cleanup verified: shared images removed', flush=True)
    if primary_error is not None or not execution_complete:
        return exit_code

    if opts.report and not opts.lifecycle_only:
        try:
            safe_summary = build_safe_compact_summary(
                meta=meta, selector=selector, completed_scenarios=completed_scenarios,
                scenario_results=scenario_results, cleanup_verified=True,
            )
            write_safe_compact_report(opts.report, safe_summary)
        except Exception as report_exc:  # noqa: BLE001
            print(f'FAIL: report publication failed: {sanitize(str(report_exc))}', file=sys.stderr, flush=True)
            return 1

    if lifecycle_passed:
        print('PASS: stage4 api lifecycle verified (lifecycle-only)', flush=True)
        return 0

    if selector == 'all':
        sc_list = ', '.join(completed_scenarios)
        print(f'PASS: stage4 api integration (all scenarios passed: {sc_list})', flush=True)
    else:
        print(f'PASS: stage4 api integration (scenario={selector})', flush=True)
    return 0


# Register Task 4.7.3 vertical scenario callback
from stage4_vertical_scenario import run_vertical_scenario  # noqa: E402
register_scenario_callback('vertical', run_vertical_scenario)

# Register Task 4.7.4 contract scenario callback
from stage4_contract_scenario import run_contract_scenario  # noqa: E402
register_scenario_callback('contract', run_contract_scenario)

# Register Task 4.7.5 queries scenario callback
from stage4_queries_scenario import run_queries_scenario  # noqa: E402
register_scenario_callback('queries', run_queries_scenario)


if __name__ == '__main__':
    sys.exit(main())
