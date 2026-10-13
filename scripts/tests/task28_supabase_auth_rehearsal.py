#!/usr/bin/env python3
"""Part 2: Isolated Supabase Auth (GoTrue + Envoy) and Go backend rehearsal.

Runs disposable GoTrue v2.196.0 and Envoy v1.39.1 alongside Mosquitto, PostgreSQL,
Go backend, and Nginx in an isolated network topology. Human tokens are obtained
strictly through real password login via Nginx -> Envoy -> GoTrue.
No fake login tokens, no direct password seeding, and no service_role exposure.
"""
from __future__ import annotations
import base64
import hashlib
import hmac
import http.client
import json
import os
import re
import secrets
import signal
import stat
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stage2_mosquitto_runtime as runtime
from task28_disable_client_probe import (
    DynSecProbeFixture, NativeTargetConnection, probe_raw_connack,
    create_verified_host_fixture_dir, verified_remove_host_fixture_dir,
    inspect_owned_resource, inspect_resource_presence,
)
from task28_simplified_schema_runner import DatabaseFixture, PG_IMAGE

ROOT = Path(__file__).resolve().parents[2]
MOSQUITTO_IMAGE = runtime.IMAGE
NGINX_IMAGE = 'nginx:1.25-alpine'
GOTRUE_IMAGE = 'supabase/gotrue:v2.196.0'
ENVOY_IMAGE = 'envoyproxy/envoy:v1.39.1'
BACKEND_EXECUTABLE = '/usr/sbin/mosquitto'
ENV = dict(runtime.ENV, COMPOSE_DISABLE_ENV_FILE='1')
ISSUER = 'http://localhost/auth/v1'


def private_write(path: Path | str, content: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(content)


def docker(args: list[str], data: bytes | None = None, timeout: float = 15,
           capture_stderr: bool = False) -> bytes:
    p = subprocess.run(['docker'] + args, input=data, env=ENV,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if p.returncode:
        raise RuntimeError('docker_operation_failed')
    return p.stdout + (b'\n' + p.stderr if capture_stderr else b'')


def classify_start_failure(exit_code: int, stderr: bytes) -> dict:
    text = stderr.lower()
    if b'mkdir' in text and b'read-only file system' in text:
        code = 'missing_mount_destination'
    elif b'permission denied' in text:
        code = 'permission_denied'
    elif b'exec' in text and b'no such file or directory' in text:
        code = 'no_such_executable'
    elif b'read-only file system' in text:
        code = 'readonly_rootfs'
    elif b'exec format error' in text:
        code = 'exec_format'
    else:
        code = 'generic_start_failure'
    return {'exit_code': int(exit_code), 'code': code}


class ContainerStartFailure(RuntimeError):
    def __init__(self, diagnostic: dict):
        super().__init__('container_start_failed')
        self.diagnostic = diagnostic


def require_consumer_owned(cid: str, receipt: dict) -> None:
    if (not receipt or not re.fullmatch('[a-f0-9]{64}', cid) or
            receipt.get('resource_id') != cid):
        raise RuntimeError('consumer_not_owned')
    owned = inspect_owned_resource('container', cid, receipt['expected_project'],
        receipt['expected_role'], expected_name=receipt.get('expected_name'))
    if owned.get('status') != 'owned' or owned.get('resource_id') != cid:
        raise RuntimeError('consumer_not_owned')


def start_container(cid: str, receipt: dict | None = None) -> dict:
    if receipt is not None:
        require_consumer_owned(cid, receipt)
    p = subprocess.run(['docker', 'start', cid], env=ENV, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, timeout=15)
    if p.returncode:
        diagnostic = classify_start_failure(p.returncode, p.stderr)
        del p
        raise ContainerStartFailure(diagnostic)
    return {'exit_code': 0, 'code': 'started'}


def binary_metadata(path: Path) -> dict:
    st = path.lstat()
    if not stat.S_ISREG(st.st_mode):
        raise RuntimeError('binary_metadata_invalid')
    with path.open('rb') as stream:
        header = stream.read(64)
        if (len(header) != 64 or header[:6] != b'\x7fELF\x02\x01' or
                struct.unpack_from('<H', header, 18)[0] != 62):
            raise RuntimeError('binary_metadata_invalid')
        offset = struct.unpack_from('<Q', header, 32)[0]
        entry_size, count = struct.unpack_from('<HH', header, 54)
        if entry_size != 56 or count < 1 or count > 128 or offset + count * entry_size > st.st_size:
            raise RuntimeError('binary_metadata_invalid')
        stream.seek(offset)
        types = [struct.unpack_from('<I', stream.read(entry_size))[0] for _ in range(count)]
    return {'file_type': 'regular', 'mode': format(stat.S_IMODE(st.st_mode), '04o'),
            'uid': st.st_uid, 'gid': st.st_gid, 'size': st.st_size,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'elf': {'class': 'ELF64', 'byte_order': 'little', 'target': 'x86_64',
                    'type': struct.unpack_from('<H', header, 16)[0],
                    'has_interpreter': 3 in types, 'has_dynamic_segment': 2 in types,
                    'static_format': 3 not in types and 2 not in types}}


def binary_mount_flags(path: Path) -> list[str]:
    p = subprocess.run(['findmnt', '--target', str(path), '--noheadings', '--output', 'OPTIONS'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
    if p.returncode or len(p.stdout.splitlines()) != 1:
        raise RuntimeError('binary_mount_metadata_failed')
    allowed = {'rw', 'ro', 'exec', 'noexec', 'suid', 'nosuid', 'dev', 'nodev',
               'noatime', 'relatime', 'strictatime', 'nodiratime', 'inode64', 'seclabel'}
    flags = [f for f in p.stdout.decode('ascii').strip().split(',') if f in allowed]
    if not ({'rw', 'ro'} & set(flags)):
        raise RuntimeError('binary_mount_metadata_failed')
    return flags


def sanitize_backend_start_metadata(data: dict, binary: Path) -> dict:
    keys = {'entrypoint', 'cmd', 'user', 'readonly_root', 'apparmor', 'userns',
            'cap_add', 'cap_drop', 'security_opt', 'binary_source', 'binary_destination', 'binary_rw'}
    if (set(data) != keys or data['entrypoint'] != [BACKEND_EXECUTABLE] or
            data['cmd'] not in (None, [], [BACKEND_EXECUTABLE, '-c', '/mosquitto/config/mosquitto.conf']) or
            data['user'] != '1883:1883' or data['readonly_root'] is not True or
            data['apparmor'] not in ('', 'docker-default') or data['userns'] != '' or
            data['cap_add'] not in (None, []) or data['cap_drop'] != ['ALL'] or
            data['security_opt'] != ['no-new-privileges:true'] or
            data['binary_source'] != str(binary) or data['binary_destination'] != BACKEND_EXECUTABLE or
            data['binary_rw'] is not False):
        raise RuntimeError('backend_metadata_invalid')
    return {k: v for k, v in data.items() if k != 'binary_source'} | {'binary_source_matches_fixture': True}


def backend_start_metadata(receipt: dict, binary: Path, expected_sha256: str) -> dict:
    cid = receipt.get('resource_id', '')
    require_consumer_owned(cid, receipt)
    fields = {'state': '.State.Status', 'entrypoint': '.Config.Entrypoint', 'cmd': '.Config.Cmd', 'user': '.Config.User',
              'readonly_root': '.HostConfig.ReadonlyRootfs', 'apparmor': '.AppArmorProfile',
              'userns': '.HostConfig.UsernsMode', 'cap_add': '.HostConfig.CapAdd',
              'cap_drop': '.HostConfig.CapDrop', 'security_opt': '.HostConfig.SecurityOpt'}
    parts = ['"' + k + '":{{json ' + v + '}}' for k, v in fields.items()]
    template = '{' + ','.join(parts) + ',"binary_mount":['
    template += '{{range .Mounts}}{{if eq .Destination "' + BACKEND_EXECUTABLE + '"}}'
    template += '{"source":{{json .Source}},"destination":{{json .Destination}},"rw":{{json .RW}}}'
    template += '{{end}}{{end}}]}'
    data = json.loads(docker(['inspect', '-f', template, cid]))
    if data.pop('state', None) != 'created':
        raise RuntimeError('backend_metadata_invalid')
    mounts = data.pop('binary_mount')
    if len(mounts) != 1 or set(mounts[0]) != {'source', 'destination', 'rw'}:
        raise RuntimeError('backend_metadata_invalid')
    data.update({'binary_' + k: v for k, v in mounts[0].items()})
    safe = sanitize_backend_start_metadata(data, binary)
    safe['effective_source_metadata'] = binary_metadata(binary)
    source = safe['effective_source_metadata']
    if (source['file_type'] != 'regular' or source['mode'] != '0555' or
            source['sha256'] != expected_sha256):
        raise RuntimeError('backend_binary_contract_failed')
    safe['effective_source_mount_flags'] = binary_mount_flags(binary)
    return safe


def inspect_local_image(image_ref: str, timeout: float = 10) -> tuple[bool, dict | None, str | None]:
    try:
        p = subprocess.run(['docker', 'image', 'inspect', '-f', '{{json .Config.Volumes}}', image_ref],
                           env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        if p.returncode:
            return False, None, 'image_unavailable'
        return True, json.loads(p.stdout), None
    except Exception:
        return False, None, 'image_inspect_error'


def compile_backend_binary(out_path: Path, timeout: float = 120) -> tuple[bool, str | None]:
    env = dict(os.environ, CGO_ENABLED='0', GOOS='linux', GOARCH='amd64',
               GOPROXY='off', GOSUMDB='off', GOTOOLCHAIN='local')
    try:
        p = subprocess.run(['go', 'build', '-o', str(out_path), './cmd/server'],
                           cwd=ROOT / 'src', env=env, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, timeout=timeout)
        return p.returncode == 0, None if p.returncode == 0 else 'offline_build_failed'
    except Exception:
        return False, 'offline_build_unavailable'


def backend_environment(db_password: str, manager_password: str, backend_password: str, jwt_secret: str) -> dict[str, str]:
    return dict(
        SERVER_ENV='production', SERVER_PORT='8080',
        DATABASE_URL=f'postgres://iot_backend_app:{db_password}@task28a-pg:5432/fixture_service?sslmode=disable',
        DATABASE_MAX_CONNS='5', DATABASE_MIN_CONNS='1', DATABASE_CONNECT_TIMEOUT='5s',
        HTTP_WRITE_TIMEOUT='55s', SHUTDOWN_TIMEOUT='55s',
        MQTT_CREDENTIAL_RUNTIME_ENABLED='false', MQTT_CREDENTIAL_API_ENABLED='true',
        INGRESS_GATE_ENABLED='true', MQTT_PASSWORD=backend_password,
        MQTT_INTERNAL_MANAGEMENT_URL='ssl://broker:8883', MQTT_DYNSEC_MANAGER_USERNAME='admin',
        MQTT_DYNSEC_MANAGER_PASSWORD=manager_password, MQTT_TLS_CA_FILE='/public-ca/ca.crt',
        MQTT_DYNSEC_JSON_PATH='/native-store/dynsec.json', MQTT_DYNSEC_WRITER_UID='1883',
        MQTT_DYNSEC_IMAGE_REFERENCE=MOSQUITTO_IMAGE,
        MQTT_DYNSEC_IMAGE_CONFIG_ID=MOSQUITTO_IMAGE.split('@')[1], MQTT_DYNSEC_PLATFORM='linux/amd64',
        SUPABASE_JWT_SECRET=jwt_secret, SUPABASE_JWT_ISSUER=ISSUER,
        SUPABASE_JWT_AUDIENCE='authenticated'
    )


def make_bootstrap_token(secret: str, claims: dict) -> str:
    """Generate bootstrap-only capability tokens (anon or service_role).
    
    Never used as human credentials or given to human users.
    """
    def enc(v: bytes) -> bytes:
        return base64.urlsafe_b64encode(v).rstrip(b'=')
    header = enc(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = enc(json.dumps(claims).encode())
    msg = header + b'.' + payload
    sig = enc(hmac.new(secret.encode(), msg, hashlib.sha256).digest())
    return (msg + b'.' + sig).decode()


def effective_metadata(cid: str) -> dict:
    def field(template: str):
        return json.loads(docker(['inspect', '-f', template, cid]))
    mounts = field('{{json .Mounts}}')
    return {'user': field('{{json .Config.User}}'),
            'mounts': [{k: m[k] for k in ('Destination', 'RW', 'Name', 'Type') if k in m} for m in mounts],
            'networks': sorted(field('{{json .NetworkSettings.Networks}}'))}


def validate_backend_metadata(meta: dict, project: str) -> bool:
    mounts = {m['Destination']: m for m in meta['mounts'] if m.get('Type') != 'tmpfs'}
    if any(m.get('Type') == 'tmpfs' and m['Destination'] not in
           {'/tmp', '/mosquitto/data', '/mosquitto/log', '/mosquitto/config'} for m in meta['mounts']):
        return False
    return (meta['user'] == '1883:1883' and
            set(mounts) == {BACKEND_EXECUTABLE, '/native-store', '/public-ca'} and
            all(m.get('RW') is False for m in mounts.values()) and
            mounts['/native-store'].get('Name') == project + '_native_store' and
            mounts['/public-ca'].get('Name') == project + '_public_ca' and
            meta['networks'] == [project + '_default'])


def remove_consumer(receipt: dict) -> bool:
    cid = receipt.get('resource_id', '')
    if not re.fullmatch('[a-f0-9]{64}', cid):
        return False
    try:
        state = inspect_resource_presence('container', cid)['status']
        if state == 'absent':
            return True
        owned = inspect_owned_resource('container', cid, receipt['expected_project'],
                    receipt['expected_role'], expected_name=receipt.get('expected_name'))
        if owned['status'] != 'owned' or owned['resource_id'] != cid:
            return False
        docker(['rm', '-f', cid])
        return inspect_resource_presence('container', cid)['status'] == 'absent'
    except Exception:
        return False


def teardown(consumers: dict, pg: DatabaseFixture | None, broker: DynSecProbeFixture | None, host: Path | None) -> dict:
    order = ('nginx', 'backend', 'envoy', 'auth')
    result = {r + '_cleanup_verified': False for r in order + ('pg', 'broker', 'host')}
    for role in order:
        result[role + '_cleanup_verified'] = role not in consumers or remove_consumer(consumers[role])
        if result[role + '_cleanup_verified'] and role in consumers and broker is not None:
            cid = consumers[role]['resource_id']
            if cid in broker.owned_containers:
                broker.owned_containers.remove(cid)
    if not all(result[r + '_cleanup_verified'] for r in order):
        return result
    try:
        if pg is None:
            result['pg_cleanup_verified'] = True
        else:
            cid = pg.container_id
            if cid and pg._load_and_verify_receipt() != cid:
                return result
            if not cid and pg.cidfile.exists():
                cid = pg._load_and_verify_receipt()
                if not cid:
                    return result
            result['pg_cleanup_verified'] = pg.cleanup() is True
            if cid:
                result['pg_cleanup_verified'] = (result['pg_cleanup_verified'] and
                    inspect_resource_presence('container', cid)['status'] == 'absent')
    except Exception:
        pass
    if not result['pg_cleanup_verified']:
        return result
    try:
        detail = {'clean': True} if broker is None else broker.cleanup()
        result['broker_detail'] = detail
        result['broker_cleanup_verified'] = detail.get('clean') is True
    except Exception:
        pass
    if not result['broker_cleanup_verified']:
        return result
    try:
        result['host_cleanup_verified'] = host is None or verified_remove_host_fixture_dir(host).get('verified') is True
    except Exception:
        pass
    return result


def aggregate(smoke: str, cleanup: dict) -> dict:
    roles = ('nginx', 'backend', 'envoy', 'auth', 'pg', 'broker', 'host')
    clean = all(cleanup.get(r + '_cleanup_verified') is True for r in roles)
    return {'success': smoke == 'PASS' and clean,
            'verdict': 'CLEANUP_FAILURE_VETO' if not clean else ('PASS' if smoke == 'PASS' else 'NOT_PASSED'),
            'smoke_status': smoke}


def http_call(port: int, method: str, path: str, *, token: str = '', api_key: str = '', idempotency_key: str = '', body: dict | None = None, timeout: float = 50) -> tuple[int, dict, dict | str]:
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=timeout)
    try:
        headers = {'Content-Type': 'application/json'}
        if token:
            headers['Authorization'] = 'Bearer ' + token
        if api_key:
            headers['apikey'] = api_key
        if idempotency_key:
            headers['Idempotency-Key'] = idempotency_key
        payload = json.dumps(body).encode() if body is not None else None
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise RuntimeError('http_body_too_large')
        try:
            parsed = json.loads(raw.decode('utf-8', errors='replace')) if raw else {}
        except Exception:
            parsed = 'non_json'
        return response.status, dict(response.getheaders()), parsed
    finally:
        conn.close()


def native_store(cid: str) -> dict:
    return json.loads(docker(['exec', '-u', '1883:1883', cid, 'cat', '/native-store/dynsec.json']))


def classify_semantic_rejection(status: int, body: dict | str | None, content_type: str) -> str:
    body_str = str(body).lower() if body else ''
    if isinstance(body, dict):
        code = str(body.get('error_code') or body.get('code') or '').lower()
        if code == 'signup_disabled':
            return 'signup_disabled'
        if status in (401, 403) and ('api key' in body_str or 'apikey' in body_str or 'api_key' in body_str or code == 'invalid_api_key'):
            return 'invalid_api_key'
        if status == 401 or code in ('unauthorized', 'invalid_credentials'):
            return 'unauthorized'
    if status == 401:
        if 'api key' in body_str or 'apikey' in body_str or 'api_key' in body_str:
            return 'invalid_api_key'
        return 'unauthorized'
    if status == 404:
        return 'route_not_found'
    if status in (502, 503, 504):
        return 'upstream_unavailable'
    return 'unknown'


def inspect_container_lifecycle(cid: str) -> dict:
    """Safe inspection of container lifecycle status and exit code only.

    No raw command strings, environment, secret tokens, or logs returned.
    """
    if not isinstance(cid, str) or not re.fullmatch(r'[a-f0-9]{64}', cid):
        return {'status': 'unknown', 'exit_code': -1}
    try:
        raw = docker(['inspect', '-f', '{"Status":{{json .State.Status}},"ExitCode":{{json .State.ExitCode}}}', cid], timeout=5)
        state = json.loads(raw)
        status = state.get('Status', 'unknown')
        exit_code = state.get('ExitCode', -1)
        return {
            'status': status if status in ('created', 'running', 'paused', 'restarting', 'removing', 'exited', 'dead') else 'unknown',
            'exit_code': int(exit_code) if isinstance(exit_code, int) else -1
        }
    except Exception:
        return {'status': 'inspect_error', 'exit_code': -1}


def fatal_message_text(value, depth: int = 0) -> str:
    """Read message/error fields only, including nested JSON; never return externally."""
    if depth > 6:
        return ''
    if isinstance(value, dict):
        return '\n'.join(fatal_message_text(value[k], depth + 1)
                         for k in ('msg', 'message', 'error', 'cause') if k in value)
    if isinstance(value, str):
        try:
            nested = json.loads(value)
        except ValueError:
            return value
        return fatal_message_text(nested, depth + 1) if isinstance(nested, dict) else value
    return ''


def migration_failure_details(text: str) -> dict:
    """Emit constrained diagnostics only; Pop's embedded SQL stays in memory.

    Source: supabase/auth v2.196.0 migrations/20240612123726_enable_rls_update_grants.up.sql
    explicitly grants to postgres; gobuffalo/pop v6.1.1 migration_box.go emits
    `error executing <path>, sql: <content>: <error>`.
    """
    codes = set(re.findall(r'\bSQLSTATE\s+([0-9A-Z]{5})\b', text, re.I))
    codes = {c.upper() for c in codes}
    versions = set(re.findall(
        r'\bmigration\s+(?:["\']?)([0-9]{1,14})(?=[_\s:"\']|$)', text, re.I))
    filenames = re.findall(
        r'\berror (?:executing|processing) (?:migrations/)?'
        r'([0-9]{1,14}_[A-Za-z][A-Za-z0-9_]{0,100}(?:\.postgres)?\.up\.sql)(?=[,:\s]|$)', text)
    versions.update(name.split('_', 1)[0] for name in filenames)
    code = next(iter(codes)) if len(codes) == 1 else None
    detail = {'migration_identifier': next(iter(versions)) if len(versions) == 1 else None,
              'sqlstate': code, 'category': 'unknown'}
    known_labels = {'20240612123726_enable_rls_update_grants.up.sql',
                    '00_init_auth_schema.up.sql'}
    if len(set(filenames)) == 1 and filenames[0] in known_labels:
        detail['migration_label'] = filenames[0]
    lowered = text.lower()
    if re.search(r'must be owner of', lowered):
        detail['category'] = 'object_ownership'
    elif re.search(r'permission denied for schema', lowered):
        detail['category'] = 'schema_privileges'
    elif code in ('42P01', '42703'):
        detail['category'] = 'missing_relation_or_column'
    elif code in ('42P07', '42710', '42701'):
        detail['category'] = 'duplicate_object'
    elif code == '42601':
        detail['category'] = 'invalid_syntax'
    elif code == '42704':
        detail['category'] = 'undefined_object'
    elif re.search(r'extension.*(?:not available|not supported|could not open)', lowered):
        detail['category'] = 'unsupported_extension'
    # Only object references in error phrases, never arbitrary log/SQL identifiers.
    objects = set(re.findall(
        r'(?:must be owner of|permission denied for|(?:relation|column))\s+'
        r'(?:(?:table|schema|relation|column)\s+)?["\']?'
        r'((?:auth\.)?(?:schema_migrations|users|refresh_tokens|instances|audit_log_entries)|auth|version)'
        r'(?=["\'\s)]|$)', text, re.I))
    if len(objects) == 1:
        detail['object_name'] = next(iter(objects)).lower()
    # Quoted identifier grammar is deliberately narrower than PostgreSQL's.
    # Unknown/user-generated names never escape, even if syntactically valid.
    undefined = re.findall(
        r'\b(role|schema|table|relation|column|type|function|extension|operator|collation|constraint) '
        r'"([^"\r\n]*)" does not exist\b', text, re.I)
    if code == '42704' and len(undefined) == 1:
        kind, identifier = undefined[0]
        detail['object_type'] = kind.lower()
        static = {'role': {'postgres', 'supabase_auth_admin'}, 'schema': {'auth'}}
        if (re.fullmatch(r'[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?', identifier) and
                identifier in static.get(kind.lower(), set())):
            detail['object_name'] = identifier
    return detail


def classify_auth_fatal(raw: bytes) -> dict:
    """Discard raw logs; emit only a fixed category from fatal-level lines."""
    for line in raw.decode('utf-8', errors='replace').splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            entry = None
        if isinstance(entry, dict):
            if entry.get('level') not in ('fatal', 'panic'):
                continue
        elif not re.search(r'\blevel=(?:fatal|panic)\b|^FATAL\b', line):
            continue
        message = fatal_message_text(entry) if isinstance(entry, dict) else line
        text = message.lower()
        details = {'migration_diagnostic': migration_failure_details(message)} if 'migrat' in text else {}
        patterns = (
            ('db_authentication', r'password authentication failed|sqlstate 28p01'),
            ('permission_denied', r'permission denied|sqlstate 42501'),
            ('missing_relation_or_column', r'sqlstate (?:42p01|42703)|(?:relation|column).*does not exist'),
            ('missing_setting', r'required key .*missing value|missing .*setting|is required'),
            ('invalid_config', r'invalid config|configuration.*invalid|error loading config|error parsing'),
            ('binding', r'address already in use|bind:'),
            ('connection_failure', r'connection refused|failed to connect|connection timed out|no such host'),
            ('migration_failure', r'migrat'),
        )
        for category, pattern in patterns:
            if re.search(pattern, text):
                return {'fatal_category': category} | details
        if details:
            return {'fatal_category': 'unknown'} | details
    return {'fatal_category': 'unknown'}


def auth_exit_diagnostic(cid: str, receipt: dict | None) -> dict:
    if not receipt or receipt.get('expected_role') != 'auth':
        raise RuntimeError('consumer_not_owned')
    require_consumer_owned(cid, receipt)
    lifecycle = inspect_container_lifecycle(cid)
    safe = lifecycle | {'fatal_category': 'unknown'}
    if lifecycle['status'] not in ('exited', 'dead'):
        return safe
    try:
        # Capture in memory only, never print or persist raw stdout/stderr.
        raw = docker(['logs', '--tail', '100', cid], timeout=5, capture_stderr=True)
        safe.update(classify_auth_fatal(raw))
        del raw
    except Exception:
        pass
    return safe


def probe_auth_gateway_readiness(port: int, anon_key: str, auth_cid: str,
                                 timeout_sec: float = 30.0, per_call_timeout: float = 2.0,
                                 auth_receipt: dict | None = None) -> dict:
    """Probe Auth gateway readiness before attempting user mutations.

    Probes /auth/v1/health with required apikey through Nginx -> Envoy -> GoTrue.
    Validates correct upstream response semantics: HTTP 200 with JSON body
    containing version and name. Returns diagnostic dict on success or failure.
    Never exposes raw response bodies, API keys, or tokens.
    """
    deadline = time.monotonic() + timeout_sec
    last_status = 0
    last_cat = 'unknown'
    last_ct = ''
    attempts = 0
    while time.monotonic() < deadline:
        lifecycle = inspect_container_lifecycle(auth_cid)
        if lifecycle['status'] in ('exited', 'dead'):
            diagnostic = auth_exit_diagnostic(auth_cid, auth_receipt) if auth_receipt else lifecycle
            return {'ready': False, 'status': last_status, 'category': 'auth_exited',
                    'attempts': attempts, 'auth_container': diagnostic}
        attempts += 1
        try:
            status, headers, body = http_call(
                port, 'GET', '/auth/v1/health', api_key=anon_key, timeout=per_call_timeout
            )
            ct = headers.get('content-type', headers.get('Content-Type', ''))
            last_status = status
            last_ct = ct
            last_cat = classify_semantic_rejection(status, body, ct)
            if status == 200 and isinstance(body, dict) and ('version' in body or 'name' in body or 'description' in body):
                return {
                    'ready': True,
                    'status': 200,
                    'category': 'ready',
                    'attempts': attempts
                }
        except (OSError, ValueError, http.client.HTTPException):
            last_status = 0
            last_cat = 'connection_error'
        time.sleep(0.5)

    auth_lifecycle = inspect_container_lifecycle(auth_cid)
    return {
        'ready': False,
        'status': last_status,
        'category': last_cat,
        'attempts': attempts,
        'auth_container': auth_lifecycle
    }


def assert_signup_disabled(status: int, body: dict) -> None:
    if not isinstance(body, dict) or body.get("error_code") != "signup_disabled":
        raise RuntimeError(f"public signup was not rejected with signup_disabled semantic error (HTTP {status})")


def create_user_via_admin_api(port: int, service_key: str, email: str, password: str) -> str:
    status, _, body = http_call(
        port, 'POST', '/auth/v1/admin/users',
        token=service_key, api_key=service_key,
        body={"email": email, "password": password, "email_confirm": True}
    )
    if status != 200 or not isinstance(body, dict) or not body.get("id"):
        raise RuntimeError(f"admin user creation failed (HTTP {status})")
    uid = body["id"]
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', uid):
        raise RuntimeError("invalid user uuid returned from admin api")
    return uid


def login_user_for_token(port: int, anon_key: str, email: str, password: str) -> tuple[str, str]:
    status, _, body = http_call(
        port, 'POST', '/auth/v1/token?grant_type=password',
        api_key=anon_key,
        body={"email": email, "password": password}
    )
    if status != 200 or not isinstance(body, dict) or not body.get("access_token") or not body.get("refresh_token"):
        raise RuntimeError(f"user password login failed (HTTP {status})")
    return body["access_token"], body["refresh_token"]


def auth_smoke_flow(port: int, broker: DynSecProbeFixture, backend_cid: str, pg: DatabaseFixture,
                    anon_key: str, service_key: str, jwt_secret: str, gateway: str, observations: dict) -> None:
    checks = observations['checks'] = {}

    def check(name: str, ok: bool) -> None:
        checks[name] = bool(ok)
        if not ok:
            raise RuntimeError('smoke_assertion_failed')

    # 1. Verify public signup is disabled through Nginx -> Envoy -> GoTrue
    dummy_email = f"signup_denied_{secrets.token_hex(6)}@example.invalid"
    dummy_pwd = secrets.token_urlsafe(24)
    signup_status, signup_headers, signup_body = http_call(
        port, 'POST', '/auth/v1/signup', api_key=anon_key,
        body={"email": dummy_email, "password": dummy_pwd}
    )
    ct = signup_headers.get('content-type', signup_headers.get('Content-Type', ''))
    observations['signup_diagnostic'] = {
        'status': signup_status,
        'content_type': ct,
        'semantic_error_code': classify_semantic_rejection(signup_status, signup_body, ct),
        'sent_header_presence': {
            'apikey_present': bool(anon_key),
            'authorization_present': False,
        },
        'response_type': 'json' if isinstance(signup_body, dict) else 'non_json',
        'gateway_route_stage': 'nginx_to_envoy_auth_v1_signup'
    }
    assert_signup_disabled(signup_status, signup_body)
    check('public_signup_disabled', True)

    # 2. Create admin and owner users via Admin API
    admin_email = f"admin_{secrets.token_hex(6)}@example.invalid"
    admin_pwd = secrets.token_urlsafe(24)
    owner_email = f"owner_{secrets.token_hex(6)}@example.invalid"
    owner_pwd = secrets.token_urlsafe(24)

    admin_uid = create_user_via_admin_api(port, service_key, admin_email, admin_pwd)
    owner_uid = create_user_via_admin_api(port, service_key, owner_email, owner_pwd)
    check('admin_user_created', True)
    check('owner_user_created', True)

    # 3. Grant platform_admins to admin_uid and owner membership to owner_uid
    # Ensure profiles exist (via trigger) and execute authorizations
    pg.sql(
        f"INSERT INTO profiles(id, full_name) VALUES ('{admin_uid}', 'Fixture Admin') ON CONFLICT (id) DO NOTHING; "
        f"INSERT INTO profiles(id, full_name) VALUES ('{owner_uid}', 'Fixture Owner') ON CONFLICT (id) DO NOTHING; "
        f"INSERT INTO platform_admins(user_id) VALUES ('{admin_uid}'); "
        f"INSERT INTO gateways(gateway_id, name) VALUES ('{gateway}', 'Fixture Gateway'); "
        f"INSERT INTO user_gateways(user_id, gateway_id, role) VALUES ('{owner_uid}', '{gateway}', 'owner');".encode()
    )
    check('authorization_granted', True)

    # 4. Perform real GoTrue password login through Nginx
    admin_token, _ = login_user_for_token(port, anon_key, admin_email, admin_pwd)
    owner_token, _ = login_user_for_token(port, anon_key, owner_email, owner_pwd)

    # Verify token payload without exposing it
    for t_val, expected_uid in ((admin_token, admin_uid), (owner_token, owner_uid)):
        parts = t_val.split('.')
        if len(parts) != 3:
            raise RuntimeError('malformed_jwt')
        padding = b'=' * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(parts[1].encode() + padding))
        if claims.get('iss') != ISSUER or claims.get('aud') != 'authenticated' or claims.get('role') != 'authenticated' or claims.get('sub') != expected_uid:
            raise RuntimeError('token_claims_mismatch')
    check('tokens_valid_unmodified', True)

    # 5. Reuse credential management smoke assertions with genuine issued tokens
    endpoint = '/v1/admin/gateways/' + gateway + '/mqtt-credential'

    def call(method: str, suffix: str, token: str, expected: int, key: str | None = None) -> dict:
        status, resp_headers, body = http_call(
            port, method, endpoint + suffix, token=token,
            idempotency_key=str(uuid.uuid4()) if key is None else key
        )
        observations.setdefault('http_statuses', []).append(status)
        if status != expected:
            observations['credential_http_failure'] = {
                'status': status, 'expected_status': expected,
                'category': 'unexpected_http_status'
            }
        check(method.lower() + '_' + suffix.replace('/', '_') + '_' + str(expected) + '_' + str(len(checks)), status == expected)
        if expected in (200, 201):
            check('no_store_' + str(len(checks)), resp_headers.get('Cache-Control') == 'no-store' and resp_headers.get('Pragma') == 'no-cache')
        return body

    def login(password: str, code: int) -> None:
        actual = probe_raw_connack(broker.port, broker.d / 'ca.crt', gateway, password, 'probe_' + uuid.uuid4().hex)
        observations.setdefault('connack_codes', []).append(actual)
        check('connack_' + str(len(checks)), actual == code)

    def session(password: str) -> NativeTargetConnection:
        conn = NativeTargetConnection(broker.port, broker.d / 'ca.crt', gateway, password, 'session_' + uuid.uuid4().hex)
        broker.open_connections.append(conn)
        return conn

    def journal() -> int:
        _, raw = pg.sql(f"SELECT count(*) FROM public.gateway_mqtt_credential_events WHERE gateway_id='{gateway}';".encode())
        numbers = re.findall(rb'^\s*(\d+)\s*$', raw, re.M)
        if len(numbers) != 1:
            raise RuntimeError('journal_observation_invalid')
        return int(numbers[0])

    call('POST', '/rotate', 'invalid-token', 401)
    provision = call('POST', '', admin_token, 201)
    old = provision.get('password')
    check('provision_secret', isinstance(old, str) and len(old) >= 22 and provision.get('secret_returned') is True)
    login(old, 0)
    active = session(old)
    before = call('GET', '', admin_token, 200, '')
    count, baseline = journal(), native_store(backend_cid)

    # Owner attempts rotate and revoke -> 403 Forbidden without mutation
    call('POST', '/rotate', owner_token, 403)
    call('DELETE', '', owner_token, 403)
    after = call('GET', '', admin_token, 200, '')
    check('owner_baseline_intact', before == after and count == journal() and baseline == native_store(backend_cid))
    login(old, 0)

    # Admin rotates credential
    key = str(uuid.uuid4())
    rotated = call('POST', '/rotate', admin_token, 200, key)
    new = rotated.get('password')
    check('rotate_secret_and_version', isinstance(new, str) and len(new) >= 22 and new != old and rotated.get('credential_version') == 2)
    login(new, 0)
    login(old, 5)
    dropped, kind, elapsed = active.wait_for_disconnect(5)
    observations['rotate_disconnect'] = {'observed': dropped, 'kind': kind, 'elapsed_seconds': round(elapsed, 3)}
    check('rotate_session_closed', dropped and kind == 'EOF (server closed TLS)')

    # Same-key replay is metadata-only
    replay = call('POST', '/rotate', admin_token, 200, key)
    check('metadata_only_replay', not replay.get('password') and replay.get('secret_returned') is not True and replay.get('credential_version') == 2)
    active_new = session(new)

    # Admin revokes credential
    revoked = call('DELETE', '', admin_token, 200)
    check('revoke_metadata_only', not revoked.get('password') and revoked.get('secret_returned') is not True and revoked.get('status') == 'revoked')
    login(new, 5)
    dropped, kind, elapsed = active_new.wait_for_disconnect(5)
    observations['revoke_disconnect'] = {'observed': dropped, 'kind': kind, 'elapsed_seconds': round(elapsed, 3)}
    check('revoke_session_closed', dropped and kind == 'EOF (server closed TLS)')

    # Verify native broker state
    store = native_store(backend_cid)
    clients = [c for c in store['clients'] if c['username'] == gateway]
    check('native_disabled', len(clients) == 1 and clients[0].get('disabled') is True)
    check('native_roles_unchanged', clients[0].get('roles') == next(c for c in baseline['clients'] if c['username'] == gateway).get('roles'))


def bootstrap_auth_prerequisites(pg: DatabaseFixture) -> None:
    """Disposable grant recipient for GoTrue migration 20240612123726.

    fixture_admin replaces the upstream postgres bootstrap user. The recipient
    needs no login, membership, or elevated privileges; Auth stays non-superuser.
    """
    pg.sql(b'CREATE ROLE postgres NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE '
           b'NOINHERIT NOREPLICATION NOBYPASSRLS;', expect=True)


def apply_auth_compatibility(pg: DatabaseFixture, readiness: dict) -> None:
    # auth.users exists after the first migration, not after all migrations.
    if readiness.get('ready') is not True:
        raise RuntimeError('auth_gateway_not_ready')
    pg.sql((ROOT / 'migrations/000004_supabase_compat.up.sql').read_bytes(), expect=True)


def run_rehearsal_suite(dry_run: bool = False, bounded_native: bool = False) -> dict:
    result = {
        'success': False, 'allocated': False, 'failure_stage': None, 'failure_code': None,
        'build_scope': 'offline host static binary mounted read-only; NOT Dockerfile-build/full root-image parity',
        'auth_scope': 'disposable GoTrue v2.196.0 + Envoy v1.39.1 real password login; NOT fake tokens',
        'observations': {}
    }
    result['root_cause'] = 'UNKNOWN'
    pg = broker = host = None
    consumers = {}
    smoke = 'NOT_EXECUTED'
    stage = 'preflight'
    old_handler = None
    if bounded_native:
        def budget_expired(signum, frame):
            raise RuntimeError('outer_budget_exceeded')
        old_handler = signal.signal(signal.SIGALRM, budget_expired)
        signal.alarm(200)
    try:
        image_volumes = {}
        for role, image in [('pg', PG_IMAGE), ('broker', MOSQUITTO_IMAGE), ('nginx', NGINX_IMAGE),
                            ('auth', GOTRUE_IMAGE), ('envoy', ENVOY_IMAGE)]:
            ok, vols, error = inspect_local_image(image)
            if not ok:
                raise RuntimeError(role + '_image_unavailable')
            image_volumes[role] = vols or {}
        if dry_run:
            result['failure_code'] = 'dry_run_only'
        else:
            d, host = create_verified_host_fixture_dir(Path('/tmp/opencode'))
            result['allocated'] = True

            # 1. Database Fixture
            stage = 'database'
            pg = DatabaseFixture()
            pg.tmpfs_mounts = sorted(set(image_volumes['pg']) | {'/var/lib/postgresql/data'})
            pg.create()
            pg.start()
            pg.wait_ready()

            # 2. Migrations (000000 -> 000019)
            stage = 'migrations'
            migrations = sorted(p for p in (ROOT / 'migrations').iterdir() if
                (p.name == '000000_configure_supabase_roles.sh' or
                 re.match(r'^0000(?:0[1-9]|1[0-9])_.*\.up\.sql$', p.name)))
            if len(migrations) != 20:
                raise RuntimeError('migration_set_invalid')
            for p in migrations:
                if p.suffix == '.sh':
                    docker(['exec', '-i', pg.container_id, 'sh'], p.read_bytes())
                else:
                    pg.sql(p.read_bytes())
            bootstrap_auth_prerequisites(pg)

            gateway = 'gw_' + secrets.token_hex(6)

            # 3. Broker Fixture
            stage = 'broker'
            broker = DynSecProbeFixture(d, MOSQUITTO_IMAGE, initialize_backend=True, consumer_rehearsal=True)
            broker.prepare()
            broker.start()

            # 4. Offline Backend Build
            stage = 'offline_build'
            binary = d / 'backend-server'
            ok, error = compile_backend_binary(binary)
            if not ok:
                raise RuntimeError(error)
            binary.chmod(0o555)
            result['observations']['compiled_binary'] = binary_metadata(binary)
            result['observations']['binary_mount_flags'] = binary_mount_flags(binary)
            result['binary_sha256'] = result['observations']['compiled_binary']['sha256']

            # 5. Broker Network & PG alias
            stage = 'broker_network'
            net = next(r for r in broker.verified_receipts if r['kind'] == 'network')
            if inspect_owned_resource('network', net['resource_id'], broker.project, 'default')['status'] != 'owned':
                raise RuntimeError('network_not_owned')
            if pg._load_and_verify_receipt() != pg.container_id:
                raise RuntimeError('pg_not_owned')
            docker(['network', 'connect', '--alias', 'task28a-pg', net['resource_id'], pg.container_id])

            jwt_secret = secrets.token_urlsafe(32)
            now = int(time.time())
            anon_token = make_bootstrap_token(jwt_secret, {"role": "anon", "iat": now, "exp": now + 3600})
            service_token = make_bootstrap_token(jwt_secret, {"role": "service_role", "iat": now, "exp": now + 3600})
            dashboard_pwd = secrets.token_urlsafe(24)

            # 6. Auth (GoTrue) container creation & startup
            stage = 'auth_create'
            auth_name = f"{broker.project}-auth-1"
            auth_cidfile = d / 'auth.cid'
            auth_envfile = d / 'auth.env'
            auth_cfg = {
                'API_EXTERNAL_URL': ISSUER.rsplit('/auth/v1', 1)[0],
                'GOTRUE_API_HOST': '0.0.0.0',
                'GOTRUE_API_PORT': '9999',
                'GOTRUE_DB_DRIVER': 'postgres',
                'GOTRUE_DB_DATABASE_URL': f'postgres://supabase_auth_admin:{pg.passwords[2]}@task28a-pg:5432/fixture_service?sslmode=disable',
                'GOTRUE_SITE_URL': 'http://localhost',
                'GOTRUE_URI_ALLOW_LIST': '*',
                'GOTRUE_DISABLE_SIGNUP': 'true',
                'GOTRUE_JWT_ADMIN_ROLES': 'service_role',
                'GOTRUE_JWT_AUD': 'authenticated',
                'GOTRUE_JWT_ISSUER': ISSUER,
                'GOTRUE_JWT_DEFAULT_GROUP_NAME': 'authenticated',
                'GOTRUE_JWT_EXP': '3600',
                'GOTRUE_JWT_SECRET': jwt_secret,
                'GOTRUE_EXTERNAL_EMAIL_ENABLED': 'true',
                'GOTRUE_MAILER_AUTOCONFIRM': 'true',
            }
            private_write(auth_envfile, ''.join(f"{k}={v}\n" for k, v in auth_cfg.items()))
            auth_args = ['create', '--pull=never', '--name', auth_name, '--cidfile', str(auth_cidfile),
                         '--label', f'com.docker.compose.project={broker.project}',
                         '--label', 'com.docker.compose.service=auth',
                         '--label', 'com.docker.compose.oneoff=false',
                         '--network', net['resource_id'], '--network-alias', 'auth',
                         '--env-file', str(auth_envfile)]
            for dest in image_volumes['auth']:
                auth_args += ['--tmpfs', dest + ':rw']
            auth_args += [GOTRUE_IMAGE]
            consumers['auth'] = {'resource_id': '', 'expected_project': broker.project, 'expected_role': 'auth', 'expected_name': auth_name}
            try:
                docker(auth_args, timeout=20)
            finally:
                if auth_cidfile.is_file() and not auth_cidfile.is_symlink():
                    cid = auth_cidfile.read_text().strip()
                    consumers['auth']['resource_id'] = cid
                    captured = broker.capture_consumer(cid, 'auth', auth_name)
                    if captured['status'] != 'owned' or captured['resource_id'] != cid:
                        raise RuntimeError('consumer_capture_failed')
            stage = 'auth_start'
            start_container(consumers['auth']['resource_id'], consumers['auth'])

            # 8. Envoy (Supabase API Gateway) container creation & startup
            stage = 'envoy_create'
            envoy_name = f"{broker.project}-envoy-1"
            envoy_cidfile = d / 'envoy.cid'
            envoy_envfile = d / 'envoy.env'
            envoy_cfg = {
                'ANON_KEY': anon_token,
                'SERVICE_ROLE_KEY': service_token,
                'SUPABASE_PUBLIC_URL': 'http://localhost',
                'DASHBOARD_USERNAME': 'admin',
                'DASHBOARD_PASSWORD': dashboard_pwd,
            }
            private_write(envoy_envfile, ''.join(f"{k}={v}\n" for k, v in envoy_cfg.items()))
            envoy_args = ['create', '--pull=never', '--name', envoy_name, '--cidfile', str(envoy_cidfile),
                          '--label', f'com.docker.compose.project={broker.project}',
                          '--label', 'com.docker.compose.service=envoy',
                          '--label', 'com.docker.compose.oneoff=false',
                          '--network', net['resource_id'], '--network-alias', 'kong', '--network-alias', 'envoy',
                          '--env-file', str(envoy_envfile),
                          '--entrypoint', '/bin/sh',
                          '--mount', f'type=bind,source={ROOT / "config/envoy/envoy.yaml"},target=/etc/envoy/envoy.yaml,readonly',
                          '--mount', f'type=bind,source={ROOT / "config/envoy/cds.yaml"},target=/etc/envoy/cds.yaml,readonly',
                          '--mount', f'type=bind,source={ROOT / "config/envoy/lds.template.yaml"},target=/etc/envoy/lds.template.yaml,readonly',
                          '--mount', f'type=bind,source={ROOT / "config/envoy/docker-entrypoint.sh"},target=/docker-entrypoint.sh,readonly']
            for dest in image_volumes['envoy']:
                envoy_args += ['--tmpfs', dest + ':rw']
            envoy_args += [ENVOY_IMAGE, '/docker-entrypoint.sh']
            consumers['envoy'] = {'resource_id': '', 'expected_project': broker.project, 'expected_role': 'envoy', 'expected_name': envoy_name}
            try:
                docker(envoy_args, timeout=20)
            finally:
                if envoy_cidfile.is_file() and not envoy_cidfile.is_symlink():
                    cid = envoy_cidfile.read_text().strip()
                    consumers['envoy']['resource_id'] = cid
                    captured = broker.capture_consumer(cid, 'envoy', envoy_name)
                    if captured['status'] != 'owned' or captured['resource_id'] != cid:
                        raise RuntimeError('consumer_capture_failed')
            stage = 'envoy_start'
            start_container(consumers['envoy']['resource_id'], consumers['envoy'])

            # Diagnostic check on generated Envoy LDS configuration (private substitution verification)
            try:
                rendered_lds = docker(['exec', consumers['envoy']['resource_id'], 'cat', '/etc/envoy/lds.yaml']).decode()
                import re as _re
                anon_h = hashlib.sha256(anon_token.encode()).hexdigest()[:16]
                srv_h = hashlib.sha256(service_token.encode()).hexdigest()[:16]
                result['observations']['envoy_config_diagnostic'] = {
                    'anon_key_present': anon_token in rendered_lds,
                    'service_role_key_present': service_token in rendered_lds,
                    'anon_key_sha256_prefix': anon_h,
                    'service_role_key_sha256_prefix': srv_h,
                    'unresolved_placeholders': sorted(set(_re.findall(r'\$\{[A-Za-z0-9_]+\}', rendered_lds))),
                    'auth_v1_prefix_rewrite': '/auth/v1/' in rendered_lds and 'cluster: auth' in rendered_lds,
                }
            except Exception:
                result['observations']['envoy_config_diagnostic'] = {'status': 'inspect_failed'}

            # 9. Backend & Nginx containers
            envfile = d / 'backend.env'
            cfg = backend_environment(pg.passwords[1], broker.passwords['admin'], broker.passwords['backend_service'], jwt_secret)
            private_write(envfile, ''.join(f"{k}={v}\n" for k, v in cfg.items()))

            for role in ('backend', 'nginx'):
                stage = role + '_create'
                name = f"{broker.project}-{role}-1"
                cidfile = d / (role + '.cid')
                args = ['create', '--pull=never', '--name', name, '--cidfile', str(cidfile),
                        '--label', f'com.docker.compose.project={broker.project}',
                        '--label', f'com.docker.compose.service={role}',
                        '--label', 'com.docker.compose.oneoff=false', '--network', net['resource_id']]
                if role == 'backend':
                    args += ['--network-alias', 'backend', '--user', '1883:1883', '--read-only',
                             '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                             '--tmpfs', '/tmp:rw,noexec,nosuid,size=1m,mode=0700,uid=1883,gid=1883',
                             '--env-file', str(envfile), '--mount', f'type=bind,source={binary},target={BACKEND_EXECUTABLE},readonly']
                    for mount in broker.consumer_mounts():
                        args += ['-v', mount]
                    for dest in image_volumes['broker']:
                        args += ['--tmpfs', dest + ':rw,noexec,nosuid,size=1m']
                    args += ['--entrypoint', BACKEND_EXECUTABLE, MOSQUITTO_IMAGE]
                else:
                    args += ['-p', '127.0.0.1::80', '-e', 'API_DOMAIN=localhost',
                             '--mount', f'type=bind,source={ROOT / "config/nginx/nginx.conf.template"},target=/etc/nginx/templates/default.conf.template,readonly']
                    for dest in image_volumes['nginx']:
                        args += ['--tmpfs', dest + ':rw']
                    args += [NGINX_IMAGE]
                consumers[role] = {'resource_id': '', 'expected_project': broker.project, 'expected_role': role, 'expected_name': name}
                try:
                    docker(args, timeout=20)
                finally:
                    if cidfile.is_file() and not cidfile.is_symlink():
                        cid = cidfile.read_text().strip()
                        consumers[role]['resource_id'] = cid
                        captured = broker.capture_consumer(cid, role, name)
                        if captured['status'] != 'owned' or captured['resource_id'] != cid:
                            raise RuntimeError('consumer_capture_failed')
                cid = consumers[role]['resource_id']
                if role == 'backend':
                    stage = 'backend_prestart_metadata'
                    result['observations']['backend_prestart_metadata'] = backend_start_metadata(consumers[role], binary, result['binary_sha256'])
                    meta = effective_metadata(cid)
                    result['observations']['backend_metadata'] = meta
                    if not validate_backend_metadata(meta, broker.project):
                        raise RuntimeError('backend_mount_contract_failed')
                    source = binary_metadata(binary)
                    if source['mode'] != '0555' or source['sha256'] != result['binary_sha256']:
                        raise RuntimeError('backend_binary_contract_failed')
                stage = role + '_start'
                result['observations'][role + '_start'] = start_container(cid, consumers[role])

            backend_cid, nginx_cid = consumers['backend']['resource_id'], consumers['nginx']['resource_id']
            stage = 'effective_metadata'
            meta = effective_metadata(backend_cid)
            result['observations']['backend_metadata'] = meta
            if not validate_backend_metadata(meta, broker.project):
                raise RuntimeError('backend_mount_contract_failed')
            result['observations']['nginx_metadata'] = effective_metadata(nginx_cid)

            public = docker(['exec', '-u', '1883:1883', backend_cid, 'sh', '-c',
                'test "$(ls -A /public-ca)" = ca.crt && test ! -e /security && test ! -e /var/run/docker.sock && stat -c "%u:%g:%a" /native-store /native-store/dynsec.json']).decode().splitlines()
            result['observations']['protected_store_stat'] = public
            if public != ['1883:1883:700', '1883:1883:600']:
                raise RuntimeError('protected_store_permissions_failed')

            portdata = json.loads(docker(['inspect', '-f', '{{json .NetworkSettings.Ports}}', nginx_cid]))
            ports = portdata['80/tcp']
            if len(ports) != 1 or ports[0]['HostIp'] != '127.0.0.1':
                raise RuntimeError('http_binding_unsafe')
            port = int(ports[0]['HostPort'])
            result['observations']['http_binding'] = {'host': '127.0.0.1', 'port': port}

            # 10. Readiness check via Nginx
            stage = 'standalone_readiness'
            deadline = time.monotonic() + 30
            ready = False
            while time.monotonic() < deadline:
                if inspect_container_lifecycle(consumers['auth']['resource_id'])['status'] in ('exited', 'dead'):
                    raise RuntimeError('auth_unexpected_exit')
                try:
                    status, _, _ = http_call(port, 'GET', '/readyz')
                    if status == 200:
                        ready = True
                        break
                except (OSError, ValueError, http.client.HTTPException):
                    pass
                time.sleep(0.2)
            result['observations']['readiness_200'] = ready
            if not ready:
                raise RuntimeError('standalone_readiness_timeout')

            # 10b. Auth gateway readiness check via Nginx -> Envoy -> GoTrue
            stage = 'auth_gateway_readiness'
            auth_ready_diag = probe_auth_gateway_readiness(
                port, anon_token, consumers['auth']['resource_id'],
                timeout_sec=30.0, per_call_timeout=2.0, auth_receipt=consumers['auth']
            )
            result['observations']['auth_gateway_diagnostic'] = auth_ready_diag
            if not auth_ready_diag.get('ready'):
                raise RuntimeError('auth_gateway_not_ready')
            stage = 'supabase_compat_migration'
            apply_auth_compatibility(pg, auth_ready_diag)

            # 11. Run complete auth & credential smoke flow
            stage = 'smoke'
            smoke = 'FAIL'
            auth_smoke_flow(
                port, broker, backend_cid, pg,
                anon_token, service_token, jwt_secret,
                gateway, result['observations']
            )
            smoke = 'PASS'
    except Exception as exc:
        result['failure_stage'] = stage
        if 'auth' in consumers:
            try:
                diagnostic = auth_exit_diagnostic(consumers['auth']['resource_id'], consumers['auth'])
                result['observations']['auth_exit_diagnostic'] = diagnostic
                if diagnostic['status'] in ('exited', 'dead'):
                    result['root_cause'] = diagnostic['fatal_category']
            except Exception:
                result['observations']['auth_exit_diagnostic'] = {'status': 'ownership_unverified', 'exit_code': -1, 'fatal_category': 'unknown'}
        if isinstance(exc, ContainerStartFailure):
            result['observations']['start_diagnostic'] = exc.diagnostic
        allowed = {
            'pg_image_unavailable', 'broker_image_unavailable', 'nginx_image_unavailable',
            'auth_image_unavailable', 'envoy_image_unavailable',
            'offline_build_failed', 'offline_build_unavailable', 'migration_set_invalid',
            'network_not_owned', 'pg_not_owned', 'consumer_capture_failed',
            'auth_schema_initialization_timeout', 'auth_unexpected_exit',
            'backend_mount_contract_failed', 'backend_binary_contract_failed',
            'protected_store_permissions_failed', 'http_binding_unsafe',
            'standalone_readiness_timeout', 'auth_gateway_not_ready', 'smoke_assertion_failed'
        }
        code = str(exc) if isinstance(exc, RuntimeError) else ''
        result['failure_code'] = code if code in allowed else stage + '_failed'
    finally:
        if bounded_native:
            signal.alarm(90)
        try:
            clean = teardown(consumers, pg, broker, host)
        except Exception:
            clean = {r + '_cleanup_verified': False for r in ('nginx', 'backend', 'envoy', 'auth', 'pg', 'broker', 'host')}
        finally:
            if bounded_native:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        result['cleanup'] = clean
        result['aggregate'] = aggregate(smoke, clean)
        result['success'] = result['aggregate']['success']
    return result


def source_identity() -> dict[str, str]:
    files = [
        Path(__file__),
        Path(__file__).with_name('task28_disable_client_probe.py'),
        Path(__file__).with_name('task28_simplified_schema_runner.py'),
        Path(__file__).with_name('stage2_mosquitto_runtime.py'),
        Path(__file__).with_name('compose.task260-dynsec.yml'),
        Path(__file__).with_name('test_task28_supabase_auth_rehearsal.py'),
        ROOT / 'src/go.mod', ROOT / 'src/go.sum',
        ROOT / 'docker-compose.yml', ROOT / 'config/nginx/nginx.conf.template',
        ROOT / 'config/envoy/envoy.yaml', ROOT / 'config/envoy/cds.yaml',
        ROOT / 'config/envoy/lds.template.yaml', ROOT / 'config/envoy/docker-entrypoint.sh',
        ROOT / 'src/Dockerfile'
    ]
    files += sorted((ROOT / 'src').rglob('*.go'))
    files += sorted((ROOT / 'migrations').glob('*'))
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}


if __name__ == '__main__':
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, env=ENV,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5).stdout.decode().strip()
    identity = source_identity()
    out = run_rehearsal_suite(bounded_native=True)
    out['head'], out['source_sha256'] = head, identity
    evidence_dir = Path('/home/trung/.opencode/plan/task28-evidence/simplified')
    evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence = evidence_dir / ('auth-rehearsal-' + time.strftime('%Y%m%d') + '-' + secrets.token_hex(6) + '.json')
    private_write(evidence, json.dumps(out, indent=2) + '\n')
    print(json.dumps({'verdict': out['aggregate']['verdict'], 'failure_stage': out['failure_stage'],
                      'failure_code': out['failure_code'], 'evidence': str(evidence)}))
    sys.exit(0 if out['success'] else 1)
