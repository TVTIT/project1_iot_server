#!/usr/bin/env python3
"""Task 2.8 Native capability probe: DynSec disableClient disconnect and reconnect denial.

Runs isolated tests on the pinned Mosquitto Docker image only.
No automatic fallback to latest.
Strict fixture boundaries, sanitized metadata, RAM-only credentials, and exact-ID cleanup.
"""
from __future__ import annotations

import json
import base64
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import ssl
import stat
import struct
import subprocess
import time
import uuid

import stage2_mosquitto_runtime as runtime
from task26_harness_helpers import CONTROL, RESPONSE, MQTT, require, text

COMPOSE_BASE = Path(__file__).with_name('compose.task260-dynsec.yml')

SUPPORTED_RESOURCE_KINDS = {'container', 'volume', 'network'}
SAFE_RESOURCE_ID_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$')
SAFE_HEX_64_PATTERN = re.compile(r'^[0-9a-f]{64}$')
SAFE_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$')

DEFAULT_CLEANUP_DEADLINE_SEC = 30.0
DEFAULT_DISCOVERY_DEADLINE_SEC = 5.0
DEFAULT_PER_CALL_CAP_SEC = 5.0
MAX_DISCOVERY_CANDIDATES = 8
SAFE_IMAGE_CONFIG_ID_PATTERN = re.compile(r'^sha256:[0-9a-f]{64}$')
SAFE_BROKER_VERSION_PATTERN = re.compile(r'^mosquitto version 2\.0\.18$')
DEFAULT_IMAGE_INSPECT_TIMEOUT_SEC = 5.0
DEFAULT_EXEC_TIMEOUT_SEC = 5.0


class DuplicateKeyCheckingDecoder(json.JSONDecoder):
    """JSON decoder that raises ValueError when duplicate keys are encountered in an object."""
    def __init__(self, *args, **kwargs):
        super().__init__(object_pairs_hook=self._check_duplicates, *args, **kwargs)

    @staticmethod
    def _check_duplicates(pairs):
        res = {}
        for k, v in pairs:
            if k in res:
                raise ValueError(f'duplicate key in JSON object: {k}')
            res[k] = v
        return res


def inspect_owned_resource(
    kind: str,
    candidate_id: str,
    expected_project: str,
    expected_role: str,
    timeout_sec: float = 5.0,
    expected_name: str | None = None
) -> dict:
    """Bounded, type-specific ownership verification helper for Docker resources.

    Returns structured sanitized receipt:
    {'kind': str, 'resource_id': str, 'status': 'owned' | 'not_owned' | 'unknown', 'reason': str}
    Never returns raw stdout/stderr/exceptions/labels/mounts.
    """
    valid_kind = isinstance(kind, str) and kind in SUPPORTED_RESOURCE_KINDS
    valid_candidate = isinstance(candidate_id, str) and bool(SAFE_RESOURCE_ID_PATTERN.fullmatch(candidate_id))
    valid_project = isinstance(expected_project, str) and bool(SAFE_IDENTIFIER_PATTERN.fullmatch(expected_project))
    valid_role = isinstance(expected_role, str) and bool(SAFE_IDENTIFIER_PATTERN.fullmatch(expected_role))
    valid_expected_name = (
        expected_name is None or
        (isinstance(expected_name, str) and bool(SAFE_IDENTIFIER_PATTERN.fullmatch(expected_name)))
    )

    valid_budget = (
        isinstance(timeout_sec, (int, float)) and
        not isinstance(timeout_sec, bool) and
        not math.isnan(timeout_sec) and
        not math.isinf(timeout_sec) and
        timeout_sec > 0
    )

    receipt = {
        'kind': kind if valid_kind else '',
        'resource_id': candidate_id if valid_candidate else '',
        'status': 'unknown',
        'reason': 'unknown'
    }

    if not (valid_kind and valid_candidate and valid_project and valid_role and valid_expected_name and valid_budget):
        receipt['reason'] = 'invalid_parameters'
        return receipt

    # Role validation against known fixture semantics
    if kind == 'container' and expected_role not in {'broker', 'setup', 'backend', 'nginx', 'auth', 'envoy'}:
        receipt['reason'] = 'invalid_parameters'
        return receipt
    if kind == 'network' and expected_role != 'default':
        receipt['reason'] = 'invalid_parameters'
        return receipt
    if kind == 'volume' and expected_role not in {'security', 'native_store', 'public_ca'}:
        receipt['reason'] = 'invalid_parameters'
        return receipt

    cmd = ['docker', 'inspect', f'--type={kind}', candidate_id]
    try:
        proc = subprocess.run(
            cmd,
            env=runtime.ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=timeout_sec
        )
    except subprocess.TimeoutExpired:
        receipt['reason'] = 'timeout'
        return receipt
    except OSError:
        receipt['reason'] = 'os_error'
        return receipt

    if proc.returncode != 0:
        receipt['reason'] = 'inspect_failed'
        return receipt

    try:
        stdout_text = proc.stdout.decode('utf-8')
    except Exception:
        receipt['reason'] = 'decode_error'
        return receipt

    try:
        data = json.loads(stdout_text, cls=DuplicateKeyCheckingDecoder)
    except ValueError as e:
        if 'duplicate key' in str(e):
            receipt['reason'] = 'duplicate_json_keys'
        else:
            receipt['reason'] = 'json_error'
        return receipt
    except Exception:
        receipt['reason'] = 'json_error'
        return receipt

    if not isinstance(data, list) or len(data) != 1:
        receipt['reason'] = 'schema_mismatch'
        return receipt

    obj = data[0]
    if not isinstance(obj, dict):
        receipt['reason'] = 'schema_mismatch'
        return receipt

    if kind == 'container':
        if expected_role in {'backend', 'nginx', 'auth', 'envoy'} and (
                not SAFE_HEX_64_PATTERN.fullmatch(candidate_id) or expected_name is None):
            receipt['reason'] = 'invalid_parameters'
            return receipt
        raw_id = obj.get('Id')
        raw_name = obj.get('Name')
        config = obj.get('Config')
        if not (isinstance(raw_id, str) and isinstance(raw_name, str) and isinstance(config, dict)):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'schema_mismatch'
            return receipt

        # Validate full 64-char lowercase hex ID
        if not SAFE_HEX_64_PATTERN.fullmatch(raw_id):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'schema_mismatch'
            return receipt

        # Check candidate matching: candidate_id must equal raw_id, or if candidate_id is a short prefix, it must match prefix
        if candidate_id != raw_id and not (len(candidate_id) >= 12 and raw_id.startswith(candidate_id)):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'id_mismatch'
            return receipt

        labels = config.get('Labels')
        if not isinstance(labels, dict):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'schema_mismatch'
            return receipt

        proj_label = labels.get('com.docker.compose.project')
        service_label = labels.get('com.docker.compose.service')
        oneoff_label = labels.get('com.docker.compose.oneoff')

        if proj_label != expected_project:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'label_mismatch'
            return receipt

        if service_label != expected_role:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'role_mismatch'
            return receipt

        if oneoff_label is not None:
            if not isinstance(oneoff_label, str) or oneoff_label.lower() not in {'true', 'false'}:
                receipt['status'] = 'not_owned'
                receipt['reason'] = 'oneoff_mismatch'
                return receipt

        # Validate oneoff semantics if present
        if expected_role in {'broker', 'backend', 'nginx', 'auth', 'envoy'}:
            # broker is a daemon service, must not be a oneoff container
            if oneoff_label is not None and oneoff_label.lower() == 'true':
                receipt['status'] = 'not_owned'
                receipt['reason'] = 'oneoff_mismatch'
                return receipt
        elif expected_role == 'setup':
            # setup is executed with `compose run --rm`, so oneoff must be true if present
            if oneoff_label is not None and oneoff_label.lower() != 'true':
                receipt['status'] = 'not_owned'
                receipt['reason'] = 'oneoff_mismatch'
                return receipt

        clean_name = raw_name.lstrip('/')
        if expected_name is not None:
            if clean_name != expected_name:
                receipt['status'] = 'not_owned'
                receipt['reason'] = 'name_mismatch'
                return receipt
        else:
            is_valid_name = False
            if expected_role == 'broker':
                if clean_name in (f'{expected_project}-broker-1', f'{expected_project}_broker_1'):
                    is_valid_name = True

            if not is_valid_name:
                receipt['status'] = 'not_owned'
                receipt['reason'] = 'name_mismatch'
                return receipt

        receipt['resource_id'] = raw_id
        receipt['status'] = 'owned'
        receipt['reason'] = 'verified'
        return receipt

    elif kind == 'network':
        raw_id = obj.get('Id')
        raw_name = obj.get('Name')
        labels = obj.get('Labels')

        if not (isinstance(raw_id, str) and isinstance(raw_name, str) and isinstance(labels, dict)):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'schema_mismatch'
            return receipt

        if not SAFE_HEX_64_PATTERN.fullmatch(raw_id):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'schema_mismatch'
            return receipt

        if candidate_id != raw_id and not (len(candidate_id) >= 12 and raw_id.startswith(candidate_id)):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'id_mismatch'
            return receipt

        proj_label = labels.get('com.docker.compose.project')
        net_label = labels.get('com.docker.compose.network')

        if proj_label != expected_project:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'label_mismatch'
            return receipt

        if net_label != 'default':
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'role_mismatch'
            return receipt

        expected_net_name = f'{expected_project}_default'
        if raw_name != expected_net_name:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'name_mismatch'
            return receipt

        receipt['resource_id'] = raw_id
        receipt['status'] = 'owned'
        receipt['reason'] = 'verified'
        return receipt

    elif kind == 'volume':
        raw_name = obj.get('Name')
        labels = obj.get('Labels')

        if not (isinstance(raw_name, str) and isinstance(labels, dict)):
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'schema_mismatch'
            return receipt

        proj_label = labels.get('com.docker.compose.project')
        vol_label = labels.get('com.docker.compose.volume')

        if proj_label != expected_project:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'label_mismatch'
            return receipt

        if vol_label != expected_role:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'role_mismatch'
            return receipt

        expected_vol_name = f'{expected_project}_{expected_role}'
        if raw_name != expected_vol_name:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'name_mismatch'
            return receipt

        if candidate_id != raw_name:
            receipt['status'] = 'not_owned'
            receipt['reason'] = 'id_mismatch'
            return receipt

        receipt['resource_id'] = raw_name
        receipt['status'] = 'owned'
        receipt['reason'] = 'verified'
        return receipt

    receipt['status'] = 'unknown'
    receipt['reason'] = 'unsupported_kind'
    return receipt


def inspect_resource_presence(kind: str, resource_id: str, timeout_sec: float = 5.0) -> dict:
    """Bounded, sanitized inspector for Docker container/volume/network presence.

    Returns structured status: {'status': 'exists' | 'absent' | 'unknown', 'kind': str, 'resource_id': str}.
    Never returns raw stdout/stderr/exceptions.
    """
    valid_kind = isinstance(kind, str) and kind in SUPPORTED_RESOURCE_KINDS
    valid_id = isinstance(resource_id, str) and bool(SAFE_RESOURCE_ID_PATTERN.fullmatch(resource_id))

    receipt = {
        'status': 'unknown',
        'kind': kind if valid_kind else '',
        'resource_id': resource_id if valid_id else ''
    }

    # Input validation & budget fail-closed check before subprocess
    if not valid_kind or not valid_id:
        return receipt
    if not isinstance(timeout_sec, (int, float)) or isinstance(timeout_sec, bool):
        return receipt
    if math.isnan(timeout_sec) or math.isinf(timeout_sec) or timeout_sec <= 0:
        return receipt

    deadline = time.monotonic() + timeout_sec

    # Step 1: Type-specific docker inspect
    cmd = ['docker', 'inspect', f'--type={kind}', resource_id]
    try:
        rem = deadline - time.monotonic()
        if rem <= 0:
            return receipt
        proc = subprocess.run(
            cmd,
            env=runtime.ENV,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=rem
        )
    except (subprocess.TimeoutExpired, OSError):
        return receipt

    if proc.returncode == 0:
        receipt['status'] = 'exists'
        return receipt

    # Must be exactly returncode 1 to be a candidate for absence
    if proc.returncode != 1:
        return receipt

    err_bytes = proc.stderr if proc.stderr is not None else b''
    try:
        err_text = err_bytes.decode('utf-8', errors='replace').strip()
    except Exception:
        return receipt

    exact_patterns = [
        f"Error: No such {kind}: {resource_id}",
        f"Error: No such object: {resource_id}",
    ]
    if kind == 'container':
        exact_patterns.append(f"Error response from daemon: No such container: {resource_id}")
    elif kind == 'volume':
        exact_patterns.append(f"Error response from daemon: get {resource_id}: no such volume")
    elif kind == 'network':
        exact_patterns.append(f"Error response from daemon: network {resource_id} not found")

    if err_text not in exact_patterns:
        return receipt

    # Candidate not-found confirmed; now verify daemon health with remaining budget
    rem_health = deadline - time.monotonic()
    if rem_health <= 0:
        return receipt

    try:
        info_proc = subprocess.run(
            ['docker', 'info', '--format', '{{.ID}}'],
            env=runtime.ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=rem_health
        )
    except (subprocess.TimeoutExpired, OSError):
        return receipt

    if info_proc.returncode != 0:
        return receipt

    try:
        info_out = info_proc.stdout.decode('utf-8', errors='replace')
        lines = [line.strip() for line in info_out.splitlines() if line.strip()]
        if len(lines) == 1 and len(lines[0]) > 0:
            receipt['status'] = 'absent'
            return receipt
    except Exception:
        pass

    return receipt


def exact_connack_code(sock: socket.socket) -> int:
    """Read exact 4-byte MQTT 3.1.1 CONNACK and return return_code."""
    buf = bytearray()
    while len(buf) < 4:
        chunk = sock.recv(4 - len(buf))
        if not chunk:
            raise EOFError(f'connection closed after reading {len(buf)} bytes of CONNACK')
        buf.extend(chunk)
    require(len(buf) == 4 and buf[0] == 0x20 and buf[1] == 0x02, 'invalid CONNACK header')
    return buf[3]


class NativeTargetConnection:
    """Bounded MQTT 3.1.1 client with explicit ClientID, keepalive, and disconnect observation."""
    def __init__(self, port: int, ca_file: Path, user: str, password: str, client_id: str, keepalive: int = 60):
        self.port = port
        self.ca_file = ca_file
        self.user = user
        self.password = password
        self.client_id = client_id
        self.keepalive = keepalive
        self.pid = 0

        context = ssl.create_default_context(cafile=str(self.ca_file))
        raw_sock = socket.create_connection(('127.0.0.1', self.port), timeout=5)
        self.sock = context.wrap_socket(raw_sock, server_hostname='localhost')
        self.sock.settimeout(5)

        body = (text('MQTT') +
                struct.pack('!BBH', 4, 0xC2, self.keepalive) +
                text(self.client_id) +
                text(self.user) +
                text(self.password))

        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        self.sock.sendall(bytes([0x10]) + remaining + body)
        code = exact_connack_code(self.sock)
        require(code == 0x00, f'target connect refused with code {code}')

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def send(self, header: int, body: bytes):
        require(len(body) <= 16384, 'payload size bound')
        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        self.sock.sendall(bytes([header]) + remaining + body)

    def exact(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = self.sock.recv(n - len(data))
            if not chunk:
                raise EOFError('connection closed')
            data.extend(chunk)
        return bytes(data)

    def packet(self) -> tuple[int, bytes]:
        header = self.exact(1)[0]
        size = 0
        for i in range(4):
            digit = self.exact(1)[0]
            size += (digit & 127) << (7 * i)
            if not (digit & 128):
                require(size <= 65536, 'response size bound')
                return header, self.exact(size)
        raise RuntimeError('invalid frame')

    def subscribe(self, topic: str, allowed: bool = True):
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self.send(0x82, pid + text(topic) + b'\x00')
        header, body = self.packet()
        require(header == 0x90 and body == pid + (b'\x00' if allowed else b'\x80'),
                'exact SUBACK mismatch')

    def publish(self, topic: str, payload: bytes):
        self.send(0x30, text(topic) + payload)

    def message(self) -> tuple[str, bytes]:
        header, body = self.packet()
        require(header == 0x30, 'unexpected/retained response frame')
        n = struct.unpack('!H', body[:2])[0]
        return body[2:2 + n].decode(), body[2 + n:]

    def ping(self):
        self.send(0xC0, b'')
        header, body = self.packet()
        require(header == 0xD0 and len(body) == 0, 'PINGRESP mismatch')

    def wait_for_disconnect(self, timeout_sec: float = 3.0) -> tuple[bool, str, float]:
        """Observes actual EOF or connection reset within timeout_sec."""
        start = time.monotonic()
        self.sock.settimeout(timeout_sec)
        try:
            data = self.sock.recv(1024)
            elapsed = time.monotonic() - start
            if len(data) == 0:
                return True, 'EOF (server closed TLS)', elapsed
            return False, f'unexpected data received: {len(data)} bytes', elapsed
        except (ConnectionResetError, ssl.SSLEOFError) as e:
            elapsed = time.monotonic() - start
            return True, f'transport reset ({type(e).__name__})', elapsed
        except socket.timeout:
            elapsed = time.monotonic() - start
            return False, 'timeout (connection remained open)', elapsed
        except Exception as e:
            elapsed = time.monotonic() - start
            return False, f'unexpected error ({type(e).__name__})', elapsed
        finally:
            self.sock.settimeout(5)


def probe_raw_connack(port: int, ca_file: Path, user: str, password: str, client_id: str) -> int:
    """Attempt a fresh TLS CONNECT and return the exact CONNACK return code."""
    context = ssl.create_default_context(cafile=str(ca_file))
    raw_sock = socket.create_connection(('127.0.0.1', port), timeout=5)
    wrapped = context.wrap_socket(raw_sock, server_hostname='localhost')
    wrapped.settimeout(5)
    try:
        body = (text('MQTT') +
                struct.pack('!BBH', 4, 0xC2, 60) +
                text(client_id) +
                text(user) +
                text(password))

        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        wrapped.sendall(bytes([0x10]) + remaining + body)
        code = exact_connack_code(wrapped)
        return code
    finally:
        wrapped.close()


class DynSecProbeFixture:
    """Manages an isolated Compose fixture with exact-ID resource tracking."""
    def __init__(self, base_dir: Path, image_ref: str, *, initialize_backend: bool = False,
                 consumer_rehearsal: bool = False):
        self.d = base_dir.resolve()
        self.image_ref = image_ref
        self.project = 'dynsec_probe_' + uuid.uuid4().hex[:12]
        self.env = dict(runtime.ENV, DYNSEC_FIXTURE=str(self.d), COMPOSE_DISABLE_ENV_FILE='1')
        self.compose_file = self.d / 'compose.probe.yml'
        self.passwords = {
            'admin': secrets.token_urlsafe(32),
            'target': secrets.token_urlsafe(32),
            'observer': secrets.token_urlsafe(32),
        }
        self.initialize_backend = initialize_backend
        self.consumer_rehearsal = consumer_rehearsal
        if initialize_backend:
            self.passwords['backend_service'] = secrets.token_urlsafe(32)
        self.port: int | None = None
        self.open_connections: list[NativeTargetConnection | MQTT] = []

        # Exact receipts captured during lifecycle
        self.owned_containers: list[str] = []
        self.owned_volumes: list[str] = []
        self.owned_networks: list[str] = []
        self.ownership_unknown: list[dict] = []
        self.verified_receipts: list[dict] = []
        self.setup_name = f'{self.project}-setup'

    def consumer_mounts(self) -> tuple[str, str]:
        """Read-only mounts for a UID 1883 consumer; never includes broker certificates."""
        require(self.consumer_rehearsal, 'consumer rehearsal mode is required')
        for role in ('native_store', 'public_ca'):
            require(any(r['kind'] == 'volume' and
                        r['resource_id'] == f'{self.project}_{role}' and
                        r['expected_project'] == self.project and r['expected_role'] == role
                        for r in self.verified_receipts), 'missing volume creation receipt')
        return (f'{self.project}_native_store:/native-store:ro',
                f'{self.project}_public_ca:/public-ca:ro')

    def capture_consumer(self, resource_id: str, role: str, name: str) -> dict:
        """Capture the actual docker create ID before a consumer starts."""
        require(self.consumer_rehearsal, 'consumer rehearsal mode is required')
        if role not in {'backend', 'nginx', 'auth', 'envoy'}:
            return {'status': 'unknown', 'reason': 'invalid_parameters'}
        receipt = inspect_owned_resource('container', resource_id, self.project, role,
                                         expected_name=name)
        if receipt['status'] == 'owned':
            self.record_verified_receipt('container', receipt['resource_id'], role, name)
        else:
            self._record_unknown('container', resource_id, receipt['reason'])
        return receipt

    def _create_consumer_volumes(self):
        """Require authoritative absence, create unique volumes, then capture actual names."""
        for role in ('security', 'native_store', 'public_ca'):
            name = f'{self.project}_{role}'
            presence = inspect_resource_presence('volume', name)
            if presence['status'] != 'absent':
                self._record_unknown('volume', name, 'creation_absence_unverified')
                raise RuntimeError('fixture volume must be authoritatively absent')
            try:
                raw = runtime.run(['docker', 'volume', 'create',
                                   '--label', f'com.docker.compose.project={self.project}',
                                   '--label', f'com.docker.compose.volume={role}', name],
                                  env=self.env, capture=True)
                require(raw.decode().strip() == name, 'volume creation ID mismatch')
            finally:
                result = inspect_owned_resource('volume', name, self.project, role)
                if result['status'] == 'owned':
                    self.record_verified_receipt('volume', name, role)
                else:
                    self._record_unknown('volume', name, result['reason'])
            require(result['status'] == 'owned', 'volume creation ownership unverified')

    def _run_setup(self, script: str, data: bytes):
        if not self.consumer_rehearsal:
            runtime.run(self._compose_cmd('run', '--name', self.setup_name, '--rm', '-T',
                                         'setup', '-ec', script), data=data, env=self.env)
            return
        name = f'{self.project}-setup-{uuid.uuid4().hex[:8]}'
        cmd = ['docker', 'create', '-i', '--name', name, '--network', 'none',
               '--label', f'com.docker.compose.project={self.project}',
               '--label', 'com.docker.compose.service=setup',
               '--label', 'com.docker.compose.oneoff=True',
               '--security-opt', 'no-new-privileges:true', '--cap-drop', 'ALL']
        for cap in ('CHOWN', 'DAC_OVERRIDE', 'FOWNER'):
            cmd += ['--cap-add', cap]
        for mount in (f'{self.project}_security:/security',
                      f'{self.project}_native_store:/native-store',
                      f'{self.project}_public_ca:/public-ca', f'{self.d}/broker:/fixture'):
            cmd += ['--volume', mount]
        cmd += ['--entrypoint', '/bin/sh', self.image_ref, '-ec', script]
        try:
            raw = runtime.run(cmd, env=self.env, capture=True)
            cid = raw.decode().strip()
            require(bool(SAFE_HEX_64_PATTERN.fullmatch(cid)), 'setup creation ID invalid')
            result = inspect_owned_resource('container', cid, self.project, 'setup', expected_name=name)
            require(result['status'] == 'owned', 'setup creation ownership unverified')
            self.record_verified_receipt('container', cid, 'setup', name)
        finally:
            self._discover_and_verify('container', 'setup', name)
        runtime.run(['docker', 'start', '-ai', cid], data=data, env=self.env)

    def _record_unknown(self, kind: str, candidate_id: str, reason: str):
        safe_kind = kind if kind in SUPPORTED_RESOURCE_KINDS else ''
        safe_id = candidate_id if bool(SAFE_RESOURCE_ID_PATTERN.fullmatch(candidate_id)) else ''
        safe_reason = reason if bool(SAFE_IDENTIFIER_PATTERN.fullmatch(reason)) else 'unknown'
        entry = {'kind': safe_kind, 'resource_id': safe_id, 'reason': safe_reason}
        if entry not in self.ownership_unknown:
            self.ownership_unknown.append(entry)

    def record_verified_receipt(self, kind: str, resource_id: str, expected_role: str, expected_name: str | None = None):
        """Authoritatively record a verified creation/capture receipt."""
        receipt = {
            'kind': kind,
            'resource_id': resource_id,
            'expected_project': self.project,
            'expected_role': expected_role,
            'expected_name': expected_name
        }
        if receipt not in self.verified_receipts:
            self.verified_receipts.append(receipt)
        if kind == 'container' and resource_id not in self.owned_containers:
            self.owned_containers.append(resource_id)
        elif kind == 'network' and resource_id not in self.owned_networks:
            self.owned_networks.append(resource_id)
        elif kind == 'volume' and resource_id not in self.owned_volumes:
            self.owned_volumes.append(resource_id)

    def _discover_and_verify(
        self,
        kind: str,
        expected_role: str,
        expected_name: str | None = None,
        discovery_deadline_sec: float = DEFAULT_DISCOVERY_DEADLINE_SEC,
        max_candidates: int = MAX_DISCOVERY_CANDIDATES
    ):
        """Bounded discovery and inspection of resources labeled with this fixture's project."""
        deadline = time.monotonic() + discovery_deadline_sec

        if kind == 'container':
            cmd = ['docker', 'ps', '-a', '--no-trunc', '-q',
                   '--filter', f'label=com.docker.compose.project={self.project}',
                   '--filter', f'label=com.docker.compose.service={expected_role}']
            if expected_name is not None:
                cmd += ['--filter', f'name=^/{expected_name}$']
        elif kind == 'network':
            cmd = ['docker', 'network', 'ls', '--no-trunc', '-q',
                   '--filter', f'label=com.docker.compose.project={self.project}']
        elif kind == 'volume':
            cmd = ['docker', 'volume', 'ls', '-q',
                   '--filter', f'label=com.docker.compose.project={self.project}']
        else:
            self._record_unknown('', '', 'unsupported_kind')
            return

        rem = deadline - time.monotonic()
        if rem <= 0:
            self._record_unknown(kind, '', 'discovery_timeout')
            return

        try:
            proc = subprocess.run(
                cmd,
                env=self.env,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=min(DEFAULT_PER_CALL_CAP_SEC, rem)
            )
        except subprocess.TimeoutExpired:
            self._record_unknown(kind, '', 'discovery_timeout')
            return
        except OSError:
            self._record_unknown(kind, '', 'discovery_os_error')
            return

        if proc.returncode != 0:
            self._record_unknown(kind, '', 'discovery_failed')
            return

        try:
            out_lines = proc.stdout.decode('utf-8').splitlines()
        except Exception:
            self._record_unknown(kind, '', 'discovery_decode_error')
            return

        candidates: list[str] = []
        for line in out_lines:
            cand = line.strip()
            if not cand:
                continue
            if not bool(SAFE_RESOURCE_ID_PATTERN.fullmatch(cand)):
                self._record_unknown(kind, '', 'malformed_candidate_id')
                continue
            if cand not in candidates:
                candidates.append(cand)

        if len(candidates) > max_candidates:
            self._record_unknown(kind, '', 'candidate_count_exceeded')
            return

        for cand in candidates:
            rem_cand = deadline - time.monotonic()
            if rem_cand <= 0:
                self._record_unknown(kind, cand, 'discovery_timeout')
                continue

            receipt = inspect_owned_resource(
                kind=kind,
                candidate_id=cand,
                expected_project=self.project,
                expected_role=expected_role,
                timeout_sec=min(DEFAULT_PER_CALL_CAP_SEC, rem_cand),
                expected_name=expected_name
            )
            if receipt['status'] == 'owned':
                verified_id = receipt['resource_id']
                self.record_verified_receipt(
                    kind=kind,
                    resource_id=verified_id,
                    expected_role=expected_role,
                    expected_name=expected_name
                )
            else:
                self._record_unknown(kind, cand, receipt['reason'])

    def _compose_cmd(self, *args: str) -> list[str]:
        return ['docker', 'compose', '--env-file', '/dev/null', '-p',
                self.project, '-f', str(self.compose_file)] + list(args)

    def prepare(self):
        require(self.d.is_dir(), 'fixture directory must exist')
        broker_dir = self.d / 'broker'
        broker_dir.mkdir(mode=0o700, exist_ok=True)

        # Generate custom compose file with specified image_ref and named volume broker mount
        content = COMPOSE_BASE.read_text()
        content = content.replace('image: eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9',
                                  f'image: {self.image_ref}')
        target_mount = '${DYNSEC_FIXTURE:?protected fixture required}/broker:/fixture:ro'
        replacement_mount = 'security:/fixture:ro'
        require(content.count(target_mount) == 1,
                'expected broker fixture mount template must occur exactly once')
        content = content.replace(target_mount, replacement_mount)
        require(content.count(replacement_mount) == 1,
                'replacement broker fixture mount must occur exactly once')
        if self.consumer_rehearsal:
            self._create_consumer_volumes()
            content = content.replace('      - security:/security',
                                      '      - security:/security\n'
                                      '      - native_store:/native-store\n'
                                      '      - public_ca:/public-ca:ro', 1)
            content = content.replace('    ports: ["127.0.0.1::8883"]',
                                      '    ports: ["127.0.0.1::8883"]\n'
                                      '    networks:\n      default:\n        aliases: [broker]')
            content = content.replace('  security:\n',
                                      f'  security:\n    external: true\n    name: {self.project}_security\n')
            for role in ('native_store', 'public_ca'):
                content += f'  {role}:\n    external: true\n    name: {self.project}_{role}\n'
        self.compose_file.write_text(content)
        self.compose_file.chmod(0o600)

        # Generate CA and server certificates
        for args in (
            ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
             '-subj', '/CN=isolated-ca', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
             '-keyout', 'ca.key', '-out', 'ca.crt'],
            ['req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
             '-keyout', 'server.key', '-out', 'server.csr'],
        ):
            fixed_args = [str(self.d / a) if a.endswith(('.key', '.crt', '.csr')) else a for a in args]
            runtime.run(['openssl'] + fixed_args)

        (self.d / 'extensions').write_text(
            ('subjectAltName=DNS:localhost,DNS:broker\n' if self.consumer_rehearsal
             else 'subjectAltName=DNS:localhost\n') +
                                           'extendedKeyUsage=serverAuth\nbasicConstraints=critical,CA:FALSE\n')
        runtime.run(['openssl', 'x509', '-req', '-in', str(self.d / 'server.csr'),
                     '-CA', str(self.d / 'ca.crt'), '-CAkey', str(self.d / 'ca.key'),
                     '-CAcreateserial', '-days', '1', '-extfile', str(self.d / 'extensions'),
                     '-out', str(self.d / 'server.crt')])

        for name in ('server.key', 'server.crt', 'ca.crt'):
            shutil.copyfile(self.d / name, broker_dir / name)

        (broker_dir / 'broker.conf').write_text(
            'listener 8883\nallow_anonymous false\n'
            'cafile /fixture/ca.crt\ncertfile /fixture/server.crt\nkeyfile /fixture/server.key\n'
            'plugin /usr/lib/mosquitto_dynamic_security.so\n'
            'plugin_opt_config_file /security/dynsec.json\n'
            'log_dest stdout\nlog_type error\nlog_type warning\n'
        )
        if self.consumer_rehearsal:
            config = broker_dir / 'broker.conf'
            config.write_text(config.read_text().replace('/security/dynsec.json',
                                                         '/native-store/dynsec.json'))

        for p in self.d.iterdir():
            if p.is_file():
                p.chmod(0o600)

        # Offline dynsec init via setup service with stdin password
        admin_pw = self.passwords['admin']
        script = (
            'umask 077; mkdir -p /security; chmod 700 /security; '
            'cp /fixture/broker.conf /fixture/ca.crt /fixture/server.crt /fixture/server.key /security/; '
            'chmod 600 /security/broker.conf /security/ca.crt /security/server.crt /security/server.key; '
            'mosquitto_ctrl dynsec init /security/dynsec.json admin >/dev/null; '
            'chmod 600 /security/dynsec.json; '
            'chown -R 1883:1883 /security'
        )
        stdin_data = (admin_pw + '\n' + admin_pw + '\n').encode()
        if self.initialize_backend:
            # Native hashes stay in the private fixture; never capture tool output.
            # Shell builtins split stdin so native stdio cannot consume the next secret.
            script = (
                'umask 077; mkdir -p /security; chmod 700 /security; '
                'cp /fixture/broker.conf /fixture/ca.crt /fixture/server.crt /fixture/server.key /security/; '
                'chmod 600 /security/broker.conf /security/ca.crt /security/server.crt /security/server.key; '
                'IFS= read -r admin_pw; IFS= read -r admin_confirm; '
                'IFS= read -r backend_pw; IFS= read -r backend_confirm; '
                'printf "%s\\n%s\\n" "$admin_pw" "$admin_confirm" | '
                'mosquitto_ctrl dynsec init /security/dynsec.json admin >/dev/null; '
                'printf "%s\\n%s\\n" "$backend_pw" "$backend_confirm" | '
                'mosquitto_ctrl dynsec init /fixture/backend-init.json backend_service >/dev/null; '
                'cp /security/dynsec.json /fixture/admin-init.json; '
                'chmod 600 /fixture/admin-init.json /fixture/backend-init.json; '
                f'chown {os.getuid()}:{os.getgid()} /fixture/admin-init.json /fixture/backend-init.json; '
                'chmod 600 /security/dynsec.json; chown -R 1883:1883 /security'
            )
            stdin_data += ((self.passwords['backend_service'] + '\n') * 2).encode()

        if self.consumer_rehearsal:
            script = script.replace('/security/dynsec.json', '/native-store/dynsec.json')
            script = ('umask 077; mkdir -p /native-store /public-ca; '
                      'chmod 700 /native-store; chmod 755 /public-ca; '
                      'cp /fixture/ca.crt /public-ca/ca.crt; chmod 644 /public-ca/ca.crt; ' +
                      script + '; chown -R 1883:1883 /native-store')
        try:
            self._run_setup(script, stdin_data)
            if self.initialize_backend:
                self._install_backend_store(broker_dir)
        finally:
            if self.initialize_backend:
                for name in ('admin-init.json', 'backend-init.json'):
                    (broker_dir / name).unlink(missing_ok=True)
            # Capture resources created by setup stage immediately, even on failure
            if not self.consumer_rehearsal:
                self._discover_and_verify('volume', expected_role='security')
                self._discover_and_verify('container', expected_role='setup', expected_name=self.setup_name)

    def _install_backend_store(self, broker_dir: Path):
        """Merge native backend credentials offline without changing admin probe ACLs."""
        sources = []
        for name, username in (('admin', 'admin'), ('backend', 'backend_service')):
            with (broker_dir / (name + '-init.json')).open() as stream:
                source = json.loads(stream.read(65537), cls=DuplicateKeyCheckingDecoder)
            require(isinstance(source, dict) and len(source.get('clients', [])) == 1,
                    'unexpected native client schema')
            client = source['clients'][0]
            try:
                valid = (client['username'] == username and
                         len(base64.b64decode(client['password'], validate=True)) == 64 and
                         len(base64.b64decode(client['salt'], validate=True)) == 12 and
                         type(client['iterations']) is int and client['iterations'] > 0)
            except (KeyError, ValueError, TypeError):
                valid = False
            require(valid, 'unexpected native credential schema')
            sources.append(source)
        store, backend_source = sources
        store['defaultACLAccess'] = {kind: False for kind in
                                    ('publishClientSend', 'publishClientReceive', 'subscribe', 'unsubscribe')}
        backend = dict(backend_source['clients'][0])
        backend['roles'] = [{'rolename': 'bootstrap_backend'}]
        store['clients'].append(backend)
        store['roles'].append({'rolename': 'bootstrap_backend', 'acls': [
            {'acltype': kind, 'topic': topic, 'allow': True, 'priority': 0}
            for kind, topic in (
                ('publishClientReceive', 'gateways/+/telemetry/#'),
                ('subscribeLiteral', 'gateways/+/telemetry/#'),
                ('unsubscribeLiteral', 'gateways/+/telemetry/#'),
                ('publishClientSend', 'gateways/+/acks/#'))]})
        script = ('umask 077; cat > /security/dynsec.json; '
                  'chmod 600 /security/dynsec.json; chown -R 1883:1883 /security')
        if self.consumer_rehearsal:
            script = script.replace('/security', '/native-store')
        self._run_setup(script, json.dumps(store, separators=(',', ':')).encode())

    def start(self):
        if self.consumer_rehearsal:
            try:
                runtime.run(self._compose_cmd('create', 'broker'), env=self.env)
            finally:
                self._discover_and_verify('container', 'broker', f'{self.project}-broker-1')
                self._discover_and_verify('network', 'default')
            require(any(r['kind'] == 'container' and r['expected_role'] == 'broker'
                        for r in self.verified_receipts), 'broker creation ownership unverified')
        up_cmd = self._compose_cmd('up', '-d', 'broker')
        try:
            runtime.run(up_cmd, env=self.env)
        finally:
            # Capture container and network immediately after allocation, BEFORE port/readiness
            self._discover_and_verify('container', expected_role='broker', expected_name=f'{self.project}-broker-1')
            self._discover_and_verify('network', expected_role='default')

        port_cmd = self._compose_cmd('port', 'broker', '8883')
        port_raw = runtime.run(port_cmd, env=self.env, capture=True).decode().strip()
        self.port = int(port_raw.rsplit(':', 1)[1])

        # Wait for broker readiness with admin probe
        deadline = time.monotonic() + 10
        ready = False
        while time.monotonic() < deadline:
            try:
                conn = runtime.mqtt_connection(self.port, self.d / 'ca.crt', 'admin', self.passwords['admin'], accepted=True)
                conn.close()
                ready = True
                break
            except (OSError, RuntimeError):
                time.sleep(0.1)
        require(ready, 'broker startup readiness timeout')

    def restart(self):
        # Controlled restart: kill broker with SIGTERM/stop then start again
        self._compose_cmd('stop', 'broker')
        runtime.run(self._compose_cmd('stop', 'broker'), env=self.env)
        self.start()

    def cleanup(self, cleanup_deadline_sec: float = DEFAULT_CLEANUP_DEADLINE_SEC) -> dict:
        """Exact-receipt teardown with post-cleanup presence inspection under a finite shared deadline."""
        valid_deadline = (
            isinstance(cleanup_deadline_sec, (int, float)) and
            not isinstance(cleanup_deadline_sec, bool) and
            not math.isnan(cleanup_deadline_sec) and
            not math.isinf(cleanup_deadline_sec) and
            cleanup_deadline_sec > 0
        )

        receipt = {
            'containers_before': list(self.owned_containers),
            'volumes_before': list(self.owned_volumes),
            'networks_before': list(self.owned_networks),
            'containers_remaining': [],
            'volumes_remaining': [],
            'networks_remaining': [],
            'cleanup_unknown': [],
            'ownership_unknown': list(self.ownership_unknown),
            'clean': False
        }

        def _record_cleanup_unknown(kind: str, resource_id: str, reason: str):
            safe_kind = kind if kind in SUPPORTED_RESOURCE_KINDS else ''
            safe_id = resource_id if bool(SAFE_RESOURCE_ID_PATTERN.fullmatch(resource_id)) else ''
            safe_reason = reason if bool(SAFE_IDENTIFIER_PATTERN.fullmatch(reason)) else 'unknown'
            entry = {'kind': safe_kind, 'resource_id': safe_id, 'reason': safe_reason}
            if entry not in receipt['cleanup_unknown']:
                receipt['cleanup_unknown'].append(entry)

        # 1. Close open sockets
        for conn in self.open_connections:
            try:
                conn.close()
            except Exception:
                pass
        self.open_connections.clear()

        if not valid_deadline:
            _record_cleanup_unknown('', '', 'invalid_budget')
            return receipt

        deadline = time.monotonic() + cleanup_deadline_sec

        # Build index of verified receipts by (kind, resource_id)
        verified_by_key = {}
        for r in self.verified_receipts:
            key = (r.get('kind'), r.get('resource_id'))
            verified_by_key[key] = r

        # 2. Verify and stop/rm containers
        for cid in list(self.owned_containers):
            key = ('container', cid)
            v_receipt = verified_by_key.get(key)
            if not v_receipt:
                _record_cleanup_unknown('container', cid, 'no_capture_receipt')
                continue

            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('container', cid, 'deadline_exhausted')
                continue

            # Re-verify ownership immediately before delete
            v_res = inspect_owned_resource(
                kind='container',
                candidate_id=cid,
                expected_project=v_receipt['expected_project'],
                expected_role=v_receipt['expected_role'],
                timeout_sec=min(DEFAULT_PER_CALL_CAP_SEC, rem),
                expected_name=v_receipt['expected_name']
            )
            if v_res['status'] != 'owned':
                _record_cleanup_unknown('container', cid, v_res['reason'])
                continue

            # Stop container
            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('container', cid, 'deadline_exhausted')
                continue
            try:
                stop_proc = subprocess.run(
                    ['docker', 'stop', '-t', '2', cid],
                    env=self.env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=min(DEFAULT_PER_CALL_CAP_SEC, rem)
                )
                if stop_proc.returncode != 0:
                    _record_cleanup_unknown('container', cid, 'stop_failed')
            except subprocess.TimeoutExpired:
                _record_cleanup_unknown('container', cid, 'stop_timeout')
            except OSError:
                _record_cleanup_unknown('container', cid, 'stop_os_error')

            # Re-verify ownership immediately before rm
            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('container', cid, 'deadline_exhausted')
                continue

            v_res_rm = inspect_owned_resource(
                kind='container',
                candidate_id=cid,
                expected_project=v_receipt['expected_project'],
                expected_role=v_receipt['expected_role'],
                timeout_sec=min(DEFAULT_PER_CALL_CAP_SEC, rem),
                expected_name=v_receipt['expected_name']
            )
            if v_res_rm['status'] != 'owned':
                _record_cleanup_unknown('container', cid, v_res_rm['reason'])
                continue
            if v_res_rm.get('resource_id') != cid:
                _record_cleanup_unknown('container', cid, 'id_mismatch')
                continue

            # Rm container
            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('container', cid, 'deadline_exhausted')
                continue
            try:
                rm_proc = subprocess.run(
                    ['docker', 'rm', '-f', cid],
                    env=self.env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=min(DEFAULT_PER_CALL_CAP_SEC, rem)
                )
                if rm_proc.returncode != 0:
                    _record_cleanup_unknown('container', cid, 'rm_failed')
            except subprocess.TimeoutExpired:
                _record_cleanup_unknown('container', cid, 'rm_timeout')
            except OSError:
                _record_cleanup_unknown('container', cid, 'rm_os_error')

        # 3. Verify and remove networks ONLY after container handling
        for nid in list(self.owned_networks):
            key = ('network', nid)
            v_receipt = verified_by_key.get(key)
            if not v_receipt:
                _record_cleanup_unknown('network', nid, 'no_capture_receipt')
                continue

            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('network', nid, 'deadline_exhausted')
                continue

            v_res = inspect_owned_resource(
                kind='network',
                candidate_id=nid,
                expected_project=v_receipt['expected_project'],
                expected_role=v_receipt['expected_role'],
                timeout_sec=min(DEFAULT_PER_CALL_CAP_SEC, rem),
                expected_name=v_receipt['expected_name']
            )
            if v_res['status'] != 'owned':
                _record_cleanup_unknown('network', nid, v_res['reason'])
                continue

            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('network', nid, 'deadline_exhausted')
                continue
            try:
                net_proc = subprocess.run(
                    ['docker', 'network', 'rm', nid],
                    env=self.env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=min(DEFAULT_PER_CALL_CAP_SEC, rem)
                )
                if net_proc.returncode != 0:
                    _record_cleanup_unknown('network', nid, 'network_rm_failed')
            except subprocess.TimeoutExpired:
                _record_cleanup_unknown('network', nid, 'network_rm_timeout')
            except OSError:
                _record_cleanup_unknown('network', nid, 'network_rm_os_error')

        # 4. Verify and remove volumes ONLY after container handling
        # Volume names lack immutable IDs; verified against expected_name in single-writer/unique-project model
        for vid in list(self.owned_volumes):
            key = ('volume', vid)
            v_receipt = verified_by_key.get(key)
            if not v_receipt:
                _record_cleanup_unknown('volume', vid, 'no_capture_receipt')
                continue

            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('volume', vid, 'deadline_exhausted')
                continue

            v_res = inspect_owned_resource(
                kind='volume',
                candidate_id=vid,
                expected_project=v_receipt['expected_project'],
                expected_role=v_receipt['expected_role'],
                timeout_sec=min(DEFAULT_PER_CALL_CAP_SEC, rem),
                expected_name=v_receipt['expected_name']
            )
            if v_res['status'] != 'owned':
                _record_cleanup_unknown('volume', vid, v_res['reason'])
                continue

            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown('volume', vid, 'deadline_exhausted')
                continue
            try:
                vol_proc = subprocess.run(
                    ['docker', 'volume', 'rm', '-f', vid],
                    env=self.env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=min(DEFAULT_PER_CALL_CAP_SEC, rem)
                )
                if vol_proc.returncode != 0:
                    _record_cleanup_unknown('volume', vid, 'volume_rm_failed')
            except subprocess.TimeoutExpired:
                _record_cleanup_unknown('volume', vid, 'volume_rm_timeout')
            except OSError:
                _record_cleanup_unknown('volume', vid, 'volume_rm_os_error')

        # 5. Final presence proof for every receipt and owned resource
        all_presence_checks = []
        for r in self.verified_receipts:
            item = (r['kind'], r['resource_id'])
            if item not in all_presence_checks:
                all_presence_checks.append(item)
        for cid in self.owned_containers:
            item = ('container', cid)
            if item not in all_presence_checks:
                all_presence_checks.append(item)
        for vid in self.owned_volumes:
            item = ('volume', vid)
            if item not in all_presence_checks:
                all_presence_checks.append(item)
        for nid in self.owned_networks:
            item = ('network', nid)
            if item not in all_presence_checks:
                all_presence_checks.append(item)

        # Resources with unverified capture receipts or revalidation failure should not be presence-inspected
        unknown_rids = {e.get('resource_id') for e in receipt['cleanup_unknown']}

        for kind, rid in all_presence_checks:
            if rid in unknown_rids:
                continue

            rem = deadline - time.monotonic()
            if rem <= 0:
                _record_cleanup_unknown(kind, rid, 'deadline_exhausted')
                continue

            insp = inspect_resource_presence(kind, rid, timeout_sec=min(DEFAULT_PER_CALL_CAP_SEC, rem))
            if insp['status'] == 'exists':
                if kind == 'container' and rid not in receipt['containers_remaining']:
                    receipt['containers_remaining'].append(rid)
                elif kind == 'volume' and rid not in receipt['volumes_remaining']:
                    receipt['volumes_remaining'].append(rid)
                elif kind == 'network' and rid not in receipt['networks_remaining']:
                    receipt['networks_remaining'].append(rid)
            elif insp['status'] == 'unknown':
                _record_cleanup_unknown(kind, rid, 'presence_unknown')

        receipt['clean'] = (
            len(receipt['containers_remaining']) == 0 and
            len(receipt['volumes_remaining']) == 0 and
            len(receipt['networks_remaining']) == 0 and
            len(receipt['cleanup_unknown']) == 0 and
            len(receipt['ownership_unknown']) == 0
        )
        return receipt


FIXTURE_DIR_NAME_PATTERN = re.compile(r'^fixture_[0-9a-f]{8}$')


def _validate_ancestor_chain_no_symlinks(lexical_abs_path: Path) -> None:
    """Walks every component from root to lexical_abs_path using lstat, rejecting any symlink."""
    # lexical_abs_path.parts gives ('/', 'tmp', 'dir', ...)
    curr = Path(lexical_abs_path.parts[0])
    for part in lexical_abs_path.parts[1:]:
        curr = curr / part
        try:
            st = os.lstat(str(curr))
        except FileNotFoundError:
            raise
        except OSError as e:
            raise ValueError(f"failed to lstat ancestor component '{curr}': {e}") from e
        if stat.S_ISLNK(st.st_mode):
            raise ValueError(f"ancestor path '{curr}' is a symlink")


def create_verified_host_fixture_dir(tmp_base: Path) -> tuple[Path, dict]:
    """Atomically creates a unique private host fixture directory directly under tmp_base.

    Captures an immutable creation receipt verifying ownership and filesystem identity:
    - Real directory directly under caller's tmp_base
    - Lexical path validation without symlink resolution
    - Rejects symlink ancestors from root to parent
    - Captures parent device, inode, and UID
    - Private mode (0700)
    - Current process UID
    - Device and inode identifiers via lstat (no symlink resolution)

    Returns:
        tuple[Path, dict]: (fixture_dir, creation_receipt)
    """
    if not isinstance(tmp_base, Path):
        raise TypeError("tmp_base must be a Path")

    # Lexical absolute path without following symlinks (.resolve() is NOT used)
    tmp_base_abs = tmp_base.absolute()

    # Verify no symlinks anywhere in ancestor chain from root to tmp_base
    _validate_ancestor_chain_no_symlinks(tmp_base_abs)

    parent_st = os.lstat(str(tmp_base_abs))
    if not stat.S_ISDIR(parent_st.st_mode) or stat.S_ISLNK(parent_st.st_mode):
        raise ValueError("tmp_base must be a real directory, not a symlink")

    unique_name = f'fixture_{uuid.uuid4().hex[:8]}'
    fixture_dir = tmp_base_abs / unique_name

    # Create private directory (0700)
    os.mkdir(str(fixture_dir), 0o700)

    try:
        # Capture lstat identity without following symlinks
        st = os.lstat(str(fixture_dir))
    except Exception:
        # If stat fails immediately after creation, fail closed
        raise RuntimeError("failed to lstat newly created fixture root")

    if not stat.S_ISDIR(st.st_mode) or stat.S_ISLNK(st.st_mode):
        raise ValueError("fixture root is not a real directory")

    current_uid = os.getuid()
    if st.st_uid != current_uid:
        raise ValueError(f"fixture root UID mismatch: expected {current_uid}, got {st.st_uid}")

    file_mode = stat.S_IMODE(st.st_mode)
    if file_mode != 0o700:
        raise ValueError(f"fixture root mode mismatch: expected 0700, got {oct(file_mode)}")

    receipt = {
        'status': 'created',
        'path': str(fixture_dir),
        'parent': str(tmp_base_abs),
        'name': unique_name,
        'dev': st.st_dev,
        'ino': st.st_ino,
        'uid': current_uid,
        'mode': file_mode,
        'parent_dev': parent_st.st_dev,
        'parent_ino': parent_st.st_ino,
        'parent_uid': parent_st.st_uid,
    }
    return fixture_dir, receipt


def verified_remove_host_fixture_dir(receipt: dict | None) -> dict:
    """Verifies and removes ONLY the unique host fixture root created by this run.

    Pre-removal checks against creation receipt:
    - Valid receipt structure and field types (exact strings, ints, bools excluded)
    - Path must be absolute, direct child of parent, with name matching fixture_<8 lowercase hex>
    - Expected mode is 0700 and UID matches current process UID
    - Lexical parent and ancestor chain verified without symlinks
    - Captured parent device, inode, and UID verified matching current parent lstat
    - Target exists and lstat confirms same device, inode, current UID, mode 0700
    - Target is a real directory and NOT a symlink
    - If target is already absent and valid receipt captured, records verified absent only after parent verification

    Removal:
    - Uses shutil.rmtree without ignore_errors.
    - Python stdlib rmtree avoids symlink traversal on supported platforms (dir_fd / avoids_symlink_attacks).
    - Post-removal lstat explicitly confirms FileNotFoundError.

    Returns sanitized status receipt:
    {
        'status': 'removed' | 'already_absent' | 'unverified' | 'rejected' | 'invalid_receipt',
        'verified': bool,
        'reason': str
    }
    """
    res = {
        'status': 'unverified',
        'verified': False,
        'reason': 'unknown'
    }

    if not isinstance(receipt, dict) or receipt.get('status') != 'created':
        res['status'] = 'invalid_receipt'
        res['reason'] = 'invalid_receipt_format'
        return res

    required_keys = {'path', 'parent', 'name', 'dev', 'ino', 'uid', 'mode', 'parent_dev', 'parent_ino', 'parent_uid'}
    if not required_keys.issubset(receipt.keys()):
        res['status'] = 'invalid_receipt'
        res['reason'] = 'missing_receipt_keys'
        return res

    path_str = receipt['path']
    parent_str = receipt['parent']
    name_str = receipt['name']
    exp_dev = receipt['dev']
    exp_ino = receipt['ino']
    exp_uid = receipt['uid']
    exp_mode = receipt['mode']
    exp_parent_dev = receipt['parent_dev']
    exp_parent_ino = receipt['parent_ino']
    exp_parent_uid = receipt['parent_uid']

    # Strict type validations: path/parent/name must be str, others exact int (not bool)
    if not (isinstance(path_str, str) and isinstance(parent_str, str) and isinstance(name_str, str)):
        res['status'] = 'invalid_receipt'
        res['reason'] = 'invalid_field_types'
        return res

    int_fields = [exp_dev, exp_ino, exp_uid, exp_mode, exp_parent_dev, exp_parent_ino, exp_parent_uid]
    for f in int_fields:
        if not isinstance(f, int) or isinstance(f, bool):
            res['status'] = 'invalid_receipt'
            res['reason'] = 'invalid_field_types'
            return res

    # Validate name shape: fixture_<8 lowercase hex>
    if not FIXTURE_DIR_NAME_PATTERN.fullmatch(name_str):
        res['status'] = 'invalid_receipt'
        res['reason'] = 'invalid_fixture_name'
        return res

    # Validate paths: must be absolute, target must be direct child of parent
    target_path = Path(path_str)
    parent_path = Path(parent_str)

    if not target_path.is_absolute() or not parent_path.is_absolute():
        res['status'] = 'invalid_receipt'
        res['reason'] = 'path_not_absolute'
        return res

    # Reject traversal segments ('..' or '.')
    if '..' in target_path.parts or '..' in parent_path.parts:
        res['status'] = 'invalid_receipt'
        res['reason'] = 'path_traversal_detected'
        return res

    if str(target_path.parent) != parent_str or target_path.name != name_str:
        res['status'] = 'rejected'
        res['reason'] = 'parent_mismatch'
        return res

    if exp_mode != 0o700:
        res['status'] = 'rejected'
        res['reason'] = 'mode_mismatch'
        return res

    if exp_uid != os.getuid():
        res['status'] = 'rejected'
        res['reason'] = 'uid_mismatch'
        return res

    # Revalidate parent ancestor chain and parent identity using lstat before checking target
    try:
        _validate_ancestor_chain_no_symlinks(parent_path)
        cur_parent_st = os.lstat(str(parent_path))
    except (ValueError, FileNotFoundError, OSError):
        res['status'] = 'rejected'
        res['reason'] = 'parent_validation_failed'
        return res

    if not stat.S_ISDIR(cur_parent_st.st_mode) or stat.S_ISLNK(cur_parent_st.st_mode):
        res['status'] = 'rejected'
        res['reason'] = 'parent_not_a_directory'
        return res

    if (cur_parent_st.st_dev != exp_parent_dev or
            cur_parent_st.st_ino != exp_parent_ino or
            cur_parent_st.st_uid != exp_parent_uid):
        res['status'] = 'rejected'
        res['reason'] = 'parent_identity_mismatch'
        return res

    # Check current state of target root using lstat (no symlink following)
    try:
        cur_st = os.lstat(path_str)
    except FileNotFoundError:
        # Valid receipt captured and parent identity verified; root is already absent
        res['status'] = 'already_absent'
        res['verified'] = True
        res['reason'] = 'verified_absent'
        return res
    except OSError:
        # Permission or other OS error is unknown, not absence
        res['status'] = 'unverified'
        res['reason'] = 'lstat_os_error'
        return res

    # Reject symlinks unconditionally
    if stat.S_ISLNK(cur_st.st_mode):
        res['status'] = 'rejected'
        res['reason'] = 'symlink_root_rejected'
        return res

    if not stat.S_ISDIR(cur_st.st_mode):
        res['status'] = 'rejected'
        res['reason'] = 'not_a_directory'
        return res

    # Identity and ownership verification
    if cur_st.st_dev != exp_dev:
        res['status'] = 'rejected'
        res['reason'] = 'device_mismatch'
        return res

    if cur_st.st_ino != exp_ino:
        res['status'] = 'rejected'
        res['reason'] = 'inode_mismatch'
        return res

    if cur_st.st_uid != exp_uid or cur_st.st_uid != os.getuid():
        res['status'] = 'rejected'
        res['reason'] = 'uid_mismatch'
        return res

    if stat.S_IMODE(cur_st.st_mode) != exp_mode or stat.S_IMODE(cur_st.st_mode) != 0o700:
        res['status'] = 'rejected'
        res['reason'] = 'mode_mismatch'
        return res

    # Enforce safe removal without ignore_errors
    # Limitation: Concurrent mutation by a trusted privileged process during deletion
    # cannot be completely prevented without kernel filesystem namespace isolation.
    if not getattr(shutil.rmtree, 'avoids_symlink_attacks', False):
        res['status'] = 'unverified'
        res['reason'] = 'unsupported_symlink_safe_rmtree'
        return res

    try:
        shutil.rmtree(path_str, ignore_errors=False)
    except OSError:
        res['status'] = 'unverified'
        res['reason'] = 'rmtree_failed'
        return res

    # Explicit post-removal verification: must confirm FileNotFoundError
    try:
        os.lstat(path_str)
        # If lstat succeeds, root was not removed
        res['status'] = 'unverified'
        res['reason'] = 'root_still_present'
        return res
    except FileNotFoundError:
        res['status'] = 'removed'
        res['verified'] = True
        res['reason'] = 'verified_absent'
        return res
    except OSError:
        res['status'] = 'unverified'
        res['reason'] = 'post_lstat_os_error'
        return res


def compute_probe_outcomes(results: dict) -> tuple[bool, bool]:
    """Computes pure independent (pass_all_core, pass_all_required) outcome booleans.

    Core requires exact boolean True for steps 1 through 6.
    Required requires core True, step7 True, and all cleanup receipts verified True.
    """
    if not isinstance(results, dict):
        return False, False

    core_steps = [
        results.get('step1_init_and_admin'),
        results.get('step2_create_client_and_role'),
        results.get('step3_dual_session_functional'),
        results.get('step4_disable_command_and_readback'),
        results.get('step5_active_sessions_disconnected'),
        results.get('step5_observer_unaffected'),
        results.get('step6_same_password_rejected'),
    ]
    pass_all_core = all(x is True for x in core_steps)

    step7_val = results.get('step7_persistence_check')
    step7_pass = (step7_val is True)

    cleanup_receipt = results.get('cleanup_receipt')
    clean_pass = False
    if isinstance(cleanup_receipt, dict):
        docker_clean = (cleanup_receipt.get('docker_clean') is True)
        host_cleanup = cleanup_receipt.get('host_directory_cleanup')
        host_verified = isinstance(host_cleanup, dict) and (host_cleanup.get('verified') is True)
        overall_clean = (cleanup_receipt.get('clean') is True)
        clean_pass = (docker_clean and host_verified and overall_clean)

    pass_all_required = bool(pass_all_core and step7_pass and clean_pass)
    return pass_all_core, pass_all_required


def run_capability_probe(image_ref: str, tmp_base: Path, test_persistence: bool = True) -> dict:
    """Executes the exact native capability test sequence for a given image reference."""
    results = {
        'image_ref': image_ref,
        'image_id': None,
        'broker_version': None,
        'step1_init_and_admin': False,
        'step2_create_client_and_role': False,
        'step3_dual_session_functional': False,
        'step4_disable_command_and_readback': False,
        'step5_active_sessions_disconnected': False,
        'step5_disconnect_details': {},
        'step5_observer_unaffected': False,
        'step6_same_password_rejected': False,
        'step6_connack_code': None,
        'step7_persistence_check': False,
        'persistence_stage': 'pending' if test_persistence else 'skipped',
        'cleanup_receipt': None,
        'failure_stage': None,
        'failure_code': None,
        'pass_all_core': False,
        'pass_all_required': False,
        'aggregate_pass': False,
    }

    # 1. Preflight Image Validation (Zero subprocess / allocation on mismatch)
    if not isinstance(image_ref, str) or image_ref != runtime.IMAGE:
        results['failure_stage'] = 'preflight'
        results['failure_code'] = 'invalid_image_reference'
        return results

    # 2. Bounded Image Inspection with sanitized runtime.ENV
    try:
        insp = subprocess.run(
            ['docker', 'image', 'inspect', image_ref, '--format', '{{.Id}}'],
            env=runtime.ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=DEFAULT_IMAGE_INSPECT_TIMEOUT_SEC
        )
    except subprocess.TimeoutExpired:
        results['failure_stage'] = 'preflight'
        results['failure_code'] = 'image_inspect_timeout'
        return results
    except OSError:
        results['failure_stage'] = 'preflight'
        results['failure_code'] = 'image_inspect_os_error'
        return results

    if insp.returncode != 0:
        results['failure_stage'] = 'preflight'
        results['failure_code'] = 'image_inspect_failed'
        return results

    raw_img_id = insp.stdout.strip()
    if not bool(SAFE_IMAGE_CONFIG_ID_PATTERN.fullmatch(raw_img_id)):
        results['failure_stage'] = 'preflight'
        results['failure_code'] = 'image_inspect_malformed'
        return results

    results['image_id'] = raw_img_id

    host_receipt = None
    try:
        fixture_dir, host_receipt = create_verified_host_fixture_dir(tmp_base)
    except Exception:
        results['failure_stage'] = 'host_dir_setup'
        results['failure_code'] = 'setup_error'
        return results

    fixture = DynSecProbeFixture(fixture_dir, image_ref)
    failure_stage = None

    try:
        try:
            # Step 1: Bootstrap & Start
            failure_stage = 'step1_init_and_admin'
            fixture.prepare()
            fixture.start()

            # Verify broker container receipt exists and revalidate identity
            broker_receipt = None
            for r in fixture.verified_receipts:
                if (
                    r.get('kind') == 'container' and
                    r.get('expected_role') == 'broker' and
                    r.get('expected_project') == fixture.project and
                    r.get('expected_name') == f'{fixture.project}-broker-1'
                ):
                    broker_receipt = r
                    break

            if not broker_receipt:
                results['failure_code'] = 'broker_receipt_missing'
                raise RuntimeError('broker container creation receipt missing')

            cid = broker_receipt['resource_id']
            v_res = inspect_owned_resource(
                kind='container',
                candidate_id=cid,
                expected_project=broker_receipt['expected_project'],
                expected_role=broker_receipt['expected_role'],
                timeout_sec=DEFAULT_EXEC_TIMEOUT_SEC,
                expected_name=broker_receipt['expected_name']
            )
            if v_res.get('status') != 'owned':
                results['failure_code'] = 'broker_revalidation_failed'
                raise RuntimeError(f"broker container revalidation failed: {v_res.get('reason')}")

            # Obtain broker version via bounded docker exec in already tracked broker container
            try:
                v_proc = subprocess.run(
                    ['docker', 'exec', cid, 'mosquitto', '-h'],
                    env=runtime.ENV,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=DEFAULT_EXEC_TIMEOUT_SEC
                )
            except subprocess.TimeoutExpired:
                results['failure_code'] = 'broker_version_timeout'
                raise TimeoutError('broker version exec timed out')
            except OSError:
                results['failure_code'] = 'broker_version_os_error'
                raise OSError('broker version exec failed')

            if v_proc.returncode not in (0, 3):
                results['failure_code'] = 'broker_version_failed'
                raise RuntimeError(f'broker version exec returned unexpected code: {v_proc.returncode}')

            matched_ver = None
            for line in v_proc.stdout.splitlines():
                stripped = line.strip()
                if bool(SAFE_BROKER_VERSION_PATTERN.fullmatch(stripped)):
                    matched_ver = stripped
                    break

            if not matched_ver:
                results['failure_code'] = 'broker_version_mismatch'
                raise ValueError('broker version did not match expected anchored pattern')

            results['broker_version'] = matched_ver
            results['step1_init_and_admin'] = True

            # Helper wrapper for admin MQTT control
            class FakeSpike:
                def __init__(self, port, d):
                    self.port = port
                    self.d = d

            fake_spike = FakeSpike(fixture.port, fixture.d)
            admin = MQTT(fake_spike, 'admin', fixture.passwords['admin'])
            fixture.open_connections.append(admin)
            admin.subscribe(RESPONSE)

            # Step 2: Create target client, observer client, and roles
            failure_stage = 'step2_create_client_and_role'
            # Create roles for target and observer
            admin.request('createRole', rolename='target_role')
            admin.request('addRoleACL', rolename='target_role', acltype='publishClientSend', topic='probe/target/#', allow=True, priority=1)
            admin.request('addRoleACL', rolename='target_role', acltype='publishClientReceive', topic='probe/target/#', allow=True, priority=1)
            admin.request('addRoleACL', rolename='target_role', acltype='subscribePattern', topic='probe/target/#', allow=True, priority=1)

            admin.request('createRole', rolename='observer_role')
            admin.request('addRoleACL', rolename='observer_role', acltype='publishClientSend', topic='probe/observer/#', allow=True, priority=1)
            admin.request('addRoleACL', rolename='observer_role', acltype='publishClientReceive', topic='probe/observer/#', allow=True, priority=1)
            admin.request('addRoleACL', rolename='observer_role', acltype='subscribePattern', topic='probe/observer/#', allow=True, priority=1)

            # Create target client: createClient -> setClientPassword -> addClientRole
            target_user = 'target_client'
            target_pw = fixture.passwords['target']
            admin.request('createClient', username=target_user)
            admin.request('setClientPassword', username=target_user, password=target_pw)
            admin.request('addClientRole', username=target_user, rolename='target_role')

            # Create observer client
            observer_user = 'observer_client'
            observer_pw = fixture.passwords['observer']
            admin.request('createClient', username=observer_user)
            admin.request('setClientPassword', username=observer_user, password=observer_pw)
            admin.request('addClientRole', username=observer_user, rolename='observer_role')

            # Connect observer
            observer = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', observer_user, observer_pw, 'obs_1')
            fixture.open_connections.append(observer)
            observer.subscribe('probe/observer/echo')

            # Connect TWO target devices with different client IDs sharing target_user/target_pw
            target_dev1 = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_1')
            target_dev2 = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_2')
            fixture.open_connections.append(target_dev1)
            fixture.open_connections.append(target_dev2)
            results['step2_create_client_and_role'] = True

            # Step 3: Prove both target connections function immediately before disable
            failure_stage = 'step3_dual_session_functional'
            target_dev1.subscribe('probe/target/dev1')
            target_dev2.subscribe('probe/target/dev2')

            # Bidirectional message exchanges
            msg1 = b'ping_from_dev1_' + uuid.uuid4().hex[:8].encode()
            target_dev1.publish('probe/target/dev1', msg1)
            top1, rcv1 = target_dev1.message()
            require(top1 == 'probe/target/dev1' and rcv1 == msg1, 'target 1 echo delivery failed')

            msg2 = b'ping_from_dev2_' + uuid.uuid4().hex[:8].encode()
            target_dev2.publish('probe/target/dev2', msg2)
            top2, rcv2 = target_dev2.message()
            require(top2 == 'probe/target/dev2' and rcv2 == msg2, 'target 2 echo delivery failed')

            # Keep alive check
            target_dev1.ping()
            target_dev2.ping()
            results['step3_dual_session_functional'] = True

            # Step 4: Send ONE disableClient command and read back
            failure_stage = 'step4_disable_command_and_readback'
            admin.request('disableClient', username=target_user)
            client_info = admin.request('getClient', username=target_user)
            require(client_info.get('client', {}).get('disabled') is True, 'getClient disabled field not true')
            results['step4_disable_command_and_readback'] = True

            # Step 5: Require actual EOF / connection reset on both already-open target connections
            failure_stage = 'step5_active_sessions_disconnected'
            disc1, detail1, elapsed1 = target_dev1.wait_for_disconnect(timeout_sec=3.0)
            disc2, detail2, elapsed2 = target_dev2.wait_for_disconnect(timeout_sec=3.0)
            results['step5_disconnect_details'] = {
                'dev1': {'disconnected': disc1, 'detail': detail1, 'elapsed_sec': round(elapsed1, 4)},
                'dev2': {'disconnected': disc2, 'detail': detail2, 'elapsed_sec': round(elapsed2, 4)},
            }
            require(disc1 and disc2, f'target connections not disconnected: dev1={disc1}, dev2={disc2}')
            results['step5_active_sessions_disconnected'] = True

            # Verify observer is unaffected and exchanges a fresh payload
            failure_stage = 'step5_observer_unaffected'
            obs_msg = b'obs_payload_' + uuid.uuid4().hex[:8].encode()
            observer.publish('probe/observer/echo', obs_msg)
            obs_top, obs_rcv = observer.message()
            require(obs_top == 'probe/observer/echo' and obs_rcv == obs_msg, 'observer communication failed after target disable')
            results['step5_observer_unaffected'] = True

            # Step 6: Attempt fresh TLS CONNECT with SAME valid password
            failure_stage = 'step6_same_password_rejected'
            connack_code = probe_raw_connack(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_fresh')
            results['step6_connack_code'] = connack_code
            # In MQTT 3.1.1, return code 0x04 = Bad user name or password, 0x05 = Not authorized.
            # Dynsec returns 0x05 or 0x04 for disabled/unauthorized client.
            require(connack_code in (0x04, 0x05), f'expected rejection CONNACK (0x04 or 0x05), got 0x{connack_code:02x}')
            results['step6_same_password_rejected'] = True

            # Step 7: Persistence check
            if test_persistence:
                failure_stage = 'step7_persistence_check'
                fixture.restart()
                fake_spike_after = FakeSpike(fixture.port, fixture.d)
                # Verify admin still connects and can read disabled status
                admin_after = MQTT(fake_spike_after, 'admin', fixture.passwords['admin'])
                fixture.open_connections.append(admin_after)
                admin_after.subscribe(RESPONSE)
                client_info_after = admin_after.request('getClient', username=target_user)
                require(client_info_after.get('client', {}).get('disabled') is True, 'restart: disabled not preserved')

                # Verify same-password rejection persists
                code_after = probe_raw_connack(fixture.port, fixture.d / 'ca.crt', target_user, target_pw, 'dev_target_after_restart')
                require(code_after in (0x04, 0x05), f'restart: expected rejection CONNACK, got 0x{code_after:02x}')

                # Verify observer still connects with its password
                obs_after = NativeTargetConnection(fixture.port, fixture.d / 'ca.crt', observer_user, observer_pw, 'obs_after')
                fixture.open_connections.append(obs_after)
                obs_after.subscribe('probe/observer/echo')
                obs_after.publish('probe/observer/echo', b'after_restart_msg')
                top_a, rcv_a = obs_after.message()
                require(top_a == 'probe/observer/echo' and rcv_a == b'after_restart_msg', 'observer failed after restart')

                results['step7_persistence_check'] = True
                results['persistence_stage'] = 'succeeded'
        except Exception as ex:
            results['failure_stage'] = failure_stage or 'unknown_stage'
            if failure_stage == 'step7_persistence_check':
                results['persistence_stage'] = 'failed'
            # Record small, safe, sanitized failure code without raw exception message/tokens/secrets
            if results.get('failure_code') is None:
                if isinstance(ex, TimeoutError):
                    results['failure_code'] = 'timeout'
                elif isinstance(ex, ConnectionError):
                    results['failure_code'] = 'connection_error'
                elif isinstance(ex, OSError):
                    results['failure_code'] = 'os_error'
                elif isinstance(ex, ValueError):
                    results['failure_code'] = 'value_error'
                elif isinstance(ex, AssertionError):
                    results['failure_code'] = 'assertion_error'
                elif isinstance(ex, RuntimeError):
                    results['failure_code'] = 'runtime_error'
                else:
                    results['failure_code'] = 'probe_execution_error'
    finally:
        docker_cleanup_receipt = None
        try:
            docker_cleanup_receipt = fixture.cleanup()
        except Exception:
            docker_cleanup_receipt = {
                'clean': False,
                'containers_remaining': [],
                'volumes_remaining': [],
                'networks_remaining': [],
                'cleanup_unknown': [{'kind': 'docker', 'resource_id': '', 'reason': 'cleanup_exception'}],
                'ownership_unknown': [],
            }

        # Verified host fixture directory removal
        host_cleanup_res = verified_remove_host_fixture_dir(host_receipt)

        # Separate docker and host status
        docker_is_clean = (
            isinstance(docker_cleanup_receipt, dict) and
            docker_cleanup_receipt.get('clean') is True
        )
        host_is_clean = (
            isinstance(host_cleanup_res, dict) and
            host_cleanup_res.get('verified') is True
        )

        overall_cleanup_receipt = dict(docker_cleanup_receipt)
        overall_cleanup_receipt['docker_clean'] = docker_is_clean
        overall_cleanup_receipt['host_directory_cleanup'] = host_cleanup_res
        overall_cleanup_receipt['clean'] = docker_is_clean and host_is_clean

        results['cleanup_receipt'] = overall_cleanup_receipt

        # Compute independent outcomes only after cleanup accounting completes
        core_pass, req_pass = compute_probe_outcomes(results)
        results['pass_all_core'] = core_pass
        results['pass_all_required'] = req_pass
        results['aggregate_pass'] = req_pass

    return results
