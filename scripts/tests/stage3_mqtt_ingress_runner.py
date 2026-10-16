#!/usr/bin/env python3
"""Task 3.4 bounded real integration slice: MQTT telemetry ingress smoke.

Disposable fixture stack, all owned resources created and removed by exact ID
under /tmp/opencode:

  * pinned TimescaleDB container (reused DatabaseFixture), migrations 1..20,
    seeded Gateway/Sensor graph with sentinel desired/Gateway state;
  * pinned Mosquitto 2.0.18 broker using the production config file (with the
    new max_packet_size/message_size_limit limits), TLS and a fresh DynSec store
    created by the existing container bootstrap helper (bootstrap_backend);
  * the actual cmd/server executable with the new MQTT ingress composition.

Flow proven once through the real broker: a distinct Gateway principal
subscribes to its own ACK filter (SUBACK verified first), publishes a QoS 1
non-retained 100-sample batch, and receives the five-field post-commit
application ACK. Independent SQL asserts 100 raw / 1 ledger / 1 temporal
observation, the Sensor current state advanced from the last sample with version
+1, and desired/Gateway state unchanged. Resending the exact topic/bytes/id is
deduplicated and re-ACKed without new rows or version change.

No deployment .env, deployment cert scripts, Auth/Storage stack, or SQLite
simulator is used. Secrets travel only via env files and stdin; stdout/stderr
are sanitized and raw payloads are never logged.

Two modes share the same disposable fixture (broker + TimescaleDB + Go backend)
so the Task 3.4 ingress slice stays selectable and is never bypassed:

  * ``--mode ingress`` (default): the Task 3.4 bounded Python MQTT producer
    publishes one hand-built 100-sample batch, then resends it byte-exact.
  * ``--mode simulator``: the *actual* ``cmd/gateway`` CLI with the durable
    SQLite outbox (``--batches 1``) publishes one synthetic 100-sample batch
    against the broker, and independent SQL checks the known synthetic envelope
    (sequence/sample_index/time/value/provenance) plus the reported state
    advance and preserved desired/Gateway state.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))
from task28_simplified_schema_runner import DatabaseFixture, ENV, sanitize_output  # noqa: E402
from stage3_persistence_runner import bootstrap_to_9, run_existing_migration_runner  # noqa: E402
from task26_harness_helpers import MQTT, RESPONSE  # noqa: E402

MOSQUITTO_IMAGE = 'eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9'
BOOTSTRAP_SCRIPT = ROOT / 'scripts/mqtt-bootstrap.py'
MOSQUITTO_CONF = ROOT / 'config/mosquitto/mosquitto.conf'

GATEWAY_ID = 'gw_stage3_smoke'
SENSOR_ID = 'sensor_stage3_smoke'
SENSOR_UNIT = 'degC'
GATEWAY_URN = f'urn:ngsi-ld:Gateway:{GATEWAY_ID}'
SENSOR_URN = f'urn:ngsi-ld:Sensor:{GATEWAY_ID}:{SENSOR_ID}'

SAMPLE_COUNT = 100
FIRST_SEQUENCE = 1000
SAMPLE_INTERVAL_US = 10000
# Integer-valued samples keep the reported-state comparison exact (no float
# rounding between Go, JSONB numeric and PostgreSQL float8).
SAMPLES = [1000.0 + i for i in range(SAMPLE_COUNT)]
LAST_VALUE = int(SAMPLES[-1])
LAST_SEQUENCE = FIRST_SEQUENCE + SAMPLE_COUNT - 1
MEASURED_AT = '2026-10-01T00:00:00.000Z'
# The final sample is measured at measured_at + (sample_count-1)*interval. It is
# both the Sensor reported_state.observed_at and the temporal observed_at, so the
# smoke compares those exact provenance timestamps instead of only row counts.
LAST_OBSERVED_AT = (
    datetime.strptime(MEASURED_AT, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    + timedelta(microseconds=SAMPLE_INTERVAL_US * (SAMPLE_COUNT - 1))
).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
TELEMETRY_TOPIC = f'gateways/{GATEWAY_ID}/telemetry/{SENSOR_ID}'
ACK_FILTER = f'gateways/{GATEWAY_ID}/acks/#'

SENSOR_DESIRED = {'sentinel': 'keep'}
SENSOR_DESIRED_VERSION = 7
GATEWAY_REPORTED = {'connectionStatus': 'online'}
GATEWAY_DESIRED = {'gw_desired': 'keep'}

CONTAINER_TIMEOUT = 120
CONTAINER_START_TIMEOUT = 60
BROKER_READY_TIMEOUT = 45
READY_TIMEOUT = 60
ACK_TIMEOUT = 20
LOG_TIMEOUT = 30

SECRETS: list[str] = []


# --- Gateway simulator (Task 3.5) slice configuration -----------------------

# The simulator connects to the host-published broker port via localhost so the
# TLS server certificate SAN (DNS:localhost) is verified.
SIM_BROKER_HOST = 'localhost'
SIM_CLIENT_ID = 'gw_stage3_smoke_sim'
SIM_COMPLETION_TIMEOUT = '30s'
SIM_RUN_TIMEOUT = 120
SIM_ADMIN_TIMEOUT = 30
SIM_PROFILE = {
    'GATEWAY_SAMPLE_RATE_HZ': '100',
    'GATEWAY_SAMPLES_PER_BATCH': '100',
    'GATEWAY_ACK_TIMEOUT': '5s',
    'GATEWAY_RETRY_MIN_DELAY': '250ms',
    'GATEWAY_RETRY_MAX_DELAY': '2s',
    'GATEWAY_MQTT_CONNECT_TIMEOUT': '10s',
    'GATEWAY_MQTT_SUBSCRIBE_TIMEOUT': '5s',
    'GATEWAY_MQTT_PUBLISH_TIMEOUT': '5s',
    'GATEWAY_SQLITE_TIMEOUT': '5s',
    'GATEWAY_SHUTDOWN_TIMEOUT': '10s',
    'GATEWAY_MAX_INFLIGHT': '8',
}
# The simulator emits syntheticValue(sequence) = 20 + clamped(sequence % 2000)/100
# (a triangular wave between 20.00 and 30.00). These constants mirror
# internal/gatewaysim/batch.go so the smoke checks the known synthetic output
# independently of the Go code.
SIM_PERIOD = 2000
SIM_BASE = 20.0
SIM_LAST_VALUE = SIM_BASE + (SAMPLE_COUNT - 1) / 100.0


@dataclass(frozen=True)
class Envelope:
    """Parameters for the shared SQL state assertion.

    message_id/boot_id are None when the assertion derives them from the
    processed_messages ledger (simulator mode); when set they are the known
    ingress values. last_observed_at is None when the assertion derives it from
    the raw telemetry max(measured_at).
    """

    message_id: str | None
    boot_id: str | None
    sample_count: int
    first_sequence: int
    sample_interval_us: int
    last_value: float
    value_is_int: bool
    last_observed_at: str | None
    expected_reported_version: int
    verify_rows: bool = False

    @property
    def last_sequence(self) -> int:
        return self.first_sequence + self.sample_count - 1



def sanitize(text: str) -> str:
    return sanitize_output(text, SECRETS)


def run(args, *, input_data=None, timeout=30, expect=True, sanitize_out=True):
    """Run a bounded subprocess; never echo raw output on failure."""
    proc = subprocess.run(args, env=ENV, input=input_data, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    if (proc.returncode == 0) != expect:
        clean = sanitize((proc.stdout + proc.stderr).decode('utf-8', errors='replace')) if sanitize_out \
            else '(output suppressed)'
        raise RuntimeError(f'command failed ({args[0]} ...): {clean}')
    return proc.stdout


def docker(args, **kwargs):
    return run(['docker', *args], **kwargs)


def container_logs(cid: str) -> str:
    """Merge the container's stdout and stderr; docker logs separates them."""
    proc = subprocess.run(['docker', 'logs', cid], env=ENV, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=LOG_TIMEOUT)
    return proc.stdout.decode('utf-8', errors='replace')


def docker_create(args) -> str:
    out = docker(['create', *args], timeout=CONTAINER_TIMEOUT)
    cid = out.decode('utf-8').strip().splitlines()[-1].strip()
    if len(cid) < 12:
        raise RuntimeError('docker create returned no container id')
    return cid


def container_absent(cid: str) -> bool:
    proc = subprocess.run(['docker', 'inspect', cid], env=ENV,
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
    if proc.returncode == 0:
        return False
    err = proc.stderr.decode('utf-8', errors='replace').lower()
    return 'no such container' in err or 'no such object' in err


def remove_container(cid: str):
    docker(['rm', '-f', '-v', cid], timeout=60)
    if not container_absent(cid):
        raise RuntimeError(f'container {cid} still present after removal')


def network_absent(name: str) -> bool:
    proc = subprocess.run(['docker', 'network', 'inspect', name], env=ENV,
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
    if proc.returncode == 0:
        return False
    err = proc.stderr.decode('utf-8', errors='replace').lower()
    return 'no such network' in err or 'not found' in err


def volume_absent(name: str) -> bool:
    proc = subprocess.run(['docker', 'volume', 'inspect', name], env=ENV,
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
    if proc.returncode == 0:
        return False
    return 'no such volume' in proc.stderr.decode('utf-8', errors='replace').lower()


def mapped_port(cid: str, container_port: str) -> int:
    out = docker(['port', cid, container_port]).decode('utf-8').strip()
    if not out:
        raise RuntimeError('no published port')
    return int(out.splitlines()[-1].rsplit(':', 1)[1])


def write_private(path: Path, data: bytes, mode=0o600):
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)


def generate_certs(work: Path):
    """Disposable CA + broker server certificate with localhost/broker SANs."""
    d = work
    for args in (
        ['req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
         '-subj', '/CN=stage3-smoke-ca', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
         '-keyout', str(d / 'ca.key'), '-out', str(d / 'ca.crt')],
        ['req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
         '-keyout', str(d / 'server.key'), '-out', str(d / 'server.csr')],
    ):
        run(['openssl', *args])
    (d / 'extensions').write_text(
        'subjectAltName=DNS:localhost,DNS:broker\n'
        'extendedKeyUsage=serverAuth\n'
        'basicConstraints=critical,CA:FALSE\n'
        'keyUsage=critical,digitalSignature,keyEncipherment\n')
    run(['openssl', 'x509', '-req', '-in', str(d / 'server.csr'), '-CA', str(d / 'ca.crt'),
         '-CAkey', str(d / 'ca.key'), '-CAcreateserial', '-days', '1',
         '-extfile', str(d / 'extensions'), '-out', str(d / 'server.crt')])
    # Producer and broker config read the CA; keep it readable (test-only).
    for name in ('ca.crt', 'server.crt', 'server.key'):
        (d / name).chmod(0o644)

    broker_dir = d / 'broker'
    certs = broker_dir / 'certs'
    certs.mkdir(mode=0o755, parents=True)
    shutil.copy(MOSQUITTO_CONF, broker_dir / 'mosquitto.conf')
    (broker_dir / 'mosquitto.conf').chmod(0o644)
    for name in ('ca.crt', 'server.crt', 'server.key'):
        shutil.copy(d / name, certs / name)
        (certs / name).chmod(0o644)

    public = d / 'public'
    public.mkdir(mode=0o755)
    shutil.copy(d / 'ca.crt', public / 'ca.crt')
    (public / 'ca.crt').chmod(0o644)


def build_go_binary(work: Path, package: str, name: str) -> Path:
    binary = work / name
    env = dict(ENV, CGO_ENABLED='0')
    for key in ('GOCACHE', 'GOMODCACHE', 'GOPROXY', 'GOPATH'):
        if key in os.environ:
            env[key] = os.environ[key]
    env.setdefault('GOCACHE', str(work / 'gocache'))
    proc = subprocess.run(['go', 'build', '-o', str(binary), package],
                          cwd=ROOT / 'src', env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=300)
    if proc.returncode:
        raise RuntimeError(f'{name} build failed: ' +
                           proc.stderr.decode('utf-8', errors='replace')[-4000:])
    binary.chmod(0o755)
    return binary


def build_server(work: Path) -> Path:
    return build_go_binary(work, './cmd/server', 'server')


def build_gateway(work: Path) -> Path:
    return build_go_binary(work, './cmd/gateway', 'gateway')


def bootstrap_dynsec(work: Path, manager_password: str, backend_password: str) -> str:
    config = json.dumps({
        'manager': 'bootstrap_manager',
        'backend': 'bootstrap_backend',
        'manager_password': manager_password,
        'backend_password': backend_password,
    }).encode()
    candidate = work / 'bootstrap-config.json'
    write_private(candidate, config)
    try:
        out = run([sys.executable, str(BOOTSTRAP_SCRIPT), '--container', '--config', str(candidate)],
                  timeout=240)
    finally:
        candidate.unlink(missing_ok=True)
    try:
        parsed = json.loads(out.decode('utf-8'))
    except ValueError:
        raise RuntimeError('dynsec bootstrap did not emit JSON') from None
    if parsed.get('status') != 'store_created' or not parsed.get('volume_name'):
        raise RuntimeError('dynsec bootstrap did not create a store')
    return parsed['volume_name']


def wait_tcp(port: int, timeout: int):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(('127.0.0.1', port), 1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError('broker port never accepted TCP connections')


def wait_http(port: int, path: str, timeout: int):
    deadline = time.monotonic() + timeout
    last = ''
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=2) as resp:
                if resp.status == 200:
                    return resp.read()
                last = f'HTTP {resp.status}'
        except Exception as exc:  # noqa: BLE001 - bounded probe
            last = type(exc).__name__
        time.sleep(0.3)
    raise RuntimeError(f'{path} never became ready ({last})')


def connect_manager(port: int, work: Path, manager_password: str):
    deadline = time.monotonic() + BROKER_READY_TIMEOUT
    while True:
        try:
            return MQTT(_Endpoint(port, work), 'bootstrap_manager', manager_password)
        except Exception:  # noqa: BLE001 - bounded readiness probe
            if time.monotonic() > deadline:
                raise RuntimeError('bootstrap_manager TLS login never succeeded') from None
            time.sleep(0.3)


class _Endpoint:
    def __init__(self, port: int, directory: Path):
        self.port = port
        self.d = directory


def mqtt_text(value: str) -> bytes:
    raw = value.encode()
    return struct.pack('!H', len(raw)) + raw


class GatewayProducer:
    """Minimal bounded MQTT 3.1.1 QoS1 producer with exact SUBACK verification."""

    def __init__(self, port: int, ca: Path, username: str, password: str):
        from task26_harness_helpers import mqtt_connection  # reuse existing TLS connect
        self.s = mqtt_connection(port, ca, username, password)
        self.s.settimeout(ACK_TIMEOUT)
        self.pid = 0

    def close(self):
        self.s.close()

    def _send(self, header: int, body: bytes):
        remaining = bytearray()
        size = len(body)
        while True:
            digit = size % 128
            size //= 128
            remaining.append(digit | (128 if size else 0))
            if not size:
                break
        self.s.sendall(bytes([header]) + remaining + body)

    def _exact(self, n: int) -> bytes:
        data = bytearray()
        while len(data) < n:
            chunk = self.s.recv(n - len(data))
            if not chunk:
                raise RuntimeError('connection closed')
            data.extend(chunk)
        return bytes(data)

    def _packet(self):
        header = self._exact(1)[0]
        size = 0
        for i in range(4):
            digit = self._exact(1)[0]
            size += (digit & 127) << (7 * i)
            if not digit & 128:
                if size > 1 << 20:
                    raise RuntimeError('response exceeds message size bound')
                return header, self._exact(size)
        raise RuntimeError('invalid MQTT frame')

    def subscribe(self, topic: str):
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self._send(0x82, pid + mqtt_text(topic) + b'\x01')
        header, body = self._packet()
        if header != 0x90 or body != pid + b'\x01':
            raise RuntimeError('gateway ACK-filter SUBACK was not granted QoS1')

    def publish_qos1(self, topic: str, payload: bytes):
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self._send(0x32, mqtt_text(topic) + pid + payload)
        header, body = self._packet()
        if header != 0x40 or body != pid:
            raise RuntimeError('gateway PUBACK mismatch')

    def read_publish(self):
        header, body = self._packet()
        if header & 0xF0 != 0x30:
            raise RuntimeError('expected a PUBLISH frame')
        qos = (header >> 1) & 0x03
        name_len = struct.unpack('!H', body[:2])[0]
        topic = body[2:2 + name_len].decode()
        rest = body[2 + name_len:]
        if qos > 0:
            self._send(0x40, rest[:2])
            rest = rest[2:]
        return topic, rest


def build_payload() -> bytes:
    return json.dumps({
        'protocol_version': 1,
        'message_id': str(uuid.uuid4()),
        'message_type': 'telemetry_batch',
        'gateway_id': GATEWAY_ID,
        'sensor_id': SENSOR_ID,
        'boot_id': str(uuid.uuid4()),
        'first_sequence': FIRST_SEQUENCE,
        'sample_count': SAMPLE_COUNT,
        'measured_at': MEASURED_AT,
        'sample_interval_us': SAMPLE_INTERVAL_US,
        'samples': [int(v) for v in SAMPLES],
    }, separators=(',', ':')).encode()


def verify_ack(message_id: str, raw: bytes):
    payload = json.loads(raw.decode('utf-8'))
    expected_fields = {'protocol_version', 'message_type', 'message_id', 'status', 'committed_at'}
    if set(payload) != expected_fields:
        raise RuntimeError(f'ACK field set is not the exact five-field contract: {sorted(payload)}')
    if payload.get('protocol_version') != 1:
        raise RuntimeError('ACK protocol_version is not 1')
    if payload.get('message_type') != 'telemetry_ack':
        raise RuntimeError('ACK message_type is not telemetry_ack')
    if payload.get('message_id') != message_id:
        raise RuntimeError('ACK message_id does not match the batch')
    if payload.get('status') != 'committed':
        raise RuntimeError('ACK status is not committed')
    committed = payload.get('committed_at')
    if not isinstance(committed, str):
        raise RuntimeError('ACK committed_at is missing')
    parsed = datetime.fromisoformat(committed.replace('Z', '+00:00'))
    if parsed.tzinfo is None or parsed.year < 2000:
        raise RuntimeError('ACK committed_at is not a valid UTC timestamp')


def seed_graph(fixture: DatabaseFixture):
    fixture.sql(f"""
      INSERT INTO gateways(gateway_id,name) VALUES ('{GATEWAY_ID}','Stage 3 smoke gateway');
      INSERT INTO sensors(gateway_id,sensor_id,name,unit)
        VALUES ('{GATEWAY_ID}','{SENSOR_ID}','Stage 3 smoke sensor','{SENSOR_UNIT}');
      INSERT INTO twin_entities(entity_id,entity_type,gateway_id)
        VALUES ('{GATEWAY_URN}','Gateway','{GATEWAY_ID}'),
               ('{SENSOR_URN}','Sensor','{GATEWAY_ID}');
      INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id)
        SELECT p.id,'hasSensor',c.id FROM twin_entities p,twin_entities c
        WHERE p.entity_id='{GATEWAY_URN}' AND c.entity_id='{SENSOR_URN}';
      INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities;
      UPDATE twin_states SET desired_state='{json.dumps(SENSOR_DESIRED)}'::jsonb,
             desired_version={SENSOR_DESIRED_VERSION}
        WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='{SENSOR_URN}');
      UPDATE twin_states SET reported_state='{json.dumps(GATEWAY_REPORTED)}'::jsonb,
             desired_state='{json.dumps(GATEWAY_DESIRED)}'::jsonb,
             reported_version=3, desired_version=3
        WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='{GATEWAY_URN}');
    """.encode())


def assert_phase(fixture: DatabaseFixture, marker: str, env: Envelope):
    """One shared, independent SQL state assertion for both modes.

    It checks the ledger row, the raw telemetry rows, the single temporal
    observation, the Sensor reported-state advance from the last sample and the
    preserved desired/Gateway state. The expectation is supplied by ``env``: the
    ingress slice keeps its fixed 1000..1099 envelope, while the simulator
    validates its actual synthetic envelope and also checks per-row
    sequence/sample_index/time/value consistency.
    """
    def q(value: str) -> str:
        return "'" + value + "'"

    if env.message_id is not None:
        msg_known = (f"IF msg <> {q(env.message_id)}::uuid THEN "
                     "RAISE EXCEPTION 'ledger message_id %', msg; END IF;")
    else:
        msg_known = ''
    if env.boot_id is not None:
        boot_known = (f"IF boot <> {q(env.boot_id)}::uuid THEN "
                      "RAISE EXCEPTION 'ledger boot_id %', boot; END IF;")
    else:
        boot_known = ''
    if env.last_observed_at is not None:
        obs_sql = f"TIMESTAMPTZ {q(env.last_observed_at)}"
    else:
        obs_sql = ("(SELECT max(measured_at) FROM public.telemetry "
                   f"WHERE gateway_id={q(GATEWAY_ID)} AND sensor_id={q(SENSOR_ID)} AND message_id = msg)")
    if env.value_is_int:
        value_check = (f"IF (rep->>'value')::bigint <> {int(env.last_value)} THEN "
                       "RAISE EXCEPTION 'reported value %', rep->>'value'; END IF;")
    else:
        value_check = (f"IF abs((rep->>'value')::double precision - {env.last_value!r}) > 1e-9 THEN "
                       "RAISE EXCEPTION 'reported value %', rep->>'value'; END IF;")

    if env.verify_rows:
        span_us = (env.sample_count - 1) * env.sample_interval_us
        raw_filter = (f"t.gateway_id={q(GATEWAY_ID)} AND t.sensor_id={q(SENSOR_ID)} "
                      "AND t.message_id = msg")
        row_checks = f"""
      IF EXISTS (
        SELECT 1 FROM public.telemetry t
        WHERE {raw_filter}
          AND ( t.sequence_num < {env.first_sequence} OR t.sequence_num > {env.last_sequence}
                OR t.sample_index <> t.sequence_num - {env.first_sequence}
                OR t.boot_id <> boot
                OR abs(t.value - ({SIM_BASE} + (CASE WHEN mod(t.sequence_num, {SIM_PERIOD}) > {SIM_PERIOD // 2}
                     THEN {SIM_PERIOD} - mod(t.sequence_num, {SIM_PERIOD})
                     ELSE mod(t.sequence_num, {SIM_PERIOD}) END)::double precision / 100.0)) > 1e-9 )
      ) THEN RAISE EXCEPTION 'telemetry rows inconsistent with the synthetic envelope'; END IF;
      IF (SELECT count(DISTINCT t.sequence_num) FROM public.telemetry t WHERE {raw_filter}) <> {env.sample_count}
        THEN RAISE EXCEPTION 'telemetry distinct sequence count mismatch'; END IF;
      IF (SELECT count(DISTINCT t.sample_index) FROM public.telemetry t WHERE {raw_filter}) <> {env.sample_count}
        THEN RAISE EXCEPTION 'telemetry distinct sample_index count mismatch'; END IF;
      IF (SELECT min(measured_at) FROM public.telemetry t WHERE {raw_filter})
         + {span_us} * INTERVAL '1 microsecond'
         <> (SELECT max(measured_at) FROM public.telemetry t WHERE {raw_filter})
        THEN RAISE EXCEPTION 'telemetry time span does not match the sample interval'; END IF;
"""
    else:
        row_checks = ''

    sql = f"""
    DO $$
    DECLARE
      raw_count int; ledger int; temporal int;
      msg uuid; boot uuid;
      l_first bigint; l_last bigint; l_count int; l_sensor text; l_topic text;
      t_val double precision; t_text text; t_bool boolean; t_meta jsonb; t_obs timestamptz;
      rep jsonb; des jsonb; rep_ver bigint; des_ver bigint;
      gw_rep jsonb; gw_des jsonb;
    BEGIN
      SELECT count(*) INTO ledger FROM public.processed_messages
        WHERE gateway_id={q(GATEWAY_ID)} AND sensor_id={q(SENSOR_ID)};
      IF ledger <> 1 THEN RAISE EXCEPTION 'ledger rows %', ledger; END IF;
      SELECT message_id, boot_id, first_sequence, last_sequence, sample_count, sensor_id, source_topic
        INTO msg, boot, l_first, l_last, l_count, l_sensor, l_topic
        FROM public.processed_messages
        WHERE gateway_id={q(GATEWAY_ID)} AND sensor_id={q(SENSOR_ID)};
      IF l_first <> {env.first_sequence} OR l_last <> {env.last_sequence} OR l_count <> {env.sample_count} THEN
        RAISE EXCEPTION 'ledger envelope first/last/count % % %', l_first, l_last, l_count; END IF;
      IF l_sensor <> {q(SENSOR_ID)} THEN RAISE EXCEPTION 'ledger sensor_id %', l_sensor; END IF;
      IF l_topic <> {q(TELEMETRY_TOPIC)} THEN RAISE EXCEPTION 'ledger source_topic %', l_topic; END IF;
      {msg_known}
      {boot_known}
      SELECT count(*) INTO raw_count FROM public.telemetry
        WHERE gateway_id={q(GATEWAY_ID)} AND sensor_id={q(SENSOR_ID)} AND message_id = msg;
      IF raw_count <> {env.sample_count} THEN RAISE EXCEPTION 'raw count %', raw_count; END IF;
      SELECT count(*) INTO temporal FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(SENSOR_URN)})
          AND property_name='sample_interval_us' AND message_id = msg;
      IF temporal <> 1 THEN RAISE EXCEPTION 'temporal rows %', temporal; END IF;
      SELECT value_number, value_text, value_boolean, metadata, observed_at
        INTO t_val, t_text, t_bool, t_meta, t_obs
        FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(SENSOR_URN)})
          AND property_name='sample_interval_us' AND message_id = msg;
      IF t_val <> {env.sample_interval_us} THEN RAISE EXCEPTION 'temporal value_number %', t_val; END IF;
      IF t_text IS NOT NULL OR t_bool IS NOT NULL THEN
        RAISE EXCEPTION 'temporal typed columns are not exclusive'; END IF;
      IF t_meta->>'unit' <> 'us' OR t_meta->>'source' <> 'telemetry_batch' OR t_meta->>'boot_id' <> boot::text THEN
        RAISE EXCEPTION 'temporal metadata %', t_meta; END IF;
      IF t_obs <> {obs_sql} THEN RAISE EXCEPTION 'temporal observed_at %', t_obs; END IF;
      SELECT reported_state, desired_state, reported_version, desired_version
        INTO rep, des, rep_ver, des_ver FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(SENSOR_URN)});
      IF rep_ver <> {env.expected_reported_version} THEN RAISE EXCEPTION 'sensor reported_version %', rep_ver; END IF;
      {value_check}
      IF (rep->>'sequence_num')::bigint <> {env.last_sequence} THEN RAISE EXCEPTION 'reported sequence %', rep->>'sequence_num'; END IF;
      IF (rep->>'sample_interval_us')::bigint <> {env.sample_interval_us} THEN RAISE EXCEPTION 'reported interval %', rep->>'sample_interval_us'; END IF;
      IF rep->>'unit' IS DISTINCT FROM {q(SENSOR_UNIT)} THEN RAISE EXCEPTION 'reported unit %', rep->>'unit'; END IF;
      IF rep->>'boot_id' <> boot::text THEN RAISE EXCEPTION 'reported boot_id %', rep->>'boot_id'; END IF;
      IF (rep->>'observed_at')::timestamptz <> {obs_sql} THEN
        RAISE EXCEPTION 'reported observed_at %', rep->>'observed_at'; END IF;
      {row_checks}
      IF des <> {q(json.dumps(SENSOR_DESIRED))}::jsonb OR des_ver <> {SENSOR_DESIRED_VERSION} THEN
        RAISE EXCEPTION 'sensor desired state changed'; END IF;
      SELECT reported_state, desired_state INTO gw_rep, gw_des FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(GATEWAY_URN)});
      IF gw_rep <> {q(json.dumps(GATEWAY_REPORTED))}::jsonb OR gw_des <> {q(json.dumps(GATEWAY_DESIRED))}::jsonb THEN
        RAISE EXCEPTION 'gateway state changed'; END IF;
      RAISE NOTICE '{marker}';
    END $$;
    """
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'SQL assertions did not emit {marker}')


def parse_run_summary(stdout: str) -> dict:
    """Parse the CLI's single ``run summary:`` line into a key=value mapping."""
    for line in stdout.splitlines():
        if line.startswith('run summary:'):
            summary = {}
            for token in line[len('run summary:'):].split():
                if '=' in token:
                    key, value = token.split('=', 1)
                    summary[key] = value
            return summary
    raise RuntimeError('gateway run did not print a run summary')


def run_gateway(binary: Path, args, env, timeout: int, expect: bool = True) -> str:
    """Run the CLI with a bounded subprocess timeout; never echo raw output."""
    proc = subprocess.run([str(binary), *args], env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    out = proc.stdout.decode('utf-8', errors='replace')
    err = proc.stderr.decode('utf-8', errors='replace')
    if (proc.returncode == 0) != expect:
        raise RuntimeError(f'gateway {" ".join(args)} exit={proc.returncode}: ' +
                           sanitize((out[-600:] + err[-600:])))
    return out


def run_simulator_producer(gateway_bin: Path, work: Path, broker_port: int, gateway_password: str):
    """Run the actual cmd/gateway CLI through the broker for one finite batch.

    The CLI subscribes to its own ACK filter before any publish (readiness gate)
    and only deletes a row after the application ACK commits. The run must exit 0
    with a truthful completed/counted summary, and the path-only count/pending
    administrative reports must show an empty outbox.
    """
    outbox_path = work / 'gateway_outbox.db'
    env = dict(ENV)
    env.update(SIM_PROFILE)
    env.update({
        'GATEWAY_ID': GATEWAY_ID,
        'SENSOR_ID': SENSOR_ID,
        'GATEWAY_MQTT_BROKER_URL': f'ssl://{SIM_BROKER_HOST}:{broker_port}',
        'GATEWAY_MQTT_CLIENT_ID': SIM_CLIENT_ID,
        'GATEWAY_MQTT_USERNAME': GATEWAY_ID,
        'GATEWAY_MQTT_PASSWORD': gateway_password,
        'GATEWAY_MQTT_CA_FILE': str(work / 'ca.crt'),
        'GATEWAY_OUTBOX_PATH': str(outbox_path),
    })

    stdout = run_gateway(gateway_bin, ['run', '--batches', '1', '--completion-timeout', SIM_COMPLETION_TIMEOUT],
                         env, SIM_RUN_TIMEOUT)
    summary = parse_run_summary(stdout)
    truthful = (summary.get('completed') == 'true' and summary.get('enqueued') == '1'
                and summary.get('app_acknowledged') == '1' and summary.get('pending_rows') == '0'
                and summary.get('pending_bytes') == '0' and summary.get('quarantined_rows') == '0'
                and summary.get('quarantined_bytes') == '0')
    if not truthful:
        raise RuntimeError('gateway run summary is not a truthful completed/0-pending report: ' +
                           sanitize(stdout[-600:]))
    print('PASS: gateway CLI run exit 0 with completed=true enqueued=%s app_acknowledged=%s '
          'pending_rows=%s quarantined_rows=%s' % (summary.get('enqueued'), summary.get('app_acknowledged'),
                                                   summary.get('pending_rows'), summary.get('quarantined_rows')),
          flush=True)

    # Path-only administrative reports: the outbox must be empty with no
    # quarantined rows, and pending must list nothing.
    admin_env = dict(ENV, GATEWAY_OUTBOX_PATH=str(outbox_path))
    count_out = run_gateway(gateway_bin, ['count'], admin_env, SIM_ADMIN_TIMEOUT).strip()
    if count_out != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
        raise RuntimeError('gateway count did not report an empty outbox: ' + sanitize(count_out))
    pending_out = run_gateway(gateway_bin, ['pending', '--limit', '5'], admin_env, SIM_ADMIN_TIMEOUT)
    if 'pending message_id=' in pending_out or 'pending_rows=0' not in pending_out:
        raise RuntimeError('gateway pending did not report an empty outbox: ' + sanitize(pending_out.strip()))
    print('PASS: gateway count/pending (path-only) report 0 pending and 0 quarantined', flush=True)


def simulator_envelope() -> Envelope:
    """Expectation for the simulator's one synthetic batch (known synthetic
    output); the ledger message_id/boot_id and the observed_at are derived from
    the committed rows by the shared SQL assertion."""
    return Envelope(
        message_id=None,
        boot_id=None,
        sample_count=SAMPLE_COUNT,
        first_sequence=0,
        sample_interval_us=SAMPLE_INTERVAL_US,
        last_value=SIM_LAST_VALUE,
        value_is_int=False,
        last_observed_at=None,
        expected_reported_version=1,
        verify_rows=True,
    )


def docker_available() -> bool:
    try:
        subprocess.run(['docker', 'version', '--format', '{{.Server.Version}}'], env=ENV,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15, check=True)
        return True
    except Exception:  # noqa: BLE001
        return False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Task 3.4/3.5 bounded real integration smoke')
    parser.add_argument('--mode', choices=('ingress', 'simulator'), default='ingress',
                        help='ingress: Task 3.4 Python producer; simulator: actual cmd/gateway CLI')
    opts = parser.parse_args(argv)
    mode = opts.mode

    if not docker_available():
        print('SKIP: docker daemon or CLI unavailable', flush=True)
        return 0

    work = Path(tempfile.mkdtemp(prefix='stage3-ingress-', dir='/tmp/opencode'))
    os.chmod(work, 0o700)
    hexid = secrets.token_hex(6)
    net_name = f'stage3_ingress_net_{hexid}'
    fixture = None
    network_created = False
    dynsec_volume = None
    broker_cid = None
    backend_cid = None
    primary_error = None

    manager_password = secrets.token_urlsafe(48)
    backend_password = secrets.token_urlsafe(48)
    gateway_password = secrets.token_urlsafe(48)
    jwt_secret = secrets.token_urlsafe(48)
    SECRETS.extend([manager_password, backend_password, gateway_password, jwt_secret])

    try:
        print('stage=build-server', flush=True)
        server = build_server(work)
        gateway_bin = None
        if mode == 'simulator':
            print('stage=build-gateway', flush=True)
            gateway_bin = build_gateway(work)
        print('stage=generate-certs', flush=True)
        generate_certs(work)

        print('stage=create-network', flush=True)
        docker(['network', 'create', net_name], timeout=30)
        network_created = True

        print('stage=database-fixture', flush=True)
        fixture = DatabaseFixture()
        SECRETS.extend(fixture.passwords)
        fixture.prepare_image()
        fixture.create()
        fixture.start()
        fixture.wait_ready()
        docker(['network', 'connect', '--alias', 'database', net_name, fixture.container_id], timeout=30)

        print('stage=migrate-000020', flush=True)
        bootstrap_to_9(fixture)
        run_existing_migration_runner(fixture, 20)
        seed_graph(fixture)

        print('stage=bootstrap-dynsec', flush=True)
        dynsec_volume = bootstrap_dynsec(work, manager_password, backend_password)

        print('stage=start-broker', flush=True)
        broker_cid = docker_create([
            '--name', f'stage3-ingress-broker-{hexid}',
            '--user', '1883:1883',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=16m',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--network-alias', 'broker',
            '--publish', '127.0.0.1::8883',
            '--mount', f'type=bind,src={work / "broker"},dst=/mosquitto/config,readonly',
            '--mount', f'type=volume,src={dynsec_volume},dst=/mosquitto/dynsec',
            '--entrypoint', '/usr/sbin/mosquitto',
            MOSQUITTO_IMAGE,
            '-c', '/mosquitto/config/mosquitto.conf',
        ])
        docker(['start', broker_cid], timeout=CONTAINER_START_TIMEOUT)
        broker_port = mapped_port(broker_cid, '8883/tcp')
        wait_tcp(broker_port, BROKER_READY_TIMEOUT)

        print('stage=dynsec-control', flush=True)
        manager = connect_manager(broker_port, work, manager_password)
        try:
            manager.subscribe(RESPONSE)
            manager.request('createClient', username=GATEWAY_ID, password=gateway_password)
            manager.request('createRole', rolename='gateway_ingress_smoke', acls=[
                {'acltype': 'publishClientSend', 'topic': f'gateways/{GATEWAY_ID}/telemetry/#', 'allow': True, 'priority': 0},
                {'acltype': 'publishClientReceive', 'topic': ACK_FILTER, 'allow': True, 'priority': 0},
                {'acltype': 'subscribeLiteral', 'topic': ACK_FILTER, 'allow': True, 'priority': 0},
                {'acltype': 'unsubscribeLiteral', 'topic': ACK_FILTER, 'allow': True, 'priority': 0},
            ])
            manager.request('addClientRole', username=GATEWAY_ID, rolename='gateway_ingress_smoke')
        finally:
            manager.close()
        print('DynSec control traffic with new packet limits: PASS', flush=True)

        print('stage=start-backend', flush=True)
        env_file = work / 'backend.env'
        backend_env = {
            'DATABASE_URL': f'postgres://iot_backend_app:{fixture.passwords[1]}@database:5432/fixture_service?sslmode=disable',
            'SUPABASE_JWT_SECRET': jwt_secret,
            'SUPABASE_JWT_ISSUER': 'http://auth.invalid/auth/v1',
            'SUPABASE_JWT_AUDIENCE': 'authenticated',
            'SERVER_ENV': 'smoke',
            'MQTT_INGRESS_ENABLED': 'true',
            'MQTT_BROKER_URL': 'ssl://broker:8883',
            'MQTT_CLIENT_ID': 'backend_ingress_smoke',
            'MQTT_USERNAME': 'bootstrap_backend',
            'MQTT_PASSWORD': backend_password,
            'MQTT_TLS_CA_FILE': '/certs/ca.crt',
            'MQTT_CREDENTIAL_RUNTIME_ENABLED': 'false',
            'MQTT_QUEUE_CAPACITY': '256',
            'MQTT_WORKER_COUNT': '2',
            'MQTT_MAX_PAYLOAD_BYTES': '65536',
            'DATABASE_MAX_CONNS': '10',
            'SHUTDOWN_TIMEOUT': '10s',
        }
        write_private(env_file, ''.join(f'{k}={v}\n' for k, v in backend_env.items()).encode())
        backend_cid = docker_create([
            '--name', f'stage3-ingress-backend-{hexid}',
            '--user', '1883:1883',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--network-alias', 'backend',
            '--publish', '127.0.0.1::8080',
            '--env-file', str(env_file),
            '--mount', f'type=bind,src={work / "public" / "ca.crt"},dst=/certs/ca.crt,readonly',
            '--mount', f'type=bind,src={server},dst=/app/server,readonly',
            '--entrypoint', '/app/server',
            MOSQUITTO_IMAGE,
        ])
        docker(['start', backend_cid], timeout=CONTAINER_START_TIMEOUT)
        backend_port = mapped_port(backend_cid, '8080/tcp')
        wait_http(backend_port, '/readyz', READY_TIMEOUT)
        wait_http(backend_port, '/healthz', READY_TIMEOUT)
        print('PASS: backend /readyz and /healthz after positive ingress subscription', flush=True)

        if mode == 'simulator':
            print('stage=simulator', flush=True)
            run_simulator_producer(gateway_bin, work, broker_port, gateway_password)
            assert_phase(fixture, 'SIMULATOR_STATE_PASS', simulator_envelope())
            print('PASS: independent SQL asserts 100 raw synthetic rows (sequence/sample_index/time/'
                  'value formula) / 1 ledger / 1 temporal (value+metadata+observed_at) / reported last '
                  'value from the synthetic envelope with unit+boot_id+observed_at+interval +1 / '
                  'desired & gateway preserved', flush=True)
        else:
            print('stage=producer', flush=True)
            payload = build_payload()
            batch = json.loads(payload.decode())
            message_id = batch['message_id']
            # The Dashboard-reported provenance (boot_id) and observed_at must be the
            # exact values the payload carried, so capture the batch boot_id.
            boot_id = batch['boot_id']
            producer = GatewayProducer(broker_port, work / 'ca.crt', GATEWAY_ID, gateway_password)
            try:
                producer.subscribe(ACK_FILTER)   # SUBACK verified before any publish
                producer.publish_qos1(TELEMETRY_TOPIC, payload)
                ack_topic, ack_body = producer.read_publish()
            finally:
                producer.close()
            expected_topic = f'gateways/{GATEWAY_ID}/acks/{message_id}'
            if ack_topic != expected_topic:
                raise RuntimeError(f'ACK topic mismatch: {ack_topic}')
            verify_ack(message_id, ack_body)
            print('PASS: post-commit application ACK received on exact topic/status/version/type/id', flush=True)

            ingress_env = Envelope(
                message_id=message_id,
                boot_id=boot_id,
                sample_count=SAMPLE_COUNT,
                first_sequence=FIRST_SEQUENCE,
                sample_interval_us=SAMPLE_INTERVAL_US,
                last_value=float(LAST_VALUE),
                value_is_int=True,
                last_observed_at=LAST_OBSERVED_AT,
                expected_reported_version=1,
            )
            assert_phase(fixture, 'PHASE1_STATE_PASS', ingress_env)
            print('PASS: SQL asserts 100 raw / 1 ledger / 1 temporal (value+metadata+observed_at) / '
                  'reported state from last sample with unit+boot_id+observed_at+interval +1 / desired & gateway preserved', flush=True)

            # Resend the exact topic/bytes/id: duplicate branch must re-ACK with no new rows.
            print('stage=resend', flush=True)
            producer = GatewayProducer(broker_port, work / 'ca.crt', GATEWAY_ID, gateway_password)
            try:
                producer.subscribe(ACK_FILTER)
                producer.publish_qos1(TELEMETRY_TOPIC, payload)
                ack_topic2, ack_body2 = producer.read_publish()
            finally:
                producer.close()
            if ack_topic2 != expected_topic:
                raise RuntimeError(f'resend ACK topic mismatch: {ack_topic2}')
            verify_ack(message_id, ack_body2)
            assert_phase(fixture, 'PHASE2_DUPLICATE_PASS', ingress_env)
            print('PASS: exact resend re-ACKed with unchanged counts and version', flush=True)

        print('stage=shutdown', flush=True)
        run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
        exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
        if exit_code != '0':
            raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
        logs = container_logs(backend_cid)
        if 'backend shutdown incomplete' in logs or 'panic:' in logs:
            raise RuntimeError('backend shutdown reported an incomplete/panicked teardown; log=' +
                               sanitize(logs[-1200:]))
        if 'backend shutdown complete' not in logs:
            raise RuntimeError('backend did not log a confirmed complete shutdown; log=' +
                               sanitize(logs[-1200:]))
        print('PASS: normal SIGTERM shutdown within budget with confirmed complete log', flush=True)
    except Exception as exc:  # noqa: BLE001 - report sanitized, preserve for cleanup
        primary_error = exc
        raise
    finally:
        cleanup_errors = []
        for label, action in (
            ('backend container', lambda: remove_container(backend_cid) if backend_cid else None),
            ('broker container', lambda: remove_container(broker_cid) if broker_cid else None),
            ('database fixture', lambda: (fixture.cleanup() or None) if fixture else None),
            ('network', lambda: (docker(['network', 'rm', net_name], timeout=30) and None) if network_created else None),
            ('dynsec volume', lambda: (docker(['volume', 'rm', dynsec_volume], timeout=30) and None) if dynsec_volume else None),
        ):
            try:
                action()
            except Exception as cleanup_exc:  # noqa: BLE001
                cleanup_errors.append(f'{label}: {sanitize(str(cleanup_exc))}')
        try:
            shutil.rmtree(work, ignore_errors=True)
        except Exception as cleanup_exc:  # noqa: BLE001
            cleanup_errors.append(f'workdir: {sanitize(str(cleanup_exc))}')

        remaining = []
        if backend_cid and not container_absent(backend_cid):
            remaining.append('backend container')
        if broker_cid and not container_absent(broker_cid):
            remaining.append('broker container')
        if network_created and not network_absent(net_name):
            remaining.append('network')
        if dynsec_volume and not volume_absent(dynsec_volume):
            remaining.append('dynsec volume')
        if work.exists():
            remaining.append('workdir')

        if cleanup_errors or remaining:
            message = 'cleanup incomplete: ' + '; '.join(cleanup_errors + [f'still present: {r}' for r in remaining])
            if primary_error is None:
                print(f'FAIL: {sanitize(message)}', file=sys.stderr, flush=True)
                return 1
            print(f'cleanup warning: {sanitize(message)}', file=sys.stderr, flush=True)
        else:
            print('Fixture cleanup verified: container/network/volume/workdir absent', flush=True)

    if mode == 'simulator':
        print('PASS: stage3 gateway simulator end-to-end smoke '
              '(cmd/gateway CLI + durable SQLite -> broker -> backend -> DB -> app ACK)', flush=True)
    else:
        print('PASS: stage3 mqtt ingress end-to-end smoke (broker -> backend -> DB -> ACK)', flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f'FAIL: {sanitize(str(exc))}', file=sys.stderr, flush=True)
        raise
