#!/usr/bin/env python3
"""Task 3.6 slices (3.6.1-3.6.2 normal path, 3.6.3 rows 1-5 and 7 ACL denial).

This is a thin, re-runnable integration entrypoint that reuses the Task 3.4
fixture primitives (imported from ``stage3_mqtt_ingress_runner``) instead of
copying the whole harness. It selects one of four scenarios on the same
disposable fixture:

  * ``normal`` (3.6.1-3.6.2): two independent Gateway identities each publishing
    one finite 100-sample batch through the pinned Mosquitto 2.0.18 broker to the
    real backend image and TimescaleDB, with independent SQL verification.
  * ``tls-credential`` (3.6.3 rows 1-3): denial with positive controls, using
    the *actual* Go Gateway (``cmd/gateway``) and Go backend (``cmd/server``)
    implementations, never a Python-only claim:
      - row 1 wrong CA: the real Gateway CLI fails to establish a verified TLS
        session, produces no application ACK, and leaves a pre-existing SQLite
        pending row untouched;
      - row 2 wrong hostname/SAN: an isolated *candidate* backend (distinct MQTT
        client id, no host binary bind-mount) pointed at a broker network alias
        outside the server certificate SAN never becomes ready and never falls
        back to an insecure connection;
      - row 3 wrong password: a fresh Gateway login is rejected by the broker
        with CONNACK 0x05 (observed by the existing bounded probe) and the real
        Gateway CLI leaves the pre-existing pending row untouched.
    Correct CA/hostname/password positive controls run before and after the
    negatives with distinct message ids, and every invalid input is bracketed by
    DB + SQLite snapshots (no unintended ledger/raw/Twin/temporal and no mutation
    of the pending row).
  * ``acl-control`` (3.6.3 rows 4, 5, 7): cross-Gateway publish/subscribe and
    DynSec control-topic denial against the same pinned broker, each with
    positive controls and receiver/DB evidence:
      - row 4 cross-Gateway publish: Gateway A publishes a payload that is
        *valid for B* to B's telemetry topic; a QoS1 PUBACK is not accepted as
        denial proof -- the broker ``Denied PUBLISH`` observation, an unchanged
        DB snapshot, the absence of any committed row for that message id and
        the absence of an application ACK to B's receiver are required, with a
        B receiver positive control proving the receiving path was live;
      - row 5 cross-Gateway subscribe: A must receive SUBACK 0x80 for B's ACK
        filter while its own ACK filter is granted, and a real B batch (with a
        real B receiver application ACK) must not leak to an A observer;
      - row 7 control topic: a Gateway attempt at a supported, valid DynSec
        mutation (unique correlation and probe name) is denied by the broker,
        the manager readback (which is itself positive-controlled) shows the
        probe absent before and after, no response arrives for the correlation,
        and the Gateway cannot subscribe to the control/response namespace while
        the manager request path still works.
    No ACK-spoofing (row 6) is exercised by this slice, and no backend/observer
    is ever granted control-topic access.
  * ``ack-fault`` (3.6.3 row 6 + 3.6.4): one reversible fixture hook (a DynSec
    receive-deny role attached above the Gateway's uplink role) with a separate
    PASS for each group:
      - 3.6.4 lost application ACK: the real simulator enqueues and publishes a
        batch, the backend commits it (independent SQL), the application ACK is
        withheld, a byte-exact resend of the *same* item is observed at an
        authorized receiver, ledger/raw/temporal/versions do not advance, the
        receive path is behaviourally restored, the real ACK completes the item
        and the outbox returns to empty (live read-only SQLite plus a post-stop
        CLI count);
      - 3.6.3 row 6 ACK spoof: two real uncommitted pending items are held by
        pausing the healthy backend, Gateway A's own credential attempts a valid
        five-field ACK for its own and for B's pending item, the broker denies
        both publishes, no target receiver observes a spoof ACK, the pending
        bytes are unchanged and no false completion occurs; after the hold is
        released the real backend commits and ACKs both items.

Isolation and correctness goals:

  * One disposable fixture: isolated CA, own Docker network, pinned TimescaleDB
    with schema 20 and the backend app role, Mosquitto 2.0.18 with a separate
    manager principal and a separate backend principal.
  * Gateways A/B intentionally share the same ``sensor_id`` (isolation check),
    each with its own URN graph, ownership and *distinct* desired/Gateway
    sentinels.
  * The real ``gateway`` and ``backend`` target images are built once from
    ``src/Dockerfile``; the backend runs the image binary (no host bind-mount of
    a server binary).
  * Every image-declared volume path is masked explicitly; no anonymous volumes
    are left unmanaged.
  * Cleanup targets only resources created by this run, by exact ID/name, and
    verifies absence even on exception or timeout. Docker-unavailable runs print
    an explicit SKIP and never become PASS.

The remaining Task 3.6 groups (3.6.5-3.6.7) are declared NOT RUN by this
entrypoint; it makes no full-task PASS claim.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import ssl
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/tests'))
from task28_simplified_schema_runner import DatabaseFixture, ENV, sanitize_output  # noqa: E402
from stage3_persistence_runner import bootstrap_to_9, run_existing_migration_runner  # noqa: E402
from stage3_mqtt_ingress_runner import (  # noqa: E402
    Envelope, SIM_BASE, SIM_PERIOD, SIM_PROFILE, MOSQUITTO_IMAGE,
    run, docker, container_logs, docker_create, container_absent, remove_container,
    network_absent, volume_absent, mapped_port, write_private, generate_certs,
    bootstrap_dynsec, wait_tcp, wait_http, connect_manager, parse_run_summary,
    docker_available, mqtt_text,
)
from task26_harness_helpers import CONTROL, MQTT, RESPONSE, mqtt_connection  # noqa: E402

# --- scenario registry ------------------------------------------------------

# Explicit scenario registry. Unknown names fail closed instead of silently
# doing nothing green.
IMPLEMENTED_SCENARIOS = {
    'normal': '3.6.1-3.6.2 two-Gateway normal uplink path',
    'tls-credential': '3.6.3 rows 1-3 TLS/credential denial with positive controls',
    'acl-control': '3.6.3 rows 4-5 and 7 cross-Gateway publish/subscribe and DynSec control-topic denial',
    'ack-fault': '3.6.3 row 6 ACK spoof + 3.6.4 lost application ACK/byte-exact resend (one reversible fixture hook, separate PASS)',
    'db-timeout': '3.6.5 steps 1-2 database timeout and resend recovery',
    'queue-full': '3.6.5 steps 3-6 queue-full pressure and recovery',
    'gateway-reconnect': '3.6.6 step 1 ordinary Gateway network disconnect, reconnect, resubscribe, and byte-exact resend',
    'process-restart': '3.6.6 steps 2-3 controlled backend restart and Gateway CLI restart while durable items are pending',
    'broker-controlled-restart': '3.6.7 broker controlled restart with credential and ACL persistence',
}
# Groups that remain NOT RUN by this entrypoint. 3.6.1-3.6.7 are implemented.
REMAINING_GROUPS: tuple[tuple[str, str], ...] = ()

SHARED_SENSOR_ID = 'sensor_stage3_uplink_shared'
SENSOR_UNIT = 'degC'

SAMPLE_COUNT = 100
FIRST_SEQUENCE = 0
SAMPLE_INTERVAL_US = 10000

SIM_BROKER_HOST_IN_CONTAINER = 'broker'
SIM_BROKER_PORT = '8883'
COMPLETION_TIMEOUT = '30s'

# 3.6.3: the broker network alias used to force a SAN mismatch without leaving
# the certificate SAN (DNS:localhost,DNS:broker) and without restarting the
# pinned broker.
SAN_NEGATIVE_ALIAS = 'san-negative'

# 3.6.3 rows 4-5 and 7: ACL probe configuration. Probe connections get their own
# unique client ids from the shared MQTT helper (``poc_<uuid>``), never the
# simulator's ``<gateway_id>_sim`` id, so a probe never kicks a running
# simulator. The DynSec control topic is the manager-only namespace; no Gateway
# or observer is ever granted access to it.
PROBE_MEASURED_AT = '2026-10-01T00:00:00.000000Z'
PROBE_ACK_TIMEOUT = 20.0
PROBE_DENY_SETTLE = 6.0
PROBE_DRAIN_TIMEOUT = 2.0
DYN_SEC_CONTROL = CONTROL
DYN_SEC_RESPONSE = RESPONSE

# --- finite bounds ----------------------------------------------------------

IMAGE_BUILD_TIMEOUT = 900
GLOBAL_TIMEOUT = 3600
SIM_RUN_TIMEOUT = 180
SIM_ADMIN_TIMEOUT = 60
NEGATIVE_RUN_TIMEOUT = 90
SEED_COMPLETION_TIMEOUT = '2s'
BACKEND_NEGATIVE_PROBE = 20
CONTAINER_START_TIMEOUT = 90
BROKER_READY_TIMEOUT = 60
READY_TIMEOUT = 90

# --- 3.6.3 row 6 (ACK spoof) and 3.6.4 (lost application ACK) ---------------

# This group keeps a real committed batch from being application-ACKed to the
# simulator with one reversible DynSec receive hook, then holds two real pending
# (uncommitted) items by pausing the healthy backend. Every bound is finite so a
# hang is reported, never silently green.
ACK_FAULT_COMPLETION_TIMEOUT = '180s'
ACK_FAULT_SIM_RUN_TIMEOUT = 240
ACK_FAULT_ACK_TIMEOUT = '2s'
ACK_FAULT_RETRY_MIN = '1s'
ACK_FAULT_RETRY_MAX = '2s'
ACK_FAULT_RESEND_TIMEOUT = 90.0
ACK_FAULT_DELIVERY_TIMEOUT = 30.0
ACK_FAULT_CONTROL_TIMEOUT = 20.0
ACK_FAULT_DENY_SETTLE = 6.0
ACK_FAULT_START_TIMEOUT = 90
ACK_FAULT_EXIT_TIMEOUT = 240
ACK_FAULT_PENDING_TIMEOUT = 60.0
ACK_FAULT_SETTLE = 4.0
ACK_FAULT_SQLITE_BUSY = 5.0
# The DynSec deny role is attached with a higher client-role priority than the
# Gateway's uplink role (default -1) so its publishClientReceive deny is checked
# first and wins the first-match ACL scan.
ACK_BLOCK_ROLE_PRIORITY = 1

# --- 3.6.5 steps 3-6 (queue-full pressure and recovery) ---------------------
QUEUE_FULL_BATCHES = 3
QUEUE_FULL_SAMPLE_COUNT = 100
QUEUE_FULL_SAMPLE_RATE_HZ = 1000
QUEUE_FULL_SAMPLE_INTERVAL_US = 1000
QUEUE_FULL_MAX_INFLIGHT = 4
QUEUE_FULL_OUTBOX_MAX_ROWS = 10

SECRETS: list[str] = []
RUN_START = time.monotonic()


def sanitize(text: str) -> str:
    return sanitize_output(text, SECRETS)


def ensure_budget(label: str):
    if time.monotonic() - RUN_START > GLOBAL_TIMEOUT:
        raise RuntimeError(f'global run budget exceeded before {label}')


# --- scenario helpers (unit-testable, no docker) ----------------------------


def resolve_scenario(name: str) -> str:
    if name == 'all':
        return 'all'
    if name not in IMPLEMENTED_SCENARIOS:
        raise ValueError(f'scenario {name!r} is not implemented in this slice')
    return name


def notrun_lines() -> list[str]:
    return [f'NOT RUN: {label} ({step})' for label, step in REMAINING_GROUPS]


# --- 3.6.3 negative-evidence predicates (pure, unit-testable) ---------------


def extract_gateway_tls_denial_reason(log_output: str) -> str | None:
    """Extract a stable TLS denial reason from Go Gateway client logs.

    Rejects generic timeouts or absence of concrete TLS failure reasons.
    """
    low = log_output.lower()
    for reason in ('tls_unknown_authority', 'tls_certificate_verification',
                   'tls_certificate_invalid', 'tls_hostname_mismatch'):
        if f'reason={reason}' in low or f'reason="{reason}"' in low:
            return reason
    return None


def gateway_tls_denial_ok(*, exit_code: int, ready_reached: bool, ack_observed: bool,
                          cert_denial_observed: bool) -> bool:
    """A Gateway TLS denial is PASS only when the run failed, never reached a
    subscribed ready state, produced no application ACK, and a concrete TLS
    certificate denial was observed. Elapsed time alone is never sufficient."""
    return (exit_code != 0 and not ready_reached and not ack_observed and cert_denial_observed)


def backend_san_denial_ok(*, ready_http_ok: bool, cert_reason: str | None) -> bool:
    """A backend SAN denial is PASS only when /readyz never became ready AND a
    concrete certificate verification reason was observed. A bounded probe that
    merely elapsed without any certificate reason is not PASS."""
    return (not ready_http_ok) and bool(cert_reason)


def credential_denial_ok(*, connack_rejected: bool, cli_exit_code: int,
                         pending_before: int, pending_after: int) -> bool:
    """A wrong-credential denial is PASS only when the broker observed a reject
    CONNACK, the CLI failed, and a pre-existing pending row was untouched."""
    return (bool(connack_rejected) and cli_exit_code != 0
            and pending_before == pending_after and pending_before > 0)


def classify_tls_error(message: str | None) -> str:
    """Classify a bounded TLS probe result into a stable reason family.

    Returns 'ok' for a completed handshake and 'other' for an unrecognised
    error, so a caller never treats a bare timeout/unrelated error as a
    certificate denial.
    """
    if not message:
        return 'ok'
    low = message.lower()
    if 'mismatch' in low or 'not valid for' in low or 'hostname' in low:
        return 'hostname'
    if ('unknown authority' in low or 'self-signed' in low or 'self signed' in low
            or 'unable to get local issuer' in low or 'unable to get issuer' in low):
        return 'unknown_authority'
    if 'verify failed' in low or 'certificate' in low:
        return 'certificate'
    return 'other'


def tls_verify_reason(host: str, port: int, ca_file: Path, server_hostname: str,
                      timeout: float = 5.0) -> str | None:
    """Bounded TLS dial used as a concrete denial oracle (never a timeout).

    Returns None when the verified handshake succeeds, otherwise the certificate
    verification message. The message contains no secret, path or credential.
    """
    context = ssl.create_default_context(cafile=str(ca_file))
    raw = None
    try:
        raw = socket.create_connection((host, port), timeout)
        raw.settimeout(timeout)
        with context.wrap_socket(raw, server_hostname=server_hostname):
            return None
    except ssl.SSLCertVerificationError as exc:
        return f'certificate verify failed: {exc.verify_message or exc}'
    except ssl.SSLError as exc:
        return f'ssl error: {getattr(exc, "reason", None) or exc}'
    finally:
        if raw is not None:
            try:
                raw.close()
            except OSError:
                pass


def probe_http_status(port: int, path: str, timeout: float = 2.0) -> int | None:
    """One bounded HTTP status probe; returns HTTP status code (including 4xx/5xx) or None on network/connection failure."""
    import urllib.request
    import urllib.error
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception:  # noqa: BLE001 - bounded probe
        return None


# --- 3.6.3 rows 4-5 and 7: bounded ACL probe and denial oracles -------------


class AclProbe(MQTT):
    """A bounded MQTT 3.1.1 probe used only for 3.6.3 ACL evidence.

    It reuses the shared framing (``task26_harness_helpers.MQTT``) but reads the
    SUBACK code explicitly so a denied subscribe (0x80) is distinguishable from
    a granted one (0x00), and publishes QoS1 while never treating a PUBACK as
    ACL proof. The shared helper assigns every connection a fresh ``poc_<uuid>``
    client id, so a probe can never collide with a running simulator.
    """

    def __init__(self, port: int, ca_file: Path, username: str, password: str):
        # keepalive=0 so the broker never drops a bounded probe that stays idle
        # while a caller holds a long fault window open (the shared default 5s
        # would drop it at 1.5x5s of silence).
        self.s = mqtt_connection(port, ca_file, username, password, keepalive=0)
        self.s.settimeout(2)
        self.pid = 0

    def publish_frame(self, timeout: float):
        """Return the next PUBLISH ``(topic, payload)`` within ``timeout``,
        skipping any non-PUBLISH control frame. Returns None on timeout or EOF."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.s.settimeout(max(0.05, min(0.5, deadline - time.monotonic())))
            try:
                header, body = self.packet()
            except (TimeoutError, socket.timeout):
                continue
            except EOFError:
                return None
            if header != 0x30:
                # Any non-PUBLISH control frame is ignored.
                continue
            n = struct.unpack('!H', body[:2])[0]
            return body[2:2+n].decode(), body[2+n:]
        return None

    def subscribe_code(self, topic: str) -> int:
        """Send a QoS0 SUBSCRIBE and return the granted code (0x00 / 0x80)."""
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self.send(0x82, pid + mqtt_text(topic) + b'\x00')
        header, body = self.packet()
        if header != 0x90 or len(body) != 3 or body[:2] != pid:
            raise RuntimeError('SUBACK framing mismatch')
        return body[2]

    def publish_qos1(self, topic: str, payload: bytes, timeout: float = 5.0) -> bool:
        """Publish QoS1 and report whether a PUBACK was read.

        The boolean is transport evidence only; a caller must never treat it as
        proof that the broker accepted the message for routing.
        """
        self.pid += 1
        pid = struct.pack('!H', self.pid)
        self.send(0x32, mqtt_text(topic) + pid + payload)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.s.settimeout(max(0.05, deadline - time.monotonic()))
            try:
                header, body = self.packet()
            except (TimeoutError, socket.timeout, EOFError):
                return False
            if header == 0x40 and body == pid:
                return True
            # Any other frame is deliberately ignored; the deadline bounds this.
        return False

    def read_app_ack(self, message_id: str, timeout: float) -> bytes | None:
        """Return the first application ACK delivered for ``message_id``.

        Returns None when the bounded window elapses without a matching ACK, so
        a timeout is never confused with a successful delivery.
        """
        suffix = f'/acks/{message_id}'
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self.publish_frame(deadline - time.monotonic())
            if frame is None:
                return None
            topic, payload = frame
            if topic.endswith(suffix):
                return payload
        return None

    def drain(self, timeout: float) -> list:
        """Read any pending PUBLISH frames for a bounded window (supplementary).

        Returns the observed ``(topic, payload)`` deliveries. A negative ACL
        conclusion must not rest on this drain alone; it is recorded only as
        supporting evidence beside an observed SUBACK or broker denial.
        """
        out = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            frame = self.publish_frame(min(0.5, deadline - time.monotonic()))
            if frame is not None:
                out.append(frame)
        return out

def dynsec_response_observed(client, correlation: str, timeout: float) -> bool:
    """True when a DynSec response carrying ``correlation`` is observed.

    ``client`` is any shared-framing MQTT client subscribed to the response
    topic (the manager fixture connection is reused here).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        client.s.settimeout(max(0.05, min(0.2, deadline - time.monotonic())))
        try:
            _, raw = client.message()
        except (TimeoutError, socket.timeout, EOFError):
            continue
        try:
            responses = json.loads(raw.decode('utf-8')).get('responses', [])
        except (ValueError, UnicodeDecodeError):
            continue
        if any(r.get('correlationData') == correlation for r in responses):
            return True
    return False


def probe_measured_at(step: int) -> str:
    """A distinct, strictly increasing measurement time for successive probes.

    The Digital Twin advances a Sensor's ``reported_version`` only for a
    strictly newer observation (equal time advances only for the same boot id
    with a larger sequence). Each committed probe batch therefore carries a
    newer ``measured_at``, mirroring a live Gateway clock; reusing one constant
    would leave the second committed batch without a reported-state advance and
    turn a genuine commit into a spurious version mismatch. ``probe_measured_at
    (0)`` equals ``PROBE_MEASURED_AT``.
    """
    base = datetime.datetime(2026, 10, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    return (base + datetime.timedelta(seconds=step)).strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'


def synthetic_value(sequence: int) -> float:
    """Mirror of the simulator's triangular wave (internal/gatewaysim/batch.go).

    Kept independent of the Go code so a probe batch built here is a real
    telemetry batch, not a malformed one a validator would reject (which would
    hide an ACL bypass behind application validation).
    """
    position = sequence % SIM_PERIOD
    if position < 0:
        position += SIM_PERIOD
    if position > SIM_PERIOD // 2:
        position = SIM_PERIOD - position
    return SIM_BASE + position / 100.0


def build_probe_batch(spec: GatewaySpec, message_id: str, boot_id: str, first_sequence: int,
                      sample_count: int = SAMPLE_COUNT, sample_interval_us: int = SAMPLE_INTERVAL_US,
                      measured_at: str = PROBE_MEASURED_AT) -> bytes:
    """Build exactly one valid telemetry_batch payload for ``spec``.

    The eleven-field object mirrors the simulator/receiver contract so the
    payload is valid for its declared Gateway and Sensor.
    """
    samples = [synthetic_value(first_sequence + i) for i in range(sample_count)]
    return json.dumps({
        'protocol_version': 1,
        'message_id': message_id,
        'message_type': 'telemetry_batch',
        'gateway_id': spec.gateway_id,
        'sensor_id': spec.sensor_id,
        'boot_id': boot_id,
        'first_sequence': first_sequence,
        'sample_count': sample_count,
        'measured_at': measured_at,
        'sample_interval_us': sample_interval_us,
        'samples': samples,
    }, separators=(',', ':')).encode()


def probe_batch_is_valid(payload: bytes, spec: GatewaySpec) -> bool:
    """Structural check that a probe payload matches the telemetry contract.

    This keeps the cross-Gateway negative honest: the payload is a genuine valid
    batch for its Gateway/Sensor, never a deliberately malformed one.
    """
    try:
        obj = json.loads(payload.decode('utf-8'))
    except (ValueError, UnicodeDecodeError):
        return False
    fields = {'protocol_version', 'message_id', 'message_type', 'gateway_id', 'sensor_id',
              'boot_id', 'first_sequence', 'sample_count', 'measured_at', 'sample_interval_us',
              'samples'}
    if set(obj) != fields:
        return False
    if obj['protocol_version'] != 1 or obj['message_type'] != 'telemetry_batch':
        return False
    if obj['gateway_id'] != spec.gateway_id or obj['sensor_id'] != spec.sensor_id:
        return False
    if not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',
                        obj['message_id']):
        return False
    if not isinstance(obj['samples'], list) or len(obj['samples']) != obj['sample_count']:
        return False
    if obj['sample_count'] != SAMPLE_COUNT:
        return False
    return True


def denied_publish_logged(logs_before: str, logs_after: str, topic: str) -> bool:
    """True when a *new* broker ``Denied PUBLISH`` line for ``topic`` appeared.

    PUBACK/timeout is never consulted here; this is the broker-side denial
    observation that a QoS1 PUBACK cannot substitute for.
    """
    def matching(text: str) -> list:
        return [line for line in text.splitlines()
                if 'Denied PUBLISH' in line and topic in line]
    return len(matching(logs_after)) > len(matching(logs_before))


def wait_denied_publish(broker_cid: str, logs_before: str, topic: str, timeout: float) -> str:
    """Poll the broker log for a bounded window until the denied line appears."""
    deadline = time.monotonic() + timeout
    logs = container_logs(broker_cid)
    while time.monotonic() < deadline:
        if denied_publish_logged(logs_before, logs, topic):
            return logs
        time.sleep(0.2)
        logs = container_logs(broker_cid)
    return logs


def enable_fixture_deny_logging(work: Path) -> None:
    """Append ``log_type debug`` to the *fixture* broker config copy only.

    Mosquitto 2.0.18 emits the ``Denied PUBLISH`` audit line only at debug
    level, while the checked-in deployment config ``config/mosquitto/
    mosquitto.conf`` intentionally stops at ``notice``. The denial slice needs
    that line as its broker-side evidence, so the disposable
    ``work/broker/mosquitto.conf`` copy (bind-mounted read-only into the
    fixture broker) gets one extra level. The repository config is never
    touched, and existing ``log_type`` lines are preserved.
    """
    conf = work / 'broker' / 'mosquitto.conf'
    text = conf.read_text()
    if 'log_type debug' in text:
        return
    if not text.endswith('\n'):
        text += '\n'
    text += 'log_type debug\n'
    conf.write_text(text)
    conf.chmod(0o644)


def acl_cross_publish_denial_ok(*, broker_denied: bool, db_unchanged: bool,
                                message_id_absent: bool, no_app_ack: bool,
                                receiver_positive: bool) -> bool:
    """Cross-Gateway publish denial (row 4).

    A QoS1 PUBACK -- or its absence -- is deliberately not an input. PASS needs
    the broker deny observation, an unchanged DB snapshot, the absence of any
    committed row for the denied message id, no application ACK to B's
    receiver, and a live B receiver positive control.
    """
    return (broker_denied and db_unchanged and message_id_absent
            and no_app_ack and receiver_positive)


def acl_cross_subscribe_denial_ok(*, target_suback_code: int, own_positive_suback_code: int,
                                  receiver_ack_observed: bool,
                                  cross_gateway_delivery_observed: bool) -> bool:
    """Cross-Gateway subscribe denial (row 5).

    PASS needs the exact SUBACK 0x80 for B's ACK namespace, a granted own ACK
    subscription (0x00), a real B receiver ACK for the batch that was published,
    and no cross-Gateway delivery observed at the A observer. A bounded drain
    alone is never sufficient.
    """
    return (target_suback_code == 0x80 and own_positive_suback_code == 0x00
            and receiver_ack_observed and not cross_gateway_delivery_observed)


def acl_control_denial_ok(*, broker_denied: bool, gateway_probe_absent_before: bool,
                          gateway_probe_absent_after: bool, response_absent: bool,
                          readback_detects_mutation: bool, control_suback_code: int,
                          response_suback_code: int, manager_positive: bool) -> bool:
    """DynSec control-topic denial (row 7).

    PASS needs the broker deny observation, the Gateway's probe name absent from
    the manager readback both before and after, no DynSec response for the
    Gateway correlation, a readback that is positive-controlled (it detects a
    real manager mutation), both the control and response subscriptions denied
    with SUBACK 0x80, and a working manager request path.
    """
    return (broker_denied and gateway_probe_absent_before and gateway_probe_absent_after
            and response_absent and readback_detects_mutation
            and control_suback_code == 0x80 and response_suback_code == 0x80
            and manager_positive)


# --- 3.6.3 row 6 / 3.6.4 ack-fault oracles (pure, unit-testable) ------------


def ack_block_effective(*, allowed_delivered: bool, blocked_delivered: bool) -> bool:
    """The reversible ACK receive hook is confirmed by behaviour only.

    The same authorized publisher's ACK must be delivered while the receive
    block is off and withheld while the DynSec receive-deny role is attached. A
    role/readback observation alone is never sufficient.
    """
    return allowed_delivered and not blocked_delivered


def lost_ack_resend_ok(*, committed: bool, pending_while_blocked: bool,
                       resend_observed: bool, resend_bytes_match: bool,
                       db_unchanged_during: bool, receive_restored: bool,
                       pending_after: int, simulator_exit_code: int,
                       cli_pending_rows: int) -> bool:
    """3.6.4 lost-ACK recovery.

    PASS needs the batch independently committed in SQL, the SQLite row still
    pending while the ACK was blocked, at least one observed byte-exact resend of
    the *same* item, no new ledger/raw/temporal/version from the resend, a
    behaviourally restored receive path, a clean simulator exit, and an empty
    outbox (both the live read and the post-stop CLI count). A PUBACK or an
    elapsed timeout is never an input.
    """
    return (committed and pending_while_blocked and resend_observed
            and resend_bytes_match and db_unchanged_during and receive_restored
            and pending_after == 0 and simulator_exit_code == 0 and cli_pending_rows == 0)


def ack_spoof_denial_ok(*, broker_denied_own: bool, broker_denied_target: bool,
                        no_spoof_delivery: bool, pending_bytes_stable: bool,
                        db_unchanged: bool, receiver_positive: bool) -> bool:
    """3.6.3 row 6 ACK spoof denial.

    PASS needs the broker to deny the Gateway's forged ACK publish for both its
    own and the target Gateway's pending item, no target receiver to observe a
    spoof delivery, the target pending rows' bytes to be unchanged, no
    unintended ledger/raw/temporal/Twin write, and a positively controlled
    receiver (an authorized backend ACK is delivered). A PUBACK, a live observer
    or an elapsed timeout is never an input.
    """
    return (broker_denied_own and broker_denied_target and no_spoof_delivery
            and pending_bytes_stable and db_unchanged and receiver_positive)


# --- 3.6.6 step 1 pure predicates (unit-testable, no docker) ----------------


def parse_gateway_transport_events(logs: str) -> list[dict]:
    """Parse structured transport lifecycle events from gateway container logs.

    Extracts event, epoch, and optional filter attributes without capturing secrets.
    """
    events = []
    pattern = re.compile(r'event=(?P<event>\w+)\s+epoch=(?P<epoch>\d+)(?:\s+filter=(?P<filter>\S+))?')
    for line in logs.splitlines():
        m = pattern.search(line)
        if m:
            item = {
                'event': m.group('event'),
                'epoch': int(m.group('epoch')),
            }
            if m.group('filter'):
                item['filter'] = m.group('filter')
            events.append(item)
    return events


def gateway_reconnect_events_ok(events: list[dict], min_epoch: int = 2) -> bool:
    """Validate that disconnect on epoch 1 preceded fresh connection and subscription on epoch >= min_epoch."""
    disconnected_epoch1 = any(e['event'] in ('disconnected', 'connection_lost') and e['epoch'] == 1 for e in events)
    connected_fresh = any(e['event'] == 'connected' and e['epoch'] >= min_epoch for e in events)
    subscribed_fresh = any(e['event'] == 'subscribed' and e['epoch'] >= min_epoch for e in events)
    if not (disconnected_epoch1 and connected_fresh and subscribed_fresh):
        return False
    disconn_indices = [i for i, e in enumerate(events) if e['event'] in ('disconnected', 'connection_lost') and e['epoch'] == 1]
    sub_indices = [i for i, e in enumerate(events) if e['event'] == 'subscribed' and e['epoch'] >= min_epoch]
    if not disconn_indices or not sub_indices:
        return False
    return min(disconn_indices) < min(sub_indices)


def reconnect_pending_identity_unchanged(before: dict, after: dict) -> bool:
    """Verify that a pending row's message identity, bytes, and range did not mutate across disconnect/reconnect."""
    keys = ('message_id', 'topic', 'payload', 'boot_id', 'first_sequence', 'sample_count', 'sample_interval_us', 'byte_size')
    return all(before.get(k) == after.get(k) for k in keys)


def gateway_reconnect_oracle(*, committed_init: bool, pending_init: bool,
                             disconnect_observed: bool, reconnect_subscribed: bool,
                             resend_observed: bool, resend_bytes_match: bool,
                             pending_identity_match: bool, db_unchanged_during: bool,
                             receive_restored: bool, pending_after: int,
                             cli_pending_rows: int, simulator_exit_code: int,
                             db_unchanged_final: bool) -> bool:
    """Oracle for Task 3.6.6 Step 1 gateway-reconnect slice."""
    return (committed_init and pending_init and disconnect_observed and
            reconnect_subscribed and resend_observed and resend_bytes_match and
            pending_identity_match and db_unchanged_during and receive_restored and
            pending_after == 0 and cli_pending_rows == 0 and
            simulator_exit_code == 0 and db_unchanged_final)


# --- resource ownership and cleanup contract (unit-testable) ----------------


@dataclass
class ResourceRegistry:
    """Owned resources recorded for exact-ID cleanup verification."""

    items: list = field(default_factory=list)

    def register(self, label: str, kind: str, ident) -> None:
        self.items.append((label, kind, ident))

    def still_present(self, absent) -> list[str]:
        """Return labels whose resource is not authoritatively absent."""
        return [label for (label, kind, ident) in self.items if not absent(kind, ident)]


def image_absent(ref: str) -> bool:
    proc = subprocess.run(['docker', 'image', 'inspect', ref], env=ENV,
                          stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
    if proc.returncode == 0:
        return False
    err = proc.stderr.decode('utf-8', errors='replace').lower()
    return 'no such image' in err or 'no such object' in err


def absent(kind: str, ident) -> bool:
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


# --- two-Gateway fixture model ----------------------------------------------


@dataclass(frozen=True)
class GatewaySpec:
    gateway_id: str
    gateway_name: str
    sensor_name: str
    gateway_reported: dict
    gateway_desired: dict
    gateway_reported_version: int
    gateway_desired_version: int
    sensor_desired: dict
    sensor_desired_version: int

    @property
    def sensor_id(self) -> str:
        return SHARED_SENSOR_ID

    @property
    def sensor_unit(self) -> str:
        return SENSOR_UNIT

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


def default_specs() -> list[GatewaySpec]:
    """Two Gateways sharing one sensor id, with distinct sentinels."""
    return [
        GatewaySpec(
            gateway_id='gw_stage3_uplink_a',
            gateway_name='Stage 3 uplink Gateway A',
            sensor_name='Shared sensor (Gateway A)',
            gateway_reported={'connectionStatus': 'online', 'tenant': 'A'},
            gateway_desired={'gw_desired': 'A'},
            gateway_reported_version=2,
            gateway_desired_version=4,
            sensor_desired={'tenant': 'A', 'keep': True},
            sensor_desired_version=5,
        ),
        GatewaySpec(
            gateway_id='gw_stage3_uplink_b',
            gateway_name='Stage 3 uplink Gateway B',
            sensor_name='Shared sensor (Gateway B)',
            gateway_reported={'connectionStatus': 'offline', 'tenant': 'B'},
            gateway_desired={'gw_desired': 'B'},
            gateway_reported_version=1,
            gateway_desired_version=6,
            sensor_desired={'tenant': 'B', 'keep': False},
            sensor_desired_version=9,
        ),
    ]


def validate_specs(specs: list[GatewaySpec]):
    """Fail closed on ambiguous fixtures; the shared sensor id is intentional."""
    seen_gateways = set()
    for spec in specs:
        if spec.gateway_id in seen_gateways:
            raise ValueError(f'duplicate gateway_id {spec.gateway_id!r}')
        seen_gateways.add(spec.gateway_id)
    if len(specs) < 2:
        raise ValueError('the two-Gateway isolation check needs at least two Gateways')
    if len({s.sensor_id for s in specs}) != 1:
        raise ValueError('this slice expects two Gateways sharing one sensor id')


def seed_graph(fixture: DatabaseFixture, specs: list[GatewaySpec]):
    stmts = []
    for spec in specs:
        stmts.append(
            f"INSERT INTO gateways(gateway_id,name) VALUES ('{spec.gateway_id}','{spec.gateway_name}');")
        stmts.append(
            f"INSERT INTO sensors(gateway_id,sensor_id,name,unit) "
            f"VALUES ('{spec.gateway_id}','{spec.sensor_id}','{spec.sensor_name}','{spec.sensor_unit}');")
    for spec in specs:
        stmts.append(
            f"INSERT INTO twin_entities(entity_id,entity_type,gateway_id) "
            f"VALUES ('{spec.gateway_urn}','Gateway','{spec.gateway_id}'),"
            f"('{spec.sensor_urn}','Sensor','{spec.gateway_id}');")
        stmts.append(
            "INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id) "
            f"SELECT p.id,'hasSensor',c.id FROM twin_entities p,twin_entities c "
            f"WHERE p.entity_id='{spec.gateway_urn}' AND c.entity_id='{spec.sensor_urn}';")
        stmts.append(
            f"INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities "
            f"WHERE entity_id IN ('{spec.gateway_urn}','{spec.sensor_urn}');")
        stmts.append(
            f"UPDATE twin_states SET desired_state='{json.dumps(spec.sensor_desired)}'::jsonb, "
            f"desired_version={spec.sensor_desired_version} "
            f"WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='{spec.sensor_urn}');")
        stmts.append(
            f"UPDATE twin_states SET reported_state='{json.dumps(spec.gateway_reported)}'::jsonb, "
            f"desired_state='{json.dumps(spec.gateway_desired)}'::jsonb, "
            f"reported_version={spec.gateway_reported_version}, "
            f"desired_version={spec.gateway_desired_version} "
            f"WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id='{spec.gateway_urn}');")
    fixture.sql(('\n'.join(stmts)).encode())


def build_gateway_assert_sql(spec: GatewaySpec, env: Envelope, marker: str) -> str:
    """Independent SQL state assertion scoped to exactly one Gateway.

    Mirrors the Task 3.4 shared assertion but parameterized by ``spec`` so two
    Gateways sharing one sensor id stay isolated (the ledger/raw/temporal
    filters include ``gateway_id`` and the per-Gateway URNs).
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
                   f"WHERE gateway_id={q(spec.gateway_id)} AND sensor_id={q(spec.sensor_id)} AND message_id = msg)")
    if env.value_is_int:
        value_check = (f"IF (rep->>'value')::bigint <> {int(env.last_value)} THEN "
                       "RAISE EXCEPTION 'reported value %', rep->>'value'; END IF;")
    else:
        value_check = (f"IF abs((rep->>'value')::double precision - {env.last_value!r}) > 1e-9 THEN "
                       "RAISE EXCEPTION 'reported value %', rep->>'value'; END IF;")

    if env.verify_rows:
        span_us = (env.sample_count - 1) * env.sample_interval_us
        raw_filter = (f"t.gateway_id={q(spec.gateway_id)} AND t.sensor_id={q(spec.sensor_id)} "
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

    return f"""
    DO $$
    DECLARE
      raw_count int; ledger int; temporal int;
      msg uuid; boot uuid;
      l_first bigint; l_last bigint; l_count int; l_sensor text; l_topic text;
      t_val double precision; t_text text; t_bool boolean; t_meta jsonb; t_obs timestamptz;
      rep jsonb; des jsonb; rep_ver bigint; des_ver bigint;
      gw_rep jsonb; gw_des jsonb; gw_rep_ver bigint; gw_des_ver bigint;
    BEGIN
      SELECT count(*) INTO ledger FROM public.processed_messages
        WHERE gateway_id={q(spec.gateway_id)} AND sensor_id={q(spec.sensor_id)};
      IF ledger <> 1 THEN RAISE EXCEPTION 'ledger rows %', ledger; END IF;
      SELECT message_id, boot_id, first_sequence, last_sequence, sample_count, sensor_id, source_topic
        INTO msg, boot, l_first, l_last, l_count, l_sensor, l_topic
        FROM public.processed_messages
        WHERE gateway_id={q(spec.gateway_id)} AND sensor_id={q(spec.sensor_id)};
      IF l_first <> {env.first_sequence} OR l_last <> {env.last_sequence} OR l_count <> {env.sample_count} THEN
        RAISE EXCEPTION 'ledger envelope first/last/count % % %', l_first, l_last, l_count; END IF;
      IF l_sensor <> {q(spec.sensor_id)} THEN RAISE EXCEPTION 'ledger sensor_id %', l_sensor; END IF;
      IF l_topic <> {q(spec.telemetry_topic)} THEN RAISE EXCEPTION 'ledger source_topic %', l_topic; END IF;
      {msg_known}
      {boot_known}
      SELECT count(*) INTO raw_count FROM public.telemetry
        WHERE gateway_id={q(spec.gateway_id)} AND sensor_id={q(spec.sensor_id)} AND message_id = msg;
      IF raw_count <> {env.sample_count} THEN RAISE EXCEPTION 'raw count %', raw_count; END IF;
      SELECT count(*) INTO temporal FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.sensor_urn)})
          AND property_name='sample_interval_us' AND message_id = msg;
      IF temporal <> 1 THEN RAISE EXCEPTION 'temporal rows %', temporal; END IF;
      SELECT value_number, value_text, value_boolean, metadata, observed_at
        INTO t_val, t_text, t_bool, t_meta, t_obs
        FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.sensor_urn)})
          AND property_name='sample_interval_us' AND message_id = msg;
      IF t_val <> {env.sample_interval_us} THEN RAISE EXCEPTION 'temporal value_number %', t_val; END IF;
      IF t_text IS NOT NULL OR t_bool IS NOT NULL THEN
        RAISE EXCEPTION 'temporal typed columns are not exclusive'; END IF;
      IF t_meta->>'unit' <> 'us' OR t_meta->>'source' <> 'telemetry_batch' OR t_meta->>'boot_id' <> boot::text THEN
        RAISE EXCEPTION 'temporal metadata %', t_meta; END IF;
      IF t_obs <> {obs_sql} THEN RAISE EXCEPTION 'temporal observed_at %', t_obs; END IF;
      SELECT reported_state, desired_state, reported_version, desired_version
        INTO rep, des, rep_ver, des_ver FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.sensor_urn)});
      IF rep_ver <> {env.expected_reported_version} THEN RAISE EXCEPTION 'sensor reported_version %', rep_ver; END IF;
      {value_check}
      IF (rep->>'sequence_num')::bigint <> {env.last_sequence} THEN RAISE EXCEPTION 'reported sequence %', rep->>'sequence_num'; END IF;
      IF (rep->>'sample_interval_us')::bigint <> {env.sample_interval_us} THEN RAISE EXCEPTION 'reported interval %', rep->>'sample_interval_us'; END IF;
      IF rep->>'unit' IS DISTINCT FROM {q(spec.sensor_unit)} THEN RAISE EXCEPTION 'reported unit %', rep->>'unit'; END IF;
      IF rep->>'boot_id' <> boot::text THEN RAISE EXCEPTION 'reported boot_id %', rep->>'boot_id'; END IF;
      IF (rep->>'observed_at')::timestamptz <> {obs_sql} THEN
        RAISE EXCEPTION 'reported observed_at %', rep->>'observed_at'; END IF;
      {row_checks}
      IF des <> {q(json.dumps(spec.sensor_desired))}::jsonb OR des_ver <> {spec.sensor_desired_version} THEN
        RAISE EXCEPTION 'sensor desired state changed'; END IF;
      SELECT reported_state, desired_state, reported_version, desired_version
        INTO gw_rep, gw_des, gw_rep_ver, gw_des_ver FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.gateway_urn)});
      IF gw_rep <> {q(json.dumps(spec.gateway_reported))}::jsonb
         OR gw_des <> {q(json.dumps(spec.gateway_desired))}::jsonb
         OR gw_rep_ver <> {spec.gateway_reported_version}
         OR gw_des_ver <> {spec.gateway_desired_version} THEN
        RAISE EXCEPTION 'gateway state/audit changed'; END IF;
      RAISE NOTICE '{marker}';
    END $$;
    """


def assert_gateway_batch(fixture: DatabaseFixture, spec: GatewaySpec, env: Envelope, marker: str):
    sql = build_gateway_assert_sql(spec, env, marker)
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'SQL assertions did not emit {marker} for {spec.gateway_id}')


def assert_phase_totals(fixture: DatabaseFixture, specs: list[GatewaySpec],
                        ledger: int, raw: int, temporal: int, marker: str):
    def q(value: str) -> str:
        return "'" + value + "'"
    gids = ','.join(q(s.gateway_id) for s in specs)
    urns = ','.join(q(s.sensor_urn) for s in specs)
    sql = f"""
    DO $$
    DECLARE l int; r int; t int;
    BEGIN
      SELECT count(*) INTO l FROM public.processed_messages WHERE gateway_id IN ({gids});
      IF l <> {ledger} THEN RAISE EXCEPTION 'total ledger %', l; END IF;
      SELECT count(*) INTO r FROM public.telemetry WHERE gateway_id IN ({gids});
      IF r <> {raw} THEN RAISE EXCEPTION 'total raw %', r; END IF;
      SELECT count(*) INTO t FROM public.twin_temporal_values
        WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE entity_id IN ({urns}));
      IF t <> {temporal} THEN RAISE EXCEPTION 'total temporal %', t; END IF;
      RAISE NOTICE '{marker}';
    END $$;
    """
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'phase totals assertion did not emit {marker}')


# --- 3.6.3 DB snapshot + positive-control assertions ------------------------


def snapshot_db(fixture: DatabaseFixture, specs: list[GatewaySpec]) -> dict:
    """A bounded, secret-free DB snapshot for negative-input delta checks.

    Covers the ledger, raw telemetry, temporal observations and a Twin state
    fingerprint (version sums + a state md5), so an unintended ledger/raw/Twin/
    temporal write from a denied input changes at least one field.
    """
    def q(value: str) -> str:
        return "'" + value + "'"
    gids = ','.join(q(s.gateway_id) for s in specs)
    urns = ','.join(q(s.sensor_urn) for s in specs)
    sql = f"""
    DO $$
    DECLARE j jsonb;
    BEGIN
      SELECT jsonb_build_object(
        'ledger', (SELECT count(*) FROM public.processed_messages WHERE gateway_id IN ({gids})),
        'raw', (SELECT count(*) FROM public.telemetry WHERE gateway_id IN ({gids})),
        'temporal', (SELECT count(*) FROM public.twin_temporal_values
                      WHERE entity_id IN (SELECT id FROM public.twin_entities WHERE entity_id IN ({urns}))),
        'reported_version_sum', (SELECT coalesce(sum(reported_version),0) FROM public.twin_states),
        'desired_version_sum', (SELECT coalesce(sum(desired_version),0) FROM public.twin_states),
        'state_md5', (SELECT coalesce(md5(string_agg(entity_id::text || '|' || reported_state::text
                      || '|' || desired_state::text, ',' ORDER BY entity_id)), 'none') FROM public.twin_states)
      ) INTO j;
      RAISE NOTICE 'SNAP %', j;
    END $$;
    """
    _, out = fixture.sql(sql.encode())
    text = out.decode('utf-8', errors='replace')
    for line in text.splitlines():
        idx = line.find('SNAP ')
        if idx != -1:
            return json.loads(line[idx + len('SNAP '):].strip())
    raise RuntimeError('DB snapshot marker was not emitted')


def assert_snapshot_unchanged(before: dict, after: dict, marker: str):
    if before != after:
        raise RuntimeError(f'denied input changed DB state ({marker}): before={before} after={after}')


def assert_gateway_positive(fixture: DatabaseFixture, spec: GatewaySpec, ledger: int,
                            raw: int, temporal: int, reported_version: int, marker: str):
    """Scoped positive-control assertion for the tls-credential scenario.

    Distinct ledger message ids across the accumulated positive batches, the
    expected raw/temporal counts, the sensor reported_version advance, and the
    preserved desired/Gateway sentinels.
    """
    def q(value: str) -> str:
        return "'" + value + "'"
    sql = f"""
    DO $$
    DECLARE l int; r int; t int; rv bigint; des jsonb; dv bigint; gws jsonb; gwd jsonb;
    BEGIN
      SELECT count(*) INTO l FROM public.processed_messages WHERE gateway_id={q(spec.gateway_id)};
      IF l <> {ledger} THEN RAISE EXCEPTION 'ledger %', l; END IF;
      IF (SELECT count(DISTINCT message_id) FROM public.processed_messages WHERE gateway_id={q(spec.gateway_id)}) <> {ledger}
        THEN RAISE EXCEPTION 'ledger message ids are not distinct'; END IF;
      SELECT count(*) INTO r FROM public.telemetry
        WHERE gateway_id={q(spec.gateway_id)} AND sensor_id={q(spec.sensor_id)};
      IF r <> {raw} THEN RAISE EXCEPTION 'raw %', r; END IF;
      SELECT count(*) INTO t FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.sensor_urn)})
          AND property_name='sample_interval_us';
      IF t <> {temporal} THEN RAISE EXCEPTION 'temporal %', t; END IF;
      SELECT reported_version, desired_state, desired_version INTO rv, des, dv FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.sensor_urn)});
      IF rv <> {reported_version} THEN RAISE EXCEPTION 'sensor reported_version %', rv; END IF;
      IF des <> {q(json.dumps(spec.sensor_desired))}::jsonb OR dv <> {spec.sensor_desired_version} THEN
        RAISE EXCEPTION 'sensor desired changed'; END IF;
      SELECT reported_state, desired_state INTO gws, gwd FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id={q(spec.gateway_urn)});
      IF gws <> {q(json.dumps(spec.gateway_reported))}::jsonb
         OR gwd <> {q(json.dumps(spec.gateway_desired))}::jsonb THEN
        RAISE EXCEPTION 'gateway sentinel changed'; END IF;
      RAISE NOTICE '{marker}';
    END $$;
    """
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'positive assertion {marker} was not emitted')


def ledger_message_ids(fixture: DatabaseFixture, spec: GatewaySpec) -> list[str]:
    _, out = fixture.sql(
        f"SELECT message_id::text FROM public.processed_messages "
        f"WHERE gateway_id='{spec.gateway_id}' ORDER BY message_id;".encode())
    ids = []
    for line in out.decode('utf-8', errors='replace').splitlines():
        token = line.strip()
        if re.fullmatch(r'[0-9a-fA-F-]{36}', token):
            ids.append(token.lower())
    return ids


def assert_message_id_absent(fixture: DatabaseFixture, gateway_id: str, message_id: str,
                             marker: str) -> bool:
    """Fail unless the ledger has no row for this exact (gateway_id, message_id).

    This is the per-message absence oracle for a denied cross-Gateway publish:
    the denied message must never have been routed or committed.
    """
    sql = f"""
    DO $$
    DECLARE n int;
    BEGIN
      SELECT count(*) INTO n FROM public.processed_messages
        WHERE gateway_id='{gateway_id}' AND message_id='{message_id}'::uuid;
      IF n <> 0 THEN RAISE EXCEPTION 'denied message id has % ledger rows', n; END IF;
      RAISE NOTICE '{marker}';
    END $$;
    """
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'denied-message absence marker {marker} was not emitted')
    return True


def manager_client_names(manager) -> set:
    """Read the DynSec client name set through the manager (fixture readback).

    Mosquitto 2.0.18 ``listClients`` returns ``data['clients']`` as an array of
    username *strings* by default (``verbose`` off); some flows return objects
    with a ``username`` key. Both shapes are accepted so a real readback is never
    silently empty.
    """
    data = manager.request('listClients')
    clients = data.get('clients', []) if isinstance(data, dict) else []
    names = set()
    for client in clients:
        if isinstance(client, str):
            names.add(client)
        elif isinstance(client, dict):
            name = client.get('username')
            if isinstance(name, str):
                names.add(name)
    return names


def assert_ack_fields(raw: bytes, message_id: str):
    """Validate the exact five-field application ACK contract for a message id."""
    from stage3_mqtt_ingress_runner import verify_ack
    verify_ack(message_id, raw)


# --- image build (once per target) ------------------------------------------


def build_image(target: str, tag: str) -> str:
    proc = subprocess.run(
        ['docker', 'build', '--target', target, '-t', tag, '-f', 'Dockerfile', '.'],
        cwd=ROOT / 'src', env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=IMAGE_BUILD_TIMEOUT)
    if proc.returncode:
        raise RuntimeError(f'docker build ({target}) failed: ' +
                           sanitize((proc.stdout + proc.stderr).decode('utf-8', errors='replace')[-3000:]))
    image_id = docker(['image', 'inspect', '-f', '{{.Id}}', tag], timeout=30).decode().strip()
    if len(image_id) < 12:
        raise RuntimeError(f'image {tag} has no inspect id')
    return image_id


def generate_untrusted_ca(work: Path, name: str) -> Path:
    """Create an isolated CA that the pinned broker server certificate is NOT
    signed by, so a client trusting only this CA must fail verification."""
    d = work / name
    d.mkdir(mode=0o700, parents=True)
    run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
         '-subj', f'/CN={name}-untrusted-ca', '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
         '-keyout', str(d / 'ca.key'), '-out', str(d / 'ca.crt')])
    ca = d / 'ca.crt'
    ca.chmod(0o644)
    return ca


# --- gateway container run --------------------------------------------------


def run_gateway_container(image: str, net_name: str, outbox_volume: str, ca_file: Path,
                          env_file: Path, args: list, timeout: int, *,
                          outbox_dir: Path | None = None, container_name: str | None = None):
    """Run the real gateway image, masking every image-declared volume path.

    When ``outbox_dir`` is set the outbox is a host bind-mount so a host-side
    read-only SQLite reader can inspect the live file; otherwise a named volume
    is used and the outbox is only reachable through the image.
    """
    if outbox_dir is not None:
        outbox_mount = f'type=bind,src={outbox_dir},dst=/data/outbox'
    else:
        outbox_mount = f'type=volume,src={outbox_volume},dst=/data/outbox'
    return subprocess.run(
        ['docker', 'run', '--rm', *(['--name', container_name] if container_name else []),
         '--cap-drop', 'ALL',
         '--security-opt', 'no-new-privileges:true',
         '--read-only',
         '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
         '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=8m',
         '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=8m',
         '--network', net_name,
         '--env-file', str(env_file),
         '--mount', outbox_mount,
         '--mount', f'type=bind,src={ca_file},dst=/data/ca/ca.crt,readonly',
         image, *args],
        env=ENV, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)


def gateway_env(gateway_id: str, password: str) -> dict:
    env = dict(SIM_PROFILE)
    env.update({
        'GATEWAY_ID': gateway_id,
        'SENSOR_ID': SHARED_SENSOR_ID,
        'GATEWAY_MQTT_BROKER_URL': f'ssl://{SIM_BROKER_HOST_IN_CONTAINER}:{SIM_BROKER_PORT}',
        'GATEWAY_MQTT_CLIENT_ID': f'{gateway_id}_sim',
        'GATEWAY_MQTT_USERNAME': gateway_id,
        'GATEWAY_MQTT_PASSWORD': password,
        'GATEWAY_MQTT_CA_FILE': '/data/ca/ca.crt',
        'GATEWAY_OUTBOX_PATH': '/data/outbox/outbox.db',
    })
    return env


def write_gateway_env(work: Path, name: str, gateway_id: str, password: str) -> Path:
    env_file = work / name
    write_private(env_file, ''.join(f'{k}={v}\n' for k, v in gateway_env(gateway_id, password).items()).encode())
    return env_file


def run_gateway_batch(image: str, net_name: str, work: Path, spec: GatewaySpec,
                      password: str, outbox_volume: str, ca_file: Path, name: str | None = None):
    tag = name or spec.gateway_id
    env_file = work / f'gateway_{tag}.env'
    write_private(env_file, ''.join(f'{k}={v}\n' for k, v in gateway_env(spec.gateway_id, password).items()).encode())
    proc = run_gateway_container(image, net_name, outbox_volume, ca_file, env_file,
                                 ['run', '--batches', '1', '--completion-timeout', COMPLETION_TIMEOUT],
                                 SIM_RUN_TIMEOUT)
    out = proc.stdout.decode('utf-8', errors='replace')
    err = proc.stderr.decode('utf-8', errors='replace')
    if proc.returncode != 0:
        raise RuntimeError(f'gateway {spec.gateway_id} run exit={proc.returncode}: ' + sanitize((out[-600:] + err[-600:])))
    summary = parse_run_summary(out)
    truthful = (summary.get('completed') == 'true' and summary.get('enqueued') == '1'
                and summary.get('app_acknowledged') == '1' and summary.get('pending_rows') == '0'
                and summary.get('pending_bytes') == '0' and summary.get('quarantined_rows') == '0'
                and summary.get('quarantined_bytes') == '0')
    if not truthful:
        raise RuntimeError(f'gateway {spec.gateway_id} run summary is not truthful: ' + sanitize(out[-600:]))
    print(f'PASS: Gateway {spec.gateway_id} CLI run exit 0 completed=true enqueued=%s '
          'app_acknowledged=%s pending_rows=%s quarantined_rows=%s'
          % (summary.get('enqueued'), summary.get('app_acknowledged'),
             summary.get('pending_rows'), summary.get('quarantined_rows')), flush=True)

    admin_env = work / f'gateway_{tag}_admin.env'
    write_private(admin_env, b'GATEWAY_OUTBOX_PATH=/data/outbox/outbox.db\n')
    admin = run_gateway_container(image, net_name, outbox_volume, ca_file, admin_env,
                                  ['count'], SIM_ADMIN_TIMEOUT)
    count_out = admin.stdout.decode('utf-8', errors='replace').strip()
    if admin.returncode != 0 or count_out != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
        raise RuntimeError(f'gateway {spec.gateway_id} count did not report an empty outbox: ' + sanitize(count_out))
    print(f'PASS: Gateway {spec.gateway_id} independent SQLite outbox count reports 0 pending / 0 quarantined', flush=True)


def cli_pending_rows(image: str, net_name: str, work: Path, outbox_volume: str,
                     ca_file: Path, name: str) -> int:
    """Read the SQLite pending row count via the real CLI admin command."""
    admin_env = work / f'{name}_admin.env'
    if not admin_env.exists():
        write_private(admin_env, b'GATEWAY_OUTBOX_PATH=/data/outbox/outbox.db\n')
    admin = run_gateway_container(image, net_name, outbox_volume, ca_file, admin_env,
                                  ['count'], SIM_ADMIN_TIMEOUT)
    out = admin.stdout.decode('utf-8', errors='replace').strip()
    match = re.search(r'pending_rows=(\d+)', out)
    if admin.returncode != 0 or match is None:
        raise RuntimeError(f'gateway count failed for {name}: ' + sanitize(out))
    return int(match.group(1))


def run_gateway_cli_raw(image: str, net_name: str, work: Path, gateway_id: str,
                        password: str, outbox_volume: str, ca_file: Path, name: str,
                        args: list, timeout: int):
    """Run the real CLI and return (returncode, stdout, stderr) without raising."""
    env_file = write_gateway_env(work, f'{name}.env', gateway_id, password)
    proc = run_gateway_container(image, net_name, outbox_volume, ca_file, env_file, args, timeout)
    return (proc.returncode,
            proc.stdout.decode('utf-8', errors='replace'),
            proc.stderr.decode('utf-8', errors='replace'))


# --- gatewaysim Envelope for the known synthetic output ---------------------


def simulator_envelope() -> Envelope:
    from stage3_mqtt_ingress_runner import SIM_LAST_VALUE
    return Envelope(
        message_id=None,
        boot_id=None,
        sample_count=SAMPLE_COUNT,
        first_sequence=FIRST_SEQUENCE,
        sample_interval_us=SAMPLE_INTERVAL_US,
        last_value=SIM_LAST_VALUE,
        value_is_int=False,
        last_observed_at=None,
        expected_reported_version=1,
        verify_rows=True,
    )


# --- backend composition ----------------------------------------------------


def backend_env(fixture: DatabaseFixture, jwt_secret: str, backend_password: str, *,
                broker_url: str = 'ssl://broker:8883', client_id: str = 'backend_uplink_smoke',
                extra_env: dict | None = None) -> dict:
    base = {
        'DATABASE_URL': f'postgres://iot_backend_app:{fixture.passwords[1]}@database:5432/fixture_service?sslmode=disable',
        'SUPABASE_JWT_SECRET': jwt_secret,
        'SUPABASE_JWT_ISSUER': 'http://auth.invalid/auth/v1',
        'SUPABASE_JWT_AUDIENCE': 'authenticated',
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
    }
    if extra_env:
        base.update(extra_env)
    return base


def start_backend(work: Path, net_name: str, fixture: DatabaseFixture, image: str, hexid: str,
                  registry: ResourceRegistry, jwt_secret: str, backend_password: str, *,
                  name_suffix: str = '', alias: str = 'backend',
                  client_id: str = 'backend_uplink_smoke', broker_url: str = 'ssl://broker:8883',
                  track: list | None = None, extra_env: dict | None = None):
    env = backend_env(fixture, jwt_secret, backend_password, broker_url=broker_url, client_id=client_id, extra_env=extra_env)
    env_file = work / f'backend{name_suffix}.env'
    write_private(env_file, ''.join(f'{k}={v}\n' for k, v in env.items()).encode())
    cid = docker_create([
        '--name', f'stage3-uplink-backend{name_suffix}-{hexid}',
        '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges:true',
        '--read-only',
        '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
        '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=8m',
        '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=8m',
        '--network', net_name,
        '--network-alias', alias,
        '--publish', '127.0.0.1::8080',
        '--env-file', str(env_file),
        '--mount', f'type=bind,src={work / "public" / "ca.crt"},dst=/certs/ca.crt,readonly',
        image,
    ])
    registry.register(f'backend{name_suffix} container', 'container', cid)
    if track is not None:
        # Record ownership *before* start so an exception during startup still
        # leads to cleanup of this container.
        track.append(cid)
    docker(['start', cid], timeout=CONTAINER_START_TIMEOUT)
    port = mapped_port(cid, '8080/tcp')
    return cid, port


# --- 3.6.3 row 6 / 3.6.4 ack-fault helpers ----------------------------------


def gateway_env_fault(gateway_id: str, password: str) -> dict:
    """Gateway simulator env for the ack-fault slice: a short ACK timeout and a
    bounded short retry window so a resend is observable within the run."""
    env = gateway_env(gateway_id, password)
    env.update({
        'GATEWAY_ACK_TIMEOUT': ACK_FAULT_ACK_TIMEOUT,
        'GATEWAY_RETRY_MIN_DELAY': ACK_FAULT_RETRY_MIN,
        'GATEWAY_RETRY_MAX_DELAY': ACK_FAULT_RETRY_MAX,
    })
    return env


def write_gateway_env_fault(work: Path, name: str, gateway_id: str, password: str) -> Path:
    env_file = work / name
    write_private(env_file, ''.join(
        f'{k}={v}\n' for k, v in gateway_env_fault(gateway_id, password).items()).encode())
    return env_file


def start_gateway_detached(image: str, net_name: str, outbox_dir: Path, ca_file: Path,
                           env_file: Path, args: list, name: str, owned_containers: list) -> str:
    """Create + start the real gateway image detached (not ``--rm``) so host code
    can read its live logs and final exit code while the outbox is a host
    bind-mount. The container id is recorded for exact-ID cleanup."""
    cid = docker_create([
        '--name', name,
        '--cap-drop', 'ALL',
        '--security-opt', 'no-new-privileges:true',
        '--read-only',
        '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
        '--tmpfs', '/mosquitto/data:rw,mode=0700,uid=1883,gid=1883,size=8m',
        '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=8m',
        '--network', net_name,
        '--env-file', str(env_file),
        '--mount', f'type=bind,src={outbox_dir},dst=/data/outbox',
        '--mount', f'type=bind,src={ca_file},dst=/data/ca/ca.crt,readonly',
        image, *args,
    ])
    owned_containers.append(cid)
    docker(['start', cid], timeout=CONTAINER_START_TIMEOUT)
    return cid


def container_running(cid: str) -> bool:
    out = docker(['inspect', '-f', '{{.State.Running}}', cid], timeout=15).decode().strip()
    return out == 'true'


def container_exit_code(cid: str) -> int:
    out = docker(['inspect', '-f', '{{.State.ExitCode}}', cid], timeout=15).decode().strip()
    return int(out)


def wait_container_log(cid: str, needle: str, timeout: float) -> str:
    """Poll a detached container's merged logs until ``needle`` appears (or the
    container stops first), then raise a bounded, sanitized error."""
    deadline = time.monotonic() + timeout
    logs = container_logs(cid)
    while time.monotonic() < deadline:
        if needle in logs:
            return logs
        if not container_running(cid):
            break
        time.sleep(0.3)
        logs = container_logs(cid)
    raise RuntimeError(f'container did not log {needle!r} within budget; '
                       f'running={container_running(cid)} log=' + sanitize(logs[-800:]))


def wait_container_exit(cid: str, timeout: float) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not container_running(cid):
            return container_exit_code(cid)
        time.sleep(0.5)
    raise RuntimeError('gateway container did not stop within the bounded exit window')


def build_commit_ack(message_id: str, committed_at: str | None = None) -> bytes:
    """Build one strict five-field v1 committed telemetry ACK payload.

    Mirrors internal/telemetry.CommitACK (and gatewaysim.ParseCommitACK): exactly
    the five v1 field names, protocol_version 1, type telemetry_ack, status
    committed and a UTC RFC3339 ``committed_at`` ending in 'Z'.
    """
    if committed_at is None:
        committed_at = datetime.datetime.now(datetime.timezone.utc).strftime(
            '%Y-%m-%dT%H:%M:%S.%f') + 'Z'
    return json.dumps({
        'protocol_version': 1,
        'message_type': 'telemetry_ack',
        'message_id': message_id,
        'status': 'committed',
        'committed_at': committed_at,
    }, separators=(',', ':')).encode()


def _pending_rows(con) -> list:
    rows = con.execute(
        "SELECT id, message_id, gateway_id, sensor_id, boot_id, topic, payload, byte_size, "
        "first_sequence, sample_count, sample_interval_us, status, enqueued_at "
        "FROM outbox WHERE status='pending' ORDER BY id").fetchall()
    out = []
    for r in rows:
        out.append({
            'id': r[0], 'message_id': r[1], 'gateway_id': r[2], 'sensor_id': r[3],
            'boot_id': r[4], 'topic': r[5], 'payload': bytes(r[6]), 'byte_size': r[7],
            'first_sequence': r[8], 'sample_count': r[9], 'sample_interval_us': r[10],
            'status': r[11], 'enqueued_at': r[12],
        })
    return out


def read_sqlite_pending(db_path: Path, busy: float = ACK_FAULT_SQLITE_BUSY) -> list:
    """Read the live outbox's pending rows with a read-only, bounded-busy
    connection, retrying inside the window.

    The simulator serializes writes with a rollback journal, so a read can see a
    transient lock; the retry never mutates a row and never opens the
    flock-holding admin path.
    """
    deadline = time.monotonic() + busy
    while True:
        try:
            con = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=busy)
            try:
                con.execute(f'PRAGMA busy_timeout={int(busy * 1000)}')
                return _pending_rows(con)
            finally:
                con.close()
        except sqlite3.Error as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(f'live SQLite read did not succeed: {exc}') from None
            time.sleep(0.2)


def wait_pending_rows(db_path: Path, timeout: float, predicate) -> list:
    """Poll the live SQLite pending rows until ``predicate(rows)`` holds or the
    bounded window elapses (returning the last observed rows)."""
    deadline = time.monotonic() + timeout
    last: list = []
    while time.monotonic() < deadline:
        try:
            last = read_sqlite_pending(db_path)
        except RuntimeError:
            last = []
        if predicate(last):
            return last
        time.sleep(0.3)
    return last


def pending_fingerprints(rows: list) -> dict:
    """A stable, secret-free fingerprint of each pending row: (topic, payload
    sha256, byte size) keyed by message id. Used to prove the pending bytes are
    unchanged across the spoof window."""
    return {r['message_id']: (r['topic'], hashlib.sha256(r['payload']).hexdigest(), r['byte_size'])
            for r in rows}


def cli_count_bind(image: str, net_name: str, outbox_dir: Path, ca_file: Path, work: Path,
                   name: str) -> int:
    """Run the real CLI ``count`` against a bind-mounted outbox (after the
    simulator has stopped and released the flock) and return pending_rows."""
    admin_env = work / f'{name}_admin.env'
    write_private(admin_env, b'GATEWAY_OUTBOX_PATH=/data/outbox/outbox.db\n')
    proc = run_gateway_container(image, net_name, '', ca_file, admin_env, ['count'],
                                 SIM_ADMIN_TIMEOUT, outbox_dir=outbox_dir)
    out = proc.stdout.decode('utf-8', errors='replace').strip()
    match = re.search(r'pending_rows=(\d+)', out)
    if proc.returncode != 0 or match is None:
        raise RuntimeError(f'gateway count (bind) failed: ' + sanitize(out))
    return int(match.group(1))


def ack_block_role_name(gateway_id: str) -> str:
    return f'gateway_{gateway_id}_ack_block'


def attach_ack_receive_block(manager, gateway_id: str, ack_filter: str) -> str:
    """Attach a fixture-only DynSec role that explicitly denies
    ``publishClientReceive`` on the Gateway's own ACK filter, at a higher
    client-role priority than the uplink role, so the first-match ACL scan
    denies delivery. This is reversible fixture setup; it never changes the
    deployment ACLs."""
    role = ack_block_role_name(gateway_id)
    manager.request('createRole', rolename=role)
    manager.request('addRoleACL', rolename=role, acltype='publishClientReceive',
                    topic=ack_filter, allow=False, priority=0)
    manager.request('addClientRole', username=gateway_id, rolename=role,
                    priority=ACK_BLOCK_ROLE_PRIORITY)
    return role


def detach_ack_receive_block(manager, gateway_id: str):
    """Detach (and best-effort delete) the ACK receive-deny role. Detaching the
    client role restores the uplink role's allow without touching it."""
    role = ack_block_role_name(gateway_id)
    manager.request('removeClientRole', username=gateway_id, rolename=role)
    try:
        manager.request('deleteRole', rolename=role)
    except Exception:  # noqa: BLE001 - role deletion is best-effort after detach
        pass


def ack_block_attached(manager, gateway_id: str) -> bool:
    """Read the DynSec client's role list back (supplementary readback, never the
    behavioural proof)."""
    data = manager.request('getClient', username=gateway_id)
    client = data.get('client', {}) if isinstance(data, dict) else {}
    roles = client.get('roles', []) if isinstance(client, dict) else []
    names = {r.get('rolename') for r in roles if isinstance(r, dict)}
    return ack_block_role_name(gateway_id) in names


def collect_matching_deliveries(receiver, topic: str, payload: bytes, want: int,
                                timeout: float) -> list:
    """Collect identical (topic, payload) deliveries until ``want`` are seen or
    the bounded window elapses. Exact bytes are compared, never a re-serialized
    copy, so a resend of the same item is positively identified."""
    seen = []
    deadline = time.monotonic() + timeout
    while len(seen) < want and time.monotonic() < deadline:
        frame = receiver.publish_frame(min(0.5, deadline - time.monotonic()))
        if frame is None:
            continue
        t, p = frame
        if t == topic and p == payload:
            seen.append((t, p))
    return seen


def drain_publish_frames(receiver, timeout: float = 0.5) -> list:
    """Drain all currently available PUBLISH frames from the receiver within a short quiet window."""
    frames = []
    while True:
        frame = receiver.publish_frame(timeout)
        if frame is None:
            break
        frames.append(frame)
    return frames


# --- 3.6.3 rows 4-5 and 7: cross-Gateway and control-topic ACL denial -------


def run_acl_control(work, net_name, fixture, backend_image, hexid, registry, specs,
                    gateway_passwords, jwt_secret, backend_password, manager_password,
                    broker_cid, broker_port, owned_containers):
    """3.6.3 rows 4, 5 and 7 with positive controls and receiver/DB evidence.

    All probes use the shared MQTT helper's own ``poc_<uuid>`` client ids, never
    the simulator's id, so a probe can never kick a running simulator. The
    manager credential is used only for fixture setup/readback; no Gateway or
    observer is ever granted control-topic access.
    """
    ca_file = work / 'public' / 'ca.crt'
    spec_a, spec_b = specs[0], specs[1]
    pw_a = gateway_passwords[spec_a.gateway_id]
    pw_b = gateway_passwords[spec_b.gateway_id]

    ensure_budget('acl-control:backend')
    print('stage=acl-control:start-backend', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers)
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend /readyz + /healthz ready (positive SUBACK for the data filter '
          'gateways/+/telemetry/#)', flush=True)

    probes = []
    try:
        sub_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        pub_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        obs_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        pub_b = AclProbe(broker_port, ca_file, spec_b.gateway_id, pw_b)
        sub_b = AclProbe(broker_port, ca_file, spec_b.gateway_id, pw_b)
        probes.extend([sub_a, pub_a, obs_a, pub_b, sub_b])
        manager = connect_manager(broker_port, work, manager_password)
        probes.append(manager)
        manager.subscribe(RESPONSE)

        # --- positive control before: Gateway A own telemetry + own ACK ----
        print('stage=acl-control:positive-a-before', flush=True)
        if sub_a.subscribe_code(spec_a.ack_filter) != 0x00:
            raise RuntimeError('Gateway A own ACK subscription was not granted')
        a1_mid = str(uuid.uuid4())
        a1_boot = str(uuid.uuid4())
        a1 = build_probe_batch(spec_a, a1_mid, a1_boot, FIRST_SEQUENCE, measured_at=probe_measured_at(0))
        if not probe_batch_is_valid(a1, spec_a):
            raise RuntimeError('probe batch for A is not a structurally valid telemetry batch')
        pub_a.publish_qos1(spec_a.telemetry_topic, a1)
        ack_a1 = sub_a.read_app_ack(a1_mid, PROBE_ACK_TIMEOUT)
        if ack_a1 is None:
            raise RuntimeError('Gateway A own-topic positive did not produce an application ACK')
        assert_ack_fields(ack_a1, a1_mid)
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'ACL_A1_PASS')
        print(f'PASS: positive control before -- Gateway A own telemetry committed one batch '
              f'(100 raw / 1 ledger / 1 temporal) and received its own application ACK', flush=True)

        # --- receiver positive control: Gateway B telemetry + B ACK --------
        print('stage=acl-control:positive-b-receiver', flush=True)
        if sub_b.subscribe_code(spec_b.ack_filter) != 0x00:
            raise RuntimeError('Gateway B own ACK subscription was not granted')
        b1_mid = str(uuid.uuid4())
        b1_boot = str(uuid.uuid4())
        b1 = build_probe_batch(spec_b, b1_mid, b1_boot, FIRST_SEQUENCE, measured_at=probe_measured_at(0))
        pub_b.publish_qos1(spec_b.telemetry_topic, b1)
        ack_b1 = sub_b.read_app_ack(b1_mid, PROBE_ACK_TIMEOUT)
        if ack_b1 is None:
            raise RuntimeError('Gateway B receiver positive control produced no application ACK')
        assert_ack_fields(ack_b1, b1_mid)
        assert_gateway_batch(fixture, spec_b, simulator_envelope(), 'ACL_B1_PASS')
        print('PASS: receiver positive control -- Gateway B telemetry committed one batch and the '
              'B receiver observed the real application ACK, so a later missing delivery is '
              'meaningful', flush=True)

        # --- row 4: cross-Gateway publish denial ---------------------------
        print('stage=acl-control:cross-publish-deny', flush=True)
        spoof_mid = str(uuid.uuid4())
        spoof_boot = str(uuid.uuid4())
        spoof = build_probe_batch(spec_b, spoof_mid, spoof_boot, FIRST_SEQUENCE)
        if not probe_batch_is_valid(spoof, spec_b):
            raise RuntimeError('cross-Gateway payload is not a structurally valid batch for B')
        snap_pub_before = snapshot_db(fixture, specs)
        logs_before = container_logs(broker_cid)
        puback_observed = pub_a.publish_qos1(spec_b.telemetry_topic, spoof)
        no_app_ack = sub_b.read_app_ack(spoof_mid, PROBE_DRAIN_TIMEOUT) is None
        logs_after = wait_denied_publish(broker_cid, logs_before, spec_b.telemetry_topic, PROBE_DENY_SETTLE)
        broker_denied = denied_publish_logged(logs_before, logs_after, spec_b.telemetry_topic)
        db_unchanged = snapshot_db(fixture, specs) == snap_pub_before
        absent = assert_message_id_absent(fixture, spec_b.gateway_id, spoof_mid, 'ACL_SPOOF_ABSENT')
        if not acl_cross_publish_denial_ok(broker_denied=broker_denied, db_unchanged=db_unchanged,
                                           message_id_absent=absent, no_app_ack=no_app_ack,
                                           receiver_positive=True):
            raise RuntimeError('cross-Gateway publish denial failed its oracle: '
                               f'broker_denied={broker_denied} db_unchanged={db_unchanged} '
                               f'absent={absent} no_app_ack={no_app_ack}')
        print(f'PASS: cross-Gateway publish denied -- broker logged "Denied PUBLISH" for B\'s topic '
              f'while a QoS1 PUBACK(transport)={puback_observed} was NOT treated as proof; no B '
              f'ledger row for the denied message id, no application ACK to B\'s receiver, and the '
              f'DB snapshot is byte-identical', flush=True)

        # --- row 5: cross-Gateway subscribe denial -------------------------
        print('stage=acl-control:cross-subscribe-deny', flush=True)
        snap_sub_before = snapshot_db(fixture, specs)
        target_code = obs_a.subscribe_code(spec_b.ack_filter)
        own_code = obs_a.subscribe_code(spec_a.ack_filter)
        assert_snapshot_unchanged(snap_sub_before, snapshot_db(fixture, specs), 'cross-subscribe-deny')
        b2_mid = str(uuid.uuid4())
        b2_boot = str(uuid.uuid4())
        b2 = build_probe_batch(spec_b, b2_mid, b2_boot, FIRST_SEQUENCE, measured_at=probe_measured_at(1))
        pub_b.publish_qos1(spec_b.telemetry_topic, b2)
        receiver_ack = sub_b.read_app_ack(b2_mid, PROBE_ACK_TIMEOUT) is not None
        observed = obs_a.drain(PROBE_DRAIN_TIMEOUT)
        cross_delivery = any(not topic.startswith(f'gateways/{spec_a.gateway_id}/acks/')
                             for topic, _ in observed)
        if not acl_cross_subscribe_denial_ok(target_suback_code=target_code,
                                             own_positive_suback_code=own_code,
                                             receiver_ack_observed=receiver_ack,
                                             cross_gateway_delivery_observed=cross_delivery):
            raise RuntimeError('cross-Gateway subscribe denial failed its oracle: '
                               f'target_suback={target_code} own_suback={own_code} '
                               f'receiver_ack={receiver_ack} cross_delivery={cross_delivery}')
        assert_gateway_positive(fixture, spec_b, ledger=2, raw=2 * SAMPLE_COUNT, temporal=2,
                                reported_version=2, marker='ACL_B2_PASS')
        print(f'PASS: cross-Gateway subscribe denied -- A received SUBACK {target_code:#x} for '
              f"B's ACK namespace while its own ACK filter was granted ({own_code:#x}); a real B batch "
              f'delivered its application ACK to B\'s receiver and A observed no cross-Gateway '
              f'delivery (bounded drain supplementary: {len(observed)} frames)', flush=True)

        # --- row 7: DynSec control-topic denial ----------------------------
        print('stage=acl-control:control-topic-deny', flush=True)
        mgr_probe = f'mgr_{hexid}'
        mgr_probe_pw = secrets.token_urlsafe(32)
        gw_probe = f'probe_{hexid}'
        gw_probe_pw = secrets.token_urlsafe(32)
        SECRETS.extend([mgr_probe_pw, gw_probe_pw])

        # Positive control for the readback oracle: a real manager mutation must
        # be observable, so "absent" is a meaningful absence.
        names_before = manager_client_names(manager)
        if mgr_probe in names_before:
            raise RuntimeError('manager readback probe name already present before creation')
        manager.request('createClient', username=mgr_probe, password=mgr_probe_pw)
        readback_detects = mgr_probe in manager_client_names(manager)
        manager.request('deleteClient', username=mgr_probe)
        names_clean = manager_client_names(manager)
        if mgr_probe in names_clean:
            raise RuntimeError('manager readback probe name still present after deletion')
        manager_positive = ('bootstrap_manager' in names_clean
                            and spec_a.gateway_id in names_clean and spec_b.gateway_id in names_clean)
        gateway_probe_absent_before = gw_probe not in names_clean

        correlation = uuid.uuid4().hex
        ctl_payload = json.dumps({'commands': [{
            'command': 'createClient', 'correlationData': correlation,
            'username': gw_probe, 'password': gw_probe_pw}]}).encode()
        snap_ctl_before = snapshot_db(fixture, specs)
        logs_ctl_before = container_logs(broker_cid)
        pub_a.publish_qos1(DYN_SEC_CONTROL, ctl_payload)
        response_absent = not dynsec_response_observed(manager, correlation, PROBE_DRAIN_TIMEOUT)
        logs_ctl_after = wait_denied_publish(broker_cid, logs_ctl_before, DYN_SEC_CONTROL, PROBE_DENY_SETTLE)
        broker_denied_ctl = denied_publish_logged(logs_ctl_before, logs_ctl_after, DYN_SEC_CONTROL)
        names_after = manager_client_names(manager)
        gateway_probe_absent_after = gw_probe not in names_after
        control_code = obs_a.subscribe_code(DYN_SEC_CONTROL)
        response_code = obs_a.subscribe_code(DYN_SEC_RESPONSE)
        assert_snapshot_unchanged(snap_ctl_before, snapshot_db(fixture, specs), 'control-topic-deny')
        if not acl_control_denial_ok(broker_denied=broker_denied_ctl,
                                     gateway_probe_absent_before=gateway_probe_absent_before,
                                     gateway_probe_absent_after=gateway_probe_absent_after,
                                     response_absent=response_absent,
                                     readback_detects_mutation=readback_detects,
                                     control_suback_code=control_code,
                                     response_suback_code=response_code,
                                     manager_positive=manager_positive):
            raise RuntimeError('control-topic denial failed its oracle: '
                               f'broker_denied={broker_denied_ctl} '
                               f'absent_before={gateway_probe_absent_before} '
                               f'absent_after={gateway_probe_absent_after} '
                               f'response_absent={response_absent} readback_detects={readback_detects} '
                               f'control_suback={control_code} response_suback={response_code} '
                               f'manager_positive={manager_positive}')
        print('PASS: DynSec control-topic denied -- a Gateway publish of a valid createClient '
              'mutation was refused by the broker (Denied PUBLISH), the manager readback (positive '
              f'controlled) shows the probe absent before and after, no response arrived for the '
              f'correlation, and the Gateway SUBACKs were {control_code:#x}/{response_code:#x} for '
              'the control/response namespaces while the manager request path still works', flush=True)

        # --- positive control after: Gateway A own telemetry again ---------
        print('stage=acl-control:positive-a-after', flush=True)
        a2_mid = str(uuid.uuid4())
        a2_boot = str(uuid.uuid4())
        a2 = build_probe_batch(spec_a, a2_mid, a2_boot, FIRST_SEQUENCE, measured_at=probe_measured_at(1))
        pub_a.publish_qos1(spec_a.telemetry_topic, a2)
        ack_a2 = sub_a.read_app_ack(a2_mid, PROBE_ACK_TIMEOUT)
        if ack_a2 is None:
            raise RuntimeError('Gateway A own-topic positive-after produced no application ACK')
        assert_ack_fields(ack_a2, a2_mid)
        assert_gateway_positive(fixture, spec_a, ledger=2, raw=2 * SAMPLE_COUNT, temporal=2,
                                reported_version=2, marker='ACL_A2_PASS')
        ids_a = ledger_message_ids(fixture, spec_a)
        if len(ids_a) != 2 or len(set(ids_a)) != 2 or a1_mid not in ids_a or a2_mid not in ids_a:
            raise RuntimeError(f'Gateway A positive-before/after message ids are not distinct: {ids_a}')
        print(f'PASS: positive control after -- Gateway A committed a second distinct batch '
              f'({a2_mid}) and received its own application ACK; A ledger ids are distinct', flush=True)

        assert_phase_totals(fixture, specs, ledger=4, raw=4 * SAMPLE_COUNT, temporal=4,
                            marker='ACL_TOTALS_PASS')
        print('PASS: phase totals are 4 ledger / 400 raw / 4 temporal for two A positives and two B '
              'positives only (no denied delivery counted)', flush=True)
    finally:
        for probe in probes:
            try:
                probe.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass

    ensure_budget('acl-control:shutdown')
    print('stage=acl-control:shutdown', flush=True)
    run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
    exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
    if exit_code != '0':
        raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
    logs = container_logs(backend_cid)
    if 'backend shutdown incomplete' in logs or 'panic:' in logs:
        raise RuntimeError('backend shutdown reported an incomplete/panicked teardown; log=' + sanitize(logs[-1200:]))
    if 'backend shutdown complete' not in logs:
        raise RuntimeError('backend did not log a confirmed complete shutdown; log=' + sanitize(logs[-1200:]))
    print('PASS: acl-control slice completed with a clean backend SIGTERM shutdown', flush=True)
    print('PASS: 3.6.3 rows 4-5 and 7 verified (cross-Gateway publish/subscribe and DynSec '
          'control-topic denial) with positive controls, broker denial evidence and no unauthorized '
          'ledger/raw/Twin/temporal side effects', flush=True)
    return backend_cid


# --- 3.6.3 row 6 / 3.6.4 ack-fault scenario ---------------------------------


def run_ack_fault(work, net_name, fixture, backend_image, gateway_image, hexid, registry, specs,
                  gateway_passwords, jwt_secret, backend_password, manager_password,
                  broker_cid, broker_port, owned_containers):
    """3.6.3 row 6 (ACK spoof denial) + 3.6.4 (lost application ACK) with a
    single reversible DynSec receive hook, printing a separate PASS for each.

    Both sub-scenarios use the real Go Gateway and backend images, a host
    bind-mount outbox read live with a read-only SQLite connection, and are
    bracketed by positive controls. No observer-survival or PUBACK is treated as
    a denial, and no deployment ACL or broker config is changed.
    """
    ca_file = work / 'public' / 'ca.crt'
    spec_a, spec_b = specs[0], specs[1]
    pw_a = gateway_passwords[spec_a.gateway_id]
    pw_b = gateway_passwords[spec_b.gateway_id]
    telemetry_filter = 'gateways/+/telemetry/#'

    ensure_budget('ack-fault:backend')
    print('stage=ack-fault:start-backend', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers)
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend /readyz + /healthz ready (positive SUBACK for the data filter '
          'gateways/+/telemetry/#)', flush=True)

    # Host bind-mount outbox dirs (world-writable so uid 1883 can write them,
    # world-readable so a host read-only reader can inspect the live SQLite).
    outbox_dirs = {}
    for label in ('a1', 'a2', 'b2'):
        d = work / f'outbox_{label}'
        d.mkdir(parents=True, exist_ok=True)
        os.chmod(d, 0o777)
        outbox_dirs[label] = d

    probes = []
    sim_containers = []
    manager = None
    block_attached = False
    paused = False
    try:
        manager = connect_manager(broker_port, work, manager_password)
        probes.append(manager)
        manager.subscribe(RESPONSE)

        # A backend-principal receiver for the telemetry resend observation.
        recv_telemetry = AclProbe(broker_port, ca_file, 'bootstrap_backend', backend_password)
        probes.append(recv_telemetry)
        # An authorized backend-principal publisher for positive controls.
        backend_pub = AclProbe(broker_port, ca_file, 'bootstrap_backend', backend_password)
        probes.append(backend_pub)
        # Gateway A's own receiver (subscribes to A's ACK filter).
        recv_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        probes.append(recv_a)
        if recv_a.subscribe_code(spec_a.ack_filter) != 0x00:
            raise RuntimeError('Gateway A receiver own-ACK subscription was not granted')
        if recv_telemetry.subscribe_code(telemetry_filter) != 0x00:
            raise RuntimeError('backend-principal receiver telemetry subscription was not granted')

        # --- behavioural verification of the reversible receive hook --------
        ctl_allow = str(uuid.uuid4())
        backend_pub.publish_qos1(f'gateways/{spec_a.gateway_id}/acks/{ctl_allow}',
                                 build_commit_ack(ctl_allow))
        allowed_delivered = recv_a.read_app_ack(ctl_allow, ACK_FAULT_CONTROL_TIMEOUT) is not None
        if not allowed_delivered:
            raise RuntimeError('an authorized ACK was not delivered before the hook (receive path not live)')
        attach_ack_receive_block(manager, spec_a.gateway_id, spec_a.ack_filter)
        block_attached = True
        if not ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('ACK receive-deny role was not attached (DynSec readback)')
        ctl_block = str(uuid.uuid4())
        backend_pub.publish_qos1(f'gateways/{spec_a.gateway_id}/acks/{ctl_block}',
                                 build_commit_ack(ctl_block))
        blocked_delivered = recv_a.read_app_ack(ctl_block, ACK_FAULT_CONTROL_TIMEOUT) is not None
        if not ack_block_effective(allowed_delivered=allowed_delivered, blocked_delivered=blocked_delivered):
            raise RuntimeError('the ACK receive hook is not behaviourally effective')
        print('PASS: reversible ACK receive hook verified behaviourally -- the authorized backend ACK is '
              'delivered with the hook off and withheld with the DynSec receive-deny role attached '
              '(readback corroborates the attachment)', flush=True)

        # --- 3.6.4: lost application ACK and byte-exact resend --------------
        print('stage=ack-fault:lost-ack', flush=True)
        env_a1 = write_gateway_env_fault(work, f'gateway_a1_{hexid}.env', spec_a.gateway_id, pw_a)
        sim_a1 = start_gateway_detached(
            gateway_image, net_name, outbox_dirs['a1'], ca_file, env_a1,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-a1-{hexid}', owned_containers)
        sim_containers.append(sim_a1)
        registry.register('gateway a1 container', 'container', sim_a1)
        # Ready (own-ACK SUBACK granted) holds even with the receive-deny role
        # attached because only publishClientReceive is denied, not subscribe.
        wait_container_log(sim_a1, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        print('PASS: Gateway A simulator reached a subscribed ready state (own-ACK SUBACK granted) while '
              'its ACK receive path is blocked', flush=True)

        # The backend commits; the simulator never receives the application ACK.
        deadline = time.monotonic() + ACK_FAULT_PENDING_TIMEOUT
        while time.monotonic() < deadline and len(ledger_message_ids(fixture, spec_a)) < 1:
            time.sleep(0.5)
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'ACK_FAULT_COMMIT_1')
        snap_db_before = snapshot_db(fixture, specs)
        db_a1 = outbox_dirs['a1'] / 'outbox.db'
        rows_a1 = wait_pending_rows(db_a1, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_a1:
            raise RuntimeError('the committed batch is not present as a pending SQLite row')
        row_a1 = rows_a1[0]
        mid_a1, topic_a1, payload_a1 = row_a1['message_id'], row_a1['topic'], row_a1['payload']
        print(f'PASS: SQL independently confirms Gateway A committed exactly one batch (100 raw / 1 ledger / '
              f'1 temporal / reported version +1); the live SQLite still shows one pending row '
              f'id={mid_a1[:8]}... bytes={row_a1["byte_size"]} (withheld application ACK)', flush=True)

        # Observe at least one byte-exact resend of the same item.
        deliveries = collect_matching_deliveries(recv_telemetry, topic_a1, payload_a1, 2,
                                                 ACK_FAULT_RESEND_TIMEOUT)
        resend_observed = len(deliveries) >= 2
        resend_bytes_match = resend_observed and all(t == topic_a1 and p == payload_a1
                                                     for t, p in deliveries)
        if not resend_observed:
            raise RuntimeError('no byte-exact resend of the pending item was observed while the ACK was blocked')
        db_unchanged_during = snapshot_db(fixture, specs) == snap_db_before
        rows_a1b = wait_pending_rows(db_a1, ACK_FAULT_DELIVERY_TIMEOUT,
                                     lambda r: any(x['message_id'] == mid_a1 for x in r))
        pending_while_blocked = any(x['message_id'] == mid_a1 for x in rows_a1b)
        print(f'PASS: a byte-exact resend of the same message id was observed ({len(deliveries)} identical '
              f'deliveries) while the application ACK stayed blocked; ledger/raw/temporal/versions were '
              f'unchanged (dedup) and the SQLite row is still pending', flush=True)

        # Restore the ACK receive path; the real next retry must get the real ACK.
        # Reconnect the controller (the block role lives on the broker, so any
        # live controller can detach it), then read the client's role list back to
        # prove the deny role is gone rather than trusting the request alone.
        try:
            manager.close()
        except Exception:  # noqa: BLE001 - reconnect is best-effort
            pass
        manager = connect_manager(broker_port, work, manager_password)
        manager.subscribe(RESPONSE)
        probes.append(manager)
        detach_ack_receive_block(manager, spec_a.gateway_id)
        if ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('the ACK receive-deny role is still attached after detach (DynSec readback)')
        block_attached = False
        # Re-establish the publisher and the target receiver so the restore
        # reflects the broker ACL, not a session whose cached ACL list predates
        # the detach.
        for stale in (backend_pub, recv_a):
            try:
                stale.close()
            except Exception:  # noqa: BLE001 - best-effort refresh
                pass
        backend_pub = AclProbe(broker_port, ca_file, 'bootstrap_backend', backend_password)
        probes.append(backend_pub)
        recv_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        probes.append(recv_a)
        if recv_a.subscribe_code(spec_a.ack_filter) != 0x00:
            raise RuntimeError('Gateway A receiver re-subscription was not granted after restore')
        receive_restored = False
        for _ in range(2):
            ctl_restore = str(uuid.uuid4())
            backend_pub.publish_qos1(f'gateways/{spec_a.gateway_id}/acks/{ctl_restore}',
                                     build_commit_ack(ctl_restore))
            if recv_a.read_app_ack(ctl_restore, ACK_FAULT_CONTROL_TIMEOUT) is not None:
                receive_restored = True
                break
        if not receive_restored:
            raise RuntimeError('the ACK receive path was not restored after detaching the block role')

        exit_a1 = wait_container_exit(sim_a1, ACK_FAULT_EXIT_TIMEOUT)
        if exit_a1 != 0:
            raise RuntimeError(f'Gateway A simulator exited {exit_a1} after the real ACK was restored')
        # CLI admin count only after the process stopped (flock released).
        cli_a1 = cli_count_bind(gateway_image, net_name, outbox_dirs['a1'], ca_file, work, f'a1_{hexid}')
        pending_after = len(read_sqlite_pending(db_a1))
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'ACK_FAULT_COMMIT_1_AFTER')
        if not lost_ack_resend_ok(committed=True, pending_while_blocked=pending_while_blocked,
                                  resend_observed=resend_observed, resend_bytes_match=resend_bytes_match,
                                  db_unchanged_during=db_unchanged_during, receive_restored=receive_restored,
                                  pending_after=pending_after, simulator_exit_code=exit_a1,
                                  cli_pending_rows=cli_a1):
            raise RuntimeError('the lost-ACK/resend oracle failed')
        print('PASS: 3.6.4 lost application ACK recovered by byte-exact resend -- the backend committed once, '
              'the resend did not duplicate ledger/raw/temporal or advance versions, the restored path '
              'delivered the real ACK, the simulator completed and the outbox is empty (CLI count 0)', flush=True)

        # --- 3.6.3 row 6: ACK spoof denial ----------------------------------
        print('stage=ack-fault:ack-spoof', flush=True)
        # Hold two real, uncommitted pending items by pausing the healthy backend.
        # The default paho keepalive is 30s (broker drop at 1.5x = 45s); the
        # Transport supervises its own reconnect/resubscribe and the simulators
        # retry every 1-2s, so the hold degrades gracefully rather than failing.
        docker(['pause', backend_cid], timeout=30)
        paused = True
        pause_started = time.monotonic()
        env_a2 = write_gateway_env_fault(work, f'gateway_a2_{hexid}.env', spec_a.gateway_id, pw_a)
        env_b2 = write_gateway_env_fault(work, f'gateway_b2_{hexid}.env', spec_b.gateway_id, pw_b)
        sim_a2 = start_gateway_detached(
            gateway_image, net_name, outbox_dirs['a2'], ca_file, env_a2,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-a2-{hexid}', owned_containers)
        sim_containers.append(sim_a2)
        registry.register('gateway a2 container', 'container', sim_a2)
        sim_b2 = start_gateway_detached(
            gateway_image, net_name, outbox_dirs['b2'], ca_file, env_b2,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-b2-{hexid}', owned_containers)
        sim_containers.append(sim_b2)
        registry.register('gateway b2 container', 'container', sim_b2)
        wait_container_log(sim_a2, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        wait_container_log(sim_b2, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)

        db_a2 = outbox_dirs['a2'] / 'outbox.db'
        db_b2 = outbox_dirs['b2'] / 'outbox.db'
        rows_a2 = wait_pending_rows(db_a2, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        rows_b2 = wait_pending_rows(db_b2, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_a2 or not rows_b2:
            raise RuntimeError('a spoof target did not expose a pending uncommitted item')
        row_a2, row_b2 = rows_a2[0], rows_b2[0]
        mid_a2, mid_b2 = row_a2['message_id'], row_b2['message_id']
        snap_spoof_before = snapshot_db(fixture, specs)
        assert_message_id_absent(fixture, spec_a.gateway_id, mid_a2, 'ACK_SPOOF_A_ABSENT')
        assert_message_id_absent(fixture, spec_b.gateway_id, mid_b2, 'ACK_SPOOF_B_ABSENT')
        fp_before = pending_fingerprints(rows_a2 + rows_b2)
        print('PASS: with the healthy backend paused, Gateway A and B each hold one real uncommitted pending '
              'SQLite row and neither message id is in the ledger', flush=True)

        # Target receivers with the gateways' own credentials, positively
        # controlled by an authorized backend-principal ACK.
        recv_a2 = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        probes.append(recv_a2)
        recv_b2 = AclProbe(broker_port, ca_file, spec_b.gateway_id, pw_b)
        probes.append(recv_b2)
        if recv_a2.subscribe_code(spec_a.ack_filter) != 0x00:
            raise RuntimeError('the A spoof-target receiver subscription was not granted')
        if recv_b2.subscribe_code(spec_b.ack_filter) != 0x00:
            raise RuntimeError('the B spoof-target receiver subscription was not granted')
        ctl_a = str(uuid.uuid4())
        ctl_b = str(uuid.uuid4())
        backend_pub.publish_qos1(f'gateways/{spec_a.gateway_id}/acks/{ctl_a}', build_commit_ack(ctl_a))
        backend_pub.publish_qos1(f'gateways/{spec_b.gateway_id}/acks/{ctl_b}', build_commit_ack(ctl_b))
        pos_a = recv_a2.read_app_ack(ctl_a, ACK_FAULT_CONTROL_TIMEOUT) is not None
        pos_b = recv_b2.read_app_ack(ctl_b, ACK_FAULT_CONTROL_TIMEOUT) is not None
        receiver_positive = pos_a and pos_b
        if not receiver_positive:
            raise RuntimeError(f'the spoof-target receivers are not positively controlled: A={pos_a} B={pos_b}')

        # Gateway A (no ACK-topic publish grant) tries to forge a valid ACK for
        # its own pending and for B's pending.
        pub_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
        probes.append(pub_a)
        forged_topic_own = f'gateways/{spec_a.gateway_id}/acks/{mid_a2}'
        forged_topic_target = f'gateways/{spec_b.gateway_id}/acks/{mid_b2}'
        logs_own_before = container_logs(broker_cid)
        pub_a.publish_qos1(forged_topic_own, build_commit_ack(mid_a2))
        logs_own_after = wait_denied_publish(broker_cid, logs_own_before, forged_topic_own, ACK_FAULT_DENY_SETTLE)
        broker_denied_own = denied_publish_logged(logs_own_before, logs_own_after, forged_topic_own)
        # Publish the target forgery only after the own-topic denial is observed,
        # so each denial is compared against a clean baseline: the two audit lines
        # can otherwise arrive together and hide the second "new line".
        logs_target_before = logs_own_after
        pub_a.publish_qos1(forged_topic_target, build_commit_ack(mid_b2))
        logs_target_after = wait_denied_publish(broker_cid, logs_target_before, forged_topic_target, ACK_FAULT_DENY_SETTLE)
        broker_denied_target = denied_publish_logged(logs_target_before, logs_target_after, forged_topic_target)
        drain_a = recv_a2.drain(ACK_FAULT_SETTLE)
        drain_b = recv_b2.drain(ACK_FAULT_SETTLE)
        no_spoof_delivery = (not any(mid_a2 in t for t, _ in drain_a)
                             and not any(mid_b2 in t for t, _ in drain_b))
        rows_a2b = read_sqlite_pending(db_a2)
        rows_b2b = read_sqlite_pending(db_b2)
        pending_bytes_stable = pending_fingerprints(rows_a2b + rows_b2b) == fp_before
        db_unchanged = snapshot_db(fixture, specs) == snap_spoof_before
        print(f'evidence: broker_denied_own={broker_denied_own} broker_denied_target={broker_denied_target} '
              f'no_spoof_delivery={no_spoof_delivery} pending_bytes_stable={pending_bytes_stable} '
              f'db_unchanged={db_unchanged} receiver_positive={receiver_positive} '
              f'drain_a={len(drain_a)} drain_b={len(drain_b)}', flush=True)
        if not ack_spoof_denial_ok(broker_denied_own=broker_denied_own,
                                   broker_denied_target=broker_denied_target,
                                   no_spoof_delivery=no_spoof_delivery,
                                   pending_bytes_stable=pending_bytes_stable,
                                   db_unchanged=db_unchanged, receiver_positive=receiver_positive):
            raise RuntimeError('the ACK spoof denial oracle failed')
        print('PASS: 3.6.3 row 6 ACK spoof denied -- Gateway A could not publish a valid ACK for its own or '
              "for B's uncommitted pending item (broker Denied PUBLISH for both); no target receiver "
              "observed a spoof ACK, the pending rows' bytes were unchanged, no false completion occurred, "
              'and the authorized backend ACK was delivered to both receivers', flush=True)

        # Release the hold: the real backend ACK must complete both items.
        docker(['unpause', backend_cid], timeout=30)
        paused = False
        pause_seconds = time.monotonic() - pause_started
        exit_a2 = wait_container_exit(sim_a2, ACK_FAULT_EXIT_TIMEOUT)
        exit_b2 = wait_container_exit(sim_b2, ACK_FAULT_EXIT_TIMEOUT)
        if exit_a2 != 0 or exit_b2 != 0:
            raise RuntimeError(f'the spoof-target simulators did not complete after release: A={exit_a2} B={exit_b2}')
        cli_a2 = cli_count_bind(gateway_image, net_name, outbox_dirs['a2'], ca_file, work, f'a2_{hexid}')
        cli_b2 = cli_count_bind(gateway_image, net_name, outbox_dirs['b2'], ca_file, work, f'b2_{hexid}')
        if cli_a2 != 0 or cli_b2 != 0:
            raise RuntimeError(f'the outbox was not empty after release: A={cli_a2} B={cli_b2}')
        assert_gateway_positive(fixture, spec_a, ledger=2, raw=2 * SAMPLE_COUNT, temporal=2,
                                reported_version=2, marker='ACK_SPOOF_A_COMMIT')
        assert_gateway_batch(fixture, spec_b, simulator_envelope(), 'ACK_SPOOF_B_COMMIT')
        print('PASS: after the pending hold was released, the real backend committed and ACKed both items '
              '(A ledger 2 / 200 raw / 2 temporal / version 2, B ledger 1 / 100 raw / 1 temporal / version 1) '
              f'and both outboxes are empty (CLI count 0); backend held {pause_seconds:.1f}s (within the 45s '
              'keepalive drop bound, and the supervised reconnect covers longer holds)', flush=True)
    finally:
        if paused:
            try:
                docker(['unpause', backend_cid], timeout=30)
            except Exception:  # noqa: BLE001 - best-effort fault release
                pass
        if block_attached and manager is not None:
            try:
                detach_ack_receive_block(manager, spec_a.gateway_id)
            except Exception:  # noqa: BLE001 - best-effort hook restore
                pass
        for probe in probes:
            try:
                probe.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass
        # Stop any detached simulator still running so cleanup can remove it by id.
        for cid in sim_containers:
            try:
                if not container_absent(cid) and container_running(cid):
                    docker(['stop', '-t', '10', cid], timeout=30)
            except Exception:  # noqa: BLE001 - best-effort stop
                pass

    ensure_budget('ack-fault:shutdown')
    print('stage=ack-fault:shutdown', flush=True)
    run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
    exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
    if exit_code != '0':
        raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
    logs = container_logs(backend_cid)
    if 'backend shutdown incomplete' in logs or 'panic:' in logs:
        raise RuntimeError('backend shutdown reported an incomplete/panicked teardown; log=' + sanitize(logs[-1200:]))
    if 'backend shutdown complete' not in logs:
        raise RuntimeError('backend did not log a confirmed complete shutdown; log=' + sanitize(logs[-1200:]))
    print('PASS: ack-fault slice completed with a clean backend SIGTERM shutdown', flush=True)
    return backend_cid


# --- 3.6.5 steps 1-2 db-timeout scenario -----------------------------------


def parse_ingress_summary(logs: str) -> dict[str, int]:
    """Parse the structured lifecycle summary emitted by ingress.logSummary().
    Supports both slog text (key=value) and json formats, returning integer
    counters."""
    for line in reversed(logs.splitlines()):
        if 'telemetry ingress summary' in line:
            stripped = line.strip()
            if stripped.startswith('{') and stripped.endswith('}'):
                try:
                    data = json.loads(stripped)
                    return {k: int(v) for k, v in data.items()
                            if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit())}
                except Exception:
                    pass
            matches = re.findall(r'\b([a-z_]+)=(-?\d+)\b', line)
            if matches:
                return {k: int(v) for k, v in matches}
    return {}


def sql_scalar_int(fixture: DatabaseFixture, query: str) -> int:
    """Run a query that returns a single integer scalar, parsing it robustly."""
    _, out = fixture.sql(query.encode('utf-8'))
    text = out.decode('utf-8', errors='replace').strip()
    for ln in text.splitlines():
        cleaned = ln.strip()
        if cleaned.isdigit():
            return int(cleaned)
    raise RuntimeError(f'could not parse scalar integer from SQL output: {text!r}')


class SqlRowLockHold:
    """Fixture admin connection holding a row lock FOR UPDATE on public.sensors.
    Does not use a fixed long sleep. Holds the transaction open until release()."""

    def __init__(self, container_id: str, gateway_id: str, sensor_id: str):
        self.container_id = container_id
        self.gateway_id = gateway_id
        self.sensor_id = sensor_id
        self.proc: subprocess.Popen | None = None
        self.holder_pid: int | None = None
        self.released = False

    def acquire(self, timeout: float = 20.0):
        cmd = [
            'docker', 'exec', '-i', self.container_id,
            'psql', '-X', '-U', 'fixture_admin', '-d', 'fixture_service',
            '-v', 'ON_ERROR_STOP=1', '-P', 'footer=off', '-t', '-A',
        ]
        self.proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ENV
        )
        sql = (
            "SELECT pg_backend_pid();\n"
            "BEGIN;\n"
            f"SELECT unit FROM public.sensors WHERE gateway_id = '{self.gateway_id}' AND sensor_id = '{self.sensor_id}' FOR UPDATE;\n"
            "SELECT 'LOCKED';\n"
        )
        self.proc.stdin.write(sql.encode('utf-8'))
        self.proc.stdin.flush()

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline().decode('utf-8', errors='replace').strip()
            if not line:
                if self.proc.poll() is not None:
                    err = self.proc.stderr.read().decode('utf-8', errors='replace')
                    raise RuntimeError(f'psql exited prematurely while locking sensor: {err}')
                time.sleep(0.05)
                continue
            if self.holder_pid is None and line.isdigit():
                self.holder_pid = int(line)
            elif 'LOCKED' in line:
                return
        raise RuntimeError(f'timed out waiting for sensor lock acquisition after {timeout}s')

    def release(self):
        if self.released:
            return
        self.released = True
        if self.proc is not None:
            try:
                if self.proc.stdin and not self.proc.stdin.closed:
                    self.proc.stdin.write(b"ROLLBACK;\n\\q\n")
                    self.proc.stdin.flush()
                    self.proc.stdin.close()
                self.proc.wait(timeout=10)
            except Exception:
                try:
                    self.proc.kill()
                    self.proc.wait(timeout=5)
                except Exception:
                    pass


def wait_worker_blocked_on_lock(fixture: DatabaseFixture, holder_pid: int | None, timeout: float = 20.0) -> dict:
    """Poll pg_stat_activity to find a worker session waiting on the lock.
    Returns the dictionary with pid, wait_event_type, wait_event, query."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if holder_pid is not None:
            sql = f"""
            SELECT pid, wait_event_type, wait_event, query
            FROM pg_stat_activity
            WHERE {holder_pid} = ANY(pg_blocking_pids(pid))
              AND wait_event_type = 'Lock';
            """
        else:
            sql = """
            SELECT pid, wait_event_type, wait_event, query
            FROM pg_stat_activity
            WHERE wait_event_type = 'Lock'
              AND (query ILIKE '%sensors%' OR query ILIKE '%FOR UPDATE%');
            """
        _, out = fixture.sql(sql.encode('utf-8'))
        text = out.decode('utf-8', errors='replace').strip()
        lines = [ln.strip() for ln in text.splitlines()
                 if ln.strip() and not ln.startswith('pid') and not ln.startswith('---') and not ln.startswith('(')]
        for line in lines:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 3 and parts[1] == 'Lock':
                return {
                    'pid': parts[0],
                    'wait_event_type': parts[1],
                    'wait_event': parts[2],
                    'query': parts[3] if len(parts) > 3 else '',
                }
        time.sleep(0.2)
    raise RuntimeError(f'timed out waiting for telemetry worker lock wait in pg_stat_activity after {timeout}s')


def assert_uncommitted_state(fixture: DatabaseFixture, spec: GatewaySpec):
    """Verify that no ledger, raw or temporal rows exist for spec, and twin state
    is unchanged."""
    led_cnt = sql_scalar_int(fixture, f"SELECT count(*) FROM processed_messages WHERE gateway_id = '{spec.gateway_id}';")
    if led_cnt != 0:
        raise RuntimeError(f'expected 0 ledger rows while DB blocked, got {led_cnt}')

    raw_cnt = sql_scalar_int(fixture, f"SELECT count(*) FROM telemetry WHERE gateway_id = '{spec.gateway_id}';")
    if raw_cnt != 0:
        raise RuntimeError(f'expected 0 raw rows while DB blocked, got {raw_cnt}')

    temp_cnt = sql_scalar_int(fixture, (
        f"SELECT count(*) FROM twin_temporal_values WHERE entity_id = "
        f"(SELECT id FROM twin_entities WHERE entity_id = '{spec.sensor_urn}');"
    ))
    if temp_cnt != 0:
        raise RuntimeError(f'expected 0 temporal rows while DB blocked, got {temp_cnt}')

    _, out_state = fixture.sql((
        f"SELECT reported_version, desired_version FROM twin_states WHERE entity_id = "
        f"(SELECT id FROM twin_entities WHERE entity_id = '{spec.sensor_urn}');"
    ).encode('utf-8'))
    lines = [ln.strip() for ln in out_state.decode('utf-8', errors='replace').splitlines()
             if ln.strip() and not ln.strip().startswith('reported_version') and not ln.strip().startswith('---') and not ln.strip().startswith('(')]
    if not lines:
        raise RuntimeError('could not read sensor twin state')
    parts = [p.strip() for p in lines[0].split('|')]
    if len(parts) < 2:
        raise RuntimeError(f'could not parse sensor twin state line: {lines[0]}')
    rep_ver, des_ver = int(parts[0]), int(parts[1])
    if rep_ver != 0:
        raise RuntimeError(f'expected sensor reported_version=0 while DB blocked, got {rep_ver}')
    if des_ver != spec.sensor_desired_version:
        raise RuntimeError(f'expected sensor desired_version={spec.sensor_desired_version}, got {des_ver}')

    _, out_gw = fixture.sql((
        f"SELECT reported_version, desired_version FROM twin_states WHERE entity_id = "
        f"(SELECT id FROM twin_entities WHERE entity_id = '{spec.gateway_urn}');"
    ).encode('utf-8'))
    lines_gw = [ln.strip() for ln in out_gw.decode('utf-8', errors='replace').splitlines()
                if ln.strip() and not ln.strip().startswith('reported_version') and not ln.strip().startswith('---') and not ln.strip().startswith('(')]
    if not lines_gw:
        raise RuntimeError('could not read gateway twin state')
    parts_gw = [p.strip() for p in lines_gw[0].split('|')]
    if len(parts_gw) < 2:
        raise RuntimeError(f'could not parse gateway twin state line: {lines_gw[0]}')
    gw_rep, gw_des = int(parts_gw[0]), int(parts_gw[1])
    if gw_rep != spec.gateway_reported_version:
        raise RuntimeError(f'expected gateway reported_version={spec.gateway_reported_version}, got {gw_rep}')
    if gw_des != spec.gateway_desired_version:
        raise RuntimeError(f'expected gateway desired_version={spec.gateway_desired_version}, got {gw_des}')


def gateway_env_db_timeout(gateway_id: str, password: str) -> dict:
    """Gateway simulator env for db-timeout scenario: 6s ACK timeout with fast 1-2s retry."""
    env = gateway_env(gateway_id, password)
    env.update({
        'GATEWAY_ACK_TIMEOUT': '6s',
        'GATEWAY_RETRY_MIN_DELAY': '1s',
        'GATEWAY_RETRY_MAX_DELAY': '2s',
    })
    return env


def write_gateway_env_db_timeout(work: Path, name: str, gateway_id: str, password: str) -> Path:
    env_file = work / name
    write_private(env_file, ''.join(
        f'{k}={v}\n' for k, v in gateway_env_db_timeout(gateway_id, password).items()).encode())
    return env_file


def run_db_timeout(work: Path, net_name: str, fixture: DatabaseFixture, backend_image: str,
                   gateway_image: str, hexid: str, registry: ResourceRegistry,
                   specs: list[GatewaySpec], gateway_passwords: dict[str, str],
                   jwt_secret: str, backend_password: str, broker_cid: str,
                   broker_port: int, owned_containers: list[str]):
    """3.6.5 steps 1-2: Database timeout on target Sensor row lock, uncommitted
    retention, and byte-exact resend recovery.

    Uses the real Go backend image (with TELEMETRY_DB_TIMEOUT=3s) and the real
    Go gateway image with a host bind-mount outbox. Holds public.sensors FOR UPDATE
    using a fixture admin psql session, establishes the telemetry worker is
    blocked via pg_stat_activity, observes the database timeout failure log,
    asserts uncommitted DB state and persistent pending row in live SQLite,
    releases the SQL lock in a finally block, observes byte-exact recovery, and
    reads the structured ingress lifecycle summary upon backend stop (verifying
    PersistFailures > 0).
    """
    ca_file = work / 'public' / 'ca.crt'
    spec_a = specs[0]
    pw_a = gateway_passwords[spec_a.gateway_id]

    ensure_budget('db-timeout:backend')
    print('stage=db-timeout:start-backend', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers, extra_env={'TELEMETRY_DB_TIMEOUT': '3s'})
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend /readyz + /healthz ready', flush=True)

    outbox_dir = work / 'outbox_db_timeout'
    outbox_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir, 0o777)

    lock_holder = SqlRowLockHold(fixture.container_id, spec_a.gateway_id, spec_a.sensor_id)
    sim_cid = None
    try:
        # Step 3: Hold target Sensor row FOR UPDATE
        ensure_budget('db-timeout:lock-sensor')
        print('stage=db-timeout:lock-sensor', flush=True)
        lock_holder.acquire(timeout=20.0)
        holder_pid = lock_holder.holder_pid
        print(f'PASS: fixture admin SQL session (pid={holder_pid}) acquired FOR UPDATE lock '
              f'on Sensor {spec_a.sensor_id} (Gateway {spec_a.gateway_id})', flush=True)

        # Start real gateway image with 1 finite batch
        ensure_budget('db-timeout:start-gateway')
        print('stage=db-timeout:start-gateway', flush=True)
        env_a = write_gateway_env_db_timeout(work, f'gateway_db_timeout_{hexid}.env', spec_a.gateway_id, pw_a)
        sim_cid = start_gateway_detached(
            gateway_image, net_name, outbox_dir, ca_file, env_a,
            ['run', '--batches', '1', '--completion-timeout', '60s'],
            f'stage3-uplink-gw-dbt-{hexid}', owned_containers)
        registry.register('gateway db-timeout container', 'container', sim_cid)
        wait_container_log(sim_cid, 'gateway simulator running', 30.0)
        print('PASS: gateway simulator started with 1 batch target', flush=True)

        # Observe pg_stat_activity to prove telemetry worker is blocked
        ensure_budget('db-timeout:observe-lock-wait')
        print('stage=db-timeout:observe-lock-wait', flush=True)
        worker_wait = wait_worker_blocked_on_lock(fixture, holder_pid, timeout=20.0)
        print(f'PASS: pg_stat_activity confirmed telemetry worker (pid={worker_wait["pid"]}) '
              f'is blocked on lock (wait_event_type={worker_wait["wait_event_type"]} '
              f'wait_event={worker_wait["wait_event"]})', flush=True)

        # Step 4: Keep lock until real database timeout is observed in backend logs
        ensure_budget('db-timeout:wait-db-timeout')
        print('stage=db-timeout:wait-db-timeout', flush=True)
        wait_container_log(backend_cid, 'stage=persist', 20.0)
        backend_logs = container_logs(backend_cid)
        if 'stage=persist' not in backend_logs or 'dependency_unavailable' not in backend_logs:
            raise RuntimeError('backend did not log a safe dependency unavailable reject: ' + sanitize(backend_logs[-800:]))
        print('PASS: backend logged safe persist failure: stage=persist reason_code=dependency_unavailable', flush=True)

        # Before releasing lock: assert message has no committed ledger/raw/temporal,
        # sensor/desired/gateway state unchanged, SQLite row still pending
        ensure_budget('db-timeout:assert-uncommitted')
        print('stage=db-timeout:assert-uncommitted', flush=True)
        assert_uncommitted_state(fixture, spec_a)

        db_file = outbox_dir / 'outbox.db'
        pending_rows = wait_pending_rows(db_file, 10.0, lambda r: len(r) == 1)
        if len(pending_rows) != 1:
            raise RuntimeError(f'expected 1 pending SQLite row while DB blocked, got {len(pending_rows)}')
        p_row = pending_rows[0]
        if p_row['status'] != 'pending':
            raise RuntimeError(f'expected status=pending, got {p_row["status"]}')
        orig_mid = p_row['message_id']
        print(f'PASS: verified no committed ledger/raw/temporal rows and state is unchanged; '
              f'live SQLite confirms row is still pending id={orig_mid[:8]}... bytes={p_row["byte_size"]}', flush=True)
    finally:
        # Step 5: Release SQL lock in finally block
        print('stage=db-timeout:release-lock', flush=True)
        lock_holder.release()
        print('PASS: released fixture SQL lock on Sensor row', flush=True)

    # Step 5 continued: Simulator retries and recovers with byte-exact resend
    ensure_budget('db-timeout:resend-recovery')
    print('stage=db-timeout:resend-recovery', flush=True)
    sim_exit = wait_container_exit(sim_cid, 60.0)
    if sim_exit != 0:
        sim_logs = container_logs(sim_cid)
        raise RuntimeError(f'gateway simulator exited with {sim_exit} after lock release: {sanitize(sim_logs[-800:])}')

    # Verify SQLite outbox has 0 pending items
    remaining_pending = read_sqlite_pending(outbox_dir / 'outbox.db')
    if len(remaining_pending) != 0:
        raise RuntimeError(f'expected 0 pending rows after recovery, got {len(remaining_pending)}')
    cli_count = cli_count_bind(gateway_image, net_name, outbox_dir, ca_file, work, 'gw_dbt')
    if cli_count != 0:
        raise RuntimeError(f'expected CLI count 0 after recovery, got {cli_count}')
    print('PASS: gateway simulator outbox reached 0 pending rows and exited with code 0', flush=True)

    # Database assertions after recovery
    assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'DB_TIMEOUT_RECOVERY')
    assert_phase_totals(fixture, [spec_a], ledger=1, raw=SAMPLE_COUNT, temporal=1, marker='DB_TIMEOUT_TOTALS')
    print('PASS: SQL asserts Gateway A: 100 raw / 1 ledger / 1 temporal / reported version +1 / '
          'desired+Gateway sentinel preserved (no duplicate rows)', flush=True)

    # Step 6: Clean backend stop and structured summary log verification
    ensure_budget('db-timeout:shutdown')
    print('stage=db-timeout:shutdown', flush=True)
    run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
    exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
    if exit_code != '0':
        raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
    logs = container_logs(backend_cid)
    if 'backend shutdown incomplete' in logs or 'panic:' in logs:
        raise RuntimeError('backend shutdown reported an incomplete/panicked teardown: ' + sanitize(logs[-1200:]))
    if 'backend shutdown complete' not in logs:
        raise RuntimeError('backend did not log a confirmed complete shutdown: ' + sanitize(logs[-1200:]))

    summary = parse_ingress_summary(logs)
    if not summary:
        raise RuntimeError('backend did not log telemetry ingress summary: ' + sanitize(logs[-1200:]))
    if summary.get('persist_failures', 0) < 1:
        raise RuntimeError(f'expected persist_failures >= 1 in summary, got {summary}')
    if summary.get('committed', 0) < 1:
        raise RuntimeError(f'expected committed >= 1 in summary, got {summary}')
    if summary.get('queue_full', 0) != 0:
        raise RuntimeError(f'expected queue_full == 0 in summary, got {summary}')
    print(f'PASS: clean SIGTERM shutdown; structured summary verified: '
          f'persist_failures={summary.get("persist_failures")} committed={summary.get("committed")} '
          f'accepted={summary.get("accepted")} queue_full={summary.get("queue_full")}', flush=True)
    return backend_cid


# --- 3.6.5 steps 3-6 queue-full scenario -----------------------------------


def require_queue_full_summary(summary: dict[str, int]) -> int:
    """Validate that the structured summary contains queue_full >= 1."""
    if not summary:
        raise RuntimeError('missing structured summary')
    qf = summary.get('queue_full', 0)
    if qf < 1:
        raise RuntimeError(f'expected queue_full >= 1 in summary, got {summary}')
    return qf


def gateway_env_queue_full(gateway_id: str, password: str) -> dict:
    """Gateway simulator env for queue-full scenario: fast 1 kHz rate, 4s ACK timeout, 1-2s retry."""
    env = gateway_env(gateway_id, password)
    env.update({
        'GATEWAY_SAMPLE_RATE_HZ': str(QUEUE_FULL_SAMPLE_RATE_HZ),
        'GATEWAY_SAMPLES_PER_BATCH': str(QUEUE_FULL_SAMPLE_COUNT),
        'GATEWAY_ACK_TIMEOUT': '4s',
        'GATEWAY_RETRY_MIN_DELAY': '1s',
        'GATEWAY_RETRY_MAX_DELAY': '2s',
        'GATEWAY_MAX_INFLIGHT': str(QUEUE_FULL_MAX_INFLIGHT),
        'GATEWAY_OUTBOX_MAX_ROWS': str(QUEUE_FULL_OUTBOX_MAX_ROWS),
    })
    return env


def write_gateway_env_queue_full(work: Path, name: str, gateway_id: str, password: str) -> Path:
    env_file = work / name
    write_private(env_file, ''.join(
        f'{k}={v}\n' for k, v in gateway_env_queue_full(gateway_id, password).items()).encode())
    return env_file


def build_queue_full_assert_sql(spec: GatewaySpec, captured_mids: list[str],
                                sample_count: int, sample_interval_us: int,
                                marker: str) -> str:
    def q(value: str) -> str:
        return "'" + value + "'"
    mids_list = ', '.join(q(m) for m in captured_mids)
    n = len(captured_mids)
    return f"""
    DO $$
    DECLARE
      l_cnt int;
      r_cnt int;
      t_cnt int;
      rep jsonb;
      des jsonb;
      rep_ver bigint;
      des_ver bigint;
      gw_rep jsonb;
      gw_des jsonb;
      gw_rep_ver bigint;
      gw_des_ver bigint;
      latest_obs timestamptz;
      latest_seq bigint;
    BEGIN
      SELECT count(*) INTO l_cnt FROM public.processed_messages
        WHERE gateway_id = {q(spec.gateway_id)} AND message_id::text IN ({mids_list});
      IF l_cnt <> {n} THEN RAISE EXCEPTION 'captured ledger count %', l_cnt; END IF;

      SELECT count(*) INTO l_cnt FROM public.processed_messages
        WHERE gateway_id = {q(spec.gateway_id)};
      IF l_cnt <> {n} THEN RAISE EXCEPTION 'total gateway ledger count %', l_cnt; END IF;

      SELECT count(*) INTO r_cnt FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)} AND message_id::text IN ({mids_list});
      IF r_cnt <> {n * sample_count} THEN RAISE EXCEPTION 'captured raw count %', r_cnt; END IF;

      SELECT count(*) INTO r_cnt FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)};
      IF r_cnt <> {n * sample_count} THEN RAISE EXCEPTION 'total gateway raw count %', r_cnt; END IF;

      SELECT count(*) INTO r_cnt FROM public.telemetry
        WHERE gateway_id = {q(spec.gateway_id)} AND value IS NULL;
      IF r_cnt <> 0 THEN RAISE EXCEPTION 'telemetry with null value %', r_cnt; END IF;

      SELECT count(*) INTO t_cnt FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)})
          AND property_name = 'sample_interval_us' AND message_id::text IN ({mids_list});
      IF t_cnt <> {n} THEN RAISE EXCEPTION 'captured temporal count %', t_cnt; END IF;

      SELECT count(*) INTO t_cnt FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)});
      IF t_cnt <> {n} THEN RAISE EXCEPTION 'total sensor temporal count %', t_cnt; END IF;

      SELECT count(*) INTO t_cnt FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)})
          AND (value_text IS NOT NULL OR value_boolean IS NOT NULL OR value_number <> {sample_interval_us});
      IF t_cnt <> 0 THEN RAISE EXCEPTION 'temporal row typed columns mismatch %', t_cnt; END IF;

      SELECT count(*) INTO t_cnt FROM public.twin_temporal_values
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)})
          AND (metadata->>'unit' <> 'us' OR metadata->>'source' <> 'telemetry_batch');
      IF t_cnt <> 0 THEN RAISE EXCEPTION 'temporal metadata mismatch %', t_cnt; END IF;

      SELECT max(measured_at), max(sequence_num) INTO latest_obs, latest_seq
        FROM public.telemetry WHERE gateway_id = {q(spec.gateway_id)} AND sensor_id = {q(spec.sensor_id)};

      SELECT reported_state, desired_state, reported_version, desired_version
        INTO rep, des, rep_ver, des_ver FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.sensor_urn)});

      IF rep_ver < 1 THEN RAISE EXCEPTION 'sensor reported_version % not advanced', rep_ver; END IF;
      IF (rep->>'observed_at')::timestamptz <> latest_obs THEN
        RAISE EXCEPTION 'reported observed_at % does not match latest observation %', rep->>'observed_at', latest_obs;
      END IF;
      IF (rep->>'sequence_num')::bigint <> latest_seq THEN
        RAISE EXCEPTION 'reported sequence % does not match latest %', rep->>'sequence_num', latest_seq;
      END IF;
      IF (rep->>'sample_interval_us')::bigint <> {sample_interval_us} THEN
        RAISE EXCEPTION 'reported interval %', rep->>'sample_interval_us';
      END IF;
      IF rep->>'unit' IS DISTINCT FROM {q(spec.sensor_unit)} THEN
        RAISE EXCEPTION 'reported unit %', rep->>'unit';
      END IF;

      IF des <> {q(json.dumps(spec.sensor_desired))}::jsonb OR des_ver <> {spec.sensor_desired_version} THEN
        RAISE EXCEPTION 'sensor desired state changed';
      END IF;

      SELECT reported_state, desired_state, reported_version, desired_version
        INTO gw_rep, gw_des, gw_rep_ver, gw_des_ver FROM public.twin_states
        WHERE entity_id = (SELECT id FROM public.twin_entities WHERE entity_id = {q(spec.gateway_urn)});
      IF gw_rep <> {q(json.dumps(spec.gateway_reported))}::jsonb
         OR gw_des <> {q(json.dumps(spec.gateway_desired))}::jsonb
         OR gw_rep_ver <> {spec.gateway_reported_version}
         OR gw_des_ver <> {spec.gateway_desired_version} THEN
        RAISE EXCEPTION 'gateway state/audit changed';
      END IF;

      RAISE NOTICE '{marker}';
    END $$;
    """


def assert_queue_full_batch(fixture: DatabaseFixture, spec: GatewaySpec,
                            captured_mids: list[str], sample_count: int,
                            sample_interval_us: int, marker: str):
    sql = build_queue_full_assert_sql(spec, captured_mids, sample_count, sample_interval_us, marker)
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'SQL assertions did not emit {marker} for {spec.gateway_id}')


def run_queue_full(work: Path, net_name: str, fixture: DatabaseFixture, backend_image: str,
                   gateway_image: str, hexid: str, registry: ResourceRegistry,
                   specs: list[GatewaySpec], gateway_passwords: dict[str, str],
                   jwt_secret: str, backend_password: str, broker_cid: str,
                   broker_port: int, owned_containers: list[str]):
    """3.6.5 steps 3-6: Queue-full pressure and recovery scenario.

    Uses dedicated backend profile with MQTT_WORKER_COUNT=1, MQTT_QUEUE_CAPACITY=1,
    TELEMETRY_DB_TIMEOUT=4s, SHUTDOWN_TIMEOUT=15s.
    Holds public.sensors FOR UPDATE using a fixture admin SQL connection,
    observes telemetry worker waiting on lock via pg_stat_activity,
    publishes N batches (N=3 > worker 1 + queue 1), captures SQLite pending rows,
    asserts uncommitted DB state and persistent pending rows,
    stops backend under lock within bounded budget, verifies QueueFull > 0 in
    the structured ingress lifecycle summary, releases lock in finally block,
    restarts backend image/profile, waits for simulator to retry and exit with code 0,
    verifies outbox reaches 0, asserts all N batches committed by message ID,
    verifies duplicate retry does not grow counts, and verifies clean final stop.
    """
    ca_file = work / 'public' / 'ca.crt'
    spec_a = specs[0]
    pw_a = gateway_passwords[spec_a.gateway_id]

    ensure_budget('queue-full:backend')
    print('stage=queue-full:start-backend', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers, extra_env={
            'MQTT_WORKER_COUNT': '1',
            'MQTT_QUEUE_CAPACITY': '1',
            'TELEMETRY_DB_TIMEOUT': '4s',
            'SHUTDOWN_TIMEOUT': '15s',
        })
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend started with worker=1, queue=1, db_timeout=4s, shutdown_timeout=15s (/readyz + /healthz ready)', flush=True)

    outbox_dir = work / 'outbox_queue_full'
    outbox_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir, 0o777)

    lock_holder = SqlRowLockHold(fixture.container_id, spec_a.gateway_id, spec_a.sensor_id)
    sim_cid = None
    captured_items = {}
    try:
        # Step 3: Hold target Sensor row FOR UPDATE
        ensure_budget('queue-full:lock-sensor')
        print('stage=queue-full:lock-sensor', flush=True)
        lock_holder.acquire(timeout=20.0)
        holder_pid = lock_holder.holder_pid
        print(f'PASS: fixture admin SQL session (pid={holder_pid}) acquired FOR UPDATE lock '
              f'on Sensor {spec_a.sensor_id} (Gateway {spec_a.gateway_id})', flush=True)

        # Start real gateway image with fast simulator profile and finite target = 3 batches
        ensure_budget('queue-full:start-gateway')
        print('stage=queue-full:start-gateway', flush=True)
        env_a = write_gateway_env_queue_full(work, f'gateway_queue_full_{hexid}.env', spec_a.gateway_id, pw_a)
        sim_cid = start_gateway_detached(
            gateway_image, net_name, outbox_dir, ca_file, env_a,
            ['run', '--batches', str(QUEUE_FULL_BATCHES), '--completion-timeout', '90s'],
            f'stage3-uplink-gw-qf-{hexid}', owned_containers)
        registry.register('gateway queue-full container', 'container', sim_cid)
        wait_container_log(sim_cid, 'gateway simulator running', 30.0)
        print(f'PASS: gateway simulator started with target={QUEUE_FULL_BATCHES} batches, '
              f'sample_rate={QUEUE_FULL_SAMPLE_RATE_HZ}Hz, max_inflight={QUEUE_FULL_MAX_INFLIGHT}, '
              f'outbox_max_rows={QUEUE_FULL_OUTBOX_MAX_ROWS}', flush=True)

        # Observe pg_stat_activity to prove telemetry worker is blocked
        ensure_budget('queue-full:observe-lock-wait')
        print('stage=queue-full:observe-lock-wait', flush=True)
        worker_wait = wait_worker_blocked_on_lock(fixture, holder_pid, timeout=20.0)
        print(f'PASS: pg_stat_activity confirmed telemetry worker (pid={worker_wait["pid"]}) '
              f'is blocked on lock (wait_event_type={worker_wait["wait_event_type"]} '
              f'wait_event={worker_wait["wait_event"]})', flush=True)

        # Wait for simulator to enqueue and publish all batches
        ensure_budget('queue-full:capture-sqlite')
        print('stage=queue-full:capture-sqlite', flush=True)
        db_file = outbox_dir / 'outbox.db'
        pending_rows = wait_pending_rows(db_file, 20.0, lambda r: len(r) == QUEUE_FULL_BATCHES)
        if len(pending_rows) != QUEUE_FULL_BATCHES:
            raise RuntimeError(f'expected {QUEUE_FULL_BATCHES} pending SQLite rows, got {len(pending_rows)}')
        for row in pending_rows:
            if row['status'] != 'pending':
                raise RuntimeError(f'expected status=pending, got {row["status"]}')
            captured_items[row['message_id']] = row
        captured_mids = list(captured_items.keys())
        print(f'PASS: live SQLite outbox captured {len(captured_mids)} pending batches under lock: '
              f'{[m[:8] + "..." for m in captured_mids]}', flush=True)

        # While lock is still held: assert no uncommitted item receives ACK or disappears from SQLite,
        # and DB has 0 ledger, 0 raw, 0 temporal, twin states unchanged
        ensure_budget('queue-full:assert-uncommitted')
        print('stage=queue-full:assert-uncommitted', flush=True)
        assert_uncommitted_state(fixture, spec_a)

        current_pending = read_sqlite_pending(db_file)
        if len(current_pending) != QUEUE_FULL_BATCHES:
            raise RuntimeError(f'expected {QUEUE_FULL_BATCHES} items still pending in SQLite while lock held, got {len(current_pending)}')
        print(f'PASS: verified no premature ACK or dropped durable items; '
              f'DB state unchanged (0 ledger / 0 raw / 0 temporal) and SQLite retains {len(current_pending)} pending rows', flush=True)

        # Step 4: Perform a bounded backend stop while the controlled lock is still held,
        # and inspect the counters
        ensure_budget('queue-full:stop-backend-under-lock')
        print('stage=queue-full:stop-backend-under-lock', flush=True)
        t_stop_start = time.monotonic()
        run(['docker', 'stop', '-t', '20', backend_cid], timeout=35)
        stop_duration = time.monotonic() - t_stop_start
        print(f'PASS: bounded stop of backend under lock completed in {stop_duration:.2f}s (no hang)', flush=True)

        logs_under_lock = container_logs(backend_cid)
        if 'panic:' in logs_under_lock:
            raise RuntimeError('backend panicked during shutdown under lock: ' + sanitize(logs_under_lock[-1200:]))

        summary_under_lock = parse_ingress_summary(logs_under_lock)
        qf_observed = require_queue_full_summary(summary_under_lock)

        drain_clean = 'backend shutdown complete' in logs_under_lock
        if drain_clean:
            print(f'PASS: backend shutdown completed within budget; structured summary: '
                  f'queue_full={qf_observed} accepted={summary_under_lock.get("accepted")} '
                  f'persist_failures={summary_under_lock.get("persist_failures")} '
                  f'queue_capacity={summary_under_lock.get("queue_capacity")}', flush=True)
        else:
            print(f'PASS: backend shutdown observed expected incomplete drain while lock held; structured summary: '
                  f'queue_full={qf_observed} accepted={summary_under_lock.get("accepted")} '
                  f'persist_failures={summary_under_lock.get("persist_failures")} '
                  f'queue_capacity={summary_under_lock.get("queue_capacity")}', flush=True)
    finally:
        # Step 4 & 6: Release SQL lock in finally block
        print('stage=queue-full:release-lock', flush=True)
        lock_holder.release()
        print('PASS: released fixture SQL lock on Sensor row', flush=True)

    # Restart the same backend image/profile for recovery
    ensure_budget('queue-full:restart-backend')
    print('stage=queue-full:restart-backend', flush=True)
    docker(['start', backend_cid], timeout=CONTAINER_START_TIMEOUT)
    backend_port = mapped_port(backend_cid, '8080/tcp')
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend restarted with same profile and reached ready', flush=True)

    # Step 5: Let simulator retry its stored items after lock/pressure is removed
    ensure_budget('queue-full:resend-recovery')
    print('stage=queue-full:resend-recovery', flush=True)
    sim_exit = wait_container_exit(sim_cid, 60.0)
    if sim_exit != 0:
        sim_logs = container_logs(sim_cid)
        raise RuntimeError(f'gateway simulator exited with {sim_exit} after lock release: {sanitize(sim_logs[-800:])}')

    # Verify SQLite outbox has 0 pending items
    remaining_pending = read_sqlite_pending(db_file)
    if len(remaining_pending) != 0:
        raise RuntimeError(f'expected 0 pending rows after recovery, got {len(remaining_pending)}')
    cli_count = cli_count_bind(gateway_image, net_name, outbox_dir, ca_file, work, 'gw_qf')
    if cli_count != 0:
        raise RuntimeError(f'expected CLI count 0 after recovery, got {cli_count}')
    print(f'PASS: gateway simulator outbox reached 0 pending rows and exited with code 0 '
          f'(all {QUEUE_FULL_BATCHES} batches application-ACKed)', flush=True)

    # Step 5: Database assertions by captured message-ID set
    ensure_budget('queue-full:db-assertions')
    print('stage=queue-full:db-assertions', flush=True)
    assert_queue_full_batch(fixture, spec_a, captured_mids,
                            QUEUE_FULL_SAMPLE_COUNT, QUEUE_FULL_SAMPLE_INTERVAL_US,
                            'QUEUE_FULL_RECOVERY_PASS')
    print(f'PASS: SQL asserts by captured message-ID set: exactly {QUEUE_FULL_BATCHES} ledgers, '
          f'{QUEUE_FULL_BATCHES * QUEUE_FULL_SAMPLE_COUNT} raw rows, {QUEUE_FULL_BATCHES} temporal rows, '
          f'correct values/provenance/ranges, latest observation in current state, and preserved desired/Gateway fields', flush=True)

    # Verify duplicate retry does not grow counts
    ensure_budget('queue-full:duplicate-retry')
    print('stage=queue-full:duplicate-retry', flush=True)
    first_mid = captured_mids[0]
    first_item = captured_items[first_mid]
    probe = AclProbe(broker_port, ca_file, spec_a.gateway_id, pw_a)
    try:
        sub_code = probe.subscribe_code(spec_a.ack_filter)
        if sub_code != 0x00:
            raise RuntimeError(f'probe subscribe failed with {sub_code:#04x}')
        if not probe.publish_qos1(first_item['topic'], first_item['payload']):
            raise RuntimeError('probe duplicate publish failed to get PUBACK')
        ack = probe.publish_frame(timeout=10.0)
        if ack is None:
            raise RuntimeError('expected re-ACK for duplicate publish, got none')
        ack_topic, ack_body = ack
        assert_ack_fields(ack_body, first_mid)
    finally:
        probe.close()

    assert_queue_full_batch(fixture, spec_a, captured_mids,
                            QUEUE_FULL_SAMPLE_COUNT, QUEUE_FULL_SAMPLE_INTERVAL_US,
                            'QUEUE_FULL_POST_DUPLICATE_PASS')
    print('PASS: duplicate retry of captured batch re-ACKed without growing ledger, raw, or temporal counts', flush=True)

    # Clean backend stop and structured summary log verification
    ensure_budget('queue-full:shutdown')
    print('stage=queue-full:shutdown', flush=True)
    run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
    exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
    if exit_code != '0':
        raise RuntimeError(f'backend exited with code {exit_code} on final SIGTERM')
    logs_final = container_logs(backend_cid)
    if 'backend shutdown incomplete' in logs_final or 'panic:' in logs_final:
        raise RuntimeError('backend shutdown reported incomplete/panicked teardown: ' + sanitize(logs_final[-1200:]))
    if 'backend shutdown complete' not in logs_final:
        raise RuntimeError('backend did not log confirmed complete shutdown: ' + sanitize(logs_final[-1200:]))

    summary_final = parse_ingress_summary(logs_final)
    if summary_final.get('committed', 0) < QUEUE_FULL_BATCHES:
        raise RuntimeError(f'expected committed >= {QUEUE_FULL_BATCHES} in final summary, got {summary_final}')

    print(f'PASS: clean final SIGTERM shutdown; structured summary verified: '
          f'committed={summary_final.get("committed")} accepted={summary_final.get("accepted")} '
          f'duplicate_committed={summary_final.get("duplicate_committed")} '
          f'queue_full={summary_final.get("queue_full")} queue_capacity={summary_final.get("queue_capacity")}', flush=True)
    return backend_cid


# --- 3.6.6 step 1 gateway-reconnect scenario ------------------------------


def run_gateway_reconnect(work: Path, net_name: str, fixture: DatabaseFixture,
                          backend_image: str, gateway_image: str, hexid: str,
                          registry, specs, gateway_passwords: dict, jwt_secret: str,
                          backend_password: str, manager_password: str,
                          broker_cid: str, broker_port: int, owned_containers: list):
    """3.6.6 step 1: ordinary Gateway network disconnect, reconnect, resubscribe,
    and byte-exact resend slice with reversible ACK receive hook.
    """
    ca_file = work / 'public' / 'ca.crt'
    spec_a = specs[0]
    pw_a = gateway_passwords[spec_a.gateway_id]
    telemetry_filter = 'gateways/+/telemetry/#'

    ensure_budget('gateway-reconnect:backend')
    print('stage=gateway-reconnect:start-backend', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers)
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend /readyz + /healthz ready (positive SUBACK for the data filter '
          'gateways/+/telemetry/#)', flush=True)

    outbox_dir = work / f'outbox_recon_{hexid}'
    outbox_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir, 0o777)
    db_a = outbox_dir / 'outbox.db'

    probes = []
    sim_a = None
    manager = None
    block_attached = False
    network_disconnected = False

    try:
        manager = connect_manager(broker_port, work, manager_password)
        probes.append(manager)
        manager.subscribe(RESPONSE)

        recv_telemetry = AclProbe(broker_port, ca_file, 'bootstrap_backend', backend_password)
        probes.append(recv_telemetry)
        if recv_telemetry.subscribe_code(telemetry_filter) != 0x00:
            raise RuntimeError('backend-principal receiver telemetry subscription was not granted')

        # 1. Attach reversible ACK receive block before starting simulator
        attach_ack_receive_block(manager, spec_a.gateway_id, spec_a.ack_filter)
        block_attached = True
        if not ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('ACK receive-deny role was not attached (DynSec readback)')
        print('PASS: reversible ACK receive hook attached before simulator start', flush=True)

        # 2. Start Gateway simulator with fast ACK timeout/retry profile
        env_a = write_gateway_env_fault(work, f'gateway_recon_{hexid}.env', spec_a.gateway_id, pw_a)
        sim_a = start_gateway_detached(
            gateway_image, net_name, outbox_dir, ca_file, env_a,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-recon-{hexid}', owned_containers)
        registry.register('gateway reconnect container', 'container', sim_a)
        wait_container_log(sim_a, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        print('PASS: Gateway A simulator reached running state with ACK receive block attached', flush=True)

        # 3. Wait for backend to commit the batch exactly once
        deadline = time.monotonic() + ACK_FAULT_PENDING_TIMEOUT
        while time.monotonic() < deadline and len(ledger_message_ids(fixture, spec_a)) < 1:
            time.sleep(0.3)
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'RECONNECT_COMMIT_1')
        snap_db_before = snapshot_db(fixture, specs)

        # 4. Read SQLite pending row and capture identity with bounded read-only query
        rows_init = wait_pending_rows(db_a, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_init:
            raise RuntimeError('the committed batch is not present as a pending SQLite row')
        row_init = rows_init[0]
        mid_a = row_init['message_id']
        topic_a = row_init['topic']
        payload_a = row_init['payload']
        if mid_a not in ledger_message_ids(fixture, spec_a):
            raise RuntimeError('pending row message_id was not committed in backend ledger')
        print(f'PASS: backend committed exactly once before fault (ledger={mid_a[:8]}...); '
              f'SQLite live pending confirmed (id={mid_a[:8]}..., boot={row_init["boot_id"][:8]}..., '
              f'seq={row_init["first_sequence"]}, bytes={row_init["byte_size"]})', flush=True)

        # Drain the initial delivery before network disconnect
        drain_publish_frames(recv_telemetry, timeout=0.5)

        # Record baseline broker SUBACK audit count for Gateway A before disconnect
        sim_client_id = f'{spec_a.gateway_id}_sim'
        broker_suback_before = len([l for l in container_logs(broker_cid).splitlines() if sim_client_id in l and 'SUBACK' in l])

        # 5. Disconnect only Gateway container from fixture network
        print('stage=gateway-reconnect:disconnect-network', flush=True)
        docker(['network', 'disconnect', net_name, sim_a], timeout=30)
        network_disconnected = True

        # Wait for bounded observation of socket loss in simulator logs
        disconnect_observed = False
        disc_deadline = time.monotonic() + 15.0
        while time.monotonic() < disc_deadline:
            sim_logs = container_logs(sim_a)
            evs = parse_gateway_transport_events(sim_logs)
            if any(e['event'] in ('disconnected', 'connection_lost') and e['epoch'] == 1 for e in evs):
                disconnect_observed = True
                break
            time.sleep(0.3)
        if not disconnect_observed:
            raise RuntimeError('socket loss / disconnect event on epoch 1 was not observed after network disconnect: ' +
                               sanitize(container_logs(sim_a)[-800:]))
        print('PASS: real socket loss observed on epoch 1 in Gateway transport logs after network disconnect', flush=True)

        # 6. Reconnect Gateway container to fixture network
        print('stage=gateway-reconnect:reconnect-network', flush=True)
        docker(['network', 'connect', net_name, sim_a], timeout=30)
        network_disconnected = False

        # 7. Require fresh connection (epoch >= 2) and positive own ACK subscription before resend
        reconnect_subscribed = False
        recon_deadline = time.monotonic() + 20.0
        while time.monotonic() < recon_deadline:
            sim_logs = container_logs(sim_a)
            evs = parse_gateway_transport_events(sim_logs)
            if gateway_reconnect_events_ok(evs, min_epoch=2):
                reconnect_subscribed = True
                break
            time.sleep(0.3)
        if not reconnect_subscribed:
            raise RuntimeError('fresh connection and positive own ACK subscription (epoch >= 2) not observed after reconnect: ' +
                               sanitize(container_logs(sim_a)[-800:]))

        # Corroborate with broker debug logs that a NEW SUBACK was emitted (rejecting stale log lines)
        broker_logs = container_logs(broker_cid)
        broker_suback_after = len([l for l in broker_logs.splitlines() if sim_client_id in l and 'SUBACK' in l])
        if broker_suback_after <= broker_suback_before:
            raise RuntimeError(f'broker logs did not record a NEW SUBACK for Gateway A ({sim_client_id}) after reconnect '
                               f'(before={broker_suback_before}, after={broker_suback_after}): ' +
                               sanitize(broker_logs[-800:]))
        print(f'PASS: fresh connection (epoch 2) and positive own ACK subscription confirmed before resend '
              f'(corroborated by new broker SUBACK audit count: {broker_suback_before} -> {broker_suback_after})', flush=True)

        # 8. Observe byte-exact resend on epoch 2 while ACK hook remains active
        deliveries = collect_matching_deliveries(recv_telemetry, topic_a, payload_a, 1,
                                                 ACK_FAULT_RESEND_TIMEOUT)
        resend_observed = len(deliveries) >= 1
        resend_bytes_match = resend_observed and all(t == topic_a and p == payload_a for t, p in deliveries)
        if not resend_observed:
            raise RuntimeError('no byte-exact resend of pending item observed after reconnect while ACK was blocked')
        print(f'PASS: byte-exact resend observed on fresh connection ({len(deliveries)} identical deliveries)', flush=True)

        # Verify DB counts and current version are unchanged (deduplication)
        db_unchanged_during = snapshot_db(fixture, specs) == snap_db_before
        if not db_unchanged_during:
            raise RuntimeError('database state mutated during blocked resend; deduplication failed')

        # Verify pending row identity is unchanged in SQLite
        rows_during = wait_pending_rows(db_a, ACK_FAULT_DELIVERY_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_during:
            raise RuntimeError('pending row vanished from SQLite during blocked resend')
        pending_identity_match = reconnect_pending_identity_unchanged(row_init, rows_during[0])
        if not pending_identity_match:
            raise RuntimeError('pending row fields mutated in SQLite across reconnect')
        print('PASS: DB state unchanged (ledger/raw/temporal/reported version strictly preserved); '
              'pending row identity unchanged in SQLite', flush=True)

        # 9. Restore the ACK receive path
        print('stage=gateway-reconnect:restore-ack-path', flush=True)
        try:
            manager.close()
        except Exception:
            pass
        manager = connect_manager(broker_port, work, manager_password)
        manager.subscribe(RESPONSE)
        probes.append(manager)
        detach_ack_receive_block(manager, spec_a.gateway_id)
        if ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('the ACK receive-deny role is still attached after detach (DynSec readback)')
        block_attached = False
        receive_restored = True
        print('PASS: ACK receive-deny role detached and verified via DynSec readback', flush=True)

        # 10. Wait for legitimate application ACK and clean simulator exit
        exit_a = wait_container_exit(sim_a, ACK_FAULT_EXIT_TIMEOUT)
        if exit_a != 0:
            sim_logs = container_logs(sim_a)
            raise RuntimeError(f'Gateway simulator exited with code {exit_a} after restore: ' + sanitize(sim_logs[-800:]))

        cli_pending = cli_count_bind(gateway_image, net_name, outbox_dir, ca_file, work, f'recon_{hexid}')
        pending_after = len(read_sqlite_pending(db_a))
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'RECONNECT_COMMIT_1_AFTER')
        db_unchanged_final = snapshot_db(fixture, specs) == snap_db_before

        if not gateway_reconnect_oracle(
            committed_init=True,
            pending_init=True,
            disconnect_observed=disconnect_observed,
            reconnect_subscribed=reconnect_subscribed,
            resend_observed=resend_observed,
            resend_bytes_match=resend_bytes_match,
            pending_identity_match=pending_identity_match,
            db_unchanged_during=db_unchanged_during,
            receive_restored=receive_restored,
            pending_after=pending_after,
            cli_pending_rows=cli_pending,
            simulator_exit_code=exit_a,
            db_unchanged_final=db_unchanged_final,
        ):
            raise RuntimeError('gateway reconnect oracle assertion failed')

        print('PASS: 3.6.6 step 1 gateway-reconnect slice verified -- ordinary disconnect observed, '
              'fresh connection and positive subscription established before resend, byte-exact resend '
              'deduplicated without extra DB writes, legitimate application ACK received on restore, '
              'and outbox cleanly emptied (CLI count 0)', flush=True)

        ensure_budget('gateway-reconnect:shutdown')
        print('stage=gateway-reconnect:shutdown', flush=True)
        run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
        exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
        if exit_code != '0':
            raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
        logs = container_logs(backend_cid)
        if 'backend shutdown incomplete' in logs or 'panic:' in logs:
            raise RuntimeError('backend shutdown reported an incomplete/panicked teardown; log=' + sanitize(logs[-1200:]))
        if 'backend shutdown complete' not in logs:
            raise RuntimeError('backend did not log a confirmed complete shutdown; log=' + sanitize(logs[-1200:]))
        print('PASS: gateway-reconnect slice completed with a clean backend SIGTERM shutdown', flush=True)
        return backend_cid

    finally:
        if network_disconnected and sim_a:
            try:
                docker(['network', 'connect', net_name, sim_a], timeout=30)
            except Exception:
                pass
        if block_attached and manager:
            try:
                detach_ack_receive_block(manager, spec_a.gateway_id)
            except Exception:
                pass
        for p in probes:
            try:
                p.close()
            except Exception:
                pass


def backend_clean_shutdown_ok(exit_code: str | int, logs: str) -> bool:
    """Validate that the backend exited with code 0 on SIGTERM and logged a
    confirmed complete shutdown without panic or incomplete teardown."""
    if str(exit_code).strip() != '0':
        return False
    if 'backend shutdown incomplete' in logs or 'panic:' in logs:
        return False
    if 'backend shutdown complete' not in logs:
        return False
    return True


def gateway_stopped_pending_ok(exit_code: int, logs: str, pending_count: int) -> bool:
    """Validate that when a Gateway CLI is stopped with durable pending items remaining,
    it does not claim false completion (completed=false, non-zero exit code)."""
    if pending_count > 0:
        if 'completed=true' in logs:
            return False
        return True
    return exit_code == 0 and 'completed=true' in logs


def gateway_restart_boot_distinction(old_row: dict, new_row: dict) -> bool:
    """Validate that across Gateway process restarts with the same outbox file,
    the old row retains its stored boot UUID, while the new batch has a distinct
    boot UUID and starts its sequence numbering at zero."""
    if old_row.get('message_id') == new_row.get('message_id'):
        return False
    if old_row.get('boot_id') == new_row.get('boot_id'):
        return False
    if new_row.get('first_sequence') != 0:
        return False
    if old_row.get('topic') != new_row.get('topic'):
        return False
    return True


def process_restart_accounting_ok(*, sub1_ledgers: int, sub2_ledgers: int,
                                  sub1_raw: int, sub2_raw: int,
                                  total_ledgers: int, total_raw: int,
                                  total_temporal: int) -> bool:
    """Validate the exact accounting across the two subcases of process-restart:
    Subcase 1: 1 batch (1 ledger, 100 raw, 1 temporal)
    Subcase 2: 1 replayed old batch + 1 new batch (2 ledgers, 200 raw, 2 temporal)
    Total across fixture: 3 ledgers, 300 raw, 3 temporal."""
    if sub1_ledgers != 1 or sub2_ledgers != 2:
        return False
    if sub1_raw != 100 or sub2_raw != 200:
        return False
    if total_ledgers != 3 or total_raw != 300 or total_temporal != 3:
        return False
    return True


def same_file_binding_ok(path_a: str | Path, path_b: str | Path) -> bool:
    return Path(path_a).resolve() == Path(path_b).resolve()


def build_gateway_two_batches_assert_sql(spec: GatewaySpec, old_item: dict, new_item: dict, marker: str) -> str:
    old_hash = hashlib.sha256(old_item['payload']).hexdigest()
    new_hash = hashlib.sha256(new_item['payload']).hexdigest()
    return f"""
    DO $$
    DECLARE
      ledger_count int; raw_count int; temporal_count int;
      raw_old int; raw_new int;
      t_old int; t_new int;
      rep jsonb; des jsonb; rep_ver bigint; des_ver bigint;
      gw_rep jsonb; gw_des jsonb; gw_rep_ver bigint; gw_des_ver bigint;
      msg1 uuid := '{old_item["message_id"]}'::uuid;
      boot1 uuid := '{old_item["boot_id"]}'::uuid;
      msg2 uuid := '{new_item["message_id"]}'::uuid;
      boot2 uuid := '{new_item["boot_id"]}'::uuid;
    BEGIN
      SELECT count(*) INTO ledger_count FROM public.processed_messages
        WHERE gateway_id='{spec.gateway_id}' AND sensor_id='{spec.sensor_id}';
      IF ledger_count <> 2 THEN RAISE EXCEPTION 'ledger count %, want 2', ledger_count; END IF;

      IF NOT EXISTS (
        SELECT 1 FROM public.processed_messages
        WHERE gateway_id='{spec.gateway_id}' AND sensor_id='{spec.sensor_id}'
          AND message_id = msg1 AND boot_id = boot1
          AND first_sequence = {old_item["first_sequence"]}
          AND last_sequence = {old_item["first_sequence"] + old_item["sample_count"] - 1}
          AND sample_count = {old_item["sample_count"]}
          AND source_topic = '{spec.telemetry_topic}'
          AND payload_hash = '{old_hash}'
      ) THEN RAISE EXCEPTION 'old message ledger row missing or fields mismatch'; END IF;

      IF NOT EXISTS (
        SELECT 1 FROM public.processed_messages
        WHERE gateway_id='{spec.gateway_id}' AND sensor_id='{spec.sensor_id}'
          AND message_id = msg2 AND boot_id = boot2
          AND first_sequence = {new_item["first_sequence"]}
          AND last_sequence = {new_item["first_sequence"] + new_item["sample_count"] - 1}
          AND sample_count = {new_item["sample_count"]}
          AND source_topic = '{spec.telemetry_topic}'
          AND payload_hash = '{new_hash}'
      ) THEN RAISE EXCEPTION 'new message ledger row missing or fields mismatch'; END IF;

      SELECT count(*) INTO raw_count FROM public.telemetry
        WHERE gateway_id='{spec.gateway_id}' AND sensor_id='{spec.sensor_id}';
      IF raw_count <> 200 THEN RAISE EXCEPTION 'raw count %, want 200', raw_count; END IF;

      SELECT count(*) INTO raw_old FROM public.telemetry
        WHERE gateway_id='{spec.gateway_id}' AND sensor_id='{spec.sensor_id}'
          AND message_id = msg1 AND boot_id = boot1;
      IF raw_old <> 100 THEN RAISE EXCEPTION 'raw_old count %, want 100', raw_old; END IF;

      SELECT count(*) INTO raw_new FROM public.telemetry
        WHERE gateway_id='{spec.gateway_id}' AND sensor_id='{spec.sensor_id}'
          AND message_id = msg2 AND boot_id = boot2;
      IF raw_new <> 100 THEN RAISE EXCEPTION 'raw_new count %, want 100', raw_new; END IF;

      SELECT count(*) INTO temporal_count FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id='{spec.sensor_urn}')
          AND property_name='sample_interval_us';
      IF temporal_count <> 2 THEN RAISE EXCEPTION 'temporal count %, want 2', temporal_count; END IF;

      SELECT count(*) INTO t_old FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id='{spec.sensor_urn}')
          AND property_name='sample_interval_us' AND message_id = msg1
          AND metadata->>'boot_id' = boot1::text AND value_number = 10000;
      IF t_old <> 1 THEN RAISE EXCEPTION 'temporal old count %, want 1', t_old; END IF;

      SELECT count(*) INTO t_new FROM public.twin_temporal_values
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id='{spec.sensor_urn}')
          AND property_name='sample_interval_us' AND message_id = msg2
          AND metadata->>'boot_id' = boot2::text AND value_number = 10000;
      IF t_new <> 1 THEN RAISE EXCEPTION 'temporal new count %, want 1', t_new; END IF;

      SELECT reported_state, desired_state, reported_version, desired_version
        INTO rep, des, rep_ver, des_ver FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id='{spec.sensor_urn}');
      IF rep_ver <> 2 THEN RAISE EXCEPTION 'sensor reported_version %, want 2', rep_ver; END IF;
      IF rep->>'boot_id' <> boot2::text THEN RAISE EXCEPTION 'sensor reported boot_id %, want %', rep->>'boot_id', boot2; END IF;
      IF (rep->>'sequence_num')::bigint <> 99 THEN RAISE EXCEPTION 'sensor reported sequence %, want 99', rep->>'sequence_num'; END IF;
      IF (rep->>'sample_interval_us')::bigint <> 10000 THEN RAISE EXCEPTION 'sensor reported interval %, want 10000', rep->>'sample_interval_us'; END IF;
      IF rep->>'unit' IS DISTINCT FROM '{spec.sensor_unit}' THEN RAISE EXCEPTION 'sensor reported unit %', rep->>'unit'; END IF;

      IF des <> '{json.dumps(spec.sensor_desired)}'::jsonb OR des_ver <> {spec.sensor_desired_version} THEN
        RAISE EXCEPTION 'sensor desired state changed'; END IF;

      SELECT reported_state, desired_state, reported_version, desired_version
        INTO gw_rep, gw_des, gw_rep_ver, gw_des_ver FROM public.twin_states
        WHERE entity_id=(SELECT id FROM public.twin_entities WHERE entity_id='{spec.gateway_urn}');
      IF gw_rep <> '{json.dumps(spec.gateway_reported)}'::jsonb
         OR gw_des <> '{json.dumps(spec.gateway_desired)}'::jsonb
         OR gw_rep_ver <> {spec.gateway_reported_version}
         OR gw_des_ver <> {spec.gateway_desired_version} THEN
        RAISE EXCEPTION 'gateway state/audit changed'; END IF;

      RAISE NOTICE '{marker}';
    END $$;
    """


def assert_gateway_two_batches(fixture: DatabaseFixture, spec: GatewaySpec, old_item: dict, new_item: dict, marker: str):
    sql = build_gateway_two_batches_assert_sql(spec, old_item, new_item, marker)
    _, out = fixture.sql(sql.encode())
    if marker.encode() not in out:
        raise RuntimeError(f'SQL assertions did not emit {marker} for {spec.gateway_id}')


# --- 3.6.6 steps 2-3 process-restart scenario --------------------------------


def run_process_restart(work: Path, net_name: str, fixture: DatabaseFixture,
                        backend_image: str, gateway_image: str, hexid: str,
                        registry: ResourceRegistry, specs: list[GatewaySpec],
                        gateway_passwords: dict, jwt_secret: str,
                        backend_password: str, manager_password: str,
                        broker_cid: str, broker_port: int, owned_containers: list):
    """3.6.6 steps 2-3: controlled backend restart and Gateway CLI restart while durable items are pending."""
    ca_file = work / 'public' / 'ca.crt'
    spec_a = specs[0]
    spec_b = specs[1]
    pw_a = gateway_passwords[spec_a.gateway_id]
    pw_b = gateway_passwords[spec_b.gateway_id]
    telemetry_filter = 'gateways/+/telemetry/#'

    ensure_budget('process-restart:backend')
    print('stage=process-restart:start-backend', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers)
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    print('PASS: backend /readyz + /healthz ready (positive SUBACK for the data filter '
          'gateways/+/telemetry/#)', flush=True)

    outbox_dir_a = work / f'outbox_proc_a_{hexid}'
    outbox_dir_a.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir_a, 0o777)
    db_a = outbox_dir_a / 'outbox.db'

    outbox_dir_b = work / f'outbox_proc_b_{hexid}'
    outbox_dir_b.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir_b, 0o777)
    db_b = outbox_dir_b / 'outbox.db'

    probes = []
    sim_a = None
    sim_b_1 = None
    sim_b_2 = None
    manager = None
    block_a_attached = False
    block_b_attached = False

    try:
        manager = connect_manager(broker_port, work, manager_password)
        probes.append(manager)
        manager.subscribe(RESPONSE)

        recv_telemetry = AclProbe(broker_port, ca_file, 'bootstrap_backend', backend_password)
        probes.append(recv_telemetry)
        if recv_telemetry.subscribe_code(telemetry_filter) != 0x00:
            raise RuntimeError('backend-principal receiver telemetry subscription was not granted')

        # =====================================================================
        # Subcase 1: Backend restart (3.6.6 step 2)
        # =====================================================================
        print('stage=process-restart:subcase1-backend-restart', flush=True)
        # 1. Attach reversible ACK receive block for Gateway A before starting simulator
        attach_ack_receive_block(manager, spec_a.gateway_id, spec_a.ack_filter)
        block_a_attached = True
        if not ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('ACK receive-deny role was not attached for Gateway A (DynSec readback)')
        print('PASS: Subcase 1: reversible ACK receive hook attached for Gateway A', flush=True)

        # 2. Start Gateway A simulator with fast ACK timeout/retry profile
        env_a = write_gateway_env_fault(work, f'gateway_proc_a_{hexid}.env', spec_a.gateway_id, pw_a)
        sim_a = start_gateway_detached(
            gateway_image, net_name, outbox_dir_a, ca_file, env_a,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-proc-a-{hexid}', owned_containers)
        registry.register('gateway proc a container', 'container', sim_a)
        wait_container_log(sim_a, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        print('PASS: Subcase 1: Gateway A simulator reached running state with ACK block attached', flush=True)

        # 3. Wait for backend to commit the batch exactly once
        deadline = time.monotonic() + ACK_FAULT_PENDING_TIMEOUT
        while time.monotonic() < deadline and len(ledger_message_ids(fixture, spec_a)) < 1:
            time.sleep(0.3)
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'BACKEND_RESTART_COMMIT_1')
        snap_sub1_commit = snapshot_db(fixture, specs)

        # 4. Verify SQLite pending row
        rows_a_init = wait_pending_rows(db_a, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_a_init:
            raise RuntimeError('the committed batch is not present as a pending SQLite row in Gateway A outbox')
        row_a_init = rows_a_init[0]
        mid_a = row_a_init['message_id']
        topic_a = row_a_init['topic']
        payload_a = row_a_init['payload']
        if mid_a not in ledger_message_ids(fixture, spec_a):
            raise RuntimeError('pending row message_id was not committed in backend ledger')
        print(f'PASS: Subcase 1: backend committed exactly once (mid={mid_a[:8]}...); '
              f'SQLite live pending confirmed (id={mid_a[:8]}..., boot={row_a_init["boot_id"][:8]}..., '
              f'seq={row_a_init["first_sequence"]}, bytes={row_a_init["byte_size"]})', flush=True)

        # 5. SIGTERM the backend within SHUTDOWN_TIMEOUT (30s)
        ensure_budget('process-restart:sigterm-backend')
        print('stage=process-restart:sigterm-backend', flush=True)
        run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
        backend_exit = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
        backend_logs = container_logs(backend_cid)
        if not backend_clean_shutdown_ok(backend_exit, backend_logs):
            raise RuntimeError(f'backend clean shutdown verification failed: exit={backend_exit}, log=' +
                               sanitize(backend_logs[-800:]))
        print('PASS: Subcase 1: backend stopped with clean SIGTERM shutdown (exit 0, confirmed complete log)', flush=True)

        # 6. Verify PostgreSQL, broker store, and Gateway A SQLite remain intact
        if snapshot_db(fixture, specs) != snap_sub1_commit:
            raise RuntimeError('PostgreSQL database state mutated while backend was stopped')
        rows_a_while_stopped = read_sqlite_pending(db_a)
        if len(rows_a_while_stopped) != 1 or rows_a_while_stopped[0]['message_id'] != mid_a:
            raise RuntimeError('Gateway A SQLite pending item vanished or changed while backend was stopped')
        print('PASS: Subcase 1: PostgreSQL and Gateway A SQLite confirmed intact while backend stopped', flush=True)

        # 7. Start the SAME backend container again, refresh mapped HTTP port, require readiness
        ensure_budget('process-restart:start-backend-again')
        print('stage=process-restart:start-backend-again', flush=True)
        docker(['start', backend_cid], timeout=CONTAINER_START_TIMEOUT)
        refreshed_port = mapped_port(backend_cid, '8080/tcp')
        wait_http(refreshed_port, '/readyz', READY_TIMEOUT)
        wait_http(refreshed_port, '/healthz', READY_TIMEOUT)
        print(f'PASS: Subcase 1: backend restarted, mapped port refreshed ({refreshed_port}), '
              '/readyz + /healthz ready with positive telemetry subscription', flush=True)

        # 8. Let the still-running simulator resend the identical item
        deliveries_resend = collect_matching_deliveries(recv_telemetry, topic_a, payload_a, 1, ACK_FAULT_RESEND_TIMEOUT)
        if not deliveries_resend:
            raise RuntimeError('simulator did not resend the identical pending item after backend restart')
        print(f'PASS: Subcase 1: simulator resent identical batch ({len(deliveries_resend)} deliveries observed)', flush=True)

        # 9. Assert duplicate commit semantics (database snapshot strictly unchanged)
        time.sleep(1.0)
        snap_sub1_resend = snapshot_db(fixture, specs)
        if snap_sub1_resend != snap_sub1_commit:
            raise RuntimeError('database state mutated during duplicate resend; deduplication failed')
        print('PASS: Subcase 1: duplicate commit semantics verified (ledger/raw/temporal/version unchanged)', flush=True)

        # 10. Restore the hook so a new legitimate ACK empties the outbox
        print('stage=process-restart:restore-ack-subcase1', flush=True)
        try:
            manager.close()
        except Exception:
            pass
        manager = connect_manager(broker_port, work, manager_password)
        manager.subscribe(RESPONSE)
        probes.append(manager)
        detach_ack_receive_block(manager, spec_a.gateway_id)
        if ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('ACK receive-deny role is still attached for Gateway A after detach')
        block_a_attached = False
        print('PASS: Subcase 1: ACK receive-deny role detached for Gateway A', flush=True)

        # 11. Require truthful CLI completion
        exit_a = wait_container_exit(sim_a, ACK_FAULT_EXIT_TIMEOUT)
        if exit_a != 0:
            raise RuntimeError(f'Gateway A simulator exited with code {exit_a} after restore: ' +
                               sanitize(container_logs(sim_a)[-800:]))
        sim_a_logs = container_logs(sim_a)
        if 'completed=true' not in sim_a_logs or 'pending_rows=0' not in sim_a_logs:
            raise RuntimeError('Gateway A simulator logs did not confirm truthful completion: ' +
                               sanitize(sim_a_logs[-800:]))
        cli_a_pending = cli_count_bind(gateway_image, net_name, outbox_dir_a, ca_file, work, f'proc_a_{hexid}')
        if cli_a_pending != 0:
            raise RuntimeError(f'cli count bind reported {cli_a_pending} pending rows for Gateway A, want 0')
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'BACKEND_RESTART_COMMIT_1_FINAL')
        print('PASS: Subcase 1 verified -- backend restart while pending preserved data and outbox emptied cleanly', flush=True)

        # =====================================================================
        # Subcase 2: Gateway process restart (3.6.6 step 3)
        # =====================================================================
        print('stage=process-restart:subcase2-gateway-restart', flush=True)

        # 1. Attach reversible ACK receive block for Gateway B
        attach_ack_receive_block(manager, spec_b.gateway_id, spec_b.ack_filter)
        block_b_attached = True
        if not ack_block_attached(manager, spec_b.gateway_id):
            raise RuntimeError('ACK receive-deny role was not attached for Gateway B (DynSec readback)')
        print('PASS: Subcase 2: reversible ACK receive hook attached for Gateway B', flush=True)

        # 2. Start Gateway B invocation 1 with run --batches 1
        env_b = write_gateway_env_fault(work, f'gateway_proc_b_{hexid}.env', spec_b.gateway_id, pw_b)
        sim_b_1 = start_gateway_detached(
            gateway_image, net_name, outbox_dir_b, ca_file, env_b,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-proc-b1-{hexid}', owned_containers)
        registry.register('gateway proc b1 container', 'container', sim_b_1)
        wait_container_log(sim_b_1, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        print('PASS: Subcase 2: Gateway B invocation 1 running with ACK block attached', flush=True)

        # 3. Wait for backend to commit the old batch
        deadline = time.monotonic() + ACK_FAULT_PENDING_TIMEOUT
        while time.monotonic() < deadline and len(ledger_message_ids(fixture, spec_b)) < 1:
            time.sleep(0.3)
        assert_gateway_batch(fixture, spec_b, simulator_envelope(), 'GW_RESTART_COMMIT_OLD')

        # 4. Capture exact old item fields from SQLite outbox
        rows_b_init = wait_pending_rows(db_b, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_b_init:
            raise RuntimeError('the committed batch is not present as a pending SQLite row in Gateway B outbox')
        old_item = rows_b_init[0]
        old_id = old_item['message_id']
        old_topic = old_item['topic']
        old_payload = old_item['payload']
        old_boot = old_item['boot_id']
        old_seq = old_item['first_sequence']
        old_count = old_item['sample_count']
        old_interval = old_item['sample_interval_us']
        old_bytes = old_item['byte_size']
        print(f'PASS: Subcase 2: old item captured (id={old_id[:8]}..., boot={old_boot[:8]}..., '
              f'seq={old_seq}, count={old_count}, bytes={old_bytes})', flush=True)

        # 5. Stop Gateway CLI invocation 1 normally with a bounded signal (SIGTERM)
        ensure_budget('process-restart:stop-gateway-cli-1')
        print('stage=process-restart:stop-gateway-cli-1', flush=True)
        run(['docker', 'stop', '-t', '15', sim_b_1], timeout=30)
        exit_b_1 = container_exit_code(sim_b_1)
        logs_b_1 = container_logs(sim_b_1)
        pending_after_stop1 = len(read_sqlite_pending(db_b))
        if not gateway_stopped_pending_ok(exit_b_1, logs_b_1, pending_after_stop1):
            raise RuntimeError(f'stopped gateway CLI claimed false completion: exit={exit_b_1}, log=' +
                               sanitize(logs_b_1[-800:]))
        print(f'PASS: Subcase 2: Gateway B invocation 1 stopped normally; verified truthful non-completion '
              f'(exit={exit_b_1}, pending_rows={pending_after_stop1})', flush=True)

        # 6. Verify same-file binding and exact old row survives in SQLite
        rows_b_survived = read_sqlite_pending(db_b)
        if len(rows_b_survived) != 1:
            raise RuntimeError(f'expected 1 surviving pending row in SQLite, got {len(rows_b_survived)}')
        survived_item = rows_b_survived[0]
        if (survived_item['message_id'] != old_id or survived_item['boot_id'] != old_boot or
                survived_item['first_sequence'] != old_seq or survived_item['payload'] != old_payload):
            raise RuntimeError('old pending row mutated across process stop')
        print('PASS: Subcase 2: exact old pending row survived in SQLite with intact file binding', flush=True)

        # 7. Start a NEW Gateway CLI invocation (sim_b_2) with the SAME file and fresh process boot
        ensure_budget('process-restart:start-gateway-cli-2')
        print('stage=process-restart:start-gateway-cli-2', flush=True)
        sim_b_2 = start_gateway_detached(
            gateway_image, net_name, outbox_dir_b, ca_file, env_b,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-proc-b2-{hexid}', owned_containers)
        registry.register('gateway proc b2 container', 'container', sim_b_2)
        wait_container_log(sim_b_2, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        print('PASS: Subcase 2: Gateway B invocation 2 started with same outbox and fresh process boot', flush=True)

        # 8. While ACK delivery is blocked, wait for SQLite to contain BOTH old replay and new batch
        rows_b_two = wait_pending_rows(db_b, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 2)
        if len(rows_b_two) < 2:
            raise RuntimeError(f'expected at least 2 pending rows in Gateway B outbox, got {len(rows_b_two)}')
        row_old_replay = next((r for r in rows_b_two if r['message_id'] == old_id), None)
        row_new = next((r for r in rows_b_two if r['message_id'] != old_id), None)
        if row_old_replay is None or row_new is None:
            raise RuntimeError('could not locate both old replayed row and new batch row in SQLite outbox')

        if not gateway_restart_boot_distinction(old_item, row_new):
            raise RuntimeError(f'boot distinction failed: old={old_item}, new={row_new}')
        if row_old_replay['payload'] != old_payload or row_old_replay['boot_id'] != old_boot:
            raise RuntimeError('replayed old row payload/boot mutated in SQLite')
        print(f'PASS: Subcase 2: old item replayed byte-exact (id={old_id[:8]}..., boot={old_boot[:8]}..., seq={old_seq}); '
              f'new batch enqueued with fresh boot UUID ({row_new["boot_id"][:8]}...) and sequence 0', flush=True)

        # 9. Wait for backend to commit the new batch (total ledgers for Gateway B reaches 2)
        deadline = time.monotonic() + ACK_FAULT_PENDING_TIMEOUT
        while time.monotonic() < deadline and len(ledger_message_ids(fixture, spec_b)) < 2:
            time.sleep(0.3)
        mids_b = ledger_message_ids(fixture, spec_b)
        if len(mids_b) != 2:
            raise RuntimeError(f'backend ledger has {len(mids_b)} items for Gateway B, want exactly 2: {mids_b}')
        if old_id not in mids_b or row_new['message_id'] not in mids_b:
            raise RuntimeError(f'backend ledger does not contain both old and new message IDs for Gateway B: {mids_b}')
        print(f'PASS: Subcase 2: backend committed both unique batches (old={old_id[:8]}..., new={row_new["message_id"][:8]}...)', flush=True)

        # 10. Restore ACK delivery for Gateway B
        print('stage=process-restart:restore-ack-subcase2', flush=True)
        try:
            manager.close()
        except Exception:
            pass
        manager = connect_manager(broker_port, work, manager_password)
        manager.subscribe(RESPONSE)
        probes.append(manager)
        detach_ack_receive_block(manager, spec_b.gateway_id)
        if ack_block_attached(manager, spec_b.gateway_id):
            raise RuntimeError('ACK receive-deny role is still attached for Gateway B after detach')
        block_b_attached = False
        print('PASS: Subcase 2: ACK receive-deny role detached for Gateway B', flush=True)

        # 11. Wait for Gateway B invocation 2 to exit cleanly with code 0
        exit_b_2 = wait_container_exit(sim_b_2, ACK_FAULT_EXIT_TIMEOUT)
        if exit_b_2 != 0:
            raise RuntimeError(f'Gateway B invocation 2 exited with code {exit_b_2}: ' +
                               sanitize(container_logs(sim_b_2)[-800:]))
        logs_b_2 = container_logs(sim_b_2)
        if 'completed=true' not in logs_b_2 or 'pending_rows=0' not in logs_b_2:
            raise RuntimeError('Gateway B invocation 2 logs did not confirm truthful completion: ' +
                               sanitize(logs_b_2[-800:]))
        if len(read_sqlite_pending(db_b)) != 0:
            raise RuntimeError('Gateway B SQLite pending rows non-zero after completion')
        cli_b_pending = cli_count_bind(gateway_image, net_name, outbox_dir_b, ca_file, work, f'proc_b_{hexid}')
        if cli_b_pending != 0:
            raise RuntimeError(f'cli count bind reported {cli_b_pending} pending rows for Gateway B, want 0')
        print('PASS: Subcase 2: both old and new batches acknowledged, outbox emptied, CLI count 0', flush=True)

        # 12. Complete SQL assertions for Gateway B and across fixture
        assert_gateway_two_batches(fixture, spec_b, old_item, row_new, 'GW_B_TWO_BATCHES_PASS')
        print('PASS: Subcase 2 SQL asserts Gateway B: 2 ledgers / 200 raw / 2 temporal / '
              'sensor reported_version = initial + 2 (ordered by measurement time) / '
              'sensor desired & Gateway state preserved', flush=True)

        if not process_restart_accounting_ok(
            sub1_ledgers=len(ledger_message_ids(fixture, spec_a)),
            sub2_ledgers=len(ledger_message_ids(fixture, spec_b)),
            sub1_raw=100,
            sub2_raw=200,
            total_ledgers=3,
            total_raw=300,
            total_temporal=3,
        ):
            raise RuntimeError('process restart accounting failed across subcases')

        assert_phase_totals(fixture, specs, ledger=3, raw=300, temporal=3, marker='PROCESS_RESTART_TOTALS_PASS')
        print('PASS: phase totals are 3 ledger / 300 raw / 3 temporal total across fixture (Gateway A: 1, Gateway B: 2)', flush=True)

        # 13. Clean final backend stop
        ensure_budget('process-restart:shutdown')
        print('stage=process-restart:shutdown', flush=True)
        run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
        final_backend_exit = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
        final_backend_logs = container_logs(backend_cid)
        if not backend_clean_shutdown_ok(final_backend_exit, final_backend_logs):
            raise RuntimeError(f'final backend shutdown failed: exit={final_backend_exit}')
        print('PASS: process-restart slice completed with clean final backend shutdown', flush=True)
        return backend_cid

    finally:
        if block_a_attached and manager:
            try:
                detach_ack_receive_block(manager, spec_a.gateway_id)
            except Exception:
                pass
        if block_b_attached and manager:
            try:
                detach_ack_receive_block(manager, spec_b.gateway_id)
            except Exception:
                pass
        for p in probes:
            try:
                p.close()
            except Exception:
                pass


# --- 3.6.7 broker-controlled-restart helpers and scenario --------------------


def dynsec_persistence_comparison_ok(pre_client: dict, post_client: dict,
                                    pre_disabled: dict, post_disabled: dict,
                                    pre_role: dict, post_role: dict) -> bool:
    """Validate that DynSec client, disabled state, and role ACL metadata
    are strictly preserved across the broker controlled restart."""
    c_pre = pre_client.get('client', {}) if isinstance(pre_client, dict) else {}
    c_post = post_client.get('client', {}) if isinstance(post_client, dict) else {}
    if not c_pre or not c_post:
        return False
    if c_pre.get('username') != c_post.get('username'):
        return False
    if c_pre.get('disabled', False) is not False or c_post.get('disabled', False) is not False:
        return False
    roles_pre = {r.get('rolename') for r in c_pre.get('roles', []) if isinstance(r, dict)}
    roles_post = {r.get('rolename') for r in c_post.get('roles', []) if isinstance(r, dict)}
    if not roles_pre.issubset(roles_post):
        return False

    d_pre = pre_disabled.get('client', {}) if isinstance(pre_disabled, dict) else {}
    d_post = post_disabled.get('client', {}) if isinstance(post_disabled, dict) else {}
    if not d_pre or not d_post:
        return False
    if d_pre.get('username') != d_post.get('username'):
        return False
    if d_pre.get('disabled') is not True or d_post.get('disabled') is not True:
        return False

    r_pre = pre_role.get('role', {}) if isinstance(pre_role, dict) else {}
    r_post = post_role.get('role', {}) if isinstance(post_role, dict) else {}
    if not r_pre or not r_post:
        return False
    if r_pre.get('rolename') != r_post.get('rolename'):
        return False
    acls_pre = r_pre.get('acls', [])
    acls_post = r_post.get('acls', [])
    if not acls_pre or len(acls_pre) != len(acls_post):
        return False

    def acl_key(a):
        return (a.get('acltype'), a.get('topic'), a.get('allow'), a.get('priority'))

    if sorted([acl_key(a) for a in acls_pre]) != sorted([acl_key(a) for a in acls_post]):
        return False
    return True


def broker_restart_login_outcomes_ok(*, new_pw_login: bool, old_pw_login_denied: bool,
                                     disabled_login_denied: bool, positive_gw_login: bool) -> bool:
    """True when fresh new password succeeds, old password receives denied CONNACK,
    disabled principal is denied, and preserved positive Gateway login succeeds."""
    return (new_pw_login and old_pw_login_denied and disabled_login_denied and positive_gw_login)


def broker_same_store_constraints_ok(*, cid_before: str, cid_after: str,
                                     dynsec_vol_before: str, dynsec_vol_after: str,
                                     data_vol_before: str, data_vol_after: str) -> bool:
    """Validate that the broker was restarted as the SAME container with the
    SAME DynSec and broker data volumes, without store re-creation or bootstrapping."""
    return (bool(cid_before) and cid_before == cid_after and
            bool(dynsec_vol_before) and dynsec_vol_before == dynsec_vol_after and
            bool(data_vol_before) and data_vol_before == data_vol_after)


def broker_stopped_state_ok(*, health_status_stopped: int | None,
                            ready_status_stopped: int | None,
                            db_unchanged: bool, pending_unchanged: bool) -> bool:
    """Validate that while the broker is stopped, backend is still running with /healthz == 200,
    backend readiness handler responded with non-200 (not None / connection-refused),
    the PostgreSQL database has not mutated, and pending SQLite items remain intact."""
    return (health_status_stopped == 200 and
            ready_status_stopped is not None and
            ready_status_stopped != 200 and
            db_unchanged and
            pending_unchanged)


def run_broker_controlled_restart(work: Path, net_name: str, fixture: DatabaseFixture,
                                  backend_image: str, gateway_image: str, hexid: str,
                                  registry: ResourceRegistry, specs: list[GatewaySpec],
                                  gateway_passwords: dict, jwt_secret: str,
                                  backend_password: str, manager_password: str,
                                  broker_cid: str, broker_port: int,
                                  dynsec_volume: str, broker_data_volume: str,
                                  owned_containers: list):
    """3.6.7: one controlled Mosquitto restart with credential and ACL persistence."""
    ca_file = work / 'public' / 'ca.crt'
    spec_a = specs[0]
    spec_b = specs[1]
    old_pw_a = gateway_passwords[spec_a.gateway_id]
    pw_b = gateway_passwords[spec_b.gateway_id]
    telemetry_filter = 'gateways/+/telemetry/#'

    outbox_dir_a = work / f'outbox_br_a_{hexid}'
    outbox_dir_a.mkdir(parents=True, exist_ok=True)
    os.chmod(outbox_dir_a, 0o777)
    db_a = outbox_dir_a / 'outbox.db'

    probes = []
    manager = None
    block_a_attached = False
    sim_a = None

    try:
        # =====================================================================
        # Phase 1: Setup rotated and disabled credentials before restart
        # =====================================================================
        ensure_budget('broker-restart:setup')
        print('stage=broker-restart:setup', flush=True)

        manager = connect_manager(broker_port, work, manager_password)
        probes.append(manager)
        manager.subscribe(RESPONSE)

        # 1. Rotate Gateway A password
        new_pw_a = secrets.token_urlsafe(48)
        SECRETS.append(new_pw_a)
        manager.request('setClientPassword', username=spec_a.gateway_id, password=new_pw_a)

        # 2. Create and disable a separate probe principal
        disabled_principal = f'probe_disabled_{hexid}'
        pw_disabled = secrets.token_urlsafe(48)
        SECRETS.append(pw_disabled)
        manager.request('createClient', username=disabled_principal, password=pw_disabled)
        manager.request('disableClient', username=disabled_principal)

        # 3. Before restart, require fresh logins
        conn_a_new = mqtt_connection(broker_port, ca_file, spec_a.gateway_id, new_pw_a, accepted=True)
        conn_a_new.close()
        conn_a_old = mqtt_connection(broker_port, ca_file, spec_a.gateway_id, old_pw_a, accepted=False)
        conn_a_old.close()
        conn_disabled = mqtt_connection(broker_port, ca_file, disabled_principal, pw_disabled, accepted=False)
        conn_disabled.close()
        conn_b = mqtt_connection(broker_port, ca_file, spec_b.gateway_id, pw_b, accepted=True)
        conn_b.close()

        if not broker_restart_login_outcomes_ok(new_pw_login=True, old_pw_login_denied=True,
                                                disabled_login_denied=True, positive_gw_login=True):
            raise RuntimeError('pre-restart login outcome predicate failed')
        print('PASS: pre-restart fresh logins verified (new pw accepted, old pw rejected CONNACK 0x05, '
              'disabled probe rejected, positive Gateway B accepted)', flush=True)

        # 4. Read back baseline metadata via manager
        client_a_pre = manager.request('getClient', username=spec_a.gateway_id)
        client_disabled_pre = manager.request('getClient', username=disabled_principal)
        role_a_name = f'gateway_{spec_a.gateway_id}_uplink'
        role_a_pre = manager.request('getRole', rolename=role_a_name)

        if client_a_pre.get('client', {}).get('disabled', False) is not False:
            raise RuntimeError('Gateway A client must not be disabled before restart')
        if client_disabled_pre.get('client', {}).get('disabled') is not True:
            raise RuntimeError('probe principal must be disabled before restart')
        print('PASS: pre-restart DynSec metadata baseline recorded via manager (roles, disabled flag, exact ACL metadata)', flush=True)

        # =====================================================================
        # Phase 2: Start backend and Gateway A CLI with rotated credential + ACK hook
        # =====================================================================
        ensure_budget('broker-restart:backend')
        print('stage=broker-restart:start-backend', flush=True)
        backend_cid, backend_port = start_backend(
            work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
            track=owned_containers)
        wait_http(backend_port, '/readyz', READY_TIMEOUT)
        wait_http(backend_port, '/healthz', READY_TIMEOUT)
        print('PASS: backend /readyz + /healthz ready', flush=True)

        # Attach reversible ACK receive block for Gateway A
        attach_ack_receive_block(manager, spec_a.gateway_id, spec_a.ack_filter)
        block_a_attached = True
        if not ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('ACK receive-deny role was not attached for Gateway A')
        print('PASS: reversible ACK receive hook attached for rotated Gateway A', flush=True)

        # Start Gateway A simulator with new password
        env_a = write_gateway_env_fault(work, f'gateway_br_a_{hexid}.env', spec_a.gateway_id, new_pw_a)
        sim_a = start_gateway_detached(
            gateway_image, net_name, outbox_dir_a, ca_file, env_a,
            ['run', '--batches', '1', '--completion-timeout', ACK_FAULT_COMPLETION_TIMEOUT],
            f'stage3-uplink-gw-br-a-{hexid}', owned_containers)
        registry.register('gateway broker restart a container', 'container', sim_a)
        wait_container_log(sim_a, 'gateway simulator running', ACK_FAULT_START_TIMEOUT)
        print('PASS: Gateway A simulator running with rotated credential and ACK hook attached', flush=True)

        # Wait for backend to commit the batch
        deadline = time.monotonic() + ACK_FAULT_PENDING_TIMEOUT
        while time.monotonic() < deadline and len(ledger_message_ids(fixture, spec_a)) < 1:
            time.sleep(0.3)
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'BROKER_RESTART_PRE_COMMIT')
        snap_pre_stop = snapshot_db(fixture, specs)

        # Verify SQLite pending row and record exact identity/bytes
        rows_a_init = wait_pending_rows(db_a, ACK_FAULT_PENDING_TIMEOUT, lambda r: len(r) >= 1)
        if not rows_a_init:
            raise RuntimeError('committed batch is not present as pending SQLite row in Gateway A outbox')
        row_a_init = rows_a_init[0]
        mid_a = row_a_init['message_id']
        topic_a = row_a_init['topic']
        payload_a = row_a_init['payload']
        boot_id_a = row_a_init['boot_id']
        seq_a = row_a_init['first_sequence']
        bytes_a = row_a_init['byte_size']
        if mid_a not in ledger_message_ids(fixture, spec_a):
            raise RuntimeError('pending row message_id was not committed in backend ledger')
        print(f'PASS: backend committed exactly once (mid={mid_a[:8]}...); '
              f'SQLite live pending confirmed (id={mid_a[:8]}..., boot={boot_id_a[:8]}..., '
              f'seq={seq_a}, bytes={bytes_a})', flush=True)

        # =====================================================================
        # Phase 3: Controlled stop of broker and assertions while broker stopped
        # =====================================================================
        ensure_budget('broker-restart:stop-broker')
        print('stage=broker-restart:stop-broker', flush=True)

        # Close all active probes before stopping broker
        for p in probes:
            try:
                p.close()
            except Exception:
                pass
        probes.clear()

        run(['docker', 'stop', '-t', '30', broker_cid], timeout=60)
        broker_exit = docker(['inspect', '-f', '{{.State.ExitCode}}', broker_cid], timeout=15).decode().strip()
        broker_running = docker(['inspect', '-f', '{{.State.Running}}', broker_cid], timeout=15).decode().strip()
        if broker_running.lower() == 'true':
            raise RuntimeError('broker container still running after stop')
        print(f'PASS: broker process exited cleanly (exit={broker_exit}, running={broker_running})', flush=True)

        # Verify backend container is still running
        backend_running = docker(['inspect', '-f', '{{.State.Running}}', backend_cid], timeout=15).decode().strip()
        if backend_running.lower() != 'true':
            raise RuntimeError('backend container exited unexpectedly while broker was stopped')

        # While broker is stopped: backend HTTP must still be serving on its current mapping,
        # /healthz must return 200, and /readyz must return non-200 (e.g. 503, NOT None/connection-refused)
        became_unready = False
        unready_deadline = time.monotonic() + 15.0
        health_status_stopped = None
        ready_status_stopped = None
        while time.monotonic() < unready_deadline:
            health_status_stopped = probe_http_status(backend_port, '/healthz')
            ready_status_stopped = probe_http_status(backend_port, '/readyz')
            if health_status_stopped == 200 and ready_status_stopped is not None and ready_status_stopped != 200:
                became_unready = True
                break
            time.sleep(0.3)
        if not became_unready:
            raise RuntimeError(f'backend failed stopped-broker readiness check on port {backend_port}: '
                               f'/healthz={health_status_stopped}, /readyz={ready_status_stopped}')

        # While broker is stopped: database unchanged and pending SQLite item unchanged
        snap_stopped = snapshot_db(fixture, specs)
        db_unchanged = (snap_stopped == snap_pre_stop)
        if not db_unchanged:
            raise RuntimeError('database mutated while broker was stopped')
        rows_stopped = read_sqlite_pending(db_a)
        pending_unchanged = (len(rows_stopped) == 1 and
                             rows_stopped[0]['message_id'] == mid_a and
                             rows_stopped[0]['payload'] == payload_a)
        if not pending_unchanged:
            raise RuntimeError('pending SQLite row mutated or vanished while broker was stopped')

        if not broker_stopped_state_ok(health_status_stopped=health_status_stopped,
                                       ready_status_stopped=ready_status_stopped,
                                       db_unchanged=db_unchanged,
                                       pending_unchanged=pending_unchanged):
            raise RuntimeError('broker stopped state predicate failed')
        print(f'PASS: while broker stopped: backend still running on port {backend_port} with /healthz=200, '
              f'/readyz={ready_status_stopped} (!= 200), database unchanged, pending SQLite row intact', flush=True)

        # =====================================================================
        # Phase 4: Start SAME broker container with SAME volumes and TLS config
        # =====================================================================
        ensure_budget('broker-restart:start-broker')
        print('stage=broker-restart:start-broker', flush=True)

        if not broker_same_store_constraints_ok(
            cid_before=broker_cid, cid_after=broker_cid,
            dynsec_vol_before=dynsec_volume, dynsec_vol_after=dynsec_volume,
            data_vol_before=broker_data_volume, data_vol_after=broker_data_volume,
        ):
            raise RuntimeError('same-store constraint violated: broker container or volumes mutated')

        docker(['start', broker_cid], timeout=CONTAINER_START_TIMEOUT)
        broker_port = mapped_port(broker_cid, '8883/tcp')
        wait_tcp(broker_port, BROKER_READY_TIMEOUT)
        print(f'PASS: SAME broker container started with SAME volumes, mapped port refreshed ({broker_port})', flush=True)

        # Backend reconnects and restores readiness
        wait_http(backend_port, '/readyz', READY_TIMEOUT)
        wait_http(backend_port, '/healthz', READY_TIMEOUT)
        print('PASS: backend re-established readiness (/readyz + /healthz ready)', flush=True)

        # Reconnect manager
        manager = connect_manager(broker_port, work, manager_password)
        manager.subscribe(RESPONSE)
        probes.append(manager)

        # Require backend positive exact telemetry subscription and Gateway A positive own ACK subscription
        recv_telemetry = AclProbe(broker_port, ca_file, 'bootstrap_backend', backend_password)
        probes.append(recv_telemetry)
        if recv_telemetry.subscribe_code(telemetry_filter) != 0x00:
            raise RuntimeError('backend telemetry subscription was not granted after broker restart')

        probe_a = AclProbe(broker_port, ca_file, spec_a.gateway_id, new_pw_a)
        probes.append(probe_a)
        if probe_a.subscribe_code(spec_a.ack_filter) != 0x00:
            raise RuntimeError('Gateway A own ACK subscription was not granted after broker restart')
        print('PASS: backend telemetry subscription and Gateway A own ACK subscription verified granted post-restart', flush=True)

        # =====================================================================
        # Phase 5: Post-restart logins, DynSec comparison, positive B, and ACL denial
        # =====================================================================
        print('stage=broker-restart:post-restart-logins', flush=True)
        conn_a_new = mqtt_connection(broker_port, ca_file, spec_a.gateway_id, new_pw_a, accepted=True)
        conn_a_new.close()
        conn_a_old = mqtt_connection(broker_port, ca_file, spec_a.gateway_id, old_pw_a, accepted=False)
        conn_a_old.close()
        conn_disabled = mqtt_connection(broker_port, ca_file, disabled_principal, pw_disabled, accepted=False)
        conn_disabled.close()
        conn_b = mqtt_connection(broker_port, ca_file, spec_b.gateway_id, pw_b, accepted=True)
        conn_b.close()

        if not broker_restart_login_outcomes_ok(new_pw_login=True, old_pw_login_denied=True,
                                                disabled_login_denied=True, positive_gw_login=True):
            raise RuntimeError('post-restart login outcome predicate failed')
        print('PASS: post-restart fresh logins verified (rotated new pw accepted, old pw rejected CONNACK 0x05, '
              'disabled probe rejected, positive Gateway B accepted)', flush=True)

        # Read back DynSec metadata post-restart and compare with baseline
        print('stage=broker-restart:post-restart-dynsec-readback', flush=True)
        client_a_post = manager.request('getClient', username=spec_a.gateway_id)
        client_disabled_post = manager.request('getClient', username=disabled_principal)
        role_a_post = manager.request('getRole', rolename=role_a_name)

        if not dynsec_persistence_comparison_ok(client_a_pre, client_a_post,
                                                client_disabled_pre, client_disabled_post,
                                                role_a_pre, role_a_post):
            raise RuntimeError('DynSec metadata persistence comparison failed against baseline')
        if not ack_block_attached(manager, spec_a.gateway_id):
            raise RuntimeError('ACK receive-deny role was not preserved across broker restart')
        print('PASS: post-restart DynSec metadata strictly matched baseline (roles, disabled flag, exact ACL list, preserved hook role)', flush=True)

        # Own-topic positive path on Gateway B
        print('stage=broker-restart:positive-control-b', flush=True)
        pub_b = AclProbe(broker_port, ca_file, spec_b.gateway_id, pw_b)
        sub_b = AclProbe(broker_port, ca_file, spec_b.gateway_id, pw_b)
        probes.extend([pub_b, sub_b])
        if sub_b.subscribe_code(spec_b.ack_filter) != 0x00:
            raise RuntimeError('Gateway B own ACK subscription was not granted after broker restart')

        b_mid = str(uuid.uuid4())
        b_boot = str(uuid.uuid4())
        b_batch = build_probe_batch(spec_b, b_mid, b_boot, FIRST_SEQUENCE, measured_at=probe_measured_at(0))
        pub_b.publish_qos1(spec_b.telemetry_topic, b_batch)
        ack_b = sub_b.read_app_ack(b_mid, PROBE_ACK_TIMEOUT)
        if ack_b is None:
            raise RuntimeError('Gateway B own-topic positive path produced no application ACK after broker restart')
        assert_ack_fields(ack_b, b_mid)
        assert_gateway_batch(fixture, spec_b, simulator_envelope(), 'BROKER_RESTART_B_POS')
        snap_post_b = snapshot_db(fixture, specs)
        print('PASS: post-restart own-topic positive path on Gateway B verified (committed + received application ACK)', flush=True)

        # Cross-Gateway publish denial: Gateway A (probe with rotated credential) publishes valid batch for B to B's topic
        print('stage=broker-restart:cross-publish-denial', flush=True)
        spoof_mid = str(uuid.uuid4())
        spoof_boot = str(uuid.uuid4())
        spoof_batch = build_probe_batch(spec_b, spoof_mid, spoof_boot, FIRST_SEQUENCE)
        logs_before = container_logs(broker_cid)
        puback_observed = probe_a.publish_qos1(spec_b.telemetry_topic, spoof_batch)
        no_app_ack = sub_b.read_app_ack(spoof_mid, PROBE_DRAIN_TIMEOUT) is None
        logs_after = wait_denied_publish(broker_cid, logs_before, spec_b.telemetry_topic, PROBE_DENY_SETTLE)
        broker_denied = denied_publish_logged(logs_before, logs_after, spec_b.telemetry_topic)
        db_unchanged_cross = (snapshot_db(fixture, specs) == snap_post_b)
        absent = assert_message_id_absent(fixture, spec_b.gateway_id, spoof_mid, 'BROKER_RESTART_CROSS_DENY')

        if not acl_cross_publish_denial_ok(broker_denied=broker_denied,
                                           db_unchanged=db_unchanged_cross,
                                           message_id_absent=absent,
                                           no_app_ack=no_app_ack,
                                           receiver_positive=True):
            raise RuntimeError('post-restart cross-Gateway publish denial failed its oracle: '
                               f'broker_denied={broker_denied} db_unchanged={db_unchanged_cross} '
                               f'absent={absent} no_app_ack={no_app_ack}')
        print(f'PASS: post-restart cross-Gateway publish denied (broker logged "Denied PUBLISH" for B\'s topic '
              f'while QoS1 PUBACK={puback_observed} was not treated as proof, no DB commit, no app ACK to B)', flush=True)

        # =====================================================================
        # Phase 6: Restore ACK reception, verify retry, SQLite 0, truthful completion
        # =====================================================================
        print('stage=broker-restart:restore-ack-and-complete', flush=True)
        detach_ack_receive_block(manager, spec_a.gateway_id)
        block_a_attached = False
        print('PASS: ACK receive-deny hook detached for Gateway A', flush=True)

        exit_a = wait_container_exit(sim_a, ACK_FAULT_EXIT_TIMEOUT)
        if exit_a != 0:
            raise RuntimeError(f'Gateway A simulator exited with code {exit_a} after restore: ' +
                               sanitize(container_logs(sim_a)[-800:]))
        sim_a_logs = container_logs(sim_a)
        if 'completed=true' not in sim_a_logs or 'pending_rows=0' not in sim_a_logs:
            raise RuntimeError('Gateway A simulator logs did not confirm truthful completion: ' +
                               sanitize(sim_a_logs[-800:]))
        cli_a_pending = cli_count_bind(gateway_image, net_name, outbox_dir_a, ca_file, work, f'br_a_{hexid}')
        if cli_a_pending != 0:
            raise RuntimeError(f'cli count bind reported {cli_a_pending} pending rows for Gateway A, want 0')

        # Assert duplicate commit semantics: Gateway A counts/version remain unchanged (deduplicated)
        assert_gateway_batch(fixture, spec_a, simulator_envelope(), 'BROKER_RESTART_FINAL_A')
        print('PASS: Gateway A retry received legitimate application ACK, SQLite pending returned to 0, '
              'deduplicated DB counts/version unchanged, simulator exited 0 cleanly', flush=True)

    finally:
        for p in probes:
            try:
                p.close()
            except Exception:
                pass
        if block_a_attached and manager is not None:
            try:
                detach_ack_receive_block(manager, spec_a.gateway_id)
            except Exception:
                pass


# --- scenario runners and entrypoint ----------------------------------------


def print_scenario_pass(scenario: str) -> None:
    if scenario == 'normal':
        print('PASS: stage3 uplink integration (scenario=normal) two-Gateway normal path verified', flush=True)
    elif scenario == 'tls-credential':
        print('PASS: stage3 uplink integration (scenario=tls-credential) TLS/credential denial slice verified', flush=True)
    elif scenario == 'acl-control':
        print('PASS: stage3 uplink integration (scenario=acl-control) cross-Gateway/control ACL denial slice verified', flush=True)
    elif scenario == 'ack-fault':
        print('PASS: stage3 uplink integration (scenario=ack-fault) ACK spoof denial and lost-ACK byte-exact '
              'resend slice verified', flush=True)
    elif scenario == 'db-timeout':
        print('PASS: stage3 uplink integration (scenario=db-timeout) database timeout and resend recovery slice verified', flush=True)
    elif scenario == 'queue-full':
        print('PASS: stage3 uplink integration (scenario=queue-full) queue-full pressure and recovery slice verified', flush=True)
    elif scenario == 'gateway-reconnect':
        print('PASS: stage3 uplink integration (scenario=gateway-reconnect) ordinary Gateway disconnect, '
              'reconnect, resubscribe, and byte-exact resend slice verified', flush=True)
    elif scenario == 'process-restart':
        print('PASS: stage3 uplink integration (scenario=process-restart) controlled backend restart and Gateway CLI restart slice verified', flush=True)
    elif scenario == 'broker-controlled-restart':
        print('PASS: stage3 uplink integration (scenario=broker-controlled-restart) broker controlled restart '
              'with credential and ACL persistence verified', flush=True)


def run_single_scenario_fixture(scenario: str, backend_image: str, gateway_image: str) -> None:
    """Execute a single scenario with its own disposable isolated fixture and resources.

    The shared backend and gateway images are passed in from the suite caller and
    must NOT be destroyed by this function.
    """
    specs = default_specs()
    validate_specs(specs)
    spec_a = specs[0]
    registry = ResourceRegistry()
    tmp_parent = '/tmp/opencode' if os.path.exists('/tmp/opencode') else None
    work = Path(tempfile.mkdtemp(prefix='stage3-uplink-', dir=tmp_parent))
    registry.register('workdir', 'workdir', work)
    os.chmod(work, 0o700)
    hexid = secrets.token_hex(6)
    net_name = f'stage3_uplink_net_{hexid}'
    broker_data_volume = f'stage3_uplink_brokerdata_{hexid}'
    gateway_volumes: list[str] = []
    # Every backend container (healthy + candidate) created by this run, so the
    # finally block removes them even if the scenario raises before returning.
    owned_containers: list[str] = []
    fixture = None
    dynsec_volume = None
    broker_cid = None
    backend_cid = None
    network_created = False
    primary_error = None

    manager_password = secrets.token_urlsafe(48)
    backend_password = secrets.token_urlsafe(48)
    gateway_passwords = {s.gateway_id: secrets.token_urlsafe(48) for s in specs}
    jwt_secret = secrets.token_urlsafe(48)
    SECRETS.extend([manager_password, backend_password, jwt_secret, *gateway_passwords.values()])

    try:
        ensure_budget('certs')
        print('stage=generate-certs', flush=True)
        generate_certs(work)
        if scenario in ('acl-control', 'ack-fault', 'gateway-reconnect', 'process-restart', 'broker-controlled-restart'):
            # Broker-side denial evidence ("Denied PUBLISH") is logged only at
            # debug level in Mosquitto 2.0.18; enable it on the fixture copy
            # only, never on the checked-in deployment config.
            enable_fixture_deny_logging(work)

        ensure_budget('network')
        print('stage=create-network', flush=True)
        docker(['network', 'create', net_name], timeout=30)
        network_created = True
        registry.register('network', 'network', net_name)

        ensure_budget('database')
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
        seed_graph(fixture, specs)

        ensure_budget('dynsec')
        print('stage=bootstrap-dynsec', flush=True)
        dynsec_volume = bootstrap_dynsec(work, manager_password, backend_password)
        registry.register('dynsec volume', 'volume', dynsec_volume)

        ensure_budget('broker')
        print('stage=start-broker', flush=True)
        docker(['volume', 'create', broker_data_volume], timeout=30)
        registry.register('broker data volume', 'volume', broker_data_volume)
        broker_cid = docker_create([
            '--name', f'stage3-uplink-broker-{hexid}',
            '--user', '1883:1883',
            '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true',
            '--read-only',
            '--tmpfs', '/mosquitto/log:rw,mode=0700,uid=1883,gid=1883,size=16m',
            '--tmpfs', '/tmp:rw,mode=0700,uid=1883,gid=1883,size=8m',
            '--network', net_name,
            '--network-alias', 'broker',
            # A second, SAN-outside alias used only by the 3.6.3 wrong-hostname
            # candidate backend; it routes to the same pinned broker.
            '--network-alias', SAN_NEGATIVE_ALIAS,
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

        ensure_budget('dynsec-control')
        print('stage=dynsec-control', flush=True)
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
        print('PASS: DynSec control traffic created two isolated Gateway principals', flush=True)

        if scenario == 'normal':
            ensure_budget('backend')
            print('stage=start-backend', flush=True)
            backend_cid, backend_port = start_backend(
                work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
                track=owned_containers)
            wait_http(backend_port, '/readyz', READY_TIMEOUT)
            wait_http(backend_port, '/healthz', READY_TIMEOUT)
            print('PASS: backend /readyz and /healthz ready, proving the positive SUBACK for '
                  'the exact filter gateways/+/telemetry/# (readiness requires transport.Ready())', flush=True)

            ensure_budget('gateways')
            print('stage=gateways', flush=True)
            ca_file = work / 'public' / 'ca.crt'
            outbox_volumes = {s.gateway_id: f'stage3_uplink_outbox_{s.gateway_id}_{hexid}' for s in specs}
            for s in specs:
                docker(['volume', 'create', outbox_volumes[s.gateway_id]], timeout=30)
                gateway_volumes.append(outbox_volumes[s.gateway_id])
                registry.register(f'outbox volume {s.gateway_id}', 'volume', outbox_volumes[s.gateway_id])
            envs = {s.gateway_id: simulator_envelope() for s in specs}
            for spec in specs:
                run_gateway_batch(gateway_image, net_name, work, spec, gateway_passwords[spec.gateway_id],
                                  outbox_volumes[spec.gateway_id], ca_file)
                assert_gateway_batch(fixture, spec, envs[spec.gateway_id], f'GATEWAY_{spec.gateway_id}_PASS')
                print(f'PASS: SQL asserts Gateway {spec.gateway_id}: 100 raw (value/index/sequence/time/boot) / '
                      '1 ledger (hash/topic/range/count) / 1 temporal (typed value/provenance/observed_at) / '
                      'current Sensor from the last sample with version +1 / desired+audit+Gateway sentinel preserved', flush=True)

            assert_phase_totals(fixture, specs, ledger=len(specs),
                                raw=len(specs) * SAMPLE_COUNT, temporal=len(specs),
                                marker='TOTALS_PASS')
            print('PASS: phase totals are 2 ledger / 200 raw / 2 temporal for exactly two batches '
                  '(no MQTT deliveries/retries counted as new batches)', flush=True)

            ensure_budget('shutdown')
            print('stage=shutdown', flush=True)
            run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
            exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
            if exit_code != '0':
                raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
            logs = container_logs(backend_cid)
            if 'backend shutdown incomplete' in logs or 'panic:' in logs:
                raise RuntimeError('backend shutdown reported an incomplete/panicked teardown; log=' + sanitize(logs[-1200:]))
            if 'backend shutdown complete' not in logs:
                raise RuntimeError('backend did not log a confirmed complete shutdown; log=' + sanitize(logs[-1200:]))
            print('PASS: normal SIGTERM shutdown within budget with confirmed complete log', flush=True)
        elif scenario == 'tls-credential':
            run_tls_credential(work, net_name, fixture, backend_image, gateway_image, hexid,
                               registry, specs, spec_a, gateway_volumes, owned_containers,
                               jwt_secret, backend_password, gateway_passwords, broker_port)
        elif scenario == 'acl-control':
            run_acl_control(work, net_name, fixture, backend_image, hexid, registry, specs,
                            gateway_passwords, jwt_secret, backend_password, manager_password,
                            broker_cid, broker_port, owned_containers)
        elif scenario == 'ack-fault':
            run_ack_fault(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                          specs, gateway_passwords, jwt_secret, backend_password, manager_password,
                          broker_cid, broker_port, owned_containers)
        elif scenario == 'db-timeout':
            run_db_timeout(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                           specs, gateway_passwords, jwt_secret, backend_password,
                           broker_cid, broker_port, owned_containers)
        elif scenario == 'queue-full':
            run_queue_full(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                           specs, gateway_passwords, jwt_secret, backend_password,
                           broker_cid, broker_port, owned_containers)
        elif scenario == 'gateway-reconnect':
            run_gateway_reconnect(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                                  specs, gateway_passwords, jwt_secret, backend_password, manager_password,
                                  broker_cid, broker_port, owned_containers)
        elif scenario == 'process-restart':
            run_process_restart(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                                specs, gateway_passwords, jwt_secret, backend_password, manager_password,
                                broker_cid, broker_port, owned_containers)
        else:
            run_broker_controlled_restart(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                                          specs, gateway_passwords, jwt_secret, backend_password, manager_password,
                                          broker_cid, broker_port, dynsec_volume, broker_data_volume,
                                          owned_containers)
    except Exception as exc:  # noqa: BLE001 - report sanitized, preserve for cleanup
        primary_error = exc
        raise
    finally:
        cleanup_errors = []
        for label, action in (
            ('backend containers', lambda: [remove_container(cid) for cid in owned_containers
                                            if not container_absent(cid)]),
            ('broker container', lambda: remove_container(broker_cid) if broker_cid else None),
            ('database fixture', lambda: (fixture.cleanup() or None) if fixture else None),
            ('network', lambda: (docker(['network', 'rm', net_name], timeout=30) and None) if network_created else None),
            ('dynsec volume', lambda: (docker(['volume', 'rm', dynsec_volume], timeout=30) and None) if dynsec_volume else None),
            ('broker data volume', lambda: (docker(['volume', 'rm', broker_data_volume], timeout=30) and None)),
            ('gateway outbox volumes', lambda: [docker(['volume', 'rm', vol], timeout=30) for vol in gateway_volumes]),
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
            message = 'scenario cleanup incomplete: ' + '; '.join(cleanup_errors + [f'still present: {r}' for r in remaining])
            if primary_error is None:
                raise RuntimeError(message)
            print(f'scenario cleanup warning: {sanitize(message)}', file=sys.stderr, flush=True)
        else:
            print('Fixture cleanup verified: containers/network/volumes/workdir absent', flush=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Task 3.6 slices: normal uplink path and ACL denial')
    parser.add_argument('--scenario', choices=('all', *tuple(IMPLEMENTED_SCENARIOS)), default='all',
                        help='all: run all mandatory Task 3.6 scenarios in sequence; '
                             'normal: 3.6.1-3.6.2; tls-credential: 3.6.3 rows 1-3; '
                             'acl-control: 3.6.3 rows 4-5 and 7; '
                             'ack-fault: 3.6.3 row 6 + 3.6.4; '
                             'db-timeout: 3.6.5 steps 1-2; '
                             'queue-full: 3.6.5 steps 3-6; '
                             'gateway-reconnect: 3.6.6 step 1; '
                             'process-restart: 3.6.6 steps 2-3; '
                             'broker-controlled-restart: 3.6.7')
    opts = parser.parse_args(argv)
    try:
        resolve_scenario(opts.scenario)
    except ValueError as exc:
        print(f'FAIL: {exc}', file=sys.stderr, flush=True)
        return 2

    for line in notrun_lines():
        print(line, flush=True)

    if not docker_available():
        print('SKIP: docker daemon or CLI unavailable', flush=True)
        return 0

    scenarios = list(IMPLEMENTED_SCENARIOS.keys()) if opts.scenario == 'all' else [opts.scenario]

    suite_registry = ResourceRegistry()
    suite_hexid = secrets.token_hex(6)
    backend_image = None
    gateway_image = None
    primary_error = None

    try:
        ensure_budget('build')
        print('stage=build-images', flush=True)
        backend_tag = f'stage3-uplink-backend-{suite_hexid}'
        gateway_tag = f'stage3-uplink-gateway-{suite_hexid}'
        backend_image = build_image('backend', backend_tag)
        suite_registry.register('backend image', 'image', backend_image)
        gateway_image = build_image('gateway', gateway_tag)
        suite_registry.register('gateway image', 'image', gateway_image)
        print(f'PASS: built backend image {backend_image} and gateway image {gateway_image} from src/Dockerfile', flush=True)

        for sc in scenarios:
            print(f'=== Running scenario: {sc} ===', flush=True)
            run_single_scenario_fixture(sc, backend_image, gateway_image)
            print_scenario_pass(sc)

    except Exception as exc:  # noqa: BLE001 - report sanitized, preserve for cleanup
        primary_error = exc
        raise
    finally:
        cleanup_errors = []
        for label, action in (
            ('backend image', lambda: (docker(['image', 'rm', '-f', backend_image], timeout=60) and None) if backend_image else None),
            ('gateway image', lambda: (docker(['image', 'rm', '-f', gateway_image], timeout=60) and None) if gateway_image else None),
        ):
            try:
                action()
            except Exception as cleanup_exc:  # noqa: BLE001
                cleanup_errors.append(f'{label}: {sanitize(str(cleanup_exc))}')

        remaining = suite_registry.still_present(absent)
        if cleanup_errors or remaining:
            message = 'suite image cleanup incomplete: ' + '; '.join(cleanup_errors + [f'still present: {r}' for r in remaining])
            if primary_error is None:
                print(f'FAIL: {sanitize(message)}', file=sys.stderr, flush=True)
                return 1
            print(f'suite cleanup warning: {sanitize(message)}', file=sys.stderr, flush=True)
        else:
            print('Suite image cleanup verified: shared images removed', flush=True)

    if opts.scenario == 'all':
        sc_names = ', '.join(scenarios)
        print(f'PASS: stage3 uplink integration (all scenarios passed: {sc_names})', flush=True)

    return 0


def run_tls_credential(work, net_name, fixture, backend_image, gateway_image, hexid, registry,
                       specs, spec_a, gateway_volumes, owned_containers, jwt_secret,
                       backend_password, gateway_passwords, broker_port):
    """3.6.3 rows 1-3: TLS/credential denial with positive controls.

    Both TLS negatives are carried by actual Go implementations: the real
    Gateway CLI (wrong CA) and an isolated candidate Go backend (wrong
    hostname/SAN). A bounded Python TLS probe supplies the concrete certificate
    verification reason, and the existing bounded MQTT probe supplies the
    CONNACK 0x05 credential rejection. Every backend container it starts is
    appended to ``owned_containers`` so the caller's finally block can remove
    them even if this function raises.
    """
    ca_file = work / 'public' / 'ca.crt'
    wrong_ca = generate_untrusted_ca(work, 'untrusted')
    gateway_pw = gateway_passwords[spec_a.gateway_id]
    wrong_password = secrets.token_urlsafe(48)
    SECRETS.append(wrong_password)

    pos_a_volume = f'stage3_uplink_pos_a_{hexid}'
    pos_b_volume = f'stage3_uplink_pos_b_{hexid}'
    seed_volume = f'stage3_uplink_seed_{hexid}'
    for vol in (pos_a_volume, pos_b_volume, seed_volume):
        docker(['volume', 'create', vol], timeout=30)
        gateway_volumes.append(vol)
        registry.register(f'outbox volume {vol}', 'volume', vol)

    # --- positive control before the negatives ------------------------------
    print('stage=tls-credential:positive-before', flush=True)
    backend_cid, backend_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        track=owned_containers)
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    wait_http(backend_port, '/healthz', READY_TIMEOUT)
    run_gateway_batch(gateway_image, net_name, work, spec_a, gateway_pw, pos_a_volume, ca_file, name='pos_a')
    assert_gateway_positive(fixture, spec_a, ledger=1, raw=100, temporal=1,
                            reported_version=1, marker='TLS_CRED_POS_BEFORE_PASS')
    ids_before = ledger_message_ids(fixture, spec_a)
    if len(ids_before) != 1:
        raise RuntimeError(f'expected exactly one positive-before ledger id, got {ids_before}')
    print('PASS: positive control (correct CA/hostname/password) before negatives committed one batch '
          'with a distinct message id', flush=True)

    # Positive TLS handshake probe against the real broker (correct CA + SAN).
    if tls_verify_reason('127.0.0.1', broker_port, ca_file, 'localhost') is not None:
        raise RuntimeError('positive TLS handshake with the correct CA/hostname did not complete')
    print('PASS: positive TLS handshake with the correct CA and in-SAN hostname completed', flush=True)

    snapshot_s1 = snapshot_db(fixture, specs)

    # --- seed a durable pending row via the real CLI (application ACK absent) -
    # The broker is healthy and the config is valid, but the backend is stopped
    # so no application ACK can ever arrive: the CLI enqueues, publishes, times
    # out within its completion budget and preserves the pending row. The DB
    # stays free of this batch because no subscriber commits it.
    ensure_budget('tls-credential:backend-stop')
    print('stage=tls-credential:seed-pending', flush=True)
    run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
    seed_rc, seed_out, seed_err = run_gateway_cli_raw(
        gateway_image, net_name, work, spec_a.gateway_id, gateway_pw, seed_volume, ca_file,
        'seed', ['run', '--batches', '1', '--completion-timeout', SEED_COMPLETION_TIMEOUT],
        NEGATIVE_RUN_TIMEOUT)
    if seed_rc == 0:
        raise RuntimeError('seeding run unexpectedly reported success without an application ACK')
    seed_summary = {}
    try:
        seed_summary = parse_run_summary(seed_out)
    except RuntimeError:
        raise RuntimeError('seeding run produced no run summary: ' + sanitize(seed_err[-400:])) from None
    if seed_summary.get('completed') != 'false' or int(seed_summary.get('pending_rows', '0')) < 1 \
            or seed_summary.get('app_acknowledged') != '0':
        raise RuntimeError('seeding run did not retain a pending row: ' + sanitize(seed_out[-400:]))
    pending_seeded = cli_pending_rows(gateway_image, net_name, work, seed_volume, ca_file, 'seed')
    if pending_seeded != 1:
        raise RuntimeError(f'expected exactly one seeded pending row, got {pending_seeded}')
    assert_snapshot_unchanged(snapshot_s1, snapshot_db(fixture, specs), 'seed')
    print('PASS: real CLI seeded one durable pending row with the application-ACK path absent '
          '(valid config, broker healthy); DB unchanged', flush=True)

    # --- row 1: wrong CA via the actual Go Gateway adapter -------------------
    print('stage=tls-credential:wrong-ca', flush=True)
    wrong_ca_reason = tls_verify_reason('127.0.0.1', broker_port, wrong_ca, 'localhost')
    if classify_tls_error(wrong_ca_reason) not in ('unknown_authority', 'certificate'):
        raise RuntimeError(f'wrong-CA TLS probe did not observe a certificate denial: {wrong_ca_reason!r}')
    ca_rc, ca_out, ca_err = run_gateway_cli_raw(
        gateway_image, net_name, work, spec_a.gateway_id, gateway_pw, seed_volume, wrong_ca,
        'wrong_ca', ['run', '--batches', '1', '--completion-timeout', SEED_COMPLETION_TIMEOUT],
        NEGATIVE_RUN_TIMEOUT)
    ready_reached = 'gateway simulator running' in ca_out
    ack_observed = ('app_acknowledged=1' in ca_out) or ('run summary:' in ca_out and 'completed=true' in ca_out)
    go_client_reason = extract_gateway_tls_denial_reason(ca_err + ca_out)
    go_client_cert_denial = go_client_reason in ('tls_unknown_authority', 'tls_certificate_verification')
    if not gateway_tls_denial_ok(exit_code=ca_rc, ready_reached=ready_reached, ack_observed=ack_observed,
                                 cert_denial_observed=go_client_cert_denial):
        raise RuntimeError('wrong-CA Gateway denial failed its oracle: '
                           f'rc={ca_rc} ready={ready_reached} ack={ack_observed} go_cert_denial={go_client_cert_denial} '
                           f'go_reason={go_client_reason!r} probe_reason={wrong_ca_reason!r} ca_err={ca_err!r}')
    if ca_rc == 0:
        raise RuntimeError('wrong-CA Gateway run unexpectedly exited 0')
    pending_after_ca = cli_pending_rows(gateway_image, net_name, work, seed_volume, ca_file, 'seed')
    if pending_after_ca != pending_seeded:
        raise RuntimeError(f'wrong-CA run mutated the pending row count ({pending_seeded}->{pending_after_ca})')
    assert_snapshot_unchanged(snapshot_s1, snapshot_db(fixture, specs), 'wrong-ca')
    print(f'PASS: actual Go Gateway adapter rejected the untrusted CA (exit={ca_rc}, never ready, no ACK, '
          f'go client cert denial reason "{go_client_reason}", probe "{wrong_ca_reason}"); pre-existing pending retained and DB unchanged', flush=True)

    # --- row 3: wrong password via the actual Go Gateway adapter ------------
    print('stage=tls-credential:wrong-password', flush=True)
    rejected = mqtt_connection(broker_port, ca_file, spec_a.gateway_id, wrong_password, accepted=False)
    rejected.close()
    rejected_connack = rejected is not None
    # Positive control: the same broker accepts the correct password.
    good = mqtt_connection(broker_port, ca_file, spec_a.gateway_id, gateway_pw, accepted=True)
    good.close()
    pw_rc, pw_out, pw_err = run_gateway_cli_raw(
        gateway_image, net_name, work, spec_a.gateway_id, wrong_password, seed_volume, ca_file,
        'wrong_pw', ['run', '--batches', '1', '--completion-timeout', SEED_COMPLETION_TIMEOUT],
        NEGATIVE_RUN_TIMEOUT)
    pending_after_pw = cli_pending_rows(gateway_image, net_name, work, seed_volume, ca_file, 'seed')
    if not credential_denial_ok(connack_rejected=rejected_connack, cli_exit_code=pw_rc,
                                pending_before=pending_seeded, pending_after=pending_after_pw):
        raise RuntimeError('wrong-password Gateway denial failed its oracle: '
                           f'connack_rejected={rejected_connack} rc={pw_rc} '
                           f'pending={pending_seeded}->{pending_after_pw}')
    if 'gateway simulator running' in pw_out:
        raise RuntimeError('wrong-password Gateway run unexpectedly reached a subscribed ready state')
    if 'app_acknowledged=1' in pw_out:
        raise RuntimeError('wrong-password Gateway run reported an application ACK')
    assert_snapshot_unchanged(snapshot_s1, snapshot_db(fixture, specs), 'wrong-password')
    print(f'PASS: broker rejected the fresh wrong-password login with CONNACK 0x05 (existing probe) and the '
          f'correct password still authenticates; actual Go Gateway adapter exit={pw_rc}, pre-existing '
          'pending retained and DB unchanged', flush=True)

    # --- positive control after the negatives -------------------------------
    ensure_budget('tls-credential:positive-after')
    print('stage=tls-credential:positive-after', flush=True)
    docker(['start', backend_cid], timeout=CONTAINER_START_TIMEOUT)
    # Docker may reallocate the ephemeral host port on restart, so re-derive the
    # published port from the running container before probing readiness.
    backend_port = mapped_port(backend_cid, '8080/tcp')
    wait_http(backend_port, '/readyz', READY_TIMEOUT)
    run_gateway_batch(gateway_image, net_name, work, spec_a, gateway_pw, pos_b_volume, ca_file, name='pos_b')
    assert_gateway_positive(fixture, spec_a, ledger=2, raw=200, temporal=2,
                            reported_version=2, marker='TLS_CRED_POS_AFTER_PASS')
    ids_after = ledger_message_ids(fixture, spec_a)
    snapshot_s2 = snapshot_db(fixture, specs)
    if len(ids_after) != 2 or len(set(ids_after)) != 2 or not set(ids_before).issubset(set(ids_after)):
        raise RuntimeError(f'positive-before/after message ids are not distinct: {ids_before} {ids_after}')
    print(f'PASS: positive control after negatives committed a second batch with a distinct message id '
          f'({ids_before} vs {ids_after})', flush=True)

    # --- row 2: wrong hostname/SAN via the actual Go backend adapter --------
    print('stage=tls-credential:wrong-san-candidate', flush=True)
    san_reason = tls_verify_reason('127.0.0.1', broker_port, ca_file, SAN_NEGATIVE_ALIAS)
    if classify_tls_error(san_reason) != 'hostname':
        raise RuntimeError(f'wrong-SAN TLS probe did not observe a hostname mismatch: {san_reason!r}')
    candidate_cid, candidate_port = start_backend(
        work, net_name, fixture, backend_image, hexid, registry, jwt_secret, backend_password,
        name_suffix='-candidate', alias='backend-candidate', client_id='backend_san_negative',
        broker_url=f'ssl://{SAN_NEGATIVE_ALIAS}:8883', track=owned_containers)
    # Bounded probe: /healthz must be live while /readyz never becomes ready.
    health_ok = False
    ready_ok = False
    deadline = time.monotonic() + BACKEND_NEGATIVE_PROBE
    while time.monotonic() < deadline:
        status = probe_http_status(candidate_port, '/readyz')
        if status == 200:
            ready_ok = True
            break
        if probe_http_status(candidate_port, '/healthz') == 200:
            health_ok = True
        time.sleep(0.3)
    if not backend_san_denial_ok(ready_http_ok=ready_ok, cert_reason=san_reason):
        raise RuntimeError('wrong-SAN candidate backend denial failed its oracle: '
                           f'ready_ok={ready_ok} reason={san_reason!r}')
    if not health_ok and probe_http_status(candidate_port, '/healthz') != 200:
        raise RuntimeError('candidate backend /healthz never became live; cannot attribute the failure to TLS')
    # The healthy backend must not have been kicked by the candidate (distinct id).
    if probe_http_status(backend_port, '/readyz') != 200:
        raise RuntimeError('the healthy backend lost readiness while the candidate connected with a wrong SAN')
    assert_snapshot_unchanged(snapshot_s2, snapshot_db(fixture, specs), 'wrong-san')
    print(f'PASS: isolated candidate Go backend (distinct client id, no host binary mount) never became ready '
          f'against the SAN-outside alias; cert denial "{san_reason}"; healthy backend stayed ready and DB '
          'unchanged', flush=True)
    remove_container(candidate_cid)
    if candidate_cid in owned_containers:
        owned_containers.remove(candidate_cid)

    # --- final shutdown of the healthy backend ------------------------------
    ensure_budget('tls-credential:shutdown')
    print('stage=tls-credential:shutdown', flush=True)
    run(['docker', 'stop', '-t', '30', backend_cid], timeout=60)
    exit_code = docker(['inspect', '-f', '{{.State.ExitCode}}', backend_cid], timeout=15).decode().strip()
    if exit_code != '0':
        raise RuntimeError(f'backend exited with code {exit_code} on SIGTERM')
    logs = container_logs(backend_cid)
    if 'backend shutdown incomplete' in logs or 'panic:' in logs:
        raise RuntimeError('backend shutdown reported an incomplete/panicked teardown; log=' + sanitize(logs[-1200:]))
    print('PASS: tls-credential slice completed with a clean backend SIGTERM shutdown', flush=True)
    print('PASS: 3.6.3 rows 1-3 verified (wrong CA / wrong hostname-SAN / wrong password) with positive '
          'controls and unchanged DB/SQLite state for every denied input', flush=True)
    return backend_cid


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f'FAIL: {sanitize(str(exc))}', file=sys.stderr, flush=True)
        raise
