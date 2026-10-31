"""Bounded client-side E4 durable Gateway probe helpers.

This module deliberately accepts only the fixed E4 probe contract; it is not a
general command runner. A owns provisioning and SQL verification, while B owns
the Gateway process and its SQLite outbox.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import sqlite3
import subprocess
import tempfile
import urllib.parse


def _private_file(path, *, maximum):
    path = Path(path).absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError('durable_input_symlink_rejected')
    stat = path.lstat()
    if (not path.is_file() or stat.st_uid != os.getuid() or stat.st_mode & 0o077
            or stat.st_size <= 0 or stat.st_size > maximum):
        raise ValueError('durable_input_file_rejected')
    return path


def _input_digest(value):
    return hashlib.sha256(json.dumps({k: v for k, v in value.items()
        if not k.startswith('_')}, sort_keys=True).encode()).hexdigest()


def validate_inputs(value, *, workspace, stage, binding):
    """Validate an explicitly typed, provisioner-created E4 input object."""
    fields = {'schema_version', 'experiment', 'gateway_id', 'sensor_id', 'mqtt_endpoint',
              'mqtt_server_name', 'ca_file', 'username', 'password_file', 'outbox_path',
              'sample_interval_us', 'samples_per_batch', 'max_rows', 'max_bytes',
              'shutdown_timeout_seconds'}
    if not isinstance(value, dict) or set(value) != fields | {'session_id', 'run_id'} or value.get('schema_version') != 1 \
            or value.get('experiment') != 'E4':
        raise ValueError('durable_input_schema_rejected')
    if (not isinstance(binding, dict) or value.get('session_id') != binding.get('session', {}).get('session_id')
            or value.get('run_id') not in binding.get('session', {}).get('run_ids', [])):
        raise ValueError('durable_input_provenance_rejected')
    if stage not in ('fault', 'reopen'):
        raise ValueError('durable_stage_rejected')
    for key in ('gateway_id', 'sensor_id', 'mqtt_server_name', 'username'):
        if not isinstance(value.get(key), str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value[key]):
            raise ValueError('durable_input_identity_rejected')
    if value['sensor_id'] != 'p8-e4-durable':
        raise ValueError('durable_input_sensor_rejected')
    try:
        endpoint = urllib.parse.urlsplit(value['mqtt_endpoint'])
        port = endpoint.port
    except (TypeError, ValueError):
        endpoint, port = None, None
    if (endpoint is None or endpoint.scheme != 'mqtts' or not endpoint.hostname
            or endpoint.username or endpoint.password or endpoint.path not in ('', '/')
            or endpoint.query or endpoint.fragment or port is None or not 1 <= port <= 65535):
        raise ValueError('durable_input_endpoint_rejected')
    canonical = {**value, 'mqtt_endpoint': 'ssl://' + endpoint.netloc}
    for key, low, high in (('sample_interval_us', 10000, 10000),
                           ('samples_per_batch', 100, 100), ('max_rows', 1, 64),
                           ('max_bytes', 1024, 1 << 20), ('shutdown_timeout_seconds', 1, 30)):
        if type(value.get(key)) is not int or not low <= value[key] <= high:
            raise ValueError('durable_input_bounds_rejected')
    root = Path(workspace).resolve(strict=True)
    ca = _private_file(value['ca_file'], maximum=1 << 20)
    password = _private_file(value['password_file'], maximum=4096)
    immutable = (root / 'inputs').resolve(strict=True)
    if (not ca.is_relative_to(immutable) or ca != immutable / 'certs/mqtt-ca.pem'
            or not password.is_relative_to(immutable)
            or password.parent != immutable / 'private/gateways'
            or password.name != value['gateway_id'] + '.json'):
        raise ValueError('durable_input_reference_outside_bundle')
    credential = json.loads(password.read_text(encoding='utf-8'))
    if (not isinstance(credential, dict) or set(credential) != {'username', 'password'}
            or credential.get('username') != value['username']
            or not isinstance(credential.get('password'), str) or not credential['password']):
        raise ValueError('durable_gateway_credential_rejected')
    outbox = Path(value['outbox_path']).absolute()
    if any(p.is_symlink() for p in (outbox, *outbox.parents)) or not outbox.is_relative_to(root):
        raise ValueError('durable_outbox_path_rejected')
    if stage == 'fault' and outbox.exists():
        raise ValueError('durable_outbox_must_be_new')
    if stage == 'reopen':
        proof_path = root / 'runtime/e4-durable-fault.json'
        if (proof_path.is_symlink() or not proof_path.is_file() or proof_path.stat().st_uid != os.getuid()
                or proof_path.stat().st_mode & 0o077 or proof_path.stat().st_size > 65536
                or not outbox.is_file() or outbox.is_symlink()
                or outbox.stat().st_uid != os.getuid() or outbox.stat().st_mode & 0o077):
            raise ValueError('durable_fault_proof_required_for_reopen')
        proof = json.loads(proof_path.read_text(encoding='utf-8'))
        prior = readonly_pending_snapshot(outbox, value['gateway_id'], value['sensor_id'])
        if (proof.get('stage') != 'fault' or proof.get('classification') != 'INTENTIONAL_FAULT_STOP'
                or proof.get('joined') is not True or proof.get('pending') != prior
                or proof.get('inputs_sha256') != _input_digest(canonical)):
            raise ValueError('durable_fault_proof_or_pending_mismatch')
        canonical['_prior_pending'] = prior
    return {**canonical, 'ca_file': str(ca), 'password_file': str(password),
            'outbox_path': str(outbox)}


def readonly_pending_snapshot(outbox, gateway_id, sensor_id):
    """Read and hash exactly one pending row without modifying the SQLite file."""
    path = Path(outbox).absolute()
    if (any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file()
            or path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077):
        raise ValueError('durable_sqlite_missing_or_unsafe')
    db = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        rows = db.execute("SELECT message_id,boot_id,gateway_id,sensor_id,first_sequence,"
            "sample_interval_us,sample_count,payload,byte_size FROM outbox WHERE status='pending'").fetchall()
    except sqlite3.Error:
        raise ValueError('durable_sqlite_schema_rejected') from None
    finally:
        db.close()
    if len(rows) != 1:
        raise ValueError('durable_pending_row_count')
    mid, boot, gw, sensor, seq, interval, count, payload, byte_size = rows[0]
    payload = bytes(payload)
    if (gw != gateway_id or sensor != sensor_id or count != 100 or not payload
            or type(byte_size) is not int or byte_size < len(payload)):
        raise ValueError('durable_pending_identity_or_payload_rejected')
    try:
        header = json.loads(payload)
    except (UnicodeError, json.JSONDecodeError):
        raise ValueError('durable_pending_payload_invalid') from None
    keys = {'protocol_version', 'message_id', 'message_type', 'gateway_id', 'sensor_id',
            'boot_id', 'first_sequence', 'sample_count', 'measured_at',
            'sample_interval_us', 'samples'}
    if (not isinstance(header, dict) or set(header) != keys or header.get('protocol_version') != 1
            or header.get('message_type') != 'telemetry_batch' or header.get('message_id') != mid
            or header.get('gateway_id') != gw or header.get('sensor_id') != sensor
            or header.get('boot_id') != boot or header.get('first_sequence') != seq
            or header.get('sample_count') != 100 or header.get('sample_interval_us') != interval
            or not isinstance(header.get('samples'), list) or len(header['samples']) != 100):
        raise ValueError('durable_pending_header_mismatch')
    return {'message_id': mid, 'boot_id': boot, 'gateway_id': gw, 'sensor_id': sensor,
            'sequence': seq, 'interval_us': interval, 'sample_count': count,
            'payload_sha256': hashlib.sha256(payload).hexdigest(),
            'payload_bytes': len(payload), 'header': {k: header[k] for k in
                ('gateway_id', 'sensor_id', 'boot_id', 'first_sequence', 'sample_count',
                 'sample_interval_us', 'measured_at')}}


def run_stage(*, gateway_binary, inputs, phase, env_base, timeout=45):
    """Run only the approved fixed fault-stop or reopen invocation."""
    if phase not in ('fault', 'reopen'):
        raise ValueError('durable_stage_rejected')
    binary = Path(gateway_binary).absolute()
    if binary.is_symlink() or not binary.is_file():
        raise ValueError('durable_gateway_binary_rejected')
    env = dict(env_base)
    password = json.loads(Path(inputs['password_file']).read_text(encoding='utf-8'))['password']
    env.update({'GATEWAY_ID': inputs['gateway_id'], 'GATEWAY_CONTROL_ENABLED': 'false', 'SENSOR_ID': inputs['sensor_id'],
        'GATEWAY_MQTT_BROKER_URL': inputs['mqtt_endpoint'],
        'GATEWAY_MQTT_CA_FILE': inputs['ca_file'],
        'GATEWAY_MQTT_SERVER_NAME': inputs['mqtt_server_name'],
        'GATEWAY_MQTT_USERNAME': inputs['username'], 'GATEWAY_MQTT_PASSWORD': password,
        'GATEWAY_MQTT_CLIENT_ID': f"{inputs['gateway_id']}_p8_e4_durable_{phase}",
        'GATEWAY_OUTBOX_PATH': inputs['outbox_path'], 'GATEWAY_SAMPLE_RATE_HZ': '100',
        'GATEWAY_SAMPLES_PER_BATCH': '100', 'GATEWAY_OUTBOX_MAX_ROWS': str(inputs['max_rows']),
        'GATEWAY_OUTBOX_MAX_BYTES': str(inputs['max_bytes']),
        'GATEWAY_SHUTDOWN_TIMEOUT': f"{inputs['shutdown_timeout_seconds']}s"})
    args = [str(binary), 'run', '--batches', '1', '--completion-timeout', '20s']
    if phase == 'fault':
        args += ['--ignore-first-valid-acks', '1', '--stop-after-ignored-ack']
    def limit_output():
        resource.setrlimit(resource.RLIMIT_FSIZE, (1 << 20, 1 << 20))
    with tempfile.TemporaryFile(mode='w+b') as output:
        proc = subprocess.Popen(args, env=env, stdin=subprocess.DEVNULL, stdout=output,
            stderr=subprocess.STDOUT, close_fds=True, preexec_fn=limit_output,
            umask=0o077)
        try:
            proc.wait(timeout=min(timeout, 60))
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
            raise ValueError('durable_gateway_timeout') from None
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=2)
        output.seek(0)
        raw_output = output.read((1 << 20) + 1)
        if len(raw_output) > 1 << 20:
            raise ValueError('durable_gateway_output_limit')
    # Raw process output can contain operational details; only fixed marker fields escape B.
    text = raw_output.decode('utf-8', errors='replace')
    summaries = []
    for line in text.splitlines():
        if line.startswith('run summary:'):
            summary = {}
            for part in line[len('run summary:'):].split():
                key, sep, val = part.partition('=')
                if sep and re.fullmatch(r'[a-z_]+', key) and re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', val):
                    summary[key] = val
            summaries.append(summary)
    summary = summaries[-1] if len(summaries) == 1 else {}
    if phase == 'fault':
        valid = (proc.returncode == 1 and summary.get('intentional_fault_stop') == 'true'
            and summary.get('ignored_valid_acks') == '1' and summary.get('completed') == 'false'
            and summary.get('enqueued') == '1' and summary.get('app_acknowledged') == '0'
            and summary.get('pending_rows') == '1')
        if not valid:
            raise ValueError('durable_intentional_stop_unproven')
        pending = readonly_pending_snapshot(inputs['outbox_path'], inputs['gateway_id'], inputs['sensor_id'])
        return {'stage': 'fault', 'classification': 'INTENTIONAL_FAULT_STOP',
                'exit_code': proc.returncode, 'joined': True, 'ignored_valid_acks': 1,
                'pending': pending, 'inputs_sha256': _input_digest(inputs)}
    if proc.returncode != 0 or summary.get('completed') != 'true':
        raise ValueError('durable_reopen_incomplete')
    db = sqlite3.connect(f"file:{inputs['outbox_path']}?mode=ro", uri=True)
    try:
        pending, quarantine = (db.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0],
            db.execute("SELECT count(*) FROM outbox WHERE status='quarantined'").fetchone()[0])
    finally:
        db.close()
    if (pending != 0 or quarantine != 0 or int(summary.get('app_acknowledged', '0')) < 2
            or summary.get('enqueued') != '1'):
        raise ValueError('durable_reopen_not_drained')
    return {'stage': 'reopen', 'classification': 'DRAINED', 'exit_code': 0,
            'joined': True, 'pending_rows': pending, 'quarantined_rows': quarantine,
            'application_acks': int(summary['app_acknowledged']), 'generated': int(summary['enqueued']),
            'replayed_pending': inputs.get('_prior_pending')}
