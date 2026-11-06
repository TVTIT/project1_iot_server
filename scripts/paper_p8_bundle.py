#!/usr/bin/env python3
"""Xuất/kiểm tra bundle input client P8 có giới hạn, di chuyển được, không dùng mạng."""
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import ssl
import stat
import time
import urllib.parse
import urllib.request
import urllib.error

SCHEMA_VERSION = 1
MAX_FILE_BYTES = 8 << 20
MAX_MANIFEST_BYTES = 8 << 20
MAX_TOTAL_BYTES = 32 << 20
MAX_FILES = 96
MAX_WAIT_SECONDS = 7200
MAX_E5_VIEWER_SECONDS = 3600
MAX_READINESS_SECONDS = 120
MAX_E1_VIEWER_READINESS_SECONDS = 300
SAFE_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$')
MANIFEST_TOP = {'schema_version','experiment','arm','profile','mode','run_id','seed','roles',
    'samples_per_message','topology','targets','subscriptions','traffic','phases','limits','tls',
    'identity_files','gateway_identity_files','bundle_path','durability_mode','retry_interval_ms',
    'publish_timeout_ms','viewer','commands','e4','equal_samples','retry_policy'}
RUN_SPEC_KEYS = {'paperload','gateways','stop_timeout_seconds','disk_guard'}

E2_EQUAL_SAMPLES_CONTEXT_KEYS = frozenset({
    'schema_version', 'workload_type', 'seed', 'total_samples', 'sample_rate',
    'duration_seconds', 'logical_producers', 'samples_per_producer',
    'producer_sample_rate', 'sample_interval_us', 'stream_sequence_start',
    'timestamp_anchor', 'max_context_age_seconds', 'context_sha256',
})
E2_EQUAL_SAMPLES_MANIFEST_KEYS = frozenset(
    set(E2_EQUAL_SAMPLES_CONTEXT_KEYS) | {
        'expected_samples', 'expected_messages', 'sample_stream_version',
    }
)
E2_EQUAL_SAMPLES_WORKLOAD_KEYS = frozenset({
    'experiment', 'arm', 'phase', 'profile', 'workload', 'workload_type',
    'seed', 'samples_per_message', 'duration_seconds', 'drain_seconds',
    'grace_seconds', 'wall_seconds', 'context_sha256', 'expected_counts',
    'context', 'generator_colocated',
})


def _fail(code):
    raise ValueError(code)


def _is_e1_viewer_manifest(manifest_data, role):
    if role not in ('viewer', 'auto'):
        return False
    if not isinstance(manifest_data, dict):
        return False
    return manifest_data.get('experiment') == 'E1' and manifest_data.get('roles') == ['viewer']


def _is_validated_e1_viewer_file(manifest_path, role, ready_timeout_seconds=None):
    if role not in ('viewer', 'auto'):
        return False
    if not isinstance(manifest_path, (str, Path)) or not manifest_path or '\x00' in str(manifest_path) or str(manifest_path).startswith('-'):
        return False
    try:
        p = Path(manifest_path)
        if any(component.is_symlink() for component in (p, *p.parents)):
            return False
        if not p.is_file():
            return False
        st = p.stat()
        if st.st_size <= 0 or st.st_size > MAX_MANIFEST_BYTES:
            return False
        data = json.loads(p.read_text(encoding='utf-8'))
        if not isinstance(data, dict):
            return False
        if data.get('experiment') != 'E1' or data.get('roles') != ['viewer']:
            return False
        _strict_manifest(data, 'E1', data.get('arm', ''))
        if ready_timeout_seconds is not None and ready_timeout_seconds > MAX_READINESS_SECONDS:
            viewers = data.get('topology', {}).get('viewers', 0)
            connections = data.get('limits', {}).get('connections', 0)
            count = viewers if (type(viewers) is int and viewers > 0) else connections
            ramp = data.get('traffic', {}).get('ramp_seconds', 0)
            wall = data.get('limits', {}).get('wall_seconds', 0)
            if type(count) is not int or count < 1:
                return False
            if type(ramp) is not int or ramp <= 0:
                return False
            if type(wall) is not int or wall < ready_timeout_seconds:
                return False
            timing = e1_timing_budget(count, phases=data.get('phases'), declared_ramp=ramp)
            if wall < timing['wall_seconds']:
                return False
        return True
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        return False


def e1_timing_budget(viewers, phases=None, declared_ramp=None):
    """Compute deterministic E1 ramp, readiness timeout, and wall seconds."""
    if type(viewers) is not int or type(viewers) is bool or viewers < 1:
        raise ValueError('e1_viewer_count_rejected')
    phase_keys = ('warmup_seconds', 'measure_seconds', 'drain_seconds', 'cooldown_seconds')
    phase_values = dict(phases or {'warmup_seconds': 0, 'measure_seconds': 14,
                                   'drain_seconds': 8, 'cooldown_seconds': 0})
    if not isinstance(phase_values, dict):
        raise ValueError('e1_phase_budget_rejected')
    for k in phase_keys:
        v = phase_values.get(k, 0)
        if type(v) is not int or type(v) is bool or v < 0 or v > 7200:
            raise ValueError('e1_phase_budget_rejected')
    phase_sum = sum(phase_values.get(k, 0) for k in phase_keys)
    phase_workload = sum(phase_values.get(k, 0) for k in ('warmup_seconds', 'measure_seconds', 'drain_seconds'))
    if phase_sum < 0 or phase_sum > 7200:
        raise ValueError('e1_phase_budget_rejected')
    if declared_ramp is not None:
        if type(declared_ramp) is not int or type(declared_ramp) is bool or declared_ramp < 0 or declared_ramp > 7200:
            raise ValueError('e1_ramp_budget_rejected')

    if declared_ramp is not None:
        ramp_seconds = declared_ramp
    else:
        ramp_seconds = min(120, max(1, viewers // 50))

    if viewers >= 2000:
        setup_grace = 60
        ready_seconds = ramp_seconds + setup_grace
    else:
        pilot_grace = max(5, phase_workload)
        ready_seconds = max(pilot_grace, ramp_seconds)

    wall_seconds = ready_seconds + phase_sum + 8

    if ready_seconds > MAX_E1_VIEWER_READINESS_SECONDS:
        raise ValueError('e1_readiness_budget_rejected')
    if wall_seconds < 1 or wall_seconds > 7200:
        raise ValueError('e1_phase_budget_rejected')

    return {
        'ramp_seconds': ramp_seconds,
        'ready_timeout_seconds': ready_seconds,
        'wall_seconds': wall_seconds,
    }


def _source(path, *, limit=MAX_FILE_BYTES):
    p = Path(path)
    if any(component.is_symlink() for component in (p, *p.parents)):
        _fail('bundle_source_symlink_rejected')
    try:
        st = p.lstat()
        if (not stat.S_ISREG(st.st_mode) or st.st_size <= 0 or st.st_size > limit
                or st.st_uid != os.getuid() or st.st_mode & 0o077):
            _fail('bundle_source_file_rejected')
        with p.open('rb') as source:
            data = source.read(limit + 1)
    except OSError:
        _fail('bundle_source_missing')
    if len(data) > limit:
        _fail('bundle_source_file_rejected')
    if len(data) != st.st_size:
        _fail('bundle_source_changed')
    return data


def _json_file(path, *, limit=MAX_FILE_BYTES):
    data = _source(path, limit=limit)
    def reject_constant(_):
        raise ValueError('nonfinite')
    try:
        value = json.loads(data, parse_constant=reject_constant)
    except (ValueError, UnicodeDecodeError):
        _fail('bundle_source_json_rejected')
    return value


def _derive_e1_lifecycle(manifests):
    """Conservative finite upper budget for staggered viewer/telemetry startup."""
    if type(manifests) is not dict or set(manifests) != {'viewer', 'telemetry'}:
        _fail('bundle_e1_lifecycle_rejected')
    walls = {}
    for role, source in manifests.items():
        raw = _json_file(source, limit=MAX_MANIFEST_BYTES) if isinstance(source, (str, Path)) else source
        if type(raw) is not dict:
            _fail('bundle_e1_lifecycle_rejected')
        limits = raw.get('limits')
        wall = limits.get('wall_seconds') if type(limits) is dict else None
        if (raw.get('roles') != [role] or type(wall) is not int or wall < 1 or wall > 7200):
            _fail('bundle_e1_lifecycle_rejected')
        walls[role] = wall
    # Viewer starts first and remains alive while telemetry starts; sum is a
    # conservative finite upper budget, not measured sequential execution.
    return sum(walls.values())


def _write(root, rel, data, *, private=True):
    relpath = PurePosixPath(rel)
    if relpath.is_absolute() or any(x in ('', '.', '..') for x in relpath.parts):
        _fail('bundle_path_rejected')
    dest = root.joinpath(*relpath.parts)
    current=root
    for component in relpath.parts[:-1]:
        current=current/component
        try:
            current.mkdir(mode=0o700)
            os.chmod(current,0o700)
        except FileExistsError:
            st=current.lstat()
            if not stat.S_ISDIR(st.st_mode) or stat.S_IMODE(st.st_mode) != 0o700:
                _fail('bundle_directory_rejected')
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600 if private else 0o644)
    with os.fdopen(fd, 'wb') as f:
        os.fchmod(f.fileno(),0o600 if private else 0o644)
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    return dest


def _safe_ref(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        _fail('bundle_path_reference_rejected')
    path = PurePosixPath(value.replace('\\','/'))
    if path.is_absolute() or any(part in ('.','..') for part in path.parts):
        _fail('bundle_path_reference_rejected')
    return str(path)


def _jwt_expiry(token):
    if not isinstance(token, str): _fail('bundle_identity_rejected')
    parts = token.split('.')
    if len(parts) != 3: _fail('bundle_identity_rejected')
    try:
        raw = parts[1] + '=' * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(raw.encode('ascii')))
        exp = payload.get('exp') if isinstance(payload, dict) else None
    except (ValueError, UnicodeError, TypeError): _fail('bundle_identity_rejected')
    if type(exp) not in (int,float) or not math.isfinite(exp) or exp <= 0:
        _fail('bundle_identity_expiry_missing')
    return float(exp)


def _collect_jwts(value, path='$'):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _collect_jwts(child, f'{path}.{key}')
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _collect_jwts(child, f'{path}[{index}]')
    elif isinstance(value, str) and value.count('.') == 2:
        yield path, value


def _public_metadata(value):
    forbidden=('admin','password','secret','credential','token','jwt','private_key','user_id','email','identity')
    if isinstance(value,dict):
        if any(any(fragment in str(key).lower() for fragment in forbidden) for key in value):
            _fail('bundle_public_metadata_rejected')
        for child in value.values(): _public_metadata(child)
    elif isinstance(value,list):
        for child in value: _public_metadata(child)
    elif isinstance(value,str) and (value.startswith('/') or re.match(r'^[A-Za-z]:[\\/]',value)):
        _fail('bundle_public_metadata_path_rejected')


def _endpoint(value, scheme):
    if not isinstance(value,str): _fail('bundle_endpoint_rejected')
    try: parsed=urllib.parse.urlsplit(value); port=parsed.port
    except ValueError: _fail('bundle_endpoint_rejected')
    if (parsed.scheme != scheme or not parsed.hostname or parsed.username or parsed.password
            or parsed.path not in ('','/') or parsed.query or parsed.fragment
            or port is not None and (not 1 <= port <= 65535
                or port in {80,1883,5432,5433,8000,8080,2375,2376,8884})):
        _fail('bundle_endpoint_rejected')
    return value


def _record_inventory(inventory, rel, kind, data):
    if (len(inventory) >= MAX_FILES or len(data) > MAX_FILE_BYTES
            or sum(item['size_bytes'] for item in inventory) + len(data) > MAX_TOTAL_BYTES):
        _fail('bundle_size_limit_exceeded')
    inventory.append({
        'path': rel,
        'kind': kind,
        'size_bytes': len(data),
        'sha256': hashlib.sha256(data).hexdigest(),
    })


def _copy_input(root, src, rel, inventory, kind):
    data = _source(src)
    _write(root, rel, data)
    _record_inventory(inventory, rel, kind, data)
    return rel


def _strict_manifest(value, experiment, arm):
    if type(value) is not dict or set(value) - MANIFEST_TOP:
        _fail('bundle_manifest_fields_rejected')
    required = {'schema_version','experiment','arm','profile','mode','run_id','seed','roles','topology',
                'targets','traffic','phases','limits','tls','identity_files'}
    roles = value.get('roles')
    if not isinstance(roles, list) or roles != ['control']:
        required.update({'samples_per_message', 'subscriptions'})
    if not required <= set(value):
        _fail('bundle_manifest_required_input_missing')
    if value.get('schema_version') != 1 or value.get('experiment') not in ('E1','E2','E3','E4','E5'):
        _fail('bundle_manifest_contract_rejected')
    allowed_mode = (value.get('mode') == 'remote' or
                    experiment == 'E3' and arm == 'control' and value.get('mode') == 'local')
    if value['experiment'] != experiment or value.get('arm') != arm or not allowed_mode:
        _fail('bundle_manifest_identity_mismatch')
    if not isinstance(value.get('run_id'), str) or not SAFE_ID.fullmatch(value['run_id']):
        _fail('bundle_manifest_identity_mismatch')
    if value.get('profile') not in ('pilot','representative','official') or not isinstance(value.get('roles'),list):
        _fail('bundle_manifest_contract_rejected')
    object_fields={'topology':{'gateways','sensors_per_gateway','users','viewers'},
        'traffic':{'rate_per_second','ramp_seconds','burst_messages'},
        'phases':{'warmup_seconds','measure_seconds','drain_seconds','cooldown_seconds'},
        'limits':{'retries','workers','connections','pending_messages','pending_bytes',
                  'disk_growth_bytes','wall_seconds'},
        'tls':{'https_endpoint','mqtt_tls_endpoint','https_ca_file','mqtt_tls_ca_file','mqtt_tls_server_name'},
        'viewer':{'mode','slow_pause_ms','reconnect_count','identity_window','max_messages','max_bytes'},
        'e4':{'mode','journal_path','max_messages','max_bytes','replay_count','ignore_first_valid_acks',
              'expiry_ttl_seconds'}}
    for key, allowed in object_fields.items():
        if key == 'traffic' and experiment == 'E2' and value.get('profile') == 'official':
            allowed = allowed | {'total_rate_per_second'}
        if key in value and value[key] is not None and (type(value[key]) is not dict or set(value[key])-allowed):
            _fail('bundle_manifest_fields_rejected')
    for field, allowed in (('targets',{'gateway_id','sensor_id'}),
                           ('subscriptions',{'viewer_id','user_id','gateway_id','sensor_id'}),
                           ('commands',{'gateway_id','sensor_id','frequency_hz','offset_ms','actor_label',
                               'idempotency_key','expected_desired_version'})):
        rows=value.get(field,[])
        if type(rows) is not list or any(type(row) is not dict or set(row)-allowed for row in rows):
            _fail('bundle_manifest_fields_rejected')
    if 'equal_samples' in value and value['equal_samples'] is not None:
        if experiment != 'E2' or value.get('experiment') != 'E2':
            _fail('bundle_manifest_contract_rejected')
        _validate_equal_samples_manifest_obj(value['equal_samples'], value.get('samples_per_message'), value.get('seed'))
    if 'retry_policy' in value:
        if experiment != 'E2' or value.get('profile') != 'official' or value.get('arm') not in ('A', 'B'):
            _fail('bundle_retry_policy_scope_rejected')
        try:
            from scripts.tests.paper_p8_e2_retry_policy import validate_retry_policy
        except ModuleNotFoundError:
            import importlib.util
            spec = importlib.util.spec_from_file_location('paper_p8_e2_retry_policy',
                Path(__file__).parent / 'tests' / 'paper_p8_e2_retry_policy.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            validate_retry_policy = module.validate_retry_policy
        try:
            validate_retry_policy(value['retry_policy'], pending_messages=value['limits']['pending_messages'])
        except ValueError:
            _fail('bundle_retry_policy_rejected')
    return value


def _validate_equal_samples_manifest_obj(eq, S, seed):
    if type(eq) is not dict or set(eq) != E2_EQUAL_SAMPLES_MANIFEST_KEYS:
        _fail('bundle_manifest_fields_rejected')
    int_fields = (
        'schema_version', 'seed', 'total_samples', 'sample_rate', 'duration_seconds',
        'samples_per_producer', 'producer_sample_rate', 'sample_interval_us',
        'stream_sequence_start', 'max_context_age_seconds', 'expected_samples',
        'expected_messages', 'sample_stream_version'
    )
    for field in int_fields:
        if type(eq.get(field)) is not int:
            _fail('bundle_manifest_contract_rejected')
    if (eq['schema_version'] != 1
            or eq['workload_type'] != 'equal_samples'
            or eq['sample_stream_version'] != 1
            or eq['logical_producers'] != ['A', 'B']
            or eq['stream_sequence_start'] != 0):
        _fail('bundle_manifest_contract_rejected')
    if seed is not None and eq['seed'] != seed:
        _fail('bundle_manifest_contract_rejected')
    if not (0 <= eq['seed'] <= (1 << 63) - 1):
        _fail('bundle_manifest_contract_rejected')
    if eq['total_samples'] <= 0 or eq['total_samples'] > 60000 or eq['total_samples'] % 200 != 0:
        _fail('bundle_manifest_contract_rejected')
    if eq['sample_rate'] < 2 or eq['sample_rate'] > 200 or eq['sample_rate'] % 2 != 0:
        _fail('bundle_manifest_contract_rejected')
    if eq['total_samples'] % eq['sample_rate'] != 0:
        _fail('bundle_manifest_contract_rejected')
    duration_seconds = eq['total_samples'] // eq['sample_rate']
    if eq['duration_seconds'] != duration_seconds or not (10 <= duration_seconds <= 120):
        _fail('bundle_manifest_contract_rejected')
    if 2_000_000 % eq['sample_rate'] != 0:
        _fail('bundle_manifest_contract_rejected')
    if eq['sample_interval_us'] != 2_000_000 // eq['sample_rate']:
        _fail('bundle_manifest_contract_rejected')
    if eq['samples_per_producer'] != eq['total_samples'] // 2:
        _fail('bundle_manifest_contract_rejected')
    if eq['producer_sample_rate'] != eq['sample_rate'] // 2:
        _fail('bundle_manifest_contract_rejected')
    if eq['max_context_age_seconds'] <= 0 or eq['max_context_age_seconds'] > 2592000:
        _fail('bundle_manifest_contract_rejected')
    if S not in (1, 100):
        _fail('bundle_manifest_contract_rejected')
    if eq['expected_samples'] != eq['samples_per_producer']:
        _fail('bundle_manifest_contract_rejected')
    if eq['expected_messages'] != eq['samples_per_producer'] // S:
        _fail('bundle_manifest_contract_rejected')
    ctx_part = {k: v for k, v in eq.items() if k in E2_EQUAL_SAMPLES_CONTEXT_KEYS and k != 'context_sha256'}
    canonical_bytes = json.dumps(ctx_part, sort_keys=True, separators=(',', ':')).encode('utf-8')
    expected_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    if eq.get('context_sha256') != expected_sha256:
        _fail('bundle_manifest_contract_rejected')


def _validate_e2_equal_samples_workload(workload):
    if type(workload) is not dict or set(workload) != E2_EQUAL_SAMPLES_WORKLOAD_KEYS:
        _fail('bundle_workload_metadata_rejected')
    if (workload.get('experiment') != 'E2'
            or workload.get('workload') != 'equal_samples'
            or workload.get('workload_type') != 'equal_samples'
            or workload.get('profile') not in ('pilot', 'official')
            or not isinstance(workload.get('arm'), str) or not SAFE_ID.fullmatch(workload['arm'])
            or not isinstance(workload.get('phase'), str) or not SAFE_ID.fullmatch(workload['phase'])):
        _fail('bundle_workload_metadata_rejected')
    if (type(workload.get('seed')) is not int or not (0 <= workload['seed'] <= (1 << 63) - 1)
            or type(workload.get('samples_per_message')) is not int
            or workload['samples_per_message'] not in (1, 100)
            or type(workload.get('duration_seconds')) is not int
            or not (10 <= workload['duration_seconds'] <= 120)
            or type(workload.get('drain_seconds')) is not int
            or not (0 <= workload['drain_seconds'] <= 60)
            or type(workload.get('grace_seconds')) is not int
            or not (0 <= workload['grace_seconds'] <= 60)
            or type(workload.get('wall_seconds')) is not int
            or workload['wall_seconds'] != workload['duration_seconds'] + workload['drain_seconds'] + workload['grace_seconds']):
        _fail('bundle_workload_metadata_rejected')
    if (not isinstance(workload.get('context_sha256'), str)
            or not re.fullmatch(r'[0-9a-f]{64}', workload['context_sha256'])):
        _fail('bundle_workload_metadata_rejected')
    if workload.get('generator_colocated') not in ('unverified', True, False, 'true', 'false'):
        _fail('bundle_workload_metadata_rejected')

    ctx = workload.get('context')
    if type(ctx) is not dict or set(ctx) != E2_EQUAL_SAMPLES_CONTEXT_KEYS:
        _fail('bundle_workload_metadata_rejected')
    int_fields = (
        'schema_version', 'seed', 'total_samples', 'sample_rate', 'duration_seconds',
        'samples_per_producer', 'producer_sample_rate', 'sample_interval_us',
        'stream_sequence_start', 'max_context_age_seconds'
    )
    for field in int_fields:
        if type(ctx.get(field)) is not int:
            _fail('bundle_workload_metadata_rejected')


    if (ctx['schema_version'] != 1
            or ctx['workload_type'] != 'equal_samples'
            or ctx['logical_producers'] != ['A', 'B']
            or ctx['stream_sequence_start'] != 0):
        _fail('bundle_workload_metadata_rejected')
    if ctx['seed'] != workload['seed']:
        _fail('bundle_workload_metadata_rejected')
    if ctx['duration_seconds'] != workload['duration_seconds']:
        _fail('bundle_workload_metadata_rejected')
    if ctx['total_samples'] <= 0 or ctx['total_samples'] > 60000 or ctx['total_samples'] % 200 != 0:
        _fail('bundle_workload_metadata_rejected')
    if ctx['sample_rate'] < 2 or ctx['sample_rate'] > 200 or ctx['sample_rate'] % 2 != 0:
        _fail('bundle_workload_metadata_rejected')
    if ctx['total_samples'] % ctx['sample_rate'] != 0:
        _fail('bundle_workload_metadata_rejected')
    if ctx['duration_seconds'] != ctx['total_samples'] // ctx['sample_rate']:
        _fail('bundle_workload_metadata_rejected')
    if 2_000_000 % ctx['sample_rate'] != 0:
        _fail('bundle_workload_metadata_rejected')
    if ctx['sample_interval_us'] != 2_000_000 // ctx['sample_rate']:
        _fail('bundle_workload_metadata_rejected')
    if ctx['samples_per_producer'] != ctx['total_samples'] // 2:
        _fail('bundle_workload_metadata_rejected')
    if ctx['producer_sample_rate'] != ctx['sample_rate'] // 2:
        _fail('bundle_workload_metadata_rejected')
    if ctx['max_context_age_seconds'] <= 0 or ctx['max_context_age_seconds'] > 2592000:
        _fail('bundle_workload_metadata_rejected')
    ctx_payload = {k: v for k, v in ctx.items() if k != 'context_sha256'}
    canonical_bytes = json.dumps(ctx_payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    expected_sha256 = hashlib.sha256(canonical_bytes).hexdigest()
    if ctx.get('context_sha256') != expected_sha256 or workload['context_sha256'] != expected_sha256:
        _fail('bundle_workload_metadata_rejected')

    exp = workload.get('expected_counts')
    allowed_counts_keys = {
        'samples_per_message', 'total_samples', 'total_messages',
        'total_raw_rows', 'total_ledger_rows', 'total_temporal_rows',
        'total_configuration_delta', 'per_role'
    }
    if type(exp) is not dict or set(exp) != allowed_counts_keys:
        _fail('bundle_workload_metadata_rejected')
    S = workload['samples_per_message']
    total_samples = ctx['total_samples']
    total_messages = total_samples // S
    if (exp['samples_per_message'] != S
            or exp['total_samples'] != total_samples
            or exp['total_messages'] != total_messages
            or exp['total_raw_rows'] != total_samples
            or exp['total_ledger_rows'] != total_messages
            or exp['total_temporal_rows'] != total_messages
            or exp['total_configuration_delta'] != 0):
        _fail('bundle_workload_metadata_rejected')
    per_role = exp.get('per_role')
    if type(per_role) is not dict or set(per_role) != {'A', 'B'}:
        _fail('bundle_workload_metadata_rejected')
    allowed_role_keys = {'samples', 'messages', 'raw_rows', 'ledger_rows', 'temporal_rows', 'configuration_delta'}
    for role in ('A', 'B'):
        r = per_role[role]
        if type(r) is not dict or set(r) != allowed_role_keys:
            _fail('bundle_workload_metadata_rejected')
        role_samples = ctx['samples_per_producer']
        role_messages = role_samples // S
        if (r['samples'] != role_samples
                or r['messages'] != role_messages
                or r['raw_rows'] != role_samples
                or r['ledger_rows'] != role_messages
                or r['temporal_rows'] != role_messages
                or r['configuration_delta'] != 0):
            _fail('bundle_workload_metadata_rejected')


def _validate_e2_official_workload(workload):
    required = {'experiment','arm','phase','profile','workload','seed','samples_per_message',
        'producer_rates_per_second','offered_rate_per_second','measure_seconds','burst_messages',
        'expected_counts','estimate','durability_mode','generator_colocated'}
    allowed = (required, required | {'pending_limits'}, required | {'retry_policy'},
               required | {'pending_limits', 'retry_policy'})
    if (type(workload) is not dict or set(workload) not in allowed
            or workload.get('experiment') != 'E2' or workload.get('profile') != 'official'
            or workload.get('workload') != 'e2-telemetry-ab-official'
            or not isinstance(workload.get('arm'), str) or not SAFE_ID.fullmatch(workload['arm'])
            or not isinstance(workload.get('phase'), str) or not SAFE_ID.fullmatch(workload['phase'])
            or type(workload.get('seed')) is not int or not 0 <= workload['seed'] <= (1 << 63)-1
            or type(workload.get('samples_per_message')) is not int or workload['samples_per_message'] not in (1,100)
            or type(workload.get('offered_rate_per_second')) is not int
            or workload['offered_rate_per_second'] not in (100,1000,5000,20000)
            or type(workload.get('measure_seconds')) is not int or not 1 <= workload['measure_seconds'] <= 7200
            or type(workload.get('burst_messages')) is not int or not 0 <= workload['burst_messages'] <= 100000
            or workload.get('durability_mode') != 'synthetic-load'
            or workload.get('generator_colocated') not in ('unverified',True,False,'true','false')):
        _fail('bundle_workload_metadata_rejected')
    if 'retry_policy' in workload:
        try:
            from scripts.tests.paper_p8_e2_retry_policy import validate_retry_policy
        except ModuleNotFoundError:
            import importlib.util
            spec = importlib.util.spec_from_file_location('paper_p8_e2_retry_policy',
                Path(__file__).parent / 'tests' / 'paper_p8_e2_retry_policy.py')
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
            validate_retry_policy = module.validate_retry_policy
        try:
            validate_retry_policy(workload['retry_policy'],
                pending_messages=(workload.get('pending_limits') or {}).get('pending_messages', 0))
        except ValueError:
            _fail('bundle_workload_metadata_rejected')
    rates = workload['producer_rates_per_second']
    if (type(rates) is not dict or set(rates) != {'A','B'}
            or any(type(v) is not int or v < 0 for v in rates.values())
            or sum(rates.values()) != workload['offered_rate_per_second']):
        _fail('bundle_workload_metadata_rejected')
    messages = workload['offered_rate_per_second'] * workload['measure_seconds'] + workload['burst_messages']
    expected = workload['expected_counts']
    if (type(expected) is not dict or set(expected) != {'unique','raw','temporal','ack'}
            or expected != {'unique':messages,'raw':messages*workload['samples_per_message'],
                            'temporal':messages,'ack':messages}):
        _fail('bundle_workload_metadata_rejected')
    estimate = workload['estimate']
    estimate_keys = {'estimate_only','rate_per_second','duration_seconds','unique_messages_estimate',
        'raw_rows_estimate','application_payload_bytes_estimate','raw_database_row_index_wal_bytes_estimate',
        'total_storage_growth_bytes_estimate','pending_retry_memory_bytes_estimate',
        'application_bandwidth_bytes_per_second_estimate','payload_bytes_wire','payload_size_basis',
        'estimate_basis','not_measurements','retry_queue_logical_bytes_estimate','retry_queue_sizing_basis',
        'unique_offered_rate_per_producer','retry_attempt_rate_per_producer',
        'retry_attempt_burst_per_producer','fixture_shaper_attempt_rate_per_gateway',
        'fixture_shaper_attempt_burst_per_gateway','retry_headroom_not_reserved'}
    legacy_estimate_keys = estimate_keys - {'retry_queue_logical_bytes_estimate','retry_queue_sizing_basis',
        'unique_offered_rate_per_producer','retry_attempt_rate_per_producer',
        'retry_attempt_burst_per_producer','fixture_shaper_attempt_rate_per_gateway',
        'fixture_shaper_attempt_burst_per_gateway','retry_headroom_not_reserved'}
    extended_estimate = type(estimate) is dict and set(estimate) == estimate_keys
    pending = workload.get('pending_limits')
    if 'pending_limits' in workload and (type(pending) is not dict
            or set(pending) != {'pending_messages', 'pending_bytes'}
            or type(pending['pending_messages']) is not int or not 1 <= pending['pending_messages'] <= 100000
            or type(pending['pending_bytes']) is not int or not 1 <= pending['pending_bytes'] <= 1 << 30
            or 2 * (pending['pending_bytes'] + pending['pending_messages'] * 4096) > 256 << 20):
        _fail('bundle_workload_metadata_rejected')
    estimate_messages = workload['offered_rate_per_second'] * workload['measure_seconds']
    if pending is not None:
        estimate_messages += workload['burst_messages']
    estimate_rows = estimate_messages * workload['samples_per_message']
    basis = estimate.get('estimate_basis') if isinstance(estimate, dict) else None
    if (type(estimate) is not dict or set(estimate) not in (estimate_keys, legacy_estimate_keys)
            or (workload.get('retry_policy') is not None and not extended_estimate)
            or estimate.get('estimate_only') is not True or estimate.get('not_measurements') is not True
            or estimate.get('payload_bytes_wire') is not None
            or estimate.get('rate_per_second') != workload['offered_rate_per_second']
            or estimate.get('duration_seconds') != workload['measure_seconds']
            or estimate.get('payload_size_basis') != 'actual_builder_or_conservative_assumption'
            or estimate.get('unique_messages_estimate') != estimate_messages
            or estimate.get('raw_rows_estimate') != estimate_rows
            or any(type(estimate.get(k)) is not int or estimate[k] < 0 for k in (
                'application_payload_bytes_estimate','raw_database_row_index_wal_bytes_estimate',
                'total_storage_growth_bytes_estimate','pending_retry_memory_bytes_estimate',
                'application_bandwidth_bytes_per_second_estimate'))
            or type(basis) is not dict or set(basis) != {'raw_row_bytes','index_bytes_per_row','wal_multiplier'}
            or any(type(basis[k]) is not int for k in ('raw_row_bytes','index_bytes_per_row','wal_multiplier'))
            or estimate.get('application_payload_bytes_estimate') != estimate_messages * 4096
            or estimate.get('raw_database_row_index_wal_bytes_estimate') != estimate_rows * 320 * 2
            or estimate.get('total_storage_growth_bytes_estimate') != estimate_messages * 4096 + estimate_rows * 320 * 2
             or estimate.get('pending_retry_memory_bytes_estimate') != (
                2 * (pending['pending_bytes'] + pending['pending_messages'] * 4096)
                + (2 * pending['pending_messages'] * 16 if extended_estimate and workload.get('retry_policy') else 0)
                if pending is not None else (min(estimate_messages,64)+min(estimate_messages,16))*4096)
             or (extended_estimate and (
                estimate.get('retry_queue_logical_bytes_estimate') != (
                    2 * pending['pending_messages'] * 16 if pending is not None and workload.get('retry_policy') else 0)
                or estimate.get('retry_queue_sizing_basis') != '2 producers * pending_messages * 16-byte UUID slots; logical estimate, not RSS'
                or estimate.get('unique_offered_rate_per_producer') != workload['offered_rate_per_second'] / 2
                or estimate.get('retry_attempt_rate_per_producer') != (workload['retry_policy']['rate_per_second'] if workload.get('retry_policy') else 0)
                or estimate.get('retry_attempt_burst_per_producer') != (workload['retry_policy']['burst'] if workload.get('retry_policy') else 0)
                or estimate.get('fixture_shaper_attempt_rate_per_gateway') != 500
                or estimate.get('fixture_shaper_attempt_burst_per_gateway') != 500
                or estimate.get('retry_headroom_not_reserved') is not (
                    estimate.get('unique_offered_rate_per_producer') + estimate.get('retry_attempt_rate_per_producer') > 500)))
            or estimate.get('application_bandwidth_bytes_per_second_estimate') != workload['offered_rate_per_second']*4096
            or basis != {'raw_row_bytes':256,'index_bytes_per_row':64,'wal_multiplier':2}
             or estimate.get('pending_retry_memory_bytes_estimate') > 256 << 20
             or estimate.get('total_storage_growth_bytes_estimate') > 10 << 30
            or estimate_messages > 1000000 or estimate_rows > 100000000
            ):
        _fail('bundle_workload_metadata_rejected')


def _e5_context_projection(value):
    allowed = {'dataset_path', 'sha256', 'bytes', 'seed', 'anchor', 'gateway_ids',
                'sensor_ids', 'command_keys', 'viewer_subscriptions', 'workload_selection',
                'workload_selection_sha256'}
    if type(value) is not dict or set(value) != allowed:
        _fail('bundle_e5_context_rejected')
    selection = value.get('workload_selection')
    if (type(selection) is not dict or set(selection) != {'profile', 'telemetry_messages', 'initial_replay_count',
            'samples_per_message', 'command_count', 'viewer_count',
            'initial_replay_rate_per_second', 'warm_replay_prefix'}
            or selection not in ({'profile': 'pilot', 'telemetry_messages': 20, 'initial_replay_count': 20,
                'samples_per_message': 100, 'command_count': 4, 'viewer_count': 2,
                'initial_replay_rate_per_second': 1, 'warm_replay_prefix': 10},
                {'profile': 'representative', 'telemetry_messages': 8, 'initial_replay_count': 8,
                'samples_per_message': 100, 'command_count': 4, 'viewer_count': 2,
                'initial_replay_rate_per_second': 1, 'warm_replay_prefix': 2})
            or value.get('workload_selection_sha256') != hashlib.sha256(
                json.dumps(selection, sort_keys=True, separators=(',', ':')).encode()).hexdigest()):
        _fail('bundle_e5_context_rejected')
    # The source path is A-local and must never be portable metadata.
    return {key: value[key] for key in allowed if key != 'dataset_path'}


def _validate_e5_lifecycle(value):
    allowed = {'mode', 'viewer_count', 'phases', 'deadline_seconds', 'idle_transfer_excluded'}
    if type(value) is not dict or set(value) != allowed:
        _fail('bundle_e5_lifecycle_rejected')
    if (value.get('mode') != 'continuous' or value.get('viewer_count') != 2
            or value.get('phases') != ['initial', 'warm_replay', 'control']
            or type(value.get('deadline_seconds')) is not int
            or not 1 <= value['deadline_seconds'] <= MAX_E5_VIEWER_SECONDS
            or value.get('idle_transfer_excluded') is not True):
        _fail('bundle_e5_lifecycle_rejected')
    return dict(value)


def _validate_e5_workload(workload, context):
    required = {'workload_type','journal_sha256','journal_bytes','schedule','telemetry',
                'commands','subscriptions','viewer_count','security'}
    if (type(workload) is not dict or set(workload) != required | {'profile','workload_selection_sha256'}
            or workload.get('workload_type') != 'p8_e5_prebuilt_journal'
            or workload.get('journal_sha256') != context.get('sha256')
            or workload.get('journal_bytes') != context.get('bytes')
            or workload.get('viewer_count') != 2
            or workload.get('profile') != context['workload_selection']['profile']
            or workload.get('workload_selection_sha256') != context.get('workload_selection_sha256')):
        _fail('bundle_workload_metadata_rejected')
    selection = context['workload_selection']
    schedule = workload.get('schedule')
    if (type(schedule) is not dict or set(schedule) != {'initial_imported_messages','initial_replay',
            'warm_replay_prefix','replay_after_commit_ack_baseline','generated_unique'}
            or schedule.get('initial_imported_messages') != selection['telemetry_messages']
            or schedule.get('warm_replay_prefix') != selection['warm_replay_prefix']
            or schedule.get('replay_after_commit_ack_baseline') is not True
            or schedule.get('generated_unique') != 0
            or schedule.get('initial_replay') != {'rate_per_second':selection['initial_replay_rate_per_second'],
                'measure_seconds':1,'burst_messages':max(0, selection['initial_replay_count'] - 1)}):
        _fail('bundle_workload_metadata_rejected')
    telemetry = workload.get('telemetry')
    if (type(telemetry) is not dict or set(telemetry) != {'gateway_ids','sensor_ids','messages',
            'samples_per_message','sample_interval_us','dataset_anchor'}
            or telemetry.get('gateway_ids') != context.get('gateway_ids')
            or telemetry.get('sensor_ids') != context.get('sensor_ids')
            or telemetry.get('messages') != selection['telemetry_messages']
            or telemetry.get('samples_per_message') != selection['samples_per_message']
            or telemetry.get('sample_interval_us') != 10000
            or telemetry.get('dataset_anchor') != context.get('anchor')):
        _fail('bundle_workload_metadata_rejected')
    commands = workload.get('commands')
    if (type(commands) is not dict or set(commands) != {'gateway_id','command_sensor_ids','idempotency_keys',
            'count','frequencies_hz','versions','executor'}
            or commands.get('gateway_id') != 'p8-e5-a'
            or commands.get('command_sensor_ids') != ['p8-e5-command-a','p8-e5-command-b']
            or commands.get('idempotency_keys') != context.get('command_keys')
            or commands.get('count') != selection['command_count'] or commands.get('frequencies_hz') != [2,4]
            or commands.get('versions') != {'initial':0,'after_each_sensor':1}
            or commands.get('executor') != 'existing_e3_sqlite_executor'):
        _fail('bundle_workload_metadata_rejected')
    if workload.get('subscriptions') != context.get('viewer_subscriptions'):
        _fail('bundle_workload_metadata_rejected')
    security = workload.get('security')
    if (type(security) is not dict or set(security) != {'tls','jwt','acl','application_commit_ack'}
            or any(security.get(key) is not True for key in security)):
        _fail('bundle_workload_metadata_rejected')


def _validate_e5_context(context, dataset_source, manifests, arm):
    projected = _e5_context_projection(context)
    data = _source(dataset_source, limit=MAX_FILE_BYTES)
    if (not re.fullmatch(r'[0-9a-f]{64}', str(projected.get('sha256')))
            or projected.get('bytes') != len(data)
            or hashlib.sha256(data).hexdigest() != projected.get('sha256')
            or type(projected.get('seed')) is not int or projected['seed'] < 0
            or not isinstance(projected.get('anchor'), str)):
        _fail('bundle_e5_dataset_context_mismatch')
    gateway_ids = projected.get('gateway_ids')
    sensor_ids = projected.get('sensor_ids')
    command_keys = projected.get('command_keys')
    subscriptions = projected.get('viewer_subscriptions')
    if (gateway_ids != ['p8-e5-a', 'p8-e5-b'] or type(sensor_ids) is not list or len(sensor_ids) != 2
            or any(not isinstance(x, str) or not SAFE_ID.fullmatch(x) for x in sensor_ids)
            or type(command_keys) is not list or len(command_keys) != 4
            or len(set(command_keys)) != 4
            or any(not isinstance(x, str) or not re.fullmatch(
                r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', x)
                for x in command_keys)
            or type(subscriptions) is not list or len(subscriptions) != 3):
        _fail('bundle_e5_context_rejected')
    allowed_subscription_keys = {'viewer_id','user_id','gateway_id','sensor_id'}
    if any(type(row) is not dict or set(row) - allowed_subscription_keys
           or not {'viewer_id','user_id','gateway_id'} <= set(row)
           or row['gateway_id'] not in gateway_ids
           or row['viewer_id'] not in ('e5-viewer-a','e5-viewer-b')
           or row['user_id'] not in ('user-a','user-b')
           for row in subscriptions):
        _fail('bundle_e5_context_rejected')
    records = []
    try:
        for line in data.splitlines():
            if not line:
                continue
            row = json.loads(line)
            if (type(row) is not dict or set(row) != {'topic','gateway_id','sensor_id','message_id',
                    'boot_id','first_sequence','sample_count','payload_sha256','payload'}):
                _fail('bundle_e5_dataset_record_rejected')
            payload = base64.b64decode(row['payload'], validate=True)
            payload_obj = json.loads(payload)
            if (hashlib.sha256(payload).hexdigest() != row['payload_sha256']
                    or set(payload_obj) != {'protocol_version','message_id','message_type','gateway_id',
                        'sensor_id','boot_id','first_sequence','sample_count','measured_at',
                        'sample_interval_us','samples'}
                    or payload_obj.get('protocol_version') != 1
                    or payload_obj.get('message_type') != 'telemetry_batch'
                    or payload_obj.get('message_id') != row.get('message_id')
                    or payload_obj.get('gateway_id') != row.get('gateway_id')
                    or payload_obj.get('sensor_id') != row.get('sensor_id')
                    or payload_obj.get('boot_id') != row.get('boot_id')
                    or str(payload_obj.get('first_sequence')) != str(row.get('first_sequence'))
            or payload_obj.get('sample_count') != projected['workload_selection']['samples_per_message']
                    or payload_obj.get('sample_interval_us') != 10000
                    or not isinstance(payload_obj.get('samples'), list)
            or len(payload_obj['samples']) != projected['workload_selection']['samples_per_message']
                    or row.get('topic') != f"gateways/{row.get('gateway_id')}/telemetry/{row.get('sensor_id')}"
            or row.get('sample_count') != projected['workload_selection']['samples_per_message']):
                _fail('bundle_e5_dataset_record_rejected')
            records.append(row)
    except (ValueError, KeyError, TypeError, UnicodeDecodeError):
        _fail('bundle_e5_dataset_record_rejected')
    message_count = projected['workload_selection']['telemetry_messages']
    if len(records) != message_count or len({row.get('message_id') for row in records}) != message_count:
        _fail('bundle_e5_dataset_record_count_rejected')
    if any((row.get('gateway_id'), row.get('sensor_id')) not in set(zip(gateway_ids, sensor_ids))
           for row in records):
        _fail('bundle_e5_dataset_identity_rejected')
    if Path(context['dataset_path']).resolve() != Path(dataset_source).resolve():
        _fail('bundle_e5_dataset_path_mismatch')
    initial, warm, viewer, control = (manifests[k] for k in ('initial','warm_replay','viewer','control'))
    def replay_source_matches(manifest):
        journal = manifest.get('e4', {}).get('journal_path')
        if not isinstance(journal, str):
            return False
        if journal == 'datasets/dataset.jsonl':
            return True
        try:
            return hashlib.sha256(_source(journal, limit=MAX_FILE_BYTES)).hexdigest() == projected['sha256']
        except (ValueError, OSError):
            return False

    if (initial.get('e4', {}).get('mode') != 'replay'
            or initial.get('e4', {}).get('replay_count') != projected['workload_selection']['initial_replay_count']
            or initial.get('e4', {}).get('max_messages') != projected['workload_selection']['initial_replay_count']
            or warm.get('e4', {}).get('mode') != 'replay'
            or warm.get('e4', {}).get('replay_count') != projected['workload_selection']['warm_replay_prefix']
            or warm.get('e4', {}).get('max_messages') != projected['workload_selection']['warm_replay_prefix']
            or not replay_source_matches(initial) or not replay_source_matches(warm)):
        _fail('bundle_e5_replay_manifest_rejected')
    command_rows = control.get('commands')
    if (not isinstance(command_rows, list) or len(command_rows) != 4
            or [row.get('idempotency_key') for row in command_rows] != command_keys):
        _fail('bundle_e5_command_keys_mismatch')
    if viewer.get('subscriptions') != subscriptions:
        _fail('bundle_e5_viewer_subscriptions_mismatch')


def export_bundle(*, output_leaf, session_id, experiment, arm, phase, manifests, identity_files,
                  gateway_identity_files, gateway_env_files, ca_files, role_metadata, wait_seconds, workload_seconds,
                  topology, workload, run_spec=None, datasets=None, durable_probe=None,
                  e5_context=None, e5_lifecycle=None):
    """Chỉ sao chép input client có kiểu rõ ràng vào output leaf private mới.

    role_metadata do provisioner đáng tin cậy cung cấp, ánh xạ role JWT đã chọn
    tới principal ID chính xác; đây không phải bằng chứng token đã được ký.
    """
    if (not isinstance(session_id, str) or not SAFE_ID.fullmatch(session_id)
            or experiment not in ('E1','E2','E3','E4','E5') or not SAFE_ID.fullmatch(arm)
            or not SAFE_ID.fullmatch(phase)):
        _fail('bundle_identity_mismatch')
    if (type(wait_seconds) not in (int,float) or type(workload_seconds) not in (int,float)
            or not math.isfinite(wait_seconds) or not math.isfinite(workload_seconds)
            or wait_seconds < 0 or workload_seconds < 0 or wait_seconds + workload_seconds > MAX_WAIT_SECONDS):
        _fail('bundle_wait_bound_rejected')
    allowed_manifest_labels = {'viewer','telemetry','control','A','B','unique','replay','cold-replay','expiry-replay'}
    if experiment == 'E5':
        allowed_manifest_labels |= {'initial', 'warm_replay'}
    if not isinstance(manifests,dict) or not manifests or set(manifests) - allowed_manifest_labels:
        _fail('bundle_manifest_roles_rejected')
    if experiment == 'E5':
        if (set(manifests) != {'initial', 'warm_replay', 'viewer', 'control'}
                or not isinstance(datasets, dict) or set(datasets) != {'shared_journal'}
                or type(e5_context) is not dict or type(e5_lifecycle) is not dict
                or arm not in {'optimized','shaper_off','microbatch_off','ram_dedup_off','polling','fanout_scan'}):
            _fail('bundle_e5_inputs_missing')
        validated_e5_lifecycle = _validate_e5_lifecycle(e5_lifecycle)
    elif e5_context is not None or e5_lifecycle is not None:
        _fail('bundle_e5_inputs_rejected')
    else:
        validated_e5_lifecycle = None
    if (not isinstance(identity_files,dict) or not isinstance(gateway_identity_files,dict)
            or not isinstance(gateway_env_files,dict) or not isinstance(ca_files,dict)):
        _fail('bundle_inputs_rejected')
    if (set(ca_files) != {'https','mqtt'} or type(identity_files) is not dict or type(role_metadata) is not dict
            or set(role_metadata) != {'roles','gateways'}
            or not isinstance(role_metadata['roles'],dict) or set(role_metadata['roles']) != set(identity_files)):
        _fail('bundle_required_inputs_missing')
    if (type(topology) is not dict or set(topology)-{'gateways','sensors','sensors_per_gateway','users','viewers'}
            or any(type(x) is not int or x < 0 for x in topology.values())):
        _fail('bundle_topology_metadata_rejected')
    workload_keys={'experiment','arm','run_id','cache_mode','samples_per_message','unique_batches','rate_per_second',
        'measure_seconds','generator_colocated','profile','workload','seed','phase','offered_messages',
        'dispatch_mode','expiry_replay_enabled','expiry_required_lifetime_seconds'}
    if experiment == 'E5':
        e5_workload_keys = {'workload_type','profile','workload_selection_sha256','journal_sha256',
                            'journal_bytes','schedule','telemetry','commands','subscriptions','viewer_count','security'}
        if (type(workload) is not dict or set(workload) != e5_workload_keys
                or workload.get('workload_type') != 'p8_e5_prebuilt_journal'
                or workload.get('journal_sha256') != e5_context.get('sha256')
                or workload.get('journal_bytes') != e5_context.get('bytes')
                or workload.get('viewer_count') != 2
                or type(workload.get('schedule')) is not dict
                or type(workload.get('telemetry')) is not dict
                or type(workload.get('commands')) is not dict
                or type(workload.get('subscriptions')) is not list
                or type(workload.get('security')) is not dict):
            _fail('bundle_workload_metadata_rejected')
        _validate_e5_workload(workload, e5_context)
    elif experiment == 'E2' and isinstance(workload, dict) and workload.get('profile') == 'official':
        _validate_e2_official_workload(workload)
    elif experiment == 'E2' and isinstance(workload, dict) and (
            workload.get('workload_type') == 'equal_samples' or workload.get('workload') == 'equal_samples'):
        _validate_e2_equal_samples_workload(workload)
    elif experiment == 'E1' and isinstance(workload, dict):
        e1_workload_keys = workload_keys | {'viewer_count', 'principal_count', 'density',
                                             'expected_deliveries_by_target'}
        if (set(workload) - e1_workload_keys
                or any(type(x) not in (str, int, float, bool) or type(x) is float and not math.isfinite(x)
                       for key, x in workload.items() if key != 'expected_deliveries_by_target')
                or ('expected_deliveries_by_target' in workload
                    and (type(workload['expected_deliveries_by_target']) is not dict
                         or any(not isinstance(k, str) or type(v) is not int or v < 0
                                for k, v in workload['expected_deliveries_by_target'].items())))):
            _fail('bundle_workload_metadata_rejected')
    elif (type(workload) is not dict or set(workload)-workload_keys
            or any(type(x) not in (str,int,float,bool) or type(x) is float and not math.isfinite(x)
                   for x in workload.values())):
        _fail('bundle_workload_metadata_rejected')
    if experiment == 'E4' and ('expiry_replay_enabled' in workload
            and (type(workload['expiry_replay_enabled']) is not bool
                 or workload['expiry_replay_enabled'] != ('expiry-replay' in manifests)
                 or workload['expiry_replay_enabled'] and workload.get('cache_mode') != 'on')):
        _fail('bundle_e4_expiry_metadata_rejected')
    _public_metadata(topology)
    leaf = Path(output_leaf)
    if leaf.exists() or leaf.is_symlink(): _fail('bundle_output_leaf_exists')
    parent = leaf.parent
    if not parent.is_dir() or parent.is_symlink(): _fail('bundle_output_parent_rejected')
    root = leaf
    root.mkdir(mode=0o700)
    os.chmod(root,0o700)
    try:
        inventory=[]; manifest_refs={}; manifest_values={}; jwt_expiries=[]; copied_identity_refs={}
        selected_profile = workload.get('profile', 'pilot') if isinstance(workload, dict) else 'pilot'
        if selected_profile not in ('pilot', 'representative', 'official'):
            _fail('bundle_profile_mismatch')
        _copy_input(root, ca_files['https'], 'certs/https-ca.pem', inventory, 'public_ca')
        _copy_input(root, ca_files['mqtt'], 'certs/mqtt-ca.pem', inventory, 'public_ca')
        for role_key, source in identity_files.items():
            role=role_key.split(':')[-1]
            role_info=role_metadata['roles'].get(role_key)
            if (role not in ('viewer','telemetry','control') or type(role_info) is not dict
                    or set(role_info) != {'role','principals'} or role_info.get('role') != role):
                _fail('bundle_identity_role_unverified')
            value=_json_file(source,limit=65536)
            if type(value) is not dict: _fail('bundle_identity_rejected')
            selected=role_info.get('principals')
            if (not isinstance(selected,list) or any(not isinstance(x,str) for x in selected)
                    or (role != 'telemetry' and not selected) or set(value) != set(selected)):
                _fail('bundle_identity_principals_mismatch')
            role_jwts=list(_collect_jwts(value))
            if role != 'telemetry' and not role_jwts: _fail('bundle_identity_missing')
            for path, token in role_jwts: jwt_expiries.append((path,_jwt_expiry(token)))
            rel=f'private/identities/{role_key.replace(":","-")}.json'
            _copy_input(root,source,rel,inventory,'role_identity')
            copied_identity_refs[role_key]=rel
        if type(role_metadata['gateways']) is not dict or set(role_metadata['gateways']) != set(gateway_identity_files):
            _fail('bundle_gateway_role_unverified')
        gateway_refs={}
        for gateway_id, source in gateway_identity_files.items():
            if not SAFE_ID.fullmatch(gateway_id): _fail('bundle_gateway_id_rejected')
            credential=_json_file(source,limit=65536)
            metadata=role_metadata['gateways'].get(gateway_id) if isinstance(role_metadata['gateways'],dict) else None
            if (type(credential) is not dict or set(credential) != {'username','password'}
                    or not isinstance(credential.get('username'),str) or not credential['username']
                    or len(credential['username']) > 128 or any(ord(c)<0x20 for c in credential['username'])
                    or not isinstance(credential.get('password'),str) or not credential['password']
                    or len(credential['password']) > 1024 or any(c in credential['password'] for c in '\x00\r\n')
                    or type(metadata) is not dict or set(metadata) != {'role','gateway_id','username'}
                    or metadata.get('role') != 'gateway' or metadata.get('gateway_id') != gateway_id
                    or metadata.get('username') != credential['username']):
                _fail('bundle_gateway_role_unverified')
            rel=f'private/gateways/{gateway_id}.json'
            _copy_input(root,source,rel,inventory,'gateway_credential')
            gateway_refs[gateway_id]=rel
        if experiment == 'E1':
            e1_lifecycle = _derive_e1_lifecycle(manifests)
            effective_workload_seconds = max(workload_seconds, e1_lifecycle)
        elif experiment == 'E5':
            effective_workload_seconds = max(workload_seconds, validated_e5_lifecycle['deadline_seconds'])
        else:
            effective_workload_seconds = workload_seconds

        if (type(effective_workload_seconds) not in (int, float)
                or not math.isfinite(effective_workload_seconds)
                or effective_workload_seconds < 0
                or wait_seconds + effective_workload_seconds > MAX_WAIT_SECONDS):
            _fail('bundle_wait_bound_rejected')

        expiry_required = workload.get('expiry_required_lifetime_seconds') if isinstance(workload, dict) else None
        if expiry_required is not None:
            if (experiment != 'E4' or type(expiry_required) not in (int, float)
                    or not math.isfinite(expiry_required) or expiry_required < 0):
                _fail('bundle_expiry_budget_rejected')
            required_lifetime = max(wait_seconds + effective_workload_seconds,
                                    expiry_required)
        else:
            required_lifetime = wait_seconds + effective_workload_seconds
        now = time.time()
        if any(exp - now < required_lifetime for _, exp in jwt_expiries):
            _fail('bundle_identity_near_expiry')

        referenced_gateways=set(); referenced_identities=set()
        for label, source in manifests.items():
            raw=_json_file(source,limit=MAX_MANIFEST_BYTES)
            if (experiment == 'E5' and label in ('initial','warm_replay','viewer')
                    and isinstance(raw, dict) and raw.get('mode') == 'local'):
                # E5 preparation uses the existing local manifest constructors;
                # its TLS endpoints are already remote-safe. The portable
                # counterpart must select paperload's remote transport mode.
                raw = dict(raw, mode='remote')
            if experiment == 'E5' and label == 'viewer' and isinstance(raw, dict):
                deadline = validated_e5_lifecycle['deadline_seconds']
                phases = dict(raw.get('phases') or {})
                phases['measure_seconds'] = deadline
                limits = dict(raw.get('limits') or {})
                limits['wall_seconds'] = max(limits.get('wall_seconds', 0), deadline + 30)
                raw = dict(raw, phases=phases, limits=limits)
            role=(raw.get('roles') or [None])[0] if isinstance(raw,dict) else None
            expected=role if label in ('viewer','telemetry','control') else 'telemetry'
            expected_manifest_experiment=('E4' if experiment == 'E5' and label in ('initial','warm_replay') else
                'E1' if experiment == 'E5' and label == 'viewer' else
                'E3' if experiment == 'E5' and label == 'control' else
                'E1' if (experiment=='E1' or experiment=='E4') and label=='viewer' else
                'E2' if experiment=='E1' and label=='telemetry' else
                'E2' if experiment=='E2' and label in ('A','B') else
                'E3' if experiment=='E3' and label=='control' else
                'E2' if experiment=='E3' and label in ('A','B') else
                'E4' if experiment=='E4' and label in ('viewer','unique','replay','cold-replay','expiry-replay') else None)
            expected_arm=('control' if experiment == 'E5' and label == 'control' else
                          'warm' if experiment == 'E5' else
                          label if experiment=='E2' and label in ('A','B') else
                          'control' if experiment=='E3' and label=='control' else
                          label if experiment=='E3' and label in ('A','B') else arm)
            _strict_manifest(raw,expected_manifest_experiment,expected_arm)
            expected_manifest_profile = 'pilot' if experiment == 'E5' else selected_profile
            # E3 official denotes the 30-command control schedule; its reused E2
            # telemetry background remains the established pilot manifest.
            if experiment == 'E3' and selected_profile == 'official' and label in ('A', 'B'):
                expected_manifest_profile = 'pilot'
            if raw.get('profile') != expected_manifest_profile:
                _fail('bundle_profile_mismatch')
            if raw.get('roles') != [expected]: _fail('bundle_manifest_role_mismatch')
            if (type(raw.get('identity_files')) is not dict or set(raw['identity_files']) != {expected}
                    or expected == 'telemetry' and not raw.get('gateway_identity_files')):
                _fail('bundle_manifest_identity_references_missing')
            identity_key=f'{label}:{expected}' if f'{label}:{expected}' in copied_identity_refs else expected
            if identity_key not in copied_identity_refs: _fail('bundle_identity_reference_missing')
            referenced_identities.add(identity_key)
            if not set(raw.get('gateway_identity_files',{})) <= set(gateway_identity_files):
                _fail('bundle_gateway_identity_missing')
            referenced_gateways.update(raw.get('gateway_identity_files',{}))
            manifest_values[label]=raw
            manifest_refs[label]=f'manifests/{label}.json'
        if referenced_gateways != set(gateway_identity_files): _fail('bundle_gateway_identity_missing')
        if referenced_identities != set(copied_identity_refs): _fail('bundle_identity_unreferenced_input')
        if experiment == 'E2' and (selected_profile == 'official' or 'retry_policy' in workload):
            policies = [manifest_values[label].get('retry_policy') for label in ('A', 'B')]
            if policies[0] != policies[1] or workload.get('retry_policy') != policies[0]:
                _fail('bundle_retry_policy_mismatch')

        if set(ca_files) != {'https','mqtt'}: _fail('bundle_ca_rejected')
        rewritten={}
        for label, raw in manifest_values.items():
            item=json.loads(json.dumps(raw))
            tls=item.get('tls')
            if not isinstance(tls,dict) or not tls.get('https_endpoint'):
                _fail('bundle_tls_inputs_missing')
            _endpoint(tls['https_endpoint'],'https')
            tls['https_ca_file']='certs/https-ca.pem'
            if item['experiment'] != 'E3' or item['roles'] != ['control']:
                if not tls.get('mqtt_tls_endpoint'): _fail('bundle_tls_inputs_missing')
                _endpoint(tls['mqtt_tls_endpoint'],'mqtts')
                tls['mqtt_tls_ca_file']='certs/mqtt-ca.pem'
            for role, ref in item.get('identity_files',{}).items():
                identity_key=f'{label}:{role}' if f'{label}:{role}' in copied_identity_refs else role
                if identity_key not in copied_identity_refs: _fail('bundle_identity_reference_missing')
                item['identity_files'][role]=copied_identity_refs[identity_key]
            for gateway_id, ref in item.get('gateway_identity_files',{}).items():
                mapping=gateway_refs.get(gateway_id)
                if mapping is None: _fail('bundle_gateway_identity_missing')
                item['gateway_identity_files'][gateway_id]=mapping
            if item['roles'] == ['control']:
                item.pop('bundle_path', None)
            else:
                item['bundle_path']='.'
            if isinstance(item.get('e4'),dict) and item['e4'].get('journal_path'):
                item['e4']['journal_path'] = ('datasets/dataset.jsonl' if experiment == 'E5'
                                               else 'outputs/e4-journal.sqlite')
            rel=manifest_refs[label]
            _write(root,rel,json.dumps(item,sort_keys=True,separators=(',',':')).encode())
            manifest_data = (root / rel).read_bytes()
            _record_inventory(inventory, rel, 'manifest', manifest_data)
            rewritten[label]=item

        env_refs = {}
        executor_sensors = set()
        executor_clients = set()
        for executor_label, source in gateway_env_files.items():
            if not isinstance(executor_label, str) or not SAFE_ID.fullmatch(executor_label):
                _fail('bundle_executor_label_rejected')
            env=_json_file(source,limit=65536)
            allowed={'GATEWAY_ID','SENSOR_ID','GATEWAY_CONTROL_ENABLED','GATEWAY_MQTT_BROKER_URL',
                'GATEWAY_MQTT_CLIENT_ID','GATEWAY_MQTT_USERNAME','GATEWAY_MQTT_PASSWORD',
                'GATEWAY_MQTT_PASSWORD_FILE','GATEWAY_MQTT_CA_FILE','GATEWAY_MQTT_SERVER_NAME',
                'GATEWAY_OUTBOX_PATH','GATEWAY_SAMPLE_RATE_HZ','GATEWAY_SAMPLES_PER_BATCH',
                'GATEWAY_ACK_TIMEOUT','GATEWAY_RETRY_MIN_DELAY','GATEWAY_RETRY_MAX_DELAY',
                'GATEWAY_MAX_INFLIGHT','GATEWAY_OUTBOX_MAX_ROWS','GATEWAY_OUTBOX_MAX_BYTES',
                'GATEWAY_CONNECT_TIMEOUT','GATEWAY_SUBSCRIBE_TIMEOUT','GATEWAY_PUBLISH_TIMEOUT',
                'GATEWAY_SQLITE_TIMEOUT','GATEWAY_SHUTDOWN_TIMEOUT','GATEWAY_GENERATION_INTERVAL'}
            required_env = {
                'GATEWAY_ID', 'SENSOR_ID', 'GATEWAY_CONTROL_ENABLED',
                'GATEWAY_MQTT_BROKER_URL', 'GATEWAY_MQTT_CLIENT_ID',
                'GATEWAY_MQTT_USERNAME', 'GATEWAY_MQTT_PASSWORD',
                'GATEWAY_MQTT_CA_FILE', 'GATEWAY_OUTBOX_PATH',
            }
            if type(env) is not dict or set(env)-allowed or not required_env <= set(env):
                _fail('bundle_gateway_env_rejected')
            if any(not isinstance(v,str) or not v or '\x00' in v for v in env.values()):
                _fail('bundle_gateway_env_rejected')
            if env.get('GATEWAY_MQTT_PASSWORD_FILE') or not env.get('GATEWAY_MQTT_PASSWORD'):
                _fail('bundle_gateway_password_source_rejected')
            gateway_id = env.get('GATEWAY_ID')
            if gateway_id not in gateway_identity_files:
                _fail('bundle_gateway_identity_missing')
            credential=_json_file(gateway_identity_files[gateway_id],limit=65536)
            if (env.get('GATEWAY_MQTT_USERNAME') != credential.get('username')
                    or env.get('GATEWAY_MQTT_PASSWORD') != credential.get('password')
                    or env.get('GATEWAY_CONTROL_ENABLED') != 'true'
                    or not env.get('GATEWAY_MQTT_CLIENT_ID') or not env.get('GATEWAY_OUTBOX_PATH')):
                _fail('bundle_gateway_identity_mismatch')
            client_id = env['GATEWAY_MQTT_CLIENT_ID']
            sensor_key = (gateway_id, env['SENSOR_ID'])
            if (client_id in executor_clients or sensor_key in executor_sensors
                    or gateway_id not in referenced_gateways):
                _fail('bundle_executor_collision')
            executor_clients.add(client_id)
            executor_sensors.add(sensor_key)
            env.pop('GATEWAY_MQTT_PASSWORD')
            password_rel = f'private/gateway-passwords/{executor_label}.txt'
            env['GATEWAY_MQTT_PASSWORD_FILE'] = password_rel
            env['GATEWAY_MQTT_CA_FILE'] = 'certs/mqtt-ca.pem'
            env['GATEWAY_OUTBOX_PATH'] = f'outputs/outbox/{executor_label}.sqlite'
            password_src=json.loads(_source(source,limit=65536))['GATEWAY_MQTT_PASSWORD']
            _write(root, password_rel, password_src.encode())
            _record_inventory(inventory, password_rel, 'gateway_credential', password_src.encode())
            rel = f'gateway-env/{executor_label}.json'
            data = json.dumps(env, sort_keys=True, separators=(',', ':')).encode()
            _write(root, rel, data)
            _record_inventory(inventory, rel, 'gateway_environment', data)
            env_refs[executor_label] = rel

        if experiment == 'E5':
            _validate_e5_context(e5_context, datasets['shared_journal'], manifest_values, arm)
            _copy_input(root, datasets['shared_journal'], 'datasets/dataset.jsonl', inventory, 'dataset')
            context = _e5_context_projection(e5_context)
            context_data = json.dumps(context, sort_keys=True, separators=(',', ':')).encode()
            _write(root, 'contexts/e5-context.json', context_data)
            _record_inventory(inventory, 'contexts/e5-context.json', 'e5_context', context_data)
            lifecycle = validated_e5_lifecycle
            phase_data = json.dumps({'schema_version': 1, 'phases': ['initial','warm_replay','control'],
                'viewer_lifecycle': lifecycle}, sort_keys=True, separators=(',', ':')).encode()
            _write(root, 'execution/e5-phase-plan.json', phase_data)
            _record_inventory(inventory, 'execution/e5-phase-plan.json', 'e5_phase_plan', phase_data)
        elif datasets:
            _fail('bundle_dataset_reference_unsupported')
        (root/'outputs').mkdir(mode=0o700)
        os.chmod(root/'outputs',0o700)

        spec={'paperload':[],'gateways':[],'stop_timeout_seconds':30}
        if run_spec:
            supplied=_json_file(run_spec,limit=1<<20)
            if type(supplied) is not dict or set(supplied)-RUN_SPEC_KEYS:
                _fail('bundle_run_spec_rejected')
            if type(supplied.get('paperload')) is not list or type(supplied.get('gateways',[])) is not list:
                _fail('bundle_run_spec_rejected')
            spec_path = Path(run_spec).resolve()
            spec_parent = spec_path.parent

            def resolve_source_reference(value):
                if not isinstance(value, str) or not value:
                    _fail('bundle_run_spec_reference_rejected')
                candidate = Path(value)
                if not candidate.is_absolute():
                    candidate = spec_parent / candidate
                return str(candidate.resolve())

            source_by_manifest = {
                str(Path(source_path).resolve()): label
                for label, source_path in manifests.items()
            }
            for job in supplied['paperload']:
                required_job_keys = {'manifest', 'output_dir'}
                if (type(job) is not dict
                        or set(job) - {'manifest', 'output_dir', 'role', 'ready_timeout_seconds'}
                        or not required_job_keys <= set(job)):
                    _fail('bundle_run_spec_rejected')
                if not isinstance(job['output_dir'], str) or not job['output_dir']:
                    _fail('bundle_run_spec_rejected')
                label = source_by_manifest.get(resolve_source_reference(job['manifest']))
                if label is None or job.get('role',rewritten[label]['roles'][0]) != rewritten[label]['roles'][0]:
                    _fail('bundle_run_spec_reference_rejected')
                timeout=job.get('ready_timeout_seconds',90)
                if type(timeout) is bool or type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout <= 0:
                    _fail('bundle_run_spec_rejected')
                job_role = job.get('role', rewritten[label]['roles'][0])
                is_e1_viewer = (
                    experiment == 'E1'
                    and _is_e1_viewer_manifest(rewritten[label], job_role)
                )
                max_timeout = MAX_E1_VIEWER_READINESS_SECONDS if is_e1_viewer else MAX_READINESS_SECONDS
                if timeout > max_timeout:
                    _fail('bundle_run_spec_rejected')
                spec['paperload'].append({'manifest':manifest_refs[label],
                    'output_dir':f'outputs/{label}', 'role':rewritten[label]['roles'][0],
                    'ready_timeout_seconds':timeout})
            source_by_env = {
                str(Path(source_path).resolve()): executor_label
                for executor_label, source_path in gateway_env_files.items()
            }
            for gateway in supplied.get('gateways',[]):
                if (type(gateway) is not dict or set(gateway)-{'env_file','role','ready_timeout_seconds'}
                        or 'env_file' not in gateway): _fail('bundle_run_spec_rejected')
                executor_label = source_by_env.get(resolve_source_reference(gateway['env_file']))
                if executor_label is None:
                    _fail('bundle_run_spec_reference_rejected')
                role=gateway.get('role','executor'); timeout=gateway.get('ready_timeout_seconds',30)
                if (role not in ('executor','durable-telemetry') or type(timeout) not in (int,float)
                        or not math.isfinite(timeout) or not 0 < timeout <= 60):
                    _fail('bundle_run_spec_rejected')
                spec['gateways'].append({
                    'env_file': env_refs[executor_label],
                    'role': role,
                    'ready_timeout_seconds': timeout,
                })
            stop=supplied.get('stop_timeout_seconds',30)
            max_stop = 300 if experiment == 'E2' else 60
            if type(stop) is not int or not 1 <= stop <= max_stop: _fail('bundle_run_spec_rejected')
            spec['stop_timeout_seconds']=stop
            # The guard is always enabled for portable runs. The growth limit is
            # derived from the selected manifests; paths are reconstructed locally
            # from owned job/output and Gateway outbox references at bind/run time.
            growth_limits = [m.get('limits', {}).get('disk_growth_bytes') for m in rewritten.values()]
            growth_limits = [v for v in growth_limits if type(v) is int and v > 0]
            if not growth_limits:
                _fail('bundle_disk_guard_budget_missing')
            supplied_guard = supplied.get('disk_guard')
            if supplied_guard is not None and (type(supplied_guard) is not dict or
                    set(supplied_guard) - {'growth_threshold_bytes','minimum_free_bytes','interval_seconds','max_samples'}):
                _fail('bundle_disk_guard_rejected')
            growth = min(growth_limits)
            free = (supplied_guard or {}).get('minimum_free_bytes', 64 << 20)
            interval = (supplied_guard or {}).get('interval_seconds', 1.0)
            max_samples = (supplied_guard or {}).get('max_samples', 128)
            if (type(free) is not int or free < 0 or free > 1 << 40
                    or type(interval) not in (int,float) or not math.isfinite(interval) or not .1 <= interval <= 60
                    or type(max_samples) is not int or not 2 <= max_samples <= 4096
                    or (supplied_guard is not None and supplied_guard.get('growth_threshold_bytes', growth) != growth)):
                _fail('bundle_disk_guard_rejected')
            spec['disk_guard'] = {'growth_threshold_bytes': growth, 'minimum_free_bytes': free,
                                  'interval_seconds': interval, 'max_samples': max_samples}
            if experiment != 'E5' and {job['manifest'] for job in spec['paperload']} != set(manifest_refs.values()):
                _fail('bundle_run_spec_reference_rejected')
            if experiment != 'E5' and {job['env_file'] for job in spec['gateways']} != set(env_refs.values()):
                _fail('bundle_run_spec_reference_rejected')
            if experiment == 'E5':
                # Inventory every role input. The E5 phase plan, not this
                # inventory, selects which foreground workload is run.
                spec['paperload'] = [
                    {'manifest': manifest_refs[label], 'output_dir': f'outputs/{label}',
                     'role': rewritten[label]['roles'][0], 'ready_timeout_seconds': 90}
                    for label in ('initial','warm_replay','viewer','control')]
                spec['gateways'] = [
                    {'env_file': env_refs[label], 'role': 'executor', 'ready_timeout_seconds': 30}
                    for label in sorted(env_refs)]
        else:
            for label, rel in manifest_refs.items():
                role=rewritten[label]['roles'][0]
                spec['paperload'].append({'manifest':rel,'output_dir':f'outputs/{label}',
                    'role':role,'ready_timeout_seconds':90})
            for gateway_id, rel in env_refs.items():
                spec['gateways'].append({'env_file':rel,'role':'durable-telemetry','ready_timeout_seconds':30})
        if 'disk_guard' not in spec:
            growth_limits = [value.get('limits', {}).get('disk_growth_bytes') for value in rewritten.values()]
            growth_limits = [value for value in growth_limits if type(value) is int and value > 0]
            if not growth_limits:
                _fail('bundle_disk_guard_budget_missing')
            spec['disk_guard'] = {'growth_threshold_bytes': min(growth_limits),
                                  'minimum_free_bytes': 64 << 20,
                                  'interval_seconds': 1.0, 'max_samples': 128}
        spec_data=json.dumps(spec,sort_keys=True,separators=(',',':')).encode()
        _write(root,'execution/run-spec.json',spec_data)
        _record_inventory(inventory, 'execution/run-spec.json', 'run_spec', spec_data)
        first_manifest=next(iter(rewritten.values()))
        durable_ref = None
        if durable_probe is not None:
            required_probe = {'gateway_id', 'sensor_id', 'username', 'mqtt_server_name',
                              'sample_interval_us', 'max_rows', 'max_bytes', 'shutdown_timeout_seconds'}
            if experiment != 'E4' or type(durable_probe) is not dict or set(durable_probe) != required_probe:
                _fail('bundle_durable_probe_rejected')
            gateway_id = durable_probe['gateway_id']
            if (gateway_id not in gateway_refs or durable_probe['sensor_id'] != 'p8-e4-durable'
                    or durable_probe['username'] != role_metadata['gateways'][gateway_id]['username']
                    or durable_probe['sample_interval_us'] != 10000
                    or any(type(durable_probe[k]) is not int for k in
                           ('sample_interval_us', 'max_rows', 'max_bytes', 'shutdown_timeout_seconds'))
                    or not 1 <= durable_probe['max_rows'] <= 64
                    or not 1024 <= durable_probe['max_bytes'] <= 1 << 20
                    or not 1 <= durable_probe['shutdown_timeout_seconds'] <= 30
                    or not isinstance(durable_probe['mqtt_server_name'], str)
                    or not SAFE_ID.fullmatch(durable_probe['mqtt_server_name'])):
                _fail('bundle_durable_probe_rejected')
            durable_ref = {'schema_version': 1, 'experiment': 'E4', 'session_id': session_id,
                'run_id': manifest_values['unique']['run_id'], 'gateway_id': gateway_id,
                'sensor_id': durable_probe['sensor_id'], 'username': durable_probe['username'],
                'password_ref': gateway_refs[gateway_id], 'ca_ref': 'certs/mqtt-ca.pem',
                'mqtt_endpoint': manifest_values['unique']['tls']['mqtt_tls_endpoint'],
                'mqtt_server_name': durable_probe['mqtt_server_name'],
                'sample_interval_us': 10000, 'samples_per_batch': 100,
                'max_rows': durable_probe['max_rows'], 'max_bytes': durable_probe['max_bytes'],
                'shutdown_timeout_seconds': durable_probe['shutdown_timeout_seconds'],
                'outbox_ref': 'outputs/outbox/e4-durable.sqlite'}
            data = json.dumps(durable_ref, sort_keys=True, separators=(',', ':')).encode()
            _write(root, 'durable-probe.json', data)
            _record_inventory(inventory, 'durable-probe.json', 'durable_probe', data)
        portable_workload = workload
        if experiment == 'E5':
            portable_workload = dict(workload)
            portable_workload['subscriptions'] = [
                {key: row[key] for key in ('viewer_id','gateway_id','sensor_id') if key in row}
                for row in workload['subscriptions'] if isinstance(row, dict)]
            portable_workload['security'] = {
                key: value for key, value in workload['security'].items()
                if key in {'tls','acl','application_commit_ack'}
            }
        _public_metadata(portable_workload)
        metadata={'schema_version':SCHEMA_VERSION,'contract':'p8-portable-client-inputs-only',
            'path_reference_base':'bundle-root',
            'session':{'session_id':session_id,'run_ids':sorted({
                value['run_id'] for value in rewritten.values()}),
                'experiment':experiment,'arm':arm,'phase':phase,'profile':selected_profile},
            'topology':topology,'workload':portable_workload,
            'endpoints':{'https':first_manifest['tls']['https_endpoint'],
                         'mqtt_tls':first_manifest['tls'].get('mqtt_tls_endpoint')},
            'inputs':inventory,'immutable_paths':[x['path'] for x in inventory],
            'mutable_outputs':['outputs/'],'identity_expiry_earliest':min((x[1] for x in jwt_expiries),default=None),
            'created_at_unix':time.time(),'wait_seconds':wait_seconds,
            'workload_seconds':effective_workload_seconds,
            'additional_token_lifetime_seconds':max(0, required_lifetime - wait_seconds - effective_workload_seconds),
            'integrity_note':'SHA-256 chỉ để đối chiếu/tương quan, không phải chữ ký hay bằng chứng nguồn tin cậy.'}
        if experiment == 'E5':
            metadata['e5_context_ref'] = 'contexts/e5-context.json'
            metadata['e5_dataset_ref'] = 'datasets/dataset.jsonl'
            metadata['e5_phase_plan_ref'] = 'execution/e5-phase-plan.json'
        if durable_ref is not None:
            metadata['durable_probe'] = durable_ref
        meta_data=json.dumps(metadata,sort_keys=True,separators=(',',':')).encode()
        _write(root,'bundle.json',meta_data)
        return {'bundle_root':str(root),'bundle_json':'bundle.json','file_count':len(inventory),
                'total_bytes':sum(x['size_bytes'] for x in inventory),'identity_expiry_earliest':metadata['identity_expiry_earliest']}
    except BaseException:
        # Only remove the newly-created leaf owned by this invocation.
        shutil.rmtree(root)
        raise


def bind_bundle(*, bundle_root, workspace_leaf, elapsed_transfer_wait_seconds=0):
    """Bind portable root-relative references into one new private B workspace."""
    source_root = Path(bundle_root)
    inspection = inspect_bundle(source_root)
    metadata = _json_file(source_root / 'bundle.json', limit=1 << 20)
    if (type(elapsed_transfer_wait_seconds) not in (int, float)
            or not math.isfinite(elapsed_transfer_wait_seconds)
            or elapsed_transfer_wait_seconds < 0
            or elapsed_transfer_wait_seconds > MAX_WAIT_SECONDS):
        _fail('bundle_transfer_wait_rejected')
    workspace = Path(workspace_leaf)
    if workspace.exists() or workspace.is_symlink():
        _fail('workspace_output_leaf_exists')
    if not workspace.parent.is_dir() or any(part.is_symlink() for part in (workspace.parent, *workspace.parent.parents)):
        _fail('workspace_output_parent_rejected')
    workspace.mkdir(mode=0o700)
    os.chmod(workspace, 0o700)
    try:
        bundle_metadata_bytes = (source_root / 'bundle.json').read_bytes()
        _write(workspace, 'runtime/bundle-metadata.json', bundle_metadata_bytes)
        inventory = {row['path']: row for row in metadata['inputs']}
        for rel, row in inventory.items():
            data = _source(source_root / rel, limit=MAX_FILE_BYTES)
            if hashlib.sha256(data).hexdigest() != row['sha256']:
                _fail('bundle_checksum_mismatch')
            _write(workspace, f'inputs/{rel}', data)
        # Bound E5 replay journal retains owner-only 0600 written by _write, matching
        # Go checkPrivateRegularFile (src/internal/paperload/e4_journal.go: st.Mode().Perm() == 0600).
        # Logical input immutability is verified cryptographically by SHA-256 digest on every
        # client command invocation, not by a POSIX 0400 permission override.

        bound_durable_probe = None
        if 'durable_probe' in metadata:
            probe = _json_file(workspace / 'inputs/durable-probe.json', limit=65536)
            if (type(probe) is not dict or probe != metadata['durable_probe']
                    or probe.get('schema_version') != 1 or probe.get('experiment') != 'E4'
                    or probe.get('session_id') != metadata['session']['session_id']
                    or probe.get('run_id') not in metadata['session']['run_ids']):
                _fail('bundle_durable_probe_rejected')
            password_rel = _safe_ref(probe.get('password_ref'))
            ca_rel = _safe_ref(probe.get('ca_ref'))
            if (inventory.get(password_rel, {}).get('kind') != 'gateway_credential'
                    or inventory.get(ca_rel, {}).get('kind') != 'public_ca'
                    or probe.get('outbox_ref') != 'outputs/outbox/e4-durable.sqlite'):
                _fail('bundle_durable_probe_reference_rejected')
            bound_durable_probe = {**probe,
                'password_file': str((workspace / 'inputs' / password_rel).resolve()),
                'ca_file': str((workspace / 'inputs' / ca_rel).resolve()),
                'outbox_path': str((workspace / 'outputs/outbox/e4-durable.sqlite').resolve())}
            for key in ('password_ref', 'ca_ref', 'outbox_ref'):
                bound_durable_probe.pop(key, None)
            _write(workspace, 'runtime/durable-probe.json',
                   json.dumps(bound_durable_probe, sort_keys=True, separators=(',', ':')).encode())

        elapsed = max(elapsed_transfer_wait_seconds, time.time() - metadata['created_at_unix'])
        effective_bound_workload = metadata['workload_seconds']
        if metadata.get('session', {}).get('experiment') == 'E1':
            manifest_inputs = {}
            for rel, row in inventory.items():
                if row['kind'] != 'manifest':
                    continue
                source = workspace / 'inputs' / rel
                roles = _json_file(source, limit=MAX_MANIFEST_BYTES).get('roles')
                if type(roles) is list and len(roles) == 1:
                    if roles[0] in manifest_inputs:
                        _fail('bundle_e1_lifecycle_rejected')
                    manifest_inputs[roles[0]] = source
            bound_e1_lifecycle = _derive_e1_lifecycle(manifest_inputs)
            if effective_bound_workload < bound_e1_lifecycle:
                _fail('bundle_workload_budget_rejected')
            effective_bound_workload = max(effective_bound_workload, bound_e1_lifecycle)

        required_lifetime = (
            max(0.0, metadata['wait_seconds'] - elapsed) + effective_bound_workload
            + metadata.get('additional_token_lifetime_seconds', 0)
        )
        expiries = []
        identity_files = [
            rel for rel, row in inventory.items() if row['kind'] == 'role_identity'
        ]
        for rel in identity_files:
            identity_map = _json_file(workspace / 'inputs' / rel, limit=65536)
            for _, jwt in _collect_jwts(identity_map):
                expiries.append(_jwt_expiry(jwt))
        if any(expiry - time.time() < required_lifetime for expiry in expiries):
            _fail('bundle_identity_near_expiry')

        for name in ('outputs', 'outputs/outbox'):
            path = workspace / name
            path.mkdir(mode=0o700)
            os.chmod(path, 0o700)

        manifest_paths = sorted(
            rel for rel, row in inventory.items() if row['kind'] == 'manifest'
        )
        gateway_env_paths = sorted(
            rel for rel, row in inventory.items() if row['kind'] == 'gateway_environment'
        )
        manifest_map = {rel: workspace / 'runtime' / rel for rel in manifest_paths}
        env_map = {rel: workspace / 'runtime' / rel for rel in gateway_env_paths}
        labels = {PurePosixPath(rel).stem: rel for rel in manifest_paths}
        gateway_labels = {PurePosixPath(rel).stem: rel for rel in gateway_env_paths}
        executor_secret_labels = {
            PurePosixPath(rel).stem for rel, row in inventory.items()
            if row['kind'] == 'gateway_credential' and rel.startswith('private/gateway-passwords/')
        }
        if executor_secret_labels != set(gateway_labels):
            _fail('bundle_executor_inventory_mismatch')

        bound_manifest_hashes = {}
        for rel in manifest_paths:
            raw = _json_file(workspace / 'inputs' / rel, limit=MAX_MANIFEST_BYTES)
            label = PurePosixPath(rel).stem
            tls = raw.get('tls')
            if type(tls) is not dict:
                _fail('bundle_manifest_fields_rejected')
            tls['https_ca_file'] = str((workspace / 'inputs/certs/https-ca.pem').resolve())
            if raw['experiment'] != 'E3' or raw['roles'] != ['control']:
                tls['mqtt_tls_ca_file'] = str((workspace / 'inputs/certs/mqtt-ca.pem').resolve())
            for role in raw['identity_files']:
                relative_identity = raw['identity_files'][role]
                safe_relative = _safe_ref(relative_identity)
                identity_row = inventory.get(safe_relative)
                if identity_row is None or identity_row['kind'] != 'role_identity':
                    _fail('bundle_identity_reference_missing')
                raw['identity_files'][role] = str((workspace / 'inputs' / safe_relative).resolve())
            for gateway_id in raw.get('gateway_identity_files', {}):
                relative_credential = raw['gateway_identity_files'][gateway_id]
                safe_relative = _safe_ref(relative_credential)
                credential_row = inventory.get(safe_relative)
                if credential_row is None or credential_row['kind'] != 'gateway_credential':
                    _fail('bundle_gateway_identity_missing')
                raw['gateway_identity_files'][gateway_id] = str(
                    (workspace / 'inputs' / safe_relative).resolve()
                )
            if raw['roles'] == ['control']:
                raw.pop('bundle_path', None)
            else:
                raw['bundle_path'] = str(workspace.resolve())
            e4 = raw.get('e4')
            if isinstance(e4, dict) and e4.get('journal_path'):
                if metadata['session']['experiment'] == 'E5':
                    e4['journal_path'] = str((workspace / 'inputs/datasets/dataset.jsonl').resolve())
                else:
                    journal = workspace / 'outputs/e4-journal.sqlite'
                    if journal.exists() or journal.is_symlink():
                        _fail('workspace_mutable_output_exists')
                    e4['journal_path'] = str(journal.resolve())
            bound_path = manifest_map[rel]
            data = json.dumps(raw, sort_keys=True, separators=(',', ':')).encode()
            _write(workspace, str(bound_path.relative_to(workspace)), data)
            bound_manifest_hashes[label] = hashlib.sha256(data).hexdigest()

        bound_env_hashes = {}
        for rel in gateway_env_paths:
            label = PurePosixPath(rel).stem
            env = _json_file(workspace / 'inputs' / rel, limit=65536)
            env['GATEWAY_MQTT_CA_FILE'] = str(
                (workspace / 'inputs/certs/mqtt-ca.pem').resolve()
            )
            password = workspace / 'inputs/private/gateway-passwords' / f'{label}.txt'
            if not password.is_file() or password.is_symlink():
                _fail('bundle_gateway_password_missing')
            env['GATEWAY_MQTT_PASSWORD_FILE'] = str(password.resolve())
            outbox = workspace / 'outputs/outbox' / f'{label}.sqlite'
            if outbox.exists() or outbox.is_symlink():
                _fail('workspace_mutable_output_exists')
            env['GATEWAY_OUTBOX_PATH'] = str(outbox.resolve())
            data = json.dumps(env, sort_keys=True, separators=(',', ':')).encode()
            env_rel = f'runtime/gateway-env/{label}.json'
            _write(workspace, env_rel, data)
            bound_env_hashes[label] = hashlib.sha256(data).hexdigest()

        run_spec_rel = 'inputs/execution/run-spec.json'
        portable_spec = _json_file(workspace / run_spec_rel, limit=1 << 20)
        if (type(portable_spec) is not dict or set(portable_spec) not in (RUN_SPEC_KEYS, RUN_SPEC_KEYS - {'disk_guard'})
                or type(portable_spec.get('paperload')) is not list
                or type(portable_spec.get('gateways')) is not list):
            _fail('bundle_run_spec_rejected')
        bound_jobs = []
        seen_jobs = set()
        for job in portable_spec['paperload']:
            if (type(job) is not dict or set(job) != {'manifest', 'output_dir', 'role', 'ready_timeout_seconds'}):
                _fail('bundle_run_spec_rejected')
            manifest_rel = _safe_ref(job['manifest'])
            if manifest_rel not in manifest_map or manifest_rel in seen_jobs:
                _fail('bundle_run_spec_reference_rejected')
            seen_jobs.add(manifest_rel)
            label = PurePosixPath(manifest_rel).stem
            role = job['role']
            manifest_input = _json_file(workspace / 'inputs' / manifest_rel, limit=MAX_MANIFEST_BYTES)
            if role != manifest_input['roles'][0]:
                _fail('bundle_run_spec_reference_rejected')
            timeout = job['ready_timeout_seconds']
            if type(timeout) is bool or type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                _fail('bundle_run_spec_rejected')
            is_e1_viewer = (
                metadata.get('session', {}).get('experiment') == 'E1'
                and _is_e1_viewer_manifest(manifest_input, role)
            )
            max_timeout = MAX_E1_VIEWER_READINESS_SECONDS if is_e1_viewer else MAX_READINESS_SECONDS
            if timeout > max_timeout:
                _fail('bundle_run_spec_rejected')
            output_ref = _safe_ref(job['output_dir'])
            if not output_ref.startswith('outputs/'):
                _fail('bundle_run_spec_reference_rejected')
            bound_jobs.append({
                'manifest': str(manifest_map[manifest_rel].resolve()),
                'output_dir': str((workspace / output_ref).resolve()),
                'role': role,
                'ready_timeout_seconds': timeout,
            })
        if seen_jobs != set(manifest_map):
            _fail('bundle_run_spec_reference_rejected')
        bound_gateways = []
        seen_env = set()
        for gateway in portable_spec['gateways']:
            if (type(gateway) is not dict
                    or set(gateway) != {'env_file', 'role', 'ready_timeout_seconds'}):
                _fail('bundle_run_spec_rejected')
            env_rel = _safe_ref(gateway['env_file'])
            if env_rel not in env_map or env_rel in seen_env:
                _fail('bundle_run_spec_reference_rejected')
            seen_env.add(env_rel)
            timeout = gateway['ready_timeout_seconds']
            if (gateway['role'] not in ('executor', 'durable-telemetry')
                    or type(timeout) not in (int, float) or not math.isfinite(timeout)
                    or not 0 < timeout <= 60):
                _fail('bundle_run_spec_rejected')
            bound_gateways.append({
                'env_file': str(env_map[env_rel].resolve()),
                'role': gateway['role'],
                'ready_timeout_seconds': timeout,
            })
        if seen_env != set(env_map):
            _fail('bundle_run_spec_reference_rejected')
        stop_timeout = portable_spec['stop_timeout_seconds']
        max_stop = 300 if metadata['session']['experiment'] == 'E2' else 60
        if type(stop_timeout) is not int or not 1 <= stop_timeout <= max_stop:
            _fail('bundle_run_spec_rejected')
        disk_guard = portable_spec.get('disk_guard')
        if disk_guard is None:
            # Legacy pilot bundle qualification: guard defaults from manifest budgets.
            limits = [_json_file(workspace / 'inputs' / rel, limit=MAX_MANIFEST_BYTES).get('limits', {})
                      for rel in manifest_paths]
            growth_values = [item.get('disk_growth_bytes') for item in limits
                             if type(item.get('disk_growth_bytes')) is int and item['disk_growth_bytes'] > 0]
            if not growth_values:
                _fail('bundle_disk_guard_budget_missing')
            disk_guard = {'growth_threshold_bytes': min(growth_values),
                          'minimum_free_bytes': 64 << 20, 'interval_seconds': 1.0,
                          'max_samples': 128, 'qualification': 'legacy_bundle_guard_defaulted'}
        if (type(disk_guard) is not dict or type(disk_guard.get('growth_threshold_bytes')) is not int
                or disk_guard['growth_threshold_bytes'] <= 0 or type(disk_guard.get('minimum_free_bytes')) is not int
                or disk_guard['minimum_free_bytes'] < 0 or not .1 <= disk_guard.get('interval_seconds', 0) <= 60):
            _fail('bundle_disk_guard_rejected')
        bound_spec = {
            'paperload': bound_jobs,
            'gateways': bound_gateways,
            'stop_timeout_seconds': stop_timeout,
            'disk_guard': {**disk_guard, 'allowed_root': str((workspace / 'outputs').absolute())},
        }
        spec_data = json.dumps(bound_spec, sort_keys=True, separators=(',', ':')).encode()
        _write(workspace, 'runtime/execution/run-spec.json', spec_data)
        bound_e5 = None
        if metadata['session']['experiment'] == 'E5':
            context = _json_file(workspace / 'inputs/contexts/e5-context.json', limit=65536)
            context['dataset_path'] = str((workspace / 'inputs/datasets/dataset.jsonl').resolve())
            bound_context_data = json.dumps(context, sort_keys=True, separators=(',', ':')).encode()
            _write(workspace, 'runtime/e5-context.json', bound_context_data)
            phase_plan = _json_file(workspace / 'inputs/execution/e5-phase-plan.json', limit=65536)
            phase_plan['context_path'] = str((workspace / 'runtime/e5-context.json').resolve())
            phase_plan['dataset_path'] = context['dataset_path']
            phase_plan_data = json.dumps(phase_plan, sort_keys=True, separators=(',', ':')).encode()
            _write(workspace, 'runtime/e5-phase-plan.json', phase_plan_data)
            bound_e5 = {
                'context_sha256': hashlib.sha256(bound_context_data).hexdigest(),
                'phase_plan_sha256': hashlib.sha256(phase_plan_data).hexdigest(),
                'dataset_sha256': context['sha256'],
            }
        binding = {
            'schema_version': 1,
            'session': metadata['session'],
            'source_bundle_sha256': hashlib.sha256(
                (source_root / 'bundle.json').read_bytes()
            ).hexdigest(),
            'source_inventory': [
                {'path': row['path'], 'sha256': row['sha256']}
                for row in metadata['inputs']
            ],
            'bound_manifest_sha256': bound_manifest_hashes,
            'bound_gateway_environment_sha256': bound_env_hashes,
            'bound_run_spec_sha256': hashlib.sha256(spec_data).hexdigest(),
            'elapsed_transfer_wait_seconds': elapsed,
            'required_token_lifetime_seconds': required_lifetime,
            'immutable_root': 'inputs/',
            'mutable_root': 'outputs/',
        }
        if bound_durable_probe is not None:
            binding['bound_durable_probe_sha256'] = hashlib.sha256(
                (workspace / 'runtime/durable-probe.json').read_bytes()).hexdigest()
        if bound_e5 is not None:
            binding['e5'] = bound_e5
        _write(
            workspace,
            'runtime/binding.json',
            json.dumps(binding, sort_keys=True, separators=(',', ':')).encode(),
        )
        return {
            'workspace': str(workspace),
            'session': metadata['session'],
            'manifest_count': len(bound_jobs),
            'executor_count': len(bound_gateways),
            'elapsed_transfer_wait_seconds': elapsed,
            **({'e5_context': str((workspace / 'runtime/e5-context.json').resolve()),
                'e5_dataset': str((workspace / 'inputs/datasets/dataset.jsonl').resolve()),
                'e5_phase_plan': str((workspace / 'runtime/e5-phase-plan.json').resolve()),
                'e5_run_spec': str((workspace / 'runtime/execution/run-spec.json').resolve())}
               if bound_e5 is not None else {}),
        }
    except BaseException:
        shutil.rmtree(workspace)
        raise


def inspect_bundle(root):
    root=Path(root)
    if root.is_symlink() or not root.is_dir() or stat.S_IMODE(root.stat().st_mode) != 0o700:
        _fail('bundle_directory_rejected')
    if stat.S_IMODE((root/'bundle.json').stat().st_mode) != 0o600:
        _fail('bundle_metadata_permissions_rejected')
    metadata=_json_file(root/'bundle.json',limit=1<<20)
    required_metadata = {
        'schema_version', 'contract', 'path_reference_base', 'session', 'topology', 'workload', 'endpoints',
        'inputs', 'immutable_paths', 'mutable_outputs', 'identity_expiry_earliest',
        'created_at_unix', 'wait_seconds', 'workload_seconds', 'additional_token_lifetime_seconds', 'integrity_note',
    }
    if type(metadata) is dict and 'durable_probe' in metadata:
        required_metadata.add('durable_probe')
    session_hint = metadata.get('session') if type(metadata) is dict else None
    if type(session_hint) is dict and session_hint.get('experiment') == 'E5':
        required_metadata |= {'e5_context_ref', 'e5_dataset_ref', 'e5_phase_plan_ref'}
    if (type(metadata) is not dict or set(metadata) != required_metadata
            or metadata.get('schema_version') != SCHEMA_VERSION
            or metadata.get('contract') != 'p8-portable-client-inputs-only'
            or metadata.get('path_reference_base') != 'bundle-root'
            or type(metadata.get('inputs')) is not list):
        _fail('bundle_metadata_rejected')
    session = metadata.get('session')
    if (type(session) is not dict or set(session) not in (
                {'session_id','run_ids','experiment','arm','phase'},
                {'session_id','run_ids','experiment','arm','phase','profile'})
            or not isinstance(session.get('session_id'),str) or not SAFE_ID.fullmatch(session['session_id'])
            or type(session.get('run_ids')) is not list or not session['run_ids']
            or any(not isinstance(run_id,str) or not SAFE_ID.fullmatch(run_id) for run_id in session['run_ids'])
            or session.get('experiment') not in ('E1','E2','E3','E4','E5')
            or session.get('profile', 'pilot') not in ('pilot','representative','official')
            or not isinstance(session.get('arm'),str) or not SAFE_ID.fullmatch(session['arm'])
            or not isinstance(session.get('phase'),str) or not SAFE_ID.fullmatch(session['phase'])):
        _fail('bundle_session_metadata_rejected')
    if (type(metadata.get('created_at_unix')) not in (int,float)
            or not math.isfinite(metadata['created_at_unix'])
            or type(metadata.get('wait_seconds')) not in (int,float)
            or type(metadata.get('workload_seconds')) not in (int,float)
            or type(metadata.get('additional_token_lifetime_seconds')) not in (int,float)
            or not math.isfinite(metadata['wait_seconds']) or not math.isfinite(metadata['workload_seconds'])
            or not math.isfinite(metadata['additional_token_lifetime_seconds'])
            or metadata['wait_seconds'] < 0 or metadata['workload_seconds'] < 0
            or metadata['additional_token_lifetime_seconds'] < 0
            or metadata['wait_seconds'] + metadata['workload_seconds'] > MAX_WAIT_SECONDS):
        _fail('bundle_metadata_rejected')
    if (type(metadata.get('immutable_paths')) is not list
            or metadata.get('mutable_outputs') != ['outputs/']
            or not isinstance(metadata.get('topology'),dict)
            or not isinstance(metadata.get('workload'),dict)
            or type(metadata.get('endpoints')) is not dict
            or set(metadata['endpoints']) != {'https','mqtt_tls'}):
        _fail('bundle_metadata_rejected')
    if 'durable_probe' in metadata:
        probe = metadata['durable_probe']
        expected_probe = {'schema_version','experiment','session_id','run_id','gateway_id',
            'sensor_id','username','password_ref','ca_ref','mqtt_endpoint','mqtt_server_name',
            'sample_interval_us','samples_per_batch','max_rows','max_bytes',
            'shutdown_timeout_seconds','outbox_ref'}
        if (type(probe) is not dict or set(probe) != expected_probe or probe.get('schema_version') != 1
                or probe.get('experiment') != 'E4' or metadata['session'].get('experiment') != 'E4'
                or probe.get('session_id') != metadata['session'].get('session_id')
                or probe.get('run_id') not in metadata['session'].get('run_ids', [])
                or not SAFE_ID.fullmatch(probe.get('gateway_id',''))
                or probe.get('sensor_id') != 'p8-e4-durable'
                or not isinstance(probe.get('username'),str) or not probe['username']
                or probe.get('password_ref') != f"private/gateways/{probe.get('gateway_id')}.json"
                or probe.get('ca_ref') != 'certs/mqtt-ca.pem'
                or probe.get('outbox_ref') != 'outputs/outbox/e4-durable.sqlite'
                or probe.get('sample_interval_us') != 10000 or probe.get('samples_per_batch') != 100
                or type(probe.get('sample_interval_us')) is not int
                or type(probe.get('samples_per_batch')) is not int
                or type(probe.get('max_rows')) is not int or not 1 <= probe['max_rows'] <= 64
                or type(probe.get('max_bytes')) is not int or not 1024 <= probe['max_bytes'] <= 1 << 20
                or type(probe.get('shutdown_timeout_seconds')) is not int
                or not 1 <= probe['shutdown_timeout_seconds'] <= 30
                or not isinstance(probe.get('mqtt_server_name'),str)
                or not SAFE_ID.fullmatch(probe['mqtt_server_name'])):
            _fail('bundle_durable_probe_rejected')
        _endpoint(probe['mqtt_endpoint'],'mqtts')
    if len(metadata['inputs']) > MAX_FILES:
        _fail('bundle_size_limit_exceeded')
    seen=set()
    declared_total = 0
    for row in metadata['inputs']:
        if type(row) is not dict or set(row) != {'path','kind','size_bytes','sha256'}:
            _fail('bundle_inventory_rejected')
        if (type(row.get('size_bytes')) is not int or not 0 < row['size_bytes'] <= MAX_FILE_BYTES
                or not isinstance(row.get('sha256'),str)
                or not re.fullmatch(r'[0-9a-f]{64}',row['sha256'])):
            _fail('bundle_inventory_rejected')
        declared_total += row['size_bytes']
    if declared_total > MAX_TOTAL_BYTES:
        _fail('bundle_size_limit_exceeded')
    if metadata['immutable_paths'] != [row.get('path') for row in metadata['inputs']]:
        _fail('bundle_inventory_rejected')
    expected_paths = {
        'public_ca': {'certs/https-ca.pem','certs/mqtt-ca.pem'},
        'gateway_environment': None,
        'gateway_credential': None,
        'role_identity': None,
        'manifest': None,
        'run_spec': {'execution/run-spec.json'},
        'durable_probe': {'durable-probe.json'},
        'dataset': {'datasets/dataset.jsonl'},
        'e5_context': {'contexts/e5-context.json'},
        'e5_phase_plan': {'execution/e5-phase-plan.json'},
    }
    seen_kinds = set()
    for row in metadata['inputs']:
        rel=_safe_ref(row['path'])
        if rel != row['path'] or rel in seen:
            _fail('bundle_inventory_duplicate')
        seen.add(rel)
        kind = row['kind']
        if kind not in expected_paths:
            _fail('bundle_inventory_kind_rejected')
        exact_paths = expected_paths[kind]
        if exact_paths is not None and rel not in exact_paths:
            _fail('bundle_inventory_path_rejected')
        if exact_paths is None:
            allowed_prefixes = {
                'gateway_environment': ('gateway-env/', '.json'),
                'gateway_credential': (('private/gateways/', '.json'), ('private/gateway-passwords/', '.txt')),
                'role_identity': ('private/identities/', '.json'),
                'manifest': ('manifests/', '.json'),
            }[kind]
            prefixes = allowed_prefixes if isinstance(allowed_prefixes[0],tuple) else (allowed_prefixes,)
            if not any(rel.startswith(prefix) and rel.endswith(suffix) for prefix,suffix in prefixes):
                _fail('bundle_inventory_path_rejected')
        seen_kinds.add(kind)
        data=_source(root/rel, limit=MAX_FILE_BYTES)
        if stat.S_IMODE((root/rel).stat().st_mode) != 0o600:
            _fail('bundle_input_permissions_rejected')
        if len(data) != row['size_bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']:
            _fail('bundle_checksum_mismatch')
    if ('public_ca' not in seen_kinds or 'manifest' not in seen_kinds or 'run_spec' not in seen_kinds
            or not {'certs/https-ca.pem','certs/mqtt-ca.pem'} <= seen):
        _fail('bundle_inventory_incomplete')
    for row in metadata['inputs']:
        if row.get('kind') == 'manifest':
            manifest = _json_file(root / row['path'], limit=MAX_MANIFEST_BYTES)
            expected_manifest_profile = ('pilot' if session.get('experiment') == 'E5' or
                session.get('experiment') == 'E3' and session.get('profile') == 'official'
                and Path(row['path']).name in ('A.json', 'B.json')
                else session.get('profile', 'pilot'))
            if (type(manifest) is not dict
                    or manifest.get('profile', 'pilot') != expected_manifest_profile):
                _fail('bundle_profile_mismatch')
    if session.get('experiment') == 'E5':
        if not {'dataset','e5_context','e5_phase_plan'} <= seen_kinds:
            _fail('bundle_e5_inventory_incomplete')
        expected_refs = {'e5_context_ref':'contexts/e5-context.json',
                         'e5_dataset_ref':'datasets/dataset.jsonl',
                         'e5_phase_plan_ref':'execution/e5-phase-plan.json'}
        if any(metadata.get(k) != v for k, v in expected_refs.items()):
            _fail('bundle_e5_metadata_rejected')
        context = _json_file(root / expected_refs['e5_context_ref'], limit=65536)
        context = dict(context, dataset_path=str(root / expected_refs['e5_dataset_ref']))
        child_manifests = {}
        for label in ('initial','warm_replay','viewer','control'):
            path = root / 'manifests' / f'{label}.json'
            if not path.is_file():
                _fail('bundle_e5_manifest_missing')
            child_manifests[label] = _json_file(path, limit=MAX_MANIFEST_BYTES)
        _validate_e5_context(context, root / expected_refs['e5_dataset_ref'], child_manifests,
                             session['arm'])
        plan = _json_file(root / expected_refs['e5_phase_plan_ref'], limit=65536)
        if (type(plan) is not dict or set(plan) != {'schema_version','phases','viewer_lifecycle'}
                or plan.get('schema_version') != 1
                or plan.get('phases') != ['initial','warm_replay','control']):
            _fail('bundle_e5_phase_plan_rejected')
        _validate_e5_lifecycle(plan['viewer_lifecycle'])
    if session.get('experiment') == 'E2':
        wl = metadata.get('workload')
        if isinstance(wl, dict) and ('retry_policy' in wl or any(
                isinstance(_json_file(root / 'manifests' / f'{arm}.json', limit=MAX_MANIFEST_BYTES), dict)
                and 'retry_policy' in _json_file(root / 'manifests' / f'{arm}.json', limit=MAX_MANIFEST_BYTES)
                for arm in ('A', 'B'))):
            policies = [_json_file(root / 'manifests' / f'{arm}.json', limit=MAX_MANIFEST_BYTES).get('retry_policy')
                        for arm in ('A', 'B')]
            if policies[0] != policies[1] or wl.get('retry_policy') != policies[0]:
                _fail('bundle_retry_policy_mismatch')
        if isinstance(wl, dict) and wl.get('profile') == 'official':
            _validate_e2_official_workload(wl)
            expected_limits = wl.get('pending_limits', {'pending_messages': 64, 'pending_bytes': 1 << 20})
            for arm in ('A', 'B'):
                manifest = _json_file(root / 'manifests' / f'{arm}.json', limit=MAX_MANIFEST_BYTES)
                limits = manifest.get('limits') if isinstance(manifest, dict) else None
                if not isinstance(limits, dict) or any(
                        limits.get(key) != value for key, value in expected_limits.items()):
                    _fail('bundle_e2_pending_limits_mismatch')
        elif isinstance(wl, dict) and (wl.get('workload_type') == 'equal_samples' or wl.get('workload') == 'equal_samples'):
            _validate_e2_equal_samples_workload(wl)
    return {'schema_version':SCHEMA_VERSION,'valid':True,'files':len(seen),'total_bytes':declared_total,
            'session':metadata.get('session')}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    inspect=sub.add_parser('inspect'); inspect.add_argument('bundle',type=Path)
    args=parser.parse_args(argv)
    try: result=inspect_bundle(args.bundle) if args.command=='inspect' else None
    except ValueError as exc: parser.error(str(exc))
    print(json.dumps(result,sort_keys=True)); return 0


if __name__=='__main__': raise SystemExit(main())
