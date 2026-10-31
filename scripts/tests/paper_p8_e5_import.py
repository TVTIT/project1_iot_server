"""Bounded A-side import of copied E5 phase results.

The B host writes ``results.json`` (plus an optional generator resource CSV)
into ``incoming/<phase>/`` on this host. Nothing here executes, imports or opens
paths named by B: only the two known private files are read, and only their
typed contents are projected. Owned immutable import receipts pin the binding,
source and identity declarations so later phases cannot silently re-declare
them.
"""
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat

PHASE_NAMES = ('initial', 'warm_replay', 'control')

MAX_RESULTS_BYTES = 8 << 20
MAX_RESOURCES_BYTES = 512 * 1024
MAX_CONTEXT_BYTES = 16384
MAX_RESOURCE_SAMPLES = 128
MAX_RESOURCES_ROWS = 128

_HEX = re.compile(r'[0-9a-f]{64}')

_BASE_KEYS = {
    'schema_version', 'experiment', 'arm', 'phase', 'session_id', 'run_id', 'status',
    'source_bundle_sha256', 'source_inventory', 'bound_manifest_sha256',
    'bound_gateway_environment_sha256', 'bound_run_spec_sha256',
    'bound_e5_context_sha256', 'bound_e5_phase_plan_sha256', 'bound_dataset_sha256',
    'binding', 'binding_sha256', 'runner', 'resource_samples', 'projection',
    'full_p8_accepted', 'physical_ab',
}

_PHASE_PROJECTION_KEYS = {
    'initial': {'status', 'cleanup_errors', 'e4', 'telemetry', 'holder_evidence'},
    'warm_replay': {'status', 'cleanup_errors', 'e4', 'telemetry', 'holder_evidence'},
    'control': {'status', 'cleanup_errors', 'commands', 'executors', 'sqlite_executors',
                'joined_proof', 'viewer', 'lifecycle', 'lifecycle_proof',
                'readiness_projection', 'b_declared'},
}

_BINDING_KEYS = {
    'schema_version', 'session', 'source_bundle_sha256', 'source_inventory',
    'bound_manifest_sha256', 'bound_gateway_environment_sha256',
    'bound_run_spec_sha256', 'elapsed_transfer_wait_seconds',
    'required_token_lifetime_seconds', 'e5', 'immutable_root', 'mutable_root',
}

_RUNNER_NUMERIC_KEYS = {
    'started_at_unix', 'finished_at_unix', 'durable_gateway_process_count',
    'durable_gateway_count', 'durable_sensor_connections', 'durable_sensor_count',
}

_RESOURCE_SAMPLE_KEYS = {
    'sample_index', 'timestamp', 'elapsed_seconds', 'interval_seconds', 'available',
    'cpu_percent', 'delta_ticks', 'rss_bytes', 'process_count', 'current_fd_count',
    'reason',
}

_PINNED_KEYS = (
    'session_id', 'arm', 'source_bundle_sha256', 'source_inventory_sha256',
    'bound_manifest_sha256', 'bound_gateway_environment_sha256',
    'bound_run_spec_sha256', 'bound_e5_context_sha256',
    'bound_e5_phase_plan_sha256', 'bound_dataset_sha256', 'binding_sha256', 'binding',
)

_KNOWN_FILES = {'results.json', 'resources-generator.csv'}


def _no_duplicate_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate_key')
        value[key] = item
    return value


def _reject_constant(_name):
    raise ValueError('non_finite_constant')


def _check_value(value, code, depth=0):
    if depth > 32:
        raise ValueError(code)
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(code)
            _check_value(item, code, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _check_value(item, code, depth + 1)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(code)
    elif isinstance(value, (str, int, bool)) or value is None:
        return
    else:
        raise ValueError(code)


def strict_json(raw, code):
    """Parse JSON rejecting duplicate keys and non-finite numbers."""
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_no_duplicate_pairs,
                           parse_constant=_reject_constant)
    except (ValueError, UnicodeDecodeError, RecursionError):
        raise ValueError(code) from None
    _check_value(value, code)
    return value


def _check_chain(path, code):
    for item in (path, *path.parents):
        if item.is_symlink():
            raise ValueError(code)
    return path


def read_private_file(path, *, code, max_bytes):
    """Read one owned, single-link, private regular file with a size bound."""
    path = Path(path).absolute()
    _check_chain(path, code)
    try:
        stream = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0))
    except OSError:
        raise ValueError(code) from None
    with os.fdopen(stream, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1
                or info.st_size > max_bytes):
            raise ValueError(code)
        raw = handle.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError(code)
    return raw


def private_dir(path, *, code, create=False):
    path = Path(path).absolute()
    _check_chain(path, code)
    if create and not path.exists():
        path.mkdir(mode=0o700, parents=False, exist_ok=False)
        path.chmod(0o700)
    try:
        info = path.lstat()
    except OSError:
        raise ValueError(code) from None
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError(code)
    return path


def incoming_root(private):
    return private_dir(Path(private) / 'incoming', code='e5_incoming_root_rejected')


def phase_incoming_dir(private, phase):
    if phase not in PHASE_NAMES:
        raise ValueError(f'e5_unknown_phase: {phase}')
    root = incoming_root(private)
    leaf = Path(root) / phase
    return private_dir(leaf, code='e5_incoming_phase_rejected')


def _write_receipt(path, value, code):
    path = Path(path)
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        raise ValueError(code) from None
    with os.fdopen(fd, 'wb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())


def receipt_root(private):
    return private_dir(Path(private) / 'e5-imports', code='e5_import_root_rejected', create=True)


def read_receipt(private, phase):
    path = Path(receipt_root(private)) / f'{phase}.json'
    raw = read_private_file(path, code='e5_import_receipt_rejected', max_bytes=256 * 1024)
    receipt = strict_json(raw, 'e5_import_receipt_rejected')
    if not isinstance(receipt, dict) or receipt.get('phase') != phase:
        raise ValueError('e5_import_receipt_rejected')
    return receipt


def _inventory(value, code):
    if not isinstance(value, list) or not value:
        raise ValueError(code)
    normalized = []
    for row in value:
        if (not isinstance(row, dict) or set(row) != {'path', 'sha256'}
                or not isinstance(row['path'], str) or not row['path']
                or not isinstance(row['sha256'], str) or not _HEX.fullmatch(row['sha256'])):
            raise ValueError(code)
        normalized.append({'path': row['path'], 'sha256': row['sha256']})
    return normalized


def _digest_map(value, expected, code):
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError(code)
    if any(not isinstance(item, str) or not _HEX.fullmatch(item) for item in value.values()):
        raise ValueError(code)
    return dict(value)


def _finite(value, code, *, minimum=0.0):
    if type(value) not in (int, float) or type(value) is bool:
        raise ValueError(code)
    number = float(value)
    if not math.isfinite(number) or number < minimum:
        raise ValueError(code)
    return number


def _validate_runner(runner, phase):
    code = 'e5_import_runner_rejected'
    if not isinstance(runner, dict) or not runner.get('status'):
        raise ValueError(code)
    allowed = ({'status', 'readiness', 'pending_outbox_readback', 'actual_stop_confirmed',
                'timer_driven_gateway_workload', 'resource_sampling',
                'phase_observations'} | _RUNNER_NUMERIC_KEYS)
    if set(runner) - allowed:
        raise ValueError(code)
    if runner.get('status') != 'COMPLETE':
        raise ValueError(code)
    if runner.get('actual_stop_confirmed') is not True:
        raise ValueError(code)
    if 'timer_driven_gateway_workload' in runner:
        if type(runner['timer_driven_gateway_workload']) is not bool:
            raise ValueError(code)
    for key in _RUNNER_NUMERIC_KEYS:
        if key in runner:
            _finite(runner[key], code)
    readiness = runner.get('readiness')
    if not isinstance(readiness, list) or any(
            not isinstance(row, dict) or row.get('ready') is not True for row in readiness):
        raise ValueError(code)
    readback = runner.get('pending_outbox_readback')
    if not isinstance(readback, list) or any(not isinstance(row, dict) for row in readback):
        raise ValueError(code)
    if phase == 'control':
        if len(readiness) < 1 or len(readback) < 1:
            raise ValueError(code)
        if any(row.get('status') != 'readback' or row.get('pending_rows') != 0
               for row in readback):
            raise ValueError(code)
    return runner


def _validate_resource_samples(samples):
    code = 'e5_import_resource_samples_rejected'
    if not isinstance(samples, list) or len(samples) > MAX_RESOURCE_SAMPLES:
        raise ValueError(code)
    for sample in samples:
        if not isinstance(sample, dict) or set(sample) - _RESOURCE_SAMPLE_KEYS:
            raise ValueError(code)
        for key in ('elapsed_seconds', 'interval_seconds', 'cpu_percent', 'delta_ticks',
                    'rss_bytes', 'process_count', 'current_fd_count', 'sample_index'):
            if key in sample and sample[key] is not None:
                _finite(sample[key], code, minimum=-1e18)
        if 'available' in sample and sample['available'] is not None and type(sample['available']) is not bool:
            raise ValueError(code)
    return samples


def _validate_resources_csv(raw):
    code = 'e5_import_resources_csv_rejected'
    if len(raw) > MAX_RESOURCES_BYTES:
        raise ValueError(code)
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError(code) from None
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        raise ValueError(code)
    header, body = rows[0], rows[1:]
    if len(body) > MAX_RESOURCES_ROWS:
        raise ValueError(code)
    for row in body:
        if len(row) != len(header):
            raise ValueError(code)
    return {'rows': len(body), 'columns': len(header)}


def _expected_phase_run_id(prepared, phase):
    manifest_path = Path(prepared['manifests'][phase])
    raw = read_private_file(manifest_path, code='e5_a_manifest_unavailable', max_bytes=1 << 20)
    manifest = strict_json(raw, 'e5_a_manifest_unavailable')
    run_id = manifest.get('run_id') if isinstance(manifest, dict) else None
    if not isinstance(run_id, str) or not run_id:
        raise ValueError('e5_a_manifest_unavailable')
    return run_id


def _bundle_identity(prepared):
    session = prepared.get('bundle_session')
    bundle_sha = prepared.get('bundle_sha256')
    inventory = prepared.get('bundle_inventory')
    if (not isinstance(session, dict) or not isinstance(bundle_sha, str)
            or not _HEX.fullmatch(bundle_sha)):
        raise ValueError('e5_a_bundle_identity_missing')
    normalized = _inventory(inventory, 'e5_a_bundle_inventory_rejected')
    context = prepared.get('context') if isinstance(prepared.get('context'), dict) else {}
    dataset_sha = context.get('sha256')
    if not isinstance(dataset_sha, str) or not _HEX.fullmatch(dataset_sha):
        raise ValueError('e5_a_dataset_identity_missing')
    return session, bundle_sha, normalized, dataset_sha


def _expected_manifest_stems(prepared):
    # The portable bundle names bound manifests by label under runtime/manifests,
    # never by the A-side source filename.
    return {'initial', 'warm_replay', 'viewer', 'control'}


def _expected_executor_labels(prepared):
    env_files = prepared['manifests'].get('gateway_env') or []
    if not env_files:
        raise ValueError('e5_a_executor_inventory_missing')
    return {f'executor-{index + 1}' for index in range(len(env_files))}


def _validate_binding(binding, *, bundle_session, bundle_sha, inventory, dataset_sha,
                      manifest_stems, executor_labels, summary):
    code = 'e5_import_binding_rejected'
    if not isinstance(binding, dict) or set(binding) != _BINDING_KEYS:
        raise ValueError(code)
    if binding.get('schema_version') != 1:
        raise ValueError(code)
    if binding.get('session') != bundle_session:
        raise ValueError(code)
    if binding.get('source_bundle_sha256') != bundle_sha:
        raise ValueError(code)
    if _inventory(binding.get('source_inventory'), code) != inventory:
        raise ValueError(code)
    if binding.get('immutable_root') != 'inputs/' or binding.get('mutable_root') != 'outputs/':
        raise ValueError(code)
    _finite(binding.get('elapsed_transfer_wait_seconds'), code)
    _finite(binding.get('required_token_lifetime_seconds'), code)
    manifests = _digest_map(binding.get('bound_manifest_sha256'), manifest_stems, code)
    environments = _digest_map(binding.get('bound_gateway_environment_sha256'), executor_labels, code)
    run_spec = binding.get('bound_run_spec_sha256')
    if not isinstance(run_spec, str) or not _HEX.fullmatch(run_spec):
        raise ValueError(code)
    e5 = binding.get('e5')
    if (not isinstance(e5, dict)
            or set(e5) != {'context_sha256', 'phase_plan_sha256', 'dataset_sha256'}):
        raise ValueError(code)
    for value in e5.values():
        if not isinstance(value, str) or not _HEX.fullmatch(value):
            raise ValueError(code)
    if e5['dataset_sha256'] != dataset_sha:
        raise ValueError('e5_import_dataset_mismatch')
    if summary.get('bound_manifest_sha256') != manifests:
        raise ValueError('e5_import_manifest_declaration_mismatch')
    if summary.get('bound_gateway_environment_sha256') != environments:
        raise ValueError('e5_import_environment_declaration_mismatch')
    if summary.get('bound_run_spec_sha256') != run_spec:
        raise ValueError('e5_import_run_spec_declaration_mismatch')
    if summary.get('bound_e5_context_sha256') != e5['context_sha256']:
        raise ValueError('e5_import_context_declaration_mismatch')
    if summary.get('bound_e5_phase_plan_sha256') != e5['phase_plan_sha256']:
        raise ValueError('e5_import_phase_plan_declaration_mismatch')
    if summary.get('bound_dataset_sha256') != dataset_sha:
        raise ValueError('e5_import_dataset_declaration_mismatch')
    return manifests, environments, run_spec, e5


def _validate_projection(summary, phase):
    projection = summary.get('projection')
    expected_keys = _PHASE_PROJECTION_KEYS[phase]
    if not isinstance(projection, dict) or set(projection) != expected_keys:
        raise ValueError('e5_import_projection_rejected')
    for key in expected_keys:
        if key == 'status':
            continue
        if summary.get(key) != projection[key]:
            raise ValueError('e5_import_projection_duplicate_mismatch')
    if summary.get('status') != projection.get('status'):
        raise ValueError('e5_import_projection_duplicate_mismatch')
    if projection.get('status') != 'COMPLETE':
        raise ValueError(f'e5_workload_incomplete_{phase}')
    cleanup = projection.get('cleanup_errors')
    if not isinstance(cleanup, list) or cleanup:
        raise ValueError(f'e5_import_cleanup_errors_{phase}')
    return projection


def _summarize_pin(receipt):
    return {key: receipt[key] for key in _PINNED_KEYS}


def import_phase_result(prepared, private, *, phase):
    """Load one copied B phase result and persist an owned immutable receipt."""
    if phase not in PHASE_NAMES:
        raise ValueError(f'e5_unknown_phase: {phase}')
    session, bundle_sha, inventory, dataset_sha = _bundle_identity(prepared)
    run_id = _expected_phase_run_id(prepared, phase)
    directory = phase_incoming_dir(private, phase)
    entries = sorted(item.name for item in Path(directory).iterdir())
    if 'results.json' not in entries or set(entries) - _KNOWN_FILES:
        raise ValueError('e5_import_incoming_files_rejected')
    results_raw = read_private_file(Path(directory) / 'results.json',
                                    code='e5_import_results_rejected',
                                    max_bytes=MAX_RESULTS_BYTES)
    summary = strict_json(results_raw, 'e5_import_results_rejected')
    if not isinstance(summary, dict):
        raise ValueError('e5_import_results_rejected')
    allowed = _BASE_KEYS | _PHASE_PROJECTION_KEYS[phase]
    if set(summary) != allowed:
        raise ValueError('e5_import_results_schema_rejected')
    if summary.get('schema_version') != 1 or summary.get('experiment') != 'E5':
        raise ValueError('e5_import_identity_rejected')
    if summary.get('phase') != phase:
        raise ValueError('e5_import_phase_mismatch')
    if summary.get('arm') != prepared.get('arm'):
        raise ValueError('e5_import_arm_mismatch')
    if summary.get('session_id') != session.get('session_id'):
        raise ValueError('e5_import_session_mismatch')
    if summary.get('run_id') != run_id:
        raise ValueError('e5_import_run_id_mismatch')
    if summary.get('full_p8_accepted') is not False or summary.get('physical_ab') != 'NOT RUN':
        raise ValueError('e5_import_acceptance_flags_rejected')
    if summary.get('source_bundle_sha256') != bundle_sha:
        raise ValueError('e5_import_source_bundle_mismatch')
    if _inventory(summary.get('source_inventory'), 'e5_import_inventory_rejected') != inventory:
        raise ValueError('e5_import_inventory_mismatch')
    binding = summary.get('binding')
    binding_sha = summary.get('binding_sha256')
    if not isinstance(binding_sha, str) or not _HEX.fullmatch(binding_sha):
        raise ValueError('e5_import_binding_digest_rejected')
    canonical = json.dumps(binding, sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(canonical).hexdigest() != binding_sha:
        raise ValueError('e5_import_binding_digest_mismatch')
    manifests, environments, run_spec, e5 = _validate_binding(
        binding, bundle_session=session, bundle_sha=bundle_sha, inventory=inventory,
        dataset_sha=dataset_sha, manifest_stems=_expected_manifest_stems(prepared),
        executor_labels=_expected_executor_labels(prepared), summary=summary)
    runner = _validate_runner(summary.get('runner'), phase)
    samples = _validate_resource_samples(summary.get('resource_samples'))
    projection = _validate_projection(summary, phase)
    resources = None
    csv_path = Path(directory) / 'resources-generator.csv'
    if csv_path.exists():
        raw_csv = read_private_file(csv_path, code='e5_import_resources_csv_rejected',
                                    max_bytes=MAX_RESOURCES_BYTES)
        resources = _validate_resources_csv(raw_csv)
        resources['sha256'] = hashlib.sha256(raw_csv).hexdigest()

    receipt = {
        'schema_version': 1,
        'experiment': 'E5',
        'phase': phase,
        'arm': prepared.get('arm'),
        'session_id': session.get('session_id'),
        'run_id': run_id,
        'status': projection['status'],
        'source_bundle_sha256': summary['source_bundle_sha256'],
        'source_inventory_sha256': hashlib.sha256(
            json.dumps(inventory, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'bound_manifest_sha256': manifests,
        'bound_gateway_environment_sha256': environments,
        'bound_run_spec_sha256': run_spec,
        'bound_e5_context_sha256': e5['context_sha256'],
        'bound_e5_phase_plan_sha256': e5['phase_plan_sha256'],
        'bound_dataset_sha256': e5['dataset_sha256'],
        'binding_sha256': binding_sha,
        'binding': binding,
        'results_sha256': hashlib.sha256(results_raw).hexdigest(),
        'results_bytes': len(results_raw),
        'projection_sha256': hashlib.sha256(
            json.dumps(projection, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'resource_samples_sha256': hashlib.sha256(
            json.dumps(samples, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
        'resources_generator': resources,
        'runner_status': runner.get('status'),
        'imported_at_unix': 0.0,
        'provenance': 'a_owned_import_of_operator_copied_b_export',
    }
    receipt['pinned'] = _summarize_pin(receipt)
    if phase != 'initial':
        previous = read_receipt(private, 'initial')
        if previous.get('pinned') != receipt['pinned']:
            raise ValueError('e5_import_pin_mismatch_' + phase)
    import time as _time
    receipt['imported_at_unix'] = _time.time()
    _write_receipt(Path(receipt_root(private)) / f'{phase}.json', receipt,
                   'e5_import_receipt_exists')
    loaded = dict(receipt)
    loaded['projection'] = projection
    loaded['runner'] = runner
    loaded['resource_samples'] = samples
    loaded['incoming'] = str(directory)
    return loaded


def copy_generator_resources(private, phase, destination):
    """Copy the bounded optional B resource CSV to an owned A artifact path."""
    source = Path(phase_incoming_dir(private, phase)) / 'resources-generator.csv'
    if not source.exists():
        return None
    raw = read_private_file(source, code='e5_import_resources_csv_rejected',
                            max_bytes=MAX_RESOURCES_BYTES)
    destination = Path(destination)
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    return {'path': str(destination), 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}


def read_context_seed(path):
    """Read the bounded private E5 dataset context seed on host A."""
    raw = read_private_file(path, code='server_context_rejected', max_bytes=MAX_CONTEXT_BYTES)
    context = strict_json(raw, 'server_context_rejected')
    seed = context.get('seed') if isinstance(context, dict) else None
    if type(seed) is not int or isinstance(seed, bool) or not 0 <= seed < 2 ** 31:
        raise ValueError('server_context_seed_rejected')
    return seed
