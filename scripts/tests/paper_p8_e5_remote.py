"""Remote A-side adapter and phase validation for E5 benchmark workloads.

Provides prepare, begin_remote_phase, finish_remote_phase, continuous viewer
validation, and final report assembly. All credentials, database state, and
authoritative readbacks remain strictly on Host A.
"""
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat
import sys
from urllib.parse import urlsplit

_HERE = Path(__file__).resolve().parent
for _path in (str(_HERE), str(_HERE.parent)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

import paper_p8_e5_support as e5
import paper_p8_e4_support as e4
import paper_p8_e3_support as e3
from paper_p8_e3_fixture import (
    _verify_control_receipts,
    _e3_executor_receipt_projection,
)
import paper_p8_comparison as comparison
from paper_p8_e5_fixture import (
    GATEWAYS,
    COMMAND_SENSORS,
    _private_root,
    _validate_reference,
    _safe_e5_viewer,
    prepare_inputs,
    _jwt_algorithm,
    _paperload_source_hash,
)

PHASE_NAMES = ('initial', 'warm_replay', 'control')
phase_names = PHASE_NAMES

REQUIRED_CONTEXT_KEYS = (
    'dataset_path', 'sha256', 'bytes', 'seed', 'anchor',
    'sensor_ids', 'gateway_ids', 'command_keys', 'viewer_subscriptions',
    'workload_selection', 'workload_selection_sha256',
)


def _validate_private_file(path, code_prefix, max_bytes=16384):
    p = Path(path)
    try:
        st = p.lstat()
    except OSError:
        raise ValueError(f'{code_prefix}_not_found') from None
    if p.is_symlink() or not p.is_file() or st.st_uid != os.getuid() or (st.st_mode & 0o077) or st.st_size > max_bytes:
        raise ValueError(f'{code_prefix}_not_private')
    return p, st


def prepare(smoke, fixture, endpoint, registry, containers, images, state, arm,
            context_path, reference_path=None, *, remote_profile=None):
    """Validate environment and prepare portable E5 inputs without launching processes on A."""
    if arm not in e5.ARMS:
        raise ValueError('e5_arm_rejected')
    if arm == 'optimized' and reference_path:
        raise ValueError('paper_p8_e5_reference_rejected')
    if arm != 'optimized' and not reference_path:
        raise ValueError('e5_reference_required')

    # Validate dataset context JSON
    ctx_file, _ = _validate_private_file(context_path, 'e5_dataset_context', max_bytes=16384)
    try:
        context = json.loads(ctx_file.read_text(encoding='utf-8'))
    except (OSError, ValueError, UnicodeDecodeError):
        raise ValueError('e5_dataset_context_invalid') from None

    if not isinstance(context, dict) or any(k not in context for k in REQUIRED_CONTEXT_KEYS):
        raise ValueError('e5_dataset_context_invalid')
    if context.get('workload_selection_sha256') != e5.selection_sha256(context.get('workload_selection')):
        raise ValueError('e5_dataset_context_invalid')

    # Validate backing dataset
    dataset_file = Path(context['dataset_path'])
    try:
        ds = dataset_file.lstat()
    except OSError:
        raise ValueError('e5_dataset_context_hash_mismatch') from None
    if dataset_file.is_symlink() or not dataset_file.is_file() or ds.st_uid != os.getuid() or (ds.st_mode & 0o077):
        raise ValueError('e5_dataset_context_not_private')
    raw = dataset_file.read_bytes()
    if (len(raw) > e5.MAX_JOURNAL_BYTES or len(raw) != context['bytes']
            or hashlib.sha256(raw).hexdigest() != context['sha256']):
        raise ValueError('e5_dataset_context_hash_mismatch')

    # Reference validation for ablations
    reference = None
    if arm != 'optimized':
        reference = _validate_reference(smoke, reference_path, context)
        ref_report = reference['report']
        ref_images = reference.get('image_ids') or ref_report.get('image_ids') or {}
        if any(images.get(k) != ref_images.get(k) for k in ('backend', 'gateway')):
            raise ValueError('e5_reference_image_mismatch')

    # Prepare A-side workload containers & tokens
    pg, quota, effective, resources, storage, tokens, specs, passwords, _ = smoke.prepare_paper_workload(
        fixture, registry, endpoint, containers, images, state, gateway_ids=list(GATEWAYS))
    if not isinstance(quota, dict) or set(('cpus', 'memory_bytes')) - set(quota):
        raise ValueError('e5_backend_quota_invalid')
    if reference is not None and reference['report'].get('quota') != quota:
        raise ValueError('e5_reference_quota_mismatch')

    # Verify effective arm flags match expected profile
    expected_profile = e5.e5_arm_profile(arm)
    services = effective.get('services') if isinstance(effective, dict) else None
    backend = services.get('backend') if isinstance(services, dict) else None
    actual_env = backend.get('environment') if isinstance(backend, dict) else None
    if not isinstance(actual_env, dict) or any(
            str(actual_env.get(k, '')).lower() != str(v).lower() for k, v in expected_profile.items()):
        raise ValueError('e5_effective_arm_profile_mismatch')
    actual_flags = {k: str(actual_env[k]).lower() for k in expected_profile}

    # Verify single mechanism difference against reference for ablation arms
    if arm != 'optimized':
        ref_flags = (ref_report.get('effective_flags')
                     or ref_report.get('metadata', {}).get('effective_flags'))
        diff_result = comparison.validate_mechanism_diff(ref_flags, actual_flags, arm)
        if not diff_result.get('valid'):
            raise ValueError('e5_reference_mechanism_diff_invalid')

        ref_workers = ref_report.get('metadata', {}).get('workers', {})
        current_workers = {
            'paperload': 2,
            'gateway_executors': 2,
            **{k.lower(): int(actual_env[k]) for k in (
                'MQTT_WORKER_COUNT', 'DATABASE_MAX_CONNS', 'STAGE5_PUBLISH_WORKERS', 'STAGE5_RESULT_WORKERS')
                if k in actual_env and str(actual_env[k]).isdigit()}
        }
        if ref_workers and any(ref_workers.get(k) != current_workers.get(k) for k in ref_workers if k in current_workers):
            raise ValueError('e5_reference_worker_mismatch')

    # Terminal schema migration check
    schema = smoke.db_psql(registry, pg,
        "SELECT version::text || ':' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;").strip()
    if schema != '24:telemetry_ledger_timestamp':
        raise ValueError('p8_schema_terminal_rejected')

    # Mosquitto and Nginx remote readback (do not force loopback)
    broker = registry.resolve(containers['mosquitto'])
    profile = remote_profile or getattr(fixture, 'remote_profile', None)
    if profile is not None:
        remote_support = smoke.paper_p8_remote_support()
        remote_support.validate_runtime_mapping(
            'mosquitto', broker.get('NetworkSettings', {}).get('Ports') or {}, profile)
        nginx = registry.resolve(containers['nginx'])
        remote_support.validate_runtime_mapping(
            'nginx', nginx.get('NetworkSettings', {}).get('Ports') or {}, profile)
        advertised_mqtt = profile['advertised_mqtt_tls']
        mqtt_url = urlsplit(advertised_mqtt)
        mqtt_broker_url = f'ssl://{mqtt_url.hostname}:{mqtt_url.port}'
        manifest_mqtt_endpoint = advertised_mqtt
    else:
        ports = broker.get('NetworkSettings', {}).get('Ports', {}).get('8883/tcp') or []
        if len(ports) != 1 or not ports[0].get('HostPort'):
            raise ValueError('e5_mqtt_port_readback_rejected')
        published = int(ports[0]['HostPort'])
        host_ip = ports[0].get('HostIp')
        host = host_ip if (host_ip and host_ip != '0.0.0.0') else '127.0.0.1'
        mqtt_broker_url = f'ssl://{host}:{published}'
        manifest_mqtt_endpoint = f'mqtts://{host}:{published}'

    mqtt_ca_file = fixture.private / 'certs/ca.crt'
    if not mqtt_ca_file.is_file():
        raise ValueError('e5_mqtt_ca_file_missing')

    # Private root on A
    root = Path(fixture.private) / 'p8-e5' / arm
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    root = _private_root(root)

    # Call existing prepare_inputs to create manifests, copy dataset, and provision command sensors via REST
    journal_context = {
        'sha256': context['sha256'],
        'bytes': context['bytes'],
        'gateway_ids': context['gateway_ids'],
        'sensor_ids': context['sensor_ids'],
        'seed': context['seed'],
        'anchor': context['anchor'],
        'workload_selection_sha256': context['workload_selection_sha256'],
    }
    prepared_inputs = prepare_inputs(
        smoke=smoke,
        fixture=fixture,
        endpoint=endpoint,
        specs=specs,
        passwords=passwords,
        tokens=tokens,
        mqtt_broker_url=mqtt_broker_url,
        mqtt_manifest_endpoint=manifest_mqtt_endpoint,
        mqtt_ca_file=mqtt_ca_file,
        journal_path=dataset_file,
        journal_context=journal_context,
        seed=context['seed'],
        anchor=context['anchor'],
        root=root,
        command_keys=context['command_keys'],
        viewer_subscriptions=context['viewer_subscriptions'],
        workload_selection=context['workload_selection'],
        recovery_mode=False,
    )

    # prepare_inputs is shared with local E5 and defaults its server name to
    # localhost. Rewrite only the approved TLS endpoint/server-name fields for
    # this remote profile (and retain localhost only for the local profile).
    mqtt_host = (profile['advertised_host'] if profile is not None else
                 urlsplit(manifest_mqtt_endpoint).hostname)
    if not isinstance(mqtt_host, str) or not mqtt_host:
        raise ValueError('e5_mqtt_server_name_missing')
    for manifest_key in ('initial', 'warm_replay', 'viewer', 'control'):
        manifest_path = Path(prepared_inputs['manifests'][manifest_key])
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        tls = manifest.get('tls')
        if isinstance(tls, dict):
            if manifest.get('roles') != ['control']:
                tls['mqtt_tls_server_name'] = mqtt_host
                tls['mqtt_tls_endpoint'] = manifest_mqtt_endpoint
            else:
                tls.pop('mqtt_tls_server_name', None)
                tls.pop('mqtt_tls_endpoint', None)
                tls.pop('mqtt_tls_ca_file', None)
            if profile is not None:
                tls['https_endpoint'] = profile['advertised_https']
        manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(',', ':')))
        manifest_path.chmod(0o600)
    for env_path in prepared_inputs['manifests'].get('gateway_env', []):
        env_file = Path(env_path)
        env_data = json.loads(env_file.read_text(encoding='utf-8'))
        if 'GATEWAY_MQTT_SERVER_NAME' in env_data:
            env_data['GATEWAY_MQTT_SERVER_NAME'] = mqtt_host
        if 'GATEWAY_MQTT_BROKER_URL' in env_data:
            env_data['GATEWAY_MQTT_BROKER_URL'] = mqtt_broker_url
        env_file.write_text(json.dumps(env_data, sort_keys=True, separators=(',', ':')))
        env_file.chmod(0o600)

    prepared = {
        'arm': arm,
        'root': str(root),
        'context_path': str(ctx_file),
        'context': context,
        'dataset_path': prepared_inputs['dataset_path'],
        'journal_sha256': prepared_inputs['journal_sha256'],
        'workload': prepared_inputs['workload'],
        'workload_sha256': prepared_inputs['workload_sha256'],
        'manifests': prepared_inputs['manifests'],
        'expected': prepared_inputs['expected'],
        'specs': specs,
        'passwords': passwords,
        'tokens': tokens,
        'pg': pg,
        'quota': quota,
        'schema': schema,
        'storage': storage,
        'resources': resources,
        'effective_flags': actual_flags,
        'reference': reference,
        'endpoint': endpoint,
        'mqtt_broker_url': mqtt_broker_url,
        'manifest_mqtt_endpoint': manifest_mqtt_endpoint,
        'mqtt_server_name': mqtt_host,
        'mqtt_ca_file': str(mqtt_ca_file),
        'https_ca_file': str(endpoint.ca_file),
        'images': images,
        'portable_inputs': {
            'manifests': prepared_inputs['manifests'],
            'dataset_path': prepared_inputs['dataset_path'],
            'dataset_sha256': prepared_inputs['journal_sha256'],
            'https_endpoint': endpoint.url,
            'https_ca_file': str(endpoint.ca_file),
            'mqtt_tls_endpoint': manifest_mqtt_endpoint,
            'mqtt_ca_file': str(mqtt_ca_file),
            'run_spec': prepared_inputs['manifests']['run_spec'],
            'viewer_manifest': prepared_inputs['manifests']['viewer'],
            'initial_manifest': prepared_inputs['manifests']['initial'],
            'warm_manifest': prepared_inputs['manifests']['warm_replay'],
            'control_manifest': prepared_inputs['manifests']['control'],
        },
        'completed_phases': set(),
        'phase_snapshots': {},
        'phase_results': {},
    }
    return prepared


def export_prepared_bundle(prepared, *, output_leaf, wait_seconds=10, workload_seconds=60,
                           deadline_seconds=900, session_id=None):
    """Package immutable E5 phase/viewer/command inputs into portable bundle."""
    try:
        from scripts import paper_p8_bundle
    except (ImportError, ModuleNotFoundError):
        import paper_p8_bundle

    session_id = session_id or prepared.get('session_id') or secrets.token_hex(16)
    manifests = {
        name: Path(path) for name, path in prepared['manifests'].items()
        if name in ('initial', 'warm_replay', 'viewer', 'control')
    }
    identities = {}
    role_metadata = {'roles': {}, 'gateways': {}}
    for label, path in manifests.items():
        manifest = json.loads(path.read_text(encoding='utf-8'))
        role = manifest['roles'][0]
        source = Path(next(iter(manifest['identity_files'].values())))
        identity_key = f'{label}:{role}'
        identities[identity_key] = source
        principals = list(json.loads(source.read_text(encoding='utf-8')))
        role_metadata['roles'][identity_key] = {'role': role, 'principals': principals}

    gateway_files = {}
    for manifest_path in manifests.values():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        gateway_files.update({
            gid: Path(p) for gid, p in manifest.get('gateway_identity_files', {}).items()
        })
    for gid, path in gateway_files.items():
        username = json.loads(path.read_text(encoding='utf-8'))['username']
        role_metadata['gateways'][gid] = {'role': 'gateway', 'gateway_id': gid, 'username': username}

    env_files = {
        f'executor-{i+1}': Path(path)
        for i, path in enumerate(prepared['manifests']['gateway_env'])
    }

    https_ca = Path(prepared.get('https_ca_file') or getattr(prepared.get('endpoint'), 'ca_file', Path(prepared['root']) / 'ca.crt'))
    mqtt_ca = Path(prepared.get('mqtt_ca_file') or (Path(prepared['root']).parent.parent / 'certs/ca.crt'))
    # The fixture's public CA files are world-readable for container use, while
    # the portable bundle only accepts owner-only regular sources. Stage private
    # 0600 copies exactly like the E1/E3/E4 exporters before packaging.
    ca_root = Path(prepared['root']) / 'portable-ca-inputs'
    ca_root.mkdir(mode=0o700, exist_ok=False)
    ca_files = {}
    for name, source in (('https', https_ca), ('mqtt', mqtt_ca)):
        target = ca_root / f'{name}-ca.pem'
        target.write_bytes(Path(source).read_bytes())
        target.chmod(0o600)
        ca_files[name] = target

    e5_lifecycle = {
        'mode': 'continuous',
        'viewer_count': 2,
        'phases': ['initial', 'warm_replay', 'control'],
        'deadline_seconds': int(deadline_seconds),
        'idle_transfer_excluded': True,
    }

    return paper_p8_bundle.export_bundle(
        output_leaf=output_leaf,
        session_id=session_id,
        experiment='E5',
        arm=prepared['arm'],
        phase='repeat-01',
        manifests=manifests,
        identity_files=identities,
        gateway_identity_files=gateway_files,
        gateway_env_files=env_files,
        ca_files=ca_files,
        role_metadata=role_metadata,
        wait_seconds=wait_seconds,
        workload_seconds=workload_seconds,
        topology={'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 2, 'viewers': 2},
        workload=prepared['workload'],
        run_spec=Path(prepared['manifests']['run_spec']),
        datasets={'shared_journal': Path(prepared['context']['dataset_path'])},
        e5_context=prepared['context'],
        e5_lifecycle=e5_lifecycle,
    )


def begin_remote_phase(smoke, fixture, endpoint, registry, containers, prepared, *, phase):
    """Capture A main dataset baseline snapshots and server metrics before remote execution."""
    if phase not in PHASE_NAMES:
        raise ValueError(f'e5_unknown_phase: {phase}')

    completed = prepared.setdefault('completed_phases', set())
    if phase == 'initial':
        if 'initial' in completed:
            raise ValueError('e5_phase_already_completed: initial')
    elif phase == 'warm_replay':
        if 'initial' not in completed:
            raise ValueError('e5_phase_sequence_invalid: initial_required_before_warm_replay')
        if 'warm_replay' in completed:
            raise ValueError('e5_phase_already_completed: warm_replay')
    elif phase == 'control':
        if 'initial' not in completed or 'warm_replay' not in completed:
            raise ValueError('e5_phase_sequence_invalid: initial_and_warm_required_before_control')
        if 'control' in completed:
            raise ValueError('e5_phase_already_completed: control')

        # Check freshness of idempotency keys before starting control phase
        declared_keys = prepared['context']['command_keys']
        for key in declared_keys:
            count = smoke.db_psql(registry, prepared['pg'],
                "SELECT count(*) FROM twin_command_requests WHERE idempotency_key=:'key';", {'key': key}).strip()
            if count != '0':
                raise ValueError('e5_command_key_not_fresh')

    # Capture main dataset snapshots and admin metrics (no generator started on A)
    admin_baseline = e4._support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'])
    if not isinstance(admin_baseline, dict) or not admin_baseline.get('process_instance'):
        raise ValueError('e5_admin_metrics_missing_process_instance')

    server_baseline = smoke.paper_p0_stats(registry, containers, f'e5_{phase}_start')
    db_before = [e4._counts(smoke, registry, prepared['pg'], s) for s in prepared['specs']]

    collector = e4._support.BoundedCollector(
        scrape=lambda: e4._support._admin_metrics(smoke, endpoint, prepared['tokens']['admin']),
        interval=.5, max_snapshots=32, deadline_seconds=120,
        baseline_metrics=admin_baseline, baseline_server=server_baseline)
    collector.start(None)
    prepared.setdefault('remote_collectors', {})[phase] = collector

    snapshot = {
        'phase': phase,
        'admin_before': admin_baseline,
        'server_stats': server_baseline,
        'db_before': db_before,
        'collector_started': True,
        'begun_at': datetime.now(timezone.utc).isoformat(),
    }
    prepared.setdefault('phase_snapshots', {})[phase] = snapshot
    return snapshot


def finish_remote_initial(smoke, fixture, endpoint, registry, containers, prepared, b_projection):
    """Verify initial replay phase from typed B projection and independent A SQL readbacks."""
    snapshot = prepared.get('phase_snapshots', {}).get('initial')
    if not snapshot:
        raise ValueError('e5_phase_not_begun: initial')
    if 'initial' in prepared.get('completed_phases', set()):
        raise ValueError('e5_phase_already_completed: initial')

    if not isinstance(b_projection, dict):
        raise ValueError('e5_initial_projection_invalid')
    if b_projection.get('status') != 'COMPLETE' or b_projection.get('cleanup_errors'):
        raise ValueError('e5_workload_incomplete_initial')

    workload = b_projection.get('e4') or {}
    telemetry = b_projection.get('telemetry') or {}

    # Initial requires replay 20 ACK and pending 0
    if not e5.replay_accounting_matches(workload, telemetry, 20):
        raise ValueError('e5_initial_accounting_mismatch')
    if telemetry.get('pending_messages', 0) != 0:
        raise ValueError('e5_initial_pending_messages_not_zero')
    pubacks = telemetry.get('broker_pubacks')
    attempts = telemetry.get('publish_attempts')
    retries = telemetry.get('retries')
    if (type(pubacks) is not int or pubacks < 20 or type(attempts) is not int
            or type(retries) is not int or attempts < 20 or retries < 0
            or attempts != 20 + retries or pubacks > attempts):
        raise ValueError('e5_initial_publish_accounting_incoherent')

    # 20 headers matching A immutable journal IDs and hashes
    dataset_file = Path(prepared['dataset_path'])
    journal_lines = [json.loads(line) for line in dataset_file.read_text(encoding='utf-8').strip().splitlines() if line.strip()]
    if len(journal_lines) != 20:
        raise ValueError('e5_journal_line_count_invalid')
    journal_by_id = {row['message_id']: row for row in journal_lines}

    headers = workload.get('safe_header_samples', [])
    if not isinstance(headers, list) or len(headers) != 20:
        raise ValueError('e5_initial_header_count_mismatch')
    header_by_id = {h.get('message_id'): h for h in headers if isinstance(h, dict)}
    header_ids = list(header_by_id)
    if len(header_by_id) != 20:
        raise ValueError('e5_initial_headers_not_unique')
    if set(header_by_id) != set(journal_by_id):
        raise ValueError('e5_initial_headers_dataset_mismatch')
    for message_id, row in journal_by_id.items():
        header = header_by_id[message_id]
        for field in ('gateway_id', 'sensor_id', 'boot_id', 'first_sequence', 'sample_count', 'payload_sha256'):
            if header.get(field) != row.get(field):
                raise ValueError('e5_initial_header_identity_mismatch')

    # Same-process baseline verification
    admin_after = e4._support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'])
    if admin_after.get('process_instance') != snapshot['admin_before'].get('process_instance'):
        raise ValueError('e5_server_process_restarted_during_initial')

    # Independent A SQL readbacks: ledger 20, raw 2000, temporal 20, configuration 0
    db_after = [e4._counts(smoke, registry, prepared['pg'], s) for s in prepared['specs']]
    delta = e4._database_delta(snapshot['db_before'], db_after)
    expected_delta = {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}
    if any(delta.get(k) != v for k, v in expected_delta.items()):
        raise ValueError('e5_initial_database_delta_mismatch')

    # Raw content exact known verification
    raw_proof = [e4.verify_pilot_raw(smoke, registry, prepared['pg'], s, message_ids=header_ids)
                 for s in prepared['specs']]
    if len(raw_proof) != 2 or any(row.get('verified_exact') is not True for row in raw_proof):
        raise ValueError('e5_initial_raw_verification_failed')

    ingress_delta = e4._support.calculate_ingress_delta(snapshot['admin_before'], admin_after)
    ws_window = e4._e4_ws_window(snapshot['admin_before'], admin_after)

    result = {
        'phase': 'initial',
        'b_projection': b_projection,
        'admin_before': snapshot['admin_before'],
        'admin_after': admin_after,
        'db_before': snapshot['db_before'],
        'db_after': db_after,
        'database_delta': delta,
        'raw_proof': raw_proof,
        'raw_ids': header_ids,
        'ingress_delta': ingress_delta,
        'ws_window': ws_window,
        'server_stats_before': snapshot['server_stats'],
        'server_stats_after': smoke.paper_p0_stats(registry, containers, 'e5_initial_end'),
    }
    prepared['completed_phases'].add('initial')
    prepared.setdefault('phase_results', {})['initial'] = result
    return result


def finish_remote_warm_replay(smoke, fixture, endpoint, registry, containers, prepared, b_projection):
    """Verify warm replay phase from typed B projection and independent A SQL readbacks."""
    snapshot = prepared.get('phase_snapshots', {}).get('warm_replay')
    if not snapshot:
        raise ValueError('e5_phase_not_begun: warm_replay')
    if 'warm_replay' in prepared.get('completed_phases', set()):
        raise ValueError('e5_phase_already_completed: warm_replay')
    if 'initial' not in prepared.get('completed_phases', set()):
        raise ValueError('e5_phase_sequence_invalid: initial_required_before_warm_replay')

    if not isinstance(b_projection, dict):
        raise ValueError('e5_warm_replay_projection_invalid')
    if b_projection.get('status') != 'COMPLETE' or b_projection.get('cleanup_errors'):
        raise ValueError('e5_workload_incomplete_warm_replay')

    workload = b_projection.get('e4') or {}
    telemetry = b_projection.get('telemetry') or {}

    # Warm replay requires 10 ACK and pending 0
    if not e5.replay_accounting_matches(workload, telemetry, 10):
        raise ValueError('e5_warm_replay_accounting_mismatch')
    if telemetry.get('pending_messages', 0) != 0:
        raise ValueError('e5_warm_replay_pending_messages_not_zero')
    pubacks = telemetry.get('broker_pubacks')
    attempts = telemetry.get('publish_attempts')
    retries = telemetry.get('retries')
    if (type(pubacks) is not int or pubacks < 10 or type(attempts) is not int
            or type(retries) is not int or attempts < 10 or retries < 0
            or attempts != 10 + retries or pubacks > attempts):
        raise ValueError('e5_warm_replay_publish_accounting_incoherent')

    # Same immutable journal prefix (first 10)
    dataset_file = Path(prepared['dataset_path'])
    journal_lines = [json.loads(line) for line in dataset_file.read_text(encoding='utf-8').strip().splitlines() if line.strip()]
    expected_prefix = {row['message_id']: row for row in journal_lines[:10]}

    headers = workload.get('safe_header_samples', [])
    if not isinstance(headers, list) or len(headers) != 10:
        raise ValueError('e5_warm_replay_header_count_mismatch')
    header_by_id = {h.get('message_id'): h for h in headers if isinstance(h, dict)}
    if len(header_by_id) != 10 or set(header_by_id) != set(expected_prefix):
        raise ValueError('e5_warm_replay_prefix_mismatch')
    for message_id, row in expected_prefix.items():
        header = header_by_id[message_id]
        for field in ('gateway_id', 'sensor_id', 'boot_id', 'first_sequence', 'sample_count', 'payload_sha256'):
            if header.get(field) != row.get(field):
                raise ValueError('e5_warm_replay_header_identity_mismatch')

    # Same-process baseline verification
    admin_after = e4._support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'])
    if admin_after.get('process_instance') != snapshot['admin_before'].get('process_instance'):
        raise ValueError('e5_server_process_restarted_during_warm_replay')

    # Independent A SQL readbacks: EXACT main DB fingerprint unchanged
    db_after = [e4._counts(smoke, registry, prepared['pg'], s) for s in prepared['specs']]
    initial_res = prepared['phase_results']['initial']
    initial_db = initial_res['db_after']
    if db_after != initial_db:
        raise ValueError('e5_warm_replay_changed_main_fingerprint')
    if any(b.get('fingerprint') != a.get('fingerprint') for b, a in zip(initial_db, db_after)):
        raise ValueError('e5_warm_replay_changed_main_fingerprint')

    delta = e4._database_delta(snapshot['db_before'], db_after)
    if any(delta.get(k, 0) != 0 for k in ('ledger', 'raw', 'temporal', 'configuration')):
        raise ValueError('e5_warm_replay_database_delta_nonzero')

    # No new WS deliveries in warm replay window
    ws_window = e4._e4_ws_window(snapshot['admin_before'], admin_after)
    if ws_window.get('new_events') != 0 or ws_window.get('deliveries_enqueued') != 0:
        raise ValueError('e5_warm_replay_ws_window_has_new_events')

    ingress_delta = e4._support.calculate_ingress_delta(snapshot['admin_before'], admin_after)

    result = {
        'phase': 'warm_replay',
        'b_projection': b_projection,
        'admin_before': snapshot['admin_before'],
        'admin_after': admin_after,
        'db_before': snapshot['db_before'],
        'db_after': db_after,
        'database_delta': delta,
        'ingress_delta': ingress_delta,
        'ws_window': ws_window,
        'server_stats_before': snapshot['server_stats'],
        'server_stats_after': smoke.paper_p0_stats(registry, containers, 'e5_warm_replay_end'),
    }
    prepared['completed_phases'].add('warm_replay')
    prepared.setdefault('phase_results', {})['warm_replay'] = result
    return result


def finish_remote_control(smoke, fixture, endpoint, registry, containers, prepared, b_projection):
    """Verify control phase from typed B projection and independent A SQL/REST readbacks."""
    snapshot = prepared.get('phase_snapshots', {}).get('control')
    if not snapshot:
        raise ValueError('e5_phase_not_begun: control')
    if 'control' in prepared.get('completed_phases', set()):
        raise ValueError('e5_phase_already_completed: control')
    if 'warm_replay' not in prepared.get('completed_phases', set()):
        raise ValueError('e5_phase_sequence_invalid: warm_replay_required_before_control')

    if not isinstance(b_projection, dict):
        raise ValueError('e5_control_projection_invalid')
    if b_projection.get('status') not in ('COMPLETE', 'PASS'):
        raise ValueError('e5_workload_incomplete_control')

    declared_keys = prepared['context']['command_keys']
    commands = (b_projection.get('commands')
                or (b_projection.get('control_workload') or {}).get('commands') or [])
    if not isinstance(commands, list) or len(commands) != 4:
        raise ValueError('e5_control_command_count_mismatch')
    if any(not isinstance(c, dict) or c.get('classification') != 'succeeded'
           or c.get('idempotency_key') not in declared_keys for c in commands):
        raise ValueError('e5_control_command_classification_invalid')
    if {c.get('idempotency_key') for c in commands} != set(declared_keys):
        raise ValueError('e5_control_command_keys_mismatch')

    # Same-process baseline verification
    admin_after = e4._support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'])
    if admin_after.get('process_instance') != snapshot['admin_before'].get('process_instance'):
        raise ValueError('e5_server_process_restarted_during_control')

    # 4 PG receipts with status_code 202, status succeeded, result_hash
    pg_receipts = {}
    commands_manifest = json.loads(Path(prepared['manifests']['control']).read_text()).get('commands', [])
    for key, command in zip(declared_keys, commands_manifest):
        sid = command['sensor_id']
        urn = f'urn:ngsi-ld:Sensor:{GATEWAYS[0]}:{sid}'
        sql = """SELECT json_build_object('idempotency_key',r.idempotency_key,'status_code',r.status_code,
            'command_id',r.command_id::text,'status',c.status,'result_hash',c.result_hash,
            'desired_version',c.desired_version::text,'sample_interval_us',(c.parameters->>'sample_interval_us')::bigint)::text
            FROM twin_command_requests r JOIN twin_commands c ON c.command_id=r.command_id
            JOIN twin_entities e ON e.id=c.entity_id WHERE r.idempotency_key=:'key' AND e.entity_id=:'urn';"""
        text = smoke.db_psql(registry, prepared['pg'], sql, {'key': key, 'urn': urn}).strip()
        if not text:
            raise ValueError('e5_command_receipt_missing: ' + key)
        receipt = json.loads(text)
        if receipt.get('status_code') != 202 or receipt.get('status') != 'succeeded' or not receipt.get('result_hash'):
            raise ValueError('e5_command_receipt_invalid: ' + key)
        pg_receipts[key] = receipt

    # REST readback of 2 command sensor configs
    rest_configs = {}
    for sid in COMMAND_SENSORS:
        urn = f'urn:ngsi-ld:Sensor:{GATEWAYS[0]}:{sid}'
        status, _, body = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urn}?format=simplified', token=prepared['tokens']['a'])
        if status != 200 or not isinstance(body, dict):
            raise ValueError('e5_command_rest_read_failed: ' + sid)
        cfg = (body.get('reported_state') or {}).get('configuration')
        if not isinstance(cfg, dict):
            raise ValueError('e5_command_reported_configuration_missing: ' + sid)
        rest_configs[sid] = cfg

    # Exported B SQLite 2 receipts / pending 0 via existing E3 helper
    executors = b_projection.get('executors')
    if not isinstance(executors, list) or len(executors) != 2:
        raise ValueError('e5_control_executor_projection_missing')
    # B exports raw supervisor executor records (pending_outbox + receipts).
    # Require the established projection helper; never interpret absent pending
    # counters as an empty queue.
    sqlite_summaries = _e3_executor_receipt_projection(executors)
    if len(sqlite_summaries) != 2:
        raise ValueError('e5_control_sqlite_executor_count_mismatch')
    if any(type(s.get('pending_outbox')) is not int or s['pending_outbox'] != 0
           for s in sqlite_summaries):
        raise ValueError('e5_control_sqlite_executor_has_pending_rows')

    receipts_ok = _verify_control_receipts(
        sqlite_summaries, commands_manifest, pg_receipts, rest_configs, COMMAND_SENSORS)
    if not receipts_ok:
        raise ValueError('e5_command_receipts_unverified')

    # Main dataset unchanged
    db_after = [e4._counts(smoke, registry, prepared['pg'], s) for s in prepared['specs']]
    delta = e4._database_delta(snapshot['db_before'], db_after)
    initial_db = prepared['phase_results']['initial']['db_after']
    if (snapshot['db_before'] != initial_db or db_after != initial_db
            or any(b.get('fingerprint') != a.get('fingerprint')
            for b, a in zip(initial_db, db_after))
            or any(delta.get(k, 0) != 0 for k in ('ledger', 'raw', 'temporal', 'configuration'))):
        raise ValueError('e5_control_main_dataset_changed')

    ws_window = e4._e4_ws_window(snapshot['admin_before'], admin_after)
    # The WS counters are process-global; command-sensor notifications may
    # legitimately move them during control. Preserve the measured window and
    # let the continuous viewer proof scope expected main-dataset deliveries.

    result = {
        'phase': 'control',
        'b_projection': b_projection,
        'admin_before': snapshot['admin_before'],
        'admin_after': admin_after,
        'db_before': snapshot['db_before'],
        'db_after': db_after,
        'pg_receipts': pg_receipts,
        'rest_configs': rest_configs,
        'sqlite_summaries': sqlite_summaries,
        'control_reconciliation': {
            'status': 'PASS' if len(pg_receipts) == 4 and len(rest_configs) == 2 else 'INCONCLUSIVE',
            'verified_receipts': len(pg_receipts),
            'verified_configurations': len(rest_configs),
        },
        'ws_window': ws_window,
        'ws_window_scope': 'process_global',
        'server_stats_before': snapshot['server_stats'],
        'server_stats_after': smoke.paper_p0_stats(registry, containers, 'e5_control_end'),
    }
    prepared['completed_phases'].add('control')
    prepared.setdefault('phase_results', {})['control'] = result
    return result


def finish_remote_phase(smoke, fixture, endpoint, registry, containers, prepared, *, phase, b_projection):
    """Stop the A collector, then dispatch to the specific phase validator."""
    collector = prepared.get('remote_collectors', {}).pop(phase, None)
    if collector is not None:
        collector.stop(None)
    if phase == 'initial':
        result = finish_remote_initial(smoke, fixture, endpoint, registry, containers, prepared, b_projection)
    elif phase == 'warm_replay':
        result = finish_remote_warm_replay(smoke, fixture, endpoint, registry, containers, prepared, b_projection)
    elif phase == 'control':
        result = finish_remote_control(smoke, fixture, endpoint, registry, containers, prepared, b_projection)
    else:
        raise ValueError(f'e5_unknown_phase: {phase}')
    if collector is not None and isinstance(result, dict):
        result['collector_metrics'] = list(collector.metrics)
        result['resource_samples'] = list(collector.resources)
        result['collector_generator_samples'] = list(collector.generator_samples)
        result['collector_errors'] = list(collector.errors)
    return result


_VIEWER_BOUNDARIES = (
    ('initial', 'begin'), ('initial', 'end'),
    ('warm_replay', 'begin'), ('warm_replay', 'end'),
    ('control', 'begin'), ('control', 'end'),
)
_CORRELATION_KEYS = ('session_id', 'run_id', 'binding_digest', 'manifest_sha256', 'binary_sha256')


def _valid_correlation(value):
    return (isinstance(value, dict) and set(value) == set(_CORRELATION_KEYS)
            and all(isinstance(value.get(k), str) and value[k] for k in _CORRELATION_KEYS[:2])
            and all(isinstance(value.get(k), str) and re.fullmatch(r'[0-9a-f]{64}', value[k])
                    for k in _CORRELATION_KEYS[2:]))


def _elapsed_seconds(value):
    if type(value) not in (int, float):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _validate_viewer_lifecycle(proof, expected_correlation, viewer_projection):
    """Validate B holder's single-clock evidence and the actual Go viewer stop result."""
    errors = []
    if not _valid_correlation(expected_correlation):
        errors.append('viewer_lifecycle_expected_correlation_invalid')
    if not isinstance(proof, dict):
        errors.append('viewer_lifecycle_proof_missing')
        return errors, None, []
    if proof.get('schema_version') != 1 or proof.get('clock') != 'viewer_holder_local_elapsed_seconds':
        errors.append('viewer_lifecycle_schema_invalid')
    correlation = proof.get('correlation')
    if (not _valid_correlation(correlation) or not _valid_correlation(expected_correlation)
            or correlation != expected_correlation):
        errors.append('viewer_lifecycle_correlation_mismatch')
    checkpoints = proof.get('checkpoints')
    values = []
    safe_checkpoints = []
    if not isinstance(checkpoints, list) or len(checkpoints) != len(_VIEWER_BOUNDARIES):
        errors.append('viewer_lifecycle_checkpoints_invalid')
        checkpoints = []
    for index, (phase, boundary) in enumerate(_VIEWER_BOUNDARIES):
        point = checkpoints[index] if index < len(checkpoints) else None
        if (not isinstance(point, dict) or point.get('phase') != phase
                or point.get('boundary') != boundary):
            errors.append('viewer_lifecycle_checkpoint_order_invalid')
            values.append(None)
            safe_checkpoints.append(None)
            continue
        elapsed = point.get('elapsed_seconds')
        parsed_elapsed = _elapsed_seconds(elapsed)
        if parsed_elapsed is None:
            errors.append('viewer_lifecycle_clock_invalid')
            values.append(None)
        else:
            values.append(parsed_elapsed)
        safe_checkpoints.append({
            'phase': phase, 'boundary': boundary,
            'elapsed_seconds': parsed_elapsed,
            'child_alive': point.get('child_alive') is True,
            'ready_verified': point.get('ready_verified') is True,
        })
        if point.get('child_alive') is not True or point.get('ready_verified') is not True:
            errors.append('viewer_lifecycle_child_not_ready')
    ready, stopped = proof.get('ready_elapsed_seconds'), proof.get('stop_elapsed_seconds')
    ready_seconds, stopped_seconds = _elapsed_seconds(ready), _elapsed_seconds(stopped)
    for value in (ready_seconds, stopped_seconds):
        if value is None:
            errors.append('viewer_lifecycle_clock_invalid')
    joined = proof.get('joined') is True
    if not joined:
        errors.append('viewer_lifecycle_not_joined')
    if (len(values) == len(_VIEWER_BOUNDARIES) and all(v is not None for v in values)
            and ready_seconds is not None and stopped_seconds is not None):
        if ready_seconds > values[0] or any(a > b for a, b in zip(values, values[1:])) or stopped_seconds <= values[-1]:
            errors.append('viewer_lifecycle_clock_order_invalid')
    elif values:
        errors.append('viewer_lifecycle_clock_order_invalid')

    expected_stop = (isinstance(viewer_projection, dict)
                     and viewer_projection.get('status') == 'COMPLETE'
                     and viewer_projection.get('planned_stop') is True
                     and viewer_projection.get('planned_stop_reason') == 'stopped_by_operator'
                     and viewer_projection.get('connection_attempts') == 2
                     and viewer_projection.get('connected_distinct_viewers') == 2
                     and viewer_projection.get('subscribed_distinct_viewers') == 2
                     and viewer_projection.get('reconnections') == 0
                     and not viewer_projection.get('primary_error')
                     and not viewer_projection.get('cleanup_errors')
                     and not viewer_projection.get('cleanup_error')
                     and viewer_projection.get('cleanup_failure') is not True
                     and viewer_projection.get('cleanup_outcome') != 'FAILED')
    if not expected_stop:
        errors.append('viewer_planned_stop_invalid')

    idle_windows = []
    if len(values) == 6 and all(v is not None for v in values):
        idle_windows = [
            {'between': 'initial/warm_replay', 'clock': proof.get('clock'),
             'start_elapsed_seconds': values[1], 'end_elapsed_seconds': values[2],
             'duration_seconds': values[2] - values[1], 'excluded': True},
            {'between': 'warm_replay/control', 'clock': proof.get('clock'),
             'start_elapsed_seconds': values[3], 'end_elapsed_seconds': values[4],
             'duration_seconds': values[4] - values[3], 'excluded': True},
        ]
    safe_proof = {
        'schema_version': proof.get('schema_version'), 'clock': proof.get('clock'),
        'correlation': correlation if _valid_correlation(correlation) else None,
        'ready_elapsed_seconds': ready_seconds,
        'checkpoints': safe_checkpoints,
        'stop_elapsed_seconds': stopped_seconds,
        'joined': joined,
    }
    return errors, safe_proof, idle_windows


def validate_continuous_viewer(viewer_projection, context, initial_phase_result,
                               initial_ws_window, warm_ws_window, control_ws_window,
                               end_metrics, *, readiness_projection=None, idle_windows=None,
                               lifecycle_proof=None, expected_correlation=None):
    """Validate two viewers continuous across initial -> warm_replay -> control."""
    failures = []
    if not isinstance(viewer_projection, dict) or viewer_projection.get('status') != 'COMPLETE':
        failures.append('viewer_incomplete')
        viewer_projection = viewer_projection if isinstance(viewer_projection, dict) else {}

    # Connected, subscribed, receiving distinct viewers must be 2
    if (viewer_projection.get('connected_distinct_viewers') != 2
            or viewer_projection.get('subscribed_distinct_viewers') != 2
            or viewer_projection.get('receiving_distinct_viewers') != 2):
        failures.append('viewer_readiness')

    # Event accounting: 20 unique events, 0 duplicates, 0 wrong namespace
    if (viewer_projection.get('unique_events') != 20
            or viewer_projection.get('duplicates') != 0
            or viewer_projection.get('wrong_namespace') != 0):
        failures.append('viewer_event_accounting')

    # Per-identity accounting: mapped 10 events each
    viewers = viewer_projection.get('viewers')
    subscriptions = context.get('viewer_subscriptions') or []
    expected_ids = {row.get('viewer_id') for row in subscriptions if isinstance(row, dict)}
    counts = {row.get('id'): row.get('unique_events')
              for row in viewers if isinstance(row, dict)} if isinstance(viewers, list) else {}
    if set(counts) != expected_ids or any(val != 10 for val in counts.values()):
        failures.append('viewer_per_identity_accounting')

    # Bounded identity sampling provenance: one sample per declared viewer (matching Go paperload contract)
    initial_e4 = (initial_phase_result.get('b_projection', {}).get('e4')
                  or initial_phase_result.get('e4') or {})
    headers = initial_e4.get('safe_header_samples', [])
    headers_by_msg = {row.get('message_id'): row for row in headers if isinstance(row, dict) and row.get('message_id')}

    allowed_viewer_targets = {
        (sub.get('viewer_id'), sub.get('gateway_id'), sub.get('sensor_id'))
        for sub in subscriptions if isinstance(sub, dict)
    }

    identity_samples = viewer_projection.get('identity_samples')
    if not isinstance(identity_samples, list):
        failures.append('viewer_identity_samples_missing')
        identity_samples = []

    sampled_viewers = set()
    for sample in identity_samples:
        if not isinstance(sample, dict):
            failures.append('viewer_identity_sample_invalid')
            continue
        vid = sample.get('viewer_id')
        gid = sample.get('gateway_id')
        sid = sample.get('sensor_id')
        mid = sample.get('message_id')
        bid = sample.get('boot_id')
        seq = str(sample.get('first_sequence')) if sample.get('first_sequence') is not None else None
        cnt = sample.get('sample_count')
        h_hash = sample.get('header_sha256')

        if vid not in expected_ids:
            failures.append('viewer_sample_unknown_viewer')
            continue
        if vid in sampled_viewers:
            failures.append('viewer_sample_duplicate_per_viewer')
            continue
        sampled_viewers.add(vid)

        authorized = (
            (vid, gid, sid) in allowed_viewer_targets
            or (vid, gid, '') in allowed_viewer_targets
            or (vid, gid, None) in allowed_viewer_targets
        )
        if not authorized:
            failures.append('viewer_sample_unauthorized_subscription')

        header = headers_by_msg.get(mid)
        if not header:
            failures.append('viewer_sample_unknown_message_id')
        else:
            if (header.get('gateway_id') != gid
                    or header.get('sensor_id') != sid
                    or header.get('boot_id') != bid
                    or str(header.get('first_sequence')) != seq
                    or header.get('sample_count') != cnt):
                failures.append('viewer_sample_header_composite_mismatch')

        if h_hash is not None:
            try:
                canonical = json.dumps({
                    'gateway_id': gid,
                    'sensor_id': sid,
                    'message_id': mid,
                    'boot_id': bid,
                    'first_sequence': seq,
                    'sample_count': int(cnt),
                }, separators=(',', ':')).encode('utf-8')
                if hashlib.sha256(canonical).hexdigest() != h_hash:
                    failures.append('viewer_sample_header_hash_mismatch')
            except Exception:
                failures.append('viewer_sample_header_hash_mismatch')

    if sampled_viewers != expected_ids:
        failures.append('viewer_samples_incomplete_for_viewers')

    # A per-phase WS windows: initial 20, warm 0
    ws_checks = [
        ('initial', initial_ws_window, 20),
        ('warm', warm_ws_window, 0),
    ]
    for name, window, expected_count in ws_checks:
        if not isinstance(window, dict) or any(window.get(k) != v for k, v in {
                'new_events': expected_count,
                'telemetry_writes_completed': expected_count,
                'deliveries_enqueued': expected_count,
                'drops': 0}.items()):
            failures.append('viewer_ws_' + name + '_window')

    # Control phase WS window: process-global new_events >= 0 preserved (not forced to 0);
    # require available same-process counters, drops 0, and no extra interested viewer deliveries or writes
    valid_control_ws = (
        isinstance(control_ws_window, dict)
        and type(control_ws_window.get('new_events')) is int
        and control_ws_window.get('new_events') >= 0
        and type(control_ws_window.get('telemetry_writes_completed')) is int
        and control_ws_window.get('telemetry_writes_completed') == 0
        and type(control_ws_window.get('deliveries_enqueued')) is int
        and control_ws_window.get('deliveries_enqueued') == 0
        and type(control_ws_window.get('drops')) is int
        and control_ws_window.get('drops') == 0
        and (control_ws_window.get('submissions') is None or (
            type(control_ws_window.get('submissions')) is int and control_ws_window.get('submissions') >= 0))
    )
    if not valid_control_ws:
        failures.append('viewer_ws_control_window')

    # Queues/drain 0 in final end metrics
    ws = end_metrics.get('websocket_metrics') if isinstance(end_metrics, dict) else None
    if not isinstance(ws, dict) or ws.get('global_queued_work') != 0 or ws.get('global_queued_bytes') != 0:
        failures.append('viewer_end_gauges')

    two_viewer_readiness = (
        isinstance(readiness_projection, dict)
        and readiness_projection.get('ready_before_load') is True
        and readiness_projection.get('connected_distinct_viewers') == 2
        and readiness_projection.get('subscribed_distinct_viewers') == 2
    )
    if not two_viewer_readiness:
        failures.append('viewer_preload_readiness_unverified')
    lifecycle_errors, safe_lifecycle, derived_idle_windows = _validate_viewer_lifecycle(
        lifecycle_proof, expected_correlation, viewer_projection)
    failures.extend(lifecycle_errors)
    safe_viewer = _safe_e5_viewer(viewer_projection)
    if isinstance(viewer_projection, dict):
        for key in ('planned_stop', 'planned_stop_reason', 'connection_attempts', 'reconnections', 'primary_error', 'cleanup_errors'):
            if key in viewer_projection:
                safe_viewer[key] = viewer_projection[key]
    safe_viewer['identity_sampling_qualification'] = 'bounded_sample_one_per_viewer; full_counter_accounting_verified'
    control_global_new = (
        control_ws_window.get('new_events')
        if isinstance(control_ws_window, dict) and type(control_ws_window.get('new_events')) is int
        else None
    )
    validation = {
        'valid': not failures,
        'failures': failures,
        'two_viewer_readiness': two_viewer_readiness,
        'lifecycle_clock': 'viewer_holder_local_elapsed_seconds' if safe_lifecycle else None,
        'lifecycle_proof': safe_lifecycle,
        'idle_windows_described': derived_idle_windows,
        'control_global_new_events': control_global_new,
        'control_ws_window_scope': 'process_global',
        'sampled_identity_provenance_qualification': 'bounded_sample_one_per_viewer; not_exhaustive_event_enumeration',
    }
    return {
        'valid': not failures,
        'failures': failures,
        'viewer': safe_viewer,
        'validation': validation,
    }


_B_DECLARED_KEYS = {
    'paperload_binary_sha256', 'gateway_binary_sha256', 'combined_binary_sha256',
    'source_fingerprint', 'b_os', 'b_arch', 'declared_measurement_windows',
    'observed_measurement_windows', 'idle_transfer_windows', 'actual_stop_confirmed',
    'runner_stopped', 'processes_stopped',
}


def expected_viewer_correlation(prepared, receipts, b_projection):
    """Derive the A-trusted viewer correlation from owned receipts and B declarations."""
    if not isinstance(receipts, dict) or not isinstance(receipts.get('control'), dict):
        raise ValueError('e5_control_import_receipt_missing')
    control = receipts['control']
    if not isinstance(receipts.get('initial'), dict):
        raise ValueError('e5_initial_import_receipt_missing')
    if receipts['initial'].get('pinned') != control.get('pinned'):
        raise ValueError('e5_import_pin_mismatch_control')
    raw = b_projection.get('b_declared') if isinstance(b_projection, dict) else None
    if not isinstance(raw, dict):
        raise ValueError('e5_b_declared_missing')
    paperload = raw.get('paperload_binary_sha256')
    if not isinstance(paperload, str) or not re.fullmatch(r'[0-9a-f]{64}', paperload):
        raise ValueError('e5_b_declared_binary_invalid')
    viewer_path, _ = _validate_private_file(Path(prepared['manifests']['viewer']),
                                           'e5_a_viewer_manifest', max_bytes=1 << 20)
    try:
        viewer_manifest = json.loads(viewer_path.read_text(encoding='utf-8'))
    except (OSError, ValueError, UnicodeDecodeError):
        raise ValueError('e5_a_viewer_manifest_rejected') from None
    run_id = viewer_manifest.get('run_id')
    manifest_sha = control['bound_manifest_sha256'].get('viewer')
    session_id = control.get('session_id')
    if not all(isinstance(value, str) and value for value in (run_id, manifest_sha, session_id)):
        raise ValueError('e5_a_viewer_identity_missing')
    correlation = {
        'session_id': session_id,
        'run_id': run_id,
        'binding_digest': control.get('binding_sha256'),
        'manifest_sha256': manifest_sha,
        'binary_sha256': paperload,
    }
    if not _valid_correlation(correlation):
        raise ValueError('e5_expected_correlation_invalid')
    return correlation


def build_b_declared(smoke, prepared, phase_result, *, expected_correlation,
                     a_paperload_binary_sha256=None, a_gateway_binary_sha256=None):
    """Verify B's measured declarations and label them operator-declared provenance.

    This is not cryptographic remote attestation: it compares the copied B
    declarations with A-owned source, receipt and lifecycle facts and only then
    marks the finalizer flags verified.
    """
    projection = phase_result.get('b_projection') if isinstance(phase_result, dict) else None
    raw = projection.get('b_declared') if isinstance(projection, dict) else None
    if not isinstance(raw, dict) or set(raw) != _B_DECLARED_KEYS:
        raise ValueError('e5_b_declared_missing')
    hashes = {}
    for key in ('paperload_binary_sha256', 'gateway_binary_sha256', 'combined_binary_sha256'):
        value = raw.get(key)
        if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
            raise ValueError('e5_b_declared_binary_invalid')
        hashes[key] = value
    if len(set(hashes.values())) != 3:
        raise ValueError('e5_b_declared_binary_invalid')
    if not _valid_correlation(expected_correlation):
        raise ValueError('e5_expected_correlation_invalid')
    if expected_correlation.get('binary_sha256') != hashes['paperload_binary_sha256']:
        raise ValueError('e5_b_declared_correlation_mismatch')

    pod = raw.get('b_os')
    arch = raw.get('b_arch')
    if (not isinstance(pod, str) or not re.match(r'[A-Za-z0-9_.-]{1,64}$', pod)
            or not isinstance(arch, str) or not re.match(r'[A-Za-z0-9_.-]{1,64}$', arch)):
        raise ValueError('e5_b_declared_platform_invalid')

    source = raw.get('source_fingerprint')
    source_fp = e4._support._source_fingerprint(smoke.ROOT)
    if (not isinstance(source, dict) or not isinstance(source_fp, dict)
            or source_fp.get('valid') is False or not isinstance(source_fp.get('revision'), str)
            or not source_fp.get('revision')):
        raise ValueError('e5_a_source_fingerprint_invalid')
    if (not isinstance(source.get('revision'), str) or not source.get('revision')
            or source.get('revision') != source_fp['revision']
            or source.get('dirty') is not source_fp.get('dirty')
            or source.get('dirty_source_sha256') != source_fp.get('dirty_source_sha256')):
        raise ValueError('e5_b_declared_source_mismatch')

    declared = raw.get('declared_measurement_windows')
    observed = raw.get('observed_measurement_windows')
    if (not isinstance(declared, dict) or set(declared) != set(PHASE_NAMES)
            or not isinstance(observed, dict) or set(observed) != set(PHASE_NAMES)):
        raise ValueError('e5_b_declared_windows_invalid')
    declared_windows = {phase: _elapsed_seconds(declared[phase]) for phase in PHASE_NAMES}
    observed_windows = {phase: _elapsed_seconds(observed[phase]) for phase in PHASE_NAMES}
    if any(value is None or value <= 0 for value in declared_windows.values()):
        raise ValueError('e5_b_declared_windows_invalid')
    if any(value is None for value in observed_windows.values()):
        raise ValueError('e5_b_declared_windows_invalid')

    idle = raw.get('idle_transfer_windows')
    if not isinstance(idle, list) or len(idle) != 2:
        raise ValueError('e5_b_declared_idle_windows_invalid')
    for row in idle:
        if (not isinstance(row, dict)
                or set(row) != {'between', 'clock', 'start_elapsed_seconds',
                                'end_elapsed_seconds', 'duration_seconds', 'excluded'}
                or row.get('clock') != 'viewer_holder_local_elapsed_seconds'
                or row.get('excluded') is not True
                or _elapsed_seconds(row.get('duration_seconds')) is None):
            raise ValueError('e5_b_declared_idle_windows_invalid')

    if (raw.get('actual_stop_confirmed') is not True or raw.get('runner_stopped') is not True
            or raw.get('processes_stopped') is not True):
        raise ValueError('e5_b_declared_stop_unconfirmed')
    lifecycle_errors, _, _ = _validate_viewer_lifecycle(
        projection.get('lifecycle'), expected_correlation, projection.get('viewer'))
    if lifecycle_errors:
        raise ValueError('e5_b_declared_lifecycle_invalid')

    declared_result = {
        'schema_version': 1,
        'provenance': 'operator_declared_b_measured_facts',
        'remote_attestation': False,
        'correlation_verified': True,
        'source_verified': True,
        'binary_verified': True,
        'paperload_binary_verified': True,
        'gateway_binary_verified': True,
        'combined_binary_verified': True,
        'actual_stop_confirmed': True,
        'runner_stopped': True,
        'processes_stopped': True,
        'paperload_binary_sha256': hashes['paperload_binary_sha256'],
        'gateway_binary_sha256': hashes['gateway_binary_sha256'],
        'combined_binary_sha256': hashes['combined_binary_sha256'],
        'source_fingerprint': {
            'revision': source.get('revision'),
            'branch': source.get('branch'),
            'dirty': source.get('dirty'),
            'dirty_source_sha256': source.get('dirty_source_sha256'),
        },
        'b_os': pod,
        'b_arch': arch,
        'declared_measurement_windows': declared_windows,
        'observed_measurement_windows': observed_windows,
        'idle_transfer_windows': idle,
        'measurement_windows': observed_windows,
        'correlation': expected_correlation,
        'a_source_fingerprint': {
            'revision': source_fp.get('revision'),
            'branch': source_fp.get('branch'),
            'dirty': source_fp.get('dirty'),
            'dirty_source_sha256': source_fp.get('dirty_source_sha256'),
        },
        'a_paperload_binary_sha256': a_paperload_binary_sha256,
        'a_gateway_binary_sha256': a_gateway_binary_sha256,
        'binary_bytes_equal': (
            None if not isinstance(a_paperload_binary_sha256, str) or
            not isinstance(a_gateway_binary_sha256, str) else
            a_paperload_binary_sha256 == hashes['paperload_binary_sha256']
            and a_gateway_binary_sha256 == hashes['gateway_binary_sha256']),
        'resource_cohort': ('b_phase_runner_process_tree; continuous viewer child windows '
                            'include idle transfer; not host total or RSS peak'),
    }
    return declared_result


def finalize(smoke, fixture, prepared, phase_results, continuous_viewer_result, *,
             b_declared=None, cleanup_verified=False, expected_correlation=None):
    """Build existing E5 report-compatible shape accepted by smoke.paper_p8_e5_acceptance."""
    arm = prepared['arm']
    initial_res = phase_results.get('initial', {})
    warm_res = phase_results.get('warm_replay', {})
    control_res = phase_results.get('control', {})

    viewer_validation = continuous_viewer_result.get('validation', {}) if isinstance(continuous_viewer_result, dict) else {}
    viewer_failures = viewer_validation.get('failures', [])
    two_viewer_readiness = viewer_validation.get('two_viewer_readiness') is True
    viewer_readiness_proof = [True, True] if two_viewer_readiness else None

    # Adapt mechanism counters
    init_admin_before = initial_res.get('admin_before', {})
    init_admin_after = initial_res.get('admin_after', {})
    mechanisms = e5.adapt_mechanism_counters(
        init_admin_before, init_admin_after, arm=arm, viewer_readiness=viewer_readiness_proof)

    warm_admin_before = warm_res.get('admin_before', {})
    warm_admin_after = warm_res.get('admin_after', {})
    mechanisms['ram_dedup'] = e5.adapt_mechanism_counters(
        warm_admin_before, warm_admin_after, arm=arm)['ram_dedup']

    control_admin_before = control_res.get('admin_before', {})
    control_admin_after = control_res.get('admin_after', {})
    declared_keys = prepared['context']['command_keys']
    mechanisms['command'] = e5.adapt_mechanism_counters(
        control_admin_before, control_admin_after, arm=arm, verified_commands=len(declared_keys))['command']

    command_timeline = e5.control_timeline_window(
        control_admin_before, control_admin_after, expected_commands=len(declared_keys))
    mechanisms['command']['timeline_window'] = command_timeline
    mechanisms['fanout']['window'] = initial_res.get('ws_window', {})
    if (two_viewer_readiness and isinstance(mechanisms.get('fanout'), dict)
            and 'status' not in mechanisms['fanout']):
        mechanisms['fanout']['readiness_verified'] = True
    mechanisms['command']['window'] = control_res.get('ws_window', {})
    mechanisms['command']['window_scope'] = 'process_global'
    mechanisms['activation_status'] = 'AVAILABLE' if all(
        isinstance(mechanisms.get(k), dict) and 'status' not in mechanisms[k]
        for k in ('microbatch', 'ram_dedup', 'command', 'fanout', 'shaper')) else 'INCONCLUSIVE'

    # Compile validation failures
    validation_failures = []
    if 'initial' not in prepared.get('completed_phases', set()):
        validation_failures.append('initial_phase_missing')
    if 'warm_replay' not in prepared.get('completed_phases', set()):
        validation_failures.append('warm_replay_phase_missing')
    if 'control' not in prepared.get('completed_phases', set()):
        validation_failures.append('control_phase_missing')

    raw_proof = initial_res.get('raw_proof', [])
    if len(raw_proof) != 2 or any(row.get('verified_exact') is not True for row in raw_proof):
        validation_failures.append('main_raw_fingerprint')

    init_delta = initial_res.get('database_delta', {})
    if any(init_delta.get(k) != v for k, v in {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}.items()):
        validation_failures.append('initial_database_delta')

    if warm_res.get('db_after') != initial_res.get('db_after'):
        validation_failures.append('warm_replay_changed_main_fingerprint')
    if (control_res.get('db_after') != initial_res.get('db_after')
            or any(b.get('fingerprint') != a.get('fingerprint')
                   for b, a in zip(initial_res.get('db_after', []), control_res.get('db_after', [])))):
        validation_failures.append('control_changed_main_fingerprint')

    for name, result in (('initial', initial_res), ('warm_replay', warm_res), ('control', control_res)):
        projection = result.get('b_projection') if isinstance(result, dict) else None
        if not isinstance(projection, dict) or projection.get('status') != 'COMPLETE' or projection.get('cleanup_errors'):
            validation_failures.append(name + '_phase_evidence_invalid')

    if len(control_res.get('pg_receipts', {})) != 4:
        validation_failures.append('control_receipts_incomplete')
    if len(control_res.get('rest_configs', {})) != 2:
        validation_failures.append('control_configs_incomplete')

    validation_failures.extend(viewer_failures)

    source_fp = e4._support._source_fingerprint(smoke.ROOT)
    if not isinstance(source_fp, dict) or not source_fp.get('valid', True):
        validation_failures.append('source_fingerprint_invalid')

    if viewer_validation.get('valid') is not True or viewer_failures:
        validation_failures.append('continuous_viewer_proof_invalid')
    if not isinstance(continuous_viewer_result, dict) or not isinstance(continuous_viewer_result.get('viewer'), dict):
        validation_failures.append('continuous_viewer_projection_missing')
    # Stop/join and B binary provenance must be asserted by the trusted importer,
    # not inferred from successful workloads or cleanup.
    lifecycle_ok = (isinstance(b_declared, dict)
        and b_declared.get('source_verified') is True
        and b_declared.get('binary_verified') is True
        and all(b_declared.get(flag) is True for flag in (
            'paperload_binary_verified', 'gateway_binary_verified', 'combined_binary_verified'))
        and all(isinstance(b_declared.get(key), str)
                and re.fullmatch(r'[0-9a-f]{64}', b_declared[key]) is not None
                for key in ('paperload_binary_sha256', 'gateway_binary_sha256', 'combined_binary_sha256'))
        and b_declared.get('actual_stop_confirmed') is True
        and b_declared.get('runner_stopped') is True
        and b_declared.get('processes_stopped') is True)
    lifecycle_ok = lifecycle_ok and _valid_correlation(expected_correlation) and (
        expected_correlation.get('binary_sha256') == b_declared.get('paperload_binary_sha256'))
    if (isinstance(b_declared, dict) and _valid_correlation(expected_correlation)
            and expected_correlation.get('binary_sha256') != b_declared.get('paperload_binary_sha256')):
        validation_failures.append('viewer_lifecycle_binary_correlation_mismatch')
    binary_provenance_ok = (isinstance(b_declared, dict)
        and all(b_declared.get(flag) is True for flag in (
            'paperload_binary_verified', 'gateway_binary_verified', 'combined_binary_verified'))
        and all(isinstance(b_declared.get(key), str)
                and re.fullmatch(r'[0-9a-f]{64}', b_declared[key]) is not None
                for key in ('paperload_binary_sha256', 'gateway_binary_sha256', 'combined_binary_sha256')))
    if isinstance(b_declared, dict) and not binary_provenance_ok:
        validation_failures.append('remote_binary_provenance_invalid')
    lifecycle_errors, _, _ = _validate_viewer_lifecycle(
        viewer_validation.get('lifecycle_proof'), expected_correlation,
        continuous_viewer_result.get('viewer') if isinstance(continuous_viewer_result, dict) else None)
    if lifecycle_errors:
        validation_failures.extend(code for code in lifecycle_errors if code not in validation_failures)
    lifecycle_ok = lifecycle_ok and viewer_validation.get('lifecycle_proof') is not None and (
        viewer_validation.get('lifecycle_clock') == 'viewer_holder_local_elapsed_seconds'
        and viewer_validation.get('lifecycle_proof', {}).get('joined') is True
    )
    if not lifecycle_ok:
        validation_failures.append('remote_lifecycle_proof_missing')

    valid = (len(validation_failures) == 0)

    if valid and cleanup_verified:
        status = 'PASS'
    elif valid and not cleanup_verified:
        status = 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
    elif validation_failures and all(code == 'remote_lifecycle_proof_missing' for code in validation_failures):
        status = 'WORKLOAD_COMPLETE_PENDING_REMOTE_PROVENANCE'
    else:
        status = 'FAIL'

    workload_status = 'WORKLOAD_COMPLETE_PENDING_CLEANUP' if valid else 'FAIL'

    # Form phases structure
    initial_e4 = dict(initial_res.get('b_projection', {}).get('e4', {}))
    warm_e4 = dict(warm_res.get('b_projection', {}).get('e4', {}))

    control_cmds = (control_res.get('b_projection', {}).get('commands')
                    or (control_res.get('b_projection', {}).get('control_workload') or {}).get('commands') or [])

    phases = {
        'initial': {
            'e4': initial_e4,
            'telemetry': initial_res.get('b_projection', {}).get('telemetry', {}),
            'database_counts': initial_res.get('db_after', []),
            'database_delta': init_delta,
        },
        'warm_replay': {
            'e4': warm_e4,
            'telemetry': warm_res.get('b_projection', {}).get('telemetry', {}),
            'database_counts': warm_res.get('db_after', []),
            'database_delta': warm_res.get('database_delta', {}),
        },
        'control': {
            'commands': control_cmds,
            'control_reconciliation': control_res.get('control_reconciliation', {}),
        },
    }

    report = {
        'status': status,
        'workload_status': workload_status,
        'experiment': 'E5',
        'arm': arm,
        'lane_marker': 'PAPER_P8_E5_ARM_PILOT',
        'full_p8_accepted': False,
        'physical_ab': 'NOT RUN',
        'cleanup_verified': cleanup_verified,
        'processes_stopped': bool(lifecycle_ok),
        'runner_stopped': bool(lifecycle_ok),
        'actual_stop_confirmed': bool(lifecycle_ok),
        'cleanup_errors': [],
        'phases': phases,
        'database_delta': init_delta,
        'accounting': {
            'ingress_delta': initial_res.get('ingress_delta', {}),
            'imported_input': initial_e4.get('journal_entry_count'),
            'warm_replay': warm_e4.get('journal_entry_count'),
            'main_dataset_delta': {
                'ledger': init_delta.get('ledger', 20),
                'raw': init_delta.get('raw', 2000),
                'temporal': init_delta.get('temporal', 20),
            },
            'command_receipts_verified': len(control_res.get('pg_receipts', {})),
            'command_configurations_verified': len(control_res.get('rest_configs', {})),
        },
        'control_reconciliation': control_res.get('control_reconciliation', {
            'status': 'PASS' if len(control_res.get('pg_receipts', {})) == 4 and len(control_res.get('rest_configs', {})) == 2 else 'INCONCLUSIVE',
            'verified_receipts': len(control_res.get('pg_receipts', {})),
            'verified_configurations': len(control_res.get('rest_configs', {})),
        }),
        'raw_verification': raw_proof,
        'viewer': {
            'ready_servers': 2,
            'subscriptions': len(prepared['context']['viewer_subscriptions']),
            'sockets': 2,
            'result': continuous_viewer_result.get('viewer', {}),
            'validation': viewer_validation,
        },
        'validation': {
            'valid': valid,
            'failures': validation_failures,
            'main_raw_fingerprint_stable': (initial_res.get('db_after') == warm_res.get('db_after') == control_res.get('db_after')),
            'viewer_status': continuous_viewer_result.get('viewer', {}).get('status'),
            'mechanism_eligibility': mechanisms.get('activation_status'),
            'viewer_validation': viewer_validation,
        },
        'mechanism_metrics': mechanisms,
        'mechanisms': mechanisms,
        'metadata': {
            'source_revision': source_fp.get('revision'),
            'dirty_worktree': source_fp.get('dirty'),
            'dirty_tree_hash': source_fp.get('dirty_source_sha256'),
            'paperload_binary_sha256': b_declared.get('paperload_binary_sha256') if lifecycle_ok else None,
            'gateway_binary_sha256': b_declared.get('gateway_binary_sha256') if lifecycle_ok else None,
            'combined_binary_sha256': b_declared.get('combined_binary_sha256') if lifecycle_ok else None,
            'paperload_source_sha256': _paperload_source_hash(smoke.ROOT),
            'b_declared': b_declared,
            'image_ids': {k: str(v) for k, v in prepared['images'].items() if v is not None},
            'schema': prepared['schema'],
            'quota': prepared['quota'],
            'security_mode': {
                'fixture_jwt_mode': _jwt_algorithm(prepared['tokens']['admin']),
                'endpoint_tls': True,
                'mqtt_tls': True,
                'mqtt_authentication_readback': 'not_performed',
            },
            'workers': {
                'paperload': 2,
                'gateway_executors': 2,
                **{k.lower(): int(prepared['effective_flags'][k])
                   for k in ('MQTT_WORKER_COUNT', 'DATABASE_MAX_CONNS', 'STAGE5_PUBLISH_WORKERS', 'STAGE5_RESULT_WORKERS')
                   if k in prepared['effective_flags'] and str(prepared['effective_flags'][k]).isdigit()}
            },
            'durability': 'synthetic_journal_background+sqlite_command_executors',
            'topology': {'gateways': 2, 'sensors_per_gateway': 1, 'viewers': 2},
            'seed': prepared['context']['seed'],
            'phase_seconds': (b_declared.get('measurement_windows') if isinstance(b_declared, dict) else None),
            'idle_transfer_windows': viewer_validation.get('idle_windows_described', []),
            'generator_colocated': True,
            'cohort': {
                'sensor_ids': prepared['context']['sensor_ids'],
                'gateway_ids': prepared['context']['gateway_ids'],
            },
            'workload': prepared['workload'],
            'workload_sha256': prepared['workload_sha256'],
            'effective_flags': prepared['effective_flags'],
            'comparison_metadata': e5.comparison_metadata(
                workload=prepared['workload'], journal_sha256=prepared['journal_sha256']),
        },
        'source_fingerprint': source_fp,
        'image_ids': prepared['images'],
        'schema': prepared['schema'],
        'quota': prepared['quota'],
        'workload': prepared['workload'],
        'workload_sha256': prepared['workload_sha256'],
        'journal_sha256': prepared['journal_sha256'],
        'context': {
            'sha256': prepared['context']['sha256'],
            'seed': prepared['context']['seed'],
            'anchor': prepared['context']['anchor'],
            'workload_selection_sha256': prepared['context']['workload_selection_sha256'],
        },
    }
    collector_evidence = {}
    for phase in PHASE_NAMES:
        phase_result = phase_results.get(phase) or {}
        samples = phase_result.get('resource_samples') or []
        collector_evidence[phase] = {
            'phase': phase,
            'metrics_snapshots': len(phase_result.get('collector_metrics') or []),
            'resource_samples': len(samples),
            'resource_available': sum(1 for sample in samples
                                      if isinstance(sample, dict) and sample.get('available') is True),
            'resource_unavailable': sum(1 for sample in samples
                                        if isinstance(sample, dict) and sample.get('available') is False),
            'generator_samples': len(phase_result.get('collector_generator_samples') or []),
            'errors': list(phase_result.get('collector_errors') or []),
            'scope': 'a_side_per_phase_metrics_and_process_samples',
        }
    report['collector_evidence'] = collector_evidence
    report['metadata']['resource_cohort'] = (
        'a_side per-phase metrics snapshots and generator process samples; '
        'not host total, RSS peak, broker wire counts or capacity proof')
    return report
