"""Native E1 pilot orchestration; called by the enclosing disposable fixture."""
import hashlib
import importlib.util
import os
import stat
import json
import re
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone
from types import SimpleNamespace

_support_spec = importlib.util.spec_from_file_location(
    'paper_p8_support_runtime', Path(__file__).with_name('paper_p8_support.py'))
_support = importlib.util.module_from_spec(_support_spec)
_support_spec.loader.exec_module(_support)
BoundedCollector = _support.BoundedCollector
build_e1_mapping = _support.build_e1_mapping
build_e1_execution_plan = _support.build_e1_execution_plan
sensor_scoped_delta = _support.sensor_scoped_delta
validate_mqtt_readback = _support.validate_mqtt_readback
verify_pilot_raw = _support.verify_pilot_raw
_admin_metrics = _support._admin_metrics
metrics_window = _support.metrics_window


_WS_DRAIN_ZERO_FIELDS = ('sessions', 'subscriptions', 'session_reference_bytes',
    'global_queued_work', 'global_queued_bytes', 'session_queued_in_flight_bytes')
_WS_COUNTER_FIELDS = ('fanout_indexed_selections', 'fanout_scan_evaluations',
    'deliveries_enqueued', 'telemetry_writes_completed', 'drops', 'slow_closes',
    'aggregate_capacity_closes', 'recheck_sessions_examined', 'recheck_distinct_keys',
    'recheck_queries_started', 'recheck_timeouts')
_WS_HISTOGRAM_FIELDS = ('confirmed_to_write_seconds', 'confirmed_to_dispatch_seconds',
    'confirmed_to_submit_seconds', 'recheck_duration_seconds')


def _drain_websocket_metrics(scrape, *, clock=time.monotonic, sleep=time.sleep,
                             deadline_seconds=5, max_scrapes=20):
    """Wait boundedly for every exported WS workload gauge to reach known zero."""
    deadline = clock() + deadline_seconds
    last_error = None
    for attempt in range(max_scrapes):
        try:
            snapshot = scrape()
            ws = snapshot.get('websocket_metrics') if isinstance(snapshot, dict) else None
            if not isinstance(ws, dict) or any(type(ws.get(key)) is not int for key in _WS_DRAIN_ZERO_FIELDS):
                last_error = 'metrics_missing_or_invalid'
            elif all(ws[key] == 0 for key in _WS_DRAIN_ZERO_FIELDS):
                return snapshot
            else:
                last_error = 'metrics_nonzero'
        except Exception:
            last_error = 'metrics_scrape_error'
        if attempt + 1 >= max_scrapes or clock() >= deadline:
            break
        sleep(min(0.1, max(0, deadline - clock())))
    raise ValueError('p8_e1_websocket_drain_rejected:' + (last_error or 'deadline'))


def websocket_metrics_window(before, after, histogram_helper, expected_deliveries):
    """Report same-process cumulative-counter deltas and histogram windows."""
    result = {'availability': 'unavailable', 'reason': None, 'counters': {}, 'histograms': {}}
    if not isinstance(before, dict) or not isinstance(after, dict):
        result['reason'] = 'metrics_missing'
        return result
    if not before.get('process_instance') or not after.get('process_instance'):
        result['reason'] = 'process_instance_missing'
        return result
    if before['process_instance'] != after['process_instance']:
        result['reason'] = 'process_instance_changed'
        return result
    old = before.get('websocket_metrics'); new = after.get('websocket_metrics')
    if not isinstance(old, dict) or not isinstance(new, dict):
        result['reason'] = 'websocket_metrics_missing'
        return result
    deltas = {}
    for key in _WS_COUNTER_FIELDS:
        if type(old.get(key)) is not int or type(new.get(key)) is not int:
            result['reason'] = 'counter_missing:' + key
            return result
        delta = new[key] - old[key]
        if delta < 0:
            result['reason'] = 'counter_reset:' + key
            return result
        deltas[key] = delta
    if new.get('fanout_mode') != 'indexed':
        result['reason'] = 'fanout_not_indexed'
        return result
    if deltas['fanout_indexed_selections'] <= 0:
        result['reason'] = 'indexed_fanout_not_observed'
        return result
    if deltas['deliveries_enqueued'] != expected_deliveries:
        result['reason'] = 'delivery_count_mismatch'
        return result
    if deltas['telemetry_writes_completed'] != expected_deliveries:
        result['reason'] = 'telemetry_write_count_mismatch'
        return result
    if deltas['drops'] != 0:
        result['reason'] = 'websocket_drops_observed'
        return result
    windows = {}
    for key in _WS_HISTOGRAM_FIELDS:
        windows[key] = metrics_window(before, after, 'websocket_metrics', key, histogram_helper)
        if windows[key].get('available') is None and windows[key].get('availability') != 'available':
            result['reason'] = 'histogram_unavailable:' + key + ':' + str(windows[key].get('reason', 'unknown'))
            return result
    result.update(availability='available', reason=None, counters=deltas, histograms=windows,
                  gauge_values={key: new.get(key) for key in _WS_DRAIN_ZERO_FIELDS},
                  fanout_mode=new['fanout_mode'])
    return result


def mixed_e1_mapping(targets, *, seed=86):
    """Compose three deterministic density plans without crossing user gateways."""
    mappings = [build_e1_mapping(targets=targets, viewers=64, users=2,
                                 density=density, seed=seed)
                for density in ('sparse', 'dense', 'overlap')]
    rows = []
    for index in range(64):
        source = mappings[index % len(mappings)]['viewers'][index]
        row = {'viewer_id': source['viewer_id'], 'user_id': source['user_id'],
               'filters': [dict(item) for item in source['filters']]}
        rows.append(row)
    recipients = {f"{t['gateway_id']}\0{t['sensor_id']}": [] for t in targets}
    owner_gateway = {f'user-{i:03d}': gw for i, gw in enumerate(sorted({t['gateway_id'] for t in targets}))}
    for row in rows:
        if any(f['gateway_id'] != owner_gateway[row['user_id']] for f in row['filters']):
            raise ValueError('e1_cross_gateway_filter_rejected')
        for target in targets:
            if any(f['gateway_id'] == target['gateway_id'] and
                   (not f.get('sensor_id') or f['sensor_id'] == target['sensor_id'])
                   for f in row['filters']):
                recipients[f"{target['gateway_id']}\0{target['sensor_id']}"].append(row['viewer_id'])
    if any(not values for values in recipients.values()):
        raise ValueError('e1_target_without_recipients')
    return {'seed': seed, 'targets': targets, 'viewers': rows,
            'expected_recipients_by_target': recipients,
            'expected_deliveries_by_target': {key: len(value) * 5 for key, value in recipients.items()},
             'counts': {'sockets': 64, 'users': 2, 'targets': len(targets),
                        'receiving_viewers': len({v for values in recipients.values() for v in values})}}


def role_artifacts(root):
    """Read actual run-spec role output locations and fail closed if incomplete."""
    result = {}
    for role in ('viewer', 'telemetry'):
        path = Path(root) / 'plan' / f'{role}-output' / 'workload.json'
        try:
            value = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise ValueError(f'p8_e1_{role}_artifact_missing_or_invalid') from None
        if not isinstance(value, dict) or value.get('status') != 'COMPLETE':
            raise ValueError(f'p8_e1_{role}_artifact_incomplete')
        result[role] = value
    return result


def safe_effective_flags(effective):
    """Project only explicitly approved backend flags; never persist Compose env."""
    allowed = ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
        'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
        'WS_MAX_SESSIONS', 'WS_SESSION_REFERENCE_BYTES', 'WS_FANOUT_MODE',
        'WS_METRICS_INTERVAL_SECONDS', 'WS_MEMBERSHIP_RECHECK_SECONDS',
        'WS_MAX_MEMBERSHIP_RECHECK_SECONDS', 'WS_MAX_FRAME_BYTES',
        'WS_MAX_SUBSCRIPTIONS', 'WS_MAX_VIEWERS_PER_EVENT')
    env = effective.get('services', {}).get('backend', {}).get('environment', {})
    return {key: env[key] for key in allowed if key in env}


def e1_role_manifests(common, *, subscriptions, viewer_identity, telemetry_identity,
                      gateway_refs, root):
    """Build E1 role inputs without assigning telemetry-only settings to viewers."""
    viewer_common = {key: value for key, value in common.items() if key != 'durability_mode'}
    viewer = dict(viewer_common, experiment='E1', roles=['viewer'], subscriptions=subscriptions,
        phases={'warmup_seconds': 0, 'measure_seconds': 14, 'drain_seconds': 8, 'cooldown_seconds': 0},
        viewer={'mode': 'healthy'},
        limits={'workers': 2, 'connections': 64, 'pending_messages': 64, 'pending_bytes': 1048576,
                'wall_seconds': 30, 'retries': 2, 'disk_growth_bytes': 16777216},
        identity_files={'viewer': str(viewer_identity)}, bundle_path=str(root))
    telemetry = dict(common, experiment='E2', roles=['telemetry'], subscriptions=[],
        topology={'gateways': 2, 'sensors_per_gateway': 2, 'users': 0, 'viewers': 0},
        phases={'warmup_seconds': 0, 'measure_seconds': 4, 'drain_seconds': 8, 'cooldown_seconds': 0},
        limits={'workers': 2, 'connections': 2, 'pending_messages': 64, 'pending_bytes': 1048576,
                'wall_seconds': 30, 'retries': 2, 'disk_growth_bytes': 16777216},
        identity_files={'telemetry': str(telemetry_identity)},
        gateway_identity_files=gateway_refs, bundle_path=str(root),
        durability_mode='synthetic-load')
    return viewer, telemetry


def validate_e1_result(workload, mapping, *, database_delta, raw_verification,
                       viewer_sessions, sampled_headers, expected_headers):
    """Fail closed on workload totals, per-viewer deliveries, DB and WS identities."""
    telemetry = workload.get('telemetry', {}) if isinstance(workload, dict) else {}
    counters = workload.get('viewer', {}) if isinstance(workload, dict) else {}
    failures = []
    wanted = {'generated_unique': 20, 'offered_unique': 20, 'publish_attempts': 20,
              'broker_pubacks': 20, 'application_acks': 20, 'pending_messages': 0,
              'pending_bytes': 0}
    failures += [f'telemetry_{key}' for key, value in wanted.items() if telemetry.get(key) != value]
    if workload.get('status') != 'COMPLETE': failures.append('workload_status')
    if (viewer_sessions.get('connected_distinct_viewers') != 64 or
            viewer_sessions.get('subscribed_distinct_viewers') != 64 or
            viewer_sessions.get('receiving_distinct_viewers') != 64):
        failures.append('viewer_session_totals')
    rows = viewer_sessions.get('viewers', [])
    viewers = {r.get('id'): r for r in rows if isinstance(r, dict)}
    if (len(viewers) != 64 or set(viewers) != {r['viewer_id'] for r in mapping['viewers']} or
            viewer_sessions.get('duplicates') != 0 or viewer_sessions.get('wrong_namespace') != 0 or
            any(r.get('duplicates') != 0 or r.get('wrong_namespace', 0) != 0 for r in viewers.values())):
        failures.append('viewer_identity_set')
    else:
        expected = {r['viewer_id']: sum(5 for key, ids in mapping['expected_recipients_by_target'].items()
                                         if r['viewer_id'] in ids) for r in mapping['viewers']}
        if {k: v.get('unique_events') for k, v in viewers.items()} != expected:
            failures.append('viewer_delivery_counts')
    if database_delta != {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}:
        failures.append('database_delta')
    if (len(raw_verification) != 4 or not all(
            x.get('verified_exact') and x.get('ledger_count') == 5 and
            x.get('raw_identity_value_mismatches') == 0 for x in raw_verification)):
        failures.append('raw_identity')
    header_ids = set(expected_headers) if not isinstance(expected_headers, dict) else set(expected_headers)
    sampled_ids = {x.get('message_id') if isinstance(x,dict) else x for x in sampled_headers}
    if sampled_ids - header_ids or not sampled_headers or len(sampled_headers) > 20:
        failures.append('websocket_producer_headers')
    # Samples are global-bounded and may repeat a message across distinct viewers.
    samples = viewer_sessions.get('identity_samples', [])
    identities = ('gateway_id','sensor_id','message_id','boot_id','first_sequence','sample_count')
    invalid_sample = False
    for sample in samples:
        if not isinstance(sample,dict):
            invalid_sample=True; continue
        expected = expected_headers.get(sample.get('message_id')) if isinstance(expected_headers,dict) else None
        if sample.get('message_id') not in header_ids:
            invalid_sample=True; continue
        if expected is not None and any(sample.get(k) != expected.get(k) for k in identities):
            invalid_sample=True
        viewer = viewers.get(sample.get('viewer_id'))
        mapping_viewer = next((r for r in mapping['viewers'] if r['viewer_id']==sample.get('viewer_id')),None)
        if not viewer or not mapping_viewer or not any(
                f['gateway_id']==sample.get('gateway_id') and
                (not f.get('sensor_id') or f['sensor_id']==sample.get('sensor_id'))
                for f in mapping_viewer['filters']):
            invalid_sample=True
    if not samples or len(samples) > 20 or invalid_sample:
        failures.append('websocket_identity_samples')
    return {'valid': not failures, 'failures': failures}


def prepare(smoke, fixture, endpoint, registry, containers, images, state, *,
            session_id=None, phase='pilot', mode=None, arm='armpilot-e1', output_root=None):
    """Provision E1's two Gateway/four Sensor inputs and build its Go plan.

    This seam is shared by the historical local E1 lane and the remote-prepared
    server path. It does not start the paperload child or a producer.
    """
    pg, quota, effective, resources, storage_status, tokens, specs, passwords, sensor_path = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, state))
    schema = smoke.db_psql(registry, pg,
        'SELECT version::text || \':\' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;').strip()
    if schema != '24:telemetry_ledger_timestamp': raise ValueError('p8_schema_terminal_rejected')
    # Add one separately provisioned sensor under each already-provisioned gateway.
    for index, spec in enumerate(specs):
        status, _, _ = smoke.http_request(endpoint, 'PUT', f"/v1/gateways/{spec.gateway_id}/sensors/p8_e1_second",
            token=tokens['a' if index == 0 else 'b'], body={'name': 'P8 E1 second sensor', 'unit': 'unit'})
        if status != 201: raise ValueError('p8_e1_sensor_provision_failed')
    targets = []
    for spec in specs:
        targets.extend([{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id},
                        {'gateway_id': spec.gateway_id, 'sensor_id': 'p8_e1_second'}])
    mapping = mixed_e1_mapping(targets)
    broker = registry.resolve(containers['mosquitto'])
    ports = broker.get('NetworkSettings', {}).get('Ports', {}).get('8883/tcp') or []
    if len(ports) != 1: raise ValueError('p8_mqtt_mapping_readback_rejected')
    mqtt_readback = {'service': 'mosquitto', 'host_ip': ports[0].get('HostIp'), 'target': 8883,
                     'published': int(ports[0].get('HostPort', '0'))}
    remote_profile = getattr(fixture, 'remote_network_config', None)
    if remote_profile:
        smoke.paper_p8_remote_support().validate_runtime_mapping(
            'mosquitto', broker.get('NetworkSettings', {}).get('Ports') or {}, remote_profile)
        mqtt_endpoint = remote_profile['advertised_mqtt_tls']
        https_endpoint = remote_profile['advertised_https']
        mode = mode or 'remote'
    else:
        validate_mqtt_readback(mqtt_readback, owned_service='mosquitto')
        mqtt_endpoint = f"mqtts://127.0.0.1:{mqtt_readback['published']}"
        https_endpoint = endpoint.url
        mode = mode or 'local'
    ca_path = fixture.private / 'certs/ca.crt'
    remote_session = session_id is not None
    session_id = session_id or fixture.project + '-local'
    run_id = 'p8e1-' + session_id if remote_session else fixture.project + '-e1'
    root = Path(output_root) if output_root else fixture.private / 'p8-e1'
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    targets_db = [SimpleNamespace(gateway_id=t['gateway_id'], sensor_id=t['sensor_id'],
                                  sensor_urn=next((s.sensor_urn for s in specs if s.gateway_id == t['gateway_id']), '')
                                  if t['sensor_id'] != 'p8_e1_second' else f"urn:ngsi-ld:Sensor:{t['gateway_id']}:p8_e1_second") for t in targets]
    # Private identity files stay mode 0600 and are role separated.
    viewer_identity = root / 'viewer-identities.json'
    smoke.private_write(viewer_identity, json.dumps({f'user-{i:03d}': tokens['a' if i == 0 else 'b'] for i in range(2)}, sort_keys=True))
    telemetry_identity = root / 'telemetry-identities.json'; smoke.private_write(telemetry_identity, '{}')
    gateway_refs = {}
    for s, password in zip(specs, passwords):
        gateway_identity = root / f'gateway-{s.gateway_id}.json'
        smoke.private_write(gateway_identity, json.dumps({'username': s.gateway_id, 'password': password}, sort_keys=True))
        gateway_refs[s.gateway_id] = str(gateway_identity)
    tls = {'https_endpoint': https_endpoint, 'https_ca_file': str(endpoint.ca_file),
           'mqtt_tls_endpoint': mqtt_endpoint, 'mqtt_tls_ca_file': str(ca_path),
           'mqtt_tls_server_name': (remote_profile['advertised_host'] if remote_profile else 'localhost')}
    subscriptions = [{'viewer_id': row['viewer_id'], 'user_id': row['user_id'],
                      'gateway_id': f['gateway_id'], **({'sensor_id': f['sensor_id']} if f.get('sensor_id') else {})}
                     for row in mapping['viewers'] for f in row['filters']]
    common = {'schema_version': 1, 'arm': arm, 'profile': 'pilot', 'mode': mode,
        'run_id': run_id, 'seed': 86, 'samples_per_message': 100,
        'topology': {'gateways': 2, 'sensors_per_gateway': 2, 'users': 2, 'viewers': 64},
        'targets': targets, 'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0},
        'tls': tls, 'retry_interval_ms': 200, 'publish_timeout_ms': 3000,
        'bundle_path': str(root)}
    viewer, telemetry = e1_role_manifests(common, subscriptions=subscriptions,
        viewer_identity=viewer_identity, telemetry_identity=telemetry_identity,
        gateway_refs=gateway_refs, root=root)
    paths=[]
    for name, value in (('viewer', viewer), ('telemetry', telemetry)):
        p=root/f'{name}.json'; smoke.private_write(p, json.dumps(value, sort_keys=True, separators=(',', ':'))); paths.append(p)
    plan=build_e1_execution_plan(output_dir=root/'plan', viewer_manifest=paths[0], telemetry_manifest=paths[1], seed=86, mode=mode)
    return {'pg':pg, 'quota':quota, 'effective':effective, 'resources':resources,
        'storage_status':storage_status, 'tokens':tokens, 'specs':specs, 'passwords':passwords,
        'sensor_path':sensor_path, 'targets':targets, 'targets_db':targets_db, 'mapping':mapping,
        'mqtt_endpoint':mqtt_endpoint, 'https_endpoint':https_endpoint, 'run_id':run_id,
        'https_ca_file':endpoint.ca_file, 'mqtt_ca_file':ca_path,
        'root':root, 'viewer_identity':viewer_identity, 'telemetry_identity':telemetry_identity,
        'gateway_refs':gateway_refs, 'mode':mode, 'tls':tls, 'subscriptions':subscriptions,
        'common':common, 'viewer_manifest':viewer, 'telemetry_manifest':telemetry,
        'manifest_paths':paths, 'plan':plan, 'session_id':session_id, 'phase':phase,
        'schema':schema}


def export_prepared_bundle(prepared, *, output_leaf, wait_seconds=90, workload_seconds=30):
    """Export only client inputs; never include admin credentials or A state."""
    try:
        from scripts import paper_p8_bundle
    except ModuleNotFoundError:
        bundle_spec = importlib.util.spec_from_file_location(
            'paper_p8_bundle_runtime', Path(__file__).parents[1] / 'paper_p8_bundle.py')
        paper_p8_bundle = importlib.util.module_from_spec(bundle_spec)
        bundle_spec.loader.exec_module(paper_p8_bundle)
    root = Path(output_leaf).absolute()
    root.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    ca_root = Path(prepared['root']) / 'bundle-ca-inputs'
    ca_root.mkdir(mode=0o700, exist_ok=True)
    ca_files = {}
    for name, source in (('https', prepared['https_ca_file']), ('mqtt', prepared['mqtt_ca_file'])):
        private_copy = ca_root / f'{name}-ca.pem'
        private_copy.write_bytes(Path(source).read_bytes())
        private_copy.chmod(0o600)
        ca_files[name] = private_copy
    identity_files = {'viewer': prepared['viewer_identity'],
                      'telemetry': prepared['telemetry_identity']}
    gateway_files = prepared['gateway_refs']
    principals = {role: sorted(json.loads(Path(path).read_text()).keys())
                  for role, path in identity_files.items()}
    role_metadata = {
        'roles': {role: {'role': role, 'principals': ids} for role, ids in principals.items()},
        'gateways': {gateway_id: {'role': 'gateway', 'gateway_id': gateway_id,
                                  'username': json.loads(Path(path).read_text())['username']}
                     for gateway_id, path in gateway_files.items()},
    }
    manifests = {'viewer': prepared['plan']['run_spec'] and
                 json.loads(Path(prepared['plan']['run_spec']).read_text())['paperload'][0]['manifest'],
                 'telemetry': json.loads(Path(prepared['plan']['run_spec']).read_text())['paperload'][1]['manifest']}
    # The builder's generated manifest paths are the exact source inputs in its run-spec.
    spec = json.loads(Path(prepared['plan']['run_spec']).read_text())
    workload = {'experiment': 'E1', 'arm': prepared['common']['arm'],
        'phase': prepared['phase'], 'profile': 'pilot', 'workload': 'e1-wss-mqtt-pilot',
        'seed': 86, 'samples_per_message': 100, 'unique_batches': 20,
        'rate_per_second': 5, 'measure_seconds': 4,
        'generator_colocated': 'unverified'}
    return paper_p8_bundle.export_bundle(output_leaf=root,
        session_id=prepared['session_id'], experiment='E1', arm=prepared['common']['arm'],
        phase=prepared['phase'], manifests=manifests, identity_files=identity_files,
        gateway_identity_files=gateway_files, gateway_env_files={},
        ca_files=ca_files,
        role_metadata=role_metadata, wait_seconds=wait_seconds,
        workload_seconds=workload_seconds,
        topology={'gateways': prepared['common']['topology']['gateways'],
                  'sensors': len(prepared['targets']),
                  'sensors_per_gateway': prepared['common']['topology']['sensors_per_gateway'],
                  'users': prepared['common']['topology']['users'],
                  'viewers': prepared['common']['topology']['viewers']}, workload=workload,
        run_spec=prepared['plan']['run_spec'])


def _private_result_file(root, relative, limit):
    root = Path(root).absolute()
    path = root / relative
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ValueError('result_path_symlink_rejected')
    if not path.is_relative_to(root):
        raise ValueError('result_path_escape_rejected')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0))
    with os.fdopen(fd, 'rb') as stream:
        st = os.fstat(stream.fileno())
        if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1
                or stat.S_IMODE(st.st_mode) != 0o600 or st.st_size > limit):
            raise ValueError('result_file_rejected')
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ValueError('result_file_too_large')
    return data


def _load_result_bundle(results_root, prepared, *, phase_id, binary_sha256):
    root = Path(results_root).absolute()
    for part in (root, *root.parents):
        if part.is_symlink():
            raise ValueError('result_root_symlink_rejected')
    st = root.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
        raise ValueError('result_root_rejected')
    summary = json.loads(_private_result_file(root, 'results.json', 1 << 20))
    expected = {
        'results/runner/run.json': 'runner',
        'results/runner/provenance.json': 'provenance',
        'results/workloads/viewer/workload.json': 'viewer',
        'results/workloads/telemetry/workload.json': 'telemetry',
    }
    files = summary.get('files') if isinstance(summary, dict) else None
    if (not isinstance(summary, dict) or summary.get('schema_version') != 1 or summary.get('full_p8_accepted') is not False
            or summary.get('physical_ab') != 'NOT RUN'
            or summary.get('session') != {'session_id': prepared['session_id'],
                'run_ids': [prepared['run_id']], 'experiment': 'E1',
                'arm': prepared['common']['arm'], 'phase': phase_id}
            or summary.get('source_bundle_sha256') != prepared['bundle_sha256']
            or summary.get('runner_status') not in ('COMPLETE','PARTIAL','FAILED')
            or summary.get('expected_workload_roles') != ['telemetry', 'viewer']
            or summary.get('present_workload_roles') != ['telemetry', 'viewer']
            or summary.get('missing_workload_roles') != []
            or not isinstance(files, list) or len(files) != len(expected)):
        raise ValueError('result_bundle_correlation_rejected')
    bound_manifests = summary.get('bound_manifest_sha256')
    source_inventory = summary.get('source_inventory')
    if (not isinstance(bound_manifests, dict) or set(bound_manifests) != {'viewer','telemetry'}
            or any(not isinstance(digest,str) or not re.fullmatch(r'[0-9a-f]{64}',digest)
                   for digest in bound_manifests.values())
            # E1 uses telemetry/viewer clients only; there are no B-side
            # durable-gateway environment files to bind or execute.
            or summary.get('bound_gateway_environment_sha256') != {}
            or not isinstance(summary.get('bound_run_spec_sha256'),str)
            or not re.fullmatch(r'[0-9a-f]{64}',summary['bound_run_spec_sha256'])
            or source_inventory != prepared['bundle_inventory']):
        raise ValueError('result_manifest_provenance_rejected')
    seen = set()
    values = {}
    total = 0
    for row in files:
        if (not isinstance(row, dict) or set(row) != {'path', 'size_bytes', 'sha256'}
                or row['path'] not in expected or row['path'] in seen
                or type(row['size_bytes']) is not int or not 0 <= row['size_bytes'] <= 8 << 20
                or not isinstance(row['sha256'], str)
                or not re.fullmatch(r'[0-9a-f]{64}', row['sha256'])):
            raise ValueError('result_inventory_rejected')
        seen.add(row['path']); total += row['size_bytes']
        if total > 32 << 20:
            raise ValueError('result_total_size_rejected')
        data = _private_result_file(root, row['path'], 8 << 20)
        if len(data) != row['size_bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']:
            raise ValueError('result_digest_mismatch')
        value = json.loads(data)
        role = expected[row['path']]
        if role == 'provenance':
            if (not isinstance(value, dict) or value.get('binary_sha256') != binary_sha256
                    or value.get('binary_os') != sys.platform or value.get('binary_arch') != __import__('platform').machine()):
                raise ValueError('result_binary_provenance_mismatch')
            if summary.get('binary') != value:
                raise ValueError('result_binary_summary_mismatch')
            values['binary_provenance'] = value
        elif role in ('viewer', 'telemetry'):
            wanted_experiment = 'E1' if role == 'viewer' else 'E2'
            if (not isinstance(value, dict) or value.get('status') not in ('COMPLETE','PARTIAL','FAILED')
                    or value.get('experiment') != wanted_experiment
                    or value.get('arm') != prepared['common']['arm']
                    or value.get('run_id') != prepared['run_id']
                    or value.get('mode') != 'remote' or value.get('profile') != 'pilot'):
                raise ValueError('result_workload_correlation_rejected')
            values[role] = value
        elif role == 'runner' and (not isinstance(value, dict) or value.get('status') not in ('COMPLETE','PARTIAL','FAILED')):
            raise ValueError('result_runner_incomplete')
        elif role == 'runner':
            values['runner'] = value
    if seen != set(expected):
        raise ValueError('result_roles_missing')
    found = set()
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in dirs:
            if (Path(directory) / name).is_symlink():
                raise ValueError('result_path_symlink_rejected')
        for name in names:
            path = Path(directory) / name
            if path.is_symlink():
                raise ValueError('result_path_symlink_rejected')
            relative = path.relative_to(root).as_posix()
            if relative != 'results.json':
                found.add(relative)
    if found != set(expected):
        raise ValueError('result_unexpected_files')
    values['_result_status'] = {'runner':summary['runner_status'],
        'viewer':values['viewer']['status'],'telemetry':values['telemetry']['status']}
    return values


def begin_remote_collection(smoke, fixture, endpoint, registry, containers, prepared, *, phase_id):
    pg = prepared['pg']
    before = smoke.paper_p1_count_db(registry, pg)
    sensors = []
    for sensor in prepared['targets_db']:
        counts = json.loads(smoke.db_psql(registry, pg,
            "SELECT json_build_object('ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));",
            {'gw':sensor.gateway_id,'sensor':sensor.sensor_id,'urn':sensor.sensor_urn}).strip())
        sensors.append({'gateway_id':sensor.gateway_id,'sensor_id':sensor.sensor_id,'counts':counts})
    baseline = _admin_metrics(smoke, endpoint, prepared['tokens']['admin'])
    collector = BoundedCollector(scrape=lambda: _admin_metrics(smoke, endpoint, prepared['tokens']['admin']),
        interval=0.5, max_snapshots=64, deadline_seconds=60, baseline_metrics=baseline,
        baseline_server=smoke.paper_p0_stats(registry, containers, 'workload_start'))
    collector.start(None)
    prepared['remote_collector'] = collector
    prepared['remote_baseline'] = {'database':before, 'sensors':sensors, 'metrics':baseline,
        'server_resources':collector.resources[0], 'phase_id':phase_id}
    return {'database_counts':before, 'target_count':len(sensors), 'metrics_baseline_captured':True}


def finish_remote_collection(smoke, fixture, endpoint, registry, containers, prepared, results_root,
                             *, phase_id, paperload_binary):
    collector = prepared.get('remote_collector')
    baseline = prepared.get('remote_baseline')
    if collector is None or not isinstance(baseline, dict) or baseline.get('phase_id') != phase_id:
        raise ValueError('collector_phase_not_started')
    binary_sha256 = hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest()
    imported = _load_result_bundle(results_root, prepared, phase_id=phase_id,
                                   binary_sha256=binary_sha256)
    collector.stop(None)
    terminal_metrics = _drain_websocket_metrics(
        lambda: _admin_metrics(smoke, endpoint, prepared['tokens']['admin']))
    terminal_resources = smoke.paper_p0_stats(registry, containers, 'workload_terminal')
    after = smoke.paper_p1_count_db(registry, prepared['pg'])
    delta = {key:after[key]-baseline['database'][key] for key in baseline['database']}
    after_sensors = []
    for sensor in prepared['targets_db']:
        counts = json.loads(smoke.db_psql(registry, prepared['pg'],
            "SELECT json_build_object('ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));",
            {'gw':sensor.gateway_id,'sensor':sensor.sensor_id,'urn':sensor.sensor_urn}).strip())
        after_sensors.append({'gateway_id':sensor.gateway_id,'sensor_id':sensor.sensor_id,'counts':counts})
    scoped = sensor_scoped_delta(baseline['sensors'], after_sensors)
    per_target = [{'key':key,**value} for key,value in (scoped.get('available') or {}).items()]
    config_before={(x['gateway_id'],x['sensor_id']):x['counts']['configuration'] for x in baseline['sensors']}
    config_after={(x['gateway_id'],x['sensor_id']):x['counts']['configuration'] for x in after_sensors}
    for item in per_target:
        gateway,sensor=item['key'].split('/',1)
        item['configuration']=config_after.get((gateway,sensor),-1)-config_before.get((gateway,sensor),-1)
    raw = [verify_pilot_raw(smoke, registry, prepared['pg'], sensor) for sensor in prepared['targets_db']]
    telemetry, viewer = imported['telemetry'], imported['viewer']
    expected_headers = {row.get('message_id'):row for row in telemetry.get('identity_samples', [])
                        if isinstance(row,dict) and isinstance(row.get('message_id'),str)}
    validation = validate_e1_result(telemetry, prepared['mapping'], database_delta=delta,
        raw_verification=raw, viewer_sessions=viewer,
        sampled_headers=viewer.get('identity_samples', []), expected_headers=expected_headers)
    if any(value != 'COMPLETE' for value in imported['_result_status'].values()):
        validation['valid'] = False
        validation['failures'].append('client_runner_or_role_incomplete:' +
            ','.join(f'{key}={value}' for key,value in sorted(imported['_result_status'].items())))
    ws_window = websocket_metrics_window(baseline['metrics'], terminal_metrics,
        smoke.paper_p1_histogram_window, sum(prepared['mapping']['expected_deliveries_by_target'].values()))
    if ws_window.get('availability') != 'available':
        validation['valid'] = False
        validation['failures'].append('websocket_metrics_window:' + str(ws_window.get('reason')))
    if collector.errors:
        validation['valid'] = False
        validation['failures'].append('collector_incomplete:' + ','.join(collector.errors))
    if any(row.get('ledger') != 5 or row.get('raw') != 500 or row.get('temporal') != 5
           or row.get('configuration') != 0 for row in per_target):
        validation['valid'] = False
        validation['failures'].append('per_target_database_delta')
    resource_samples = collector.resources + [terminal_resources]
    b_resource = imported.get('runner', {}).get('resource_sampling')
    if isinstance(b_resource, dict):
        resource_samples.append({'phase':'workload_B_generator', **b_resource})
    result = {'workload_outcome':'VALIDATED' if validation['valid'] else 'VALIDATION_FAILED',
        'validation':validation,'database_delta':delta,'per_target':per_target,
        'raw_verification':raw,'websocket_metrics_window':ws_window,
        'collector_errors':collector.errors,'collector_metrics':collector.metrics,
        'resource_samples':resource_samples,'client_runner':imported.get('runner'),
        'binary_provenance':imported.get('binary_provenance'),
        'source_bundle_sha256':prepared['bundle_sha256'],'binary_sha256':binary_sha256,
        'physical_ab':'NOT RUN','generator_colocated':True,'full_p8_accepted':False}
    return result


def run(smoke, fixture, endpoint, registry, containers, images, state, paperload_binary):
    prepared = prepare(smoke, fixture, endpoint, registry, containers, images, state)
    pg, quota, effective, resources = (prepared['pg'], prepared['quota'],
        prepared['effective'], prepared['resources'])
    schema = prepared['schema']
    storage_status, tokens, specs, passwords = (prepared['storage_status'], prepared['tokens'],
        prepared['specs'], prepared['passwords'])
    sensor_path, targets, targets_db, mapping = (prepared['sensor_path'], prepared['targets'],
        prepared['targets_db'], prepared['mapping'])
    mqtt_endpoint, ca_path = prepared['mqtt_endpoint'], fixture.private / 'certs/ca.crt'
    run_id, root, viewer_identity = prepared['run_id'], prepared['root'], prepared['viewer_identity']
    telemetry_identity, gateway_refs, mode = (prepared['telemetry_identity'], prepared['gateway_refs'],
        prepared['mode'])
    tls, subscriptions, common, viewer, telemetry = (prepared['tls'], prepared['subscriptions'],
        prepared['common'], prepared['viewer_manifest'], prepared['telemetry_manifest'])
    paths, plan = prepared['manifest_paths'], prepared['plan']
    output=root/'output'; output.mkdir(mode=0o700)
    before=smoke.paper_p1_count_db(registry, pg)
    before_sensors=[]
    for s in targets_db:
        counts=json.loads(smoke.db_psql(registry, pg, "SELECT json_build_object('ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));",
            {'gw':s.gateway_id,'sensor':s.sensor_id,'urn':s.sensor_urn}).strip())
        before_sensors.append({'gateway_id':s.gateway_id,'sensor_id':s.sensor_id,'counts':counts})
    baseline=_admin_metrics(smoke, endpoint, tokens['admin'])
    collector=BoundedCollector(scrape=lambda:_admin_metrics(smoke,endpoint,tokens['admin']), interval=0.5,
        max_snapshots=64, deadline_seconds=60, baseline_metrics=baseline,
        baseline_server=smoke.paper_p0_stats(registry, containers, 'workload_start'))
    log=root/'runner.log'; proc=None; primary=None
    with log.open('xb') as stream:
        log.chmod(0o600)
        try:
            proc=subprocess.Popen(['python3', str(smoke.ROOT/'scripts/paper_p8.py'), '--run', '--binary', str(paperload_binary),
                '--run-spec', plan['run_spec'], '--run-artifact', str(root/'run.json')], cwd=smoke.ROOT,
                stdout=stream, stderr=subprocess.STDOUT, env=smoke.process_env(fixture), close_fds=True)
            collector.start(proc.pid)
            try: proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try: proc.wait(timeout=3)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
                raise ValueError('p8_e1_runner_timeout') from None
        except Exception as exc: primary=exc
        finally:
            collector.stop(proc.pid if proc else None)
            if proc and proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=3)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
    if primary: raise primary
    if proc.returncode != 0:
        raise ValueError('p8_e1_role_workload_incomplete')
    role_workloads = role_artifacts(root)
    workload = role_workloads['telemetry']
    after=smoke.paper_p1_count_db(registry,pg)
    delta={k:after[k]-before[k] for k in before}
    after_sensors=[]
    for s in targets_db:
        counts=json.loads(smoke.db_psql(registry, pg, "SELECT json_build_object('ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));",
            {'gw':s.gateway_id,'sensor':s.sensor_id,'urn':s.sensor_urn}).strip())
        after_sensors.append({'gateway_id':s.gateway_id,'sensor_id':s.sensor_id,'counts':counts})
    scoped=sensor_scoped_delta(before_sensors,after_sensors)
    per_target=[{'key':key,**value} for key,value in (scoped.get('available') or {}).items()]
    before_config={(x['gateway_id'],x['sensor_id']):x['counts']['configuration'] for x in before_sensors}
    after_config={(x['gateway_id'],x['sensor_id']):x['counts']['configuration'] for x in after_sensors}
    for item in per_target:
        gw,sensor=item['key'].split('/',1)
        item['configuration']=after_config.get((gw,sensor),-1)-before_config.get((gw,sensor),-1)
    raw_checks=[verify_pilot_raw(smoke,registry,pg,s) for s in targets_db]
    # Header identities are sampled from producer headers; each sampled websocket
    # record must carry an exact producer identity and satisfy that viewer's filters.
    ws = role_workloads['viewer']
    producer_headers = role_workloads['telemetry'].get('identity_samples', [])
    expected_header_ids = {x.get('message_id'):x for x in producer_headers if isinstance(x,dict)}
    ws_samples = ws.get('identity_samples', [])
    sampled_ids = ws_samples
    validation=validate_e1_result(workload, mapping, database_delta=delta,
        raw_verification=raw_checks, viewer_sessions=ws, sampled_headers=sampled_ids,
        expected_headers=expected_header_ids)
    admin_terminal=_drain_websocket_metrics(
        lambda: _admin_metrics(smoke, endpoint, tokens['admin']))
    server_terminal=smoke.paper_p0_stats(registry,containers,'workload_terminal')
    ws_metrics_window = websocket_metrics_window(
        baseline, admin_terminal, smoke.paper_p1_histogram_window,
        sum(mapping['expected_deliveries_by_target'].values()))
    if ws_metrics_window['availability'] != 'available':
        validation['failures'].append('websocket_metrics_window:' + str(ws_metrics_window['reason']))
        validation['valid'] = False
    collector.metrics.append({'phase':'workload_terminal','snapshot':admin_terminal})
    collector.resources.append(server_terminal)
    if collector.errors:
        validation['failures'].append('collector_incomplete:' + ','.join(collector.errors))
        validation['valid'] = False
    if any(x.get('ledger') != 5 or x.get('raw') != 500 or x.get('temporal') != 5 or x.get('configuration') != 0 for x in per_target):
        validation['failures'].append('per_target_database_delta'); validation['valid']=False
    if not validation['valid']:
        raise ValueError('p8_e1_validation_failed:' + ','.join(validation['failures']))
    safe_flags=safe_effective_flags(effective)
    result={'status':'WORKLOAD_COMPLETE_PENDING_CLEANUP','schema':schema,'quota':quota,'effective_flags':safe_flags,
        'storage_status':storage_status,'mapping':mapping,'execution_plan':plan,'manifest_sha256':[hashlib.sha256(p.read_bytes()).hexdigest() for p in paths],
        'binary_sha256':hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest(),
        'images':{name:{'id':value} for name,value in images.items()},
        'source_revision':smoke.paper_p0_git(fixture,'rev-parse','HEAD'),'database_delta':delta,
        'database_delta_by_sensor':per_target,'raw_verification':raw_checks,'validation':validation,
        'collector_metrics':collector.metrics,'collector_resources':collector.resources,'collector_errors':collector.errors,
        'collector_generator_samples':collector.generator_samples,'terminal_admin_metrics':admin_terminal,
        'terminal_server_resources':server_terminal,
        'websocket_metrics_window':ws_metrics_window,
        'mode':mode,'generator_colocated':True,'cross_process_latency':'unavailable','principals':2,'sockets':64,
        'workload':workload,'generated_at':datetime.now(timezone.utc).isoformat()}
    smoke.paper_p1_metrics_csv(fixture.artifacts/'metrics.csv',collector.metrics)
    smoke.paper_p1_resources_csv(fixture.artifacts/'resources.csv',[r for r in collector.resources if r.get('services') and not r.get('error')],resources)
    smoke.durable_json(fixture.artifacts/'p8-e1-pilot.json',result)
    smoke.durable_json(fixture.artifacts/'run.json',result)
    return result
