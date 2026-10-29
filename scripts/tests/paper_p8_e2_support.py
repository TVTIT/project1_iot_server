"""E2 workload orchestration and accounting helpers for the disposable fixture."""
import hashlib
import importlib.util
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

_support_spec = importlib.util.spec_from_file_location(
    'paper_p8_support', Path(__file__).with_name('paper_p8_support.py'))
_support = importlib.util.module_from_spec(_support_spec)
_support_spec.loader.exec_module(_support)
sensor_scoped_delta = _support.sensor_scoped_delta

BoundedCollector = _support.BoundedCollector
verify_pilot_raw = _support.verify_pilot_raw

PILOT_SAMPLES = (100, 1)
OFFICIAL_RATES = (100, 1000, 5000, 20000)
PILOT_SEED = 84
PILOT_MESSAGES = {'A': 8, 'B': 16}


def e2_pilot_manifests(*, common, targets, gateway_identity_files, telemetry_identity_files):
    """Create paired same-seed A/B synthetic telemetry manifests for sample profiles."""
    if len(targets) != 2 or len({t.get('gateway_id') for t in targets}) != 2:
        raise ValueError('e2_requires_two_gateway_targets')
    if set(gateway_identity_files) != {t['gateway_id'] for t in targets}:
        raise ValueError('e2_gateway_credentials_mismatch')
    profiles = {}
    for samples in PILOT_SAMPLES:
        pair = {}
        for arm in ('A', 'B'):
            measure, burst = (8, 0) if arm == 'A' else (4, 12)
            m = {**common, 'schema_version': 1, 'experiment': 'E2', 'arm': arm,
             'run_id': f"{common.get('run_id', 'e2')}-{samples}",
                 'profile': 'pilot', 'roles': ['telemetry'], 'seed': PILOT_SEED,
                  'samples_per_message': samples, 'targets': [dict(targets[0 if arm == 'A' else 1])],
                 'subscriptions': [],
                  'topology': {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
                 'traffic': {'rate_per_second': 1, 'ramp_seconds': 0, 'burst_messages': burst},
                 'phases': {'warmup_seconds': 0, 'measure_seconds': measure,
                            'drain_seconds': 12, 'cooldown_seconds': 0},
                 'limits': {'retries': 16, 'workers': 2,
                            'connections': 1, 'pending_messages': 64, 'pending_bytes': 1 << 20,
                            'disk_growth_bytes': 1 << 24, 'wall_seconds': 30},
                  'gateway_identity_files': {targets[0 if arm == 'A' else 1]['gateway_id']:
                                             gateway_identity_files[targets[0 if arm == 'A' else 1]['gateway_id']]},
                 'identity_files': {'telemetry': telemetry_identity_files[arm]},
                 'durability_mode': 'synthetic-load', 'retry_interval_ms': 500}
            pair[arm] = m
        profiles[str(samples)] = pair
    return {'profiles': profiles, 'unique_messages_per_profile': 24,
            'shared_seed': PILOT_SEED, 'same_topology_security_quota_worker_policy': True,
            'execution': 'NOT RUN'}


def e2_fixture_profile(*, microbatch_wait_ms):
    """Proposed next-fixture-only flags/caps; does not mutate deployment defaults."""
    if microbatch_wait_ms not in (50, 100):
        raise ValueError('e2_microbatch_wait_rejected')
    return {'effective_fixture_flags': {'MQTT_SHAPER_ENABLED': True,
            'MQTT_MICROBATCH_ENABLED': True, 'MQTT_RAM_DEDUP_ENABLED': False,
            'STAGE5_ENABLED': False, 'STAGE5_WAKEUP_ENABLED': False,
            'WEBSOCKET_ENABLED': False},
            'shaper': {'rate_per_second': 2, 'burst': 4},
            'microbatch': {'max_messages': 32, 'max_rows': 500,
                           'max_bytes': 4 << 20, 'wait_ms': microbatch_wait_ms},
            'scope': 'proposed disposable E2 fixture only; deployment defaults unchanged'}


def build_e2_execution_plan(*, output_dir, manifest_a, manifest_b, seed=PILOT_SEED,
                            official_opt_in=False, profile='pilot'):
    """Validate private provisioned role manifests and emit a bounded parallel run-spec."""
    if profile == 'official' and not official_opt_in:
        raise ValueError('official_profile_requires_opt_in')
    if profile not in ('pilot', 'official'):
        raise ValueError('e2_profile_rejected')
    root = Path(output_dir)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    loaded = {}
    for arm, path in (('A', manifest_a), ('B', manifest_b)):
        source = Path(path)
        if source.is_symlink() or not source.is_file() or source.stat().st_mode & 0o077 or source.stat().st_size > 1 << 20:
            raise ValueError('e2_manifest_input_rejected')
        try:
            m = json.loads(source.read_text(encoding='utf-8'))
        except (UnicodeError, json.JSONDecodeError):
            raise ValueError('e2_manifest_invalid') from None
        targets = m.get('targets')
        credentials = m.get('gateway_identity_files')
        if (m.get('experiment') != 'E2' or m.get('arm') != arm or m.get('profile') != profile
                or m.get('roles') != ['telemetry'] or m.get('seed') != seed
                or not isinstance(targets, list) or len(targets) != 1
                or not isinstance(targets[0], dict) or not targets[0].get('gateway_id')
                or not targets[0].get('sensor_id')
                or not isinstance(credentials, dict) or len(credentials) != 1
                or set(credentials) != {targets[0]['gateway_id']}
                or not next(iter(credentials.values()), None)
                or not isinstance(m.get('identity_files'), dict)
                or not m['identity_files'].get('telemetry')):
            raise ValueError('e2_role_manifest_mismatch')
        if (not isinstance(m.get('tls'), dict) or not all(m['tls'].get(k) for k in
                ('https_endpoint', 'https_ca_file', 'mqtt_tls_endpoint', 'mqtt_tls_ca_file'))):
            raise ValueError('e2_security_values_missing')
        if profile == 'pilot' and (m.get('samples_per_message') not in PILOT_SAMPLES
                or m.get('topology') != {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0}
                or m.get('phases', {}).get('measure_seconds') != (8 if arm == 'A' else 4)
                or m.get('traffic', {}).get('burst_messages') != (0 if arm == 'A' else 12)
                or m.get('limits', {}).get('retries') != 16
                or m.get('retry_interval_ms') != 500
                or m.get('limits', {}).get('workers') != 2
                or m.get('limits', {}).get('connections') != 1):
            raise ValueError('e2_pilot_bounds_rejected')
        loaded[arm] = m
    a, b = loaded['A'], loaded['B']
    for key in ('seed', 'samples_per_message', 'tls', 'limits', 'topology', 'durability_mode'):
        if a.get(key) != b.get(key):
            raise ValueError('e2_shared_profile_mismatch:' + key)
    if a['targets'][0]['gateway_id'] == b['targets'][0]['gateway_id']:
        raise ValueError('e2_gateway_targets_not_distinct')
    if a.get('limits', {}).get('connections') != 1:
        raise ValueError('e2_connection_bound_rejected')
    if a.get('traffic', {}).get('rate_per_second') != b.get('traffic', {}).get('rate_per_second'):
        raise ValueError('e2_rate_mismatch')
    outputs = []
    for arm, source in (('A', manifest_a), ('B', manifest_b)):
        dst = root / f'{arm}.manifest.json'
        with dst.open('x', encoding='utf-8') as out:
            json.dump(loaded[arm], out, sort_keys=True, separators=(',', ':'))
        dst.chmod(0o600)
        outdir = root / f'{arm}-output'
        outputs.append({'manifest': str(dst), 'output_dir': str(outdir), 'role': 'telemetry',
                        'ready_timeout_seconds': 90})
    spec = {'paperload': outputs, 'stop_timeout_seconds': 30}
    run_spec = root / 'e2-run-spec.json'
    with run_spec.open('x', encoding='utf-8') as out:
        json.dump(spec, out, sort_keys=True, indent=2)
    run_spec.chmod(0o600)
    return {'run_spec': str(run_spec), 'roles': ['telemetry:A', 'telemetry:B'],
            'seed': seed, 'execution': 'NOT RUN', 'official_opt_in': bool(official_opt_in)}


def e2_official_estimate(*, rate_per_second, duration_seconds, samples_per_message,
                         payload_bytes, official_opt_in=False, max_duration_seconds=7200, max_messages=1000000,
                         max_raw_rows=100000000, max_disk_bytes=10 << 30,
                         raw_row_bytes=256, index_bytes_per_row=64, wal_multiplier=2,
                         pending_memory_bytes=256 << 20, retry_memory_bytes=256 << 20,
                         application_bandwidth_bytes_per_second=100 << 20):
    """Conservative sizing estimate, not a measurement or wire-byte estimate."""
    import math
    if not official_opt_in:
        raise ValueError('official_profile_requires_opt_in')
    if (rate_per_second not in OFFICIAL_RATES or type(duration_seconds) is not int or duration_seconds < 1
            or type(samples_per_message) is not int or samples_per_message not in PILOT_SAMPLES
            or type(payload_bytes) is not int or payload_bytes < 1):
        raise ValueError('e2_estimate_input_rejected')
    messages = math.ceil(rate_per_second * duration_seconds)
    raw_rows = messages * samples_per_message
    payload_estimate = messages * payload_bytes
    database_estimate = raw_rows * (raw_row_bytes + index_bytes_per_row) * wal_multiplier
    total_storage_estimate = payload_estimate + database_estimate
    pending_estimate = min(messages, 64) * payload_bytes
    retry_estimate = min(messages, 16) * payload_bytes
    bandwidth_estimate = rate_per_second * payload_bytes
    if (duration_seconds > max_duration_seconds or messages > max_messages or raw_rows > max_raw_rows
            or total_storage_estimate > max_disk_bytes or pending_estimate > pending_memory_bytes
            or retry_estimate > retry_memory_bytes
            or bandwidth_estimate > application_bandwidth_bytes_per_second):
        raise ValueError('e2_predicted_bound_exceeded')
    return {'estimate_only': True, 'rate_per_second': rate_per_second,
            'duration_seconds': duration_seconds, 'unique_messages_estimate': messages,
            'raw_rows_estimate': raw_rows, 'application_payload_bytes_estimate': payload_estimate,
            'raw_database_row_index_wal_bytes_estimate': database_estimate,
            'total_storage_growth_bytes_estimate': total_storage_estimate,
            'pending_retry_memory_bytes_estimate': pending_estimate + retry_estimate,
            'application_bandwidth_bytes_per_second_estimate': bandwidth_estimate,
            'payload_bytes_wire': None, 'payload_size_basis': 'actual_builder_or_conservative_assumption',
            'estimate_basis': {'raw_row_bytes': raw_row_bytes, 'index_bytes_per_row': index_bytes_per_row,
                               'wal_multiplier': wal_multiplier}, 'not_measurements': True}


def _phase_intervals(events, arm):
    def belongs(row):
        # Go-local phase events use a different clock and cannot compare children.
        return row.get('child') == f'paperload-telemetry:{arm}.manifest.json'
    rows = [e for e in events if isinstance(e, dict) and belongs(e)]
    times = {}
    for row in rows:
        name, stamp = row.get('event'), row.get('observed_at_runner_monotonic_ns')
        if name in times or type(stamp) is not int or stamp < 0:
            raise ValueError('e2_phase_events_invalid')
        times[name] = stamp
    if any(times[k] > times[v] for k, v in (('measure_begin', 'measure_end'),
            ('burst_begin', 'burst_end')) if k in times and v in times):
        raise ValueError('e2_phase_interval_reversed')
    return times


def validate_e2_result(*, workload_a, workload_b, ingress_delta, database_delta,
                       database_by_gateway, phase_events, raw_verification=None,
                       execution_context, resources=None, histogram_windows=None):
    """Reconcile actual child results, phase overlap and scoped DB; unknown stays unavailable."""
    failures = []
    if not isinstance(workload_a, dict) or not isinstance(workload_b, dict):
        return {'valid': False, 'failures': ['workload_missing'], 'availability': 'inconclusive'}
    ca, cb = workload_a.get('telemetry', {}), workload_b.get('telemetry', {})
    if workload_a.get('status') != 'COMPLETE' or workload_b.get('status') != 'COMPLETE':
        failures.append('workload_status')
    samples = execution_context.get('samples_per_message') if isinstance(execution_context, dict) else None
    if samples not in PILOT_SAMPLES:
        failures.append('samples_per_message_missing_or_invalid')
    for arm, counters, expected in (('A', ca, 8), ('B', cb, 16)):
        for field, value in (('generated_unique', expected), ('offered_unique', expected),
                             ('application_acks', expected), ('pending_messages', 0), ('pending_bytes', 0)):
            if counters.get(field) != value: failures.append(f'{arm}_{field}')
        attempts, retries, invalid_acks = (counters.get('publish_attempts'),
                counters.get('retries'), counters.get('invalid_acks'))
        if (type(attempts) is not int or type(retries) is not int or retries < 0
                or retries > attempts or type(invalid_acks) is not int or invalid_acks != 0):
            failures.append(f'{arm}_retry_or_invalid_ack_metrics')
    for arm, counters, minimum in (('A', ca, 8), ('B', cb, 16)):
        for field in ('publish_attempts', 'broker_pubacks'):
            value = counters.get(field)
            if type(value) is not int or value < minimum: failures.append(f'{arm}_{field}')
    if not isinstance(phase_events, list):
        phase = {'available': None, 'reason': 'phase_events_missing'}
    else:
        try:
            a, b = _phase_intervals(phase_events, 'A'), _phase_intervals(phase_events, 'B')
            required = ('measure_begin', 'measure_end')
            if any(k not in a for k in required) or any(k not in b for k in ('burst_begin', 'burst_end')):
                raise ValueError('phase_event_missing')
            overlap = max(0, min(a['measure_end'], b['burst_end']) - max(a['measure_begin'], b['burst_begin']))
            phase = {'available': True, 'a_measure_runner_interval_ns': [a['measure_begin'], a['measure_end']],
                     'b_burst_runner_interval_ns': [b['burst_begin'], b['burst_end']],
                     'observed_overlap_ns': overlap, 'observer_lag_qualification': 'runner receipt time; pipe lag unknown'}
            if overlap <= 0: failures.append('a_measure_does_not_overlap_b_burst')
        except ValueError as exc:
            phase = {'available': None, 'reason': str(exc)}
            failures.append('phase_overlap_inconclusive')
    expected_total = {'ledger': 24, 'raw': 24 * samples if samples in PILOT_SAMPLES else None,
                      'temporal': 24, 'configuration': 0}
    if database_delta != expected_total: failures.append('database_delta_mismatch')
    mapping = execution_context.get('role_to_target') if isinstance(execution_context, dict) else None
    mapped_a_gateway = mapping.get('A', {}).get('gateway_id') if isinstance(mapping, dict) and isinstance(mapping.get('A'), dict) else None
    if (not isinstance(mapping, dict) or set(mapping) != {'A', 'B'} or
            any(not isinstance(mapping.get(arm), dict) or not mapping[arm].get('gateway_id') or
                not mapping[arm].get('sensor_id') for arm in ('A', 'B')) or
            (isinstance(mapping, dict) and mapping.get('A') == mapping.get('B'))):
        failures.append('execution_target_mapping_invalid')
    if not isinstance(database_by_gateway, dict) or len(database_by_gateway) != 2:
        failures.append('database_gateway_scope_missing')
    elif (isinstance(mapping, dict) and set(mapping) == {'A', 'B'} and
            all(isinstance(mapping.get(arm), dict) and mapping[arm].get('gateway_id') for arm in ('A', 'B')) and
            set(database_by_gateway) == {mapping['A']['gateway_id'], mapping['B']['gateway_id']}):
        expected_scopes = {mapping['A']['gateway_id']: 8, mapping['B']['gateway_id']: 16}
        if samples not in PILOT_SAMPLES or any(database_by_gateway[g] != {'ledger': n, 'raw': n * samples,
                'temporal': n, 'configuration': 0} for g, n in expected_scopes.items()):
            failures.append('database_gateway_delta_mismatch')
    else:
        failures.append('database_gateway_scope_mismatch')
    if (not isinstance(raw_verification, list) or len(raw_verification) != 2 or
            any(not isinstance(row, dict) or not row.get('verified_exact') or
                row.get('ledger_count') not in (8, 16) or
                (row.get('ledger_count') != (8 if row.get('gateway_id') == mapped_a_gateway else 16)) or
                not isinstance(mapping, dict) or row.get('gateway_id') not in
                (mapping.get('A', {}).get('gateway_id'), mapping.get('B', {}).get('gateway_id')) or
                (row.get('gateway_id'), row.get('sensor_id')) not in
                    ((mapping.get('A', {}).get('gateway_id'), mapping.get('A', {}).get('sensor_id')),
                     (mapping.get('B', {}).get('gateway_id'), mapping.get('B', {}).get('sensor_id')))
                for row in raw_verification) or
            (isinstance(raw_verification, list) and len({r.get('gateway_id') for r in raw_verification
             if isinstance(r, dict)}) != 2) or
            (isinstance(raw_verification, list) and sorted(r.get('ledger_count') for r in raw_verification
             if isinstance(r, dict)) != [8, 16])):
        failures.append('pilot_raw_retention_verification')
    ingress = ingress_delta.get('delta') if isinstance(ingress_delta, dict) and ingress_delta.get('available') else None
    if ingress is None:
        received = admitted = rejected = drops = {'available': None, 'reason': 'ingress_delta_unavailable'}
    else:
        for field, minimum in (('Committed', 24), ('SamplesNew', expected_total['raw'])):
            if minimum is None or type(ingress.get(field)) is not int or ingress[field] != minimum:
                failures.append('ingress_' + field)
        for field, minimum in (('ACKPublished', 24), ('Received', 24), ('Accepted', 24)):
            if type(ingress.get(field)) is not int or ingress[field] < minimum:
                failures.append('ingress_' + field)
        for field in ('RejectedNotRunning','RejectedStopping','RejectedRetained','RejectedOversized',
                      'RejectedInvalidTopic','QueueFull','ByteBudgetFull','ShaperRateLimited'):
            if type(ingress.get(field)) is not int or ingress[field] < 0:
                failures.append('ingress_metric_' + field)
        if type(ingress.get('ShaperRateLimited')) is not int or ingress.get('ShaperRateLimited', 0) <= 0:
            failures.append('ingress_burst_not_rate_limited')
        received = {'available': ingress.get('Received')}
        admitted = {'available': ingress.get('Accepted')}
        rejected = {'available': {k: ingress.get(k) for k in ('RejectedNotRunning','RejectedStopping','RejectedRetained','RejectedOversized','RejectedInvalidTopic','QueueFull','ByteBudgetFull','ShaperRateLimited')}}
        drops = {'available': ingress.get('ShaperRateLimited')}
    if ingress is None: failures.append('ingress_delta_unavailable')
    return {'valid': not failures, 'failures': failures,
            'availability': 'available' if phase.get('available') and ingress is not None else 'inconclusive',
            'phase_overlap': phase, 'received': received, 'admitted': admitted,
            'rejected': rejected, 'actual_shaper_drops': drops,
            'database_expected': expected_total, 'queue_histogram_windows': histogram_windows,
            'resource_roles': resources, 'fairness': {'status': 'not_proven',
                'reason': 'runner phase events provide timestamps, not cross-child counter snapshots'},
             'durability': 'synthetic-load; not durable gateway outbox'}


def _safe_flags(effective):
    allowed = ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
        'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
        'MQTT_SHAPER_RATE_PER_SECOND', 'MQTT_SHAPER_BURST', 'MQTT_MICROBATCH_MAX_MESSAGES',
        'MQTT_MICROBATCH_MAX_ROWS', 'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT')
    env = effective.get('services', {}).get('backend', {}).get('environment', {})
    return {key: env[key] for key in allowed if key in env}


def _validate_fixture_flags(effective):
    flags = _safe_flags(effective)
    expected = {'WEBSOCKET_ENABLED': 'false', 'STAGE5_ENABLED': 'false',
        'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'true',
        'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'false',
        'MQTT_SHAPER_RATE_PER_SECOND': '2', 'MQTT_SHAPER_BURST': '4',
        'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
    normalized = {key: str(value).lower() for key, value in flags.items()}
    if any(normalized.get(key) != value for key, value in expected.items()):
        raise ValueError('p8_e2_effective_fixture_flags_mismatch')
    return flags


def _target_counts(smoke, registry, pg, spec):
    sql = """SELECT json_build_object(
 'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),
 'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));"""
    value = smoke.db_psql(registry, pg, sql,
        {'gw': spec.gateway_id, 'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip()
    return json.loads(value)


def _stop(proc, timeout=5):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def run(smoke, fixture, endpoint, registry, containers, images, state, paperload_binary):
    """Run A8/B16 once for each sample profile and persist a safe pending-cleanup result."""
    pg, quota, effective, resources, storage_status, tokens, specs, passwords, sensor_path = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, state))
    schema = smoke.db_psql(registry, pg,
        "SELECT version::text || ':' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;").strip()
    if schema != '24:telemetry_ledger_timestamp':
        raise ValueError('p8_schema_terminal_rejected')
    broker = registry.resolve(containers['mosquitto'])
    ports = broker.get('NetworkSettings', {}).get('Ports', {}).get('8883/tcp') or []
    if len(ports) != 1:
        raise ValueError('p8_mqtt_mapping_readback_rejected')
    published = int(ports[0].get('HostPort', '0'))
    _support.validate_mqtt_readback({'service': 'mosquitto', 'host_ip': '127.0.0.1',
        'target': 8883, 'published': published}, owned_service='mosquitto')
    mqtt_endpoint = f'mqtts://127.0.0.1:{published}'
    root = fixture.private / 'p8-e2'
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    ca_path = fixture.private / 'certs/ca.crt'
    targets = [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id} for s in specs]
    gateway_files = {}
    telemetry_files = {}
    for spec, password, arm in zip(specs, passwords, ('A', 'B')):
        identity = root / f'gateway-{spec.gateway_id}.json'
        smoke.private_write(identity, json.dumps({'username': spec.gateway_id, 'password': password}, sort_keys=True))
        gateway_files[spec.gateway_id] = str(identity)
        telemetry = root / f'telemetry-{arm}-identities.json'
        smoke.private_write(telemetry, '{}')
        telemetry_files[arm] = str(telemetry)
    common = {'mode': 'local', 'run_id': fixture.project + '-e2', 'seed': PILOT_SEED,
        'bundle_path': str(root), 'publish_timeout_ms': 3000,
        'tls': {'https_endpoint': endpoint.url, 'https_ca_file': str(endpoint.ca_file),
            'mqtt_tls_endpoint': mqtt_endpoint, 'mqtt_tls_ca_file': str(ca_path),
            'mqtt_tls_server_name': 'localhost'}}
    manifests = e2_pilot_manifests(common=common, targets=targets,
        gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files)
    if manifests['profiles']['100']['A']['run_id'] == manifests['profiles']['1']['A']['run_id']:
        raise ValueError('e2_profile_run_ids_not_distinct')
    profiles = []
    for sample_key in ('100', '1'):
        pair = manifests['profiles'][sample_key]
        paths = {}
        for arm in ('A', 'B'):
            path = root / f'{arm}-{sample_key}.manifest.json'
            smoke.private_write(path, json.dumps(pair[arm], sort_keys=True, separators=(',', ':')))
            paths[arm] = path
        plan_dir = root / f'plan-{sample_key}'
        plan = build_e2_execution_plan(output_dir=plan_dir, manifest_a=paths['A'], manifest_b=paths['B'])
        before_global = smoke.paper_p1_count_db(registry, pg)
        before_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
        baseline = _support._admin_metrics(smoke, endpoint, tokens['admin'])
        collector = BoundedCollector(scrape=lambda: _support._admin_metrics(smoke, endpoint, tokens['admin']),
            interval=0.5, max_snapshots=64, deadline_seconds=60, baseline_metrics=baseline,
            baseline_server=smoke.paper_p0_stats(registry, containers, 'workload_start'))
        log = plan_dir / 'runner.log'
        proc = None
        failure = None
        with log.open('xb') as stream:
            log.chmod(0o600)
            try:
                proc = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run',
                    '--binary', str(paperload_binary), '--run-spec', plan['run_spec'],
                    '--run-artifact', str(plan_dir / 'run.json')], cwd=smoke.ROOT,
                    stdout=stream, stderr=subprocess.STDOUT, env=smoke.process_env(fixture), close_fds=True)
                collector.start(proc.pid)
                try:
                    proc.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    raise ValueError('p8_e2_runner_timeout') from None
            except Exception as exc:
                failure = exc
            finally:
                try:
                    collector.stop(proc.pid if proc else None)
                    _stop(proc)
                except Exception as cleanup_exc:
                    if failure is None:
                        failure = cleanup_exc
        if failure:
            smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json',
                {'status': 'FAIL', 'failure': 'runner_exception', 'profiles': profiles,
                 'full_p8_accepted': False})
            raise failure
        if proc is None or proc.returncode != 0:
            smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json',
                {'status': 'FAIL', 'failure': 'runner_nonzero_or_missing', 'profiles': profiles,
                 'full_p8_accepted': False})
            raise ValueError('p8_e2_runner_failed')
        after_admin = _support._admin_metrics(smoke, endpoint, tokens['admin'])
        terminal_server = smoke.paper_p0_stats(registry, containers, 'workload_terminal')
        collector.metrics.append({'phase': 'workload_terminal', 'snapshot': after_admin})
        collector.resources.append(terminal_server)
        after_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
        after_global = smoke.paper_p1_count_db(registry, pg)
        scoped = sensor_scoped_delta(
            [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id, 'counts': c} for s, c in zip(specs, before_targets)],
            [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id, 'counts': c} for s, c in zip(specs, after_targets)])
        # Validate retention only against this profile's producer IDs, excluding prior profile rows.
        workloads = {}
        raw_checks = []
        mapping = {}
        for index, arm in enumerate(('A', 'B')):
            output = plan_dir / f'{arm}-output' / 'workload.json'
            try:
                workloads[arm] = json.loads(output.read_text(encoding='utf-8'))
            except (OSError, UnicodeError, json.JSONDecodeError):
                workloads[arm] = {}
            producer_ids = [row.get('message_id') for row in workloads[arm].get('identity_samples', [])
                            if isinstance(row, dict) and isinstance(row.get('message_id'), str)]
            spec = specs[index]
            mapping[arm] = {'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id}
            raw_checks.append(verify_pilot_raw(smoke, registry, pg, spec, message_ids=producer_ids))
        by_gateway = {}
        for arm, index in (('A', 0), ('B', 1)):
            item = (scoped.get('available') or {}).get(f'{specs[index].gateway_id}/{specs[index].sensor_id}', {})
            item = dict(item)
            item['configuration'] = after_targets[index]['configuration'] - before_targets[index]['configuration']
            by_gateway[mapping[arm]['gateway_id']] = item
        delta = {key: after_global[key] - before_global[key] for key in before_global}
        events = json.loads((plan_dir / 'run.json').read_text(encoding='utf-8')).get('phase_observations', {}).get('events', [])
        ingress = _support.calculate_ingress_delta(baseline, after_admin)
        # Keep the existing P1 same-process histogram-window calculation; the
        # admin endpoint nests ingress histograms under ingress.Durations.
        duration_names = ('queue_wait', 'validation', 'batch_wait',
                          'group_persistence', 'persistence', 'ack_publish')
        def ingress_duration_snapshot(snapshot):
            ing = snapshot.get('ingress') if isinstance(snapshot, dict) else None
            durations = ing.get('Durations') if isinstance(ing, dict) else None
            availability = (ing.get('availability') if isinstance(ing, dict) else None)
            return {'process_instance': snapshot.get('process_instance') if isinstance(snapshot, dict) else None,
                    'availability': {'ingress': availability},
                    'ingress': durations if isinstance(durations, dict) else {}}
        histogram_before = ingress_duration_snapshot(baseline)
        histogram_after = ingress_duration_snapshot(after_admin)
        histogram_windows = {}
        for duration_name in duration_names:
            window = smoke.paper_p1_histogram_window(
                histogram_before, histogram_after, 'ingress', duration_name)
            if window.get('availability') != 'available':
                window['reason'] = 'same-instance ingress duration histogram unavailable or invalid'
            histogram_windows[duration_name] = window
        base_ingress, end_ingress = baseline.get('ingress'), after_admin.get('ingress')
        drain = {}
        for field in ('InFlight', 'BufferedPayloadBytes'):
            old = base_ingress.get(field) if isinstance(base_ingress, dict) else None
            new = end_ingress.get(field) if isinstance(end_ingress, dict) else None
            drain[field] = {'baseline': old if type(old) is int else None,
                            'terminal': new if type(new) is int else None}
            if type(old) is not int or type(new) is not int or old != new:
                raise ValueError('p8_e2_ingress_drain_unverified:' + field)
        if collector.errors:
            raise ValueError('p8_e2_collector_incomplete:' + ','.join(collector.errors))
        validation = validate_e2_result(workload_a=workloads['A'], workload_b=workloads['B'],
            ingress_delta=ingress, database_delta=delta, database_by_gateway=by_gateway,
            phase_events=events, raw_verification=raw_checks,
            execution_context={'samples_per_message': int(sample_key), 'role_to_target': mapping},
            resources={'generator_samples': collector.generator_samples, 'server': collector.resources})
        if collector.errors:
            validation['failures'].append('collector_incomplete:' + ','.join(collector.errors)); validation['valid'] = False
        profile_result = {'samples_per_message': int(sample_key), 'workloads': workloads,
            'database_delta': delta, 'database_by_gateway': by_gateway, 'raw_verification': raw_checks,
            'validation': validation, 'phase_events': events, 'collector_metrics': collector.metrics,
            'stage_histogram_windows': histogram_windows,
            'collector_resources': collector.resources, 'collector_generator_samples': collector.generator_samples,
            'terminal_admin_metrics': after_admin, 'terminal_server_resources': terminal_server,
            'global_delta_cross_check': delta, 'status': 'PASS' if validation['valid'] else 'FAIL'}
        smoke.durable_json(root / f'profile-{sample_key}.json', profile_result)
        profiles.append(profile_result)
        if not validation['valid']:
            result = {'status': 'FAIL', 'profiles': profiles, 'full_p8_accepted': False}
            smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json', result)
            raise ValueError('p8_e2_validation_failed:' + ','.join(validation['failures']))
        # P1 CSVs preserve existing counters and qualify unavailable nested histograms.
        p1_samples = [{'phase': 'workload_start', 'snapshot': baseline}]
        for item in collector.metrics:
            p1_samples.append({'phase': item['phase'], 'snapshot': item.get('snapshot'),
                               **({'error': item['error']} if item.get('error') else {})})
        p1_samples.append({'phase': 'workload_terminal', 'snapshot': after_admin})
        metrics_path = fixture.artifacts / f'metrics-{sample_key}.csv'
        smoke.paper_p1_metrics_csv(metrics_path, p1_samples)
        resources_path = fixture.artifacts / f'resources-{sample_key}.csv'
        smoke.paper_p1_resources_csv(resources_path, collector.resources + [terminal_server], {})
    result = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'profiles': profiles,
        'schema': schema, 'quota': quota, 'effective_flags': _validate_fixture_flags(effective),
        'fixture_profile': e2_fixture_profile(microbatch_wait_ms=50), 'storage_status': storage_status,
        'source_revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'source_fingerprint': _support._source_fingerprint(smoke.ROOT),
        'binary_sha256': hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest(),
        'manifest_sha256': [hashlib.sha256((root / f'{arm}-{s}.manifest.json').read_bytes()).hexdigest()
                            for s in ('100', '1') for arm in ('A', 'B')],
        'images': {name: {'id': value} for name, value in images.items()},
        'cross_clock_latency': {'available': None, 'reason': 'independent clocks'},
        'fairness': {'status': 'not_proven'}, 'full_p8_accepted': False,
        'generated_at': datetime.now(timezone.utc).isoformat()}
    smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json', result)
    smoke.durable_json(fixture.artifacts / 'run.json', result)
    return result
