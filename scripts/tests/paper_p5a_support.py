"""Finite P5a MQTT/Gateway workload on native_compose_smoke's owned fixture."""

import hashlib
import json
import os
import re
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

PROFILE_COMMON = {
    'MQTT_INGRESS_ENABLED': 'true',
    'MQTT_QUEUE_CAPACITY': '256',
    'MQTT_MAX_PAYLOAD_BYTES': '65536',
    'MQTT_WORKER_COUNT': '4',
    'DATABASE_MAX_CONNS': '10',
    'STAGE5_ENABLED': 'false',
    'METRICS_ENABLED': 'true',
    'MQTT_SHAPER_ENABLED': 'false',
    'MQTT_SHAPER_RATE_PER_SECOND': '2',
    'MQTT_SHAPER_BURST': '4',
    'MQTT_SHAPER_MAX_GATEWAYS': '4096',
    'MQTT_SHAPER_IDLE_RETENTION': '10m',
    'MQTT_INGRESS_BUFFERED_BYTES': '16777216',
    'MQTT_MICROBATCH_MAX_MESSAGES': '32',
    'MQTT_MICROBATCH_MAX_ROWS': '500',
    'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304',
    'MQTT_MICROBATCH_MAX_WAIT': '1s',
    'SUPABASE_JWT_MODE': 'rs256',
    'SUPABASE_JWKS_URL': 'http://supabase-envoy:8000/auth/v1/.well-known/jwks.json',
    'AUTH_UNIX_SOCKET_PATH': '/run/iot-auth/auth.sock',
}

BACKEND_QUOTA = {'cpus': 2, 'memory_bytes': 4294967296}
REQUIRED_CHECKS = {
    'workload_accounting', 'ledger_raw_provenance', 'temporal_per_message',
    'current_state_and_versions', 'application_acks_and_empty_outboxes',
    'duplicate_replay_unchanged', 'wrong_namespace_denied', 'https_reads',
    'rs256_https_mqtt_tls_acl', 'payload_budget_drained', 'group_metrics_observed',
    'transaction_copy_metrics_observed', 'exporters_written',
}
DATASET_PROFILES = {
    '100': {'name': '100-samples-per-message', 'samples_per_message': 100},
    '1': {'name': '1-sample-per-message', 'samples_per_message': 1},
}

# Keep these query bodies shared with the real workload and the PostgreSQL
# regression. The psql variables are bound by the existing db_psql helper.
PER_GATEWAY_SUMMARY_SQL = """SELECT coalesce(json_agg(json_build_object(
  'gateway_id', gateway_id, 'messages', messages, 'samples', samples) ORDER BY gateway_id),'[]'::json)
  FROM (
    SELECT gateway_id, count(*) AS messages, sum(sample_count) AS samples
    FROM processed_messages WHERE gateway_id IN (:'a',:'b') GROUP BY gateway_id
  ) AS grouped;"""

PER_TEMPORAL_SUMMARY_SQL = """SELECT coalesce(json_agg(json_build_object(
  'entity_id', entity_id, 'observations', observations) ORDER BY entity_id),'[]'::json)
  FROM (
    SELECT e.entity_id, count(*) AS observations
    FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
    WHERE e.entity_id IN (:'ua',:'ub') GROUP BY e.entity_id
  ) AS grouped;"""


def profile(mode, dataset, *, shutdown_smoke=False):
    """Return an effective backend profile; off/on differ only in the feature flag."""
    if mode not in ('off', 'on'):
        raise ValueError('paper_p5a_mode_rejected')
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p5a_dataset_rejected')
    if shutdown_smoke and (mode != 'on' or dataset != '1'):
        raise ValueError('paper_p5a_shutdown_profile_rejected')
    values = {**PROFILE_COMMON, 'MQTT_MICROBATCH_ENABLED': 'true' if mode == 'on' else 'false'}
    if shutdown_smoke:
        # Isolated functional smoke only: leave time to observe the partial group
        # before SIGTERM while retaining the existing 10s shared shutdown budget.
        values['MQTT_MICROBATCH_MAX_WAIT'] = '2s'
    return values


def validate_effective_profile(environment, mode, dataset, *, shutdown_smoke=False):
    expected = profile(mode, dataset, shutdown_smoke=shutdown_smoke)
    if any(str(environment.get(key)).lower() != value for key, value in expected.items()):
        raise ValueError('paper_p5a_profile_rejected')
    return True


def run_metadata(mode, dataset, *, backend_quota=BACKEND_QUOTA, effective_environment=None,
                 shutdown_smoke=False):
    """Safe run.json fields for a future workload; does not execute or claim a run."""
    effective = profile(mode, dataset, shutdown_smoke=shutdown_smoke)
    if backend_quota != BACKEND_QUOTA:
        raise ValueError('paper_p5a_quota_rejected')
    source = 'fixture_profile'
    if effective_environment is not None:
        validate_effective_profile(effective_environment, mode, dataset, shutdown_smoke=shutdown_smoke)
        source = 'validated_compose_environment'
    metadata = {
        'mode': mode,
        'dataset': dict(DATASET_PROFILES[dataset]),
        'effective_profile': effective,
        'effective_profile_source': source,
        'backend_quota': dict(BACKEND_QUOTA),
        'shaper': {key: effective[key] for key in PROFILE_COMMON if key.startswith('MQTT_SHAPER_')
                   or key == 'MQTT_INGRESS_BUFFERED_BYTES'},
        'auth_tls_acl': {'auth_mode': 'rs256', 'https': True, 'mqtt_tls': True, 'acl_enforced': True},
        'offered_pattern': {'status': 'configured_not_run', 'comparison_rule': 'identical_between_off_and_on'},
        'ram_cache': {'enabled': False, 'status': 'not_implemented'},
    }
    if shutdown_smoke:
        metadata['shutdown_smoke'] = True
    return metadata


def workload_config(dataset):
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p5a_dataset_rejected')
    count = 12
    samples = DATASET_PROFILES[dataset]['samples_per_message']
    config = {
        'gateway_count': 2,
        'messages_per_gateway': count,
        'samples_per_message': samples,
        'messages_per_group_target': 4,
        'steady_sample_rate_hz': 100,
        'run_deadline_seconds': 150,
        'drain_deadline_seconds': 90,
        'comparison': 'same gateway order, message count, sample rate, values, limits and retries in off/on',
        'generation': 'finite Gateway simulator --batches; measured separately from drain/commit',
        'retry_delivery': 'real Gateway SQLite outbox and application commit ACK',
    }
    config['offered_unique_messages'] = config['gateway_count'] * count
    config['offered_unique_samples'] = config['offered_unique_messages'] * samples
    return config


def observer_specs(config):
    """Unique observer identities with a bounded lifetime, selecting telemetry explicitly."""
    return [
        {'tag': 'p5a-telemetry-a', 'kind': 'telemetry',
         'count': config['offered_unique_messages'], 'timeout': 90},
        {'tag': 'p5a-telemetry-b', 'kind': 'telemetry',
         'count': config['offered_unique_messages'], 'timeout': 90},
    ]


def gateway_measurement_args(config):
    return ['--measure', '--measure-capacity', str(config['messages_per_gateway'])]


TEMPORAL_RAW_WINDOW = timedelta(seconds=9)


def temporal_read_windows(first, last):
    """Partition exact telemetry bounds into raw windows below the fixture's 10s cap."""
    try:
        start = datetime.fromisoformat(first.replace('Z', '+00:00'))
        final = datetime.fromisoformat(last.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError):
        raise ValueError('paper_p5a_read_failed') from None
    if start.utcoffset() is None or final.utcoffset() is None or final < start:
        raise ValueError('paper_p5a_read_failed')
    # Temporal ranges are [from, to); one microsecond includes the last stored
    # timestamp without widening beyond the bounded raw-query window.
    end_exclusive = final + timedelta(microseconds=1)
    windows = []
    cursor = start
    while cursor < end_exclusive:
        end = min(cursor + TEMPORAL_RAW_WINDOW, end_exclusive)
        windows.append((cursor, end))
        cursor = end
    return windows


def _utc_z(value):
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def temporal_read_path(smoke, sensor_urn, first, last, max_points):
    if not isinstance(sensor_urn, str) or not re.fullmatch(r'urn:ngsi-ld:Sensor:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+', sensor_urn):
        raise ValueError('paper_p5a_read_failed')
    if type(max_points) is not int or not 100 <= max_points <= 2000:
        raise ValueError('paper_p5a_read_failed')
    del smoke  # Keep the existing helper call shape; this endpoint uses exact split bounds.
    query = urllib.parse.urlencode({
        'attrs': 'measurement', 'timerel': 'between',
        'timeAt': first if isinstance(first, str) else _utc_z(first),
        'endTimeAt': last if isinstance(last, str) else _utc_z(last),
        'max_points': str(max_points),
    })
    return ('/v1/digital-twins/' + urllib.parse.quote(sensor_urn, safe=':') + '/temporal?'
            + query)


def temporal_count_matches(body, expected):
    values = body.get('measurement', {}).get('values', []) if isinstance(body, dict) else []
    return isinstance(values, list) and len(values) == expected


def expected_measurement_points(config):
    return config['messages_per_gateway'] * config['samples_per_message']


def history_max_points(config):
    # The API's minimum is 100; never request more than the contract hard cap.
    return min(2000, max(100, expected_measurement_points(config)))


def history_read_diagnostic(sensor_urn, twin_status, twin_body, temporal_status,
                            temporal_body, *, expected_points, max_points, first, last,
                            query_windows=None):
    twin_shape_ok = isinstance(twin_body, dict)
    reported_state = twin_body.get('reported_state') if twin_shape_ok else None
    reported_version = twin_body.get('reported_version') if twin_shape_ok else None
    twin_state_ok = (twin_shape_ok and twin_body.get('id') == sensor_urn
                     and isinstance(reported_state, dict) and bool(reported_state)
                     and isinstance(reported_version, str) and reported_version.isdigit()
                     and int(reported_version) > 0)
    values = None
    if isinstance(temporal_body, dict):
        measurement = temporal_body.get('measurement')
        if isinstance(measurement, dict) and isinstance(measurement.get('values'), list):
            values = measurement['values']
    actual_points = len(values) if values is not None else None
    reason = None
    if twin_status != 200:
        reason = 'twin_http_error'
    elif not twin_shape_ok:
        reason = 'twin_body_malformed'
    elif not twin_state_ok:
        reason = 'twin_state_mismatch'
    elif temporal_status != 200:
        reason = 'temporal_http_error'
    elif actual_points is None:
        reason = 'temporal_body_malformed'
    elif actual_points != expected_points:
        reason = 'temporal_point_count_mismatch'
    return {
        'sensor_urn': sensor_urn,
        'twin_http_status': twin_status,
        'twin_state_valid': twin_state_ok,
        'temporal_http_status': temporal_status,
        'expected_points': expected_points,
        'actual_points': actual_points,
        'query': {'attrs': 'measurement', 'max_points': max_points,
                  'window_source': 'telemetry.measured_at_min_max',
                  'first_measured_at': first, 'last_measured_at': last,
                  'range_semantics': '[timeAt,endTimeAt); last sample included with 1us end extension',
                  'time_format': 'UTC-Z', 'windows': query_windows or []},
        'reason': reason,
        'accepted': reason is None,
    }


def persist_history_read_diagnostic(run_data, diagnostic, persist):
    run_data.setdefault('history_reads', []).append(safe_artifact(diagnostic))
    persist()
    if not diagnostic['accepted']:
        raise ValueError('paper_p5a_read_regression_failed')


def gateway_measurement(output):
    """Preserve actual bounded ACK summary; never substitute window durations for latency."""
    from stage3_workload_runner import parse_measurement_summary

    parsed = parse_measurement_summary(output)
    if not parsed:
        reason = 'summary_missing' if 'measurement summary:' not in output else 'summary_malformed'
        return {'status': 'unavailable', 'reason': reason}
    keys = ('samples', 'p50_ms', 'p95_ms', 'min_ms', 'max_ms', 'first_publishes',
            'retry_attempts', 'missing_origins', 'overflow_samples', 'gen_duration_ms',
            'publish_drain_duration_ms', 'total_duration_ms')
    result = {'status': 'available', **{key: parsed[key] for key in keys}}
    # ACK durations are bounded exact samples, not a histogram; CLI percentiles
    # are rendered to two decimal places in milliseconds.
    result['histogram_resolution_ms'] = None
    result['reported_resolution_ms'] = 0.01 if parsed['samples'] and parsed['p50_ms'] != 'none' else None
    result['resolution_basis'] = 'no histogram; duration samples, CLI rendering rounds to 0.01 ms'
    return result


def validate_accounting(config, accounting):
    expected = {
        'offered_unique': config['offered_unique_messages'],
        'committed_unique': config['offered_unique_messages'],
        'raw_samples': config['offered_unique_samples'],
        'temporal': config['offered_unique_messages'],
        'application_acks': config['offered_unique_messages'],
        'outbox_rows': 0, 'outbox_bytes': 0, 'payload_budget_bytes': 0,
    }
    if any(type(accounting.get(key)) is not int or accounting[key] != value
           for key, value in expected.items()):
        raise ValueError('paper_p5a_accounting_rejected')
    return True


def acceptance_ready(checks, mode):
    if mode not in ('off', 'on') or not REQUIRED_CHECKS.issubset(checks):
        return False
    if any(checks[name].get('status') != 'PASS' for name in REQUIRED_CHECKS):
        return False
    group = checks['group_metrics_observed']
    if mode == 'on' and (group.get('max_members', 0) < 2 or group.get('groups', 0) < 1):
        return False
    if mode == 'off' and group.get('microbatch_enabled') is not False:
        return False
    return True


def safe_artifact(value):
    if isinstance(value, dict):
        return {key: safe_artifact(item) for key, item in value.items()
                if key.lower() not in {'password', 'token', 'secret', 'payload', 'payload_bytes', 'raw_payload'}
                and not key.lower().endswith(('_password', '_token', '_secret'))}
    if isinstance(value, list):
        return [safe_artifact(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ValueError('paper_p5a_artifact_rejected')


def run(smoke, fixture, endpoint, registry, containers, images, lifecycle_state):
    """Run a finite two-Gateway workload; every success is read back from runtime."""
    if not getattr(fixture, 'paper_p5a', False):
        raise ValueError('paper_p5a_fixture_required')
    if any(value is None for value in (endpoint, registry, containers, images, lifecycle_state)):
        raise ValueError('paper_p5a_fixture_required')

    import stage3_uplink_runner as uplink

    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, lifecycle_state))
    mode, dataset = fixture.paper_p5a_mode, fixture.paper_p5a_dataset
    config = workload_config(dataset)
    shutdown_smoke = bool(getattr(fixture, 'paper_p5a_shutdown', False))
    profile_data = run_metadata(mode, dataset, shutdown_smoke=shutdown_smoke, effective_environment=
        effective.get('services', {}).get('backend', {}).get('environment', {}))
    if quota != BACKEND_QUOTA or not isinstance(endpoint, smoke.HTTPSEndpoint):
        raise ValueError('paper_p5a_profile_rejected')
    if len(specs) < 2 or len(passwords) < 2:
        raise ValueError('paper_p5a_fixture_required')

    run_data = {
        'schema_version': 1, 'run_id': fixture.project, 'status': 'FAIL',
        'mode': mode, 'dataset': profile_data['dataset'], 'native_executed': True,
        'revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'branch': smoke.paper_p0_git(fixture, 'branch', '--show-current'),
        'dirty': bool(smoke.paper_p0_git(fixture, 'status', '--porcelain')),
        'auth_mode': 'rs256', 'https_verified': True, 'backend_quota': quota,
        'service_resources': resources,
        'images': {name: images[name] for name in smoke.fixture_services(fixture)},
        'gateway_image_id': images['gateway'], 'effective_profile': profile_data['effective_profile'],
        'effective_profile_source': profile_data['effective_profile_source'],
        'shaper': profile_data['shaper'], 'auth_tls_acl': profile_data['auth_tls_acl'],
        'storage_status': storage_status, 'workload': config, 'phases': [], 'checks': {},
        'history_reads': [],
        'offered_pattern': {'status': 'same_finite_gateway_cli_pattern_for_both_modes',
                            'cadence': "Gateway simulator configured sample-rate controls generation; no synthetic future samples"},
        'cleanup_verified': False, 'ram_cache': {'enabled': False, 'status': 'not_implemented'},
    }
    metrics, resource_rows = [], []
    started = time.monotonic()

    def persist():
        smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))

    def check(name, success, **detail):
        run_data['checks'][name] = {'status': 'PASS' if success else 'FAIL', **safe_artifact(detail)}
        persist()
        if not success:
            raise ValueError('paper_p5a_acceptance_failed')

    def sql(query, **variables):
        return smoke.db_psql(registry, pg, query, variables).strip()

    def scrape(phase):
        lifecycle_state.checkpoint()
        status, _, snapshot = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        if status != 200 or not isinstance(snapshot, dict) or not isinstance(snapshot.get('ingress'), dict):
            raise ValueError('paper_p5a_metrics_unavailable')
        sample = {'phase': phase, 'observed_monotonic': time.monotonic(), 'snapshot': snapshot}
        metrics.append(sample)
        smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', metrics)
        resource = smoke.paper_p0_stats(registry, containers, phase)
        resource_rows.append(resource)
        smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv', resource_rows, resources)
        return sample

    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    volumes = []
    for index in range(2):
        volume = registry.intent('volume', fixture.project + '_outbox_p5a_' + str(index), None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        result = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, volume['name']])
        if result.returncode or registry.resolve(volume) is None:
            raise ValueError('helper_allocation_unresolved')
        volumes.append(volume)

    def gateway(index, tag, *, during=None):
        spec = specs[index]
        env = uplink.gateway_env(spec.gateway_id, passwords[index])
        env.update(GATEWAY_MQTT_CLIENT_ID=fixture.project + '_p5a_' + str(index) + '_' + tag,
                   GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883',
                   GATEWAY_SAMPLE_RATE_HZ=str(config['steady_sample_rate_hz']),
                   GATEWAY_SAMPLES_PER_BATCH=str(config['samples_per_message']))
        env_path = fixture.private / ('gateway-p5a-' + str(index) + '-' + tag + '.env')
        smoke.private_write(env_path, ''.join(key + '=' + value + '\n' for key, value in env.items()))
        return smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[index], env_path,
            fixture.private / 'certs/ca.crt', 'p5a-' + str(index) + '-' + tag,
            ['run', '--batches', str(config['messages_per_gateway']), '--completion-timeout', '80s',
             *gateway_measurement_args(config)],
            lifecycle_state, during=during, timeout=90)

    def gateway_count(index):
        spec = specs[index]
        env_path = fixture.private / ('gateway-p5a-' + str(index) + '-count.env')
        smoke.private_write(env_path, 'GATEWAY_OUTBOX_PATH=/data/outbox/outbox.db\n')
        output = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[index], env_path,
            fixture.private / 'certs/ca.crt', 'p5a-' + str(index) + '-count', ['count'],
            lifecycle_state, timeout=30)
        return output.strip()

    observers = []
    try:
        before = scrape('before_generation')
        for index, observer_spec in enumerate(observer_specs(config)):
            observers.append(smoke.paper_p1_subscriber(fixture, registry,
                containers['mosquitto']['receipt']['id'], specs[index], passwords[index], observer_spec['tag'],
                message_count=observer_spec['count'], message_timeout=observer_spec['timeout'],
                subscription_kind=observer_spec['kind']))
        run_data['phases'].append({'name': 'generation_start', 'observed_monotonic': before['observed_monotonic']})
        second_output = []
        gateway_windows = {}

        def concurrent_second_gateway():
            gateway_windows['second_start'] = time.monotonic()
            second_output.append(gateway(1, 'generation'))
            gateway_windows['second_end'] = time.monotonic()

        gateway_windows['first_start'] = time.monotonic()
        first_output = gateway(0, 'generation', during=concurrent_second_gateway)
        gateway_windows['first_end'] = time.monotonic()
        summaries = []
        ack_measurements = []
        for index, output in enumerate((first_output, second_output[0] if second_output else '')):
            from stage3_mqtt_ingress_runner import parse_run_summary
            summary = parse_run_summary(output)
            if (summary.get('completed') != 'true'
                    or int(summary.get('enqueued', '-1')) != config['messages_per_gateway']
                    or int(summary.get('app_acknowledged', '-1')) != config['messages_per_gateway']
                    or summary.get('pending_rows') != '0' or summary.get('pending_bytes') != '0'):
                raise ValueError('paper_p5a_gateway_summary_rejected')
            summaries.append(summary)
            measurement = gateway_measurement(output)
            ack_measurements.append(measurement)
            run_data['phases'].append({'name': 'gateway_' + str(index) + '_generation_and_ack',
                'start_monotonic': gateway_windows['first_start' if index == 0 else 'second_start'],
                'end_monotonic': gateway_windows['first_end' if index == 0 else 'second_end'],
                'duration_seconds': gateway_windows[('first_end' if index == 0 else 'second_end')]
                    - gateway_windows[('first_start' if index == 0 else 'second_start')],
                'enqueued': int(summary['enqueued']),
                'application_acknowledged': int(summary['app_acknowledged']),
                'pending_rows_at_cli_exit': int(summary['pending_rows']),
                'pending_bytes_at_cli_exit': int(summary['pending_bytes']),
                'ack_measurement': measurement})
        outbox_counts = [gateway_count(index) for index in range(2)]
        captured = []
        for index, spec in enumerate(specs[:2]):
            payload = smoke.paper_p1_capture_subscription(observers[index],
                'gateways/' + spec.gateway_id + '/telemetry/' + spec.sensor_id, timeout=10)
            if not isinstance(payload, bytes):
                raise ValueError('paper_p5a_observer_capture_failed')
            parsed = json.loads(payload)
            if not isinstance(parsed.get('message_id'), str):
                raise ValueError('paper_p5a_observer_capture_failed')
            captured.append((payload, parsed['message_id']))
        drained = scrape('drain_commit_end')
        ingress = drained['snapshot']['ingress']
        persistence = drained['snapshot'].get('persistence')
        if ingress.get('BufferedPayloadBytes') != 0:
            raise ValueError('paper_p5a_accounting_rejected')
        run_data['phases'].append({'name': 'generation_and_drain_complete',
            'observed_monotonic': drained['observed_monotonic'],
            'generation_plus_drain_seconds': drained['observed_monotonic'] - before['observed_monotonic'],
            'generation_window': 'Gateway generation and application-ACKing CLI run windows',
            'drain_commit_window': 'after final Gateway summary through final metrics sample'})

        ids = [specs[i].gateway_id for i in range(2)]
        ledgers = int(sql("SELECT count(*) FROM processed_messages WHERE gateway_id IN (:'a',:'b');",
                          a=ids[0], b=ids[1]))
        raw = int(sql("SELECT count(*) FROM telemetry WHERE gateway_id IN (:'a',:'b');", a=ids[0], b=ids[1]))
        temporal = int(sql("SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id IN (:'ua',:'ub');",
            ua=specs[0].sensor_urn, ub=specs[1].sensor_urn))
        per_gateway = json.loads(sql(PER_GATEWAY_SUMMARY_SQL, a=ids[0], b=ids[1]))
        per_temporal = json.loads(sql(PER_TEMPORAL_SUMMARY_SQL,
          ua=specs[0].sensor_urn, ub=specs[1].sensor_urn))
        mismatch = int(sql("""SELECT count(*) FROM processed_messages p WHERE p.gateway_id IN (:'a',:'b')
          AND (p.sample_count <> (SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
            OR p.first_sequence <> (SELECT min(sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
            OR p.last_sequence <> (SELECT max(sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id));""", a=ids[0], b=ids[1]))
        unique_values = int(sql("""SELECT count(*) FROM telemetry t JOIN processed_messages p USING(gateway_id,message_id)
          WHERE t.gateway_id IN (:'a',:'b') AND (t.boot_id <> p.boot_id OR t.sample_index <> t.sequence_num-p.first_sequence);""", a=ids[0], b=ids[1]))
        timestamp_mismatch = int(sql("""SELECT count(*) FROM processed_messages p WHERE p.gateway_id IN (:'a',:'b')
          AND (SELECT max(t.measured_at)-min(t.measured_at) FROM telemetry t
               WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
              <> (p.sample_count-1)*10000*interval '1 microsecond';""", a=ids[0], b=ids[1]))
        raw_values = json.loads(sql("""SELECT coalesce(json_agg(json_build_array(sequence_num,value)
          ORDER BY gateway_id,message_id,sample_index),'[]'::json) FROM telemetry WHERE gateway_id IN (:'a',:'b');""",
          a=ids[0], b=ids[1]))
        value_mismatch = sum(abs(float(value) - uplink.synthetic_value(int(sequence))) > 1e-9
                             for sequence, value in raw_values)
        expected = config['offered_unique_messages']
        expected_samples = config['messages_per_gateway'] * config['samples_per_message']
        check('workload_accounting', ledgers == expected and raw == config['offered_unique_samples']
              and len(per_gateway) == 2 and all(row['messages'] == config['messages_per_gateway']
              and row['samples'] == expected_samples for row in per_gateway),
              offered_unique=expected, committed_unique=ledgers, raw_samples=raw,
              per_gateway=per_gateway)
        check('ledger_raw_provenance', mismatch == 0 and unique_values == 0
              and timestamp_mismatch == 0 and value_mismatch == 0,
              aggregate_mismatch=mismatch, provenance_mismatch=unique_values,
              timestamp_mismatch=timestamp_mismatch, synthetic_value_mismatch=value_mismatch)
        check('temporal_per_message', temporal == expected and len(per_temporal) == 2
              and all(row['observations'] == config['messages_per_gateway'] for row in per_temporal),
              temporal=temporal, per_sensor=per_temporal)
        states = json.loads(sql("""SELECT coalesce(json_agg(json_build_object(
          'reported_version',s.reported_version,'reported_state',s.reported_state) ORDER BY e.entity_id),'[]'::json)
          FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id IN (:'ua',:'ub');""",
          ua=specs[0].sensor_urn, ub=specs[1].sensor_urn))
        check('current_state_and_versions', len(states) == 2 and all(
              row.get('reported_version', 0) > 0 and isinstance(row.get('reported_state'), dict)
              and bool(row['reported_state']) for row in states), state_rows=len(states),
              reported_versions=[row.get('reported_version') for row in states],
              reported_state_keys=[sorted(row.get('reported_state', {}).keys()) for row in states])
        ack_total = sum(int(item['app_acknowledged']) for item in summaries)
        empty_count = 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0'
        check('application_acks_and_empty_outboxes', ack_total == expected and all(
            item['pending_rows'] == '0' and item['pending_bytes'] == '0' for item in summaries)
            and all(value == empty_count for value in outbox_counts),
            application_acks=ack_total, outbox_count_readbacks=outbox_counts)

        micro = ingress.get('microbatch', {})
        check('group_metrics_observed', isinstance(micro, dict)
              and micro.get('enabled') is (mode == 'on')
              and (mode != 'on' or (micro.get('groups', 0) > 0 and micro.get('max_members', 0) >= 2)),
              microbatch_enabled=micro.get('enabled'), groups=micro.get('groups', 0),
              max_members=micro.get('max_members', 0), microbatch=micro)
        persistence_fields = ('single_attempts', 'group_attempts', 'fallback_attempts',
            'begin_attempts', 'begin_succeeded', 'confirmed_commits', 'rollback_attempts',
            'confirmed_rollbacks', 'commit_unconfirmed', 'copy_calls',
            'copy_returned_rows', 'confirmed_new_rows')
        check('transaction_copy_metrics_observed', isinstance(persistence, dict)
              and all(type(persistence.get(key)) is int and persistence[key] >= 0
                      for key in persistence_fields)
              and (persistence.get('group_attempts', 0) > 0 if mode == 'on'
                   else persistence.get('single_attempts', 0) > 0),
              persistence=persistence)
        check('payload_budget_drained', ingress.get('BufferedPayloadBytes') == 0,
              buffered_payload_bytes=ingress.get('BufferedPayloadBytes'))

        # Read the existing HTTPS APIs under the provisioned gateway identity.
        for index, spec in enumerate(specs[:2]):
            twin_status, _, twin_body = smoke.http_request(endpoint, 'GET', '/v1/digital-twins/' + spec.sensor_urn
                + '?format=simplified', token=tokens['a' if index == 0 else 'b'])
            times = json.loads(sql("SELECT json_build_object('first',min(measured_at),'last',max(measured_at)) "
                "FROM telemetry WHERE gateway_id=:'gw';", gw=spec.gateway_id))
            if not times.get('first') or not times.get('last'):
                raise ValueError('paper_p5a_read_regression_failed')
            max_points = history_max_points(config)
            expected_points = expected_measurement_points(config)
            query_windows = []
            combined_values = []
            all_windows_well_formed = True
            window_statuses = []
            for window_start, window_end in temporal_read_windows(times['first'], times['last']):
                history_path = temporal_read_path(smoke, spec.sensor_urn,
                    window_start, window_end, max_points)
                window_status, _, window_body = smoke.http_request(endpoint, 'GET', history_path,
                    token=tokens['a' if index == 0 else 'b'])
                measurement = window_body.get('measurement') if isinstance(window_body, dict) else None
                values = measurement.get('values') if isinstance(measurement, dict) else None
                if not isinstance(values, list):
                    all_windows_well_formed = False
                    actual_window_points = None
                else:
                    combined_values.extend(values)
                    actual_window_points = len(values)
                query_windows.append({'timeAt': _utc_z(window_start),
                    'endTimeAt': _utc_z(window_end), 'http_status': window_status,
                    'actual_points': actual_window_points})
                window_statuses.append(window_status)
            temporal_status = next((status for status in window_statuses if status != 200), 200)
            temporal_body = ({'measurement': {'values': combined_values}}
                             if all_windows_well_formed else {})
            diagnostic = history_read_diagnostic(spec.sensor_urn, twin_status, twin_body,
                temporal_status, temporal_body, expected_points=expected_points,
                max_points=max_points, first=times['first'], last=times['last'],
                query_windows=query_windows)
            persist_history_read_diagnostic(run_data, diagnostic, persist)
        check('https_reads', True, twin_and_temporal_status=200,
              temporal_points_per_sensor=expected_measurement_points(config),
              temporal_point_unit='raw measurement samples per Sensor',
              temporal_max_points=history_max_points(config),
              temporal_window_source='actual telemetry measured_at min/max partitioned below the 10-second raw-window bound')
        jwt_valid = all(smoke.paper_p2_support().decode_token(token)[0].get('alg') == 'RS256'
                        for token in tokens.values())
        check('rs256_https_mqtt_tls_acl', run_data['https_verified'] and jwt_valid
              and profile_data['auth_tls_acl']['acl_enforced'],
              auth='RS256', https=True, mqtt_tls=True, acl=True)

        def fingerprint(gateway_id, sensor_urn):
            return json.loads(sql("""SELECT json_build_object(
              'ledger', md5(coalesce((SELECT string_agg(to_jsonb(p)::text,'' ORDER BY p.message_id)
                FROM processed_messages p WHERE p.gateway_id=:'gw'),'')),
              'raw', md5(coalesce((SELECT string_agg(to_jsonb(t)::text,'' ORDER BY t.message_id,t.sample_index)
                FROM telemetry t WHERE t.gateway_id=:'gw'),'')),
              'temporal', md5(coalesce((SELECT string_agg(to_jsonb(v)::text,'' ORDER BY v.message_id,v.property_name)
                FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
                WHERE e.entity_id=:'urn'),'')),
              'state', (SELECT to_jsonb(s)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
                WHERE e.entity_id=:'urn'));""", gw=gateway_id, urn=sensor_urn))

        replay_payload, _ = captured[1]
        replay_path = fixture.private / 'p5a-exact-replay-payload.bin'
        replay_fd = os.open(replay_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(replay_fd, 'wb') as replay_file:
            replay_file.write(replay_payload)
            replay_file.flush()
            os.fsync(replay_file.fileno())
        replay_payload = replay_path.read_bytes()
        target = specs[1]
        before_fp = fingerprint(target.gateway_id, target.sensor_urn)
        before_metrics = scrape('before_exact_replay')
        broker_id = containers['mosquitto']['receipt']['id']
        replay = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(target.gateway_id + '\n' + passwords[1] + '\n' +
                   'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id + '\n').encode() + replay_payload)
        if replay.returncode:
            raise ValueError('paper_p5a_duplicate_replay_failed')
        replay_after = None
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            lifecycle_state.checkpoint()
            candidate = scrape('exact_replay_observed')
            old = before_metrics['snapshot']['ingress'].get('DuplicateCommitted')
            new = candidate['snapshot']['ingress'].get('DuplicateCommitted')
            old_ack = before_metrics['snapshot']['ingress'].get('ACKPublished')
            new_ack = candidate['snapshot']['ingress'].get('ACKPublished')
            if (isinstance(old, int) and isinstance(new, int) and new > old
                    and isinstance(old_ack, int) and isinstance(new_ack, int) and new_ack > old_ack):
                replay_after = candidate
                break
            time.sleep(0.25)
        unchanged = before_fp == fingerprint(target.gateway_id, target.sensor_urn)
        check('duplicate_replay_unchanged', replay_after is not None and unchanged,
              duplicate_metric_observed=replay_after is not None, database_unchanged=unchanged,
              payload_sha256=hashlib.sha256(replay_payload).hexdigest(),
              manual_replay_not_counted_as_gateway_attempt=True)

        import stage3_uplink_runner as uplink
        logs_before = uplink.container_logs(broker_id)
        before_denial_fp = fingerprint(target.gateway_id, target.sensor_urn)
        deny_payload, _ = captured[0]
        denied = registry.command(['exec', '-i', broker_id, 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(specs[0].gateway_id + '\n' + passwords[0] + '\n' +
                   'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id + '\n').encode()
                  + deny_payload)
        logs_after = uplink.wait_denied_publish(broker_id, logs_before,
            'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id,
            uplink.PROBE_DENY_SETTLE)
        denied_observed = uplink.denied_publish_logged(logs_before, logs_after,
            'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id)
        denial_unchanged = before_denial_fp == fingerprint(target.gateway_id, target.sensor_urn)
        check('wrong_namespace_denied', denied_observed and denial_unchanged,
              broker_audit_denial=denied_observed, target_database_unchanged=denial_unchanged,
              client_exit_code=denied.returncode,
              payload_sha256=hashlib.sha256(deny_payload).hexdigest())
        check('exporters_written', (fixture.artifacts / 'metrics.csv').is_file()
              and (fixture.artifacts / 'resources.csv').is_file(),
              metrics_csv=True, resources_csv=True)

        if shutdown_smoke:
            # Separate functional smoke: all finite Gateway writers are done.
            # Admit one fresh TLS/DynSec message, prove it is validated and held
            # in an underfilled group, then signal the real backend container.
            from stage3_mqtt_ingress_runner import verify_ack
            from stage3_uplink_runner import build_probe_batch

            primary = specs[0]
            ack_observer = smoke.paper_p1_subscriber(fixture, registry,
                containers['mosquitto']['receipt']['id'], primary, passwords[0],
                'p5a-shutdown-ack', message_count=1, message_timeout=8, subscription_kind='ack')
            shutdown_id = str(__import__('uuid').uuid4())
            sample = json.loads(sql("""SELECT json_build_object(
              'boot_id',boot_id,'sequence',coalesce(max(sequence_num),0)+1)
              FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor' GROUP BY boot_id
              ORDER BY max(sequence_num) DESC LIMIT 1;""", gw=primary.gateway_id, sensor=primary.sensor_id))
            measured = datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
            topic = 'gateways/' + primary.gateway_id + '/telemetry/' + primary.sensor_id
            ack_topic = 'gateways/' + primary.gateway_id + '/acks/' + shutdown_id
            payload = build_probe_batch(primary, shutdown_id, sample['boot_id'], int(sample['sequence']),
                sample_count=1, sample_interval_us=10000, measured_at=measured)
            before = scrape('before_shutdown_partial_group')['snapshot']['ingress']
            pub_result = registry.command(['exec', '-i', containers['mosquitto']['receipt']['id'],
                'sh', '-ec', 'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                input=(primary.gateway_id + '\n' + passwords[0] + '\n' + topic + '\n').encode() + payload)
            if pub_result.returncode:
                smoke.paper_p1_stop_subscription(ack_observer)
                raise ValueError('paper_p5a_shutdown_publish_failed')
            publish_at = time.monotonic()
            msg_count = "SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id';"
            pending_snapshot = None
            deadline = publish_at + 1.5
            try:
                while time.monotonic() < deadline:
                    candidate = scrape('shutdown_partial_group_observed')['snapshot']['ingress']
                    ledger_before_stop = int(sql(msg_count, gw=primary.gateway_id, id=shutdown_id))
                    if (candidate.get('Processed', 0) > before.get('Processed', 0)
                            and candidate.get('BufferedPayloadBytes', 0) > 0
                            and candidate.get('ValidationRejected', 0) == before.get('ValidationRejected', 0)
                            and ledger_before_stop == 0
                            and time.monotonic() - publish_at < 2.0):
                        pending_snapshot = candidate
                        break
                    time.sleep(0.1)
                if pending_snapshot is None:
                    raise ValueError('paper_p5a_shutdown_partial_group_unproven')
                backend_id = containers['backend']['receipt']['id']
                shutdown_started = time.monotonic()
                signalled = registry.command(['kill', '--signal', 'TERM', backend_id])
                if signalled.returncode:
                    raise ValueError('paper_p5a_shutdown_signal_failed')
                common_budget = int(effective.get('services', {}).get('backend', {})
                                    .get('environment', {}).get('SHUTDOWN_TIMEOUT', '10s').rstrip('s'))
                stop_deadline = shutdown_started + common_budget
                exit_code = None
                ack_payload = None
                while time.monotonic() < stop_deadline:
                    inspect = registry.command(['inspect', '--format', '{{.State.Running}} {{.State.ExitCode}}', backend_id])
                    if inspect.returncode == 0:
                        state = inspect.stdout.decode().strip().split()
                        if len(state) == 2 and state[0] == 'false':
                            exit_code = int(state[1])
                    if ack_payload is None:
                        ack_payload = smoke.paper_p1_capture_subscription(ack_observer, ack_topic, timeout=0.2)
                    if exit_code is not None and ack_payload is not None:
                        break
                    time.sleep(0.1)
                if exit_code != 0 or ack_payload is None:
                    raise ValueError('paper_p5a_shutdown_deadline_failed')
                verify_ack(shutdown_id, ack_payload)
                persisted = json.loads(sql("""SELECT json_build_object(
                  'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id'),
                  'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND message_id=:'id'),
                  'temporal',(SELECT count(*) FROM twin_temporal_values WHERE message_id=:'id'));""",
                  gw=primary.gateway_id, id=shutdown_id))
                if persisted != {'ledger': 1, 'raw': 1, 'temporal': 1}:
                    raise ValueError('paper_p5a_shutdown_persistence_failed')
                log_result = registry.command(['logs', backend_id])
                summary = (log_result.stdout + log_result.stderr).decode(errors='replace')
                if not re.search(r'buffered_payload_bytes=0(?:\s|$)', summary):
                    raise ValueError('paper_p5a_shutdown_bytes_not_drained')
                check('controlled_shutdown_partial_group', True,
                    max_wait_seconds=2, members_before_signal=1, validated_before_signal=True,
                    ledger_absent_before_signal=True, buffered_payload_bytes_before_signal=
                        pending_snapshot.get('BufferedPayloadBytes'), timer_elapsed_before_signal=False,
                    signal='SIGTERM', backend_exit_code=exit_code,
                    shutdown_elapsed_seconds=time.monotonic() - shutdown_started,
                    common_shutdown_budget_seconds=common_budget, persisted=persisted,
                    application_ack_verified=True, final_buffered_payload_bytes=0)
                run_data['shutdown_smoke'] = {'status': 'PASS', 'message_id': shutdown_id,
                    'ack_topic': ack_topic, 'payload_sha256': hashlib.sha256(payload).hexdigest(),
                    'group_members': 1, 'timer_target_seconds': 2, 'backend_exit_code': exit_code,
                    'shutdown_elapsed_seconds': time.monotonic() - shutdown_started,
                    'shutdown_budget_seconds': common_budget, 'persisted': persisted,
                    'final_buffered_payload_bytes': 0}
            finally:
                smoke.paper_p1_stop_subscription(ack_observer)

        accounting = {'offered_unique': expected, 'committed_unique': ledgers,
            'raw_samples': raw, 'temporal': temporal, 'application_acks': ack_total,
            'outbox_rows': 0, 'outbox_bytes': 0, 'payload_budget_bytes': ingress['BufferedPayloadBytes']}
        validate_accounting(config, accounting)
        if time.monotonic() - started > config['run_deadline_seconds']:
            raise ValueError('paper_p5a_deadline_exceeded')
        run_data.update(accounting=accounting, backend_metrics=ingress, status='PASS')
        run_data['backend_snapshot'] = drained['snapshot']
        run_data['gateway_ack_measurements'] = ack_measurements
        gateway_attempts = sum(int(row.get('publish_attempts', '0')) for row in summaries)
        run_data['gateway_attempt_accounting'] = {
            'publish_attempts': gateway_attempts,
            'unique_messages': expected,
            'retries': max(0, gateway_attempts - expected),
            'basis': 'sum of actual Gateway CLI publish_attempts minus generated unique messages',
            'pending_backlog_at_final_cli_exit': 0,
        }
        run_data['full_p5a_workload_accepted'] = acceptance_ready(run_data['checks'], mode)
        if shutdown_smoke:
            run_data['full_p5a_workload_accepted'] = (run_data['full_p5a_workload_accepted']
                and run_data.get('shutdown_smoke', {}).get('status') == 'PASS')
        if not run_data['full_p5a_workload_accepted']:
            raise ValueError('paper_p5a_acceptance_failed')
        persist()
        return run_data
    except BaseException:
        persist()
        raise
    finally:
        for observer in observers:
            smoke.paper_p1_stop_subscription(observer)
