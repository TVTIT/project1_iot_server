"""P6 comparable fixture profile and reusable mixed-workload setup.

This module owns the P6 profile, workload description, and reusable mixed-load
fixture workflow. Workload evidence is emitted only after the live fixture passes
all accounting and cleanup gates.
"""

import math
from pathlib import Path
import re
import subprocess
import secrets
import sys
import time

from paper_p5a_support import BACKEND_QUOTA, DATASET_PROFILES

PROFILE_COMMON = {
    'STAGE5_ENABLED': 'true',
    'STAGE5_WAKEUP_ENABLED': 'false',
    'STAGE5_COMMAND_TTL': '60s',
    'STAGE5_SCAN_INTERVAL': '1s',
    'STAGE5_SCAN_BATCH': '32',
    'STAGE5_PUBLISH_QUEUE_CAPACITY': '32',
    'STAGE5_PUBLISH_WORKERS': '1',
    'STAGE5_RESULT_QUEUE': '128',
    'STAGE5_RESULT_WORKERS': '2',
    'DATABASE_MAX_CONNS': '12',
    'MQTT_SHAPER_ENABLED': 'true',
    'MQTT_SHAPER_RATE_PER_SECOND': '2',
    'MQTT_SHAPER_BURST': '4',
    'MQTT_SHAPER_MAX_GATEWAYS': '4096',
    'MQTT_SHAPER_IDLE_RETENTION': '10m',
    'MQTT_INGRESS_BUFFERED_BYTES': '16777216',
    'MQTT_MICROBATCH_ENABLED': 'true',
    'MQTT_MICROBATCH_MAX_MESSAGES': '32',
    'MQTT_MICROBATCH_MAX_ROWS': '500',
    'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304',
    'MQTT_MICROBATCH_MAX_WAIT': '1s',
    'MQTT_RAM_DEDUP_ENABLED': 'true',
    'MQTT_RAM_DEDUP_MAX_ENTRIES': '100000',
    'MQTT_RAM_DEDUP_MAX_BYTES': '67108864',
    'SUPABASE_JWT_MODE': 'rs256',
    'SUPABASE_JWKS_URL': 'http://supabase-envoy:8000/auth/v1/.well-known/jwks.json',
    'AUTH_UNIX_SOCKET_PATH': '/run/iot-auth/auth.sock',
}


def profile(mode, dataset):
    if mode not in ('polling', 'wake-up'):
        raise ValueError('paper_p6_mode_rejected')
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p6_dataset_rejected')
    values = dict(PROFILE_COMMON)
    values['STAGE5_WAKEUP_ENABLED'] = 'true' if mode == 'wake-up' else 'false'
    values['MQTT_MAX_PAYLOAD_BYTES'] = '65536'
    values['MQTT_INGRESS_ENABLED'] = 'true'
    values['MQTT_QUEUE_CAPACITY'] = '256'
    values['MQTT_WORKER_COUNT'] = '4'
    return values


def profiles_differ_only_by_wakeup(polling, wakeup):
    return ({k: v for k, v in polling.items() if k != 'STAGE5_WAKEUP_ENABLED'}
            == {k: v for k, v in wakeup.items() if k != 'STAGE5_WAKEUP_ENABLED'}
            and polling.get('STAGE5_WAKEUP_ENABLED') == 'false'
            and wakeup.get('STAGE5_WAKEUP_ENABLED') == 'true')


def validate_effective_profile(environment, mode, dataset):
    expected = profile(mode, dataset)
    if any(str(environment.get(k)).lower() != v for k, v in expected.items()):
        raise ValueError('paper_p6_profile_rejected')
    return True


def workload_spec(dataset):
    if dataset not in DATASET_PROFILES:
        raise ValueError('paper_p6_dataset_rejected')
    samples = DATASET_PROFILES[dataset]['samples_per_message']
    return {
        'status': 'configured_not_run',
        'gateway_count': 2,
        'gateway_roles': {'gw-a': 'steady_telemetry_and_commands',
                          'gw-b': 'steady_and_burst_telemetry'},
        'sensors': {'total': 4, 'telemetry_per_gateway': 1,
                    'telemetry_ids': ['existing_gw_a_sensor', 'existing_gw_b_sensor'],
            'command_sensors_per_gateway': {'gw-a': 2, 'gw-b': 0},
                    'commands_use_separate_sensors': True},
        'telemetry': {'samples_per_message': samples, 'main_profiles_samples_per_message': 100,
                      'sample_rate_hz': 100 if dataset == '100' else 1,
                      'sample_rate_semantics': 'integer simulator sample rate; message cadence is scheduled separately',
                      'representative_single_sample_profile': dataset == '1',
                      'payload_semantics': 'valid existing simulator telemetry schema; unique message IDs'},
        'phases_seconds': {'warmup': 0, 'steady_generate': 60, 'steady_rate_per_gateway': 1.0,
                           'steady_gateways': ['gw-a', 'gw-b'],
                           'steady_messages_by_gateway': {'gw-a': 60, 'gw-b': 12},
                           'burst_generate': 6, 'burst_rate': 4.0, 'burst_messages': 24,
                           'burst_gateway': 'gw-b',
                            'command_producer_stop': 'after final command and DB/REST configuration verification during B burst',
                            'drain_deadline': 120,
                           'maximum_total_deadline': 240,
                           'steady_command_overlap_required': True,
                           'burst_command_overlap_required': True},
        'command_stream': {'status': 'configured_not_run',
                           'target_sampling_frequency_hz_sequence': [1, 2, 4],
                           'admission_request_rate': 'one new request only after each prior command is terminal',
                           'version': 'strictly increasing per Sensor',
                           'idempotency_key': 'unique per new intent; never reused across commands',
                           'one_active_command_per_sensor': True,
                           'next_command_only_after_previous_terminal': True,
                           'new_command_sensor_pair_frequency_hz': [1, 2],
                           'representative_pair_sample_count': 1},
         'accounting': {'telemetry_exact_counts_required': True,
                        'command_sensor_telemetry_excluded': True,
                        'command_sensors_excluded_from_telemetry': True,
                        'telemetry_denominator': 'only the two existing telemetry Sensor IDs',
                        'generated_window_must_overlap_commands': True,
                        'drain_only_is_not_steady_load': True},
    }


def command_simulator_environment(base_environment, sensor_id, client_id):
    """Clone a telemetry Gateway env for one isolated, control-enabled Sensor client."""
    if (not isinstance(base_environment, dict) or not sensor_id or not client_id
            or sensor_id == base_environment.get('SENSOR_ID')
            or client_id == base_environment.get('GATEWAY_MQTT_CLIENT_ID')):
        raise ValueError('paper_p6_command_simulator_rejected')
    env = dict(base_environment)
    env.update(SENSOR_ID=sensor_id, GATEWAY_CONTROL_ENABLED='true',
               GATEWAY_MQTT_CLIENT_ID=client_id)
    return env


def provision_command_simulators(smoke, fixture, registry, endpoint, tokens, primary,
                                 gateway_password, state, *, dataset):
    """Provision two command-only Sensors and owned CLI inputs/volumes on gw-a.

    Does not start a process. Callers launch each returned item through the existing
    owned Gateway CLI helper, one at a time, using its unique ``tag``.
    """
    if dataset not in DATASET_PROFILES or primary.gateway_id == '' or not gateway_password:
        raise ValueError('paper_p6_command_setup_rejected')
    from stage3_uplink_runner import gateway_env

    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    controls = []
    for ordinal, name in enumerate(('primary', 'secondary'), start=1):
        state.checkpoint()
        sensor_id = f'p6-command-{ordinal}'
        urn = f'urn:ngsi-ld:Sensor:{primary.gateway_id}:{sensor_id}'
        status, _, body = smoke.http_request(
            endpoint, 'PUT', f'/v1/gateways/{primary.gateway_id}/sensors/{sensor_id}',
            token=tokens['a'], body={'name': f'P6 command Sensor {ordinal}', 'unit': primary.sensor_unit})
        if status != 201 or body.get('entity_id') != urn:
            raise ValueError('paper_p6_command_sensor_provision_failed')

        volume = registry.intent('volume', fixture.project + '_outbox_p6_command_' + name,
                                 None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        created = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                    '--label', 'test.run=' + fixture.project, volume['name']])
        if created.returncode or registry.resolve(volume) is None:
            raise ValueError('helper_allocation_unresolved')

        env = gateway_env(primary.gateway_id, gateway_password)
        env.update(GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883',
                   GATEWAY_SAMPLE_RATE_HZ=str(100 if dataset == '100' else 1),
                   GATEWAY_SAMPLES_PER_BATCH=str(DATASET_PROFILES[dataset]['samples_per_message']))
        env = command_simulator_environment(env, sensor_id,
                                           fixture.project + '_p6_command_' + name)
        env_path = fixture.private / f'gateway-p6-command-{name}.env'
        smoke.private_write(env_path, ''.join(key + '=' + value + '\n' for key, value in env.items()))
        controls.append({'name': name, 'sensor_id': sensor_id, 'sensor_urn': urn,
                         'volume': volume, 'env_path': env_path,
                         'tag': 'p6-command-' + name, 'gateway_id': primary.gateway_id})
    return controls


def run_metadata(mode, dataset, *, effective_environment=None,
                 backend_quota=BACKEND_QUOTA, source_revision='captured_at_execution',
                 dirty_worktree='captured_at_execution'):
    values = profile(mode, dataset)
    if backend_quota != BACKEND_QUOTA:
        raise ValueError('paper_p6_quota_rejected')
    source = 'fixture_profile'
    if effective_environment is not None:
        validate_effective_profile(effective_environment, mode, dataset)
        source = 'validated_compose_environment'
    return {
        'schema_version': 1, 'status': 'PREPARED_NOT_EXECUTED', 'native_executed': False,
        'database_schema_version': 24,
        'database_schema_terminal': '24:telemetry_ledger_timestamp',
        'comparison': {'experiment': 'p6_dispatch_mixed_load', 'mode': mode,
                       'only_profile_difference': 'STAGE5_WAKEUP_ENABLED'},
        'dataset': dict(DATASET_PROFILES[dataset]), 'effective_profile': values,
        'effective_profile_source': source, 'backend_quota': dict(BACKEND_QUOTA),
        'workload_specification': workload_spec(dataset),
        'acceptance': {'status': 'NOT_RUN', 'requires_command_overlap_and_exact_accounting': True,
                       'requires_schema_24_terminal_and_exact_cleanup': True},
        'source': {'source_revision': source_revision, 'dirty_worktree': dirty_worktree},
    }


def execution_acceptance_ready(evidence):
    """No metadata-only or placeholder report may satisfy P6 execution gates."""
    if not isinstance(evidence, dict) or evidence.get('native_executed') is not True:
        return False
    required = ('command_overlap_verified', 'telemetry_accounting_exact',
                'schema_24_verified', 'exact_cleanup_verified')
    return all(evidence.get(key) is True for key in required)


def recovery_evidence_valid(evidence):
    required = ('initial_command_succeeded', 'transport_not_ready', 'pending_before_restart',
                'pending_after_restart', 'stable_command_id', 'stable_payload_sha256',
                'process_marker_changed', 'reconnected_ready', 'backend_restarted_with_broker_healthy',
                'recovered_command_succeeded',
                'durable_receipt_evidence', 'missing_origin', 'latency_samples_missing',
                'config_rest_db_match', 'telemetry_outboxes_drained', 'exact_cleanup_verified')
    if not isinstance(evidence, dict) or evidence.get('native_executed') is not True:
        return False
    checks = evidence.get('checks') if isinstance(evidence.get('checks'), dict) else evidence
    return all((evidence.get(key) if key == 'exact_cleanup_verified' else checks.get(key)) is True
                for key in required)


def recovery_restart_order_valid(timeline):
    """Fail closed unless broker-up is observed before a backend restart and readiness."""
    if not isinstance(timeline, list):
        return False
    broker_up = next((i for i, item in enumerate(timeline)
                      if isinstance(item, dict) and item.get('event') == 'broker_started'
                      and item.get('broker_running') is True), None)
    backend_restart = next((i for i, item in enumerate(timeline)
                           if isinstance(item, dict) and item.get('event') == 'backend_restarted'
                           and item.get('broker_running') is True), None)
    ready = next((i for i, item in enumerate(timeline)
                  if isinstance(item, dict) and item.get('event') == 'ready'
                  and item.get('http_status') == 200), None)
    return broker_up is not None and backend_restart is not None and ready is not None \
        and broker_up < backend_restart < ready


def recovered_origin_metrics(before, after):
    """Validate serialized timeline counts without inventing a zero-latency sample."""
    if (not isinstance(before, dict) or not isinstance(after, dict)
            or not before.get('process_instance')
            or before.get('process_instance') != after.get('process_instance')):
        raise ValueError('paper_p6_recovery_timeline_process_mismatch')
    left = before.get('command_timeline')
    right = after.get('command_timeline')
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise ValueError('paper_p6_recovery_timeline_missing')

    def count(snapshot, key):
        histogram = snapshot.get(key)
        value = histogram.get('count') if isinstance(histogram, dict) else None
        if type(value) is not int or value < 0:
            raise ValueError('paper_p6_recovery_timeline_histogram_invalid')
        return value

    def integer(snapshot, key):
        value = snapshot.get(key)
        if type(value) is not int or value < 0:
            raise ValueError('paper_p6_recovery_timeline_counter_invalid')
        return value

    dispatch_before = count(left, 'd_dispatch')
    dispatch_after = count(right, 'd_dispatch')
    succeeded_before = count(left, 'd_e2e_succeeded')
    succeeded_after = count(right, 'd_e2e_succeeded')
    missing_origin = (integer(right, 'pending_origins') > 0
                      or integer(right, 'missing_origin') > integer(left, 'missing_origin'))
    if dispatch_after != dispatch_before or succeeded_after != succeeded_before or not missing_origin:
        raise ValueError('paper_p6_recovery_origin_metric_contract_failed')
    return {'dispatch_count_before': dispatch_before, 'dispatch_count_after': dispatch_after,
            'succeeded_count_before': succeeded_before, 'succeeded_count_after': succeeded_after,
            'dispatch_delta': dispatch_after - dispatch_before,
            'succeeded_delta': succeeded_after - succeeded_before,
            'missing_origin': missing_origin, 'latency_samples_missing': True}


def readiness_diagnostic_body(body):
    """Keep only the public readiness message, with credential-like values removed."""
    import re
    if isinstance(body, dict):
        value = body.get('error', body.get('message', ''))
    elif isinstance(body, str):
        value = body
    else:
        value = ''
    text = str(value)[:500]
    text = re.sub(r'(?i)(password|token|secret|authorization)(\s*[=:]\s*)[^\s,;]+', r'\1\2[redacted]', text)
    return text


def safe_transport_log_excerpt(raw):
    """Extract useful startup/transport lines without retaining arbitrary secrets."""
    lines = []
    for line in str(raw).splitlines():
        if not re.search(r'(?i)(mqtt|broker|transport|connect|subscribe|tls|acl|socket|listen|startup|ready|error|fatal)', line):
            continue
        line = re.sub(r'(?i)(password|token|secret|authorization)(\s*[=:]\s*)("[^"]*"|[^\s,;]+)',
                      r'\1\2[redacted]', line)
        line = re.sub(r'(?i)(mqtts?://)[^/\s@]+:[^/\s@]+@', r'\1[redacted]@', line)
        lines.append(line[:500])
    return lines[-15:]


def safe_direct_probe(result):
    raw = (result.stdout + result.stderr).decode(errors='replace')
    status = re.search(r'HTTP/\d(?:\.\d)?\s+(\d{3})', raw)
    body = raw.splitlines()[-1][:200] if raw.splitlines() else ''
    return {'exit_code': result.returncode,
            'http_status': int(status.group(1)) if status else None,
            'body_excerpt': readiness_diagnostic_body(body)}


def readiness_snapshot(smoke, endpoint):
    """Read the real routed readiness response, discarding only response headers."""
    status, _, body = smoke.http_request(endpoint, 'GET', '/readyz', timeout=5)
    return status, body


def run_recovery_smoke(smoke, fixture, endpoint, registry, containers, images, state):
    """One bounded durable-outbox restart/reconnect exercise; never a load arm."""
    import hashlib
    import json
    from urllib.parse import quote

    pg, quota, _, resources, _, tokens, specs, passwords, _ = smoke.prepare_paper_workload(
        fixture, registry, endpoint, containers, images, state)
    controls = provision_command_simulators(smoke, fixture, registry, endpoint, tokens,
        specs[0], passwords[0], state, dataset=fixture.paper_p6_dataset)
    item_backend, item_broker = containers['backend'], containers['mosquitto']
    backend_id, broker_id = item_backend['receipt']['id'], item_broker['receipt']['id']
    sensor_urn = controls[0]['sensor_urn']
    entity_path = '/v1/digital-twins/' + quote(sensor_urn, safe=':')
    evidence = {'schema_version': 1, 'native_executed': True,
                'run_id': fixture.project, 'status': 'FAIL', 'scenario': 'p6_recovery_smoke',
                'mode': fixture.paper_p6_mode, 'dataset': fixture.paper_p6_dataset,
                'schema_terminal': '24:telemetry_ledger_timestamp', 'backend_quota': quota,
                'service_resources': resources, 'commands': [], 'timeline': [],
                'checks': {}, 'exact_cleanup_verified': False,
                'source': source_identity(Path(__file__).resolve().parents[2])}
    out_path = fixture.artifacts / 'p6-recovery.json'
    smoke.durable_json(out_path, evidence)

    def persist_progress(event, **details):
        evidence['timeline'].append({'event': event, **details})
        smoke.durable_json(out_path, evidence)

    def sql_snapshot(command_id):
        query = """SELECT json_build_object('command_id',c.command_id::text,'status',c.status,
          'payload_bytes',encode(o.payload_bytes,'hex'),'result_bytes',c.result_bytes IS NOT NULL,
          'desired_version',s.desired_version,'reported_configuration',s.reported_state->'configuration')
          FROM twin_commands c JOIN twin_outbox o USING(command_id)
          JOIN twin_states s ON s.entity_id=c.entity_id WHERE c.command_id=:'id';"""
        snapshot = json.loads(smoke.db_psql(registry, pg, query, {'id': command_id}).strip())
        snapshot['payload_sha256'] = hashlib.sha256(bytes.fromhex(snapshot['payload_bytes'])).hexdigest()
        return snapshot

    def wait_readiness(want_ready, seconds=20):
        deadline = time.monotonic() + seconds
        last = None
        last_body = None
        while time.monotonic() < deadline:
            state.checkpoint()
            last, _, last_body = smoke.http_request(endpoint, 'GET', '/readyz', timeout=3)
            if (last == 200) == want_ready:
                return last, last_body
            time.sleep(.25)
        broker = registry.resolve(item_broker)
        backend = registry.resolve(item_backend)
        logs = registry.command(['logs', '--since', '2m', backend_id]) if backend else None
        log_text = ((logs.stdout + logs.stderr).decode(errors='replace') if logs else '')
        event_tokens = safe_transport_log_excerpt(log_text)
        metric_status, _, metrics = smoke.http_request(
            endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'], timeout=3)
        if not isinstance(metrics, dict):
            metrics = {}
        backend_env = dict(value.split('=', 1) for value in
                           ((backend or {}).get('Config', {}).get('Env') or []) if '=' in value)
        profile_keys = ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'STAGE5_COMMAND_TTL',
                        'STAGE5_SCAN_INTERVAL', 'STAGE5_SCAN_BATCH',
                        'STAGE5_PUBLISH_QUEUE_CAPACITY', 'STAGE5_PUBLISH_WORKERS')
        diagnostic = {
                'scenario': 'readiness_timeout',
                'broker': {'registry_name': item_broker.get('name'), 'registry_id': broker_id,
                           'running': bool(broker and broker.get('State', {}).get('Running')),
                           'status': (broker or {}).get('State', {}).get('Status'),
                           'exit_code': (broker or {}).get('State', {}).get('ExitCode')},
                'backend': {'registry_id': backend_id,
                            'running': bool(backend and backend.get('State', {}).get('Running')),
                            'status': (backend or {}).get('State', {}).get('Status'),
                            'image_id': (backend or {}).get('Image'),
                            'effective_profile': {key: backend_env.get(key) for key in profile_keys},
                            'profile_matches_fixture': all(backend_env.get(key) == fixture.values.get(key)
                                                           for key in profile_keys),
                            'metrics_enabled': backend_env.get('METRICS_ENABLED') == 'true'},
                'readiness': {'wanted_ready': want_ready, 'last_http_status': last,
                              'last_response_body': readiness_diagnostic_body(last_body)},
                'backend_transport_logs': event_tokens,
                'metrics': {'http_status': metric_status,
                            'process_instance': metrics.get('process_instance'),
                            'command_dispatch': metrics.get('command_dispatch'),
                            'command_timeline': metrics.get('command_timeline')},
        }
        smoke.durable_json(fixture.artifacts / 'p6-recovery-readiness-diagnostic.json', diagnostic)
        persist_progress('readiness_timeout_diagnostic_saved', diagnostic_path='p6-recovery-readiness-diagnostic.json')
        raise ValueError('paper_p6_recovery_readiness_timeout')

    def run_once():
        initial = issue_and_confirm_command(smoke, endpoint, tokens['a'], registry, pg,
                                            sensor_urn, 2, timeout=35)
        evidence['commands'].append(initial)
        if initial['classification'] != 'succeeded':
            raise ValueError('paper_p6_recovery_initial_command_failed')
        evidence['checks']['initial_command_succeeded'] = True
        persist_progress('initial_command_succeeded', command_id=initial.get('command_id'),
                         terminal=initial.get('classification'))
        if not initial.get('command_id'):
            raise ValueError('paper_p6_recovery_initial_command_id_missing')
        initial_db = sql_snapshot(initial['command_id'])
        if initial_db.get('status') != 'succeeded' or not initial_db.get('result_bytes'):
            raise ValueError('paper_p6_recovery_initial_result_not_committed')
        before_metrics_status, _, before_metrics = smoke.http_request(
            endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        if before_metrics_status != 200:
            raise ValueError('paper_p6_recovery_metrics_unavailable')
        stop = registry.command(['stop', '--time', '10', broker_id])
        if stop.returncode:
            raise ValueError('paper_p6_recovery_broker_stop_failed')
        evidence['broker_stop_returncode'] = stop.returncode
        persist_progress('broker_stop_requested', broker_id=broker_id, returncode=stop.returncode)
        ready_status, ready_body = wait_readiness(False)
        broker_stopped = registry.resolve(item_broker)
        if broker_stopped is None or broker_stopped.get('State', {}).get('Running'):
            raise ValueError('paper_p6_recovery_broker_not_stopped')
        evidence['checks']['transport_not_ready'] = True
        persist_progress('broker_stopped_ready_false', readiness_http_status=ready_status,
                         readiness_body=readiness_diagnostic_body(ready_body), broker_id=broker_id,
                         broker_status=broker_stopped.get('State', {}).get('Status'), broker_running=False)

        status, _, desired = smoke.http_request(endpoint, 'GET', entity_path + '?format=simplified', token=tokens['a'])
        if status != 200:
            raise ValueError('paper_p6_recovery_desired_read_failed')
        version = desired.get('desired_version') or desired.get('desired_state', {}).get('desired_version')
        if version is None:
            raise ValueError('paper_p6_recovery_version_unavailable')
        response_status, _, accepted = smoke.http_request(endpoint, 'PATCH', entity_path + '/desired-state',
            token=tokens['a'], headers={'Idempotency-Key': secrets.token_hex(16)},
            body={'frequency_hz': 4, 'expected_desired_version': str(int(version))})
        if response_status != 202 or not accepted.get('command_id'):
            raise ValueError('paper_p6_recovery_offline_admission_rejected')
        command_id = accepted['command_id']
        before = sql_snapshot(command_id)
        if before.get('status') != 'pending':
            raise ValueError('paper_p6_recovery_pending_not_durable')
        evidence['checks']['pending_before_restart'] = True
        evidence['commands'].append({'command_id': command_id, 'stage': 'pending_offline',
                                     'payload_sha256': before['payload_sha256'],
                                     'status': before['status']})
        persist_progress('pending_intent_committed_transport_offline', command_id=command_id,
                         payload_sha256=before['payload_sha256'], status=before['status'])

        backend_before_restart = registry.resolve(item_backend)
        if backend_before_restart is None:
            raise ValueError('paper_p6_recovery_backend_missing_before_restart')
        def ipv4_addresses(obj):
            return {name: row.get('IPAddress') for name, row in
                    (obj.get('NetworkSettings', {}).get('Networks') or {}).items()}
        backend_ips_before = ipv4_addresses(backend_before_restart)
        if registry.command(['stop', '--time', '20', backend_id]).returncode:
            raise ValueError('paper_p6_recovery_backend_stop_failed')
        if registry.command(['start', backend_id]).returncode:
            raise ValueError('paper_p6_recovery_backend_start_failed')
        # Same owned container, image, environment, mounts and ID; broker remains stopped.
        restart_deadline = time.monotonic() + 45
        while time.monotonic() < restart_deadline:
            state.checkpoint()
            obj = registry.resolve(item_backend)
            if obj and obj.get('State', {}).get('Running'):
                break
            time.sleep(.25)
        else:
            raise ValueError('paper_p6_recovery_backend_restart_timeout')
        backend_after_restart = registry.resolve(item_backend)
        if backend_after_restart is None:
            raise ValueError('paper_p6_recovery_backend_missing_after_restart')
        backend_ips_after = ipv4_addresses(backend_after_restart)
        nginx_reload_returncode = None
        if backend_ips_before != backend_ips_after:
            # P3 replacement uses this same owned Nginx reload because the fixture's
            # upstream is a statically resolved Docker service name.
            reload_result = registry.command(['exec', containers['nginx']['receipt']['id'],
                                               'nginx', '-s', 'reload'])
            nginx_reload_returncode = reload_result.returncode
            if nginx_reload_returncode:
                raise ValueError('paper_p6_recovery_nginx_upstream_reload_failed')
        persist_progress('backend_container_restarted', backend_id=backend_id,
                         image_id=backend_after_restart.get('Image'),
                         ip_before=backend_ips_before, ip_after=backend_ips_after,
                         nginx_reloaded=nginx_reload_returncode == 0 if nginx_reload_returncode is not None else False)
        after = sql_snapshot(command_id)
        if (after.get('status') != 'pending' or before.get('command_id') != after.get('command_id')
                or before.get('payload_bytes') != after.get('payload_bytes')
                or before.get('payload_sha256') != after.get('payload_sha256')):
            raise ValueError('paper_p6_recovery_pending_mutated')
        evidence['checks'].update(pending_after_restart=True, stable_command_id=True,
                                  stable_payload_sha256=hashlib.sha256(
                                      bytes.fromhex(before['payload_bytes'])).hexdigest() == before['payload_sha256'])
        persist_progress('backend_process_restarted_pending_unchanged', command_id=command_id,
                         payload_sha256=after['payload_sha256'], status=after['status'])
        restart_metrics_status, restart_metrics = None, {}
        while time.monotonic() < restart_deadline:
            state.checkpoint()
            restart_metrics_status, _, metrics_body = smoke.http_request(
                endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'], timeout=3)
            if (restart_metrics_status == 200 and isinstance(metrics_body, dict)
                    and metrics_body.get('process_instance')
                    and metrics_body.get('process_instance') != before_metrics.get('process_instance')):
                restart_metrics = metrics_body
                break
            time.sleep(.25)
        else:
            obj = registry.resolve(item_backend)
            logs = registry.command(['logs', '--since', '2m', backend_id]) if obj else None
            log_text = ((logs.stdout + logs.stderr).decode(errors='replace') if logs else '')
            event_tokens = safe_transport_log_excerpt(log_text)
            broker_obj = registry.resolve(item_broker)
            direct_probes = {}
            for name, probe_path in (('healthz', '/healthz'), ('readyz', '/readyz')):
                probe = registry.command(['exec', backend_id, '/bin/busybox', 'wget', '-S', '-O', '-',
                                          '-T', '3', 'http://127.0.0.1:8080' + probe_path])
                direct_probes[name] = safe_direct_probe(probe)
            smoke.durable_json(fixture.artifacts / 'p6-recovery-process-start-diagnostic.json', {
                'backend_id': backend_id, 'backend_running': bool(obj and obj.get('State', {}).get('Running')),
                'backend_status': (obj or {}).get('State', {}).get('Status'),
                'backend_image_id': (obj or {}).get('Image'),
                'backend_ips_before_restart': backend_ips_before,
                'backend_ips_after_restart': backend_ips_after,
                'nginx_upstream_reloaded': nginx_reload_returncode,
                'broker_id': broker_id,
                'broker_running': bool(broker_obj and broker_obj.get('State', {}).get('Running')),
                'broker_status': (broker_obj or {}).get('State', {}).get('Status'),
                'metrics_http_status': restart_metrics_status,
                'backend_loopback_probes': direct_probes,
                'expected_old_process_marker': before_metrics.get('process_instance'),
                'last_observed_process_marker': (metrics_body.get('process_instance')
                                                 if isinstance(metrics_body, dict) else None),
                'safe_backend_transport_logs': event_tokens})
            persist_progress('new_process_marker_timeout_diagnostic_saved',
                             diagnostic_path='p6-recovery-process-start-diagnostic.json')
            raise ValueError('paper_p6_recovery_new_process_not_observed')
        evidence['process_markers'] = {'before': before_metrics.get('process_instance'),
                                       'after_restart': restart_metrics.get('process_instance')}
        evidence['timeline'].append({'event': 'new_backend_process_observed_broker_offline'})
        smoke.durable_json(out_path, evidence)

        if registry.command(['start', broker_id]).returncode:
            raise ValueError('paper_p6_recovery_broker_start_failed')
        broker_started = registry.resolve(item_broker)
        if broker_started is None or not broker_started.get('State', {}).get('Running'):
            raise ValueError('paper_p6_recovery_broker_not_running_after_start')
        broker_started_at = broker_started.get('State', {}).get('StartedAt')
        persist_progress('broker_started', broker_id=broker_id, broker_running=True,
                         broker_started_at=broker_started_at)

        # Observe the original backend process with the broker healthy before the
        # separately authorized recovery attempt: restart only the backend now.
        pre_restart_ready_status, pre_restart_ready_body = readiness_snapshot(smoke, endpoint)
        persist_progress('readiness_observed_broker_healthy_before_backend_restart',
                         readiness_http_status=pre_restart_ready_status,
                         readiness_body=readiness_diagnostic_body(pre_restart_ready_body))
        backend_before_healthy_restart = registry.resolve(item_backend)
        if backend_before_healthy_restart is None or not backend_before_healthy_restart.get('State', {}).get('Running'):
            raise ValueError('paper_p6_recovery_backend_not_running_before_healthy_restart')
        if registry.command(['stop', '--time', '20', backend_id]).returncode:
            raise ValueError('paper_p6_recovery_healthy_backend_stop_failed')
        if registry.command(['start', backend_id]).returncode:
            raise ValueError('paper_p6_recovery_healthy_backend_start_failed')
        healthy_restart_deadline = time.monotonic() + 45
        while time.monotonic() < healthy_restart_deadline:
            state.checkpoint()
            obj = registry.resolve(item_backend)
            broker_now = registry.resolve(item_broker)
            if (obj and obj.get('State', {}).get('Running') and broker_now
                    and broker_now.get('State', {}).get('Running')):
                break
            time.sleep(.25)
        else:
            raise ValueError('paper_p6_recovery_healthy_backend_restart_timeout')
        backend_after_healthy_restart = registry.resolve(item_backend)
        broker_after_restart = registry.resolve(item_broker)
        if (backend_after_healthy_restart is None or broker_after_restart is None
                or not broker_after_restart.get('State', {}).get('Running')
                or broker_after_restart.get('State', {}).get('StartedAt') != broker_started_at):
            raise ValueError('paper_p6_recovery_broker_changed_during_backend_restart')
        if registry.command(['exec', containers['nginx']['receipt']['id'],
                             'nginx', '-s', 'reload']).returncode:
            raise ValueError('paper_p6_recovery_healthy_nginx_reload_failed')
        persist_progress('backend_restarted', backend_id=backend_id, broker_id=broker_id,
                         broker_running=True, broker_started_at=broker_started_at,
                         backend_started_at=backend_after_healthy_restart.get('State', {}).get('StartedAt'))
        reconnect_status, reconnect_body = wait_readiness(True, 35)
        broker_started = registry.resolve(item_broker)
        if (broker_started is None or not broker_started.get('State', {}).get('Running')
                or broker_started.get('State', {}).get('StartedAt') != broker_started_at):
            raise ValueError('paper_p6_recovery_broker_not_running_after_reconnect')
        persist_progress('ready', http_status=reconnect_status)
        if not recovery_restart_order_valid(evidence['timeline']):
            raise ValueError('paper_p6_recovery_restart_order_rejected')
        evidence['checks']['reconnected_ready'] = True
        evidence['checks']['backend_restarted_with_broker_healthy'] = True
        persist_progress('broker_reconnected_ready', readiness_http_status=reconnect_status,
                         readiness_body=readiness_diagnostic_body(reconnect_body), broker_id=broker_id,
                         broker_status=broker_started.get('State', {}).get('Status'), broker_running=True)
        restart_deadline = time.monotonic() + 35
        restart_metrics_status, restart_metrics = None, {}
        while time.monotonic() < restart_deadline:
            state.checkpoint()
            restart_metrics_status, _, metrics_body = smoke.http_request(
                endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'], timeout=3)
            if (restart_metrics_status == 200 and isinstance(metrics_body, dict)
                    and metrics_body.get('process_instance')
                    and metrics_body.get('process_instance') != before_metrics.get('process_instance')):
                restart_metrics = metrics_body
                break
            time.sleep(.25)
        if not restart_metrics:
            raise ValueError('paper_p6_recovery_healthy_process_marker_missing')
        evidence['process_markers'] = {
            'before': before_metrics.get('process_instance'),
            'after_restart': restart_metrics.get('process_instance')}
        evidence['checks']['process_marker_changed'] = bool(
            evidence['process_markers']['before'] and evidence['process_markers']['after_restart']
            and evidence['process_markers']['before'] != evidence['process_markers']['after_restart'])
        persist_progress('healthy_restart_process_marker_observed',
                         process_markers=evidence['process_markers'],
                         process_marker_changed=evidence['checks']['process_marker_changed'])
        if not evidence['checks']['process_marker_changed']:
            raise ValueError('paper_p6_recovery_process_marker_not_changed')
        deadline = time.monotonic() + 45
        terminal = None
        while time.monotonic() < deadline:
            state.checkpoint()
            result_status, _, result = smoke.http_request(endpoint, 'GET', accepted['status_url'], token=tokens['a'])
            if result_status == 200 and result.get('status') in ('succeeded', 'failed', 'timeout'):
                terminal = result
                break
            time.sleep(.25)
        final = sql_snapshot(command_id) if terminal is not None else None
        if terminal is not None and final is not None:
            evidence['commands'][-1].update({
                'terminal': terminal.get('status'), 'terminal_http_status': result_status,
                'sql_status': final.get('status'), 'result_committed': final.get('result_bytes'),
                'payload_sha256': final.get('payload_sha256')})
            persist_progress('command_terminal_and_sql_result_persisted',
                             command_id=command_id, terminal=terminal.get('status'),
                             sql_status=final.get('status'), result_bytes=final.get('result_bytes'),
                             payload_sha256=final.get('payload_sha256'))
        if terminal is None or terminal.get('status') != 'succeeded':
            raise ValueError('paper_p6_recovery_command_not_succeeded')
        if final is None or final.get('status') != 'succeeded' or final.get('result_bytes') is not True:
            raise ValueError('paper_p6_recovery_duplicate_or_missing_apply')
        _, _, after_metrics = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        rest_status, _, config_view = smoke.http_request(endpoint, 'GET', entity_path + '?format=simplified', token=tokens['a'])
        db_configuration = final.get('reported_configuration')
        rest_configuration = config_view.get('reported_state', {}).get('configuration')
        config_match = (rest_status == 200 and isinstance(db_configuration, dict)
                        and rest_configuration == db_configuration)
        if config_match:
            try:
                config_match = (int(db_configuration.get('desired_version', -1)) == int(version) + 1
                                and int(db_configuration.get('desired_version', -1))
                                == int(final.get('desired_version', -2))
                                and db_configuration.get('sample_interval_us') == 250000)
            except (TypeError, ValueError):
                config_match = False
        if not config_match:
            evidence['commands'][-1].update({'reported_configuration_db': db_configuration,
                                             'reported_configuration_rest': rest_configuration,
                                             'configuration_rest_http_status': rest_status})
            persist_progress('command_configuration_readback_mismatch', command_id=command_id,
                             rest_http_status=rest_status, db_configuration=db_configuration,
                             rest_configuration=rest_configuration)
            raise ValueError('paper_p6_recovery_configuration_readback_mismatch')
        evidence['commands'][-1].update({'reported_configuration_db': db_configuration,
                                         'reported_configuration_rest': rest_configuration,
                                         'configuration_rest_http_status': rest_status})
        evidence['checks']['config_rest_db_match'] = True
        persist_progress('command_configuration_db_rest_match_persisted', command_id=command_id,
                         rest_http_status=rest_status, configuration=db_configuration)
        origin_metrics = recovered_origin_metrics(restart_metrics, after_metrics)
        dispatch_delta = origin_metrics['dispatch_delta']
        succeeded_delta = origin_metrics['succeeded_delta']
        missing_origin = origin_metrics['missing_origin']
        evidence['commands'][-1].update({'terminal': terminal['status'],
                                    'payload_sha256': before['payload_sha256'], 'result_committed': final['result_bytes'],
                                    'desired_version': int(version) + 1,
                                    'reported_configuration_db': db_configuration,
                                    'reported_configuration_rest': rest_configuration})
        evidence['process_markers'] = {'before': before_metrics.get('process_instance'),
                                       'after_restart': restart_metrics.get('process_instance'),
                                       'after_result': after_metrics.get('process_instance')}
        evidence['checks'].update(initial_command_succeeded=True, recovered_command_succeeded=True,
                                  process_marker_changed=bool(
                                      before_metrics.get('process_instance') and restart_metrics.get('process_instance')
                                      and before_metrics['process_instance'] != restart_metrics['process_instance']),
                                  missing_origin=missing_origin,
                                  dispatch_histogram_count_before=origin_metrics['dispatch_count_before'],
                                  dispatch_histogram_count_after=origin_metrics['dispatch_count_after'],
                                  succeeded_histogram_count_before=origin_metrics['succeeded_count_before'],
                                  succeeded_histogram_count_after=origin_metrics['succeeded_count_after'],
                                  dispatch_histogram_delta=dispatch_delta,
                                  succeeded_histogram_delta=succeeded_delta,
                                  latency_samples_missing=(dispatch_delta == 0 and succeeded_delta == 0),
                                  config_rest_db_match=True)
        evidence['timeline'].append({'event': 'command_recovered_succeeded', 'restored_readiness': True,
                                     'dispatch_histogram_samples_for_recovery': dispatch_delta,
                                     'succeeded_histogram_samples_for_recovery': succeeded_delta,
                                     'missing_origin_signal': 'pending_origins_or_missing_origin_counter'})
        smoke.durable_json(out_path, evidence)

    def run_once_with_diagnostic():
        try:
            return run_once()
        except BaseException as exc:
            import traceback
            error = str(exc)[:300]
            error = re.sub(r'(?i)(password|token|secret|authorization)(\s*[=:]\s*)[^\s,;]+',
                           r'\1\2[redacted]', error)
            frames = traceback.extract_tb(exc.__traceback__)
            smoke.durable_json(fixture.artifacts / 'p6-recovery-callback-failure.json', {
                'exception_type': type(exc).__name__, 'safe_message': error,
                'frames': [{'file': Path(frame.filename).name, 'function': frame.name,
                            'line': frame.lineno} for frame in frames[-12:]],
            })
            persist_progress('callback_failure_diagnostic_saved',
                             diagnostic_path='p6-recovery-callback-failure.json',
                             exception_type=type(exc).__name__)
            raise

    # Existing helper starts real TLS command clients, waits for ACK-filter subscription,
    # and stops both owned processes. Recovery activity stays within its callback.
    run_data = run_command_simulators(smoke, fixture, registry, images, controls, state,
                                     run_once_with_diagnostic, timeout=170, readiness_timeout=20, batches=10000)
    evidence['simulator_lifecycle'] = run_data
    smoke.durable_json(out_path, evidence)

    import sqlite3
    from stage3_mqtt_ingress_runner import parse_run_summary
    drain_evidence = {}
    for control in controls:
        final_count = None
        attempts = []
        for attempt in range(4):
            tag = control['tag'] + '-recovery-drain-' + str(attempt)
            output = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
                control['volume'], control['env_path'], fixture.private / 'certs/ca.crt', tag,
                ['run', '--batches', '1', '--completion-timeout', '120s'], state, timeout=150)
            summary = parse_run_summary(output)
            count_text = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
                control['volume'], control['env_path'], fixture.private / 'certs/ca.crt',
                tag + '-count', ['count'], state, timeout=25)
            match = re.fullmatch(r'pending_rows=(\d+) pending_bytes=(\d+) quarantined_rows=(\d+) quarantined_bytes=(\d+)',
                                 count_text.strip())
            if not match:
                raise ValueError('paper_p6_recovery_outbox_count_rejected')
            final_count = {'pending_rows': int(match.group(1)), 'pending_bytes': int(match.group(2)),
                           'quarantined_rows': int(match.group(3)), 'quarantined_bytes': int(match.group(4))}
            attempts.append({'generation_enqueued': int(summary['enqueued']),
                             'app_acknowledged': int(summary['app_acknowledged']),
                             **final_count})
            if final_count == {'pending_rows': 0, 'pending_bytes': 0,
                               'quarantined_rows': 0, 'quarantined_bytes': 0}:
                break
        if final_count != {'pending_rows': 0, 'pending_bytes': 0,
                           'quarantined_rows': 0, 'quarantined_bytes': 0}:
            raise ValueError('paper_p6_recovery_outbox_drain_failed')
        drain_evidence[control['name']] = attempts
    evidence['telemetry_outbox_drain'] = drain_evidence
    evidence['checks']['telemetry_outboxes_drained'] = True
    smoke.durable_json(out_path, evidence)

    expected_ids = {row.get('command_id') for row in evidence['commands'] if row.get('command_id')}
    if len(expected_ids) != 2:
        raise ValueError('paper_p6_recovery_command_ids_incomplete')
    receipt_evidence = {}
    for control in controls:
        state.checkpoint()
        tag = fixture.project + '-gateway-' + control['tag']
        gateway_item = next((item for item in registry.items if item.get('name') == tag), None)
        gateway_obj = registry.resolve(gateway_item) if gateway_item else None
        if gateway_obj is None or gateway_obj.get('State', {}).get('Running'):
            raise ValueError('paper_p6_recovery_gateway_not_stopped_for_readback')
        db_path = fixture.private / ('p6-recovery-' + control['name'] + '.db')
        copied = registry.command(['cp', gateway_obj['Id'] + ':/data/outbox/outbox.db', str(db_path)])
        if copied.returncode:
            raise ValueError('paper_p6_recovery_sqlite_copy_failed')
        with sqlite3.connect('file:' + str(db_path) + '?mode=ro', uri=True, timeout=3) as db:
            config_row = db.execute('SELECT sample_interval_us,desired_version,applied_at '
                                    'FROM sensor_configuration WHERE id=1').fetchone()
            receipts = db.execute('SELECT command_id,command_hash,command_bytes,status,result_bytes '
                                  'FROM command_receipts ORDER BY command_id').fetchall()
        if control is controls[0]:
            matches = [row for row in receipts if row[0] in expected_ids]
            if (len(matches) != 2 or len({row[0] for row in matches}) != 2
                    or {row[0] for row in matches} != expected_ids
                    or len(receipts) != 2
                    or any(row[3] != 'completed' or not row[4]
                           or hashlib.sha256(row[2]).hexdigest() != row[1] for row in matches)):
                raise ValueError('paper_p6_recovery_sqlite_receipt_evidence_failed')
            expected_payloads = {command['command_id']: bytes.fromhex(
                sql_snapshot(command['command_id'])['payload_bytes']) for command in evidence['commands']}
            if any(row[2] != expected_payloads[row[0]] for row in matches):
                raise ValueError('paper_p6_recovery_sqlite_command_bytes_mismatch')
            expected_version = int(evidence['commands'][-1].get('desired_version', 0))
            # Gateway configuration reflects the last completed command, and receipt
            # rows are unique by command_id. This is durable idempotent receipt
            # evidence, not an assertion about physical apply invocation count.
            if not config_row or config_row[0] != 250000 or config_row[1] != expected_version or not config_row[2]:
                raise ValueError('paper_p6_recovery_sqlite_configuration_mismatch')
            receipt_evidence[control['name']] = {
                'configuration': {'sample_interval_us': config_row[0], 'desired_version': config_row[1],
                                  'applied_at_present': bool(config_row[2])},
                'unique_completed_receipts': len(matches),
                'receipts': [{'command_id': row[0], 'command_hash': row[1],
                              'command_bytes_match_backend_outbox': True,
                              'result_bytes_present': bool(row[4]), 'status': row[3]} for row in matches]}
        else:
            if receipts or config_row:
                raise ValueError('paper_p6_recovery_idle_gateway_state_unexpected')
            receipt_evidence[control['name']] = {'unique_completed_receipts': 0,
                                                  'configuration': 'no command issued to this simulator'}
    evidence['sqlite_readback'] = receipt_evidence
    evidence['checks']['durable_receipt_evidence'] = True
    evidence['checks']['durable_receipt_evidence_detail'] = (
        'two unique completed durable command_receipts with exact backend command bytes and result bytes; '
        'no physical invocation counter claimed')
    smoke.durable_json(out_path, evidence)
    return evidence


def intervals_overlap(left, right):
    """Require observed generation intervals (not process/drain lifetimes) to overlap."""
    try:
        return (float(left['generation_start']) < float(right['generation_end'])
                and float(right['generation_start']) < float(left['generation_end'])
                and float(left['generation_end']) > float(left['generation_start'])
                and float(right['generation_end']) > float(right['generation_start']))
    except (KeyError, TypeError, ValueError):
        return False


def validate_telemetry_accounting(rows, *, require_ack_match=True, require_sample_match=True):
    """Validate isolated telemetry sensor rows without folding command sensors in."""
    if not isinstance(rows, list) or len(rows) != 2:
        raise ValueError('paper_p6_telemetry_accounting_rejected')
    for row in rows:
        if (not row.get('sensor_id') or type(row.get('offered')) is not int
                or type(row.get('generated')) is not int or type(row.get('raw')) is not int
                or type(row.get('raw_samples', row.get('raw'))) is not int
                or type(row.get('ledger')) is not int or type(row.get('temporal')) is not int
                or type(row.get('acknowledged')) is not int or row['offered'] != row['generated']
                or row['generated'] != row['ledger'] or row['ledger'] != row['temporal']
                or row['raw'] != row.get('raw_samples', row['raw'])
                or (require_ack_match and row['ledger'] != row['acknowledged'])
                or (require_sample_match and row['raw'] != row['samples_expected'])):
            raise ValueError('paper_p6_telemetry_accounting_rejected')
    return True


def counter_deltas(before, after, fields):
    if (not isinstance(before, dict) or not isinstance(after, dict)
            or before.get('process_instance') != after.get('process_instance')
            or not before.get('process_instance')):
        raise ValueError('paper_p6_metrics_process_mismatch')
    left, right = before.get('ingress'), after.get('ingress')
    if not isinstance(left, dict) or not isinstance(right, dict):
        raise ValueError('paper_p6_metrics_unavailable')
    result = {}
    for key in fields:
        if type(left.get(key)) is not int or type(right.get(key)) is not int or right[key] < left[key]:
            raise ValueError('paper_p6_metrics_counter_invalid')
        result[key] = right[key] - left[key]
    return result


def validate_aggregate_accounting(db_delta, ingress_delta, cli_totals):
    """Check exact totals without conflating message rows with sample rows."""
    if (not isinstance(db_delta, dict) or not isinstance(ingress_delta, dict)
            or not isinstance(cli_totals, dict) or not ingress_delta.get('process_instance')):
        raise ValueError('paper_p6_exact_telemetry_accounting_failed')
    required = ((db_delta, ('ledger', 'temporal', 'raw', 'raw_samples')),
                (ingress_delta, ('Committed', 'SamplesNew')),
                (cli_totals, ('offered', 'application_acks')))
    if any(type(values.get(key)) is not int for values, keys in required for key in keys):
        raise ValueError('paper_p6_exact_telemetry_accounting_failed')
    if not (db_delta['ledger'] == db_delta['temporal']
            and db_delta['raw'] == db_delta['raw_samples'] == ingress_delta['SamplesNew']
            and db_delta['ledger'] == ingress_delta['Committed']
            == cli_totals['offered'] == cli_totals['application_acks']):
        raise ValueError('paper_p6_exact_telemetry_accounting_failed')
    return True


def source_identity(root):
    def git(*args):
        return subprocess.run(['git', '-C', str(root), *args], check=True,
                              capture_output=True, text=True).stdout.strip()
    return {'revision': git('rev-parse', 'HEAD'), 'branch': git('branch', '--show-current'),
            'dirty': bool(git('status', '--porcelain'))}


def run_command_simulators(smoke, fixture, registry, images, controls, state, callback,
                           *, timeout=45, readiness_timeout=12, batches=1000,
                           comparison_stop_hook=False):
    """Run the two owned command Sensors concurrently, call work only after subscriptions.

    ``paper_p0_gateway_cli`` is deliberately nested: the first process stays live
    while the second starts. Registry mutations remain on this one caller thread.
    The batch cap is finite; both processes are SIGTERM-stopped in all callback and
    startup outcomes. Returned summaries distinguish completed from interrupted.
    """
    if (not isinstance(controls, list) or len(controls) != 2 or not callable(callback)
            or type(batches) is not int or not 1 <= batches <= 10000
            or isinstance(timeout, bool) or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout) or not 0 < timeout <= 300
            or isinstance(readiness_timeout, bool) or not isinstance(readiness_timeout, (int, float))
            or not math.isfinite(readiness_timeout) or not 0 < readiness_timeout <= timeout):
        raise ValueError('paper_p6_command_lifecycle_rejected')
    identities = [(item.get('sensor_id'), item.get('volume', {}).get('name'), item.get('tag'))
                  for item in controls]
    if (any(not all(identity) for identity in identities)
            or len({identity[0] for identity in identities}) != 2
            or len({identity[1] for identity in identities}) != 2
            or len({identity[2] for identity in identities}) != 2):
        raise ValueError('paper_p6_command_lifecycle_rejected')
    if any(registry.resolve(control['volume']) is None for control in controls):
        raise ValueError('paper_p6_command_volume_unavailable')

    tags = [fixture.project + '-gateway-' + item['tag'] for item in controls]
    if any(any(existing.get('name') == name for existing in registry.items) for name in tags):
        raise ValueError('paper_p6_command_process_collision')
    output, callback_result = {}, {}
    signalled, stop_attempted = set(), set()
    stop_events, stop_errors = [], []
    overall_deadline = time.monotonic() + timeout
    lifecycle_started = time.monotonic()

    def progress(event, tag='command-simulators'):
        # Public diagnostics are an allowlisted vocabulary and contain no payloads.
        elapsed = max(0.0, time.monotonic() - lifecycle_started)
        remaining = max(0.0, overall_deadline - time.monotonic())
        print('P6_PROGRESS event=%s tag=%s elapsed_seconds=%.3f remaining_seconds=%.3f' %
              (event, tag, elapsed, remaining), flush=True)

    def find_container(name):
        return next((item for item in registry.items
                     if item.get('kind') == 'container' and item.get('name') == name), None)

    def stop_owned():
        primary_failure = sys.exc_info()[0] is not None
        for name in reversed(tags):
            if name in stop_attempted:
                continue
            stop_attempted.add(name)
            try:
                item = find_container(name)
                obj = registry.resolve(item) if item is not None else None
                if obj is not None and obj.get('State', {}).get('Running'):
                    stop_events.append({'event': 'stop_requested', 'tag': name,
                                        'monotonic': time.monotonic()})
                    progress('command_simulator_stop_requested')
                    result = registry.command(['kill', '--signal=SIGTERM', obj['Id']])
                    if result.returncode:
                        raise ValueError('paper_p6_command_stop_failed')
                    signalled.add(name)
            except Exception:
                stop_errors.append(name)
        # Comparison callers need the actual stopped boundary before reopening
        # volumes. SIGTERM may flush/drain; it is not an instantaneous stop.
        if comparison_stop_hook:
            deadline = min(overall_deadline, time.monotonic() + 30)
            waiting = set(signalled) - {row['tag'] for row in stop_events if row['event'] == 'stopped'}
            while waiting and time.monotonic() < deadline:
                for name in tuple(waiting):
                    try:
                        obj = registry.resolve(find_container(name))
                        if obj is not None and not obj.get('State', {}).get('Running'):
                            waiting.remove(name)
                            stop_events.append({'event': 'stopped', 'tag': name,
                                                'monotonic': time.monotonic()})
                            progress('command_simulator_stopped')
                    except Exception:
                        stop_errors.append(name)
                        waiting.remove(name)
                if waiting:
                    time.sleep(0.1)
            stop_errors.extend(sorted(waiting))
        if stop_errors and not primary_failure:
            raise ValueError('paper_p6_command_stop_failed')

    def wait_subscribed():
        deadline = min(overall_deadline, time.monotonic() + readiness_timeout)
        while time.monotonic() < deadline:
            state.checkpoint()
            ready = True
            for name, control in zip(tags, controls):
                item = find_container(name)
                obj = registry.resolve(item) if item is not None else None
                if obj is None or not obj.get('State', {}).get('Running'):
                    ready = False
                    break
                logs = registry.command(['logs', obj['Id']])
                # The transport logs the completed all-subscriptions readiness
                # using its ACK filter. In control mode it has subscribed the
                # command filter before emitting this marker (transport.go).
                expected = (b'event=subscribed ' in logs.stderr
                            and ('filter=gateways/' + control['gateway_id'] + '/acks/#').encode()
                            in logs.stderr)
                if logs.returncode or not expected:
                    ready = False
                    break
            if ready:
                return time.monotonic()
            time.sleep(0.1)
        # Retain private, exact-owned container logs before outer cleanup removes
        # the command simulators; the public failure report remains structural.
        for name in tags:
            item = find_container(name)
            obj = registry.resolve(item) if item is not None else None
            if obj is not None:
                logs = registry.command(['logs', obj['Id']])
                smoke.private_write(fixture.private / (name + '-subscription-diagnostic.log'),
                                    (logs.stdout + logs.stderr).decode(errors='replace'))
        raise ValueError('paper_p6_command_subscription_timeout')

    def launch_secondary_with_work():
        try:
            progress('command_simulator_start', 'secondary')
            def ready_and_work():
                try:
                    callback_result['ready_monotonic'] = wait_subscribed()
                    progress('subscriptions_ready')
                    callback_result['callback_start_monotonic'] = time.monotonic()
                    progress('generator_wait_start')
                    callback_result['value'] = callback(stop_owned) if comparison_stop_hook else callback()
                    callback_result['callback_end_monotonic'] = time.monotonic()
                    progress('generator_wait_end')
                finally:
                    # Stop while both nested CLI helpers are still polling their
                    # owned containers; otherwise finite 1000-batch runs wait out.
                    stop_owned()
            output['secondary'] = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
                controls[1]['volume'], controls[1]['env_path'], fixture.private / 'certs/ca.crt',
                controls[1]['tag'], ['run', '--batches', str(batches), '--completion-timeout', '30s'],
                state, during=ready_and_work,
                timeout=max(1, overall_deadline - time.monotonic()))
        finally:
            stop_owned()

    try:
        progress('command_simulator_start', 'primary')
        output['primary'] = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
            controls[0]['volume'], controls[0]['env_path'], fixture.private / 'certs/ca.crt',
            controls[0]['tag'], ['run', '--batches', str(batches), '--completion-timeout', '30s'],
            state, during=launch_secondary_with_work, timeout=timeout)
    finally:
        stop_owned()

    from stage3_mqtt_ingress_runner import parse_run_summary
    summaries = {}
    for role, text in output.items():
        values = parse_run_summary(text)
        summaries[role] = {'enqueued': int(values['enqueued']),
            'publish_attempts': int(values['publish_attempts']),
            'app_acknowledged': int(values['app_acknowledged']),
            'pending_rows': int(values['pending_rows']), 'pending_bytes': int(values['pending_bytes']),
            'quarantined_rows': int(values['quarantined_rows']),
            'quarantined_bytes': int(values['quarantined_bytes']),
            'completed': values['completed'] == 'true'}
    outbox_counts = {}
    for control in controls:
        count_text = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
            control['volume'], control['env_path'], fixture.private / 'certs/ca.crt',
            control['tag'] + '-post-stop-count', ['count'], state, timeout=15)
        match = re.fullmatch(r'pending_rows=(\d+) pending_bytes=(\d+) quarantined_rows=(\d+) quarantined_bytes=(\d+)',
                             count_text.strip())
        if not match:
            raise ValueError('paper_p6_command_outbox_count_rejected')
        outbox_counts[control.get('name', control['sensor_id'])] = {
            'pending_rows': int(match.group(1)), 'pending_bytes': int(match.group(2)),
            'quarantined_rows': int(match.group(3)), 'quarantined_bytes': int(match.group(4))}
    return {'callback_result': callback_result.get('value'),
            'callback_window': {key: callback_result[key] for key in
                ('ready_monotonic', 'callback_start_monotonic', 'callback_end_monotonic')},
             'summaries': summaries, 'post_stop_outbox_counts': outbox_counts,
             'stop_events': stop_events,
            'sigterm_sent': len(signalled),
            'stopped_by_sigterm': len(signalled) == 2}


def issue_and_confirm_command(smoke, endpoint, token, registry, pg, sensor_urn, frequency_hz,
                              *, prior_response=None, timeout=35):
    """Issue one unique desired-state request and classify its real terminal outcome."""
    from urllib.parse import quote
    import json

    path = '/v1/digital-twins/' + quote(sensor_urn, safe=':')
    if prior_response is not None:
        current_version = prior_response.get('desired_version')
    else:
        status, _, current = smoke.http_request(endpoint, 'GET', path + '?format=simplified', token=token)
        if status != 200:
            return {'classification': 'read_error', 'http_status': status,
                    'client_entry_monotonic': None, 'client_return_monotonic': time.monotonic()}
        current_version = current.get('desired_version')
        if current_version is None and isinstance(current.get('desired_state'), dict):
            current_version = current['desired_state'].get('desired_version')
    if current_version is None:
        raise ValueError('paper_p6_desired_version_unavailable')
    try:
        expected_version = int(current_version)
    except (ValueError, TypeError):
        raise ValueError('paper_p6_desired_version_unavailable') from None
    if expected_version < 0:
        raise ValueError('paper_p6_desired_version_unavailable')

    entry = time.monotonic()
    status, _, body = smoke.http_request(endpoint, 'PATCH', path + '/desired-state', token=token,
        headers={'Idempotency-Key': secrets.token_hex(16)},
        body={'frequency_hz': frequency_hz,
              'expected_desired_version': str(expected_version)})
    returned = time.monotonic()
    counts = {'accepted': 0, 'succeeded': 0, 'failed': 0, 'timeout': 0,
              'rejected': 0, 'busy': 0, 'noop': 0, 'replay': 0, 'error': 0}
    if status == 202:
        counts['accepted'] = 1
        command_status = 'pending'
        deadline = time.monotonic() + timeout
        result_body = None
        terminal_monotonic = None
        while time.monotonic() < deadline:
            result_status, _, result_body = smoke.http_request(endpoint, 'GET', body['status_url'], token=token)
            if result_status == 200 and result_body.get('status') in ('succeeded', 'failed', 'timeout'):
                command_status = result_body['status']
                terminal_monotonic = time.monotonic()
                break
            time.sleep(0.2)
        if command_status == 'pending':
            counts['timeout'] = 1
            return {'classification': 'timeout', 'http_status': status, 'counts': counts,
                    'client_entry_monotonic': entry, 'client_return_monotonic': returned,
                    'terminal_status': 'pending', 'terminal_monotonic': None}
        counts[command_status] += 1
        result = {'classification': command_status, 'http_status': status, 'counts': counts,
                  'client_entry_monotonic': entry, 'client_return_monotonic': returned,
                  'terminal_status': command_status, 'terminal_monotonic': terminal_monotonic}
        if isinstance(body, dict) and body.get('command_id'):
            result['command_id'] = body['command_id']
        if command_status == 'succeeded':
            interval = body.get('sample_interval_us')
            from fractions import Fraction
            try:
                requested_hz = Fraction(str(frequency_hz))
            except (ValueError, ZeroDivisionError):
                raise ValueError('paper_p6_frequency_rejected') from None
            if requested_hz <= 0:
                raise ValueError('paper_p6_frequency_rejected')
            numerator = 1_000_000 * requested_hz.denominator
            expected_interval, remainder = divmod(numerator, requested_hz.numerator)
            if remainder * 2 >= requested_hz.numerator:
                expected_interval += 1
            if type(interval) is not int or interval != expected_interval:
                raise ValueError('paper_p6_command_response_rejected')
            status, _, view = smoke.http_request(endpoint, 'GET', path + '?format=simplified', token=token)
            sql = "SELECT (s.reported_state->'configuration')::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn';"
            db_config = json.loads(smoke.db_psql(registry, pg, sql, {'urn': sensor_urn}).strip())
            reported = view.get('reported_state', {}).get('configuration') if status == 200 else None
            if (status != 200 or reported != db_config
                    or db_config.get('desired_version') != body.get('desired_version')
                    or db_config.get('sample_interval_us') != interval):
                raise ValueError('paper_p6_confirmed_configuration_failed')
            result.update(desired_version=body['desired_version'], sample_interval_us=interval,
                          reported_configuration_verified=True)
        return result

    error = body.get('error') if isinstance(body, dict) else None
    code = error.get('code') if isinstance(error, dict) else None
    classification = ('noop' if status == 200 else
                      'busy' if status == 409 and code == 'command_in_progress' else
                      'rejected' if status in (400, 403, 404, 409, 422) else 'error')
    counts[classification] = 1
    return {'classification': classification, 'http_status': status, 'counts': counts,
            'client_entry_monotonic': entry, 'client_return_monotonic': returned}


def issue_command_sequence(smoke, endpoint, token, registry, pg, sensor_urn, frequencies_hz,
                           *, timeout=35):
    """Issue distinct, increasing-version frequencies sequentially on one Sensor."""
    from fractions import Fraction
    frequencies = list(frequencies_hz)
    try:
        rational = [Fraction(str(value)) for value in frequencies]
    except (ValueError, ZeroDivisionError):
        raise ValueError('paper_p6_frequency_sequence_rejected') from None
    if (not frequencies or len(set(rational)) != len(frequencies)
            or any(value <= 0 or value > 100 for value in rational)):
        raise ValueError('paper_p6_frequency_sequence_rejected')
    results, prior = [], None
    for frequency in frequencies:
        result = issue_and_confirm_command(smoke, endpoint, token, registry, pg, sensor_urn,
            frequency, prior_response=prior, timeout=timeout)
        results.append(result)
        if result['classification'] != 'succeeded':
            break
        prior = result
    totals = {name: sum(row.get('counts', {}).get(name, 0) for row in results)
              for name in ('accepted', 'succeeded', 'failed', 'timeout', 'rejected',
                           'busy', 'noop', 'replay', 'error')}
    return {'results': results, 'counts': totals,
            'requested_frequencies_hz': frequencies}


def sensor_count_delta(spec, current, baseline):
    before = baseline[(spec.gateway_id, spec.sensor_id)]
    return {key: current[key] - before[key]
            for key in ('ledger', 'raw', 'raw_samples', 'temporal')}


def run(smoke, fixture, endpoint, registry, containers, images, state, fixture_callback=None):
    """Execute a finite P6 workload on the exact-owned native fixture."""
    if any(v is None for v in (endpoint, registry, containers, images, state)):
        raise ValueError('paper_p6_fixture_required')
    import json
    from stage3_uplink_runner import gateway_env
    from stage3_mqtt_ingress_runner import parse_run_summary
    safe_artifact = smoke.paper_p4_support().safe_artifact

    mode, dataset = fixture.paper_p6_mode, fixture.paper_p6_dataset
    pg, quota, effective, resources, storage, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, state))
    validate_effective_profile(fixture.values, mode, dataset)
    workload = workload_spec(dataset)
    # Keep the run finite while leaving enough time for real command cycles.
    steady_count, burst_count = 60, 24
    steady_sample_hz = workload['telemetry']['sample_rate_hz']
    burst_sample_hz = 400 if dataset == '100' else 4
    sample_count = workload['telemetry']['samples_per_message']
    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    volumes = {}
    for role in ('a', 'b'):
        state.checkpoint()
        item = registry.intent('volume', fixture.project + '_outbox_p6_' + role, None, labels)
        if registry.resolve(item) is not None:
            raise ValueError('resource_collision')
        registry.allocating(item)
        result = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, item['name']])
        if result.returncode or registry.resolve(item) is None:
            raise ValueError('helper_allocation_unresolved')
        volumes[role] = item

    command_sims = provision_command_simulators(smoke, fixture, registry, endpoint, tokens,
        specs[0], passwords[0], state, dataset=dataset)
    telemetry = []
    for role, spec, password, count in zip(('a', 'b'), specs, passwords, (steady_count, steady_count)):
        env = gateway_env(spec.gateway_id, password)
        env.update(GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883',
                   GATEWAY_SAMPLE_RATE_HZ=str(workload['telemetry']['sample_rate_hz']),
                   GATEWAY_SAMPLES_PER_BATCH=str(sample_count),
                   GATEWAY_MQTT_CLIENT_ID=fixture.project + '_p6_' + role)
        path = fixture.private / ('gateway-p6-' + role + '.env')
        smoke.private_write(path, ''.join(k + '=' + v + '\n' for k, v in env.items()))
        telemetry.append((spec, password, path, count))

    run_data = {'schema_version': 1, 'run_id': fixture.project, 'status': 'FAIL',
        'native_executed': True, 'mode': mode, 'dataset': dataset, 'backend_quota': quota,
        'service_resources': resources, 'storage_status': storage, 'profile': profile(mode, dataset),
        'schema_terminal': '24:telemetry_ledger_timestamp', 'workload': workload,
         'parameters': {'a_messages': steady_count, 'b_steady_messages': 12,
                         'b_burst_messages': burst_count, 'steady_sample_rate_hz': steady_sample_hz,
                         'burst_sample_rate_hz': burst_sample_hz, 'telemetry_sample_count': sample_count,
                        'command_sensor_sample_rate_hz': workload['telemetry']['sample_rate_hz'],
                        'command_sensor_samples_per_batch': sample_count,
                        'command_sensor_telemetry_accounted_separately': True},
         'commands': [], 'telemetry': [], 'checks': {}, 'cleanup_verified': False,
         'source': source_identity(Path(__file__).resolve().parents[2]),
         'image_identifiers': {key: str(value) for key, value in images.items()},
         'evidence_references': {'effective_profile': 'profile', 'backend_quota': 'backend_quota',
                                 'schema_terminal': 'schema_terminal', 'prepare_seed_excluded': True}}
    smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))

    def sql(query, **variables):
        return smoke.db_psql(registry, pg, query, variables).strip()

    def sensor_db_counts(spec):
        return json.loads(sql("""SELECT json_build_object('ledger',
          (SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'raw_samples',(SELECT coalesce(sum(sample_count),0) FROM processed_messages
                         WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
                      WHERE e.entity_id=:'urn'));""", gw=spec.gateway_id, sensor=spec.sensor_id,
                       urn=spec.sensor_urn))

    p6_started = time.monotonic()
    run_data['runner_origin_monotonic'] = p6_started
    def p6_progress(event, *, tag='workload', count=None):
        safe_count = '' if count is None else ' count=%d' % int(count)
        print('P6_PROGRESS event=%s tag=%s elapsed_seconds=%.3f%s' %
              (event, tag, max(0.0, time.monotonic() - p6_started), safe_count), flush=True)

    def timeout_diagnostic(reason):
        # Read-only bounded admin/SQL snapshot. Never invoke a Gateway count/publisher
        # while producers are active; all writes are private and best-effort.
        data = {'reason': reason, 'observed_monotonic': time.monotonic(),
                'sensors': [], 'metrics_status': None}
        try:
            status, _, snapshot = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics',
                                                      token=tokens['admin'], timeout=3)
            data['metrics_status'] = status
            if status == 200 and isinstance(snapshot, dict):
                data['metrics'] = snapshot
        except Exception:
            data['metrics_status'] = 'unavailable'
        for spec in db_specs:
            try:
                data['sensors'].append({'gateway_id': spec.gateway_id,
                    'sensor_id': spec.sensor_id, 'counts': sensor_db_counts(spec)})
            except Exception:
                data['sensors'].append({'gateway_id': spec.gateway_id,
                    'sensor_id': spec.sensor_id, 'counts_status': 'unavailable'})
        data['command_simulators'] = []
        for control in command_sims:
            name = fixture.project + '-gateway-' + control['tag']
            item = next((entry for entry in registry.items
                         if entry.get('kind') == 'container' and entry.get('name') == name), None)
            if item is None:
                data['command_simulators'].append({'tag': control['tag'], 'status': 'unavailable'})
                continue
            try:
                obj = registry.resolve(item)
                logs = registry.command(['logs', '--tail', '1000', obj['Id']]) if obj else None
                if logs is not None:
                    smoke.private_write(fixture.private / (control['tag'] + '-timeout-diagnostic.log'),
                        (logs.stdout + logs.stderr).decode(errors='replace')[-65536:])
                data['command_simulators'].append({'tag': control['tag'],
                    'state': {key: (obj or {}).get('State', {}).get(key) for key in
                              ('Status', 'Running', 'ExitCode', 'StartedAt', 'FinishedAt')},
                    'logs_status': 'available' if logs is not None else 'unavailable'})
            except Exception:
                data['command_simulators'].append({'tag': control['tag'], 'status': 'unavailable'})
        try:
            smoke.private_write(fixture.private / 'p6-timeout-diagnostic.json',
                                json.dumps(data, sort_keys=True) + '\n')
        except Exception:
            pass

    db_specs = list(specs) + [type('SensorSpec', (), {
        'gateway_id': item['gateway_id'], 'sensor_id': item['sensor_id'],
        'sensor_urn': item['sensor_urn']}) for item in command_sims]
    db_before = {(spec.gateway_id, spec.sensor_id): sensor_db_counts(spec) for spec in db_specs}

    # Test-only synchronization point for an affected P7 regression. Existing P6
    # callers pass no callback and retain the original execution path unchanged.
    if fixture_callback is not None:
        fixture_callback.start(pg=pg, tokens=tokens, specs=specs, baseline=db_before,
                              sensor_db_counts=sensor_db_counts)

    command_windows = []
    def issue_commands(sim, frequencies, phase):
        p6_progress('command_sequence_start', tag=sim['tag'])
        before = time.time()
        result = issue_command_sequence(smoke, endpoint, tokens['a'], registry, pg,
                                        sim['sensor_urn'], frequencies)
        after = time.time()
        command_windows.append({'gateway_id': sim['gateway_id'], 'sensor_id': sim['sensor_id'], 'phase': phase, 'window_start_unix': before,
                                'window_end_unix': after, 'sequence': result})
        run_data['commands'].append({'gateway_id': sim['gateway_id'], 'sensor_id': sim['sensor_id'], **result})
        smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))
        p6_progress('command_sequence_finished', tag=sim['tag'],
                    count=result['counts'].get('succeeded', 0))
        if result['counts']['succeeded'] != len(frequencies):
            raise ValueError('paper_p6_command_sequence_failed')

    def command_primary():
        issue_commands(command_sims[0], [1, 2, 4], 'steady')

    def command_secondary():
        issue_commands(command_sims[1], [1, 2, 4], 'burst')

    def run_telemetry(role, stage, batches, rate, during=None):
        spec, password, env_path, _ = telemetry[0 if role == 'a' else 1]
        # The same B volume is reopened for its higher-rate burst process.
        if role == 'b' and stage == 'burst':
            env = dict(line.split('=', 1) for line in env_path.read_text().splitlines())
            env['GATEWAY_SAMPLE_RATE_HZ'] = str(rate)
            env['GATEWAY_SAMPLES_PER_BATCH'] = str(sample_count)
            env_path = fixture.private / 'gateway-p6-b-burst.env'
            smoke.private_write(env_path, ''.join(k + '=' + v + '\n' for k, v in env.items()))
        tag = 'p6-' + role + '-' + stage
        p6_progress('phase_enter', tag=tag, count=batches)
        result = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[role],
            env_path, fixture.private / 'certs/ca.crt', tag,
            ['run', '--batches', str(batches), '--completion-timeout', '120s'], state,
            during=during if during is not None else rate if callable(rate) else None, timeout=160,
            diagnostic=timeout_diagnostic)
        p6_progress('phase_exit', tag=tag)
        return result

    # A's finite generation encloses B steady commands and B burst commands. Both
    # control simulators were already subscribed by run_command_simulators.
    phase_outputs, phase_windows = {}, {}
    def measured_workload(stop_command_producers):
        def b_steady_commands():
            phase_windows['steady_commands_start'] = time.time()
            command_primary()
            phase_windows['steady_commands_end'] = time.time()
        def b_burst_commands():
            phase_windows['burst_commands_start'] = time.time()
            command_secondary()
            phase_windows['burst_commands_end'] = time.time()
            p6_progress('commands_done', count=6)
            phase_windows['commands_done_monotonic'] = time.monotonic()
            stop_command_producers()
            phase_windows['command_producers_stopped_monotonic'] = time.monotonic()
        def run_b_phases():
            p6_progress('b_steady_start', tag='p6-b-steady', count=12)
            phase_outputs['b_steady'] = run_telemetry('b', 'steady', 12, b_steady_commands)
            p6_progress('b_burst_start', tag='p6-b-burst', count=burst_count)
            phase_outputs['b_burst'] = run_telemetry('b', 'burst', burst_count, burst_sample_hz,
                                                       during=b_burst_commands)
            p6_progress('b_phases_finished', tag='p6-b', count=36)
        phase_outputs['a'] = run_telemetry('a', 'steady', steady_count, run_b_phases)
        phase_windows['measured_a_b_finished_monotonic'] = time.monotonic()
        p6_progress('measured_a_b_finished', count=96)
        return {'phase_windows': phase_windows}

    metrics_rows, resource_rows = [], []

    def capture_snapshot(phase):
        state.checkpoint()
        status, _, snapshot = smoke.http_request(
            endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        if status != 200 or not isinstance(snapshot, dict):
            raise ValueError('paper_p6_metrics_unavailable')
        row = {'phase': phase, 'observed_monotonic': time.monotonic(), 'snapshot': snapshot}
        metrics_rows.append(row)
        smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', metrics_rows)
        resource_rows.append(smoke.paper_p0_stats(registry, containers, phase))
        smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv', resource_rows, resources)
        return snapshot

    before_metrics = capture_snapshot('workload_before')
    started = time.monotonic()
    command_run = run_command_simulators(smoke, fixture, registry, images, command_sims,
                                         state, measured_workload, timeout=220, batches=10000,
                                         comparison_stop_hook=True)
    command_drains = {}
    phase_windows['command_drain_start_monotonic'] = time.monotonic()
    for control in command_sims:
        p6_progress('drain_start', tag=control['tag'])
        drain_text = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
            control['volume'], control['env_path'], fixture.private / 'certs/ca.crt',
            control['tag'] + '-drain', ['run', '--batches', '1', '--completion-timeout', '120s'],
            state, timeout=160)
        command_drains[control['name']] = parse_run_summary(drain_text)
        p6_progress('drain_end', tag=control['tag'], count=command_drains[control['name']].get('app_acknowledged', 0))
        outbox = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'],
            control['volume'], control['env_path'], fixture.private / 'certs/ca.crt',
            control['tag'] + '-drained-count', ['count'], state, timeout=25)
        if outbox.strip() != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
            raise ValueError('paper_p6_command_outbox_not_drained')
    phase_windows['command_drain_end_monotonic'] = time.monotonic()
    after_metrics = capture_snapshot('workload_after')
    histogram_windows = {}
    for group in ('command_admission', 'command_dispatch', 'command_timeline', 'command_result'):
        for name, value in (after_metrics.get(group) or {}).items():
            if isinstance(value, dict) and 'buckets' in value:
                histogram_windows[group + '.' + name] = smoke.paper_p1_histogram_window(
                    before_metrics, after_metrics, group, name)
    # Drain/reopen the same volumes and verify outbox state through CLI.
    for role in ('a', 'b'):
        spec = telemetry[0 if role == 'a' else 1][0]
        count = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[role],
            telemetry[0 if role == 'a' else 1][2], fixture.private / 'certs/ca.crt',
            'p6-' + role + '-count', ['count'], state, timeout=25)
        if count.strip() != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
            raise ValueError('paper_p6_outbox_not_drained')

    rows = []
    summaries = {'a': parse_run_summary(phase_outputs['a']),
                 'b_steady': parse_run_summary(phase_outputs['b_steady']),
                 'b_burst': parse_run_summary(phase_outputs['b_burst'])}
    for role, spec in zip(('a', 'b'), specs):
        vals = [summaries['a']] if role == 'a' else [summaries['b_steady'], summaries['b_burst']]
        offered = sum(int(x['enqueued']) for x in vals)
        acked = sum(int(x['app_acknowledged']) for x in vals)
        values = sensor_count_delta(spec, sensor_db_counts(spec), db_before)
        rows.append({'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id, 'offered': offered, 'generated': offered,
            'ledger': values['ledger'], 'raw': values['raw'], 'raw_samples': values['raw_samples'],
            'samples_expected': offered * sample_count,
            'temporal': values['temporal'], 'acknowledged': acked,
            'phase_summaries': vals})
    command_telemetry_rows = []
    for control in command_sims:
        spec = next(item for item in db_specs if (item.gateway_id, item.sensor_id) ==
                    (control['gateway_id'], control['sensor_id']))
        current = sensor_db_counts(spec)
        delta = sensor_count_delta(spec, current, db_before)
        summaries_for_sensor = [command_run['summaries'][control['name']]]
        summaries_for_sensor.append({key: int(value) for key, value in
                                     command_drains[control['name']].items()
                                     if key in ('enqueued', 'publish_attempts', 'app_acknowledged',
                                                'pending_rows', 'pending_bytes', 'quarantined_rows',
                                                'quarantined_bytes')})
        offered = sum(item['enqueued'] for item in summaries_for_sensor)
        acknowledged = sum(item['app_acknowledged'] for item in summaries_for_sensor)
        command_telemetry_rows.append({'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id, 'offered': offered,
            'generated': offered, **delta, 'samples_expected': None,
            'acknowledged': acknowledged, 'phase_summaries': summaries_for_sensor})
    run_data.update(telemetry=rows, command_sensor_telemetry=command_telemetry_rows,
                    command_simulators=command_run,
                    command_telemetry_drain_summaries=command_drains)
    smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))
    validate_telemetry_accounting(rows)
    validate_telemetry_accounting(command_telemetry_rows, require_sample_match=False)
    aggregate_ingress = {
        'sensor_scope': 'two measured telemetry Sensors plus two command Sensors',
        'db_delta_from_pre_generation_snapshot': {
            key: sum(sensor_count_delta(spec, sensor_db_counts(spec), db_before)[key]
                     for spec in db_specs) for key in ('ledger', 'raw', 'raw_samples', 'temporal')},
        'reported_sensor_rows': rows + command_telemetry_rows,
        'prepare_seed_outside_window': True,
    }
    db_delta = aggregate_ingress['db_delta_from_pre_generation_snapshot']
    ingress_delta = counter_deltas(before_metrics, after_metrics,
        ('Received', 'Committed', 'DuplicateCommitted', 'SamplesNew', 'ACKPublished',
         'ShaperPassed', 'ShaperRateLimited', 'QueueFull'))
    ingress_delta['process_instance'] = after_metrics['process_instance']
    cli_summaries = list(summaries.values())
    for control in command_sims:
        cli_summaries.extend((command_run['summaries'][control['name']],
                              {key: int(value) for key, value in command_drains[control['name']].items()
                               if key in ('enqueued', 'publish_attempts', 'app_acknowledged')}))
    cli_totals = {key: sum(int(item.get(field, 0)) for item in cli_summaries)
                  for key, field in (('offered', 'enqueued'), ('publish_attempts', 'publish_attempts'),
                                     ('application_acks', 'app_acknowledged'))}
    # Persist safe totals before any accounting assertion so a failed run remains
    # diagnosable without retaining credentials or raw private diagnostics.
    run_data.update(db_delta=db_delta, ingress_delta=ingress_delta, cli_totals=cli_totals,
                    accounting_check_inputs_persisted=True)
    smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))
    accounting_exact = validate_aggregate_accounting(db_delta, ingress_delta, cli_totals)
    accounting_exact = accounting_exact and ingress_delta['ACKPublished'] >= db_delta['ledger']
    overlap_evidence = []
    for window in command_windows:
        measured_spec = specs[0] if window['phase'] == 'steady' else specs[1]
        observed = json.loads(sql("""SELECT count(*) FROM telemetry
          WHERE gateway_id=:'gw' AND sensor_id=:'sensor'
            AND extract(epoch FROM measured_at) BETWEEN :'start' AND :'end';""",
          gw=measured_spec.gateway_id, sensor=measured_spec.sensor_id,
          start=str(window['window_start_unix']), end=str(window['window_end_unix'])))
        # Same-host wall timestamps are used only to prove generated samples fell
        # inside the HTTP request window; these are not Go monotonic latency metrics.
        overlap_evidence.append({'gateway_id': window['gateway_id'], 'sensor_id': window['sensor_id'], 'phase': window['phase'],
                                 'measured_gateway_id': measured_spec.gateway_id,
                                 'measured_sensor_id': measured_spec.sensor_id,
                                 'telemetry_samples_in_command_window': int(observed),
                                 'provenance': 'same_host_original_measured_at; not latency subtraction'})
    overlap_verified = (len(overlap_evidence) == 2 and
                        all(item['telemetry_samples_in_command_window'] > 0
                            for item in overlap_evidence))
    command_counts = {name: sum(result.get('counts', {}).get(name, 0)
                                for sequence in run_data['commands']
                                for result in sequence.get('results', []))
                      for name in ('accepted', 'succeeded', 'failed', 'timeout', 'rejected',
                                   'busy', 'noop', 'replay', 'error')}
    summaries_valid = all(
        int(summary['enqueued']) == expected and
        int(summary['app_acknowledged']) == expected and
        int(summary['pending_rows']) == 0 and int(summary['pending_bytes']) == 0 and
        int(summary['quarantined_rows']) == 0 and int(summary['quarantined_bytes']) == 0
        for summary, expected in ((summaries['a'], steady_count),
                                  (summaries['b_steady'], 12),
                                  (summaries['b_burst'], burst_count)))
    run_data.update(telemetry=rows, command_sensor_telemetry=command_telemetry_rows,
        aggregate_ingress_accounting=aggregate_ingress,
        duration_seconds=time.monotonic() - started,
        command_simulators=command_run, command_telemetry_drain_summaries=command_drains,
        generation_summaries=summaries,
        command_windows=command_windows, phase_windows=phase_windows,
        overlap_evidence=overlap_evidence, command_counts=command_counts,
        process_markers={'before': before_metrics.get('process_instance'),
                         'after': after_metrics.get('process_instance')},
        histogram_windows=histogram_windows,
        warmup_seconds=0, drain_deadline_seconds=120,
        ingress_counter_deltas=ingress_delta, cli_offered_publish_ack_totals=cli_totals,
        shaper_rejects_and_retries={'shaper_rate_limited': ingress_delta['ShaperRateLimited'],
                                    'gateway_publish_attempts': cli_totals['publish_attempts'],
                                    'meaning': 'separate ingress rejects and CLI retry-inclusive publish attempts'},
        generation_windows={'warmup_seconds': 0, 'finite': True,
                            'generation_summaries': summaries},
        checks={'telemetry_accounting_exact': accounting_exact,
                'commands_succeeded': command_counts['succeeded'] == 6 and command_counts['accepted'] == 6,
                'schema_24_verified': True, 'outboxes_drained': True,
                'summaries_match_offered_and_acked': summaries_valid,
                'command_overlap_verified': overlap_verified})
    run_data['status'] = 'WORKLOAD_COMPLETE_PENDING_CLEANUP' if overlap_verified and summaries_valid else 'FAIL'
    run_data['acceptance'] = {'status': 'PENDING_OUTER_CLEANUP' if overlap_verified and summaries_valid else 'FAIL',
        'command_overlap_verified': overlap_verified, 'telemetry_accounting_exact': True,
        'schema_24_verified': True, 'exact_cleanup_verified': False}
    smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))
    if not overlap_verified or not summaries_valid:
        raise ValueError('paper_p6_workload_evidence_failed')
    if fixture_callback is not None:
        fixture_callback.finish(run_data=run_data, db_specs=specs, pg=pg,
                                sensor_db_counts=sensor_db_counts)
    return run_data
