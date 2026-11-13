"""Thin P3 workload on native_compose_smoke's owned P2 topology and registry."""
import hashlib
import json
import secrets
import sqlite3
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


MICROBATCH_REGRESSION_TRAFFIC_LABEL = 'fixture_mqtt_publish_not_gateway_generation'


def microbatch_regression_accepted(result):
    return (type(result) is dict and result.get('microbatch_enabled') is True
            and result.get('command_result_confirmed') is True
            and result.get('operation_evidence') is True
            and result.get('traffic_source') == MICROBATCH_REGRESSION_TRAFFIC_LABEL
            and type(result.get('messages')) is list and len(result['messages']) == 2
            and all(type(message) is dict and message.get('persisted') is True
                    and message.get('acknowledged') is True and message.get('state_preserved') is True
                    for message in result['messages'])
            and [message.get('kind') for message in result['messages']] == ['old', 'new'])


@dataclass(frozen=True)
class SensorTarget:
    gateway_id: str
    sensor_id: str
    sensor_unit: str = 'degC'

    @property
    def sensor_urn(self) -> str:
        return f'urn:ngsi-ld:Sensor:{self.gateway_id}:{self.sensor_id}'


def replace_backend_without_metrics(smoke, fixture, registry, containers, state, preserve_ram_dedup=False):
    """One backend at a time; same exact-owned replacement pattern as P2 rotation."""
    old = containers['backend']
    name = fixture.project + '-backend-metrics-off'
    fragment = fixture.private / 'compose.metrics-off.yml'
    environment = '      METRICS_ENABLED: "false"\n'
    if preserve_ram_dedup:
        environment += ('      MQTT_RAM_DEDUP_ENABLED: "true"\n'
                        '      MQTT_RAM_DEDUP_MAX_ENTRIES: "100000"\n'
                        '      MQTT_RAM_DEDUP_MAX_BYTES: "67108864"\n')
    smoke.private_write(fragment, 'services:\n  backend:\n    container_name: ' + name
                        + '\n    environment:\n' + environment)
    # Let the Unix auth listener unlink its socket before removing the old
    # container. Forced removal alone leaves a stale socket on the owned volume.
    if registry.command(['stop', '--time', '20', old['receipt']['id']]).returncode:
        raise ValueError('paper_p3_acceptance_failed')
    registry.remove(old)
    item = registry.intent('container', name, old['image'], old['labels'])
    item['expectation'] = old['expectation']
    registry.save(item)
    if registry.resolve(item) is not None:
        raise ValueError('resource_collision')
    registry.allocating(item)
    containers['backend'] = item
    command = smoke.compose_command(fixture, 'config')[:-3] + ['-f', str(fragment), 'up', '--detach',
        '--no-deps', '--no-build', '--pull', 'never', 'backend']
    result = subprocess.run(command, capture_output=True, env=smoke.process_env(fixture),
                            timeout=smoke.BUDGETS['up'], check=False)
    smoke.private_write(fixture.private / 'metrics-off-replacement.log',
                        (result.stdout + result.stderr).decode(errors='replace'))
    obj = registry.resolve(item)
    if result.returncode or obj is None:
        raise ValueError('paper_p3_acceptance_failed')
    deadline = time.monotonic() + 45
    try:
        while not smoke.startup_state('backend', obj['State']):
            state.checkpoint()
            if time.monotonic() >= deadline:
                raise ValueError('paper_p3_acceptance_failed')
            time.sleep(0.5)
            obj = registry.resolve(item)
            if obj is None:
                raise ValueError('paper_p3_acceptance_failed')
    except BaseException:
        if obj is not None:
            logs = registry.command(['logs', '--tail', '50', obj['Id']])
            smoke.private_write(fixture.private / 'metrics-off-startup-failure.log',
                                (logs.stdout + logs.stderr).decode(errors='replace'))
            smoke.durable_json(fixture.private / 'metrics-off-startup-state.json', obj['State'])
        raise
    if 'METRICS_ENABLED=false' not in obj['Config']['Env']:
        raise ValueError('paper_p3_acceptance_failed')
    if preserve_ram_dedup and any(value not in obj['Config']['Env'] for value in (
            'MQTT_RAM_DEDUP_ENABLED=true', 'MQTT_RAM_DEDUP_MAX_ENTRIES=100000',
            'MQTT_RAM_DEDUP_MAX_BYTES=67108864')):
        raise ValueError('paper_p3_acceptance_failed')
    # Nginx may retain the former container IP in its upstream configuration.
    if registry.command(['exec', containers['nginx']['receipt']['id'], 'nginx', '-s', 'reload']).returncode:
        raise ValueError('paper_p3_acceptance_failed')


def run(smoke, fixture, registry, endpoint, containers, images, state):
    from stage3_uplink_runner import gateway_env
    from stage3_mqtt_ingress_runner import parse_run_summary

    pg, quota, effective, limits, _, tokens, specs, passwords, path = smoke.prepare_paper_workload(
        fixture, registry, endpoint, containers, images, state)
    primary = SensorTarget(specs[0].gateway_id, specs[0].sensor_id, specs[0].sensor_unit)
    secondary = SensorTarget(specs[0].gateway_id, 'sensor_p3_secondary', specs[0].sensor_unit)
    run = {'schema_version': 1, 'run_id': fixture.project, 'status': 'FAIL',
           'full_p3_accepted': False,
           'mode': ('p5b_cache_control_regression' if fixture.paper_p5b_control_regression else
                    'microbatch_control_regression' if fixture.paper_p3_microbatch else 'baseline'),
           'effective_optimizations': (['mqtt_microbatch', 'mqtt_ram_dedup'] if fixture.paper_p5b_control_regression
                                      else ['mqtt_microbatch'] if fixture.paper_p3_microbatch else []),
           'effective_flags': {'MQTT_MICROBATCH_ENABLED': fixture.values['MQTT_MICROBATCH_ENABLED'],
                               'MQTT_RAM_DEDUP_ENABLED': fixture.values['MQTT_RAM_DEDUP_ENABLED'],
                               'MQTT_RAM_DEDUP_MAX_ENTRIES': fixture.values['MQTT_RAM_DEDUP_MAX_ENTRIES'],
                               'MQTT_RAM_DEDUP_MAX_BYTES': fixture.values['MQTT_RAM_DEDUP_MAX_BYTES']},
           'revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
           'branch': smoke.paper_p0_git(fixture, 'branch', '--show-current'),
           'dirty': bool(smoke.paper_p0_git(fixture, 'status', '--porcelain')),
           'auth_mode': 'rs256', 'https_verified': isinstance(endpoint, smoke.HTTPSEndpoint),
           'backend_quota': quota, 'service_resources': limits,
           'images': {name: images[name] for name in smoke.fixture_services(fixture)},
           'gateway_image_id': images['gateway'],
           'config': {key: fixture.values[key] for key in (
               'DATABASE_MAX_CONNS', 'MQTT_WORKER_COUNT', 'MQTT_QUEUE_CAPACITY',
               'MQTT_MICROBATCH_ENABLED', 'STAGE5_COMMAND_TTL', 'STAGE5_RESULT_QUEUE', 'STAGE5_RESULT_WORKERS',
               'STAGE5_SCAN_INTERVAL', 'STAGE5_PUBLISH_WORKERS')},
           'checks': {}, 'commands': [], 'cleanup_verified': False}
    samples, private_control = [], {}

    def persist():
        smoke.durable_json(fixture.artifacts / 'run.json', run)

    def check(name, success, **safe):
        run['checks'][name] = dict(status='PASS' if success else 'FAIL', **safe)
        persist()
        if not success:
            raise ValueError('paper_p3_acceptance_failed')

    def sql(query, **variables):
        return smoke.db_psql(registry, pg, query, variables).strip()

    def scrape(phase):
        status, _, snapshot = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        check('metrics_available', status == 200 and snapshot.get('command_timeline') is not None
              and snapshot.get('command_result') is not None)
        if len(samples) >= 256:
            raise ValueError('paper_p3_acceptance_failed')
        samples.append({'phase': phase, 'snapshot': snapshot})
        smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', samples)
        return snapshot

    def poll(name, predicate, budget=10, fail=True):
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            state.checkpoint()
            value = predicate()
            if value:
                return value
            time.sleep(0.2)
        if fail:
            check(name, False)
        return None

    def sensor_path(spec):
        return '/v1/digital-twins/' + spec.sensor_urn

    def patch(spec, hz, version, token, expected=202):
        started = time.monotonic()
        status, _, body = smoke.http_request(endpoint, 'PATCH', sensor_path(spec) + '/desired-state',
            token=token, headers={'Idempotency-Key': secrets.token_hex(16)},
            body={'frequency_hz': hz, 'expected_desired_version': str(version)})
        check('patch_' + str(hz) + '_' + str(version) + '_' + str(expected), status == expected, http_status=status)
        if expected == 202:
            run['commands'].append({'requested_frequency_hz': hz, 'desired_version': body['desired_version'],
                                    'sample_interval_us': body['sample_interval_us'],
                                    'client_https_round_trip_seconds': time.monotonic() - started})
            persist()
        return body

    def confirmed(spec, command, interval, budget=25):
        last_body = [None]
        def command_result():
            status, _, body = smoke.http_request(endpoint, 'GET', command['status_url'], token=tokens['a'])
            last_body[0] = (status, body)
            return body if status == 200 and body.get('status') == 'succeeded' else None
        view = poll('command_succeeded', command_result, budget=budget, fail=False)
        if not view:
            db_row = sql("SELECT status, error_code, error_message, result_bytes IS NOT NULL FROM twin_commands WHERE command_id=:'id';", id=command['command_id'])
            backend_log = registry.command(['logs', '--tail', '50', containers['backend']['receipt']['id']]).stderr.decode(errors='replace')
            gw_item = next(i for i in registry.items if i['name'] == fixture.project + '-gateway-p3-a')
            gw_log = registry.command(['logs', '--tail', '50', registry.resolve(gw_item)['Id']]).stderr.decode(errors='replace')
            smoke.durable_json(fixture.private / 'confirmed-failure-diagnostic.json', {
                'command_id': command['command_id'], 'last_poll': last_body[0],
                'db_row': db_row, 'backend_log': backend_log, 'gw_log': gw_log,
            })
            check('command_succeeded', False, last_poll=str(last_body[0]), db_row=db_row)
        status, _, body = smoke.http_request(endpoint, 'GET', sensor_path(spec) + '?format=simplified', token=tokens['a'])
        smoke.private_write(fixture.private / ('p3-state-' + command['command_id'] + '.json'), json.dumps(body))
        config = json.loads(sql("SELECT (s.reported_state->'configuration')::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn';", urn=spec.sensor_urn))
        check('confirmed_' + str(interval), status == 200 and view.get('parameters', {}).get('sample_interval_us') == interval
              and config.get('sample_interval_us') == interval and config.get('desired_version') == command['desired_version']
              and body.get('reported_state', {}).get('configuration') == config)
        private_control['confirmed_configuration'] = {
            'desired_version': command['desired_version'], 'sample_interval_us': interval,
            'command_status': view.get('status')}
        return view

    def interval_batches(interval, one_sample=False):
        return sql("""SELECT count(*) FROM processed_messages p JOIN twin_temporal_values v
          ON v.message_id=p.message_id JOIN twin_entities e ON e.id=v.entity_id
          WHERE p.gateway_id=:'gw' AND p.sensor_id=:'sensor' AND e.entity_id=:'urn'
            AND v.property_name='sample_interval_us' AND v.value_number=:'interval'::double precision
            AND (NOT :'single'::boolean OR p.sample_count=1);""",
            gw=primary.gateway_id, sensor=primary.sensor_id, urn=primary.sensor_urn,
            interval=str(interval), single='true' if one_sample else 'false')

    def mqtt_publish(topic, payload, username, password):
        # Credentials enter on stdin, not the host command line or artifacts.
        result = registry.command(['exec', '-i', containers['mosquitto']['receipt']['id'], 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(username + '\n' + password + '\n' + topic + '\n').encode() + payload)
        check('cached_mqtt_publish', result.returncode == 0)

    def run_microbatch_configuration_regression():
        """Publish two fixture messages after the real command path is idle."""
        from stage3_mqtt_ingress_runner import verify_ack
        from stage3_uplink_runner import build_probe_batch, synthetic_value

        before_operations = scrape('microbatch_control_before')
        cache_before = before_operations.get('ingress', {}).get('ram_dedup', {})
        cache_profile_ok = (not fixture.paper_p5b_control_regression or
                            cache_before.get('enabled') is True)
        check('p5b_cache_enabled_during_command_regression', cache_profile_ok,
              cache_enabled=cache_before.get('enabled'),
              effective_flag=fixture.values['MQTT_RAM_DEDUP_ENABLED'])
        current = json.loads(sql("""SELECT json_build_object(
          'boot_id', boot_id, 'last_sequence', max(sequence_num),
          'measured_at', to_char(max(measured_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'))
          FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor' GROUP BY boot_id
          ORDER BY max(measured_at) DESC LIMIT 1;""", gw=primary.gateway_id, sensor=primary.sensor_id))
        current_time = datetime.fromisoformat(current['measured_at'].replace('Z', '+00:00'))
        old_time = (current_time - timedelta(seconds=1)).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        current_measured_at = current_time.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        first_sequence = int(current['last_sequence']) + 1
        messages = [
            {'kind': 'old', 'message_id': str(uuid.uuid4()), 'sequence': first_sequence,
             'measured_at': old_time, 'advances_current': False},
            {'kind': 'new', 'message_id': str(uuid.uuid4()), 'sequence': first_sequence + 1,
             'measured_at': current_measured_at, 'advances_current': True},
        ]
        status, _, initial_api = smoke.http_request(endpoint, 'GET',
            sensor_path(primary) + '?format=simplified', token=tokens['a'])
        initial_db = json.loads(sql("""SELECT json_build_object(
          'reported_state', s.reported_state, 'desired_state', s.desired_state,
          'reported_version', s.reported_version, 'desired_version', s.desired_version)
          FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn';""",
          urn=primary.sensor_urn))
        confirmed_config = private_control.get('confirmed_configuration', {})
        api_state_matches = initial_api.get('reported_state') == initial_db['reported_state']
        api_desired_matches = initial_api.get('desired_state') == initial_db['desired_state']
        command_succeeded = confirmed_config.get('command_status') == 'succeeded'
        reported_configuration = initial_db['reported_state'].get('configuration')
        config_matches_command = (isinstance(reported_configuration, dict)
            and reported_configuration.get('desired_version') == confirmed_config.get('desired_version')
            and reported_configuration.get('sample_interval_us') == confirmed_config.get('sample_interval_us')
            and isinstance(reported_configuration.get('applied_at'), str)
            and bool(reported_configuration['applied_at']))
        check('microbatch_control_initial_snapshot', status == 200 and api_state_matches
              and api_desired_matches and command_succeeded and config_matches_command,
              http_status=status, api_state_matches_db=api_state_matches,
              api_desired_matches_db=api_desired_matches, command_succeeded=command_succeeded,
              configuration_matches_command=config_matches_command,
              reported_configuration=initial_db['reported_state'].get('configuration'),
              confirmed_configuration={key: confirmed_config.get(key) for key in
                                       ('desired_version', 'sample_interval_us', 'command_status')})
        check('microbatch_control_time_ordering',
              datetime.fromisoformat(initial_db['reported_state']['observed_at'].replace('Z', '+00:00'))
                  == current_time
              and current_time <= datetime.now(timezone.utc)
              and datetime.fromisoformat(old_time.replace('Z', '+00:00')) < current_time
              and datetime.fromisoformat(current_measured_at.replace('Z', '+00:00')) <= datetime.now(timezone.utc),
              old_is_late=True, new_is_current=True, future_timestamps=False)
        ack_observer = smoke.paper_p1_subscriber(fixture, registry,
            containers['mosquitto']['receipt']['id'], primary, passwords[0],
            'p5a-control-regression-ack', message_count=2, message_timeout=30, subscription_kind='ack')
        accepted_messages = []
        try:
            for entry in messages:
                payload = build_probe_batch(primary, entry['message_id'], current['boot_id'], entry['sequence'],
                    sample_count=1, sample_interval_us=12345, measured_at=entry['measured_at'])
                topic = 'gateways/' + primary.gateway_id + '/telemetry/' + primary.sensor_id
                mqtt_publish(topic, payload, primary.gateway_id, passwords[0])
                ack_topic = 'gateways/' + primary.gateway_id + '/acks/' + entry['message_id']
                ack_payload = smoke.paper_p1_capture_subscription(ack_observer, ack_topic, timeout=20)
                check('microbatch_control_application_ack_' + entry['kind'], ack_payload is not None)
                verify_ack(entry['message_id'], ack_payload)
                raw_result = json.loads(sql("""SELECT json_build_object(
                  'ledger', (SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id'),
                  'raw', (SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND message_id=:'id'),
                  'raw_match', (SELECT count(*)=1 FROM telemetry t JOIN processed_messages p
                    ON p.gateway_id=t.gateway_id AND p.message_id=t.message_id
                    WHERE t.gateway_id=:'gw' AND t.message_id=:'id' AND t.sensor_id=:'sensor'
                      AND t.boot_id=:'boot'::uuid AND t.sequence_num=:'seq'::bigint
                      AND t.sample_index=0 AND t.measured_at=:'measured'::timestamptz
                      AND abs(t.value-:'value'::double precision)<1e-9
                      AND p.first_sequence=:'seq'::bigint AND p.last_sequence=:'seq'::bigint
                      AND p.sample_count=1 AND p.source_topic=:'topic' AND p.payload_hash=:'hash'),
                  'temporal', (SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
                    WHERE e.entity_id=:'urn' AND v.message_id=:'id' AND v.property_name='sample_interval_us'
                      AND v.value_number=12345 AND v.observed_at=:'measured'::timestamptz
                      AND v.metadata->>'source'='telemetry_batch' AND v.metadata->>'boot_id'=:'boot'));""",
                  gw=primary.gateway_id, id=entry['message_id'], sensor=primary.sensor_id,
                  boot=current['boot_id'], seq=str(entry['sequence']), measured=entry['measured_at'],
                  value=str(synthetic_value(entry['sequence'])), topic=topic,
                  hash=hashlib.sha256(payload).hexdigest(), urn=primary.sensor_urn))
                check('microbatch_control_message_persisted_' + entry['kind'],
                      raw_result == {'ledger': 1, 'raw': 1, 'raw_match': True, 'temporal': 1},
                      persisted=raw_result, sequence=entry['sequence'], measured_at=entry['measured_at'],
                      sample_interval_us=12345)
                status, _, after_api = smoke.http_request(endpoint, 'GET',
                    sensor_path(primary) + '?format=simplified', token=tokens['a'])
                after_db = json.loads(sql("""SELECT json_build_object(
                  'reported_state', s.reported_state, 'desired_state', s.desired_state,
                  'reported_version', s.reported_version, 'desired_version', s.desired_version)
                  FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn';""",
                  urn=primary.sensor_urn))
                config_preserved = (after_db['reported_state'].get('configuration')
                                    == initial_db['reported_state'].get('configuration'))
                desired_preserved = (after_db['desired_state'] == initial_db['desired_state']
                                     and after_db['desired_version'] == initial_db['desired_version'])
                if entry['advances_current']:
                    state_ok = (after_db['reported_version'] == initial_db['reported_version'] + 1
                                and datetime.fromisoformat(after_db['reported_state']['observed_at'].replace('Z', '+00:00'))
                                    == datetime.fromisoformat(entry['measured_at'].replace('Z', '+00:00'))
                                and after_db['reported_state'].get('sequence_num') == entry['sequence'])
                else:
                    state_ok = (after_db['reported_version'] == initial_db['reported_version']
                                and after_db['reported_state'] == initial_db['reported_state'])
                check('microbatch_control_state_' + entry['kind'], status == 200
                      and after_api.get('reported_state') == after_db['reported_state']
                      and after_api.get('desired_state') == after_db['desired_state']
                      and config_preserved and desired_preserved and state_ok,
                      reported_version=after_db['reported_version'], desired_version=after_db['desired_version'],
                      configuration_preserved=config_preserved, desired_preserved=desired_preserved,
                      old_measurement_did_not_rollback=state_ok if not entry['advances_current'] else None,
                      new_measurement_advanced=state_ok if entry['advances_current'] else None)
                accepted_messages.append({'kind': entry['kind'], 'persisted': True,
                    'acknowledged': True, 'state_preserved': config_preserved and desired_preserved and state_ok,
                    'sequence': entry['sequence'], 'measured_at': entry['measured_at']})
        finally:
            smoke.paper_p1_stop_subscription(ack_observer)
        after_operations = scrape('microbatch_control_after')
        cache_after = after_operations.get('ingress', {}).get('ram_dedup', {})
        if fixture.paper_p5b_control_regression:
            check('p5b_cache_enabled_after_telemetry', cache_after.get('enabled') is True,
                  cache_enabled=cache_after.get('enabled'))
        before_mb = before_operations['ingress']['microbatch']
        after_mb = after_operations['ingress']['microbatch']
        group_attempt_delta = (after_operations['persistence']['group_attempts']
                               - before_operations['persistence']['group_attempts'])
        group_member_delta = after_mb['members'] - before_mb['members']
        operation_evidence = (before_mb.get('enabled') is True and after_mb.get('enabled') is True
                              and group_attempt_delta > 0 and group_member_delta >= 2)
        check('microbatch_control_operation_evidence', operation_evidence,
              group_attempt_delta=group_attempt_delta, group_member_delta=group_member_delta,
              enabled_before=before_mb.get('enabled'), enabled_after=after_mb.get('enabled'))
        outcome = {'microbatch_enabled': fixture.values['MQTT_MICROBATCH_ENABLED'] == 'true',
                   'command_result_confirmed': (private_control.get('confirmed_configuration', {}).get('command_status')
                                                == 'succeeded'),
                   'messages': accepted_messages,
                   'operation_evidence': operation_evidence,
                   'traffic_source': MICROBATCH_REGRESSION_TRAFFIC_LABEL,
                   'confirmed_configuration': initial_db['reported_state'].get('configuration'),
                   'desired_version': initial_db['desired_version']}
        check('microbatch_configuration_preservation', microbatch_regression_accepted(outcome),
              traffic_source=outcome['traffic_source'], messages=accepted_messages,
              effective_microbatch_flag=fixture.values['MQTT_MICROBATCH_ENABLED'])
        return outcome

    def signal(tag):
        item = next(i for i in registry.items if i['name'] == fixture.project + '-gateway-' + tag)
        obj = registry.resolve(item)
        check('stop_' + tag, registry.command(['kill', '--signal=SIGTERM', obj['Id']]).returncode == 0)

    def sqlite_snapshot(tag, phase):
        item = next(i for i in registry.items if i['name'] == fixture.project + '-gateway-' + tag)
        target = fixture.private / ('p3-' + phase + '.db')
        result = registry.command(['cp', registry.resolve(item)['Id'] + ':/data/outbox/outbox.db', str(target)])
        check('sqlite_copy_' + phase, result.returncode == 0)
        with sqlite3.connect('file:' + str(target) + '?mode=ro', uri=True, timeout=2) as db:
            cfg = db.execute('SELECT sample_interval_us,desired_version FROM sensor_configuration').fetchone()
            receipts = db.execute('SELECT command_id,command_hash,result_bytes FROM command_receipts ORDER BY command_id').fetchall()
            pending = db.execute("SELECT message_id,payload FROM outbox WHERE status='pending' ORDER BY id").fetchall()
        return cfg, receipts, pending

    # All provisioning uses real API membership/credential guards, not state seeds.
    status = smoke.http_request(endpoint, 'PUT', f'/v1/gateways/{primary.gateway_id}/sensors/{secondary.sensor_id}',
        token=tokens['a'], body={'name': 'P3 secondary Sensor', 'unit': primary.sensor_unit})[0]
    check('provision_second_sensor', status == 201)
    check('configuration_not_seeded', sql("SELECT count(*) FROM twin_states WHERE reported_state ? 'configuration';") == '0')
    check('real_rs256_login', all(smoke.paper_p2_support().decode_token(t)[0].get('alg') == 'RS256' for t in tokens.values()))
    before = scrape('before_control')
    patch(primary, 100, 0, tokens['b'], 404)
    patch(primary, 100, 0, tokens['admin'], 404)
    user_b = smoke.paper_p2_support().decode_token(tokens['b'])[1]['sub']
    membership = f'/v1/admin/gateways/{primary.gateway_id}/users/{user_b}'
    status = smoke.http_request(endpoint, 'PUT', membership, token=tokens['admin'], body={'role': 'viewer'})[0]
    check('grant_viewer', status in (200, 201))
    check('viewer_reads', smoke.http_request(endpoint, 'GET', path + '/commands', token=tokens['b'])[0] == 200)
    patch(primary, 100, 0, tokens['b'], 403)
    status = smoke.http_request(endpoint, 'PUT', membership, token=tokens['admin'], body={'role': 'operator'})[0]
    check('grant_operator', status in (200, 201))

    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    volumes, envs = {}, {}
    for tag, spec in (('p3-a', primary), ('p3-second', secondary)):
        volume = registry.intent('volume', fixture.project + '_outbox_' + tag, None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        result = registry.command(['volume', 'create', '--label', 'test.task=native-compose-startup',
                                  '--label', 'test.run=' + fixture.project, volume['name']])
        check('volume_' + tag, result.returncode == 0 and registry.resolve(volume) is not None)
        env = gateway_env(spec.gateway_id, passwords[0])
        env.update(SENSOR_ID=spec.sensor_id, GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883',
                   GATEWAY_MQTT_CLIENT_ID=fixture.project + '_' + tag, GATEWAY_CONTROL_ENABLED='true')
        env_file = fixture.private / ('gateway-' + tag + '.env')
        smoke.private_write(env_file, ''.join(k + '=' + v + '\n' for k, v in env.items()))
        volumes[tag], envs[tag] = volume, env_file

    def gateway(tag, spec_tag, args, during=None):
        return smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[spec_tag], envs[spec_tag],
            fixture.private / 'certs/ca.crt', tag, args, state, during=during, timeout=40)

    def exercise():
        first = patch(primary, 100, 0, tokens['a'])
        other = patch(secondary, 100, 0, tokens['a'])
        confirmed(primary, first, 10000)
        confirmed(secondary, other, 10000)
        check('two_sensors_same_gateway', first['command_id'] != other['command_id'])
        decimal = patch(primary, 1.5, 1, tokens['a'])
        confirmed(primary, decimal, 666667)
        poll('decimal_measurement', lambda: interval_batches(666667) != '0')
        # Hold only result acceptance, not the intent or publisher. Cached resend
        # must recover after a real, temporary DB error without any new command.
        sql("""CREATE TABLE public.p3_hold(kind text PRIMARY KEY);
          GRANT SELECT ON public.p3_hold TO iot_backend_app;
          INSERT INTO public.p3_hold VALUES('result');
          CREATE FUNCTION public.p3_hold_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            IF NEW.result_bytes IS NOT NULL AND OLD.result_bytes IS NULL AND EXISTS(SELECT 1 FROM public.p3_hold WHERE kind='result') THEN
              RAISE EXCEPTION 'P3 fixture result hold'; END IF; RETURN NEW; END $$;
          CREATE TRIGGER p3_hold_result BEFORE UPDATE ON public.twin_commands FOR EACH ROW EXECUTE FUNCTION public.p3_hold_result();""")
        low = patch(primary, 0.1, 2, tokens['b'])
        poll('db_failure_observed', lambda: scrape('result_db_hold')['command_result']['db_error'] > 0)
        check('db_failure_never_confirms', sql("SELECT reported_state->'configuration'->>'sample_interval_us' FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn';", urn=primary.sensor_urn) == '666667')
        sql("DROP TRIGGER p3_hold_result ON public.twin_commands; DROP FUNCTION public.p3_hold_result(); DROP TABLE public.p3_hold;")
        confirmed(primary, low, 10000000)
        check('low_hz_result_before_sample', interval_batches(10000000) == '0')
        poll('one_low_hz_sample', lambda: interval_batches(10000000, True) != '0', budget=12)
        restore = patch(primary, 60, 3, tokens['a'])
        confirmed(primary, restore, 16667)
        run['restart_command'] = {'desired_version': restore['desired_version'], 'sample_interval_us': 16667}
        # Block new telemetry commits so a controlled stop/reopen has real
        # pending payloads; no application ACK can be fabricated for them.
        sql("""CREATE FUNCTION public.p3_hold_telemetry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
            RAISE EXCEPTION 'P3 fixture telemetry hold'; RETURN NEW; END $$;
          CREATE TRIGGER p3_hold_telemetry BEFORE INSERT ON public.processed_messages FOR EACH ROW EXECUTE FUNCTION public.p3_hold_telemetry();""")
        poll('telemetry_persist_failure', lambda: scrape('telemetry_hold')['ingress']['PersistFailures'] > 0)
        # The secondary finite process completes independently; stop the primary
        # with pending rows, then inspect only the stopped SQLite file.
        signal('p3-a')
        private_control['restore_id'] = restore['command_id']

    try:
        secondary_output = []
        def both():
            secondary_output.append(gateway('p3-second', 'p3-second', ['run', '--batches', '2', '--completion-timeout', '30s'], exercise))
        gateway('p3-a', 'p3-a', ['run'], both)
        check('secondary_application_ack', parse_run_summary(secondary_output[0]).get('pending_rows') == '0')
        cfg, receipts, pending = sqlite_snapshot('p3-a', 'before-reopen')
        check('durable_config_receipts_pending', cfg == (16667, 4) and len(receipts) == 4 and bool(pending))
        restore_id = private_control['restore_id']
        sql('DROP TRIGGER p3_hold_telemetry ON public.processed_messages;')
        duplicate_before = scrape('before_reopen')['command_result']['duplicate']
        def duplicate_command():
            payload = bytes.fromhex(sql("SELECT encode(payload_bytes,'hex') FROM twin_outbox WHERE command_id=:'id';", id=restore_id))
            mqtt_publish('gateways/' + primary.gateway_id + '/commands/' + restore_id, payload,
                         fixture.values['MQTT_USERNAME'], fixture.values['MQTT_PASSWORD'])
            poll('cached_result_after_reopen', lambda: scrape('reopen_duplicate')['command_result']['duplicate'] > duplicate_before)
        output = gateway('p3-reopen', 'p3-a', ['run', '--batches', '1', '--completion-timeout', '30s'], duplicate_command)
        check('reopen_active_readback', 'active_interval_us=16667' in output and parse_run_summary(output).get('pending_rows') == '0')
        cfg_after, receipts_after, pending_after = sqlite_snapshot('p3-reopen', 'after-reopen')
        check('canonical_receipts_byte_exact', cfg_after == cfg and receipts_after == receipts and not pending_after)
        for message_id, payload in pending:
            digest = sql("SELECT payload_hash FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id';", gw=primary.gateway_id, id=message_id)
            check('pending_replay_byte_exact', digest == hashlib.sha256(payload).hexdigest())
        final = scrape('confirmed_drained')
        timeline = final['command_timeline']
        check('runtime_durations_once', before['process_instance'] == final['process_instance']
              and timeline['d_dispatch']['count'] == 5 and timeline['d_e2e_succeeded']['count'] == 5
              and final['command_admission']['accepted'] == 5)
        # Raw accounting uses variable decimal/boundary batch sizes, not 100
        # samples assumed for every message. Config results create no temporal row.
        reconciliation = sql("""SELECT count(*) FROM processed_messages p WHERE
          (SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)<>p.sample_count;""")
        check('raw_samples_match_committed_batches', reconciliation == '0')
        # Preserve the five-command, single-process timing sample before the
        # supplementary metrics-disabled regression restarts this owned backend.
        run.update(metrics_enabled_command_count=5, durations=timeline, result_outcomes=final['command_result'])
        if fixture.paper_p3_microbatch:
            run['microbatch_control_regression'] = run_microbatch_configuration_regression()
            run['microbatch_control_regression_accepted'] = microbatch_regression_accepted(
                run['microbatch_control_regression'])
            if fixture.paper_p5b_control_regression:
                run['p5b_control_configuration_preservation_accepted'] = (
                    run['microbatch_control_regression_accepted'] is True
                    and fixture.values['MQTT_RAM_DEDUP_ENABLED'] == 'true')
                run['effective_optimizations'] = ['mqtt_microbatch', 'mqtt_ram_dedup']
        persist()
        replace_backend_without_metrics(smoke, fixture, registry, containers, state,
                                        preserve_ram_dedup=fixture.paper_p5b_control_regression)
        if fixture.paper_p5b_control_regression:
            replacement_env = registry.resolve(containers['backend'])['Config']['Env']
            check('metrics_off_ram_dedup_effective_flags', all(value in replacement_env for value in (
                'MQTT_RAM_DEDUP_ENABLED=true', 'MQTT_RAM_DEDUP_MAX_ENTRIES=100000',
                'MQTT_RAM_DEDUP_MAX_BYTES=67108864')))
        check('metrics_disabled_readback', smoke.paper_p2_support().direct_go(
            smoke, registry, containers, tokens['admin'])[0] == 404)
        poll('metrics_off_https_ready', lambda: smoke.http_request(
            endpoint, 'GET', sensor_path(primary), token=tokens['a'])[0] == 200, budget=15)

        def metrics_off_command():
            # Wait for the real two-subscription lifetime before issuing intent.
            item = next(i for i in registry.items if i['name'] == fixture.project + '-gateway-p3-metrics-off')
            def subscribed():
                logs = registry.command(['logs', registry.resolve(item)['Id']])
                return logs.returncode == 0 and b'event=connected' in logs.stderr
            poll('metrics_off_gateway_ready', subscribed)
            command = patch(primary, 50, 4, tokens['a'])
            confirmed(primary, command, 20000)
            check('metrics_off_result_committed', sql(
                "SELECT status='succeeded' AND result_bytes IS NOT NULL FROM twin_commands WHERE command_id=:'id';",
                id=command['command_id']) == 't')

        off_output = gateway('p3-metrics-off', 'p3-a', ['run', '--batches', '4', '--completion-timeout', '30s'], metrics_off_command)
        check('metrics_off_application_ack', parse_run_summary(off_output).get('pending_rows') == '0')
        check('metrics_off_backend_alive', smoke.paper_p2_support().direct_go(
            smoke, registry, containers, tokens['a'], sensor_path(primary))[0] == 200)
        check('raw_samples_match_after_metrics_off', sql("""SELECT count(*) FROM processed_messages p WHERE
          (SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)<>p.sample_count;""") == '0')
        run.update(full_p3_accepted=True,
                   status='PAPER_P5B_CONTROL_PRESERVATION_ACCEPTED' if fixture.paper_p5b_control_regression
                   else 'WORKLOAD_VERIFIED_CLEANUP_PENDING',
                   command_count=6, metrics_disabled_command_count=1,
                   db_counts=smoke.paper_p1_count_db(registry, pg))
        return run
    finally:
        persist()
