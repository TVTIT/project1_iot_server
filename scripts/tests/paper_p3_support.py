"""Thin P3 workload on native_compose_smoke's owned P2 topology and registry."""
import hashlib
import json
import secrets
import sqlite3
import subprocess
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class SensorTarget:
    gateway_id: str
    sensor_id: str
    sensor_unit: str = 'degC'

    @property
    def sensor_urn(self) -> str:
        return f'urn:ngsi-ld:Sensor:{self.gateway_id}:{self.sensor_id}'


def replace_backend_without_metrics(smoke, fixture, registry, containers, state):
    """One backend at a time; same exact-owned replacement pattern as P2 rotation."""
    old = containers['backend']
    name = fixture.project + '-backend-metrics-off'
    fragment = fixture.private / 'compose.metrics-off.yml'
    smoke.private_write(fragment, 'services:\n  backend:\n    container_name: ' + name
                        + '\n    environment:\n      METRICS_ENABLED: "false"\n')
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
           'full_p3_accepted': False, 'mode': 'baseline', 'effective_optimizations': [],
           'revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
           'branch': smoke.paper_p0_git(fixture, 'branch', '--show-current'),
           'dirty': bool(smoke.paper_p0_git(fixture, 'status', '--porcelain')),
           'auth_mode': 'rs256', 'https_verified': isinstance(endpoint, smoke.HTTPSEndpoint),
           'backend_quota': quota, 'service_resources': limits,
           'images': {name: images[name] for name in smoke.fixture_services(fixture)},
           'gateway_image_id': images['gateway'],
           'config': {key: fixture.values[key] for key in (
               'DATABASE_MAX_CONNS', 'MQTT_WORKER_COUNT', 'MQTT_QUEUE_CAPACITY',
               'STAGE5_COMMAND_TTL', 'STAGE5_RESULT_QUEUE', 'STAGE5_RESULT_WORKERS',
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
        persist()
        replace_backend_without_metrics(smoke, fixture, registry, containers, state)
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
        run.update(full_p3_accepted=True, status='WORKLOAD_VERIFIED_CLEANUP_PENDING',
                   command_count=6, metrics_disabled_command_count=1,
                   db_counts=smoke.paper_p1_count_db(registry, pg))
        return run
    finally:
        persist()
