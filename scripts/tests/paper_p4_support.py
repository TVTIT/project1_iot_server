"""Finite P4 two-Gateway workload on native_compose_smoke's owned fixture."""
import hashlib
import json
import re
import sqlite3
import time


PROFILE_COMMON = {
    'MQTT_SHAPER_RATE_PER_SECOND': '2',
    'MQTT_SHAPER_BURST': '4',
    'MQTT_SHAPER_MAX_GATEWAYS': '4096',
    'MQTT_SHAPER_IDLE_RETENTION': '10m',
    'MQTT_INGRESS_BUFFERED_BYTES': '16777216',
}

RESOURCE_PROFILE = {
    'MQTT_QUEUE_CAPACITY': '256',
    'MQTT_MAX_PAYLOAD_BYTES': '65536',
    'MQTT_WORKER_COUNT': '4',
    'DATABASE_MAX_CONNS': '10',
    'STAGE5_ENABLED': 'false',
    'METRICS_ENABLED': 'true',
}

BROKER_PROFILE = {
    'max_packet_size': 2097152,
    'message_size_limit': 1048576,
    'max_inflight_messages': 20,
    'max_inflight_bytes': 1048576,
    'max_queued_messages': 256,
    'max_queued_bytes': 16777216,
}


def profile(mode):
    if mode not in ('baseline', 'optimized'):
        raise ValueError('paper_p4_mode_rejected')
    return {**PROFILE_COMMON, 'MQTT_SHAPER_ENABLED': 'true' if mode == 'optimized' else 'false'}


def validate_broker_profile(text):
    values = {name: int(value) for name, value in re.findall(
        r'(?m)^(max_packet_size|message_size_limit|max_inflight_messages|'
        r'max_inflight_bytes|max_queued_messages|max_queued_bytes) (\d+)\s*$', text)}
    if values != BROKER_PROFILE or values['max_packet_size'] < values['message_size_limit'] + 1024:
        raise ValueError('paper_p4_broker_profile_rejected')
    return values


def workload_config():
    """Message/sample counts are identical in both modes; rates are real cadence."""
    config = {
        'a_batches': 35,
        'b_steady_batches': 3,
        'b_burst_batches': 20,
        'b_drain_batches': 1,
        'steady_sample_rate_hz': 100,
        'steady_samples_per_batch': 100,
        'burst_sample_rate_hz': 100,
        'burst_samples_per_batch': 10,
        'steady_batch_cadence_per_second': 1,
        'burst_batch_cadence_per_second': 10,
        'drain_deadline_seconds': 80,
        'run_deadline_seconds': 90,
        'optimized_burst_completion_seconds': 4,
        'retry_delivery': 'real Gateway SQLite outbox; startup replay then application ACK',
    }
    config['offered_unique_messages'] = (config['a_batches'] + config['b_steady_batches']
                                        + config['b_burst_batches'] + config['b_drain_batches'])
    config['offered_unique_samples'] = (
        config['a_batches'] * config['steady_samples_per_batch']
        + config['b_steady_batches'] * config['steady_samples_per_batch']
        + (config['b_burst_batches'] + config['b_drain_batches']) * config['burst_samples_per_batch'])
    return config


def validate_effective_profile(fixture, effective):
    environment = effective.get('services', {}).get('backend', {}).get('environment', {})
    return validate_effective_environment(fixture, environment)


def validate_effective_environment(fixture, environment):
    expected = {**profile(fixture.paper_p4_mode), **RESOURCE_PROFILE}
    if any(str(environment.get(key)).lower() != value for key, value in expected.items()):
        raise ValueError('paper_p4_profile_rejected')
    return True


def validate_accounting(accounting):
    required = ('offered_unique', 'backend_attempts', 'committed_unique', 'duplicate',
                'rejected', 'pending_rows', 'pending_bytes', 'wire_bytes')
    if any(key not in accounting for key in required):
        raise ValueError('paper_p4_accounting_rejected')
    if any(type(accounting[key]) is not int or accounting[key] < 0
           for key in required[:-1]):
        raise ValueError('paper_p4_accounting_rejected')
    if accounting['wire_bytes'] is not None and (
            type(accounting['wire_bytes']) is not int or accounting['wire_bytes'] < 0):
        raise ValueError('paper_p4_accounting_rejected')
    if accounting['committed_unique'] > accounting['backend_attempts']:
        raise ValueError('paper_p4_accounting_rejected')
    if accounting['offered_unique'] > accounting['backend_attempts']:
        raise ValueError('paper_p4_accounting_rejected')
    if accounting['pending_rows'] or accounting['pending_bytes']:
        raise ValueError('paper_p4_accounting_rejected')
    return True


def broker_denial_observed(logs_before, logs_after, topic):
    """Use the broker audit record, not QoS1 PUBACK/CLI status, as denial proof."""
    from stage3_uplink_runner import denied_publish_logged
    return denied_publish_logged(logs_before, logs_after, topic)


def acceptance_ready(checks, mode='baseline'):
    required = {'a_progress_during_b_burst', 'a_application_ack_during_b_burst',
                'a_container_active_during_b_burst', 'shaper_profile_response', 'wrong_namespace_denied',
                'own_namespace_payload_captured', 'unique_ledgers_and_raw_samples', 'temporal_per_message',
                'real_gateway_summaries_and_empty_outboxes', 'rs256_https_and_namespace_control'}
    required.add('rejected_traffic_remains_pending' if mode == 'optimized' else 'baseline_burst_state_recorded')
    required.add('duplicate_replay_deduplicated')
    if mode == 'optimized':
        required.add('pending_retry_byte_exact')
    return (mode in ('baseline', 'optimized') and required.issubset(checks)
            and all(checks[key].get('status') == 'PASS' for key in required))


def safe_artifact(value):
    """Allowlist-safe recursive metadata; never serialize secrets or payloads."""
    if isinstance(value, dict):
        return {key: safe_artifact(item) for key, item in value.items()
                if key.lower() not in {'password', 'token', 'secret', 'payload', 'payload_bytes', 'raw_payload'}
                and not key.lower().endswith(('_password', '_token', '_secret'))}
    if isinstance(value, list):
        return [safe_artifact(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise ValueError('paper_p4_artifact_rejected')


def _expected_summary(smoke, output, *, batches, completed):
    from stage3_mqtt_ingress_runner import parse_run_summary
    summary = parse_run_summary(output)
    actual_completed = summary.get('completed') == 'true'
    if (summary.get('completed') not in ('true', 'false')
            or completed is not None and actual_completed != completed
            or int(summary.get('enqueued', '-1')) != batches
            or (actual_completed and (summary.get('pending_rows') != '0'
                                      or summary.get('pending_bytes') != '0'
                                      or summary.get('quarantined_rows') != '0'
                                      or summary.get('quarantined_bytes') != '0'))
            or (not actual_completed and (int(summary.get('pending_rows', '0')) <= 0
                                          or int(summary.get('pending_bytes', '0')) <= 0
                                          or summary.get('quarantined_rows') != '0'
                                          or summary.get('quarantined_bytes') != '0'))):
        raise ValueError('paper_p4_gateway_summary_rejected')
    return summary


def run(smoke, fixture, endpoint, registry, containers, images, state):
    """Run bounded steady/burst/drain phases; all MQTT and ACK work is real Go."""
    if any(value is None for value in (endpoint, registry, containers, images, state)):
        raise ValueError('paper_p4_fixture_required')

    import stage3_uplink_runner as uplink

    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, state))
    config = workload_config()
    mode = fixture.paper_p4_mode
    shaped = mode == 'optimized'
    if not smoke.paper_p4_support().validate_effective_profile(fixture, effective):
        raise ValueError('paper_p4_profile_rejected')
    broker_config = validate_broker_profile((fixture.private / 'mosquitto.conf').read_text())

    run_data = {
        'schema_version': 1,
        'run_id': fixture.project,
        'status': 'FAIL',
        'full_p4_accepted': False,
        'mode': mode,
        'effective_optimizations': ['mqtt_shaper'] if shaped else [],
        'revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'branch': smoke.paper_p0_git(fixture, 'branch', '--show-current'),
        'dirty': bool(smoke.paper_p0_git(fixture, 'status', '--porcelain')),
        'auth_mode': 'rs256',
        'https_verified': isinstance(endpoint, smoke.HTTPSEndpoint),
        'backend_quota': quota,
        'service_resources': resources,
        'images': {name: images[name] for name in smoke.fixture_services(fixture)},
        'gateway_image_id': images['gateway'],
        'profile': profile(mode),
        'resource_profile': RESOURCE_PROFILE,
        'broker_config_limits': broker_config,
        'quota_and_environment': {'storage_status': storage_status, 'stage5_enabled': False,
                                  'metrics_enabled': True},
        'workload': config,
        'phases': [],
        'checks': {},
        'cleanup_verified': False,
    }
    metrics, resource_rows = [], []
    started = time.monotonic()

    def persist():
        smoke.durable_json(fixture.artifacts / 'run.json', safe_artifact(run_data))

    def check(name, success, **detail):
        run_data['checks'][name] = {'status': 'PASS' if success else 'FAIL', **safe_artifact(detail)}
        persist()
        if not success:
            raise ValueError('paper_p4_acceptance_failed')

    def sql(query, **variables):
        return smoke.db_psql(registry, pg, query, variables).strip()

    def counts(phase):
        result = json.loads(sql("""SELECT json_build_object(
          'a', (SELECT json_build_object('messages', count(*), 'samples', coalesce(sum(sample_count),0))
                FROM processed_messages WHERE gateway_id=:'a'),
          'b', (SELECT json_build_object('messages', count(*), 'samples', coalesce(sum(sample_count),0))
                FROM processed_messages WHERE gateway_id=:'b'),
          'temporal_a', (SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
                         WHERE e.entity_id=:'urn_a'),
          'temporal_b', (SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
                         WHERE e.entity_id=:'urn_b'));
        """, a=specs[0].gateway_id, b=specs[1].gateway_id,
             urn_a=specs[0].sensor_urn, urn_b=specs[1].sensor_urn))
        return result

    def database_fingerprint(gateway_id):
        return json.loads(sql("""SELECT json_build_object(
          'ledger', md5(coalesce((SELECT string_agg(to_jsonb(p)::text, '' ORDER BY p.message_id)
            FROM processed_messages p WHERE p.gateway_id=:'gw'), '')),
          'raw', md5(coalesce((SELECT string_agg(to_jsonb(t)::text, '' ORDER BY t.message_id,t.sample_index)
            FROM telemetry t WHERE t.gateway_id=:'gw'), '')),
          'temporal', md5(coalesce((SELECT string_agg(to_jsonb(v)::text, '' ORDER BY v.message_id,v.property_name)
            FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
            WHERE e.entity_id=:'urn'), '')),
          'state', (SELECT to_jsonb(s)::text FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id
                    WHERE e.entity_id=:'urn'));
        """, gw=gateway_id, urn=specs[1].sensor_urn))

    def scrape(phase):
        if len(metrics) >= 256 or len(resource_rows) >= 256:
            raise ValueError('paper_p4_metrics_window_rejected')
        state.checkpoint()
        status, _, snapshot = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=tokens['admin'])
        if status != 200 or not isinstance(snapshot, dict) or not isinstance(snapshot.get('ingress'), dict):
            raise ValueError('paper_p4_metrics_unavailable')
        row = {'phase': phase, 'observed_monotonic': time.monotonic(), 'snapshot': snapshot}
        metrics.append(row)
        smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', metrics)
        resource = smoke.paper_p0_stats(registry, containers, phase)
        gateways = []
        for item in registry.items:
            if item['kind'] != 'container' or '-gateway-p4-' not in item['name']:
                continue
            obj = registry.resolve(item)
            if obj is not None and obj['State']['Running']:
                gateways.append((item['name'], obj['Id']))
        if gateways:
            result = registry.command(['stats', '--no-stream', '--format', '{{json .}}',
                                       *[container_id for _, container_id in gateways]])
            if result.returncode:
                raise ValueError('paper_p4_stats_failed')
            rows = {row['Name']: row for row in (json.loads(line) for line in result.stdout.splitlines())}
            if len(rows) != len(gateways):
                raise ValueError('paper_p4_stats_failed')
            resource['services'].update({name: {key: rows[name][key]
                for key in ('CPUPerc', 'MemUsage', 'MemPerc')} for name, _ in gateways})
        resource_rows.append(resource)
        limits = {name: values for name, values in resources.items()}
        for sample in resource_rows:
            for service in sample['services']:
                limits.setdefault(service, {})
        smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv', resource_rows, limits)
        return row

    def checkpoint(phase):
        row = scrape(phase)
        phase_row = {'name': phase, 'monotonic_seconds': row['observed_monotonic'] - started,
                     'process_instance': row['snapshot'].get('process_instance'),
                     'elapsed_seconds': row['snapshot'].get('elapsed_seconds'),
                     'gateway_counts': counts(phase)}
        run_data['phases'].append(phase_row)
        persist()
        return row

    def delta(before, after, name):
        if (before['snapshot'].get('process_instance') != after['snapshot'].get('process_instance')
                or after['snapshot'].get('elapsed_seconds', 0) <= before['snapshot'].get('elapsed_seconds', 0)
                or after['observed_monotonic'] <= before['observed_monotonic']):
            raise ValueError('paper_p4_metrics_window_rejected')
        try:
            value = after['snapshot']['ingress'][name] - before['snapshot']['ingress'][name]
        except (KeyError, TypeError):
            raise ValueError('paper_p4_metrics_unavailable') from None
        if type(value) is not int or value < 0:
            raise ValueError('paper_p4_metrics_unavailable')
        return value

    labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
    volumes = {}
    for role in ('a', 'b'):
        volume = registry.intent('volume', fixture.project + '_outbox_p4_' + role, None, labels)
        if registry.resolve(volume) is not None:
            raise ValueError('resource_collision')
        registry.allocating(volume)
        created = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, volume['name']])
        if created.returncode or registry.resolve(volume) is None:
            raise ValueError('helper_allocation_unresolved')
        volumes[role] = volume

    def env_for(role, stage):
        spec = specs[0 if role == 'a' else 1]
        env = uplink.gateway_env(spec.gateway_id, passwords[0 if role == 'a' else 1])
        env.update(GATEWAY_MQTT_CLIENT_ID=fixture.project + '_p4_' + role + '_' + stage,
                   GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883')
        if stage == 'burst' or stage == 'drain':
            env.update(GATEWAY_SAMPLE_RATE_HZ=str(config['burst_sample_rate_hz']),
                       GATEWAY_SAMPLES_PER_BATCH=str(config['burst_samples_per_batch']))
        path = fixture.private / ('gateway-p4-' + role + '-' + stage + '.env')
        smoke.private_write(path, ''.join(key + '=' + value + '\n' for key, value in env.items()))
        return path

    def gateway(role, stage, args, *, during=None, tick=None, timeout=45, allow_incomplete=False):
        tag = 'p4-' + role + '-' + stage
        return smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[role],
            env_for(role, stage), fixture.private / 'certs/ca.crt', tag, args, state,
            during=during, tick=tick, timeout=timeout, allow_incomplete=allow_incomplete)

    def summary(output, batches, completed=True):
        return _expected_summary(smoke, output, batches=batches, completed=completed)

    def gateway_count(role, stage):
        output = gateway(role, stage, ['count'])
        if output.strip() != 'pending_rows=0 pending_bytes=0 quarantined_rows=0 quarantined_bytes=0':
            raise ValueError('paper_p4_outbox_not_drained')
        return output

    def stopped_pending_snapshot():
        """Copy only a STOPPED Gateway volume; SQLite/WAL state is then stable."""
        item = next(item for item in registry.items if item['name'] == fixture.project + '-gateway-p4-b-burst')
        obj = registry.resolve(item)
        if obj is None or obj['State']['Running']:
            raise ValueError('paper_p4_snapshot_not_stopped')
        target = fixture.private / 'p4-burst-outbox'
        target.mkdir(mode=0o700, exist_ok=True)
        copied = registry.command(['cp', obj['Id'] + ':/data/outbox/.', str(target)])
        if copied.returncode:
            raise ValueError('paper_p4_pending_snapshot_failed')
        db_path = target / 'outbox.db'
        if not db_path.is_file():
            raise ValueError('paper_p4_pending_snapshot_failed')
        with sqlite3.connect('file:' + str(db_path) + '?mode=ro', uri=True, timeout=3) as db:
            if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
                raise ValueError('paper_p4_pending_snapshot_failed')
            rows = db.execute("SELECT message_id,payload FROM outbox WHERE status='pending' ORDER BY id").fetchall()
            counts_row = db.execute("SELECT count(*),coalesce(sum(length(payload)),0) FROM outbox WHERE status='pending'").fetchone()
        return [{'message_id': str(message_id), 'sha256': hashlib.sha256(payload).hexdigest(),
                 'payload_length_bytes': len(payload)} for message_id, payload in rows], counts_row

    checkpoint('before_workload')
    counts('before_workload')
    # A's finite CLI stays live while every B phase runs synchronously inside its
    # start callback. This avoids mutating the exact-owned Registry concurrently.
    phase_data = {}
    observed_b_payload = [None]
    observer = ack_observer = None
    try:
        observer = smoke.paper_p1_subscriber(fixture, registry,
            containers['mosquitto']['receipt']['id'], specs[1], passwords[1], 'telemetry',
            message_count=min(config['offered_unique_messages'], 100), message_timeout=config['run_deadline_seconds'])
        ack_observer = smoke.paper_p1_subscriber(fixture, registry,
            containers['mosquitto']['receipt']['id'], specs[0], passwords[0], 'ack',
            message_count=min(config['offered_unique_messages'], 100), message_timeout=config['run_deadline_seconds'])
    except BaseException:
        if observer is not None:
            smoke.paper_p1_stop_subscription(observer)
        raise

    def run_b_stages():
        checkpoint('b_steady_start')
        steady = gateway('b', 'steady', ['run', '--batches', str(config['b_steady_batches']),
                                           '--completion-timeout', '20s'])
        phase_data['b_steady'] = summary(steady, config['b_steady_batches'])
        phase_data['b_steady_end'] = checkpoint('b_steady_end')
        b_topic = 'gateways/' + specs[1].gateway_id + '/telemetry/' + specs[1].sensor_id
        observed_b_payload[0] = smoke.paper_p1_capture_subscription(observer, b_topic, timeout=8)
        if observed_b_payload[0] is None:
            raise ValueError('paper_p4_observer_capture_failed')
        observed_message = json.loads(observed_b_payload[0])
        if not isinstance(observed_message.get('message_id'), str):
            raise ValueError('paper_p4_observer_capture_failed')
        committed_hash = sql("SELECT payload_hash FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id';",
                             gw=specs[1].gateway_id, id=observed_message['message_id'])
        check('own_namespace_payload_captured', bool(committed_hash), gateway='B',
              message_id=observed_message['message_id'], payload_bytes=len(observed_b_payload[0]),
              payload_sha256=hashlib.sha256(observed_b_payload[0]).hexdigest(), committed=True)
        smoke.paper_p1_stop_subscription(observer)

        a_before_ids = set(json.loads(sql("""SELECT coalesce(json_agg(message_id::text ORDER BY message_id),'[]'::json)
            FROM processed_messages WHERE gateway_id=:'gw';""", gw=specs[0].gateway_id)))
        window_start = time.monotonic()
        burst_before = checkpoint('b_burst_start')
        a_before = counts('a_progress_start')['a']['messages']
        last_scrape = [0.0]
        a_alive = [True]
        a_ack_observed = [None]
        a_item = next(item for item in registry.items
                      if item['name'] == fixture.project + '-gateway-p4-a-steady')

        def burst_tick():
            active_a = registry.resolve(a_item)
            a_alive[0] = a_alive[0] and active_a is not None and active_a['State']['Running']
            now = time.monotonic()
            if now - last_scrape[0] >= 1.0:
                last_scrape[0] = now
                checkpoint('b_burst_active')
                current_ids = set(json.loads(sql("""SELECT coalesce(json_agg(message_id::text ORDER BY message_id),'[]'::json)
                    FROM processed_messages WHERE gateway_id=:'gw';""", gw=specs[0].gateway_id)))
                new_ids = current_ids - a_before_ids
                for topic, payload in smoke.paper_p1_subscription_log_messages(ack_observer):
                    if topic.startswith('gateways/' + specs[0].gateway_id + '/acks/'):
                        try:
                            ack_message_id = json.loads(payload).get('message_id')
                        except (ValueError, AttributeError):
                            continue
                        if ack_message_id in new_ids:
                            from stage3_mqtt_ingress_runner import verify_ack
                            verify_ack(ack_message_id, payload)
                            a_ack_observed[0] = {'message_id': ack_message_id,
                                'observed_monotonic': time.monotonic(),
                                'window_start': window_start,
                                'process_instance': burst_before['snapshot'].get('process_instance')}
                            break

        timeout = str(config['optimized_burst_completion_seconds']) + 's'
        burst = gateway('b', 'burst', ['run', '--batches', str(config['b_burst_batches']),
                                       '--completion-timeout', timeout], tick=burst_tick,
                        timeout=20, allow_incomplete=True)
        burst_summary = _expected_summary(smoke, burst, batches=config['b_burst_batches'], completed=None)
        phase_data['b_burst_summary'] = burst_summary
        phase_data['b_burst_end'] = checkpoint('b_burst_end')
        a_after = counts('a_progress_end')['a']['messages']
        burst_rejects = delta(burst_before, phase_data['b_burst_end'], 'ShaperRateLimited')
        check('a_progress_during_b_burst', a_after > a_before,
              a_messages_before=a_before, a_messages_after=a_after,
              window_seconds=phase_data['b_burst_end']['observed_monotonic'] - burst_before['observed_monotonic'])
        check('a_application_ack_during_b_burst', a_ack_observed[0] is not None
              and a_ack_observed[0]['observed_monotonic'] >= window_start
              and a_ack_observed[0]['observed_monotonic'] <= phase_data['b_burst_end']['observed_monotonic'],
              ack_observation=a_ack_observed[0], a_new_commits=len(set(json.loads(sql(
                  "SELECT coalesce(json_agg(message_id::text),'[]'::json) FROM processed_messages WHERE gateway_id=:'gw';",
                  gw=specs[0].gateway_id))) - a_before_ids))
        check('a_container_active_during_b_burst', a_alive[0],
              checked_ticks='bounded Gateway CLI polling ticks')
        check('shaper_profile_response', burst_rejects > 0 if shaped else burst_rejects == 0,
              mode=mode, shaper_enabled=shaped, rate_limited_delta=burst_rejects,
              shaper_pass_delta=delta(burst_before, phase_data['b_burst_end'], 'ShaperPassed'),
              accepted_delta=delta(burst_before, phase_data['b_burst_end'], 'Accepted'))

        pending, pending_totals = stopped_pending_snapshot()
        phase_data['pending_snapshot'] = {'rows': len(pending), 'bytes': pending_totals[1],
                                          'items': pending,
                                          'authority': 'stopped SQLite DB plus any copied WAL/SHM files'}
        if not pending:
            run_data.setdefault('unavailable_observations', []).append(
                'duplicate replay payload unavailable: no pending B row in stopped baseline outbox')
        if shaped:
            check('rejected_traffic_remains_pending', pending_totals[0] > 0 and bool(pending),
                  pending_rows=pending_totals[0], pending_bytes=pending_totals[1])
        else:
            check('baseline_burst_state_recorded',
                  (burst_summary['completed'] == 'true' and pending_totals == (0, 0))
                  or (burst_summary['completed'] == 'false' and pending_totals[0] > 0),
                  completed=burst_summary['completed'], pending_rows=pending_totals[0],
                  pending_bytes=pending_totals[1])

        # A's valid credential must not publish an observed, valid B payload into
        # B's otherwise valid topic. Compare B only; A remains active and commits.
        broker_id = containers['mosquitto']['receipt']['id']
        logs_before = uplink.container_logs(broker_id)
        b_before_acl = database_fingerprint(specs[1].gateway_id)
        denied = registry.command(['exec', '-i', containers['mosquitto']['receipt']['id'], 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(specs[0].gateway_id + '\n' + passwords[0] + '\n'
                   + 'gateways/' + specs[1].gateway_id + '/telemetry/' + specs[1].sensor_id + '\n').encode()
                   + observed_b_payload[0])
        logs_after = uplink.wait_denied_publish(broker_id, logs_before,
            'gateways/' + specs[1].gateway_id + '/telemetry/' + specs[1].sensor_id,
            uplink.PROBE_DENY_SETTLE)
        broker_rejected = broker_denial_observed(logs_before, logs_after,
            'gateways/' + specs[1].gateway_id + '/telemetry/' + specs[1].sensor_id)
        b_after_acl = database_fingerprint(specs[1].gateway_id)
        check('wrong_namespace_denied', broker_rejected and b_before_acl == b_after_acl,
              broker_rejected=broker_rejected, client_exit_code=denied.returncode,
              target_gateway_b_database_unchanged=b_before_acl == b_after_acl,
              payload_sha256=hashlib.sha256(observed_b_payload[0]).hexdigest())

        checkpoint('b_retry_restart')
        retry_out = gateway('b', 'drain', ['run', '--batches', str(config['b_drain_batches']),
                                           '--completion-timeout', str(config['drain_deadline_seconds']) + 's'],
                           timeout=config['run_deadline_seconds'])
        phase_data['b_drain_summary'] = summary(retry_out, config['b_drain_batches'])
        drained = checkpoint('b_drained')
        count = gateway_count('b', 'final-count')
        phase_data['b_count'] = count.strip()
        if pending:
            ids = [item['message_id'] for item in pending]
            hashes = json.loads(sql("""SELECT coalesce(json_agg(json_build_object('message_id',message_id::text,
              'payload_hash',payload_hash) ORDER BY message_id),'[]'::json)
              FROM processed_messages WHERE gateway_id=:'gw' AND message_id = ANY(:'ids'::uuid[]);""",
              gw=specs[1].gateway_id, ids='{' + ','.join(ids) + '}'))
            hash_by_id = {row['message_id']: row['payload_hash'] for row in hashes}
            check('pending_retry_byte_exact', len(hash_by_id) == len(pending) and all(
                hash_by_id.get(item['message_id']) == item['sha256'] for item in pending),
                pending_rows=len(pending), verified_hashes=len(hash_by_id),
                proof='SHA256(stopped SQLite payload) equals committed processed_messages.payload_hash')
        phase_data['drained_metrics'] = drained
        before_duplicate = checkpoint('before_duplicate_replay')
        duplicate_message = json.loads(observed_b_payload[0])['message_id']
        if sql("SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'id';",
               gw=specs[1].gateway_id, id=duplicate_message) != '1':
            raise ValueError('paper_p4_duplicate_replay_failed')
        before_duplicate_fingerprint = database_fingerprint(specs[1].gateway_id)
        duplicate_pub = registry.command(['exec', '-i', containers['mosquitto']['receipt']['id'], 'sh', '-ec',
                'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
                'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
                '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
                input=(specs[1].gateway_id + '\n' + passwords[1] + '\n'
                       + 'gateways/' + specs[1].gateway_id + '/telemetry/' + specs[1].sensor_id + '\n').encode()
                      + observed_b_payload[0])
        if duplicate_pub.returncode:
            raise ValueError('paper_p4_duplicate_replay_failed')
        duplicate_after = None
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            state.checkpoint()
            candidate = scrape('duplicate_replay')
            try:
                if candidate['snapshot']['ingress']['DuplicateCommitted'] > before_duplicate['snapshot']['ingress']['DuplicateCommitted']:
                    duplicate_after = candidate
                    break
            except (KeyError, TypeError):
                raise ValueError('paper_p4_metrics_unavailable') from None
            time.sleep(0.25)
        after_duplicate_fingerprint = database_fingerprint(specs[1].gateway_id)
        duplicate_delta = delta(before_duplicate, duplicate_after, 'DuplicateCommitted') if duplicate_after else 0
        duplicate_ack_delta = delta(before_duplicate, duplicate_after, 'ACKPublished') if duplicate_after else 0
        check('duplicate_replay_deduplicated', duplicate_after is not None
              and before_duplicate_fingerprint == after_duplicate_fingerprint
              and duplicate_delta > 0 and duplicate_ack_delta > 0,
              target_gateway_b_database_unchanged=before_duplicate_fingerprint == after_duplicate_fingerprint,
              duplicate_delta=duplicate_delta, ack_published_delta=duplicate_ack_delta,
              payload_sha256=hashlib.sha256(observed_b_payload[0]).hexdigest(),
              application_ack='backend published only after committed duplicate recognition')
        if time.monotonic() - started > config['run_deadline_seconds']:
            raise ValueError('paper_p4_deadline_exceeded')

    try:
        a_output = gateway('a', 'steady', ['run', '--batches', str(config['a_batches']),
                                           '--completion-timeout', '75s'],
                           during=run_b_stages, timeout=config['run_deadline_seconds'])
    finally:
        if observer is not None:
            smoke.paper_p1_stop_subscription(observer)
        if ack_observer is not None:
            smoke.paper_p1_stop_subscription(ack_observer)
    a_summary = summary(a_output, config['a_batches'])
    phase_data['a_summary'] = a_summary
    checkpoint('a_drained')
    gateway_count('a', 'final-count')
    if time.monotonic() - started > config['run_deadline_seconds']:
        raise ValueError('paper_p4_deadline_exceeded')

    # Independent per-message provenance and variable sample-count reconciliation.
    ledgers = json.loads(sql("""SELECT coalesce(json_agg(json_build_object('gateway_id',gateway_id,
       'message_id',message_id::text,'sample_count',sample_count) ORDER BY gateway_id,message_id),'[]'::json)
       FROM processed_messages WHERE gateway_id IN (:'a',:'b');""",
       a=specs[0].gateway_id, b=specs[1].gateway_id))
    raw_mismatch = sql("""SELECT count(*) FROM processed_messages p WHERE p.gateway_id IN (:'a',:'b')
       AND (SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id) <> p.sample_count;""",
       a=specs[0].gateway_id, b=specs[1].gateway_id)
    provenance_mismatch = sql("""SELECT count(*) FROM processed_messages p WHERE p.gateway_id IN (:'a',:'b')
        AND (p.first_sequence <> (SELECT min(t.sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
          OR p.last_sequence <> (SELECT max(t.sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
          OR EXISTS (SELECT 1 FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id
            AND (t.sample_index <> t.sequence_num-p.first_sequence OR t.boot_id <> p.boot_id
              OR t.measured_at < (SELECT min(t0.measured_at) FROM telemetry t0
                    WHERE t0.gateway_id=p.gateway_id AND t0.message_id=p.message_id)
                 OR t.measured_at > (SELECT min(t0.measured_at) FROM telemetry t0
                    WHERE t0.gateway_id=p.gateway_id AND t0.message_id=p.message_id)
                    + (p.sample_count-1)*10000*interval '1 microsecond'))
          OR (SELECT max(t.measured_at)-min(t.measured_at) FROM telemetry t
              WHERE t.gateway_id=p.gateway_id AND t.message_id=p.message_id)
              <> (p.sample_count-1)*10000*interval '1 microsecond');""",
        a=specs[0].gateway_id, b=specs[1].gateway_id)
    raw_values = json.loads(sql("""SELECT coalesce(json_agg(json_build_array(sequence_num,value)
        ORDER BY gateway_id,message_id,sample_index),'[]'::json) FROM telemetry
        WHERE gateway_id IN (:'a',:'b');""", a=specs[0].gateway_id, b=specs[1].gateway_id))
    synthetic_mismatch = sum(abs(float(value) - uplink.synthetic_value(int(sequence))) > 1e-9
                             for sequence, value in raw_values)
    final_counts = counts('final')
    check('unique_ledgers_and_raw_samples', len(ledgers) == config['offered_unique_messages']
          and raw_mismatch == '0' and provenance_mismatch == '0' and synthetic_mismatch == 0
          and final_counts['a'] == {'messages': config['a_batches'],
                                    'samples': config['a_batches'] * config['steady_samples_per_batch']}
          and final_counts['b'] == {'messages': config['b_steady_batches'] + config['b_burst_batches'] + config['b_drain_batches'],
                                    'samples': config['b_steady_batches'] * config['steady_samples_per_batch']
                                    + (config['b_burst_batches'] + config['b_drain_batches']) * config['burst_samples_per_batch']},
          ledger_count=len(ledgers), a=final_counts['a'], b=final_counts['b'],
          raw_mismatch=raw_mismatch, provenance_mismatch=provenance_mismatch,
          synthetic_mismatch=synthetic_mismatch)
    check('temporal_per_message', final_counts['temporal_a'] == config['a_batches']
          and final_counts['temporal_b'] == config['b_steady_batches'] + config['b_burst_batches'] + config['b_drain_batches'],
          temporal_a=final_counts['temporal_a'], temporal_b=final_counts['temporal_b'])
    check('real_gateway_summaries_and_empty_outboxes',
          a_summary['app_acknowledged'] == str(config['a_batches'])
          and phase_data['b_steady']['app_acknowledged'] == str(config['b_steady_batches'])
          and int(phase_data['b_drain_summary']['app_acknowledged']) >=
              phase_data['pending_snapshot']['rows'] + config['b_drain_batches']
          and phase_data['b_count'].startswith('pending_rows=0 pending_bytes=0'),
          a_summary=a_summary, b_steady=phase_data['b_steady'], b_drain=phase_data['b_drain_summary'])
    check('rs256_https_and_namespace_control', run_data['https_verified']
          and all(smoke.paper_p2_support().decode_token(token)[0].get('alg') == 'RS256'
                  for token in tokens.values()), auth_mode='RS256', https=run_data['https_verified'])

    # The provisioned B simulator's actual TLS publish is the positive own-topic
    # bracket. Reuse one committed B payload for a wrong-namespace ACL denial;
    # credentials enter only on stdin and MQTT output is not persisted.
    # Both real outboxes provide positive own-topic TLS publishes. The negative
    # cross-namespace publish is explicitly verified above.
    run_data['traffic_accounting'] = {
        'offered_unique': config['offered_unique_messages'],
        'committed_unique': len(ledgers),
        'duplicate': 0,
        'retries': None,
        'gateway_publish_attempts': None,
        'pending_rows': 0,
        'pending_bytes': 0,
        'wire_bytes': None,
        'duplicate_and_wire_note': 'not separately observable by this bounded CLI/fixture surface',
    }
    ingress_first, ingress_last = metrics[0]['snapshot']['ingress'], metrics[-1]['snapshot']['ingress']
    run_data['traffic_accounting'].update({
        'backend_attempts': ingress_last.get('Received') - ingress_first.get('Received'),
        'admitted': ingress_last.get('Accepted') - ingress_first.get('Accepted'),
        'committed_metric': ingress_last.get('Committed') - ingress_first.get('Committed'),
        'duplicate_metric': ingress_last.get('DuplicateCommitted') - ingress_first.get('DuplicateCommitted'),
        'rejected_metric': ingress_last.get('ShaperRateLimited') - ingress_first.get('ShaperRateLimited'),
        'shaper_pass_metric': ingress_last.get('ShaperPassed') - ingress_first.get('ShaperPassed'),
        'application_ack_metric': ingress_last.get('ACKPublished') - ingress_first.get('ACKPublished'),
        'actual_payload_bytes': ingress_last.get('ReceivedBytes') - ingress_first.get('ReceivedBytes'),
    })
    run_data['traffic_accounting']['duplicate'] = run_data['traffic_accounting']['duplicate_metric']
    run_data['traffic_accounting']['rejected'] = run_data['traffic_accounting']['rejected_metric']
    # Wire bytes and payload bytes are distinct; no MQTT wire counter is exported.
    gateway_attempts = sum(int(summary_row['publish_attempts']) for summary_row in (
        phase_data['a_summary'], phase_data['b_steady'], phase_data['b_burst_summary'],
        phase_data['b_drain_summary']))
    run_data['traffic_accounting']['gateway_publish_attempts'] = gateway_attempts
    run_data['traffic_accounting']['retries'] = max(0, gateway_attempts - config['offered_unique_messages'])
    run_data['traffic_accounting']['retry_count_basis'] = 'Gateway CLI publish_attempts minus unique generated batches'
    backend_attempts = run_data['traffic_accounting']['backend_attempts']
    validate_accounting({
        'offered_unique': config['offered_unique_messages'],
        'backend_attempts': backend_attempts,
        'committed_unique': len(ledgers),
        'duplicate': run_data['traffic_accounting']['duplicate_metric'],
        'rejected': run_data['traffic_accounting']['rejected_metric'],
        'pending_rows': 0, 'pending_bytes': 0, 'wire_bytes': None,
    })
    run_data['status'] = 'PASS'
    run_data['full_p4_accepted'] = acceptance_ready(run_data['checks'], mode)
    if not run_data['full_p4_accepted']:
        raise ValueError('paper_p4_acceptance_failed')
    persist()
    return run_data
