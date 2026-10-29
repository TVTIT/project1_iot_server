"""Finite P8.3a3 slow-viewer probe using the isolated P2 perimeter fixture."""
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import time
import uuid


# The initial 200-batch profile completed without a server-side slow close; the
# lane therefore uses the explicitly permitted 400-batch finite upper profile.
BATCHES_A = 400
SAMPLES_PER_BATCH = 1000
BATCHES_B = 2
WAVE_SIZE = 10
LIFETIME_MS = 150000


def profile():
    """Opt-in P8 runtime; all six deployment feature flags remain disabled."""
    from paper_p8_support import profile as base_profile
    return {**base_profile(), 'WS_HEARTBEAT_INTERVAL': '2s', 'WS_PONG_TIMEOUT': '4s',
            'WS_WRITE_TIMEOUT': '1s'}


def expected_counts(*, batches_a=BATCHES_A, batches_b=BATCHES_B):
    if type(batches_a) is not int or not 0 <= batches_a <= 400 or type(batches_b) is not int or not 0 <= batches_b <= 100:
        raise ValueError('p8_slow_finite_budget_rejected')
    return {'A': batches_a, 'B': batches_b,
            'raw_A': batches_a * SAMPLES_PER_BATCH, 'raw_B': batches_b * SAMPLES_PER_BATCH}


def validate_peer_state(state, *, batches_a=BATCHES_A, batches_b=BATCHES_B):
    expected = expected_counts(batches_a=batches_a, batches_b=batches_b)
    peers = state.get('peers') if isinstance(state, dict) else None
    if not isinstance(peers, list) or len(peers) != 2:
        raise ValueError('p8_slow_healthy_peer_set_rejected')
    by_role = {peer.get('role'): peer for peer in peers}
    if set(by_role) != {'A', 'B'}:
        raise ValueError('p8_slow_healthy_peer_roles_rejected')
    for role, count in (('A', batches_a), ('B', batches_b)):
        peer = by_role[role]
        if (peer.get('unique_events') != count or peer.get('events') != count
                or peer.get('duplicates') != 0 or peer.get('unexpected_namespace') != 0
                or peer.get('closed') is True):
            raise ValueError('p8_slow_healthy_peer_counts_rejected')
    if state.get('failure') or state.get('events') != batches_a + batches_b:
        raise ValueError('p8_slow_healthy_peer_state_rejected')
    return {'expected': {'A': expected['A'], 'B': expected['B']},
            'actual': {role: by_role[role]['unique_events'] for role in ('A', 'B')}}


def stop_child(proc, timeout=5):
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=timeout)


def run(smoke, fixture, endpoint, registry, containers, images, lifecycle_state):
    import paper_p7_support as p7
    import paper_p8_support as p8

    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, lifecycle_state))
    env_cfg = effective.get('services', {}).get('backend', {}).get('environment', {})
    if any(str(env_cfg.get(k)) != v for k, v in profile().items()):
        raise ValueError('p8_slow_effective_profile_rejected')
    schema = smoke.db_psql(registry, pg,
        'SELECT version::text || \':\' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;').strip()
    if schema != '24:telemetry_ledger_timestamp' or len(specs) < 2:
        raise ValueError('p8_slow_schema_or_targets_rejected')

    target_a, target_b = specs[0], specs[1]
    ws = endpoint.url.replace('https://', 'wss://').rstrip('/')
    if not ws.endswith('/v1/ws'):
        ws += '/v1/ws'
    probe_dir = fixture.private / 'p8-slow'
    probe_dir.mkdir(mode=0o700)
    probe_dir.chmod(0o700)
    healthy_dir = probe_dir / 'healthy'
    healthy_dir.mkdir(mode=0o700)
    healthy_cfg = {
        'wss_endpoint': ws, 'ca_file': str(endpoint.ca_file),
        'role_jwts': {'A': tokens['a'], 'B': tokens['b']},
        'peers': [
            {'id': 'healthy-a', 'role': 'A', 'target': {'gateway_id': target_a.gateway_id, 'sensor_id': target_a.sensor_id}},
            {'id': 'healthy-b', 'role': 'B', 'target': {'gateway_id': target_b.gateway_id, 'sensor_id': target_b.sensor_id}},
        ],
        'max_lifetime_ms': LIFETIME_MS, 'max_bytes': 64 * 1024 * 1024,
        'max_events': BATCHES_A + BATCHES_B + 10,
        'ready_path': str(healthy_dir / 'ready.json'), 'state_path': str(healthy_dir / 'state.json'),
        'stop_path': str(healthy_dir / 'stop'), 'output_path': str(healthy_dir / 'result.json'),
    }
    private_json(smoke, healthy_dir / 'config.json', healthy_cfg)

    slow_dir = probe_dir / 'slow'
    slow_dir.mkdir(mode=0o700)
    slow_dir.chmod(0o700)
    slow_cfg = p7.slow_viewer_probe_config(ws, endpoint.ca_file, tokens['a'],
        {'gateway_id': target_a.gateway_id, 'sensor_id': target_a.sensor_id}, slow_dir)
    # The Go contract's documented bounds allow a shorter opt-in finite lifetime.
    slow_cfg['max_lifetime_ms'] = LIFETIME_MS
    private_json(smoke, slow_dir / 'config.json', slow_cfg)

    children, logs, subscriptions = [], [], []
    primary = None
    cleanup_errors = []
    result = {'status': 'FAIL', 'full_p8_accepted': False, 'qualification': 'three peers only; not 64-session acceptance'}
    try:
        for env_name, config_path, test_name, log_name in (
            ('P8_HEALTHY_PEERS_CONFIG_FILE', healthy_dir / 'config.json', 'TestNativeP8HealthyPeersProbe', 'healthy.log'),
            ('P7_SLOW_VIEWER_CONFIG_FILE', slow_dir / 'config.json', 'TestNativeSlowViewerProbe', 'slow.log'),
        ):
            fd = os.open(probe_dir / log_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            log = os.fdopen(fd, 'wb')
            logs.append(log)
            child_env = os.environ.copy()
            child_env[env_name] = str(config_path)
            child = subprocess.Popen(['go', 'test', './internal/telemetryws', '-run', '^' + test_name + '$',
                                      '-count=1', '-timeout=180s'], cwd=Path(smoke.__file__).parents[2] / 'src',
                                     env=child_env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            children.append(child)
        wait_file(Path(healthy_cfg['ready_path']), children[0], 50)
        wait_file(Path(slow_cfg['ready_path']), children[1], 50)

        before = ws_metrics(smoke, endpoint, tokens['admin'])
        if before.get('sessions') != 3:
            raise ValueError('p8_slow_peer_baseline_rejected')
        broker = containers['mosquitto']['receipt']['id']
        payloads_a = p7.build_slow_viewer_payloads(target_a.gateway_id, target_a.sensor_id,
            str(uuid.uuid4()), start_sequence=int(time.time()) * 1_000_000, message_count=BATCHES_A)
        payloads_b = p7.build_slow_viewer_payloads(target_b.gateway_id, target_b.sensor_id,
            str(uuid.uuid4()), start_sequence=int(time.time()) * 1_000_000 + 500_000_000,
            message_count=BATCHES_B)
        acked = {'A': 0, 'B': 0}
        for role, spec, payloads, password in (
            ('A', target_a, payloads_a, passwords[0]), ('B', target_b, payloads_b, passwords[1])):
            for start in range(0, len(payloads), WAVE_SIZE):
                wave = payloads[start:start + WAVE_SIZE]
                ids = [json.loads(item)['message_id'] for item in wave]
                tag = f'p8slow-{role.lower()}-{start:04d}-{secrets.token_hex(4)}'
                sub = smoke.paper_p1_subscriber(fixture, registry, broker, spec, password, tag,
                    message_count=len(wave), message_timeout=60, subscription_kind='ack')
                subscriptions.append(sub)
                client_id = f'{fixture.project}_{tag}'
                command = ['exec', '-i', broker, 'sh', '-ec',
                    'IFS= read -r user; IFS= read -r password; IFS= read -r client; IFS= read -r topic; exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt -u "$user" -P "$password" -i "$client" -q 1 -t "$topic" -l']
                preamble = (spec.gateway_id + '\n' + password + '\n' + client_id + '\n' +
                    'gateways/' + spec.gateway_id + '/telemetry/' + spec.sensor_id + '\n').encode()
                published = registry.command(command, input=preamble + b''.join(x + b'\n' for x in wave))
                if published.returncode:
                    raise ValueError('p8_slow_manual_publish_failed')
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    messages = smoke.paper_p1_subscription_log_messages(sub, max_bytes=2_000_000)
                    received = p7.validate_slow_ack_messages(messages, ids, spec.gateway_id, spec.sensor_id)
                    if set(received) == set(ids):
                        acked[role] += len(received)
                        break
                    time.sleep(0.1)
                else:
                    raise ValueError('p8_slow_application_ack_timeout')
                smoke.paper_p1_stop_subscription(sub)
                subscriptions.remove(sub)
                time.sleep(0.05)

        # A message-ID replay must return an application ACK but not another event or DB row.
        pre_replay_state = wait_peer_counts(Path(healthy_cfg['state_path']), children[0], BATCHES_A, BATCHES_B, 20)
        pre_replay_db = db_counts(smoke, registry, pg, (target_a, target_b))
        replay = payloads_b[0]
        replay_id = json.loads(replay)['message_id']
        replay_tag = 'p8slow-replay-' + secrets.token_hex(4)
        replay_sub = smoke.paper_p1_subscriber(fixture, registry, broker, target_b, passwords[1], replay_tag,
            message_count=1, message_timeout=60, subscription_kind='ack')
        subscriptions.append(replay_sub)
        replay_client = f'{fixture.project}_{replay_tag}'
        replay_command = ['exec', '-i', broker, 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r client; IFS= read -r topic; exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt -u "$user" -P "$password" -i "$client" -q 1 -t "$topic" -l']
        preamble = (target_b.gateway_id + '\n' + passwords[1] + '\n' + replay_client + '\n' +
                    'gateways/' + target_b.gateway_id + '/telemetry/' + target_b.sensor_id + '\n').encode()
        if registry.command(replay_command, input=preamble + replay + b'\n').returncode:
            raise ValueError('p8_slow_replay_publish_failed')
        replay_ack = smoke.paper_p1_capture_subscription(replay_sub,
            'gateways/' + target_b.gateway_id + '/acks/' + replay_id, timeout=30)
        if not replay_ack:
            raise ValueError('p8_slow_duplicate_ack_missing')
        p7.validate_slow_ack_messages([('gateways/' + target_b.gateway_id + '/acks/' + replay_id, replay_ack)],
                                      [replay_id], target_b.gateway_id, target_b.sensor_id)
        time.sleep(1)
        post_replay_state = json.loads(Path(healthy_cfg['state_path']).read_text())
        post_replay_db = db_counts(smoke, registry, pg, (target_a, target_b))
        if (post_replay_state.get('events') != pre_replay_state.get('events')
                or post_replay_state.get('unique_events') != pre_replay_state.get('unique_events')
                or post_replay_db != pre_replay_db):
            raise ValueError('p8_slow_duplicate_replay_mutated_state')
        smoke.paper_p1_stop_subscription(replay_sub)
        subscriptions.remove(replay_sub)

        # Wait for actual policy closure while slow client remains paused, then resume it to read close.
        closed_at = wait_slow_close(smoke, endpoint, tokens['admin'], before, 20)
        touch_private(Path(slow_cfg['resume_path']))
        slow_result = wait_json(Path(slow_cfg['output_path']), children[1], 20)
        if slow_result.get('outcome') not in ('server_closed', 'transport_closed'):
            raise ValueError('p8_slow_close_observation_rejected')
        healthy_state = wait_peer_counts(Path(healthy_cfg['state_path']), children[0], BATCHES_A, BATCHES_B, 20)
        delivery = validate_peer_state(healthy_state)
        db = db_counts(smoke, registry, pg, (target_a, target_b))
        wanted_db = {'A': {'ledger': 400, 'raw': 400000, 'temporal': 400},
                     'B': {'ledger': 2, 'raw': 2000, 'temporal': 2}}
        if db != wanted_db:
            raise ValueError('p8_slow_database_counts_rejected')
        smoke.private_write(Path(healthy_cfg['stop_path']), '')
        children[0].wait(timeout=15)
        if children[0].returncode:
            raise ValueError('p8_healthy_probe_failed')
        final_metrics = ws_metrics(smoke, endpoint, tokens['admin'])
        if any(final_metrics.get(key) != 0 for key in
               ('sessions', 'subscriptions', 'session_reference_bytes', 'session_queued_in_flight_bytes')):
            raise ValueError('p8_slow_shutdown_gauges_nonzero')
        result.update(status='PASS', database_counts=db, healthy_delivery=delivery,
            application_ack_counts=acked, replay={'acknowledged': True, 'status': 'committed',
                'new_ledger_raw_temporal_rows': 0, 'new_ws_event': False,
                'db_counts_unchanged': post_replay_db == pre_replay_db,
                'healthy_event_counts_unchanged': post_replay_state.get('events') == pre_replay_state.get('events')},
            slow_probe=slow_result, slow_close_metrics=closed_at,
            final_websocket_gauges={key: final_metrics.get(key) for key in
                ('sessions', 'subscriptions', 'session_reference_bytes', 'session_queued_in_flight_bytes')},
            effective_profile={key: env_cfg.get(key) for key in profile()},
            workload={'batches_A': BATCHES_A, 'batches_B': BATCHES_B, 'samples_per_batch': SAMPLES_PER_BATCH,
                      'wave_size': WAVE_SIZE, 'manual_mqtt_publisher': True,
                      'gateway_outbox_claim': False, 'peers': 3, 'session_cap': 64,
                      'slow_probe_max_lifetime_ms': LIFETIME_MS},
            schema_terminal=schema, backend_quota=quota, service_resources=resources,
            storage_status=storage_status, probe_logs=['healthy.log', 'slow.log'])
        smoke.durable_json(fixture.artifacts / 'p8-slow-viewer.json', result)
        return result
    except BaseException as exc:
        primary = exc
        result.update(status='FAIL', primary_error=type(exc).__name__)
        smoke.durable_json(fixture.artifacts / 'p8-slow-viewer.json', result)
        raise
    finally:
        for cfg in (healthy_cfg, slow_cfg):
            try:
                touch_private(Path(cfg['stop_path']))
            except BaseException as exc:
                cleanup_errors.append(exc)
        for sub in list(subscriptions):
            try:
                smoke.paper_p1_stop_subscription(sub)
            except BaseException as exc:
                cleanup_errors.append(exc)
        for child in children:
            try:
                stop_child(child)
            except BaseException as exc:
                cleanup_errors.append(exc)
        for log in logs:
            try:
                log.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            smoke.durable_json(fixture.artifacts / 'p8-slow-cleanup.json', {
                'primary_error': type(primary).__name__ if primary else None,
                'cleanup_errors': [type(error).__name__ for error in cleanup_errors]})
            if primary is None:
                raise ValueError('p8_slow_child_cleanup_failed') from cleanup_errors[0]


def private_json(smoke, path, value):
    smoke.private_write(path, json.dumps(value, separators=(',', ':')))


def touch_private(path):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if not path.is_file() or path.lstat().st_mode & 0o077:
            raise ValueError('p8_slow_control_file_rejected') from None
        return
    os.close(fd)


def wait_file(path, child, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise ValueError('p8_slow_probe_exited_before_ready')
        if path.is_file():
            return json.loads(path.read_text())
        time.sleep(.1)
    raise ValueError('p8_slow_probe_ready_timeout')


def wait_json(path, child, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.is_file():
            return json.loads(path.read_text())
        if child.poll() is not None and child.returncode:
            raise ValueError('p8_slow_probe_failed')
        time.sleep(.1)
    raise ValueError('p8_slow_probe_result_timeout')


def wait_peer_counts(path, child, batches_a, batches_b, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise ValueError('p8_healthy_probe_exited_before_stop')
        if path.is_file():
            state = json.loads(path.read_text())
            peers = state.get('peers', [])
            by_role = {peer.get('role'): peer for peer in peers}
            if (by_role.get('A', {}).get('unique_events') == batches_a
                    and by_role.get('B', {}).get('unique_events') == batches_b):
                return state
        time.sleep(.1)
    raise ValueError('p8_healthy_peer_count_timeout')


def ws_metrics(smoke, endpoint, token):
    status, _, body = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=token)
    metrics = body.get('websocket_metrics') if status == 200 and isinstance(body, dict) else None
    if not isinstance(metrics, dict) or type(metrics.get('slow_closes')) is not int:
        raise ValueError('p8_slow_metrics_unavailable')
    return metrics


def wait_slow_close(smoke, endpoint, token, baseline, timeout):
    deadline = time.monotonic() + timeout
    prior = {(row.get('session_id'), row.get('reason'), row.get('closed_at'))
             for row in (baseline.get('close_observations') or []) if isinstance(row, dict)}
    while time.monotonic() < deadline:
        current = ws_metrics(smoke, endpoint, token)
        new_closes = [row for row in (current.get('close_observations') or []) if isinstance(row, dict)
                      and (row.get('session_id'), row.get('reason'), row.get('closed_at')) not in prior]
        slow_close = next((row for row in new_closes if row.get('reason') in ('queue_overflow', 'ping_timeout', 'write_timeout')), None)
        if slow_close is not None and current.get('sessions') == 2:
            return {'reason': slow_close['reason'], 'slow_closes_before': baseline['slow_closes'],
                    'slow_closes_after': current['slow_closes'], 'sessions_before': baseline.get('sessions'),
                    'sessions_after': current.get('sessions'), 'close_observation': slow_close}
        time.sleep(.25)
    raise ValueError('p8_slow_server_close_not_observed')


def db_counts(smoke, registry, pg, specs):
    result = {}
    for role, spec in zip(('A', 'B'), specs):
        row = json.loads(smoke.db_psql(registry, pg, """SELECT json_build_object('ledger',
          (SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'));""",
          {'gw': spec.gateway_id, 'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip())
        result[role] = row
    return result
