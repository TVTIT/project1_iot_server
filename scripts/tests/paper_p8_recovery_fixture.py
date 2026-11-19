"""Bounded P8.13 recovery lane composed from the existing E5/E4 fixture paths."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import traceback
import os
import subprocess
import time
import urllib.parse
import sqlite3

import paper_p8_recovery_support as recovery
import paper_p8_e4_cold_support as cold


def pending_payload_snapshot(db_path):
    path = Path(db_path).resolve(strict=True)
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as conn:
        rows = conn.execute("SELECT payload, message_id, gateway_id, sensor_id, first_sequence "
                            "FROM outbox WHERE status='pending' ORDER BY id LIMIT 2").fetchall()
    if len(rows) != 1 or not isinstance(rows[0][0], bytes) or not rows[0][0] or len(rows[0][0]) > 65536:
        raise ValueError('recovery_pending_row_count_invalid')
    payload, message_id, gateway_id, sensor_id, sequence = rows[0]
    return {'payload': payload, 'message_id': message_id, 'gateway_id': gateway_id,
        'sensor_id': sensor_id, 'first_sequence': int(sequence),
        'payload_sha256': hashlib.sha256(payload).hexdigest(), 'payload_bytes': len(payload)}


def _safe_http_diagnostic(status, body, *, response_count=None):
    """Persist only bounded response metadata, never response text or identifiers."""
    result = {'status_code': status if type(status) is int else None,
              'body_type': type(body).__name__,
              'body_fields_present': sorted(key for key in ('code', 'error', 'message',
                  'measurement', 'reported_state', 'status') if key in body)
                  if isinstance(body, dict) else [],
              'response_count': response_count}
    if isinstance(body, dict):
        candidate = body.get('code')
        if isinstance(candidate, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,63}', candidate):
            result['error_code'] = candidate
        nested = body.get('error')
        if isinstance(nested, dict):
            candidate = nested.get('code')
            if isinstance(candidate, str) and re.fullmatch(r'[a-z][a-z0-9_]{0,63}', candidate):
                result['error_code'] = candidate
    return result


def _join_child_processes(child_processes, report):
    """Join every recorded subprocess and derive stopped state from its handle."""
    for child in child_processes:
        proc = child['process']
        if proc.poll() is None:
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
                child['joined'] = True
            except Exception as cleanup_exc:
                report['cleanup_errors'].append(child['name'] + '_' + type(cleanup_exc).__name__)
        elif not child['joined']:
            try:
                proc.wait(timeout=0)
                child['joined'] = True
            except Exception as cleanup_exc:
                report['cleanup_errors'].append(child['name'] + '_' + type(cleanup_exc).__name__)
    report['children'] = [{'name': child['name'], 'joined': bool(child['joined'])}
                          for child in child_processes]
    report['all_children_stopped'] = bool(child_processes) and all(
        child['joined'] and child['process'].poll() is not None for child in child_processes)


def verify_pending_ledger(registry, postgres_item, database, pending):
    if (not isinstance(pending.get('message_id'), str) or not re.fullmatch(
            r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}',
            pending['message_id']) or not isinstance(pending.get('gateway_id'), str)
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', pending['gateway_id'])):
        raise ValueError('recovery_pending_identity_invalid')
    query = ("SELECT payload_hash FROM public.processed_messages WHERE gateway_id='" +
        pending['gateway_id'] + "' AND message_id='" + pending['message_id'] + "';")
    value = recovery._psql(registry, recovery._validate_owned_container(registry, postgres_item),
                           database, query)
    if value != pending['payload_sha256']:
        raise ValueError('recovery_pending_ledger_hash_mismatch')
    return value


def database_url_transition(old_url, target):
    if not re.fullmatch(r'p8_restore_[a-f0-9]{20}', target):
        raise ValueError('recovery_target_database_invalid')
    parsed = urllib.parse.urlsplit(old_url)
    if (parsed.scheme != 'postgres' or parsed.hostname != 'postgres' or parsed.port != 5432
            or parsed.username != 'iot_backend_app' or parsed.path != '/smoke'
            or parsed.query != 'sslmode=disable' or parsed.fragment or parsed.password is None):
        raise ValueError('recovery_database_url_invalid')
    updated = urllib.parse.urlsplit(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc,
        '/' + target, parsed.query, parsed.fragment)))
    if (updated.scheme, updated.netloc, updated.query, updated.fragment) != (
            parsed.scheme, parsed.netloc, parsed.query, parsed.fragment):
        raise ValueError('recovery_database_url_transition_invalid')
    return urllib.parse.urlunsplit((updated.scheme, updated.netloc, updated.path,
                                    updated.query, updated.fragment))


def run_aux_gateway(smoke, fixture, gateway_binary, env_path, *, ignore_ack, child_processes=None):
    from stage3_uplink_runner import parse_run_summary
    from paper_p8_e3_fixture import _gateway_environment
    env = _gateway_environment(str(env_path))
    env['GATEWAY_MQTT_CLIENT_ID'] += '_p813_' + ('retain' if ignore_ack else 'reopen')
    log = Path(env_path).with_name('p813-aux-' + ('retain' if ignore_ack else 'reopen') + '.log')
    command = [str(gateway_binary), 'run', '--batches', '1', '--completion-timeout', '30s']
    if ignore_ack:
        command += ['--ignore-first-valid-acks', '1', '--stop-after-ignored-ack']
    with log.open('xb') as stream:
        log.chmod(0o600)
        proc = subprocess.Popen(command, cwd=smoke.ROOT, stdout=stream, stderr=subprocess.STDOUT,
            env={**smoke.process_env(fixture), **env}, close_fds=True)
        child = {'name': 'auxiliary_' + ('retain' if ignore_ack else 'reopen'),
                 'process': proc, 'joined': False}
        if child_processes is not None:
            child_processes.append(child)
        try:
            proc.wait(timeout=40)
            child['joined'] = True
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=3)
                child['joined'] = True
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait(timeout=3); child['joined'] = True
            raise ValueError('recovery_aux_gateway_timeout') from None
        except BaseException:
            if proc.poll() is None:
                proc.terminate()
                try: proc.wait(timeout=3); child['joined'] = True
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3); child['joined'] = True
            raise
    summary = parse_run_summary(log.read_text(encoding='utf-8', errors='replace'))
    return {'returncode': proc.returncode,
        'summary': {k: summary.get(k) for k in ('enqueued', 'publish_attempts', 'app_acknowledged',
            'ignored_valid_acks', 'intentional_fault_stop', 'pending_rows', 'pending_bytes', 'completed')},
        'log_private': str(log)}


def verify_rest_recovery_reads(smoke, endpoint, registry, postgres_item, target_database, token, context):
    command_keys = context['context']['command_keys']
    if len(command_keys) != 4:
        raise ValueError('recovery_command_key_count_invalid')
    command_rows = []
    for index, key in enumerate(command_keys):
        sensor_id = ('p8-e5-command-a', 'p8-e5-command-b')[index % 2]
        urn = f"urn:ngsi-ld:Sensor:{context['context']['gateway_ids'][0]}:{sensor_id}"
        text = recovery._psql_variables(registry,
            recovery._validate_owned_container(registry, postgres_item), target_database, """SELECT json_build_object(
            'command_id',c.command_id::text,'status',c.status,'result_hash',c.result_hash)::text
            FROM twin_command_requests r JOIN twin_commands c ON c.command_id=r.command_id
            JOIN twin_entities e ON e.id=c.entity_id
            WHERE r.idempotency_key=:'key' AND e.entity_id=:'urn';""", {'key': key, 'urn': urn})
        try:
            receipt = json.loads(text)
        except (ValueError, TypeError):
            raise ValueError('recovery_command_receipt_missing') from None
        command_id = receipt.get('command_id') if isinstance(receipt, dict) else None
        if (not command_id or receipt.get('status') != 'succeeded' or not receipt.get('result_hash')):
            raise ValueError('recovery_command_receipt_invalid')
        status, _, detail = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urn}/commands/{command_id}', token=token)
        if status != 200 or not isinstance(detail, dict) or detail.get('status') != 'succeeded':
            raise ValueError('recovery_command_detail_read_failed')
        command_rows.append({'sensor_id': sensor_id, 'command_status': detail['status']})
    configs = {}
    for sensor_id in ('p8-e5-command-a', 'p8-e5-command-b'):
        urn = f"urn:ngsi-ld:Sensor:{context['context']['gateway_ids'][0]}:{sensor_id}"
        status, _, body = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urn}?format=simplified', token=token)
        config = (body.get('reported_state') or {}).get('configuration') if isinstance(body, dict) else None
        if status != 200 or not isinstance(config, dict) or not config:
            raise ValueError('recovery_reported_configuration_read_failed')
        configs[sensor_id] = True
    return {'command_receipts': len(command_rows), 'command_details': len(command_rows),
        'final_configurations': len(configs), 'command_statuses': [row['command_status'] for row in command_rows]}


def replace_backend_database(smoke, fixture, registry, containers, state, target):
    """Replace the owned backend with exactly one DATABASE_URL database change."""
    if not isinstance(target, str) or not re.fullmatch(r'p8_restore_[a-f0-9]{20}', target):
        raise ValueError('recovery_target_database_invalid')
    old = containers['backend']
    old_obj = registry.resolve(old)
    if old_obj is None:
        raise ValueError('recovery_backend_not_owned')
    old_url = fixture.values.get('DATABASE_URL', '')
    new_url = database_url_transition(old_url, target)
    env_rows = old_obj.get('Config', {}).get('Env') or []
    old_env = [row for row in env_rows if isinstance(row, str)]
    old_pairs = [row for row in old_env if row.startswith('DATABASE_URL=')]
    if len(old_pairs) != 1 or old_pairs[0] != 'DATABASE_URL=' + old_url:
        raise ValueError('recovery_backend_environment_invalid')
    expected_env = [row for row in old_env if not row.startswith('DATABASE_URL=')]
    expected_env.append('DATABASE_URL=' + new_url)

    env_data = fixture.env_file.read_text(encoding='utf-8').splitlines()
    replaced = 0
    output = []
    for row in env_data:
        if row.startswith('DATABASE_URL='):
            output.append('DATABASE_URL=' + new_url)
            replaced += 1
        else:
            output.append(row)
    if replaced != 1:
        raise ValueError('recovery_fixture_environment_invalid')
    temp = fixture.env_file.with_name('fixture.env.recovery-new')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        stream.write('\n'.join(output) + '\n')
        stream.flush(); os.fsync(stream.fileno())
    os.replace(temp, fixture.env_file)
    fixture.values['DATABASE_URL'] = new_url
    fragment = fixture.private / 'compose.p8-recovery.yml'
    name = fixture.project + '-backend-restored'
    smoke.private_write(fragment, cold.replacement_fragment(name))
    if registry.command(['stop', '--time', '20', old['receipt']['id']]).returncode:
        raise ValueError('recovery_backend_stop_failed')
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
    smoke.private_write(fixture.private / 'p8-recovery-replacement.log',
                        (result.stdout + result.stderr).decode(errors='replace')[:16384])
    obj = registry.resolve(item)
    if result.returncode or obj is None:
        raise ValueError('recovery_backend_replace_failed')
    deadline = time.monotonic() + 45
    while not smoke.startup_state('backend', obj['State']):
        state.checkpoint()
        if time.monotonic() >= deadline:
            raise ValueError('recovery_backend_health_failed')
        time.sleep(.5)
        obj = registry.resolve(item)
        if obj is None:
            raise ValueError('recovery_backend_health_failed')
    if (obj.get('Image') != old_obj.get('Image')
            or sorted(obj.get('Config', {}).get('Env') or []) != sorted(expected_env)
            or cold.normalize_host_config(obj.get('HostConfig')) != cold.normalize_host_config(old_obj.get('HostConfig'))
            or sorted(obj.get('Mounts') or [], key=lambda x: x.get('Destination', '')) !=
               sorted(old_obj.get('Mounts') or [], key=lambda x: x.get('Destination', ''))):
        raise ValueError('recovery_backend_runtime_changed')
    if registry.command(['exec', containers['nginx']['receipt']['id'], 'nginx', '-s', 'reload']).returncode:
        raise ValueError('recovery_nginx_reload_failed')
    active = registry.command(['ps', '--filter', 'label=io.iot.native-smoke.run=' + fixture.project,
                               '--filter', 'label=io.iot.native-smoke.role=backend',
                               '--format', '{{.ID}}'])
    active_ids = active.stdout.decode(errors='replace').splitlines() if active.returncode == 0 else []
    if len(active_ids) != 1 or not (obj['Id'].startswith(active_ids[0]) or active_ids[0].startswith(obj['Id'])):
        raise ValueError('recovery_backend_active_count_invalid')
    actual = recovery._psql(registry, containers['postgres']['receipt']['id'], target,
                            'SELECT current_database();')
    if actual != target:
        raise ValueError('recovery_active_database_mismatch')
    schema = recovery._psql(registry, containers['postgres']['receipt']['id'], target,
                            'SELECT max(version)::text FROM public.schema_migrations;')
    if schema != '24':
        raise ValueError('recovery_active_schema_mismatch')
    return {'status': 'replaced', 'old_container_id': old_obj['Id'], 'new_container_id': obj['Id'],
        'database_url_only_change': True, 'active_database': actual,
        'active_backend_count': 1, 'schema_version': int(schema)}


def run(smoke, fixture, endpoint, registry, containers, images, state,
        paperload_binary, gateway_binary):
    report = {'status': 'FAIL', 'lane_marker': 'PAPER_P8_RECOVERY_PILOT',
        'full_p8_accepted': False, 'primary_error': None, 'cleanup_errors': [],
        'phases': {}, 'generated_at': datetime.now(timezone.utc).isoformat(),
        'all_children_stopped': False,
        'database_scope': 'fixture-only-smoke-to-new-database-same-postgres-cluster'}
    context = {}
    viewer_proc = None
    child_processes = []
    try:
        report['phases']['baseline'] = {'status': 'RUNNING'}
        baseline = smoke.paper_p8_e5_support().run(smoke, fixture, endpoint, registry,
            containers, images, state, paperload_binary, gateway_binary, 'optimized',
            fixture.paper_p8_e5_context, recovery_context_out=context)
        if (baseline.get('status') not in ('WORKLOAD_COMPLETE_PENDING_CLEANUP', 'PASS')
                or baseline.get('processes_stopped') is not True
                or baseline.get('runner_stopped') is not True):
            raise ValueError('recovery_e5_baseline_failed')
        report['phases']['baseline'] = {'status': 'PASS', 'e5_control_receipts':
            (baseline.get('control_reconciliation') or {}).get('verified_receipts')}
        report['source_fingerprint'] = baseline.get('source_fingerprint')
        report['image_ids'] = baseline.get('image_ids')
        report['toolchain'] = {'paperload_binary_sha256': baseline.get('paperload_binary_sha256'),
            'gateway_binary_sha256': baseline.get('gateway_binary_sha256')}

        # One owner-provisioned auxiliary Sensor retains exactly one application-ACK
        # pending SQLite row through an intentional, bounded Gateway stop.
        report['phases']['auxiliary_pending_retention'] = {'status': 'RUNNING'}
        retained = run_aux_gateway(smoke, fixture, gateway_binary,
            context['recovery_aux_env_file'], ignore_ack=True, child_processes=child_processes)
        summary = retained['summary']
        if (summary.get('enqueued') != '1' or summary.get('ignored_valid_acks') != '1'
                or summary.get('pending_rows') != '1' or summary.get('intentional_fault_stop') != 'true'):
            raise ValueError('recovery_aux_pending_not_retained')
        original_pending = pending_payload_snapshot(context['recovery_aux_db_path'])
        report['phases']['auxiliary_pending_retention'] = {'status': 'PASS',
            'actual_enqueued': int(summary['enqueued']), 'pending_before_restart': 1,
            'payload_sha256': original_pending['payload_sha256'],
            'payload_bytes': original_pending['payload_bytes'],
            'message_id': original_pending['message_id'], 'gateway_id': original_pending['gateway_id'],
            'sensor_id': original_pending['sensor_id'], 'first_sequence': original_pending['first_sequence']}

        # Existing E4 replacement path stops/removes only the registry-owned backend
        # and brings up one backend with the exact same image, env, mounts and limits.
        report['phases']['controlled_backend_restart'] = {'status': 'RUNNING'}
        import paper_p8_e4_support as e4
        metrics_before = e4._support._admin_metrics(smoke, endpoint, context['tokens']['admin'])
        replacement = cold.replace_owned_backend(smoke, fixture, registry, containers, state)
        metrics_after = e4._support._admin_metrics(smoke, endpoint, context['tokens']['admin'])
        old_instance = metrics_before.get('process_instance')
        new_instance = metrics_after.get('process_instance')
        if not old_instance or not new_instance or old_instance == new_instance:
            raise ValueError('recovery_process_instance_not_changed')
        report['phases']['controlled_backend_restart'] = {'status': 'PASS',
            'old_process_instance': old_instance, 'new_process_instance': new_instance,
            'replacement': replacement['replacement']}

        # Reconnect the real finite viewer, replay the already-committed journal,
        # and prove the duplicate caused no database or WebSocket event delta.
        viewer_out = Path(context['root']) / 'recovery-viewer-output'
        viewer_log = Path(context['root']) / 'recovery-viewer.log'
        viewer_proc = e4.start_e4_viewer(paperload_binary=paperload_binary,
            manifest_path=context['prepared']['manifests']['viewer'], output_dir=viewer_out,
            log_path=viewer_log, expected_viewers=2)
        viewer_child = {'name': 'recovery_viewer', 'process': viewer_proc, 'joined': False}
        child_processes.append(viewer_child)
        before_replay = recovery.snapshot_database(registry, containers['postgres'])
        replay_log = Path(context['root']) / 'recovery-background-replay.log'
        replay_out = Path(context['root']) / 'recovery-background-replay'
        replay_artifact = Path(context['root']) / 'recovery-background-replay-run.json'
        with replay_log.open('xb') as stream:
            replay_log.chmod(0o600)
            replay_proc = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py',), '--run',
                '--binary', str(paperload_binary), '--manifest', context['prepared']['manifests']['warm_replay'],
                '--output-dir', str(replay_out), '--run-artifact', str(replay_artifact)],
                cwd=smoke.ROOT, stdout=stream, stderr=subprocess.STDOUT,
                env=smoke.process_env(fixture), close_fds=True)
            replay_child = {'name': 'background_replay', 'process': replay_proc, 'joined': False}
            child_processes.append(replay_child)
            try:
                replay_proc.wait(timeout=45)
                replay_child['joined'] = True
            except subprocess.TimeoutExpired:
                replay_proc.terminate()
                try: replay_proc.wait(timeout=3); replay_child['joined'] = True
                except subprocess.TimeoutExpired: replay_proc.kill(); replay_proc.wait(timeout=3); replay_child['joined'] = True
                raise ValueError('recovery_background_replay_timeout') from None
            except BaseException:
                if replay_proc.poll() is None:
                    replay_proc.terminate()
                    try: replay_proc.wait(timeout=3); replay_child['joined'] = True
                    except subprocess.TimeoutExpired: replay_proc.kill(); replay_proc.wait(timeout=3); replay_child['joined'] = True
                raise
        replay_result = json.loads((replay_out / 'workload.json').read_text())
        after_replay = recovery.snapshot_database(registry, containers['postgres'])
        recovery.assert_snapshot_equal(before_replay, after_replay)
        if replay_proc.returncode or replay_result.get('status') != 'COMPLETE':
            raise ValueError('recovery_background_replay_failed')
        viewer_result = e4.finish_e4_viewer(viewer_proc, viewer_out, timeout=50)
        viewer_child['joined'] = True
        viewer_proc = None
        viewer_safe = smoke.paper_p8_e5_support()._safe_e5_viewer(viewer_result)
        if (viewer_safe.get('status') != 'COMPLETE'
                or viewer_safe.get('connected_distinct_viewers') != 2
                or viewer_safe.get('subscribed_distinct_viewers') != 2
                or viewer_safe.get('unique_events') != 0):
            raise ValueError('recovery_viewer_reconnect_or_replay_delta_failed')
        report['phases']['reconnect_and_background_replay'] = {'status': 'PASS',
            'viewer': viewer_safe, 'replay_status': replay_result.get('status'),
            'database_unchanged': True, 'new_websocket_events': 0}

        pg = containers['postgres']
        source = recovery.snapshot_database(registry, pg)
        sqlite_before = []
        for path in context.get('sqlite_paths', []):
            raw = Path(path).read_bytes()
            sqlite_before.append({'sha256': hashlib.sha256(raw).hexdigest(), 'size_bytes': len(raw)})
        pending_before_backup = pending_payload_snapshot(context['recovery_aux_db_path'])
        if pending_before_backup['payload'] != original_pending['payload']:
            raise ValueError('recovery_pending_payload_changed_before_backup')
        source_pending_hash = verify_pending_ledger(registry, pg, recovery.SOURCE_DATABASE,
                                                   original_pending)
        archive = fixture.private / 'p8-recovery-smoke.dump'
        backup = recovery.backup_database(registry, fixture, pg, archive)
        restored = recovery.restore_database(registry, fixture, pg, archive, fixture.project)
        after = recovery.snapshot_database(registry, pg, restored['target_database'])
        recovery.assert_snapshot_equal(source, after)
        sqlite_after = []
        for path in context.get('sqlite_paths', []):
            raw = Path(path).read_bytes()
            sqlite_after.append({'sha256': hashlib.sha256(raw).hexdigest(), 'size_bytes': len(raw)})
        if sqlite_before != sqlite_after:
            raise ValueError('recovery_sqlite_changed')
        pending_after_restore = pending_payload_snapshot(context['recovery_aux_db_path'])
        if pending_after_restore['payload'] != original_pending['payload']:
            raise ValueError('recovery_pending_payload_changed_during_restore')
        restored_pending_hash = verify_pending_ledger(registry, pg, restored['target_database'],
                                                      original_pending)
        if restored_pending_hash != source_pending_hash:
            raise ValueError('recovery_pending_ledger_changed')
        report['phases']['database_backup_restore'] = {'status': 'PASS',
            'backup': backup, 'restore': restored, 'snapshot': source,
            'snapshot_equal': True, 'sqlite_before': sqlite_before, 'sqlite_after': sqlite_after,
            'pending_payload_unchanged': True, 'pending_ledger_hash': restored_pending_hash}

        report['phases']['restored_backend_switch'] = {'status': 'RUNNING'}
        replacement = replace_backend_database(smoke, fixture, registry, containers, state,
                                                restored['target_database'])
        report['phases']['restored_backend_switch'] = {**replacement, 'status': 'PASS'}

        reopened = run_aux_gateway(smoke, fixture, gateway_binary,
            context['recovery_aux_env_file'], ignore_ack=False, child_processes=child_processes)
        reopened_summary = reopened['summary']
        if (reopened_summary.get('completed') != 'true' or reopened_summary.get('pending_rows') != '0'
                or not reopened_summary.get('enqueued', '').isdigit()
                or int(reopened_summary['enqueued']) < 1):
            raise ValueError('recovery_aux_reopen_not_drained')
        pending_after_reopen = sqlite3.connect(context['recovery_aux_db_path']).execute(
            "SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
        if pending_after_reopen != 0:
            raise ValueError('recovery_aux_pending_remains')
        acknowledged_ledger_hash = verify_pending_ledger(registry, pg,
            restored['target_database'], original_pending)
        if acknowledged_ledger_hash != original_pending['payload_sha256']:
            raise ValueError('recovery_aux_acknowledged_payload_mismatch')
        report['phases']['auxiliary_reopen'] = {'status': 'PASS',
            'actual_enqueued': int(reopened_summary['enqueued']),
            'application_acknowledged': int(reopened_summary['app_acknowledged']),
            'pending_after_reopen': pending_after_reopen,
            'original_payload_sha256': original_pending['payload_sha256'],
            'original_payload_bytes': original_pending['payload_bytes']}

        owner_token = context['tokens']['a']
        gateway_ids = context['context']['gateway_ids']
        sensor_ids = context['context']['sensor_ids']
        urn = f'urn:ngsi-ld:Sensor:{gateway_ids[0]}:{sensor_ids[0]}'
        status, _, body = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urn}?format=simplified', token=owner_token)
        report['api_diagnostics'] = {'owner_detail': _safe_http_diagnostic(status, body)}
        if status != 200 or not isinstance(body, dict):
            raise ValueError('recovery_owner_detail_read_failed')
        # Scope the read to one known 100-sample journal batch in the restored DB.
        # Wall-clock query windows can be rejected by the history age/point bounds.
        batch = recovery._psql_variables(registry,
            recovery._validate_owned_container(registry, containers['postgres']),
            restored['target_database'], """SELECT json_build_object(
              'count', count(*), 'first', to_char(min(measured_at AT TIME ZONE 'UTC'),
                'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
              'last', to_char(max(measured_at AT TIME ZONE 'UTC'),
                'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'))::text
            FROM public.telemetry
            WHERE gateway_id=:'gateway' AND sensor_id=:'sensor'
              AND sequence_num BETWEEN 1 AND 100;""",
            {'gateway': gateway_ids[0], 'sensor': sensor_ids[0]})
        try:
            batch = json.loads(batch)
        except (ValueError, TypeError):
            raise ValueError('recovery_temporal_batch_invalid') from None
        if (not isinstance(batch, dict) or batch.get('count') != 100
                or not isinstance(batch.get('first'), str) or not isinstance(batch.get('last'), str)):
            raise ValueError('recovery_temporal_batch_invalid')
        query = smoke.paper_p0_temporal_query(batch['first'], batch['last'])
        temporal_status, _, temporal_body = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urllib.parse.quote(urn, safe=":")}/temporal?{query}', token=owner_token)
        measurement_values = (temporal_body.get('measurement') or {}).get('values') \
            if isinstance(temporal_body, dict) else None
        denied_status, _, denied_body = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/urn:ngsi-ld:Sensor:{gateway_ids[1]}:{sensor_ids[0]}?format=simplified',
            token=owner_token)
        missing_status, _, missing_body = smoke.http_request(endpoint, 'GET',
            f'/v1/digital-twins/{urn}?format=simplified')
        report['api_diagnostics'].update({
            'owner_temporal': _safe_http_diagnostic(temporal_status, temporal_body,
                response_count=len(measurement_values) if isinstance(measurement_values, list) else None),
            'cross_gateway_nonmember': _safe_http_diagnostic(denied_status, denied_body),
            'missing_jwt': _safe_http_diagnostic(missing_status, missing_body),
            'temporal_batch_count': batch['count']})
        if temporal_status != 200:
            raise ValueError('recovery_temporal_read_failed')
        if not isinstance(measurement_values, list) or len(measurement_values) != batch['count']:
            raise ValueError('recovery_temporal_business_result_mismatch')
        if denied_status not in (403, 404):
            raise ValueError('recovery_nonmember_authorization_failed')
        if missing_status != 401:
            raise ValueError('recovery_missing_jwt_authorization_failed')
        rest_commands = verify_rest_recovery_reads(smoke, endpoint, registry, pg,
            restored['target_database'], owner_token, context)
        report['phases']['api_restored_readback'] = {'status': 'PASS',
            'owner_detail_status': status, 'owner_temporal_status': temporal_status,
            'cross_gateway_status': denied_status, 'missing_jwt_status': missing_status,
            **rest_commands}
        report['status'] = 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
    except Exception as exc:
        code = str(exc) if isinstance(exc, ValueError) and re.fullmatch(r'[a-z0-9_]+', str(exc)) else None
        report['primary_error'] = {'code': code, 'exception': type(exc).__name__,
            'location': None if code else _safe_location(exc)}
    finally:
        if viewer_proc is not None:
            try:
                e4._stop_viewer(viewer_proc)
                viewer_child['joined'] = True
                report['viewer_joined_after_failure'] = True
            except Exception as cleanup_exc:
                report['cleanup_errors'].append('viewer_' + type(cleanup_exc).__name__)
        _join_child_processes(child_processes, report)
        try:
            smoke.durable_json(fixture.private / 'p8-recovery-result.json', report)
        except Exception:
            report['cleanup_errors'].append('recovery_artifact_write_failed')
            report['status'] = 'FAIL'
    return report


def _safe_location(exc):
    frames = traceback.extract_tb(exc.__traceback__)
    return {'file': Path(frames[-1].filename).name, 'line': frames[-1].lineno} if frames else None
