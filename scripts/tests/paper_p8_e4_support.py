"""Bounded E4 unique/journal/replay warm-path fixture orchestration."""
import hashlib
import base64
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sqlite3
import time
from datetime import datetime, timezone, timedelta

_here = Path(__file__).parent
_spec = importlib.util.spec_from_file_location('paper_p8_support', _here / 'paper_p8_support.py')
_support = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_support)
_cold_spec = importlib.util.spec_from_file_location('paper_p8_e4_cold_support', _here / 'paper_p8_e4_cold_support.py')
_cold = importlib.util.module_from_spec(_cold_spec)
_cold_spec.loader.exec_module(_cold)
BoundedCollector = _support.BoundedCollector
sensor_scoped_delta = _support.sensor_scoped_delta
verify_pilot_raw = _support.verify_pilot_raw


def validate_e4_results(unique, replay, *, db_delta, scoped_delta, raw_checks,
                        unique_window, replay_window, cache_mode):
    """Validate actual Go result JSON and separated same-process accounting windows."""
    failures = []
    if cache_mode not in ('on', 'off'):
        failures.append('cache_mode_invalid')
    if not isinstance(unique, dict) or unique.get('status') != 'COMPLETE':
        failures.append('unique_result_incomplete')
    if not isinstance(replay, dict) or replay.get('status') != 'COMPLETE':
        failures.append('replay_result_incomplete')
    for label, result in (('unique', unique), ('replay', replay)):
        if isinstance(result, dict) and result.get('cleanup_errors'):
            failures.append(label + '_cleanup_failure')
    uc = unique.get('telemetry', {}) if isinstance(unique, dict) else {}
    rc = replay.get('telemetry', {}) if isinstance(replay, dict) else {}
    for key, expected in (('generated_unique', 20), ('offered_unique', 20),
                          ('application_acks', 20), ('pending_messages', 0), ('pending_bytes', 0)):
        if type(uc.get(key)) is not int or uc[key] != expected:
            failures.append('unique_' + key)
    ue = unique.get('e4') if isinstance(unique, dict) else None
    if not isinstance(ue, dict) or ue.get('mode') != 'unique' or ue.get('journal_entry_count') != 20:
        failures.append('unique_journal_proof')
    if uc.get('ignored_valid_acks') != 1 or type(uc.get('retries')) is not int or uc['retries'] < 1:
        failures.append('ignored_ack_retry_proof')
    for key, expected in (('generated_unique', 0), ('offered_unique', 0),
                          ('replay_offered', 10), ('replay_application_acks', 10),
                          ('application_acks', 0), ('pending_messages', 0), ('pending_bytes', 0)):
        if type(rc.get(key)) is not int or rc[key] != expected:
            failures.append('replay_' + key)
    re4 = replay.get('e4') if isinstance(replay, dict) else None
    if not isinstance(re4, dict) or re4.get('mode') != 'replay' or re4.get('journal_entry_count') != 10:
        failures.append('replay_journal_proof')
    for label, result in (('unique', unique), ('replay', replay)):
        counters = result.get('telemetry', {}) if isinstance(result, dict) else {}
        if type(counters.get('broker_pubacks')) is not int or counters['broker_pubacks'] < (20 if label == 'unique' else 10):
            failures.append(label + '_puback_count')
    if db_delta != {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}:
        failures.append('database_delta_mismatch')
    if not isinstance(scoped_delta, dict) or not isinstance(scoped_delta.get('available'), dict) or len(scoped_delta['available']) != 2 or any(
            counts != {'ledger': 10, 'raw': 1000, 'temporal': 10} for counts in scoped_delta['available'].values()):
        failures.append('sensor_scope_mismatch')
    if not isinstance(raw_checks, list) or len(raw_checks) != 2 or any(
            not isinstance(x, dict) or x.get('verified_exact') is not True or x.get('ledger_count') != 10
            for x in raw_checks):
        failures.append('raw_identity_mismatch')
    for label, window in (('unique', unique_window), ('replay', replay_window)):
        delta = window.get('delta') if isinstance(window, dict) and window.get('available') else None
        if not isinstance(delta, dict):
            failures.append(label + '_counter_window_unavailable')
            continue
        expected = {'Committed': 20, 'SamplesNew': 2000} if label == 'unique' else {
            'Committed': 0, 'SamplesNew': 0}
        for key, value in expected.items():
            if type(delta.get(key)) is not int or delta[key] != value:
                failures.append(f'{label}_ingress_{key}')
        min_acks = 21 if label == 'unique' else 10
        if type(delta.get('ACKPublished')) is not int or delta['ACKPublished'] < min_acks:
            failures.append(label + '_ingress_ACKPublished')
        for key in ('Committed', 'SamplesNew', 'DuplicateCommitted', 'ACKPublished'):
            before, after = window.get('before'), window.get('after')
            if not isinstance(before, dict) or not isinstance(after, dict) or key not in before or key not in after:
                failures.append(label + '_counter_baseline_missing_' + key)
                break
    unique_delta = (unique_window.get('delta') or {}) if isinstance(unique_window, dict) else {}
    replay_delta = (replay_window.get('delta') or {}) if isinstance(replay_window, dict) else {}
    tx = replay_window.get('cache') if isinstance(replay_window, dict) else None
    if not isinstance(tx, dict):
        failures.append('cache_counters_unavailable')
    elif cache_mode == 'on':
        if not (type(tx.get('warm_lookup_hits')) is int and tx['warm_lookup_hits'] >= 10 and
                type(tx.get('repository_calls_avoided')) is int and tx['repository_calls_avoided'] >= 10):
            failures.append('cache_warm_hit_unproven')
        if any(type(tx.get(field)) is not int or tx[field] != 0 for field in
               ('database_duplicates', 'confirmed_new_rows', 'copy_calls', 'transaction_commits', 'attempts')):
            failures.append('cache_on_database_work_unexpected')
    elif not (type(tx.get('attempts')) is int and tx['attempts'] > 0 and
              type(tx.get('transaction_commits')) is int and tx['transaction_commits'] > 0 and
              type(tx.get('copy_calls')) is int and tx['copy_calls'] == 0 and
              type(tx.get('confirmed_new_rows')) is int and tx['confirmed_new_rows'] == 0):
        failures.append('cache_off_database_path_unproven')
    if not (type(unique_delta.get('Committed')) is int and unique_delta['Committed'] == 20 and
            type(unique_delta.get('SamplesNew')) is int and unique_delta['SamplesNew'] == 2000 and
            type(unique_delta.get('ACKPublished')) is int and unique_delta['ACKPublished'] >= 21):
        failures.append('unique_ignored_ack_path_unproven')
    # DuplicateCommitted is an ingress result counter; it may include RAM-cache
    # re-ACKs and is not evidence of a database duplicate transaction. The
    # repository-avoidance and persistence deltas above prove the DB path stayed idle.
    if isinstance(replay, dict):
        uh = (ue or {}).get('safe_header_samples') if isinstance(ue, dict) else None
        rh = (re4 or {}).get('safe_header_samples') if isinstance(re4, dict) else None
        if not isinstance(uh, list) or not isinstance(rh, list) or len(uh) < 10 or len(rh) != 10 or uh[:10] != rh:
            failures.append('replay_journal_content_mismatch')
    snapshots = replay_window.get('database_proof') if isinstance(replay_window, dict) else None
    def valid_snapshot(value):
        return (isinstance(value, list) and len(value) == 2 and all(
            isinstance(item, dict) and all(type(item.get(k)) is int for k in
                ('ledger', 'raw', 'temporal', 'configuration')) and
            isinstance(item.get('fingerprint'), str) and len(item['fingerprint']) == 32
            for item in value))
    if (not isinstance(snapshots, dict) or not valid_snapshot(snapshots.get('unique_ack')) or
            not valid_snapshot(snapshots.get('replay_end')) or
            snapshots['unique_ack'] != snapshots['replay_end'] or snapshots.get('exact_equal') is not True):
        failures.append('replay_database_fingerprint_changed')
    return {'valid': not failures, 'failures': failures,
            'status': 'PASS' if not failures else 'FAIL', 'cache_mode': cache_mode}


def _counts(smoke, registry, pg, spec):
    sql = """SELECT json_build_object(
 'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),
 'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));"""
    counts = json.loads(smoke.db_psql(registry, pg, sql, {'gw': spec.gateway_id,
        'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip())
    fingerprint_sql = """SELECT md5(concat_ws('|',
 (SELECT coalesce(string_agg(md5(row_to_json(t)::text),'' ORDER BY message_id::text),'') FROM processed_messages t WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 (SELECT coalesce(string_agg(md5(row_to_json(t)::text),'' ORDER BY measured_at::text,sequence_num),'') FROM telemetry t WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 (SELECT coalesce(string_agg(md5(row_to_json(v)::text),'' ORDER BY v.observed_at::text),'') FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),
 (SELECT coalesce(string_agg(md5(row_to_json(s)::text),'' ORDER BY s.entity_id::text),'') FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn')));"""
    counts['fingerprint'] = smoke.db_psql(registry, pg, fingerprint_sql, {'gw': spec.gateway_id,
        'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip()
    return counts


def _database_delta(before, after):
    fields = ('ledger', 'raw', 'temporal', 'configuration')
    return {key: sum(new[key] - old[key] for old, new in zip(before, after)) for key in fields}


def validate_representative_conflict(*, schema_valid, guard_precondition,
                                     rejection_reason, expected_reason, ack_count,
                                     before_db, after_db):
    """Fail closed unless a schema-valid conflict hit its persisted guard and had no effects."""
    failures = []
    if schema_valid is not True:
        failures.append('conflict_schema_invalid')
    if guard_precondition is not True:
        failures.append('conflict_guard_precondition_missing')
    if not isinstance(expected_reason, str) or not expected_reason or rejection_reason != expected_reason:
        failures.append('conflict_backend_rejection_unproven')
    if type(ack_count) is not int or ack_count != 0:
        failures.append('conflict_ack_observed_or_unbounded')
    if not isinstance(before_db, dict) or before_db != after_db:
        failures.append('conflict_database_changed')
    return {'status': 'PASS' if not failures else 'FAIL', 'failures': failures,
            'database_unchanged': isinstance(before_db, dict) and before_db == after_db,
            'application_ack_count': ack_count}


def run_representative_conflicts(smoke, fixture, registry, containers, pg,
                                 specs, passwords, journal):
    """Publish two schema-valid, DB-guarded conflicts using actual journal bytes."""
    from datetime import datetime, timezone
    import paper_p5a_support as p5a
    from stage3_mqtt_ingress_runner import verify_ack
    from stage3_uplink_runner import probe_batch_is_valid
    from types import SimpleNamespace

    selected = None
    try:
        journal_path = Path(journal)
        if journal_path.stat().st_size > 64 << 20:
            raise ValueError
        with journal_path.open('r', encoding='utf-8') as stream:
            for index, line in enumerate(stream):
                if index >= 10000 or len(line) > (1 << 20):
                    raise ValueError
                record = json.loads(line)
                header = (record.get('header') if isinstance(record, dict) else None) or record
                if not isinstance(header, dict):
                    continue
                payload_value = record.get('payload') or record.get('payload_base64') or record.get('payload_b64')
                if not isinstance(payload_value, str):
                    continue
                payload = base64.b64decode(payload_value, validate=True)
                if (header.get('sample_count') == 100 and int(header.get('first_sequence', 0)) > 0
                        and len(payload) > 0 and
                        (not header.get('payload_sha256') or
                         hashlib.sha256(payload).hexdigest() == header.get('payload_sha256'))):
                    selected = (header, payload)
                    break
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        selected = None
    if selected is None:
        raise ValueError('p8_e4_conflict_journal_record_missing')
    header, original = selected
    target_index = next((i for i, spec in enumerate(specs)
                         if spec.gateway_id == header.get('gateway_id')
                         and spec.sensor_id == header.get('sensor_id')), None)
    if target_index is None or target_index >= len(passwords):
        raise ValueError('p8_e4_conflict_journal_identity_unmatched')
    target, password = specs[target_index], passwords[target_index]
    broker_id = containers['mosquitto']['receipt']['id']
    backend_id = containers['backend']['receipt']['id']
    cases = p5a.conflict_payloads(original, gateway_id=target.gateway_id,
        original_sensor_id=target.sensor_id, conflict_sensor_id='p8-unused-conflict',
        now=datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'))
    names = ('same_id_changed_payload', 'different_first_sequence_overlap')

    def sql(query, variables):
        return smoke.db_psql(registry, pg, query, variables).strip()

    def db_state():
        targets = []
        for spec in specs:
            counts = _counts(smoke, registry, pg, spec)
            targets.append({'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id, **counts})
        return {'targets': targets}

    def publish(payload, topic):
        return registry.command(['exec', '-i', broker_id, 'sh', '-ec',
            'IFS= read -r user; IFS= read -r password; IFS= read -r topic; '
            'exec mosquitto_pub -h mosquitto -p 8883 --cafile /mosquitto/config/certs/ca.crt '
            '-u "$user" -P "$password" -q 1 -t "$topic" -s'],
            input=(target.gateway_id + '\n' + password + '\n' + topic + '\n').encode() + payload)

    results = {}
    for name in names:
        conflict_bytes = cases[name]
        message = json.loads(conflict_bytes)
        expected_count = 2 if name == 'different_first_sequence_overlap' else 100
        schema_valid = probe_batch_is_valid(conflict_bytes,
            SimpleNamespace(gateway_id=target.gateway_id, sensor_id=target.sensor_id),
            expected_samples=expected_count)
        if schema_valid is not True:
            raise ValueError('p8_e4_conflict_schema_invalid')
        guard_vars = p5a.conflict_guard_variables(message, gateway_id=target.gateway_id,
            conflict_id=message['message_id'], conflict_bytes=conflict_bytes)
        guard_precondition = sql(p5a.conflict_guard_query(name), guard_vars).lower() == 't'
        if not guard_precondition:
            raise ValueError('p8_e4_conflict_guard_not_matched')
        pre_logs = registry.command(['logs', '--tail', '500', backend_id])
        if pre_logs.returncode != 0:
            raise ValueError('p8_e4_conflict_backend_logs_unavailable')
        pre_text = (pre_logs.stdout + pre_logs.stderr).decode(errors='replace')
        sub = None
        positive_ack = None
        conflict_ack = None
        try:
            tag = 'p8-e4-conflict-' + name.replace('_', '-')[:20]
            sub = smoke.paper_p1_subscriber(fixture, registry, broker_id, target,
                password, tag, message_count=2, message_timeout=10, subscription_kind='ack')
            original_message = json.loads(original)
            original_topic = 'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id
            original_ack_topic = 'gateways/' + target.gateway_id + '/acks/' + original_message['message_id']
            positive_publish = publish(original, original_topic)
            if positive_publish.returncode != 0:
                raise ValueError('p8_e4_conflict_positive_publish_failed')
            positive_ack = smoke.paper_p1_capture_subscription(sub, original_ack_topic, timeout=4)
            if positive_ack is None:
                raise ValueError('p8_e4_conflict_positive_ack_missing')
            verify_ack(original_message['message_id'], positive_ack)
            if sub[0].poll() is not None:
                raise ValueError('p8_e4_conflict_observer_not_joined')
            before_db = db_state()
            conflict_topic = 'gateways/' + target.gateway_id + '/telemetry/' + target.sensor_id
            conflict_ack_topic = 'gateways/' + target.gateway_id + '/acks/' + message['message_id']
            conflict_publish = publish(conflict_bytes, conflict_topic)
            if conflict_publish.returncode != 0:
                raise ValueError('p8_e4_conflict_publish_failed')
            conflict_ack = smoke.paper_p1_capture_subscription(sub, conflict_ack_topic,
                timeout=4, after_count=1 if message['message_id'] == original_message['message_id'] else 0)
            observed = smoke.paper_p1_subscription_messages(sub[2].read_bytes())
            expected_topics = [original_ack_topic]
            if conflict_ack is not None:
                expected_topics.append(conflict_ack_topic)
            if [topic for topic, _ in observed] != expected_topics:
                raise ValueError('p8_e4_conflict_unexpected_ack_observed')
        finally:
            if sub is not None:
                smoke.paper_p1_stop_subscription(sub)
        after_db = db_state()
        if (not before_db.get('targets') or not after_db.get('targets') or
                any(not isinstance(item.get('fingerprint'), str) or not item['fingerprint']
                    for item in before_db['targets'] + after_db['targets'])):
            raise ValueError('p8_e4_conflict_database_fingerprint_missing')
        post_logs = registry.command(['logs', '--tail', '500', backend_id])
        if post_logs.returncode != 0:
            raise ValueError('p8_e4_conflict_backend_logs_unavailable')
        post_text = (post_logs.stdout + post_logs.stderr).decode(errors='replace')
        expected_reason = p5a.conflict_rejection_reasons()[name]
        markers = ('reason_code=' + expected_reason, '"reason_code":"' + expected_reason + '"')
        pre_count = sum(pre_text.count(marker) for marker in markers)
        post_count = sum(post_text.count(marker) for marker in markers)
        reason = expected_reason if post_count > pre_count else None
        digest = hashlib.sha256(conflict_bytes).hexdigest()
        evidence = validate_representative_conflict(schema_valid=schema_valid,
            guard_precondition=guard_precondition, rejection_reason=reason,
            expected_reason=expected_reason, ack_count=0 if conflict_ack is None else 1,
            before_db=before_db, after_db=after_db)
        evidence.update({'message_id': message['message_id'], 'payload_sha256': digest,
            'expected_rejection_reason': expected_reason, 'observed_rejection_reason': reason,
            'guard_precondition': {'matched': guard_precondition},
            'positive_control_ack_count': 1, 'conflict_ack_count': 0 if conflict_ack is None else 1,
            'database_before': before_db, 'database_after': after_db,
            'backend_reason_log_delta': max(0, post_count - pre_count)})
        results[name] = evidence
        if evidence['status'] != 'PASS':
            raise ValueError('p8_e4_conflict_reconciliation_failed')
    return {'status': 'PASS' if len(results) == len(names) else 'FAIL', 'cases': results,
            'diagnostic_traffic_separate_from_workload_windows': True}


def validate_durable_reopen(*, intentional_stop, ignored_valid_acks, ack_deleted,
                            before, committed_payload_sha256, replay, final_pending,
                            final_quarantine, joined, main_dataset_counts):
    """Validate the stopped-writer SQLite snapshot and a real subsequent replay process."""
    failures = []
    if intentional_stop is not True:
        failures.append('durable_stop_not_intentional')
    if ignored_valid_acks != 1 or ack_deleted != 0:
        failures.append('durable_ack_ignore_not_proven')
    if not isinstance(before, dict) or not isinstance(committed_payload_sha256, str):
        failures.append('durable_snapshot_missing')
    elif (before.get('payload_sha256') != committed_payload_sha256 or
          before.get('pending_messages') != 1 or before.get('ack_deleted') != 0 or
          not isinstance(before.get('payload_bytes'), int) or before['payload_bytes'] <= 0 or
          not isinstance(before.get('header'), dict)):
        failures.append('durable_pending_bytes_hash_mismatch')
    if not isinstance(replay, dict) or replay.get('status') != 'COMPLETE':
        failures.append('durable_reopen_incomplete')
    elif (replay.get('old_payload_sha256') != committed_payload_sha256 or
          replay.get('old_message_id') != (before or {}).get('message_id') or
          replay.get('old_sequence') != (before or {}).get('sequence') or
          replay.get('old_interval_us') != (before or {}).get('interval_us') or
          replay.get('old_boot_id') != (before or {}).get('header', {}).get('boot_id') or
          replay.get('old_raw_count') != 100 or replay.get('old_sample_count') != 100 or
          replay.get('old_ledger_fingerprint') != (before or {}).get('ledger_fingerprint')):
        failures.append('durable_replay_identity_changed')
    if final_pending != 0 or final_quarantine != 0:
        failures.append('durable_outbox_not_drained')
    if joined is not True:
        failures.append('durable_reopen_process_not_joined')
    if (not isinstance(main_dataset_counts, dict) or main_dataset_counts.get('unchanged') is not True or
            not isinstance(main_dataset_counts.get('before'), dict) or
            not isinstance(main_dataset_counts['before'].get('fingerprint'), str) or
            not main_dataset_counts['before']['fingerprint'] or
            not isinstance(main_dataset_counts.get('after'), dict) or
            not isinstance(main_dataset_counts['after'].get('fingerprint'), str) or
            not main_dataset_counts['after']['fingerprint']):
        failures.append('durable_main_dataset_accounting_missing')
    return {'status': 'PASS' if not failures else 'FAIL', 'failures': failures,
            'extra_generated': replay.get('generated') if isinstance(replay, dict) else None,
            'extra_acks': replay.get('application_acks') if isinstance(replay, dict) else None}


def run_durable_sqlite_probe(smoke, fixture, endpoint, registry, pg, spec, password,
                             token, gateway_binary, root, main_snapshot_callback,
                             *, mqtt_endpoint):
    """Exercise one real Gateway process stop/reopen against a private SQLite outbox."""
    from stage3_uplink_runner import gateway_env, parse_run_summary
    failures = []
    sid = 'p8-e4-durable'
    urn = f'urn:ngsi-ld:Sensor:{spec.gateway_id}:{sid}'
    status, _, body = smoke.http_request(endpoint, 'PUT',
        f'/v1/gateways/{spec.gateway_id}/sensors/{sid}', token=token,
        body={'name': 'P8 E4 durable probe', 'unit': spec.sensor_unit})
    if status != 201 or not isinstance(body, dict) or body.get('entity_id') != urn:
        raise ValueError('p8_e4_durable_sensor_provision_failed')
    outbox = Path(root) / 'durable.sqlite'
    env = gateway_env(spec.gateway_id, password)
    env.update({'GATEWAY_CONTROL_ENABLED': 'false', 'SENSOR_ID': sid,
        'GATEWAY_MQTT_BROKER_URL': mqtt_endpoint,
        'GATEWAY_MQTT_CA_FILE': str(fixture.private / 'certs/ca.crt'),
        'GATEWAY_MQTT_SERVER_NAME': 'localhost',
        'GATEWAY_MQTT_CLIENT_ID': f'{spec.gateway_id}_p8_e4_durable_1',
        'GATEWAY_OUTBOX_PATH': str(outbox), 'GATEWAY_SAMPLE_RATE_HZ': '100',
        'GATEWAY_SAMPLES_PER_BATCH': '100', 'GATEWAY_OUTBOX_MAX_ROWS': '64',
        'GATEWAY_OUTBOX_MAX_BYTES': str(1 << 20), 'GATEWAY_SHUTDOWN_TIMEOUT': '10s'})
    initial_main = main_snapshot_callback()
    def aux_counts():
        return json.loads(smoke.db_psql(registry, pg,
            "SELECT json_build_object('ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'));",
            {'gw': spec.gateway_id, 'sensor': sid, 'urn': urn}).strip() or '{}')
    aux_before = aux_counts()
    summary = None
    fault_summary = None
    replay = None
    before = {}
    committed = {}
    joined_states = []
    for phase, args in (
        ('fault', ['--ignore-first-valid-acks', '1', '--stop-after-ignored-ack']),
        ('reopen', [])):
        env['GATEWAY_MQTT_CLIENT_ID'] = f'{spec.gateway_id}_p8_e4_durable_{phase}'
        env_path = Path(root) / f'durable-{phase}-env.json'
        smoke.private_write(env_path, json.dumps(env, sort_keys=True))
        log = Path(root) / f'durable-{phase}.log'
        with log.open('xb') as stream:
            log.chmod(0o600)
            proc = subprocess.Popen([str(gateway_binary), 'run', '--batches', '1',
                '--completion-timeout', '20s', *args], cwd=smoke.ROOT, env={**smoke.process_env(fixture), **env},
                stdout=stream, stderr=subprocess.STDOUT, close_fds=True)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try: proc.wait(timeout=2)
                except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=2)
                raise ValueError('p8_e4_durable_' + phase + '_timeout') from None
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try: proc.wait(timeout=2)
                    except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=2)
                joined_states.append(proc.poll() is not None)
        text = log.read_text(errors='replace')
        summary = parse_run_summary(text)
        if phase == 'fault':
            if (proc.returncode != 1 or summary.get('intentional_fault_stop') != 'true' or
                summary.get('ignored_valid_acks') != '1' or summary.get('completed') != 'false' or
                summary.get('app_acknowledged') != '0' or summary.get('pending_rows') != '1'):
                raise ValueError('p8_e4_durable_intentional_stop_unproven')
            fault_summary = dict(summary)
            db = sqlite3.connect(f'file:{outbox}?mode=ro', uri=True)
            try:
                rows = db.execute("SELECT message_id,boot_id,gateway_id,sensor_id,first_sequence,sample_interval_us,sample_count,payload,byte_size FROM outbox WHERE status='pending'").fetchall()
            finally: db.close()
            if len(rows) != 1: raise ValueError('p8_e4_durable_pending_row_count')
            (mid, boot, gw, sensor, seq, interval, count, payload, byte_size) = rows[0]
            payload = bytes(payload); digest = hashlib.sha256(payload).hexdigest()
            if not (len(payload) > 0 and byte_size >= len(payload)) or gw != spec.gateway_id or sensor != sid or count != 100:
                raise ValueError('p8_e4_durable_sqlite_payload_mismatch')
            committed = json.loads(smoke.db_psql(registry, pg,
            "SELECT json_build_object('hash',payload_hash,'ledger_fingerprint',md5(row_to_json(p)::text),'raw',(SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id)) FROM processed_messages p WHERE gateway_id=:'gw' AND sensor_id=:'sensor' AND message_id=:'mid';",
                {'gw': spec.gateway_id, 'sensor': sid, 'mid': mid}).strip() or '{}')
            if committed.get('hash') != digest or committed.get('raw') != 100:
                raise ValueError('p8_e4_durable_backend_commit_mismatch')
            before = {'payload_sha256': digest, 'payload_bytes': len(payload),
                'header': {'gateway_id': gw, 'sensor_id': sensor, 'boot_id': boot},
                'pending_messages': 1, 'ack_deleted': 0, 'message_id': mid,
                'sequence': seq, 'interval_us': interval,
                'ledger_fingerprint': committed.get('ledger_fingerprint')}
            continue
        if proc.returncode != 0 or summary.get('completed') != 'true':
            raise ValueError('p8_e4_durable_reopen_incomplete')
        db = sqlite3.connect(f'file:{outbox}?mode=ro', uri=True)
        try:
            final_pending = db.execute("SELECT count(*) FROM outbox WHERE status='pending'").fetchone()[0]
            final_quarantine = db.execute("SELECT count(*) FROM outbox WHERE status='quarantined'").fetchone()[0]
        finally: db.close()
        if summary.get('app_acknowledged') is None or int(summary['app_acknowledged']) < 2:
            raise ValueError('p8_e4_durable_reopen_acks_insufficient')
        try:
            payload_header = json.loads(payload.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValueError('p8_e4_durable_payload_invalid') from None
        payload_fields = {'protocol_version', 'message_id', 'message_type', 'gateway_id', 'sensor_id',
                          'boot_id', 'first_sequence', 'sample_count', 'measured_at',
                          'sample_interval_us', 'samples'}
        if (not isinstance(payload_header, dict) or set(payload_header) != payload_fields or
                payload_header.get('protocol_version') != 1 or payload_header.get('message_type') != 'telemetry_batch' or
                not isinstance(payload_header.get('samples'), list) or len(payload_header['samples']) != 100 or
                type(payload_header.get('sample_count')) is not int or payload_header.get('sample_count') != 100 or
                type(payload_header.get('sample_interval_us')) is not int or
                payload_header.get('sample_interval_us') != interval or
                not re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[47][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',
                                 str(payload_header.get('message_id')), re.IGNORECASE) or
                payload_header.get('message_id') != mid or payload_header.get('first_sequence') != seq or
                payload_header.get('gateway_id') != gw or payload_header.get('sensor_id') != sensor or
                payload_header.get('sample_count') != 100 or payload_header.get('boot_id') != boot):
            raise ValueError('p8_e4_durable_payload_header_mismatch')
        reopened = json.loads(smoke.db_psql(registry, pg,
            "SELECT json_build_object('hash',p.payload_hash,'ledger_fingerprint',md5(row_to_json(p)::text),'raw',(SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id),'boot',(SELECT min(t.boot_id::text) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id),'first_sequence',(SELECT min(t.sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id),'last_sequence',(SELECT max(t.sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id),'first_timestamp',(SELECT min(t.measured_at::text) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id),'last_timestamp',(SELECT max(t.measured_at::text) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id),'sample_count',(SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id)) FROM processed_messages p WHERE p.gateway_id=:'gw' AND p.sensor_id=:'sensor' AND p.message_id=:'mid';",
            {'gw': spec.gateway_id, 'sensor': sid, 'mid': mid}).strip() or '{}')
        expected_reopened = {'hash': digest, 'ledger_fingerprint': committed.get('ledger_fingerprint'), 'raw': 100, 'boot': boot,
            'first_sequence': seq, 'last_sequence': seq + 99, 'sample_count': 100}
        interval_proof = 'hashed_original_payload; no interval column in raw table'
        replay = {'status': 'COMPLETE', 'old_payload_sha256': reopened.get('hash'),
            'old_message_id': mid, 'old_sequence': reopened.get('first_sequence'),
            'old_interval_us': payload_header.get('sample_interval_us'),
            'old_boot_id': reopened.get('boot'), 'old_raw_count': reopened.get('raw'),
            'old_sample_count': reopened.get('sample_count'),
            'old_ledger_fingerprint': reopened.get('ledger_fingerprint'),
            'generated': summary.get('enqueued'), 'application_acks': summary.get('app_acknowledged')}
        if any(reopened.get(key) != value for key, value in expected_reopened.items() if key != 'last_timestamp'):
            raise ValueError('p8_e4_durable_post_reopen_backend_readback_mismatch')
        raw_readback = json.loads(smoke.db_psql(registry, pg,
            "SELECT json_agg(json_build_object('sequence',sequence_num,'measured_at',measured_at::text) ORDER BY sequence_num) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor' AND message_id=:'mid';",
            {'gw': spec.gateway_id, 'sensor': sid, 'mid': mid}).strip() or 'null')
        if (not isinstance(raw_readback, list) or len(raw_readback) != 100 or
                [row.get('sequence') for row in raw_readback] != list(range(seq, seq + 100)) or
                any(not isinstance(row.get('measured_at'), str) for row in raw_readback)):
            raise ValueError('p8_e4_durable_raw_timestamp_sequence_readback_mismatch')
        try:
            first_timestamp = datetime.fromisoformat(payload_header['measured_at'].replace('Z', '+00:00'))
            timestamps = [datetime.fromisoformat(row['measured_at'].replace('Z', '+00:00'))
                          for row in raw_readback]
        except (KeyError, TypeError, ValueError):
            raise ValueError('p8_e4_durable_raw_timestamp_invalid') from None
        if timestamps != [first_timestamp + timedelta(microseconds=interval * index)
                          for index in range(100)]:
            raise ValueError('p8_e4_durable_raw_timestamp_interval_mismatch')
        reopened['raw_timestamp_sequence_readback'] = raw_readback
        reopened['interval_proof'] = interval_proof
    final_main = main_snapshot_callback()
    main_counts = {'unchanged': initial_main == final_main,
                   'before': initial_main if isinstance(initial_main, dict) else {},
                   'after': final_main if isinstance(final_main, dict) else {}}
    aux_after = aux_counts()
    enqueued = summary.get('enqueued') if isinstance(summary, dict) else None
    if isinstance(enqueued, str) and enqueued.isdigit():
        enqueued = int(enqueued)
    if type(enqueued) is not int or enqueued < 0:
        raise ValueError('p8_e4_durable_enqueued_count_missing')
    aux_before_keys = ('ledger', 'raw', 'temporal')
    if (not isinstance(aux_before, dict) or not isinstance(aux_after, dict) or any(
            type(aux_before.get(key)) is not int or aux_before[key] < 0 or
            type(aux_after.get(key)) is not int or aux_after[key] < 0 for key in aux_before_keys)):
        raise ValueError('p8_e4_durable_aux_counts_missing')
    aux_delta = {key: aux_after[key] - aux_before[key] for key in aux_before_keys}
    expected_batches = 1 + enqueued
    expected_aux_delta = {'ledger': expected_batches, 'raw': 100 * expected_batches,
                          'temporal': expected_batches}
    if aux_delta != expected_aux_delta:
        raise ValueError('p8_e4_durable_aux_sensor_delta_mismatch')
    proof = validate_durable_reopen(intentional_stop=True, ignored_valid_acks=1, ack_deleted=0,
        before=before, committed_payload_sha256=digest, replay=replay, final_pending=final_pending,
        final_quarantine=final_quarantine, joined=all(joined_states) and len(joined_states) == 2,
        main_dataset_counts=main_counts)
    return {**proof, 'before': before, 'fault_summary': fault_summary,
        'reopen_summary': summary, 'main_dataset_counts': main_counts,
        'process_join_states': joined_states,
        'aux_dataset_counts': {'before': aux_before, 'after': aux_after,
            'delta': aux_delta,
            'reopened_row': reopened}}


def _safe_result(result):
    """Keep Go's bounded counters and header hashes; omit arbitrary result fields."""
    return {key: result.get(key) for key in ('status', 'implementation_status', 'experiment',
        'arm', 'profile', 'mode', 'durability_mode', 'telemetry', 'e4',
        'identity_sampling_qualification', 'phase_events', 'phase_observation_status',
        'primary_error', 'cleanup_errors') if key in result}


def _ingress_duration_snapshot(snapshot):
    ingress = snapshot.get('ingress') if isinstance(snapshot, dict) else None
    durations = ingress.get('Durations') if isinstance(ingress, dict) else None
    return {'process_instance': snapshot.get('process_instance') if isinstance(snapshot, dict) else None,
            'availability': {'ingress': ingress.get('availability') if isinstance(ingress, dict) else None},
            'ingress': durations if isinstance(durations, dict) else {}}


def _counter_delta(old, new, field):
    a, b = old.get(field), new.get(field)
    return b - a if type(a) is int and type(b) is int and b >= a else None


def _cache_counter_window(before, after):
    if (not before.get('process_instance') or
            before.get('process_instance') != after.get('process_instance')):
        return None
    rb, ra = before.get('ram_dedup'), after.get('ram_dedup')
    pb, pa = before.get('persistence'), after.get('persistence')
    if not all(isinstance(x, dict) for x in (rb, ra, pb, pa)):
        return None
    return {'warm_lookup_hits': _counter_delta(rb, ra, 'lookup_hit_total'),
            'lookup_misses': _counter_delta(rb, ra, 'lookup_miss_total'),
            'repository_calls_avoided': _counter_delta(rb, ra, 'database_calls_avoided_total'),
            'database_duplicates': None, 'confirmed_new_rows': _counter_delta(pb, pa, 'confirmed_new_rows'),
            'copy_calls': _counter_delta(pb, pa, 'copy_calls'),
            'transaction_commits': _counter_delta(pb, pa, 'confirmed_commits'),
            'attempts': _counter_delta(pb, pa, 'begin_attempts'),
            'raw_counters': {'ram_dedup_before': rb, 'ram_dedup_after': ra,
                             'persistence_before': pb, 'persistence_after': pa}}


def _stop_collector_and_process(collector, proc):
    error = None
    try:
        collector.stop(proc.pid)
    except Exception as exc:
        error = exc
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
    return error


def _flag_profile(effective, cache_mode):
    services = effective.get('services') if isinstance(effective, dict) else None
    backend = services.get('backend') if isinstance(services, dict) else None
    env = backend.get('environment') if isinstance(backend, dict) else None
    names = ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
             'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
             'MQTT_MICROBATCH_MAX_MESSAGES', 'MQTT_MICROBATCH_MAX_ROWS',
             'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT')
    if not isinstance(env, dict) or any(k not in env for k in names):
        raise ValueError('p8_e4_effective_flag_readback_missing')
    flags = {k: str(env[k]).lower() for k in names}
    for key in ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
                'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED'):
        if flags[key] not in ('true', 'false'):
            raise ValueError('p8_e4_effective_flag_invalid_' + key)
    flags['MQTT_RAM_DEDUP_ENABLED'] = 'on' if flags['MQTT_RAM_DEDUP_ENABLED'] == 'true' else 'off'
    expected = {'WEBSOCKET_ENABLED': 'true', 'STAGE5_ENABLED': 'false',
        'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'false',
        'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': cache_mode,
        'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
    if flags != expected:
        raise ValueError('p8_e4_effective_fixture_flags_mismatch')
    return {key: env[key] for key in names}


def e4_viewer_manifest(common, *, subscriptions, viewer_identity, root):
    """Build E4's private two-socket E1-contract viewer input (no telemetry fields)."""
    if (not isinstance(subscriptions, list) or len(subscriptions) not in (2, 3) or
            not isinstance(viewer_identity, str) or not isinstance(root, (str, Path))):
        raise ValueError('p8_e4_viewer_inputs_rejected')
    viewer_common = {key: value for key, value in common.items()
                     if key not in ('durability_mode', 'gateway_identity_files')}
    viewer_common.update(experiment='E1', roles=['viewer'], subscriptions=subscriptions,
        topology={**common.get('topology', {}), 'users': 2, 'viewers': 2},
        phases={'warmup_seconds': 0, 'measure_seconds': 40, 'drain_seconds': 0, 'cooldown_seconds': 0},
        viewer={'mode': 'healthy'}, identity_files={'viewer': viewer_identity},
        limits={'workers': 2, 'connections': 2, 'pending_messages': 32,
                'pending_bytes': 8388608, 'wall_seconds': 60, 'retries': 2,
                'disk_growth_bytes': 1048576}, bundle_path=str(root))
    return viewer_common


def validate_e4_ws_proof(*, unique_delta, replay_delta, state, expected_viewers=2):
    """Validate actual paperload ViewerResult and same-process server metrics."""
    failures = []
    if expected_viewers != 2:
        failures.append('viewer_recipient_count')
    for name, counts, expected in (('unique', unique_delta, 20), ('replay', replay_delta, 0)):
        if not isinstance(counts, dict):
            failures.append(name + '_ws_metrics_unavailable')
            continue
        for key in ('submissions', 'new_events'):
            if type(counts.get(key)) is not int or counts[key] != expected:
                failures.append(name + '_ws_' + key)
        for key, value in (('deliveries_enqueued', expected),
                           ('telemetry_writes_completed', expected), ('drops', 0)):
            if type(counts.get(key)) is not int or counts[key] != value:
                failures.append(name + '_ws_' + key)
    peers = state.get('viewers') if isinstance(state, dict) else None
    if (not isinstance(state, dict) or state.get('status') != 'COMPLETE' or
            state.get('connected_distinct_viewers') != 2 or
            state.get('subscribed_distinct_viewers') != 2 or
            state.get('receiving_distinct_viewers') != 2 or
            not isinstance(peers, list) or len(peers) != 2 or state.get('primary_error') or
            state.get('unique_events') != 20 or state.get('duplicates') != 0 or
            state.get('wrong_namespace') != 0):
        failures.append('viewer_state_totals')
    else:
        if {peer.get('id') for peer in peers if isinstance(peer, dict)} != {'e4-viewer-a', 'e4-viewer-b'}:
            failures.append('viewer_identity_set')
        if any(not isinstance(peer, dict) or peer.get('unique_events') != 10 or
               peer.get('duplicates') != 0 or peer.get('close_reason') not in ('measurement_complete', None)
               for peer in peers):
            failures.append('viewer_recipient_delivery')
    return {'status': 'PASS' if not failures else 'FAIL', 'failures': failures,
            'unique_ws_metrics': unique_delta if isinstance(unique_delta, dict) else None,
            'replay_ws_metrics': replay_delta if isinstance(replay_delta, dict) else None,
            'viewer_count': len(peers) if isinstance(peers, list) else None}


def start_e4_viewer(*, paperload_binary, manifest_path, output_dir, log_path,
                    expected_viewers=2, preflight_timeout=10):
    """Preflight and launch the actual finite-window viewer before any producer."""
    try:
        from scripts import paper_p8
    except ModuleNotFoundError as exc:
        if exc.name != 'scripts':
            raise
        # The native CLI is executed by file path, so scripts/ is not necessarily
        # importable as a package. Load the existing runner by its repository path.
        spec = importlib.util.spec_from_file_location(
            'paper_p8', Path(__file__).resolve().parents[1] / 'paper_p8.py')
        if spec is None or spec.loader is None:
            raise RuntimeError('p8_e4_runner_import_unavailable') from None
        paper_p8 = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(paper_p8)
    paper_p8.invoke_paperload(str(paperload_binary), str(manifest_path), str(output_dir),
                              timeout=preflight_timeout, mode='preflight')
    Path(output_dir).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    Path(log_path).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    Path(log_path).parent.chmod(0o700)
    with Path(log_path).open('xb') as stream:
        Path(log_path).chmod(0o600)
        proc = subprocess.Popen([str(Path(paperload_binary).resolve()), '--manifest',
            str(Path(manifest_path).resolve()), '--output-dir', str(Path(output_dir).resolve()), '--run'],
            stdout=stream, stderr=subprocess.STDOUT, close_fds=True, env=paper_p8._clean_env())
    try:
        paper_p8._wait_viewer_ready(proc, output_dir, expected_viewers, 15)
        return proc
    except Exception:
        _stop_viewer(proc)
        raise


def _stop_viewer(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


def finish_e4_viewer(proc, output_dir, *, timeout=50):
    """Join the configured 40s healthy window; never terminate a claimed PASS."""
    if proc.poll() is None:
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _stop_viewer(proc)
            raise ValueError('p8_e4_viewer_window_timeout') from None
    if proc.returncode != 0:
        raise ValueError('p8_e4_viewer_nonzero_exit')
    path = Path(output_dir) / 'workload.json'
    try:
        result = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ValueError('p8_e4_viewer_result_missing') from None
    if not isinstance(result, dict) or result.get('status') != 'COMPLETE':
        raise ValueError('p8_e4_viewer_result_incomplete')
    return result


def _e4_ws_window(before, after):
    """Return deltas only when process identity and every required counter are known."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise ValueError('p8_e4_ws_metrics_unavailable')
    if not before.get('process_instance') or before.get('process_instance') != after.get('process_instance'):
        raise ValueError('p8_e4_ws_process_instance_changed')
    old, new = before.get('websocket_metrics'), after.get('websocket_metrics')
    keys = ('submissions', 'new_events', 'deliveries_enqueued', 'telemetry_writes_completed', 'drops')
    if not isinstance(old, dict) or not isinstance(new, dict) or any(
            type(old.get(key)) is not int or type(new.get(key)) is not int for key in keys):
        raise ValueError('p8_e4_ws_counter_missing')
    delta = {key: new[key] - old[key] for key in keys}
    if any(value < 0 for value in delta.values()):
        raise ValueError('p8_e4_ws_counter_reset')
    return delta


def _wait_e4_ws_drain(scrape, expected_deliveries, *, timeout=8):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = scrape()
        ws = last.get('websocket_metrics', {}) if isinstance(last, dict) else {}
        if (type(ws.get('deliveries_enqueued')) is int and
                type(ws.get('telemetry_writes_completed')) is int and
                ws['deliveries_enqueued'] >= expected_deliveries and
                ws['telemetry_writes_completed'] >= expected_deliveries and
                type(ws.get('global_queued_work')) is int and ws['global_queued_work'] == 0 and
                type(ws.get('global_queued_bytes')) is int and ws['global_queued_bytes'] == 0):
            return last
        time.sleep(.1)
    raise ValueError('p8_e4_ws_drain_timeout')


def run(smoke, fixture, endpoint, registry, containers, images, state,
        paperload_binary, gateway_binary=None, cache_mode='on'):
    """Run one E4 unique then exact-byte replay against the same private journal."""
    if cache_mode not in ('on', 'off'):
        raise ValueError('p8_e4_cache_mode_rejected')
    report_path = fixture.artifacts / f'p8-e4-{cache_mode}.json'
    report = {'status': 'FAIL', 'experiment': 'E4', 'cache_mode': cache_mode,
              'lane_marker': 'PAPER_P8_E4_WARM_PILOT',
              'full_p8_accepted': False, 'native_selector': 'NOT RUN',
              'coldrestart': 'NOT RUN', 'conflicts': 'NOT RUN',
              'durable_gateway_reopen': 'NOT RUN', 'new_ws_event_proof': 'NOT RUN'}
    processes = []
    collector = None
    viewer_proc = None
    try:
        pg, quota, effective, resources, storage, tokens, specs, passwords, _ = smoke.prepare_paper_workload(
            fixture, registry, endpoint, containers, images, state)
        if len(specs) != 2 or len(passwords) != 2:
            raise ValueError('p8_e4_two_gateway_specs_required')
        report['effective_flags'] = _flag_profile(effective, cache_mode)
        schema = smoke.db_psql(registry, pg,
            'SELECT version::text || \':\' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;').strip()
        if schema != '24:telemetry_ledger_timestamp':
            raise ValueError('p8_schema_terminal_rejected')
        broker = registry.resolve(containers['mosquitto'])
        mappings = broker.get('NetworkSettings', {}).get('Ports', {}).get('8883/tcp') or []
        if len(mappings) != 1:
            raise ValueError('p8_mqtt_mapping_readback_rejected')
        mapping = {'service': 'mosquitto', 'host_ip': mappings[0].get('HostIp'), 'target': 8883,
                   'published': int(mappings[0].get('HostPort', '0'))}
        _support.validate_mqtt_readback(mapping, owned_service='mosquitto')
        root = fixture.private / 'p8-e4'
        root.mkdir(mode=0o700, parents=True, exist_ok=False); root.chmod(0o700)
        journal = root / 'dataset.jsonl'; journal.parent.chmod(0o700)
        targets = [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id} for s in specs]
        cred_files = {}
        for spec, password in zip(specs, passwords):
            p = root / ('gateway-' + spec.gateway_id + '.json')
            smoke.private_write(p, json.dumps({'username': spec.gateway_id, 'password': password}))
            cred_files[spec.gateway_id] = str(p)
        identity = root / 'telemetry-identities.json'; smoke.private_write(identity, '{}')
        viewer_identity = root / 'viewer-identities.json'
        smoke.private_write(viewer_identity, json.dumps({'user-000': tokens['a'], 'user-001': tokens['b']}))
        common = {'schema_version': 1, 'experiment': 'E4', 'arm': 'warm', 'profile': 'pilot',
            'mode': 'local', 'run_id': fixture.project + '-e4', 'seed': 89, 'roles': ['telemetry'],
            'samples_per_message': 100, 'topology': {'gateways': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            'targets': targets, 'subscriptions': [], 'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0},
            'phases': {'warmup_seconds': 0, 'measure_seconds': 4, 'drain_seconds': 8, 'cooldown_seconds': 0},
            'limits': {'retries': 4, 'workers': 2, 'connections': 2, 'pending_messages': 64,
                       'pending_bytes': 1 << 20, 'disk_growth_bytes': 1 << 20, 'wall_seconds': 30},
            'tls': {'https_endpoint': endpoint.url, 'mqtt_tls_endpoint': f"mqtts://127.0.0.1:{mapping['published']}",
                    'https_ca_file': str(endpoint.ca_file), 'mqtt_tls_ca_file': str(fixture.private / 'certs/ca.crt'),
                    'mqtt_tls_server_name': 'localhost'}, 'identity_files': {'telemetry': str(identity)},
            'gateway_identity_files': cred_files, 'bundle_path': str(root),
            'durability_mode': 'synthetic-load', 'retry_interval_ms': 500, 'publish_timeout_ms': 3000}
        manifests = []
        for phase, mode, seconds, replay_count, ignored in (
                ('unique', 'unique', 4, 0, 1), ('replay', 'replay', 2, 10, 0)):
            manifest = {**common, 'run_id': common['run_id'] + '-' + phase,
                'phases': {**common['phases'], 'measure_seconds': seconds},
                'e4': {'mode': mode, 'journal_path': str(journal), 'max_messages': 20,
                       'max_bytes': 1 << 20, 'replay_count': replay_count,
                       'ignore_first_valid_acks': ignored}}
            path = root / f'{phase}.manifest.json'; smoke.private_write(path, json.dumps(manifest, sort_keys=True))
            manifests.append(path)
        before = [_counts(smoke, registry, pg, s) for s in specs]
        subscriptions = [{'viewer_id': 'e4-viewer-a', 'user_id': 'user-000',
                          'gateway_id': specs[0].gateway_id, 'sensor_id': specs[0].sensor_id},
                         {'viewer_id': 'e4-viewer-b', 'user_id': 'user-001',
                          'gateway_id': specs[1].gateway_id, 'sensor_id': specs[1].sensor_id}]
        viewer_manifest = e4_viewer_manifest(common, subscriptions=subscriptions,
            viewer_identity=str(viewer_identity), root=root)
        viewer_manifest_path = root / 'viewer.manifest.json'
        smoke.private_write(viewer_manifest_path, json.dumps(viewer_manifest, sort_keys=True))
        viewer_out = root / 'viewer-output'
        log_root = fixture.private / 'logs'
        log_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        viewer_log = Path('/var/tmp/opencode/p8') / f'p89h-ws-runtime-{fixture.project}-{cache_mode}.log'
        viewer_proc = start_e4_viewer(paperload_binary=paperload_binary,
            manifest_path=viewer_manifest_path, output_dir=viewer_out, log_path=viewer_log)
        processes.append(viewer_proc)
        # Baseline is captured only after both real WSS sessions have subscribed.
        ws_baseline = _support._admin_metrics(smoke, endpoint, tokens['admin'])
        windows = {}; phase_results = {}; all_metrics = []; all_resources = []; histograms = {}
        log_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        log_paths = []
        unique_ack_snapshot = None
        ws_windows = {}
        for phase, manifest_path in zip(('unique', 'replay'), manifests):
            baseline = _support._admin_metrics(smoke, endpoint, tokens['admin'])
            server_base = smoke.paper_p0_stats(registry, containers, phase + '_start')
            collector = BoundedCollector(scrape=lambda: _support._admin_metrics(smoke, endpoint, tokens['admin']),
                interval=.5, max_snapshots=16, deadline_seconds=30,
                baseline_metrics=baseline, baseline_server=server_base)
            out = root / (phase + '-output')
            log = log_root / f'p89d-helper-{fixture.project}-{cache_mode}-{phase}.log'
            log_paths.append(str(log))
            with log.open('xb') as stream:
                log.chmod(0o600)
                proc = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run',
                    '--binary', str(paperload_binary), '--manifest', str(manifest_path),
                    '--output-dir', str(out), '--run-artifact', str(root / (phase + '-run.json'))],
                    cwd=smoke.ROOT, stdout=stream, stderr=subprocess.STDOUT,
                    env=smoke.process_env(fixture), close_fds=True)
                processes.append(proc); collector.start(proc.pid)
                stop_error = None
                try:
                    proc.wait(timeout=45)
                except subprocess.TimeoutExpired:
                    raise ValueError('p8_e4_runner_timeout') from None
                finally:
                    stop_error = _stop_collector_and_process(collector, proc)
                    if stop_error:
                        report.setdefault('cleanup_errors', []).append('collector_stop_' + type(stop_error).__name__)
                if stop_error:
                    raise ValueError('p8_e4_collector_stop_failed') from stop_error
            if proc.returncode != 0:
                raise ValueError('p8_e4_runner_nonzero_' + phase)
            result_path = out / 'workload.json'
            result = json.loads(result_path.read_text(encoding='utf-8'))
            if result.get('status') != 'COMPLETE' or result.get('cleanup_errors'):
                raise ValueError('p8_e4_workload_not_complete_' + phase)
            phase_results[phase] = _safe_result(result)
            after_metrics = _support._admin_metrics(smoke, endpoint, tokens['admin'])
            drained_ws = _wait_e4_ws_drain(
                lambda: _support._admin_metrics(smoke, endpoint, tokens['admin']),
                20, timeout=8)
            ws_windows[phase] = _e4_ws_window(ws_baseline, drained_ws)
            ws_baseline = drained_ws
            delta = _support.calculate_ingress_delta(baseline, after_metrics)
            windows[phase] = {'available': delta.get('available'), 'reason': delta.get('reason'),
                'before': baseline.get('ingress'), 'after': after_metrics.get('ingress'),
                'admin_before': baseline, 'admin_after': after_metrics, 'delta': delta.get('delta')}
            db_snapshot = [_counts(smoke, registry, pg, s) for s in specs]
            if phase == 'unique':
                unique_ack_snapshot = db_snapshot
            else:
                windows[phase]['database_proof'] = {'unique_ack': unique_ack_snapshot,
                    'replay_end': db_snapshot, 'exact_equal': db_snapshot == unique_ack_snapshot}
            phase_histograms = {}
            gap = any(item.get('snapshot') is None for item in collector.metrics)
            for group in ('ingress', 'persistence', 'microbatch', 'telemetry'):
                group_metrics = after_metrics.get(group) or {}
                before_metrics = baseline
                if group == 'ingress':
                    before_metrics = _ingress_duration_snapshot(baseline)
                    group_metrics = _ingress_duration_snapshot(after_metrics).get('ingress', {})
                    after_for_window = _ingress_duration_snapshot(after_metrics)
                else:
                    after_for_window = after_metrics
                for name, value in group_metrics.items():
                    if isinstance(value, dict) and 'buckets' in value:
                        phase_histograms[group + '.' + name] = (
                            {'availability': 'unavailable', 'reason': 'scrape_missing'} if gap else
                            _support.metrics_window(before_metrics, after_for_window, group, name,
                                                    smoke.paper_p1_histogram_window))
            histograms[phase] = phase_histograms
            if collector.errors: raise ValueError('p8_e4_collector_incomplete')
            phase_results[phase]['generator_samples'] = list(collector.generator_samples)
            all_metrics.extend(collector.metrics)
            all_metrics.append({'phase': phase + '_terminal', 'snapshot': after_metrics})
            all_resources.extend(collector.resources); all_resources.extend(collector.generator_samples)
        # The viewer owns a finite 40s measurement; wait for COMPLETE instead of
        # terminating it after the workload and mistaking an early stop for success.
        viewer_result = finish_e4_viewer(viewer_proc, viewer_out, timeout=50)
        ws_proof = validate_e4_ws_proof(unique_delta=ws_windows['unique'],
            replay_delta=ws_windows['replay'], state=viewer_result)
        unique_headers = ((phase_results.get('unique') or {}).get('e4') or {}).get('safe_header_samples')
        viewer_samples = viewer_result.get('identity_samples')
        expected_headers = {item.get('message_id'): item for item in unique_headers
                            if isinstance(item, dict) and isinstance(item.get('message_id'), str)} if isinstance(unique_headers, list) else {}
        def matches_journal_hash(sample):
            header = expected_headers.get(sample.get('message_id'))
            fields = ('gateway_id', 'sensor_id', 'message_id', 'boot_id', 'first_sequence', 'sample_count')
            if not isinstance(header, dict) or any(sample.get(field) != header.get(field) for field in fields):
                return False
            canonical = json.dumps({field: sample[field] for field in fields},
                                   separators=(',', ':'), ensure_ascii=False).encode('utf-8')
            return hashlib.sha256(canonical).hexdigest() == sample.get('header_sha256')
        if (not isinstance(viewer_samples, list) or not viewer_samples or any(
                not isinstance(item, dict) or item.get('message_id') not in expected_headers or
                not matches_journal_hash(item)
                for item in viewer_samples)):
            ws_proof['failures'].append('viewer_journal_hash_provenance')
            ws_proof['status'] = 'FAIL'
        report['new_ws_event_proof'] = ws_proof
        ws_proof['viewer_manifest_sha256'] = hashlib.sha256(viewer_manifest_path.read_bytes()).hexdigest()
        ws_proof['viewer_result_sha256'] = hashlib.sha256((viewer_out / 'workload.json').read_bytes()).hexdigest()
        if ws_proof['status'] != 'PASS':
            raise ValueError('p8_e4_ws_event_proof_failed')
        after = [_counts(smoke, registry, pg, s) for s in specs]
        scoped = sensor_scoped_delta([{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id, 'counts': c}
            for s, c in zip(specs, before)], [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id, 'counts': c}
            for s, c in zip(specs, after)])
        db_delta = _database_delta(before, after)
        unique_raw = json.loads((root / 'unique-output' / 'workload.json').read_text(encoding='utf-8'))
        raw_ids = [r.get('message_id') for r in unique_raw.get('identity_samples', [])
                   if isinstance(r, dict) and isinstance(r.get('message_id'), str)]
        raw = [verify_pilot_raw(smoke, registry, pg, s, message_ids=raw_ids) for s in specs]
        # Cache counters are intentionally read only from an actual exposed metric; absent fields stay unavailable.
        replay_admin_after = windows['replay'].get('admin_after') or {}
        replay_admin_before = windows['replay'].get('admin_before') or {}
        cache_window = _cache_counter_window(replay_admin_before, replay_admin_after)
        if cache_window is not None:
            # DuplicateCommitted is an ingress outcome (including RAM-cache re-ACKs),
            # not proof that PostgreSQL ran its duplicate-transaction path.
            cache_window['ingress_duplicate_outcomes'] = (windows['replay'].get('delta') or {}).get('DuplicateCommitted')
            cache_window['database_duplicates'] = 0 if cache_window.get('repository_calls_avoided') == 10 else None
            windows['replay']['cache'] = cache_window
        validation = validate_e4_results(phase_results['unique'], phase_results['replay'],
            db_delta=db_delta, scoped_delta=scoped, raw_checks=raw,
            unique_window=windows['unique'], replay_window=windows['replay'], cache_mode=cache_mode)
        report.update({'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP' if validation['valid'] else 'FAIL',
            'runner_stopped': all(proc.poll() is not None for proc in processes),
            'schema': schema, 'quota': quota, 'storage_status': storage, 'database_delta': db_delta,
            'sensor_scoped_delta': scoped, 'raw_verification': raw, 'counter_windows': windows,
            'workloads': phase_results, 'validation': validation,
            'journal_path_private': str(journal), 'manifest_paths_private': [str(p) for p in manifests],
            'runner_logs_private': log_paths + [str(viewer_log)],
            'manifest_sha256': [hashlib.sha256(p.read_bytes()).hexdigest() for p in manifests],
            'journal_sha256': hashlib.sha256(journal.read_bytes()).hexdigest(),
            'paperload_binary_sha256': hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest(),
            'source_revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
            'source_fingerprint': _support._source_fingerprint(smoke.ROOT),
            'image_ids': {k: images.get(k) for k in ('backend', 'mosquitto', 'gateway')},
            'resources': {'server': all_resources,
                'generator': [x for r in phase_results.values() for x in r.get('generator_samples', [])],
                'fixture_resources': resources},
            'histogram_windows': histograms,
            'histogram_qualification': 'P1 same-process bucket qualification per independent phase; unavailable counters retain reason',
            'full_p8_accepted': False, 'generated_at': datetime.now(timezone.utc).isoformat()})
        smoke.paper_p1_metrics_csv(fixture.artifacts / f'p8-e4-{cache_mode}-metrics.csv', all_metrics)
        server_resources = [item for item in all_resources if isinstance(item, dict) and 'services' in item]
        smoke.paper_p1_resources_csv(fixture.artifacts / f'p8-e4-{cache_mode}-resources.csv', server_resources, resources)
        if not validation['valid']:
            raise ValueError('p8_e4_validation_failed')
        if cache_mode == 'on':
            cold_report = _execute_cold_lane(smoke, fixture, endpoint, registry, containers,
                paperload_binary, root, manifests[1], journal, pg, tokens, specs,
                (windows['replay'].get('admin_before') or {}).get('process_instance'),
                phase_results['replay'], state, report)
            report['cold_cache'] = cold_report
            report['coldrestart'] = cold_report.get('status', 'FAIL')
            if cold_report.get('status') != 'PASS':
                report['status'] = 'FAIL'
                raise ValueError('p8_e4_cold_replay_failed')
            report['status'] = 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
        else:
            report['cold_cache'] = {'status': 'NOT APPLICABLE', 'reason': 'cache_off_lane'}
            report['coldrestart'] = 'NOT APPLICABLE'
        durable = run_durable_sqlite_probe(smoke, fixture, endpoint, registry, pg, specs[0],
            passwords[0], tokens['a'], gateway_binary, root,
            lambda: _counts(smoke, registry, pg, specs[0]),
            mqtt_endpoint=f"ssl://127.0.0.1:{mapping['published']}")
        report['durable_gateway_reopen'] = durable
        if durable.get('status') != 'PASS':
            report['status'] = 'FAIL'
            raise ValueError('p8_e4_durable_reopen_failed')
        conflicts = run_representative_conflicts(smoke, fixture, registry, containers,
            pg, specs, passwords, journal)
        report['conflicts'] = conflicts
        if conflicts.get('status') != 'PASS':
            report['status'] = 'FAIL'
            raise ValueError('p8_e4_representative_conflicts_failed')
        report['lane_marker'] = 'PAPER_P8_E4_PILOT_COMPLETE'
        report['status'] = 'WORKLOAD_COMPLETE_PENDING_CLEANUP'
    except Exception as exc:
        if viewer_proc is not None and report.get('new_ws_event_proof') == 'NOT RUN':
            report['new_ws_event_proof'] = {'status': 'FAIL', 'failures': ['runtime_incomplete'],
                                            'failure_type': type(exc).__name__}
        if cache_mode == 'on' and 'cold_cache' not in report:
            report['cold_cache'] = {'status': 'FAIL', 'reason': 'cold_runtime_failed',
                                    'failure': type(exc).__name__}
            report['coldrestart'] = 'FAIL'
            report['status'] = 'FAIL'
        report['failure'] = type(exc).__name__
        report['primary_error'] = type(exc).__name__
        report.setdefault('cleanup', {'status': 'not_verified', 'reason': 'fixture cleanup owned by caller'})
        raise
    finally:
        for proc in processes:
            if proc.poll() is None:
                _stop_viewer(proc)
        if viewer_proc is not None and viewer_proc.poll() is None:
            _stop_viewer(viewer_proc)
        report.setdefault('cleanup', {'status': 'not_verified', 'reason': 'fixture cleanup owned by caller'})
        smoke.durable_json(report_path, report)
        smoke.durable_json(fixture.artifacts / 'run.json', report)
    return report


def _execute_cold_lane(smoke, fixture, endpoint, registry, containers, paperload_binary,
                       root, replay_manifest, journal, pg, tokens, specs, old_process,
                       warm_replay, state, report):
    """The cache-on lane's concrete replacement followed by its real cold replay."""
    replacement = _cold.replace_owned_backend(smoke, fixture, registry, containers, state)
    return _run_cold_replay(smoke, fixture, endpoint, registry, containers,
        paperload_binary, root, replay_manifest, journal, pg, tokens, specs, old_process,
        warm_replay, replacement, report)


def _run_cold_replay(smoke, fixture, endpoint, registry, containers, paperload_binary,
                     root, replay_manifest, journal, pg, tokens, specs, old_process,
                     warm_replay, replacement, report):
    """Replay the exact ten journal records against a newly started backend process."""
    manifest = json.loads(replay_manifest.read_text(encoding='utf-8'))
    manifest['run_id'] += '-cold'
    cold_manifest = root / 'cold-replay.manifest.json'
    smoke.private_write(cold_manifest, json.dumps(manifest, sort_keys=True))
    before_db = [_counts(smoke, registry, pg, s) for s in specs]
    baseline = _support._admin_metrics(smoke, endpoint, tokens['admin'])
    schema = smoke.db_psql(registry, pg,
        'SELECT version::text || \':\' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;').strip()
    out = root / 'cold-replay-output'
    log = fixture.private / 'logs' / f'p89-e4-cold-{fixture.project}.log'
    process = None
    collector = BoundedCollector(scrape=lambda: _support._admin_metrics(smoke, endpoint, tokens['admin']),
        interval=.5, max_snapshots=16, deadline_seconds=30, baseline_metrics=baseline,
        baseline_server=smoke.paper_p0_stats(registry, containers, 'cold_replay_start'))
    try:
        with log.open('xb') as stream:
            log.chmod(0o600)
            process = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py',), '--run',
                '--binary', str(paperload_binary), '--manifest', str(cold_manifest),
                '--output-dir', str(out), '--run-artifact', str(root / 'cold-replay-run.json')],
                cwd=smoke.ROOT, stdout=stream, stderr=subprocess.STDOUT,
                env=smoke.process_env(fixture), close_fds=True)
            collector.start(process.pid)
            process.wait(timeout=45)
    except subprocess.TimeoutExpired:
        report['cold_cache'] = {'status': 'FAIL', 'reason': 'runner_timeout'}
    finally:
        if process is not None:
            stop_error = _stop_collector_and_process(collector, process)
            if stop_error:
                report.setdefault('cleanup_errors', []).append('cold_collector_stop_' + type(stop_error).__name__)
    if process is None or process.returncode != 0 or collector.errors or schema != '24:telemetry_ledger_timestamp':
        return {'status': 'FAIL', 'reason': 'runner_or_schema_failure', 'schema': schema}
    result = json.loads((out / 'workload.json').read_text(encoding='utf-8'))
    after_metrics = _support._admin_metrics(smoke, endpoint, tokens['admin'])
    after_db = [_counts(smoke, registry, pg, s) for s in specs]
    telemetry = result.get('telemetry') if isinstance(result, dict) else {}
    cold = _cold.cold_window_evidence({'process_instance': old_process}, baseline, after_metrics,
        before_db=before_db, after_db=after_db,
        application_ack_count=telemetry.get('replay_application_acks') if isinstance(telemetry, dict) else None)
    warm_e4 = warm_replay.get('e4') if isinstance(warm_replay, dict) else {}
    cold_e4 = result.get('e4') if isinstance(result, dict) else {}
    headers = (warm_e4 or {}).get('safe_header_samples')
    cold_headers = (cold_e4 or {}).get('safe_header_samples')
    if not isinstance(headers, list) or not isinstance(cold_headers, list) or headers[:10] != cold_headers or len(cold_headers) != 10:
        cold['reasons'].append('cold_replay_header_identity_mismatch')
        cold['status'] = 'FAIL'
    if result.get('status') != 'COMPLETE' or result.get('cleanup_errors'):
        cold['reasons'].append('cold_runner_incomplete')
        cold['status'] = 'FAIL'
    if (not isinstance(telemetry, dict) or telemetry.get('replay_offered') != 10 or
            telemetry.get('pending_messages') != 0 or telemetry.get('pending_bytes') != 0):
        cold['reasons'].append('cold_replay_pending_or_offer_mismatch')
        cold['status'] = 'FAIL'
    cold_metrics = list(collector.metrics) + [{'phase': 'cold_replay_terminal', 'snapshot': after_metrics}]
    cold_resources = list(collector.resources) + list(collector.generator_samples)
    cold_histograms = {}
    scrape_gap = any(item.get('snapshot') is None for item in collector.metrics)
    for group in ('ingress', 'persistence', 'microbatch', 'telemetry'):
        group_data = after_metrics.get(group) or {}
        before_data = baseline
        after_data = after_metrics
        if group == 'ingress':
            before_data = _ingress_duration_snapshot(baseline)
            group_data = _ingress_duration_snapshot(after_metrics).get('ingress', {})
            after_data = _ingress_duration_snapshot(after_metrics)
        for name, value in group_data.items():
            if isinstance(value, dict) and 'buckets' in value:
                cold_histograms[group + '.' + name] = (
                    {'availability': 'unavailable', 'reason': 'scrape_missing'} if scrape_gap else
                    _support.metrics_window(before_data, after_data, group, name,
                                            smoke.paper_p1_histogram_window))
    smoke.paper_p1_metrics_csv(fixture.artifacts / 'p8-e4-on-cold-metrics.csv', cold_metrics)
    smoke.paper_p1_resources_csv(fixture.artifacts / 'p8-e4-on-cold-resources.csv',
        [item for item in cold_resources if isinstance(item, dict) and 'services' in item],
        (report.get('resources') or {}).get('fixture_resources') or {})
    cold.update({'runner_status': result.get('status'), 'source_revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'source_fingerprint': _support._source_fingerprint(smoke.ROOT), 'schema': schema,
        'replacement': replacement.get('replacement'), 'workload': _safe_result(result),
        'database_before': before_db, 'database_after': after_db,
        'resources': cold_resources, 'histogram_windows': cold_histograms,
        'log_private': str(log), 'manifest_sha256': hashlib.sha256(cold_manifest.read_bytes()).hexdigest(),
        'journal_sha256': hashlib.sha256(journal.read_bytes()).hexdigest(),
        'original_warm_replay_unchanged': True})
    return cold
