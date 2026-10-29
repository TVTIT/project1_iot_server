"""E3 disposable fixture workload orchestration and accounting helpers."""
import hashlib
import importlib.util
import http.client
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import secrets
import ssl
import time
import urllib.parse
from fractions import Fraction
from datetime import datetime, timezone
from types import SimpleNamespace

_HERE = Path(__file__).resolve().parent
_SCRIPTS = _HERE.parent
for _p in (str(_HERE), str(_SCRIPTS)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from paper_p8 import GATEWAY_ENV_KEYS, _gateway_environment
from paper_p8_e3_support import (
    e3_pilot_arms, e3_pilot_manifests, build_e3_execution_plan, validate_e3_result,
)
from stage3_uplink_runner import gateway_env
from paper_p6_support import command_simulator_environment

_support_spec = importlib.util.spec_from_file_location(
    'paper_p8_support', _HERE / 'paper_p8_support.py')
_support = importlib.util.module_from_spec(_support_spec)
_support_spec.loader.exec_module(_support)
BoundedCollector = _support.BoundedCollector
verify_pilot_raw = _support.verify_pilot_raw


def _safe_flags(effective):
    allowed = (
        'WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
        'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
        'MQTT_SHAPER_RATE_PER_SECOND', 'MQTT_SHAPER_BURST',
        'MQTT_MICROBATCH_MAX_MESSAGES', 'MQTT_MICROBATCH_MAX_ROWS',
        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT',
    )
    env = effective.get('services', {}).get('backend', {}).get('environment', {}) if isinstance(effective, dict) else {}
    return {key: env[key] for key in allowed if key in env}


def _validate_e3_fixture_flags(effective, arm):
    flags = _safe_flags(effective)
    wakeup_str = 'true' if arm == 'wakeup' else 'false'
    expected = {
        'WEBSOCKET_ENABLED': 'false',
        'STAGE5_ENABLED': 'true',
        'STAGE5_WAKEUP_ENABLED': wakeup_str,
        'MQTT_SHAPER_ENABLED': 'true',
        'MQTT_MICROBATCH_ENABLED': 'true',
        'MQTT_RAM_DEDUP_ENABLED': 'false',
        'MQTT_SHAPER_RATE_PER_SECOND': '2',
        'MQTT_SHAPER_BURST': '4',
        'MQTT_MICROBATCH_MAX_MESSAGES': '32',
        'MQTT_MICROBATCH_MAX_ROWS': '500',
        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20),
        'MQTT_MICROBATCH_MAX_WAIT': '50ms',
    }
    normalized = {key: str(value).lower() for key, value in flags.items()}
    if any(normalized.get(key) != value for key, value in expected.items()):
        raise ValueError(f'p8_e3_effective_fixture_flags_mismatch:{arm}')
    return flags


def _executor_environment(base_env, sensor_id, client_id):
    env = command_simulator_environment(base_env, sensor_id, client_id)
    # A 100-sample command-sensor batch can exceed the backend's 60-second span
    # after low-frequency commands apply. Use single-sample executor batches;
    # background paperload keeps its independent 100-sample profile.
    env['GATEWAY_SAMPLE_RATE_HZ'] = '2'
    env['GATEWAY_SAMPLES_PER_BATCH'] = '1'
    return {key: str(value) for key, value in env.items() if key in GATEWAY_ENV_KEYS}


def _target_counts(smoke, registry, pg, spec):
    sql = """SELECT json_build_object(
 'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),
 'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));"""
    value = smoke.db_psql(registry, pg, sql,
        {'gw': spec.gateway_id, 'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip()
    return json.loads(value)


def _probe_http(endpoint, method, path, token, body=None):
    """Bounded HTTPS request retaining the actual response bytes for receipt comparison."""
    parsed = urllib.parse.urlparse(endpoint.url)
    conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=10,
                                       context=ssl.create_default_context(cafile=str(endpoint.ca_file)))
    try:
        payload = json.dumps(body, separators=(',', ':')).encode() if body is not None else None
        headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
        conn.request(method, path, body=payload, headers=headers)
        response = conn.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError('p8_e3_probe_response_too_large')
        try:
            parsed_body = json.loads(raw.decode('utf-8')) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed_body = {}
        return response.status, raw, parsed_body
    finally:
        conn.close()


def _probe_status_classification(kind, status):
    expected = {'offline_admission': 202, 'same_key_replay': 202,
                'busy': 409, 'stale': 409, 'noop': 200, 'noop_replay': 200}
    return {'expected': expected[kind], 'actual': status, 'match': status == expected[kind]}


def _representative_probe_accounting(operations, before, after, pending):
    expected = ('offline_admission', 'same_key_replay', 'busy', 'stale', 'noop', 'noop_replay')
    return (tuple(row.get('kind') for row in operations) == expected
            and all(row.get('match') is True for row in operations)
            and before == after and pending == 0)


def run_representative_probes(smoke, fixture, endpoint, registry, pg, token, gateway_binary,
                              executor_env_file, executor_db_path, gateway_id, sensor_id,
                              sensor_urn):
    """Bounded no-op/replay/busy/stale probes; intentionally outside E3 measurement."""
    report = {'valid': False, 'failures': [], 'operations': [], 'auxiliary_command_count': 0,
              'receipt_count': 0, 'outside_fresh_command_measurement': True}
    try:
        path = f'/v1/digital-twins/{sensor_urn}'
        status, _, current = smoke.http_request(endpoint, 'GET', path + '?format=simplified', token=token)
        if status != 200 or not isinstance(current, dict):
            raise ValueError('probe_current_state_unavailable')
        version = int(current.get('desired_version') or current.get('desired_state', {}).get('desired_version'))
        key = secrets.token_hex(16)
        request = {'frequency_hz': 4, 'expected_desired_version': str(version)}
        # Keep the body and idempotency key identical for receipt replay.
        status, raw, body = _probe_http_request_with_headers(endpoint, path, token, key, request)
        report['operations'].append({'kind': 'offline_admission', **_probe_status_classification('offline_admission', status),
                                     'command_id': body.get('command_id') if isinstance(body, dict) else None})
        if status != 202 or not isinstance(body, dict) or not body.get('command_id'):
            raise ValueError('probe_offline_admission_rejected')
        command_id = body['command_id']
        admitted_bytes = raw
        status, replay_raw, replay_body = _probe_http_request_with_headers(endpoint, path, token, key, request)
        report['operations'].append({'kind': 'same_key_replay', **_probe_status_classification('same_key_replay', status)})
        if status != 202 or replay_raw != admitted_bytes:
            raise ValueError('probe_admission_replay_mismatch')
        busy_key = secrets.token_hex(16)
        busy_status, _, _ = _probe_http_request_with_headers(endpoint, path, token, busy_key,
            {'frequency_hz': 5, 'expected_desired_version': str(version + 1)})
        report['operations'].append({'kind': 'busy', **_probe_status_classification('busy', busy_status)})
        stale_status, _, _ = _probe_http_request_with_headers(endpoint, path, token, secrets.token_hex(16),
            {'frequency_hz': 5, 'expected_desired_version': str(version)})
        report['operations'].append({'kind': 'stale', **_probe_status_classification('stale', stale_status)})
        if busy_status != 409 or stale_status != 409:
            raise ValueError('probe_busy_or_stale_status_mismatch')

        env = _gateway_environment(executor_env_file)
        env['GATEWAY_SAMPLES_PER_BATCH'] = '1'
        env['GATEWAY_MQTT_CLIENT_ID'] += '_representative_probe'
        probe_env_file = Path(executor_env_file).with_name('executor_a_representative_probe.json')
        smoke.private_write(probe_env_file, json.dumps(env, sort_keys=True))
        stdout = probe_env_file.with_suffix('.log')
        with stdout.open('xb') as stream:
            stdout.chmod(0o600)
            proc = subprocess.Popen([str(gateway_binary), 'run', '--batches', '1',
                                     '--completion-timeout', '30s'], cwd=smoke.ROOT,
                                    stdout=stream, stderr=subprocess.STDOUT,
                                    env={**smoke.process_env(fixture),
                                         **_gateway_environment(str(probe_env_file))}, close_fds=True)
            try:
                proc.wait(timeout=40)
            except subprocess.TimeoutExpired:
                _stop(proc)
                raise ValueError('probe_executor_timeout') from None
        before_noop = _target_counts(smoke, registry, pg, SimpleNamespace(
            gateway_id=gateway_id, sensor_id=sensor_id, sensor_urn=sensor_urn))
        deadline = time.monotonic() + 30
        applied = None
        while time.monotonic() < deadline:
            st, _, detail = smoke.http_request(endpoint, 'GET', path + '?format=simplified', token=token)
            rep = detail.get('reported_state', {}).get('configuration', {}) if isinstance(detail, dict) else {}
            if st == 200 and str(rep.get('desired_version')) == str(version + 1) and rep.get('sample_interval_us') == 250000:
                applied = detail
                break
            time.sleep(.25)
        if proc.returncode != 0 or applied is None:
            raise ValueError('probe_apply_or_result_not_confirmed')
        noop_key = secrets.token_hex(16)
        noop_request = {'frequency_hz': 4, 'expected_desired_version': str(version + 1)}
        noop_status, noop_raw, noop_body = _probe_http_request_with_headers(endpoint, path, token, noop_key, noop_request)
        report['operations'].append({'kind': 'noop', **_probe_status_classification('noop', noop_status)})
        replay_status, noop_replay_raw, _ = _probe_http_request_with_headers(endpoint, path, token, noop_key, noop_request)
        report['operations'].append({'kind': 'noop_replay', **_probe_status_classification('noop_replay', replay_status)})
        if noop_status != 200 or replay_status != 200 or noop_raw != noop_replay_raw:
            raise ValueError('probe_noop_replay_mismatch')
        after_noop = _target_counts(smoke, registry, pg, SimpleNamespace(
            gateway_id=gateway_id, sensor_id=sensor_id, sensor_urn=sensor_urn))
        if any(after_noop[k] != before_noop[k] for k in ('ledger', 'raw', 'temporal')):
            raise ValueError('probe_noop_manufactured_command_or_telemetry')
        persisted = smoke.db_psql(registry, pg, """SELECT COALESCE(json_agg(json_build_object(
          'status_code',status_code,'response_md5',md5(response_bytes),
          'command_id',command_id::text) ORDER BY status_code),'[]'::json)::text
          FROM twin_command_requests WHERE idempotency_key IN (:'admit', :'noop');""",
          {'admit': key, 'noop': noop_key}).strip()
        receipts = json.loads(persisted) if persisted else []
        if len(receipts) != 2 or sorted(int(row['status_code']) for row in receipts) != [200, 202]:
            raise ValueError('probe_persisted_receipt_mismatch')
        if hashlib.md5(admitted_bytes).hexdigest() not in [r['response_md5'] for r in receipts] or hashlib.md5(noop_raw).hexdigest() not in [r['response_md5'] for r in receipts]:
            raise ValueError('probe_receipt_response_bytes_mismatch')
        command_proof = smoke.db_psql(registry, pg, """SELECT json_build_object(
          'command_count',count(*),'status',min(c.status),'desired_version',min(c.desired_version)::text,
          'sample_interval_us',min((c.parameters->>'sample_interval_us')::bigint))::text
          FROM twin_command_requests r JOIN twin_commands c ON c.command_id=r.command_id
          WHERE r.idempotency_key=:'key';""", {'key': key}).strip()
        command_proof = json.loads(command_proof) if command_proof else {}
        if (command_proof.get('command_count') != 1 or command_proof.get('status') != 'succeeded'
                or str(command_proof.get('desired_version')) != str(version + 1)
                or command_proof.get('sample_interval_us') != 250000):
            raise ValueError('probe_auxiliary_command_proof_mismatch')
        config = _inspect_executor_sqlite(executor_db_path)
        if config.get('pending_outbox') != 0 or config.get('configuration') != {
                'id': 1, 'desired_version': str(version + 1), 'sample_interval_us': 250000,
                'applied_at': config.get('configuration', {}).get('applied_at')}:
            raise ValueError('probe_executor_not_drained_or_config_mismatch')
        if not _representative_probe_accounting(report['operations'], before_noop, after_noop,
                                                config.get('pending_outbox')):
            raise ValueError('probe_accounting_or_pending_mismatch')
        report.update({'valid': True, 'auxiliary_command_count': command_proof['command_count'], 'receipt_count': len(receipts),
                       'desired_version': str(version + 1), 'sample_interval_us': 250000,
                       'admission_response_sha256': hashlib.sha256(admitted_bytes).hexdigest(),
                       'noop_response_sha256': hashlib.sha256(noop_raw).hexdigest(),
                       'database_counts_before_noop': before_noop, 'database_counts_after_noop': after_noop})
    except Exception as exc:
        report['failures'].append(str(exc) if str(exc).startswith('probe_') else 'probe_execution_failed')
    return report


def _probe_http_request_with_headers(endpoint, path, token, key, body):
    parsed = urllib.parse.urlparse(endpoint.url)
    conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=10,
                                       context=ssl.create_default_context(cafile=str(endpoint.ca_file)))
    try:
        conn.request('PATCH', path + '/desired-state', body=json.dumps(body, separators=(',', ':')).encode(),
                     headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json',
                              'Idempotency-Key': key})
        response = conn.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError('probe_response_too_large')
        try:
            parsed = json.loads(raw.decode()) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            parsed = {}
        return response.status, raw, parsed
    finally:
        conn.close()


def _stop(proc, timeout=5):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def _stop_collector_and_process(collector, proc):
    collector_error = cleanup_failure = None
    try:
        collector.stop(proc.pid if proc else None)
    except Exception as exc:
        collector_error = exc
    try:
        _stop(proc)
    except Exception as exc:
        cleanup_failure = exc
    return collector_error, cleanup_failure


def _inspect_executor_sqlite(db_path):
    p = Path(db_path).resolve()
    if not p.is_file():
        return {
            'configuration': None,
            'receipts': [],
            'completed_receipts_count': 0,
            'pending_outbox': -1,
            'error': 'file_not_found',
        }
    uri = f"file:{p}?mode=ro"
    try:
        with sqlite3.connect(uri, uri=True) as conn:
            cursor = conn.cursor()
            config_summary = None
            try:
                cursor.execute(
                    "SELECT id, desired_version, sample_interval_us, applied_at "
                    "FROM sensor_configuration WHERE id=1;")
                config_row = cursor.fetchone()
                if config_row:
                    config_summary = {
                        'id': config_row[0],
                        'desired_version': str(config_row[1]),
                        'sample_interval_us': config_row[2],
                        'applied_at': config_row[3],
                    }
            except sqlite3.OperationalError:
                pass

            receipts_summary = []
            try:
                cursor.execute(
                    "SELECT command_id, command_hash, desired_version, sample_interval_us, "
                    "expires_at, status, result_bytes FROM command_receipts ORDER BY desired_version ASC;")
                receipt_rows = cursor.fetchall()
                for r in receipt_rows:
                    res_bytes = r[6]
                    res_len = len(res_bytes) if res_bytes is not None else 0
                    res_sha = hashlib.sha256(res_bytes).hexdigest() if res_bytes is not None else None
                    result_summary = None
                    if res_bytes is not None and res_len <= 8192:
                        try:
                            result = json.loads(res_bytes)
                            reported = result.get('reported_state') if isinstance(result, dict) else None
                            result_summary = {
                                'command_id': result.get('command_id'),
                                'desired_version': (str(result['desired_version'])
                                                    if result.get('desired_version') is not None else None),
                                'status': result.get('status'),
                                'sample_interval_us': (reported.get('sample_interval_us')
                                                       if isinstance(reported, dict) else None),
                            }
                        except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
                            pass
                    receipts_summary.append({
                        'command_id': r[0],
                        'command_hash': r[1],
                        'desired_version': str(r[2]),
                        'sample_interval_us': r[3],
                        'expires_at': r[4],
                        'status': r[5],
                        'result_bytes_len': res_len,
                        'result_hash': res_sha,
                        'result_summary': result_summary,
                    })
            except sqlite3.OperationalError:
                pass

            pending_outbox = -1
            try:
                cursor.execute("SELECT count(*) FROM outbox WHERE status='pending';")
                row = cursor.fetchone()
                if row:
                    pending_outbox = row[0]
            except sqlite3.OperationalError:
                pass

        return {
            'configuration': config_summary,
            'receipts': receipts_summary,
            'completed_receipts_count': sum(1 for r in receipts_summary if r['status'] == 'completed'),
            'pending_outbox': pending_outbox,
        }
    except Exception:
        return {
            'configuration': None,
            'receipts': [],
            'completed_receipts_count': 0,
            'pending_outbox': -1,
            'error': 'sqlite_read_failed',
        }


def _verify_control_receipts(sqlite_summaries, commands, postgres_receipts, rest_configurations,
                             executor_sensor_ids):
    """Reconcile private executor receipts to their Sensor's server records and REST state."""
    verified = True
    by_key = {row.get('idempotency_key'): row for row in postgres_receipts.values()}
    expected_ids_by_executor = []
    for idx, data in enumerate(sqlite_summaries):
        sid = executor_sensor_ids[idx] if idx < len(executor_sensor_ids) else None
        executor_commands = [c for c in commands if c.get('sensor_id') == sid]
        expected_by_id = {}
        for command in executor_commands:
            pg = by_key.get(command.get('idempotency_key'))
            if pg:
                expected_by_id[pg.get('command_id')] = (command, pg)
        expected_ids = set(expected_by_id)
        actual_ids = [r.get('command_id') for r in data.get('receipts', [])]
        expected_ids_by_executor.append(expected_ids)
        if (data.get('completed_receipts_count') != 2 or data.get('pending_outbox') != 0
                or set(actual_ids) != expected_ids or len(actual_ids) != 2):
            verified = False
        for receipt in data.get('receipts', []):
            pair = expected_by_id.get(receipt.get('command_id'))
            if not pair:
                verified = False
                continue
            command, pg = pair
            summary = receipt.get('result_summary') or {}
            expected_version = str(int(command['expected_desired_version']) + 1)
            try:
                frequency = Fraction(str(command['frequency_hz']))
                numerator = 1_000_000 * frequency.denominator
                quotient, remainder = divmod(numerator, frequency.numerator)
                expected_interval = quotient + (2 * remainder >= frequency.numerator)
            except (KeyError, TypeError, ValueError, ZeroDivisionError):
                expected_interval = None
            if (receipt.get('status') != 'completed' or pg.get('status') != 'succeeded'
                    or receipt.get('result_hash') != pg.get('result_hash')
                    or receipt.get('desired_version') != pg.get('desired_version')
                    or receipt.get('sample_interval_us') != pg.get('sample_interval_us')
                    or receipt.get('desired_version') != expected_version
                    or receipt.get('sample_interval_us') != expected_interval
                    or summary.get('command_id') != receipt.get('command_id')
                    or summary.get('desired_version') != receipt.get('desired_version')
                    or summary.get('status') != 'succeeded'
                    or summary.get('sample_interval_us') != receipt.get('sample_interval_us')):
                verified = False
        config = data.get('configuration')
        rest = rest_configurations.get(sid)
        if (not isinstance(config, dict) or not isinstance(rest, dict)
                or str(config.get('desired_version')) != str(rest.get('desired_version'))
                or config.get('sample_interval_us') != rest.get('sample_interval_us')):
            verified = False
    if len(expected_ids_by_executor) != 2 or expected_ids_by_executor[0] & expected_ids_by_executor[1]:
        verified = False
    return verified


def _pending_outbox_identities(db_path, max_rows=500, max_bytes=4 << 20):
    """Read, but never mutate, a bounded snapshot of pending payload identities."""
    p = Path(db_path).resolve()
    with sqlite3.connect(f'file:{p}?mode=ro', uri=True) as conn:
        rows = conn.execute(
            "SELECT message_id, payload, byte_size, gateway_id, sensor_id, first_sequence, sample_count "
            "FROM outbox WHERE status='pending' ORDER BY id LIMIT ?",
            (max_rows + 1,)).fetchall()
    if len(rows) > max_rows or sum(int(row[2]) for row in rows) > max_bytes:
        raise ValueError('p8_e3_pending_outbox_exceeds_capacity')
    return [{'message_id': row[0], 'payload_sha256': hashlib.sha256(row[1]).hexdigest(),
             'payload_bytes': len(row[1]), 'byte_size': int(row[2]),
             'gateway_id': row[3], 'sensor_id': row[4], 'first_sequence': int(row[5]),
             'sample_count': int(row[6])} for row in rows]


def _eligible_post_stop_drain(artifact, outputs, executor_summaries):
    """Only recover a clean, confirmed stop whose sole defect is valid pending rows."""
    if (not isinstance(artifact, dict) or artifact.get('status') != 'PARTIAL'
            or artifact.get('primary_error') is not None
            or artifact.get('cleanup_errors') != []
            or (artifact.get('phase_observations') or {}).get('errors') != []
            or artifact.get('actual_stop_confirmed') is not True):
        return False
    children = artifact.get('children')
    if (not isinstance(children, list) or not children
            or any(not isinstance(child, dict) or child.get('status') not in
                   ('complete', 'stopped', 'stopped_after_kill') for child in children)):
        return False
    if not outputs or any(not isinstance(row, dict) or row.get('status') != 'COMPLETE'
                          for row in outputs):
        return False
    return bool(executor_summaries) and all(
        row.get('pending_outbox', -1) >= 0 for row in executor_summaries) and any(
        row.get('pending_outbox', 0) > 0 for row in executor_summaries)


def _post_stop_drain_verified(returncode, summary, prior_pending, pending_after,
                              sqlite_after, config_before, identity_checks):
    return (returncode == 0 and summary.get('completed') == 'true'
            and int(summary.get('enqueued', '-1')) >= 1
            and int(summary.get('app_acknowledged', '-1')) >= len(prior_pending)
            and summary.get('pending_rows') == '0' and summary.get('pending_bytes') == '0'
            and not pending_after and sqlite_after.get('pending_outbox') == 0
            and sqlite_after.get('configuration') == config_before
            and all(row.get('ledger_hash') == row['payload_sha256']
                    and row.get('sample_count') == row['sample_count']
                    and row.get('raw_count') == row['sample_count']
                    and row.get('gateway_id') == row['gateway_id']
                    and row.get('sensor_id') == row['sensor_id']
                    and row.get('first_sequence') == row['first_sequence']
                    for row in identity_checks))


def _drain_executor_outboxes(smoke, fixture, registry, pg, gateway_binary, env_files, db_paths):
    """Sequential bounded reopen using the existing gateway CLI and private env."""
    from stage3_uplink_runner import parse_run_summary
    results = []
    for index, (env_path, db_path) in enumerate(zip(env_files, db_paths)):
        before = _pending_outbox_identities(db_path)
        if not before:
            results.append({'executor': index + 1, 'prior_pending': [], 'skipped': 'no_pending_rows'})
            continue
        config_before = _inspect_executor_sqlite(db_path).get('configuration')
        env = _gateway_environment(str(env_path))
        env['GATEWAY_SAMPLES_PER_BATCH'] = '1'
        env['GATEWAY_MQTT_CLIENT_ID'] += '_poststop_drain'
        drain_env_path = Path(env_path).with_name(Path(env_path).stem + '_drain.json')
        smoke.private_write(drain_env_path, json.dumps(env, sort_keys=True))
        env = _gateway_environment(str(drain_env_path))
        stdout_path = Path(env_path).with_name(f'executor_{index + 1}_drain.log')
        with stdout_path.open('xb') as stream:
            stdout_path.chmod(0o600)
            proc = subprocess.Popen([str(gateway_binary), 'run', '--batches', '1',
                                     '--completion-timeout', '30s'], cwd=smoke.ROOT,
                                    stdout=stream, stderr=subprocess.STDOUT,
                                    env={**smoke.process_env(fixture), **env}, close_fds=True)
            try:
                proc.wait(timeout=40)
            except subprocess.TimeoutExpired:
                _stop(proc, timeout=5)
                raise ValueError('p8_e3_post_stop_drain_timeout') from None
        raw = stdout_path.read_text(encoding='utf-8', errors='replace')
        summary = parse_run_summary(raw)
        after = _pending_outbox_identities(db_path)
        sqlite_after = _inspect_executor_sqlite(db_path)
        identity_checks = []
        for pending in before:
            row = smoke.db_psql(registry, pg, """SELECT json_build_object(
              'ledger_hash',(SELECT payload_hash FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'mid'),
              'sample_count',(SELECT sample_count FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'mid'),
              'raw_count',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor' AND message_id=:'mid'),
              'gateway_id',(SELECT gateway_id::text FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'mid'),
              'sensor_id',(SELECT sensor_id::text FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'mid'),
              'first_sequence',(SELECT first_sequence FROM processed_messages WHERE gateway_id=:'gw' AND message_id=:'mid'))::text;""",
              {'gw': pending['gateway_id'], 'sensor': pending['sensor_id'], 'mid': pending['message_id']}).strip()
            evidence = json.loads(row) if row else {}
            identity_checks.append({'message_id': pending['message_id'],
                                    'payload_sha256': pending['payload_sha256'],
                                    'ledger_hash': evidence.get('ledger_hash'),
                                    'sample_count': evidence.get('sample_count'),
                                    'raw_count': evidence.get('raw_count'),
                                    'gateway_id': evidence.get('gateway_id'),
                                    'sensor_id': evidence.get('sensor_id'),
                                    'first_sequence': evidence.get('first_sequence'),
                                    **{key: pending[key] for key in
                                       ('sample_count', 'gateway_id', 'sensor_id', 'first_sequence')}})
        if not _post_stop_drain_verified(proc.returncode, summary, before, after,
                                         sqlite_after, config_before, identity_checks):
            raise ValueError('p8_e3_post_stop_drain_rejected')
        results.append({'executor': index + 1, 'prior_pending': before,
                        'summary': {k: summary.get(k) for k in
                                    ('completed', 'enqueued', 'app_acknowledged', 'pending_rows', 'pending_bytes')},
                        'new_batch_profile': {'samples_per_batch': 1, 'excluded_from_background': True},
                        'backend_identity_checks': identity_checks,
                        'stdout': str(stdout_path), 'pending_after': after})
    return results


def run(smoke, fixture, endpoint, registry, containers, images, state,
        paperload_binary, gateway_binary, arm, workload='mixed'):
    """Execute E3 pilot workload for given arm (polling or wakeup) in disposable fixture."""
    pilot_arms = e3_pilot_arms()
    if arm not in pilot_arms:
        raise ValueError(f'p8_e3_arm_rejected:{arm}')
    if workload not in ('mixed', 'low_load'):
        raise ValueError('p8_e3_workload_rejected')
    artifact_name = f"p8-e3-{arm}{'-low-load' if workload == 'low_load' else ''}.json"
    arm_profile = pilot_arms[arm]

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
    host_ip = ports[0].get('HostIp')
    _support.validate_mqtt_readback({'service': 'mosquitto', 'host_ip': host_ip,
        'target': 8883, 'published': published}, owned_service='mosquitto')
    mqtt_broker_url = f'ssl://127.0.0.1:{published}'
    manifest_mqtt_endpoint = f'mqtts://127.0.0.1:{published}'

    root = fixture.private / 'p8-e3' / arm
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    ca_path = fixture.private / 'certs/ca.crt'

    gw_id = specs[0].gateway_id
    cmd_sensor_names = ('a', 'b')
    cmd_sensor_ids = [f'p8-e3-command-{x}' for x in cmd_sensor_names]
    initial_versions = {}
    control_targets = []

    for sid in cmd_sensor_ids:
        urn = f'urn:ngsi-ld:Sensor:{gw_id}:{sid}'
        put_status, _, put_body = smoke.http_request(
            endpoint, 'PUT', f'/v1/gateways/{gw_id}/sensors/{sid}',
            token=tokens['a'], body={'name': f'P8 E3 Command Sensor {sid}', 'unit': specs[0].sensor_unit})
        if put_status != 201 or put_body.get('entity_id') != urn:
            raise ValueError(f'p8_e3_command_sensor_provision_failed:{sid}')

        get_status, _, detail = smoke.http_request(
            endpoint, 'GET', f'/v1/digital-twins/{urn}?format=simplified',
            token=tokens['a'])
        if get_status != 200 or not isinstance(detail, dict):
            raise ValueError(f'p8_e3_command_sensor_get_failed:{sid}')

        version = detail.get('desired_version')
        if version is None and isinstance(detail.get('desired_state'), dict):
            version = detail['desired_state'].get('desired_version')
        if version is None:
            raise ValueError(f'p8_e3_initial_desired_version_unavailable:{sid}')
        initial_versions[sid] = str(version)
        control_targets.append({'gateway_id': gw_id, 'sensor_id': sid})

    background_targets = [
        {'gateway_id': specs[0].gateway_id, 'sensor_id': specs[0].sensor_id},
        {'gateway_id': specs[1].gateway_id, 'sensor_id': specs[1].sensor_id},
    ]
    gateway_files = {}
    telemetry_files = {}
    for spec, password, role in zip(specs, passwords, ('A', 'B')):
        identity = root / f'gateway-{spec.gateway_id}.json'
        smoke.private_write(identity, json.dumps({'username': spec.gateway_id, 'password': password}, sort_keys=True))
        gateway_files[spec.gateway_id] = str(identity)
        telemetry = root / f'telemetry-{role}-identities.json'
        smoke.private_write(telemetry, '{}')
        telemetry_files[role] = str(telemetry)

    control_token_file = root / 'control-tokens.json'
    smoke.private_write(control_token_file, json.dumps({'e3-operator': tokens['a']}, sort_keys=True))

    common = {
        'mode': 'local',
        'run_id': f'{fixture.project}-e3-{arm}',
        'seed': 89,
        'bundle_path': str(root),
        'publish_timeout_ms': 3000,
    }
    tls_config = {
        'https_endpoint': endpoint.url,
        'https_ca_file': str(endpoint.ca_file),
        'mqtt_tls_endpoint': manifest_mqtt_endpoint,
        'mqtt_tls_ca_file': str(ca_path),
        'mqtt_tls_server_name': 'localhost',
    }
    manifest_bundle = e3_pilot_manifests(
        common=common,
        background_targets=background_targets,
        background_gateway_identity_files=gateway_files,
        telemetry_identity_files=telemetry_files,
        control_targets=control_targets,
        initial_versions=initial_versions,
        control_identity_file=str(control_token_file),
        tls=tls_config,
        workload=workload,
    )

    gateway_env_files = []
    executor_outbox_paths = []
    for idx, sid in enumerate(cmd_sensor_ids):
        client_id = f'{gw_id}_p8_e3_executor_{idx + 1}'
        outbox_path = str((root / f'executor_{idx + 1}.sqlite').resolve())
        executor_outbox_paths.append(outbox_path)

        base_env = gateway_env(gw_id, passwords[0])
        base_env.update({
            'GATEWAY_CONTROL_ENABLED': 'true',
            'GATEWAY_MQTT_BROKER_URL': mqtt_broker_url,
            'GATEWAY_MQTT_CA_FILE': str(ca_path.resolve()),
            'GATEWAY_MQTT_SERVER_NAME': 'localhost',
            'GATEWAY_SAMPLES_PER_BATCH': '1',
            'GATEWAY_OUTBOX_PATH': outbox_path,
            'GATEWAY_OUTBOX_MAX_ROWS': '500',
            'GATEWAY_OUTBOX_MAX_BYTES': str(4 << 20),
            'GATEWAY_SHUTDOWN_TIMEOUT': '10s',
            'GATEWAY_ACK_TIMEOUT': '5s',
            'GATEWAY_RETRY_MIN_DELAY': '250ms',
            'GATEWAY_RETRY_MAX_DELAY': '2s',
            'GATEWAY_SQLITE_TIMEOUT': '5s',
            'GATEWAY_MAX_INFLIGHT': '8',
        })
        clean_env = _executor_environment(base_env, sid, client_id)

        env_file = root / f'executor_{idx + 1}.env.json'
        smoke.private_write(env_file, json.dumps(clean_env, sort_keys=True))
        _gateway_environment(str(env_file))
        gateway_env_files.append(str(env_file))

    plan_dir = root / 'plan'
    plan = build_e3_execution_plan(
        output_dir=plan_dir,
        manifests={
            'A': manifest_bundle['background']['A'],
            'B': manifest_bundle['background']['B'],
            'control': manifest_bundle['control'],
        },
        gateway_env_files=gateway_env_files,
        shutdown_timeouts_seconds=[10, 10],
    )

    before_global = smoke.paper_p1_count_db(registry, pg)
    before_bg_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
    before_cmd_targets = [
        _target_counts(smoke, registry, pg, SimpleNamespace(
            gateway_id=gw_id, sensor_id=sid, sensor_urn=f'urn:ngsi-ld:Sensor:{gw_id}:{sid}'))
        for sid in cmd_sensor_ids
    ]
    baseline_admin = _support._admin_metrics(smoke, endpoint, tokens['admin'])
    baseline_server = smoke.paper_p0_stats(registry, containers, 'workload_start')
    collector = BoundedCollector(
        scrape=lambda: _support._admin_metrics(smoke, endpoint, tokens['admin']),
        interval=0.5,
        max_snapshots=64,
        deadline_seconds=60,
        baseline_metrics=baseline_admin,
        baseline_server=baseline_server,
    )

    log = plan_dir / 'runner.log'
    proc = None
    failure = None
    run_artifact = plan_dir / 'run.json'

    with log.open('xb') as stream:
        log.chmod(0o600)
        try:
            proc = subprocess.Popen([
                'python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run',
                '--binary', str(paperload_binary),
                '--gateway-binary', str(gateway_binary),
                '--run-spec', plan['run_spec'],
                '--run-artifact', str(run_artifact),
            ], cwd=smoke.ROOT, stdout=stream, stderr=subprocess.STDOUT,
               env=smoke.process_env(fixture), close_fds=True)
            collector.start(proc.pid)
            try:
                proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                raise ValueError('p8_e3_runner_timeout') from None
        except Exception as exc:
            failure = exc
        finally:
            collector_error, cleanup_failure = _stop_collector_and_process(collector, proc)
            if failure is None and (collector_error or cleanup_failure):
                failure = RuntimeError('p8_e3_cleanup_failed')

    if failure:
        smoke.durable_json(fixture.artifacts / artifact_name, {
            'status': 'FAIL', 'failure': 'runner_exception', 'arm': arm,
            'error': 'runner_or_cleanup_error',
            'collector_cleanup_failed': collector_error is not None,
            'process_cleanup_failed': cleanup_failure is not None,
            'full_p8_accepted': False,
        })
        raise failure

    if proc is None or proc.returncode != 0:
        # The child runner intentionally exits nonzero for its recorded PARTIAL
        # result. Continue only when that exact clean-stop/pending-outbox case is
        # eligible for the separately reported bounded drain below.
        candidate_artifact = {}
        try:
            candidate_artifact = json.loads(run_artifact.read_text(encoding='utf-8'))
            candidate_outputs = [
                json.loads((plan_dir / output / 'workload.json').read_text(encoding='utf-8'))
                for output in ('A-output', 'B-output', 'control-output')]
            candidate_sqlite = [_inspect_executor_sqlite(path) for path in executor_outbox_paths]
        except (OSError, ValueError, json.JSONDecodeError):
            candidate_outputs, candidate_sqlite = [], []
        if not _eligible_post_stop_drain(candidate_artifact, candidate_outputs, candidate_sqlite):
            smoke.durable_json(fixture.artifacts / artifact_name, {
                'status': 'FAIL', 'failure': 'runner_nonzero_or_missing', 'arm': arm,
                'returncode': proc.returncode if proc else None, 'full_p8_accepted': False,
            })
            raise ValueError(f'p8_e3_runner_failed_code_{proc.returncode if proc else None}')

    after_admin = _support._admin_metrics(smoke, endpoint, tokens['admin'])
    terminal_server = smoke.paper_p0_stats(registry, containers, 'workload_terminal')
    collector.metrics.append({'phase': 'workload_terminal', 'snapshot': after_admin})
    collector.resources.append(terminal_server)
    after_global = smoke.paper_p1_count_db(registry, pg)
    after_bg_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
    after_cmd_targets = [
        _target_counts(smoke, registry, pg, SimpleNamespace(
            gateway_id=gw_id, sensor_id=sid, sensor_urn=f'urn:ngsi-ld:Sensor:{gw_id}:{sid}'))
        for sid in cmd_sensor_ids
    ]

    telemetry_a_file = plan_dir / 'A-output' / 'workload.json'
    telemetry_b_file = plan_dir / 'B-output' / 'workload.json'
    control_file = plan_dir / 'control-output' / 'workload.json'

    try:
        telemetry_a = json.loads(telemetry_a_file.read_text(encoding='utf-8'))
    except Exception:
        telemetry_a = {}
    try:
        telemetry_b = json.loads(telemetry_b_file.read_text(encoding='utf-8'))
    except Exception:
        telemetry_b = {}
    try:
        control_workload = json.loads(control_file.read_text(encoding='utf-8'))
    except Exception:
        control_workload = {}

    try:
        runner_artifact = json.loads(run_artifact.read_text(encoding='utf-8'))
    except Exception:
        runner_artifact = {}
    initial_sqlite = [_inspect_executor_sqlite(path) for path in executor_outbox_paths]
    post_stop_drain = []
    drain_command_counts = []
    if _eligible_post_stop_drain(
            runner_artifact, [telemetry_a, telemetry_b, control_workload], initial_sqlite):
        snapshots = [_pending_outbox_identities(path) for path in executor_outbox_paths]
        if not any(snapshots):
            raise ValueError('p8_e3_post_stop_drain_identity_missing')
        drain_before = [
            _target_counts(smoke, registry, pg, SimpleNamespace(
                gateway_id=gw_id, sensor_id=sid, sensor_urn=f'urn:ngsi-ld:Sensor:{gw_id}:{sid}'))
            for sid in cmd_sensor_ids]
        post_stop_drain = _drain_executor_outboxes(
            smoke, fixture, registry, pg, gateway_binary, gateway_env_files, executor_outbox_paths)
        if [row['prior_pending'] for row in post_stop_drain] != snapshots:
            raise ValueError('p8_e3_post_stop_drain_identity_changed')
        # The drain's generated samples are command-Sensor traffic, not measured background.
        after_admin = _support._admin_metrics(smoke, endpoint, tokens['admin'])
        terminal_server = smoke.paper_p0_stats(registry, containers, 'post_stop_drain_terminal')
        collector.metrics.append({'phase': 'post_stop_drain_terminal', 'snapshot': after_admin})
        collector.resources.append(terminal_server)
        after_bg_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
        after_cmd_targets = [
            _target_counts(smoke, registry, pg, SimpleNamespace(
                gateway_id=gw_id, sensor_id=sid, sensor_urn=f'urn:ngsi-ld:Sensor:{gw_id}:{sid}'))
            for sid in cmd_sensor_ids]
        drain_command_counts = [
            {'sensor_id': sid,
             'new_ledger': after_cmd_targets[i]['ledger'] - drain_before[i]['ledger'],
             'new_raw': after_cmd_targets[i]['raw'] - drain_before[i]['raw'],
             'new_temporal': after_cmd_targets[i]['temporal'] - drain_before[i]['temporal']}
            for i, sid in enumerate(cmd_sensor_ids)]

    raw_checks = []
    for index, (arm_role, telemetry_workload) in enumerate((('A', telemetry_a), ('B', telemetry_b))):
        producer_ids = [row.get('message_id') for row in telemetry_workload.get('identity_samples', [])
                        if isinstance(row, dict) and isinstance(row.get('message_id'), str)]
        spec = specs[index]
        raw_checks.append(verify_pilot_raw(smoke, registry, pg, spec, message_ids=producer_ids))

    bg_delta = {
        key: (after_bg_targets[0][key] - before_bg_targets[0][key]) +
             (after_bg_targets[1][key] - before_bg_targets[1][key])
        for key in ('ledger', 'raw', 'temporal')
    }
    cmd_delta = {
        'raw_samples': sum(after_cmd_targets[i]['raw'] - before_cmd_targets[i]['raw'] for i in range(2)),
        'temporal_observations': sum(after_cmd_targets[i]['temporal'] - before_cmd_targets[i]['temporal'] for i in range(2)),
    }

    declared_keys = manifest_bundle['idempotency_keys']
    verified_idempotency_keys = []
    postgres_receipts = {}
    for key in declared_keys:
        sql = """SELECT json_build_object(
            'idempotency_key', r.idempotency_key,
            'status_code', r.status_code,
            'command_id', r.command_id::text,
            'status', c.status,
            'result_hash', c.result_hash,
            'desired_version', c.desired_version::text,
            'sample_interval_us', (c.parameters->>'sample_interval_us')::bigint
        )::text
        FROM twin_command_requests r
        JOIN twin_commands c ON c.command_id = r.command_id
        WHERE r.idempotency_key = :'key';"""
        row_text = smoke.db_psql(registry, pg, sql, {'key': key}).strip()
        if row_text:
            row = json.loads(row_text)
            if row.get('status_code') == 202 and row.get('status') == 'succeeded' and row.get('result_hash'):
                verified_idempotency_keys.append(key)
                postgres_receipts[key] = row

    sqlite_summaries = []
    for idx, db_path in enumerate(executor_outbox_paths):
        sqlite_data = _inspect_executor_sqlite(db_path)
        sqlite_summaries.append(sqlite_data)

    expected_final_configs = {
        cmd_sensor_ids[0]: {
            'desired_version': manifest_bundle['expected_versions_after'][cmd_sensor_ids[0]],
            'sample_interval_us': 500000,
        },
        cmd_sensor_ids[1]: {
            'desired_version': manifest_bundle['expected_versions_after'][cmd_sensor_ids[1]],
            'sample_interval_us': 1333333,
        },
    }
    rest_configurations = {}
    for sid in cmd_sensor_ids:
        urn = f'urn:ngsi-ld:Sensor:{gw_id}:{sid}'
        status, _, body = smoke.http_request(
            endpoint, 'GET', f'/v1/digital-twins/{urn}?format=simplified',
            token=tokens['a'])
        if status != 200 or not isinstance(body, dict):
            raise ValueError(f'p8_e3_rest_config_read_failed:{sid}')
        rep = body.get('reported_state', {})
        if not isinstance(rep, dict):
            rep = {}
        config = rep.get('configuration', {})
        rest_configurations[sid] = config
        expected = expected_final_configs[sid]
        if (str(config.get('desired_version')) != expected['desired_version']
                or config.get('sample_interval_us') != expected['sample_interval_us']):
            raise ValueError(f'p8_e3_rest_config_mismatch:{sid}')

    sqlite_receipts_verified = _verify_control_receipts(
        sqlite_summaries, manifest_bundle['control']['commands'], postgres_receipts,
        rest_configurations, cmd_sensor_ids)
    control_receipt_verification = {
        'verified': (len(verified_idempotency_keys) == 4 and sqlite_receipts_verified),
        'verified_idempotency_keys': verified_idempotency_keys,
    }

    run_json_path = plan_dir / 'run.json'
    try:
        phase_events = json.loads(run_json_path.read_text(encoding='utf-8')).get('phase_observations', {}).get('events', [])
    except Exception:
        phase_events = []

    runner_stopped = (runner_artifact.get('actual_stop_confirmed') is True
                      and (runner_artifact.get('status') == 'COMPLETE' or bool(post_stop_drain)))

    collector_failed = bool(collector.errors)
    if collector_failed:
        validation_stub = {'valid': False, 'availability': 'inconclusive',
                           'failures': ['collector_failed']}
        smoke.durable_json(fixture.artifacts / artifact_name, {
            'status': 'FAIL', 'failure': 'collector_failed', 'reason': 'collector_readback_failed',
            'validation': validation_stub, 'full_p8_accepted': False})
        raise ValueError('p8_e3_collector_failed')

    validation = validate_e3_result(
        control_workload=control_workload,
        telemetry_a=telemetry_a,
        telemetry_b=telemetry_b,
        phase_events=phase_events,
        background_database_delta=bg_delta,
        command_database_delta=cmd_delta,
        control_receipt_verification=control_receipt_verification,
        runner_stopped=runner_stopped,
        declared_idempotency_keys=declared_keys,
        raw_checks=raw_checks,
        workload=workload,
    )

    if not validation['valid']:
        smoke.durable_json(fixture.artifacts / artifact_name, {
            'status': 'FAIL', 'failure': 'validation_failed', 'arm': arm,
            'failures': validation['failures'], 'full_p8_accepted': False,
        })
        raise ValueError('p8_e3_validation_failed:' + ','.join(validation['failures']))

    # Kept after measured validation and collector terminal snapshot: these
    # representative requests must never enter the fresh-command histogram.
    representative_probes = run_representative_probes(
        smoke, fixture, endpoint, registry, pg, tokens['a'], gateway_binary,
        gateway_env_files[0], executor_outbox_paths[0], gw_id, cmd_sensor_ids[0],
        f'urn:ngsi-ld:Sensor:{gw_id}:{cmd_sensor_ids[0]}')

    histogram_windows = {}
    for group in ('command_admission', 'command_dispatch', 'command_timeline', 'command_result'):
        for name, value in (after_admin.get(group) or {}).items():
            if isinstance(value, dict) and 'buckets' in value:
                histogram_windows[f'{group}.{name}'] = smoke.paper_p1_histogram_window(
                    baseline_admin, after_admin, group, name)

    ctl_timeline = after_admin.get('command_timeline', {}) if isinstance(after_admin, dict) else {}
    missing_origin = ctl_timeline.get('missing_origin') if isinstance(ctl_timeline, dict) else None

    smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', collector.metrics)
    smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv',
        [r for r in collector.resources if r.get('services') and not r.get('error')], resources)

    result = {
        'workload': workload,
        'workload_input_difference': ('B burst_messages=0 (mixed uses 12)' if workload == 'low_load' else None),
        'full_p8_accepted': False,
        'status': ('WORKLOAD_COMPLETE_PENDING_CLEANUP' if representative_probes['valid'] else 'FAIL'),
        'arm': arm,
        'full_p8_accepted': False,
        'schema': schema,
        'quota': quota,
        'effective_flags': _validate_e3_fixture_flags(effective, arm),
        'fixture_profile': arm_profile,
        'storage_status': storage_status,
        'source_revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'source_fingerprint': _support._source_fingerprint(smoke.ROOT),
        'binary_sha256': hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest(),
        'gateway_binary_sha256': (hashlib.sha256(Path(gateway_binary).read_bytes()).hexdigest()
                                  if gateway_binary and Path(gateway_binary).is_file() else None),
        'manifest_sha256': {
            name: hashlib.sha256(Path(plan_dir / f'{name}.manifest.json').read_bytes()).hexdigest()
            for name in ('A', 'B', 'control')
        },
        'images': {name: {'id': value} for name, value in images.items()},
        'client_server_clock_qualification': {'available': None, 'reason': 'independent clocks'},
        'accounting': {
            'background_database_delta': bg_delta,
            'command_database_delta': cmd_delta,
            'telemetry_a': telemetry_a.get('telemetry'),
            'telemetry_b': telemetry_b.get('telemetry'),
            'control': {
                'planned': control_workload.get('planned'),
                'sent': control_workload.get('sent'),
                'skipped': control_workload.get('skipped'),
                'pending': control_workload.get('pending'),
            },
        },
        'raw_verification': raw_checks,
        'control_reconciliation': {
            'receipt_verification': control_receipt_verification,
            'sqlite_executors': sqlite_summaries,
            'rest_configuration': rest_configurations,
        },
        'validation': validation,
        'runner_stopped': runner_stopped,
        'initial_runner_artifact_status': runner_artifact.get('status'),
        'post_stop_drain': post_stop_drain,
        'post_stop_drain_command_counts': drain_command_counts,
        'server_origin_latency': {
            'histogram_windows': histogram_windows,
            'missing_origin': missing_origin,
            'note': 'missing origin counts remain missing; HTTP RTT never substituted',
        },
        'collector_resources': collector.resources,
        'collector_generator_samples': collector.generator_samples,
        'representative_probes': representative_probes,
        'measurement_scope': {'representative_probes_outside_primary_window': True,
                              'auxiliary_commands': 1,
                              'note': 'known representative probes are post-validation and excluded from primary command measurements'},
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }
    smoke.durable_json(fixture.artifacts / artifact_name, result)
    smoke.durable_json(fixture.artifacts / 'run.json', result)
    if not representative_probes['valid']:
        raise ValueError('p8_e3_representative_probes_failed')
    return result
