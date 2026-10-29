"""Pure E5 deterministic journal/profile/comparison adapters; no fixture control."""
import base64
import hashlib
import json
import os
from pathlib import Path
import random
import re
import uuid
from datetime import datetime, timedelta, timezone

import paper_p8_comparison as comparison

ARMS = comparison.ARM_NAMES
MAX_JOURNAL_BYTES = 1 << 20
_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$')


def _uuid4(rng):
    raw = bytearray(rng.getrandbits(8) for _ in range(16))
    raw[6] = (raw[6] & 0x0f) | 0x40
    raw[8] = (raw[8] & 0x3f) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def build_dataset(*, seed, sensor_ids, anchor, path):
    """Write one bounded deterministic E4-compatible JSONL journal, private/exclusive."""
    if type(seed) is not int or seed < 0 or not isinstance(sensor_ids, (list, tuple)) or len(sensor_ids) != 2:
        raise ValueError('e5_dataset_inputs_invalid')
    if any(not isinstance(value, str) or not _ID.fullmatch(value) for value in sensor_ids):
        raise ValueError('e5_sensor_id_invalid')
    try:
        instant = datetime.fromisoformat(anchor.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError):
        raise ValueError('e5_anchor_invalid') from None
    if instant.tzinfo is None or instant.utcoffset() != timedelta(0):
        raise ValueError('e5_anchor_must_be_utc')
    target = Path(path)
    try:
        parent_stat = target.parent.lstat()
    except OSError:
        raise ValueError('e5_dataset_parent_must_exist_and_not_symlink') from None
    if (not target.parent.is_dir() or target.parent.is_symlink() or
            parent_stat.st_uid != os.getuid() or parent_stat.st_mode & 0o777 != 0o700):
        raise ValueError('e5_dataset_parent_must_exist_and_not_symlink')
    rng = random.Random(seed)
    gateway_ids = ('p8-e5-a', 'p8-e5-b')
    boot_ids = {(gateway, sensor): _uuid4(rng)
                for gateway, sensor in zip(gateway_ids, sensor_ids)}
    records = []
    for index in range(20):
        gw_index = index % 2
        gateway_id, sensor_id = gateway_ids[gw_index], sensor_ids[index % 2]
        first_sequence = str((index // 2) * 100 + 1)
        measured_at = (instant + timedelta(seconds=index)).isoformat(timespec='microseconds').replace('+00:00', 'Z')
        first = int(first_sequence)
        samples = []
        for sequence in range(first, first + 100):
            position = sequence % 2000
            if position > 1000:
                position = 2000 - position
            samples.append(20 + position / 100.0)
        payload_obj = {'protocol_version': 1, 'message_id': _uuid4(rng),
            'message_type': 'telemetry_batch', 'gateway_id': gateway_id, 'sensor_id': sensor_id,
            'boot_id': boot_ids[(gateway_id, sensor_id)], 'first_sequence': first, 'sample_count': 100,
            'measured_at': measured_at, 'sample_interval_us': 10000, 'samples': samples}
        payload = json.dumps(payload_obj, separators=(',', ':'), ensure_ascii=False).encode()
        digest = hashlib.sha256(payload).hexdigest()
        records.append({'topic': f'gateways/{gateway_id}/telemetry/{sensor_id}',
            'gateway_id': gateway_id, 'sensor_id': sensor_id,
            'message_id': payload_obj['message_id'], 'boot_id': payload_obj['boot_id'],
            'first_sequence': first_sequence, 'sample_count': 100,
            'payload_sha256': digest, 'payload': base64.b64encode(payload).decode('ascii')})
    content = b''.join(json.dumps(row, sort_keys=True, separators=(',', ':')).encode() + b'\n' for row in records)
    if len(content) > MAX_JOURNAL_BYTES:
        raise ValueError('e5_dataset_size_limit')
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        raise
    return {'path': str(target), 'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest(),
            'entries': len(records), 'gateway_ids': list(gateway_ids), 'sensor_ids': list(sensor_ids)}


def e5_arm_profile(arm):
    flags = comparison.get_expected_arm_flags(arm)
    return {**flags, 'MQTT_SHAPER_RATE_PER_SECOND': '2', 'MQTT_SHAPER_BURST': '4',
        'MQTT_MICROBATCH_MAX_MESSAGES': '32', 'MQTT_MICROBATCH_MAX_ROWS': '500',
        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms',
        'WS_MAX_SESSIONS': '32', 'WS_SESSION_REFERENCE_BYTES': str(8 << 20),
        'MQTT_WORKER_COUNT': '4', 'DATABASE_MAX_CONNS': '11',
        'STAGE5_PUBLISH_WORKERS': '1', 'STAGE5_RESULT_WORKERS': '2'}


def comparison_metadata(*, workload, journal_sha256, **fingerprints):
    if not re.fullmatch(r'[0-9a-f]{64}', str(journal_sha256)):
        raise ValueError('e5_journal_hash_invalid')
    result = dict(fingerprints)
    result['workload'] = dict(workload, journal_sha256=journal_sha256)
    result['workload_sha256'] = comparison.normalized_workload_fingerprint(result['workload']).removeprefix('sha256:')
    return result


def command_idempotency_keys(seed):
    """Derive the paired workload's four caller-owned idempotency keys."""
    if type(seed) is not int or seed < 0:
        raise ValueError('e5_command_seed_invalid')
    rng = random.Random(seed ^ 0xE5C0A11)
    return [_uuid4(rng) for _ in range(4)]


def workload_manifest(*, journal_sha256, journal_bytes, command_ids, sensor_ids, viewer_targets, anchor):
    """Normalized paired workload declaration; values are declarative, never executed here."""
    if (not re.fullmatch(r'[0-9a-f]{64}', str(journal_sha256)) or
            type(journal_bytes) is not int or journal_bytes < 1 or
            not isinstance(command_ids, (list, tuple)) or len(command_ids) != 4 or
            not isinstance(sensor_ids, (list, tuple)) or len(sensor_ids) != 2 or
            not isinstance(viewer_targets, (list, tuple)) or len(viewer_targets) not in (2, 3)):
        raise ValueError('e5_workload_manifest_inputs_invalid')
    return {'workload_type': 'p8_e5_prebuilt_journal', 'journal_sha256': journal_sha256,
        'journal_bytes': journal_bytes, 'schedule': {'initial_imported_messages': 20,
            'initial_replay': {'rate_per_second': 1, 'measure_seconds': 1, 'burst_messages': 19},
            'warm_replay_prefix': 10, 'replay_after_commit_ack_baseline': True,
            'generated_unique': 0},
        'telemetry': {'gateway_ids': ['p8-e5-a', 'p8-e5-b'], 'sensor_ids': list(sensor_ids),
            'messages': 20, 'samples_per_message': 100, 'sample_interval_us': 10000,
            'dataset_anchor': anchor},
        'commands': {'gateway_id': 'p8-e5-a', 'command_sensor_ids': ['p8-e5-command-a', 'p8-e5-command-b'],
            'idempotency_keys': list(command_ids), 'count': 4,
            'frequencies_hz': [2, 4], 'versions': {'initial': 0, 'after_each_sensor': 1},
            'executor': 'existing_e3_sqlite_executor'},
        'subscriptions': list(viewer_targets), 'viewer_count': 2,
        'security': {'tls': True, 'jwt': True, 'acl': True, 'application_commit_ack': True}}


def replay_accounting_matches(workload, telemetry, expected_count):
    """Validate the replay-specific counters emitted by the actual paperload schema."""
    return (isinstance(workload, dict) and isinstance(telemetry, dict) and
        workload.get('mode') == 'replay' and workload.get('journal_entry_count') == expected_count and
        telemetry.get('generated_unique') == 0 and telemetry.get('offered_unique') == 0 and
        telemetry.get('replay_offered') == expected_count and
        telemetry.get('replay_application_acks') == expected_count and
        telemetry.get('pending_messages') == 0 and telemetry.get('pending_bytes') == 0)


def _window_delta(before, after, section, fields):
    if not isinstance(before, dict) or not isinstance(after, dict):
        return {'status': 'INCONCLUSIVE', 'reason': 'counter_snapshot_missing'}
    if not before.get('process_instance') or before.get('process_instance') != after.get('process_instance'):
        return {'status': 'INCONCLUSIVE', 'reason': 'counter_process_changed'}
    parts = section.split('.')
    old, new = before, after
    for part in parts:
        old = old.get(part) if isinstance(old, dict) else None
        new = new.get(part) if isinstance(new, dict) else None
    if not isinstance(old, dict) or not isinstance(new, dict):
        return {'status': 'INCONCLUSIVE', 'reason': 'counter_section_missing'}
    result = {}
    for name, source in fields.items():
        a, b = old.get(source), new.get(source)
        if type(a) is not int or type(b) is not int:
            return {'status': 'INCONCLUSIVE', 'reason': 'counter_field_missing_' + source}
        if a < 0 or b < a:
            return {'status': 'INCONCLUSIVE', 'reason': 'counter_reset_' + source}
        result[name] = b - a
    return {'status': 'AVAILABLE', 'delta': result}


def adapt_mechanism_counters(before, after, *, arm, verified_commands=0, viewer_readiness=None):
    """Translate only actual server counters into comparison adapter field names."""
    metrics = {}
    if arm not in ARMS:
        raise ValueError('unknown_arm: ' + str(arm))
    mb = _window_delta(before, after, 'ingress.microbatch', {'groups': 'groups', 'members': 'members'})
    if mb.get('status') == 'AVAILABLE':
        # max_members is a window gauge, not a monotonically increasing total.
        latest = ((after.get('ingress') or {}).get('microbatch') or {}).get('max_members')
        if type(latest) is not int or latest < 0:
            mb = {'status': 'INCONCLUSIVE', 'reason': 'counter_field_missing_max_group_members'}
        else:
            mb['delta']['max_members'] = latest
    rd = _window_delta(before, after, 'ram_dedup', {
        'lookup_hit_total': 'lookup_hit_total', 'database_calls_avoided_total': 'database_calls_avoided_total'})
    cmd = _window_delta(before, after, 'command_dispatch', {
        'notification_signals_accepted': 'notification_signals_accepted',
        'puback': 'puback', 'scans': 'scans'})
    fan = _window_delta(before, after, 'websocket_metrics', {
        'indexed_selections': 'fanout_indexed_selections',
        'scan_evaluations': 'fanout_scan_evaluations', 'writes': 'telemetry_writes_completed'})
    ws_after = after.get('websocket_metrics') if isinstance(after, dict) else None
    readiness_ok = (isinstance(ws_after, dict) and ws_after.get('availability') == 'available' and
                    type(ws_after.get('subscriptions')) is int and ws_after['subscriptions'] >= 2 and
                    isinstance(viewer_readiness, (list, tuple)) and len(viewer_readiness) == 2 and
                    all(value is True for value in viewer_readiness))
    if fan.get('status') == 'AVAILABLE':
        if readiness_ok:
            fan['delta']['subscriptions_ready'] = 2
        else:
            fan = {'status': 'INCONCLUSIVE', 'reason': 'two_viewer_readiness_unverified'}
    for key, value in (('microbatch', mb), ('ram_dedup', rd), ('command', cmd), ('fanout', fan)):
        metrics[key] = value['delta'] if value['status'] == 'AVAILABLE' else value
    shaper = _window_delta(before, after, 'ingress', {
        'rate_limited': 'ShaperRateLimited', 'passed': 'ShaperPassed'})
    metrics['shaper'] = shaper['delta'] if shaper['status'] == 'AVAILABLE' else shaper
    if isinstance(metrics['microbatch'], dict) and 'status' not in metrics['microbatch']:
        groups, members = metrics['microbatch']['groups'], metrics['microbatch']['members']
        if groups > 0 and members > groups and metrics['microbatch']['max_members'] >= 2:
            metrics['microbatch']['multi_member_groups_lower_bound'] = 1
            metrics['microbatch']['multi_member_provenance'] = 'members_exceed_groups_and_max_members_gte_2'
    if type(verified_commands) is not int or verified_commands < 0:
        metrics['command']['fresh_commands_dispatched'] = {'status': 'INCONCLUSIVE', 'reason': 'verified_command_count_missing'}
    else:
        metrics['command']['fresh_commands_dispatched'] = verified_commands
    metrics['activation_status'] = 'AVAILABLE' if all(isinstance(metrics[k], dict) and 'status' not in metrics[k]
        for k in ('microbatch', 'ram_dedup', 'command', 'fanout', 'shaper')) else 'INCONCLUSIVE'
    return metrics
