"""Pure E3 pilot manifest, run-plan and result-accounting helpers.

No fixture orchestration or deployment mutation lives here; generated manifests
are private inputs for the existing paper_p8 runner.
"""
import importlib.util
import json
import re
import uuid
from pathlib import Path
from decimal import Decimal, InvalidOperation

_paper_p8_spec = importlib.util.spec_from_file_location(
    'paper_p8_for_e3', Path(__file__).with_name('paper_p8.py'))
_paper_p8 = importlib.util.module_from_spec(_paper_p8_spec)
_paper_p8_spec.loader.exec_module(_paper_p8)
GATEWAY_ENV_KEYS = _paper_p8.GATEWAY_ENV_KEYS

PILOT_MESSAGES = {'A': 8, 'B': 16}
LOW_LOAD_MESSAGES = {'A': 8, 'B': 4}
FREQUENCIES = ('1.5', '0.5', '2', '0.75')
OFFSETS_MS = (1000, 3800, 10000, 12000)
BASE_FLAGS = {'STAGE5_ENABLED': True, 'STAGE5_WAKEUP_ENABLED': False,
              'MQTT_SHAPER_ENABLED': True, 'MQTT_MICROBATCH_ENABLED': True,
              'MQTT_RAM_DEDUP_ENABLED': False, 'WEBSOCKET_ENABLED': False}


def e3_pilot_arms():
    """Return identical bounded arm profiles differing only in wake-up flag."""
    common = {'flags': dict(BASE_FLAGS), 'shaper': {'rate_per_second': 2, 'burst': 4},
              'microbatch': {'max_messages': 32, 'max_rows': 500,
                             'max_bytes': 4 << 20, 'wait_ms': 50},
              'control_isolation': 'existing bounded control path unchanged'}
    arms = {}
    for name, wakeup in (('polling', False), ('wakeup', True)):
        arms[name] = {**common, 'flags': {**BASE_FLAGS, 'STAGE5_WAKEUP_ENABLED': wakeup}}
    return arms


def server_origin_latency_from_collector(metrics):
    """Subtract same-process start/terminal snapshots; never use client RTT."""
    samples = metrics if isinstance(metrics, list) else []
    def get_sample(phase):
        return next((x for x in samples if isinstance(x, dict) and x.get('phase') == phase
                     and isinstance(x.get('snapshot'), dict)
                     and isinstance(x['snapshot'].get('command_timeline'), dict)), None)
    before, after = get_sample('workload_start'), get_sample('terminal')
    if before is None or after is None:
        return {'histograms': None, 'latency': None, 'availability': 'unavailable'}
    start = before['snapshot']['command_timeline']
    timeline = after['snapshot']['command_timeline']
    def delta_counter(key):
        a, b = start.get(key), timeline.get(key)
        return b - a if type(a) is int and type(b) is int and b >= a else None
    def histogram_delta(key):
        first, last = start.get(key), timeline.get(key)
        def valid(value):
            return (isinstance(value, dict) and type(value.get('count')) is int
                    and type(value.get('overflow')) is int and isinstance(value.get('buckets'), list)
                    and isinstance(value.get('upper_seconds'), list)
                    and len(value['buckets']) == len(value['upper_seconds'])
                    and isinstance(value.get('sum_seconds'), (int, float)))
        if not valid(first) or not valid(last) or first['upper_seconds'] != last['upper_seconds']:
            return None
        buckets = [b-a for a, b in zip(first['buckets'], last['buckets'])
                   if type(a) is int and type(b) is int and b >= a]
        if len(buckets) != len(first['buckets']) or last['count'] < first['count'] or last['overflow'] < first['overflow'] or last['sum_seconds'] < first['sum_seconds']:
            return None
        return {'upper_seconds': last['upper_seconds'], 'buckets': buckets,
                'count': last['count']-first['count'], 'overflow': last['overflow']-first['overflow'],
                'sum_seconds': last['sum_seconds']-first['sum_seconds']}
    dispatch, succeeded = histogram_delta('d_dispatch'), histogram_delta('d_e2e_succeeded')
    missing_origin, missing_dispatch = delta_counter('missing_origin'), delta_counter('missing_dispatch')
    missing_result = delta_counter('missing_result')
    timeline_overflow = delta_counter('overflow')
    pending = timeline.get('pending_origins')
    if (dispatch is None or succeeded is None or missing_origin is None or missing_dispatch is None
            or missing_result is None or timeline_overflow is None or type(pending) is not int):
        return {'histograms': None, 'latency': None, 'availability': 'unavailable'}
    histograms = {'d_dispatch': dispatch, 'd_e2e_succeeded': succeeded,
                  'missing_origin': missing_origin, 'missing_dispatch': missing_dispatch,
                  'missing_result': missing_result, 'overflow': timeline_overflow,
                  'histogram_overflow': dispatch['overflow'] + succeeded['overflow'],
                  'pending_origins': timeline.get('pending_origins'),
                  'snapshot_phase': 'workload_start..terminal'}
    def summary(histogram):
        count = histogram['count']
        return {'count': count, 'sum_seconds': histogram['sum_seconds'],
                'mean_seconds': histogram['sum_seconds'] / count if count else None,
                'overflow': histogram['overflow']}
    return {'histograms': histograms,
            'latency': {'dispatch': summary(dispatch), 'e2e_succeeded': summary(succeeded)},
            'availability': 'available' if dispatch['count'] or succeeded['count'] else 'no_observations'}


def e3_pilot_manifests(*, common, background_targets, background_gateway_identity_files,
                       telemetry_identity_files, control_targets, initial_versions,
                       control_identity_file, tls, idempotency_keys=None, workload='mixed',
                       command_count=4, schedule='smoke', background_profile=None, seed=89,
                       profile='pilot', official_opt_in=False):
    """Compose two workload manifests and one HTTPS-only four-command manifest."""
    if (len(background_targets) != 2 or
            len({x.get('gateway_id') for x in background_targets}) != 2):
        raise ValueError('e3_background_requires_two_gateways')
    if set(background_gateway_identity_files) != {x['gateway_id'] for x in background_targets}:
        raise ValueError('e3_background_credentials_mismatch')
    if len(control_targets) != 2 or len({(x.get('gateway_id'), x.get('sensor_id')) for x in control_targets}) != 2:
        raise ValueError('e3_requires_two_control_sensors')
    bg = {(x['gateway_id'], x['sensor_id']) for x in background_targets}
    if any((x['gateway_id'], x['sensor_id']) in bg for x in control_targets):
        raise ValueError('e3_control_target_overlaps_background')
    if set(initial_versions) != {x['sensor_id'] for x in control_targets}:
        raise ValueError('e3_initial_versions_mismatch')
    if type(command_count) is not int or not 1 <= command_count <= 30:
        raise ValueError('e3_command_count_invalid')
    if schedule not in ('smoke', 'steady') or (schedule == 'smoke' and command_count != 4):
        raise ValueError('e3_schedule_invalid')
    keys = list(idempotency_keys) if idempotency_keys is not None else [str(uuid.uuid4()) for _ in range(command_count)]
    if len(keys) != command_count or len(set(keys)) != command_count or any(not re.fullmatch(
            r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', k) for k in keys):
        raise ValueError('e3_idempotency_keys_invalid')
    if workload not in ('mixed', 'low_load'):
        raise ValueError('e3_workload_rejected')
    if profile not in ('pilot', 'official') or (profile == 'official' and not official_opt_in):
        raise ValueError('e3_profile_opt_in_rejected')
    if profile == 'official' and (command_count not in (3, 30) or schedule != 'steady'
            or command_count == 3 and workload != 'low_load'):
        raise ValueError('e3_official_requires_30_or_low_load_3_steady_commands')
    messages = PILOT_MESSAGES if workload == 'mixed' else LOW_LOAD_MESSAGES
    if background_profile is not None:
        required = {'rate_per_second', 'duration_seconds', 'burst_messages'}
        if (not isinstance(background_profile, dict) or set(background_profile) != required
                or any(type(background_profile[k]) is not int or background_profile[k] < 0 for k in required)
                or not 1 <= background_profile['rate_per_second'] <= 10
                or not 1 <= background_profile['duration_seconds'] <= 3600
                or background_profile['burst_messages'] > 1000):
            raise ValueError('e3_background_profile_invalid')
        messages = {'A': background_profile['rate_per_second'] * background_profile['duration_seconds'],
                    'B': background_profile['rate_per_second'] * background_profile['duration_seconds'] + background_profile['burst_messages']}
    profiles = {}
    for arm in ('A', 'B'):
        target = dict(background_targets[0 if arm == 'A' else 1])
        profiles[arm] = {**common, 'schema_version': 1, 'experiment': 'E2', 'arm': arm,
            'profile': 'pilot', 'roles': ['telemetry'], 'seed': seed,
            'samples_per_message': 100, 'targets': [target], 'subscriptions': [],
            'topology': {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            'traffic': {'rate_per_second': background_profile['rate_per_second'] if background_profile else 1, 'ramp_seconds': 0,
                        'burst_messages': (background_profile['burst_messages'] if arm == 'B' else 0) if background_profile else (0 if arm == 'A' or workload == 'low_load' else 12)},
            'phases': {'warmup_seconds': 0, 'measure_seconds': background_profile['duration_seconds'] if background_profile else (8 if arm == 'A' else 4),
                       'drain_seconds': 12, 'cooldown_seconds': 0},
            'limits': {'retries': 16, 'workers': 2, 'connections': 1,
                       'pending_messages': 64, 'pending_bytes': 1 << 20,
                       'disk_growth_bytes': 1 << 24, 'wall_seconds': 30},
            'gateway_identity_files': {target['gateway_id']: background_gateway_identity_files[target['gateway_id']]},
            'identity_files': {'telemetry': telemetry_identity_files[arm]},
            'tls': {'https_endpoint': tls['https_endpoint'], 'https_ca_file': tls['https_ca_file'],
                    'mqtt_tls_endpoint': tls['mqtt_tls_endpoint'], 'mqtt_tls_ca_file': tls['mqtt_tls_ca_file'],
                    **({'mqtt_tls_server_name': tls['mqtt_tls_server_name']} if tls.get('mqtt_tls_server_name') else {})},
            'durability_mode': 'synthetic-load', 'retry_interval_ms': 500}
    control_commands = []
    current = {sid: int(v) for sid, v in initial_versions.items()}
    frequencies = [FREQUENCIES[i % len(FREQUENCIES)] for i in range(command_count)]
    interval = 1000
    offsets = list(OFFSETS_MS) if schedule == 'smoke' else [1000 + interval * i for i in range(command_count)]
    for i, (frequency, offset, key) in enumerate(zip(frequencies, offsets, keys)):
        target = control_targets[i % 2]
        sid = target['sensor_id']
        control_commands.append({'gateway_id': target['gateway_id'], 'sensor_id': sid,
            'frequency_hz': float(Decimal(frequency)), 'expected_desired_version': str(current[sid]),
            'idempotency_key': key, 'offset_ms': offset, 'actor_label': 'e3-operator'})
        current[sid] += 1
    endpoint = tls.get('https_endpoint', '')
    if not endpoint.startswith('https://') or not tls.get('https_ca_file'):
        raise ValueError('e3_https_tls_required')
    control = {'schema_version': 1, 'experiment': 'E3', 'arm': 'control',
        'mode': 'local', 'run_id': common.get('run_id', 'e3-pilot-control'),
        'profile': profile, 'roles': ['control'], 'seed': seed,
        'topology': {'gateways': 1, 'sensors_per_gateway': 2, 'users': 0, 'viewers': 0},
        'targets': [dict(x) for x in control_targets],
        'commands': control_commands, 'tls': {'https_endpoint': endpoint,
        'https_ca_file': tls['https_ca_file']}, 'identity_files': {'control': control_identity_file},
        'traffic': {'rate_per_second': 0, 'ramp_seconds': 0, 'burst_messages': 0},
        'phases': {'warmup_seconds': 0, 'measure_seconds': max(8, offsets[-1] // 1000 + 2), 'drain_seconds': 30,
                   'cooldown_seconds': 0},
        'limits': {'retries': 0, 'workers': 1, 'connections': 1,
                   'pending_messages': 4, 'pending_bytes': 65536,
                   'disk_growth_bytes': 8 << 20, 'wall_seconds': max(8, offsets[-1] // 1000 + 2) + 30 + 3}}
    return {'workload': workload, 'command_count': command_count, 'schedule': schedule,
            'background_messages': messages,
            'background_profile': background_profile, 'input_difference': ('B offered burst 0 instead of 12' if workload == 'low_load' else None),
            'background': profiles, 'control': control, 'idempotency_keys': keys,
            'expected_versions_after': {k: str(v) for k, v in current.items()}}


def build_e3_execution_plan(*, output_dir, manifests, gateway_env_files,
                            shutdown_timeouts_seconds):
    """Write a private runner spec with two telemetry jobs, control, and two executors."""
    if len(gateway_env_files) != 2 or len(set(gateway_env_files)) != 2:
        raise ValueError('e3_requires_two_gateway_env_files')
    envs = []
    paths = []
    for filename in gateway_env_files:
        path = Path(filename)
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077 or path.stat().st_size > 65536:
            raise ValueError('e3_gateway_env_file_not_private')
        try: env = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, UnicodeError, json.JSONDecodeError): raise ValueError('e3_gateway_env_invalid') from None
        if not isinstance(env, dict) or not env or set(env) - GATEWAY_ENV_KEYS:
            raise ValueError('e3_gateway_env_invalid')
        required = ('GATEWAY_ID', 'SENSOR_ID', 'GATEWAY_CONTROL_ENABLED', 'GATEWAY_MQTT_BROKER_URL',
                    'GATEWAY_MQTT_CLIENT_ID', 'GATEWAY_MQTT_USERNAME', 'GATEWAY_MQTT_CA_FILE', 'GATEWAY_OUTBOX_PATH')
        if any(not isinstance(env.get(k), str) or not env[k] or '\x00' in env[k] for k in required) or env['GATEWAY_CONTROL_ENABLED'] != 'true':
            raise ValueError('e3_gateway_env_invalid')
        envs.append(env)
        paths.append(path.resolve())
    client_ids = [x.get('GATEWAY_MQTT_CLIENT_ID') for x in envs]
    outboxes = [x.get('GATEWAY_OUTBOX_PATH') for x in envs]
    gateway_ids = [x.get('GATEWAY_ID') for x in envs]
    sensors = [x.get('SENSOR_ID') for x in envs]
    canonical_outboxes = [str(Path(x).resolve()) if isinstance(x, str) else '' for x in outboxes]
    targets = {(x.get('gateway_id'), x.get('sensor_id')) for x in manifests['control'].get('targets', [])}
    if (any(not isinstance(x, str) or not x for x in client_ids + outboxes + gateway_ids + sensors)
            or len(set(client_ids)) != 2 or len(set(canonical_outboxes)) != 2
            or len(set(zip(gateway_ids, sensors))) != 2
            or set(zip(gateway_ids, sensors)) != targets):
        raise ValueError('e3_gateway_client_or_outbox_collision')
    if len(shutdown_timeouts_seconds) != 2 or any(type(x) not in (int, float) or x <= 0 for x in shutdown_timeouts_seconds):
        raise ValueError('e3_gateway_shutdown_timeout_invalid')
    stop = max(30, *(int(x) for x in shutdown_timeouts_seconds))
    root = Path(output_dir); root.mkdir(mode=0o700, parents=True, exist_ok=True); root.chmod(0o700)
    files = {}
    for name, data in manifests.items():
        path = root / f'{name}.manifest.json'
        with path.open('x', encoding='utf-8') as f: json.dump(data, f, sort_keys=True, separators=(',', ':'))
        path.chmod(0o600); files[name] = path
    try:
        from scripts.paper_p8_resources import disk_guard_config
    except ModuleNotFoundError:
        from paper_p8_resources import disk_guard_config
    spec = {'paperload': [
        {'manifest': str(files['A']), 'output_dir': str(root / 'A-output'), 'role': 'telemetry', 'ready_timeout_seconds': 90},
        {'manifest': str(files['B']), 'output_dir': str(root / 'B-output'), 'role': 'telemetry', 'ready_timeout_seconds': 90},
        {'manifest': str(files['control']), 'output_dir': str(root / 'control-output'), 'role': 'control', 'ready_timeout_seconds': 90}],
        'gateways': [{'env_file': str(p), 'role': 'executor', 'ready_timeout_seconds': 30} for p in gateway_env_files],
        'stop_timeout_seconds': stop,
        'disk_guard': disk_guard_config(list(manifests.values()))}
    if len(spec['gateways']) > 2 or len({x['output_dir'] for x in spec['paperload']}) != 3:
        raise ValueError('e3_child_collision')
    path = root / 'e3-run-spec.json'
    with path.open('x', encoding='utf-8') as f: json.dump(spec, f, sort_keys=True, indent=2)
    path.chmod(0o600)
    return {'run_spec': str(path), 'roles': ['A:telemetry', 'B:telemetry', 'control', 'executor:A', 'executor:B'],
            'execution': 'NOT RUN'}


def validate_e3_result(*, control_workload, telemetry_a, telemetry_b, phase_events,
                       background_database_delta, command_database_delta,
                       control_receipt_verification, runner_stopped, declared_idempotency_keys,
                        raw_checks=None, workload='mixed', collector_metrics=None,
                        expected_command_count=None, background_expected=None,
                        expected_messages=None):
    """Adapt actual artifacts; freshness/DB evidence is supplied by native fixture."""
    failures = []
    commands = control_workload.get('commands') if isinstance(control_workload, dict) else None
    if not isinstance(commands, list):
        return {'valid': False, 'availability': 'inconclusive', 'failures': ['control_commands_missing']}
    classes = [row.get('classification') for row in commands if isinstance(row, dict)]
    expected_count = expected_command_count if expected_command_count is not None else len(declared_idempotency_keys or [])
    if type(expected_count) is not int or expected_count < 1:
        expected_count = len(commands)
    if len(classes) != len(commands) or len(commands) != expected_count or classes.count('succeeded') != expected_count:
        failures.append('fresh_success_count_mismatch')
    for forbidden in ('noop', 'replay', 'busy', 'rejected', 'timeout'):
        if forbidden in classes: failures.append('control_' + forbidden)
    if not runner_stopped: failures.append('runner_not_stopped')
    if control_workload.get('pending') != 0: failures.append('control_pending_not_zero')
    declared_keys = declared_idempotency_keys
    actual_keys = [row.get('idempotency_key') for row in commands if isinstance(row, dict)]
    if not isinstance(declared_keys, list) or len(declared_keys) != expected_count or sorted(declared_keys) != sorted(actual_keys):
        failures.append('declared_fresh_keys_missing')
    if (not isinstance(control_receipt_verification, dict) or control_receipt_verification.get('verified') is not True
            or set(control_receipt_verification.get('verified_idempotency_keys', [])) != set(actual_keys)):
        failures.append('control_receipts_unverified')
    if workload not in ('mixed', 'low_load'):
        return {'valid': False, 'availability': 'inconclusive', 'failures': ['e3_workload_rejected']}
    expected_messages = expected_messages or (PILOT_MESSAGES if workload == 'mixed' else LOW_LOAD_MESSAGES)
    total_messages = sum(expected_messages.values())
    bg_expected = background_expected or {'ledger': total_messages, 'raw': total_messages * 100, 'temporal': total_messages}
    if background_database_delta != bg_expected: failures.append('background_database_delta_mismatch')
    if not isinstance(command_database_delta, dict) or type(command_database_delta.get('raw_samples')) is not int:
        failures.append('command_raw_counts_unavailable')
    elif command_database_delta['raw_samples'] <= 0:
        failures.append('command_raw_counts_invalid')
    if not all(isinstance(w, dict) and w.get('status') == 'COMPLETE' and isinstance(w.get('telemetry'), dict)
               for w in (telemetry_a, telemetry_b)):
        failures.append('telemetry_child_incomplete')
    for child, expected in ((telemetry_a, expected_messages['A']), (telemetry_b, expected_messages['B'])):
        counters = child.get('telemetry') if isinstance(child, dict) else None
        if (not isinstance(counters, dict)
                or counters.get('generated_unique') != expected
                or counters.get('offered_unique') != expected
                or counters.get('application_acks') != expected
                or counters.get('pending_messages') != 0
                or counters.get('pending_bytes') != 0):
            failures.append('telemetry_accounting_mismatch')
    expected_raw = tuple((expected_messages[arm], expected_messages[arm] * 100) for arm in ('A', 'B'))
    if not isinstance(raw_checks, list) or len(raw_checks) != 2:
        failures.append('raw_verification_missing')
    else:
        for proof, (messages, samples) in zip(raw_checks, expected_raw):
            ranges = proof.get('identity_ranges', []) if isinstance(proof, dict) else []
            if (not isinstance(proof, dict)
                    or not (proof.get('verified_exact') is True or proof.get('verified_sample') is True)
                    or proof.get('raw_identity_value_mismatches') != 0
                    or proof.get('ledger_count') != messages
                    or len(ranges) != messages
                    or sum(int(row.get('sample_count', 0)) for row in ranges) != samples):
                failures.append('raw_verification_mismatch')
    if (not isinstance(control_workload, dict) or control_workload.get('status') != 'COMPLETE'
            or control_workload.get('planned') != expected_count or control_workload.get('sent') != expected_count
            or control_workload.get('skipped') != 0 or control_workload.get('pending') != 0):
        failures.append('control_workload_incomplete')
    phases = {'availability': 'inconclusive', 'reason': 'phase_events_missing'}
    try:
        if not isinstance(phase_events, list): raise ValueError('phase_events_missing')
        def interval(child, begin, end):
            # The local fixture uses *.manifest.json; the portable bundle binds
            # those same roles as A.json/B.json/control.json.
            aliases = {child}
            if child.startswith('paperload-telemetry:'):
                aliases.add(child.replace('.manifest.json', '.json'))
            elif child == 'paperload-control:control.manifest.json':
                aliases.add('paperload-control:control.json')
            rows = [e for e in phase_events if isinstance(e, dict) and e.get('child') in aliases]
            found = {}
            for e in rows:
                ev, stamp = e.get('event'), e.get('observed_at_runner_monotonic_ns')
                if ev not in (begin, end): continue
                if ev in found or type(stamp) is not int or stamp < 0: raise ValueError('phase_event_invalid')
                found[ev] = stamp
            if begin not in found or end not in found: raise ValueError('phase_event_missing')
            if found[begin] >= found[end]: raise ValueError('phase_interval_reversed')
            return found[begin], found[end]
        a0, a1 = interval('paperload-telemetry:A.manifest.json', 'measure_begin', 'measure_end')
        b0 = b1 = None
        if workload == 'mixed':
            b0, b1 = interval('paperload-telemetry:B.manifest.json', 'burst_begin', 'burst_end')
            overlap = max(0, min(a1, b1) - max(a0, b0))
        else:
            overlap = None
        ctrl = [e for e in phase_events if isinstance(e, dict) and e.get('child') in
                ('paperload-control:control.manifest.json', 'paperload-control:control.json')]
        by_ordinal = {}
        lifecycle = {'request_begin', 'admission_observed', 'terminal_observed'}
        for e in ctrl:
            ordinal, stamp = e.get('command_ordinal'), e.get('observed_at_runner_monotonic_ns')
            event = e.get('event')
            if event in ('generation_stopped', 'producers_stopped', 'workers_joined'):
                if ordinal != 0 or type(stamp) is not int or stamp < 0 or event in by_ordinal.setdefault(0, {}):
                    raise ValueError('control_stop_event_invalid')
                by_ordinal[0][event] = stamp
                continue
            if event not in lifecycle:
                continue
            if type(ordinal) is not int or not 1 <= ordinal <= expected_count or type(stamp) is not int or stamp < 0:
                raise ValueError('control_event_invalid')
            timeline = by_ordinal.setdefault(ordinal, {})
            if event in timeline: raise ValueError('control_event_duplicate')
            timeline[event] = stamp
        stops = by_ordinal.get(0, {})
        if set(stops) != {'generation_stopped', 'producers_stopped', 'workers_joined'} or not (
                stops['generation_stopped'] <= stops['producers_stopped'] <= stops['workers_joined']):
            raise ValueError('control_stop_order_invalid')
        if set(by_ordinal) != {0, *range(1, expected_count + 1)} or any(not {'request_begin', 'admission_observed', 'terminal_observed'} <= set(by_ordinal[n]) for n in range(1, expected_count + 1)):
            raise ValueError('control_lifecycle_missing')
        if workload == 'mixed' and overlap <= 0: failures.append('command_overlap_not_observed')
        active = []
        previous_terminal_by_sensor = {}
        for ordinal, timeline in sorted(by_ordinal.items()):
            if ordinal == 0:
                continue
            begin, admission, terminal = (timeline[k] for k in ('request_begin', 'admission_observed', 'terminal_observed'))
            if not begin <= admission <= terminal or begin == terminal:
                raise ValueError('control_lifecycle_reversed')
            command = commands[ordinal - 1]
            sensor = command.get('sensor_id') if isinstance(command, dict) else None
            if isinstance(sensor, str) and sensor:
                previous_terminal = previous_terminal_by_sensor.get(sensor)
                if previous_terminal is not None and begin < previous_terminal:
                    raise ValueError('same_sensor_command_before_previous_terminal')
                previous_terminal_by_sensor[sensor] = terminal
            active.append((ordinal, begin, terminal))
        if max(y for _, _, y in active) > stops['generation_stopped']:
            raise ValueError('control_stop_before_commands_complete')
        if workload == 'mixed' and not any(max(x, b0) < min(y, b1) for _, x, y in active): failures.append('no_command_burst_overlap')
        if workload == 'low_load' and not any(max(x, a0) < min(y, a1) for _, x, y in active): failures.append('no_command_steady_overlap')
        phases = {'availability': 'available', 'workload': workload,
                  'a_steady_runner_interval_ns': [a0, a1],
                  'b_burst_runner_interval_ns': [b0, b1] if workload == 'mixed' else 'NOT APPLICABLE',
                  'observed_overlap_ns': overlap,
                  'burst_overlap': 'NOT APPLICABLE' if workload == 'low_load' else 'observed',
                  'pipe_lag': 'unknown; runner receipt timestamps qualify overlap',
                  'command_active_intervals': active}
    except (ValueError, TypeError) as exc:
        failures.append('phase_overlap_inconclusive'); phases = {'availability': 'inconclusive', 'reason': str(exc)}
    latency = server_origin_latency_from_collector(collector_metrics)
    timeline = latency.get('histograms')
    if collector_metrics is not None and not isinstance(timeline, dict):
        failures.append('server_timeline_window_unavailable')
    elif isinstance(timeline, dict) and (timeline['d_dispatch']['count'] != expected_count
          or timeline['d_e2e_succeeded']['count'] != expected_count
          or timeline['pending_origins'] != 0
          or timeline['missing_origin'] != 0 or timeline['missing_dispatch'] != 0
          or timeline['overflow'] != 0 or timeline['histogram_overflow'] != 0):
        failures.append('server_timeline_count_reconciliation_failed')
    return {'valid': not failures, 'availability': 'available' if phases['availability'] == 'available' else 'inconclusive',
            'failures': failures, 'phase_overlap': phases,
            'command_raw_samples_actual': command_database_delta.get('raw_samples') if isinstance(command_database_delta, dict) else None,
            'server_dispatch_e2e_histograms': latency['histograms'],
            'server_origin_latency': latency['latency'],
            'server_origin_latency_availability': latency['availability']}
