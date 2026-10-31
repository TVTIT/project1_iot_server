#!/usr/bin/env python3
"""Bounded P8 workload preflight and supervised child runner.

Preflight validates the existing paperload manifest without network or writes.
Run accepts one legacy manifest or a small run-spec containing role manifests
and already-provisioned durable Gateway simulator environment files.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time


MAX_CHILD_TIMEOUT_SECONDS = 30
MAX_OWNED_CHILDREN = 16
SAFE_PROCESS_ENV = ('PATH', 'LANG', 'LC_ALL', 'TZ', 'TMPDIR')
GATEWAY_ENV_KEYS = {
    'GATEWAY_CONTROL_ENABLED', 'GATEWAY_ID', 'SENSOR_ID', 'GATEWAY_MQTT_BROKER_URL',
    'GATEWAY_MQTT_CLIENT_ID', 'GATEWAY_MQTT_USERNAME', 'GATEWAY_MQTT_PASSWORD',
    'GATEWAY_MQTT_PASSWORD_FILE', 'GATEWAY_MQTT_CA_FILE', 'GATEWAY_MQTT_SERVER_NAME',
    'GATEWAY_OUTBOX_PATH', 'GATEWAY_SAMPLE_RATE_HZ', 'GATEWAY_SAMPLES_PER_BATCH',
    'GATEWAY_ACK_TIMEOUT', 'GATEWAY_RETRY_MIN_DELAY', 'GATEWAY_RETRY_MAX_DELAY',
    'GATEWAY_MAX_INFLIGHT', 'GATEWAY_OUTBOX_MAX_ROWS', 'GATEWAY_OUTBOX_MAX_BYTES',
    'GATEWAY_CONNECT_TIMEOUT', 'GATEWAY_SUBSCRIBE_TIMEOUT', 'GATEWAY_PUBLISH_TIMEOUT',
    'GATEWAY_SQLITE_TIMEOUT', 'GATEWAY_SHUTDOWN_TIMEOUT', 'GATEWAY_GENERATION_INTERVAL',
}
READY_MARKER = 'gateway simulator running '


class PaperloadInputError(ValueError):
    pass


class PaperloadTimeout(RuntimeError):
    pass


class PaperloadLaunchError(RuntimeError):
    pass


class PaperloadExitError(RuntimeError):
    pass


def _safe_path(value, *, must_exist=False):
    if isinstance(value, Path):
        value = str(value)
    if not isinstance(value, str) or not value or '\x00' in value or value.startswith('-'):
        raise PaperloadInputError('invalid file path argument')
    path = Path(value)
    if must_exist and not path.is_file():
        raise PaperloadInputError('input file does not exist')
    return str(path.resolve())


def _clean_env(extra=None):
    env = {key: os.environ[key] for key in SAFE_PROCESS_ENV if key in os.environ}
    env['PYTHONUNBUFFERED'] = '1'
    if extra:
        env.update(extra)
    return env


def _private_json(path):
    path = Path(_safe_path(path, must_exist=True))
    if (path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077
            or path.stat().st_size > 65536):
        raise PaperloadInputError('credential file must be a private regular file')
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise PaperloadInputError('credential file is invalid') from None


def _gateway_environment(path):
    raw = _private_json(path)
    if not isinstance(raw, dict) or not raw or set(raw) - GATEWAY_ENV_KEYS:
        raise PaperloadInputError('Gateway environment file contains unsupported keys')
    env = {}
    for key, value in raw.items():
        if not isinstance(value, str) or not value or '\x00' in value:
            raise PaperloadInputError('Gateway environment value is invalid')
        env[key] = value
    if env.get('GATEWAY_CONTROL_ENABLED') != 'true':
        raise PaperloadInputError('durable Gateway child must enable control')
    required = ('GATEWAY_ID', 'SENSOR_ID', 'GATEWAY_MQTT_BROKER_URL',
                'GATEWAY_MQTT_CLIENT_ID', 'GATEWAY_MQTT_USERNAME', 'GATEWAY_MQTT_CA_FILE',
                'GATEWAY_OUTBOX_PATH')
    if any(not env.get(key) for key in required):
        raise PaperloadInputError('Gateway environment is missing required configuration')
    password_file = env.get('GATEWAY_MQTT_PASSWORD_FILE')
    if password_file:
        if env.get('GATEWAY_MQTT_PASSWORD'):
            raise PaperloadInputError('Gateway environment has conflicting password sources')
        source = Path(password_file).absolute()
        try:
            for parent in (source, *source.parents):
                if parent.is_symlink():
                    raise PaperloadInputError('Gateway password file must be private regular file')
            st = source.lstat()
            if (not source.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o077
                    or st.st_size <= 0 or st.st_size > 4096):
                raise PaperloadInputError('Gateway password file must be private regular file')
            password = source.read_text(encoding='utf-8')
        except (OSError, UnicodeError):
            raise PaperloadInputError('Gateway password file must be private regular file') from None
        if not password or '\x00' in password:
            raise PaperloadInputError('Gateway password file content is invalid')
        # Keep the value in memory for the child only; never serialize it.
        env['GATEWAY_MQTT_PASSWORD'] = password
    if env['GATEWAY_MQTT_CLIENT_ID'] == env['GATEWAY_ID']:
        raise PaperloadInputError('Gateway MQTT client ID must be distinct from Gateway identity')
    return env


def _duration_seconds(value):
    match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)(ms|s|m|h)', value)
    if not match:
        raise PaperloadInputError('Gateway duration setting is invalid')
    amount = float(match.group(1))
    return amount * {'ms': .001, 's': 1, 'm': 60, 'h': 3600}[match.group(2)]


def invoke_paperload(binary, manifest, output_dir, *, official=False, timeout=20, mode='preflight', env=None):
    """Invoke supplied binary with fixed argv; never forward arbitrary CLI flags."""
    if not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > MAX_CHILD_TIMEOUT_SECONDS:
        raise PaperloadInputError('timeout must be between 0 and 30 seconds')
    executable = _safe_path(binary, must_exist=True)
    manifest_path = _safe_path(manifest, must_exist=True)
    out_path = _safe_path(output_dir)
    if mode not in ('preflight', 'run'):
        raise PaperloadInputError('mode must be preflight or run')
    argv = [executable, '--manifest', manifest_path, '--output-dir', out_path, '--' + mode]
    if official:
        argv.append('--official-opt-in')
    try:
        result = subprocess.run(argv, shell=False, check=False, capture_output=True,
                                text=True, timeout=timeout, env=_clean_env(env))
    except subprocess.TimeoutExpired:
        raise PaperloadTimeout(f'paperload {mode} timed out') from None
    except OSError:
        raise PaperloadLaunchError('paperload binary could not be started') from None
    if result.returncode:
        raise PaperloadExitError(f'paperload exited with code {result.returncode}')
    return result


def _read_spec(path, manifest, output_dir):
    if path:
        spec_path = Path(_safe_path(path, must_exist=True))
        try:
            if spec_path.stat().st_size > 1 << 20:
                raise PaperloadInputError('run spec exceeds size limit')
            spec = json.loads(spec_path.read_text(encoding='utf-8'))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise PaperloadInputError('run spec is invalid JSON') from None
        if not isinstance(spec, dict) or set(spec) - {'paperload', 'gateways', 'stop_timeout_seconds'}:
            raise PaperloadInputError('run spec has unsupported fields')
        def resolve(item):
            if not isinstance(item, str):
                raise PaperloadInputError('run spec path is invalid')
            candidate = Path(item)
            return str(candidate if candidate.is_absolute() else spec_path.parent / candidate)
        jobs = spec.get('paperload', [])
        gateways = spec.get('gateways', [])
        if not isinstance(jobs, list) or not isinstance(gateways, list):
            raise PaperloadInputError('run spec child lists are invalid')
        jobs = [{'manifest': resolve(x['manifest']), 'output_dir': resolve(x['output_dir']),
                 'role': x.get('role', 'auto'), 'ready_timeout_seconds': x.get('ready_timeout_seconds', 30)}
                for x in jobs if isinstance(x, dict) and set(x) <= {'manifest', 'output_dir', 'role', 'ready_timeout_seconds'}
                and {'manifest', 'output_dir'} <= set(x)]
        if len(jobs) != len(spec.get('paperload', [])):
            raise PaperloadInputError('run spec paperload entry is invalid')
        for job in jobs:
            if job['role'] not in ('auto', 'viewer', 'telemetry', 'control') or not isinstance(job['ready_timeout_seconds'], (int,float)) or not 0 < job['ready_timeout_seconds'] <= 120:
                raise PaperloadInputError('run spec paperload role/readiness is invalid')
        gateway_items = []
        for item in gateways:
            if not isinstance(item, dict) or set(item) - {'env_file', 'role', 'ready_timeout_seconds'} or 'env_file' not in item:
                raise PaperloadInputError('run spec Gateway entry is invalid')
            role = item.get('role', 'executor')
            if role not in ('executor', 'durable-telemetry'):
                raise PaperloadInputError('run spec Gateway role is invalid')
            gateway_items.append({'env_file': resolve(item['env_file']), 'role': role,
                                  'ready_timeout_seconds': item.get('ready_timeout_seconds', 30)})
        stop_timeout = spec.get('stop_timeout_seconds', 30)
    else:
        jobs = [{'manifest': _safe_path(manifest, must_exist=True), 'output_dir': _safe_path(output_dir), 'role':'telemetry', 'ready_timeout_seconds':30}]
        gateway_items, stop_timeout = [], 30
    count = len(jobs) + len(gateway_items)
    if count < 1 or count > MAX_OWNED_CHILDREN or len(gateway_items) > 16:
        raise PaperloadInputError('owned child count must be between 1 and 16')
    if not jobs:
        raise PaperloadInputError('run spec must include at least one paperload producer')
    profiles = []
    for job in jobs:
        try:
            if Path(job['manifest']).stat().st_size > 1 << 20:
                raise PaperloadInputError('manifest exceeds size limit')
            public_manifest = json.loads(Path(job['manifest']).read_text(encoding='utf-8'))
            profiles.append(public_manifest.get('profile') if isinstance(public_manifest, dict) else None)
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise PaperloadInputError('manifest is not readable JSON') from None
    if profiles and all(profile == 'pilot' for profile in profiles) and len(gateway_items) > 2:
        raise PaperloadInputError('pilot mode is capped at two durable Gateway processes')
    if not isinstance(stop_timeout, (int, float)) or stop_timeout <= 0 or stop_timeout > 60:
        raise PaperloadInputError('stop timeout must be between 0 and 60 seconds')
    for child in gateway_items:
        value = child['ready_timeout_seconds']
        if not isinstance(value, (int, float)) or value <= 0 or value > 60:
            raise PaperloadInputError('Gateway readiness timeout must be between 0 and 60 seconds')
    return jobs, gateway_items, float(stop_timeout), path


def _hash_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for chunk in iter(lambda: source.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _workload_complete(output_dir):
    path = Path(output_dir) / 'workload.json'
    if not path.is_file():
        return False
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return isinstance(data, dict) and data.get('status') == 'COMPLETE'


def _viewer_ready(output_dir, expected_viewers):
    path = Path(output_dir) / 'ready.json'
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077 or path.stat().st_size > 4096:
            return False
        data = json.loads(path.read_text(encoding='utf-8'))
        return (isinstance(data, dict) and data.get('status') == 'ready'
                and type(data.get('connected')) is int and data['connected'] == expected_viewers
                and type(data.get('subscribed')) is int and data['subscribed'] == expected_viewers
                and type(data.get('principals')) is int and data['principals'] >= 1
                and data.get('mode') in ('healthy', 'slow', 'reconnect')
                and isinstance(data.get('monotonic_origin_qualification'), str)
                and 'not comparable' in data['monotonic_origin_qualification'])
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False


def _wait_viewer_ready(proc, output_dir, expected_viewers, timeout, *, sleep=time.sleep):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _viewer_ready(output_dir, expected_viewers):
            return True
        if proc.poll() is not None:
            raise PaperloadExitError('viewer child exited before readiness')
        sleep(0.025)
    raise PaperloadTimeout('viewer readiness timed out or marker malformed')


def _ordered_role_jobs(jobs, roles):
    """Fixed E1 startup order, not a general dependency scheduler."""
    return sorted(jobs, key=lambda job: {'viewer':0,'telemetry':1,'control':2}[roles[id(job)]])


PHASE_PREFIX = 'P8_PHASE '
PHASE_NAMES = {'warmup_begin','warmup_end','measure_begin','measure_end','burst_begin',
               'burst_end','generation_stopped','drain_begin','drain_end','workers_joined'}
PHASE_COUNTERS = {'generated_unique','offered_unique','publish_attempts','application_acks',
                  'pending_messages','pending_bytes'}
CONTROL_PHASE_NAMES = {'request_begin','admission_observed','terminal_observed',
                       'generation_stopped','producers_stopped','workers_joined'}


def _pump_lines(pipe, lines, done, child_label=None, phase_events=None, phase_errors=None):
    observed_count = 0
    control_role = bool(child_label and child_label.startswith('paperload-control:'))
    error_label = 'control:' if control_role else ''
    try:
        while True:
            line = pipe.readline(4097)
            if not line:
                break
            if len(line.encode('utf-8', errors='replace')) > 4096 and not line.endswith('\n'):
                while line and not line.endswith('\n'):
                    line = pipe.readline(4097)
                if phase_errors is not None:
                    phase_errors.append(error_label + 'phase_line_oversize')
                continue
            line = line.rstrip('\r\n')
            if line.startswith(PHASE_PREFIX):
                try:
                    raw = line[len(PHASE_PREFIX):]
                    event = json.loads(raw)
                    if len(raw.encode('utf-8')) > 4000 or not isinstance(event,dict):
                        raise ValueError('invalid')
                    if control_role:
                        if (event.get('event') not in CONTROL_PHASE_NAMES
                                or type(event.get('elapsed_ns')) is not int or event['elapsed_ns'] < 0
                                or type(event.get('command_ordinal', 0)) is not int
                                or event.get('command_ordinal', 0) < 0
                                or event.get('clock_qualification') != 'process-local elapsed from RunControl origin; not server latency'):
                            raise ValueError('invalid')
                    else:
                        counters = event.get('counters')
                        if (event.get('event') not in PHASE_NAMES
                                or type(event.get('elapsed_ns')) is not int or event['elapsed_ns'] < 0
                                or not isinstance(event.get('configured_phases'),dict)
                                or not isinstance(counters,dict)
                                or any(type(counters.get(key)) is not int or counters[key] < 0 for key in PHASE_COUNTERS)):
                            raise ValueError('invalid')
                    observed_count += 1
                    if observed_count > (32 if control_role else 16):
                        raise ValueError('count')
                    if phase_events is not None:
                        observed = {'child':child_label, 'event':event['event'],
                                    'child_elapsed_ns':event['elapsed_ns'],
                                    'observed_at_runner_monotonic_ns':time.monotonic_ns(),
                                    'clock_qualification':'runner observed pipe receipt; observation lag unknown; child elapsed is process-local'}
                        if control_role:
                            observed['command_ordinal'] = event.get('command_ordinal', 0)
                        else:
                            observed['configured_phases'] = event['configured_phases']
                            observed['counters'] = {key:counters[key] for key in sorted(PHASE_COUNTERS)}
                        phase_events.append(observed)
                except (ValueError, TypeError, json.JSONDecodeError, UnicodeError):
                    if phase_errors is not None:
                        phase_errors.append(error_label + 'phase_line_invalid_or_over_limit')
                continue
            # Keep only bounded, redacted diagnostics; never expose child text.
            lines.append(READY_MARKER if READY_MARKER in line else '[child output]')
            if len(lines) > 200:
                del lines[:len(lines) - 200]
    except (OSError, ValueError):
        pass
    finally:
        done.set()


def run_supervised(*, binary, gateway_binary=None, jobs=(), gateways=(), stop_timeout=30,
                   run_artifact, official=False, popen=subprocess.Popen):
    """Run producers, then boundedly drain and join every owned durable child."""
    started = time.time()
    started_monotonic_ns = time.monotonic_ns()
    source = []
    children = []
    primary_error = None
    cleanup_errors = []
    stopped = False
    readiness = []
    outbox_counts = []
    phase_events = []
    phase_errors = []
    gateway_configs = []
    previous_handlers = {}
    signal_state = {'cleanup_started': False, 'received_during_cleanup': []}
    if threading.current_thread() is threading.main_thread():
        def request_stop(signum, frame):
            if signal_state['cleanup_started']:
                signal_state['received_during_cleanup'].append(signum)
                return
            raise KeyboardInterrupt(f'signal {signum}')
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, request_stop)

    def child_poll(item, proc):
        try:
            return proc.poll()
        except OSError as exc:
            cleanup_errors.append(f"{item['name']} poll: {type(exc).__name__}")
            item['status'] = 'unjoined'
            return None

    def start(argv, env, role, name, ready_timeout=None):
        item = {'role': role, 'name': name, 'status': 'starting', 'ready': False,
                'pid': None, 'exit_code': None}
        children.append((item, None, None, None))
        proc = popen(argv, shell=False, env=env, stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT, text=True, bufsize=1)
        lines, done = [], threading.Event()
        thread = threading.Thread(target=_pump_lines, args=(proc.stdout, lines, done, role + ':' + name,
                                    phase_events, phase_errors), daemon=True)
        thread.start()
        children[-1] = (item, proc, thread, (lines, done))
        item['pid'] = proc.pid
        item['status'] = 'running'
        if ready_timeout is not None:
            deadline = time.monotonic() + ready_timeout
            while time.monotonic() < deadline:
                if child_poll(item, proc) is not None:
                    raise PaperloadExitError(f'{role} child exited before readiness')
                if any(READY_MARKER in line for line in lines):
                    item['ready'] = True
                    item['status'] = 'ready'
                    return
                time.sleep(0.02)
            raise PaperloadTimeout(f'{role} readiness timed out')

    try:
        gateway_count = len(gateways)
        if gateway_count and not gateway_binary:
            raise PaperloadInputError('Gateway binary is required for durable children')
        gateway_configs = [_gateway_environment(item['env_file']) for item in gateways]
        for gateway, env in zip(gateways, gateway_configs):
            outbox = Path(env['GATEWAY_OUTBOX_PATH'])
            outbox_counts.append({'path_hash': hashlib.sha256(str(outbox).encode()).hexdigest(),
                                  'rows': None, 'status': 'not_checked'})
            source.append({'gateway_env_hash': _hash_file(gateway['env_file'])})
        executable = _safe_path(binary, must_exist=True)
        job_roles = {}
        for job in jobs:
            try:
                raw = json.loads(Path(job['manifest']).read_text(encoding='utf-8'))
                declared = job.get('role', 'auto')
                roles = raw.get('roles', [])
                role = declared if declared != 'auto' else (roles[0] if isinstance(roles,list) and len(roles)==1 else ('telemetry' if 'role' not in job and not roles else ''))
                if role not in ('viewer','telemetry','control') or (declared != 'auto' and roles and roles != [declared]):
                    raise PaperloadInputError('paperload child role is ambiguous or mismatched')
                job_roles[id(job)] = role
            except (OSError, UnicodeError, json.JSONDecodeError, AttributeError):
                raise PaperloadInputError('paperload manifest role is invalid') from None
        # Viewer readiness gates dependent publishers. Fixed role ordering is
        # intentional; this is not a general dependency-graph scheduler.
        ordered_jobs = _ordered_role_jobs(jobs, job_roles)
        for job in ordered_jobs:
            manifest_path = _safe_path(job['manifest'], must_exist=True)
            argv = [executable, '--manifest', manifest_path, '--output-dir',
                    _safe_path(job['output_dir']), '--preflight']
            if official:
                argv.append('--official-opt-in')
            try:
                checked = subprocess.run(argv, shell=False, env=_clean_env(), capture_output=True,
                                         text=True, timeout=MAX_CHILD_TIMEOUT_SECONDS, check=False)
            except subprocess.TimeoutExpired:
                raise PaperloadTimeout('paperload manifest preflight timed out') from None
            except OSError:
                raise PaperloadLaunchError('paperload preflight could not be started') from None
            if checked.returncode:
                raise PaperloadExitError(f'paperload preflight exited with code {checked.returncode}')
        if any(_duration_seconds(env.get('GATEWAY_SHUTDOWN_TIMEOUT', '10s')) > stop_timeout
               for env in gateway_configs):
            raise PaperloadInputError('run stop budget must cover each Gateway shutdown timeout')
        outbox_paths = [str(Path(env['GATEWAY_OUTBOX_PATH']).resolve()) for env in gateway_configs]
        client_ids = [env['GATEWAY_MQTT_CLIENT_ID'] for env in gateway_configs]
        if len(set(outbox_paths)) != len(outbox_paths):
            raise PaperloadInputError('each Gateway process must own a separate SQLite outbox file')
        if len(set(client_ids)) != len(client_ids):
            raise PaperloadInputError('each Gateway process must use a unique MQTT client ID')
        # A command producer must not begin until each durable executor is subscribed.
        for index, (gateway, env) in enumerate(zip(gateways, gateway_configs)):
            start([_safe_path(gateway_binary, must_exist=True), 'run'], _clean_env(env),
                  'gateway-' + gateway.get('role', 'executor'), f'gateway-{index + 1}',
                  float(gateway.get('ready_timeout_seconds', 30)))
            readiness.append({'child': f'gateway-{index + 1}', 'ready': True})
        for job in ordered_jobs:
            manifest_path = _safe_path(job['manifest'], must_exist=True)
            output_path = _safe_path(job['output_dir'])
            source.append({'manifest_name': Path(manifest_path).name, 'sha256': _hash_file(manifest_path)})
            argv = [_safe_path(binary, must_exist=True), '--manifest', manifest_path,
                    '--output-dir', output_path, '--run']
            if official:
                argv.append('--official-opt-in')
            role = job_roles[id(job)]
            start(argv, _clean_env(), 'paperload-' + role, Path(manifest_path).name)
            if role == 'viewer':
                try:
                    raw = json.loads(Path(manifest_path).read_text(encoding='utf-8'))
                    expected_viewers = raw['topology']['viewers']
                except (OSError, ValueError, KeyError, TypeError):
                    raise PaperloadInputError('viewer expected count unavailable') from None
                proc = children[-1][1]
                _wait_viewer_ready(proc, job['output_dir'], expected_viewers,
                                   float(job.get('ready_timeout_seconds', 30)))
                readiness.append({'child':Path(manifest_path).name,'role':'viewer','ready':True,'viewers':expected_viewers})
        # Existing single-manifest mode and producers are finite and own their run window.
        manifest_wall = []
        for job in jobs:
            try:
                raw = json.loads(Path(job['manifest']).read_text(encoding='utf-8'))
                manifest_wall.append(float(raw['limits']['wall_seconds']))
            except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError):
                raise PaperloadInputError('manifest run wall budget is unavailable') from None
        producer_deadline = time.monotonic() + max([1.0, stop_timeout, *manifest_wall])
        for item, proc, _, _ in children:
            if item['role'].startswith('paperload-'):
                remaining = max(0.01, producer_deadline - time.monotonic())
                try:
                    code = proc.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    raise PaperloadTimeout('paperload producer exceeded bounded run window') from None
                item['exit_code'] = code
                item['status'] = 'complete' if code == 0 else 'failed'
                if code != 0:
                    raise PaperloadExitError(f'paperload producer exited with code {code}')
        for job in jobs:
            if not _workload_complete(job['output_dir']):
                raise PaperloadExitError('paperload workload artifact is missing, malformed, or incomplete')
    except BaseException as exc:
        primary_error = f'{type(exc).__name__}: {exc}'
    finally:
        signal_state['cleanup_started'] = True
        cleanup_deadline = time.monotonic() + stop_timeout

        def remaining_cleanup(maximum):
            return max(0.01, min(maximum, cleanup_deadline - time.monotonic()))

        # Stop all producers before any Gateway executor; request every stop even
        # when one signal/wait operation fails. SQLite files are never removed.
        for item, proc, _, _ in children:
            if proc is None or child_poll(item, proc) is not None:
                continue
            if item['role'].startswith('paperload-'):
                try:
                    proc.terminate()
                    item['status'] = 'stopping'
                except OSError as exc:
                    cleanup_errors.append(f"{item['name']} terminate: {type(exc).__name__}")
        for item, proc, _, _ in children:
            if proc is None or not item['role'].startswith('paperload-') or child_poll(item, proc) is not None:
                continue
            try:
                proc.wait(timeout=remaining_cleanup(5))
            except subprocess.TimeoutExpired:
                cleanup_errors.append(f"{item['name']} drain timed out")
                try:
                    proc.kill()
                    proc.wait(timeout=remaining_cleanup(5))
                except (OSError, subprocess.TimeoutExpired) as exc:
                    cleanup_errors.append(f"{item['name']} final join: {type(exc).__name__}")
        # Existing Gateway SIGTERM path stops generation, drains scheduler and
        # joins ACK/control workers within GATEWAY_SHUTDOWN_TIMEOUT.
        for item, proc, _, _ in children:
            if proc is None or not item['role'].startswith('gateway-') or child_poll(item, proc) is not None:
                continue
            try:
                proc.terminate()
                item['status'] = 'draining'
            except OSError as exc:
                cleanup_errors.append(f"{item['name']} terminate: {type(exc).__name__}")
        for item, proc, thread, pump in children:
            if proc is None:
                continue
            try:
                if child_poll(item, proc) is None:
                    if item['role'].startswith('gateway-'):
                        gateway_index = int(item['name'].rsplit('-', 1)[1]) - 1
                        child_budget = _duration_seconds(gateway_configs[gateway_index].get(
                            'GATEWAY_SHUTDOWN_TIMEOUT', '10s'))
                    else:
                        child_budget = 5
                    proc.wait(timeout=remaining_cleanup(child_budget))
                item['exit_code'] = child_poll(item, proc)
                item['status'] = 'stopped' if item['exit_code'] is not None else 'unjoined'
            except subprocess.TimeoutExpired:
                cleanup_errors.append(f"{item['name']} join timed out")
                item['status'] = 'unjoined'
                try:
                    proc.kill()
                    proc.wait(timeout=remaining_cleanup(5))
                    item['exit_code'] = child_poll(item, proc)
                    item['status'] = 'stopped_after_kill'
                except (OSError, subprocess.TimeoutExpired) as exc:
                    cleanup_errors.append(f"{item['name']} final join: {type(exc).__name__}")
            except OSError as exc:
                cleanup_errors.append(f"{item['name']} join: {type(exc).__name__}")
                item['status'] = 'unjoined'
            try:
                if thread is not None:
                    thread.join(timeout=remaining_cleanup(2))
                    if thread.is_alive():
                        cleanup_errors.append(f"{item['name']} output reader did not join")
                if proc.stdout is not None:
                    proc.stdout.close()
            except OSError as exc:
                cleanup_errors.append(f"{item['name']} output cleanup: {type(exc).__name__}")
        stopped = all(proc is None or (child_poll(item, proc) is not None and (thread is None or not thread.is_alive()))
                      for item, proc, thread, _ in children)
        # Count only after the writer process has exited, using the Gateway's
        # existing read-only CLI against the same persistent SQLite file.
        for idx, gateway in enumerate(gateways):
            try:
                owned = next((proc for item, proc, _, _ in children
                              if item['name'] == f'gateway-{idx + 1}'), None)
                owned_item = next((item for item, proc, _, _ in children
                                   if proc is owned and item['name'] == f'gateway-{idx + 1}'), None)
                if owned is not None and child_poll(owned_item, owned) is None:
                    cleanup_errors.append(f'gateway-{idx + 1} count skipped: writer is not confirmed stopped')
                    continue
                env = gateway_configs[idx] if idx < len(gateway_configs) else _gateway_environment(gateway['env_file'])
                if time.monotonic() >= cleanup_deadline:
                    cleanup_errors.append(f'gateway-{idx + 1} count skipped: cleanup budget exhausted')
                    continue
                result = subprocess.run([_safe_path(gateway_binary, must_exist=True), 'count'],
                                         shell=False, env=_clean_env(env), capture_output=True,
                                         text=True, timeout=remaining_cleanup(10), check=False)
                if result.returncode == 0:
                    import re
                    found = dict(re.findall(r'(pending_rows|pending_bytes|quarantined_rows|quarantined_bytes)=(\d+)', result.stdout))
                    if len(found) == 4:
                        outbox_counts[idx].update({key: int(value) for key, value in found.items()})
                        outbox_counts[idx]['status'] = 'readback'
                    else:
                        cleanup_errors.append(f'gateway-{idx + 1} count output malformed')
                else:
                    cleanup_errors.append(f'gateway-{idx + 1} count exited {result.returncode}')
            except (OSError, subprocess.TimeoutExpired, PaperloadInputError) as exc:
                cleanup_errors.append(f'gateway-{idx + 1} count: {type(exc).__name__}')

    status = 'COMPLETE' if primary_error is None and not cleanup_errors and stopped and all(
        item.get('status') == 'complete' or item.get('status') in ('stopped', 'stopped_after_kill')
        for item, *_ in children) and all(count.get('status') == 'readback' for count in outbox_counts) else 'PARTIAL'
    if status == 'COMPLETE' and any(count.get('pending_rows', 0) for count in outbox_counts):
        status = 'PARTIAL'
    telemetry_children = [item['role'] + ':' + item['name'] for item, *_ in children if item['role'] == 'paperload-telemetry']
    control_children = [item['role'] + ':' + item['name'] for item, *_ in children if item['role'] == 'paperload-control']
    phase_order = ['warmup_begin','warmup_end','measure_begin','measure_end','burst_begin','burst_end',
                   'generation_stopped','drain_begin','drain_end','workers_joined']
    telemetry_phase_errors = any(not error.startswith('control:') for error in phase_errors)
    control_phase_errors = any(error.startswith('control:') for error in phase_errors)
    phase_unavailable = telemetry_phase_errors or any(
        [e['event'] for e in phase_events if e['child'] == child] != phase_order for child in telemetry_children)
    control_lifecycle = ['generation_stopped','producers_stopped','workers_joined']
    def control_child_invalid(child):
        events = [event for event in phase_events if event['child'] == child]
        commands = {}
        for event in events:
            ordinal = event['command_ordinal']
            if not ordinal:
                continue
            commands.setdefault(ordinal, []).append(event['event'])
        lifecycle = [event['event'] for event in events if not event['command_ordinal']]
        return (lifecycle[-3:] != control_lifecycle or not commands or
                any(names[:2] != ['request_begin', 'admission_observed'] or
                    names.count('request_begin') != 1 or names.count('admission_observed') != 1 or
                    names.count('terminal_observed') > 1 or
                    ('terminal_observed' in names and names.index('terminal_observed') < 2)
                    for names in commands.values()))
    control_unavailable = control_phase_errors or any(control_child_invalid(child) for child in control_children)
    phase_unavailable = phase_unavailable or control_unavailable
    if phase_unavailable and status == 'COMPLETE':
        status = 'PARTIAL'
        cleanup_errors.append('required role phase observations unavailable or malformed')
    report = {'status': status, 'implementation_status': 'P8.4 bounded child lifecycle; not native acceptance',
              'started_at_unix': started, 'finished_at_unix': time.time(), 'timing_origin': 'runner_process_wall_clock',
              'started_at_runner_monotonic_ns': started_monotonic_ns,
              'finished_at_runner_monotonic_ns': time.monotonic_ns(),
              'runner_monotonic_qualification': 'one runner-process monotonic origin; not comparable with child clocks or another runner',
              'source_manifest_hashes': source, 'children': [item for item, *_ in children],
              'durable_gateway_process_count': len(gateways),
              'durable_gateway_count': len({env['GATEWAY_ID'] for env in gateway_configs}),
              'durable_sensor_connections': len(gateways),
              'durable_sensor_count': len({(env['GATEWAY_ID'], env['SENSOR_ID']) for env in gateway_configs}),
              'timer_driven_gateway_workload': bool(gateways), 'readiness': readiness,
              'actual_stop_confirmed': stopped, 'pending_outbox_readback': outbox_counts,
              'signals_received_during_cleanup': signal_state['received_during_cleanup'],
              'resource_sampling': {'status': 'not_collected_in_P8.4'},
               'phase_observations': {'events': phase_events[:(16 * len(telemetry_children) + 32 * len(control_children))],
                  'status': 'unavailable' if phase_unavailable else ('observed' if telemetry_children else 'not_applicable'),
                  'clock_qualification':'runner monotonic receipt times are descriptive; child process elapsed clocks are not comparable across children; pipe observation lag unknown',
                  'errors': sorted(set(phase_errors))},
              'primary_error': primary_error, 'cleanup_errors': cleanup_errors,
              'limitations': ['timer-driven durable simulator is not open-loop offered-rate load',
                              'local Compose orchestration is not implemented; no native pilot run']}
    try:
        artifact = Path(run_artifact)
        artifact.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temp = artifact.with_suffix(artifact.suffix + '.tmp')
        if artifact.exists() or temp.exists():
            raise FileExistsError('run artifact path already exists')
        with temp.open('x', encoding='utf-8') as output:
            output.write(json.dumps(report, indent=2) + '\n')
        os.chmod(temp, 0o600)
        temp.replace(artifact)
    except (OSError, TypeError, ValueError) as exc:
        report['cleanup_errors'].append(f'run artifact flush: {type(exc).__name__}')
        report['status'] = 'PARTIAL'
        status = 'PARTIAL'
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return status, report


def preflight(binary, manifest, output_dir, *, official=False, timeout=20):
    return invoke_paperload(binary, manifest, output_dir, official=official, timeout=timeout, mode='preflight')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, help='path to an already-built paperload binary')
    parser.add_argument('--gateway-binary', help='optional already-built Gateway simulator binary')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--preflight', action='store_true', help='validate manifest only; no network or writes')
    mode.add_argument('--run', action='store_true', help='execute bounded child processes')
    parser.add_argument('--manifest', help='single paperload manifest (legacy child interface)')
    parser.add_argument('--output-dir', help='single workload output directory')
    parser.add_argument('--run-spec', help='JSON run spec selecting up to 16 owned processes')
    parser.add_argument('--run-artifact', help='run.json path for --run')
    parser.add_argument('--official-opt-in', action='store_true')
    parser.add_argument('--timeout', type=float, default=20)
    args = parser.parse_args(argv)
    try:
        if args.run_spec and (args.manifest or args.output_dir):
            raise PaperloadInputError('--run-spec cannot be combined with --manifest/--output-dir')
        if not args.run_spec and (not args.manifest or not args.output_dir):
            raise PaperloadInputError('--manifest and --output-dir are required without --run-spec')
        jobs, gateways, stop_timeout, spec_path = _read_spec(args.run_spec, args.manifest, args.output_dir)
        if args.preflight:
            if args.run_spec:
                # Preflight every manifest using its actual public contract; it never reads gateway files.
                for job in jobs:
                    result = preflight(args.binary, job['manifest'], job['output_dir'],
                                       official=args.official_opt_in, timeout=args.timeout)
                    sys.stdout.write(result.stdout)
                return 0
            result = preflight(args.binary, jobs[0]['manifest'], jobs[0]['output_dir'],
                               official=args.official_opt_in, timeout=args.timeout)
            sys.stdout.write(result.stdout)
            return 0
        if not args.run_artifact:
            raise PaperloadInputError('--run-artifact is required for --run')
        status, report = run_supervised(binary=args.binary, gateway_binary=args.gateway_binary,
                                        jobs=jobs, gateways=gateways, stop_timeout=stop_timeout,
                                        run_artifact=args.run_artifact, official=args.official_opt_in)
        print(json.dumps(report, indent=2))
        return 0 if status == 'COMPLETE' else 1
    except PaperloadInputError as exc:
        print(f'input error: {exc}', file=sys.stderr)
        return 2
    except (PaperloadTimeout, PaperloadLaunchError, PaperloadExitError) as exc:
        print(f'run error: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
