"""Private foreground supervisor primitives for an owned native fixture.

This is not a network control plane or an E1 runner. startup_only owns normal
fixture cleanup; persisted exact receipts support only explicit stale recovery.
"""
import fcntl
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import time
from types import SimpleNamespace


SCHEMA_VERSION = 1
MAX_STATE_BYTES = 256 * 1024
MAX_REQUEST_BYTES = 4096
MAX_RECEIPT_BYTES = 128 * 1024


def _root(path):
    path = Path(path).absolute()
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('session_path_symlink_rejected')
    st = path.lstat()
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
        raise ValueError('session_root_rejected')
    return path


class SessionLock:
    def __init__(self, root):
        self.root = _root(root)
        path = self.root / 'supervisor.lock'
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        st = os.fstat(self.fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o600:
            os.close(self.fd)
            raise ValueError('session_lock_rejected')
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.fd)
            raise ValueError('session_active') from None

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def acquire_lock(root):
    return SessionLock(root)


def _atomic_json(path, value, *, no_replace=False):
    path = Path(path)
    temp = path.with_name('.' + path.name + '.' + secrets.token_hex(8) + '.tmp')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, sort_keys=True, separators=(',', ':'), allow_nan=False)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        if no_replace:
            os.link(temp, path, follow_symlinks=False)
            temp.unlink()
        else:
            os.replace(temp, path)
        dfd = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _read_json(path, limit, code, *, max_links=1):
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, 'O_NONBLOCK', 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, 'rb') as stream:
        st = os.fstat(stream.fileno())
        if (not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid()
                or stat.S_IMODE(st.st_mode) != 0o600 or not 1 <= st.st_nlink <= max_links
                or st.st_size > limit):
            raise ValueError(code)
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError(code)
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise ValueError(code) from None


def initialize(root, *, experiment, arm, seed, deadline_seconds, receipts,
               source_metadata=None, image_metadata=None, endpoint_metadata=None,
               project=None, lock=None, session_id=None, artifacts=None,
               preparation_status='LIFECYCLE_READY', cold_replay_enabled=False,
               e4_phase_plan=None, e5_phase_plan=None):
    root = _root(root)
    if (not isinstance(experiment, str) or not re.fullmatch(r'E[1-5]', experiment)
            or not isinstance(arm, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,32}', arm)
            or isinstance(seed, bool) or not isinstance(seed, int) or not -(2**31) <= seed < 2**31
            or isinstance(deadline_seconds, bool) or not isinstance(deadline_seconds, (int, float))
            or not math.isfinite(deadline_seconds) or not 0 <= deadline_seconds <= 86400
            or not isinstance(receipts, list) or len(receipts) > 1024
            or artifacts is not None and (not isinstance(artifacts, list) or len(artifacts) > 64)
            or preparation_status not in ('LIFECYCLE_READY', 'E1_PREPARED', 'E2_PREPARED', 'E3_PREPARED', 'E4_PREPARED', 'E5_PREPARED')
            or session_id is not None and not re.fullmatch(r'[a-f0-9]{32}', session_id)
            or project is not None and not re.fullmatch(r'ncs_[a-f0-9]{12}', project)):
        raise ValueError('session_metadata_rejected')
    if type(cold_replay_enabled) is not bool or cold_replay_enabled and experiment != 'E4':
        raise ValueError('session_metadata_rejected')
    if e4_phase_plan is None and experiment == 'E4':
        e4_phase_plan = ['unique', 'replay'] + (['cold-replay'] if cold_replay_enabled else [])
    if e4_phase_plan is not None:
        allowed_plans = (['unique','replay'], ['unique','replay','cold-replay'],
            ['unique','replay','durable-fault','durable-reopen'],
            ['unique','replay','cold-replay','durable-fault','durable-reopen'],
            ['unique','replay','conflicts'],
            ['unique','replay','cold-replay','conflicts'],
            ['unique','replay','durable-fault','durable-reopen','conflicts'],
            ['unique','replay','cold-replay','durable-fault','durable-reopen','conflicts'])
        if (experiment != 'E4' or type(e4_phase_plan) is not list or e4_phase_plan not in allowed_plans
                or ('cold-replay' in e4_phase_plan) != cold_replay_enabled):
            raise ValueError('session_e4_phase_plan_rejected')
    if e5_phase_plan is None and experiment == 'E5':
        e5_phase_plan = ['initial', 'warm_replay', 'control']
    if e5_phase_plan is not None:
        if (experiment != 'E5' or type(e5_phase_plan) is not list
                or e5_phase_plan != ['initial', 'warm_replay', 'control']):
            raise ValueError('session_e5_phase_plan_rejected')
    manager = acquire_lock(root) if lock is None else None
    if lock is not None and (not isinstance(lock, SessionLock) or lock.root != root or lock.fd is None):
        raise ValueError('session_lock_mismatch')
    try:
        state_path = root / 'session.json'
        if state_path.exists():
            raise ValueError('session_already_initialized')
        now = time.time()
        state = {'schema_version': SCHEMA_VERSION, 'session_id': session_id or secrets.token_hex(16),
            'experiment': experiment, 'arm': arm, 'seed': seed, 'project': project,
            'source_metadata': source_metadata or {}, 'image_metadata': image_metadata or {},
            'endpoint_metadata': endpoint_metadata or {},
            'preparation_status': preparation_status,
            'phase': 'PREPARING', 'created_at': now, 'deadline_at': now + float(deadline_seconds),
            'receipts': receipts, 'artifacts': artifacts or [], 'workload_outcome': 'NOT_RUN',
            'cold_replay_enabled': cold_replay_enabled,
            'e4_phase_plan': e4_phase_plan if experiment == 'E4' else None,
            'e5_phase_plan': e5_phase_plan if experiment == 'E5' else None,
            'phase_id': (e4_phase_plan[0] if experiment == 'E4' and e4_phase_plan else
                         e5_phase_plan[0] if experiment == 'E5' and e5_phase_plan else None),
            'cleanup_outcome': 'PENDING'}
        _atomic_json(state_path, state, no_replace=True)
        state['phase'] = 'READY'
        state['deadline_monotonic'] = time.monotonic() + float(deadline_seconds)
        _atomic_json(state_path, state)
        return state
    finally:
        if manager is not None:
            manager.close()


def read_state(root):
    root = _root(root)
    state = _read_json(root / 'session.json', MAX_STATE_BYTES, 'session_state_rejected')
    if (not isinstance(state, dict) or state.get('schema_version') != SCHEMA_VERSION
            or not isinstance(state.get('session_id'), str) or len(state['session_id']) != 32):
        raise ValueError('session_state_rejected')
    return state


def write_request(root, request):
    root = _root(root)
    if not isinstance(request, dict) or len(request) > 16:
        raise ValueError('request_contract_rejected')
    raw = json.dumps(request, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    if len(raw) > MAX_REQUEST_BYTES:
        raise ValueError('request_size_rejected')
    path = root / 'request.json'
    temp = root / ('.request.' + secrets.token_hex(8) + '.tmp')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temp, path, follow_symlinks=False)
        except FileExistsError:
            raise ValueError('request_pending') from None
        dfd = os.open(root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def begin_finalizing(root, outcome):
    state = read_state(root)
    if state.get('phase') not in ('READY', 'MEASURING', 'VERIFYING', 'PREPARING'):
        raise ValueError('session_transition_invalid')
    return _set_finalizing(root, state, outcome)


def _set_finalizing(root, state, outcome):
    state.update(phase='FINALIZING', workload_outcome=outcome,
                 finalizing_at=time.time())
    _atomic_json(Path(root) / 'session.json', state)
    return state


def finalize(root, state=None, *, cleanup_verified, cleanup_error=None):
    root = _root(root)
    state = state or read_state(root)
    if state.get('phase') != 'FINALIZING' or type(cleanup_verified) is not bool:
        raise ValueError('session_finalization_invalid')
    state.update(phase='CLOSED', cleanup_outcome='VERIFIED' if cleanup_verified else 'FAILED',
                 cleanup_error=cleanup_error if isinstance(cleanup_error, str) else None,
                 closed_at=time.time())
    _atomic_json(root / 'session.json', state)
    return state


def refresh_owned_receipts(root, state, receipts):
    """Persist only the exact post-allocation receipt set already written by registry items."""
    root = _root(root)
    if not isinstance(state, dict) or state.get('experiment') != 'E4' or not isinstance(receipts, list):
        raise ValueError('replacement_receipts_rejected')
    files = sorted(path for path in root.glob('resource-*.json')
                   if path.name != 'resource-receipts.json')
    observed = []
    for path in files:
        item = _read_json(path, MAX_RECEIPT_BYTES, 'replacement_receipt_invalid')
        if (not isinstance(item, dict) or item.get('run') != state.get('project')
                or item.get('task') != 'native-compose-allocation'
                or item.get('allocation_attempted') is not True
                or not isinstance(item.get('receipt'), dict)):
            raise ValueError('replacement_receipt_invalid')
        observed.append((item.get('kind'), item.get('name'), json.dumps(item['receipt'], sort_keys=True)))
    supplied = [(row.get('kind'), row.get('name'), json.dumps(row.get('receipt'), sort_keys=True))
                for row in receipts if isinstance(row, dict)]
    if len(supplied) != len(receipts) or len(set(supplied)) != len(supplied) or set(supplied) != set(observed):
        raise ValueError('replacement_receipt_set_mismatch')
    state['receipts'] = receipts
    _atomic_json(root / 'session.json', state)
    return state


def _e4_next_phase_matches(state, requested):
    plan = state.get('e4_phase_plan')
    current = state.get('phase_id')
    if not isinstance(plan, list) or current not in plan:
        return False
    index = plan.index(current)
    return index + 1 < len(plan) and plan[index + 1] == requested


E5_PHASE_PLAN = ['initial', 'warm_replay', 'control']
E5_PHASE_COMMANDS = ('begin', 'finish-phase', 'next-phase')


def e5_phase_transition_error(state, command, phase_id):
    """Return an error code when an E5 phase transition is not allowed.

    Single source of truth shared by the supervisor hold loop and the
    ``paper_p8_server`` CLI. E5 gates only on immutable phase-plan ordering and
    an already validated previous phase; it adds no service or extra state
    machine beyond the existing E4 shape.
    """
    if not isinstance(state, dict) or state.get('experiment') != 'E5':
        return 'e5_transition_wrong_experiment'
    if state.get('e5_phase_plan') != E5_PHASE_PLAN:
        return 'e5_phase_plan_rejected'
    history = state.get('phase_history', [])
    if history is None:
        history = []
    if (not isinstance(history, list) or len(history) > len(E5_PHASE_PLAN)
            or any(item not in E5_PHASE_PLAN for item in history)
            or history != E5_PHASE_PLAN[:len(history)]):
        return 'e5_phase_history_rejected'
    if command not in E5_PHASE_COMMANDS:
        return 'e5_transition_command_rejected'
    if command == 'begin':
        if (state.get('phase') != 'READY'
                or not isinstance(phase_id, str) or phase_id not in E5_PHASE_PLAN
                or state.get('phase_id') != phase_id
                or len(history) >= len(E5_PHASE_PLAN)
                or E5_PHASE_PLAN[len(history)] != phase_id):
            return 'e5_begin_phase_transition_invalid'
        return None
    if command == 'finish-phase':
        if state.get('phase') != 'MEASURING' or phase_id != state.get('phase_id'):
            return 'e5_finish_phase_transition_invalid'
        return None
    imported = state.get('imported_result')
    if (state.get('phase') != 'VERIFYING'
            or state.get('workload_outcome') != 'VALIDATED'
            or not isinstance(imported, dict)
            or imported.get('phase_id') != state.get('phase_id')
            or imported.get('validation_valid') is not True
            or not isinstance(phase_id, str) or phase_id not in E5_PHASE_PLAN
            or len(history) >= len(E5_PHASE_PLAN)
            or E5_PHASE_PLAN[len(history)] != phase_id):
        return 'e5_next_phase_requires_validated_previous_phase'
    return None


def _consume_request(root, state):
    path = root / 'request.json'
    try:
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('request_file_rejected')
        # Request publishers stage a complete, fsynced inode and link it into
        # place atomically. During the short publish window that inode has two
        # links (staging + request); receipts/state remain single-link strict.
        request = _read_json(path, MAX_REQUEST_BYTES, 'request_file_rejected', max_links=2)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        try:
            path.unlink()
        except OSError:
            pass
        state['last_request_error'] = str(exc.args[0]) if exc.args else 'request_file_rejected'
        _atomic_json(root / 'session.json', state)
        return None
    error = None
    if not isinstance(request, dict):
        error = 'request_contract_rejected'
    elif request.get('session_id') != state['session_id']:
        error = 'request_session_mismatch'
    elif request.get('phase') != state['phase']:
        error = 'request_phase_mismatch'
    elif request.get('command') not in ('begin', 'finish-phase', 'next-phase', 'finalize', 'abort'):
        error = 'request_command_rejected'
    elif request.get('command') in ('begin', 'finish-phase', 'next-phase') and state.get('experiment') == 'E5':
        error = e5_phase_transition_error(state, request.get('command'), request.get('phase_id'))
    elif request.get('command') == 'next-phase' and (
            state.get('experiment') != 'E4' or state.get('phase') != 'VERIFYING'
            or request.get('phase_id') not in ('replay', 'cold-replay','durable-fault','durable-reopen','conflicts')
            or state.get('workload_outcome') != 'VALIDATED'
            or not isinstance(state.get('imported_result'), dict)
            or state['imported_result'].get('phase_id') != state.get('phase_id')
            or state['imported_result'].get('validation_valid') is not True
            or request.get('phase_id') == 'replay' and (state.get('phase_id') != 'unique'
                or state.get('phase_history') != ['unique'])
            or request.get('phase_id') == 'cold-replay' and (
                state.get('cold_replay_enabled') is not True or state.get('phase_id') != 'replay'
                or state.get('phase_history') != ['unique', 'replay']
                or state['imported_result'].get('children_stopped_verified') is not True)
            or not _e4_next_phase_matches(state, request.get('phase_id'))):
        error = 'e4_next_phase_requires_validated_previous_phase'
    elif request.get('command') == 'begin' and (state.get('phase') != 'READY'
            or not isinstance(request.get('phase_id'), str)
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', request.get('phase_id', ''))):
        error = 'request_begin_transition_invalid'
    elif request.get('command') == 'finish-phase' and (state.get('phase') != 'MEASURING'
            or request.get('phase_id') != state.get('phase_id')
            or not isinstance(request.get('results'), str)):
        error = 'request_finish_transition_invalid'
    elif request.get('command') == 'begin' and state.get('experiment') == 'E4' and (
            request.get('phase_id') not in state.get('e4_phase_plan', [])
            or state.get('phase_id') != request.get('phase_id')
            or len(state.get('phase_history', [])) >= len(state.get('e4_phase_plan', []))
            or state.get('e4_phase_plan', [])[len(state.get('phase_history', []))] != request.get('phase_id')):
        error = 'e4_begin_phase_transition_invalid'
    path.unlink()
    if error:
        state['last_request_error'] = error
        _atomic_json(root / 'session.json', state)
        return None
    return request


def hold(root, *, poll_seconds=0.2, lock=None, on_begin=None, on_finish=None, on_next=None):
    root = _root(root)
    if isinstance(poll_seconds, bool) or not isinstance(poll_seconds, (int, float)) or not math.isfinite(poll_seconds) or not 0 <= poll_seconds <= 1:
        raise ValueError('poll_interval_rejected')
    owned_lock = lock is None
    lock = acquire_lock(root) if owned_lock else lock
    if not isinstance(lock, SessionLock) or lock.root != root or lock.fd is None:
        if owned_lock:
            lock.close()
        raise ValueError('session_lock_mismatch')
    try:
        state = read_state(root)
        if state['phase'] != 'READY':
            raise ValueError('session_not_ready')
        deadline = state.get('deadline_monotonic')
        if not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
            raise ValueError('session_deadline_invalid')
        interrupted = []
        previous = {}
        def on_signal(signum, _frame):
            interrupted.append(signum)
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, on_signal)
        try:
            while True:
                if interrupted:
                    state['interrupted'] = True
                    return _set_finalizing(root, state, 'INTERRUPTED')
                if time.monotonic() >= deadline:
                    return _set_finalizing(root, state, 'DEADLINE')
                command = _consume_request(root, state)
                if command:
                    operation = command['command']
                    try:
                        if operation == 'begin':
                            baseline = on_begin(command) if on_begin else {}
                            state.update(phase='MEASURING', phase_id=command['phase_id'],
                                         collector_ready=True, baseline=baseline,
                                         begun_at=time.time())
                            _atomic_json(root / 'session.json', state)
                        elif operation == 'finish-phase':
                            result = on_finish(command) if on_finish else None
                            if not isinstance(result, dict):
                                raise ValueError('result_import_handler_missing')
                            if state.get('experiment') == 'E4':
                                expected = command['phase_id']
                                if (expected not in state.get('e4_phase_plan', [])
                                        or result.get('phase_id') != expected
                                        or result.get('workload_outcome') not in ('VALIDATED', 'VALIDATION_FAILED')):
                                    raise ValueError('e4_phase_result_transition_invalid')
                                history = list(state.get('phase_history', []))
                                if result.get('workload_outcome') == 'VALIDATED':
                                    if expected == 'unique' and history == []:
                                        history.append('unique')
                                    elif expected == 'replay' and history == ['unique']:
                                        history.append('replay')
                                    elif (len(history) < len(state.get('e4_phase_plan', []))
                                            and state['e4_phase_plan'][len(history)] == expected):
                                        history.append(expected)
                                    else:
                                        raise ValueError('e4_phase_result_repeated')
                                state['phase_history'] = history
                            if state.get('experiment') == 'E5':
                                expected = command['phase_id']
                                plan = state.get('e5_phase_plan')
                                history = list(state.get('phase_history') or [])
                                if (not isinstance(plan, list) or expected not in plan
                                        or result.get('phase_id') != expected
                                        or result.get('workload_outcome') not in ('VALIDATED', 'VALIDATION_FAILED')):
                                    raise ValueError('e5_phase_result_transition_invalid')
                                if result.get('workload_outcome') == 'VALIDATED':
                                    if len(history) >= len(plan) or plan[len(history)] != expected:
                                        raise ValueError('e5_phase_result_repeated')
                                    history.append(expected)
                                state['phase_history'] = history
                            state.update(phase='VERIFYING', imported_result=result,
                                         workload_outcome=result.get('workload_outcome', 'VALIDATION_FAILED'),
                                         finished_at=time.time())
                            _atomic_json(root / 'session.json', state)
                        elif operation == 'next-phase':
                            next_phase = command['phase_id']
                            transition = on_next(command, state) if next_phase == 'cold-replay' and on_next else None
                            if next_phase == 'cold-replay' and (
                                    not isinstance(transition, dict)
                                    or transition.get('replacement_verified') is not True
                                    or not isinstance(transition.get('receipts'), list)):
                                raise ValueError('e4_cold_restart_not_verified')
                            if transition is not None:
                                state['receipts'] = transition['receipts']
                                state['replacement_proof'] = transition.get('replacement_proof')
                            state.update(phase='READY', phase_id=next_phase, collector_ready=False,
                                         next_phase_authorized=True, next_phase_at=time.time())
                            _atomic_json(root / 'session.json', state)
                        else:
                            outcome = ('ABORTED' if operation == 'abort' else
                                       state.get('workload_outcome', 'NOT_RUN'))
                            if (operation == 'finalize' and state.get('experiment') == 'E3'
                                    and state.get('collector_ready') is True and outcome == 'NOT_RUN'):
                                outcome = 'ABORTED'
                            return _set_finalizing(root, state, outcome)
                    except Exception as exc:
                        state['last_request_error'] = str(exc.args[0])[:160] if exc.args else 'request_handler_failed'
                        _atomic_json(root / 'session.json', state)
                time.sleep(poll_seconds)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    finally:
        if owned_lock:
            lock.close()


def recover_cleanup(root, *, project, runner=None):
    """Remove only receipt-verified fixture resources, with no broad Compose action."""
    from native_compose_smoke import ResourceRegistry, bootstrap_lifecycle
    import subprocess

    root = _root(root)
    if not isinstance(project, str) or not re.fullmatch(r'ncs_[a-f0-9]{12}', project):
        raise ValueError('recovery_project_rejected')
    with acquire_lock(root):
        state = read_state(root)
        if state.get('project') != project:
            raise ValueError('recovery_project_mismatch')
        registry = ResourceRegistry(SimpleNamespace(project=project, private=root), runner=runner or subprocess.run)
        receipt_files = sorted(path for path in root.glob('resource-*.json')
                               if path.name != 'resource-receipts.json')
        if not receipt_files:
            raise ValueError('recovery_receipts_missing')
        for path in receipt_files:
            item = _read_json(path, MAX_RECEIPT_BYTES, 'recovery_receipt_invalid')
            if (not isinstance(item, dict) or item.get('run') != project
                    or item.get('task') != 'native-compose-allocation'
                    or item.get('kind') not in ('container', 'network', 'volume')
                    or not item.get('allocation_attempted') or not isinstance(item.get('receipt'), dict)):
                raise ValueError('recovery_receipt_incomplete')
            item['path'] = path
            registry.items.append(item)
        if len({(item['kind'], item['name']) for item in registry.items}) != len(registry.items):
            raise ValueError('recovery_receipt_duplicate')
        state_receipts = state.get('receipts')
        if not isinstance(state_receipts, list):
            raise ValueError('recovery_session_receipts_missing')
        state_identity = {(row.get('kind'), row.get('name'), json.dumps(row.get('receipt'), sort_keys=True))
                          for row in state_receipts if isinstance(row, dict)}
        resource_identity = {(item['kind'], item['name'], json.dumps(item['receipt'], sort_keys=True))
                             for item in registry.items}
        if state_identity != resource_identity:
            raise ValueError('recovery_session_receipts_mismatch')
        first = registry.items[0]
        lifecycle = bootstrap_lifecycle()
        aggregate_path = root / 'resource-receipts.json'
        if aggregate_path.exists():
            aggregate = _read_json(aggregate_path, MAX_STATE_BYTES, 'recovery_receipt_invalid')
            expected = {(item['kind'], item['name'], json.dumps(item['receipt'], sort_keys=True))
                        for item in registry.items}
            if not isinstance(aggregate, list) or any(not isinstance(row, dict) for row in aggregate):
                raise ValueError('recovery_receipt_invalid')
            try:
                persisted = {(row['kind'], row['name'], json.dumps(row['receipt'], sort_keys=True)) for row in aggregate
                             if row.get('allocation_attempted')}
            except (KeyError, TypeError):
                raise ValueError('recovery_receipt_invalid') from None
            if expected != persisted:
                raise ValueError('recovery_receipt_mismatch')
        if state['phase'] != 'FINALIZING':
            outcome = state.get('workload_outcome') if state['phase'] == 'CLOSED' else 'STALE_RECOVERY'
            state = _set_finalizing(root, state, outcome or 'STALE_RECOVERY')
        ordered = sorted(reversed(registry.items), key=lambda item: {'container': 0, 'network': 1, 'volume': 2}[item['kind']])
        try:
            bootstrap_item = next((item for item in registry.items
                                   if item['labels'].get('bootstrap.managed') == 'true'), None)
            if bootstrap_item is None:
                raise ValueError('recovery_bootstrap_receipt_missing')
            lifecycle.reconcile_child_receipts(root, bootstrap_item['name'], dict(bootstrap_item['labels']),
                runner=runner or subprocess.run)
            deadline = time.monotonic() + 2 * 60
            for item in ordered:
                if time.monotonic() >= deadline:
                    raise ValueError('recovery_cleanup_deadline')
                registry.remove(item)
        except Exception as exc:
            finalize(root, state, cleanup_verified=False, cleanup_error=str(exc)[:160])
            raise
        finalize(root, state, cleanup_verified=True)
        return {'cleanup_verified': True, 'resources': len(ordered)}
