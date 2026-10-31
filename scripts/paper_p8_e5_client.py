#!/usr/bin/env python3
"""Foreground E5 B viewer holder and local, correlation-bound stop request seam."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import stat
import subprocess
import time
from typing import Any

try:
    from paper_p8_resources import GeneratorResourceSampler
except ImportError:  # package import (scripts.paper_p8_e5_client)
    from scripts.paper_p8_resources import GeneratorResourceSampler

MAX_DEADLINE_SECONDS = 3600
MAX_POLL_SECONDS = 0.2
MAX_PHASE_ACK_SECONDS = 10.0
CORRELATION_KEYS = ('session_id', 'run_id', 'binding_digest', 'manifest_sha256', 'binary_sha256')
PHASE_SEQUENCE = tuple((phase, boundary) for phase in ('initial', 'warm_replay', 'control') for boundary in ('begin', 'end'))


class HolderError(RuntimeError):
    """A bounded E5 viewer-holder failure with a stable machine-readable code."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def _private_dir(path: Path, *, create: bool = False, fresh: bool = False) -> Path:
    path = Path(os.path.abspath(path))
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=not fresh)
    if path.resolve(strict=True) != path:
        raise HolderError('directory_path_symlink_rejected')
    st = path.lstat()
    if path.is_symlink() or not path.is_dir() or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise HolderError('private_directory_rejected')
    return path


def _private_regular(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if path.resolve(strict=True) != path:
        raise HolderError('input_path_symlink_rejected')
    st = path.lstat()
    if path.is_symlink() or not path.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise HolderError('private_input_rejected')
    return path


def _trusted_binary(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    if path.resolve(strict=True) != path:
        raise HolderError('binary_path_symlink_rejected')
    st = path.lstat()
    if (path.is_symlink() or not path.is_file() or st.st_uid != os.getuid()
            or st.st_mode & 0o022 or not os.access(path, os.X_OK)):
        raise HolderError('binary_rejected')
    return path


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    tmp = path.with_name(path.name + '.tmp')
    data = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def _read_json(path: Path, max_bytes: int = 1 << 20) -> dict[str, Any]:
    st = path.lstat()
    if path.is_symlink() or not path.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o077 or st.st_size > max_bytes:
        raise HolderError('private_artifact_rejected')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HolderError('malformed_artifact') from exc
    if not isinstance(value, dict):
        raise HolderError('malformed_artifact')
    return value


def _correlation(value: dict[str, Any]) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != set(CORRELATION_KEYS):
        raise HolderError('correlation_shape_invalid')
    result = {}
    for key in CORRELATION_KEYS:
        item = value[key]
        if not isinstance(item, str) or not item or len(item) > 256 or any(ord(c) < 32 for c in item):
            raise HolderError('correlation_value_invalid')
        result[key] = item
    for key in ('manifest_sha256', 'binary_sha256', 'binding_digest'):
        if len(result[key]) != 64 or any(c not in '0123456789abcdef' for c in result[key]):
            raise HolderError('correlation_digest_invalid')
    return result


def _validate_manifest(manifest_path: Path, correlation: dict[str, str]) -> dict[str, Any]:
    _private_regular(manifest_path)
    if _digest(manifest_path) != correlation['manifest_sha256']:
        raise HolderError('manifest_digest_mismatch')
    try:
        m = json.loads(manifest_path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise HolderError('manifest_invalid') from exc
    viewer = m.get('viewer') if isinstance(m, dict) else None
    topology = m.get('topology') if isinstance(m, dict) else None
    if (not isinstance(m, dict) or not isinstance(viewer, dict) or not isinstance(topology, dict)
            or m.get('roles') != ['viewer'] or m.get('run_id') != correlation['run_id']
            or m.get('experiment') != 'E1' or viewer.get('mode', 'healthy') != 'healthy'
            or topology.get('viewers') != 2):
        raise HolderError('viewer_manifest_required')
    subs = m.get('subscriptions')
    ids = {row.get('viewer_id') for row in subs if isinstance(row, dict)} if isinstance(subs, list) else set()
    if len(subs or []) != 3 or len(ids) != 2 or None in ids:
        raise HolderError('e5_viewer_topology_required')
    return m


def _identity_matches(value: dict[str, Any], m: dict[str, Any], c: dict[str, str]) -> bool:
    return (value.get('run_id') == c['run_id'] and value.get('experiment') == 'E1'
            and value.get('arm') == m.get('arm') and value.get('profile') == m.get('profile')
            and value.get('mode') == m.get('mode'))


def read_viewer_holder_status(control_dir: str | Path) -> dict[str, Any]:
    """Read status.json; consumers must verify correlation against their bound inputs."""
    try:
        return _read_json(_private_dir(Path(control_dir)) / 'status.json')
    except FileNotFoundError as exc:
        raise HolderError('holder_status_missing') from exc


def _holder_lock_is_active(root: Path) -> bool:
    fd = os.open(root / 'viewer-holder.lock', os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0))
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise HolderError('holder_lock_rejected')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _publish_stop_request(root: Path, req: dict[str, Any]) -> None:
    """Complete and fsync the fixed request before making its name visible."""
    target = root / 'stop-request.json'
    tmp = root / ('.stop-request.' + os.urandom(12).hex() + '.tmp')
    data = json.dumps(req, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.link(tmp, target, follow_symlinks=False)
        dfd = os.open(root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except FileExistsError as exc:
        raise HolderError('stop_request_exists') from exc
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _publish_phase_request(root: Path, index: int, req: dict[str, Any]) -> None:
    target = root / f'phase-request-{index + 1:02d}.json'
    tmp = root / (f'.phase-request-{index + 1:02d}.' + os.urandom(12).hex() + '.tmp')
    data = json.dumps(req, sort_keys=True, separators=(',', ':'), allow_nan=False).encode() + b'\n'
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.link(tmp, target, follow_symlinks=False)
        dfd = os.open(root, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except FileExistsError as exc:
        raise HolderError('phase_request_exists') from exc
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def request_viewer_phase(control_dir: str | Path, correlation: dict[str, str], phase: str,
                         boundary: str, *, ack_timeout_seconds: float = 5.0) -> dict[str, Any]:
    """Publish and await one of six fixed, ordered boundaries on holder's clock."""
    root = _private_dir(Path(control_dir))
    c = _correlation(correlation)
    status = read_viewer_holder_status(root)
    if status.get('correlation') != c or status.get('state') not in ('READY', 'PHASE'):
        raise HolderError('holder_not_ready_or_correlation_mismatch')
    if not _holder_lock_is_active(root):
        raise HolderError('holder_not_active')
    index = status.get('next_checkpoint_index')
    if type(index) is not int or not 0 <= index < len(PHASE_SEQUENCE) or PHASE_SEQUENCE[index] != (phase, boundary):
        raise HolderError('phase_boundary_order_invalid')
    if not isinstance(ack_timeout_seconds, (int, float)) or isinstance(ack_timeout_seconds, bool) or not 0 < ack_timeout_seconds <= MAX_PHASE_ACK_SECONDS:
        raise HolderError('phase_ack_timeout_invalid')
    req = {'schema_version': 1, 'action': 'phase', 'correlation': c, 'owner_token': status.get('owner_token'),
           'phase': phase, 'boundary': boundary, 'sequence': index + 1}
    _publish_phase_request(root, index, req)
    ack_path = root / f'phase-ack-{index + 1:02d}.json'
    deadline = time.monotonic() + float(ack_timeout_seconds)
    while time.monotonic() < deadline:
        if ack_path.exists():
            ack = _read_json(ack_path)
            checkpoint = ack.get('checkpoint')
            if (ack.get('correlation') != c or ack.get('owner_token') != status.get('owner_token')
                    or not isinstance(checkpoint, dict) or checkpoint.get('phase') != phase
                    or checkpoint.get('boundary') != boundary or checkpoint.get('child_alive') is not True
                    or checkpoint.get('ready_verified') is not True):
                raise HolderError('phase_ack_invalid')
            return checkpoint
        time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    raise HolderError('phase_ack_timeout')


def request_viewer_stop(control_dir: str | Path, correlation: dict[str, str]) -> dict[str, Any]:
    """Atomically request planned stop. This function never signals or kills a PID."""
    root = _private_dir(Path(control_dir))
    c = _correlation(correlation)
    status = read_viewer_holder_status(root)
    if (status.get('correlation') != c or status.get('state') not in ('READY', 'PHASE')
            or status.get('next_checkpoint_index') != len(PHASE_SEQUENCE)
            or not isinstance(status.get('checkpoints'), list)
            or len(status['checkpoints']) != len(PHASE_SEQUENCE)
            or not isinstance(status.get('owner_token'), str) or not status['owner_token']
            or not _holder_lock_is_active(root)):
        raise HolderError('control_end_checkpoint_required')
    req = {'schema_version': 1, 'action': 'stop', 'correlation': c, 'owner_token': status['owner_token']}
    _publish_stop_request(root, req)
    return {'requested': True, 'correlation': c}


def _ready(path: Path) -> bool:
    try:
        v = _read_json(path)
    except (OSError, HolderError):
        return False
    return v.get('status') == 'ready' and v.get('connected') == 2 and v.get('subscribed') == 2 and v.get('principals') == 2 and v.get('mode') == 'healthy'


def _validate_result(path: Path, m: dict[str, Any], c: dict[str, str]) -> dict[str, Any]:
    result = _read_json(path)
    if not _identity_matches(result, m, c):
        raise HolderError('result_identity_mismatch')
    if result.get('status') != 'COMPLETE' or result.get('planned_stop') is not True or result.get('planned_stop_reason') != 'stopped_by_operator':
        raise HolderError('planned_stop_result_required')
    if (result.get('connection_attempts') != 2 or result.get('socket_count') != 2
            or result.get('connected_distinct_viewers') != 2 or result.get('subscribed_distinct_viewers') != 2
            or result.get('receiving_distinct_viewers') != 2 or result.get('principal_count') != 2
            or result.get('reconnections') != 0 or result.get('unique_events') != 20
            or result.get('duplicates') != 0 or result.get('wrong_namespace') != 0
            or result.get('primary_error')):
        raise HolderError('result_viewer_provenance_invalid')
    return result


def _fresh_output_path(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    parent = _private_dir(path.parent)
    if path.resolve(strict=False) != path or path.parent != parent:
        raise HolderError('output_path_symlink_rejected')
    try:
        path.lstat()
    except FileNotFoundError:
        return path
    raise HolderError('output_directory_not_fresh')


def run_viewer_holder(binary: str | Path, manifest_path: str | Path, output_dir: str | Path,
                      control_dir: str | Path, correlation: dict[str, str], deadline_seconds: int,
                      *, poll_seconds: float = 0.1) -> dict[str, Any]:
    """Own one E5 B viewer foreground until a correlated planned-stop request or failure."""
    c = _correlation(correlation)
    if not isinstance(deadline_seconds, int) or isinstance(deadline_seconds, bool) or not 1 <= deadline_seconds <= MAX_DEADLINE_SECONDS:
        raise HolderError('deadline_invalid')
    if not isinstance(poll_seconds, (int, float)) or isinstance(poll_seconds, bool) or not 0 < poll_seconds <= MAX_POLL_SECONDS:
        raise HolderError('poll_interval_invalid')
    binary = _trusted_binary(Path(binary))
    if not os.access(binary, os.X_OK) or _digest(binary) != c['binary_sha256']:
        raise HolderError('binary_digest_mismatch')
    manifest_path = _private_regular(Path(manifest_path))
    manifest = _validate_manifest(manifest_path, c)
    control = _private_dir(Path(control_dir))
    output = _fresh_output_path(Path(output_dir))
    if output == control or output in control.parents or control in output.parents:
        raise HolderError('control_output_overlap')
    lock_path = control / 'viewer-holder.lock'
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        lock_stat = os.fstat(lock_fd)
        if not stat.S_ISREG(lock_stat.st_mode) or lock_stat.st_uid != os.getuid() or lock_stat.st_mode & 0o077:
            raise HolderError('holder_lock_rejected')
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise HolderError('holder_already_active') from exc
        owner = os.urandom(24).hex()
        start = time.monotonic()
        status_path = control / 'status.json'
        events_path = control / 'events.jsonl'
        if status_path.exists() or events_path.exists() or (control / 'stop-request.json').exists():
            raise HolderError('control_not_fresh')

        child = None
        def status(state: str, **extra: Any) -> None:
            _atomic_json(status_path, {'schema_version': 1, 'state': state, 'correlation': c,
                'owner_token': owner, 'pid': child.pid if child else None, 'updated_monotonic': time.monotonic(), **extra})
        def event(kind: str, **extra: Any) -> None:
            fd = os.open(events_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
            with os.fdopen(fd, 'a', encoding='utf-8') as f:
                f.write(json.dumps({'event': kind, 'monotonic': time.monotonic(), **extra}, sort_keys=True) + '\n')
                f.flush(); os.fsync(f.fileno())

        sampler = None
        saved_handlers = {}
        interrupted = []
        try:
            status('STARTING', next_checkpoint_index=0, checkpoints=[])
            for sig in (signal.SIGINT, signal.SIGTERM):
                saved_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, lambda signum, frame: interrupted.append(signum))
            event('holder_started')
            remaining_preflight = deadline_seconds - (time.monotonic() - start)
            if remaining_preflight <= 0:
                raise HolderError('holder_deadline_expired')
            try:
                preflight = subprocess.run([str(binary), '--manifest', str(manifest_path), '--preflight'],
                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    timeout=min(30, remaining_preflight), check=False)
            except subprocess.TimeoutExpired as exc:
                raise HolderError('holder_deadline_expired') from exc
            if interrupted:
                raise HolderError('holder_interrupted_during_preflight')
            if preflight.returncode != 0:
                raise HolderError('preflight_failed')
            if time.monotonic() - start >= deadline_seconds:
                raise HolderError('holder_deadline_expired')
            argv = [str(binary), '--manifest', str(manifest_path), '--output-dir', str(output), '--run', '--viewer-stop-on-sigusr1']
            child = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
            sampler = GeneratorResourceSampler(root_pid=child.pid, deadline_seconds=min(deadline_seconds, 120), max_snapshots=128)
            sampler.start()
            status('STARTING', child_pid=child.pid)
            event('viewer_child_started', child_pid=child.pid)
            ready_path = output / 'ready.json'
            ready_seen = False
            ready_elapsed = None
            checkpoints = []
            stop_seen = False
            while time.monotonic() - start < deadline_seconds:
                rc = child.poll()
                if interrupted:
                    raise HolderError('holder_interrupted')
                if rc is not None:
                    raise HolderError('child_exited_before_join')
                if not ready_seen and _ready(ready_path):
                    if child.poll() is not None:
                        raise HolderError('child_exited_before_ready')
                    ready_seen = True
                    ready_elapsed = max(0.0, time.monotonic() - start)
                    status('READY', ready_verified=True, ready_elapsed_seconds=ready_elapsed,
                           next_checkpoint_index=0, checkpoints=checkpoints)
                    event('viewer_ready_verified')
                checkpoint_index = len(checkpoints)
                for unexpected in range(checkpoint_index + 1, len(PHASE_SEQUENCE)):
                    if (control / f'phase-request-{unexpected + 1:02d}.json').exists():
                        raise HolderError('phase_boundary_order_invalid')
                request_path = control / f'phase-request-{checkpoint_index + 1:02d}.json'
                if request_path.exists():
                    if not ready_seen or child.poll() is not None:
                        raise HolderError('phase_before_live_ready_child')
                    req = _read_json(request_path)
                    phase, boundary = PHASE_SEQUENCE[checkpoint_index]
                    if req != {'schema_version': 1, 'action': 'phase', 'correlation': c,
                               'owner_token': owner, 'phase': phase, 'boundary': boundary,
                               'sequence': checkpoint_index + 1}:
                        raise HolderError('phase_request_invalid')
                    elapsed = max(ready_elapsed, time.monotonic() - start,
                                  checkpoints[-1]['elapsed_seconds'] if checkpoints else 0.0)
                    checkpoint = {'phase': phase, 'boundary': boundary, 'elapsed_seconds': elapsed,
                                  'child_alive': True, 'ready_verified': True}
                    checkpoints.append(checkpoint)
                    _atomic_json(control / f'phase-ack-{checkpoint_index + 1:02d}.json',
                                 {'schema_version': 1, 'correlation': c, 'owner_token': owner,
                                  'checkpoint': checkpoint})
                    status('PHASE', ready_verified=True, ready_elapsed_seconds=ready_elapsed,
                           next_checkpoint_index=len(checkpoints), checkpoints=checkpoints)
                    event('phase_checkpoint', **checkpoint, sequence=len(checkpoints))
                req_path = control / 'stop-request.json'
                if req_path.exists():
                    if not ready_seen:
                        raise HolderError('stop_before_ready')
                    if len(checkpoints) != len(PHASE_SEQUENCE) or PHASE_SEQUENCE[-1] != ('control', 'end'):
                        raise HolderError('control_end_checkpoint_required')
                    req = _read_json(req_path)
                    if req != {'schema_version': 1, 'action': 'stop', 'correlation': c, 'owner_token': owner}:
                        raise HolderError('stop_request_invalid')
                    if child.poll() is not None:
                        raise HolderError('child_exited_before_stop')
                    os.kill(child.pid, signal.SIGUSR1)
                    stop_seen = True
                    status('STOP_REQUESTED')
                    event('sigusr1_sent_to_owned_child')
                    break
                time.sleep(min(float(poll_seconds), max(0, deadline_seconds - (time.monotonic() - start))))
            else:
                raise HolderError('holder_deadline_expired')
            if not stop_seen:
                raise HolderError('stop_request_missing')
            remaining = deadline_seconds - (time.monotonic() - start)
            if remaining <= 0:
                raise HolderError('holder_deadline_expired')
            try:
                rc = child.wait(timeout=remaining)
            except subprocess.TimeoutExpired as exc:
                raise HolderError('child_join_timeout') from exc
            if rc != 0:
                raise HolderError('child_failed')
            result = _validate_result(output / 'workload.json', manifest, c)
            stop_elapsed = max(0.0, time.monotonic() - start)
            if (ready_elapsed is None or len(checkpoints) != 6
                    or not math.isfinite(ready_elapsed) or not math.isfinite(stop_elapsed)
                    or any(type(cp.get('elapsed_seconds')) not in (int, float)
                           or not math.isfinite(cp['elapsed_seconds']) for cp in checkpoints)
                    or any(checkpoints[i]['elapsed_seconds'] > checkpoints[i + 1]['elapsed_seconds'] for i in range(5))
                    or ready_elapsed > checkpoints[0]['elapsed_seconds']
                    or checkpoints[-1]['elapsed_seconds'] > stop_elapsed):
                raise HolderError('lifecycle_proof_invalid')
            lifecycle = {'schema_version': 1, 'clock': 'viewer_holder_local_elapsed_seconds',
                         'correlation': c, 'ready_elapsed_seconds': ready_elapsed,
                         'checkpoints': checkpoints, 'stop_elapsed_seconds': stop_elapsed, 'joined': True}
            _atomic_json(control / 'lifecycle.json', lifecycle)
            binary_provenance = {'sha256': c['binary_sha256'], 'os': os.name, 'platform': platform.system(),
                                 'architecture': platform.machine()}
            status('STOPPED', ready_verified=ready_seen, child_returncode=rc, joined=True,
                   binary_provenance=binary_provenance)
            event('child_joined_planned_stop', returncode=rc)
            _atomic_json(control / 'holder-result.json', {'schema_version': 1, 'correlation': c,
                'binary_provenance': binary_provenance, 'lifecycle': lifecycle, 'workload': result})
            return {'state': 'STOPPED', 'correlation': c, 'status_path': str(status_path),
                'events_path': str(events_path), 'output_dir': str(output), 'workload_path': str(output / 'workload.json'),
                'lifecycle_path': str(control / 'lifecycle.json'), 'holder_result_path': str(control / 'holder-result.json'),
                'result': result, 'lifecycle': lifecycle}
        except BaseException as exc:
            code = exc.code if isinstance(exc, HolderError) else ('holder_interrupted' if isinstance(exc, KeyboardInterrupt) else 'holder_internal_failure')
            cleanup_errors = []
            if child is not None and child.poll() is None:
                try:
                    child.terminate()
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    try:
                        child.kill()
                        child.wait(timeout=2)
                    except Exception as cleanup_exc:
                        cleanup_errors.append(type(cleanup_exc).__name__)
                except Exception as cleanup_exc:
                    cleanup_errors.append(type(cleanup_exc).__name__)
            if child is not None:
                try:
                    child.wait(timeout=2)
                except Exception as cleanup_exc:
                    cleanup_errors.append(type(cleanup_exc).__name__)
            joined = child is None or child.poll() is not None
            if child is not None and not joined:
                cleanup_errors.append('child_not_joined')
            try:
                status('FAILED', failure_code=code, joined=joined, cleanup_errors=cleanup_errors)
                event('holder_failed', failure_code=code, joined=joined, cleanup_errors=cleanup_errors)
            except Exception as status_exc:
                cleanup_errors.append('failure_status_' + type(status_exc).__name__)
            if isinstance(exc, HolderError):
                exc.cleanup_errors = cleanup_errors
                raise
            raise HolderError(code) from exc
        finally:
            for sig, handler in saved_handlers.items():
                signal.signal(sig, handler)
            if sampler is not None:
                try:
                    sampler.stop()
                    report = sampler.report()
                except Exception as sample_exc:
                    report = {'summary': {'total_samples': 0, 'available_samples': 0},
                              'errors': [type(sample_exc).__name__], 'samples': []}
                try:
                    if not output.exists():
                        raise HolderError('output_directory_missing')
                    _private_dir(output)
                    sampler.write_csv(output / 'resources-generator.csv')
                    summary = report.get('summary', {}) if isinstance(report, dict) else {}
                    available = bool(summary.get('total_samples', 0) and summary.get('available_samples', 0))
                    proof = {'available': available,
                             'reason': None if available else 'no_available_samples',
                             'scope': 'viewer_child_process_tree', 'includes_idle_and_manual_transfer_time': True,
                             'max_seconds': 120, 'max_samples': 128, 'report': report}
                except Exception as exc:
                    proof = {'available': False, 'reason': type(exc).__name__, 'scope': 'viewer_child_process_tree',
                             'includes_idle_and_manual_transfer_time': True, 'max_seconds': 120,
                             'max_samples': 128, 'report': report}
                try:
                    _atomic_json(control / 'resources-generator.json', proof)
                except Exception:
                    pass
    finally:
        os.close(lock_fd)
