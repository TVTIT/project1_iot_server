"""B-only bounded process resource sampling helper for P8 generator workload.

Samples current runner process tree using reusable process_tree_resources reader.
Strictly scoped to B runner process tree; no Docker/session API, no admin scrapers.
Provides JSON-safe sample exports and exclusive private CSV writer for resources-generator.csv.
"""
from datetime import datetime, timezone
import csv
import json
import math
import os
from pathlib import Path
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

# Bounded configuration defaults
DEFAULT_SAMPLE_INTERVAL_SECONDS: float = 0.5
DEFAULT_MAX_SNAPSHOTS: int = 128
MAX_ALLOWED_SNAPSHOTS: int = 128
DEFAULT_DEADLINE_SECONDS: float = 120.0
MAX_ALLOWED_DEADLINE_SECONDS: float = 120.0
DEFAULT_JOIN_TIMEOUT_SECONDS: float = 3.0

GENERATOR_RESOURCES_CSV_COLUMNS: Tuple[str, ...] = (
    'timestamp',
    'elapsed_seconds',
    'interval_seconds',
    'available',
    'cpu_percent',
    'delta_ticks',
    'rss_bytes',
    'process_count',
    'current_fd_count',
    'reason',
)


def _resolve_repo_root() -> Path:
    """Resolve repository root directory anchored from this file."""
    return Path(__file__).resolve().parent.parent


def _get_default_process_tree_reader() -> Callable:
    """Lazy import of reusable process_tree_resources reader with anchored fallback."""
    try:
        from scripts.tests.paper_p8_support import process_tree_resources
        return process_tree_resources
    except ModuleNotFoundError:
        repo_root = _resolve_repo_root()
        repo_str = str(repo_root)
        if repo_str not in sys.path:
            sys.path.insert(0, repo_str)
        from scripts.tests.paper_p8_support import process_tree_resources
        return process_tree_resources


def _safe_fd_count(pid: int, proc_root: Path = Path('/proc')) -> Optional[int]:
    """Safely count open file descriptors for root process pid without crashing.

    Explicitly scoped to the root process only; does not scan child process FDs.
    """
    if not isinstance(pid, int) or pid <= 0:
        return None
    try:
        fd_dir = Path(proc_root) / str(pid) / 'fd'
        with os.scandir(fd_dir) as it:
            return sum(1 for _ in it)
    except (OSError, ValueError, PermissionError):
        return None


def _call_reader(reader: Callable, pid: int, proc_root: Path) -> Any:
    """Invoke reader handling signature differences (proc_root keyword or positional)."""
    try:
        return reader(pid, proc_root=proc_root)
    except TypeError:
        return reader(pid)


class GeneratorResourceSampler:
    """Bounded B-only process tree resource sampler.

    Samples current runner process tree using reusable process_tree_resources reader at
    bounded intervals (default 0.5s), capped at max 128 snapshots, deadline <= 120s,
    with finite thread join. Safe for use as context manager or explicit start()/stop().
    """

    def __init__(
        self,
        root_pid: Optional[int] = None,
        *,
        interval: float = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        max_snapshots: int = DEFAULT_MAX_SNAPSHOTS,
        deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
        join_timeout: float = DEFAULT_JOIN_TIMEOUT_SECONDS,
        reader: Optional[Callable] = None,
        fd_reader: Optional[Callable] = None,
        proc_root: Path = Path('/proc'),
        time_fn: Callable[[], float] = time.monotonic,
        host_cpus: Optional[int] = None,
        clk_tck: Optional[int] = None,
        cpu_normalization: str = 'host_percent',
    ) -> None:
        if root_pid is not None:
            if not isinstance(root_pid, int) or isinstance(root_pid, bool) or root_pid <= 0:
                raise ValueError(f'root_pid must be a positive integer, got {root_pid!r}')
            self.root_pid: int = root_pid
        else:
            self.root_pid = os.getpid()

        # Strict validation of finite numeric timings
        if not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval <= 0:
            raise ValueError(f'interval must be a finite positive number, got {interval!r}')
        self.interval: float = float(interval)

        if not isinstance(max_snapshots, int) or isinstance(max_snapshots, bool) or max_snapshots <= 0:
            raise ValueError(f'max_snapshots must be a positive integer, got {max_snapshots!r}')
        self.max_snapshots: int = min(MAX_ALLOWED_SNAPSHOTS, int(max_snapshots))

        if not isinstance(deadline_seconds, (int, float)) or not math.isfinite(deadline_seconds) or deadline_seconds <= 0:
            raise ValueError(f'deadline_seconds must be a finite positive number, got {deadline_seconds!r}')
        self.deadline_seconds: float = min(MAX_ALLOWED_DEADLINE_SECONDS, float(deadline_seconds))

        if not isinstance(join_timeout, (int, float)) or not math.isfinite(join_timeout) or join_timeout <= 0:
            raise ValueError(f'join_timeout must be a finite positive number, got {join_timeout!r}')
        self.join_timeout: float = float(join_timeout)

        self.proc_root: Path = Path(proc_root)
        self._time_fn: Callable[[], float] = time_fn

        # CPU normalization: 'host_percent' or 'core_percent'
        if cpu_normalization not in ('host_percent', 'core_percent'):
            raise ValueError(f"Invalid cpu_normalization: {cpu_normalization}; expected 'host_percent' or 'core_percent'")
        self.cpu_normalization: str = cpu_normalization

        # Environment limits
        if host_cpus is not None:
            if not isinstance(host_cpus, int) or isinstance(host_cpus, bool) or host_cpus <= 0:
                raise ValueError(f'host_cpus must be a positive integer, got {host_cpus!r}')
            self.host_cpus: int = host_cpus
        else:
            self.host_cpus = max(1, os.cpu_count() or 1)

        if clk_tck is not None:
            if not isinstance(clk_tck, int) or isinstance(clk_tck, bool) or clk_tck <= 0:
                raise ValueError(f'clk_tck must be a positive integer, got {clk_tck!r}')
            self.clk_tck: int = clk_tck
        else:
            try:
                self.clk_tck = os.sysconf('SC_CLK_TCK')
            except (AttributeError, ValueError, OSError):
                self.clk_tck = 100

        # Readers
        self._reader: Callable = reader if reader is not None else _get_default_process_tree_reader()
        self._fd_reader: Optional[Callable] = fd_reader

        # Internal synchronization and state
        self._stop_event: threading.Event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock: threading.Lock = threading.Lock()
        self._samples: List[Dict[str, Any]] = []
        self._errors: List[str] = []
        self._last_valid_proc: Optional[Tuple[float, Dict[str, Any]]] = None
        self._started_monotonic: Optional[float] = None
        self._started: bool = False
        self._stopped: bool = False
        self._is_running: bool = False

    def _do_reader_io(self) -> Tuple[Any, Optional[int]]:
        """Perform reader and FD I/O completely outside state locks."""
        try:
            proc = _call_reader(self._reader, self.root_pid, self.proc_root)
        except Exception as exc:
            proc = {'available': None, 'reason': f'reader_exception_{type(exc).__name__}'}

        current_fd_count = None
        if self._fd_reader is not None:
            try:
                current_fd_count = _call_reader(self._fd_reader, self.root_pid, self.proc_root)
            except Exception:
                current_fd_count = None
        else:
            current_fd_count = _safe_fd_count(self.root_pid, self.proc_root)

        return proc, current_fd_count

    def start(self) -> 'GeneratorResourceSampler':
        """Start sampler background thread and take initial baseline snapshot."""
        with self._lock:
            if self._started:
                if self._stopped:
                    raise RuntimeError('sampler_stopped')
                raise RuntimeError('sampler_already_started')
            self._started = True
            self._is_running = True
            self._stopped = False
            self._stop_event.clear()
            self._started_monotonic = self._time_fn()

        # Perform initial baseline read outside lock
        proc, fd_count = self._do_reader_io()

        with self._lock:
            if not self._stopped:
                self._commit_snapshot_locked(proc, fd_count)

        self._thread = threading.Thread(target=self._worker_loop, name='p8-generator-resource-sampler', daemon=True)
        self._thread.start()
        return self

    def _worker_loop(self) -> None:
        """Background sampling loop; does not hold locks during sleep or I/O."""
        while True:
            with self._lock:
                if self._stopped:
                    break
                now = self._time_fn()
                elapsed = now - (self._started_monotonic if self._started_monotonic is not None else now)
                remaining = self.deadline_seconds - elapsed
                if remaining <= 0:
                    if 'collector_deadline_exceeded' not in self._errors:
                        self._errors.append('collector_deadline_exceeded')
                    break
                if len(self._samples) >= self.max_snapshots:
                    if 'snapshot_cap_reached' not in self._errors:
                        self._errors.append('snapshot_cap_reached')
                    break
                # Wait bounded by interval or remaining deadline, whichever is smaller
                wait_time = min(self.interval, max(0.0001, remaining))

            if self._stop_event.wait(wait_time):
                break

            with self._lock:
                if self._stopped:
                    break
                now = self._time_fn()
                elapsed = now - (self._started_monotonic if self._started_monotonic is not None else now)
                if elapsed >= self.deadline_seconds:
                    if 'collector_deadline_exceeded' not in self._errors:
                        self._errors.append('collector_deadline_exceeded')
                    break
                if len(self._samples) >= self.max_snapshots:
                    if 'snapshot_cap_reached' not in self._errors:
                        self._errors.append('snapshot_cap_reached')
                    break

            # Perform reader I/O completely outside lock
            proc, fd_count = self._do_reader_io()

            # Commit result under short lock
            with self._lock:
                if self._stopped:
                    break
                self._commit_snapshot_locked(proc, fd_count)
                if len(self._samples) >= self.max_snapshots:
                    if 'snapshot_cap_reached' not in self._errors:
                        self._errors.append('snapshot_cap_reached')
                    break

    def _commit_snapshot_locked(self, proc: Any, current_fd_count: Optional[int]) -> None:
        """Commit an immutable sample into self._samples while holding self._lock."""
        now = self._time_fn()
        elapsed = now - (self._started_monotonic if self._started_monotonic is not None else now)
        if self._started_monotonic is not None and elapsed > self.deadline_seconds:
            if 'collector_deadline_exceeded' not in self._errors:
                self._errors.append('collector_deadline_exceeded')
            return
        if len(self._samples) >= self.max_snapshots:
            if 'snapshot_cap_reached' not in self._errors:
                self._errors.append('snapshot_cap_reached')
            return

        stamp = datetime.now(timezone.utc).isoformat()

        # Handle unavailable proc (disappeared, unreadable, cap reached)
        if not isinstance(proc, dict) or not proc.get('available'):
            reason = proc.get('reason', 'generator_process_unavailable') if isinstance(proc, dict) else 'generator_process_unavailable'
            self._last_valid_proc = None  # Reset baseline on process tree loss
            sample = {
                'sample_index': len(self._samples),
                'timestamp': stamp,
                'elapsed_seconds': round(elapsed, 6),
                'interval_seconds': None,
                'available': False,
                'cpu_percent': None,
                'delta_ticks': None,
                'rss_bytes': None,
                'process_count': None,
                'current_fd_count': current_fd_count,
                'reason': reason,
            }
            self._samples.append(sample)
            return

        rss_bytes = proc.get('rss_bytes')
        process_count = proc.get('process_count')
        curr_processes = proc.get('processes', {})

        cpu_percent = None
        delta_ticks = None
        interval_seconds = None
        sample_reason = None

        if self._last_valid_proc is None:
            interval_seconds = None
            delta_ticks = None
            cpu_percent = None
            sample_reason = 'first_sample_baseline_unavailable' if len(self._samples) == 0 else 'no_origin'
            self._last_valid_proc = (now, proc)
        else:
            prev_time, prev_proc = self._last_valid_proc
            interval = now - prev_time
            if interval <= 0:
                interval_seconds = None
                delta_ticks = None
                cpu_percent = None
                sample_reason = 'invalid_elapsed_duration'
                self._last_valid_proc = (now, proc)
            else:
                interval_seconds = round(interval, 6)
                prev_processes = prev_proc.get('processes', {})
                prev_keys = set(prev_processes.keys())
                curr_keys = set(curr_processes.keys())

                if prev_keys - curr_keys:
                    # Explicit process loss (process exited, or PID reused with new starttime)
                    cpu_percent = None
                    delta_ticks = None
                    sample_reason = 'process_loss'
                    self._last_valid_proc = (now, proc)
                elif curr_keys - prev_keys:
                    # New child process observed: lifetime CPU cannot be attributed to this interval without birth proof.
                    # Rebaseline on new keys without fake 0 or ticks overcount.
                    cpu_percent = None
                    delta_ticks = None
                    sample_reason = 'process_appeared'
                    self._last_valid_proc = (now, proc)
                else:
                    # Exact same keys present: calculate ticks delta
                    ticks_delta = 0
                    reset_detected = False
                    for key in curr_keys:
                        diff = curr_processes[key]['ticks'] - prev_processes[key]['ticks']
                        if diff < 0:
                            reset_detected = True
                            break
                        ticks_delta += diff

                    if reset_detected:
                        cpu_percent = None
                        delta_ticks = None
                        sample_reason = 'counter_reset_negative_delta'
                        self._last_valid_proc = (now, proc)
                    else:
                        delta_ticks = ticks_delta
                        if self.cpu_normalization == 'core_percent':
                            raw_percent = (ticks_delta / self.clk_tck / interval) * 100.0
                        else:
                            raw_percent = (ticks_delta / self.clk_tck / interval / self.host_cpus) * 100.0
                        cpu_percent = round(raw_percent, 6)
                        sample_reason = None
                        self._last_valid_proc = (now, proc)

        sample = {
            'sample_index': len(self._samples),
            'timestamp': stamp,
            'elapsed_seconds': round(elapsed, 6),
            'interval_seconds': interval_seconds,
            'available': True,
            'cpu_percent': cpu_percent,
            'delta_ticks': delta_ticks,
            'rss_bytes': rss_bytes,
            'process_count': process_count,
            'current_fd_count': current_fd_count,
            'reason': sample_reason,
        }
        self._samples.append(sample)

    def stop(self) -> Dict[str, Any]:
        """Idempotently stop sampling thread and return safe report dict.

        Sets stop event first without holding lock; does not hold lock during thread join.
        If reader thread hangs, returns within join_timeout budget and marks collector_join_timeout.
        """
        # Set stop event first
        self._stop_event.set()

        thread_to_join = None
        with self._lock:
            if not self._stopped:
                self._stopped = True
                self._is_running = False
                thread_to_join = self._thread

        if thread_to_join and thread_to_join.is_alive():
            if threading.current_thread() != thread_to_join:
                thread_to_join.join(timeout=self.join_timeout)
                if thread_to_join.is_alive():
                    with self._lock:
                        if 'collector_join_timeout' not in self._errors:
                            self._errors.append('collector_join_timeout')
                else:
                    with self._lock:
                        self._thread = None

        return self.report()

    def report(self) -> Dict[str, Any]:
        """Return JSON-safe descriptive report with declared units, scopes, and samples."""
        with self._lock:
            samples_copy = [dict(s) for s in self._samples[:self.max_snapshots]]
            errors_copy = list(self._errors)

        valid_rss = [s['rss_bytes'] for s in samples_copy if s.get('rss_bytes') is not None]
        missing_reasons = sorted(set(s['reason'] for s in samples_copy if s.get('reason') is not None))

        normalization_desc = (
            'ticks_delta / CLK_TCK / interval_seconds / host_cpus * 100'
            if self.cpu_normalization == 'host_percent'
            else 'ticks_delta / CLK_TCK / interval_seconds * 100'
        )

        return {
            'schema_version': 1,
            'role': 'generator_B',
            'cpu_normalization': self.cpu_normalization,
            'cpu_normalization_description': normalization_desc,
            'host_cpus': self.host_cpus,
            'clk_tck': self.clk_tck,
            'scopes': {
                'process_tree_scope': 'owned_children_capped_at_32',
                'fd_scope': 'root_process_only',
                'rss_scope': 'sum_sampled_RSS_may_double_count_shared_pages_not_peak',
                'cpu_scope': 'monotonic_process_tree_ticks_delta_same_keys_only',
            },
            'units': {
                'rss_bytes': 'bytes (sum_sampled_RSS_may_double_count_shared_pages)',
                'cpu_percent': 'percent',
                'elapsed_seconds': 'seconds',
                'interval_seconds': 'seconds',
                'delta_ticks': 'ticks',
                'process_count': 'count',
                'current_fd_count': 'count (root_process_only)',
            },
            'clock': {
                'monotonic_start': self._started_monotonic,
                'sample_interval_seconds': self.interval,
                'deadline_seconds': self.deadline_seconds,
                'max_snapshots': self.max_snapshots,
            },
            'summary': {
                'total_samples': len(samples_copy),
                'available_samples': sum(1 for s in samples_copy if s.get('available')),
                'unavailable_samples': sum(1 for s in samples_copy if not s.get('available')),
                'cpu_available_samples': sum(1 for s in samples_copy if s.get('cpu_percent') is not None),
                'max_sample_rss_bytes': max(valid_rss) if valid_rss else None,
                'max_sample_rss_qualification': 'sum_sampled_RSS_may_double_count_shared_pages_not_peak',
                'fd_scope': 'root_process_only',
                'missing_reasons': missing_reasons,
                'errors': errors_copy,
            },
            'samples': samples_copy,
        }

    def write_csv(self, path: Union[str, Path]) -> Path:
        """Helper to write the current report samples to a private CSV file."""
        return write_generator_resources_csv(self.report(), path)

    def __enter__(self) -> 'GeneratorResourceSampler':
        self.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.stop()
        return False


# BoundedProcessSampler alias
BoundedProcessSampler = GeneratorResourceSampler


def write_generator_resources_csv(
    report_or_samples: Union[Dict[str, Any], List[Dict[str, Any]]],
    path: Union[str, Path]
) -> Path:
    """Write generator resource samples to an exclusive, private (0600) CSV file.

    Parameters:
        report_or_samples: Report dictionary returned by GeneratorResourceSampler or list of samples.
        path: Destination CSV file path.

    Validation rules:
        - Parent directory must exist, be a directory, and not be a symlink.
        - Target path must not already exist and must not be a symlink.
        - File is created exclusively (O_CREAT | O_EXCL | O_NOFOLLOW) with mode 0600.
        - Unavailable values are written as blank (''), never converted to fake zeros.
        - Fixed columns only per GENERATOR_RESOURCES_CSV_COLUMNS.
        - Write/serialization failures propagate exceptions without swallowing or publishing partial file as success.
    """
    if isinstance(report_or_samples, dict):
        samples = report_or_samples.get('samples', [])
    elif isinstance(report_or_samples, list):
        samples = report_or_samples
    else:
        raise TypeError(f'report_or_samples must be a dict or list, got {type(report_or_samples).__name__}')

    target = Path(path)

    # Validate parent directory
    try:
        parent_stat = target.parent.lstat()
    except OSError:
        raise ValueError('generator_resources_csv_parent_invalid') from None

    if not target.parent.is_dir() or target.parent.is_symlink():
        raise ValueError('generator_resources_csv_parent_invalid')

    # Validate target does not exist and is not a symlink
    if target.is_symlink():
        raise ValueError('generator_resources_csv_target_symlink_rejected')
    if target.exists():
        raise FileExistsError(f'Target file already exists: {target}')

    # Create private file exclusively without following symlinks
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(target, flags, 0o600)
    try:
        stream = os.fdopen(fd, 'w', newline='', encoding='utf-8')
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        raise

    with stream:
        os.fchmod(stream.fileno(), 0o600)
        writer = csv.DictWriter(stream, fieldnames=GENERATOR_RESOURCES_CSV_COLUMNS)
        writer.writeheader()
        for sample in samples:
            if not isinstance(sample, dict):
                raise ValueError(f'Malformed sample item: expected dict, got {type(sample).__name__}')
            row = {}
            for col in GENERATOR_RESOURCES_CSV_COLUMNS:
                val = sample.get(col)
                if val is None:
                    row[col] = ''
                elif isinstance(val, bool):
                    row[col] = 'true' if val else 'false'
                elif isinstance(val, (int, float)):
                    if isinstance(val, float):
                        if not math.isfinite(val):
                            raise ValueError(f'Non-finite float value in column {col}: {val}')
                        row[col] = f'{val:.6f}'
                    else:
                        row[col] = str(val)
                else:
                    row[col] = str(val)
            writer.writerow(row)
        stream.flush()

    return target


# Alias
write_resources_generator_csv = write_generator_resources_csv
