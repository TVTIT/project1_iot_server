"""Bounded, read-only Linux resource snapshot for the isolated P8 E2 fixture.

Diskstats are host-device counters, not per-container latency; cgroup io.stat is
attribution only. Memory samples are instantaneous and do not measure peak RAM.
Linux /proc/stat guest counters are included in user/nice and therefore double
counted if those fields are added again independently. Host CPU counters are
converted from cumulative ticks to seconds using os.sysconf('SC_CLK_TCK'); cgroup
CPU counters remain cumulative microseconds, and memory/I/O counters retain their
kernel-reported byte, operation, or millisecond units.
"""
import hashlib
import math
import os
import re
import time
from datetime import datetime, timezone

MAX_CONTAINERS = 2
MAX_DEVICES = 16
MAX_IO_DEVICES = 16
MAX_FILE_BYTES = 65536
MAX_CGROUP_PATH = 4096
_REASONS = {'missing', 'permission_denied', 'malformed', 'unavailable', 'not_running',
            'ownership_unverified', 'cgroup_v2_unavailable', 'invalid_cgroup_path',
            'read_error', 'limit_exceeded'}


def _unavailable(reason):
    return {'available': False, 'reason': reason if reason in _REASONS else 'unavailable'}


def _read(path, *, cap=MAX_FILE_BYTES):
    try:
        with open(path, 'rb') as stream:
            data = stream.read(cap + 1)
        if len(data) > cap:
            return None, 'limit_exceeded'
        return data.decode('ascii'), None
    except FileNotFoundError:
        return None, 'missing'
    except PermissionError:
        return None, 'permission_denied'
    except (OSError, UnicodeError):
        return None, 'read_error'


def _number(value):
    if not re.fullmatch(r'[0-9]+', value or ''):
        raise ValueError
    number = int(value)
    if number > 2**63 - 1:
        raise ValueError
    return number


def _key_values(text):
    values = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2 or parts[0] in values:
            raise ValueError
        values[parts[0]] = _number(parts[1])
    return values


def _safe_cgroup_dir(root, rel):
    if not rel or rel == '/' or len(rel) > MAX_CGROUP_PATH or '\x00' in rel:
        return None
    # /proc reports hierarchy paths with one leading slash; strip only that root marker.
    parts = rel[1:].split('/') if rel.startswith('/') else rel.split('/')
    if any(part in ('', '.', '..') for part in parts):
        return None
    root_real = os.path.realpath(root)
    candidate = os.path.realpath(os.path.join(root_real, *parts))
    try:
        if os.path.commonpath((root_real, candidate)) != root_real or candidate == root_real:
            return None
    except ValueError:
        return None
    return candidate


def _field(path, parser):
    text, error = _read(path)
    if error:
        return _unavailable(error)
    try:
        return {'available': True, 'value': parser(text)}
    except (ValueError, KeyError, IndexError):
        return _unavailable('malformed')


def _cgroup_v2(proc_root, pid):
    text, error = _read(os.path.join(proc_root, str(pid), 'cgroup'), cap=8192)
    if error:
        return None, error
    for line in text.splitlines():
        fields = line.split(':', 2)
        if len(fields) == 3 and fields[0] == '0' and fields[1] == '':
            return fields[2], None
    return None, 'cgroup_v2_unavailable'


def _container(registry, entry, role, proc_root, cgroup_root, timeout=None):
    try:
        resolved = (registry.resolve(entry) if timeout is None else
                    registry.resolve(entry, timeout=timeout, read_only=True))
    except Exception:
        resolved = None
    if resolved is None:
        return {'kind': role, **_unavailable('ownership_unverified')}
    state = _get(resolved, 'State')
    pid = _get(state, 'Pid')
    cid = _get(resolved, 'Id')
    if type(pid) is not int or pid <= 0 or not isinstance(cid, str) or not cid:
        return {'kind': role, **_unavailable('not_running')}
    identity = hashlib.sha256((cid + ':' + str(pid)).encode()).hexdigest()[:24]
    rel, error = _cgroup_v2(proc_root, pid)
    if error:
        return {'kind': role, 'identity': identity, **_unavailable(error)}
    directory = _safe_cgroup_dir(cgroup_root, rel)
    if directory is None:
        return {'kind': role, 'identity': identity, **_unavailable('invalid_cgroup_path')}
    cpu = _cpu(directory)
    memory = _memory(directory)
    io = _io(directory)
    return {'kind': role, 'identity': identity, 'available': True,
            'cpu': cpu, 'memory': memory, 'io': io}


def _get(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _entry_kind(entry):
    kind = _get(entry, 'kind') or _get(entry, 'name') or _get(entry, 'role')
    if not isinstance(kind, str):
        return None
    return kind.lower() if kind.lower() in ('backend', 'postgres') else None


def _cpu(directory):
    quota = _field(os.path.join(directory, 'cpu.max'), lambda text: _parse_cpu_max(text))
    stat = _field(os.path.join(directory, 'cpu.stat'), lambda text: _key_values(text))
    if stat['available']:
        values = stat['value']
        stat = {'available': True, 'usage_usec': values.get('usage_usec'),
                'user_usec': values.get('user_usec'), 'system_usec': values.get('system_usec'),
                'nr_periods': values.get('nr_periods'), 'nr_throttled': values.get('nr_throttled'),
                'throttled_usec': values.get('throttled_usec')}
    return {'quota': quota, 'stats': stat}


def _parse_cpu_max(text):
    fields = text.split()
    if len(fields) != 2:
        raise ValueError
    period = _number(fields[1])
    if not period:
        raise ValueError
    return {'quota_usec': None if fields[0] == 'max' else _number(fields[0]), 'period_usec': period}


def _memory(directory):
    fields = ('memory.current', 'memory.max', 'memory.swap.current', 'memory.swap.max')
    names = ('current_bytes', 'max_bytes', 'swap_current_bytes', 'swap_max_bytes')
    result = {}
    for field, name in zip(fields, names):
        result[name] = _field(os.path.join(directory, field), lambda text: None if text.strip() == 'max' else _number(text.strip()))
    events = _field(os.path.join(directory, 'memory.events'), _key_values)
    if events['available']:
        events = {'available': True, **{key: events['value'].get(key) for key in ('oom', 'oom_kill', 'high', 'max')}}
    result['events'] = events
    return result


def _io(directory):
    def parse(text):
        rows = []
        for line in text.splitlines()[:MAX_IO_DEVICES]:
            pieces = line.split()
            if len(pieces) < 2 or not re.fullmatch(r'[0-9]+:[0-9]+', pieces[0]):
                raise ValueError
            row = {'device': pieces[0]}
            seen = set()
            for pair in pieces[1:]:
                key, value = pair.split('=', 1)
                if key not in ('rbytes', 'wbytes', 'rios', 'wios', 'dbytes', 'dios') or key in seen:
                    raise ValueError
                seen.add(key)
                number = _number(value)
                if key in ('rbytes', 'wbytes', 'rios', 'wios'):
                    row[{'rbytes': 'read_bytes', 'wbytes': 'write_bytes',
                         'rios': 'read_ios', 'wios': 'write_ios'}[key]] = number
            rows.append(row)
        return rows
    return _field(os.path.join(directory, 'io.stat'), parse)


def _host(proc_root, sys_root):
    cpu = _field(os.path.join(proc_root, 'stat'), _host_cpu)
    memory = _field(os.path.join(proc_root, 'meminfo'), _meminfo)
    vmstat = _field(os.path.join(proc_root, 'vmstat'), lambda text: _select_stats(text, ('pswpin', 'pswpout')))
    disk = _field(os.path.join(proc_root, 'diskstats'), lambda text: _diskstats(text, sys_root))
    return {'cpu': cpu, 'memory': memory, 'vmstat': vmstat, 'diskstats': disk}


def _host_cpu(text):
    fields = text.splitlines()[0].split()
    if fields[0] != 'cpu' or len(fields) < 6:
        raise ValueError
    vals = [_number(item) for item in fields[1:]]
    names = ('user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq', 'steal', 'guest', 'guest_nice')
    hz = os.sysconf('SC_CLK_TCK')
    if type(hz) is not int or hz <= 0:
        raise ValueError
    return {f'{name}_seconds': vals[index] / hz if index < len(vals) else None
            for index, name in enumerate(names)}


def _meminfo(text):
    result = {}
    wanted = {'MemTotal':'mem_total_bytes', 'MemFree':'mem_free_bytes', 'MemAvailable':'mem_available_bytes',
              'SwapTotal':'swap_total_bytes', 'SwapFree':'swap_free_bytes'}
    for line in text.splitlines():
        key, sep, rest = line.partition(':')
        if key in wanted:
            parts = rest.split()
            if len(parts) != 2 or parts[1] != 'kB':
                raise ValueError
            result[wanted[key]] = _number(parts[0]) * 1024
    if len(result) != len(wanted):
        raise ValueError
    return result


def _select_stats(text, keys):
    vals = _key_values(text)
    return {key: vals[key] for key in keys}


def _diskstats(text, sys_root):
    devices = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 14:
            raise ValueError
        major, minor = _number(fields[0]), _number(fields[1])
        check = os.path.join(sys_root, 'dev', 'block', f'{major}:{minor}')
        if not os.path.exists(check):
            continue
        if os.path.exists(os.path.join(check, 'partition')):
            continue
        name = fields[2]
        if not re.fullmatch(r'[A-Za-z0-9_.!-]{1,64}', name):
            continue
        if name.startswith(('loop', 'ram')):
            continue
        counters = [_number(item) for item in fields[3:14]]
        devices.append({'device': name, 'reads_completed': counters[0], 'reads_merged': counters[1],
                        'sectors_read': counters[2], 'read_time_ms': counters[3],
                        'writes_completed': counters[4], 'writes_merged': counters[5],
                        'sectors_written': counters[6], 'write_time_ms': counters[7],
                        'inflight': counters[8], 'io_time_ms': counters[9],
                        'weighted_io_time_ms': counters[10]})
        if len(devices) >= MAX_DEVICES:
            break
    return devices


def read_linux_resources(registry, containers, *, proc_root='/proc', cgroup_root='/sys/fs/cgroup', sys_root='/sys', timeout=None):
    """Return bounded sanitized counters; no raw paths or container configuration escape."""
    deadline = None
    if timeout is not None:
        timeout = float(timeout)
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError('linux_resource_timeout_invalid')
        deadline = time.monotonic() + min(2.0, timeout)
    sample_start = {'utc': datetime.now(timezone.utc).isoformat(),
                    'monotonic_seconds': time.monotonic()}
    items = []
    if hasattr(containers, 'get'):
        selected = ((role, containers.get(role)) for role in ('backend', 'postgres'))
    else:
        selected = ((_entry_kind(entry), entry) for entry in containers)
    for role, entry in selected:
        if role not in ('backend', 'postgres') or entry is None:
            continue
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        if deadline is not None and remaining <= 0:
            result = {'kind': role, **_unavailable('deadline_exceeded')}
        else:
            result = _container(registry, entry, role, proc_root, cgroup_root, remaining)
        items.append(result)
        if len(items) >= MAX_CONTAINERS:
            break
    host = _host(proc_root, sys_root)
    sample_end = {'utc': datetime.now(timezone.utc).isoformat(),
                  'monotonic_seconds': time.monotonic()}
    return {'schema_version': 1, 'sample_start': sample_start, 'sample_end': sample_end,
            'containers': items, 'host': host,
            'limitations': {'diskstats_scope': 'host_device_not_container_latency',
                            'cgroup_io_scope': 'attribution_only', 'memory_scope': 'sampled_not_peak',
                            'proc_stat_guest_counters': 'guest_included_in_user_nice_do_not_add_twice'}}
