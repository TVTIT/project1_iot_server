"""Bounded disposable fixture orchestration for the P8 native WSS session probe."""
import json
import importlib.util
import os
from pathlib import Path
try:
    from scripts.paper_p8_resources import disk_guard_config
except ModuleNotFoundError:
    _resources_path = Path(__file__).resolve().parents[1] / 'paper_p8_resources.py'
    _resources_spec = importlib.util.spec_from_file_location('paper_p8_resources', _resources_path)
    if _resources_spec is None or _resources_spec.loader is None:
        raise ImportError('paper_p8_resources_unavailable') from None
    _resources = importlib.util.module_from_spec(_resources_spec)
    _resources_spec.loader.exec_module(_resources)
    disk_guard_config = _resources.disk_guard_config
try:
    from scripts.paper_p8_bundle import e1_timing_budget
except ImportError:
    try:
        from paper_p8_bundle import e1_timing_budget
    except ImportError:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from paper_p8_bundle import e1_timing_budget
import signal
import subprocess
import time
import hashlib
import resource
import re
import urllib.parse
import threading
from datetime import datetime, timezone


SESSION_CAP = 64
RECHECK_SECONDS = 5
RECHECK_BUDGET_SECONDS = 2
HEARTBEAT_SECONDS = 20
PILOT_LIMIT_SECONDS = 175
MAX_EVENTS = 10000
MAX_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
COLLECTOR_INTERVAL_SECONDS = 0.5
COLLECTOR_MAX_SNAPSHOTS = 32
E1_OFFICIAL_VIEWER_RUNGS = (32, 64, 128, 512, 1000, 2000, 5000, 10000)


def build_e1_mapping(*, targets, viewers=64, users=2, density='sparse', seed=1,
                     filters_per_viewer=None, official_opt_in=False, user_gateways=None):
    """Deterministic E1 topology/mapping plan; filters are deduped by composite target."""
    if viewers not in E1_OFFICIAL_VIEWER_RUNGS or users < 1 or users > viewers:
        raise ValueError('e1_topology_rejected')
    if viewers > 64 and not official_opt_in:
        raise ValueError('official_profile_requires_opt_in')
    if density not in ('sparse', 'dense', 'overlap') or not isinstance(seed, int) or seed < 0:
        raise ValueError('e1_density_or_seed_rejected')
    normalized = []
    for target in targets:
        if not isinstance(target, dict) or not all(isinstance(target.get(k), str) and target[k] for k in ('gateway_id','sensor_id')):
            raise ValueError('e1_target_rejected')
        key = (target['gateway_id'], target['sensor_id'])
        if key in {(x['gateway_id'],x['sensor_id']) for x in normalized}:
            raise ValueError('e1_target_duplicate')
        normalized.append({'gateway_id': key[0], 'sensor_id': key[1]})
    gateways = sorted({t['gateway_id'] for t in normalized})
    if len(gateways) < 2 or any(sum(t['gateway_id'] == gw for t in normalized) < 2 for gw in gateways):
        raise ValueError('e1_requires_two_gateways_and_two_sensors_each')
    if user_gateways is None:
        if users != len(gateways):
            raise ValueError('e1_user_gateway_mapping_required')
        user_gateways = {f'user-{i:03d}': gateway for i, gateway in enumerate(gateways)}
    if (not isinstance(user_gateways, dict) or len(user_gateways) != users
            or set(user_gateways.values()) - set(gateways)):
        raise ValueError('e1_user_gateway_mapping_rejected')
    if not isinstance(filters_per_viewer, (list, tuple)) or len(filters_per_viewer) != viewers:
        # Sparse and dense differ in concrete, authorized Sensor subscriptions.
        filters_per_viewer = [1 if density == 'sparse' else 2] * viewers
    if any(type(n) is not int or n < 1 or n > 16 for n in filters_per_viewer):
        raise ValueError('e1_filter_cap_rejected')
    mapping = []
    recipients = {f"{t['gateway_id']}\0{t['sensor_id']}": [] for t in normalized}
    for i in range(viewers):
        count = filters_per_viewer[i]
        user = f'user-{i % users:03d}'
        own_targets = [t for t in normalized if t['gateway_id'] == user_gateways[user]]
        if not own_targets or count > len(own_targets):
            raise ValueError('e1_user_gateway_target_mismatch')
        filters = [own_targets[(i // users + j) % len(own_targets)] for j in range(count)]
        # overlap adds a Gateway wildcard and one concrete Sensor filter.
        if density == 'overlap':
            t = filters[0]
            filters = [{'gateway_id':t['gateway_id'],'sensor_id':''}, t]
        dedup = {(f['gateway_id'], f.get('sensor_id','')):f for f in filters}
        filters = list(dedup.values())
        row = {'viewer_id':f'viewer-{i:05d}', 'user_id':user, 'filters':filters}
        mapping.append(row)
        for target in normalized:
            if any(f['gateway_id']==target['gateway_id'] and (not f.get('sensor_id') or f['sensor_id']==target['sensor_id']) for f in filters):
                recipients[f"{target['gateway_id']}\0{target['sensor_id']}"].append(row['viewer_id'])
    if any(not ids for ids in recipients.values()):
        raise ValueError('e1_target_without_recipients')
    return {'seed':seed,'targets':normalized,'viewers':mapping,
        'expected_recipients_by_target':recipients,
        'counts':{'sockets':viewers,'users':users,'filters':sum(len(v['filters']) for v in mapping),
                  'targets':len(normalized),'receiving_viewers':len({v for ids in recipients.values() for v in ids})},
        'official_presets':list(E1_OFFICIAL_VIEWER_RUNGS),'executed':False}


def e1_expected_deliveries(mapping, unique_messages_by_target):
    if set(unique_messages_by_target) != set(mapping['expected_recipients_by_target']):
        raise ValueError('e1_message_target_set_mismatch')
    return {target: count * len(mapping['expected_recipients_by_target'][target])
            for target, count in unique_messages_by_target.items()}


def build_e1_execution_plan(*, output_dir, viewer_manifest, telemetry_manifest, seed,
                            mode='remote', official_opt_in=False,
                            viewer_output='viewer-output', telemetry_output='telemetry-output'):
    """Write two real Go role manifests/run-spec from provisioned, safe manifest inputs."""
    root = Path(output_dir)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    manifests=[]
    loaded={}
    for name, path, role in (('viewer',viewer_manifest,'viewer'),('telemetry',telemetry_manifest,'telemetry')):
        source=Path(path)
        if source.is_symlink() or not source.is_file() or source.stat().st_mode & 0o077 or source.stat().st_size > MAX_MANIFEST_BYTES:
            raise ValueError('e1_manifest_input_rejected')
        raw=json.loads(source.read_text(encoding='utf-8'))
        if (not isinstance(raw,dict) or raw.get('roles') != [role]
                or raw.get('profile') not in ('pilot', 'official')
                or raw.get('mode') not in ('local', 'remote') or raw.get('mode') != mode):
            raise ValueError('e1_role_manifest_mismatch')
        if raw.get('profile') == 'official' and not official_opt_in:
            raise ValueError('official_profile_requires_opt_in')
        if raw.get('seed') != seed:
            raise ValueError('e1_shared_seed_mismatch')
        tls=raw.get('tls'); limits=raw.get('limits')
        if (not isinstance(tls,dict) or not tls.get('https_endpoint') or not tls.get('https_ca_file')
                or not isinstance(raw.get('identity_files'),dict) or not raw['identity_files'].get(role)
                or not isinstance(limits,dict) or type(limits.get('workers')) is not int
                or type(limits.get('connections')) is not int or type(limits.get('pending_messages')) is not int
                or type(limits.get('pending_bytes')) is not int):
            raise ValueError('e1_provisioned_inputs_missing')
        loaded[name]=raw
        if name == 'viewer':
            drain=raw.get('phases',{}).get('drain_seconds')
            if type(drain) is not int or drain < 1:
                raise ValueError('e1_viewer_drain_window_required')
        dst=root/f'{name}.manifest.json'
        with dst.open('x',encoding='utf-8') as f: json.dump(raw,f,sort_keys=True,separators=(',',':'))
        dst.chmod(0o600); manifests.append((name,dst))
    viewer, telemetry=loaded['viewer'],loaded['telemetry']
    for key in ('https_endpoint','https_ca_file'):
        if viewer['tls'].get(key) != telemetry['tls'].get(key):
            raise ValueError('e1_security_profile_mismatch')
    # Worker pools are role-specific (viewer socket fanout vs telemetry
    # publishers); only the bounded queue policy must match across roles.
    for key in ('pending_messages','pending_bytes'):
        if viewer['limits'].get(key) != telemetry['limits'].get(key):
            raise ValueError('e1_worker_queue_policy_mismatch')
    if viewer.get('targets') != telemetry.get('targets'):
        raise ValueError('e1_target_topology_mismatch')
    # The viewer must have time to establish every selected socket and complete
    # its subscription handshake before telemetry is allowed to start. Keep this finite
    # and derived from the manifest's configured timing budget.
    viewer_count = viewer.get('topology', {}).get('viewers', viewer.get('limits', {}).get('connections', 64))
    timing = e1_timing_budget(
        viewer_count,
        phases=viewer.get('phases', {}),
        declared_ramp=viewer.get('traffic', {}).get('ramp_seconds'),
    )
    readiness_budget = timing['ready_timeout_seconds']
    spec={'paperload':[{'manifest':str(path),'output_dir':str(root/(out)),'role':role,
                        'ready_timeout_seconds': readiness_budget if role == 'viewer' else 90}
                       for (role,path),out in zip(manifests,(viewer_output,telemetry_output))],
          'stop_timeout_seconds':30,
          'disk_guard': disk_guard_config([viewer, telemetry])}
    run_spec=root/'e1-run-spec.json'
    with run_spec.open('x',encoding='utf-8') as f: json.dump(spec,f,sort_keys=True,indent=2)
    run_spec.chmod(0o600)
    return {'run_spec':str(run_spec),'roles':['viewer','telemetry'],'seed':seed,
            'requested_mode':mode,'generator_colocated':mode == 'local',
            'actual_remote_execution':False,
            'start_contract':'all selected viewer subscriptions must be acknowledged before telemetry startup; bounded viewer-ready budget',
            'viewer_readiness_budget_seconds': readiness_budget,
            'native_execution':'NOT RUN'}


class BoundedCollector:
    """Finite server scrape and B-local process-tree sampling for a short run."""
    def __init__(self, *, scrape, interval=COLLECTOR_INTERVAL_SECONDS,
                 max_snapshots=COLLECTOR_MAX_SNAPSHOTS, deadline_seconds=30,
                 baseline_metrics=None, baseline_server=None):
        if (not 0.1 <= interval <= 5 or type(max_snapshots) is not int or not 2 <= max_snapshots <= 20000
                or not 5 <= deadline_seconds <= 86400):
            raise ValueError('collector_bounds_rejected')
        self.scrape = scrape
        self.interval = interval
        self.max_snapshots = max_snapshots
        self.deadline_seconds = deadline_seconds
        self._started_monotonic = None
        self.metrics = []
        if baseline_metrics is not None:
            self.metrics.append({'phase': 'workload_start', 'snapshot': baseline_metrics})
        self.resources = []
        if baseline_server is not None:
            self.resources.append(baseline_server)
        self.errors = []
        self.generator_samples = []
        self._last_generator = None
        self._stop = threading.Event()
        self._thread = None

    def _sample(self, phase, process_pid):
        if (phase != 'terminal' and self._started_monotonic is not None and
                time.monotonic() - self._started_monotonic > self.deadline_seconds):
            if 'collector_deadline_exceeded' not in self.errors:
                self.errors.append('collector_deadline_exceeded')
            return
        stamp = datetime.now(timezone.utc).isoformat()
        metric_sample = None
        if self.scrape:
            try:
                snap = self.scrape()
                if not isinstance(snap, dict):
                    raise ValueError('metrics_shape_invalid')
                metric_sample = {'phase': phase, 'snapshot': snap, 'sampled_at': stamp,
                                 'monotonic_seconds': time.monotonic()}
            except Exception as exc:
                metric_sample = {'phase': phase, 'snapshot': None,
                                 'error': 'scrape_' + type(exc).__name__, 'sampled_at': stamp,
                                 'monotonic_seconds': time.monotonic()}
        if metric_sample is not None:
            self.metrics.append(metric_sample)
            self._bound_samples(self.metrics)
        proc = process_tree_resources(process_pid) if process_pid else _unavailable('generator_process_unavailable')
        if isinstance(proc, dict) and proc.get('available'):
            now = time.monotonic()
            cpu_percent = None
            if self._last_generator:
                previous_at, previous_proc = self._last_generator
                elapsed = now - previous_at
                if elapsed > 0:
                    prev_p = previous_proc.get('processes', {})
                    curr_p = proc.get('processes', {})
                    ticks_delta = 0
                    reset = set(prev_p) != set(curr_p)
                    for p_key, p_curr in curr_p.items():
                        if reset:
                            break
                        d = p_curr['ticks'] - prev_p[p_key]['ticks']
                        if d < 0:
                            reset = True
                            break
                        ticks_delta += d
                    if not reset:
                        host_cpus = max(1, os.cpu_count() or 1)
                        cpu_percent = (ticks_delta / os.sysconf('SC_CLK_TCK') / elapsed / host_cpus * 100)
                        self.generator_samples.append({
                            'phase': phase, 'available': True, 'cpu_percent': cpu_percent,
                            'delta_ticks': ticks_delta, 'elapsed': elapsed,
                            'rss_bytes': proc['rss_bytes'], 'process_count': proc['process_count']
                        })
                    else:
                        self.generator_samples.append({
                            'phase': phase, 'available': None,
                            'reason': ('generator_process_set_changed' if set(prev_p) != set(curr_p)
                                       else 'generator_counter_reset_negative_delta')
                        })
                else:
                    self.generator_samples.append({
                        'phase': phase, 'available': None,
                        'reason': 'generator_invalid_window_duration'
                    })
            else:
                self.generator_samples.append({
                    'phase': phase, 'available': None,
                    'reason': 'generator_cpu_baseline_unavailable'
                })
            self._last_generator = (now, proc)
            generator = {
                'services': {
                    'generator_B': {
                        'CPUPerc': f'{cpu_percent:.6f}%' if cpu_percent is not None else '0.0%',
                        'MemUsage': f"{proc['rss_bytes']}B / {proc['rss_bytes']}B",
                        'cpu_ticks': proc['cpu_ticks'], 'process_count': proc['process_count'],
                        'cpu_normalization': 'host_normalized_percent (ticks / CLK_TCK / elapsed / host_cpu_count * 100)'
                    }
                },
                'started_at': stamp, 'finished_at': stamp,
                'phase': phase + '_generator'
            }
            self.resources.append(generator)
        else:
            self.generator_samples.append({'phase': phase, 'available': None,
                'reason': proc.get('reason', 'generator_resources_unavailable')})
        self._bound_samples(self.generator_samples)

    def _bound_samples(self, samples):
        if len(samples) <= self.max_snapshots:
            return
        # Keep original baseline and the newest window, reserving a slot for terminal.
        if samples and samples[0].get('phase') in ('workload_start', 'baseline'):
            samples[:] = [samples[0]] + samples[-(self.max_snapshots - 1):]
        else:
            samples[:] = samples[-self.max_snapshots:]

    def start(self, process_pid):
        self._started_monotonic = time.monotonic()
        self._sample('workload_sample_0', process_pid)
        def loop():
            index = 0
            while not self._stop.wait(self.interval):
                index += 1
                self._sample(f'workload_sample_{index}', process_pid)
                if len(self.metrics) >= self.max_snapshots:
                    break
        self._thread = threading.Thread(target=loop, name='p8-bounded-collector', daemon=True)
        self._thread.start()

    def stop(self, process_pid):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=12)
            if self._thread.is_alive():
                self.errors.append('collector_join_timeout')
                return False
        if getattr(self, '_terminal_sampled', False):
            return True
        self._sample('terminal', process_pid)
        self._bound_samples(self.metrics)
        self._terminal_sampled = True
        return True


def process_tree_resources(root_pid, *, proc_root=Path('/proc')):
    """Read bounded owned-child RSS/CPU counters on the generator host only."""
    if type(root_pid) is not int or root_pid <= 0:
        return _unavailable('generator_process_unavailable')
    pending, seen = [root_pid], set()
    rss, cpu, count = 0, 0, 0
    processes = {}
    try:
        while pending and len(seen) < 32:
            pid = pending.pop()
            if pid in seen:
                continue
            seen.add(pid)
            base = Path(proc_root) / str(pid)
            raw = (base / 'stat').read_text()
            tail = raw[raw.rfind(')') + 2:].split()
            ppid = int(tail[1])
            ticks = int(tail[11]) + int(tail[12])
            starttime = int(tail[19])
            pages = int((base / 'statm').read_text().split()[1])
            proc_rss = pages * os.sysconf('SC_PAGE_SIZE')
            processes[(pid, starttime)] = {'ticks': ticks, 'rss_bytes': proc_rss}
            rss += proc_rss
            cpu += ticks
            count += 1
            children = (base / 'task' / str(pid) / 'children').read_text().split()
            pending.extend(int(child) for child in children if child.isdigit())
        if pending:
            return _unavailable('generator_process_tree_cap_reached')
        return {'available': True, 'rss_bytes': rss, 'cpu_ticks': cpu,
                'process_count': count, 'processes': processes}
    except (OSError, ValueError, IndexError):
        return _unavailable('generator_process_read_failed')


def metrics_window(before, after, group, name, histogram_helper):
    """Reject scrape gaps, process changes and resets before deriving a window."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return {'availability': 'unavailable', 'reason': 'scrape_missing'}
    if not before.get('process_instance') or not after.get('process_instance'):
        return {'availability': 'unavailable', 'reason': 'process_instance_missing'}
    if before.get('process_instance') != after.get('process_instance'):
        return {'availability': 'unavailable', 'reason': 'process_instance_changed'}
    result = histogram_helper(before, after, group, name)
    if result.get('availability') != 'available':
        result['reason'] = 'counter_reset_or_histogram_unavailable'
    return result


def sensor_scoped_delta(before, after):
    """Compute exact deltas only for the same composite Gateway/Sensor keys.

    Rejects missing fields, negative deltas, scope mismatches, and duplicate keys.
    """
    if not isinstance(before, list) or not isinstance(after, list):
        return {'available': None, 'reason': 'invalid_scope_input'}
    old, new = {}, {}
    for collection, target in ((before, old), (after, new)):
        for item in collection:
            if not isinstance(item, dict):
                return {'available': None, 'reason': 'invalid_scope_item'}
            gw, sensor = item.get('gateway_id'), item.get('sensor_id')
            if not gw or not sensor:
                return {'available': None, 'reason': 'composite_sensor_key_missing'}
            key = (gw, sensor)
            if key in target:
                return {'available': None, 'reason': 'duplicate_composite_sensor_key'}
            counts = item.get('counts')
            if not isinstance(counts, dict):
                return {'available': None, 'reason': 'missing_counts_dict'}
            target[key] = counts
    if not old or old.keys() != new.keys():
        return {'available': None, 'reason': 'composite_sensor_scope_mismatch'}
    required_fields = ('ledger', 'raw', 'temporal')
    deltas = {}
    for key, old_counts in old.items():
        new_counts = new[key]
        item_delta = {}
        for field in required_fields:
            if field not in old_counts or field not in new_counts:
                return {'available': None, 'reason': f'missing_counter_field_{field}'}
            old_val, new_val = old_counts[field], new_counts[field]
            if type(old_val) is not int or type(new_val) is not int:
                return {'available': None, 'reason': f'counter_not_int_{field}'}
            if new_val < old_val:
                return {'available': None, 'reason': f'counter_reset_{field}'}
            item_delta[field] = new_val - old_val
        deltas[f'{key[0]}/{key[1]}'] = item_delta
    return {'available': deltas, 'reason': None}


def calculate_ingress_delta(before, after):
    """Calculate same-process monotonic deltas for backend ingress metrics."""
    if not isinstance(before, dict) or not isinstance(after, dict):
        return {'available': None, 'reason': 'scrape_missing'}
    p_before, p_after = before.get('process_instance'), after.get('process_instance')
    if not p_before or not p_after:
        return {'available': None, 'reason': 'process_instance_missing'}
    if p_before != p_after:
        return {'available': None, 'reason': 'process_instance_changed'}
    old_ingress = before.get('ingress')
    new_ingress = after.get('ingress')
    if not isinstance(old_ingress, dict) or not isinstance(new_ingress, dict):
        return {'available': None, 'reason': 'ingress_metrics_missing'}
    delta = {}
    for field in (
        'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
        'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
        'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
        'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
        'ShaperStateCapacity', 'Processed', 'ValidationRejected',
        'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
        'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
        'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
    ):
        if field not in old_ingress or field not in new_ingress:
            return {'available': None, 'reason': f'field_missing_{field}'}
        o, n = old_ingress[field], new_ingress[field]
        if type(o) not in (int, float) or type(n) not in (int, float):
            return {'available': None, 'reason': f'field_not_number_{field}'}
        if n < o:
            return {'available': None, 'reason': f'counter_reset_{field}'}
        delta[field] = n - o
    return {'available': True, 'delta': delta, 'reason': None}


def verify_pilot_raw(smoke, registry, pg, spec, *, message_ids=None):
    """Exact small-pilot ledger/raw identity, sequence, sample-index and value check."""
    if message_ids is not None:
        if (not isinstance(message_ids, (set, list, tuple)) or not message_ids or len(message_ids) > 20
                or any(not isinstance(value, str) or not re.fullmatch(
                    r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', value)
                    for value in message_ids)):
            raise ValueError('pilot_raw_message_ids_rejected')
        ids = list(dict.fromkeys(value.lower() for value in message_ids))
        if not ids or len(ids) > 20:
            raise ValueError('pilot_raw_message_ids_rejected')
    else:
        ids = None
    id_filter = (' AND p.message_id IN (' + ','.join(f":'message_{chr(97 + i)}'::uuid" for i in range(len(ids))) + ')') if ids is not None else ''
    params = {'gw': spec.gateway_id, 'sensor': spec.sensor_id}
    if ids is not None:
        params.update({f'message_{chr(97 + i)}': value for i, value in enumerate(ids)})
    rows = json.loads(smoke.db_psql(registry, pg, """
SELECT coalesce(json_agg(json_build_object('message_id',p.message_id::text,
 'first_sequence',p.first_sequence::text,'last_sequence',p.last_sequence::text,
 'sample_count',p.sample_count) ORDER BY p.first_sequence),'[]'::json)
FROM processed_messages p WHERE p.gateway_id=:'gw' AND p.sensor_id=:'sensor'""" + id_filter + ';', params).strip())
    mismatch = smoke.db_psql(registry, pg, """
SELECT count(*) FROM processed_messages p WHERE p.gateway_id=:'gw' AND p.sensor_id=:'sensor'
""" + id_filter + """
 AND (p.sample_count <> (SELECT count(*) FROM telemetry t WHERE t.gateway_id=p.gateway_id
       AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id)
   OR p.first_sequence <> (SELECT min(t.sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id
      AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id)
   OR p.last_sequence <> (SELECT max(t.sequence_num) FROM telemetry t WHERE t.gateway_id=p.gateway_id
      AND t.sensor_id=p.sensor_id AND t.message_id=p.message_id)
   OR EXISTS (SELECT 1 FROM telemetry t WHERE t.gateway_id=p.gateway_id AND t.sensor_id=p.sensor_id
      AND t.message_id=p.message_id AND (t.sample_index <> t.sequence_num-p.first_sequence
       OR t.boot_id <> p.boot_id OR abs(t.value - (20 + (CASE
        WHEN mod(t.sequence_num,2000)>1000 THEN 2000-mod(t.sequence_num,2000)
        ELSE mod(t.sequence_num,2000) END)::double precision/100.0)) > 1e-9)));""",
        params).strip()
    return {'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id,
        'ledger_count': len(rows), 'raw_identity_value_mismatches': int(mismatch),
        'identity_ranges': [{'message_id': row['message_id'],
            'first_sequence': row['first_sequence'], 'last_sequence': row['last_sequence'],
            'sample_count': row['sample_count']} for row in rows],
        'verified_exact': message_ids is None and bool(rows) and int(mismatch) == 0,
        'verified_sample': message_ids is not None and bool(rows) and len(rows) == len(set(message_ids))
            and int(mismatch) == 0}


def _unavailable(reason):
    return {'available': None, 'reason': reason}


def _admin_metrics(smoke, endpoint, token, *, timeout=2):
    status, _, payload = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics',
                                             token=token, timeout=timeout)
    if status != 200 or not isinstance(payload, dict):
        raise ValueError('admin_metrics_unavailable')
    # The shared HTTP helper caps responses at 64 KiB; keep the P8 cap explicit too.
    if len(json.dumps(payload, separators=(',', ':')).encode()) > 65536:
        raise ValueError('admin_metrics_response_too_large')
    return payload


def read_generator_preflight(*, proc_root=Path('/proc'), resource_module=resource):
    """Read current process limits and local ephemeral range; never changes either."""
    try:
        soft, hard = resource_module.getrlimit(resource_module.RLIMIT_NOFILE)
        limits = {'soft': None if soft == resource_module.RLIM_INFINITY else int(soft),
                  'hard': None if hard == resource_module.RLIM_INFINITY else int(hard)}
    except (OSError, ValueError, AttributeError):
        limits = None
    try:
        raw = (Path(proc_root) / 'sys/net/ipv4/ip_local_port_range').read_text().strip()
        match = re.fullmatch(r'(\d+)\s+(\d+)', raw)
        port_range = tuple(map(int, match.groups())) if match else None
    except (OSError, ValueError):
        port_range = None
    try:
        system_max = int((Path(proc_root) / 'sys/fs/file-max').read_text().strip())
        file_nr = [int(part) for part in (Path(proc_root) / 'sys/fs/file-nr').read_text().split()]
        os_limits = {'file_max': system_max, 'file_nr': file_nr if len(file_nr) == 3 else None}
        if os_limits['file_nr'] is None:
            os_limits = None
    except (OSError, ValueError):
        os_limits = None
    return resource_estimate(limits, ephemeral_range=port_range,
                             pilot_viewers=64, proposed_rungs=(128, 256, 10000),
                             os_file_limits=os_limits)


def resource_estimate(generator_limits, *, ephemeral_range, pilot_viewers, proposed_rungs,
                      os_file_limits=None):
    """Finite conservative estimates: 64 process FDs plus two per WS client."""
    rungs = tuple(proposed_rungs)
    if (not isinstance(pilot_viewers, int) or pilot_viewers < 1 or pilot_viewers > 10000
            or any(not isinstance(rung, int) or rung < 1 or rung > 10000 for rung in rungs)):
        raise ValueError('finite_budget_rejected')
    required = {'pilot64': 64 + 2 * pilot_viewers,
                'proposed_rungs': {str(rung): 64 + 2 * rung for rung in rungs}}
    if (not isinstance(generator_limits, dict) or not isinstance(generator_limits.get('soft'), int)
            or not isinstance(generator_limits.get('hard'), int)):
        generator = {'available': None, 'reason': 'process_rlimit_unavailable',
                     'required_fds': required}
    else:
        soft, hard = generator_limits['soft'], generator_limits['hard']
        generator = {'available': {'soft': soft, 'hard': hard}, 'required_fds': required,
                     'pilot64_fits_soft': soft >= required['pilot64']}
    system = (os_file_limits if isinstance(os_file_limits, dict)
              and isinstance(os_file_limits.get('file_max'), int)
              and isinstance(os_file_limits.get('file_nr'), list)
              else _unavailable('system_file_limits_unavailable'))
    if (isinstance(ephemeral_range, (tuple, list)) and len(ephemeral_range) == 2
            and all(isinstance(v, int) for v in ephemeral_range) and 0 <= ephemeral_range[0] <= ephemeral_range[1] <= 65535):
        ports = {'range': list(ephemeral_range), 'available': ephemeral_range[1] - ephemeral_range[0] + 1}
    else:
        ports = {'range': None, 'available': None, 'reason': 'ephemeral_port_range_unavailable'}
    return {'generator': generator, 'generator_os_fd_limits': system, 'ephemeral_ports': ports,
            'estimate_note': 'finite planning estimate; 64 process FDs + 2 per viewer; not a capacity claim'}


def fixture_process_preflight(*, worker_processes, worker_connections, process_fd_limits,
                              backend_fd_availability=None, broker_fd_availability=None,
                              viewers=64, proxied_websockets=True, role_connections=None,
                              reserve_connections=32):
    """Validate/report owned fixture process limits; Nginx WS uses downstream+upstream slots."""
    if not isinstance(viewers, int) or viewers < 1 or viewers > 10000:
        raise ValueError('finite_budget_rejected')
    if type(reserve_connections) is not int or not 0 <= reserve_connections <= 100000:
        raise ValueError('finite_budget_rejected')
    roles = role_connections or {'viewer': viewers}
    if (not isinstance(roles, dict) or not roles or any(type(count) is not int or count < 0
            or count > 10000 for count in roles.values())):
        raise ValueError('finite_budget_rejected')
    viewer_connections = roles.get('viewer', viewers)
    other_connections = sum(count for role, count in roles.items() if role != 'viewer')
    required_slots = viewer_connections * (2 if proxied_websockets else 1) + other_connections + reserve_connections
    workers = worker_processes if type(worker_processes) is int and worker_processes > 0 else 1
    per_worker_required = (required_slots + workers - 1) // workers
    nginx = {'worker_processes': worker_processes, 'worker_connections': worker_connections,
             'required_connections_total': required_slots,
             'required_connections_per_worker': per_worker_required,
             'role_connections': roles, 'reserve_connections': reserve_connections,
             'fits': isinstance(worker_connections, int) and worker_connections >= per_worker_required}
    if worker_processes is None:
        nginx['worker_processes_reason'] = 'nginx_worker_processes_readback_unavailable'
    if worker_connections is None:
        nginx['worker_connections_reason'] = 'nginx_worker_connections_readback_unavailable'
    def availability(value, reason):
        known_limit = (isinstance(value, dict) and
                       (type(value.get('soft')) is int or
                        value.get('soft') is None and type(value.get('available')) is int))
        return value if known_limit else _unavailable(reason)
    return {'nginx': nginx,
            'process_fd_limits': process_fd_limits if isinstance(process_fd_limits, dict) else _unavailable('fixture_process_fd_limits_unavailable'),
            'backend_fd_availability': availability(backend_fd_availability, 'backend_fd_readback_unavailable'),
             'broker_fd_availability': availability(broker_fd_availability, 'broker_fd_readback_unavailable')}


LIFECYCLE_OVERHEAD_SECONDS = {
    'preparation_readiness': 300,
    'bundle_transfer': 120,
    'import_verification': 240,
    'cleanup': 240,
}


def lifecycle_budget(*, workload_seconds, session_deadline_seconds, overhead=None):
    """Require explicit finite session headroom around the measured workload."""
    values = (workload_seconds, session_deadline_seconds)
    if any(type(value) is not int or value < 0 for value in values):
        raise ValueError('lifecycle_budget_invalid')
    components = overhead or LIFECYCLE_OVERHEAD_SECONDS
    if not isinstance(components, dict) or any(type(value) is not int or value < 0
                                               for value in components.values()):
        raise ValueError('lifecycle_budget_invalid')
    required = workload_seconds + sum(components.values())
    if session_deadline_seconds < required:
        raise ValueError(f'lifecycle_deadline_insufficient: required {required} seconds')
    return {'workload_seconds': workload_seconds, 'overhead_seconds': dict(components),
            'required_session_seconds': required, 'session_deadline_seconds': session_deadline_seconds,
            'measured_throughput_excludes_overhead': True}


def parse_process_limits(text):
    for line in str(text).splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[:3] == ['Max', 'open', 'files']:
            def value(raw):
                return None if raw == 'unlimited' else int(raw)
            return {'soft': value(fields[3]), 'hard': value(fields[4]) if len(fields) > 4 else None}
    return None


def read_owned_fixture_resources(exec_owned_service, *, viewers=64, role_connections=None):
    """Read only the named owned services; failures are explicit null/reason records."""
    def read(service, command):
        try:
            result = exec_owned_service(service, command)
            return result if isinstance(result, str) else None
        except (OSError, ValueError, RuntimeError):
            return None
    nginx_text = read('nginx', ['nginx', '-T'])
    def directive(name):
        if nginx_text is None:
            return None
        matches = re.findall(r'^\s*' + name + r'\s+([^;\s]+)\s*;', nginx_text, re.MULTILINE)
        if not matches:
            return None
        value = matches[-1]
        return int(value) if value.isdigit() else value
    nginx_limits = parse_process_limits(read('nginx', ['cat', '/proc/1/limits']) or '')
    backend_limits = parse_process_limits(read('backend', ['cat', '/proc/1/limits']) or '')
    broker_limits = parse_process_limits(read('mosquitto', ['cat', '/proc/1/limits']) or '')
    def availability(limits, service):
        if not isinstance(limits, dict):
            return _unavailable(service + '_fd_readback_unavailable')
        raw_open = read(service, ['sh', '-c', 'set -- /proc/1/fd/*; echo $#'])
        try:
            opened = int(raw_open.strip()) if raw_open is not None else 0
        except ValueError:
            opened = 0
        if opened < 3:
            return {'available': None, 'reason': service + '_open_fd_readback_unavailable',
                    'soft': limits['soft'], 'hard': limits.get('hard')}
        available = (2**63 - 1) if limits.get('soft') is None else max(0, limits['soft'] - opened)
        return {'available': available, 'soft': limits['soft'],
                'hard': limits.get('hard'), 'open': opened}
    result = fixture_process_preflight(worker_processes=directive('worker_processes'),
        worker_connections=directive('worker_connections'), process_fd_limits=nginx_limits,
        backend_fd_availability=availability(backend_limits, 'backend'),
        broker_fd_availability=availability(broker_limits, 'mosquitto'),
        viewers=viewers, proxied_websockets=True, role_connections=role_connections)
    # Only inspect direct children of the owned Nginx master, never host processes.
    worker_text = read('nginx', ['sh', '-c', r'''
set -eu
count=0
for pid in $(cat /proc/1/task/1/children); do
    count=$((count + 1))
    [ "$count" -le 64 ] || exit 1
    case "$(tr '\000' ' ' < /proc/"$pid"/cmdline)" in
        'nginx: worker process'*)
            set -- /proc/"$pid"/fd/*
            [ "$#" -ge 3 ] || exit 1
            printf '%s %s ' "$pid" "$#"
            awk '$1 == "Max" && $2 == "open" && $3 == "files" { print $4, $5; found=1 }
                 END { if (!found) exit 1 }' /proc/"$pid"/limits
            ;;
    esac
done
'''])
    workers = []
    try:
        lines = worker_text.splitlines() if worker_text is not None else []
        if not 1 <= len(lines) <= 32:
            raise ValueError('worker_count')
        for line in lines:
            pid, opened, soft, hard = map(int, line.split())
            if pid <= 1 or opened < 3 or soft < opened or hard < soft:
                raise ValueError('worker_limits')
            workers.append({'pid': pid, 'open': opened, 'soft': soft, 'hard': hard,
                            'available': soft - opened})
        if len({row['pid'] for row in workers}) != len(workers):
            raise ValueError('worker_duplicates')
    except (ValueError, TypeError):
        result['nginx_workers'] = None
        result['nginx_workers_reason'] = 'nginx_worker_fd_readback_unavailable'
    else:
        result['nginx_workers'] = workers
    return result


def validate_mqtt_mapping(mapping, *, owned_service):
    """Optional fixture mapping contract; only loopback host to owned MQTT TLS 8883."""
    if mapping is None:
        return None
    if (not isinstance(mapping, dict) or mapping.get('service') != owned_service
            or owned_service != 'mosquitto' or mapping.get('host_ip') not in ('127.0.0.1', '::1')
            or mapping.get('target') != 8883 or not isinstance(mapping.get('published'), int)
            or not 1024 <= mapping['published'] <= 65535):
        raise ValueError('mqtt_mapping_rejected')
    return mapping['published']


def validate_dynamic_mqtt_mapping(mapping, *, owned_service):
    """Validate Compose's loopback-only dynamic 8883 publish before readback."""
    if (not isinstance(mapping, dict) or mapping.get('service') != owned_service
            or owned_service != 'mosquitto' or mapping.get('host_ip') != '127.0.0.1'
            or mapping.get('published') not in (0, '0') or mapping.get('target') != 8883):
        raise ValueError('mqtt_mapping_rejected')


def validate_mqtt_readback(mapping, *, owned_service):
    validate_mqtt_mapping(mapping, owned_service=owned_service)
    if mapping.get('host_ip') != '127.0.0.1':
        raise ValueError('mqtt_mapping_rejected')
    return mapping['published']


def assemble_e2_report(
    *,
    success,
    source_revision,
    dirty_worktree,
    schema,
    quota,
    image_ids,
    paperload_source_sha256,
    paperload_sha256,
    manifest_sha256,
    workload_path,
    effective_flags,
    endpoints,
    counters,
    database_delta,
    database_by_gateway,
    scoped_delta,
    pilot_raw_verification,
    manifest,
    collector_metrics,
    collector_resources,
    collector_errors,
    ingress_delta,
    histogram_windows,
):
    """Assemble complete P8 E2 run report without self-referential unbound variables."""
    ingress_d = ingress_delta.get('delta', {}) if ingress_delta.get('available') else None
    accounting = {
        'generated_unique': counters.get('generated_unique'),
        'scheduled': counters.get('scheduled_opportunities'),
        'actual_unique_offered': counters.get('offered_unique'),
        'publish_attempts': counters.get('publish_attempts'),
        'retries': counters.get('retries'),
        'broker_pubacks': counters.get('broker_pubacks'),
        'backend_observed': (
            {'available': ingress_d['Received'], 'note': 'application-level received batches, not wire traffic'}
            if ingress_d is not None else ingress_delta
        ),
        'admitted': (
            {'available': ingress_d['Accepted']}
            if ingress_d is not None else ingress_delta
        ),
        'rejected': (
            {
                'available': sum(ingress_d[k] for k in (
                    'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                    'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                    'ByteBudgetFull', 'ShaperRateLimited'
                )),
                'breakdown': {k: ingress_d[k] for k in (
                    'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                    'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                    'ByteBudgetFull', 'ShaperRateLimited'
                )}
            }
            if ingress_d is not None else ingress_delta
        ),
        'new_commit': (
            {'available': ingress_d['Committed'], 'scoped_delta': scoped_delta}
            if ingress_d is not None else scoped_delta
        ),
        'duplicate': (
            {'available': ingress_d['DuplicateCommitted']}
            if ingress_d is not None else ingress_delta
        ),
        'raw_rows': (
            {'available': ingress_d['SamplesNew'], 'scoped_delta': scoped_delta}
            if ingress_d is not None else scoped_delta
        ),
        'ack_published': (
            {'available': ingress_d['ACKPublished']}
            if ingress_d is not None else ingress_delta
        ),
        'valid_ack_received': counters.get('application_acks'),
        'outbox_deleted': {'available': None, 'reason': 'not_applicable_synthetic_workload'},
    }

    metadata = {
        'source_sha256': paperload_source_sha256,
        'binary_sha256': paperload_sha256,
        'image_ids': image_ids,
        'manifest_sha256': manifest_sha256,
        'backend_quota': quota,
        'actual_schema': schema,
        'effective_flags': effective_flags,
        'caps': manifest['limits'],
        'tls': {'mode': 'HTTPS+MQTTS', 'verified': True},
        'topology': manifest['topology'],
        'principals': 2,
        'connections': 2,
        'subscriptions': 0,
        'filter_counts': 0,
        'payload_bytes_wire': None,
        'payload_bytes_wire_reason': 'not_measured',
        'sampling': {
            'proposed_window': {'rate_per_second': 20, 'measure_seconds': 1},
            'effective_window': {'rate_per_second': manifest['traffic']['rate_per_second'],
                                'measure_seconds': manifest['phases']['measure_seconds'],
                                'reason': 'adjusted_for_bounded_sampling_cadence'},
            'metrics_snapshots': len(collector_metrics),
        },
        'resource_roles': {
            'server': 'machine_A_owned_fixture_containers',
            'generator': 'machine_B_runner_process_tree',
        },
        'collector_errors': collector_errors,
        'cleanup': {
            'fixture_cleanup_verified': False,
            'synthetic_outbox_deleted': {'available': None, 'reason': 'not_applicable_synthetic_workload'}
        },
    }

    return {
        'schema_version': 1,
        'status': 'PASS' if success else 'FAIL',
        'experiment': 'E2',
        'profile': 'pilot',
        'arm': 'smoke',
        'generator_colocated': True,
        'native_executed': True,
        'source_revision': source_revision,
        'dirty_worktree': dirty_worktree,
        'schema_terminal': schema,
        'backend_quota': quota,
        'image_ids': image_ids,
        'paperload_source_sha256': paperload_source_sha256,
        'effective_flags': effective_flags,
        'endpoints': endpoints,
        'artifacts': {
            'manifest_sha256': manifest_sha256,
            'paperload_sha256': paperload_sha256,
            'paperload_result': str(workload_path),
        },
        'counters': counters,
        'database_delta': database_delta,
        'database_by_gateway': database_by_gateway,
        'cleanup_verified': False,
        'metadata': metadata,
        'phase_windows': {
            'configured': manifest['phases'],
            'collector_samples': [row['phase'] for row in collector_metrics],
            'actual_generator_cutoffs': {
                'available': None,
                'reason': 'engine_exports_cohort_counters_not_phase_timestamps'
            },
            'cohort_accounting': 'ACK cohort follows generated identity through drain; phase counters in workload.json'
        },
        'accounting': accounting,
        'cohort_counters': {
            key: counters.get(key) for key in (
                'warmup_scheduled', 'warmup_generated', 'warmup_application_acks',
                'measure_scheduled', 'measure_generated', 'measure_application_acks',
                'burst_scheduled', 'burst_generated', 'burst_application_acks'
            )
        },
        'sensor_scoped_delta': scoped_delta,
        'pilot_raw_identity_value_verification': pilot_raw_verification,
        'histogram_windows': histogram_windows,
        'remote_execution': 'NOT RUN',
        'other_E2_profiles': 'NOT RUN',
        'collectors': 'bounded_server_and_generator',
    }


def run_e2_telemetry100(smoke, fixture, endpoint, registry, containers, images, state, paperload_binary):
    """One local-only, 20-batch E2 smoke using the actual Go paperload binary."""
    import json
    import subprocess
    from pathlib import Path
    from datetime import datetime, timezone

    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, state))
    schema = smoke.db_psql(registry, pg,
        "SELECT version::text || ':' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;").strip()
    if schema != '24:telemetry_ledger_timestamp':
        raise ValueError('p8_schema_terminal_rejected')
    broker = registry.resolve(containers['mosquitto'])
    mappings = broker.get('NetworkSettings', {}).get('Ports', {}).get('8883/tcp') or []
    if len(mappings) != 1:
        raise ValueError('p8_mqtt_mapping_readback_rejected')
    mapping = {'service': 'mosquitto', 'host_ip': mappings[0].get('HostIp'),
               'target': 8883, 'published': int(mappings[0].get('HostPort', '0'))}
    validate_mqtt_readback(mapping, owned_service='mosquitto')
    host = '127.0.0.1'
    mqtt_endpoint = f'mqtts://{host}:{mapping["published"]}'
    ca_http = Path(endpoint.ca_file).read_bytes()
    ca_mqtt_path = fixture.private / 'certs/ca.crt'
    ca_mqtt = ca_mqtt_path.read_bytes()
    run_id = fixture.project + '-e2'
    targets = [{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id} for spec in specs]
    gateway_ids = {}
    for spec, password in zip(specs, passwords):
        gateway_ids[spec.gateway_id] = {'username': spec.gateway_id, 'password': password}
    bundle = prepare_client_bundle(fixture.private / 'p8-client-bundle', run_id=run_id,
        endpoints={'https': endpoint.url, 'mqtt_tls': mqtt_endpoint},
        topology={'gateways': 2, 'sensors': 2, 'users': 2, 'viewers': 0},
        public_cas={'https': ca_http, 'mqtt': ca_mqtt}, targets=targets,
        workload={'experiment': 'E2', 'arm': 'smoke', 'samples_per_message': 100,
                  'unique_batches': 20, 'rate_per_second': 5, 'measure_seconds': 4,
                  'generator_colocated': True},
        identities={'role_jwts': {'owner_a': tokens['a'], 'owner_b': tokens['b']},
                    'gateways': gateway_ids})
    identity_dir = fixture.private / 'p8-client-bundle' / 'private'
    gateway_refs = {}
    for gw, relative in ((specs[0].gateway_id, 'gateways/' + specs[0].gateway_id + '.json'),
                         (specs[1].gateway_id, 'gateways/' + specs[1].gateway_id + '.json')):
        gateway_refs[gw] = str(identity_dir / relative)
    telemetry_identity = identity_dir / 'telemetry.json'
    smoke.private_write(telemetry_identity, '{}')
    manifest_path = fixture.private / 'p8-e2-telemetry100.json'
    manifest = {'schema_version': 1, 'experiment': 'E2', 'arm': 'smoke', 'profile': 'pilot',
        'mode': 'local', 'run_id': run_id, 'seed': 84, 'roles': ['telemetry'],
        'samples_per_message': 100, 'topology': {'gateways': 2, 'sensors_per_gateway': 1,
            'users': 0, 'viewers': 0}, 'targets': targets, 'subscriptions': [],
        'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0},
        'phases': {'warmup_seconds': 0, 'measure_seconds': 4, 'drain_seconds': 8, 'cooldown_seconds': 0},
        'limits': {'retries': 2, 'workers': 2, 'connections': 2, 'pending_messages': 32,
            'pending_bytes': 1048576, 'disk_growth_bytes': 1048576, 'wall_seconds': 15},
        'tls': {'https_endpoint': endpoint.url, 'mqtt_tls_endpoint': mqtt_endpoint,
            'https_ca_file': str(fixture.private / 'p2-ca.crt'),
            'mqtt_tls_ca_file': str(ca_mqtt_path), 'mqtt_tls_server_name': 'localhost'},
        'identity_files': {'telemetry': str(telemetry_identity)},
        'gateway_identity_files': gateway_refs, 'bundle_path': str(Path(bundle['public_manifest']).parent),
        'durability_mode': 'synthetic-load', 'retry_interval_ms': 200,
        'publish_timeout_ms': 3000}
    smoke.private_write(manifest_path, json.dumps(manifest, sort_keys=True, separators=(',', ':')))
    out_dir = fixture.private / 'p8-e2-workload'
    before = smoke.paper_p1_count_db(registry, pg)
    before_by_sensor = []
    for spec in specs:
        before_by_sensor.append({'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id,
            'counts': json.loads(smoke.db_psql(registry, pg, """
SELECT json_build_object('ledger', (SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw', (SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal', (SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
              WHERE e.entity_id=:'urn'));""", {'gw': spec.gateway_id, 'sensor': spec.sensor_id,
                  'urn': spec.sensor_urn}).strip())})
    baseline_metrics = _admin_metrics(smoke, endpoint, tokens['admin'])
    baseline_server = smoke.paper_p0_stats(registry, containers, 'workload_start')
    collector = BoundedCollector(
        scrape=lambda: _admin_metrics(smoke, endpoint, tokens['admin']),
        interval=0.5, max_snapshots=32,
        baseline_metrics=baseline_metrics, baseline_server=baseline_server)
    log = fixture.private / 'p8-paperload-run.log'
    with log.open('xb') as stream:
        proc = subprocess.Popen(['python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run',
            '--binary', str(paperload_binary), '--manifest', str(manifest_path),
            '--output-dir', str(out_dir), '--run-artifact', str(fixture.private / 'p8-run.json')],
            cwd=smoke.ROOT, stdout=stream, stderr=subprocess.STDOUT,
            env=smoke.process_env(fixture), close_fds=True)
        collector.start(proc.pid)
        try:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)
                raise ValueError('p8_paperload_timeout') from None
        finally:
            collector.stop(proc.pid if proc else None)
        result = subprocess.CompletedProcess(proc.args, proc.returncode)
    terminal_metrics = _admin_metrics(smoke, endpoint, tokens['admin'])
    terminal_server = smoke.paper_p0_stats(registry, containers, 'workload_end')
    collector.metrics.append({'phase': 'workload_end', 'snapshot': terminal_metrics})
    collector.resources.append(terminal_server)
    workload_path = out_dir / 'workload.json'
    if not workload_path.is_file():
        raise ValueError('p8_paperload_artifact_missing')
    workload = json.loads(workload_path.read_text())
    counters = workload.get('telemetry', {})
    ingress_delta = calculate_ingress_delta(baseline_metrics, terminal_metrics)
    after = smoke.paper_p1_count_db(registry, pg)
    delta = {key: after[key] - before[key] for key in before}
    after_by_sensor = []
    by_gateway = {}
    for spec in specs:
        counts = json.loads(smoke.db_psql(registry, pg, """
SELECT json_build_object('ledger', (SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw', (SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal', (SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id
              WHERE e.entity_id=:'urn'));
""", {'gw': spec.gateway_id, 'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip())
        after_by_sensor.append({'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id,
                                'counts': counts})
        by_gateway[spec.gateway_id] = counts
    scoped_delta = sensor_scoped_delta(before_by_sensor, after_by_sensor)
    raw_verification = [verify_pilot_raw(smoke, registry, pg, spec) for spec in specs]
    histogram_windows = {}
    gap = any(sample.get('snapshot') is None for sample in collector.metrics)
    for group in ('ingress', 'persistence', 'microbatch', 'telemetry'):
        for metric, value in (terminal_metrics.get(group) or {}).items():
            if isinstance(value, dict) and 'buckets' in value:
                histogram_windows[group + '.' + metric] = (
                    {'availability': 'unavailable', 'reason': 'scrape_missing'} if gap else
                    metrics_window(baseline_metrics, terminal_metrics, group, metric,
                                   smoke.paper_p1_histogram_window))
    ingress_d = ingress_delta.get('delta', {}) if ingress_delta.get('available') else {}
    success = (result.returncode == 0 and workload.get('status') == 'COMPLETE'
        and counters.get('generated_unique') == 20 and counters.get('offered_unique') == 20
        and counters.get('publish_attempts') == counters.get('broker_pubacks') == 20
        and counters.get('application_acks') == 20 and counters.get('pending_messages') == 0
        and counters.get('pending_bytes') == 0 and counters.get('invalid_acks') == 0
        and delta == {'ledger': 20, 'raw': 2000, 'temporal': 20, 'configuration': 0}
        and scoped_delta.get('available') is not None
        and all(counts == {'ledger': 10, 'raw': 1000, 'temporal': 10}
                for counts in scoped_delta['available'].values())
        and all(item['verified_exact'] and item['ledger_count'] == 10
                for item in raw_verification)
        and all(counts == {'ledger': 10, 'raw': 1000, 'temporal': 10}
                for counts in by_gateway.values()) and len(by_gateway) == 2
        and ingress_delta.get('available') is True
        and ingress_d.get('Received') == 20
        and ingress_d.get('Accepted') == 20
        and ingress_d.get('Committed') == 20
        and ingress_d.get('DuplicateCommitted') == 0
        and ingress_d.get('SamplesNew') == 2000
        and ingress_d.get('ACKPublished') == 20)
    source_rev = smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD')
    dirty = bool(smoke.paper_p0_git(fixture, 'status', '--porcelain'))
    paperload_source_sha = hashlib.sha256(b''.join(
        path.relative_to(smoke.ROOT / 'src').as_posix().encode() + b'\0' + path.read_bytes()
        for path in sorted((smoke.ROOT / 'src/internal/paperload').glob('*.go'))
        + sorted((smoke.ROOT / 'src/cmd/paperload').glob('*.go')))).hexdigest()
    paperload_bin_sha = hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest()
    manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    effective_fl = {key: fixture.values[key] for key in ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED',
        'STAGE5_WAKEUP_ENABLED', 'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED')}
    ep_data = {'https': endpoint.url, 'mqtt_tls': mqtt_endpoint,
               'mqtt_tls_server_name': 'localhost', 'ca_sha256': bundle['ca_sha256']}
    run = assemble_e2_report(
        success=success, source_revision=source_rev, dirty_worktree=dirty,
        schema=schema, quota=quota,
        image_ids={name: images.get(name) for name in ('backend', 'mosquitto', 'gateway')},
        paperload_source_sha256=paperload_source_sha, paperload_sha256=paperload_bin_sha,
        manifest_sha256=manifest_sha, workload_path=workload_path,
        effective_flags=effective_fl, endpoints=ep_data, counters=counters,
        database_delta=delta, database_by_gateway=by_gateway, scoped_delta=scoped_delta,
        pilot_raw_verification=raw_verification, manifest=manifest,
        collector_metrics=collector.metrics, collector_resources=collector.resources,
        collector_errors=collector.errors + collector.generator_samples,
        ingress_delta=ingress_delta, histogram_windows=histogram_windows)
    smoke.paper_p1_metrics_csv(fixture.artifacts / 'metrics.csv', collector.metrics)
    smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv', [r for r in collector.resources if r.get('services') and not r.get('error')], resources)
    smoke.durable_json(fixture.artifacts / 'run.json', run)
    smoke.durable_json(fixture.artifacts / 'p8-e2-smoke.json', run)
    if not success:
        raise ValueError('p8_e2_telemetry100_reconciliation_failed')
    return run


def prepare_client_bundle(root, *, run_id, endpoints, topology, public_cas, targets, workload, identities):
    """Prepare public run metadata plus a separate mode-0600 private A/B identity input."""
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    forbidden_keys = {'admin', 'auth_admin', 'db', 'service_role', 'operator', 'authadmin'}
    def identity_keys(value):
        if isinstance(value, dict):
            return {str(key).lower() for key in value} | set().union(*(identity_keys(child) for child in value.values()))
        if isinstance(value, (list, tuple)):
            return set().union(*(identity_keys(child) for child in value)) if value else set()
        return set()
    if (not isinstance(identities, dict) or set(identities) != {'role_jwts', 'gateways'}
            or not isinstance(identities.get('role_jwts'), dict)
            or not isinstance(identities.get('gateways'), dict)
            or forbidden_keys.intersection(identity_keys(identities))):
        raise ValueError('bundle_identity_rejected')
    public_sensitive = forbidden_keys | {'jwt', 'password', 'secret', 'credential', 'token', 'email',
                                         'identity', 'user', 'user_id', 'userid', 'principal', 'operator_id'}
    if public_sensitive.intersection(identity_keys({'topology': topology, 'targets': targets, 'workload': workload})):
        raise ValueError('bundle_public_data_rejected')
    if not isinstance(run_id, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', run_id):
        raise ValueError('bundle_public_data_rejected')
    if not isinstance(endpoints, dict) or set(endpoints) != {'https', 'mqtt_tls'}:
        raise ValueError('bundle_endpoint_rejected')
    for key, value in endpoints.items():
        parsed = urllib.parse.urlparse(value)
        allowed = {'https'} if key == 'https' else {'mqtts'}
        if (parsed.scheme not in allowed or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in ('', '/')
                or parsed.port in {1883, 5432, 5433, 2375, 2376}):
            raise ValueError('bundle_endpoint_rejected')
        if key == 'mqtt_tls' and parsed.port is not None and not 1024 <= parsed.port <= 65535:
            raise ValueError('bundle_endpoint_rejected')
    if not isinstance(public_cas, dict) or set(public_cas) != {'https', 'mqtt'}:
        raise ValueError('bundle_ca_rejected')
    ca_hashes = {name: hashlib.sha256(data).hexdigest() for name, data in public_cas.items()}
    public = {'contract': 'p8-client-bundle-preparation-only', 'run_id': run_id,
              'endpoints': endpoints, 'topology': topology, 'public_ca_sha256': ca_hashes,
              'targets': targets, 'workload': workload,
              'transport': {'https_ca': 'https', 'mqtt_tls_ca': 'mqtt',
                            'tls_verification': 'required; hostname/SAN verification; no insecure mode'}}
    public_path = root / 'public-manifest.json'
    public_path.write_text(json.dumps(public, sort_keys=True, separators=(',', ':')))
    public_path.chmod(0o644)
    private_dir = root / 'private'
    private_dir.mkdir(mode=0o700, exist_ok=True)
    private_dir.chmod(0o700)
    private_files = {}
    private_payloads = {'role_jwts.json': identities['role_jwts']}
    for gateway_id, credentials in identities['gateways'].items():
        if not isinstance(gateway_id, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', gateway_id):
            raise ValueError('bundle_identity_rejected')
        private_payloads['gateways/' + gateway_id + '.json'] = credentials
    for relative, payload in private_payloads.items():
        path = private_dir / relative
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(payload, stream, sort_keys=True, separators=(',', ':'))
        private_files[relative] = str(path)
    return {'public_manifest': str(public_path), 'private_files': private_files,
            'ca_sha256': ca_hashes, 'contract': 'preparation-only; remote run not exercised'}


def profile():
    # This profile is only consumed by a disposable --paper-p8-sessions fixture.
    return {'WEBSOCKET_ENABLED': 'true', 'WS_MAX_SESSIONS': str(SESSION_CAP),
            'WS_SESSION_REFERENCE_BYTES': str(8 * 1024 * 1024),
            'STAGE5_ENABLED': 'false', 'STAGE5_WAKEUP_ENABLED': 'false',
            'MQTT_SHAPER_ENABLED': 'false', 'MQTT_MICROBATCH_ENABLED': 'false',
            'MQTT_RAM_DEDUP_ENABLED': 'false'}


def session_specs(targets):
    """Make 64 separate principals' sockets split evenly with sparse/overlap filters."""
    if len(targets) != 2 or {entry['group'] for entry in targets} != {'A', 'B'}:
        raise ValueError('p8_namespace_targets_rejected')
    result = []
    for group in ('A', 'B'):
        target = next(item['target'] for item in targets if item['group'] == group)
        for index in range(32):
            sensor = {'gateway_id': target['gateway_id'], 'sensor_id': target['sensor_id']}
            gateway = {'gateway_id': target['gateway_id']}
            filters = [sensor] if index % 3 == 0 else [gateway]
            if index % 3 == 2:
                filters.append(sensor)  # intentional Gateway + Sensor overlap, one delivery/session
            result.append({'id': f'{group.lower()}-{index:02d}', 'group': group, 'filters': filters})
    return result


def expected_deliveries(sessions, *, batches_a, batches_b):
    counts = {group: sum(session['group'] == group for session in sessions) for group in ('A', 'B')}
    return {'A': counts['A'] * batches_a, 'B': counts['B'] * batches_b}


def validate_state(state, *, sessions, before_a, before_b, after_b):
    expected = expected_deliveries(sessions, batches_a=before_a, batches_b=before_b + after_b)
    viewers = state.get('viewers')
    if not isinstance(viewers, list) or len(viewers) != SESSION_CAP:
        raise ValueError('p8_viewer_state_rejected')
    actual = {'A': 0, 'B': 0}
    for viewer in viewers:
        if viewer.get('group') not in actual or viewer.get('unexpected_namespace') or viewer.get('duplicates'):
            raise ValueError('p8_namespace_delivery_rejected')
        actual[viewer['group']] += viewer.get('unique_events', 0)
    # Group A closes after its pre-revoke messages. Group B remains live for post-revoke data.
    expected['A'] = sum(1 for s in sessions if s['group'] == 'A') * before_a
    if actual != expected or state.get('unexpected_namespace') or state.get('duplicates'):
        raise ValueError('p8_expected_delivery_count_mismatch')
    if state.get('revoke_complete') is not True:
        raise ValueError('p8_revoke_not_completed')
    if state.get('cap_plus_one_denied') is not True or state.get('replacement_ready') is not True:
        raise ValueError('p8_capacity_replacement_not_verified')
    if any(v.get('group') == 'B' and v.get('closed') for v in viewers):
        raise ValueError('p8_healthy_namespace_closed')
    if any(v.get('group') == 'A' and not v.get('closed') for v in viewers):
        raise ValueError('p8_revoked_namespace_still_open')
    return {'expected': expected, 'actual': actual}


def stop_child(proc, *, timeout=5):
    """Stop and join an owned process even when the caller is unwinding an error."""
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGINT)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGTERM)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=timeout)


def run(smoke, fixture, endpoint, registry, containers, images, lifecycle_state):
    """Run the 64-session native probe against the current exact-owned P2 fixture."""
    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
        smoke.prepare_paper_workload(fixture, registry, endpoint, containers, images, lifecycle_state))
    config_env = effective.get('services', {}).get('backend', {}).get('environment', {})
    if any(str(config_env.get(key)) != value for key, value in profile().items()):
        raise ValueError('p8_effective_session_cap_rejected')
    schema = smoke.db_psql(registry, pg,
        "SELECT version::text || ':' || name FROM schema_migrations ORDER BY version DESC LIMIT 1;").strip()
    if schema != '24:telemetry_ledger_timestamp':
        raise ValueError('p8_schema_terminal_rejected')
    targets = [{'group': role.upper(), 'target': {'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id}}
               for role, spec in zip(('a', 'b'), specs)]
    sessions = session_specs(targets)
    ws_url = endpoint.url.replace('https://', 'wss://').rstrip('/')
    if not ws_url.endswith('/v1/ws'):
        ws_url += '/v1/ws'
    probe_dir = fixture.private / 'p8-sessions'
    probe_dir.mkdir(mode=0o700)
    probe_dir.chmod(0o700)
    resource_rows = []
    fixture_resource_rows = []

    def collect_resources(phase):
        resource_rows.append(smoke.paper_p0_stats(registry, containers, phase))
        smoke.paper_p1_resources_csv(fixture.artifacts / 'resources.csv', resource_rows, resources)
        def exec_owned_service(service, command):
            if service not in ('nginx', 'backend', 'mosquitto') or service not in containers:
                raise ValueError('fixture_resource_service_rejected')
            result = registry.command(['exec', containers[service]['name'], *command])
            if result.returncode:
                raise ValueError('fixture_resource_readback_failed')
            return result.stdout.decode(errors='replace')
        fixture_resource_rows.append({'phase': phase,
            'readback': read_owned_fixture_resources(exec_owned_service, viewers=SESSION_CAP)})

    config = {'wss_endpoint': ws_url, 'origin': 'https://client.example.test',
              'ca_file': str(endpoint.ca_file),
              'role_jwts': {'A': tokens['a'], 'B': tokens['b']},
              'namespace_targets': targets, 'sessions': sessions,
              'max_lifetime_ms': PILOT_LIMIT_SECONDS * 1000,
              'max_bytes': MAX_BYTES, 'max_events': MAX_EVENTS,
              'ready_path': str(probe_dir / 'ready.json'), 'state_path': str(probe_dir / 'state.json'),
              'stop_path': str(probe_dir / 'stop'), 'revoke_group_a_path': str(probe_dir / 'revoke-a'),
              'output_path': str(probe_dir / 'result.json')}
    config_path = probe_dir / 'config.json'
    smoke.private_write(config_path, json.dumps(config, separators=(',', ':')))
    log_path = fixture.artifacts / 'p8-sessions-probe.log'
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    child = log = None
    primary = None
    cleanup_errors = []
    try:
        log = os.fdopen(log_fd, 'wb')
        env = os.environ.copy()
        env['P8_SESSIONS_CONFIG_FILE'] = str(config_path)
        child = subprocess.Popen(['go', 'test', './internal/telemetryws', '-run',
            '^TestNativeP8SessionsProbe$', '-count=1', '-timeout=180s'],
            cwd=Path(smoke.__file__).parents[2] / 'src', env=env,
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        ready = _wait_json(Path(config['ready_path']), child, 60)
        if ready != {'ready': True, 'socket_count': SESSION_CAP, 'principal_count': 2,
                     'target_count': 2}:
            raise ValueError('p8_probe_ready_rejected')
        collect_resources('64_sessions_ready')
        metrics_before_timers = _websocket_metrics(smoke, endpoint, tokens['admin'])
        # Two recheck cycles and two server heartbeat intervals; check all healthy peers remain open.
        time.sleep(max(2 * RECHECK_SECONDS + 1, 2 * HEARTBEAT_SECONDS + 1))
        state = _read_json(Path(config['state_path']))
        if len(state.get('viewers', [])) != SESSION_CAP or any(v.get('closed') for v in state['viewers']):
            raise ValueError('p8_sessions_not_healthy_after_timers')
        metrics_after_timers = _websocket_metrics(smoke, endpoint, tokens['admin'])
        collect_resources('after_recheck_heartbeat_cycles')
        recheck_delta = metrics_after_timers.get('recheck_sessions_examined', 0) - metrics_before_timers.get('recheck_sessions_examined', 0)
        if recheck_delta < SESSION_CAP * 2:
            raise ValueError('p8_recheck_cycles_not_observed')

        volumes = _owned_outboxes(fixture, registry, specs)
        first = _emit_batches(smoke, fixture, registry, images, specs, passwords, volumes,
                              lifecycle_state, (1, 1), 'before')
        _wait_state_counts(Path(config['state_path']), sessions, 1, 1, 0, child)
        # Revoke only A's concrete DB membership; user JWT remains unchanged.
        smoke.db_psql(registry, pg,
            "DELETE FROM user_gateways WHERE user_id=:'uid' AND gateway_id=:'gw';",
            {'uid': _user_id_from_token(tokens['a']), 'gw': specs[0].gateway_id})
        smoke.private_write(Path(config['revoke_group_a_path']), 'revoked\n')
        _wait_revoke(Path(config['state_path']), child)
        second = _emit_batches(smoke, fixture, registry, images, specs, passwords, volumes,
                               lifecycle_state, (0, 1), 'after-revoke')
        collect_resources('post_revoke_delivery')
        _wait_state_counts(Path(config['state_path']), sessions, 1, 1, 1, child)
        _touch_private(Path(config['stop_path']))
        child.wait(timeout=15)
        if child.returncode:
            raise ValueError('p8_probe_failed')
        result = _read_json(Path(config['output_path']))
        if result.get('cap_plus_one_denied') is not True or result.get('replacement_ready') is not True:
            raise ValueError('p8_capacity_replacement_not_verified')
        delivery = validate_state(result, sessions=sessions, before_a=1, before_b=1, after_b=1)
        metric_status, _, final_metrics = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics',
                                                              token=tokens['admin'])
        ws_metrics = final_metrics.get('websocket_metrics') if isinstance(final_metrics, dict) else None
        if (metric_status != 200 or not isinstance(ws_metrics, dict)
                or ws_metrics.get('sessions') != 0 or ws_metrics.get('subscriptions') != 0
                or ws_metrics.get('session_reference_bytes') != 0
                or ws_metrics.get('session_queued_in_flight_bytes') != 0):
            raise ValueError('p8_shutdown_reservations_not_zero')
        collect_resources('after_session_shutdown')
        db = _db_counts(smoke, registry, pg, specs)
        if db != {'ledger': 3, 'raw': 300, 'temporal': 3}:
            raise ValueError('p8_database_counts_rejected')
        result.update(status='PASS', delivery_counts=delivery, database_counts=db,
                      generator_colocated=True, schema_terminal=schema,
                      backend_quota=quota, service_resources=resources, storage_status=storage_status,
                      source=_source_fingerprint(Path(smoke.__file__).parents[2]),
                      image_ids={key: images.get(key) for key in sorted(images)},
                      effective_profile={key: effective.get('services', {}).get('backend', {}).get('environment', {}).get(key)
                                         for key in profile()},
                      workload={'session_cap': SESSION_CAP, 'aggregate_event_budget': MAX_EVENTS,
                                'aggregate_byte_budget': MAX_BYTES, 'sessions': 64,
                                'principals': 2, 'per_namespace_sessions': 32,
                                'expected_filters': sum(len(s['filters']) for s in sessions),
                                'generator_colocated': True},
                      timers={'recheck_seconds': _duration_seconds(config_env['WS_RECHECK_INTERVAL']),
                              'recheck_budget_seconds': _duration_seconds(config_env['WS_RECHECK_SWEEP_BUDGET']),
                              'heartbeat_seconds': _duration_seconds(config_env['WS_HEARTBEAT_INTERVAL']),
                              'pong_timeout_seconds': _duration_seconds(config_env['WS_PONG_TIMEOUT']),
                              'recheck_concurrency': int(config_env['WS_RECHECK_CONCURRENCY']),
                              'observed_wait_seconds': 41,
                              'recheck_cycles_observed_min': recheck_delta // SESSION_CAP,
                              'heartbeat_cycles_observed_min': 2,
                              'recheck_sessions_examined_delta': recheck_delta,
                              'healthy_sockets_after_timer_wait': sum(not v.get('closed') for v in state['viewers'])},
                      final_websocket_metrics={k: ws_metrics.get(k) for k in
                          ('sessions', 'subscriptions', 'session_reference_bytes', 'session_queued_in_flight_bytes')},
                      gateway_summaries=first + second, cleanup_verified=False,
                      application_ack_count=sum(int(row['app_acknowledged']) for row in first + second),
                      pending_outbox_rows=sum(int(row['pending_rows']) for row in first + second),
                      pending_outbox_bytes=sum(int(row['pending_bytes']) for row in first + second),
                      resource_sample_count=len(resource_rows),
                      fixture_resource_readbacks=fixture_resource_rows,
                      flags=profile(), probe_log='p8-sessions-probe.log')
        smoke.durable_json(fixture.artifacts / 'p8-sessions.json', result)
        return result
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            _touch_private(Path(config['stop_path']))
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            stop_child(child)
        except BaseException as exc:
            cleanup_errors.append(exc)
        if log is not None:
            try:
                log.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            smoke.durable_json(fixture.artifacts / 'p8-sessions-cleanup.json', {
                'primary_error': type(primary).__name__ if primary else None,
                'cleanup_errors': [type(e).__name__ for e in cleanup_errors]})
            if primary is None:
                raise ValueError('p8_child_cleanup_failed') from cleanup_errors[0]


def _wait_json(path, child, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise ValueError('p8_probe_exited_before_ready')
        if path.exists():
            return _read_json(path)
        time.sleep(.1)
    raise ValueError('p8_probe_ready_timeout')


def _read_json(path):
    if not path.is_file() or path.stat().st_size > 2 * 1024 * 1024:
        raise ValueError('p8_probe_state_invalid')
    return json.loads(path.read_text())


def _websocket_metrics(smoke, endpoint, token):
    status, _, body = smoke.http_request(endpoint, 'GET', '/v1/admin/metrics', token=token)
    ws = body.get('websocket_metrics') if status == 200 and isinstance(body, dict) else None
    if not isinstance(ws, dict) or type(ws.get('recheck_sessions_examined')) is not int:
        raise ValueError('p8_websocket_metrics_unavailable')
    return ws


def _duration_seconds(value):
    text = str(value)
    if text.endswith('ms'):
        return float(text[:-2]) / 1000
    if text.endswith('s'):
        return float(text[:-1])
    raise ValueError('p8_effective_timer_rejected')


def _wait_state_counts(path, sessions, before_a, before_b, after_b, child):
    expected = expected_deliveries(sessions, batches_a=before_a, batches_b=before_b + after_b)
    if after_b:
        expected['A'] = 32 * before_a
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise ValueError('p8_probe_exited_during_delivery')
        state = _read_json(path)
        actual = {'A': 0, 'B': 0}
        for v in state.get('viewers', []):
            if v.get('group') in actual:
                actual[v['group']] += v.get('unique_events', 0)
        if actual == expected:
            return state
        time.sleep(.1)
    raise ValueError('p8_expected_delivery_timeout')


def _wait_revoke(path, child):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if child.poll() is not None:
            raise ValueError('p8_probe_exited_during_revoke')
        if _read_json(path).get('revoke_complete') is True:
            return
        time.sleep(.1)
    raise ValueError('p8_revoke_timeout')


def _touch_private(path):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        info = path.lstat()
        if not path.is_file() or info.st_mode & 0o077:
            raise ValueError('p8_control_file_rejected') from None
        return
    os.close(fd)


def _user_id_from_token(token):
    import base64
    payload = token.split('.')[1]
    return json.loads(base64.urlsafe_b64decode(payload + '=' * (-len(payload) % 4)))['sub']


def _owned_outboxes(fixture, registry, specs):
    volumes = []
    for index in range(2):
        labels = {'test.task': 'native-compose-startup', 'test.run': fixture.project}
        item = registry.intent('volume', fixture.project + '_p8_session_outbox_' + str(index), None, labels)
        if registry.resolve(item) is not None:
            raise ValueError('resource_collision')
        registry.allocating(item)
        result = registry.command(['volume', 'create', '--label', 'test.task=' + labels['test.task'],
                                   '--label', 'test.run=' + fixture.project, item['name']])
        if result.returncode or registry.resolve(item) is None:
            raise ValueError('p8_outbox_allocation_failed')
        volumes.append(item)
    return volumes


def _emit_batches(smoke, fixture, registry, images, specs, passwords, volumes, lifecycle_state, counts, label):
    from stage3_uplink_runner import gateway_env, parse_run_summary
    output = []
    for i, count in enumerate(counts):
        if not count:
            continue
        env = gateway_env(specs[i].gateway_id, passwords[i])
        env.update(GATEWAY_MQTT_CLIENT_ID=f'{fixture.project}_p8_{label}_{i}_{os.urandom(4).hex()}',
                   GATEWAY_MQTT_BROKER_URL='ssl://mosquitto:8883',
                   GATEWAY_SAMPLE_RATE_HZ='100', GATEWAY_SAMPLES_PER_BATCH='100')
        env_path = fixture.private / f'gateway-p8-{label}-{i}.env'
        smoke.private_write(env_path, ''.join(f'{k}={v}\n' for k, v in env.items()))
        raw = smoke.paper_p0_gateway_cli(fixture, registry, images['gateway'], volumes[i], env_path,
            fixture.private / 'certs/ca.crt', f'p8-{label}-{i}',
            ['run', '--batches', str(count), '--completion-timeout', '60s'],
            lifecycle_state, timeout=75)
        summary = parse_run_summary(raw)
        if (summary.get('completed') != 'true' or int(summary.get('enqueued', '-1')) != count
                or int(summary.get('app_acknowledged', '-1')) != count
                or summary.get('pending_rows') != '0' or summary.get('pending_bytes') != '0'):
            raise ValueError('p8_gateway_summary_rejected')
        output.append({key: summary.get(key) for key in ('completed', 'enqueued', 'app_acknowledged',
                                                        'pending_rows', 'pending_bytes')})
    return output


def _db_counts(smoke, registry, pg, specs):
    ledger = raw = temporal = 0
    for spec in specs:
        data = json.loads(smoke.db_psql(registry, pg, """SELECT json_build_object('ledger',
          (SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
          'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'));""",
          {'gw': spec.gateway_id, 'sensor': spec.sensor_id, 'urn': spec.sensor_urn}).strip())
        ledger += data['ledger']
        raw += data['raw']
        temporal += data['temporal']
    return {'ledger': ledger, 'raw': raw, 'temporal': temporal}


def _source_fingerprint(root):
    def git(*args):
        return subprocess.run(['git', '-C', str(root), *args], check=True,
                              capture_output=True).stdout
    status = git('status', '--porcelain', '--untracked-files=all')
    diff = git('diff', '--binary', 'HEAD')
    untracked = []
    for line in status.decode(errors='replace').splitlines():
        if line.startswith('?? '):
            path = root / line[3:]
            if path.is_file():
                untracked.append((line[3:].encode() + b'\0' + path.read_bytes()))
    digest = hashlib.sha256(status + b'\0' + diff + b'\0' + b'\0'.join(untracked)).hexdigest()
    revision = git('rev-parse', 'HEAD').decode().strip()
    branch = git('branch', '--show-current').decode().strip()
    return {'revision': revision, 'branch': branch, 'dirty': bool(status),
            'dirty_source_sha256': digest if status else None}
