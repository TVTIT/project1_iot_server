"""E2 workload orchestration and accounting helpers for the disposable fixture."""
import collections
import hashlib
import importlib.util
import json
import math
import os
import re
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

_support_spec = importlib.util.spec_from_file_location(
    'paper_p8_support', Path(__file__).with_name('paper_p8_support.py'))
_support = importlib.util.module_from_spec(_support_spec)
_support_spec.loader.exec_module(_support)
sensor_scoped_delta = _support.sensor_scoped_delta

BoundedCollector = _support.BoundedCollector
verify_pilot_raw = _support.verify_pilot_raw

def _load_sibling_module(name):
    path = Path(__file__).with_name(name + '.py')
    spec = importlib.util.spec_from_file_location('paper_p8_e2_' + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

_pg_diagnostics = _load_sibling_module('paper_p8_e2_diagnostics')
_linux_resources = _load_sibling_module('paper_p8_e2_resources')
read_postgres_diagnostics = _pg_diagnostics.read_postgres_diagnostics
read_linux_resources = _linux_resources.read_linux_resources

try:
    from scripts import paper_p8_client
except ModuleNotFoundError as exc:
    if exc.name != 'scripts':
        raise
    # The native CLI starts with scripts/tests on sys.path, not the repo root.
    # Resolve our sibling package from this file rather than the caller's cwd
    # or an externally supplied PYTHONPATH.
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from scripts import paper_p8_client

PILOT_SAMPLES = (100, 1)
OFFICIAL_RATES = (100, 200, 500, 1000, 2000, 5000, 8000, 10000, 15000, 20000)
PILOT_SEED = 84
PILOT_MESSAGES = {'A': 8, 'B': 16}

try:
    from scripts.tests.paper_p8_e2_retry_policy import validate_retry_policy
except ModuleNotFoundError:
    from paper_p8_e2_retry_policy import validate_retry_policy


def validate_applied_retry_policy(prepared, workloads):
    manifests = prepared.get('manifests', {})
    policies = [manifests.get(arm, {}).get('retry_policy') for arm in ('A', 'B')]
    expected = policies[0]
    if policies[1] != expected:
        raise ValueError('e2_retry_policy_mismatch')
    if expected is None:
        return None
    for arm in ('A', 'B'):
        if workloads.get(arm, {}).get('retry_policy') != expected:
            raise ValueError(f'e2_retry_policy_applied_proof_mismatch:{arm}')
    return dict(expected)


def e2_pilot_manifests(*, common, targets, gateway_identity_files, telemetry_identity_files):
    """Create paired same-seed A/B synthetic telemetry manifests for sample profiles."""
    if len(targets) != 2 or len({t.get('gateway_id') for t in targets}) != 2:
        raise ValueError('e2_requires_two_gateway_targets')
    if set(gateway_identity_files) != {t['gateway_id'] for t in targets}:
        raise ValueError('e2_gateway_credentials_mismatch')
    profiles = {}
    for samples in PILOT_SAMPLES:
        pair = {}
        for arm in ('A', 'B'):
            measure, burst = (8, 0) if arm == 'A' else (4, 12)
            m = {**common, 'schema_version': 1, 'experiment': 'E2', 'arm': arm,
             'run_id': f"{common.get('run_id', 'e2')}-{samples}",
                 'profile': 'pilot', 'roles': ['telemetry'], 'seed': PILOT_SEED,
                  'samples_per_message': samples, 'targets': [dict(targets[0 if arm == 'A' else 1])],
                 'subscriptions': [],
                  'topology': {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
                 'traffic': {'rate_per_second': 1, 'ramp_seconds': 0, 'burst_messages': burst},
                 'phases': {'warmup_seconds': 0, 'measure_seconds': measure,
                            'drain_seconds': 12, 'cooldown_seconds': 0},
                 'limits': {'retries': 16, 'workers': 2,
                            'connections': 1, 'pending_messages': 64, 'pending_bytes': 1 << 20,
                            'disk_growth_bytes': 1 << 24, 'wall_seconds': 30},
                  'gateway_identity_files': {targets[0 if arm == 'A' else 1]['gateway_id']:
                                             gateway_identity_files[targets[0 if arm == 'A' else 1]['gateway_id']]},
                 'identity_files': {'telemetry': telemetry_identity_files[arm]},
                 'durability_mode': 'synthetic-load', 'retry_interval_ms': 500}
            pair[arm] = m
        profiles[str(samples)] = pair
    return {'profiles': profiles, 'unique_messages_per_profile': 24,
            'shared_seed': PILOT_SEED, 'same_topology_security_quota_worker_policy': True,
            'execution': 'NOT RUN'}


def e2_official_manifests(*, common, targets, gateway_identity_files, telemetry_identity_files,
                          samples_per_message, offered_rate, duration_seconds, burst_messages=0,
                          pending_messages=64, pending_bytes=1 << 20, retry_policy=None):
    """Build two role manifests whose rates sum to the selected run-wide offer."""
    if (len(targets) != 2 or len({t.get('gateway_id') for t in targets}) != 2
            or samples_per_message not in PILOT_SAMPLES or offered_rate not in OFFICIAL_RATES
            or type(duration_seconds) is not int or duration_seconds < 1
            or type(burst_messages) is not int or not 0 <= burst_messages <= 100000
            or type(pending_messages) is not int or not 1 <= pending_messages <= 100000
            or type(pending_bytes) is not int or not 1 <= pending_bytes <= 1 << 30
            or 2 * (pending_bytes + pending_messages * 4096) > 256 << 20):
        raise ValueError('e2_official_workload_rejected')
    rates = {'A': offered_rate // 2, 'B': offered_rate - offered_rate // 2}
    pair = {}
    if retry_policy is not None:
        retry_policy = validate_retry_policy(retry_policy, pending_messages=pending_messages)
    for arm, target in zip(('A', 'B'), targets):
        if target['gateway_id'] not in gateway_identity_files or arm not in telemetry_identity_files:
            raise ValueError('e2_gateway_credentials_mismatch')
        pair[arm] = {**common, 'schema_version': 1, 'experiment': 'E2', 'arm': arm,
            'run_id': f"{common.get('run_id', 'e2')}-{samples_per_message}-{offered_rate}",
            'profile': 'official', 'roles': ['telemetry'], 'seed': common.get('seed', PILOT_SEED),
            'samples_per_message': samples_per_message, 'targets': [dict(target)], 'subscriptions': [],
            'topology': {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            'traffic': {'rate_per_second': rates[arm], 'total_rate_per_second': offered_rate,
                        'ramp_seconds': 0, 'burst_messages': burst_messages if arm == 'B' else 0},
            'phases': {'warmup_seconds': 0, 'measure_seconds': duration_seconds,
                       'drain_seconds': 30, 'cooldown_seconds': 0},
            'limits': {'retries': 16, 'workers': 2, 'connections': 1,
                       'pending_messages': pending_messages, 'pending_bytes': pending_bytes,
                       'disk_growth_bytes': 10 << 30, 'wall_seconds': duration_seconds + 60},
            'gateway_identity_files': {target['gateway_id']: gateway_identity_files[target['gateway_id']]},
            'identity_files': {'telemetry': telemetry_identity_files[arm]},
            'durability_mode': 'synthetic-load', 'retry_interval_ms': 500}
        if retry_policy is not None:
            pair[arm]['retry_policy'] = dict(retry_policy)
    return {'profiles': {str(samples_per_message): pair}, 'profile': 'official',
            'offered_rate_per_second': offered_rate, 'producer_rates_per_second': rates,
            'duration_seconds': duration_seconds, 'unique_messages_per_profile': offered_rate * duration_seconds + burst_messages,
            'raw_rows_per_profile': (offered_rate * duration_seconds + burst_messages) * samples_per_message,
            'execution': 'NOT RUN'}


def e2_shaper_caps(*, profile='pilot', offered_rate=2, burst_messages=0,
                   retry_rate=0, retry_burst=0, tolerance_seconds=1.0):
    """Calculates shaper rate and burst capacity for E2 execution.

    Rate accounts for nominal unique rate with a 10% scheduler/TCP jitter allowance
    plus retry attempt rate:
        shaper_rate = ceil(rate_nominal * 1.10 + retry_rate)
    Burst accounts for rate tolerance window plus retry burst and workload burst:
        shaper_burst = ceil(shaper_rate * tolerance_seconds) + retry_burst + workload_burst
    """
    if profile != 'official':
        return {'rate_per_second': 2, 'burst': 4}
    rate_nominal = (offered_rate + 1) // 2
    retry_rate = max(0, int(retry_rate or 0))
    retry_burst = max(0, int(retry_burst or 0))
    burst_messages = max(0, int(burst_messages or 0))
    shaper_rate = math.ceil(round(rate_nominal * 1.10, 6) + retry_rate)
    shaper_burst = math.ceil(round(shaper_rate * tolerance_seconds, 6)) + retry_burst + burst_messages
    if shaper_rate > 10000:
        raise ValueError(f'e2_shaper_rate_exceeds_backend_limit: {shaper_rate} > 10000')
    if shaper_burst > 100000:
        raise ValueError(f'e2_shaper_burst_exceeds_backend_limit: {shaper_burst} > 100000')
    return {'rate_per_second': shaper_rate, 'burst': shaper_burst}


def e2_fixture_profile(*, microbatch_wait_ms, profile='pilot', offered_rate=2,
                       burst_messages=4, samples_per_message=1,
                       retry_rate=0, retry_burst=0, tolerance_seconds=1.0):
    """Proposed next-fixture-only flags/caps; does not mutate deployment defaults."""
    if microbatch_wait_ms not in (50, 100):
        raise ValueError('e2_microbatch_wait_rejected')
    if profile not in ('pilot','official') or samples_per_message not in PILOT_SAMPLES:
        raise ValueError('e2_fixture_profile_rejected')
    shaper = e2_shaper_caps(profile=profile, offered_rate=offered_rate,
                            burst_messages=burst_messages, retry_rate=retry_rate,
                            retry_burst=retry_burst, tolerance_seconds=tolerance_seconds)
    rate = shaper['rate_per_second']
    burst = shaper['burst']
    max_rows = min(500, max(1, rate * samples_per_message)) if profile == 'official' else 500
    return {'effective_fixture_flags': {'MQTT_SHAPER_ENABLED': True,
            'MQTT_MICROBATCH_ENABLED': True, 'MQTT_RAM_DEDUP_ENABLED': False,
            'STAGE5_ENABLED': False, 'STAGE5_WAKEUP_ENABLED': False,
            'WEBSOCKET_ENABLED': False},
            'shaper': {'rate_per_second': rate, 'burst': burst},
            'microbatch': {'max_messages': 32, 'max_rows': max_rows,
                           'max_bytes': 4 << 20, 'wait_ms': microbatch_wait_ms},
            'scope': 'proposed disposable E2 fixture only; deployment defaults unchanged'}


def prepare_remote(smoke, fixture, endpoint, registry, containers, images, state, *,
                   session_id, samples_per_message, output_root, bundle_root,
                   arm='E2-pilot', workload='steady_burst', context_path=None,
                   reference_path=None, context=None, grace_seconds=10,
                   check_freshness=True, now_utc=None, profile='pilot',
                   official_opt_in=False, offered_rate=100, duration_seconds=10,
                    burst_messages=0, pending_messages=64, pending_bytes=1 << 20,
                    retry_policy=None):
    """Prepare one bounded E2 profile for the existing A/B helper lifecycle."""
    if samples_per_message not in PILOT_SAMPLES:
        raise ValueError('e2_samples_profile_rejected')
    if workload not in ('steady_burst', 'equal_samples'):
        raise ValueError('e2_workload_rejected')
    if profile not in ('pilot', 'official'):
        raise ValueError('e2_profile_rejected')
    if profile != 'official' and (pending_messages != 64 or pending_bytes != 1 << 20):
        raise ValueError('e2_pending_requires_official')
    if profile == 'official' and not official_opt_in:
        raise ValueError('official_profile_requires_opt_in')
    if profile == 'official' and workload != 'steady_burst':
        raise ValueError('official_equal_samples_not_supported')
    if retry_policy is not None and (profile != 'official' or workload != 'steady_burst'):
        raise ValueError('e2_retry_policy_scope_rejected')
    if retry_policy is not None:
        retry_policy = validate_retry_policy(retry_policy, pending_messages=pending_messages)
    estimate = (e2_official_estimate(rate_per_second=offered_rate,
        duration_seconds=duration_seconds, samples_per_message=samples_per_message,
        payload_bytes=4096, official_opt_in=True, burst_messages=burst_messages,
        pending_messages=pending_messages, pending_bytes=pending_bytes,
        retry_policy=retry_policy) if profile == 'official' else None)

    validated_ctx = None
    if workload == 'equal_samples':
        if context is None:
            if not context_path:
                raise ValueError('equal_samples_requires_context')
            cp = Path(context_path)
            if cp.is_symlink() or not cp.is_file():
                raise ValueError('e2_context_file_rejected')
            try:
                raw_ctx = json.loads(cp.read_text(encoding='utf-8'))
            except (UnicodeError, json.JSONDecodeError):
                raise ValueError('e2_context_invalid') from None
        else:
            raw_ctx = context
        validated_ctx = validate_e2_equal_samples_context(raw_ctx, now_utc=now_utc, check_freshness=check_freshness)

    pg, quota, effective, resources, storage_status, tokens, specs, passwords, _ = (
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
    remote_config = getattr(fixture, 'remote_network_config', None)
    mqtt_host = remote_config['advertised_host'] if remote_config else '127.0.0.1'
    if remote_config:
        smoke.paper_p8_remote_support().validate_runtime_mapping(
            'mosquitto', broker.get('NetworkSettings', {}).get('Ports') or {}, remote_config)
    else:
        _support.validate_mqtt_readback({'service': 'mosquitto', 'host_ip': '127.0.0.1',
            'target': 8883, 'published': published}, owned_service='mosquitto')
    root = Path(output_root)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    ca_path = fixture.private / 'certs/ca.crt'
    targets = [{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id} for spec in specs]
    gateway_files, telemetry_files = {}, {}
    for spec, password, role_arm in zip(specs, passwords, ('A', 'B')):
        credential = root / f'gateway-{spec.gateway_id}.json'
        smoke.private_write(credential, json.dumps({'username': spec.gateway_id, 'password': password}, sort_keys=True))
        gateway_files[spec.gateway_id] = str(credential)
        identity = root / f'telemetry-{role_arm}-identities.json'
        smoke.private_write(identity, '{}')
        telemetry_files[role_arm] = str(identity)

    if workload == 'steady_burst':
        seed = PILOT_SEED
        run_id = f'p8e2-{session_id}-{samples_per_message}'
        common = {'mode': 'remote', 'run_id': f'p8e2-{session_id}', 'seed': seed,
            'bundle_path': str(root), 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': endpoint.url, 'https_ca_file': str(endpoint.ca_file),
                'mqtt_tls_endpoint': f'mqtts://{mqtt_host}:{published}',
                'mqtt_tls_ca_file': str(ca_path), 'mqtt_tls_server_name': 'localhost'}}
        manifests = (e2_official_manifests(common=common, targets=targets,
            gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files,
            samples_per_message=samples_per_message, offered_rate=offered_rate,
            duration_seconds=duration_seconds, burst_messages=burst_messages,
            pending_messages=pending_messages, pending_bytes=pending_bytes,
            retry_policy=retry_policy)
            if profile == 'official' else e2_pilot_manifests(common=common, targets=targets,
            gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files))
        pair = manifests['profiles'][str(samples_per_message)]
        phase_id = f'samples-{samples_per_message}'
        expected_counts = None
    else:
        seed = validated_ctx['seed']
        run_id = f'p8e2-{session_id}-{samples_per_message}'
        common = {'mode': 'remote', 'run_id': f'p8e2-{session_id}', 'seed': seed,
            'bundle_path': str(root), 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': endpoint.url, 'https_ca_file': str(endpoint.ca_file),
                'mqtt_tls_endpoint': f'mqtts://{mqtt_host}:{published}',
                'mqtt_tls_ca_file': str(ca_path), 'mqtt_tls_server_name': 'localhost'}}
        manifests = e2_equal_samples_manifests(
            context=validated_ctx, common=common, targets=targets,
            gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files,
            grace_seconds=grace_seconds, now_utc=now_utc, check_freshness=check_freshness)
        pair = manifests['profiles'][str(samples_per_message)]
        phase_id = f'equal-samples-{samples_per_message}'
        expected_counts = manifests['expected_counts'][str(samples_per_message)]

    run_id = pair['A']['run_id']
    manifest_paths = {}
    for role_arm in ('A', 'B'):
        path = root / f'{role_arm}.manifest.json'
        smoke.private_write(path, json.dumps(pair[role_arm], sort_keys=True, separators=(',', ':')))
        manifest_paths[role_arm] = path
    plan_root = root / 'plan'
    plan = build_e2_execution_plan(output_dir=plan_root, manifest_a=manifest_paths['A'],
                                   manifest_b=manifest_paths['B'], seed=seed,
                                   profile=profile, official_opt_in=official_opt_in)
    prepared = {'session_id': session_id, 'run_id': run_id, 'arm': arm,
        'samples_per_message': samples_per_message, 'profile': profile, 'workload': workload,
        'official_estimate': estimate,
        'root': root, 'bundle_root': Path(bundle_root), 'plan': plan, 'manifest_paths': manifest_paths,
        'manifests': pair, 'pg': pg, 'quota': quota, 'effective': effective,
        'resources': resources, 'storage_status': storage_status, 'tokens': tokens,
        'specs': specs, 'schema': schema, 'gateway_files': gateway_files,
        'telemetry_files': telemetry_files, 'https_ca_file': endpoint.ca_file,
        'mqtt_ca_file': ca_path, 'endpoint': endpoint, 'phase_id': phase_id,
        'seed': seed, 'reference_path': reference_path, 'images': images}
    if workload == 'equal_samples':
        prepared['context'] = validated_ctx
        prepared['context_path'] = str(context_path) if context_path else None
        prepared['expected_counts'] = expected_counts
        duration_seconds = validated_ctx['duration_seconds']
        drain_seconds = 20
        wall_seconds = duration_seconds + drain_seconds + grace_seconds
        prepared['duration_seconds'] = duration_seconds
        prepared['drain_seconds'] = drain_seconds
        prepared['grace_seconds'] = grace_seconds
        prepared['wall_seconds'] = wall_seconds

    prepared['bundle'] = export_prepared_bundle(prepared)
    bundle_file = Path(bundle_root) / 'bundle.json'
    if bundle_file.is_file():
        try:
            inputs = json.loads(bundle_file.read_text(encoding='utf-8')).get('inputs', [])
            prepared['bundle_inventory'] = [
                {'path': row['path'], 'sha256': row['sha256']}
                for row in inputs
                if isinstance(row, dict) and 'path' in row and 'sha256' in row
            ]
        except Exception:
            prepared['bundle_inventory'] = []
    else:
        prepared['bundle_inventory'] = []
    return prepared


def export_prepared_bundle(prepared, *, wait_seconds=90, workload_seconds=None):
    try:
        from scripts import paper_p8_bundle
    except ModuleNotFoundError:
        spec = importlib.util.spec_from_file_location(
            'paper_p8_bundle_runtime', Path(__file__).parents[1] / 'paper_p8_bundle.py')
        paper_p8_bundle = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(paper_p8_bundle)
    ca_root = prepared['root'] / 'bundle-ca-inputs'
    ca_root.mkdir(mode=0o700)
    ca_files = {}
    for name, source in (('https', prepared['https_ca_file']), ('mqtt', prepared['mqtt_ca_file'])):
        target = ca_root / f'{name}-ca.pem'
        target.write_bytes(Path(source).read_bytes()); target.chmod(0o600); ca_files[name] = target
    identities = {f'{arm}:telemetry': prepared['telemetry_files'][arm] for arm in ('A', 'B')}
    principals = {key: [] for key in identities}
    role_metadata = {'roles': {key: {'role': 'telemetry', 'principals': value}
                               for key, value in principals.items()},
        'gateways': {gateway_id: {'role': 'gateway', 'gateway_id': gateway_id,
            'username': json.loads(Path(path).read_text())['username']}
            for gateway_id, path in prepared['gateway_files'].items()}}
    spec = json.loads(Path(prepared['plan']['run_spec']).read_text())
    manifests = {arm: spec['paperload'][index]['manifest'] for index, arm in enumerate(('A', 'B'))}
    for arm, path in manifests.items():
        manifests[arm] = str(Path(path).resolve())

    if prepared.get('workload') == 'equal_samples':
        duration_seconds = prepared['duration_seconds']
        drain_seconds = prepared.get('drain_seconds', 20)
        grace_seconds = prepared.get('grace_seconds', 10)
        wall_seconds = prepared.get('wall_seconds', duration_seconds + drain_seconds + grace_seconds)
        eff_workload_seconds = workload_seconds if workload_seconds is not None else wall_seconds
        workload = {
            'experiment': 'E2',
            'arm': prepared['arm'],
            'phase': prepared['phase_id'],
            'profile': 'pilot',
            'workload': 'equal_samples',
            'workload_type': 'equal_samples',
            'seed': prepared['seed'],
            'samples_per_message': prepared['samples_per_message'],
            'duration_seconds': duration_seconds,
            'drain_seconds': drain_seconds,
            'grace_seconds': grace_seconds,
            'wall_seconds': wall_seconds,
            'context_sha256': prepared['context']['context_sha256'],
            'expected_counts': prepared['expected_counts'],
            'context': prepared['context'],
            'generator_colocated': 'unverified',
        }
    else:
        eff_workload_seconds = workload_seconds if workload_seconds is not None else 30
        if prepared['profile'] == 'official':
            a, b = prepared['manifests']['A'], prepared['manifests']['B']
            offered = a['traffic']['total_rate_per_second']
            duration = a['phases']['measure_seconds']
            workload = {'experiment': 'E2', 'arm': prepared['arm'], 'phase': prepared['phase_id'],
                'profile': 'official', 'workload': 'e2-telemetry-ab-official', 'seed': prepared['seed'],
                'samples_per_message': prepared['samples_per_message'],
                'producer_rates_per_second': {'A': a['traffic']['rate_per_second'],
                                              'B': b['traffic']['rate_per_second']},
                'offered_rate_per_second': offered, 'measure_seconds': duration,
                'burst_messages': b['traffic']['burst_messages'],
                'pending_limits': {'pending_messages': a['limits']['pending_messages'],
                                   'pending_bytes': a['limits']['pending_bytes']},
                'expected_counts': prepared['plan']['expected_counts'],
                'estimate': prepared['official_estimate'], 'durability_mode': 'synthetic-load',
                'generator_colocated': 'unverified'}
            if 'retry_policy' in a:
                workload['retry_policy'] = dict(a['retry_policy'])
            eff_workload_seconds = duration + 60 if workload_seconds is None else workload_seconds
        else:
            a, b = prepared['manifests']['A'], prepared['manifests']['B']
            official = prepared['profile'] == 'official'
            workload = {'experiment': 'E2', 'arm': prepared['arm'], 'phase': prepared['phase_id'],
                'profile': 'official' if official else 'pilot',
                'workload': 'e2-telemetry-ab-official' if official else 'e2-telemetry-ab-pilot',
                'seed': prepared['seed'], 'samples_per_message': prepared['samples_per_message'],
                'unique_batches': prepared['plan']['expected_counts']['unique'] if official else 24,
                'rate_per_second': a['traffic']['total_rate_per_second'] if official else 1,
                'measure_seconds': a['phases']['measure_seconds'] if official else 8,
                'offered_messages': prepared['plan']['expected_counts']['unique'] if official else 24,
                'generator_colocated': 'unverified'}
    return paper_p8_bundle.export_bundle(output_leaf=prepared['bundle_root'],
        session_id=prepared['session_id'], experiment='E2', arm=prepared['arm'],
        phase=prepared['phase_id'], manifests=manifests, identity_files=identities,
        gateway_identity_files=prepared['gateway_files'], gateway_env_files={}, ca_files=ca_files,
        role_metadata=role_metadata, wait_seconds=wait_seconds,
        workload_seconds=eff_workload_seconds, topology={'gateways': 2, 'sensors': 2,
            'sensors_per_gateway': 1, 'users': 0, 'viewers': 0}, workload=workload,
        run_spec=prepared['plan']['run_spec'])


class EqualSamplesCompactSampler:
    """Bounded compact rolling sampler for equal_samples server-side metrics.

    Maintains baseline and terminal snapshots strictly outside the ring buffer.
    Retains a finite rolling window (up to 20,000 snapshots) in a rolling deque,
    evicting the oldest entries when either the snapshot count or the explicit byte cap
    is exceeded. Records coverage, timestamps, errors, and evicted count/bytes.
    """
    def __init__(self, *, scrape, interval=1.0, max_snapshots=3600, byte_cap=2 * 1024 * 1024,
                 deadline_seconds=1800.0, baseline_metrics=None, baseline_resources=None,
                 resource_reader=None, time_fn=time.monotonic,
                 wall_fn=lambda: datetime.now(timezone.utc).isoformat()):
        if not (0.1 <= interval <= 5.0):
            raise ValueError('sampler_interval_out_of_bounds')
        if not (2 <= max_snapshots <= 20000):
            raise ValueError('sampler_max_snapshots_out_of_bounds')
        if not (1024 <= byte_cap <= 16 * 1024 * 1024):
            raise ValueError('sampler_byte_cap_out_of_bounds')
        if not (0.1 <= deadline_seconds <= 86400.0):
            raise ValueError('sampler_deadline_out_of_bounds')

        self.scrape = scrape
        self.resource_reader = resource_reader
        self.interval = float(interval)
        self.max_snapshots = int(max_snapshots)
        self.byte_cap = int(byte_cap)
        self.deadline_seconds = float(deadline_seconds)
        self.time_fn = time_fn
        self.wall_fn = wall_fn

        self._started_monotonic = None
        self._started_wall = None
        self._ring = collections.deque()
        self.evicted_count = 0
        self.evicted_bytes = 0
        self.total_captured = 0
        self.current_ring_bytes = 0
        self._errors = []
        self.max_errors = 16
        self._lock = threading.RLock()
        self._stop_result = None
        self._stop_lock = threading.Lock()

        self.baseline_metrics = self._project_metrics(baseline_metrics) if baseline_metrics else None
        self.baseline_resources = self._project_resources(baseline_resources) if baseline_resources else None
        self.terminal_metrics = None
        self.terminal_resources = None

        self._stop = threading.Event()
        self._thread = None
        self.side_sampler = None

    @staticmethod
    def _project_metrics(raw):
        if not isinstance(raw, dict):
            return None
        proj = {
            'process_instance': raw.get('process_instance')[:128]
                if isinstance(raw.get('process_instance'), str) else None,
            'timestamp': raw.get('timestamp')[:64]
                if isinstance(raw.get('timestamp'), str) else None,
            'monotonic': raw.get('monotonic')
                if type(raw.get('monotonic')) in (int, float) else None,
        }
        for key in ('scrape_started_timestamp', 'scrape_finished_timestamp'):
            if isinstance(raw.get(key), str):
                proj[key] = raw[key][:64]
        for key in ('scrape_started_monotonic', 'scrape_finished_monotonic'):
            value = raw.get(key)
            if type(value) in (int, float) and math.isfinite(value) and value >= 0:
                proj[key] = value
        ing_raw = raw.get('ingress')
        if isinstance(ing_raw, dict):
            ing_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            ing = {k: ing_raw[k] for k in ing_fields if k in ing_raw and type(ing_raw[k]) in (int, float)}
            durations = ing_raw.get('Durations', ing_raw.get('durations'))
            names = ('queue_wait', 'validation', 'persistence', 'ack_publish',
                     'batch_wait', 'group_persistence')
            projected_durations = {}
            for name in names:
                hist = EqualSamplesCompactSampler._project_histogram(
                    durations.get(name) if isinstance(durations, dict) else None)
                existing = durations.get(name) if isinstance(durations, dict) else None
                projected_durations[name] = hist or (
                    {'available': False, 'reason': existing['reason'][:32]}
                    if isinstance(existing, dict) and existing.get('available') is False
                    and existing.get('reason') in ('missing', 'invalid') else
                    {'available': False, 'reason':
                     'missing' if not isinstance(durations, dict) or name not in durations else 'invalid'})
            ing['durations'] = projected_durations
            proj['ingress'] = ing
        else:
            proj['ingress'] = {}

        pers_raw = raw.get('persistence')
        if isinstance(pers_raw, dict):
            pers_fields = (
                'confirmed_commits', 'single_attempts', 'group_attempts',
                'group_members', 'group_rows', 'group_payload_bytes',
                'fallback_attempts', 'begin_attempts', 'begin_succeeded',
                'rollback_attempts', 'confirmed_rollbacks', 'commit_unconfirmed',
                'copy_calls', 'copy_returned_rows', 'confirmed_new_rows',
                'fallback_calls', 'rollback_calls', 'batch_wait_timeouts',
                'groups_flushed',
            )
            pers = {k: pers_raw[k] for k in pers_fields if k in pers_raw and type(pers_raw[k]) in (int, float)}
            duration_names = ('begin', 'gateway_lock', 'sensor_lock', 'graph_lookup',
                'entity_lock', 'state_lock', 'ledger', 'copy', 'reported_state', 'temporal', 'commit')
            durations_raw = pers_raw.get('durations')
            projected_durations = {}
            for name in duration_names:
                hist = EqualSamplesCompactSampler._project_histogram(
                    durations_raw.get(name) if isinstance(durations_raw, dict) else None)
                existing = durations_raw.get(name) if isinstance(durations_raw, dict) else None
                projected_durations[name] = hist or (
                    {'available': False, 'reason': existing['reason'][:32]}
                    if isinstance(existing, dict) and existing.get('available') is False
                    and existing.get('reason') in ('missing', 'invalid') else
                    {'available': False, 'reason':
                     'missing' if not isinstance(durations_raw, dict) or name not in durations_raw else 'invalid'})
            pers['durations'] = projected_durations
            for hist_k in ('batch_wait_latency', 'persistence_latency'):
                if hist_k in pers_raw and isinstance(pers_raw[hist_k], dict):
                    hist = EqualSamplesCompactSampler._project_histogram(pers_raw[hist_k])
                    if hist:
                        pers[hist_k] = hist
            proj['persistence'] = pers
        else:
            proj['persistence'] = {}

        db_raw = raw.get('db')
        db_fields = ('acquired', 'idle', 'total', 'max', 'acquire_count',
                     'empty_acquire_count', 'canceled_acquire_count', 'acquire_seconds')
        proj['db'] = {k: db_raw[k] for k in db_fields if isinstance(db_raw, dict)
                      and type(db_raw.get(k)) in (int, float)
                      and math.isfinite(db_raw[k]) and db_raw[k] >= 0}

        if 'resources' in raw and isinstance(raw['resources'], dict):
            proj['resources'] = EqualSamplesCompactSampler._project_resources(raw['resources'])

        return proj

    @staticmethod
    def _project_histogram(raw):
        """Project the backend HistogramSnapshot DTO with bounded, finite arrays."""
        if not isinstance(raw, dict):
            return None
        bounds, buckets = raw.get('upper_seconds'), raw.get('buckets')
        count, overflow, total = raw.get('count'), raw.get('overflow'), raw.get('sum_seconds')
        if (not isinstance(bounds, list) or not isinstance(buckets, list)
                or len(bounds) != len(buckets) or len(bounds) > 32
                or type(count) is not int or count < 0
                or type(overflow) is not int or overflow < 0
                or type(total) not in (int, float) or not math.isfinite(total)
                or any(type(x) not in (int, float) or not math.isfinite(x) or x < 0 for x in bounds)
                or any(type(x) is not int or x < 0 for x in buckets)
                or any(bounds[i] >= bounds[i + 1] for i in range(len(bounds) - 1))
                or sum(buckets) + overflow != count):
            return None
        return {'available': True, 'upper_seconds': bounds, 'buckets': buckets,
                'count': count, 'overflow': overflow, 'sum_seconds': total}

    @staticmethod
    def _project_resources(raw):
        if not isinstance(raw, dict):
            return None
        source_svcs = raw.get('services') if 'services' in raw and isinstance(raw['services'], dict) else raw
        projected = {}
        canonical = {
            'backend': 'backend',
            'postgres': 'postgres',
            'db': 'postgres',
            'mosquitto': 'mosquitto',
            'broker': 'mosquitto',
            'nginx': 'nginx',
            'proxy': 'nginx',
        }
        for index, (k, v) in enumerate(source_svcs.items()):
            if index >= 8:
                break
            if isinstance(v, dict) and k in canonical:
                canon_k = canonical[k]
                row = {field: v[field] for field in ('cpu_cumulative_seconds', 'memory_bytes')
                       if type(v.get(field)) in (int, float)}
                if type(v.get('available')) is bool:
                    row['available'] = v['available']
                if isinstance(v.get('reason'), str):
                    row['reason'] = v['reason'][:80]
                projected[canon_k] = row
        return projected

    def _error(self, code):
        safe = str(code)[:96]
        with self._lock:
            if safe not in self._errors and len(self._errors) < self.max_errors:
                self._errors.append(safe)

    @property
    def errors(self):
        with self._lock:
            return list(self._errors)

    def sample_step(self, phase='sample'):
        if self._started_monotonic is None:
            self._started_monotonic = self.time_fn()
            self._started_wall = self.wall_fn()

        now_mono = self.time_fn()
        now_wall = self.wall_fn()

        if now_mono - self._started_monotonic > self.deadline_seconds:
            self._error('collector_deadline_exceeded')
            return

        raw_metrics = None
        scrape_started_mono, scrape_started_wall = self.time_fn(), self.wall_fn()
        if self.scrape:
            try:
                raw_metrics = self.scrape()
            except Exception as exc:
                self._error('scrape_' + type(exc).__name__)
        scrape_finished_mono, scrape_finished_wall = self.time_fn(), self.wall_fn()

        # Project metrics immediately after HTTP scrape, BEFORE resource reader
        proj_metrics = self._project_metrics(raw_metrics) or {}

        # Notify side sampler for EVERY valid projected HTTP scrape, BEFORE resource reader
        # and BEFORE retention/ring-cap rejection
        if (self.side_sampler is not None and raw_metrics is not None
                and isinstance(proj_metrics.get('ingress'), dict)
                and proj_metrics.get('ingress')
                and proj_metrics.get('process_instance')
                and scrape_finished_mono is not None
                and scrape_finished_wall is not None):
            # Build observation snapshot with required fields (using local scrape finish times)
            obs_snapshot = {
                'process_instance': proj_metrics['process_instance'],
                'ingress': proj_metrics['ingress'],
                'scrape_finished_monotonic': scrape_finished_mono,
                'scrape_finished_timestamp': scrape_finished_wall,
            }
            try:
                self.side_sampler.observe_ingress(obs_snapshot)
            except Exception:
                # Side sampler errors are handled internally; don't block metrics collection
                pass

        raw_res = None
        if self.resource_reader:
            try:
                raw_res = self.resource_reader()
            except Exception as exc:
                self._error('resource_read_' + type(exc).__name__)

        proj_res = self._project_resources(raw_res) if raw_res else None

        snapshot = {
            'phase': phase,
            'timestamp': now_wall,
            'monotonic': now_mono,
            'scrape_started_timestamp': scrape_started_wall,
            'scrape_started_monotonic': scrape_started_mono,
            'scrape_finished_timestamp': scrape_finished_wall,
            'scrape_finished_monotonic': scrape_finished_mono,
            'process_instance': proj_metrics.get('process_instance'),
            'ingress': proj_metrics.get('ingress', {}),
            'persistence': proj_metrics.get('persistence', {}),
            'db': proj_metrics.get('db', {}),
        }
        if proj_res is not None:
            snapshot['resources'] = proj_res

        snap_size = len(json.dumps(snapshot, separators=(',', ':')).encode('utf-8'))
        snapshot['_size'] = snap_size
        snap_size = len(json.dumps(snapshot, separators=(',', ':')).encode('utf-8'))

        if snap_size > self.byte_cap:
            with self._lock:
                self.total_captured += 1
            self._error('collector_snapshot_exceeds_byte_cap')
            return

        with self._lock:
            if len(self._ring) >= self.max_snapshots:
                oldest = self._ring.popleft()
                self.evicted_count += 1
                self.evicted_bytes += oldest.get('_size', 0)
                self.current_ring_bytes -= oldest.get('_size', 0)
            while self.current_ring_bytes + snap_size > self.byte_cap and self._ring:
                oldest = self._ring.popleft()
                self.evicted_count += 1
                self.evicted_bytes += oldest.get('_size', 0)
                self.current_ring_bytes -= oldest.get('_size', 0)
            self._ring.append(snapshot)
            self.total_captured += 1
            self.current_ring_bytes += snap_size

    def start_manual(self):
        self._started_monotonic = self.time_fn()
        self._started_wall = self.wall_fn()
        self.sample_step('workload_sample_0')

    def start(self, process_pid=None):
        if self._started_monotonic is None:
            self.start_manual()
        def loop():
            idx = 0
            while not self._stop.wait(self.interval):
                idx += 1
                self.sample_step(f'workload_sample_{idx}')
                if 'collector_deadline_exceeded' in self.errors:
                    break
        self._thread = threading.Thread(target=loop, name='e2-compact-sampler', daemon=True)
        self._thread.start()

    def stop(self, process_pid=None, *, timeout_seconds=10.0):
        with self._stop_lock:
            if self._stop_result is not None:
                return self._stop_result
            self._stop.set()
            if self.side_sampler is not None:
                self.side_sampler._stop.set()
            deadline = time.monotonic() + max(0.0, float(timeout_seconds))
            if self._thread:
                self._thread.join(timeout=max(0.0, deadline - time.monotonic()))
                metrics_joined = not self._thread.is_alive()
            else:
                metrics_joined = True
            side_joined = True
            if self.side_sampler is not None:
                side_joined = self.side_sampler.stop(timeout_seconds=max(0.0, deadline - time.monotonic()))
            if not metrics_joined:
                self._error('collector_join_timeout')
            if not side_joined:
                self._error('side_sampler_join_timeout')
            joined = metrics_joined and side_joined
            if joined:
                self._stop_result = True
            return joined

    def record_terminal(self, terminal_metrics, terminal_resources):
        with self._lock:
            self.terminal_metrics = self._project_metrics(terminal_metrics) if terminal_metrics else None
            self.terminal_resources = self._project_resources(terminal_resources) if terminal_resources else None

    @property
    def retained_snapshots(self):
        with self._lock:
            return [dict(s) for s in self._ring]

    @property
    def metrics(self):
        with self._lock:
            res = []
            if self.baseline_metrics:
                res.append({'phase': 'workload_start', 'snapshot': dict(self.baseline_metrics)})
            for s in self._ring:
                res.append({'phase': s.get('phase', 'sample'), 'snapshot': dict(s)})
            if self.terminal_metrics:
                res.append({'phase': 'workload_terminal', 'snapshot': dict(self.terminal_metrics)})
            return res

    @property
    def resources(self):
        with self._lock:
            res = []
            if self.baseline_resources:
                res.append({'phase': 'workload_start', 'services': self.baseline_resources})
            for s in self._ring:
                if 'resources' in s:
                    res.append({'phase': s.get('phase', 'sample'), 'services': s['resources']})
            if self.terminal_resources:
                res.append({'phase': 'workload_terminal', 'services': self.terminal_resources})
            return res

    def get_coverage(self):
        with self._lock:
            return {
                'sampling_interval_seconds': self.interval,
                'deadline_seconds': self.deadline_seconds,
                'snapshots_captured': self.total_captured,
                'snapshots_retained': len(self._ring),
                'snapshots_evicted': self.evicted_count,
                'evicted_bytes': self.evicted_bytes,
                'ring_byte_cap': self.byte_cap,
                'ring_bytes_used': self.current_ring_bytes,
                'collector_errors': list(self._errors),
            }


class IndependentSideSampler:
    """One bounded slow-reader thread, independent of the HTTP metrics sampler.

    Supports optional active capture mode that preserves evidence during the
    critical window after ingress trigger (via observe_ingress), while maintaining
    backward compatibility with the existing rolling behavior when capture options are absent.
    """
    def __init__(self, *, reader, interval=1.0, max_samples=64, byte_cap=512 * 1024,
                 deadline_seconds=None, capture_seconds=None, baseline_metrics=None,
                 time_fn=None, wall_fn=None):
        if type(max_samples) is not int or max_samples <= 0 or max_samples > 20000:
            raise ValueError('side_sampler_max_samples_invalid')
        if type(byte_cap) is not int or byte_cap <= 0 or byte_cap > 16 * 1024 * 1024:
            raise ValueError('side_sampler_byte_cap_invalid')
        if not (0.001 <= interval <= 5.0):
            raise ValueError('side_sampler_interval_out_of_bounds')

        self.reader = reader
        self.interval = float(interval)
        self.max_samples = int(max_samples)
        self.byte_cap = int(byte_cap)
        self.time_fn = time_fn if time_fn is not None else time.monotonic
        self.wall_fn = wall_fn if wall_fn is not None else (lambda: datetime.now(timezone.utc).isoformat())
        # Deadline uses the same clock as sample timestamps (injected or real)
        self._start_time = self.time_fn()
        if deadline_seconds is not None:
            if type(deadline_seconds) not in (int, float) or isinstance(deadline_seconds, bool):
                raise ValueError('deadline_seconds_must_be_number')
            if not math.isfinite(deadline_seconds) or deadline_seconds < 0:
                raise ValueError('deadline_seconds_must_be_finite_nonnegative')
        self._deadline_seconds = None if deadline_seconds is None else max(0.0, float(deadline_seconds))
        self.deadline = None if self._deadline_seconds is None else self._start_time + self._deadline_seconds

        # Validate capture options
        if capture_seconds is not None:
            if type(capture_seconds) not in (int, float) or isinstance(capture_seconds, bool):
                raise ValueError('capture_seconds_must_be_number')
            if not math.isfinite(capture_seconds) or capture_seconds <= 0:
                raise ValueError('capture_seconds_must_be_finite_positive')
            # Note: capture_seconds can exceed deadline_seconds; runtime will cap capture window
        self.capture_seconds = capture_seconds

        # Validate baseline_metrics - store only minimal scalar copy
        if baseline_metrics is not None:
            if not isinstance(baseline_metrics, dict):
                raise ValueError('baseline_metrics_must_be_dict')
            ingress = baseline_metrics.get('ingress')
            if not isinstance(ingress, dict):
                raise ValueError('baseline_metrics_missing_ingress')
            received = ingress.get('Received')
            if type(received) is not int or isinstance(received, bool) or received < 0:
                raise ValueError('baseline_metrics_received_must_be_nonnegative_int')
            # Store process_instance if present for trigger validation
            proc_inst = baseline_metrics.get('process_instance')
            if proc_inst is not None and not isinstance(proc_inst, str):
                raise ValueError('baseline_metrics_process_instance_must_be_string')
            # Keep only minimal scalar copy
            self.baseline_metrics = {
                'ingress': {'Received': received},
                'process_instance': proc_inst
            }
        else:
            self.baseline_metrics = None

        # Internal state
        self._ring = collections.deque()
        self._bytes = 0
        self.evicted_count = 0
        self.evicted_bytes = 0
        self.total_captured = 0
        self.errors = []
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread = None
        self._stop_result = None

        # Active capture state (triggered ONLY by observe_ingress)
        self._triggered = False
        self._trigger_monotonic = None
        self._trigger_wall = None
        self._capture_end_monotonic = None
        self._capture_end_wall = None  # Derived once at trigger: trigger_wall + capture_seconds delta
        self._active = False
        self._frozen = False
        self._pre_trigger_evictions = 0
        self._active_truncated = False
        self._active_truncated_dropped = 0
        self._state = 'waiting'  # waiting, active_capture, capture_complete, no_trigger, early_stop, deadline
        self._freeze_monotonic = None
        self._freeze_wall = None
        self._freeze_reason = None  # natural_end, deadline, active_count_cap, active_byte_cap, early_stop

        # Last observed ingress state for counter reset detection
        self._last_observed_received = None
        self._last_observed_process_instance = None

        # PARENT B: Dedicated sticky qualification flags (not just errors list)
        self._qualification_reasons = set()  # e.g., 'process_instance_changed', 'counter_reset', 'baseline_missing_process_instance'

        # PARENT A: Track if capture end was clipped by deadline vs natural
        self._capture_end_clipped_by_deadline = False

        # PARENT E: Track if we're in legacy mode (no capture options).
        # Legacy = capture_seconds is None ONLY. baseline_metrics absence is a
        # diagnostics qualification (baseline_unavailable), NOT a legacy/active
        # mode switch. Active capture is selected by capture_seconds alone.
        self._legacy_mode = (capture_seconds is None)

    def observe_ingress(self, snapshot):
        """SOLE trigger mechanism. Call from HTTP collector with projected snapshot.

        Snapshot must contain:
        - process_instance: non-empty string (must match baseline if baseline has one)
        - ingress: dict with 'Received' (int >= 0)
        - scrape_finished_monotonic: finite numeric (non-bool) from Host A
        - scrape_finished_timestamp: valid bounded ISO UTC timestamp string

        Only Received increase above baseline (or last observation) triggers capture.
        Equal Received = idle (no trigger, no error). Decrease = counter_reset qualification.
        Process instance change = process_instance_changed qualification.
        Validation continues after trigger for counter reset/process change detection.
        """
        if self.capture_seconds is None:
            return  # Legacy mode: no capture requested

        with self._lock:
            if self._frozen:
                return  # Frozen - no more observations

            # Validate snapshot structure
            if not isinstance(snapshot, dict):
                self._add_error('observe_ingress_snapshot_must_be_dict')
                return

            # process_instance: non-empty string
            proc_inst = snapshot.get('process_instance')
            if not isinstance(proc_inst, str) or not proc_inst:
                self._add_error('observe_ingress_process_instance_missing_or_invalid')
                return

            # ingress: dict with Received (int >= 0)
            ingress = snapshot.get('ingress')
            if not isinstance(ingress, dict):
                self._add_error('observe_ingress_missing_ingress')
                return
            received = ingress.get('Received')
            # Must be numeric (int or float), not bool
            if not isinstance(received, (int, float)) or isinstance(received, bool):
                self._add_error('observe_ingress_received_must_be_nonnegative_int')
                return
            # Must be finite
            if not math.isfinite(received):
                self._add_error('observe_ingress_received_must_be_finite')
                return
            # Must be int (not float)
            if type(received) is not int:
                self._add_error('observe_ingress_received_must_be_nonnegative_int')
                return
            if received < 0:
                self._add_error('observe_ingress_received_must_be_nonnegative_int')
                return

            # scrape_finished_monotonic: finite numeric non-bool
            scrape_mono = snapshot.get('scrape_finished_monotonic')
            if type(scrape_mono) not in (int, float) or isinstance(scrape_mono, bool):
                self._add_error('observe_ingress_scrape_monotonic_must_be_numeric')
                return
            if not math.isfinite(scrape_mono):
                self._add_error('observe_ingress_scrape_monotonic_must_be_finite')
                return

            # scrape_finished_timestamp: valid bounded ISO UTC timestamp
            scrape_wall = snapshot.get('scrape_finished_timestamp')
            if not isinstance(scrape_wall, str):
                self._add_error('observe_ingress_scrape_timestamp_missing_or_invalid')
                return
            # PARENT C: Proper ISO UTC parsing with validation
            try:
                if scrape_wall.endswith('Z'):
                    dt = datetime.fromisoformat(scrape_wall[:-1] + '+00:00')
                elif '+' in scrape_wall[-6:] or '-' in scrape_wall[-6:]:
                    dt = datetime.fromisoformat(scrape_wall)
                    if dt.tzinfo is None:
                        self._add_error('observe_ingress_scrape_timestamp_must_be_utc')
                        return
                    if dt.utcoffset() != timedelta(0):
                        self._add_error('observe_ingress_scrape_timestamp_must_be_utc')
                        return
                else:
                    self._add_error('observe_ingress_scrape_timestamp_must_be_utc')
                    return
                # Check for reasonable bounds (year 1970-2100)
                if dt.year < 1970 or dt.year > 2100:
                    self._add_error('observe_ingress_scrape_timestamp_must_be_utc')
                    return
            except Exception:
                self._add_error('observe_ingress_scrape_timestamp_must_be_utc')
                return

            # Baseline unavailable: active capture requested but no baseline.
            # Sticky diagnostics qualification, no trigger, state stays waiting.
            if self.baseline_metrics is None:
                self._add_qualification('baseline_unavailable')
                return

            # Check process_instance match with baseline
            baseline_proc = self.baseline_metrics.get('process_instance')
            if baseline_proc is not None:
                if proc_inst != baseline_proc:
                    self._add_qualification('process_instance_changed')
                    self._last_observed_process_instance = proc_inst
                    return  # Process instance mismatch - no trigger, no counter reset check
            else:
                # PARENT D: Baseline has no process_instance - cannot infer sameness
                self._add_qualification('baseline_missing_process_instance')
                return

            baseline_received = self.baseline_metrics['ingress']['Received']

            # Check for counter reset (decrease from last observation or baseline)
            if self._last_observed_received is not None:
                if received < self._last_observed_received:
                    self._add_qualification('counter_reset')
                    self._last_observed_received = received
                    return
            elif received < baseline_received:
                self._add_qualification('counter_reset')
                self._last_observed_received = received
                return

            # Update last observed
            self._last_observed_received = received
            self._last_observed_process_instance = proc_inst

            # Equal = idle, not trigger
            if received <= baseline_received:
                return  # Idle, no trigger, no error

            # Valid trigger: increase above baseline (only if not already triggered)
            if not self._triggered:
                self._triggered = True
                self._trigger_monotonic = scrape_mono
                self._trigger_wall = scrape_wall
                self._capture_end_monotonic = scrape_mono + self.capture_seconds
                # Cap by deadline
                if self.deadline is not None and self._capture_end_monotonic > self.deadline:
                    self._capture_end_monotonic = self.deadline
                    self._capture_end_clipped_by_deadline = True
                else:
                    self._capture_end_clipped_by_deadline = False
                # Derive capture_end_wall once from trigger_wall + effective delta (UTC)
                delta = self._capture_end_monotonic - scrape_mono
                self._capture_end_wall = self._add_seconds_to_utc(scrape_wall, delta)
                self._active = True
                self._state = 'active_capture'

    def _add_qualification(self, reason):
        """Add a qualification reason (sticky, deduplicated, bounded)."""
        self._qualification_reasons.add(reason)
        # Also add to errors for backward compatibility, but with bounding
        if len(self.errors) < 16:
            self.errors.append(reason)

    def _add_error(self, error_code):
        """Add error with bounding (max 16). Observe_ingress validation errors are not deduplicated."""
        if len(self.errors) < 16:
            self.errors.append(error_code)

    @staticmethod
    def _add_seconds_to_utc(utc_timestamp, seconds):
        """Add seconds to UTC timestamp string, return new UTC timestamp string."""
        try:
            # Parse ISO format with Z or offset
            if utc_timestamp.endswith('Z'):
                dt = datetime.fromisoformat(utc_timestamp[:-1] + '+00:00')
            else:
                dt = datetime.fromisoformat(utc_timestamp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            new_dt = dt + timedelta(seconds=seconds)
            return new_dt.isoformat().replace('+00:00', 'Z')
        except Exception:
            # Fallback: return original (should not happen with validated input)
            return utc_timestamp

    @staticmethod
    def _timed(reader, time_fn, wall_fn):
        start_utc, start_mono = wall_fn(), time_fn()
        try:
            value = reader()
            error = None
        except Exception:
            value, error = None, 'reader_failed'
        end_mono, end_utc = time_fn(), wall_fn()
        return {'start_utc': start_utc, 'end_utc': end_utc,
                'start_monotonic_seconds': start_mono, 'end_monotonic_seconds': end_mono,
                'value': value, 'error': error}

    def sample_step(self):
        # BUG 3 FIX: Check frozen at VERY BEGINNING, before ANY state mutations
        if self._frozen:
            return

        now_mono = self.time_fn()
        now_wall = self.wall_fn()

        # Check deadline (uses same clock as sample timestamps)
        if self.deadline is not None and now_mono >= self.deadline:
            with self._lock:
                # BUG 3 FIX: Only mutate state if NOT already frozen (double-check under lock)
                if self._frozen:
                    return
                if not self._triggered:
                    self._state = 'deadline'
                elif self._active and not self._frozen:
                    self._state = 'capture_complete'
                self._freeze_if_needed(now_mono, now_wall, reason='deadline')
            return

        # Check active capture window end
        if self._active and self._capture_end_monotonic is not None:
            if now_mono >= self._capture_end_monotonic:
                with self._lock:
                    if self._frozen:
                        return
                    self._active = False
                    self._state = 'capture_complete'
                    self._freeze_if_needed(now_mono, now_wall, reason='natural_end')
                return

        # Also check deadline during active capture
        if self._active and self.deadline is not None and now_mono >= self.deadline:
            with self._lock:
                if self._frozen:
                    return
                self._active = False
                self._state = 'capture_complete'
                self._freeze_if_needed(now_mono, now_wall, reason='deadline')
            return

        # Perform the timed reads - check before EACH reader
        def active_check():
            return not self._stop.is_set() and not self._frozen and \
                   (self.deadline is None or self.time_fn() < self.deadline) and \
                   (not self._active or self._capture_end_monotonic is None or self.time_fn() < self._capture_end_monotonic)

        if not active_check():
            return

        # Track whether capture end was crossed during reader execution
        capture_end_crossed = False

        # PG reader - always allowed to start if active_check passes
        pg = self._timed(self.reader['postgres'], self.time_fn, self.wall_fn)

        # Check if capture end was crossed during PG execution
        if self._active and self._capture_end_monotonic is not None:
            if pg['end_monotonic_seconds'] >= self._capture_end_monotonic:
                capture_end_crossed = True

        # Linux reader - only start if capture end not crossed
        if capture_end_crossed:
            # Create unavailable envelope for Linux
            linux_start_mono = self.time_fn()
            linux_start_utc = self.wall_fn()
            linux = {
                'start_utc': linux_start_utc,
                'end_utc': linux_start_utc,
                'start_monotonic_seconds': linux_start_mono,
                'end_monotonic_seconds': linux_start_mono,
                'value': None,
                'error': 'capture_window_ended'
            }
        else:
            if not active_check():
                return
            linux = self._timed(self.reader['linux'], self.time_fn, self.wall_fn)
            # Check if capture end was crossed during Linux execution
            if self._active and self._capture_end_monotonic is not None:
                if linux['end_monotonic_seconds'] >= self._capture_end_monotonic:
                    capture_end_crossed = True

        # Docker reader - only start if capture end not crossed
        docker = None
        if self.reader.get('docker'):
            if capture_end_crossed:
                # Create unavailable envelope for Docker
                docker_start_mono = self.time_fn()
                docker_start_utc = self.wall_fn()
                docker = {
                    'start_utc': docker_start_utc,
                    'end_utc': docker_start_utc,
                    'start_monotonic_seconds': docker_start_mono,
                    'end_monotonic_seconds': docker_start_mono,
                    'value': None,
                    'error': 'capture_window_ended'
                }
            else:
                if not active_check():
                    return
                docker = self._timed(self.reader['docker'], self.time_fn, self.wall_fn)
                # Check if capture end was crossed during Docker execution
                if self._active and self._capture_end_monotonic is not None:
                    if docker['end_monotonic_seconds'] >= self._capture_end_monotonic:
                        capture_end_crossed = True
                # BUG 2 FIX: Do NOT return early after Docker reader completes.
                # If all started readers completed (even if clock advanced past capture end),
                # we still append the sample. The active_check before each reader START
                # prevents new readers from starting after capture end.
                docker['value'] = EqualSamplesCompactSampler._project_resources(docker['value'])

        # Build sample with FULL PG value preserved (no filtering)
        sample = {'postgres': pg, 'linux': linux}
        if docker is not None:
            sample['docker'] = docker

        # If capture end was crossed during any reader, we have a partial sample
        # Mark qualification for incomplete source coverage
        # BUG 2 FIX: Only add qualification if some reader was PREVENTED from starting,
        # not if all started readers completed (even if they ended after capture end).
        # We track which sources had unavailable envelopes.
        if capture_end_crossed:
            # Check if any source has 'capture_window_ended' error (was not started)
            has_unstarted = False
            if linux.get('error') == 'capture_window_ended':
                has_unstarted = True
            if docker is not None and docker.get('error') == 'capture_window_ended':
                has_unstarted = True
            if has_unstarted:
                self._add_qualification('inflight_partial_sample')

        try:
            size = len(json.dumps(sample, separators=(',', ':'), allow_nan=False).encode())
        except (TypeError, ValueError):
            sample = {'postgres': {**{k: pg[k] for k in ('start_utc','end_utc','start_monotonic_seconds','end_monotonic_seconds')}, 'error':'reader_payload_invalid','value':None},
                      'linux': {**{k: linux[k] for k in ('start_utc','end_utc','start_monotonic_seconds','end_monotonic_seconds')}, 'error':'reader_payload_invalid','value':None}}
            size = len(json.dumps(sample, separators=(',', ':')).encode())

        while True:
            sample['_size'] = size
            exact_size = len(json.dumps(sample, separators=(',', ':'), allow_nan=False).encode())
            if exact_size == size:
                break
            size = exact_size

        with self._lock:
            # Recheck frozen state under lock after readers completed
            if self._frozen:
                return

            self.total_captured += 1

            # Single sample exceeds total byte_cap -> truncate/freeze (only in capture mode)
            if size > self.byte_cap:
                if self._legacy_mode:
                    return  # Legacy mode: skip oversized sample, continue
                if not self._active_truncated:
                    self._active_truncated = True
                    self._active_truncated_dropped = 0
                self._active_truncated_dropped += 1
                # BUG 1 FIX: Use actual current time for freeze, not stale now_mono
                freeze_mono = self.time_fn()
                freeze_wall = self.wall_fn()
                self._freeze_if_needed(freeze_mono, freeze_wall, reason='active_byte_cap')
                return

            # Determine pre-trigger buffer limit
            if self.capture_seconds is not None and not self._triggered:
                pre_trigger_limit = min(4, self.max_samples)
                pre_trigger_byte_limit = min(64 * 1024, self.byte_cap)
            else:
                pre_trigger_limit = self.max_samples
                pre_trigger_byte_limit = self.byte_cap

            # Eviction logic
            if not self._triggered:
                # Pre-trigger: bounded by min(4, max_samples) and min(64KiB, byte_cap)
                # Single oversized pre-trigger sample must not be appended even if total cap permits
                if size > pre_trigger_byte_limit:
                    return  # Drop oversized pre-trigger sample
                while self._ring and (len(self._ring) >= pre_trigger_limit or self._bytes + size > pre_trigger_byte_limit):
                    old = self._ring.popleft()
                    self._bytes -= old['_size']
                    self.evicted_count += 1
                    self.evicted_bytes += old['_size']
                    self._pre_trigger_evictions += 1
            else:
                # Post-trigger: bounded by max_samples and byte_cap (retained evidence not evicted)
                # But if we exceed caps, we truncate from the end (keep earliest) and FREEZE
                if len(self._ring) >= self.max_samples or self._bytes + size > self.byte_cap:
                    if not self._active_truncated:
                        self._active_truncated = True
                        self._active_truncated_dropped = 0
                    # Count how many would be dropped
                    self._active_truncated_dropped += 1
                    # Freeze on truncation - determine reason
                    freeze_reason = 'active_count_cap' if len(self._ring) >= self.max_samples else 'active_byte_cap'
                    # BUG 1 FIX: Use actual current time for freeze
                    freeze_mono = self.time_fn()
                    freeze_wall = self.wall_fn()
                    self._freeze_if_needed(freeze_mono, freeze_wall, reason=freeze_reason)
                    return

            self._ring.append(sample)
            self._bytes += size

            # If capture end was crossed during reader execution, freeze immediately with natural_end reason
            if capture_end_crossed:
                # BUG 1 FIX: Use actual current time for freeze (clock may have been advanced by readers)
                freeze_mono = self.time_fn()
                freeze_wall = self.wall_fn()
                self._freeze_if_needed(freeze_mono, freeze_wall, reason='natural_end')

    def _freeze_if_needed(self, now_mono, now_wall, reason='natural_end'):
        """Freeze evidence collection.
        BUG 1 FIX: Uses actual current time internally (self.time_fn(), self.wall_fn())
        to ensure freeze timestamps reflect the real moment of freezing, not a stale
        caller-provided time that may predate slow reader execution.
        """
        if not self._frozen:
            self._frozen = True
            # BUG 1 FIX: Use actual current time at freeze point, not stale caller args
            self._freeze_monotonic = self.time_fn()
            self._freeze_wall = self.wall_fn()
            self._freeze_reason = reason
            self._active = False  # Always deactivate on freeze
            if self._state == 'waiting':
                self._state = 'no_trigger'
            elif self._state == 'active_capture':
                self._state = 'capture_complete'

    def start(self):
        if self._thread is not None:
            return
        def loop():
            while not self._stop.is_set():
                # BUG 3 FIX: Check frozen at loop start, before any state mutations
                if self._frozen:
                    return
                now_mono = self.time_fn()
                if self.deadline is not None and now_mono >= self.deadline:
                    with self._lock:
                        # Double-check frozen under lock
                        if self._frozen:
                            return
                        if not self._triggered:
                            self._state = 'deadline'
                        elif self._active and not self._frozen:
                            self._state = 'capture_complete'
                        self._freeze_if_needed(now_mono, self.wall_fn(), reason='deadline')
                    return
                # Exit thread when naturally frozen (capture end or cap hit)
                if self._frozen:
                    return
                if self._stop.wait(self.interval):
                    with self._lock:
                        if self._state == 'waiting':
                            self._state = 'no_trigger'
                        elif self._state == 'active_capture':
                            self._state = 'early_stop'
                        self._freeze_if_needed(self.time_fn(), self.wall_fn(), reason='early_stop')
                    return
                self.sample_step()
        self._thread = threading.Thread(target=loop, name='e2-side-diagnostics', daemon=True)
        self._thread.start()

    def stop(self, *, timeout_seconds=10.0):
        if self._stop_result is True:
            return True
        with self._lock:
            if self._state == 'waiting':
                self._state = 'no_trigger'
                self._freeze_if_needed(self.time_fn(), self.wall_fn(), reason='early_stop')
            elif self._state == 'active_capture':
                # Check if natural capture end has elapsed (current time >= capture_end_monotonic)
                # This handles the case where stop is called after natural end but without sample_step at that time
                now_mono = self.time_fn()
                natural_end_reached = (self._capture_end_monotonic is not None and
                                       now_mono >= self._capture_end_monotonic and
                                       (self.deadline is None or self._capture_end_monotonic <= self.deadline + 1e-9))
                if natural_end_reached:
                    self._state = 'capture_complete'
                    self._freeze_if_needed(now_mono, self.wall_fn(), reason='natural_end')
                else:
                    self._state = 'early_stop'
                    self._freeze_if_needed(now_mono, self.wall_fn(), reason='early_stop')
            # If already frozen (capture_complete, deadline, etc.), don't change state
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(0.0, float(timeout_seconds)))
        joined = not (self._thread and self._thread.is_alive())
        if joined:
            self._stop_result = True
        return joined

    @property
    def samples(self):
        with self._lock:
            return [dict(row) for row in self._ring]

    def coverage(self):
        with self._lock:
            cov = {'samples_captured': self.total_captured, 'samples_retained': len(self._ring),
                    'samples_evicted': self.evicted_count, 'evicted_bytes': self.evicted_bytes,
                    'ring_count_cap': self.max_samples, 'ring_byte_cap': self.byte_cap,
                    'ring_bytes_used': self._bytes, 'errors': list(self.errors)}

            if self.capture_seconds is not None:
                cov['mode'] = 'active_capture'
                cov['state'] = self._state
                cov['pre_trigger_evictions'] = self._pre_trigger_evictions
                if self._triggered:
                    cov['trigger_observed'] = True
                    cov['trigger_monotonic'] = self._trigger_monotonic
                    cov['trigger_wall'] = self._trigger_wall
                    cov['capture_end_monotonic'] = self._capture_end_monotonic
                    cov['capture_end_wall'] = self._capture_end_wall  # Stable, derived at trigger
                    # capture_end_reached: true only if declared capture end was actually reached (not clipped by deadline)
                    # PARENT A: If clipped by deadline, declared end was NOT reached
                    # Also: cap freeze (active_count_cap, active_byte_cap) must not equate to declared end reached
                    # Use freeze_reason to distinguish: only natural_end (or deadline==natural_end) means reached
                    declared_end_reached = (self._capture_end_monotonic is not None and
                                           not self._capture_end_clipped_by_deadline and
                                           self._freeze_reason == 'natural_end')
                    cov['capture_end_reached'] = declared_end_reached
                else:
                    cov['trigger_observed'] = False
                if self._frozen:
                    cov['freeze_monotonic'] = self._freeze_monotonic
                    cov['freeze_wall'] = self._freeze_wall
                    cov['freeze_reason'] = self._freeze_reason
                # PARENT B: Include qualification reasons
                if self._qualification_reasons:
                    cov['qualifications'] = list(self._qualification_reasons)
                if self._active_truncated:
                    cov['truncated'] = {
                        'reason': 'active_count_cap' if len(self._ring) >= self.max_samples else 'active_byte_cap',
                        'dropped_count': self._active_truncated_dropped
                    }
                    cov['active_truncation'] = {
                        'reason': 'active_count_cap' if len(self._ring) >= self.max_samples else 'active_byte_cap',
                        'dropped_count': self._active_truncated_dropped
                    }
                    cov['complete'] = False
                else:
                    # Complete only if not truncated, no qualifications, not early_stop, and capture_end_reached
                    cov['complete'] = (not self._active and self._state == 'capture_complete' and
                                       cov.get('capture_end_reached', False) and
                                       not self._qualification_reasons)
            # Legacy mode: no 'mode' key for backward compatibility
            else:
                cov['complete'] = True

            return cov


def read_container_resources(registry, containers, *, docker_client=None, socket_path='/var/run/docker.sock', timeout=2.0, read_only=False):
    """Safe bounded Docker readback for backend/db/broker/proxy containers.

    Collects cumulative CPU seconds (nanoseconds / 1e9) and memory usage bytes.
    Never uses CPU percent as CPU seconds. If cumulative counters are unreadable or
    unsupported, records unavailable with reason, NOT fake zero.
    """
    canonical_roles = {
        'backend': 'backend',
        'postgres': 'postgres',
        'mosquitto': 'mosquitto',
        'nginx': 'nginx',
    }
    role_aliases = {
        'db': 'postgres',
        'broker': 'mosquitto',
        'proxy': 'nginx',
    }
    services = {}

    for role_name, canon_role in canonical_roles.items():
        container_entry = None
        for cand in (canon_role, role_name):
            if isinstance(containers, dict) and cand in containers:
                container_entry = containers[cand]
                break
        if not container_entry:
            for k, alias_target in role_aliases.items():
                if alias_target == canon_role and isinstance(containers, dict) and k in containers:
                    container_entry = containers[k]
                    break

        if not container_entry or not isinstance(container_entry, dict):
            services[canon_role] = {
                'cpu_cumulative_seconds': None,
                'memory_bytes': None,
                'available': False,
                'container_id': None,
                'reason': 'container_not_in_manifest',
            }
            continue

        cid = None
        if registry and hasattr(registry, 'resolve'):
            try:
                resolved = (registry.resolve(container_entry, timeout=min(2.0, timeout), read_only=True)
                            if read_only else registry.resolve(container_entry))
                if resolved and isinstance(resolved, dict):
                    cid = resolved.get('Id')
            except Exception as exc:
                services[canon_role] = {
                    'cpu_cumulative_seconds': None,
                    'memory_bytes': None,
                    'available': False,
                    'container_id': None,
                    'reason': f'ownership_resolution_{type(exc).__name__}',
                }
                continue

        if not cid and not read_only:
            receipt = container_entry.get('receipt') or {}
            cid = receipt.get('id') or container_entry.get('name')

        if not cid:
            services[canon_role] = {
                'cpu_cumulative_seconds': None,
                'memory_bytes': None,
                'available': False,
                'container_id': None,
                'reason': 'container_id_missing',
            }
            continue

        if docker_client is not None:
            try:
                res = docker_client(cid)
                if isinstance(res, dict):
                    services[canon_role] = {
                        'cpu_cumulative_seconds': res.get('cpu_cumulative_seconds'),
                        'memory_bytes': res.get('memory_bytes'),
                        'available': bool(res.get('available')),
                        'container_id': cid,
                        'reason': res.get('reason'),
                    }
                else:
                    services[canon_role] = {
                        'cpu_cumulative_seconds': None,
                        'memory_bytes': None,
                        'available': False,
                        'container_id': cid,
                        'reason': 'docker_client_returned_non_dict',
                    }
            except Exception as exc:
                services[canon_role] = {
                    'cpu_cumulative_seconds': None,
                    'memory_bytes': None,
                    'available': False,
                    'container_id': cid,
                    'reason': f'docker_client_{type(exc).__name__}',
                }
            continue

        try:
            if not os.path.exists(socket_path):
                raise FileNotFoundError('docker_socket_not_found')
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            read_deadline = time.monotonic() + min(2.0, max(0.001, float(timeout)))
            try:
                sock.settimeout(max(0.001, read_deadline - time.monotonic()))
                sock.connect(socket_path)
                req = f'GET /containers/{cid}/stats?stream=false&one-shot=true HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'.encode('utf-8')
                sock.sendall(req)
                raw_data = b''
                while len(raw_data) < 262144:
                    remaining = read_deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError('docker_stats_deadline_exceeded')
                    sock.settimeout(remaining)
                    chunk = sock.recv(8192)
                    if not chunk:
                        break
                    raw_data += chunk
            finally:
                sock.close()

            header_end = raw_data.find(b'\r\n\r\n')
            if header_end == -1:
                raise ValueError('docker_stats_malformed_response')
            header_bytes = raw_data[:header_end]
            status_line = header_bytes.split(b'\r\n', 1)[0]
            if b' 200 ' not in status_line:
                raise ValueError(f'docker_stats_status_{status_line.decode(errors="replace")}')

            body = raw_data[header_end + 4:]
            if b'Transfer-Encoding: chunked' in header_bytes or b'transfer-encoding: chunked' in header_bytes:
                parts = []
                cur = body
                while cur:
                    line_end = cur.find(b'\r\n')
                    if line_end == -1:
                        break
                    chunk_len_hex = cur[:line_end].strip()
                    if not chunk_len_hex:
                        break
                    chunk_len = int(chunk_len_hex, 16)
                    if chunk_len == 0:
                        break
                    parts.append(cur[line_end + 2:line_end + 2 + chunk_len])
                    cur = cur[line_end + 2 + chunk_len + 2:]
                body = b''.join(parts)

            stats = json.loads(body.decode('utf-8'))
            total_usage = stats.get('cpu_stats', {}).get('cpu_usage', {}).get('total_usage')
            mem_usage = stats.get('memory_stats', {}).get('usage')
            if isinstance(total_usage, (int, float)):
                services[canon_role] = {
                    'cpu_cumulative_seconds': float(total_usage) / 1e9,
                    'memory_bytes': int(mem_usage) if isinstance(mem_usage, (int, float)) else None,
                    'available': True,
                    'container_id': cid,
                    'reason': None,
                }
            else:
                services[canon_role] = {
                    'cpu_cumulative_seconds': None,
                    'memory_bytes': int(mem_usage) if isinstance(mem_usage, (int, float)) else None,
                    'available': False,
                    'container_id': cid,
                    'reason': 'cpu_total_usage_missing',
                }
        except Exception as exc:
            services[canon_role] = {
                'cpu_cumulative_seconds': None,
                'memory_bytes': None,
                'available': False,
                'container_id': cid,
                'reason': f'docker_stats_read_failed_{type(exc).__name__}',
            }

    all_available = all(services[r].get('available') for r in canonical_roles)
    total_cpu = sum(services[r]['cpu_cumulative_seconds'] for r in canonical_roles) if all_available else None

    return {
        'services': services,
        'total_cpu_cumulative_seconds': total_cpu,
        'available': all_available,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'monotonic': time.monotonic(),
    }


def infer_host_a_active_window(*, snapshots, baseline, terminal, expected_total_messages,
                               expected_total_samples, sampling_interval=1.0, evicted_count=0):
    """Infer active workload window boundaries strictly on Host A sampling timeline.

    Identifies first observed ingress change through final commit counter boundaries.
    Requires sufficient pre- and post-workload boundaries. Rejects counter resets,
    container identity changes, evicted start boundaries, and sparse observations as INCONCLUSIVE.
    """
    if not isinstance(snapshots, list) or not snapshots:
        return {
            'status': 'INCONCLUSIVE',
            'availability': 'inconclusive',
            'reason': 'sparse_observations',
            'start_sample_index': None,
            'end_sample_index': None,
            'duration_seconds': None,
            'duration_uncertainty_seconds': None,
            'active_cpu_seconds': None,
            'sampled_max_memory_bytes': None,
        }

    if not isinstance(baseline, dict):
        return {
            'status': 'INCONCLUSIVE',
            'availability': 'inconclusive',
            'reason': 'pre_boundary_missing',
            'start_sample_index': None,
            'end_sample_index': None,
            'duration_seconds': None,
            'duration_uncertainty_seconds': None,
            'active_cpu_seconds': None,
            'sampled_max_memory_bytes': None,
        }

    base_committed = baseline.get('ingress', {}).get('Committed', 0)
    base_received = baseline.get('ingress', {}).get('Received', 0)
    base_samples = baseline.get('ingress', {}).get('SamplesNew', 0)

    if evicted_count > 0:
        first_s = snapshots[0]
        first_ing = first_s.get('ingress') or {}
        if (first_ing.get('Committed', 0) > base_committed or
                first_ing.get('Received', 0) > base_received or
                first_ing.get('SamplesNew', 0) > base_samples):
            return {
                'status': 'INCONCLUSIVE',
                'availability': 'inconclusive',
                'reason': 'active_window_start_evicted',
                'start_sample_index': None,
                'end_sample_index': None,
                'duration_seconds': None,
                'duration_uncertainty_seconds': None,
                'active_cpu_seconds': None,
                'sampled_max_memory_bytes': None,
            }

    all_steps = [baseline] + list(snapshots)
    if terminal and isinstance(terminal, dict):
        all_steps.append(terminal)

    for idx in range(len(all_steps) - 1):
        prev_s, next_s = all_steps[idx], all_steps[idx + 1]

        p_prev = prev_s.get('process_instance')
        p_next = next_s.get('process_instance')
        if p_prev and p_next and p_prev != p_next:
            return {
                'status': 'INCONCLUSIVE',
                'availability': 'inconclusive',
                'reason': 'process_instance_changed',
                'start_sample_index': None,
                'end_sample_index': None,
                'duration_seconds': None,
                'duration_uncertainty_seconds': None,
                'active_cpu_seconds': None,
                'sampled_max_memory_bytes': None,
            }

        res_prev = prev_s.get('resources') or {}
        res_next = next_s.get('resources') or {}
        for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
            cid_prev = res_prev.get(svc, {}).get('container_id') if isinstance(res_prev.get(svc), dict) else None
            cid_next = res_next.get(svc, {}).get('container_id') if isinstance(res_next.get(svc), dict) else None
            if cid_prev and cid_next and cid_prev != cid_next:
                return {
                    'status': 'INCONCLUSIVE',
                    'availability': 'inconclusive',
                    'reason': 'container_identity_changed',
                    'start_sample_index': None,
                    'end_sample_index': None,
                    'duration_seconds': None,
                    'duration_uncertainty_seconds': None,
                    'active_cpu_seconds': None,
                    'sampled_max_memory_bytes': None,
                }

        ing_prev = prev_s.get('ingress') or {}
        ing_next = next_s.get('ingress') or {}
        for ckey in ('Committed', 'Received', 'Accepted', 'ACKPublished', 'SamplesNew'):
            v_prev = ing_prev.get(ckey)
            v_next = ing_next.get(ckey)
            if isinstance(v_prev, (int, float)) and isinstance(v_next, (int, float)):
                if v_next < v_prev:
                    return {
                        'status': 'INCONCLUSIVE',
                        'availability': 'inconclusive',
                        'reason': 'counter_reset',
                        'start_sample_index': None,
                        'end_sample_index': None,
                        'duration_seconds': None,
                        'duration_uncertainty_seconds': None,
                        'active_cpu_seconds': None,
                        'sampled_max_memory_bytes': None,
                    }

        for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
            c_prev = res_prev.get(svc, {}).get('cpu_cumulative_seconds') if isinstance(res_prev.get(svc), dict) else None
            c_next = res_next.get(svc, {}).get('cpu_cumulative_seconds') if isinstance(res_next.get(svc), dict) else None
            if isinstance(c_prev, (int, float)) and isinstance(c_next, (int, float)):
                if c_next < c_prev:
                    return {
                        'status': 'INCONCLUSIVE',
                        'availability': 'inconclusive',
                        'reason': 'counter_reset',
                        'start_sample_index': None,
                        'end_sample_index': None,
                        'duration_seconds': None,
                        'duration_uncertainty_seconds': None,
                        'active_cpu_seconds': None,
                        'sampled_max_memory_bytes': None,
                    }

    start_idx = None
    for idx, s in enumerate(snapshots):
        ing = s.get('ingress') or {}
        if (ing.get('Received', 0) > base_received or
                ing.get('Committed', 0) > base_committed or
                ing.get('SamplesNew', 0) > base_samples):
            start_idx = idx
            break

    if start_idx is None:
        return {
            'status': 'INCONCLUSIVE',
            'availability': 'inconclusive',
            'reason': 'zero_ingress_observations',
            'start_sample_index': None,
            'end_sample_index': None,
            'duration_seconds': None,
            'duration_uncertainty_seconds': None,
            'active_cpu_seconds': None,
            'sampled_max_memory_bytes': None,
        }

    target_commits = base_committed + expected_total_messages
    end_idx = None
    for idx in range(start_idx, len(snapshots)):
        ing = snapshots[idx].get('ingress') or {}
        if ing.get('Committed', 0) >= target_commits:
            end_idx = idx
            break

    if end_idx is None:
        return {
            'status': 'INCONCLUSIVE',
            'availability': 'inconclusive',
            'reason': 'final_commit_boundary_not_observed',
            'start_sample_index': None,
            'end_sample_index': None,
            'duration_seconds': None,
            'duration_uncertainty_seconds': None,
            'active_cpu_seconds': None,
            'sampled_max_memory_bytes': None,
        }

    if end_idx < len(snapshots) - 1:
        post_snap = snapshots[end_idx + 1]
    elif terminal and isinstance(terminal, dict) and terminal.get('resources') and terminal.get('monotonic'):
        post_snap = terminal
    else:
        return {
            'status': 'INCONCLUSIVE',
            'availability': 'inconclusive',
            'reason': 'post_boundary_missing',
            'start_sample_index': None,
            'end_sample_index': None,
            'duration_seconds': None,
            'duration_uncertainty_seconds': None,
            'active_cpu_seconds': None,
            'sampled_max_memory_bytes': None,
        }

    if start_idx == end_idx:
        return {
            'status': 'INCONCLUSIVE',
            'availability': 'inconclusive',
            'reason': 'short_burst_unresolved',
            'start_sample_index': start_idx,
            'end_sample_index': end_idx,
            'duration_seconds': None,
            'duration_uncertainty_seconds': None,
            'active_cpu_seconds': None,
            'sampled_max_memory_bytes': None,
        }

    start_snap = snapshots[start_idx]
    end_snap = snapshots[end_idx]
    t_start = start_snap.get('monotonic', 0.0)
    t_end = end_snap.get('monotonic', 0.0)
    duration_s = max(0.0, t_end - t_start)

    pre_snap = snapshots[start_idx - 1] if start_idx > 0 else baseline
    t_pre = pre_snap.get('monotonic') if isinstance(pre_snap, dict) else None
    t_post = post_snap.get('monotonic') if isinstance(post_snap, dict) else None

    uncertainty_candidates = [float(sampling_interval)]
    if t_pre is not None and t_start is not None and t_start >= t_pre:
        uncertainty_candidates.append(t_start - t_pre)
    if t_post is not None and t_end is not None and t_post >= t_end:
        uncertainty_candidates.append(t_post - t_end)
    for g_idx in range(start_idx, end_idx):
        m1 = snapshots[g_idx].get('monotonic')
        m2 = snapshots[g_idx + 1].get('monotonic')
        if m1 is not None and m2 is not None and m2 >= m1:
            uncertainty_candidates.append(m2 - m1)
    duration_uncertainty = max(uncertainty_candidates)

    active_cpu = {}
    active_cpu_available = True
    for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
        cpu_pre = pre_snap.get('resources', {}).get(svc, {}).get('cpu_cumulative_seconds') if isinstance(pre_snap.get('resources'), dict) else None
        cpu_end = end_snap.get('resources', {}).get(svc, {}).get('cpu_cumulative_seconds') if isinstance(end_snap.get('resources'), dict) else None
        if isinstance(cpu_pre, (int, float)) and isinstance(cpu_end, (int, float)):
            active_cpu[svc] = max(0.0, cpu_end - cpu_pre)
        else:
            active_cpu_available = False
            active_cpu[svc] = None

    if active_cpu_available:
        active_cpu['total'] = sum(active_cpu[s] for s in ('backend', 'postgres', 'mosquitto', 'nginx'))
    else:
        active_cpu = None

    sampled_max_mem = {}
    for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
        mems = []
        for s in snapshots[start_idx:end_idx + 1]:
            m = s.get('resources', {}).get(svc, {}).get('memory_bytes') if isinstance(s.get('resources'), dict) else None
            if isinstance(m, (int, float)):
                mems.append(int(m))
        sampled_max_mem[svc] = max(mems) if mems else None
    if any(sampled_max_mem[s] is not None for s in ('backend', 'postgres', 'mosquitto', 'nginx')):
        sampled_max_mem['total'] = sum(sampled_max_mem[s] for s in ('backend', 'postgres', 'mosquitto', 'nginx') if sampled_max_mem[s] is not None)
    else:
        sampled_max_mem = None

    return {
        'status': 'AVAILABLE',
        'availability': 'available',
        'reason': None,
        'start_timestamp': start_snap.get('timestamp'),
        'end_timestamp': end_snap.get('timestamp'),
        'start_sample_index': start_idx,
        'end_sample_index': end_idx,
        'duration_seconds': round(duration_s, 6),
        'duration_uncertainty_seconds': round(duration_uncertainty, 6),
        'active_cpu_seconds': active_cpu,
        'active_cpu_scope': 'sum_server_components(backend+postgres+mosquitto+nginx); not_backend_only',
        'sampled_max_memory_bytes': sampled_max_mem,
    }


def qualify_generator_resources(gen_data, events, *, max_deadline_seconds=120.0):
    """Qualify Host B generator resource sampler metrics, clocks, and intervals.

    Aligns active measurement window on Host B monotonic timeline:
    active_start_s = (observed_at_runner_monotonic_ns / 1e9) - clock.monotonic_start.
    Go child process-local elapsed_ns is rejected as process-local.
    Requires complete interval bracketing and complete valid delta_ticks across the active window.
    Any process_appeared, process_disappeared, or invalid deltas in the active window make active CPU INCONCLUSIVE.
    Full lifecycle CPU requires all post-baseline intervals to have valid delta_ticks;
    sums over incomplete intervals are labeled as partial_observed_lifecycle_cpu_seconds.
    CPU=0 is valid only when evidence is complete.
    """
    unavailable_template = {
        'status': 'UNAVAILABLE',
        'availability': 'unavailable',
        'reason': 'no_sampler_evidence' if not gen_data else 'generator_resources_schema_rejected',
        'role': 'generator_B',
        'lifecycle_cpu_seconds': None,
        'lifecycle_cpu_available': False,
        'lifecycle_cpu_unavailable_reason': 'no_sampler_evidence' if not gen_data else 'generator_resources_schema_rejected',
        'partial_observed_lifecycle_cpu_seconds': None,
        'active_cpu_seconds': None,
        'active_cpu_available': False,
        'active_cpu_unavailable_reason': 'no_sampler_evidence' if not gen_data else 'generator_resources_schema_rejected',
        'active_window': None,
        'sampled_max_rss_bytes': None,
        'absolute_peak_rss_bytes': None,
        'peak_qualification': 'sampler_evidence_unavailable',
        'clock_qualification': 'sampler_evidence_unavailable',
        'cpu_seconds_unit': 'seconds',
        'rss_unit': 'bytes',
        'total_samples': 0,
        'available_samples': 0,
        'unavailable_samples': 0,
        'errors': [],
        'missing_reasons': ['no_sampler_evidence'] if not gen_data else ['generator_resources_schema_rejected'],
        'samples': [],
    }

    if not isinstance(gen_data, dict) or gen_data.get('role') != 'generator_B' or gen_data.get('schema_version', 1) != 1:
        return unavailable_template

    gen_clock = gen_data.get('clock')
    clock_valid = True
    clock_error = None
    mono_start = None
    if not isinstance(gen_clock, dict):
        clock_valid = False
        clock_error = 'generator_clock_missing'
    else:
        mono_start = gen_clock.get('monotonic_start')
        if not isinstance(mono_start, (int, float)) or isinstance(mono_start, bool) or not math.isfinite(mono_start) or mono_start <= 0:
            clock_valid = False
            clock_error = 'generator_clock_monotonic_start_invalid'
        deadline_s = gen_clock.get('deadline_seconds')
        if not isinstance(deadline_s, (int, float)) or isinstance(deadline_s, bool) or not math.isfinite(deadline_s) or deadline_s <= 0:
            clock_valid = False
            clock_error = 'generator_clock_deadline_invalid'
        elif deadline_s > max_deadline_seconds:
            clock_valid = False
            clock_error = f'max_120s_boundary_exceeded:deadline_{deadline_s}'

    clk_tck = gen_data.get('clk_tck', 100)
    if not isinstance(clk_tck, int) or isinstance(clk_tck, bool) or clk_tck <= 0:
        clk_tck = 100

    raw_samples = gen_data.get('samples', [])
    if not isinstance(raw_samples, list):
        raw_samples = []

    samples = []
    samples_valid = True
    samples_error = None
    for s in raw_samples:
        if not isinstance(s, dict):
            samples_valid = False
            samples_error = 'sample_not_dict'
            break
        elapsed = s.get('elapsed_seconds')
        if not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool) or not math.isfinite(elapsed) or elapsed < 0:
            samples_valid = False
            samples_error = 'sample_elapsed_seconds_invalid'
            break
        if elapsed > max_deadline_seconds:
            samples_valid = False
            samples_error = f'max_120s_boundary_exceeded:sample_elapsed_{elapsed}'
            break
        interval_s = s.get('interval_seconds')
        if interval_s is not None:
            if not isinstance(interval_s, (int, float)) or isinstance(interval_s, bool) or not math.isfinite(interval_s) or interval_s <= 0:
                samples_valid = False
                samples_error = 'sample_interval_seconds_invalid'
                break
        dt = s.get('delta_ticks')
        if dt is not None:
            if not isinstance(dt, int) or isinstance(dt, bool) or dt < 0:
                samples_valid = False
                samples_error = 'sample_delta_ticks_invalid'
                break
        samples.append(s)

    # Lifecycle CPU qualification
    valid_delta_samples = [s for s in samples if s.get('available') and isinstance(s.get('delta_ticks'), int)]
    partial_life_cpu_s = (sum(s['delta_ticks'] for s in valid_delta_samples) / clk_tck) if valid_delta_samples else None

    lifecycle_cpu_s = None
    lifecycle_cpu_available = False
    lifecycle_reason = None
    if not clock_valid:
        lifecycle_reason = clock_error
    elif not samples_valid:
        lifecycle_reason = samples_error
    elif len(samples) < 2:
        lifecycle_reason = 'insufficient_samples_for_lifecycle'
    else:
        incomplete_reason = None
        for s in samples[1:]:
            s_rs = s.get('reason')
            if not s.get('available') or not isinstance(s.get('delta_ticks'), int):
                reason_str = s_rs or 'missing_delta_ticks'
                incomplete_reason = f'process_change_samples_lack_delta:{reason_str}'
                break
            if s_rs in ('process_appeared', 'process_disappeared', 'all_processes_exited'):
                incomplete_reason = f'process_change_samples_lack_delta:{s_rs}'
                break
        if incomplete_reason is not None:
            lifecycle_reason = incomplete_reason
        else:
            lifecycle_cpu_s = sum(s['delta_ticks'] for s in samples[1:]) / clk_tck
            lifecycle_cpu_available = True

    # Active CPU qualification on Host B monotonic timeline
    b_events = [e for e in (events or []) if isinstance(e, dict) and (
        e.get('child') in ('paperload-telemetry:B.manifest.json', 'paperload-telemetry:B.json', 'B') or
        e.get('arm') == 'B'
    )]
    if not b_events:
        b_events = [e for e in (events or []) if isinstance(e, dict)]

    m_begin_events = [e for e in b_events if e.get('event') == 'measure_begin']
    m_end_events = [e for e in b_events if e.get('event') == 'measure_end']

    active_start_s = None
    active_end_s = None
    active_gen_cpu_s = None
    active_gen_reason = None

    if not m_begin_events or not m_end_events:
        active_gen_reason = 'generator_active_window_unobserved'
    else:
        begin_mono_ns = [e.get('observed_at_runner_monotonic_ns') for e in m_begin_events
                         if isinstance(e.get('observed_at_runner_monotonic_ns'), (int, float))
                         and not isinstance(e.get('observed_at_runner_monotonic_ns'), bool)
                         and math.isfinite(e.get('observed_at_runner_monotonic_ns'))
                         and e.get('observed_at_runner_monotonic_ns') > 0]
        end_mono_ns = [e.get('observed_at_runner_monotonic_ns') for e in m_end_events
                       if isinstance(e.get('observed_at_runner_monotonic_ns'), (int, float))
                       and not isinstance(e.get('observed_at_runner_monotonic_ns'), bool)
                       and math.isfinite(e.get('observed_at_runner_monotonic_ns'))
                       and e.get('observed_at_runner_monotonic_ns') > 0]
        if not begin_mono_ns or not end_mono_ns:
            active_gen_reason = 'generator_active_window_runner_monotonic_unobserved'
        elif not clock_valid:
            active_gen_reason = clock_error
        elif not samples_valid:
            active_gen_reason = samples_error
        else:
            t_begin_host_b_s = min(begin_mono_ns) / 1e9
            t_end_host_b_s = max(end_mono_ns) / 1e9
            active_start_s = t_begin_host_b_s - mono_start
            active_end_s = t_end_host_b_s - mono_start

            if active_start_s < 0:
                active_gen_reason = f'active_start_negative:{round(active_start_s, 6)}'
            elif active_end_s <= active_start_s:
                active_gen_reason = 'active_window_reversed_or_empty'
            elif active_end_s > max_deadline_seconds or active_start_s > max_deadline_seconds:
                active_gen_reason = f'max_120s_boundary_exceeded:active_end_{round(active_end_s, 6)}'
            elif not samples:
                active_gen_reason = 'no_samples_captured'
            else:
                first_s = samples[0]
                last_s = samples[-1]
                if first_s.get('elapsed_seconds', float('inf')) > active_start_s:
                    active_gen_reason = 'active_window_start_not_bracketed'
                elif last_s.get('elapsed_seconds', -1.0) < active_end_s:
                    active_gen_reason = 'active_window_end_not_bracketed'
                else:
                    active_samples = [s for s in samples if active_start_s < s.get('elapsed_seconds', -1.0) <= active_end_s]
                    if not active_samples:
                        active_gen_reason = 'no_samples_in_active_window'
                    else:
                        nominal_interval = gen_clock.get('sample_interval_seconds', 1.0) if isinstance(gen_clock, dict) else 1.0
                        max_allowed_gap = max(3.0, nominal_interval * 2.5)

                        for s in active_samples:
                            s_rs = s.get('reason')
                            s_idx = s.get('sample_index')
                            if not s.get('available'):
                                active_gen_reason = f'sample_unavailable_in_active_window:index_{s_idx}'
                                break
                            dt = s.get('delta_ticks')
                            if dt is None or not isinstance(dt, int) or dt < 0:
                                reason_str = s_rs or 'missing_delta_ticks'
                                active_gen_reason = f'invalid_delta_ticks_in_active_window:{reason_str}'
                                break
                            if s_rs in ('process_appeared', 'process_disappeared', 'all_processes_exited'):
                                active_gen_reason = f'process_change_in_active_window:{s_rs}'
                                break
                            int_s = s.get('interval_seconds')
                            if int_s is not None and int_s > max_allowed_gap:
                                active_gen_reason = f'sampling_gap_within_active_window:{round(int_s, 6)}s'
                                break

                        if active_gen_reason is None:
                            active_gen_cpu_s = sum(s['delta_ticks'] for s in active_samples) / clk_tck

    active_cpu_available = (active_gen_cpu_s is not None)
    summary_gen = gen_data.get('summary', {}) if isinstance(gen_data, dict) else {}
    rss_vals = [s['rss_bytes'] for s in samples if isinstance(s.get('rss_bytes'), (int, float))]
    sampled_max_rss = max(rss_vals) if rss_vals else summary_gen.get('max_sample_rss_bytes')

    overall_status = 'AVAILABLE' if active_cpu_available else 'INCONCLUSIVE'
    overall_avail = 'available' if active_cpu_available else 'inconclusive'
    overall_reason = None if active_cpu_available else (active_gen_reason or lifecycle_reason or 'incomplete_generator_cpu_evidence')

    return {
        'status': overall_status,
        'availability': overall_avail,
        'reason': overall_reason,
        'role': 'generator_B',
        'clock': gen_clock if isinstance(gen_clock, dict) else {},
        'scopes': gen_data.get('scopes', {}),
        'units': gen_data.get('units', {}),
        'cpu_seconds_unit': 'seconds',
        'rss_unit': 'bytes',
        'lifecycle_cpu_seconds': round(lifecycle_cpu_s, 6) if lifecycle_cpu_s is not None else None,
        'lifecycle_cpu_available': lifecycle_cpu_available,
        'lifecycle_cpu_unavailable_reason': lifecycle_reason if not lifecycle_cpu_available else None,
        'partial_observed_lifecycle_cpu_seconds': round(partial_life_cpu_s, 6) if partial_life_cpu_s is not None else None,
        'active_cpu_seconds': round(active_gen_cpu_s, 6) if active_gen_cpu_s is not None else None,
        'active_cpu_available': active_cpu_available,
        'active_cpu_unavailable_reason': active_gen_reason if not active_cpu_available else None,
        'active_window': {
            'start_seconds': round(active_start_s, 6) if active_start_s is not None else None,
            'end_seconds': round(active_end_s, 6) if active_end_s is not None else None,
            'duration_seconds': round(active_end_s - active_start_s, 6) if (active_start_s is not None and active_end_s is not None) else None,
            'aligned_on_host_b_monotonic': True if (active_start_s is not None and active_end_s is not None) else False,
            'pipe_receipt_lag_qualification': 'pipe_receipt_lag_unknown_child_elapsed_process_local',
            'status': 'AVAILABLE' if active_cpu_available else 'INCONCLUSIVE',
            'reason': active_gen_reason,
        },
        'sampled_max_rss_bytes': sampled_max_rss,
        'absolute_peak_rss_bytes': None,
        'peak_qualification': 'sampled_max_rss_not_peak; sum_sampled_RSS_may_double_count_shared_pages',
        'clock_qualification': 'runner observed pipe receipt; pipe_receipt_lag_unknown; observation lag unknown; child elapsed is process-local; aligned on host B monotonic timeline (observed_at_runner_monotonic_ns / 1e9 - generator clock.monotonic_start)',
        'total_samples': len(samples),
        'available_samples': summary_gen.get('available_samples', sum(1 for s in samples if s.get('available'))),
        'unavailable_samples': summary_gen.get('unavailable_samples', sum(1 for s in samples if not s.get('available'))),
        'errors': summary_gen.get('errors', []),
        'missing_reasons': summary_gen.get('missing_reasons', []),
        'samples': samples,
    }


def calculate_equal_samples_normalized_metrics(*, actual_n, total_payload_bytes, total_db_transactions,
                                              active_window, lifecycle_window,
                                              generator_resources=None,
                                              db_transactions_unavailable_reason=None,
                                              ledger_rows=None,
                                              offered_payload_bytes=None,
                                              attempted_payload_bytes=None):
    """Normalize metrics per sample based on validated actual sample count N.

    Retains histogram definitions, never sums percentiles across phases.
    Distinguishes active-window CPU/sample from lifecycle-window CPU/sample.
    """
    if not isinstance(actual_n, int) or actual_n <= 0:
        raise ValueError('actual_n_must_be_positive_int')

    bytes_per_sample = total_payload_bytes / actual_n if isinstance(total_payload_bytes, (int, float)) else None
    admitted_bytes_per_sample = bytes_per_sample
    offered_bytes_per_sample = offered_payload_bytes / actual_n if isinstance(offered_payload_bytes, (int, float)) else None
    attempted_bytes_per_sample = attempted_payload_bytes / actual_n if isinstance(attempted_payload_bytes, (int, float)) else None

    db_tx_avail = False
    db_tx_reason = None
    db_tx_per_sample = None
    if isinstance(total_db_transactions, (int, float)) and not isinstance(total_db_transactions, bool) and total_db_transactions >= 0:
        db_tx_avail = True
        db_tx_per_sample = total_db_transactions / actual_n
    else:
        db_tx_reason = db_transactions_unavailable_reason or 'db_transactions_counter_unavailable'

    ledger_rows_per_sample = ledger_rows / actual_n if isinstance(ledger_rows, (int, float)) and not isinstance(ledger_rows, bool) else None

    active_cpu_avail = False
    active_cpu_reason = None
    active_cpu_per_sample = None
    if isinstance(active_window, dict) and active_window.get('status') == 'AVAILABLE':
        act_cpu = active_window.get('active_cpu_seconds')
        if isinstance(act_cpu, dict) and isinstance(act_cpu.get('total'), (int, float)):
            active_cpu_avail = True
            active_cpu_per_sample = act_cpu['total'] / actual_n
        else:
            active_cpu_reason = 'active_cpu_counters_unavailable'
    else:
        active_cpu_reason = active_window.get('reason') if isinstance(active_window, dict) else 'active_window_unavailable'

    lifecycle_cpu_avail = False
    lifecycle_cpu_reason = None
    lifecycle_cpu_per_sample = None
    if isinstance(lifecycle_window, dict):
        base_cpu = lifecycle_window.get('baseline_cpu_seconds')
        term_cpu = lifecycle_window.get('terminal_cpu_seconds')
        if isinstance(base_cpu, (int, float)) and isinstance(term_cpu, (int, float)) and term_cpu >= base_cpu:
            lifecycle_cpu_avail = True
            lifecycle_cpu_per_sample = (term_cpu - base_cpu) / actual_n
        else:
            lifecycle_cpu_reason = 'lifecycle_cpu_counters_unavailable'

    gen_lifecycle_avail = False
    gen_lifecycle_reason = None
    gen_lifecycle_per_sample = None
    gen_active_avail = False
    gen_active_reason = None
    gen_active_per_sample = None

    if isinstance(generator_resources, dict):
        gen_life_cpu = generator_resources.get('lifecycle_cpu_seconds')
        if isinstance(gen_life_cpu, (int, float)) and not isinstance(gen_life_cpu, bool):
            gen_lifecycle_avail = True
            gen_lifecycle_per_sample = gen_life_cpu / actual_n
        else:
            gen_lifecycle_reason = (generator_resources.get('lifecycle_cpu_unavailable_reason') or
                                    generator_resources.get('reason') or 'generator_lifecycle_cpu_unavailable')

        gen_act_cpu = generator_resources.get('active_cpu_seconds')
        if isinstance(gen_act_cpu, (int, float)) and not isinstance(gen_act_cpu, bool):
            gen_active_avail = True
            gen_active_per_sample = gen_act_cpu / actual_n
        else:
            gen_active_reason = (generator_resources.get('active_cpu_unavailable_reason') or
                                 generator_resources.get('reason') or 'generator_active_cpu_unavailable')
    else:
        gen_lifecycle_reason = 'generator_resources_unavailable'
        gen_active_reason = 'generator_resources_unavailable'

    return {
        'actual_samples': actual_n,
        'bytes_per_sample': bytes_per_sample,
        'admitted_bytes_per_sample': admitted_bytes_per_sample,
        'offered_bytes_per_sample': offered_bytes_per_sample,
        'attempted_bytes_per_sample': attempted_bytes_per_sample,
        'db_transactions_per_sample': db_tx_per_sample,
        'db_transactions_available': db_tx_avail,
        'db_transactions_unavailable_reason': db_tx_reason,
        'ledger_rows_per_sample': ledger_rows_per_sample,
        'committed_rows_per_sample': ledger_rows_per_sample,
        'active_cpu_seconds_per_sample': active_cpu_per_sample,
        'lifecycle_cpu_seconds_per_sample': lifecycle_cpu_per_sample,
        'active_cpu_available': active_cpu_avail,
        'active_cpu_unavailable_reason': active_cpu_reason,
        'lifecycle_cpu_available': lifecycle_cpu_avail,
        'lifecycle_cpu_unavailable_reason': lifecycle_cpu_reason,
        'generator_lifecycle_cpu_seconds_per_sample': gen_lifecycle_per_sample,
        'generator_lifecycle_cpu_available': gen_lifecycle_avail,
        'generator_lifecycle_cpu_unavailable_reason': gen_lifecycle_reason,
        'generator_active_cpu_seconds_per_sample': gen_active_per_sample,
        'generator_active_cpu_available': gen_active_avail,
        'generator_active_cpu_unavailable_reason': gen_active_reason,
        'partial_observed_generator_lifecycle_cpu_seconds_per_sample': (
            (generator_resources.get('partial_observed_lifecycle_cpu_seconds') / actual_n)
            if isinstance(generator_resources, dict) and isinstance(generator_resources.get('partial_observed_lifecycle_cpu_seconds'), (int, float))
            else None
        ),
        'payload_bytes_qualification': 'backend_admitted_attempt_bytes; includes_retries_if_admitted; application_payload_bytes_only; not_wire_traffic_not_tls',
        'latency_qualification': 'ack_latency_and_persistence_histograms; not_sensor_acquisition_latency',
        'cpu_qualification': 'sum_server_components_cpu_seconds(backend+postgres+mosquitto+nginx); not_backend_only; not_whole_host_capacity',
        'memory_qualification': 'sampled_max_rss_bytes_may_double_count_shared_pages_not_peak',
    }


def failure_diagnostics(prepared, outcome, *, stop_timeout_seconds=15.0, smoke=None,
                        endpoint=None, registry=None, containers=None,
                        persist_callback=None,
                        terminal_data=None,
                        time_fn=time.monotonic, wall_fn=lambda: datetime.now(timezone.utc).isoformat()):
    """Bounded, idempotent best-effort E2 diagnostics while fixture resources are alive."""
    if prepared.get('_failure_diagnostics_result') is not None:
        return prepared['_failure_diagnostics_result']
    started = time_fn()
    deadline = started + min(15.0, max(0.0, float(stop_timeout_seconds)))
    remaining = lambda: max(0.0, deadline - time_fn())
    collector = prepared['collector']
    side = getattr(collector, 'side_sampler', None)
    stop_budget = remaining()
    if getattr(collector, 'side_sampler', None) is not None:
        stop_budget = max(0.0, stop_budget - min(3.0, stop_budget * 0.3))
    stopped = collector.stop(None, timeout_seconds=stop_budget)
    snapshots = []
    for row in getattr(collector, 'metrics', []):
        raw = row.get('snapshot')
        projected = EqualSamplesCompactSampler._project_metrics(raw)
        if projected is not None:
            snapshots.append({'phase': str(row.get('phase', 'unknown'))[:64], 'snapshot': projected})
    errors = [e for e in list(getattr(collector, 'errors', []))[:16]
              if isinstance(e, str) and len(e) <= 96 and e.replace('_', '').replace('-', '').isalnum()]
    result = {'schema_version': 1, 'experiment': 'E2',
        'session_id': str(prepared.get('session_id', ''))[:128], 'run_id': str(prepared.get('run_id', ''))[:128],
        'phase_id': str(prepared.get('phase_id', ''))[:128], 'workload_outcome': str(outcome)[:64],
        'stop_cause': str(prepared.get('stop_cause', outcome))[:64], 'full_p8_accepted': False,
        'source': prepared.get('source_provenance'), 'bundle_sha256': prepared.get('bundle_sha256'),
        'manifest_sha256': prepared.get('manifest_sha256'), 'sample_times': {
            'flush_started': wall_fn(), 'collector_started': getattr(collector, '_started_wall', None)},
        'snapshot_status': ('retained' if stopped else
            'retained_copy_after_join_timeout' if snapshots else 'collector_join_timeout'),
        'threads_joined': {'metrics': not bool(getattr(getattr(collector, '_thread', None), 'is_alive', lambda: False)()),
                            'side': (side is None or not bool(getattr(getattr(side, '_thread', None), 'is_alive', lambda: False)()))},
        'snapshots': snapshots,
        'resource_samples': list(getattr(collector, 'resources', []))[:64],
        'coverage': collector.get_coverage() if hasattr(collector, 'get_coverage') else None,
        'collector_errors': errors, 'terminal_metrics_status': {'available': False, 'reason': 'not_collected'},
        'database_counts': {'available': False, 'reason': 'not_collected'},
        'resources_status': {'available': False, 'reason': 'not_collected'},
        'qualification': 'retained samples; not a workload acceptance or peak measurement'}
    if side is not None:
        result['postgres_linux'] = {'baseline': prepared.get('diagnostic_baseline'),
            'samples': side.samples, 'coverage': side.coverage(), 'terminal': None}
    if persist_callback is not None:
        try:
            persist_callback(result)
        except Exception:
            result['snapshot_persist_status'] = {'available': False, 'reason': 'initial_persist_failed'}
    terminal_http_ok = False
    terminal = None
    if terminal_data is None and remaining() > 0 and smoke is not None and endpoint is not None:
        try:
            # HTTP helper's timeout is explicit and bounded by the shared deadline.
            timeout = min(2.0, remaining())
            if timeout <= 0: raise TimeoutError()
            result['sample_times']['terminal_scrape_started'] = wall_fn()
            terminal = _support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'], timeout=timeout)
            result['sample_times']['terminal_scrape_finished'] = wall_fn()
            projected = EqualSamplesCompactSampler._project_metrics(terminal)
            result['terminal_metrics_status'] = {'available': projected is not None, 'reason': None if projected else 'schema_unavailable'}
            result['terminal_metrics'] = projected
            terminal_http_ok = True
        except Exception:
            result['terminal_metrics_status'] = {'available': False, 'reason': 'terminal_scrape_failed'}
        if remaining() > 0:
            try:
                resources = read_container_resources(None, containers or {}, timeout=min(2.0, remaining()) / 4)
                projected_resources = EqualSamplesCompactSampler._project_resources(resources) if resources else None
                known_services = ('backend', 'postgres', 'mosquitto', 'nginx')
                terminal_services = projected_resources or {}
                for service in known_services:
                    terminal_services.setdefault(service, {'available': False,
                        'reason': 'service_resource_unavailable'})
                result['resource_samples'].append({'phase':'workload_terminal','services':terminal_services})
                complete = all(row.get('available') is True for row in terminal_services.values())
                result['resources_status'] = {'available': complete,
                    'reason': None if complete else 'partial_or_unavailable'}
                if hasattr(collector, 'record_terminal'):
                    collector.record_terminal(terminal, resources)
            except Exception:
                result['resources_status'] = {'available': False, 'reason': 'terminal_resource_read_failed'}
                result['resource_samples'].append({'phase': 'workload_terminal', 'services': {
                    service: {'available': False, 'reason': 'terminal_resource_read_failed'}
                    for service in ('backend', 'postgres', 'mosquitto', 'nginx')}})
        if remaining() > 0 and registry is not None and smoke is not None:
            targets = {}
            for spec, before in zip(prepared.get('specs', []), prepared.get('before_targets', [])):
                if remaining() <= 0:
                    targets[f'{spec.gateway_id}/{spec.sensor_id}'] = {'available': False, 'reason': 'diagnostics_deadline_exceeded'}
                    continue
                key = f'{spec.gateway_id}/{spec.sensor_id}'
                try:
                    after = _target_counts(smoke, registry, prepared['pg'], spec, timeout=min(2.0, remaining()))
                    delta = {k: after[k] - before[k] for k in ('ledger', 'raw', 'temporal', 'configuration')
                             if type(after.get(k)) is int and type(before.get(k)) is int and after[k] >= before[k]}
                    targets[key] = {'available': len(delta) == 4, 'before': before, 'after': after,
                                    'delta': delta, 'reason': None if len(delta) == 4 else 'count_invalid_or_reset'}
                except Exception:
                    targets[key] = {'available': False, 'reason': 'database_query_failed'}
            result['database_counts'] = {'available': bool(targets) and all(v.get('available') for v in targets.values()),
                                         'targets': targets,
                                         'reason': None if targets and all(v.get('available') for v in targets.values()) else 'partial_or_unavailable'}
    elif remaining() <= 0:
        result['terminal_metrics_status'] = {'available': False, 'reason': 'diagnostics_deadline_exceeded'}
        result['database_counts'] = {'available': False, 'reason': 'diagnostics_deadline_exceeded'}
    if side is not None:
        # Readers are attempted independently of HTTP; terminal PG/Linux do not
        # inherit a fresh metrics timestamp or an HTTP success condition.
        if terminal_data is not None:
            result['postgres_linux']['terminal'] = terminal_data.get('postgres_linux')
        elif remaining() > 0 and registry is not None:
            pg_entry = (containers or {}).get('postgres') if isinstance(containers, dict) else None
            try:
                pg_cid = registry.resolve(pg_entry, timeout=min(2.0, remaining()), read_only=True).get('Id') if pg_entry else None
            except Exception:
                pg_cid = None
            budget = min(2.0, max(0.0, remaining() - 1.0))
            result['postgres_linux']['terminal'] = _read_diagnostic_sources(
                smoke, registry, pg_cid, containers or {}, timeout=max(0.05, budget)) if budget > 0 else {
                    'postgres': {'available': False, 'reason': 'deadline_exceeded'},
                    'linux': {'available': False, 'reason': 'deadline_exceeded'}}
        result['postgres_linux']['samples'] = side.samples
        result['postgres_linux']['coverage'] = side.coverage()
    if terminal_data is not None:
        terminal_metrics = terminal_data.get('metrics')
        terminal_resources = terminal_data.get('resources')
        projected = EqualSamplesCompactSampler._project_metrics(terminal_metrics) if terminal_metrics else None
        result['terminal_metrics'] = projected
        result['terminal_metrics_status'] = {'available': projected is not None,
            'reason': None if projected is not None else 'schema_unavailable'}
        services = EqualSamplesCompactSampler._project_resources(terminal_resources) if terminal_resources else None
        if services is not None:
            result['resource_samples'] = list(getattr(collector, 'resources', []))[:64]
            complete = bool(services) and all(row.get('available') is True for row in services.values())
            result['resources_status'] = {'available': complete,
                'reason': None if complete else 'partial_or_unavailable'}
        counts = terminal_data.get('database_counts')
        if counts is not None:
            result['database_counts'] = counts
        result['collector_stop_joined'] = bool(terminal_data.get('collector_stop_joined'))
    result['flush_elapsed_seconds'] = max(0.0, time_fn() - started)
    if persist_callback is not None:
        try:
            persist_callback(result)
        except Exception:
            result['snapshot_persist_status'] = {'available': False, 'reason': 'final_persist_failed'}
    prepared['_failure_diagnostics_result'] = result
    return result


def _compute_official_capture_seconds(manifests):
    """Compute capture_seconds = max(phase duration A, phase duration B) + 10 seconds postprocessing.

    Phase duration = warmup_seconds + measure_seconds + drain_seconds + cooldown_seconds.
    """
    durations = []
    for arm in ('A', 'B'):
        phases = manifests.get(arm, {}).get('phases', {})
        warmup = int(phases.get('warmup_seconds', 0))
        measure = int(phases.get('measure_seconds', 0))
        drain = int(phases.get('drain_seconds', 0))
        cooldown = int(phases.get('cooldown_seconds', 0))
        durations.append(warmup + measure + drain + cooldown)
    if not durations:
        return 0
    return max(durations) + 10


def begin_remote_collection(smoke, fixture, endpoint, registry, containers, prepared, *, phase_id):
    if phase_id != prepared['phase_id']:
        raise ValueError('e2_phase_id_mismatch')
    before_global = smoke.paper_p1_count_db(registry, prepared['pg'])
    before_targets = [_target_counts(smoke, registry, prepared['pg'], spec) for spec in prepared['specs']]
    is_equal_samples = (prepared.get('workload') == 'equal_samples'
                        or 'context' in prepared
                        or phase_id.startswith('equal-samples-'))
    use_compact_sampler = is_equal_samples or prepared.get('workload') == 'steady_burst'

    # Compute absolute session expiry BEFORE slow baseline reads
    if 'wall_seconds' in prepared:
        declared_lifetime = prepared['wall_seconds']
    else:
        declared_lifetime = max(
            int(prepared.get('manifests', {}).get(arm, {}).get('phases', {}).get('measure_seconds', 0))
            + int(prepared.get('manifests', {}).get(arm, {}).get('phases', {}).get('drain_seconds', 60))
            for arm in ('A', 'B'))
    deadline_mono = prepared.get('session_deadline_monotonic')
    if type(deadline_mono) in (int, float) and math.isfinite(deadline_mono):
        absolute_expiry = float(deadline_mono)
    else:
        # Use a finite declared workload lifetime when supervisor deadline metadata is absent.
        fallback = prepared.get('session_remaining_seconds', declared_lifetime)
        absolute_expiry = time.monotonic() + max(0.1, float(fallback))

    # Now read baseline (slow diagnostic)
    baseline = _support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'])

    # Compute remaining deadline AFTER baseline read
    collector_deadline = max(0.1, absolute_expiry - time.monotonic())

    if use_compact_sampler:
        baseline_res = read_container_resources(registry, containers,
            read_only=prepared.get('profile') == 'official')
        collector = EqualSamplesCompactSampler(
            scrape=lambda: _support._admin_metrics(smoke, endpoint, prepared['tokens']['admin']),
            resource_reader=(None if prepared.get('profile') == 'official' and
                prepared.get('workload') == 'steady_burst' else
                lambda: read_container_resources(registry, containers)),
            interval=1.0,
            max_snapshots=3600,
            byte_cap=2 * 1024 * 1024,
            deadline_seconds=min(86400.0, collector_deadline),
            baseline_metrics=baseline,
            baseline_resources=baseline_res,
        )
    else:
        collector = BoundedCollector(scrape=lambda: _support._admin_metrics(
            smoke, endpoint, prepared['tokens']['admin']), interval=0.5, max_snapshots=64,
            deadline_seconds=60, baseline_metrics=baseline,
            baseline_server=smoke.paper_p0_stats(registry, containers, 'workload_start'))
    prepared.update({'before_global': before_global, 'before_targets': before_targets,
                     'baseline': baseline, 'collector': collector, 'containers': containers,
                     'registry': registry, 'smoke': smoke, 'endpoint': endpoint})
    if prepared.get('profile') == 'official' and prepared.get('workload') == 'steady_burst':
        pg_entry = containers.get('postgres') if isinstance(containers, dict) else None
        try:
            pg_cid = registry.resolve(pg_entry, timeout=2.0, read_only=True).get('Id') if pg_entry else None
        except Exception:
            pg_cid = None
        prepared['diagnostic_baseline'] = _read_diagnostic_sources(
            smoke, registry, pg_cid, containers, timeout=2.0)
        # Compute capture_seconds from declared manifest phases
        manifests = prepared.get('manifests', {})
        capture_seconds = _compute_official_capture_seconds(manifests)
        # Baseline metrics for side sampler - ensure process_instance is included
        side_baseline = None
        if isinstance(baseline, dict):
            ingress = baseline.get('ingress')
            proc_inst = baseline.get('process_instance')
            if isinstance(ingress, dict) and type(ingress.get('Received')) is int and ingress.get('Received') >= 0:
                side_baseline = {'ingress': {'Received': ingress['Received']}}
                if isinstance(proc_inst, str) and proc_inst:
                    side_baseline['process_instance'] = proc_inst
        # Pass remaining deadline (absolute_expiry - now) to side sampler, not stale collector_deadline
        side_deadline = max(0.0, absolute_expiry - time.monotonic())
        collector.side_sampler = IndependentSideSampler(reader={
            'postgres': lambda: read_postgres_diagnostics(smoke, registry, pg_cid, timeout=2.0),
            'linux': lambda: read_linux_resources(registry, containers, timeout=0.5),
            'docker': lambda: read_container_resources(registry, containers, timeout=0.5, read_only=True)},
            max_samples=128,
            byte_cap=2 * 1024 * 1024,
            deadline_seconds=side_deadline,
            capture_seconds=capture_seconds,
            baseline_metrics=side_baseline)
    collector.start(None)
    if collector.side_sampler is not None:
        collector.side_sampler.start()
    return {'collector_ready': True, 'phase_id': phase_id}


def _read_diagnostic_sources(smoke, registry, pg_cid, containers, *, timeout=2.0):
    timeout = min(2.0, max(0.0, float(timeout)))
    deadline = time.monotonic() + timeout
    result = {}
    for key, reader in (('postgres', lambda budget: read_postgres_diagnostics(smoke, registry, pg_cid, timeout=budget)),
                        ('linux', lambda budget: read_linux_resources(registry, containers, timeout=budget))):
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            result[key] = {'available': False, 'reason': 'deadline_exceeded'}
            continue
        try:
            result[key] = reader(remaining)
        except Exception:
            result[key] = {'available': False, 'reason': 'reader_failed'}
    return result


def calculate_canonical_database_delta(before, after):
    """Compute exact canonical 4-key database delta from measured before/after counts.

    Fails closed on missing, malformed, boolean, negative, or reset count data.
    Returns exactly {'ledger': int, 'raw': int, 'temporal': int, 'configuration': int}.
    """
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise ValueError('e2_database_count_data_rejected')
    required_keys = ('ledger', 'raw', 'temporal', 'configuration')
    missing_b = [k for k in required_keys if k not in before]
    missing_a = [k for k in required_keys if k not in after]
    if missing_b:
        raise ValueError(f'e2_database_count_missing_key:{missing_b[0]}')
    if missing_a:
        raise ValueError(f'e2_database_count_missing_key:{missing_a[0]}')
    if any(k not in required_keys for k in before) or any(k not in required_keys for k in after):
        raise ValueError('e2_database_count_schema_rejected')
    delta = {}
    for key in required_keys:
        b_val = before[key]
        a_val = after[key]
        if type(b_val) is not int or type(a_val) is not int:
            raise ValueError(f'e2_database_count_not_int:{key}')
        if b_val < 0 or a_val < 0:
            raise ValueError(f'e2_database_count_negative:{key}')
        if a_val < b_val:
            raise ValueError(f'e2_database_count_reset:{key}')
        delta[key] = a_val - b_val
    return delta


def e2_database_counter_window(baseline, terminal):
    """Return real backend persistence-counter deltas, never ledger-derived counts."""
    fields = ('begin_attempts', 'begin_succeeded', 'confirmed_commits', 'copy_calls',
              'copy_returned_rows', 'confirmed_new_rows', 'fallback_attempts',
              'rollback_attempts', 'confirmed_rollbacks', 'commit_unconfirmed')
    if not isinstance(baseline, dict) or not isinstance(terminal, dict):
        return {'available': False, 'reason': 'db_counter_snapshot_missing', 'deltas': {}}
    before_p, after_p = baseline.get('persistence'), terminal.get('persistence')
    if not isinstance(before_p, dict) or not isinstance(after_p, dict):
        return {'available': False, 'reason': 'db_counter_snapshot_missing', 'deltas': {}}
    before_instance, after_instance = baseline.get('process_instance'), terminal.get('process_instance')
    if not before_instance or not after_instance or before_instance != after_instance:
        return {'available': False, 'reason': 'process_instance_missing_or_changed', 'deltas': {}}
    deltas = {}
    for field in fields:
        b, a = before_p.get(field), after_p.get(field)
        if type(b) is not int or type(a) is not int or b < 0 or a < b:
            return {'available': False, 'reason': 'db_counter_missing_or_reset:' + field, 'deltas': {}}
        deltas[field] = a - b
    return {'available': True, 'reason': None, 'process_instance': before_instance,
            'deltas': deltas}


def _target_counts(smoke, registry, pg, spec, *, timeout=None):
    sql = """SELECT json_build_object(
 'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),
 'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));"""
    kwargs = {'timeout': timeout} if timeout is not None else {}
    return json.loads(smoke.db_psql(registry, pg, sql, {'gw': spec.gateway_id,
        'sensor': spec.sensor_id, 'urn': spec.sensor_urn}, **kwargs).strip())


def finish_remote_collection(smoke, fixture, endpoint, registry, containers, prepared,
                             results_root, *, phase_id, paperload_binary):
    if phase_id != prepared['phase_id'] or not prepared.get('collector'):
        raise ValueError('e2_phase_collection_mismatch')
    collector = prepared['collector']
    bin_path = Path(paperload_binary).absolute()
    if bin_path.is_symlink() or not bin_path.is_file() or bin_path.stat().st_uid != os.getuid() or bin_path.stat().st_size > 64 << 20:
        raise ValueError('e2_binary_file_rejected')
    bin_sha256 = hashlib.sha256(bin_path.read_bytes()).hexdigest()
    imported = _load_e2_result_bundle(results_root, prepared, bin_path)
    collector_stop_joined = collector.stop(None)
    after_admin = _support._admin_metrics(smoke, endpoint, prepared['tokens']['admin'])
    is_equal_samples = (prepared.get('workload') == 'equal_samples'
                        or 'context' in prepared
                        or phase_id.startswith('equal-samples-'))
    compact_collector = isinstance(collector, EqualSamplesCompactSampler)
    official_side_path = prepared.get('profile') == 'official' and getattr(collector, 'side_sampler', None) is not None
    if compact_collector:
        after_res = (read_container_resources(registry, containers, timeout=0.5, read_only=True)
                     if official_side_path else read_container_resources(registry, containers))
        collector.record_terminal(after_admin, after_res)
    else:
        collector.metrics.append({'phase': 'workload_terminal', 'snapshot': after_admin})
        terminal_server = smoke.paper_p0_stats(registry, containers, 'workload_terminal')
        collector.resources.append(terminal_server)
    if getattr(collector, 'side_sampler', None) is not None:
        pg_entry = containers.get('postgres') if isinstance(containers, dict) else None
        try:
            pg_cid = (registry.resolve(pg_entry, timeout=2.0, read_only=True).get('Id')
                      if pg_entry and official_side_path else
                      registry.resolve(pg_entry).get('Id') if pg_entry else None)
        except Exception:
            pg_cid = None
        terminal_diag = _read_diagnostic_sources(smoke, registry, pg_cid, containers, timeout=2.0)
        side = collector.side_sampler
        prepared['diagnostic_terminal'] = terminal_diag
    after_global = smoke.paper_p1_count_db(registry, prepared['pg'])
    delta = calculate_canonical_database_delta(prepared['before_global'], after_global)
    after_targets = [_target_counts(smoke, registry, prepared['pg'], spec) for spec in prepared['specs']]
    scoped = sensor_scoped_delta(
        [{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id, 'counts': count}
         for spec, count in zip(prepared['specs'], prepared['before_targets'])],
        [{'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id, 'counts': count}
         for spec, count in zip(prepared['specs'], after_targets)])
    mapping = {'A': {'gateway_id': prepared['specs'][0].gateway_id,
                     'sensor_id': prepared['specs'][0].sensor_id},
               'B': {'gateway_id': prepared['specs'][1].gateway_id,
                     'sensor_id': prepared['specs'][1].sensor_id}}
    by_gateway, raw = {}, []
    if is_equal_samples:
        exp_counts = prepared['expected_counts']
        ctx = prepared['context']
        for index, (role, spec) in enumerate(zip(('A', 'B'), prepared['specs'])):
            counts = dict((scoped.get('available') or {}).get(
                f'{spec.gateway_id}/{spec.sensor_id}', {}))
            counts['configuration'] = (after_targets[index]['configuration']
                                       - prepared['before_targets'][index]['configuration'])
            by_gateway[spec.gateway_id] = counts
            raw.append(verify_e2_equal_samples_raw(
                smoke, registry, prepared['pg'], spec,
                expected_samples=exp_counts['per_role'][role]['samples'],
                expected_messages=exp_counts['per_role'][role]['messages'],
                samples_per_message=prepared['samples_per_message'],
                timestamp_anchor=ctx['timestamp_anchor'],
                sample_interval_us=ctx['sample_interval_us'],
                identity_samples=imported[role].get('identity_samples', []),
            ))
    else:
        for index, (role, spec) in enumerate(zip(('A', 'B'), prepared['specs'])):
            counts = dict((scoped.get('available') or {}).get(
                f'{spec.gateway_id}/{spec.sensor_id}', {}))
            counts['configuration'] = (after_targets[index]['configuration']
                                       - prepared['before_targets'][index]['configuration'])
            by_gateway[spec.gateway_id] = counts
            if prepared.get('profile') == 'official':
                raw.append({'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id,
                    'ledger_count': counts.get('ledger'), 'raw_count': counts.get('raw'),
                    'verified_exact': False, 'count_verified': True,
                    'verification_scope': 'transactional scoped DB delta; not per-row content audit'})
            else:
                producer_ids = [sample['message_id'] for sample in imported[role].get('identity_samples', [])
                                if isinstance(sample, dict) and isinstance(sample.get('message_id'), str)]
                raw.append(verify_pilot_raw(smoke, registry, prepared['pg'], spec, message_ids=producer_ids))
    ingress = _support.calculate_ingress_delta(prepared['baseline'], after_admin)
    runner = imported['runner']
    events = runner.get('phase_observations', {}).get('events', [])
    exec_ctx = {
        'samples_per_message': prepared['samples_per_message'],
        'role_to_target': mapping,
    }
    if prepared.get('profile') == 'official':
        exec_ctx.update(profile='official', role_manifests=prepared['manifests'])

    # Generator resources on B (ES.5b)
    gen_data = imported.get('generator_resources') or prepared.get('generator_resources')
    generator_res = qualify_generator_resources(gen_data, events)

    if is_equal_samples:
        exec_ctx['workload'] = 'equal_samples'
        exec_ctx['context'] = prepared['context']
        exec_ctx['expected_counts'] = prepared['expected_counts']
        validation = validate_e2_equal_samples_result(
            workload_a=imported['A'], workload_b=imported['B'],
            ingress_delta=ingress, database_delta=delta, database_by_gateway=by_gateway,
            phase_events=events, raw_verification=raw,
            execution_context=exec_ctx,
            resources={'server': collector.resources,
                       'generator_samples': generator_res.get('samples', []),
                       'generator_resources': generator_res,
                       'collector_errors': collector.errors})
    else:
        validation = validate_e2_result(
            workload_a=imported['A'], workload_b=imported['B'],
            ingress_delta=ingress, database_delta=delta, database_by_gateway=by_gateway,
            phase_events=events, raw_verification=raw,
            execution_context=exec_ctx,
            resources={'server': collector.resources, 'generator_samples': []})
    if not is_equal_samples and collector.errors:
        validation['valid'] = False
        validation['failures'].append('collector_incomplete:' + ','.join(collector.errors))
    if is_equal_samples:
        exp_counts = prepared['expected_counts']
        retained = collector.retained_snapshots if hasattr(collector, 'retained_snapshots') else [
            m.get('snapshot', m) for m in collector.metrics
        ]
        cov = collector.get_coverage() if hasattr(collector, 'get_coverage') else {
            'sampling_interval_seconds': 1.0,
            'deadline_seconds': 1800.0,
            'snapshots_captured': len(collector.metrics),
            'snapshots_retained': len(collector.metrics),
            'snapshots_evicted': getattr(collector, 'evicted_count', 0),
            'evicted_bytes': getattr(collector, 'evicted_bytes', 0),
            'ring_byte_cap': 2 * 1024 * 1024,
            'ring_bytes_used': 0,
            'collector_errors': list(collector.errors),
        }
        baseline_snap = getattr(collector, 'baseline_metrics', None) or prepared.get('baseline')
        terminal_snap = getattr(collector, 'terminal_metrics', None) or after_admin

        active_win = infer_host_a_active_window(
            snapshots=retained,
            baseline=baseline_snap,
            terminal=terminal_snap,
            expected_total_messages=exp_counts['total_messages'],
            expected_total_samples=exp_counts['total_samples'],
            sampling_interval=cov.get('sampling_interval_seconds', 1.0),
            evicted_count=cov.get('snapshots_evicted', 0),
        )

        base_res = getattr(collector, 'baseline_resources', None)
        term_res = getattr(collector, 'terminal_resources', None) or (after_res if 'after_res' in locals() else None)
        base_tot_cpu = base_res.get('total_cpu_cumulative_seconds') if isinstance(base_res, dict) else None
        term_tot_cpu = term_res.get('total_cpu_cumulative_seconds') if isinstance(term_res, dict) else None
        base_mono = baseline_snap.get('monotonic') if isinstance(baseline_snap, dict) else None
        term_mono = terminal_snap.get('monotonic') if isinstance(terminal_snap, dict) else None
        lifecycle_duration = (term_mono - base_mono) if (base_mono is not None and term_mono is not None) else None

        lifecycle_win = {
            'duration_seconds': lifecycle_duration,
            'baseline_timestamp': baseline_snap.get('timestamp') if isinstance(baseline_snap, dict) else None,
            'terminal_timestamp': terminal_snap.get('timestamp') if isinstance(terminal_snap, dict) else None,
            'baseline_cpu_seconds': base_tot_cpu,
            'terminal_cpu_seconds': term_tot_cpu,
        }

        ing_delta_dict = ingress.get('delta', {}) if isinstance(ingress, dict) and isinstance(ingress.get('delta'), dict) else {}
        tot_payload_bytes = ing_delta_dict.get('AcceptedBytes', 0)

        # Genuine cumulative transaction commit counter baseline->terminal if retained and monotonic
        base_pers = baseline_snap.get('persistence', {}) if isinstance(baseline_snap, dict) else {}
        term_pers = terminal_snap.get('persistence', {}) if isinstance(terminal_snap, dict) else {}
        db_counter_window = e2_database_counter_window(baseline_snap, terminal_snap)
        base_commits = base_pers.get('confirmed_commits')
        term_commits = term_pers.get('confirmed_commits')

        tot_db_tx = None
        db_tx_reason = None
        if isinstance(base_commits, (int, float)) and isinstance(term_commits, (int, float)) and not isinstance(base_commits, bool) and not isinstance(term_commits, bool):
            if term_commits >= base_commits:
                tot_db_tx = int(term_commits - base_commits)
            else:
                db_tx_reason = 'db_transactions_counter_reset'
        else:
            db_tx_reason = 'db_transactions_counter_not_retained_in_sampler'

        ledger_rows = delta.get('ledger', 0) if isinstance(delta, dict) else 0

        # Offered unique bytes vs backend admitted attempt bytes
        offered_bytes = None
        attempted_bytes = None
        if isinstance(imported, dict):
            offered_list = []
            attempted_list = []
            for arm_k in ('A', 'B'):
                w_arm = imported.get(arm_k, {})
                tel_arm = w_arm.get('telemetry', {}) if isinstance(w_arm, dict) else {}
                if isinstance(tel_arm.get('offered_unique_payload_bytes'), (int, float)):
                    offered_list.append(tel_arm['offered_unique_payload_bytes'])
                if isinstance(tel_arm.get('attempted_payload_bytes'), (int, float)):
                    attempted_list.append(tel_arm['attempted_payload_bytes'])
            if len(offered_list) == 2:
                offered_bytes = sum(offered_list)
            if len(attempted_list) == 2:
                attempted_bytes = sum(attempted_list)

        normalized = calculate_equal_samples_normalized_metrics(
            actual_n=exp_counts['total_samples'],
            total_payload_bytes=tot_payload_bytes,
            total_db_transactions=tot_db_tx,
            active_window=active_win,
            lifecycle_window=lifecycle_win,
            generator_resources=generator_res,
            db_transactions_unavailable_reason=db_tx_reason,
            ledger_rows=ledger_rows,
            offered_payload_bytes=offered_bytes,
            attempted_payload_bytes=attempted_bytes,
        )

        histograms = {
            'ack_latency': None,
            'batch_wait_latency': None,
            'persistence_latency': None,
        }
        if isinstance(after_admin, dict):
            ing_h = after_admin.get('ingress', {})
            pers_h = after_admin.get('persistence', {})
            if isinstance(ing_h, dict):
                histograms['ack_latency'] = ing_h.get('ack_latency')
            if isinstance(pers_h, dict):
                histograms['batch_wait_latency'] = pers_h.get('batch_wait_latency')
                histograms['persistence_latency'] = pers_h.get('persistence_latency')

        per_metric = {
            'payload_bytes': {
                'status': 'AVAILABLE' if normalized.get('bytes_per_sample') is not None else 'INCONCLUSIVE',
                'availability': 'available' if normalized.get('bytes_per_sample') is not None else 'inconclusive',
                'reason': None if normalized.get('bytes_per_sample') is not None else 'payload_bytes_unavailable',
            },
            'db_transactions': {
                'status': 'AVAILABLE' if normalized.get('db_transactions_available') else 'INCONCLUSIVE',
                'availability': 'available' if normalized.get('db_transactions_available') else 'inconclusive',
                'reason': normalized.get('db_transactions_unavailable_reason'),
            },
            'server_active_cpu': {
                'status': 'AVAILABLE' if normalized.get('active_cpu_available') else 'INCONCLUSIVE',
                'availability': 'available' if normalized.get('active_cpu_available') else 'inconclusive',
                'reason': normalized.get('active_cpu_unavailable_reason'),
            },
            'server_lifecycle_cpu': {
                'status': 'AVAILABLE' if normalized.get('lifecycle_cpu_available') else 'INCONCLUSIVE',
                'availability': 'available' if normalized.get('lifecycle_cpu_available') else 'inconclusive',
                'reason': normalized.get('lifecycle_cpu_unavailable_reason'),
            },
            'generator_lifecycle_cpu': {
                'status': 'AVAILABLE' if normalized.get('generator_lifecycle_cpu_available') else 'INCONCLUSIVE',
                'availability': 'available' if normalized.get('generator_lifecycle_cpu_available') else 'inconclusive',
                'reason': normalized.get('generator_lifecycle_cpu_unavailable_reason'),
            },
            'generator_active_cpu': {
                'status': 'AVAILABLE' if normalized.get('generator_active_cpu_available') else 'INCONCLUSIVE',
                'availability': 'available' if normalized.get('generator_active_cpu_available') else 'inconclusive',
                'reason': normalized.get('generator_active_cpu_unavailable_reason'),
            },
        }

        measurement_quality = {
            'availability': 'available' if active_win.get('status') == 'AVAILABLE' and not cov.get('collector_errors') else 'inconclusive',
            'per_metric': per_metric,
            'active_window': active_win,
            'lifecycle_window': lifecycle_win,
            'coverage': cov,
            'normalized_metrics': normalized,
            'histograms': histograms,
        }

        server_res = {
            'lifecycle_cpu_seconds': {
                'backend': None, 'db': None, 'broker': None, 'proxy': None, 'total': None,
            },
            'active_cpu_seconds': active_win.get('active_cpu_seconds'),
            'sampled_max_memory_bytes': active_win.get('sampled_max_memory_bytes'),
            'availability': 'available' if base_tot_cpu is not None and term_tot_cpu is not None else 'unavailable',
            'reason': None if base_tot_cpu is not None and term_tot_cpu is not None else 'docker_cumulative_cpu_unavailable',
        }
        if isinstance(base_res, dict) and isinstance(term_res, dict):
            b_svcs = base_res.get('services') if 'services' in base_res else base_res
            t_svcs = term_res.get('services') if 'services' in term_res else term_res
            if isinstance(b_svcs, dict) and isinstance(t_svcs, dict):
                for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
                    b_cpu = b_svcs.get(svc, {}).get('cpu_cumulative_seconds') if isinstance(b_svcs.get(svc), dict) else None
                    t_cpu = t_svcs.get(svc, {}).get('cpu_cumulative_seconds') if isinstance(t_svcs.get(svc), dict) else None
                    key = 'db' if svc == 'postgres' else ('broker' if svc == 'mosquitto' else ('proxy' if svc == 'nginx' else svc))
                    if isinstance(b_cpu, (int, float)) and isinstance(t_cpu, (int, float)):
                        server_res['lifecycle_cpu_seconds'][key] = (t_cpu - b_cpu) if t_cpu >= b_cpu else None
                if all(server_res['lifecycle_cpu_seconds'][k] is not None for k in ('backend', 'db', 'broker', 'proxy')):
                    server_res['lifecycle_cpu_seconds']['total'] = sum(server_res['lifecycle_cpu_seconds'][k] for k in ('backend', 'db', 'broker', 'proxy'))

        resources_dict = {
            'server': server_res,
            'generator': generator_res,
        }

        try:
            if hasattr(smoke, 'ROOT') and isinstance(smoke.ROOT, (str, Path)) and Path(str(smoke.ROOT)).is_dir():
                source_fp = _support._source_fingerprint(smoke.ROOT)
            else:
                source_fp = prepared.get('source_fingerprint', {'revision': 'unknown', 'dirty': False, 'dirty_source_sha256': None})
        except Exception:
            source_fp = prepared.get('source_fingerprint', {'revision': 'unknown', 'dirty': False, 'dirty_source_sha256': None})

        eff_flags = prepared.get('effective_flags')
        if not eff_flags and prepared.get('effective'):
            try:
                eff_flags = _validate_fixture_flags(prepared['effective'], fixture)
            except Exception:
                eff_flags = None
        if not eff_flags:
            eff_flags = {
                'STAGE5_ENABLED': False,
                'STAGE5_WAKEUP_ENABLED': False,
                'WEBSOCKET_ENABLED': False,
                'MQTT_SHAPER_ENABLED': False,
                'MQTT_MICROBATCH_ENABLED': True,
                'MQTT_RAM_DEDUP_ENABLED': False,
            }

        raw_images = prepared.get('images') or prepared.get('image_ids')
        def _extract_img_id(v):
            if isinstance(v, dict):
                return v.get('id') or v.get('Id')
            return v
        norm_image_ids = {k: _extract_img_id(v) for k, v in raw_images.items()} if isinstance(raw_images, dict) else None
        cleanup_verified = prepared.get('cleanup_verified', prepared.get('exact_cleanup_verified', True))

        result = {
            'schema_version': 1,
            'status': 'PASS' if validation['valid'] else 'FAIL',
            'workload_outcome': 'VALIDATED' if validation['valid'] else 'VALIDATION_FAILED',
            'clean_comparison': validation.get('clean_comparison', False),
            'contamination_reasons': validation.get('contamination_reasons', []),
            'experiment': 'E2',
            'workload': 'equal_samples',
            'workload_type': 'equal_samples',
            'phase_id': phase_id,
            'samples_per_message': prepared['samples_per_message'],
            'context': prepared['context'],
            'context_sha256': prepared['context']['context_sha256'],
            'seed': prepared['context'].get('seed') if isinstance(prepared.get('context'), dict) else prepared.get('seed'),
            'expected_counts': prepared['expected_counts'],
            'applied_retry_policy': validate_applied_retry_policy(prepared, imported),
            'binary_provenance': imported.get('binary_provenance', imported.get('provenance', {})),
            'actual_counters': {
                'A': imported['A'].get('telemetry', {}),
                'B': imported['B'].get('telemetry', {}),
                'equal_samples_A': imported['A'].get('equal_samples', {}),
                'equal_samples_B': imported['B'].get('equal_samples', {}),
            },
            'workloads': {'A': imported['A'], 'B': imported['B']},
            'runner': runner,
            'database_delta': delta,
            'database_by_gateway': by_gateway,
            'raw_verification': raw,
            'ingress_delta': ingress,
            'validation': validation,
            'measurement_quality': measurement_quality,
            'database_counter_window': db_counter_window,
            'normalized_metrics': measurement_quality.get('normalized_metrics'),
            'resources': resources_dict,
            'collector_errors': collector.errors,
            'collector_metrics': collector.metrics if hasattr(collector, 'metrics') else [],
            'collector_resources': collector.resources if hasattr(collector, 'resources') else [],
            'collector_stop_joined': collector_stop_joined,
            'full_p8_accepted': False,
            'generator_colocated': 'unverified',
            'physical_ab': 'NOT RUN',
            'source_fingerprint': source_fp,
            'schema': prepared.get('schema', '24:telemetry_ledger_timestamp'),
            'logical_topology': {'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            'workers': {'workers': 2, 'connections': 1, 'pending_messages': 64},
            'effective_flags': eff_flags,
            'quota': prepared.get('quota', {}),
            'cleanup_verified': cleanup_verified,
            'exact_cleanup_verified': cleanup_verified,
            'cleanup_provenance': prepared.get('cleanup_provenance', {'exact_cleanup_verified': cleanup_verified, 'cleanup_counts': {'containers': 5}}),
            'image_ids': norm_image_ids,
            'images': raw_images,
            'binary_sha256': bin_sha256,
            'paperload_binary_sha256': bin_sha256,
            'binary_path': 'bin/paperload',
        }
        artifact_name = f'p8-e2-{phase_id}-remote-result.json'
    else:
        result = {
            'status': 'PASS' if validation['valid'] else 'FAIL',
            'workload_outcome': 'VALIDATED' if validation['valid'] else 'VALIDATION_FAILED',
            'experiment': 'E2',
            'phase_id': phase_id,
            'samples_per_message': prepared['samples_per_message'],
            'workloads': {'A': imported['A'], 'B': imported['B']},
            'runner': runner,
            'database_delta': delta,
            'database_by_gateway': by_gateway,
            'raw_verification': raw,
            'ingress_delta': ingress,
            'validation': validation,
            'collector_errors': collector.errors,
            'collector_metrics': collector.metrics,
            'collector_resources': collector.resources,
            'collector_stop_joined': collector_stop_joined,
            'full_p8_accepted': False,
            'generator_colocated': True,
            'physical_ab': 'NOT RUN',
        }
        if prepared.get('profile') == 'official':
            limits = prepared['manifests']['A']['limits']
            result['pending_limits'] = {key: limits[key] for key in ('pending_messages', 'pending_bytes')}
        artifact_name = f'p8-e2-samples-{prepared["samples_per_message"]}-remote-result.json'
    if getattr(collector, 'side_sampler', None) is not None and prepared.get('_failure_diagnostics_result') is None:
        envelope = failure_diagnostics(prepared, result.get('workload_outcome', result.get('status', 'FINISHED')),
            stop_timeout_seconds=0, terminal_data={'metrics': after_admin,
                'resources': after_res if compact_collector else None,
                'postgres_linux': prepared.get('diagnostic_terminal'),
                'database_counts': {'available': True, 'delta': delta},
                'collector_stop_joined': collector_stop_joined})
        smoke.durable_json(fixture.artifacts / 'p8-e2-diagnostics.json', envelope)
    smoke.durable_json(fixture.artifacts / artifact_name, result)
    return result


def _load_e2_result_bundle(results_root, prepared, paperload_binary):
    root = Path(results_root).absolute()
    if root.is_symlink() or not root.is_dir() or root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
        raise ValueError('e2_result_root_rejected')
    summary_path = root / 'results.json'
    if (summary_path.is_symlink() or not summary_path.is_file()
            or summary_path.stat().st_uid != os.getuid() or summary_path.stat().st_mode & 0o077
            or summary_path.stat().st_size > 8 << 20):
        raise ValueError('e2_result_summary_rejected')
    summary = json.loads(summary_path.read_text())
    expected_roles = ['A', 'B']
    expected_session = {'session_id': prepared['session_id'],
            'run_ids': [prepared['run_id']], 'experiment': 'E2', 'arm': prepared['arm'],
            'phase': prepared['phase_id']}
    portable_session = {**expected_session, 'profile': prepared['profile']}
    if (summary.get('session') not in (expected_session, portable_session)
            or summary.get('source_bundle_sha256') != prepared['bundle_sha256']
            or summary.get('expected_workload_roles') != expected_roles
            or summary.get('present_workload_roles') != expected_roles
            or summary.get('missing_workload_roles') != [] or summary.get('physical_ab') != 'NOT RUN'
            or summary.get('full_p8_accepted') is not False
            or summary.get('runner_status') not in ('COMPLETE', 'PARTIAL', 'FAILED')):
        raise ValueError('e2_result_correlation_rejected')
    bound_manifests = summary.get('bound_manifest_sha256')
    source_inventory = summary.get('source_inventory')
    prep_inventory = prepared.get('bundle_inventory')
    if not isinstance(source_inventory, list) or not isinstance(prep_inventory, list):
        raise ValueError('e2_result_manifest_provenance_rejected')
    norm_source_inv = [
        {'path': row['path'], 'sha256': row['sha256']}
        for row in source_inventory
        if isinstance(row, dict) and 'path' in row and 'sha256' in row
    ]
    norm_prep_inv = [
        {'path': row['path'], 'sha256': row['sha256']}
        for row in prep_inventory
        if isinstance(row, dict) and 'path' in row and 'sha256' in row
    ]
    if (len(norm_source_inv) != len(source_inventory)
            or len(norm_prep_inv) != len(prep_inventory)
            or norm_source_inv != norm_prep_inv
            or not isinstance(bound_manifests, dict) or set(bound_manifests) != {'A', 'B'}
            or any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value)
                   for value in bound_manifests.values())
            or summary.get('bound_gateway_environment_sha256') != {}
            or not isinstance(summary.get('bound_run_spec_sha256'), str)
            or not re.fullmatch(r'[0-9a-f]{64}', summary['bound_run_spec_sha256'])):
        raise ValueError('e2_result_manifest_provenance_rejected')

    if 'bound_manifest_sha256' in prepared and isinstance(prepared['bound_manifest_sha256'], dict):
        if bound_manifests != prepared['bound_manifest_sha256']:
            raise ValueError('e2_manifest_hash_mismatch')

    # Authenticate portable bundle manifests via inventory using bounded owned private no-symlink checks
    source_inv_map = {row['path']: row['sha256'] for row in norm_prep_inv}
    if 'bundle_root' in prepared and prepared['bundle_root']:
        b_root = Path(prepared['bundle_root']).absolute()
        if (b_root.is_symlink() or not b_root.is_dir()
                or b_root.stat().st_uid != os.getuid()
                or b_root.stat().st_mode & 0o077):
            raise ValueError('e2_source_manifest_inventory_mismatch')
        for arm in expected_roles:
            inv_hash = source_inv_map.get(f'manifests/{arm}.json')
            if not inv_hash or not re.fullmatch(r'[0-9a-f]{64}', inv_hash):
                raise ValueError('e2_source_manifest_inventory_mismatch')
            manifest_file = b_root / 'manifests' / f'{arm}.json'
            if (manifest_file.is_symlink() or not manifest_file.is_file()
                    or manifest_file.stat().st_uid != os.getuid()
                    or manifest_file.stat().st_mode & 0o077
                    or manifest_file.stat().st_size > 8 << 20):
                raise ValueError('e2_source_manifest_inventory_mismatch')
            mf_hash = hashlib.sha256(manifest_file.read_bytes()).hexdigest()
            if mf_hash != inv_hash:
                raise ValueError('e2_source_manifest_inventory_mismatch')
    elif 'manifest_paths' in prepared and isinstance(prepared['manifest_paths'], dict):
        # Historical legacy path compatibility
        for arm in expected_roles:
            if arm not in prepared['manifest_paths']:
                raise ValueError('e2_source_manifest_inventory_mismatch')
            inv_hash = source_inv_map.get(f'manifests/{arm}.json')
            if not inv_hash or not re.fullmatch(r'[0-9a-f]{64}', inv_hash):
                raise ValueError('e2_source_manifest_inventory_mismatch')
            mp = Path(prepared['manifest_paths'][arm]).absolute()
            if (mp.is_symlink() or not mp.is_file()
                    or mp.stat().st_uid != os.getuid()
                    or mp.stat().st_mode & 0o077
                    or mp.stat().st_size > 8 << 20):
                raise ValueError('e2_source_manifest_inventory_mismatch')
            src_hash = hashlib.sha256(mp.read_bytes()).hexdigest()
            if src_hash != inv_hash:
                raise ValueError('e2_source_manifest_inventory_mismatch')

    files = {f'results/workloads/{arm}/workload.json': arm for arm in expected_roles}
    files.update({'results/runner/run.json': 'runner', 'results/runner/provenance.json': 'provenance'})
    gen_rel = 'results/runner/resources-generator.json'
    inventory = summary.get('files')
    if not isinstance(inventory, list):
        raise ValueError('e2_result_inventory_rejected')
    has_gen = any(isinstance(row, dict) and row.get('path') == gen_rel for row in inventory)
    if has_gen:
        files[gen_rel] = 'generator_resources'
    if len(inventory) != len(files):
        raise ValueError('e2_result_inventory_rejected')
    values, seen = {}, set()

    bin_path = Path(paperload_binary).absolute()
    if bin_path.is_symlink() or not bin_path.is_file() or bin_path.stat().st_uid != os.getuid() or bin_path.stat().st_size > 64 << 20:
        raise ValueError('e2_binary_file_rejected')
    expected_bin_sha256 = hashlib.sha256(bin_path.read_bytes()).hexdigest()

    for row in inventory:
        rel = row.get('path') if isinstance(row, dict) else None
        if rel not in files or rel in seen or row.get('size_bytes', 1 << 30) > 8 << 20:
            raise ValueError('e2_result_inventory_rejected')
        seen.add(rel)
        path = root / rel
        if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077 or path.stat().st_size > 8 << 20:
            raise ValueError('e2_result_file_rejected')
        data = path.read_bytes()
        if len(data) != row['size_bytes'] or hashlib.sha256(data).hexdigest() != row.get('sha256'):
            raise ValueError('e2_result_digest_mismatch')
        value = json.loads(data)
        role = files[rel]
        if role == 'provenance':
            if (value.get('binary_sha256') != expected_bin_sha256
                    or value.get('binary_os') != __import__('sys').platform
                    or value.get('binary_arch') != __import__('platform').machine()):
                raise ValueError('e2_binary_provenance_mismatch')
            values[role] = value
        elif role in ('A', 'B'):
            if (value.get('experiment') != 'E2' or value.get('arm') != role
                    or value.get('run_id') != prepared['run_id'] or value.get('mode') != 'remote'
                    or value.get('profile') != prepared.get('profile', 'pilot')):
                raise ValueError('e2_workload_provenance_mismatch')
            values[role] = value
        elif role == 'generator_resources':
            paper_p8_client._safe_generator_resources(value)
            values[role] = value
        else:
            values[role] = value
    if seen != set(files) or summary.get('binary') != values.get('provenance'):
        raise ValueError('e2_result_roles_missing')
    values['binary_provenance'] = values['provenance']

    # Policy runs require output-side proof from the Go runner, not merely the
    # child's self-reported summary or a rehashed input inventory.
    expected_policy = validate_applied_retry_policy(prepared, values)

    if prepared.get('workload') == 'equal_samples':
        is_equal_samples = True
    elif prepared.get('workload') == 'steady_burst':
        is_equal_samples = False
    else:
        is_equal_samples = ('context' in prepared or prepared.get('phase_id', '').startswith('equal-samples-'))
    if is_equal_samples:
        expected_ctx_sha = (prepared.get('context', {}).get('context_sha256')
                            or prepared.get('context_sha256'))
        expected_seed = prepared.get('seed')
        if expected_seed is None and 'context' in prepared:
            expected_seed = prepared['context'].get('seed')

        for arm in expected_roles:
            workload = values.get(arm)
            if not isinstance(workload, dict) or 'equal_samples' not in workload:
                raise ValueError('e2_equal_samples_missing_in_workload')
            eq = workload['equal_samples']
            if not isinstance(eq, dict):
                raise ValueError('e2_equal_samples_missing_in_workload')

            # Apply strict safe_equal_samples validation on imported workload
            paper_p8_client._safe_equal_samples(eq, role=arm)

            if expected_seed is not None:
                if 'seed' in workload and workload['seed'] != expected_seed:
                    raise ValueError('e2_seed_mismatch')
                if 'seed' in eq and eq['seed'] != expected_seed:
                    raise ValueError('e2_seed_mismatch')

            if eq.get('workload_type') != 'equal_samples':
                raise ValueError('e2_equal_samples_workload_type_mismatch')
            if eq.get('logical_producer') != arm:
                raise ValueError('e2_equal_samples_logical_producer_mismatch')
            if expected_ctx_sha and eq.get('context_sha256') != expected_ctx_sha:
                raise ValueError('e2_equal_samples_context_hash_mismatch')
            if 'samples_per_message' in prepared and eq.get('samples_per_message') != prepared['samples_per_message']:
                raise ValueError('e2_equal_samples_samples_per_message_mismatch')

            if prepared.get('context'):
                ctx = prepared['context']
                validate_e2_equal_samples_context(ctx, check_freshness=False)
                for field in ('total_samples', 'sample_rate', 'duration_seconds',
                              'producer_sample_rate', 'sample_interval_us'):
                    if field in ctx and eq.get(field) != ctx[field]:
                        raise ValueError(f'e2_equal_samples_context_field_mismatch:{field}')
                if expected_seed is not None and ctx.get('seed') != expected_seed:
                    raise ValueError('e2_seed_mismatch')

            if prepared.get('expected_counts'):
                exp_counts = prepared['expected_counts']['per_role'][arm]
                if (eq.get('expected_samples') != exp_counts['samples']
                        or eq.get('expected_messages') != exp_counts['messages']):
                    raise ValueError('e2_equal_samples_expected_counts_mismatch')

        eq_a = values['A']['equal_samples']
        eq_b = values['B']['equal_samples']
        if eq_a.get('context_sha256') != eq_b.get('context_sha256'):
            raise ValueError('e2_equal_samples_context_hash_mismatch')
        for ck in ('total_samples', 'sample_rate', 'duration_seconds', 'producer_sample_rate',
                   'sample_interval_us', 'samples_per_message'):
            if eq_a.get(ck) != eq_b.get(ck):
                raise ValueError(f'e2_equal_samples_peer_mismatch:{ck}')
    else:
        if 'equal_samples' in values.get('A', {}) or 'equal_samples' in values.get('B', {}):
            raise ValueError('e2_workload_mode_mismatch')

    return values


def build_e2_execution_plan(*, output_dir, manifest_a, manifest_b, seed=None,
                            official_opt_in=False, profile='pilot'):
    """Validate private provisioned role manifests and emit a bounded parallel run-spec."""
    if profile == 'official' and not official_opt_in:
        raise ValueError('official_profile_requires_opt_in')
    if profile not in ('pilot', 'official'):
        raise ValueError('e2_profile_rejected')
    root = Path(output_dir)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    loaded = {}
    is_equal_samples = False
    effective_seed = seed
    for arm, path in (('A', manifest_a), ('B', manifest_b)):
        source = Path(path)
        if source.is_symlink() or not source.is_file() or source.stat().st_mode & 0o077 or source.stat().st_size > 1 << 20:
            raise ValueError('e2_manifest_input_rejected')
        try:
            m = json.loads(source.read_text(encoding='utf-8'))
        except (UnicodeError, json.JSONDecodeError):
            raise ValueError('e2_manifest_invalid') from None
        if not isinstance(m, dict):
            raise ValueError('e2_manifest_invalid')
        targets = m.get('targets')
        credentials = m.get('gateway_identity_files')
        if (m.get('experiment') != 'E2' or m.get('arm') != arm or m.get('profile') != profile
                or m.get('roles') != ['telemetry']
                or not isinstance(targets, list) or len(targets) != 1
                or not isinstance(targets[0], dict) or not targets[0].get('gateway_id')
                or not targets[0].get('sensor_id')
                or not isinstance(credentials, dict) or len(credentials) != 1
                or set(credentials) != {targets[0]['gateway_id']}
                or not next(iter(credentials.values()), None)
                or not isinstance(m.get('identity_files'), dict)
                or not m['identity_files'].get('telemetry')):
            raise ValueError('e2_role_manifest_mismatch')
        if (not isinstance(m.get('tls'), dict) or not all(m['tls'].get(k) for k in
                ('https_endpoint', 'https_ca_file', 'mqtt_tls_endpoint', 'mqtt_tls_ca_file'))):
            raise ValueError('e2_security_values_missing')

        if 'equal_samples' in m:
            is_equal_samples = True
            eq = m['equal_samples']
            if not isinstance(eq, dict):
                raise ValueError('e2_equal_samples_manifest_invalid')
            if set(eq.keys()) != E2_EQUAL_SAMPLES_MANIFEST_KEYS:
                raise ValueError('e2_equal_samples_manifest_keys_rejected')
            if eq.get('sample_stream_version') != 1 or type(eq.get('sample_stream_version')) is not int:
                raise ValueError('e2_sample_stream_version_rejected')
            ctx_part = {k: v for k, v in eq.items() if k in E2_EQUAL_SAMPLES_CONTEXT_KEYS}
            validate_e2_equal_samples_context(ctx_part, check_freshness=False)
            if m.get('seed') != eq['seed']:
                raise ValueError('e2_role_manifest_mismatch')
            if seed is not None and m.get('seed') != seed:
                raise ValueError('e2_role_manifest_mismatch')
            effective_seed = m.get('seed')
            S = m.get('samples_per_message')
            if S not in (1, 100) or type(S) is not int:
                raise ValueError('e2_samples_profile_rejected')
            expected_counts = calculate_e2_equal_samples_expected_counts(ctx_part, S)
            role_expected = expected_counts['per_role'][arm]
            if (eq.get('expected_samples') != role_expected['samples']
                    or eq.get('expected_messages') != role_expected['messages']):
                raise ValueError('e2_equal_samples_counts_mismatch')
            if m.get('topology') != {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0}:
                raise ValueError('e2_pilot_bounds_rejected')
            duration_seconds = ctx_part['duration_seconds']
            rate = ctx_part['producer_sample_rate'] / S
            expected_rate = int(rate) if rate.is_integer() else rate
            phases = m.get('phases')
            traffic = m.get('traffic')
            if (not isinstance(phases, dict)
                    or phases.get('warmup_seconds') != 0
                    or phases.get('measure_seconds') != duration_seconds
                    or phases.get('drain_seconds') != 20
                    or phases.get('cooldown_seconds') != 0):
                raise ValueError('e2_pilot_bounds_rejected')
            if (not isinstance(traffic, dict)
                    or traffic.get('burst_messages') != 0
                    or traffic.get('ramp_seconds') != 0
                    or traffic.get('rate_per_second') != expected_rate):
                raise ValueError('e2_pilot_bounds_rejected')
            limits = m.get('limits')
            if (not isinstance(limits, dict)
                    or limits.get('retries') != 16
                    or limits.get('workers') != 2
                    or limits.get('connections') != 1
                    or limits.get('pending_messages') != 64
                    or limits.get('wall_seconds') < duration_seconds + 20
                    or m.get('retry_interval_ms') != 500):
                raise ValueError('e2_pilot_bounds_rejected')
        else:
            expected_seed = PILOT_SEED if seed is None else seed
            if m.get('seed') != expected_seed:
                raise ValueError('e2_role_manifest_mismatch')
            effective_seed = expected_seed
            if profile == 'pilot' and (m.get('samples_per_message') not in PILOT_SAMPLES
                    or m.get('topology') != {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0}
                    or m.get('phases', {}).get('measure_seconds') != (8 if arm == 'A' else 4)
                    or m.get('traffic', {}).get('burst_messages') != (0 if arm == 'A' else 12)
                    or m.get('limits', {}).get('retries') != 16
                    or m.get('retry_interval_ms') != 500
                    or m.get('limits', {}).get('workers') != 2
                    or m.get('limits', {}).get('connections') != 1):
                raise ValueError('e2_pilot_bounds_rejected')
            if profile == 'official':
                traffic, phases = m.get('traffic'), m.get('phases')
                if (m.get('samples_per_message') not in PILOT_SAMPLES
                        or type(m.get('samples_per_message')) is not int
                        or not isinstance(traffic, dict) or type(traffic.get('rate_per_second')) is not int
                        or type(traffic.get('total_rate_per_second')) is not int
                        or traffic.get('total_rate_per_second') not in OFFICIAL_RATES
                        or type(phases.get('measure_seconds')) is not int or phases['measure_seconds'] < 1
                        or traffic.get('burst_messages', -1) < 0):
                    raise ValueError('e2_official_bounds_rejected')
        loaded[arm] = m

    a, b = loaded['A'], loaded['B']
    if a.get('retry_policy') != b.get('retry_policy'):
        raise ValueError('e2_retry_policy_mismatch')
    if a.get('retry_policy') is not None:
        validate_retry_policy(a['retry_policy'], pending_messages=a['limits']['pending_messages'])
    if ('equal_samples' in a) != ('equal_samples' in b):
        raise ValueError('e2_workload_mismatch')
    for key in ('seed', 'samples_per_message', 'tls', 'limits', 'topology', 'durability_mode'):
        if a.get(key) != b.get(key):
            raise ValueError('e2_shared_profile_mismatch:' + key)
    if a['targets'][0]['gateway_id'] == b['targets'][0]['gateway_id']:
        raise ValueError('e2_gateway_targets_not_distinct')
    if a.get('limits', {}).get('connections') != 1:
        raise ValueError('e2_connection_bound_rejected')
    if a.get('traffic', {}).get('rate_per_second') != b.get('traffic', {}).get('rate_per_second'):
        if profile != 'official':
            raise ValueError('e2_rate_mismatch')
    stop_timeout_seconds = 30
    if profile == 'official':
        if (a['traffic']['total_rate_per_second'] != b['traffic']['total_rate_per_second']
                or a['traffic']['rate_per_second'] + b['traffic']['rate_per_second']
                    != a['traffic']['total_rate_per_second']
                or a['phases']['measure_seconds'] != b['phases']['measure_seconds']):
            raise ValueError('e2_official_rate_distribution_mismatch')
    if profile == 'official':
        # The lifecycle stop/drain budget is deliberately independent from the
        # (potentially two-hour) producer wall. Bundle validation caps it at 300s.
        stop_timeout_seconds = 30
    elif is_equal_samples:
        eq_a = a['equal_samples']
        eq_b = b['equal_samples']
        if eq_a['context_sha256'] != eq_b['context_sha256']:
            raise ValueError('e2_equal_samples_context_hash_mismatch')
        for ck in E2_EQUAL_SAMPLES_CONTEXT_KEYS:
            if eq_a[ck] != eq_b[ck]:
                raise ValueError(f'e2_equal_samples_context_mismatch:{ck}')

    outputs = []
    for arm, source in (('A', manifest_a), ('B', manifest_b)):
        dst = root / f'{arm}.manifest.json'
        with dst.open('x', encoding='utf-8') as out:
            json.dump(loaded[arm], out, sort_keys=True, separators=(',', ':'))
        dst.chmod(0o600)
        outdir = root / f'{arm}-output'
        outputs.append({'manifest': str(dst), 'output_dir': str(outdir), 'role': 'telemetry',
                        'ready_timeout_seconds': 90})
    if is_equal_samples:
        duration_seconds = a['equal_samples']['duration_seconds']
        # Workload wall governs production only; shutdown/drain remains bounded.
        stop_timeout_seconds = 30
    from scripts.paper_p8_resources import disk_guard_config
    spec = {'paperload': outputs, 'stop_timeout_seconds': stop_timeout_seconds,
            'disk_guard': disk_guard_config([a, b])}
    run_spec = root / 'e2-run-spec.json'
    with run_spec.open('x', encoding='utf-8') as out:
        json.dump(spec, out, sort_keys=True, indent=2)
    run_spec.chmod(0o600)
    return {'run_spec': str(run_spec), 'roles': ['telemetry:A', 'telemetry:B'],
            'seed': effective_seed, 'execution': 'NOT RUN', 'official_opt_in': bool(official_opt_in),
            'profile': profile,
            'expected_counts': ({'unique': a['traffic']['total_rate_per_second'] * a['phases']['measure_seconds']
                    + b['traffic'].get('burst_messages', 0),
                'raw': (a['traffic']['total_rate_per_second'] * a['phases']['measure_seconds']
                    + b['traffic'].get('burst_messages', 0)) * a['samples_per_message'],
                'temporal': a['traffic']['total_rate_per_second'] * a['phases']['measure_seconds']
                    + b['traffic'].get('burst_messages', 0),
                'ack': a['traffic']['total_rate_per_second'] * a['phases']['measure_seconds']
                    + b['traffic'].get('burst_messages', 0)}
                if profile == 'official' else None)}


def e2_official_estimate(*, rate_per_second, duration_seconds, samples_per_message,
                         payload_bytes, official_opt_in=False, max_duration_seconds=7200, max_messages=1000000,
                         max_raw_rows=100000000, max_disk_bytes=10 << 30,
                         raw_row_bytes=256, index_bytes_per_row=64, wal_multiplier=2,
                         pending_memory_bytes=256 << 20, retry_memory_bytes=256 << 20,
                         application_bandwidth_bytes_per_second=100 << 20,
                          burst_messages=0, pending_messages=64, pending_bytes=1 << 20,
                          retry_policy=None, shaper_rate_per_gateway=500, shaper_burst_per_gateway=500):
    """Conservative sizing estimate, not a measurement or wire-byte estimate."""
    import math
    if not official_opt_in:
        raise ValueError('official_profile_requires_opt_in')
    if (rate_per_second not in OFFICIAL_RATES or type(duration_seconds) is not int or duration_seconds < 1
            or type(samples_per_message) is not int or samples_per_message not in PILOT_SAMPLES
            or type(payload_bytes) is not int or payload_bytes < 1
            or type(burst_messages) is not int or not 0 <= burst_messages <= 100000
            or type(pending_messages) is not int or not 1 <= pending_messages <= 100000
            or type(pending_bytes) is not int or not 1 <= pending_bytes <= 1 << 30
            or 2 * (pending_bytes + pending_messages * 4096) > 256 << 20
            or type(shaper_rate_per_gateway) is not int or shaper_rate_per_gateway < 1
            or type(shaper_burst_per_gateway) is not int or shaper_burst_per_gateway < 0):
        raise ValueError('e2_estimate_input_rejected')
    if retry_policy is not None:
        retry_policy = validate_retry_policy(retry_policy, pending_messages=pending_messages)
    messages = math.ceil(rate_per_second * duration_seconds) + burst_messages
    raw_rows = messages * samples_per_message
    payload_estimate = messages * payload_bytes
    database_estimate = raw_rows * (raw_row_bytes + index_bytes_per_row) * wal_multiplier
    total_storage_estimate = payload_estimate + database_estimate
    pending_estimate = 2 * (pending_bytes + pending_messages * 4096)
    # The policy-only Go channel is one UUID slot per pending message per producer.
    # Add its 16-byte slots once; the existing 4096/message allowance remains a
    # conservative structure/bookkeeping allowance, not RSS and not a second queue.
    retry_queue_estimate = 2 * pending_messages * 16 if retry_policy is not None else 0
    retry_estimate = retry_queue_estimate
    bandwidth_estimate = rate_per_second * payload_bytes
    combined_memory_estimate = pending_estimate + retry_estimate
    producer_unique_rate = rate_per_second / 2
    retry_rate = retry_policy['rate_per_second'] if retry_policy else 0
    retry_burst = retry_policy['burst'] if retry_policy else 0
    retry_headroom_not_reserved = producer_unique_rate + retry_rate > shaper_rate_per_gateway
    if (duration_seconds > max_duration_seconds or messages > max_messages or raw_rows > max_raw_rows
            or total_storage_estimate > max_disk_bytes or combined_memory_estimate > pending_memory_bytes
            or retry_estimate > retry_memory_bytes
            or bandwidth_estimate > application_bandwidth_bytes_per_second):
        raise ValueError('e2_predicted_bound_exceeded')
    return {'estimate_only': True, 'rate_per_second': rate_per_second,
            'duration_seconds': duration_seconds, 'unique_messages_estimate': messages,
            'raw_rows_estimate': raw_rows, 'application_payload_bytes_estimate': payload_estimate,
            'raw_database_row_index_wal_bytes_estimate': database_estimate,
            'total_storage_growth_bytes_estimate': total_storage_estimate,
            'pending_retry_memory_bytes_estimate': combined_memory_estimate,
            'retry_queue_logical_bytes_estimate': retry_queue_estimate,
            'retry_queue_sizing_basis': '2 producers * pending_messages * 16-byte UUID slots; logical estimate, not RSS',
            'unique_offered_rate_per_producer': producer_unique_rate,
            'retry_attempt_rate_per_producer': retry_rate,
            'retry_attempt_burst_per_producer': retry_burst,
            'fixture_shaper_attempt_rate_per_gateway': shaper_rate_per_gateway,
            'fixture_shaper_attempt_burst_per_gateway': shaper_burst_per_gateway,
            'retry_headroom_not_reserved': retry_headroom_not_reserved,
            'application_bandwidth_bytes_per_second_estimate': bandwidth_estimate,
            'payload_bytes_wire': None, 'payload_size_basis': 'actual_builder_or_conservative_assumption',
            'estimate_basis': {'raw_row_bytes': raw_row_bytes, 'index_bytes_per_row': index_bytes_per_row,
                               'wal_multiplier': wal_multiplier}, 'not_measurements': True}


def _phase_intervals(events, arm):
    def belongs(row):
        # Go-local phase events use a different clock and cannot compare children.
        return row.get('child') in (f'paperload-telemetry:{arm}.manifest.json',
                                    f'paperload-telemetry:{arm}.json')
    rows = [e for e in events if isinstance(e, dict) and belongs(e)]
    times = {}
    for row in rows:
        name, stamp = row.get('event'), row.get('observed_at_runner_monotonic_ns')
        if name in times or type(stamp) is not int or stamp < 0:
            raise ValueError('e2_phase_events_invalid')
        times[name] = stamp
    if any(times[k] > times[v] for k, v in (('measure_begin', 'measure_end'),
            ('burst_begin', 'burst_end')) if k in times and v in times):
        raise ValueError('e2_phase_interval_reversed')
    return times


def verify_e2_equal_samples_raw(smoke, registry, pg, spec, *,
                                expected_samples: int,
                                expected_messages: int,
                                samples_per_message: int,
                                timestamp_anchor: str,
                                sample_interval_us: int,
                                identity_samples=None):
    """Full-stream SQL raw audit using owned Gateway/Sensor without row-count limits.

    Validates:
      1. Expected sequence 0..expected_samples-1 with no gaps or overlaps.
      2. Expected timestamps anchor + seq * sample_interval_us.
      3. Exact synthetic triangular values matching synthetic_value(sequence).
      4. Ledger sample_count/sequence boundaries/counts matching expected_messages.
      5. Server-side aggregate checks returning a bounded scalar result.
      6. Header identity sample cap stays <= 20 and does NOT limit the raw audit.
    """
    def _validate_decimal_int_param(val, name: str, *, min_val: int = 1, max_val: int = (1 << 53) - 1) -> str:
        if isinstance(val, bool) or not isinstance(val, int):
            raise ValueError(f"{name}_must_be_integer")
        if val < min_val or val > max_val:
            raise ValueError(f"{name}_out_of_bounds")
        return str(val)

    if not isinstance(timestamp_anchor, str) or not timestamp_anchor or len(timestamp_anchor) > 256:
        raise ValueError('timestamp_anchor_invalid')
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in timestamp_anchor):
        raise ValueError('timestamp_anchor_invalid')

    gw = getattr(spec, 'gateway_id', None)
    sensor = getattr(spec, 'sensor_id', None)
    if not isinstance(gw, str) or not gw or len(gw) > 256 or any(ord(ch) < 32 or ord(ch) == 127 for ch in gw):
        raise ValueError('spec_gateway_id_invalid')
    if not isinstance(sensor, str) or not sensor or len(sensor) > 256 or any(ord(ch) < 32 or ord(ch) == 127 for ch in sensor):
        raise ValueError('spec_sensor_id_invalid')

    params = {
        'gw': gw,
        'sensor': sensor,
        'expected_samples': _validate_decimal_int_param(expected_samples, 'expected_samples', min_val=1, max_val=10_000_000),
        'expected_messages': _validate_decimal_int_param(expected_messages, 'expected_messages', min_val=1, max_val=10_000_000),
        'samples_per_message': _validate_decimal_int_param(samples_per_message, 'samples_per_message', min_val=1, max_val=10_000_000),
        'anchor': timestamp_anchor,
        'interval_us': _validate_decimal_int_param(sample_interval_us, 'sample_interval_us', min_val=1, max_val=(1 << 53) - 1),
    }

    # 1. Server-side ledger checks
    ledger_json = smoke.db_psql(registry, pg, """
SELECT json_build_object(
    'ledger_count', count(*),
    'min_first_sequence', coalesce(min(p.first_sequence), -1),
    'max_last_sequence', coalesce(max(p.last_sequence), -1),
    'total_sample_count', coalesce(sum(p.sample_count), 0),
    'invalid_sample_count', count(*) FILTER (WHERE p.sample_count IS DISTINCT FROM :'samples_per_message'::int),
    'invalid_sequence_bounds', count(*) FILTER (
        WHERE (p.last_sequence - p.first_sequence) IS DISTINCT FROM (p.sample_count - 1)
           OR p.first_sequence IS NULL
           OR p.first_sequence < 0
           OR p.last_sequence IS NULL
           OR p.last_sequence >= :'expected_samples'::bigint
    )
)
FROM processed_messages p
WHERE p.gateway_id = :'gw' AND p.sensor_id = :'sensor';
""", params).strip()
    try:
        ledger_stats = json.loads(ledger_json)
        if not isinstance(ledger_stats, dict):
            raise ValueError("raw_audit_invalid_ledger_stats")
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"raw_audit_invalid_ledger_json: {e}")

    # 2. Ledger overlap check
    overlap_count_str = smoke.db_psql(registry, pg, """
SELECT count(*)
FROM processed_messages p1
JOIN processed_messages p2
  ON p1.gateway_id = p2.gateway_id
 AND p1.sensor_id = p2.sensor_id
 AND p1.message_id <> p2.message_id
 AND p1.first_sequence <= p2.last_sequence
 AND p1.last_sequence >= p2.first_sequence
WHERE p1.gateway_id = :'gw' AND p1.sensor_id = :'sensor';
""", params).strip()
    if not overlap_count_str.isdigit():
        raise ValueError(f"raw_audit_invalid_overlap_count: {overlap_count_str!r}")
    overlap_count = int(overlap_count_str)

    # 3. Provenance correspondence between ledger and telemetry
    provenance_mismatches_str = smoke.db_psql(registry, pg, """
SELECT count(*)
FROM processed_messages p
WHERE p.gateway_id = :'gw' AND p.sensor_id = :'sensor'
  AND (
    p.sample_count IS DISTINCT FROM (SELECT count(*) FROM telemetry t WHERE t.gateway_id = p.gateway_id AND t.sensor_id = p.sensor_id AND t.message_id = p.message_id)
    OR p.first_sequence IS DISTINCT FROM (SELECT min(t.sequence_num) FROM telemetry t WHERE t.gateway_id = p.gateway_id AND t.sensor_id = p.sensor_id AND t.message_id = p.message_id)
    OR p.last_sequence IS DISTINCT FROM (SELECT max(t.sequence_num) FROM telemetry t WHERE t.gateway_id = p.gateway_id AND t.sensor_id = p.sensor_id AND t.message_id = p.message_id)
    OR p.timestamp IS NULL
    OR p.timestamp IS DISTINCT FROM (SELECT min(t.measured_at) FROM telemetry t WHERE t.gateway_id = p.gateway_id AND t.sensor_id = p.sensor_id AND t.message_id = p.message_id)
    OR EXISTS (
        SELECT 1 FROM telemetry t
        WHERE t.gateway_id = p.gateway_id AND t.sensor_id = p.sensor_id AND t.message_id = p.message_id
          AND (t.sample_index IS DISTINCT FROM (t.sequence_num - p.first_sequence) OR t.boot_id IS DISTINCT FROM p.boot_id)
    )
  );
""", params).strip()
    if not provenance_mismatches_str.isdigit():
        raise ValueError(f"raw_audit_invalid_provenance_count: {provenance_mismatches_str!r}")
    provenance_mismatches = int(provenance_mismatches_str)

    # 4. Server-side telemetry raw sample audit: counts, sequence, values, timestamps
    tele_json = smoke.db_psql(registry, pg, """
SELECT json_build_object(
    'raw_count', count(*),
    'unique_sequence_count', count(DISTINCT t.sequence_num),
    'min_sequence', coalesce(min(t.sequence_num), -1),
    'max_sequence', coalesce(max(t.sequence_num), -1),
    'value_mismatches', count(*) FILTER (
        WHERE t.value IS NULL OR abs(t.value - (20.0 + (CASE
            WHEN mod(t.sequence_num, 2000) > 1000 THEN 2000 - mod(t.sequence_num, 2000)
            ELSE mod(t.sequence_num, 2000)
        END)::double precision / 100.0)) > 1e-6
    ),
    'timestamp_mismatches', count(*) FILTER (
        WHERE t.measured_at IS NULL OR t.measured_at IS DISTINCT FROM (:'anchor'::timestamptz + (t.sequence_num * :'interval_us'::bigint * interval '1 microsecond'))
    )
)
FROM telemetry t
WHERE t.gateway_id = :'gw' AND t.sensor_id = :'sensor';
""", params).strip()
    try:
        tele_stats = json.loads(tele_json)
        if not isinstance(tele_stats, dict):
            raise ValueError("raw_audit_invalid_tele_stats")
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"raw_audit_invalid_tele_json: {e}")

    # 5. Header identity sample cap stays 20 for reporting (does not limit raw audit)
    identity_rows_json = smoke.db_psql(registry, pg, """
SELECT coalesce(json_agg(json_build_object(
    'message_id', p.message_id::text,
    'first_sequence', p.first_sequence::text,
    'last_sequence', p.last_sequence::text,
    'sample_count', p.sample_count
) ORDER BY p.first_sequence), '[]'::json)
FROM (
    SELECT message_id, first_sequence, last_sequence, sample_count
    FROM processed_messages
    WHERE gateway_id = :'gw' AND sensor_id = :'sensor'
    ORDER BY first_sequence
    LIMIT 20
) p;
""", params).strip()
    try:
        identity_ranges = json.loads(identity_rows_json)
        if not isinstance(identity_ranges, list):
            raise ValueError("raw_audit_invalid_identity_ranges")
    except (json.JSONDecodeError, TypeError) as e:
        raise ValueError(f"raw_audit_invalid_identity_json: {e}")

    ledger_count = ledger_stats.get('ledger_count', ledger_stats.get('ledger', 0))
    raw_count = tele_stats.get('raw_count', tele_stats.get('raw', 0))
    value_mismatches = tele_stats.get('value_mismatches', tele_stats.get('raw_identity_value_mismatches', 0))
    timestamp_mismatches = tele_stats.get('timestamp_mismatches', 0)
    min_seq = tele_stats.get('min_sequence', 0)
    max_seq = tele_stats.get('max_sequence', expected_samples - 1)
    unique_seq = tele_stats.get('unique_sequence_count', raw_count)

    sequence_gaps_or_overlaps = (
        1 if (
            raw_count != expected_samples
            or unique_seq != expected_samples
            or min_seq != 0
            or max_seq != expected_samples - 1
            or overlap_count > 0
            or ledger_stats.get('invalid_sequence_bounds', 0) > 0
        ) else 0
    )

    verified_exact = bool(
        ledger_count == expected_messages
        and raw_count == expected_samples
        and unique_seq == expected_samples
        and min_seq == 0
        and max_seq == expected_samples - 1
        and value_mismatches == 0
        and timestamp_mismatches == 0
        and provenance_mismatches == 0
        and overlap_count == 0
        and ledger_stats.get('invalid_sample_count', 0) == 0
        and ledger_stats.get('invalid_sequence_bounds', 0) == 0
        and ledger_stats.get('total_sample_count', raw_count) == expected_samples
    )

    return {
        'gateway_id': spec.gateway_id,
        'sensor_id': spec.sensor_id,
        'ledger_count': ledger_count,
        'raw_count': raw_count,
        'raw_identity_value_mismatches': value_mismatches,
        'timestamp_mismatches': timestamp_mismatches,
        'sequence_gaps_or_overlaps': sequence_gaps_or_overlaps,
        'provenance_mismatches': provenance_mismatches,
        'identity_ranges': identity_ranges[:20],
        'verified_exact': verified_exact,
    }


def validate_e2_equal_samples_result(*, workload_a, workload_b, ingress_delta, database_delta,
                                     database_by_gateway, phase_events, raw_verification=None,
                                     execution_context, resources=None, histogram_windows=None):
    """Reconcile equal_samples child results, phase overlap and scoped DB.

    Distinguishes functionally correct data (valid=True) from clean_comparison eligibility.
    Schedule misses, retries, and rate limits remain visible as contamination reasons.
    """
    failures = []
    contamination_reasons = []

    if not isinstance(workload_a, dict) or not isinstance(workload_b, dict):
        return {
            'valid': False,
            'failures': ['workload_missing'],
            'clean_comparison': False,
            'contamination_reasons': ['workload_missing'],
            'availability': 'inconclusive',
            'full_p8_accepted': False,
            'generator_colocated': 'unverified',
            'physical_ab': 'NOT RUN',
        }

    # Status check
    if workload_a.get('status') != 'COMPLETE' or workload_b.get('status') != 'COMPLETE':
        failures.append('workload_status')

    # Primary and cleanup errors check
    for arm, w in (('A', workload_a), ('B', workload_b)):
        if w.get('primary_error'):
            failures.append(f'{arm}_primary_error')
        if w.get('cleanup_errors'):
            failures.append(f'{arm}_cleanup_errors')

    if not isinstance(execution_context, dict):
        return {
            'valid': False,
            'failures': ['execution_context_missing'],
            'clean_comparison': False,
            'contamination_reasons': ['execution_context_missing'],
            'availability': 'inconclusive',
            'full_p8_accepted': False,
            'generator_colocated': 'unverified',
            'physical_ab': 'NOT RUN',
        }

    samples = execution_context.get('samples_per_message')
    if samples not in PILOT_SAMPLES or type(samples) is not int:
        failures.append('samples_per_message_missing_or_invalid')

    ctx = execution_context.get('context')
    if not isinstance(ctx, dict):
        failures.append('equal_samples_context_invalid')
        validated_ctx = None
    else:
        try:
            validated_ctx = validate_e2_equal_samples_context(ctx, check_freshness=False)
        except ValueError:
            failures.append('equal_samples_context_invalid')
            validated_ctx = None

    expected_counts = execution_context.get('expected_counts')
    if not isinstance(expected_counts, dict) and validated_ctx and samples in PILOT_SAMPLES:
        try:
            expected_counts = calculate_e2_equal_samples_expected_counts(validated_ctx, samples)
        except ValueError:
            expected_counts = None

    if not isinstance(expected_counts, dict) or 'per_role' not in expected_counts:
        failures.append('equal_samples_expected_counts_invalid')
        exp_m_a = exp_m_b = exp_s_a = exp_s_b = None
        total_m = total_s = None
    else:
        exp_m_a = expected_counts['per_role']['A']['messages']
        exp_s_a = expected_counts['per_role']['A']['samples']
        exp_m_b = expected_counts['per_role']['B']['messages']
        exp_s_b = expected_counts['per_role']['B']['samples']
        total_m = expected_counts['total_messages']
        total_s = expected_counts['total_samples']

    ca, cb = workload_a.get('telemetry', {}), workload_b.get('telemetry', {})
    eq_a, eq_b = workload_a.get('equal_samples', {}), workload_b.get('equal_samples', {})

    for arm, counters, eq_summary, exp_m, exp_s in (
        ('A', ca, eq_a, exp_m_a, exp_s_a),
        ('B', cb, eq_b, exp_m_b, exp_s_b),
    ):
        if not isinstance(counters, dict):
            failures.append(f'{arm}_telemetry_missing')
            continue

        for field, exp_val in (
            ('generated_unique', exp_m),
            ('offered_unique', exp_m),
            ('application_acks', exp_m),
            ('pending_messages', 0),
            ('pending_bytes', 0),
        ):
            if exp_val is not None and counters.get(field) != exp_val:
                failures.append(f'{arm}_{field}')

        attempts = counters.get('publish_attempts')
        retries = counters.get('retries')
        invalid_acks = counters.get('invalid_acks')
        if (type(attempts) is not int or type(retries) is not int or retries < 0
                or retries > attempts or type(invalid_acks) is not int or invalid_acks != 0):
            failures.append(f'{arm}_retry_or_invalid_ack_metrics')
        elif exp_m is not None and attempts < exp_m:
            failures.append(f'{arm}_publish_attempts')

        broker_pubacks = counters.get('broker_pubacks')
        if type(broker_pubacks) is not int or (exp_m is not None and broker_pubacks < exp_m):
            failures.append(f'{arm}_broker_pubacks')

        # Contamination by retry
        if type(retries) is int and retries > 0:
            contamination_reasons.append(f'{arm}_retries_observed:{retries}')

        # Cross-check equal_samples summary against telemetry actual counters
        if isinstance(eq_summary, dict):
            if (eq_summary.get('generated_messages') != counters.get('generated_unique')
                    or eq_summary.get('offered_unique_messages') != counters.get('offered_unique')
                    or eq_summary.get('application_acks') != counters.get('application_acks')
                    or (exp_s is not None and eq_summary.get('generated_samples') != exp_s)
                    or (exp_s is not None and eq_summary.get('offered_unique_samples') != exp_s)
                    or (samples in PILOT_SAMPLES and eq_summary.get('samples_per_message') != samples)
                    or (exp_s is not None and eq_summary.get('expected_samples') != exp_s)
                    or (exp_m is not None and eq_summary.get('expected_messages') != exp_m)
                    or (validated_ctx and eq_summary.get('context_sha256') != validated_ctx['context_sha256'])
                    or eq_summary.get('logical_producer') != arm):
                failures.append(f'{arm}_equal_samples_telemetry_cross_check_mismatch')

            # Contamination by schedule misses
            misses = eq_summary.get('schedule_misses')
            if type(misses) is int and misses > 0:
                contamination_reasons.append(f'{arm}_schedule_misses_observed:{misses}')
        else:
            failures.append(f'{arm}_equal_samples_missing')

    # Phase events check (both steady producers, measure_begin and measure_end)
    if not isinstance(phase_events, list):
        phase = {'available': None, 'reason': 'phase_events_missing'}
        failures.append('phase_events_missing')
    else:
        try:
            a_int = _phase_intervals(phase_events, 'A')
            b_int = _phase_intervals(phase_events, 'B')
            required = ('measure_begin', 'measure_end')
            if any(k not in a_int for k in required) or any(k not in b_int for k in required):
                raise ValueError('phase_event_missing')
            overlap = max(0, min(a_int['measure_end'], b_int['measure_end']) - max(a_int['measure_begin'], b_int['measure_begin']))
            phase = {
                'available': True,
                'a_measure_runner_interval_ns': [a_int['measure_begin'], a_int['measure_end']],
                'b_measure_runner_interval_ns': [b_int['measure_begin'], b_int['measure_end']],
                'observed_overlap_ns': overlap,
                'observer_lag_qualification': 'runner receipt time; pipe lag unknown',
            }
            if overlap <= 0:
                failures.append('a_measure_does_not_overlap_b_measure')
        except ValueError as exc:
            phase = {'available': None, 'reason': str(exc)}
            failures.append('phase_overlap_inconclusive')

    # Global DB delta check
    expected_total = {
        'ledger': total_m,
        'raw': total_s,
        'temporal': total_m,
        'configuration': 0,
    }
    if not isinstance(database_delta, dict):
        failures.append('database_delta_mismatch')
    else:
        norm_delta = {
            'ledger': database_delta.get('ledger', database_delta.get('processed')),
            'raw': database_delta.get('raw'),
            'temporal': database_delta.get('temporal', database_delta.get('telemetry')),
            'configuration': database_delta.get('configuration', 0),
        }
        if any(norm_delta.get(k) != expected_total.get(k) for k in expected_total):
            failures.append('database_delta_mismatch')

    # Scoped target mapping and database_by_gateway check
    mapping = execution_context.get('role_to_target')
    if (not isinstance(mapping, dict) or set(mapping) != {'A', 'B'} or
            any(not isinstance(mapping.get(arm), dict) or not mapping[arm].get('gateway_id') or
                not mapping[arm].get('sensor_id') for arm in ('A', 'B')) or
            mapping.get('A') == mapping.get('B')):
        failures.append('execution_target_mapping_invalid')
        expected_gw_ids = set()
    else:
        expected_gw_ids = {mapping['A']['gateway_id'], mapping['B']['gateway_id']}

    if not isinstance(database_by_gateway, dict) or len(database_by_gateway) != 2:
        failures.append('database_gateway_scope_missing')
    elif expected_gw_ids and set(database_by_gateway) != expected_gw_ids:
        failures.append('database_gateway_scope_mismatch')
    elif expected_gw_ids and expected_counts:
        for arm in ('A', 'B'):
            gw = mapping[arm]['gateway_id']
            item = database_by_gateway.get(gw, {})
            norm_item = {
                'ledger': item.get('ledger', item.get('processed')),
                'raw': item.get('raw'),
                'temporal': item.get('temporal', item.get('telemetry')),
                'configuration': item.get('configuration', 0),
            }
            exp_scope = {
                'ledger': expected_counts['per_role'][arm]['messages'],
                'raw': expected_counts['per_role'][arm]['samples'],
                'temporal': expected_counts['per_role'][arm]['messages'],
                'configuration': 0,
            }
            if any(norm_item.get(k) != exp_scope.get(k) for k in exp_scope):
                failures.append('database_gateway_delta_mismatch')
                break

    # Raw verification checks
    if (not isinstance(raw_verification, list) or len(raw_verification) != 2 or
            any(not isinstance(row, dict) or not row.get('verified_exact')
                for row in raw_verification)):
        failures.append('equal_samples_raw_verification_failed')
    elif expected_counts and expected_gw_ids:
        raw_by_gw = {r.get('gateway_id'): r for r in raw_verification if isinstance(r, dict)}
        if set(raw_by_gw) != expected_gw_ids:
            failures.append('equal_samples_raw_verification_failed')
        else:
            for arm in ('A', 'B'):
                gw = mapping[arm]['gateway_id']
                sensor = mapping[arm]['sensor_id']
                r_entry = raw_by_gw.get(gw, {})
                exp_role = expected_counts['per_role'][arm]
                if (r_entry.get('sensor_id') != sensor
                        or r_entry.get('ledger_count') != exp_role['messages']
                        or r_entry.get('raw_count') != exp_role['samples']
                        or r_entry.get('raw_identity_value_mismatches', 0) != 0
                        or r_entry.get('timestamp_mismatches', 0) != 0
                        or r_entry.get('sequence_gaps_or_overlaps', 0) != 0
                        or r_entry.get('provenance_mismatches', 0) != 0):
                    failures.append('equal_samples_raw_verification_failed')
                    break

    # Ingress delta check
    ingress = ingress_delta.get('delta') if isinstance(ingress_delta, dict) and ingress_delta.get('available') else None
    if ingress is None:
        received = admitted = rejected = drops = {'available': None, 'reason': 'ingress_delta_unavailable'}
        failures.append('ingress_delta_unavailable')
    else:
        if total_m is not None:
            if ingress.get('Committed') != total_m:
                failures.append('ingress_Committed')
            if ingress.get('ACKPublished', 0) < total_m:
                failures.append('ingress_ACKPublished')
            if ingress.get('Received', 0) < total_m:
                failures.append('ingress_Received')
            if ingress.get('Accepted', 0) < total_m:
                failures.append('ingress_Accepted')
        if total_s is not None and ingress.get('SamplesNew') != total_s:
            failures.append('ingress_SamplesNew')

        for field in ('RejectedNotRunning', 'RejectedStopping', 'RejectedRetained', 'RejectedOversized',
                      'RejectedInvalidTopic', 'QueueFull', 'ByteBudgetFull', 'ShaperRateLimited'):
            val = ingress.get(field)
            if type(val) is not int or val < 0:
                failures.append('ingress_metric_' + field)

        # Contamination from rate limiting, queue drops, or rejection in equal_samples
        if ingress.get('ShaperRateLimited', 0) > 0:
            contamination_reasons.append(f"ingress_shaper_rate_limited:{ingress['ShaperRateLimited']}")
        if ingress.get('QueueFull', 0) > 0:
            contamination_reasons.append(f"ingress_queue_full:{ingress['QueueFull']}")
        if ingress.get('ByteBudgetFull', 0) > 0:
            contamination_reasons.append(f"ingress_byte_budget_full:{ingress['ByteBudgetFull']}")
        for rej in ('RejectedNotRunning', 'RejectedStopping', 'RejectedRetained', 'RejectedOversized', 'RejectedInvalidTopic'):
            if ingress.get(rej, 0) > 0:
                contamination_reasons.append(f"ingress_{rej}:{ingress[rej]}")

        received = {'available': ingress.get('Received')}
        admitted = {'available': ingress.get('Accepted')}
        rejected = {'available': {k: ingress.get(k) for k in ('RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                                                               'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                                                               'ByteBudgetFull', 'ShaperRateLimited')}}
        drops = {'available': ingress.get('ShaperRateLimited')}

    # Collector errors contamination check
    if resources and isinstance(resources.get('collector_errors'), list) and resources['collector_errors']:
        contamination_reasons.append(f"collector_errors:{','.join(resources['collector_errors'])}")

    clean_comparison = (not failures) and (not contamination_reasons)

    return {
        'valid': not failures,
        'failures': failures,
        'clean_comparison': clean_comparison,
        'contamination_reasons': contamination_reasons,
        'availability': 'available' if phase.get('available') and ingress is not None else 'inconclusive',
        'phase_overlap': phase,
        'received': received,
        'admitted': admitted,
        'rejected': rejected,
        'actual_shaper_drops': drops,
        'database_expected': expected_total,
        'expected_counts': expected_counts,
        'queue_histogram_windows': histogram_windows,
        'resource_roles': resources,
        'fairness': {
            'status': 'not_proven',
            'reason': 'equal_samples uses identical offered rate per producer; runner phase events provide timestamps, not cross-child counter snapshots',
        },
        'durability': 'synthetic-load; not durable gateway outbox',
        'full_p8_accepted': False,
        'generator_colocated': 'unverified',
        'physical_ab': 'NOT RUN',
    }


def validate_e2_result(*, workload_a, workload_b, ingress_delta, database_delta,
                       database_by_gateway, phase_events, raw_verification=None,
                       execution_context, resources=None, histogram_windows=None):
    """Reconcile actual child results, phase overlap and scoped DB; unknown stays unavailable."""
    if isinstance(execution_context, dict) and (
            execution_context.get('workload') == 'equal_samples'
            or execution_context.get('workload_type') == 'equal_samples'
            or 'context' in execution_context
            or (isinstance(workload_a, dict) and 'equal_samples' in workload_a)):
        return validate_e2_equal_samples_result(
            workload_a=workload_a, workload_b=workload_b,
            ingress_delta=ingress_delta, database_delta=database_delta,
            database_by_gateway=database_by_gateway, phase_events=phase_events,
            raw_verification=raw_verification, execution_context=execution_context,
            resources=resources, histogram_windows=histogram_windows)
    failures = []
    if not isinstance(workload_a, dict) or not isinstance(workload_b, dict):
        return {'valid': False, 'failures': ['workload_missing'], 'availability': 'inconclusive'}
    ca, cb = workload_a.get('telemetry', {}), workload_b.get('telemetry', {})
    if workload_a.get('status') != 'COMPLETE' or workload_b.get('status') != 'COMPLETE':
        failures.append('workload_status')
    samples = execution_context.get('samples_per_message') if isinstance(execution_context, dict) else None
    if samples not in PILOT_SAMPLES:
        failures.append('samples_per_message_missing_or_invalid')
    manifest_pair = execution_context.get('role_manifests') if isinstance(execution_context, dict) else None
    if isinstance(manifest_pair, dict) and set(manifest_pair) == {'A', 'B'}:
        expected_by_role = {arm: (m['traffic']['rate_per_second'] * m['phases']['measure_seconds']
            + m['traffic'].get('burst_messages', 0)) for arm, m in manifest_pair.items()}
        official = execution_context.get('profile') == 'official'
    else:
        expected_by_role = {'A': 8, 'B': 16}
        official = False
    expected_total_messages = sum(expected_by_role.values())
    for arm, counters in (('A', ca), ('B', cb)):
        expected = expected_by_role[arm]
        for field, value in (('generated_unique', expected), ('offered_unique', expected),
                             ('application_acks', expected), ('pending_messages', 0), ('pending_bytes', 0)):
            if counters.get(field) != value: failures.append(f'{arm}_{field}')
        attempts, retries, invalid_acks = (counters.get('publish_attempts'),
                counters.get('retries'), counters.get('invalid_acks'))
        if (type(attempts) is not int or type(retries) is not int or retries < 0
                or retries > attempts or type(invalid_acks) is not int or invalid_acks != 0):
            failures.append(f'{arm}_retry_or_invalid_ack_metrics')
    for arm, counters in (('A', ca), ('B', cb)):
        minimum = expected_by_role[arm]
        for field in ('publish_attempts', 'broker_pubacks'):
            value = counters.get(field)
            if type(value) is not int or value < minimum: failures.append(f'{arm}_{field}')
    no_b_burst = (official and isinstance(manifest_pair, dict)
                  and manifest_pair.get('B', {}).get('traffic', {}).get('burst_messages', 0) == 0)
    if no_b_burst:
        phase = {'available': None, 'reason': 'b_burst_not_configured'}
    elif not isinstance(phase_events, list):
        phase = {'available': None, 'reason': 'phase_events_missing'}
    else:
        try:
            a, b = _phase_intervals(phase_events, 'A'), _phase_intervals(phase_events, 'B')
            required = ('measure_begin', 'measure_end')
            if any(k not in a for k in required) or any(k not in b for k in ('burst_begin', 'burst_end')):
                raise ValueError('phase_event_missing')
            overlap = max(0, min(a['measure_end'], b['burst_end']) - max(a['measure_begin'], b['burst_begin']))
            phase = {'available': True, 'a_measure_runner_interval_ns': [a['measure_begin'], a['measure_end']],
                     'b_burst_runner_interval_ns': [b['burst_begin'], b['burst_end']],
                     'observed_overlap_ns': overlap, 'observer_lag_qualification': 'runner receipt time; pipe lag unknown'}
            if overlap <= 0: failures.append('a_measure_does_not_overlap_b_burst')
        except ValueError as exc:
            phase = {'available': None, 'reason': str(exc)}
            failures.append('phase_overlap_inconclusive')
    expected_total = {'ledger': expected_total_messages,
                      'raw': expected_total_messages * samples if samples in PILOT_SAMPLES else None,
                      'temporal': expected_total_messages, 'configuration': 0}
    if database_delta != expected_total: failures.append('database_delta_mismatch')
    mapping = execution_context.get('role_to_target') if isinstance(execution_context, dict) else None
    mapped_a_gateway = mapping.get('A', {}).get('gateway_id') if isinstance(mapping, dict) and isinstance(mapping.get('A'), dict) else None
    if (not isinstance(mapping, dict) or set(mapping) != {'A', 'B'} or
            any(not isinstance(mapping.get(arm), dict) or not mapping[arm].get('gateway_id') or
                not mapping[arm].get('sensor_id') for arm in ('A', 'B')) or
            (isinstance(mapping, dict) and mapping.get('A') == mapping.get('B'))):
        failures.append('execution_target_mapping_invalid')
    if not isinstance(database_by_gateway, dict) or len(database_by_gateway) != 2:
        failures.append('database_gateway_scope_missing')
    elif (isinstance(mapping, dict) and set(mapping) == {'A', 'B'} and
            all(isinstance(mapping.get(arm), dict) and mapping[arm].get('gateway_id') for arm in ('A', 'B')) and
            set(database_by_gateway) == {mapping['A']['gateway_id'], mapping['B']['gateway_id']}):
        expected_scopes = {mapping[arm]['gateway_id']: expected_by_role[arm] for arm in ('A', 'B')}
        if samples not in PILOT_SAMPLES or any(database_by_gateway[g] != {'ledger': n, 'raw': n * samples,
                'temporal': n, 'configuration': 0} for g, n in expected_scopes.items()):
            failures.append('database_gateway_delta_mismatch')
    else:
        failures.append('database_gateway_scope_mismatch')
    if official:
        raw_ok = (isinstance(raw_verification, list) and len(raw_verification) == 2 and
            all(isinstance(row, dict) and row.get('count_verified') is True and
                row.get('ledger_count') == expected_by_role[arm] and
                row.get('raw_count') == expected_by_role[arm] * samples and
                row.get('gateway_id') == mapping[arm].get('gateway_id') and
                row.get('sensor_id') == mapping[arm].get('sensor_id') for arm, row in zip(('A','B'), raw_verification)))
    else:
        raw_ok = isinstance(raw_verification, list) and len(raw_verification) == 2
        if raw_ok:
            for row in raw_verification:
                arm = next((a for a in ('A','B') if mapping.get(a, {}).get('gateway_id') == row.get('gateway_id')),
                           None) if isinstance(row, dict) and isinstance(mapping, dict) else None
                if (arm is None or row.get('verified_exact') is not True
                        or row.get('ledger_count') != expected_by_role[arm]
                        or row.get('sensor_id') != mapping[arm].get('sensor_id')):
                    raw_ok = False
                    break
        if raw_ok:
            raw_ok = (len({r.get('gateway_id') for r in raw_verification}) == 2
                      and sorted(r.get('ledger_count') for r in raw_verification)
                          == sorted(expected_by_role.values()))
    if not raw_ok:
        failures.append('official_raw_counts_mismatch' if official else 'pilot_raw_retention_verification')
    ingress = ingress_delta.get('delta') if isinstance(ingress_delta, dict) and ingress_delta.get('available') else None
    if ingress is None:
        received = admitted = rejected = drops = {'available': None, 'reason': 'ingress_delta_unavailable'}
    else:
        for field, minimum in (('Committed', expected_total_messages), ('SamplesNew', expected_total['raw'])):
            if minimum is None or type(ingress.get(field)) is not int or ingress[field] != minimum:
                failures.append('ingress_' + field)
        for field, minimum in (('ACKPublished', expected_total_messages), ('Received', expected_total_messages), ('Accepted', expected_total_messages)):
            if type(ingress.get(field)) is not int or ingress[field] < minimum:
                failures.append('ingress_' + field)
        if type(ingress.get('DuplicateCommitted')) is not int or ingress.get('DuplicateCommitted', -1) < 0:
            failures.append('ingress_metric_DuplicateCommitted')
        for field in ('RejectedNotRunning','RejectedStopping','RejectedRetained','RejectedOversized',
                      'RejectedInvalidTopic','QueueFull','ByteBudgetFull','ShaperRateLimited'):
            if type(ingress.get(field)) is not int or ingress[field] < 0:
                failures.append('ingress_metric_' + field)
        if not official and (type(ingress.get('ShaperRateLimited')) is not int or ingress.get('ShaperRateLimited', 0) <= 0):
            failures.append('ingress_burst_not_rate_limited')
        received = {'available': ingress.get('Received')}
        admitted = {'available': ingress.get('Accepted')}
        rejected = {'available': {k: ingress.get(k) for k in ('RejectedNotRunning','RejectedStopping','RejectedRetained','RejectedOversized','RejectedInvalidTopic','QueueFull','ByteBudgetFull','ShaperRateLimited')}}
        drops = {'available': ingress.get('ShaperRateLimited')}
    if ingress is None: failures.append('ingress_delta_unavailable')
    def counter_sum(field):
        left, right = ca.get(field), cb.get(field)
        return left + right if type(left) is int and type(right) is int else None
    return {'valid': not failures, 'failures': failures,
            'availability': 'available' if phase.get('available') and ingress is not None else 'inconclusive',
            'phase_overlap': phase, 'received': received, 'admitted': admitted,
            'rejected': rejected, 'actual_shaper_drops': drops,
             'database_expected': expected_total, 'queue_histogram_windows': histogram_windows,
             'actual_accounting': {'generated_unique': counter_sum('generated_unique'),
                 'offered_unique': counter_sum('offered_unique'),
                 'publish_attempts': counter_sum('publish_attempts'),
                 'broker_pubacks': counter_sum('broker_pubacks'),
                 'retries': counter_sum('retries'),
                 'application_acks': counter_sum('application_acks'),
                 'committed_unique': ingress.get('Committed') if isinstance(ingress, dict) else None,
                 'duplicate_committed': ingress.get('DuplicateCommitted') if isinstance(ingress, dict) else None,
                 'pending_messages': counter_sum('pending_messages'),
                 'pending_bytes': counter_sum('pending_bytes')},
            'resource_roles': resources, 'fairness': {'status': 'not_proven',
                'reason': 'runner phase events provide timestamps, not cross-child counter snapshots'},
             'durability': 'synthetic-load; not durable gateway outbox'}


def _safe_flags(effective):
    allowed = ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED',
        'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
        'MQTT_SHAPER_RATE_PER_SECOND', 'MQTT_SHAPER_BURST', 'MQTT_MICROBATCH_MAX_MESSAGES',
        'MQTT_MICROBATCH_MAX_ROWS', 'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT')
    env = effective.get('services', {}).get('backend', {}).get('environment', {})
    return {key: env[key] for key in allowed if key in env}


def _validate_fixture_flags(effective, fixture=None):
    flags = _safe_flags(effective)
    shaper = e2_shaper_caps(
        profile=getattr(fixture, 'paper_p8_profile', 'pilot'),
        offered_rate=getattr(fixture, 'paper_p8_e2_offered_rate', 2),
        burst_messages=getattr(fixture, 'paper_p8_e2_burst_messages', 0),
        retry_rate=getattr(fixture, 'paper_p8_e2_retry_rate_per_second', 0),
        retry_burst=getattr(fixture, 'paper_p8_e2_retry_burst', 0))
    rate = shaper['rate_per_second']
    burst = shaper['burst']
    max_rows = min(500, max(1, rate * getattr(fixture, 'paper_p8_e2_samples_per_message', 1))) if getattr(fixture, 'paper_p8_profile', 'pilot') == 'official' else 500
    expected = {'WEBSOCKET_ENABLED': 'false', 'STAGE5_ENABLED': 'false',
        'STAGE5_WAKEUP_ENABLED': 'false', 'MQTT_SHAPER_ENABLED': 'true',
        'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_RAM_DEDUP_ENABLED': 'false',
        'MQTT_SHAPER_RATE_PER_SECOND': str(rate),
        'MQTT_SHAPER_BURST': str(burst),
        'MQTT_MICROBATCH_MAX_MESSAGES': '32',
        'MQTT_MICROBATCH_MAX_ROWS': str(max_rows),
        'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT': '50ms'}
    normalized = {key: str(value).lower() for key, value in flags.items()}
    if any(normalized.get(key) != value for key, value in expected.items()):
        raise ValueError('p8_e2_effective_fixture_flags_mismatch')
    return flags


def _target_counts(smoke, registry, pg, spec, *, timeout=None):
    sql = """SELECT json_build_object(
 'ledger',(SELECT count(*) FROM processed_messages WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'raw',(SELECT count(*) FROM telemetry WHERE gateway_id=:'gw' AND sensor_id=:'sensor'),
 'temporal',(SELECT count(*) FROM twin_temporal_values v JOIN twin_entities e ON e.id=v.entity_id WHERE e.entity_id=:'urn'),
 'configuration',(SELECT count(*) FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=:'urn' AND s.reported_state ? 'configuration'));"""
    kwargs = {'timeout': timeout} if timeout is not None else {}
    value = smoke.db_psql(registry, pg, sql,
        {'gw': spec.gateway_id, 'sensor': spec.sensor_id, 'urn': spec.sensor_urn}, **kwargs).strip()
    return json.loads(value)


def _stop(proc, timeout=5):
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def run(smoke, fixture, endpoint, registry, containers, images, state, paperload_binary):
    """Run A8/B16 once for each sample profile and persist a safe pending-cleanup result."""
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
    _support.validate_mqtt_readback({'service': 'mosquitto', 'host_ip': '127.0.0.1',
        'target': 8883, 'published': published}, owned_service='mosquitto')
    remote_config = getattr(fixture, 'remote_network_config', None)
    mqtt_host = remote_config['advertised_host'] if remote_config else '127.0.0.1'
    mqtt_endpoint = f'mqtts://{mqtt_host}:{published}'
    root = fixture.private / 'p8-e2'
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    ca_path = fixture.private / 'certs/ca.crt'
    targets = [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id} for s in specs]
    gateway_files = {}
    telemetry_files = {}
    for spec, password, arm in zip(specs, passwords, ('A', 'B')):
        identity = root / f'gateway-{spec.gateway_id}.json'
        smoke.private_write(identity, json.dumps({'username': spec.gateway_id, 'password': password}, sort_keys=True))
        gateway_files[spec.gateway_id] = str(identity)
        telemetry = root / f'telemetry-{arm}-identities.json'
        smoke.private_write(telemetry, '{}')
        telemetry_files[arm] = str(telemetry)
    common = {'mode': 'local', 'run_id': fixture.project + '-e2', 'seed': PILOT_SEED,
        'bundle_path': str(root), 'publish_timeout_ms': 3000,
        'tls': {'https_endpoint': endpoint.url, 'https_ca_file': str(endpoint.ca_file),
            'mqtt_tls_endpoint': mqtt_endpoint, 'mqtt_tls_ca_file': str(ca_path),
            'mqtt_tls_server_name': 'localhost'}}
    manifests = e2_pilot_manifests(common=common, targets=targets,
        gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files)
    if manifests['profiles']['100']['A']['run_id'] == manifests['profiles']['1']['A']['run_id']:
        raise ValueError('e2_profile_run_ids_not_distinct')
    profiles = []
    for sample_key in ('100', '1'):
        pair = manifests['profiles'][sample_key]
        paths = {}
        for arm in ('A', 'B'):
            path = root / f'{arm}-{sample_key}.manifest.json'
            smoke.private_write(path, json.dumps(pair[arm], sort_keys=True, separators=(',', ':')))
            paths[arm] = path
        plan_dir = root / f'plan-{sample_key}'
        plan = build_e2_execution_plan(output_dir=plan_dir, manifest_a=paths['A'], manifest_b=paths['B'])
        before_global = smoke.paper_p1_count_db(registry, pg)
        before_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
        baseline = _support._admin_metrics(smoke, endpoint, tokens['admin'])
        collector = BoundedCollector(scrape=lambda: _support._admin_metrics(smoke, endpoint, tokens['admin']),
            interval=0.5, max_snapshots=64, deadline_seconds=60, baseline_metrics=baseline,
            baseline_server=smoke.paper_p0_stats(registry, containers, 'workload_start'))
        log = plan_dir / 'runner.log'
        proc = None
        failure = None
        with log.open('xb') as stream:
            log.chmod(0o600)
            try:
                runner_command = ['python3', str(smoke.ROOT / 'scripts/paper_p8.py'), '--run',
                    '--binary', str(paperload_binary), '--run-spec', plan['run_spec'],
                    '--run-artifact', str(plan_dir / 'run.json')]
                if getattr(fixture, 'paper_p8_profile', 'pilot') == 'official':
                    runner_command.append('--official-opt-in')
                proc = subprocess.Popen(runner_command, cwd=smoke.ROOT,
                    stdout=stream, stderr=subprocess.STDOUT, env=smoke.process_env(fixture), close_fds=True)
                collector.start(proc.pid)
                try:
                    proc.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    raise ValueError('p8_e2_runner_timeout') from None
            except Exception as exc:
                failure = exc
            finally:
                try:
                    collector.stop(proc.pid if proc else None)
                    _stop(proc)
                except Exception as cleanup_exc:
                    if failure is None:
                        failure = cleanup_exc
        if failure:
            smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json',
                {'status': 'FAIL', 'failure': 'runner_exception', 'profiles': profiles,
                 'full_p8_accepted': False})
            raise failure
        if proc is None or proc.returncode != 0:
            smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json',
                {'status': 'FAIL', 'failure': 'runner_nonzero_or_missing', 'profiles': profiles,
                 'full_p8_accepted': False})
            raise ValueError('p8_e2_runner_failed')
        after_admin = _support._admin_metrics(smoke, endpoint, tokens['admin'])
        terminal_server = smoke.paper_p0_stats(registry, containers, 'workload_terminal')
        collector.metrics.append({'phase': 'workload_terminal', 'snapshot': after_admin})
        collector.resources.append(terminal_server)
        after_targets = [_target_counts(smoke, registry, pg, spec) for spec in specs]
        after_global = smoke.paper_p1_count_db(registry, pg)
        scoped = sensor_scoped_delta(
            [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id, 'counts': c} for s, c in zip(specs, before_targets)],
            [{'gateway_id': s.gateway_id, 'sensor_id': s.sensor_id, 'counts': c} for s, c in zip(specs, after_targets)])
        # Validate retention only against this profile's producer IDs, excluding prior profile rows.
        workloads = {}
        raw_checks = []
        mapping = {}
        for index, arm in enumerate(('A', 'B')):
            output = plan_dir / f'{arm}-output' / 'workload.json'
            try:
                workloads[arm] = json.loads(output.read_text(encoding='utf-8'))
            except (OSError, UnicodeError, json.JSONDecodeError):
                workloads[arm] = {}
            producer_ids = [row.get('message_id') for row in workloads[arm].get('identity_samples', [])
                            if isinstance(row, dict) and isinstance(row.get('message_id'), str)]
            spec = specs[index]
            mapping[arm] = {'gateway_id': spec.gateway_id, 'sensor_id': spec.sensor_id}
            raw_checks.append(verify_pilot_raw(smoke, registry, pg, spec, message_ids=producer_ids))
        by_gateway = {}
        for arm, index in (('A', 0), ('B', 1)):
            item = (scoped.get('available') or {}).get(f'{specs[index].gateway_id}/{specs[index].sensor_id}', {})
            item = dict(item)
            item['configuration'] = after_targets[index]['configuration'] - before_targets[index]['configuration']
            by_gateway[mapping[arm]['gateway_id']] = item
        delta = calculate_canonical_database_delta(before_global, after_global)
        events = json.loads((plan_dir / 'run.json').read_text(encoding='utf-8')).get('phase_observations', {}).get('events', [])
        ingress = _support.calculate_ingress_delta(baseline, after_admin)
        # Keep the existing P1 same-process histogram-window calculation; the
        # admin endpoint nests ingress histograms under ingress.Durations.
        duration_names = ('queue_wait', 'validation', 'batch_wait',
                          'group_persistence', 'persistence', 'ack_publish')
        def ingress_duration_snapshot(snapshot):
            ing = snapshot.get('ingress') if isinstance(snapshot, dict) else None
            durations = ing.get('Durations') if isinstance(ing, dict) else None
            availability = (ing.get('availability') if isinstance(ing, dict) else None)
            return {'process_instance': snapshot.get('process_instance') if isinstance(snapshot, dict) else None,
                    'availability': {'ingress': availability},
                    'ingress': durations if isinstance(durations, dict) else {}}
        histogram_before = ingress_duration_snapshot(baseline)
        histogram_after = ingress_duration_snapshot(after_admin)
        histogram_windows = {}
        for duration_name in duration_names:
            window = smoke.paper_p1_histogram_window(
                histogram_before, histogram_after, 'ingress', duration_name)
            if window.get('availability') != 'available':
                window['reason'] = 'same-instance ingress duration histogram unavailable or invalid'
            histogram_windows[duration_name] = window
        base_ingress, end_ingress = baseline.get('ingress'), after_admin.get('ingress')
        drain = {}
        for field in ('InFlight', 'BufferedPayloadBytes'):
            old = base_ingress.get(field) if isinstance(base_ingress, dict) else None
            new = end_ingress.get(field) if isinstance(end_ingress, dict) else None
            drain[field] = {'baseline': old if type(old) is int else None,
                            'terminal': new if type(new) is int else None}
            if type(old) is not int or type(new) is not int or old != new:
                raise ValueError('p8_e2_ingress_drain_unverified:' + field)
        if collector.errors:
            raise ValueError('p8_e2_collector_incomplete:' + ','.join(collector.errors))
        validation = validate_e2_result(workload_a=workloads['A'], workload_b=workloads['B'],
            ingress_delta=ingress, database_delta=delta, database_by_gateway=by_gateway,
            phase_events=events, raw_verification=raw_checks,
            execution_context={'samples_per_message': int(sample_key), 'role_to_target': mapping},
            resources={'generator_samples': collector.generator_samples, 'server': collector.resources})
        if collector.errors:
            validation['failures'].append('collector_incomplete:' + ','.join(collector.errors)); validation['valid'] = False
        profile_result = {'samples_per_message': int(sample_key), 'workloads': workloads,
            'database_delta': delta, 'database_by_gateway': by_gateway, 'raw_verification': raw_checks,
            'validation': validation, 'phase_events': events, 'collector_metrics': collector.metrics,
            'stage_histogram_windows': histogram_windows,
            'collector_resources': collector.resources, 'collector_generator_samples': collector.generator_samples,
            'terminal_admin_metrics': after_admin, 'terminal_server_resources': terminal_server,
            'global_delta_cross_check': delta, 'status': 'PASS' if validation['valid'] else 'FAIL'}
        smoke.durable_json(root / f'profile-{sample_key}.json', profile_result)
        profiles.append(profile_result)
        if not validation['valid']:
            result = {'status': 'FAIL', 'profiles': profiles, 'full_p8_accepted': False}
            smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json', result)
            raise ValueError('p8_e2_validation_failed:' + ','.join(validation['failures']))
        # P1 CSVs preserve existing counters and qualify unavailable nested histograms.
        p1_samples = [{'phase': 'workload_start', 'snapshot': baseline}]
        for item in collector.metrics:
            p1_samples.append({'phase': item['phase'], 'snapshot': item.get('snapshot'),
                               **({'error': item['error']} if item.get('error') else {})})
        p1_samples.append({'phase': 'workload_terminal', 'snapshot': after_admin})
        metrics_path = fixture.artifacts / f'metrics-{sample_key}.csv'
        smoke.paper_p1_metrics_csv(metrics_path, p1_samples)
        resources_path = fixture.artifacts / f'resources-{sample_key}.csv'
        smoke.paper_p1_resources_csv(resources_path, collector.resources + [terminal_server], {})
    result = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'profiles': profiles,
        'schema': schema, 'quota': quota, 'effective_flags': _validate_fixture_flags(effective, fixture),
        'fixture_profile': e2_fixture_profile(microbatch_wait_ms=50,
            profile=getattr(fixture, 'paper_p8_profile', 'pilot'),
            offered_rate=getattr(fixture, 'paper_p8_e2_offered_rate', 2),
            burst_messages=getattr(fixture, 'paper_p8_e2_burst_messages', 4),
            samples_per_message=getattr(fixture, 'paper_p8_e2_samples_per_message', 1),
            retry_rate=getattr(fixture, 'paper_p8_e2_retry_rate_per_second', 0),
            retry_burst=getattr(fixture, 'paper_p8_e2_retry_burst', 0)),
        'storage_status': storage_status,
        'source_revision': smoke.paper_p0_git(fixture, 'rev-parse', 'HEAD'),
        'source_fingerprint': _support._source_fingerprint(smoke.ROOT),
        'binary_sha256': hashlib.sha256(Path(paperload_binary).read_bytes()).hexdigest(),
        'manifest_sha256': [hashlib.sha256((root / f'{arm}-{s}.manifest.json').read_bytes()).hexdigest()
                            for s in ('100', '1') for arm in ('A', 'B')],
        'images': {name: {'id': value} for name, value in images.items()},
        'cross_clock_latency': {'available': None, 'reason': 'independent clocks'},
        'fairness': {'status': 'not_proven'}, 'full_p8_accepted': False,
        'generated_at': datetime.now(timezone.utc).isoformat()}
    smoke.durable_json(fixture.artifacts / 'p8-e2-pilot.json', result)
    smoke.durable_json(fixture.artifacts / 'run.json', result)
    return result


MAX_INT64 = (1 << 63) - 1

# Documented fixed maximum age policy: 30 days (2,592,000 seconds).
# Distinguishes helper context validity policy TTL from production parser facts.
MAX_CONTEXT_AGE_SECONDS_BOUND = 2592000

E2_EQUAL_SAMPLES_CONTEXT_KEYS = frozenset({
    'schema_version',
    'workload_type',
    'seed',
    'total_samples',
    'sample_rate',
    'duration_seconds',
    'logical_producers',
    'samples_per_producer',
    'producer_sample_rate',
    'sample_interval_us',
    'stream_sequence_start',
    'timestamp_anchor',
    'max_context_age_seconds',
    'context_sha256',
})

E2_EQUAL_SAMPLES_MANIFEST_KEYS = frozenset(
    set(E2_EQUAL_SAMPLES_CONTEXT_KEYS) | {
        'expected_samples',
        'expected_messages',
        'sample_stream_version',
    }
)


def synthetic_value(sequence: int) -> float:
    """Bounded deterministic simulated reading for global sample sequence.

    Triangular wave between 20.00 and 30.00 mirroring Go internal/gatewaysim/batch.go.
    """
    period = 2000
    pos = sequence % period
    if pos < 0:
        pos += period
    if pos > period // 2:
        pos = period - pos
    return round(20.0 + pos / 100.0, 2)


def calculate_e2_equal_samples_context_sha256(context: dict) -> str:
    """Calculate canonical SHA256 identity hex digest of an equal_samples context.

    Serializes canonical compact JSON over all context fields excluding 'context_sha256'.
    """
    if not isinstance(context, dict):
        raise ValueError('context must be a dictionary')
    payload = {k: v for k, v in context.items() if k != 'context_sha256'}
    canonical_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(canonical_bytes).hexdigest()


_RFC3339_UTC_Z_REGEX = re.compile(
    r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?Z$'
)


def _parse_rfc3339_anchor(anchor: str) -> datetime:
    """Parse and validate RFC3339 UTC timestamp anchor with uppercase 'Z' and calendar year 0001-9999."""
    if not isinstance(anchor, str) or isinstance(anchor, bool):
        raise ValueError('anchor must be a string')
    match = _RFC3339_UTC_Z_REGEX.fullmatch(anchor)
    if not match:
        if anchor.endswith('Z'):
            parts = anchor[:-1].split('-')
            if len(parts) >= 1 and (parts[0] == '0000' or (parts[0].isdigit() and (int(parts[0]) < 1 or int(parts[0]) > 9999))):
                raise ValueError('anchor_year_out_of_bounds')
        raise ValueError('anchor_must_be_utc_z')
    year, month, day, hour, minute, second, frac = match.groups()
    if year == '0000' or int(year) < 1 or int(year) > 9999:
        raise ValueError('anchor_year_out_of_bounds')
    if int(hour) > 23 or int(minute) > 59 or int(second) > 59:
        raise ValueError('anchor_must_be_utc_z')
    try:
        dt = datetime.fromisoformat(anchor[:-1] + '+00:00')
    except (ValueError, TypeError):
        raise ValueError('anchor_must_be_utc_z') from None
    if not (1 <= dt.year <= 9999):
        raise ValueError('anchor_year_out_of_bounds')
    if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
        raise ValueError('anchor_must_be_utc_z')
    return dt


def build_e2_equal_samples_context(
    *,
    total_samples=6000,
    sample_rate=100,
    seed=84,
    timestamp_anchor=None,
    max_context_age_seconds=86400,
    now_utc=None,
    **kwargs,
):
    """Construct a canonical versioned equal_samples context dictionary."""
    if kwargs:
        raise ValueError(f"unknown_context_parameters: {sorted(kwargs.keys())}")

    # Strict type and bound checks
    if type(total_samples) is not int:
        raise ValueError('total_samples must be an integer')
    if type(sample_rate) is not int:
        raise ValueError('sample_rate must be an integer')
    if type(seed) is not int or not (0 <= seed <= MAX_INT64):
        raise ValueError('seed must be an integer in [0, 2^63 - 1]')
    if type(max_context_age_seconds) is not int or max_context_age_seconds <= 0 or max_context_age_seconds > MAX_CONTEXT_AGE_SECONDS_BOUND:
        raise ValueError(f'max_context_age_seconds must be a positive integer <= {MAX_CONTEXT_AGE_SECONDS_BOUND}')

    # Bounds on total_samples
    if total_samples <= 0:
        raise ValueError('total_samples must be positive')
    if total_samples > 60000:
        raise ValueError('total_samples_max_exceeded')
    if total_samples % 200 != 0:
        raise ValueError('total_samples_divisible_by_200')

    # Bounds on sample_rate
    if not (2 <= sample_rate <= 200):
        raise ValueError('sample_rate_out_of_bounds')
    if sample_rate % 2 != 0:
        raise ValueError('sample_rate_must_be_even')

    # Duration check
    if total_samples % sample_rate != 0:
        raise ValueError('duration_must_be_integer')
    duration_seconds = total_samples // sample_rate
    if not (10 <= duration_seconds <= 120):
        raise ValueError('duration_seconds_out_of_bounds')

    # Microsecond interval check
    if 2_000_000 % sample_rate != 0:
        raise ValueError('sample_interval_us_must_be_integer')
    sample_interval_us = 2_000_000 // sample_rate

    logical_producers = ['A', 'B']
    samples_per_producer = total_samples // 2
    producer_sample_rate = sample_rate // 2
    stream_sequence_start = 0

    if now_utc is None:
        now_dt = datetime.now(timezone.utc)
    else:
        if not isinstance(now_utc, datetime) or now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
            raise ValueError('now_utc must be a UTC datetime')
        now_dt = now_utc

    stream_span_us = (samples_per_producer - 1) * sample_interval_us
    if timestamp_anchor is None:
        lead_seconds = 1
        anchor_dt = now_dt - timedelta(seconds=duration_seconds + lead_seconds)
        if anchor_dt.microsecond:
            timestamp_anchor = anchor_dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            timestamp_anchor = anchor_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        anchor_dt = _parse_rfc3339_anchor(timestamp_anchor)

    stream_end_dt = anchor_dt + timedelta(microseconds=stream_span_us)
    if stream_end_dt > now_dt:
        raise ValueError('equal_samples_stream_not_in_past')
    if anchor_dt > now_dt:
        raise ValueError('equal_samples_stream_not_in_past')

    age_seconds = (now_dt - anchor_dt).total_seconds()
    if age_seconds > max_context_age_seconds:
        raise ValueError('equal_samples_context_expired')

    ctx = {
        'schema_version': 1,
        'workload_type': 'equal_samples',
        'seed': seed,
        'total_samples': total_samples,
        'sample_rate': sample_rate,
        'duration_seconds': duration_seconds,
        'logical_producers': logical_producers,
        'samples_per_producer': samples_per_producer,
        'producer_sample_rate': producer_sample_rate,
        'sample_interval_us': sample_interval_us,
        'stream_sequence_start': stream_sequence_start,
        'timestamp_anchor': timestamp_anchor,
        'max_context_age_seconds': max_context_age_seconds,
    }
    ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(ctx)
    return ctx


def validate_e2_equal_samples_context(context: dict, *, now_utc=None, check_freshness: bool = True) -> dict:
    """Validate a canonical equal_samples context dictionary without mutating it.

    Rejects bools, floats, unknown keys, missing keys, invalid boundaries,
    hash mismatches, future timestamps, and expired contexts (when check_freshness is True).
    """
    if not isinstance(context, dict):
        raise ValueError('context must be a dictionary')

    keys = set(context.keys())
    if keys != E2_EQUAL_SAMPLES_CONTEXT_KEYS:
        unknown = keys - E2_EQUAL_SAMPLES_CONTEXT_KEYS
        missing = E2_EQUAL_SAMPLES_CONTEXT_KEYS - keys
        if unknown:
            raise ValueError(f'unknown_context_keys: {sorted(unknown)}')
        if missing:
            raise ValueError(f'missing_context_keys: {sorted(missing)}')

    int_fields = (
        'schema_version', 'seed', 'total_samples', 'sample_rate', 'duration_seconds',
        'samples_per_producer', 'producer_sample_rate', 'sample_interval_us',
        'stream_sequence_start', 'max_context_age_seconds'
    )
    for field in int_fields:
        val = context[field]
        if type(val) is not int:
            raise ValueError(f'{field} must be an integer, got {type(val)}')

    # Validate hash identity before inspecting semantic consistency
    expected_sha256 = calculate_e2_equal_samples_context_sha256(context)
    if context.get('context_sha256') != expected_sha256:
        raise ValueError('equal_samples_context_hash_mismatch')

    if context['schema_version'] != 1:
        raise ValueError('unsupported_schema_version')
    if context['workload_type'] != 'equal_samples':
        raise ValueError('unsupported_workload_type')
    if context['logical_producers'] != ['A', 'B']:
        raise ValueError('invalid_logical_producers')
    if context['stream_sequence_start'] != 0:
        raise ValueError('stream_sequence_start_must_be_zero')
    if not (0 <= context['seed'] <= MAX_INT64):
        raise ValueError('seed must be an integer in [0, 2^63 - 1]')
    if context['max_context_age_seconds'] <= 0 or context['max_context_age_seconds'] > MAX_CONTEXT_AGE_SECONDS_BOUND:
        raise ValueError('invalid_max_context_age_seconds')

    total_samples = context['total_samples']
    sample_rate = context['sample_rate']
    if total_samples <= 0 or total_samples > 60000 or total_samples % 200 != 0:
        raise ValueError('invalid_total_samples')
    if sample_rate < 2 or sample_rate > 200 or sample_rate % 2 != 0:
        raise ValueError('invalid_sample_rate')

    if total_samples % sample_rate != 0:
        raise ValueError('duration_must_be_integer')
    duration_seconds = total_samples // sample_rate
    if context['duration_seconds'] != duration_seconds or not (10 <= duration_seconds <= 120):
        raise ValueError('invalid_duration_seconds')

    if 2_000_000 % sample_rate != 0:
        raise ValueError('sample_interval_us_must_be_integer')
    sample_interval_us = 2_000_000 // sample_rate
    if context['sample_interval_us'] != sample_interval_us:
        raise ValueError('sample_interval_us_mismatch')

    if context['samples_per_producer'] != total_samples // 2:
        raise ValueError('samples_per_producer_mismatch')
    if context['producer_sample_rate'] != sample_rate // 2:
        raise ValueError('producer_sample_rate_mismatch')

    # Anchor & freshness validation
    anchor_str = context['timestamp_anchor']
    anchor_dt = _parse_rfc3339_anchor(anchor_str)

    if check_freshness:
        if now_utc is None:
            now_dt = datetime.now(timezone.utc)
        else:
            if not isinstance(now_utc, datetime) or now_utc.tzinfo is None or now_utc.utcoffset() != timedelta(0):
                raise ValueError('now_utc must be a UTC datetime')
            now_dt = now_utc

        stream_span_us = (context['samples_per_producer'] - 1) * sample_interval_us
        stream_end_dt = anchor_dt + timedelta(microseconds=stream_span_us)
        if stream_end_dt > now_dt or anchor_dt > now_dt:
            raise ValueError('equal_samples_stream_not_in_past')

        age_seconds = (now_dt - anchor_dt).total_seconds()
        if age_seconds > context['max_context_age_seconds']:
            raise ValueError('equal_samples_context_expired')

    return context


def calculate_e2_equal_samples_expected_counts(context: dict, samples_per_message: int) -> dict:
    """Calculate integer-derived per-role and total expected counts for S=1 or S=100.

    Validates context structure, bounds and hash identity without requiring wall-clock
    freshness so historical comparisons remain possible.
    """
    if type(samples_per_message) is not int or samples_per_message not in (1, 100):
        raise ValueError('samples_per_message must be an integer in (1, 100)')

    validated = validate_e2_equal_samples_context(context, check_freshness=False)

    total_samples = validated['total_samples']
    samples_per_producer = validated['samples_per_producer']
    logical_producers = validated['logical_producers']

    per_role = {}
    for role in logical_producers:
        if samples_per_producer % samples_per_message != 0:
            raise ValueError('samples_per_producer not divisible by samples_per_message')
        messages = samples_per_producer // samples_per_message
        per_role[role] = {
            'samples': samples_per_producer,
            'messages': messages,
            'raw_rows': samples_per_producer,
            'ledger_rows': messages,
            'temporal_rows': messages,
            'configuration_delta': 0,
        }

    total_messages = total_samples // samples_per_message
    return {
        'samples_per_message': samples_per_message,
        'total_samples': total_samples,
        'total_messages': total_messages,
        'total_raw_rows': total_samples,
        'total_ledger_rows': total_messages,
        'total_temporal_rows': total_messages,
        'total_configuration_delta': 0,
        'per_role': per_role,
    }


def e2_equal_samples_logical_stream(context: dict, role: str, samples_per_message: int) -> list:
    """Generate the logical sequence of messages and samples for one producer role.

    Verifies that the sequence of (sequence, timestamp, synthetic_value) is identical
    between S=1 and S=100.
    """
    if type(samples_per_message) is not int or isinstance(samples_per_message, bool) or samples_per_message not in (1, 100):
        raise ValueError('samples_per_message must be an integer in (1, 100)')
    if role not in context.get('logical_producers', []):
        raise ValueError(f'unknown_role: {role}')

    anchor_dt = _parse_rfc3339_anchor(context['timestamp_anchor'])
    interval_us = context['sample_interval_us']
    count = context['samples_per_producer']
    messages_count = count // samples_per_message

    messages = []
    for msg_idx in range(messages_count):
        msg_samples = []
        for s_idx in range(samples_per_message):
            seq = msg_idx * samples_per_message + s_idx
            sample_time = anchor_dt + timedelta(microseconds=seq * interval_us)
            if sample_time.microsecond:
                ts_str = sample_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                ts_str = sample_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            val = synthetic_value(seq)
            msg_samples.append({
                'sequence': seq,
                'measured_at': ts_str,
                'value': val,
            })
        messages.append({
            'message_index': msg_idx,
            'first_sequence': msg_idx * samples_per_message,
            'sample_count': samples_per_message,
            'samples': msg_samples,
        })
    return messages


def e2_equal_samples_manifests(
    *,
    context: dict,
    common: dict,
    targets: list,
    gateway_identity_files: dict,
    telemetry_identity_files: dict,
    grace_seconds: int = 10,
    now_utc=None,
    check_freshness: bool = True,
) -> dict:
    """Construct paired same-seed A/B manifests for equal_samples workload (S=1 and S=100).

    Context is mandatory and validated before use.
    Produces identical logical sample streams across both profiles (S=1 and S=100)
    with fixed safe limits (workers=2, connections=1, pending_messages=64, drain=20s).
    """
    if not isinstance(context, dict):
        raise ValueError('context must be a dictionary')
    validated_ctx = validate_e2_equal_samples_context(context, now_utc=now_utc, check_freshness=check_freshness)

    if not isinstance(common, dict):
        raise ValueError('common must be a dictionary')

    if not isinstance(targets, (list, tuple)) or len(targets) != 2:
        raise ValueError('e2_requires_two_gateway_targets')
    for t in targets:
        if not isinstance(t, dict) or not t.get('gateway_id') or not t.get('sensor_id'):
            raise ValueError('e2_requires_two_gateway_targets')
    if targets[0]['gateway_id'] == targets[1]['gateway_id']:
        raise ValueError('e2_requires_two_gateway_targets')

    if not isinstance(gateway_identity_files, dict) or set(gateway_identity_files) != {t['gateway_id'] for t in targets}:
        raise ValueError('e2_gateway_credentials_mismatch')
    if any(not v or not isinstance(v, str) for v in gateway_identity_files.values()):
        raise ValueError('e2_gateway_credentials_mismatch')

    if not isinstance(telemetry_identity_files, dict) or set(telemetry_identity_files) != {'A', 'B'}:
        raise ValueError('e2_telemetry_credentials_mismatch')
    if any(not v or not isinstance(v, str) for v in telemetry_identity_files.values()):
        raise ValueError('e2_telemetry_credentials_mismatch')

    if type(grace_seconds) is not int or grace_seconds < 0 or grace_seconds > 60:
        raise ValueError('grace_seconds must be an integer in [0, 60]')

    duration_seconds = validated_ctx['duration_seconds']
    drain_seconds = 20
    wall_seconds = duration_seconds + drain_seconds + grace_seconds

    expected_counts_by_profile = {
        '1': calculate_e2_equal_samples_expected_counts(validated_ctx, 1),
        '100': calculate_e2_equal_samples_expected_counts(validated_ctx, 100),
    }

    profiles = {}
    for samples in PILOT_SAMPLES:  # (100, 1)
        pair = {}
        expected_counts = expected_counts_by_profile[str(samples)]
        rate = validated_ctx['producer_sample_rate'] / samples
        rate_per_second = int(rate) if rate.is_integer() else rate

        for arm in ('A', 'B'):
            target_idx = 0 if arm == 'A' else 1
            target_gw = targets[target_idx]['gateway_id']

            equal_samples_obj = dict(validated_ctx)
            equal_samples_obj['expected_samples'] = expected_counts['per_role'][arm]['samples']
            equal_samples_obj['expected_messages'] = expected_counts['per_role'][arm]['messages']
            equal_samples_obj['sample_stream_version'] = 1

            m = {
                **common,
                'schema_version': 1,
                'experiment': 'E2',
                'arm': arm,
                'run_id': f"{common.get('run_id', 'e2')}-{samples}",
                'profile': 'pilot',
                'roles': ['telemetry'],
                'seed': validated_ctx['seed'],
                'samples_per_message': samples,
                'targets': [dict(targets[target_idx])],
                'subscriptions': [],
                'topology': {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
                'traffic': {
                    'rate_per_second': rate_per_second,
                    'ramp_seconds': 0,
                    'burst_messages': 0,
                },
                'phases': {
                    'warmup_seconds': 0,
                    'measure_seconds': duration_seconds,
                    'drain_seconds': drain_seconds,
                    'cooldown_seconds': 0,
                },
                'limits': {
                    'retries': 16,
                    'workers': 2,
                    'connections': 1,
                    'pending_messages': 64,
                    'pending_bytes': 1 << 20,
                    'disk_growth_bytes': 1 << 24,
                    'wall_seconds': wall_seconds,
                },
                'gateway_identity_files': {
                    target_gw: gateway_identity_files[target_gw],
                },
                'identity_files': {
                    'telemetry': telemetry_identity_files[arm],
                },
                'durability_mode': 'synthetic-load',
                'retry_interval_ms': 500,
                'equal_samples': equal_samples_obj,
            }
            pair[arm] = m
        profiles[str(samples)] = pair

    return {
        'profiles': profiles,
        'context': validated_ctx,
        'shared_seed': validated_ctx['seed'],
        'same_topology_security_quota_worker_policy': True,
        'execution': 'NOT RUN',
        'workload_type': 'equal_samples',
        'expected_counts': expected_counts_by_profile,
    }


def prevalidate_e2_reference(report: dict, current_context: dict) -> bool:
    """Prevalidate reference JSON report against current equal_samples context before launching subprocess."""
    if not isinstance(report, dict):
        raise ValueError('server_reference_rejected')
    if report.get('experiment') != 'E2':
        raise ValueError('server_reference_rejected')
    if report.get('workload') != 'equal_samples' and report.get('workload_type') != 'equal_samples':
        raise ValueError('server_reference_rejected')
    if report.get('status') != 'PASS':
        raise ValueError('server_reference_rejected')
    if report.get('cleanup_verified') is not True and report.get('exact_cleanup_verified') is not True:
        raise ValueError('server_reference_rejected')
    if report.get('full_p8_accepted') is not False:
        raise ValueError('server_reference_rejected')

    ref_ctx = report.get('context') or report.get('dataset_context')
    ref_ctx_hash = report.get('context_sha256') or (ref_ctx.get('context_sha256') if isinstance(ref_ctx, dict) else None)
    cur_ctx_hash = current_context.get('context_sha256')
    if not ref_ctx_hash or ref_ctx_hash != cur_ctx_hash:
        raise ValueError('server_reference_rejected')

    if isinstance(ref_ctx, dict):
        for k in ('seed', 'total_samples', 'duration_seconds', 'sample_rate', 'sample_interval_us', 'timestamp_anchor'):
            if k in ref_ctx and k in current_context and ref_ctx[k] != current_context[k]:
                raise ValueError('server_reference_rejected')

    ids = report.get('image_ids') or report.get('images')
    if not isinstance(ids, dict):
        raise ValueError('server_reference_rejected')
    for role in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
        val = ids.get(role)
        if isinstance(val, dict):
            val = val.get('id') or val.get('Id')
        if not isinstance(val, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', val):
            raise ValueError('server_reference_rejected')

    bin_hash = report.get('binary_sha256') or report.get('paperload_binary_sha256') or (
        report.get('binaries', {}).get('paperload', {}).get('sha256') if isinstance(report.get('binaries'), dict) else None)
    if not isinstance(bin_hash, str) or not re.fullmatch(r'[0-9a-f]{64}', bin_hash):
        raise ValueError('server_reference_rejected')

    schema = report.get('schema')
    if schema and schema != '24:telemetry_ledger_timestamp':
        raise ValueError('server_reference_rejected')

    return True


def validate_e2_reference(path, current_context: dict, *, current_source_fp=None,
                          current_schema='24:telemetry_ledger_timestamp') -> dict:
    """Validate bounded private reference report file and its immutable binaries/images."""
    if not path:
        raise ValueError('e2_reference_required')
    ref_path = Path(path).resolve()
    try:
        st = ref_path.lstat()
    except OSError:
        raise ValueError('e2_reference_file_rejected') from None

    if ref_path.is_symlink() or any(p.is_symlink() for p in ref_path.parents):
        raise ValueError('e2_reference_file_rejected')
    if not ref_path.is_file():
        raise ValueError('e2_reference_file_rejected')
    if st.st_uid != os.getuid() or (st.st_mode & 0o077):
        raise ValueError('e2_reference_file_rejected')
    if st.st_size > 8 * 1024 * 1024:
        raise ValueError('e2_reference_file_rejected')

    try:
        report = json.loads(ref_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        raise ValueError('e2_reference_artifact_invalid') from None

    if not isinstance(report, dict):
        raise ValueError('e2_reference_artifact_invalid')
    if report.get('experiment') != 'E2':
        raise ValueError('e2_reference_artifact_rejected')
    if report.get('workload') != 'equal_samples' and report.get('workload_type') != 'equal_samples':
        raise ValueError('e2_reference_artifact_rejected')
    if report.get('status') != 'PASS' or report.get('full_p8_accepted') is not False:
        raise ValueError('e2_reference_artifact_rejected')
    if report.get('cleanup_verified') is not True and report.get('exact_cleanup_verified') is not True:
        raise ValueError('e2_reference_cleanup_not_verified')
    if not report.get('validation', {}).get('valid', False) and report.get('workload_outcome') != 'VALIDATED':
        raise ValueError('e2_reference_artifact_rejected')

    ref_ctx = report.get('context') or report.get('dataset_context')
    ref_ctx_hash = report.get('context_sha256') or (ref_ctx.get('context_sha256') if isinstance(ref_ctx, dict) else None)
    cur_ctx_hash = current_context.get('context_sha256')
    if not ref_ctx_hash or ref_ctx_hash != cur_ctx_hash:
        raise ValueError('e2_reference_context_mismatch')
    if isinstance(ref_ctx, dict):
        for k in ('seed', 'total_samples', 'duration_seconds', 'sample_rate', 'sample_interval_us', 'timestamp_anchor'):
            if k in ref_ctx and k in current_context and ref_ctx[k] != current_context[k]:
                raise ValueError('e2_reference_context_mismatch')

    if current_source_fp is not None:
        ref_source = report.get('source_fingerprint') or report.get('source', {}).get('source_fingerprint')
        if not isinstance(ref_source, dict) or not ref_source.get('revision'):
            raise ValueError('e2_reference_source_mismatch')
        if ref_source.get('revision') != current_source_fp.get('revision'):
            raise ValueError('e2_reference_source_mismatch')
        if ref_source.get('dirty_source_sha256') != current_source_fp.get('dirty_source_sha256'):
            raise ValueError('e2_reference_source_mismatch')

    ref_schema = report.get('schema')
    if ref_schema and ref_schema != current_schema:
        raise ValueError('e2_reference_schema_mismatch')

    raw_ids = report.get('image_ids') or report.get('images')
    if not isinstance(raw_ids, dict):
        raise ValueError('e2_reference_images_invalid')
    image_ids = {}
    for k, v in raw_ids.items():
        if isinstance(v, dict):
            image_ids[k] = v.get('id') or v.get('Id')
        else:
            image_ids[k] = v
    for role in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
        val = image_ids.get(role)
        if not isinstance(val, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', val):
            raise ValueError('e2_reference_images_invalid')

    expected_bin_hash = report.get('binary_sha256') or report.get('paperload_binary_sha256') or (
        report.get('binaries', {}).get('paperload', {}).get('sha256') if isinstance(report.get('binaries'), dict) else None)
    if not isinstance(expected_bin_hash, str) or not re.fullmatch(r'[0-9a-f]{64}', expected_bin_hash):
        raise ValueError('e2_reference_binary_missing')

    # Locate binary file
    candidate_paths = []
    rel_path = report.get('binary_path') or report.get('paperload_binary_path') or (
        report.get('binaries', {}).get('paperload', {}).get('path') if isinstance(report.get('binaries'), dict) else None)
    if rel_path:
        cand = Path(rel_path)
        if not cand.is_absolute():
            cand = ref_path.parent / cand
        candidate_paths.append(cand)
    candidate_paths.append(ref_path.parent / 'bin' / 'paperload')
    candidate_paths.append(ref_path.parent / 'paperload')
    candidate_paths.append(ref_path.parent.parent / 'artifacts' / 'bin' / 'paperload')

    found_bin = None
    for cand in candidate_paths:
        try:
            if cand.is_file():
                found_bin = cand
                break
        except OSError:
            pass

    if found_bin is None:
        raise ValueError('e2_reference_binary_missing')

    try:
        bin_st = found_bin.lstat()
    except OSError:
        raise ValueError('e2_reference_binary_missing') from None
    if found_bin.is_symlink() or not found_bin.is_file() or bin_st.st_uid != os.getuid() or (bin_st.st_mode & 0o077):
        raise ValueError('e2_reference_binary_missing')
    raw_bin = found_bin.read_bytes()
    if hashlib.sha256(raw_bin).hexdigest() != expected_bin_hash:
        raise ValueError('e2_reference_binary_mismatch')

    return {
        'status': 'PASS',
        'image_ids': image_ids,
        'binary_sha256': expected_bin_hash,
        'binary_path': found_bin,
        'report': report,
    }


def _compute_ratio_100_over_1(val_100, val_1, *, metric_name):
    """Compute ratio profile_100 / profile_1 with strict numeric rejection."""
    if isinstance(val_1, bool) or isinstance(val_100, bool):
        return {
            'ratio': None,
            'available': False,
            'unavailable_reason': f'invalid_numeric_type_bool_in_{metric_name}',
            'profile_1_value': None,
            'profile_100_value': None,
        }
    if val_1 is None or val_100 is None:
        missing = []
        if val_1 is None:
            missing.append('profile_1')
        if val_100 is None:
            missing.append('profile_100')
        return {
            'ratio': None,
            'available': False,
            'unavailable_reason': f'missing_metric_in_{",".join(missing)}',
            'profile_1_value': val_1,
            'profile_100_value': val_100,
        }
    if not isinstance(val_1, (int, float)) or not isinstance(val_100, (int, float)):
        return {
            'ratio': None,
            'available': False,
            'unavailable_reason': f'non_numeric_type_in_{metric_name}',
            'profile_1_value': None,
            'profile_100_value': None,
        }
    if math.isnan(val_1) or math.isnan(val_100) or math.isinf(val_1) or math.isinf(val_100):
        return {
            'ratio': None,
            'available': False,
            'unavailable_reason': f'nan_or_inf_in_{metric_name}',
            'profile_1_value': None,
            'profile_100_value': None,
        }
    if val_1 < 0 or val_100 < 0:
        return {
            'ratio': None,
            'available': False,
            'unavailable_reason': f'negative_value_in_{metric_name}',
            'profile_1_value': val_1,
            'profile_100_value': val_100,
        }
    if val_1 == 0:
        return {
            'ratio': None,
            'available': False,
            'unavailable_reason': 'zero_denominator',
            'profile_1_value': val_1,
            'profile_100_value': val_100,
        }
    ratio = val_100 / val_1
    return {
        'ratio': round(ratio, 6),
        'available': True,
        'unavailable_reason': None,
        'profile_1_value': val_1,
        'profile_100_value': val_100,
    }


def compare_e2_reports(report_a: dict, report_b: dict) -> dict:
    """Compare two private bounded owned E2 equal_samples reports read-only."""
    if not isinstance(report_a, dict) or not isinstance(report_b, dict):
        raise ValueError('compare_e2_requires_dict_reports')

    s_a = report_a.get('samples_per_message')
    s_b = report_b.get('samples_per_message')
    if {s_a, s_b} != {1, 100}:
        mismatch_reasons = [f'samples_per_message_pair_invalid: expected exactly S=1 and S=100, got ({s_a}, {s_b})']
        return {
            'schema_version': 1,
            'experiment': 'E2',
            'workload': 'equal_samples',
            'status': 'NOT_COMPARABLE',
            'comparable': False,
            'mismatch_reasons': mismatch_reasons,
            'functional_validity': {
                'profile_1': {'valid': False, 'status': 'FAIL', 'failures': ['invalid_profile_pairing']},
                'profile_100': {'valid': False, 'status': 'FAIL', 'failures': ['invalid_profile_pairing']},
            },
            'cleanup': {'profile_1_verified': False, 'profile_100_verified': False, 'all_cleanup_verified': False},
            'workload_contamination': {
                'profile_1': {'clean': False, 'contamination_reasons': []},
                'profile_100': {'clean': False, 'contamination_reasons': []},
                'contaminated': False,
                'clean_comparison': False,
            },
            'qualifications': {
                'full_p8_accepted': False,
                'physical_ab': 'NOT RUN',
                'generator_colocated': 'unverified',
                'same_host_qualification': 'same-host/unverified',
                'cpu_window_label': 'active_cpu_seconds vs lifecycle_cpu_seconds',
                'cpu_granularity_label': 'sampler_interval_1s',
                'claims_disclaimer': 'No acquisition, wire, or absolute-peak claims; backend observed payload bytes; sampled max memory only',
            },
            'normalized_metrics': {'profile_1': {}, 'profile_100': {}},
            'ratios_100_over_1': {},
            'per_metric_quality': {},
            'headline_speedup': {'available': False, 'reason': 'not_comparable_no_headline_speedup'},
        }

    rep_1 = report_a if s_a == 1 else report_b
    rep_100 = report_a if s_a == 100 else report_b

    # 1. Functional validity per run
    functional_validity = {}
    for s_val, rep, p_key in ((1, rep_1, 'profile_1'), (100, rep_100, 'profile_100')):
        failures = []
        if rep.get('experiment') != 'E2':
            failures.append('experiment_not_E2')
        if rep.get('workload') not in ('equal_samples',) and rep.get('workload_type') not in ('equal_samples',):
            failures.append('workload_not_equal_samples')
        if rep.get('status') != 'PASS':
            failures.append('status_not_PASS')
        if rep.get('full_p8_accepted') is not False:
            failures.append('full_p8_accepted_must_be_false')
        clean_v = (rep.get('exact_cleanup_verified') is True or rep.get('cleanup_verified') is True)
        if not clean_v:
            failures.append('cleanup_not_verified')

        val = rep.get('validation')
        if not isinstance(val, dict):
            failures.append('validation_missing')
        else:
            if val.get('valid') is not True:
                failures.append('validation_invalid')
            if val.get('failures'):
                failures.extend([f'validation_failure:{f}' for f in val['failures']])

        ctx = rep.get('context')
        exp_counts = rep.get('expected_counts')
        if not isinstance(exp_counts, dict):
            failures.append('expected_counts_missing')
        else:
            tot_s = exp_counts.get('total_samples')
            if not isinstance(tot_s, int) or tot_s <= 0:
                failures.append('expected_total_samples_invalid')
            if isinstance(ctx, dict):
                ctx_tot_s = ctx.get('total_samples')
                if isinstance(ctx_tot_s, int) and tot_s != ctx_tot_s:
                    failures.append('expected_counts_context_total_samples_mismatch')

        act_counts = rep.get('actual_counters')
        if not isinstance(act_counts, dict):
            failures.append('actual_counters_missing')
        else:
            for r in ('A', 'B'):
                r_act = act_counts.get(r) or rep.get('workloads', {}).get(r, {}).get('telemetry')
                if not isinstance(r_act, dict):
                    failures.append(f'actual_counters_missing_role_{r}')
                else:
                    if 'pending_messages' not in r_act or not isinstance(r_act['pending_messages'], int):
                        failures.append(f'missing_pending_messages_role_{r}')
                    elif r_act['pending_messages'] != 0:
                        failures.append(f'pending_not_drained_role_{r}')

                    if 'pending_bytes' not in r_act or not isinstance(r_act['pending_bytes'], int):
                        failures.append(f'missing_pending_bytes_role_{r}')
                    elif r_act['pending_bytes'] != 0:
                        failures.append(f'pending_bytes_not_drained_role_{r}')

                    exp_m = exp_counts.get('per_role', {}).get(r, {}).get('messages') if isinstance(exp_counts, dict) else None
                    for cfield in ('application_acks', 'generated_unique', 'offered_unique'):
                        if cfield not in r_act or not isinstance(r_act[cfield], int):
                            failures.append(f'missing_{cfield}_role_{r}')
                        elif isinstance(exp_m, int) and r_act[cfield] != exp_m:
                            failures.append(f'mismatch_{cfield}_role_{r}')

        raw_ver = rep.get('raw_verification')
        if isinstance(raw_ver, list) and len(raw_ver) >= 2:
            raw_ver = {'A': raw_ver[0], 'B': raw_ver[1]}
        elif isinstance(raw_ver, list) and len(raw_ver) == 1:
            raw_ver = {'A': raw_ver[0]}
        if not isinstance(raw_ver, dict):
            failures.append('raw_verification_missing')
        else:
            for r in ('A', 'B'):
                r_v = raw_ver.get(r) if r in raw_ver else (raw_ver.get('per_role', {}).get(r) if 'per_role' in raw_ver else None)
                if not isinstance(r_v, dict):
                    failures.append(f'raw_verification_missing_role_{r}')
                else:
                    if (r_v.get('raw_identity_value_mismatches', 0) != 0
                            or r_v.get('timestamp_mismatches', 0) != 0
                            or r_v.get('sequence_gaps_or_overlaps', 0) != 0
                            or r_v.get('provenance_mismatches', 0) != 0):
                        failures.append(f'raw_verification_failed_role_{r}')

        functional_validity[p_key] = {
            'valid': len(failures) == 0,
            'status': 'PASS' if len(failures) == 0 else 'FAIL',
            'failures': failures,
        }

    # 2. Cleanup verification per run
    c_1_ok = (rep_1.get('exact_cleanup_verified') is True or rep_1.get('cleanup_verified') is True)
    c_100_ok = (rep_100.get('exact_cleanup_verified') is True or rep_100.get('cleanup_verified') is True)
    cleanup_status = {
        'profile_1_verified': c_1_ok,
        'profile_100_verified': c_100_ok,
        'all_cleanup_verified': c_1_ok and c_100_ok,
    }

    # 3. Comparability (mismatch reasons)
    mismatch_reasons = []

    if rep_1.get('full_p8_accepted') is not False:
        mismatch_reasons.append(f'full_p8_accepted_must_be_false:profile_1={rep_1.get("full_p8_accepted")}')
    if rep_100.get('full_p8_accepted') is not False:
        mismatch_reasons.append(f'full_p8_accepted_must_be_false:profile_100={rep_100.get("full_p8_accepted")}')

    if not c_1_ok:
        mismatch_reasons.append('cleanup_not_verified:profile_1')
    if not c_100_ok:
        mismatch_reasons.append('cleanup_not_verified:profile_100')

    # seed
    seed_1 = rep_1.get('seed') if 'seed' in rep_1 else (rep_1.get('context', {}).get('seed') if isinstance(rep_1.get('context'), dict) else None)
    seed_100 = rep_100.get('seed') if 'seed' in rep_100 else (rep_100.get('context', {}).get('seed') if isinstance(rep_100.get('context'), dict) else None)
    if seed_1 is None or seed_100 is None:
        mismatch_reasons.append('missing_required_field:seed')
    elif seed_1 != seed_100:
        mismatch_reasons.append(f'seed_mismatch: profile_1={seed_1} profile_100={seed_100}')

    # context
    ctx_1 = rep_1.get('context')
    ctx_100 = rep_100.get('context')
    if not isinstance(ctx_1, dict) or not isinstance(ctx_100, dict):
        mismatch_reasons.append('missing_required_field:context')
    else:
        try:
            validate_e2_equal_samples_context(ctx_1, check_freshness=False)
        except Exception as exc:
            mismatch_reasons.append(f'context_invalid_profile_1:{exc}')
        try:
            validate_e2_equal_samples_context(ctx_100, check_freshness=False)
        except Exception as exc:
            mismatch_reasons.append(f'context_invalid_profile_100:{exc}')

        h1 = rep_1.get('context_sha256') if 'context_sha256' in rep_1 else ctx_1.get('context_sha256')
        h100 = rep_100.get('context_sha256') if 'context_sha256' in rep_100 else ctx_100.get('context_sha256')
        if not h1 or not h100:
            mismatch_reasons.append('missing_required_field:context_sha256')
        elif h1 != h100:
            mismatch_reasons.append(f'context_sha256_mismatch: profile_1={h1} profile_100={h100}')

        canonical_keys = (
            'seed', 'total_samples', 'sample_rate', 'duration_seconds',
            'logical_producers', 'samples_per_producer', 'producer_sample_rate',
            'sample_interval_us', 'stream_sequence_start', 'timestamp_anchor',
            'max_context_age_seconds'
        )
        for ck in canonical_keys:
            v1 = ctx_1.get(ck)
            v100 = ctx_100.get(ck)
            if v1 is None or v100 is None:
                mismatch_reasons.append(f'missing_required_context_field:{ck}')
            elif v1 != v100:
                mismatch_reasons.append(f'context_field_mismatch:{ck}: profile_1={v1} profile_100={v100}')

    # schema
    sch_1 = rep_1.get('schema')
    sch_100 = rep_100.get('schema')
    if sch_1 is None or sch_100 is None:
        mismatch_reasons.append('missing_required_field:schema')
    elif sch_1 != sch_100:
        mismatch_reasons.append(f'schema_mismatch: profile_1={sch_1} profile_100={sch_100}')
    elif sch_1 != '24:telemetry_ledger_timestamp':
        mismatch_reasons.append(f'schema_invalid:{sch_1}')

    # logical_topology
    top_1 = rep_1.get('logical_topology')
    top_100 = rep_100.get('logical_topology')
    if not isinstance(top_1, dict) or not isinstance(top_100, dict):
        mismatch_reasons.append('missing_required_field:logical_topology')
    else:
        for tk in ('gateways', 'sensors', 'sensors_per_gateway', 'users', 'viewers'):
            v1 = top_1.get(tk)
            v100 = top_100.get(tk)
            if v1 is None or v100 is None:
                mismatch_reasons.append(f'missing_required_topology_field:{tk}')
            elif v1 != v100:
                mismatch_reasons.append(f'topology_mismatch:{tk}: profile_1={v1} profile_100={v100}')

    # effective_flags
    flags_1 = rep_1.get('effective_flags')
    flags_100 = rep_100.get('effective_flags')
    if not isinstance(flags_1, dict) or not isinstance(flags_100, dict):
        mismatch_reasons.append('missing_required_field:effective_flags')
    else:
        def _norm_f(v):
            if isinstance(v, bool): return 'true' if v else 'false'
            if isinstance(v, (int, float)): return str(v)
            return str(v).lower() if isinstance(v, str) else v
        n_f1 = {k: _norm_f(v) for k, v in flags_1.items()}
        n_f100 = {k: _norm_f(v) for k, v in flags_100.items()}
        for fk in ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'WEBSOCKET_ENABLED',
                   'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED'):
            if fk not in n_f1 or fk not in n_f100:
                mismatch_reasons.append(f'missing_required_effective_flag:{fk}')
            elif n_f1[fk] != n_f100[fk]:
                mismatch_reasons.append(f'effective_flags_mismatch:{fk}: profile_1={n_f1[fk]} profile_100={n_f100[fk]}')

    # quota
    q_1 = rep_1.get('quota')
    q_100 = rep_100.get('quota')
    if not isinstance(q_1, dict) or not isinstance(q_100, dict):
        mismatch_reasons.append('missing_required_field:quota')
    else:
        has_quota_field = False
        if 'cpus' in q_1 or 'cpus' in q_100:
            has_quota_field = True
            for qk in ('cpus', 'memory_bytes'):
                v1 = q_1.get(qk)
                v100 = q_100.get(qk)
                if v1 is None or v100 is None:
                    mismatch_reasons.append(f'missing_required_quota_field:{qk}')
                elif v1 != v100:
                    mismatch_reasons.append(f'quota_mismatch:{qk}: profile_1={v1} profile_100={v100}')
        if 'rate_per_second' in q_1 or 'rate_per_second' in q_100:
            has_quota_field = True
            for qk in ('rate_per_second', 'burst'):
                v1 = q_1.get(qk)
                v100 = q_100.get(qk)
                if v1 is None or v100 is None:
                    mismatch_reasons.append(f'missing_required_quota_field:{qk}')
                elif v1 != v100:
                    mismatch_reasons.append(f'quota_mismatch:{qk}: profile_1={v1} profile_100={v100}')
        if not has_quota_field:
            mismatch_reasons.append('missing_required_field:quota')

    # workers
    w_1 = rep_1.get('workers')
    w_100 = rep_100.get('workers')
    if not isinstance(w_1, dict) or not isinstance(w_100, dict):
        mismatch_reasons.append('missing_required_field:workers')
    else:
        for wk in ('workers', 'connections', 'pending_messages'):
            v1 = w_1.get(wk)
            v100 = w_100.get(wk)
            if v1 is None or v100 is None:
                mismatch_reasons.append(f'missing_required_workers_field:{wk}')
            elif v1 != v100:
                mismatch_reasons.append(f'workers_mismatch:{wk}: profile_1={v1} profile_100={v100}')

    # binary_sha256
    bin_1 = rep_1.get('binary_sha256') or rep_1.get('paperload_binary_sha256')
    bin_100 = rep_100.get('binary_sha256') or rep_100.get('paperload_binary_sha256')
    if not bin_1 or not bin_100:
        mismatch_reasons.append('missing_required_field:binary_sha256')
    elif bin_1 != bin_100:
        mismatch_reasons.append(f'binary_sha256_mismatch: profile_1={bin_1} profile_100={bin_100}')
    elif not re.fullmatch(r'[0-9a-f]{64}', bin_1):
        mismatch_reasons.append('binary_sha256_invalid_format')

    # image_ids
    imgs_1 = rep_1.get('image_ids') or rep_1.get('images')
    imgs_100 = rep_100.get('image_ids') or rep_100.get('images')
    if not isinstance(imgs_1, dict) or not isinstance(imgs_100, dict):
        mismatch_reasons.append('missing_required_field:image_ids')
    else:
        def _ext_id(val):
            return (val.get('id') or val.get('Id')) if isinstance(val, dict) else val
        map_1 = {k: _ext_id(v) for k, v in imgs_1.items()}
        map_100 = {k: _ext_id(v) for k, v in imgs_100.items()}
        for svc in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
            id1 = map_1.get(svc)
            id100 = map_100.get(svc)
            if not id1 or not id100:
                mismatch_reasons.append(f'missing_required_image_id:{svc}')
            elif id1 != id100:
                mismatch_reasons.append(f'image_id_mismatch:{svc}: profile_1={id1} profile_100={id100}')
            elif not re.fullmatch(r'sha256:[0-9a-f]{64}', id1):
                mismatch_reasons.append(f'image_id_invalid_format:{svc}')

    # source_fingerprint
    src_1 = rep_1.get('source_fingerprint')
    src_100 = rep_100.get('source_fingerprint')
    if not isinstance(src_1, dict) or not isinstance(src_100, dict):
        mismatch_reasons.append('missing_required_field:source_fingerprint')
    else:
        for sk in ('revision', 'dirty'):
            v1 = src_1.get(sk)
            v100 = src_100.get(sk)
            if v1 is None or v100 is None:
                mismatch_reasons.append(f'missing_required_source_fingerprint_field:{sk}')
            elif v1 != v100:
                mismatch_reasons.append(f'source_fingerprint_mismatch:{sk}: profile_1={v1} profile_100={v100}')
        if src_1.get('dirty'):
            v1 = src_1.get('dirty_source_sha256')
            v100 = src_100.get('dirty_source_sha256')
            if v1 is None or v100 is None:
                mismatch_reasons.append('missing_required_source_fingerprint_field:dirty_source_sha256')
            elif v1 != v100:
                mismatch_reasons.append(f'source_fingerprint_mismatch:dirty_source_sha256: profile_1={v1} profile_100={v100}')
        else:
            if src_1.get('dirty_source_sha256') != src_100.get('dirty_source_sha256'):
                mismatch_reasons.append(f'source_fingerprint_mismatch:dirty_source_sha256: profile_1={src_1.get("dirty_source_sha256")} profile_100={src_100.get("dirty_source_sha256")}')

    comparable = (len(mismatch_reasons) == 0)

    # 4. Workload Contamination
    contam_reasons_1 = []
    contam_reasons_100 = []
    for rep, clist in ((rep_1, contam_reasons_1), (rep_100, contam_reasons_100)):
        if rep.get('clean_comparison') is False:
            if 'clean_comparison_false' not in clist:
                clist.append('clean_comparison_false')
        if isinstance(rep.get('contamination_reasons'), list):
            for cr in rep['contamination_reasons']:
                cr_str = str(cr)
                if cr_str not in clist:
                    clist.append(cr_str)
        ing = rep.get('ingress_delta')
        if isinstance(ing, dict):
            delta = ing.get('delta') if 'delta' in ing else ing
            if isinstance(delta, dict):
                for k in ('ShaperRateLimited', 'QueueFull', 'ByteBudgetFull', 'RejectedNotRunning',
                          'RejectedStopping', 'RejectedRetained', 'RejectedOversized', 'RejectedInvalidTopic'):
                    if delta.get(k, 0) > 0:
                        reason_str = f'ingress_{k}:{delta[k]}'
                        if reason_str not in clist:
                            clist.append(reason_str)
        c_errs = rep.get('collector_errors')
        if isinstance(c_errs, list) and c_errs:
            err_str = 'collector_errors:' + ','.join(str(e) for e in c_errs)
            if err_str not in clist:
                clist.append(err_str)

    contaminated = bool(contam_reasons_1 or contam_reasons_100)
    clean_comparison = not contaminated

    # 5. Normalized metrics and Ratios 100/1
    def _get_norm(rep):
        nm = rep.get('normalized_metrics') or rep.get('measurement_quality', {}).get('normalized_metrics')
        if isinstance(nm, dict):
            return dict(nm)
        return {}

    norm_1 = _get_norm(rep_1)
    norm_100 = _get_norm(rep_100)

    def _metric_evidence_available(rep, norm, m):
        mq = rep.get('measurement_quality', {}) if isinstance(rep.get('measurement_quality'), dict) else {}
        pm = mq.get('per_metric', {}) if isinstance(mq.get('per_metric'), dict) else (rep.get('per_metric', {}) if isinstance(rep.get('per_metric'), dict) else {})
        if m == 'bytes_per_sample':
            p_stat = pm.get('payload_bytes', {}).get('status') if isinstance(pm.get('payload_bytes'), dict) else None
            if p_stat == 'INCONCLUSIVE':
                return False
            return norm.get('bytes_per_sample') is not None
        elif m == 'db_transactions_per_sample':
            p_stat = pm.get('db_transactions', {}).get('status') if isinstance(pm.get('db_transactions'), dict) else None
            if p_stat == 'INCONCLUSIVE':
                return False
            if norm.get('db_transactions_available') is False:
                return False
            return norm.get('db_transactions_per_sample') is not None
        elif m == 'active_cpu_seconds_per_sample':
            p_stat = pm.get('server_active_cpu', {}).get('status') if isinstance(pm.get('server_active_cpu'), dict) else None
            if p_stat == 'INCONCLUSIVE':
                return False
            if norm.get('active_cpu_available') is False:
                return False
            return norm.get('active_cpu_seconds_per_sample') is not None
        elif m == 'lifecycle_cpu_seconds_per_sample':
            p_stat = pm.get('server_lifecycle_cpu', {}).get('status') if isinstance(pm.get('server_lifecycle_cpu'), dict) else None
            if p_stat == 'INCONCLUSIVE':
                return False
            if norm.get('lifecycle_cpu_available') is False:
                return False
            return norm.get('lifecycle_cpu_seconds_per_sample') is not None
        elif m == 'generator_active_cpu_seconds_per_sample':
            p_stat = pm.get('generator_active_cpu', {}).get('status') if isinstance(pm.get('generator_active_cpu'), dict) else None
            if p_stat == 'INCONCLUSIVE':
                return False
            if norm.get('generator_active_cpu_available') is False:
                return False
            return norm.get('generator_active_cpu_seconds_per_sample') is not None
        elif m == 'generator_lifecycle_cpu_seconds_per_sample':
            p_stat = pm.get('generator_lifecycle_cpu', {}).get('status') if isinstance(pm.get('generator_lifecycle_cpu'), dict) else None
            if p_stat == 'INCONCLUSIVE':
                return False
            if norm.get('generator_lifecycle_cpu_available') is False:
                return False
            return norm.get('generator_lifecycle_cpu_seconds_per_sample') is not None
        return True

    ratios = {}
    per_metric_quality = {}
    for m in ('bytes_per_sample', 'db_transactions_per_sample',
              'active_cpu_seconds_per_sample', 'lifecycle_cpu_seconds_per_sample',
              'generator_active_cpu_seconds_per_sample', 'generator_lifecycle_cpu_seconds_per_sample'):
        ev_1 = _metric_evidence_available(rep_1, norm_1, m)
        ev_100 = _metric_evidence_available(rep_100, norm_100, m)
        if not ev_1 or not ev_100:
            missing = []
            if not ev_1:
                missing.append('profile_1')
            if not ev_100:
                missing.append('profile_100')
            unavail_reason = f'missing_metric_in_{",".join(missing)}'
            ratios[m] = {
                'ratio': None,
                'available': False,
                'unavailable_reason': unavail_reason,
                'profile_1_value': norm_1.get(m) if ev_1 else None,
                'profile_100_value': norm_100.get(m) if ev_100 else None,
            }
            per_metric_quality[m] = {
                'available': False,
                'status': 'INCONCLUSIVE',
                'reason': unavail_reason,
            }
        else:
            res_ratio = _compute_ratio_100_over_1(norm_100.get(m), norm_1.get(m), metric_name=m)
            ratios[m] = res_ratio
            per_metric_quality[m] = {
                'available': res_ratio['available'],
                'status': 'AVAILABLE' if res_ratio['available'] else 'INCONCLUSIVE',
                'reason': res_ratio['unavailable_reason'],
            }

    # 6. Headline comparison
    if not comparable:
        headline_speedup = {'available': False, 'reason': 'not_comparable_no_headline_speedup'}
    elif not functional_validity['profile_1']['valid'] or not functional_validity['profile_100']['valid']:
        headline_speedup = {'available': False, 'reason': 'functional_invalid_no_headline_speedup'}
    elif not c_1_ok or not c_100_ok:
        headline_speedup = {'available': False, 'reason': 'cleanup_not_verified_no_headline_speedup'}
    elif contaminated:
        headline_speedup = {'available': False, 'reason': 'contaminated_run_no_headline_speedup'}
    else:
        headline_speedup = {
            'available': True,
            'reason': None,
            'bytes_per_sample_ratio': ratios['bytes_per_sample'].get('ratio'),
            'db_transactions_ratio': ratios['db_transactions_per_sample'].get('ratio'),
            'active_cpu_seconds_ratio': ratios['active_cpu_seconds_per_sample'].get('ratio'),
        }

    # 7. Overall status
    if not comparable:
        status = 'NOT_COMPARABLE'
    elif not functional_validity['profile_1']['valid'] or not functional_validity['profile_100']['valid']:
        status = 'FUNCTIONAL_FAILURE'
    elif not c_1_ok or not c_100_ok:
        status = 'CLEANUP_NOT_VERIFIED'
    elif contaminated:
        status = 'CONTAMINATED'
    else:
        status = 'COMPARABLE_CLEAN'

    return {
        'schema_version': 1,
        'experiment': 'E2',
        'workload': 'equal_samples',
        'status': status,
        'comparable': comparable,
        'mismatch_reasons': mismatch_reasons,
        'functional_validity': functional_validity,
        'cleanup': cleanup_status,
        'workload_contamination': {
            'profile_1': {'clean': not bool(contam_reasons_1), 'contamination_reasons': contam_reasons_1},
            'profile_100': {'clean': not bool(contam_reasons_100), 'contamination_reasons': contam_reasons_100},
            'contaminated': contaminated,
            'clean_comparison': clean_comparison,
        },
        'context': {
            'context_sha256': rep_1.get('context_sha256') or (rep_1.get('context', {}).get('context_sha256') if isinstance(rep_1.get('context'), dict) else None),
            'seed': seed_1,
            'total_samples': ctx_1.get('total_samples') if isinstance(ctx_1, dict) else None,
        },
        'qualifications': {
            'full_p8_accepted': False,
            'physical_ab': 'NOT RUN',
            'generator_colocated': 'unverified',
            'same_host_qualification': 'same-host/unverified',
            'cpu_window_label': 'active_cpu_seconds vs lifecycle_cpu_seconds',
            'cpu_granularity_label': 'sampler_interval_1s',
            'claims_disclaimer': 'No acquisition, wire, or absolute-peak claims; backend observed payload bytes; sampled max memory only',
        },
        'normalized_metrics': {
            'profile_1': norm_1,
            'profile_100': norm_100,
        },
        'ratios_100_over_1': ratios,
        'per_metric_quality': per_metric_quality,
        'headline_speedup': headline_speedup,
    }
