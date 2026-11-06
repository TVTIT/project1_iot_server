#!/usr/bin/env python3
"""Foreground local-file operator interface for bounded P8 E1/E2 fixtures."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import base64
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts' / 'tests'))
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT))
import paper_p8_remote as remote
import paper_p8_supervisor as supervisor
import paper_p8_e5_import as e5_import
import paper_p8_support as p8_support


def build_e1_offline_contract(*, viewers, principals, density, messages_per_target,
                              scratch_root=None, paperload_binary=None):
    """Build and inspect a schema-only large E1 bundle without starting a workload."""
    if type(principals) is not int or principals != 2:
        raise ValueError('e1_fixture_principal_topology_unsupported')
    if type(viewers) is not int or viewers not in (128, 2000, 5000, 10000):
        raise ValueError('e1_offline_rung_unsupported')
    import paper_p8_e1_support as e1
    import paper_p8_bundle as bundle
    targets = [{'gateway_id': f'gw-{gateway}', 'sensor_id': f'sensor-{sensor}'}
               for gateway in range(2) for sensor in range(2)]
    mapping = e1.mixed_e1_mapping(targets, seed=86, viewers=viewers, principals=principals,
                                  density=density, messages_per_target=messages_per_target)
    if scratch_root is None:
        scratch_base = Path('/var/tmp/opencode/p8-e1-large-viewer-repair')
        scratch_base.mkdir(mode=0o700, parents=True, exist_ok=True)
        scratch_parent = Path(tempfile.mkdtemp(prefix='e1r5-', dir=scratch_base))
    else:
        scratch_parent = Path(scratch_root)
        scratch_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if (scratch_parent.is_symlink() or scratch_parent.stat().st_uid != os.getuid()
            or scratch_parent.stat().st_mode & 0o077):
        raise ValueError('e1_offline_scratch_rejected')
    with tempfile.TemporaryDirectory(prefix='contract-', dir=scratch_parent) as td:
        root = Path(td); root.chmod(0o700)
        private = root / 'private'; private.mkdir(mode=0o700)
        payload = base64.urlsafe_b64encode(json.dumps({'exp': int(time.time()) + 86400}).encode()).decode().rstrip('=')
        token = 'offline.' + payload + '.schema-only'
        identities = {}
        for name, value in (('viewer.json', {f'user-{i:03d}': token for i in range(principals)}),
                            ('telemetry.json', {})):
            path = private / name; path.write_text(json.dumps(value)); path.chmod(0o600); identities[name.split('.')[0]] = path
        ca_files = {}
        for name in ('https', 'mqtt'):
            path = private / f'{name}.pem'; path.write_text('schema-only CA placeholder\n'); path.chmod(0o600); ca_files[name] = path
        gateway_files = {}
        for target in targets:
            gid = target['gateway_id']; path = private / f'{gid}.json'
            path.write_text(json.dumps({'username': gid, 'password': 'schema-only'})); path.chmod(0o600); gateway_files[gid] = path
        subscriptions = [{'viewer_id': row['viewer_id'], 'user_id': row['user_id'], 'gateway_id': f['gateway_id'],
                          **({'sensor_id': f['sensor_id']} if f.get('sensor_id') else {})}
                         for row in mapping['viewers'] for f in row['filters']]
        common = {'schema_version': 1, 'arm': 'e1-offline', 'profile': 'official', 'mode': 'remote',
            'run_id': 'e1-offline-contract', 'seed': 86, 'samples_per_message': 100,
            'topology': {'gateways': 2, 'sensors_per_gateway': 2, 'users': principals, 'viewers': viewers},
            'targets': targets, 'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0},
            'tls': {'https_endpoint': 'https://example.invalid', 'https_ca_file': str(ca_files['https']),
                    'mqtt_tls_endpoint': 'mqtts://example.invalid:8883', 'mqtt_tls_ca_file': str(ca_files['mqtt']),
                    'mqtt_tls_server_name': 'example.invalid'}, 'retry_interval_ms': 200, 'publish_timeout_ms': 3000}
        viewer, telemetry = e1.e1_role_manifests(common, subscriptions=subscriptions,
            viewer_identity=identities['viewer'], telemetry_identity=identities['telemetry'],
            gateway_refs=gateway_files, root=root, mapping=mapping,
            messages_per_target=mapping['messages_per_target'])
        for data in (viewer, telemetry):
            data['tls']['https_ca_file'] = data['tls']['mqtt_tls_ca_file'] = 'certs/https-ca.pem'
        paths = {}
        for role, value in (('viewer', viewer), ('telemetry', telemetry)):
            path = root / f'{role}.json'; path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)); path.chmod(0o600); paths[role] = path
            bundle._strict_manifest(value, 'E1' if role == 'viewer' else 'E2', 'e1-offline')
        plan = e1.build_e1_execution_plan(output_dir=root / 'plan', viewer_manifest=paths['viewer'],
            telemetry_manifest=paths['telemetry'], seed=86, mode='remote', official_opt_in=True)
        spec = json.loads(Path(plan['run_spec']).read_text())
        manifest_refs = {role: spec['paperload'][i]['manifest'] for i, role in enumerate(('viewer', 'telemetry'))}
        role_metadata = {'roles': {'viewer': {'role': 'viewer', 'principals': sorted(json.loads(identities['viewer'].read_text()))},
                                   'telemetry': {'role': 'telemetry', 'principals': []}},
                         'gateways': {gid: {'role': 'gateway', 'gateway_id': gid, 'username': gid} for gid in gateway_files}}
        workload = {'experiment': 'E1', 'arm': 'e1-offline', 'phase': 'offline-preflight', 'profile': 'official',
            'workload': 'e1-wss-mqtt', 'seed': 86, 'samples_per_message': 100,
            'unique_batches': sum(mapping['messages_per_target'].values()), 'rate_per_second': 5,
            'measure_seconds': 20, 'viewer_count': viewers, 'principal_count': principals, 'density': density,
            'expected_deliveries_by_target': mapping['expected_deliveries_by_target'], 'generator_colocated': 'unverified'}
        exported = bundle.export_bundle(output_leaf=root / 'bundle', session_id='e1-offline-session',
            experiment='E1', arm='e1-offline', phase='offline-preflight', manifests=manifest_refs,
            identity_files={'viewer': identities['viewer'], 'telemetry': identities['telemetry']},
            gateway_identity_files=gateway_files, gateway_env_files={}, ca_files=ca_files,
            role_metadata=role_metadata, wait_seconds=0, workload_seconds=50,
            topology={'gateways': 2, 'sensors': 4, 'sensors_per_gateway': 2, 'users': principals, 'viewers': viewers},
            workload=workload, run_spec=plan['run_spec'])
        exported_root = exported['bundle_root']
        inspected = bundle.inspect_bundle(exported_root)
        bound = bundle.bind_bundle(bundle_root=exported_root, workspace_leaf=root / 'workspace')
        metadata = json.loads((Path(exported_root) / 'bundle.json').read_text())
        if inspected.get('session', {}).get('experiment') != 'E1' or metadata.get('workload', {}).get('viewer_count') != viewers:
            raise ValueError('e1_offline_result_projection_rejected')
        run_spec = json.loads(Path(bound['workspace'], 'runtime/execution/run-spec.json').read_text())
        binary = Path(paperload_binary) if paperload_binary else root / 'paperload'
        if paperload_binary is None:
            build = subprocess.run(['go', 'build', '-o', str(binary), './cmd/paperload'],
                cwd=ROOT / 'src', text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                timeout=180, check=False)
            if build.returncode:
                raise ValueError('e1_offline_paperload_build_failed')
            binary.chmod(0o700)
        elif not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError('e1_offline_paperload_binary_rejected')
        preflight_checks = []
        for job in run_spec['paperload']:
            check = subprocess.run([str(binary), '--manifest', job['manifest'], '--output-dir', job['output_dir'],
                '--preflight', '--official-opt-in'], capture_output=True, text=True, timeout=30, check=False)
            if check.returncode:
                raise ValueError(f'e1_offline_paperload_preflight_failed:{job["role"]}:{check.stderr[:300]}')
            preflight_checks.append(check)
        # Safe result projection/schema check is a projection of the actual bundle metadata,
        # not a fabricated completed workload or native acceptance record.
        projected = {'experiment': metadata['session']['experiment'],
            'profile': metadata['session']['profile'], 'viewer_count': metadata['workload']['viewer_count'],
            'principal_count': metadata['workload']['principal_count'],
            'expected_deliveries_by_target': metadata['workload']['expected_deliveries_by_target'],
            'status': 'PARTIAL', 'native_completed_acceptance': False}
        manifest_limit = bundle.MAX_MANIFEST_BYTES
        projection_bytes = len(json.dumps(projected, sort_keys=True, separators=(',', ':')).encode())
        if (any(path.stat().st_size > manifest_limit for path in paths.values())
                or inspected['total_bytes'] > bundle.MAX_TOTAL_BYTES
                or projection_bytes > bundle.MAX_FILE_BYTES):
            raise ValueError('e1_offline_serialized_size_limit_exceeded')
        if (set(projected) != {'experiment', 'profile', 'viewer_count', 'principal_count',
                'expected_deliveries_by_target', 'status', 'native_completed_acceptance'}
                or projected['viewer_count'] != viewers or projected['principal_count'] != principals):
            raise ValueError('e1_offline_result_projection_rejected')
        overlay = e1.e1_resource_overlay(viewers, official_opt_in=True)
        return {'status': 'VALIDATED_PLAN_ONLY', 'native_executed': False, 'physical_ab': 'NOT RUN',
            'identity_fixture': 'unsigned_schema_only_never_used_on_network',
            'mapping': {'counts': mapping['counts'], 'expected_deliveries_by_target': mapping['expected_deliveries_by_target']},
            'lifecycle': {'ramp_seconds': viewer['traffic']['ramp_seconds'],
                'warmup_seconds': viewer['phases']['warmup_seconds'],
                'measure_seconds': viewer['phases']['measure_seconds'],
                'drain_seconds': viewer['phases']['drain_seconds'],
                'cooldown_seconds': viewer['phases']['cooldown_seconds'],
                'wall_seconds': viewer['limits']['wall_seconds'],
                'viewer_ready_timeout_seconds': run_spec['paperload'][0]['ready_timeout_seconds'],
                'required_token_lifetime_seconds': json.loads(
                    Path(bound['workspace'], 'runtime/binding.json').read_text())['required_token_lifetime_seconds']},
            'receive_limits': {'max_messages': viewer['viewer']['max_messages'],
                'max_bytes': viewer['viewer']['max_bytes']},
            'manifest_correlation': {role: {'run_id': data['run_id'],
                'profile': data['profile'], 'targets': data['targets']}
                for role, data in (('viewer', viewer), ('telemetry', telemetry))},
            'bundle_correlation': {'session_id': inspected['session']['session_id'],
                'profile': inspected['session']['profile'], 'run_ids': inspected['session']['run_ids']},
            'resource_requirements': {'selected_overlay': overlay,
                'effective_a_limits': 'not_read_in_offline_dry_run'},
            'bundle_reader_caps': {'manifest_bytes': bundle.MAX_MANIFEST_BYTES,
                'file_bytes': bundle.MAX_FILE_BYTES, 'inventory_bytes': bundle.MAX_TOTAL_BYTES,
                'inventory_files': bundle.MAX_FILES},
            'serialized_sizes': {'mapping_bytes': len(json.dumps(mapping, separators=(',', ':')).encode()),
                'viewer_manifest_bytes': paths['viewer'].stat().st_size,
                'telemetry_manifest_bytes': paths['telemetry'].stat().st_size,
                'bundle_bytes': inspected['total_bytes'],
                'result_projection_bytes': projection_bytes},
            'serialized_limits': {'manifest_bytes': bundle.MAX_MANIFEST_BYTES,
                'file_bytes': bundle.MAX_FILE_BYTES, 'bundle_bytes': bundle.MAX_TOTAL_BYTES},
            'bundle_roundtrip': {'export': True, 'inspect': True, 'bind': bool(bound),
                'paperload_preflight': True},
            'paperload_preflight_outcomes': [{'role': job['role'], 'exit_code': check.returncode,
                'status': json.loads(check.stdout).get('status')}
                for job, check in zip(run_spec['paperload'], preflight_checks)],
            'result_schema_checked': True, 'result_projection_status': projected['status'],
            'native_completed_acceptance': projected['native_completed_acceptance']}


def build_e4_offline_contract(*, unique_count, replay_count, deadline_seconds,
                              official_opt_in, expiry_replay):
    """Export, bind and Go-preflight E4 manifests without opening a network path."""
    import paper_p8_e4_support as e4
    import paper_p8_bundle as bundle

    scratch = Path('/var/tmp/opencode/p8-final-20261105/e4-offline')
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    if scratch.is_symlink() or scratch.stat().st_uid != os.getuid() or scratch.stat().st_mode & 0o077:
        raise ValueError('e4_offline_scratch_rejected')
    with tempfile.TemporaryDirectory(prefix='official-', dir=scratch) as td:
        root = Path(td); root.chmod(0o700)
        private = root / 'private'; private.mkdir(mode=0o700)
        # These deliberately unsigned fixtures satisfy bundle schema only; they
        # are not credentials and this path never performs a network request.
        exp = int(time.time()) + 86400
        jwt_payload = base64.urlsafe_b64encode(json.dumps({'exp': exp}).encode()).decode().rstrip('=')
        token = 'offline.' + jwt_payload + '.schema-only'
        telemetry_identity = private / 'telemetry.json'
        telemetry_identity.write_text(json.dumps({'schema-telemetry': token}))
        telemetry_identity.chmod(0o600)
        targets = [{'gateway_id': 'gw-a', 'sensor_id': 'sensor-a'},
                   {'gateway_id': 'gw-b', 'sensor_id': 'sensor-b'}]
        gateway_files = {}
        for target in targets:
            gid = target['gateway_id']; path = private / f'{gid}.json'
            path.write_text(json.dumps({'username': gid, 'password': 'schema-only'}))
            path.chmod(0o600); gateway_files[gid] = path
        ca_files = {}
        for name in ('https', 'mqtt'):
            path = private / f'{name}-ca.pem'
            path.write_text('schema-only public CA fixture\n'); path.chmod(0o600); ca_files[name] = path

        common = {'schema_version': 1, 'experiment': 'E4', 'arm': 'warm',
            'profile': 'official', 'mode': 'remote', 'run_id': 'e4-official-offline',
            'seed': 89, 'roles': ['telemetry'], 'samples_per_message': 100,
            'topology': {'gateways': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            'targets': targets, 'subscriptions': [],
            'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0},
            'phases': {'warmup_seconds': 0, 'measure_seconds': 4, 'drain_seconds': 8,
                       'cooldown_seconds': 0},
            'limits': {'retries': 4, 'workers': 2, 'connections': 2,
                'pending_messages': 64, 'pending_bytes': 1 << 20,
                'disk_growth_bytes': e4.e4_journal_byte_bound(unique_count), 'wall_seconds': 720},
            'tls': {'https_endpoint': 'https://example.invalid',
                'mqtt_tls_endpoint': 'mqtts://example.invalid:8883',
                'https_ca_file': str(ca_files['https']), 'mqtt_tls_ca_file': str(ca_files['mqtt']),
                'mqtt_tls_server_name': 'example.invalid'},
            'identity_files': {'telemetry': str(telemetry_identity)},
            'gateway_identity_files': {key: str(value) for key, value in gateway_files.items()},
            'bundle_path': 'outputs/e4-offline', 'durability_mode': 'synthetic-load',
            'retry_interval_ms': 500, 'publish_timeout_ms': 3000}
        phases = ['unique', 'replay'] + (['expiry-replay'] if expiry_replay else [])
        schedule = e4.e4_phase_durations(unique_count, replay_count, base_rate=5)
        if schedule['workload_seconds'] > deadline_seconds:
            raise ValueError('e4_offline_session_budget_insufficient')
        common['limits']['wall_seconds'] = schedule['unique_seconds'] + schedule['drain_seconds'] + 30
        manifest_paths = {}
        for phase in phases:
            manifest = e4.e4_phase_manifest(common, 'outputs/e4-journal.sqlite', phase,
                unique_count=unique_count, replay_count=replay_count,
                official_opt_in=official_opt_in)
            path = root / f'{phase}.json'
            path.write_text(json.dumps(manifest, sort_keys=True, separators=(',', ':')))
            path.chmod(0o600); manifest_paths[phase] = path

        workload_seconds = 720 if expiry_replay else schedule['workload_seconds']
        workload = {'experiment': 'E4', 'arm': 'warm', 'run_id': 'e4-official-offline',
            'cache_mode': 'on', 'phase': 'e4-official-offline', 'profile': 'official',
            'seed': 89, 'generator_colocated': 'unverified',
            'expiry_replay_enabled': bool(expiry_replay)}
        if expiry_replay:
            workload['expiry_required_lifetime_seconds'] = 1164
        jobs = [{'manifest': str(path), 'output_dir': f'outputs/{phase}',
                 'role': 'telemetry', 'ready_timeout_seconds': 120}
                for phase, path in manifest_paths.items()]
        run_spec = root / 'run-spec.json'
        run_spec.write_text(json.dumps({'paperload': jobs, 'gateways': [],
            'stop_timeout_seconds': 30}, sort_keys=True, separators=(',', ':')))
        run_spec.chmod(0o600)
        role_metadata = {'roles': {
            **{f'{phase}:telemetry': {'role': 'telemetry',
                'principals': ['schema-telemetry']} for phase in phases}},
            'gateways': {gid: {'role': 'gateway', 'gateway_id': gid, 'username': gid}
                         for gid in gateway_files}}
        identities = {f'{phase}:telemetry': telemetry_identity for phase in phases}
        exported = bundle.export_bundle(output_leaf=root / 'bundle',
            session_id='e4-offline-session', experiment='E4', arm='warm',
            phase='e4-official-offline', manifests=manifest_paths,
            identity_files=identities, gateway_identity_files=gateway_files,
            gateway_env_files={}, ca_files=ca_files, role_metadata=role_metadata,
            wait_seconds=0, workload_seconds=workload_seconds,
            topology={'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1,
                      'users': 2, 'viewers': 2}, workload=workload, run_spec=str(run_spec))
        inspected = bundle.inspect_bundle(exported['bundle_root'])
        bound = bundle.bind_bundle(bundle_root=exported['bundle_root'], workspace_leaf=root / 'workspace')
        metadata = json.loads((Path(exported['bundle_root']) / 'bundle.json').read_text())
        binding = json.loads((Path(bound['workspace']) / 'runtime/binding.json').read_text())
        inventory_manifests = sorted(Path(row['path']).stem for row in metadata['inputs']
                                     if row.get('kind') == 'manifest')
        if (inventory_manifests != sorted(phases)
                or metadata.get('session', {}).get('session_id') != 'e4-offline-session'
                or metadata.get('session', {}).get('profile') != 'official'
                or binding.get('session', {}).get('session_id') != 'e4-offline-session'):
            raise ValueError('e4_offline_bundle_correlation_rejected')

        binary = scratch / 'paperload'
        if not binary.is_file():
            build = subprocess.run(['go', 'build', '-o', str(binary), './cmd/paperload'],
                cwd=ROOT / 'src', text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, timeout=180, check=False)
            if build.returncode:
                raise ValueError('e4_offline_paperload_build_failed')
            binary.chmod(0o700)
        bound_spec = json.loads((Path(bound['workspace']) / 'runtime/execution/run-spec.json').read_text())
        preflight = []
        for phase, job in zip(phases, bound_spec['paperload']):
            check = subprocess.run([str(binary), '--manifest', job['manifest'],
                '--output-dir', job['output_dir'], '--preflight', '--official-opt-in'],
                cwd=bound['workspace'], capture_output=True, text=True, timeout=30, check=False)
            preflight.append({'phase': phase, 'exit_code': check.returncode,
                              'stdout': check.stdout.strip(), 'stderr': check.stderr.strip()})
            if check.returncode:
                raise ValueError(f'e4_offline_paperload_preflight_failed:{phase}:{check.stdout.strip()}:{check.stderr.strip()}')

        # Independently derive the ledger/raw/ACK expectations from selected inputs.
        samples_per_message = 100
        expected = {'unique_batches': unique_count,
            'unique_raw_samples': unique_count * samples_per_message,
            'unique_application_acks': unique_count,
            'replay_batches': replay_count,
            'replay_payload_raw_samples': replay_count * samples_per_message,
            'replay_new_raw_samples': 0,
            'replay_application_acks': replay_count, 'replay_new_deliveries': 0}
        import paper_p8_client as client
        projected = client._safe_workload({'status': 'PARTIAL', 'experiment': 'E4',
            'arm': 'warm', 'profile': 'official', 'mode': 'remote',
            'run_id': 'e4-official-offline-unique', 'telemetry': {
                'generated_unique': unique_count, 'offered_unique': unique_count,
                'application_acks': unique_count, 'pending_messages': 0,
                'pending_bytes': 0}, 'e4': {'mode': 'unique',
                'journal_entry_count': unique_count}}, role='telemetry')
        if (projected.get('status') != 'PARTIAL'
                or projected.get('telemetry', {}).get('generated_unique') != unique_count
                or projected.get('e4', {}).get('journal_entry_count') != unique_count):
            raise ValueError('e4_offline_result_projection_rejected')
        return {'status': 'VALIDATED_PLAN_ONLY', 'native_executed': False,
            'physical_ab': 'NOT RUN',
            'identity_fixture': 'unsigned_schema_only_never_used_on_network',
            'bundle_roundtrip': {'export': True, 'inspect': True, 'bind': True},
            'manifest_inventory': inventory_manifests, 'bound_profile': metadata['session']['profile'],
            'bound_session_id': binding['session']['session_id'], 'expected_counts': expected,
            'result_projection': {'schema_valid': True, 'status': projected['status'],
                'native_completed_acceptance': False},
             'preflight': preflight, 'bundle_bytes': inspected['total_bytes'],
             'manifest_count': len(phases), 'required_token_lifetime_seconds':
                binding['required_token_lifetime_seconds'], 'schedule': schedule,
             'disk_growth_bytes': json.loads(manifest_paths['unique'].read_text())['limits']['disk_growth_bytes'],
             'journal_max_bytes': json.loads(manifest_paths['unique'].read_text())['e4']['max_bytes']}


def build_e3_offline_contract(*, workload, dispatch_mode, command_count, schedule, deadline_seconds):
    """Validate the selected E3 command stream and its portable input schemas offline."""
    import paper_p8_e3_support as e3
    import paper_p8_bundle as bundle
    scratch = Path('/var/tmp/opencode/p8-final-20261105/e3')
    scratch.mkdir(mode=0o700, parents=True, exist_ok=True)
    if scratch.is_symlink() or scratch.stat().st_uid != os.getuid() or scratch.stat().st_mode & 0o077:
        raise ValueError('e3_offline_scratch_rejected')
    with tempfile.TemporaryDirectory(prefix='official30-', dir=scratch) as td:
        root = Path(td); root.chmod(0o700)
        private = root / 'private'; private.mkdir(mode=0o700)
        exp = int(time.time()) + 86400
        payload = base64.urlsafe_b64encode(json.dumps({'exp': exp}).encode()).decode().rstrip('=')
        token = 'offline.' + payload + '.schema-only'
        files = {}
        def write(name, data):
            path = private / name
            path.write_text(json.dumps(data) if isinstance(data, (dict, list)) else data)
            path.chmod(0o600); files[name] = path; return path
        bg_targets = [{'gateway_id':'gw-a','sensor_id':'bg-a'}, {'gateway_id':'gw-b','sensor_id':'bg-b'}]
        control_targets = [{'gateway_id':'gw-a','sensor_id':'ctl-a'}, {'gateway_id':'gw-a','sensor_id':'ctl-b'}]
        gateway_identity = {gid: write(f'{gid}.json', {'username':gid, 'password':'schema-only'})
                            for gid in ('gw-a','gw-b')}
        gateway_envs = []
        for index, target in enumerate(control_targets):
            gateway_envs.append(write(f'executor-{index}.json', {
                'GATEWAY_ID':target['gateway_id'], 'SENSOR_ID':target['sensor_id'],
                'GATEWAY_CONTROL_ENABLED':'true', 'GATEWAY_MQTT_BROKER_URL':'mqtts://example.invalid:8883',
                'GATEWAY_MQTT_CLIENT_ID':f'client-{index}', 'GATEWAY_MQTT_USERNAME':target['gateway_id'],
                'GATEWAY_MQTT_PASSWORD':'schema-only',
                'GATEWAY_MQTT_CA_FILE':'certs/mqtt-ca.pem', 'GATEWAY_OUTBOX_PATH':f'outputs/gw-{index}.sqlite'}))
        ca = {name: write(f'{name}-ca.pem', 'schema-only CA placeholder\n') for name in ('https','mqtt')}
        identity = {arm: write(f'{arm}.json', {}) for arm in ('A','B')}
        control_id = write('control.json', {'e3-operator':token})
        manifests = e3.e3_pilot_manifests(common={'run_id':'e3-official30-offline', 'mode':'remote',
            'bundle_path':'outputs/e3-offline', 'publish_timeout_ms':3000}, background_targets=bg_targets,
            background_gateway_identity_files={k:str(v) for k,v in gateway_identity.items()},
            telemetry_identity_files={k:str(v) for k,v in identity.items()}, control_targets=control_targets,
            initial_versions={'ctl-a':'0','ctl-b':'0'}, control_identity_file=str(control_id),
            tls={'https_endpoint':'https://example.invalid','https_ca_file':str(ca['https']),
                 'mqtt_tls_endpoint':'mqtts://example.invalid:8883','mqtt_tls_ca_file':str(ca['mqtt'])},
            workload=workload, command_count=command_count, schedule=schedule, seed=89)
        # The official label is valid only in this offline schema contract; native E3 stays pilot-only.
        for manifest in manifests['background'].values():
            bundle._strict_manifest(manifest, manifest['experiment'], manifest['arm'])
        manifests['control']['profile'] = 'official'
        for manifest in (manifests['control'],):
            bundle._strict_manifest(manifest, manifest['experiment'], manifest['arm'])
        plan = e3.build_e3_execution_plan(output_dir=root/'plan',
            manifests={'A':manifests['background']['A'], 'B':manifests['background']['B'],
                       'control':manifests['control']}, gateway_env_files=[str(p) for p in gateway_envs],
            shutdown_timeouts_seconds=[35,45])
        spec = json.loads(Path(plan['run_spec']).read_text())
        refs = {name: str((root/'plan'/f'{name}.manifest.json').resolve())
                for name in ('A','B','control')}
        role_metadata = {'roles': {'A:telemetry': {'role':'telemetry','principals':[]},
            'B:telemetry': {'role':'telemetry','principals':[]},
            'control:control': {'role':'control','principals':['e3-operator']}},
            'gateways': {gid: {'role':'gateway','gateway_id':gid,'username':gid} for gid in gateway_identity}}
        exported = bundle.export_bundle(output_leaf=root/'bundle', session_id='e3-official30-offline',
            experiment='E3', arm=dispatch_mode, phase=f'e3-{workload}-{dispatch_mode}', manifests=refs,
            identity_files={'A:telemetry':identity['A'], 'B:telemetry':identity['B'],
                            'control:control':control_id},
            gateway_identity_files=gateway_identity,
            gateway_env_files={'executor-1':gateway_envs[0], 'executor-2':gateway_envs[1]},
            ca_files=ca, role_metadata=role_metadata, wait_seconds=0, workload_seconds=62,
            topology={'gateways':2,'sensors':4,'sensors_per_gateway':2,'users':0,'viewers':0},
            workload={'experiment':'E3','arm':dispatch_mode,'phase':f'e3-{workload}-{dispatch_mode}',
                'profile':'official','workload':workload,'seed':89,'dispatch_mode':dispatch_mode,
                'generator_colocated':'unverified'}, run_spec=plan['run_spec'])
        inspected = bundle.inspect_bundle(exported['bundle_root'])
        bound = bundle.bind_bundle(bundle_root=exported['bundle_root'], workspace_leaf=root/'workspace')
        if inspected.get('session', {}).get('profile') != 'official' or not bound:
            raise ValueError('e3_offline_bundle_roundtrip_rejected')
        binary = root / 'paperload'
        built = subprocess.run(['go', 'build', '-o', str(binary), './cmd/paperload'],
            cwd=ROOT/'src', capture_output=True, text=True, timeout=180, check=False)
        if built.returncode:
            raise ValueError('e3_offline_paperload_build_failed')
        binary.chmod(0o700)
        bound_spec_path = Path(bound['workspace']) / 'runtime/execution/run-spec.json'
        bound_spec = json.loads(bound_spec_path.read_text())
        binding = json.loads((Path(bound['workspace']) / 'runtime/binding.json').read_text())
        preflight = []
        for job in bound_spec['paperload']:
            check = subprocess.run([str(binary), '--manifest', job['manifest'], '--output-dir', job['output_dir'],
                '--preflight', '--official-opt-in'], cwd=bound['workspace'], capture_output=True,
                text=True, timeout=30, check=False)
            if check.returncode:
                raise ValueError(f'e3_offline_paperload_preflight_failed:{job["role"]}:{check.stdout.strip()}:{check.stderr.strip()}')
            try:
                output = json.loads(check.stdout)
            except json.JSONDecodeError as exc:
                raise ValueError(f'e3_offline_paperload_preflight_output_invalid:{job["role"]}') from exc
            preflight.append({'role': job['role'], 'exit_code': check.returncode, 'stdout': output})
        import paper_p8_client as client
        control = manifests['control']
        snapshot = {'status':'PARTIAL', 'planned':len(control['commands']), 'sent':0,
            'skipped':len(control['commands']), 'pending':0,
            'commands':[{'classification':'not_sent', 'idempotency_key':row['idempotency_key'],
                'sensor_id':row['sensor_id'], 'gateway_id':row['gateway_id'],
                'expected_desired_version':row['expected_desired_version'],
                'frequency_hz':row['frequency_hz']} for row in control['commands']]}
        safe_result_projection = client._safe_workload(snapshot, role='control')
        if len(safe_result_projection.get('commands', [])) != command_count:
            raise ValueError('e3_offline_result_projection_rejected')
        return {'status':'VALIDATED_PLAN_ONLY', 'native_executed':False, 'physical_ab':'NOT RUN',
            'schema_only_identities':True, 'profile':'official', 'workload':workload,
            'dispatch_mode':dispatch_mode, 'command_count':len(manifests['control']['commands']),
            'schedule':schedule, 'expected_versions_after':manifests['expected_versions_after'],
            'unique_idempotency_keys':len(set(manifests['idempotency_keys'])),
            'manifest_profiles':{'background':sorted({m['profile'] for m in manifests['background'].values()}),
                'control':manifests['control']['profile']},
            'command_manifest':manifests['control'], 'run_spec':spec,
            'deadline_seconds':deadline_seconds,
            'required_token_lifetime_seconds':binding['required_token_lifetime_seconds'],
            'budgets':{'control_pending_messages':manifests['control']['limits']['pending_messages'],
                'control_pending_bytes':manifests['control']['limits']['pending_bytes'],
                'control_disk_growth_bytes':manifests['control']['limits']['disk_growth_bytes'],
                'background':{arm:{'wall_seconds':value['limits']['wall_seconds'],
                    'pending_messages':value['limits']['pending_messages'],
                    'pending_bytes':value['limits']['pending_bytes'],
                    'disk_growth_bytes':value['limits']['disk_growth_bytes']}
                    for arm,value in manifests['background'].items()},
                'bound_disk_guard':bound_spec['disk_guard']},
            'bundle_schema_checked':True, 'bundle_roundtrip':{'export':True,'inspect':True,'bind':True},
            'paperload_preflight':preflight,
            'result_projection':{'experiment':'E3','profile':'official','command_count':30,
                'expected_versions_after':manifests['expected_versions_after']},
            'result_schema_checked':True, 'result_snapshot_status':'PARTIAL schema-only; no execution claimed',
            'execution_supported':False}


def _private_root(path, create=False):
    path = Path(path).absolute()
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise ValueError('server_path_symlink_rejected')
    if create:
        path.mkdir(mode=0o700, parents=True, exist_ok=False)
    stat = path.lstat()
    if not path.is_dir() or stat.st_uid != os.getuid() or stat.st_mode & 0o077:
        raise ValueError('server_root_rejected')
    return path


def _validate_private_new_file(path):
    path = Path(path).absolute()
    if path.is_symlink():
        raise ValueError('server_path_symlink_rejected')
    for parent in path.parents:
        if parent.is_symlink():
            raise ValueError('server_path_symlink_rejected')
    parent = path.parent
    if not parent.is_dir():
        raise ValueError('server_parent_dir_rejected')
    parent_stat = parent.lstat()
    if parent_stat.st_uid != os.getuid() or parent_stat.st_mode & 0o077:
        raise ValueError('server_parent_permissions_rejected')
    if path.exists():
        raise ValueError('server_output_file_exists')
    return path


def _validate_private_readable_file(path, code='server_file_rejected', max_bytes=65536):
    path = Path(path).absolute()
    if path.is_symlink():
        raise ValueError(f'{code}_symlink')
    for parent in path.parents:
        if parent.is_symlink():
            raise ValueError(f'{code}_symlink')
    if not path.is_file():
        raise ValueError(f'{code}_missing')
    st = path.lstat()
    if st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise ValueError(f'{code}_permissions')
    if st.st_size > max_bytes:
        raise ValueError(f'{code}_size')
    return path


def _state_root(root):
    root = _private_root(root)
    fixture = root / 'fixture'
    private = fixture / 'private'
    return private


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    serve = sub.add_parser('serve', help='start an owned P8 fixture and hold until operator finalizes')
    serve.add_argument('--root', type=Path, required=True)
    serve.add_argument('--network-config', type=Path, required=True)
    serve.add_argument('--deadline-seconds', type=int, default=900)
    serve.add_argument('--profile', choices=('pilot', 'official'), default='pilot',
                       help='bounded workload profile selector')
    serve.add_argument('--official-opt-in', action='store_true',
                       help='explicitly authorize preparation of an official profile')
    serve.add_argument('--dry-run', action='store_true',
                       help='validate and print the selected plan without fixture or network access')
    serve.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    serve.add_argument('--arm', choices=('optimized', 'shaper_off', 'microbatch_off', 'ram_dedup_off', 'polling', 'fanout_scan'))
    serve.add_argument('--context', type=Path)
    serve.add_argument('--reference', type=Path)
    serve.add_argument('--samples-per-message', choices=('100', '1'))
    serve.add_argument('--e2-offered-rate', type=int, choices=(100, 1000, 5000, 20000), default=100)
    serve.add_argument('--e2-duration-seconds', type=int, default=10)
    serve.add_argument('--e2-burst-messages', type=int, default=0)
    serve.add_argument('--dispatch-mode', choices=('polling', 'wakeup'))
    serve.add_argument('--workload', choices=('low_load', 'mixed', 'steady_burst', 'equal_samples'))
    serve.add_argument('--e3-command-count', type=int, choices=range(1, 31), default=4)
    serve.add_argument('--e3-schedule', choices=('smoke', 'steady'), default='smoke')
    serve.add_argument('--e1-viewers', type=int, default=64)
    serve.add_argument('--e1-principals', type=int, default=2)
    serve.add_argument('--e1-density', choices=('mixed', 'sparse', 'dense', 'overlap'), default='mixed')
    serve.add_argument('--e1-messages-per-target', type=int, default=5)
    serve.add_argument('--cache-mode', choices=('on', 'off'), default='on')
    serve.add_argument('--cold-replay', action='store_true',
                       help='include the optional cache-on fresh-backend E4 replay phase')
    serve.add_argument('--expiry-replay', action='store_true',
                       help='include optional same-process E4 replay after production TTL (600s)')
    serve.add_argument('--durable-reopen', action='store_true',
                       help='include the optional sequential durable Gateway SQLite fault/reopen phases')
    serve.add_argument('--conflicts', action='store_true',
                       help='include the optional E4 representative conflict rejection phase')
    serve.add_argument('--e4-unique-count', type=int, default=20)
    serve.add_argument('--e4-replay-count', type=int, default=10)
    e2_ctx = sub.add_parser('e2-context', help='construct private equal_samples context JSON')
    e2_ctx.add_argument('--output', type=Path, required=True)
    e2_ctx.add_argument('--total-samples', type=int, default=6000)
    e2_ctx.add_argument('--sample-rate', type=int, default=100)
    e2_ctx.add_argument('--seed', type=int, default=84)
    cmp_e2 = sub.add_parser('compare-e2', help='compare two E2 equal_samples reports')
    cmp_e2.add_argument('reports', nargs='*', type=Path, help='two E2 equal_samples report JSON paths')
    cmp_e2.add_argument('--report-1', '--report1', '-1', type=Path, dest='report_1', default=None)
    cmp_e2.add_argument('--report-2', '--report2', '-2', type=Path, dest='report_2', default=None)
    cmp_e2.add_argument('--output', '-o', type=Path, required=True)
    status = sub.add_parser('status')
    status.add_argument('--root', type=Path, required=True)
    status.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    for name in ('begin', 'finish-phase', 'next-phase', 'finalize', 'abort'):
        command = sub.add_parser(name)
        command.add_argument('--root', type=Path, required=True)
        command.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
        command.add_argument('--session-id', required=True)
        command.add_argument('--phase', required=True)
        if name in ('begin', 'finish-phase', 'next-phase'):
            command.add_argument('--phase-id', required=True)
        if name == 'finish-phase':
            command.add_argument('--results', type=Path, required=True)
    cleanup = sub.add_parser('cleanup', help='receipt-verified stale fixture cleanup')
    cleanup.add_argument('--root', type=Path, required=True)
    cleanup.add_argument('--project', required=True)
    cleanup.add_argument('--experiment', choices=('E1', 'E2', 'E3', 'E4', 'E5'), default='E1')
    args = parser.parse_args(argv)
    if args.command == 'e2-context':
        output = _validate_private_new_file(args.output)
        import paper_p8_e2_support as e2_support
        ctx = e2_support.build_e2_equal_samples_context(
            total_samples=args.total_samples,
            sample_rate=args.sample_rate,
            seed=args.seed,
        )
        raw = json.dumps(ctx, sort_keys=True, indent=2).encode('utf-8') + b'\n'
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        print(json.dumps({'status': 'CREATED', 'output': str(output), 'context_sha256': ctx['context_sha256']}))
        return 0
    if args.command == 'compare-e2':
        output = _validate_private_new_file(args.output)
        if args.report_1 and args.report_2:
            p1, p2 = args.report_1, args.report_2
        elif len(args.reports) == 2:
            p1, p2 = args.reports[0], args.reports[1]
        else:
            raise ValueError('compare-e2 requires exactly two report files')

        p1 = _validate_private_readable_file(p1, 'compare_report_rejected', max_bytes=8 << 20)
        p2 = _validate_private_readable_file(p2, 'compare_report_rejected', max_bytes=8 << 20)

        if p1.resolve() == p2.resolve():
            raise ValueError('compare-e2 requires two distinct report files')

        try:
            r1 = json.loads(p1.read_text(encoding='utf-8'))
        except Exception as exc:
            raise ValueError('compare_report_invalid_json') from exc

        try:
            r2 = json.loads(p2.read_text(encoding='utf-8'))
        except Exception as exc:
            raise ValueError('compare_report_invalid_json') from exc

        import paper_p8_e2_support as e2_support
        comparison = e2_support.compare_e2_reports(r1, r2)

        raw = json.dumps(comparison, sort_keys=True, indent=2).encode('utf-8') + b'\n'
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

        summary = {
            'status': comparison['status'],
            'comparable': comparison['comparable'],
            'clean_comparison': comparison['workload_contamination']['clean_comparison'],
            'output': str(output),
        }
        print(json.dumps(summary))
        return 0
    if getattr(args, 'experiment', 'E1') not in ('E1', 'E2', 'E3', 'E4', 'E5'):
        parser.error('unsupported experiment')
    if args.command == 'serve':
        if not 60 <= args.deadline_seconds <= 86400:
            raise ValueError('P8 session deadline must be 60..86400 seconds')
        if args.profile == 'official' and not args.official_opt_in:
            raise ValueError('official_profile_opt_in_required')
        if args.experiment in ('E1', 'E4', 'E5') and args.workload is not None:
            raise ValueError(f'{args.experiment} does not accept --workload')
        if args.arm is not None and args.experiment != 'E5':
            raise ValueError('E5 options require --experiment E5')
        if args.experiment not in ('E2', 'E5') and (args.context is not None or args.reference is not None):
            raise ValueError('E5 options require --experiment E5')
        if args.experiment == 'E5':
            if args.arm is None:
                raise ValueError('E5 requires explicit --arm')
            if args.context is None:
                raise ValueError('E5 requires --context')
            _validate_private_readable_file(args.context, 'server_context_rejected', max_bytes=65536)
            try:
                e5_ctx_raw = json.loads(Path(args.context).read_text(encoding='utf-8'))
                if isinstance(e5_ctx_raw, dict) and (e5_ctx_raw.get('workload_type') == 'equal_samples' or e5_ctx_raw.get('experiment') == 'E2'):
                    raise ValueError('E5 does not accept E2 context')
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError('server_context_rejected') from exc
            if args.arm == 'optimized' and args.reference is not None:
                raise ValueError('E5 optimized arm does not accept --reference')
            if args.arm != 'optimized' and args.reference is None:
                raise ValueError('E5 ablation arms require --reference')
            if args.reference is not None:
                _validate_private_readable_file(args.reference, 'server_reference_rejected', max_bytes=8 << 20)
                try:
                    ref_check = json.loads(Path(args.reference).read_text(encoding='utf-8'))
                    if isinstance(ref_check, dict) and ref_check.get('experiment') == 'E2':
                        raise ValueError('E5 does not accept E2 reference')
                except ValueError:
                    raise
                except Exception as exc:
                    raise ValueError('server_reference_rejected') from exc
            if (args.samples_per_message is not None or args.dispatch_mode is not None
                    or args.cache_mode != 'on'
                     or args.cold_replay or args.expiry_replay or args.durable_reopen or args.conflicts):
                raise ValueError('E1-E4 options are rejected for E5')
        e2_seed = None
        if args.experiment == 'E2':
            if args.samples_per_message is None:
                raise ValueError('E2 requires one explicit --samples-per-message profile')
            workload = args.workload or 'steady_burst'
            if workload not in ('steady_burst', 'equal_samples'):
                raise ValueError('E2 workload must be steady_burst or equal_samples')
            if args.profile == 'official' and workload != 'steady_burst':
                raise ValueError('official_equal_samples_not_supported')
            if args.profile == 'official':
                p8_support.lifecycle_budget(workload_seconds=args.e2_duration_seconds + 60,
                                            session_deadline_seconds=args.deadline_seconds)
            if workload == 'steady_burst':
                if args.context is not None or args.reference is not None:
                    raise ValueError('E2 steady_burst does not accept --context or --reference')
            elif workload == 'equal_samples':
                if args.context is None:
                    raise ValueError('E2 equal_samples requires --context')
                ctx_path = _validate_private_readable_file(args.context, 'server_context_rejected', max_bytes=65536)
                import paper_p8_e2_support as e2_support
                try:
                    ctx_data = json.loads(ctx_path.read_text(encoding='utf-8'))
                except Exception as exc:
                    raise ValueError('server_context_rejected') from exc
                ctx = e2_support.validate_e2_equal_samples_context(ctx_data, check_freshness=True)
                e2_seed = ctx['seed']
                if args.reference is not None:
                    ref_path = _validate_private_readable_file(args.reference, 'server_reference_rejected', max_bytes=8 << 20)
                    try:
                        ref_data = json.loads(ref_path.read_text(encoding='utf-8'))
                    except Exception as exc:
                        raise ValueError('server_reference_rejected') from exc
                    e2_support.prevalidate_e2_reference(ref_data, ctx)
                duration_seconds = ctx['duration_seconds']
                p8_support.lifecycle_budget(workload_seconds=duration_seconds,
                                            session_deadline_seconds=args.deadline_seconds)
            if (args.arm is not None or args.dispatch_mode is not None
                     or args.cache_mode != 'on' or args.cold_replay or args.expiry_replay
                    or args.durable_reopen or args.conflicts):
                raise ValueError('E2 does not accept unrelated experiment options')
        if args.experiment == 'E3':
            if args.dispatch_mode is None or args.workload is None:
                raise ValueError('E3 requires explicit --dispatch-mode and --workload')
            if args.workload not in ('low_load', 'mixed'):
                raise ValueError('E3 requires explicit --dispatch-mode and --workload')
            if (args.arm is not None or args.context is not None or args.reference is not None
                    or args.samples_per_message is not None or args.cache_mode != 'on'
                     or args.cold_replay or args.expiry_replay or args.durable_reopen or args.conflicts):
                raise ValueError('E3 profile options require --experiment E3')
            if args.profile == 'official' and (args.e3_command_count not in (3, 30)
                    or args.e3_schedule != 'steady'
                    or args.e3_command_count == 3 and args.workload != 'low_load'):
                raise ValueError('E3_official_requires_30_or_low_load_3_steady_commands')
            if args.e3_schedule == 'smoke' and args.e3_command_count != 4:
                raise ValueError('E3 smoke schedule requires four commands')
        elif args.e3_command_count != 4 or args.e3_schedule != 'smoke':
            raise ValueError('E3 command options require --experiment E3')
        if args.experiment == 'E1':
            if (args.e1_viewers < 1 or args.e1_viewers > 10000 or args.e1_principals < 2
                    or args.e1_principals > args.e1_viewers or args.e1_messages_per_target < 1
                    or args.e1_messages_per_target > 10000):
                raise ValueError('E1 workload count rejected')
            if args.profile == 'official' and not args.official_opt_in:
                raise ValueError('official profile requires --official-opt-in')
        elif (args.e1_viewers, args.e1_principals, args.e1_density, args.e1_messages_per_target) != (64, 2, 'mixed', 5):
            raise ValueError('E1 workload options require --experiment E1')
        if args.experiment != 'E4' and args.experiment != 'E5' and args.cache_mode != 'on':
            raise ValueError('--cache-mode requires --experiment E4')
        if args.experiment == 'E4':
            if (args.arm is not None or args.context is not None or args.reference is not None
                    or args.samples_per_message is not None or args.dispatch_mode is not None
                    or args.workload is not None):
                raise ValueError('E4 does not accept unrelated experiment options')
            import paper_p8_e4_support as e4_support
            e4_counts = e4_support.validate_e4_dataset_counts(args.e4_unique_count,
                args.e4_replay_count, profile=args.profile, official_opt_in=args.official_opt_in)
            if args.cold_replay and args.cache_mode != 'on':
                raise ValueError('--cold-replay requires --experiment E4 --cache-mode on')
            if args.expiry_replay and args.cache_mode != 'on':
                raise ValueError('--expiry-replay requires --experiment E4 --cache-mode on')
            if args.expiry_replay and args.cold_replay:
                raise ValueError('--expiry-replay cannot be combined with --cold-replay')
            if args.expiry_replay and (args.durable_reopen or args.conflicts):
                raise ValueError('--expiry-replay currently supports only unique/replay phases')
            if args.expiry_replay:
                # This is a planning assumption only. The signed fixture JWT is
                # checked by bundle export/bind/preflight at its actual exp.
                e4_support.validate_e4_expiry_budget(session_seconds=args.deadline_seconds,
                    token_seconds=368 + 16 + 30 + 30 + 120 + 600, workload_seconds=368,
                    drain_seconds=16, import_seconds=30, cleanup_seconds=30)
        elif (args.e4_unique_count, args.e4_replay_count) != (20, 10):
            raise ValueError('E4 dataset options require --experiment E4')
        elif args.durable_reopen or args.conflicts or args.cold_replay or args.expiry_replay:
            raise ValueError('E4 phase options require --experiment E4')
        if args.dry_run:
            e2_plan_summary = None
            e1_plan_summary = None
            if args.experiment == 'E1' and args.profile == 'official':
                e1_plan_summary = build_e1_offline_contract(viewers=args.e1_viewers,
                    principals=args.e1_principals, density=args.e1_density,
                    messages_per_target=args.e1_messages_per_target)
            if args.experiment == 'E2' and args.profile == 'official':
                import paper_p8_e2_support as e2_support
                with tempfile.TemporaryDirectory(prefix='p8-e2-dry-run-') as tmp:
                    root = Path(tmp); root.chmod(0o700)
                    manifests = e2_support.e2_official_manifests(
                        common={'tls': {'https_endpoint':'https://example.invalid', 'https_ca_file':'ca.pem',
                            'mqtt_tls_endpoint':'mqtts://example.invalid:8883', 'mqtt_tls_ca_file':'ca.pem'}},
                        targets=[{'gateway_id':'gw-a','sensor_id':'sensor-a'},
                                 {'gateway_id':'gw-b','sensor_id':'sensor-b'}],
                        gateway_identity_files={'gw-a':'gw-a.json','gw-b':'gw-b.json'},
                        telemetry_identity_files={'A':'a.json','B':'b.json'},
                        samples_per_message=int(args.samples_per_message), offered_rate=args.e2_offered_rate,
                        duration_seconds=args.e2_duration_seconds, burst_messages=args.e2_burst_messages)
                    pair = manifests['profiles'][str(int(args.samples_per_message))]
                    paths = {}
                    for arm in ('A','B'):
                        paths[arm] = root / f'{arm}.json'
                        paths[arm].write_text(json.dumps(pair[arm])); paths[arm].chmod(0o600)
                    plan = e2_support.build_e2_execution_plan(output_dir=root/'plan',
                        manifest_a=paths['A'], manifest_b=paths['B'], profile='official', official_opt_in=True)
                    e2_plan_summary = {'expected_counts': plan['expected_counts'],
                        'producer_rates_per_second': manifests['producer_rates_per_second'],
                        'estimate': e2_support.e2_official_estimate(rate_per_second=args.e2_offered_rate,
                            duration_seconds=args.e2_duration_seconds,
                            samples_per_message=int(args.samples_per_message), payload_bytes=4096,
                            official_opt_in=True)}
            e4_plan = None
            e4_offline_contract = None
            e3_plan = None
            if args.experiment == 'E3' and args.profile == 'official':
                if not args.official_opt_in:
                    raise ValueError('official_profile_opt_in_required')
                p8_support.lifecycle_budget(workload_seconds=62,
                    session_deadline_seconds=args.deadline_seconds)
                e3_plan = build_e3_offline_contract(workload=args.workload,
                    dispatch_mode=args.dispatch_mode, command_count=args.e3_command_count,
                    schedule=args.e3_schedule, deadline_seconds=args.deadline_seconds)
            if args.experiment == 'E4':
                import paper_p8_e4_support as e4_support
                with tempfile.TemporaryDirectory(prefix='p8-e4-dry-run-') as tmp:
                    root = Path(tmp)
                    common = {'run_id': 'dry-run-e4', 'profile': args.profile,
                              'phases': {'measure_seconds': 4}}
                    phases = ['unique', 'replay'] + (['expiry-replay'] if args.expiry_replay else [])
                    manifests = [e4_support.e4_phase_manifest(common, 'outputs/e4-journal.sqlite', phase,
                        **e4_counts, official_opt_in=args.official_opt_in)
                        for phase in phases]
                    expiry_budget = (e4_support.validate_e4_expiry_budget(
                        session_seconds=args.deadline_seconds,
                        token_seconds=368 + 16 + 30 + 30 + 120 + 600,
                        workload_seconds=368, drain_seconds=16, import_seconds=30,
                        cleanup_seconds=30, journal_bytes=min(64 << 20,
                            max(1 << 20, args.e4_unique_count * (48 << 10))),
                        output_bytes=1 << 20) if args.expiry_replay else None)
                    e4_plan = {'unique_count': e4_counts['unique_count'],
                        'replay_count': e4_counts['replay_count'], 'manifests': manifests,
                        'journal_entry_bound': args.e4_unique_count,
                        'journal_byte_bound': min(64 << 20, max(1 << 20, args.e4_unique_count * (48 << 10))),
                        'expiry_budget': expiry_budget}
                    if args.profile == 'official':
                        e4_offline_contract = build_e4_offline_contract(
                            unique_count=args.e4_unique_count,
                            replay_count=args.e4_replay_count,
                            deadline_seconds=args.deadline_seconds,
                            official_opt_in=args.official_opt_in,
                            expiry_replay=args.expiry_replay)
            print(json.dumps({'status': 'VALIDATED_PLAN_ONLY', 'experiment': args.experiment,
                'profile': args.profile, 'workload': getattr(args, 'workload', None),
                'samples_per_message': getattr(args, 'samples_per_message', None),
                'e1': ({'viewers': args.e1_viewers, 'principals': args.e1_principals,
                        'density': args.e1_density, 'messages_per_target': args.e1_messages_per_target}
                       if args.experiment == 'E1' else None),
                'arm': args.arm, 'seed_source': 'context' if args.context else 'experiment-default',
                  'e2_official_plan': e2_plan_summary, 'e4_plan': e4_plan,
                  'e4_offline_contract': e4_offline_contract,
                 'e1_offline_contract': e1_plan_summary, 'e3_offline_contract': e3_plan,
                'e3': ({'command_count': args.e3_command_count, 'schedule': args.e3_schedule,
                        'workload': args.workload} if args.experiment == 'E3' else None),
                'official_opt_in': bool(args.official_opt_in)}, sort_keys=True))
            return 0
        if args.experiment == 'E3' and args.profile == 'official':
            if not args.official_opt_in:
                raise ValueError('official_profile_opt_in_required')
            if args.e3_command_count not in (3, 30) or args.e3_schedule != 'steady' \
                    or args.e3_command_count == 3 and args.workload != 'low_load':
                raise ValueError('E3_official_requires_30_or_low_load_3_steady_commands')
        root = _private_root(args.root, create=True)
        profile = remote.load_config(args.network_config)
        remote.preflight_bind(profile)
        log = root / 'server.log'
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        hold_flag = {'E1':'--p8-e1-prepare-hold','E2':'--p8-e2-prepare-hold',
                     'E3':'--p8-e3-prepare-hold','E4':'--p8-e4-prepare-hold',
                     'E5':'--p8-e5-prepare-hold'}[args.experiment]
        bundle_flag = {'E1':'--p8-e1-bundle-root','E2':'--p8-e2-bundle-root',
                        'E3':'--p8-e3-bundle-root','E4':'--p8-e4-bundle-root',
                        'E5':'--p8-e5-bundle-root'}[args.experiment]
        arm = (args.arm if args.experiment == 'E5' else
               args.dispatch_mode if args.experiment == 'E3' else
               'e1-pilot' if args.experiment == 'E1' else
               'warm' if args.experiment == 'E4' else 'E2-pilot')
        e5_seed = e5_import.read_context_seed(args.context) if args.experiment == 'E5' else None
        command = [sys.executable, str(ROOT / 'scripts/tests/native_compose_smoke.py'),
                   '--prepare', str(root / 'fixture'), '--p8-lifecycle-hold',
                   hold_flag, bundle_flag, str(root / 'client-bundle'),
                   '--p8-experiment', args.experiment,
                    '--p8-arm', arm,
                    '--p8-seed', (str(e5_seed) if e5_seed is not None else
                                  str(e2_seed) if e2_seed is not None else
                                  '86' if args.experiment == 'E1'
                                  else '89' if args.experiment in ('E3', 'E4') else '84'),
                    '--p8-deadline-seconds', str(args.deadline_seconds),
                    '--p8-profile', args.profile,
                    '--p8-remote-config', str(args.network_config)]
        if args.official_opt_in:
            command.append('--p8-official-opt-in')
        if args.experiment == 'E1':
            command += ['--p8-e1-viewers', str(args.e1_viewers), '--p8-e1-principals', str(args.e1_principals),
                        '--p8-e1-density', args.e1_density,
                        '--p8-e1-messages-per-target', str(args.e1_messages_per_target)]
        if args.experiment == 'E2':
            command += ['--p8-e2-samples-per-message', args.samples_per_message]
            command += ['--p8-e2-workload', workload]
            command += ['--p8-e2-offered-rate', str(args.e2_offered_rate),
                        '--p8-e2-duration-seconds', str(args.e2_duration_seconds),
                        '--p8-e2-burst-messages', str(args.e2_burst_messages)]
            if workload == 'equal_samples':
                command += ['--p8-e2-context', str(args.context)]
                if args.reference is not None:
                    command += ['--p8-e2-reference', str(args.reference)]
        if args.experiment == 'E3':
            command += ['--p8-e3-workload', args.workload,
                        '--p8-e3-command-count', str(args.e3_command_count),
                        '--p8-e3-schedule', args.e3_schedule]
        if args.experiment == 'E4':
            command += ['--p8-e4-cache-mode', args.cache_mode,
                        '--p8-e4-unique-count', str(args.e4_unique_count),
                        '--p8-e4-replay-count', str(args.e4_replay_count)]
            if args.cold_replay:
                command += ['--p8-e4-cold-replay']
            if args.expiry_replay:
                command += ['--p8-e4-expiry-replay']
            if args.durable_reopen:
                command += ['--p8-e4-durable-reopen']
            if args.conflicts:
                command += ['--p8-e4-conflicts']
        if args.experiment == 'E5':
            command += ['--p8-e5-context', str(args.context)]
            if args.reference is not None:
                command += ['--p8-e5-reference', str(args.reference)]
        with os.fdopen(fd, 'wb') as output:
            proc = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
                                    close_fds=True, start_new_session=False)
            try:
                code = proc.wait()
            except KeyboardInterrupt:
                proc.send_signal(signal.SIGINT)
                code = proc.wait(timeout=120)
        print(json.dumps({'server_exit_code': code, 'log': str(log),
                          'state': str(_state_root(root) / 'session.json')}))
        return code
    if args.command == 'status':
        state = supervisor.read_state(_state_root(args.root))
        print(json.dumps({key: state.get(key) for key in (
            'session_id', 'experiment', 'arm', 'profile', 'phase', 'preparation_status',
            'deadline_at', 'endpoint_metadata', 'artifacts', 'workload_outcome',
            'cleanup_outcome', 'project', 'phase_id', 'collector_ready',
            'imported_result', 'phase_history', 'cold_replay_enabled',
             'e4_phase_plan', 'e5_phase_plan', 'durable_verified', 'expiry_evidence',
            'replacement_proof', 'last_request_error', 'workload') if key in state}, sort_keys=True))
        return 0
    if args.command == 'cleanup':
        state = supervisor.read_state(_state_root(args.root))
        print(json.dumps(supervisor.recover_cleanup(_state_root(args.root), project=args.project), sort_keys=True))
        return 0
    state_root = _state_root(args.root)
    state = supervisor.read_state(state_root)
    if state.get('experiment') == 'E5' and args.command in supervisor.E5_PHASE_COMMANDS:
        error = supervisor.e5_phase_transition_error(state, args.command, args.phase_id)
        if error:
            raise ValueError(error)
    if args.command == 'next-phase' and state.get('experiment') not in ('E4', 'E5'):
        raise ValueError('next_phase_is_e4_only')
    if state.get('experiment') == 'E4' and args.command in ('begin', 'finish-phase', 'next-phase'):
        if args.command == 'begin' and (state.get('phase') != 'READY'
                or args.phase_id not in state.get('e4_phase_plan', [])):
            raise ValueError('e4_begin_phase_transition_invalid')
        if args.command == 'finish-phase' and (state.get('phase') != 'MEASURING'
                or args.phase_id != state.get('phase_id')):
            raise ValueError('e4_finish_phase_transition_invalid')
        if args.command == 'next-phase' and (state.get('phase') != 'VERIFYING'
                or args.phase_id not in ('replay', 'cold-replay','durable-fault','durable-reopen','conflicts','expiry-replay')
                or state.get('workload_outcome') != 'VALIDATED'
                or not isinstance(state.get('imported_result'), dict)
                or state['imported_result'].get('validation_valid') is not True
                or args.phase_id == 'replay' and (state.get('phase_id') != 'unique'
                    or state.get('phase_history') != ['unique'])
                or args.phase_id == 'cold-replay' and (state.get('phase_id') != 'replay'
                    or state.get('cold_replay_enabled') is not True
                    or state.get('phase_history') != ['unique', 'replay'])
                or args.phase_id == 'expiry-replay' and (state.get('phase_id') != 'replay'
                    or state.get('expiry_replay_enabled') is not True
                    or state.get('phase_history') != ['unique', 'replay'])
                or not supervisor._e4_next_phase_matches(state, args.phase_id)):
            raise ValueError('e4_next_phase_requires_validated_unique')
    if state.get('session_id') != args.session_id or state.get('phase') != args.phase:
        raise ValueError('request_session_or_phase_mismatch')
    if args.command == 'begin' and state.get('experiment') != 'E4' and (state.get('phase') != 'READY'
            or state.get('preparation_status') != f"{state.get('experiment')}_PREPARED"):
        raise ValueError('request_begin_transition_invalid')
    if args.command == 'finish-phase' and state.get('experiment') != 'E4' and (state.get('phase') != 'MEASURING'
            or state.get('phase_id') != args.phase_id):
        raise ValueError('request_finish_transition_invalid')
    if args.command in ('finalize', 'abort') and state.get('phase') not in ('READY', 'MEASURING', 'VERIFYING'):
        raise ValueError('request_finalization_transition_invalid')
    request = {'session_id': args.session_id, 'phase': args.phase, 'command': args.command}
    if args.command in ('begin', 'finish-phase', 'next-phase'):
        request['phase_id'] = args.phase_id
    if args.command == 'finish-phase':
        request['results'] = str(args.results.absolute())
    supervisor.write_request(state_root, request)
    print(json.dumps({'request_published': True, 'command': args.command,
                      'acknowledgement': 'check status for phase transition and collector/import outcome'}))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        print(f'P8 server error: {exc}', file=sys.stderr)
        raise SystemExit(2)
