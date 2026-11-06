import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from scripts import paper_p8_bundle as bundle
from scripts import paper_p8_client as client


def token(exp):
    header=base64.urlsafe_b64encode(b'{"alg":"RS256","typ":"JWT"}').decode().rstrip('=')
    payload=base64.urlsafe_b64encode(json.dumps({'exp':exp},separators=(',',':')).encode()).decode().rstrip('=')
    return f'{header}.{payload}.b3BhcXVlLXNpZw'


def write(path, value, mode=0o600):
    path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    path.write_text(value if isinstance(value,str) else json.dumps(value,separators=(',',':')))
    path.chmod(mode)
    return path


class PortableBundleTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='p8-bundle-',dir='/var/tmp/opencode/p8-remote-step2')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.src=self.root/'source'; self.src.mkdir(mode=0o700)
        self.parent=self.root/'A-out'; self.parent.mkdir(mode=0o700)
        self.exp=int(time.time())+900
        write(self.src/'https-ca.pem','test https public CA')
        write(self.src/'mqtt-ca.pem','test mqtt public CA')
        write(self.src/'viewer.json',{'user-a':token(self.exp), 'user-b':token(self.exp)})
        write(self.src/'telemetry.json',{})
        write(self.src/'gw-1.json',{'username':'gw-user','password':'gw-password'})
        write(self.src/'gateway-env.json',{'GATEWAY_CONTROL_ENABLED':'true','GATEWAY_ID':'gw-1',
            'SENSOR_ID':'sensor-1','GATEWAY_MQTT_BROKER_URL':'mqtts://broker.test:18884',
            'GATEWAY_MQTT_CLIENT_ID':'client-1','GATEWAY_MQTT_USERNAME':'gw-user',
            'GATEWAY_MQTT_PASSWORD':'gw-password','GATEWAY_MQTT_CA_FILE':'/A/private/ca.pem',
            'GATEWAY_OUTBOX_PATH':'/A/private/outbox.sqlite'})
        common={'schema_version':1,'experiment':'E1','arm':'baseline','profile':'pilot','mode':'remote',
            'run_id':'run-1','seed':7,'samples_per_message':100,'topology':{'gateways':1,
            'sensors_per_gateway':1,'users':1,'viewers':1},'targets':[{'gateway_id':'gw-1','sensor_id':'sensor-1'}],
            'subscriptions':[],'traffic':{'rate_per_second':1,'ramp_seconds':0,'burst_messages':0},
            'phases':{'warmup_seconds':0,'measure_seconds':10,'drain_seconds':5,'cooldown_seconds':0},
            'limits':{'workers':1,'connections':1,'pending_messages':4,'pending_bytes':65536,'wall_seconds':30,
                      'retries':1,'disk_growth_bytes':65536},
            'tls':{'https_endpoint':'https://fixture.test:14443','mqtt_tls_endpoint':'mqtts://fixture.test:18884',
                   'https_ca_file':'/A/secret/p2-ca.pem','mqtt_tls_ca_file':'/A/secret/mqtt-ca.pem'},
            'gateway_identity_files':{'gw-1':'/A/private/gw-1.json'},'bundle_path':'/A/run'}
        self.viewer={**common,'experiment':'E1','roles':['viewer'],'subscriptions':[{'viewer_id':'v1',
            'user_id':'user-a','gateway_id':'gw-1'}],'identity_files':{'viewer':'/A/private/viewer.json'}}
        self.telemetry={**common,'experiment':'E2','roles':['telemetry'],'identity_files':{'telemetry':'/A/private/telemetry.json'}}
        write(self.src/'viewer.manifest.json',self.viewer); write(self.src/'telemetry.manifest.json',self.telemetry)
        write(self.src/'run-spec.json',{'paperload':[{'manifest':'viewer.manifest.json',
            'output_dir':'/A/output/viewer','role':'viewer','ready_timeout_seconds':90},
            {'manifest':'telemetry.manifest.json','output_dir':'/A/output/telemetry',
             'role':'telemetry','ready_timeout_seconds':90}],
            'gateways':[{'env_file':'gateway-env.json','role':'durable-telemetry'}],
            'stop_timeout_seconds':30})

    def export(self, *, exp=None, leaf=None, **overrides):
        args={'output_leaf':leaf or self.parent/'bundle','session_id':'session-actual-1',
            'experiment':'E1','arm':'baseline','phase':'measure',
            'manifests':{'viewer':self.src/'viewer.manifest.json','telemetry':self.src/'telemetry.manifest.json'},
            'identity_files':{'viewer':self.src/'viewer.json','telemetry':self.src/'telemetry.json'},
            'gateway_identity_files':{'gw-1':self.src/'gw-1.json'},
            'gateway_env_files':{'gw-1':self.src/'gateway-env.json'},
            'ca_files':{'https':self.src/'https-ca.pem','mqtt':self.src/'mqtt-ca.pem'},
            'role_metadata':{'roles':{'viewer':{'role':'viewer','principals':['user-a','user-b']},
                 'telemetry':{'role':'telemetry','principals':[]}},
                 'gateways':{'gw-1':{'role':'gateway','gateway_id':'gw-1','username':'gw-user'}}},
            'wait_seconds':30,'workload_seconds':20,'topology':{'gateways':1,'sensors':1},
            'workload':{'measure_seconds':10},'run_spec':self.src/'run-spec.json'}
        args.update(overrides)
        return bundle.export_bundle(**args)

    def test_e1_export_relocates_and_inventory_validates_selected_inputs_only(self):
        result=self.export()
        root=Path(result['bundle_root'])
        copied=self.root/'B'/'different'/'filesystem'
        copied.parent.mkdir(parents=True); shutil.copytree(root,copied)
        self.assertTrue(bundle.inspect_bundle(copied)['valid'])
        manifest=json.loads((copied/'manifests/viewer.json').read_text())
        self.assertEqual(manifest['tls']['https_ca_file'],'certs/https-ca.pem')
        self.assertEqual(manifest['identity_files']['viewer'],'private/identities/viewer.json')
        self.assertEqual(manifest['gateway_identity_files']['gw-1'],'private/gateways/gw-1.json')
        self.assertEqual(manifest['bundle_path'],'.')
        spec=json.loads((copied/'execution/run-spec.json').read_text())
        self.assertEqual(spec['paperload'][0]['manifest'],'manifests/viewer.json')
        self.assertEqual(spec['paperload'][0]['output_dir'],'outputs/viewer')
        self.assertNotIn('/A/',(copied/'execution/run-spec.json').read_text())
        self.assertEqual(bundle.inspect_bundle(copied)['session']['profile'], 'pilot')

        env=json.loads((copied/'gateway-env/gw-1.json').read_text())
        self.assertNotIn('GATEWAY_MQTT_PASSWORD',env)
        self.assertEqual(env['GATEWAY_MQTT_CA_FILE'],'certs/mqtt-ca.pem')
        self.assertEqual(env['GATEWAY_OUTBOX_PATH'],'outputs/outbox/gw-1.sqlite')
        self.assertEqual((copied/'private/gateway-passwords/gw-1.txt').read_text(),'gw-password')
        self.assertEqual((copied/'certs/https-ca.pem').read_bytes(),(self.src/'https-ca.pem').read_bytes())
        for path in root.rglob('*'):
            if path.is_file(): self.assertEqual(path.stat().st_mode & 0o777,0o600)
            elif path.is_dir(): self.assertEqual(path.stat().st_mode & 0o777,0o700)
        metadata=json.loads((root/'bundle.json').read_text())
        self.assertNotIn('gw-password',(root/'bundle.json').read_text())
        self.assertNotIn('header.',(root/'bundle.json').read_text())
        self.assertTrue(bundle.inspect_bundle(root)['valid'])
        self.assertEqual(metadata['session']['session_id'],'session-actual-1')
        self.assertEqual(metadata['session']['run_ids'],['run-1'])
        copied_ca=root/'certs/https-ca.pem'
        copied_ca.write_text('tampered')
        with self.assertRaisesRegex(ValueError,'bundle_checksum_mismatch'):
            bundle.inspect_bundle(root)

    def test_e1_bundle_lifetime_covers_manifest_wall_after_copy_delay(self):
        self.viewer['limits']['wall_seconds'] = 210
        write(self.src/'viewer.manifest.json', self.viewer)
        result=self.export(workload_seconds=50, wait_seconds=0)
        metadata=json.loads((Path(result['bundle_root'])/'bundle.json').read_text())
        self.assertGreaterEqual(metadata['workload_seconds'],210)
        (self.root / 'B').mkdir(mode=0o700, exist_ok=True)
        workspace = self.root / 'B' / 'delay-workspace'
        bound = bundle.bind_bundle(bundle_root=result['bundle_root'],
            workspace_leaf=workspace, elapsed_transfer_wait_seconds=40)
        self.assertEqual(bound['session']['session_id'], 'session-actual-1')

    def test_e1_export_rejects_insufficient_jwt_for_large_manifest_wall(self):
        self.viewer['limits']['wall_seconds'] = 210
        write(self.src/'viewer.manifest.json', self.viewer)
        issued_at = int(time.time())
        write(self.src/'viewer.json', {'user-a': token(issued_at + 150),
                                       'user-b': token(issued_at + 150)})
        with self.assertRaisesRegex(ValueError, 'bundle_identity_near_expiry'):
            self.export(leaf=self.parent/'short-jwt', workload_seconds=50, wait_seconds=0)

    def test_e1_bind_rejects_near_expiry_after_transfer_elapsed_for_large_manifest_wall(self):
        self.viewer['limits']['wall_seconds'] = 210
        write(self.src/'viewer.manifest.json', self.viewer)
        issued_at = int(time.time())
        write(self.src/'viewer.json', {'user-a': token(issued_at + 260),
                                       'user-b': token(issued_at + 260)})
        exported = self.export(leaf=self.parent/'transfer-expiry', wait_seconds=0, workload_seconds=50)
        (self.root / 'B').mkdir(mode=0o700, exist_ok=True)
        with patch.object(bundle.time, 'time', return_value=issued_at + 60):
            with self.assertRaisesRegex(ValueError, 'bundle_identity_near_expiry'):
                bundle.bind_bundle(bundle_root=exported['bundle_root'],
                    workspace_leaf=self.root / 'B' / 'expired-workspace',
                    elapsed_transfer_wait_seconds=60)

    def test_e1_bind_rejects_tampered_workload_budget_smaller_than_manifest_wall(self):
        self.viewer['limits']['wall_seconds'] = 210
        write(self.src/'viewer.manifest.json', self.viewer)
        exported = self.export(leaf=self.parent/'tampered-budget', wait_seconds=0, workload_seconds=50)
        bundle_root = Path(exported['bundle_root'])
        b_meta = json.loads((bundle_root / 'bundle.json').read_text())
        b_meta['workload_seconds'] = 50
        (bundle_root / 'bundle.json').write_text(json.dumps(b_meta))
        (self.root / 'B').mkdir(mode=0o700, exist_ok=True)
        with self.assertRaisesRegex(ValueError, 'bundle_workload_budget_rejected'):
            bundle.bind_bundle(bundle_root=bundle_root,
                workspace_leaf=self.root / 'B' / 'tampered-workspace')

    def test_e1_export_rejects_workload_exceeding_max_wait(self):
        self.viewer['limits']['wall_seconds'] = 7150
        write(self.src/'viewer.manifest.json', self.viewer)
        with self.assertRaisesRegex(ValueError, 'bundle_wait_bound_rejected'):
            self.export(leaf=self.parent/'over-max-wait', wait_seconds=100, workload_seconds=50)

    def test_e1_bundle_preserves_readiness_budget_above_120(self):
        spec=json.loads((self.src/'run-spec.json').read_text())
        spec['paperload'][0]['ready_timeout_seconds']=160
        spec['paperload'][1]['ready_timeout_seconds']=90
        write(self.src/'run-spec.json',spec)
        result=self.export()
        bound=bundle.bind_bundle(bundle_root=result['bundle_root'],workspace_leaf=self.root/'B-ready')
        bound_spec=json.loads((Path(bound['workspace'])/'runtime/execution/run-spec.json').read_text())
        self.assertEqual(bound_spec['paperload'][0]['ready_timeout_seconds'],160)
        self.assertEqual(bound_spec['paperload'][1]['ready_timeout_seconds'],90)

    def test_e1_bundle_readiness_boundary_negatives(self):
        # Non-E1 viewer (telemetry) over 120 rejected at export
        spec = json.loads((self.src / 'run-spec.json').read_text())
        spec['paperload'][0]['ready_timeout_seconds'] = 90
        spec['paperload'][1]['ready_timeout_seconds'] = 121
        write(self.src / 'run-spec.json', spec)
        with self.assertRaisesRegex(ValueError, 'bundle_run_spec_rejected'):
            self.export(leaf=self.parent / 'b-over-non-e1')

        # E1 viewer over 300 rejected at export
        spec['paperload'][0]['ready_timeout_seconds'] = 301
        spec['paperload'][1]['ready_timeout_seconds'] = 90
        write(self.src / 'run-spec.json', spec)
        with self.assertRaisesRegex(ValueError, 'bundle_run_spec_rejected'):
            self.export(leaf=self.parent / 'b-over-300')

        # Boolean readiness rejected at export
        spec['paperload'][0]['ready_timeout_seconds'] = True
        write(self.src / 'run-spec.json', spec)
        with self.assertRaisesRegex(ValueError, 'bundle_run_spec_rejected'):
            self.export(leaf=self.parent / 'b-bool')

    def test_bundle_rejects_profile_mismatch_between_workload_and_manifests(self):
        with self.assertRaisesRegex(ValueError, 'bundle_profile_mismatch'):
            self.export(leaf=self.parent/'profile-mismatch', workload={'profile':'official'})

    def test_bind_creates_absolute_runtime_paths_and_preserves_input_bundle(self):
        bundle_root = Path(self.export()['bundle_root'])
        relocated = self.root / 'B' / 'copied' / 'bundle'
        relocated.parent.mkdir(parents=True)
        shutil.copytree(bundle_root, relocated)
        workspace = self.root / 'B' / 'workspace'
        result = bundle.bind_bundle(bundle_root=relocated, workspace_leaf=workspace)
        spec = json.loads((workspace / 'runtime/execution/run-spec.json').read_text())
        self.assertTrue(Path(spec['paperload'][0]['manifest']).is_absolute())
        self.assertTrue(Path(spec['gateways'][0]['env_file']).is_absolute())
        env = json.loads(Path(spec['gateways'][0]['env_file']).read_text())
        self.assertTrue(Path(env['GATEWAY_OUTBOX_PATH']).is_absolute())
        self.assertFalse(Path(env['GATEWAY_OUTBOX_PATH']).exists())
        self.assertEqual(spec['disk_guard']['growth_threshold_bytes'], 64 << 10)
        self.assertEqual(spec['disk_guard']['allowed_root'], str((workspace / 'outputs').absolute()))
        self.assertEqual(result['session']['session_id'], 'session-actual-1')
        binding = json.loads((workspace / 'runtime/binding.json').read_text())
        self.assertEqual(binding['immutable_root'], 'inputs/')
        self.assertTrue((workspace / 'inputs/execution/run-spec.json').is_file())

    def test_bind_refuses_existing_workspace(self):
        bundle_root = Path(self.export()['bundle_root'])
        workspace = self.root / 'B' / 'workspace'
        workspace.mkdir(parents=True)
        with self.assertRaisesRegex(ValueError, 'workspace_output_leaf_exists'):
            bundle.bind_bundle(bundle_root=bundle_root, workspace_leaf=workspace)

    def test_new_output_leaf_required_and_source_files_stay_unchanged(self):
        before={p.name:p.read_bytes() for p in self.src.iterdir() if p.is_file()}
        self.export()
        self.assertEqual(before,{p.name:p.read_bytes() for p in self.src.iterdir() if p.is_file()})
        with self.assertRaisesRegex(ValueError,'bundle_output_leaf_exists'):
            self.export()

    def test_e1_manifest_credentials_do_not_require_gateway_executor_env(self):
        spec=write(self.src/'client-only-run-spec.json',{'paperload':[
            {'manifest':'viewer.manifest.json','output_dir':'/A/viewer','role':'viewer'},
            {'manifest':'telemetry.manifest.json','output_dir':'/A/telemetry','role':'telemetry'}]})
        result=self.export(leaf=self.parent/'client-only',gateway_env_files={},run_spec=spec)
        root=Path(result['bundle_root'])
        run=json.loads((root/'execution/run-spec.json').read_text())
        self.assertEqual(run['gateways'],[])
        self.assertTrue((root/'private/gateways/gw-1.json').is_file())

    def test_e2_dual_telemetry_manifest_bundle_relocates_and_binds_distinct_roles(self):
        write(self.src/'gw-2.json', {'username':'gw-user-2','password':'gw-password-2'})
        a = {**self.telemetry, 'experiment':'E2', 'arm':'A', 'mode':'remote',
             'run_id':'e2-run-100', 'targets':[{'gateway_id':'gw-1','sensor_id':'sensor-1'}],
             'gateway_identity_files':{'gw-1':'/A/private/gw-1.json'}}
        b = {**self.telemetry, 'experiment':'E2', 'arm':'B', 'mode':'remote',
             'run_id':'e2-run-100', 'targets':[{'gateway_id':'gw-2','sensor_id':'sensor-2'}],
             'gateway_identity_files':{'gw-2':'/A/private/gw-2.json'}}
        ap, bp = write(self.src/'A.manifest.json', a), write(self.src/'B.manifest.json', b)
        spec = write(self.src/'e2-run-spec.json', {'paperload':[
            {'manifest':'A.manifest.json','output_dir':'/A/output/A','role':'telemetry','ready_timeout_seconds':90},
            {'manifest':'B.manifest.json','output_dir':'/A/output/B','role':'telemetry','ready_timeout_seconds':90}],
            'gateways':[], 'stop_timeout_seconds':30})
        result = bundle.export_bundle(output_leaf=self.parent/'e2-bundle', session_id='e2-session-100',
            experiment='E2', arm='E2-pilot', phase='samples-100', manifests={'A':ap,'B':bp},
            identity_files={'A:telemetry':self.src/'telemetry.json','B:telemetry':self.src/'telemetry.json'},
            gateway_identity_files={'gw-1':self.src/'gw-1.json','gw-2':self.src/'gw-2.json'},
            gateway_env_files={}, ca_files={'https':self.src/'https-ca.pem','mqtt':self.src/'mqtt-ca.pem'},
            role_metadata={'roles':{'A:telemetry':{'role':'telemetry','principals':[]},
                'B:telemetry':{'role':'telemetry','principals':[]}}, 'gateways':{
                'gw-1':{'role':'gateway','gateway_id':'gw-1','username':'gw-user'},
                'gw-2':{'role':'gateway','gateway_id':'gw-2','username':'gw-user-2'}}},
            wait_seconds=10, workload_seconds=30, topology={'gateways':2,'sensors':2},
            workload={'experiment':'E2','samples_per_message':100,'unique_batches':24}, run_spec=spec)
        source = Path(result['bundle_root'])
        relocated = self.root/'B'/'relocated-e2'
        relocated.parent.mkdir(mode=0o700); shutil.copytree(source, relocated)
        self.assertTrue(bundle.inspect_bundle(relocated)['valid'])
        workspace = self.root/'B'/'e2-workspace'
        bundle.bind_bundle(bundle_root=relocated, workspace_leaf=workspace)
        bound = json.loads((workspace/'runtime/execution/run-spec.json').read_text())
        self.assertEqual([Path(job['manifest']).stem for job in bound['paperload']], ['A','B'])
        self.assertEqual([job['output_dir'] for job in bound['paperload']],
                         [str(workspace/'outputs/A'), str(workspace/'outputs/B')])

    def test_expiry_unknown_fields_path_traversal_symlink_and_unselected_identity_fail_closed(self):
        write(self.src/'viewer.json',{'user-a':token(int(time.time())+40), 'user-b':token(int(time.time())+40)})
        with self.assertRaisesRegex(ValueError,'bundle_identity_near_expiry'): self.export(leaf=self.parent/'expired')
        write(self.src/'viewer.json',{'user-a':token(self.exp), 'user-b':token(self.exp)})
        raw=json.loads((self.src/'viewer.manifest.json').read_text()); raw['unexpected']='x'
        write(self.src/'viewer.manifest.json',raw)
        with self.assertRaisesRegex(ValueError,'bundle_manifest_fields_rejected'): self.export(leaf=self.parent/'unknown')
        write(self.src/'viewer.manifest.json',self.viewer)
        with self.assertRaisesRegex(ValueError,'bundle_path_reference_rejected'): bundle._safe_ref('../outside')
        link=self.src/'viewer-link.json'; link.symlink_to(self.src/'viewer.json')
        with self.assertRaisesRegex(ValueError,'bundle_source_symlink_rejected'):
            self.export(leaf=self.parent/'symlink',identity_files={'viewer':link,'telemetry':self.src/'telemetry.json'})
        with self.assertRaisesRegex(ValueError,'bundle_identity_principals_mismatch'):
            self.export(leaf=self.parent/'identity',role_metadata={'roles':{'viewer':{'role':'viewer','principals':['wrong']},
                'telemetry':{'role':'telemetry','principals':[]}},'gateways':{'gw-1':{
                    'role':'gateway','gateway_id':'gw-1','username':'gw-user'}}})

    def test_planned_lifetime_does_not_substitute_for_issued_jwt_expiry(self):
        write(self.src/'viewer.json', {'user-a':token(int(time.time())+3600),
            'user-b':token(int(time.time())+3600)})
        with self.assertRaisesRegex(ValueError, 'bundle_identity_near_expiry'):
            self.export(leaf=self.parent/'insufficient-issued-token', workload_seconds=4200,
                wait_seconds=0)

    def test_bind_rechecks_actual_exp_after_late_transfer(self):
        issued_at = int(time.time())
        self.viewer['limits']['wall_seconds'] = 20
        write(self.src/'viewer.manifest.json', self.viewer)
        write(self.src/'viewer.json', {'user-a':token(issued_at+60),
            'user-b':token(issued_at+60)})
        exported=self.export(leaf=self.parent/'late-bind', wait_seconds=0, workload_seconds=20)
        (self.root/'B').mkdir(mode=0o700)
        with patch.object(bundle.time, 'time', return_value=issued_at+25):
            with self.assertRaisesRegex(ValueError, 'bundle_identity_near_expiry'):
                bundle.bind_bundle(bundle_root=exported['bundle_root'],
                    workspace_leaf=self.root/'B'/'late-workspace', elapsed_transfer_wait_seconds=25)

    def test_e3_control_and_executor_gateway_environment_references_are_bounded(self):
        arm_a={**self.telemetry,'experiment':'E2','arm':'A','targets':[{'gateway_id':'gw-1','sensor_id':'sensor-1'}]}
        arm_b={**self.telemetry,'experiment':'E2','arm':'B','targets':[{'gateway_id':'gw-1','sensor_id':'sensor-2'}],
               'gateway_identity_files':{'gw-1':'/A/private/gw-1.json'}}
        control={**self.telemetry,'experiment':'E3','arm':'control','roles':['control'],
            'identity_files':{'control':'/A/private/control.json'},'gateway_identity_files':{}}
        control['profile']='official'
        control['tls']={'https_endpoint':'https://fixture.test:14443','https_ca_file':'/A/ca.pem'}
        write(self.src/'arm-a.json',arm_a); write(self.src/'arm-b.json',arm_b); write(self.src/'control.json',control)
        write(self.src/'control-id.json',{'operator-a':token(self.exp)})
        write(self.src/'gateway-env-2.json',{'GATEWAY_CONTROL_ENABLED':'true','GATEWAY_ID':'gw-1',
            'SENSOR_ID':'sensor-2','GATEWAY_MQTT_BROKER_URL':'mqtts://broker.test:18884',
            'GATEWAY_MQTT_CLIENT_ID':'client-2','GATEWAY_MQTT_USERNAME':'gw-user',
            'GATEWAY_MQTT_PASSWORD':'gw-password','GATEWAY_MQTT_CA_FILE':'/A/private/ca.pem',
            'GATEWAY_OUTBOX_PATH':'/A/private/outbox-2.sqlite'})
        write(self.src/'e3-run-spec.json',{'paperload':[
            {'manifest':'arm-a.json','output_dir':'/A/A-output','role':'telemetry'},
            {'manifest':'arm-b.json','output_dir':'/A/B-output','role':'telemetry'},
            {'manifest':'control.json','output_dir':'/A/control-output','role':'control'}],
            'gateways':[{'env_file':'gateway-env.json','role':'executor'},
                        {'env_file':'gateway-env-2.json','role':'executor'}]})
        out=self.export(leaf=self.parent/'e3',experiment='E3',arm='mixed',
            manifests={'A':self.src/'arm-a.json','B':self.src/'arm-b.json','control':self.src/'control.json'},
            identity_files={'A:telemetry':self.src/'telemetry.json','B:telemetry':self.src/'telemetry.json',
                            'control':self.src/'control-id.json'},
            gateway_identity_files={'gw-1':self.src/'gw-1.json'},
            gateway_env_files={'executor-a':self.src/'gateway-env.json',
                               'executor-b':self.src/'gateway-env-2.json'},
            role_metadata={'roles':{'A:telemetry':{'role':'telemetry','principals':[]},
                'B:telemetry':{'role':'telemetry','principals':[]},
                'control':{'role':'control','principals':['operator-a']}},
                'gateways':{'gw-1':{'role':'gateway','gateway_id':'gw-1','username':'gw-user'}}},
             run_spec=self.src/'e3-run-spec.json',
             workload={'experiment':'E3','profile':'official','workload':'low_load',
                 'dispatch_mode':'polling','seed':89,'generator_colocated':'unverified'})
        root=Path(out['bundle_root'])
        metadata=json.loads((root/'bundle.json').read_text())
        self.assertEqual(metadata['session']['profile'],'official')
        self.assertEqual(json.loads((root/'manifests/control.json').read_text())['profile'],'official')
        self.assertEqual(json.loads((root/'manifests/A.json').read_text())['profile'],'pilot')
        spec=json.loads((root/'execution/run-spec.json').read_text())
        self.assertEqual({x['role'] for x in spec['paperload']},{'telemetry','control'})
        self.assertEqual({x['role'] for x in spec['gateways']},{'executor'})
        self.assertEqual({x['env_file'] for x in spec['gateways']},
                         {'gateway-env/executor-a.json','gateway-env/executor-b.json'})
        for item in spec['gateways']:
            env=json.loads((root/item['env_file']).read_text())
            self.assertTrue(env['GATEWAY_OUTBOX_PATH'].startswith('outputs/'))
        env_a=json.loads((root/'gateway-env/executor-a.json').read_text())
        env_b=json.loads((root/'gateway-env/executor-b.json').read_text())
        self.assertNotEqual(env_a['GATEWAY_OUTBOX_PATH'],env_b['GATEWAY_OUTBOX_PATH'])
        self.assertNotEqual(env_a['GATEWAY_MQTT_CLIENT_ID'],env_b['GATEWAY_MQTT_CLIENT_ID'])
        (self.root/'B').mkdir(mode=0o700)
        workspace = self.root / 'B' / 'e3-workspace'
        bundle.bind_bundle(bundle_root=root, workspace_leaf=workspace)
        bound_spec=json.loads((workspace/'runtime/execution/run-spec.json').read_text())
        bound_envs=[json.loads(Path(item['env_file']).read_text()) for item in bound_spec['gateways']]
        self.assertEqual(len(bound_envs),2)
        self.assertEqual({item['GATEWAY_ID'] for item in bound_envs},{'gw-1'})
        self.assertEqual(len({item['GATEWAY_OUTBOX_PATH'] for item in bound_envs}),2)

    def test_e4_phase_manifests_share_new_journal_target_without_copying_source_db(self):
        source_journal=write(self.src/'existing-pending.sqlite','do-not-copy-this-state')
        common={**self.telemetry,'experiment':'E4','arm':'warm','e4':{
            'mode':'unique','journal_path':str(source_journal),'max_messages':20,'max_bytes':65536,
            'replay_count':0,'ignore_first_valid_acks':1}}
        unique={**common,'run_id':'run-unique'}
        replay={**common,'run_id':'run-replay','e4':{**common['e4'],'mode':'replay','replay_count':10,
            'ignore_first_valid_acks':0}}
        write(self.src/'unique.json',unique); write(self.src/'replay.json',replay)
        out=self.export(leaf=self.parent/'e4',experiment='E4',arm='warm',phase='unique-replay',
            manifests={'unique':self.src/'unique.json','replay':self.src/'replay.json'},run_spec=None,
            identity_files={'telemetry':self.src/'telemetry.json'},
            role_metadata={'roles':{'telemetry':{'role':'telemetry','principals':[]}},
                'gateways':{'gw-1':{'role':'gateway','gateway_id':'gw-1','username':'gw-user'}}})
        root=Path(out['bundle_root'])
        unique_b=json.loads((root/'manifests/unique.json').read_text())
        replay_b=json.loads((root/'manifests/replay.json').read_text())
        self.assertEqual(unique_b['e4']['journal_path'],'outputs/e4-journal.sqlite')
        self.assertEqual(replay_b['e4']['journal_path'],'outputs/e4-journal.sqlite')
        self.assertFalse((root/'outputs/e4-journal.sqlite').exists())
        self.assertEqual(source_journal.read_text(),'do-not-copy-this-state')

    def test_real_cold_e4_export_bind_and_phase_eligibility_use_digest_verified_metadata(self):
        common={**self.telemetry,'experiment':'E4','arm':'warm','mode':'remote',
            'e4':{'mode':'unique','journal_path':'/A/e4-journal.sqlite','max_messages':20,
                'max_bytes':65536,'replay_count':0,'ignore_first_valid_acks':1}}
        telemetry_manifests={}
        for label, mode, run_id in (('unique','unique','run-unique'),
                ('replay','replay','run-replay'),('cold-replay','replay','run-cold-replay')):
            telemetry_manifests[label]=write(self.src/f'{label}.manifest.json',{
                **common,'run_id':run_id,'e4':{**common['e4'],'mode':mode,
                    'replay_count':0 if label=='unique' else 10,
                    'ignore_first_valid_acks':1 if label=='unique' else 0}})
        viewer=write(self.src/'e4-viewer.manifest.json',{
            **self.viewer,'arm':'warm','experiment':'E1','identity_files':{'viewer':'/A/viewer.json'}})

        def bind_e4(cache_mode, include_cold, suffix):
            manifests={**telemetry_manifests,'viewer':viewer}
            if not include_cold:
                manifests.pop('cold-replay')
            identity_files={f'{label}:telemetry':self.src/'telemetry.json'
                for label in manifests if label!='viewer'}
            identity_files['viewer']=self.src/'viewer.json'
            roles={f'{label}:telemetry':{'role':'telemetry','principals':[]}
                for label in manifests if label!='viewer'}
            roles['viewer']={'role':'viewer','principals':['user-a','user-b']}
            result=bundle.export_bundle(output_leaf=self.parent/f'e4-{suffix}',
                session_id='e4-session-actual',experiment='E4',arm='warm',phase='unique-replay',
                manifests=manifests,identity_files=identity_files,
                gateway_identity_files={'gw-1':self.src/'gw-1.json'},
                gateway_env_files={'gw-1':self.src/'gateway-env.json'},
                ca_files={'https':self.src/'https-ca.pem','mqtt':self.src/'mqtt-ca.pem'},
                role_metadata={'roles':roles,'gateways':{'gw-1':{'role':'gateway',
                    'gateway_id':'gw-1','username':'gw-user'}}},wait_seconds=30,workload_seconds=20,
                topology={'gateways':1,'sensors':1,'users':2,'viewers':2},
                workload={'experiment':'E4','arm':'warm','run_id':'e4-run',
                    'cache_mode':cache_mode,'phase':'unique-replay','profile':'pilot',
                    'seed':89,'generator_colocated':True},
                durable_probe={'gateway_id':'gw-1','sensor_id':'p8-e4-durable',
                    'username':'gw-user','mqtt_server_name':'localhost',
                    'sample_interval_us':10000,'max_rows':64,'max_bytes':1 << 20,
                    'shutdown_timeout_seconds':10})
            workspace=self.root/'B'/f'workspace-{suffix}'
            workspace.parent.mkdir(mode=0o700,exist_ok=True)
            bound=bundle.bind_bundle(bundle_root=result['bundle_root'],workspace_leaf=workspace)
            root=client._workspace(workspace)
            binding=json.loads((root/'runtime/binding.json').read_text())
            probe=json.loads((root/'runtime/durable-probe.json').read_text())
            self.assertEqual(probe['gateway_id'],'gw-1')
            self.assertEqual(probe['run_id'],'run-unique')
            self.assertEqual(probe['ca_file'],str((root/'inputs/certs/mqtt-ca.pem').resolve()))
            self.assertEqual(probe['password_file'],str((root/'inputs/private/gateways/gw-1.json').resolve()))
            self.assertTrue(binding['bound_durable_probe_sha256'])
            spec=json.loads((root/'runtime/execution/run-spec.json').read_text())
            state={'last_completed_phase':'replay','unique_validated':True,
                'session_id':binding['session']['session_id'],'run_ids':binding['session']['run_ids']}
            return root,binding,spec['paperload'],state,bound

        root,binding,jobs,state,bound=bind_e4('on',True,'cold-on')
        self.assertEqual(bound['manifest_count'],4)
        self._run_bound_durable_stage(root, binding)
        self.assertTrue(client._validate_e4_phase_eligibility(root,binding,jobs,state,'cold-replay'))
        selected=client._select_e4_phase_jobs(jobs,'cold-replay',replay_validated=True)
        self.assertEqual({Path(row['manifest']).stem for row in selected},{'cold-replay','viewer'})
        with self.assertRaisesRegex(ValueError,'E4 cold replay phase is not enabled or not next'):
            client._validate_e4_phase_eligibility(root,binding,jobs,
                {**state,'last_completed_phase':'unique'},'cold-replay')
        with self.assertRaisesRegex(ValueError,'E4 cold replay phase is not enabled or not next'):
            client._validate_e4_phase_eligibility(root,binding,jobs[:-2]+jobs[-1:],state,'cold-replay')
        metadata_path=root/'runtime/bundle-metadata.json'
        metadata=json.loads(metadata_path.read_text()); metadata['workload']['cache_mode']='off'
        metadata_path.write_text(json.dumps(metadata)); metadata_path.chmod(0o600)
        with self.assertRaisesRegex(ValueError,'bound bundle metadata provenance mismatch'):
            client._validate_e4_phase_eligibility(root,binding,jobs,state,'cold-replay')
        off_root,off_binding,off_jobs,off_state,_=bind_e4('off',True,'cold-off')
        with self.assertRaisesRegex(ValueError,'E4 cold replay phase is not enabled or not next'):
            client._validate_e4_phase_eligibility(off_root,off_binding,off_jobs,off_state,'cold-replay')

    def _run_bound_durable_stage(self, root, binding):
        from types import SimpleNamespace
        from scripts.tests import paper_p8_e4_durable_client as durable
        binary=root/'gateway-test'; binary.write_bytes(b'fixture'); binary.chmod(0o700)
        inputs=root/'runtime/durable-probe.json'
        sqlite_path=root/'outputs/outbox/e4-durable.sqlite'
        header={'protocol_version':1,'message_id':'m-1','message_type':'telemetry_batch',
            'gateway_id':'gw-1','sensor_id':'p8-e4-durable','boot_id':'boot-1',
            'first_sequence':5,'sample_count':100,'measured_at':'2026-01-01T00:00:00Z',
            'sample_interval_us':10000,'samples':[{'value':1}] * 100}
        payload=json.dumps(header,separators=(',',':')).encode()
        class Child:
            stage=None
            def __init__(self, argv, **kwargs):
                if kwargs.get('umask') != 0o077:
                    raise AssertionError('durable Gateway child must create private SQLite files')
                self.argv=argv; self.env=kwargs['env']; self.out=kwargs['stdout']
                self.stage='fault' if '--stop-after-ignored-ack' in argv else 'reopen'
                Child.stage=self.stage
                self.returncode=None
                self.path=Path(self.env['GATEWAY_OUTBOX_PATH'])
                self.assert_valid()
            def assert_valid(self):
                self_outer.assertEqual(self.env['GATEWAY_ID'],'gw-1')
                self_outer.assertEqual(self.env['GATEWAY_SENSOR_ID'] if 'GATEWAY_SENSOR_ID' in self.env else self.env['SENSOR_ID'],'p8-e4-durable')
                self_outer.assertEqual(self.env['GATEWAY_CONTROL_ENABLED'],'false')
                self_outer.assertEqual(self.env['GATEWAY_OUTBOX_PATH'],str(sqlite_path))
                self_outer.assertIn('_p8_e4_durable_' + self.stage,self.env['GATEWAY_MQTT_CLIENT_ID'])
            def wait(self, timeout=None):
                if self.stage=='fault':
                    db=sqlite3.connect(self.path)
                    db.execute('CREATE TABLE outbox (message_id TEXT,boot_id TEXT,gateway_id TEXT,sensor_id TEXT,first_sequence INTEGER,sample_interval_us INTEGER,sample_count INTEGER,payload BLOB,byte_size INTEGER,status TEXT)')
                    db.execute('INSERT INTO outbox VALUES (?,?,?,?,?,?,?,?,?,?)',
                        ('m-1','boot-1','gw-1','p8-e4-durable',5,10000,100,payload,len(payload),'pending'))
                    db.commit(); db.close(); self.path.chmod(0o600)
                    self.returncode=1
                    self.out.write(b'run summary: intentional_fault_stop=true ignored_valid_acks=1 completed=false enqueued=1 app_acknowledged=0 pending_rows=1\n')
                else:
                    db=sqlite3.connect(self.path); db.execute("UPDATE outbox SET status='acked'"); db.commit(); db.close()
                    self.returncode=0
                    self.out.write(b'run summary: completed=true enqueued=1 app_acknowledged=2\n')
                self.out.flush(); return self.returncode
            def poll(self): return self.returncode
            def terminate(self): self.returncode=-15
            def kill(self): self.returncode=-9
        self_outer=self
        fake_stage3=SimpleNamespace(gateway_env=lambda gateway, password:{'GATEWAY_ID':gateway,'GATEWAY_MQTT_PASSWORD':password})
        with patch.dict('sys.modules',{'stage3_uplink_runner':fake_stage3}), \
             patch.object(durable.subprocess,'Popen',Child):
            args=['durable-stage','--workspace',str(root),'--gateway-binary',str(binary),
                '--inputs',str(inputs),'--stage','fault']
            self.assertEqual(client.main(args),0)
        fault=json.loads((root/'runtime/e4-durable-fault.json').read_text())
        self.assertEqual(fault['classification'],'INTENTIONAL_FAULT_STOP')
        a_support=__import__('scripts.tests.paper_p8_e4_support',fromlist=['x'])
        export=root/'fault-results'
        self.assertEqual(client.main(['export-durable-results','--workspace',str(root),
            '--output',str(export),'--stage','durable-fault']),0)
        incoming=root/'incoming'; incoming.mkdir(mode=0o700)
        fault_incoming=incoming/'fault'; shutil.copytree(export,fault_incoming)
        prepared={'session_id':binding['session']['session_id'],'run_id':'e4-run',
            'phase_id':binding['session']['phase'],'arm':'warm','bundle_sha256':binding['source_bundle_sha256'],
            'bundle_inventory':binding['source_inventory'],'bundle_session':binding['session'],
            'durable_probe':json.loads(inputs.read_text()),'specs':[object(),object()],'pg':'pg',
            'durable_sensor_urn':'urn:ngsi-ld:Sensor:gw-1:p8-e4-durable'}
        prepared['durable_probe']['gateway_id']='gw-1'
        empty={'ledger':0,'raw':0,'temporal':0,'configuration':0,'fingerprint':'8'*32}
        after_fault={'ledger':1,'raw':100,'temporal':1,'configuration':0,'fingerprint':'c'*32}
        class Smoke:
            def db_psql(self, _registry, _pg, sql, _params):
                if "'ledger'" in sql:
                    return json.dumps({'ledger':0,'raw':0,'temporal':0,'configuration':0})
                if sql.startswith('SELECT md5'):
                    return '8'*32
                return json.dumps({'hash':fault['pending']['payload_sha256'],
                    'ledger_fingerprint':'3'*32,'gateway_id':'gw-1',
                    'sensor_id':'p8-e4-durable','raw':100})
        smoke=Smoke()
        with patch.object(a_support,'_counts',return_value={'fingerprint':'main'}):
            a_support.begin_remote_durable_phase(smoke,None,prepared,'durable-fault')
        with patch.object(a_support,'_durable_sensor_counts',autospec=True,return_value=after_fault), \
             patch.object(a_support,'_counts',return_value={'fingerprint':'main'}):
            accepted=a_support.finish_remote_durable_phase(smoke,SimpleNamespace(private=root),None,
                prepared,fault_incoming,phase='durable-fault')
        self.assertTrue(accepted['validation_valid'])
        reopen_base=dict(after_fault)
        with patch.object(a_support,'_durable_sensor_counts',autospec=True,return_value=reopen_base), \
             patch.object(a_support,'_counts',return_value={'fingerprint':'main'}):
            a_support.begin_remote_durable_phase(smoke,None,prepared,'durable-reopen')
        with patch.dict('sys.modules',{'stage3_uplink_runner':fake_stage3}), \
             patch.object(durable.subprocess,'Popen',Child):
            args[-1]='reopen'
            self.assertEqual(client.main(args),0)
        reopen_export=root/'reopen-results'
        self.assertEqual(client.main(['export-durable-results','--workspace',str(root),
            '--output',str(reopen_export),'--stage','durable-reopen']),0)
        reopen_incoming=incoming/'reopen'; shutil.copytree(reopen_export,reopen_incoming)
        first=datetime(2026,1,1,tzinfo=timezone.utc)
        raw_rows=[{'sequence':5+i,'measured_at':(first+timedelta(microseconds=10000*i)).isoformat()}
            for i in range(100)]
        class ReopenSmoke:
            def db_psql(self,_registry,_pg,sql,_params):
                if "'message_id',p.message_id" in sql:
                    return json.dumps({'message_id':'m-1','hash':fault['pending']['payload_sha256'],
                        'ledger_fingerprint':'3'*32,'raw':100,'boot':'boot-1',
                        'first_sequence':5,'last_sequence':104,'first_timestamp':first.isoformat(),
                        'sample_count':100})
                if 'json_agg(json_build_object' in sql:
                    return json.dumps(raw_rows)
                raise AssertionError('unexpected SQL read')
        after_reopen={'ledger':2,'raw':200,'temporal':2,'configuration':0,'fingerprint':'9'*32}
        with patch.object(a_support,'_durable_sensor_counts',autospec=True,return_value=after_reopen), \
             patch.object(a_support,'_counts',return_value={'fingerprint':'main'}):
            done=a_support.finish_remote_durable_phase(ReopenSmoke(),SimpleNamespace(private=root),None,
                prepared,reopen_incoming,phase='durable-reopen')
        self.assertTrue(done['validation_valid'])
        self.assertEqual(prepared['durable_verified']['durable-reopen']['database_delta'],
            {'ledger':1,'raw':100,'temporal':1,'configuration':0})
        self.assertEqual(prepared['durable_verified']['durable-reopen']['sql']['auxiliary_delta'],
            {'ledger':2,'raw':200,'temporal':2})
        zero_ws={'submissions':0,'new_events':0,'deliveries_enqueued':0,
            'telemetry_writes_completed':0,'drops':0}
        unique_telemetry = {'experiment': 'E4', 'telemetry': {
            'generated_unique': 20, 'application_acks': 20,
            'ignored_valid_acks': 1, 'retries':1,'pending_messages': 0,'pending_bytes':0}}
        replay_telemetry = {'experiment': 'E4', 'telemetry': {
            'replay_offered': 10, 'replay_application_acks': 10}}
        prepared.update(include_durable_reopen=True,include_cold_replay=False,
            cache_mode='on',arm='warm',run_id='e4-run',schema='24:test',quota={},
            effective_flags={},image_ids={},phase_run_ids={},overall_validation={'valid':True,'failures':[]},
            phase_results={'unique':{'validation_valid':True,'telemetry':unique_telemetry},
                           'replay':{'validation_valid':True,'telemetry':replay_telemetry}},
            phase_windows={'unique':{'database_delta':{'ledger':20,'raw':2000,'temporal':20,'configuration':0},
                'ws_delta':{'submissions':20,'new_events':20,'deliveries_enqueued':20,
                    'telemetry_writes_completed':20,'drops':0},'journal_sha256':'a'*64,
                 'raw':[{'verified_exact':True,'ledger_count':10,'raw_identity_value_mismatches':0},
                        {'verified_exact':True,'ledger_count':10,'raw_identity_value_mismatches':0}]},
                'replay':{'database_delta':{'ledger':0,'raw':0,'temporal':0,'configuration':0},
                    'ws_delta':zero_ws,'journal_sha256':'a'*64,
                     'cache':{'warm_lookup_hits':10,'repository_calls_avoided':10},
                     'database_proof':{
                         'unique_ack':[{'ledger':20,'raw':2000,'temporal':20,'configuration':0,'fingerprint':'1'*32}]*2,
                         'replay_end':[{'ledger':20,'raw':2000,'temporal':20,'configuration':0,'fingerprint':'1'*32}]*2,
                         'exact_equal':True}}})
        report=a_support.finalize_remote_report(prepared,{'phase_history':[
            'unique','replay','durable-fault','durable-reopen'],'workload_outcome':'VALIDATED'},
            cleanup_verified=True,cleanup_counts={})
        self.assertEqual(report['status'],'PASS')
        self.assertEqual(report['database_delta']['raw'],2000)
        self.assertEqual(report['durable_gateway_reopen']['status'],'PASS')
        self.assertFalse(report['full_p8_accepted'])
        self.assertEqual(report['conflicts'],'NOT RUN')
        from scripts.tests import native_compose_smoke as smoke
        self.assertTrue(smoke.paper_p8_e4_warm_subset_acceptance(report, cleanup_verified=True))
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(report, cleanup_verified=False))
        bad_fault_delta = json.loads(json.dumps(report))
        bad_fault_delta['durable_gateway_reopen']['fault']['database_delta']['raw'] = 99
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_fault_delta, cleanup_verified=True))
        bad_aux_delta = json.loads(json.dumps(report))
        bad_aux_delta['durable_gateway_reopen']['reopen']['sql']['auxiliary_delta']['raw'] = 300
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_aux_delta, cleanup_verified=True))
        bad_binding = json.loads(json.dumps(report))
        bad_binding['durable_gateway_reopen']['reopen']['binding_sha256'] = 'f' * 64
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_binding, cleanup_verified=True))
        bad_payload_sha = json.loads(json.dumps(report))
        bad_payload_sha['durable_gateway_reopen']['fault']['sql']['payload_sha256'] = 'e' * 64
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_payload_sha, cleanup_verified=True))
        bad_pending = json.loads(json.dumps(report))
        bad_pending['durable_gateway_reopen']['reopen']['pending_rows'] = 1
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_pending, cleanup_verified=True))
        bad_sql = json.loads(json.dumps(report))
        bad_sql['durable_gateway_reopen']['reopen'].pop('sql')
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_sql, cleanup_verified=True))
        bad_phase = json.loads(json.dumps(report))
        bad_phase['phase_history'] = ['unique', 'replay', 'durable-fault']
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_phase, cleanup_verified=True))
        bad_base = json.loads(json.dumps(report))
        bad_base['cache_window']['warm_lookup_hits'] = 9
        self.assertFalse(smoke.paper_p8_e4_warm_subset_acceptance(bad_base, cleanup_verified=True))
        prepared['durable_verified']['durable-reopen']['sql']['auxiliary_delta']['raw'] = 300
        rejected_total = a_support.finalize_remote_report(prepared, {'phase_history': [
            'unique', 'replay', 'durable-fault', 'durable-reopen'], 'workload_outcome': 'VALIDATED'},
            cleanup_verified=True, cleanup_counts={})
        self.assertNotEqual(rejected_total['status'], 'PASS')
        prepared['durable_verified']['durable-reopen']['sql']['auxiliary_delta']['raw'] = 200
        prepared['durable_verified'].pop('durable-reopen')
        rejected=a_support.finalize_remote_report(prepared,{'phase_history':[
            'unique','replay','durable-fault','durable-reopen'],'workload_outcome':'VALIDATED'},
            cleanup_verified=True,cleanup_counts={})
        self.assertNotEqual(rejected['status'],'PASS')

    def test_e2_equal_samples_bundle_export_relocate_and_bind(self):
        from scripts.tests.paper_p8_e2_support import (
            build_e2_equal_samples_context,
            calculate_e2_equal_samples_expected_counts,
            e2_equal_samples_manifests,
        )
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {
            'mode': 'remote', 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': 'https://fixture.test:14443', 'https_ca_file': str(self.src / 'https-ca.pem'),
                    'mqtt_tls_endpoint': 'mqtts://fixture.test:18884', 'mqtt_tls_ca_file': str(self.src / 'mqtt-ca.pem'),
                    'mqtt_tls_server_name': 'localhost'},
        }
        gw_files = {'gw-1': str(self.src / 'gw-1.json'), 'gw-2': str(self.src / 'gw-1.json')}
        write(self.src / 'gw-2.json', {'username': 'gw-2', 'password': 'pass-2'})
        gw_files['gw-2'] = str(self.src / 'gw-2.json')
        tele_files = {'A': str(self.src / 'telemetry.json'), 'B': str(self.src / 'telemetry.json')}
        targets = [{'gateway_id': 'gw-1', 'sensor_id': 's-1'}, {'gateway_id': 'gw-2', 'sensor_id': 's-2'}]

        all_manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        pair = all_manifests['profiles']['100']
        ma_path = write(self.src / 'A.manifest.json', pair['A'])
        mb_path = write(self.src / 'B.manifest.json', pair['B'])
        expected_counts = calculate_e2_equal_samples_expected_counts(ctx, 100)

        workload = {
            'experiment': 'E2',
            'arm': 'E2-pilot',
            'phase': 'equal-samples-100',
            'profile': 'pilot',
            'workload': 'equal_samples',
            'workload_type': 'equal_samples',
            'seed': ctx['seed'],
            'samples_per_message': 100,
            'duration_seconds': ctx['duration_seconds'],
            'drain_seconds': 20,
            'grace_seconds': 10,
            'wall_seconds': ctx['duration_seconds'] + 20 + 10,
            'context_sha256': ctx['context_sha256'],
            'expected_counts': expected_counts,
            'context': ctx,
            'generator_colocated': 'unverified',
        }
        run_spec = write(self.src / 'e2-run-spec.json', {
            'paperload': [
                {'manifest': str(ma_path), 'output_dir': '/A/output/A', 'role': 'telemetry', 'ready_timeout_seconds': 90},
                {'manifest': str(mb_path), 'output_dir': '/A/output/B', 'role': 'telemetry', 'ready_timeout_seconds': 90},
            ],
            'stop_timeout_seconds': 90,
        })

        out = bundle.export_bundle(
            output_leaf=self.parent / 'e2-equal-samples-bundle',
            session_id='sess-e2-equal',
            experiment='E2',
            arm='E2-pilot',
            phase='equal-samples-100',
            manifests={'A': ma_path, 'B': mb_path},
            identity_files={'A:telemetry': self.src / 'telemetry.json', 'B:telemetry': self.src / 'telemetry.json'},
            gateway_identity_files={'gw-1': self.src / 'gw-1.json', 'gw-2': self.src / 'gw-2.json'},
            gateway_env_files={},
            ca_files={'https': self.src / 'https-ca.pem', 'mqtt': self.src / 'mqtt-ca.pem'},
            role_metadata={
                'roles': {'A:telemetry': {'role': 'telemetry', 'principals': []},
                          'B:telemetry': {'role': 'telemetry', 'principals': []}},
                'gateways': {'gw-1': {'role': 'gateway', 'gateway_id': 'gw-1', 'username': 'gw-user'},
                             'gw-2': {'role': 'gateway', 'gateway_id': 'gw-2', 'username': 'gw-2'}}},
            wait_seconds=90,
            workload_seconds=90,
            topology={'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
            workload=workload,
            run_spec=run_spec,
        )

        root = Path(out['bundle_root'])
        self.assertTrue(bundle.inspect_bundle(root)['valid'])

        # Relocation test
        relocated = self.root / 'relocated-e2-bundle'
        shutil.copytree(root, relocated)
        self.assertTrue(bundle.inspect_bundle(relocated)['valid'])

        # Bind test
        workspace = self.root / 'e2-workspace'
        binding = bundle.bind_bundle(bundle_root=relocated, workspace_leaf=workspace)
        self.assertTrue((workspace / 'runtime/bundle-metadata.json').is_file())
        bound_meta = json.loads((workspace / 'runtime/bundle-metadata.json').read_text())
        self.assertEqual(bound_meta['workload']['workload_type'], 'equal_samples')
        self.assertEqual(bound_meta['workload']['context_sha256'], ctx['context_sha256'])
        bound_ma = json.loads((workspace / 'inputs/manifests/A.json').read_text())
        self.assertEqual(bound_ma['equal_samples']['sample_stream_version'], 1)
        self.assertEqual(bound_ma['equal_samples']['context_sha256'], ctx['context_sha256'])

    def test_e2_equal_samples_bundle_negatives(self):
        from scripts.tests.paper_p8_e2_support import (
            build_e2_equal_samples_context,
            calculate_e2_equal_samples_expected_counts,
            e2_equal_samples_manifests,
        )
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {
            'mode': 'remote', 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': 'https://fixture.test:14443', 'https_ca_file': str(self.src / 'https-ca.pem'),
                    'mqtt_tls_endpoint': 'mqtts://fixture.test:18884', 'mqtt_tls_ca_file': str(self.src / 'mqtt-ca.pem'),
                    'mqtt_tls_server_name': 'localhost'},
        }
        write(self.src / 'gw-2.json', {'username': 'gw-2', 'password': 'pass-2'})
        gw_files = {'gw-1': str(self.src / 'gw-1.json'), 'gw-2': str(self.src / 'gw-2.json')}
        tele_files = {'A': str(self.src / 'telemetry.json'), 'B': str(self.src / 'telemetry.json')}
        targets = [{'gateway_id': 'gw-1', 'sensor_id': 's-1'}, {'gateway_id': 'gw-2', 'sensor_id': 's-2'}]

        all_manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        pair = all_manifests['profiles']['100']
        ma_path = write(self.src / 'neg_A.manifest.json', pair['A'])
        mb_path = write(self.src / 'neg_B.manifest.json', pair['B'])
        expected_counts = calculate_e2_equal_samples_expected_counts(ctx, 100)

        def make_workload(**overrides):
            wl = {
                'experiment': 'E2',
                'arm': 'E2-pilot',
                'phase': 'equal-samples-100',
                'profile': 'pilot',
                'workload': 'equal_samples',
                'workload_type': 'equal_samples',
                'seed': ctx['seed'],
                'samples_per_message': 100,
                'duration_seconds': ctx['duration_seconds'],
                'drain_seconds': 20,
                'grace_seconds': 10,
                'wall_seconds': ctx['duration_seconds'] + 20 + 10,
                'context_sha256': ctx['context_sha256'],
                'expected_counts': expected_counts,
                'context': ctx,
                'generator_colocated': 'unverified',
            }
            wl.update(overrides)
            return wl

        run_spec = write(self.src / 'neg-e2-run-spec.json', {
            'paperload': [
                {'manifest': str(ma_path), 'output_dir': '/A/output/A', 'role': 'telemetry', 'ready_timeout_seconds': 90},
                {'manifest': str(mb_path), 'output_dir': '/A/output/B', 'role': 'telemetry', 'ready_timeout_seconds': 90},
            ],
            'stop_timeout_seconds': 90,
        })

        def do_export(leaf_name, wl, m_a=ma_path, m_b=mb_path):
            return bundle.export_bundle(
                output_leaf=self.parent / leaf_name,
                session_id='sess-e2-neg',
                experiment='E2',
                arm='E2-pilot',
                phase='equal-samples-100',
                manifests={'A': m_a, 'B': m_b},
                identity_files={'A:telemetry': self.src / 'telemetry.json', 'B:telemetry': self.src / 'telemetry.json'},
                gateway_identity_files={'gw-1': self.src / 'gw-1.json', 'gw-2': self.src / 'gw-2.json'},
                gateway_env_files={},
                ca_files={'https': self.src / 'https-ca.pem', 'mqtt': self.src / 'mqtt-ca.pem'},
                role_metadata={
                    'roles': {'A:telemetry': {'role': 'telemetry', 'principals': []},
                              'B:telemetry': {'role': 'telemetry', 'principals': []}},
                    'gateways': {'gw-1': {'role': 'gateway', 'gateway_id': 'gw-1', 'username': 'gw-user'},
                                 'gw-2': {'role': 'gateway', 'gateway_id': 'gw-2', 'username': 'gw-2'}}},
                wait_seconds=90,
                workload_seconds=90,
                topology={'gateways': 2, 'sensors': 2, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0},
                workload=wl,
                run_spec=run_spec,
            )

        # 1. Wrong workload_type
        with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
            do_export('neg1', make_workload(workload_type='invalid_type'))

        # 2. Context hash mismatch
        with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
            do_export('neg2', make_workload(context_sha256='0' * 64))

        # 3. Wrong profile
        with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
            do_export('neg3', make_workload(profile='unsupported'))

        # 4. Unknown nested key in context
        bad_ctx = dict(ctx, unexpected_key=123)
        with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
            do_export('neg4', make_workload(context=bad_ctx))

        # 5. Unknown nested key in expected_counts
        bad_counts = dict(expected_counts, unexpected_key=456)
        with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
            do_export('neg5', make_workload(expected_counts=bad_counts))

        # 6. Count mismatch in expected_counts
        mismatched_counts = dict(expected_counts, total_samples=9999)
        with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
            do_export('neg6', make_workload(expected_counts=mismatched_counts))

        # 7. Manifest with tampered sample_stream_version
        bad_ma_data = dict(pair['A'])
        bad_ma_data['equal_samples'] = dict(bad_ma_data['equal_samples'], sample_stream_version=2)
        bad_ma_path = write(self.src / 'bad_ver_A.json', bad_ma_data)
        with self.assertRaises(ValueError):
            do_export('neg7', make_workload(), m_a=bad_ma_path)

        # 8. Manifest with unknown nested key in equal_samples
        bad_ma_data2 = dict(pair['A'])
        bad_ma_data2['equal_samples'] = dict(bad_ma_data2['equal_samples'], unknown_key=True)
        bad_ma_path2 = write(self.src / 'bad_keys_A.json', bad_ma_data2)
        with self.assertRaises(ValueError):
            do_export('neg8', make_workload(), m_a=bad_ma_path2)



if __name__=='__main__': unittest.main()
