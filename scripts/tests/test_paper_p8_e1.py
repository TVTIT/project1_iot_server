import json
import os
from pathlib import Path
import tempfile
import unittest

from scripts.tests.paper_p8_support import build_e1_mapping, e1_expected_deliveries, build_e1_execution_plan
from scripts.tests.native_compose_smoke import paper_p8_e1_report_acceptance, paper_p8_e1_validation_valid
from scripts.paper_p8 import (_viewer_ready, _wait_viewer_ready, _ordered_role_jobs,
                              PaperloadExitError, PaperloadTimeout)


class E1MappingTests(unittest.TestCase):
    def test_report_validation_requires_valid_empty_failures_object(self):
        self.assertTrue(paper_p8_e1_validation_valid({'validation': {'valid': True, 'failures': []}}))
        for evidence in (
                {'validation': {'valid': False, 'failures': []}},
                {'validation': {'valid': True, 'failures': ['bad']}},
                {'validation': True},
                {}):
            self.assertFalse(paper_p8_e1_validation_valid(evidence))

    def test_report_acceptance_requires_pass_full_flag_and_exact_cleanup(self):
        evidence = {'status': 'PASS', 'validation': {'valid': True, 'failures': []},
                    'full_p8_accepted': False}
        self.assertTrue(paper_p8_e1_report_acceptance(evidence, True, True))
        self.assertFalse(paper_p8_e1_report_acceptance(evidence, False, True))
        self.assertFalse(paper_p8_e1_report_acceptance(evidence, True, False))
        for invalid in (
                {**evidence, 'validation': {'valid': False, 'failures': []}},
                {**evidence, 'validation': {'valid': True, 'failures': ['invalid']}},
                {**evidence, 'validation': True},
                {key: value for key, value in evidence.items() if key != 'validation'},
                {**evidence, 'full_p8_accepted': True},
                {**evidence, 'status': 'FAIL'}):
            self.assertFalse(paper_p8_e1_report_acceptance(invalid, True, True))

    def targets(self):
        return [{'gateway_id':g,'sensor_id':f's{i}'} for g in ('g-a','g-b') for i in range(2)]

    def test_sparse_dense_overlap_and_composite_identity_counts(self):
        mappings={}
        for density in ('sparse','dense','overlap'):
            mapping=build_e1_mapping(targets=self.targets(),density=density,viewers=64,users=2)
            mappings[density]=mapping
            self.assertEqual(mapping['counts']['sockets'],64)
            self.assertEqual(mapping['counts']['users'],2)
            self.assertTrue(all(len(v['filters']) <= 16 for v in mapping['viewers']))
            counts={key:20 for key in mapping['expected_recipients_by_target']}
            expected=e1_expected_deliveries(mapping,counts)
            self.assertEqual(sum(expected.values()),sum(20*len(x) for x in mapping['expected_recipients_by_target'].values()))
        self.assertNotEqual(('g-a','same'),('g-b','same'))
        self.assertNotEqual(mappings['sparse']['expected_recipients_by_target'],
                            mappings['dense']['expected_recipients_by_target'])
        for mapping in mappings.values():
            for viewer in mapping['viewers']:
                own_gateway = 'g-a' if viewer['user_id'] == 'user-000' else 'g-b'
                self.assertTrue(all(f['gateway_id'] == own_gateway for f in viewer['filters']))

    def test_mapping_requires_correct_principal_gateway_assignment(self):
        with self.assertRaisesRegex(ValueError, 'user_gateway_mapping'):
            build_e1_mapping(targets=self.targets(), users=3)
        with self.assertRaisesRegex(ValueError, 'user_gateway_mapping'):
            build_e1_mapping(targets=self.targets(), user_gateways={'user-000':'g-missing','user-001':'g-a'})

    def test_invalid_filter_and_official_guard(self):
        with self.assertRaisesRegex(ValueError,'filter_cap'):
            build_e1_mapping(targets=self.targets(),filters_per_viewer=[17]*64)
        with self.assertRaisesRegex(ValueError,'official_profile'):
            build_e1_mapping(targets=self.targets(),viewers=128)

    def test_readiness_validates_counts_private_file_and_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'ready.json'; p.write_text(json.dumps({'status':'ready','connected':64,'subscribed':64,'principals':2,'mode':'healthy','monotonic_origin_qualification':'process local; not comparable across processes'})); p.chmod(0o600)
            self.assertTrue(_viewer_ready(tmp,64))
            p.write_text('{'); self.assertFalse(_viewer_ready(tmp,64))
            p.write_text(json.dumps({'status':'ready','connected':63,'subscribed':64,'principals':2,'mode':'healthy','monotonic_origin_qualification':'not comparable'})); self.assertFalse(_viewer_ready(tmp,64))

    def test_child_exit_malformed_marker_and_fixed_role_order(self):
        class Child:
            def __init__(self, code): self.code=code
            def poll(self): return self.code
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(PaperloadExitError):
                _wait_viewer_ready(Child(2),tmp,64,1)
            with self.assertRaises(PaperloadTimeout):
                _wait_viewer_ready(Child(None),tmp,64,.001,sleep=lambda _:None)
            ready=Path(tmp)/'ready.json'
            ready.write_text(json.dumps({'status':'ready','connected':64,'subscribed':64,
                'principals':2,'mode':'healthy',
                'monotonic_origin_qualification':'process local; not comparable across processes'}))
            ready.chmod(0o600)
            self.assertTrue(_wait_viewer_ready(Child(0),tmp,64,1))
        viewer,telemetry,control=object(),object(),object()
        jobs=[control,telemetry,viewer]
        roles={id(viewer):'viewer',id(telemetry):'telemetry',id(control):'control'}
        self.assertEqual(_ordered_role_jobs(jobs,roles),[viewer,telemetry,control])

    def test_builder_requires_safe_provisioned_role_inputs_and_shared_seed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); paths=[]
            for role in ('viewer','telemetry'):
                p=root/(role+'.json'); p.write_text(json.dumps({'roles':[role],'profile':'pilot','mode':'remote','seed':9,'phases':{'drain_seconds':20},'tls':{'https_endpoint':'https://endpoint.test','https_ca_file':'ca.pem'},'identity_files':{role:'identities.json'},'limits':{'workers':2,'connections':64,'pending_messages':100,'pending_bytes':10000},'targets':[{'gateway_id':'g-a','sensor_id':'s-a'}]})); p.chmod(0o600); paths.append(p)
            result=build_e1_execution_plan(output_dir=root/'out',viewer_manifest=paths[0],telemetry_manifest=paths[1],seed=9,mode='remote')
            self.assertEqual(result['viewer_readiness_budget_seconds'], 20)
            self.assertEqual(json.loads(Path(result['run_spec']).read_text())['paperload'][0]['ready_timeout_seconds'], 20)
            self.assertEqual(result['native_execution'],'NOT RUN')
            spec=json.loads(Path(result['run_spec']).read_text()); self.assertEqual([j['role'] for j in spec['paperload']],['viewer','telemetry'])

    def test_builder_accepts_local_but_never_infers_remote_execution_from_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); paths=[]
            for role in ('viewer','telemetry'):
                p=root/(role+'.json'); p.write_text(json.dumps({'roles':[role],'profile':'pilot','mode':'local','seed':9,'phases':{'drain_seconds':20},'tls':{'https_endpoint':'https://endpoint.test','https_ca_file':'ca.pem'},'identity_files':{role:'identities.json'},'limits':{'workers':2,'connections':64,'pending_messages':100,'pending_bytes':10000},'targets':[{'gateway_id':'g-a','sensor_id':'s-a'}]})); p.chmod(0o600); paths.append(p)
            result=build_e1_execution_plan(output_dir=root/'out',viewer_manifest=paths[0],telemetry_manifest=paths[1],seed=9,mode='local')
            self.assertEqual(result['native_execution'],'NOT RUN')
            self.assertTrue(result['generator_colocated'])
            self.assertFalse(result['actual_remote_execution'])

    def test_e1_readiness_budget_above_120_flows_through_execution_consumers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            viewer_manifest = {
                'schema_version': 1, 'experiment': 'E1', 'arm': 'reference', 'profile': 'official',
                'mode': 'remote', 'roles': ['viewer'], 'run_id': 'p8e1-execution-test', 'seed': 9, 'samples_per_message': 100,
                'topology': {'gateways': 2, 'sensors_per_gateway': 2, 'users': 2, 'viewers': 5000},
                'targets': [{'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'subscriptions': [{'viewer_id': 'v1', 'user_id': 'u1', 'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'traffic': {'rate_per_second': 1, 'ramp_seconds': 100, 'burst_messages': 0},
                'phases': {'warmup_seconds': 0, 'measure_seconds': 14, 'drain_seconds': 8, 'cooldown_seconds': 0},
                'limits': {'workers': 2, 'connections': 5000, 'pending_messages': 100, 'pending_bytes': 10000,
                           'wall_seconds': 190, 'retries': 2, 'disk_growth_bytes': 65536},
                'tls': {'https_endpoint': 'https://endpoint.test', 'https_ca_file': 'ca.pem'},
                'identity_files': {'viewer': 'identities.json'},
            }
            telemetry_manifest = {
                'schema_version': 1, 'experiment': 'E2', 'arm': 'reference', 'profile': 'official',
                'mode': 'remote', 'roles': ['telemetry'], 'seed': 9, 'samples_per_message': 100,
                'topology': {'gateways': 2, 'sensors_per_gateway': 2, 'users': 0, 'viewers': 0},
                'targets': [{'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'subscriptions': [],
                'traffic': {'rate_per_second': 5, 'ramp_seconds': 0, 'burst_messages': 0},
                'phases': {'warmup_seconds': 0, 'measure_seconds': 14, 'drain_seconds': 8, 'cooldown_seconds': 0},
                'limits': {'workers': 2, 'connections': 2, 'pending_messages': 100, 'pending_bytes': 10000,
                           'wall_seconds': 60, 'retries': 2, 'disk_growth_bytes': 65536},
                'tls': {'https_endpoint': 'https://endpoint.test', 'https_ca_file': 'ca.pem'},
                'identity_files': {'telemetry': 'identities.json'},
                'gateway_identity_files': {},
            }
            paths = []
            for role, data in (('viewer', viewer_manifest), ('telemetry', telemetry_manifest)):
                p = root / f'{role}.json'
                p.write_text(json.dumps(data))
                p.chmod(0o600)
                paths.append(p)
            result = build_e1_execution_plan(output_dir=root / 'out', viewer_manifest=paths[0],
                telemetry_manifest=paths[1], seed=9, mode='remote', official_opt_in=True)
            self.assertEqual(result['viewer_readiness_budget_seconds'], 160)
            spec = json.loads(Path(result['run_spec']).read_text())
            self.assertEqual(spec['paperload'][0]['ready_timeout_seconds'], 160)
            self.assertEqual(spec['paperload'][1]['ready_timeout_seconds'], 90)

    def test_runner_accepts_e1_readiness_above_120(self):
        from scripts.paper_p8 import _read_spec
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / 'viewer.json'
            viewer_manifest = {
                'schema_version': 1, 'experiment': 'E1', 'arm': 'reference', 'profile': 'official',
                'mode': 'remote', 'roles': ['viewer'], 'run_id': 'p8e1-runner-test', 'seed': 9, 'samples_per_message': 100,
                'topology': {'gateways': 2, 'sensors_per_gateway': 2, 'users': 2, 'viewers': 5000},
                'targets': [{'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'subscriptions': [{'viewer_id': 'v1', 'user_id': 'u1', 'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'traffic': {'rate_per_second': 1, 'ramp_seconds': 100, 'burst_messages': 0},
                'phases': {'warmup_seconds': 0, 'measure_seconds': 14, 'drain_seconds': 8, 'cooldown_seconds': 0},
                'limits': {'workers': 2, 'connections': 5000, 'pending_messages': 100, 'pending_bytes': 10000,
                           'wall_seconds': 190, 'retries': 0, 'disk_growth_bytes': 65536},
                'tls': {'https_endpoint': 'https://endpoint.test', 'https_ca_file': 'ca.pem'},
                'identity_files': {'viewer': 'identities.json'},
            }
            manifest.write_text(json.dumps(viewer_manifest))
            manifest.chmod(0o600)
            spec = root / 'run-spec.json'
            spec.write_text(json.dumps({'paperload': [{
                'manifest': str(manifest),
                'output_dir': str(root / 'out'),
                'role': 'viewer',
                'ready_timeout_seconds': 160,
            }]}))
            spec.chmod(0o600)
            jobs, gateways, *_ = _read_spec(spec, None, None)
            self.assertEqual(jobs[0]['ready_timeout_seconds'], 160)
            self.assertEqual(gateways, [])

    def test_runner_rejects_readiness_above_120_for_non_e1_or_malformed_context(self):
        from scripts.paper_p8 import _read_spec, PaperloadInputError
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            telemetry = root / 'telemetry.json'
            telemetry.write_text(json.dumps({
                'schema_version': 1, 'experiment': 'E2', 'arm': 'reference', 'profile': 'official',
                'mode': 'remote', 'roles': ['telemetry'],
            }))
            telemetry.chmod(0o600)
            spec_telemetry = root / 'spec-telemetry.json'
            spec_telemetry.write_text(json.dumps({'paperload': [{
                'manifest': str(telemetry), 'output_dir': str(root / 'out-telemetry'),
                'role': 'telemetry', 'ready_timeout_seconds': 160,
            }]}))
            spec_telemetry.chmod(0o600)
            with self.assertRaises(PaperloadInputError):
                _read_spec(spec_telemetry, None, None)

            malformed = root / 'malformed.json'
            malformed.write_text('invalid-json-content')
            malformed.chmod(0o600)
            spec_malformed = root / 'spec-malformed.json'
            spec_malformed.write_text(json.dumps({'paperload': [{
                'manifest': str(malformed), 'output_dir': str(root / 'out-malformed'),
                'role': 'viewer', 'ready_timeout_seconds': 160,
            }]}))
            spec_malformed.chmod(0o600)
            with self.assertRaises(PaperloadInputError):
                _read_spec(spec_malformed, None, None)

            # Negative test: manifest containing only E1 and roles markers is not a validated context
            only_e1 = root / 'only_e1.json'
            only_e1.write_text(json.dumps({'experiment': 'E1', 'roles': ['viewer']}))
            only_e1.chmod(0o600)
            spec_only_e1 = root / 'spec-only-e1.json'
            spec_only_e1.write_text(json.dumps({'paperload': [{
                'manifest': str(only_e1), 'output_dir': str(root / 'out-only-e1'),
                'role': 'viewer', 'ready_timeout_seconds': 160,
            }]}))
            spec_only_e1.chmod(0o600)
            with self.assertRaises(PaperloadInputError):
                _read_spec(spec_only_e1, None, None)

    def test_runner_readiness_boundary_negatives_and_cap(self):
        from scripts.paper_p8 import _read_spec, PaperloadInputError
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            viewer_manifest = {
                'schema_version': 1, 'experiment': 'E1', 'arm': 'reference', 'profile': 'official',
                'mode': 'remote', 'roles': ['viewer'], 'run_id': 'p8e1-cap-test', 'seed': 9, 'samples_per_message': 100,
                'topology': {'gateways': 2, 'sensors_per_gateway': 2, 'users': 2, 'viewers': 5000},
                'targets': [{'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'subscriptions': [{'viewer_id': 'v1', 'user_id': 'u1', 'gateway_id': 'g-a', 'sensor_id': 's-a'}],
                'traffic': {'rate_per_second': 1, 'ramp_seconds': 100, 'burst_messages': 0},
                'phases': {'warmup_seconds': 0, 'measure_seconds': 14, 'drain_seconds': 8, 'cooldown_seconds': 0},
                'limits': {'workers': 2, 'connections': 5000, 'pending_messages': 100, 'pending_bytes': 10000,
                           'wall_seconds': 340, 'retries': 0, 'disk_growth_bytes': 65536},
                'tls': {'https_endpoint': 'https://endpoint.test', 'https_ca_file': 'ca.pem'},
                'identity_files': {'viewer': 'identities.json'},
            }
            v_path = root / 'viewer.json'
            v_path.write_text(json.dumps(viewer_manifest))
            v_path.chmod(0o600)

            # Cap at exactly 300 is accepted for E1 viewer
            spec_300 = root / 'spec-300.json'
            spec_300.write_text(json.dumps({'paperload': [{
                'manifest': str(v_path), 'output_dir': str(root / 'out-300'),
                'role': 'viewer', 'ready_timeout_seconds': 300,
            }]}))
            spec_300.chmod(0o600)
            jobs, *_ = _read_spec(spec_300, None, None)
            self.assertEqual(jobs[0]['ready_timeout_seconds'], 300)

            # Over cap 301 is rejected
            for bad_val in (301, 300.5, True, False, 0, -1, float('nan'), float('inf')):
                spec_bad = root / 'spec-bad.json'
                spec_bad.write_text(json.dumps({'paperload': [{
                    'manifest': str(v_path), 'output_dir': str(root / 'out-bad'),
                    'role': 'viewer', 'ready_timeout_seconds': bad_val,
                }]}))
                spec_bad.chmod(0o600)
                with self.assertRaises(PaperloadInputError, msg=f'expected rejection for {bad_val}'):
                    _read_spec(spec_bad, None, None)

            # Non-E1 experiment (E3) rejected for > 120
            e3_manifest = {
                'schema_version': 1, 'experiment': 'E3', 'arm': 'reference', 'profile': 'official',
                'mode': 'remote', 'roles': ['control'],
            }
            e3_path = root / 'e3.json'
            e3_path.write_text(json.dumps(e3_manifest))
            e3_path.chmod(0o600)
            spec_e3 = root / 'spec-e3.json'
            spec_e3.write_text(json.dumps({'paperload': [{
                'manifest': str(e3_path), 'output_dir': str(root / 'out-e3'),
                'role': 'control', 'ready_timeout_seconds': 121,
            }]}))
            spec_e3.chmod(0o600)
            with self.assertRaises(PaperloadInputError):
                _read_spec(spec_e3, None, None)


if __name__ == '__main__': unittest.main()
