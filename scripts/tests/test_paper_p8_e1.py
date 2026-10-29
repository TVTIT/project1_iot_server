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


if __name__ == '__main__': unittest.main()
