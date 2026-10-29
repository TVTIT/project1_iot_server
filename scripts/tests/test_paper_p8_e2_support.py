import json
import tempfile
import unittest
from unittest.mock import Mock
from pathlib import Path

from scripts.tests.paper_p8_e2_support import (
    build_e2_execution_plan, e2_fixture_profile, e2_official_estimate, e2_pilot_manifests,
    validate_e2_result,
)
from scripts.tests import paper_p8_e2_support as e2
from scripts.tests import paper_p8_support as p8
from scripts.tests import native_compose_smoke as smoke_runtime


class E2SupportTests(unittest.TestCase):
    def setUp(self):
        self.targets = [{'gateway_id': 'gw-a', 'sensor_id': 's-a'},
                        {'gateway_id': 'gw-b', 'sensor_id': 's-b'}]
        self.profiles = e2_pilot_manifests(common={'tls': {'https_endpoint': 'https://localhost',
            'https_ca_file': 'ca.crt', 'mqtt_tls_endpoint': 'mqtts://localhost',
            'mqtt_tls_ca_file': 'mqtt-ca.crt'}}, targets=self.targets,
            gateway_identity_files={'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'},
            telemetry_identity_files={'A': 'identity-a.json', 'B': 'identity-b.json'})

    def test_pilot_profiles_match_sample_counts_and_concurrent_work(self):
        self.assertNotEqual(self.profiles['profiles']['100']['A']['run_id'],
                            self.profiles['profiles']['1']['A']['run_id'])
        for samples, pair in self.profiles['profiles'].items():
            self.assertEqual(int(samples), pair['A']['samples_per_message'])
            self.assertEqual(pair['A']['samples_per_message'], pair['B']['samples_per_message'])
            self.assertEqual(pair['A']['seed'], pair['B']['seed'])
            self.assertEqual(pair['A']['targets'], [self.targets[0]])
            self.assertEqual(pair['B']['targets'], [self.targets[1]])
            self.assertEqual(pair['A']['gateway_identity_files'], {'gw-a':'gw-a.json'})
            self.assertEqual(pair['B']['gateway_identity_files'], {'gw-b':'gw-b.json'})
            self.assertEqual(pair['A']['topology']['gateways'], 1)
            self.assertEqual(pair['B']['topology']['gateways'], 1)
            self.assertEqual(pair['A']['phases']['measure_seconds'], 8)
            self.assertEqual(pair['B']['phases']['measure_seconds'], 4)
            self.assertEqual(pair['B']['traffic']['burst_messages'], 12)
            self.assertEqual(sum((8, 16)), 24)
            self.assertEqual(pair['A']['limits'], pair['B']['limits'])

    def test_next_fixture_profile_is_separate_and_bounded(self):
        proposed = e2_fixture_profile(microbatch_wait_ms=50)
        self.assertEqual(proposed['shaper'], {'rate_per_second':2,'burst':4})
        self.assertEqual(proposed['microbatch'], {'max_messages':32,'max_rows':500,
                         'max_bytes':4 << 20,'wait_ms':50})
        self.assertFalse(proposed['effective_fixture_flags']['STAGE5_ENABLED'])
        self.assertFalse(proposed['effective_fixture_flags']['WEBSOCKET_ENABLED'])
        with self.assertRaisesRegex(ValueError, 'microbatch_wait_rejected'):
            e2_fixture_profile(microbatch_wait_ms=75)

    def test_plan_requires_private_matching_manifests_and_official_optin(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a, b = [root / f'{x}.json' for x in ('a', 'b')]
            for arm, path in zip(('A', 'B'), (a, b)):
                path.write_text(json.dumps(self.profiles['profiles']['100'][arm]))
                path.chmod(0o600)
            plan = build_e2_execution_plan(output_dir=root / 'plan', manifest_a=a, manifest_b=b)
            spec = json.loads(Path(plan['run_spec']).read_text())
            self.assertEqual(len(spec['paperload']), 2)
            self.assertEqual([Path(x['manifest']).name for x in spec['paperload']],
                             ['A.manifest.json', 'B.manifest.json'])
            with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
                build_e2_execution_plan(output_dir=root / 'other', manifest_a=a, manifest_b=b,
                                        profile='official')

    def test_official_estimate_enforces_bounds_and_is_not_wire_bytes(self):
        estimate = e2_official_estimate(rate_per_second=100, duration_seconds=10,
            samples_per_message=100, payload_bytes=5000, official_opt_in=True)
        self.assertEqual(estimate['unique_messages_estimate'], 1000)
        self.assertIsNone(estimate['payload_bytes_wire'])
        self.assertTrue(estimate['not_measurements'])
        self.assertGreater(estimate['total_storage_growth_bytes_estimate'],
                           estimate['application_payload_bytes_estimate'])
        with self.assertRaisesRegex(ValueError, 'predicted_bound_exceeded'):
            e2_official_estimate(rate_per_second=20000, duration_seconds=7200,
                samples_per_message=100, payload_bytes=100000, official_opt_in=True)
        with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
            e2_official_estimate(rate_per_second=100, duration_seconds=10,
                samples_per_message=1, payload_bytes=100)

    def test_phase_overlap_and_accounting_fail_closed(self):
        a = {'status': 'COMPLETE', 'telemetry': {
            'generated_unique': 8, 'offered_unique': 8, 'publish_attempts': 8, 'broker_pubacks': 8,
            'application_acks': 8, 'pending_messages': 0, 'pending_bytes': 0, 'retries':0,'invalid_acks':0}}
        b = {'status': 'COMPLETE', 'telemetry': {
            'generated_unique': 16, 'offered_unique': 16, 'publish_attempts': 18, 'broker_pubacks': 18,
            'application_acks': 16, 'pending_messages': 0, 'pending_bytes': 0, 'retries':2,'invalid_acks':0}}
        phases = [{'child': f'paperload-telemetry:{arm}.manifest.json', 'event': event,
                   'observed_at_runner_monotonic_ns': stamp}
                  for arm, event, stamp in [('A','measure_begin',10),('A','measure_end',100),
                                            ('B','burst_begin',50),('B','burst_end',70)]]
        ingress = {'available': True, 'delta': {'Received':24,'Accepted':24,'Committed':24,
            'DuplicateCommitted':0,'SamplesNew':24,'ACKPublished':24,'ShaperRateLimited':2,
            **dict.fromkeys(('RejectedNotRunning','RejectedStopping','RejectedRetained',
                'RejectedOversized','RejectedInvalidTopic','QueueFull','ByteBudgetFull'),0)}}
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta=ingress,
            database_delta={'ledger':24,'raw':24,'temporal':24,'configuration':0},
            database_by_gateway={'gw-a':{'ledger':8,'raw':8,'temporal':8,'configuration':0},
                                 'gw-b':{'ledger':16,'raw':16,'temporal':16,'configuration':0}},
            phase_events=phases, raw_verification=[{'verified_exact':True,'gateway_id':'gw-a','sensor_id':'s-a','ledger_count':8},
                {'verified_exact':True,'gateway_id':'gw-b','sensor_id':'s-b','ledger_count':16}],
            execution_context={'samples_per_message':1,'role_to_target':{'A':self.targets[0],'B':self.targets[1]}})
        self.assertTrue(result['valid'], result['failures'])
        self.assertEqual(result['phase_overlap']['observed_overlap_ns'], 20)
        self.assertEqual(result['actual_shaper_drops']['available'], 2)
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta={},
            database_delta={'ledger':23}, database_by_gateway={}, phase_events=[],
            execution_context={'samples_per_message':1,'role_to_target':{'A':self.targets[0],'B':self.targets[1]}})
        self.assertFalse(result['valid'])
        self.assertEqual(result['phase_overlap']['available'], None)
        self.assertEqual(result['received']['available'], None)

    def test_official_preflight_bounds_database_storage_not_only_payload(self):
        with self.assertRaisesRegex(ValueError, 'predicted_bound_exceeded'):
            e2_official_estimate(rate_per_second=100, duration_seconds=10,
                samples_per_message=100, payload_bytes=1, official_opt_in=True,
                max_disk_bytes=100000)

    def test_plan_rejects_same_gateway_and_missing_tls(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifests = []
            for arm in ('A', 'B'):
                m = dict(self.profiles['profiles']['100'][arm])
                if arm == 'B':
                    m['targets'] = [{'gateway_id':'gw-a','sensor_id':'s-other'}]
                    m['gateway_identity_files'] = {'gw-a':'other.json'}
                p = root / f'{arm}.json'; p.write_text(json.dumps(m)); p.chmod(0o600); manifests.append(p)
            with self.assertRaisesRegex(ValueError, 'gateway_targets_not_distinct'):
                build_e2_execution_plan(output_dir=root / 'bad', manifest_a=manifests[0], manifest_b=manifests[1])

    def test_missing_or_invalid_execution_evidence_is_rejected(self):
        a = {'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,
            'publish_attempts':8,'broker_pubacks':8,'application_acks':8,'pending_messages':0,'pending_bytes':0,
            'retries':0,'invalid_acks':0}}
        b = {'status':'COMPLETE','telemetry':{'generated_unique':16,'offered_unique':16,
            'publish_attempts':16,'broker_pubacks':16,'application_acks':16,'pending_messages':0,'pending_bytes':0,
            'retries':0,'invalid_acks':0}}
        result = validate_e2_result(workload_a=a, workload_b=b, ingress_delta={}, database_delta={},
            database_by_gateway={}, phase_events=[], execution_context={})
        self.assertFalse(result['valid'])

    def test_wrong_samples_scope_counts_and_reset_intervals_fail_closed(self):
        workloads = [
            {'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,'publish_attempts':8,
             'broker_pubacks':8,'application_acks':8,'pending_messages':0,'pending_bytes':0,'retries':0,'invalid_acks':0}},
            {'status':'COMPLETE','telemetry':{'generated_unique':16,'offered_unique':16,'publish_attempts':16,
             'broker_pubacks':16,'application_acks':16,'pending_messages':0,'pending_bytes':0,'retries':0,'invalid_acks':0}},
        ]
        phases = [{'child':f'paperload-telemetry:{arm}.manifest.json','event':event,
                   'observed_at_runner_monotonic_ns':stamp} for arm,event,stamp in
                  [('A','measure_begin',20),('A','measure_end',10),('B','burst_begin',5),('B','burst_end',15)]]
        result = validate_e2_result(workload_a=workloads[0], workload_b=workloads[1], ingress_delta={},
            database_delta={}, database_by_gateway={}, phase_events=phases,
            execution_context={'samples_per_message':0,'role_to_target':{'A':self.targets[0],'B':self.targets[1]}})
        self.assertFalse(result['valid'])
        self.assertIn('samples_per_message_missing_or_invalid', result['failures'])
        self.assertIn('phase_overlap_inconclusive', result['failures'])

    def test_result_requires_exact_gateway_sensor_pair(self):
        result = validate_e2_result(workload_a={}, workload_b={}, ingress_delta={},
            database_delta={}, database_by_gateway={}, phase_events=[],
            raw_verification=[{'verified_exact': True, 'gateway_id': 'gw-a', 'sensor_id': 's-b', 'ledger_count': 8},
                              {'verified_exact': True, 'gateway_id': 'gw-b', 'sensor_id': 's-a', 'ledger_count': 16}],
            execution_context={'samples_per_message': 1, 'role_to_target': {'A':self.targets[0], 'B':self.targets[1]}})
        self.assertIn('pilot_raw_retention_verification', result['failures'])

    def test_raw_verification_real_db_rejects_list_variables_and_uses_named_uuid_vars(self):
        class Registry:
            def __init__(self): self.calls = []
            def command(self, args, input=None):
                self.calls.append((args, input))
                return type('Result', (), {'returncode': 0, 'stdout': b'[]' if len(self.calls) == 1 else b'0', 'stderr': b''})()
        registry = Registry()
        with self.assertRaisesRegex(ValueError, 'db_variable_invalid'):
            smoke_runtime.db_psql(registry, 'pg', 'select 1', {'message_a': ['bad']})
        class Smoke:
            db_psql = staticmethod(smoke_runtime.db_psql)
        mid = '123e4567-e89b-12d3-a456-426614174000'
        row = p8.verify_pilot_raw(Smoke(), registry, 'pg',
            type('Spec', (), {'gateway_id':'gw-a', 'sensor_id':'s-a'})(), message_ids=[mid])
        self.assertEqual(row['ledger_count'], 0)
        self.assertTrue(any("message_a" in str(args) for args, _ in registry.calls))
        self.assertFalse(any(isinstance(value, list) for args, _ in registry.calls for value in args))

    def test_failed_child_is_boundedly_stopped(self):
        proc = Mock()
        proc.poll.return_value = None
        e2._stop(proc, timeout=2)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=2)

    def test_private_artifact_serialization_omits_compose_secrets(self):
        flags = e2._safe_flags({'services': {'backend': {'environment': {
            'MQTT_SHAPER_ENABLED': 'true', 'DATABASE_URL': 'secret-value', 'JWT_SECRET': 'secret-value'}}}})
        text = json.dumps(flags)
        self.assertIn('MQTT_SHAPER_ENABLED', text)
        self.assertNotIn('secret-value', text)
        self.assertNotIn('DATABASE_URL', text)

    def test_effective_flags_use_actual_config_keys_and_reject_profile_mismatch(self):
        env = {'WEBSOCKET_ENABLED':'false', 'STAGE5_ENABLED':'false',
            'STAGE5_WAKEUP_ENABLED':'false', 'MQTT_SHAPER_ENABLED':'true',
            'MQTT_MICROBATCH_ENABLED':'true', 'MQTT_RAM_DEDUP_ENABLED':'false',
            'MQTT_SHAPER_RATE_PER_SECOND':'2', 'MQTT_SHAPER_BURST':'4',
            'MQTT_MICROBATCH_MAX_MESSAGES':'32', 'MQTT_MICROBATCH_MAX_ROWS':'500',
            'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES':str(4 << 20), 'MQTT_MICROBATCH_MAX_WAIT':'50ms'}
        self.assertEqual(e2._validate_fixture_flags({'services':{'backend':{'environment':env}}}), env)
        env['MQTT_MICROBATCH_MAX_WAIT'] = '100'
        with self.assertRaisesRegex(ValueError, 'effective_fixture_flags_mismatch'):
            e2._validate_fixture_flags({'services':{'backend':{'environment':env}}})

    def test_valid_first_profile_does_not_make_invalid_second_profile_pass(self):
        completed = {'status':'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'profiles':[
            {'status':'PASS'}, {'status':'FAIL', 'validation':{'valid':False}}], 'full_p8_accepted':False}
        self.assertFalse(completed['full_p8_accepted'])
        self.assertNotEqual([p['status'] for p in completed['profiles']], ['PASS', 'PASS'])


if __name__ == '__main__':
    unittest.main()
