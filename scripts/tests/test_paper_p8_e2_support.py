import copy
import hashlib
import json
import os
import platform
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from pathlib import Path

from scripts import paper_p8_bundle

from scripts.tests.paper_p8_e2_support import (
    build_e2_execution_plan, e2_fixture_profile, e2_official_estimate, e2_pilot_manifests,
    e2_official_manifests,
    validate_e2_result,
    validate_e2_equal_samples_result,
    verify_e2_equal_samples_raw,
    build_e2_equal_samples_context,
    validate_e2_equal_samples_context,
    calculate_e2_equal_samples_expected_counts,
    calculate_e2_equal_samples_context_sha256,
    e2_equal_samples_logical_stream,
    e2_equal_samples_manifests,
    synthetic_value,
    E2_EQUAL_SAMPLES_CONTEXT_KEYS,
    E2_EQUAL_SAMPLES_MANIFEST_KEYS,
    MAX_CONTEXT_AGE_SECONDS_BOUND,
    prepare_remote,
)
from scripts.tests import paper_p8_e2_support as e2
from scripts.tests import paper_p8_support as p8
from scripts.tests import native_compose_smoke as smoke_runtime
from scripts import paper_p8_client


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
            self.assertEqual(pair['A']['run_id'], f'e2-{samples}')
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

    def test_official_schedule_splits_total_rate_and_accounts_both_sample_sizes(self):
        for samples, expected_raw in ((100, 100_000), (1, 1_000)):
            manifests = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
                'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost', 'mqtt_tls_ca_file':'ca'}},
                targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
                telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=samples,
                offered_rate=100, duration_seconds=10, burst_messages=7)
            pair = manifests['profiles'][str(samples)]
            self.assertEqual(pair['A']['traffic']['rate_per_second'] +
                             pair['B']['traffic']['rate_per_second'], 100)
            self.assertEqual(manifests['raw_rows_per_profile'], expected_raw)
            self.assertEqual(pair['B']['traffic']['burst_messages'], 7)
            self.assertEqual(pair['A']['traffic']['burst_messages'], 0)

    def test_official_plan_requires_optin_and_rejects_oversize_schedule(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pair = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
                'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost', 'mqtt_tls_ca_file':'ca'}},
                targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
                telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=1,
                offered_rate=100, duration_seconds=10)
            paths=[]
            for arm in ('A','B'):
                path=root/f'{arm}.json'; path.write_text(json.dumps(pair['profiles']['1'][arm])); path.chmod(0o600); paths.append(path)
            with self.assertRaisesRegex(ValueError, 'official_profile_requires_opt_in'):
                build_e2_execution_plan(output_dir=root/'no-opt', manifest_a=paths[0], manifest_b=paths[1], profile='official')
            plan=build_e2_execution_plan(output_dir=root/'yes', manifest_a=paths[0], manifest_b=paths[1], profile='official', official_opt_in=True)
            self.assertEqual(plan['expected_counts'], {'unique':1000,'raw':1000,'temporal':1000,'ack':1000})
            bad=json.loads(paths[1].read_text()); bad['traffic']['total_rate_per_second']=20000
            paths[1].write_text(json.dumps(bad))
            with self.assertRaisesRegex(ValueError, 'distribution_mismatch'):
                build_e2_execution_plan(output_dir=root/'bad', manifest_a=paths[0], manifest_b=paths[1], profile='official', official_opt_in=True)

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

    def test_official_accounting_uses_declared_nonpilot_counts_and_keeps_retries(self):
        pair = e2_official_manifests(common={'tls': {'https_endpoint':'https://localhost',
            'https_ca_file':'ca','mqtt_tls_endpoint':'mqtts://localhost','mqtt_tls_ca_file':'ca'}},
            targets=self.targets, gateway_identity_files={'gw-a':'a','gw-b':'b'},
            telemetry_identity_files={'A':'ia','B':'ib'}, samples_per_message=100,
            offered_rate=100, duration_seconds=1, burst_messages=3)['profiles']['100']
        role_counts = {'A':50,'B':53}
        def child(arm, retry=0):
            n=role_counts[arm]
            return {'status':'COMPLETE','telemetry':{'generated_unique':n,'offered_unique':n,
                'publish_attempts':n+retry,'broker_pubacks':n+retry,'application_acks':n,
                'pending_messages':0,'pending_bytes':0,'retries':retry,'invalid_acks':0}}
        ingress = {'available':True,'delta':{'Received':105,'Accepted':104,'Committed':103,
            'DuplicateCommitted':1,'SamplesNew':10300,'ACKPublished':104,
            'ShaperRateLimited':2,**dict.fromkeys(('RejectedNotRunning','RejectedStopping',
            'RejectedRetained','RejectedOversized','RejectedInvalidTopic','QueueFull','ByteBudgetFull'),0)}}
        phases=[{'child':f'paperload-telemetry:{arm}.manifest.json','event':event,
            'observed_at_runner_monotonic_ns':stamp} for arm,event,stamp in
            [('A','measure_begin',10),('A','measure_end',100),('B','burst_begin',50),('B','burst_end',70)]]
        result=validate_e2_result(workload_a=child('A',2),workload_b=child('B',1),
            ingress_delta=ingress,database_delta={'ledger':103,'raw':10300,'temporal':103,'configuration':0},
            database_by_gateway={'gw-a':{'ledger':50,'raw':5000,'temporal':50,'configuration':0},
                'gw-b':{'ledger':53,'raw':5300,'temporal':53,'configuration':0}},
            phase_events=phases,raw_verification=[{'count_verified':True,'gateway_id':'gw-a','sensor_id':'s-a','ledger_count':50,'raw_count':5000},
                {'count_verified':True,'gateway_id':'gw-b','sensor_id':'s-b','ledger_count':53,'raw_count':5300}],
            execution_context={'profile':'official','samples_per_message':100,
                'role_to_target':{'A':self.targets[0],'B':self.targets[1]},'role_manifests':pair})
        self.assertTrue(result['valid'],result['failures'])
        self.assertEqual(result['database_expected']['ledger'],103)
        self.assertEqual(result['actual_accounting']['publish_attempts'],106)
        self.assertEqual(result['actual_accounting']['duplicate_committed'],1)
        missing_ack=child('A',2)
        missing_ack['telemetry']['application_acks'] -= 1
        rejected=validate_e2_result(workload_a=missing_ack,workload_b=child('B',1),
            ingress_delta=ingress,database_delta={'ledger':103,'raw':10300,'temporal':103,'configuration':0},
            database_by_gateway={'gw-a':{'ledger':50,'raw':5000,'temporal':50,'configuration':0},
                'gw-b':{'ledger':53,'raw':5300,'temporal':53,'configuration':0}},
            phase_events=phases,raw_verification=[{'count_verified':True,'gateway_id':'gw-a','sensor_id':'s-a','ledger_count':50,'raw_count':5000},
                {'count_verified':True,'gateway_id':'gw-b','sensor_id':'s-b','ledger_count':53,'raw_count':5300}],
            execution_context={'profile':'official','samples_per_message':100,
                'role_to_target':{'A':self.targets[0],'B':self.targets[1]},'role_manifests':pair})
        self.assertFalse(rejected['valid'])
        self.assertIn('A_application_acks',rejected['failures'])

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

    def test_remote_phase_event_projection_preserves_only_bounded_arm_source(self):
        events = [{'child':'paperload-telemetry:A.manifest.json','event':'measure_begin',
                   'observed_at_runner_monotonic_ns':10},
                  {'child':'paperload-telemetry:B.manifest.json','event':'burst_begin',
                   'observed_at_runner_monotonic_ns':20}]
        projected = paper_p8_client._safe_phase_events(events)
        self.assertEqual([row['child'] for row in projected], [row['child'] for row in events])
        with self.assertRaisesRegex(ValueError, 'phase event schema rejected'):
            paper_p8_client._safe_phase_events([{'child':'/private/path', 'event':'measure_begin'}])

    def test_e2_runner_phase_events_use_runtime_child_names_and_elapsed_field(self):
        events = [{'child':'paperload-telemetry:A.json','event':'measure_begin',
                   'child_elapsed_ns':123,'observed_at_runner_monotonic_ns':456,
                   'clock_qualification':'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
                   'configured_phases':{'measure_seconds':8},
                   'counters':{'generated_unique':4}}]
        projected = paper_p8_client._safe_phase_events(events)
        self.assertEqual(projected[0]['child'], 'paperload-telemetry:A.json')
        self.assertEqual(projected[0]['elapsed_ns'], 123)
        self.assertNotIn('child_elapsed_ns', projected[0])

    def test_e2_phase_overlap_accepts_actual_runner_child_names(self):
        events = []
        for arm, names, start in (
                ('A', ('measure_begin', 'measure_end'), 100),
                ('B', ('burst_begin', 'burst_end'), 150)):
            for offset, name in enumerate(names):
                events.append({'child':f'paperload-telemetry:{arm}.json', 'event':name,
                               'observed_at_runner_monotonic_ns':start + offset * 100})
        a = e2._phase_intervals(events, 'A')
        b = e2._phase_intervals(events, 'B')
        self.assertGreater(min(a['measure_end'], b['burst_end']) -
                           max(a['measure_begin'], b['burst_begin']), 0)

    def test_e2_remote_prepare_rejects_unselected_sample_profile(self):
        with self.assertRaisesRegex(ValueError, 'samples_profile_rejected'):
            e2.prepare_remote(None, None, None, None, None, None, None,
                session_id='a'*32, samples_per_message=10, output_root='unused', bundle_root='unused')

    def test_equal_samples_context_defaults(self):
        ctx = build_e2_equal_samples_context()
        self.assertEqual(ctx['schema_version'], 1)
        self.assertEqual(ctx['workload_type'], 'equal_samples')
        self.assertEqual(ctx['seed'], 84)
        self.assertEqual(ctx['total_samples'], 6000)
        self.assertEqual(ctx['sample_rate'], 100)
        self.assertEqual(ctx['duration_seconds'], 60)
        self.assertEqual(ctx['logical_producers'], ['A', 'B'])
        self.assertEqual(ctx['samples_per_producer'], 3000)
        self.assertEqual(ctx['producer_sample_rate'], 50)
        self.assertEqual(ctx['sample_interval_us'], 20000)
        self.assertEqual(ctx['stream_sequence_start'], 0)
        self.assertEqual(ctx['max_context_age_seconds'], 86400)
        self.assertIn('timestamp_anchor', ctx)
        self.assertTrue(ctx['timestamp_anchor'].endswith('Z'))
        self.assertIn('context_sha256', ctx)
        self.assertEqual(len(ctx['context_sha256']), 64)
        self.assertEqual(ctx['context_sha256'], calculate_e2_equal_samples_context_sha256(ctx))
        self.assertEqual(set(ctx.keys()), E2_EQUAL_SAMPLES_CONTEXT_KEYS)
        validated = validate_e2_equal_samples_context(ctx)
        self.assertEqual(validated, ctx)

    def test_equal_samples_context_boundaries(self):
        ctx_min_dur = build_e2_equal_samples_context(total_samples=200, sample_rate=20)
        self.assertEqual(ctx_min_dur['duration_seconds'], 10)
        self.assertEqual(ctx_min_dur['samples_per_producer'], 100)
        self.assertEqual(ctx_min_dur['sample_interval_us'], 100000)
        validate_e2_equal_samples_context(ctx_min_dur)

        ctx_max_dur = build_e2_equal_samples_context(total_samples=24000, sample_rate=200)
        self.assertEqual(ctx_max_dur['duration_seconds'], 120)
        self.assertEqual(ctx_max_dur['samples_per_producer'], 12000)
        self.assertEqual(ctx_max_dur['sample_interval_us'], 10000)
        validate_e2_equal_samples_context(ctx_max_dur)

        ctx_min_rate = build_e2_equal_samples_context(total_samples=200, sample_rate=2)
        self.assertEqual(ctx_min_rate['duration_seconds'], 100)
        self.assertEqual(ctx_min_rate['sample_interval_us'], 1000000)
        validate_e2_equal_samples_context(ctx_min_rate)

        with self.assertRaisesRegex(ValueError, 'duration_seconds_out_of_bounds'):
            build_e2_equal_samples_context(total_samples=60000, sample_rate=200)

    def test_equal_samples_context_invalid_inputs(self):
        for bad in (6000.0, 100.0, 84.0):
            with self.assertRaises(ValueError):
                build_e2_equal_samples_context(total_samples=bad)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=100.0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=84.0)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(total_samples=True)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=False)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=True)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(total_samples=0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(total_samples=-200)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(sample_rate=-2)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=-1)

        with self.assertRaisesRegex(ValueError, 'total_samples_divisible_by_200'):
            build_e2_equal_samples_context(total_samples=6100)

        with self.assertRaisesRegex(ValueError, 'total_samples_max_exceeded'):
            build_e2_equal_samples_context(total_samples=60200)

        with self.assertRaisesRegex(ValueError, 'sample_rate_must_be_even'):
            build_e2_equal_samples_context(sample_rate=99)

        with self.assertRaisesRegex(ValueError, 'sample_rate_out_of_bounds'):
            build_e2_equal_samples_context(sample_rate=1)
        with self.assertRaisesRegex(ValueError, 'sample_rate_out_of_bounds'):
            build_e2_equal_samples_context(sample_rate=202)

        with self.assertRaisesRegex(ValueError, 'duration_must_be_integer'):
            build_e2_equal_samples_context(total_samples=6000, sample_rate=70)

        with self.assertRaisesRegex(ValueError, 'duration_seconds_out_of_bounds'):
            build_e2_equal_samples_context(total_samples=400, sample_rate=100)
        with self.assertRaisesRegex(ValueError, 'duration_seconds_out_of_bounds'):
            build_e2_equal_samples_context(total_samples=16000, sample_rate=100)

        with self.assertRaisesRegex(ValueError, 'sample_interval_us_must_be_integer'):
            build_e2_equal_samples_context(total_samples=1400, sample_rate=14)

        with self.assertRaisesRegex(ValueError, 'unknown_context_parameters'):
            build_e2_equal_samples_context(extra_parameter=123)

        # Validation rejects unknown or missing keys in context dict
        valid_ctx = build_e2_equal_samples_context()
        tampered_unknown = dict(valid_ctx)
        tampered_unknown['bogus_key'] = 'bad'
        with self.assertRaisesRegex(ValueError, 'unknown_context_keys'):
            validate_e2_equal_samples_context(tampered_unknown)

        tampered_missing = dict(valid_ctx)
        del tampered_missing['seed']
        with self.assertRaisesRegex(ValueError, 'missing_context_keys'):
            validate_e2_equal_samples_context(tampered_missing)

    def test_equal_samples_context_anchor_and_freshness(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        anchor_valid = '2026-10-31T11:58:00Z'
        ctx = build_e2_equal_samples_context(timestamp_anchor=anchor_valid, now_utc=fixed_now)
        validate_e2_equal_samples_context(ctx, now_utc=fixed_now)

        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00+07:00', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00', now_utc=fixed_now)

        with self.assertRaisesRegex(ValueError, 'anchor_year_out_of_bounds'):
            build_e2_equal_samples_context(timestamp_anchor='0000-10-31T11:58:00Z', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_year_out_of_bounds'):
            build_e2_equal_samples_context(timestamp_anchor='10000-01-01T00:00:00Z', now_utc=fixed_now)

        anchor_future_stream = '2026-10-31T11:59:30Z'
        with self.assertRaisesRegex(ValueError, 'equal_samples_stream_not_in_past'):
            build_e2_equal_samples_context(timestamp_anchor=anchor_future_stream, now_utc=fixed_now)

        with self.assertRaisesRegex(ValueError, 'equal_samples_stream_not_in_past'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T12:05:00Z', now_utc=fixed_now)

        old_anchor = '2026-10-29T10:00:00Z'
        ctx_old = build_e2_equal_samples_context(
            timestamp_anchor=old_anchor,
            max_context_age_seconds=86400,
            now_utc=datetime(2026, 10, 29, 12, 0, 0, tzinfo=timezone.utc),
        )
        ctx_old_copy = json.loads(json.dumps(ctx_old))
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_expired'):
            validate_e2_equal_samples_context(ctx_old, now_utc=fixed_now)
        self.assertEqual(ctx_old, ctx_old_copy)

    def test_equal_samples_context_hash_tampering(self):
        ctx = build_e2_equal_samples_context()
        validate_e2_equal_samples_context(ctx)

        tampered = dict(ctx)
        tampered['seed'] = 85
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            validate_e2_equal_samples_context(tampered)

        tampered = dict(ctx)
        tampered['total_samples'] = 4000
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            validate_e2_equal_samples_context(tampered)

        tampered = dict(ctx)
        tampered['context_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            validate_e2_equal_samples_context(tampered)

    def test_equal_samples_expected_counts(self):
        ctx = build_e2_equal_samples_context()

        c1 = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message=1)
        self.assertEqual(c1['samples_per_message'], 1)
        self.assertEqual(c1['total_samples'], 6000)
        self.assertEqual(c1['total_messages'], 6000)
        self.assertEqual(c1['total_raw_rows'], 6000)
        self.assertEqual(c1['total_ledger_rows'], 6000)
        self.assertEqual(c1['total_temporal_rows'], 6000)
        self.assertEqual(c1['total_configuration_delta'], 0)
        for role in ('A', 'B'):
            self.assertEqual(c1['per_role'][role]['samples'], 3000)
            self.assertEqual(c1['per_role'][role]['messages'], 3000)
            self.assertEqual(c1['per_role'][role]['raw_rows'], 3000)
            self.assertEqual(c1['per_role'][role]['ledger_rows'], 3000)
            self.assertEqual(c1['per_role'][role]['temporal_rows'], 3000)
            self.assertEqual(c1['per_role'][role]['configuration_delta'], 0)

        c100 = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message=100)
        self.assertEqual(c100['samples_per_message'], 100)
        self.assertEqual(c100['total_samples'], 6000)
        self.assertEqual(c100['total_messages'], 60)
        self.assertEqual(c100['total_raw_rows'], 6000)
        self.assertEqual(c100['total_ledger_rows'], 60)
        self.assertEqual(c100['total_temporal_rows'], 60)
        self.assertEqual(c100['total_configuration_delta'], 0)
        for role in ('A', 'B'):
            self.assertEqual(c100['per_role'][role]['samples'], 3000)
            self.assertEqual(c100['per_role'][role]['messages'], 30)
            self.assertEqual(c100['per_role'][role]['raw_rows'], 3000)
            self.assertEqual(c100['per_role'][role]['ledger_rows'], 30)
            self.assertEqual(c100['per_role'][role]['temporal_rows'], 30)
            self.assertEqual(c100['per_role'][role]['configuration_delta'], 0)

        for bad_s in (10, 50, True, False, 1.0, 100.0, '100', None):
            with self.assertRaises(ValueError):
                calculate_e2_equal_samples_expected_counts(ctx, samples_per_message=bad_s)

    def test_equal_samples_logical_stream_independent_of_batching(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00Z', now_utc=fixed_now)

        for role in ('A', 'B'):
            stream_s1 = e2_equal_samples_logical_stream(ctx, role=role, samples_per_message=1)
            stream_s100 = e2_equal_samples_logical_stream(ctx, role=role, samples_per_message=100)

            flat_s1 = [sample for msg in stream_s1 for sample in msg['samples']]
            flat_s100 = [sample for msg in stream_s100 for sample in msg['samples']]

            self.assertEqual(len(flat_s1), 3000)
            self.assertEqual(len(flat_s100), 3000)
            self.assertEqual(flat_s1, flat_s100)

            self.assertEqual(flat_s1[0]['sequence'], 0)
            self.assertAlmostEqual(flat_s1[0]['value'], synthetic_value(0))
            self.assertEqual(flat_s1[-1]['sequence'], 2999)
            self.assertAlmostEqual(flat_s1[-1]['value'], synthetic_value(2999))

    def test_legacy_e2_pilot_manifests_regression(self):
        legacy = e2_pilot_manifests(common={'tls': {'endpoint': 'test'}},
                                    targets=self.targets,
                                    gateway_identity_files={'gw-a': 'a.json', 'gw-b': 'b.json'},
                                    telemetry_identity_files={'A': 'a.json', 'B': 'b.json'})
        self.assertEqual(legacy['unique_messages_per_profile'], 24)
        self.assertEqual(legacy['shared_seed'], 84)
        self.assertEqual(set(legacy['profiles'].keys()), {'1', '100'})
        self.assertEqual(legacy['profiles']['100']['A']['samples_per_message'], 100)
        self.assertEqual(legacy['profiles']['100']['A']['phases']['measure_seconds'], 8)
        self.assertEqual(legacy['profiles']['100']['B']['phases']['measure_seconds'], 4)
        self.assertEqual(legacy['profiles']['100']['B']['traffic']['burst_messages'], 12)

    def test_equal_samples_context_tightened_seed_bound(self):
        max_i64 = (1 << 63) - 1
        ctx_max_seed = build_e2_equal_samples_context(seed=max_i64)
        self.assertEqual(ctx_max_seed['seed'], max_i64)
        validate_e2_equal_samples_context(ctx_max_seed)

        ctx_zero_seed = build_e2_equal_samples_context(seed=0)
        self.assertEqual(ctx_zero_seed['seed'], 0)
        validate_e2_equal_samples_context(ctx_zero_seed)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=-1)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=1 << 63)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(seed=1 << 64)

        bad_seed_ctx = dict(ctx_max_seed)
        bad_seed_ctx['seed'] = 1 << 63
        bad_seed_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_seed_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_seed_ctx)

    def test_equal_samples_context_tightened_age_and_ttl(self):
        ctx_max_ttl = build_e2_equal_samples_context(max_context_age_seconds=MAX_CONTEXT_AGE_SECONDS_BOUND)
        self.assertEqual(ctx_max_ttl['max_context_age_seconds'], MAX_CONTEXT_AGE_SECONDS_BOUND)
        validate_e2_equal_samples_context(ctx_max_ttl)

        ctx_small_ttl = build_e2_equal_samples_context(max_context_age_seconds=70)
        self.assertEqual(ctx_small_ttl['max_context_age_seconds'], 70)
        validate_e2_equal_samples_context(ctx_small_ttl)

        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(max_context_age_seconds=0)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(max_context_age_seconds=-10)
        with self.assertRaises(ValueError):
            build_e2_equal_samples_context(max_context_age_seconds=MAX_CONTEXT_AGE_SECONDS_BOUND + 1)

        bad_ttl_ctx = dict(ctx_max_ttl)
        bad_ttl_ctx['max_context_age_seconds'] = 0
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_ttl_ctx)

        bad_ttl_ctx['max_context_age_seconds'] = -100
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_ttl_ctx)

        bad_ttl_ctx['max_context_age_seconds'] = MAX_CONTEXT_AGE_SECONDS_BOUND + 1
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            validate_e2_equal_samples_context(bad_ttl_ctx)

    def test_equal_samples_context_tightened_anchor_syntax_matches_go(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        valid_anchor_frac = '2026-10-31T11:58:00.123456Z'
        ctx_frac = build_e2_equal_samples_context(timestamp_anchor=valid_anchor_frac, now_utc=fixed_now)
        self.assertEqual(ctx_frac['timestamp_anchor'], valid_anchor_frac)
        validate_e2_equal_samples_context(ctx_frac, now_utc=fixed_now)

        # Fraction with 7 digits (exceeds Go's 6 digits microsecond precision)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00.1234567Z', now_utc=fixed_now)

        # Empty fraction
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00.Z', now_utc=fixed_now)

        # Invalid hour 24
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T24:00:00Z', now_utc=fixed_now)

        # Invalid minute 60, second 60
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:60:00Z', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:60Z', now_utc=fixed_now)

        # Lowercase z and missing z
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00z', now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'anchor_must_be_utc_z'):
            build_e2_equal_samples_context(timestamp_anchor='2026-10-31T11:58:00', now_utc=fixed_now)

    def test_equal_samples_expected_counts_rejects_malformed_without_wall_clock(self):
        # 1. Historical context with past anchor (e.g. from year 2024, age > 86400s)
        historical_anchor = '2024-01-01T00:00:00Z'
        historical_ctx = build_e2_equal_samples_context(
            timestamp_anchor=historical_anchor,
            max_context_age_seconds=86400,
            now_utc=datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
        )
        # Expected counts should SUCCEED on historical context today without wall-clock failure
        counts = calculate_e2_equal_samples_expected_counts(historical_ctx, samples_per_message=1)
        self.assertEqual(counts['total_samples'], 6000)
        self.assertEqual(counts['total_messages'], 6000)
        self.assertEqual(counts['per_role']['A']['messages'], 3000)

        # 2. Malformed contexts must be REJECTED by expected_counts
        # Non-dict context
        with self.assertRaises(ValueError):
            calculate_e2_equal_samples_expected_counts('not-a-dict', samples_per_message=1)

        # Tampered context hash
        tampered_ctx = dict(historical_ctx)
        tampered_ctx['seed'] = 999
        with self.assertRaisesRegex(ValueError, 'equal_samples_context_hash_mismatch'):
            calculate_e2_equal_samples_expected_counts(tampered_ctx, samples_per_message=1)

        # Missing key
        missing_key_ctx = dict(historical_ctx)
        del missing_key_ctx['logical_producers']
        with self.assertRaisesRegex(ValueError, 'missing_context_keys'):
            calculate_e2_equal_samples_expected_counts(missing_key_ctx, samples_per_message=1)

        # Nonpositive TTL
        bad_ttl_ctx = dict(historical_ctx)
        bad_ttl_ctx['max_context_age_seconds'] = -5
        bad_ttl_ctx['context_sha256'] = calculate_e2_equal_samples_context_sha256(bad_ttl_ctx)
        with self.assertRaises(ValueError):
            calculate_e2_equal_samples_expected_counts(bad_ttl_ctx, samples_per_message=1)

    def test_e2_equal_samples_manifests_defaults_and_exact_schema(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(
            timestamp_anchor='2026-10-31T11:58:00Z',
            now_utc=fixed_now,
            seed=42,
        )
        common = {
            'mode': 'remote',
            'run_id': 'test-e2-equal',
            'bundle_path': '/var/tmp/bundle',
            'publish_timeout_ms': 3000,
            'tls': {
                'https_endpoint': 'https://localhost:8443',
                'https_ca_file': 'ca.crt',
                'mqtt_tls_endpoint': 'mqtts://localhost:8883',
                'mqtt_tls_ca_file': 'mqtt-ca.crt',
                'mqtt_tls_server_name': 'localhost',
            },
        }
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}

        manifest_bundle = e2_equal_samples_manifests(
            context=ctx,
            common=common,
            targets=self.targets,
            gateway_identity_files=gw_files,
            telemetry_identity_files=tele_files,
            now_utc=fixed_now,
        )

        self.assertEqual(manifest_bundle['workload_type'], 'equal_samples')
        self.assertEqual(manifest_bundle['shared_seed'], 42)
        self.assertEqual(manifest_bundle['context'], ctx)
        self.assertTrue(manifest_bundle['same_topology_security_quota_worker_policy'])
        self.assertEqual(manifest_bundle['execution'], 'NOT RUN')
        self.assertEqual(set(manifest_bundle['profiles'].keys()), {'1', '100'})
        self.assertEqual(set(manifest_bundle['expected_counts'].keys()), {'1', '100'})

        # Duration = 60, Drain = 20, default Grace = 10 -> Wall = 90
        expected_wall = 60 + 20 + 10

        for sample_str, expected_msg_count, expected_rate in (('1', 3000, 50), ('100', 30, 0.5)):
            pair = manifest_bundle['profiles'][sample_str]
            self.assertEqual(set(pair.keys()), {'A', 'B'})

            for arm in ('A', 'B'):
                m = pair[arm]
                # Legacy shape check
                self.assertEqual(m['schema_version'], 1)
                self.assertEqual(m['experiment'], 'E2')
                self.assertEqual(m['arm'], arm)
                self.assertEqual(m['profile'], 'pilot')
                self.assertEqual(m['roles'], ['telemetry'])
                self.assertEqual(m['seed'], 42)
                self.assertEqual(m['samples_per_message'], int(sample_str))
                target_idx = 0 if arm == 'A' else 1
                self.assertEqual(m['targets'], [self.targets[target_idx]])
                self.assertEqual(m['subscriptions'], [])
                self.assertEqual(m['topology'], {'gateways': 1, 'sensors_per_gateway': 1, 'users': 0, 'viewers': 0})
                self.assertEqual(m['durability_mode'], 'synthetic-load')
                self.assertEqual(m['retry_interval_ms'], 500)
                self.assertEqual(m['run_id'], f'test-e2-equal-{sample_str}')

                # Steady producers, zero warmup, zero cooldown, zero burst
                self.assertEqual(m['traffic']['burst_messages'], 0)
                self.assertEqual(m['traffic']['ramp_seconds'], 0)
                self.assertEqual(m['traffic']['rate_per_second'], expected_rate)

                # Phases: measure=context duration, drain=20, warmup=cooldown=0
                self.assertEqual(m['phases']['warmup_seconds'], 0)
                self.assertEqual(m['phases']['cooldown_seconds'], 0)
                self.assertEqual(m['phases']['measure_seconds'], 60)
                self.assertEqual(m['phases']['drain_seconds'], 20)

                # Limits: fixed safe workers/connections, wall=duration+drain+grace
                self.assertEqual(m['limits']['workers'], 2)
                self.assertEqual(m['limits']['connections'], 1)
                self.assertEqual(m['limits']['retries'], 16)
                self.assertEqual(m['limits']['pending_messages'], 64)
                self.assertEqual(m['limits']['pending_bytes'], 1 << 20)
                self.assertEqual(m['limits']['disk_growth_bytes'], 1 << 24)
                self.assertEqual(m['limits']['wall_seconds'], expected_wall)

                # Target and credential binding
                target_gw = self.targets[target_idx]['gateway_id']
                self.assertEqual(m['gateway_identity_files'], {target_gw: gw_files[target_gw]})
                self.assertEqual(m['identity_files'], {'telemetry': tele_files[arm]})

                # Opt-in equal_samples object
                self.assertIn('equal_samples', m)
                eq = m['equal_samples']
                self.assertEqual(set(eq.keys()), E2_EQUAL_SAMPLES_MANIFEST_KEYS)
                for k in E2_EQUAL_SAMPLES_CONTEXT_KEYS:
                    self.assertEqual(eq[k], ctx[k])
                self.assertEqual(eq['expected_samples'], 3000)
                self.assertEqual(eq['expected_messages'], expected_msg_count)
                self.assertEqual(eq['sample_stream_version'], 1)

    def test_e2_equal_samples_manifests_two_profiles_same_context_and_stream(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(
            timestamp_anchor='2026-10-31T11:58:00Z',
            now_utc=fixed_now,
            seed=84,
        )
        common = {'run_id': 'stream-test'}
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}

        manifest_bundle = e2_equal_samples_manifests(
            context=ctx,
            common=common,
            targets=self.targets,
            gateway_identity_files=gw_files,
            telemetry_identity_files=tele_files,
            now_utc=fixed_now,
        )

        p1 = manifest_bundle['profiles']['1']
        p100 = manifest_bundle['profiles']['100']

        for arm in ('A', 'B'):
            # Same shared seed
            self.assertEqual(p1[arm]['seed'], p100[arm]['seed'])
            self.assertEqual(p1[arm]['seed'], 84)

            # Same canonical equal_samples context
            for k in E2_EQUAL_SAMPLES_CONTEXT_KEYS:
                self.assertEqual(p1[arm]['equal_samples'][k], p100[arm]['equal_samples'][k])

            # Identical logical samples stream
            stream_1 = e2_equal_samples_logical_stream(p1[arm]['equal_samples'], role=arm, samples_per_message=1)
            stream_100 = e2_equal_samples_logical_stream(p100[arm]['equal_samples'], role=arm, samples_per_message=100)

            flat_1 = [s for msg in stream_1 for s in msg['samples']]
            flat_100 = [s for msg in stream_100 for s in msg['samples']]

            self.assertEqual(len(flat_1), 3000)
            self.assertEqual(len(flat_100), 3000)
            self.assertEqual(flat_1, flat_100)

    def test_e2_equal_samples_manifests_validation_and_boundaries(self):
        fixed_now = datetime(2026, 10, 31, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(
            timestamp_anchor='2026-10-31T11:58:00Z',
            now_utc=fixed_now,
        )
        common = {'run_id': 'val-test'}
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}

        # 1. Missing or invalid context
        with self.assertRaises(ValueError):
            e2_equal_samples_manifests(
                context=None, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files)
        with self.assertRaises(ValueError):
            e2_equal_samples_manifests(
                context={}, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files)

        # 2. Targets validation: not 2 targets
        with self.assertRaisesRegex(ValueError, 'e2_requires_two_gateway_targets'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=[self.targets[0]],
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # Targets validation: non-distinct gateway_id
        same_gw_targets = [{'gateway_id': 'gw-a', 'sensor_id': 's-1'}, {'gateway_id': 'gw-a', 'sensor_id': 's-2'}]
        with self.assertRaisesRegex(ValueError, 'e2_requires_two_gateway_targets'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=same_gw_targets,
                gateway_identity_files={'gw-a': 'gw-a.json'}, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # Targets validation: missing sensor_id
        bad_targets = [{'gateway_id': 'gw-a'}, {'gateway_id': 'gw-b', 'sensor_id': 's-b'}]
        with self.assertRaisesRegex(ValueError, 'e2_requires_two_gateway_targets'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=bad_targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # 3. Gateway credentials mismatch
        bad_gw_files = {'gw-a': 'gw-a.json', 'gw-other': 'gw-other.json'}
        with self.assertRaisesRegex(ValueError, 'e2_gateway_credentials_mismatch'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=bad_gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        # 4. Telemetry credentials mismatch
        bad_tele_files = {'A': 'tele-a.json', 'C': 'tele-c.json'}
        with self.assertRaisesRegex(ValueError, 'e2_telemetry_credentials_mismatch'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=bad_tele_files, now_utc=fixed_now)

        # 5. Grace seconds out of bounds
        with self.assertRaisesRegex(ValueError, 'grace_seconds'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files,
                grace_seconds=-1, now_utc=fixed_now)
        with self.assertRaisesRegex(ValueError, 'grace_seconds'):
            e2_equal_samples_manifests(
                context=ctx, common=common, targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files,
                grace_seconds=100, now_utc=fixed_now)

        # 6. Common not a dict
        with self.assertRaises(ValueError):
            e2_equal_samples_manifests(
                context=ctx, common='not-a-dict', targets=self.targets,
                gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

    def test_build_e2_execution_plan_equal_samples_valid(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {
            'mode': 'remote', 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': 'https://127.0.0.1:443', 'https_ca_file': 'ca.pem',
                    'mqtt_tls_endpoint': 'mqtts://127.0.0.1:8883', 'mqtt_tls_ca_file': 'ca.pem',
                    'mqtt_tls_server_name': 'localhost'},
        }
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}
        manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=self.targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for S in ('100', '1'):
                pair = manifests['profiles'][S]
                paths = {}
                for arm in ('A', 'B'):
                    p = root / f'{arm}-{S}.json'
                    p.write_text(json.dumps(pair[arm]))
                    p.chmod(0o600)
                    paths[arm] = p

                plan = build_e2_execution_plan(
                    output_dir=root / f'plan-{S}', manifest_a=paths['A'], manifest_b=paths['B'])
                spec = json.loads(Path(plan['run_spec']).read_text())
                self.assertEqual(len(spec['paperload']), 2)
                self.assertEqual([x['role'] for x in spec['paperload']], ['telemetry', 'telemetry'])
                self.assertEqual(spec['stop_timeout_seconds'], 30)
                # Verify written manifests in plan output
                written_a = json.loads((root / f'plan-{S}/A.manifest.json').read_text())
                written_b = json.loads((root / f'plan-{S}/B.manifest.json').read_text())
                self.assertEqual(written_a['equal_samples']['duration_seconds'], 60)
                self.assertEqual(written_a['limits']['wall_seconds'], 90)
                self.assertNotEqual(written_a['limits']['wall_seconds'], spec['stop_timeout_seconds'])
                self.assertEqual(written_a['equal_samples']['sample_stream_version'], 1)
                self.assertEqual(written_b['equal_samples']['sample_stream_version'], 1)
                self.assertEqual(written_a['equal_samples']['context_sha256'], ctx['context_sha256'])
                self.assertEqual(written_b['equal_samples']['context_sha256'], ctx['context_sha256'])

    def test_build_e2_execution_plan_equal_samples_negatives(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        common = {
            'mode': 'remote', 'publish_timeout_ms': 3000,
            'tls': {'https_endpoint': 'https://127.0.0.1:443', 'https_ca_file': 'ca.pem',
                    'mqtt_tls_endpoint': 'mqtts://127.0.0.1:8883', 'mqtt_tls_ca_file': 'ca.pem',
                    'mqtt_tls_server_name': 'localhost'},
        }
        gw_files = {'gw-a': 'gw-a.json', 'gw-b': 'gw-b.json'}
        tele_files = {'A': 'tele-a.json', 'B': 'tele-b.json'}
        manifests = e2_equal_samples_manifests(
            context=ctx, common=common, targets=self.targets,
            gateway_identity_files=gw_files, telemetry_identity_files=tele_files, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pair = manifests['profiles']['100']

            def write_pair(mod_a=None, mod_b=None):
                a_data = json.loads(json.dumps(pair['A']))
                b_data = json.loads(json.dumps(pair['B']))
                if mod_a:
                    mod_a(a_data)
                if mod_b:
                    mod_b(b_data)
                pa = root / 'a.json'
                pb = root / 'b.json'
                pa.write_text(json.dumps(a_data))
                pb.write_text(json.dumps(b_data))
                pa.chmod(0o600)
                pb.chmod(0o600)
                return pa, pb

            # 1. Context hash mismatch between A and B
            pa, pb = write_pair(mod_b=lambda b: b['equal_samples'].update({'context_sha256': '0' * 64}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p1', manifest_a=pa, manifest_b=pb)

            # 2. Tampered context hash in A
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'context_sha256': 'f' * 64}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p2', manifest_a=pa, manifest_b=pb)

            # 3. Expected count mismatch in A
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'expected_samples': 9999}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p3', manifest_a=pa, manifest_b=pb)

            # 4. Sample stream version != 1
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'sample_stream_version': 2}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p4', manifest_a=pa, manifest_b=pb)

            # 5. Unknown nested key in equal_samples
            pa, pb = write_pair(mod_a=lambda a: a['equal_samples'].update({'unexpected_key': True}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p5', manifest_a=pa, manifest_b=pb)

            # 6. Missing key in equal_samples
            def drop_seed(a):
                a['equal_samples'] = dict(a['equal_samples'])
                del a['equal_samples']['seed']
            pa, pb = write_pair(mod_a=drop_seed)
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p6', manifest_a=pa, manifest_b=pb)

            # 7. S mismatch between A and B (A has 100, B has 1)
            b_s1 = dict(manifests['profiles']['1']['B'])
            pa = root / 'a_s100.json'
            pb = root / 'b_s1.json'
            pa.write_text(json.dumps(pair['A']))
            pb.write_text(json.dumps(b_s1))
            pa.chmod(0o600); pb.chmod(0o600)
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p7', manifest_a=pa, manifest_b=pb)

            # 8. Same gateway target rejected
            pa, pb = write_pair(mod_b=lambda b: (
                b.update({'targets': [{'gateway_id': 'gw-a', 'sensor_id': 's-other'}]}),
                b.update({'gateway_identity_files': {'gw-a': 'gw-a.json'}})))
            with self.assertRaisesRegex(ValueError, 'e2_gateway_targets_not_distinct'):
                build_e2_execution_plan(output_dir=root / 'p8', manifest_a=pa, manifest_b=pb)

            # 9. Limits tampered
            pa, pb = write_pair(mod_a=lambda a: a['limits'].update({'retries': 5}))
            with self.assertRaises(ValueError):
                build_e2_execution_plan(output_dir=root / 'p9', manifest_a=pa, manifest_b=pb)

    def test_prepare_remote_equal_samples_and_steady_burst(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ctx_path = root / 'context.json'
            ctx_path.write_text(json.dumps(ctx))

            # Helper mock setup
            class FakeEndpoint:
                url = 'https://fixture.test:14443'
                ca_file = root / 'https-ca.pem'
            FakeEndpoint.ca_file.write_text('https-ca')

            class FakeSpec:
                def __init__(self, gw, s):
                    self.gateway_id = gw
                    self.sensor_id = s

            class FakeFixture:
                private = root / 'fixture-private'
            FakeFixture.private.mkdir(parents=True)
            (FakeFixture.private / 'certs').mkdir()
            (FakeFixture.private / 'certs/ca.crt').write_text('mqtt-ca')

            smoke = Mock()
            specs = [FakeSpec('gw-a', 's-a'), FakeSpec('gw-b', 's-b')]
            passwords = ['pass-a', 'pass-b']
            smoke.prepare_paper_workload.return_value = (
                'fake-pg', {}, {}, {}, 'ready', {'admin': 'token'}, specs, passwords, None)
            smoke.db_psql.return_value = '24:telemetry_ledger_timestamp'
            def fake_private_write(path, content):
                p = Path(path)
                p.write_text(content)
                p.chmod(0o600)
            smoke.private_write.side_effect = fake_private_write

            registry = Mock()
            registry.resolve.return_value = {
                'NetworkSettings': {'Ports': {'8883/tcp': [{'HostPort': '18883'}]}}}

            # 1. Negative: equal_samples requires context, must fail BEFORE prepare_paper_workload
            smoke.prepare_paper_workload.reset_mock()
            with self.assertRaisesRegex(ValueError, 'equal_samples_requires_context'):
                prepare_remote(
                    smoke, FakeFixture, FakeEndpoint, registry, {}, {}, {},
                    session_id='sess-1', samples_per_message=100,
                    output_root=root / 'out-err', bundle_root=root / 'bnd-err',
                    workload='equal_samples', context_path=None, context=None)
            smoke.prepare_paper_workload.assert_not_called()

            # 2. Negative: invalid context fails before prepare_paper_workload
            smoke.prepare_paper_workload.reset_mock()
            bad_ctx = dict(ctx, total_samples=5999)  # not divisible by 200
            with self.assertRaises(ValueError):
                prepare_remote(
                    smoke, FakeFixture, FakeEndpoint, registry, {}, {}, {},
                    session_id='sess-2', samples_per_message=100,
                    output_root=root / 'out-err2', bundle_root=root / 'bnd-err2',
                    workload='equal_samples', context=bad_ctx)
            smoke.prepare_paper_workload.assert_not_called()

            # 3. Positive: equal_samples valid preparation
            smoke.prepare_paper_workload.reset_mock()
            out_root = root / 'out-eq'
            bnd_root = root / 'bnd-eq'
            test_images = {'backend': 'sha256:' + 'a' * 64, 'gateway': 'sha256:' + 'b' * 64}
            prepared = prepare_remote(
                smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, test_images, {},
                session_id='sess-valid', samples_per_message=100,
                output_root=out_root, bundle_root=bnd_root,
                workload='equal_samples', context=ctx, reference_path='/ref/path.json',
                check_freshness=False)
            smoke.prepare_paper_workload.assert_called_once()
            self.assertEqual(prepared['phase_id'], 'equal-samples-100')
            self.assertEqual(prepared['seed'], ctx['seed'])
            self.assertEqual(prepared['workload'], 'equal_samples')
            self.assertEqual(prepared['context']['context_sha256'], ctx['context_sha256'])
            self.assertIn('expected_counts', prepared)
            self.assertEqual(prepared['expected_counts']['total_samples'], 6000)
            self.assertEqual(prepared['expected_counts']['total_messages'], 60)
            self.assertEqual(prepared['wall_seconds'], 60 + 20 + 10)
            self.assertEqual(prepared['reference_path'], '/ref/path.json')
            self.assertTrue(Path(prepared['bundle']['bundle_root']).is_dir())
            self.assertEqual(prepared.get('images'), test_images)
            self.assertIn('bundle_inventory', prepared)
            self.assertIsInstance(prepared['bundle_inventory'], list)
            self.assertTrue(any(row.get('path') == 'manifests/A.json' for row in prepared['bundle_inventory']))

            # 3b. Regression: prepare_remote defaults to check_freshness=True, rejecting expired context
            smoke.prepare_paper_workload.reset_mock()
            with self.assertRaisesRegex(ValueError, 'equal_samples_context_expired'):
                prepare_remote(
                    smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, test_images, {},
                    session_id='sess-expired', samples_per_message=100,
                    output_root=root / 'out-expired', bundle_root=root / 'bnd-expired',
                    workload='equal_samples', context=ctx)
            smoke.prepare_paper_workload.assert_not_called()

            # 4. Positive: steady_burst legacy preparation (default)
            smoke.prepare_paper_workload.reset_mock()
            out_root_sb = root / 'out-sb'
            bnd_root_sb = root / 'bnd-sb'
            prepared_sb = prepare_remote(
                smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, {}, {},
                session_id='sess-sb', samples_per_message=100,
                output_root=out_root_sb, bundle_root=bnd_root_sb)
            smoke.prepare_paper_workload.assert_called_once()
            self.assertEqual(prepared_sb['phase_id'], 'samples-100')
            self.assertEqual(prepared_sb['seed'], 84)
            self.assertEqual(prepared_sb['workload'], 'steady_burst')
            self.assertNotIn('context', prepared_sb)
            self.assertTrue(Path(prepared_sb['bundle']['bundle_root']).is_dir())

            # Official workload must pass the real portable export/schema path with
            # counts derived from manifests rather than the 24-message pilot.
            out_off = root / 'out-official'
            bnd_off = root / 'bnd-official'
            prepared_off = prepare_remote(
                smoke, FakeFixture, FakeEndpoint, registry, {'mosquitto': 'mosq'}, test_images, {},
                session_id='sess-official', samples_per_message=100,
                output_root=out_off, bundle_root=bnd_off, profile='official',
                official_opt_in=True, offered_rate=100, duration_seconds=10, burst_messages=7)
            portable = json.loads((bnd_off / 'bundle.json').read_text())
            from scripts import paper_p8_bundle
            self.assertTrue(paper_p8_bundle.inspect_bundle(bnd_off)['valid'])
            self.assertEqual(prepared_off['plan']['expected_counts'],
                {'unique':1007,'raw':100700,'temporal':1007,'ack':1007})
            self.assertEqual(portable['workload']['producer_rates_per_second'], {'A':50,'B':50})
            self.assertEqual(portable['workload']['expected_counts'], prepared_off['plan']['expected_counts'])
            with self.assertRaisesRegex(ValueError, 'bundle_workload_metadata_rejected'):
                bad_workload = dict(portable['workload'], offered_rate_per_second=200)
                paper_p8_bundle._validate_e2_official_workload(bad_workload)

    def _create_test_equal_samples_bundle(self, root, ctx, *, binary_bytes=b'binary-123',
                                          session_id='sess-e2', run_id='p8e2-sess-100', arm='e2-test',
                                          phase_id='equal-samples-100', samples_per_message=100,
                                          profile='pilot',
                                          a_mod=None, b_mod=None, summary_mod=None, provenance_mod=None,
                                          generator_resources=None):
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        binary_file = root / 'paperload_bin'
        binary_file.write_bytes(binary_bytes)
        binary_file.chmod(0o700)
        bin_hash = hashlib.sha256(binary_bytes).hexdigest()

        bundle_dir = root / 'result_bundle'
        bundle_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        results_dir = bundle_dir / 'results'
        results_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        (results_dir / 'runner').mkdir(mode=0o700, parents=True, exist_ok=True)
        (results_dir / 'workloads/A').mkdir(mode=0o700, parents=True, exist_ok=True)
        (results_dir / 'workloads/B').mkdir(mode=0o700, parents=True, exist_ok=True)

        exp_counts = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message)
        expected_s = exp_counts['per_role']['A']['samples']
        expected_m = exp_counts['per_role']['A']['messages']

        def make_workload(role):
            mid = '550e8400-e29b-41d4-a716-446655440001' if role == 'A' else '550e8400-e29b-41d4-a716-446655440002'
            gw = 'gw-a' if role == 'A' else 'gw-b'
            s = 's-a' if role == 'A' else 's-b'
            return {
                'status': 'COMPLETE',
                'implementation_status': 'P8.4b telemetry synthetic-load; no durable restart assurance',
                'experiment': 'E2',
                'arm': role,
                'profile': profile,
                'mode': 'remote',
                'run_id': run_id,
                'durability_mode': 'synthetic-load',
                'started_at': '2026-10-31T12:00:00.000000000Z',
                'finished_at': '2026-10-31T12:01:00.000000000Z',
                'configured_phases': {'warmup_seconds': 0, 'measure_seconds': 60, 'drain_seconds': 20, 'cooldown_seconds': 0},
                'telemetry': {
                    'scheduled_opportunities': expected_m, 'generated_unique': expected_m, 'offered_unique': expected_m,
                    'application_acks': expected_m, 'pending_messages': 0, 'pending_bytes': 0,
                    'publish_attempts': expected_m, 'broker_pubacks': expected_m, 'retries': 0, 'invalid_acks': 0,
                },
                'identity_sampling_qualification': 'header_sha256_only; maximum 20 globally; not every payload; not a full-content audit',
                'identity_samples': [{
                    'viewer_id': '',
                    'gateway_id': gw,
                    'sensor_id': s,
                    'message_id': mid,
                    'boot_id': 'boot-1',
                    'first_sequence': '0',
                    'sample_count': 100,
                    'header_sha256': 'a' * 64,
                }],
                'equal_samples': {
                    'context_sha256': ctx['context_sha256'],
                    'workload_type': 'equal_samples',
                    'logical_producer': role,
                    'total_samples': ctx['total_samples'],
                    'sample_rate': ctx['sample_rate'],
                    'duration_seconds': ctx['duration_seconds'],
                    'producer_sample_rate': ctx['producer_sample_rate'],
                    'sample_interval_us': ctx['sample_interval_us'],
                    'samples_per_message': samples_per_message,
                    'expected_samples': expected_s,
                    'expected_messages': expected_m,
                    'generated_samples': expected_s,
                    'generated_messages': expected_m,
                    'offered_unique_samples': expected_s,
                    'offered_unique_messages': expected_m,
                    'application_acks': expected_m,
                    'offered_unique_bytes': 15000,
                    'attempted_bytes': 15000,
                    'schedule_misses': 0,
                    'scheduling_lag_max_ns': 1250000,
                    'completed_on_time': True,
                    'sampling_qualification': 'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only'
                }
            }

        workload_a = make_workload('A')
        if a_mod:
            a_mod(workload_a)
        workload_b = make_workload('B')
        if b_mod:
            b_mod(workload_b)

        gen_m_start = 0.0
        if generator_resources and isinstance(generator_resources, dict):
            gen_m_start = float(generator_resources.get('clock', {}).get('monotonic_start', 0.0))
        b_begin_ns = int((gen_m_start + 0.0) * 1e9) if gen_m_start else 1000
        b_end_ns = int((gen_m_start + 2.0) * 1e9) if gen_m_start else 61000
        a_begin_ns = b_begin_ns if gen_m_start else 1000
        a_end_ns = b_end_ns if gen_m_start else 61000

        run_json = {
            'status': 'COMPLETE',
            'phase_observations': {
                'status': 'observed',
                'events': [
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': a_begin_ns},
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': a_end_ns},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': b_begin_ns},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': b_end_ns}
                ]
            }
        }
        prov_json = {
            'binary_sha256': bin_hash,
            'binary_os': sys.platform,
            'binary_arch': platform.machine()
        }
        if provenance_mod:
            provenance_mod(prov_json)

        files_data = {
            'results/workloads/A/workload.json': json.dumps(workload_a, separators=(',', ':')).encode(),
            'results/workloads/B/workload.json': json.dumps(workload_b, separators=(',', ':')).encode(),
            'results/runner/run.json': json.dumps(run_json, separators=(',', ':')).encode(),
            'results/runner/provenance.json': json.dumps(prov_json, separators=(',', ':')).encode(),
        }
        if generator_resources is not None:
            files_data['results/runner/resources-generator.json'] = json.dumps(generator_resources, separators=(',', ':')).encode()
        inventory = []
        for rel_path, data in files_data.items():
            f_path = bundle_dir / rel_path
            f_path.write_bytes(data)
            f_path.chmod(0o600)
            inventory.append({
                'path': rel_path,
                'size_bytes': len(data),
                'sha256': hashlib.sha256(data).hexdigest()
            })

        bundle_sha256 = 'b' * 64
        bundle_inventory = [{'path': 'input.json', 'sha256': 'a' * 64}]
        manifest_a_hash = '1' * 64
        manifest_b_hash = '2' * 64
        run_spec_hash = '3' * 64

        results_summary = {
            'schema_version': 1,
            'session': {
                'session_id': session_id,
                'run_ids': [run_id],
                'experiment': 'E2',
                'arm': arm,
                'phase': phase_id
            },
            'source_bundle_sha256': bundle_sha256,
            'source_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
            'bound_gateway_environment_sha256': {},
            'bound_run_spec_sha256': run_spec_hash,
            'binary': dict(prov_json),
            'files': inventory,
            'runner_status': 'COMPLETE',
            'expected_workload_roles': ['A', 'B'],
            'present_workload_roles': ['A', 'B'],
            'missing_workload_roles': [],
            'full_p8_accepted': False,
            'physical_ab': 'NOT RUN'
        }
        if summary_mod:
            summary_mod(results_summary)

        summary_file = bundle_dir / 'results.json'
        summary_file.write_text(json.dumps(results_summary))
        summary_file.chmod(0o600)

        prepared = {
            'session_id': session_id,
            'run_id': run_id,
            'arm': arm,
            'phase_id': phase_id,
            'profile': profile,
            'workload': 'equal_samples',
            'samples_per_message': samples_per_message,
            'bundle_sha256': bundle_sha256,
            'bundle_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
            'context': ctx,
            'expected_counts': exp_counts,
            'seed': ctx['seed'],
        }
        return bundle_dir, binary_file, prepared

    def test_load_e2_result_bundle_equal_samples_positive_and_relocation(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            # Test relocation to another directory
            relocated = root / 'relocated_bundle'
            shutil.copytree(bundle_dir, relocated)

            imported = e2._load_e2_result_bundle(relocated, prepared, binary_file)
            self.assertIn('A', imported)
            self.assertIn('B', imported)
            self.assertIn('runner', imported)
            self.assertIn('binary_provenance', imported)

            eq_a = imported['A']['equal_samples']
            self.assertEqual(len(eq_a), 22)
            self.assertEqual(eq_a['logical_producer'], 'A')
            self.assertEqual(eq_a['context_sha256'], ctx['context_sha256'])
            self.assertEqual(eq_a['total_samples'], 6000)
            self.assertEqual(eq_a['expected_samples'], 3000)
            self.assertEqual(eq_a['expected_messages'], 30)

            eq_b = imported['B']['equal_samples']
            self.assertEqual(eq_b['logical_producer'], 'B')
            self.assertEqual(eq_b['context_sha256'], ctx['context_sha256'])

            # Test historical context from year 2024: no freshness rejection on historical result import
            hist_ctx = build_e2_equal_samples_context(
                timestamp_anchor='2024-01-01T00:00:00Z',
                now_utc=datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
                seed=84,
            )
            hist_bundle, hist_bin, hist_prep = self._create_test_equal_samples_bundle(root / 'hist', hist_ctx)
            hist_imported = e2._load_e2_result_bundle(hist_bundle, hist_prep, hist_bin)
            self.assertEqual(hist_imported['A']['equal_samples']['context_sha256'], hist_ctx['context_sha256'])

    def test_load_e2_result_bundle_accepts_portable_session_profile(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def add_profile(summary):
                summary['session']['profile'] = 'pilot'

            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, summary_mod=add_profile)
            e2._load_e2_result_bundle(bundle_dir, prepared, binary_file)

            def add_wrong_profile(summary):
                summary['session']['profile'] = 'official'

            bad_bundle, bad_binary, bad_prepared = self._create_test_equal_samples_bundle(
                root / 'wrong-profile', ctx, summary_mod=add_wrong_profile)
            with self.assertRaisesRegex(ValueError, 'e2_result_correlation_rejected'):
                e2._load_e2_result_bundle(bad_bundle, bad_prepared, bad_binary)

    def test_load_e2_result_bundle_equal_samples_negatives(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            # 1. Wrong context hash in workload
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg1', ctx, a_mod=lambda a: a['equal_samples'].update({'context_sha256': '0' * 64}))
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_context_hash_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 2. Wrong profile in workload
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg2', ctx, a_mod=lambda a: a.update({'profile': 'official'}))
            with self.assertRaisesRegex(ValueError, 'e2_workload_provenance_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 3. Wrong expected counts
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg3', ctx, a_mod=lambda a: a['equal_samples'].update({'expected_samples': 9999}))
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_expected_counts_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 4. Wrong samples_per_message
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg4', ctx, a_mod=lambda a: a['equal_samples'].update({'samples_per_message': 1}))
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_samples_per_message_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 5. Missing equal_samples in workload
            def drop_eq(a):
                del a['equal_samples']
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(root / 'neg5', ctx, a_mod=drop_eq)
            with self.assertRaisesRegex(ValueError, 'e2_equal_samples_missing_in_workload'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 6. Mode mismatch: steady_burst expected, but equal_samples in workload
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(root / 'neg6', ctx, phase_id='samples-100')
            prep_sb = dict(prep, workload='steady_burst', phase_id='samples-100')
            del prep_sb['context']
            del prep_sb['expected_counts']
            with self.assertRaisesRegex(ValueError, 'e2_workload_mode_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep_sb, b_bin)

            # 7. Wrong binary SHA256 in provenance
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg7', ctx, provenance_mod=lambda p: p.update({'binary_sha256': 'f' * 64}))
            with self.assertRaisesRegex(ValueError, 'e2_binary_provenance_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 8. Missing role B
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'neg8', ctx, summary_mod=lambda s: s.update({'expected_workload_roles': ['A'], 'present_workload_roles': ['A']}))
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 9. Invalid root permission (0o777)
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(root / 'neg9', ctx)
            b_dir.chmod(0o777)
            with self.assertRaisesRegex(ValueError, 'e2_result_root_rejected'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)
            b_dir.chmod(0o700)

            # 10. Symlink in results root
            symlink_dir = root / 'symlink_bundle'
            symlink_dir.symlink_to(b_dir, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, 'e2_result_root_rejected'):
                e2._load_e2_result_bundle(symlink_dir, prep, b_bin)

    def test_finish_remote_collection_keeps_collector_running_on_import_failure_and_succeeds_on_retry(self):
        class MockCollector:
            def __init__(self):
                self.stopped = False
                self.metrics = []
                self.resources = []
                self.errors = []
            def stop(self, pid):
                self.stopped = True

        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            collector = MockCollector()
            prepared['collector'] = collector

            class FakeSpec:
                def __init__(self, gw, s):
                    self.gateway_id = gw
                    self.sensor_id = s
                    self.sensor_urn = f'urn:ngsi-ld:Sensor:{gw}:{s}'

            specs = [FakeSpec('gw-a', 's-a'), FakeSpec('gw-b', 's-b')]
            prepared['specs'] = specs
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            prepared['before_targets'] = [{'configuration': 0}, {'configuration': 0}]
            prepared['baseline'] = {'accepted_batches': 0, 'received_messages': 0}

            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {'accepted_batches': 60, 'received_messages': 60,
                                                         'mqtt_ingress_batches_accepted': 60})
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 60, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'sample_count IS DISTINCT FROM (SELECT count(*)' in sql or 'sample_count <>' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'SELECT coalesce(json_agg' in sql or 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': params.get('message_a', '550e8400-e29b-41d4-a716-446655440001'),
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 30, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql
            smoke.durable_json = Mock()

            # Set invalid permissions on bundle_dir to cause _load_e2_result_bundle failure
            bundle_dir.chmod(0o777)

            with self.assertRaisesRegex(ValueError, 'e2_result_root_rejected'):
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # Collector must NOT have been stopped!
            self.assertFalse(collector.stopped)

            # Now restore valid permissions and retry
            bundle_dir.chmod(0o700)

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # On successful import, collector MUST have been stopped!
            self.assertTrue(collector.stopped)
            self.assertEqual(result['phase_id'], 'equal-samples-100')
            self.assertIn('A', result['workloads'])
            self.assertIn('B', result['workloads'])
            self.assertIn('equal_samples', result['workloads']['A'])
            self.assertEqual(result['workloads']['A']['equal_samples']['logical_producer'], 'A')
            self.assertEqual(result['workloads']['B']['equal_samples']['logical_producer'], 'B')

    def _build_test_equal_samples_validation_payload(
        self, ctx, samples_per_message=100, *,
        status_a='COMPLETE', status_b='COMPLETE',
        retries_a=0, retries_b=0,
        misses_a=0, misses_b=0,
        pending_a=0, pending_b=0,
        primary_error_a=None, primary_error_b=None,
        invalid_acks_a=0, invalid_acks_b=0,
        delta_override=None,
        by_gateway_override=None,
        raw_verification_override=None,
        phase_events_override=None,
        ingress_override=None,
        rate_limited=0,
        queue_full=0,
        exec_ctx_override=None,
        role_mapping=None,
    ):
        exp_counts = calculate_e2_equal_samples_expected_counts(ctx, samples_per_message)
        exp_s = exp_counts['per_role']['A']['samples']
        exp_m = exp_counts['per_role']['A']['messages']
        total_s = exp_counts['total_samples']
        total_m = exp_counts['total_messages']

        def make_workload(role, status, retries, misses, pending, primary_err, invalid_acks):
            w = {
                'status': status,
                'experiment': 'E2',
                'arm': role,
                'profile': 'pilot',
                'mode': 'remote',
                'run_id': f'test-run-{samples_per_message}',
                'telemetry': {
                    'generated_unique': exp_m,
                    'offered_unique': exp_m,
                    'application_acks': exp_m,
                    'pending_messages': pending,
                    'pending_bytes': pending * 100,
                    'publish_attempts': exp_m + retries,
                    'broker_pubacks': exp_m + retries,
                    'retries': retries,
                    'invalid_acks': invalid_acks,
                },
                'equal_samples': {
                    'context_sha256': ctx['context_sha256'],
                    'workload_type': 'equal_samples',
                    'logical_producer': role,
                    'total_samples': ctx['total_samples'],
                    'sample_rate': ctx['sample_rate'],
                    'duration_seconds': ctx['duration_seconds'],
                    'producer_sample_rate': ctx['producer_sample_rate'],
                    'sample_interval_us': ctx['sample_interval_us'],
                    'samples_per_message': samples_per_message,
                    'expected_samples': exp_s,
                    'expected_messages': exp_m,
                    'generated_samples': exp_s,
                    'generated_messages': exp_m,
                    'offered_unique_samples': exp_s,
                    'offered_unique_messages': exp_m,
                    'application_acks': exp_m,
                    'offered_unique_bytes': exp_s * 50,
                    'attempted_bytes': (exp_s + retries * samples_per_message) * 50,
                    'schedule_misses': misses,
                    'scheduling_lag_max_ns': 1000000,
                    'completed_on_time': True,
                    'sampling_qualification': 'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only',
                }
            }
            if primary_err:
                w['primary_error'] = primary_err
            return w

        wa = make_workload('A', status_a, retries_a, misses_a, pending_a, primary_error_a, invalid_acks_a)
        wb = make_workload('B', status_b, retries_b, misses_b, pending_b, primary_error_b, invalid_acks_b)

        mapping = role_mapping or {
            'A': {'gateway_id': 'gw-a', 'sensor_id': 's-a'},
            'B': {'gateway_id': 'gw-b', 'sensor_id': 's-b'},
        }

        database_delta = delta_override if delta_override is not None else {
            'ledger': total_m, 'raw': total_s, 'temporal': total_m, 'configuration': 0,
        }

        database_by_gateway = by_gateway_override if by_gateway_override is not None else {
            'gw-a': {'ledger': exp_m, 'raw': exp_s, 'temporal': exp_m, 'configuration': 0},
            'gw-b': {'ledger': exp_m, 'raw': exp_s, 'temporal': exp_m, 'configuration': 0},
        }

        raw_verification = raw_verification_override if raw_verification_override is not None else [
            {
                'gateway_id': 'gw-a',
                'sensor_id': 's-a',
                'ledger_count': exp_m,
                'raw_count': exp_s,
                'raw_identity_value_mismatches': 0,
                'timestamp_mismatches': 0,
                'sequence_gaps_or_overlaps': 0,
                'provenance_mismatches': 0,
                'identity_ranges': [{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': str(samples_per_message - 1), 'sample_count': samples_per_message}],
                'verified_exact': True,
            },
            {
                'gateway_id': 'gw-b',
                'sensor_id': 's-b',
                'ledger_count': exp_m,
                'raw_count': exp_s,
                'raw_identity_value_mismatches': 0,
                'timestamp_mismatches': 0,
                'sequence_gaps_or_overlaps': 0,
                'provenance_mismatches': 0,
                'identity_ranges': [{'message_id': '550e8400-e29b-41d4-a716-446655440002', 'first_sequence': '0', 'last_sequence': str(samples_per_message - 1), 'sample_count': samples_per_message}],
                'verified_exact': True,
            },
        ]

        phase_events = phase_events_override if phase_events_override is not None else [
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': 60001000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': 60001000},
        ]

        ingress_delta = ingress_override if ingress_override is not None else {
            'available': True,
            'delta': {
                'Committed': total_m,
                'SamplesNew': total_s,
                'ACKPublished': total_m,
                'Received': total_m,
                'Accepted': total_m,
                'RejectedNotRunning': 0,
                'RejectedStopping': 0,
                'RejectedRetained': 0,
                'RejectedOversized': 0,
                'RejectedInvalidTopic': 0,
                'QueueFull': queue_full,
                'ByteBudgetFull': 0,
                'ShaperRateLimited': rate_limited,
            }
        }

        execution_context = exec_ctx_override if exec_ctx_override is not None else {
            'workload': 'equal_samples',
            'samples_per_message': samples_per_message,
            'role_to_target': mapping,
            'context': ctx,
            'expected_counts': exp_counts,
        }

        return {
            'workload_a': wa,
            'workload_b': wb,
            'ingress_delta': ingress_delta,
            'database_delta': database_delta,
            'database_by_gateway': database_by_gateway,
            'phase_events': phase_events,
            'raw_verification': raw_verification,
            'execution_context': execution_context,
        }

    def test_validate_e2_equal_samples_result_valid_s1_and_s100(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        for S in (100, 1):
            payload = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=S)
            res = validate_e2_equal_samples_result(**payload)
            self.assertTrue(res['valid'], f"Failed for S={S}: {res.get('failures')}")
            self.assertTrue(res['clean_comparison'], f"Contaminated for S={S}: {res.get('contamination_reasons')}")
            self.assertEqual(res['failures'], [])
            self.assertEqual(res['contamination_reasons'], [])
            self.assertFalse(res['full_p8_accepted'])
            self.assertEqual(res['generator_colocated'], 'unverified')
            self.assertEqual(res['physical_ab'], 'NOT RUN')
            self.assertEqual(res['availability'], 'available')

    def test_validate_e2_equal_samples_result_contaminated_but_functionally_valid(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        # 1. Contamination by telemetry retry in A
        p1 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100, retries_a=2)
        r1 = validate_e2_equal_samples_result(**p1)
        self.assertTrue(r1['valid'], "Functionally valid data must have valid=True")
        self.assertFalse(r1['clean_comparison'], "Run with retries must not be clean_comparison")
        self.assertEqual(r1['failures'], [])
        self.assertTrue(any('A_retries_observed:2' in c for c in r1['contamination_reasons']))

        # 2. Contamination by schedule misses in B
        p2 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100, misses_b=1)
        r2 = validate_e2_equal_samples_result(**p2)
        self.assertTrue(r2['valid'])
        self.assertFalse(r2['clean_comparison'])
        self.assertTrue(any('B_schedule_misses_observed:1' in c for c in r2['contamination_reasons']))

        # 3. Contamination by shaper rate limiting in ingress
        p3 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100, rate_limited=5)
        r3 = validate_e2_equal_samples_result(**p3)
        self.assertTrue(r3['valid'])
        self.assertFalse(r3['clean_comparison'])
        self.assertTrue(any('ingress_shaper_rate_limited:5' in c for c in r3['contamination_reasons']))

        # 4. Contamination by collector errors
        p4 = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100)
        p4['resources'] = {'collector_errors': ['transient_scrape_timeout']}
        r4 = validate_e2_equal_samples_result(**p4)
        self.assertTrue(r4['valid'])
        self.assertFalse(r4['clean_comparison'])
        self.assertTrue(any('collector_errors' in c for c in r4['contamination_reasons']))

    def test_validate_e2_equal_samples_result_failures(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        # 1. Status not complete
        p = self._build_test_equal_samples_validation_payload(ctx, status_a='FAILED')
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('workload_status', r['failures'])

        # 2. Telemetry counter mismatch (generated_unique wrong)
        p = self._build_test_equal_samples_validation_payload(ctx)
        p['workload_a']['telemetry']['generated_unique'] = 29
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('A_generated_unique', r['failures'])

        # 3. Pending messages > 0
        p = self._build_test_equal_samples_validation_payload(ctx, pending_a=1)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('A_pending_messages', r['failures'])

        # 4. Primary error present
        p = self._build_test_equal_samples_validation_payload(ctx, primary_error_b='gateway_unreachable')
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('B_primary_error', r['failures'])

        # 5. Telemetry vs equal_samples summary cross-check failure
        p = self._build_test_equal_samples_validation_payload(ctx)
        p['workload_a']['equal_samples']['offered_unique_messages'] = 999
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('A_equal_samples_telemetry_cross_check_mismatch', r['failures'])

        # 6. Global DB delta mismatch (e.g. raw is 5999 instead of 6000)
        p = self._build_test_equal_samples_validation_payload(ctx, delta_override={'ledger': 60, 'raw': 5999, 'temporal': 60, 'configuration': 0})
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('database_delta_mismatch', r['failures'])

        # 7. Scoped DB gateway delta mismatch (gw-a has wrong raw count)
        bad_by_gw = {
            'gw-a': {'ledger': 30, 'raw': 2999, 'temporal': 30, 'configuration': 0},
            'gw-b': {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0},
        }
        p = self._build_test_equal_samples_validation_payload(ctx, by_gateway_override=bad_by_gw)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('database_gateway_delta_mismatch', r['failures'])

        # 8. Scoped DB gateway scope mismatch (wrong gateway namespace)
        wrong_ns_by_gw = {
            'gw-a': {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0},
            'gw-unknown': {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0},
        }
        p = self._build_test_equal_samples_validation_payload(ctx, by_gateway_override=wrong_ns_by_gw)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('database_gateway_scope_mismatch', r['failures'])

        # 9. Raw verification failed (verified_exact is False)
        bad_raw = [
            {'gateway_id': 'gw-a', 'sensor_id': 's-a', 'ledger_count': 30, 'raw_count': 3000,
             'raw_identity_value_mismatches': 1, 'timestamp_mismatches': 0, 'verified_exact': False},
            {'gateway_id': 'gw-b', 'sensor_id': 's-b', 'ledger_count': 30, 'raw_count': 3000,
             'raw_identity_value_mismatches': 0, 'timestamp_mismatches': 0, 'verified_exact': True},
        ]
        p = self._build_test_equal_samples_validation_payload(ctx, raw_verification_override=bad_raw)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('equal_samples_raw_verification_failed', r['failures'])

        # 10. Phase intervals non-overlapping
        no_overlap_events = [
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
            {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 10000, 'observed_at_runner_monotonic_ns': 10000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 20000, 'observed_at_runner_monotonic_ns': 20000},
            {'child': 'paperload-telemetry:B.manifest.json', 'event': 'measure_end', 'elapsed_ns': 30000, 'observed_at_runner_monotonic_ns': 30000},
        ]
        p = self._build_test_equal_samples_validation_payload(ctx, phase_events_override=no_overlap_events)
        r = validate_e2_equal_samples_result(**p)
        self.assertFalse(r['valid'])
        self.assertIn('a_measure_does_not_overlap_b_measure', r['failures'])

    def test_validate_e2_result_mode_dispatch(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        # 1. With workload='equal_samples', validate_e2_result dispatches to validate_e2_equal_samples_result
        payload = self._build_test_equal_samples_validation_payload(ctx, samples_per_message=100)
        res = validate_e2_result(**payload)
        self.assertTrue(res['valid'])
        self.assertEqual(res['generator_colocated'], 'unverified')
        self.assertIn('clean_comparison', res)

        # 2. Without equal_samples, validate_e2_result uses legacy validation (and fails on 30/3000 counts)
        legacy_exec_ctx = {'samples_per_message': 100, 'role_to_target': payload['execution_context']['role_to_target']}
        legacy_payload = dict(payload, execution_context=legacy_exec_ctx)
        # Drop equal_samples from workload dicts so dispatch detects steady_burst
        legacy_wa = dict(payload['workload_a'])
        del legacy_wa['equal_samples']
        legacy_wb = dict(payload['workload_b'])
        del legacy_wb['equal_samples']
        legacy_payload['workload_a'] = legacy_wa
        legacy_payload['workload_b'] = legacy_wb
        legacy_res = validate_e2_result(**legacy_payload)
        # Legacy requires 8 and 16 messages and burst overlap, so it will fail
        self.assertFalse(legacy_res['valid'])
        self.assertIn('pilot_raw_retention_verification', legacy_res['failures'])

    def test_verify_e2_equal_samples_raw_server_side_checks(self):
        smoke = Mock()
        registry = Mock()
        spec = Mock(gateway_id='gw-a', sensor_id='s-a')

        def setup_psql_mock(ledger_override=None, overlap_override='0',
                            prov_override='0', tele_override=None):
            def fake_db(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql:
                    return str(overlap_override)
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    data = {
                        'ledger_count': 30,
                        'min_first_sequence': 0,
                        'max_last_sequence': 2999,
                        'total_sample_count': 3000,
                        'invalid_sample_count': 0,
                        'invalid_sequence_bounds': 0,
                    }
                    if ledger_override:
                        data.update(ledger_override)
                    return json.dumps(data)
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql:
                    return str(prov_override)
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    data = {
                        'raw_count': 3000,
                        'unique_sequence_count': 3000,
                        'min_sequence': 0,
                        'max_sequence': 2999,
                        'value_mismatches': 0,
                        'timestamp_mismatches': 0,
                    }
                    if tele_override:
                        data.update(tele_override)
                    return json.dumps(data)
                if 'LIMIT 20' in sql:
                    return json.dumps([
                        {'message_id': f'550e8400-e29b-41d4-a716-{i:012d}',
                         'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                         'sample_count': 100}
                        for i in range(20)
                    ])
                return '0'
            smoke.db_psql.side_effect = fake_db

        # 1. Positive: exact valid check
        setup_psql_mock()
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertTrue(res['verified_exact'])
        self.assertEqual(res['raw_count'], 3000)
        self.assertEqual(res['ledger_count'], 30)
        self.assertEqual(res['raw_identity_value_mismatches'], 0)
        self.assertEqual(res['timestamp_mismatches'], 0)
        self.assertEqual(res['sequence_gaps_or_overlaps'], 0)
        self.assertEqual(len(res['identity_ranges']), 20)

        # 2. Negative: synthetic value mismatch
        setup_psql_mock(tele_override={'value_mismatches': 3})
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['raw_identity_value_mismatches'], 3)

        # 3. Negative: timestamp mismatch
        setup_psql_mock(tele_override={'timestamp_mismatches': 1})
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['timestamp_mismatches'], 1)

        # 4. Negative: sequence gap/overlap in telemetry
        setup_psql_mock(tele_override={'unique_sequence_count': 2999})
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['sequence_gaps_or_overlaps'], 1)

        # 5. Negative: ledger overlap
        setup_psql_mock(overlap_override='1')
        res = verify_e2_equal_samples_raw(
            smoke, registry, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)
        self.assertFalse(res['verified_exact'])
        self.assertEqual(res['sequence_gaps_or_overlaps'], 1)

    def test_verify_e2_equal_samples_raw_real_db_psql_adapter_and_param_validation(self):
        """Invoke REAL native_compose_smoke.db_psql to assert string validation and argv -v values."""
        smoke = smoke_runtime
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a')

        captured_commands = []
        class FakeRegistry:
            def command(self, args, input=None):
                captured_commands.append((list(args), input))
                sql = input.decode() if input else ""
                if 'FROM processed_messages p1' in sql:
                    out = b'0\n'
                elif 'json_build_object' in sql and 'ledger_count' in sql:
                    out = json.dumps({
                        'ledger_count': 30,
                        'min_first_sequence': 0,
                        'max_last_sequence': 2999,
                        'total_sample_count': 3000,
                        'invalid_sample_count': 0,
                        'invalid_sequence_bounds': 0,
                    }).encode() + b'\n'
                elif 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                    out = b'0\n'
                elif 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    out = json.dumps({
                        'raw_count': 3000,
                        'unique_sequence_count': 3000,
                        'min_sequence': 0,
                        'max_sequence': 2999,
                        'value_mismatches': 0,
                        'timestamp_mismatches': 0,
                    }).encode() + b'\n'
                elif 'LIMIT 20' in sql:
                    out = json.dumps([
                        {'message_id': f'550e8400-e29b-41d4-a716-{i:012d}',
                         'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                         'sample_count': 100}
                        for i in range(20)
                    ]).encode() + b'\n'
                else:
                    out = b'0\n'
                return SimpleNamespace(returncode=0, stdout=out, stderr=b'')

        reg = FakeRegistry()
        res = verify_e2_equal_samples_raw(
            smoke, reg, 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        self.assertTrue(res['verified_exact'])
        self.assertGreater(len(captured_commands), 0)

        # Assert argv -v values across every psql invocation
        for args, _ in captured_commands:
            v_pairs = {}
            for i in range(len(args) - 1):
                if args[i] == '-v' and '=' in args[i + 1]:
                    k, v = args[i + 1].split('=', 1)
                    v_pairs[k] = v
            self.assertEqual(v_pairs.get('gw'), 'gw-a')
            self.assertEqual(v_pairs.get('sensor'), 's-a')
            self.assertEqual(v_pairs.get('expected_samples'), '3000')
            self.assertEqual(v_pairs.get('expected_messages'), '30')
            self.assertEqual(v_pairs.get('samples_per_message'), '100')
            self.assertEqual(v_pairs.get('anchor'), '2026-10-31T11:58:00Z')
            self.assertEqual(v_pairs.get('interval_us'), '20000')

        # Guard input types and count bounds before conversion - do not coerce bool or invalid strings
        for bad_samples in (True, False, '3000', 3000.0, 0, -1, 10_000_001):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=bad_samples, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        for bad_messages in (True, False, '30', 30.5, 0, -5):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=3000, expected_messages=bad_messages, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        for bad_spm in (True, False, '100', 0, -100):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=bad_spm,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        for bad_interval in (True, False, '20000', 0, -20, (1 << 53)):
            with self.assertRaises(ValueError):
                verify_e2_equal_samples_raw(
                    smoke, reg, 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=bad_interval)

    def test_verify_e2_equal_samples_raw_numeric_parsing_fails_closed(self):
        """Non-digit or corrupt count strings must fail closed (raise ValueError), never defaulting to 0."""
        import native_compose_smoke as smoke
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a')

        def make_reg(overlap_out=b'0\n', prov_out=b'0\n', ledger_out=None, tele_out=None):
            class FakeRegistry:
                def command(self, args, input=None):
                    sql = input.decode() if input else ""
                    if 'FROM processed_messages p1' in sql:
                        out = overlap_out
                    elif 'json_build_object' in sql and 'ledger_count' in sql:
                        out = ledger_out if ledger_out is not None else json.dumps({
                            'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                            'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                        }).encode() + b'\n'
                    elif 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                        out = prov_out
                    elif 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                        out = tele_out if tele_out is not None else json.dumps({
                            'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                            'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                        }).encode() + b'\n'
                    elif 'LIMIT 20' in sql:
                        out = json.dumps([
                            {'message_id': f'550e8400-e29b-41d4-a716-{i:012d}',
                             'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                             'sample_count': 100}
                            for i in range(20)
                        ]).encode() + b'\n'
                    else:
                        out = b'0\n'
                    return SimpleNamespace(returncode=0, stdout=out, stderr=b'')
            return FakeRegistry()

        # Non-digit overlap must fail closed
        for bad_overlap in (b'', b'   \n', b'invalid\n', b'None\n', b'-1\n', b'NaN\n'):
            with self.assertRaisesRegex(ValueError, 'raw_audit_invalid_overlap_count'):
                verify_e2_equal_samples_raw(
                    smoke, make_reg(overlap_out=bad_overlap), 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        # Non-digit provenance mismatches must fail closed
        for bad_prov in (b'', b'   \n', b'corrupted\n', b'None\n', b'-5\n'):
            with self.assertRaisesRegex(ValueError, 'raw_audit_invalid_provenance_count'):
                verify_e2_equal_samples_raw(
                    smoke, make_reg(prov_out=bad_prov), 'fake-pg', spec,
                    expected_samples=3000, expected_messages=30, samples_per_message=100,
                    timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        # Corrupt JSON in ledger or telemetry stats must fail closed
        with self.assertRaises(ValueError):
            verify_e2_equal_samples_raw(
                smoke, make_reg(ledger_out=b'NOT_JSON\n'), 'fake-pg', spec,
                expected_samples=3000, expected_messages=30, samples_per_message=100,
                timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        with self.assertRaises(ValueError):
            verify_e2_equal_samples_raw(
                smoke, make_reg(tele_out=b'NOT_JSON\n'), 'fake-pg', spec,
                expected_samples=3000, expected_messages=30, samples_per_message=100,
                timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

    def test_verify_e2_equal_samples_raw_sql_queries_inspect_null_comparisons(self):
        """All SQL queries must use IS DISTINCT FROM or explicit IS NULL checks to prevent hiding mismatches."""
        import native_compose_smoke as smoke
        spec = SimpleNamespace(gateway_id='gw-a', sensor_id='s-a')
        executed_sqls = []

        class QueryCapturingRegistry:
            def command(self, args, input=None):
                if input:
                    executed_sqls.append(input.decode())
                if len(executed_sqls) == 1:
                    out = json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    }).encode() + b'\n'
                elif len(executed_sqls) in (2, 3):
                    out = b'0\n'
                elif len(executed_sqls) == 4:
                    out = json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    }).encode() + b'\n'
                else:
                    out = b'[]\n'
                return SimpleNamespace(returncode=0, stdout=out, stderr=b'')

        verify_e2_equal_samples_raw(
            smoke, QueryCapturingRegistry(), 'fake-pg', spec,
            expected_samples=3000, expected_messages=30, samples_per_message=100,
            timestamp_anchor='2026-10-31T11:58:00Z', sample_interval_us=20000)

        self.assertEqual(len(executed_sqls), 5)
        q1_ledger, q2_overlap, q3_prov, q4_tele, q5_identity = executed_sqls

        # Query 1: invalid_sample_count must use IS DISTINCT FROM, invalid_sequence_bounds must guard NULLs
        self.assertIn("p.sample_count IS DISTINCT FROM :'samples_per_message'::int", q1_ledger)
        self.assertIn('p.first_sequence IS NULL', q1_ledger)
        self.assertIn('p.last_sequence IS NULL', q1_ledger)

        # Query 3: provenance comparisons must use IS DISTINCT FROM so missing telemetry rows (NULL) are detected
        self.assertIn('p.sample_count IS DISTINCT FROM (SELECT count(*)', q3_prov)
        self.assertIn('p.first_sequence IS DISTINCT FROM (SELECT min(t.sequence_num)', q3_prov)
        self.assertIn('p.last_sequence IS DISTINCT FROM (SELECT max(t.sequence_num)', q3_prov)
        self.assertIn('p.timestamp IS DISTINCT FROM (SELECT min(t.measured_at)', q3_prov)
        self.assertIn('t.sample_index IS DISTINCT FROM (t.sequence_num - p.first_sequence)', q3_prov)
        self.assertIn('t.boot_id IS DISTINCT FROM p.boot_id', q3_prov)

        # Query 4: telemetry NULLs must not be hidden in value or timestamp filters
        self.assertIn('t.value IS NULL', q4_tele)
        self.assertIn('t.measured_at IS NULL', q4_tele)

    def test_actual_shaped_images_and_five_service_roles(self):
        """Test extraction from actual inspect dicts with capital Id across all 5 service roles."""
        images_actual_dict = {
            'backend': {'Id': 'sha256:' + '1' * 64},
            'gateway': {'Id': 'sha256:' + '2' * 64},
            'postgres': {'Id': 'sha256:' + '3' * 64},
            'mosquitto': {'Id': 'sha256:' + '4' * 64},
            'nginx': {'Id': 'sha256:' + '5' * 64},
            'supabase-auth': {'Id': 'sha256:' + '6' * 64},
            'supabase-envoy': {'Id': 'sha256:' + '7' * 64},
        }

        # validate_e2_reference requires all 5 roles and handles 'Id'
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            from scripts.tests.test_paper_p8_e2_reference import _make_sample_context, _make_sample_reference
            ctx = _make_sample_context()
            ref_file, report, _ = _make_sample_reference(td_path, context=ctx)

            # Update report with actual-shaped images dict
            rep = json.loads(ref_file.read_text())
            rep['images'] = images_actual_dict
            del rep['image_ids']
            ref_file.write_text(json.dumps(rep))

            validated = e2.validate_e2_reference(ref_file, ctx)
            self.assertEqual(validated['status'], 'PASS')
            for role in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
                self.assertIn(role, validated['image_ids'])
                self.assertTrue(validated['image_ids'][role].startswith('sha256:'))

            # Missing one of the 5 roles must fail closed
            bad_images = dict(images_actual_dict)
            del bad_images['nginx']
            rep['images'] = bad_images
            bad_file = td_path / 'bad_ref.json'
            bad_file.write_text(json.dumps(rep))
            bad_file.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'e2_reference_images_invalid'):
                e2.validate_e2_reference(bad_file, ctx)

    def test_finish_remote_collection_actual_shaped_images_quota_cleanup_and_consumers(self):
        """finish_remote_collection produces finish report compatible with reference and compare consumers."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            # Actual-shaped image inspect output with service role keys and capital Id
            actual_images = {
                'backend': {'Id': 'sha256:' + '1' * 64},
                'gateway': {'Id': 'sha256:' + '2' * 64},
                'postgres': {'Id': 'sha256:' + '3' * 64},
                'mosquitto': {'Id': 'sha256:' + '4' * 64},
                'nginx': {'Id': 'sha256:' + '5' * 64},
                'supabase-auth': {'Id': 'sha256:' + '6' * 64},
                'supabase-envoy': {'Id': 'sha256:' + '7' * 64},
            }
            prepared['images'] = actual_images
            prepared['quota'] = {'cpus': 2, 'memory_bytes': 4294967296}
            prepared['cleanup_verified'] = True
            prepared['exact_cleanup_verified'] = True

            specs = [
                SimpleNamespace(gateway_id='gw_a', sensor_id='s_a', sensor_urn='urn:ngsi-ld:Sensor:gw_a:s_a'),
                SimpleNamespace(gateway_id='gw_b', sensor_id='s_b', sensor_urn='urn:ngsi-ld:Sensor:gw_b:s_b'),
            ]
            prepared['specs'] = specs
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60,
                'Accepted': 60,
                'Committed': 60,
                'ACKPublished': 60,
                'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing}
            prepared['before_targets'] = [
                {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
            ]

            class FakeCollector:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                def stop(self, _):
                    self.stopped = True
                def record_terminal(self, a, r):
                    pass

            collector = FakeCollector()
            prepared['collector'] = collector

            class FakeFixture:
                artifacts = root / 'artifacts'
                private = root / 'private'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)
            FakeFixture.private.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.ROOT = str(root)
            smoke.durable_json = lambda p, d: Path(p).write_text(json.dumps(d))
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1',
                'ingress': term_ing,
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}

            def fake_psql(reg, pg, sql, params=None):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                    return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001',
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # 1. Check extracted image IDs: must be valid strings, not None
            self.assertIsNotNone(result.get('image_ids'))
            for svc in ('backend', 'gateway', 'postgres', 'mosquitto', 'nginx'):
                self.assertIn(svc, result['image_ids'])
                val = result['image_ids'][svc]
                self.assertIsInstance(val, str)
                self.assertTrue(val.startswith('sha256:'))

            # 2. Check quota and cleanup keys
            self.assertEqual(result.get('quota'), {'cpus': 2, 'memory_bytes': 4294967296})
            self.assertTrue(result.get('cleanup_verified'))
            self.assertTrue(result.get('exact_cleanup_verified'))

            # 3. Reference consumer accepts report
            report_path = FakeFixture.artifacts / f'p8-e2-{prepared["phase_id"]}-remote-result.json'
            self.assertTrue(report_path.is_file())
            report_path.chmod(0o600)
            art_bin = FakeFixture.artifacts / 'bin'
            art_bin.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(binary_file, art_bin / 'paperload')
            (art_bin / 'paperload').chmod(0o700)
            ref_val = e2.validate_e2_reference(report_path, ctx)
            self.assertEqual(ref_val['status'], 'PASS')

            # 4. Compare consumer accepts reports
            rep_1 = json.loads(report_path.read_text())
            rep_100 = copy.deepcopy(rep_1)
            rep_1['samples_per_message'] = 1
            rep_100['samples_per_message'] = 100
            comp_res = e2.compare_e2_reports(rep_1, rep_100)
            self.assertEqual(comp_res['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(comp_res['comparable'])
            self.assertEqual(comp_res['mismatch_reasons'], [])

    def test_load_e2_result_bundle_relocated_manifest_hashes_differ(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Create manifest files on host A with source bytes
            a_source_manifest = root / 'A.source.manifest.json'
            b_source_manifest = root / 'B.source.manifest.json'
            a_source_manifest.write_text(json.dumps({'experiment': 'E2', 'arm': 'A', 'source_host': 'host_a_path'}))
            b_source_manifest.write_text(json.dumps({'experiment': 'E2', 'arm': 'B', 'source_host': 'host_a_path'}))
            a_source_manifest.chmod(0o600); b_source_manifest.chmod(0o600)
            a_source_hash = hashlib.sha256(a_source_manifest.read_bytes()).hexdigest()
            b_source_hash = hashlib.sha256(b_source_manifest.read_bytes()).hexdigest()

            # Bound manifests on host B have DIFFERENT hashes because bundle relocation rewrote paths
            a_bound_hash = 'a' * 64
            b_bound_hash = 'b' * 64
            self.assertNotEqual(a_source_hash, a_bound_hash)
            self.assertNotEqual(b_source_hash, b_bound_hash)

            # Create test bundle on host B where bound_manifest_sha256 has the bound hashes
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'relocated_run', ctx,
                summary_mod=lambda s: s.update({
                    'bound_manifest_sha256': {'A': a_bound_hash, 'B': b_bound_hash},
                    'source_inventory': [
                        {'path': 'manifests/A.json', 'sha256': a_source_hash},
                        {'path': 'manifests/B.json', 'sha256': b_source_hash},
                    ],
                }))

            # On host A, prepared has manifest_paths pointing to A's source manifests
            prep['manifest_paths'] = {'A': a_source_manifest, 'B': b_source_manifest}
            prep['bundle_inventory'] = [
                {'path': 'manifests/A.json', 'sha256': a_source_hash},
                {'path': 'manifests/B.json', 'sha256': b_source_hash},
            ]
            # prepared must NOT require direct byte equality with bound manifests
            prep.pop('bound_manifest_sha256', None)

            # Loading the result bundle on host A MUST succeed
            imported = e2._load_e2_result_bundle(b_dir, prep, b_bin)
            self.assertIn('A', imported)
            self.assertIn('B', imported)

    def test_load_e2_result_bundle_real_exported_portable_bundle_sha_provenance(self):
        """Newly exported real portable bundle: source != portable != bound SHA, loader authenticates bundle_root."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ca_root = root / 'ca'
            ca_root.mkdir(mode=0o700)
            https_ca = ca_root / 'https.pem'; https_ca.write_text('fake https ca'); https_ca.chmod(0o600)
            mqtt_ca = ca_root / 'mqtt.pem'; mqtt_ca.write_text('fake mqtt ca'); mqtt_ca.chmod(0o600)

            gw_dir = root / 'gateways'
            gw_dir.mkdir(mode=0o700)
            gw_a = gw_dir / 'gw_a.json'; gw_a.write_text(json.dumps({'username': 'gw_a_user', 'password': 'p'})); gw_a.chmod(0o600)
            gw_b = gw_dir / 'gw_b.json'; gw_b.write_text(json.dumps({'username': 'gw_b_user', 'password': 'p'})); gw_b.chmod(0o600)

            id_dir = root / 'identities'
            id_dir.mkdir(mode=0o700)
            id_a = id_dir / 'telemetry-A.json'; id_a.write_text('{}'); id_a.chmod(0o600)
            id_b = id_dir / 'telemetry-B.json'; id_b.write_text('{}'); id_b.chmod(0o600)

            gateway_files = {'gw_a': str(gw_a), 'gw_b': str(gw_b)}
            telemetry_files = {'A': str(id_a), 'B': str(id_b)}
            targets = [
                {'gateway_id': 'gw_a', 'sensor_id': 's1'},
                {'gateway_id': 'gw_b', 'sensor_id': 's2'},
            ]
            common = {
                'mode': 'remote',
                'bundle_path': str(root), 'publish_timeout_ms': 3000,
                'tls': {'https_endpoint': 'https://localhost:8443', 'https_ca_file': str(https_ca),
                        'mqtt_tls_endpoint': 'mqtts://localhost:8883',
                        'mqtt_tls_ca_file': str(mqtt_ca), 'mqtt_tls_server_name': 'localhost'}
            }

            manifests = e2.e2_equal_samples_manifests(
                context=ctx, common=common, targets=targets,
                gateway_identity_files=gateway_files, telemetry_identity_files=telemetry_files,
                now_utc=fixed_now, check_freshness=False
            )
            pair = manifests['profiles']['100']

            manifest_paths = {}
            for arm in ('A', 'B'):
                path = root / f'{arm}.manifest.json'
                path.write_text(json.dumps(pair[arm], sort_keys=True, separators=(',', ':')))
                path.chmod(0o600)
                manifest_paths[arm] = path

            plan_root = root / 'plan'
            plan = e2.build_e2_execution_plan(output_dir=plan_root, manifest_a=manifest_paths['A'], manifest_b=manifest_paths['B'], seed=ctx['seed'])

            bundle_root = root / 'client-bundle'
            prepared = {
                'session_id': 's123', 'run_id': pair['A']['run_id'], 'arm': 'E2-pilot',
                'samples_per_message': 100, 'profile': 'pilot', 'workload': 'equal_samples',
                'root': root, 'bundle_root': bundle_root, 'plan': plan, 'manifest_paths': manifest_paths,
                'manifests': pair, 'gateway_files': gateway_files, 'telemetry_files': telemetry_files,
                'https_ca_file': https_ca, 'mqtt_ca_file': mqtt_ca, 'phase_id': 'equal-samples-100',
                'seed': ctx['seed'], 'context': ctx, 'duration_seconds': 60, 'drain_seconds': 20,
                'grace_seconds': 10, 'wall_seconds': 90, 'expected_counts': manifests['expected_counts']['100'],
            }

            e2.export_prepared_bundle(prepared)

            # Assert SHA differences between source, portable, and bound
            source_sha_a = hashlib.sha256(manifest_paths['A'].read_bytes()).hexdigest()
            portable_sha_a = hashlib.sha256((bundle_root / 'manifests/A.json').read_bytes()).hexdigest()

            workspace = root / 'workspace'
            paper_p8_bundle.bind_bundle(bundle_root=bundle_root, workspace_leaf=workspace)
            bound_sha_a = hashlib.sha256((workspace / 'runtime/manifests/A.json').read_bytes()).hexdigest()
            bound_sha_b = hashlib.sha256((workspace / 'runtime/manifests/B.json').read_bytes()).hexdigest()

            self.assertNotEqual(source_sha_a, portable_sha_a)
            self.assertNotEqual(portable_sha_a, bound_sha_a)
            self.assertNotEqual(source_sha_a, bound_sha_a)

            bundle_meta = json.loads((bundle_root / 'bundle.json').read_text())
            prepared['bundle_inventory'] = [{'path': r['path'], 'sha256': r['sha256']} for r in bundle_meta['inputs']]
            prepared['bundle_sha256'] = hashlib.sha256((bundle_root / 'bundle.json').read_bytes()).hexdigest()

            bin_file = root / 'paperload_bin'
            bin_file.write_bytes(b'paperload-bin-exec')
            bin_file.chmod(0o700)
            bin_sha = hashlib.sha256(b'paperload-bin-exec').hexdigest()

            results_root = root / 'results_export'
            results_root.mkdir(mode=0o700)
            (results_root / 'results/workloads/A').mkdir(parents=True, mode=0o700)
            (results_root / 'results/workloads/B').mkdir(parents=True, mode=0o700)
            (results_root / 'results/runner').mkdir(parents=True, mode=0o700)

            def make_workload(arm):
                return {
                    'status': 'COMPLETE', 'mode': 'remote', 'experiment': 'E2', 'arm': arm,
                    'profile': 'pilot', 'run_id': pair['A']['run_id'], 'seed': ctx['seed'],
                    'telemetry': {'scheduled_opportunities': 30, 'generated_unique': 30, 'offered_unique': 30, 'application_acks': 30},
                    'equal_samples': {
                        'workload_type': 'equal_samples', 'logical_producer': arm,
                        'context_sha256': ctx['context_sha256'], 'samples_per_message': 100,
                        'total_samples': 6000, 'sample_rate': 100, 'duration_seconds': 60,
                        'producer_sample_rate': 50, 'sample_interval_us': 20000,
                        'expected_samples': 3000, 'expected_messages': 30,
                        'generated_samples': 3000, 'generated_messages': 30,
                        'offered_unique_samples': 3000, 'offered_unique_messages': 30,
                        'application_acks': 30, 'attempted_bytes': 30000, 'offered_unique_bytes': 30000,
                        'completed_on_time': True, 'schedule_misses': 0, 'scheduling_lag_max_ns': 1000,
                        'sampling_qualification': 'logical_offered_stream; not physical sensor acquisition latency; commit ack from application outbox only'
                    }
                }

            workload_a = make_workload('A')
            workload_b = make_workload('B')
            prov_json = {'binary_sha256': bin_sha, 'binary_os': sys.platform, 'binary_arch': platform.machine()}
            run_json = {'status': 'COMPLETE', 'phase_observations': {'status': 'observed', 'events': []}}

            files_data = {
                'results/workloads/A/workload.json': json.dumps(workload_a).encode(),
                'results/workloads/B/workload.json': json.dumps(workload_b).encode(),
                'results/runner/run.json': json.dumps(run_json).encode(),
                'results/runner/provenance.json': json.dumps(prov_json).encode(),
            }
            inventory = []
            for rel_path, data in files_data.items():
                p = results_root / rel_path
                p.write_bytes(data)
                p.chmod(0o600)
                inventory.append({'path': rel_path, 'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})

            results_summary = {
                'session': {'session_id': 's123', 'run_ids': [pair['A']['run_id']], 'experiment': 'E2', 'arm': 'E2-pilot', 'phase': 'equal-samples-100'},
                'source_bundle_sha256': prepared['bundle_sha256'],
                'source_inventory': prepared['bundle_inventory'],
                'bound_manifest_sha256': {'A': bound_sha_a, 'B': bound_sha_b},
                'bound_gateway_environment_sha256': {},
                'bound_run_spec_sha256': hashlib.sha256((workspace / 'runtime/execution/run-spec.json').read_bytes()).hexdigest(),
                'binary': prov_json,
                'files': inventory,
                'runner_status': 'COMPLETE',
                'expected_workload_roles': ['A', 'B'],
                'present_workload_roles': ['A', 'B'],
                'missing_workload_roles': [],
                'full_p8_accepted': False,
                'physical_ab': 'NOT RUN'
            }
            summary_file = results_root / 'results.json'
            summary_file.write_text(json.dumps(results_summary))
            summary_file.chmod(0o600)

            # Positive authentication
            imported = e2._load_e2_result_bundle(results_root, prepared, bin_file)
            self.assertIn('A', imported)
            self.assertIn('B', imported)
            self.assertIn('runner', imported)

            # Negatives:
            # 1. Missing portable manifest rejects
            manifest_a = bundle_root / 'manifests/A.json'
            manifest_a.unlink()
            with self.assertRaisesRegex(ValueError, 'e2_source_manifest_inventory_mismatch'):
                e2._load_e2_result_bundle(results_root, prepared, bin_file)

            # 2. Corrupted portable manifest hash mismatch rejects
            manifest_a.write_text('corrupted')
            manifest_a.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'e2_source_manifest_inventory_mismatch'):
                e2._load_e2_result_bundle(results_root, prepared, bin_file)

            # 3. Symlink portable manifest rejects
            manifest_a.unlink()
            target_f = root / 'target_manifest.json'; target_f.write_text('target'); target_f.chmod(0o600)
            manifest_a.symlink_to(target_f)
            with self.assertRaisesRegex(ValueError, 'e2_source_manifest_inventory_mismatch'):
                e2._load_e2_result_bundle(results_root, prepared, bin_file)

    def test_compare_e2_reports_clean_git_tree_source_fingerprint(self):
        """When repo is clean, dirty_source_sha256 is None; reports must compare without field missing error."""
        from scripts.tests.test_paper_p8_e2_compare import _make_report
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            clean_fp = {'revision': 'a' * 40, 'branch': 'feat/x', 'dirty': False, 'dirty_source_sha256': None}
            _, r1 = _make_report(td_path, samples_per_message=1, source_fingerprint=clean_fp)
            _, r100 = _make_report(td_path, samples_per_message=100, source_fingerprint=clean_fp)

            res = e2.compare_e2_reports(r1, r100)
            self.assertEqual(res['status'], 'COMPARABLE_CLEAN')
            self.assertTrue(res['comparable'])
            self.assertNotIn('missing_required_source_fingerprint_field:dirty_source_sha256', res['mismatch_reasons'])

    def test_finish_remote_collection_binary_authentication_and_sha256(self):
        """finish_remote_collection requires valid binary path, calculates SHA256 once, and rejects invalid binaries."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            class FakeCollector:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                def stop(self, error):
                    self.stopped = True
                def record_terminal(self, admin, res):
                    pass

            collector = FakeCollector()
            prepared['collector'] = collector
            prepared['tokens'] = {'admin': 'fake-token'}
            prepared['specs'] = [
                Mock(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a'),
                Mock(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:s-b'),
            ]
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            prepared['before_targets'] = [{'configuration': 0}, {'configuration': 0}]
            prepared['baseline'] = {'process_instance': 'proc-1', 'ingress': {'Received': 0, 'Committed': 0, 'Accepted': 0}}
            prepared['pg'] = 'fake-pg'

            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, exist_ok=True)

            smoke = Mock()
            smoke.ROOT = str(root)
            smoke.db_psql.return_value = '0'
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'proc-1',
                'ingress': {'Received': 60, 'Committed': 60, 'Accepted': 60},
                'persistence': {'copy_calls': 30, 'rows': 6000},
            })
            smoke.paper_p1_count_db.return_value = {'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0}

            def fake_psql(reg, pg, sql, params=None):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <>' in sql:
                    return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            # 1. Missing binary file rejected
            non_existent = root / 'non_existent_bin'
            with self.assertRaisesRegex(ValueError, 'e2_binary_file_rejected'):
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=non_existent)

            # 2. Symlink binary rejected
            symlink_bin = root / 'symlink_bin'
            symlink_bin.symlink_to(binary_file)
            with self.assertRaisesRegex(ValueError, 'e2_binary_file_rejected'):
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=symlink_bin)

            # 3. Valid executable file succeeds and hashes match
            expected_hash = hashlib.sha256(binary_file.read_bytes()).hexdigest()
            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertEqual(result['binary_sha256'], expected_hash)
            self.assertEqual(result['paperload_binary_sha256'], expected_hash)

    def test_load_e2_result_bundle_strict_safe_equal_samples_and_seed_check(self):
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            # 1. Tampered sampling qualification rejected by strict _safe_equal_samples
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'strict1', ctx,
                a_mod=lambda a: a['equal_samples'].update({'sampling_qualification': 'fake_qualification'}))
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 2. Negative counter rejected by strict _safe_equal_samples
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'strict2', ctx,
                a_mod=lambda a: a['equal_samples'].update({'schedule_misses': -1}))
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

            # 3. Seed mismatch in workload rejected
            b_dir, b_bin, prep = self._create_test_equal_samples_bundle(
                root / 'strict3', ctx,
                a_mod=lambda a: a.update({'seed': 99999}))
            with self.assertRaisesRegex(ValueError, 'e2_seed_mismatch'):
                e2._load_e2_result_bundle(b_dir, prep, b_bin)

    def test_finish_remote_collection_equal_samples_artifact_and_dispatch(self):
        class MockCollector:
            def __init__(self):
                self.stopped = False
                self.metrics = []
                self.resources = []
                self.errors = []
            def stop(self, pid):
                self.stopped = True

        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            collector = MockCollector()
            prepared['collector'] = collector

            class FakeSpec:
                def __init__(self, gw, s):
                    self.gateway_id = gw
                    self.sensor_id = s
                    self.sensor_urn = f'urn:ngsi-ld:Sensor:{gw}:{s}'

            specs = [FakeSpec('gw-a', 's-a'), FakeSpec('gw-b', 's-b')]
            prepared['specs'] = specs
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60,
                'Accepted': 60,
                'Committed': 60,
                'ACKPublished': 60,
                'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing}
            prepared['before_targets'] = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                                          {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}]
            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1',
                'ingress': term_ing,
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql:
                    return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({
                        'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                        'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0,
                    })
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql:
                    return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({
                        'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                        'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0,
                    })
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001',
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            self.assertTrue(collector.stopped)
            self.assertEqual(result['phase_id'], 'equal-samples-100')
            self.assertEqual(result['workload'], 'equal_samples')
            self.assertEqual(result['status'], 'PASS')
            self.assertTrue(result['clean_comparison'])
            self.assertEqual(result['contamination_reasons'], [])
            self.assertEqual(result['generator_colocated'], 'unverified')
            self.assertEqual(result['physical_ab'], 'NOT RUN')
            self.assertIn('context', result)
            self.assertIn('expected_counts', result)
            self.assertIn('actual_counters', result)
            self.assertIn('binary_provenance', result)
            # Verify artifact written with equal-samples name
            artifact_file = FakeFixture.artifacts / 'p8-e2-equal-samples-100-remote-result.json'
            smoke.durable_json.assert_called_with(artifact_file, result)

    def test_e2_compact_sampler_delayed_start_beyond_64_snapshots(self):
        """Sampler must continue sampling past 64 snapshots up to deadline/cap without stopping."""
        clock = {'mono': 100.0, 'wall': datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}
        def fake_mono():
            return clock['mono']
        def fake_wall():
            return clock['wall'].isoformat()
        def fake_scrape():
            return {
                'process_instance': 'proc-1',
                'ingress': {'Received': 0, 'Committed': 0, 'Accepted': 0},
                'persistence': {'copy_calls': 0},
            }

        sampler = e2.EqualSamplesCompactSampler(
            scrape=fake_scrape,
            interval=1.0,
            max_snapshots=3600,
            deadline_seconds=1800,
            time_fn=fake_mono,
            wall_fn=fake_wall,
        )
        sampler.start_manual()
        for i in range(70):
            clock['mono'] += 1.0
            clock['wall'] += timedelta(seconds=1)
            sampler.sample_step(phase=f'sample_{i}')

        coverage = sampler.get_coverage()
        self.assertEqual(coverage['snapshots_captured'], 71)  # initial + 70 steps
        self.assertEqual(coverage['snapshots_retained'], 71)
        self.assertEqual(coverage['snapshots_evicted'], 0)
        self.assertEqual(sampler.errors, [])
        self.assertGreater(len(sampler.retained_snapshots), 64)

    def test_e2_compact_sampler_retains_real_db_counters_and_process_windows(self):
        counters = {'begin_attempts': 4, 'begin_succeeded': 3, 'confirmed_commits': 2,
                    'copy_calls': 1, 'copy_returned_rows': 100, 'confirmed_new_rows': 100}
        baseline = {'process_instance': 'proc-a', 'monotonic': 10,
                    'persistence': counters, 'resources': {'backend': {
                        'cpu_cumulative_seconds': 2.0, 'memory_bytes': 100}}}
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {
            'process_instance': 'proc-a', 'persistence': counters}, baseline_metrics=baseline,
            baseline_resources=baseline['resources'])
        sampler.start_manual()
        counters.update(begin_attempts=9, begin_succeeded=8, confirmed_commits=7,
                        copy_calls=3, copy_returned_rows=500, confirmed_new_rows=500)
        sampler.sample_step()
        sampler.record_terminal({'process_instance': 'proc-a', 'monotonic': 12,
                                 'persistence': counters}, {'backend': {
                                     'cpu_cumulative_seconds': 5.0, 'memory_bytes': 200}})

        self.assertEqual(sampler.baseline_metrics['persistence']['begin_attempts'], 4)
        self.assertEqual(sampler.terminal_metrics['persistence']['confirmed_commits'], 7)
        self.assertEqual(sampler.terminal_metrics['persistence']['copy_calls'], 3)
        self.assertEqual(sampler.baseline_metrics['process_instance'], 'proc-a')
        self.assertEqual(sampler.terminal_metrics['process_instance'], 'proc-a')
        self.assertEqual(sampler.baseline_resources['backend']['cpu_cumulative_seconds'], 2.0)
        self.assertEqual(sampler.terminal_resources['backend']['cpu_cumulative_seconds'], 5.0)

    def test_e2_terminal_process_reset_and_missing_resources_stay_unavailable(self):
        sampler = e2.EqualSamplesCompactSampler(scrape=lambda: {
            'process_instance': 'proc-a', 'persistence': {'confirmed_commits': 1}},
            baseline_metrics={'process_instance': 'proc-a', 'persistence': {'confirmed_commits': 0}},
            baseline_resources={'backend': {'cpu_cumulative_seconds': 1.0, 'available': True}})
        sampler.start_manual()
        sampler.record_terminal({'process_instance': 'proc-b', 'persistence': {'confirmed_commits': 2}}, None)
        self.assertEqual(sampler.terminal_metrics['process_instance'], 'proc-b')
        self.assertIsNone(sampler.terminal_resources)
        self.assertIsNone(sampler.terminal_metrics.get('resources'))

    def test_e2_db_counter_window_uses_real_counters_and_refuses_reset_or_process_change(self):
        before = {'process_instance': 'p1', 'persistence': {
            'begin_attempts': 2, 'begin_succeeded': 2, 'confirmed_commits': 1,
            'copy_calls': 0, 'copy_returned_rows': 0, 'confirmed_new_rows': 0,
            'fallback_attempts': 0, 'rollback_attempts': 0, 'confirmed_rollbacks': 0,
            'commit_unconfirmed': 0}}
        after = {'process_instance': 'p1', 'persistence': {
            'begin_attempts': 5, 'begin_succeeded': 4, 'confirmed_commits': 3,
            'copy_calls': 1, 'copy_returned_rows': 200, 'confirmed_new_rows': 200,
            'fallback_attempts': 1, 'rollback_attempts': 1, 'confirmed_rollbacks': 1,
            'commit_unconfirmed': 0}}
        result = e2.e2_database_counter_window(before, after)
        self.assertTrue(result['available'])
        self.assertEqual(result['deltas']['begin_attempts'], 3)
        self.assertEqual(result['deltas']['confirmed_commits'], 2)
        self.assertEqual(result['deltas']['copy_calls'], 1)
        self.assertEqual(result['deltas']['copy_returned_rows'], 200)
        self.assertFalse(e2.e2_database_counter_window(
            before, dict(after, process_instance='p2'))['available'])
        reset = dict(after, persistence=dict(after['persistence'], confirmed_commits=0))
        self.assertEqual(e2.e2_database_counter_window(before, reset)['reason'],
                         'db_counter_missing_or_reset:confirmed_commits')
        missing = dict(after, persistence=dict(after['persistence']))
        del missing['persistence']['begin_attempts']
        self.assertFalse(e2.e2_database_counter_window(before, missing)['available'])

    def test_e2_compact_sampler_ring_eviction_preserves_baseline_and_terminal(self):
        """Ring buffer eviction drops oldest rolling samples but keeps baseline/terminal outside ring."""
        clock = {'mono': 100.0, 'wall': datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}
        baseline_m = {'process_instance': 'proc-1', 'ingress': {'Committed': 0}}
        baseline_r = {'backend': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 1000}}
        terminal_m = {'process_instance': 'proc-1', 'ingress': {'Committed': 60}}
        terminal_r = {'backend': {'cpu_cumulative_seconds': 5.0, 'memory_bytes': 1500}}

        sampler = e2.EqualSamplesCompactSampler(
            scrape=lambda: {'process_instance': 'proc-1', 'ingress': {'Committed': 10}},
            interval=1.0,
            max_snapshots=5,
            deadline_seconds=1800,
            baseline_metrics=baseline_m,
            baseline_resources=baseline_r,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'].isoformat(),
        )
        sampler.start_manual()
        for i in range(10):
            clock['mono'] += 1.0
            clock['wall'] += timedelta(seconds=1)
            sampler.sample_step(phase=f'sample_{i}')
        sampler.record_terminal(terminal_m, terminal_r)

        coverage = sampler.get_coverage()
        self.assertEqual(coverage['snapshots_retained'], 5)
        self.assertEqual(coverage['snapshots_evicted'], 6)  # 1 initial + 10 steps = 11 total; 11 - 5 = 6 evicted
        # Baseline and terminal must be strictly preserved outside the ring
        self.assertEqual(sampler.baseline_metrics['ingress']['Committed'], 0)
        self.assertEqual(sampler.baseline_resources['backend']['cpu_cumulative_seconds'], 1.0)
        self.assertEqual(sampler.terminal_metrics['ingress']['Committed'], 60)
        self.assertEqual(sampler.terminal_resources['backend']['cpu_cumulative_seconds'], 5.0)

    def test_e2_compact_sampler_explicit_byte_cap_eviction(self):
        """Byte cap eviction evicts older snapshots when serialized ring size exceeds byte cap."""
        clock = {'mono': 100.0, 'wall': datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}
        sampler = e2.EqualSamplesCompactSampler(
            scrape=lambda: {'process_instance': 'proc-1', 'ingress': {'Committed': 10, 'Received': 10}},
            interval=1.0,
            max_snapshots=100,
            byte_cap=1200,  # small byte cap
            deadline_seconds=1800,
            time_fn=lambda: clock['mono'],
            wall_fn=lambda: clock['wall'].isoformat(),
        )
        sampler.start_manual()
        for i in range(15):
            clock['mono'] += 1.0
            clock['wall'] += timedelta(seconds=1)
            sampler.sample_step(phase=f'sample_{i}')

        coverage = sampler.get_coverage()
        self.assertGreater(coverage['snapshots_evicted'], 0)
        self.assertGreater(coverage['evicted_bytes'], 0)
        self.assertLessEqual(coverage['ring_bytes_used'], 1200)

    def test_e2_active_window_counter_reset_inconclusive(self):
        """Active window inference rejects counter decrease as counter_reset -> INCONCLUSIVE."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 10, 'Received': 10, 'SamplesNew': 1000}}
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 5, 'Received': 10, 'SamplesNew': 1000}}  # Reset!
        terminal = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}

        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'counter_reset')

    def test_e2_active_window_container_identity_change_inconclusive(self):
        """Active window inference rejects container restart / ID change as container_identity_changed."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0},
                    'resources': {'backend': {'container_id': 'cid-1'}}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 30, 'Received': 30, 'SamplesNew': 3000},
              'resources': {'backend': {'container_id': 'cid-1'}}}
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000},
              'resources': {'backend': {'container_id': 'cid-2'}}}  # Restart!
        terminal = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000},
                    'resources': {'backend': {'container_id': 'cid-2'}}}

        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertEqual(res['reason'], 'container_identity_changed')

    def test_e2_active_window_missing_terminal_or_post_boundary_inconclusive(self):
        """Active window inference requires stable post-boundary observation."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 30, 'Received': 30, 'SamplesNew': 3000}}
        s3 = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}

        # terminal missing, and no post snapshot confirming flatness
        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2, s3],
            baseline=baseline,
            terminal=None,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertIn(res['reason'], ('post_boundary_missing', 'terminal_missing'))

    def test_e2_active_window_short_burst_inconclusive(self):
        """When all messages land in a single snapshot interval, window is marked INCONCLUSIVE."""
        baseline = {'timestamp': 't0', 'monotonic': 100.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        s1 = {'timestamp': 't1', 'monotonic': 101.0, 'process_instance': 'p1',
              'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0}}
        # Everything committed in single snapshot interval
        s2 = {'timestamp': 't2', 'monotonic': 102.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}
        s3 = {'timestamp': 't3', 'monotonic': 103.0, 'process_instance': 'p1',
              'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}
        terminal = {'timestamp': 't4', 'monotonic': 104.0, 'process_instance': 'p1',
                    'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 6000}}

        res = e2.infer_host_a_active_window(
            snapshots=[s1, s2, s3],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertIn(res['reason'], ('sparse_observations', 'short_burst_unresolved'))

    def test_e2_active_window_valid_window_and_normalized_metrics(self):
        """Normal progressive workload yields valid active window and normalized metrics."""
        baseline = {
            'timestamp': '2026-01-01T12:00:00Z', 'monotonic': 100.0, 'process_instance': 'p1',
            'ingress': {'Committed': 0, 'Received': 0, 'AcceptedBytes': 0, 'SamplesNew': 0},
            'persistence': {'copy_calls': 0},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 10.0, 'memory_bytes': 10000000},
                'postgres': {'cpu_cumulative_seconds': 5.0, 'memory_bytes': 20000000},
                'mosquitto': {'cpu_cumulative_seconds': 2.0, 'memory_bytes': 5000000},
                'nginx': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 3000000},
            }
        }
        s1 = {
            'timestamp': '2026-01-01T12:00:01Z', 'monotonic': 101.0, 'process_instance': 'p1',
            'ingress': {'Committed': 0, 'Received': 0, 'AcceptedBytes': 0, 'SamplesNew': 0},
            'persistence': {'copy_calls': 0},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 10.1, 'memory_bytes': 10000000},
                'postgres': {'cpu_cumulative_seconds': 5.0, 'memory_bytes': 20000000},
                'mosquitto': {'cpu_cumulative_seconds': 2.0, 'memory_bytes': 5000000},
                'nginx': {'cpu_cumulative_seconds': 1.0, 'memory_bytes': 3000000},
            }
        }
        s2 = {
            'timestamp': '2026-01-01T12:00:02Z', 'monotonic': 102.0, 'process_instance': 'p1',
            'ingress': {'Committed': 20, 'Received': 20, 'AcceptedBytes': 20000, 'SamplesNew': 2000},
            'persistence': {'copy_calls': 20},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 11.0, 'memory_bytes': 12000000},
                'postgres': {'cpu_cumulative_seconds': 6.0, 'memory_bytes': 22000000},
                'mosquitto': {'cpu_cumulative_seconds': 2.5, 'memory_bytes': 5500000},
                'nginx': {'cpu_cumulative_seconds': 1.2, 'memory_bytes': 3200000},
            }
        }
        s3 = {
            'timestamp': '2026-01-01T12:00:03Z', 'monotonic': 103.0, 'process_instance': 'p1',
            'ingress': {'Committed': 40, 'Received': 40, 'AcceptedBytes': 40000, 'SamplesNew': 4000},
            'persistence': {'copy_calls': 40},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 12.0, 'memory_bytes': 13000000},
                'postgres': {'cpu_cumulative_seconds': 7.0, 'memory_bytes': 23000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.0, 'memory_bytes': 5600000},
                'nginx': {'cpu_cumulative_seconds': 1.5, 'memory_bytes': 3300000},
            }
        }
        s4 = {
            'timestamp': '2026-01-01T12:00:04Z', 'monotonic': 104.0, 'process_instance': 'p1',
            'ingress': {'Committed': 60, 'Received': 60, 'AcceptedBytes': 60000, 'SamplesNew': 6000},
            'persistence': {'copy_calls': 60},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 13.0, 'memory_bytes': 14000000},
                'postgres': {'cpu_cumulative_seconds': 8.0, 'memory_bytes': 24000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.5, 'memory_bytes': 5800000},
                'nginx': {'cpu_cumulative_seconds': 1.8, 'memory_bytes': 3400000},
            }
        }
        s5 = {
            'timestamp': '2026-01-01T12:00:05Z', 'monotonic': 105.0, 'process_instance': 'p1',
            'ingress': {'Committed': 60, 'Received': 60, 'AcceptedBytes': 60000, 'SamplesNew': 6000},
            'persistence': {'copy_calls': 60},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 13.1, 'memory_bytes': 14000000},
                'postgres': {'cpu_cumulative_seconds': 8.0, 'memory_bytes': 24000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.5, 'memory_bytes': 5800000},
                'nginx': {'cpu_cumulative_seconds': 1.8, 'memory_bytes': 3400000},
            }
        }
        terminal = {
            'timestamp': '2026-01-01T12:00:06Z', 'monotonic': 106.0, 'process_instance': 'p1',
            'ingress': {'Committed': 60, 'Received': 60, 'AcceptedBytes': 60000, 'SamplesNew': 6000},
            'persistence': {'copy_calls': 60},
            'resources': {
                'backend': {'cpu_cumulative_seconds': 13.5, 'memory_bytes': 14000000},
                'postgres': {'cpu_cumulative_seconds': 8.2, 'memory_bytes': 24000000},
                'mosquitto': {'cpu_cumulative_seconds': 3.6, 'memory_bytes': 5800000},
                'nginx': {'cpu_cumulative_seconds': 1.9, 'memory_bytes': 3400000},
            }
        }

        win = e2.infer_host_a_active_window(
            snapshots=[s1, s2, s3, s4, s5],
            baseline=baseline,
            terminal=terminal,
            expected_total_messages=60,
            expected_total_samples=6000,
            sampling_interval=1.0,
            evicted_count=0,
        )
        self.assertEqual(win['status'], 'AVAILABLE')
        self.assertEqual(win['start_sample_index'], 1)  # s2 (at index 1) is first change
        self.assertEqual(win['end_sample_index'], 3)    # s4 (at index 3) reached 60 commits
        self.assertAlmostEqual(win['duration_seconds'], 2.0)
        self.assertLessEqual(win['duration_uncertainty_seconds'], 1.0)
        self.assertAlmostEqual(win['active_cpu_seconds']['total'], 8.2)

        norm = e2.calculate_equal_samples_normalized_metrics(
            actual_n=6000,
            total_payload_bytes=60000,
            total_db_transactions=60,
            active_window=win,
            lifecycle_window={'duration_seconds': 6.0, 'baseline_cpu_seconds': 18.0, 'terminal_cpu_seconds': 27.2},
        )
        self.assertAlmostEqual(norm['bytes_per_sample'], 10.0)
        self.assertAlmostEqual(norm['db_transactions_per_sample'], 0.01)
        self.assertTrue(norm['active_cpu_available'])
        self.assertAlmostEqual(norm['active_cpu_seconds_per_sample'], win['active_cpu_seconds']['total'] / 6000)

    def test_docker_cumulative_cpu_readback_unsupported_honesty(self):
        """When Docker cumulative CPU is unsupported/unreadable, return None and available=False."""
        mock_registry = Mock()
        containers = {
            'backend': {'receipt': {'id': 'cid-b'}, 'name': 'c-backend'},
            'postgres': {'receipt': {'id': 'cid-p'}, 'name': 'c-postgres'},
            'mosquitto': {'receipt': {'id': 'cid-m'}, 'name': 'c-mosquitto'},
            'nginx': {'receipt': {'id': 'cid-n'}, 'name': 'c-nginx'},
        }
        # Docker API returns connection error / failure
        res = e2.read_container_resources(
            mock_registry,
            containers,
            docker_client=lambda cid: {'available': False, 'reason': 'docker_socket_unreachable'},
        )
        self.assertEqual(res['available'], False)
        for svc in ('backend', 'postgres', 'mosquitto', 'nginx'):
            self.assertIsNone(res['services'][svc]['cpu_cumulative_seconds'])
            self.assertFalse(res['services'][svc]['available'])
            self.assertEqual(res['services'][svc]['reason'], 'docker_socket_unreachable')

    def test_finish_remote_collection_equal_samples_reports_measurement_quality_and_resources(self):
        """finish_remote_collection builds measurement_quality and resources separately from functional validity."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(root, ctx)

            class FakeSampler:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                    self.evicted_count = 0
                def stop(self, pid):
                    self.stopped = True
                def get_coverage(self):
                    return {
                        'sampling_interval_seconds': 1.0,
                        'deadline_seconds': 1800.0,
                        'snapshots_captured': 5,
                        'snapshots_retained': 5,
                        'snapshots_evicted': 0,
                        'evicted_bytes': 0,
                        'ring_byte_cap': 2097152,
                        'ring_bytes_used': 2500,
                        'collector_errors': [],
                    }
                @property
                def retained_snapshots(self):
                    return self.metrics
                def record_terminal(self, m, r):
                    self.terminal_metrics = m
                    self.terminal_resources = r

            sampler = FakeSampler()
            prepared['collector'] = sampler
            prepared['specs'] = [Mock(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a'),
                                 Mock(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:s-b')]
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60, 'Accepted': 60, 'AcceptedBytes': 60000,
                'Committed': 60, 'ACKPublished': 60, 'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing, 'monotonic': 100.0,
                                    'timestamp': '2026-01-01T12:00:00Z'}
            prepared['before_targets'] = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                                          {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}]
            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1', 'ingress': term_ing,
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql: return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                                       'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0})
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql: return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                                       'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0})
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            self.assertEqual(result['status'], 'PASS')
            self.assertIn('measurement_quality', result)
            self.assertIn('resources', result)
            self.assertIn('active_window', result['measurement_quality'])
            self.assertIn('lifecycle_window', result['measurement_quality'])
            self.assertIn('coverage', result['measurement_quality'])
            self.assertIn('normalized_metrics', result['measurement_quality'])
            # ES.5b: No generator resources provided -> unavailable with reason, not DEFERRED_ES5B
            self.assertEqual(result['resources']['generator']['status'], 'UNAVAILABLE')
            self.assertEqual(result['resources']['generator']['availability'], 'unavailable')
            self.assertEqual(result['resources']['generator']['reason'], 'no_sampler_evidence')
            self.assertIsNone(result['resources']['generator']['lifecycle_cpu_seconds'])

    def test_finish_remote_collection_with_generator_resources_available(self):
        """When generator resources are in bundle, finish_remote_collection wires generator metrics."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        gen_res_data = self._sample_generator_resources()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, generator_resources=gen_res_data)

            class FakeSampler:
                def __init__(self):
                    self.stopped = False
                    self.errors = []
                    self.metrics = []
                    self.resources = []
                    self.evicted_count = 0
                def stop(self, pid):
                    self.stopped = True
                def get_coverage(self):
                    return {
                        'sampling_interval_seconds': 1.0,
                        'deadline_seconds': 1800.0,
                        'snapshots_captured': 5,
                        'snapshots_retained': 5,
                        'snapshots_evicted': 0,
                        'evicted_bytes': 0,
                        'ring_byte_cap': 2097152,
                        'ring_bytes_used': 2500,
                        'collector_errors': [],
                    }
                @property
                def retained_snapshots(self):
                    return self.metrics
                def record_terminal(self, m, r):
                    self.terminal_metrics = m
                    self.terminal_resources = r

            prepared['collector'] = FakeSampler()
            prepared['specs'] = [Mock(gateway_id='gw-a', sensor_id='s-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:s-a'),
                                 Mock(gateway_id='gw-b', sensor_id='s-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:s-b')]
            prepared['pg'] = 'fake-pg'
            prepared['tokens'] = {'admin': 'admin-tok'}
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
            ingress_fields = (
                'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
                'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
                'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
                'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
                'ShaperStateCapacity', 'Processed', 'ValidationRejected',
                'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
                'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
                'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
            )
            base_ing = {f: 0 for f in ingress_fields}
            term_ing = {f: 0 for f in ingress_fields}
            term_ing.update({
                'Received': 60, 'Accepted': 60, 'AcceptedBytes': 60000,
                'Committed': 60, 'ACKPublished': 60, 'SamplesNew': 6000,
            })
            prepared['baseline'] = {'process_instance': 'p-1', 'ingress': base_ing, 'monotonic': 100.0,
                                    'timestamp': '2026-01-01T12:00:00Z',
                                    'persistence': {'confirmed_commits': 0}}
            prepared['before_targets'] = [{'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
                                          {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}]
            class FakeFixture:
                artifacts = root / 'artifacts'
            FakeFixture.artifacts.mkdir(mode=0o700, parents=True)

            smoke = Mock()
            smoke.http_request.return_value = (200, {}, {
                'process_instance': 'p-1', 'ingress': term_ing,
                'persistence': {'confirmed_commits': 60},
            })
            smoke.paper_p0_stats.return_value = {'cpu': 10}
            smoke.paper_p1_count_db.return_value = {'ledger': 60, 'raw': 6000, 'temporal': 60, 'configuration': 0}
            def fake_psql(reg, pg, sql, params):
                if 'FROM processed_messages p1' in sql: return '0'
                if 'json_build_object' in sql and 'ledger_count' in sql:
                    return json.dumps({'ledger_count': 30, 'min_first_sequence': 0, 'max_last_sequence': 2999,
                                       'total_sample_count': 3000, 'invalid_sample_count': 0, 'invalid_sequence_bounds': 0})
                if 'sample_count IS DISTINCT FROM' in sql or 'sample_count <> (SELECT count(*)' in sql: return '0'
                if 'FROM telemetry t' in sql and 'unique_sequence_count' in sql:
                    return json.dumps({'raw_count': 3000, 'unique_sequence_count': 3000, 'min_sequence': 0,
                                       'max_sequence': 2999, 'value_mismatches': 0, 'timestamp_mismatches': 0})
                if 'LIMIT 20' in sql:
                    return json.dumps([{'message_id': '550e8400-e29b-41d4-a716-446655440001', 'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                return json.dumps({'ledger': 30, 'raw': 3000, 'temporal': 30, 'configuration': 0})
            smoke.db_psql.side_effect = fake_psql

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            self.assertEqual(result['status'], 'PASS')
            gen = result['resources']['generator']
            self.assertEqual(gen['status'], 'AVAILABLE')
            self.assertEqual(gen['availability'], 'available')
            self.assertEqual(gen['role'], 'generator_B')
            self.assertIsNone(gen['reason'])
            # 200 ticks / 100 clk_tck = 2.0 seconds
            self.assertAlmostEqual(gen['lifecycle_cpu_seconds'], 2.0)
            self.assertEqual(gen['cpu_seconds_unit'], 'seconds')
            self.assertEqual(gen['rss_unit'], 'bytes')
            self.assertEqual(gen['sampled_max_rss_bytes'], 35000000)
            self.assertIsNone(gen['absolute_peak_rss_bytes'])
            self.assertIn('sampled_max_rss_not_peak', gen['peak_qualification'])

            # Normalized generator metrics
            norm = result['measurement_quality']['normalized_metrics']
            self.assertTrue(norm['generator_lifecycle_cpu_available'])
            self.assertAlmostEqual(norm['generator_lifecycle_cpu_seconds_per_sample'], 2.0 / 6000)

            # Per-metric quality
            per_metric = result['measurement_quality']['per_metric']
            self.assertEqual(per_metric['payload_bytes']['status'], 'AVAILABLE')
            self.assertEqual(per_metric['db_transactions']['status'], 'AVAILABLE')
            self.assertEqual(per_metric['generator_lifecycle_cpu']['status'], 'AVAILABLE')

    def test_load_e2_result_bundle_rejects_bad_generator_resources_hash_and_shape(self):
        """Tampered generator resource hash or invalid shape is rejected."""
        fixed_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ctx = build_e2_equal_samples_context(total_samples=6000, sample_rate=100, seed=84, now_utc=fixed_now)
        gen_res = self._sample_generator_resources()

        # 1. Digest mismatch
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, generator_resources=gen_res)
            # Tamper the file content
            gen_file = bundle_dir / 'results/runner/resources-generator.json'
            gen_file.write_bytes(b'{"tampered": true}')
            with self.assertRaises(ValueError) as ctx_err:
                e2._load_e2_result_bundle(bundle_dir, prepared, binary_file)
            self.assertIn('e2_result_digest_mismatch', str(ctx_err.exception))

        # 2. Bad shape (unknown keys or invalid role)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bad_gen_res = dict(gen_res, role='invalid_role')
            bundle_dir, binary_file, prepared = self._create_test_equal_samples_bundle(
                root, ctx, generator_resources=bad_gen_res)
            with self.assertRaises(ValueError):
                e2._load_e2_result_bundle(bundle_dir, prepared, binary_file)

    def test_calculate_equal_samples_normalized_metrics_denominator_and_units(self):
        """calculate_equal_samples_normalized_metrics requires positive N and labels units."""
        # Non-positive N
        with self.assertRaises(ValueError) as ctx_err:
            e2.calculate_equal_samples_normalized_metrics(
                actual_n=0, total_payload_bytes=1000, total_db_transactions=10,
                active_window={}, lifecycle_window={})
        self.assertIn('actual_n_must_be_positive_int', str(ctx_err.exception))

        with self.assertRaises(ValueError):
            e2.calculate_equal_samples_normalized_metrics(
                actual_n=-10, total_payload_bytes=1000, total_db_transactions=10,
                active_window={}, lifecycle_window={})

        # Valid N with generator resources
        gen_res = {
            'availability': 'available',
            'lifecycle_cpu_seconds': 3.0,
            'active_cpu_seconds': 1.5,
        }
        res = e2.calculate_equal_samples_normalized_metrics(
            actual_n=3000, total_payload_bytes=60000, total_db_transactions=30,
            active_window={'status': 'AVAILABLE', 'active_cpu_seconds': {'total': 6.0}},
            lifecycle_window={'baseline_cpu_seconds': 10.0, 'terminal_cpu_seconds': 20.0},
            generator_resources=gen_res,
        )
        self.assertEqual(res['actual_samples'], 3000)
        self.assertAlmostEqual(res['bytes_per_sample'], 20.0)
        self.assertAlmostEqual(res['db_transactions_per_sample'], 0.01)
        self.assertAlmostEqual(res['active_cpu_seconds_per_sample'], 6.0 / 3000)
        self.assertAlmostEqual(res['lifecycle_cpu_seconds_per_sample'], 10.0 / 3000)
        self.assertAlmostEqual(res['generator_lifecycle_cpu_seconds_per_sample'], 3.0 / 3000)
        self.assertAlmostEqual(res['generator_active_cpu_seconds_per_sample'], 1.5 / 3000)
        self.assertIn('not_wire_traffic_not_tls', res['payload_bytes_qualification'])
        self.assertIn('not_sensor_acquisition_latency', res['latency_qualification'])

    def test_sparse_window_reports_per_metric_inconclusive_without_suppressing_clean_payload(self):
        """When server active window is sparse/inconclusive, server CPU is INCONCLUSIVE but payload/DB are AVAILABLE."""
        # Empty snapshots -> sparse_observations
        win = e2.infer_host_a_active_window(
            snapshots=[], baseline={'ingress': {'Committed': 0}}, terminal={},
            expected_total_messages=60, expected_total_samples=6000)
        self.assertEqual(win['status'], 'INCONCLUSIVE')
        self.assertEqual(win['reason'], 'sparse_observations')

        norm = e2.calculate_equal_samples_normalized_metrics(
            actual_n=6000, total_payload_bytes=60000, total_db_transactions=60,
            active_window=win, lifecycle_window={})
        self.assertFalse(norm['active_cpu_available'])
        self.assertEqual(norm['active_cpu_unavailable_reason'], 'sparse_observations')
        self.assertAlmostEqual(norm['bytes_per_sample'], 10.0)
        self.assertAlmostEqual(norm['db_transactions_per_sample'], 0.01)

    def test_sampler_projection_retains_confirmed_commits(self):
        """EqualSamplesCompactSampler._project_metrics must project confirmed_commits and supported persistence keys."""
        raw = {
            'persistence': {
                'confirmed_commits': 42,
                'single_attempts': 1,
                'group_attempts': 2,
                'group_members': 3,
                'copy_calls': 4,
                'confirmed_new_rows': 5,
            }
        }
        proj = e2.EqualSamplesCompactSampler._project_metrics(raw)
        self.assertIn('confirmed_commits', proj['persistence'])
        self.assertEqual(proj['persistence']['confirmed_commits'], 42)
        self.assertEqual(proj['persistence']['single_attempts'], 1)
        self.assertEqual(proj['persistence']['group_attempts'], 2)

    def test_active_window_unbracketed_final_commits_returns_inconclusive(self):
        """When snapshots do not observe final commit target with timestamped resources, return INCONCLUSIVE."""
        base = {'monotonic': 100.0, 'timestamp': '2026-10-31T00:00:00Z', 'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0},
                'resources': {'backend': {'cpu_cumulative_seconds': 1.0}, 'postgres': {'cpu_cumulative_seconds': 1.0},
                              'mosquitto': {'cpu_cumulative_seconds': 1.0}, 'nginx': {'cpu_cumulative_seconds': 1.0}}}
        snaps = [
            {'monotonic': 108.0, 'timestamp': '2026-10-31T00:00:08Z', 'ingress': {'Committed': 0, 'Received': 0, 'SamplesNew': 0},
             'resources': {'backend': {'cpu_cumulative_seconds': 1.1}, 'postgres': {'cpu_cumulative_seconds': 1.1},
                           'mosquitto': {'cpu_cumulative_seconds': 1.1}, 'nginx': {'cpu_cumulative_seconds': 1.1}}},
            {'monotonic': 116.0, 'timestamp': '2026-10-31T00:00:16Z', 'ingress': {'Committed': 10, 'Received': 10, 'SamplesNew': 10},
             'resources': {'backend': {'cpu_cumulative_seconds': 2.0}, 'postgres': {'cpu_cumulative_seconds': 2.0},
                           'mosquitto': {'cpu_cumulative_seconds': 2.0}, 'nginx': {'cpu_cumulative_seconds': 2.0}}},
            {'monotonic': 124.0, 'timestamp': '2026-10-31T00:00:24Z', 'ingress': {'Committed': 50, 'Received': 50, 'SamplesNew': 50},
             'resources': {'backend': {'cpu_cumulative_seconds': 3.0}, 'postgres': {'cpu_cumulative_seconds': 3.0},
                           'mosquitto': {'cpu_cumulative_seconds': 3.0}, 'nginx': {'cpu_cumulative_seconds': 3.0}}},
        ]
        term = {'monotonic': None, 'timestamp': None, 'ingress': {'Committed': 60, 'Received': 60, 'SamplesNew': 60}, 'resources': None}

        win = e2.infer_host_a_active_window(
            snapshots=snaps, baseline=base, terminal=term,
            expected_total_messages=60, expected_total_samples=6000, sampling_interval=1.0)
        self.assertEqual(win['status'], 'INCONCLUSIVE')
        self.assertEqual(win['reason'], 'final_commit_boundary_not_observed')

    def test_calculate_normalized_metrics_missing_commit_counter(self):
        """When total_db_transactions is None, db_transactions_per_sample is None and unavailable."""
        res = e2.calculate_equal_samples_normalized_metrics(
            actual_n=6000, total_payload_bytes=60000, total_db_transactions=None,
            active_window={}, lifecycle_window={},
            db_transactions_unavailable_reason='db_transactions_counter_not_retained_in_sampler',
            ledger_rows=6000,
        )
        self.assertIsNone(res['db_transactions_per_sample'])
        self.assertFalse(res['db_transactions_available'])
        self.assertEqual(res['db_transactions_unavailable_reason'], 'db_transactions_counter_not_retained_in_sampler')
        self.assertEqual(res['ledger_rows_per_sample'], 1.0)

    def test_finish_remote_collection_legacy_steady_burst_exact_canonical_delta_pass(self):
        """finish_remote_collection in legacy steady_burst mode derives canonical 4-key delta and passes validation."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # Assert exact canonical 4 keys in delta
            expected_delta = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 0}
            self.assertEqual(result['database_delta'], expected_delta)
            self.assertEqual(set(result['database_delta'].keys()), {'ledger', 'raw', 'temporal', 'configuration'})
            self.assertNotIn('processed', result['database_delta'])
            self.assertNotIn('telemetry', result['database_delta'])

            # Canonical counts alone do not prove exact raw retention. This legacy
            # fixture only verifies sampled identity ranges, so acceptance fails closed.
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['workload_outcome'], 'VALIDATION_FAILED')
            self.assertFalse(result['validation']['valid'])
            self.assertEqual(result['validation']['failures'], ['pilot_raw_retention_verification'])
            self.assertTrue(collector.stopped)

    def test_finish_remote_collection_nonzero_configuration_triggers_failure(self):
        """finish_remote_collection captures actual configuration difference and triggers validation failure."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            # Simulate DB configuration count increased from 0 to 1
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 1}

            result = e2.finish_remote_collection(
                smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)

            # Delta must reflect actual configuration difference, not fake 0
            self.assertEqual(result['database_delta']['configuration'], 1)
            self.assertEqual(result['status'], 'FAIL')
            self.assertFalse(result['validation']['valid'])
            self.assertIn('database_delta_mismatch', result['validation']['failures'])

    def test_finish_remote_collection_count_validation_fails_closed(self):
        """finish_remote_collection fails closed on missing, boolean, negative, or reset count input."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bundle_dir, binary_file, prepared = self._create_test_legacy_bundle(root, samples_per_message=100)

            # 1. Missing key in before_global
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('missing_key', str(ctx.exception).lower())

            # 2. Missing key in after_global
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('missing_key', str(ctx.exception).lower())

            # 3. Boolean count value in before_global (bool rejected)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            prepared['before_global'] = {'ledger': True, 'raw': 0, 'temporal': 0, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('not_int', str(ctx.exception).lower())

            # 4. Boolean count value in after_global
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': False, 'temporal': 24, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('not_int', str(ctx.exception).lower())

            # 5. Negative count value
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': -100, 'temporal': 24, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('negative', str(ctx.exception).lower())

            # 6. Counter reset (after < before)
            smoke, FakeFixture, collector = self._setup_legacy_finish_env(root, prepared)
            prepared['before_global'] = {'ledger': 100, 'raw': 0, 'temporal': 0, 'configuration': 0}
            smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 0}
            with self.assertRaises(ValueError) as ctx:
                e2.finish_remote_collection(
                    smoke, FakeFixture, Mock(url='https://127.0.0.1:443'), Mock(), {},
                    prepared, bundle_dir, phase_id=prepared['phase_id'], paperload_binary=binary_file)
            self.assertIn('reset', str(ctx.exception).lower())

    def _create_test_legacy_bundle(self, root, samples_per_message=100):
        session_id = 'sess-legacy-e2'
        run_id = 'run-legacy-e2'
        arm = 'A'
        phase_id = f'samples-{samples_per_message}'
        profile = f'S{samples_per_message}'
        bundle_dir = root / 'results'
        bundle_dir.mkdir(parents=True, mode=0o700)
        (bundle_dir / 'results' / 'workloads' / 'A').mkdir(parents=True, mode=0o700)
        (bundle_dir / 'results' / 'workloads' / 'B').mkdir(parents=True, mode=0o700)
        (bundle_dir / 'results' / 'runner').mkdir(parents=True, mode=0o700)

        binary_file = root / 'paperload'
        binary_file.write_bytes(b'fake_binary_content_for_legacy_testing')
        binary_file.chmod(0o700)
        bin_hash = hashlib.sha256(b'fake_binary_content_for_legacy_testing').hexdigest()

        def make_legacy_workload(role):
            expected_m = 8 if role == 'A' else 16
            gw = f'gw_{role.lower()}'
            s = f's_{role.lower()}'
            identity_samples = []
            for i in range(min(expected_m, 20)):
                mid = f'550e8400-e29b-41d4-a716-4466554400{i:02d}'
                identity_samples.append({
                    'viewer_id': '',
                    'gateway_id': gw,
                    'sensor_id': s,
                    'message_id': mid,
                    'boot_id': 'boot-1',
                    'first_sequence': str(i * samples_per_message),
                    'sample_count': samples_per_message,
                    'header_sha256': 'a' * 64,
                })
            return {
                'schema_version': 1,
                'status': 'COMPLETE',
                'implementation_status': 'P8.4b telemetry synthetic-load; no durable restart assurance',
                'experiment': 'E2',
                'arm': role,
                'profile': profile,
                'mode': 'remote',
                'run_id': run_id,
                'durability_mode': 'synthetic-load',
                'role': role,
                'gateway_id': gw,
                'sensor_id': s,
                'started_at': '2026-10-31T12:00:00.000000000Z',
                'finished_at': '2026-10-31T12:01:00.000000000Z',
                'configured_phases': {'warmup_seconds': 0, 'measure_seconds': 60, 'drain_seconds': 20, 'cooldown_seconds': 0},
                'telemetry': {
                    'scheduled_opportunities': expected_m,
                    'generated_unique': expected_m,
                    'offered_unique': expected_m,
                    'application_acks': expected_m,
                    'pending_messages': 0,
                    'pending_bytes': 0,
                    'publish_attempts': expected_m,
                    'broker_pubacks': expected_m,
                    'retries': 0,
                    'invalid_acks': 0,
                },
                'identity_sampling_qualification': 'header_sha256_only; maximum 20 globally',
                'identity_samples': identity_samples,
            }

        workload_a = make_legacy_workload('A')
        workload_b = make_legacy_workload('B')

        run_json = {
            'status': 'COMPLETE',
            'phase_observations': {
                'status': 'observed',
                'events': [
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'elapsed_ns': 100, 'observed_at_runner_monotonic_ns': 1000},
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'elapsed_ns': 60000000000, 'observed_at_runner_monotonic_ns': 61000},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_begin', 'elapsed_ns': 200, 'observed_at_runner_monotonic_ns': 2000},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_end', 'elapsed_ns': 50000000000, 'observed_at_runner_monotonic_ns': 51000},
                ]
            }
        }
        prov_json = {
            'binary_sha256': bin_hash,
            'binary_os': sys.platform,
            'binary_arch': platform.machine()
        }

        files_data = {
            'results/workloads/A/workload.json': json.dumps(workload_a, separators=(',', ':')).encode(),
            'results/workloads/B/workload.json': json.dumps(workload_b, separators=(',', ':')).encode(),
            'results/runner/run.json': json.dumps(run_json, separators=(',', ':')).encode(),
            'results/runner/provenance.json': json.dumps(prov_json, separators=(',', ':')).encode(),
        }
        inventory = []
        for rel_path, data in files_data.items():
            f_path = bundle_dir / rel_path
            f_path.write_bytes(data)
            f_path.chmod(0o600)
            inventory.append({
                'path': rel_path,
                'size_bytes': len(data),
                'sha256': hashlib.sha256(data).hexdigest()
            })

        bundle_sha256 = 'b' * 64
        bundle_inventory = [{'path': 'input.json', 'sha256': 'a' * 64}]
        manifest_a_hash = '1' * 64
        manifest_b_hash = '2' * 64
        run_spec_hash = '3' * 64

        results_summary = {
            'schema_version': 1,
            'session': {
                'session_id': session_id,
                'run_ids': [run_id],
                'experiment': 'E2',
                'arm': arm,
                'phase': phase_id
            },
            'source_bundle_sha256': bundle_sha256,
            'source_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
            'bound_gateway_environment_sha256': {},
            'bound_run_spec_sha256': run_spec_hash,
            'binary': dict(prov_json),
            'files': inventory,
            'runner_status': 'COMPLETE',
            'expected_workload_roles': ['A', 'B'],
            'present_workload_roles': ['A', 'B'],
            'missing_workload_roles': [],
            'full_p8_accepted': False,
            'physical_ab': 'NOT RUN'
        }

        summary_file = bundle_dir / 'results.json'
        summary_file.write_text(json.dumps(results_summary))
        summary_file.chmod(0o600)

        prepared = {
            'session_id': session_id,
            'run_id': run_id,
            'arm': arm,
            'phase_id': phase_id,
            'profile': profile,
            'workload': 'steady_burst',
            'samples_per_message': samples_per_message,
            'bundle_sha256': bundle_sha256,
            'bundle_inventory': bundle_inventory,
            'bound_manifest_sha256': {'A': manifest_a_hash, 'B': manifest_b_hash},
        }
        return bundle_dir, binary_file, prepared

    def _setup_legacy_finish_env(self, root, prepared):
        specs = [
            SimpleNamespace(gateway_id='gw_a', sensor_id='s_a', sensor_urn='urn:ngsi-ld:Sensor:gw_a:s_a'),
            SimpleNamespace(gateway_id='gw_b', sensor_id='s_b', sensor_urn='urn:ngsi-ld:Sensor:gw_b:s_b'),
        ]
        prepared['specs'] = specs
        prepared['pg'] = 'fake-pg'
        prepared['tokens'] = {'admin': 'admin-tok'}
        prepared['before_global'] = {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0}
        prepared['before_targets'] = [
            {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
            {'ledger': 0, 'raw': 0, 'temporal': 0, 'configuration': 0},
        ]
        ingress_fields = (
            'Received', 'ReceivedBytes', 'Accepted', 'AcceptedBytes',
            'RejectedNotRunning', 'RejectedStopping', 'RejectedRetained',
            'RejectedOversized', 'RejectedInvalidTopic', 'QueueFull',
            'ByteBudgetFull', 'ShaperPassed', 'ShaperRateLimited',
            'ShaperStateCapacity', 'Processed', 'ValidationRejected',
            'PersistFailures', 'CommitUnconfirmed', 'IdentityMismatch',
            'ACKBuildFailures', 'Committed', 'DuplicateCommitted',
            'SamplesNew', 'ACKPublished', 'ACKPublishFailures'
        )
        base_ing = {f: 0 for f in ingress_fields}
        term_ing = {f: 0 for f in ingress_fields}
        term_ing.update({
            'Received': 24, 'Accepted': 24, 'Committed': 24,
            'ACKPublished': 24, 'SamplesNew': 2400,
            'ShaperRateLimited': 27,
        })
        prepared['baseline'] = {'process_instance': 'proc-1', 'ingress': base_ing}

        class FakeCollector:
            def __init__(self):
                self.stopped = False
                self.errors = []
                self.metrics = []
                self.resources = []
            def stop(self, _):
                self.stopped = True

        collector = FakeCollector()
        prepared['collector'] = collector

        class FakeFixture:
            artifacts = root / 'artifacts'
        FakeFixture.artifacts.mkdir(mode=0o700, parents=True, exist_ok=True)

        smoke = Mock()
        smoke.ROOT = str(root)
        smoke.durable_json = lambda p, d: Path(p).write_text(json.dumps(d))
        smoke.http_request.return_value = (200, {}, {
            'process_instance': 'proc-1',
            'ingress': term_ing,
        })
        smoke.paper_p0_stats.return_value = {'cpu': 5}
        smoke.paper_p1_count_db.return_value = {'ledger': 24, 'raw': 2400, 'temporal': 24, 'configuration': 0}

        def fake_psql(reg, pg, sql, params=None):
            if 'FROM processed_messages WHERE gateway_id=:' in sql or 'SELECT count(*) FROM processed_messages WHERE gateway_id=:' in sql:
                if params.get('gw') == 'gw_a':
                    return json.dumps({'ledger': 8, 'raw': 800, 'temporal': 8, 'configuration': 0})
                return json.dumps({'ledger': 16, 'raw': 1600, 'temporal': 16, 'configuration': 0})
            if 'FROM processed_messages p WHERE' in sql and 'coalesce(json_agg' in sql:
                count = 8 if params.get('gw') == 'gw_a' else 16
                rows = [{'message_id': f'550e8400-e29b-41d4-a716-4466554400{i:02d}',
                         'first_sequence': str(i * 100), 'last_sequence': str((i + 1) * 100 - 1),
                         'sample_count': 100} for i in range(count)]
                return json.dumps(rows)
            if 'FROM processed_messages p WHERE' in sql and 'sample_count <>' in sql:
                return '0'
            return json.dumps({'ledger': 8, 'raw': 800, 'temporal': 8, 'configuration': 0})

        smoke.db_psql.side_effect = fake_psql
        return smoke, FakeFixture, collector

    @staticmethod
    def _sample_generator_resources():
        return {
            'schema_version': 1,
            'role': 'generator_B',
            'cpu_normalization': 'host_percent',
            'cpu_normalization_description': 'ticks_delta / CLK_TCK / interval_seconds / host_cpus * 100',
            'host_cpus': 4,
            'clk_tck': 100,
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
                'monotonic_start': 500.0,
                'sample_interval_seconds': 1.0,
                'deadline_seconds': 120.0,
                'max_snapshots': 128,
            },
            'summary': {
                'total_samples': 3,
                'available_samples': 3,
                'unavailable_samples': 0,
                'cpu_available_samples': 2,
                'max_sample_rss_bytes': 35000000,
                'max_sample_rss_qualification': 'sum_sampled_RSS_may_double_count_shared_pages_not_peak',
                'fd_scope': 'root_process_only',
                'missing_reasons': [],
                'errors': [],
            },
            'samples': [
                {
                    'sample_index': 0,
                    'timestamp': '2026-01-01T12:00:00Z',
                    'elapsed_seconds': 0.0,
                    'interval_seconds': None,
                    'available': True,
                    'cpu_percent': None,
                    'delta_ticks': None,
                    'rss_bytes': 30000000,
                    'process_count': 3,
                    'current_fd_count': 12,
                    'reason': None,
                },
                {
                    'sample_index': 1,
                    'timestamp': '2026-01-01T12:00:01Z',
                    'elapsed_seconds': 1.0,
                    'interval_seconds': 1.0,
                    'available': True,
                    'cpu_percent': 25.0,
                    'delta_ticks': 100,
                    'rss_bytes': 35000000,
                    'process_count': 3,
                    'current_fd_count': 12,
                    'reason': None,
                },
                {
                    'sample_index': 2,
                    'timestamp': '2026-01-01T12:00:02Z',
                    'elapsed_seconds': 2.0,
                    'interval_seconds': 1.0,
                    'available': True,
                    'cpu_percent': 25.0,
                    'delta_ticks': 100,
                    'rss_bytes': 34000000,
                    'process_count': 3,
                    'current_fd_count': 12,
                    'reason': None,
                },
            ],
        }


class GeneratorResourceQualificationTests(unittest.TestCase):
    """Test suite for generator resource qualification, clock origin alignment, and interval validation."""

    def test_qualify_generator_resources_shifted_origin(self):
        """Host B monotonic alignment correctly computes active window despite shifted generator clock origin.

        Child-local Go elapsed_ns (e.g. 0.005s) must NEVER be used directly;
        observed_at_runner_monotonic_ns / 1e9 - clock.monotonic_start gives true generator elapsed timeline.
        """
        # Generator sampler started at monotonic 980.0
        gen_data = {
            'schema_version': 1,
            'role': 'generator_B',
            'clk_tck': 100,
            'clock': {
                'monotonic_start': 980.0,
                'sample_interval_seconds': 1.0,
                'deadline_seconds': 120.0,
                'max_snapshots': 128,
            },
            'samples': [
                {
                    'sample_index': 0,
                    'elapsed_seconds': 0.0,
                    'interval_seconds': None,
                    'available': True,
                    'delta_ticks': None,
                    'reason': 'first_sample_baseline_unavailable',
                    'rss_bytes': 30000000,
                }
            ] + [
                {
                    'sample_index': i,
                    'elapsed_seconds': float(i),
                    'interval_seconds': 1.0,
                    'available': True,
                    'delta_ticks': 100,
                    'reason': None,
                    'rss_bytes': 35000000,
                }
                for i in range(1, 85)
            ]
        }

        # Runner events on Host B monotonic timeline:
        # measure_begin at 1000.5s -> active_start = 1000.5 - 980.0 = 20.5s
        # measure_end at 1060.5s -> active_end = 1060.5 - 980.0 = 80.5s
        events = [
            {
                'child': 'paperload-telemetry:B.json',
                'event': 'measure_begin',
                'elapsed_ns': 5000000,  # 0.005s process-local child elapsed
                'observed_at_runner_monotonic_ns': 1000500000000,
                'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
            },
            {
                'child': 'paperload-telemetry:B.json',
                'event': 'measure_end',
                'elapsed_ns': 60005000000,  # 60.005s process-local child elapsed
                'observed_at_runner_monotonic_ns': 1060500000000,
                'clock_qualification': 'runner observed pipe receipt; observation lag unknown; child elapsed is process-local',
            },
        ]

        res = e2.qualify_generator_resources(gen_data, events)
        self.assertEqual(res['status'], 'AVAILABLE')
        self.assertTrue(res['active_cpu_available'])
        self.assertAlmostEqual(res['active_window']['start_seconds'], 20.5)
        self.assertAlmostEqual(res['active_window']['end_seconds'], 80.5)
        self.assertTrue(res['active_window']['aligned_on_host_b_monotonic'])
        self.assertIn('pipe_receipt_lag_unknown', res['clock_qualification'])
        self.assertIn('aligned on host B monotonic timeline', res['clock_qualification'])
        # Samples with 20.5 <= elapsed_seconds <= 80.5 are samples 21 through 80 = 60 samples
        # 60 * 100 delta_ticks / 100 clk_tck = 60.0 seconds
        self.assertAlmostEqual(res['active_cpu_seconds'], 60.0)

    def test_qualify_generator_resources_invalid_interval_and_gap(self):
        """Invalid intervals, gaps, and process changes fail closed to INCONCLUSIVE."""
        base_clock = {
            'monotonic_start': 100.0,
            'sample_interval_seconds': 1.0,
            'deadline_seconds': 120.0,
            'max_snapshots': 128,
        }

        # 1. Reversed interval: begin > end
        events_rev = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 160000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 120000000000},
        ]
        gen_data = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [{'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None}],
        }
        res = e2.qualify_generator_resources(gen_data, events_rev)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'active_window_reversed_or_empty')

        # 2. Negative start
        events_neg = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 90000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 150000000000},
        ]
        res = e2.qualify_generator_resources(gen_data, events_neg)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertTrue(res['active_cpu_unavailable_reason'].startswith('active_start_negative'))

        # 3. Window start not bracketed
        events_valid = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 110000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 150000000000},
        ]
        # First sample is at 15.0s, which is after active_start (10.0s)
        gen_unbracketed_start = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 15.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 60.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_unbracketed_start, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'active_window_start_not_bracketed')

        # 4. Window end not bracketed
        # Last sample is at 40.0s, which is before active_end (50.0s)
        gen_unbracketed_end = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 40.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_unbracketed_end, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'active_window_end_not_bracketed')

        # 5. Sampling gap within active window
        gen_gap = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 12.0, 'interval_seconds': 12.0, 'available': True, 'delta_ticks': 100, 'reason': None},
                {'sample_index': 2, 'elapsed_seconds': 55.0, 'interval_seconds': 43.0, 'available': True, 'delta_ticks': 100, 'reason': None},
            ],
        }
        res = e2.qualify_generator_resources(gen_gap, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertTrue(res['active_cpu_unavailable_reason'].startswith('sampling_gap_within_active_window'))

        # 6. Process change in active window (process_appeared)
        gen_proc_appeared = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
                {'sample_index': 1, 'elapsed_seconds': 20.0, 'interval_seconds': 1.0, 'available': True, 'delta_ticks': None, 'reason': 'process_appeared'},
                {'sample_index': 2, 'elapsed_seconds': 55.0, 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 100, 'reason': None},
            ],
        }
        res = e2.qualify_generator_resources(gen_proc_appeared, events_valid)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertFalse(res['active_cpu_available'])
        self.assertEqual(res['active_cpu_unavailable_reason'], 'invalid_delta_ticks_in_active_window:process_appeared')

    def test_qualify_generator_resources_max_120s_boundary(self):
        """Deadlines, samples, or active windows exceeding 120s max boundary fail closed."""
        events_120 = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 100000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 225000000000},
        ]
        # 1. Deadline exceeds 120s
        gen_bad_deadline = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': {'monotonic_start': 100.0, 'sample_interval_seconds': 1.0, 'deadline_seconds': 125.0, 'max_snapshots': 128},
            'samples': [{'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None}],
        }
        res = e2.qualify_generator_resources(gen_bad_deadline, events_120)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertTrue('max_120s_boundary_exceeded' in res['active_cpu_unavailable_reason'])

        # 2. Active window exceeds 120s (active_end = 225 - 100 = 125s)
        gen_window_125 = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': {'monotonic_start': 100.0, 'sample_interval_seconds': 1.0, 'deadline_seconds': 120.0, 'max_snapshots': 128},
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 125.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_window_125, events_120)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertTrue('max_120s_boundary_exceeded' in res['active_cpu_unavailable_reason'])

        # 3. Sample elapsed exceeds 120s
        events_ok = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 110000000000},
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 150000000000},
        ]
        gen_sample_121 = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': {'monotonic_start': 100.0, 'sample_interval_seconds': 1.0, 'deadline_seconds': 120.0, 'max_snapshots': 128},
            'samples': [
                {'sample_index': 0, 'elapsed_seconds': 0.0, 'available': True, 'delta_ticks': None},
                {'sample_index': 1, 'elapsed_seconds': 121.0, 'available': True, 'delta_ticks': 100},
            ],
        }
        res = e2.qualify_generator_resources(gen_sample_121, events_ok)
        self.assertEqual(res['status'], 'INCONCLUSIVE')
        self.assertTrue('max_120s_boundary_exceeded' in res['active_cpu_unavailable_reason'])

    def test_qualify_generator_resources_complete_valid_interval(self):
        """Complete valid intervals compute CPU; CPU=0 is valid only with complete evidence; partial observed CPU labeled."""
        base_clock = {
            'monotonic_start': 500.0,
            'sample_interval_seconds': 1.0,
            'deadline_seconds': 120.0,
            'max_snapshots': 128,
        }
        events = [
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 510000000000},  # 10.0s
            {'child': 'paperload-telemetry:B.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 530000000000},    # 30.0s
        ]

        # 1. Complete valid interval with CPU > 0
        samples_valid = [
            {'sample_index': 0, 'elapsed_seconds': 0.0, 'interval_seconds': None, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
        ] + [
            {'sample_index': i, 'elapsed_seconds': float(i), 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 50, 'reason': None}
            for i in range(1, 35)
        ]
        gen_data_valid = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': samples_valid,
        }
        res = e2.qualify_generator_resources(gen_data_valid, events)
        self.assertEqual(res['status'], 'AVAILABLE')
        self.assertTrue(res['active_cpu_available'])
        # Active window [10.0, 30.0]: samples 11..30 (covering 10.0 to 30.0s = 20s) = 20 samples * 50 ticks / 100 = 10.0s
        self.assertAlmostEqual(res['active_cpu_seconds'], 10.0)
        self.assertTrue(res['lifecycle_cpu_available'])
        # Lifecycle samples 1..34 = 34 samples * 50 ticks / 100 = 17.0s
        self.assertAlmostEqual(res['lifecycle_cpu_seconds'], 17.0)
        self.assertAlmostEqual(res['partial_observed_lifecycle_cpu_seconds'], 17.0)

        # 2. CPU=0 valid only when evidence complete
        samples_zero = [
            {'sample_index': 0, 'elapsed_seconds': 0.0, 'interval_seconds': None, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
        ] + [
            {'sample_index': i, 'elapsed_seconds': float(i), 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 0, 'reason': None}
            for i in range(1, 35)
        ]
        gen_data_zero = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': samples_zero,
        }
        res_zero = e2.qualify_generator_resources(gen_data_zero, events)
        self.assertEqual(res_zero['status'], 'AVAILABLE')
        self.assertTrue(res_zero['active_cpu_available'])
        self.assertEqual(res_zero['active_cpu_seconds'], 0.0)
        self.assertTrue(res_zero['lifecycle_cpu_available'])
        self.assertEqual(res_zero['lifecycle_cpu_seconds'], 0.0)

        # 3. Partial observed lifecycle CPU when startup process change occurs
        samples_partial = [
            {'sample_index': 0, 'elapsed_seconds': 0.0, 'interval_seconds': None, 'available': True, 'delta_ticks': None, 'reason': 'first_sample_baseline_unavailable'},
            {'sample_index': 1, 'elapsed_seconds': 1.0, 'interval_seconds': 1.0, 'available': True, 'delta_ticks': None, 'reason': 'process_appeared'},
        ] + [
            {'sample_index': i, 'elapsed_seconds': float(i), 'interval_seconds': 1.0, 'available': True, 'delta_ticks': 100, 'reason': None}
            for i in range(2, 35)
        ]
        gen_data_partial = {
            'schema_version': 1, 'role': 'generator_B', 'clk_tck': 100,
            'clock': base_clock,
            'samples': samples_partial,
        }
        res_partial = e2.qualify_generator_resources(gen_data_partial, events)
        # Sample 1 (outside active window [10.0, 30.0]) has process_appeared, so full lifecycle CPU is unavailable
        self.assertFalse(res_partial['lifecycle_cpu_available'])
        self.assertIsNone(res_partial['lifecycle_cpu_seconds'])
        self.assertEqual(res_partial['lifecycle_cpu_unavailable_reason'], 'process_change_samples_lack_delta:process_appeared')
        # Partial observed lifecycle CPU is sum of available deltas: 33 samples * 100 / 100 = 33.0s
        self.assertAlmostEqual(res_partial['partial_observed_lifecycle_cpu_seconds'], 33.0)
        # But active window [10.0, 30.0] has all valid samples (index 11..30 = 20 samples), so active CPU IS available
        self.assertTrue(res_partial['active_cpu_available'])
        self.assertAlmostEqual(res_partial['active_cpu_seconds'], 20.0)






if __name__ == '__main__':
    unittest.main()
