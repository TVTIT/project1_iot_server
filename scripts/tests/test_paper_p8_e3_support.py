import importlib.util
import json
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
from pathlib import Path
from scripts import paper_p8_server

SPEC = importlib.util.spec_from_file_location('e3', Path(__file__).parents[1] / 'paper_p8_e3_support.py')
e3 = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(e3)


def fixture():
    targets = [{'gateway_id': f'gw{i}', 'sensor_id': f'bg{i}'} for i in (1, 2)]
    control_targets = [{'gateway_id': 'gw1', 'sensor_id': 'ctl1'}, {'gateway_id': 'gw1', 'sensor_id': 'ctl2'}]
    return e3.e3_pilot_manifests(common={'run_id': 'x', 'bundle_path': 'private/bundle',
        'publish_timeout_ms': 3000}, background_targets=targets,
        background_gateway_identity_files={'gw1':'gw1.json','gw2':'gw2.json'},
        telemetry_identity_files={'A':'a.token','B':'b.token'}, control_targets=control_targets,
        initial_versions={'ctl1':'4','ctl2':'8'}, control_identity_file='control.tokenmap.json',
        tls={'https_endpoint':'https://api.example.test','https_ca_file':'ca.pem',
             'mqtt_tls_endpoint':'mqtts://broker.example.test','mqtt_tls_ca_file':'mqtt-ca.pem',
             'mqtt_tls_server_name':'localhost'},
        idempotency_keys=[f'00000000-0000-4000-8000-{i:012d}' for i in range(1,5)])


def _manifest_args():
    return dict(common={'run_id':'x','bundle_path':'private/bundle','publish_timeout_ms':3000},
        background_targets=[{'gateway_id':f'gw{i}','sensor_id':f'bg{i}'} for i in (1,2)],
        background_gateway_identity_files={'gw1':'gw1.json','gw2':'gw2.json'},
        telemetry_identity_files={'A':'a.token','B':'b.token'},
        control_targets=[{'gateway_id':'gw1','sensor_id':'ctl1'},{'gateway_id':'gw1','sensor_id':'ctl2'}],
        initial_versions={'ctl1':'4','ctl2':'8'}, control_identity_file='control.tokenmap.json',
        tls={'https_endpoint':'https://api.example.test','https_ca_file':'ca.pem',
            'mqtt_tls_endpoint':'mqtts://broker.example.test','mqtt_tls_ca_file':'mqtt-ca.pem'})


class E3SupportTests(unittest.TestCase):
    def test_official30_cli_dry_run_roundtrips_schema_only_and_pilot_stays_compatible(self):
        output = StringIO()
        with redirect_stdout(output):
            code = paper_p8_server.main(['serve', '--root', '/not-used', '--network-config', '/not-used',
                '--dry-run', '--experiment', 'E3', '--profile', 'official', '--official-opt-in',
                '--dispatch-mode', 'polling', '--workload', 'low_load', '--e3-command-count', '30',
                '--e3-schedule', 'steady', '--deadline-seconds', '1200'])
        self.assertEqual(code, 0)
        contract = json.loads(output.getvalue())['e3_offline_contract']
        self.assertEqual(contract['command_count'], 30)
        self.assertEqual(contract['profile'], 'official')
        self.assertEqual(contract['unique_idempotency_keys'], 30)
        self.assertTrue(contract['bundle_schema_checked'])
        self.assertTrue(contract['bundle_roundtrip']['bind'])
        self.assertTrue(contract['result_schema_checked'])
        self.assertEqual([row['role'] for row in contract['paperload_preflight']],
                         ['telemetry', 'telemetry', 'control'])
        self.assertTrue(all(row['exit_code'] == 0 for row in contract['paperload_preflight']))
        self.assertTrue(all(row['stdout']['status'] == 'PREFLIGHT_ONLY_NOT_RUN'
                            for row in contract['paperload_preflight']))
        self.assertEqual(contract['paperload_preflight'][2]['stdout']['profile'], 'official')
        self.assertEqual(contract['paperload_preflight'][0]['stdout']['profile'], 'pilot')
        self.assertEqual(contract['result_snapshot_status'], 'PARTIAL schema-only; no execution claimed')
        self.assertEqual(contract['deadline_seconds'], 1200)
        self.assertGreater(contract['required_token_lifetime_seconds'], 0)
        self.assertEqual(contract['budgets']['control_pending_messages'], 4)
        self.assertGreater(contract['budgets']['bound_disk_guard']['growth_threshold_bytes'], 0)
        self.assertFalse(contract['native_executed'])
        self.assertFalse(contract['execution_supported'])
        self.assertEqual(contract['manifest_profiles'], {'background':['pilot'], 'control':'official'})

    def test_official_missing_optin_and_invalid_schedule_refuse_without_fixture(self):
        base = ['serve', '--root', '/not-used', '--network-config', '/not-used', '--dry-run',
            '--experiment', 'E3', '--profile', 'official', '--dispatch-mode', 'polling', '--workload', 'low_load',
            '--e3-command-count', '30', '--e3-schedule', 'steady']
        with redirect_stderr(StringIO()), self.assertRaisesRegex(ValueError, 'official_profile_opt_in_required'):
            paper_p8_server.main(base)
        with redirect_stderr(StringIO()), self.assertRaisesRegex(ValueError, 'E3_official_requires_30_or_low_load_3_steady_commands'):
            paper_p8_server.main(base + ['--official-opt-in', '--e3-schedule', 'smoke', '--e3-command-count', '4'])

    def test_official_execution_passes_profile_gate_and_pilot_dry_run_works(self):
        base = ['serve', '--root', '/not-used', '--network-config', '/not-used', '--experiment', 'E3',
            '--profile', 'official', '--official-opt-in', '--dispatch-mode', 'polling', '--workload', 'low_load',
            '--e3-command-count', '30', '--e3-schedule', 'steady']
        with self.assertRaises(PermissionError):
            paper_p8_server.main(base)
        output = StringIO()
        with redirect_stdout(output):
            code = paper_p8_server.main(['serve', '--root', '/not-used', '--network-config', '/not-used',
                '--dry-run', '--experiment', 'E3', '--dispatch-mode', 'polling', '--workload', 'low_load'])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())['e3']['command_count'], 4)

    def test_parameterized_command_schedule_and_offline_thirty_command_plan(self):
        args = _manifest_args()
        plan=e3.e3_pilot_manifests(**args, command_count=30, schedule='steady')
        commands=plan['control']['commands']
        self.assertEqual(len(commands),30)
        self.assertEqual(len({c['idempotency_key'] for c in commands}),30)
        self.assertEqual([int(c['expected_desired_version']) for c in commands],
            [4 + sum(1 for x in commands[:i] if x['sensor_id']=='ctl1') if c['sensor_id']=='ctl1'
             else 8 + sum(1 for x in commands[:i] if x['sensor_id']=='ctl2') for i,c in enumerate(commands)])
        self.assertTrue(any(0 < c['frequency_hz'] < 1 for c in commands))
        self.assertEqual(commands[0]['offset_ms'], 1000)
        self.assertEqual(commands[-1]['offset_ms'], 30000)

    def test_official_profile_requires_opt_in_and_exact_30_steady(self):
        with self.assertRaisesRegex(ValueError, 'opt_in'):
            e3.e3_pilot_manifests(**_manifest_args(), command_count=30, schedule='steady', profile='official')
        plan = e3.e3_pilot_manifests(**_manifest_args(), command_count=30, schedule='steady',
                                    profile='official', official_opt_in=True)
        self.assertEqual(plan['control']['profile'], 'official')
        self.assertEqual(plan['background']['A']['profile'], 'pilot')
        with self.assertRaisesRegex(ValueError, '30_or_low_load_3_steady'):
            e3.e3_pilot_manifests(**_manifest_args(), command_count=3, schedule='steady',
                                  profile='official', official_opt_in=True, workload='mixed')
        self.assertEqual(e3.e3_pilot_manifests(**_manifest_args(), command_count=3,
            schedule='steady', profile='official', official_opt_in=True,
            workload='low_load')['control']['profile'], 'official')

    def test_invalid_command_schedule_rejected(self):
        with self.assertRaisesRegex(ValueError, 'schedule'):
            e3.e3_pilot_manifests(**_manifest_args(), command_count=30, schedule='immediate')

    def test_two_command_steady_stream_has_sensor_local_versions(self):
        plan = e3.e3_pilot_manifests(**_manifest_args(), command_count=2, schedule='steady')
        commands = plan['control']['commands']
        self.assertEqual([row['sensor_id'] for row in commands], ['ctl1', 'ctl2'])
        self.assertEqual([row['expected_desired_version'] for row in commands], ['4', '8'])
        self.assertEqual([row['offset_ms'] for row in commands], [1000, 2000])
        self.assertEqual(plan['expected_versions_after'], {'ctl1': '5', 'ctl2': '9'})

    def test_background_schedule_and_expected_accounting_are_shared(self):
        plan=e3.e3_pilot_manifests(**_manifest_args(), command_count=4, schedule='smoke', seed=321,
            background_profile={'rate_per_second':2,'duration_seconds':5,'burst_messages':3})
        self.assertEqual(plan['control']['seed'], 321)
        self.assertEqual(plan['background_messages'], {'A':10,'B':13})
        self.assertEqual(plan['background']['A']['phases']['measure_seconds'], 5)
        self.assertEqual(plan['background']['B']['traffic']['burst_messages'], 3)
        self.assertEqual(plan['background']['A']['seed'], plan['background']['B']['seed'])

    def test_result_verifier_uses_declared_count_not_four(self):
        result = e3.validate_e3_result(control_workload={'commands': [], 'planned': 0, 'sent': 0,
            'skipped': 0, 'pending': 0, 'status': 'COMPLETE'}, telemetry_a={}, telemetry_b={},
            phase_events=[], background_database_delta={}, command_database_delta={},
            control_receipt_verification={}, runner_stopped=True, declared_idempotency_keys=['k'] * 30,
            expected_command_count=30)
        self.assertIn('fresh_success_count_mismatch', result['failures'])

    def test_collector_latency_uses_same_process_window_delta(self):
        metrics = [
            {'phase': 'workload_start', 'snapshot': {'command_timeline': {
                'd_dispatch': {'upper_seconds':[1], 'buckets':[1], 'count':1,
                    'overflow':0, 'sum_seconds':0.2},
                'd_e2e_succeeded': {'upper_seconds':[1], 'buckets':[1], 'count':1,
                    'overflow':0, 'sum_seconds':0.3}, 'missing_origin':0,
                'missing_dispatch':0, 'missing_result':0, 'overflow':0, 'pending_origins':0}}},
            {'phase': 'terminal', 'snapshot': {'command_timeline': {
                'd_dispatch': {'upper_seconds':[1], 'buckets':[2], 'count':2,
                    'overflow':0, 'sum_seconds':0.7},
                'd_e2e_succeeded': {'upper_seconds':[1], 'buckets':[2], 'count':2,
                    'overflow':0, 'sum_seconds':0.9}, 'missing_origin':0,
                'missing_dispatch':0, 'missing_result':0, 'overflow':0, 'pending_origins':0}}},
        ]
        evidence = e3.server_origin_latency_from_collector(metrics)
        self.assertEqual(evidence['histograms']['snapshot_phase'], 'workload_start..terminal')
        self.assertEqual(evidence['latency']['dispatch']['count'], 1)
        self.assertAlmostEqual(evidence['latency']['dispatch']['sum_seconds'], 0.5)
        self.assertAlmostEqual(evidence['latency']['dispatch']['mean_seconds'], 0.5)
        self.assertEqual(e3.server_origin_latency_from_collector([])['availability'], 'unavailable')

    def test_separate_targets_and_control_manifest_contract(self):
        manifests = fixture()
        self.assertNotIn(('gw1','ctl1'), {(m['targets'][0]['gateway_id'],m['targets'][0]['sensor_id']) for m in manifests['background'].values()})
        control = manifests['control']
        self.assertEqual(control['roles'], ['control'])
        self.assertEqual(control['identity_files'], {'control':'control.tokenmap.json'})
        for key in ('mqtt_tls_endpoint','mqtt_tls_ca_file'):
            self.assertNotIn(key, control['tls'])
        for key in ('mqtt','bundle_path','durability_mode'):
            self.assertNotIn(key, control)
        self.assertEqual(control['topology']['gateways'], 1)
        self.assertEqual(control['topology']['sensors_per_gateway'], 2)
        self.assertTrue(all(isinstance(x['frequency_hz'], (int, float)) for x in control['commands']))
        self.assertEqual([x['frequency_hz'] for x in control['commands']], [1.5, .5, 2, .75])
        self.assertEqual({m['experiment'] for m in manifests['background'].values()}, {'E2'})
        self.assertEqual(manifests['background']['B']['traffic']['burst_messages'], 12)

    def test_versions_decimal_subhz_and_arm_flag_difference(self):
        data = fixture(); cmds = data['control']['commands']
        self.assertEqual([x['frequency_hz'] for x in cmds], [1.5,.5,2,.75])
        self.assertEqual([x['offset_ms'] for x in cmds], [1000, 3800, 10000, 12000])
        self.assertEqual([x['expected_desired_version'] for x in cmds], ['4','8','5','9'])
        arms = e3.e3_pilot_arms()
        self.assertEqual({k:v for k,v in arms['polling'].items() if k != 'flags'},
                         {k:v for k,v in arms['wakeup'].items() if k != 'flags'})
        self.assertEqual({k:v for k,v in arms['polling']['flags'].items() if k != 'STAGE5_WAKEUP_ENABLED'},
                         {k:v for k,v in arms['wakeup']['flags'].items() if k != 'STAGE5_WAKEUP_ENABLED'})

    def test_low_load_changes_only_b_burst_and_records_profile(self):
        mixed = fixture()
        low = e3.e3_pilot_manifests(common={'run_id':'x','bundle_path':'private/bundle','publish_timeout_ms':3000},
            background_targets=[{'gateway_id':f'gw{i}','sensor_id':f'bg{i}'} for i in (1,2)],
            background_gateway_identity_files={'gw1':'gw1.json','gw2':'gw2.json'},
            telemetry_identity_files={'A':'a.token','B':'b.token'},
            control_targets=[{'gateway_id':'gw1','sensor_id':'ctl1'},{'gateway_id':'gw1','sensor_id':'ctl2'}],
            initial_versions={'ctl1':'4','ctl2':'8'}, control_identity_file='control.tokenmap.json',
            tls={'https_endpoint':'https://api.example.test','https_ca_file':'ca.pem',
                'mqtt_tls_endpoint':'mqtts://broker.example.test','mqtt_tls_ca_file':'mqtt-ca.pem',
                'mqtt_tls_server_name':'localhost'},
            idempotency_keys=[f'00000000-0000-4000-8000-{i:012d}' for i in range(1,5)], workload='low_load')
        self.assertEqual(mixed['control']['commands'], low['control']['commands'])
        self.assertEqual(low['workload'], 'low_load')
        self.assertEqual([low['background'][x]['traffic']['burst_messages'] for x in ('A','B')], [0,0])
        self.assertEqual([low['background'][x]['phases']['measure_seconds'] for x in ('A','B')], [8,4])
        for arm in ('A','B'):
            for key in ('targets','samples_per_message','topology','limits','tls','seed','roles','identity_files','phases'):
                self.assertEqual(mixed['background'][arm][key], low['background'][arm][key])
        self.assertEqual(mixed['background']['B']['traffic']['burst_messages'], 12)

    def test_private_run_spec_two_executors(self):
        with tempfile.TemporaryDirectory() as d:
            manifests = fixture()
            inputs = {'A': manifests['background']['A'], 'B': manifests['background']['B'], 'control': manifests['control']}
            env_files=[]
            for i in (1,2):
                p=Path(d)/f'gw{i}.json'
                p.write_text(json.dumps({'GATEWAY_ID':'gw1','SENSOR_ID':f'ctl{i}',
                    'GATEWAY_CONTROL_ENABLED':'true','GATEWAY_MQTT_BROKER_URL':'mqtts://broker.example.test:8883',
                    'GATEWAY_MQTT_CLIENT_ID':f'client{i}','GATEWAY_MQTT_USERNAME':f'user{i}',
                    'GATEWAY_MQTT_CA_FILE':'/private/ca.pem','GATEWAY_OUTBOX_PATH':f'/private/gw{i}.sqlite'}))
                p.chmod(0o600); env_files.append(str(p))
            result = e3.build_e3_execution_plan(output_dir=d, manifests=inputs,
                gateway_env_files=env_files, shutdown_timeouts_seconds=[35,45])
            spec = json.loads(Path(result['run_spec']).read_text())
            self.assertEqual(len(spec['gateways']), 2); self.assertEqual(spec['stop_timeout_seconds'], 45)
            self.assertEqual(Path(result['run_spec']).stat().st_mode & 0o777, 0o600)
            self.assertTrue(all(Path(x['manifest']).stat().st_mode & 0o077 == 0 for x in spec['paperload']))

    def test_workload_specific_overlap_and_counters(self):
        self.assertGreaterEqual(fixture()['control']['limits']['disk_growth_bytes'], 8 << 20)
        control = {'status':'COMPLETE','planned':4,'sent':4,'skipped':0,'pending':0,
                   'commands':[{'classification':'succeeded','idempotency_key':f'k{i}'} for i in range(4)]}
        ta = {'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,'application_acks':8,'pending_messages':0,'pending_bytes':0}}
        tb_low = {'status':'COMPLETE','telemetry':{'generated_unique':4,'offered_unique':4,'application_acks':4,'pending_messages':0,'pending_bytes':0}}
        events = [{'child':'paperload-telemetry:A.manifest.json','event':'measure_begin','observed_at_runner_monotonic_ns':100},
                  {'child':'paperload-telemetry:A.manifest.json','event':'measure_end','observed_at_runner_monotonic_ns':900}]
        for i in range(1,5):
            events.extend({'child':'paperload-control:control.manifest.json','event':ev,'command_ordinal':i,'observed_at_runner_monotonic_ns':stamp}
                          for ev,stamp in [('request_begin',350+i),('admission_observed',360+i),('terminal_observed',400+i)])
        events.extend({'child':'paperload-control:control.manifest.json','event':ev,'command_ordinal':0,'observed_at_runner_monotonic_ns':stamp}
                      for ev,stamp in [('generation_stopped',900),('producers_stopped',910),('workers_joined',920)])
        kwargs = dict(control_workload=control, telemetry_a=ta, telemetry_b=tb_low, phase_events=events,
            background_database_delta={'ledger':12,'raw':1200,'temporal':12}, command_database_delta={'raw_samples':4},
            control_receipt_verification={'verified':True,'verified_idempotency_keys':[f'k{i}' for i in range(4)]},
            runner_stopped=True, declared_idempotency_keys=[f'k{i}' for i in range(4)],
            raw_checks=[{'verified_exact':True,'raw_identity_value_mismatches':0,'ledger_count':8,'identity_ranges':[{'sample_count':100} for _ in range(8)]},
                        {'verified_exact':True,'raw_identity_value_mismatches':0,'ledger_count':4,'identity_ranges':[{'sample_count':100} for _ in range(4)]}])
        result = e3.validate_e3_result(**kwargs, workload='low_load')
        self.assertTrue(result['valid'], result['failures'])
        sampled_raw = {**kwargs, 'raw_checks': [
            {'verified_exact': False, 'verified_sample': True,
             'raw_identity_value_mismatches': 0, 'ledger_count': messages,
             'identity_ranges': [{'sample_count': 100} for _ in range(messages)]}
            for messages in (8, 4)]}
        sampled = e3.validate_e3_result(**sampled_raw, workload='low_load')
        self.assertTrue(sampled['valid'], sampled['failures'])
        self.assertEqual(result['phase_overlap']['burst_overlap'], 'NOT APPLICABLE')
        overlapping_same_sensor = {**kwargs, 'control_workload': {**control,
            'commands': [{'classification': 'succeeded', 'idempotency_key': f'k{i}',
                          'sensor_id': 'same-sensor'} for i in range(4)]}}
        sequenced = e3.validate_e3_result(**overlapping_same_sensor, workload='low_load')
        self.assertIn('phase_overlap_inconclusive', sequenced['failures'])
        self.assertEqual(sequenced['phase_overlap']['reason'],
                         'same_sensor_command_before_previous_terminal')
        portable_events = [{**row, 'child': row['child'].replace('.manifest.json', '.json')}
                           for row in events]
        portable = e3.validate_e3_result(**{**kwargs, 'phase_events': portable_events}, workload='low_load')
        self.assertTrue(portable['valid'], portable['failures'])
        zero_command_raw = e3.validate_e3_result(**{**kwargs, 'command_database_delta': {'raw_samples': 0}},
                                                 workload='low_load')
        self.assertFalse(zero_command_raw['valid'])
        self.assertIn('command_raw_counts_invalid', zero_command_raw['failures'])
        kwargs['background_database_delta'] = {'ledger':11,'raw':1100,'temporal':11}
        bad = e3.validate_e3_result(**kwargs, workload='low_load')
        self.assertIn('background_database_delta_mismatch', bad['failures'])
        mixed = e3.validate_e3_result(**kwargs, workload='mixed')
        self.assertFalse(mixed['valid'])
        self.assertIn('phase_overlap_inconclusive', mixed['failures'])

    def test_result_real_phase_overlap_and_missing_data(self):
        events=[]
        for child, items in [('paperload-telemetry:A.manifest.json',[('measure_begin',100),('measure_end',900)]),
                             ('paperload-telemetry:B.manifest.json',[('burst_begin',300),('burst_end',800)])]:
            events += [{'child':child,'event':k,'observed_at_runner_monotonic_ns':v} for k,v in items]
        for i in range(1,5):
            for event, stamp in [('request_begin',350+i),('admission_observed',360+i),('terminal_observed',400+i)]:
                events.append({'child':'paperload-control:control.manifest.json','event':event,
                               'command_ordinal':i,'observed_at_runner_monotonic_ns':stamp})
        cmds=[{'classification':'succeeded','idempotency_key':f'key{i}'} for i in range(4)]
        events += [{'child':'paperload-control:control.manifest.json','event':ev,
                    'command_ordinal':0,'observed_at_runner_monotonic_ns':t}
                   for ev,t in [('generation_stopped',900),('producers_stopped',910),('workers_joined',920)]]
        result=e3.validate_e3_result(control_workload={'commands':cmds,'pending':0,
            'status':'COMPLETE','planned':4,'sent':4,'skipped':0,'pending':0},
            telemetry_a={'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,
                'application_acks':8,'pending_messages':0,'pending_bytes':0}},
            telemetry_b={'status':'COMPLETE','telemetry':{'generated_unique':16,'offered_unique':16,
                'application_acks':16,'pending_messages':0,'pending_bytes':0}},
            phase_events=events, background_database_delta={'ledger':24,'raw':2400,'temporal':24},
            command_database_delta={'raw_samples':37}, control_receipt_verification={'verified':True,
                'verified_idempotency_keys':[f'key{i}' for i in range(4)]}, runner_stopped=True,
            declared_idempotency_keys=[f'key{i}' for i in range(4)], raw_checks=[
                {'verified_exact': True, 'raw_identity_value_mismatches': 0, 'ledger_count': 8,
                 'identity_ranges': [{'sample_count': 100} for _ in range(8)]},
                {'verified_exact': True, 'raw_identity_value_mismatches': 0, 'ledger_count': 16,
                 'identity_ranges': [{'sample_count': 100} for _ in range(16)]}])
        self.assertTrue(result['valid'])
        self.assertTrue(result['phase_overlap']['observed_overlap_ns'] > 0)
        self.assertEqual(result['command_raw_samples_actual'], 37)
        self.assertFalse(e3.validate_e3_result(control_workload={}, telemetry_a={}, telemetry_b={},
            phase_events=[], background_database_delta={}, command_database_delta={},
            control_receipt_verification={}, runner_stopped=False, declared_idempotency_keys=[])['valid'])

    def test_schema_regression_telemetry_and_control_actual_fields(self):
        events=[]
        for child, items in [('paperload-telemetry:A.manifest.json',[('measure_begin',100),('measure_end',900)]),
                             ('paperload-telemetry:B.manifest.json',[('burst_begin',300),('burst_end',800)])]:
            events += [{'child':child,'event':k,'observed_at_runner_monotonic_ns':v} for k,v in items]
        for i in range(1,5):
            for event, stamp in [('request_begin',350+i),('admission_observed',360+i),('terminal_observed',400+i)]:
                events.append({'child':'paperload-control:control.manifest.json','event':event,
                               'command_ordinal':i,'observed_at_runner_monotonic_ns':stamp})
        events += [{'child':'paperload-control:control.manifest.json','event':ev,
                    'command_ordinal':0,'observed_at_runner_monotonic_ns':t}
                   for ev,t in [('generation_stopped',900),('producers_stopped',910),('workers_joined',920)]]
        cmds=[{'classification':'succeeded','idempotency_key':f'key{i}'} for i in range(4)]
        valid_control = {'status':'COMPLETE','planned':4,'sent':4,'skipped':0,'pending':0,'commands':cmds}
        valid_ta = {'status':'COMPLETE','telemetry':{'generated_unique':8,'offered_unique':8,
            'application_acks':8,'pending_messages':0,'pending_bytes':0}}
        valid_tb = {'status':'COMPLETE','telemetry':{'generated_unique':16,'offered_unique':16,
            'application_acks':16,'pending_messages':0,'pending_bytes':0}}
        rcpt = {'verified':True,'verified_idempotency_keys':[f'key{i}' for i in range(4)]}
        keys = [f'key{i}' for i in range(4)]
        bg = {'ledger':24,'raw':2400,'temporal':24}
        cmd_db = {'raw_samples':4}

        # 1. Telemetry old field (acknowledged instead of application_acks or missing generated_unique)
        bad_ta = {'status':'COMPLETE','telemetry':{'acknowledged':8,'pending':0}}
        r = e3.validate_e3_result(control_workload=valid_control, telemetry_a=bad_ta, telemetry_b=valid_tb,
            phase_events=events, background_database_delta=bg, command_database_delta=cmd_db,
            control_receipt_verification=rcpt, runner_stopped=True, declared_idempotency_keys=keys)
        self.assertFalse(r['valid'])
        self.assertIn('telemetry_accounting_mismatch', r['failures'])

        # 2. Control old field (acknowledged: 4 instead of sent: 4, skipped: 0)
        bad_ctrl = {'status':'COMPLETE','planned':4,'acknowledged':4,'pending':0,'commands':cmds}
        r = e3.validate_e3_result(control_workload=bad_ctrl, telemetry_a=valid_ta, telemetry_b=valid_tb,
            phase_events=events, background_database_delta=bg, command_database_delta=cmd_db,
            control_receipt_verification=rcpt, runner_stopped=True, declared_idempotency_keys=keys)
        self.assertFalse(r['valid'])
        self.assertIn('control_workload_incomplete', r['failures'])

        # 3. Control planned != 4 or sent != 4
        bad_ctrl2 = {'status':'COMPLETE','planned':4,'sent':3,'skipped':1,'pending':0,'commands':cmds}
        r = e3.validate_e3_result(control_workload=bad_ctrl2, telemetry_a=valid_ta, telemetry_b=valid_tb,
            phase_events=events, background_database_delta=bg, command_database_delta=cmd_db,
            control_receipt_verification=rcpt, runner_stopped=True, declared_idempotency_keys=keys)
        self.assertFalse(r['valid'])
        self.assertIn('control_workload_incomplete', r['failures'])

        # 4. MQTT TLS server name propagation
        manifests = fixture()
        self.assertIn('mqtt_tls_server_name', manifests['background']['A']['tls'])
        self.assertEqual(manifests['background']['A']['tls']['mqtt_tls_server_name'], 'localhost')

    def test_official30_control_wall_budget_is_sufficient_for_30_commands(self):
        """Control manifest wall_seconds must be measure + drain + guard for 30 commands steady schedule."""
        args = _manifest_args()
        plan = e3.e3_pilot_manifests(**args, command_count=30, schedule='steady',
            profile='official', official_opt_in=True, workload='low_load')
        control = plan['control']
        # measure_seconds = max(8, 30000//1000 + 2) = 32
        # drain_seconds = 30
        # wall_seconds should be measure + drain + 3 (guard) = 65
        # Previously hardcoded 60 was insufficient
        self.assertEqual(control['phases']['measure_seconds'], 32)
        self.assertEqual(control['phases']['drain_seconds'], 30)
        self.assertEqual(control['limits']['wall_seconds'], 32 + 30 + 3)  # 65
        self.assertGreater(control['limits']['wall_seconds'], 60)

    def test_runner_pump_lines_allows_full_control_phase_events_for_30_commands(self):
        """_pump_lines must accept 93 events for 30 commands (3*30 + 3 stop), not cap at 32."""
        import paper_p8
        import io
        import json
        
        # Build 30 commands * 3 events + 3 stop events = 93 valid control events
        events = []
        for i in range(1, 31):
            for ev in ('request_begin', 'admission_observed', 'terminal_observed'):
                events.append(json.dumps({
                    'event': ev,
                    'elapsed_ns': 1000000 * i,
                    'command_ordinal': i,
                    'clock_qualification': 'process-local elapsed from RunControl origin; not server latency'
                }))
        for ev in ('generation_stopped', 'producers_stopped', 'workers_joined'):
            events.append(json.dumps({
                'event': ev,
                'elapsed_ns': 1000000 * 31,
                'command_ordinal': 0,
                'clock_qualification': 'process-local elapsed from RunControl origin; not server latency'
            }))
        
        input_data = '\n'.join('P8_PHASE ' + e for e in events) + '\n'
        pipe = io.StringIO(input_data)
        captured, errors = [], []
        paper_p8._pump_lines(pipe, [], __import__('threading').Event(),
            'paperload-control:control.json', captured, errors)
        
        # Should capture all 93 events without hitting the cap
        self.assertEqual(len(captured), 93, f"Expected 93 events, got {len(captured)}. Errors: {errors}")
        self.assertEqual(errors, [])
        # Verify last event is workers_joined
        self.assertEqual(captured[-1]['event'], 'workers_joined')

    def test_official30_steady_schedule_offsets_are_1s_apart(self):
        """official30 steady schedule has offsets 1s apart: 1000, 2000, ..., 30000 ms."""
        args = _manifest_args()
        plan = e3.e3_pilot_manifests(**args, command_count=30, schedule='steady',
            profile='official', official_opt_in=True, workload='low_load')
        commands = plan['control']['commands']
        self.assertEqual(len(commands), 30)
        offsets = [c['offset_ms'] for c in commands]
        self.assertEqual(offsets[0], 1000)
        self.assertEqual(offsets[-1], 30000)
        for i in range(1, 30):
            self.assertEqual(offsets[i] - offsets[i-1], 1000)


if __name__ == '__main__': unittest.main()
