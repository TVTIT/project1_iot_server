import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

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


class E3SupportTests(unittest.TestCase):
    def test_collector_latency_uses_latest_cumulative_snapshot_once(self):
        metrics = [
            {'phase': 'sample-1', 'snapshot': {'command_timeline': {
                'd_dispatch': {'upper_seconds':[1], 'buckets':[1], 'count':1,
                    'overflow':0, 'sum_seconds':0.2},
                'd_e2e_succeeded': {'upper_seconds':[1], 'buckets':[1], 'count':1,
                    'overflow':0, 'sum_seconds':0.3}, 'missing_origin':0,
                'missing_dispatch':0, 'pending_origins':0}}},
            {'phase': 'terminal', 'snapshot': {'command_timeline': {
                'd_dispatch': {'upper_seconds':[1], 'buckets':[2], 'count':2,
                    'overflow':0, 'sum_seconds':0.7},
                'd_e2e_succeeded': {'upper_seconds':[1], 'buckets':[2], 'count':2,
                    'overflow':0, 'sum_seconds':0.9}, 'missing_origin':0,
                'missing_dispatch':0, 'pending_origins':0}}},
        ]
        evidence = e3.server_origin_latency_from_collector(metrics)
        self.assertEqual(evidence['histograms']['snapshot_phase'], 'terminal')
        self.assertEqual(evidence['latency']['dispatch']['count'], 2)
        self.assertEqual(evidence['latency']['dispatch']['sum_seconds'], 0.7)
        self.assertEqual(evidence['latency']['dispatch']['mean_seconds'], 0.35)
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
        self.assertEqual(result['phase_overlap']['burst_overlap'], 'NOT APPLICABLE')
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


if __name__ == '__main__': unittest.main()
