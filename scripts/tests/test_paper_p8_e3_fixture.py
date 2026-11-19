"""Unit and contract tests for paper_p8_e3_fixture."""
import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import scripts.tests.paper_p8_e3_fixture as e3_fixture
from scripts.paper_p8 import GATEWAY_ENV_KEYS, _gateway_environment


class E3FixtureTests(unittest.TestCase):
    def test_control_target_dicts_map_to_real_sensor_scopes_and_positive_raw_delta(self):
        prepared = {'control_targets': [
            {'gateway_id': 'gw-real', 'sensor_id': 'sensor-alpha'},
            {'gateway_id': 'gw-real', 'sensor_id': 'sensor-beta'},
        ]}
        targets = e3_fixture._e3_control_targets(prepared)
        self.assertEqual([target.sensor_id for target in targets], ['sensor-alpha', 'sensor-beta'])
        self.assertEqual([target.sensor_urn for target in targets], [
            'urn:ngsi-ld:Sensor:gw-real:sensor-alpha',
            'urn:ngsi-ld:Sensor:gw-real:sensor-beta'])
        self.assertEqual([target.rest_path for target in targets], [
            '/v1/digital-twins/urn:ngsi-ld:Sensor:gw-real:sensor-alpha?format=simplified',
            '/v1/digital-twins/urn:ngsi-ld:Sensor:gw-real:sensor-beta?format=simplified'])
        delta = e3_fixture._e3_command_database_delta(
            [{'raw': 10, 'temporal': 10}, {'raw': 20, 'temporal': 20}],
            [{'raw': 13, 'temporal': 13}, {'raw': 22, 'temporal': 22}])
        self.assertEqual(delta, {'raw_samples': 5, 'temporal_observations': 5})

    def test_e3_workload_provenance_qualifies_e2_telemetry_children_and_e3_control(self):
        run_id = 'p8e3-session-polling'
        for role in ('A', 'B'):
            self.assertTrue(e3_fixture._e3_workload_provenance_valid(
                {'experiment': 'E2', 'arm': role, 'run_id': run_id, 'mode': 'remote', 'profile':'pilot'},
                role, run_id))
        self.assertTrue(e3_fixture._e3_workload_provenance_valid(
            {'experiment': 'E3', 'arm': 'control', 'run_id': run_id, 'profile':'official'},
            'control', run_id, 'official'))
        self.assertFalse(e3_fixture._e3_workload_provenance_valid(
            {'experiment': 'E3', 'arm': 'control', 'run_id': run_id, 'profile':'pilot'},
            'control', run_id, 'official'))
        self.assertFalse(e3_fixture._e3_workload_provenance_valid(
            {'experiment': 'E3', 'arm': 'A', 'run_id': run_id, 'mode': 'remote'}, 'A', run_id))
        self.assertFalse(e3_fixture._e3_workload_provenance_valid(
            {'experiment': 'E2', 'arm': 'B', 'run_id': 'stale', 'mode': 'remote'}, 'B', run_id))

    def test_e3_result_session_profile_must_match_selected_profile(self):
        expected = {'session_id':'s'*32, 'run_ids':['r'], 'experiment':'E3',
            'arm':'polling', 'phase':'e3-low_load-polling', 'profile':'official'}
        self.assertTrue(e3_fixture._e3_result_session_matches(dict(expected), expected))
        self.assertFalse(e3_fixture._e3_result_session_matches(
            {**expected, 'profile':'pilot'}, expected))

    def test_prepare_seam_is_explicitly_preparation_only_and_accepts_profile_arguments(self):
        with patch.object(e3_fixture, 'run', return_value={'experiment': 'E3'}) as runner:
            result = e3_fixture.prepare('smoke', 'fixture', 'endpoint', 'registry',
                'containers', 'images', 'state', arm='polling', workload='low_load',
                remote_profile={'advertised_https': 'https://example.invalid',
                                'advertised_mqtt_tls': 'mqtts://example.invalid:8883'},
                session_id='0123456789abcdef0123456789abcdef', command_count=30,
                schedule='steady', seed=123, profile='official', official_opt_in=True)
        self.assertEqual(result, {'experiment': 'E3'})
        args, kwargs = runner.call_args
        self.assertEqual(args[7:9], (None, None))
        self.assertEqual(kwargs, {'prepare_only': True, 'remote_profile': {
            'advertised_https': 'https://example.invalid',
            'advertised_mqtt_tls': 'mqtts://example.invalid:8883'},
            'session_id': '0123456789abcdef0123456789abcdef', 'command_count': 30,
            'schedule': 'steady', 'background_profile': None, 'seed': 123,
            'profile': 'official', 'official_opt_in': True})

    def test_e3_bundle_export_keeps_three_roles_and_two_executor_envs(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for rel in ('https-ca.pem','mqtt-ca.pem','telemetry-a.json','telemetry-b.json',
                        'control.json','gw-a.json','env-a.json','env-b.json','run-spec.json',
                        'plan/A.manifest.json','plan/B.manifest.json','plan/control.manifest.json'):
                path=root/rel; path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                path.write_text('{}'); path.chmod(0o600)
            (root/'gw-a.json').write_text(json.dumps({'username':'gw-a','password':'pw'}))
            prepared={'root':root,'https_ca_file':root/'https-ca.pem','mqtt_ca_file':root/'mqtt-ca.pem',
                'telemetry_files':{'A':root/'telemetry-a.json','B':root/'telemetry-b.json'},
                'control_token_file':root/'control.json','gateway_files':{'gw-a':root/'gw-a.json'},
                'gateway_env_files':[root/'env-a.json',root/'env-b.json'],
                'plan':{'run_spec':str(root/'run-spec.json')},'session_id':'s'*32,
                'arm':'polling','phase_id':'e3-low_load-polling','workload':'low_load'}
            prepared['profile'] = 'official'
            with patch('scripts.paper_p8_bundle.export_bundle', return_value={'file_count':12}) as export:
                result=e3_fixture.export_prepared_bundle(prepared,output_leaf=root/'bundle')
            self.assertEqual(result['file_count'],12)
            kwargs=export.call_args.kwargs
            self.assertEqual(kwargs['experiment'],'E3')
            self.assertEqual(set(kwargs['manifests']),{'A','B','control'})
            self.assertEqual(len(kwargs['gateway_env_files']),2)
            self.assertEqual(set(kwargs['identity_files']),{'A:telemetry','B:telemetry','control:control'})
            self.assertEqual(kwargs['workload']['workload'],'low_load')
            self.assertEqual(kwargs['workload']['profile'],'official')

    def test_representative_probe_status_classification_is_literal_and_distinguishes_conflicts(self):
        self.assertTrue(e3_fixture._probe_status_classification('busy', 409)['match'])
        self.assertTrue(e3_fixture._probe_status_classification('stale', 409)['match'])
        self.assertFalse(e3_fixture._probe_status_classification('busy', 202)['match'])
        self.assertFalse(e3_fixture._probe_status_classification('noop', 202)['match'])

    def test_representative_probe_classification_order_and_metric_separation(self):
        kinds = ('offline_admission', 'same_key_replay', 'busy', 'stale',
                 'online_fresh_apply', 'noop', 'noop_replay')
        statuses = (202, 202, 409, 409, 202, 200, 200)
        ops = [{'kind': k, **e3_fixture._probe_status_classification(k, s)}
               for k, s in zip(kinds, statuses) if k != 'online_fresh_apply']
        ops.insert(4, {'kind': 'online_fresh_apply', 'expected': 202, 'actual': 202, 'match': True})
        self.assertEqual([row['expected'] for row in ops], list(statuses))
        counts = {'ledger': 5, 'raw': 5, 'temporal': 5}
        offline = {'executor_running': False, 'receipt_verified': True,
                   'terminal_status': 'published', 'outbox_status': 'published',
                   'same_key_replay_bytes_match': True}
        self.assertTrue(e3_fixture._representative_probe_accounting(ops, counts, counts, 0, offline))
        self.assertFalse(e3_fixture._representative_probe_accounting(ops, counts, counts, 1, offline))
        self.assertFalse(e3_fixture._representative_probe_accounting(ops, counts, counts, 0))
        self.assertFalse(e3_fixture._representative_probe_accounting(
            [{**ops[2], 'actual': 202, 'match': False}, *ops[1:]], counts, counts, 0, offline))
        self.assertFalse(e3_fixture._representative_probe_accounting(ops, counts, {**counts, 'raw': 6}, 0, offline))
        self.assertTrue(e3_fixture.run_representative_probes.__doc__.find('outside E3 measurement') >= 0)

    def test_wakeup_probe_orders_executor_readiness_before_online_admission(self):
        source = Path(e3_fixture.__file__).read_text(encoding='utf-8')
        helper = source[source.index('def run_representative_probes'):source.index('\ndef _probe_http_request_with_headers')]
        self.assertLess(helper.index('READY_MARKER in'), helper.index('online_status, online_raw'))
        self.assertLess(helper.index("'executor_running': False"), helper.index('online_status, online_raw'))
        self.assertIn("'delivery_guarantee': 'none'", helper)
        self.assertIn("'terminal_status': offline_proof['terminal_status']", helper)

    def test_post_stop_drain_gate_accepts_only_clean_pending_partial(self):
        artifact = {'status': 'PARTIAL', 'primary_error': None,
                    'cleanup_errors': [], 'actual_stop_confirmed': True,
                    'phase_observations': {'errors': []},
                    'children': [{'status': 'complete'}]}
        outputs = [{'status': 'COMPLETE'}] * 3
        self.assertTrue(e3_fixture._eligible_post_stop_drain(
            artifact, outputs, [{'pending_outbox': 2}, {'pending_outbox': 0}]))
        self.assertEqual(artifact['status'], 'PARTIAL')

    def test_post_stop_drain_gate_refuses_other_failure_modes(self):
        base = {'status': 'PARTIAL', 'primary_error': None,
                'cleanup_errors': [], 'actual_stop_confirmed': True,
                'phase_observations': {'errors': []},
                'children': [{'status': 'complete'}]}
        outputs = [{'status': 'COMPLETE'}] * 3
        summary = [{'pending_outbox': 2}]
        for changes in ({'primary_error': 'error'}, {'cleanup_errors': ['error']},
                        {'actual_stop_confirmed': False}, {'status': 'FAIL'},
                        {'phase_observations': {'errors': ['invalid']}},
                        {'children': [{'status': 'running'}]}):
            with self.subTest(changes=changes):
                self.assertFalse(e3_fixture._eligible_post_stop_drain(
                    {**base, **changes}, outputs, summary))
        self.assertFalse(e3_fixture._eligible_post_stop_drain(
            base, [{'status': 'FAILED'}, *outputs[1:]], summary))
        self.assertFalse(e3_fixture._eligible_post_stop_drain(
            base, outputs, [{'pending_outbox': 0}]))

    def test_pending_outbox_snapshot_preserves_payload_hash_and_is_bounded(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / 'outbox.sqlite'
            payload = b'exact original payload'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE outbox (id INTEGER PRIMARY KEY, message_id TEXT, payload BLOB, byte_size INTEGER, gateway_id TEXT, sensor_id TEXT, first_sequence INTEGER, sample_count INTEGER, status TEXT)')
                conn.execute('INSERT INTO outbox VALUES (1, ?, ?, ?, ?, ?, ?, ?, \'pending\')',
                             ('message-1', payload, len(payload), 'gw-1', 'sensor-1', 12, 2))
            rows = e3_fixture._pending_outbox_identities(db)
            self.assertEqual(rows, [{'message_id': 'message-1',
                                     'payload_sha256': hashlib.sha256(payload).hexdigest(),
                                      'payload_bytes': len(payload), 'byte_size': len(payload),
                                      'gateway_id': 'gw-1', 'sensor_id': 'sensor-1',
                                      'first_sequence': 12, 'sample_count': 2}])
            with self.assertRaisesRegex(ValueError, 'exceeds_capacity'):
                e3_fixture._pending_outbox_identities(db, max_rows=0)

    def test_post_stop_drain_rejects_incomplete_counts_and_config_or_identity_changes(self):
        prior = [{'message_id': 'old', 'payload_sha256': 'sha-old', 'sample_count': 2,
                  'gateway_id': 'gw', 'sensor_id': 'sensor', 'first_sequence': 7}]
        summary = {'completed': 'true', 'enqueued': '1', 'app_acknowledged': '2',
                   'pending_rows': '0', 'pending_bytes': '0'}
        config = {'desired_version': '2', 'sample_interval_us': 1333333}
        after = {'pending_outbox': 0, 'configuration': config}
        identity = [{'payload_sha256': 'sha-old', 'ledger_hash': 'sha-old',
                     'sample_count': 2, 'raw_count': 2, 'gateway_id': 'gw',
                     'sensor_id': 'sensor', 'first_sequence': 7}]
        self.assertTrue(e3_fixture._post_stop_drain_verified(0, summary, prior, [], after, config, identity))
        self.assertFalse(e3_fixture._post_stop_drain_verified(
            0, {**summary, 'enqueued': '0'}, prior, [], after, config, identity))
        self.assertFalse(e3_fixture._post_stop_drain_verified(
            0, {**summary, 'app_acknowledged': '0'}, prior, [], after, config, identity))
        self.assertFalse(e3_fixture._post_stop_drain_verified(
            0, summary, prior, [], {**after, 'configuration': {'desired_version': '3'}}, config, identity))
        self.assertFalse(e3_fixture._post_stop_drain_verified(
            0, summary, prior, [], after, config,
             [{'payload_sha256': 'sha-old', 'ledger_hash': 'other',
               'sample_count': 2, 'raw_count': 2, 'gateway_id': 'gw',
               'sensor_id': 'sensor', 'first_sequence': 7}]))
        self.assertFalse(e3_fixture._post_stop_drain_verified(
            0, summary, prior, [], after, config,
            [{'payload_sha256': 'sha-old', 'ledger_hash': 'sha-old',
              'sample_count': 2, 'raw_count': 1, 'gateway_id': 'gw',
              'sensor_id': 'sensor', 'first_sequence': 7}]))

    def test_executor_telemetry_batch_fits_backend_span_limit(self):
        env = e3_fixture._executor_environment(
            {'GATEWAY_ID': 'gw1', 'GATEWAY_MQTT_CLIENT_ID': 'base-client',
             'GATEWAY_SAMPLE_RATE_HZ': '100', 'GATEWAY_SAMPLES_PER_BATCH': '100'},
            'command-sensor', 'executor-client')
        self.assertEqual(env['GATEWAY_SAMPLE_RATE_HZ'], '2')
        self.assertEqual(env['GATEWAY_SAMPLES_PER_BATCH'], '1')

    def _receipt_fixture(self):
        commands = []
        pg = {}
        for sid, suffix, frequency in (('p8-e3-command-a', 'a', '1.5'), ('p8-e3-command-b', 'b', '0.75')):
            for version in (1, 2):
                key = f'{sid}-{version}'
                cid = f'cmd-{suffix}-{version}'
                command = {'sensor_id': sid, 'idempotency_key': key,
                           'expected_desired_version': version - 1, 'frequency_hz': frequency}
                commands.append(command)
                pg[key] = {'idempotency_key': key, 'command_id': cid, 'status': 'succeeded',
                           'status_code': 202, 'result_hash': f'hash-{cid}',
                           'desired_version': str(version),
                            'sample_interval_us': {'1.5': 666667, '0.75': 1333333}[frequency]}
        data = []
        rest = {}
        for sid, suffix, interval in (('p8-e3-command-a', 'a', 666667), ('p8-e3-command-b', 'b', 1333333)):
            receipts = []
            for version in (1, 2):
                cid = f'cmd-{suffix}-{version}'
                receipts.append({'command_id': cid, 'status': 'completed', 'desired_version': str(version),
                                 'sample_interval_us': interval, 'result_hash': f'hash-{cid}',
                                 'result_summary': {'command_id': cid, 'desired_version': str(version),
                                                    'status': 'succeeded', 'sample_interval_us': interval}})
            data.append({'completed_receipts_count': 2, 'pending_outbox': 0, 'receipts': receipts,
                         'configuration': {'desired_version': '2', 'sample_interval_us': interval}})
            rest[sid] = {'desired_version': '2', 'sample_interval_us': interval}
        return data, commands, pg, rest

    def test_reconciliation_rejects_cross_executor_duplicate_receipt_ids(self):
        data, commands, pg, rest = self._receipt_fixture()
        data[1]['receipts'][0] = dict(data[0]['receipts'][0])
        self.assertFalse(e3_fixture._verify_control_receipts(
            data, commands, pg, rest, ['p8-e3-command-a', 'p8-e3-command-b']))

    def test_reconciliation_rejects_decoded_result_identity_mismatch(self):
        data, commands, pg, rest = self._receipt_fixture()
        data[0]['receipts'][0]['result_summary']['command_id'] = 'other-command'
        self.assertFalse(e3_fixture._verify_control_receipts(
            data, commands, pg, rest, ['p8-e3-command-a', 'p8-e3-command-b']))

    def test_reconciliation_rejects_mismatched_final_configuration(self):
        data, commands, pg, rest = self._receipt_fixture()
        data[1]['configuration']['sample_interval_us'] = 500000
        self.assertFalse(e3_fixture._verify_control_receipts(
            data, commands, pg, rest, ['p8-e3-command-a', 'p8-e3-command-b']))

    def test_reconciliation_rejects_missing_reported_or_executor_configuration(self):
        data, commands, pg, rest = self._receipt_fixture()
        data[0]['configuration'] = None
        self.assertFalse(e3_fixture._verify_control_receipts(
            data, commands, pg, rest, ['p8-e3-command-a', 'p8-e3-command-b']))

    def test_real_exported_executor_receipt_shape_projects_and_reconciles(self):
        commands = []
        postgres = {}
        exported = []
        rest = {}
        for sid, interval, frequency in (('sensor-alpha', 666667, '1.5'),
                                         ('sensor-beta', 1333333, '0.75')):
            executor = {'configuration': {'desired_version': '2',
                        'sample_interval_us': interval, 'applied_at': 'captured'},
                        'pending_outbox': 0, 'receipts': []}
            rest[sid] = dict(executor['configuration'])
            for version in (1, 2):
                key, command_id = f'{sid}-{version}', f'cmd-{sid}-{version}'
                commands.append({'sensor_id': sid, 'idempotency_key': key,
                    'expected_desired_version': version - 1, 'frequency_hz': frequency})
                digest = f'hash-{command_id}'
                result = {'command_id': command_id, 'desired_version': str(version),
                    'sample_interval_us': interval, 'status': 'succeeded', 'result_sha256': digest}
                executor['receipts'].append({'command_id': command_id, 'desired_version': str(version),
                    'sample_interval_us': interval, 'status': 'completed', 'result': result})
                postgres[key] = {'idempotency_key': key, 'command_id': command_id,
                    'status': 'succeeded', 'result_hash': digest, 'desired_version': str(version),
                    'sample_interval_us': interval}
            exported.append(executor)
        projected = e3_fixture._e3_executor_receipt_projection(exported)
        self.assertEqual([row['configuration']['sample_interval_us'] for row in projected],
                         [666667, 1333333])
        self.assertTrue(e3_fixture._verify_control_receipts(
            projected, commands, postgres, rest, ['sensor-alpha', 'sensor-beta']))
        with self.assertRaisesRegex(ValueError, 'e3_rest_reported_configuration_missing'):
            e3_fixture._e3_reported_configuration({'reported_state': {}})
        data, commands, pg, rest = self._receipt_fixture()
        del rest['p8-e3-command-b']
        self.assertFalse(e3_fixture._verify_control_receipts(
            data, commands, pg, rest, ['p8-e3-command-a', 'p8-e3-command-b']))

    def test_collector_stop_exception_still_terminates_process(self):
        collector = Mock()
        collector.stop.side_effect = RuntimeError('collector stop failed')
        proc = SimpleNamespace(pid=123)
        with patch.object(e3_fixture, '_stop') as stop_process:
            collector_error, cleanup_failure = e3_fixture._stop_collector_and_process(collector, proc)
        self.assertIsInstance(collector_error, RuntimeError)
        self.assertIsNone(cleanup_failure)
        stop_process.assert_called_once_with(proc)

    def test_direct_script_loader_imports_without_repo_package_context(self):
        spec = importlib.util.spec_from_file_location(
            'e3_fixture_direct_loader', Path(e3_fixture.__file__).resolve())
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertTrue(callable(module.run))

    def test_effective_flags_missing_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'effective_fixture_flags_mismatch'):
            e3_fixture._validate_e3_fixture_flags({}, 'polling')

    def test_two_same_gateway_separate_executors_env_contract(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            gw_id = 'gw-alpha'
            password = 'test-secret-password-123'
            ca_path = root / 'ca.crt'
            ca_path.write_text('dummy-ca', encoding='utf-8')
            ca_path.chmod(0o600)

            cmd_sensors = ['p8-e3-command-a', 'p8-e3-command-b']
            env_files = []
            for idx, sid in enumerate(cmd_sensors):
                client_id = f'{gw_id}_p8_e3_executor_{idx + 1}'
                outbox_path = str((root / f'executor_{idx + 1}.sqlite').resolve())
                base_env = e3_fixture.gateway_env(gw_id, password)
                base_env.update({
                    'GATEWAY_CONTROL_ENABLED': 'true',
                    'GATEWAY_MQTT_BROKER_URL': 'ssl://127.0.0.1:18883',
                    'GATEWAY_MQTT_CA_FILE': str(ca_path.resolve()),
                    'GATEWAY_MQTT_SERVER_NAME': 'localhost',
                    'GATEWAY_SAMPLE_RATE_HZ': '1',
                    'GATEWAY_SAMPLES_PER_BATCH': '100',
                    'GATEWAY_OUTBOX_PATH': outbox_path,
                    'GATEWAY_OUTBOX_MAX_ROWS': '500',
                    'GATEWAY_OUTBOX_MAX_BYTES': str(4 << 20),
                    'GATEWAY_SHUTDOWN_TIMEOUT': '10s',
                    'GATEWAY_ACK_TIMEOUT': '5s',
                    'GATEWAY_RETRY_MIN_DELAY': '250ms',
                    'GATEWAY_RETRY_MAX_DELAY': '2s',
                    'GATEWAY_SQLITE_TIMEOUT': '5s',
                    'GATEWAY_MAX_INFLIGHT': '8',
                })
                env = e3_fixture.command_simulator_environment(base_env, sid, client_id)
                clean_env = {k: str(v) for k, v in env.items() if k in GATEWAY_ENV_KEYS}

                env_file = root / f'executor_{idx + 1}.env.json'
                env_file.write_text(json.dumps(clean_env, sort_keys=True), encoding='utf-8')
                env_file.chmod(0o600)
                # Must pass _gateway_environment
                checked = _gateway_environment(str(env_file))
                env_files.append((env_file, checked))

            # Validate both
            self.assertEqual(len(env_files), 2)
            e1, e2 = env_files[0][1], env_files[1][1]
            # Same gateway
            self.assertEqual(e1['GATEWAY_ID'], gw_id)
            self.assertEqual(e2['GATEWAY_ID'], gw_id)
            # Distinct sensors
            self.assertEqual(e1['SENSOR_ID'], 'p8-e3-command-a')
            self.assertEqual(e2['SENSOR_ID'], 'p8-e3-command-b')
            # Distinct clients
            self.assertNotEqual(e1['GATEWAY_MQTT_CLIENT_ID'], e2['GATEWAY_MQTT_CLIENT_ID'])
            self.assertNotEqual(e1['GATEWAY_MQTT_CLIENT_ID'], gw_id)
            # Distinct outbox SQLite paths
            self.assertNotEqual(e1['GATEWAY_OUTBOX_PATH'], e2['GATEWAY_OUTBOX_PATH'])
            # Rates & batch bounded
            self.assertEqual(e1['GATEWAY_SAMPLE_RATE_HZ'], '1')
            self.assertEqual(e1['GATEWAY_SAMPLES_PER_BATCH'], '100')
            self.assertEqual(e1['GATEWAY_CONTROL_ENABLED'], 'true')
            # No unsupported keys
            self.assertTrue(set(e1).issubset(GATEWAY_ENV_KEYS))
            self.assertTrue(set(e2).issubset(GATEWAY_ENV_KEYS))

    def test_inspect_executor_sqlite_private_and_accurate(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / 'test_outbox.sqlite'
            with sqlite3.connect(db_path) as conn:
                conn.execute("""
                CREATE TABLE sensor_configuration (
                    id INTEGER PRIMARY KEY CHECK(id=1),
                    sample_interval_us INTEGER NOT NULL,
                    desired_version INTEGER NOT NULL,
                    applied_at TEXT NOT NULL
                );""")
                conn.execute("""
                CREATE TABLE command_receipts (
                    command_id TEXT PRIMARY KEY,
                    command_hash TEXT NOT NULL,
                    command_topic TEXT NOT NULL,
                    command_bytes BLOB NOT NULL,
                    desired_version INTEGER NOT NULL,
                    sample_interval_us INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    result_bytes BLOB,
                    retain_until INTEGER NOT NULL
                );""")
                conn.execute("""
                CREATE TABLE outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    status TEXT NOT NULL
                );""")
                # Insert configuration
                conn.execute(
                    "INSERT INTO sensor_configuration VALUES (1, 500000, 2, '2026-10-28T12:00:00Z');")
                # Insert 2 completed receipts
                res_bytes_1 = b'{"command_id":"cmd-1","desired_version":1,"status":"succeeded","reported_state":{"sample_interval_us":666667}}'
                conn.execute(
                    "INSERT INTO command_receipts VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?);",
                    ('cmd-1', 'hash-1', 'topic-1', b'raw-cmd-1', 1, 666667, 1000, res_bytes_1, 2000))
                res_bytes_2 = b'{"command_id":"cmd-2","desired_version":2,"status":"succeeded","reported_state":{"sample_interval_us":500000}}'
                conn.execute(
                    "INSERT INTO command_receipts VALUES (?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?);",
                    ('cmd-2', 'hash-2', 'topic-2', b'raw-cmd-2', 2, 500000, 1001, res_bytes_2, 2001))

            data = e3_fixture._inspect_executor_sqlite(str(db_path))
            self.assertEqual(data['completed_receipts_count'], 2)
            self.assertEqual(data['pending_outbox'], 0)
            self.assertEqual(data['configuration']['desired_version'], '2')
            self.assertEqual(data['configuration']['sample_interval_us'], 500000)
            self.assertEqual(len(data['receipts']), 2)
            # Result hashes are computed, raw bytes are omitted
            self.assertEqual(data['receipts'][0]['result_hash'], hashlib.sha256(res_bytes_1).hexdigest())
            self.assertEqual(data['receipts'][0]['result_bytes_len'], len(res_bytes_1))
            self.assertNotIn('result_bytes', data['receipts'][0])
            self.assertNotIn('command_bytes', data['receipts'][0])
            self.assertEqual(data['receipts'][0]['result_summary'], {
                'command_id': 'cmd-1', 'desired_version': '1', 'status': 'succeeded',
                'sample_interval_us': 666667})

    def test_inspect_executor_sqlite_missing_file_safe(self):
        data = e3_fixture._inspect_executor_sqlite('/non/existent/path/outbox.sqlite')
        self.assertEqual(data['completed_receipts_count'], 0)
        self.assertEqual(data['pending_outbox'], -1)
        self.assertEqual(data['error'], 'file_not_found')

    def test_run_success_report_schema_and_secret_safety(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            private_dir = root / 'private'
            artifacts_dir = root / 'artifacts'
            private_dir.mkdir(parents=True, mode=0o700)
            artifacts_dir.mkdir(parents=True, mode=0o700)
            certs_dir = private_dir / 'certs'
            certs_dir.mkdir(parents=True, mode=0o700)
            ca_crt = certs_dir / 'ca.crt'
            ca_crt.write_text('dummy-ca', encoding='utf-8')
            ca_crt.chmod(0o600)

            paperload_bin = root / 'paperload'
            paperload_bin.write_bytes(b'dummy-paperload-binary')
            paperload_bin.chmod(0o700)
            gateway_bin = root / 'gateway'
            gateway_bin.write_bytes(b'dummy-gateway-binary')
            gateway_bin.chmod(0o700)

            fixture_mock = SimpleNamespace(
                private=private_dir,
                artifacts=artifacts_dir,
                project='test-e3-project',
            )
            endpoint_mock = SimpleNamespace(
                url='https://127.0.0.1:443',
                ca_file=ca_crt,
            )
            registry_mock = Mock()
            registry_mock.resolve.return_value = {
                'NetworkSettings': {'Ports': {'8883/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18883'}]}}
            }
            state_mock = Mock()
            containers_mock = {'mosquitto': 'mosq-1'}
            images_mock = {'gateway': 'sha256:12345'}

            gw_spec_a = SimpleNamespace(
                gateway_id='gw-a',
                sensor_id='sensor-a',
                sensor_urn='urn:ngsi-ld:Sensor:gw-a:sensor-a',
                sensor_unit='CEL',
            )
            gw_spec_b = SimpleNamespace(
                gateway_id='gw-b',
                sensor_id='sensor-b',
                sensor_urn='urn:ngsi-ld:Sensor:gw-b:sensor-b',
                sensor_unit='CEL',
            )

            token_a = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.dummy_token_a'
            smoke_mock = Mock()
            smoke_mock.ROOT = Path(__file__).resolve().parents[2]
            smoke_mock.prepare_paper_workload.return_value = (
                'test-pg', {'cpus': 2, 'memory_bytes': 4294967296},
                {}, {}, {}, {'admin': 'dummy_admin_token', 'a': token_a}, [gw_spec_a, gw_spec_b],
                ['gw-pass-a', 'gw-pass-b'], '/sensor',
            )
            target_call_count = {'n': 0}
            def mock_db_psql(reg, pg, sql, params=None):
                if 'schema_migrations' in sql:
                    return '24:telemetry_ledger_timestamp'
                if 'json_agg' in sql:
                    return json.dumps([{'message_id': '00000000-0000-0000-0000-000000000001',
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                if 'count(*)' in sql and 'abs(t.value' in sql:
                    return '0'
                if 'processed_messages' in sql and 'json_build_object' in sql:
                    target_call_count['n'] += 1
                    # Calls 1-4 are before (2 bg + 2 cmd), subsequent are after
                    if target_call_count['n'] <= 4:
                        return '{"ledger":0,"raw":0,"temporal":0,"configuration":0}'
                    return '{"ledger":12,"raw":1200,"temporal":12,"configuration":1}'
                if 'twin_command_requests' in sql:
                    key = params['key'] if params else 'k'
                    # Deterministic command ID for this key
                    cid = f'00000000-0000-0000-0000-{key[-12:]}'
                    return json.dumps({
                        'idempotency_key': key,
                        'status_code': 202,
                        'command_id': cid,
                        'status': 'succeeded',
                        'result_hash': hashlib.sha256(b'fake-res').hexdigest(),
                        'desired_version': '1',
                        'sample_interval_us': 666667,
                    })
                return '0'

            smoke_mock.db_psql.side_effect = mock_db_psql
            smoke_mock.paper_p1_count_db.return_value = {'ledger': 100, 'raw': 10000, 'temporal': 100}
            smoke_mock.paper_p0_stats.return_value = {'cpu_percent': 10.0, 'rss_bytes': 1000000}
            smoke_mock.paper_p0_git.return_value = 'mock-git-rev'
            smoke_mock.process_env.return_value = {}
            smoke_mock.paper_p1_histogram_window.return_value = {'availability': 'available'}
            smoke_mock.durable_json.side_effect = lambda p, d: Path(p).write_text(json.dumps(d), encoding='utf-8')
            def mock_private_write(path, content):
                p = Path(path)
                p.write_text(content, encoding='utf-8')
                p.chmod(0o600)
            smoke_mock.private_write.side_effect = mock_private_write

            # Mock http_request responses
            call_counts = {}
            def mock_http_request(ep, method, path, token=None, body=None, headers=None, timeout=None):
                call_counts[path] = call_counts.get(path, 0) + 1
                if method == 'PUT':
                    sid = path.split('/')[-1]
                    return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:gw-a:{sid}'}
                if method == 'GET' and '?format=simplified' in path:
                    if call_counts[path] == 1:
                        return 200, {}, {'desired_version': '0'}
                    if 'p8-e3-command-a' in path:
                        return 200, {}, {
                            'desired_version': '2',
                            'reported_state': {'configuration': {'desired_version': '2', 'sample_interval_us': 500000}}
                        }
                    else:
                        return 200, {}, {
                            'desired_version': '2',
                            'reported_state': {'configuration': {'desired_version': '2', 'sample_interval_us': 1333333}}
                        }
                if path == '/v1/admin/metrics':
                    return 200, {}, {
                        'command_admission': {},
                        'command_dispatch': {},
                        'command_timeline': {'missing_origin': 0},
                        'command_result': {},
                    }
                return 404, {}, {}

            smoke_mock.http_request.side_effect = mock_http_request

            # Mock subprocess.Popen for runner to write fake outputs
            real_popen = subprocess.Popen
            def mock_popen(argv, **kwargs):
                if argv and argv[0] == 'git':
                    return real_popen(argv, **kwargs)
                plan_dir = private_dir / 'p8-e3' / 'polling' / 'plan'
                a_out = plan_dir / 'A-output'
                b_out = plan_dir / 'B-output'
                ctrl_out = plan_dir / 'control-output'
                a_out.mkdir(parents=True, exist_ok=True)
                b_out.mkdir(parents=True, exist_ok=True)
                ctrl_out.mkdir(parents=True, exist_ok=True)

                (a_out / 'workload.json').write_text(json.dumps({
                    'status': 'COMPLETE',
                    'telemetry': {
                        'generated_unique': 8, 'offered_unique': 8,
                        'application_acks': 8, 'pending_messages': 0, 'pending_bytes': 0,
                    },
                    'identity_samples': [{'message_id': '00000000-0000-0000-0000-000000000001'}],
                }), encoding='utf-8')
                (b_out / 'workload.json').write_text(json.dumps({
                    'status': 'COMPLETE',
                    'telemetry': {
                        'generated_unique': 16, 'offered_unique': 16,
                        'application_acks': 16, 'pending_messages': 0, 'pending_bytes': 0,
                    },
                    'identity_samples': [{'message_id': '00000000-0000-0000-0000-000000000002'}],
                }), encoding='utf-8')

                run_spec_path = plan_dir / 'e3-run-spec.json'
                spec_data = json.loads(run_spec_path.read_text(encoding='utf-8'))
                # Read manifests to get idempotency keys
                ctrl_manifest = json.loads(Path(spec_data['paperload'][2]['manifest']).read_text(encoding='utf-8'))
                keys = [c['idempotency_key'] for c in ctrl_manifest['commands']]

                (ctrl_out / 'workload.json').write_text(json.dumps({
                    'status': 'COMPLETE',
                    'planned': 4, 'sent': 4, 'skipped': 0, 'pending': 0,
                    'commands': [{'classification': 'succeeded', 'idempotency_key': k} for k in keys],
                }), encoding='utf-8')

                events = [
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 100},
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 900},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_begin', 'observed_at_runner_monotonic_ns': 300},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_end', 'observed_at_runner_monotonic_ns': 800},
                ]
                for i in range(1, 5):
                    events.extend([
                        {'child': 'paperload-control:control.manifest.json', 'event': 'request_begin', 'command_ordinal': i, 'observed_at_runner_monotonic_ns': 350 + i},
                        {'child': 'paperload-control:control.manifest.json', 'event': 'admission_observed', 'command_ordinal': i, 'observed_at_runner_monotonic_ns': 360 + i},
                        {'child': 'paperload-control:control.manifest.json', 'event': 'terminal_observed', 'command_ordinal': i, 'observed_at_runner_monotonic_ns': 400 + i},
                    ])
                events.extend([
                    {'child': 'paperload-control:control.manifest.json', 'event': 'generation_stopped', 'command_ordinal': 0, 'observed_at_runner_monotonic_ns': 900},
                    {'child': 'paperload-control:control.manifest.json', 'event': 'producers_stopped', 'command_ordinal': 0, 'observed_at_runner_monotonic_ns': 910},
                    {'child': 'paperload-control:control.manifest.json', 'event': 'workers_joined', 'command_ordinal': 0, 'observed_at_runner_monotonic_ns': 920},
                ])
                (plan_dir / 'run.json').write_text(json.dumps({'phase_observations': {'events': events}}), encoding='utf-8')

                # Create fake SQLite databases
                for idx, assigned_keys in ((1, (keys[0], keys[2])), (2, (keys[1], keys[3]))):
                    sqlite_path = private_dir / 'p8-e3' / 'polling' / f'executor_{idx}.sqlite'
                    with sqlite3.connect(sqlite_path) as conn:
                        conn.execute("CREATE TABLE sensor_configuration (id INTEGER PRIMARY KEY, sample_interval_us INTEGER, desired_version INTEGER, applied_at TEXT);")
                        conn.execute("CREATE TABLE command_receipts (command_id TEXT PRIMARY KEY, command_hash TEXT, desired_version INTEGER, sample_interval_us INTEGER, expires_at INTEGER, status TEXT, result_bytes BLOB);")
                        conn.execute("CREATE TABLE outbox (id INTEGER PRIMARY KEY, status TEXT);")
                        interval = 500000 if idx == 1 else 1333333
                        conn.execute("INSERT INTO sensor_configuration VALUES (1, ?, 2, 'applied');", (interval,))
                        cid1 = f'00000000-0000-0000-0000-{assigned_keys[0][-12:]}'
                        cid2 = f'00000000-0000-0000-0000-{assigned_keys[1][-12:]}'
                        conn.execute("INSERT INTO command_receipts VALUES (?, 'h1', 1, ?, 100, 'completed', ?);",
                                     (cid1, interval, b'fake-res'))
                        conn.execute("INSERT INTO command_receipts VALUES (?, 'h2', 2, ?, 100, 'completed', ?);",
                                     (cid2, interval, b'fake-res'))

                p = Mock()
                p.poll.return_value = 0
                p.returncode = 0
                p.wait.return_value = 0
                p.pid = 9999
                return p

            with patch('subprocess.Popen', side_effect=mock_popen):
                with self.assertRaisesRegex(ValueError, 'p8_e3_validation_failed'):
                    e3_fixture.run(
                        smoke_mock, fixture_mock, endpoint_mock, registry_mock,
                        containers_mock, images_mock, state_mock,
                        str(paperload_bin), str(gateway_bin), 'polling',
                    )
            result = json.loads((artifacts_dir / 'p8-e3-polling.json').read_text(encoding='utf-8'))
            self.assertEqual(result['status'], 'FAIL')
            self.assertFalse(result['full_p8_accepted'])
            self.assertIn('validation_failed', result['failure'])

            # Secret safety checks: Ensure no tokens or passwords leaked
            dumped = json.dumps(result)
            self.assertNotIn('gw-pass-a', dumped)
            self.assertNotIn('gw-pass-b', dumped)
            self.assertNotIn(token_a, dumped)
            self.assertNotIn('test-pg', dumped)
            self.assertNotIn('fake-res', dumped)

    def test_run_failure_writes_fail_before_raising(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            private_dir = root / 'private'
            artifacts_dir = root / 'artifacts'
            private_dir.mkdir(parents=True, mode=0o700)
            artifacts_dir.mkdir(parents=True, mode=0o700)
            ca_crt = private_dir / 'ca.crt'
            ca_crt.write_text('ca', encoding='utf-8')
            ca_crt.chmod(0o600)

            fixture_mock = SimpleNamespace(
                private=private_dir,
                artifacts=artifacts_dir,
                project='test-fail-project',
            )
            endpoint_mock = SimpleNamespace(
                url='https://127.0.0.1:443',
                ca_file=ca_crt,
            )
            registry_mock = Mock()
            registry_mock.resolve.return_value = {
                'NetworkSettings': {'Ports': {'8883/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18883'}]}}
            }
            state_mock = Mock()
            containers_mock = {'mosquitto': 'mosq-1'}
            images_mock = {'gateway': 'sha256:12345'}

            smoke_mock = Mock()
            smoke_mock.ROOT = root
            gw_spec = SimpleNamespace(
                gateway_id='gw-1', sensor_id='s-1',
                sensor_urn='urn:ngsi-ld:Sensor:gw-1:s-1', sensor_unit='CEL')
            smoke_mock.prepare_paper_workload.return_value = (
                'test-pg', {}, {}, {}, {}, {'a': 'tok'}, [gw_spec, gw_spec],
                ['p1', 'p2'], '/sensor')
            smoke_mock.db_psql.return_value = '24:telemetry_ledger_timestamp'
            smoke_mock.durable_json.side_effect = lambda p, d: Path(p).write_text(json.dumps(d), encoding='utf-8')

            # Failure during PUT
            smoke_mock.http_request.return_value = (500, {}, {'error': 'internal'})

            with self.assertRaises(ValueError) as cm:
                e3_fixture.run(
                    smoke_mock, fixture_mock, endpoint_mock, registry_mock,
                    containers_mock, images_mock, state_mock,
                    '/fake/paperload', '/fake/gateway', 'polling')
            self.assertIn('p8_e3_command_sensor_provision_failed', str(cm.exception))

    def test_runner_nonzero_exit_writes_fail_before_raising(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            private_dir = root / 'private'
            artifacts_dir = root / 'artifacts'
            private_dir.mkdir(parents=True, mode=0o700)
            artifacts_dir.mkdir(parents=True, mode=0o700)
            certs_dir = private_dir / 'certs'
            certs_dir.mkdir(parents=True, mode=0o700)
            ca_crt = certs_dir / 'ca.crt'
            ca_crt.write_text('dummy-ca', encoding='utf-8')
            ca_crt.chmod(0o600)

            paperload_bin = root / 'paperload'
            paperload_bin.write_bytes(b'dummy-paperload-binary')
            paperload_bin.chmod(0o700)
            gateway_bin = root / 'gateway'
            gateway_bin.write_bytes(b'dummy-gateway-binary')
            gateway_bin.chmod(0o700)

            fixture_mock = SimpleNamespace(private=private_dir, artifacts=artifacts_dir, project='test-fail')
            endpoint_mock = SimpleNamespace(url='https://127.0.0.1:443', ca_file=ca_crt)
            registry_mock = Mock()
            registry_mock.resolve.return_value = {'NetworkSettings': {'Ports': {'8883/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18883'}]}}}
            state_mock = Mock()
            containers_mock = {'mosquitto': 'mosq-1'}
            images_mock = {'gateway': 'sha256:12345'}
            gw_spec_a = SimpleNamespace(gateway_id='gw-a', sensor_id='sensor-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:sensor-a', sensor_unit='CEL')
            gw_spec_b = SimpleNamespace(gateway_id='gw-b', sensor_id='sensor-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:sensor-b', sensor_unit='CEL')

            smoke_mock = Mock()
            smoke_mock.ROOT = Path(__file__).resolve().parents[2]
            smoke_mock.prepare_paper_workload.return_value = (
                'test-pg', {'cpus': 2, 'memory_bytes': 4294967296},
                {}, {}, {}, {'admin': 'tok', 'a': 'tok'}, [gw_spec_a, gw_spec_b],
                ['gw-pass-a', 'gw-pass-b'], '/sensor')
            def mock_fail_db_psql(reg, pg, sql, params=None):
                if 'schema_migrations' in sql:
                    return '24:telemetry_ledger_timestamp'
                return '{"ledger": 0, "raw": 0, "temporal": 0, "configuration": 0}'
            smoke_mock.db_psql.side_effect = mock_fail_db_psql
            smoke_mock.paper_p1_count_db.return_value = {'ledger': 0, 'raw': 0, 'temporal': 0}
            smoke_mock.paper_p0_stats.return_value = {'cpu_percent': 0, 'rss_bytes': 0}
            smoke_mock.paper_p0_git.return_value = 'mock-rev'
            smoke_mock.process_env.return_value = {}
            smoke_mock.durable_json.side_effect = lambda p, d: Path(p).write_text(json.dumps(d), encoding='utf-8')
            def mock_private_write(path, content):
                p = Path(path); p.write_text(content, encoding='utf-8'); p.chmod(0o600)
            smoke_mock.private_write.side_effect = mock_private_write

            def mock_http_request(ep, method, path, token=None, body=None, headers=None, timeout=None):
                if method == 'PUT':
                    sid = path.split('/')[-1]
                    return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:gw-a:{sid}'}
                if method == 'GET' and '?format=simplified' in path:
                    return 200, {}, {'desired_version': '0'}
                if path == '/v1/admin/metrics':
                    return 200, {}, {'command_admission': {}, 'command_dispatch': {}, 'command_timeline': {}, 'command_result': {}}
                return 404, {}, {}
            smoke_mock.http_request.side_effect = mock_http_request

            real_popen = subprocess.Popen
            def mock_fail_popen(argv, **kwargs):
                if argv and argv[0] == 'git':
                    return real_popen(argv, **kwargs)
                p = Mock()
                p.poll.return_value = 1
                p.returncode = 1
                p.wait.return_value = 1
                p.pid = 9999
                return p

            with patch('subprocess.Popen', side_effect=mock_fail_popen):
                with self.assertRaises(ValueError) as cm:
                    e3_fixture.run(
                        smoke_mock, fixture_mock, endpoint_mock, registry_mock,
                        containers_mock, images_mock, state_mock,
                        str(paperload_bin), str(gateway_bin), 'polling')
            self.assertIn('p8_e3_runner_failed_code_1', str(cm.exception))
            written_artifact = artifacts_dir / 'p8-e3-polling.json'
            self.assertTrue(written_artifact.is_file())
            fail_data = json.loads(written_artifact.read_text(encoding='utf-8'))
            self.assertEqual(fail_data['status'], 'FAIL')
            self.assertEqual(fail_data['failure'], 'runner_nonzero_or_missing')
            self.assertFalse(fail_data['full_p8_accepted'])

    def test_persisted_receipt_sqlite_mismatch_detected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            private_dir = root / 'private'
            artifacts_dir = root / 'artifacts'
            private_dir.mkdir(parents=True, mode=0o700)
            artifacts_dir.mkdir(parents=True, mode=0o700)
            certs_dir = private_dir / 'certs'
            certs_dir.mkdir(parents=True, mode=0o700)
            ca_crt = certs_dir / 'ca.crt'
            ca_crt.write_text('dummy-ca', encoding='utf-8')
            ca_crt.chmod(0o600)

            paperload_bin = root / 'paperload'
            paperload_bin.write_bytes(b'dummy-paperload-binary')
            paperload_bin.chmod(0o700)
            gateway_bin = root / 'gateway'
            gateway_bin.write_bytes(b'dummy-gateway-binary')
            gateway_bin.chmod(0o700)

            fixture_mock = SimpleNamespace(private=private_dir, artifacts=artifacts_dir, project='test-mismatch')
            endpoint_mock = SimpleNamespace(url='https://127.0.0.1:443', ca_file=ca_crt)
            registry_mock = Mock()
            registry_mock.resolve.return_value = {'NetworkSettings': {'Ports': {'8883/tcp': [{'HostIp': '127.0.0.1', 'HostPort': '18883'}]}}}
            state_mock = Mock()
            containers_mock = {'mosquitto': 'mosq-1'}
            images_mock = {'gateway': 'sha256:12345'}

            gw_spec_a = SimpleNamespace(gateway_id='gw-a', sensor_id='sensor-a', sensor_urn='urn:ngsi-ld:Sensor:gw-a:sensor-a', sensor_unit='CEL')
            gw_spec_b = SimpleNamespace(gateway_id='gw-b', sensor_id='sensor-b', sensor_urn='urn:ngsi-ld:Sensor:gw-b:sensor-b', sensor_unit='CEL')

            smoke_mock = Mock()
            smoke_mock.ROOT = Path(__file__).resolve().parents[2]
            smoke_mock.prepare_paper_workload.return_value = (
                'test-pg', {'cpus': 2, 'memory_bytes': 4294967296},
                {}, {}, {}, {'admin': 'admin-tok', 'a': 'token-a'}, [gw_spec_a, gw_spec_b],
                ['gw-pass-a', 'gw-pass-b'], '/sensor')

            target_call_count = {'n': 0}
            def mock_db_psql(reg, pg, sql, params=None):
                if 'schema_migrations' in sql:
                    return '24:telemetry_ledger_timestamp'
                if 'json_agg' in sql:
                    return json.dumps([{'message_id': '00000000-0000-0000-0000-000000000001',
                                        'first_sequence': '0', 'last_sequence': '99', 'sample_count': 100}])
                if 'count(*)' in sql and 'abs(t.value' in sql:
                    return '0'
                if 'processed_messages' in sql and 'json_build_object' in sql:
                    target_call_count['n'] += 1
                    if target_call_count['n'] <= 4:
                        return '{"ledger":0,"raw":0,"temporal":0,"configuration":0}'
                    return '{"ledger":12,"raw":1200,"temporal":12,"configuration":1}'
                if 'twin_command_requests' in sql:
                    key = params['key'] if params else 'k'
                    cid = f'00000000-0000-0000-0000-{key[-12:]}'
                    return json.dumps({
                        'idempotency_key': key,
                        'status_code': 202,
                        'command_id': cid,
                        'status': 'succeeded',
                        'result_hash': 'server_sha256_hash_here',
                        'desired_version': '1',
                        'sample_interval_us': 666667,
                    })
                return '0'

            smoke_mock.db_psql.side_effect = mock_db_psql
            smoke_mock.paper_p1_count_db.return_value = {'ledger': 100, 'raw': 10000, 'temporal': 100}
            smoke_mock.paper_p0_stats.return_value = {'cpu_percent': 10.0, 'rss_bytes': 1000000}
            smoke_mock.paper_p0_git.return_value = 'mock-rev'
            smoke_mock.process_env.return_value = {}
            smoke_mock.paper_p1_histogram_window.return_value = {'availability': 'available'}
            smoke_mock.durable_json.side_effect = lambda p, d: Path(p).write_text(json.dumps(d), encoding='utf-8')
            def mock_private_write(path, content):
                p = Path(path); p.write_text(content, encoding='utf-8'); p.chmod(0o600)
            smoke_mock.private_write.side_effect = mock_private_write

            call_counts = {}
            def mock_http_request(ep, method, path, token=None, body=None, headers=None, timeout=None):
                call_counts[path] = call_counts.get(path, 0) + 1
                if method == 'PUT':
                    sid = path.split('/')[-1]
                    return 201, {}, {'entity_id': f'urn:ngsi-ld:Sensor:gw-a:{sid}'}
                if method == 'GET' and '?format=simplified' in path:
                    if call_counts[path] == 1:
                        return 200, {}, {'desired_version': '0'}
                    if 'p8-e3-command-a' in path:
                        return 200, {}, {
                            'desired_version': '2',
                            'reported_state': {'configuration': {'desired_version': '2', 'sample_interval_us': 500000}}
                        }
                    else:
                        return 200, {}, {
                            'desired_version': '2',
                            'reported_state': {'configuration': {'desired_version': '2', 'sample_interval_us': 1333333}}
                        }
                if path == '/v1/admin/metrics':
                    return 200, {}, {'command_admission': {}, 'command_dispatch': {}, 'command_timeline': {'missing_origin': 0}, 'command_result': {}}
                return 404, {}, {}
            smoke_mock.http_request.side_effect = mock_http_request

            real_popen = subprocess.Popen
            def mock_mismatch_popen(argv, **kwargs):
                if argv and argv[0] == 'git':
                    return real_popen(argv, **kwargs)
                plan_dir = private_dir / 'p8-e3' / 'polling' / 'plan'
                a_out = plan_dir / 'A-output'
                b_out = plan_dir / 'B-output'
                ctrl_out = plan_dir / 'control-output'
                a_out.mkdir(parents=True, exist_ok=True)
                b_out.mkdir(parents=True, exist_ok=True)
                ctrl_out.mkdir(parents=True, exist_ok=True)

                (a_out / 'workload.json').write_text(json.dumps({
                    'status': 'COMPLETE',
                    'telemetry': {'generated_unique': 8, 'offered_unique': 8, 'application_acks': 8, 'pending_messages': 0, 'pending_bytes': 0},
                    'identity_samples': [{'message_id': '00000000-0000-0000-0000-000000000001'}],
                }), encoding='utf-8')
                (b_out / 'workload.json').write_text(json.dumps({
                    'status': 'COMPLETE',
                    'telemetry': {'generated_unique': 16, 'offered_unique': 16, 'application_acks': 16, 'pending_messages': 0, 'pending_bytes': 0},
                    'identity_samples': [{'message_id': '00000000-0000-0000-0000-000000000002'}],
                }), encoding='utf-8')

                spec_data = json.loads((plan_dir / 'e3-run-spec.json').read_text(encoding='utf-8'))
                ctrl_manifest = json.loads(Path(spec_data['paperload'][2]['manifest']).read_text(encoding='utf-8'))
                keys = [c['idempotency_key'] for c in ctrl_manifest['commands']]

                (ctrl_out / 'workload.json').write_text(json.dumps({
                    'status': 'COMPLETE', 'planned': 4, 'sent': 4, 'skipped': 0, 'pending': 0,
                    'commands': [{'classification': 'succeeded', 'idempotency_key': k} for k in keys],
                }), encoding='utf-8')

                events = [
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_begin', 'observed_at_runner_monotonic_ns': 100},
                    {'child': 'paperload-telemetry:A.manifest.json', 'event': 'measure_end', 'observed_at_runner_monotonic_ns': 900},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_begin', 'observed_at_runner_monotonic_ns': 300},
                    {'child': 'paperload-telemetry:B.manifest.json', 'event': 'burst_end', 'observed_at_runner_monotonic_ns': 800},
                ]
                for i in range(1, 5):
                    events.extend([
                        {'child': 'paperload-control:control.manifest.json', 'event': 'request_begin', 'command_ordinal': i, 'observed_at_runner_monotonic_ns': 350 + i},
                        {'child': 'paperload-control:control.manifest.json', 'event': 'admission_observed', 'command_ordinal': i, 'observed_at_runner_monotonic_ns': 360 + i},
                        {'child': 'paperload-control:control.manifest.json', 'event': 'terminal_observed', 'command_ordinal': i, 'observed_at_runner_monotonic_ns': 400 + i},
                    ])
                events.extend([
                    {'child': 'paperload-control:control.manifest.json', 'event': 'generation_stopped', 'command_ordinal': 0, 'observed_at_runner_monotonic_ns': 900},
                    {'child': 'paperload-control:control.manifest.json', 'event': 'producers_stopped', 'command_ordinal': 0, 'observed_at_runner_monotonic_ns': 910},
                    {'child': 'paperload-control:control.manifest.json', 'event': 'workers_joined', 'command_ordinal': 0, 'observed_at_runner_monotonic_ns': 920},
                ])
                (plan_dir / 'run.json').write_text(json.dumps({'phase_observations': {'events': events}}), encoding='utf-8')

                # In SQLite, insert MISMATCHED command_id in executor_2
                for idx, assigned_keys in ((1, (keys[0], keys[2])), (2, (keys[1], keys[3]))):
                    sqlite_path = private_dir / 'p8-e3' / 'polling' / f'executor_{idx}.sqlite'
                    with sqlite3.connect(sqlite_path) as conn:
                        conn.execute("CREATE TABLE sensor_configuration (id INTEGER PRIMARY KEY, sample_interval_us INTEGER, desired_version INTEGER, applied_at TEXT);")
                        conn.execute("CREATE TABLE command_receipts (command_id TEXT PRIMARY KEY, command_hash TEXT, desired_version INTEGER, sample_interval_us INTEGER, expires_at INTEGER, status TEXT, result_bytes BLOB);")
                        conn.execute("CREATE TABLE outbox (id INTEGER PRIMARY KEY, status TEXT);")
                        interval = 500000 if idx == 1 else 1333333
                        conn.execute("INSERT INTO sensor_configuration VALUES (1, ?, 2, 'applied');", (interval,))
                        # If idx == 2, introduce mismatching command id
                        cid1 = f'00000000-0000-0000-0000-{assigned_keys[0][-12:]}' if idx == 1 else 'mismatched-cmd-id-1'
                        cid2 = f'00000000-0000-0000-0000-{assigned_keys[1][-12:]}'
                        conn.execute("INSERT INTO command_receipts VALUES (?, 'h1', 1, ?, 100, 'completed', ?);",
                                     (cid1, interval, b'fake-res'))
                        conn.execute("INSERT INTO command_receipts VALUES (?, 'h2', 2, ?, 100, 'completed', ?);",
                                     (cid2, interval, b'fake-res'))

                p = Mock()
                p.poll.return_value = 0
                p.returncode = 0
                p.wait.return_value = 0
                p.pid = 9999
                return p

            with patch('subprocess.Popen', side_effect=mock_mismatch_popen):
                with self.assertRaises(ValueError) as cm:
                    e3_fixture.run(
                        smoke_mock, fixture_mock, endpoint_mock, registry_mock,
                        containers_mock, images_mock, state_mock,
                        str(paperload_bin), str(gateway_bin), 'polling')
            self.assertIn('p8_e3_validation_failed', str(cm.exception))
            self.assertIn('control_receipts_unverified', str(cm.exception))


if __name__ == '__main__':
    unittest.main()
