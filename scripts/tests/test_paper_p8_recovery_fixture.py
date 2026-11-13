import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import paper_p8_recovery_fixture as fixture_module
import paper_p8_recovery_support as recovery
import paper_p8_e4_support as e4
import paper_p8_e5_fixture as e5_fixture


class RecoveryCompositionTests(unittest.TestCase):
    def test_auxiliary_pending_gateway_limits_are_config_valid(self):
        limits = e5_fixture.recovery_aux_outbox_limits()
        self.assertLessEqual(int(limits['GATEWAY_MAX_INFLIGHT']),
                             int(limits['GATEWAY_OUTBOX_MAX_ROWS']))

    def test_rest_command_receipts_are_read_from_owned_restored_database(self):
        command_keys = ['key-1', 'key-2', 'key-3', 'key-4']
        context = {'context': {'command_keys': command_keys, 'gateway_ids': ['gw-a']}}
        postgres = {'kind': 'container', 'receipt': {'id': 'postgres-id'}}
        target = recovery._target_database('rest-readback')
        calls = []

        def psql(registry, container_id, database, sql, variables):
            calls.append((container_id, database, variables))
            return json.dumps({'command_id': 'cmd-1', 'status': 'succeeded',
                               'result_hash': 'a' * 64})

        def http(endpoint, method, path, token=None):
            if '/commands/' in path:
                return 200, {}, {'status': 'succeeded'}
            return 200, {}, {'reported_state': {'configuration': {'frequency_hz': 10}}}

        with patch.object(recovery, '_validate_owned_container', return_value='postgres-id'), \
             patch.object(recovery, '_psql_variables', side_effect=psql):
            result = fixture_module.verify_rest_recovery_reads(
                SimpleNamespace(http_request=http), 'endpoint', object(), postgres, target,
                'token', context)
        self.assertEqual(len(calls), 4)
        self.assertTrue(all(row[0] == 'postgres-id' and row[1] == target for row in calls))
        self.assertEqual(calls[0][2], {'key': 'key-1',
            'urn': 'urn:ngsi-ld:Sensor:gw-a:p8-e5-command-a'})
        self.assertEqual(result['command_receipts'], 4)

    def test_http_diagnostics_are_allowlisted_and_never_copy_response_values(self):
        body = {'code': 'not_member', 'private_id': 'sensitive-id',
                'message': 'contains a private detail'}
        diagnostic = fixture_module._safe_http_diagnostic(403, body)
        self.assertEqual(diagnostic['status_code'], 403)
        self.assertEqual(diagnostic['error_code'], 'not_member')
        self.assertEqual(diagnostic['body_fields_present'], ['code', 'message'])
        self.assertNotIn('sensitive-id', json.dumps(diagnostic))
        self.assertNotIn('private detail', json.dumps(diagnostic))

    def test_database_url_transition_changes_only_database_component(self):
        original = 'postgres://iot_backend_app:opaque-password@postgres:5432/smoke?sslmode=disable'
        target = recovery._target_database('db-change')
        changed = fixture_module.database_url_transition(original, target)
        old_parts = __import__('urllib.parse').parse.urlsplit(original)
        new_parts = __import__('urllib.parse').parse.urlsplit(changed)
        self.assertEqual(new_parts.path, '/' + target)
        self.assertEqual((old_parts.scheme, old_parts.netloc, old_parts.query),
                         (new_parts.scheme, new_parts.netloc, new_parts.query))
        with self.assertRaisesRegex(ValueError, 'recovery_database_url_invalid'):
            fixture_module.database_url_transition(original.replace('postgres:5432', 'other:5432'), target)

    def test_restore_replacement_stops_old_before_creating_one_backend(self):
        with tempfile.TemporaryDirectory() as td:
            private = Path(td) / 'private'
            private.mkdir(mode=0o700)
            target = recovery._target_database('ordered')
            old_url = 'postgres://iot_backend_app:pw@postgres:5432/smoke?sslmode=disable'
            new_url = fixture_module.database_url_transition(old_url, target)
            fixture = SimpleNamespace(project='ncs_order', private=private,
                values={'DATABASE_URL': old_url}, env_file=private / 'fixture.env')
            fixture.env_file.write_text('DATABASE_URL=' + old_url + '\nOTHER=value\n')
            old = {'image': 'image-id', 'labels': {'role': 'backend'}, 'expectation': {},
                'receipt': {'id': 'old-id'}}
            old_obj = {'Id': 'old-id', 'Image': 'image-id',
                'Config': {'Env': ['DATABASE_URL=' + old_url]}, 'Mounts': [], 'HostConfig': {}}
            new_obj = {'Id': 'b' * 64, 'Image': 'image-id',
                'Config': {'Env': ['DATABASE_URL=' + new_url]}, 'Mounts': [], 'HostConfig': {},
                'State': {'Status': 'running'}}
            replacement_item = {'name': 'replacement', 'image': 'image-id', 'labels': {},
                'receipt': None}
            class Registry:
                def __init__(self): self.calls = [] ; self.new_resolves = 0
                def resolve(self, item):
                    if item is old: return old_obj
                    self.new_resolves += 1
                    return None if self.new_resolves == 1 else new_obj
                def command(self, args):
                    self.calls.append(('command', args))
                    return SimpleNamespace(returncode=0, stdout=(b'b' * 12 + b'\n' if args[0] == 'ps' else b''), stderr=b'')
                def remove(self, item): self.calls.append(('remove', item['receipt']['id']))
                def intent(self, *args): self.calls.append(('intent', args)); return replacement_item
                def save(self, item): pass
                def allocating(self, item): self.calls.append(('allocating', item['name']))
            registry = Registry()
            smoke = SimpleNamespace(private_write=lambda path, text: Path(path).write_text(text),
                compose_command=lambda *args: ['docker', 'compose', '-f', 'override', 'config'],
                process_env=lambda fixture: {}, BUDGETS={'up': 60},
                startup_state=lambda service, state: state.get('Status') == 'running')
            containers = {'backend': old, 'postgres': {'receipt': {'id': 'pg'}},
                'nginx': {'receipt': {'id': 'nginx'}}}
            with patch.object(fixture_module.subprocess, 'run', return_value=SimpleNamespace(
                    returncode=0, stdout=b'', stderr=b'')), \
                 patch.object(recovery, '_psql', side_effect=(target, '24')):
                result = fixture_module.replace_backend_database(smoke, fixture, registry,
                    containers, SimpleNamespace(checkpoint=lambda: None), target)
            calls = registry.calls
            self.assertLess(next(i for i, row in enumerate(calls) if row[0] == 'command' and row[1][0] == 'stop'),
                            next(i for i, row in enumerate(calls) if row[0] == 'remove'))
            self.assertEqual(result['active_backend_count'], 1)
            active_call = next(row[1] for row in calls if row[0] == 'command' and row[1][0] == 'ps')
            self.assertIn('label=io.iot.native-smoke.run=' + fixture.project, active_call)
            self.assertEqual(result['active_database'], target)
            self.assertEqual(fixture.values['DATABASE_URL'], new_url)
            self.assertEqual(fixture.env_file.read_text().splitlines()[1], 'OTHER=value')

    def test_composed_normal_path_calls_e5_restart_backup_restore_switch_and_api(self):
        with tempfile.TemporaryDirectory() as td:
            private = Path(td) / 'private'
            private.mkdir(mode=0o700)
            sqlite_path = private / 'aux.sqlite'
            with sqlite3.connect(sqlite_path) as conn:
                conn.execute('CREATE TABLE outbox(status TEXT)')
                conn.execute("INSERT INTO outbox VALUES ('pending')")
            fixture = SimpleNamespace(private=private, project='ncs_recovery',
                paper_p8_e5_context='/private/context')
            secret = 'private-fixture-jwt'
            context = {'tokens': {'admin': 'admin-token', 'a': secret},
                'context': {'gateway_ids': ['gw-a', 'gw-b'], 'sensor_ids': ['s1'],
                    'command_keys': ['key-1', 'key-2', 'key-3', 'key-4'],
                    'anchor': '2026-10-29T00:00:00Z'},
                'sqlite_paths': [], 'recovery_aux_env_file': '/private/aux.env',
                'recovery_aux_db_path': str(sqlite_path), 'prepared': {'manifests': {
                    'viewer': '/private/viewer.json', 'warm_replay': '/private/replay.json'}},
                'root': private}
            baseline = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP',
                'processes_stopped': True, 'runner_stopped': True,
                'source_fingerprint': {'revision': 'r', 'dirty_source_sha256': 'd'},
                'image_ids': {'backend': 'backend-image'},
                'paperload_binary_sha256': 'p', 'gateway_binary_sha256': 'g',
                'control_reconciliation': {'verified_receipts': 4}}
            def http_request(endpoint, method, path, token=None):
                if token is None:
                    return 401, {}, {}
                if 'gw-b' in path:
                    return 403, {}, {}
                if '/temporal?' in path:
                    return 200, {}, {'measurement': {'values': list(range(100))}}
                return 200, {}, {}
            smoke = SimpleNamespace(
                paper_p8_e5_support=lambda: SimpleNamespace(
                    run=lambda *a, **kw: (kw['recovery_context_out'].update(context) or baseline),
                    _safe_e5_viewer=lambda row: row),
                durable_json=lambda path, body: Path(path).write_text(json.dumps(body)),
                ROOT=Path(td), process_env=lambda fixture: {},
                http_request=http_request,
                paper_p0_temporal_query=lambda first, last: 'timeAt=' + first + '&endTimeAt=' + last)
            before_metrics = {'process_instance': 'old'}
            after_metrics = {'process_instance': 'new'}
            metric_calls = iter((before_metrics, after_metrics))
            snapshot = {'database': 'smoke', 'tables': {name: {'count': 1, 'hash': 'a' * 32}
                for name in recovery.SNAPSHOT_TABLES}}
            class Proc:
                returncode = 0
                def wait(self, timeout=None): return 0
                def poll(self): return 0
            def popen(argv, **kwargs):
                output = Path(argv[argv.index('--output-dir') + 1])
                output.mkdir(mode=0o700)
                (output / 'workload.json').write_text(json.dumps({'status': 'COMPLETE'}))
                return Proc()
            def aux_run(*args, ignore_ack, **kwargs):
                if not ignore_ack:
                    with sqlite3.connect(sqlite_path) as conn:
                        conn.execute("UPDATE outbox SET status='acked'")
                    return {'returncode': 0, 'summary': {'completed': 'true', 'pending_rows': '0',
                        'enqueued': '1', 'app_acknowledged': '2'}}
                return {'returncode': 0, 'summary': {'enqueued': '1', 'ignored_valid_acks': '1',
                    'pending_rows': '1', 'intentional_fault_stop': 'true'}}
            pending = {'payload': b'payload', 'payload_sha256': 'e' * 64, 'payload_bytes': 7,
                'message_id': '00000000-0000-4000-8000-000000000001',
                'gateway_id': 'gw-a', 'sensor_id': 'aux', 'first_sequence': 1}
            viewer_result = {'status': 'COMPLETE', 'connected_distinct_viewers': 2,
                'subscribed_distinct_viewers': 2, 'unique_events': 0}
            target = recovery._target_database(fixture.project)
            with patch.object(e4._support, '_admin_metrics', side_effect=lambda *a: next(metric_calls)), \
                 patch.object(fixture_module.cold, 'replace_owned_backend', return_value={'replacement': {'new_id': 'b'}}), \
                 patch.object(recovery, 'snapshot_database', side_effect=(snapshot, snapshot, snapshot, dict(snapshot, database=target))), \
                 patch.object(recovery, 'backup_database', return_value={'status': 'backed_up',
                     'size_bytes': 10, 'sha256': 'c' * 64}), \
                 patch.object(recovery, 'restore_database', return_value={'status': 'restored',
                     'target_database': target, 'schema_version': 24, 'timescaledb_ready': True}), \
                 patch.object(recovery, 'assert_snapshot_equal', return_value=True), \
                  patch.object(recovery, '_validate_owned_container', return_value='postgres-id'), \
                  patch.object(recovery, '_psql', return_value='e' * 64), \
                  patch.object(recovery, '_psql_variables', return_value=json.dumps({
                      'count': 100, 'first': '2026-10-29T00:00:00.000000Z',
                      'last': '2026-10-29T00:00:00.990000Z'})), \
                 patch.object(fixture_module, 'replace_backend_database', return_value={'status': 'replaced',
                     'database_url_only_change': True, 'active_database': target}), \
                 patch.object(fixture_module, 'run_aux_gateway', side_effect=aux_run), \
                 patch.object(fixture_module, 'pending_payload_snapshot', return_value=pending), \
                 patch.object(fixture_module, 'verify_rest_recovery_reads', return_value={
                    'command_receipts': 4, 'command_details': 4, 'final_configurations': 2,
                    'command_statuses': ['succeeded'] * 4}), \
                 patch.object(e4, 'start_e4_viewer', return_value=Proc()), \
                 patch.object(e4, 'finish_e4_viewer', return_value=viewer_result), \
                 patch.object(fixture_module.subprocess, 'Popen', side_effect=popen):
                result = fixture_module.run(smoke, fixture, None, None,
                    {'postgres': {'receipt': {'id': 'postgres-id'}}}, {}, None, 'paperload', 'gateway')
            self.assertEqual(result['status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP', result)
            self.assertEqual(result['phases']['baseline']['e5_control_receipts'], 4)
            self.assertEqual(result['phases']['controlled_backend_restart']['new_process_instance'], 'new')
            self.assertTrue(result['phases']['database_backup_restore']['snapshot_equal'])
            self.assertEqual(result['phases']['restored_backend_switch']['active_database'], target)
            self.assertEqual(result['phases']['restored_backend_switch']['status'], 'PASS')
            self.assertEqual(result['phases']['api_restored_readback']['command_receipts'], 4)
            self.assertEqual(result['api_diagnostics']['owner_temporal']['response_count'], 100)
            self.assertEqual(result['api_diagnostics']['temporal_batch_count'], 100)
            self.assertTrue(result['all_children_stopped'])
            artifact = json.loads((private / 'p8-recovery-result.json').read_text())
            self.assertNotIn(secret, json.dumps(artifact))
            self.assertFalse(artifact['full_p8_accepted'])

    def test_temporal_query_uses_observed_batch_bounds_not_wall_clock(self):
        seen = []
        smoke = SimpleNamespace(paper_p0_temporal_query=lambda first, last:
            seen.append((first, last)) or 'bounded')
        self.assertEqual(smoke.paper_p0_temporal_query('2026-10-29T00:00:00Z',
            '2026-10-29T00:00:00.990000Z'), 'bounded')
        self.assertEqual(seen, [('2026-10-29T00:00:00Z', '2026-10-29T00:00:00.990000Z')])

    def test_late_failure_still_derives_children_stopped_from_joined_process_handles(self):
        class RunningChild:
            stopped = False
            def poll(self): return 0 if self.stopped else None
            def terminate(self): self.stopped = True
            def wait(self, timeout=None):
                if not self.stopped: raise AssertionError('wait without stop')
                return 0
        proc = RunningChild()
        report = {'status': 'FAIL', 'primary_error': {'code': 'late_api_mismatch'},
                  'cleanup_errors': []}
        fixture_module._join_child_processes(
            [{'name': 'background_replay', 'process': proc, 'joined': False}], report)
        self.assertEqual(report['status'], 'FAIL')
        self.assertTrue(report['all_children_stopped'])
        self.assertEqual(report['children'], [{'name': 'background_replay', 'joined': True}])

    def test_partial_failure_preserves_safe_code_and_never_claims_acceptance(self):
        with tempfile.TemporaryDirectory() as td:
            private = Path(td) / 'private'
            private.mkdir(mode=0o700)
            fixture = SimpleNamespace(private=private, project='ncs_failure',
                paper_p8_e5_context='/private/context')
            smoke = SimpleNamespace(
                paper_p8_e5_support=lambda: SimpleNamespace(run=lambda *a, **kw:
                    (_ for _ in ()).throw(ValueError('recovery_e5_baseline_failed'))),
                durable_json=lambda path, body: Path(path).write_text(json.dumps(body)))
            result = fixture_module.run(smoke, fixture, None, None, {}, {}, None, 'p', 'g')
            self.assertEqual(result['status'], 'FAIL')
            self.assertEqual(result['primary_error']['code'], 'recovery_e5_baseline_failed')
            self.assertFalse(result['full_p8_accepted'])
            self.assertNotIn('private/context', (private / 'p8-recovery-result.json').read_text())

    def test_database_only_replacement_rejects_invalid_target_before_side_effects(self):
        with tempfile.TemporaryDirectory() as td:
            fixture = SimpleNamespace(values={'DATABASE_URL': 'postgres://x'},
                env_file=Path(td) / 'fixture.env', private=Path(td), project='ncs')
            with self.assertRaisesRegex(ValueError, 'recovery_target_database_invalid'):
                fixture_module.replace_backend_database(None, fixture, None, {}, None, 'smoke')
            self.assertFalse(fixture.env_file.exists())


if __name__ == '__main__':
    unittest.main()
