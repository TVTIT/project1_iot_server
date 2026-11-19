import importlib.util
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


MODULE_PATH = Path(__file__).with_name('paper_p8_e2_diagnostics.py')
SPEC = importlib.util.spec_from_file_location('paper_p8_e2_diagnostics', MODULE_PATH)
diagnostics = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnostics)
SMOKE_PATH = MODULE_PATH.with_name('native_compose_smoke.py')
SMOKE_SPEC = importlib.util.spec_from_file_location('native_compose_smoke_for_e2_test', SMOKE_PATH)
native_smoke = importlib.util.module_from_spec(SMOKE_SPEC)
sys.modules[SMOKE_SPEC.name] = native_smoke
SMOKE_SPEC.loader.exec_module(native_smoke)


class FakeSmoke:
    def __init__(self, payload=None, error=None):
        self.payload = payload or {}
        self.error = error
        self.calls = []

    def db_psql(self, registry, cid, sql, variables=None, *, timeout=None):
        self.calls.append((registry, cid, sql, variables, timeout))
        if self.error:
            raise self.error
        return json.dumps(self.payload, separators=(',', ':')) + '\n'


def fake_registry():
    return SimpleNamespace(command=lambda *args, **kwargs: None)


def valid_payload():
    return {
        'server_version_num': 160004,
        'postmaster_start_time': '2026-11-07T10:00:00Z',
        'stats_reset': '2026-11-07T09:00:00Z',
        'settings': {'track_activities': True, 'track_counts': True,
                     'track_io_timing': False, 'track_wal_io_timing': False},
        'activity': [{'pid': 12, 'blocking_pids': [11], 'state': 'active',
                      'backend_type': 'client backend', 'wait_event_type': 'Lock',
                      'wait_event': 'transactionid', 'xact_age_seconds': 0.25,
                      'query_age_seconds': 0.1}],
        'database': {'numbackends': 2, 'xact_commit': 10, 'xact_rollback': 1,
                     'blks_read': 3, 'blks_hit': 7, 'tup_returned': 9,
                     'tup_fetched': 8, 'tup_inserted': 4, 'tup_updated': 2,
                     'tup_deleted': 1, 'conflicts': 0, 'deadlocks': 0,
                     'temp_files': 0, 'temp_bytes': 0, 'blk_read_time': None,
                     'blk_write_time': None},
        'wal': {'available': True, 'scope': 'cluster',
                'stats_reset': '2026-11-07T09:00:00Z', 'stats': {'wal_records': 50,
                 'wal_fpi': 2, 'wal_bytes': 4096, 'wal_buffers_full': 0,
                 'wal_write': 2, 'wal_sync': 1, 'wal_write_time': None, 'wal_sync_time': None}},
        'io': {'available': True, 'scope': 'cluster',
               'stats_reset': '2026-11-07T09:00:00Z', 'rows': [
            {'backend_type': 'client backend', 'object': 'relation', 'context': 'normal',
             'reads': 1, 'read_time': None, 'writes': 2, 'write_time': None,
             'writebacks': 0, 'writeback_time': None, 'extends': 0, 'extend_time': None,
             'hits': 10, 'evictions': 0, 'reuses': 0, 'fsyncs': 0, 'fsync_time': None}]},
    }


class DiagnosticsTests(unittest.TestCase):
    def test_valid_postgres16_returns_sanitized_snapshot_and_uses_owned_psql(self):
        smoke = FakeSmoke(valid_payload())
        registry = SimpleNamespace(command=lambda *args, **kwargs: None)
        result = diagnostics.read_postgres_diagnostics(smoke, registry, 'owned-pg')
        self.assertEqual(result['server_version_num'], 160004)
        self.assertEqual(result['activity'][0]['blocking_pids'], [11])
        self.assertEqual(result['timing']['track_io_timing'], False)
        self.assertEqual(result['io_timing'], {'available': False,
                                               'reason': 'track_io_timing_disabled'})
        self.assertEqual(result['wal']['scope'], 'cluster')
        self.assertEqual(result['io']['scope'], 'cluster')
        self.assertEqual(result['wal']['stats']['wal_write'], 2)
        self.assertEqual(result['wal']['stats']['wal_sync'], 1)
        self.assertEqual(result['wal']['stats_reset'], '2026-11-07T09:00:00Z')
        self.assertEqual(result['io']['stats_reset'], '2026-11-07T09:00:00Z')
        self.assertIn('not proof', result['contention_note'])
        self.assertIsNot(smoke.calls[0][0], registry)
        self.assertTrue(callable(smoke.calls[0][0].command))
        self.assertEqual(smoke.calls[0][1], 'owned-pg')
        self.assertLessEqual(smoke.calls[0][4], 2.0)
        self.assertNotIn('query text', smoke.calls[0][2].lower())
        self.assertIn('BEGIN READ ONLY', smoke.calls[0][2])
        self.assertIn('pg_stat_io', smoke.calls[0][2])

    def test_real_db_psql_command_adapter_suppresses_psql_tags(self):
        output = json.dumps(valid_payload(), separators=(',', ':')) + '\n'

        class Registry:
            def __init__(self):
                self.args = None

            def command(self, args, **kwargs):
                self.args = args
                stdout = (output if '-q' in args else
                          'BEGIN READ ONLY\nSET\nSET\n' + output + 'COMMIT\n')
                return SimpleNamespace(returncode=0, stdout=stdout.encode(), stderr=b'')

        registry = Registry()
        result = diagnostics.read_postgres_diagnostics(native_smoke, registry, 'fixture-pg')
        self.assertEqual(result['server_version_num'], 160004)
        self.assertIn('-q', registry.args)

    def test_payload_can_represent_unavailable_views_and_disabled_timing(self):
        payload = valid_payload()
        payload['settings']['track_io_timing'] = False
        payload['wal'] = {'available': False, 'reason': 'view_unsupported', 'scope': 'cluster'}
        payload['io'] = {'available': False, 'reason': 'view_unsupported', 'scope': 'cluster'}
        smoke = FakeSmoke(payload)
        result = diagnostics.read_postgres_diagnostics(smoke, fake_registry(), 'pg')
        self.assertFalse(result['wal']['available'])
        self.assertEqual(result['wal']['reason'], 'view_unsupported')
        self.assertIsNone(result['database']['blk_read_time'])

    def test_disabled_timing_forces_all_io_and_wal_times_null(self):
        payload = valid_payload()
        payload['wal']['stats']['wal_write_time'] = 999
        payload['wal']['stats']['wal_sync_time'] = 888
        payload['io']['rows'][0]['read_time'] = 777
        payload['io']['rows'][0]['write_time'] = 666
        result = diagnostics.read_postgres_diagnostics(FakeSmoke(payload), fake_registry(), 'pg')
        self.assertIsNone(result['wal']['stats']['wal_write_time'])
        self.assertIsNone(result['wal']['stats']['wal_sync_time'])
        self.assertIsNone(result['io']['rows'][0]['read_time'])
        self.assertIsNone(result['io']['rows'][0]['write_time'])
        self.assertIn('(pg_blocking_pids(pid))[1:16]', diagnostics._SQL)

    def test_diagnostics_select_closes_outer_json_object_before_commit(self):
        self.assertIn("FROM pg_stat_io)) END));\nCOMMIT;", diagnostics._SQL)

    def test_reader_rejects_versions_outside_fixture_pinned_postgresql16(self):
        payload = valid_payload() | {'server_version_num': 150000}
        with self.assertRaisesRegex(ValueError, 'diagnostics_payload_rejected'):
            diagnostics.read_postgres_diagnostics(FakeSmoke(payload), fake_registry(), 'pg')

    def test_rejects_malformed_nonfinite_and_boolean_numeric_values(self):
        for value in ('not json', json.dumps({'server_version_num': 160004}),
                      json.dumps(valid_payload() | {'server_version_num': True}),
                      json.dumps(valid_payload() | {'database': {'xact_commit': float('nan')}})):
            smoke = FakeSmoke()
            smoke.db_psql = lambda *args, v=value, **kwargs: v
            with self.subTest(value=value[:30]), self.assertRaises(ValueError):
                diagnostics.read_postgres_diagnostics(smoke, fake_registry(), 'pg')

    def test_bounds_activity_arrays_and_payload_bytes(self):
        payload = valid_payload()
        row = payload['activity'][0]
        payload['activity'] = [row | {'pid': i + 2, 'blocking_pids': list(range(30))}
                               for i in range(40)]
        result = diagnostics.read_postgres_diagnostics(FakeSmoke(payload), fake_registry(), 'pg')
        self.assertLessEqual(len(result['activity']), 32)
        self.assertLessEqual(len(result['activity'][0]['blocking_pids']), 16)
        smoke = FakeSmoke()
        smoke.db_psql = lambda *args, **kwargs: ' ' * 70000
        with self.assertRaisesRegex(ValueError, 'diagnostics_payload_rejected'):
            diagnostics.read_postgres_diagnostics(smoke, fake_registry(), 'pg')

    def test_timeout_and_db_errors_are_sanitized(self):
        smoke = FakeSmoke(error=TimeoutError('password=secret'))
        with self.assertRaisesRegex(ValueError, 'diagnostics_timeout') as raised:
            diagnostics.read_postgres_diagnostics(smoke, fake_registry(), 'pg', timeout=0.01)
        self.assertNotIn('secret', str(raised.exception))
        smoke = FakeSmoke(error=RuntimeError('postgres://secret'))
        with self.assertRaisesRegex(ValueError, 'diagnostics_query_failed') as raised:
            diagnostics.read_postgres_diagnostics(smoke, fake_registry(), 'pg')
        self.assertNotIn('secret', str(raised.exception))


if __name__ == '__main__':
    unittest.main()
