import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import paper_p8_recovery_support as recovery


class Result:
    def __init__(self, stdout='', returncode=0):
        self.stdout = stdout.encode()
        self.returncode = returncode
        self.stderr = b''


class Registry:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.calls = []

    def resolve(self, item):
        return {'Name': '/' + item['name']} if item.get('receipt') else None

    def command(self, args, **kwargs):
        self.calls.append((args, kwargs))
        response = self.responses.pop(0) if self.responses else Result()
        return response if isinstance(response, Result) else Result(response)


class FakeProc:
    def __init__(self, args, **kwargs):
        self.args = args
        self.returncode = 0
        self.stdout = open(os.devnull, 'rb')
        self.stderr = open(os.devnull, 'rb')
        self.kwargs = kwargs

    def communicate(self, input=None, timeout=None):
        self.input = input
        self.stdout.close()
        self.stderr.close()
        return b'', b''

    def kill(self):
        self.returncode = -9


class P8RecoveryPrimitiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'private'
        self.root.mkdir(mode=0o700)
        self.root.chmod(0o700)
        self.fixture = SimpleNamespace(private=self.root)
        self.item = {'kind': 'container', 'name': 'paper-postgres',
                     'receipt': {'id': 'a' * 64}}

    def tearDown(self):
        self.tmp.cleanup()

    def test_requires_current_registry_owned_container_receipt(self):
        reg = Registry()
        with self.assertRaisesRegex(ValueError, 'recovery_container_not_owned'):
            recovery.backup_database(reg, self.fixture, {'kind': 'container'}, self.root / 'x')
        item = dict(self.item, receipt=None)
        with self.assertRaisesRegex(ValueError, 'recovery_container_not_owned'):
            recovery.backup_database(reg, self.fixture, item, self.root / 'x')

    def test_rejects_private_directory_symlink_permissions_and_foreign_path(self):
        reg = Registry()
        archive = self.root / 'db.dump'
        archive.write_bytes(b'x')
        archive.chmod(0o600)
        outside = Path(self.tmp.name) / 'outside'
        outside.mkdir(mode=0o700)
        with self.assertRaisesRegex(ValueError, 'recovery_archive_path_invalid'):
            recovery.backup_database(reg, self.fixture, self.item, outside / 'db.dump')
        archive.unlink()
        archive.symlink_to(outside / 'secret')
        with self.assertRaisesRegex(ValueError, 'recovery_archive_path_invalid'):
            recovery.backup_database(reg, self.fixture, self.item, archive)
        archive.unlink()
        self.root.chmod(0o755)
        with self.assertRaisesRegex(ValueError, 'recovery_private_directory_invalid'):
            recovery.backup_database(reg, self.fixture, self.item, self.root / 'dump')

    def test_dump_uses_custom_format_owned_container_and_exclusive_0600_output(self):
        data = b'fixture archive bytes'
        reg = Registry()
        path = self.root / 'backup.dump'

        class StreamProc:
            def __init__(self, args, **kwargs):
                self.args = args
                self.returncode = 0
                self.stdout = mock.Mock()
                self.stdout.fileno.return_value = 100
                self.stderr = None
                self.calls = 0
                self.kwargs = kwargs

            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        proc = StreamProc(None)
        selector = mock.Mock()
        selector.select.side_effect = [[(None, None)], []]
        with mock.patch.object(recovery.os, 'read', side_effect=[data, b'']), \
             mock.patch.object(recovery.selectors, 'DefaultSelector', return_value=selector):
            result = recovery.backup_database(reg, self.fixture, self.item, path,
                                               popen=lambda args, **kw: (setattr(proc, 'args', args) or proc))
        self.assertEqual(result, {'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                                  'status': 'backed_up'})
        self.assertEqual(path.read_bytes(), data)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        diagnostic = self.root / 'backup.dump.pg_dump.stderr'
        self.assertTrue(diagnostic.is_file())
        self.assertEqual(diagnostic.stat().st_mode & 0o777, 0o600)
        self.assertIn('--format=custom', proc.args)
        self.assertIn('-U', proc.args)
        self.assertEqual(proc.args[proc.args.index('-U') + 1], 'postgres')
        self.assertIn('a' * 64, proc.args)
        self.assertEqual(reg.calls, [])

    def test_dump_nonzero_timeout_and_size_bounds_fail_closed(self):
        def run_case(*, select_results, reads=(), returncode=0, poll='done', timeout=5, max_bytes=None):
            proc = mock.Mock()
            proc.stdout = mock.Mock()
            proc.stdout.fileno.return_value = 123
            proc.stderr = None
            proc.poll.return_value = 0 if poll == 'done' else poll
            proc.wait.return_value = returncode
            selector = mock.Mock()
            selector.select.side_effect = select_results
            patches = [mock.patch.object(recovery.selectors, 'DefaultSelector', return_value=selector)]
            if reads:
                patches.append(mock.patch.object(recovery.os, 'read', side_effect=reads))
            if max_bytes is not None:
                patches.append(mock.patch.object(recovery, 'MAX_DUMP_BYTES', max_bytes))
            with patches[0]:
                context = patches[1:]
                for patcher in context: patcher.start()
                try:
                    archive = mock.Mock()
                    archive.name = str(Path(self.tmp.name) / 'mock.dump')
                    return recovery._write_bounded_dump(['pg_dump'], archive, timeout=timeout,
                                                        popen=lambda *a, **kw: proc)
                finally:
                    for patcher in reversed(context): patcher.stop()

        with self.assertRaisesRegex(ValueError, 'recovery_dump_failed'):
            run_case(select_results=[[]], returncode=7)
        with mock.patch.object(recovery, 'time') as clock:
            clock.monotonic.side_effect = [0, 2]
            with self.assertRaisesRegex(ValueError, 'recovery_dump_timeout'):
                run_case(select_results=[[]], poll=None, timeout=1)
        with self.assertRaisesRegex(ValueError, 'recovery_dump_too_large'):
            run_case(select_results=[[(None, None)]], reads=[b'12345'], max_bytes=4)
        path = self.root / 'large'
        path.write_bytes(b'x' * (recovery.MAX_DUMP_BYTES + 1))
        path.chmod(0o600)
        archive_reg = Registry([Result('t'), Result('f'), Result('smoke'), Result(''), Result('')])
        with self.assertRaisesRegex(ValueError, 'recovery_archive_invalid'):
            recovery.restore_database(archive_reg, self.fixture, self.item, path, 'run')

    def test_restore_refuses_existing_target_without_create_or_drop(self):
        responses = [Result('t'), Result('t')]
        reg = Registry(responses)
        path = self.root / 'dump'
        path.write_bytes(b'custom archive')
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'recovery_target_collision'):
            recovery.restore_database(reg, self.fixture, self.item, path, 'run-1')
        self.assertEqual(len(reg.calls), 2)
        self.assertTrue(all('DROP' not in args[-1] for args, _ in reg.calls))

    def test_restore_stops_on_pre_restore_and_pg_restore_errors(self):
        path = self.root / 'dump'
        path.write_bytes(b'archive')
        path.chmod(0o600)
        target = recovery._target_database('run-error')
        reg = Registry([Result('t'), Result('f'), Result('smoke'), Result(''),
                        Result('', returncode=1)])
        with self.assertRaisesRegex(ValueError, 'recovery_psql_failed'):
            recovery.restore_database(reg, self.fixture, self.item, path, 'run-error',
                                      popen=lambda *a, **kw: self.fail('restore must not start'))
        self.assertTrue(all('DROP' not in args[-1] for args, _ in reg.calls))

        reg = Registry([Result('t'), Result('f'), Result('smoke'), Result(''), Result(''), Result('')])

        def failed_restore(args, **kwargs):
            proc = FakeProc(args, **kwargs)
            proc.returncode = 2
            return proc

        with self.assertRaisesRegex(ValueError, 'recovery_restore_failed'):
            recovery.restore_database(reg, self.fixture, self.item, path, 'run-error',
                                      popen=failed_restore)
        self.assertIn('timescaledb_pre_restore', reg.calls[-1][0][-1])
        self.assertNotIn('timescaledb_post_restore', [args[-1] for args, _ in reg.calls])

    def test_restore_sequence_preserves_roles_and_verifies_schema_extension(self):
        target = recovery._target_database('run-1')
        reg = Registry([Result('t'), Result('f'), Result('smoke'), Result(''), Result(''),
                        Result(''), Result(target), Result('24'), Result('2.13')])
        path = self.root / 'dump'
        path.write_bytes(b'custom archive')
        path.chmod(0o600)
        procs = []

        def popen(args, **kwargs):
            proc = FakeProc(args, **kwargs)
            procs.append(proc)
            return proc

        result = recovery.restore_database(reg, self.fixture, self.item, path, 'run-1', popen=popen)
        self.assertEqual(result, {'status': 'restored', 'target_database': target,
                                  'schema_version': 24, 'timescaledb_ready': True})
        sql = [call[0][-1] for call in reg.calls]
        self.assertIn('CREATE DATABASE ' + target, sql[3])
        self.assertIn('timescaledb_pre_restore', sql[4])
        self.assertIn('timescaledb_post_restore', sql[5])
        self.assertIn('--exit-on-error', procs[0].args)
        self.assertIn('-U', procs[0].args)
        self.assertEqual(procs[0].args[procs[0].args.index('-U') + 1], 'postgres')
        self.assertNotIn('--no-owner', procs[0].args)
        self.assertEqual(procs[0].input, b'custom archive')
        self.assertFalse(any('DROP' in statement for statement in sql))
        diagnostic = self.root / 'dump.pg_restore.stderr'
        self.assertTrue(diagnostic.is_file())
        self.assertEqual(diagnostic.stat().st_mode & 0o777, 0o600)

    def test_snapshot_returns_only_counts_and_hashes_and_mismatch_is_deterministic(self):
        digest = 'a' * 32
        reg = Registry([Result('{"count":1,"hash":"' + digest + '"}') for _ in recovery.SNAPSHOT_TABLES])
        first = recovery.snapshot_database(reg, self.item)
        self.assertEqual(first['tables']['telemetry'], {'count': 1, 'hash': digest})
        self.assertNotIn('payload', str(first))
        queries = [args[-1] for args, _ in reg.calls]
        self.assertTrue(all('to_jsonb(t)' in q for q in queries))
        self.assertEqual(recovery._target_database('stable'), recovery._target_database('stable'))
        self.assertNotEqual(recovery._target_database('stable'), recovery._target_database('other'))
        self.assertTrue(recovery.assert_snapshot_equal(first, first))
        changed = {'database': 'restored', 'tables': dict(first['tables'])}
        changed['tables']['telemetry'] = {'count': 2, 'hash': 'b' * 32}
        with self.assertRaisesRegex(ValueError, 'recovery_snapshot_mismatch'):
            recovery.assert_snapshot_equal(first, changed)
        with self.assertRaisesRegex(ValueError, 'recovery_run_id_invalid'):
            recovery._target_database('../shared')

    def test_snapshot_equality_rejects_empty_incomplete_and_malformed_rows(self):
        for tables in ({}, {'telemetry': {'count': 0, 'hash': 'a' * 32}},
                       {table: {'count': 0, 'hash': ''} for table in recovery.SNAPSHOT_TABLES},
                       {table: {'count': -1, 'hash': 'a' * 32} for table in recovery.SNAPSHOT_TABLES}):
            with self.subTest(tables=len(tables)):
                with self.assertRaisesRegex(ValueError, 'recovery_snapshot_mismatch'):
                    recovery.assert_snapshot_equal({'tables': tables}, {'tables': tables})
        all_empty = {table: {'count': 0, 'hash': 'a' * 32} for table in recovery.SNAPSHOT_TABLES}
        with self.assertRaisesRegex(ValueError, 'recovery_snapshot_mismatch'):
            recovery.assert_snapshot_equal({'tables': all_empty}, {'tables': all_empty})

    def test_selector_setup_failure_kills_and_joins_dump_child(self):
        proc = mock.Mock()
        proc.stdout = mock.Mock()
        proc.poll.side_effect = [None, -9]
        proc.wait.return_value = -9
        archive = mock.Mock()
        archive.name = str(Path(self.tmp.name) / 'selector.dump')
        with mock.patch.object(recovery.selectors, 'DefaultSelector', side_effect=OSError('selector')):
            with self.assertRaises(OSError):
                recovery._write_bounded_dump(['pg_dump'], archive, timeout=5,
                    popen=lambda *a, **kw: proc)
        proc.kill.assert_called_once()
        proc.wait.assert_called()

    def test_dump_write_failure_kills_and_joins_its_child(self):
        proc = mock.Mock()
        proc.stdout = mock.Mock()
        proc.stdout.fileno.return_value = 124
        proc.poll.side_effect = [None, -9]
        proc.wait.return_value = -9
        selector = mock.Mock()
        selector.select.return_value = [(None, None)]
        archive = mock.Mock()
        archive.name = str(Path(self.tmp.name) / 'disk.dump')
        archive.write.side_effect = OSError('disk full')
        with mock.patch.object(recovery.selectors, 'DefaultSelector', return_value=selector), \
                mock.patch.object(recovery.os, 'read', return_value=b'payload'):
            with self.assertRaises(OSError):
                recovery._write_bounded_dump(['pg_dump'], archive, timeout=5,
                                              popen=lambda *a, **kw: proc)
        proc.kill.assert_called_once()
        proc.wait.assert_called()


if __name__ == '__main__':
    unittest.main()
