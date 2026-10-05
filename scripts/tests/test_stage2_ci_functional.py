import json
import os
from pathlib import Path
import subprocess
import signal
import time
import tempfile
import unittest
from unittest import mock
import stage2_ci as ci


class Functional(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp/opencode')
        self.base = Path(self.tmp.name)
        self.run_id = '12345678' + 'a' * 24
        self.owner = {'run_id': self.run_id, 'directory': str(self.base / 'fixture'),
                      'prefix': 'e2e_12345678', 'project': None}
        self.manifest = self.base / 'owner.json'
        self.manifest.write_text(json.dumps(self.owner))
        (self.base / 'identity').write_text(self.run_id)

    def tearDown(self):
        self.tmp.cleanup()

    def test_interrupted_owner_update_preserves_valid_previous_manifest(self):
        import stage2_e2e as harness
        stack = harness.IsolatedE2EStack.__new__(harness.IsolatedE2EStack)
        stack.d = self.base / 'fixture'
        stack.ci_run_id = self.run_id
        stack.prefix = self.owner['prefix']
        stack.spike = mock.Mock(project='dynsec_spike_' + 'b' * 12)
        previous = self.manifest.read_bytes()
        with mock.patch.dict(os.environ, STAGE2_CI_OWNER=str(self.manifest)), \
             mock.patch.object(harness.os, 'fsync', side_effect=InterruptedError), \
             mock.patch.object(ci, 'docker') as docker:
            with self.assertRaises(InterruptedError):
                stack.write_ci_owner()
            docker.assert_not_called()
        self.assertEqual(self.manifest.read_bytes(), previous)
        self.assertEqual(ci.validate_owner(self.manifest), self.owner)
        self.assertEqual(list(self.base.glob('.owner-*.tmp')), [])
        # Interrupted update cannot authorize the new project's cleanup.
        self.assertIsNone(ci.validate_owner(self.manifest)['project'])

    def test_owner_update_atomic_protected_success(self):
        import stage2_e2e as harness
        stack = harness.IsolatedE2EStack.__new__(harness.IsolatedE2EStack)
        stack.d = self.base / 'fixture'
        stack.ci_run_id = self.run_id
        stack.prefix = self.owner['prefix']
        stack.spike = mock.Mock(project='dynsec_spike_' + 'b' * 12)
        with mock.patch.dict(os.environ, STAGE2_CI_OWNER=str(self.manifest)):
            stack.write_ci_owner()
        self.assertEqual(ci.validate_owner(self.manifest)['project'], stack.spike.project)
        self.assertEqual(self.manifest.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(self.base.glob('.owner-*.tmp')), [])

    def test_not_run_retained_in_report_and_unknown_rejected(self):
        report = ci.evidence('SCENARIO E22: NOT RUN transport unavailable\n', 0)
        self.assertEqual(report['scenarios']['E22'], 'NOT RUN')
        self.assertFalse(report['complete'])
        report = ci.evidence('SCENARIO E22: PASSING arbitrary-secret\n', 0)
        self.assertNotIn('E22', report['scenarios'])
        self.assertNotIn('arbitrary-secret', str(report))
        self.assertFalse(report['complete'])

    def test_foreign_symlink_invalid_and_missing_never_delete(self):
        for change in [{'directory': '/tmp/opencode/stage2-e2e-foreign'},
                       {'run_id': 'b' * 32}, {'project': 'foreign'}]:
            self.manifest.write_text(json.dumps(dict(self.owner, **change)))
            with mock.patch.object(ci.subprocess, 'run') as docker:
                with self.assertRaises(Exception):
                    ci.fallback(self.manifest)
                docker.assert_not_called()
        self.manifest.unlink()
        with mock.patch.object(ci.subprocess, 'run') as docker:
            with self.assertRaises(Exception):
                ci.fallback(self.manifest)
            docker.assert_not_called()
        self.manifest.symlink_to(self.base / 'identity')
        with mock.patch.object(ci.subprocess, 'run') as docker:
            with self.assertRaises(Exception):
                ci.fallback(self.manifest)
            docker.assert_not_called()

    def test_daemon_error_never_deletes(self):
        with mock.patch.object(ci.subprocess, 'run', side_effect=OSError) as docker:
            with self.assertRaises(Exception):
                ci.fallback(self.manifest)
            self.assertEqual(docker.call_count, 1)

    def test_wrong_label_substring_project_never_deletes(self):
        for name, labels in [('foreign_e2e_12345678_pg', {ci.LABEL: self.run_id}),
                             ('e2e_12345678_pg', {ci.LABEL: 'b' * 32}),
                             ('e2e_12345678_pg', {ci.LABEL: self.run_id,
                                                   'com.docker.compose.project': 'foreign'})]:
            calls = []
            def docker(args, **kw):
                calls.append(args)
                output = json.dumps([{'Id': 'c' * 64, 'Name': '/' + name,
                                      'Config': {'Labels': labels}}]) if 'inspect' in args else 'c' * 64
                return subprocess.CompletedProcess(args, 0, output, '')
            with mock.patch.object(ci.subprocess, 'run', side_effect=docker):
                with self.assertRaises(Exception):
                    ci.fallback(self.manifest)
            self.assertFalse(any('rm' in c or 'run' in c for c in calls))

    def test_exact_inspected_ids_deleted_only_after_all_validations(self):
        calls = []
        deleted = False
        def docker(args):
            nonlocal deleted
            calls.append(args)
            if args[:2] == ['ps', '-aq'] and not deleted:
                return 'c' * 64
            if args[:2] == ['container', 'inspect']:
                return json.dumps([{'Id': 'c' * 64, 'Name': '/e2e_12345678_pg',
                                    'Config': {'Labels': {ci.LABEL: self.run_id}}}])
            if args[0] == 'rm':
                deleted = True
            return ''
        with mock.patch.object(ci, 'docker', side_effect=docker):
            ci.fallback(self.manifest)
        removal = calls.index(['rm', '-f', '-v', 'c' * 64])
        self.assertTrue(any(c[:2] == ['network', 'ls'] for c in calls[:removal]))
        self.assertNotIn('--filter', calls[removal])

    def test_invalid_json_before_docker(self):
        self.manifest.write_text('not json')
        with mock.patch.object(ci, 'docker') as docker:
            with self.assertRaises(Exception):
                ci.fallback(self.manifest)
            docker.assert_not_called()

    def test_executable_nonzero_early_failure_metadata(self):
        for child, expected in [(['python3', '-c', 'raise SystemExit(23)'], 23),
                                (['/no/such/child'], 1)]:
            base = self.base / str(expected)
            with mock.patch.object(ci, 'metadata', return_value={'head': 'unavailable'}):
                self.assertEqual(ci.supervise(base, child, timeout=2, grace=.2), expected)
            report = json.loads((base / 'safe/report.json').read_text())
            self.assertFalse(report['complete'])
            self.assertEqual((base / 'safe/report.json').stat().st_mode & 0o777, 0o600)

    def test_timeout_reaps_child(self):
        self.assertEqual(ci.supervise(self.base / 'timeout',
                         ['python3', '-c', 'import time; time.sleep(60)'], timeout=.1, grace=.1), 124)

    def test_git_and_image_failures_are_safe(self):
        with mock.patch.object(ci.subprocess, 'run', side_effect=subprocess.TimeoutExpired('secret', 1)):
            result = ci.metadata()
        self.assertNotIn('secret', str(result))
        self.assertEqual(result['head'], 'unavailable')

    def test_selector_counterexample(self):
        text = '\n'.join(f'SCENARIO E{i:02}: PASS' for i in range(1, 28))
        text += '\nSTAGE 2 E2E EXECUTION SUMMARY\nPost-cleanup known-secret scan passed'
        self.assertFalse(ci.complete(text, 0))

    def test_selector_anomalies(self):
        text = '\n'.join(f'SCENARIO E{i:02}: PASS' for i in range(1, 28))
        text += '\nSTAGE 2 E2E EXECUTION SUMMARY\nPost-cleanup known-secret scan passed\n'
        selectors = '\n'.join(f"Selector '{s}': PASS" for s in ci.SELECTORS)
        self.assertTrue(ci.complete(text + selectors, 0))
        for invalid in [selectors.replace("Selector 'accounts/auth': PASS", ''),
                        selectors + "\nSelector 'accounts/auth': PASS",
                        selectors + "\nSelector 'foreign': PASS",
                        selectors.replace('PASS', 'NOT RUN', 1)]:
            self.assertFalse(ci.complete(text + invalid, 0))

    def test_signal_terminates_group(self):
        base = self.base / 'signal'
        # Executable supervisor receives SIGTERM while waiting on a real child.
        script = ("import sys; sys.path.insert(0, 'scripts/tests'); import stage2_ci as c; "
                  "from pathlib import Path; sys.exit(c.supervise(Path(sys.argv[1]), "
                  "['python3','-c','import time; time.sleep(60)'], timeout=5, grace=.2))")
        proc = subprocess.Popen(['python3', '-c', script, str(base)], cwd=ci.ROOT,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 3
        while not (base / 'private-console.log').exists() and time.monotonic() < deadline:
            time.sleep(.01)
        time.sleep(.1)
        proc.send_signal(signal.SIGTERM)
        self.assertEqual(proc.wait(timeout=10), 143)
        self.assertEqual(json.loads((base / 'safe/report.json').read_text())['harness_exit'], 143)

    def test_kill_escalation_and_reap(self):
        child = mock.Mock(pid=999999, wait=mock.Mock(side_effect=[
            subprocess.TimeoutExpired('child', .1), subprocess.TimeoutExpired('child', .1), 0]))
        with mock.patch.object(ci.subprocess, 'Popen', return_value=child), \
             mock.patch.object(ci.os, 'killpg') as kill, \
             mock.patch.object(ci, 'fallback'), mock.patch.object(ci, 'metadata', return_value={}):
            self.assertEqual(ci.supervise(self.base / 'escalation', timeout=.1, grace=.1), 124)
        self.assertEqual(kill.call_args_list, [mock.call(999999, signal.SIGTERM),
                                               mock.call(999999, signal.SIGKILL)])
        self.assertEqual(child.wait.call_count, 3)

    def test_cleanup_failure_changes_success_exit_and_retains_manifest(self):
        child = mock.Mock(wait=mock.Mock(return_value=0))
        with mock.patch.object(ci.subprocess, 'Popen', return_value=child), \
             mock.patch.object(ci, 'complete', return_value=True), \
             mock.patch.object(ci, 'fallback', side_effect=OSError), \
             mock.patch.object(ci, 'metadata', side_effect=ImportError('secret')):
            self.assertEqual(ci.supervise(self.base / 'cleanup-fail'), 1)
        report = json.loads((self.base / 'cleanup-fail/safe/report.json').read_text())
        self.assertFalse(report['cleanup_verified'])
        self.assertNotIn('secret', str(report))
        self.assertTrue((self.base / 'cleanup-fail/owner.json').exists())

    def test_uid_utility_mount_only_verified_fixture(self):
        fixture = self.base / 'fixture'
        fixture.mkdir()
        calls = []
        def docker(args):
            calls.append(args)
            if args[0] == 'run':
                fixture.rmdir()
                fixture.mkdir()  # emulate clearing contents, leaving mountpoint
            return ''
        with mock.patch.object(ci, 'docker', side_effect=docker), \
             mock.patch.object(ci.shutil, 'rmtree'):
            ci.fallback(self.manifest)
        utility = [args for args in calls if args[0] == 'run'][0]
        self.assertIn(f'type=bind,src={fixture},dst=/owned', utility)
        self.assertIn('none', utility)
        self.assertNotIn('--privileged', utility)
        self.assertFalse(fixture.exists())

    def test_fixture_symlink_rejected_before_docker(self):
        foreign = self.base / 'foreign'
        foreign.mkdir()
        (self.base / 'fixture').symlink_to(foreign, target_is_directory=True)
        with mock.patch.object(ci, 'docker') as docker:
            with self.assertRaises(Exception):
                ci.fallback(self.manifest)
            docker.assert_not_called()
        self.assertTrue(foreign.exists())

    def test_report_filesystem_failure_nonzero_no_artifact(self):
        with mock.patch.object(ci, 'publish', side_effect=PermissionError), \
             mock.patch.object(ci, 'fallback'), mock.patch.object(ci, 'metadata', return_value={}):
            self.assertEqual(ci.supervise(self.base / 'no-report', ['python3', '-c', 'pass']), 1)
        self.assertFalse((self.base / 'no-report/safe/report.json').exists())
