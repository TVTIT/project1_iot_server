"""Bounded, secret-safe owned PostgreSQL fixture startup regressions."""
import subprocess
import json
import unittest
from unittest.mock import patch

import task266_provision as fixture


class StartupTests(unittest.TestCase):
    def test_present_image_no_pull_and_distinct_start_budget(self):
        with patch.object(fixture.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as inspect, \
                patch.object(fixture, 'run') as run:
            fixture.prepare_database_image()
            fixture.start_database('owned', 'isolated', '/protected/database.env')
        self.assertEqual(inspect.call_args.args[0],
                         ['docker', 'image', 'inspect', fixture.PG_IMAGE])
        args = run.call_args
        self.assertIn('--pull=never', args.args[0])
        self.assertIn('--env-file', args.args[0])
        self.assertEqual(args.kwargs['timeout'], 60)

    def test_missing_image_exact_pull_budget(self):
        with patch.object(fixture.subprocess, 'run', return_value=subprocess.CompletedProcess(
                [], 1, stderr=b'error: no such image: ' + fixture.PG_IMAGE.encode())), \
                patch.object(fixture, 'run') as run:
            fixture.prepare_database_image()
        self.assertEqual(run.call_args.args[0], ['docker', 'pull', fixture.PG_IMAGE])
        self.assertEqual(run.call_args.kwargs['timeout'], 300)

    def test_safe_phase_errors(self):
        for phase, error, expected in (
            ('pull', subprocess.TimeoutExpired('password-secret', 300), 'image_pull_timeout'),
            ('pull', RuntimeError('password-secret'), 'image_pull_failed'),
            ('start', subprocess.TimeoutExpired('password-secret', 60), 'container_start_timeout'),
            ('start', RuntimeError('password-secret'), 'container_start_failed'),
        ):
            effects = [error]
            with self.subTest(phase=phase, expected=expected), \
                    patch.object(fixture.subprocess, 'run', return_value=subprocess.CompletedProcess(
                        [], 1, stderr=b'Error: No such image: ' + fixture.PG_IMAGE.encode())), \
                    patch.object(fixture, 'run', side_effect=effects):
                with self.assertRaisesRegex(RuntimeError, '^' + expected + '$') as caught:
                    if phase == 'pull':
                        fixture.prepare_database_image()
                    else:
                        fixture.start_database('owned', 'network', 'password-secret')
                self.assertTrue(caught.exception.__suppress_context__)

    def test_inspection_daemon_failure_never_pulls(self):
        with patch.object(fixture.subprocess, 'run', return_value=subprocess.CompletedProcess(
                [], 1, stderr=b'secret daemon failure')), patch.object(fixture, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, '^image_inspect_failed$'):
                fixture.prepare_database_image()
        run.assert_not_called()

    def test_inspection_timeout_is_safe_and_bounded(self):
        with patch.object(fixture.subprocess, 'run', side_effect=
                          subprocess.TimeoutExpired('secret', 10)) as inspect, \
                patch.object(fixture, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, '^image_inspect_timeout$'):
                fixture.prepare_database_image()
        self.assertEqual(inspect.call_args.kwargs['timeout'], 10)
        run.assert_not_called()

    def test_readiness_total_deadline_caps_every_subprocess_and_sleep(self):
        now = [0.0]
        bounds = []

        def probe(args, **kwargs):
            bounds.append(kwargs['timeout'])
            now[0] += kwargs['timeout']
            raise subprocess.TimeoutExpired('secret', kwargs['timeout'])

        with patch.object(fixture.time, 'monotonic', side_effect=lambda: now[0]), \
                patch.object(fixture.time, 'sleep', side_effect=lambda n: now.__setitem__(0, now[0] + n)), \
                patch.object(fixture.subprocess, 'run', side_effect=probe):
            with self.assertRaisesRegex(RuntimeError, '^database_not_ready$'):
                fixture.wait_database('owned', timeout=7)
        self.assertEqual(now[0], 7)
        self.assertTrue(all(0 < n <= 2 for n in bounds))

    def test_tcp_ready_and_terminal_state(self):
        with patch.object(fixture.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
            fixture.wait_database('owned')
        self.assertIn('127.0.0.1', run.call_args.args[0])
        self.assertEqual(run.call_args.kwargs['timeout'], 2)
        with patch.object(fixture.subprocess, 'run', side_effect=[
                subprocess.CompletedProcess([], 1),
                subprocess.CompletedProcess([], 0, b'exited\n')]):
            with self.assertRaisesRegex(RuntimeError, '^database_not_ready:exited$'):
                fixture.wait_database('owned')

    def test_partial_start_timeout_still_removes_owned_container(self):
        with patch.object(fixture, 'run', side_effect=subprocess.TimeoutExpired('secret', 60)), \
                patch.object(fixture.subprocess, 'run', side_effect=[
                    subprocess.CompletedProcess([], 0),
                    subprocess.CompletedProcess([], 1, stderr=b'error: no such object: owned')]) as run:
            try:
                fixture.start_database('owned', 'network', 'protected')
            except RuntimeError:
                fixture.cleanup_container('owned')
        self.assertEqual(run.call_args_list[0].args[0], ['docker', 'rm', '-f', '-v', 'owned'])

    def test_cleanup_daemon_errors_fail_closed(self):
        with patch.object(fixture.subprocess, 'run', return_value=
                          subprocess.CompletedProcess([], 1, stderr=b'secret daemon error')):
            with self.assertRaisesRegex(RuntimeError, 'owned container cleanup failed'):
                fixture.cleanup_container('owned')

    def test_main_finally_cleans_all_names_after_ambiguous_start_timeout(self):
        from unittest.mock import MagicMock
        spike = MagicMock()
        spike.compose = ['compose', 'fixture']
        spike.command.return_value = b'broker-id'
        inspected = [{'Mounts': [], 'NetworkSettings': {'Networks': {'isolated': {}}}}]
        with patch.object(fixture, 'Spike', return_value=spike), \
                patch.object(fixture, 'run', return_value=json.dumps(inspected).encode()), \
                patch.object(fixture, 'prepare_database_image'), \
                patch.object(fixture, 'start_database', side_effect=RuntimeError('container_start_timeout')) as start, \
                patch.object(fixture, 'cleanup_container') as cleanup:
            # prepare() normally creates this certificate; only chmod needs it.
            with patch.object(fixture.Path, 'chmod'):
                with self.assertRaisesRegex(RuntimeError, '^container_start_timeout$'):
                    fixture.main()
        database = start.call_args.args[0]
        self.assertIn(database, [call.args[0] for call in cleanup.call_args_list])
        self.assertEqual(cleanup.call_count, 4)
        spike.cleanup.assert_called_once()


if __name__ == '__main__':
    unittest.main()
