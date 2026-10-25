"""Guards for the supplementary metrics-off slice; no deployment resources."""
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import paper_p3_support as runner


class MetricsOffReplacementTests(unittest.TestCase):
    def setUp(self):
        self.fixture = SimpleNamespace(project='ncs_fixture', private=Path('/unused/private'))
        self.old = {'image': 'sha256:fixture', 'labels': {'test.run': 'ncs_fixture'},
                    'expectation': {'network': 'ncs_fixture_iot_net'}, 'receipt': {'id': 'owned-backend'}}
        self.item = {}
        self.containers = {'backend': self.old, 'nginx': {'receipt': {'id': 'owned-nginx'}}}
        self.registry = mock.Mock()
        self.registry.intent.return_value = self.item
        self.obj = {'Id': 'new-backend', 'State': {'Running': True}, 'Config': {'Env': ['METRICS_ENABLED=false']}}
        self.registry.resolve.side_effect = [None, self.obj]
        self.registry.command.return_value = SimpleNamespace(returncode=0)
        self.smoke = mock.Mock()
        self.smoke.BUDGETS = {'up': 30}
        self.smoke.compose_command.return_value = ['docker', 'compose', '-p', 'ncs_fixture', 'config', '--format', 'json']
        self.smoke.process_env.return_value = {'PATH': '/usr/bin'}
        self.smoke.startup_state.return_value = True
        self.state = mock.Mock()

    def invoke(self, returncode=0):
        with mock.patch.object(runner.subprocess, 'run', return_value=SimpleNamespace(
                returncode=returncode, stdout=b'', stderr=b'')) as command:
            runner.replace_backend_without_metrics(self.smoke, self.fixture, self.registry,
                                                   self.containers, self.state)
            return command

    def test_removes_exact_old_backend_before_allocating_replacement(self):
        command = self.invoke()
        names = [call[0] for call in self.registry.mock_calls]
        self.assertLess(names.index('command'), names.index('remove'))
        self.assertLess(names.index('remove'), names.index('allocating'))
        self.registry.remove.assert_called_once_with(self.old)
        self.registry.intent.assert_called_once_with('container', 'ncs_fixture-backend-metrics-off',
                                                     self.old['image'], self.old['labels'])
        self.assertIs(self.containers['backend'], self.item)
        self.assertIs(self.item['expectation'], self.old['expectation'])
        fragment = self.smoke.private_write.call_args_list[0].args[1]
        self.assertIn('METRICS_ENABLED: "false"', fragment)
        self.assertEqual(command.call_args.args[0][-7:],
                         ['up', '--detach', '--no-deps', '--no-build', '--pull', 'never', 'backend'])
        self.assertEqual(self.registry.command.call_args_list, [
            mock.call(['stop', '--time', '20', 'owned-backend']),
            mock.call(['exec', 'owned-nginx', 'nginx', '-s', 'reload'])])

    def test_collision_refused_before_compose_allocation(self):
        self.registry.resolve.side_effect = [self.obj]
        with self.assertRaisesRegex(ValueError, 'resource_collision'):
            self.invoke()
        self.registry.allocating.assert_not_called()

    def test_failed_create_does_not_reload_nginx(self):
        with self.assertRaisesRegex(ValueError, 'paper_p3_acceptance_failed'):
            self.invoke(returncode=1)
        self.registry.command.assert_called_once_with(['stop', '--time', '20', 'owned-backend'])
        self.assertIs(self.containers['backend'], self.item)

    def test_env_readback_must_confirm_metrics_disabled(self):
        self.obj['Config']['Env'] = ['METRICS_ENABLED=true']
        with self.assertRaisesRegex(ValueError, 'paper_p3_acceptance_failed'):
            self.invoke()
        self.registry.command.assert_called_once_with(['stop', '--time', '20', 'owned-backend'])

    def test_nginx_reload_failure_is_not_accepted(self):
        self.registry.command.side_effect = [SimpleNamespace(returncode=0), SimpleNamespace(returncode=1)]
        with self.assertRaisesRegex(ValueError, 'paper_p3_acceptance_failed'):
            self.invoke()

    def test_stop_failure_does_not_remove_old_backend(self):
        self.registry.command.return_value.returncode = 1
        with self.assertRaisesRegex(ValueError, 'paper_p3_acceptance_failed'):
            self.invoke()
        self.registry.remove.assert_not_called()
        self.registry.allocating.assert_not_called()


if __name__ == '__main__':
    unittest.main()
