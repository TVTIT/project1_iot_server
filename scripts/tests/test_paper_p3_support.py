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

    def invoke(self, returncode=0, preserve_ram_dedup=False):
        with mock.patch.object(runner.subprocess, 'run', return_value=SimpleNamespace(
                returncode=returncode, stdout=b'', stderr=b'')) as command:
            runner.replace_backend_without_metrics(self.smoke, self.fixture, self.registry,
                                                   self.containers, self.state, preserve_ram_dedup)
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

    def test_metrics_off_replacement_keeps_cache_effective_flags_when_requested(self):
        self.obj['Config']['Env'] += ['MQTT_RAM_DEDUP_ENABLED=true',
                                      'MQTT_RAM_DEDUP_MAX_ENTRIES=100000',
                                      'MQTT_RAM_DEDUP_MAX_BYTES=67108864']
        self.invoke(preserve_ram_dedup=True)
        fragment = self.smoke.private_write.call_args_list[0].args[1]
        self.assertIn('METRICS_ENABLED: "false"', fragment)
        self.assertIn('MQTT_RAM_DEDUP_ENABLED: "true"', fragment)
        self.assertIn('MQTT_RAM_DEDUP_MAX_ENTRIES: "100000"', fragment)
        self.assertIn('MQTT_RAM_DEDUP_MAX_BYTES: "67108864"', fragment)

    def test_metrics_off_cache_readback_fails_closed_if_flag_was_lost(self):
        with self.assertRaisesRegex(ValueError, 'paper_p3_acceptance_failed'):
            self.invoke(preserve_ram_dedup=True)


class MicrobatchRegressionContractTests(unittest.TestCase):
    def test_acceptance_requires_confirmed_command_and_both_real_acknowledged_messages(self):
        accepted = {
            'microbatch_enabled': True,
            'command_result_confirmed': True,
            'operation_evidence': True,
            'traffic_source': runner.MICROBATCH_REGRESSION_TRAFFIC_LABEL,
            'messages': [
                {'kind': 'old', 'persisted': True, 'acknowledged': True, 'state_preserved': True},
                {'kind': 'new', 'persisted': True, 'acknowledged': True, 'state_preserved': True},
            ],
        }
        self.assertTrue(runner.microbatch_regression_accepted(accepted))
        for mutation in (
                {'microbatch_enabled': False},
                {'command_result_confirmed': False},
                {'messages': accepted['messages'][:1]},
                {'messages': [accepted['messages'][0], dict(accepted['messages'][1], acknowledged=False)]},
                {'messages': [accepted['messages'][0], dict(accepted['messages'][1], state_preserved=False)]}):
            candidate = dict(accepted, **mutation)
            self.assertFalse(runner.microbatch_regression_accepted(candidate))

    def test_traffic_label_is_not_misrepresented_as_gateway_generation(self):
        label = runner.MICROBATCH_REGRESSION_TRAFFIC_LABEL
        self.assertEqual(label, 'fixture_mqtt_publish_not_gateway_generation')


if __name__ == '__main__':
    unittest.main()
