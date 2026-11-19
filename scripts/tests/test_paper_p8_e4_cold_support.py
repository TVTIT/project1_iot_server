import importlib.util
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest import mock

SPEC = importlib.util.spec_from_file_location('e4cold', Path(__file__).with_name('paper_p8_e4_cold_support.py'))
cold = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(cold)


class E4ColdSupportTests(unittest.TestCase):
    def test_replacement_fragment_only_sets_owned_container_name(self):
        self.assertEqual(cold.replacement_fragment('fixture-backend-restarted'),
                         'services:\n  backend:\n    container_name: fixture-backend-restarted\n')
        with self.assertRaises(ValueError):
            cold.replacement_fragment('bad name')

    def test_replacement_readback_requires_same_settings_and_new_identity(self):
        old = {'Id': 'old'}
        new = {'Id': 'new', 'Image': 'sha256:image', 'Config': {'Env': ['A=1']},
               'Mounts': [{'Destination': '/data', 'Source': '/owned'}], 'HostConfig': {'NetworkMode': 'owned', 'Binds': ['/b:/b:ro', '/a:/a:rw']}}
        kwargs = {'expected_image': 'sha256:image', 'expected_environment': ['A=1'],
                  'expected_mounts': new['Mounts'], 'expected_host_config': {'NetworkMode': 'owned', 'Binds': ['/a:/a:rw', '/b:/b:ro']}}
        self.assertTrue(cold.verify_replacement(old, new, **kwargs)['environment_unchanged'])
        for field, value in (('Image', 'other'), ('HostConfig', {})):
            changed = dict(new); changed[field] = value
            with self.assertRaises(ValueError):
                cold.verify_replacement(old, changed, **kwargs)
        with self.assertRaises(ValueError):
            cold.verify_replacement(old, dict(new, Id='old'), **kwargs)

    def test_host_config_keeps_dns_order_and_normalizes_only_safe_lists(self):
        old = {'Id': 'old'}
        new = {'Id': 'new', 'Image': 'image', 'Config': {'Env': []}, 'Mounts': [],
               'HostConfig': {'Dns': ['1.1.1.1', '8.8.8.8'],
                   'Binds': ['/b:/b:ro', '/a:/a:rw'], 'CapAdd': ['NET_ADMIN', 'SYS_TIME'],
                   'SecurityOpt': ['no-new-privileges:true', 'seccomp=default']}}
        expected = {'Dns': ['8.8.8.8', '1.1.1.1'],
                    'Binds': ['/a:/a:rw', '/b:/b:ro'], 'CapAdd': ['SYS_TIME', 'NET_ADMIN'],
                    'SecurityOpt': ['no-new-privileges:true', 'seccomp=default']}
        args = dict(expected_image='image', expected_environment=[], expected_mounts=[],
                    expected_host_config=expected)
        with self.assertRaisesRegex(ValueError, 'host_config_changed'):
            cold.verify_replacement(old, new, **args)
        new['HostConfig']['Dns'] = expected['Dns']
        self.assertTrue(cold.verify_replacement(old, new, **args))
        self.assertNotEqual(cold.normalize_host_config({'Binds': ['/a:/dst:ro', '/b:/dst:rw']}),
                            cold.normalize_host_config({'Binds': ['/b:/dst:rw', '/a:/dst:ro']}))
        new['HostConfig']['SecurityOpt'] = ['no-new-privileges:false', 'seccomp=default']
        with self.assertRaisesRegex(ValueError, 'host_config_changed'):
            cold.verify_replacement(old, new, **args)
        new['HostConfig']['SecurityOpt'] = expected['SecurityOpt']
        new['HostConfig']['CpuQuota'] = 100000
        args['expected_host_config'] = {**expected, 'CpuQuota': 100000}
        cold.verify_replacement(old, new, **args)
        new['HostConfig']['CpuQuota'] = 200000
        with self.assertRaisesRegex(ValueError, 'host_config_changed'):
            cold.verify_replacement(old, new, **args)

    def test_cold_replay_uses_independent_process_window_and_db_work_counters(self):
        old = {'process_instance': 'old'}
        before = {'process_instance': 'new', 'ram_dedup': {'entries': 0, 'lookup_miss_total': 0,
                  'lookup_hit_total': 0}, 'persistence': {'begin_attempts': 2, 'confirmed_commits': 2,
                  'confirmed_new_rows': 2000, 'copy_calls': 2},
                  'ingress': {'Committed': 20, 'SamplesNew': 2000}}
        after = {'process_instance': 'new', 'ram_dedup': {'entries': 0, 'lookup_miss_total': 10,
                 'lookup_hit_total': 0}, 'persistence': {'begin_attempts': 12, 'confirmed_commits': 12,
                 'confirmed_new_rows': 2000, 'copy_calls': 2},
                 'ingress': {'Committed': 20, 'SamplesNew': 2000}}
        result = cold.cold_window_evidence(old, before, after, before_db='same', after_db='same',
                                           application_ack_count=10)
        self.assertEqual(result['status'], 'PASS', result['reasons'])
        self.assertEqual(result['persistence_delta']['confirmed_commits'], 10)
        after['process_instance'] = 'other'
        self.assertIn('cold_window_crossed_process_restart', cold.cold_window_evidence(
            old, before, after, before_db='same', after_db='same', application_ack_count=10)['reasons'])

    def test_conflict_needs_db_guard_rejection_and_no_ack(self):
        good = cold.conflict_no_ack_evidence('fingerprint', 'fingerprint',
            guard_precondition={'matched': True}, rejection_reason='sequence_overlap',
            expected_reason='sequence_overlap', ack_count=0)
        self.assertEqual(good['status'], 'PASS')
        false_absence = cold.conflict_no_ack_evidence('fingerprint', 'changed',
            guard_precondition={'matched': False}, rejection_reason=None,
            expected_reason='sequence_overlap', ack_count=0)
        self.assertEqual(false_absence['status'], 'FAIL')
        self.assertIn('conflict_database_guard_not_matched', false_absence['reasons'])
        self.assertIn('conflict_changed_database_state', false_absence['reasons'])
        self.assertEqual(cold.conflict_no_ack_evidence('same', 'same', guard_precondition={'matched': True},
            rejection_reason='sequence_overlap', expected_reason='sequence_overlap', ack_count=1)['status'], 'FAIL')

    def test_missing_counters_and_empty_fingerprints_fail_closed(self):
        before = {'process_instance': 'new', 'ram_dedup': {'entries': 0}, 'persistence': {}, 'ingress': {}}
        after = {'process_instance': 'new', 'ram_dedup': {'entries': 0}, 'persistence': {}, 'ingress': {}}
        result = cold.cold_window_evidence({'process_instance': 'old'}, before, after,
            before_db='', after_db='', application_ack_count=10)
        self.assertEqual(result['status'], 'FAIL')
        self.assertIsNone(result['cache_hit_delta'])
        self.assertIn('cold_replay_ingress_new_data', result['reasons'])
        self.assertIn('cold_replay_database_fingerprint_changed', result['reasons'])
        known_metrics = {'process_instance': 'new', 'ram_dedup': {'entries': 0,
            'lookup_miss_total': 1, 'lookup_hit_total': 0}, 'persistence': {
            'begin_attempts': 1, 'confirmed_commits': 1, 'confirmed_new_rows': 0, 'copy_calls': 0},
            'ingress': {'Committed': 0, 'SamplesNew': 0}}
        unknown_ack = cold.cold_window_evidence({'process_instance': 'old'}, known_metrics,
            known_metrics, before_db='db', after_db='db', application_ack_count=None)
        self.assertEqual(unknown_ack['status'], 'FAIL')
        self.assertIn('cold_replay_application_ack_count', unknown_ack['reasons'])

    def test_owned_replacement_changes_only_container_name_and_registers_before_up(self):
        fixture = SimpleNamespace(project='fixture', private=Path('/private'))
        old_obj = {'Id': 'oldid', 'Image': 'sha256:image', 'Config': {'Env': ['A=1']},
            'Mounts': [], 'HostConfig': {'NetworkMode': 'owned'}, 'State': {'Running': True}}
        new_obj = {**old_obj, 'Id': 'newid', 'Name': '/fixture-backend-e4-cold'}
        old = {'image': 'sha256:image', 'labels': {'test.run': 'fixture'},
            'expectation': {'network': 'owned'}, 'receipt': {'id': 'oldid'}}
        item = {}
        registry = mock.Mock()
        registry.resolve.side_effect = [old_obj, None, new_obj]
        registry.intent.return_value = item
        registry.command.return_value = SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
        smoke = mock.Mock(BUDGETS={'up': 30})
        smoke.compose_command.return_value = ['docker', 'compose', '-p', 'fixture', 'config', '--format', 'json']
        smoke.process_env.return_value = {}
        smoke.startup_state.return_value = True
        containers = {'backend': old, 'nginx': {'receipt': {'id': 'nginx'}}}
        with mock.patch.object(cold.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'', stderr=b'')) as up:
            cold.replace_owned_backend(smoke, fixture, registry, containers, mock.Mock())
        calls = [call[0] for call in registry.mock_calls]
        self.assertLess(calls.index('command'), calls.index('remove'))
        self.assertLess(calls.index('remove'), calls.index('allocating'))
        self.assertLess(calls.index('allocating'), calls.index('resolve', calls.index('allocating')))
        self.assertEqual(registry.command.call_args_list[0].args[0], ['stop', '--time', '20', 'oldid'])
        self.assertEqual(registry.command.call_args_list[1].args[0], ['exec', 'nginx', 'nginx', '-s', 'reload'])
        self.assertEqual(smoke.private_write.call_args_list[0].args[1],
            'services:\n  backend:\n    container_name: fixture-backend-e4-cold\n')
        self.assertEqual(up.call_args.args[0][-7:], ['up', '--detach', '--no-deps', '--no-build', '--pull', 'never', 'backend'])

    def test_replacement_readiness_timeout_is_failure(self):
        fixture = SimpleNamespace(project='fixture', private=Path('/private'))
        old_obj = {'Id': 'oldid', 'Image': 'sha256:image', 'Config': {'Env': []}, 'Mounts': [],
            'HostConfig': {}, 'State': {}}
        old = {'image': 'sha256:image', 'labels': {}, 'expectation': {}, 'receipt': {'id': 'oldid'}}
        registry = mock.Mock()
        registry.resolve.side_effect = [old_obj, None, {**old_obj, 'Id': 'newid'}]
        registry.intent.return_value = {}
        registry.command.return_value = SimpleNamespace(returncode=0, stdout=b'', stderr=b'')
        smoke = mock.Mock(BUDGETS={'up': 30}, startup_state=mock.Mock(return_value=False))
        smoke.compose_command.return_value = ['docker', 'compose', 'config', '--format', 'json']
        with mock.patch.object(cold.subprocess, 'run', return_value=SimpleNamespace(returncode=0, stdout=b'', stderr=b'')), \
             mock.patch.object(cold.time, 'monotonic', side_effect=[0, 46]), \
             mock.patch.object(cold.time, 'sleep'):
            with self.assertRaisesRegex(ValueError, 'backend_restart_failed'):
                cold.replace_owned_backend(smoke, fixture, registry,
                    {'backend': old, 'nginx': {'receipt': {'id': 'nginx'}}}, mock.Mock())


if __name__ == '__main__':
    unittest.main()
