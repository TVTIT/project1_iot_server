import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.tests import native_compose_smoke as smoke
from scripts import paper_p8_e5 as driver
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import paper_p8_e5_support as e5


class E5SelectorTests(unittest.TestCase):
    def test_recovery_selector_builds_fresh_private_optimized_profile_without_changing_defaults(self):
        with tempfile.TemporaryDirectory() as td:
            fixture = smoke.prepare(Path(td) / 'recovery', paper_p8_recovery=True)
            self.assertTrue(fixture.paper_p8_recovery)
            self.assertEqual(fixture.paper_p8_e5, 'optimized')
            self.assertTrue(fixture.paper_p8_e5_context)
            context = json.loads(Path(fixture.paper_p8_e5_context).read_text())
            self.assertEqual(context['seed'], 813)
            self.assertEqual(len(context['command_keys']), 4)
            self.assertEqual(fixture.values['WEBSOCKET_ENABLED'], 'true')
            self.assertEqual(fixture.values['STAGE5_ENABLED'], 'true')
        self.assertEqual(smoke.DEFAULTS['WEBSOCKET_ENABLED'], 'false')
        self.assertEqual(smoke.DEFAULTS['STAGE5_ENABLED'], 'false')

    def test_recovery_selector_rejects_external_mode_combinations(self):
        with tempfile.TemporaryDirectory() as td:
            for kwargs in ({'paper_p8_recovery': True, 'paper_p8_e5': 'optimized'},
                           {'paper_p8_recovery': True, 'paper_p8_load': True},
                           {'paper_p8_recovery': True, 'paper_p8_e4': 'on'},
                           {'paper_p8_recovery': True, 'paper_p2': True}):
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    smoke.prepare(Path(td) / str(len(kwargs)), **kwargs)

    def test_recovery_cli_rejects_competing_experiment_selector_before_prepare(self):
        with tempfile.TemporaryDirectory() as td:
            with patch.object(sys, 'argv', ['native_compose_smoke.py', '--prepare', str(Path(td) / 'new'),
                    '--paper-p8-recovery', '--paper-p8-e5', 'optimized', '--paper-p8-e5-context', '/private/context']):
                with self.assertRaises(SystemExit) as raised:
                    smoke.main()
            self.assertEqual(raised.exception.code, 2)
            self.assertFalse((Path(td) / 'new').exists())

    def test_reference_loader_rechecks_actual_source_before_image_reuse(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'reference.json'
            path.write_text(json.dumps({'status': 'PASS', 'arm': 'optimized',
                'cleanup_verified': True, 'full_p8_accepted': False,
                'source_fingerprint': {'revision': 'a' * 40, 'dirty_source_sha256': 'b' * 64}}))
            path.chmod(0o600)
            support = SimpleNamespace(_source_fingerprint=lambda root: {
                'revision': 'a' * 40, 'dirty_source_sha256': 'c' * 64})
            with patch.object(smoke, 'paper_p8_e5_acceptance', return_value=True), \
                    patch.object(smoke, 'paper_p8_support', return_value=support):
                with self.assertRaisesRegex(ValueError, 'e5_reference_source_mismatch'):
                    smoke._load_e5_reference(path, SimpleNamespace())

    def test_image_reuse_checks_id_and_refuses_tag_collision(self):
        from types import SimpleNamespace
        image_id = 'sha256:' + 'a' * 64
        class Registry:
            def __init__(self, collision=False): self.calls = []; self.collision = collision; self.tagged = False
            def command(self, argv):
                self.calls.append(argv)
                if argv[:2] == ['image', 'inspect']:
                    if argv[2] == image_id or (argv[2] == 'target' and (self.tagged or self.collision)):
                        found = ('sha256:' + 'b' * 64 if self.collision and argv[2] == 'target' else image_id)
                        return SimpleNamespace(returncode=0, stdout=json.dumps([{'Id': found}]))
                    return SimpleNamespace(returncode=1, stdout='')
                if argv[:2] == ['image', 'tag']: self.tagged = True
                return SimpleNamespace(returncode=0, stdout='')
        registry = Registry()
        self.assertEqual(smoke._reuse_e5_image(registry, {'backend': image_id}, 'backend', 'target'), image_id)
        self.assertIn(['image', 'tag', image_id, 'target'], registry.calls)
        with self.assertRaisesRegex(ValueError, 'target_tag_collision'):
            smoke._reuse_e5_image(Registry(collision=True), {'backend': image_id}, 'backend', 'target')
        with self.assertRaisesRegex(ValueError, 'image_unavailable'):
            smoke._reuse_e5_image(Registry(), {'backend': 'sha256:' + 'c' * 64}, 'backend', 'target')

    def test_arm_profiles_and_p2_topology_flags(self):
        profiles = {}
        for arm in e5.ARMS:
            with tempfile.TemporaryDirectory() as tmp:
                fixture = smoke.prepare(Path(tmp) / 'fixture', paper_p8_e5=arm,
                                        paper_p8_e5_context='/private/context.json')
                self.assertTrue(fixture.paper_p8_load)
                self.assertTrue(fixture.paper_p2)
                self.assertEqual(fixture.paper_p8_e5, arm)
                self.assertIn("WEBSOCKET_ENABLED: 'true'", (fixture.private / 'compose.p2.yml').read_text())
                profiles[arm] = fixture.values
                self.assertEqual(fixture.values['STAGE5_ENABLED'], 'true')
                self.assertEqual(fixture.values['WEBSOCKET_ENABLED'], 'true')
        for arm in e5.ARMS[1:]:
            self.assertTrue(e5.comparison.validate_mechanism_diff(
                {k: profiles['optimized'][k] for k in e5.comparison.MECHANISM_FLAGS},
                {k: profiles[arm][k] for k in e5.comparison.MECHANISM_FLAGS}, arm)['valid'])

    def test_conflicts_and_missing_context_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            for kwargs in ({'paper_p8_e5': 'optimized'},
                           {'paper_p8_e5_context': '/private/context'},
                           {'paper_p8_e5': 'optimized', 'paper_p8_e5_context': '/private/context',
                            'paper_p8_e4': 'on'},
                           {'paper_p8_e5': 'optimized', 'paper_p8_e5_context': '/private/context',
                            'paper_p8_load': True}):
                with self.assertRaises(ValueError):
                    smoke.prepare(Path(tmp) / ('fixture-' + str(len(kwargs))), **kwargs)

    def test_dispatch_includes_arm_context_and_gateway_binary(self):
        from types import SimpleNamespace
        fixture = SimpleNamespace(paper_p8_e5='optimized', paper_p8_e5_context='/ctx', private=Path('/private'))
        helper = type('Helper', (), {'run': lambda self, *args: args})()
        with patch.object(smoke, 'paper_p8_e5_support', return_value=helper):
            result = smoke.dispatch_paper_p8_load(fixture, 's', 'e', 'r', 'c', 'i', 'state', 'pl', 'gw')
        self.assertEqual(result[-3:], ('optimized', '/ctx', None))
        self.assertIsNone(result[-1])

    def test_reference_flag_is_ablation_only_and_forwarded(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, 'reference_rejected'):
                smoke.prepare(Path(tmp) / 'bad', paper_p8_e5='optimized',
                    paper_p8_e5_context='/ctx', paper_p8_e5_reference='/ref')
            fixture = smoke.prepare(Path(tmp) / 'good', paper_p8_e5='shaper_off',
                paper_p8_e5_context='/ctx', paper_p8_e5_reference='/ref')
            self.assertEqual(fixture.paper_p8_e5_reference, '/ref')

    def test_partial_workload_or_cleanup_cannot_be_accepted(self):
        report = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'arm': 'optimized',
            'lane_marker': 'PAPER_P8_E5_ARM_PILOT', 'full_p8_accepted': False,
            'validation': {'valid': True, 'failures': []}, 'processes_stopped': True,
            'runner_stopped': True, 'cleanup_errors': [],
            'phases': {'initial': {'e4': {'journal_entry_count': 20},
                                   'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20}},
                       'warm_replay': {'e4': {'journal_entry_count': 10}}},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'viewer': {'ready_servers': 2, 'subscriptions': 3},
            'control_reconciliation': {'status': 'PASS', 'verified_receipts': 4,
                                       'verified_configurations': 2},
            'accounting': {'imported_input': 20, 'warm_replay': 10,
                'command_receipts_verified': 4, 'command_configurations_verified': 2,
                'ingress_delta': {'available': True}}, 'mechanism_metrics': {'activation_status': 'AVAILABLE'},
            'metadata': {'workload': {'workload_type': 'test'}, 'effective_flags': {'STAGE5_ENABLED': 'true'}}}
        self.assertTrue(smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True))
        self.assertFalse(smoke.paper_p8_e5_acceptance(report, {}, False))
        report['workload_status'] = report['status']
        report['status'] = 'PASS'
        self.assertTrue(smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True))
        report['status'] = 'FAIL'
        self.assertFalse(smoke.paper_p8_e5_acceptance(report, {'arm': 'optimized'}, True))
        report['status'] = 'PASS'
        report['phases']['initial']['database_delta']['raw'] = 1999
        self.assertFalse(smoke.paper_p8_e5_acceptance(report, {}, True))

    def test_driver_prepare_only_creates_repeat_context_without_native_invocation(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / 'scratch'
            with patch.object(subprocess, 'run', side_effect=AssertionError('native must not run')):
                self.assertEqual(driver.main(['--root', str(root)]), 0)
            context_path = root / 'repeat-01' / 'context.json'
            context = json.loads(context_path.read_text())
            raw = Path(context['dataset_path']).read_bytes()
            self.assertEqual(len(raw.splitlines()), 20)
            self.assertEqual(context['bytes'], len(raw))
            self.assertEqual(context['sha256'], __import__('hashlib').sha256(raw).hexdigest())
            self.assertEqual(len(context['command_keys']), 4)
            self.assertEqual(len(context['viewer_subscriptions']), 3)
            self.assertNotIn('sensor_id', context['viewer_subscriptions'][1])
            self.assertEqual(os.stat(context_path).st_mode & 0o777, 0o600)
            self.assertNotEqual(context['anchor'], '2026-01-01T00:00:00Z')

    def test_e5_cli_preserves_private_evidence_after_success(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td) / 'fixture'
            completed = {'status': 'PAPER_P8_E5_ARM_PILOT', 'full_p8_accepted': False}
            with patch.object(smoke, 'startup_only', return_value=completed), \
                    patch.object(smoke.shutil, 'rmtree', side_effect=AssertionError('must retain E5 inputs')), \
                    patch.object(sys, 'argv', ['native_compose_smoke.py', '--prepare', str(base),
                                              '--paper-p8-e5', 'optimized',
                                              '--paper-p8-e5-context', '/private/context.json']):
                result = smoke.main()
            self.assertEqual(result, 0)
            self.assertTrue((base / 'private' / 'fixture.env').is_file())

    def test_context_reuse_validates_seed_and_preserves_existing_context(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / 'scratch'
            driver.main(['--root', str(root), '--seed', '805'])
            context = root / 'repeat-01' / 'context.json'
            before = context.read_bytes()
            reuse_root = Path(td) / 'reuse'
            self.assertEqual(driver.main(['--root', str(reuse_root), '--seed', '805',
                                          '--context', str(context)]), 0)
            self.assertEqual(context.read_bytes(), before)
            with self.assertRaisesRegex(ValueError, 'context_seed_mismatch'):
                driver.main(['--root', str(Path(td) / 'bad-seed'), '--seed', '806',
                             '--context', str(context)])


if __name__ == '__main__':
    unittest.main()
