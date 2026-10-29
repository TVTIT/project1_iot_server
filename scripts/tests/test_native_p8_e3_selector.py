import tempfile
import json
import unittest
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts.tests import native_compose_smoke as smoke


class P8E3SelectorTests(unittest.TestCase):
    def test_low_load_cli_requires_e3_and_rejects_other_lane(self):
        script = Path(smoke.__file__)
        with tempfile.TemporaryDirectory() as tmp:
            base = [sys.executable, str(script), '--prepare', str(Path(tmp) / 'fixture')]
            missing = subprocess.run(base + ['--paper-p8-e3-low-load'], capture_output=True, text=True)
            self.assertEqual(missing.returncode, 2)
            self.assertIn('--paper-p8-e3-low-load requires --paper-p8-e3', missing.stderr)
            conflict = subprocess.run(base + ['--paper-p8-e3', 'polling', '--paper-p8-e3-low-load', '--paper-p8-e1'],
                                      capture_output=True, text=True)
            self.assertEqual(conflict.returncode, 2)
            self.assertIn('P8 probe selectors are isolated opt-in fixture lanes', conflict.stderr)

    def test_arms_differ_only_by_wakeup_and_reuse_p2_load_profile(self):
        prepared = []
        for arm in ('polling', 'wakeup'):
            with tempfile.TemporaryDirectory() as tmp:
                fixture = smoke.prepare(Path(tmp) / 'fixture', paper_p8_e3=arm)
                prepared.append((fixture, dict(fixture.values)))
                self.assertTrue(fixture.paper_p8_load)
                self.assertTrue(fixture.paper_p2)
                self.assertEqual(fixture.paper_p8_e3, arm)
        for key in ('WEBSOCKET_ENABLED', 'STAGE5_ENABLED', 'MQTT_SHAPER_ENABLED',
                    'MQTT_MICROBATCH_ENABLED', 'MQTT_RAM_DEDUP_ENABLED',
                    'MQTT_SHAPER_RATE_PER_SECOND', 'MQTT_SHAPER_BURST',
                    'MQTT_MICROBATCH_MAX_MESSAGES', 'MQTT_MICROBATCH_MAX_ROWS',
                    'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT'):
            self.assertEqual(prepared[0][1][key], prepared[1][1][key])
        self.assertNotEqual(prepared[0][1]['STAGE5_WAKEUP_ENABLED'],
                            prepared[1][1]['STAGE5_WAKEUP_ENABLED'])
        values = prepared[0][1]
        self.assertEqual(values['STAGE5_ENABLED'], 'true')
        self.assertEqual(values['WEBSOCKET_ENABLED'], 'false')
        self.assertEqual(values['MQTT_SHAPER_RATE_PER_SECOND'], '2')
        self.assertEqual(values['MQTT_SHAPER_BURST'], '4')
        self.assertEqual(values['MQTT_MICROBATCH_MAX_MESSAGES'], '32')
        self.assertEqual(values['MQTT_MICROBATCH_MAX_ROWS'], '500')
        self.assertEqual(values['MQTT_MICROBATCH_MAX_PAYLOAD_BYTES'], '4194304')
        self.assertEqual(values['MQTT_MICROBATCH_MAX_WAIT'], '50ms')

    def test_mode_conflicts_and_invalid_arm_rejected(self):
        for kwargs in ({'paper_p8_e3': 'polling', 'paper_p8_e2': True},
                       {'paper_p8_e3': 'polling', 'paper_p8_load': True},
            {'paper_p8_e3': 'invalid'}):
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
                smoke.prepare(Path(tmp) / 'fixture', **kwargs)
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            smoke.prepare(Path(tmp) / 'fixture', paper_p8_e3_workload='low_load')
        with tempfile.TemporaryDirectory() as tmp:
            fixture = smoke.prepare(Path(tmp) / 'fixture', paper_p8_e3='polling', paper_p8_e3_workload='low_load')
            self.assertEqual(fixture.paper_p8_e3_workload, 'low_load')

    def test_gateway_build_is_private_bounded_and_uses_fixture_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            private = Path(tmp) / 'private'
            (private / 'bin').mkdir(parents=True)
            fixture = SimpleNamespace(private=private)
            def build(argv, **kwargs):
                self.assertEqual(argv[-1], './cmd/gateway')
                self.assertEqual(argv[argv.index('-o') + 1], str(private / 'bin/gateway'))
                self.assertEqual(kwargs['cwd'], smoke.ROOT / 'src')
                self.assertEqual(kwargs['env'], {'TEST': '1'})
                self.assertGreater(kwargs['timeout'], 0)
                Path(argv[argv.index('-o') + 1]).write_bytes(b'gateway')
                return SimpleNamespace(returncode=0)
            with patch.object(smoke, 'process_env', return_value={'TEST': '1'}), \
                 patch.object(smoke.subprocess, 'run', side_effect=build):
                binary = smoke.build_paper_p8_e3_gateway(fixture, '/go')
            self.assertEqual(binary.read_bytes(), b'gateway')

    def test_dispatch_passes_arm_and_gateway_binary(self):
        fixture = SimpleNamespace(paper_p8_e3='wakeup', private=Path('/private'))
        support = Mock()
        support.run.return_value = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP'}
        with patch.object(smoke, 'paper_p8_e3_support', return_value=support):
            result = smoke.dispatch_paper_p8_load(fixture, None, None, None, None, None,
                                                  None, '/paperload', '/gateway')
        support.run.assert_called_once_with(None, fixture, None, None, None, None, None,
                                            '/paperload', '/gateway', 'wakeup', 'mixed')
        self.assertEqual(result['status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')

    def test_cleanup_rejects_missing_or_partial_validation(self):
        fixture = SimpleNamespace()
        registry = SimpleNamespace(items=[])
        self.assertFalse(smoke.paper_p8_e3_cleanup_acceptance(
            fixture, {}, {}, True, registry))
        evidence = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'full_p8_accepted': False,
                    'validation': {'valid': False, 'failures': ['missing_receipt']},
                    'representative_probes_pending': 'pending'}
        self.assertFalse(smoke.paper_p8_e3_cleanup_acceptance(
            fixture, evidence, {}, True, registry))

    def test_cleanup_requires_actual_representative_probe_results(self):
        fixture = SimpleNamespace(paper_p8_e3='polling', paper_p8_e3_workload='low_load')
        registry = SimpleNamespace(items=[])
        evidence = {
            'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'arm': 'polling', 'workload': 'low_load',
            'full_p8_accepted': False, 'validation': {'valid': True, 'failures': []},
            'runner_stopped': True, 'accounting': {'control': {'planned': 4, 'sent': 4, 'pending': 0}},
            'control_reconciliation': {
                'receipt_verification': {'verified': True, 'verified_idempotency_keys': [1, 2, 3, 4]},
                'rest_configuration': {'a': {}, 'b': {}}, 'sqlite_executors': [{}, {}]},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'representative_probes': {
                'valid': True, 'failures': [],
                'operations': [
                    {'kind': kind, 'expected': status, 'actual': status, 'match': True}
                    for kind, status in [('offline_admission', 202), ('same_key_replay', 202),
                                         ('busy', 409), ('stale', 409), ('noop', 200),
                                         ('noop_replay', 200)]],
                'auxiliary_command_count': 1, 'receipt_count': 2,
                'outside_fresh_command_measurement': True},
        }
        with patch.object(smoke, 'paper_p4_cleanup_acceptance', return_value=True):
            self.assertTrue(smoke.paper_p8_e3_cleanup_acceptance(fixture, evidence, {}, True, registry))
            evidence['representative_probes_pending'] = 'pending'
            self.assertTrue(smoke.paper_p8_e3_cleanup_acceptance(fixture, evidence, {}, True, registry))
            del evidence['representative_probes']
            self.assertFalse(smoke.paper_p8_e3_cleanup_acceptance(fixture, evidence, {}, True, registry))
            evidence['representative_probes'] = {'valid': False, 'failures': ['probe_failed']}
            self.assertFalse(smoke.paper_p8_e3_cleanup_acceptance(fixture, evidence, {}, True, registry))

    def test_low_load_finalizer_and_report_share_artifact_filename(self):
        fixture = SimpleNamespace(paper_p8_e3='polling', paper_p8_e3_workload='low_load')
        name = smoke.paper_p8_e3_artifact_name(fixture)
        self.assertEqual(name, 'p8-e3-polling-low-load.json')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / name
            path.write_text('{"status":"WORKLOAD_COMPLETE_PENDING_CLEANUP"}')
            finalizer_path = Path(directory) / smoke.paper_p8_e3_artifact_name(fixture)
            report_path = Path(directory) / smoke.paper_p8_e3_artifact_name(fixture)
            self.assertEqual(finalizer_path, report_path)
            self.assertEqual(json.loads(report_path.read_text())['status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')


if __name__ == '__main__':
    unittest.main()
