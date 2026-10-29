import tempfile
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts.tests import native_compose_smoke as smoke


class P8E4SelectorTests(unittest.TestCase):
    def test_cli_selector_rejects_conflicting_probe_without_running_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run([sys.executable, str(Path(smoke.__file__),), '--prepare',
                str(Path(tmp) / 'fixture'), '--paper-p8-e4', 'on', '--paper-p8-e1'],
                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn('select only one P8 probe lane', result.stderr)

    def test_on_off_profiles_keep_fixed_microbatch_and_fail_closed_switches(self):
        fixtures = []
        for mode in ('on', 'off'):
            with tempfile.TemporaryDirectory() as tmp:
                fixture = smoke.prepare(Path(tmp) / 'fixture', paper_p8_e4=mode)
                fixtures.append(dict(fixture.values))
                self.assertTrue(fixture.paper_p8_load)
                self.assertTrue(fixture.paper_p2)
                self.assertEqual(fixture.paper_p8_e4_cache_mode, mode)
        on, off = fixtures
        self.assertEqual(on['MQTT_RAM_DEDUP_ENABLED'], 'true')
        self.assertEqual(off['MQTT_RAM_DEDUP_ENABLED'], 'false')
        for key, value in {'STAGE5_ENABLED': 'false', 'STAGE5_WAKEUP_ENABLED': 'false',
                           'WEBSOCKET_ENABLED': 'true', 'MQTT_SHAPER_ENABLED': 'false',
                           'MQTT_MICROBATCH_ENABLED': 'true', 'MQTT_MICROBATCH_MAX_MESSAGES': '32',
                           'MQTT_MICROBATCH_MAX_ROWS': '500', 'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES': '4194304',
                           'MQTT_MICROBATCH_MAX_WAIT': '50ms'}.items():
            self.assertEqual(on[key], value)
            self.assertEqual(off[key], value)
        profile_keys = ('STAGE5_ENABLED', 'STAGE5_WAKEUP_ENABLED', 'WEBSOCKET_ENABLED',
            'MQTT_SHAPER_ENABLED', 'MQTT_MICROBATCH_ENABLED', 'MQTT_MICROBATCH_MAX_MESSAGES',
            'MQTT_MICROBATCH_MAX_ROWS', 'MQTT_MICROBATCH_MAX_PAYLOAD_BYTES', 'MQTT_MICROBATCH_MAX_WAIT')
        self.assertEqual({k: on[k] for k in profile_keys}, {k: off[k] for k in profile_keys})

    def test_mode_conflicts_and_invalid_cache_mode_rejected(self):
        for kwargs in ({'paper_p8_e4': 'on', 'paper_p8_e1': True},
                       {'paper_p8_e4': 'off', 'paper_p8_e3': 'polling'},
                       {'paper_p8_e4': 'on', 'paper_p8_load': True},
                       {'paper_p8_e4': 'invalid'}):
            with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
                smoke.prepare(Path(tmp) / 'fixture', **kwargs)

    def test_dispatch_passes_mode_and_builds_with_e3_gateway_builder(self):
        fixture = SimpleNamespace(paper_p8_e4='off', private=Path('/private'))
        helper = Mock()
        with patch.object(smoke, 'paper_p8_e4_support', return_value=helper):
            result = smoke.dispatch_paper_p8_load(fixture, 'smoke', 'endpoint', 'registry',
                'containers', 'images', 'state', 'paperload', 'gateway')
        helper.run.assert_called_once_with('smoke', fixture, 'endpoint', 'registry', 'containers',
                                           'images', 'state', 'paperload', 'gateway', 'off')
        self.assertEqual(result, helper.run.return_value)

    def test_cleanup_requires_validation_raw_database_and_stop_then_exact_cleanup(self):
        fixture = SimpleNamespace(paper_p8_e4='on', paper_p8_e4_cache_mode='on')
        evidence = {'status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'lane_marker': 'PAPER_P8_E4_PILOT_COMPLETE',
            'full_p8_accepted': False, 'new_ws_event_proof': {'status': 'PASS'},
            'coldrestart': 'PASS', 'conflicts': {'status': 'PASS'},
            'durable_gateway_reopen': {'status': 'PASS'},
            'validation': {'valid': True, 'failures': []},
            'runner_stopped': True, 'database_delta': {'ledger': 20, 'raw': 2000, 'temporal': 20},
            'raw_verification': [{'verified_exact': True}, {'verified_exact': True}],
            'workloads': {'unique': {'telemetry': {'generated_unique': 20, 'application_acks': 20,
                'ignored_valid_acks': 1, 'pending_messages': 0}}, 'replay': {'telemetry': {
                'replay_offered': 10, 'replay_application_acks': 10}}}}
        registry = SimpleNamespace(items=[])
        with patch.object(smoke, 'paper_p4_cleanup_acceptance', return_value=True):
            self.assertTrue(smoke.paper_p8_e4_cleanup_acceptance(fixture, evidence, {}, True, registry))
            evidence['new_ws_event_proof'] = 'NOT RUN'
            self.assertFalse(smoke.paper_p8_e4_cleanup_acceptance(fixture, evidence, {}, True, registry))
            evidence['new_ws_event_proof'] = {'status': 'PASS'}
            evidence['coldrestart'] = 'NOT RUN'
            self.assertFalse(smoke.paper_p8_e4_cleanup_acceptance(fixture, evidence, {}, True, registry))
            evidence['coldrestart'] = 'PASS'
            for key in ('runner_stopped', 'validation', 'raw_verification'):
                original = evidence[key]
                evidence[key] = False if key == 'runner_stopped' else {'valid': False, 'failures': ['bad']} if key == 'validation' else []
                self.assertFalse(smoke.paper_p8_e4_cleanup_acceptance(fixture, evidence, {}, True, registry))
                evidence[key] = original
            self.assertFalse(smoke.paper_p8_e4_cleanup_acceptance(fixture, evidence, {}, False, registry))


if __name__ == '__main__':
    unittest.main()
