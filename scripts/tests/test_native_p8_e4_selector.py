import tempfile
import subprocess
import sys
import unittest
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts.tests import native_compose_smoke as smoke
from scripts import paper_p8_server


class P8E4SelectorTests(unittest.TestCase):
    def test_coldrestart_summary_requires_finalized_accepted_cold_report(self):
        self.assertEqual(smoke.paper_p8_e4_cold_restart_status({
            'status': 'FAIL', 'workload_status': 'COLD_REPLAY_SUBSET_VALIDATED',
            'lane_marker': 'PAPER_P8_E4_COLD_REPLAY_SUBSET', 'coldrestart': 'PASS'}, True), 'FAIL')
        self.assertEqual(smoke.paper_p8_e4_cold_restart_status({
            'status': 'PASS', 'workload_status': 'WARM_SUBSET_VALIDATED',
            'lane_marker': 'PAPER_P8_E4_WARM_SUBSET', 'coldrestart': 'NOT RUN'}, True), 'FAIL')

    def test_cold_preparation_metadata_has_all_phase_inputs_and_viewer_manifest(self):
        cold = smoke.paper_p8_e4_preparation_metadata({
            'unique': 'u', 'replay': 'r', 'cold-replay': 'c'})
        warm = smoke.paper_p8_e4_preparation_metadata({'unique': 'u', 'replay': 'r'})
        self.assertEqual(cold, {'phase_inputs': ['unique', 'replay', 'cold-replay'],
            'manifest_count': 4})
        self.assertEqual(warm['manifest_count'], 3)

    def test_server_rejects_cold_replay_without_cache_on_e4(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp) / 'server'
            with self.assertRaisesRegex(ValueError, '--cold-replay requires --experiment E4 --cache-mode on'):
                paper_p8_server.main(['serve', '--root', str(root), '--network-config',
                    str(Path(tmp) / 'unused-profile.json'), '--experiment', 'E4',
                    '--cache-mode', 'off', '--cold-replay'])
            self.assertTrue(root.is_dir())

    def test_e4_prepare_report_is_not_run_and_never_claims_workload(self):
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); artifacts = root / 'artifacts'; artifacts.mkdir(mode=0o700)
            private = root / 'private'; private.mkdir(mode=0o700)
            (private / 'session.json').write_text(json.dumps({'experiment': 'E4'}))
            (artifacts / 'p8-e4-on.json').write_text(json.dumps({
                'status': 'PREPARATION_ONLY', 'workload_status': 'NOT_RUN',
                'coldrestart': 'NOT RUN', 'durable_gateway_reopen': 'NOT RUN',
                'conflicts': 'NOT RUN', 'full_p8_accepted': False}))
            fixture = SimpleNamespace(private=private, artifacts=artifacts, project='ncs_0123456789ab',
                                      paper_p8_e4='on', paper_p8_e4_cache_mode='on')
            report = smoke.paper_p8_prepare_only_report(fixture, remote_outcome='NOT_RUN',
                cleanup_verified=True, cleanup_counts={'container': 1})
            self.assertEqual(report['status'], 'PAPER_P8_E4_PREPARATION_ONLY')
            self.assertEqual(report['workload_outcome'], 'NOT_RUN')
            self.assertFalse(report['p8_e4_workload_verified'])
            self.assertFalse(report['full_p8_accepted'])
            self.assertEqual(smoke.paper_p8_prepare_only_run_status('E4', failed=False,
                cleanup_verified=True, remote_outcome='NOT_RUN'), 'E4_PREPARATION_ONLY_CLEANUP')

    def test_e4_server_allows_unique_begin_but_rejects_finish_without_active_phase(self):
        state = {'experiment': 'E4', 'session_id': 'a' * 32, 'phase': 'READY',
                 'preparation_status': 'E4_PREPARED', 'e4_phase_plan': ['unique', 'replay']}
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); (root / 'fixture/private').mkdir(parents=True, mode=0o700)
            with patch.object(paper_p8_server, '_state_root', return_value=root / 'fixture/private'), \
                 patch.object(paper_p8_server.supervisor, 'read_state', return_value=state), \
                 patch.object(paper_p8_server.supervisor, 'write_request') as publish:
                paper_p8_server.main(['begin', '--root', str(root), '--experiment', 'E4',
                    '--session-id', 'a' * 32, '--phase', 'READY', '--phase-id', 'unique'])
                self.assertEqual(publish.call_args.args[1]['phase_id'], 'unique')
                publish.reset_mock()
                with self.assertRaisesRegex(ValueError, 'e4_finish_phase_transition_invalid'):
                    paper_p8_server.main(['finish-phase', '--root', str(root), '--experiment', 'E4',
                        '--session-id', 'a' * 32, '--phase', 'READY', '--phase-id', 'unique',
                        '--results', '/not-used'])
                publish.assert_not_called()

    def test_e4_server_next_phase_requires_a_validated_unique_import(self):
        state = {'experiment': 'E4', 'session_id': 'b' * 32, 'phase': 'VERIFYING',
            'phase_id': 'unique', 'workload_outcome': 'VALIDATED',
            'phase_history': ['unique'],
            'imported_result': {'phase_id': 'unique', 'validation_valid': True},
            'e4_phase_plan': ['unique', 'replay']}
        with tempfile.TemporaryDirectory(dir='/var/tmp/opencode') as tmp:
            root = Path(tmp); private = root / 'fixture/private'; private.mkdir(parents=True, mode=0o700)
            with patch.object(paper_p8_server, '_state_root', return_value=private), \
                 patch.object(paper_p8_server.supervisor, 'read_state', return_value=state), \
                 patch.object(paper_p8_server.supervisor, 'write_request') as publish:
                paper_p8_server.main(['next-phase', '--root', str(root), '--experiment', 'E4',
                    '--session-id', 'b' * 32, '--phase', 'VERIFYING', '--phase-id', 'replay'])
                self.assertEqual(publish.call_args.args[1]['command'], 'next-phase')
                publish.reset_mock()
                state['workload_outcome'] = 'VALIDATION_FAILED'
                with self.assertRaisesRegex(ValueError, 'e4_next_phase_requires_validated_unique'):
                    paper_p8_server.main(['next-phase', '--root', str(root), '--experiment', 'E4',
                        '--session-id', 'b' * 32, '--phase', 'VERIFYING', '--phase-id', 'replay'])
                publish.assert_not_called()

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
