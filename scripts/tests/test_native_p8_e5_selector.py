import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts.tests import native_compose_smoke as smoke
from scripts import paper_p8_e5 as driver
from scripts import paper_p8_server
import paper_p8_supervisor as supervisor
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

    def test_server_e5_cli_flag_forwarding_and_validation(self):
        with tempfile.TemporaryDirectory() as td:
            net_config = Path(td) / 'net.json'
            net_config.write_text(json.dumps({
                'schema_version': 1,
                'bind_ip': '192.168.50.20',
                'advertised_host': 'fixture-a.lan',
                'https_port': 14443,
                'mqtt_tls_port': 18884,
                'required_san_dns': ['localhost', 'nginx', 'mosquitto', 'fixture-a.lan'],
                'required_san_ips': ['127.0.0.1', '192.168.50.20'],
            }))
            net_config.chmod(0o600)
            context_file = Path(td) / 'context.json'
            context_file.write_text(json.dumps({'seed': 805}))
            context_file.chmod(0o600)
            ref_file = Path(td) / 'ref.json'
            ref_file.write_text(json.dumps({'status': 'PASS'}))
            ref_file.chmod(0o600)

            # The server derives the prepared seed from the private context file
            # instead of hard-coding one experiment seed.
            malformed_context = Path(td) / 'malformed-context.json'
            malformed_context.write_text(json.dumps({'valid': 'context'}))
            malformed_context.chmod(0o600)
            with patch.object(paper_p8_server.remote, 'preflight_bind'):
                with self.assertRaisesRegex(ValueError, 'server_context_seed_rejected'):
                    paper_p8_server.main(['serve', '--root', str(Path(td) / 's0'),
                                          '--network-config', str(net_config),
                                          '--experiment', 'E5', '--arm', 'optimized',
                                          '--context', str(malformed_context)])

            # Missing --arm
            with self.assertRaisesRegex(ValueError, 'E5 requires explicit --arm'):
                paper_p8_server.main(['serve', '--root', str(Path(td) / 's1'), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--context', str(context_file)])
            # Missing --context
            with self.assertRaisesRegex(ValueError, 'E5 requires --context'):
                paper_p8_server.main(['serve', '--root', str(Path(td) / 's2'), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--arm', 'optimized'])
            # Optimized with reference rejected
            with self.assertRaisesRegex(ValueError, 'E5 optimized arm does not accept --reference'):
                paper_p8_server.main(['serve', '--root', str(Path(td) / 's3'), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--arm', 'optimized', '--context', str(context_file),
                                      '--reference', str(ref_file)])
            # Ablation without reference rejected
            with self.assertRaisesRegex(ValueError, 'E5 ablation arms require --reference'):
                paper_p8_server.main(['serve', '--root', str(Path(td) / 's4'), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--arm', 'shaper_off', '--context', str(context_file)])
            # E5 with E1-E4 option rejected
            with self.assertRaisesRegex(ValueError, 'E1-E4 options are rejected for E5'):
                paper_p8_server.main(['serve', '--root', str(Path(td) / 's5'), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--arm', 'optimized', '--context', str(context_file),
                                      '--dispatch-mode', 'polling'])
            # E1 with E5 options rejected
            with self.assertRaisesRegex(ValueError, 'E5 options require --experiment E5'):
                paper_p8_server.main(['serve', '--root', str(Path(td) / 's6'), '--network-config', str(net_config),
                                      '--experiment', 'E1', '--arm', 'optimized'])

            # Command forwarding for optimized
            root_opt = Path(td) / 'server-opt'
            mock_proc = Mock(wait=Mock(return_value=0))
            with patch.object(subprocess, 'Popen', return_value=mock_proc) as mock_popen, \
                 patch.object(paper_p8_server.remote, 'preflight_bind'):
                paper_p8_server.main(['serve', '--root', str(root_opt), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--arm', 'optimized', '--context', str(context_file)])
                cmd = mock_popen.call_args[0][0]
                self.assertIn('--p8-lifecycle-hold', cmd)
                self.assertIn('--p8-e5-prepare-hold', cmd)
                self.assertIn('--p8-e5-bundle-root', cmd)
                self.assertIn(str(root_opt / 'client-bundle'), cmd)
                self.assertIn('--p8-experiment', cmd)
                self.assertEqual(cmd[cmd.index('--p8-experiment') + 1], 'E5')
                self.assertEqual(cmd[cmd.index('--p8-arm') + 1], 'optimized')
                self.assertEqual(cmd[cmd.index('--p8-seed') + 1], '805')
                self.assertEqual(cmd[cmd.index('--p8-e5-context') + 1], str(context_file))
                self.assertNotIn('--p8-e5-reference', cmd)

            # Command forwarding for ablation
            root_ablation = Path(td) / 'server-ablation'
            with patch.object(subprocess, 'Popen', return_value=mock_proc) as mock_popen, \
                 patch.object(paper_p8_server.remote, 'preflight_bind'):
                paper_p8_server.main(['serve', '--root', str(root_ablation), '--network-config', str(net_config),
                                      '--experiment', 'E5', '--arm', 'shaper_off', '--context', str(context_file),
                                      '--reference', str(ref_file)])
                cmd = mock_popen.call_args[0][0]
                self.assertEqual(cmd[cmd.index('--p8-arm') + 1], 'shaper_off')
                self.assertEqual(cmd[cmd.index('--p8-e5-reference') + 1], str(ref_file))

    def test_server_and_supervisor_e5_lifecycle(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / 'fixture/private'
            root.mkdir(parents=True, mode=0o700)
            state = supervisor.initialize(
                root, experiment='E5', arm='optimized', seed=813, deadline_seconds=900,
                receipts=[], preparation_status='E5_PREPARED', project='ncs_0123456789ab',
            )
            self.assertEqual(state['preparation_status'], 'E5_PREPARED')
            self.assertEqual(state['e5_phase_plan'], ['initial', 'warm_replay', 'control'])
            self.assertEqual(state['phase_id'], 'initial')
            self.assertEqual(state['phase'], 'READY')
            self.assertEqual(state['workload_outcome'], 'NOT_RUN')

            # status command outputs e5_phase_plan
            with patch.object(paper_p8_server, '_state_root', return_value=root):
                import io, contextlib
                stdout = io.StringIO()
                with contextlib.redirect_stdout(stdout):
                    paper_p8_server.main(['status', '--root', str(root.parent.parent), '--experiment', 'E5'])
                out_json = json.loads(stdout.getvalue())
                self.assertEqual(out_json.get('e5_phase_plan'), ['initial', 'warm_replay', 'control'])

            # Ordered E5 gating: begin is allowed only for the next planned phase
            self.assertIsNone(supervisor.e5_phase_transition_error(state, 'begin', 'initial'))
            self.assertEqual(supervisor.e5_phase_transition_error(state, 'begin', 'warm_replay'),
                             'e5_begin_phase_transition_invalid')
            self.assertEqual(supervisor.e5_phase_transition_error(state, 'finish-phase', 'initial'),
                             'e5_finish_phase_transition_invalid')
            self.assertEqual(supervisor.e5_phase_transition_error(state, 'next-phase', 'warm_replay'),
                             'e5_next_phase_requires_validated_previous_phase')

            # begin command publishes an allowed request
            with patch.object(paper_p8_server, '_state_root', return_value=root):
                with patch('sys.stdout'):
                    paper_p8_server.main(['begin', '--root', str(root.parent.parent), '--experiment', 'E5',
                                          '--session-id', state['session_id'], '--phase', 'READY',
                                          '--phase-id', 'initial'])

            # finish while READY and out-of-order begin are rejected before publication
            with patch.object(paper_p8_server, '_state_root', return_value=root):
                with self.assertRaisesRegex(ValueError, 'e5_finish_phase_transition_invalid'):
                    paper_p8_server.main(['finish-phase', '--root', str(root.parent.parent), '--experiment', 'E5',
                                          '--session-id', state['session_id'], '--phase', 'READY',
                                          '--phase-id', 'initial', '--results', str(Path(td) / 'res')])
            with patch.object(paper_p8_server, '_state_root', return_value=root):
                with self.assertRaisesRegex(ValueError, 'e5_begin_phase_transition_invalid'):
                    paper_p8_server.main(['begin', '--root', str(root.parent.parent), '--experiment', 'E5',
                                          '--session-id', state['session_id'], '--phase', 'READY',
                                          '--phase-id', 'warm_replay'])

            # supervisor _consume_request accepts the ordered begin request
            read_req = supervisor._consume_request(root, state)
            self.assertEqual(read_req['command'], 'begin')
            self.assertEqual(read_req['phase_id'], 'initial')
            re_read_state = supervisor.read_state(root)
            self.assertNotIn('last_request_error', re_read_state)
            self.assertEqual(re_read_state['phase'], 'READY')
            self.assertEqual(re_read_state['workload_outcome'], 'NOT_RUN')

            # A validated initial phase authorizes only the warm_replay transition
            progressed = dict(re_read_state, phase='VERIFYING', phase_id='initial',
                              workload_outcome='VALIDATED', phase_history=['initial'],
                              imported_result={'phase_id': 'initial', 'validation_valid': True})
            self.assertIsNone(supervisor.e5_phase_transition_error(progressed, 'next-phase', 'warm_replay'))
            self.assertEqual(
                supervisor.e5_phase_transition_error(progressed, 'next-phase', 'control'),
                'e5_next_phase_requires_validated_previous_phase')
            reopened = dict(progressed, phase='READY', phase_id='warm_replay',
                            phase_history=['initial'])
            self.assertIsNone(supervisor.e5_phase_transition_error(reopened, 'begin', 'warm_replay'))
            self.assertEqual(supervisor.e5_phase_transition_error(reopened, 'begin', 'initial'),
                             'e5_begin_phase_transition_invalid')

            # finalize command succeeds, records NOT_RUN
            with patch.object(paper_p8_server, '_state_root', return_value=root):
                paper_p8_server.main(['finalize', '--root', str(root.parent.parent), '--experiment', 'E5',
                                      '--session-id', state['session_id'], '--phase', 'READY'])
            final_req = supervisor._consume_request(root, re_read_state)
            self.assertEqual(final_req['command'], 'finalize')
            supervisor.begin_finalizing(root, 'NOT_RUN')
            supervisor.finalize(root, cleanup_verified=True)
            closed_state = supervisor.read_state(root)
            self.assertEqual(closed_state['phase'], 'CLOSED')
            self.assertEqual(closed_state['workload_outcome'], 'NOT_RUN')
            self.assertEqual(closed_state['cleanup_outcome'], 'VERIFIED')

    def test_native_compose_e5_prepare_hold_cli_validation(self):
        with tempfile.TemporaryDirectory() as td:
            # Without --p8-lifecycle-hold
            with patch.object(sys, 'argv', ['native_compose_smoke.py', '--prepare', str(Path(td) / 'p1'),
                                            '--p8-e5-prepare-hold', '--p8-experiment', 'E5']):
                with self.assertRaises(SystemExit) as raised:
                    smoke.main()
                self.assertEqual(raised.exception.code, 2)

            # With wrong experiment (E4)
            with patch.object(sys, 'argv', ['native_compose_smoke.py', '--prepare', str(Path(td) / 'p2'),
                                            '--p8-lifecycle-hold', '--p8-e5-prepare-hold',
                                            '--p8-experiment', 'E4', '--p8-e5-bundle-root', str(Path(td) / 'b'),
                                            '--p8-e5-context', str(Path(td) / 'c')]):
                with self.assertRaises(SystemExit) as raised:
                    smoke.main()
                self.assertEqual(raised.exception.code, 2)

            # E5 prepare report produces PREPARATION_ONLY with NOT_RUN
            fixture = Mock(
                private=Path(td) / 'private', artifacts=Path(td) / 'artifacts',
                project='ncs_0123456789ab', paper_p8_e5='optimized',
            )
            fixture.private.mkdir(parents=True, mode=0o700)
            fixture.artifacts.mkdir(parents=True, mode=0o700)
            (fixture.private / 'session.json').write_text(json.dumps({'experiment': 'E5', 'workload_outcome': 'NOT_RUN'}))
            (fixture.artifacts / 'p8-e5-optimized.json').write_text(json.dumps({
                'status': 'PREPARATION_ONLY', 'workload_status': 'NOT_RUN',
                'full_p8_accepted': False, 'physical_ab': 'NOT RUN',
            }))
            report = smoke.paper_p8_prepare_only_report(
                fixture, remote_outcome='NOT_RUN', cleanup_verified=True, cleanup_counts={'container': 0},
            )
            self.assertEqual(report['status'], 'PAPER_P8_E5_PREPARATION_ONLY')
            self.assertEqual(report['workload_outcome'], 'NOT_RUN')
            self.assertFalse(report['full_p8_accepted'])
            self.assertFalse(report['p8_e5_workload_verified'])

            # Run status helper
            self.assertEqual(smoke.paper_p8_prepare_only_run_status(
                'E5', failed=False, cleanup_verified=True, remote_outcome='NOT_RUN'), 'E5_PREPARATION_ONLY_CLEANUP')
            self.assertEqual(smoke.paper_p8_prepare_only_run_status(
                'E5', failed=True, cleanup_verified=False, remote_outcome='NOT_RUN'), 'FAIL')

    def _valid_e5_evidence(self):
        return {
            'status': 'PASS', 'workload_status': 'WORKLOAD_COMPLETE_PENDING_CLEANUP', 'arm': 'optimized',
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
                'ingress_delta': {'available': True}},
            'mechanism_metrics': {'activation_status': 'AVAILABLE'},
            'metadata': {'workload': {'workload_type': 'test'}, 'effective_flags': {'STAGE5_ENABLED': 'true'}},
        }

    def test_local_staged_e5_post_cleanup_artifact_accepts_valid_workload_and_rejects_failures(self):
        valid = self._valid_e5_evidence()
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            priv = root / 'private'
            art = root / 'artifacts'
            priv.mkdir(mode=0o700)
            art.mkdir(mode=0o700)

            # Local non-held E5 fixture with explicit boolean attributes
            fixture = SimpleNamespace(
                private=priv, artifacts=art,
                paper_p8_e5='optimized',
                paper_p8_prepare_only=False,  # explicit boolean False for local non-held
            )
            staged_e5 = priv / 'p8-e5-result.json'
            p8_path = art / 'p8-e5-optimized.json'

            # 1. Valid staged local E5 report with call-site default remote_outcome='NOT_RUN'
            staged_e5.write_text(json.dumps(valid))
            res = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='NOT_RUN', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 12},
            )
            self.assertEqual(res['status'], 'PASS')
            self.assertEqual(res['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
            self.assertTrue(res['exact_cleanup_verified'])
            self.assertEqual(res['cleanup_counts'], {'container': 12})
            self.assertTrue(p8_path.is_file())
            on_disk = json.loads(p8_path.read_text())
            self.assertEqual(on_disk['status'], 'PASS')
            self.assertEqual(on_disk['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')

            # 2. Invalid staged local E5 report remains FAIL
            bad = dict(valid, validation={'valid': False, 'failures': ['viewer_header_provenance']})
            staged_e5.write_text(json.dumps(bad))
            p8_path.unlink()
            res_bad = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='NOT_RUN', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 12},
            )
            self.assertEqual(res_bad['status'], 'FAIL')

            # 3. Missing staged artifact does NOT return PREPARATION_ONLY or PASS
            staged_e5.unlink()
            p8_path.unlink()
            res_missing = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='NOT_RUN', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 12},
            )
            self.assertEqual(res_missing['status'], 'FAIL')

            # 4. cleanup_verified=False fails
            staged_e5.write_text(json.dumps(valid))
            p8_path.unlink()
            res_unclean = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='NOT_RUN', failure=None,
                cleanup_verified=False, cleanup_counts={'container': 0},
            )
            self.assertEqual(res_unclean['status'], 'FAIL')
            self.assertFalse(res_unclean['exact_cleanup_verified'])

    def test_native_compose_e5_prepare_hold_final_artifact_merge_and_post_run_acceptance(self):
        valid_evidence = self._valid_e5_evidence()

        with tempfile.TemporaryDirectory() as td:
            fixture = Mock(
                private=Path(td) / 'private', artifacts=Path(td) / 'artifacts',
                project='ncs_0123456789ab', paper_p8_e5='optimized',
                paper_p8_load=True, paper_p8_prepare_only=True,
                paper_p8_e5_prepare_only=True, paper_p8_recovery=False,
                paper_p8_e4_remote_warm=False, paper_p2=False, paper_p1=False,
                paper_p3=False, paper_p4=False, paper_p5a=False, paper_p6=False,
                paper_p8_sessions=False, paper_p8_slow=False, paper_p8_e1=False,
                paper_p8_e2=False, paper_p8_e3=None, paper_p8_e4=None,
                paper_p2_spike=False,
            )
            fixture.private.mkdir(parents=True, mode=0o700)
            fixture.artifacts.mkdir(parents=True, mode=0o700)

            # 1. Valid finalized report survives, passes post_run_status and main acceptance
            session_state = {
                'experiment': 'E5', 'phase': 'CLOSED', 'phase_history': ['initial', 'warm_replay', 'control'],
                'workload_outcome': 'VALIDATED', 'cleanup_outcome': 'VERIFIED',
            }
            (fixture.private / 'session.json').write_text(json.dumps(session_state))
            (fixture.artifacts / 'p8-e5-optimized.json').write_text(json.dumps(valid_evidence))

            # Exercise actual production post-cleanup artifact handler called by startup_only
            p8_path = fixture.artifacts / 'p8-e5-optimized.json'
            staged_e5 = fixture.private / 'p8-e5-result.json'
            p8 = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='VALIDATED', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 12},
            )

            # Check that workload_status was not clobbered
            self.assertEqual(p8['status'], 'PASS')
            self.assertEqual(p8['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
            self.assertTrue(p8['exact_cleanup_verified'])
            self.assertEqual(p8['cleanup_counts'], {'container': 12})
            self.assertIn('phases', p8)
            self.assertIn('control_reconciliation', p8)

            reloaded_p8 = json.loads(p8_path.read_text())
            self.assertEqual(reloaded_p8['status'], 'PASS')
            self.assertEqual(reloaded_p8['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
            self.assertTrue(reloaded_p8['exact_cleanup_verified'])
            self.assertIn('phases', reloaded_p8)
            self.assertIn('control_reconciliation', reloaded_p8)

            # Test paper_p8_e5_post_run_status
            ok, outcome = smoke.paper_p8_e5_post_run_status(session_state, reloaded_p8)
            self.assertTrue(ok)
            self.assertEqual(outcome, 'WORKLOAD_COMPLETE_PENDING_CLEANUP')

            # Test prepare-only report on validated success
            rep = smoke.paper_p8_prepare_only_report(
                fixture, remote_outcome='VALIDATED', cleanup_verified=True, cleanup_counts={'container': 12})
            self.assertEqual(rep['status'], 'PAPER_P8_E5_WORKLOAD_VERIFIED')
            self.assertTrue(rep['p8_e5_workload_verified'])

            # 1b. Test staged local E5 report resolution
            p8_path.unlink()
            staged_e5.write_text(json.dumps(valid_evidence))
            staged_p8 = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='VALIDATED', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 12},
            )
            self.assertEqual(staged_p8['status'], 'PASS')
            self.assertEqual(staged_p8['workload_status'], 'WORKLOAD_COMPLETE_PENDING_CLEANUP')
            self.assertTrue(p8_path.is_file())
            staged_e5.unlink()

            # 2. Invalid validation in evidence fails post_run_status and post_cleanup_artifact
            bad_evidence = dict(valid_evidence, validation={'valid': False, 'failures': ['viewer_header_provenance']})
            p8_path.write_text(json.dumps(bad_evidence))
            p8_bad = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='VALIDATED', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 12},
            )
            self.assertEqual(p8_bad['status'], 'FAIL')
            ok_bad, _ = smoke.paper_p8_e5_post_run_status(session_state, p8_bad)
            self.assertFalse(ok_bad)
            rep_bad = smoke.paper_p8_prepare_only_report(
                fixture, remote_outcome='VALIDATED', cleanup_verified=True, cleanup_counts={'container': 12})
            self.assertEqual(rep_bad['status'], 'PAPER_P8_E5_WORKLOAD_VALIDATION_FAILED')

            # 3. Incomplete phase history fails post_run_status
            partial_session = dict(session_state, phase_history=['initial', 'warm_replay'])
            ok_part, _ = smoke.paper_p8_e5_post_run_status(partial_session, reloaded_p8)
            self.assertFalse(ok_part)

            # 4. cleanup_verified=False produces FAIL and exact_cleanup_verified=False
            p8_path.write_text(json.dumps(valid_evidence))
            p8_unclean = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='VALIDATED', failure=None,
                cleanup_verified=False, cleanup_counts={'container': 0},
            )
            self.assertEqual(p8_unclean['status'], 'FAIL')
            self.assertFalse(p8_unclean['exact_cleanup_verified'])
            ok_clean, _ = smoke.paper_p8_e5_post_run_status(session_state, p8_unclean)
            self.assertFalse(ok_clean)

            # 5. Never-begun run produces PREPARATION_ONLY and NOT_RUN
            not_run_session = {'experiment': 'E5', 'phase': 'CLOSED', 'phase_history': [],
                               'workload_outcome': 'NOT_RUN', 'collector_ready': False}
            prep_evidence = {'status': 'PREPARATION_ONLY', 'workload_status': 'NOT_RUN', 'full_p8_accepted': False}
            p8_path.write_text(json.dumps(prep_evidence))
            p8_prep = smoke.paper_p8_e5_post_cleanup_artifact(
                fixture, p8_path=p8_path, staged_e5=staged_e5,
                remote_outcome='NOT_RUN', failure=None,
                cleanup_verified=True, cleanup_counts={'container': 0},
            )
            self.assertEqual(p8_prep['status'], 'PREPARATION_ONLY')
            self.assertEqual(p8_prep['workload_status'], 'NOT_RUN')
            ok_not_run, outcome_not_run = smoke.paper_p8_e5_post_run_status(not_run_session, p8_prep)
            self.assertTrue(ok_not_run)
            self.assertEqual(outcome_not_run, 'NOT_RUN')
            prep_rep = smoke.paper_p8_prepare_only_report(
                fixture, remote_outcome='NOT_RUN', cleanup_verified=True, cleanup_counts={'container': 0})
            self.assertEqual(prep_rep['status'], 'PAPER_P8_E5_PREPARATION_ONLY')


if __name__ == '__main__':
    unittest.main()
