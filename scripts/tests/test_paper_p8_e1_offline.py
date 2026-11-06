import unittest
import json
from pathlib import Path
import subprocess
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

from scripts import paper_p8_server
from scripts.tests import paper_p8_e1_support


class E1OfflineContractTests(unittest.TestCase):
    def test_offline_e1_phases_match_prepare_default(self):
        result=paper_p8_server.build_e1_offline_contract(
            viewers=5000, principals=2, density='mixed', messages_per_target=5)
        self.assertEqual(result['lifecycle']['measure_seconds'],14)
        self.assertEqual(result['lifecycle']['drain_seconds'],8)

    def test_e4_official_dry_run_exports_binds_and_preflights_real_manifests(self):
        output = StringIO()
        with redirect_stdout(output):
            code = paper_p8_server.main(['serve', '--root', '/not-used',
                '--network-config', '/not-used', '--dry-run', '--experiment', 'E4',
                '--profile', 'official', '--official-opt-in', '--e4-unique-count', '1000',
                '--e4-replay-count', '100', '--expiry-replay', '--deadline-seconds', '1200'])
        self.assertEqual(code, 0)
        result = json.loads(output.getvalue())['e4_offline_contract']
        self.assertEqual(result['native_executed'], False)
        self.assertEqual(result['identity_fixture'], 'unsigned_schema_only_never_used_on_network')
        self.assertEqual(result['bundle_roundtrip'], {'export': True, 'inspect': True, 'bind': True})
        self.assertEqual(result['manifest_inventory'], ['expiry-replay', 'replay', 'unique'])
        self.assertEqual(result['bound_profile'], 'official')
        self.assertEqual(result['bound_session_id'], 'e4-offline-session')
        self.assertEqual(result['result_projection'], {
            'schema_valid': True, 'status': 'PARTIAL', 'native_completed_acceptance': False})
        self.assertEqual({row['phase']: row['exit_code'] for row in result['preflight']},
                         {'unique': 0, 'replay': 0, 'expiry-replay': 0})
        for row in result['preflight']:
            output = json.loads(row['stdout'])
            self.assertEqual(output['status'], 'PREFLIGHT_ONLY_NOT_RUN')
            self.assertEqual(output['profile'], 'official')
            expected_messages = 1000 if row['phase'] == 'unique' else 100
            self.assertEqual(output['estimate']['proposed_messages'], expected_messages)
            self.assertEqual(output['estimate']['proposed_raw_samples'], expected_messages * 100)
        self.assertEqual(result['expected_counts'], {
            'unique_batches': 1000, 'unique_raw_samples': 100000,
            'unique_application_acks': 1000, 'replay_batches': 100,
            'replay_payload_raw_samples': 10000, 'replay_new_raw_samples': 0,
            'replay_application_acks': 100,
            'replay_new_deliveries': 0})

    def test_selected_overlay_sizing_is_finite_and_does_not_scale_queues(self):
        for viewers in (64, 128, 1000, 5000, 10000):
            with self.subTest(viewers=viewers):
                overlay = paper_p8_e1_support.e1_resource_overlay(viewers, official_opt_in=True)
                self.assertEqual(overlay['proposed']['WS_MAX_SESSIONS'], viewers)
                workers = min(32, max(1, (viewers + 63) // 64))
                base = (2 * viewers + 34 + workers - 1) // workers
                connections = max(256, 2 * base + 32)
                self.assertEqual(overlay['proposed']['nginx_worker_connections'], connections)
                self.assertEqual(overlay['proposed']['nginx_process_fd_soft'], max(1024, connections + 128 + 64))
                self.assertEqual(overlay['proposed']['nginx_worker_processes'], workers)
                self.assertEqual(overlay['proposed']['WS_SESSION_REFERENCE_BYTES'], 8 * 1024 * 1024)
                self.assertEqual(overlay['estimated_only'], True)
        with self.assertRaisesRegex(ValueError, 'e1_resource_gate_denied:official_opt_in_required'):
            paper_p8_e1_support.e1_resource_overlay(128, official_opt_in=False)

    def test_selected_overlay_refuses_insufficient_host_memory_or_fd(self):
        with self.assertRaisesRegex(ValueError, 'e1_resource_gate_denied:host_fd'):
            paper_p8_e1_support.e1_resource_overlay(128, official_opt_in=True,
                host_limits={'fd_soft': 100})
        with self.assertRaisesRegex(ValueError, 'e1_resource_gate_denied:host_memory'):
            paper_p8_e1_support.e1_resource_overlay(128, official_opt_in=True,
                host_limits={'memory_available_bytes': 1})

    def test_official_large_rungs_build_mapping_and_bundle_contract_without_workload(self):
        scratch_base = Path('/var/tmp/opencode/p8-e1-large-viewer-repair')
        scratch_base.mkdir(mode=0o700, parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='e1r5-test-', dir=scratch_base) as td:
            scratch = Path(td); scratch.chmod(0o700)
            binary = scratch / 'paperload'
            built = subprocess.run(['go', 'build', '-o', str(binary), './cmd/paperload'],
                cwd=paper_p8_server.ROOT / 'src', capture_output=True, text=True,
                timeout=180, check=False)
            self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
            binary.chmod(0o700)
            for viewers, ready, wall in ((2000, 100, 130), (5000, 160, 190), (10000, 180, 210)):
                with self.subTest(viewers=viewers):
                    result = paper_p8_server.build_e1_offline_contract(viewers=viewers,
                        principals=2, density='mixed', messages_per_target=5,
                        scratch_root=scratch, paperload_binary=binary)
                    self.assertEqual(result['native_executed'], False)
                    self.assertEqual(result['physical_ab'], 'NOT RUN')
                    self.assertEqual(result['mapping']['counts']['sockets'], viewers)
                    self.assertGreater(result['serialized_sizes']['mapping_bytes'], 1000)
                    self.assertGreater(result['serialized_sizes']['bundle_bytes'], 0)
                    self.assertEqual(result['lifecycle']['ramp_seconds'], min(120, viewers // 50))
                    self.assertEqual(result['lifecycle']['viewer_ready_timeout_seconds'], ready)
                    self.assertEqual(result['lifecycle']['measure_seconds'], 14)
                    self.assertEqual(result['lifecycle']['drain_seconds'], 8)
                    self.assertEqual(result['lifecycle']['wall_seconds'], wall)
                    self.assertEqual(result['lifecycle']['warmup_seconds'], 0)
                    self.assertEqual(result['lifecycle']['cooldown_seconds'], 0)
                    self.assertGreater(result['receive_limits']['max_messages'], 0)
                    self.assertLessEqual(result['receive_limits']['max_messages'], 10_000_000)
                    self.assertGreaterEqual(result['receive_limits']['max_bytes'],
                                            result['receive_limits']['max_messages'])
                    expected_messages = sum(result['mapping']['expected_deliveries_by_target'].values())
                    self.assertEqual(result['receive_limits']['max_messages'], expected_messages)
                    self.assertEqual(result['receive_limits']['max_bytes'], expected_messages * 8192)
                    self.assertLessEqual(result['receive_limits']['max_bytes'], 1 << 30)
                    self.assertEqual(result['manifest_correlation']['viewer']['profile'], 'official')
                    self.assertEqual(result['manifest_correlation']['telemetry']['profile'], 'official')
                    self.assertEqual(result['manifest_correlation']['viewer']['run_id'],
                                     result['manifest_correlation']['telemetry']['run_id'])
                    self.assertEqual(result['manifest_correlation']['viewer']['targets'],
                                     result['manifest_correlation']['telemetry']['targets'])
                    self.assertEqual(result['bundle_correlation']['session_id'], 'e1-offline-session')
                    self.assertEqual(result['bundle_correlation']['profile'], 'official')
                    self.assertIn(result['manifest_correlation']['viewer']['run_id'],
                                  result['bundle_correlation']['run_ids'])
                    self.assertEqual(result['resource_requirements']['effective_a_limits'],
                                     'not_read_in_offline_dry_run')
                    overlay = result['resource_requirements']['selected_overlay']
                    self.assertEqual(overlay['viewers'], viewers)
                    self.assertTrue(overlay['estimated_only'])
                    self.assertIsNone(overlay['readback'])
                    self.assertFalse(overlay['queues_scaled_with_viewers'])
                    self.assertTrue(result['bundle_roundtrip']['inspect'])
                    self.assertTrue(result['bundle_roundtrip']['bind'])
                    self.assertTrue(result['bundle_roundtrip']['paperload_preflight'])
                    self.assertEqual([(row['role'], row['exit_code'], row['status'])
                        for row in result['paperload_preflight_outcomes']],
                        [('viewer', 0, 'PREFLIGHT_ONLY_NOT_RUN'),
                         ('telemetry', 0, 'PREFLIGHT_ONLY_NOT_RUN')])
                    self.assertTrue(result['result_schema_checked'])
                    self.assertEqual(result['result_projection_status'], 'PARTIAL')
                    self.assertFalse(result['native_completed_acceptance'])
                    sizes = result['serialized_sizes']; limits = result['serialized_limits']
                    self.assertLessEqual(sizes['viewer_manifest_bytes'], limits['manifest_bytes'])
                    self.assertLessEqual(sizes['telemetry_manifest_bytes'], limits['manifest_bytes'])
                    self.assertLessEqual(sizes['bundle_bytes'], limits['bundle_bytes'])
                    self.assertLessEqual(sizes['result_projection_bytes'], limits['file_bytes'])

    def test_resource_gate_rejects_known_nginx_and_fd_caps_before_workload(self):
        resources = {
            'nginx': {'worker_processes': 1, 'worker_connections': 1024,
                      'required_connections_per_worker': 10032, 'fits': False},
            'process_fd_limits': {'soft': 4096, 'hard': 4096},
            'backend_fd_availability': {'available': 4096, 'soft': 4096, 'open': 0},
            'broker_fd_availability': {'available': 4096, 'soft': 4096, 'open': 0},
        }
        with self.assertRaisesRegex(ValueError, 'e1_resource_gate_denied:nginx_worker_connections'):
            paper_p8_e1_support.require_e1_resource_gate(resources, viewers=5000, session_cap=10000)

    def test_resource_gate_rejects_known_64_session_ceiling(self):
        with self.assertRaisesRegex(ValueError, 'backend_ws_session_cap:required=5000:available=64'):
            paper_p8_e1_support.require_e1_resource_gate({}, viewers=5000, session_cap=64)

    def test_resource_gate_uses_live_per_worker_readback_for_selected_overlay(self):
        result = paper_p8_e1_support.require_e1_resource_gate({
            'nginx': {'worker_processes':2,'worker_connections':322},
            'process_fd_limits': {'soft':1024,'hard':1024},
            'nginx_workers': [{'pid': i, 'soft': 1024, 'hard': 1024, 'open': 50, 'available': 974}
                              for i in (2, 3)],
            'backend_fd_availability': {'available':160,'soft':224,'open':64},
            'broker_fd_availability': {'available':34,'soft':98,'open':64}},
            viewers=128, session_cap=128)
        self.assertEqual(result['proxy_slots_total_required'], 290)
        self.assertEqual(result['proxy_slots_per_worker_required'], 322)
        self.assertEqual(result['nginx_fd_limit_required'], 1024)
        self.assertTrue(result['native_executed'] is False)

    def test_10000_viewer_overlay_covers_baseline_fds_without_relaxing_gate(self):
        proposal = paper_p8_e1_support.e1_resource_overlay(
            10000, official_opt_in=True)['proposed']
        soft = proposal['nginx_process_fd_soft']
        connections = proposal['nginx_worker_connections']
        resources = {
            'nginx': {'worker_processes':proposal['nginx_worker_processes'],
                      'worker_connections':connections},
            'process_fd_limits': {'soft':soft,'hard':soft},
            'nginx_workers': [{'pid':i+2,'soft':soft,'hard':soft,'open':40+i%2,
                              'available':soft-40-i%2} for i in range(32)],
            'backend_fd_availability': {'available':10084},
            'broker_fd_availability': {'available':90},
        }
        # Reproduce the observed 40/41 baseline FDs, then exercise the full
        # proposed baseline allowance and the unchanged free-FD boundary.
        result = paper_p8_e1_support.require_e1_resource_gate(
            resources, viewers=10000, session_cap=10000)
        self.assertEqual(result['status'], 'RESOURCE_GATE_PASSED')
        self.assertEqual(connections, 1286)
        self.assertEqual(soft, 1478)
        self.assertEqual(result['nginx_fd_limit_required'], connections + 128)
        for worker in resources['nginx_workers']:
            worker.update(open=64, available=soft-64)
        self.assertEqual(paper_p8_e1_support.require_e1_resource_gate(
            resources, viewers=10000, session_cap=10000)['status'], 'RESOURCE_GATE_PASSED')
        # One exhausted worker must refuse the whole lane, even though other
        # workers have sufficient room and its configured soft limit is correct.
        resources['nginx_workers'][-1].update(open=65, available=soft-65)
        with self.assertRaisesRegex(ValueError, 'nginx_worker_fd_available'):
            paper_p8_e1_support.require_e1_resource_gate(
                resources, viewers=10000, session_cap=10000)
        # The original 1414-FD readback must still fail, not become accepted.
        resources['process_fd_limits'].update(soft=1414, hard=1414)
        for worker in resources['nginx_workers']:
            worker.update(soft=1414, hard=1414, open=41, available=1373)
        with self.assertRaisesRegex(ValueError, 'nginx_worker_fd_available'):
            paper_p8_e1_support.require_e1_resource_gate(
                resources, viewers=10000, session_cap=10000)

    def test_resource_gate_rejects_known_nginx_fd_limit(self):
        resources = {'nginx': {'worker_processes': 1, 'worker_connections': 20100},
            'process_fd_limits': {'soft': 4096, 'hard': 4096},
            'backend_fd_availability': {'available': 20000},
            'broker_fd_availability': {'available': 64}}
        with self.assertRaisesRegex(ValueError, 'nginx_fd_limit:required=20228:available=4096'):
            paper_p8_e1_support.require_e1_resource_gate(resources, viewers=5000, session_cap=10000)

    def test_1000_viewer_gate_rejects_old_overlay_and_insufficient_worker_fd(self):
        resources = {'nginx': {'worker_processes': 16, 'worker_connections': 128},
            'process_fd_limits': {'soft': 128, 'hard': 128},
            'backend_fd_availability': {'available': 1084},
            'broker_fd_availability': {'available': 90}}
        with self.assertRaisesRegex(ValueError, 'nginx_worker_connections:required=288:available=128'):
            paper_p8_e1_support.require_e1_resource_gate(resources, viewers=1000, session_cap=1000)
        resources['nginx']['worker_connections'] = 288
        resources['process_fd_limits'] = {'soft': 1024, 'hard': 1024}
        resources['nginx_workers'] = [{'pid': i + 2, 'soft': 1024, 'hard': 1024,
            'open': 50, 'available': 974} for i in range(16)]
        self.assertEqual(paper_p8_e1_support.require_e1_resource_gate(resources,
            viewers=1000, session_cap=1000)['status'], 'RESOURCE_GATE_PASSED')
        resources['nginx_workers'][0].update(soft=128, available=78)
        with self.assertRaisesRegex(ValueError, 'nginx_worker_fd_limit'):
            paper_p8_e1_support.require_e1_resource_gate(resources, viewers=1000, session_cap=1000)
        resources['nginx_workers'][0].update(soft=1024, open=700, available=324)
        with self.assertRaisesRegex(ValueError, 'nginx_worker_fd_available'):
            paper_p8_e1_support.require_e1_resource_gate(resources, viewers=1000, session_cap=1000)
        resources['nginx_workers'] = None
        with self.assertRaisesRegex(ValueError, 'nginx_workers_unavailable'):
            paper_p8_e1_support.require_e1_resource_gate(resources, viewers=1000, session_cap=1000)

    def test_resource_gate_rejects_unknown_critical_readback(self):
        with self.assertRaisesRegex(ValueError, 'e1_resource_gate_denied:nginx_worker_connections_unavailable'):
            paper_p8_e1_support.require_e1_resource_gate({}, viewers=5000, session_cap=10000)

    def test_offline_contract_rejects_unsupported_principals(self):
        with self.assertRaisesRegex(ValueError, 'e1_fixture_principal_topology_unsupported'):
            paper_p8_server.build_e1_offline_contract(
                viewers=5000, principals=3, density='mixed', messages_per_target=5)

    def test_cli_rejects_invalid_profile_before_any_fixture_access(self):
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit) as raised:
            paper_p8_server.main(['serve', '--root', '/not-used', '--network-config', '/not-used',
                '--dry-run', '--experiment', 'E1', '--profile', 'invalid'])
        self.assertEqual(raised.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
