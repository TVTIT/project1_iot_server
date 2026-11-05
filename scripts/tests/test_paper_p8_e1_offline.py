import unittest
import json
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO

from scripts import paper_p8_server
from scripts.tests import paper_p8_e1_support


class E1OfflineContractTests(unittest.TestCase):
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
                self.assertEqual(overlay['proposed']['nginx_process_fd_soft'], max(1024, connections + 128))
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
        for viewers in (5000, 10000):
            with self.subTest(viewers=viewers):
                output = StringIO()
                with redirect_stdout(output):
                    code = paper_p8_server.main(['serve', '--root', '/not-used',
                        '--network-config', '/not-used', '--dry-run', '--experiment', 'E1',
                        '--profile', 'official', '--official-opt-in', '--e1-viewers', str(viewers),
                        '--e1-principals', '2', '--e1-density', 'mixed', '--e1-messages-per-target', '5'])
                self.assertEqual(code, 0)
                result = json.loads(output.getvalue())['e1_offline_contract']
                self.assertEqual(result['native_executed'], False)
                self.assertEqual(result['physical_ab'], 'NOT RUN')
                self.assertEqual(result['mapping']['counts']['sockets'], viewers)
                self.assertGreater(result['serialized_sizes']['mapping_bytes'], 1000)
                self.assertGreater(result['serialized_sizes']['bundle_bytes'], 0)
                self.assertEqual(result['lifecycle']['ramp_seconds'], min(120, viewers // 50))
                self.assertEqual(result['lifecycle']['viewer_ready_timeout_seconds'], 120)
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
                self.assertTrue(result['result_schema_checked'])

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
