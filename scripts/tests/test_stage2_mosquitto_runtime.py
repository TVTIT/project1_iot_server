#!/usr/bin/env python3
import unittest
from unittest.mock import patch

import stage2_mosquitto_runtime as runtime


class Contracts(unittest.TestCase):
    def test_cleanup_owned_scope_partial_build_and_recreated_resources(self):
        poc = runtime.PoC('/tmp/opencode')
        commands = []
        def fake_run(argv, **kwargs):
            commands.append(argv)
            if 'reference=' + poc.owned_images[0] in argv and '--format' in argv:
                return (poc.owned_images[0] + '\n').encode()
            if 'ls' in argv and any(v.startswith('label=') for v in argv):
                # Registry before down sees resources created/recreated after
                # the initial snapshot; verification after down sees none.
                return b'new-owned-id\n' if len([c for c in commands if c[:2] == argv[:2]]) == 1 else b''
            return b''
        with patch.object(runtime, 'run', side_effect=fake_run), patch.object(poc, 'command') as command:
            poc.cleanup()
        command.assert_called_once_with('down', '-v', '--remove-orphans', timeout=60)
        self.assertEqual(poc.owned['container'], ['new-owned-id'])
        self.assertIn(['docker', 'image', 'rm', poc.owned_images[0]], commands)
        for argv in commands:
            self.assertNotIn('prune', argv)
            self.assertNotIn('-f', argv)
            if 'label=com.docker.compose.project=' + poc.project in argv:
                self.assertIn('--filter', argv)

    def test_cleanup_failures_are_fatal(self):
        for failure in ('down', 'remaining', 'image-remove'):
            poc = runtime.PoC('/tmp/opencode')
            def fake_run(argv, failure=failure, **kwargs):
                if failure == 'remaining' and 'ls' in argv and 'container' in argv:
                    return b'owned-leftover\n'
                if failure == 'image-remove':
                    if '--format' in argv: return b'owned-tag\n'
                    if 'rm' in argv: raise RuntimeError('subprocess failed')
                return b''
            with patch.object(runtime, 'run', side_effect=fake_run), patch.object(poc, 'command', side_effect=RuntimeError('down failed') if failure == 'down' else None), self.assertRaises(RuntimeError):
                poc.cleanup()

    def test_cleanup_rejects_foreign_resource_names(self):
        poc = runtime.PoC('/tmp/opencode')
        poc.project = 'user-deployment'
        with patch.object(runtime, 'run') as run, patch.object(poc, 'command') as command, self.assertRaises(RuntimeError):
            poc.cleanup()
        run.assert_not_called()
        command.assert_not_called()

    def test_fault_selectors_no_skip_and_output_secrecy(self):
        import tempfile
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            poc = runtime.PoC(directory)
            fixture = {'Old': 'private-old', 'New': 'private-new', 'Backend': 'private-backend', 'BPassword': 'private-b'}
            proof = ('--- PASS: TestProductionRuntimeFaultIntegration\n'
                     'production adapter phase completed: fault-probe\nadapter_exit=0\n')
            with patch.object(poc, 'exec', return_value=(proof + 'private-new').encode()), self.assertRaisesRegex(RuntimeError, 'secret detected'):
                poc.adapter_phase(fixture, 'fault-probe')
            runtime.assert_adapter_output(proof, 'fault-probe', 'TestProductionRuntimeFaultIntegration')
            with self.assertRaises(RuntimeError):
                runtime.assert_adapter_output(proof + '--- SKIP:', 'fault-probe', 'TestProductionRuntimeFaultIntegration')

    def test_adapter_proof_cannot_skip_or_select_nothing(self):
        valid = ('--- PASS: TestProductionRuntimeIntegration (1s)\n'
                 'production adapter phase completed: mutate\nadapter_exit=0\n')
        runtime.assert_adapter_output(valid, 'mutate')
        for output in ('PASS', valid + '--- SKIP: other',
                       valid.replace('mutate', 'persist'),
                       valid.replace('--- PASS:', '--- FAIL:'),
                       valid.replace('adapter_exit=0', 'adapter_exit=1')):
            with self.assertRaises(RuntimeError):
                runtime.assert_adapter_output(output, 'mutate')

    def test_adapter_fixture_only_stdin_and_coverage_not_store(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory(dir='/tmp/opencode') as directory:
            poc = runtime.PoC(directory)
            poc.project = Path(directory).name
            fixture = {'Old': 'old-private-secret', 'New': 'new-private-secret'}
            proof = (b'--- PASS: TestProductionRuntimeIntegration\n'
                     b'production adapter phase completed: mutate\nadapter_exit=0\n')
            with patch.object(poc, 'exec', side_effect=[proof, b'mode: set\n']) as execute:
                poc.adapter_phase(fixture, 'mutate')
            args, kwargs = execute.call_args_list[0]
            self.assertNotIn('private-secret', repr(args))
            self.assertIn(b'private-secret', kwargs['data'])
            self.assertIn('MQTT_RUNTIME_INTEGRATION=1', repr(args))
            artifact = Path('/tmp/opencode') / (poc.project + '-adapter-mutate.cover')
            self.assertEqual(artifact.read_bytes(), b'mode: set\n')
            artifact.unlink()

    def test_production_reloader_protocol_helper_half_closes(self):
        self.assertIn('CloseWrite()', runtime.HELPER)
        self.assertIn('io.ReadAll(c)', runtime.HELPER)
        self.assertNotIn('syscall.Kill', runtime.HELPER)

    def test_connack_requires_exact_auth_rejection(self):
        runtime.assert_connack(b'\x20\x02\x00\x05', False)
        for packet in (b'', b'\x20\x02\x00\x00', b'\x20\x02\x00\x03'):
            with self.assertRaises(RuntimeError):
                runtime.assert_connack(packet, False)

    def test_success_requires_exact_connack(self):
        runtime.assert_connack(b'\x20\x02\x00\x00', True)
        with self.assertRaises(RuntimeError):
            runtime.assert_connack(b'', True)

    def test_backend_protected(self):
        with self.assertRaises(ValueError):
            runtime.validate_gateway('backend_service')
        with self.assertRaises(ValueError):
            runtime.validate_gateway('bad:entry')
        with self.assertRaises(ValueError):
            runtime.validate_gateway('_bad')
        runtime.validate_gateway('gw_test')

    def test_native_tool_secrecy(self):
        with patch.object(runtime.subprocess, 'run') as run:
            run.return_value.returncode = 0
            runtime.run(['docker', 'exec', '-i', 'owned', runtime.TOOL,
                         '-H', 'sha512-pbkdf2', '/mosquitto/data/candidate',
                         'gw_test'], b'test-secret\ntest-secret\n')
            args, kwargs = run.call_args
            self.assertNotIn('test-secret', repr(args))
            self.assertNotIn('test-secret', repr(kwargs['env']))
            self.assertEqual(kwargs['input'], b'test-secret\ntest-secret\n')
            self.assertEqual(kwargs['stdout'], runtime.subprocess.DEVNULL)

    def test_command_errors_suppressed(self):
        with patch.object(runtime.subprocess, 'run') as run:
            run.return_value.returncode = 1
            with self.assertRaisesRegex(RuntimeError, '^subprocess failed$'):
                runtime.run(['false'])

    def test_connack_network_error_is_not_auth_denial(self):
        with patch.object(runtime.ssl, 'create_default_context'), \
                patch.object(runtime.socket, 'create_connection', side_effect=TimeoutError), self.assertRaises(TimeoutError):
            runtime.mqtt_connection(1, '/missing-ca', 'test', 'password', False)


class WorkflowGuards(unittest.TestCase):
    def test_ci_keeps_baseline_and_runs_real_runtime(self):
        workflow = (runtime.ROOT / '.github/workflows/ci.yml').read_text()
        import re
        jobs = re.findall(r'^  ([a-z-]+):\n    runs-on:', workflow, re.MULTILINE)
        self.assertEqual(set(jobs), {'lint-and-test', 'cross-compile', 'migration-check',
                                   'docker-build', 'authorization-integration',
                                   'provisioning-integration', 'auth-integration',
                                   'mosquitto-runtime-integration'})
        job = workflow.split('  mosquitto-runtime-integration:', 1)[1]
        self.assertIn('sh scripts/test-stage2-mosquitto-runtime.sh', job)
        self.assertIn("-p 'test_stage2_mosquitto_runtime.py'", job)
        self.assertIn('go-version:', job)
        self.assertNotIn('continue-on-error', job)
        self.assertNotIn('|| true', job)
        for target in ('auth-init', 'reloader', 'backend'):
            self.assertIn('target: ' + target, workflow)


if __name__ == '__main__':
    unittest.main()
