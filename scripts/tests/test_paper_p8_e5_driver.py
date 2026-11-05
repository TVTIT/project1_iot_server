import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import paper_p8_e5 as driver


class E5RepeatDriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)

    def invoke(self, *args):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return driver.main(list(args))

    def test_child_supervision_sends_interrupt_then_joins_process_group(self):
        marker = self.base / 'stopped'
        child = ("import signal,time,pathlib; "
                 f"signal.signal(signal.SIGINT, lambda *_: (pathlib.Path({str(marker)!r}).write_text('stopped'), exit(0))); "
                 "time.sleep(30)")
        outcome = driver._supervise_child([sys.executable, '-c', child], cwd=ROOT,
            stdout=subprocess.DEVNULL, timeout=0.1, cleanup_grace=2)
        self.assertEqual(outcome['reason'], 'timeout')
        self.assertEqual(outcome['cleanup']['result'], 'joined_after_interrupt')
        self.assertTrue(marker.is_file())

    def test_child_supervision_escalates_when_child_ignores_interrupt(self):
        child = ("import signal,time; signal.signal(signal.SIGINT, signal.SIG_IGN); "
                 "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)")
        outcome = driver._supervise_child([sys.executable, '-c', child], cwd=ROOT,
            stdout=subprocess.DEVNULL, timeout=0.1, cleanup_grace=0.1)
        self.assertEqual(outcome['reason'], 'timeout')
        self.assertEqual(outcome['cleanup']['result'], 'joined_after_kill')

    def test_repeated_matrix_pairs_each_repeat_to_its_own_reference(self):
        root = self.base / 'matrix'
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(list(argv))
            arm = argv[argv.index('--paper-p8-e5') + 1]
            context_path = Path(argv[argv.index('--paper-p8-e5-context') + 1])
            context = json.loads(context_path.read_text())
            prepare = Path(argv[argv.index('--prepare') + 1])
            artifact = prepare / 'artifacts' / f'p8-e5-{arm}.json'
            artifact.parent.mkdir(parents=True)
            artifact.write_text(json.dumps({'status': 'PASS', 'arm': arm,
                'cleanup_verified': True, 'full_p8_accepted': False,
                'context': {k: context[k] for k in ('sha256', 'seed', 'anchor')},
                'image_ids': {'backend': 'backend-image', 'gateway': 'gateway-image'}}))
            artifact.chmod(0o600)
            return {'returncode': 0, 'reason': None, 'cleanup': {'result': 'normal_exit'}}

        with mock.patch.object(driver, '_supervise_child', side_effect=fake_run):
            self.assertEqual(self.invoke('--root', str(root), '--mode', 'official', '--repeat', '3',
                '--arms', 'shaper_off', 'optimized', 'polling', '--run'), 0)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'RUNS_COMPLETE')
        self.assertFalse(summary['execution_policy']['cooldown_executed'])
        self.assertEqual(len(calls), 9)
        by_repeat = {}
        for run in summary['native_runs']:
            by_repeat.setdefault(run['repeat'], []).append(run)
        seeds = []
        for repeat, runs in by_repeat.items():
            self.assertEqual(runs[0]['arm'], 'optimized')
            seed = json.loads(Path(summary['contexts'][repeat - 1]).read_text())['seed']
            seeds.append(seed)
            self.assertEqual(len({json.loads(Path(r['argv'][r['argv'].index('--paper-p8-e5-context') + 1]).read_text())['sha256']
                                  for r in runs}), 1)
            optimized_artifact = Path(runs[0]['artifact'])
            for run in runs[1:]:
                ref_arg = run['argv'][run['argv'].index('--paper-p8-e5-reference') + 1]
                self.assertEqual(Path(ref_arg), optimized_artifact)
                self.assertEqual(json.loads(Path(ref_arg).read_text())['context']['seed'], seed)
        self.assertEqual(seeds, [805, 806, 807])
        self.assertEqual([r['arm'] for r in by_repeat[1]][1:],
                         [item['arm'] for item in summary['plans'][0]['plan'][1:]])

    def test_representative_preset_three_repeat_offline_propagation(self):
        root = self.base / 'representative-dry-run'
        self.assertEqual(self.invoke('--root', str(root), '--mode', 'official', '--repeat', '3',
            '--seed', '901', '--workload-preset', 'representative',
            '--arms', 'shaper_off', 'optimized', 'polling', '--prepare-only'), 0)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'PREPARED')
        self.assertEqual(summary['native_runs'], [])
        self.assertEqual([plan['plan'][0]['arm'] for plan in summary['plans']], ['optimized'] * 3)
        for i, context_path in enumerate(summary['contexts']):
            context = json.loads(Path(context_path).read_text())
            self.assertEqual(context['seed'], 901 + i)
            self.assertEqual(context['workload_selection']['profile'], 'representative')
            self.assertEqual(context['workload_selection']['telemetry_messages'], 8)
            self.assertEqual(context['workload_selection']['warm_replay_prefix'], 2)
            self.assertEqual(context['workload_selection_sha256'],
                driver.e5.selection_sha256(context['workload_selection']))
            rows = [json.loads(line) for line in Path(context['dataset_path']).read_text().splitlines()]
            self.assertEqual(len(rows), 8)
            self.assertEqual(summary['workload_selections'][i], context['workload_selection'])

    def test_reference_rejects_different_workload_selection_digest(self):
        context_root = self.base / 'mismatch-context'
        context_root.mkdir(mode=0o700)
        context_root.chmod(0o700)
        _, context_path = driver.prepare_repeat(context_root, 1, 902, 'representative')
        context = json.loads(context_path.read_text())
        reference = self.base / 'mismatch-reference.json'
        reference.write_text(json.dumps({'status': 'PASS', 'arm': 'optimized',
            'cleanup_verified': True, 'full_p8_accepted': False,
            'context': {'sha256': context['sha256'], 'seed': context['seed'],
                'anchor': context['anchor'], 'workload_selection_sha256': '0' * 64},
            'image_ids': {'backend': 'sha256:' + 'a' * 64, 'gateway': 'sha256:' + 'b' * 64}}))
        reference.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'reference_context_seed_mismatch'):
            driver._validate_reference(reference, context_path, 902)

    def test_unknown_workload_preset_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.invoke('--root', str(self.base / 'bad-preset'), '--workload-preset', 'large')
        self.assertFalse((self.base / 'bad-preset').exists())

    def test_failure_aborts_matrix_and_never_claims_complete(self):
        root = self.base / 'failed'
        calls = []

        def fail_second(argv, **kwargs):
            calls.append(argv)
            arm = argv[argv.index('--paper-p8-e5') + 1]
            prepare = Path(argv[argv.index('--prepare') + 1])
            artifact = prepare / 'artifacts' / f'p8-e5-{arm}.json'
            artifact.parent.mkdir(parents=True)
            artifact.write_text(json.dumps({'status': 'FAIL', 'cleanup_verified': True,
                'full_p8_accepted': False}))
            artifact.chmod(0o600)
            return {'returncode': 1, 'reason': None, 'cleanup': {'result': 'normal_exit'}}

        with mock.patch.object(driver, '_supervise_child', side_effect=fail_second):
            self.assertEqual(self.invoke('--root', str(root), '--arms', 'optimized', 'polling',
                'shaper_off', '--run'), 1)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'FAIL')
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(summary['status'], 'RUNS_COMPLETE')

    def test_child_timeout_writes_partial_summary_preserving_completed_arm_and_cleanup(self):
        root = self.base / 'timed-out'
        calls = 0
        def timeout_second(argv, **kwargs):
            nonlocal calls
            calls += 1
            arm = argv[argv.index('--paper-p8-e5') + 1]
            artifact = Path(argv[argv.index('--prepare') + 1]) / 'artifacts' / f'p8-e5-{arm}.json'
            artifact.parent.mkdir(parents=True)
            artifact.write_text(json.dumps({'status': 'PASS' if calls == 1 else 'PARTIAL',
                'cleanup_verified': True, 'full_p8_accepted': False}))
            artifact.chmod(0o600)
            if calls == 2:
                return {'returncode': None, 'reason': 'timeout', 'cleanup': {'result': 'mocked_timeout'}}
            return {'returncode': 0, 'reason': None, 'cleanup': {'result': 'normal_exit'}}
        with mock.patch.object(driver, '_supervise_child', side_effect=timeout_second):
            self.assertEqual(self.invoke('--root', str(root), '--child-timeout-seconds', '1',
                '--arms', 'optimized', 'polling', '--run'), 124)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'PARTIAL')
        self.assertEqual(summary['primary_failure']['reason'], 'child_timeout')
        self.assertEqual([run['arm'] for run in summary['native_runs']], ['optimized', 'polling'])
        self.assertEqual(summary['native_runs'][0]['cleanup_verified'], True)
        self.assertEqual(summary['native_runs'][1]['cleanup_verified'], False)
        self.assertEqual(summary['native_runs'][1]['cleanup']['result'], 'mocked_timeout')
        self.assertEqual(summary['timeout_budget']['declared_total_seconds'], 122)

    def test_keyboard_interrupt_writes_partial_summary_and_retains_primary_reason(self):
        root = self.base / 'interrupted'
        def interrupt(argv, **kwargs):
            artifact = Path(argv[argv.index('--prepare') + 1]) / 'artifacts/p8-e5-optimized.json'
            artifact.parent.mkdir(parents=True)
            artifact.write_text(json.dumps({'status':'PARTIAL','cleanup_verified':True,
                'full_p8_accepted':False}))
            artifact.chmod(0o600)
            return {'returncode': None, 'reason': 'interrupted', 'cleanup': {'result': 'mocked_interrupt'}}
        with mock.patch.object(driver, '_supervise_child', side_effect=interrupt):
            self.assertEqual(self.invoke('--root', str(root), '--child-timeout-seconds', '1',
                '--arm', 'optimized', '--run'), 130)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'PARTIAL')
        self.assertEqual(summary['primary_failure']['reason'], 'interrupted')
        self.assertFalse(summary['native_runs'][0]['cleanup_verified'])
        self.assertEqual(summary['native_runs'][0]['cleanup']['result'], 'mocked_interrupt')

    def test_official_prepare_only_does_not_invoke_native_runner(self):
        root = self.base / 'prepare'
        with mock.patch.object(driver, '_supervise_child', side_effect=AssertionError('native invoked')):
            self.assertEqual(self.invoke('--root', str(root), '--mode', 'official', '--repeat', '3',
                '--arms', 'optimized', 'polling'), 0)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'PREPARED')
        self.assertEqual(len(summary['contexts']), 3)

    def test_ablation_only_requires_reusable_context_and_reference(self):
        root = self.base / 'invalid'
        with self.assertRaises(SystemExit):
            self.invoke('--root', str(root), '--arm', 'polling', '--run')
        self.assertFalse(root.exists())

    def test_external_ablation_uses_matching_context_and_reference(self):
        context_root = self.base / 'external-context-root'
        context_root.mkdir(mode=0o700)
        context_root.chmod(0o700)
        _, context_path = driver.prepare_repeat(context_root, 1, 805)
        context = json.loads(context_path.read_text())
        reference = self.base / 'reference.json'
        reference.write_text(json.dumps({'status': 'PASS', 'arm': 'optimized',
            'cleanup_verified': True, 'full_p8_accepted': False,
            'context': {k: context[k] for k in ('sha256', 'seed', 'anchor', 'workload_selection_sha256')},
            'image_ids': {'backend': 'sha256:backend', 'gateway': 'sha256:gateway'}}))
        reference.chmod(0o600)
        root = self.base / 'external-run'
        calls = []

        def fake_run(argv, **kwargs):
            calls.append(argv)
            artifact = Path(argv[argv.index('--prepare') + 1]) / 'artifacts' / 'p8-e5-polling.json'
            artifact.parent.mkdir(parents=True)
            artifact.write_text(json.dumps({'status': 'PASS', 'cleanup_verified': True,
                'full_p8_accepted': False}))
            artifact.chmod(0o600)
            return {'returncode': 0, 'reason': None, 'cleanup': {'result': 'normal_exit'}}

        with mock.patch.object(driver, '_supervise_child', side_effect=fake_run):
            self.assertEqual(self.invoke('--root', str(root), '--context', str(context_path),
                '--reference', str(reference), '--arm', 'polling', '--run'), 0)
        self.assertEqual(calls[0][calls[0].index('--paper-p8-e5-context') + 1], str(context_path))
        self.assertEqual(calls[0][calls[0].index('--paper-p8-e5-reference') + 1], str(reference))

    def test_optimized_matrix_rejects_external_reference_before_root_creation(self):
        reference = self.base / 'unused.json'
        reference.write_text('{}')
        reference.chmod(0o600)
        root = self.base / 'conflict'
        with self.assertRaises(SystemExit):
            self.invoke('--root', str(root), '--context', str(self.base / 'context'),
                '--reference', str(reference), '--arms', 'optimized', 'polling', '--run')
        self.assertFalse(root.exists())

    def test_context_load_does_not_modify_shared_inputs(self):
        root = self.base / 'pilot'
        context_root = self.base / 'seed-context'
        context_root.mkdir(mode=0o700)
        context_root.chmod(0o700)
        _, context_path = driver.prepare_repeat(context_root, 1, 805)
        context = json.loads(context_path.read_text())
        self.assertEqual(context['workload_selection'], driver.e5.workload_preset('pilot'))
        self.assertEqual(len(Path(context['dataset_path']).read_text().splitlines()), 20)
        before = context_path.read_bytes()
        with mock.patch.object(driver, '_supervise_child', return_value={'returncode': 0,
                'reason': None, 'cleanup': {'result': 'normal_exit'}}):
            # Preparation reuses the existing byte-identical context; no native invocation.
            self.assertEqual(self.invoke('--root', str(root), '--context', str(context_path)), 0)
        self.assertEqual(context_path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
