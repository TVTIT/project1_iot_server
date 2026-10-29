import contextlib
import io
import json
import os
from pathlib import Path
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
            return mock.Mock(returncode=0)

        with mock.patch.object(driver.subprocess, 'run', side_effect=fake_run):
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
            return mock.Mock(returncode=1)

        with mock.patch.object(driver.subprocess, 'run', side_effect=fail_second):
            self.assertEqual(self.invoke('--root', str(root), '--arms', 'optimized', 'polling',
                'shaper_off', '--run'), 1)
        summary = json.loads((root / 'driver-run.json').read_text())
        self.assertEqual(summary['status'], 'FAIL')
        self.assertEqual(len(calls), 1)
        self.assertNotEqual(summary['status'], 'RUNS_COMPLETE')

    def test_official_prepare_only_does_not_invoke_native_runner(self):
        root = self.base / 'prepare'
        with mock.patch.object(driver.subprocess, 'run', side_effect=AssertionError('native invoked')):
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
            'context': {k: context[k] for k in ('sha256', 'seed', 'anchor')},
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
            return mock.Mock(returncode=0)

        with mock.patch.object(driver.subprocess, 'run', side_effect=fake_run):
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
        before = context_path.read_bytes()
        with mock.patch.object(driver.subprocess, 'run', return_value=mock.Mock(returncode=0)):
            # Preparation reuses the existing byte-identical context; no native invocation.
            self.assertEqual(self.invoke('--root', str(root), '--context', str(context_path)), 0)
        self.assertEqual(context_path.read_bytes(), before)


if __name__ == '__main__':
    unittest.main()
