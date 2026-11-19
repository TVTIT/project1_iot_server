"""Narrow guards for the opt-in Stage 6 P3 profile and shared CI lane."""
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class Stage6P3SelectorTests(unittest.TestCase):
    def test_stage6_selector_without_p3_is_rejected_before_fixture_creation(self):
        target = Path('/var/tmp/opencode/stage6-r81-invalid-selector-must-not-exist')
        self.assertFalse(target.exists())
        result = subprocess.run(
            [sys.executable, str(ROOT / 'scripts/tests/native_compose_smoke.py'),
             '--prepare', str(target), '--stage6-p3'],
            cwd=ROOT, capture_output=True, text=True, check=False, timeout=30)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('--stage6-p3 requires baseline --paper-p3', result.stderr)
        self.assertFalse(target.exists())

    def test_stage6_selector_rejects_conflicting_p5_profile(self):
        target = Path('/var/tmp/opencode/stage6-r81-conflicting-selector-must-not-exist')
        result = subprocess.run(
            [sys.executable, str(ROOT / 'scripts/tests/native_compose_smoke.py'),
             '--prepare', str(target), '--paper-p3', '--stage6-p3',
             '--p5a-control-regression'],
            cwd=ROOT, capture_output=True, text=True, check=False, timeout=30)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('--stage6-p3 requires baseline --paper-p3', result.stderr)
        self.assertFalse(target.exists())

    def test_baseline_wrapper_and_full_http_are_not_redefined(self):
        wrapper = (ROOT / 'scripts/test-paper-p3.sh').read_text()
        smoke = (ROOT / 'scripts/tests/native_compose_smoke.py').read_text()
        self.assertIn('--paper-p3\n', wrapper)
        self.assertIn('if [ "$STAGE6" -eq 1 ]; then', wrapper)
        self.assertIn("if args.stage6_p3 and (not args.paper_p3", smoke)
        self.assertIn('args.full_http', smoke)

    def test_stage6_uses_one_existing_control_job_and_safe_reports(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        control_job = workflow.split('  stage5-control-integration:', 1)[1].split(
            '\n  native-compose-smoke:', 1)[0]
        self.assertEqual(workflow.count('  stage5-control-integration:'), 1)
        self.assertIn('sh scripts/test-paper-p3.sh "$STAGE5_CONTROL_RUN_DIR"', control_job)
        self.assertIn('sh scripts/test-paper-p3.sh --stage6 "$STAGE6_CONTROL_RUN_DIR"', control_job)
        self.assertIn('timeout --signal=TERM --kill-after=90s 1200s python3 scripts/tests/stage5_mqtt_fixture.py', control_job)
        self.assertNotIn('runner.log\n', control_job[control_job.find('Upload'):])
        self.assertIn('stage6_profile', control_job)


if __name__ == '__main__':
    unittest.main()
