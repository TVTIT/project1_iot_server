"""Fail-closed CI evidence guards (no Docker required)."""
import unittest
import stage2_ci


class EvidenceGuards(unittest.TestCase):
    def log(self):
        return '\n'.join([f'SCENARIO E{i:02}: PASS' for i in range(1, 28)] +
                         [f"Selector '{key}': PASS" for key in stage2_ci.SELECTORS] +
                         ['STAGE 2 E2E EXECUTION SUMMARY',
                          'Post-cleanup known-secret scan passed'])

    def test_complete(self):
        self.assertTrue(stage2_ci.complete(self.log(), 0))

    def test_missing_duplicate_skip_exit_cleanup_summary(self):
        for text, code in [(self.log().replace('SCENARIO E22: PASS', ''), 0),
                           (self.log() + '\nSCENARIO E22: PASS', 0),
                           (self.log().replace('E22: PASS', 'E22: NOT RUN'), 0),
                           (self.log(), 23),
                           (self.log().replace('Post-cleanup known-secret scan passed', ''), 0),
                           (self.log() + '\nSTAGE 2 E2E EXECUTION SUMMARY', 0)]:
            self.assertFalse(stage2_ci.complete(text, code))

    def test_failure_evidence_excludes_raw_output(self):
        secret = 'raw-password-or-response'
        report = stage2_ci.evidence(secret, 1)
        self.assertNotIn(secret, str(report))
        self.assertFalse(report['complete'])

    def test_workflow_contract(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
        job = source.split('  stage2-e2e:', 1)[1].split('  lint-and-test:', 1)[0]
        self.assertIn('timeout-minutes: 30', job)
        self.assertIn('stage2_ci.py --cleanup', job)
        self.assertEqual(job.count('python3 scripts/tests/stage2_ci.py\n'), 2)
        self.assertIn('/run-*/safe/report.json', job)
        self.assertNotIn('STAGE2_SKIP_UNIT_GUARDS', job)
        self.assertNotIn('continue-on-error', job)
