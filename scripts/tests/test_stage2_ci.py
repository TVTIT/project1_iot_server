"""Fail-closed CI evidence guards (no Docker required)."""
import re
import unittest
from pathlib import Path

import stage2_ci


def extract_job_section(workflow_text: str, job_id: str) -> str:
    match = re.search(r'^  ' + re.escape(job_id) + r':(?:[^\n]*\n)(.*?)(?=^  [a-z0-9-]+:|^\S|\Z)',
                      workflow_text, re.MULTILINE | re.DOTALL)
    if not match:
        raise AssertionError(f"job '{job_id}' not found in workflow")
    return match.group(0)


def assert_stage2_e2e_contract(job: str) -> None:
    marker = 'legacy-execution-deferred: stage2-e2e'
    if 'continue-on-error' in job:
        raise AssertionError("job must not use continue-on-error")
    if '|| true' in job:
        raise AssertionError("job must not suppress failure with || true")
    if re.search(r'(?m)^\s*if:\s*false\b', job):
        raise AssertionError("job must not disable steps with if: false")
    if 'STAGE2_SKIP_UNIT_GUARDS' in job:
        raise AssertionError("job must not bypass unit guards with STAGE2_SKIP_UNIT_GUARDS")

    if 'test_stage2_ci.py' not in job:
        raise AssertionError("job must retain test_stage2_ci.py harness tests")
    if 'test_stage2_ci_functional.py' not in job or 'test_stage2_response_loss.py' not in job:
        raise AssertionError("job must retain functional and response loss harness tests")

    if marker in job:
        run_blocks = re.findall(r'(?m)^\s*run:\s*\|?\n(.*?)(?=\n\s*(?:uses:|name:|env:|with:|if:|id:|\Z))',
                                job, re.DOTALL)
        step_found = False
        for block in run_blocks:
            active_lines = [l for l in block.splitlines() if not l.strip().startswith('#')]
            active_text = '\n'.join(active_lines)
            if marker in active_text and ('GITHUB_STEP_SUMMARY' in active_text or '$GITHUB_STEP_SUMMARY' in active_text):
                if re.search(r'>>\s*["\']?\$?GITHUB_STEP_SUMMARY["\']?', active_text):
                    step_found = True
                    text_lower = active_text.lower()
                    has_omitted = any(k in text_lower for k in ('omitted', 'legacy e2e', 'stage2_ci native'))
                    has_unresolved = any(k in text_lower for k in ('unresolved', 'failure', 'exit 2', 'exit2', 'fault-lost-ack'))
                    has_replacement = any(k in text_lower for k in ('replacement', 'native-compose-smoke', 'dynsec-native-integration'))
                    if not (has_omitted and has_unresolved and has_replacement):
                        raise AssertionError("deferred notice must explain omitted native scenario, known unresolved failure, and replacement job reference")
                    break
        if not step_found:
            raise AssertionError("deferred marker must be in an executable notice step writing to $GITHUB_STEP_SUMMARY, not a comment")

        if 'python3 scripts/tests/stage2_ci.py' in job:
            raise AssertionError("deferred mode must not execute native runner scripts/tests/stage2_ci.py")
        if 'stage2_ci.py --cleanup' in job:
            raise AssertionError("deferred mode must not retain native cleanup step")

        if '/run-*/safe/report.json' in job or 'stage2-e2e-safe-reports' in job:
            raise AssertionError("deferred mode must not retain native report artifact upload")

        header = job.split('steps:', 1)[0]
        name_match = re.search(r'(?m)^\s*name:\s*(.+)$', header)
        if name_match:
            label = name_match.group(1).strip().strip("'\"")
            if re.search(r'\b(native\s+)?pass\b', label, re.IGNORECASE):
                raise AssertionError(f"misleading job display label claiming pass: {label}")
    else:
        if 'timeout-minutes: 30' not in job:
            raise AssertionError("intact native mode requires timeout-minutes: 30")
        if 'stage2_ci.py --cleanup' not in job:
            raise AssertionError("intact native mode requires stage2_ci.py --cleanup")
        if job.count('python3 scripts/tests/stage2_ci.py\n') != 2:
            raise AssertionError("intact native mode requires two python3 scripts/tests/stage2_ci.py invocations")
        if '/run-*/safe/report.json' not in job:
            raise AssertionError("intact native mode requires /run-*/safe/report.json report upload")
        if 'stage2-e2e-safe-reports' not in job:
            raise AssertionError("intact native mode requires stage2-e2e-safe-reports artifact name")


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
        source = (Path(__file__).resolve().parents[2] / '.github/workflows/ci.yml').read_text()
        job = extract_job_section(source, 'stage2-e2e')
        assert_stage2_e2e_contract(job)

    def test_deferred_workflow_contract_and_mutations(self):
        valid_deferred = '''  stage2-e2e:
    name: Legacy E2E contracts
    runs-on: ubuntu-latest
    timeout-minutes: 15
    defaults:
      run:
        working-directory: .
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v7
        with:
          go-version: '1.27.1'
          cache-dependency-path: src/go.sum
      - name: Check prerequisites
        run: |
          mkdir -p /tmp/opencode
          python3 --version
          go version
      - name: Test harness
        run: |
          python3 -m unittest discover -s scripts/tests -p 'test_stage2_ci.py'
          python3 -m unittest discover -s scripts/tests -p 'test_stage2_ci_functional.py'
          python3 -m unittest discover -s scripts/tests -p 'test_stage2_response_loss.py'
      - name: Legacy execution deferred notice
        run: |
          echo "legacy-execution-deferred: stage2-e2e" >> "$GITHUB_STEP_SUMMARY"
          echo "legacy-execution-deferred: stage2-e2e"
          echo "Omitted native scenario: legacy E2E supervisor runs."
          echo "Known unresolved failure: historical fault-lost-ack exit 2 root cause unknown."
          echo "Current replacement job: native-compose-smoke."
'''
        # 1. Valid deferred mode must pass
        assert_stage2_e2e_contract(valid_deferred)

        # 2. Silent deletion without deferred notice must fail
        silent_deletion = valid_deferred.replace('      - name: Legacy execution deferred notice\n'
                                                 '        run: |\n'
                                                 '          echo "legacy-execution-deferred: stage2-e2e" >> "$GITHUB_STEP_SUMMARY"\n'
                                                 '          echo "legacy-execution-deferred: stage2-e2e"\n'
                                                 '          echo "Omitted native scenario: legacy E2E supervisor runs."\n'
                                                 '          echo "Known unresolved failure: historical fault-lost-ack exit 2 root cause unknown."\n'
                                                 '          echo "Current replacement job: native-compose-smoke."\n', '')
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(silent_deletion)

        # 3. Marker present but unit tests removed must fail
        no_units = valid_deferred.replace("test_stage2_ci.py", "other_test.py")
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(no_units)

        # 4. Marker present but fake native report upload retained must fail
        fake_upload = valid_deferred + '''      - name: Upload safe reports
        uses: actions/upload-artifact@v4
        with:
          name: stage2-e2e-safe-reports
          path: /tmp/opencode/stage2-ci-run/safe/report.json
'''
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(fake_upload)

        # 5. Native commands accidentally retained with marker must fail
        retained_native = valid_deferred + '''      - name: Legacy E2E
        run: python3 scripts/tests/stage2_ci.py
'''
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(retained_native)

        # 6. continue-on-error must fail
        with_continue = valid_deferred.replace('    steps:', '    continue-on-error: true\n    steps:')
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(with_continue)

        # 7. if: false must fail
        with_if_false = valid_deferred.replace('      - name: Test harness', '      - name: Test harness\n        if: false')
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(with_if_false)

        # 8. || true must fail
        with_or_true = valid_deferred.replace("test_stage2_response_loss.py'", "test_stage2_response_loss.py' || true")
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(with_or_true)

        # 9. Comment-only marker without executable write to GITHUB_STEP_SUMMARY must fail
        comment_only = valid_deferred.replace('echo "legacy-execution-deferred: stage2-e2e" >> "$GITHUB_STEP_SUMMARY"',
                                              '# legacy-execution-deferred: stage2-e2e')
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(comment_only)

        # 10. Misleading display label claiming native pass must fail
        misleading_label = valid_deferred.replace('name: Legacy E2E contracts', 'name: Native E2E Integration PASS')
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(misleading_label)

        # 11. Notice missing required explanation must fail
        incomplete_notice = valid_deferred.replace('echo "Known unresolved failure: historical fault-lost-ack exit 2 root cause unknown."', '')
        with self.assertRaises(AssertionError):
            assert_stage2_e2e_contract(incomplete_notice)
