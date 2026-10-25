"""Runner guards only; the actual parent-pair assertions run on native PG."""
from pathlib import Path
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stage5_intent_fixture as runner


class HardeningParentScopeTests(unittest.TestCase):
    def test_negative_case_keeps_enforcement_and_checks_rollback(self):
        fixture = mock.Mock()
        fixture.sql.side_effect = [
            (3, b'expected 9 hardening constraints, found 8'), (0, b'')]
        runner.verify_hardening_parent_scope(fixture)
        mutation, verification = fixture.sql.call_args_list
        sql = mutation.args[0]
        self.assertFalse(mutation.kwargs['expect'])
        self.assertIn(b'BEGIN;', sql)
        self.assertIn(b'RENAME CONSTRAINT', sql)
        self.assertIn(b'public.p3_guard_wrong_parent', sql)
        self.assertNotIn(b'DROP CONSTRAINT', sql)
        self.assertIn(b'Hardening negative check did not roll back', verification.args[0])

    def test_unrelated_sql_error_is_not_an_expected_refusal(self):
        fixture = mock.Mock()
        fixture.sql.return_value = (3, b'unrelated SQL failure')
        with self.assertRaisesRegex(RuntimeError, 'did not reject missing parent pair'):
            runner.verify_hardening_parent_scope(fixture)
        self.assertEqual(fixture.sql.call_count, 1)


if __name__ == '__main__':
    unittest.main()
