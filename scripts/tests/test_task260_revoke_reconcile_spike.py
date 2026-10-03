#!/usr/bin/env python3
"""Test-first metadata-only contracts; real PostgreSQL is tested by harness."""
import unittest
from task26_revoke_cases import intent_sql, resolve_commit, retry_delays


class Contracts(unittest.TestCase):
    def test_intent_identity_validation(self):
        with self.assertRaises(ValueError):
            intent_sql("bad'", 'A')
        with self.assertRaises(ValueError):
            intent_sql('a' * 32, '../security')

    def test_commit_unknown_never_means_not_performed(self):
        self.assertEqual(resolve_commit(None), 'unknown')
        self.assertEqual(resolve_commit(''), 'absent_observed')
        self.assertEqual(resolve_commit('pending'), 'committed')
        with self.assertRaises(ValueError):
            resolve_commit('garbage')

    def test_intent_transaction_is_metadata_only(self):
        sql = intent_sql('a' * 32, 'A')
        self.assertIn('BEGIN;', sql)
        self.assertIn('COMMIT;', sql)
        self.assertIn('ON CONFLICT', sql)
        self.assertIn('identity conflict', sql)
        self.assertNotIn('password', sql.lower())
        self.assertNotIn('hash', sql.lower())

    def test_retry_is_bounded(self):
        self.assertEqual(len(retry_delays()), 3)
        self.assertTrue(all(0 < delay <= .4 for delay in retry_delays()))


if __name__ == '__main__':
    unittest.main()
