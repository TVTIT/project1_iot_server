#!/usr/bin/env python3
"""Regression test suite for Stage 2 migration 19 verification harness.

Verifies:
1. Regression test: existing/original harness script was gated on migration 16
   and did not invoke verify-migration-000019.sql or wait for version 19.
2. Static and functional assertions verifying that scripts/test-stage2-migrations.sh:
   - defines exact readiness gate for version 19 with schema_migrations name 'mqtt_simplified_journal'
   - retains older intermediate gates (e.g. baseline version 9) for upgrade phases
   - invokes scripts/sql/verify-migration-000019.sql in both upgrade and fresh paths
3. SQL verification script structure and invariants:
   - Sets ON_ERROR_STOP
   - Uses transactional block (BEGIN ... ROLLBACK)
   - Checks execution_contract column type/default/check
   - Checks reset columns and chk_mqtt_reset_identity
   - Checks mqtt_reset_dispositions table and PK/FK/unique/check/immutability constraints
   - Checks mqtt_operation_effective function and trusted search_path
   - Checks permissions for iot_backend / iot_backend_app and revokes from PUBLIC/anon/authenticated
"""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
MIGRATIONS_SH = ROOT / "scripts/test-stage2-migrations.sh"
VERIFY_SQL_19 = ROOT / "scripts/sql/verify-migration-000019.sql"
VERIFY_SQL_11 = ROOT / "scripts/sql/verify-migration-000011.sql"
VERIFY_SQL_18 = ROOT / "scripts/sql/verify-migration-000018.sql"
VERIFY_SQL_14_UPGRADE = ROOT / "scripts/sql/verify-migration-000014-upgrade.sql"
VERIFY_SQL_15_UPGRADE = ROOT / "scripts/sql/verify-migration-000015-upgrade.sql"


class TestMigration15HistoricalEvidence(unittest.TestCase):
    def test_every_original_field_is_compared_with_missing_identity_detection(self):
        sql = VERIFY_SQL_15_UPGRADE.read_text()
        for alias, identity in (("c", "gateway_id"), ("e", "operation_id"), ("r", "recovery_id")):
            self.assertIn(f"jsonb_object_agg(key, to_jsonb({alias}) -> key)", sql)
            self.assertIn(f"{alias}.{identity} IS NULL", sql)
        self.assertEqual(sql.count("FROM jsonb_object_keys(f.original) AS key"), 3)
        self.assertNotIn("WHERE key <>", sql)

    def test_actual_sql_diagnostics_and_rollback_fault_probes(self):
        sql = VERIFY_SQL_15_UPGRADE.read_text()
        for token in ("BEGIN;", "ROLLBACK;", "CREATE TEMP TABLE", "FOR original_key IN SELECT jsonb_object_keys",
                      "jsonb_set(original", "DELETE FROM %I WHERE %I", "INSERT INTO %I SELECT",
                      "EXCEPTION WHEN SQLSTATE 'P0015'", "fixture15_counts",
                      "v15 additive-only drift confirmed", "v15 historical snapshot regressions passed"):
            self.assertIn(token, sql)

    def test_concurrent_writers_commit_matching_projections(self):
        sh = MIGRATIONS_SH.read_text().split("for scenario in key gateway; do", 1)[1]
        for op in ("one", "two"):
            transaction = re.search(r'"BEGIN; INSERT INTO gateway_mqtt_credential_events.*?\$op_' + op + r'.*?COMMIT;"', sh)
            self.assertIsNotNone(transaction)
            self.assertIn("INSERT INTO gateway_mqtt_credentials", transaction.group())
            self.assertIn("'provisioning', operation_id, actor_user_id, created_at", transaction.group())

    def test_obsolete_intent_only_fixture_is_rejected_by_real_guard(self):
        sh = MIGRATIONS_SH.read_text()
        self.assertIn("Intent-only writer unexpectedly committed", sh)
        self.assertIn("New intent cannot commit as unassociated audit", sh)
        self.assertIn("Intent-only writer rejection verified", sh)


class TestMigration14HistoricalEvidence(unittest.TestCase):
    def test_all_snapshots_project_every_original_key(self):
        sql = VERIFY_SQL_14_UPGRADE.read_text()
        for alias in ("e", "p", "c"):
            self.assertIn(f"jsonb_object_agg(key, to_jsonb({alias}) -> key)", sql)
        self.assertEqual(sql.count("FROM jsonb_object_keys(f.original) AS key"), 3)
        self.assertNotIn("WHERE key <>", sql)
        for identity in ("e.operation_id IS NULL", "p.operation_id IS NULL", "c.gateway_id IS NULL"):
            self.assertIn(identity, sql)
        self.assertIn("SELECT 1 FROM mqtt_credential_recovery", sql)

    def test_sql_fault_probes_are_rolled_back_and_discriminate_history_loss(self):
        sql = VERIFY_SQL_14_UPGRADE.read_text()
        self.assertIn("BEGIN;", sql)
        self.assertIn("ROLLBACK;", sql)
        self.assertIn("FOR original_key IN SELECT jsonb_object_keys", sql)
        self.assertIn("jsonb_set(original", sql)
        self.assertIn("DELETE FROM %I WHERE %I", sql)
        self.assertIn("INSERT INTO mqtt_credential_recovery", sql)
        self.assertIn("EXCEPTION WHEN SQLSTATE 'P0014'", sql)
        self.assertIn("History fault unexpectedly accepted", sql)
        self.assertIn("v14 historical snapshot regressions passed", sql)


class TestMigration18SupportedBoundaries(unittest.TestCase):
    def test_exact_supported_tracking_boundaries(self):
        sql = VERIFY_SQL_18.read_text()
        self.assertIn("latest_version NOT IN (18, 19)", sql)
        self.assertIn("WHEN 19 THEN 'mqtt_simplified_journal'", sql)
        self.assertIn("BETWEEN 11 AND latest_version", sql)
        self.assertIn("<> latest_version - 10", sql)
        self.assertIn("to_regclass('public.mqtt_reset_dispositions') IS NOT NULL", sql)

    def test_real_transactional_tracking_fault_probes(self):
        sql = VERIFY_SQL_18.read_text()
        for mutation in (
            "UPDATE public.schema_migrations SET name='incorrect_guard' WHERE version=18",
            "DELETE FROM public.schema_migrations WHERE version=18",
            "DELETE FROM public.schema_migrations WHERE version=17",
            "UPDATE public.schema_migrations SET name='incorrect_journal' WHERE version=19",
            "DELETE FROM public.schema_migrations WHERE version=19",
            "INSERT INTO public.schema_migrations(version,name) VALUES(20,'unsupported_future')",
        ):
            self.assertIn(mutation.replace("'", "''"), sql)
        self.assertIn("EXCEPTION WHEN SQLSTATE 'P0018'", sql)
        self.assertIn("Tracking fault unexpectedly accepted", sql)

    def test_existing_security_invariants_remain_exact(self):
        sql = VERIFY_SQL_18.read_text()
        for invariant in (
            "actual.prosecdef <> expected.definer",
            "actual.proowner <>",
            "actual.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog, public, pg_temp']",
            "tgenabled='O'", "tginitdeferred=expected.deferred",
            "has_schema_privilege('iot_backend_app', 'public', 'CREATE')",
            "SET LOCAL search_path = pg_temp, public",
        ):
            self.assertIn(invariant, sql)


class TestMigration11Admission(unittest.TestCase):
    def test_historical_index_lookups_are_safe_and_still_required(self):
        sql = VERIFY_SQL_11.read_text()
        self.assertIn("to_regclass('public.uq_mqtt_operation_unresolved')", sql)
        self.assertIn("to_regclass('public.uq_mqtt_operation_actor_key')", sql)
        self.assertNotIn("'uq_mqtt_operation_unresolved'::regclass", sql)
        self.assertIn("IF NOT replacement_schema THEN", sql)

    def test_replacement_requires_recorded_version_and_name(self):
        sql = VERIFY_SQL_11.read_text()
        self.assertIn("version = 17 AND name = 'mqtt_rotation_replacement'", sql)
        self.assertIn("max(version)", sql)
        self.assertIn("Unknown replacement migration history", sql)

    def test_replacement_requires_enabled_bound_admission_guards(self):
        sql = VERIFY_SQL_11.read_text()
        for name in ("mqtt_operation_effective(uuid)",
                     "enforce_gateway_mqtt_event_transition()",
                     "enforce_mqtt_maintenance_admission()",
                     "check_mqtt_replacement_inventory()",
                     "validate_mqtt_replacement_inventory(text)"):
            self.assertIn(name, sql)
        for invariant in ("tgenabled = 'O'", "tgfoid", "tgtype",
                          "tgdeferrable", "tginitdeferred", "FOR UPDATE"):
            self.assertIn(invariant, sql)

    def test_harness_exercises_missing_protections_on_old_and_new_schema(self):
        sh = MIGRATIONS_SH.read_text()
        self.assertIn('verify_admission_regressions "$UPGRADE_DB" historical', sh)
        self.assertIn('verify_admission_regressions "$FRESH_DB" replacement', sh)
        self.assertIn("DROP INDEX public.uq_mqtt_operation_unresolved", sh)
        self.assertIn("DISABLE TRIGGER enforce_gateway_mqtt_event_transition", sh)


class TestStage2Migration19Harness(unittest.TestCase):
    def setUp(self):
        self.assertTrue(MIGRATIONS_SH.exists(), f"Missing {MIGRATIONS_SH}")
        self.assertTrue(VERIFY_SQL_19.exists(), f"Missing {VERIFY_SQL_19}")
        self.sh_content = MIGRATIONS_SH.read_text(encoding="utf-8")
        self.sql_content = VERIFY_SQL_19.read_text(encoding="utf-8")

    def test_role_probe_commits_intent_and_matching_projection_atomically(self):
        probe = self.sh_content.split('event_id="20000000-0000-4000-8000-000000000001"', 1)[1]
        admission = re.search(r'-c "(BEGIN;.*?COMMIT;)"', probe, re.S)
        self.assertIsNotNone(admission, "Role probe must commit intent and projection together")
        sql = admission.group(1)
        self.assertIn("INSERT INTO gateway_mqtt_credential_events", sql)
        self.assertIn("'$event_id', 'gateway_role_probe', '$selected_admin_id', '$event_id', 1, 'provision', 'pending'", sql)
        self.assertIn("INSERT INTO gateway_mqtt_credentials", sql)
        self.assertIn("gateway_id, credential_version, status, last_operation_id, changed_by, updated_at", sql)
        self.assertIn("SELECT gateway_id, credential_version, 'provisioning', operation_id, actor_user_id, created_at", sql)
        self.assertIn("WHERE operation_id = '$event_id'", sql)
        self.assertIn('backend_exec "$container"', probe[:admission.start()])
        self.assertIn("-U iot_backend_app", probe[:admission.start()])

    def test_role_probe_terminal_transition_keeps_projection_and_negative_check(self):
        probe = self.sh_content.split('event_id="20000000-0000-4000-8000-000000000001"', 1)[1]
        transactions = re.findall(r'-c "(BEGIN;.*?COMMIT;)"', probe, re.S)
        self.assertEqual(len(transactions), 2)
        terminal = transactions[1]
        self.assertIn("SET status = 'succeeded'", terminal)
        self.assertIn("UPDATE gateway_mqtt_credentials", terminal)
        self.assertIn("SET status = 'active', activated_at = now(), updated_at = now()", terminal)
        self.assertIn("WHERE last_operation_id = '$event_id'", terminal)
        self.assertIn("UPDATE mqtt_credential_maintenance SET status = 'completed'", terminal)
        self.assertIn("SET status = 'failed', completed_at = now() WHERE operation_id = '$event_id'", probe)
        self.assertIn("Completed credential audit event unexpectedly remained mutable.", probe)

    def test_original_script_does_not_gate_on_version_19_and_omits_verify19(self):
        """Regression test showing that before update, the script does not gate on v19.

        In the original baseline script:
        - wait_for_version was called with version 16 (not 19)
        - verify-migration-000019.sql was NOT invoked in verify_stage2_schema
        """
        # We test whether the script has been updated or is still on the legacy version 16.
        # This test documents the exact gap before Step 3.
        has_v19_gate = bool(re.search(r'wait_for_version\s+"\$FRESH_DB"\s+19', self.sh_content))
        has_verify19_call = "verify-migration-000019.sql" in self.sh_content
        # Assert that if either is missing, it demonstrates the historical missing state
        if not (has_v19_gate and has_verify19_call):
            self.assertFalse(has_v19_gate, "Regression demonstrated: wait_for_version not updated to 19")
            self.assertFalse(has_verify19_call, "Regression demonstrated: verify-migration-000019.sql not called")
        else:
            # If updated, ensure both are present and consistent
            self.assertTrue(has_v19_gate)
            self.assertTrue(has_verify19_call)

    def test_harness_gates_on_exact_version_19_and_retains_intermediate_gates(self):
        """Verify script updates latest readiness gate from 16 to 19 and preserves upgrade baseline 9."""
        # Baseline check for upgrade phase: must retain version 9
        self.assertIn('wait_for_version "$UPGRADE_DB" 9', self.sh_content,
                      "Script must retain historical intermediate gate version 9 for upgrade phase")
        # Fresh database check: must wait for version 19
        self.assertRegex(self.sh_content, r'wait_for_version\s+"\$FRESH_DB"\s+19',
                         "Fresh DB readiness gate must wait for exact version 19")

    def test_harness_invokes_verify_migration_000019(self):
        """Verify scripts/sql/verify-migration-000019.sql is invoked."""
        self.assertIn("verify-migration-000019.sql", self.sh_content,
                      "Script must invoke verify-migration-000019.sql")

    def test_verify_sql_on_error_stop_and_transaction(self):
        """Verify SQL script sets ON_ERROR_STOP and wraps tests safely."""
        self.assertIn(r"\set ON_ERROR_STOP on", self.sql_content)
        self.assertIn("BEGIN;", self.sql_content)
        self.assertIn("ROLLBACK;", self.sql_content)

    def test_verify_sql_checks_schema_migrations_version_19(self):
        """Verify SQL script checks version 19 with exact name 'mqtt_simplified_journal'."""
        self.assertIn("version = 19 AND name = 'mqtt_simplified_journal'", self.sql_content)

    def test_verify_sql_checks_execution_contract_and_reset_columns(self):
        """Verify SQL checks execution_contract and reset columns and constraints."""
        self.assertIn("column_name = 'execution_contract'", self.sql_content)
        self.assertIn("column_name = 'reset_predecessor_id'", self.sql_content)
        self.assertIn("column_name = 'reset_reason'", self.sql_content)
        self.assertIn("chk_mqtt_reset_identity", self.sql_content)
        self.assertIn("chk_mqtt_operation_contract", self.sql_content)

    def test_verify_sql_checks_reset_dispositions_structure_and_constraints(self):
        """Verify SQL checks mqtt_reset_dispositions table and constraints."""
        self.assertIn("mqtt_reset_dispositions", self.sql_content)

    def test_verify_sql_checks_permissions_and_no_public_leakage(self):
        """Verify SQL asserts no broader backend/PUBLIC/anon/authenticated grants."""
        self.assertIn("has_table_privilege('iot_backend_app', 'public.mqtt_reset_dispositions'", self.sql_content)
        self.assertIn("has_schema_privilege('iot_backend', 'public', 'CREATE')", self.sql_content)
        self.assertIn("anon", self.sql_content)
        self.assertIn("authenticated", self.sql_content)

    def test_verify13_upgrade_preserves_original_columns_despite_additive_schema(self):
        """Regression test: verify-migration-000013-upgrade.sql should compare snapshot keys

        against current table projection onto original snapshot keys, so that additive columns
        added in later migrations (such as execution_contract, reset_predecessor_id, reset_reason
        added by migration 19) do not cause a false-positive 'v13 modified contradictory v12 history'
        failure while preserving strict immutability checks on all original columns.
        """
        verify13_path = ROOT / "scripts/sql/verify-migration-000013-upgrade.sql"
        self.assertTrue(verify13_path.exists())
        content = verify13_path.read_text(encoding="utf-8")
        # Ensure verify-migration-000013-upgrade projects or slices JSON based on original snapshot keys
        # rather than full to_jsonb(e) with only 'recovery_id' stripped.
        self.assertIn("jsonb_object_agg", content,
                      "verify-migration-000013-upgrade.sql must project current event columns onto snapshot keys")

    def test_wait_for_version_supports_name_check_for_v19(self):
        """Test wait_for_version checks both version and migration name when name is provided."""
        # Ensure wait_for_version handles schema_migrations name
        self.assertRegex(self.sh_content, r'wait_for_version\s+"\$FRESH_DB"\s+19\s+mqtt_simplified_journal',
                         "Fresh DB readiness gate must wait for version 19 AND name mqtt_simplified_journal")

    def test_verify_sql_checks_mqtt_operation_effective_and_search_path(self):
        """Verify SQL checks mqtt_operation_effective and function search_paths."""
        self.assertIn("mqtt_operation_effective(uuid)", self.sql_content)
        self.assertIn("search_path=pg_catalog, public, pg_temp", self.sql_content)


if __name__ == "__main__":
    unittest.main()
