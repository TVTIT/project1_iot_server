"""Preparation and preflight regression tests; these tests never contact Docker."""
import base64
from contextlib import ExitStack
import dataclasses
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

MODULE = Path(__file__).with_name("native_compose_smoke.py")
spec = importlib.util.spec_from_file_location("native_compose_smoke", MODULE)
smoke = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = smoke
spec.loader.exec_module(smoke)


class PreparationTests(unittest.TestCase):
    def test_startup_migration_failure_blocks_next_phase(self):
        with self.assertRaisesRegex(ValueError, 'migration_nonzero'):
            smoke.startup_state('application-migrations', {'Running': False, 'ExitCode': 1})

    def test_startup_health_never_accepts_running_alone(self):
        self.assertFalse(smoke.startup_state('postgres', {'Running': True}))
        self.assertTrue(smoke.startup_state('postgres', {'Running': True, 'Health': {'Status': 'healthy'}}))
        with self.assertRaisesRegex(ValueError, 'health_failed'):
            smoke.startup_state('postgres', {'Running': True, 'Health': {'Status': 'unhealthy'}})

    def test_startup_http_gate_remains_closed(self):
        with self.assertRaisesRegex(RuntimeError, 'native_execution_deferred'):
            smoke.execute_native(None)

    def test_runtime_receipt_rejects_mount_security_and_extra_network(self):
        import copy
        expected = {'mounts': [], 'tmpfs': {}, 'user': '1883:1883', 'readonly': True,
                    'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'], 'network': 'owned'}
        obj = {'Config': {'User': '1883:1883'}, 'Mounts': [],
               'HostConfig': {'ReadonlyRootfs': True, 'CapDrop': ['ALL'], 'SecurityOpt': ['no-new-privileges:true']},
               'NetworkSettings': {'Networks': {'owned': {}}}}
        smoke.verify_runtime_receipt(obj, expected)
        for key, value in [('Privileged', True), ('Tmpfs', {'/unknown': 'rw'}), ('CapDrop', [])]:
            bad = copy.deepcopy(obj)
            bad['HostConfig'][key] = value
            with self.assertRaisesRegex(ValueError, 'runtime_receipt_rejected'):
                smoke.verify_runtime_receipt(bad, expected)
        obj['NetworkSettings']['Networks']['unowned'] = {}
        with self.assertRaisesRegex(ValueError, 'runtime_receipt_rejected'):
            smoke.verify_runtime_receipt(obj, expected)

    def test_startup_jobs_created_or_running_are_not_complete(self):
        for state in ({'Status': 'created', 'ExitCode': 0}, {'Running': True, 'ExitCode': 0}):
            self.assertFalse(smoke.startup_state('application-migrations', state))

    def test_runtime_wrong_image_label_and_cleanup_daemon_failure_refused(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            runner = Mock()
            registry = smoke.ResourceRegistry(fixture, runner)
            item = registry.intent('container', fixture.project + '-fixture', 'sha256:expected', {'test.run': fixture.project})
            obj = {'Id': 'a' * 64, 'Name': '/' + item['name'], 'Image': 'sha256:wrong',
                   'Created': '2026-10-13T00:00:00Z', 'Config': {'Labels': {'test.run': fixture.project}}}
            runner.return_value = Mock(returncode=0, stdout=json.dumps([obj]).encode(), stderr=b'')
            with self.assertRaisesRegex(ValueError, 'resource_ownership_refused'):
                registry.resolve(item)
            obj['Image'] = item['image']
            obj['Config']['Labels']['test.run'] = 'foreign'
            runner.return_value.stdout = json.dumps([obj]).encode()
            with self.assertRaisesRegex(ValueError, 'resource_ownership_refused'):
                registry.resolve(item)
            runner.return_value = Mock(returncode=1, stderr=b'daemon unavailable')
            with self.assertRaisesRegex(ValueError, 'resource_inspect_unresolved'):
                registry.remove(item)


    def prepare(self, directory):
        return smoke.prepare(Path(directory) / "fixture")

    def test_private_and_safe_directories(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            for path in (fixture.base, fixture.private, fixture.artifacts):
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            self.assertEqual(fixture.env_file.stat().st_mode & 0o777, 0o600)
            self.assertFalse(list(fixture.artifacts.iterdir()))

    def test_unique_projects_and_fresh_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            a = smoke.prepare(Path(directory) / "a")
            b = smoke.prepare(Path(directory) / "b")
            self.assertNotEqual(a.project, b.project)
            self.assertIn(a.project, a.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"])

    def test_existing_directory_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            with self.assertRaises(FileExistsError):
                self.prepare(directory)

    def test_no_process_or_deployment_env_leak(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "POSTGRES_PASSWORD": "deployment-secret", "COMPOSE_FILE": "evil.yml",
                "DOCKER_HOST": "tcp://deployment", "HOME": "/deployment"}):
            fixture = self.prepare(directory)
            self.assertNotIn("deployment-secret", fixture.env_file.read_text())
            env = smoke.process_env(fixture)
            self.assertEqual(set(env), {"PATH", "HOME", "DOCKER_CONFIG", "COMPOSE_DISABLE_ENV_FILE"})
            self.assertEqual(env["COMPOSE_DISABLE_ENV_FILE"], "1")
            self.assertNotEqual(env["HOME"], "/deployment")

    def test_all_root_interpolation_inputs_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            smoke.require_inputs(smoke.ROOT_COMPOSE.read_text(), fixture.values)
            with self.assertRaisesRegex(ValueError, "missing_interpolation"):
                smoke.require_inputs("${NEW_REQUIRED:-production}", fixture.values)

    def test_task34_telemetry_ingress_defaults_explicit(self):
        # Task 3.4 added telemetry/MQTT-ingress keys to the root compose; the
        # fixture must carry explicit safe local defaults mirroring the compose
        # `:-` fallbacks so prepare() never fails closed with a missing key.
        expected = {
            "MQTT_INGRESS_ENABLED": "false", "MQTT_CONNECT_TIMEOUT": "5s",
            "MQTT_SUBSCRIBE_TIMEOUT": "3s", "MQTT_PUBLISH_TIMEOUT": "3s",
            "MQTT_RECONNECT_MAX_INTERVAL": "30s",
            "TELEMETRY_MAX_SAMPLES_PER_MESSAGE": "1000",
            "TELEMETRY_MAX_BATCH_DURATION": "60s",
            "TELEMETRY_MAX_FUTURE_SKEW": "2m", "TELEMETRY_DB_TIMEOUT": "5s",
        }
        for key, value in expected.items():
            self.assertEqual(smoke.DEFAULTS.get(key), value, key)
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            for key, value in expected.items():
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", fixture.env_file.read_text())

    def test_user_management_and_auth_admin_defaults_explicit(self):
        expected = {
            "AUTH_ADMIN_BASE_URL": "http://supabase-envoy:8000",
            "AUTH_ADMIN_TIMEOUT": "10s",
            "AUTH_ADMIN_MAX_BODY_BYTES": "16384",
            "USER_LIST_MAX_USERS": "1000",
            "USER_LIST_MAX_MEMBERSHIPS": "10000",
            "USER_LIST_MAX_RESPONSE_BYTES": "2097152",
            "SUPABASE_SERVICE_ROLE_KEY": "",
        }
        for key, value in expected.items():
            self.assertEqual(smoke.DEFAULTS.get(key), value, key)
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            for key, value in expected.items():
                self.assertEqual(fixture.values[key], value)
                self.assertIn(f"{key}={value}\n", fixture.env_file.read_text())

    def test_dynsec_qualification_defaults_match_verified_config_identity(self):
        root = Path(__file__).resolve().parents[2]
        env_text = (root / ".env.example").read_text()
        compose_text = smoke.ROOT_COMPOSE.read_text()
        image = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        config_id = "sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4"
        self.assertRegex(env_text, rf"(?m)^MQTT_DYNSEC_IMAGE_REFERENCE={re.escape(image)}$")
        self.assertRegex(env_text, rf"(?m)^MQTT_DYNSEC_IMAGE_CONFIG_ID={re.escape(config_id)}$")
        self.assertRegex(env_text, r"(?m)^MQTT_DYNSEC_PLATFORM=linux/amd64$")
        self.assertRegex(compose_text, rf"(?m)^\s+MQTT_DYNSEC_IMAGE_REFERENCE: \$\{{MQTT_DYNSEC_IMAGE_REFERENCE:-{re.escape(image)}\}}$")
        self.assertRegex(compose_text, rf"(?m)^\s+MQTT_DYNSEC_IMAGE_CONFIG_ID: \$\{{MQTT_DYNSEC_IMAGE_CONFIG_ID:-{re.escape(config_id)}\}}$")
        self.assertRegex(compose_text, r"(?m)^\s+MQTT_DYNSEC_PLATFORM: \$\{MQTT_DYNSEC_PLATFORM:-linux/amd64\}$")

    def test_prepared_fixture_contains_verified_dynsec_qualification_identity(self):
        image = "eclipse-mosquitto@sha256:d12c8f80dfc65b768bb9acecc7ef182b976f71fb681640b66358e5e0cf94e9e9"
        config_id = "sha256:d25945831d6b8935b016caacc6f612ad4876ba124601b240e64953886cf264b4"
        platform = "linux/amd64"
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "MQTT_DYNSEC_IMAGE_CONFIG_ID": "sha256:deployment-leak",
                "MQTT_DYNSEC_PLATFORM": "linux/arm64"}):
            fixture = self.prepare(directory)
            self.assertEqual(smoke.DEFAULTS["MQTT_DYNSEC_IMAGE_REFERENCE"], image)
            self.assertEqual(smoke.DEFAULTS["MQTT_DYNSEC_IMAGE_CONFIG_ID"], config_id)
            self.assertEqual(smoke.DEFAULTS["MQTT_DYNSEC_PLATFORM"], platform)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_REFERENCE"], image)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_CONFIG_ID"], config_id)
            self.assertEqual(fixture.values["MQTT_DYNSEC_PLATFORM"], platform)
            env_content = fixture.env_file.read_text()
            self.assertIn(f"MQTT_DYNSEC_IMAGE_REFERENCE={image}\n", env_content)
            self.assertIn(f"MQTT_DYNSEC_IMAGE_CONFIG_ID={config_id}\n", env_content)
            self.assertIn(f"MQTT_DYNSEC_PLATFORM={platform}\n", env_content)
            self.assertNotIn("deployment-leak", env_content)
            # Consistency with production bootstrap and CLI qualification constants
            bootstrap_spec = importlib.util.spec_from_file_location("mqtt_bootstrap", smoke.ROOT / "scripts/mqtt-bootstrap.py")
            bootstrap_mod = importlib.util.module_from_spec(bootstrap_spec)
            bootstrap_spec.loader.exec_module(bootstrap_mod)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_CONFIG_ID"], getattr(bootstrap_mod, "PINNED_CONFIG_ID"))
            cli_spec = importlib.util.spec_from_file_location("dynsec_bootstrap_cli", smoke.ROOT / "scripts/tests/dynsec_bootstrap_cli.py")
            cli_mod = importlib.util.module_from_spec(cli_spec)
            cli_spec.loader.exec_module(cli_mod)
            self.assertEqual(fixture.values["MQTT_DYNSEC_IMAGE_CONFIG_ID"], getattr(cli_mod, "PINNED_CONFIG_ID"))

    def _verify_root_postgres_healthcheck(self, compose_text):
        pg_match = re.search(r"(?ms)^  postgres:\n(.*?)(?=\n  [a-z0-9_-]+:|\Z)", compose_text)
        self.assertIsNotNone(pg_match, "postgres service definition missing in root compose")
        pg_block = pg_match.group(1)

        hc_match = re.search(r"(?ms)^\s+healthcheck:\n(.*?)(?=\n    [a-z_]+:|\n  #|\Z)", pg_block)
        self.assertIsNotNone(hc_match, "healthcheck missing from postgres service in root compose")
        hc_block = hc_match.group(1)

        tm = re.search(r'test:\s*\[\s*"CMD-SHELL",\s*"([^"]+)"\s*\]', hc_block)
        self.assertIsNotNone(tm, "CMD-SHELL test command missing from postgres healthcheck")
        cmd_str = tm.group(1)

        # Validate explicit TCP host 127.0.0.1 and port 5432 transport
        tokens = cmd_str.split()
        self.assertEqual(tokens[0], "pg_isready", "healthcheck must execute pg_isready")
        self.assertIn("-h", tokens, "healthcheck command missing explicit -h flag")
        self.assertEqual(tokens[tokens.index("-h") + 1], "127.0.0.1", "healthcheck must target 127.0.0.1")
        self.assertIn("-p", tokens, "healthcheck command missing explicit -p flag")
        self.assertEqual(tokens[tokens.index("-p") + 1], "5432", "healthcheck must target port 5432")

        # Validate preserved user and db interpolation and safe defaults
        self.assertIn("-U ${POSTGRES_USER:-postgres}", cmd_str)
        self.assertIn("-d ${POSTGRES_DB:-iot_platform}", cmd_str)

        # Validate preserved healthcheck budgets and invariants
        self.assertRegex(hc_block, r"(?m)^\s+interval:\s*10s$")
        self.assertRegex(hc_block, r"(?m)^\s+timeout:\s*5s$")
        self.assertRegex(hc_block, r"(?m)^\s+retries:\s*5$")
        self.assertNotIn("start_period", hc_block)
        self.assertNotIn("sleep", cmd_str)

        # Validate localhost-only port mapping and service isolation preserved
        self.assertRegex(pg_block, r'(?m)^\s+-\s*"127\.0\.0\.1:5432:5432"$')
        return cmd_str

    def test_root_compose_postgres_healthcheck_explicit_tcp_transport_and_budgets(self):
        compose_text = smoke.ROOT_COMPOSE.read_text()
        self._verify_root_postgres_healthcheck(compose_text)

    def test_root_compose_postgres_healthcheck_regression_rejects_socket_only(self):
        real_text = smoke.ROOT_COMPOSE.read_text()
        socket_only_text = real_text.replace(
            "pg_isready -h 127.0.0.1 -p 5432 -U",
            "pg_isready -U",
        )
        self.assertNotEqual(real_text, socket_only_text)
        with self.assertRaises(AssertionError):
            self._verify_root_postgres_healthcheck(socket_only_text)
        mock_compose = Mock()
        mock_compose.read_text.return_value = socket_only_text
        with patch.object(smoke, "ROOT_COMPOSE", mock_compose):
            with self.assertRaises(AssertionError):
                self._verify_root_postgres_healthcheck(smoke.ROOT_COMPOSE.read_text())

    def test_smoke_override_inherits_root_postgres_healthcheck(self):
        """Verify smoke override template does not redefine or remove postgres healthcheck.

        Note: This asserts source template inheritance intent only (the override file does not
        touch healthcheck, intending Compose to inherit root compose definition).
        Actual effective configuration merge must be verified in the pending native run.
        """
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            override_text = fixture.override.read_text()
            pg_match = re.search(r"(?ms)^  postgres:\n(.*?)(?=\n  [a-z0-9_-]+:|\Z)", override_text)
            self.assertIsNotNone(pg_match, "postgres service missing in generated override")
            self.assertNotIn("healthcheck", pg_match.group(1), "smoke override must not replace root healthcheck")


    def test_compose_command_is_explicit_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            command = smoke.compose_command(fixture, "up")
            self.assertIn("--env-file", command)
            self.assertIn(str(fixture.env_file), command)
            self.assertEqual(command[-len(smoke.SERVICES):], list(smoke.SERVICES))
            self.assertNotIn("rathole", command)
            self.assertNotIn("supabase-storage", command)
            self.assertNotIn("cloudflared", command)
            self.assertIn("--no-deps", command)
            self.assertGreater(smoke.BUDGETS["up"], 0)
            self.assertLessEqual(max(smoke.BUDGETS.values()), 600)
            with self.assertRaises(ValueError):
                smoke.compose_command(fixture, "logs")

    def test_override_isolates_names_ports_mounts_and_dependencies(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            text = fixture.override.read_text()
            self.assertEqual(text.count("container_name: !reset null"), len(smoke.SERVICES))
            self.assertEqual(text.count("ports: !override"), len(smoke.SERVICES))
            self.assertIn('host_ip: "127.0.0.1"', text)
            self.assertIn('published: "0"', text)
            self.assertIn("external: true", text)
            self.assertIn("target: backend", text)
            self.assertNotIn("/app/server", text)
            self.assertNotIn("storage_data", text)
            self.assertIn("service_completed_successfully", text)
            self.assertIn("io.iot.native-smoke.run", text)

    def test_supported_compose_version(self):
        smoke.require_compose_version("2.24.4")
        smoke.require_compose_version("v2.40.1")
        for value in ("2.24.3", "1.29.2", "unknown", "2.24"):
            with self.assertRaises(ValueError):
                smoke.require_compose_version(value)

    def test_bootstrap_hook_contract_and_collision(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            result = {"volume": fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"],
                      "store_created": True, "readiness": "NOT_READY", "fresh_volume": True}
            smoke.verify_bootstrap_result(fixture, result)
            for key, value in (("fresh_volume", False), ("store_created", False),
                               ("readiness", "READY"), ("volume", "deployment")):
                with self.assertRaises(ValueError):
                    smoke.verify_bootstrap_result(fixture, dict(result, **{key: value}))

    def test_no_native_execution_even_with_claimed_gate(self):
        with tempfile.TemporaryDirectory() as directory, patch("subprocess.run") as run:
            fixture = self.prepare(directory)
            for gate in (None, "approved", "bootstrap-green"):
                with self.assertRaisesRegex(RuntimeError, "native_execution_deferred"):
                    smoke.execute_native(fixture, gate)
            run.assert_not_called()

    def test_summary_rejects_raw_logs_and_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.prepare(directory)
            report = smoke.summary(fixture)
            self.assertEqual(report["status"], "PREPARED_NOT_EXECUTED")
            smoke.validate_summary(report)
            for extra in ("logs", "inspect", "password", "stdout", "access_token"):
                with self.assertRaises(ValueError):
                    smoke.validate_summary(dict(report, **{extra: "secret"}))
            with self.assertRaises(ValueError):
                smoke.validate_summary(dict(report, status="PASS"))


class SchemaHistoryTests(unittest.TestCase):
    """Focused regression: the smoke must accept only the exact supported schema.

    The pinned terminal is ``EXPECTED_SCHEMA_TERMINAL`` (current migration 22);
    stale, missing, renamed, extra, or failed histories must all fail closed.
    """

    def test_current_schema_history_matches_pinned_terminal(self):
        expected = smoke.expected_schema_history()
        self.assertIn("22:gateway_membership_revocation", expected)
        self.assertTrue(expected.endswith(smoke.EXPECTED_SCHEMA_TERMINAL))
        self.assertTrue(smoke.schema_history_verified(0, expected, expected))

    def test_stale_schema21_history_rejected(self):
        expected = smoke.expected_schema_history()
        stale = expected.removesuffix(",22:gateway_membership_revocation")
        self.assertFalse(smoke.schema_history_verified(0, stale, expected))

    def test_missing_terminal_migration_rejected(self):
        expected = smoke.expected_schema_history()
        missing = ",".join(p for p in expected.split(",") if p != "22:gateway_membership_revocation")
        self.assertFalse(smoke.schema_history_verified(0, missing, expected))

    def test_wrong_terminal_name_rejected(self):
        expected = smoke.expected_schema_history()
        wrong = expected.replace("22:gateway_membership_revocation", "22:unexpected_name")
        self.assertFalse(smoke.schema_history_verified(0, wrong, expected))

    def test_extra_migration_row_rejected(self):
        expected = smoke.expected_schema_history()
        self.assertFalse(smoke.schema_history_verified(0, expected + ",23:future_migration", expected))

    def test_failed_migration_returncode_rejected(self):
        expected = smoke.expected_schema_history()
        self.assertFalse(smoke.schema_history_verified(1, expected, expected))

    def test_unpinned_future_terminal_rejected(self):
        # Guards the pin itself: a history that matches its own files but no
        # longer ends at the pinned terminal is rejected, so adding a supported
        # migration forces an explicit terminal update instead of passing.
        expected = smoke.expected_schema_history()
        future = expected.replace("22:gateway_membership_revocation", "23:future_migration")
        self.assertFalse(smoke.schema_history_verified(0, future, future))


def schema_query_result(expected, query):
    # Pre-Auth startup stops at 20; both 21 and 22 run in the compat phase.
    if 'WHERE version <= 20' in query:
        return ','.join(row for row in expected.split(',') if int(row.split(':')[0]) <= 20)
    return expected


class ReceiptTests(unittest.TestCase):
    def receipt(self, kind="container"):
        return smoke.Receipt(kind, "a" * 64 if kind != "volume" else "ncs_123456789abc_store",
                             "ncs_123456789abc", "store" if kind == "volume" else "backend", "2026-10-13T00:00:00Z")

    def test_receipt_is_immutable(self):
        receipt = self.receipt()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            receipt.role = "nginx"

    def test_wrong_ownership_identity_and_timestamp_refused(self):
        receipt = self.receipt()
        good = smoke.receipt_metadata(receipt)
        for key in good:
            with self.assertRaises(ValueError):
                smoke.cleanup_commands((receipt,), lambda _: dict(good, **{key: "wrong"}))

    def test_cleanup_veto_and_verify_all_before_commands(self):
        receipt = self.receipt()
        inspect = Mock(side_effect=smoke.receipt_metadata)
        with self.assertRaises(ValueError):
            smoke.cleanup_commands((receipt,), inspect, veto=True)
        inspect.assert_not_called()
        commands = smoke.cleanup_commands((receipt,), inspect)
        self.assertEqual(commands, [["docker", "container", "rm", "--force", receipt.identity]])
        self.assertFalse(any("prune" in c or "down" in c for c in commands))
        with self.assertRaises(ValueError):
            smoke.cleanup_commands((receipt, receipt), inspect)

    def test_volume_requires_unique_name_and_created_stamp(self):
        receipt = self.receipt("volume")
        self.assertEqual(smoke.cleanup_commands((receipt,), smoke.receipt_metadata)[0],
                         ["docker", "volume", "rm", receipt.identity])
        for identity in ("postgres_data", "ncs_123456789abc_other", "*"):
            with self.assertRaises(ValueError):
                smoke.cleanup_commands((dataclasses.replace(receipt, identity=identity),),
                                       smoke.receipt_metadata)


class AssertionTests(unittest.TestCase):
    def test_role_cases_cover_admin_and_each_denied_sensor_target(self):
        cases = smoke.role_http_cases()
        self.assertEqual(len(cases), 14)
        self.assertEqual(sum(case["method"] == "GET" for case in cases), 4)
        self.assertEqual(sum(case["method"] == "PUT" for case in cases), 10)
        self.assertEqual({case["sensor_kind"] for case in cases if case["method"] == "PUT"},
                         {"new", "existing"})
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            report = smoke.role_http_summary(fixture, {
                "positive_http": {"admin_inventory_status": 200, "owner_sensor_create_status": 201,
                                  "owner_sensor_retry_status": 200, "db_guarantees_verified": True},
                "role_http": {"role_cases": {c["label"]: c["status"] for c in cases},
                              "role_snapshot_comparisons": 10}},
                {"container": 10, "network": 1, "volume": 3})
            smoke.validate_summary(report)
            self.assertFalse(report["http_accepted"])
            self.assertEqual(set(report["evidence"]), {"positive_http_statuses", "role_case_count", "role_denials_verified"})

    def test_role_executor_checks_snapshot_around_each_denied_write(self):
        self.assertTrue(callable(smoke.run_role_http_slice))
        self.assertIn("--role-http-only", smoke.cli_arguments())
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            token_map = {r: r for r in ("platform_admin", "owner", "operator", "viewer", "nonmember", "cross_gateway_owner")}
            with patch.object(smoke, "provision_role_identities", return_value=token_map), \
                 patch.object(smoke, "denial_snapshot", return_value={"sensors": [], "twin_entities": [], "twin_relationships": [], "twin_states": []}) as snapshot, \
                 patch.object(smoke, "http_request", return_value=(403, {}, {"error": {"code": "forbidden", "message": "access denied"}})):
                # Supply 404 responses for the three not-found identities.
                def request(*args, **kwargs):
                    token = kwargs["token"]
                    status = 404 if args[2] != "/v1/admin/gateways" and token in {"nonmember", "cross_gateway_owner", "platform_admin"} else 403
                    return status, {}, {"error": {"code": "not_found", "message": "resource not found"}} if status == 404 else {"error": {"code": "forbidden", "message": "access denied"}}
                with patch.object(smoke, "http_request", side_effect=request):
                    result = smoke.run_role_http_slice(fixture, object(), "http://127.0.0.1", "pg")
            self.assertEqual(len(result["role_cases"]), 14)
            self.assertEqual(snapshot.call_count, 20)

    def test_http_request_timeout_and_headers(self):
        with self.assertRaises(ValueError):
            smoke.http_request("http://127.0.0.1:1", "GET", "/test", timeout=0.01)

    def test_assert_safe_secret_leak_prevention(self):
        smoke.assert_safe_data({"ok": True, "count": 1})
        for bad in [{"token": "abc"}, {"password": "123"}, {"secret_key": "x"}, {"jwt": "ey..."}]:
            with self.assertRaisesRegex(ValueError, "secret_response_rejected"):
                smoke.assert_safe_data(bad)

    def test_positive_http_summary_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            rep = smoke.positive_summary(fixture, evidence={
                "admin_inventory_status": 200,
                "owner_sensor_create_status": 201,
                "owner_sensor_retry_status": 200,
            })
            rep["backend_image_id"] = "sha256:mock"
            rep["phase"] = "positive_http"
            self.assertEqual(rep["status"], "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED")
            self.assertFalse(rep["http_accepted"])
            smoke.validate_summary(rep)

    def test_inventory_allowlist(self):
        smoke.assert_response("inventory", 200, {"items": [], "next_cursor": None})
        smoke.assert_response("inventory", 200, {"items": [{"gateway_id": "fixture", "name": "Fixture",
                              "description": None, "created_at": None}], "next_cursor": None})
        with self.assertRaises(ValueError):
            smoke.assert_response("inventory", 200, {"items": [{"password_hash": "secret"}], "next_cursor": None})

    def test_owner_sensor_supported_fields_only(self):
        body = {"gateway_id": "fixture", "sensor_id": "sample", "name": "Sample",
                "unit": None, "entity_id": "urn:ngsi-ld:Sensor:fixture:sample", "created_at": None}
        smoke.assert_response("sensor_create", 201, body)
        smoke.assert_response("sensor_retry", 200, body)
        with self.assertRaises(ValueError):
            smoke.assert_response("sensor_create", 201, dict(body, desired_state={}))

    def test_denials_and_login_are_not_saved_as_artifacts(self):
        smoke.assert_response("forbidden", 403, {"error": {"code": "forbidden", "message": "access denied"}})
        smoke.assert_response("not_found", 404, {"error": {"code": "not_found", "message": "resource not found"}})
        smoke.assert_response("unauthorized", 401, {})
        with self.assertRaises(ValueError):
            smoke.assert_response("forbidden", 200, {})
        self.assertIsNone(smoke.assert_login(200, {"access_token": "private", "token_type": "bearer"}))
        with self.assertRaises(ValueError):
            smoke.assert_login(200, {"access_token": ""})

    def test_pending_routes_and_no_admin_owner_bypass(self):
        nonmember = [c for c in smoke.ACCEPTANCE if c[3] == "nonmember" and c[1] == "PUT"]
        self.assertEqual(nonmember[0][4], 404)
        admin = [c for c in smoke.ACCEPTANCE if c[3] == "admin_without_membership"]
        self.assertEqual(admin[0][4], 404)
        self.assertEqual(set(smoke.SENSOR_PAYLOAD), {"name", "unit"})

    def test_denial_snapshot_preserves_full_rows_not_just_counts(self):
        before = {"sensors": [{"gateway_id": "gw", "sensor_id": "s", "name": "old", "unit": "C"}],
                  "twin_entities": [{"id": "id", "entity_id": "urn:ngsi-ld:Sensor:gw:s", "attributes": {"unit": "C"}}],
                  "twin_relationships": [{"source": "g", "type": "hasSensor", "target": "id"}],
                  "twin_states": [{"entity_id": "id", "reported_state": {"unit": "C"}, "desired_state": {}}]}
        self.assertTrue(smoke.assert_unchanged(before, before))
        changed = json.loads(json.dumps(before))
        changed["sensors"][0]["name"] = "tampered"
        with self.assertRaisesRegex(ValueError, "denial_mutated_database"):
            smoke.assert_unchanged(before, changed)

    def test_full_rbac_report_cannot_accept_partial_cases(self):
        with self.assertRaisesRegex(ValueError, "acceptance_incomplete"):
            smoke.acceptance_summary(None, {}, {"container": 10, "network": 1, "volume": 3})

    def test_jwt_denial_cases_cover_all_conditions_and_targets(self):
        cases = smoke.jwt_http_cases()
        self.assertEqual(len(cases), 9)
        self.assertEqual(sum(case["method"] == "GET" for case in cases), 3)
        self.assertEqual(sum(case["method"] == "PUT" for case in cases), 6)
        self.assertEqual({c["condition"] for c in cases}, {"missing", "invalid", "expired"})
        self.assertEqual({c["sensor_kind"] for c in cases if c["method"] == "PUT"}, {"new", "existing"})
        for c in cases:
            self.assertEqual(c["status"], 401)
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            tokens = smoke.generate_jwt_denial_tokens(fixture)
            self.assertIn("missing", tokens)
            self.assertIn("invalid", tokens)
            self.assertIn("expired", tokens)
            self.assertEqual(tokens["missing"], "")
            # Expired token claims check
            parts = tokens["expired"].split(".")
            self.assertEqual(len(parts), 3)
            padding = b"=" * (-len(parts[1]) % 4)
            claims = json.loads(base64.urlsafe_b64decode(parts[1].encode() + padding))
            self.assertEqual(claims["role"], "authenticated")
            self.assertEqual(claims["aud"], "authenticated")
            self.assertEqual(claims["iss"], fixture.values["GOTRUE_JWT_ISSUER"])
            self.assertEqual(claims["sub"], "00000000-0000-4000-8000-000000000001")
            header = json.loads(base64.urlsafe_b64decode(parts[0] + "=" * (-len(parts[0]) % 4)))
            self.assertEqual(header["alg"], "HS256")
            self.assertTrue(claims["exp"] < time.time() - 3600)
            # Invalid token is malformed / wrong signature
            self.assertTrue(len(tokens["invalid"].split(".")) in (1, 3))

    def test_jwt_denial_executor_verifies_snapshots_and_rejections(self):
        self.assertTrue(callable(smoke.run_jwt_denial_slice))
        self.assertIn("--full-http", smoke.cli_arguments())
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            empty_snap = {"sensors": [], "twin_entities": [], "twin_relationships": [], "twin_states": []}
            with patch.object(smoke, "denial_snapshot", return_value=empty_snap) as snapshot, \
                 patch.object(smoke, "http_request", return_value=(401, {}, {"error": {"code": "unauthorized", "message": "authentication required"}})):
                result = smoke.run_jwt_denial_slice(fixture, object(), "http://127.0.0.1", "pg", "gw_test", "sensor_test")
            self.assertEqual(len(result["jwt_cases"]), 9)
            self.assertEqual(result["jwt_case_count"], 9)
            self.assertTrue(result["jwt_denials_verified"])
            self.assertEqual(snapshot.call_count, 12)  # 6 PUTs * 2 (before/after)
            self.assertEqual(result["jwt_snapshot_comparisons"], 6)

    def test_full_http_acceptance_summary_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            role_cases = {c["label"]: c["status"] for c in smoke.role_http_cases()}
            jwt_cases = {c["label"]: c["status"] for c in smoke.jwt_http_cases()}
            evidence = {
                "positive_http": {
                    "admin_inventory_status": 200,
                    "owner_sensor_create_status": 201,
                    "owner_sensor_retry_status": 200,
                    "db_guarantees_verified": True,
                },
                "role_http": {
                    "role_cases": role_cases,
                    "role_snapshot_comparisons": 10,
                },
                "jwt_http": {
                    "jwt_cases": jwt_cases,
                    "jwt_snapshot_comparisons": 6,
                },
            }
            cleanup_counts = {"container": 10, "network": 1, "volume": 3}
            report = smoke.full_http_summary(fixture, evidence, cleanup_counts)
            report["backend_image_id"] = "sha256:mock"
            report["phase"] = "full_http"
            self.assertEqual(report["status"], "RBAC_JWT_HTTP_ACCEPTED")
            self.assertTrue(report["http_accepted"])
            self.assertTrue(report["exact_cleanup_verified"])
            smoke.validate_summary(report)

            # Incomplete cases rejected
            incomplete_ev = dict(evidence)
            incomplete_ev["jwt_http"] = {"jwt_cases": {k: v for k, v in list(jwt_cases.items())[:-1]}, "jwt_snapshot_comparisons": 5}
            with self.assertRaisesRegex(ValueError, "jwt_acceptance_incomplete"):
                smoke.full_http_summary(fixture, incomplete_ev, cleanup_counts)


class ModelTests(unittest.TestCase):
    def test_envoy_probe_requires_model_and_no_foreign_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            with patch.object(smoke, "dry_effective_config", side_effect=ValueError("blocked")):
                with self.assertRaisesRegex(ValueError, "blocked"):
                    smoke.probe_envoy_toolchain(fixture)

    def test_build_requires_model_before_real_dockerfile(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            runner = Mock()
            with patch.object(smoke, "dry_effective_config", side_effect=ValueError("blocked")):
                with self.assertRaisesRegex(ValueError, "blocked"):
                    smoke.build_native_image(fixture, runner=runner)
            runner.assert_not_called()

    def test_model_rejects_extra_services_before_allocation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            with self.assertRaisesRegex(ValueError, "effective_services_rejected"):
                smoke.validate_effective_model(fixture, {"services": {"rathole": {}}})

    def test_root_topology_removed_explicitly_not_hidden_by_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            text = fixture.override.read_text()
            for name in ("rathole", "supabase-storage", "cloudflared"):
                self.assertIn("  " + name + ": !reset null", text)
            self.assertIn("\nvolumes: !override\n", text)
            self.assertIn("\nnetworks: !override\n", text)
            self.assertIn("  mosquitto:\n", text)
            self.assertIn("    volumes: !override\n", text)

    def test_dry_config_uses_private_capture_and_finite_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = smoke.prepare(Path(directory) / "fixture")
            runner = Mock(side_effect=[type("Result", (), {"returncode": 0, "stdout": b"2.24.4\n"})(),
                                      type("Result", (), {"returncode": 0, "stdout": b'{"services":{}}'})()])
            with self.assertRaisesRegex(ValueError, "effective_services_rejected"):
                smoke.dry_effective_config(fixture, runner=runner)
            self.assertEqual(runner.call_count, 2)
            for call in runner.call_args_list:
                self.assertGreater(call.kwargs["timeout"], 0)
                self.assertEqual(call.kwargs["env"], smoke.process_env(fixture))
            self.assertEqual((fixture.private / "effective.json").stat().st_mode & 0o777, 0o600)

    def test_rendered_compose_backend_environment_carries_nonsecret_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            fixture = smoke.prepare(prep_dir)
            model = smoke.dry_effective_config(fixture)
            backend_env = model["services"]["backend"].get("environment", {})
            self.assertEqual(backend_env.get("AUTH_ADMIN_BASE_URL"), "http://supabase-envoy:8000")
            self.assertEqual(backend_env.get("AUTH_ADMIN_TIMEOUT"), "10s")
            self.assertEqual(backend_env.get("AUTH_ADMIN_MAX_BODY_BYTES"), "16384")
            self.assertEqual(backend_env.get("USER_LIST_MAX_USERS"), "1000")
            self.assertEqual(backend_env.get("USER_LIST_MAX_MEMBERSHIPS"), "10000")
            self.assertEqual(backend_env.get("USER_LIST_MAX_RESPONSE_BYTES"), "2097152")


class AllocationTests(unittest.TestCase):
    def test_readback_mount_metadata_rejects_anonymous_image_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'fixture')
            obj = {'HostConfig': {'Tmpfs': {p: 'rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883' for p in ('/mosquitto/data', '/mosquitto/log')}},
                   'Mounts': [{'Type': 'volume', 'Name': f.project + '_store', 'Destination': '/store', 'RW': False},
                              {'Type': 'bind', 'Source': str(f.private / 'certs'), 'Destination': '/certs', 'RW': False}]}
            volume = {'name': f.project + '_store'}
            self.assertTrue(smoke.verify_readback_mounts(f, volume, obj))
            obj['Mounts'].append({'Type': 'volume', 'Name': 'unowned', 'Destination': '/mosquitto/data', 'RW': True})
            with self.assertRaisesRegex(ValueError, 'mounts_rejected'):
                smoke.verify_readback_mounts(f, volume, obj)

    def test_readback_uses_real_open_not_busybox_mode_only_access_test(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / 'fixture')
            registry = Mock()
            item = {'name': f.project + '-reader', 'image': 'sha256:' + 'a' * 64}
            registry.intent.return_value = item
            registry.resolve.side_effect = [None, {'Id': 'b' * 64}, {'State': {'Running': False, 'ExitCode': 1}}]
            registry.command.side_effect = [subprocess.CompletedProcess([], 0, ('b' * 64).encode(), b''), subprocess.CompletedProcess([], 1, b'', b'phase=server_key')]
            with patch.object(smoke, 'verify_readback_mounts'), self.assertRaises(ValueError):
                registry.resolve.side_effect = [None, {'Id': 'b' * 64, 'Mounts': [], 'HostConfig': {'Tmpfs': {}}}, {'State': {'Running': False, 'ExitCode': 1}}]
                smoke.allocation_readback(f, registry, {'image': item['image'], 'name': f.project + '_store'})
            script = registry.command.call_args_list[0].args[0][-1]
            self.assertIn('cat /certs/server.key >/dev/null', script)
            self.assertNotIn('test -r', script)

    def test_helper_disables_implicit_image_volumes_and_retains_failure_output(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            registry = Mock()
            item = {"name": f.project + "-reader", "image": "sha256:" + "a" * 64}
            registry.intent.return_value = item
            registry.resolve.side_effect = [None, {"Id": "b" * 64}, {"State": {"Running": False, "ExitCode": 1}}]
            registry.command.side_effect = [subprocess.CompletedProcess([], 0, ("b" * 64).encode(), b""), subprocess.CompletedProcess([], 1, b"private-output", b"private-error")]
            registry.resolve.side_effect = [None, {"Id": "b" * 64, 'Mounts': [], 'HostConfig': {'Tmpfs': {}}}, {"State": {"Running": False, "ExitCode": 1}}]
            with patch.object(smoke, 'verify_readback_mounts'), self.assertRaisesRegex(ValueError, "readback_failed"):
                smoke.allocation_readback(f, registry, {"image": item["image"], "name": f.project + "_store"})
            command = registry.command.call_args_list[0].args[0]
            self.assertIn("/mosquitto/data:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883", command)
            self.assertIn("/mosquitto/log:rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883", command)
            self.assertEqual((f.private / "readback-output.json").stat().st_mode & 0o777, 0o600)
            self.assertFalse(list(f.artifacts.iterdir()))

    def test_cancellation_reconciles_before_cleanup_and_restores_handlers(self):
        import signal
        lifecycle = smoke.bootstrap_lifecycle()
        before = signal.getsignal(signal.SIGTERM)
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            events = []
            def allocate(fixture, registry, runner):
                item = registry.intent("volume", f.project + "_store", "sha256:" + "a" * 64, {"test.run": f.project})
                registry.allocating(item)
                raise lifecycle.RunnerInterrupted(signal.SIGTERM)
            with patch.object(smoke, "bootstrap_lifecycle", return_value=lifecycle), patch.object(smoke, "generate_fixture_tls"), patch.object(smoke, "allocate_bootstrap_store", side_effect=allocate), patch.object(lifecycle, "reconcile_child_receipts", side_effect=lambda *a: events.append("reconcile")), patch.object(smoke.ResourceRegistry, "remove", side_effect=lambda *a: events.append("remove")):
                with self.assertRaises(lifecycle.RunnerInterrupted):
                    smoke.allocation_roundtrip(f)
            self.assertEqual(events, ["reconcile", "remove"])
            self.assertEqual(signal.getsignal(signal.SIGTERM), before)

    def test_unjoined_child_vetoes_cleanup_and_retains_inputs(self):
        lifecycle = smoke.bootstrap_lifecycle()
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            smoke.private_write(f.private / "bootstrap.json", "protected")
            with patch.object(smoke, "bootstrap_lifecycle", return_value=lifecycle), patch.object(smoke, "generate_fixture_tls"), patch.object(smoke, "allocate_bootstrap_store", side_effect=lifecycle.IncompleteCleanup("unjoined")), patch.object(smoke.ResourceRegistry, "remove") as remove:
                with self.assertRaisesRegex(ValueError, "child_unresolved"):
                    smoke.allocation_roundtrip(f)
                remove.assert_not_called()
            self.assertTrue((f.private / "bootstrap.json").exists())

    def test_allocation_attempt_is_durable(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            registry = smoke.ResourceRegistry(f, Mock())
            item = registry.intent("volume", f.project + "_store", "sha256:" + "a" * 64, {"test.run": f.project})
            registry.allocating(item)
            self.assertTrue(json.loads(item["path"].read_text())["allocation_attempted"])

    def test_actual_tls_fields_and_protected_keys(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            smoke.generate_fixture_tls(f)
            certs = f.private / "certs"
            text = subprocess.check_output(["openssl", "x509", "-in", str(certs / "server.crt"), "-noout", "-text"])
            self.assertIn(b"DNS:mosquitto", text)
            self.assertIn(b"DNS:localhost", text)
            self.assertEqual((f.private / "ca.key").stat().st_mode & 0o777, 0o600)
            self.assertEqual((certs / "server.key").stat().st_mode & 0o007, 0)
            self.assertFalse((certs / "ca.key").exists())
            self.assertNotEqual((f.private / "ca.key").read_bytes(), (certs / "server.key").read_bytes())
            subprocess.check_call(["openssl", "verify", "-CAfile", str(certs / "ca.crt"), str(certs / "server.crt")], stdout=subprocess.DEVNULL)
            acl = subprocess.check_output(["getfacl", "-cp", str(certs / "server.key")])
            self.assertIn(b"user:1883:r--", acl)

    def test_registry_intent_is_durable_before_reply_and_unknown_is_not_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            runner = Mock(side_effect=OSError("lost reply"))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", f.project + "-reader", "sha256:" + "a" * 64, {"test.run": f.project})
            self.assertEqual(item["path"].stat().st_mode & 0o777, 0o600)
            with self.assertRaisesRegex(ValueError, "resource_inspect_unresolved"):
                registry.resolve(item)
            self.assertTrue(item["path"].exists())

    def test_volume_replacement_refused_before_delete(self):
        import json
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            obj = {"Name": f.project + "_store", "CreatedAt": "2026-10-13T01:00:00Z", "Labels": {"test.run": f.project}, "Driver": "local", "Options": None}
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("volume", obj["Name"], "sha256:" + "a" * 64, obj["Labels"])
            registry.resolve(item)
            obj["CreatedAt"] = "2026-10-13T02:00:00Z"
            runner.return_value.stdout = json.dumps([obj]).encode()
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.remove(item)
            self.assertFalse(any("rm" in call.args[0] for call in runner.call_args_list))

    def test_absence_requires_explicit_daemon_not_found(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            runner = Mock(return_value=subprocess.CompletedProcess([], 1, b"", b"daemon unavailable"))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("volume", f.project + "_store", "sha256:" + "a" * 64, {"test.run": f.project})
            with self.assertRaisesRegex(ValueError, "resource_inspect_unresolved"):
                registry.resolve(item)
            runner.return_value.stderr = b"Error: No such volume: fixture"
            self.assertIsNone(registry.resolve(item))

    def test_helper_immutable_identity_and_creation_receipt(self):
        import json
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            obj = {"Id": "b" * 64, "Name": "/" + f.project + "-reader", "Created": "2026-10-13T01:00:00Z", "Image": "sha256:" + "a" * 64, "Config": {"Labels": {"test.run": f.project}}}
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            obj["Id"] = "c" * 64
            runner.return_value.stdout = json.dumps([obj]).encode()
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.remove(item)

    def _create_helper_container_obj(self, f):
        return {
            "Id": "b" * 64,
            "Name": "/" + f.project + "-reader",
            "Created": "2026-10-13T01:00:00Z",
            "Image": "sha256:" + "a" * 64,
            "Config": {"Labels": {"test.run": f.project}},
            "HostConfig": {
                "Tmpfs": {
                    "/mosquitto/data": "rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883",
                    "/mosquitto/log": "rw,noexec,nosuid,nodev,size=1m,mode=0700,uid=1883,gid=1883",
                }
            },
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": str(f.private / "certs"),
                    "Destination": "/certs",
                    "Mode": "ro",
                    "RW": False,
                    "Propagation": "rprivate",
                },
                {
                    "Type": "volume",
                    "Name": "test_volume",
                    "Source": "/var/lib/docker/volumes/test_volume/_data",
                    "Destination": "/store",
                    "Driver": "local",
                    "Mode": "ro",
                    "RW": False,
                    "Propagation": "",
                },
            ],
        }

    def test_helper_mount_canonicalization_and_reorder_acceptance(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock()
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b"")
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])

            # Initial resolve
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Reordered inspect Mounts: volume first, then bind
            reordered_obj = copy.deepcopy(obj)
            reordered_obj["Mounts"] = list(reversed(obj["Mounts"]))
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([reordered_obj]).encode(), b"")

            resolved = registry.resolve(item)
            self.assertEqual(resolved["Id"], obj["Id"])

            # Resolve / remove symmetry after reorder
            removed_state = {"removed": False}
            def fake_runner(args, **kwargs):
                if "rm" in args:
                    removed_state["removed"] = True
                    return subprocess.CompletedProcess(args, 0, b"", b"")
                if "inspect" in args:
                    if removed_state["removed"]:
                        return subprocess.CompletedProcess(args, 1, b"", b"Error: No such container")
                    return subprocess.CompletedProcess(args, 0, json.dumps([reordered_obj]).encode(), b"")
                return subprocess.CompletedProcess(args, 0, json.dumps([reordered_obj]).encode(), b"")

            runner.side_effect = fake_runner
            registry.remove(item)
            self.assertTrue(item.get("removed", False))

    def test_helper_mount_attribute_mutations_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # 1. Volume Name mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Name"] = "mutated_volume_name"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 2. Volume Source path mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Source"] = "/var/lib/docker/volumes/other_volume/_data"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 3. Bind Source path mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["Source"] = str(f.private / "other_certs")
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 4. Driver mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Driver"] = "nfs"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 5. Mode mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["Mode"] = "rw"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 6. Propagation mutation
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["Propagation"] = "rshared"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 7. Type mutation (volume -> bind)
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Type"] = "bind"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

            # 8. RW flip (False -> True)
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][0]["RW"] = True
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_attributes_mismatch"):
                registry.resolve(item)

    def test_helper_mount_destinations_and_counts_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Destination mutated (/store -> /other_store)
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"][1]["Destination"] = "/other_store"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_destinations_mismatch"):
                registry.resolve(item)

            # Extra mount added
            mut_obj = copy.deepcopy(obj)
            extra = copy.deepcopy(obj["Mounts"][0])
            extra["Destination"] = "/extra"
            mut_obj["Mounts"].append(extra)
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_count_mismatch"):
                registry.resolve(item)

            # Missing mount
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"] = mut_obj["Mounts"][:1]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_count_mismatch"):
                registry.resolve(item)

    def test_helper_mount_malformed_and_duplicate_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Duplicate destination
            mut_obj = copy.deepcopy(obj)
            dup = copy.deepcopy(obj["Mounts"][0])
            mut_obj["Mounts"] = [obj["Mounts"][0], dup]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Wrong-type RW: string "false", int 0, int 1, None
            for bad_rw in ("false", 0, 1, None, []):
                mut_obj = copy.deepcopy(obj)
                mut_obj["Mounts"][0]["RW"] = bad_rw
                runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
                with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                    registry.resolve(item)

            # Mounts not a list
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"] = {"Destination": "/certs"}
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Mount element not a dict
            mut_obj = copy.deepcopy(obj)
            mut_obj["Mounts"] = ["/certs", "/store"]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Destination missing or not a string
            for bad_dest in (123, None, "", ["/certs"]):
                mut_obj = copy.deepcopy(obj)
                mut_obj["Mounts"][0]["Destination"] = bad_dest
                runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
                with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                    registry.resolve(item)

            # Tmpfs not a dict
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"] = ["/mosquitto/data"]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

            # Tmpfs options not a string
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/data"] = 123
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_mount_receipt_invalid"):
                registry.resolve(item)

    def test_helper_tmpfs_options_and_entries_rejected(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Tmpfs options changed
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/data"] = "rw,size=2m"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_tmpfs_mismatch"):
                registry.resolve(item)

            # Tmpfs entry added
            mut_obj = copy.deepcopy(obj)
            mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/extra"] = "rw,size=1m"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_tmpfs_mismatch"):
                registry.resolve(item)

            # Tmpfs entry removed
            mut_obj = copy.deepcopy(obj)
            del mut_obj["HostConfig"]["Tmpfs"]["/mosquitto/data"]
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "helper_tmpfs_mismatch"):
                registry.resolve(item)

    def test_helper_identity_protection_retained(self):
        import copy
        import subprocess
        with ExitStack() as stack:
            directory = stack.enter_context(tempfile.TemporaryDirectory())
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            runner = Mock(return_value=subprocess.CompletedProcess([], 0, json.dumps([obj]).encode(), b""))
            registry = smoke.ResourceRegistry(f, runner)
            item = registry.intent("container", obj["Name"][1:], obj["Image"], obj["Config"]["Labels"])
            registry.resolve(item)
            item["helper_mount_receipt"] = smoke.canonicalize_helper_mounts(obj)

            # Container Id mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Id"] = "f" * 64
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.resolve(item)

            # Container Created mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Created"] = "2026-10-14T12:00:00Z"
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_replacement_refused"):
                registry.resolve(item)

            # Container Labels mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Config"]["Labels"] = {"test.run": "wrong_project"}
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_ownership_refused"):
                registry.resolve(item)

            # Container Image mutated
            mut_obj = copy.deepcopy(obj)
            mut_obj["Image"] = "sha256:" + "9" * 64
            runner.return_value = subprocess.CompletedProcess([], 0, json.dumps([mut_obj]).encode(), b"")
            with self.assertRaisesRegex(ValueError, "resource_ownership_refused"):
                registry.resolve(item)

    def test_helper_receipt_json_roundtrip_stable(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            obj = self._create_helper_container_obj(f)
            receipt = smoke.canonicalize_helper_mounts(obj)

            # Must contain only JSON-native lists, dicts, primitives (no tuples)
            self.assertIsInstance(receipt["mounts"], list)
            self.assertIsInstance(receipt["tmpfs"], dict)
            for m in receipt["mounts"]:
                self.assertIsInstance(m, dict)
                self.assertIsInstance(m["RW"], bool)
                self.assertIsInstance(m["Destination"], str)
            for k, v in receipt["tmpfs"].items():
                self.assertIsInstance(k, str)
                self.assertIsInstance(v, str)

            # JSON roundtrip stability
            dumped = json.dumps(receipt)
            loaded = json.loads(dumped)
            self.assertEqual(loaded, receipt)

            # Re-canonicalization produces identical receipt
            re_canonical = smoke.canonicalize_helper_mounts(loaded)
            self.assertEqual(re_canonical, receipt)

    def test_helper_mount_mismatch_codes_survive_safe_report_and_diagnostic(self):
        new_codes = [
            "helper_mount_count_mismatch",
            "helper_mount_destinations_mismatch",
            "helper_mount_attributes_mismatch",
            "helper_tmpfs_mismatch",
            "helper_mount_receipt_invalid",
        ]
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            for code in new_codes:
                self.assertIn(code, smoke.ALLOWED_FAILURE_ERROR_CODES)
                rep = smoke.record_failure_report(f, phase="bootstrap", exc=ValueError(code), cleanup_verified=True)
                self.assertEqual(rep["error_code"], code)
                smoke.validate_failure_report(rep)

                diag = smoke.FailureDiagnostic(rep, cause=ValueError(code))
                self.assertEqual(str(diag), code)

            # Verify secrets do not leak into error_code
            secret = f.values["MQTT_PASSWORD"]
            leak_rep = smoke.record_failure_report(f, phase="bootstrap", exc=ValueError(f"leak_{secret}"), cleanup_verified=True)
            self.assertEqual(leak_rep["error_code"], "unclassified")
            self.assertNotIn(secret, json.dumps(leak_rep))


    def test_probe_startup_envoy_and_nginx_contracts(self):
        registry = Mock()
        running_obj = {"Id": "c" * 64, "State": {"Status": "running", "Running": True}}
        dead_obj = {"Id": "c" * 64, "State": {"Status": "dead", "Running": False}}
        stopped_obj = {"Id": "c" * 64, "State": {"Status": "created", "Running": False}}

        # Container exited or dead raises health_failed
        with self.assertRaisesRegex(ValueError, "health_failed"):
            smoke.probe_startup(registry, "supabase-envoy", dead_obj)

        # Container not running returns False without executing commands
        self.assertFalse(smoke.probe_startup(registry, "supabase-envoy", stopped_obj))
        registry.command.assert_not_called()

        # Envoy probe command: must use /bin/bash /dev/tcp, bounded timeout, no curl/wget
        registry.command.return_value = type("Res", (), {"returncode": 0})()
        self.assertTrue(smoke.probe_startup(registry, "supabase-envoy", running_obj))
        envoy_cmd = registry.command.call_args_list[0].args[0]
        self.assertEqual(envoy_cmd[:3], ["exec", "c" * 64, "/bin/bash"])
        script = envoy_cmd[4]
        self.assertIn("/dev/tcp/127.0.0.1/9901", script)
        self.assertIn("GET /ready HTTP/1.1", script)
        self.assertIn("HTTP/1.1 200 OK", script)
        self.assertIn("-t 2", script)  # bounded read timeout
        self.assertNotIn("curl", script)
        self.assertNotIn("wget", script)
        self.assertNotIn("token", script.lower())
        self.assertNotIn("secret", script.lower())

        # Envoy probe failure: non-zero return code returns False (timeout, connection failure, wrong status)
        registry.command.return_value = type("Res", (), {"returncode": 1})()
        self.assertFalse(smoke.probe_startup(registry, "supabase-envoy", running_obj))

        # Nginx probe command: checks /readyz endpoint, returncode 0 -> True, 1 -> False
        registry.command.reset_mock()
        registry.command.return_value = type("Res", (), {"returncode": 0})()
        self.assertTrue(smoke.probe_startup(registry, "nginx", running_obj))
        nginx_cmd = registry.command.call_args_list[0].args[0]
        self.assertEqual(nginx_cmd[:3], ["exec", "c" * 64, "wget"])
        self.assertIn("http://127.0.0.1/readyz", nginx_cmd)
        self.assertIn("-q", nginx_cmd)
        self.assertIn("-O", nginx_cmd)
        self.assertIn("/dev/null", nginx_cmd)

        # Nginx probe failure: non-zero return code returns False
        registry.command.return_value = type("Res", (), {"returncode": 1})()
        self.assertFalse(smoke.probe_startup(registry, "nginx", running_obj))

    def test_startup_phases_envoy_readiness_failure_stops_before_nginx(self):
        import json
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            model = {"networks": {"iot_net": {"name": f.project + "_iot_net", "labels": {}}},
                     "volumes": {},
                     "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}}
            image_inspect = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()

            registry = smoke.ResourceRegistry(f, Mock(side_effect=lambda args: type("Res", (), {
                "returncode": 0, "stdout": image_inspect, "stderr": b""})()))

            started_services = []

            def fake_run(cmd, **kwargs):
                if "up" in cmd:
                    srv = cmd[-1]
                    started_services.append(srv)
                return type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            # Mock probe_startup to fail when name == "supabase-envoy"
            def fake_probe(reg, name, obj):
                if name == "supabase-envoy":
                    return False
                return True

            # Mock startup_state to always return True for jobs / healthy services
            fake_state = lambda name, st: True

            with patch.object(smoke, "dry_effective_config", return_value=model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke, "allocate_bootstrap_store"), \
                 patch.object(smoke, "allocation_readback"), \
                 patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)), \
                 patch.object(smoke, "startup_state", side_effect=fake_state), \
                 patch.object(smoke, "probe_startup", side_effect=fake_probe), \
                 patch("subprocess.run", side_effect=fake_run):

                # Give registry.resolve None initially for collision check, then running container
                running_state = {"Status": "running", "Running": True, "ExitCode": 0}
                running_obj = {"Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"}}
                resolve_calls = 0
                def fake_resolve(item):
                    nonlocal resolve_calls
                    resolve_calls += 1
                    # During initial collision checks, resolve returns None
                    if resolve_calls <= len(smoke.SERVICES) + 1:
                        return None
                    item["receipt"] = {"id": "cid"}
                    return running_obj

                expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                            for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

                def fake_cmd(cmd):
                    if "psql" in cmd:
                        if "SELECT string_agg" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                        if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                    return type("Res", (), {"returncode": 0, "stdout": image_inspect, "stderr": b""})()

                with patch.object(registry, "resolve", side_effect=fake_resolve), \
                     patch.object(registry, "remove"), \
                     patch.object(registry, "command", side_effect=fake_cmd):
                    with self.assertRaisesRegex(ValueError, "phase_health_timeout"):
                        smoke.startup_only(f, registry=registry)

            # supabase-envoy was started, but nginx was NEVER started because envoy failed readiness
            self.assertIn("supabase-envoy", started_services)
            self.assertNotIn("nginx", started_services)

    def test_run_positive_http_slice_flow(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            registry = Mock()

            # Mock psql responses
            def fake_psql(cmd):
                sql = cmd[-1]
                if "SELECT count(*)" in sql:
                    return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            registry.command.side_effect = fake_psql

            # Mock HTTP calls
            def fake_http(base_url, method, path, *, token="", api_key="", headers=None, body=None, timeout=10.0):
                if path == "/auth/v1/admin/users":
                    return 200, {}, {"id": "11111111-2222-3333-4444-555555555555"}
                if path.startswith("/auth/v1/token"):
                    # Return valid standard JWT format with authenticated role
                    claims = base64.urlsafe_b64encode(json.dumps({"role": "authenticated", "aud": "authenticated"}).encode()).decode().rstrip("=")
                    tok = f"eyJhbGciOiJIUzI1NiJ9.{claims}.sig"
                    return 200, {}, {"access_token": tok, "token_type": "bearer"}
                if path == "/v1/admin/gateways":
                    # Extract gateway_id from query/body or provide matching item
                    gw_id = [arg for arg in fake_psql.last_sql if "INSERT INTO public.gateways" in arg] if hasattr(fake_psql, "last_sql") else []
                    return 200, {}, {"items": [{"gateway_id": "test_gw", "name": "test_name", "description": None, "created_at": None}], "next_cursor": None}
                if "/sensors/" in path:
                    status = 201 if getattr(fake_http, "sensor_called", False) is False else 200
                    fake_http.sensor_called = True
                    parts = path.split("/")
                    gw, sensor = parts[3], parts[5]
                    return status, {}, {
                        "gateway_id": gw, "sensor_id": sensor, "name": body["name"],
                        "unit": body["unit"], "entity_id": f"urn:ngsi-ld:Sensor:{gw}:{sensor}",
                        "created_at": None
                    }
                return 404, {}, {}

            def intercept_psql(cmd):
                sql = cmd[-1]
                if "INSERT INTO public.gateways" in sql:
                    # capture gw_id and gw_name
                    m = re.search(r"INSERT INTO public\.gateways \(gateway_id, name, description\) VALUES \('([^']+)', '([^']+)'", sql)
                    if m:
                        fake_http.seeded_gw_id = m.group(1)
                        fake_http.seeded_gw_name = m.group(2)
                if "SELECT count(*)" in sql:
                    return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            registry.command.side_effect = intercept_psql

            def updated_fake_http(base_url, method, path, *, token="", api_key="", headers=None, body=None, timeout=10.0):
                if path == "/auth/v1/admin/users":
                    return 200, {}, {"id": "11111111-2222-3333-4444-555555555555"}
                if path.startswith("/auth/v1/token"):
                    claims = base64.urlsafe_b64encode(json.dumps({"role": "authenticated", "aud": "authenticated"}).encode()).decode().rstrip("=")
                    tok = f"eyJhbGciOiJIUzI1NiJ9.{claims}.sig"
                    return 200, {}, {"access_token": tok, "token_type": "bearer"}
                if path == "/v1/admin/gateways":
                    return 200, {}, {"items": [{"gateway_id": getattr(fake_http, "seeded_gw_id", "gw_1"), "name": getattr(fake_http, "seeded_gw_name", "Positive Smoke"), "description": None, "created_at": None}], "next_cursor": None}
                if "/sensors/" in path:
                    status = 201 if getattr(fake_http, "sensor_called", False) is False else 200
                    fake_http.sensor_called = True
                    parts = path.split("/")
                    gw, sensor = parts[3], parts[5]
                    return status, {}, {
                        "gateway_id": gw, "sensor_id": sensor, "name": body["name"],
                        "unit": body["unit"], "entity_id": f"urn:ngsi-ld:Sensor:{gw}:{sensor}",
                        "created_at": None
                    }
                return 404, {}, {}

            with patch.object(smoke, "http_request", side_effect=updated_fake_http):
                ev = smoke.run_positive_http_slice(f, registry, "http://127.0.0.1:8080", "cid_postgres")
                self.assertEqual(ev["admin_inventory_status"], 200)
                self.assertEqual(ev["owner_sensor_create_status"], 201)
                self.assertEqual(ev["owner_sensor_retry_status"], 200)
                self.assertTrue(ev["db_guarantees_verified"])

    def test_startup_with_positive_http_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            model = {"networks": {"iot_net": {"name": f.project + "_iot_net", "labels": {}}},
                     "volumes": {},
                     "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}}
            image_inspect = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()

            registry = smoke.ResourceRegistry(f, Mock(side_effect=lambda args: type("Res", (), {
                "returncode": 0, "stdout": image_inspect, "stderr": b""})()))

            def fake_state(name, st):
                return True

            def fake_probe(reg, name, obj):
                return True

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": image_inspect, "stderr": b""})()

            running_state = {"Status": "running", "Running": True, "ExitCode": 0}
            running_obj = {
                "Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            resolve_calls = 0
            def fake_resolve(item):
                nonlocal resolve_calls
                resolve_calls += 1
                if resolve_calls <= len(smoke.SERVICES) + 1:
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            mock_lifecycle = smoke.bootstrap_lifecycle()
            with patch.object(smoke, "dry_effective_config", return_value=model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke, "allocate_bootstrap_store"), \
                 patch.object(smoke, "allocation_readback"), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)), \
                 patch.object(smoke, "startup_state", side_effect=fake_state), \
                 patch.object(smoke, "probe_startup", side_effect=fake_probe), \
                 patch.object(smoke, "run_positive_http_slice", return_value={"admin_inventory_status": 200, "owner_sensor_create_status": 201, "owner_sensor_retry_status": 200, "db_guarantees_verified": True}), \
                 patch.object(registry, "resolve", side_effect=fake_resolve), \
                 patch.object(registry, "remove"), \
                 patch.object(registry, "command", side_effect=fake_cmd), \
                 patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()):

                rep = smoke.startup_only(f, registry=registry, positive_http=True)
                self.assertEqual(rep["status"], "POSITIVE_HTTP_VERIFIED_NOT_RBAC_ACCEPTED")
                self.assertFalse(rep["http_accepted"])
                self.assertTrue(rep["exact_cleanup_verified"])
                self.assertEqual(rep["positive_verifications"]["admin_inventory"], True)
                smoke.validate_summary(rep)

    def test_safe_failure_report_schema_and_allowlists(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            valid_report = {
                "schema_version": 1,
                "status": "STARTUP_FAILED",
                "phase": "preflight",
                "error_type": "ValueError",
                "error_code": "local_image_unavailable",
                "http_accepted": False,
                "cleanup_verified": False,
                "primary_phase": None,
                "primary_error_code": None,
                "cleanup_step": None,
                "cleanup_error_code": None,
                "resource_kind": None,
            }
            smoke.validate_failure_report(valid_report)
            smoke.record_failure_report(f, phase="preflight", exc=ValueError("local_image_unavailable"), cleanup_verified=False)
            failure_path = f.artifacts / "smoke-failure.json"
            self.assertTrue(failure_path.exists())
            loaded = json.loads(failure_path.read_text())
            smoke.validate_failure_report(loaded)
            self.assertEqual(loaded["error_code"], "local_image_unavailable")
            self.assertFalse(loaded["http_accepted"])
            self.assertFalse(loaded["cleanup_verified"])

            # Null semantics: when phase is not cleanup, all 5 new fields must be None
            for key in ("primary_phase", "primary_error_code", "cleanup_step", "cleanup_error_code", "resource_kind"):
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, **{key: "not_none"}))

            # When phase is cleanup:
            cleanup_report = {
                "schema_version": 1,
                "status": "STARTUP_FAILED",
                "phase": "cleanup",
                "error_type": "ValueError",
                "error_code": "startup_cleanup_unresolved_evidence_retained",
                "http_accepted": False,
                "cleanup_verified": False,
                "primary_phase": "bootstrap",
                "primary_error_code": "bootstrap_contract_rejected",
                "cleanup_step": "remove_resource",
                "cleanup_error_code": "resource_cleanup_unresolved",
                "resource_kind": "container",
            }
            smoke.validate_failure_report(cleanup_report)

            # cleanup_verified must be False for cleanup phase
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_verified=True))

            # cleanup_step must be in ALLOWED_CLEANUP_STEPS
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step="unauthorized_step"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step=None))

            # cleanup_error_code must be in ALLOWED_FAILURE_ERROR_CODES
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_error_code="evil_code"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_error_code=None))

            # resource_kind must be in ALLOWED_RESOURCE_KINDS or None
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, resource_kind="unknown_kind"))
            smoke.validate_failure_report(dict(cleanup_report, resource_kind=None))

            # primary_phase and primary_error_code must either both be set or both be None
            smoke.validate_failure_report(dict(cleanup_report, primary_phase=None, primary_error_code=None))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, primary_phase="bootstrap", primary_error_code=None))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, primary_phase=None, primary_error_code="bootstrap_contract_rejected"))

            # resource_kind may be non-null only when cleanup_step is remove_resource
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step="check_deadline", resource_kind="container"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(cleanup_report, cleanup_step="reconcile_child_receipts", resource_kind="container"))
            smoke.validate_failure_report(dict(cleanup_report, cleanup_step="check_deadline", resource_kind=None))
            smoke.validate_failure_report(dict(cleanup_report, cleanup_step="reconcile_child_receipts", resource_kind=None))

            # Each nullable string field rejects wrong types (list, dict, bool, int, etc.) with safe ValueError
            for field in ("cleanup_step", "cleanup_error_code", "resource_kind", "primary_phase", "primary_error_code"):
                for bad_val in ([1, 2], {"bad": "type"}, True, False, 123, 1.5):
                    with self.assertRaises(ValueError):
                        smoke.validate_failure_report(dict(cleanup_report, **{field: bad_val}))

            # Test invalid schemas: extra field, wrong types, unknown values, bool-as-int
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, extra="unexpected"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, schema_version=2))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, status="SUCCESS"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, phase="unknown_phase"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, error_type="EvilException"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, error_code="evil_code"))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, http_accepted=True))
            # bool-as-int: 1 or 0 instead of True/False
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, http_accepted=0))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, cleanup_verified=1))
            # strict schema_version true and 1.0 rejection
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, schema_version=True))
            with self.assertRaises(ValueError):
                smoke.validate_failure_report(dict(valid_report, schema_version=1.0))
            # wrong unhashable phase/type/code/status values rejected predictably with ValueError, not TypeError
            for unhashable in ([1, 2], {"bad": "type"}):
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, phase=unhashable))
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, error_type=unhashable))
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, error_code=unhashable))
                with self.assertRaises(ValueError):
                    smoke.validate_failure_report(dict(valid_report, status=unhashable))

    def test_adversarial_exception_secret_leak_prevention(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            # Adversarial exception whose text is a plausible lowercase secret
            secret_exc = ValueError("super_secret_token_12345")
            report = smoke.record_failure_report(f, phase="positive_http", exc=secret_exc, cleanup_verified=False)
            self.assertEqual(report["error_code"], "unclassified")
            self.assertEqual(report["error_type"], "ValueError")
            self.assertNotIn("super_secret_token_12345", json.dumps(report))

            # Attacker-defined exception subclass with evil __str__
            class AttackerExploitError(ValueError):
                def __init__(self):
                    self.called = False
                def __str__(self):
                    self.called = True
                    raise RuntimeError("exploit __str__ invoked")
            custom_exc = AttackerExploitError()
            report2 = smoke.record_failure_report(f, phase="positive_http", exc=custom_exc, cleanup_verified=False)
            self.assertEqual(report2["error_type"], "Exception")
            self.assertEqual(report2["error_code"], "unclassified")
            self.assertFalse(custom_exc.called)

            # Class named ValueError that is not builtin
            class FakeValueError(Exception):
                pass
            FakeValueError.__name__ = "ValueError"
            fake_exc = FakeValueError("fake")
            report3 = smoke.record_failure_report(f, phase="positive_http", exc=fake_exc, cleanup_verified=False)
            self.assertEqual(report3["error_type"], "Exception")
            self.assertEqual(report3["error_code"], "unclassified")

    def test_unsafe_prepare_path_does_not_write_anything(self):
        with tempfile.TemporaryDirectory() as directory:
            unsafe_dir = Path(directory) / "unsafe symlink dir"
            os.symlink(directory, unsafe_dir)
            with self.assertRaises(ValueError):
                smoke.prepare(unsafe_dir / "fixture")
            self.assertFalse((unsafe_dir / "fixture").exists())

    def test_main_prepare_failure_emits_safe_json_stdout_without_writes(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            unsafe_dir = Path(directory) / "unsafe_symlink_dir"
            os.symlink(directory, unsafe_dir)
            prep_target = unsafe_dir / "fixture"
            stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_target)]), \
                 patch("sys.stdout", stdout_buf), patch("sys.stderr", stderr_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            self.assertEqual(stderr_buf.getvalue(), "")
            out_str = stdout_buf.getvalue().strip()
            rep = json.loads(out_str)
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "prepare")
            self.assertEqual(rep["error_code"], "unsafe_fixture_path")
            self.assertFalse(rep["cleanup_verified"])
            self.assertFalse(prep_target.exists())

    def test_main_preflight_dry_config_failure_propagation(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", side_effect=ValueError("compose_preflight_failed")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            self.assertEqual(rep["error_code"], "compose_preflight_failed")
            self.assertFalse(rep["cleanup_verified"])
            # Dedicated artifact matches stdout
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_bootstrap_failure_preserves_phase_and_cleanup_true(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=ValueError("bootstrap_contract_rejected")), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "bootstrap")
            self.assertEqual(rep["error_code"], "bootstrap_contract_rejected")
            self.assertTrue(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_service_envoy_readiness_failure_preserves_envoy_phase(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            def fake_dry(fixture, **kwargs):
                return {
                    "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                    "volumes": {},
                    "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()

            def fake_probe(reg, name, obj):
                if name == "supabase-envoy":
                    return False
                return True

            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            running_obj = {
                "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            def fake_resolve(item):
                if not item.get("allocation_attempted"):
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", side_effect=fake_dry), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc), \
                 patch.object(smoke, "allocation_readback", return_value=True), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)), \
                 patch.object(smoke, "startup_state", return_value=True), \
                 patch.object(smoke, "probe_startup", side_effect=fake_probe), \
                 patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()), \
                 patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve), \
                 patch.object(smoke.ResourceRegistry, "remove"), \
                 patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "supabase-envoy")
            self.assertEqual(rep["error_code"], "phase_health_timeout")
            self.assertTrue(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_http_phases_preservation(self):
        import io
        for flag, failing_phase, helper_to_fail in (
            ("--positive-http-only", "positive_http", "run_positive_http_slice"),
            ("--role-http-only", "role_http", "run_role_http_slice"),
            ("--full-http", "jwt_http", "run_jwt_denial_slice"),
        ):
            with tempfile.TemporaryDirectory() as directory:
                prep_dir = Path(directory) / "fixture"
                def fake_dry(fixture, **kwargs):
                    return {
                        "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                        "volumes": {},
                        "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                    }
                img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
                mock_lifecycle = smoke.bootstrap_lifecycle()
                def fake_alloc(fixture, registry, lifecycle):
                    item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                    registry.allocating(item)
                    return item

                running_obj = {
                    "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                    "receipt": {"id": "cid"},
                    "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
                }
                def fake_resolve(item):
                    if not item.get("allocation_attempted"):
                        return None
                    item["receipt"] = {"id": "cid"}
                    return running_obj

                expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                            for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
                def fake_cmd(cmd):
                    if "psql" in cmd:
                        if "SELECT string_agg" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                        if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                    return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

                dummy_positive = {"admin_inventory_status": 200, "owner_sensor_create_status": 201, "owner_sensor_retry_status": 200, "db_guarantees_verified": True}
                dummy_role = {"role_cases": {c["label"]: c["status"] for c in smoke.role_http_cases()}, "role_snapshot_comparisons": 10, "gateway_id": "gw", "existing_sensor": "s"}

                patches = [
                    patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), flag]),
                    patch.object(smoke, "dry_effective_config", side_effect=fake_dry),
                    patch.object(smoke, "build_native_image"),
                    patch.object(smoke, "generate_fixture_tls"),
                    patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc),
                    patch.object(smoke, "allocation_readback", return_value=True),
                    patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle),
                    patch.object(mock_lifecycle, "reconcile_child_receipts"),
                    patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)),
                    patch.object(smoke, "startup_state", return_value=True),
                    patch.object(smoke, "probe_startup", return_value=True),
                    patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()),
                    patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve),
                    patch.object(smoke.ResourceRegistry, "remove"),
                    patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd),
                ]
                if helper_to_fail == "run_positive_http_slice":
                    patches.append(patch.object(smoke, "run_positive_http_slice", side_effect=ValueError("response_contract_failed")))
                elif helper_to_fail == "run_role_http_slice":
                    patches.append(patch.object(smoke, "run_positive_http_slice", return_value=dummy_positive))
                    patches.append(patch.object(smoke, "run_role_http_slice", side_effect=ValueError("role_case_inventory_owner_contract_failed")))
                elif helper_to_fail == "run_jwt_denial_slice":
                    patches.append(patch.object(smoke, "run_positive_http_slice", return_value=dummy_positive))
                    patches.append(patch.object(smoke, "run_role_http_slice", return_value=dummy_role))
                    patches.append(patch.object(smoke, "run_jwt_denial_slice", side_effect=ValueError("response_contract_failed")))

                import contextlib
                stdout_buf = io.StringIO()
                patches.append(patch("sys.stdout", stdout_buf))

                with contextlib.ExitStack() as stack:
                    for p in patches:
                        stack.enter_context(p)
                    ret = smoke.main()
                self.assertEqual(ret, 1)
                rep = json.loads(stdout_buf.getvalue().strip())
                smoke.validate_failure_report(rep)
                self.assertEqual(rep["phase"], failing_phase)
                self.assertTrue(rep["cleanup_verified"])
                art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
                self.assertEqual(rep, art)

    def test_main_cleanup_failure_overrides_primary_phase(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                raise ValueError("bootstrap_contract_rejected")

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=RuntimeError("cleanup_daemon_dead")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)
            self.assertEqual(rep["primary_phase"], "bootstrap")
            self.assertEqual(rep["primary_error_code"], "bootstrap_contract_rejected")
            self.assertEqual(rep["cleanup_step"], "reconcile_child_receipts")
            self.assertEqual(rep["cleanup_error_code"], "unclassified")
            self.assertIsNone(rep["resource_kind"])

    def test_cleanup_failure_reconcile_child_receipts_with_incomplete_cleanup(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                raise ValueError("bootstrap_contract_rejected")

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=mock_model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=mock_lifecycle.IncompleteCleanup("invalid recovery receipt")))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertEqual(rep["primary_phase"], "bootstrap")
            self.assertEqual(rep["primary_error_code"], "bootstrap_contract_rejected")
            self.assertEqual(rep["cleanup_step"], "reconcile_child_receipts")
            self.assertEqual(rep["cleanup_error_code"], "child_cleanup_unresolved")
            self.assertIsNone(rep["resource_kind"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_cleanup_failure_resource_removal_container_and_kinds(self):
        import io, contextlib
        for target_kind in ("container", "network", "volume"):
            with tempfile.TemporaryDirectory() as directory:
                prep_dir = Path(directory) / "fixture"
                def fake_dry(fixture, **kwargs):
                    return {
                        "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                        "volumes": {},
                        "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                    }
                img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
                mock_lifecycle = smoke.bootstrap_lifecycle()
                def fake_alloc(fixture, registry, lifecycle):
                    item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                    registry.allocating(item)
                    return item

                running_obj = {
                    "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                    "receipt": {"id": "cid"},
                    "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
                }
                def fake_resolve(item):
                    if not item.get("allocation_attempted"):
                        return None
                    item["receipt"] = {"id": "cid"}
                    return running_obj

                expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                            for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
                def fake_cmd(cmd):
                    if "psql" in cmd:
                        if "SELECT string_agg" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                        if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                            return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                    return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

                def fake_remove(self, item):
                    if item.get("kind") == target_kind:
                        raise ValueError("resource_cleanup_unresolved")
                    item["removed"] = True

                stdout_buf = io.StringIO()
                with contextlib.ExitStack() as stack:
                    stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                    stack.enter_context(patch.object(smoke, "dry_effective_config", side_effect=fake_dry))
                    stack.enter_context(patch.object(smoke, "build_native_image"))
                    stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                    stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                    stack.enter_context(patch.object(smoke, "allocation_readback", return_value=True))
                    stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                    stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                    stack.enter_context(patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)))
                    stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                    stack.enter_context(patch.object(smoke, "probe_startup", side_effect=lambda reg, name, obj: False if name == "supabase-envoy" else True))
                    stack.enter_context(patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()))
                    stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                    stack.enter_context(patch.object(smoke.ResourceRegistry, "remove", fake_remove))
                    stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                    stack.enter_context(patch("sys.stdout", stdout_buf))
                    ret = smoke.main()
                self.assertEqual(ret, 1)
                rep = json.loads(stdout_buf.getvalue().strip())
                smoke.validate_failure_report(rep)
                self.assertEqual(rep["phase"], "cleanup")
                self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
                self.assertFalse(rep["cleanup_verified"])
                self.assertEqual(rep["primary_phase"], "supabase-envoy")
                self.assertEqual(rep["primary_error_code"], "phase_health_timeout")
                self.assertEqual(rep["cleanup_step"], "remove_resource")
                self.assertEqual(rep["cleanup_error_code"], "resource_cleanup_unresolved")
                self.assertEqual(rep["resource_kind"], target_kind)
                art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
                self.assertEqual(rep, art)

    def test_cleanup_failure_resource_ownership_refusal_network(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            def fake_dry(fixture, **kwargs):
                return {
                    "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                    "volumes": {},
                    "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            running_obj = {
                "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            def fake_resolve(item):
                if not item.get("allocation_attempted"):
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

            def fake_remove(self, item):
                if item.get("kind") == "network":
                    raise ValueError("resource_ownership_refused")
                item["removed"] = True

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", side_effect=fake_dry))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "allocation_readback", return_value=True))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "remove", fake_remove))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertIsNone(rep["primary_phase"])
            self.assertIsNone(rep["primary_error_code"])
            self.assertEqual(rep["cleanup_step"], "remove_resource")
            self.assertEqual(rep["cleanup_error_code"], "resource_ownership_refused")
            self.assertEqual(rep["resource_kind"], "network")
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_cleanup_failure_deadline_with_preceding_http_failure(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            def fake_dry(fixture, **kwargs):
                return {
                    "networks": {"iot_net": {"name": fixture.project + "_iot_net", "labels": {}}},
                    "volumes": {},
                    "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
                }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            running_obj = {
                "Image": "img", "Id": "cid", "State": {"Status": "running", "Running": True, "ExitCode": 0},
                "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            def fake_resolve(item):
                if not item.get("allocation_attempted"):
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))
            def fake_cmd(cmd):
                if "psql" in cmd:
                    if "SELECT string_agg" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": schema_query_result(expected_schema, cmd[-1]).encode(), "stderr": b""})()
                    if "profiles_id_fkey" in cmd[-1] or "on_auth_user_created" in cmd[-1]:
                        return type("Res", (), {"returncode": 0, "stdout": b"1\n", "stderr": b""})()
                return type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()

            real_monotonic = time.monotonic
            call_count = [0]
            def fake_monotonic():
                call_count[0] += 1
                if call_count[0] > 10:
                    return real_monotonic() + 100000.0
                return real_monotonic()

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--positive-http-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", side_effect=fake_dry))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "allocation_readback", return_value=True))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch.object(smoke, "BUDGETS", dict(smoke.BUDGETS, up=0.01)))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch("subprocess.run", return_value=type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch.object(smoke, "run_positive_http_slice", side_effect=ValueError("response_contract_failed")))
                stack.enter_context(patch("time.monotonic", side_effect=fake_monotonic))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertEqual(rep["primary_phase"], "positive_http")
            self.assertEqual(rep["primary_error_code"], "response_contract_failed")
            self.assertEqual(rep["cleanup_step"], "check_deadline")
            self.assertEqual(rep["cleanup_error_code"], "startup_cleanup_deadline")
            self.assertIsNone(rep["resource_kind"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_cleanup_failure_unknown_secret_bearing_exception_redacted(self):
        import io, contextlib
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.values["MQTT_DYNSEC_BOOTSTRAP_VOLUME"], "sha256:" + "a" * 64, {"test.task": "native-compose-allocation", "test.run": fixture.project})
                registry.allocating(item)
                return item

            stdout_buf = io.StringIO()
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=mock_model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=RuntimeError("leak_secret_key_abcdef12345")))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])
            self.assertEqual(rep["cleanup_step"], "reconcile_child_receipts")
            self.assertEqual(rep["cleanup_error_code"], "unclassified")
            self.assertNotIn("leak_secret_key_abcdef12345", json.dumps(rep))
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)
            self.assertNotIn("leak_secret_key_abcdef12345", json.dumps(art))

    def test_main_report_validation_failure_preserves_its_own_phase(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            dummy_summary = {"status": "RBAC_JWT_HTTP_ACCEPTED", "exact_cleanup_verified": True}
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--full-http"]), \
                 patch.object(smoke, "startup_only", return_value=dummy_summary), \
                 patch.object(smoke, "validate_summary", side_effect=ValueError("unsafe_summary")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "report_validation")
            self.assertEqual(rep["error_code"], "unsafe_summary")
            self.assertTrue(rep["cleanup_verified"])
            art = json.loads((prep_dir / "artifacts" / "smoke-failure.json").read_text())
            self.assertEqual(rep, art)

    def test_main_secret_bearing_service_error_sanitized(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            secret_leak = "password_super_secret_12345"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=ValueError(secret_leak)), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            raw_out = stdout_buf.getvalue()
            self.assertNotIn(secret_leak, raw_out)
            rep = json.loads(raw_out.strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["error_code"], "unclassified")
            self.assertTrue(rep["cleanup_verified"])

    def test_main_replaced_artifact_directory_symlink_refuses_writes(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            # First prepare properly
            fixture = smoke.prepare(prep_dir)
            # Now replace artifacts directory with a newly created dir (different inode) or symlink
            import shutil
            shutil.rmtree(fixture.artifacts)
            os.symlink(directory, fixture.artifacts)
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "prepare", return_value=fixture), \
                 patch.object(smoke, "dry_effective_config", side_effect=ValueError("compose_preflight_failed")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            # Artifact was NOT written through symlink
            self.assertFalse((fixture.artifacts / "smoke-failure.json").exists())

    def test_main_artifact_persistence_failure_emits_json_and_omits_artifact(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", side_effect=ValueError("compose_preflight_failed")), \
                 patch.object(smoke, "durable_json", side_effect=OSError("disk read-only")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            self.assertFalse((prep_dir / "artifacts" / "smoke-failure.json").exists())

    def test_cleanup_failure_reports_unresolved_not_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            f = smoke.prepare(Path(directory) / "fixture")
            # If cleanup failed, cleanup_verified must be false
            report = smoke.record_failure_report(f, phase="cleanup", exc=ValueError("startup_cleanup_unresolved_evidence_retained"), cleanup_verified=False)
            self.assertEqual(report["phase"], "cleanup")
            self.assertFalse(report["cleanup_verified"])
            self.assertEqual(report["error_code"], "startup_cleanup_unresolved_evidence_retained")

    def test_main_attacker_smoke_diagnostic_attribute_not_printed(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            attacker_secret = "secret_leak_in_smoke_diag_12345"
            exc = RuntimeError("attacker_controlled")
            exc._smoke_diagnostic = {"token": attacker_secret, "status": "EXPLOITED"}
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "startup_only", side_effect=exc), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            raw_out = stdout_buf.getvalue()
            self.assertNotIn(attacker_secret, raw_out)
            rep = json.loads(raw_out.strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")
            self.assertEqual(rep["error_code"], "unclassified")

    def test_main_hostile_getattribute_for_smoke_diagnostic_not_invoked(self):
        import io
        class HostileException(RuntimeError):
            def __getattribute__(self, name):
                if name == "_smoke_diagnostic":
                    raise AssertionError("hostile attribute accessed")
                return super().__getattribute__(name)

        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "startup_only", side_effect=HostileException("boom")), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "preflight")

    def test_main_malformed_failure_diagnostic_fails_closed(self):
        import io
        secret_text = "super_secret_token_123"
        malformed_reports = [
            # Extra fields
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "preflight",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": False, "cleanup_verified": False, "extra_leak": secret_text},
            # Secret-bearing values
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "preflight",
             "error_type": "RuntimeError", "error_code": secret_text,
             "http_accepted": False, "cleanup_verified": False,
             "primary_phase": None, "primary_error_code": None,
             "cleanup_step": None, "cleanup_error_code": None,
             "resource_kind": None},
            # Malformed new fields with wrong types / secret payload
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "cleanup",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": False, "cleanup_verified": False,
             "primary_phase": [secret_text], "primary_error_code": None,
             "cleanup_step": {"bad": secret_text}, "cleanup_error_code": [1, 2],
             "resource_kind": 123},
            # Context contradiction carrying secret in resource_kind
            {"schema_version": 1, "status": "STARTUP_FAILED", "phase": "cleanup",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": False, "cleanup_verified": False,
             "primary_phase": None, "primary_error_code": None,
             "cleanup_step": "check_deadline", "cleanup_error_code": "resource_cleanup_unresolved",
             "resource_kind": "container"},
            # False success
            {"schema_version": 1, "status": "RBAC_JWT_HTTP_ACCEPTED", "phase": "preflight",
             "error_type": "RuntimeError", "error_code": "unclassified",
             "http_accepted": True, "cleanup_verified": True},
        ]
        for bad_rep in malformed_reports:
            with tempfile.TemporaryDirectory() as directory:
                prep_dir = Path(directory) / "fixture"
                stdout_buf = io.StringIO()
                diag = smoke.FailureDiagnostic(bad_rep)
                with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir)]), \
                     patch.object(smoke, "summary", side_effect=diag), \
                     patch("sys.stdout", stdout_buf):
                    ret = smoke.main()
                self.assertEqual(ret, 1)
                raw_out = stdout_buf.getvalue()
                self.assertNotIn(secret_text, raw_out)
                rep = json.loads(raw_out.strip())
                smoke.validate_failure_report(rep)
                self.assertEqual(rep["status"], "STARTUP_FAILED")
                self.assertFalse(rep["http_accepted"])

    def test_main_custom_report_object_repr_str_not_invoked(self):
        import io
        secret_repr = "custom_repr_secret_token_abc"
        class HostileReport:
            def __repr__(self):
                return secret_repr
            def __str__(self):
                return secret_repr

        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            stdout_buf = io.StringIO()
            diag = smoke.FailureDiagnostic(HostileReport())
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir)]), \
                 patch.object(smoke, "summary", side_effect=diag), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            raw_out = stdout_buf.getvalue()
            self.assertNotIn(secret_repr, raw_out)
            rep = json.loads(raw_out.strip())
            smoke.validate_failure_report(rep)

    def test_full_http_summary_failure_classified_as_report_validation(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            f = smoke.prepare(prep_dir)
            model = smoke.dry_effective_config(f)
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            running_state = {"Status": "running", "Running": True, "ExitCode": 0}
            running_obj = {
                "Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            started = [False]
            def fake_resolve(item):
                if not started[0]:
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            def fake_subp(*args, **kwargs):
                started[0] = True
                return type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

            def fake_cmd(cmd):
                if 'psql' in cmd:
                    if 'SELECT string_agg' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': schema_query_result(expected_schema, cmd[-1]).encode(), 'stderr': b''})()
                    if 'profiles_id_fkey' in cmd[-1] or 'on_auth_user_created' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': b'1\n', 'stderr': b''})()
                return type('Res', (), {'returncode': 0, 'stdout': img_out, 'stderr': b''})()

            stdout_buf = io.StringIO()
            with ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--full-http"]))
                stack.enter_context(patch.object(smoke, "prepare", return_value=f))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "remove"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store"))
                stack.enter_context(patch.object(smoke, "allocation_readback"))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch.object(smoke, "run_positive_http_slice", return_value={"admin_inventory_status": 200, "owner_sensor_create_status": 201, "owner_sensor_retry_status": 200, "db_guarantees_verified": True}))
                stack.enter_context(patch.object(smoke, "run_role_http_slice", return_value={"gateway_id": "gw1", "existing_sensor": "s1", "role_cases": {c["label"]: c["status"] for c in smoke.role_http_cases()}, "role_snapshot_comparisons": 10}))
                stack.enter_context(patch.object(smoke, "run_jwt_denial_slice", return_value={"jwt_cases": {c["label"]: c["status"] for c in smoke.jwt_http_cases()}, "jwt_snapshot_comparisons": 6}))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch("subprocess.run", side_effect=fake_subp))
                stack.enter_context(patch.object(smoke, "full_http_summary", side_effect=ValueError("cleanup_acceptance_incomplete")))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "report_validation")
            self.assertEqual(rep["error_code"], "cleanup_acceptance_incomplete")
            self.assertTrue(rep["cleanup_verified"])

    def test_durable_startup_summary_write_failure_classified_as_report_validation(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            f = smoke.prepare(prep_dir)
            model = smoke.dry_effective_config(f)
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            running_state = {"Status": "running", "Running": True, "ExitCode": 0}
            running_obj = {
                "Image": "img", "Id": "cid", "State": running_state, "receipt": {"id": "cid"},
                "NetworkSettings": {"Ports": {"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}
            }
            started = [False]
            def fake_resolve(item):
                if not started[0]:
                    return None
                item["receipt"] = {"id": "cid"}
                return running_obj

            def fake_subp(*args, **kwargs):
                started[0] = True
                return type("Proc", (), {"returncode": 0, "stdout": b"", "stderr": b""})()

            expected_schema = ','.join(str(int(p.name.split('_')[0])) + ':' + p.name.split('_', 1)[1].removesuffix('.up.sql')
                                        for p in sorted((smoke.ROOT / 'migrations').glob('0*.up.sql')) if int(p.name.split('_')[0]) not in (4, 5))

            def fake_cmd(cmd):
                if 'psql' in cmd:
                    if 'SELECT string_agg' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': schema_query_result(expected_schema, cmd[-1]).encode(), 'stderr': b''})()
                    if 'profiles_id_fkey' in cmd[-1] or 'on_auth_user_created' in cmd[-1]:
                        return type('Res', (), {'returncode': 0, 'stdout': b'1\n', 'stderr': b''})()
                return type('Res', (), {'returncode': 0, 'stdout': img_out, 'stderr': b''})()

            real_durable_json = smoke.durable_json
            def selective_durable_json(path, data):
                if str(path).endswith("startup-summary.json") and data.get("status") == "STARTUP_VERIFIED_NOT_HTTP_ACCEPTED":
                    raise OSError("disk_write_error")
                return real_durable_json(path, data)

            stdout_buf = io.StringIO()
            with ExitStack() as stack:
                stack.enter_context(patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]))
                stack.enter_context(patch.object(smoke, "prepare", return_value=f))
                stack.enter_context(patch.object(smoke, "dry_effective_config", return_value=model))
                stack.enter_context(patch.object(smoke, "build_native_image"))
                stack.enter_context(patch.object(smoke, "generate_fixture_tls"))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "command", side_effect=fake_cmd))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "resolve", side_effect=fake_resolve))
                stack.enter_context(patch.object(smoke.ResourceRegistry, "remove"))
                stack.enter_context(patch.object(smoke, "allocate_bootstrap_store"))
                stack.enter_context(patch.object(smoke, "allocation_readback"))
                stack.enter_context(patch.object(smoke, "startup_state", return_value=True))
                stack.enter_context(patch.object(smoke, "probe_startup", return_value=True))
                stack.enter_context(patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle))
                stack.enter_context(patch.object(mock_lifecycle, "reconcile_child_receipts"))
                stack.enter_context(patch("subprocess.run", side_effect=fake_subp))
                stack.enter_context(patch.object(smoke, "durable_json", side_effect=selective_durable_json))
                stack.enter_context(patch("sys.stdout", stdout_buf))
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "report_validation")
            self.assertTrue(rep["cleanup_verified"])

    def test_private_diagnostic_write_failure_does_not_suppress_primary_failure(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            real_durable_json = smoke.durable_json
            def selective_durable_json(path, data):
                # Fail private startup-failure.json write
                if str(path).endswith("startup-failure.json"):
                    raise OSError("private_diagnostic_write_failed")
                return real_durable_json(path, data)

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=ValueError("bootstrap_contract_rejected")), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts"), \
                 patch.object(smoke, "durable_json", side_effect=selective_durable_json), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            # Primary controlled phase must be bootstrap, not preflight
            self.assertEqual(rep["phase"], "bootstrap")
            self.assertEqual(rep["error_code"], "bootstrap_contract_rejected")
            self.assertTrue(rep["cleanup_verified"])

    def test_private_diagnostic_write_failure_does_not_suppress_cleanup_failure(self):
        import io
        with tempfile.TemporaryDirectory() as directory:
            prep_dir = Path(directory) / "fixture"
            mock_model = {
                "networks": {"iot_net": {"name": "ncs_test_iot_net", "labels": {}}},
                "volumes": {},
                "services": {name: {"image": "img:" + name, "labels": {}, "volumes": []} for name in smoke.SERVICES}
            }
            img_out = json.dumps([{"Id": "sha256:" + "a" * 64, "Config": {"User": "1000", "Volumes": None}}]).encode()
            mock_lifecycle = smoke.bootstrap_lifecycle()
            real_durable_json = smoke.durable_json
            def selective_durable_json(path, data):
                # Fail cleanup startup-summary.json write
                if str(path).endswith("startup-summary.json") and data.get("status") == "CLEANUP_UNRESOLVED":
                    raise OSError("cleanup_summary_write_failed")
                return real_durable_json(path, data)

            def fake_alloc(fixture, registry, lifecycle):
                item = registry.intent("volume", fixture.project + "_store", "img1", {})
                registry.allocating(item)
                raise ValueError("bootstrap_contract_rejected")

            stdout_buf = io.StringIO()
            with patch("sys.argv", ["native_compose_smoke.py", "--prepare", str(prep_dir), "--startup-only"]), \
                 patch.object(smoke, "dry_effective_config", return_value=mock_model), \
                 patch.object(smoke, "build_native_image"), \
                 patch.object(smoke, "generate_fixture_tls"), \
                 patch.object(smoke.ResourceRegistry, "command", return_value=type("Res", (), {"returncode": 0, "stdout": img_out, "stderr": b""})()), \
                 patch.object(smoke, "allocate_bootstrap_store", side_effect=fake_alloc), \
                 patch.object(smoke, "bootstrap_lifecycle", return_value=mock_lifecycle), \
                 patch.object(mock_lifecycle, "reconcile_child_receipts", side_effect=RuntimeError("cleanup_daemon_dead")), \
                 patch.object(smoke, "durable_json", side_effect=selective_durable_json), \
                 patch("sys.stdout", stdout_buf):
                ret = smoke.main()
            self.assertEqual(ret, 1)
            rep = json.loads(stdout_buf.getvalue().strip())
            smoke.validate_failure_report(rep)
            self.assertEqual(rep["phase"], "cleanup")
            self.assertEqual(rep["error_code"], "startup_cleanup_unresolved_evidence_retained")
            self.assertFalse(rep["cleanup_verified"])


if __name__ == "__main__":
    unittest.main()
