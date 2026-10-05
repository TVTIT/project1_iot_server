"""Regression checks for the isolated Auth test harness itself."""

import base64
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import stage2_auth


class AuthHarnessTests(unittest.TestCase):
    def test_provision_response_strict_schema_metadata_and_utc(self):
        expected = {"gateway_id": "fixture", "name": " Name ", "description": None,
                    "owner_user_id": "11111111-1111-4111-8111-111111111111",
                    "entity_id": "urn:ngsi-ld:Gateway:fixture"}
        valid = {**expected, "created_at": "2026-01-01T00:00:00.123Z"}
        stage2_auth.assert_provision_response(201, valid, {}, 201, expected)
        stage2_auth.assert_provision_response(200, {**expected, "created_at": None}, {}, 200, expected)
        for changed in [{**valid, "extra": True}, {**valid, "name": "Name"},
                        {**valid, "description": ""}, {**valid, "created_at": 1},
                        {**valid, "created_at": "2026-01-01T01:00:00+01:00"},
                        {**valid, "created_at": "invalidZ"},
                        {**valid, "created_at": "2026-01-01Z"}]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_provision_response(201, changed, {}, 201, expected)
        with self.assertRaises(RuntimeError):
            stage2_auth.assert_provision_response(200, valid, {}, 201, expected)

    def test_sensor_provision_response_requires_scoped_urn_and_nullable_unit(self):
        expected = {"gateway_id": "fixture", "sensor_id": "shared", "name": " Sensor ",
                    "unit": None, "entity_id": "urn:ngsi-ld:Sensor:fixture:shared"}
        valid = {**expected, "created_at": "2026-01-01T00:00:00Z"}
        stage2_auth.assert_provision_response(201, valid, {}, 201, expected)
        for changed in [{**valid, "unit": ""}, {**valid, "entity_id": "urn:ngsi-ld:Sensor:shared"},
                        {**valid, "gateway_id": "other"}, {**valid, "internal_id": "private"}]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_provision_response(201, changed, {}, 201, expected)

    def test_provision_graph_probe_rejects_partial_or_duplicate_graph(self):
        stack = stage2_auth.Stack()
        stack.postgres = "owned-postgres"
        stack.command = mock.Mock(return_value="1|1|1|1|1|2|2|1")
        stack.assert_provisioned_graph("fixture", "owner", {}, {})
        call = stack.command.call_args.kwargs
        self.assertNotIn("\"owner\"", call["stdin"])
        self.assertIn("v->>'owner'", call["stdin"])
        self.assertIn("last_desired_by IS NULL", call["stdin"])
        for result in ("1|0|1|1|1|2|2|1", "1|1|1|1|1|2|1|1", "1|1|1|1|1|3|2|2"):
            stack.command.return_value = result
            with self.assertRaises(RuntimeError):
                stack.assert_provisioned_graph("fixture", "owner", {}, {})

    def test_provision_cleanup_orders_restrictive_fks_and_propagates_errors(self):
        stack = stage2_auth.Stack()
        stack.postgres = "owned-postgres"
        stack.provisioned_gateways = ["scenario_owned"]
        stack.command = mock.Mock(return_value="")
        stack.cleanup_provisioned_resources()
        call = stack.command.call_args.kwargs
        sql = call["stdin"]
        self.assertNotIn("scenario_owned", sql)
        self.assertEqual(json.loads(call["env"]["AUTHORIZATION_FIXTURE"]),
                         {"gateways": ["scenario_owned"]})
        tables = ["twin_relationships", "twin_states", "twin_entities", "sensors", "user_gateways", "gateways"]
        positions = [sql.index("DELETE FROM " + table + " ") for table in tables]
        self.assertEqual(positions, sorted(positions))
        stack.command.side_effect = RuntimeError("cleanup failed")
        with self.assertRaises(RuntimeError):
            stack.cleanup_provisioned_resources()

    def test_provisioning_cleanup_ownership_and_exit_codes(self):
        for test_exit, cleanup_exit, run_exit, expected in [
            (0, 0, 0, 0), (7, 0, 0, 7), (0, 1, 0, 1),
            (7, 1, 0, 7), (0, 0, 9, 9),
        ]:
            with (
                self.subTest(test_exit=test_exit, cleanup_exit=cleanup_exit, run_exit=run_exit),
                tempfile.TemporaryDirectory(prefix="provisioning-cleanup-") as directory,
            ):
                path = Path(directory)
                docker = path / "docker"
                docker.write_text("""#!/bin/sh
case "$1" in
  run) exit "$RUN_EXIT" ;;
  logs) echo 'PostgreSQL init process complete; ready for start up' ;;
  port) echo '127.0.0.1:12345' ;;
  rm) printf '%s\\n' "$*" > "$CLEANUP_RECORD"; exit "$CLEANUP_EXIT" ;;
esac
""")
                go = path / "go"
                go.write_text('#!/bin/sh\nexit "$TEST_EXIT"\n')
                docker.chmod(0o700)
                go.chmod(0o700)
                result = subprocess.run(
                    ["sh", str(stage2_auth.ROOT / "scripts/test-stage2-provisioning.sh")],
                    env={**os.environ, "PATH": f"{path}:{os.environ['PATH']}",
                         "CLEANUP_RECORD": str(path / "record"), "CLEANUP_EXIT": str(cleanup_exit),
                         "TEST_EXIT": str(test_exit), "RUN_EXIT": str(run_exit)},
                    capture_output=True, text=True, timeout=10, check=False,
                )
                self.assertEqual(result.returncode, expected)
                if run_exit:
                    self.assertFalse((path / "record").exists(), "must not remove an unowned container")
                else:
                    self.assertRegex((path / "record").read_text(), r"^rm -f -v stage2-provisioning-[0-9]+\n$")
                self.assertEqual("Failed to remove" in result.stderr, cleanup_exit != 0)

    def test_authorization_cleanup_removes_owned_volumes_and_propagates_failure(self):
        for test_exit, cleanup_exit, expected_exit in [
            (0, 0, 0),
            (0, 1, 1),
            (7, 0, 7),
            (7, 1, 7),
        ]:
            with self.subTest(test_exit=test_exit, cleanup_exit=cleanup_exit):
                with tempfile.TemporaryDirectory(prefix="auth-cleanup-") as directory:
                    path = Path(directory)
                    docker = path / "docker"
                    docker.write_text("""#!/bin/sh
case "$1" in
  logs) echo 'PostgreSQL init process complete; ready for start up' ;;
  port) echo '127.0.0.1:12345' ;;
  rm) printf '%s\\n' "$*" > "$CLEANUP_RECORD"; exit "$CLEANUP_EXIT" ;;
esac
""")
                    go = path / "go"
                    go.write_text('#!/bin/sh\nexit "$TEST_EXIT"\n')
                    docker.chmod(0o700)
                    go.chmod(0o700)
                    result = subprocess.run(
                        [
                            "sh",
                            str(
                                stage2_auth.ROOT
                                / "scripts/test-stage2-authorization.sh"
                            ),
                        ],
                        env={
                            **os.environ,
                            "PATH": f"{path}:{os.environ['PATH']}",
                            "CLEANUP_RECORD": str(path / "record"),
                            "CLEANUP_EXIT": str(cleanup_exit),
                            "TEST_EXIT": str(test_exit),
                        },
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, expected_exit)
                    self.assertRegex(
                        (path / "record").read_text(),
                        r"^rm -f -v stage2-authorization-[0-9a-f]{16}\n$",
                    )
                    self.assertEqual(
                        "Failed to clean up" in result.stderr, cleanup_exit != 0
                    )

    def test_sensor_metadata_rejects_wrong_values(self):
        expected = [
            {
                "sensor_id": "a",
                "name": "first",
                "unit": None,
                "created_at": "2026-01-01T00:00:00Z",
            }
        ]
        for field, wrong in [
            ("name", "other Gateway"),
            ("unit", "wrong"),
            ("created_at", 123),
            ("created_at", "not-a-timestamp"),
            ("created_at", "2026-01-01T01:00:00+01:00"),
            ("created_at", None),
        ]:
            with self.subTest(field=field, wrong=wrong):
                body = {
                    "gateway_id": "gateway_a",
                    "items": [dict(expected[0], **{field: wrong})],
                }
                with self.assertRaises(RuntimeError):
                    stage2_auth.assert_sensor_list(
                        200, body, "gateway_a", ["a"], expected
                    )
        stage2_auth.assert_sensor_list(
            200,
            {"gateway_id": "gateway_a", "items": expected},
            "gateway_a",
            ["a"],
            expected,
        )

    def test_gateway_list_assertion_requires_exact_authorized_items(self):
        stage2_auth.assert_gateway_list(
            200,
            {
                "items": [
                    {
                        "gateway_id": "gateway_a",
                        "name": "Gateway A",
                        "description": None,
                        "role": "owner",
                        "created_at": None,
                    }
                ]
            },
            ["gateway_a"],
            ["owner"],
        )
        stage2_auth.assert_gateway_list(200, {"items": []}, [], [])
        for status, body, ids, roles in [
            (401, {"items": []}, [], []),
            (200, {"items": None}, [], []),
            (
                200,
                {
                    "items": [
                        {
                            "gateway_id": "gateway_b",
                            "name": "B",
                            "description": None,
                            "role": "owner",
                            "created_at": None,
                        }
                    ]
                },
                ["gateway_a"],
                ["owner"],
            ),
            (
                200,
                {
                    "items": [
                        {"gateway_id": "gateway_a", "role": "owner", "extra": True}
                    ]
                },
                ["gateway_a"],
                ["owner"],
            ),
        ]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_gateway_list(status, body, ids, roles)

    def test_sensor_list_assertion_requires_parent_order_and_nullable_fields(self):
        stage2_auth.assert_sensor_list(
            200,
            {
                "gateway_id": "gateway_a",
                "items": [
                    {
                        "sensor_id": "a",
                        "name": "first",
                        "unit": None,
                        "created_at": None,
                    },
                    {
                        "sensor_id": "z",
                        "name": "last",
                        "unit": "unit",
                        "created_at": "2026-01-01T00:00:00Z",
                    },
                ],
            },
            "gateway_a",
            ["a", "z"],
        )
        stage2_auth.assert_sensor_list(
            200, {"gateway_id": "gateway_a", "items": []}, "gateway_a", []
        )
        for status, body, parent, sensors in [
            (404, {"gateway_id": "gateway_a", "items": []}, "gateway_a", []),
            (200, {"gateway_id": "gateway_b", "items": []}, "gateway_a", []),
            (200, {"gateway_id": "gateway_a", "items": None}, "gateway_a", []),
            (
                200,
                {
                    "gateway_id": "gateway_a",
                    "items": [
                        {
                            "sensor_id": "z",
                            "name": "last",
                            "unit": None,
                            "created_at": None,
                        }
                    ],
                },
                "gateway_a",
                ["a"],
            ),
        ]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_sensor_list(status, body, parent, sensors)

    def test_not_found_assertion_requires_safe_non_enumerating_contract(self):
        body = {
            "error": {
                "code": "not_found",
                "message": "resource not found",
                "request_id": "request-1",
            }
        }
        headers = {"X-Request-ID": "request-1"}
        stage2_auth.assert_resource_not_found(404, body, headers)
        for status, changed in [
            (403, body),
            (
                404,
                {
                    "error": {
                        "code": "not_found",
                        "message": "gateway exists but is forbidden",
                        "request_id": "request-1",
                    }
                },
            ),
        ]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_resource_not_found(status, changed, headers)

    def test_centralized_signup_config_is_closed_by_default(self):
        compose = (stage2_auth.ROOT / "docker-compose.yml").read_text()
        example = (stage2_auth.ROOT / ".env.example").read_text()
        self.assertIn("GOTRUE_DISABLE_SIGNUP: ${GOTRUE_DISABLE_SIGNUP:-true}", compose)
        self.assertIn("\nGOTRUE_DISABLE_SIGNUP=true\n", example)

    def test_signup_denial_requires_gotrue_error_not_proxy_failure(self):
        stage2_auth.assert_signup_denied(422, {"error_code": "signup_disabled"})
        for status, body in [
            (200, {}),
            (403, {}),
            (502, None),
            (422, {"error_code": "invalid_email"}),
        ]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_signup_denied(status, body)

    def test_admin_creation_uses_user_response_not_signup_session(self):
        user_id = "11111111-1111-4111-8111-111111111111"
        self.assertEqual(
            stage2_auth.admin_created_user_id(200, {"id": user_id}), user_id
        )
        for status, body in [
            (403, {"id": user_id}),
            (200, {"user": {"id": user_id}}),
            (200, {"id": "invalid"}),
            (200, {"id": str(stage2_auth.uuid.UUID(int=0))}),
        ]:
            with self.assertRaises(RuntimeError):
                stage2_auth.admin_created_user_id(status, body)

    def test_synthetic_token_preserves_claims_and_algorithm(self):
        claims = {"iss": "http://localhost/auth/v1", "role": "authenticated"}
        token = stage2_auth.make_token("test-only-secret", claims)
        header, payload, _ = token.split(".")

        def decode(part):
            return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))

        self.assertEqual(decode(header)["alg"], "HS256")
        self.assertEqual(decode(payload), claims)

    def test_error_response_requires_matching_request_id(self):
        body = {"error": {"code": "unauthorized", "request_id": "request-1"}}
        stage2_auth.assert_response(
            401, body, {"X-Request-ID": "request-1", "WWW-Authenticate": "Bearer"}, 401
        )
        stage2_auth.assert_response(
            401, body, {"X-Request-Id": "request-1", "Www-Authenticate": "Bearer"}, 401
        )
        with self.assertRaises(RuntimeError):
            stage2_auth.assert_response(401, body, {"X-Request-ID": "wrong"}, 401)

    def test_startup_check_rejects_success_timeout_and_secret_leak(self):
        stage2_auth.assert_startup_failure(
            1,
            "SUPABASE_JWT_ISSUER is required",
            "SUPABASE_JWT_ISSUER",
            ["private-secret"],
        )
        for code, output in [
            (0, "SUPABASE_JWT_ISSUER"),
            (124, "SUPABASE_JWT_ISSUER"),
            (1, "SUPABASE_JWT_ISSUER private-secret"),
            (1, "unrelated database failure"),
        ]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_startup_failure(
                    code, output, "SUPABASE_JWT_ISSUER", ["private-secret"]
                )

    def test_error_assertion_cannot_accept_wrong_status_or_missing_envelope(self):
        for status, body in [(200, {}), (401, None), (401, {"error": "unsafe"})]:
            with self.assertRaises(RuntimeError):
                stage2_auth.assert_response(status, body, {}, 401)
        with self.assertRaises(RuntimeError):
            stage2_auth.assert_response(
                401,
                {"error": {"request_id": "request-1"}},
                {"X-Request-ID": "request-1"},
                401,
            )

    def test_container_secrets_use_environment_not_command_arguments(self):
        stack = stage2_auth.Stack()
        stack.command = mock.Mock(return_value="container-id")
        stack.create("fixture", "test-image", {"TEST_SECRET": "private-value"})
        args = stack.command.call_args.args[0]
        self.assertIn("TEST_SECRET", args)
        self.assertNotIn("private-value", args)
        self.assertEqual(
            stack.command.call_args.kwargs["env"], {"TEST_SECRET": "private-value"}
        )

    def test_cleanup_continues_after_one_resource_fails(self):
        stack = stage2_auth.Stack()
        stack.containers = ["owned-first", "owned-second"]
        stack.network_created = stack.owned_image = True
        stack.command = mock.Mock(
            side_effect=[RuntimeError("remove failed"), "", "", ""]
        )
        with self.assertRaises(RuntimeError):
            stack.cleanup()
        self.assertEqual(stack.command.call_count, 4)
        self.assertEqual(stack.command.call_args_list[1].args[0][-1], "owned-first")

    def test_wait_stops_immediately_when_container_exits(self):
        stack = stage2_auth.Stack()
        stack.command = mock.Mock(return_value="false")
        with self.assertRaises(RuntimeError):
            stack.wait(lambda: True, "fixture readiness", "owned-container")

    def test_account_probe_uses_sql_parameter_and_requires_profile_without_grants(self):
        stack = stage2_auth.Stack()
        stack.postgres = "owned-postgres"
        stack.command = mock.Mock(return_value="1|1|0|0")
        email = "fixture@example.invalid"
        stack.check_account_rows(email, present=True)
        self.assertNotIn(email, stack.command.call_args.kwargs["stdin"])
        self.assertIn(":'fixture_email'", stack.command.call_args.kwargs["stdin"])
        self.assertEqual(
            stack.command.call_args.kwargs["env"], {"AUTH_TEST_EMAIL": email}
        )
        for unexpected in ("1|0|0|0", "1|1|1|0", "1|1|0|1", "0|0|0|0"):
            stack.command.return_value = unexpected
            with self.assertRaises(RuntimeError):
                stack.check_account_rows(email, present=True)
        stack.command.return_value = "0|0|0|0"
        stack.check_account_rows(email, present=False)
        stack.command.return_value = "1|1|0|0"
        with self.assertRaises(RuntimeError):
            stack.check_account_rows(email, present=False)

    def test_authorization_fixture_uses_environment_and_parameterized_json(self):
        stack = stage2_auth.Stack()
        stack.postgres = "owned-postgres"
        stack.command = mock.Mock(return_value="")
        users = [
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        ]
        gateways = stack.seed_authorization_resources(users)
        call = stack.command.call_args
        sql = call.kwargs["stdin"]
        fixture = json.loads(call.kwargs["env"]["AUTHORIZATION_FIXTURE"])
        self.assertNotIn(users[0], sql)
        self.assertNotIn(users[1], sql)
        self.assertIn(":'fixture_json'::jsonb", sql)
        self.assertEqual(fixture["users"], users)
        self.assertEqual(
            list(gateways),
            [fixture["gateway_a"], fixture["gateway_b"], fixture["gateway_empty"]],
        )

    def test_membership_mutation_uses_parameterized_fixture(self):
        stack = stage2_auth.Stack()
        stack.postgres = "owned-postgres"
        stack.command = mock.Mock(return_value="")
        user = "11111111-1111-4111-8111-111111111111"
        stack.update_gateway_membership(user, "gateway_a", "viewer")
        insert = stack.command.call_args.kwargs
        self.assertNotIn(user, insert["stdin"])
        self.assertIn("FROM fixture", insert["stdin"])
        self.assertEqual(
            json.loads(insert["env"]["AUTHORIZATION_FIXTURE"])["role"], "viewer"
        )
        stack.update_gateway_membership(user, "gateway_a", None)
        delete = stack.command.call_args.kwargs
        self.assertIn("DELETE FROM user_gateways AS ug USING fixture", delete["stdin"])
        self.assertIsNone(json.loads(delete["env"]["AUTHORIZATION_FIXTURE"])["role"])


if __name__ == "__main__":
    unittest.main()
