"""Regression checks for the isolated Auth test harness itself."""

import base64
import json
import unittest
from unittest import mock

import stage2_auth


class AuthHarnessTests(unittest.TestCase):
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
        self.assertEqual(stage2_auth.admin_created_user_id(200, {"id": user_id}), user_id)
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
            1, "SUPABASE_JWT_ISSUER is required", "SUPABASE_JWT_ISSUER", ["private-secret"]
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
                401, {"error": {"request_id": "request-1"}}, {"X-Request-ID": "request-1"}, 401
            )

    def test_container_secrets_use_environment_not_command_arguments(self):
        stack = stage2_auth.Stack()
        stack.command = mock.Mock(return_value="container-id")
        stack.create("fixture", "test-image", {"TEST_SECRET": "private-value"})
        args = stack.command.call_args.args[0]
        self.assertIn("TEST_SECRET", args)
        self.assertNotIn("private-value", args)
        self.assertEqual(stack.command.call_args.kwargs["env"], {"TEST_SECRET": "private-value"})

    def test_cleanup_continues_after_one_resource_fails(self):
        stack = stage2_auth.Stack()
        stack.containers = ["owned-first", "owned-second"]
        stack.network_created = stack.owned_image = True
        stack.command = mock.Mock(side_effect=[RuntimeError("remove failed"), "", "", ""])
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
        self.assertEqual(stack.command.call_args.kwargs["env"], {"AUTH_TEST_EMAIL": email})
        for unexpected in ("1|0|0|0", "1|1|1|0", "1|1|0|1", "0|0|0|0"):
            stack.command.return_value = unexpected
            with self.assertRaises(RuntimeError):
                stack.check_account_rows(email, present=True)
        stack.command.return_value = "0|0|0|0"
        stack.check_account_rows(email, present=False)
        stack.command.return_value = "1|1|0|0"
        with self.assertRaises(RuntimeError):
            stack.check_account_rows(email, present=False)


if __name__ == "__main__":
    unittest.main()
