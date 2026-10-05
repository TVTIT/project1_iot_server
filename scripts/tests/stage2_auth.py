#!/usr/bin/env python3
"""Isolated Docker smoke / GoTrue integration; never reads deployment .env."""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import importlib.util
import json
import os
import re
import secrets
import signal
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
POSTGRES_IMAGE = "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
ISSUER = "http://auth.test.local/auth/v1"


def make_token(secret: str, claims: dict, algorithm: str = "HS256") -> str:
    """Test tokens only; production verification remains in Go's JWT library."""

    def encode(value: dict) -> str:
        return (
            base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()
        )

    message = encode({"alg": algorithm, "typ": "JWT"}) + "." + encode(claims)
    signature = hmac.new(secret.encode(), message.encode(), hashlib.sha256).digest()
    return message + "." + base64.urlsafe_b64encode(signature).rstrip(b"=").decode()


def assert_response(status: int, body: object, headers: dict, expected: int) -> None:
    headers = {key.lower(): value for key, value in headers.items()}
    if status != expected:
        raise RuntimeError(f"HTTP status {status}; expected {expected}")
    if expected >= 400:
        if not isinstance(body, dict) or not isinstance(body.get("error"), dict):
            raise RuntimeError("response is missing safe error envelope")
        request_id = headers.get("x-request-id")
        if not request_id or body["error"].get("request_id") != request_id:
            raise RuntimeError("error body and request-ID header do not match")
    if expected == 401 and headers.get("www-authenticate") != "Bearer":
        raise RuntimeError("401 response is missing Bearer challenge")


def assert_startup_failure(
    code: int, output: str, field: str, sensitive: list[str]
) -> None:
    if code != 1 or field not in output:
        raise RuntimeError(
            f"startup did not fail at configuration validation for {field}"
        )
    if any(value and value in output for value in sensitive):
        raise RuntimeError("startup log exposed test credentials")


def assert_signup_denied(status: int, body: object) -> None:
    # GoTrue v2.196.0: distinguish policy rejection from bad input/proxy failure.
    if (
        status != 422
        or not isinstance(body, dict)
        or body.get("error_code") != "signup_disabled"
    ):
        raise RuntimeError(
            f"public signup was not rejected by GoTrue signup policy (HTTP {status})"
        )


def admin_created_user_id(status: int, body: object) -> str:
    if status != 200 or not isinstance(body, dict):
        raise RuntimeError(
            f"Admin API user creation failed (HTTP {status}); response suppressed"
        )
    try:
        user_id = uuid.UUID(body["id"])
    except (KeyError, ValueError, TypeError, AttributeError) as error:
        raise RuntimeError("Admin API response is missing a valid user UUID") from error
    if user_id.int == 0:
        raise RuntimeError("Admin API returned a zero user UUID")
    return str(user_id)


def assert_gateway_list(
    status: int, body: object, expected_ids: list[str], expected_roles: list[str]
) -> None:
    if status != 200 or not isinstance(body, dict) or set(body) != {"items"}:
        raise RuntimeError(
            f"Gateway list contract failed (HTTP {status}); response suppressed"
        )
    items = body["items"]
    if not isinstance(items, list) or len(items) != len(expected_ids):
        raise RuntimeError("Gateway list did not match authorized resources")
    ids, roles = [], []
    required = {"gateway_id", "name", "description", "role", "created_at"}
    for item in items:
        if not isinstance(item, dict) or set(item) != required:
            raise RuntimeError("Gateway list item has an unexpected response shape")
        ids.append(item["gateway_id"])
        roles.append(item["role"])
    if ids != expected_ids or roles != expected_roles:
        raise RuntimeError("Gateway list did not match authorized resources")


def assert_sensor_list(
    status: int,
    body: object,
    expected_gateway: str,
    expected_ids: list[str],
    expected_items: list[dict] | None = None,
) -> None:
    if (
        status != 200
        or not isinstance(body, dict)
        or set(body) != {"gateway_id", "items"}
    ):
        raise RuntimeError(
            f"Sensor list contract failed (HTTP {status}); response suppressed"
        )
    if body["gateway_id"] != expected_gateway or not isinstance(body["items"], list):
        raise RuntimeError("Sensor list parent or items are invalid")
    ids = []
    required = {"sensor_id", "name", "unit", "created_at"}
    for item in body["items"]:
        if not isinstance(item, dict) or set(item) != required:
            raise RuntimeError("Sensor list item has an unexpected response shape")
        ids.append(item["sensor_id"])
    if ids != expected_ids:
        raise RuntimeError("Sensor list did not match authorized resources")
    if expected_items is not None and body["items"] != expected_items:
        raise RuntimeError(
            "Sensor metadata did not match fixture values, NULL or UTC timestamps"
        )


def assert_resource_not_found(status: int, body: object, headers: dict) -> None:
    assert_response(status, body, headers, 404)
    error = body["error"]
    if error.get("code") != "not_found" or error.get("message") != "resource not found":
        raise RuntimeError("resource denial exposed distinguishable information")


def assert_provision_response(status, body, headers, expected_status, expected):
    assert_response(status, body, headers, expected_status)
    if not isinstance(body, dict) or set(body) != set(expected) | {"created_at"}:
        raise RuntimeError("provisioning response shape mismatch; response suppressed")
    if any(body[key] != value for key, value in expected.items()):
        raise RuntimeError("provisioning metadata/identity mismatch; response suppressed")
    timestamp = body["created_at"]
    if timestamp is not None:
        try:
            if not isinstance(timestamp, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z", timestamp
            ):
                raise ValueError()
            datetime.fromisoformat(timestamp[:-1] + "+00:00")
        except ValueError as error:
            raise RuntimeError("provisioning timestamp is not UTC RFC3339 or NULL") from error


def http_request(
    base: str,
    path: str,
    token: str | None = None,
    key: str | None = None,
    body: dict | None = None,
    method: str = "GET",
    upgrade: bool = False,
) -> tuple[int, object, dict]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    if key:
        headers["apikey"] = key
    if upgrade:
        headers.update(
            {
                "Connection": "Upgrade",
                "Upgrade": "websocket",
                "Sec-WebSocket-Version": "13",
                "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ==",
            }
        )
    request = urllib.request.Request(
        base + path,
        headers=headers,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
    )
    try:
        response = urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = response.read()
        try:
            payload = json.loads(data)
        except (ValueError, UnicodeError):
            payload = None
        return response.code, payload, dict(response.headers)


class Stack:
    def __init__(self) -> None:
        self.prefix = "stage2-auth-" + secrets.token_hex(6)
        self.network = self.prefix + "-net"
        self.containers: list[str] = []
        self.network_created = False
        self.owned_image = False
        self.image = os.environ.get("STAGE2_BACKEND_IMAGE") or self.prefix + ":test"
        self.secret = secrets.token_urlsafe(32)
        self.credentials = {
            "POSTGRES_USER": "stage2_admin",
            "POSTGRES_DB": "stage2_auth_test",
            **{
                key: secrets.token_urlsafe(24)
                for key in (
                    "POSTGRES_PASSWORD",
                    "BACKEND_DB_PASSWORD",
                    "AUTH_DB_PASSWORD",
                    "STORAGE_DB_PASSWORD",
                )
            },
        }
        self.sensitive = [
            self.secret,
            *[value for key, value in self.credentials.items() if "PASSWORD" in key],
        ]
        self.provisioned_gateways: list[str] = []

    def fixture_sql(self, fixture: dict, sql: str) -> str:
        return self.command(
            ["docker", "exec", "-i", "-e", "AUTHORIZATION_FIXTURE", self.postgres,
             "psql", "-U", "stage2_admin", "-d", "stage2_auth_test", "-At",
             "-v", "ON_ERROR_STOP=1"],
            env={"AUTHORIZATION_FIXTURE": json.dumps(fixture)},
            stdin=r"\getenv fixture_json AUTHORIZATION_FIXTURE" + "\n" + sql,
        )

    def cleanup_provisioned_resources(self) -> None:
        # Restrictive Twin FKs require explicit graph removal before Gateways/users.
        self.fixture_sql({"gateways": self.provisioned_gateways}, """
BEGIN;
CREATE TEMP TABLE owned_gateways AS
SELECT jsonb_array_elements_text(:'fixture_json'::jsonb->'gateways') AS gateway_id;
DELETE FROM twin_relationships WHERE source_entity_id IN
 (SELECT id FROM twin_entities JOIN owned_gateways USING (gateway_id))
 OR target_entity_id IN (SELECT id FROM twin_entities JOIN owned_gateways USING (gateway_id));
DELETE FROM twin_states WHERE entity_id IN
 (SELECT id FROM twin_entities JOIN owned_gateways USING (gateway_id));
DELETE FROM twin_entities USING owned_gateways WHERE twin_entities.gateway_id = owned_gateways.gateway_id;
DELETE FROM sensors USING owned_gateways WHERE sensors.gateway_id = owned_gateways.gateway_id;
DELETE FROM user_gateways USING owned_gateways WHERE user_gateways.gateway_id = owned_gateways.gateway_id;
DELETE FROM gateways USING owned_gateways WHERE gateways.gateway_id = owned_gateways.gateway_id;
COMMIT;
""")

    def check_real_provisioning_api(self, base, users, tokens) -> None:
        admin, owner, nonmember = tokens
        gateway_ids = sorted("provision_" + secrets.token_hex(6) for _ in range(2))
        self.provisioned_gateways.extend(gateway_ids)  # Track before first PUT, including failures.
        expected_gateways = []
        for index, gateway_id in enumerate(gateway_ids):
            path = f"/v1/admin/gateways/{gateway_id}"
            payload = {"name": " Gateway ", "description": None if index == 0 else "",
                       "owner_user_id": users[1]}
            expected = {"gateway_id": gateway_id, **payload,
                        "entity_id": f"urn:ngsi-ld:Gateway:{gateway_id}"}
            response = http_request(base, path, admin, body=payload, method="PUT")
            assert_provision_response(*response, 201, expected)
            if response[1]["created_at"] is None:
                raise RuntimeError("new Gateway timestamp unexpectedly NULL")
            expected_gateways.append({key: response[1][key] for key in
                                      ("gateway_id", "name", "description", "created_at")})
            retry = http_request(base, path, admin, body=payload, method="PUT")
            assert_provision_response(*retry, 200, expected)
            if retry[1] != response[1]:
                raise RuntimeError("Gateway retry changed persisted representation")
            assert_response(*http_request(base, path, admin,
                body={**payload, "description": "conflict"}, method="PUT"), 409)
            for token in (owner, nonmember):
                assert_response(*http_request(base, path, token, body=payload, method="PUT"), 403)
            sensor_path = path + "/sensors/shared"
            sensor_payload = {"name": " Sensor ", "unit": None if index == 0 else ""}
            sensor_expected = {"gateway_id": gateway_id, "sensor_id": "shared", **sensor_payload,
                               "entity_id": f"urn:ngsi-ld:Sensor:{gateway_id}:shared"}
            sensor = http_request(base, sensor_path, admin, body=sensor_payload, method="PUT")
            assert_provision_response(*sensor, 201, sensor_expected)
            if sensor[1]["created_at"] is None:
                raise RuntimeError("new Sensor timestamp unexpectedly NULL")
            sensor_retry = http_request(base, sensor_path, admin, body=sensor_payload, method="PUT")
            assert_provision_response(*sensor_retry, 200, sensor_expected)
            if sensor_retry[1] != sensor[1]:
                raise RuntimeError("Sensor retry changed persisted representation")
            assert_response(*http_request(base, sensor_path, admin,
                body={**sensor_payload, "unit": "conflict"}, method="PUT"), 409)
            for token in (owner, nonmember):
                assert_response(*http_request(base, sensor_path, token,
                    body=sensor_payload, method="PUT"), 403)
            read_path = f"/v1/gateways/{gateway_id}/sensors"
            assert_sensor_list(*http_request(base, read_path, owner)[:2], gateway_id, ["shared"],
                [{key: sensor[1][key] for key in ("sensor_id", "name", "unit", "created_at")}])
            for token in (admin, nonmember):
                assert_resource_not_found(*http_request(base, read_path, token))
            self.assert_provisioned_graph(gateway_id, users[1], payload, sensor_payload)
        owner_list = http_request(base, "/v1/gateways", owner)
        assert_gateway_list(*owner_list[:2], gateway_ids, ["owner", "owner"])
        if owner_list[1]["items"] != [{**item, "role": "owner"} for item in expected_gateways]:
            raise RuntimeError("owner Gateway read metadata differs from provisioned metadata")
        for token in (admin, nonmember):
            assert_gateway_list(*http_request(base, "/v1/gateways", token)[:2], [], [])
        print("PASS: real human JWT -> Nginx admin PUT Gateway/Sensor -> Twin graph; retries/conflicts and membership isolation")

    def assert_provisioned_graph(self, gateway_id, owner, gateway, sensor):
        result = self.fixture_sql({"gateway_id": gateway_id, "owner": owner,
                                   "gateway": gateway, "sensor": sensor}, """
WITH f AS (SELECT :'fixture_json'::jsonb AS v),
e AS (SELECT t.* FROM twin_entities t, f WHERE t.gateway_id = v->>'gateway_id'),
s AS (SELECT ts.* FROM twin_states ts JOIN e ON e.id = ts.entity_id)
SELECT
 (SELECT count(*) FROM gateways g, f WHERE g.gateway_id = v->>'gateway_id'
  AND g.name = v->'gateway'->>'name' AND g.description IS NOT DISTINCT FROM v->'gateway'->>'description'),
 (SELECT count(*) FROM user_gateways ug, f WHERE ug.gateway_id = v->>'gateway_id'
  AND ug.user_id = (v->>'owner')::uuid AND ug.role = 'owner'),
 (SELECT count(*) FROM sensors sn, f WHERE sn.gateway_id = v->>'gateway_id'
  AND sn.sensor_id = 'shared' AND sn.name = v->'sensor'->>'name'
  AND sn.unit IS NOT DISTINCT FROM v->'sensor'->>'unit'),
 (SELECT count(*) FROM e, f WHERE entity_type = 'Gateway'
  AND entity_id = 'urn:ngsi-ld:Gateway:' || (v->>'gateway_id')
  AND name = v->'gateway'->>'name' AND attributes = '{}'::jsonb),
 (SELECT count(*) FROM e, f WHERE entity_type = 'Sensor'
  AND entity_id = 'urn:ngsi-ld:Sensor:' || (v->>'gateway_id') || ':shared'
  AND name = v->'sensor'->>'name' AND attributes = '{}'::jsonb),
 (SELECT count(*) FROM e),
 (SELECT count(*) FROM s WHERE reported_state = '{}'::jsonb AND desired_state = '{}'::jsonb
  AND reported_version = 0 AND desired_version = 0 AND last_reported_at IS NULL
  AND last_desired_at IS NULL AND last_desired_by IS NULL),
 (SELECT count(*) FROM twin_relationships r JOIN e src ON src.id = r.source_entity_id
  JOIN e dst ON dst.id = r.target_entity_id WHERE relationship_type = 'hasSensor'
  AND src.entity_type = 'Gateway' AND dst.entity_type = 'Sensor');
""")
        if result != "1|1|1|1|1|2|2|1":
            raise RuntimeError("provisioned database/Twin/state/hasSensor invariant failed")

    def command(
        self,
        args: list[str],
        *,
        env: dict | None = None,
        stdin: str | None = None,
        timeout: int = 120,
    ) -> str:
        result = subprocess.run(
            args,
            input=stdin,
            text=True,
            capture_output=True,
            env=None if env is None else {**os.environ, **env},
            timeout=timeout,
            cwd=ROOT,
        )
        if result.returncode:
            # Tool output may contain config credentials. Never dump it into CI.
            raise RuntimeError(
                f"{args[0]} {args[1]} failed (exit {result.returncode}); raw output suppressed"
            )
        return result.stdout.strip()

    def create(
        self,
        name: str,
        image: str,
        env: dict | None = None,
        options: list[str] | None = None,
        command: list[str] | None = None,
    ) -> str:
        name = self.prefix + "-" + name
        args = ["docker", "create", "--name", name, "--network", self.network]
        for key in env or {}:
            args.extend(["-e", key])  # Values go through process env, never argv.
        args.extend(options or [])
        args.append(image)
        args.extend(command or [])
        self.command(args, env=env)
        self.containers.append(name)
        return name

    def start(self, name: str) -> None:
        self.command(["docker", "start", name])

    def port(self, name: str, port: int) -> int:
        binding = self.command(["docker", "port", name, f"{port}/tcp"])
        return int(binding.rsplit(":", 1)[1])

    def wait(self, predicate, label: str, container: str) -> None:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if (
                self.command(
                    ["docker", "inspect", "-f", "{{.State.Running}}", container]
                )
                != "true"
            ):
                raise RuntimeError(f"container stopped while waiting for {label}")
            try:
                if predicate():
                    return
            except (RuntimeError, urllib.error.URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(1)
        raise RuntimeError(f"timed out waiting for {label}; raw logs suppressed")

    def db_url(self, host: str, user: str, password_key: str) -> str:
        return f"postgres://{user}:{self.credentials[password_key]}@{host}/{self.credentials['POSTGRES_DB']}?sslmode=disable"

    def prepare(self) -> dict:
        if not os.environ.get("STAGE2_BACKEND_IMAGE"):
            self.command(
                ["docker", "build", "-t", self.image, str(ROOT / "src")], timeout=300
            )
            self.owned_image = True
        self.command(["docker", "network", "create", self.network])
        self.network_created = True
        self.postgres = self.create(
            "postgres",
            POSTGRES_IMAGE,
            self.credentials,
            [
                "--network-alias",
                "postgres",
                "-p",
                "127.0.0.1::5432",
                "-v",
                f"{ROOT / 'migrations'}:/docker-entrypoint-initdb.d:ro",
                "-v",
                f"{ROOT / 'scripts/run-migrations.sh'}:/run-migrations.sh:ro",
                "-v",
                f"{ROOT / 'migrations'}:/migrations:ro",
                "-v",
                f"{ROOT / 'scripts/bootstrap-platform-admin.sh'}:/bootstrap.sh:ro",
            ],
        )
        self.start(self.postgres)
        self.wait(
            lambda: (
                "PostgreSQL init process complete; ready for start up"
                in self.command(["docker", "logs", self.postgres])
                and self.command(
                    [
                        "docker",
                        "exec",
                        self.postgres,
                        "pg_isready",
                        "-U",
                        "stage2_admin",
                        "-d",
                        "stage2_auth_test",
                    ]
                )
            ),
            "stable PostgreSQL initialization",
            self.postgres,
        )
        self.command(
            [
                "docker",
                "exec",
                self.postgres,
                "sh",
                "/migrations/000000_configure_supabase_roles.sh",
            ]
        )
        self.command(
            [
                "docker",
                "exec",
                self.postgres,
                "sh",
                "-c",
                'export PGHOST=127.0.0.1 PGUSER="$POSTGRES_USER" PGDATABASE="$POSTGRES_DB" PGPASSWORD="$POSTGRES_PASSWORD"; exec sh /run-migrations.sh',
            ]
        )
        self.command(
            [
                "docker",
                "exec",
                "-i",
                self.postgres,
                "psql",
                "-U",
                "stage2_admin",
                "-d",
                "stage2_auth_test",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            stdin=(ROOT / "scripts/sql/verify-migration-000010.sql").read_text(),
        )
        host = f"127.0.0.1:{self.port(self.postgres, 5432)}"
        self.host_db_url = self.db_url(host, "iot_backend_app", "BACKEND_DB_PASSWORD")
        return {
            "SERVER_ENV": "production",
            "DATABASE_URL": self.db_url(
                "postgres:5432", "iot_backend_app", "BACKEND_DB_PASSWORD"
            ),
            "SUPABASE_JWT_SECRET": self.secret,
            "SUPABASE_JWT_ISSUER": ISSUER,
            "SUPABASE_JWT_AUDIENCE": "authenticated",
            "SUPABASE_JWT_CLOCK_SKEW": "30s",
        }

    def negative_startup(self, config: dict) -> None:
        cases = [
            ("missing-secret", "SUPABASE_JWT_SECRET", None),
            ("short-secret", "SUPABASE_JWT_SECRET", "short-test-key"),
            ("missing-issuer", "SUPABASE_JWT_ISSUER", None),
            ("bad-issuer", "SUPABASE_JWT_ISSUER", "/relative"),
            ("missing-audience", "SUPABASE_JWT_AUDIENCE", None),
            ("bad-skew", "SUPABASE_JWT_CLOCK_SKEW", "invalid"),
        ]
        for name, key, value in cases:
            env = dict(config)
            if value is None:
                del env[key]
            else:
                env[key] = value
            container = self.create(name, self.image, env)
            subprocess.run(
                ["docker", "start", "-a", container],
                text=True,
                capture_output=True,
                timeout=20,
            )
            code = int(
                self.command(
                    ["docker", "inspect", "-f", "{{.State.ExitCode}}", container]
                )
            )
            result = subprocess.run(
                ["docker", "logs", container],
                text=True,
                capture_output=True,
                timeout=10,
            )
            if result.returncode:
                raise RuntimeError("cannot read negative startup logs")
            assert_startup_failure(
                code,
                result.stdout + result.stderr,
                key,
                self.sensitive + ["short-test-key"],
            )
        print(
            "PASS: six negative startup cases (exit 1, config error, no credential leak)"
        )

    def start_backend(self, config: dict) -> str:
        self.backend = self.create(
            "backend",
            self.image,
            config,
            ["--network-alias", "backend", "-p", "127.0.0.1::8080"],
        )
        self.start(self.backend)
        base = f"http://127.0.0.1:{self.port(self.backend, 8080)}"
        self.wait(
            lambda: http_request(base, "/readyz")[0] == 200,
            "backend readiness",
            self.backend,
        )
        return base

    def check_routes(self, base: str, token: str) -> None:
        for path in ("/healthz", "/v1/health"):
            assert_response(*http_request(base, path), 200)
        for path in ("/v1/telemetry/history", "/v1/digital-twins", "/v1/ws"):
            assert_response(*http_request(base, path), 401)
            assert_response(*http_request(base, path, token), 501)
        assert_response(*http_request(base, "/v1/ws", upgrade=True), 401)
        assert_response(*http_request(base, "/v1/ws", token, upgrade=True), 501)
        print("PASS: public 200, unauthenticated 401, authenticated stubs 501")

    def check_bad_tokens(self, base: str, claims: dict) -> None:
        now = int(time.time())
        for change in (
            {"iss": ISSUER + "/"},
            {"aud": "wrong"},
            {"exp": now - 120},
            {"nbf": now + 120},
            {"role": "anon"},
            {"role": "service_role"},
            {"sub": "not-a-uuid"},
        ):
            token = make_token(self.secret, {**claims, **change})
            self.sensitive.append(token)
            assert_response(*http_request(base, "/v1/ws", token), 401)
        for token in (
            "malformed",
            make_token("wrong-test-secret", claims),
            make_token(self.secret, claims, "HS512"),
        ):
            if token != "malformed":
                self.sensitive.append(token)
            assert_response(*http_request(base, "/v1/ws", token), 401)
        print("PASS: invalid signature/algorithm/claims rejected through HTTP")

    def check_account_rows(self, email: str, present: bool) -> None:
        # psql variables prevent interpolating even fixture identifiers into SQL.
        counts = self.command(
            [
                "docker",
                "exec",
                "-i",
                "-e",
                "AUTH_TEST_EMAIL",
                self.postgres,
                "psql",
                "-U",
                "stage2_admin",
                "-d",
                "stage2_auth_test",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            env={"AUTH_TEST_EMAIL": email},
            stdin=r"""\getenv fixture_email AUTH_TEST_EMAIL
SELECT count(u.id), count(p.id), count(ug.user_id), count(pa.user_id)
FROM auth.users u
LEFT JOIN public.profiles p ON p.id = u.id
LEFT JOIN public.user_gateways ug ON ug.user_id = u.id
LEFT JOIN public.platform_admins pa ON pa.user_id = u.id
WHERE u.email = :'fixture_email';
""",
        )
        if counts != ("1|1|0|0" if present else "0|0|0|0"):
            raise RuntimeError(
                "unexpected account/profile/membership/admin rows in isolated DB"
            )

    def seed_authorization_resources(self, users: list[str]) -> tuple[str, str, str]:
        gateway_a = "auth_a_" + secrets.token_hex(6)
        gateway_b = "auth_b_" + secrets.token_hex(6)
        gateway_empty = "auth_empty_" + secrets.token_hex(6)
        fixture = {
            "users": users,
            "gateway_a": gateway_a,
            "gateway_b": gateway_b,
            "gateway_empty": gateway_empty,
        }
        self.command(
            [
                "docker",
                "exec",
                "-i",
                "-e",
                "AUTHORIZATION_FIXTURE",
                self.postgres,
                "psql",
                "-U",
                "stage2_admin",
                "-d",
                "stage2_auth_test",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            env={"AUTHORIZATION_FIXTURE": json.dumps(fixture)},
            stdin=r"""\getenv fixture_json AUTHORIZATION_FIXTURE
WITH fixture AS (SELECT :'fixture_json'::jsonb AS value)
INSERT INTO gateways (gateway_id, name, description, created_at)
SELECT gateway_id, name, description, created_at
FROM fixture,
LATERAL (VALUES
    (value->>'gateway_a', 'Gateway A', NULL::text, NULL::timestamptz),
    (value->>'gateway_b', 'Gateway B', 'private B', now()),
    (value->>'gateway_empty', 'Gateway Empty', NULL::text, now())
) AS rows(gateway_id, name, description, created_at);

WITH fixture AS (SELECT :'fixture_json'::jsonb AS value)
INSERT INTO user_gateways (user_id, gateway_id, role)
SELECT (value->'users'->>1)::uuid, value->>'gateway_b', 'operator'
FROM fixture;

WITH fixture AS (SELECT :'fixture_json'::jsonb AS value)
INSERT INTO sensors (gateway_id, sensor_id, name, unit, created_at)
SELECT gateway_id, sensor_id, name, unit, created_at
FROM fixture,
LATERAL (VALUES
    (value->>'gateway_a', 'z', 'last', 'unit', '2026-01-01T01:00:00+01:00'::timestamptz),
    (value->>'gateway_a', 'a', 'first', NULL::text, NULL::timestamptz),
    (value->>'gateway_b', 'a', 'other', 'different', '2026-01-01T01:00:00+01:00'::timestamptz)
) AS rows(gateway_id, sensor_id, name, unit, created_at);
""",
        )
        return gateway_a, gateway_b, gateway_empty

    def update_gateway_membership(
        self, user_id: str, gateway_id: str, role: str | None
    ) -> None:
        fixture = {"user_id": user_id, "gateway_id": gateway_id, "role": role}
        statement = (
            "DELETE FROM user_gateways AS ug USING fixture "
            "WHERE ug.user_id = (value->>'user_id')::uuid "
            "AND ug.gateway_id = value->>'gateway_id';"
            if role is None
            else "INSERT INTO user_gateways (user_id, gateway_id, role) "
            "SELECT (value->>'user_id')::uuid, value->>'gateway_id', value->>'role' "
            "FROM fixture "
            "ON CONFLICT (user_id, gateway_id) DO UPDATE SET role = EXCLUDED.role;"
        )
        self.command(
            [
                "docker",
                "exec",
                "-i",
                "-e",
                "AUTHORIZATION_FIXTURE",
                self.postgres,
                "psql",
                "-U",
                "stage2_admin",
                "-d",
                "stage2_auth_test",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            env={"AUTHORIZATION_FIXTURE": json.dumps(fixture)},
            stdin=(
                r"\getenv fixture_json AUTHORIZATION_FIXTURE"
                "\nWITH fixture AS (SELECT :'fixture_json'::jsonb AS value)\n"
                + statement
                + "\n"
            ),
        )

    def check_real_authorization_api(
        self, base: str, users: list[str], admin_token: str, normal_token: str
    ) -> None:
        gateway_a, gateway_b, gateway_empty = self.seed_authorization_resources(users)

        assert_gateway_list(
            *http_request(base, "/v1/gateways", admin_token)[:2], [], []
        )
        assert_sensor_list(
            *http_request(base, f"/v1/gateways/{gateway_b}/sensors", normal_token)[:2],
            gateway_b,
            ["a"],
            [
                {
                    "sensor_id": "a",
                    "name": "other",
                    "unit": "different",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ],
        )
        forbidden = http_request(
            base, f"/v1/gateways/{gateway_a}/sensors", normal_token
        )
        missing = http_request(
            base,
            f"/v1/gateways/auth_missing_{secrets.token_hex(6)}/sensors",
            normal_token,
        )
        assert_resource_not_found(*forbidden)
        assert_resource_not_found(*missing)
        # Request IDs intentionally differ; the public status/code/message must not.
        for field in ("code", "message"):
            if forbidden[1]["error"][field] != missing[1]["error"][field]:
                raise RuntimeError("missing and forbidden resource errors differ")

        self.update_gateway_membership(users[0], gateway_a, "owner")
        self.update_gateway_membership(users[0], gateway_empty, "viewer")
        assert_gateway_list(
            *http_request(base, "/v1/gateways", admin_token)[:2],
            [gateway_a, gateway_empty],
            ["owner", "viewer"],
        )
        assert_sensor_list(
            *http_request(base, f"/v1/gateways/{gateway_a}/sensors", admin_token)[:2],
            gateway_a,
            ["a", "z"],
            [
                {"sensor_id": "a", "name": "first", "unit": None, "created_at": None},
                {
                    "sensor_id": "z",
                    "name": "last",
                    "unit": "unit",
                    "created_at": "2026-01-01T00:00:00Z",
                },
            ],
        )
        assert_sensor_list(
            *http_request(base, f"/v1/gateways/{gateway_empty}/sensors", admin_token)[
                :2
            ],
            gateway_empty,
            [],
            [],
        )
        assert_resource_not_found(
            *http_request(base, f"/v1/gateways/{gateway_b}/sensors", admin_token)
        )

        self.update_gateway_membership(users[0], gateway_a, None)
        assert_gateway_list(
            *http_request(base, "/v1/gateways", admin_token)[:2],
            [gateway_empty],
            ["viewer"],
        )
        assert_resource_not_found(
            *http_request(base, f"/v1/gateways/{gateway_a}/sensors", admin_token)
        )
        assert_gateway_list(
            *http_request(base, "/v1/gateways", normal_token)[:2],
            [gateway_b],
            ["operator"],
        )
        print(
            "PASS: real GoTrue tokens through Nginx enforce Gateway/Sensor membership and revocation"
        )

    def real_auth(self) -> None:
        spec = importlib.util.spec_from_file_location(
            "jwt_spike", ROOT / "scripts/spikes/jwt_contract.py"
        )
        spike = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(spike)
        # GoTrue v2.196.0 migrations grant SELECT to a role literally named
        # postgres. Our custom setup superuser is stage2_admin; supply only a
        # NOLOGIN compatibility role, never use it as the backend identity.
        self.command(
            [
                "docker",
                "exec",
                "-i",
                self.postgres,
                "psql",
                "-U",
                "stage2_admin",
                "-d",
                "stage2_auth_test",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            stdin="CREATE ROLE postgres NOLOGIN;\n",
        )
        now = int(time.time())
        self.anon = make_token(
            self.secret, {"role": "anon", "iat": now, "exp": now + 3600}
        )
        self.service = make_token(
            self.secret, {"role": "service_role", "iat": now, "exp": now + 3600}
        )
        self.sensitive.extend([self.anon, self.service])
        gotrue = self.create(
            "gotrue",
            "supabase/gotrue:v2.196.0",
            {
                "API_EXTERNAL_URL": "http://localhost",
                "GOTRUE_API_HOST": "0.0.0.0",
                "GOTRUE_API_PORT": "9999",
                "GOTRUE_DB_DRIVER": "postgres",
                "GOTRUE_DB_DATABASE_URL": self.db_url(
                    "postgres:5432", "supabase_auth_admin", "AUTH_DB_PASSWORD"
                ),
                "GOTRUE_SITE_URL": "http://localhost",
                "GOTRUE_JWT_ADMIN_ROLES": "service_role",
                "GOTRUE_JWT_AUD": "authenticated",
                "GOTRUE_JWT_DEFAULT_GROUP_NAME": "authenticated",
                "GOTRUE_JWT_ISSUER": ISSUER,
                "GOTRUE_JWT_SECRET": self.secret,
                "GOTRUE_EXTERNAL_EMAIL_ENABLED": "true",
                "GOTRUE_MAILER_AUTOCONFIRM": "true",
                "GOTRUE_DISABLE_SIGNUP": "true",
            },
            ["--network-alias", "auth", "-p", "127.0.0.1::9999"],
        )
        self.start(gotrue)
        auth_base = f"http://127.0.0.1:{self.port(gotrue, 9999)}"
        self.wait(
            lambda: http_request(auth_base, "/health")[0] == 200,
            "GoTrue health",
            gotrue,
        )
        self.command(
            [
                "docker",
                "exec",
                "-i",
                self.postgres,
                "psql",
                "-U",
                "stage2_admin",
                "-d",
                "stage2_auth_test",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            stdin=(ROOT / "migrations/000004_supabase_compat.up.sql").read_text(),
        )
        dashboard_password = secrets.token_urlsafe(24)
        self.sensitive.append(dashboard_password)
        envoy = self.create(
            "envoy",
            "envoyproxy/envoy:v1.39.1",
            {
                "ANON_KEY": self.anon,
                "SERVICE_ROLE_KEY": self.service,
                "SUPABASE_PUBLIC_URL": "http://localhost",
                "DASHBOARD_USERNAME": "test-admin",
                "DASHBOARD_PASSWORD": dashboard_password,
            },
            [
                "--network-alias",
                "kong",
                "--entrypoint",
                "/bin/sh",
                *[
                    part
                    for name in ("envoy.yaml", "cds.yaml", "lds.template.yaml")
                    for part in (
                        "-v",
                        f"{ROOT / 'config/envoy' / name}:/etc/envoy/{name}:ro",
                    )
                ],
                "-v",
                f"{ROOT / 'config/envoy/docker-entrypoint.sh'}:/docker-entrypoint.sh:ro",
            ],
            ["/docker-entrypoint.sh"],
        )
        self.start(envoy)
        nginx = self.create(
            "nginx",
            "nginx:1.25-alpine",
            {"API_DOMAIN": "localhost"},
            [
                "-p",
                "127.0.0.1::80",
                "-v",
                f"{ROOT / 'config/nginx/nginx.conf.template'}:/etc/nginx/templates/default.conf.template:ro",
            ],
        )
        self.start(nginx)
        base = f"http://127.0.0.1:{self.port(nginx, 80)}"
        self.wait(
            lambda: http_request(base, "/auth/v1/health", key=self.anon)[0] == 200,
            "Nginx -> Envoy -> GoTrue",
            nginx,
        )
        self.command(["docker", "exec", nginx, "nginx", "-t"])
        denied_email = (
            "stage2-signup-denied-" + secrets.token_hex(8) + "@example.invalid"
        )
        denied_password = secrets.token_urlsafe(24)
        self.sensitive.append(denied_password)
        status, body, _ = http_request(
            base,
            "/auth/v1/signup",
            key=self.anon,
            body={"email": denied_email, "password": denied_password},
            method="POST",
        )
        assert_signup_denied(status, body)
        self.check_account_rows(denied_email, present=False)
        print(
            "PASS: public signup denied by GoTrue; no user/profile/membership created"
        )
        users = []
        emails = []
        try:
            tokens = []
            for _ in range(3):
                password = secrets.token_urlsafe(24)
                self.sensitive.append(password)
                email = "stage2-auth-" + secrets.token_hex(8) + "@example.invalid"
                status, created, _ = http_request(
                    base,
                    "/auth/v1/admin/users",
                    token=self.service,
                    key=self.service,
                    body={"email": email, "password": password, "email_confirm": True},
                    method="POST",
                )
                user_id = admin_created_user_id(status, created)
                users.append(user_id)
                emails.append(email)
                self.check_account_rows(email, present=True)
                status, login, _ = http_request(
                    base,
                    "/auth/v1/token?grant_type=password",
                    key=self.anon,
                    body={"email": email, "password": password},
                    method="POST",
                )
                if status != 200:
                    raise RuntimeError("GoTrue login failed; response suppressed")
                token, refresh = login["access_token"], login["refresh_token"]
                tokens.append(token)
                self.sensitive.extend([token, refresh])
                _, claims = spike.verify_token(
                    token, self.secret, ISSUER, "authenticated"
                )
                if claims["sub"] != user_id:
                    raise RuntimeError("GoTrue token subject does not match test user")
                self.check_routes(base, token)
                self.check_bad_tokens(base, claims)
                # Human identity (even later bootstrapped admin) is not a service key.
                status, _, _ = http_request(
                    base,
                    "/auth/v1/admin/users",
                    token=token,
                    key=self.anon,
                    body={
                        "email": denied_email,
                        "password": denied_password,
                        "email_confirm": True,
                    },
                    method="POST",
                )
                if status != 403:
                    raise RuntimeError(
                        f"human token could not demonstrate Admin API denial (HTTP {status})"
                    )
                self.check_account_rows(denied_email, present=False)
                status, refreshed, _ = http_request(
                    base,
                    "/auth/v1/token?grant_type=refresh_token",
                    key=self.anon,
                    body={"refresh_token": refresh},
                    method="POST",
                )
                if status != 200:
                    raise RuntimeError("GoTrue refresh failed; response suppressed")
                self.sensitive.extend(
                    [refreshed["access_token"], refreshed["refresh_token"]]
                )
                _, refreshed_claims = spike.verify_token(
                    refreshed["access_token"], self.secret, ISSUER, "authenticated"
                )
                if any(
                    claims[key] != refreshed_claims[key]
                    for key in ("iss", "sub", "aud", "role")
                ):
                    raise RuntimeError("refresh changed stable identity claims")
                assert_response(
                    *http_request(base, "/v1/ws", refreshed["access_token"]), 501
                )
                if len(users) == 1:
                    admin_token = token
                elif len(users) == 2:
                    normal_token = token
            # Bootstrap uses the existing script, administrator DB credential stays in the container.
            self.command(
                [
                    "docker",
                    "exec",
                    "-i",
                    self.postgres,
                    "sh",
                    "-c",
                    'export PGHOST=127.0.0.1 PGUSER="$POSTGRES_USER" PGDATABASE="$POSTGRES_DB" PGPASSWORD="$POSTGRES_PASSWORD" PLATFORM_ADMIN_USER_ID; read -r PLATFORM_ADMIN_USER_ID; exec sh /bootstrap.sh',
                ],
                stdin=users[0] + "\n",
            )
            result = subprocess.run(
                [
                    "go",
                    "test",
                    "-v",
                    "-race",
                    "-count=1",
                    "./internal/httpserver",
                    "-run",
                    "^TestGoTrueAdminBoundaryIntegration$",
                ],
                cwd=ROOT / "src",
                text=True,
                capture_output=True,
                timeout=180,
                env={
                    **os.environ,
                    "AUTH_INTEGRATION_ISOLATED": "1",
                    "AUTH_TEST_DATABASE_URL": self.host_db_url,
                    "AUTH_TEST_SECRET": self.secret,
                    "AUTH_TEST_ISSUER": ISSUER,
                    "AUTH_TEST_ADMIN_TOKEN": admin_token,
                    "AUTH_TEST_NORMAL_TOKEN": normal_token,
                },
            )
            if (
                result.returncode
                or "--- PASS: TestGoTrueAdminBoundaryIntegration " not in result.stdout
            ):
                raise RuntimeError(
                    "GoTrue-token/real-DB admin guard integration failed; output suppressed"
                )
            self.check_real_provisioning_api(base, users, tokens)
            self.cleanup_provisioned_resources()
            self.check_real_authorization_api(base, users, admin_token, normal_token)
            print(
                "PASS: Admin-created users have profiles but no automatic Gateway/admin grants"
            )
            print(
                "PASS: human token cannot create Auth users; GoTrue login/refresh + admin guard 204/403"
            )
        finally:
            self.cleanup_provisioned_resources()
            cleanup_errors = []
            for user_id, email in zip(users, emails):
                try:
                    status, _, _ = http_request(
                        base,
                        "/auth/v1/admin/users/" + user_id,
                        token=self.service,
                        key=self.service,
                        method="DELETE",
                    )
                    if status not in (200, 204):
                        raise RuntimeError("test user cleanup failed")
                    self.check_account_rows(email, present=False)
                except Exception:
                    # Attempt every owned account; never silently accept an orphan.
                    cleanup_errors.append("account")
            if cleanup_errors:
                raise RuntimeError("test user cleanup failed; response suppressed")

    def check_logs(self) -> None:
        for name in self.containers:
            result = subprocess.run(
                ["docker", "logs", name], text=True, capture_output=True, timeout=10
            )
            if result.returncode:
                raise RuntimeError("cannot verify container log redaction")
            if any(value in result.stdout + result.stderr for value in self.sensitive):
                raise RuntimeError(
                    "container logs exposed test credentials; output suppressed"
                )
        print("PASS: container logs contain no test credentials or tokens")

    def diagnostics(self) -> None:
        for name in self.containers:
            if not name.endswith(
                ("-postgres", "-backend", "-gotrue", "-envoy", "-nginx")
            ):
                continue
            result = subprocess.run(
                ["docker", "logs", "--tail", "12", name],
                text=True,
                capture_output=True,
                timeout=10,
            )
            output = result.stdout + result.stderr
            for value in self.sensitive:
                output = output.replace(value, "[REDACTED]")
            print(f"Diagnostic {name}:\n{output}")

    def cleanup(self) -> None:
        errors = []
        for name in reversed(self.containers):
            try:
                self.command(["docker", "rm", "-f", "-v", name])
            except Exception:
                errors.append("container")
        if self.network_created:
            try:
                self.command(["docker", "network", "rm", self.network])
            except Exception:
                errors.append("network")
        if self.owned_image:
            try:
                self.command(["docker", "image", "rm", self.image])
            except Exception:
                errors.append("image")
        if errors:
            raise RuntimeError("isolated resource cleanup failed: " + ", ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("smoke", "auth"))
    args = parser.parse_args()
    stack = Stack()

    def interrupted(signum, _frame):
        raise RuntimeError(f"integration interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        config = stack.prepare()
        stack.negative_startup(config)
        base = stack.start_backend(config)
        now = int(time.time())
        token = make_token(
            stack.secret,
            {
                "iss": ISSUER,
                "aud": "authenticated",
                "role": "authenticated",
                "sub": str(uuid.uuid4()),
                "iat": now,
                "exp": now + 3600,
            },
        )
        stack.sensitive.append(token)
        stack.check_routes(base, token)
        if args.mode == "auth":
            stack.real_auth()
        stack.check_logs()
    except Exception:
        stack.diagnostics()
        raise
    finally:
        stack.cleanup()
    print(f"Stage 2 {args.mode} tests passed; isolated containers/network cleaned up.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Do not expose subprocess/env/token details in unexpected tracebacks.
        print(
            f"Stage 2 integration failed ({type(error).__name__}): "
            + (str(error) if isinstance(error, RuntimeError) else "details suppressed")
        )
        raise SystemExit(1) from None
