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
from pathlib import Path
import secrets
import signal
import subprocess
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
POSTGRES_IMAGE = (
    "timescale/timescaledb@sha256:289d55704b1b3ee8263cd3805c6930f9cd54506835a8f19f9b85dad17d5c5a8a"
)
ISSUER = "http://auth.test.local/auth/v1"


def make_token(secret: str, claims: dict, algorithm: str = "HS256") -> str:
    """Test tokens only; production verification remains in Go's JWT library."""

    def encode(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()

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


def assert_startup_failure(code: int, output: str, field: str, sensitive: list[str]) -> None:
    if code != 1 or field not in output:
        raise RuntimeError(f"startup did not fail at configuration validation for {field}")
    if any(value and value in output for value in sensitive):
        raise RuntimeError("startup log exposed test credentials")


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
            if self.command(["docker", "inspect", "-f", "{{.State.Running}}", container]) != "true":
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
            self.command(["docker", "build", "-t", self.image, str(ROOT / "src")], timeout=300)
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
            "DATABASE_URL": self.db_url("postgres:5432", "iot_backend_app", "BACKEND_DB_PASSWORD"),
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
                ["docker", "start", "-a", container], text=True, capture_output=True, timeout=20
            )
            code = int(self.command(["docker", "inspect", "-f", "{{.State.ExitCode}}", container]))
            result = subprocess.run(
                ["docker", "logs", container], text=True, capture_output=True, timeout=10
            )
            if result.returncode:
                raise RuntimeError("cannot read negative startup logs")
            assert_startup_failure(
                code, result.stdout + result.stderr, key, self.sensitive + ["short-test-key"]
            )
        print("PASS: six negative startup cases (exit 1, config error, no credential leak)")

    def start_backend(self, config: dict) -> str:
        self.backend = self.create(
            "backend", self.image, config, ["--network-alias", "backend", "-p", "127.0.0.1::8080"]
        )
        self.start(self.backend)
        base = f"http://127.0.0.1:{self.port(self.backend, 8080)}"
        self.wait(
            lambda: http_request(base, "/readyz")[0] == 200, "backend readiness", self.backend
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
        self.anon = make_token(self.secret, {"role": "anon", "iat": now, "exp": now + 3600})
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
            },
            ["--network-alias", "auth", "-p", "127.0.0.1::9999"],
        )
        self.start(gotrue)
        auth_base = f"http://127.0.0.1:{self.port(gotrue, 9999)}"
        self.wait(lambda: http_request(auth_base, "/health")[0] == 200, "GoTrue health", gotrue)
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
                    for part in ("-v", f"{ROOT / 'config/envoy' / name}:/etc/envoy/{name}:ro")
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
        users = []
        try:
            for _ in range(2):
                password = secrets.token_urlsafe(24)
                self.sensitive.append(password)
                email = "stage2-auth-" + secrets.token_hex(8) + "@example.invalid"
                status, signup, _ = http_request(
                    base,
                    "/auth/v1/signup",
                    key=self.anon,
                    body={"email": email, "password": password},
                    method="POST",
                )
                if status != 200 or not isinstance(signup, dict) or "user" not in signup:
                    raise RuntimeError(f"signup failed (HTTP {status}); response suppressed")
                user_id = str(uuid.UUID(signup["user"]["id"]))
                users.append(user_id)
                self.sensitive.extend([signup["access_token"], signup["refresh_token"]])
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
                self.sensitive.extend([token, refresh])
                _, claims = spike.verify_token(token, self.secret, ISSUER, "authenticated")
                if claims["sub"] != user_id:
                    raise RuntimeError("GoTrue token subject does not match test user")
                self.check_routes(base, token)
                self.check_bad_tokens(base, claims)
                status, refreshed, _ = http_request(
                    base,
                    "/auth/v1/token?grant_type=refresh_token",
                    key=self.anon,
                    body={"refresh_token": refresh},
                    method="POST",
                )
                if status != 200:
                    raise RuntimeError("GoTrue refresh failed; response suppressed")
                self.sensitive.extend([refreshed["access_token"], refreshed["refresh_token"]])
                _, refreshed_claims = spike.verify_token(
                    refreshed["access_token"], self.secret, ISSUER, "authenticated"
                )
                if any(
                    claims[key] != refreshed_claims[key] for key in ("iss", "sub", "aud", "role")
                ):
                    raise RuntimeError("refresh changed stable identity claims")
                assert_response(*http_request(base, "/v1/ws", refreshed["access_token"]), 501)
                if len(users) == 1:
                    admin_token = token
                else:
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
            print("PASS: GoTrue login/refresh + real PostgreSQL admin 204 / normal user 403")
        finally:
            for user_id in users:
                status, _, _ = http_request(
                    base,
                    "/auth/v1/admin/users/" + user_id,
                    token=self.service,
                    key=self.service,
                    method="DELETE",
                )
                if status not in (200, 204):
                    raise RuntimeError("test user cleanup failed")

    def check_logs(self) -> None:
        for name in self.containers:
            result = subprocess.run(
                ["docker", "logs", name], text=True, capture_output=True, timeout=10
            )
            if result.returncode:
                raise RuntimeError("cannot verify container log redaction")
            if any(value in result.stdout + result.stderr for value in self.sensitive):
                raise RuntimeError("container logs exposed test credentials; output suppressed")
        print("PASS: container logs contain no test credentials or tokens")

    def diagnostics(self) -> None:
        for name in self.containers:
            if not name.endswith(("-postgres", "-backend", "-gotrue", "-envoy", "-nginx")):
                continue
            result = subprocess.run(
                ["docker", "logs", "--tail", "12", name], text=True, capture_output=True, timeout=10
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
