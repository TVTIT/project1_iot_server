#!/usr/bin/env python3
"""Verify the JWT contract emitted by the running local GoTrue stack."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def decode_segment(value: str) -> dict[str, object]:
    padding = "=" * (-len(value) % 4)
    return json.loads(base64.urlsafe_b64decode(value + padding))


def verify_token(
    token: str, secret: str, expected_issuer: str, expected_audience: str
) -> tuple[dict[str, object], dict[str, object]]:
    parts = token.split(".")
    if len(parts) != 3:
        raise RuntimeError("access token is not a three-part JWT")

    header = decode_segment(parts[0])
    claims = decode_segment(parts[1])
    if header.get("alg") != "HS256":
        raise RuntimeError(f"unexpected JWT algorithm: {header.get('alg')!r}")

    expected = hmac.new(
        secret.encode(), f"{parts[0]}.{parts[1]}".encode(), hashlib.sha256
    ).digest()
    actual = base64.urlsafe_b64decode(parts[2] + "=" * (-len(parts[2]) % 4))
    if not hmac.compare_digest(expected, actual):
        raise RuntimeError("JWT signature verification failed")

    now = int(time.time())
    for claim in ("iss", "aud", "sub", "role", "iat", "exp"):
        if claim not in claims:
            raise RuntimeError(f"JWT is missing required claim {claim!r}")
    if claims["iss"] != expected_issuer:
        raise RuntimeError("JWT issuer does not match GOTRUE_JWT_ISSUER")
    if not isinstance(claims["exp"], int) or claims["exp"] <= now:
        raise RuntimeError("JWT is expired or has a non-integer exp claim")
    if not isinstance(claims["iat"], int):
        raise RuntimeError("JWT has a non-integer iat claim")
    if claims.get("role") != "authenticated":
        raise RuntimeError(f"unexpected human-user role: {claims.get('role')!r}")
    audience = claims.get("aud")
    if audience != expected_audience and not (
        isinstance(audience, list) and expected_audience in audience
    ):
        raise RuntimeError(f"unexpected JWT audience: {audience!r}")
    try:
        uuid.UUID(str(claims["sub"]))
    except ValueError as error:
        raise RuntimeError("JWT sub claim is not a UUID") from error
    return header, claims


def request_json(
    url: str,
    api_key: str,
    body: dict[str, object] | None = None,
    bearer: str | None = None,
    method: str = "POST",
) -> dict[str, object]:
    data = None if body is None else json.dumps(body).encode()
    headers = {"apikey": api_key, "Content-Type": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        raise RuntimeError(f"{method} {url} returned HTTP {error.code}: {detail}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise RuntimeError(f"{method} {url} failed: {error}") from error


def main() -> int:
    base_url = os.environ.get("JWT_SPIKE_BASE_URL", "http://127.0.0.1").rstrip("/")
    parsed_url = urllib.parse.urlparse(base_url)
    if parsed_url.scheme != "http" or parsed_url.hostname not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("JWT_SPIKE_BASE_URL must be a local HTTP endpoint")
    anon_key = required("ANON_KEY")
    service_key = required("SERVICE_ROLE_KEY")
    jwt_secret = required("JWT_SECRET")
    expected_issuer = required("GOTRUE_JWT_ISSUER")
    expected_audience = required("SUPABASE_JWT_AUDIENCE")
    email = f"stage2-jwt-spike-{secrets.token_hex(8)}@example.invalid"
    password = secrets.token_urlsafe(24)
    user_id = ""
    primary_error: Exception | None = None

    try:
        jwks = request_json(
            f"{base_url}/auth/v1/.well-known/jwks.json",
            anon_key,
            method="GET",
        )
        keys = jwks.get("keys")
        if not isinstance(keys, list):
            raise RuntimeError("JWKS response does not contain a keys array")

        signup = request_json(
            f"{base_url}/auth/v1/signup",
            anon_key,
            {"email": email, "password": password},
        )
        user = signup.get("user")
        if not isinstance(user, dict) or not isinstance(user.get("id"), str):
            raise RuntimeError("signup response did not contain a user id")
        user_id = user["id"]

        login = request_json(
            f"{base_url}/auth/v1/token?grant_type=password",
            anon_key,
            {"email": email, "password": password},
        )
        access_token = login.get("access_token")
        refresh_token = login.get("refresh_token")
        if not isinstance(access_token, str) or not isinstance(refresh_token, str):
            raise RuntimeError("password login did not return access and refresh tokens")
        header, claims = verify_token(
            access_token, jwt_secret, expected_issuer, expected_audience
        )

        refreshed = request_json(
            f"{base_url}/auth/v1/token?grant_type=refresh_token",
            anon_key,
            {"refresh_token": refresh_token},
        )
        refreshed_token = refreshed.get("access_token")
        if not isinstance(refreshed_token, str):
            raise RuntimeError("refresh flow did not return an access token")
        refreshed_header, refreshed_claims = verify_token(
            refreshed_token, jwt_secret, expected_issuer, expected_audience
        )

        stable_claims = ("iss", "aud", "sub", "role")
        for claim in stable_claims:
            if claims.get(claim) != refreshed_claims.get(claim):
                raise RuntimeError(f"claim {claim!r} changed during token refresh")

        summary = {
            "algorithm": header["alg"],
            "refresh_algorithm": refreshed_header["alg"],
            "issuer_matches_expected": True,
            "jwks_key_count": len(keys),
            "audience": claims["aud"],
            "role": claims["role"],
            "subject_type": "uuid",
            "has_nbf": "nbf" in claims,
            "refresh_contract_stable": True,
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
    except (RuntimeError, ValueError, json.JSONDecodeError) as error:
        primary_error = error

    cleanup_error: Exception | None = None
    if user_id:
        try:
            request_json(
                f"{base_url}/auth/v1/admin/users/{user_id}",
                service_key,
                bearer=service_key,
                method="DELETE",
            )
        except RuntimeError as error:
            cleanup_error = error

    if primary_error is not None:
        if cleanup_error is not None:
            print(f"JWT spike cleanup also failed: {cleanup_error}", file=sys.stderr)
        raise primary_error
    if cleanup_error is not None:
        raise RuntimeError(f"JWT contract passed but test-user cleanup failed: {cleanup_error}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"JWT contract spike failed: {error}", file=sys.stderr)
        sys.exit(1)
