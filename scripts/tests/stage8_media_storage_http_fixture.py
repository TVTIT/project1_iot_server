#!/usr/bin/env python3
"""Owned local require-run proof for the Stage 8 signed Storage PUT route."""

import datetime as dt
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
import struct
from pathlib import Path
import re
import secrets
import shutil
import signal
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import time
import threading
import uuid
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/tests"))
sys.path.insert(0, str(ROOT / "scripts"))
import native_compose_smoke as smoke  # noqa: E402

VIDEO_ID = "media-video"
VIDEO_BYTES = b"\x00\x00\x00\x18ftypmp42stage8-local-native-proof"
MAX_OBJECT_BYTES = 80_000_000
ACTIVE_GATEWAY_PROCESS = None


class FirstStorageReadDelayProxy:
    """Fixture-only Storage forwarder that delays one authenticated object read response."""

    def __init__(self, storage_port, bind_address, delay_seconds=5):
        self.storage_port = storage_port
        self.bind_address = bind_address
        self.delay_seconds = delay_seconds
        self.first_get_delayed = threading.Event()
        self.lock = threading.Lock()
        self.get_count = 0

        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, _format, *_args):
                return

            def do_GET(self):
                self.forward()

            def do_POST(self):
                self.forward()

            def do_PUT(self):
                self.forward()

            def do_DELETE(self):
                self.forward()

            def do_OPTIONS(self):
                self.forward()

            def forward(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() not in ("host", "connection", "transfer-encoding")
                }
                request = urllib.request.Request(
                    f"http://127.0.0.1:{owner.storage_port}{self.path}",
                    data=body if body else None, headers=headers, method=self.command,
                )
                try:
                    with urllib.request.build_opener(
                        urllib.request.ProxyHandler({}),
                    ).open(request, timeout=20) as response:
                        status = response.status
                        response_headers = dict(response.headers.items())
                        payload = response.read(MAX_OBJECT_BYTES + 1)
                except urllib.error.HTTPError as error:
                    status = error.code
                    response_headers = dict(error.headers.items())
                    payload = error.read(MAX_OBJECT_BYTES + 1)
                    error.close()
                except (urllib.error.URLError, TimeoutError, OSError):
                    self.send_error(502)
                    return

                delay_this_response = False
                if self.command == "GET" and self.path.startswith(
                    "/object/authenticated/media-video/",
                ):
                    with owner.lock:
                        owner.get_count += 1
                        if owner.get_count == 1:
                            delay_this_response = True
                            owner.first_get_delayed.set()
                if delay_this_response:
                    time.sleep(owner.delay_seconds)

                self.send_response(status)
                for key, value in response_headers.items():
                    if key.lower() not in (
                        "connection", "transfer-encoding", "content-length",
                        "keep-alive", "content-encoding",
                    ):
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass

        self.server = ThreadingHTTPServer((self.bind_address, 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class ProofFailure(Exception):
    pass


def require(condition, code):
    if not condition:
        raise ProofFailure(code)


def safe_compose_diagnostic(output, fixture):
    text = output.decode(errors="replace")
    for name, value in fixture.values.items():
        if ("KEY" in name or "SECRET" in name or "PASSWORD" in name) and isinstance(value, str) and len(value) >= 8:
            text = text.replace(value, "[redacted]")
    candidates = [line.strip() for line in text.splitlines() if line.strip()]
    return candidates[-1][:500] if candidates else "compose_up_nonzero"


def compose_prefix(fixture, storage_overlay, media_overlay=True):
    files = [
        "-f", str(smoke.ROOT_COMPOSE), "-f", str(fixture.override),
        "-f", str(smoke.PAPER_P0_TEMPLATE), "-f", str(fixture.private / "compose.p2.yml"),
    ]
    if media_overlay:
        files.extend(["-f", str(ROOT / "docker-compose.stage8-media.yml")])
    files.extend(["-f", str(storage_overlay)])
    return [
        "docker", "compose", "--project-directory", str(ROOT), "--project-name",
        fixture.project, "--env-file", str(fixture.env_file), *files,
    ]


def run(command, fixture, timeout=120, input_bytes=None, check=True):
    try:
        result = subprocess.run(
            command, input=input_bytes, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=smoke.process_env(fixture), timeout=timeout, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ProofFailure("fixture_command_failed") from None
    if check and result.returncode:
        raise ProofFailure("fixture_command_failed")
    return result


def storage_request(port, method, path, token, body=None, headers=None):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=body, method=method,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json",
                 **(headers or {})},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=10) as response:
            raw = response.read(65537)
            if len(raw) > 65536:
                raise ProofFailure("storage_control_response_too_large")
            return response.status, raw
    except urllib.error.HTTPError as error:
        raw = error.read(65537)
        if len(raw) > 65536:
            raise ProofFailure("storage_control_response_too_large")
        return error.code, raw
    except (urllib.error.URLError, TimeoutError, OSError):
        raise ProofFailure("storage_internal_request_failed") from None


def https_request(endpoint, method, target, body=b"", headers=None, timeout=20):
    parsed = urllib.parse.urlsplit(endpoint.url)
    context = ssl.create_default_context(cafile=str(endpoint.ca_file))
    connection = http.client.HTTPSConnection(
        parsed.hostname, parsed.port, timeout=timeout, context=context,
    )
    try:
        connection.request(method, target, body=body, headers=headers or {})
        response = connection.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ProofFailure("http_response_too_large")
        return response.status, dict(response.getheaders()), raw
    except (OSError, http.client.HTTPException, ssl.SSLError) as exc:
        raise ProofFailure(f"verified_https_request_failed_{type(exc).__name__}_{exc}") from None
    finally:
        connection.close()


def mapped_loopback_port(prefix, fixture, service, target):
    result = run(prefix + ["port", service, str(target)], fixture)
    address = result.stdout.decode().strip()
    host, separator, port = address.rpartition(":")
    require(separator and host in ("127.0.0.1", "0.0.0.0") and port.isdigit(),
            "fixture_port_mapping_invalid")
    require(host == "127.0.0.1", "fixture_port_not_loopback")
    return int(port)


def docker_host_gateway(fixture):
    result = subprocess.run(
        ["docker", "network", "inspect", "bridge", "--format",
         "{{(index .IPAM.Config 0).Gateway}}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=smoke.process_env(fixture), timeout=15, check=False,
    )
    try:
        address = ipaddress.ip_address(result.stdout.decode().strip())
    except ValueError:
        raise ProofFailure("docker_host_gateway_unavailable") from None
    require(result.returncode == 0 and address.is_private,
            "docker_host_gateway_unavailable")
    return str(address)


def make_grant(fixture, internal_port, public_port, object_path):
    result_path = fixture.private / "signed-grant.json"
    result_path.unlink(missing_ok=True)
    env = smoke.process_env(fixture)
    env["PATH"] = os.environ.get("PATH", env["PATH"])
    for key in ("GOCACHE", "GOMODCACHE", "GOPROXY"):
        if key in os.environ:
            env[key] = os.environ[key]
    env.setdefault("GOCACHE", str(fixture.private / "go-cache"))
    env.update({
        "MEDIA_STORAGE_NATIVE_INTERNAL_URL": f"http://127.0.0.1:{internal_port}",
        "MEDIA_STORAGE_NATIVE_PUBLIC_ORIGIN": f"https://127.0.0.1:{public_port}",
        "MEDIA_STORAGE_NATIVE_TOKEN": fixture.values["SERVICE_ROLE_KEY"],
        "MEDIA_STORAGE_NATIVE_OBJECT_PATH": object_path,
        "MEDIA_STORAGE_NATIVE_RESULT_FILE": str(result_path),
    })
    try:
        tested = subprocess.run(
            ["go", "test", "./internal/mediastorage", "-count=1",
             "-run", "^TestNativeStage8SignedUpload$"],
            cwd=ROOT / "src", env=env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=90, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise ProofFailure("native_client_sign_test_failed") from None
    if tested.returncode or not result_path.is_file():
        raise ProofFailure(
            f"native_client_sign_test_failed_exit_{tested.returncode}:" +
            safe_compose_diagnostic(tested.stdout + tested.stderr, fixture)
        )
    try:
        raw = result_path.read_text()
        grant = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        raise ProofFailure("native_signed_grant_invalid") from None
    finally:
        result_path.unlink(missing_ok=True)
    require(result_path.exists() is False, "private_grant_cleanup_failed")
    return grant


def grant_target(grant, public_port, object_path):
    parsed = urllib.parse.urlsplit(grant.get("url", ""))
    require(parsed.scheme == "https" and parsed.hostname == "127.0.0.1"
            and parsed.port == public_port and parsed.fragment == "",
            "native_signed_grant_origin_invalid")
    expected_path = f"/storage/v1/object/upload/sign/{VIDEO_ID}/{object_path}"
    require(parsed.path == expected_path, "native_signed_grant_path_invalid")
    values = urllib.parse.parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    require(set(values) == {"token"} and len(values["token"]) == 1 and values["token"][0],
            "native_signed_grant_query_invalid")
    expires = dt.datetime.fromisoformat(grant["expires_at"].replace("Z", "+00:00"))
    require(expires > dt.datetime.now(dt.timezone.utc), "native_signed_grant_expired")
    return parsed.path + "?" + parsed.query, values["token"][0], expires


def safe_http_error_class(body):
    lowered = body.lower()
    if b"upstream connect error" in lowered:
        return "upstream_connect_error"
    if b"upstream reset" in lowered or b"upstream_reset" in lowered:
        return "upstream_reset"
    if b"no healthy upstream" in lowered:
        return "no_healthy_upstream"
    if b"access denied" in lowered:
        return "access_denied"
    return "unclassified"


def safe_json_error_fields(body):
    try:
        value = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    if not isinstance(value, dict):
        return {}
    allowed = ("statusCode", "error", "message", "code")
    return {
        key: value[key] for key in allowed
        if key in value and isinstance(value[key], (str, int))
        and len(str(value[key])) < 160
    }


def prove_legacy_storage(fixture, prefix, report, *, api_endpoint, dedicated_endpoint=None):
    report["phase"] = "legacy_storage_route_check"
    storage_token = fixture.values["SERVICE_ROLE_KEY"]
    anon_token = fixture.values["ANON_KEY"]
    legacy_image_name = "legacy-probe-" + secrets.token_hex(4) + ".png"
    legacy_image_bytes = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00"
        b"\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    legacy_auth_headers = {
        "Authorization": "Bearer " + storage_token,
        "apikey": storage_token,
    }

    # 1. Bucket read on media-images
    legacy_bucket = https_request(
        api_endpoint, "GET", "/storage/v1/bucket/media-images",
        headers=legacy_auth_headers,
    )
    require(
        legacy_bucket[0] == 200,
        f"legacy_bucket_read_failed_http_{legacy_bucket[0]}",
    )

    # 2. Standard image read/write
    legacy_post = https_request(
        api_endpoint, "POST", f"/storage/v1/object/media-images/{legacy_image_name}",
        body=legacy_image_bytes,
        headers={**legacy_auth_headers, "Content-Type": "image/png"},
    )
    require(
        legacy_post[0] in (200, 201),
        f"legacy_image_write_failed_http_{legacy_post[0]}",
    )
    legacy_get = https_request(
        api_endpoint, "GET",
        f"/storage/v1/object/authenticated/media-images/{legacy_image_name}",
        headers=legacy_auth_headers,
    )
    require(
        legacy_get[0] == 200 and legacy_get[2] == legacy_image_bytes,
        f"legacy_image_read_failed_http_{legacy_get[0]}",
    )

    # 3. Exact basename containing media-video: media-video.png
    basename_image = "media-video.png"
    basename_post = https_request(
        api_endpoint, "POST", f"/storage/v1/object/media-images/{basename_image}",
        body=legacy_image_bytes,
        headers={**legacy_auth_headers, "Content-Type": "image/png"},
    )
    require(
        basename_post[0] in (200, 201),
        f"legacy_image_exact_basename_write_failed_http_{basename_post[0]}",
    )
    basename_get = https_request(
        api_endpoint, "GET",
        f"/storage/v1/object/authenticated/media-images/{basename_image}",
        headers=legacy_auth_headers,
    )
    require(
        basename_get[0] == 200 and basename_get[2] == legacy_image_bytes,
        f"legacy_image_exact_basename_read_failed_http_{basename_get[0]}",
    )

    # 4. Nested segment containing media-video: nested/media-video/photo.png
    nested_image = "nested/media-video/photo.png"
    nested_post = https_request(
        api_endpoint, "POST", f"/storage/v1/object/media-images/{nested_image}",
        body=legacy_image_bytes,
        headers={**legacy_auth_headers, "Content-Type": "image/png"},
    )
    require(
        nested_post[0] in (200, 201),
        f"legacy_image_nested_segment_write_failed_http_{nested_post[0]}",
    )
    nested_get = https_request(
        api_endpoint, "GET",
        f"/storage/v1/object/authenticated/media-images/{nested_image}",
        headers=legacy_auth_headers,
    )
    require(
        nested_get[0] == 200 and nested_get[2] == legacy_image_bytes,
        f"legacy_image_nested_segment_read_failed_http_{nested_get[0]}",
    )

    # 5. Dedicated listener isolation (if present)
    if dedicated_endpoint is not None:
        require(
            https_request(dedicated_endpoint, "GET", "/storage/v1/bucket/media-images")[0] == 404,
            "dedicated_listener_leaked_legacy_storage_bucket",
        )
        require(
            https_request(dedicated_endpoint, "POST", f"/storage/v1/object/media-images/{legacy_image_name}",
                          body=legacy_image_bytes, headers={"Content-Type": "image/png"})[0] == 404,
            "dedicated_listener_leaked_legacy_storage_write",
        )

    # 6. Defensive route boundary assertions on api_endpoint for media-video
    object_probe = "9e55f26c-2a01-4f77-a1cb-" + secrets.token_hex(6) + ".mp4"
    require(
        https_request(api_endpoint, "GET",
                      f"/storage/v1/object/authenticated/media-video/{object_probe}")[0] == 404,
        "nginx_media_video_read_not_rejected",
    )
    require(
        https_request(api_endpoint, "POST",
                      f"/storage/v1/object/upload/sign/media-video/{object_probe}")[0] == 404,
        "nginx_media_video_sign_not_rejected",
    )
    require(
        https_request(api_endpoint, "PUT",
                      f"/storage/v1/object/upload/sign/media-video/{object_probe}",
                      body=VIDEO_BYTES)[0] == 404,
        "nginx_media_video_put_not_rejected",
    )
    require(
        https_request(api_endpoint, "POST", "/storage/v1/bucket/media-video")[0] == 404,
        "nginx_media_video_bucket_admin_not_rejected",
    )
    unauth_bucket_resp = https_request(
        api_endpoint, "POST", "/storage/v1/bucket",
        body=json.dumps({"id": "media-video", "name": "media-video"}).encode(),
        headers={
            "Authorization": "Bearer " + anon_token,
            "apikey": anon_token,
            "Content-Type": "application/json",
        },
    )
    unauth_bucket_error = safe_json_error_fields(unauth_bucket_resp[2])
    require(
        unauth_bucket_resp[0] in (401, 403) or (
            unauth_bucket_resp[0] == 400
            and str(unauth_bucket_error.get("statusCode")) == "403"
        ),
        f"nginx_unauthorized_media_bucket_create_not_rejected_http_{unauth_bucket_resp[0]}",
    )

    report["checks"]["legacy_storage_operations"] = {
        "status": "PASS",
        "image_post_status": legacy_post[0],
        "image_get_status": legacy_get[0],
        "bucket_get_status": legacy_bucket[0],
        "exact_basename_status": basename_post[0],
        "exact_basename_get_status": basename_get[0],
        "nested_segment_status": nested_post[0],
        "nested_segment_get_status": nested_get[0],
    }
    report["checks"]["defensive_media_route_boundaries"] = {"status": "PASS"}


def prove_fixture(fixture, prefix, report):
    key = fixture.values["MEDIA_UPLOAD_ROUTE_KEY"]
    storage_token = fixture.values["SERVICE_ROLE_KEY"]
    storage_port = mapped_loopback_port(prefix, fixture, "supabase-storage", 5000)
    upload_port = mapped_loopback_port(prefix, fixture, "stage8-media-upload-test-edge", 443)
    endpoint = smoke.HTTPSEndpoint(
        f"https://127.0.0.1:{upload_port}", fixture.private / "p2-ca.crt",
    )

    # Bootstrap through the existing initializer against the real, loopback-only
    # Storage v1.74 endpoint; both create/update and the initializer readback run.
    env = smoke.process_env(fixture)
    env.update(
        STORAGE_INTERNAL_URL=f"http://127.0.0.1:{storage_port}",
        SERVICE_ROLE_KEY=storage_token,
    )
    init = subprocess.run(
        [sys.executable, str(ROOT / "scripts/init-media-bucket.py")],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=30, check=False,
    )
    if init.returncode:
        readback_status, readback_raw = storage_request(
            storage_port, "GET", "/bucket/media-video", storage_token,
        )
        try:
            storage_error = json.loads(readback_raw).get("code")
        except (json.JSONDecodeError, AttributeError):
            storage_error = None
        allowed_codes = {
            "AccessDenied", "InvalidJWT", "InvalidToken", "InvalidSignature",
            "DatabaseError", "NoSuchBucket", "InvalidRequest",
        }
        safe_code = storage_error if storage_error in allowed_codes else "unclassified"
        raise ProofFailure(
            f"storage_bucket_bootstrap_failed_exit_{init.returncode}:" +
            safe_compose_diagnostic(init.stdout + init.stderr, fixture) +
            f":readback_http_{readback_status}_{safe_code}"
        )
    status, raw = storage_request(storage_port, "GET", "/bucket/media-video",
                                  storage_token)
    try:
        bucket = json.loads(raw)
    except json.JSONDecodeError:
        raise ProofFailure("storage_bucket_readback_invalid") from None
    require(status == 200 and bucket.get("public") is False
            and bucket.get("file_size_limit") == MAX_OBJECT_BYTES
            and bucket.get("allowed_mime_types") == ["video/mp4"],
            "storage_bucket_policy_mismatch")
    report["checks"]["bucket_bootstrap_readback"] = {"status": "PASS", "http_status": status}

    # The dedicated origin has just its probe and upload route. The profile
    # adds a separate service rather than replacing or redirecting base Nginx.
    report["phase"] = "media_health"
    health = https_request(endpoint, "GET", "/healthz")
    require(health[0] == 200 and health[2] == b"OK", "media_listener_health_failed")
    report["phase"] = "media_business_deny"
    require(https_request(endpoint, "GET", "/v1/health")[0] == 404,
            "media_listener_exposed_business_route")
    report["checks"]["dedicated_listener_isolation"] = {"status": "PASS"}

    # Legacy storage API regression check: properly authenticated image write/read
    # through the normal Nginx -> Envoy -> Storage route must succeed, and the dedicated
    # media listener must not leak legacy storage routes.
    api_port = mapped_loopback_port(prefix, fixture, "nginx", 443)
    api_endpoint = smoke.HTTPSEndpoint(
        f"https://127.0.0.1:{api_port}", fixture.private / "p2-ca.crt",
    )
    prove_legacy_storage(fixture, prefix, report, api_endpoint=api_endpoint, dedicated_endpoint=endpoint)

    # Construct a canonical UUID rather than relying on a fixture-specific shape.
    object_path = "8e55f26c-2a01-4f77-a1cb-" + secrets.token_hex(6) + ".mp4"
    report["phase"] = "native_signing"
    grant = make_grant(fixture, storage_port, upload_port, object_path)
    target, token, _ = grant_target(grant, upload_port, object_path)
    report["phase"] = "valid_put_read_overwrite"
    uploaded = https_request(endpoint, "PUT", target, VIDEO_BYTES, {
        "Content-Type": "video/mp4", "Authorization": "Bearer client-spoof",
        "apikey": "client-spoof", "X-User-ID": "client-spoof",
        "X-Role": "service_role", "X-Upsert": "true",
        "X-Stage8-Media-Route": "client-spoof",
    })
    require(uploaded[0] in (200, 201),
            f"signed_https_put_failed_http_{uploaded[0]}_{safe_http_error_class(uploaded[2])}")
    read_status, read_body = storage_request(
        storage_port, "GET", f"/object/authenticated/{VIDEO_ID}/{object_path}",
        storage_token,
    )
    require(read_status == 200 and hashlib.sha256(read_body).hexdigest()
            == hashlib.sha256(VIDEO_BYTES).hexdigest(),
            "direct_storage_read_hash_mismatch")
    overwrite_status = https_request(endpoint, "PUT", target, VIDEO_BYTES, {
        "Content-Type": "video/mp4", "X-Upsert": "true",
    })[0]
    require(overwrite_status not in (200, 201),
            f"signed_put_overwrote_existing_object_http_{overwrite_status}")
    report["checks"]["signed_put_read_hash_and_no_overwrite"] = {
        "status": "PASS", "put_http_status": uploaded[0],
        "read_http_status": read_status, "overwrite_http_status": overwrite_status,
        "bytes": len(VIDEO_BYTES),
        "sha256": hashlib.sha256(VIDEO_BYTES).hexdigest(),
    }

    wrong_id = "3e55f26c-2a01-4f77-a1cb-" + secrets.token_hex(6) + ".mp4"
    wrong_target = target.replace(object_path, wrong_id)
    report["phase"] = "negative_route_probes"
    wrong_status = https_request(endpoint, "PUT", wrong_target, VIDEO_BYTES, {
        "Content-Type": "video/mp4", "X-Stage8-Media-Route": "client-spoof",
    })[0]
    require(wrong_status in (400, 403, 404),
            f"wrong_signed_path_accepted_http_{wrong_status}")
    wrong_method = https_request(endpoint, "GET", target)[0]
    require(wrong_method == 405, "non_put_method_not_rejected")
    require(https_request(endpoint, "POST",
                          "/storage/v1/object/upload/sign/media-video/" + object_path)[0] == 405,
            "public_signing_route_not_rejected")
    require(https_request(endpoint, "POST", "/storage/v1/bucket/media-video")[0] == 404,
            "public_bucket_admin_route_not_rejected")
    require(https_request(endpoint, "GET",
                          "/storage/v1/object/authenticated/media-video/" + object_path)[0] == 404,
            "public_alternate_storage_route_not_rejected")
    # Defensive route boundary assertions: normal clients cannot read, sign,
    # or administer private video via public endpoints.
    require(
        https_request(api_endpoint, "GET",
                      f"/storage/v1/object/authenticated/media-video/{object_path}")[0] == 404,
        "nginx_media_video_read_not_rejected",
    )
    require(
        https_request(api_endpoint, "POST",
                      f"/storage/v1/object/upload/sign/media-video/{object_path}")[0] == 404,
        "nginx_media_video_sign_not_rejected",
    )
    require(
        https_request(api_endpoint, "PUT",
                      f"/storage/v1/object/upload/sign/media-video/{object_path}",
                      body=VIDEO_BYTES)[0] == 404,
        "nginx_media_video_put_not_rejected",
    )
    require(
        https_request(api_endpoint, "POST", "/storage/v1/bucket/media-video")[0] == 404,
        "nginx_media_video_bucket_admin_not_rejected",
    )
    anon_token = fixture.values["ANON_KEY"]
    unauth_bucket_resp = https_request(
        api_endpoint, "POST", "/storage/v1/bucket",
        body=json.dumps({"id": "media-video", "name": "media-video"}).encode(),
        headers={
            "Authorization": "Bearer " + anon_token,
            "apikey": anon_token,
            "Content-Type": "application/json",
        },
    )
    unauth_bucket_error = safe_json_error_fields(unauth_bucket_resp[2])
    require(
        unauth_bucket_resp[0] in (401, 403) or (
            unauth_bucket_resp[0] == 400
            and str(unauth_bucket_error.get("statusCode")) == "403"
        ),
        f"nginx_unauthorized_media_bucket_create_not_rejected_http_{unauth_bucket_resp[0]}",
    )
    report["checks"]["wrong_path_method_and_admin_denials"] = {
        "status": "PASS", "wrong_path": wrong_status, "wrong_method": wrong_method,
        "nginx_defensive_boundaries": "PASS",
    }

    report["phase"] = "missing_object_contract"
    missing_path = "ce55f26c-2a01-4f77-a1cb-" + secrets.token_hex(6) + ".mp4"
    missing_status, missing_raw = storage_request(
        storage_port, "GET", f"/object/authenticated/{VIDEO_ID}/{missing_path}",
        storage_token,
    )
    try:
        missing = json.loads(missing_raw)
    except json.JSONDecodeError:
        raise ProofFailure("native_missing_object_contract_mismatch") from None
    require(missing_status in (400, 404) and missing == {
        "statusCode": "404", "error": "not_found",
        "message": "Object not found", "code": "NoSuchKey",
    }, f"native_missing_object_contract_mismatch_http_{missing_status}_"
       f"{missing.get('code', 'no-code') if isinstance(missing, dict) else 'invalid-json'}")
    report["checks"]["native_no_such_key"] = {
        "status": "PASS", "missing_http_status": missing_status,
        "storage_status_code": missing.get("statusCode"),
        "storage_code": "NoSuchKey",
    }

    report["phase"] = "cap_plus_one"
    cap_path = "9e55f26c-2a01-4f77-a1cb-" + secrets.token_hex(6) + ".mp4"
    cap_grant = make_grant(fixture, storage_port, upload_port, cap_path)
    cap_target, cap_token, _ = grant_target(cap_grant, upload_port, cap_path)
    oversized = bytes(MAX_OBJECT_BYTES + 1)
    cap_response = https_request(endpoint, "PUT", cap_target, oversized, {
        "Content-Type": "video/mp4",
    }, timeout=90)
    cap_status = cap_response[0]
    report["checks"]["storage_cap_response"] = {
        "http_status": cap_status,
        "error": safe_json_error_fields(cap_response[2]),
    }
    cap_error = safe_json_error_fields(cap_response[2])
    require(
        cap_status == 413 or (
            cap_status == 400 and cap_error == {
                "statusCode": "413", "error": "Payload too large",
                "message": "The object exceeded the maximum allowed size",
                "code": "EntityTooLarge",
            }
        ),
        f"storage_cap_plus_one_not_rejected_http_{cap_status}",
    )
    report["checks"]["storage_cap_plus_one"] = {
        "status": "PASS", "declared_bytes": len(oversized), "http_status": cap_status,
    }

    report["phase"] = "expired_token"
    expired_path = "ae55f26c-2a01-4f77-a1cb-" + secrets.token_hex(6) + ".mp4"
    expired_grant = make_grant(fixture, storage_port, upload_port, expired_path)
    expired_target, expired_token, expires_at = grant_target(
        expired_grant, upload_port, expired_path,
    )
    wait_seconds = max(0, (expires_at - dt.datetime.now(dt.timezone.utc)).total_seconds()) + 1.2
    require(wait_seconds <= 40, "native_signed_ttl_unbounded")
    time.sleep(wait_seconds)
    expired_response = https_request(endpoint, "PUT", expired_target, VIDEO_BYTES, {
        "Content-Type": "video/mp4",
    })
    expired_status = expired_response[0]
    expired_error = safe_json_error_fields(expired_response[2])
    require(
        expired_status == 400
        and expired_error == {
            "statusCode": "400", "error": "InvalidJWT",
            "message": '"exp" claim timestamp check failed', "code": "InvalidJWT",
        },
        "expired_signed_token_not_rejected_as_expired",
    )
    report["checks"]["expired_signed_token"] = {
        "status": "PASS", "http_status": expired_status,
        "error": expired_error,
    }

    # Inspect only in-memory logs and artifacts for the actual grant tokens and
    # fixture marker. Raw log text and signed URLs are never persisted or printed.
    report["phase"] = "log_scan"
    logs = run(prefix + ["logs", "--no-color", "stage8-media-upload",
                         "supabase-envoy", "supabase-storage"],
               fixture, timeout=30)
    log_bytes = logs.stdout + logs.stderr
    require(all(value.encode() not in log_bytes for value in (token, cap_token, expired_token, key)),
            "signed_token_present_in_runtime_logs")
    for artifact in fixture.artifacts.rglob("*"):
        if artifact.is_file():
            content = artifact.read_bytes()
            require(all(value.encode() not in content for value in
                        (token, cap_token, expired_token, key)),
                    "signed_token_present_in_artifact")
    report["checks"]["token_free_logs_and_artifacts"] = {"status": "PASS"}


def provision_gateway_credential(fixture, prefix, registry, report, *, sensor_id=None):
    """Create one fixture Gateway and credential through the ordinary P2 APIs."""
    api_port = mapped_loopback_port(prefix, fixture, "nginx", 443)
    endpoint = smoke.HTTPSEndpoint(
        f"https://127.0.0.1:{api_port}", fixture.private / "p2-ca.crt",
    )
    require(smoke.http_request(endpoint, "GET", "/readyz")[0] == 200,
            "p2_api_readiness_failed")
    email = "stage8_" + secrets.token_hex(8) + "@example.invalid"
    password = secrets.token_urlsafe(32)
    user_id = smoke.create_user_via_admin_api(
        endpoint, fixture.values["SERVICE_ROLE_KEY"], email, password,
    )
    token = smoke.login_user_for_token(endpoint, fixture.values["ANON_KEY"], email, password)
    postgres_id = run(prefix + ["ps", "--quiet", "--no-trunc", "postgres"], fixture).stdout.decode().strip()
    require(len(postgres_id) == 64 and all(c in "0123456789abcdef" for c in postgres_id),
            "fixture_postgres_identity_invalid")
    smoke.db_psql(registry, postgres_id,
                  "INSERT INTO platform_admins(user_id) VALUES (:'uid');",
                  {"uid": user_id})
    gateway_id = "gw_stage8_" + secrets.token_hex(6)
    report["phase"] = "gateway_api_provisioning"
    status, _, body = smoke.http_request(
        endpoint, "POST", "/v1/digital-twins", token=token,
        body={"entity_type": "Gateway", "gateway_id": gateway_id,
              "name": "Stage 8 local fixture", "owner_user_id": user_id},
    )
    require(status == 201 and isinstance(body, dict)
            and body.get("entity_id") == "urn:ngsi-ld:Gateway:" + gateway_id,
            "gateway_api_provisioning_failed")
    if sensor_id is not None:
        sensor_status, _, sensor_body = smoke.http_request(
            endpoint, "PUT", f"/v1/gateways/{gateway_id}/sensors/{sensor_id}",
            token=token, body={"name": "Stage 8 local media sensor", "unit": "C"},
        )
        require(sensor_status == 201 and isinstance(sensor_body, dict)
                and sensor_body.get("entity_id")
                == f"urn:ngsi-ld:Sensor:{gateway_id}:{sensor_id}",
                "gateway_sensor_api_provisioning_failed")
    status, headers, credential = smoke.http_request(
        endpoint, "POST", f"/v1/admin/gateways/{gateway_id}/mqtt-credential",
        token=token, headers={"Idempotency-Key": secrets.token_hex(16)}, timeout=40,
    )
    require(status == 201 and headers.get("Cache-Control") == "no-store"
            and isinstance(credential, dict)
            and credential.get("status") == "active"
            and credential.get("username") == gateway_id
            and isinstance(credential.get("password"), str)
            and credential["password"],
            "gateway_credential_api_provisioning_failed")
    credential_status = smoke.db_psql(
        registry, postgres_id,
        "SELECT status FROM gateway_mqtt_credentials WHERE gateway_id=:'gateway_id';",
        {"gateway_id": gateway_id},
    ).strip()
    require(credential_status == "active", "gateway_credential_db_not_active")
    report["checks"]["gateway_provisioned_via_api"] = {
        "status": "PASS", "gateway_id": gateway_id,
        "credential_status": credential_status,
        "credential_cache_control": headers.get("Cache-Control"),
        "password_persisted": False,
    }
    # Read the broker's live DynSec state in memory (never persist this file:
    # it contains credential hashes) and verify the exact provisioned role.
    store_result = run(
        prefix + ["exec", "--no-TTY", "mosquitto", "cat",
                  "/mosquitto/dynsec/dynamic-security.json"],
        fixture,
    )
    try:
        store = json.loads(store_result.stdout)
        client_record = next(c for c in store["clients"] if c["username"] == gateway_id)
        role_record = next(r for r in store["roles"] if r["rolename"] == "gateway_" + gateway_id)
    except (json.JSONDecodeError, KeyError, StopIteration, TypeError):
        raise ProofFailure("gateway_dynsec_readback_invalid") from None
    sys.path.insert(0, str(ROOT / "scripts"))
    import mqtt_backend_acl
    expected_acls = mqtt_backend_acl.expected_gateway_acls(gateway_id, media_enabled=True)
    raw_acls = role_record.get("acls")
    if not isinstance(raw_acls, list):
        raise ProofFailure("gateway_dynsec_acl_list_invalid")
    try:
        actual_acls = {
            (a["acltype"], a["topic"], a["allow"], a["priority"])
            for a in raw_acls
        }
    except (KeyError, TypeError):
        raise ProofFailure("gateway_dynsec_acl_entry_invalid") from None
    assigned_roles = [r.get("rolename") for r in client_record.get("roles", [])]
    client_disabled = client_record.get("disabled", False)
    client_role_ok = (client_disabled is False
                      and assigned_roles == ["gateway_" + gateway_id])
    report["checks"]["dynsec_media_role_diagnostic"] = {
        "client_role_assigned": client_role_ok,
        "client_disabled": client_disabled,
        "assigned_roles": assigned_roles,
        "expected_acl_count": len(expected_acls),
        "actual_acl_count": len(raw_acls),
        "missing_acl_count": len(expected_acls - actual_acls),
        "unexpected_acl_count": len(actual_acls - expected_acls),
        "duplicate_acl_count": len(raw_acls) - len(actual_acls),
    }
    require(client_role_ok and len(raw_acls) == len(actual_acls) and actual_acls == expected_acls,
            "gateway_media_role_readback_mismatch")
    mqtt_port = mapped_loopback_port(prefix, fixture, "mosquitto", 8883)
    report["checks"]["gateway_provisioned_via_api"]["exact_media_dynsec_role"] = "PASS"
    return {"gateway_id": gateway_id, "username": credential["username"],
            "password": credential["password"], "api_endpoint": endpoint,
            "broker_port": mqtt_port, "postgres_id": postgres_id,
            "owner_token": token, "owner_password": password,
            "sensor_id": sensor_id}


def gateway_spool_row(spool_db, upload_id):
    if not spool_db.is_file():
        return None
    try:
        with sqlite3.connect(f"file:{spool_db}?mode=ro", uri=True, timeout=2) as db:
            db.execute("PRAGMA query_only=ON")
            count = db.execute("SELECT count(*) FROM media_spool").fetchone()[0]
            row = db.execute(
                "SELECT upload_id,phase,media_id,spool_name,size_bytes,sha256,outcome "
                "FROM media_spool WHERE upload_id=?", (upload_id,),
            ).fetchone()
            return count, row
    except sqlite3.Error:
        return None


def managed_media_rows(registry, postgres_id):
    count = smoke.db_psql(
        registry, postgres_id,
        "SELECT count(*) FROM public.media_objects WHERE managed IS TRUE;",
    ).strip()
    require(count.isdigit(), "media_managed_row_count_invalid")
    rows = smoke.db_psql(
        registry, postgres_id,
        """SELECT json_build_array(media_id::text,gateway_id,upload_id,storage_path,
           media_type,expected_size::text,actual_size::text,validation_status,
           managed::text,declared_sha256,verified_sha256,actual_media_type)::text
           FROM public.media_objects WHERE managed IS TRUE ORDER BY media_id;""",
    ).splitlines()
    try:
        parsed = [json.loads(row) for row in rows if row]
    except json.JSONDecodeError:
        raise ProofFailure("media_managed_row_decode_failed") from None
    return int(count), parsed


def media_verification_state(registry, postgres_id, upload_id):
    require(re.fullmatch(r"upload_[A-Za-z0-9_-]{1,64}", upload_id) is not None,
            "fixture_upload_id_invalid_for_verification_readback")
    rows = smoke.db_psql(
        registry, postgres_id,
        f"""SELECT json_build_array(validation_status,last_verification_error,
           (next_verification_at IS NOT NULL)::text)::text
           FROM public.media_objects WHERE managed IS TRUE AND upload_id='{upload_id}';""",
    ).splitlines()
    try:
        parsed = [json.loads(row) for row in rows if row]
    except json.JSONDecodeError:
        raise ProofFailure("media_verification_state_decode_failed") from None
    require(len(parsed) <= 1, "media_verification_state_duplicate_upload")
    return parsed[0] if parsed else None


def media_ready_trigger(registry, postgres_id, upload_id, *, install):
    require(re.fullmatch(r"upload_[A-Za-z0-9_-]{1,64}", upload_id) is not None,
            "fixture_upload_id_invalid_for_trigger")
    if install:
        sql = f"""
        CREATE OR REPLACE FUNCTION public.stage8_fixture_hold_ready()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.upload_id = '{upload_id}' AND NEW.validation_status = 'ready'
               AND OLD.validation_status <> 'ready' THEN
                RAISE EXCEPTION 'fixture holds media ready transition';
            END IF;
            RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS stage8_fixture_hold_ready ON public.media_objects;
        CREATE TRIGGER stage8_fixture_hold_ready
            BEFORE UPDATE ON public.media_objects
            FOR EACH ROW EXECUTE FUNCTION public.stage8_fixture_hold_ready();
        """
    else:
        sql = """
        DROP TRIGGER IF EXISTS stage8_fixture_hold_ready ON public.media_objects;
        DROP FUNCTION IF EXISTS public.stage8_fixture_hold_ready();
        """
    smoke.db_psql(registry, postgres_id, sql)


def envoy_media_put_count(prefix, fixture):
    result = run(prefix + ["logs", "--no-color", "supabase-envoy"], fixture, timeout=30)
    text = (result.stdout + result.stderr).decode(errors="replace")
    return fixture_media_put_count(text)


def fixture_media_put_count(log_text):
    return sum(
        1 for line in log_text.splitlines()
        if '"PUT HTTP/' in line
        and line.rsplit(" ", 1)[-1] == "route=stage8-media-signed-put"
    )


def wait_for_media_put_count(read_count, *, expected, timeout, poll_interval=0.25,
                             monotonic=time.monotonic, sleep=time.sleep):
    started = monotonic()
    observations = 0
    while True:
        count = read_count()
        observations += 1
        elapsed = monotonic() - started
        if count > expected:
            raise ProofFailure(f"fixture_media_put_duplicate_count_{count}")
        if count == expected:
            return {
                "count": count,
                "observations": observations,
                "wait_seconds": round(elapsed, 3),
            }
        if elapsed >= timeout:
            raise ProofFailure(f"fixture_media_put_count_timeout_{count}")
        sleep(min(poll_interval, timeout - elapsed))


def wait_for_media_put_quiescence(read_count, *, expected, quiet_seconds,
                                  timeout, poll_interval=0.5,
                                  monotonic=time.monotonic, sleep=time.sleep):
    started = monotonic()
    stable_since = None
    observations = 0
    while True:
        count = read_count()
        observations += 1
        now = monotonic()
        elapsed = now - started
        if count > expected:
            raise ProofFailure(f"fixture_media_put_duplicate_count_{count}")
        if count == expected:
            if stable_since is None:
                stable_since = now
            if now - stable_since >= quiet_seconds:
                return {
                    "count": count,
                    "observations": observations,
                    "observation_seconds": round(elapsed, 3),
                    "quiet_seconds": round(now - stable_since, 3),
                }
        else:
            stable_since = None
        if elapsed >= timeout:
            raise ProofFailure(f"fixture_media_put_quiescence_timeout_{count}")
        sleep(min(poll_interval, timeout - elapsed))


def storage_readback(storage_port, service_token, object_path):
    path = "/object/authenticated/media-video/" + urllib.parse.quote(object_path, safe="/")
    request = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}{path}", method="GET",
        headers={"Authorization": "Bearer " + service_token},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=180) as response:
            digest = hashlib.sha256()
            count = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                count += len(chunk)
                require(count <= MAX_OBJECT_BYTES + 1, "storage_readback_over_limit")
                digest.update(chunk)
            return response.status, response.headers.get("Content-Type", ""), count, digest.hexdigest()
    except urllib.error.HTTPError as error:
        error.close()
        return error.code, "", 0, ""
    except (urllib.error.URLError, TimeoutError, OSError):
        raise ProofFailure("storage_object_readback_failed") from None


def storage_list(storage_port, service_token):
    request = urllib.request.Request(
        f"http://127.0.0.1:{storage_port}/object/list/media-video",
        data=json.dumps({
            "prefix": "", "limit": 100, "offset": 0,
            "sortBy": {"column": "name", "order": "asc"},
        }).encode(),
        method="POST",
        headers={
            "Authorization": "Bearer " + service_token,
            "Content-Type": "application/json",
        },
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=15) as response:
            payload = response.read(1_000_001)
            require(len(payload) <= 1_000_000, "storage_list_response_over_limit")
            if response.status != 200:
                raise ProofFailure("storage_list_http_status_mismatch")
    except urllib.error.HTTPError as error:
        error.close()
        raise ProofFailure("storage_list_http_status_mismatch") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise ProofFailure("storage_list_request_failed") from None
    try:
        objects = json.loads(payload)
    except json.JSONDecodeError:
        raise ProofFailure("storage_list_response_invalid") from None
    require(isinstance(objects, list), "storage_list_response_invalid")
    return objects


def verify_media_logs(prefix, fixture, gateway_log, secrets_to_scan):
    gateway_bytes = bounded_log_tail(gateway_log)
    compose_logs = run(
        prefix + ["logs", "--no-color", "--tail", "2000", "backend", "mosquitto",
                  "stage8-media-upload", "supabase-envoy", "supabase-storage"],
        fixture, timeout=30,
    ).stdout
    combined = gateway_bytes + compose_logs
    require(all(secret.encode() not in combined for secret in secrets_to_scan if secret),
            "credential_or_token_present_in_logs")
    lowered = combined.lower()
    require(b"?token=" not in lowered and b"&token=" not in lowered
            and b"authorization: bearer " not in lowered,
            "signed_upload_credential_present_in_logs")
    return {"status": "PASS", "raw_logs_persisted": False, "signed_query_token_logged": False}


def bounded_log_tail(path, limit=4 * 1024 * 1024):
    """Inspect only a bounded logfile tail; runtime logs are never loaded wholesale."""
    try:
        with path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - limit))
            return stream.read(limit)
    except OSError:
        return b""


def streamed_file_evidence(path, limit=MAX_OBJECT_BYTES + 1):
    digest = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                break
            count += len(chunk)
            if count > limit:
                raise ProofFailure("file_size_over_inspection_bound")
            digest.update(chunk)
    return count, digest.hexdigest()


def make_exact_cap_mp4(source, target):
    """Append a valid top-level BMFF free box while preserving all source bytes."""
    source_size = source.stat().st_size
    free_box_size = MAX_OBJECT_BYTES - source_size
    require(free_box_size >= 8 and free_box_size <= 0xFFFFFFFF,
            "included_h264_fixture_invalid")
    with source.open("rb") as original, target.open("wb") as output:
        shutil.copyfileobj(original, output, length=64 * 1024)
        output.write(struct.pack(">I4s", free_box_size, b"free"))
        output.truncate(MAX_OBJECT_BYTES)
    target.chmod(0o600)
    require(target.stat().st_size == MAX_OBJECT_BYTES, "exact_cap_fixture_size_mismatch")


def safe_runtime_log_summary(prefix, fixture, gateway_log, secrets_to_scan):
    result = run(
        prefix + ["logs", "--no-color", "--tail", "2000", "backend", "mosquitto"],
        fixture, timeout=30, check=False,
    )
    text = result.stdout.decode(errors="replace")
    try:
        text += "\n" + bounded_log_tail(gateway_log).decode(errors="replace")
    except (OSError, UnicodeError):
        pass
    for secret in secrets_to_scan:
        if secret:
            text = text.replace(secret, "[redacted]")
    text = re.sub(r"(?i)(token=)[^&\s]+", r"\1[redacted]", text)
    text = re.sub(r"(?i)(authorization:\s*bearer\s+)[^\s]+", r"\1[redacted]", text)
    selected = [
        line.strip()[:300] for line in text.splitlines()
        if any(marker in line.lower() for marker in
               ("media", "denied", "error", "failed", "upload", "acl", "connected"))
    ][-8:]
    return {"command_exit": result.returncode, "selected_lines": selected,
            "raw_logs_persisted": False}


def backend_media_precondition(fixture, prefix, report):
    """Require effective media config, live manager readback and positive SUBACK."""
    result = run(prefix + ["ps", "--quiet", "backend"], fixture, timeout=20, check=False)
    container_id = result.stdout.decode().strip()
    require(result.returncode == 0 and re.fullmatch(r"[a-f0-9]{12,64}", container_id),
            "backend_media_container_unavailable")
    inspected = run(
        ["docker", "inspect", "--format",
         "{{range .Config.Env}}{{println .}}{{end}}", container_id],
        fixture, timeout=20, check=False,
    )
    require(inspected.returncode == 0, "backend_effective_media_config_unavailable")
    allowed = {"MEDIA_UPLOAD_ENABLED", "MEDIA_UPLOAD_MQTT_CLIENT_ID", "MQTT_USERNAME"}
    effective = {}
    for line in inspected.stdout.decode(errors="replace").splitlines():
        key, separator, value = line.partition("=")
        if separator and key in allowed:
            effective[key] = value
    client_id = fixture.values["MEDIA_UPLOAD_MQTT_CLIENT_ID"]
    username = fixture.values["MQTT_USERNAME"]
    require(effective.get("MEDIA_UPLOAD_ENABLED", "").lower() == "true"
            and effective.get("MEDIA_UPLOAD_MQTT_CLIENT_ID") == client_id
            and effective.get("MQTT_USERNAME") == username
            and len(client_id) <= 128,
            "backend_effective_media_config_mismatch")
    report["checks"]["backend_effective_media_config"] = {
        "status": "PASS", "media_upload_enabled": True,
        "media_client_id": client_id, "data_username": username,
    }

    broker_port = mapped_loopback_port(prefix, fixture, "mosquitto", 8883)
    probe_env = {
        **smoke.process_env(fixture),
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "STAGE8_DYNSEC_PROBE": "1",
        "STAGE8_DYNSEC_BROKER_URL": f"ssl://localhost:{broker_port}",
        "STAGE8_DYNSEC_MANAGER_USERNAME": fixture.values["MQTT_DYNSEC_MANAGER_USERNAME"],
        "STAGE8_DYNSEC_MANAGER_PASSWORD": fixture.values["MQTT_DYNSEC_MANAGER_PASSWORD"],
        "STAGE8_DYNSEC_BACKEND_USERNAME": username,
        "STAGE8_DYNSEC_BACKEND_CLIENT_ID": client_id,
        "STAGE8_DYNSEC_CA_FILE": str(fixture.private / "certs" / "ca.crt"),
    }
    probe = subprocess.run(
        ["go", "test", "-v", "./internal/mqttcredential", "-run",
         "^TestStage8LiveBackendACLProbe$", "-count=1"],
        cwd=ROOT / "src", env=probe_env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90, check=False,
    )
    probe_text = (probe.stdout + probe.stderr).decode(errors="replace")
    reason_match = re.search(
        r"fixture_(?:manager_tls_connect|backend_acl_readback):([a-z0-9_]+)",
        probe_text,
    )
    if probe.returncode != 0:
        report["checks"]["backend_live_dynsec_readback"] = {
            "status": "FAIL", "reason_code": reason_match.group(1)
            if reason_match else "probe_failed",
        }
        report["checks"]["backend_runtime_diagnostic"] = safe_runtime_log_summary(
            prefix, fixture, fixture.private / "missing-gateway-log", [],
        )
        raise ProofFailure("backend_live_dynsec_readback_failed_" +
                           (reason_match.group(1) if reason_match else "probe_failed"))
    report["checks"]["backend_live_dynsec_readback"] = {
        "status": "PASS", "manager_tls": "PASS",
        "exact_client_role_acl_and_defaults": "PASS",
        "data_username": username, "client_id": client_id,
    }

    logs = ""
    subscribed = False
    broker_connected = False
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        current = run(
            prefix + ["logs", "--no-color", "--tail", "2000", "backend", "mosquitto"],
            fixture, timeout=20, check=False,
        )
        logs = current.stdout.decode(errors="replace")
        broker_connected = re.search(
            r"New client connected .* as " + re.escape(client_id)
            + r" \(.*u'" + re.escape(username) + r"'\)", logs,
        ) is not None
        subscribed = any(
            "event=subscribed" in line and "client_id=" + client_id in line
            and "filter=gateways/+/media/up/#" in line
            for line in logs.splitlines()
        )
        if broker_connected and subscribed:
            break
        time.sleep(0.5)
    if not broker_connected or not subscribed:
        safe_logs = safe_runtime_log_summary(
            prefix, fixture, fixture.private / "missing-gateway-log", [],
        )
        report["checks"]["backend_media_transport_readiness"] = {
            "status": "FAIL", "broker_connect_observed": broker_connected,
            "media_suback_observed": subscribed, "logs": safe_logs,
        }
        raise ProofFailure("backend_media_transport_not_ready")
    report["checks"]["backend_media_transport_readiness"] = {
        "status": "PASS", "broker_connect_observed": True,
        "positive_media_suback_observed": True, "filter": "gateways/+/media/up/#",
    }


def remove_fixture_work(path):
    def retry_writable(func, target, error):
        if isinstance(error, PermissionError):
            parent = os.path.dirname(target)
            os.chmod(parent, 0o700)
            if os.path.isdir(target):
                os.chmod(target, 0o700)
            func(target)
            return
        raise error

    shutil.rmtree(path, onexc=retry_writable)


def run_gateway_media_e2e(fixture, prefix, registry, material, report, secrets_to_scan,
                          exact_cap=False, restart_recovery=False, test_case="",
                          verifier_timeout=False, storage_proxy=None):
    """Run one actual Gateway media upload and verify DB, Storage, and spool."""
    gateway_id, upload_id, sensor_id = (
        material["gateway_id"], "upload_" + secrets.token_hex(8), material["sensor_id"],
    )
    source_dir = fixture.private / "media-source"
    spool_dir = fixture.private / "media-spool"
    binary_dir = fixture.private / "bin"
    for directory in (source_dir, spool_dir, binary_dir):
        directory.mkdir(mode=0o700)
        directory.chmod(0o700)
    source_path = source_dir / "h264-16x16.mp4"
    included_clip = ROOT / "src/internal/mediaworker/testdata/h264-16x16.mp4"
    if exact_cap:
        make_exact_cap_mp4(included_clip, source_path)
    else:
        shutil.copyfile(included_clip, source_path)
        source_path.chmod(0o600)
    expected_size, expected_sha = streamed_file_evidence(source_path)
    require(expected_size > 0 and expected_size <= MAX_OBJECT_BYTES,
            "included_h264_fixture_invalid")
    with source_path.open("rb") as source_stream:
        require(b"avc1" in source_stream.read(2 * 1024 * 1024),
                "included_h264_fixture_invalid")

    binary = binary_dir / "gateway"
    build = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/gateway"],
        cwd=ROOT / "src", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={**smoke.process_env(fixture), "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")},
        timeout=300, check=False,
    )
    require(build.returncode == 0 and binary.is_file(), "gateway_simulator_build_failed")
    gateway_port = material["broker_port"]
    upload_port = urllib.parse.urlsplit(fixture.values["MEDIA_UPLOAD_PUBLIC_ORIGIN"]).port
    require(upload_port is not None, "media_upload_loopback_origin_invalid")
    import stage3_uplink_runner
    gateway_env = stage3_uplink_runner.gateway_env(gateway_id, material["password"])
    gateway_env.update({
        "GATEWAY_ID": gateway_id,
        "SENSOR_ID": sensor_id,
        "GATEWAY_MQTT_BROKER_URL": f"ssl://localhost:{gateway_port}",
        "GATEWAY_MQTT_CLIENT_ID": gateway_id + "_media_sim",
        "GATEWAY_MQTT_USERNAME": material["username"],
        "GATEWAY_MQTT_PASSWORD": material["password"],
        "GATEWAY_MQTT_CA_FILE": str(fixture.private / "certs" / "ca.crt"),
        "GATEWAY_OUTBOX_PATH": str(fixture.private / "gateway-outbox.sqlite"),
        "MEDIA_UPLOAD_ENABLED": "true",
        "GATEWAY_MEDIA_INPUT_FILE": str(source_path),
        "GATEWAY_MEDIA_SPOOL_DIR": str(spool_dir),
        "GATEWAY_MEDIA_UPLOAD_ORIGIN": f"https://127.0.0.1:{upload_port}",
        "GATEWAY_MEDIA_UPLOAD_ID": upload_id,
        "GATEWAY_MEDIA_CA_FILE": str(fixture.private / "p2-ca.crt"),
        "GATEWAY_MEDIA_UPLOAD_TTL": "10m",
        "GATEWAY_MEDIA_RETRY_MIN_DELAY": "250ms",
        "GATEWAY_MEDIA_RETRY_MAX_DELAY": "1s",
        "GATEWAY_MEDIA_PUT_TIMEOUT": "240s" if exact_cap else "30s",
        "GATEWAY_SHUTDOWN_TIMEOUT": "15s",
    })
    env = smoke.process_env(fixture)
    env.update(gateway_env)
    gateway_log = fixture.private / "gateway-runtime.log"
    fd = os.open(gateway_log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    process = None
    started_at = time.monotonic()
    with os.fdopen(fd, "wb") as log_stream:
        command = [str(binary), "run"]
        if test_case:
            command.extend(["--test-media-case", test_case])
        process = subprocess.Popen(
            command, cwd=ROOT / "src", env=env,
            stdout=log_stream, stderr=subprocess.STDOUT, close_fds=True,
        )
    global ACTIVE_GATEWAY_PROCESS
    ACTIVE_GATEWAY_PROCESS = process
    report["phase"] = "gateway_media_upload"
    spool_db = spool_dir / "media.sqlite"
    trigger_installed = False
    if restart_recovery:
        media_ready_trigger(registry, material["postgres_id"], upload_id, install=True)
        trigger_installed = True
        report["checks"]["restart_ready_gate"] = {
            "status": "PASS", "scope": "one fixture upload_id",
        }
    deadline = time.monotonic() + (600 if exact_cap else 120)
    running_seen = False
    object_missing_observed = False
    verifier_timeout_observed = False
    media_row = None
    spool_state = None
    if restart_recovery:
        report["phase"] = "gateway_restart_gate_wait"
        gate_deadline = time.monotonic() + 90
        held = False
        while time.monotonic() < gate_deadline:
            if process.poll() is not None:
                raise ProofFailure(f"gateway_simulator_exited_{process.returncode}")
            count, rows = managed_media_rows(registry, material["postgres_id"])
            spool_state = gateway_spool_row(spool_db, upload_id)
            object_count = smoke.db_psql(
                registry, material["postgres_id"],
                "SELECT count(*) FROM storage.objects WHERE bucket_id='media-video';",
            ).strip()
            if (count == 1 and len(rows) == 1 and rows[0][2] == upload_id
                    and rows[0][7] == "verifying" and object_count == "1"
                    and spool_state is not None and spool_state[0] == 1
                    and spool_state[1] is not None
                    and spool_state[1][1] == "awaiting_result"
                    and (spool_dir / (upload_id + ".mp4")).is_file()):
                held = True
                break
            time.sleep(0.2)
        require(held, "restart_gate_did_not_observe_put_and_held_ready")
        ready_gate_observed_at = time.monotonic()
        before_stop_puts = wait_for_media_put_count(
            lambda: envoy_media_put_count(prefix, fixture),
            expected=1, timeout=20,
        )
        report["checks"]["restart_put_count_before_gateway_stop"] = {
            **before_stop_puts,
            "gate_signal_seconds_since_gateway_start": round(
                ready_gate_observed_at - started_at, 3,
            ),
        }
        before_stop_put_count = before_stop_puts["count"]
        process.send_signal(signal.SIGTERM)
        try:
            exit_code = process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise ProofFailure("gateway_restart_gate_shutdown_timeout") from None
        require(exit_code == 0, "gateway_restart_gate_shutdown_failed")
        ACTIVE_GATEWAY_PROCESS = None
        media_ready_trigger(registry, material["postgres_id"], upload_id, install=False)
        trigger_installed = False
        ready_deadline = time.monotonic() + 60
        while time.monotonic() < ready_deadline:
            count, rows = managed_media_rows(registry, material["postgres_id"])
            if count == 1 and len(rows) == 1 and rows[0][2] == upload_id and rows[0][7] == "ready":
                break
            time.sleep(0.25)
        else:
            raise ProofFailure("backend_ready_commit_after_gate_release_timeout")
        require((spool_dir / (upload_id + ".mp4")).is_file(),
                "gateway_removed_spool_while_offline_before_ready")
        report["checks"]["backend_committed_ready_while_gateway_offline"] = {
            "status": "PASS", "managed_rows": count, "validation_status": rows[0][7],
            "spool_file_present": True,
        }
        fd = os.open(gateway_log, os.O_WRONLY | os.O_APPEND, 0o600)
        with os.fdopen(fd, "ab") as log_stream:
            process = subprocess.Popen(
                [str(binary), "run"], cwd=ROOT / "src", env=env,
                stdout=log_stream, stderr=subprocess.STDOUT, close_fds=True,
            )
        ACTIVE_GATEWAY_PROCESS = process
        report["checks"]["actual_gateway_restart_same_spool"] = {
            "status": "PASS", "same_upload_id": True, "same_source_path": True,
            "same_sqlite_spool": True, "new_pid": process.pid,
        }
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise ProofFailure(f"gateway_simulator_exited_{process.returncode}")
        try:
            runtime_tail = bounded_log_tail(gateway_log)
            running_seen = running_seen or b"gateway simulator running" in runtime_tail
        except OSError:
            runtime_tail = b""
        if test_case == "drop-first-grant" and b"code=OBJECT_MISSING" in runtime_tail:
            object_missing_observed = True
            put_count_at_missing = envoy_media_put_count(prefix, fixture)
            require(put_count_at_missing == 0, "lost_grant_put_started_before_object_missing")
            report["checks"]["lost_grant_object_missing_before_put"] = {
                "status": "PASS", "upload_id": upload_id, "envoy_put_count": 0,
            }
        if running_seen:
            managed_count, rows = managed_media_rows(registry, material["postgres_id"])
            spool_state = gateway_spool_row(spool_db, upload_id)
            if (spool_state is not None and spool_state[1] is not None
                    and spool_state[1][6] != "ready"):
                # A terminal Gateway-side spool record is actionable evidence;
                # do not wait out the full upload deadline in that case.
                with sqlite3.connect(f"file:{spool_db}?mode=ro", uri=True, timeout=2) as db:
                    terminal = db.execute(
                        "SELECT phase,outcome,reason,attempts FROM media_spool WHERE upload_id=?",
                        (upload_id,),
                    ).fetchone()
                report["checks"]["gateway_spool_terminal_diagnostic"] = {
                    "phase": terminal[0], "outcome": terminal[1],
                    "reason": terminal[2], "attempts": terminal[3],
                    "managed_rows": managed_count,
                }
                if terminal[0] == "failed":
                    report["checks"]["runtime_log_diagnostic"] = safe_runtime_log_summary(
                        prefix, fixture, gateway_log, secrets_to_scan,
                    )
                    raise ProofFailure("gateway_media_spool_failed_" + str(terminal[2]))
            if managed_count > 1:
                raise ProofFailure("unexpected_managed_media_rows")
            if (managed_count == 1 and len(rows) == 1 and spool_state is not None
                    and spool_state[0] == 1 and spool_state[1] is not None
                    and rows[0][7] == "ready" and spool_state[1][1] == "ready"
                    and not (spool_dir / (upload_id + ".mp4")).exists()):
                media_row = rows[0]
                break
            if verifier_timeout and managed_count == 1 and len(rows) == 1:
                verification = media_verification_state(
                    registry, material["postgres_id"], upload_id,
                )
                if (verification == ["verifying", "STORAGE_UNAVAILABLE", "true"]
                        and storage_proxy is not None
                        and storage_proxy.first_get_delayed.is_set()):
                    object_count = smoke.db_psql(
                        registry, material["postgres_id"],
                        """SELECT count(*) FROM storage.objects
                           WHERE bucket_id='media-video' AND name='""" + rows[0][0] + """.mp4';""",
                    ).strip()
                    puts = envoy_media_put_count(prefix, fixture)
                    file_present = (spool_dir / (upload_id + ".mp4")).is_file()
                    require(object_count == "1" and puts == 1 and file_present
                            and spool_state is not None and spool_state[1] is not None
                            and spool_state[1][1] == "awaiting_result",
                            "verifier_timeout_intermediate_state_mismatch")
                    verifier_timeout_observed = True
                    report["checks"]["verifier_timeout_intermediate_state"] = {
                        "status": "PASS", "upload_id": upload_id,
                        "validation_status": verification[0],
                        "last_verification_error": verification[1],
                        "next_verification_at_present": verification[2] == "true",
                        "storage_object_count": 1, "envoy_put_count": puts,
                        "sqlite_phase": spool_state[1][1], "spool_file_present": file_present,
                        "first_storage_read_delayed": True,
                    }
        time.sleep(0.5)
    if media_row is None and (test_case == "drop-first-grant" or verifier_timeout):
        result_counts = {}
        grant_drop_seen = False
        for line in bounded_log_tail(gateway_log).decode(errors="replace").splitlines():
            if "gatewaysim media test grant dropped" in line:
                grant_drop_seen = True
            elif "gatewaysim media test result observed" in line:
                match = re.search(r"status=([A-Za-z_]+).*code=([A-Za-z0-9_]*)", line)
                if match:
                    key = match.group(1) + ":" + (match.group(2) or "none")
                    result_counts[key] = result_counts.get(key, 0) + 1
        count, rows = managed_media_rows(registry, material["postgres_id"])
        report["checks"]["recovery_failure_diagnostic"] = {
            "managed_rows": count,
            "validation_status": rows[0][7] if rows else None,
            "verification_state": media_verification_state(
                registry, material["postgres_id"], upload_id,
            ) if rows else None,
            "spool_phase": spool_state[1][1] if spool_state and spool_state[1] else None,
            "spool_outcome": spool_state[1][6] if spool_state and spool_state[1] else None,
            "envoy_put_count": envoy_media_put_count(prefix, fixture),
            "grant_drop_seen": grant_drop_seen, "result_counts": result_counts,
        }
    require(media_row is not None, "gateway_media_ready_timeout")
    if test_case == "drop-first-grant":
        runtime_log = bounded_log_tail(gateway_log)
        require(b"case=drop-first-grant" in runtime_log,
                "lost_grant_drop_event_missing")
        require(object_missing_observed, "lost_grant_object_missing_event_missing")
    if verifier_timeout:
        if not verifier_timeout_observed:
            latest_verification = media_verification_state(
                registry, material["postgres_id"], upload_id,
            )
            report["checks"]["verifier_timeout_failure_diagnostic"] = {
                "verification_state": latest_verification,
                "proxy_get_count": storage_proxy.get_count if storage_proxy else None,
                "first_get_delayed": bool(
                    storage_proxy and storage_proxy.first_get_delayed.is_set()
                ),
                "envoy_put_count": envoy_media_put_count(prefix, fixture),
            }
        require(verifier_timeout_observed, "verifier_timeout_intermediate_state_not_observed")

    (media_id, db_gateway, db_upload, db_path, media_type, size_expected,
     size_actual, validation, managed, declared_sha, verified_sha, actual_type) = media_row
    require(db_gateway == gateway_id and db_upload == upload_id
            and db_path == media_id + ".mp4" and media_type == "video/mp4"
            and size_expected == str(expected_size) and size_actual == str(expected_size)
            and validation == "ready" and managed == "true"
            and declared_sha == expected_sha and verified_sha == expected_sha
            and actual_type == "video/mp4",
            "managed_media_row_content_mismatch")
    spool_row = spool_state[1]
    require(spool_row[0] == upload_id and spool_row[2] == media_id
            and spool_row[3] == upload_id + ".mp4"
            and spool_row[4] == expected_size and spool_row[5] == expected_sha
            and not (spool_dir / spool_row[3]).exists(),
            "gateway_spool_ready_release_mismatch")

    storage_count = smoke.db_psql(
        registry, material["postgres_id"],
        "SELECT count(*) FROM storage.objects WHERE bucket_id='media-video';",
    ).strip()
    require(storage_count == "1", "private_video_object_count_mismatch")
    bucket_public = smoke.db_psql(
        registry, material["postgres_id"],
        "SELECT public::text FROM storage.buckets WHERE id='media-video';",
    ).strip()
    require(bucket_public == "false", "media_video_bucket_not_private")
    storage_rows = smoke.db_psql(
        registry, material["postgres_id"],
        """SELECT json_build_array(name,COALESCE(metadata->>'mimetype',''))::text
           FROM storage.objects WHERE bucket_id='media-video' ORDER BY name;""",
    ).splitlines()
    try:
        objects = [json.loads(line) for line in storage_rows if line]
    except json.JSONDecodeError:
        raise ProofFailure("storage_object_metadata_invalid") from None
    require(len(objects) == 1 and objects[0] == [db_path, "video/mp4"],
            "private_video_object_identity_or_mime_mismatch")
    storage_port = mapped_loopback_port(prefix, fixture, "supabase-storage", 5000)
    listed_objects = storage_list(storage_port, fixture.values["SERVICE_ROLE_KEY"])
    require(len(listed_objects) == 1 and isinstance(listed_objects[0], dict)
            and listed_objects[0].get("name") == db_path,
            "private_video_storage_listing_mismatch")
    read_status, read_type, read_count, read_sha = storage_readback(
        storage_port, fixture.values["SERVICE_ROLE_KEY"], db_path,
    )
    require(read_status == 200 and read_type.split(";", 1)[0].strip().lower() == "video/mp4"
            and read_count == expected_size and read_sha == expected_sha,
            "private_video_object_readback_mismatch")
    source_count, source_sha = streamed_file_evidence(source_path)
    require(source_path.is_file() and source_count == expected_size and source_sha == expected_sha,
            "gateway_input_source_changed")

    log_check = verify_media_logs(prefix, fixture, gateway_log, secrets_to_scan)
    # The spool row reaches ready only through the validated MQTT upload_result
    # handler. Verify all durable receipts before gracefully stopping the process.
    elapsed = round(time.monotonic() - started_at, 3)
    report["checks"]["gateway_upload_result_and_durable_verification"] = {
        "status": "PASS", "gateway_id": gateway_id, "upload_id": upload_id,
        "media_id": media_id, "storage_path": db_path,
        "managed_rows": 1, "private_bucket_objects": 1,
        "bucket_public": False,
        "storage_listing_objects": len(listed_objects),
        "declared_bytes": expected_size, "actual_bytes": read_count,
        "declared_sha256": expected_sha, "verified_sha256": verified_sha,
            "actual_sha256": read_sha, "declared_mime": media_type,
            "verified_mime": actual_type, "storage_mime": read_type,
        "sqlite_phase": spool_row[1],
        "spool_copy_gone": True, "source_untouched": True,
        "gateway_to_ready_seconds": elapsed, "exact_cap_mode": exact_cap, "logs": log_check,
    }
    if test_case == "drop-first-grant":
        require(log_check.get("status") == "PASS", "lost_grant_log_safety_check_failed")
        report["checks"]["lost_grant_recovery"] = {
            "status": "PASS", "upload_id": upload_id, "media_id": media_id,
            "object_missing_observed": object_missing_observed,
            "put_count_at_object_missing": 0, "final_put_count": 1,
            "final_validation_status": validation, "final_spool_phase": spool_row[1],
            "storage_path": db_path, "sha256": expected_sha,
        }
    process.send_signal(signal.SIGTERM)
    try:
        exit_code = process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise ProofFailure("gateway_graceful_shutdown_timeout") from None
    require(exit_code == 0, "gateway_graceful_shutdown_failed")
    ACTIVE_GATEWAY_PROCESS = None
    report["checks"]["gateway_graceful_shutdown"] = {
        "status": "PASS", "exit_code": exit_code,
    }
    final_spool = gateway_spool_row(spool_db, upload_id)
    require(final_spool is not None and final_spool[0] == 1
            and final_spool[1] is not None and final_spool[1][1] == "ready"
            and final_spool[1][2] == media_id
            and not (spool_dir / (upload_id + ".mp4")).exists()
            and source_path.is_file(),
            "gateway_spool_post_stop_state_mismatch")
    report["checks"]["post_stop_spool_and_source"] = {"status": "PASS"}
    if test_case == "drop-first-grant" or verifier_timeout:
        single_put = wait_for_media_put_quiescence(
            lambda: envoy_media_put_count(prefix, fixture),
            expected=1, quiet_seconds=4, timeout=35,
        )
        report["checks"]["recovery_single_put_quiescence"] = {
            "status": "PASS", **single_put,
        }
        if verifier_timeout:
            require(storage_proxy is not None and storage_proxy.get_count >= 2,
                    "verifier_timeout_retry_did_not_read_existing_object")
            report["checks"]["verifier_timeout_recovered_existing_object"] = {
                "status": "PASS", "upload_id": upload_id, "media_id": media_id,
                "final_validation_status": validation, "sqlite_phase": spool_row[1],
                "spool_file_removed": not (spool_dir / (upload_id + ".mp4")).exists(),
                "storage_read_attempts_through_proxy": storage_proxy.get_count,
                "single_successful_put": True, "sha256": expected_sha,
                "verified_sha256": verified_sha,
            }
    if restart_recovery:
        post_restart_observation = wait_for_media_put_quiescence(
            lambda: envoy_media_put_count(prefix, fixture),
            expected=1, quiet_seconds=4, timeout=35,
        )
        report["checks"]["restart_recovery_single_put"] = {
            "status": "PASS", **post_restart_observation,
            "envoy_put_count_before_restart": before_stop_put_count,
        }
    if trigger_installed:
        media_ready_trigger(registry, material["postgres_id"], upload_id, install=False)
    return process


def run_cleanup_worker_e2e(fixture, prefix, registry, material, report):
    """Exercise the production cleanup worker against a real Storage object."""
    storage_port = mapped_loopback_port(prefix, fixture, "supabase-storage", 5000)
    upload_port = mapped_loopback_port(prefix, fixture, "stage8-media-upload-test-edge", 443)
    ready_rows = smoke.db_psql(
        registry, material["postgres_id"],
        """SELECT json_build_array(media_id::text,storage_path,verified_sha256,
           validation_status)::text FROM public.media_objects
           WHERE managed AND gateway_id='""" + material["gateway_id"] + """'
             AND validation_status='ready' ORDER BY media_id;""",
    ).splitlines()
    try:
        ready_records = [json.loads(row) for row in ready_rows if row]
    except json.JSONDecodeError:
        raise ProofFailure("cleanup_ready_control_row_invalid") from None
    require(len(ready_records) == 1 and ready_records[0][3] == "ready",
            "cleanup_ready_control_missing")
    ready_media_id, ready_path, ready_sha, _ = ready_records[0]
    require(re.fullmatch(r"[0-9a-f-]{36}\.mp4", ready_path) is not None
            and re.fullmatch(r"[0-9a-f]{64}", ready_sha) is not None,
            "cleanup_ready_control_metadata_invalid")

    cleanup_media_id = str(uuid.uuid4())
    cleanup_path = cleanup_media_id + ".mp4"
    cleanup_upload_id = "cleanup_probe_" + secrets.token_hex(8)
    payload = VIDEO_BYTES
    payload_sha = hashlib.sha256(payload).hexdigest()
    endpoint = smoke.HTTPSEndpoint(
        f"https://127.0.0.1:{upload_port}", fixture.private / "p2-ca.crt",
    )
    grant = make_grant(fixture, storage_port, upload_port, cleanup_path)
    target, _, _ = grant_target(grant, upload_port, cleanup_path)
    put_status, _, _ = https_request(endpoint, "PUT", target, payload, {
        "Content-Type": "video/mp4",
    })
    require(put_status in (200, 201), "cleanup_probe_signed_put_failed")
    initial_status, _, initial_size, initial_sha = storage_readback(
        storage_port, fixture.values["SERVICE_ROLE_KEY"], cleanup_path,
    )
    require(initial_status == 200 and initial_size == len(payload)
            and initial_sha == payload_sha, "cleanup_probe_object_readback_failed")

    # Seed only the terminal row and due timestamps. The production worker must
    # perform both Storage deletes, the follow-up wait, and DB confirmation.
    smoke.db_psql(
        registry, material["postgres_id"],
        f"""
        INSERT INTO public.media_objects
          (media_id,gateway_id,storage_path,media_type,expected_size,validation_status,
           created_at,managed,upload_id,captured_at,intent_expires_at,declared_sha256,
           latest_upload_grant_expires_at,terminal_reason,cleanup_scheduled_at,next_cleanup_at)
        VALUES ('{cleanup_media_id}','{material["gateway_id"]}','{cleanup_path}',
           'video/mp4',{len(payload)},'expired',now()-interval '30 seconds',true,
           '{cleanup_upload_id}',now()-interval '20 seconds',now()-interval '5 seconds',
           '{payload_sha}',now()-interval '4 seconds','intent_expired',
           now()-interval '3 seconds',now()-interval '2 seconds');
        """,
    )

    def cleanup_snapshot():
        raw = smoke.db_psql(
            registry, material["postgres_id"],
            f"""SELECT json_build_array(validation_status,terminal_reason,
                cleanup_attempts,cleanup_first_deleted_at IS NOT NULL,
                cleanup_confirmed_at IS NOT NULL,COALESCE(last_cleanup_error,''))
                FROM public.media_objects WHERE media_id='{cleanup_media_id}';""",
        ).strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            raise ProofFailure("cleanup_probe_db_snapshot_invalid") from None

    def unsettled_for_gateway():
        raw = smoke.db_psql(
            registry, material["postgres_id"],
            f"""SELECT count(*) FROM public.media_objects WHERE managed
                AND gateway_id='{material["gateway_id"]}'
                AND (validation_status IN ('pending','verifying') OR
                  (validation_status IN ('rejected','expired')
                   AND cleanup_confirmed_at IS NULL));""",
        ).strip()
        require(raw.isdigit(), "cleanup_probe_quota_count_invalid")
        return int(raw)

    deadline = time.monotonic() + 35
    first_absence = None
    while time.monotonic() < deadline:
        snapshot = cleanup_snapshot()
        if snapshot[3] and not snapshot[4]:
            missing = storage_readback(
                storage_port, fixture.values["SERVICE_ROLE_KEY"], cleanup_path,
            )
            if missing[0] in (400, 404):
                first_absence = snapshot
                break
        time.sleep(0.1)
    require(first_absence is not None,
            "cleanup_probe_first_delete_or_followup_not_observed")
    require(first_absence[0] == "expired" and first_absence[1] == "intent_expired"
            and first_absence[2] == 1 and first_absence[5] == "FOLLOWUP_WINDOW",
            "cleanup_probe_first_delete_state_invalid")
    quota_before_confirm = unsettled_for_gateway()
    require(quota_before_confirm == 1,
            "cleanup_probe_quota_released_before_confirmation")
    ready_before, _, ready_before_size, ready_before_sha = storage_readback(
        storage_port, fixture.values["SERVICE_ROLE_KEY"], ready_path,
    )
    require(ready_before == 200 and ready_before_size > 0 and ready_before_sha == ready_sha,
            "cleanup_probe_ready_object_changed_before_confirmation")

    while time.monotonic() < deadline:
        snapshot = cleanup_snapshot()
        if snapshot[4]:
            break
        time.sleep(0.1)
    else:
        raise ProofFailure("cleanup_probe_confirmation_timeout")
    require(snapshot[0] == "expired" and snapshot[1] == "intent_expired"
            and snapshot[2] >= 2 and snapshot[3] and snapshot[4] and snapshot[5] == "",
            "cleanup_probe_confirmed_state_invalid")
    final_missing = storage_readback(
        storage_port, fixture.values["SERVICE_ROLE_KEY"], cleanup_path,
    )
    require(final_missing[0] in (400, 404), "cleanup_probe_object_still_present")
    quota_after_confirm = unsettled_for_gateway()
    require(quota_after_confirm == 0,
            "cleanup_probe_quota_not_released_after_confirmation")
    ready_after, _, ready_after_size, ready_after_sha = storage_readback(
        storage_port, fixture.values["SERVICE_ROLE_KEY"], ready_path,
    )
    require(ready_after == 200 and ready_after_size == ready_before_size
            and ready_after_sha == ready_sha,
            "cleanup_probe_ready_object_deleted_or_changed")
    ready_status = smoke.db_psql(
        registry, material["postgres_id"],
        f"SELECT validation_status FROM public.media_objects WHERE media_id='{ready_media_id}';",
    ).strip()
    require(ready_status == "ready", "cleanup_probe_ready_row_changed")
    report["checks"]["native_cleanup_worker_storage_postgres"] = {
        "status": "PASS",
        "seeded_terminal_state": "expired",
        "real_storage_put_and_initial_readback": "PASS",
        "first_delete_absence_observed": True,
        "first_delete_attempts": first_absence[2],
        "quota_unsettled_before_confirmation": quota_before_confirm,
        "followup_delete_attempts": snapshot[2],
        "cleanup_confirmed": True,
        "object_absent_after_followup": True,
        "quota_unsettled_after_confirmation": quota_after_confirm,
        "ready_control_row_and_object_preserved": True,
    }


def run_fixture(stage8_local_base=False, prepare_stage8_e2e=False,
                stage8_local_e2e=False, stage8_local_e2e_cap=False,
                stage8_local_restart=False, stage8_local_cleanup=False,
                stage8_local_lost_grant=False, stage8_local_verifier_timeout=False):
    scratch_root = Path("/var/tmp/opencode")
    scratch_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="stage8-media-http-", dir=scratch_root))
    work.chmod(0o700)
    fixture = None
    prefix = None
    registry = None
    storage_proxy = None
    report = {"schema_version": 1, "status": "FAIL", "checks": {}}
    primary = None
    cleanup_verified = False
    prepare_mode = False
    try:
        fixture = smoke.prepare(work / "fixture", paper_p2=True)
        fixture.values.update({
            "MEDIA_UPLOAD_ROUTE_KEY": secrets.token_hex(32),
            "MEDIA_TEST_TLS_DIR": str(fixture.private / "p2-tls"),
            "MEDIA_HTTP_PORT": "0",
            "MEDIA_UPLOAD_GRANT_TTL_SECONDS": "3" if stage8_local_lost_grant else "30",
        })
        prepare_mode = (prepare_stage8_e2e or stage8_local_e2e or stage8_local_e2e_cap
                        or stage8_local_restart or stage8_local_cleanup
                        or stage8_local_lost_grant or stage8_local_verifier_timeout)
        if prepare_mode:
            fixture.values.update(
                MEDIA_UPLOAD_ENABLED="true",
                MEDIA_UPLOAD_MQTT_CLIENT_ID="backend_media_" + secrets.token_hex(5),
                STORAGE_INTERNAL_URL="http://supabase-storage:5000",
                MEDIA_UPLOAD_PUBLIC_ORIGIN="",
            )
        if stage8_local_cleanup:
            fixture.values.update(
                MEDIA_UPLOAD_CLEANUP_SCAN_INTERVAL="250ms",
                MEDIA_UPLOAD_CLEANUP_RETRY_MIN_DELAY="1s",
                MEDIA_UPLOAD_CLEANUP_RETRY_MAX_DELAY="1s",
                MEDIA_UPLOAD_CLEANUP_MAX_ATTEMPTS="3",
            )
        if stage8_local_verifier_timeout:
            fixture.values.update(
                MEDIA_UPLOAD_STORAGE_TIMEOUT="1s",
                MEDIA_UPLOAD_VERIFY_SCAN_INTERVAL="250ms",
                MEDIA_UPLOAD_VERIFY_RETRY_MIN_DELAY="2s",
                MEDIA_UPLOAD_VERIFY_RETRY_MAX_DELAY="2s",
            )
        if stage8_local_lost_grant:
            fixture.values.update(
                MEDIA_UPLOAD_INFLIGHT_GRACE="1s",
                MEDIA_UPLOAD_VERIFY_SCAN_INTERVAL="250ms",
            )
        fixture.env_file.write_text(
            "".join(f"{name}={value}\n" for name, value in sorted(fixture.values.items())),
            encoding="utf-8",
        )
        fixture.env_file.chmod(0o600)
        storage_overlay = fixture.private / "compose.storage-loopback.yml"
        smoke.private_write(storage_overlay, """services:
  supabase-storage:
    ports: !override
      - target: 5000
        published: "0"
        host_ip: 127.0.0.1
        protocol: tcp
""")
        if stage8_local_verifier_timeout:
            with storage_overlay.open("a", encoding="utf-8") as overlay:
                overlay.write("""  backend:
    extra_hosts:
      - "host.docker.internal:host-gateway"
""")
        broker_overlay = fixture.private / "compose.broker-loopback.yml"
        smoke.private_write(broker_overlay, """services:
  mosquitto:
    ports: !override
      - target: 8883
        published: "0"
        host_ip: 127.0.0.1
        protocol: tcp
""")
        prefix = compose_prefix(fixture, storage_overlay, media_overlay=not stage8_local_base) + ["-f", str(broker_overlay)]
        registry = smoke.ResourceRegistry(fixture)
        run(prefix + ["config", "--quiet"], fixture, timeout=60)
        report["checks"]["compose_model"] = {"status": "PASS"}
        lifecycle = smoke.bootstrap_lifecycle()
        smoke.generate_fixture_tls(fixture)
        volume = smoke.allocate_bootstrap_store(
            fixture, registry, lifecycle, media_upload_enabled=prepare_mode,
        )
        smoke.allocation_readback(
            fixture, registry, volume, media_upload_enabled=prepare_mode,
        )
        report["checks"]["owned_dynsec_bootstrap"] = {"status": "PASS"}
        built = run(prefix + ["build", "backend"], fixture, timeout=300, check=False)
        require(built.returncode == 0, "isolated_backend_build_failed")
        if stage8_local_base:
            services = list(smoke.fixture_services(fixture))
            started = run(prefix + ["up", "--detach", "--no-build", "--pull", "never", *services],
                          fixture, timeout=300, check=False)
            if started.returncode:
                raise ProofFailure("isolated_compose_start_failed_exit_" + str(started.returncode) + ":" +
                                   safe_compose_diagnostic(started.stdout + started.stderr, fixture))
            storage_port = mapped_loopback_port(prefix, fixture, "supabase-storage", 5000)
            api_port = mapped_loopback_port(prefix, fixture, "nginx", 443)
            api_endpoint = smoke.HTTPSEndpoint(f"https://127.0.0.1:{api_port}", fixture.private / "p2-ca.crt")
            ready = False
            for _ in range(60):
                try:
                    if smoke.http_request(api_endpoint, "GET", "/readyz", timeout=2)[0] == 200:
                        ready = True
                        break
                except ValueError:
                    pass
                time.sleep(1)
            require(ready, "base_nginx_readiness_timeout")
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "init_media_bucket", ROOT / "scripts/init-media-bucket.py",
            )
            media_init = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(media_init)
            storage_api = media_init.StorageAPI(
                f"http://127.0.0.1:{storage_port}", fixture.values["SERVICE_ROLE_KEY"],
            )
            media_init.ensure_image_bucket(storage_api)
            report["checks"]["base_bucket_bootstrap"] = {"status": "PASS"}
            upload_port_probe = subprocess.run(
                prefix + ["port", "stage8-media-upload", "80"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=smoke.process_env(fixture), timeout=10, check=False,
            )
            require(upload_port_probe.returncode != 0, "dedicated_listener_unexpectedly_present_in_base")
            report["checks"]["dedicated_listener_absent_in_base"] = {"status": "PASS"}
            prove_legacy_storage(fixture, prefix, report, api_endpoint=api_endpoint, dedicated_endpoint=None)
            report["checks"]["base_composition_verified"] = {"status": "PASS"}
            report["status"] = "PASS"
        elif prepare_mode:
            dependencies = [s for s in smoke.fixture_services(fixture)
                            if s not in ("backend", "nginx")]
            started = run(prefix + ["up", "--detach", "--no-build", "--pull", "never",
                                    *dependencies, "stage8-media-upload",
                                    "stage8-media-upload-test-edge"],
                          fixture, timeout=300, check=False)
            require(started.returncode == 0, "stage8_prerequisite_compose_start_failed")
            upload_port = mapped_loopback_port(prefix, fixture, "stage8-media-upload-test-edge", 443)
            storage_port = mapped_loopback_port(prefix, fixture, "supabase-storage", 5000)
            fixture.values["MEDIA_UPLOAD_PUBLIC_ORIGIN"] = f"https://127.0.0.1:{upload_port}"
            if stage8_local_verifier_timeout:
                storage_proxy = FirstStorageReadDelayProxy(
                    storage_port, docker_host_gateway(fixture),
                )
                storage_proxy.start()
                fixture.values["STORAGE_INTERNAL_URL"] = (
                    f"http://host.docker.internal:{storage_proxy.port}"
                )
            fixture.env_file.write_text(
                "".join(f"{name}={value}\n" for name, value in sorted(fixture.values.items())),
                encoding="utf-8",
            )
            fixture.env_file.chmod(0o600)
            started = run(prefix + ["up", "--detach", "--no-build", "--pull", "never",
                                    "backend", "nginx"],
                          fixture, timeout=300, check=False)
            require(started.returncode == 0, "stage8_backend_compose_start_failed")
            api_port = mapped_loopback_port(prefix, fixture, "nginx", 443)
            api_endpoint = smoke.HTTPSEndpoint(
                f"https://127.0.0.1:{api_port}", fixture.private / "p2-ca.crt",
            )
            ready = False
            for _ in range(90):
                try:
                    if smoke.http_request(api_endpoint, "GET", "/readyz", timeout=2)[0] == 200:
                        ready = True
                        break
                except ValueError:
                    pass
                time.sleep(1)
            require(ready, "stage8_backend_readiness_timeout")
            if (stage8_local_e2e or stage8_local_e2e_cap or stage8_local_restart
                    or stage8_local_cleanup or stage8_local_lost_grant
                    or stage8_local_verifier_timeout):
                report["phase"] = "backend_media_readiness_precondition"
                backend_media_precondition(fixture, prefix, report)
            env = smoke.process_env(fixture)
            env.update(STORAGE_INTERNAL_URL=f"http://127.0.0.1:{storage_port}",
                       SERVICE_ROLE_KEY=fixture.values["SERVICE_ROLE_KEY"])
            init = subprocess.run(
                [sys.executable, str(ROOT / "scripts/init-media-bucket.py")],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
                timeout=30, check=False,
            )
            require(init.returncode == 0, "stage8_private_bucket_bootstrap_failed")
            report["checks"]["media_backend_and_private_bucket"] = {"status": "PASS"}
            sensor_id = ("sensor_stage8_" + secrets.token_hex(4)
                         if stage8_local_e2e or stage8_local_e2e_cap or stage8_local_restart
                         or stage8_local_cleanup or stage8_local_lost_grant
                         or stage8_local_verifier_timeout
                         else None)
            material = provision_gateway_credential(
                fixture, prefix, registry, report, sensor_id=sensor_id,
            )
            report["checks"]["credential_in_memory_only"] = {
                "status": "PASS", "password_persisted": False,
            }
            if (stage8_local_e2e or stage8_local_e2e_cap or stage8_local_restart
                    or stage8_local_cleanup or stage8_local_lost_grant
                    or stage8_local_verifier_timeout):
                secrets_to_scan = [
                    value for key, value in fixture.values.items()
                    if isinstance(value, str) and value and any(
                        marker in key for marker in ("KEY", "SECRET", "PASSWORD")
                    )
                ]
                secrets_to_scan.extend(
                    material.get(key, "")
                    for key in ("password", "owner_password", "owner_token")
                )
                run_gateway_media_e2e(
                    fixture, prefix, registry, material, report, secrets_to_scan,
                    exact_cap=stage8_local_e2e_cap,
                    restart_recovery=stage8_local_restart,
                    test_case=("drop-first-grant" if stage8_local_lost_grant else ""),
                    verifier_timeout=stage8_local_verifier_timeout,
                    storage_proxy=storage_proxy,
                )
                if stage8_local_cleanup:
                    run_cleanup_worker_e2e(fixture, prefix, registry, material, report)
                material["password"] = ""
                material["owner_password"] = ""
                material["owner_token"] = ""
            else:
                material["password"] = ""
                endpoint = smoke.HTTPSEndpoint(
                    f"https://127.0.0.1:{upload_port}", fixture.private / "p2-ca.crt",
                )
                health = https_request(endpoint, "GET", "/healthz")
                require(health[0] == 200 and health[2] == b"OK", "media_listener_health_failed")
                require(https_request(endpoint, "GET", "/v1/health")[0] == 404,
                        "media_listener_exposed_business_route")
                report["checks"]["dedicated_listener_isolation"] = {"status": "PASS"}
                prove_legacy_storage(fixture, prefix, report, api_endpoint=api_endpoint, dedicated_endpoint=endpoint)
                report["checks"]["media_upload_enabled_backend_verified"] = {
                    "status": "PASS", "runtime_flag": "MEDIA_UPLOAD_ENABLED=true",
                }
            report["status"] = "PASS"
        else:
            services = list(smoke.fixture_services(fixture)) + [
                "stage8-media-upload", "stage8-media-upload-test-edge",
            ]
            started = run(prefix + ["up", "--detach", "--no-build", "--pull", "never", *services],
                          fixture, timeout=300, check=False)
            if started.returncode:
                raise ProofFailure("isolated_compose_start_failed_exit_" + str(started.returncode) + ":" +
                                   safe_compose_diagnostic(started.stdout + started.stderr, fixture))
            ready = False
            for _ in range(60):
                try:
                    port = mapped_loopback_port(prefix, fixture, "stage8-media-upload-test-edge", 443)
                    endpoint = smoke.HTTPSEndpoint(f"https://127.0.0.1:{port}", fixture.private / "p2-ca.crt")
                    if https_request(endpoint, "GET", "/healthz", timeout=1)[0] == 200:
                        ready = True
                        break
                except ProofFailure:
                    pass
                time.sleep(1)
            require(ready, "media_listener_health_timeout")
            prove_fixture(fixture, prefix, report)
            report["status"] = "PASS"
    except ProofFailure as exc:
        primary = str(exc)
        report["failure_code"] = primary
    except Exception as exc:
        primary = "fixture_unexpected_failure_" + type(exc).__name__
        safe_message = str(exc)
        if safe_message in {
            "http_request_failed: TimeoutError",
            "http_request_failed: ConnectionRefusedError",
            "http_request_failed: ConnectionResetError",
            "http_request_failed: RemoteDisconnected",
            "http_request_failed: SSLCertVerificationError",
            "http_request_failed: SSLError",
            "http_body_too_large",
        }:
            primary += "_" + safe_message.replace(" ", "_").replace(":", "")
        report["failure_code"] = primary
        report["failure_phase"] = report.get("phase", "startup")
    finally:
        global ACTIVE_GATEWAY_PROCESS
        if ACTIVE_GATEWAY_PROCESS is not None and ACTIVE_GATEWAY_PROCESS.poll() is None:
            try:
                ACTIVE_GATEWAY_PROCESS.send_signal(signal.SIGTERM)
                ACTIVE_GATEWAY_PROCESS.wait(timeout=20)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    ACTIVE_GATEWAY_PROCESS.kill()
                    ACTIVE_GATEWAY_PROCESS.wait(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    cleanup_verified = False
            ACTIVE_GATEWAY_PROCESS = None
        if storage_proxy is not None:
            try:
                storage_proxy.close()
            except OSError:
                cleanup_verified = False
        if prefix is not None:
            if registry is not None and prepare_mode:
                try:
                    # Fixture-owned DB only; safe no-op if the recovery gate was
                    # never installed or was already removed.
                    postgres_ids = subprocess.run(
                        prefix + ["ps", "--quiet", "--no-trunc", "postgres"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        env=smoke.process_env(fixture), timeout=30, check=False,
                    )
                    postgres_id = postgres_ids.stdout.decode().strip()
                    if len(postgres_id) == 64:
                        media_ready_trigger(registry, postgres_id, "upload_cleanup", install=False)
                except Exception:
                    cleanup_verified = False
            down = subprocess.run(
                prefix + ["down", "--volumes", "--remove-orphans"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=smoke.process_env(fixture), timeout=180, check=False,
            )
            ps = subprocess.run(
                prefix + ["ps", "--all", "--quiet"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=smoke.process_env(fixture), timeout=30, check=False,
            )
            cleanup_verified = down.returncode == 0 and ps.returncode == 0 and not ps.stdout.strip()
            if registry is not None:
                try:
                    for item in reversed(registry.items):
                        registry.remove(item)
                except Exception:
                    cleanup_verified = False
                cleanup_verified = cleanup_verified and not any(
                    registry.resolve(item) is not None for item in registry.items
                )
            volumes = subprocess.run(
                ["docker", "volume", "ls", "--quiet", "--filter",
                 "label=com.docker.compose.project=" + fixture.project],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=smoke.process_env(fixture), timeout=30, check=False,
            )
            networks = subprocess.run(
                ["docker", "network", "ls", "--quiet", "--filter",
                 "label=com.docker.compose.project=" + fixture.project],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=smoke.process_env(fixture), timeout=30, check=False,
            )
            cleanup_verified = (
                cleanup_verified and volumes.returncode == 0 and not volumes.stdout.strip()
                and networks.returncode == 0 and not networks.stdout.strip()
            )
            report["exact_cleanup_verified"] = cleanup_verified
            if not cleanup_verified:
                report["status"] = "FAIL"
                report["failure_code"] = report.get("failure_code", "owned_cleanup_failed")
        else:
            cleanup_verified = True
        if fixture is not None:
            smoke.durable_json(fixture.artifacts / "stage8-media-http.json", report)
        print(json.dumps(report, sort_keys=True))
        if cleanup_verified:
            remove_fixture_work(work)

    return 0 if report.get("status") == "PASS" and cleanup_verified else 1


def main():
    args = sys.argv[1:]
    if args not in ([], ["--stage8-local-base"], ["--prepare-stage8-e2e"], ["--stage8-local-e2e"],
                    ["--stage8-local-e2e-cap"], ["--stage8-local-restart"],
                    ["--stage8-local-cleanup"], ["--stage8-local-lost-grant"],
                    ["--stage8-local-verifier-timeout"]):
        print("usage: stage8_media_storage_http_fixture.py "
              "[--stage8-local-base|--prepare-stage8-e2e|--stage8-local-e2e|"
              "--stage8-local-e2e-cap|--stage8-local-restart|--stage8-local-cleanup|"
              "--stage8-local-lost-grant|--stage8-local-verifier-timeout]",
              file=sys.stderr)
        return 2
    return run_fixture(
        stage8_local_base=args == ["--stage8-local-base"],
        prepare_stage8_e2e=args == ["--prepare-stage8-e2e"],
        stage8_local_e2e=args == ["--stage8-local-e2e"],
        stage8_local_e2e_cap=args == ["--stage8-local-e2e-cap"],
        stage8_local_restart=args == ["--stage8-local-restart"],
        stage8_local_cleanup=args == ["--stage8-local-cleanup"],
        stage8_local_lost_grant=args == ["--stage8-local-lost-grant"],
        stage8_local_verifier_timeout=args == ["--stage8-local-verifier-timeout"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
